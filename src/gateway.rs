//! Network gateway: high-speed order intake over a long-lived connection.
//!
//! The order system connects once (a persistent TCP connection) and streams
//! fixed-size [`crate::wire`] frames. The gateway:
//!
//! * reads bytes into a **single reusable buffer** (no per-message allocation);
//! * decodes each frame **in place** with a borrowed [`crate::wire::WireView`]
//!   (parse-in-place, the software analogue of zero-copy NIC ingest);
//! * pushes the resulting [`Command`](crate::exchange::Command) into the lock-free
//!   [`OrderGateway`];
//! * on a separate thread, drains the [`ResultSink`] and streams execution
//!   reports back over the same connection — asynchronous notification.
//!
//! ## Transport abstraction
//!
//! The read/write path only needs `Read`/`Write`, so any byte transport works.
//! TCP is implemented here; a **WebSocket** transport plugs in at the same seam
//! (perform the HTTP upgrade, then feed unmasked frame payloads to the same
//! decode loop). A **kernel-bypass** driver (AF_XDP / DPDK / `io_uring`) plugs in
//! by handing its RX buffer slices to `WireView::parse` — the decoding does not
//! change, only where the bytes come from.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::exchange::{Command, OrderGateway, ResultSink};
use crate::wire::{self, WireView, MSG_LEN, REPORT_LEN};

/// Market-data fanout: subscribers connected on a side port each receive a copy
/// of every execution-report frame (write-only feed — e.g. a candle/K-line
/// service). Dead subscribers are dropped on first failed write.
#[derive(Clone, Default)]
pub struct MdFanout {
    subs: Arc<Mutex<Vec<TcpStream>>>,
}

impl MdFanout {
    /// Accept subscriber connections on `listener` in a background thread.
    pub fn accept_on(listener: TcpListener, running: Arc<AtomicBool>) -> MdFanout {
        let fanout = MdFanout::default();
        let subs = fanout.subs.clone();
        thread::Builder::new()
            .name("md-accept".into())
            .spawn(move || {
                for stream in listener.incoming() {
                    if !running.load(Ordering::Acquire) {
                        break;
                    }
                    if let Ok(s) = stream {
                        s.set_nodelay(true).ok();
                        eprintln!("[gateway] market-data subscriber connected");
                        subs.lock().unwrap().push(s);
                    }
                }
            })
            .expect("spawn md acceptor");
        fanout
    }

    /// Broadcast a batch of report frames to every subscriber.
    fn broadcast(&self, bytes: &[u8]) {
        let mut subs = self.subs.lock().unwrap();
        subs.retain_mut(|s| s.write_all(bytes).is_ok());
    }
}

/// Bind `addr`, accept one long-lived connection, and run the order intake and
/// report feedback loops until the peer disconnects or `running` is cleared.
///
/// `gateway` is used by the reader thread (single producer); `sink` by the writer
/// thread (single consumer) — matching the SPSC discipline of the queues.
pub fn serve<A: ToSocketAddrs>(
    addr: A,
    gateway: OrderGateway,
    sink: ResultSink,
    running: Arc<AtomicBool>,
) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    serve_on(listener, gateway, sink, running)
}

/// Like [`serve`] but takes an already-bound listener (useful when the caller
/// needs the ephemeral port, e.g. binding to `127.0.0.1:0` in tests).
pub fn serve_on(
    listener: TcpListener,
    gateway: OrderGateway,
    sink: ResultSink,
    running: Arc<AtomicBool>,
) -> io::Result<()> {
    serve_with_md(listener, None, gateway, sink, running)
}

/// [`serve_on`] plus an optional market-data side listener: subscribers on it
/// receive a copy of every report frame (see [`MdFanout`]).
pub fn serve_with_md(
    listener: TcpListener,
    md_listener: Option<TcpListener>,
    gateway: OrderGateway,
    sink: ResultSink,
    running: Arc<AtomicBool>,
) -> io::Result<()> {
    let local = listener.local_addr()?;
    eprintln!("[gateway] listening on {local}, awaiting order-system connection…");

    let fanout = md_listener.map(|l| {
        eprintln!("[gateway] market-data fanout on {}", l.local_addr().unwrap());
        MdFanout::accept_on(l, running.clone())
    });

    let (stream, peer) = listener.accept()?;
    stream.set_nodelay(true).ok();
    eprintln!("[gateway] order system connected from {peer}");

    let write_stream = stream.try_clone()?;

    // Async report feedback loop on its own thread (single consumer of results).
    let writer_running = running.clone();
    let writer =
        thread::spawn(move || report_writer(write_stream, sink, fanout, writer_running));

    // Order intake loop on this thread (single producer into the gateway).
    let read_result = read_loop(stream, &gateway, &running);

    // Tell the writer to finish and join it.
    running.store(false, Ordering::Release);
    let _ = writer.join();
    read_result
}

/// Read frames into a reusable buffer and decode each in place.
fn read_loop<S: Read>(
    mut stream: S,
    gateway: &OrderGateway,
    running: &AtomicBool,
) -> io::Result<()> {
    // One buffer, reused for the life of the connection. Sized for a batch of
    // frames; partial trailing frames are compacted to the front.
    let mut buf = vec![0u8; MSG_LEN * 512];
    let mut filled = 0usize;

    while running.load(Ordering::Acquire) {
        let n = stream.read(&mut buf[filled..])?;
        if n == 0 {
            eprintln!("[gateway] peer closed connection");
            break;
        }
        filled += n;

        // Decode every complete frame sitting in the buffer, in place.
        let mut off = 0;
        while filled - off >= MSG_LEN {
            if let Some(view) = WireView::parse(&buf[off..off + MSG_LEN]) {
                if let Some(cmd) = view.to_command() {
                    dispatch(gateway, cmd);
                }
            }
            off += MSG_LEN;
        }

        // Keep any partial trailing frame; this copies at most MSG_LEN-1 bytes.
        if off > 0 {
            buf.copy_within(off..filled, 0);
            filled -= off;
        }
        // Guard against a full buffer with no complete frame (shouldn't happen
        // with fixed frames, but keeps the loop total).
        if filled == buf.len() {
            buf.resize(buf.len() * 2, 0);
        }
    }
    Ok(())
}

/// Push a command into the lock-free gateway, spinning on backpressure.
fn dispatch(gateway: &OrderGateway, cmd: Command) {
    let mut pending = cmd;
    loop {
        match gateway.submit(pending) {
            Ok(()) => return,
            Err(returned) => {
                pending = returned;
                thread::yield_now();
            }
        }
    }
}

/// Drain execution reports, stream them back to the peer, and mirror them to
/// any market-data subscribers.
fn report_writer<S: Write>(
    mut stream: S,
    sink: ResultSink,
    fanout: Option<MdFanout>,
    running: Arc<AtomicBool>,
) {
    let mut out: Vec<u8> = Vec::with_capacity(REPORT_LEN * 256);
    loop {
        out.clear();
        sink.poll(|report| {
            let mut frame = [0u8; REPORT_LEN];
            wire::encode_report(&report, &mut frame);
            out.extend_from_slice(&frame);
        });

        if out.is_empty() {
            if !running.load(Ordering::Acquire) {
                break;
            }
            // Idle: brief sleep instead of a hot spin.
            thread::sleep(Duration::from_micros(100));
            continue;
        }
        if stream.write_all(&out).is_err() {
            break;
        }
        if let Some(f) = &fanout {
            f.broadcast(&out);
        }
    }
    let _ = stream.flush();
}
