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

const COMMITTED_BATCH_MAGIC: [u8; 4] = *b"TCB1";

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
    serve_rate_limited(listener, md_listener, gateway, sink, running, 0)
}

/// [`serve_with_md`] plus an intake **rate limit** (commands/second; 0 = off):
/// a token bucket in the read loop throttles a misbehaving order system by
/// back-pressuring TCP instead of flooding the matching queues.
pub fn serve_rate_limited(
    listener: TcpListener,
    md_listener: Option<TcpListener>,
    gateway: OrderGateway,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
) -> io::Result<()> {
    serve_rate_limited_with(
        listener,
        md_listener,
        sink,
        running,
        max_cmds_per_sec,
        move |cmd| dispatch(&gateway, cmd),
    )
}

/// Persistent standalone gateway. This is the production-friendly counterpart
/// to [`serve_rate_limited`]: a client disconnect does not terminate the
/// matching process, so HTTP/API gateways may use short-lived TCP forwards.
pub fn serve_forever(
    listener: TcpListener,
    md_listener: Option<TcpListener>,
    gateway: OrderGateway,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
) -> io::Result<()> {
    let gateway = Mutex::new(gateway);
    serve_committed_forever(
        listener,
        md_listener,
        sink,
        running,
        max_cmds_per_sec,
        move |commands| {
            let gateway = gateway.lock().expect("gateway lock");
            for command in commands {
                dispatch(&gateway, command);
            }
            Some(0)
        },
        |_| {},
    )
}

/// Serve the wire protocol while handing decoded commands to a consensus
/// ingress. The callback must return only after it has durably accepted the
/// command (or wait for capacity); the production Raft node uses this to keep
/// uncommitted client commands out of the matching queues.
pub fn serve_committed_rate_limited<F>(
    listener: TcpListener,
    md_listener: Option<TcpListener>,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
    submit: F,
) -> io::Result<()>
where
    F: Fn(Command),
{
    serve_rate_limited_with(
        listener,
        md_listener,
        sink,
        running,
        max_cmds_per_sec,
        submit,
    )
}

/// Persistent committed ingress for a long-running matching member. A client
/// disconnect ends only that TCP session; Raft, journals and the next session
/// continue with the already committed state.
pub fn serve_committed_forever<F, R>(
    listener: TcpListener,
    md_listener: Option<TcpListener>,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
    submit: F,
    report_callback: R,
) -> io::Result<()>
where
    F: Fn(Vec<Command>) -> Option<u64> + Send + Sync + 'static,
    R: Fn(&crate::exchange::ExecReport) + Send + Sync + 'static,
{
    let fanout = md_listener.map(|l| {
        eprintln!(
            "[gateway] market-data fanout on {}",
            l.local_addr().unwrap()
        );
        MdFanout::accept_on(l, running.clone())
    });
    let report_running = running.clone();
    let report_callback = Arc::new(report_callback);
    thread::Builder::new()
        .name("md-report-drain".into())
        .spawn(move || report_fanout_drain(sink, fanout, report_running, report_callback))
        .expect("spawn market-data report drain");
    let submit = Arc::new(submit);
    let local = listener.local_addr()?;
    eprintln!("[gateway] committed ingress listening on {local}");
    while running.load(Ordering::Acquire) {
        let (stream, peer) = listener.accept()?;
        stream.set_nodelay(true).ok();
        let session_running = running.clone();
        let session_submit = submit.clone();
        thread::spawn(move || {
            eprintln!("[gateway] committed client connected from {peer}");
            if let Err(error) = read_loop_ack(
                stream,
                session_submit.as_ref(),
                &session_running,
                max_cmds_per_sec,
            ) {
                eprintln!("[gateway] committed client {peer} disconnected: {error}");
            }
        });
    }
    Ok(())
}

fn serve_rate_limited_with<F>(
    listener: TcpListener,
    md_listener: Option<TcpListener>,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
    submit: F,
) -> io::Result<()>
where
    F: Fn(Command),
{
    serve_one(
        &listener,
        md_listener.as_ref(),
        sink,
        running,
        max_cmds_per_sec,
        &submit,
    )
    .map(|_| ())
}

fn serve_one<F>(
    listener: &TcpListener,
    md_listener: Option<&TcpListener>,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
    submit: &F,
) -> io::Result<ResultSink>
where
    F: Fn(Command),
{
    let fanout = md_listener.map(|l| {
        let l = l.try_clone().expect("clone market-data listener");
        eprintln!(
            "[gateway] market-data fanout on {}",
            l.local_addr().unwrap()
        );
        MdFanout::accept_on(l, running.clone())
    });
    serve_one_with_fanout(listener, fanout, sink, running, max_cmds_per_sec, submit)
}

fn serve_one_with_fanout<F>(
    listener: &TcpListener,
    fanout: Option<MdFanout>,
    sink: ResultSink,
    running: Arc<AtomicBool>,
    max_cmds_per_sec: u64,
    submit: &F,
) -> io::Result<ResultSink>
where
    F: Fn(Command),
{
    let local = listener.local_addr()?;
    eprintln!("[gateway] listening on {local}, awaiting order-system connection…");

    let (stream, peer) = listener.accept()?;
    stream.set_nodelay(true).ok();
    eprintln!("[gateway] order system connected from {peer}");

    let write_stream = stream.try_clone()?;

    // Async report feedback loop on its own thread (single consumer of results).
    let writer_running = running.clone();
    let writer = thread::spawn(move || report_writer(write_stream, sink, fanout, writer_running));

    // Order intake loop on this thread (single producer into the gateway).
    let read_result = read_loop(stream, submit, &running, max_cmds_per_sec);

    // Tell the writer to finish and join it.
    running.store(false, Ordering::Release);
    let sink = writer
        .join()
        .unwrap_or_else(|_| panic!("report writer panicked"));
    read_result.map(|()| sink)
}

/// Read frames into a reusable buffer and decode each in place.
fn read_loop<S: Read, F: Fn(Command)>(
    mut stream: S,
    submit: &F,
    running: &AtomicBool,
    max_cmds_per_sec: u64,
) -> io::Result<()> {
    // Token bucket: refill each second; sleep (TCP backpressure) when drained.
    let mut window = std::time::Instant::now();
    let mut budget = max_cmds_per_sec;
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
            if max_cmds_per_sec > 0 {
                if budget == 0 {
                    let elapsed = window.elapsed();
                    if elapsed < Duration::from_secs(1) {
                        thread::sleep(Duration::from_secs(1) - elapsed);
                    }
                    window = std::time::Instant::now();
                    budget = max_cmds_per_sec;
                }
                budget -= 1;
            }
            if let Some(view) = WireView::parse(&buf[off..off + MSG_LEN]) {
                if let Some(cmd) = view.to_command() {
                    submit(cmd);
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

/// Read fixed-size command frames and acknowledge each decoded command after it
/// has been handed to the committed ingress. Execution reports are drained by a
/// process-wide background fanout in the Raft runtime, so short-lived clients
/// cannot starve market-data consumers.
fn read_loop_ack<S: Read + Write, F: Fn(Vec<Command>) -> Option<u64>>(
    mut stream: S,
    submit: &F,
    running: &AtomicBool,
    max_cmds_per_sec: u64,
) -> io::Result<()> {
    while running.load(Ordering::Acquire) {
        let mut prefix = [0u8; 4];
        match stream.read_exact(&mut prefix) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let frames = if prefix == COMMITTED_BATCH_MAGIC {
            let mut count = [0u8; 4];
            stream.read_exact(&mut count)?;
            let count = u32::from_be_bytes(count) as usize;
            if count == 0 || count > wire::RAFT_BATCH_MAX_COMMANDS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid committed command batch size",
                ));
            }
            let mut frames = vec![0u8; count * MSG_LEN];
            stream.read_exact(&mut frames)?;
            frames
        } else {
            let mut frame = vec![0u8; MSG_LEN];
            frame[..4].copy_from_slice(&prefix);
            stream.read_exact(&mut frame[4..])?;
            frame
        };
        let command_count = frames.len() / MSG_LEN;
        if max_cmds_per_sec > 0 && command_count as u64 > max_cmds_per_sec {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "committed batch exceeds rate limit",
            ));
        }
        let commands = frames
            .chunks_exact(MSG_LEN)
            .map(|frame| {
                WireView::parse(frame)
                    .and_then(|view| view.to_command())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid command"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let Some(index) = submit(commands) else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "command batch was not committed by this leader",
            ));
        };
        let mut ack = [0u8; 9];
        ack[0] = 1;
        ack[1..].copy_from_slice(&index.to_be_bytes());
        stream.write_all(&ack)?;
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
) -> ResultSink {
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
        if let Some(f) = &fanout {
            f.broadcast(&out);
        }
        if stream.write_all(&out).is_err() {
            break;
        }
    }
    let _ = stream.flush();
    sink
}

fn report_fanout_drain(
    sink: ResultSink,
    fanout: Option<MdFanout>,
    running: Arc<AtomicBool>,
    report_callback: Arc<dyn Fn(&crate::exchange::ExecReport) + Send + Sync>,
) -> ResultSink {
    let mut out: Vec<u8> = Vec::with_capacity(REPORT_LEN * 256);
    loop {
        out.clear();
        sink.poll(|report| {
            report_callback(&report);
            let mut frame = [0u8; REPORT_LEN];
            wire::encode_report(&report, &mut frame);
            out.extend_from_slice(&frame);
        });

        if out.is_empty() {
            if !running.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_micros(100));
            continue;
        }
        if let Some(f) = &fanout {
            f.broadcast(&out);
        }
    }
    sink
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Order;
    use crate::types::{InstrumentId, OrderId, Side};
    use std::io::Cursor;

    #[test]
    fn committed_batch_protocol_acks_one_index_for_many_commands() {
        let orders = [
            Order::limit(OrderId(1), Side::Sell, 100, 2).on(InstrumentId(9)),
            Order::limit(OrderId(2), Side::Buy, 100, 2).on(InstrumentId(9)),
        ];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&COMMITTED_BATCH_MAGIC);
        bytes.extend_from_slice(&(orders.len() as u32).to_be_bytes());
        for order in orders {
            let mut frame = [0u8; MSG_LEN];
            wire::encode_new(&order, &mut frame);
            bytes.extend_from_slice(&frame);
        }
        let input_len = bytes.len();
        let mut stream = Cursor::new(bytes);
        let observed = Mutex::new(Vec::new());

        read_loop_ack(
            &mut stream,
            &|commands| {
                observed
                    .lock()
                    .unwrap()
                    .extend(commands.into_iter().map(|command| command.id()));
                Some(77)
            },
            &AtomicBool::new(true),
            0,
        )
        .unwrap();

        assert_eq!(*observed.lock().unwrap(), vec![1, 2]);
        let bytes = stream.into_inner();
        assert_eq!(bytes[input_len], 1);
        assert_eq!(
            u64::from_be_bytes(bytes[input_len + 1..].try_into().unwrap()),
            77
        );
    }
}
