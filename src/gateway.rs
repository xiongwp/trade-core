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
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::exchange::{Command, OrderGateway, ResultSink};
use crate::wire::{self, WireView, MSG_LEN, REPORT_LEN};

const COMMITTED_BATCH_MAGIC: [u8; 4] = *b"TCB1";

/// Market-data fanout: subscribers connected on a side port each receive a copy
/// of every execution-report frame (write-only feed — e.g. a candle/K-line
/// service).
///
/// The drain thread that broadcasts is the only consumer of the matching
/// shards' result rings, so it must never wait on a subscriber: a blocking
/// write to one that stopped reading (paused, partitioned, or simply a peer
/// that connected and never reads) filled the rings and parked matching on
/// every shard. Each subscriber therefore gets its own bounded queue and
/// writer thread; one that falls a queue behind is disconnected.
///
/// Frames are execution events ([`wire::EXECUTION_EVENT_LEN`] bytes) naming
/// the Raft group, index and ordinal, so a subscriber reading every replica
/// of a group can keep each report once (index 0: not from a Raft entry,
/// e.g. depth, or a standalone exchange).
#[derive(Clone, Default)]
pub struct MdFanout {
    subs: Arc<Mutex<Vec<MdSubscriber>>>,
    group: u32,
}

/// The group's incarnation as the fanout announces it (0 = not assigned or
/// not known in this process).
fn current_incarnation(metrics: &Option<Arc<crate::metrics::Metrics>>) -> u64 {
    metrics.as_ref().map_or(0, |metrics| {
        metrics.group_incarnation.load(Ordering::Acquire)
    })
}

struct MdSubscriber {
    tx: std::sync::mpsc::SyncSender<Arc<[u8]>>,
    active: Arc<AtomicBool>,
}

fn md_limit(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

/// Whether the subscriber hung up. Non-blocking: market-data never writes on
/// this socket, so a blocking peek would wait out its timeout every time.
fn peer_closed(stream: &std::net::TcpStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return false;
    }
    let mut byte = [0u8; 1];
    let closed = match stream.peek(&mut byte) {
        Ok(0) => true,
        Ok(_) => false,
        Err(error) => error.kind() != std::io::ErrorKind::WouldBlock,
    };
    let _ = stream.set_nonblocking(false);
    closed
}

impl MdFanout {
    /// Accept subscriber connections on `listener` in a background thread.
    ///
    /// Each subscriber first gets a hello frame naming the group and its
    /// incarnation ([`wire::encode_md_hello`]), again whenever the
    /// incarnation changes (before the next chunk), and as the heartbeat of
    /// an idle second: that lets market-data tell a quiet market from a dead
    /// link (no FIN after a host loss or partition), lets this writer notice
    /// such a link through a failing write, and lets market-data recognise a
    /// group rebuilt from empty logs. `metrics` carries the incarnation.
    pub fn accept_on(
        listener: TcpListener,
        running: Arc<AtomicBool>,
        group: u32,
        metrics: Option<Arc<crate::metrics::Metrics>>,
    ) -> MdFanout {
        let fanout = MdFanout {
            group,
            ..MdFanout::default()
        };
        let subs = fanout.subs.clone();
        let max_subscribers = md_limit("TC_MD_MAX_SUBSCRIBERS", 64);
        let queue_capacity = md_limit("TC_MD_SUBSCRIBER_QUEUE", 4096);
        let write_timeout = Duration::from_millis(md_limit("TC_MD_WRITE_TIMEOUT_MS", 5_000) as u64);
        thread::Builder::new()
            .name("md-accept".into())
            .spawn(move || {
                let mut last_refusal_log = std::time::Instant::now() - Duration::from_secs(30);
                for stream in listener.incoming() {
                    if !running.load(Ordering::Acquire) {
                        break;
                    }
                    let Ok(mut stream) = stream else {
                        thread::sleep(Duration::from_millis(50)); // e.g. out of fds
                        continue;
                    };
                    {
                        let mut subscribers = subs.lock().unwrap();
                        subscribers.retain(|subscriber| subscriber.active.load(Ordering::Acquire));
                        if subscribers.len() >= max_subscribers {
                            if last_refusal_log.elapsed() >= Duration::from_secs(30) {
                                crate::log_warn!(
                                    "gateway",
                                    "event=md_subscriber_refused reason=limit active={} limit={max_subscribers}",
                                    subscribers.len()
                                );
                                last_refusal_log = std::time::Instant::now();
                            }
                            continue;
                        }
                    }
                    stream.set_nodelay(true).ok();
                    stream.set_write_timeout(Some(write_timeout)).ok();
                    let (tx, rx) = std::sync::mpsc::sync_channel::<Arc<[u8]>>(queue_capacity);
                    let active = Arc::new(AtomicBool::new(true));
                    let writer_active = active.clone();
                    let writer_metrics = metrics.clone();
                    let spawned = thread::Builder::new().name("md-subscriber".into()).spawn(move || {
                        let mut announced = current_incarnation(&writer_metrics);
                        if stream.write_all(&wire::encode_md_hello(group, announced)).is_err() {
                            writer_active.store(false, Ordering::Release);
                            return;
                        }
                        loop {
                            match rx.recv_timeout(Duration::from_secs(1)) {
                                Ok(chunk) => {
                                    // One atomic load per chunk.
                                    let incarnation = current_incarnation(&writer_metrics);
                                    if incarnation != announced {
                                        announced = incarnation;
                                        if stream
                                            .write_all(&wire::encode_md_hello(group, incarnation))
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    if stream.write_all(&chunk).is_err() {
                                        break;
                                    }
                                }
                                // Only when idle: on the hot path a failed
                                // write already detects a closed peer.
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                    announced = current_incarnation(&writer_metrics);
                                    if peer_closed(&stream)
                                        || stream
                                            .write_all(&wire::encode_md_hello(group, announced))
                                            .is_err()
                                    {
                                        break;
                                    }
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                            }
                        }
                        writer_active.store(false, Ordering::Release);
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                    });
                    if spawned.is_ok() {
                        crate::log_info!("gateway", "market-data subscriber connected");
                        subs.lock().unwrap().push(MdSubscriber { tx, active });
                    }
                }
            })
            .expect("spawn md acceptor");
        fanout
    }

    /// Append one report as a fanout frame to `out`.
    fn encode(
        &self,
        raft_index: Option<u64>,
        ordinal: u32,
        report: &crate::exchange::ExecReport,
        out: &mut Vec<u8>,
    ) {
        let mut frame = [0u8; wire::EXECUTION_EVENT_LEN];
        wire::encode_execution_event(
            self.group,
            raft_index.unwrap_or(0),
            ordinal,
            report,
            &mut frame,
        );
        out.extend_from_slice(&frame);
    }

    /// Queue a batch of fanout frames for every subscriber; never blocks.
    fn broadcast(&self, bytes: &[u8]) {
        let mut subs = self.subs.lock().unwrap();
        if subs.is_empty() {
            return;
        }
        let chunk: Arc<[u8]> = bytes.into();
        subs.retain(|subscriber| {
            if !subscriber.active.load(Ordering::Acquire) {
                return false;
            }
            match subscriber.tx.try_send(chunk.clone()) {
                Ok(()) => true,
                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                    crate::log_warn!("gateway", "event=md_subscriber_dropped reason=slow");
                    subscriber.active.store(false, Ordering::Release);
                    false // dropping the sender ends its writer thread
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => false,
            }
        });
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
        CommittedServerConfig {
            listener,
            md_listener,
            md_group: 0,
            metrics: None,
            sink,
            running,
            max_cmds_per_sec,
        },
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

pub struct CommittedServerConfig {
    pub listener: TcpListener,
    pub md_listener: Option<TcpListener>,
    pub md_group: u32,
    pub metrics: Option<Arc<crate::metrics::Metrics>>,
    pub sink: ResultSink,
    pub running: Arc<AtomicBool>,
    pub max_cmds_per_sec: u64,
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
    config: CommittedServerConfig,
    submit: F,
    report_callback: R,
) -> io::Result<()>
where
    F: Fn(Vec<Command>) -> Option<u64> + Send + Sync + 'static,
    R: Fn(&crate::exchange::ExecutionReportEvent) + Send + Sync + 'static,
{
    let CommittedServerConfig {
        listener,
        md_listener,
        md_group,
        metrics,
        sink,
        running,
        max_cmds_per_sec,
    } = config;
    let fanout = md_listener.map(|l| {
        crate::log_info!(
            "gateway",
            "market-data fanout on {}",
            l.local_addr().unwrap()
        );
        MdFanout::accept_on(l, running.clone(), md_group, metrics)
    });
    let report_running = running.clone();
    let report_callback = Arc::new(report_callback);
    thread::Builder::new()
        .name("md-report-drain".into())
        .spawn(move || report_fanout_drain(sink, fanout, report_running, report_callback))
        .expect("spawn market-data report drain");
    let submit = Arc::new(submit);
    let local = listener.local_addr()?;
    crate::log_info!("gateway", "committed ingress listening on {local}");
    let sessions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let max_sessions = md_limit("TC_COMMITTED_MAX_SESSIONS", 256);
    let session_idle_timeout =
        Duration::from_millis(md_limit("TC_COMMITTED_SESSION_IDLE_TIMEOUT_MS", 300_000) as u64);
    while running.load(Ordering::Acquire) {
        // A failed accept (out of fds, an aborted handshake) is transient:
        // it must not take the matching node down.
        let (stream, peer) = match listener.accept() {
            Ok(accepted) => accepted,
            Err(error) => {
                crate::log_warn!("gateway", "event=accept_failed error={error}");
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        if sessions.load(Ordering::Acquire) >= max_sessions {
            crate::log_warn!(
                "gateway",
                "event=session_refused peer={peer} reason=limit limit={max_sessions}"
            );
            continue;
        }
        stream.set_nodelay(true).ok();
        // A peer that vanished without a FIN (crashed host, partition) would
        // otherwise hold its thread and fd forever; live clients reconnect.
        stream.set_read_timeout(Some(session_idle_timeout)).ok();
        let session_running = running.clone();
        let session_submit = submit.clone();
        let done = sessions.clone();
        sessions.fetch_add(1, Ordering::AcqRel);
        let spawned = thread::Builder::new()
            .name("committed-session".into())
            .spawn(move || {
                crate::log_info!("gateway", "committed client connected from {peer}");
                if let Err(error) = read_loop_ack(
                    stream,
                    session_submit.as_ref(),
                    &session_running,
                    max_cmds_per_sec,
                ) {
                    crate::log_warn!("gateway", "committed client {peer} disconnected: {error}");
                }
                done.fetch_sub(1, Ordering::AcqRel);
            });
        if spawned.is_err() {
            sessions.fetch_sub(1, Ordering::AcqRel);
        }
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
        crate::log_info!(
            "gateway",
            "market-data fanout on {}",
            l.local_addr().unwrap()
        );
        MdFanout::accept_on(l, running.clone(), 0, None)
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
    crate::log_info!(
        "gateway",
        "listening on {local}, awaiting order-system connection…"
    );

    let (stream, peer) = listener.accept()?;
    stream.set_nodelay(true).ok();
    crate::log_info!("gateway", "order system connected from {peer}");

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
            crate::log_info!("gateway", "peer closed connection");
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
    let mut md: Vec<u8> = Vec::new();
    loop {
        out.clear();
        md.clear();
        sink.poll(|report| {
            let mut frame = [0u8; REPORT_LEN];
            wire::encode_report(&report, &mut frame);
            out.extend_from_slice(&frame);
            if let Some(f) = &fanout {
                f.encode(None, 0, &report, &mut md);
            }
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
            f.broadcast(&md);
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
    report_callback: Arc<dyn Fn(&crate::exchange::ExecutionReportEvent) + Send + Sync>,
) -> ResultSink {
    let mut out: Vec<u8> = Vec::with_capacity(REPORT_LEN * 256);
    // Once stopping, keep draining until the shards have gone quiet: they
    // finish their queued batches during shutdown, and with nobody emptying
    // the result rings they would park forever and the join would hang.
    let mut quiet_since: Option<std::time::Instant> = None;
    loop {
        out.clear();
        sink.poll_events(|event| {
            report_callback(&event);
            if let Some(f) = &fanout {
                f.encode(event.raft_index, event.ordinal, &event.report, &mut out);
            }
        });

        if out.is_empty() {
            if !running.load(Ordering::Acquire) {
                let since = *quiet_since.get_or_insert_with(std::time::Instant::now);
                if since.elapsed() >= Duration::from_secs(2) {
                    break;
                }
            }
            thread::sleep(Duration::from_micros(100));
            continue;
        }
        quiet_since = None;
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
    fn quiet_market_data_peer_disconnect_is_detected() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        assert!(!peer_closed(&server));
        drop(client);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !peer_closed(&server) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(peer_closed(&server));
    }

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
