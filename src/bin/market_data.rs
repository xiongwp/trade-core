//! The **market-data** service: an independently deployed process that
//!
//! 1. subscribes to trade-core's market-data fanout port (TCP),
//! 2. aggregates trades into K-line candles (1s…1mo) and tracks the live
//!    **depth-of-market ladder** (top-5 bids/asks) per instrument,
//! 3. serves the chart frontend + JSON API over HTTP, **pushes updates over
//!    WebSocket** (poll fallback kept), and
//! 4. **persists candle history** to disk (atomic snapshot every 10 s, loaded
//!    on startup — a restart keeps the chart's history).
//!
//! Usage: market-data [EXCHANGE_MD_ADDR] [HTTP_ADDR] [DATA_DIR]
//!   EXCHANGE_MD_ADDR  default 127.0.0.1:9101
//!   HTTP_ADDR         default 0.0.0.0:8080
//!   DATA_DIR          default ./md-data   ("none" disables persistence)
//!
//! Endpoints:
//!   GET /                   chart UI
//!   GET /admin              operations dashboard
//!   GET /trade              authenticated order-entry terminal
//!   GET /api/admin/overview operational asset summary
//!   GET /api/symbols        [1,2,…]
//!   GET /api/candles?symbol=1&interval=1m&limit=120
//!   GET /api/depth?symbol=1 {"bids":[[p,q],…],"asks":[[p,q],…]}
//!   GET /ws?symbol=1&interval=1s   WebSocket: {"candles":…,"depth":…} pushes

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trade_core::kline::KlineAggregator;
use trade_core::wire::{self, EXECUTION_EVENT_LEN, RT_DEPTH_END, RT_DEPTH_LEVEL};
use trade_core::{log_info, log_warn, InstrumentId, Order, OrderId, Price, Qty, Side};

const CHART_HTML: &str = include_str!("../../assets/kline.html");
const ADMIN_HTML: &str = include_str!("../../assets/admin.html");
const TRADE_HTML: &str = include_str!("../../assets/trade.html");
const RT_TRADE: u8 = 2;

/// Set by the SIGTERM/SIGINT handler so the main accept loop can break out and
/// run the candle-history flush before exiting.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Async-signal-safe handler: touches only an atomic flag, nothing else.
extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// (ts, price, qty, maker_fee, taker_fee), newest last.
type TradeRing = VecDeque<(u64, Price, Qty, u64, u64)>;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Depth {
    bids: Vec<(Price, Qty)>,
    asks: Vec<(Price, Qty)>,
}

#[derive(Default)]
struct DepthStore {
    /// Last complete snapshot (published on DepthEnd). Snapshots are
    /// assembled per feed connection: frames of two replicas' snapshots
    /// must not mix.
    live: HashMap<InstrumentId, Depth>,
}

struct State {
    agg: KlineAggregator,
    depth: DepthStore,
    /// Recent trades per instrument (newest last), bounded ring:
    /// (ts, price, qty, maker_fee, taker_fee).
    trades: HashMap<InstrumentId, TradeRing>,
    /// Per Raft group, the newest (raft index, ordinal) taken. Every replica
    /// of a group sends the same reports in the same order, and a restarted
    /// node replays its log tail: anything at or below the mark was already
    /// counted.
    applied: HashMap<u32, (u64, u32)>,
    /// Newest timestamp given to a trade: candle time never goes backwards
    /// (feed threads race for the lock; wall clocks step back).
    last_ts: u64,
}

impl State {
    /// Whether a report of `group` at (`index`, `ordinal`) is new. Index 0:
    /// not from a Raft entry (a standalone exchange): always taken.
    fn take(&mut self, group: u32, index: u64, ordinal: u32) -> bool {
        if index == 0 {
            return true;
        }
        let mark = self.applied.entry(group).or_insert((0, 0));
        if (index, ordinal) <= *mark {
            return false;
        }
        *mark = (index, ordinal);
        true
    }

    /// Whether a feed that last saw `index` of `group` is caught up with the
    /// group (so its depth is current, not a lagging replica's).
    fn current(&self, group: u32, index: u64) -> bool {
        self.applied.get(&group).is_none_or(|mark| index >= mark.0)
    }
}

const MARKET_STATE_MAGIC: &[u8; 4] = b"TCM1";

/// Candle history and its exact replay watermark must cross the durability
/// boundary together. Otherwise a crash after saving candles but before
/// saving the watermark counts the same execution again when any Raft replica
/// replays its tail.
fn encode_market_state(state: &State) -> Vec<u8> {
    let candles = state.agg.encode();
    let mut applied: Vec<_> = state.applied.iter().collect();
    applied.sort_unstable_by_key(|(group, _)| **group);
    let mut out = Vec::with_capacity(24 + candles.len() + applied.len() * 16);
    out.extend_from_slice(MARKET_STATE_MAGIC);
    out.extend_from_slice(&(candles.len() as u64).to_le_bytes());
    out.extend_from_slice(&candles);
    out.extend_from_slice(&(applied.len() as u32).to_le_bytes());
    for (&group, &(index, ordinal)) in applied {
        out.extend_from_slice(&group.to_le_bytes());
        out.extend_from_slice(&index.to_le_bytes());
        out.extend_from_slice(&ordinal.to_le_bytes());
    }
    out.extend_from_slice(&state.last_ts.to_le_bytes());
    let checksum = trade_core::journal::fnv1a(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

fn decode_market_state(
    bytes: &[u8],
) -> std::io::Result<(KlineAggregator, HashMap<u32, (u64, u32)>, u64)> {
    let bad =
        |message: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string());
    // Backwards-compatible one-time upgrade from candle-only TCK2 snapshots.
    if bytes.starts_with(b"TCK2") {
        return Ok((KlineAggregator::decode(bytes)?, HashMap::new(), 0));
    }
    if bytes.len() < 32 || &bytes[..4] != MARKET_STATE_MAGIC {
        return Err(bad("bad market state magic"));
    }
    let body_end = bytes.len() - 8;
    let stored = u64::from_le_bytes(bytes[body_end..].try_into().unwrap());
    if trade_core::journal::fnv1a(&bytes[..body_end]) != stored {
        return Err(bad("market state checksum mismatch"));
    }
    let candle_len = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as usize;
    let candle_end = 12usize
        .checked_add(candle_len)
        .ok_or_else(|| bad("market state length overflow"))?;
    if candle_end + 4 > body_end {
        return Err(bad("truncated market candle state"));
    }
    let agg = KlineAggregator::decode(&bytes[12..candle_end])?;
    let count = u32::from_le_bytes(bytes[candle_end..candle_end + 4].try_into().unwrap()) as usize;
    let marks_start = candle_end + 4;
    let marks_end = marks_start
        .checked_add(
            count
                .checked_mul(16)
                .ok_or_else(|| bad("market watermark count overflow"))?,
        )
        .ok_or_else(|| bad("market watermark length overflow"))?;
    if marks_end + 8 != body_end {
        return Err(bad("truncated or trailing market watermarks"));
    }
    let mut applied = HashMap::with_capacity(count);
    for record in bytes[marks_start..marks_end].chunks_exact(16) {
        let group = u32::from_le_bytes(record[0..4].try_into().unwrap());
        let index = u64::from_le_bytes(record[4..12].try_into().unwrap());
        let ordinal = u32::from_le_bytes(record[12..16].try_into().unwrap());
        if applied.insert(group, (index, ordinal)).is_some() {
            return Err(bad("duplicate market watermark group"));
        }
    }
    let last_ts = u64::from_le_bytes(bytes[marks_end..body_end].try_into().unwrap());
    Ok((agg, applied, last_ts))
}

fn write_market_state(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    KlineAggregator::write_encoded(path, bytes)
}

fn now_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Feed: subscribe to trade-core's fanout, ingest trades + depth
// ---------------------------------------------------------------------------

fn feed_loop(md_addrs: String, state: Arc<Mutex<State>>) {
    for md_addr in md_addrs
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
    {
        let state = state.clone();
        let md_addr = md_addr.to_string();
        std::thread::spawn(move || feed_one(md_addr, state));
    }
}

/// Request line plus headers, at most.
const MAX_REQUEST_HEAD: u64 = 16 * 1024;
/// Concurrent HTTP/WebSocket connections.
const MAX_CONNECTIONS: usize = 4096;

/// Highest depth level accepted from a frame (the engine publishes 5).
const MAX_DEPTH_LEVELS: usize = 64;

fn idle_feed_read(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn feed_one(md_addr: String, state: Arc<Mutex<State>>) {
    let mut last_connect_warning = std::time::Instant::now() - Duration::from_secs(30);
    let mut last_disconnect_warning = std::time::Instant::now() - Duration::from_secs(30);
    loop {
        let mut sock = match TcpStream::connect(&md_addr) {
            Ok(s) => s,
            Err(e) => {
                if last_connect_warning.elapsed() >= Duration::from_secs(30) {
                    log_warn!(
                        "market-data",
                        "waiting for trade-core fanout at {md_addr} ({e})"
                    );
                    last_connect_warning = std::time::Instant::now();
                }
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        // A quiet market has no frames to publish. Keep a finite read timeout
        // so the thread remains interruptible, but do not mistake an idle
        // socket for a failed feed. EOF or a real I/O error still reconnects.
        sock.set_read_timeout(Some(Duration::from_secs(10))).ok();
        log_info!("market-data", "subscribed to {md_addr}");
        let mut buf = vec![0u8; EXECUTION_EVENT_LEN * 512];
        let mut filled = 0usize;
        // This connection's depth snapshots in progress, and the newest Raft
        // index it delivered (to tell whether its depth is current).
        let mut pending: HashMap<InstrumentId, Depth> = HashMap::new();
        let mut last_index = 0u64;
        loop {
            match sock.read(&mut buf[filled..]) {
                Ok(0) => break,
                Err(error) if idle_feed_read(&error) => {
                    continue;
                }
                Err(_) => break,
                Ok(n) => {
                    filled += n;
                    let mut off = 0;
                    let mut st = state.lock().unwrap();
                    let ts = now_sec().max(st.last_ts);
                    st.last_ts = ts;
                    while filled - off >= EXECUTION_EVENT_LEN {
                        let frame = &buf[off..off + EXECUTION_EVENT_LEN];
                        off += EXECUTION_EVENT_LEN;
                        let Some(event) = wire::decode_execution_event(frame) else {
                            continue;
                        };
                        let r = event.report;
                        if event.raft_index != 0 {
                            last_index = last_index.max(event.raft_index);
                        }
                        match r.type_code {
                            RT_TRADE => {
                                if !st.take(event.raft_group, event.raft_index, event.ordinal) {
                                    continue;
                                }
                                st.agg.on_trade(r.instrument, ts, r.price, r.qty);
                                let dq = st.trades.entry(r.instrument).or_default();
                                dq.push_back((ts, r.price, r.qty, r.maker_fee, r.taker_fee));
                                if dq.len() > 1000 {
                                    dq.pop_front();
                                }
                            }
                            RT_DEPTH_LEVEL => {
                                let lvl = r.aux_id as usize;
                                if lvl >= MAX_DEPTH_LEVELS {
                                    continue; // malformed: never index or allocate by it
                                }
                                let d = pending.entry(r.instrument).or_default();
                                let side = if r.side == trade_core::Side::Buy {
                                    &mut d.bids
                                } else {
                                    &mut d.asks
                                };
                                if side.len() <= lvl {
                                    side.resize(lvl + 1, (0, 0));
                                }
                                side[lvl] = (r.price, r.qty);
                            }
                            RT_DEPTH_END => {
                                if let Some(mut d) = pending.remove(&r.instrument) {
                                    if st.current(event.raft_group, last_index) {
                                        d.bids.truncate(r.aux_id as usize & 0xFF);
                                        d.asks.truncate((r.aux_id as usize >> 8) & 0xFF);
                                        st.depth.live.insert(r.instrument, d);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    drop(st);
                    if off > 0 {
                        buf.copy_within(off..filled, 0);
                        filled -= off;
                    }
                }
            }
        }
        if last_disconnect_warning.elapsed() >= Duration::from_secs(30) {
            log_warn!("market-data", "feed {md_addr} disconnected, retrying");
            last_disconnect_warning = std::time::Instant::now();
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

fn candles_json(st: &State, sym: InstrumentId, interval: &str, limit: usize) -> String {
    let candles = st.agg.candles(sym, interval, limit.min(1000));
    let mut s = String::with_capacity(candles.len() * 48 + 2);
    s.push('[');
    for (i, c) in candles.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "[{},{},{},{},{},{}]",
            c.start, c.open, c.high, c.low, c.close, c.volume
        ));
    }
    s.push(']');
    s
}

/// Read-only operating summary. Commands are deliberately absent until the
/// control plane has authentication, authorization and an immutable audit log.
fn admin_overview_json(st: &State) -> String {
    let instruments = st.agg.instruments();
    let mut rows = Vec::with_capacity(instruments.len());
    let mut total_trades = 0usize;
    let mut total_volume = 0u64;
    for instrument in instruments {
        let trades = st.trades.get(&instrument);
        let trade_count = trades.map_or(0, |q| q.len());
        let volume = trades.map_or(0, |q| q.iter().map(|(_, _, qty, _, _)| *qty).sum());
        let last_trade = trades
            .and_then(|q| q.back().map(|(ts, _, _, _, _)| *ts))
            .unwrap_or(0);
        let depth = st.depth.live.get(&instrument);
        let bid_levels = depth.map_or(0, |d| d.bids.len());
        let ask_levels = depth.map_or(0, |d| d.asks.len());
        total_trades += trade_count;
        total_volume += volume;
        rows.push(format!(
            "{{\"instrument\":{},\"trades\":{trade_count},\"volume\":{volume},\"last_trade\":{last_trade},\"bid_levels\":{bid_levels},\"ask_levels\":{ask_levels}}}",
            instrument.0
        ));
    }
    format!(
        "{{\"assets\":{},\"recent_trades\":{total_trades},\"recent_volume\":{total_volume},\"instruments\":[{}]}}",
        rows.len(),
        rows.join(",")
    )
}

fn depth_json(st: &State, sym: InstrumentId) -> String {
    let d = st.depth.live.get(&sym).cloned().unwrap_or_default();
    let fmt = |v: &[(Price, Qty)]| {
        v.iter()
            .map(|(p, q)| format!("[{p},{q}]"))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{{\"bids\":[{}],\"asks\":[{}]}}",
        fmt(&d.bids),
        fmt(&d.asks)
    )
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

// ---------------------------------------------------------------------------
// WebSocket (RFC 6455, server side, text frames) — dependency-free
// ---------------------------------------------------------------------------

/// Write one unmasked text frame (server -> client).
fn ws_send_text(stream: &mut TcpStream, payload: &str) -> std::io::Result<()> {
    let p = payload.as_bytes();
    let mut hdr: Vec<u8> = vec![0x81]; // FIN + text
    match p.len() {
        0..=125 => hdr.push(p.len() as u8),
        126..=65535 => {
            hdr.push(126);
            hdr.extend_from_slice(&(p.len() as u16).to_be_bytes());
        }
        _ => {
            hdr.push(127);
            hdr.extend_from_slice(&(p.len() as u64).to_be_bytes());
        }
    }
    stream.write_all(&hdr)?;
    stream.write_all(p)
}

/// Consume any pending client frame; `Ok(false)` = client sent close.
fn ws_drain_client(stream: &mut TcpStream) -> std::io::Result<bool> {
    let mut hdr = [0u8; 2];
    match stream.read(&mut hdr[..1]) {
        Err(ref e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            return Ok(true); // nothing pending
        }
        Err(e) => return Err(e),
        Ok(0) => return Ok(false),
        Ok(_) => {}
    }
    stream.read_exact(&mut hdr[1..2])?;
    let opcode = hdr[0] & 0x0F;
    let masked = hdr[1] & 0x80 != 0;
    let mut len = (hdr[1] & 0x7F) as u64;
    if len == 126 {
        let mut b = [0u8; 2];
        stream.read_exact(&mut b)?;
        len = u16::from_be_bytes(b) as u64;
    } else if len == 127 {
        let mut b = [0u8; 8];
        stream.read_exact(&mut b)?;
        len = u64::from_be_bytes(b);
    }
    let skip = len + if masked { 4 } else { 0 };
    std::io::copy(&mut stream.take(skip), &mut std::io::sink())?;
    Ok(opcode != 0x8) // 0x8 = close
}

/// Upgrade and run one WS session: push candles+depth for the subscribed
/// symbol/interval every 250 ms until the client goes away.
fn ws_session(
    mut stream: TcpStream,
    key: &str,
    query: &str,
    state: &Arc<Mutex<State>>,
) -> std::io::Result<()> {
    stream.write_all(trade_core::websocket::handshake_response(key).as_bytes())?;

    let symbol = query_param(query, "symbol")
        .and_then(|v| v.parse().ok())
        .map(InstrumentId)
        .unwrap_or(InstrumentId(1));
    let interval = query_param(query, "interval").unwrap_or("1s").to_string();
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    // A client that stops reading is dropped instead of pinning this thread.
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    let mut last_push: Option<std::time::Instant> = None;
    loop {
        if !ws_drain_client(&mut stream)? {
            return Ok(()); // client closed
        }
        // One push per 250 ms however many frames the client sends: frames
        // used to each trigger a full candles+depth render under the lock.
        if last_push.is_some_and(|at| at.elapsed() < Duration::from_millis(250)) {
            continue;
        }
        last_push = Some(std::time::Instant::now());
        let msg = {
            let st = state.lock().unwrap();
            format!(
                "{{\"candles\":{},\"depth\":{}}}",
                candles_json(&st, symbol, &interval, 120),
                depth_json(&st, symbol)
            )
        };
        ws_send_text(&mut stream, &msg)?;
    }
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

fn respond(stream: &mut TcpStream, status: &str, ctype: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

fn handle_http(
    mut stream: TcpStream,
    state: &Arc<Mutex<State>>,
    admin_token: Option<&str>,
    trading_token: Option<&str>,
    order_api: &OrderApiPool,
) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(10))).ok();
    let Ok(read_half) = stream.try_clone() else {
        return; // out of file descriptors: drop this connection, not the process
    };
    // The request line and headers together are bounded: a client streaming
    // bytes without a newline must not grow a buffer until allocation fails.
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_HEAD);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || !line.ends_with('\n') {
        return;
    }
    let method = line.split_whitespace().next().unwrap_or("GET");
    let path_full = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let (path, query) = path_full
        .split_once('?')
        .unwrap_or((path_full.as_str(), ""));

    // Read headers (needed for the WS key).
    let mut ws_key = None;
    let mut authorization = None;
    let mut hl = String::new();
    while reader.read_line(&mut hl).is_ok() && hl.ends_with('\n') && hl.trim() != "" {
        if let Some((k, v)) = hl.split_once(':') {
            if k.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = Some(v.trim().to_string());
            }
            if k.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_string());
            }
        }
        hl.clear();
    }

    match path {
        "/" | "/index.html" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            CHART_HTML.as_bytes(),
        ),
        "/admin" | "/admin/" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            ADMIN_HTML.as_bytes(),
        ),
        "/trade" | "/trade/" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            TRADE_HTML.as_bytes(),
        ),
        "/ws" => {
            if let Some(key) = ws_key {
                let _ = ws_session(stream, &key, query, state);
            } else {
                respond(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    b"expected websocket",
                );
            }
        }
        "/api/symbols" => {
            let syms = state.lock().unwrap().agg.instruments();
            let body = format!(
                "[{}]",
                syms.iter()
                    .map(|s| s.0.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/admin/overview" => match admin_access(authorization.as_deref(), admin_token) {
            Ok(()) => {
                let body = admin_overview_json(&state.lock().unwrap());
                respond(&mut stream, "200 OK", "application/json", body.as_bytes());
            }
            Err((status, message)) => {
                respond(&mut stream, status, "application/json", message.as_bytes());
            }
        },
        "/api/trade/order" if method == "POST" => {
            match admin_access(authorization.as_deref(), trading_token) {
                Ok(()) => match new_order_from_query(query) {
                    Ok(order) => {
                        let _ = order;
                        match order_api.forward("/orders", query) {
                            Ok(()) => respond(
                                &mut stream,
                                "202 Accepted",
                                "application/json",
                                b"{\"accepted\":true}",
                            ),
                            Err(error) => respond(
                                &mut stream,
                                "503 Service Unavailable",
                                "application/json",
                                format!("{{\"error\":\"{error}\"}}").as_bytes(),
                            ),
                        }
                    }
                    Err(message) => respond(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        message.as_bytes(),
                    ),
                },
                Err((status, message)) => {
                    respond(&mut stream, status, "application/json", message.as_bytes())
                }
            }
        }
        "/api/trade/cancel" if method == "POST" => {
            match admin_access(authorization.as_deref(), trading_token) {
                Ok(()) => match cancel_from_query(query) {
                    Ok((instrument, order_id, cmd_id)) => {
                        let _ = (instrument, order_id, cmd_id);
                        match order_api.forward("/cancels", query) {
                            Ok(()) => respond(
                                &mut stream,
                                "202 Accepted",
                                "application/json",
                                b"{\"accepted\":true}",
                            ),
                            Err(error) => respond(
                                &mut stream,
                                "503 Service Unavailable",
                                "application/json",
                                format!("{{\"error\":\"{error}\"}}").as_bytes(),
                            ),
                        }
                    }
                    Err(message) => respond(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        message.as_bytes(),
                    ),
                },
                Err((status, message)) => {
                    respond(&mut stream, status, "application/json", message.as_bytes())
                }
            }
        }
        "/api/candles" => {
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let interval = query_param(query, "interval").unwrap_or("1m");
            let limit: usize = query_param(query, "limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(120);
            let body = candles_json(&state.lock().unwrap(), symbol, interval, limit);
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/trades" => {
            // Recent executions: [[ts,price,qty,maker_fee,taker_fee],...] oldest first.
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let limit: usize = query_param(query, "limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100);
            let st = state.lock().unwrap();
            let empty = TradeRing::new();
            let dq = st.trades.get(&symbol).unwrap_or(&empty);
            let body = format!(
                "[{}]",
                dq.iter()
                    .skip(dq.len().saturating_sub(limit.min(1000)))
                    .map(|(t, p, q, mf, tf)| format!("[{t},{p},{q},{mf},{tf}]"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/depth" => {
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let body = depth_json(&state.lock().unwrap(), symbol);
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", b"not found"),
    }
}

fn required_query<T: std::str::FromStr>(query: &str, key: &str) -> Result<T, String> {
    query_param(query, key)
        .ok_or_else(|| format!("{{\"error\":\"missing {key}\"}}"))?
        .parse()
        .map_err(|_| format!("{{\"error\":\"invalid {key}\"}}"))
}

fn new_order_from_query(query: &str) -> Result<Order, String> {
    let side = match query_param(query, "side") {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err("{\"error\":\"side must be buy or sell\"}".into()),
    };
    let instrument = InstrumentId(required_query(query, "instrument")?);
    let order_id = OrderId(required_query(query, "order_id")?);
    let price = required_query(query, "price")?;
    let qty = required_query(query, "qty")?;
    let user = required_query(query, "user")?;
    Ok(Order::limit(order_id, side, price, qty)
        .on(instrument)
        .by(user))
}

fn cancel_from_query(query: &str) -> Result<(InstrumentId, OrderId, u64), String> {
    Ok((
        InstrumentId(required_query(query, "instrument")?),
        OrderId(required_query(query, "order_id")?),
        required_query(query, "cmd_id")?,
    ))
}

struct OrderApiPool {
    address: String,
    token: String,
    idle: Mutex<Vec<TcpStream>>,
    max_idle: usize,
}

impl OrderApiPool {
    fn new(address: String, token: String, max_idle: usize) -> Self {
        Self {
            address,
            token,
            idle: Mutex::new(Vec::with_capacity(max_idle)),
            max_idle,
        }
    }

    fn connect(&self) -> std::io::Result<TcpStream> {
        let stream = TcpStream::connect(&self.address)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        Ok(stream)
    }

    fn forward(&self, path: &str, query: &str) -> std::io::Result<()> {
        let request = format!(
            "POST {path}?{query} HTTP/1.1\r\nHost: order-api\r\nAuthorization: Bearer {}\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            self.token
        );
        let mut last_error = None;
        for _ in 0..2 {
            let mut stream = match self.idle.lock().unwrap().pop() {
                Some(stream) => stream,
                None => self.connect()?,
            };
            match send_order_api_request(&mut stream, request.as_bytes()) {
                Ok(accepted) => {
                    let mut idle = self.idle.lock().unwrap();
                    if idle.len() < self.max_idle {
                        idle.push(stream);
                    }
                    return if accepted {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "order API rejected request",
                        ))
                    };
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "order API unavailable")
        }))
    }
}

fn send_order_api_request(stream: &mut TcpStream, request: &[u8]) -> std::io::Result<bool> {
    stream.write_all(request)?;
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader.read_line(&mut status)?;
    if status.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "order API closed persistent connection",
        ));
    }
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid order API content length",
                    )
                })?;
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body)?;
    Ok(status.starts_with("HTTP/1.1 202"))
}

fn admin_access<'a>(
    authorization: Option<&str>,
    configured: Option<&'a str>,
) -> Result<(), (&'a str, String)> {
    let Some(token) = configured.filter(|token| !token.is_empty()) else {
        return Err((
            "503 Service Unavailable",
            "{\"error\":\"admin access is not configured\"}".into(),
        ));
    };
    let expected = format!("Bearer {token}");
    if authorization == Some(expected.as_str()) {
        Ok(())
    } else {
        Err((
            "401 Unauthorized",
            "{\"error\":\"admin authorization required\"}".into(),
        ))
    }
}

fn main() {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("market-data");

    // Install the shutdown handler up front: it only flips an atomic flag, which
    // is async-signal-safe. SIGTERM (orchestrator stop) and SIGINT (Ctrl-C) both
    // trigger a graceful drain + final candle-history flush.
    unsafe {
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
    }

    let mut args = std::env::args().skip(1);
    let md_addr = args.next().unwrap_or_else(|| "127.0.0.1:9101".to_string());
    let http_addr = args.next().unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let data_dir = args.next().unwrap_or_else(|| "./md-data".to_string());
    let admin_token = std::env::var("TC_ADMIN_TOKEN").ok();
    let trading_token = std::env::var("TC_TRADING_TOKEN").ok();
    let order_api =
        std::env::var("TC_ORDER_API_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let order_api_token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let order_api_pool = Arc::new(OrderApiPool::new(
        order_api,
        order_api_token,
        std::env::var("TC_ORDER_API_MAX_IDLE_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(32)
            .clamp(1, 4_096),
    ));

    // Load persisted candle history, if any.
    let persist: Option<PathBuf> = (data_dir != "none").then(|| {
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        PathBuf::from(&data_dir).join("klines.bin")
    });
    let restored = persist
        .as_ref()
        .and_then(
            |p| match std::fs::read(p).and_then(|bytes| decode_market_state(&bytes)) {
                Ok(state) => {
                    log_info!(
                        "market-data",
                        "loaded durable market state from {}",
                        p.display()
                    );
                    Some(state)
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    // Keep the unreadable file for inspection; the next save
                    // would otherwise replace it.
                    let mut aside = p.as_os_str().to_owned();
                    aside.push(format!(".unreadable-{}", now_sec()));
                    let moved = std::fs::rename(p, &aside);
                    log_warn!(
                        "market-data",
                        "event=history_load_failed path={} error={error} moved_aside={}",
                        p.display(),
                        moved.is_ok()
                    );
                    None
                }
            },
        )
        .unwrap_or_else(|| (KlineAggregator::new(2000), HashMap::new(), 0));

    let state = Arc::new(Mutex::new(State {
        agg: restored.0,
        depth: DepthStore::default(),
        trades: HashMap::new(),
        applied: restored.1,
        last_ts: restored.2,
    }));

    // Feed thread.
    let feed_state = state.clone();
    std::thread::Builder::new()
        .name("md-feed".into())
        .spawn(move || feed_loop(md_addr, feed_state))
        .expect("spawn feed");

    // Persistence thread: atomic snapshot every 10 s. Keep the path so the main
    // thread can take one final snapshot on graceful shutdown.
    if let Some(path) = persist.clone() {
        let save_state = state.clone();
        std::thread::Builder::new()
            .name("md-persist".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(10));
                // Encode under the lock, write and fsync after releasing it:
                // the feed threads must not wait on the disk.
                let bytes = encode_market_state(&save_state.lock().unwrap());
                if let Err(error) = write_market_state(&path, &bytes) {
                    log_warn!("market-data", "event=history_save_failed error={error}");
                }
            })
            .expect("spawn persist");
        log_info!("market-data", "persisting candle history every 10s");
    }

    let listener = TcpListener::bind(&http_addr).expect("bind http");
    // Non-blocking accept so the loop has a natural poll point to observe the
    // SHUTDOWN flag set by the signal handler.
    listener
        .set_nonblocking(true)
        .expect("set http listener non-blocking");
    log_info!("market-data", "chart UI + API + WS on http://{http_addr}/");
    let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            log_info!("market-data", "shutdown signal received, draining");
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                // One thread per connection, so bounded: past the limit (or
                // when the OS refuses a thread) the connection is closed.
                if connections.load(Ordering::Acquire) >= MAX_CONNECTIONS {
                    continue;
                }
                stream.set_nonblocking(false).ok();
                let state = state.clone();
                let admin_token = admin_token.clone();
                let trading_token = trading_token.clone();
                let order_api = order_api_pool.clone();
                let done = connections.clone();
                connections.fetch_add(1, Ordering::AcqRel);
                let spawned = std::thread::Builder::new()
                    .name("md-http".into())
                    .spawn(move || {
                        handle_http(
                            stream,
                            &state,
                            admin_token.as_deref(),
                            trading_token.as_deref(),
                            &order_api,
                        );
                        done.fetch_sub(1, Ordering::AcqRel);
                    });
                if spawned.is_err() {
                    connections.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection; nap briefly and re-check SHUTDOWN.
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }

    // Final flush of candle history so a graceful stop keeps the chart's
    // history up to the moment of shutdown (mirrors the periodic snapshot).
    if let Some(path) = persist {
        let st = state.lock().unwrap();
        let _ = write_market_state(&path, &encode_market_state(&st));
        log_info!(
            "market-data",
            "market history and replay watermarks flushed on shutdown"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_feed_timeout_is_not_a_disconnect() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            assert!(idle_feed_read(&std::io::Error::from(kind)));
        }
        assert!(!idle_feed_read(&std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        )));
    }

    #[test]
    fn each_trade_counts_once_across_replicas_and_replays() {
        let mut st = State {
            agg: KlineAggregator::new(10),
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied: HashMap::new(),
            last_ts: 0,
        };
        // Replica A delivers group 3's entries 10..=12; replica B, behind,
        // delivers 10 and 11 again; a restarted node replays 9..=12.
        let a: Vec<bool> = [(10, 0), (10, 1), (11, 0), (12, 0)]
            .iter()
            .map(|&(i, o)| st.take(3, i, o))
            .collect();
        assert_eq!(a, [true, true, true, true]);
        assert!(
            !st.take(3, 10, 1) && !st.take(3, 11, 0),
            "a lagging replica's copies"
        );
        assert!((9..=12).all(|i| !st.take(3, i, 0)), "a replayed tail");
        assert!(
            st.take(3, 13, 0) && st.take(4, 1, 0),
            "new entries, other groups"
        );
        assert!(
            st.take(0, 0, 0) && st.take(0, 0, 0),
            "not from a Raft entry: always"
        );
        // Depth from a feed behind the group is not current.
        assert!(!st.current(3, 12) && st.current(3, 13) && st.current(9, 0));
    }

    #[test]
    fn candle_history_and_replay_watermarks_restart_atomically() {
        let mut state = State {
            agg: KlineAggregator::new(10),
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied: HashMap::from([(3, (42, 7)), (9, (11, 2))]),
            last_ts: 1_700_000_123,
        };
        state
            .agg
            .on_trade(InstrumentId(17), state.last_ts, 1_005, 9);

        let bytes = encode_market_state(&state);
        let (agg, applied, last_ts) = decode_market_state(&bytes).unwrap();
        assert_eq!(applied, state.applied);
        assert_eq!(last_ts, state.last_ts);
        assert_eq!(
            agg.candles(InstrumentId(17), "1s", 10),
            state.agg.candles(InstrumentId(17), "1s", 10)
        );

        let mut restarted = State {
            agg,
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied,
            last_ts,
        };
        assert!(!restarted.take(3, 42, 7), "saved event is not recounted");
        assert!(
            restarted.take(3, 42, 8),
            "the next event remains admissible"
        );
    }

    #[test]
    fn legacy_candle_snapshot_is_read_and_upgraded_without_a_watermark() {
        let mut agg = KlineAggregator::new(10);
        agg.on_trade(InstrumentId(1), 1_700_000_000, 99, 2);
        let (decoded, applied, last_ts) = decode_market_state(&agg.encode()).unwrap();
        assert_eq!(
            decoded.candles(InstrumentId(1), "1s", 10),
            agg.candles(InstrumentId(1), "1s", 10)
        );
        assert!(applied.is_empty());
        assert_eq!(last_ts, 0);
    }

    #[test]
    fn order_api_pool_reuses_keep_alive_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            for _ in 0..2 {
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    assert!(!line.is_empty());
                    if line == "\r\n" {
                        break;
                    }
                }
                reader
                    .get_mut()
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\n{}",
                    )
                    .unwrap();
            }
        });

        let pool = OrderApiPool::new(address.to_string(), "token".into(), 1);
        pool.forward("/orders", "order_id=1").unwrap();
        pool.forward("/orders", "order_id=2").unwrap();
        server.join().unwrap();
        assert_eq!(pool.idle.lock().unwrap().len(), 1);
    }
}
