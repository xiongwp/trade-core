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

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trade_core::kline::KlineAggregator;
use trade_core::wire::{self, REPORT_LEN, RT_DEPTH_END, RT_DEPTH_LEVEL};
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
    /// Snapshot being assembled from DepthLevel frames (per instrument).
    pending: HashMap<InstrumentId, Depth>,
    /// Last complete snapshot (published on DepthEnd).
    live: HashMap<InstrumentId, Depth>,
}

struct State {
    agg: KlineAggregator,
    depth: DepthStore,
    /// Recent trades per instrument (newest last), bounded ring:
    /// (ts, price, qty, maker_fee, taker_fee).
    trades: HashMap<InstrumentId, TradeRing>,
    seen_reports: ReportDeduper,
}

#[derive(Default)]
struct ReportDeduper {
    set: HashSet<ReportKey>,
    order: VecDeque<ReportKey>,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct ReportKey {
    type_code: u8,
    side: u8,
    instrument: u32,
    order_id: u64,
    aux_id: u64,
    price: u64,
    qty: u64,
    maker_fee: u64,
    taker_fee: u64,
}

impl ReportDeduper {
    fn insert(&mut self, r: wire::DecodedReport) -> bool {
        let key = ReportKey {
            type_code: r.type_code,
            side: if r.side == Side::Buy { 0 } else { 1 },
            instrument: r.instrument.0,
            order_id: r.order_id.0,
            aux_id: r.aux_id,
            price: r.price,
            qty: r.qty,
            maker_fee: r.maker_fee,
            taker_fee: r.taker_fee,
        };
        if !self.set.insert(key) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > 100_000 {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
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

fn feed_one(md_addr: String, state: Arc<Mutex<State>>) {
    loop {
        let mut sock = match TcpStream::connect(&md_addr) {
            Ok(s) => s,
            Err(e) => {
                log_warn!("market-data", "waiting for trade-core fanout at {md_addr} ({e})");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        log_info!("market-data", "subscribed to {md_addr}");
        let mut buf = vec![0u8; REPORT_LEN * 512];
        let mut filled = 0usize;
        loop {
            match sock.read(&mut buf[filled..]) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    filled += n;
                    let mut off = 0;
                    let ts = now_sec();
                    let mut st = state.lock().unwrap();
                    while filled - off >= REPORT_LEN {
                        if let Some(r) = wire::decode_report(&buf[off..off + REPORT_LEN]) {
                            if r.type_code == RT_TRADE && !st.seen_reports.insert(r) {
                                off += REPORT_LEN;
                                continue;
                            }
                            match r.type_code {
                                RT_TRADE => {
                                    st.agg.on_trade(r.instrument, ts, r.price, r.qty);
                                    let dq = st.trades.entry(r.instrument).or_default();
                                    dq.push_back((ts, r.price, r.qty, r.maker_fee, r.taker_fee));
                                    if dq.len() > 1000 {
                                        dq.pop_front();
                                    }
                                }
                                RT_DEPTH_LEVEL => {
                                    let d = st.depth.pending.entry(r.instrument).or_default();
                                    let side = if r.side == trade_core::Side::Buy {
                                        &mut d.bids
                                    } else {
                                        &mut d.asks
                                    };
                                    let lvl = r.aux_id as usize;
                                    if side.len() <= lvl {
                                        side.resize(lvl + 1, (0, 0));
                                    }
                                    side[lvl] = (r.price, r.qty);
                                }
                                RT_DEPTH_END => {
                                    if let Some(mut d) = st.depth.pending.remove(&r.instrument) {
                                        d.bids.truncate(r.aux_id as usize & 0xFF);
                                        d.asks.truncate((r.aux_id as usize >> 8) & 0xFF);
                                        st.depth.live.insert(r.instrument, d);
                                    }
                                }
                                _ => {}
                            }
                        }
                        off += REPORT_LEN;
                    }
                    drop(st);
                    if off > 0 {
                        buf.copy_within(off..filled, 0);
                        filled -= off;
                    }
                }
            }
        }
        log_warn!("market-data", "feed disconnected, retrying…");
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

/// SHA-1 (needed only for the WS handshake accept key — not for security).
fn sha1(data: &[u8]) -> [u8; 20] {
    let (mut h0, mut h1, mut h2, mut h3, mut h4) = (
        0x67452301u32,
        0xEFCDAB89u32,
        0x98BADCFEu32,
        0x10325476u32,
        0xC3D2E1F0u32,
    );
    let ml = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&ml.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, h) in [h0, h1, h2, h3, h4].iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&h.to_be_bytes());
    }
    out
}

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::new();
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        s.push(T[(n >> 18 & 63) as usize] as char);
        s.push(T[(n >> 12 & 63) as usize] as char);
        s.push(if c.len() > 1 {
            T[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        s.push(if c.len() > 2 {
            T[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    s
}

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
    let accept = base64(&sha1(
        format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
    ));
    stream.write_all(
        format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        .as_bytes(),
    )?;

    let symbol = query_param(query, "symbol")
        .and_then(|v| v.parse().ok())
        .map(InstrumentId)
        .unwrap_or(InstrumentId(1));
    let interval = query_param(query, "interval").unwrap_or("1s").to_string();
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;

    loop {
        if !ws_drain_client(&mut stream)? {
            return Ok(()); // client closed
        }
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
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
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
    while reader.read_line(&mut hl).is_ok() && hl.trim() != "" {
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
    let agg = persist
        .as_ref()
        .and_then(|p| match KlineAggregator::load(p) {
            Ok(a) => {
                log_info!("market-data", "loaded candle history from {}", p.display());
                Some(a)
            }
            Err(_) => None,
        })
        .unwrap_or_else(|| KlineAggregator::new(2000));

    let state = Arc::new(Mutex::new(State {
        agg,
        depth: DepthStore::default(),
        trades: HashMap::new(),
        seen_reports: ReportDeduper::default(),
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
                let st = save_state.lock().unwrap();
                let _ = st.agg.save(&path);
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
    loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            log_info!("market-data", "shutdown signal received, draining");
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let state = state.clone();
                let admin_token = admin_token.clone();
                let trading_token = trading_token.clone();
                let order_api = order_api_pool.clone();
                std::thread::spawn(move || {
                    handle_http(
                        stream,
                        &state,
                        admin_token.as_deref(),
                        trading_token.as_deref(),
                        &order_api,
                    )
                });
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
        let _ = st.agg.save(&path);
        log_info!("market-data", "candle history flushed on shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
