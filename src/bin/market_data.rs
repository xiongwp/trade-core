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
//!   GET /api/symbols        [1,2,…]
//!   GET /api/candles?symbol=1&interval=1m&limit=120
//!   GET /api/depth?symbol=1 {"bids":[[p,q],…],"asks":[[p,q],…]}
//!   GET /ws?symbol=1&interval=1s   WebSocket: {"candles":…,"depth":…} pushes

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trade_core::kline::KlineAggregator;
use trade_core::wire::{self, REPORT_LEN, RT_DEPTH_END, RT_DEPTH_LEVEL};
use trade_core::{InstrumentId, Price, Qty};

const CHART_HTML: &str = include_str!("../../assets/kline.html");
const RT_TRADE: u8 = 2;

/// (ts, price, qty, maker_fee, taker_fee), newest last.
type TradeRing = std::collections::VecDeque<(u64, Price, Qty, u64, u64)>;

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
}

fn now_sec() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Feed: subscribe to trade-core's fanout, ingest trades + depth
// ---------------------------------------------------------------------------

fn feed_loop(md_addr: String, state: Arc<Mutex<State>>) {
    loop {
        let mut sock = match TcpStream::connect(&md_addr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[md] waiting for trade-core fanout at {md_addr} ({e})");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        eprintln!("[md] subscribed to {md_addr}");
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
        eprintln!("[md] feed disconnected, retrying…");
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

fn depth_json(st: &State, sym: InstrumentId) -> String {
    let d = st.depth.live.get(&sym).cloned().unwrap_or_default();
    let fmt = |v: &[(Price, Qty)]| {
        v.iter().map(|(p, q)| format!("[{p},{q}]")).collect::<Vec<_>>().join(",")
    };
    format!("{{\"bids\":[{}],\"asks\":[{}]}}", fmt(&d.bids), fmt(&d.asks))
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
    let (mut h0, mut h1, mut h2, mut h3, mut h4) =
        (0x67452301u32, 0xEFCDAB89u32, 0x98BADCFEu32, 0x10325476u32, 0xC3D2E1F0u32);
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
        s.push(if c.len() > 1 { T[(n >> 6 & 63) as usize] as char } else { '=' });
        s.push(if c.len() > 2 { T[(n & 63) as usize] as char } else { '=' });
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
            return Ok(true) // nothing pending
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

fn handle_http(mut stream: TcpStream, state: &Arc<Mutex<State>>) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let path_full = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let (path, query) = path_full.split_once('?').unwrap_or((path_full.as_str(), ""));

    // Read headers (needed for the WS key).
    let mut ws_key = None;
    let mut hl = String::new();
    while reader.read_line(&mut hl).is_ok() && hl.trim() != "" {
        if let Some((k, v)) = hl.split_once(':') {
            if k.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = Some(v.trim().to_string());
            }
        }
        hl.clear();
    }

    match path {
        "/" | "/index.html" => {
            respond(&mut stream, "200 OK", "text/html; charset=utf-8", CHART_HTML.as_bytes())
        }
        "/ws" => {
            if let Some(key) = ws_key {
                let _ = ws_session(stream, &key, query, state);
            } else {
                respond(&mut stream, "400 Bad Request", "text/plain", b"expected websocket");
            }
        }
        "/api/symbols" => {
            let syms = state.lock().unwrap().agg.instruments();
            let body = format!(
                "[{}]",
                syms.iter().map(|s| s.0.to_string()).collect::<Vec<_>>().join(",")
            );
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/candles" => {
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let interval = query_param(query, "interval").unwrap_or("1m");
            let limit: usize =
                query_param(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(120);
            let body = candles_json(&state.lock().unwrap(), symbol, interval, limit);
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/trades" => {
            // Recent executions: [[ts,price,qty,maker_fee,taker_fee],...] oldest first.
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let limit: usize =
                query_param(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(100);
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

fn main() {
    let mut args = std::env::args().skip(1);
    let md_addr = args.next().unwrap_or_else(|| "127.0.0.1:9101".to_string());
    let http_addr = args.next().unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let data_dir = args.next().unwrap_or_else(|| "./md-data".to_string());

    // Load persisted candle history, if any.
    let persist: Option<PathBuf> = (data_dir != "none").then(|| {
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        PathBuf::from(&data_dir).join("klines.bin")
    });
    let agg = persist
        .as_ref()
        .and_then(|p| match KlineAggregator::load(p) {
            Ok(a) => {
                eprintln!("[md] loaded candle history from {}", p.display());
                Some(a)
            }
            Err(_) => None,
        })
        .unwrap_or_else(|| KlineAggregator::new(2000));

    let state = Arc::new(Mutex::new(State {
        agg,
        depth: DepthStore::default(),
        trades: HashMap::new(),
    }));

    // Feed thread.
    let feed_state = state.clone();
    std::thread::Builder::new()
        .name("md-feed".into())
        .spawn(move || feed_loop(md_addr, feed_state))
        .expect("spawn feed");

    // Persistence thread: atomic snapshot every 10 s.
    if let Some(path) = persist {
        let save_state = state.clone();
        std::thread::Builder::new()
            .name("md-persist".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(10));
                let st = save_state.lock().unwrap();
                let _ = st.agg.save(&path);
            })
            .expect("spawn persist");
        eprintln!("[md] persisting candle history every 10s");
    }

    let listener = TcpListener::bind(&http_addr).expect("bind http");
    eprintln!("[md] chart UI + API + WS on http://{http_addr}/");
    for stream in listener.incoming().flatten() {
        let state = state.clone();
        std::thread::spawn(move || handle_http(stream, &state));
    }
}
