//! The market-data service: an independently deployed process that
//!
//! 1. subscribes to the matching node's market-data fanout port (TCP),
//! 2. aggregates every trade print into K-line candles at the standard stock
//!    intervals (1s, 1m, 3m, 5m, 10m, 15m, 30m, 1d, 1w, 1mo), and
//! 3. serves a candlestick-chart frontend plus a JSON API over HTTP.
//!
//! Usage: market_data [EXCHANGE_MD_ADDR] [HTTP_ADDR]
//!   EXCHANGE_MD_ADDR  default 127.0.0.1:9101
//!   HTTP_ADDR         default 0.0.0.0:8080
//!
//! Endpoints:
//!   GET /                  the chart UI
//!   GET /api/symbols       instruments that have traded, e.g. [1,2,3]
//!   GET /api/candles?symbol=1&interval=1m&limit=120
//!                          [[start_sec,o,h,l,c,volume], ...] oldest first
//!
//! Candle timestamps are stamped at feed arrival (wall clock). For historical
//! rebuilds from a journal, use the journal's own nanosecond timestamps.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trade_core::kline::KlineAggregator;
use trade_core::wire::{self, REPORT_LEN};
use trade_core::InstrumentId;

const CHART_HTML: &str = include_str!("../../assets/kline.html");
const RT_TRADE: u8 = 2;

fn now_sec() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Subscribe to the exchange fanout and feed trades into the aggregator.
/// Reconnects forever so the chart survives exchange restarts.
fn feed_loop(md_addr: String, agg: Arc<Mutex<KlineAggregator>>) {
    loop {
        let mut sock = match TcpStream::connect(&md_addr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[md] waiting for exchange fanout at {md_addr} ({e})");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        eprintln!("[md] subscribed to {md_addr}");
        let mut buf = vec![0u8; REPORT_LEN * 512];
        let mut filled = 0usize;
        loop {
            match sock.read(&mut buf[filled..]) {
                Ok(0) | Err(_) => break, // reconnect
                Ok(n) => {
                    filled += n;
                    let mut off = 0;
                    let ts = now_sec();
                    let mut agg = agg.lock().unwrap();
                    while filled - off >= REPORT_LEN {
                        if let Some(r) = wire::decode_report(&buf[off..off + REPORT_LEN]) {
                            if r.type_code == RT_TRADE {
                                agg.on_trade(r.instrument, ts, r.price, r.qty);
                            }
                        }
                        off += REPORT_LEN;
                    }
                    drop(agg);
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

/// Pull `key=value` out of a query string (no percent-decoding needed here).
fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

fn respond(stream: &mut TcpStream, status: &str, ctype: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

fn handle_http(mut stream: TcpStream, agg: &Arc<Mutex<KlineAggregator>>) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    // "GET /path?query HTTP/1.1"
    let path_full = line.split_whitespace().nth(1).unwrap_or("/");
    let (path, query) = path_full.split_once('?').unwrap_or((path_full, ""));

    match path {
        "/" | "/index.html" => {
            respond(&mut stream, "200 OK", "text/html; charset=utf-8", CHART_HTML.as_bytes())
        }
        "/api/symbols" => {
            let syms = agg.lock().unwrap().instruments();
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
            let candles = agg.lock().unwrap().candles(symbol, interval, limit.min(1000));
            let mut body = String::with_capacity(candles.len() * 48 + 2);
            body.push('[');
            for (i, c) in candles.iter().enumerate() {
                if i > 0 {
                    body.push(',');
                }
                body.push_str(&format!(
                    "[{},{},{},{},{},{}]",
                    c.start, c.open, c.high, c.low, c.close, c.volume
                ));
            }
            body.push(']');
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", b"not found"),
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let md_addr = args.next().unwrap_or_else(|| "127.0.0.1:9101".to_string());
    let http_addr = args.next().unwrap_or_else(|| "0.0.0.0:8080".to_string());

    // Keep ~2000 candles per (instrument, interval): > a day of 1m bars.
    let agg = Arc::new(Mutex::new(KlineAggregator::new(2000)));

    let feed_agg = agg.clone();
    std::thread::Builder::new()
        .name("md-feed".into())
        .spawn(move || feed_loop(md_addr, feed_agg))
        .expect("spawn feed");

    let listener = TcpListener::bind(&http_addr).expect("bind http");
    eprintln!("[md] chart UI + API on http://{http_addr}/");
    for stream in listener.incoming().flatten() {
        let agg = agg.clone();
        std::thread::spawn(move || handle_http(stream, &agg));
    }
}
