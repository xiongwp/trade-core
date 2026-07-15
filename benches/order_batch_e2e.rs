//! Persistent HTTP batch load against the real Kafka order ingress.
//!
//! Run: cargo bench --bench order_batch_e2e --
//!      [ADDR TOKEN ORDERS CONCURRENCY ASSETS BATCH_SIZE]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use trade_core::order::Order;
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire;

const RECORD_LEN: usize = 8 + wire::MSG_LEN;

fn percentile(values: &mut [u64], p: f64) -> u64 {
    values.sort_unstable();
    values[((values.len() as f64 * p) as usize).min(values.len() - 1)]
}

fn read_status(reader: &mut BufReader<TcpStream>) -> std::io::Result<u16> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "server closed persistent connection",
        ));
    }
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut content_length = 0usize;
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            if key.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body)?;
    Ok(status)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:9200".into());
    let token = args
        .next()
        .unwrap_or_else(|| "local-order-api-token".into());
    let orders = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500_000u64);
    let concurrency = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32usize)
        .max(1);
    let assets = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000u32)
        .max(1);
    let batch_size = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500u64)
        .max(1)
        .min((1 << 20) as u64 / RECORD_LEN as u64);
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        * 1_000_000;

    let cursor = Arc::new(AtomicU64::new(0));
    let accepted = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<u64>::new()));
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let addr = addr.clone();
        let token = token.clone();
        let cursor = cursor.clone();
        let accepted = accepted.clone();
        let failed = failed.clone();
        let latencies = latencies.clone();
        let errors = errors.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(&addr).expect("connect persistent order API");
            stream.set_nodelay(true).ok();
            let mut reader = BufReader::new(stream.try_clone().expect("clone socket"));
            let mut body = Vec::with_capacity(batch_size as usize * RECORD_LEN);
            let mut local_latencies = Vec::new();
            barrier.wait();
            loop {
                let first = cursor.fetch_add(batch_size, Ordering::Relaxed);
                if first >= orders {
                    break;
                }
                let count = batch_size.min(orders - first);
                body.clear();
                for offset in 0..count {
                    let i = first + offset;
                    let user = 100_000 + i % 100_000;
                    let instrument = 1 + i as u32 % assets;
                    let side = if i & 1 == 0 { Side::Buy } else { Side::Sell };
                    let order = Order::limit(OrderId(base + i), side, 1_000, 1)
                        .on(InstrumentId(instrument))
                        .by(user);
                    let mut frame = [0u8; wire::MSG_LEN];
                    wire::encode_new(&order, &mut frame);
                    body.extend_from_slice(&user.to_le_bytes());
                    body.extend_from_slice(&frame);
                }
                let header = format!(
                    "POST /commands/batch HTTP/1.1\r\nHost: order-api\r\nAuthorization: Bearer {token}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                    body.len()
                );
                let started = Instant::now();
                let result = stream
                    .write_all(header.as_bytes())
                    .and_then(|()| stream.write_all(&body))
                    .and_then(|()| read_status(&mut reader));
                local_latencies.push(started.elapsed().as_nanos() as u64);
                match result {
                    Ok(202) => {
                        accepted.fetch_add(count, Ordering::Relaxed);
                    }
                    result => {
                        failed.fetch_add(count, Ordering::Relaxed);
                        let mut samples = errors.lock().unwrap();
                        if samples.len() < 10 {
                            samples.push(format!("batch at order {first}: {result:?}"));
                        }
                    }
                }
            }
            latencies.lock().unwrap().extend(local_latencies);
        }));
    }

    let started = Instant::now();
    barrier.wait();
    for worker in workers {
        worker.join().expect("batch load worker");
    }
    let elapsed = started.elapsed();
    let ok = accepted.load(Ordering::Relaxed);
    let failures = failed.load(Ordering::Relaxed);
    let mut latencies = Arc::try_unwrap(latencies).unwrap().into_inner().unwrap();
    assert!(!latencies.is_empty(), "no batches were submitted");
    let p50 = percentile(&mut latencies, 0.50) as f64 / 1_000_000.0;
    let p95 = percentile(&mut latencies, 0.95) as f64 / 1_000_000.0;
    let p99 = percentile(&mut latencies, 0.99) as f64 / 1_000_000.0;

    println!(
        "batch order API: orders={orders}, batches={}, concurrency={concurrency}, assets={assets}, batch_size={batch_size}",
        latencies.len()
    );
    println!(
        "accepted={ok}, errors={failures}, elapsed={elapsed:.2?}, throughput={:.0} orders/s",
        ok as f64 / elapsed.as_secs_f64()
    );
    println!("HTTP+Kafka batch latency: p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms");
    for sample in errors.lock().unwrap().iter() {
        println!("error sample: {sample}");
    }
    assert_eq!(failures, 0, "order API returned batch errors");
}
