//! Concurrent load against the real persistent order HTTP API. Every request
//! uses a unique order id and waits for the 202 response after Kafka confirms
//! durable ingress. MySQL projection/direct Raft completion is measured separately.
//!
//! Run: cargo bench --bench order_e2e -- [ADDR TOKEN REQUESTS CONCURRENCY ASSETS]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn percentile(values: &mut [u64], p: f64) -> u64 {
    values.sort_unstable();
    values[((values.len() as f64 * p) as usize).min(values.len() - 1)]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:9200".into());
    let token = args
        .next()
        .unwrap_or_else(|| "local-order-api-token".into());
    let requests = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000u64);
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
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        * 1_000_000;

    let cursor = Arc::new(AtomicU64::new(0));
    let accepted = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(requests as usize)));
    let error_samples = Arc::new(Mutex::new(Vec::<String>::new()));
    let start_barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let addr = addr.clone();
        let token = token.clone();
        let cursor = cursor.clone();
        let accepted = accepted.clone();
        let failed = failed.clone();
        let latencies = latencies.clone();
        let error_samples = error_samples.clone();
        let barrier = start_barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            let mut local_latencies = Vec::new();
            loop {
                let i = cursor.fetch_add(1, Ordering::Relaxed);
                if i >= requests {
                    break;
                }
                let order_id = base + i;
                let instrument = 1 + (i as u32 % assets);
                let user = 100_000 + i % 100_000;
                let side = if i & 1 == 0 { "buy" } else { "sell" };
                let path = format!(
                    "/orders?order_id={order_id}&user={user}&instrument={instrument}&side={side}&price=1000&qty=1"
                );
                let request = format!(
                    "POST {path} HTTP/1.1\r\nHost: order-api\r\nAuthorization: Bearer {token}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let started = Instant::now();
                let result = TcpStream::connect(&addr).and_then(|mut stream| {
                    stream.write_all(request.as_bytes())?;
                    let mut response = String::new();
                    stream.read_to_string(&mut response)?;
                    Ok(response)
                });
                local_latencies.push(started.elapsed().as_nanos() as u64);
                match result {
                    Ok(response) if response.starts_with("HTTP/1.1 202") => {
                        accepted.fetch_add(1, Ordering::Relaxed);
                    }
                    failure => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        let mut samples = error_samples.lock().unwrap();
                        if samples.len() < 10 {
                            samples.push(match failure {
                                Ok(response) => response.replace(['\r', '\n'], " "),
                                Err(error) => format!("I/O error: {error}"),
                            });
                        }
                    }
                }
            }
            latencies.lock().unwrap().extend(local_latencies);
        }));
    }

    let started = Instant::now();
    start_barrier.wait();
    for worker in workers {
        worker.join().expect("load worker");
    }
    let elapsed = started.elapsed();
    let ok = accepted.load(Ordering::Relaxed);
    let errors = failed.load(Ordering::Relaxed);
    let mut latencies = Arc::try_unwrap(latencies).unwrap().into_inner().unwrap();
    let p50 = percentile(&mut latencies, 0.50) as f64 / 1_000_000.0;
    let p95 = percentile(&mut latencies, 0.95) as f64 / 1_000_000.0;
    let p99 = percentile(&mut latencies, 0.99) as f64 / 1_000_000.0;

    println!(
        "persistent order API: {requests} requests, concurrency={concurrency}, assets={assets}"
    );
    println!(
        "accepted={ok}, errors={errors}, elapsed={elapsed:.2?}, throughput={:.0} requests/s",
        ok as f64 / elapsed.as_secs_f64()
    );
    println!("HTTP+Kafka latency: p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms");
    for sample in error_samples.lock().unwrap().iter() {
        println!("error sample: {sample}");
    }
    assert_eq!(errors, 0, "order API returned errors");
}
