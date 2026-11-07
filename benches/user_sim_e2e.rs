//! End-to-end stress test with a simulated user population, against a running
//! stack (order API → Kafka → Raft matching → execution publish → MySQL).
//!
//! Unlike `order_batch_e2e` (identical 1-lot orders at one price), this models
//! real trading: N users across M instruments with a hot-set skew, limit orders
//! priced around a mid so roughly half cross, post-only quotes, market IOCs and
//! cancels of the user's own recent orders (some already filled → `NotFound`).
//!
//! It measures every stage, not just HTTP accept: while loading it samples the
//! order API's cumulative stage counters, then waits for the pipeline to drain
//! and derives per-stage latency from the cumulative curves (the time for a
//! stage's completed count to reach the submitted count of an earlier instant).
//!
//! Run: cargo bench --bench user_sim_e2e --
//!      [ADDR TOKEN COMMANDS USERS INSTRUMENTS CONCURRENCY BATCH ENGINE_METRICS]
//! ENGINE_METRICS: comma-separated metrics endpoints, one replica per Raft group
//! (default: the local compose stack's node 1 of groups 0..3).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trade_core::order::Order;
use trade_core::types::{InstrumentId, OrderId, Side, TimeInForce};
use trade_core::wire;

const RECORD_LEN: usize = 8 + wire::MSG_LEN;
const MID: u64 = 10_000;
const SPREAD: u64 = 25;
const RECENT: usize = 256;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*: deterministic per worker, no dependencies.
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[derive(Default, Clone, Copy)]
struct Mix {
    limits: u64,
    post_only: u64,
    market: u64,
    cancels: u64,
}

fn read_response(reader: &mut BufReader<TcpStream>) -> std::io::Result<(u16, Vec<u8>)> {
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
    Ok((status, body))
}

/// Scrape a Prometheus text endpoint, summing samples that share a name
/// (labels dropped).
fn scrape(addr: &str) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    let Ok(stream) = TcpStream::connect(addr) else {
        return out;
    };
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let mut stream = stream;
    if stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: m\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return out;
    }
    let mut text = String::new();
    let _ = stream.read_to_string(&mut text);
    for line in text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let mut parts = line.rsplitn(2, ' ');
        let (Some(value), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        let name = name.split('{').next().unwrap_or(name).to_string();
        if let Ok(value) = value.trim().parse::<f64>() {
            *out.entry(name).or_insert(0.0) += value;
        }
    }
    out
}

fn metric(m: &HashMap<String, f64>, name: &str) -> u64 {
    m.get(name).copied().unwrap_or(0.0) as u64
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    at: Duration,
    submitted: u64,
    published: u64,
    matched: u64,
    mysql: u64,
    exec_events: u64,
    match_lag: u64,
    mysql_lag: u64,
    backlog: u64,
}

/// Latency distribution of `stage` behind `submitted`: for each sample time t
/// with s = submitted(t), the first later time the stage reached s.
fn stage_latency(samples: &[Sample], stage: impl Fn(&Sample) -> u64) -> Vec<Duration> {
    let mut out = Vec::new();
    let mut j = 0;
    // Only instants where new commands arrived: after the load ends every
    // sample would otherwise count as a zero-latency point.
    let arrivals = samples
        .windows(2)
        .filter(|w| w[1].submitted > w[0].submitted)
        .map(|w| &w[1]);
    for s in arrivals {
        while j < samples.len() && stage(&samples[j]) < s.submitted {
            j += 1;
        }
        if j == samples.len() {
            break;
        }
        out.push(samples[j].at.saturating_sub(s.at));
    }
    out
}

fn pct(values: &mut [Duration], p: f64) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    values.sort_unstable();
    values[((values.len() as f64 * p) as usize).min(values.len() - 1)]
}

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:9200".into());
    let token = args
        .next()
        .unwrap_or_else(|| "local-order-api-token".into());
    let commands: u64 = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let users: u64 = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
        .max(1);
    let instruments: u32 = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
        .max(1);
    let concurrency: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
        .max(1);
    let batch: u64 = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500u64)
        .clamp(1, (1 << 20) as u64 / RECORD_LEN as u64);
    let engine_metrics: Vec<String> = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:9201,127.0.0.1:9211,127.0.0.1:9221,127.0.0.1:9231".into())
        .split(',')
        .map(str::to_string)
        .collect();
    let metrics_addr = addr.clone();

    // Ids: one globally unique increasing space for orders and cancel cmd_ids.
    let id_base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        * 1_000_000;
    let next_id = Arc::new(AtomicU64::new(1));
    let cursor = Arc::new(AtomicU64::new(0));
    let submitted = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let throttled = Arc::new(AtomicU64::new(0));
    let mix_total = Arc::new(Mutex::new(Mix::default()));
    let http_lat = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let barrier = Arc::new(Barrier::new(concurrency + 1));

    let before = scrape(&metrics_addr);
    let engine_before: Vec<_> = engine_metrics.iter().map(|a| scrape(a)).collect();
    let base_published = metric(&before, "tc_order_published_commands");
    let base_matched = metric(&before, "tc_order_match_completed_commands");
    let base_mysql = metric(&before, "tc_order_mysql_completed_commands");
    let base_exec = metric(&before, "tc_execution_mysql_completed_events");

    let mut workers = Vec::with_capacity(concurrency);
    for worker_id in 0..concurrency {
        let (addr, token) = (addr.clone(), token.clone());
        let (next_id, cursor, submitted, failed, throttled) = (
            next_id.clone(),
            cursor.clone(),
            submitted.clone(),
            failed.clone(),
            throttled.clone(),
        );
        let (mix_total, http_lat, errors, barrier) = (
            mix_total.clone(),
            http_lat.clone(),
            errors.clone(),
            barrier.clone(),
        );
        workers.push(std::thread::spawn(move || {
            let mut rng = Rng(0x9E3779B97F4A7C15 ^ (worker_id as u64 + 1) * 0xD1B54A32D192ED03);
            let mut stream = TcpStream::connect(&addr).expect("connect order API");
            stream.set_nodelay(true).ok();
            let mut reader = BufReader::new(stream.try_clone().expect("clone socket"));
            // Recent orders of this worker: (order id, instrument, user).
            let mut recent: Vec<(u64, u32, u64)> = Vec::with_capacity(RECENT);
            let mut mix = Mix::default();
            let mut lat = Vec::new();
            let mut body = Vec::with_capacity(batch as usize * RECORD_LEN);
            barrier.wait();
            loop {
                let first = cursor.fetch_add(batch, Ordering::Relaxed);
                if first >= commands {
                    break;
                }
                let count = batch.min(commands - first);
                body.clear();
                for _ in 0..count {
                    let id = id_base + next_id.fetch_add(1, Ordering::Relaxed);
                    let roll = rng.below(100);
                    let mut frame = [0u8; wire::MSG_LEN];
                    if roll < 12 && !recent.is_empty() {
                        let (order_id, instrument, user) =
                            recent[rng.below(recent.len() as u64) as usize];
                        wire::encode_cancel(InstrumentId(instrument), OrderId(order_id), id, &mut frame);
                        body.extend_from_slice(&user.to_le_bytes());
                        body.extend_from_slice(&frame);
                        mix.cancels += 1;
                        continue;
                    }
                    let user = 1_000_000 + rng.below(users);
                    // Hot set: half the flow on the first 10% of instruments.
                    let hot = (instruments / 10).max(1);
                    let instrument = 1 + if rng.below(2) == 0 {
                        rng.below(hot as u64) as u32
                    } else {
                        rng.below(instruments as u64) as u32
                    };
                    let side = if rng.below(2) == 0 { Side::Buy } else { Side::Sell };
                    let qty = 1 + rng.below(20);
                    let order = if roll < 15 {
                        mix.market += 1;
                        Order::market(OrderId(id), side, qty).with_tif(TimeInForce::Ioc)
                    } else {
                        let price = MID - SPREAD + rng.below(2 * SPREAD + 1);
                        let o = Order::limit(OrderId(id), side, price, qty);
                        if roll < 20 {
                            mix.post_only += 1;
                            o.with_tif(TimeInForce::PostOnly)
                        } else {
                            mix.limits += 1;
                            o
                        }
                    }
                    .on(InstrumentId(instrument))
                    .by(user);
                    wire::encode_new(&order, &mut frame);
                    body.extend_from_slice(&user.to_le_bytes());
                    body.extend_from_slice(&frame);
                    if recent.len() < RECENT {
                        recent.push((id, instrument, user));
                    } else {
                        recent[rng.below(RECENT as u64) as usize] = (id, instrument, user);
                    }
                }
                let header = format!(
                    "POST /commands/batch HTTP/1.1\r\nHost: order-api\r\nAuthorization: Bearer {token}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                    body.len()
                );
                let started = Instant::now();
                loop {
                    let result = stream
                        .write_all(header.as_bytes())
                        .and_then(|()| stream.write_all(&body))
                        .and_then(|()| read_response(&mut reader));
                    match result {
                        Ok((202, _)) => {
                            submitted.fetch_add(count, Ordering::Relaxed);
                            break;
                        }
                        Ok((429 | 503, _)) => {
                            // Backpressure: the stack is protecting itself; retry.
                            throttled.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        other => {
                            failed.fetch_add(count, Ordering::Relaxed);
                            let mut samples = errors.lock().unwrap();
                            if samples.len() < 10 {
                                let detail = match other {
                                    Ok((status, body)) => {
                                        format!("HTTP {status}: {}", String::from_utf8_lossy(&body))
                                    }
                                    Err(error) => error.to_string(),
                                };
                                samples.push(detail);
                            }
                            break;
                        }
                    }
                }
                lat.push(started.elapsed());
            }
            let mut total = mix_total.lock().unwrap();
            total.limits += mix.limits;
            total.post_only += mix.post_only;
            total.market += mix.market;
            total.cancels += mix.cancels;
            http_lat.lock().unwrap().extend(lat);
        }));
    }

    // Stage sampler: cumulative counters every 100 ms until told to stop.
    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let sampler = {
        let (samples, stop, submitted) = (samples.clone(), stop.clone(), submitted.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let at = started.elapsed();
                let sub = submitted.load(Ordering::Relaxed);
                let m = scrape(&metrics_addr);
                samples.lock().unwrap().push(Sample {
                    at,
                    submitted: sub,
                    published: metric(&m, "tc_order_published_commands")
                        .saturating_sub(base_published),
                    matched: metric(&m, "tc_order_match_completed_commands")
                        .saturating_sub(base_matched),
                    mysql: metric(&m, "tc_order_mysql_completed_commands")
                        .saturating_sub(base_mysql),
                    exec_events: metric(&m, "tc_execution_mysql_completed_events")
                        .saturating_sub(base_exec),
                    match_lag: metric(&m, "tc_order_match_consumer_lag"),
                    mysql_lag: metric(&m, "tc_order_mysql_consumer_lag"),
                    backlog: metric(&m, "tc_order_ingress_backlog"),
                });
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };

    barrier.wait();
    for worker in workers {
        worker.join().expect("load worker");
    }
    let load_elapsed = started.elapsed();
    let total_submitted = submitted.load(Ordering::Relaxed);
    println!(
        "load phase done: {total_submitted} commands accepted in {load_elapsed:.2?} ({:.0} cmd/s), drain...",
        total_submitted as f64 / load_elapsed.as_secs_f64()
    );

    // Drain: every command matched and projected, execution events stable.
    let drain_deadline = Instant::now() + Duration::from_secs(600);
    // (exec events, wall clock, sample time) when the pipeline first caught up.
    let mut stable_since: Option<(u64, Instant, Duration)> = None;
    let drained_at = loop {
        std::thread::sleep(Duration::from_millis(200));
        let last = *samples.lock().unwrap().last().expect("sampler running");
        let caught_up = last.matched >= total_submitted
            && last.mysql >= total_submitted
            && last.match_lag == 0
            && last.mysql_lag == 0;
        if caught_up {
            match stable_since {
                Some((events, since, at)) if events == last.exec_events => {
                    if since.elapsed() >= Duration::from_secs(3) {
                        break at;
                    }
                }
                _ => stable_since = Some((last.exec_events, Instant::now(), last.at)),
            }
        }
        if Instant::now() > drain_deadline {
            println!(
                "DRAIN TIMEOUT: {:?}",
                (last.matched, last.mysql, last.match_lag, last.mysql_lag)
            );
            break last.at;
        }
    };
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let samples = Arc::try_unwrap(samples).unwrap().into_inner().unwrap();
    let last = *samples.last().unwrap();

    let mix = *mix_total.lock().unwrap();
    let new_orders = mix.limits + mix.post_only + mix.market;
    println!();
    println!("== workload ==");
    println!(
        "users={users} instruments={instruments} concurrency={concurrency} batch={batch} commands={commands}"
    );
    println!(
        "new orders={new_orders} (limit {} / post-only {} / market IOC {}), cancels={}",
        mix.limits, mix.post_only, mix.market, mix.cancels
    );
    println!(
        "accepted={total_submitted} failed={} backpressure-retries={}",
        failed.load(Ordering::Relaxed),
        throttled.load(Ordering::Relaxed)
    );

    let mut http = http_lat.lock().unwrap().clone();
    println!();
    println!("== throughput ==");
    println!(
        "HTTP accept   : {:.0} cmd/s over {load_elapsed:.2?}",
        total_submitted as f64 / load_elapsed.as_secs_f64()
    );
    println!(
        "end-to-end    : {:.0} cmd/s (submit start -> fully matched + projected, {drained_at:.2?})",
        total_submitted as f64 / drained_at.as_secs_f64()
    );
    println!(
        "batch HTTP latency (batch={batch}): p50={:.1?} p99={:.1?} max={:.1?}",
        pct(&mut http, 0.50),
        pct(&mut http, 0.99),
        pct(&mut http, 1.0)
    );

    println!();
    println!("== per-stage latency behind client accept (100 ms sampling) ==");
    for (name, mut lat) in [
        ("Kafka publish ", stage_latency(&samples, |s| s.published)),
        ("matched       ", stage_latency(&samples, |s| s.matched)),
        ("order -> MySQL", stage_latency(&samples, |s| s.mysql)),
    ] {
        println!(
            "{name}: p50={:.0?} p90={:.0?} p99={:.0?} max={:.0?}",
            pct(&mut lat, 0.50),
            pct(&mut lat, 0.90),
            pct(&mut lat, 0.99),
            pct(&mut lat, 1.0)
        );
    }
    let peak = |f: fn(&Sample) -> u64| samples.iter().map(f).max().unwrap_or(0);
    println!(
        "peak lag: match={} mysql={} ingress-backlog={}",
        peak(|s| s.match_lag),
        peak(|s| s.mysql_lag),
        peak(|s| s.backlog)
    );
    println!("execution events projected to MySQL: {}", last.exec_events);

    println!();
    println!("== engine (sum over Raft groups, delta) ==");
    let mut deltas: HashMap<&str, u64> = HashMap::new();
    for (endpoint, before) in engine_metrics.iter().zip(&engine_before) {
        let after = scrape(endpoint);
        for name in [
            "tc_orders_accepted",
            "tc_trades",
            "tc_volume",
            "tc_cancels",
            "tc_rejects",
        ] {
            *deltas.entry(name).or_insert(0) +=
                metric(&after, name).saturating_sub(metric(before, name));
        }
    }
    for name in [
        "tc_orders_accepted",
        "tc_trades",
        "tc_volume",
        "tc_cancels",
        "tc_rejects",
    ] {
        println!("{name} = {}", deltas[name]);
    }
    println!(
        "ID_RANGE {} {}",
        id_base + 1,
        id_base + next_id.load(Ordering::Relaxed)
    );

    for sample in errors.lock().unwrap().iter() {
        println!("error sample: {sample}");
    }
    assert_eq!(
        failed.load(Ordering::Relaxed),
        0,
        "order API returned batch errors"
    );
}
