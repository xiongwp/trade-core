//! Crash-test client and end-to-end consistency checker.
//!
//! Submits orders like a real client: a batch that fails for any reason,
//! even a dropped connection while order-api is being killed, is resent
//! with the SAME order ids until it is accepted. So every redelivery and
//! retry path is exercised. Run alongside scripts/bench/chaos.sh, which
//! kills nodes mid-run.
//!
//! Then it reads the whole execution topic and checks:
//! - lost: every accepted order has a result;
//! - double: no order was applied at two different log positions;
//! - overfill: no order traded more than its quantity;
//! - replicas: once the stack is idle, every replica of a group reports
//!   the same snapshot index and per-category state fingerprint.
//!
//!   cargo bench --bench chaos_e2e -- 127.0.0.1:9200 TOKEN 300000 [users] [instruments] [concurrency] [batch]

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use trade_core::types::{InstrumentId, OrderId, Side, TimeInForce};
use trade_core::{wire, Order};

const MID: u64 = 10_000;
const SPREAD: u64 = 20;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn read_status(reader: &mut BufReader<TcpStream>) -> std::io::Result<u16> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(std::io::Error::other("connection closed"));
    }
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| std::io::Error::other(format!("bad status line {line:?}")))?;
    let mut length = 0usize;
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        let header = line.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((key, value)) = header.split_once(':') {
            if key.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    Ok(status)
}

fn connect(addr: &str) -> (TcpStream, BufReader<TcpStream>) {
    loop {
        if let Ok(stream) = TcpStream::connect(addr) {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
            let reader = BufReader::new(stream.try_clone().expect("clone socket"));
            return (stream, reader);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn http_get(addr: &str, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(addr).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    write!(stream, "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").ok()?;
    let mut text = String::new();
    stream.read_to_string(&mut text).ok()?;
    text.split_once("\r\n\r\n").map(|(_, body)| body.to_string())
}

fn json_u64(body: &str, key: &str) -> Option<u64> {
    let start = body.find(&format!("\"{key}\":"))? + key.len() + 3;
    body[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    let addr = args.first().cloned().unwrap_or_else(|| "127.0.0.1:9200".into());
    let token = args.get(1).cloned().unwrap_or_else(|| "local-order-api-token".into());
    let commands: u64 = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(300_000);
    let users: u64 = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(10_000);
    let instruments: u32 = args.get(4).and_then(|v| v.parse().ok()).unwrap_or(100);
    let concurrency: usize = args.get(5).and_then(|v| v.parse().ok()).unwrap_or(16);
    let batch: u64 = args.get(6).and_then(|v| v.parse().ok()).unwrap_or(500);
    let groups: u32 = std::env::var("TC_RAFT_GROUPS").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let brokers = std::env::var("TC_CHAOS_BROKERS")
        .unwrap_or_else(|_| "localhost:19092,localhost:19093,localhost:19094".into());
    let stride = 1_000u32; // one instrument per asset category
    // Unique per run so ids never collide with an earlier run's.
    let id_base = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        % 100_000)
        * 1_000_000_000;

    // ---- submit -------------------------------------------------------
    let cursor = Arc::new(AtomicU64::new(0));
    let resent = Arc::new(AtomicU64::new(0));
    let orders = Arc::new(Mutex::new(HashMap::<u64, u64>::new())); // accepted New id -> qty
    let started = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..concurrency {
        let (addr, token) = (addr.clone(), token.clone());
        let (cursor, resent, orders) = (cursor.clone(), resent.clone(), orders.clone());
        workers.push(std::thread::spawn(move || {
            let mut rng = Rng(0x9e37_79b9 ^ (worker as u64 + 1) * 0x1234_5678_9abc);
            let (mut stream, mut reader) = connect(&addr);
            let mut recent: Vec<(u64, u32, u64)> = Vec::new();
            let mut body = Vec::new();
            loop {
                let first = cursor.fetch_add(batch, Ordering::Relaxed);
                if first >= commands {
                    break;
                }
                let count = batch.min(commands - first);
                body.clear();
                let mut news = Vec::new();
                for k in 0..count {
                    let id = id_base + first + k + 1;
                    let mut frame = [0u8; wire::MSG_LEN];
                    let roll = rng.below(100);
                    if roll < 10 && !recent.is_empty() {
                        let (order, instrument, user) = recent[rng.below(recent.len() as u64) as usize];
                        wire::encode_cancel(InstrumentId(instrument), OrderId(order), id, &mut frame);
                        body.extend_from_slice(&user.to_le_bytes());
                        body.extend_from_slice(&frame);
                        continue;
                    }
                    let user = 1_000_000 + rng.below(users);
                    let instrument = 1 + stride * rng.below(instruments as u64) as u32;
                    let side = if rng.below(2) == 0 { Side::Buy } else { Side::Sell };
                    let qty = 1 + rng.below(20);
                    let order = if roll < 15 {
                        Order::market(OrderId(id), side, qty).with_tif(TimeInForce::Ioc)
                    } else {
                        Order::limit(OrderId(id), side, MID - SPREAD + rng.below(2 * SPREAD + 1), qty)
                    }
                    .on(InstrumentId(instrument))
                    .by(user);
                    wire::encode_new(&order, &mut frame);
                    body.extend_from_slice(&user.to_le_bytes());
                    body.extend_from_slice(&frame);
                    news.push((id, qty));
                    if recent.len() < 256 {
                        recent.push((id, instrument, user));
                    } else {
                        recent[rng.below(256) as usize] = (id, instrument, user);
                    }
                }
                let header = format!(
                    "POST /commands/batch HTTP/1.1\r\nHost: order-api\r\nAuthorization: Bearer {token}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                    body.len()
                );
                let mut attempts = 0u32;
                loop {
                    let status = stream
                        .write_all(header.as_bytes())
                        .and_then(|()| stream.write_all(&body))
                        .and_then(|()| read_status(&mut reader));
                    match status {
                        Ok(202) => break,
                        Ok(_) | Err(_) => {
                            // Unknown outcome (rejected, overloaded, or the
                            // server died mid-request): resend the very same
                            // batch, same ids, on a fresh connection.
                            attempts += 1;
                            resent.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Duration::from_millis((50 * attempts as u64).min(1_000)));
                            (stream, reader) = connect(&addr);
                        }
                    }
                }
                orders.lock().unwrap().extend(news);
            }
        }));
    }
    for worker in workers {
        worker.join().expect("worker");
    }
    let orders = Arc::try_unwrap(orders).unwrap().into_inner().unwrap();
    println!(
        "submitted {commands} commands ({} new orders) in {:.1}s, {} batch resends",
        orders.len(),
        started.elapsed().as_secs_f64(),
        resent.load(Ordering::Relaxed)
    );

    // ---- read the execution topic ---------------------------------------
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("chaos-check-{}", std::process::id()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("kafka consumer");
    consumer.subscribe(&["trade-executions-v1"]).expect("subscribe");
    let mut seen_keys = HashSet::<(u32, u64, u32, u64)>::new();
    let mut has_result = HashSet::<u64>::new();
    let mut accepted_at = HashMap::<u64, HashSet<(u32, u64)>>::new();
    let mut traded = HashMap::<u64, u64>::new();
    let mut events = 0u64;
    let mut republished = 0u64;
    let deadline = Instant::now() + Duration::from_secs(600);
    let mut quiet_since = Instant::now();
    loop {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                quiet_since = Instant::now();
                let Some(event) = message.payload().and_then(wire::decode_execution_event) else {
                    continue;
                };
                let key = (event.raft_group, event.raft_index, event.ordinal, event.target_order_id);
                if !seen_keys.insert(key) {
                    republished += 1;
                    continue;
                }
                events += 1;
                let target = event.target_order_id;
                has_result.insert(target);
                if event.report.type_code == wire::RT_CMD_REJECTED {
                    has_result.insert(event.report.order_id.0);
                }
                if event.report.type_code == wire::RT_ACCEPTED {
                    accepted_at.entry(target).or_default().insert((event.raft_group, event.raft_index));
                }
                if event.report.type_code == wire::RT_TRADE {
                    *traded.entry(target).or_default() += event.report.qty;
                }
            }
            Some(Err(error)) => eprintln!("kafka: {error}"),
            None => {}
        }
        let missing = orders.keys().filter(|id| !has_result.contains(id)).count();
        if missing == 0 && quiet_since.elapsed() > Duration::from_secs(5) {
            break;
        }
        if Instant::now() > deadline || quiet_since.elapsed() > Duration::from_secs(120) {
            break;
        }
    }
    let lost: Vec<u64> = orders.keys().filter(|id| !has_result.contains(id)).copied().collect();
    let double: Vec<u64> = accepted_at
        .iter()
        .filter(|(id, at)| orders.contains_key(id) && at.len() > 1)
        .map(|(id, _)| *id)
        .collect();
    let overfill: Vec<u64> = traded
        .iter()
        .filter(|(id, qty)| orders.get(id).is_some_and(|limit| **qty > *limit))
        .map(|(id, _)| *id)
        .collect();
    println!("execution topic: {events} distinct events, {republished} republished copies (deduped)");
    println!("lost orders (accepted, no result): {}  {:?}", lost.len(), &lost[..lost.len().min(5)]);
    println!("double-applied orders: {}  {:?}", double.len(), &double[..double.len().min(5)]);
    println!("over-filled orders: {}  {:?}", overfill.len(), &overfill[..overfill.len().min(5)]);

    // ---- replicas ---------------------------------------------------------
    // Snapshots are taken when a node is idle (every TC_SNAPSHOT_EVERY_SECS),
    // so once traffic stops every replica settles on the same index.
    let mut verdict = "not compared".to_string();
    for attempt in 0..12 {
        std::thread::sleep(Duration::from_secs(10));
        let mut diverged = Vec::new();
        let mut unsettled = 0;
        for group in 0..groups {
            for category in 0..instruments {
                let mut answers = Vec::new();
                for node in 1..=5u32 {
                    let port = 9200 + group * 10 + node;
                    let body = http_get(&format!("127.0.0.1:{port}"), &format!("/fingerprint?category={category}"));
                    answers.push(body.and_then(|b| Some((json_u64(&b, "raft_applied_index")?, json_u64(&b, "fingerprint")?))));
                }
                let reached: Vec<_> = answers.iter().flatten().collect();
                if reached.len() < 5 || reached.iter().any(|a| a.0 != reached[0].0) {
                    unsettled += 1;
                } else if reached.iter().any(|a| a.1 != reached[0].1) {
                    diverged.push((group, category));
                }
            }
        }
        if !diverged.is_empty() {
            verdict = format!("DIVERGED in {} (group, category): {:?}", diverged.len(), &diverged[..diverged.len().min(5)]);
            break;
        }
        verdict = if unsettled == 0 {
            "all replicas identical".into()
        } else {
            format!("{unsettled} (group, category) pairs not yet at one index (attempt {attempt})")
        };
        if unsettled == 0 {
            break;
        }
    }
    println!("replicas: {verdict}");
    let ok = lost.is_empty() && double.is_empty() && overfill.is_empty() && verdict == "all replicas identical";
    println!("CHAOS VERDICT: {}", if ok { "PASS" } else { "FAIL" });
    std::process::exit(if ok { 0 } else { 1 });
}
