//! Load a real Raft leader over its persistent binary TCP ingress and wait for
//! the leader's quorum commit index to cover the complete submitted batch.
//!
//! Run: cargo bench --bench raft_throughput -- [ORDER_ADDR METRICS_ADDR COMMANDS ASSETS]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trade_core::gateway::COMMITTED_BATCH_MAGIC;
use trade_core::order::Order;
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire::{self, MSG_LEN, RAFT_BATCH_MAX_COMMANDS};

fn metric(addr: &str, name: &str) -> Result<u64, String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| e.to_string())?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: raft\r\nConnection: close\r\n\r\n")
        .map_err(|e| e.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| e.to_string())?;
    response
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == name).then(|| value.parse().ok()).flatten()
        })
        .ok_or_else(|| format!("metric {name} missing from {addr}"))
}

fn discover_local_leader() -> (String, String) {
    for node in 1..=5 {
        let metrics_addr = format!("127.0.0.1:92{node:02}");
        if metric(&metrics_addr, "tc_raft_role") == Ok(2) {
            return (format!("127.0.0.1:93{node:02}"), metrics_addr);
        }
    }
    panic!("no local Raft leader found on nodes 1..5");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next();
    let local_discovery = first.is_none();
    let (mut order_addr, mut metrics_addr) = match first {
        Some(order_addr) => (
            order_addr,
            args.next().unwrap_or_else(|| "127.0.0.1:9205".into()),
        ),
        None => discover_local_leader(),
    };
    let commands = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000u64);
    let assets = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000u32)
        .max(1);
    let batch_size = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000usize)
        .clamp(1, RAFT_BATCH_MAX_COMMANDS);

    let role = metric(&metrics_addr, "tc_raft_role").expect("read leader role");
    assert_eq!(role, 2, "{metrics_addr} is not the current leader");
    let commit_before = metric(&metrics_addr, "tc_raft_commit_index").expect("read commit index");
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        * 1_000_000;

    let started = Instant::now();
    let submission_deadline = started + Duration::from_secs(120);
    let mut submitted = 0u64;
    let mut target = commit_before;
    let mut reconnects = 0u64;
    while submitted < commands {
        let count = (commands - submitted).min(batch_size as u64) as usize;
        let mut batch = Vec::with_capacity(8 + count * MSG_LEN);
        batch.extend_from_slice(&COMMITTED_BATCH_MAGIC);
        batch.extend_from_slice(&(count as u32).to_be_bytes());
        for offset in 0..count {
            let i = submitted + offset as u64;
            let side = if i & 1 == 0 { Side::Buy } else { Side::Sell };
            let instrument = InstrumentId(1 + (i as u32 % assets));
            let order = Order::limit(OrderId(base + i), side, 1_000, 1)
                .on(instrument)
                .by(100_000 + i % 10_000);
            let mut frame = [0u8; MSG_LEN];
            wire::encode_new(&order, &mut frame);
            batch.extend_from_slice(&frame);
        }
        loop {
            assert!(
                Instant::now() < submission_deadline,
                "submission timeout after {submitted} commands"
            );
            if local_discovery {
                (order_addr, metrics_addr) = discover_local_leader();
            }
            let attempt = (|| -> std::io::Result<u64> {
                let mut stream = TcpStream::connect(&order_addr)?;
                stream.set_nodelay(true)?;
                stream.set_read_timeout(Some(Duration::from_secs(10)))?;
                stream.set_write_timeout(Some(Duration::from_secs(10)))?;
                stream.write_all(&batch)?;
                let mut ack = [0u8; 9];
                stream.read_exact(&mut ack)?;
                if ack[0] != 1 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid committed ACK",
                    ));
                }
                Ok(u64::from_be_bytes(ack[1..].try_into().unwrap()))
            })();
            match attempt {
                Ok(index) => {
                    target = target.max(index);
                    submitted += count as u64;
                    break;
                }
                Err(error) => {
                    reconnects += 1;
                    eprintln!("batch at {submitted} retrying after {error}");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
    let ack_elapsed = started.elapsed();

    let deadline = Instant::now() + Duration::from_secs(30);
    let committed = loop {
        let current = metric(&metrics_addr, "tc_raft_commit_index").expect("poll commit index");
        if current >= target || Instant::now() >= deadline {
            break current;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let commit_elapsed = started.elapsed();
    assert!(
        committed >= target,
        "commit timeout: start={commit_before}, target={target}, current={committed}"
    );

    println!(
        "real five-node Raft: {commands} commands across {assets} assets, batch={batch_size}, reconnects={reconnects}"
    );
    println!(
        "ingress ACK: {:.2?}, {:.0} commands/s",
        ack_elapsed,
        commands as f64 / ack_elapsed.as_secs_f64()
    );
    println!(
        "quorum commit: {:.2?}, {:.0} commands/s (index {commit_before} -> {committed})",
        commit_elapsed,
        commands as f64 / commit_elapsed.as_secs_f64()
    );
}
