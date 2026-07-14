//! Load a real Raft leader over its persistent binary TCP ingress and wait for
//! the leader's quorum commit index to cover the complete submitted batch.
//!
//! Run: cargo bench --bench raft_throughput -- [ORDER_ADDR METRICS_ADDR COMMANDS ASSETS]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trade_core::order::Order;
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire::{self, MSG_LEN};

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

fn main() {
    let mut args = std::env::args().skip(1);
    let order_addr = args.next().unwrap_or_else(|| "127.0.0.1:9305".into());
    let metrics_addr = args.next().unwrap_or_else(|| "127.0.0.1:9205".into());
    let commands = args.next().and_then(|v| v.parse().ok()).unwrap_or(100_000u64);
    let assets = args.next().and_then(|v| v.parse().ok()).unwrap_or(10_000u32).max(1);

    let role = metric(&metrics_addr, "tc_raft_role").expect("read leader role");
    assert_eq!(role, 2, "{metrics_addr} is not the current leader");
    let commit_before = metric(&metrics_addr, "tc_raft_commit_index").expect("read commit index");
    let target = commit_before + commands;
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        * 1_000_000;

    let mut writer = TcpStream::connect(&order_addr).expect("connect Raft leader ingress");
    writer.set_nodelay(true).ok();
    let mut reader = writer.try_clone().expect("clone Raft ingress socket");
    reader
        .set_read_timeout(Some(Duration::from_secs(120)))
        .ok();
    let ack_reader = std::thread::spawn(move || {
        let mut ack = [0u8; 8192];
        let mut received = 0u64;
        while received < commands {
            let need = ((commands - received) as usize).min(ack.len());
            let n = reader.read(&mut ack[..need]).expect("read ingress ACKs");
            assert!(n > 0, "Raft ingress closed before all ACKs");
            assert!(ack[..n].iter().all(|v| *v == 1), "invalid ingress ACK");
            received += n as u64;
        }
    });

    let started = Instant::now();
    let mut frame = [0u8; MSG_LEN];
    for i in 0..commands {
        let side = if i & 1 == 0 { Side::Buy } else { Side::Sell };
        let instrument = InstrumentId(1 + (i as u32 % assets));
        let order = Order::limit(OrderId(base + i), side, 1_000, 1)
            .on(instrument)
            .by(100_000 + i % 10_000);
        wire::encode_new(&order, &mut frame);
        writer.write_all(&frame).expect("write command frame");
    }
    ack_reader.join().expect("ACK reader");
    let ack_elapsed = started.elapsed();
    drop(writer);

    let deadline = Instant::now() + Duration::from_secs(120);
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

    println!("real five-node Raft: {commands} commands across {assets} assets");
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
