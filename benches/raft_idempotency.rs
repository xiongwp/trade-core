//! Verify that retrying an already quorum-committed command returns the
//! original commit index without appending or matching the command again.
//!
//! Run: cargo bench --bench raft_idempotency -- [ORDER_ADDR METRICS_ADDR [EXISTING_COMMAND_ID]]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trade_core::order::Order;
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire::{self, MSG_LEN};

fn try_metric(addr: &str, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect(addr).ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: raft\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.lines().find_map(|line| {
        let (key, value) = line.split_once(' ')?;
        (key == name).then(|| value.parse().ok()).flatten()
    })
}

fn metric(addr: &str, name: &str) -> u64 {
    try_metric(addr, name).unwrap_or_else(|| panic!("metric {name} missing from {addr}"))
}

fn submit(addr: &str, frame: &[u8; MSG_LEN]) -> u64 {
    let mut stream = TcpStream::connect(addr).expect("connect Raft command ingress");
    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    stream.write_all(frame).expect("write command");
    let mut ack = [0u8; 9];
    stream.read_exact(&mut ack).expect("read committed ACK");
    assert_eq!(ack[0], 1, "command was not quorum committed");
    u64::from_be_bytes(ack[1..].try_into().unwrap())
}

fn discover_local_leader() -> (String, String) {
    for node in 1..=5 {
        let metrics_addr = format!("127.0.0.1:92{node:02}");
        if try_metric(&metrics_addr, "tc_raft_role") == Some(2) {
            return (format!("127.0.0.1:93{node:02}"), metrics_addr);
        }
    }
    panic!("no local Raft leader found on nodes 1..5");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next();
    let (order_addr, metrics_addr) = match first {
        Some(order_addr) => (
            order_addr,
            args.next().unwrap_or_else(|| "127.0.0.1:9205".into()),
        ),
        None => discover_local_leader(),
    };
    let existing_command_id = args.next().and_then(|value| value.parse::<u64>().ok());

    assert_eq!(
        metric(&metrics_addr, "tc_raft_role"),
        2,
        "target is not leader"
    );
    let before = metric(&metrics_addr, "tc_raft_commit_index");
    let command_id = existing_command_id.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    let order = Order::limit(OrderId(command_id), Side::Buy, 1, 1)
        .on(InstrumentId(9_999))
        .by(100_000);
    let mut frame = [0u8; MSG_LEN];
    wire::encode_new(&order, &mut frame);

    let first = submit(&order_addr, &frame);
    let second = submit(&order_addr, &frame);
    assert_eq!(
        second, first,
        "duplicate did not return original commit index"
    );

    std::thread::sleep(Duration::from_millis(200));
    let after = metric(&metrics_addr, "tc_raft_commit_index");
    let expected_delta = u64::from(existing_command_id.is_none());
    assert_eq!(
        after,
        before + expected_delta,
        "duplicate appended another Raft entry"
    );
    if existing_command_id.is_none() {
        assert_eq!(first, after, "ACK did not identify the committed entry");
    } else {
        assert!(first <= before, "historical command was appended again");
    }
    println!("idempotency passed: command {command_id} committed once at index {first}");
}
