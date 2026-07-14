//! Durable Raft-backed matching node.
//!
//! Usage: `raft-node NODE_ID RAFT_ADDR PEERS ORDER_ADDR DATA_DIR`.
//! Client frames reach the matching queue only through committed Raft entries.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use protobuf::Message as PbMessage;
use raft::prelude::Message;
use trade_core::asset_log;
use trade_core::exchange::{build, ExchangeConfig};
use trade_core::gateway;
use trade_core::raft_log::{ClusterConfig, RaftNode, CLUSTER_SIZE};
use trade_core::wire::{self, MSG_LEN};

fn main() {
    let mut args = std::env::args().skip(1);
    let id: u64 = args
        .next()
        .expect("node id")
        .parse()
        .expect("numeric node id");
    let raft_addr = args.next().expect("raft listen address");
    let peers = parse_peers(&args.next().expect("peers: id@host:port,..."));
    let order_addr = args.next().expect("order listen address");
    let data_dir = PathBuf::from(args.next().expect("data directory"));
    assert_eq!(peers.len(), CLUSTER_SIZE, "requires exactly five peers");
    let mut voters = [0u64; CLUSTER_SIZE];
    for (slot, voter) in voters.iter_mut().zip(peers.keys()) {
        *slot = *voter;
    }
    voters.sort_unstable();
    std::fs::create_dir_all(&data_dir).expect("create data directory");

    let config = ClusterConfig::new(id, voters).expect("valid five-node cluster");
    let (gateway, sink, handle) = build(ExchangeConfig {
        journal_dir: Some(data_dir.join("journal")),
        dedup_commands: true,
        ..ExchangeConfig::default()
    });
    let metrics = handle.metrics.clone();
    metrics.set_ready(false);
    let running = Arc::new(AtomicBool::new(true));
    let accepting = Arc::new(AtomicBool::new(false));
    let (message_tx, message_rx) = mpsc::channel();
    let (proposal_tx, proposal_rx) = mpsc::sync_channel(1 << 16);
    spawn_listener(raft_addr, message_tx);

    let runtime_running = running.clone();
    let runtime_accepting = accepting.clone();
    let runtime_peers = peers.clone();
    let runtime_metrics = metrics.clone();
    let state_path = data_dir.join("raft.state");
    let asset_root = data_dir.join("journal").join("assets");
    thread::Builder::new()
        .name("raft-runtime".into())
        .spawn(move || {
            let mut node = RaftNode::open(config, state_path).expect("open durable raft state");
            let mut pending = Vec::new();
            let mut last_tick = Instant::now();
            while runtime_running.load(Ordering::Acquire) {
                while let Ok(message) = message_rx.try_recv() {
                    node.step(message).expect("step raft message");
                }
                while let Ok(command) = proposal_rx.try_recv() {
                    pending.push(command);
                }
                if last_tick.elapsed() >= Duration::from_millis(100) {
                    node.tick();
                    last_tick = Instant::now();
                }
                let leader = node.is_leader();
                runtime_accepting.store(leader, Ordering::Release);
                runtime_metrics.set_raft_state(
                    leader as u64 * 2,
                    node.term(),
                    node.leader_id(),
                    node.commit_index(),
                );
                runtime_metrics.set_ready(node.leader_id() != 0);
                if leader {
                    for command in pending.drain(..) {
                        let mut frame = [0u8; MSG_LEN];
                        wire::encode_command(&command, &mut frame);
                        node.propose(frame.to_vec()).expect("leader proposal");
                    }
                }
                for message in node.take_outbound() {
                    if let Some(addr) = runtime_peers.get(&message.to) {
                        send(addr, &message);
                    }
                }
                for (index, payload) in node.take_committed() {
                    let Some(command) =
                        wire::WireView::parse(&payload).and_then(|view| view.to_command())
                    else {
                        panic!("committed Raft entry is not a valid order frame");
                    };
                    // The matching runtime already reconstructed commands at or
                    // below this durable watermark from its shard journal.
                    if asset_log::applied_raft_index(&asset_root, command.instrument())
                        .expect("read asset application watermark")
                        >= index
                    {
                        continue;
                    }
                    let mut pending_command = command;
                    loop {
                        match gateway.submit_committed(index, pending_command) {
                            Ok(()) => break,
                            Err(command) => {
                                pending_command = command;
                                thread::yield_now();
                            }
                        }
                    }
                }
                thread::sleep(Duration::from_millis(2));
            }
        })
        .expect("spawn raft runtime");

    trade_core::metrics::serve("0.0.0.0:9102".to_string(), metrics);
    let listener = TcpListener::bind(&order_addr).expect("bind order listener");
    let md_listener = TcpListener::bind("0.0.0.0:9101").expect("bind market-data fanout");
    let ingress_running = running.clone();
    gateway::serve_committed_forever(
        listener,
        Some(md_listener),
        sink,
        running.clone(),
        0,
        move |command| {
            while !accepting.load(Ordering::Acquire) && ingress_running.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
            if ingress_running.load(Ordering::Acquire) {
                proposal_tx.send(command).expect("raft runtime stopped");
            }
        },
    )
    .expect("serve committed gateway");
    handle.shutdown();
}

fn parse_peers(input: &str) -> HashMap<u64, String> {
    input
        .split(',')
        .map(|item| {
            let (id, addr) = item.split_once('@').expect("peer format is id@host:port");
            (id.parse().expect("numeric peer id"), addr.to_string())
        })
        .collect()
}

fn spawn_listener(addr: String, tx: mpsc::Sender<Message>) {
    thread::spawn(move || {
        let listener = TcpListener::bind(&addr).expect("bind raft listener");
        for stream in listener.incoming().flatten() {
            let tx = tx.clone();
            thread::spawn(move || {
                let mut stream = stream;
                let mut size = [0u8; 4];
                if stream.read_exact(&mut size).is_err() {
                    return;
                }
                let mut bytes = vec![0; u32::from_be_bytes(size) as usize];
                if stream.read_exact(&mut bytes).is_ok() {
                    if let Ok(message) = Message::parse_from_bytes(&bytes) {
                        let _ = tx.send(message);
                    }
                }
            });
        }
    });
}

fn send(addr: &str, message: &Message) {
    let Ok(bytes) = message.write_to_bytes() else {
        return;
    };
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return;
    };
    let _ = stream.write_all(&(bytes.len() as u32).to_be_bytes());
    let _ = stream.write_all(&bytes);
}
