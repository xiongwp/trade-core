//! Five-node Raft TCP simulation. This is intentionally an in-memory
//! demonstration runtime; production replaces it with durable storage.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use protobuf::Message as PbMessage;
use raft::prelude::Message;
use trade_core::raft_log::{ClusterConfig, RaftNode, MAX_CLUSTER_SIZE};

fn main() {
    let mut args = std::env::args().skip(1);
    let id: u64 = args
        .next()
        .expect("node id")
        .parse()
        .expect("numeric node id");
    let listen = args.next().expect("listen address");
    let peers = args.next().expect("peers: id@host:port,...");
    let peers = parse_peers(&peers);
    assert!(
        (1..=MAX_CLUSTER_SIZE).contains(&peers.len()),
        "simulation cluster must have between 1 and {MAX_CLUSTER_SIZE} peers"
    );
    let mut voters = peers.keys().copied().collect::<Vec<u64>>();
    voters.sort_unstable();
    let mut node = RaftNode::new(ClusterConfig::new(id, voters).expect("valid cluster config"))
        .expect("create raft node");
    let (tx, rx) = mpsc::channel();
    spawn_listener(listen, tx);
    let mut last_tick = Instant::now();
    let mut proposed = false;
    let mut last_status = Instant::now() - Duration::from_secs(10);

    loop {
        while let Ok(message) = rx.try_recv() {
            let _ = node.step(message);
        }
        if last_tick.elapsed() >= Duration::from_millis(100) {
            node.tick();
            last_tick = Instant::now();
        }
        if node.is_leader() && !proposed {
            node.propose(b"raft-simulation-committed".to_vec())
                .expect("leader proposal");
            proposed = true;
        }
        for message in node.take_outbound() {
            if let Some(addr) = peers.get(&message.to) {
                send(addr, &message);
            }
        }
        for committed in node.take_committed() {
            eprintln!(
                "[raft-sim node={id}] committed index={} term={} payload={:?}",
                committed.index, committed.term, committed.data
            );
        }
        if last_status.elapsed() >= Duration::from_secs(2) {
            eprintln!(
                "[raft-sim node={id}] leader={} is_leader={}",
                node.leader_id(),
                node.is_leader()
            );
            last_status = Instant::now();
        }
        thread::sleep(Duration::from_millis(5));
    }
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
