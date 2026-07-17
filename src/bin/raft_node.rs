//! Durable Raft-backed matching node.
//!
//! Usage: `raft-node NODE_ID RAFT_ADDR PEERS ORDER_ADDR DATA_DIR [MD_ADDR] [METRICS_ADDR]`.
//! Client frames reach the matching queue only through committed Raft entries.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use protobuf::Message as PbMessage;
use raft::prelude::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use trade_core::asset_log;
use trade_core::exchange::{build, ExchangeConfig};
use trade_core::gateway;
use trade_core::raft_log::{ClusterConfig, RaftNode, CLUSTER_SIZE};
use trade_core::sharding::DEFAULT_ASSET_CATEGORY_SIZE;
use trade_core::wire::{self, MSG_LEN};

const PROPOSAL_BATCH: usize = 512;
const MAX_RAFT_MESSAGE_BYTES: usize = 16 << 20;

struct ProposalRequest {
    commands: Vec<trade_core::Command>,
    committed: mpsc::SyncSender<u64>,
}

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
    let md_addr = args.next().unwrap_or_else(|| "0.0.0.0:9101".into());
    let metrics_addr = args.next().unwrap_or_else(|| "0.0.0.0:9102".into());
    assert_eq!(peers.len(), CLUSTER_SIZE, "requires exactly five peers");
    let mut voters = [0u64; CLUSTER_SIZE];
    for (slot, voter) in voters.iter_mut().zip(peers.keys()) {
        *slot = *voter;
    }
    voters.sort_unstable();
    std::fs::create_dir_all(&data_dir).expect("create data directory");
    let raft_group_id = std::env::var("TC_RAFT_GROUP_ID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let execution_outbox_dir = data_dir.join("execution-outbox");
    let pool_orders_per_book = std::env::var("TC_MATCH_POOL_PER_ASSET")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16usize)
        .max(1);

    let config = ClusterConfig::new(id, voters).expect("valid five-node cluster");
    let (gateway, sink, handle) = build(ExchangeConfig {
        journal_dir: Some(data_dir.join("journal")),
        // Exact duplicate suppression lives at the Raft ingress. The old
        // processor high-water cursors reject valid lower ids from independent
        // Kafka partitions, so they must not be enabled here.
        dedup_commands: false,
        // Ten thousand mostly sparse assets must not each reserve the
        // single-market default. Hot dedicated assets can raise this through
        // TC_MATCH_POOL_PER_ASSET; pools still grow dynamically when needed.
        pool_orders_per_book,
        execution_outbox_dir: Some(execution_outbox_dir.clone()),
        raft_group_id,
        execution_outbox_sync_every: std::env::var("TC_EXECUTION_OUTBOX_SYNC_EVERY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1),
        ..ExchangeConfig::default()
    });
    let metrics = handle.metrics.clone();
    metrics.set_ready(false);
    let execution_kafka_brokers = std::env::var("TC_EXECUTION_KAFKA_BROKERS").ok();
    metrics
        .execution_outbox_publish_healthy
        .store(execution_kafka_brokers.is_none() as u64, Ordering::Release);
    let running = Arc::new(AtomicBool::new(true));
    let accepting = Arc::new(AtomicBool::new(false));
    let (message_tx, message_rx) = mpsc::channel();
    let (proposal_tx, proposal_rx) = mpsc::sync_channel::<ProposalRequest>(1 << 16);
    spawn_listener(raft_addr, message_tx);

    let runtime_running = running.clone();
    let runtime_accepting = accepting.clone();
    let runtime_peers = peers.clone();
    let runtime_metrics = metrics.clone();
    let max_apply_lag = std::env::var("TC_RAFT_READY_MAX_APPLY_LAG")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(32);
    let max_outbox_pending = std::env::var("TC_RAFT_READY_MAX_OUTBOX_PENDING")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10_000);
    let state_path = data_dir.join("raft.state");
    let asset_root = data_dir.join("journal").join("assets");
    thread::Builder::new()
        .name("raft-runtime".into())
        .spawn(move || {
            let mut node = RaftNode::open(config, state_path).expect("open durable raft state");
            let transport = PeerTransport::spawn(runtime_peers, runtime_metrics.clone());
            let mut pending = Vec::new();
            let mut commit_waiters: BTreeMap<u64, Vec<mpsc::SyncSender<u64>>> = BTreeMap::new();
            let mut commit_started: BTreeMap<u64, Instant> = BTreeMap::new();
            let mut committed_ids: HashMap<u64, u64> = HashMap::new();
            let mut was_leader = false;
            let mut last_tick = Instant::now();
            let applied_batches = asset_log::load_applied_batches(&asset_root)
                .expect("load applied Raft batch watermarks");
            if let Some(index) = applied_batches.iter().max().copied() {
                runtime_metrics.set_raft_enqueued_index(index);
                runtime_metrics.set_raft_applied_index(index);
            }

            // Rebuild exact idempotency and matching state before this member
            // can campaign or accept a retry. Otherwise a fast election can
            // race the recovered committed prefix and append an old command a
            // second time.
            for (index, payload) in node.take_committed() {
                let commands = wire::decode_raft_entry(&payload)
                    .expect("durable committed Raft entry is not a valid command batch");
                let is_batch = payload.len() != MSG_LEN;
                if is_batch && applied_batches.contains(&index) {
                    for command in commands {
                        let command_id = command.id();
                        if command_id != 0 {
                            committed_ids.entry(command_id).or_insert(index);
                        }
                    }
                    runtime_metrics.set_raft_enqueued_index(index);
                    runtime_metrics.set_raft_applied_index(index);
                    continue;
                }
                let mut apply = Vec::new();
                for command in commands {
                    let command_id = command.id();
                    if command_id != 0 {
                        committed_ids.entry(command_id).or_insert(index);
                    }
                    if is_batch
                        || asset_log::applied_raft_index(&asset_root, command.instrument())
                            .expect("read asset application watermark")
                            < index
                    {
                        apply.push(command);
                    }
                }
                if apply.is_empty() {
                    runtime_metrics.set_raft_enqueued_index(index);
                    runtime_metrics.set_raft_applied_index(index);
                    continue;
                }
                let mut pending_command = trade_core::Command::Batch(apply);
                loop {
                    match gateway.submit_committed(index, pending_command) {
                        Ok(()) => break,
                        Err(command) => {
                            pending_command = command;
                            thread::yield_now();
                        }
                    }
                }
                runtime_metrics.set_raft_enqueued_index(index);
            }

            while runtime_running.load(Ordering::Acquire) {
                while let Ok(message) = message_rx.try_recv() {
                    node.step(message).expect("step raft message");
                }
                while pending.len() < PROPOSAL_BATCH {
                    let Ok(command) = proposal_rx.try_recv() else {
                        break;
                    };
                    pending.push(command);
                }
                if last_tick.elapsed() >= Duration::from_millis(100) {
                    node.tick();
                    last_tick = Instant::now();
                }
                let leader = node.is_leader();
                if was_leader && !leader {
                    pending.clear();
                    commit_waiters.clear();
                    commit_started.clear();
                }
                was_leader = leader;
                runtime_accepting.store(leader, Ordering::Release);
                runtime_metrics.set_raft_state(
                    leader as u64 * 2,
                    node.term(),
                    node.leader_id(),
                    node.commit_index(),
                );
                runtime_metrics.set_ready(
                    node.leader_id() != 0
                        && runtime_metrics.raft_apply_lag() <= max_apply_lag
                        && runtime_metrics.asset_wal_errors.load(Ordering::Relaxed) == 0
                        && runtime_metrics
                            .execution_outbox_pending
                            .load(Ordering::Relaxed)
                            <= max_outbox_pending
                        && runtime_metrics
                            .execution_outbox_publish_healthy
                            .load(Ordering::Acquire)
                            == 1,
                );
                if leader && !pending.is_empty() {
                    let requests = std::mem::take(&mut pending);
                    let mut entries = Vec::new();
                    let mut waiters = Vec::new();
                    for request in requests {
                        let committed_index = request
                            .commands
                            .iter()
                            .filter_map(|command| committed_ids.get(&command.id()).copied())
                            .max();
                        if request.commands.iter().all(|command| {
                            command.id() != 0 && committed_ids.contains_key(&command.id())
                        }) {
                            let _ = request.committed.send(committed_index.unwrap_or(0));
                            continue;
                        }
                        let frames = request
                            .commands
                            .iter()
                            .map(|command| {
                                let mut frame = [0u8; MSG_LEN];
                                wire::encode_command(command, &mut frame);
                                frame
                            })
                            .collect::<Vec<_>>();
                        let Some(entry) = wire::encode_raft_batch(&frames) else {
                            continue;
                        };
                        entries.push(entry);
                        waiters.push(request.committed);
                    }
                    if !entries.is_empty() {
                        node.propose_batch(entries)
                            .expect("leader category-batch proposal");
                        let first_index = node.last_index() + 1 - waiters.len() as u64;
                        for (offset, waiter) in waiters.into_iter().enumerate() {
                            let index = first_index + offset as u64;
                            commit_waiters.entry(index).or_default().push(waiter);
                            commit_started.insert(index, Instant::now());
                        }
                    }
                }
                for message in node.take_outbound() {
                    transport.send(message);
                }
                for (index, payload) in node.take_committed() {
                    if let Some(started) = commit_started.remove(&index) {
                        runtime_metrics
                            .record_raft_commit_latency(started.elapsed().as_nanos() as u64);
                    }
                    let waiters = commit_waiters.remove(&index);
                    let commands = wire::decode_raft_entry(&payload)
                        .expect("committed Raft entry is not a valid command batch");
                    let mut apply = Vec::new();
                    for command in commands {
                        let id = command.id();
                        let duplicate = id != 0 && committed_ids.contains_key(&id);
                        if id != 0 {
                            committed_ids.entry(id).or_insert(index);
                        }
                        if !duplicate
                            && asset_log::applied_raft_index(&asset_root, command.instrument())
                                .expect("read asset application watermark")
                                < index
                        {
                            apply.push(command);
                        }
                    }
                    if apply.is_empty() {
                        runtime_metrics.set_raft_enqueued_index(index);
                        runtime_metrics.set_raft_applied_index(index);
                    } else {
                        let mut pending_command = trade_core::Command::Batch(apply);
                        loop {
                            match gateway.submit_committed(index, pending_command) {
                                Ok(()) => break,
                                Err(command) => {
                                    pending_command = command;
                                    thread::yield_now();
                                }
                            }
                        }
                        runtime_metrics.set_raft_enqueued_index(index);
                    }
                    if let Some(waiters) = waiters {
                        for waiter in waiters {
                            let _ = waiter.send(index);
                        }
                    }
                }
                thread::sleep(Duration::from_millis(2));
            }
        })
        .expect("spawn raft runtime");

    trade_core::metrics::serve(metrics_addr, metrics.clone());
    let listener = TcpListener::bind(&order_addr).expect("bind order listener");
    let md_listener = TcpListener::bind(md_addr).expect("bind market-data fanout");
    let execution_topic =
        std::env::var("TC_EXECUTION_KAFKA_TOPIC").unwrap_or_else(|_| "trade-executions-v1".into());
    let category_size = std::env::var("TC_ORDER_CATEGORY_SIZE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(DEFAULT_ASSET_CATEGORY_SIZE)
        .max(1);
    let execution_producer = execution_kafka_brokers.map(|brokers| {
        ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "2")
            .set("batch.num.messages", "10000")
            .create::<FutureProducer>()
            .expect("create execution Kafka producer")
    });
    if let Some(producer) = execution_producer.clone() {
        spawn_execution_outbox_publisher(
            execution_outbox_dir,
            execution_topic.clone(),
            producer,
            category_size,
            running.clone(),
            metrics.clone(),
        );
    }
    let ingress_running = running.clone();
    gateway::serve_committed_forever(
        listener,
        Some(md_listener),
        sink,
        running.clone(),
        0,
        move |commands| {
            while !accepting.load(Ordering::Acquire) && ingress_running.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
            if ingress_running.load(Ordering::Acquire) {
                let (committed_tx, committed_rx) = mpsc::sync_channel(1);
                proposal_tx
                    .send(ProposalRequest {
                        commands,
                        committed: committed_tx,
                    })
                    .expect("raft runtime stopped");
                committed_rx.recv_timeout(Duration::from_secs(10)).ok()
            } else {
                None
            }
        },
        move |_event| {},
    )
    .expect("serve committed gateway");
    handle.shutdown();
}

fn spawn_execution_outbox_publisher(
    root: PathBuf,
    topic: String,
    producer: FutureProducer,
    category_size: u32,
    running: Arc<AtomicBool>,
    metrics: Arc<trade_core::metrics::Metrics>,
) {
    thread::Builder::new()
        .name("execution-outbox-publisher".into())
        .spawn(move || {
            let mut readers: HashMap<PathBuf, trade_core::execution_outbox::ExecutionOutboxReader> =
                HashMap::new();
            let batch_size = std::env::var("TC_EXECUTION_PUBLISH_BATCH")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(512)
                .clamp(1, 10_000);
            while running.load(Ordering::Acquire) {
                if let Ok(entries) = std::fs::read_dir(&root) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if !path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with("outbox-shard-"))
                            || readers.contains_key(&path)
                        {
                            continue;
                        }
                        let cursor_path = path.with_extension("published.cursor");
                        match trade_core::execution_outbox::ExecutionOutboxReader::open_with_cursor(
                            path.clone(),
                            cursor_path,
                        ) {
                            Ok(reader) => {
                                readers.insert(path, reader);
                            }
                            Err(error) => {
                                eprintln!("[execution-outbox] event=open_failed error={error}");
                            }
                        }
                    }
                }
                for reader in readers.values_mut() {
                    match reader.read_batch(batch_size) {
                        Ok(records) if !records.is_empty() => {
                            let prepared = records
                                .iter()
                                .map(|record| {
                                    (record.kafka_key(category_size), record.kafka_payload())
                                })
                                .collect::<Vec<_>>();
                            let deliveries = futures::executor::block_on(
                                futures::future::join_all(prepared.iter().map(|(key, payload)| {
                                    producer.send(
                                        FutureRecord::to(&topic).key(key).payload(payload),
                                        Duration::from_secs(5),
                                    )
                                })),
                            );
                            if deliveries.iter().all(Result::is_ok) {
                                if let Err(error) = reader.acknowledge(records.len()) {
                                    metrics
                                        .execution_outbox_publish_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                    metrics
                                        .execution_outbox_publish_healthy
                                        .store(0, Ordering::Release);
                                    eprintln!(
                                        "[execution-outbox] event=cursor_failed error={error}"
                                    );
                                } else {
                                    metrics
                                        .execution_outbox_published
                                        .fetch_add(records.len() as u64, Ordering::Relaxed);
                                    metrics
                                        .execution_outbox_publish_healthy
                                        .store(1, Ordering::Release);
                                }
                            } else {
                                metrics
                                    .execution_outbox_publish_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                metrics
                                    .execution_outbox_publish_healthy
                                    .store(0, Ordering::Release);
                                eprintln!(
                                    "[execution-kafka] event=batch_not_acknowledged records={}",
                                    records.len()
                                );
                            }
                        }
                        Ok(_) => {
                            metrics
                                .execution_outbox_publish_healthy
                                .store(1, Ordering::Release);
                        }
                        Err(error) => {
                            metrics
                                .execution_outbox_publish_failures
                                .fetch_add(1, Ordering::Relaxed);
                            metrics
                                .execution_outbox_publish_healthy
                                .store(0, Ordering::Release);
                            eprintln!("[execution-outbox] event=read_failed error={error}");
                        }
                    }
                }
                let pending = readers
                    .values()
                    .filter_map(|reader| reader.pending_records().ok())
                    .sum();
                metrics
                    .execution_outbox_pending
                    .store(pending, Ordering::Release);
                thread::sleep(Duration::from_millis(25));
            }
        })
        .expect("spawn execution outbox publisher");
}

#[cfg(test)]
fn execution_category_key(frame: &[u8; wire::REPORT_LEN], category_size: u32) -> [u8; 4] {
    let instrument = trade_core::InstrumentId(u32::from_le_bytes(
        frame[4..8].try_into().expect("execution instrument"),
    ));
    trade_core::sharding::asset_category(instrument, category_size).to_be_bytes()
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
            thread::spawn(move || read_peer_frames(stream, &tx));
        }
    });
}

fn read_peer_frames(mut stream: impl Read, tx: &mpsc::Sender<Message>) {
    loop {
        let mut size = [0u8; 4];
        if stream.read_exact(&mut size).is_err() {
            return;
        }
        let size = u32::from_be_bytes(size) as usize;
        if size == 0 || size > MAX_RAFT_MESSAGE_BYTES {
            return;
        }
        let mut bytes = vec![0; size];
        if stream.read_exact(&mut bytes).is_err() {
            return;
        }
        if let Ok(message) = Message::parse_from_bytes(&bytes) {
            if tx.send(message).is_err() {
                return;
            }
        }
    }
}

struct PeerTransport {
    peers: HashMap<u64, mpsc::SyncSender<Message>>,
    metrics: Arc<trade_core::metrics::Metrics>,
}

impl PeerTransport {
    fn spawn(peers: HashMap<u64, String>, metrics: Arc<trade_core::metrics::Metrics>) -> Self {
        let queue_capacity = std::env::var("TC_RAFT_TRANSPORT_QUEUE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(8192)
            .max(64);
        let mut senders = HashMap::with_capacity(peers.len());
        for (id, addr) in peers {
            let (tx, rx) = mpsc::sync_channel(queue_capacity);
            let worker_metrics = metrics.clone();
            thread::Builder::new()
                .name(format!("raft-peer-{id}"))
                .spawn(move || run_peer_writer(addr, rx, worker_metrics))
                .expect("spawn Raft peer writer");
            senders.insert(id, tx);
        }
        Self {
            peers: senders,
            metrics,
        }
    }

    fn send(&self, message: Message) {
        let Some(peer) = self.peers.get(&message.to) else {
            return;
        };
        if peer.try_send(message).is_err() {
            self.metrics
                .raft_transport_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn run_peer_writer(
    addr: String,
    rx: mpsc::Receiver<Message>,
    metrics: Arc<trade_core::metrics::Metrics>,
) {
    let mut stream: Option<TcpStream> = None;
    while let Ok(message) = rx.recv() {
        let Ok(bytes) = message.write_to_bytes() else {
            continue;
        };
        let mut frame = Vec::with_capacity(4 + bytes.len());
        frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(&bytes);
        let mut delivered = false;
        for attempt in 0..2 {
            if stream.is_none() {
                match TcpStream::connect(&addr) {
                    Ok(connected) => {
                        connected.set_nodelay(true).ok();
                        connected
                            .set_write_timeout(Some(Duration::from_millis(500)))
                            .ok();
                        stream = Some(connected);
                        metrics
                            .raft_transport_reconnects
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        if attempt == 0 {
                            thread::sleep(Duration::from_millis(10));
                        }
                        continue;
                    }
                }
            }
            if stream
                .as_mut()
                .is_some_and(|stream| stream.write_all(&frame).is_ok())
            {
                delivered = true;
                break;
            }
            stream = None;
        }
        if !delivered {
            metrics
                .raft_transport_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use trade_core::{ExecReport, InstrumentId, OrderId};

    fn category_key(instrument: u32) -> [u8; 4] {
        let mut frame = [0; wire::REPORT_LEN];
        wire::encode_report(
            &ExecReport::Accepted {
                instrument: InstrumentId(instrument),
                order_id: OrderId(instrument as u64),
            },
            &mut frame,
        );
        execution_category_key(&frame, 1_000)
    }

    #[test]
    fn execution_reports_partition_by_category_not_instrument() {
        assert_eq!(category_key(1), category_key(1_000));
        assert_ne!(category_key(1_000), category_key(1_001));
    }

    #[test]
    fn peer_connection_carries_multiple_framed_messages() {
        let messages = [(1, 2), (2, 1)].map(|(from, to)| Message {
            from,
            to,
            ..Default::default()
        });
        let mut wire = Vec::new();
        for message in &messages {
            let bytes = message.write_to_bytes().unwrap();
            wire.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            wire.extend_from_slice(&bytes);
        }
        let (tx, rx) = mpsc::channel();
        read_peer_frames(Cursor::new(wire), &tx);
        assert_eq!(rx.try_recv().unwrap().from, 1);
        assert_eq!(rx.try_recv().unwrap().from, 2);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn peer_connection_rejects_oversized_frame() {
        let (tx, rx) = mpsc::channel();
        read_peer_frames(
            Cursor::new(((MAX_RAFT_MESSAGE_BYTES + 1) as u32).to_be_bytes()),
            &tx,
        );
        assert!(rx.try_recv().is_err());
    }
}
