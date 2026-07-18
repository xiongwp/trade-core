//! Durable Raft-backed matching node.
//!
//! Usage: `raft-node NODE_ID RAFT_ADDR PEERS ORDER_ADDR DATA_DIR [MD_ADDR] [METRICS_ADDR]`.
//! Client frames reach the matching queue only through committed Raft entries.

use std::collections::{BTreeMap, HashMap, VecDeque};
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
use trade_core::metrics::LatencyMetric;
use trade_core::raft_log::{ClusterConfig, RaftNode, MAX_CLUSTER_SIZE};
use trade_core::sharding::DEFAULT_ASSET_CATEGORY_SIZE;
use trade_core::wire::{self, MSG_LEN};
use trade_core::{log_error, log_info, log_warn};

const PROPOSAL_BATCH: usize = 512;
const MAX_RAFT_MESSAGE_BYTES: usize = 16 << 20;

/// Set by the SIGTERM/SIGINT handler (async-signal-safe: a lone atomic store).
/// A watcher thread turns it into the normal drain path.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

fn install_signal_handlers() {
    let handler = on_signal as *const () as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

struct ProposalRequest {
    commands: Vec<trade_core::Command>,
    committed: mpsc::SyncSender<u64>,
}

/// Bounded retry/replay deduplication for commands already committed by Raft.
///
/// Keeping every command id forever makes resident memory proportional to the
/// lifetime traffic of a group, which prevents long-lived nodes from scaling.
/// Kafka retries and offset replays are time-bounded, so retain only the newest
/// configured working set. Raft/WAL application watermarks remain the durable
/// recovery guard; this window only suppresses command-level redelivery.
struct CommittedIdWindow {
    indexes: HashMap<u64, u64>,
    insertion_order: VecDeque<u64>,
    max_ids: usize,
}

impl CommittedIdWindow {
    fn new(max_ids: usize) -> Self {
        Self {
            indexes: HashMap::with_capacity(max_ids.min(1 << 20)),
            insertion_order: VecDeque::with_capacity(max_ids.min(1 << 20)),
            max_ids: max_ids.max(1),
        }
    }

    fn get(&self, id: u64) -> Option<u64> {
        self.indexes.get(&id).copied()
    }

    fn contains(&self, id: u64) -> bool {
        self.indexes.contains_key(&id)
    }

    fn remember(&mut self, id: u64, index: u64) {
        if id == 0 || self.indexes.contains_key(&id) {
            return;
        }
        self.indexes.insert(id, index);
        self.insertion_order.push_back(id);
        while self.indexes.len() > self.max_ids {
            if let Some(expired) = self.insertion_order.pop_front() {
                self.indexes.remove(&expired);
            }
        }
    }
}

fn main() {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("raft-node");
    install_signal_handlers();

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
    assert!(
        (1..=MAX_CLUSTER_SIZE).contains(&peers.len()),
        "cluster must have between 1 and {MAX_CLUSTER_SIZE} peers"
    );
    let mut voters = peers.keys().copied().collect::<Vec<u64>>();
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

    let config = ClusterConfig::new(id, voters).expect("valid cluster membership");
    // Bound recovery time and per-asset WAL growth: without periodic engine
    // snapshots the production node would replay from genesis and grow its
    // journals forever. Each shard writes `snapshot-shard-N.bin` and truncates
    // its journal on this cadence; that durable engine snapshot is the state the
    // Raft log compaction below folds its prefix into. Set 0 to disable.
    let snapshot_every_secs = std::env::var("TC_SNAPSHOT_EVERY_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let snapshot_every = (snapshot_every_secs > 0).then(|| Duration::from_secs(snapshot_every_secs));
    let (gateway, sink, handle) = build(ExchangeConfig {
        journal_dir: Some(data_dir.join("journal")),
        snapshot_every,
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
    // Single-group deployment: register one commit-latency series (group 0).
    // A future split-matching topology registers one per group here.
    metrics.register_raft_commit_groups(1);
    let execution_kafka_brokers = std::env::var("TC_EXECUTION_KAFKA_BROKERS").ok();
    metrics
        .execution_outbox_publish_healthy
        .store(execution_kafka_brokers.is_none() as u64, Ordering::Release);
    let running = Arc::new(AtomicBool::new(true));
    let accepting = Arc::new(AtomicBool::new(false));
    let leadership = Arc::new(AtomicBool::new(false));
    let (message_tx, message_rx) = mpsc::channel();
    let (proposal_tx, proposal_rx) = mpsc::sync_channel::<ProposalRequest>(1 << 16);
    spawn_listener(raft_addr, message_tx);

    let runtime_running = running.clone();
    let runtime_accepting = accepting.clone();
    let runtime_leadership = leadership.clone();
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
    let dedup_max_ids = std::env::var("TC_RAFT_DEDUP_MAX_IDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);
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
            let mut committed_ids = CommittedIdWindow::new(dedup_max_ids);
            let mut was_leader = false;
            let mut last_tick = Instant::now();
            // Fencing: committed terms are monotonic within a member's stream.
            // A regression would mean a stale leader's entry reached apply, so
            // refuse to matching it. route_version is reserved for the future
            // split-matching topology (single group => 0 today).
            let mut fence_term = 0u64;
            // Opt-in Raft log compaction. Once the durably-applied contiguous
            // prefix has grown by this many entries past the last snapshot, the
            // consensus log prefix (in memory and on the WAL) is folded into a
            // snapshot point. Disabled (0) by default so it never races the
            // engine's own snapshot/journal maintenance unless deliberately on.
            let compact_threshold = std::env::var("TC_RAFT_COMPACT_APPLIED_THRESHOLD")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let mut committed_batch_indexes: std::collections::BTreeSet<u64> =
                std::collections::BTreeSet::new();
            let mut last_compaction_check = Instant::now();
            let applied_batches = asset_log::load_applied_batches(&asset_root)
                .expect("load applied Raft batch watermarks");
            if let Some(index) = applied_batches.iter().max().copied() {
                runtime_metrics.set_raft_enqueued_index(index);
                runtime_metrics.set_raft_applied_index(index);
            }
            if let Some(reference) = node.take_installed_snapshot() {
                log_info!(
                    "raft-node",
                    "event=recovered_snapshot raft_index={} reference_bytes={}",
                    node.snapshot_index(),
                    reference.len()
                );
            }

            // Rebuild exact idempotency and matching state before this member
            // can campaign or accept a retry. Otherwise a fast election can
            // race the recovered committed prefix and append an old command a
            // second time.
            for committed in node.take_committed() {
                let index = committed.index;
                assert!(
                    committed.term >= fence_term,
                    "fencing violation: committed term regressed from {fence_term} to {} at index {index}",
                    committed.term
                );
                fence_term = committed.term;
                let payload = committed.data;
                let commands = wire::decode_raft_entry(&payload)
                    .expect("durable committed Raft entry is not a valid command batch");
                let is_batch = payload.len() != MSG_LEN;
                if is_batch && applied_batches.contains(&index) {
                    for command in commands {
                        let command_id = command.id();
                        committed_ids.remember(command_id, index);
                    }
                    runtime_metrics.set_raft_enqueued_index(index);
                    runtime_metrics.set_raft_applied_index(index);
                    continue;
                }
                let mut apply = Vec::new();
                for command in commands {
                    let command_id = command.id();
                    committed_ids.remember(command_id, index);
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
                runtime_leadership.store(leader, Ordering::Release);
                runtime_metrics.set_raft_state(
                    leader as u64 * 2,
                    node.term(),
                    node.leader_id(),
                    node.commit_index(),
                );
                let ready = node.leader_id() != 0
                    && runtime_metrics.raft_apply_lag() <= max_apply_lag
                    && runtime_metrics.asset_wal_errors.load(Ordering::Relaxed) == 0
                    && runtime_metrics
                        .execution_outbox_pending
                        .load(Ordering::Relaxed)
                        <= max_outbox_pending
                    && runtime_metrics
                        .execution_outbox_publish_healthy
                        .load(Ordering::Acquire)
                        == 1;
                runtime_metrics.set_ready(ready);
                runtime_accepting.store(leader && ready, Ordering::Release);
                if leader && !pending.is_empty() {
                    let requests = std::mem::take(&mut pending);
                    let mut entries = Vec::new();
                    let mut waiters = Vec::new();
                    for request in requests {
                        let committed_index = request
                            .commands
                            .iter()
                            .filter_map(|command| committed_ids.get(command.id()))
                            .max();
                        if request.commands.iter().all(|command| {
                            command.id() != 0 && committed_ids.contains(command.id())
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
                for committed in node.take_committed() {
                    let index = committed.index;
                    assert!(
                        committed.term >= fence_term,
                        "fencing violation: committed term regressed from {fence_term} to {} at index {index}",
                        committed.term
                    );
                    fence_term = committed.term;
                    let payload = committed.data;
                    committed_batch_indexes.insert(index);
                    if let Some(started) = commit_started.remove(&index) {
                        let commit_ns = started.elapsed().as_nanos() as u64;
                        runtime_metrics.record_raft_commit_latency(commit_ns);
                        runtime_metrics
                            .record_latency_hist(LatencyMetric::RaftCommit, commit_ns);
                        runtime_metrics.record_raft_commit_latency_group(0, commit_ns);
                    }
                    let waiters = commit_waiters.remove(&index);
                    let commands = wire::decode_raft_entry(&payload)
                        .expect("committed Raft entry is not a valid command batch");
                    let mut apply = Vec::new();
                    for command in commands {
                        let id = command.id();
                        let duplicate = id != 0 && committed_ids.contains(id);
                        committed_ids.remember(id, index);
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
                if compact_threshold != 0
                    && last_compaction_check.elapsed() >= Duration::from_secs(1)
                    && node
                        .applied_index()
                        .saturating_sub(node.snapshot_index())
                        >= compact_threshold
                {
                    last_compaction_check = Instant::now();
                    // The safe compaction point is the longest contiguous run of
                    // committed batches this member has *durably applied* (per
                    // the asset WAL watermarks). Compacting past an un-applied
                    // entry would drop a command still needed for recovery.
                    let applied = asset_log::load_applied_batches(&asset_root)
                        .expect("load applied Raft batch watermarks");
                    let mut safe = node.snapshot_index();
                    for &idx in committed_batch_indexes.iter() {
                        if applied.contains(&idx) {
                            safe = idx;
                        } else {
                            break;
                        }
                    }
                    if safe > node.snapshot_index() {
                        // The snapshot blob is a durable reference to the engine
                        // state at `safe`; the engine's own snapshots/WAL hold
                        // the recoverable state, so the Raft layer only needs the
                        // fencing index to bound the log.
                        let reference = safe.to_le_bytes().to_vec();
                        match node.compact(safe, reference) {
                            Ok(true) => {
                                committed_batch_indexes.retain(|&idx| idx > safe);
                                log_info!(
                                    "raft-node",
                                    "event=compacted raft_index={safe} first_log_index={}",
                                    node.first_log_index()
                                );
                            }
                            Ok(false) => {}
                            Err(error) => {
                                log_error!("raft-node", "event=compaction_failed raft_index={safe} error={error}");
                            }
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
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", std::env::var("TC_EXECUTION_KAFKA_LINGER_MS").unwrap_or_else(|_| "2".into()))
            .set("batch.num.messages", std::env::var("TC_EXECUTION_KAFKA_BATCH_MESSAGES").unwrap_or_else(|_| "10000".into()))
            .set("compression.type", std::env::var("TC_EXECUTION_KAFKA_COMPRESSION").unwrap_or_else(|_| "lz4".into()))
            .set("queue.buffering.max.kbytes", std::env::var("TC_EXECUTION_KAFKA_QUEUE_KBYTES").unwrap_or_else(|_| "1048576".into()))
            .set(
                "message.timeout.ms",
                std::env::var("TC_EXECUTION_KAFKA_DELIVERY_TIMEOUT_MS")
                    .unwrap_or_else(|_| "5000".into()),
            );
        config.create::<FutureProducer>()
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
            leadership,
        );
    }
    // Graceful stop: SIGTERM/SIGINT (e.g. `docker stop`) clears `running` so the
    // raft runtime loop and the committed ingress serve loop both wind down, and
    // a throwaway self-connection unblocks a pending `accept()`. `handle.shutdown()`
    // then drains and flushes rather than the process being SIGKILLed.
    let watch_running = running.clone();
    let watch_addr = order_addr.clone();
    thread::spawn(move || {
        while !SHUTDOWN.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(100));
        }
        log_info!("raft-node", "shutdown signal received, draining");
        watch_running.store(false, Ordering::Release);
        let _ = TcpStream::connect(&watch_addr);
    });

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
    leadership: Arc<AtomicBool>,
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
                        if !is_execution_outbox_file(&path) || readers.contains_key(&path) {
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
                                log_error!("execution-outbox", "event=open_failed path={} error={error}", path.display());
                            }
                        }
                    }
                }
                if !leadership.load(Ordering::Acquire) {
                    // Followers retain the durable outbox for failover but do
                    // not own the external side effect.
                    metrics
                        .execution_outbox_publish_healthy
                        .store(1, Ordering::Release);
                }
                let pending_before_publish = readers
                    .values()
                    .filter_map(|reader| reader.pending_records().ok())
                    .sum();
                metrics
                    .execution_outbox_pending
                    .store(pending_before_publish, Ordering::Release);
                for reader in readers.values_mut() {
                    if !leadership.load(Ordering::Acquire) {
                        continue;
                    }
                    match reader.read_batch(batch_size) {
                        Ok(records) if !records.is_empty() => {
                            let publish_started = std::time::Instant::now();
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
                                let publish_ns = publish_started.elapsed().as_nanos() as u64;
                                metrics
                                    .execution_kafka_publish_ns_total
                                    .fetch_add(publish_ns, Ordering::Relaxed);
                                metrics
                                    .execution_kafka_publish_ns_max
                                    .fetch_max(publish_ns, Ordering::Relaxed);
                                metrics
                                    .execution_kafka_publish_samples
                                    .fetch_add(1, Ordering::Relaxed);
                                if let Err(error) = reader.acknowledge(records.len()) {
                                    metrics
                                        .execution_outbox_publish_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                    metrics
                                        .execution_outbox_publish_healthy
                                        .store(0, Ordering::Release);
                                    log_error!(
                                        "execution-outbox",
                                        "event=cursor_failed error={error}"
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
                                log_warn!(
                                    "execution-kafka",
                                    "event=batch_not_acknowledged records={}",
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
                            log_error!("execution-outbox", "event=read_failed error={error}");
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

fn is_execution_outbox_file(path: &std::path::Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("bin")
        && path
            .file_stem()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("outbox-shard-"))
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
    fn execution_outbox_discovery_ignores_publisher_cursor() {
        assert!(is_execution_outbox_file(std::path::Path::new(
            "outbox-shard-0.bin"
        )));
        assert!(!is_execution_outbox_file(std::path::Path::new(
            "outbox-shard-0.published.cursor"
        )));
        assert!(!is_execution_outbox_file(std::path::Path::new(
            "unrelated.bin"
        )));
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

    #[test]
    fn committed_id_window_evicts_oldest_id() {
        let mut window = CommittedIdWindow::new(2);
        window.remember(11, 101);
        window.remember(12, 102);
        window.remember(13, 103);
        assert!(!window.contains(11));
        assert_eq!(window.get(12), Some(102));
        assert_eq!(window.get(13), Some(103));
    }

    #[test]
    fn committed_id_window_keeps_original_commit_index_for_duplicates() {
        let mut window = CommittedIdWindow::new(2);
        window.remember(11, 101);
        window.remember(11, 999);
        window.remember(12, 102);
        assert_eq!(window.get(11), Some(101));
        assert_eq!(window.get(12), Some(102));
    }
}
