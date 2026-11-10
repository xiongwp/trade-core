//! Durable Raft-backed matching node.
//!
//! Usage: `raft-node NODE_ID RAFT_ADDR PEERS ORDER_ADDR DATA_DIR [MD_ADDR] [METRICS_ADDR]`.
//! Client frames reach the matching queue only through committed Raft entries.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use protobuf::Message as PbMessage;
use raft::prelude::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use trade_core::asset_log;
use trade_core::exchange::{build, ExchangeConfig};
use trade_core::gateway;
use trade_core::metrics::LatencyMetric;
use trade_core::raft_log::{ClusterConfig, RaftNode, MAX_CLUSTER_SIZE};
use trade_core::wire::{self, MSG_LEN};
use trade_core::{log_error, log_info, log_warn};

const PROPOSAL_BATCH: usize = 512;
/// Largest Raft message accepted from a peer (`TC_RAFT_MAX_MESSAGE_BYTES`,
/// default 1 GiB, at most 2 GiB: protobuf sizes are 32-bit). A compaction
/// snapshot travels as one message and carries every resting order plus the
/// dedupe window; a follower behind the compaction point can only catch up
/// if its group's snapshot fits.
fn max_raft_message_bytes() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("TC_RAFT_MAX_MESSAGE_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1 << 30)
            .clamp(1 << 20, (2 << 30) - 1)
    })
}
/// Messages travel in frames of at most this many bytes: a big snapshot is
/// several frames, so a receiver's buffer grows with the bytes that really
/// arrived instead of being sized up front from one length field.
const PEER_FRAME_BYTES: usize = 16 << 20;
/// Set in a frame's length word when more frames of the same message follow.
const FRAME_CONTINUES: u32 = 1 << 31;

fn filesystem_available_bytes(path: &Path) -> std::io::Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in data path"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    Ok((stats.f_bavail as u64).saturating_mul(stats.f_frsize))
}
/// Operator-created marker for replacing a voter's lost stable storage while
/// retaining its configured node id. It persists across process crashes and
/// is removed only after the leader has rediscovered this member's real log
/// end and replication has caught up to a safe heartbeat boundary.
const REJOIN_MARKER: &str = "raft-rejoin.required";

fn mark_non_command_applied(metrics: &trade_core::metrics::Metrics, index: u64) {
    metrics.set_raft_enqueued_index(index);
    metrics.set_raft_applied_index(index);
}

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
    received: Instant,
}

/// How long the ingress waits for this node to be a ready leader, and for a
/// proposal to commit. Together they stay under order-api's matcher IO
/// timeout (`TC_RAFT_MATCHER_IO_TIMEOUT_MS`, 5 s), so the caller hears back
/// before it gives up and resends the same commands elsewhere.
const INGRESS_LEADER_WAIT: Duration = Duration::from_secs(1);
const INGRESS_COMMIT_WAIT: Duration = Duration::from_millis(3_500);
/// A request queued longer than this is dropped, not proposed: its caller
/// has given up (and resent it), and proposing it much later could apply it
/// after its id has left the dedupe windows.
const PROPOSAL_MAX_AGE: Duration = Duration::from_secs(2);

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

    fn clear(&mut self) {
        self.indexes.clear();
        self.insertion_order.clear();
    }

    /// Note the ids of one committed entry. An unseal (a rolled-back
    /// migration) forgets everything: commands the engine refused as
    /// "category-moved" are in here too, and a resubmission of one must be
    /// proposed again, not acknowledged as already committed. The engine's
    /// own window still rejects true duplicates.
    fn remember_entry(&mut self, commands: &[trade_core::Command], index: u64) {
        if commands.iter().any(|command| {
            matches!(command, trade_core::Command::Migrate(op)
                if matches!(**op, trade_core::exchange::CategoryOp::Unseal { .. }))
        }) {
            self.clear();
            return;
        }
        for command in commands {
            self.remember(command.id(), index);
        }
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
    let state_path = data_dir.join("raft.state");
    let rejoin_marker_path = data_dir.join(REJOIN_MARKER);
    let engine_snapshot_path = data_dir.join("journal").join("snapshot-shard-0.bin");
    // Open consensus before constructing the matching engine. A follower may
    // have durably installed a Raft snapshot immediately before a crash; its
    // embedded matching snapshot must become the engine recovery point first.
    // Exclusive for the life of this process (released by the kernel on any
    // exit, including SIGKILL): two processes on one data dir — a duplicated
    // placement, or a supervisor restarted next to orphaned children — would
    // interleave WAL writes and snapshot renames.
    let _data_dir_lock = lock_data_dir(&data_dir).unwrap_or_else(|error| {
        panic!(
            "cannot lock Raft data directory {}: {error}",
            data_dir.display()
        )
    });
    let mut raft_node = RaftNode::open(config, state_path).expect("open durable raft state");
    if rejoin_marker_path.exists() {
        raft_node.begin_rejoin();
        log_warn!(
            "raft-node",
            "event=rejoin_started marker={} local_last={} local_commit={}",
            rejoin_marker_path.display(),
            raft_node.last_index(),
            raft_node.commit_index()
        );
    }
    if let Some((index, bytes)) = raft_node.take_installed_snapshot_with_index() {
        trade_core::snapshot::install_bytes(&engine_snapshot_path, &bytes, index)
            .expect("atomically install matching snapshot recovered from Raft");
    }
    // Bound recovery time and per-asset WAL growth: without periodic engine
    // snapshots the production node would replay from genesis and grow its
    // journals forever. Each shard writes `snapshot-shard-N.bin` and truncates
    // its journal on this cadence; that durable engine snapshot is the state the
    // Raft log compaction below folds its prefix into. Set 0 to disable.
    let snapshot_every_secs = std::env::var("TC_SNAPSHOT_EVERY_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let snapshot_every =
        (snapshot_every_secs > 0).then(|| Duration::from_secs(snapshot_every_secs));
    // Per-instrument trading rules (tick / lot / qty / price / min notional),
    // e.g. `*:tick=1,lot=1;7:tick=5,lot=10,min_notional=1000`. Every node of a
    // group must run the same table: rejections are part of the replicated,
    // deterministic report stream.
    let instrument_specs_text = std::env::var("TC_INSTRUMENT_SPECS").unwrap_or_default();
    let instrument_specs = if instrument_specs_text.trim().is_empty() {
        trade_core::InstrumentSpecs::default()
    } else {
        trade_core::InstrumentSpecs::parse(&instrument_specs_text)
            .expect("valid TC_INSTRUMENT_SPECS")
    };
    // The committed prefix recovered from the WAL, replayed below on top of
    // the engine snapshot (which records the index it covers).
    let recovered_committed = raft_node.take_committed();
    let engine_snapshot_index = trade_core::snapshot::load(&engine_snapshot_path)
        .map_or(0, |snapshot| snapshot.raft_applied_index);
    // Exact command-id dedupe window of the matching state machine. It is
    // replicated state: every replica of a group must use the same value.
    let command_id_window = std::env::var("TC_DEDUP_WINDOW_IDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(trade_core::exchange::DEFAULT_COMMAND_ID_WINDOW)
        .max(1);
    let (config_fingerprint, config_description) =
        deterministic_config(&instrument_specs_text, command_id_window);
    log_info!(
        "raft-node",
        "deterministic config fingerprint={config_fingerprint:016x} ({config_description})"
    );
    let config_check = std::env::var("TC_RAFT_CONFIG_CHECK")
        .map(|value| !matches!(value.trim(), "0" | "false" | "no" | "off"))
        .unwrap_or(true);
    // Set on every replica for a coordinated, whole-group config change: lets
    // a leader announce a fingerprint that differs from the group's last one.
    let config_adopt = std::env::var("TC_RAFT_CONFIG_ADOPT")
        .map(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    let group_config_path = data_dir.join("group-config.fingerprint");
    // The group's last announced fingerprint, kept on disk: announcements in
    // the log are compacted away, and without this a misconfigured replica
    // that restarted could win an election and make every correctly
    // configured follower stop (observed in a fault test).
    let mut group_config: Option<u64> = std::fs::read_to_string(&group_config_path)
        .ok()
        .and_then(|text| u64::from_str_radix(text.trim(), 16).ok());
    let announce_every = Duration::from_secs(
        std::env::var("TC_RAFT_CONFIG_ANNOUNCE_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30),
    );
    let (gateway, sink, handle) = build(ExchangeConfig {
        journal_dir: Some(data_dir.join("journal")),
        instrument_specs,
        raft_wal_authoritative: true,
        snapshot_every,
        // Exact duplicate suppression lives at the Raft ingress. The old
        // processor high-water cursors reject valid lower ids from independent
        // Kafka partitions, so they must not be enabled here.
        dedup_commands: false,
        command_id_window,
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
    let minimum_free_bytes = std::env::var("TC_RAFT_MIN_FREE_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1 << 30);
    match filesystem_available_bytes(&data_dir) {
        Ok(bytes) => metrics.set_storage_free_bytes(bytes, minimum_free_bytes),
        Err(error) => {
            metrics.set_storage_free_bytes(0, minimum_free_bytes.max(1));
            log_error!("raft-node", "event=storage_probe_failed error={error}");
        }
    }
    // Single-group deployment: register one commit-latency series (group 0).
    // A future split-matching topology registers one per group here.
    metrics.register_raft_commit_groups(1);
    // Keep result publication independently switchable for staged capacity
    // tests. The durable execution outbox is still written and fsynced when
    // publication is disabled, so this isolates Raft/WAL/matching without
    // weakening crash recovery.
    let execution_publish_enabled = std::env::var("TC_EXECUTION_PUBLISH_ENABLED")
        .map(|value| {
            !["0", "false", "no", "off"]
                .iter()
                .any(|disabled| value.eq_ignore_ascii_case(disabled))
        })
        .unwrap_or(true);
    let execution_kafka_brokers = execution_publish_enabled
        .then(|| std::env::var("TC_EXECUTION_KAFKA_BROKERS").ok())
        .flatten();
    metrics
        .execution_outbox_publish_healthy
        .store(execution_kafka_brokers.is_none() as u64, Ordering::Release);
    let running = Arc::new(AtomicBool::new(true));
    let accepting = Arc::new(AtomicBool::new(false));
    let leadership = Arc::new(AtomicBool::new(false));
    // Bounded: a runtime that falls behind slows the peers down (TCP
    // backpressure) instead of queueing their messages without limit.
    let (message_tx, message_rx) = mpsc::sync_channel(65_536);
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
    let asset_root = data_dir.join("journal").join("assets");
    let runtime_snapshot_path = engine_snapshot_path.clone();
    let runtime_rejoin_marker_path = rejoin_marker_path.clone();
    let runtime_asset_root = asset_root.clone();
    let runtime_data_dir = data_dir.clone();
    // Migration steps proposed by the admin routes below: (entry, reply with
    // the committed index). Only a leader proposes; elsewhere the reply
    // channel is dropped and the caller is told to ask the leader.
    let (admin_tx, admin_rx) = mpsc::channel::<(Vec<u8>, mpsc::SyncSender<u64>)>();
    thread::Builder::new()
        .name("raft-runtime".into())
        .spawn(move || {
            let mut node = raft_node;
            let transport = PeerTransport::spawn(runtime_peers, runtime_metrics.clone());
            let mut pending = Vec::new();
            let mut commit_waiters: BTreeMap<u64, Vec<mpsc::SyncSender<u64>>> = BTreeMap::new();
            let mut commit_started: BTreeMap<u64, Instant> = BTreeMap::new();
            let mut committed_ids = CommittedIdWindow::new(dedup_max_ids);
            let mut was_leader = false;
            let mut last_announce = Instant::now();
            // Whether this leadership already proposed the group's
            // incarnation (see below).
            let mut incarnation_proposed = false;
            let mut last_tick = Instant::now();
            let mut last_storage_probe = Instant::now() - Duration::from_secs(1);
            // Compaction probe: the matching snapshot's applied index, cached
            // against the file's (mtime, len) and re-checked at most once a
            // second. Decoding the snapshot (every resting order) on each 2 ms
            // loop pass cost ~40% of a core per group under load.
            let mut compaction_probe: Option<(std::time::SystemTime, u64, u64)> = None;
            let mut last_compaction_check = Instant::now();
            // Fencing: committed terms are monotonic within a member's stream.
            // A regression would mean a stale leader's entry reached apply, so
            // refuse to matching it. route_version is reserved for the future
            // split-matching topology (single group => 0 today).
            let mut fence_term = 0u64;
            // Raft log compaction cadence. Once the durably-applied contiguous
            // prefix has grown by this many entries past the last snapshot, the
            // consensus log prefix (in memory and on the WAL) is folded into a
            // snapshot point — but only when that snapshot index is already
            // covered by a durable matching snapshot AND the execution outbox is
            // drained and healthy (see the compaction guard below), so it never
            // races the engine's own snapshot/journal maintenance. Defaults to
            // 1000 entries. Note the default is enabled: a value of 0 is an
            // explicit "compact as soon as any applied prefix exists" threshold,
            // not "disabled" — the guard's other conditions still apply.
            let compact_threshold = std::env::var("TC_RAFT_COMPACT_APPLIED_THRESHOLD")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1_000);
            let applied_batches = asset_log::load_applied_batches(&asset_root)
                .expect("load applied Raft batch watermarks");
            let applied_proofs = asset_log::load_applied_batch_proofs(&asset_root)
                .expect("load exact Raft replay proofs");
            runtime_metrics.set_raft_enqueued_index(engine_snapshot_index);
            runtime_metrics.set_raft_applied_index(engine_snapshot_index);

            // Rebuild exact idempotency and matching state before this member
            // can campaign or accept a retry. Otherwise a fast election can
            // race the recovered committed prefix and append an old command a
            // second time.
            for committed in recovered_committed {
                let index = committed.index;
                assert!(
                    committed.term >= fence_term,
                    "fencing violation: committed term regressed from {fence_term} to {} at index {index}",
                    committed.term
                );
                fence_term = committed.term;
                let payload = committed.data;
                if let Some((fingerprint, description)) = wire::decode_raft_config_entry(&payload) {
                    remember_group_config(&group_config_path, &mut group_config, fingerprint);
                    // Replaying history: the group may have been restarted
                    // with a new config since (a coordinated change), so an
                    // old announcement is only reported. Live ones are binding.
                    if fingerprint != config_fingerprint {
                        log_warn!(
                            "raft-node",
                            "event=historic_config_differs index={index} logged={fingerprint:016x} ({description}) local={config_fingerprint:016x}"
                        );
                    }
                    mark_non_command_applied(&runtime_metrics, index);
                    continue;
                }
                // Every replica holds the same bytes, so skipping an entry
                // that does not decode is deterministic; panicking on it would
                // crash-loop the whole group on every restart.
                let Some(commands) = wire::decode_raft_entry(&payload) else {
                    log_error!("raft-node", "event=undecodable_entry index={index} bytes={}; skipped", payload.len());
                    mark_non_command_applied(&runtime_metrics, index);
                    continue;
                };
                if index <= engine_snapshot_index {
                    committed_ids.remember_entry(&commands, index);
                    runtime_metrics.set_raft_enqueued_index(index);
                    runtime_metrics.set_raft_applied_index(index);
                    continue;
                }
                // Every committed command goes to the engine, whose replicated
                // dedupe window rejects retries identically on every replica.
                // `committed_ids` only lets a leader skip re-proposing them.
                committed_ids.remember_entry(&commands, index);
                let apply = commands;
                if apply.is_empty() {
                    runtime_metrics.set_raft_enqueued_index(index);
                    runtime_metrics.set_raft_applied_index(index);
                    continue;
                }
                let mut pending_command = trade_core::Command::Batch(apply);
                loop {
                    let result = if applied_batches.contains(&index) {
                        let proof = *applied_proofs.get(&index).unwrap_or_else(|| {
                            panic!(
                                "applied Raft index {index} is newer than snapshot {engine_snapshot_index} but has no exact replay proof"
                            )
                        });
                        gateway.submit_recovered(proof, pending_command)
                    } else {
                        gateway.submit_committed(index, pending_command)
                    };
                    match result {
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
                    // Proposals only enter through this node's own intake; a
                    // peer frame carrying one would put arbitrary bytes in
                    // the log. Anything raft refuses (a stray or local
                    // message type, an unknown peer) is dropped, not fatal.
                    // Leadership transfer is never initiated by this system;
                    // from the wire it would only make the leader drop
                    // proposals mid-transfer.
                    if matches!(
                        message.get_msg_type(),
                        raft::eraftpb::MessageType::MsgPropose | raft::eraftpb::MessageType::MsgTransferLeader
                    ) {
                        log_warn!("raft-node", "event=peer_message_dropped type={:?} from={}", message.get_msg_type(), message.from);
                        continue;
                    }
                    let was_rejoining = node.is_rejoining();
                    if let Err(error) = node.step(message) {
                        log_warn!("raft-node", "event=step_failed error={error:?}");
                    } else if was_rejoining && !node.is_rejoining() {
                        match clear_rejoin_marker(&runtime_rejoin_marker_path) {
                            Ok(()) => log_info!(
                                "raft-node",
                                "event=rejoin_complete local_last={} local_commit={}",
                                node.last_index(),
                                node.commit_index()
                            ),
                            Err(error) => log_error!(
                                "raft-node",
                                "event=rejoin_marker_clear_failed marker={} error={error}",
                                runtime_rejoin_marker_path.display()
                            ),
                        }
                    }
                }
                if let Some((index, bytes)) = node.take_installed_snapshot_with_index() {
                    runtime_accepting.store(false, Ordering::Release);
                    runtime_metrics.set_ready(false);
                    trade_core::snapshot::install_bytes(
                        &runtime_snapshot_path,
                        &bytes,
                        index,
                    )
                    .expect("atomically install matching snapshot received from Raft");
                    log_info!(
                        "raft-node",
                        "installed matching snapshot at index {index}; restarting state machine"
                    );
                    // The live Processor cannot be replaced underneath its
                    // lock-free shard. Stop the whole process cleanly (the
                    // shutdown path also wakes the ingress accept loop); the
                    // supervisor restarts it, and it loads the installed
                    // snapshot before reopening the compacted Raft tail.
                    runtime_running.store(false, Ordering::Release);
                    SHUTDOWN.store(true, Ordering::SeqCst);
                    return;
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
                // A request can still arrive after the step-down pass above
                // (the intake saw `accepting` just before it flipped). Never
                // keep it: proposing it in a later term would apply it long
                // after its caller timed out and resent it to the new leader.
                // Dropping the reply channel fails the caller, which rewinds.
                if !leader && !pending.is_empty() {
                    pending.clear();
                }
                // A new leader announces its deterministic config first; every
                // replica checks it as it applies the entry (see below).
                if leader && !was_leader && config_check {
                    // A replica whose config differs from the group's last
                    // announcement must not lead: announcing its own would make
                    // every correctly configured follower stop. Step out (exit)
                    // so a correctly configured replica is elected.
                    if group_config.is_some_and(|group| group != config_fingerprint) && !config_adopt {
                        log_error!(
                            "raft-node",
                            "event=config_mismatch_refuse_leadership group={:016x} local={config_fingerprint:016x} ({config_description}); set TC_RAFT_CONFIG_ADOPT=1 on every replica for a coordinated change",
                            group_config.unwrap_or(0)
                        );
                        std::process::exit(78);
                    }
                    last_announce = Instant::now() - announce_every;
                }
                if leader && !was_leader {
                    incarnation_proposed = false;
                }
                // A group without an incarnation yet (a new or rebuilt one):
                // its leader assigns one once it has applied everything
                // committed, so an existing assignment is never missed. Two
                // proposals racing (a leadership change) are harmless: the
                // first in log order sticks on every replica.
                if leader
                    && !incarnation_proposed
                    && runtime_metrics.group_incarnation.load(Ordering::Acquire) == 0
                    && runtime_metrics.raft_applied_index.load(Ordering::Acquire)
                        >= node.commit_index()
                {
                    incarnation_proposed = true;
                    let entry = wire::encode_raft_incarnation_entry(new_incarnation_id(node.id()));
                    if let Err(error) = node.propose(entry) {
                        incarnation_proposed = false;
                        log_warn!("raft-node", "event=incarnation_propose_failed error={error:?}");
                    }
                }
                // Announce on election and then periodically, so a replica that
                // joins (or comes back with a new disk) meets an announcement
                // in the live log within one period.
                if leader && config_check && last_announce.elapsed() >= announce_every {
                    last_announce = Instant::now();
                    let entry = wire::encode_raft_config_entry(config_fingerprint, &config_description);
                    if let Err(error) = node.propose(entry) {
                        log_warn!("raft-node", "event=config_announce_failed error={error:?}");
                    }
                }
                was_leader = leader;
                if last_storage_probe.elapsed() >= Duration::from_secs(1) {
                    last_storage_probe = Instant::now();
                    match filesystem_available_bytes(&runtime_data_dir) {
                        Ok(bytes) => runtime_metrics
                            .set_storage_free_bytes(bytes, minimum_free_bytes),
                        Err(error) => {
                            runtime_metrics
                                .set_storage_free_bytes(0, minimum_free_bytes.max(1));
                            log_error!(
                                "raft-node",
                                "event=storage_probe_failed error={error}"
                            );
                        }
                    }
                }
                runtime_leadership.store(leader, Ordering::Release);
                runtime_metrics.set_raft_state(
                    leader as u64 * 2,
                    node.term(),
                    node.leader_id(),
                    node.commit_index(),
                );
                let ready = node.leader_id() != 0
                    && !node.is_rejoining()
                    && runtime_metrics.raft_apply_lag() <= max_apply_lag
                    && runtime_metrics.storage_healthy()
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
                while let Ok((entry, reply)) = admin_rx.try_recv() {
                    if !leader {
                        continue; // dropping `reply` fails the caller
                    }
                    match node.propose(entry) {
                        Ok(()) => {
                            let index = node.last_index();
                            commit_waiters.entry(index).or_default().push(reply);
                        }
                        Err(error) => {
                            log_warn!("raft-node", "event=migration_propose_failed error={error:?}");
                        }
                    }
                }
                if leader && !pending.is_empty() {
                    let requests = std::mem::take(&mut pending);
                    let mut entries = Vec::new();
                    let mut waiters = Vec::new();
                    for request in requests {
                        if request.received.elapsed() > PROPOSAL_MAX_AGE {
                            continue; // dropping `committed` fails the caller
                        }
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
                        // Raft may refuse proposals while still leader (a
                        // leadership transfer): the refused requests' waiters
                        // are dropped, which fails their callers (they rewind
                        // and resend), instead of aborting the leader.
                        let (indexes, refused) = node.propose_each(entries);
                        if let Some(error) = refused {
                            log_warn!("raft-node", "event=proposal_refused accepted={} refused={} error={error:?}", indexes.len(), waiters.len() - indexes.len());
                        }
                        for (index, waiter) in indexes.into_iter().zip(waiters) {
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
                    if let Some(started) = commit_started.remove(&index) {
                        let commit_ns = started.elapsed().as_nanos() as u64;
                        runtime_metrics.record_raft_commit_latency(commit_ns);
                        runtime_metrics
                            .record_latency_hist(LatencyMetric::RaftCommit, commit_ns);
                        runtime_metrics.record_raft_commit_latency_group(0, commit_ns);
                    }
                    let waiters = commit_waiters.remove(&index);
                    if let Some((fingerprint, description)) = wire::decode_raft_config_entry(&payload) {
                        remember_group_config(&group_config_path, &mut group_config, fingerprint);
                        if fingerprint != config_fingerprint && config_check {
                            // Applying further would compute results that differ
                            // from the leader's: stop instead (EX_CONFIG).
                            log_error!(
                                "raft-node",
                                "event=config_mismatch index={index} leader={fingerprint:016x} ({description}) local={config_fingerprint:016x} ({config_description}); refusing to apply"
                            );
                            std::process::exit(78);
                        }
                        mark_non_command_applied(&runtime_metrics, index);
                        continue;
                    }
                    let Some(commands) = wire::decode_raft_entry(&payload) else {
                        log_error!("raft-node", "event=undecodable_entry index={index} bytes={}; skipped", payload.len());
                        mark_non_command_applied(&runtime_metrics, index);
                        continue;
                    };
                    // Every committed command goes to the engine, whose
                    // replicated dedupe window rejects retries identically on
                    // every replica (see the recovery loop above).
                    committed_ids.remember_entry(&commands, index);
                    let apply = commands;
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
                // No `pending == 0` gate: a group under sustained load always
                // has a few records in flight, so it would never compact.
                // `execution_outbox_drained_through` below is what keeps
                // unpublished entries out of a compaction.
                if compact_threshold > 0
                    && runtime_metrics
                        .execution_outbox_publish_healthy
                        .load(Ordering::Acquire)
                        == 1
                    && last_compaction_check.elapsed() >= Duration::from_secs(1)
                {
                    last_compaction_check = Instant::now();
                    let snapshot_index = std::fs::metadata(&runtime_snapshot_path)
                        .and_then(|meta| Ok((meta.modified()?, meta.len())))
                        .ok()
                        .and_then(|(modified, len)| match compaction_probe {
                            Some((cached_modified, cached_len, index))
                                if cached_modified == modified && cached_len == len =>
                            {
                                Some(index)
                            }
                            _ => {
                                let index = trade_core::snapshot::load(&runtime_snapshot_path)
                                    .ok()?
                                    .raft_applied_index;
                                compaction_probe = Some((modified, len, index));
                                Some(index)
                            }
                        });
                    if let Some(index) = snapshot_index {
                        if index <= node.applied_index()
                            && index
                                >= node
                                    .snapshot_index()
                                    .saturating_add(compact_threshold)
                        {
                            // Read the file once and take the index from those
                            // same bytes: the background snapshot writer can
                            // rename a newer file in between, and compacting at
                            // the probed index with the newer blob stored a
                            // snapshot whose label and contents disagreed (it
                            // then failed to install on restart).
                            match std::fs::read(&runtime_snapshot_path).and_then(|bytes| {
                                let stored = trade_core::snapshot::raft_applied_index_of(&bytes)?;
                                if stored > node.applied_index() {
                                    return Ok(()); // newer than applied: next pass
                                }
                                // Never fold entries whose events are not all on
                                // Kafka: a follower that installs this snapshot
                                // has no outbox records for them, so if it took
                                // over it could never publish them. The pending
                                // gauge alone was a stale, once-per-pass reading.
                                let drained = runtime_metrics
                                    .execution_outbox_drained_through
                                    .load(Ordering::Acquire);
                                if stored > drained {
                                    return Ok(()); // not yet published that far
                                }
                                node.compact(stored, bytes)?;
                                // Replay never goes below the compacted
                                // snapshot: its application proofs are done.
                                trade_core::asset_log::prune_applied_proofs(&runtime_asset_root, stored)
                                    .map(|_| ())
                            }) {
                                Ok(()) => log_info!(
                                    "raft-node",
                                    "compacted Raft WAL through matching snapshot index {index}"
                                ),
                                Err(error) => log_warn!(
                                    "raft-node",
                                    "event=raft_compaction_failed index={index} error={error}"
                                ),
                            }
                        }
                    }
                }
                thread::sleep(Duration::from_millis(2));
            }
        })
        .expect("spawn raft runtime");

    // Serve route-migration queries off the same admin port:
    //  * `GET /fingerprint?category=N` answers the per-category matching-state
    //    fingerprint (and the applied index it was taken at) from the durable
    //    engine snapshot, so the control plane can verify a cutover against real
    //    state on both the source and target groups;
    //  * `GET /applied-index` answers this node's current live Raft applied
    //    index, so the control plane can auto-capture a migration's freeze index
    //    from the source group instead of an operator hand-supplying it.
    let fingerprint_snapshot_path = engine_snapshot_path.clone();
    let migration_snapshot_path = engine_snapshot_path.clone();
    let snapshot_trigger = handle.snapshot_trigger();
    let category_size = std::env::var("TC_ORDER_CATEGORY_SIZE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|size| *size > 0)
        .unwrap_or(trade_core::sharding::DEFAULT_ASSET_CATEGORY_SIZE);
    let applied_index_metrics = metrics.clone();
    let fingerprint_route: trade_core::metrics::ExtraRoute = Box::new(move |line: &str| {
        if let Some(response) = migration_route(
            line,
            &migration_snapshot_path,
            category_size,
            &admin_tx,
            snapshot_trigger.as_ref(),
        ) {
            return Some(response);
        }
        if trade_core::migration::is_applied_index_request(line) {
            let index = applied_index_metrics
                .raft_applied_index
                .load(std::sync::atomic::Ordering::Acquire);
            return Some((
                "200 OK".to_string(),
                "application/json".to_string(),
                trade_core::migration::applied_index_response_json(index),
            ));
        }
        let category = match trade_core::migration::parse_fingerprint_request(line)? {
            Ok(category) => category,
            Err(reason) => {
                return Some((
                    "400 Bad Request".to_string(),
                    "text/plain".to_string(),
                    format!("{reason}\n"),
                ));
            }
        };
        let response = match trade_core::migration::snapshot_category_fingerprint(
            &fingerprint_snapshot_path,
            category,
            category_size,
        ) {
            Ok(Some(fp)) => (
                "200 OK".to_string(),
                "application/json".to_string(),
                trade_core::migration::fingerprint_response_json(category, fp),
            ),
            // No snapshot yet: the node cannot answer, so the verifier must
            // wait rather than treat an absent value as agreement.
            Ok(None) => (
                "503 Service Unavailable".to_string(),
                "text/plain".to_string(),
                "no durable snapshot yet\n".to_string(),
            ),
            Err(error) => (
                "500 Internal Server Error".to_string(),
                "text/plain".to_string(),
                format!("snapshot fingerprint failed: {error}\n"),
            ),
        };
        Some(response)
    });
    trade_core::metrics::serve_with_extra(metrics_addr, metrics.clone(), Some(fingerprint_route));
    let listener = TcpListener::bind(&order_addr).expect("bind order listener");
    let md_listener = TcpListener::bind(md_addr).expect("bind market-data fanout");
    let execution_topic =
        std::env::var("TC_EXECUTION_KAFKA_TOPIC").unwrap_or_else(|_| "trade-executions-v1".into());
    let execution_producer = execution_kafka_brokers.map(|brokers| {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            // A permanent produce error raises a fatal error instead of
            // skipping messages and renumbering the rest, which would let
            // later events of an order reach the topic before earlier ones.
            .set("enable.gapless.guarantee", "true")
            .set(
                "max.in.flight.requests.per.connection",
                std::env::var("TC_EXECUTION_KAFKA_MAX_IN_FLIGHT").unwrap_or_else(|_| "5".into()),
            )
            .set(
                "linger.ms",
                std::env::var("TC_EXECUTION_KAFKA_LINGER_MS").unwrap_or_else(|_| "2".into()),
            )
            .set(
                "batch.num.messages",
                std::env::var("TC_EXECUTION_KAFKA_BATCH_MESSAGES")
                    .unwrap_or_else(|_| "10000".into()),
            )
            .set(
                "compression.type",
                std::env::var("TC_EXECUTION_KAFKA_COMPRESSION").unwrap_or_else(|_| "lz4".into()),
            )
            .set(
                "queue.buffering.max.messages",
                std::env::var("TC_EXECUTION_KAFKA_QUEUE_MESSAGES")
                    .unwrap_or_else(|_| "2000000".into()),
            )
            .set(
                "queue.buffering.max.kbytes",
                std::env::var("TC_EXECUTION_KAFKA_QUEUE_KBYTES")
                    .unwrap_or_else(|_| "1048576".into()),
            )
            // A message that times out is skipped (the idempotent producer
            // bumps its epoch past it) while later ones may still land, so
            // the resend arrives out of order. Outlast a broker failover, but
            // not by much: the publisher blocks for this long, with health and
            // pending gauges frozen at their last values.
            .set(
                "message.timeout.ms",
                std::env::var("TC_EXECUTION_KAFKA_DELIVERY_TIMEOUT_MS")
                    .unwrap_or_else(|_| "15000".into()),
            );
        let producer = config
            .create::<FutureProducer>()
            .expect("create execution Kafka producer");
        (brokers, config, producer)
    });
    if let Some((brokers, producer_config, producer)) = execution_producer {
        let checkpoint_topic = std::env::var("TC_EXECUTION_CURSOR_TOPIC")
            .unwrap_or_else(|_| "trade-execution-outbox-cursors-v1".into());
        let checkpoint_partitions = std::env::var("TC_EXECUTION_CURSOR_PARTITIONS")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(16)
            .max(1);
        spawn_execution_outbox_publisher(
            execution_outbox_dir,
            execution_topic.clone(),
            checkpoint_topic,
            checkpoint_partitions,
            brokers,
            producer_config,
            producer,
            trade_core::sharding::RouteConfig::from_env(),
            raft_group_id,
            id,
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
        gateway::CommittedServerConfig {
            listener,
            md_listener: Some(md_listener),
            md_group: raft_group_id,
            metrics: Some(metrics.clone()),
            sink,
            running: running.clone(),
            max_cmds_per_sec: 0,
        },
        move |commands| {
            // Bounded: a batch held here while this node is not a ready
            // leader would otherwise be proposed whenever it becomes one,
            // possibly hours after its caller gave up and resent it.
            let deadline = Instant::now() + INGRESS_LEADER_WAIT;
            while !accepting.load(Ordering::Acquire) {
                if !ingress_running.load(Ordering::Acquire) || Instant::now() >= deadline {
                    return None;
                }
                thread::sleep(Duration::from_millis(2));
            }
            let (committed_tx, committed_rx) = mpsc::sync_channel(1);
            proposal_tx
                .send(ProposalRequest {
                    commands,
                    committed: committed_tx,
                    received: Instant::now(),
                })
                .ok()?; // the runtime stopped
            committed_rx.recv_timeout(INGRESS_COMMIT_WAIT).ok()
        },
        move |_event| {},
    )
    .expect("serve committed gateway");
    handle.shutdown();
}

#[allow(clippy::too_many_arguments)] // One startup boundary; fields map directly to publisher durability knobs.
fn spawn_execution_outbox_publisher(
    root: PathBuf,
    topic: String,
    checkpoint_topic: String,
    checkpoint_partitions: i32,
    brokers: String,
    producer_config: ClientConfig,
    mut producer: FutureProducer,
    routing: trade_core::sharding::RouteConfig,
    raft_group: u32,
    node_id: u64,
    running: Arc<AtomicBool>,
    metrics: Arc<trade_core::metrics::Metrics>,
    leadership: Arc<AtomicBool>,
) {
    thread::Builder::new()
        .name("execution-outbox-publisher".into())
        .spawn(move || {
            let mut readers: HashMap<PathBuf, trade_core::execution_outbox::ExecutionOutboxReader> =
                HashMap::new();
            let checkpoint_partition = (raft_group % checkpoint_partitions as u32) as i32;
            let checkpoint_consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set(
                    "group.id",
                    format!("raft-outbox-cursor-{raft_group}-{node_id}"),
                )
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .expect("create execution cursor consumer");
            let mut assignment = TopicPartitionList::new();
            assignment
                .add_partition_offset(&checkpoint_topic, checkpoint_partition, Offset::Beginning)
                .expect("assign execution cursor partition");
            checkpoint_consumer
                .assign(&assignment)
                .expect("start execution cursor consumer");
            // The group's broker-acknowledged publish position: every record
            // at or before this (raft_index, ordinal) is on Kafka. A logical
            // position, not a byte offset: replica outboxes differ in layout.
            let mut shared_position: Option<(u64, u32)> = None;
            let mut pending_checkpoint: Option<(u64, u32)> = None;
            // The newest shared checkpoint read, with the group incarnation
            // it belongs to. One from another incarnation (a group rebuilt
            // from empty logs while Kafka kept the old cursor) names
            // positions of the old log: acknowledging through it would mark
            // the new log's first entries published without sending them.
            let mut latest_cursor: Option<((u64, u32), u64)> = None;
            let mut batches_since_checkpoint = 0usize;
            let checkpoint_every_batches = std::env::var("TC_EXECUTION_CURSOR_EVERY_BATCHES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(16)
                .max(1);
            let batch_size = std::env::var("TC_EXECUTION_PUBLISH_BATCH")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(4_096)
                .clamp(1, 10_000);
            let execution_partitions = std::env::var("TC_EXECUTION_KAFKA_PARTITIONS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(16)
                .max(1);
            let idle_sleep = Duration::from_millis(
                std::env::var("TC_EXECUTION_PUBLISH_IDLE_MS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(1),
            );
            while running.load(Ordering::Acquire) {
                // Read before looking at the outbox: the records of every entry
                // at or below this index were appended before it was applied,
                // so if nothing is pending below, all of them are published.
                let applied_before_pass = metrics.raft_applied_index.load(Ordering::Acquire);
                let mut published_any = false;
                // Any publish, checkpoint or cursor failure in this pass.
                let mut pass_failed = false;
                while let Some(message) = checkpoint_consumer.poll(Duration::ZERO) {
                    match message {
                        Ok(message) => {
                            // Byte-offset checkpoints from older builds are
                            // ignored: a promoted node then republishes from
                            // its own cursor (duplicates, deduped downstream).
                            if let Some((position, incarnation)) = decode_outbox_checkpoint(
                                raft_group,
                                message.key(),
                                message.payload(),
                            ) {
                                latest_cursor = match latest_cursor {
                                    Some((seen, same)) if same == incarnation => {
                                        Some((seen.max(position), incarnation))
                                    }
                                    _ => Some((position, incarnation)),
                                };
                            }
                        }
                        Err(error) => {
                            metrics
                                .execution_outbox_publish_healthy
                                .store(0, Ordering::Release);
                            pass_failed = true;
                            log_warn!("execution-outbox", "event=cursor_poll_failed error={error}");
                        }
                    }
                }
                // Only a cursor of this incarnation counts (0: not assigned
                // yet, so nothing is trusted; at worst a few events go out
                // twice, which consumers deduplicate).
                let incarnation = metrics.group_incarnation.load(Ordering::Acquire);
                if let Some((position, cursor_incarnation)) = latest_cursor {
                    if incarnation != 0 && cursor_incarnation == incarnation {
                        shared_position = shared_position.max(Some(position));
                        // Every event of the entries before the
                        // checkpointed one is on Kafka.
                        metrics
                            .execution_outbox_drained_through
                            .fetch_max(position.0.saturating_sub(1), Ordering::AcqRel);
                    }
                }
                if let Some(position) = pending_checkpoint {
                    let key = outbox_checkpoint_key(raft_group);
                    let value = encode_outbox_checkpoint(position, incarnation);
                    let delivered = futures::executor::block_on(
                        producer.send(
                            FutureRecord::to(&checkpoint_topic)
                                .partition(checkpoint_partition)
                                .key(&key)
                                .payload(&value),
                            Duration::from_secs(5),
                        ),
                    );
                    if delivered.is_ok() {
                        pending_checkpoint = None;
                        shared_position = shared_position.max(Some(position));
                    } else {
                        metrics
                            .execution_outbox_publish_healthy
                            .store(0, Ordering::Release);
                        pass_failed = true;
                    }
                }
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
                                log_error!(
                                    "execution-outbox",
                                    "event=open_failed path={} error={error}",
                                    path.display()
                                );
                            }
                        }
                    }
                }
                readers.retain(|path, _| path.exists());
                // Every replica follows the broker-acknowledged cursor, so
                // followers can reclaim sealed segments and a promoted leader
                // does not replay the full lifetime outbox.
                if let Some(position) = shared_position {
                    for reader in readers.values_mut() {
                        // Records a follower has not applied yet simply stay
                        // pending; it catches up on a later pass.
                        let _ = reader.acknowledge_through(position);
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
                // A follower's pending count is measured against the leader's
                // broker-acknowledged position, so zero means published there too.
                if pending_before_publish == 0 {
                    metrics
                        .execution_outbox_drained_through
                        .fetch_max(applied_before_pass, Ordering::AcqRel);
                }
                let segments = readers
                    .iter()
                    .map(|(path, reader)| {
                        (path.clone(), reader.pending_records().unwrap_or(u64::MAX))
                    })
                    .collect::<Vec<_>>();
                for path in select_publish_segments(segments) {
                    let Some(reader) = readers.get_mut(&path) else {
                        continue;
                    };
                    if !leadership.load(Ordering::Acquire) {
                        continue;
                    }
                    let pending_in_segment = reader.pending_records().unwrap_or(u64::MAX);
                    match reader.read_batch(batch_size) {
                        Ok(records) if !records.is_empty() => {
                            let publish_started = std::time::Instant::now();
                            let prepared = records
                                .iter()
                                .flat_map(|record| {
                                    record
                                        .target_order_ids()
                                        .into_iter()
                                        .map(|target_order_id| {
                                            let partition = routing.execution_partition(
                                                target_order_id,
                                                execution_partitions,
                                            )
                                                as i32;
                                            (
                                                target_order_id.to_be_bytes(),
                                                partition,
                                                record.kafka_payload_for(target_order_id),
                                            )
                                        })
                                })
                                .collect::<Vec<_>>();
                            let deliveries =
                                futures::executor::block_on(futures::future::join_all(
                                    prepared.iter().map(|(key, partition, payload)| {
                                        producer.send(
                                            FutureRecord::to(&topic)
                                                .partition(*partition)
                                                .key(key)
                                                .payload(payload),
                                            Duration::from_secs(5),
                                        )
                                    }),
                                ));
                            if deliveries.iter().all(Result::is_ok) {
                                published_any = true;
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
                                let last = records.last().expect("non-empty batch");
                                batches_since_checkpoint += 1;
                                let checkpoint_due = batches_since_checkpoint
                                    >= checkpoint_every_batches
                                    || pending_in_segment <= records.len() as u64;
                                if checkpoint_due {
                                    pending_checkpoint = pending_checkpoint
                                        .max(Some((last.raft_index, last.ordinal)));
                                    batches_since_checkpoint = 0;
                                }
                                if let Err(error) = reader.acknowledge(records.len()) {
                                    metrics
                                        .execution_outbox_publish_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                    metrics
                                        .execution_outbox_publish_healthy
                                        .store(0, Ordering::Release);
                                    pass_failed = true;
                                    log_error!(
                                        "execution-outbox",
                                        "event=cursor_failed error={error}"
                                    );
                                } else {
                                    // Segments publish oldest first and in
                                    // (index, ordinal) order, so every entry
                                    // before the last acknowledged one is
                                    // fully on Kafka, even while newer
                                    // records keep `pending` above zero.
                                    metrics.execution_outbox_drained_through.fetch_max(
                                        last.raft_index.saturating_sub(1),
                                        Ordering::AcqRel,
                                    );
                                    metrics
                                        .execution_outbox_published
                                        .fetch_add(prepared.len() as u64, Ordering::Relaxed);
                                    metrics.execution_outbox_publish_healthy.store(
                                        pending_checkpoint.is_none() as u64,
                                        Ordering::Release,
                                    );
                                }
                            } else {
                                metrics
                                    .execution_outbox_publish_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                metrics
                                    .execution_outbox_publish_healthy
                                    .store(0, Ordering::Release);
                                pass_failed = true;
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
                                .store(pending_checkpoint.is_none() as u64, Ordering::Release);
                        }
                        Err(error) => {
                            metrics
                                .execution_outbox_publish_failures
                                .fetch_add(1, Ordering::Relaxed);
                            metrics
                                .execution_outbox_publish_healthy
                                .store(0, Ordering::Release);
                            pass_failed = true;
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
                // Refresh health once per pass. It used to be refreshed only
                // while a segment was being read; with drained segments now
                // skipped, an idle leader would otherwise keep the 0 written
                // while its last checkpoint was still in flight — and stay
                // not-ready forever.
                if leadership.load(Ordering::Acquire) && !pass_failed {
                    metrics
                        .execution_outbox_publish_healthy
                        .store(pending_checkpoint.is_none() as u64, Ordering::Release);
                }
                // A gapless producer stops for good after a produce error that
                // would have skipped messages; the unacknowledged batch is
                // still in the outbox, so a fresh producer resends it in order.
                if pass_failed {
                    if let Some((code, reason)) = producer.client().fatal_error() {
                        log_warn!(
                            "execution-kafka",
                            "event=producer_recreated fatal={code:?} reason={reason}"
                        );
                        match producer_config.create::<FutureProducer>() {
                            Ok(fresh) => producer = fresh,
                            Err(error) => {
                                log_error!(
                                    "execution-kafka",
                                    "event=producer_recreate_failed error={error}"
                                );
                                thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }
                }
                if !published_any && !idle_sleep.is_zero() {
                    thread::sleep(idle_sleep);
                }
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

/// Which outbox segment each engine shard publishes from this pass: its
/// oldest segment that still has unpublished records (an unreadable pending
/// count counts as pending). One per shard, so a newer segment is never
/// published while an older one still has records.
///
/// Fully drained segments must be skipped, not stopped at: a sealed segment is
/// only deleted when the writer next rotates, which needs new commands — and
/// a group whose outbox backlog exceeds the readiness limit receives none.
/// Stopping at the drained segment wedged the group permanently (the 25,630
/// records in the next segment could never publish, so readiness never
/// returned, so nothing rotated).
fn select_publish_segments(
    mut segments: Vec<(std::path::PathBuf, u64)>,
) -> Vec<std::path::PathBuf> {
    segments.sort_by_key(|(path, _)| outbox_segment_order(path).unwrap_or((u32::MAX, u64::MAX)));
    let mut chosen_shards = HashSet::new();
    segments
        .into_iter()
        .filter(|(_, pending)| *pending > 0)
        .filter_map(|(path, _)| {
            let (shard, _) = outbox_segment_order(&path)?;
            chosen_shards.insert(shard).then_some(path)
        })
        .collect()
}

fn outbox_segment_order(path: &std::path::Path) -> Option<(u32, u64)> {
    let stem = path.file_stem()?.to_str()?.strip_prefix("outbox-shard-")?;
    if let Some((shard, segment)) = stem.split_once("-seg-") {
        Some((shard.parse().ok()?, segment.parse().ok()?))
    } else {
        Some((stem.parse().ok()?, 0))
    }
}

/// Shared publish checkpoint, one per Raft group: key = group (big
/// endian) + `@pos`, value = raft_index (LE u64) + ordinal (LE u32) +
/// group incarnation (LE u64).
const OUTBOX_POSITION_KEY: &[u8] = b"@pos";

fn outbox_checkpoint_key(raft_group: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + OUTBOX_POSITION_KEY.len());
    key.extend_from_slice(&raft_group.to_be_bytes());
    key.extend_from_slice(OUTBOX_POSITION_KEY);
    key
}

fn encode_outbox_checkpoint((raft_index, ordinal): (u64, u32), incarnation: u64) -> [u8; 20] {
    let mut value = [0u8; 20];
    value[..8].copy_from_slice(&raft_index.to_le_bytes());
    value[8..12].copy_from_slice(&ordinal.to_le_bytes());
    value[12..].copy_from_slice(&incarnation.to_le_bytes());
    value
}

/// A checkpoint of `expected_group`: (position, group incarnation); `None`
/// for other groups and anything not in this layout.
fn decode_outbox_checkpoint(
    expected_group: u32,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
) -> Option<((u64, u32), u64)> {
    let (key, payload) = (key?, payload?);
    if key.len() != 4 + OUTBOX_POSITION_KEY.len()
        || &key[4..] != OUTBOX_POSITION_KEY
        || payload.len() != 20
        || u32::from_be_bytes(key[..4].try_into().ok()?) != expected_group
    {
        return None;
    }
    Some((
        (
            u64::from_le_bytes(payload[..8].try_into().ok()?),
            u32::from_le_bytes(payload[8..12].try_into().ok()?),
        ),
        u64::from_le_bytes(payload[12..].try_into().ok()?),
    ))
}

#[cfg(test)]
fn execution_category_key(frame: &[u8; wire::REPORT_LEN], category_size: u32) -> [u8; 4] {
    let instrument = trade_core::InstrumentId(u32::from_le_bytes(
        frame[4..8].try_into().expect("execution instrument"),
    ));
    trade_core::sharding::asset_category(instrument, category_size).to_be_bytes()
}

type RouteResponse = (String, String, String);

fn route_reply(status: &str, body: String) -> RouteResponse {
    let content_type = if body.starts_with('{') {
        "application/json"
    } else {
        "text/plain"
    };
    (status.to_string(), content_type.to_string(), body)
}

/// Admin routes for moving a category between groups (driven by order-api's
/// `/admin/migrate`; see `trade_core::exchange::CategoryOp`):
///
/// * `GET /category-state?category=C` — the category's books, stops and
///   positions from this node's durable snapshot, hex-encoded, with the
///   snapshot's Raft index;
/// * `GET /migration/seal?category=C`, `GET /migration/drop?category=C` —
///   propose the step through this group's log (leader only) and answer the
///   committed index;
/// * `GET /migration/install?category=C&from=HOST:PORT&min_index=N` — pull
///   the category's state from a node of the source group (refusing a
///   snapshot older than `min_index`, i.e. taken before the seal) and
///   propose installing it;
/// * `GET /migration/snapshot` — take a snapshot now.
fn migration_route(
    line: &str,
    snapshot_path: &std::path::Path,
    category_size: u32,
    admin_tx: &mpsc::Sender<(Vec<u8>, mpsc::SyncSender<u64>)>,
    snapshot_trigger: &(dyn Fn() + Send + Sync),
) -> Option<RouteResponse> {
    let target = line.split_whitespace().nth(1)?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let param = |key: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('='))
    };
    let category = || -> Result<u32, RouteResponse> {
        param("category")
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| route_reply("400 Bad Request", "category=<id> required\n".into()))
    };
    // The caller's category size must be ours: with another size it would
    // move a different set of instruments than it routes.
    if let Some(size) = param("size").and_then(|value| value.parse::<u32>().ok()) {
        if size != category_size && path.starts_with("/migration/") {
            return Some(route_reply(
                "409 Conflict",
                format!("category size {size} differs from this node's {category_size} (TC_ORDER_CATEGORY_SIZE)\n"),
            ));
        }
    }
    // The migration's epoch (its freeze record's route-log position);
    // required: an unfenced step could undo a newer migration.
    let epoch = || -> Result<u64, RouteResponse> {
        param("epoch")
            .and_then(|value| value.parse().ok())
            .filter(|epoch| *epoch > 0)
            .ok_or_else(|| route_reply("400 Bad Request", "epoch=<n> (n > 0) required\n".into()))
    };
    let propose = |op: trade_core::exchange::CategoryOp| -> RouteResponse {
        let entry = wire::encode_raft_migration_entry(&op);
        // One Raft message carries the entry: past the limit, followers
        // would refuse it forever and the whole group would stop.
        if entry.len() > max_raft_message_bytes() / 2 {
            return route_reply(
                "413 Payload Too Large",
                format!(
                    "migration entry of {} bytes exceeds half the {}-byte Raft message limit\n",
                    entry.len(),
                    max_raft_message_bytes()
                ),
            );
        }
        let (reply, committed) = mpsc::sync_channel(1);
        if admin_tx.send((entry, reply)).is_err() {
            return route_reply("503 Service Unavailable", "raft runtime stopped\n".into());
        }
        match committed.recv_timeout(Duration::from_secs(15)) {
            Ok(index) => route_reply("200 OK", format!("{{\"index\":{index}}}")),
            Err(_) => route_reply(
                "503 Service Unavailable",
                "not the leader, or not committed in time; ask the leader\n".into(),
            ),
        }
    };
    use trade_core::exchange::CategoryOp;
    Some(match path {
        "/migration/snapshot" => {
            snapshot_trigger();
            route_reply("200 OK", "{\"requested\":true}".into())
        }
        "/migration/seal" => match category().and_then(|c| Ok((c, epoch()?))) {
            Ok((category, epoch)) => propose(CategoryOp::Seal {
                category,
                category_size,
                epoch,
            }),
            Err(response) => response,
        },
        "/migration/drop" => match category().and_then(|c| Ok((c, epoch()?))) {
            Ok((category, epoch)) => propose(CategoryOp::Drop {
                category,
                category_size,
                epoch,
            }),
            Err(response) => response,
        },
        // Undo a seal (a migration that failed before cutover).
        "/migration/unseal" => match category().and_then(|c| Ok((c, epoch()?))) {
            Ok((category, epoch)) => propose(CategoryOp::Unseal {
                category,
                category_size,
                epoch,
            }),
            Err(response) => response,
        },
        // Whether the category is sealed here and the epoch of its last
        // migration step, as of the durable snapshot (the orchestrator
        // checks that its steps applied, not just committed).
        "/migration/status" => match category() {
            Err(response) => response,
            Ok(category) => {
                match trade_core::migration::snapshot_migration_status(snapshot_path, category) {
                    Ok(Some((index, sealed, epoch))) => route_reply(
                        "200 OK",
                        format!(
                        "{{\"raft_applied_index\":{index},\"sealed\":{sealed},\"epoch\":{epoch}}}"
                    ),
                    ),
                    Ok(None) => route_reply(
                        "503 Service Unavailable",
                        "no durable snapshot yet\n".into(),
                    ),
                    Err(error) => route_reply("500 Internal Server Error", format!("{error}\n")),
                }
            }
        },
        "/category-state" => match category() {
            Err(response) => response,
            Ok(category) => match trade_core::snapshot::load(snapshot_path) {
                Err(error) => route_reply(
                    "503 Service Unavailable",
                    format!("no durable snapshot: {error}\n"),
                ),
                Ok(snap) => {
                    let export =
                        trade_core::migration::export_category(&snap, category, category_size);
                    let bytes = trade_core::migration::encode_category_state(
                        snap.raft_applied_index,
                        &export.engines,
                        &export.positions,
                        &export.halted,
                        &export.command_ids,
                    );
                    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
                    route_reply(
                        "200 OK",
                        format!(
                            "{{\"raft_applied_index\":{},\"state\":\"{hex}\"}}",
                            snap.raft_applied_index
                        ),
                    )
                }
            },
        },
        "/migration/install" => {
            let (category, epoch) = match category().and_then(|c| Ok((c, epoch()?))) {
                Ok(pair) => pair,
                Err(response) => return Some(response),
            };
            let (Some(from), Some(min_index)) = (
                param("from"),
                param("min_index").and_then(|v| v.parse::<u64>().ok()),
            ) else {
                return Some(route_reply(
                    "400 Bad Request",
                    "from=<host:port>&min_index=<n> required\n".into(),
                ));
            };
            let fetched = trade_core::migration::http_get(
                from,
                &format!("/category-state?category={category}"),
                Duration::from_secs(10),
            );
            let body = match fetched {
                Ok((200, body)) => body,
                Ok((status, body)) => {
                    return Some(route_reply(
                        "502 Bad Gateway",
                        format!("source answered {status}: {body}\n"),
                    ))
                }
                Err(error) => {
                    return Some(route_reply(
                        "502 Bad Gateway",
                        format!("source unreachable: {error}\n"),
                    ))
                }
            };
            let hex = body
                .split("\"state\":\"")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .unwrap_or("");
            let bytes: Option<Vec<u8>> = (0..hex.len())
                .step_by(2)
                .map(|at| {
                    hex.get(at..at + 2)
                        .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                })
                .collect();
            let state = match bytes.as_deref().map(trade_core::snapshot::decode) {
                Some(Ok(state)) => state,
                _ => {
                    return Some(route_reply(
                        "502 Bad Gateway",
                        "undecodable category state\n".into(),
                    ))
                }
            };
            if state.raft_applied_index < min_index {
                return Some(route_reply(
                    "409 Conflict",
                    format!(
                        "source snapshot at {} predates the seal at {min_index}; snapshot the source and retry\n",
                        state.raft_applied_index
                    ),
                ));
            }
            if let Err(reason) = trade_core::migration::validate_category_state(
                &state.engines,
                &state.positions,
                category,
                category_size,
            ) {
                return Some(route_reply(
                    "422 Unprocessable Entity",
                    format!("refusing to install: {reason}\n"),
                ));
            }
            let command_ids = state
                .recent_command_ids
                .iter()
                .copied()
                .zip(state.recent_command_instruments.iter().copied())
                .collect();
            propose(CategoryOp::Install {
                category,
                category_size,
                epoch,
                engines: state.engines,
                positions: state.positions,
                halted: state.halted,
                command_ids,
            })
        }
        _ => return None,
    })
}

/// Bump when a change alters matching results for the same command log (the
/// replicas of a group must all run builds with the same value).
const DETERMINISM_VERSION: u32 = 20_261_110;

/// Fingerprint (and readable description) of everything that must be equal
/// on every replica of a group for them to compute identical results and
/// publish identically: matching rules, the dedupe window, and the routing
/// that places execution events on Kafka partitions.
fn deterministic_config(instrument_specs: &str, command_id_window: usize) -> (u64, String) {
    let env = |name: &str| std::env::var(name).unwrap_or_default();
    let description = format!(
        "determinism={DETERMINISM_VERSION};specs={};dedup_window={command_id_window};exec_partitions={};routing={:?}",
        instrument_specs.trim(),
        env("TC_EXECUTION_KAFKA_PARTITIONS").trim(),
        trade_core::sharding::RouteConfig::from_env(),
    );
    (
        trade_core::journal::fnv1a(description.as_bytes()),
        description,
    )
}

/// Record the group's latest announced config fingerprint (in memory and,
/// when it changed, durably on disk).
fn remember_group_config(path: &std::path::Path, current: &mut Option<u64>, fingerprint: u64) {
    if *current == Some(fingerprint) {
        return;
    }
    *current = Some(fingerprint);
    let tmp = path.with_extension("tmp");
    let written = std::fs::write(&tmp, format!("{fingerprint:016x}\n"))
        .and_then(|()| std::fs::File::open(&tmp)?.sync_all())
        .and_then(|()| std::fs::rename(&tmp, path));
    if let Err(error) = written {
        log_warn!(
            "raft-node",
            "event=group_config_persist_failed error={error}"
        );
    }
}

/// A random non-zero group incarnation id below 2^53 (exact as a metric).
fn new_incarnation_id(node_id: u64) -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    // RandomState is seeded from the OS's random source.
    let state = std::collections::hash_map::RandomState::new();
    let mut hasher = state.build_hasher();
    (nanos, node_id, std::process::id()).hash(&mut hasher);
    let random = hasher.finish();
    (random & ((1 << 53) - 1)).max(1)
}

fn clear_rejoin_marker(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Take an exclusive, non-blocking `flock` on `<dir>/.raft-node.lock`. The
/// returned file must be kept open for as long as the directory is used.
fn lock_data_dir(dir: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    std::fs::create_dir_all(dir)?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(".raft-node.lock"))?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let error = std::io::Error::last_os_error();
        return Err(std::io::Error::new(
            error.kind(),
            format!("held by another process ({error})"),
        ));
    }
    Ok(file)
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

/// Inbound peer connections at once: a few per peer; more is a misbehaving
/// or hostile client, refused rather than given threads and buffers.
const MAX_PEER_CONNECTIONS: usize = 64;
/// A peer connection silent this long is dead (the leader heartbeats every
/// tick): partitions without a FIN used to leave a reader blocked forever.
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn spawn_listener(addr: String, tx: mpsc::SyncSender<Message>) {
    thread::spawn(move || {
        let listener = TcpListener::bind(&addr).expect("bind raft listener");
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for stream in listener.incoming() {
            let Ok(stream) = stream else {
                thread::sleep(Duration::from_millis(50)); // e.g. out of fds
                continue;
            };
            if active.load(Ordering::Acquire) >= MAX_PEER_CONNECTIONS {
                continue;
            }
            stream.set_read_timeout(Some(PEER_READ_TIMEOUT)).ok();
            let (tx, done) = (tx.clone(), active.clone());
            active.fetch_add(1, Ordering::AcqRel);
            let spawned = thread::Builder::new()
                .name("raft-peer-in".into())
                .spawn(move || {
                    read_peer_frames(stream, &tx);
                    done.fetch_sub(1, Ordering::AcqRel);
                });
            if spawned.is_err() {
                active.fetch_sub(1, Ordering::AcqRel);
            }
        }
    });
}

fn read_peer_frames(mut stream: impl Read, tx: &mpsc::SyncSender<Message>) {
    let limit = max_raft_message_bytes();
    let mut message_bytes = Vec::new();
    loop {
        let mut word = [0u8; 4];
        if stream.read_exact(&mut word).is_err() {
            return;
        }
        let word = u32::from_be_bytes(word);
        let continues = word & FRAME_CONTINUES != 0;
        let size = (word & !FRAME_CONTINUES) as usize;
        if size == 0 || size > PEER_FRAME_BYTES || message_bytes.len() + size > limit {
            return; // not our framing, or a message beyond the limit
        }
        let start = message_bytes.len();
        message_bytes.resize(start + size, 0);
        if stream.read_exact(&mut message_bytes[start..]).is_err() {
            return;
        }
        if continues {
            continue;
        }
        let parsed = Message::parse_from_bytes(&message_bytes);
        message_bytes = Vec::new();
        if let Ok(message) = parsed {
            if tx.send(message).is_err() {
                return;
            }
        }
    }
}

/// `bytes` as peer frames: each at most [`PEER_FRAME_BYTES`], all but the
/// last flagged [`FRAME_CONTINUES`].
fn encode_peer_frames(bytes: &[u8]) -> Vec<u8> {
    let chunks = bytes
        .len()
        .saturating_add(PEER_FRAME_BYTES - 1)
        .checked_div(PEER_FRAME_BYTES)
        .unwrap_or(0)
        .max(1);
    let mut frame = Vec::with_capacity(bytes.len() + 4 * chunks);
    let mut pieces = bytes.chunks(PEER_FRAME_BYTES).peekable();
    while let Some(piece) = pieces.next() {
        let flag = if pieces.peek().is_some() {
            FRAME_CONTINUES
        } else {
            0
        };
        frame.extend_from_slice(&(piece.len() as u32 | flag).to_be_bytes());
        frame.extend_from_slice(piece);
    }
    frame
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
        if bytes.is_empty() || bytes.len() > max_raft_message_bytes() {
            // The receiver would drop the link on it: count it, keep the link.
            log_warn!(
                "raft-node",
                "event=raft_message_too_large type={:?} to={} bytes={} limit={}",
                message.get_msg_type(),
                message.to,
                bytes.len(),
                max_raft_message_bytes()
            );
            metrics
                .raft_transport_dropped
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let frame = encode_peer_frames(&bytes);
        let mut delivered = false;
        for attempt in 0..2 {
            if stream.is_none() {
                match trade_core::migration::connect_timeout(&addr, Duration::from_millis(500)) {
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

    #[test]
    fn non_command_entries_do_not_create_permanent_apply_lag() {
        let metrics = trade_core::metrics::Metrics::default();
        metrics.set_raft_enqueued_index(40);
        metrics.set_raft_applied_index(7);
        mark_non_command_applied(&metrics, 41);
        assert_eq!(metrics.raft_apply_lag(), 0);
        assert_eq!(metrics.raft_applied_index.load(Ordering::Acquire), 41);
    }
    use std::io::Cursor;
    use trade_core::{ExecReport, InstrumentId, OrderId};

    fn category_key(instrument: u32) -> [u8; 4] {
        let mut frame = [0; wire::REPORT_LEN];
        wire::encode_report(
            &ExecReport::Accepted {
                instrument: InstrumentId(instrument),
                order_id: OrderId(instrument as u64),
                user: 0,
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
    fn publisher_moves_past_drained_segments_but_never_ahead_of_pending_ones() {
        let path = |name: &str| std::path::PathBuf::from(format!("/o/{name}"));
        // The wedge: base segment fully published, newer one holds the backlog.
        assert_eq!(
            select_publish_segments(vec![
                (path("outbox-shard-0-seg-0000000001.bin"), 25_630),
                (path("outbox-shard-0.bin"), 0),
            ]),
            vec![path("outbox-shard-0-seg-0000000001.bin")]
        );
        // Ordering: while the older segment has records, the newer one waits.
        assert_eq!(
            select_publish_segments(vec![
                (path("outbox-shard-0-seg-0000000002.bin"), 9),
                (path("outbox-shard-0-seg-0000000001.bin"), 3),
                (path("outbox-shard-0.bin"), 0),
            ]),
            vec![path("outbox-shard-0-seg-0000000001.bin")]
        );
        // One segment per shard, shards independent; unreadable = pending.
        assert_eq!(
            select_publish_segments(vec![
                (path("outbox-shard-1.bin"), u64::MAX),
                (path("outbox-shard-0.bin"), 0),
                (path("outbox-shard-1-seg-0000000001.bin"), 4),
                (path("outbox-shard-0-seg-0000000001.bin"), 0),
            ]),
            vec![path("outbox-shard-1.bin")]
        );
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
    fn a_data_dir_can_be_locked_by_only_one_process_handle() {
        let dir = std::env::temp_dir().join(format!("tc-raft-lock-{}", std::process::id()));
        let first = lock_data_dir(&dir).expect("first lock");
        assert!(
            lock_data_dir(&dir).is_err(),
            "second lock must fail while held"
        );
        drop(first);
        assert!(lock_data_dir(&dir).is_ok(), "released with the handle");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn config_fingerprint_covers_specs_and_the_dedupe_window() {
        let (base, description) = deterministic_config("*:tick=1", 500_000);
        assert!(description.contains("dedup_window=500000"), "{description}");
        assert_ne!(deterministic_config("*:tick=5", 500_000).0, base);
        assert_ne!(deterministic_config("*:tick=1", 400_000).0, base);
        assert_eq!(deterministic_config(" *:tick=1 ", 500_000).0, base);
    }

    #[test]
    fn shared_outbox_checkpoint_is_a_logical_position_scoped_by_group() {
        let key = outbox_checkpoint_key(7);
        let payload = encode_outbox_checkpoint((12_345, 3), 99);
        assert_eq!(
            decode_outbox_checkpoint(7, Some(&key), Some(&payload)),
            Some(((12_345, 3), 99))
        );
        assert_eq!(
            decode_outbox_checkpoint(8, Some(&key), Some(&payload)),
            None
        );
        // Any other layout is ignored, not misread.
        let mut legacy_key = 7u32.to_be_bytes().to_vec();
        legacy_key.extend_from_slice(b"outbox-shard-0.bin");
        assert_eq!(
            decode_outbox_checkpoint(7, Some(&legacy_key), Some(&12_345u64.to_le_bytes())),
            None
        );
    }

    #[test]
    fn outbox_segments_sort_oldest_first_within_each_shard() {
        let base = std::path::Path::new("outbox-shard-9.bin");
        let newer = std::path::Path::new("outbox-shard-9-seg-0000000002.bin");
        assert_eq!(outbox_segment_order(base), Some((9, 0)));
        assert_eq!(outbox_segment_order(newer), Some((9, 2)));
        assert!(outbox_segment_order(base) < outbox_segment_order(newer));
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
        let (tx, rx) = mpsc::sync_channel(16);
        read_peer_frames(Cursor::new(wire), &tx);
        assert_eq!(rx.try_recv().unwrap().from, 1);
        assert_eq!(rx.try_recv().unwrap().from, 2);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn peer_connection_rejects_oversized_frame() {
        let (tx, rx) = mpsc::sync_channel(16);
        read_peer_frames(
            Cursor::new(((PEER_FRAME_BYTES + 1) as u32).to_be_bytes()),
            &tx,
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_message_larger_than_one_frame_is_reassembled() {
        // A snapshot several frames long, then an ordinary message.
        let mut snapshot = Message {
            from: 1,
            to: 2,
            ..Default::default()
        };
        snapshot.mut_snapshot().data = (0..(PEER_FRAME_BYTES * 2 + 12_345))
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>()
            .into();
        let small = Message {
            from: 3,
            to: 2,
            ..Default::default()
        };
        let mut wire = encode_peer_frames(&snapshot.write_to_bytes().unwrap());
        assert!(wire.len() > PEER_FRAME_BYTES * 2);
        wire.extend(encode_peer_frames(&small.write_to_bytes().unwrap()));
        let (tx, rx) = mpsc::sync_channel(16);
        read_peer_frames(Cursor::new(wire), &tx);
        let received = rx.try_recv().unwrap();
        assert_eq!(received.get_snapshot().data, snapshot.get_snapshot().data);
        assert_eq!(rx.try_recv().unwrap().from, 3);
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
