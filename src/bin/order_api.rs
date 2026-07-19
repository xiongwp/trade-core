//! Persistent order system: Kafka is the ordered source of truth and fans out
//! through independent consumer groups to the MySQL query projection and the
//! Raft-backed matcher.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{params, Params, Pool, Transaction, TxOpts, Value};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use trade_core::journal;
use trade_core::metrics::LatencyHistogram;
use trade_core::order::Order;
use trade_core::order_queue::{encode_envelope, QueueEnvelope, QueueRouter};
use trade_core::sharding::{self, DEFAULT_ASSET_CATEGORY_SIZE};
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire;
use trade_core::{log_error, log_info, log_warn};

/// Set by the SIGTERM/SIGINT handler (async-signal-safe: a lone atomic store).
/// The HTTP accept loop observes it and stops taking new connections.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, AtomicOrdering::SeqCst);
}

fn install_signal_handlers() {
    let handler = on_signal as *const () as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

const BATCH_RECORD_LEN: usize = 8 + wire::MSG_LEN;

#[derive(Clone)]
struct MatcherTarget {
    order_addr: String,
    metrics_addr: Option<String>,
}

#[derive(Clone)]
struct OrderStore {
    shards: Arc<Vec<Option<Pool>>>,
    owned_dbs: Arc<std::collections::HashSet<u32>>,
    /// Versioned shard routing record. All row placement goes through this so a
    /// parameter change is a fenced migration (see [`sharding::RouteConfig`]).
    routing: sharding::RouteConfig,
    metrics: Arc<OrderPipelineMetrics>,
}

#[derive(Default)]
struct OrderPipelineMetrics {
    mysql_commit_ns_total: AtomicU64,
    mysql_commit_ns_max: AtomicU64,
    mysql_commit_samples: AtomicU64,
    /// Parallel log-bucketed distribution of MySQL commit latency, backing the
    /// p50/p90/p99 SLO gauges the `*_ns_total`/`*_ns_max` triple cannot answer.
    mysql_commit_hist: LatencyHistogram,
    raft_forward_ns_total: AtomicU64,
    raft_forward_ns_max: AtomicU64,
    raft_forward_samples: AtomicU64,
    execution_mysql_commit_ns_total: AtomicU64,
    execution_mysql_commit_ns_max: AtomicU64,
    execution_mysql_commit_samples: AtomicU64,
    execution_mysql_completed_events: AtomicU64,
    execution_mysql_commit_hist: LatencyHistogram,
    /// Commands currently waiting for Kafka delivery in this API process.
    /// Durable downstream backlog is read from the shared consumer groups.
    inflight_publish_commands: AtomicU64,
    published_commands: AtomicU64,
    mysql_completed_commands: AtomicU64,
    match_completed_commands: AtomicU64,
    backpressure_rejections: AtomicU64,
    observed_mysql_lag: AtomicU64,
    observed_match_lag: AtomicU64,
    dlq_total: AtomicU64,
}

impl OrderPipelineMetrics {
    fn record(total: &AtomicU64, max: &AtomicU64, samples: &AtomicU64, elapsed: Duration) {
        let ns = elapsed.as_nanos() as u64;
        total.fetch_add(ns, AtomicOrdering::Relaxed);
        max.fetch_max(ns, AtomicOrdering::Relaxed);
        samples.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_mysql(&self, elapsed: Duration) {
        Self::record(
            &self.mysql_commit_ns_total,
            &self.mysql_commit_ns_max,
            &self.mysql_commit_samples,
            elapsed,
        );
        self.mysql_commit_hist.record(elapsed.as_nanos() as u64);
    }

    fn record_raft(&self, elapsed: Duration) {
        Self::record(
            &self.raft_forward_ns_total,
            &self.raft_forward_ns_max,
            &self.raft_forward_samples,
            elapsed,
        );
    }

    fn record_execution_mysql_batch(&self, elapsed: Duration, events: u64) {
        Self::record(
            &self.execution_mysql_commit_ns_total,
            &self.execution_mysql_commit_ns_max,
            &self.execution_mysql_commit_samples,
            elapsed,
        );
        self.execution_mysql_commit_hist
            .record(elapsed.as_nanos() as u64);
        self.execution_mysql_completed_events
            .fetch_add(events, AtomicOrdering::Relaxed);
    }

    fn try_reserve(&self, commands: u64, max_backlog: u64) -> Result<(), u64> {
        loop {
            let inflight = self.inflight_publish_commands.load(AtomicOrdering::Acquire);
            let backlog = inflight.saturating_add(
                self.observed_mysql_lag
                    .load(AtomicOrdering::Acquire)
                    .max(self.observed_match_lag.load(AtomicOrdering::Acquire)),
            );
            if backlog.saturating_add(commands) > max_backlog {
                self.backpressure_rejections
                    .fetch_add(commands, AtomicOrdering::Relaxed);
                return Err(backlog);
            }
            if self
                .inflight_publish_commands
                .compare_exchange_weak(
                    inflight,
                    inflight.saturating_add(commands),
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn rollback_reservation(&self, commands: u64) {
        self.inflight_publish_commands
            .fetch_sub(commands, AtomicOrdering::AcqRel);
    }

    fn finish_reservation(&self, reserved: u64, published: u64) {
        self.inflight_publish_commands
            .fetch_sub(reserved, AtomicOrdering::AcqRel);
        self.published_commands
            .fetch_add(published, AtomicOrdering::Relaxed);
    }

    fn complete(&self, stage: &str, commands: u64) {
        let counter = match stage {
            "mysql" => &self.mysql_completed_commands,
            "match" => &self.match_completed_commands,
            _ => return,
        };
        // Worker roles publish no HTTP commands in their own process, so this
        // counter cannot be clamped to the process-local published counter.
        // Kafka consumer-group lag is the durable shared backlog; this metric
        // reports work completed by this replica and is aggregated by role.
        counter.fetch_add(commands, AtomicOrdering::Relaxed);
    }

    fn set_observed_lag(&self, stage: &str, lag: u64) {
        let counter = match stage {
            "mysql" => &self.observed_mysql_lag,
            "match" => &self.observed_match_lag,
            _ => return,
        };
        counter.store(lag, AtomicOrdering::Release);
    }

    fn backlog(&self) -> u64 {
        self.inflight_publish_commands
            .load(AtomicOrdering::Acquire)
            .saturating_add(
                self.observed_mysql_lag
                    .load(AtomicOrdering::Acquire)
                    .max(self.observed_match_lag.load(AtomicOrdering::Acquire)),
            )
    }

    fn render(&self) -> String {
        format!(
            "# TYPE tc_order_mysql_commit_ns_total counter\ntc_order_mysql_commit_ns_total {}\n\
# TYPE tc_order_mysql_commit_ns_max gauge\ntc_order_mysql_commit_ns_max {}\n\
# TYPE tc_order_mysql_commit_samples counter\ntc_order_mysql_commit_samples {}\n\
# TYPE tc_order_raft_forward_ns_total counter\ntc_order_raft_forward_ns_total {}\n\
# TYPE tc_order_raft_forward_ns_max gauge\ntc_order_raft_forward_ns_max {}\n\
# TYPE tc_order_raft_forward_samples counter\ntc_order_raft_forward_samples {}\n",
            self.mysql_commit_ns_total.load(AtomicOrdering::Relaxed),
            self.mysql_commit_ns_max.load(AtomicOrdering::Relaxed),
            self.mysql_commit_samples.load(AtomicOrdering::Relaxed),
            self.raft_forward_ns_total.load(AtomicOrdering::Relaxed),
            self.raft_forward_ns_max.load(AtomicOrdering::Relaxed),
            self.raft_forward_samples.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_execution_mysql_commit_ns_total counter\ntc_execution_mysql_commit_ns_total {}\n\
# TYPE tc_execution_mysql_commit_ns_max gauge\ntc_execution_mysql_commit_ns_max {}\n\
# TYPE tc_execution_mysql_commit_samples counter\ntc_execution_mysql_commit_samples {}\n\
# TYPE tc_execution_mysql_completed_events counter\ntc_execution_mysql_completed_events {}\n",
            self.execution_mysql_commit_ns_total.load(AtomicOrdering::Relaxed),
            self.execution_mysql_commit_ns_max.load(AtomicOrdering::Relaxed),
            self.execution_mysql_commit_samples.load(AtomicOrdering::Relaxed),
            self.execution_mysql_completed_events.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_order_ingress_backlog gauge\ntc_order_ingress_backlog {}\n\
# TYPE tc_order_publish_inflight gauge\ntc_order_publish_inflight {}\n\
# TYPE tc_order_published_commands counter\ntc_order_published_commands {}\n\
# TYPE tc_order_mysql_completed_commands counter\ntc_order_mysql_completed_commands {}\n\
# TYPE tc_order_match_completed_commands counter\ntc_order_match_completed_commands {}\n\
# TYPE tc_order_backpressure_rejections counter\ntc_order_backpressure_rejections {}\n",
            self.backlog(),
            self.inflight_publish_commands.load(AtomicOrdering::Relaxed),
            self.published_commands.load(AtomicOrdering::Relaxed),
            self.mysql_completed_commands.load(AtomicOrdering::Relaxed),
            self.match_completed_commands.load(AtomicOrdering::Relaxed),
            self.backpressure_rejections.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_order_mysql_consumer_lag gauge\ntc_order_mysql_consumer_lag {}\n\
# TYPE tc_order_match_consumer_lag gauge\ntc_order_match_consumer_lag {}\n\
# TYPE tc_order_dlq_total counter\ntc_order_dlq_total {}\n",
            self.observed_mysql_lag.load(AtomicOrdering::Relaxed),
            self.observed_match_lag.load(AtomicOrdering::Relaxed),
            self.dlq_total.load(AtomicOrdering::Relaxed),
        ) + &self.mysql_commit_hist.render_standalone(
            "order_mysql_commit_ns",
            "Order API MySQL commit latency (nanoseconds)",
        ) + &self.execution_mysql_commit_hist.render_standalone(
            "execution_mysql_commit_ns",
            "Execution Kafka to MySQL commit processing latency (nanoseconds)",
        )
    }
}

impl OrderStore {
    fn shard(&self, db: u32) -> &Pool {
        self.shards[db as usize]
            .as_ref()
            .unwrap_or_else(|| panic!("physical DB {db} is not owned by this worker"))
    }

    fn owns(&self, db: u32) -> bool {
        self.owned_dbs.contains(&db)
    }
}

/// Ingress admission tier for one category, from the document's §5.3 ladder.
/// Ordered by severity so a batch spanning categories can take the worst.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
enum BackpressureTier {
    /// Below the soft limit — accept normally.
    Normal,
    /// >= soft limit (default 2 s of design traffic): slow the caller down.
    Soft,
    /// >= hard limit (default 5 s): reject with HTTP 429.
    Hard,
    /// >= emergency limit (default 15 s) or the category's Raft quorum is lost:
    /// stop writes for this category with HTTP 503. Independently recoverable.
    Emergency,
}

/// Per-category, three-tier ingress backpressure (production doc §5.3).
///
/// Lag is observed **per Kafka partition** (a category maps to exactly one
/// partition) as the max of the persist and match consumer groups, so a hot
/// partition throttles only the categories that share it — the global
/// [`OrderPipelineMetrics`] backlog stays as the market-wide master switch.
/// Quorum health is tracked per Raft group and clears on the next successful
/// forward, making the emergency tier independently recoverable.
struct Backpressure {
    soft: u64,
    hard: u64,
    emergency: u64,
    soft_delay: Duration,
    topic_count: u32,
    partitions_per_topic: u32,
    raft_group_count: usize,
    raft_group_pins: Arc<HashMap<u32, usize>>,
    mysql_lag: Vec<AtomicU64>,
    match_lag: Vec<AtomicU64>,
    group_unhealthy: Vec<AtomicBool>,
    soft_events: AtomicU64,
    hard_events: AtomicU64,
    emergency_events: AtomicU64,
}

impl Backpressure {
    fn from_env(
        topic_count: usize,
        partitions_per_topic: u32,
        raft_group_count: usize,
        raft_group_pins: Arc<HashMap<u32, usize>>,
    ) -> Self {
        // Defaults express ~2 s / 5 s / 15 s of a 50k commands/s/partition
        // design load; each is overridable directly for the machine's tuning.
        let soft = env_number("TC_ORDER_BP_SOFT", 100_000u64).max(1);
        let hard = env_number("TC_ORDER_BP_HARD", 250_000u64).max(soft);
        let emergency = env_number("TC_ORDER_BP_EMERGENCY", 750_000u64).max(hard);
        let partitions = topic_count * partitions_per_topic as usize;
        Self {
            soft,
            hard,
            emergency,
            soft_delay: Duration::from_millis(env_number("TC_ORDER_BP_SOFT_DELAY_MS", 2u64)),
            topic_count: topic_count.max(1) as u32,
            partitions_per_topic: partitions_per_topic.max(1),
            raft_group_count: raft_group_count.max(1),
            raft_group_pins,
            mysql_lag: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            match_lag: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            group_unhealthy: (0..raft_group_count.max(1))
                .map(|_| AtomicBool::new(false))
                .collect(),
            soft_events: AtomicU64::new(0),
            hard_events: AtomicU64::new(0),
            emergency_events: AtomicU64::new(0),
        }
    }

    /// Pure tier decision — the state machine unit-tested in isolation.
    fn classify(&self, lag: u64, quorum_lost: bool) -> BackpressureTier {
        if quorum_lost || lag >= self.emergency {
            BackpressureTier::Emergency
        } else if lag >= self.hard {
            BackpressureTier::Hard
        } else if lag >= self.soft {
            BackpressureTier::Soft
        } else {
            BackpressureTier::Normal
        }
    }

    fn partition_index(&self, category_id: u32) -> usize {
        let topic = (category_id % self.topic_count) as usize;
        let partition = ((category_id / self.topic_count) % self.partitions_per_topic) as usize;
        topic * self.partitions_per_topic as usize + partition
    }

    fn raft_group(&self, category_id: u32) -> usize {
        sharding::raft_group_for_category_pinned(
            category_id,
            self.raft_group_count,
            &self.raft_group_pins,
        )
    }

    fn partition_lag(&self, index: usize) -> u64 {
        let mysql = self
            .mysql_lag
            .get(index)
            .map_or(0, |v| v.load(AtomicOrdering::Acquire));
        let matcher = self
            .match_lag
            .get(index)
            .map_or(0, |v| v.load(AtomicOrdering::Acquire));
        mysql.max(matcher)
    }

    fn set_partition_lags(&self, stage: &str, lags: &[u64]) {
        let target = match stage {
            "mysql" => &self.mysql_lag,
            "match" => &self.match_lag,
            _ => return,
        };
        for (slot, lag) in target.iter().zip(lags.iter()) {
            slot.store(*lag, AtomicOrdering::Release);
        }
    }

    fn set_group_health(&self, group: usize, healthy: bool) {
        if let Some(flag) = self.group_unhealthy.get(group) {
            flag.store(!healthy, AtomicOrdering::Release);
        }
    }

    fn group_unhealthy(&self, group: usize) -> bool {
        self.group_unhealthy
            .get(group)
            .is_some_and(|flag| flag.load(AtomicOrdering::Acquire))
    }

    /// Current tier for a category from its partition lag and quorum health.
    fn tier_for_category(&self, category_id: u32) -> BackpressureTier {
        let lag = self.partition_lag(self.partition_index(category_id));
        self.classify(lag, self.group_unhealthy(self.raft_group(category_id)))
    }

    fn note(&self, tier: BackpressureTier) {
        match tier {
            BackpressureTier::Soft => &self.soft_events,
            BackpressureTier::Hard => &self.hard_events,
            BackpressureTier::Emergency => &self.emergency_events,
            BackpressureTier::Normal => return,
        }
        .fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Admit one category's write: apply the soft-tier slowdown inline, or map
    /// hard/emergency to an ingress error. Emergency is 503, hard is 429.
    fn admit(&self, category_id: u32) -> Result<(), String> {
        let tier = self.tier_for_category(category_id);
        self.note(tier);
        match tier {
            BackpressureTier::Normal => Ok(()),
            BackpressureTier::Soft => {
                if !self.soft_delay.is_zero() {
                    std::thread::sleep(self.soft_delay);
                }
                Ok::<(), String>(())
            }
            BackpressureTier::Hard => Err(format!(
                "backpressure: category {category_id} throttled at hard limit {}",
                self.hard
            )),
            BackpressureTier::Emergency => Err(format!(
                "backpressure-emergency: category {category_id} writes stopped (lag/quorum)"
            )),
        }
    }

    fn max_partition_lag(&self) -> u64 {
        (0..self.mysql_lag.len())
            .map(|i| self.partition_lag(i))
            .max()
            .unwrap_or(0)
    }

    fn unhealthy_groups(&self) -> u64 {
        self.group_unhealthy
            .iter()
            .filter(|flag| flag.load(AtomicOrdering::Acquire))
            .count() as u64
    }

    fn render(&self) -> String {
        format!(
            "# TYPE tc_order_bp_soft_limit gauge\ntc_order_bp_soft_limit {}\n\
# TYPE tc_order_bp_hard_limit gauge\ntc_order_bp_hard_limit {}\n\
# TYPE tc_order_bp_emergency_limit gauge\ntc_order_bp_emergency_limit {}\n\
# TYPE tc_order_bp_soft_total counter\ntc_order_bp_soft_total {}\n\
# TYPE tc_order_bp_hard_total counter\ntc_order_bp_hard_total {}\n\
# TYPE tc_order_bp_emergency_total counter\ntc_order_bp_emergency_total {}\n\
# TYPE tc_order_bp_max_partition_lag gauge\ntc_order_bp_max_partition_lag {}\n\
# TYPE tc_order_bp_quorum_unhealthy_groups gauge\ntc_order_bp_quorum_unhealthy_groups {}\n",
            self.soft,
            self.hard,
            self.emergency,
            self.soft_events.load(AtomicOrdering::Relaxed),
            self.hard_events.load(AtomicOrdering::Relaxed),
            self.emergency_events.load(AtomicOrdering::Relaxed),
            self.max_partition_lag(),
            self.unhealthy_groups(),
        )
    }
}

/// Header stamped on a fresh dead-letter file; a version bump changes it so old
/// files are rejected rather than silently misparsed.
const DLQ_HEADER: [u8; 8] = *b"TCDLQ01\0";

/// Append-only dead-letter log for order-execution messages that exhausted
/// their retry budget. Mirrors the journal WAL style: a magic header, one
/// length-framed record per poison message carrying `(partition, offset,
/// reason, original payload)`, each closed by an FNV-1a checksum so a torn
/// tail is detected and dropped on read.
struct DlqWriter {
    file: File,
}

impl DlqWriter {
    fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let mut probe = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        if probe.metadata()?.len() == 0 {
            probe.write_all(&DLQ_HEADER)?;
            probe.sync_data()?;
        } else {
            let mut header = [0u8; 8];
            probe.read_exact(&mut header)?;
            if header != DLQ_HEADER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DLQ header/version mismatch (migration required)",
                ));
            }
        }
        Ok(Self {
            file: OpenOptions::new().append(true).open(path)?,
        })
    }

    fn append(
        &mut self,
        partition: i32,
        offset: i64,
        reason: &str,
        payload: &[u8],
    ) -> io::Result<()> {
        let reason = reason.as_bytes();
        let mut body = Vec::with_capacity(28 + reason.len() + payload.len());
        body.extend_from_slice(&journal::now_nanos().to_le_bytes());
        body.extend_from_slice(&partition.to_le_bytes());
        body.extend_from_slice(&offset.to_le_bytes());
        body.extend_from_slice(&(reason.len() as u32).to_le_bytes());
        body.extend_from_slice(reason);
        body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        body.extend_from_slice(payload);
        let checksum = journal::fnv1a(&body);
        let mut frame = Vec::with_capacity(4 + body.len() + 8);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        frame.extend_from_slice(&checksum.to_le_bytes());
        self.file.write_all(&frame)?;
        self.file.sync_data()
    }
}

/// Outcome of driving one message through the persist-retry-or-dead-letter
/// loop. Both variants advance the Kafka offset — a poison message must not
/// wedge the partition forever.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(not(test), allow(dead_code))]
enum PersistOutcome {
    Committed,
    DeadLettered,
}

/// Persist one message with bounded exponential-backoff retries; on exhausting
/// the budget, dead-letter it and report `DeadLettered` so the caller advances
/// the offset. A DLQ write failure is *not* treated as success — we keep
/// retrying rather than silently drop the record.
#[cfg_attr(not(test), allow(dead_code))]
fn persist_with_retry<F>(
    mut persist: F,
    dlq: &Mutex<DlqWriter>,
    metrics: &OrderPipelineMetrics,
    max_retries: u32,
    base_backoff: Duration,
    partition: i32,
    offset: i64,
    payload: &[u8],
) -> PersistOutcome
where
    F: FnMut() -> Result<(), String>,
{
    let mut attempt = 1u32;
    loop {
        match persist() {
            Ok(()) => return PersistOutcome::Committed,
            Err(error) if attempt >= max_retries => {
                let written = dlq
                    .lock()
                    .map_err(|_| "DLQ mutex poisoned".to_string())
                    .and_then(|mut writer| {
                        writer
                            .append(partition, offset, &error, payload)
                            .map_err(|e| e.to_string())
                    });
                match written {
                    Ok(()) => {
                        metrics.dlq_total.fetch_add(1, AtomicOrdering::Relaxed);
                        return PersistOutcome::DeadLettered;
                    }
                    Err(dlq_error) => {
                        log_error!(
                            "execution-mysql",
                            "event=dlq_write_failed partition={partition} offset={offset} error={dlq_error} — retrying"
                        );
                        if !base_backoff.is_zero() {
                            std::thread::sleep(base_backoff);
                        }
                        // Keep the same attempt count: stay in the dead-letter
                        // branch until the record is durably captured.
                    }
                }
            }
            Err(_) => {
                let shift = (attempt - 1).min(9);
                let backoff = base_backoff.saturating_mul(1u32 << shift);
                if !backoff.is_zero() {
                    std::thread::sleep(backoff);
                }
                attempt += 1;
            }
        }
    }
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|part| {
        let (k, v) = part.split_once('=')?;
        (k == key).then_some(v)
    })
}

fn required<T: std::str::FromStr>(query: &str, key: &str) -> Result<T, String> {
    query_param(query, key)
        .ok_or_else(|| format!("missing {key}"))?
        .parse()
        .map_err(|_| format!("invalid {key}"))
}

/// Handle `/admin/routes/caught-up`. When `TC_MIGRATION_GROUP_ENDPOINTS` maps
/// both the migration's source and target Raft groups, this fetches each side's
/// per-category matching-state fingerprint from that group's node and verifies
/// the cutover against real state — the operator no longer supplies (and the
/// control plane no longer trusts) fingerprints as query parameters. Without an
/// endpoint map, it falls back to the legacy caller-supplied fingerprints.
fn verify_caught_up(kafka: &KafkaIngress, category: u32, query: &str) -> Result<String, String> {
    let migration = kafka
        .route_control
        .active(category)
        .ok_or("category migration is not active")?;

    let source_addr = kafka.migration_endpoints.get(&migration.from_group);
    let target_addr = kafka.migration_endpoints.get(&migration.to_group);
    let (Some(source_addr), Some(target_addr)) = (source_addr, target_addr) else {
        // Legacy path: no endpoint map for these groups, trust the caller.
        return kafka
            .route_control
            .caught_up(
                category,
                required::<u64>(query, "raft_index")?,
                required::<u64>(query, "source_fingerprint")?,
                required::<u64>(query, "target_fingerprint")?,
            )
            .map(|_| "{\"accepted\":true,\"state\":\"VERIFIED\",\"verified_by\":\"caller\"}".into());
    };

    let timeout = Duration::from_millis(env_number("TC_MIGRATION_FINGERPRINT_TIMEOUT_MS", 2_000u64));
    let fetch = |addr: &str| -> Result<trade_core::migration::CategoryFingerprint, String> {
        trade_core::migration::fetch_category_fingerprint(addr, category, timeout)
            .map_err(|error| format!("fingerprint fetch from {addr} failed: {error}"))?
            .ok_or_else(|| format!("group node {addr} has no durable snapshot yet; retry"))
    };
    let source = fetch(source_addr)?;
    let target = fetch(target_addr)?;

    let verified = trade_core::migration::verify_cutover(migration.frozen_index, source, target)?;
    // Feed the verified, node-reported values into the fenced state machine;
    // the equality check inside caught_up is now backed by real state.
    kafka
        .route_control
        .caught_up(
            category,
            verified.raft_applied_index,
            verified.fingerprint,
            verified.fingerprint,
        )
        .map(|_| {
            format!(
                "{{\"accepted\":true,\"state\":\"VERIFIED\",\"verified_by\":\"nodes\",\"raft_applied_index\":{},\"fingerprint\":{}}}",
                verified.raft_applied_index, verified.fingerprint
            )
        })
}

fn ingress_error_status(error: &str) -> &'static str {
    if error.starts_with("backpressure-emergency:") {
        // Emergency tier / lost quorum: stop writes for this category.
        "503 Service Unavailable"
    } else if error.starts_with("backpressure:") {
        // Hard tier or global backlog master switch: throttle.
        "429 Too Many Requests"
    } else {
        "503 Service Unavailable"
    }
}

fn order_from_query(query: &str) -> Result<Order, String> {
    let side = match query_param(query, "side") {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err("side must be buy or sell".into()),
    };
    Ok(Order::limit(
        OrderId(required(query, "order_id")?),
        side,
        required(query, "price")?,
        required(query, "qty")?,
    )
    .on(InstrumentId(required(query, "instrument")?))
    .by(required(query, "user")?))
}

fn cancel_from_query(query: &str) -> Result<(InstrumentId, OrderId, u64, u64), String> {
    Ok((
        InstrumentId(required(query, "instrument")?),
        OrderId(required(query, "order_id")?),
        required(query, "cmd_id")?,
        required(query, "user")?,
    ))
}

fn bootstrap(store: &OrderStore) -> mysql::Result<()> {
    for db in 0..store.routing.db_count {
        if !store.owns(db as u32) {
            continue;
        }
        let mut conn = store.shard(db as u32).get_conn()?;
        let db_name = format!("order_db_{db}");
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db_name}"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_commands (category_id INT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(category_id,kafka_offset), KEY idx_partition_offset (kafka_partition,kafka_offset)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_executions (kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(kafka_partition,kafka_offset), KEY idx_instrument_offset (instrument,kafka_offset)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_execution_events (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, target_order_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,target_order_id), KEY idx_instrument_index (instrument,raft_index), KEY idx_order_event (target_order_id,raft_index)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.pending_execution_events (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, target_order_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, event_qty BIGINT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,target_order_id), KEY idx_pending_table_order (shard_table,target_order_id,raft_index,report_ordinal)) ENGINE=InnoDB"))?;
        for table in 0..store.routing.tables_per_db {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.asset_orders_{table:03} (row_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id BIGINT UNSIGNED NOT NULL UNIQUE, category_id INT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, filled_qty BIGINT UNSIGNED NOT NULL DEFAULT 0, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_category_row (category_id,row_seq), KEY idx_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct CategorySequence {
    category_id: u32,
    category_seq: u64,
}

#[derive(Clone)]
struct KafkaIngress {
    producer: FutureProducer,
    brokers: String,
    router: QueueRouter,
    partitions_per_topic: u32,
    db_consumers: usize,
    db_router_consumers: usize,
    matcher_consumers: usize,
    db_group: String,
    db_router_group: String,
    db_route_topic: String,
    db_route_partitions: u32,
    matcher_group: String,
    execution_topic: String,
    execution_partitions: u32,
    execution_group: String,
    execution_consumers: usize,
    /// Maximum execution events collected from Kafka before dispatching them
    /// to order-id-routed DB workers.
    execution_batch_size: usize,
    execution_linger: Duration,
    route_control: Arc<trade_core::cluster::RouteControlPlane>,
    /// Raft group id → matching-node admin address serving `/fingerprint`.
    /// When both a migration's source and target groups are present, the
    /// `/admin/routes/caught-up` handler fetches and compares real state
    /// fingerprints itself instead of trusting caller-supplied numbers.
    migration_endpoints: Arc<HashMap<usize, String>>,
    batch_size: usize,
    linger: Duration,
    async_offset_commits: bool,
    max_pipeline_backlog: u64,
    metrics: Arc<OrderPipelineMetrics>,
    /// Per-category three-tier ingress backpressure (production doc §5.3).
    backpressure: Arc<Backpressure>,
}

impl KafkaIngress {
    fn from_env(
        metrics: Arc<OrderPipelineMetrics>,
        raft_group_count: usize,
    ) -> Result<Option<Self>, String> {
        let Ok(brokers) = std::env::var("TC_ORDER_KAFKA_BROKERS") else {
            return Ok(None);
        };
        let topics = std::env::var("TC_ORDER_KAFKA_TOPICS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|topic| !topic.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|topics| !topics.is_empty())
            .unwrap_or_else(|| {
                let count = env_number("TC_ORDER_KAFKA_TOPIC_COUNT", 4usize).max(1);
                let prefix = std::env::var("TC_ORDER_KAFKA_TOPIC_PREFIX")
                    .unwrap_or_else(|_| "orders-v2-g".into());
                (0..count).map(|group| format!("{prefix}{group}")).collect()
            });
        let partitions = env_number("TC_ORDER_KAFKA_PARTITIONS_PER_TOPIC", 8u32);
        let version = env_number("TC_ORDER_KAFKA_ROUTE_VERSION", 1u32);
        let partition_workers = topics.len().saturating_mul(partitions as usize).max(1);
        let raft_group_pins = Arc::new(
            std::env::var("TC_RAFT_CATEGORY_PINS")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(|entry| {
                            let (category, group) = entry.trim().split_once(':')?;
                            Some((category.parse().ok()?, group.parse().ok()?))
                        })
                        .collect::<HashMap<_, _>>()
                })
                .unwrap_or_default(),
        );
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", &brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("max.in.flight.requests.per.connection", "5")
            .set("delivery.timeout.ms", "10000")
            .set(
                "linger.ms",
                env_number("TC_ORDER_KAFKA_LINGER_MS", 1u64).to_string(),
            )
            .set(
                "batch.num.messages",
                env_number("TC_ORDER_KAFKA_BATCH_MESSAGES", 10_000usize).to_string(),
            )
            .set(
                "compression.type",
                std::env::var("TC_ORDER_KAFKA_COMPRESSION").unwrap_or_else(|_| "lz4".into()),
            )
            .set(
                "queue.buffering.max.kbytes",
                env_number("TC_ORDER_KAFKA_QUEUE_KBYTES", 1_048_576usize).to_string(),
            );
        let producer = producer_config
            .create::<FutureProducer>()
            .map_err(|error| error.to_string())?;
        let topic_count = topics.len();
        let backpressure = Arc::new(Backpressure::from_env(
            topic_count,
            partitions,
            raft_group_count,
            Arc::clone(&raft_group_pins),
        ));
        let route_control = Arc::new(trade_core::cluster::RouteControlPlane::new(
            raft_group_count.max(1),
            (*raft_group_pins).clone(),
        ));
        // Optional: group→matching-node address map enabling automatic cutover
        // verification. Absent/empty leaves the legacy behavior where an
        // operator supplies fingerprints to /admin/routes/caught-up.
        let migration_endpoints = Arc::new(
            std::env::var("TC_MIGRATION_GROUP_ENDPOINTS")
                .ok()
                .filter(|spec| !spec.trim().is_empty())
                .map(|spec| {
                    trade_core::migration::parse_group_endpoints(&spec).unwrap_or_else(|error| {
                        log_warn!("order-api", "ignoring TC_MIGRATION_GROUP_ENDPOINTS: {error}");
                        HashMap::new()
                    })
                })
                .unwrap_or_default(),
        );
        Ok(Some(Self {
            producer,
            brokers,
            router: QueueRouter::new(topics, partitions, version),
            partitions_per_topic: partitions,
            db_consumers: env_number(
                "TC_ORDER_KAFKA_DB_CONSUMERS",
                env_number("TC_ORDER_KAFKA_CONSUMERS", partition_workers),
            ),
            db_router_consumers: env_number(
                "TC_ORDER_KAFKA_DB_ROUTER_CONSUMERS",
                partition_workers,
            ),
            matcher_consumers: env_number(
                "TC_ORDER_KAFKA_MATCH_CONSUMERS",
                raft_group_count.max(1),
            ),
            db_group: std::env::var("TC_ORDER_KAFKA_DB_GROUP")
                .unwrap_or_else(|_| "trade-order-persist-v1".into()),
            db_router_group: std::env::var("TC_ORDER_KAFKA_DB_ROUTER_GROUP")
                .unwrap_or_else(|_| "trade-order-db-route-v1".into()),
            db_route_topic: std::env::var("TC_ORDER_DB_KAFKA_TOPIC")
                .unwrap_or_else(|_| "trade-order-db-v1".into()),
            db_route_partitions: env_number(
                "TC_ORDER_DB_KAFKA_PARTITIONS",
                env_number("TC_DB_COUNT", sharding::DB_COUNT as u32),
            ),
            matcher_group: std::env::var("TC_ORDER_KAFKA_MATCH_GROUP")
                .unwrap_or_else(|_| "trade-order-match-v1".into()),
            execution_topic: std::env::var("TC_EXECUTION_KAFKA_TOPIC")
                .unwrap_or_else(|_| "trade-executions-v1".into()),
            execution_partitions: env_number("TC_EXECUTION_KAFKA_PARTITIONS", 16u32),
            execution_group: std::env::var("TC_EXECUTION_KAFKA_MYSQL_GROUP")
                .unwrap_or_else(|_| "trade-order-execution-mysql-v1".into()),
            execution_consumers: env_number("TC_EXECUTION_KAFKA_MYSQL_CONSUMERS", 4usize),
            // The public knob is per physical DB. Collect one DB-wide target
            // from every shard before dispatch, so 10 DBs × 500 rows becomes a
            // roughly 5,000-event Kafka batch and each DB commits near 500.
            execution_batch_size: execution_consumer_batch_size(
                env_number("TC_EXECUTION_MYSQL_BATCH_PER_DB", 500usize),
                env_number("TC_DB_COUNT", sharding::DB_COUNT as usize),
            ),
            execution_linger: Duration::from_millis(env_number(
                "TC_EXECUTION_MYSQL_BATCH_LINGER_MS",
                10u64,
            )),
            route_control,
            migration_endpoints,
            batch_size: env_number("TC_ORDER_BATCH_SIZE", 1_000usize).max(1),
            linger: Duration::from_millis(env_number("TC_ORDER_BATCH_LINGER_MS", 2u64)),
            async_offset_commits: env_enabled("TC_KAFKA_OFFSET_COMMIT_ASYNC", true),
            max_pipeline_backlog: env_number("TC_ORDER_MAX_PIPELINE_BACKLOG", 50_000u64).max(1),
            metrics,
            backpressure,
        }))
    }

    fn reserve(&self, commands: usize) -> Result<(), String> {
        self.metrics
            .try_reserve(commands as u64, self.max_pipeline_backlog)
            .map_err(|backlog| {
                format!(
                    "backpressure: pipeline backlog {backlog} reached limit {}",
                    self.max_pipeline_backlog
                )
            })
    }

    /// Admit a batch by its worst-tier category. A batch is one HTTP request,
    /// so the client owns which categories it groups; per-request granularity
    /// still isolates other categories' *separate* requests, which is the
    /// isolation the design requires.
    fn admit_batch(&self, records: &[BatchRecord]) -> Result<(), String> {
        if let Some(record) = records
            .iter()
            .find(|record| !self.route_control.accepts(record.category_id))
        {
            return Err(format!(
                "backpressure-emergency: category {} is frozen for route migration",
                record.category_id
            ));
        }
        let Some(worst) = records
            .iter()
            .map(|record| {
                (
                    self.backpressure.tier_for_category(record.category_id),
                    record.category_id,
                )
            })
            .max()
        else {
            return Ok(());
        };
        self.backpressure.admit(worst.1)
    }

    fn publish(
        &self,
        category_id: u32,
        user: u64,
        frame: &[u8; wire::MSG_LEN],
    ) -> Result<CategorySequence, String> {
        // Per-category tier first (soft slowdown / hard 429 / emergency 503),
        // then the market-wide backlog master switch.
        if !self.route_control.accepts(category_id) {
            return Err(format!(
                "backpressure-emergency: category {category_id} is frozen for route migration"
            ));
        }
        self.backpressure.admit(category_id)?;
        self.reserve(1)?;
        let route = self.router.route(category_id);
        let envelope = encode_envelope(user, route.version, frame);
        let key = category_id.to_be_bytes();
        let delivery = futures::executor::block_on(
            self.producer.send(
                FutureRecord::to(&route.topic)
                    .partition(route.partition)
                    .key(&key)
                    .payload(&envelope),
                Duration::from_secs(5),
            ),
        );
        let offset = match delivery {
            Ok(delivery) => delivery.offset,
            Err((error, _)) => {
                self.metrics.rollback_reservation(1);
                return Err(error.to_string());
            }
        };
        self.metrics.finish_reservation(1, 1);
        Ok(CategorySequence {
            category_id,
            category_seq: (offset as u64).saturating_add(1),
        })
    }

    fn publish_batch(&self, records: &[BatchRecord]) -> Result<(), String> {
        self.admit_batch(records)?;
        self.reserve(records.len())?;
        let prepared = records
            .iter()
            .map(|record| {
                let route = self.router.route(record.category_id);
                (
                    route.clone(),
                    record.category_id.to_be_bytes(),
                    encode_envelope(record.user, route.version, &record.frame),
                )
            })
            .collect::<Vec<_>>();
        let deliveries = futures::executor::block_on(futures::future::join_all(
            prepared.iter().map(|(route, key, envelope)| {
                self.producer.send(
                    FutureRecord::to(&route.topic)
                        .partition(route.partition)
                        .key(key)
                        .payload(envelope),
                    Duration::from_secs(5),
                )
            }),
        ));
        let mut failed = 0u64;
        let mut first_error = None;
        for delivery in deliveries {
            if let Err((error, _)) = delivery {
                failed += 1;
                first_error.get_or_insert_with(|| error.to_string());
            }
        }
        self.metrics
            .finish_reservation(records.len() as u64, records.len() as u64 - failed);
        if failed > 0 {
            return Err(first_error.unwrap_or_else(|| "Kafka batch publish failed".into()));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct BatchRecord {
    category_id: u32,
    user: u64,
    frame: [u8; wire::MSG_LEN],
}

fn decode_batch(body: &[u8], category_size: u32) -> Result<Vec<BatchRecord>, String> {
    if body.is_empty() || body.len() % BATCH_RECORD_LEN != 0 {
        return Err(format!(
            "batch body must contain one or more {BATCH_RECORD_LEN}-byte records"
        ));
    }
    let mut records = Vec::with_capacity(body.len() / BATCH_RECORD_LEN);
    for chunk in body.chunks_exact(BATCH_RECORD_LEN) {
        let user = u64::from_le_bytes(chunk[..8].try_into().expect("batch user bytes"));
        let frame: [u8; wire::MSG_LEN] = chunk[8..]
            .try_into()
            .expect("fixed-size batch command frame");
        let command = wire::WireView::parse(&frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| "batch contains an invalid command frame".to_string())?;
        if let trade_core::Command::New(order) = &command {
            if order.user != user {
                return Err(format!(
                    "batch user {user} does not match order {} user {}",
                    order.id.0, order.user
                ));
            }
        }
        records.push(BatchRecord {
            category_id: sharding::asset_category(command.instrument(), category_size),
            user,
            frame,
        });
    }
    Ok(records)
}

fn env_number<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_enabled(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(default)
}

fn execution_consumer_batch_size(per_db: usize, db_count: usize) -> usize {
    per_db.max(1).saturating_mul(db_count.max(1))
}

#[derive(Clone)]
struct KafkaRecord {
    topic: String,
    partition: i32,
    offset: i64,
    category_id: u32,
    user: u64,
    frame: [u8; wire::MSG_LEN],
}

fn command_id(command: &trade_core::Command) -> u64 {
    match command {
        trade_core::Command::New(order) => order.id.0,
        trade_core::Command::Cancel { cmd_id, .. }
        | trade_core::Command::Modify { cmd_id, .. }
        | trade_core::Command::Halt { cmd_id, .. }
        | trade_core::Command::Resume { cmd_id, .. }
        | trade_core::Command::HaltUser { cmd_id, .. }
        | trade_core::Command::ResumeUser { cmd_id, .. } => *cmd_id,
        trade_core::Command::ForceClose { close_order_id, .. } => close_order_id.0,
        trade_core::Command::Batch(commands) => commands.first().map(command_id).unwrap_or(0),
    }
}

fn database_order_id(command: &trade_core::Command) -> u64 {
    match command {
        trade_core::Command::New(order) => order.id.0,
        trade_core::Command::Cancel { order_id, .. }
        | trade_core::Command::Modify { order_id, .. } => order_id.0,
        trade_core::Command::ForceClose { close_order_id, .. } => close_order_id.0,
        _ => command_id(command),
    }
}

fn exec_multi_insert(
    tx: &mut Transaction<'_>,
    prefix: &str,
    columns: usize,
    values: Vec<Value>,
) -> mysql::Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    debug_assert_eq!(values.len() % columns, 0);
    let row = format!("({})", vec!["?"; columns].join(","));
    let rows = vec![row; values.len() / columns].join(",");
    tx.exec_drop(
        format!("{prefix} VALUES {rows}"),
        Params::Positional(values),
    )
}

struct CommandShardJob {
    records: Vec<KafkaRecord>,
    reply: std::sync::mpsc::SyncSender<Result<(), String>>,
}

#[derive(Clone)]
struct CommandDbForwarder {
    shards: Arc<Vec<Option<std::sync::mpsc::SyncSender<CommandShardJob>>>>,
    routing: sharding::RouteConfig,
    owned_dbs: Arc<std::collections::HashSet<u32>>,
}

impl CommandDbForwarder {
    fn spawn(store: OrderStore) -> Self {
        let queue = env_number("TC_MYSQL_SHARD_QUEUE", 64usize).max(1);
        let coalesce_jobs = env_number("TC_MYSQL_SHARD_COALESCE_JOBS", 16usize).max(1);
        let mut shards = Vec::with_capacity(store.routing.db_count as usize);
        for shard_db in 0..store.routing.db_count as u32 {
            if !store.owns(shard_db) {
                shards.push(None);
                continue;
            }
            let (tx, rx) = std::sync::mpsc::sync_channel::<CommandShardJob>(queue);
            let shard_store = store.clone();
            std::thread::Builder::new()
                .name(format!("command-db-shard-{shard_db}"))
                .spawn(move || {
                    while let Ok(first) = rx.recv() {
                        let mut jobs = vec![first];
                        while jobs.len() < coalesce_jobs {
                            let Ok(job) = rx.try_recv() else { break };
                            jobs.push(job);
                        }
                        let records = jobs
                            .iter()
                            .flat_map(|job| job.records.iter().cloned())
                            .collect::<Vec<_>>();
                        let result = persist_mysql_shard(&shard_store, shard_db, &records);
                        for job in jobs {
                            let _ = job.reply.send(result.clone());
                        }
                    }
                })
                .expect("spawn command DB shard worker");
            shards.push(Some(tx));
        }
        Self {
            shards: Arc::new(shards),
            routing: store.routing,
            owned_dbs: store.owned_dbs.clone(),
        }
    }

    fn submit(
        &self,
        routing: sharding::RouteConfig,
        records: &[KafkaRecord],
    ) -> Result<(), String> {
        let mut by_db: HashMap<u32, Vec<KafkaRecord>> = HashMap::new();
        for record in records {
            let command = wire::WireView::parse(&record.frame)
                .and_then(|view| view.to_command())
                .ok_or_else(|| "invalid Kafka command frame".to_string())?;
            let route = routing.route_order_id(database_order_id(&command));
            by_db.entry(route.db).or_default().push(record.clone());
        }
        let mut replies = Vec::with_capacity(by_db.len());
        for (db, records) in by_db {
            let (reply, result) = std::sync::mpsc::sync_channel(1);
            self.shards[db as usize]
                .as_ref()
                .ok_or_else(|| format!("command DB {db} is not owned by this worker"))?
                .send(CommandShardJob { records, reply })
                .map_err(|_| format!("command DB shard {db} worker stopped"))?;
            replies.push(result);
        }
        for reply in replies {
            reply
                .recv()
                .map_err(|_| "command DB shard worker dropped reply".to_string())??;
        }
        Ok(())
    }

    fn owns(&self, db: u32) -> bool {
        self.owned_dbs.contains(&db)
    }
}

/// Persist one routed shard batch with one MySQL commit. Long-lived per-shard
/// workers call this after coalescing jobs from concurrent Kafka consumers.
fn persist_mysql_shard(
    store: &OrderStore,
    shard_db: u32,
    records: &[KafkaRecord],
) -> Result<(), String> {
    let started = std::time::Instant::now();
    let db = format!("order_db_{shard_db}");
    let mut conn = store
        .shard(shard_db)
        .get_conn()
        .map_err(|error| error.to_string())?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .map_err(|error| error.to_string())?;

    let mut processed = Vec::with_capacity(records.len() * 6);
    let mut orders_by_table: BTreeMap<u32, Vec<Value>> = BTreeMap::new();
    let mut cancels = Vec::new();
    let mut modifies = Vec::new();

    for record in records {
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| "invalid Kafka command frame".to_string())?;
        let id = command_id(&command);
        let route = store.routing.route_order_id(database_order_id(&command));
        processed.extend([
            Value::from(record.category_id),
            Value::from(record.partition),
            Value::from((record.offset as u64).saturating_add(1)),
            Value::from(id),
            Value::from(record.user),
            Value::from(route.table),
        ]);

        match command {
            trade_core::Command::New(order) => {
                orders_by_table.entry(route.table).or_default().extend([
                    Value::from(order.id.0),
                    Value::from(record.category_id),
                    Value::from(record.user),
                    Value::from(order.instrument.0),
                    Value::from(if order.side == Side::Buy { 0u8 } else { 1u8 }),
                    Value::from(order.price),
                    Value::from(order.quantity),
                    Value::from("PENDING"),
                ]);
            }
            trade_core::Command::Cancel { order_id, .. } => {
                cancels.push((route.table_name(), order_id.0, record.user));
            }
            trade_core::Command::Modify {
                order_id,
                new_price,
                new_qty,
                ..
            } => modifies.push((
                route.table_name(),
                order_id.0,
                record.user,
                new_price,
                new_qty,
            )),
            trade_core::Command::ForceClose {
                instrument,
                user,
                close_order_id,
                close_side,
                close_qty,
            } if close_qty > 0 => {
                orders_by_table.entry(route.table).or_default().extend([
                    Value::from(close_order_id.0),
                    Value::from(record.category_id),
                    Value::from(user),
                    Value::from(instrument.0),
                    Value::from(if close_side == Side::Buy { 0u8 } else { 1u8 }),
                    Value::from(0u64),
                    Value::from(close_qty),
                    Value::from("PENDING"),
                ]);
            }
            _ => {}
        }
    }

    exec_multi_insert(
            &mut tx,
            &format!("INSERT IGNORE INTO {db}.processed_commands (category_id,kafka_partition,kafka_offset,command_id,user_id,shard_table)"),
            6,
            processed,
        )
        .map_err(|error| error.to_string())?;
    let pending_tables = orders_by_table.keys().copied().collect::<Vec<_>>();
    for (table, values) in orders_by_table {
        exec_multi_insert(
                &mut tx,
                &format!("INSERT IGNORE INTO {db}.asset_orders_{table:03} (order_id,category_id,user_id,instrument,side,price,qty,status)"),
                8,
                values,
            )
            .map_err(|error| error.to_string())?;
    }
    for (table, order_id, user) in cancels {
        tx.exec_drop(
                format!("UPDATE {db}.{table} SET status='CANCEL_PENDING' WHERE order_id=:id AND user_id=:user"),
                params! {"id" => order_id, "user" => user},
            )
            .map_err(|error| error.to_string())?;
    }
    for (table, order_id, user, new_price, new_qty) in modifies {
        tx.exec_drop(
                format!("UPDATE {db}.{table} SET price=:price,qty=:qty,status='MODIFY_PENDING' WHERE order_id=:id AND user_id=:user"),
                params! {"price" => new_price, "qty" => new_qty, "id" => order_id, "user" => user},
            )
            .map_err(|error| error.to_string())?;
    }
    // Results may have reached this database before the command projection.
    // Once the order rows exist, drain their durable pending events in the same
    // transaction so no external scheduler is required for the common race.
    for table in pending_tables {
        apply_pending_execution_table(&mut tx, &db, table)?;
    }
    tx.commit().map_err(|error| error.to_string())?;
    store.metrics.record_mysql(started.elapsed());
    Ok(())
}

fn decode_kafka_record(
    message: &rdkafka::message::BorrowedMessage<'_>,
    router: &QueueRouter,
    category_size: u32,
) -> Result<KafkaRecord, String> {
    let payload = message.payload().ok_or("Kafka message has no payload")?;
    let envelope = QueueEnvelope::decode(payload).ok_or("invalid Kafka order envelope")?;
    let category_id = sharding::asset_category(envelope.instrument(), category_size);
    let expected = router.route(category_id);
    if expected.topic != message.topic()
        || expected.partition != message.partition()
        || expected.version != envelope.route_version
    {
        return Err(format!(
            "stale queue route for category {category_id}: got {}:{} v{}, expected {}:{} v{}",
            message.topic(),
            message.partition(),
            envelope.route_version,
            expected.topic,
            expected.partition,
            expected.version
        ));
    }
    Ok(KafkaRecord {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
        category_id,
        user: envelope.user,
        frame: *envelope.frame,
    })
}

fn commit_kafka_batch(
    consumer: &BaseConsumer,
    records: &[KafkaRecord],
    async_commit: bool,
) -> Result<(), String> {
    let mut offsets: HashMap<(&str, i32), i64> = HashMap::new();
    for record in records {
        offsets
            .entry((&record.topic, record.partition))
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }
    let mut list = TopicPartitionList::new();
    for ((topic, partition), offset) in offsets {
        list.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|error| error.to_string())?;
    }
    consumer
        .commit(
            &list,
            if async_commit {
                CommitMode::Async
            } else {
                CommitMode::Sync
            },
        )
        .map_err(|error| error.to_string())
}

fn rewind_kafka_batch(consumer: &BaseConsumer, records: &[KafkaRecord]) {
    let mut offsets: HashMap<(&str, i32), i64> = HashMap::new();
    for record in records {
        offsets
            .entry((&record.topic, record.partition))
            .and_modify(|offset| *offset = (*offset).min(record.offset))
            .or_insert(record.offset);
    }
    for ((topic, partition), offset) in offsets {
        let _ = consumer.seek(
            topic,
            partition,
            Offset::Offset(offset),
            Duration::from_secs(1),
        );
    }
}

struct RaftForwardJob {
    batches: Vec<Vec<[u8; wire::MSG_LEN]>>,
    reply: std::sync::mpsc::SyncSender<Result<(), String>>,
}

#[derive(Clone)]
struct RaftForwarder {
    groups: Arc<Vec<std::sync::mpsc::SyncSender<RaftForwardJob>>>,
}

impl RaftForwarder {
    fn spawn(target_groups: Vec<Vec<MatcherTarget>>, backpressure: Arc<Backpressure>) -> Self {
        let queue_capacity = env_number("TC_RAFT_FORWARD_QUEUE", 64usize).max(1);
        let mut groups = Vec::with_capacity(target_groups.len());
        for (group, targets) in target_groups.into_iter().enumerate() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<RaftForwardJob>(queue_capacity);
            let worker_backpressure = backpressure.clone();
            std::thread::Builder::new()
                .name(format!("raft-forward-{group}"))
                .spawn(move || {
                    let mut matcher = MatcherConnection::new(targets);
                    while let Ok(job) = rx.recv() {
                        let result = job.batches.iter().try_for_each(|frames| {
                            matcher.send_frames(frames).map_err(|error| {
                                format!("Raft group {group} batch commit failed: {error}")
                            })
                        });
                        worker_backpressure.set_group_health(group, result.is_ok());
                        let _ = job.reply.send(result);
                    }
                })
                .expect("spawn Raft forwarding worker");
            groups.push(tx);
        }
        Self {
            groups: Arc::new(groups),
        }
    }

    fn submit(
        &self,
        records: &[KafkaRecord],
        control: &trade_core::cluster::RouteControlPlane,
    ) -> Result<(), String> {
        if self.groups.is_empty() {
            return Err("no Raft groups configured".into());
        }
        let mut grouped = (0..self.groups.len())
            .map(|_| BTreeMap::<u32, Vec<[u8; wire::MSG_LEN]>>::new())
            .collect::<Vec<_>>();
        for record in records {
            if !control.accepts(record.category_id) {
                return Err(format!(
                    "category {} is frozen for migration",
                    record.category_id
                ));
            }
            let group = control.group_for(record.category_id);
            grouped[group]
                .entry(record.category_id)
                .or_default()
                .push(record.frame);
        }

        let mut replies = Vec::new();
        for (group, categories) in grouped.into_iter().enumerate() {
            if categories.is_empty() {
                continue;
            }
            let batches = categories
                .into_values()
                .flat_map(|frames| {
                    frames
                        .chunks(wire::RAFT_BATCH_MAX_COMMANDS)
                        .map(|chunk| chunk.to_vec())
                        .collect::<Vec<_>>()
                })
                .collect();
            let (reply, result) = std::sync::mpsc::sync_channel(1);
            self.groups[group]
                .send(RaftForwardJob { batches, reply })
                .map_err(|_| format!("Raft group {group} forwarding worker stopped"))?;
            replies.push(result);
        }
        for reply in replies {
            reply
                .recv()
                .map_err(|_| "Raft forwarding worker dropped acknowledgement".to_string())??;
        }
        Ok(())
    }
}

fn forward_kafka_batch(
    forwarder: &RaftForwarder,
    records: &[KafkaRecord],
    control: &trade_core::cluster::RouteControlPlane,
) -> Result<(), String> {
    if forwarder.groups.is_empty() {
        return Err("no Raft groups configured".into());
    }
    forwarder.submit(records, control)
}

fn route_command_db_batch(
    kafka: &KafkaIngress,
    routing: sharding::RouteConfig,
    records: &[KafkaRecord],
) -> Result<(), String> {
    assert!(
        kafka.db_route_partitions >= routing.db_count as u32,
        "command DB route partitions must cover every physical DB"
    );
    let prepared = records
        .iter()
        .map(|record| {
            let command = wire::WireView::parse(&record.frame)
                .and_then(|view| view.to_command())
                .expect("validated command Kafka frame");
            let order_id = database_order_id(&command);
            let partition = routing.execution_partition(order_id, kafka.db_route_partitions);
            let payload = encode_envelope(record.user, routing.route_version, &record.frame);
            let key = order_id.to_be_bytes();
            (partition, key, payload)
        })
        .collect::<Vec<_>>();
    let deliveries = futures::executor::block_on(futures::future::join_all(prepared.iter().map(
        |(partition, key, payload)| {
            kafka.producer.send(
                FutureRecord::to(&kafka.db_route_topic)
                    .partition(*partition as i32)
                    .key(key)
                    .payload(payload),
                Duration::from_secs(5),
            )
        },
    )));
    deliveries
        .into_iter()
        .find_map(Result::err)
        .map_or(Ok(()), |(error, _)| Err(error.to_string()))
}

fn decode_db_kafka_record(
    message: &rdkafka::message::BorrowedMessage<'_>,
    category_size: u32,
    routing: sharding::RouteConfig,
) -> Result<KafkaRecord, String> {
    let payload = message.payload().ok_or("Kafka message has no payload")?;
    let envelope = QueueEnvelope::decode(payload).ok_or("invalid DB command envelope")?;
    if envelope.route_version != routing.route_version {
        return Err(format!(
            "stale DB route version {}, expected {}",
            envelope.route_version, routing.route_version
        ));
    }
    let command = wire::WireView::parse(envelope.frame)
        .and_then(|view| view.to_command())
        .ok_or("invalid DB command frame")?;
    let expected = routing.execution_partition(
        database_order_id(&command),
        env_number("TC_ORDER_DB_KAFKA_PARTITIONS", routing.db_count as u32),
    );
    if message.partition() != expected as i32 {
        return Err(format!(
            "DB command partition {} does not match expected {expected}",
            message.partition()
        ));
    }
    Ok(KafkaRecord {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
        category_id: sharding::asset_category(command.instrument(), category_size),
        user: envelope.user,
        frame: *envelope.frame,
    })
}

fn run_db_kafka_stage(
    kafka: KafkaIngress,
    category_size: u32,
    worker: usize,
    forwarder: CommandDbForwarder,
) {
    let consumer: BaseConsumer = kafka_consumer_config(&kafka.brokers, &kafka.db_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("create routed command DB consumer");
    let usable = kafka.db_route_partitions / forwarder.routing.db_count as u32
        * forwarder.routing.db_count as u32;
    let owned = (0..usable)
        .filter(|partition| forwarder.owns(*partition % forwarder.routing.db_count as u32))
        .collect::<Vec<_>>();
    let mut assignment = TopicPartitionList::new();
    for (position, partition) in owned.into_iter().enumerate() {
        if position % kafka.db_consumers.max(1) == worker {
            assignment
                .add_partition_offset(&kafka.db_route_topic, partition as i32, Offset::Stored)
                .expect("assign routed command DB partition");
        }
    }
    consumer
        .assign(&assignment)
        .expect("assign routed command DB ownership");
    loop {
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let mut batch = Vec::with_capacity(kafka.batch_size);
        if let Ok(message) = first {
            if let Ok(record) = decode_db_kafka_record(&message, category_size, forwarder.routing) {
                batch.push(record);
            }
        }
        let deadline = std::time::Instant::now() + kafka.linger;
        while batch.len() < kafka.batch_size && std::time::Instant::now() < deadline {
            match consumer.poll(deadline.saturating_duration_since(std::time::Instant::now())) {
                Some(Ok(message)) => {
                    if let Ok(record) =
                        decode_db_kafka_record(&message, category_size, forwarder.routing)
                    {
                        batch.push(record);
                    }
                }
                _ => break,
            }
        }
        if batch.is_empty() {
            continue;
        }
        match forwarder
            .submit(forwarder.routing, &batch)
            .and_then(|()| commit_kafka_batch(&consumer, &batch, kafka.async_offset_commits))
        {
            Ok(()) => kafka.metrics.complete("mysql", batch.len() as u64),
            Err(error) => {
                log_warn!("order-db", worker; "event=batch_retry error={error}");
                rewind_kafka_batch(&consumer, &batch);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn run_kafka_stage<F>(
    kafka: KafkaIngress,
    category_size: u32,
    group_id: String,
    stage: &'static str,
    worker: usize,
    mut process: F,
) where
    F: FnMut(&[KafkaRecord]) -> Result<(), String>,
{
    // Matching has a much more expensive durability boundary than the command
    // DB projection: each Raft application batch crosses quorum WAL plus the
    // matching WAL/outbox barrier.  Give it an independent, larger microbatch
    // so scaling match throughput does not also inflate MySQL transactions.
    let (batch_size, linger) = if stage == "match" {
        (
            env_number("TC_ORDER_MATCH_BATCH_SIZE", wire::RAFT_BATCH_MAX_COMMANDS)
                .clamp(1, wire::RAFT_BATCH_MAX_COMMANDS),
            Duration::from_millis(env_number(
                "TC_ORDER_MATCH_BATCH_LINGER_MS",
                kafka.linger.as_millis() as u64,
            )),
        )
    } else {
        (kafka.batch_size, kafka.linger)
    };
    let consumer: BaseConsumer = kafka_consumer_config(&kafka.brokers, &group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()
        .expect("create Kafka order consumer");
    let topics = kafka
        .router
        .topics()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    consumer.subscribe(&topics).expect("subscribe order topics");
    log_info!(
        "order-kafka", format_args!("{stage}-{worker}");
        "group={group_id} subscribed to {} queue groups",
        topics.len(),
    );
    let mut consecutive_failures = 0u32;
    let mut last_failure_log = std::time::Instant::now() - Duration::from_secs(30);

    loop {
        let mut batch = Vec::with_capacity(batch_size);
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        match first {
            Ok(message) => match decode_kafka_record(&message, &kafka.router, category_size) {
                Ok(record) => batch.push(record),
                Err(error) => {
                    log_warn!("order-kafka", format_args!("{stage}-{worker}"); "event=rejected_message partition={} offset={} error={error}", message.partition(), message.offset())
                }
            },
            Err(error) => {
                if last_failure_log.elapsed() >= Duration::from_secs(30) {
                    log_warn!("order-kafka", format_args!("{stage}-{worker}"); "event=poll_failed error={error}");
                    last_failure_log = std::time::Instant::now();
                }
                continue;
            }
        }
        let deadline = std::time::Instant::now() + linger;
        while batch.len() < batch_size && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(message)) => {
                    match decode_kafka_record(&message, &kafka.router, category_size) {
                        Ok(record) => batch.push(record),
                        Err(error) => {
                            log_warn!("order-kafka", format_args!("{stage}-{worker}"); "event=rejected_message partition={} offset={} error={error}", message.partition(), message.offset())
                        }
                    }
                }
                Some(Err(error)) => {
                    if last_failure_log.elapsed() >= Duration::from_secs(30) {
                        log_warn!("order-kafka", format_args!("{stage}-{worker}"); "event=poll_failed error={error}");
                        last_failure_log = std::time::Instant::now();
                    }
                }
                None => break,
            }
        }
        match process(&batch)
            .and_then(|()| commit_kafka_batch(&consumer, &batch, kafka.async_offset_commits))
        {
            Ok(()) => {
                kafka.metrics.complete(stage, batch.len() as u64);
                consecutive_failures = 0;
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures == 1
                    || last_failure_log.elapsed() >= Duration::from_secs(30)
                {
                    log_warn!(
                        "order-kafka", format_args!("{stage}-{worker}");
                        "event=batch_retained_for_retry failures={consecutive_failures} error={error}"
                    );
                    last_failure_log = std::time::Instant::now();
                }
                rewind_kafka_batch(&consumer, &batch);
                let shift = consecutive_failures.saturating_sub(1).min(6);
                let backoff_ms = (100u64 << shift).min(5_000);
                std::thread::sleep(Duration::from_millis(backoff_ms));
            }
        }
    }
}

fn kafka_consumer_config(brokers: &str, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set(
            "fetch.min.bytes",
            env_number("TC_KAFKA_FETCH_MIN_BYTES", 1usize).to_string(),
        )
        .set(
            "fetch.wait.max.ms",
            env_number("TC_KAFKA_FETCH_WAIT_MS", 10u64).to_string(),
        )
        .set(
            "fetch.message.max.bytes",
            env_number("TC_KAFKA_FETCH_MAX_BYTES", 52_428_800usize).to_string(),
        );
    config
}

/// Per-partition consumer lag, indexed by the router's global partition index
/// (`topic_pos * partitions_per_topic + partition`). Per-category backpressure
/// keys off this; the caller also sums it for the market-wide backlog switch.
fn read_consumer_group_lag(
    consumer: &BaseConsumer,
    kafka: &KafkaIngress,
) -> Result<Vec<u64>, String> {
    let topics = kafka.router.topics();
    let per_topic = kafka.partitions_per_topic as i32;
    let mut requested = TopicPartitionList::new();
    for topic in topics {
        for partition in 0..per_topic {
            requested.add_partition(topic, partition);
        }
    }
    let committed = consumer
        .committed_offsets(requested, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    let mut committed_by_tp: HashMap<(String, i32), i64> = HashMap::new();
    for element in committed.elements() {
        let offset = match element.offset() {
            Offset::Offset(offset) => offset,
            _ => 0,
        };
        committed_by_tp.insert((element.topic().to_string(), element.partition()), offset);
    }
    let mut lags = Vec::with_capacity(topics.len() * per_topic as usize);
    for topic in topics {
        for partition in 0..per_topic {
            let offset = committed_by_tp
                .get(&(topic.clone(), partition))
                .copied()
                .unwrap_or(0);
            let (_, high) = consumer
                .fetch_watermarks(topic, partition, Duration::from_secs(2))
                .map_err(|error| error.to_string())?;
            lags.push(high.saturating_sub(offset).max(0) as u64);
        }
    }
    Ok(lags)
}

fn run_consumer_lag_monitor(kafka: KafkaIngress, group_id: String, stage: &'static str) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        .set("group.id", &group_id)
        .create()
        .expect("create Kafka lag monitor");
    let mut last_error_log = std::time::Instant::now() - Duration::from_secs(30);
    loop {
        match read_consumer_group_lag(&consumer, &kafka) {
            Ok(lags) => {
                let total: u64 = lags.iter().sum();
                // Global backlog master switch keeps the summed view; the
                // per-category tier machine keeps the per-partition breakdown.
                kafka.metrics.set_observed_lag(stage, total);
                kafka.backpressure.set_partition_lags(stage, &lags);
            }
            Err(error) if last_error_log.elapsed() >= Duration::from_secs(30) => {
                log_warn!("order-kafka", format_args!("{stage}-lag"); "event=lag_read_failed group={group_id} error={error}");
                last_error_log = std::time::Instant::now();
            }
            Err(_) => {}
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn run_execution_mysql_consumer(
    forwarder: ExecutionDbForwarder,
    metrics: Arc<OrderPipelineMetrics>,
    kafka: KafkaIngress,
    worker: usize,
    dlq: Arc<Mutex<DlqWriter>>,
) {
    let consumer: BaseConsumer = kafka_consumer_config(&kafka.brokers, &kafka.execution_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()
        .expect("create execution MySQL consumer");
    assert!(
        kafka.execution_partitions >= forwarder.routing.db_count as u32,
        "execution partitions must be >= physical DB count for DB ownership routing"
    );
    let mut assignment = TopicPartitionList::new();
    let usable_execution_partitions = kafka.execution_partitions
        / forwarder.routing.db_count as u32
        * forwarder.routing.db_count as u32;
    let owned_partitions = (0..usable_execution_partitions)
        .filter(|partition| forwarder.owns(*partition % forwarder.routing.db_count as u32))
        .collect::<Vec<_>>();
    for (position, partition) in owned_partitions.into_iter().enumerate() {
        if position % kafka.execution_consumers.max(1) == worker {
            assignment
                .add_partition_offset(&kafka.execution_topic, partition as i32, Offset::Stored)
                .expect("assign owned execution partition");
        }
    }
    consumer
        .assign(&assignment)
        .expect("assign execution DB ownership partitions");
    // Bounded retry then dead-letter: a poison message must never wedge the
    // partition (was an unbounded seek-and-retry loop).
    let max_retries = env_number("TC_ORDER_PERSIST_MAX_RETRIES", 10u32).max(1);
    let base_backoff = Duration::from_millis(env_number("TC_ORDER_PERSIST_RETRY_BASE_MS", 100u64));
    log_info!(
        "execution-mysql", worker;
        "group={} topic={} batch_size={} max_retries={max_retries}",
        kafka.execution_group, kafka.execution_topic, kafka.execution_batch_size
    );
    let mut last_failure_log = std::time::Instant::now() - Duration::from_secs(30);
    loop {
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let first = match first {
            Ok(message) => message,
            Err(error) => {
                if last_failure_log.elapsed() >= Duration::from_secs(30) {
                    log_warn!("execution-mysql", worker; "event=poll_failed error={error}");
                    last_failure_log = std::time::Instant::now();
                }
                continue;
            }
        };
        let mut records = Vec::with_capacity(kafka.execution_batch_size);
        push_execution_record(
            &metrics,
            &dlq,
            &consumer,
            worker,
            kafka.async_offset_commits,
            &first,
            &mut records,
        );
        let deadline = std::time::Instant::now() + kafka.execution_linger;
        while records.len() < kafka.execution_batch_size && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(message)) => push_execution_record(
                    &metrics,
                    &dlq,
                    &consumer,
                    worker,
                    kafka.async_offset_commits,
                    &message,
                    &mut records,
                ),
                Some(Err(error)) => {
                    log_warn!("execution-mysql", worker; "event=poll_failed error={error}")
                }
                None => break,
            }
        }
        if records.is_empty() {
            continue;
        }
        let persist_started = std::time::Instant::now();
        let mut committed = false;
        for attempt in 0..max_retries {
            match forwarder.submit(&records) {
                Ok(()) => {
                    committed = true;
                    break;
                }
                Err(error) if attempt + 1 < max_retries => {
                    let shift = attempt.min(6);
                    std::thread::sleep(base_backoff * (1u32 << shift));
                    if last_failure_log.elapsed() >= Duration::from_secs(30) {
                        log_warn!("execution-mysql", worker; "event=batch_retry attempt={} records={} error={error}", attempt + 1, records.len());
                        last_failure_log = std::time::Instant::now();
                    }
                }
                Err(error) => {
                    log_error!("execution-mysql", worker; "event=batch_failed records={} error={error}", records.len());
                }
            }
        }
        if committed {
            metrics.record_execution_mysql_batch(persist_started.elapsed(), records.len() as u64);
            if let Err(error) =
                commit_execution_batch(&consumer, &records, kafka.async_offset_commits)
            {
                log_error!("execution-mysql", worker; "event=offset_commit_failed error={error}");
            }
        } else {
            rewind_execution_batch(&consumer, &records);
        }
    }
}

#[derive(Clone)]
struct ExecutionKafkaRecord {
    topic: String,
    partition: i32,
    offset: i64,
    event: wire::ExecutionEvent,
}

fn push_execution_record(
    metrics: &OrderPipelineMetrics,
    dlq: &Arc<Mutex<DlqWriter>>,
    consumer: &BaseConsumer,
    worker: usize,
    async_commit: bool,
    message: &rdkafka::message::BorrowedMessage<'_>,
    records: &mut Vec<ExecutionKafkaRecord>,
) {
    let partition = message.partition();
    let offset = message.offset();
    if let Some(event) = message.payload().and_then(wire::decode_execution_event) {
        records.push(ExecutionKafkaRecord {
            topic: message.topic().to_string(),
            partition,
            offset,
            event,
        });
        return;
    }
    let payload = message.payload().unwrap_or(&[]);
    if let Ok(mut writer) = dlq.lock() {
        if writer
            .append(partition, offset, "invalid execution report", payload)
            .is_ok()
        {
            metrics.dlq_total.fetch_add(1, AtomicOrdering::Relaxed);
            let _ = consumer.commit_message(
                message,
                if async_commit {
                    CommitMode::Async
                } else {
                    CommitMode::Sync
                },
            );
            log_warn!("execution-mysql", worker; "event=dead_lettered reason=undecodable partition={partition} offset={offset}");
        }
    }
}

fn commit_execution_batch(
    consumer: &BaseConsumer,
    records: &[ExecutionKafkaRecord],
    async_commit: bool,
) -> Result<(), String> {
    let mut offsets: HashMap<(&str, i32), i64> = HashMap::new();
    for record in records {
        offsets
            .entry((&record.topic, record.partition))
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }
    let mut list = TopicPartitionList::new();
    for ((topic, partition), offset) in offsets {
        list.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|error| error.to_string())?;
    }
    consumer
        .commit(
            &list,
            if async_commit {
                CommitMode::Async
            } else {
                CommitMode::Sync
            },
        )
        .map_err(|error| error.to_string())
}

fn rewind_execution_batch(consumer: &BaseConsumer, records: &[ExecutionKafkaRecord]) {
    let mut offsets: HashMap<(&str, i32), i64> = HashMap::new();
    for record in records {
        offsets
            .entry((&record.topic, record.partition))
            .and_modify(|offset| *offset = (*offset).min(record.offset))
            .or_insert(record.offset);
    }
    for ((topic, partition), offset) in offsets {
        let _ = consumer.seek(
            topic,
            partition,
            Offset::Offset(offset),
            Duration::from_secs(1),
        );
    }
}

type ExecutionShardTables = HashMap<u32, Vec<(ExecutionKafkaRecord, u64)>>;

struct ExecutionShardJob {
    tables: ExecutionShardTables,
    reply: std::sync::mpsc::SyncSender<Result<(), String>>,
}

#[derive(Clone)]
struct ExecutionDbForwarder {
    routing: sharding::RouteConfig,
    shards: Arc<Vec<Option<std::sync::mpsc::SyncSender<ExecutionShardJob>>>>,
    owned_dbs: Arc<std::collections::HashSet<u32>>,
}

impl ExecutionDbForwarder {
    fn spawn(store: OrderStore) -> Self {
        let queue = env_number("TC_MYSQL_SHARD_QUEUE", 64usize).max(1);
        let coalesce_jobs = env_number("TC_MYSQL_SHARD_COALESCE_JOBS", 16usize).max(1);
        let tx_max_records = env_number("TC_EXECUTION_MYSQL_BATCH_PER_DB", 500usize).max(1);
        let mut shards = Vec::with_capacity(store.routing.db_count as usize);
        for shard_db in 0..store.routing.db_count as u32 {
            if !store.owns(shard_db) {
                shards.push(None);
                continue;
            }
            let (tx, rx) = std::sync::mpsc::sync_channel::<ExecutionShardJob>(queue);
            let shard_store = store.clone();
            std::thread::Builder::new()
                .name(format!("execution-db-shard-{shard_db}"))
                .spawn(move || {
                    loop {
                        let first = match rx.recv_timeout(Duration::from_millis(250)) {
                            Ok(first) => first,
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                if let Err(error) = sweep_pending_execution_shard(
                                    &shard_store,
                                    shard_db,
                                ) {
                                    log_warn!("execution-db", shard_db; "event=pending_sweep_failed error={error}");
                                }
                                continue;
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        let mut jobs = vec![first];
                        while jobs.len() < coalesce_jobs {
                            let Ok(job) = rx.try_recv() else { break };
                            jobs.push(job);
                        }
                        let mut tables = ExecutionShardTables::new();
                        for job in &jobs {
                            for (&table, records) in &job.tables {
                                tables
                                    .entry(table)
                                    .or_default()
                                    .extend(records.iter().cloned());
                            }
                        }
                        let result = persist_execution_shard(
                            &shard_store,
                            shard_db,
                            tables,
                            tx_max_records,
                        );
                        for job in jobs {
                            let _ = job.reply.send(result.clone());
                        }
                    }
                })
                .expect("spawn execution DB shard worker");
            shards.push(Some(tx));
        }
        Self {
            routing: store.routing,
            shards: Arc::new(shards),
            owned_dbs: store.owned_dbs.clone(),
        }
    }

    fn submit(&self, records: &[ExecutionKafkaRecord]) -> Result<(), String> {
        let mut by_db = HashMap::<u32, ExecutionShardTables>::new();
        for record in records {
            let order_id = record.event.target_order_id;
            let route = self.routing.route_order_id(order_id);
            by_db
                .entry(route.db)
                .or_default()
                .entry(route.table)
                .or_default()
                .push((record.clone(), order_id));
        }
        let mut replies = Vec::with_capacity(by_db.len());
        for (db, tables) in by_db {
            let (reply, result) = std::sync::mpsc::sync_channel(1);
            self.shards[db as usize]
                .as_ref()
                .ok_or_else(|| format!("execution DB {db} is not owned by this worker"))?
                .send(ExecutionShardJob { tables, reply })
                .map_err(|_| format!("execution DB shard {db} worker stopped"))?;
            replies.push(result);
        }
        for reply in replies {
            reply
                .recv()
                .map_err(|_| "execution DB shard worker dropped reply".to_string())??;
        }
        Ok(())
    }

    fn owns(&self, db: u32) -> bool {
        self.owned_dbs.contains(&db)
    }
}

fn sweep_pending_execution_shard(store: &OrderStore, shard_db: u32) -> Result<(), String> {
    let db = format!("order_db_{shard_db}");
    let mut conn = store
        .shard(shard_db)
        .get_conn()
        .map_err(|error| error.to_string())?;
    let tables: Vec<u32> = conn
        .query(format!(
            "SELECT DISTINCT shard_table FROM {db}.pending_execution_events LIMIT 100"
        ))
        .map_err(|error| error.to_string())?;
    if tables.is_empty() {
        return Ok(());
    }
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .map_err(|error| error.to_string())?;
    for table in tables {
        apply_pending_execution_table(&mut tx, &db, table)?;
    }
    tx.commit().map_err(|error| error.to_string())
}

fn persist_execution_shard(
    store: &OrderStore,
    shard_db: u32,
    tables: ExecutionShardTables,
    tx_max_records: usize,
) -> Result<(), String> {
    let db = format!("order_db_{shard_db}");
    let mut conn = store
        .shard(shard_db)
        .get_conn()
        .map_err(|error| error.to_string())?;
    let mut updates = Vec::new();
    for (shard_table, records) in tables {
        for (record, target_order_id) in records {
            updates.push((shard_table, record, target_order_id));
        }
    }
    // Coalescing several consumer jobs is useful for queue efficiency, but it
    // must not silently create an unbounded transaction. Chunk the merged work
    // at the configured hard limit; Kafka offsets advance only after every
    // chunk succeeds, so a partial retry remains safe through event idempotency.
    for chunk in updates.chunks(tx_max_records) {
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(|error| error.to_string())?;
        let mut values = Vec::with_capacity(chunk.len() * 10);
        let mut touched = std::collections::BTreeSet::new();
        for (table, record, target_order_id) in chunk {
            touched.insert(*table);
            let event = record.event;
            values.extend([
                Value::from(event.raft_group),
                Value::from(event.raft_index),
                Value::from(event.ordinal),
                Value::from(*target_order_id),
                Value::from(*table),
                Value::from(event.report.instrument.0),
                Value::from(event.report.type_code),
                Value::from(event.report.qty),
                Value::from(record.partition),
                Value::from(record.offset as u64),
            ]);
        }
        exec_multi_insert(
            &mut tx,
            &format!("INSERT IGNORE INTO {db}.pending_execution_events (raft_group,raft_index,report_ordinal,target_order_id,shard_table,instrument,report_type,event_qty,kafka_partition,kafka_offset)"),
            10,
            values,
        )
        .map_err(|error| error.to_string())?;
        // A Kafka retry can reinsert an already-processed event into pending;
        // discard it before applying any mutations.
        tx.query_drop(format!(
            "DELETE p FROM {db}.pending_execution_events p INNER JOIN {db}.processed_execution_events e ON e.raft_group=p.raft_group AND e.raft_index=p.raft_index AND e.report_ordinal=p.report_ordinal AND e.target_order_id=p.target_order_id"
        ))
        .map_err(|error| error.to_string())?;
        for table in touched {
            apply_pending_execution_table(&mut tx, &db, table)?;
        }
        tx.commit().map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn apply_pending_execution_table(
    tx: &mut Transaction<'_>,
    db: &str,
    shard_table: u32,
) -> Result<(), String> {
    let table = format!("asset_orders_{shard_table:03}");
    // MySQL 8 window functions select the last event per order while the
    // aggregate folds all trades into one set-based row mutation.
    tx.query_drop(format!(
        "UPDATE {db}.{table} o INNER JOIN (\
         SELECT target_order_id,\
                SUM(IF(report_type={trade},event_qty,0)) trade_qty,\
                MAX(IF(report_type={partial},event_qty,0)) partial_qty,\
                MAX(IF(report_type={filled},1,0)) has_filled,\
                MAX(IF(rn=1,report_type,0)) latest_type\
         FROM (\
           SELECT p.*, ROW_NUMBER() OVER (PARTITION BY target_order_id ORDER BY raft_index DESC,report_ordinal DESC) rn\
           FROM {db}.pending_execution_events p WHERE shard_table={shard_table}\
         ) ranked GROUP BY target_order_id\
         ) e ON e.target_order_id=o.order_id\
         SET o.status=CASE\
               WHEN e.has_filled=1 OR GREATEST(o.filled_qty+e.trade_qty,e.partial_qty)>=o.qty THEN 'FILLED'\
               WHEN e.latest_type={cancelled} THEN 'CANCELLED'\
               WHEN e.latest_type={rejected} THEN 'REJECTED'\
               WHEN GREATEST(o.filled_qty+e.trade_qty,e.partial_qty)>0 THEN 'PARTIAL'\
               ELSE 'OPEN' END,\
             o.filled_qty=LEAST(o.qty,GREATEST(o.filled_qty+e.trade_qty,e.partial_qty,IF(e.has_filled=1,o.qty,0)))",
        trade = wire::RT_TRADE,
        partial = wire::RT_PARTIAL,
        filled = wire::RT_FILLED,
        cancelled = wire::RT_CANCELLED,
        rejected = wire::RT_REJECTED,
    ))
    .map_err(|error| error.to_string())?;
    tx.query_drop(format!(
        "INSERT IGNORE INTO {db}.processed_execution_events (raft_group,raft_index,report_ordinal,target_order_id,shard_table,instrument,report_type,kafka_partition,kafka_offset) \
         SELECT p.raft_group,p.raft_index,p.report_ordinal,p.target_order_id,p.shard_table,p.instrument,p.report_type,p.kafka_partition,p.kafka_offset \
         FROM {db}.pending_execution_events p INNER JOIN {db}.{table} o ON o.order_id=p.target_order_id WHERE p.shard_table={shard_table}"
    ))
    .map_err(|error| error.to_string())?;
    tx.query_drop(format!(
        "DELETE p FROM {db}.pending_execution_events p INNER JOIN {db}.processed_execution_events e ON e.raft_group=p.raft_group AND e.raft_index=p.raft_index AND e.report_ordinal=p.report_ordinal AND e.target_order_id=p.target_order_id WHERE p.shard_table={shard_table}"
    ))
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// One MySQL URL per physical database. An explicit `TC_ORDER_MYSQL_SHARD_URLS`
/// list must match the configured `db_count` (part of the versioned route
/// record); otherwise a single `TC_ORDER_MYSQL_URL` is fanned out to every
/// database.
fn parse_shard_urls(db_count: usize) -> Vec<String> {
    if let Ok(value) = std::env::var("TC_ORDER_MYSQL_SHARD_URLS") {
        let urls = value
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        match validate_shard_urls(&urls, db_count) {
            Ok(()) => return urls,
            Err(error) => log_warn!("order-api", "{error}"),
        }
    }
    let url = std::env::var("TC_ORDER_MYSQL_URL").expect("TC_ORDER_MYSQL_URL");
    vec![url; db_count]
}

/// Startup guard: the shard URL count must equal `db_count` from the route
/// record, or rows would route to a database that has no connection pool.
fn validate_shard_urls(urls: &[String], db_count: usize) -> Result<(), String> {
    if urls.len() == db_count {
        Ok(())
    } else {
        Err(format!(
            "TC_ORDER_MYSQL_SHARD_URLS must contain {db_count} urls to match TC_DB_COUNT; got {}",
            urls.len()
        ))
    }
}

fn open_when_ready(
    shard_urls: &[String],
    routing: sharding::RouteConfig,
    metrics: Arc<OrderPipelineMetrics>,
    owned_dbs: Arc<std::collections::HashSet<u32>>,
) -> OrderStore {
    loop {
        let opened = (|| -> mysql::Result<OrderStore> {
            let mut shards = Vec::with_capacity(shard_urls.len());
            for (idx, url) in shard_urls.iter().enumerate() {
                if !owned_dbs.contains(&(idx as u32)) {
                    shards.push(None);
                    continue;
                }
                // Probe each shard eagerly so a bad shard is named in the
                // startup warning (Pool::new is lazy; without this the retry
                // loop reports bare auth/DNS errors with no shard context).
                let pool = Pool::new(url.as_str())
                    .map_err(|e| mysql::Error::from(io_shard_error(idx, &e)))?;
                pool.get_conn()
                    .map_err(|e| mysql::Error::from(io_shard_error(idx, &e)))?;
                shards.push(Some(pool));
            }
            let store = OrderStore {
                shards: Arc::new(shards),
                owned_dbs: owned_dbs.clone(),
                routing,
                metrics: metrics.clone(),
            };
            if env_enabled("TC_MYSQL_BOOTSTRAP_SCHEMA", true) {
                bootstrap(&store)?;
            }
            Ok(store)
        })();
        match opened {
            Ok(store) => return store,
            Err(error) => {
                log_warn!("order-api", "waiting for MySQL/bootstrap: {error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn parse_owned_dbs(db_count: u32) -> Result<std::collections::HashSet<u32>, String> {
    let Some(spec) = std::env::var("TC_MYSQL_OWNED_DBS").ok() else {
        return Ok((0..db_count).collect());
    };
    let mut owned = std::collections::HashSet::new();
    for token in spec
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some((start, end)) = token.split_once('-') {
            let start: u32 = start
                .parse()
                .map_err(|_| format!("invalid DB range {token}"))?;
            let end: u32 = end
                .parse()
                .map_err(|_| format!("invalid DB range {token}"))?;
            if start > end {
                return Err(format!("invalid descending DB range {token}"));
            }
            owned.extend(start..=end);
        } else {
            owned.insert(
                token
                    .parse()
                    .map_err(|_| format!("invalid DB id {token}"))?,
            );
        }
    }
    if owned.is_empty() || owned.iter().any(|db| *db >= db_count) {
        return Err(format!(
            "TC_MYSQL_OWNED_DBS must select DB ids in 0..{db_count}"
        ));
    }
    Ok(owned)
}

/// Wrap a shard connection error with the shard index (never the URL — it
/// carries credentials).
fn io_shard_error(shard: usize, error: &dyn std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!("shard {shard}: {error}"))
}

fn role_needs_mysql(db_consumers: usize, execution_consumers: usize) -> bool {
    db_consumers > 0 || execution_consumers > 0
}

fn category_size() -> u32 {
    std::env::var("TC_ORDER_CATEGORY_SIZE")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ASSET_CATEGORY_SIZE)
}

fn is_leader(metrics_addr: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(metrics_addr) else {
        return false;
    };
    let timeout = Duration::from_millis(env_number("TC_RAFT_LEADER_PROBE_TIMEOUT_MS", 1_000u64));
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: raft\r\nConnection: close\r\n\r\n");
    let mut response = String::new();
    if std::io::Read::read_to_string(&mut stream, &mut response).is_err() {
        return false;
    }
    let has = |metric: &str, expected: &str| {
        response.lines().any(|line| {
            line.split_once(' ')
                .is_some_and(|(name, value)| name == metric && value.trim() == expected)
        })
    };
    has("tc_raft_role", "2") && has("tc_ready", "1")
}

struct MatcherConnection {
    targets: Vec<MatcherTarget>,
    stream: Option<TcpStream>,
    active_target: Option<usize>,
}

impl MatcherConnection {
    fn new(targets: Vec<MatcherTarget>) -> Self {
        Self {
            targets,
            stream: None,
            active_target: None,
        }
    }

    fn connect(&mut self) -> std::io::Result<()> {
        for (index, target) in self.targets.iter().enumerate() {
            if target
                .metrics_addr
                .as_deref()
                .is_some_and(|metrics| !is_leader(metrics))
            {
                continue;
            }
            let Ok(stream) = TcpStream::connect(&target.order_addr) else {
                continue;
            };
            stream.set_nodelay(true)?;
            let timeout =
                Duration::from_millis(env_number("TC_RAFT_MATCHER_IO_TIMEOUT_MS", 5_000u64));
            stream.set_read_timeout(Some(timeout))?;
            stream.set_write_timeout(Some(timeout))?;
            self.stream = Some(stream);
            self.active_target = Some(index);
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no reachable Raft leader",
        ))
    }

    fn send_frames(&mut self, frames: &[[u8; wire::MSG_LEN]]) -> std::io::Result<()> {
        if frames.is_empty() || frames.len() > wire::RAFT_BATCH_MAX_COMMANDS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid matcher batch size",
            ));
        }
        let mut payload = Vec::with_capacity(8 + frames.len() * wire::MSG_LEN);
        payload.extend_from_slice(b"TCB1");
        payload.extend_from_slice(&(frames.len() as u32).to_be_bytes());
        for frame in frames {
            payload.extend_from_slice(frame);
        }
        for _ in 0..2 {
            if self.stream.is_none() {
                self.connect()?;
            }
            let stream = self.stream.as_mut().expect("connected matcher stream");
            let mut ack = [0u8; 9];
            if stream.write_all(&payload).is_ok()
                && stream.read_exact(&mut ack).is_ok()
                && ack[0] == 1
                && u64::from_be_bytes(ack[1..].try_into().expect("Raft ACK index")) > 0
            {
                return Ok(());
            }
            self.stream = None;
            self.active_target = None;
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "Raft leader connection failed before quorum commit acknowledgement",
        ))
    }
}

fn parse_matcher_list(value: &str) -> Vec<MatcherTarget> {
    value
        .split(',')
        .filter_map(|item| {
            let (order_addr, metrics_addr) = item.trim().split_once('@')?;
            Some(MatcherTarget {
                order_addr: order_addr.to_string(),
                metrics_addr: Some(metrics_addr.to_string()),
            })
        })
        .collect()
}

fn parse_matcher_groups() -> Vec<Vec<MatcherTarget>> {
    if let Ok(value) = std::env::var("TC_RAFT_GROUP_MATCHERS") {
        let groups = value.split(';').map(parse_matcher_list).collect::<Vec<_>>();
        if !groups.is_empty() && groups.iter().all(|group| !group.is_empty()) {
            return groups;
        }
        log_warn!("order-api", "ignored invalid TC_RAFT_GROUP_MATCHERS");
    }
    if let Ok(value) = std::env::var("TC_RAFT_MATCHERS") {
        let targets = parse_matcher_list(&value);
        if !targets.is_empty() {
            return vec![targets];
        }
    }
    vec![vec![MatcherTarget {
        order_addr: std::env::var("TC_MATCHER_ADDR").unwrap_or_else(|_| "trade-core:9001".into()),
        metrics_addr: None,
    }]]
}

fn respond(stream: &mut TcpStream, status: &str, body: &str, keep_alive: bool) {
    respond_content(stream, status, "application/json", body, keep_alive);
}

fn respond_content(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
    keep_alive: bool,
) {
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let _ = stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{body}", body.len()).as_bytes());
}

fn handle(
    stream: TcpStream,
    category_size: u32,
    metrics: Arc<OrderPipelineMetrics>,
    kafka: Arc<KafkaIngress>,
    token: &str,
    ingress_enabled: bool,
) {
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream);
    loop {
        let mut first = String::new();
        match reader.read_line(&mut first) {
            Ok(0) | Err(_) => return,
            Ok(_) if first.trim().is_empty() => continue,
            Ok(_) => {}
        }
        let mut parts = first.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let uri = parts.next().unwrap_or("/").to_string();
        let version = parts.next().unwrap_or("HTTP/1.0");
        let mut authorized = false;
        let mut keep_alive = version == "HTTP/1.1";
        let mut content_length = 0usize;
        loop {
            let mut header = String::new();
            match reader.read_line(&mut header) {
                Ok(0) | Err(_) => return,
                Ok(_) if header.trim().is_empty() => break,
                Ok(_) => {}
            }
            if let Some((key, value)) = header.split_once(':') {
                let value = value.trim();
                authorized |=
                    key.eq_ignore_ascii_case("authorization") && value == format!("Bearer {token}");
                if key.eq_ignore_ascii_case("connection") {
                    keep_alive = value.eq_ignore_ascii_case("keep-alive");
                }
                if key.eq_ignore_ascii_case("content-length") {
                    content_length = value.parse().unwrap_or(usize::MAX);
                }
            }
        }
        if content_length > 1 << 20 {
            respond(
                reader.get_mut(),
                "413 Payload Too Large",
                "{\"error\":\"payload too large\"}",
                false,
            );
            return;
        }
        let body = if content_length > 0 {
            let mut body = vec![0; content_length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
            body
        } else {
            Vec::new()
        };
        let (path, query) = uri.split_once('?').unwrap_or((&uri, ""));
        if method == "GET" && path == "/metrics" {
            respond_content(
                reader.get_mut(),
                "200 OK",
                "text/plain; version=0.0.4",
                &(metrics.render() + &kafka.backpressure.render()),
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if !authorized {
            respond(
                reader.get_mut(),
                "401 Unauthorized",
                "{\"error\":\"unauthorized\"}",
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if method == "POST" && path.starts_with("/admin/routes/") {
            let category = required::<u32>(query, "category");
            let result: Result<String, String> = category.and_then(|category| match path {
                "/admin/routes/begin" => kafka
                    .route_control
                    .begin(category, required::<usize>(query, "to_group")?)
                    .map(|migration| {
                        format!(
                            "{{\"accepted\":true,\"route_version\":{},\"state\":\"FREEZING\"}}",
                            migration.route_version
                        )
                    }),
                "/admin/routes/frozen" => kafka
                    .route_control
                    .frozen(category, required::<u64>(query, "raft_index")?)
                    .map(|_| "{\"accepted\":true,\"state\":\"CATCHING_UP\"}".into()),
                "/admin/routes/caught-up" => verify_caught_up(&kafka, category, query),
                "/admin/routes/activate" => kafka.route_control.activate(category).map(|version| {
                    format!(
                        "{{\"accepted\":true,\"route_version\":{version},\"state\":\"ACTIVE\"}}"
                    )
                }),
                "/admin/routes/abort" => Ok(format!(
                    "{{\"accepted\":{},\"state\":\"ABORTED\"}}",
                    kafka.route_control.abort(category)
                )),
                _ => Err("unknown route migration action".into()),
            });
            match result {
                Ok(body) => respond(reader.get_mut(), "200 OK", &body, keep_alive),
                Err(error) => respond(
                    reader.get_mut(),
                    "409 Conflict",
                    &format!("{{\"error\":\"{error}\"}}"),
                    keep_alive,
                ),
            }
            if !keep_alive {
                return;
            }
            continue;
        }
        if !ingress_enabled && method == "POST" {
            respond(
                reader.get_mut(),
                "503 Service Unavailable",
                "{\"error\":\"HTTP ingress disabled for this worker role\"}",
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }

        if method == "POST" && path == "/commands/batch" {
            match decode_batch(&body, category_size) {
                Ok(records) => match kafka.publish_batch(&records) {
                    Ok(()) => respond(
                        reader.get_mut(),
                        "202 Accepted",
                        &format!(
                            "{{\"accepted\":true,\"status\":\"PENDING\",\"count\":{}}}",
                            records.len()
                        ),
                        keep_alive,
                    ),
                    Err(error) => {
                        let status = ingress_error_status(&error);
                        // Rejected ingress is an異常 path: keep it correlatable
                        // (success stays silent — the metrics carry it).
                        log_warn!(
                            "order-api",
                            "event=batch_rejected status=\"{status}\" count={} error={error}",
                            records.len()
                        );
                        respond(
                            reader.get_mut(),
                            status,
                            &format!("{{\"error\":\"{error}\"}}"),
                            keep_alive,
                        )
                    }
                },
                Err(error) => {
                    log_warn!(
                        "order-api",
                        "event=batch_rejected status=\"400 Bad Request\" error={error}"
                    );
                    respond(
                        reader.get_mut(),
                        "400 Bad Request",
                        &format!("{{\"error\":\"{error}\"}}"),
                        keep_alive,
                    )
                }
            }
            if !keep_alive {
                return;
            }
            continue;
        }
        // (order_id, category) of the request, for correlating a rejection.
        let mut req_ctx: Option<(u64, u32)> = None;
        let persisted = match (method.as_str(), path) {
            ("POST", "/orders") => order_from_query(query).and_then(|order| {
                let mut frame = [0u8; wire::MSG_LEN];
                wire::encode_new(&order, &mut frame);
                let category = sharding::asset_category(order.instrument, category_size);
                req_ctx = Some((order.id.0, category));
                kafka.publish(category, order.user, &frame)
            }),
            ("POST", "/cancels") => cancel_from_query(query).and_then(|(i, o, c, u)| {
                let mut frame = [0u8; wire::MSG_LEN];
                wire::encode_cancel(i, o, c, &mut frame);
                let category = sharding::asset_category(i, category_size);
                req_ctx = Some((o.0, category));
                kafka.publish(category, u, &frame)
            }),
            _ => {
                respond(
                    reader.get_mut(),
                    "404 Not Found",
                    "{\"error\":\"not found\"}",
                    keep_alive,
                );
                if !keep_alive {
                    return;
                }
                continue;
            }
        };
        match persisted {
            Ok(seq) => respond(
                reader.get_mut(),
                "202 Accepted",
                &format!(
                    "{{\"accepted\":true,\"status\":\"PENDING\",\"category_id\":{},\"category_seq\":{}}}",
                    seq.category_id, seq.category_seq
                ),
                keep_alive,
            ),
            Err(error) => {
                let status = ingress_error_status(&error);
                match req_ctx {
                    Some((order_id, category)) => log_warn!(
                        "order-api",
                        "event=order_rejected status=\"{status}\" order_id={order_id} category={category} error={error}"
                    ),
                    None => log_warn!(
                        "order-api",
                        "event=order_rejected status=\"{status}\" error={error}"
                    ),
                }
                respond(
                    reader.get_mut(),
                    status,
                    &format!("{{\"error\":\"{error}\"}}"),
                    keep_alive,
                )
            }
        }
        if !keep_alive {
            return;
        }
    }
}

fn main() {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("order-api");
    install_signal_handlers();

    let routing = sharding::RouteConfig::from_env();
    let category_size = category_size();
    let metrics = Arc::new(OrderPipelineMetrics::default());
    let matcher_groups = parse_matcher_groups();
    let token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let kafka = KafkaIngress::from_env(metrics.clone(), matcher_groups.len())
        .expect("configure Kafka order ingress")
        .expect("TC_ORDER_KAFKA_BROKERS is required");
    // API ingress and matcher-forwarder roles do not touch MySQL. Requiring
    // every replica to open a pool to every shard creates O(processes*shards)
    // connections and repeats all bootstrap DDL, defeating horizontal scaling.
    let needs_mysql = role_needs_mysql(kafka.db_consumers, kafka.execution_consumers);
    let owned_dbs = Arc::new(
        parse_owned_dbs(routing.db_count as u32)
            .unwrap_or_else(|error| panic!("invalid MySQL ownership: {error}")),
    );
    let store = needs_mysql.then(|| {
        let shard_urls = parse_shard_urls(routing.db_count as usize);
        if let Err(error) = validate_shard_urls(&shard_urls, routing.db_count as usize) {
            panic!("shard routing mismatch: {error}");
        }
        open_when_ready(&shard_urls, routing, metrics.clone(), owned_dbs.clone())
    });
    let command_db_forwarder = (kafka.db_consumers > 0).then(|| {
        CommandDbForwarder::spawn(store.as_ref().expect("DB consumers require MySQL").clone())
    });
    let execution_db_forwarder = (kafka.execution_consumers > 0).then(|| {
        ExecutionDbForwarder::spawn(
            store
                .as_ref()
                .expect("execution DB consumers require MySQL")
                .clone(),
        )
    });
    let matcher_group_count = matcher_groups.len();
    let ingress_enabled = env_enabled("TC_ORDER_HTTP_INGRESS_ENABLED", true);
    let raft_forwarder = (kafka.matcher_consumers > 0)
        .then(|| RaftForwarder::spawn(matcher_groups, kafka.backpressure.clone()));
    let dlq_path =
        std::env::var("TC_ORDER_DLQ_PATH").unwrap_or_else(|_| "order-execution-dlq.wal".into());
    let dlq = Arc::new(Mutex::new(
        DlqWriter::open(Path::new(&dlq_path)).expect("open execution DLQ file"),
    ));
    log_info!(
        "order-api",
        "category_size={} db_count={} tables_per_db={} virtual_dbs={} virtual_tables={} route_version={} raft_groups={} db_router_consumers={} db_consumers={} matcher_consumers={} execution_consumers={} max_pipeline_backlog={} bp_soft={} bp_hard={} bp_emergency={} dlq={} db_group={} matcher_group={} execution_group={} kafka=true",
        category_size,
        routing.db_count,
        routing.tables_per_db,
        routing.virtual_db_count,
        routing.virtual_table_count,
        routing.route_version,
        matcher_group_count,
        kafka.db_router_consumers,
        kafka.db_consumers,
        kafka.matcher_consumers,
        kafka.execution_consumers,
        kafka.max_pipeline_backlog,
        kafka.backpressure.soft,
        kafka.backpressure.hard,
        kafka.backpressure.emergency,
        dlq_path,
        kafka.db_group,
        kafka.matcher_group,
        kafka.execution_group,
    );
    if ingress_enabled {
        for (stage, group_id) in [
            ("mysql", kafka.db_group.clone()),
            ("match", kafka.matcher_group.clone()),
        ] {
            let monitor_kafka = kafka.clone();
            std::thread::Builder::new()
                .name(format!("order-kafka-{stage}-lag"))
                .spawn(move || run_consumer_lag_monitor(monitor_kafka, group_id, stage))
                .expect("spawn Kafka lag monitor");
        }
    }
    for worker in 0..kafka.db_router_consumers {
        let router_kafka = kafka.clone();
        let group_id = kafka.db_router_group.clone();
        std::thread::Builder::new()
            .name(format!("order-kafka-db-route-{worker}"))
            .spawn(move || {
                let process_kafka = router_kafka.clone();
                run_kafka_stage(
                    router_kafka,
                    category_size,
                    group_id,
                    "db-route",
                    worker,
                    move |batch| route_command_db_batch(&process_kafka, routing, batch),
                )
            })
            .expect("spawn command DB route consumer");
    }
    for worker in 0..kafka.db_consumers {
        let consumer_forwarder = command_db_forwarder
            .as_ref()
            .expect("DB consumers require shard forwarder")
            .clone();
        let consumer_kafka = kafka.clone();
        let category_size = category_size;
        std::thread::Builder::new()
            .name(format!("order-kafka-mysql-{worker}"))
            .spawn(move || {
                run_db_kafka_stage(consumer_kafka, category_size, worker, consumer_forwarder)
            })
            .expect("spawn Kafka MySQL projection consumer");
    }
    for worker in 0..kafka.matcher_consumers {
        let consumer_kafka = kafka.clone();
        let group_id = kafka.matcher_group.clone();
        let category_size = category_size;
        let worker_forwarder = raft_forwarder
            .as_ref()
            .expect("matcher consumers require Raft forwarder")
            .clone();
        let consumer_store_metrics = metrics.clone();
        let route_control = kafka.route_control.clone();
        std::thread::Builder::new()
            .name(format!("order-kafka-match-{worker}"))
            .spawn(move || {
                run_kafka_stage(
                    consumer_kafka,
                    category_size,
                    group_id,
                    "match",
                    worker,
                    move |batch| {
                        let started = std::time::Instant::now();
                        let result = forward_kafka_batch(&worker_forwarder, batch, &route_control);
                        consumer_store_metrics.record_raft(started.elapsed());
                        result
                    },
                )
            })
            .expect("spawn Kafka matching consumer");
    }
    for worker in 0..kafka.execution_consumers {
        let execution_forwarder = execution_db_forwarder
            .as_ref()
            .expect("execution DB consumers require shard forwarder")
            .clone();
        let execution_metrics = metrics.clone();
        let execution_kafka = kafka.clone();
        let execution_dlq = dlq.clone();
        std::thread::Builder::new()
            .name(format!("execution-kafka-mysql-{worker}"))
            .spawn(move || {
                run_execution_mysql_consumer(
                    execution_forwarder,
                    execution_metrics,
                    execution_kafka,
                    worker,
                    execution_dlq,
                )
            })
            .expect("spawn execution MySQL projection consumer");
    }
    let listener = TcpListener::bind("0.0.0.0:9200").expect("bind order API");
    // Non-blocking accept so a SIGTERM/SIGINT (e.g. `docker stop`) stops the API
    // taking new connections and lets the process exit cleanly instead of being
    // SIGKILLed. In-flight requests already commit Kafka offsets per batch, so
    // the durable pipeline state is crash-safe regardless.
    listener
        .set_nonblocking(true)
        .expect("set order API listener non-blocking");
    let shared_metrics = metrics;
    let shared_kafka = Arc::new(kafka);
    loop {
        if SHUTDOWN.load(AtomicOrdering::SeqCst) {
            log_info!(
                "order-api",
                "shutdown signal received, no longer accepting connections"
            );
            break;
        }
        match listener.accept() {
            Ok((stream, _peer)) => {
                // Accepted sockets must be blocking for the handler's blocking reads.
                stream.set_nonblocking(false).ok();
                let metrics = shared_metrics.clone();
                let kafka = shared_kafka.clone();
                let token = token.clone();
                std::thread::spawn(move || {
                    handle(
                        stream,
                        category_size,
                        metrics,
                        kafka,
                        &token,
                        ingress_enabled,
                    )
                });
            }
            Err(ref error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mysql_test_store() -> Option<OrderStore> {
        let url = std::env::var("TC_TEST_MYSQL_URL").ok()?;
        let routing = sharding::RouteConfig::with_virtual(1, 8, 1_000, 10_000, 1);
        let pool = Pool::new(url.as_str()).expect("open isolated MySQL 8 test server");
        pool.get_conn()
            .expect("connect isolated MySQL 8 test server");
        let store = OrderStore {
            shards: Arc::new(vec![Some(pool)]),
            owned_dbs: Arc::new([0u32].into_iter().collect()),
            routing,
            metrics: Arc::new(OrderPipelineMetrics::default()),
        };
        bootstrap(&store).expect("bootstrap isolated MySQL test schema");
        Some(store)
    }

    fn kafka_new(order_id: u64, user: u64, instrument: u32, qty: u64, offset: i64) -> KafkaRecord {
        let order = Order::limit(OrderId(order_id), Side::Buy, 1_000, qty)
            .on(InstrumentId(instrument))
            .by(user);
        let mut frame = [0u8; wire::MSG_LEN];
        wire::encode_new(&order, &mut frame);
        KafkaRecord {
            topic: "mysql-it".into(),
            partition: 0,
            offset,
            category_id: sharding::asset_category(InstrumentId(instrument), 1_000),
            user,
            frame,
        }
    }

    fn trade_event(
        target_order_id: u64,
        other_order_id: u64,
        instrument: u32,
        qty: u64,
        raft_index: u64,
        ordinal: u32,
        offset: i64,
    ) -> ExecutionKafkaRecord {
        ExecutionKafkaRecord {
            topic: "execution-it".into(),
            partition: 0,
            offset,
            event: wire::ExecutionEvent {
                raft_group: 7,
                raft_index,
                ordinal,
                target_order_id,
                report: wire::DecodedReport {
                    type_code: wire::RT_TRADE,
                    maker_fee: 0,
                    taker_fee: 0,
                    instrument: InstrumentId(instrument),
                    order_id: OrderId(target_order_id),
                    aux_id: other_order_id,
                    price: 1_000,
                    qty,
                    side: Side::Buy,
                },
            },
        }
    }

    fn persist_events(
        store: &OrderStore,
        records: Vec<ExecutionKafkaRecord>,
    ) -> Result<(), String> {
        let mut tables = ExecutionShardTables::new();
        for record in records {
            let target = record.event.target_order_id;
            let route = store.routing.route_order_id(target);
            assert_eq!(route.db, 0);
            tables
                .entry(route.table)
                .or_default()
                .push((record, target));
        }
        persist_execution_shard(store, 0, tables, 500)
    }

    fn batch_order(user: u64, instrument: u32) -> Vec<u8> {
        batch_order_with_id(user, instrument, 99)
    }

    fn batch_order_with_id(user: u64, instrument: u32, order_id: u64) -> Vec<u8> {
        let order = Order::limit(OrderId(order_id), Side::Buy, 1_000, 2)
            .on(InstrumentId(instrument))
            .by(user);
        let mut frame = [0u8; wire::MSG_LEN];
        wire::encode_new(&order, &mut frame);
        let mut body = user.to_le_bytes().to_vec();
        body.extend_from_slice(&frame);
        body
    }

    #[test]
    fn execution_batch_target_scales_with_physical_db_count() {
        assert_eq!(execution_consumer_batch_size(100, 10), 1_000);
        assert_eq!(execution_consumer_batch_size(500, 10), 5_000);
        assert_eq!(execution_consumer_batch_size(0, 0), 1);
    }

    /// Run explicitly against a disposable MySQL 8 instance:
    /// `TC_TEST_MYSQL_URL=mysql://root:test@127.0.0.1:33306/mysql cargo test
    ///  --bin order-api mysql8_projection_is_race_safe_and_idempotent -- --ignored`
    #[test]
    #[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
    fn mysql8_projection_is_race_safe_and_idempotent() {
        let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
        let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
        let taker = seed | 1;
        let maker = seed | 2;
        let raced = seed | 3;
        let user = 55_001;
        let instrument = 42_001;

        // Result-before-order: staging remains durable, then the command-side
        // transaction drains it as soon as the order row becomes visible.
        persist_events(
            &store,
            vec![trade_event(taker, maker, instrument, 3, seed, 0, 1)],
        )
        .unwrap();
        persist_mysql_shard(&store, 0, &[kafka_new(taker, user, instrument, 10, 1)]).unwrap();

        // One Trade has two independently keyed target events. The idempotency
        // key includes target_order_id, so maker and taker cannot collapse.
        persist_mysql_shard(&store, 0, &[kafka_new(maker, user + 1, instrument, 10, 2)]).unwrap();
        let dual = vec![
            trade_event(taker, maker, instrument, 2, seed + 1, 0, 2),
            trade_event(maker, taker, instrument, 2, seed + 1, 0, 3),
        ];
        persist_events(&store, dual.clone()).unwrap();
        persist_events(&store, dual).unwrap(); // Kafka replay must not double-fill.

        // Deliberately race the command and execution transactions. Whichever
        // commits first, either the command drain, execution UPDATE, or sweeper
        // must converge to the same projection.
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let command_store = store.clone();
        let command_barrier = barrier.clone();
        let command = std::thread::spawn(move || {
            command_barrier.wait();
            persist_mysql_shard(
                &command_store,
                0,
                &[kafka_new(raced, user + 2, instrument, 5, 4)],
            )
        });
        let execution_store = store.clone();
        let execution = std::thread::spawn(move || {
            barrier.wait();
            persist_events(
                &execution_store,
                vec![trade_event(raced, maker, instrument, 5, seed + 2, 0, 4)],
            )
        });
        command.join().unwrap().unwrap();
        execution.join().unwrap().unwrap();
        sweep_pending_execution_shard(&store, 0).unwrap();

        let mut conn = store.shard(0).get_conn().unwrap();
        for (order_id, expected_fill, expected_status) in [
            (taker, 5u64, "PARTIAL"),
            (maker, 2u64, "PARTIAL"),
            (raced, 5u64, "FILLED"),
        ] {
            let route = store.routing.route_order_id(order_id);
            let row: Option<(u64, String)> = conn
                .exec_first(
                    format!(
                        "SELECT filled_qty,status FROM order_db_0.{} WHERE order_id=:id",
                        route.table_name()
                    ),
                    params! {"id" => order_id},
                )
                .unwrap();
            assert_eq!(row, Some((expected_fill, expected_status.into())));
        }
        let targets: u64 = conn
            .exec_first(
                "SELECT COUNT(*) FROM order_db_0.processed_execution_events WHERE raft_group=7 AND raft_index=:idx AND report_ordinal=0",
                params! {"idx" => seed + 1},
            )
            .unwrap()
            .unwrap();
        assert_eq!(targets, 2, "maker and taker use distinct idempotency keys");
        let pending: u64 = conn
            .query_first("SELECT COUNT(*) FROM order_db_0.pending_execution_events")
            .unwrap()
            .unwrap();
        assert_eq!(pending, 0);
    }

    #[test]
    fn decodes_batch_and_routes_asset_category() {
        let mut body = batch_order(100_000, 1);
        body.extend_from_slice(&batch_order(100_001, 2_501));

        let records = decode_batch(&body, 1_000).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].category_id, 0);
        assert_eq!(records[1].category_id, 2);
        assert_eq!(records[1].user, 100_001);
    }

    #[test]
    fn same_asset_routes_to_same_topic_and_partition_for_different_orders() {
        let mut body = batch_order_with_id(100_000, 42_001, 7);
        body.extend_from_slice(&batch_order_with_id(200_000, 42_001, 9_999_999));
        let records = decode_batch(&body, 1_000).unwrap();
        let router = QueueRouter::new(
            vec!["orders-0".into(), "orders-1".into(), "orders-2".into()],
            8,
            1,
        );

        assert_eq!(records[0].category_id, records[1].category_id);
        assert_eq!(
            router.route(records[0].category_id),
            router.route(records[1].category_id)
        );
    }

    #[test]
    fn rejects_partial_batch_record() {
        assert!(decode_batch(&[0; BATCH_RECORD_LEN - 1], 1_000).is_err());
    }

    #[test]
    fn rejects_user_that_differs_from_order() {
        let mut body = batch_order(100_000, 1);
        body[..8].copy_from_slice(&100_001u64.to_le_bytes());

        assert!(decode_batch(&body, 1_000)
            .unwrap_err()
            .contains("does not match"));
    }

    #[test]
    fn ingress_backpressure_uses_shared_group_lag_and_local_inflight() {
        let metrics = OrderPipelineMetrics::default();
        metrics.try_reserve(3, 5).unwrap();
        assert_eq!(metrics.backlog(), 3);
        metrics.finish_reservation(3, 3);
        assert_eq!(metrics.backlog(), 0);

        metrics.set_observed_lag("mysql", 2);
        metrics.set_observed_lag("match", 3);
        assert_eq!(metrics.backlog(), 3);
        metrics.try_reserve(2, 5).unwrap();
        assert!(metrics.try_reserve(1, 5).is_err());
        assert_eq!(
            ingress_error_status("backpressure: full"),
            "429 Too Many Requests"
        );
    }

    fn test_backpressure(
        soft: u64,
        hard: u64,
        emergency: u64,
        partitions: usize,
        groups: usize,
    ) -> Backpressure {
        Backpressure {
            soft,
            hard,
            emergency,
            soft_delay: Duration::ZERO,
            topic_count: 1,
            partitions_per_topic: partitions as u32,
            raft_group_count: groups,
            raft_group_pins: Arc::new(HashMap::new()),
            mysql_lag: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            match_lag: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            group_unhealthy: (0..groups).map(|_| AtomicBool::new(false)).collect(),
            soft_events: AtomicU64::new(0),
            hard_events: AtomicU64::new(0),
            emergency_events: AtomicU64::new(0),
        }
    }

    #[test]
    fn backpressure_tiers_climb_by_threshold_and_recover() {
        let bp = test_backpressure(100, 250, 750, 1, 1);
        // Climb through every tier as lag crosses each threshold.
        assert_eq!(bp.classify(0, false), BackpressureTier::Normal);
        assert_eq!(bp.classify(99, false), BackpressureTier::Normal);
        assert_eq!(bp.classify(100, false), BackpressureTier::Soft);
        assert_eq!(bp.classify(249, false), BackpressureTier::Soft);
        assert_eq!(bp.classify(250, false), BackpressureTier::Hard);
        assert_eq!(bp.classify(749, false), BackpressureTier::Hard);
        assert_eq!(bp.classify(750, false), BackpressureTier::Emergency);
        // Lost quorum forces emergency regardless of lag.
        assert_eq!(bp.classify(0, true), BackpressureTier::Emergency);
        // Recovery falls back down as lag drains.
        assert_eq!(bp.classify(300, false), BackpressureTier::Hard);
        assert_eq!(bp.classify(120, false), BackpressureTier::Soft);
        assert_eq!(bp.classify(10, false), BackpressureTier::Normal);
    }

    #[test]
    fn backpressure_is_isolated_per_category_and_takes_group_max() {
        // 4 partitions; category N maps to partition N (topic_count 1).
        let bp = test_backpressure(100, 250, 750, 4, 2);
        // A hot partition 0 only throttles categories on it.
        bp.set_partition_lags("mysql", &[300, 0, 0, 0]);
        assert_eq!(bp.tier_for_category(0), BackpressureTier::Hard);
        assert_eq!(bp.tier_for_category(1), BackpressureTier::Normal);
        assert_eq!(bp.tier_for_category(4), BackpressureTier::Hard); // 4 -> partition 0
                                                                     // Match-group lag on partition 1 dominates via max().
        bp.set_partition_lags("match", &[0, 800, 0, 0]);
        assert_eq!(bp.tier_for_category(1), BackpressureTier::Emergency);
        // Drain both groups -> everything recovers to normal.
        bp.set_partition_lags("mysql", &[0, 0, 0, 0]);
        bp.set_partition_lags("match", &[0, 0, 0, 0]);
        assert_eq!(bp.tier_for_category(0), BackpressureTier::Normal);
        assert_eq!(bp.tier_for_category(1), BackpressureTier::Normal);
    }

    #[test]
    fn backpressure_quorum_loss_is_per_group_and_recoverable() {
        let bp = test_backpressure(100, 250, 750, 2, 2);
        // Category 0 -> group 0, category 1 -> group 1.
        bp.set_group_health(0, false);
        assert_eq!(bp.tier_for_category(0), BackpressureTier::Emergency);
        assert_eq!(bp.tier_for_category(1), BackpressureTier::Normal);
        // Group recovers on the next healthy forward.
        bp.set_group_health(0, true);
        assert_eq!(bp.tier_for_category(0), BackpressureTier::Normal);
    }

    #[test]
    fn backpressure_admit_maps_tiers_to_ingress_status() {
        let bp = test_backpressure(100, 250, 750, 1, 1);
        // Soft accepts (slowdown only).
        bp.set_partition_lags("mysql", &[150]);
        assert!(bp.admit(0).is_ok());
        // Hard -> 429.
        bp.set_partition_lags("mysql", &[300]);
        let hard = bp.admit(0).unwrap_err();
        assert_eq!(ingress_error_status(&hard), "429 Too Many Requests");
        // Emergency -> 503.
        bp.set_partition_lags("mysql", &[900]);
        let emergency = bp.admit(0).unwrap_err();
        assert_eq!(ingress_error_status(&emergency), "503 Service Unavailable");
    }

    #[test]
    fn shard_url_count_must_match_db_count() {
        assert!(validate_shard_urls(&["db0".to_string()], 1).is_ok());
        let three = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert!(validate_shard_urls(&three, 3).is_ok());
        let err = validate_shard_urls(&three, 10).unwrap_err();
        assert!(err.contains("10"));
        assert!(validate_shard_urls(&[], 1).is_err());
    }

    #[test]
    fn ingress_and_match_only_roles_do_not_require_mysql() {
        assert!(!role_needs_mysql(0, 0));
        assert!(role_needs_mysql(1, 0));
        assert!(role_needs_mysql(0, 1));
    }

    #[test]
    fn worker_completion_is_not_clamped_by_process_local_publishes() {
        let metrics = OrderPipelineMetrics::default();
        metrics.complete("mysql", 500);
        metrics.complete("match", 700);
        assert_eq!(
            metrics
                .mysql_completed_commands
                .load(AtomicOrdering::Relaxed),
            500
        );
        assert_eq!(
            metrics
                .match_completed_commands
                .load(AtomicOrdering::Relaxed),
            700
        );
    }

    fn dlq_temp_path(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!("tc-dlq-{}-{tag}-{n}.wal", std::process::id()))
    }

    fn read_dlq(path: &Path) -> Vec<(i32, i64, String, Vec<u8>)> {
        let mut file = File::open(path).unwrap();
        let mut header = [0u8; 8];
        file.read_exact(&mut header).unwrap();
        assert_eq!(header, DLQ_HEADER);
        let mut out = Vec::new();
        loop {
            let mut len_bytes = [0u8; 4];
            if file.read_exact(&mut len_bytes).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut body = vec![0u8; len];
            file.read_exact(&mut body).unwrap();
            let mut checksum = [0u8; 8];
            file.read_exact(&mut checksum).unwrap();
            assert_eq!(journal::fnv1a(&body), u64::from_le_bytes(checksum));
            let partition = i32::from_le_bytes(body[8..12].try_into().unwrap());
            let offset = i64::from_le_bytes(body[12..20].try_into().unwrap());
            let reason_len = u32::from_le_bytes(body[20..24].try_into().unwrap()) as usize;
            let reason = String::from_utf8(body[24..24 + reason_len].to_vec()).unwrap();
            let mut cursor = 24 + reason_len;
            let payload_len =
                u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let payload = body[cursor..cursor + payload_len].to_vec();
            out.push((partition, offset, reason, payload));
        }
        out
    }

    #[test]
    fn persist_retry_dead_letters_after_exhausting_retries() {
        let path = dlq_temp_path("poison");
        let _ = std::fs::remove_file(&path);
        let dlq = Mutex::new(DlqWriter::open(&path).unwrap());
        let metrics = OrderPipelineMetrics::default();

        let attempts = AtomicU64::new(0);
        let outcome = persist_with_retry(
            || {
                attempts.fetch_add(1, AtomicOrdering::Relaxed);
                Err("poison message".to_string())
            },
            &dlq,
            &metrics,
            3,
            Duration::ZERO,
            7,
            42,
            b"raw-envelope",
        );

        assert_eq!(outcome, PersistOutcome::DeadLettered);
        assert_eq!(
            attempts.load(AtomicOrdering::Relaxed),
            3,
            "tries up to the retry cap"
        );
        assert_eq!(metrics.dlq_total.load(AtomicOrdering::Relaxed), 1);

        let records = read_dlq(&path);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, 7);
        assert_eq!(records[0].1, 42);
        assert!(records[0].2.contains("poison"));
        assert_eq!(records[0].3, b"raw-envelope");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn persist_retry_commits_and_skips_dlq_on_eventual_success() {
        let path = dlq_temp_path("recover");
        let _ = std::fs::remove_file(&path);
        let dlq = Mutex::new(DlqWriter::open(&path).unwrap());
        let metrics = OrderPipelineMetrics::default();

        let attempts = AtomicU64::new(0);
        let outcome = persist_with_retry(
            || {
                // Fail twice, then succeed on the third attempt.
                if attempts.fetch_add(1, AtomicOrdering::Relaxed) < 2 {
                    Err("transient".to_string())
                } else {
                    Ok(())
                }
            },
            &dlq,
            &metrics,
            5,
            Duration::ZERO,
            1,
            2,
            b"x",
        );

        assert_eq!(outcome, PersistOutcome::Committed);
        assert_eq!(metrics.dlq_total.load(AtomicOrdering::Relaxed), 0);
        assert!(read_dlq(&path).is_empty(), "no dead-letter on success");
        std::fs::remove_file(&path).ok();
    }
}
