//! Persistent order system: Kafka is the ordered source of truth and fans out
//! through independent consumer groups to the MySQL query projection and the
//! Raft-backed matcher.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{params, Params, Pool, Transaction, TxOpts, Value};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use trade_core::order::Order;
use trade_core::order_queue::{encode_envelope, QueueEnvelope, QueueRouter};
use trade_core::sharding::{self, DB_COUNT, DEFAULT_ASSET_CATEGORY_SIZE, TABLES_PER_DB};
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire;

const BATCH_RECORD_LEN: usize = 8 + wire::MSG_LEN;

#[derive(Clone)]
struct MatcherTarget {
    order_addr: String,
    metrics_addr: Option<String>,
}

#[derive(Clone)]
struct OrderStore {
    shards: Arc<Vec<Pool>>,
    category_size: u32,
    metrics: Arc<OrderPipelineMetrics>,
}

#[derive(Default)]
struct OrderPipelineMetrics {
    mysql_commit_ns_total: AtomicU64,
    mysql_commit_ns_max: AtomicU64,
    mysql_commit_samples: AtomicU64,
    raft_forward_ns_total: AtomicU64,
    raft_forward_ns_max: AtomicU64,
    raft_forward_samples: AtomicU64,
    published_commands: AtomicU64,
    mysql_completed_commands: AtomicU64,
    match_completed_commands: AtomicU64,
    backpressure_rejections: AtomicU64,
    observed_mysql_lag: AtomicU64,
    observed_match_lag: AtomicU64,
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
    }

    fn record_raft(&self, elapsed: Duration) {
        Self::record(
            &self.raft_forward_ns_total,
            &self.raft_forward_ns_max,
            &self.raft_forward_samples,
            elapsed,
        );
    }

    fn try_reserve(&self, commands: u64, max_backlog: u64) -> Result<(), u64> {
        loop {
            let published = self.published_commands.load(AtomicOrdering::Acquire);
            let completed = self
                .mysql_completed_commands
                .load(AtomicOrdering::Acquire)
                .min(self.match_completed_commands.load(AtomicOrdering::Acquire));
            let backlog = published.saturating_sub(completed).max(
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
                .published_commands
                .compare_exchange_weak(
                    published,
                    published.saturating_add(commands),
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
        self.published_commands
            .fetch_sub(commands, AtomicOrdering::AcqRel);
    }

    fn complete(&self, stage: &str, commands: u64) {
        let counter = match stage {
            "mysql" => &self.mysql_completed_commands,
            "match" => &self.match_completed_commands,
            _ => return,
        };
        let published = self.published_commands.load(AtomicOrdering::Acquire);
        let _ = counter.fetch_update(
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
            |completed| Some(completed.saturating_add(commands).min(published)),
        );
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
        let completed = self
            .mysql_completed_commands
            .load(AtomicOrdering::Acquire)
            .min(self.match_completed_commands.load(AtomicOrdering::Acquire));
        let local = self
            .published_commands
            .load(AtomicOrdering::Acquire)
            .saturating_sub(completed);
        local.max(
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
            "# TYPE tc_order_ingress_backlog gauge\ntc_order_ingress_backlog {}\n\
# TYPE tc_order_published_commands counter\ntc_order_published_commands {}\n\
# TYPE tc_order_mysql_completed_commands counter\ntc_order_mysql_completed_commands {}\n\
# TYPE tc_order_match_completed_commands counter\ntc_order_match_completed_commands {}\n\
# TYPE tc_order_backpressure_rejections counter\ntc_order_backpressure_rejections {}\n",
            self.backlog(),
            self.published_commands.load(AtomicOrdering::Relaxed),
            self.mysql_completed_commands.load(AtomicOrdering::Relaxed),
            self.match_completed_commands.load(AtomicOrdering::Relaxed),
            self.backpressure_rejections.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_order_mysql_consumer_lag gauge\ntc_order_mysql_consumer_lag {}\n\
# TYPE tc_order_match_consumer_lag gauge\ntc_order_match_consumer_lag {}\n",
            self.observed_mysql_lag.load(AtomicOrdering::Relaxed),
            self.observed_match_lag.load(AtomicOrdering::Relaxed),
        )
    }
}

impl OrderStore {
    fn shard(&self, db: u32) -> &Pool {
        &self.shards[db as usize]
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

fn ingress_error_status(error: &str) -> &'static str {
    if error.starts_with("backpressure:") {
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
    for db in 0..DB_COUNT {
        let mut conn = store.shard(db as u32).get_conn()?;
        let db_name = format!("order_db_{db}");
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db_name}"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_commands (category_id INT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(category_id,kafka_offset), KEY idx_partition_offset (kafka_partition,kafka_offset)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_executions (kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(kafka_partition,kafka_offset), KEY idx_instrument_offset (instrument,kafka_offset)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_execution_events (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, order_id BIGINT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal), KEY idx_instrument_index (instrument,raft_index), KEY idx_order_event (order_id,raft_index)) ENGINE=InnoDB"))?;
        for table in 0..TABLES_PER_DB {
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
    matcher_consumers: usize,
    db_group: String,
    matcher_group: String,
    execution_topic: String,
    execution_group: String,
    execution_consumers: usize,
    raft_group_pins: Arc<HashMap<u32, usize>>,
    batch_size: usize,
    linger: Duration,
    max_pipeline_backlog: u64,
    metrics: Arc<OrderPipelineMetrics>,
}

impl KafkaIngress {
    fn from_env(metrics: Arc<OrderPipelineMetrics>) -> Result<Option<Self>, String> {
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
        let raft_group_pins = std::env::var("TC_RAFT_CATEGORY_PINS")
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
            .unwrap_or_default();
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("max.in.flight.requests.per.connection", "5")
            .set("delivery.timeout.ms", "10000")
            .set("linger.ms", "1")
            .set("batch.num.messages", "10000")
            .create::<FutureProducer>()
            .map_err(|error| error.to_string())?;
        Ok(Some(Self {
            producer,
            brokers,
            router: QueueRouter::new(topics, partitions, version),
            partitions_per_topic: partitions,
            db_consumers: env_number(
                "TC_ORDER_KAFKA_DB_CONSUMERS",
                env_number("TC_ORDER_KAFKA_CONSUMERS", partition_workers),
            )
            .max(1),
            matcher_consumers: env_number(
                "TC_ORDER_KAFKA_MATCH_CONSUMERS",
                env_number("TC_ORDER_KAFKA_CONSUMERS", partition_workers),
            )
            .max(1),
            db_group: std::env::var("TC_ORDER_KAFKA_DB_GROUP")
                .unwrap_or_else(|_| "trade-order-persist-v1".into()),
            matcher_group: std::env::var("TC_ORDER_KAFKA_MATCH_GROUP")
                .unwrap_or_else(|_| "trade-order-match-v1".into()),
            execution_topic: std::env::var("TC_EXECUTION_KAFKA_TOPIC")
                .unwrap_or_else(|_| "trade-executions-v1".into()),
            execution_group: std::env::var("TC_EXECUTION_KAFKA_MYSQL_GROUP")
                .unwrap_or_else(|_| "trade-order-execution-mysql-v1".into()),
            execution_consumers: env_number("TC_EXECUTION_KAFKA_MYSQL_CONSUMERS", 4usize).max(1),
            raft_group_pins: Arc::new(raft_group_pins),
            batch_size: env_number("TC_ORDER_BATCH_SIZE", 500usize).max(1),
            linger: Duration::from_millis(env_number("TC_ORDER_BATCH_LINGER_MS", 2u64)),
            max_pipeline_backlog: env_number("TC_ORDER_MAX_PIPELINE_BACKLOG", 50_000u64).max(1),
            metrics,
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

    fn publish(
        &self,
        category_id: u32,
        user: u64,
        frame: &[u8; wire::MSG_LEN],
    ) -> Result<CategorySequence, String> {
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
        Ok(CategorySequence {
            category_id,
            category_seq: (offset as u64).saturating_add(1),
        })
    }

    fn publish_batch(&self, records: &[BatchRecord]) -> Result<(), String> {
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
        if failed > 0 {
            self.metrics.rollback_reservation(failed);
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

/// Persist one Kafka micro-batch with at most one MySQL commit per physical
/// database. Records remain ordered within each category because Kafka assigns
/// a partition to only one active consumer in the group.
fn persist_kafka_batch(store: &OrderStore, records: &[KafkaRecord]) -> Result<(), String> {
    let mut by_db: HashMap<u32, Vec<&KafkaRecord>> = HashMap::new();
    for record in records {
        let route = sharding::route_category(record.category_id);
        by_db.entry(route.db).or_default().push(record);
    }

    std::thread::scope(|scope| {
        let mut jobs = Vec::with_capacity(by_db.len());
        for (shard_db, records) in by_db {
            jobs.push(scope.spawn(move || persist_mysql_shard(store, shard_db, records)));
        }
        for job in jobs {
            job.join()
                .map_err(|_| "MySQL shard projection worker panicked".to_string())??;
        }
        Ok(())
    })
}

fn persist_mysql_shard(
    store: &OrderStore,
    shard_db: u32,
    records: Vec<&KafkaRecord>,
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

    for record in records {
        let route = sharding::route_category(record.category_id);
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| "invalid Kafka command frame".to_string())?;
        let id = command_id(&command);
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

fn commit_kafka_batch(consumer: &BaseConsumer, records: &[KafkaRecord]) -> Result<(), String> {
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
        .commit(&list, CommitMode::Sync)
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

fn forward_kafka_batch(
    matchers: &mut [MatcherConnection],
    records: &[KafkaRecord],
    pins: &HashMap<u32, usize>,
) -> Result<(), String> {
    if matchers.is_empty() {
        return Err("no Raft groups configured".into());
    }
    let mut grouped = (0..matchers.len())
        .map(|_| BTreeMap::<u32, Vec<&KafkaRecord>>::new())
        .collect::<Vec<_>>();
    for record in records {
        let group =
            sharding::raft_group_for_category_pinned(record.category_id, matchers.len(), pins);
        grouped[group]
            .entry(record.category_id)
            .or_default()
            .push(record);
    }

    // Independent groups commit concurrently. Records within one group remain
    // in Kafka poll order; a retry may repeat a committed prefix and is made
    // exact by the command-id deduplication in that Raft group.
    std::thread::scope(|scope| {
        let mut jobs = Vec::new();
        for (group, (matcher, categories)) in matchers.iter_mut().zip(grouped).enumerate() {
            if categories.is_empty() {
                continue;
            }
            jobs.push(scope.spawn(move || -> Result<(), String> {
                for records in categories.into_values() {
                    for batch in records.chunks(wire::RAFT_BATCH_MAX_COMMANDS) {
                        matcher.send_batch(batch).map_err(|error| {
                            format!("Raft group {group} batch commit failed: {error}")
                        })?;
                    }
                }
                Ok(())
            }));
        }
        for job in jobs {
            job.join()
                .map_err(|_| "Raft group forwarding worker panicked".to_string())??;
        }
        Ok(())
    })
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
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        .set("group.id", &group_id)
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
    eprintln!(
        "[order-kafka-{stage}-{worker}] group={group_id} subscribed to {} queue groups",
        topics.len(),
    );
    let mut consecutive_failures = 0u32;
    let mut last_failure_log = std::time::Instant::now() - Duration::from_secs(30);

    loop {
        let mut batch = Vec::with_capacity(kafka.batch_size);
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        match first {
            Ok(message) => match decode_kafka_record(&message, &kafka.router, category_size) {
                Ok(record) => batch.push(record),
                Err(error) => {
                    eprintln!("[order-kafka-{stage}-{worker}] rejected message: {error}")
                }
            },
            Err(error) => {
                if last_failure_log.elapsed() >= Duration::from_secs(30) {
                    eprintln!("[order-kafka-{stage}-{worker}] poll failed: {error}");
                    last_failure_log = std::time::Instant::now();
                }
                continue;
            }
        }
        let deadline = std::time::Instant::now() + kafka.linger;
        while batch.len() < kafka.batch_size && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(message)) => {
                    match decode_kafka_record(&message, &kafka.router, category_size) {
                        Ok(record) => batch.push(record),
                        Err(error) => {
                            eprintln!("[order-kafka-{stage}-{worker}] rejected message: {error}")
                        }
                    }
                }
                Some(Err(error)) => {
                    if last_failure_log.elapsed() >= Duration::from_secs(30) {
                        eprintln!("[order-kafka-{stage}-{worker}] poll failed: {error}");
                        last_failure_log = std::time::Instant::now();
                    }
                }
                None => break,
            }
        }
        match process(&batch).and_then(|()| commit_kafka_batch(&consumer, &batch)) {
            Ok(()) => {
                kafka.metrics.complete(stage, batch.len() as u64);
                consecutive_failures = 0;
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures == 1
                    || last_failure_log.elapsed() >= Duration::from_secs(30)
                {
                    eprintln!(
                        "[order-kafka-{stage}-{worker}] batch retained for retry (failure {consecutive_failures}): {error}"
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

fn read_consumer_group_lag(consumer: &BaseConsumer, kafka: &KafkaIngress) -> Result<u64, String> {
    let mut requested = TopicPartitionList::new();
    for topic in kafka.router.topics() {
        for partition in 0..kafka.partitions_per_topic as i32 {
            requested.add_partition(topic, partition);
        }
    }
    let committed = consumer
        .committed_offsets(requested, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    let mut lag = 0u64;
    for element in committed.elements() {
        let offset = match element.offset() {
            Offset::Offset(offset) => offset,
            _ => 0,
        };
        let (_, high) = consumer
            .fetch_watermarks(element.topic(), element.partition(), Duration::from_secs(2))
            .map_err(|error| error.to_string())?;
        lag = lag.saturating_add(high.saturating_sub(offset) as u64);
    }
    Ok(lag)
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
            Ok(lag) => kafka.metrics.set_observed_lag(stage, lag),
            Err(error) if last_error_log.elapsed() >= Duration::from_secs(30) => {
                eprintln!("[order-kafka-{stage}-lag] failed to read {group_id}: {error}");
                last_error_log = std::time::Instant::now();
            }
            Err(_) => {}
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn run_execution_mysql_consumer(store: OrderStore, kafka: KafkaIngress, worker: usize) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        .set("group.id", &kafka.execution_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()
        .expect("create execution MySQL consumer");
    consumer
        .subscribe(&[&kafka.execution_topic])
        .expect("subscribe execution topic");
    eprintln!(
        "[execution-mysql-{worker}] group={} topic={}",
        kafka.execution_group, kafka.execution_topic
    );
    let mut last_failure_log = std::time::Instant::now() - Duration::from_secs(30);
    loop {
        let Some(message) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                if last_failure_log.elapsed() >= Duration::from_secs(30) {
                    eprintln!("[execution-mysql-{worker}] poll failed: {error}");
                    last_failure_log = std::time::Instant::now();
                }
                continue;
            }
        };
        let Some(event) = message.payload().and_then(wire::decode_execution_event) else {
            eprintln!("[execution-mysql-{worker}] rejected invalid execution report");
            continue;
        };
        match persist_execution_report(&store, message.partition(), message.offset(), event) {
            Ok(()) => {
                if let Err(error) = consumer.commit_message(&message, CommitMode::Sync) {
                    eprintln!("[execution-mysql-{worker}] offset commit failed: {error}");
                }
            }
            Err(error) => {
                eprintln!("[execution-mysql-{worker}] projection retry: {error}");
                let _ = consumer.seek(
                    message.topic(),
                    message.partition(),
                    Offset::Offset(message.offset()),
                    Duration::from_secs(1),
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn persist_execution_report(
    store: &OrderStore,
    partition: i32,
    offset: i64,
    event: wire::ExecutionEvent,
) -> Result<(), String> {
    let report = event.report;
    let category = sharding::asset_category(report.instrument, store.category_size);
    let route = sharding::route_category(category);
    let db = route.db_name();
    let table = route.table_name();
    let mut conn = store
        .shard(route.db)
        .get_conn()
        .map_err(|error| error.to_string())?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .map_err(|error| error.to_string())?;
    if event.raft_index == 0 {
        tx.exec_drop(
            format!("INSERT IGNORE INTO {db}.processed_executions (kafka_partition,kafka_offset,instrument) VALUES (:partition,:offset,:instrument)"),
            params! {
                "partition" => partition,
                "offset" => offset as u64,
                "instrument" => report.instrument.0,
            },
        )
        .map_err(|error| error.to_string())?;
    } else {
        tx.exec_drop(
            format!("INSERT IGNORE INTO {db}.processed_execution_events (raft_group,raft_index,report_ordinal,instrument,order_id,report_type,kafka_partition,kafka_offset) VALUES (:raft_group,:raft_index,:ordinal,:instrument,:order_id,:report_type,:partition,:offset)"),
            params! {
                "raft_group" => event.raft_group,
                "raft_index" => event.raft_index,
                "ordinal" => event.ordinal,
                "instrument" => report.instrument.0,
                "order_id" => report.order_id.0,
                "report_type" => report.type_code,
                "partition" => partition,
                "offset" => offset as u64,
            },
        )
        .map_err(|error| error.to_string())?;
    }
    if tx.affected_rows() == 0 {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(());
    }
    match report.type_code {
        wire::RT_TRADE => {
            tx.exec_drop(
                format!("UPDATE {db}.{table} SET status=IF(LEAST(qty,filled_qty+:fill)>=qty,'FILLED','PARTIAL'), filled_qty=LEAST(qty,filled_qty+:fill) WHERE order_id IN (:taker,:maker)"),
                params! {"fill" => report.qty, "taker" => report.order_id.0, "maker" => report.aux_id},
            )
            .map_err(|error| error.to_string())?;
        }
        wire::RT_FILLED => {
            tx.exec_drop(
                format!(
                    "UPDATE {db}.{table} SET status='FILLED',filled_qty=qty WHERE order_id=:id"
                ),
                params! {"id" => report.order_id.0},
            )
            .map_err(|error| error.to_string())?;
        }
        wire::RT_PARTIAL => update_execution_status(
            &mut tx,
            &db,
            &table,
            report.order_id.0,
            "PARTIAL",
            Some(report.qty),
        )?,
        wire::RT_RESTING | wire::RT_ACCEPTED => {
            update_execution_status(&mut tx, &db, &table, report.order_id.0, "OPEN", None)?
        }
        wire::RT_CANCELLED => {
            update_execution_status(&mut tx, &db, &table, report.order_id.0, "CANCELLED", None)?
        }
        wire::RT_REJECTED => {
            update_execution_status(&mut tx, &db, &table, report.order_id.0, "REJECTED", None)?
        }
        wire::RT_MODIFIED => {
            update_execution_status(&mut tx, &db, &table, report.order_id.0, "OPEN", None)?
        }
        _ => {}
    }
    tx.commit().map_err(|error| error.to_string())
}

fn update_execution_status(
    tx: &mut Transaction<'_>,
    db: &str,
    table: &str,
    order_id: u64,
    status: &str,
    filled: Option<u64>,
) -> Result<(), String> {
    match filled {
        Some(filled) => tx.exec_drop(
            format!("UPDATE {db}.{table} SET status=:status,filled_qty=GREATEST(filled_qty,:filled) WHERE order_id=:id"),
            params! {"status" => status, "filled" => filled, "id" => order_id},
        ),
        None => tx.exec_drop(
            format!("UPDATE {db}.{table} SET status=:status WHERE order_id=:id"),
            params! {"status" => status, "id" => order_id},
        ),
    }
    .map_err(|error| error.to_string())
}

fn parse_shard_urls() -> Vec<String> {
    if let Ok(value) = std::env::var("TC_ORDER_MYSQL_SHARD_URLS") {
        let urls = value
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        if urls.len() == DB_COUNT as usize {
            return urls;
        }
        eprintln!(
            "[order-api] TC_ORDER_MYSQL_SHARD_URLS must contain {DB_COUNT} urls; got {}",
            urls.len()
        );
    }
    let url = std::env::var("TC_ORDER_MYSQL_URL").expect("TC_ORDER_MYSQL_URL");
    vec![url; DB_COUNT as usize]
}

fn open_when_ready(shard_urls: &[String]) -> OrderStore {
    loop {
        let opened = (|| -> mysql::Result<OrderStore> {
            let mut shards = Vec::with_capacity(shard_urls.len());
            for url in shard_urls {
                shards.push(Pool::new(url.as_str())?);
            }
            let store = OrderStore {
                shards: Arc::new(shards),
                category_size: category_size(),
                metrics: Arc::new(OrderPipelineMetrics::default()),
            };
            bootstrap(&store)?;
            Ok(store)
        })();
        match opened {
            Ok(store) => return store,
            Err(error) => {
                eprintln!("[order-api] waiting for MySQL/bootstrap: {error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
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
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
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
            stream.set_read_timeout(Some(Duration::from_millis(750)))?;
            stream.set_write_timeout(Some(Duration::from_millis(750)))?;
            self.stream = Some(stream);
            self.active_target = Some(index);
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no reachable Raft leader",
        ))
    }

    fn send_batch(&mut self, records: &[&KafkaRecord]) -> std::io::Result<()> {
        if records.is_empty() || records.len() > wire::RAFT_BATCH_MAX_COMMANDS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid matcher batch size",
            ));
        }
        let mut payload = Vec::with_capacity(8 + records.len() * wire::MSG_LEN);
        payload.extend_from_slice(b"TCB1");
        payload.extend_from_slice(&(records.len() as u32).to_be_bytes());
        for record in records {
            payload.extend_from_slice(&record.frame);
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
        eprintln!("[order-api] ignored invalid TC_RAFT_GROUP_MATCHERS");
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

fn handle(stream: TcpStream, store: Arc<OrderStore>, kafka: Arc<KafkaIngress>, token: &str) {
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
                &store.metrics.render(),
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

        if method == "POST" && path == "/commands/batch" {
            match decode_batch(&body, store.category_size) {
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
                        respond(
                            reader.get_mut(),
                            status,
                            &format!("{{\"error\":\"{error}\"}}"),
                            keep_alive,
                        )
                    }
                },
                Err(error) => respond(
                    reader.get_mut(),
                    "400 Bad Request",
                    &format!("{{\"error\":\"{error}\"}}"),
                    keep_alive,
                ),
            }
            if !keep_alive {
                return;
            }
            continue;
        }
        let persisted = match (method.as_str(), path) {
            ("POST", "/orders") => order_from_query(query).and_then(|order| {
                let mut frame = [0u8; wire::MSG_LEN];
                wire::encode_new(&order, &mut frame);
                let category = sharding::asset_category(order.instrument, store.category_size);
                kafka.publish(category, order.user, &frame)
            }),
            ("POST", "/cancels") => cancel_from_query(query).and_then(|(i, o, c, u)| {
                let mut frame = [0u8; wire::MSG_LEN];
                wire::encode_cancel(i, o, c, &mut frame);
                let category = sharding::asset_category(i, store.category_size);
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
            Err(error) => respond(
                reader.get_mut(),
                ingress_error_status(&error),
                &format!("{{\"error\":\"{error}\"}}"),
                keep_alive,
            ),
        }
        if !keep_alive {
            return;
        }
    }
}

fn main() {
    let shard_urls = parse_shard_urls();
    let matcher_groups = parse_matcher_groups();
    let token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let store = open_when_ready(&shard_urls);
    let kafka = KafkaIngress::from_env(store.metrics.clone())
        .expect("configure Kafka order ingress")
        .expect("TC_ORDER_KAFKA_BROKERS is required");
    eprintln!(
        "[order-api] category_size={} raft_groups={} db_consumers={} matcher_consumers={} execution_consumers={} max_pipeline_backlog={} db_group={} matcher_group={} execution_group={} kafka=true",
        store.category_size,
        matcher_groups.len(),
        kafka.db_consumers,
        kafka.matcher_consumers,
        kafka.execution_consumers,
        kafka.max_pipeline_backlog,
        kafka.db_group,
        kafka.matcher_group,
        kafka.execution_group,
    );
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
    for worker in 0..kafka.db_consumers {
        let consumer_store = store.clone();
        let consumer_kafka = kafka.clone();
        let group_id = kafka.db_group.clone();
        let category_size = store.category_size;
        std::thread::Builder::new()
            .name(format!("order-kafka-mysql-{worker}"))
            .spawn(move || {
                run_kafka_stage(
                    consumer_kafka,
                    category_size,
                    group_id,
                    "mysql",
                    worker,
                    move |batch| persist_kafka_batch(&consumer_store, batch),
                )
            })
            .expect("spawn Kafka MySQL projection consumer");
    }
    for worker in 0..kafka.matcher_consumers {
        let consumer_kafka = kafka.clone();
        let group_id = kafka.matcher_group.clone();
        let category_size = store.category_size;
        let worker_matchers = matcher_groups.clone();
        let consumer_store_metrics = store.metrics.clone();
        let raft_group_pins = kafka.raft_group_pins.clone();
        std::thread::Builder::new()
            .name(format!("order-kafka-match-{worker}"))
            .spawn(move || {
                let mut matchers = worker_matchers
                    .into_iter()
                    .map(MatcherConnection::new)
                    .collect::<Vec<_>>();
                run_kafka_stage(
                    consumer_kafka,
                    category_size,
                    group_id,
                    "match",
                    worker,
                    move |batch| {
                        let started = std::time::Instant::now();
                        let result = forward_kafka_batch(&mut matchers, batch, &raft_group_pins);
                        consumer_store_metrics.record_raft(started.elapsed());
                        result
                    },
                )
            })
            .expect("spawn Kafka matching consumer");
    }
    for worker in 0..kafka.execution_consumers {
        let execution_store = store.clone();
        let execution_kafka = kafka.clone();
        std::thread::Builder::new()
            .name(format!("execution-kafka-mysql-{worker}"))
            .spawn(move || run_execution_mysql_consumer(execution_store, execution_kafka, worker))
            .expect("spawn execution MySQL projection consumer");
    }
    let listener = TcpListener::bind("0.0.0.0:9200").expect("bind order API");
    let shared_store = Arc::new(store);
    let shared_kafka = Arc::new(kafka);
    for stream in listener.incoming().flatten() {
        let store = shared_store.clone();
        let kafka = shared_kafka.clone();
        let token = token.clone();
        std::thread::spawn(move || handle(stream, store, kafka, &token));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_order(user: u64, instrument: u32) -> Vec<u8> {
        let order = Order::limit(OrderId(99), Side::Buy, 1_000, 2)
            .on(InstrumentId(instrument))
            .by(user);
        let mut frame = [0u8; wire::MSG_LEN];
        wire::encode_new(&order, &mut frame);
        let mut body = user.to_le_bytes().to_vec();
        body.extend_from_slice(&frame);
        body
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
    fn ingress_backpressure_waits_for_both_fanout_stages() {
        let metrics = OrderPipelineMetrics::default();
        metrics.try_reserve(3, 5).unwrap();
        metrics.complete("mysql", 3);
        assert_eq!(metrics.backlog(), 3);

        metrics.complete("match", 2);
        assert_eq!(metrics.backlog(), 1);
        metrics.try_reserve(4, 5).unwrap();
        assert!(metrics.try_reserve(1, 5).is_err());
        assert_eq!(
            ingress_error_status("backpressure: full"),
            "429 Too Many Requests"
        );
    }
}
