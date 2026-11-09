//! Execution topic -> MySQL: staging events in pending, draining them into order rows and fills.

use super::*;

pub(crate) fn execution_consumer_batch_size(per_db: usize, db_count: usize) -> usize {
    per_db.max(1).saturating_mul(db_count.max(1))
}

/// Order tables whose new rows may have execution events waiting in
/// `pending_execution_events`, per physical database. The command writer
/// marks them after its commit; the execution writer of the same database
/// takes them and drains, so it is the only writer that touches pending
/// rows. In memory only: on start every table is marked once, which covers
/// marks lost to a crash.
pub(crate) struct DrainHandoff {
    pub(crate) dirty: Vec<std::sync::Mutex<std::collections::BTreeSet<u32>>>,
}

impl DrainHandoff {
    pub(crate) fn new(routing: sharding::RouteConfig) -> Self {
        let all: std::collections::BTreeSet<u32> = (0..routing.tables_per_db as u32).collect();
        Self {
            dirty: (0..routing.db_count)
                .map(|_| std::sync::Mutex::new(all.clone()))
                .collect(),
        }
    }

    pub(crate) fn mark(&self, db: u32, tables: impl IntoIterator<Item = u32>) {
        self.dirty[db as usize]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(tables);
    }

    pub(crate) fn take(&self, db: u32) -> std::collections::BTreeSet<u32> {
        std::mem::take(
            &mut *self.dirty[db as usize]
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// Drain the pending events of `tables` (marked by the command writer) in
/// one transaction; a table with nothing pending costs one `LIMIT 1` probe.
pub(crate) fn drain_marked_tables(
    store: &OrderStore,
    shard_db: u32,
    tables: &std::collections::BTreeSet<u32>,
) -> Result<(), String> {
    let db = format!("order_db_{shard_db}");
    let mut conn = store
        .shard(shard_db)
        .get_conn()
        .map_err(|error| error.to_string())?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .map_err(|error| error.to_string())?;
    let period = dedupe_period(&mut tx)?;
    for &table in tables {
        let has_pending: Option<u8> = tx
            .query_first(format!(
                "SELECT 1 FROM {db}.pending_execution_events WHERE shard_table={table} LIMIT 1"
            ))
            .map_err(|error| error.to_string())?;
        if has_pending.is_some() {
            apply_pending_execution_table(&mut tx, &db, table, period)?;
        }
    }
    tx.commit().map_err(|error| error.to_string())
}

pub(crate) fn run_execution_mysql_consumer(
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
    let mut idle_commit = IdleCommit::default();
    loop {
        // Between batches only: a batch in hand is finished and its offsets
        // committed. Returning drops the consumer, which leaves its group so
        // the partitions move to the replacement at once instead of after the
        // session timeout (a ~1 min matching stall after every redeploy).
        if SHUTDOWN.load(AtomicOrdering::SeqCst) {
            return;
        }
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            idle_commit.flush(&consumer);
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
        let mut dead_lettered = Vec::new();
        push_execution_record(&metrics, &dlq, worker, &first, &mut records, &mut dead_lettered);
        let deadline = std::time::Instant::now() + kafka.execution_linger;
        while records.len() < kafka.execution_batch_size && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(message)) => push_execution_record(
                    &metrics,
                    &dlq,
                    worker,
                    &message,
                    &mut records,
                    &mut dead_lettered,
                ),
                Some(Err(error)) => {
                    log_warn!("execution-mysql", worker; "event=poll_failed error={error}")
                }
                None => break,
            }
        }
        if records.is_empty() {
            // Only dead letters: nothing earlier in this batch is unpersisted.
            for (topic, partition, offset) in &dead_lettered {
                idle_commit.record_offset(topic, *partition, *offset);
            }
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
            if let Err(error) = commit_execution_batch(
                &consumer,
                &records,
                &dead_lettered,
                kafka.async_offset_commits,
            ) {
                log_error!("execution-mysql", worker; "event=offset_commit_failed error={error}");
            }
            for record in &records {
                idle_commit.record_offset(&record.topic, record.partition, record.offset);
            }
            for (topic, partition, offset) in &dead_lettered {
                idle_commit.record_offset(topic, *partition, *offset);
            }
        } else {
            rewind_execution_batch(&consumer, &records);
        }
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionKafkaRecord {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) event: wire::ExecutionEvent,
}

/// An undecodable message, already written to the dead-letter file: its
/// offset is committed with the rest of its batch, never ahead of it.
pub(crate) type DeadLettered = (String, i32, i64);

pub(crate) fn push_execution_record(
    metrics: &OrderPipelineMetrics,
    dlq: &Arc<Mutex<DlqWriter>>,
    worker: usize,
    message: &rdkafka::message::BorrowedMessage<'_>,
    records: &mut Vec<ExecutionKafkaRecord>,
    dead_lettered: &mut Vec<DeadLettered>,
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
    // Committing this offset at once (as before) moved the partition's
    // committed position past records of the same batch that were not
    // persisted yet: a crash then skipped them for good. And a failed
    // dead-letter write dropped the message silently. Now the offset goes
    // with its batch, and a failed write stops the process before any
    // offset moves, so the message is redelivered.
    let payload = message.payload().unwrap_or(&[]);
    dlq.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .append(partition, offset, "invalid execution report", payload)
        .unwrap_or_else(|error| panic!("execution DLQ append failed ({error}); refusing to skip offset {offset}"));
    metrics.dlq_total.fetch_add(1, AtomicOrdering::Relaxed);
    dead_lettered.push((message.topic().to_string(), partition, offset));
    log_warn!("execution-mysql", worker; "event=dead_lettered reason=undecodable partition={partition} offset={offset}");
}

pub(crate) fn commit_execution_batch(
    consumer: &BaseConsumer,
    records: &[ExecutionKafkaRecord],
    dead_lettered: &[DeadLettered],
    async_commit: bool,
) -> Result<(), String> {
    let mut offsets: HashMap<(&str, i32), i64> = HashMap::new();
    let positions = records
        .iter()
        .map(|record| (record.topic.as_str(), record.partition, record.offset))
        .chain(dead_lettered.iter().map(|(topic, partition, offset)| (topic.as_str(), *partition, *offset)));
    for (topic, partition, offset) in positions {
        offsets
            .entry((topic, partition))
            .and_modify(|next| *next = (*next).max(offset + 1))
            .or_insert(offset + 1);
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

pub(crate) fn rewind_execution_batch(consumer: &BaseConsumer, records: &[ExecutionKafkaRecord]) {
    rewind_to_earliest(
        consumer,
        records.iter().map(|record| (record.topic.as_str(), record.partition, record.offset)),
    );
}

pub(crate) type ExecutionShardTables = HashMap<u32, Vec<(ExecutionKafkaRecord, u64)>>;

pub(crate) struct ExecutionShardJob {
    pub(crate) tables: ExecutionShardTables,
    pub(crate) reply: std::sync::mpsc::SyncSender<Result<(), String>>,
}

#[derive(Clone)]
pub(crate) struct ExecutionDbForwarder {
    pub(crate) routing: sharding::RouteConfig,
    pub(crate) shards: Arc<Vec<Option<std::sync::mpsc::SyncSender<ExecutionShardJob>>>>,
    pub(crate) owned_dbs: Arc<std::collections::HashSet<u32>>,
}

impl ExecutionDbForwarder {
    pub(crate) fn spawn(store: OrderStore, handoff: Option<Arc<DrainHandoff>>) -> Self {
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
            let handoff = handoff.clone();
            std::thread::Builder::new()
                .name(format!("execution-db-shard-{shard_db}"))
                .spawn(move || {
                    let mut last_sweep = std::time::Instant::now();
                    let mut ring = DedupeRing::new();
                    loop {
                        match ring.maintain(&shard_store, shard_db) {
                            Ok(0) => {}
                            Ok(truncated) => {
                                shard_store
                                    .metrics
                                    .dedupe_truncated_partitions
                                    .fetch_add(truncated, AtomicOrdering::Relaxed);
                            }
                            Err(error) => {
                                log_warn!("execution-db", shard_db; "event=dedupe_ring_failed error={error}");
                            }
                        }
                        // Tables the command writer filled since the last pass.
                        if let Some(handoff) = &handoff {
                            let tables = handoff.take(shard_db);
                            if !tables.is_empty() {
                                if let Err(error) =
                                    drain_marked_tables(&shard_store, shard_db, &tables)
                                {
                                    handoff.mark(shard_db, tables);
                                    log_warn!("execution-db", shard_db; "event=pending_drain_failed error={error}");
                                }
                            }
                        }
                        let wait = if handoff.is_some() { 20 } else { 250 };
                        let first = match rx.recv_timeout(Duration::from_millis(wait)) {
                            Ok(first) => first,
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                if last_sweep.elapsed() >= Duration::from_millis(250) {
                                    last_sweep = std::time::Instant::now();
                                    if let Err(error) = sweep_pending_execution_shard(
                                        &shard_store,
                                        shard_db,
                                    ) {
                                        log_warn!("execution-db", shard_db; "event=pending_sweep_failed error={error}");
                                    }
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

    pub(crate) fn submit(&self, records: &[ExecutionKafkaRecord]) -> Result<(), String> {
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

    pub(crate) fn owns(&self, db: u32) -> bool {
        self.owned_dbs.contains(&db)
    }
}

/// Report types that change an order row. Everything else on the execution
/// topic — `NotFound`, the halt/resume acks (order id 0), command-level
/// rejects (keyed by a command id, see `ExecReport::CommandRejected`) — must
/// not be staged: no order row ever matches some of them, so they would sit in
/// `pending_execution_events` forever, and for the rest the status `CASE` in
/// `apply_pending_execution_table` reads an unrecognised latest event as "no
/// terminal state", flipping e.g. a CANCELLED order back to OPEN on a repeated
/// cancel's `NotFound`.
pub(crate) fn projects_order_state(type_code: u8) -> bool {
    matches!(
        type_code,
        wire::RT_ACCEPTED
            | wire::RT_TRADE
            | wire::RT_FILLED
            | wire::RT_PARTIAL
            | wire::RT_RESTING
            | wire::RT_CANCELLED
            | wire::RT_REJECTED
            | wire::RT_MODIFIED
            | wire::RT_STOP_ACCEPTED
    )
}

/// `fill_role` values in `pending_execution_events` / `order_fills`.
pub(crate) const FILL_ROLE_TAKER: u8 = 1;
pub(crate) const FILL_ROLE_MAKER: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TradeFill {
    pub(crate) price: u64,
    pub(crate) fee: u64,
    pub(crate) role: u8,
}

/// The fill a Trade report means for one of its two target orders. A Trade is
/// materialized once per side: `order_id` is the taker, `aux_id` the maker, and
/// the event's `target_order_id` says which side this row is for.
pub(crate) fn trade_fill(report: &wire::DecodedReport, target_order_id: u64) -> Option<TradeFill> {
    if report.type_code != wire::RT_TRADE {
        return None;
    }
    let (role, fee) = if target_order_id == report.order_id.0 {
        (FILL_ROLE_TAKER, report.taker_fee)
    } else {
        (FILL_ROLE_MAKER, report.maker_fee)
    };
    Some(TradeFill {
        price: report.price,
        fee,
        role,
    })
}

/// SQL list of the report types [`projects_order_state`] accepts.
pub(crate) fn projected_report_types_sql() -> String {
    (0..=u8::MAX)
        .filter(|&code| projects_order_state(code))
        .map(|code| code.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn sweep_pending_execution_shard(store: &OrderStore, shard_db: u32) -> Result<(), String> {
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
    let period = dedupe_period(&mut tx)?;
    for table in tables {
        apply_pending_execution_table(&mut tx, &db, table, period)?;
    }
    tx.commit().map_err(|error| error.to_string())
}

pub(crate) fn persist_execution_shard(
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
        // Reports that change no order are consumed (their Kafka offsets still
        // commit with the batch) but never staged; see `projects_order_state`.
        for (record, target_order_id) in records {
            if projects_order_state(record.event.report.type_code) {
                updates.push((shard_table, record, target_order_id));
            }
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
        let mut values = Vec::with_capacity(chunk.len() * 13);
        let mut touched = std::collections::BTreeSet::new();
        for (table, record, target_order_id) in chunk {
            touched.insert(*table);
            let event = record.event;
            let fill = trade_fill(&event.report, *target_order_id);
            values.extend([
                Value::from(event.raft_group),
                Value::from(event.raft_index),
                Value::from(event.ordinal),
                Value::from(*target_order_id),
                Value::from(*table),
                Value::from(event.report.instrument.0),
                Value::from(event.report.type_code),
                Value::from(event.report.qty),
                Value::from(fill.map_or(0, |fill| fill.price)),
                Value::from(fill.map_or(0, |fill| fill.fee)),
                Value::from(fill.map_or(0, |fill| fill.role)),
                Value::from(record.partition),
                Value::from(record.offset as u64),
            ]);
        }
        // Stage, discard redeliveries, and drain every touched table in one
        // round trip (plus the period read): the same statements in the same
        // order, inside the same transaction.
        let period = dedupe_period(&mut tx)?;
        let mut statements = Vec::with_capacity(touched.len() + 2);
        statements.extend(multi_insert_sql(
            &format!("INSERT IGNORE INTO {db}.pending_execution_events (raft_group,raft_index,report_ordinal,target_order_id,shard_table,instrument,report_type,event_qty,event_price,event_fee,fill_role,kafka_partition,kafka_offset)"),
            13,
            &values,
        ));
        // A Kafka retry can reinsert an already-processed event into pending;
        // discard it before applying any mutations.
        statements.push(discard_statements(&db, shard_db, period));
        // Every touched table was just staged into, so drain them all (an
        // empty drain is a cheap empty range scan).
        statements.extend(touched.into_iter().map(|table| drain_statements(&db, table, period)));
        exec_batch(&mut tx, &statements.join(";\n"))
            .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Drop pending events that were already applied (a Kafka or outbox
/// redelivery), checking the pre-ring table too while it still exists.
#[cfg(test)]
pub(crate) fn discard_redelivered_events(
    tx: &mut Transaction<'_>,
    db: &str,
    shard_db: u32,
    period: u64,
) -> Result<(), String> {
    exec_batch(&mut *tx, &discard_statements(db, shard_db, period))
        .map_err(|error| error.to_string())
}

/// The statements that drop already-applied (redelivered) events from
/// pending, joined with `;`. The current and the previous period's tables
/// hold every event applied within the last retention period (see
/// `DEDUPE_TABLES`).
pub(crate) fn discard_statements(db: &str, shard_db: u32, period: u64) -> String {
    let mut tables = vec![dedupe_table(period), dedupe_table(period + DEDUPE_TABLES - 1)];
    if has_legacy_dedupe(shard_db) {
        tables.push("processed_execution_events_legacy".to_string());
    }
    tables
        .iter()
        .map(|table| {
            format!(
                "DELETE p FROM {db}.pending_execution_events p INNER JOIN {db}.{table} e ON e.raft_group=p.raft_group AND e.raft_index=p.raft_index AND e.report_ordinal=p.report_ordinal AND e.target_order_id=p.target_order_id"
            )
        })
        .collect::<Vec<_>>()
        .join(";\n")
}

pub(crate) fn apply_pending_execution_table(
    tx: &mut Transaction<'_>,
    db: &str,
    shard_table: u32,
    period: u64,
) -> Result<(), String> {
    exec_batch(&mut *tx, &drain_statements(db, shard_table, period))
        .map_err(|error| error.to_string())
}

/// The four statements that drain one order table's pending events (apply
/// to the order rows, promote to the dedupe table, write fills, delete),
/// joined with `;` so they travel in one round trip; several tables' drains
/// can be concatenated into one round trip too. Same statements, same order
/// as before — only the number of client/server round trips changed (they,
/// not the statements, bounded the execution projection: up to ~5 per order
/// table per batch, with a batch spread over ~100 tables per database).
pub(crate) fn drain_statements(db: &str, shard_table: u32, period: u64) -> String {
    let processed = dedupe_table(period);
    let table = format!("asset_orders_{shard_table:03}");
    // MySQL 8 window functions select the last event per order while the
    // aggregate folds all trades into one set-based row mutation.
    // NOTE: every line ends with a SPACE before the `\` continuation. Rust's
    // string `\`-newline escape strips the newline *and the next line's leading
    // whitespace*, so without the trailing space keyword boundaries collapse
    // (`latest_type` + `FROM` -> `latest_typeFROM`), producing MySQL error 1064.
    let mut sql = format!(
        "UPDATE {db}.{table} o INNER JOIN ( \
         SELECT target_order_id, \
                SUM(IF(report_type={trade},event_qty,0)) trade_qty, \
                MAX(IF(report_type={partial},event_qty,0)) partial_qty, \
                MAX(IF(report_type={filled},1,0)) has_filled, \
                MAX(IF(rn=1,report_type,0)) latest_type, \
                SUM(IF(report_type={trade},CAST(event_qty AS DECIMAL(38,0))*event_price,0)) trade_notional \
         FROM ( \
           SELECT p.*, ROW_NUMBER() OVER (PARTITION BY target_order_id ORDER BY kafka_offset DESC) rn \
           FROM {db}.pending_execution_events p WHERE shard_table={shard_table} \
         ) ranked GROUP BY target_order_id \
         ) e ON e.target_order_id=o.order_id \
         SET o.status=CASE \
               WHEN e.has_filled=1 OR GREATEST(o.filled_qty+e.trade_qty,e.partial_qty)>=o.qty THEN 'FILLED' \
               WHEN e.latest_type={cancelled} THEN 'CANCELLED' \
               WHEN e.latest_type={rejected} THEN 'REJECTED' \
               WHEN e.latest_type={stop_accepted} THEN 'UNTRIGGERED' \
               WHEN GREATEST(o.filled_qty+e.trade_qty,e.partial_qty)>0 THEN 'PARTIAL' \
               ELSE 'OPEN' END, \
             o.filled_qty=LEAST(o.qty,GREATEST(o.filled_qty+e.trade_qty,e.partial_qty,IF(e.has_filled=1,o.qty,0))), \
             o.filled_notional=o.filled_notional+e.trade_notional",
        trade = wire::RT_TRADE,
        partial = wire::RT_PARTIAL,
        filled = wire::RT_FILLED,
        cancelled = wire::RT_CANCELLED,
        rejected = wire::RT_REJECTED,
        stop_accepted = wire::RT_STOP_ACCEPTED,
    );
    sql.push_str(";\n");
    sql.push_str(&format!(
        "INSERT IGNORE INTO {db}.{processed} (raft_group,raft_index,report_ordinal,target_order_id,shard_table,instrument,report_type,kafka_partition,kafka_offset) \
         SELECT p.raft_group,p.raft_index,p.report_ordinal,p.target_order_id,p.shard_table,p.instrument,p.report_type,p.kafka_partition,p.kafka_offset \
         FROM {db}.pending_execution_events p INNER JOIN {db}.{table} o ON o.order_id=p.target_order_id WHERE p.shard_table={shard_table}",
    ));
    sql.push_str(";\n");
    // Same join as the promotion above, so a fill lands exactly when its event
    // is promoted (an event whose order row is not projected yet waits in
    // pending for both). The order row supplies user_id and side.
    sql.push_str(&format!(
        "INSERT IGNORE INTO {db}.order_fills (raft_group,raft_index,report_ordinal,order_id,user_id,instrument,side,price,qty,fee,fill_role,created_at) \
         SELECT p.raft_group,p.raft_index,p.report_ordinal,p.target_order_id,o.user_id,p.instrument,o.side,p.event_price,p.event_qty,p.event_fee,p.fill_role,p.created_at \
         FROM {db}.pending_execution_events p INNER JOIN {db}.{table} o ON o.order_id=p.target_order_id \
         WHERE p.shard_table={shard_table} AND p.report_type={trade}",
        trade = wire::RT_TRADE,
    ));
    sql.push_str(";\n");
    sql.push_str(&format!(
        "DELETE p FROM {db}.pending_execution_events p INNER JOIN {db}.{processed} e ON e.raft_group=p.raft_group AND e.raft_index=p.raft_index AND e.report_ordinal=p.report_ordinal AND e.target_order_id=p.target_order_id WHERE p.shard_table={shard_table}"
    ));
    sql
}
