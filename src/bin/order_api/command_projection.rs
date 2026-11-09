//! Command topic -> MySQL: per-DB writer threads inserting order rows.

use super::*;

#[derive(Clone)]
pub(crate) struct KafkaRecord {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) category_id: u32,
    pub(crate) user: u64,
    pub(crate) frame: [u8; wire::MSG_LEN],
}

pub(crate) fn command_id(command: &trade_core::Command) -> u64 {
    match command {
        trade_core::Command::New(order) | trade_core::Command::NewConditional(order, _) => {
            order.id.0
        }
        trade_core::Command::Cancel { cmd_id, .. }
        | trade_core::Command::Modify { cmd_id, .. }
        | trade_core::Command::Halt { cmd_id, .. }
        | trade_core::Command::Resume { cmd_id, .. }
        | trade_core::Command::HaltUser { cmd_id, .. }
        | trade_core::Command::ResumeUser { cmd_id, .. } => *cmd_id,
        trade_core::Command::ForceClose { close_order_id, .. } => close_order_id.0,
        trade_core::Command::Batch(commands) => commands.first().map(command_id).unwrap_or(0),
        trade_core::Command::Migrate(_) => 0,
    }
}

pub(crate) fn database_order_id(command: &trade_core::Command) -> u64 {
    match command {
        trade_core::Command::New(order) | trade_core::Command::NewConditional(order, _) => {
            order.id.0
        }
        trade_core::Command::Cancel { order_id, .. }
        | trade_core::Command::Modify { order_id, .. } => order_id.0,
        trade_core::Command::ForceClose { close_order_id, .. } => close_order_id.0,
        _ => command_id(command),
    }
}

pub(crate) struct CommandShardJob {
    pub(crate) records: Vec<KafkaRecord>,
    pub(crate) reply: std::sync::mpsc::SyncSender<Result<(), String>>,
}

#[derive(Clone)]
pub(crate) struct CommandDbForwarder {
    pub(crate) shards: Arc<Vec<Option<std::sync::mpsc::SyncSender<CommandShardJob>>>>,
    pub(crate) routing: sharding::RouteConfig,
    pub(crate) owned_dbs: Arc<std::collections::HashSet<u32>>,
}

impl CommandDbForwarder {
    pub(crate) fn spawn(store: OrderStore, handoff: Option<Arc<DrainHandoff>>) -> Self {
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
            let handoff = handoff.clone();
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
                        let result = persist_mysql_shard_with(
                            &shard_store,
                            shard_db,
                            &records,
                            handoff.as_deref(),
                        );
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

    pub(crate) fn submit(
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

    pub(crate) fn owns(&self, db: u32) -> bool {
        self.owned_dbs.contains(&db)
    }
}

/// Persist one routed shard batch with one MySQL commit. Long-lived per-shard
/// workers call this after coalescing jobs from concurrent Kafka consumers.
#[cfg(test)]
pub(crate) fn persist_mysql_shard(
    store: &OrderStore,
    shard_db: u32,
    records: &[KafkaRecord],
) -> Result<(), String> {
    persist_mysql_shard_with(store, shard_db, records, None)
}

/// [`persist_mysql_shard`], optionally handing the pending-event drain to
/// the execution writer of the same database instead of running it here.
pub(crate) fn persist_mysql_shard_with(
    store: &OrderStore,
    shard_db: u32,
    records: &[KafkaRecord],
    handoff: Option<&DrainHandoff>,
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

    let mut processed = Vec::with_capacity(if command_log_enabled() { records.len() * 6 } else { 0 });
    let mut orders_by_table: BTreeMap<u32, Vec<Value>> = BTreeMap::new();
    let mut modifies = Vec::new();

    for record in records {
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| "invalid Kafka command frame".to_string())?;
        let id = command_id(&command);
        let route = store.routing.route_order_id(database_order_id(&command));
        if command_log_enabled() {
            processed.extend([
            Value::from(record.category_id),
            Value::from(record.partition),
            Value::from((record.offset as u64).saturating_add(1)),
            Value::from(id),
            Value::from(record.user),
            Value::from(route.table),
            ]);
        }

        match command {
            // A conditional order gets its row at placement like any other;
            // the execution projection moves it on when it triggers.
            trade_core::Command::New(order) | trade_core::Command::NewConditional(order, _) => {
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
            // A cancel request changes no status: only the engine's answer
            // does (`Cancelled` -> CANCELLED; `NotFound` -> nothing). Marking
            // the row CANCEL_PENDING here overwrote orders that had already
            // FILLED/CANCELLED, and a late cancel's NotFound then left them
            // stuck (Binance spot likewise never uses PENDING_CANCEL).
            trade_core::Command::Cancel { .. } => {}
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

    // Every write of the batch in one round trip (values inlined, see
    // `multi_insert_sql`): one prepare + execute per order table used to
    // dominate, with a batch spread over ~100 tables per database.
    let mut statements = Vec::with_capacity(orders_by_table.len() + modifies.len() + 1);
    if command_log_enabled() {
        statements.extend(multi_insert_sql(
            &format!("INSERT IGNORE INTO {db}.processed_commands (category_id,kafka_partition,kafka_offset,command_id,user_id,shard_table)"),
            6,
            &processed,
        ));
    }
    let pending_tables = orders_by_table.keys().copied().collect::<Vec<_>>();
    for (table, values) in &orders_by_table {
        statements.extend(multi_insert_sql(
            &format!("INSERT IGNORE INTO {db}.asset_orders_{table:03} (order_id,category_id,user_id,instrument,side,price,qty,status)"),
            8,
            values,
        ));
    }
    for (table, order_id, user, new_price, new_qty) in &modifies {
        statements.push(format!(
            "UPDATE {db}.{table} SET price={new_price},qty={new_qty},status='MODIFY_PENDING' WHERE order_id={order_id} AND user_id={user}"
        ));
    }
    if !statements.is_empty() {
        exec_batch(&mut tx, &statements.join(";\n"))
            .map_err(|error| error.to_string())?;
    }
    // Results may have reached this database before the command projection.
    // Once the order rows exist, drain their durable pending events in the same
    // transaction so no external scheduler is required for the common race.
    //
    // The drain is three heavy set-based queries (a window-function UPDATE JOIN,
    // an INSERT..SELECT and a DELETE JOIN). On the common path — a fresh order
    // that rests without any execution event yet in this table — they match
    // nothing. Guard with a single cheap probe so the pure-resting path pays one
    // indexed `LIMIT 1` lookup instead of three empty joins. This is strictly a
    // no-op skip: when a row exists the drain runs exactly as before, so the
    // projection's behaviour (and its 1064-sensitive SQL) is untouched.
    //
    // With a hand-off (this process also runs the execution writer for this
    // database) the drain is left to that writer: the order transaction then
    // never reads or deletes `pending_execution_events`, so the two writers
    // no longer lock the same pending and order rows against each other.
    if let Some(handoff) = handoff {
        tx.commit().map_err(|error| error.to_string())?;
        handoff.mark(shard_db, pending_tables);
        store.metrics.record_mysql(started.elapsed());
        return Ok(());
    }
    for table in pending_tables {
        let has_pending: Option<u8> = tx
            .query_first(format!(
                "SELECT 1 FROM {db}.pending_execution_events WHERE shard_table={table} LIMIT 1"
            ))
            .map_err(|error| error.to_string())?;
        if has_pending.is_some() {
            let period = dedupe_period(&mut tx)?;
            apply_pending_execution_table(&mut tx, &db, table, period)?;
        }
    }
    tx.commit().map_err(|error| error.to_string())?;
    store.metrics.record_mysql(started.elapsed());
    Ok(())
}
