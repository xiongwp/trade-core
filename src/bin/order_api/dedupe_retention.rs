//! Retention purge of the dedupe tables and the optional command audit log.

use super::*;

/// Whether the command projection also writes `processed_commands` (one row
/// per command: Kafka position, command id, user, shard table).
///
/// Nothing reads that table — redelivered commands are already harmless
/// (`INSERT IGNORE` on the order's unique `order_id`, modifies are plain
/// `SET`s) — so by default the row, its primary key and its `UNIQUE
/// (command_id)` index are no longer written on the hot path. Set
/// `TC_ORDER_COMMAND_LOG=1` to keep it as an audit trail; the dedupe
/// retention below then bounds it.
pub(crate) fn command_log_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_enabled("TC_ORDER_COMMAND_LOG", false))
}

/// Dedupe tables that only grow, with the primary-key prefix the purge walks:
/// `(group column, monotonically increasing position within the group)`.
pub(crate) const DEDUPE_TABLES: [(&str, &str, &str); 2] = [
    // Execution events already applied to their order: only consulted to
    // drop a Kafka/outbox redelivery of the same event.
    ("processed_execution_events", "raft_group", "raft_index"),
    ("processed_commands", "category_id", "kafka_offset"),
];
pub(crate) const DEDUPE_PURGE_CHUNK: usize = 5_000;

/// Keep the dedupe tables to a retention window so the projection does not
/// slow down as history accumulates (measured: ~20% lower end-to-end TPS
/// with 10x the data, MySQL backlog the largest factor).
///
/// `TC_ORDER_DEDUPE_RETENTION_SECS` (default 86400, 0 = keep forever) must
/// exceed the longest time an execution event can be redelivered — consumer
/// restarts and outbox replays are seconds — since an event replayed after
/// its row is purged would be applied twice. There is no index on
/// `created_at` (it would cost a write per event), so the purge samples the
/// per-group high-water mark of the primary key every interval and, once a
/// sample is older than the retention, deletes everything below it in short
/// chunked transactions. Samples live in memory: after a restart nothing is
/// purged until a full retention has passed again, which only errs towards
/// keeping more.
pub(crate) fn spawn_dedupe_retention(store: OrderStore) {
    let retention = Duration::from_secs(env_number("TC_ORDER_DEDUPE_RETENTION_SECS", 86_400u64));
    if retention.is_zero() {
        return;
    }
    let interval = Duration::from_secs(env_number(
        "TC_ORDER_DEDUPE_PURGE_INTERVAL_SECS",
        (retention.as_secs() / 8).clamp(1, 300),
    ))
    .max(Duration::from_secs(1));
    let _ = std::thread::Builder::new()
        .name("dedupe-retention".into())
        .spawn(move || {
            let mut dbs: Vec<u32> = store.owned_dbs.iter().copied().collect();
            dbs.sort_unstable();
            let mut samples: HashMap<(u32, &str), std::collections::VecDeque<(std::time::Instant, Vec<(u64, u64)>)>> =
                HashMap::new();
            while !SHUTDOWN.load(AtomicOrdering::Acquire) {
                for &db in &dbs {
                    for (table, group, position) in DEDUPE_TABLES {
                        let history = samples.entry((db, table)).or_default();
                        match purge_dedupe_table(&store, db, table, group, position, retention, history) {
                            Ok(rows) if rows > 0 => {
                                store.metrics.dedupe_purged_rows.fetch_add(rows, AtomicOrdering::Relaxed);
                                log_info!("dedupe-retention", db; "event=purged table={table} rows={rows}");
                            }
                            Ok(_) => {}
                            Err(error) => {
                                log_warn!("dedupe-retention", db; "event=purge_failed table={table} error={error}");
                            }
                        }
                    }
                }
                let deadline = std::time::Instant::now() + interval;
                while std::time::Instant::now() < deadline && !SHUTDOWN.load(AtomicOrdering::Acquire) {
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        });
}

/// One retention step for one table: record the current per-group
/// high-water mark, then delete rows below the newest mark that is older
/// than `retention`. Returns the rows deleted.
pub(crate) fn purge_dedupe_table(
    store: &OrderStore,
    db: u32,
    table: &str,
    group: &str,
    position: &str,
    retention: Duration,
    history: &mut std::collections::VecDeque<(std::time::Instant, Vec<(u64, u64)>)>,
) -> Result<u64, String> {
    let mut conn = store.shard(db).get_conn().map_err(|error| error.to_string())?;
    // Loose index scan over the primary key: one probe per group.
    let marks: Vec<(u64, u64)> = conn
        .query(format!(
            "SELECT {group}, MAX({position}) FROM order_db_{db}.{table} GROUP BY {group}"
        ))
        .map_err(|error| error.to_string())?;
    let now = std::time::Instant::now();
    history.push_back((now, marks));
    let mut cutoff = None;
    while history
        .front()
        .is_some_and(|(taken, _)| now.duration_since(*taken) >= retention)
    {
        cutoff = history.pop_front().map(|(_, marks)| marks);
    }
    let Some(cutoff) = cutoff else {
        return Ok(0);
    };
    let mut deleted = 0;
    for (group_value, below) in cutoff {
        loop {
            conn.exec_drop(
                format!(
                    "DELETE FROM order_db_{db}.{table} WHERE {group}=? AND {position}<? LIMIT {DEDUPE_PURGE_CHUNK}"
                ),
                (group_value, below),
            )
            .map_err(|error| error.to_string())?;
            let rows = conn.affected_rows();
            deleted += rows;
            if rows < DEDUPE_PURGE_CHUNK as u64 || SHUTDOWN.load(AtomicOrdering::Acquire) {
                break;
            }
        }
    }
    Ok(deleted)
}
