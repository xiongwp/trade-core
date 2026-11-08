//! Retention of the execution-event dedupe table (an hourly ring of
//! partitions) and the optional command audit log.

use super::*;

/// Whether the command projection also writes `processed_commands` (one row
/// per command: Kafka position, command id, user, shard table).
///
/// Nothing reads that table — redelivered commands are already harmless
/// (`INSERT IGNORE` on the order's unique `order_id`, modifies are plain
/// `SET`s) — so by default the row, its primary key and its `UNIQUE
/// (command_id)` index are no longer written on the hot path. Set
/// `TC_ORDER_COMMAND_LOG=1` to keep it as an audit trail; it is then never
/// purged (operators own its retention).
pub(crate) fn command_log_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_enabled("TC_ORDER_COMMAND_LOG", false))
}

/// `processed_execution_events` only answers "was this event already
/// applied?" for Kafka/outbox redeliveries, so it only needs recent history.
/// Deleting old rows one by one costs as much as inserting them (measured:
/// MySQL p99 4 s -> 8 s while purging). Instead every row carries
/// `bucket = (time / period) mod 3`, where the period is the retention, and
/// the table is `PARTITION BY HASH(bucket) PARTITIONS 3`: the current
/// period, the previous one, and the next one, which is emptied with a
/// metadata-only `TRUNCATE PARTITION` before it is reused. Retention is
/// therefore between one and two periods.
///
/// Three partitions, not one per hour: the redelivery check joins on the
/// event key without `bucket`, so MySQL probes every partition per pending
/// row. With 48 hourly partitions that made the execution projection ~5x
/// slower (clean 300k runs: 15k -> 2.9k cmd/s end to end).
pub(crate) const DEDUPE_PARTITIONS: u64 = 3;

/// Hours of dedupe history guaranteed: `TC_ORDER_DEDUPE_RETENTION_HOURS`,
/// default 24 (rows live 24-48 h). It must exceed the longest time an
/// execution event can be redelivered (consumer restarts and outbox replays
/// take seconds); an event replayed after its row is gone would be applied
/// twice.
pub(crate) fn dedupe_retention_hours() -> u64 {
    static HOURS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *HOURS.get_or_init(|| env_number("TC_ORDER_DEDUPE_RETENTION_HOURS", 24u64).max(1))
}

/// The bucket of a row inserted now, by the MySQL server's clock (the same
/// clock [`DedupeRing`] reads, so no host clock skew is involved).
pub(crate) fn dedupe_bucket_sql() -> String {
    format!(
        "MOD(UNIX_TIMESTAMP() DIV {}, {DEDUPE_PARTITIONS})",
        dedupe_retention_hours() * 3600
    )
}

/// The partition to empty during period `period`: the next one's, which
/// still holds rows from two periods ago.
pub(crate) fn bucket_to_clear(period: u64) -> u64 {
    (period + 1) % DEDUPE_PARTITIONS
}

/// Databases whose pre-ring dedupe table was renamed to
/// `processed_execution_events_legacy` at bootstrap. Redeliveries are
/// checked against it too until one retention has passed, then it is dropped.
pub(crate) fn legacy_dedupe_dbs() -> &'static Mutex<std::collections::BTreeSet<u32>> {
    static DBS: std::sync::OnceLock<Mutex<std::collections::BTreeSet<u32>>> =
        std::sync::OnceLock::new();
    DBS.get_or_init(Default::default)
}

pub(crate) fn has_legacy_dedupe(db: u32) -> bool {
    legacy_dedupe_dbs()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(&db)
}

/// Period maintenance of one database's dedupe ring, run by that database's
/// execution writer between batches — the writer that inserts into the
/// table, so the truncate never waits on its own transactions.
pub(crate) struct DedupeRing {
    /// Period whose next partition was cleared last (None: not yet in this
    /// process).
    last_period: Option<u64>,
    next_check: std::time::Instant,
    started: std::time::Instant,
}

impl DedupeRing {
    pub(crate) fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            last_period: None,
            next_check: now,
            started: now,
        }
    }

    /// Cheap unless due: one clock read every 10 s; a partition is only
    /// touched once per period. Returns the partitions truncated.
    pub(crate) fn maintain(&mut self, store: &OrderStore, db: u32) -> Result<u64, String> {
        let now = std::time::Instant::now();
        if now < self.next_check {
            return Ok(0);
        }
        self.next_check = now + Duration::from_secs(10);
        let mut conn = store.shard(db).get_conn().map_err(|error| error.to_string())?;
        let period: u64 = conn
            .query_first(format!("SELECT UNIX_TIMESTAMP() DIV {}", dedupe_retention_hours() * 3600))
            .map_err(|error| error.to_string())?
            .ok_or("no clock row")?;
        let truncated = self.advance(&mut conn, db, period)?;
        let retention = Duration::from_secs(dedupe_retention_hours() * 3600);
        if has_legacy_dedupe(db) && self.started.elapsed() >= retention {
            conn.query_drop(format!("DROP TABLE IF EXISTS order_db_{db}.processed_execution_events_legacy"))
                .map_err(|error| error.to_string())?;
            legacy_dedupe_dbs()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&db);
            log_info!("dedupe-retention", db; "event=legacy_dedupe_table_dropped");
        }
        Ok(truncated)
    }

    /// Empty the next period's partition once per `period`, if it holds rows
    /// (a truncate recreates the partition's tablespace: seconds on slow
    /// storage, on the execution writer). Nothing when the server clock went
    /// back: the "next" partition could then be the live one.
    pub(crate) fn advance(
        &mut self,
        conn: &mut mysql::PooledConn,
        db: u32,
        period: u64,
    ) -> Result<u64, String> {
        if self.last_period.is_some_and(|last| period <= last) {
            return Ok(0);
        }
        self.last_period = Some(period);
        let bucket = bucket_to_clear(period);
        let occupied: Option<u8> = conn
            .query_first(format!(
                "SELECT 1 FROM order_db_{db}.processed_execution_events PARTITION (p{bucket}) LIMIT 1"
            ))
            .map_err(|error| error.to_string())?;
        if occupied.is_none() {
            return Ok(0);
        }
        conn.query_drop(format!(
            "ALTER TABLE order_db_{db}.processed_execution_events TRUNCATE PARTITION p{bucket}"
        ))
        .map_err(|error| error.to_string())?;
        Ok(1)
    }
}
