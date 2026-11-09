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

/// Execution-event dedupe history only answers "was this event already
/// applied?" for Kafka/outbox redeliveries, so it only needs recent history.
///
/// Deleting old rows one by one costs as much as inserting them (measured:
/// MySQL p99 4 s -> 8 s while purging). So the history rotates through
/// three plain tables, `processed_execution_events_{0,1,2}`, one per
/// retention period: rows go to the current period's table, a redelivery
/// is checked against the current and the previous table, and the next
/// period's table is emptied with a metadata-only `TRUNCATE TABLE` before
/// it is reused. Any event applied less than one period ago is therefore
/// always found.
///
/// Plain tables, not a partitioned one: a partition key must be part of
/// every unique key, which turned the event-key lookup of the redelivery
/// check from a unique (eq_ref) into a prefix (ref) probe across
/// partitions. That made the execution projection ~5x slower (clean 100k
/// runs: 5.7 s -> 30 s end to end).
pub(crate) const DEDUPE_TABLES: u64 = 3;

/// Hours of dedupe history guaranteed: `TC_ORDER_DEDUPE_RETENTION_HOURS`,
/// default 24 (rows live 24-48 h). It must exceed the longest time an
/// execution event can be redelivered (consumer restarts and outbox replays
/// take seconds); an event replayed after its row is gone would be applied
/// twice.
pub(crate) fn dedupe_retention_hours() -> u64 {
    static HOURS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *HOURS.get_or_init(|| env_number("TC_ORDER_DEDUPE_RETENTION_HOURS", 24u64).max(1))
}

/// The dedupe table of `period`.
pub(crate) fn dedupe_table(period: u64) -> String {
    format!("processed_execution_events_{}", period % DEDUPE_TABLES)
}

/// The current retention period by the MySQL server's clock — the clock
/// the rotation reads too, so host clock skew plays no part. One cheap
/// query per transaction; every statement of the transaction uses it.
pub(crate) fn dedupe_period(conn: &mut impl Queryable) -> Result<u64, String> {
    conn.query_first(format!("SELECT UNIX_TIMESTAMP() DIV {}", dedupe_retention_hours() * 3600))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "no clock row".to_string())
}

/// Databases whose pre-rotation dedupe table was renamed to
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

/// Period maintenance of one database's dedupe tables, run by that
/// database's execution writer between batches — the writer that inserts
/// into them, so the truncate never waits on its own transactions.
pub(crate) struct DedupeRing {
    /// Period whose next table was handled last (None: not yet in this
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

    /// Cheap unless due: one clock read every 10 s; a table is only touched
    /// once per period. Returns the tables truncated.
    pub(crate) fn maintain(&mut self, store: &OrderStore, db: u32) -> Result<u64, String> {
        let now = std::time::Instant::now();
        if now < self.next_check {
            return Ok(0);
        }
        self.next_check = now + Duration::from_secs(10);
        let mut conn = store.shard(db).get_conn().map_err(|error| error.to_string())?;
        let period = dedupe_period(&mut conn)?;
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

    /// Empty the next period's table once per `period`, if it holds rows (a
    /// truncate recreates the table: seconds on slow storage, on the
    /// execution writer). Nothing when the server clock went back: the
    /// "next" table could then be the live one.
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
        let table = dedupe_table(period + 1);
        let occupied: Option<u8> = conn
            .query_first(format!("SELECT 1 FROM order_db_{db}.{table} LIMIT 1"))
            .map_err(|error| error.to_string())?;
        if occupied.is_none() {
            return Ok(0);
        }
        conn.query_drop(format!("TRUNCATE TABLE order_db_{db}.{table}"))
            .map_err(|error| error.to_string())?;
        Ok(1)
    }
}
