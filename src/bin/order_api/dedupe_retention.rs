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
/// `bucket = hour number mod DEDUPE_BUCKETS`, the table is `PARTITION BY
/// HASH(bucket)` (partition `p<n>` holds bucket n), and expired hours are
/// dropped with `TRUNCATE PARTITION` — a metadata operation, one per hour.
pub(crate) const DEDUPE_BUCKETS: u64 = 48;

/// The bucket of a row inserted now, by the MySQL server's clock (the same
/// clock [`DedupeRing`] reads, so no host clock skew is involved).
pub(crate) const DEDUPE_BUCKET_SQL: &str = "MOD(UNIX_TIMESTAMP() DIV 3600, 48)";

/// Hours of dedupe history kept: `TC_ORDER_DEDUPE_RETENTION_HOURS`, default
/// 24, at most 45. It must exceed the longest time an execution event can
/// be redelivered (consumer restarts and outbox replays take seconds); an
/// event replayed after its row is gone would be applied twice.
pub(crate) fn dedupe_retention_hours() -> u64 {
    env_number("TC_ORDER_DEDUPE_RETENTION_HOURS", 24u64).clamp(1, DEDUPE_BUCKETS - 3)
}

/// Buckets that are safe to truncate at `hour`: all but the live window,
/// which is the last `retention` hours, the current one, and the next one
/// (the hour can roll over between reading the clock and a concurrent
/// insert computing its bucket).
pub(crate) fn expired_buckets(hour: u64, retention: u64) -> Vec<u64> {
    let live: Vec<u64> = (0..=retention + 1)
        .map(|k| (hour + DEDUPE_BUCKETS - retention + k) % DEDUPE_BUCKETS)
        .collect();
    (0..DEDUPE_BUCKETS).filter(|bucket| !live.contains(bucket)).collect()
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

/// Hourly maintenance of one database's dedupe ring, run by that database's
/// execution writer between batches — the writer that inserts into the
/// table, so the truncate never waits on its own transactions.
pub(crate) struct DedupeRing {
    retention: u64,
    /// Hour whose expired buckets were truncated last (None: not yet in
    /// this process, so everything outside the live window is cleared once).
    last_hour: Option<u64>,
    next_check: std::time::Instant,
    started: std::time::Instant,
}

impl DedupeRing {
    pub(crate) fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            retention: dedupe_retention_hours(),
            last_hour: None,
            next_check: now,
            started: now,
        }
    }

    /// Cheap unless due: one clock read every 10 s; partitions are only
    /// touched when the hour changes. Returns the partitions truncated.
    pub(crate) fn maintain(&mut self, store: &OrderStore, db: u32) -> Result<u64, String> {
        let now = std::time::Instant::now();
        if now < self.next_check {
            return Ok(0);
        }
        self.next_check = now + Duration::from_secs(10);
        let mut conn = store.shard(db).get_conn().map_err(|error| error.to_string())?;
        let hour: u64 = conn
            .query_first("SELECT UNIX_TIMESTAMP() DIV 3600")
            .map_err(|error| error.to_string())?
            .ok_or("no clock row")?;
        let truncated = self.advance(&mut conn, db, hour)?;
        if has_legacy_dedupe(db) && self.started.elapsed() >= Duration::from_secs(self.retention * 3600) {
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

    /// Truncate the buckets that expired between the last handled hour and
    /// `hour` (all expired buckets on the first call).
    pub(crate) fn advance(
        &mut self,
        conn: &mut mysql::PooledConn,
        db: u32,
        hour: u64,
    ) -> Result<u64, String> {
        let buckets: Vec<u64> = match self.last_hour {
            // Same hour, or the server clock stepped back: truncating from an
            // earlier hour's window would clear the live buckets.
            Some(last) if hour <= last => return Ok(0),
            Some(last) if hour > last && hour - last < DEDUPE_BUCKETS => {
                let expired = expired_buckets(hour, self.retention);
                (last + 1..=hour)
                    .map(|h| (h + DEDUPE_BUCKETS - self.retention - 1) % DEDUPE_BUCKETS)
                    .filter(|bucket| expired.contains(bucket))
                    .collect()
            }
            _ => expired_buckets(hour, self.retention),
        };
        for &bucket in &buckets {
            conn.query_drop(format!(
                "ALTER TABLE order_db_{db}.processed_execution_events TRUNCATE PARTITION p{bucket}"
            ))
            .map_err(|error| error.to_string())?;
        }
        self.last_hour = Some(hour);
        Ok(buckets.len() as u64)
    }
}
