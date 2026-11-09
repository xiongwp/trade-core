//! MySQL shard pools (`OrderStore`), URL/ownership parsing and multi-row insert helper.

use super::*;

#[derive(Clone)]
pub(crate) struct OrderStore {
    pub(crate) shards: Arc<Vec<Option<Pool>>>,
    pub(crate) owned_dbs: Arc<std::collections::HashSet<u32>>,
    /// Versioned shard routing record. All row placement goes through this so a
    /// parameter change is a fenced migration (see [`sharding::RouteConfig`]).
    pub(crate) routing: sharding::RouteConfig,
    pub(crate) metrics: Arc<OrderPipelineMetrics>,
}

impl OrderStore {
    pub(crate) fn shard(&self, db: u32) -> &Pool {
        self.shards[db as usize]
            .as_ref()
            .unwrap_or_else(|| panic!("physical DB {db} is not owned by this worker"))
    }

    pub(crate) fn owns(&self, db: u32) -> bool {
        self.owned_dbs.contains(&db)
    }
}

/// A multi-row `INSERT` as SQL text with the values inlined (rendered and
/// escaped by the driver), so it can share one round trip with other
/// statements. The prepared multi-row insert it replaced cost a prepare and
/// an execute per table, and its text changed with the row count, so the
/// server-side statement cache rarely hit.
/// Run several `;`-separated statements in one round trip and fail if ANY of
/// them fails. `query_drop` returns only the first statement's error: a later
/// one (a deadlock, which rolls the whole transaction back, or a lock wait
/// timeout, which rolls back just that statement) was discarded, and the
/// caller then committed a transaction that silently lacked those writes.
pub(crate) fn exec_batch(conn: &mut impl Queryable, sql: &str) -> mysql::Result<()> {
    let mut result = conn.query_iter(sql)?;
    while let Some(set) = result.iter() {
        for row in set {
            row?;
        }
    }
    Ok(())
}

pub(crate) fn multi_insert_sql(prefix: &str, columns: usize, values: &[Value]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    debug_assert_eq!(values.len() % columns, 0);
    let mut sql = String::with_capacity(prefix.len() + values.len() * 12);
    sql.push_str(prefix);
    sql.push_str(" VALUES ");
    for (row, chunk) in values.chunks(columns).enumerate() {
        if row > 0 {
            sql.push(',');
        }
        sql.push('(');
        for (column, value) in chunk.iter().enumerate() {
            if column > 0 {
                sql.push(',');
            }
            sql.push_str(&value.as_sql(false));
        }
        sql.push(')');
    }
    Some(sql)
}

/// One MySQL URL per physical database. An explicit `TC_ORDER_MYSQL_SHARD_URLS`
/// list must match the configured `db_count` (part of the versioned route
/// record); otherwise a single `TC_ORDER_MYSQL_URL` is fanned out to every
/// database.
pub(crate) fn parse_shard_urls(db_count: usize) -> Vec<String> {
    if let Ok(value) = std::env::var("TC_ORDER_MYSQL_SHARD_URLS") {
        let urls = value
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        // A wrong list is fatal: falling back to TC_ORDER_MYSQL_URL put every
        // database on that one server, and the count then validated.
        if let Err(error) = validate_shard_urls(&urls, db_count) {
            panic!("{error}");
        }
        return urls;
    }
    let url = std::env::var("TC_ORDER_MYSQL_URL").expect("TC_ORDER_MYSQL_URL");
    vec![url; db_count]
}

/// Startup guard: the shard URL count must equal `db_count` from the route
/// record, or rows would route to a database that has no connection pool.
pub(crate) fn validate_shard_urls(urls: &[String], db_count: usize) -> Result<(), String> {
    if urls.len() == db_count {
        Ok(())
    } else {
        Err(format!(
            "TC_ORDER_MYSQL_SHARD_URLS must contain {db_count} urls to match TC_DB_COUNT; got {}",
            urls.len()
        ))
    }
}

pub(crate) fn open_when_ready(
    shard_urls: &[String],
    routing: sharding::RouteConfig,
    metrics: Arc<OrderPipelineMetrics>,
    owned_dbs: Arc<std::collections::HashSet<u32>>,
    run_bootstrap: bool,
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
                // The projection's drains assume REPEATABLE READ: the apply
                // UPDATE's next-key locks keep an order row from appearing
                // before the promote INSERT ... SELECT of the same
                // transaction, which under READ COMMITTED would mark a fill
                // applied without applying it. Pinned per connection (no
                // per-transaction round trip), whatever the server default.
                let opts = mysql::Opts::from_url(url.as_str())
                    .map_err(|e| mysql::Error::from(io_shard_error(idx, &e)))?;
                // Bounded I/O: a blackholed host must fail a connect or a
                // statement (the caller retries; the projection is
                // idempotent) instead of holding a query worker or a
                // projection thread for the OS's TCP timeout.
                let io_timeout =
                    Duration::from_millis(env_number("TC_ORDER_MYSQL_IO_TIMEOUT_MS", 30_000u64));
                let opts = mysql::OptsBuilder::from_opts(opts)
                    .init(vec![
                        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                    ])
                    .tcp_connect_timeout(Some(Duration::from_millis(env_number(
                        "TC_ORDER_MYSQL_CONNECT_TIMEOUT_MS",
                        5_000u64,
                    ))))
                    .read_timeout(Some(io_timeout))
                    .write_timeout(Some(io_timeout));
                let pool =
                    Pool::new(opts).map_err(|e| mysql::Error::from(io_shard_error(idx, &e)))?;
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
            if run_bootstrap && env_enabled("TC_MYSQL_BOOTSTRAP_SCHEMA", true) {
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

pub(crate) fn parse_owned_dbs(db_count: u32) -> Result<std::collections::HashSet<u32>, String> {
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
pub(crate) fn io_shard_error(shard: usize, error: &dyn std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!("shard {shard}: {error}"))
}

pub(crate) fn role_needs_mysql(db_consumers: usize, execution_consumers: usize) -> bool {
    db_consumers > 0 || execution_consumers > 0
}
