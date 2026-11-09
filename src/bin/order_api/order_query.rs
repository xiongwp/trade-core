//! Read API over the projection: `/orders/{id}`, `/orders?user=`, `/fills`.

use super::*;

pub(crate) const QUERY_DEFAULT_LIMIT: usize = 100;
pub(crate) const QUERY_MAX_LIMIT: usize = 1_000;
/// `status=open`: every non-terminal projection status.
pub(crate) const OPEN_STATUSES_SQL: &str =
    "'PENDING','OPEN','PARTIAL','UNTRIGGERED','MODIFY_PENDING'";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OrderQuery {
    /// `GET /orders/{order_id}`
    Order(u64),
    /// `GET /orders?user=U[&status=open|all][&limit=N]`
    UserOrders {
        user: u64,
        open_only: bool,
        limit: usize,
    },
    /// `GET /fills?order_id=X[&limit=N]`
    OrderFills { order_id: u64, limit: usize },
    /// `GET /fills?user=U[&limit=N]`
    UserFills { user: u64, limit: usize },
}

/// `None` when the request is not an order query, so it falls through to the
/// other routes (`POST /orders` among them).
pub(crate) fn parse_order_query(
    method: &str,
    path: &str,
    query: &str,
) -> Option<Result<OrderQuery, String>> {
    if method != "GET" {
        return None;
    }
    if let Some(id) = path.strip_prefix("/orders/") {
        return Some(
            id.parse()
                .map(OrderQuery::Order)
                .map_err(|_| "invalid order_id".to_string()),
        );
    }
    let parsed = match path {
        "/orders" => (|| {
            let open_only = match query_param(query, "status").unwrap_or("all") {
                "open" => true,
                "all" => false,
                _ => return Err("status must be open or all".to_string()),
            };
            Ok(OrderQuery::UserOrders {
                user: required(query, "user")?,
                open_only,
                limit: query_limit(query)?,
            })
        })(),
        "/fills" => (|| {
            let limit = query_limit(query)?;
            match (query_param(query, "order_id"), query_param(query, "user")) {
                (Some(_), None) => Ok(OrderQuery::OrderFills {
                    order_id: required(query, "order_id")?,
                    limit,
                }),
                (None, Some(_)) => Ok(OrderQuery::UserFills {
                    user: required(query, "user")?,
                    limit,
                }),
                _ => Err("pass exactly one of order_id or user".to_string()),
            }
        })(),
        _ => return None,
    };
    Some(parsed)
}

pub(crate) fn query_limit(query: &str) -> Result<usize, String> {
    match query_param(query, "limit") {
        None => Ok(QUERY_DEFAULT_LIMIT),
        Some(value) => match value.parse::<usize>() {
            Ok(limit) if (1..=QUERY_MAX_LIMIT).contains(&limit) => Ok(limit),
            _ => Err(format!("limit must be 1..={QUERY_MAX_LIMIT}")),
        },
    }
}

/// Columns of an order row, in [`OrderRow`] order. Times are epoch ms (the
/// projection's TIMESTAMP columns have second precision).
pub(crate) const ORDER_COLUMNS: &str = "order_id,user_id,instrument,side,price,qty,filled_qty, \
CAST(filled_notional AS CHAR) filled_notional,status, \
CAST(UNIX_TIMESTAMP(created_at)*1000 AS UNSIGNED) created_ms, \
CAST(UNIX_TIMESTAMP(updated_at)*1000 AS UNSIGNED) updated_ms,row_seq";

pub(crate) type OrderRow = (
    u64,
    u64,
    u32,
    i8,
    u64,
    u64,
    u64,
    String,
    String,
    u64,
    u64,
    u64,
);

/// Columns of an `order_fills` row, in [`FillRow`] order.
pub(crate) const FILL_COLUMNS: &str =
    "raft_group,raft_index,report_ordinal,order_id,user_id,instrument, \
side,price,qty,fee,fill_role,CAST(UNIX_TIMESTAMP(created_at)*1000 AS UNSIGNED) created_ms";

pub(crate) type FillRow = (u32, u64, u32, u64, u64, u32, i8, u64, u64, u64, u8, u64);

/// Newest first; matches `idx_fill_order` / `idx_fill_user_created` plus the
/// primary key InnoDB appends to them, so `LIMIT` stops early with no filesort.
pub(crate) const FILL_ORDER_BY: &str =
    "created_at DESC,raft_group DESC,raft_index DESC,report_ordinal DESC,order_id DESC";

pub(crate) fn side_name(side: i8) -> &'static str {
    if side == 0 {
        "BUY"
    } else {
        "SELL"
    }
}

/// `filled_notional / filled_qty` as a JSON number, truncated to 8 decimals.
pub(crate) fn average_price(notional: &str, filled_qty: u64) -> String {
    let notional: u128 = notional.parse().unwrap_or(0);
    if filled_qty == 0 {
        return "0".into();
    }
    let qty = filled_qty as u128;
    let whole = notional / qty;
    // remainder < qty <= u64::MAX, so the scaled remainder fits in u128.
    let frac = (notional % qty) * 100_000_000 / qty;
    if frac == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{}", format!("{frac:08}").trim_end_matches('0'))
    }
}

pub(crate) fn order_json(row: &OrderRow) -> String {
    let (
        order_id,
        user,
        instrument,
        side,
        price,
        qty,
        filled,
        notional,
        status,
        created,
        updated,
        _,
    ) = row;
    format!(
        "{{\"order_id\":{order_id},\"user_id\":{user},\"instrument\":{instrument},\"side\":\"{}\",\"price\":{price},\"qty\":{qty},\"filled_qty\":{filled},\"avg_fill_price\":{},\"status\":\"{status}\",\"created_at\":{created},\"updated_at\":{updated}}}",
        side_name(*side),
        average_price(notional, *filled),
    )
}

pub(crate) fn fill_json(row: &FillRow) -> String {
    let (group, index, ordinal, order_id, user, instrument, side, price, qty, fee, role, created) =
        row;
    format!(
        "{{\"order_id\":{order_id},\"user_id\":{user},\"instrument\":{instrument},\"side\":\"{}\",\"price\":{price},\"qty\":{qty},\"fee\":{fee},\"role\":\"{}\",\"time\":{created},\"raft_group\":{group},\"raft_index\":{index},\"report_ordinal\":{ordinal}}}",
        side_name(*side),
        if *role == FILL_ROLE_MAKER { "maker" } else { "taker" },
    )
}

pub(crate) fn json_array<T>(rows: &[T], render: fn(&T) -> String) -> String {
    format!(
        "[{}]",
        rows.iter().map(render).collect::<Vec<_>>().join(",")
    )
}

pub(crate) fn query_conn(store: &OrderStore, db: u32) -> Result<mysql::PooledConn, String> {
    store
        .shards
        .get(db as usize)
        .and_then(Option::as_ref)
        .ok_or_else(|| format!("physical DB {db} is not open on this worker"))?
        .get_conn()
        .map_err(|error| error.to_string())
}

struct QueryWorkerLimiter {
    available: Mutex<usize>,
    wake: std::sync::Condvar,
    max: usize,
}

impl QueryWorkerLimiter {
    fn new(max: usize) -> Self {
        let max = max.max(1);
        Self {
            available: Mutex::new(max),
            wake: std::sync::Condvar::new(),
            max,
        }
    }

    /// Up to `wanted` worker slots, at least one; `None` after `wait`
    /// without a free slot. Bounded: when a database stalls and its queries
    /// hold every slot, later requests fail fast instead of parking their
    /// connection threads (which share the limit with order intake).
    fn acquire_up_to(&self, wanted: usize, wait: Duration) -> Option<QueryWorkerPermit<'_>> {
        let deadline = std::time::Instant::now() + wait;
        let mut available = self
            .available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *available == 0 {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return None;
            }
            available = self
                .wake
                .wait_timeout(available, left)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        let count = wanted.max(1).min(*available);
        *available -= count;
        Some(QueryWorkerPermit {
            limiter: self,
            count,
        })
    }
}

struct QueryWorkerPermit<'a> {
    limiter: &'a QueryWorkerLimiter,
    count: usize,
}

impl Drop for QueryWorkerPermit<'_> {
    fn drop(&mut self) {
        let mut available = self
            .limiter
            .available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *available = (*available + self.count).min(self.limiter.max);
        // One waiter per returned slot, not a thundering herd.
        for _ in 0..self.count {
            self.limiter.wake.notify_one();
        }
    }
}

struct ActiveQueryWorkers<'a> {
    metrics: &'a OrderPipelineMetrics,
    count: u64,
}

impl Drop for ActiveQueryWorkers<'_> {
    fn drop(&mut self) {
        self.metrics
            .query_active_workers
            .fetch_sub(self.count, AtomicOrdering::AcqRel);
    }
}

fn query_worker_limiter() -> &'static QueryWorkerLimiter {
    static LIMITER: std::sync::OnceLock<QueryWorkerLimiter> = std::sync::OnceLock::new();
    LIMITER.get_or_init(|| {
        QueryWorkerLimiter::new(env_number("TC_ORDER_QUERY_MAX_DB_WORKERS", 32usize))
    })
}

pub(crate) const QUERY_CAPACITY_EXHAUSTED: &str = "query capacity exhausted";

fn query_wait() -> Duration {
    static WAIT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *WAIT.get_or_init(|| Duration::from_millis(env_number("TC_ORDER_QUERY_WAIT_MS", 2_000u64)))
}

fn query_parallelism() -> usize {
    static PARALLELISM: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *PARALLELISM.get_or_init(|| env_number("TC_ORDER_QUERY_DB_PARALLELISM", 4usize).max(1))
}

/// Run one statement per physical DB with bounded process-wide parallelism and
/// concatenate the rows. Expanding from 10 to 100 databases therefore changes
/// work per request, but cannot create one OS thread per database per client.
pub(crate) fn fan_out<T: mysql::prelude::FromRow + Send>(
    store: &OrderStore,
    sql_for_db: impl Fn(u32) -> String + Sync,
) -> Result<Vec<T>, String> {
    let db_count = store.routing.db_count as u32;
    let wanted = query_parallelism().min(db_count as usize).max(1);
    store
        .metrics
        .query_waiting_requests
        .fetch_add(1, AtomicOrdering::AcqRel);
    let permit = query_worker_limiter().acquire_up_to(wanted, query_wait());
    store
        .metrics
        .query_waiting_requests
        .fetch_sub(1, AtomicOrdering::AcqRel);
    let Some(permit) = permit else {
        return Err(QUERY_CAPACITY_EXHAUSTED.into());
    };
    let worker_count = permit.count;
    store
        .metrics
        .query_active_workers
        .fetch_add(worker_count as u64, AtomicOrdering::AcqRel);
    let _active = ActiveQueryWorkers {
        metrics: &store.metrics,
        count: worker_count as u64,
    };
    let next_db = std::sync::atomic::AtomicU32::new(0);
    std::thread::scope(|scope| {
        let workers = (0..worker_count)
            .map(|_| {
                let next_db = &next_db;
                let sql_for_db = &sql_for_db;
                scope.spawn(move || {
                    let mut rows = Vec::new();
                    loop {
                        let db = next_db.fetch_add(1, AtomicOrdering::Relaxed);
                        if db >= db_count {
                            break;
                        }
                        let sql = sql_for_db(db);
                        rows.extend(
                            query_conn(store, db)?
                                .query::<T, _>(sql)
                                .map_err(|error| error.to_string())?,
                        );
                    }
                    Ok::<Vec<T>, String>(rows)
                })
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for worker in workers {
            rows.extend(
                worker
                    .join()
                    .map_err(|_| "query worker panicked".to_string())??,
            );
        }
        Ok(rows)
    })
}

/// Where one order's row and fills live: the `route_order_id` placement both
/// the command projection (asset_orders) and the execution projection
/// (order_fills, routed by target order id) write with.
pub(crate) struct OrderLocation {
    pub(crate) db: u32,
    pub(crate) orders_table: String,
    pub(crate) fills_table: String,
}

pub(crate) fn order_location(routing: &sharding::RouteConfig, order_id: u64) -> OrderLocation {
    let route = routing.route_order_id(order_id);
    OrderLocation {
        db: route.db,
        orders_table: format!("{}.{}", route.db_name(), route.table_name()),
        fills_table: format!("{}.order_fills", route.db_name()),
    }
}

/// `Ok(None)` means 404 (only `GET /orders/{id}` has one).
pub(crate) fn serve_order_query(
    store: &OrderStore,
    request: &OrderQuery,
) -> Result<Option<String>, String> {
    match *request {
        OrderQuery::Order(order_id) => {
            let at = order_location(&store.routing, order_id);
            let row: Option<OrderRow> = query_conn(store, at.db)?
                .exec_first(
                    format!(
                        "SELECT {ORDER_COLUMNS} FROM {} WHERE order_id=?",
                        at.orders_table
                    ),
                    (order_id,),
                )
                .map_err(|error| error.to_string())?;
            Ok(row.as_ref().map(order_json))
        }
        OrderQuery::UserOrders {
            user,
            open_only,
            limit,
        } => {
            // Orders are sharded by order id, so one user's orders sit in every
            // asset_orders table of every DB. Fan-out cost per call: db_count
            // statements (one per DB, in parallel), each a UNION ALL of
            // tables_per_db index range scans on idx_user_created — 10 x 100 =
            // 1000 index dives with the default layout. Each table returns at
            // most `limit` rows, each DB re-sorts and keeps `limit`, and the
            // final merge keeps `limit`: bounded, but meant for account-history
            // reads, not high-rate polling (that is the push stream's job).
            // `status=open` filters while walking the index, so a user with a
            // long closed history pays for skipping it.
            // Per-table order (created_at, row_seq) is the index order, and the
            // merge key extends it, so per-table top-N merges to the true top-N.
            let status = if open_only {
                format!(" AND status IN ({OPEN_STATUSES_SQL})")
            } else {
                String::new()
            };
            let tables_per_db = store.routing.tables_per_db;
            let mut rows: Vec<OrderRow> = fan_out(store, |db| {
                let parts = (0..tables_per_db)
                    .map(|table| {
                        format!(
                            "(SELECT {ORDER_COLUMNS} FROM order_db_{db}.asset_orders_{table:03} WHERE user_id={user}{status} ORDER BY created_at DESC,row_seq DESC LIMIT {limit})"
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" UNION ALL ");
                format!("SELECT * FROM ({parts}) u ORDER BY created_ms DESC,row_seq DESC,order_id DESC LIMIT {limit}")
            })?;
            rows.sort_unstable_by(|a, b| (b.9, b.11, b.0).cmp(&(a.9, a.11, a.0)));
            rows.truncate(limit);
            Ok(Some(json_array(&rows, order_json)))
        }
        OrderQuery::OrderFills { order_id, limit } => {
            let at = order_location(&store.routing, order_id);
            let rows: Vec<FillRow> = query_conn(store, at.db)?
                .exec(
                    format!(
                        "SELECT {FILL_COLUMNS} FROM {} WHERE order_id=? ORDER BY {FILL_ORDER_BY} LIMIT {limit}",
                        at.fills_table
                    ),
                    (order_id,),
                )
                .map_err(|error| error.to_string())?;
            Ok(Some(json_array(&rows, fill_json)))
        }
        OrderQuery::UserFills { user, limit } => {
            // One order_fills table per DB: db_count index range scans on
            // idx_user_created, `limit` rows each, merged here.
            let mut rows: Vec<FillRow> = fan_out(store, |db| {
                format!(
                    "SELECT {FILL_COLUMNS} FROM order_db_{db}.order_fills WHERE user_id={user} ORDER BY {FILL_ORDER_BY} LIMIT {limit}"
                )
            })?;
            rows.sort_unstable_by(|a, b| {
                (b.11, b.0, b.1, b.2, b.3).cmp(&(a.11, a.0, a.1, a.2, a.3))
            });
            rows.truncate(limit);
            Ok(Some(json_array(&rows, fill_json)))
        }
    }
}

/// HTTP status and body for an order query.
pub(crate) fn order_query_response(
    store: Option<&OrderStore>,
    request: Result<OrderQuery, String>,
) -> (&'static str, String) {
    let request = match request {
        Ok(request) => request,
        Err(error) => return ("400 Bad Request", format!("{{\"error\":\"{error}\"}}")),
    };
    let Some(store) = store else {
        return (
            "503 Service Unavailable",
            "{\"error\":\"order queries are disabled on this worker (TC_ORDER_QUERY_ENABLED)\"}"
                .into(),
        );
    };
    match serve_order_query(store, &request) {
        Ok(Some(body)) => ("200 OK", body),
        Ok(None) => ("404 Not Found", "{\"error\":\"order not found\"}".into()),
        Err(error) if error == QUERY_CAPACITY_EXHAUSTED => (
            "503 Service Unavailable",
            format!("{{\"error\":\"{QUERY_CAPACITY_EXHAUSTED}\"}}"),
        ),
        Err(error) => {
            log_warn!(
                "order-api",
                "event=order_query_failed request={request:?} error={error}"
            );
            (
                "503 Service Unavailable",
                "{\"error\":\"order query failed\"}".into(),
            )
        }
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn query_worker_limiter_never_grants_more_than_its_global_budget() {
        let limiter = QueryWorkerLimiter::new(3);
        let wait = Duration::from_millis(10);
        let first = limiter.acquire_up_to(2, wait).unwrap();
        assert_eq!(first.count, 2);
        let second = limiter.acquire_up_to(2, wait).unwrap();
        assert_eq!(second.count, 1);
        assert!(
            limiter.acquire_up_to(1, wait).is_none(),
            "a full limiter fails fast instead of parking the caller"
        );
        drop(first);
        drop(second);
        assert_eq!(
            *limiter.available.lock().unwrap(),
            3,
            "all worker slots return through the RAII permits"
        );
    }
}
