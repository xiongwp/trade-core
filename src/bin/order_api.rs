//! Persistent order system: MySQL 10 databases x 100 user-sharded tables and
//! a transactional outbox that forwards durable commands to the matcher.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{params, Pool, TxOpts};
use trade_core::order::Order;
use trade_core::sharding::{self, DB_COUNT, DEFAULT_ASSET_CATEGORY_SIZE, TABLES_PER_DB};
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire;

#[derive(Clone)]
struct MatcherTarget {
    order_addr: String,
    metrics_addr: Option<String>,
}

#[derive(Clone)]
struct OrderStore {
    control: Pool,
    shards: Arc<Vec<Pool>>,
    category_size: u32,
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
    let mut conn = store.control.get_conn()?;
    conn.query_drop("CREATE DATABASE IF NOT EXISTS order_control")?;
    conn.query_drop("CREATE TABLE IF NOT EXISTS order_control.category_sequences (category_id INT UNSIGNED NOT NULL PRIMARY KEY, next_seq BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB")?;
    conn.query_drop("CREATE TABLE IF NOT EXISTS order_control.command_locations_by_category (category_id INT UNSIGNED NOT NULL, category_seq BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, shard_db INT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(category_id, category_seq), KEY idx_status_category (status, category_id, category_seq)) ENGINE=InnoDB")?;
    for db in 0..DB_COUNT {
        let mut conn = store.shard(db as u32).get_conn()?;
        let db_name = format!("order_db_{db}");
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db_name}"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.order_outbox (order_id BIGINT UNSIGNED PRIMARY KEY, frame BLOB NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB"))?;
        for table in 0..TABLES_PER_DB {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.orders_{table:03} (order_id BIGINT UNSIGNED PRIMARY KEY, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB"))?;
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.command_outbox_cat_{table:03} (category_id INT UNSIGNED NOT NULL, category_seq BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, frame BLOB NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(category_id, category_seq), KEY idx_status_category (status, category_id, category_seq)) ENGINE=InnoDB"))?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct CategorySequence {
    category_id: u32,
    category_seq: u64,
}

fn reserve_command(
    control: &Pool,
    route: sharding::ShardRoute,
    category_id: u32,
    command_id: u64,
    user: u64,
) -> mysql::Result<CategorySequence> {
    let mut conn = control.get_conn()?;
    let mut tx = conn.start_transaction(TxOpts::default())?;
    tx.exec_drop(
        "INSERT IGNORE INTO order_control.category_sequences (category_id,next_seq) VALUES (:category,1)",
        params! {"category" => category_id},
    )?;
    tx.exec_drop(
        "UPDATE order_control.category_sequences SET next_seq=LAST_INSERT_ID(next_seq+1) WHERE category_id=:category",
        params! {"category" => category_id},
    )?;
    let next: Option<u64> = tx.query_first("SELECT LAST_INSERT_ID()")?;
    let category_seq = next.expect("sequence update must return LAST_INSERT_ID") - 1;
    tx.exec_drop(
        "INSERT INTO order_control.command_locations_by_category (category_id,category_seq,command_id,user_id,shard_db,shard_table,status) VALUES (:category,:seq,:id,:user,:db,:table,'RESERVED')",
        params! {"category" => category_id, "seq" => category_seq, "id" => command_id, "user" => user, "db" => route.db, "table" => route.table},
    )?;
    tx.commit()?;
    Ok(CategorySequence {
        category_id,
        category_seq,
    })
}

fn promote_reserved(control: &Pool, seq: CategorySequence) -> mysql::Result<()> {
    let mut conn = control.get_conn()?;
    conn.exec_drop(
        "UPDATE order_control.command_locations_by_category SET status='PENDING' WHERE category_id=:category AND category_seq=:seq AND status='RESERVED'",
        params! {"category" => seq.category_id, "seq" => seq.category_seq},
    )
}

fn abort_reserved(control: &Pool, seq: CategorySequence) -> mysql::Result<()> {
    let mut conn = control.get_conn()?;
    conn.exec_drop(
        "UPDATE order_control.command_locations_by_category SET status='ABORTED' WHERE category_id=:category AND category_seq=:seq AND status='RESERVED'",
        params! {"category" => seq.category_id, "seq" => seq.category_seq},
    )
}

fn enqueue_command(
    shard_tx: &mut mysql::Transaction<'_>,
    route: sharding::ShardRoute,
    seq: CategorySequence,
    command_id: u64,
    user: u64,
    db: &str,
    frame: &[u8; wire::MSG_LEN],
) -> mysql::Result<()> {
    shard_tx.exec_drop(
        format!("INSERT INTO {db}.command_outbox_cat_{:03} (category_id,category_seq,command_id,user_id,frame,status) VALUES (:category,:seq,:id,:user,:frame,'PENDING')", route.table),
        params! {"category" => seq.category_id, "seq" => seq.category_seq, "id" => command_id, "user" => user, "frame" => frame.to_vec()},
    )
}

fn persist(store: &OrderStore, order: &Order) -> Result<CategorySequence, mysql::Error> {
    let route = sharding::route(order.user);
    let db = route.db_name();
    let table = route.table_name();
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_new(order, &mut frame);
    let category_id = sharding::asset_category(order.instrument, store.category_size);
    let seq = reserve_command(&store.control, route, category_id, order.id.0, order.user)?;
    let mut conn = store.shard(route.db).get_conn()?;
    let mut tx = conn.start_transaction(TxOpts::default())?;
    let result = (|| -> mysql::Result<()> {
        tx.exec_drop(
        format!("INSERT INTO {db}.{table} (order_id,user_id,instrument,side,price,qty,status) VALUES (:id,:user,:instrument,:side,:price,:qty,'PENDING')"),
        params! {"id" => order.id.0, "user" => order.user, "instrument" => order.instrument.0, "side" => if order.side == Side::Buy { 0 } else { 1 }, "price" => order.price, "qty" => order.quantity},
    )?;
        tx.exec_drop(
            format!(
                "INSERT INTO {db}.order_outbox (order_id,frame,status) VALUES (:id,:frame,'PENDING')"
            ),
            params! {"id" => order.id.0, "frame" => frame.to_vec()},
        )?;
        enqueue_command(&mut tx, route, seq, order.id.0, order.user, &db, &frame)?;
        tx.commit()
    })();
    if let Err(error) = result {
        let _ = abort_reserved(&store.control, seq);
        return Err(error);
    }
    promote_reserved(&store.control, seq)?;
    Ok(seq)
}

fn persist_cancel(
    store: &OrderStore,
    instrument: InstrumentId,
    order_id: OrderId,
    cmd_id: u64,
    user: u64,
) -> Result<CategorySequence, mysql::Error> {
    let route = sharding::route(user);
    let db = route.db_name();
    let table = route.table_name();
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_cancel(instrument, order_id, cmd_id, &mut frame);
    let category_id = sharding::asset_category(instrument, store.category_size);
    let seq = reserve_command(&store.control, route, category_id, cmd_id, user)?;
    let mut conn = store.shard(route.db).get_conn()?;
    let mut tx = conn.start_transaction(TxOpts::default())?;
    let result = (|| -> mysql::Result<()> {
        tx.exec_drop(
            format!(
                "UPDATE {db}.{table} SET status='CANCEL_PENDING' WHERE order_id=:id AND user_id=:user"
            ),
            params! {"id" => order_id.0, "user" => user},
        )?;
        tx.exec_drop(
            format!(
                "INSERT INTO {db}.order_outbox (order_id,frame,status) VALUES (:id,:frame,'PENDING')"
            ),
            params! {"id" => cmd_id, "frame" => frame.to_vec()},
        )?;
        enqueue_command(&mut tx, route, seq, cmd_id, user, &db, &frame)?;
        tx.commit()
    })();
    if let Err(error) = result {
        let _ = abort_reserved(&store.control, seq);
        return Err(error);
    }
    promote_reserved(&store.control, seq)?;
    Ok(seq)
}

#[derive(Debug)]
struct PendingCommand {
    category_id: u32,
    category_seq: u64,
    command_id: u64,
    user_id: u64,
    shard_db: u32,
    shard_table: u32,
    frame: Vec<u8>,
}

fn next_pending(
    store: &OrderStore,
    worker_id: usize,
    workers: usize,
) -> mysql::Result<Option<PendingCommand>> {
    let mut control = store.control.get_conn()?;
    let row: Option<(u32, u64, u64, u64, u32, u32)> = control.exec_first(
        "SELECT category_id,category_seq,command_id,user_id,shard_db,shard_table
         FROM order_control.command_locations_by_category loc
         WHERE status='PENDING'
           AND MOD(category_id,:workers)=:worker
           AND NOT EXISTS (
             SELECT 1
             FROM order_control.command_locations_by_category prev
             WHERE prev.category_id=loc.category_id
               AND prev.category_seq < loc.category_seq
               AND prev.status IN ('RESERVED','PENDING')
           )
         ORDER BY category_id,category_seq
         LIMIT 1",
        params! {"workers" => workers as u64, "worker" => worker_id as u64},
    )?;
    let Some((category_id, category_seq, command_id, user_id, shard_db, shard_table)) = row else {
        return Ok(None);
    };
    let db = format!("order_db_{shard_db}");
    let mut shard = store.shard(shard_db).get_conn()?;
    let frame: Option<Vec<u8>> = shard.exec_first(
        format!("SELECT frame FROM {db}.command_outbox_cat_{shard_table:03} WHERE category_id=:category AND category_seq=:seq AND status='PENDING'"),
        params! {"category" => category_id, "seq" => category_seq},
    )?;
    Ok(frame.map(|frame| PendingCommand {
        category_id,
        category_seq,
        command_id,
        user_id,
        shard_db,
        shard_table,
        frame,
    }))
}

fn mark_sent(store: &OrderStore, command: &PendingCommand) -> mysql::Result<()> {
    let db = format!("order_db_{}", command.shard_db);
    let mut shard = store.shard(command.shard_db).get_conn()?;
    let mut shard_tx = shard.start_transaction(TxOpts::default())?;
    shard_tx.exec_drop(
        format!(
            "UPDATE {db}.command_outbox_cat_{:03} SET status='SENT' WHERE category_id=:category AND category_seq=:seq AND status='PENDING'",
            command.shard_table
        ),
        params! {"category" => command.category_id, "seq" => command.category_seq},
    )?;
    shard_tx.exec_drop(
        format!(
            "UPDATE {db}.order_outbox SET status='SENT' WHERE order_id=:id AND status='PENDING'",
        ),
        params! {"id" => command.command_id},
    )?;
    shard_tx.commit()?;
    let mut control = store.control.get_conn()?;
    control.exec_drop(
        "UPDATE order_control.command_locations_by_category SET status='SENT' WHERE category_id=:category AND category_seq=:seq AND status='PENDING'",
        params! {"category" => command.category_id, "seq" => command.category_seq},
    )
}

/// This is the only path from order persistence to matching. It dispatches the
/// smallest durable sequence for each asset category and never skips it after a
/// retry. Different categories are partitioned across workers and run in
/// parallel, which is the scaling path for multi-million TPS.
fn dispatch_forever(
    store: OrderStore,
    matchers: Vec<MatcherTarget>,
    worker_id: usize,
    workers: usize,
) {
    loop {
        let command = match next_pending(&store, worker_id, workers) {
            Ok(Some(command)) => command,
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(error) => {
                eprintln!("[order-api] outbox read failed: {error}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        if command.frame.len() != wire::MSG_LEN {
            eprintln!(
                "[order-api] invalid frame at category={} seq={}",
                command.category_id, command.category_seq
            );
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        let delivered = forward(&matchers, &command.frame)
            .and_then(|()| mark_sent(&store, &command).map_err(std::io::Error::other));
        match delivered {
            Ok(()) => eprintln!(
                "[order-api] dispatched category={} seq={} command_id={} user_id={}",
                command.category_id, command.category_seq, command.command_id, command.user_id
            ),
            Err(error) => {
                eprintln!(
                    "[order-api] preserving category={} seq={} for retry: {error}",
                    command.category_id, command.category_seq
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
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

fn open_when_ready(control_url: &str, shard_urls: &[String]) -> OrderStore {
    loop {
        let opened = Pool::new(control_url).and_then(|control| {
            let mut shards = Vec::with_capacity(shard_urls.len());
            for url in shard_urls {
                shards.push(Pool::new(url.as_str())?);
            }
            let store = OrderStore {
                control,
                shards: Arc::new(shards),
                category_size: category_size(),
            };
            bootstrap(&store).map(|()| store)
        });
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
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ASSET_CATEGORY_SIZE)
}

fn dispatcher_workers() -> usize {
    std::env::var("TC_ORDER_DISPATCH_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(8)
}

fn is_leader(metrics_addr: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(metrics_addr) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
    let _ = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: raft\r\nConnection: close\r\n\r\n");
    let mut response = String::new();
    std::io::Read::read_to_string(&mut stream, &mut response).is_ok()
        && response.lines().any(|line| line.trim() == "tc_raft_role 2")
}

fn forward(targets: &[MatcherTarget], frame: &[u8]) -> std::io::Result<()> {
    for target in targets {
        if target
            .metrics_addr
            .as_deref()
            .is_some_and(|metrics| !is_leader(metrics))
        {
            continue;
        }
        let Ok(mut stream) = TcpStream::connect(&target.order_addr) else {
            continue;
        };
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_millis(750)))?;
        if stream.write_all(frame).is_ok() && wait_for_gateway_report(&mut stream).is_ok() {
            return Ok(());
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "no reachable Raft leader",
    ))
}

fn wait_for_gateway_report(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut first = [0u8; 1];
    match stream.read(&mut first) {
        Ok(0) => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "gateway closed before report",
        )),
        Ok(_) => {
            let _ = stream.shutdown(Shutdown::Both);
            Ok(())
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) =>
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "gateway did not return report before timeout",
            ))
        }
        Err(error) => Err(error),
    }
}

fn parse_matchers() -> Vec<MatcherTarget> {
    if let Ok(value) = std::env::var("TC_RAFT_MATCHERS") {
        let targets = value
            .split(',')
            .filter_map(|item| {
                let (order_addr, metrics_addr) = item.split_once('@')?;
                Some(MatcherTarget {
                    order_addr: order_addr.to_string(),
                    metrics_addr: Some(metrics_addr.to_string()),
                })
            })
            .collect::<Vec<_>>();
        if !targets.is_empty() {
            return targets;
        }
    }
    vec![MatcherTarget {
        order_addr: std::env::var("TC_MATCHER_ADDR").unwrap_or_else(|_| "trade-core:9001".into()),
        metrics_addr: None,
    }]
}

fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let _ = stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes());
}

fn handle(mut stream: TcpStream, store: Arc<OrderStore>, token: &str) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut first = String::new();
    if reader.read_line(&mut first).is_err() {
        return;
    }
    let method = first.split_whitespace().next().unwrap_or("");
    let uri = first.split_whitespace().nth(1).unwrap_or("/");
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let mut authorized = false;
    let mut header = String::new();
    while reader.read_line(&mut header).is_ok() && header.trim() != "" {
        if let Some((key, value)) = header.split_once(':') {
            authorized |= key.eq_ignore_ascii_case("authorization")
                && value.trim() == format!("Bearer {token}");
        }
        header.clear();
    }
    if !authorized {
        respond(
            &mut stream,
            "401 Unauthorized",
            "{\"error\":\"unauthorized\"}",
        );
        return;
    }
    let persisted = match (method, path) {
        ("POST", "/orders") => order_from_query(query)
            .and_then(|order| persist(&store, &order).map_err(|e| e.to_string())),
        ("POST", "/cancels") => cancel_from_query(query)
            .and_then(|(i, o, c, u)| persist_cancel(&store, i, o, c, u).map_err(|e| e.to_string())),
        _ => {
            respond(&mut stream, "404 Not Found", "{\"error\":\"not found\"}");
            return;
        }
    };
    match persisted {
        Ok(seq) => respond(
            &mut stream,
            "202 Accepted",
            &format!(
                "{{\"accepted\":true,\"status\":\"PENDING\",\"category_id\":{},\"category_seq\":{}}}",
                seq.category_id, seq.category_seq
            ),
        ),
        Err(error) => respond(
            &mut stream,
            "400 Bad Request",
            &format!("{{\"error\":\"{error}\"}}"),
        ),
    }
}

fn main() {
    let control_url = std::env::var("TC_ORDER_MYSQL_CONTROL_URL")
        .or_else(|_| std::env::var("TC_ORDER_MYSQL_URL"))
        .expect("TC_ORDER_MYSQL_CONTROL_URL or TC_ORDER_MYSQL_URL");
    let shard_urls = parse_shard_urls();
    let matchers = parse_matchers();
    let token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let store = open_when_ready(&control_url, &shard_urls);
    let workers = dispatcher_workers();
    eprintln!(
        "[order-api] category_size={} dispatch_workers={workers}",
        store.category_size
    );
    for worker_id in 0..workers {
        let dispatch_store = store.clone();
        let worker_matchers = matchers.clone();
        std::thread::Builder::new()
            .name(format!("order-outbox-dispatcher-{worker_id}"))
            .spawn(move || dispatch_forever(dispatch_store, worker_matchers, worker_id, workers))
            .expect("spawn order outbox dispatcher");
    }
    let listener = TcpListener::bind("0.0.0.0:9200").expect("bind order API");
    let shared_store = Arc::new(store);
    for stream in listener.incoming().flatten() {
        let store = shared_store.clone();
        let token = token.clone();
        std::thread::spawn(move || handle(stream, store, &token));
    }
}
