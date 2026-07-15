//! Persistent order system: Kafka is the ordered source of truth; MySQL is a
//! 10 databases x 100 asset-sharded query projection, and committed Kafka
//! records are forwarded directly to the Raft-backed matcher.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
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

#[derive(Clone)]
struct MatcherTarget {
    order_addr: String,
    metrics_addr: Option<String>,
}

#[derive(Clone)]
struct OrderStore {
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
    for db in 0..DB_COUNT {
        let mut conn = store.shard(db as u32).get_conn()?;
        let db_name = format!("order_db_{db}");
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db_name}"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_commands (category_id INT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(category_id,kafka_offset), KEY idx_partition_offset (kafka_partition,kafka_offset)) ENGINE=InnoDB"))?;
        for table in 0..TABLES_PER_DB {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.asset_orders_{table:03} (row_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id BIGINT UNSIGNED NOT NULL UNIQUE, category_id INT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_category_row (category_id,row_seq), KEY idx_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
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
    consumers: usize,
    batch_size: usize,
    linger: Duration,
}

impl KafkaIngress {
    fn from_env() -> Result<Option<Self>, String> {
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
            consumers: env_number("TC_ORDER_KAFKA_CONSUMERS", 8usize).max(1),
            batch_size: env_number("TC_ORDER_BATCH_SIZE", 500usize).max(1),
            linger: Duration::from_millis(env_number("TC_ORDER_BATCH_LINGER_MS", 2u64)),
        }))
    }

    fn publish(
        &self,
        category_id: u32,
        user: u64,
        frame: &[u8; wire::MSG_LEN],
    ) -> Result<CategorySequence, String> {
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
        let offset = delivery.map_err(|(error, _)| error.to_string())?.offset;
        Ok(CategorySequence {
            category_id,
            category_seq: (offset as u64).saturating_add(1),
        })
    }
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

    for (shard_db, records) in by_db {
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
    }
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
) -> Result<(), String> {
    if matchers.is_empty() {
        return Err("no Raft groups configured".into());
    }
    let mut grouped = (0..matchers.len())
        .map(|_| Vec::<&KafkaRecord>::new())
        .collect::<Vec<_>>();
    for record in records {
        let group = sharding::raft_group_for_category(record.category_id, matchers.len());
        grouped[group].push(record);
    }

    // Independent groups commit concurrently. Records within one group remain
    // in Kafka poll order; a retry may repeat a committed prefix and is made
    // exact by the command-id deduplication in that Raft group.
    std::thread::scope(|scope| {
        let mut jobs = Vec::new();
        for (group, (matcher, records)) in matchers.iter_mut().zip(grouped).enumerate() {
            if records.is_empty() {
                continue;
            }
            jobs.push(scope.spawn(move || -> Result<(), String> {
                for record in records {
                    matcher
                        .send(&record.frame)
                        .map_err(|error| format!("Raft group {group} commit failed: {error}"))?;
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

fn run_kafka_consumer(
    store: OrderStore,
    kafka: KafkaIngress,
    matcher_groups: Vec<Vec<MatcherTarget>>,
    worker: usize,
) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        .set("group.id", "trade-order-persist-v1")
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
        "[order-kafka-{worker}] direct MySQL+Raft consumer for {} queue groups",
        topics.len()
    );
    let mut matchers = matcher_groups
        .into_iter()
        .map(MatcherConnection::new)
        .collect::<Vec<_>>();

    loop {
        let mut batch = Vec::with_capacity(kafka.batch_size);
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        match first {
            Ok(message) => {
                match decode_kafka_record(&message, &kafka.router, store.category_size) {
                    Ok(record) => batch.push(record),
                    Err(error) => eprintln!("[order-kafka-{worker}] rejected message: {error}"),
                }
            }
            Err(error) => {
                eprintln!("[order-kafka-{worker}] poll failed: {error}");
                continue;
            }
        }
        let deadline = std::time::Instant::now() + kafka.linger;
        while batch.len() < kafka.batch_size && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(message)) => {
                    match decode_kafka_record(&message, &kafka.router, store.category_size) {
                        Ok(record) => batch.push(record),
                        Err(error) => {
                            eprintln!("[order-kafka-{worker}] rejected message: {error}")
                        }
                    }
                }
                Some(Err(error)) => eprintln!("[order-kafka-{worker}] poll failed: {error}"),
                None => break,
            }
        }
        match persist_kafka_batch(&store, &batch)
            .and_then(|()| forward_kafka_batch(&mut matchers, &batch))
            .and_then(|()| commit_kafka_batch(&consumer, &batch))
        {
            Ok(()) => {}
            Err(error) => {
                eprintln!("[order-kafka-{worker}] batch retained for retry: {error}");
                rewind_kafka_batch(&consumer, &batch);
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
    std::io::Read::read_to_string(&mut stream, &mut response).is_ok()
        && response.lines().any(|line| line.trim() == "tc_raft_role 2")
}

struct MatcherConnection {
    targets: Vec<MatcherTarget>,
    stream: Option<TcpStream>,
}

impl MatcherConnection {
    fn new(targets: Vec<MatcherTarget>) -> Self {
        Self {
            targets,
            stream: None,
        }
    }

    fn connect(&mut self) -> std::io::Result<()> {
        for target in &self.targets {
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
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no reachable Raft leader",
        ))
    }

    fn send(&mut self, frame: &[u8]) -> std::io::Result<()> {
        for _ in 0..2 {
            if self.stream.is_none() {
                self.connect()?;
            }
            let stream = self.stream.as_mut().expect("connected matcher stream");
            let mut ack = [0u8; 9];
            if stream.write_all(frame).is_ok()
                && stream.read_exact(&mut ack).is_ok()
                && ack[0] == 1
                && u64::from_be_bytes(ack[1..].try_into().expect("Raft ACK index")) > 0
            {
                return Ok(());
            }
            self.stream = None;
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

fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let _ = stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes());
}

fn handle(mut stream: TcpStream, store: Arc<OrderStore>, kafka: Arc<KafkaIngress>, token: &str) {
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
    let shard_urls = parse_shard_urls();
    let matcher_groups = parse_matcher_groups();
    let token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let store = open_when_ready(&shard_urls);
    let kafka = KafkaIngress::from_env()
        .expect("configure Kafka order ingress")
        .expect("TC_ORDER_KAFKA_BROKERS is required");
    eprintln!(
        "[order-api] category_size={} raft_groups={} direct_consumers={} kafka=true",
        store.category_size,
        matcher_groups.len(),
        kafka.consumers
    );
    for worker in 0..kafka.consumers {
        let consumer_store = store.clone();
        let consumer_kafka = kafka.clone();
        let worker_matchers = matcher_groups.clone();
        std::thread::Builder::new()
            .name(format!("order-kafka-direct-{worker}"))
            .spawn(move || {
                run_kafka_consumer(consumer_store, consumer_kafka, worker_matchers, worker)
            })
            .expect("spawn direct Kafka order consumer");
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
