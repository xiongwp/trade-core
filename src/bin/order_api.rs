//! Persistent order system: MySQL 10 databases x 100 asset-sharded tables and
//! a transactional outbox that forwards durable commands to the matcher.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
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
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.category_sequences (category_id INT UNSIGNED NOT NULL PRIMARY KEY, next_seq BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.command_locations_by_category (category_id INT UNSIGNED NOT NULL, category_seq BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(category_id, category_seq), KEY idx_status_category (status, category_id, category_seq)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.order_outbox (order_id BIGINT UNSIGNED PRIMARY KEY, frame BLOB NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB"))?;
        for table in 0..TABLES_PER_DB {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.asset_orders_{table:03} (row_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id BIGINT UNSIGNED NOT NULL UNIQUE, category_id INT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_category_row (category_id,row_seq), KEY idx_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
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
            .unwrap_or_else(|_| "orders-g0,orders-g1,orders-g2,orders-g3".into())
            .split(',')
            .map(str::trim)
            .filter(|topic| !topic.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let partitions = env_number("TC_ORDER_KAFKA_PARTITIONS_PER_TOPIC", 64u32);
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

fn reserve_command(
    tx: &mut mysql::Transaction<'_>,
    db: &str,
    route: sharding::ShardRoute,
    category_id: u32,
    command_id: u64,
    user: u64,
) -> mysql::Result<CategorySequence> {
    // One upsert takes the row's exclusive lock immediately. INSERT IGNORE
    // followed by UPDATE caused lock-upgrade deadlocks under hot-category load.
    tx.exec_drop(
        format!(
            "INSERT INTO {db}.category_sequences (category_id,next_seq) VALUES (:category,2)
         ON DUPLICATE KEY UPDATE next_seq=next_seq+1",
        ),
        params! {"category" => category_id},
    )?;
    let category_seq: Option<u64> = tx.exec_first(
        format!("SELECT next_seq-1 FROM {db}.category_sequences WHERE category_id=:category"),
        params! {"category" => category_id},
    )?;
    let category_seq = category_seq.expect("sequence upsert must leave a row");
    tx.exec_drop(
        format!("INSERT INTO {db}.command_locations_by_category (category_id,category_seq,command_id,user_id,shard_table,status) VALUES (:category,:seq,:id,:user,:table,'PENDING')"),
        params! {"category" => category_id, "seq" => category_seq, "id" => command_id, "user" => user, "table" => route.table},
    )?;
    Ok(CategorySequence {
        category_id,
        category_seq,
    })
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
    let category_id = sharding::asset_category(order.instrument, store.category_size);
    let route = sharding::route_category(category_id);
    let db = route.db_name();
    let table = route.table_name();
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_new(order, &mut frame);
    let mut conn = store.shard(route.db).get_conn()?;
    let mut tx = conn.start_transaction(TxOpts::default())?;
    let seq = reserve_command(&mut tx, &db, route, category_id, order.id.0, order.user)?;
    tx.exec_drop(
        format!("INSERT INTO {db}.{table} (order_id,category_id,user_id,instrument,side,price,qty,status) VALUES (:id,:category,:user,:instrument,:side,:price,:qty,'PENDING')"),
        params! {"id" => order.id.0, "category" => category_id, "user" => order.user, "instrument" => order.instrument.0, "side" => if order.side == Side::Buy { 0 } else { 1 }, "price" => order.price, "qty" => order.quantity},
    )?;
    tx.exec_drop(
        format!(
            "INSERT INTO {db}.order_outbox (order_id,frame,status) VALUES (:id,:frame,'PENDING')"
        ),
        params! {"id" => order.id.0, "frame" => frame.to_vec()},
    )?;
    enqueue_command(&mut tx, route, seq, order.id.0, order.user, &db, &frame)?;
    tx.commit()?;
    Ok(seq)
}

fn persist_cancel(
    store: &OrderStore,
    instrument: InstrumentId,
    order_id: OrderId,
    cmd_id: u64,
    user: u64,
) -> Result<CategorySequence, mysql::Error> {
    let category_id = sharding::asset_category(instrument, store.category_size);
    let route = sharding::route_category(category_id);
    let db = route.db_name();
    let table = route.table_name();
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_cancel(instrument, order_id, cmd_id, &mut frame);
    let mut conn = store.shard(route.db).get_conn()?;
    let mut tx = conn.start_transaction(TxOpts::default())?;
    let seq = reserve_command(&mut tx, &db, route, category_id, cmd_id, user)?;
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
    tx.commit()?;
    Ok(seq)
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

        let mut locations = Vec::with_capacity(records.len() * 6);
        let mut order_outbox = Vec::with_capacity(records.len() * 2);
        let mut orders_by_table: BTreeMap<u32, Vec<Value>> = BTreeMap::new();
        let mut commands_by_table: BTreeMap<u32, Vec<Value>> = BTreeMap::new();
        let mut cancels = Vec::new();

        for record in records {
            let route = sharding::route_category(record.category_id);
            let seq = CategorySequence {
                category_id: record.category_id,
                category_seq: (record.offset as u64).saturating_add(1),
            };
            let command = wire::WireView::parse(&record.frame)
                .and_then(|view| view.to_command())
                .ok_or_else(|| "invalid Kafka command frame".to_string())?;
            let id = command_id(&command);
            locations.extend([
                Value::from(seq.category_id),
                Value::from(seq.category_seq),
                Value::from(id),
                Value::from(record.user),
                Value::from(route.table),
                Value::from("PENDING"),
            ]);
            order_outbox.extend([
                Value::from(id),
                Value::from(record.frame.to_vec()),
                Value::from("PENDING"),
            ]);
            commands_by_table.entry(route.table).or_default().extend([
                Value::from(seq.category_id),
                Value::from(seq.category_seq),
                Value::from(id),
                Value::from(record.user),
                Value::from(record.frame.to_vec()),
                Value::from("PENDING"),
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
            &format!("INSERT IGNORE INTO {db}.command_locations_by_category (category_id,category_seq,command_id,user_id,shard_table,status)"),
            6,
            locations,
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
        exec_multi_insert(
            &mut tx,
            &format!("INSERT IGNORE INTO {db}.order_outbox (order_id,frame,status)"),
            3,
            order_outbox,
        )
        .map_err(|error| error.to_string())?;
        for (table, values) in commands_by_table {
            exec_multi_insert(
                &mut tx,
                &format!("INSERT IGNORE INTO {db}.command_outbox_cat_{table:03} (category_id,category_seq,command_id,user_id,frame,status)"),
                6,
                values,
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

fn run_kafka_consumer(store: OrderStore, kafka: KafkaIngress, worker: usize) {
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
        "[order-kafka-{worker}] consuming {} queue groups",
        topics.len()
    );

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
            .and_then(|()| commit_kafka_batch(&consumer, &batch))
        {
            Ok(()) => {}
            Err(error) => {
                eprintln!("[order-kafka-{worker}] batch retained for retry: {error}");
                for record in &batch {
                    let _ = consumer.seek(
                        &record.topic,
                        record.partition,
                        Offset::Offset(record.offset),
                        Duration::from_secs(1),
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
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
    let shard_db = (worker_id % DB_COUNT as usize) as u32;
    let lane = worker_id / DB_COUNT as usize;
    let lanes = workers.div_ceil(DB_COUNT as usize);
    let db = format!("order_db_{shard_db}");
    let mut shard = store.shard(shard_db).get_conn()?;
    let row: Option<(u32, u64, u64, u64, u32)> = shard.exec_first(
        format!(
            "SELECT category_id,category_seq,command_id,user_id,shard_table
         FROM {db}.command_locations_by_category loc
         WHERE status='PENDING'
           AND MOD(FLOOR(category_id / 10),:lanes)=:lane
           AND NOT EXISTS (
             SELECT 1
             FROM {db}.command_locations_by_category prev
             WHERE prev.category_id=loc.category_id
               AND prev.category_seq < loc.category_seq
               AND prev.status='PENDING'
           )
         ORDER BY category_id,category_seq
        LIMIT 1"
        ),
        params! {"lanes" => lanes as u64, "lane" => lane as u64},
    )?;
    let Some((category_id, category_seq, command_id, user_id, shard_table)) = row else {
        return Ok(None);
    };
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
    shard_tx.exec_drop(
        format!("UPDATE {db}.command_locations_by_category SET status='SENT' WHERE category_id=:category AND category_seq=:seq AND status='PENDING'"),
        params! {"category" => command.category_id, "seq" => command.category_seq},
    )?;
    shard_tx.commit()
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

fn dispatcher_workers() -> usize {
    std::env::var("TC_ORDER_DISPATCH_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .map(|workers| workers.div_ceil(DB_COUNT as usize) * DB_COUNT as usize)
        .unwrap_or(20)
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

fn handle(
    mut stream: TcpStream,
    store: Arc<OrderStore>,
    kafka: Option<Arc<KafkaIngress>>,
    token: &str,
) {
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
            if let Some(kafka) = &kafka {
                let mut frame = [0u8; wire::MSG_LEN];
                wire::encode_new(&order, &mut frame);
                let category = sharding::asset_category(order.instrument, store.category_size);
                kafka.publish(category, order.user, &frame)
            } else {
                persist(&store, &order).map_err(|error| error.to_string())
            }
        }),
        ("POST", "/cancels") => cancel_from_query(query).and_then(|(i, o, c, u)| {
            if let Some(kafka) = &kafka {
                let mut frame = [0u8; wire::MSG_LEN];
                wire::encode_cancel(i, o, c, &mut frame);
                let category = sharding::asset_category(i, store.category_size);
                kafka.publish(category, u, &frame)
            } else {
                persist_cancel(&store, i, o, c, u).map_err(|error| error.to_string())
            }
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
    let matchers = parse_matchers();
    let token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let store = open_when_ready(&shard_urls);
    let kafka = KafkaIngress::from_env().expect("configure Kafka order ingress");
    let workers = dispatcher_workers();
    eprintln!(
        "[order-api] category_size={} dispatch_workers={workers} kafka={}",
        store.category_size,
        kafka.is_some()
    );
    if let Some(kafka) = &kafka {
        for worker in 0..kafka.consumers {
            let consumer_store = store.clone();
            let consumer_kafka = kafka.clone();
            std::thread::Builder::new()
                .name(format!("order-kafka-consumer-{worker}"))
                .spawn(move || run_kafka_consumer(consumer_store, consumer_kafka, worker))
                .expect("spawn Kafka order consumer");
        }
    }
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
    let shared_kafka = kafka.map(Arc::new);
    for stream in listener.incoming().flatten() {
        let store = shared_store.clone();
        let kafka = shared_kafka.clone();
        let token = token.clone();
        std::thread::spawn(move || handle(stream, store, kafka, &token));
    }
}
