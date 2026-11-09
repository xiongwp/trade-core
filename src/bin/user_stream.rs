//! user-stream: real-time private push of order results to their owners
//! (the Binance "user data stream" role).
//!
//! It consumes the execution topic directly — no database — and forwards
//! every event to the WebSocket connections of the user who owns the order.
//! The owner travels in each execution event (v3 envelope,
//! [`wire::execution_event_target_user`]), so routing is a map lookup.
//!
//! What a user receives, one JSON text frame per event:
//!   COMMITTED  the order passed Raft quorum commit and the engine accepted it:
//!              its place in the sequence, and so its fill price, is now fixed
//!              (`"conditional":true` for a stop waiting for its trigger)
//!   TRADE      price, qty, fee, maker/taker role, counterparty order
//!   PARTIAL / FILLED / RESTING / CANCELLED / MODIFIED / TRIGGERED
//!   REJECTED   with the reason (`tick-size`, `post-only`, ...)
//! Every event carries `event_id` = raft group:index:ordinal:order (the
//! order id tells apart the two sides of a self-trade, which share the rest)
//! and `ts`, the event's Kafka timestamp in milliseconds. Delivery is
//! at-least-once, so clients dedupe on `event_id`.
//!
//! Reconnects resume: each user's recent events (TC_USER_STREAM_REPLAY_EVENTS,
//! default 512, and at most TC_USER_STREAM_REPLAY_SECS, default 300 s) are
//! kept, and `since=<ts>` on reconnect replays the ones at or after it before
//! the live stream continues, with no gap or reordering between the two. An
//! instance rebuilds that window from Kafka when it starts, so a restart of
//! the service does not lose it either. Beyond the window, reconcile through
//! the order query API.
//!
//! Scaling (TC_USER_STREAM_ROLE):
//!   all     (default) one instance reads the whole execution topic.
//!   router  a consumer group that re-publishes every event, unchanged and
//!           with its original timestamp, to the user-keyed topic
//!           TC_USER_STREAM_TOPIC (trade-user-events-v1): partition =
//!           hash(user) mod its partition count. Run as many as the
//!           execution topic needs; they split its partitions.
//!   pusher  shard TC_USER_STREAM_SHARD of TC_USER_STREAM_SHARDS: reads only
//!           the user-topic partitions p with p mod SHARDS == SHARD, so it
//!           holds and replays only its own users. A user connecting to the
//!           wrong shard is redirected (307) to TC_USER_STREAM_PEERS[owner]
//!           (comma-separated host:port, in shard order), or refused with 421
//!           naming the owner when no peers are configured.
//! The replay buffer is bounded per user and in total
//! (TC_USER_STREAM_REPLAY_MAX_TOTAL events, default 2,000,000).
//!
//!   GET /ws/user?user=U[&since=<ts ms>]   (Authorization: Bearer <TC_USER_STREAM_TOKEN>)
//!   GET /metrics
//!
//! Env: TC_USER_STREAM_ADDR (0.0.0.0:9300), TC_USER_STREAM_TOKEN (falls back
//! to TC_ORDER_API_TOKEN), TC_EXECUTION_KAFKA_BROKERS, TC_EXECUTION_KAFKA_TOPIC
//! (trade-executions-v1), TC_USER_STREAM_QUEUE (per-connection backlog before a
//! slow client is dropped, 10000), TC_USER_STREAM_MAX_CONNECTIONS_PER_USER (16).

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, Message as KafkaMessage};
use trade_core::websocket::{self, ClientEvent, EventReader};
use trade_core::{log_info, log_warn, wire};

/// What the writer thread of one connection sends: only complete frames, so
/// pushes and control replies never interleave on the socket.
enum Outgoing {
    Text(Arc<str>),
    Pong(Vec<u8>),
    Close(Vec<u8>),
}

#[derive(Default)]
struct Metrics {
    events_routed: AtomicU64,
    connections: AtomicU64,
    events_consumed: AtomicU64,
    events_pushed: AtomicU64,
    events_without_subscriber: AtomicU64,
    slow_clients_dropped: AtomicU64,
}

impl Metrics {
    fn render(&self) -> String {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        format!(
            "tc_user_stream_connections {}\ntc_user_stream_events_consumed_total {}\n\
             tc_user_stream_events_pushed_total {}\ntc_user_stream_events_without_subscriber_total {}\n\
             tc_user_stream_slow_clients_dropped_total {}\ntc_user_stream_events_routed_total {}\n",
            get(&self.connections),
            get(&self.events_consumed),
            get(&self.events_pushed),
            get(&self.events_without_subscriber),
            get(&self.slow_clients_dropped),
            get(&self.events_routed),
        )
    }
}

/// user -> that user's live connections (id, outbound queue), plus each
/// user's recent events for resuming after a reconnect.
#[derive(Default)]
struct Registry {
    users: RwLock<HashMap<u64, Vec<(u64, SyncSender<Outgoing>)>>>,
    next_id: AtomicU64,
    /// Lock order: `history`, then `users`. Appending an event and pushing it
    /// happen under `history`, and so do subscribing and queuing a replay:
    /// every event is therefore either replayed or pushed live, in order.
    history: Mutex<History>,
    replay_events: usize,
    replay_ms: i64,
    /// Events kept across all users: with millions of users the per-user
    /// limit alone does not bound memory. The oldest go first.
    replay_total: usize,
}

/// Kept events: per user, plus every event's insertion order so the oldest
/// can be evicted when the total limit is reached.
#[derive(Default)]
struct History {
    /// user -> (seq, ts, json), oldest first.
    users: HashMap<u64, VecDeque<(u64, i64, Arc<str>)>>,
    /// (seq, user) in insertion order. Entries whose event is already gone
    /// (evicted by the per-user limit or by time) are skipped when reached.
    order: VecDeque<(u64, u64)>,
    next_seq: u64,
    /// Events currently kept.
    kept: usize,
}

impl History {
    fn record(&mut self, user: u64, ts: i64, json: &Arc<str>, per_user: usize, horizon: i64, total: usize) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let events = self.users.entry(user).or_default();
        events.push_back((seq, ts, json.clone()));
        self.kept += 1;
        while events.len() > per_user || events.front().is_some_and(|(_, oldest, _)| *oldest < horizon) {
            events.pop_front();
            self.kept -= 1;
        }
        self.order.push_back((seq, user));
        // `order` also holds entries of already evicted events; cap it too.
        while self.kept > total || self.order.len() > total.saturating_mul(2).max(1) {
            let Some((seq, user)) = self.order.pop_front() else { break };
            if let Some(events) = self.users.get_mut(&user) {
                if events.front().is_some_and(|(front, _, _)| *front == seq) {
                    events.pop_front();
                    self.kept -= 1;
                    if events.is_empty() {
                        self.users.remove(&user);
                    }
                }
            }
        }
    }

    fn prune(&mut self, horizon: i64) {
        let mut dropped = 0;
        self.users.retain(|_, events| {
            while events.front().is_some_and(|(_, ts, _)| *ts < horizon) {
                events.pop_front();
                dropped += 1;
            }
            !events.is_empty()
        });
        self.kept -= dropped;
        let users = &self.users;
        self.order.retain(|(seq, user)| {
            users
                .get(user)
                .is_some_and(|events| events.front().is_some_and(|(front, _, _)| front <= seq))
        });
    }
}

impl Registry {
    fn with_replay(events: usize, secs: u64, total: usize) -> Self {
        Self {
            replay_events: events,
            replay_ms: secs as i64 * 1000,
            replay_total: total,
            ..Self::default()
        }
    }

    #[cfg(test)]
    fn subscribe(&self, user: u64, tx: SyncSender<Outgoing>, max_per_user: usize) -> Option<u64> {
        self.subscribe_since(user, tx, max_per_user, None)
    }

    /// Subscribe, first queuing the user's kept events at or after `since`.
    fn subscribe_since(
        &self,
        user: u64,
        tx: SyncSender<Outgoing>,
        max_per_user: usize,
        since: Option<i64>,
    ) -> Option<u64> {
        let history = self.history.lock().unwrap();
        let mut users = self.users.write().unwrap();
        let conns = users.entry(user).or_default();
        if conns.len() >= max_per_user {
            return None;
        }
        if let (Some(since), Some(events)) = (since, history.users.get(&user)) {
            for (_, _, json) in events.iter().filter(|(_, ts, _)| *ts >= since) {
                if tx.try_send(Outgoing::Text(json.clone())).is_err() {
                    break; // more than the queue holds: the client reconciles
                }
            }
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        conns.push((id, tx));
        Some(id)
    }

    /// Keep `json` (timestamp `ts`) for `user`'s reconnects and push it to
    /// the user's live connections. Returns connections reached.
    fn record_and_push(&self, user: u64, ts: i64, json: &Arc<str>, metrics: &Metrics) -> usize {
        let mut history = self.history.lock().unwrap();
        if self.replay_events > 0 && self.replay_total > 0 {
            history.record(user, ts, json, self.replay_events, ts - self.replay_ms, self.replay_total);
        }
        let reached = self.push(user, json, metrics);
        drop(history);
        reached
    }

    /// Forget events older than the replay window (users who went quiet).
    fn prune(&self, now_ms: i64) {
        self.history.lock().unwrap().prune(now_ms - self.replay_ms);
    }

    fn unsubscribe(&self, user: u64, id: u64) {
        let mut users = self.users.write().unwrap();
        if let Some(conns) = users.get_mut(&user) {
            conns.retain(|(conn, _)| *conn != id);
            if conns.is_empty() {
                users.remove(&user);
            }
        }
    }

    /// Queue `json` for every connection of `user`. A connection whose queue
    /// is full is dropped (a slow reader must not stall everyone else's
    /// push); its writer then closes the socket. Returns connections reached.
    fn push(&self, user: u64, json: &Arc<str>, metrics: &Metrics) -> usize {
        let mut slow = Vec::new();
        let mut reached = 0;
        if let Some(conns) = self.users.read().unwrap().get(&user) {
            for (id, tx) in conns {
                match tx.try_send(Outgoing::Text(json.clone())) {
                    Ok(()) => reached += 1,
                    Err(TrySendError::Full(_)) => slow.push(*id),
                    Err(TrySendError::Disconnected(_)) => {}
                }
            }
        }
        for id in slow {
            metrics.slow_clients_dropped.fetch_add(1, Ordering::Relaxed);
            self.unsubscribe(user, id);
        }
        reached
    }
}

/// The owner and the JSON push for one execution-event payload, or `None`
/// for events with no owner to push to (v1/v2 events, command rejects,
/// NotFound, admin acks) or that are not order results.
fn event_json(payload: &[u8]) -> Option<(u64, String)> {
    let event = wire::decode_execution_event(payload)?;
    let user = wire::execution_event_target_user(payload);
    if user == 0 {
        return None;
    }
    let report = event.report;
    let order_id = event.target_order_id;
    let base = format!(
        "\"order_id\":{order_id},\"user\":{user},\"instrument\":{},\"event_id\":\"{}:{}:{}:{order_id}\"",
        report.instrument.0, event.raft_group, event.raft_index, event.ordinal
    );
    let json = match report.type_code {
        wire::RT_ACCEPTED => format!("{{\"e\":\"COMMITTED\",{base}}}"),
        wire::RT_STOP_ACCEPTED => {
            format!("{{\"e\":\"COMMITTED\",{base},\"conditional\":true}}")
        }
        wire::RT_TRADE => {
            // Trades are published once per side; `target_order_id` says
            // which side this copy is for. The report's order is the taker.
            let taker = report.order_id.0 == order_id;
            let (role, fee, counterparty) = if taker {
                ("taker", report.taker_fee, report.aux_id)
            } else {
                ("maker", report.maker_fee, report.order_id.0)
            };
            let aggressor = match report.side {
                trade_core::types::Side::Buy => "buy",
                trade_core::types::Side::Sell => "sell",
            };
            format!(
                "{{\"e\":\"TRADE\",{base},\"price\":{},\"qty\":{},\"fee\":{fee},\"role\":\"{role}\",\"aggressor\":\"{aggressor}\",\"counter_order_id\":{counterparty}}}",
                report.price, report.qty
            )
        }
        wire::RT_PARTIAL => format!("{{\"e\":\"PARTIAL\",{base},\"filled_qty\":{}}}", report.qty),
        wire::RT_FILLED => format!("{{\"e\":\"FILLED\",{base}}}"),
        wire::RT_MAKER_FILLED => format!("{{\"e\":\"FILLED\",{base},\"role\":\"maker\"}}"),
        wire::RT_RESTING => format!("{{\"e\":\"RESTING\",{base}}}"),
        wire::RT_CANCELLED => format!("{{\"e\":\"CANCELLED\",{base}}}"),
        wire::RT_MODIFIED => format!("{{\"e\":\"MODIFIED\",{base},\"remaining\":{}}}", report.qty),
        wire::RT_TRIGGERED => format!("{{\"e\":\"TRIGGERED\",{base}}}"),
        wire::RT_REJECTED => format!(
            "{{\"e\":\"REJECTED\",{base},\"reason\":\"{}\"}}",
            wire::reject_reason_name(report.aux_id)
        ),
        _ => return None,
    };
    Some((user, json))
}

/// [`event_json`] with the event's timestamp (ms) as `"ts"`, the value a
/// client passes back as `since` when it reconnects.
fn event_json_at(payload: &[u8], ts: i64) -> Option<(u64, String)> {
    let (user, mut json) = event_json(payload)?;
    json.pop(); // the closing brace
    json.push_str(&format!(",\"ts\":{ts}}}"));
    Some((user, json))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

fn consume(
    brokers: String,
    topic: String,
    group: String,
    shard: Option<Shard>,
    registry: Arc<Registry>,
    metrics: Arc<Metrics>,
) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        // Never commit: each instance reads every partition itself.
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "5")
        .create()
        .expect("create execution consumer");
    // Start one replay window back, so reconnects can resume right after an
    // instance (re)starts; the tail when replay is off.
    let start_ms = now_ms() - registry.replay_ms;
    let assignment = loop {
        let owned = |partition: i32| shard.as_ref().is_none_or(|shard| shard.owns_partition(partition));
        match start_positions(&consumer, &topic, (registry.replay_events > 0).then_some(start_ms), owned) {
            Ok(assignment) => break assignment,
            Err(error) => {
                log_warn!("user-stream", "event=assign_failed error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    consumer.assign(&assignment).expect("assign execution partitions");
    log_info!(
        "user-stream",
        "consuming {topic} from {brokers}, {} partitions, replay window {} s",
        assignment.count(),
        registry.replay_ms / 1000
    );
    let mut last_prune = std::time::Instant::now();
    loop {
        if last_prune.elapsed() >= Duration::from_secs(10) {
            registry.prune(now_ms());
            last_prune = std::time::Instant::now();
        }
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                metrics.events_consumed.fetch_add(1, Ordering::Relaxed);
                let ts = message.timestamp().to_millis().unwrap_or_else(now_ms);
                let Some((user, json)) = message.payload().and_then(|payload| event_json_at(payload, ts))
                else {
                    continue;
                };
                let json: Arc<str> = json.into();
                if registry.record_and_push(user, ts, &json, &metrics) > 0 {
                    metrics.events_pushed.fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics
                        .events_without_subscriber
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Some(Err(error)) => {
                log_warn!("user-stream", "event=consume_error error={error}");
                std::thread::sleep(Duration::from_millis(200));
            }
            None => {}
        }
    }
}

/// Every partition of `topic`, positioned at the first message at or after
/// `since_ms` (the tail where there is none, or when `None`).
fn start_positions(
    consumer: &BaseConsumer,
    topic: &str,
    since_ms: Option<i64>,
    owned: impl Fn(i32) -> bool,
) -> Result<TopicPartitionList, String> {
    let partitions = topic_partitions(consumer, topic)?;
    let mut list = TopicPartitionList::new();
    for partition in (0..partitions as i32).filter(|partition| owned(*partition)) {
        let offset = since_ms.map_or(Offset::End, Offset::Offset);
        list.add_partition_offset(topic, partition, offset)
            .map_err(|error| error.to_string())?;
    }
    if since_ms.is_none() {
        return Ok(list);
    }
    consumer
        .offsets_for_times(list, Duration::from_secs(10))
        .map_err(|error| error.to_string())
}

fn topic_partitions<C: rdkafka::consumer::ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic: &str,
) -> Result<u32, String> {
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|error| error.to_string())?;
    metadata
        .topics()
        .iter()
        .find(|meta| meta.name() == topic)
        .map(|meta| meta.partitions().len() as u32)
        .filter(|count| *count > 0)
        .ok_or_else(|| format!("topic {topic} has no partitions yet"))
}

/// Partition of the user-keyed topic that carries `user`'s events. Router
/// and pushers must agree, so it depends on nothing but the two inputs.
fn user_partition(user: u64, partitions: u32) -> i32 {
    let mut x = user.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    (x % partitions.max(1) as u64) as i32
}

/// This pusher's share of the users.
#[derive(Clone)]
struct Shard {
    index: u32,
    count: u32,
    /// Partitions of the user-keyed topic.
    partitions: u32,
    /// host:port of every shard, in shard order (may be empty).
    peers: Vec<String>,
}

impl Shard {
    fn owns_partition(&self, partition: i32) -> bool {
        partition as u32 % self.count == self.index
    }

    fn owner(&self, user: u64) -> u32 {
        user_partition(user, self.partitions) as u32 % self.count
    }
}

/// Router role: re-publish every execution event to the user-keyed topic.
/// Offsets are committed only after the batch is acknowledged; a failed
/// batch is re-read (duplicates are harmless: clients dedupe on event_id).
fn route(brokers: String, source: String, target: String, group: String, metrics: Arc<Metrics>) {
    use rdkafka::consumer::CommitMode;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "5")
        .create()
        .expect("create router consumer");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "2")
        .set("batch.num.messages", "10000")
        .set("compression.type", "lz4")
        .create()
        .expect("create router producer");
    let partitions = loop {
        match topic_partitions(&consumer, &target) {
            Ok(partitions) => break partitions,
            Err(error) => {
                log_warn!("user-stream", "event=user_topic_unavailable topic={target} error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    consumer.subscribe(&[&source]).expect("subscribe execution topic");
    log_info!("user-stream", "routing {source} -> {target} ({partitions} partitions), group {group}");
    let batch_max = 10_000;
    loop {
        // (partition, offset, timestamp, payload) of one micro-batch.
        let mut batch: Vec<(i32, i64, Option<i64>, Vec<u8>)> = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_millis(5);
        let mut wait = Duration::from_millis(100);
        while batch.len() < batch_max {
            match consumer.poll(wait) {
                Some(Ok(message)) => batch.push((
                    message.partition(),
                    message.offset(),
                    message.timestamp().to_millis(),
                    message.payload().unwrap_or_default().to_vec(),
                )),
                Some(Err(error)) => {
                    log_warn!("user-stream", "event=consume_error error={error}");
                    break;
                }
                None => break,
            }
            wait = deadline.saturating_duration_since(std::time::Instant::now());
            if wait.is_zero() {
                break;
            }
        }
        if batch.is_empty() {
            continue;
        }
        // Owner of each event, as the record key; 0 = nothing to push to.
        let keys: Vec<[u8; 8]> = batch
            .iter()
            .map(|(_, _, _, payload)| wire::execution_event_target_user(payload).to_be_bytes())
            .collect();
        let deliveries = futures::executor::block_on(futures::future::join_all(
            batch.iter().zip(&keys).filter_map(|((_, _, ts, payload), key)| {
                let user = u64::from_be_bytes(*key);
                if user == 0 {
                    return None;
                }
                let mut record = FutureRecord::to(&target)
                    .partition(user_partition(user, partitions))
                    .key(key)
                    .payload(payload.as_slice());
                if let Some(ts) = ts {
                    record = record.timestamp(*ts); // keeps `ts` / `since` stable
                }
                Some(producer.send(record, Duration::from_secs(5)))
            }),
        ));
        let mut first: HashMap<i32, i64> = HashMap::new();
        let mut next: HashMap<i32, i64> = HashMap::new();
        for (partition, offset, _, _) in &batch {
            first.entry(*partition).and_modify(|o| *o = (*o).min(*offset)).or_insert(*offset);
            next.entry(*partition).and_modify(|o| *o = (*o).max(offset + 1)).or_insert(offset + 1);
        }
        if deliveries.iter().all(Result::is_ok) {
            metrics.events_routed.fetch_add(deliveries.len() as u64, Ordering::Relaxed);
            let mut list = TopicPartitionList::new();
            for (partition, offset) in next {
                let _ = list.add_partition_offset(&source, partition, Offset::Offset(offset));
            }
            if let Err(error) = consumer.commit(&list, CommitMode::Async) {
                log_warn!("user-stream", "event=router_commit_failed error={error}");
            }
            continue;
        }
        log_warn!("user-stream", "event=router_batch_retry records={}", batch.len());
        for (partition, offset) in first {
            let mut attempt = 0;
            while let Err(error) =
                consumer.seek(&source, partition, Offset::Offset(offset), Duration::from_secs(1))
            {
                let assigned = consumer.assignment().map(|list| {
                    list.elements().iter().any(|tp| tp.topic() == source && tp.partition() == partition)
                });
                if assigned == Ok(false) {
                    break; // its new owner resumes from the committed offset
                }
                attempt += 1;
                assert!(
                    attempt < 5,
                    "cannot rewind {source}[{partition}] to {offset} ({error}); restarting from committed offsets"
                );
                std::thread::sleep(Duration::from_millis(200 * attempt));
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let _ = stream.write_all(
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    );
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|part| {
        let (k, v) = part.split_once('=')?;
        (k == key).then_some(v)
    })
}

struct Config {
    token: String,
    queue: usize,
    max_per_user: usize,
    /// Set for a pusher: the users it serves.
    shard: Option<Shard>,
    /// A router serves no connections.
    router: bool,
}

fn handle(stream: TcpStream, config: &Config, registry: &Registry, metrics: &Metrics) {
    stream.set_nodelay(true).ok();
    let Ok(mut writer) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut first = String::new();
    if reader.read_line(&mut first).is_err() {
        return;
    }
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let uri = parts.next().unwrap_or("/");
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let (mut authorized, mut upgrade, mut key) = (false, false, None);
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(0) | Err(_) => return,
            Ok(_) if header.trim().is_empty() => break,
            Ok(_) => {}
        }
        if let Some((name, value)) = header.split_once(':') {
            let value = value.trim();
            if name.eq_ignore_ascii_case("authorization") {
                authorized = value == format!("Bearer {}", config.token);
            } else if name.eq_ignore_ascii_case("upgrade") {
                upgrade = value.eq_ignore_ascii_case("websocket");
            } else if name.eq_ignore_ascii_case("sec-websocket-key") {
                key = Some(value.to_string());
            }
        }
    }
    if method == "GET" && path == "/metrics" {
        respond(&mut writer, "200 OK", &metrics.render());
        return;
    }
    if !authorized {
        respond(&mut writer, "401 Unauthorized", "unauthorized");
        return;
    }
    if method != "GET" || path != "/ws/user" || config.router {
        respond(&mut writer, "404 Not Found", "not found");
        return;
    }
    let Some(user) = query_param(query, "user")
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|u| *u != 0)
    else {
        respond(&mut writer, "400 Bad Request", "user must be a non-zero id");
        return;
    };
    if let Some(shard) = &config.shard {
        let owner = shard.owner(user);
        if owner != shard.index {
            let _ = match shard.peers.get(owner as usize) {
                Some(peer) => writer.write_all(
                    format!(
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: ws://{peer}{uri}\r\n\
                         X-User-Stream-Shard: {owner}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                ),
                None => writer.write_all(
                    format!(
                        "HTTP/1.1 421 Misdirected Request\r\nX-User-Stream-Shard: {owner}\r\n\
                         Content-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                ),
            };
            return;
        }
    }
    let (Some(key), true) = (key, upgrade) else {
        respond(&mut writer, "400 Bad Request", "websocket upgrade required");
        return;
    };
    let since = query_param(query, "since").and_then(|v| v.parse::<i64>().ok());
    let (tx, rx) = mpsc::sync_channel::<Outgoing>(config.queue);
    let Some(conn) = registry.subscribe_since(user, tx.clone(), config.max_per_user, since) else {
        respond(
            &mut writer,
            "429 Too Many Requests",
            "too many connections for user",
        );
        return;
    };
    if writer
        .write_all(websocket::handshake_response(&key).as_bytes())
        .is_err()
    {
        registry.unsubscribe(user, conn);
        return;
    }
    metrics.connections.fetch_add(1, Ordering::Relaxed);

    // Reader: client pings/closes become frames for the writer to send.
    let control = tx;
    let reader_thread = std::thread::spawn(move || {
        let mut events = EventReader::new();
        loop {
            match events.next(&mut reader, 4096) {
                Ok(Some(ClientEvent::Ping(payload))) => {
                    if control.send(Outgoing::Pong(payload)).is_err() {
                        return;
                    }
                }
                Ok(Some(ClientEvent::Message(_))) => {} // nothing to receive
                Ok(Some(ClientEvent::Close(code))) => {
                    let _ = control.send(Outgoing::Close(code));
                    return;
                }
                Ok(None) | Err(_) => {
                    let _ = control.send(Outgoing::Close(Vec::new()));
                    return;
                }
            }
        }
    });
    // Writer: the only thread that writes this socket. Ends when the client
    // closes, the socket fails, or the registry drops a slow connection
    // (then every sender is gone and `recv` fails).
    loop {
        let frame = match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(frame) => frame,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if reader_thread.is_finished() {
                    break;
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let result = match frame {
            Outgoing::Text(json) => {
                websocket::write_frame(&mut writer, websocket::OP_TEXT, json.as_bytes())
            }
            Outgoing::Pong(payload) => {
                websocket::write_frame(&mut writer, websocket::OP_PONG, &payload)
            }
            Outgoing::Close(code) => {
                let _ = websocket::write_frame(&mut writer, websocket::OP_CLOSE, &code);
                break;
            }
        };
        if result.is_err() {
            break;
        }
        // Dropped for being slow: stop at once rather than keep writing.
        if !registry_has(registry, user, conn) {
            let _ =
                websocket::write_frame(&mut writer, websocket::OP_CLOSE, &1008u16.to_be_bytes());
            break;
        }
    }
    registry.unsubscribe(user, conn);
    let _ = writer.shutdown(std::net::Shutdown::Both);
    metrics.connections.fetch_sub(1, Ordering::Relaxed);
}

fn registry_has(registry: &Registry, user: u64, conn: u64) -> bool {
    registry
        .users
        .read()
        .unwrap()
        .get(&user)
        .is_some_and(|conns| conns.iter().any(|(id, _)| *id == conn))
}

fn main() {
    trade_core::oblog::init_from_env();
    let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    let addr = env("TC_USER_STREAM_ADDR", "0.0.0.0:9300");
    let token = std::env::var("TC_USER_STREAM_TOKEN")
        .or_else(|_| std::env::var("TC_ORDER_API_TOKEN"))
        .expect("TC_USER_STREAM_TOKEN or TC_ORDER_API_TOKEN");
    let brokers = std::env::var("TC_EXECUTION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("TC_ORDER_KAFKA_BROKERS"))
        .expect("TC_EXECUTION_KAFKA_BROKERS");
    let topic = env("TC_EXECUTION_KAFKA_TOPIC", "trade-executions-v1");
    let group = std::env::var("TC_USER_STREAM_GROUP").unwrap_or_else(|_| {
        format!(
            "user-stream-{}-{}",
            std::process::id(),
            trade_core::journal::now_nanos()
        )
    });
    let role = env("TC_USER_STREAM_ROLE", "all");
    let user_topic = env("TC_USER_STREAM_TOPIC", "trade-user-events-v1");
    let shard = (role == "pusher").then(|| {
        let count: u32 = env("TC_USER_STREAM_SHARDS", "1").parse().expect("TC_USER_STREAM_SHARDS");
        let index: u32 = env("TC_USER_STREAM_SHARD", "0").parse().expect("TC_USER_STREAM_SHARD");
        assert!(count >= 1 && index < count, "TC_USER_STREAM_SHARD must be below TC_USER_STREAM_SHARDS");
        let peers: Vec<String> = env("TC_USER_STREAM_PEERS", "")
            .split(',')
            .map(str::trim)
            .filter(|peer| !peer.is_empty())
            .map(String::from)
            .collect();
        assert!(
            peers.is_empty() || peers.len() == count as usize,
            "TC_USER_STREAM_PEERS must list all {count} shards in order"
        );
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .expect("create metadata client");
        let partitions = loop {
            match topic_partitions(&probe, &user_topic) {
                Ok(partitions) => break partitions,
                Err(error) => {
                    log_warn!("user-stream", "event=user_topic_unavailable topic={user_topic} error={error}");
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        };
        assert!(partitions >= count, "{user_topic} has {partitions} partitions, fewer than {count} shards");
        Shard { index, count, partitions, peers }
    });
    assert!(
        matches!(role.as_str(), "all" | "router" | "pusher"),
        "TC_USER_STREAM_ROLE must be all, router or pusher"
    );
    let config = Arc::new(Config {
        shard: shard.clone(),
        router: role == "router",
        token,
        queue: env("TC_USER_STREAM_QUEUE", "10000")
            .parse()
            .unwrap_or(10_000)
            .max(1),
        max_per_user: env("TC_USER_STREAM_MAX_CONNECTIONS_PER_USER", "16")
            .parse()
            .unwrap_or(16)
            .max(1),
    });
    let registry = Arc::new(Registry::with_replay(
        env("TC_USER_STREAM_REPLAY_EVENTS", "512").parse().unwrap_or(512),
        env("TC_USER_STREAM_REPLAY_SECS", "300").parse().unwrap_or(300),
        env("TC_USER_STREAM_REPLAY_MAX_TOTAL", "2000000").parse().unwrap_or(2_000_000),
    ));
    let metrics = Arc::new(Metrics::default());
    if role == "router" {
        let metrics = metrics.clone();
        let group = std::env::var("TC_USER_STREAM_GROUP").unwrap_or_else(|_| "user-stream-router".into());
        std::thread::Builder::new()
            .name("user-router".into())
            .spawn(move || route(brokers, topic, user_topic, group, metrics))
            .expect("spawn router");
    } else {
        let (registry, metrics) = (registry.clone(), metrics.clone());
        let topic = if shard.is_some() { user_topic } else { topic };
        std::thread::Builder::new()
            .name("execution-consumer".into())
            .spawn(move || consume(brokers, topic, group, shard, registry, metrics))
            .expect("spawn consumer");
    }
    let listener = TcpListener::bind(&addr).expect("bind user-stream");
    log_info!("user-stream", "listening on {addr}");
    for stream in listener.incoming().flatten() {
        let (config, registry, metrics) = (config.clone(), registry.clone(), metrics.clone());
        std::thread::spawn(move || handle(stream, &config, &registry, &metrics));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trade_core::exchange::ExecReport;
    use trade_core::types::{InstrumentId, OrderId, Side};

    fn payload(report: &ExecReport, target: u64) -> Vec<u8> {
        let mut out = [0u8; wire::EXECUTION_EVENT_LEN];
        wire::encode_execution_event_for_target(7, 42, 3, target, report, &mut out);
        out.to_vec()
    }

    #[test]
    fn each_report_becomes_a_push_for_its_owner() {
        let sym = InstrumentId(5);
        let committed = ExecReport::Accepted {
            instrument: sym,
            order_id: OrderId(1),
            user: 11,
        };
        assert_eq!(
            event_json(&payload(&committed, 1)).unwrap(),
            (
                11,
                r#"{"e":"COMMITTED","order_id":1,"user":11,"instrument":5,"event_id":"7:42:3:1"}"#
                    .into()
            )
        );

        let trade = ExecReport::Trade {
            instrument: sym,
            taker: OrderId(2),
            maker: OrderId(1),
            aggressor: Side::Buy,
            price: 100,
            qty: 3,
            maker_fee: 1,
            taker_fee: 2,
            taker_user: 22,
            maker_user: 11,
        };
        let (user, json) = event_json(&payload(&trade, 2)).unwrap();
        assert_eq!(user, 22);
        assert!(json.contains(r#""e":"TRADE""#), "{json}");
        assert!(
            json.contains(r#""price":100,"qty":3,"fee":2,"role":"taker""#),
            "{json}"
        );
        assert!(json.contains(r#""counter_order_id":1"#), "{json}");
        let (user, json) = event_json(&payload(&trade, 1)).unwrap();
        assert_eq!(user, 11);
        assert!(json.contains(r#""fee":1,"role":"maker""#), "{json}");
        assert!(json.contains(r#""order_id":1"#) && json.contains(r#""counter_order_id":2"#));

        let rejected = ExecReport::Rejected {
            instrument: sym,
            order_id: OrderId(3),
            reason: "tick-size",
            user: 33,
        };
        let (_, json) = event_json(&payload(&rejected, 3)).unwrap();
        assert!(json.contains(r#""e":"REJECTED""#) && json.contains(r#""reason":"tick-size""#));

        let stop = ExecReport::StopAccepted {
            instrument: sym,
            order_id: OrderId(4),
            user: 44,
        };
        assert!(event_json(&payload(&stop, 4))
            .unwrap()
            .1
            .contains(r#""conditional":true"#));

        let maker_filled = ExecReport::MakerFilled {
            instrument: sym,
            order_id: OrderId(1),
            user: 11,
        };
        assert_eq!(
            event_json(&payload(&maker_filled, 1)).unwrap(),
            (
                11,
                r#"{"e":"FILLED","order_id":1,"user":11,"instrument":5,"event_id":"7:42:3:1","role":"maker"}"#
                    .into()
            )
        );

        // No owner, nothing to push.
        let unowned = ExecReport::NotFound {
            instrument: sym,
            order_id: OrderId(9),
        };
        assert_eq!(event_json(&payload(&unowned, 9)), None);
        assert_eq!(event_json(b"garbage"), None);
    }

    #[test]
    fn the_two_sides_of_a_self_trade_have_distinct_event_ids() {
        let trade = ExecReport::Trade {
            instrument: InstrumentId(5),
            taker: OrderId(1),
            maker: OrderId(2),
            aggressor: Side::Buy,
            price: 100,
            qty: 1,
            maker_fee: 0,
            taker_fee: 0,
            taker_user: 11,
            maker_user: 11,
        };
        let (_, taker) = event_json(&payload(&trade, 1)).unwrap();
        let (_, maker) = event_json(&payload(&trade, 2)).unwrap();
        let id = |json: &str| json.split("\"event_id\":\"").nth(1).unwrap().split('"').next().unwrap().to_string();
        assert_ne!(id(&taker), id(&maker));
        let (_, stamped) = event_json_at(&payload(&trade, 1), 1_700_000_000_123).unwrap();
        assert!(stamped.ends_with(r#","ts":1700000000123}"#), "{stamped}");
    }

    #[test]
    fn a_reconnect_replays_events_since_its_last_timestamp_then_goes_live() {
        let registry = Registry::with_replay(3, 60, 1_000);
        let metrics = Metrics::default();
        let text = |n: i64| -> Arc<str> { format!("e{n}").into() };
        for ts in 1..=4 {
            registry.record_and_push(7, ts * 1000, &text(ts), &metrics);
        }
        // Only the last 3 are kept; `since` picks those at or after it.
        let (tx, rx) = mpsc::sync_channel(16);
        registry.subscribe_since(7, tx, 4, Some(3_000)).unwrap();
        registry.record_and_push(7, 5_000, &text(5), &metrics);
        let got: Vec<String> = rx
            .try_iter()
            .map(|frame| match frame {
                Outgoing::Text(json) => json.to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(got, ["e3", "e4", "e5"], "replay, then live, in order");
        // Events older than the window are forgotten.
        registry.prune(5_000 + 61_000);
        let history = registry.history.lock().unwrap();
        assert!(history.users.is_empty() && history.order.is_empty() && history.kept == 0);
    }

    #[test]
    fn the_replay_buffer_keeps_at_most_the_total_limit_oldest_out_first() {
        let registry = Registry::with_replay(2, 600, 4);
        let metrics = Metrics::default();
        let text = |n: i64| -> Arc<str> { format!("e{n}").into() };
        // User 1 is chatty (per-user limit 2), users 2..4 add one each.
        for ts in 1..=5 {
            registry.record_and_push(1, ts, &text(ts), &metrics);
        }
        for (user, ts) in [(2, 6), (3, 7), (4, 8)] {
            registry.record_and_push(user, ts, &text(ts), &metrics);
        }
        let history = registry.history.lock().unwrap();
        assert_eq!(history.kept, 4);
        assert!(history.order.len() <= 8);
        let kept = |user| -> Vec<i64> {
            history.users.get(&user).map_or(vec![], |q| q.iter().map(|(_, ts, _)| *ts).collect())
        };
        // Over the total: user 1's oldest kept event (4) went first.
        assert_eq!(kept(1), [5]);
        assert_eq!((kept(2), kept(3), kept(4)), (vec![6], vec![7], vec![8]));
        drop(history);
        // Bounded under a long stream from one user, too.
        for ts in 10..10_000 {
            registry.record_and_push(9, ts, &text(ts), &metrics);
        }
        let history = registry.history.lock().unwrap();
        assert!(history.kept <= 4 && history.order.len() <= 8, "{} {}", history.kept, history.order.len());
    }

    #[test]
    fn users_spread_evenly_and_each_belongs_to_exactly_one_shard() {
        let partitions = 64;
        let mut per_partition = vec![0u32; partitions as usize];
        for user in 1_000_000..1_064_000u64 {
            per_partition[user_partition(user, partitions) as usize] += 1;
        }
        let (min, max) = (*per_partition.iter().min().unwrap(), *per_partition.iter().max().unwrap());
        assert!(min > 850 && max < 1150, "skewed: {min}..{max}");
        let shards: Vec<Shard> = (0..3)
            .map(|index| Shard { index, count: 3, partitions, peers: vec![] })
            .collect();
        for user in [1u64, 42, 1_000_003, u64::MAX] {
            let partition = user_partition(user, partitions);
            let owners: Vec<u32> =
                shards.iter().filter(|s| s.owns_partition(partition)).map(|s| s.index).collect();
            assert_eq!(owners, [shards[0].owner(user)], "user {user}");
        }
    }

    #[test]
    fn registry_routes_to_the_user_and_drops_slow_connections() {
        let registry = Registry::default();
        let metrics = Metrics::default();
        let (fast_tx, fast_rx) = mpsc::sync_channel(8);
        let (slow_tx, _slow_rx) = mpsc::sync_channel(1);
        let fast = registry.subscribe(1, fast_tx, 4).unwrap();
        let slow = registry.subscribe(1, slow_tx, 4).unwrap();
        let other_tx = mpsc::sync_channel(8).0;
        registry.subscribe(2, other_tx, 4).unwrap();

        let json: Arc<str> = "{}".into();
        assert_eq!(registry.push(1, &json, &metrics), 2);
        // The slow connection's single slot is now full: the next push drops it.
        assert_eq!(registry.push(1, &json, &metrics), 1);
        assert_eq!(metrics.slow_clients_dropped.load(Ordering::Relaxed), 1);
        assert!(registry_has(&registry, 1, fast));
        assert!(!registry_has(&registry, 1, slow));
        assert_eq!(fast_rx.try_iter().count(), 2);
        assert_eq!(registry.push(3, &json, &metrics), 0, "no subscribers");

        // Per-user connection cap.
        let limited = Registry::default();
        assert!(limited.subscribe(5, mpsc::sync_channel(1).0, 1).is_some());
        assert!(limited.subscribe(5, mpsc::sync_channel(1).0, 1).is_none());
    }
}
