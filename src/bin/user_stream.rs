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
//! kept, and replayed before the live stream continues, with no gap or
//! reordering between the two:
//!   after=<cursor>  (pusher shards) every event carries `cursor`, its offset
//!                   in the user's partition of the user topic: an exact
//!                   position that also survives restarts. Preferred.
//!   since=<ts ms>   events at or after `ts`, reaching back
//!                   TC_USER_STREAM_SINCE_GRACE_MS (10 s): one user's events
//!                   do not arrive in timestamp order, so a few are replayed
//!                   twice (dedupe on `event_id`) rather than skipped.
//! When events after the resume point are no longer all kept (window, total
//! limit, per-user limit, not loaded since a restart) or more than the
//! connection queue holds, the first frame is {"e":"RESYNC",...}: reconcile
//! through the order query API. An instance rebuilds its window from Kafka
//! when it starts.
//!
//! Connections: the request must arrive within 10 s and 16 KiB; idle
//! connections are pinged every 30 s and closed after 75 s without any
//! frame from the client; a write blocked for 10 s drops the connection;
//! at most TC_USER_STREAM_MAX_CONNECTIONS (50,000) at once.
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
//!   GET /ws/user?user=U[&after=<cursor>|&since=<ts ms>]   (Authorization: Bearer <TC_USER_STREAM_TOKEN>)
//!   GET /metrics
//!
//! Env: TC_USER_STREAM_ADDR (0.0.0.0:9300), TC_USER_STREAM_TOKEN (falls back
//! to TC_ORDER_API_TOKEN), TC_EXECUTION_KAFKA_BROKERS, TC_EXECUTION_KAFKA_TOPIC
//! (trade-executions-v1), TC_USER_STREAM_QUEUE (per-connection backlog before a
//! slow client is dropped, 10000), TC_USER_STREAM_MAX_CONNECTIONS_PER_USER (16).

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
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

/// One connection's outbound queue. An unbounded channel plus a depth
/// counter, not a bounded `sync_channel`: that preallocates every slot
/// (~400 KB per connection at the default depth) before anything is sent.
struct Conn {
    id: u64,
    tx: mpsc::Sender<Outgoing>,
    depth: Arc<AtomicUsize>,
    cap: usize,
}

enum Offer {
    Queued,
    Full,
    Gone,
}

impl Conn {
    fn offer(&self, json: &Arc<str>) -> Offer {
        if self.depth.fetch_add(1, Ordering::AcqRel) >= self.cap {
            self.depth.fetch_sub(1, Ordering::AcqRel);
            return Offer::Full;
        }
        match self.tx.send(Outgoing::Text(json.clone())) {
            Ok(()) => Offer::Queued,
            Err(_) => {
                self.depth.fetch_sub(1, Ordering::AcqRel);
                Offer::Gone
            }
        }
    }
}

/// Where a reconnecting client resumes.
#[derive(Clone, Copy, Debug)]
enum Resume {
    /// `since=<ts ms>`: events with a timestamp at or after it, reaching
    /// back a grace period, because one user's events do not arrive in
    /// timestamp order (different partitions, routers, producer clocks).
    Since(i64),
    /// `after=<cursor>` (pusher shards): the `cursor` of the last event the
    /// client has. A user's events all sit in one partition of the user
    /// topic, so its offset is an exact, restart-proof position.
    After { cursor: i64, partition: i32 },
}

/// user -> that user's live connections, plus each user's recent events
/// for resuming after a reconnect.
#[derive(Default)]
struct Registry {
    users: RwLock<HashMap<u64, Vec<Conn>>>,
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
    /// How far before `since` a replay reaches (see [`Resume::Since`]).
    since_grace_ms: i64,
}

struct Kept {
    seq: u64,
    ts: i64,
    partition: i32,
    offset: i64,
    json: Arc<str>,
}

struct UserEvents {
    events: VecDeque<Kept>,
    /// Newest timestamp / offset evicted by the per-user limit.
    lost_ts: i64,
    lost_offset: i64,
}

impl Default for UserEvents {
    fn default() -> Self {
        Self {
            events: VecDeque::new(),
            lost_ts: i64::MIN,
            lost_offset: i64::MIN,
        }
    }
}

/// Kept events: per user, plus every event's insertion order so the oldest
/// can be evicted, by age or by the total limit, from the front.
struct History {
    users: HashMap<u64, UserEvents>,
    /// (seq, user) in insertion order. Entries whose event is already gone
    /// (evicted by the per-user limit) are skipped when reached.
    order: VecDeque<(u64, u64)>,
    next_seq: u64,
    /// Events currently kept.
    kept: usize,
    /// Events at or before these were dropped for everyone (age, total
    /// limit) or never loaded (before the startup rebuild): a client
    /// resuming from before them is told to resync.
    floor_ts: i64,
    floor_offset: HashMap<i32, i64>,
}

impl Default for History {
    fn default() -> Self {
        Self {
            users: HashMap::new(),
            order: VecDeque::new(),
            next_seq: 0,
            kept: 0,
            floor_ts: i64::MIN,
            floor_offset: HashMap::new(),
        }
    }
}

impl History {
    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        user: u64,
        ts: i64,
        partition: i32,
        offset: i64,
        json: &Arc<str>,
        per_user: usize,
        total: usize,
    ) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let entry = self.users.entry(user).or_default();
        entry.events.push_back(Kept {
            seq,
            ts,
            partition,
            offset,
            json: json.clone(),
        });
        self.kept += 1;
        while entry.events.len() > per_user {
            let old = entry.events.pop_front().expect("over the limit");
            entry.lost_ts = entry.lost_ts.max(old.ts);
            entry.lost_offset = entry.lost_offset.max(old.offset);
            self.kept -= 1;
        }
        self.order.push_back((seq, user));
        // `order` also holds entries of already evicted events; cap it too.
        while self.kept > total || self.order.len() > total.saturating_mul(2).max(1) {
            if !self.evict_front(|_| true) {
                break;
            }
        }
    }

    /// Pop the oldest `order` entry: skipped if its event is already gone,
    /// evicted for everyone if `evict` agrees. False when nothing was popped.
    fn evict_front(&mut self, evict: impl Fn(&Kept) -> bool) -> bool {
        let Some(&(seq, user)) = self.order.front() else {
            return false;
        };
        let Some(entry) = self.users.get_mut(&user) else {
            self.order.pop_front();
            return true;
        };
        match entry.events.front() {
            // The oldest entry still kept is always its user's oldest event.
            Some(front) if front.seq == seq => {
                if !evict(front) {
                    return false;
                }
                let old = entry.events.pop_front().expect("front");
                self.floor_ts = self.floor_ts.max(old.ts);
                let floor = self.floor_offset.entry(old.partition).or_insert(i64::MIN);
                *floor = (*floor).max(old.offset);
                self.kept -= 1;
                if entry.events.is_empty() && entry.lost_ts <= self.floor_ts {
                    self.users.remove(&user);
                }
            }
            _ => {} // already evicted by the per-user limit
        }
        self.order.pop_front();
        true
    }

    /// Evict events older than `horizon`, oldest first; stops at the first
    /// kept event inside the window. Proportional to what it evicts.
    fn prune(&mut self, horizon: i64) {
        while self.evict_front(|kept| kept.ts < horizon) {}
    }

    /// Drop users holding nothing but a per-user loss mark that the global
    /// floor has caught up with (rare; run every few minutes).
    fn sweep(&mut self) {
        let floor = self.floor_ts;
        self.users
            .retain(|_, entry| !entry.events.is_empty() || entry.lost_ts > floor);
    }

    /// Whether events between `resume` and the oldest kept one may be gone.
    fn incomplete(&self, user: u64, resume: Resume, grace_ms: i64) -> bool {
        let entry = self.users.get(&user);
        match resume {
            Resume::Since(since) => {
                let from = since.saturating_sub(grace_ms);
                from <= self.floor_ts || entry.is_some_and(|e| from <= e.lost_ts)
            }
            Resume::After { cursor, partition } => {
                self.floor_offset
                    .get(&partition)
                    .is_none_or(|floor| cursor < *floor)
                    || entry.is_some_and(|e| cursor < e.lost_offset)
            }
        }
    }
}

const RESYNC: &str = "{\"e\":\"RESYNC\",\"reason\":\"events since the resume point are no longer all kept; reconcile through the order query API\"}";

impl Registry {
    fn with_replay(events: usize, secs: u64, total: usize) -> Self {
        Self {
            replay_events: events,
            replay_ms: secs as i64 * 1000,
            replay_total: total,
            since_grace_ms: 10_000,
            ..Self::default()
        }
    }

    /// Before consuming: everything older than `start_ms`, and below each
    /// partition's start offset, was never loaded.
    fn set_floor(&self, start_ms: i64, start_offsets: &[(i32, i64)]) {
        let mut history = self.history.lock().unwrap();
        history.floor_ts = start_ms;
        for (partition, offset) in start_offsets {
            history.floor_offset.insert(*partition, offset - 1);
        }
    }

    #[cfg(test)]
    fn subscribe(
        &self,
        user: u64,
        conn: (mpsc::Sender<Outgoing>, usize),
        max_per_user: usize,
    ) -> Option<u64> {
        self.subscribe_from(user, conn, max_per_user, None)
    }

    /// Subscribe, first queuing the user's kept events after `resume` — or a
    /// RESYNC notice when they are not all kept, or more than the queue
    /// holds.
    fn subscribe_from(
        &self,
        user: u64,
        (tx, cap): (mpsc::Sender<Outgoing>, usize),
        max_per_user: usize,
        resume: Option<Resume>,
    ) -> Option<u64> {
        let history = self.history.lock().unwrap();
        let mut users = self.users.write().unwrap();
        let conns = users.entry(user).or_default();
        if conns.len() >= max_per_user {
            return None;
        }
        let conn = Conn {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            tx,
            depth: Arc::new(AtomicUsize::new(0)),
            cap,
        };
        if let Some(resume) = resume {
            let replay: Vec<&Arc<str>> = history.users.get(&user).map_or(Vec::new(), |entry| {
                entry
                    .events
                    .iter()
                    .filter(|kept| match resume {
                        Resume::Since(since) => {
                            kept.ts >= since.saturating_sub(self.since_grace_ms)
                        }
                        Resume::After { cursor, .. } => kept.offset > cursor,
                    })
                    .map(|kept| &kept.json)
                    .collect()
            });
            if history.incomplete(user, resume, self.since_grace_ms) || replay.len() >= cap {
                conn.offer(&Arc::from(RESYNC));
            }
            if replay.len() < cap {
                for json in replay {
                    conn.offer(json);
                }
            }
        }
        let id = conn.id;
        conns.push(conn);
        Some(id)
    }

    /// Keep `json` for `user`'s reconnects and push it to the user's live
    /// connections. Returns connections reached.
    fn record_and_push(
        &self,
        user: u64,
        (ts, partition, offset): (i64, i32, i64),
        json: &Arc<str>,
        metrics: &Metrics,
    ) -> usize {
        let mut history = self.history.lock().unwrap();
        if self.replay_events > 0 && self.replay_total > 0 {
            history.record(
                user,
                ts,
                partition,
                offset,
                json,
                self.replay_events,
                self.replay_total,
            );
        }
        let reached = self.push(user, json, metrics);
        drop(history);
        reached
    }

    /// Forget events older than the replay window (users who went quiet).
    fn prune(&self, now_ms: i64) {
        self.history.lock().unwrap().prune(now_ms - self.replay_ms);
    }

    fn sweep(&self) {
        self.history.lock().unwrap().sweep();
    }

    /// The queue-depth counter of one connection, for its writer.
    fn depth_of(&self, user: u64, id: u64) -> Option<Arc<AtomicUsize>> {
        self.users
            .read()
            .unwrap()
            .get(&user)?
            .iter()
            .find(|conn| conn.id == id)
            .map(|conn| conn.depth.clone())
    }

    fn unsubscribe(&self, user: u64, id: u64) {
        let mut users = self.users.write().unwrap();
        if let Some(conns) = users.get_mut(&user) {
            conns.retain(|conn| conn.id != id);
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
            for conn in conns {
                match conn.offer(json) {
                    Offer::Queued => reached += 1,
                    Offer::Full => slow.push(conn.id),
                    Offer::Gone => {}
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
        let owned = |partition: i32| {
            shard
                .as_ref()
                .is_none_or(|shard| shard.owns_partition(partition))
        };
        match start_positions(
            &consumer,
            &topic,
            (registry.replay_events > 0).then_some(start_ms),
            owned,
        ) {
            Ok(assignment) => break assignment,
            Err(error) => {
                log_warn!("user-stream", "event=assign_failed error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    // Where loading starts, per partition: nothing before it is kept.
    let start_offsets: Vec<(i32, i64)> = assignment
        .elements()
        .iter()
        .map(|element| {
            let offset = match element.offset() {
                Offset::Offset(offset) => offset,
                _ => consumer
                    .fetch_watermarks(&topic, element.partition(), Duration::from_secs(10))
                    .map_or(i64::MAX, |(_, high)| high),
            };
            (element.partition(), offset)
        })
        .collect();
    registry.set_floor(start_ms, &start_offsets);
    consumer
        .assign(&assignment)
        .expect("assign execution partitions");
    let partitions = topic_partitions(&consumer, &topic).unwrap_or(0);
    log_info!(
        "user-stream",
        "consuming {topic} from {brokers}, {} of {partitions} partitions, replay window {} s",
        assignment.count(),
        registry.replay_ms / 1000
    );
    let mut last_prune = std::time::Instant::now();
    let mut last_sweep = std::time::Instant::now();
    let mut last_partition_check = std::time::Instant::now();
    loop {
        if last_prune.elapsed() >= Duration::from_secs(10) {
            registry.prune(now_ms());
            last_prune = std::time::Instant::now();
        }
        if last_sweep.elapsed() >= Duration::from_secs(300) {
            registry.sweep();
            last_sweep = std::time::Instant::now();
        }
        // Partitions are assigned once, and a pusher's users are mapped by
        // the partition count: a changed count means new partitions nobody
        // reads and users routed to the wrong shard. Restart to pick it up.
        if last_partition_check.elapsed() >= Duration::from_secs(30) {
            last_partition_check = std::time::Instant::now();
            if let Ok(now) = topic_partitions(&consumer, &topic) {
                assert_eq!(
                    now, partitions,
                    "{topic} went from {partitions} to {now} partitions; restarting to reassign"
                );
            }
        }
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                metrics.events_consumed.fetch_add(1, Ordering::Relaxed);
                let ts = message.timestamp().to_millis().unwrap_or_else(now_ms);
                let Some((user, mut json)) = message
                    .payload()
                    .and_then(|payload| event_json_at(payload, ts))
                else {
                    continue;
                };
                if shard.is_some() {
                    // The resume position (`after=`): one user's events are
                    // all in this partition, in this order.
                    json.pop();
                    json.push_str(&format!(",\"cursor\":{}}}", message.offset()));
                }
                let json: Arc<str> = json.into();
                let position = (ts, message.partition(), message.offset());
                if registry.record_and_push(user, position, &json, &metrics) > 0 {
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
                log_warn!(
                    "user-stream",
                    "event=user_topic_unavailable topic={target} error={error}"
                );
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    consumer
        .subscribe(&[&source])
        .expect("subscribe execution topic");
    log_info!(
        "user-stream",
        "routing {source} -> {target} ({partitions} partitions), group {group}"
    );
    let batch_max = 10_000;
    let mut last_partition_check = std::time::Instant::now();
    loop {
        if last_partition_check.elapsed() >= Duration::from_secs(30) {
            last_partition_check = std::time::Instant::now();
            if let Ok(now) = topic_partitions(&consumer, &target) {
                assert_eq!(
                    now, partitions,
                    "{target} went from {partitions} to {now} partitions; restarting to re-key users"
                );
            }
        }
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
            batch
                .iter()
                .zip(&keys)
                .filter_map(|((_, _, ts, payload), key)| {
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
            first
                .entry(*partition)
                .and_modify(|o| *o = (*o).min(*offset))
                .or_insert(*offset);
            next.entry(*partition)
                .and_modify(|o| *o = (*o).max(offset + 1))
                .or_insert(offset + 1);
        }
        if deliveries.iter().all(Result::is_ok) {
            metrics
                .events_routed
                .fetch_add(deliveries.len() as u64, Ordering::Relaxed);
            let mut list = TopicPartitionList::new();
            for (partition, offset) in next {
                let _ = list.add_partition_offset(&source, partition, Offset::Offset(offset));
            }
            if let Err(error) = consumer.commit(&list, CommitMode::Async) {
                log_warn!("user-stream", "event=router_commit_failed error={error}");
            }
            continue;
        }
        log_warn!(
            "user-stream",
            "event=router_batch_retry records={}",
            batch.len()
        );
        for (partition, offset) in first {
            let mut attempt = 0;
            while let Err(error) = consumer.seek(
                &source,
                partition,
                Offset::Offset(offset),
                Duration::from_secs(1),
            ) {
                let assigned = consumer.assignment().map(|list| {
                    list.elements()
                        .iter()
                        .any(|tp| tp.topic() == source && tp.partition() == partition)
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
    // The request must arrive promptly and stay small: a client trickling
    // or streaming header bytes would otherwise hold a thread (and memory)
    // forever.
    stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT)).ok();
    stream.set_write_timeout(Some(WRITE_TIMEOUT)).ok();
    let Ok(mut writer) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut head = (&mut reader).take(MAX_REQUEST_HEAD);
    let mut first = String::new();
    if head.read_line(&mut first).is_err() || !first.ends_with('\n') {
        return;
    }
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let uri = parts.next().unwrap_or("/");
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let (mut authorized, mut upgrade, mut key) = (false, false, None);
    loop {
        let mut header = String::new();
        match head.read_line(&mut header) {
            Ok(0) | Err(_) => return, // closed, timed out, or too long
            Ok(_) if !header.ends_with('\n') => return,
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
    let after = query_param(query, "after").and_then(|v| v.parse::<i64>().ok());
    let resume = match (&config.shard, after) {
        (Some(shard), Some(cursor)) => Some(Resume::After {
            cursor,
            partition: user_partition(user, shard.partitions),
        }),
        (None, Some(_)) => {
            respond(
                &mut writer,
                "400 Bad Request",
                "after= needs a sharded (pusher) instance; use since=",
            );
            return;
        }
        (_, None) => query_param(query, "since")
            .and_then(|v| v.parse::<i64>().ok())
            .map(Resume::Since),
    };
    let (tx, rx) = mpsc::channel::<Outgoing>();
    let Some(conn) = registry.subscribe_from(
        user,
        (tx.clone(), config.queue),
        config.max_per_user,
        resume,
    ) else {
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
    // From here a silent peer is a dead one: the writer pings every
    // PING_EVERY, so a live client always has something to answer.
    reader.get_ref().set_read_timeout(Some(IDLE_TIMEOUT)).ok();

    // Reader: client pings/closes become frames for the writer to send.
    let control = tx;
    let spawned = std::thread::Builder::new()
        .name("ws-reader".into())
        .spawn(move || {
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
    let Ok(reader_thread) = spawned else {
        registry.unsubscribe(user, conn);
        metrics.connections.fetch_sub(1, Ordering::Relaxed);
        return;
    };
    let depth = registry.depth_of(user, conn);
    let mut last_sent = std::time::Instant::now();
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
                if last_sent.elapsed() >= PING_EVERY {
                    if websocket::write_frame(&mut writer, websocket::OP_PING, b"").is_err() {
                        break;
                    }
                    last_sent = std::time::Instant::now();
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        last_sent = std::time::Instant::now();
        let result = match frame {
            Outgoing::Text(json) => {
                if let Some(depth) = &depth {
                    depth.fetch_sub(1, Ordering::AcqRel);
                }
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
        .is_some_and(|conns| conns.iter().any(|c| c.id == conn))
}

/// Request line plus headers of a connection, at most.
const MAX_REQUEST_HEAD: u64 = 16 * 1024;
/// Time allowed for the whole HTTP request (per read).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// A frame that cannot be written in this long means a peer that stopped
/// reading: drop it rather than pin the writer thread.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// The writer pings an otherwise idle connection this often...
const PING_EVERY: Duration = Duration::from_secs(30);
/// ...and a connection the client has sent nothing on (not even the pong)
/// for this long is dead (vanished mobile network, dropped NAT flow).
const IDLE_TIMEOUT: Duration = Duration::from_secs(75);

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
        let count: u32 = env("TC_USER_STREAM_SHARDS", "1")
            .parse()
            .expect("TC_USER_STREAM_SHARDS");
        let index: u32 = env("TC_USER_STREAM_SHARD", "0")
            .parse()
            .expect("TC_USER_STREAM_SHARD");
        assert!(
            count >= 1 && index < count,
            "TC_USER_STREAM_SHARD must be below TC_USER_STREAM_SHARDS"
        );
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
                    log_warn!(
                        "user-stream",
                        "event=user_topic_unavailable topic={user_topic} error={error}"
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        };
        assert!(
            partitions >= count,
            "{user_topic} has {partitions} partitions, fewer than {count} shards"
        );
        Shard {
            index,
            count,
            partitions,
            peers,
        }
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
    let mut registry = Registry::with_replay(
        env("TC_USER_STREAM_REPLAY_EVENTS", "512")
            .parse()
            .unwrap_or(512),
        env("TC_USER_STREAM_REPLAY_SECS", "300")
            .parse()
            .unwrap_or(300),
        env("TC_USER_STREAM_REPLAY_MAX_TOTAL", "2000000")
            .parse()
            .unwrap_or(2_000_000),
    );
    registry.since_grace_ms = env("TC_USER_STREAM_SINCE_GRACE_MS", "10000")
        .parse()
        .unwrap_or(10_000);
    let registry = Arc::new(registry);
    let metrics = Arc::new(Metrics::default());
    if role == "router" {
        let metrics = metrics.clone();
        let group =
            std::env::var("TC_USER_STREAM_GROUP").unwrap_or_else(|_| "user-stream-router".into());
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
    // Every connection costs two threads; past the limit (or when the OS
    // refuses a thread) new ones are closed at once instead of aborting.
    let max_connections: usize = env("TC_USER_STREAM_MAX_CONNECTIONS", "50000")
        .parse()
        .unwrap_or(50_000);
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= max_connections {
            continue; // dropping `stream` closes it
        }
        active.fetch_add(1, Ordering::AcqRel);
        let (config, registry, metrics, done) = (
            config.clone(),
            registry.clone(),
            metrics.clone(),
            active.clone(),
        );
        let spawned = std::thread::Builder::new()
            .name("ws-conn".into())
            .spawn(move || {
                handle(stream, &config, &registry, &metrics);
                done.fetch_sub(1, Ordering::AcqRel);
            });
        if spawned.is_err() {
            active.fetch_sub(1, Ordering::AcqRel);
        }
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
        let id = |json: &str| {
            json.split("\"event_id\":\"")
                .nth(1)
                .unwrap()
                .split('"')
                .next()
                .unwrap()
                .to_string()
        };
        assert_ne!(id(&taker), id(&maker));
        let (_, stamped) = event_json_at(&payload(&trade, 1), 1_700_000_000_123).unwrap();
        assert!(stamped.ends_with(r#","ts":1700000000123}"#), "{stamped}");
    }

    fn texts(rx: &mpsc::Receiver<Outgoing>) -> Vec<String> {
        rx.try_iter()
            .map(|frame| match frame {
                Outgoing::Text(json) => json.to_string(),
                _ => unreachable!(),
            })
            .collect()
    }

    fn at(ts: i64, offset: i64) -> (i64, i32, i64) {
        (ts, 0, offset)
    }

    #[test]
    fn a_reconnect_replays_events_since_its_last_timestamp_then_goes_live() {
        let mut registry = Registry::with_replay(3, 60, 1_000);
        registry.since_grace_ms = 0;
        registry.set_floor(0, &[(0, 0)]);
        let metrics = Metrics::default();
        let text = |n: i64| -> Arc<str> { format!("e{n}").into() };
        for ts in 1..=4 {
            registry.record_and_push(7, at(ts * 1000, ts), &text(ts), &metrics);
        }
        // Only the last 3 are kept; `since` picks those at or after it.
        let (tx, rx) = mpsc::channel();
        registry
            .subscribe_from(7, (tx, 16), 4, Some(Resume::Since(3_000)))
            .unwrap();
        registry.record_and_push(7, at(5_000, 5), &text(5), &metrics);
        assert_eq!(
            texts(&rx),
            ["e3", "e4", "e5"],
            "replay, then live, in order"
        );
        // Events older than the window are forgotten.
        registry.prune(5_000 + 61_000);
        let history = registry.history.lock().unwrap();
        assert!(history.order.is_empty() && history.kept == 0);
        assert!(history.users.values().all(|entry| entry.events.is_empty()));
    }

    #[test]
    fn since_reaches_back_a_grace_period_for_late_arrivals() {
        let registry = Registry::with_replay(100, 600, 1_000); // grace 10 s
        registry.set_floor(0, &[(0, 0)]);
        let metrics = Metrics::default();
        // The client saw B (ts 20 s); A (ts 15 s) arrived only afterwards.
        registry.record_and_push(7, at(20_000, 1), &"B".into(), &metrics);
        registry.record_and_push(7, at(15_000, 2), &"A".into(), &metrics);
        let (tx, rx) = mpsc::channel();
        registry
            .subscribe_from(7, (tx, 16), 4, Some(Resume::Since(20_000)))
            .unwrap();
        assert_eq!(
            texts(&rx),
            ["B", "A"],
            "A is replayed; B again is deduped by event_id"
        );
    }

    #[test]
    fn a_cursor_resumes_exactly_and_a_lost_range_says_resync() {
        let registry = Registry::with_replay(3, 600, 1_000);
        registry.set_floor(0, &[(0, 10)]); // loading began at offset 10
        let metrics = Metrics::default();
        for offset in 10..15 {
            registry.record_and_push(
                7,
                at(1_000 + offset, offset),
                &format!("o{offset}").into(),
                &metrics,
            );
        }
        // Kept: 12, 13, 14 (per-user limit 3); 10 and 11 were evicted.
        let resume = |cursor| {
            let (tx, rx) = mpsc::channel();
            registry
                .subscribe_from(
                    7,
                    (tx, 16),
                    64,
                    Some(Resume::After {
                        cursor,
                        partition: 0,
                    }),
                )
                .unwrap();
            texts(&rx)
        };
        assert_eq!(resume(12), ["o13", "o14"], "exactly after the cursor");
        assert_eq!(resume(11), ["o12", "o13", "o14"]);
        let resync = resume(10);
        assert!(resync[0].contains("RESYNC"), "11 was evicted: {resync:?}");
        assert_eq!(&resync[1..], ["o12", "o13", "o14"]);
        // Before the startup load: never had them.
        let mut history = registry.history.lock().unwrap();
        history.users.get_mut(&7).unwrap().lost_offset = i64::MIN;
        drop(history);
        assert!(resume(5)[0].contains("RESYNC"));
        // More to replay than the queue holds: resync instead of a partial replay.
        let (tx, rx) = mpsc::channel();
        registry
            .subscribe_from(
                7,
                (tx, 2),
                64,
                Some(Resume::After {
                    cursor: 11,
                    partition: 0,
                }),
            )
            .unwrap();
        let got = texts(&rx);
        assert_eq!(got.len(), 1);
        assert!(got[0].contains("RESYNC"));
    }

    #[test]
    fn the_replay_buffer_keeps_at_most_the_total_limit_oldest_out_first() {
        let registry = Registry::with_replay(2, 600, 4);
        let metrics = Metrics::default();
        let text = |n: i64| -> Arc<str> { format!("e{n}").into() };
        // User 1 is chatty (per-user limit 2), users 2..4 add one each.
        for ts in 1..=5 {
            registry.record_and_push(1, at(ts, ts), &text(ts), &metrics);
        }
        for (user, ts) in [(2, 6), (3, 7), (4, 8)] {
            registry.record_and_push(user, at(ts, ts), &text(ts), &metrics);
        }
        let history = registry.history.lock().unwrap();
        assert_eq!(history.kept, 4);
        assert!(history.order.len() <= 8);
        let kept = |user| -> Vec<i64> {
            history
                .users
                .get(&user)
                .map_or(vec![], |e| e.events.iter().map(|k| k.ts).collect())
        };
        // Over the total: user 1's oldest kept event (4) went first.
        assert_eq!(kept(1), [5]);
        assert_eq!((kept(2), kept(3), kept(4)), (vec![6], vec![7], vec![8]));
        assert_eq!(history.floor_ts, 4, "evicted for everyone up to ts 4");
        drop(history);
        // Bounded under a long stream from one user, too.
        for ts in 10..10_000 {
            registry.record_and_push(9, at(ts, ts), &text(ts), &metrics);
        }
        let history = registry.history.lock().unwrap();
        assert!(
            history.kept <= 4 && history.order.len() <= 8,
            "{} {}",
            history.kept,
            history.order.len()
        );
    }

    #[test]
    fn users_spread_evenly_and_each_belongs_to_exactly_one_shard() {
        let partitions = 64;
        let mut per_partition = vec![0u32; partitions as usize];
        for user in 1_000_000..1_064_000u64 {
            per_partition[user_partition(user, partitions) as usize] += 1;
        }
        let (min, max) = (
            *per_partition.iter().min().unwrap(),
            *per_partition.iter().max().unwrap(),
        );
        assert!(min > 850 && max < 1150, "skewed: {min}..{max}");
        let shards: Vec<Shard> = (0..3)
            .map(|index| Shard {
                index,
                count: 3,
                partitions,
                peers: vec![],
            })
            .collect();
        for user in [1u64, 42, 1_000_003, u64::MAX] {
            let partition = user_partition(user, partitions);
            let owners: Vec<u32> = shards
                .iter()
                .filter(|s| s.owns_partition(partition))
                .map(|s| s.index)
                .collect();
            assert_eq!(owners, [shards[0].owner(user)], "user {user}");
        }
    }

    #[test]
    fn registry_routes_to_the_user_and_drops_slow_connections() {
        let registry = Registry::default();
        let metrics = Metrics::default();
        let (fast_tx, fast_rx) = mpsc::channel();
        let (slow_tx, _slow_rx) = mpsc::channel();
        let fast = registry.subscribe(1, (fast_tx, 8), 4).unwrap();
        let slow = registry.subscribe(1, (slow_tx, 1), 4).unwrap();
        registry.subscribe(2, (mpsc::channel().0, 8), 4).unwrap();

        let json: Arc<str> = "{}".into();
        assert_eq!(registry.push(1, &json, &metrics), 2);
        // The slow connection's single slot is now full: the next push drops it.
        assert_eq!(registry.push(1, &json, &metrics), 1);
        assert_eq!(metrics.slow_clients_dropped.load(Ordering::Relaxed), 1);
        assert!(registry_has(&registry, 1, fast));
        assert!(!registry_has(&registry, 1, slow));
        assert_eq!(fast_rx.try_iter().count(), 2);
        assert_eq!(registry.push(3, &json, &metrics), 0, "no subscribers");
        // The writer's dequeue frees a slot.
        let depth = registry.depth_of(1, fast).unwrap();
        assert_eq!(depth.load(Ordering::Acquire), 2);

        // Per-user connection cap.
        let limited = Registry::default();
        assert!(limited.subscribe(5, (mpsc::channel().0, 1), 1).is_some());
        assert!(limited.subscribe(5, (mpsc::channel().0, 1), 1).is_none());
    }
}
