//! Durable, shared route state for category migration: a single-partition,
//! compacted Kafka topic (`TC_ROUTE_CONTROL_TOPIC`, default
//! `trade-route-control-v1`) keyed by category.
//!
//! Route pins and migration freezes used to live only in one order-api
//! process's memory: a restart dropped a freeze mid-migration, a restart
//! after activation sent the category back to its stale source book, and
//! other instances never learned of either. Now every change is written
//! here first, every instance replays the topic at start (before its
//! consumers run) and follows it afterwards.

use super::*;

pub(crate) fn route_log_topic() -> String {
    std::env::var("TC_ROUTE_CONTROL_TOPIC").unwrap_or_else(|_| "trade-route-control-v1".into())
}

/// One route record: the category belongs to a group, or is frozen while it
/// moves from one group to another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteRecord {
    Pin(usize),
    Freeze { to: usize, from: usize },
}

impl RouteRecord {
    fn encode(self) -> String {
        match self {
            RouteRecord::Pin(group) => format!("pin:{group}"),
            RouteRecord::Freeze { to, from } => format!("freeze:{to}:{from}"),
        }
    }

    fn decode(value: &[u8]) -> Option<Self> {
        let (kind, rest) = std::str::from_utf8(value).ok()?.split_once(':')?;
        match kind {
            "pin" => Some(RouteRecord::Pin(rest.parse().ok()?)),
            "freeze" => {
                let (to, from) = rest.split_once(':')?;
                Some(RouteRecord::Freeze { to: to.parse().ok()?, from: from.parse().ok()? })
            }
            _ => None,
        }
    }

    pub(crate) fn apply(self, control: &trade_core::cluster::RouteControlPlane, category: u32) {
        match self {
            RouteRecord::Pin(group) => control.restore_pin(category, group),
            RouteRecord::Freeze { to, .. } => control.restore_freeze(category, to),
        }
    }
}

/// What this process has read of the route log: each category's latest
/// record with its offset, and the next offset to read. A migration's epoch
/// is its freeze record's offset + 1; a step only proceeds while that freeze
/// is still the category's latest record.
#[derive(Default)]
pub(crate) struct RouteLogView {
    latest: HashMap<u32, (i64, RouteRecord)>,
    next_offset: i64,
}

pub(crate) fn view() -> &'static Mutex<RouteLogView> {
    static VIEW: std::sync::OnceLock<Mutex<RouteLogView>> = std::sync::OnceLock::new();
    VIEW.get_or_init(Default::default)
}

fn observe(category: u32, offset: i64, record: RouteRecord) {
    let mut view = view().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    view.latest.insert(category, (offset, record));
    view.next_offset = view.next_offset.max(offset + 1);
}

/// The category's latest record and its offset, once this process has read
/// up to `through` (waits up to `timeout` for the follower).
pub(crate) fn latest_after(category: u32, through: i64, timeout: Duration) -> Result<Option<(i64, RouteRecord)>, String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        {
            let view = view().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if view.next_offset > through {
                return Ok(view.latest.get(&category).copied());
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("route log not read up to offset {through} within {timeout:?}"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Categories whose latest record is a freeze: a migration that did not
/// finish (its orchestrator stopped). They stay frozen until
/// `/admin/routes/abort` rolls them back.
pub(crate) fn frozen_categories() -> Vec<(u32, i64, RouteRecord)> {
    let view = view().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    view.latest
        .iter()
        .filter(|(_, (_, record))| matches!(record, RouteRecord::Freeze { .. }))
        .map(|(&category, &(offset, record))| (category, offset, record))
        .collect()
}

/// Write a route record and wait for the brokers to acknowledge it; returns
/// its offset.
pub(crate) fn publish(kafka: &KafkaIngress, category: u32, record: RouteRecord) -> Result<i64, String> {
    let topic = route_log_topic();
    let key = category.to_be_bytes();
    let value = record.encode();
    futures::executor::block_on(kafka.producer.send(
        FutureRecord::to(&topic).partition(0).key(&key[..]).payload(value.as_bytes()),
        Duration::from_secs(10),
    ))
    .map(|delivery| delivery.offset)
    .map_err(|(error, _)| format!("route log write failed: {error}"))
}

/// Replay the route log into `kafka.route_control` up to its current end,
/// then keep following it on a background thread (other instances' changes).
/// Retries until the log is readable: running on routes that miss a freeze
/// or a pin would send a moved category to its old, sealed group.
pub(crate) fn load_and_follow(kafka: &KafkaIngress) {
    let topic = route_log_topic();
    let mut attempt = 0u32;
    let (consumer, end) = loop {
        let opened = ClientConfig::new()
            .set("bootstrap.servers", &kafka.brokers)
            .set("group.id", format!("route-log-{}", std::process::id()))
            .set("enable.auto.commit", "false")
            .create::<BaseConsumer>()
            .map_err(|error| error.to_string())
            .and_then(|consumer| {
                let (_, high) = consumer
                    .fetch_watermarks(&topic, 0, Duration::from_secs(10))
                    .map_err(|error| error.to_string())?;
                let mut assignment = TopicPartitionList::new();
                assignment
                    .add_partition_offset(&topic, 0, Offset::Beginning)
                    .and_then(|()| consumer.assign(&assignment))
                    .map_err(|error| error.to_string())?;
                Ok((consumer, high))
            });
        match opened {
            Ok(opened) => break opened,
            Err(error) => {
                attempt += 1;
                log_error!("route-log", "event=unavailable topic={topic} attempt={attempt} error={error}; retrying before taking traffic");
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    };
    let control = kafka.route_control.clone();
    let apply = move |message: &rdkafka::message::BorrowedMessage<'_>| {
        let category = message
            .key()
            .and_then(|key| <[u8; 4]>::try_from(key).ok())
            .map(u32::from_be_bytes);
        let record = message.payload().and_then(RouteRecord::decode);
        if let (Some(category), Some(record)) = (category, record) {
            record.apply(&control, category);
            observe(category, message.offset(), record);
        }
        let mut view = view().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        view.next_offset = view.next_offset.max(message.offset() + 1);
    };
    // Every record up to the end seen at start, however long it takes.
    let mut position = 0;
    while position < end {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                position = message.offset() + 1;
                apply(&message);
            }
            Some(Err(error)) => {
                log_warn!("route-log", "event=read_failed error={error}");
                std::thread::sleep(Duration::from_millis(200));
            }
            None => {}
        }
    }
    {
        let mut view = view().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        view.next_offset = view.next_offset.max(end);
    }
    log_info!("route-log", "event=loaded topic={topic} records={position}");
    for (category, offset, record) in frozen_categories() {
        log_warn!(
            "route-log",
            "event=unfinished_migration category={category} freeze_offset={offset} record={record:?}; \
             it stays frozen until POST /admin/routes/abort?category={category}"
        );
    }
    std::thread::Builder::new()
        .name("route-log".into())
        .spawn(move || {
            while !SHUTDOWN.load(AtomicOrdering::Acquire) {
                if let Some(Ok(message)) = consumer.poll(Duration::from_millis(500)) {
                    apply(&message);
                }
            }
        })
        .expect("spawn route-log follower");
}
