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
/// moves to one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteRecord {
    Pin(usize),
    Freeze(usize),
}

impl RouteRecord {
    fn encode(self) -> String {
        match self {
            RouteRecord::Pin(group) => format!("pin:{group}"),
            RouteRecord::Freeze(group) => format!("freeze:{group}"),
        }
    }

    fn decode(value: &[u8]) -> Option<Self> {
        let (kind, group) = std::str::from_utf8(value).ok()?.split_once(':')?;
        let group = group.parse().ok()?;
        match kind {
            "pin" => Some(RouteRecord::Pin(group)),
            "freeze" => Some(RouteRecord::Freeze(group)),
            _ => None,
        }
    }

    pub(crate) fn apply(self, control: &trade_core::cluster::RouteControlPlane, category: u32) {
        match self {
            RouteRecord::Pin(group) => control.restore_pin(category, group),
            RouteRecord::Freeze(group) => control.restore_freeze(category, group),
        }
    }
}

/// Write a route record and wait for the brokers to acknowledge it.
pub(crate) fn publish(kafka: &KafkaIngress, category: u32, record: RouteRecord) -> Result<(), String> {
    let topic = route_log_topic();
    let key = category.to_be_bytes();
    let value = record.encode();
    futures::executor::block_on(kafka.producer.send(
        FutureRecord::to(&topic).partition(0).key(&key[..]).payload(value.as_bytes()),
        Duration::from_secs(10),
    ))
    .map(|_| ())
    .map_err(|(error, _)| format!("route log write failed: {error}"))
}

/// Replay the route log into `kafka.route_control` up to its current end,
/// then keep following it on a background thread (other instances' changes).
pub(crate) fn load_and_follow(kafka: &KafkaIngress) {
    let topic = route_log_topic();
    let consumer: BaseConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        .set("group.id", format!("route-log-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .create()
    {
        Ok(consumer) => consumer,
        Err(error) => {
            log_error!("route-log", "event=consumer_failed error={error}");
            return;
        }
    };
    let end = match consumer.fetch_watermarks(&topic, 0, Duration::from_secs(10)) {
        Ok((_, high)) => high,
        Err(error) => {
            log_error!("route-log", "event=unavailable topic={topic} error={error}; route changes will not be durable");
            return;
        }
    };
    let mut assignment = TopicPartitionList::new();
    if assignment
        .add_partition_offset(&topic, 0, Offset::Beginning)
        .and_then(|()| consumer.assign(&assignment))
        .is_err()
    {
        log_error!("route-log", "event=assign_failed topic={topic}");
        return;
    }
    let control = kafka.route_control.clone();
    let apply = move |message: &rdkafka::message::BorrowedMessage<'_>| {
        let category = message
            .key()
            .and_then(|key| <[u8; 4]>::try_from(key).ok())
            .map(u32::from_be_bytes);
        let record = message.payload().and_then(RouteRecord::decode);
        if let (Some(category), Some(record)) = (category, record) {
            record.apply(&control, category);
        }
    };
    let mut position = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while position < end && std::time::Instant::now() < deadline {
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(200)) {
            position = message.offset() + 1;
            apply(&message);
        }
    }
    log_info!("route-log", "event=loaded topic={topic} records={position}");
    let _ = std::thread::Builder::new()
        .name("route-log".into())
        .spawn(move || {
            while !SHUTDOWN.load(AtomicOrdering::Acquire) {
                if let Some(Ok(message)) = consumer.poll(Duration::from_millis(500)) {
                    apply(&message);
                }
            }
        });
}
