//! Generic Kafka consumer stages (decode, commit, rewind) and consumer-lag monitoring.

use super::*;

pub(crate) fn decode_kafka_record(
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
        frame: envelope.frame,
    })
}

pub(crate) fn commit_kafka_batch(
    consumer: &BaseConsumer,
    records: &[KafkaRecord],
    async_commit: bool,
) -> Result<(), String> {
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
        .commit(
            &list,
            if async_commit {
                CommitMode::Async
            } else {
                CommitMode::Sync
            },
        )
        .map_err(|error| error.to_string())
}

/// Offsets this consumer has processed, re-committed synchronously once it
/// goes idle. Batch commits are asynchronous and a failed one is not
/// retried: when it was the last batch of a partition (e.g. during a broker
/// leadership change), the committed offset stayed behind for good. The
/// records were projected, so this only caused redelivery on restart, but
/// the group lag stayed above zero indefinitely: it fed backpressure and
/// never let a drain finish. Idle-time only, so throughput is unaffected.
#[derive(Default)]
pub(crate) struct IdleCommit {
    offsets: HashMap<(String, i32), i64>,
    dirty: bool,
    last_failure_log: Option<std::time::Instant>,
}

impl IdleCommit {
    pub(crate) fn record(&mut self, records: &[KafkaRecord]) {
        for record in records {
            self.record_offset(&record.topic, record.partition, record.offset);
        }
    }

    /// Note that `offset` of `topic`/`partition` has been processed.
    pub(crate) fn record_offset(&mut self, topic: &str, partition: i32, offset: i64) {
        let next = self.offsets.entry((topic.to_string(), partition)).or_insert(0);
        *next = (*next).max(offset + 1);
        self.dirty = true;
    }

    pub(crate) fn flush(&mut self, consumer: &BaseConsumer) {
        if !self.dirty {
            return;
        }
        // Only partitions this consumer still owns. After a rebalance moved a
        // partition away, a commit that names it is refused whole, so one
        // stale entry kept every offset of this consumer uncommitted for good
        // (observed: 7 partitions stuck, group lag never draining). The new
        // owner commits a moved partition itself.
        if let Ok(assignment) = consumer.assignment() {
            let owned: std::collections::HashSet<(String, i32)> = assignment
                .elements()
                .iter()
                .map(|element| (element.topic().to_string(), element.partition()))
                .collect();
            self.offsets.retain(|key, _| owned.contains(key));
        }
        let mut list = TopicPartitionList::new();
        for ((topic, partition), offset) in &self.offsets {
            if list
                .add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .is_err()
            {
                return;
            }
        }
        match consumer.commit(&list, CommitMode::Sync) {
            Ok(()) => self.dirty = false,
            Err(error) => {
                if self.last_failure_log.is_none_or(|at| at.elapsed() >= Duration::from_secs(30)) {
                    log_warn!("order-kafka", "event=idle_commit_failed partitions={} error={error}", self.offsets.len());
                    self.last_failure_log = Some(std::time::Instant::now());
                }
            }
        }
    }
}

pub(crate) fn rewind_kafka_batch(consumer: &BaseConsumer, records: &[KafkaRecord]) {
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

pub(crate) fn route_command_db_batch(
    kafka: &KafkaIngress,
    routing: sharding::RouteConfig,
    records: &[KafkaRecord],
) -> Result<(), String> {
    assert!(
        kafka.db_route_partitions >= routing.db_count as u32,
        "command DB route partitions must cover every physical DB"
    );
    let prepared = records
        .iter()
        .map(|record| {
            let command = wire::WireView::parse(&record.frame)
                .and_then(|view| view.to_command())
                .expect("validated command Kafka frame");
            let order_id = database_order_id(&command);
            let partition = routing.execution_partition(order_id, kafka.db_route_partitions);
            let payload = encode_envelope(record.user, routing.route_version, &record.frame);
            let key = order_id.to_be_bytes();
            (partition, key, payload)
        })
        .collect::<Vec<_>>();
    let deliveries = futures::executor::block_on(futures::future::join_all(prepared.iter().map(
        |(partition, key, payload)| {
            kafka.producer.send(
                FutureRecord::to(&kafka.db_route_topic)
                    .partition(*partition as i32)
                    .key(key)
                    .payload(payload),
                Duration::from_secs(5),
            )
        },
    )));
    deliveries
        .into_iter()
        .find_map(Result::err)
        .map_or(Ok(()), |(error, _)| Err(error.to_string()))
}

pub(crate) fn decode_db_kafka_record(
    message: &rdkafka::message::BorrowedMessage<'_>,
    category_size: u32,
    routing: sharding::RouteConfig,
) -> Result<KafkaRecord, String> {
    let payload = message.payload().ok_or("Kafka message has no payload")?;
    let envelope = QueueEnvelope::decode(payload).ok_or("invalid DB command envelope")?;
    if envelope.route_version != routing.route_version {
        return Err(format!(
            "stale DB route version {}, expected {}",
            envelope.route_version, routing.route_version
        ));
    }
    let command = wire::WireView::parse(&envelope.frame)
        .and_then(|view| view.to_command())
        .ok_or("invalid DB command frame")?;
    let expected = routing.execution_partition(
        database_order_id(&command),
        env_number("TC_ORDER_DB_KAFKA_PARTITIONS", routing.db_count as u32),
    );
    if message.partition() != expected as i32 {
        return Err(format!(
            "DB command partition {} does not match expected {expected}",
            message.partition()
        ));
    }
    Ok(KafkaRecord {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
        category_id: sharding::asset_category(command.instrument(), category_size),
        user: envelope.user,
        frame: envelope.frame,
    })
}

pub(crate) fn run_db_kafka_stage(
    kafka: KafkaIngress,
    category_size: u32,
    worker: usize,
    forwarder: CommandDbForwarder,
) {
    let consumer: BaseConsumer = kafka_consumer_config(&kafka.brokers, &kafka.db_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("create routed command DB consumer");
    let usable = kafka.db_route_partitions / forwarder.routing.db_count as u32
        * forwarder.routing.db_count as u32;
    let owned = (0..usable)
        .filter(|partition| forwarder.owns(*partition % forwarder.routing.db_count as u32))
        .collect::<Vec<_>>();
    let mut assignment = TopicPartitionList::new();
    for (position, partition) in owned.into_iter().enumerate() {
        if position % kafka.db_consumers.max(1) == worker {
            assignment
                .add_partition_offset(&kafka.db_route_topic, partition as i32, Offset::Stored)
                .expect("assign routed command DB partition");
        }
    }
    consumer
        .assign(&assignment)
        .expect("assign routed command DB ownership");
    let mut idle_commit = IdleCommit::default();
    loop {
        // Between batches only: a batch in hand is finished and its offsets
        // committed. Returning drops the consumer, which leaves its group so
        // the partitions move to the replacement at once instead of after the
        // session timeout (a ~1 min matching stall after every redeploy).
        if SHUTDOWN.load(AtomicOrdering::SeqCst) {
            return;
        }
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            idle_commit.flush(&consumer);
            continue;
        };
        let mut batch = Vec::with_capacity(kafka.batch_size);
        if let Ok(message) = first {
            match decode_db_kafka_record(&message, category_size, forwarder.routing) {
                Ok(record) => batch.push(record),
                Err(error) => dead_letter_command("db", &message, &error),
            }
        }
        let deadline = std::time::Instant::now() + kafka.linger;
        while batch.len() < kafka.batch_size && std::time::Instant::now() < deadline {
            match consumer.poll(deadline.saturating_duration_since(std::time::Instant::now())) {
                Some(Ok(message)) => {
                    match decode_db_kafka_record(&message, category_size, forwarder.routing) {
                        Ok(record) => batch.push(record),
                        Err(error) => dead_letter_command("db", &message, &error),
                    }
                }
                _ => break,
            }
        }
        if batch.is_empty() {
            continue;
        }
        match forwarder
            .submit(forwarder.routing, &batch)
            .and_then(|()| commit_kafka_batch(&consumer, &batch, kafka.async_offset_commits))
        {
            Ok(()) => {
                idle_commit.record(&batch);
                kafka.metrics.complete("mysql", batch.len() as u64);
            }
            Err(error) => {
                log_warn!("order-db", worker; "event=batch_retry error={error}");
                rewind_kafka_batch(&consumer, &batch);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

pub(crate) fn run_kafka_stage<F>(
    kafka: KafkaIngress,
    category_size: u32,
    group_id: String,
    stage: &'static str,
    worker: usize,
    mut process: F,
) where
    F: FnMut(&[KafkaRecord]) -> Result<(), String>,
{
    // Matching has a much more expensive durability boundary than the command
    // DB projection: each Raft application batch crosses quorum WAL plus the
    // matching WAL/outbox barrier.  Give it an independent, larger microbatch
    // so scaling match throughput does not also inflate MySQL transactions.
    let (batch_size, linger) = if stage == "match" {
        (
            env_number("TC_ORDER_MATCH_BATCH_SIZE", wire::RAFT_BATCH_MAX_COMMANDS)
                .clamp(1, wire::RAFT_BATCH_MAX_COMMANDS),
            Duration::from_millis(env_number(
                "TC_ORDER_MATCH_BATCH_LINGER_MS",
                kafka.linger.as_millis() as u64,
            )),
        )
    } else {
        (kafka.batch_size, kafka.linger)
    };
    let consumer: BaseConsumer = kafka_consumer_config(&kafka.brokers, &group_id)
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
    log_info!(
        "order-kafka", format_args!("{stage}-{worker}");
        "group={group_id} subscribed to {} queue groups",
        topics.len(),
    );
    let mut consecutive_failures = 0u32;
    let mut last_failure_log = std::time::Instant::now() - Duration::from_secs(30);

    let mut idle_commit = IdleCommit::default();
    loop {
        // Between batches only: a batch in hand is finished and its offsets
        // committed. Returning drops the consumer, which leaves its group so
        // the partitions move to the replacement at once instead of after the
        // session timeout (a ~1 min matching stall after every redeploy).
        if SHUTDOWN.load(AtomicOrdering::SeqCst) {
            return;
        }
        let mut batch = Vec::with_capacity(batch_size);
        let Some(first) = consumer.poll(Duration::from_millis(100)) else {
            idle_commit.flush(&consumer);
            continue;
        };
        match first {
            Ok(message) => match decode_kafka_record(&message, &kafka.router, category_size) {
                Ok(record) => batch.push(record),
                Err(error) => {
                    dead_letter_command(stage, &message, &error)
                }
            },
            Err(error) => {
                if last_failure_log.elapsed() >= Duration::from_secs(30) {
                    log_warn!("order-kafka", format_args!("{stage}-{worker}"); "event=poll_failed error={error}");
                    last_failure_log = std::time::Instant::now();
                }
                continue;
            }
        }
        let deadline = std::time::Instant::now() + linger;
        while batch.len() < batch_size && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(message)) => {
                    match decode_kafka_record(&message, &kafka.router, category_size) {
                        Ok(record) => batch.push(record),
                        Err(error) => {
                            dead_letter_command(stage, &message, &error)
                        }
                    }
                }
                Some(Err(error)) => {
                    if last_failure_log.elapsed() >= Duration::from_secs(30) {
                        log_warn!("order-kafka", format_args!("{stage}-{worker}"); "event=poll_failed error={error}");
                        last_failure_log = std::time::Instant::now();
                    }
                }
                None => break,
            }
        }
        match process(&batch)
            .and_then(|()| commit_kafka_batch(&consumer, &batch, kafka.async_offset_commits))
        {
            Ok(()) => {
                idle_commit.record(&batch);
                kafka.metrics.complete(stage, batch.len() as u64);
                consecutive_failures = 0;
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures == 1
                    || last_failure_log.elapsed() >= Duration::from_secs(30)
                {
                    log_warn!(
                        "order-kafka", format_args!("{stage}-{worker}");
                        "event=batch_retained_for_retry failures={consecutive_failures} error={error}"
                    );
                    last_failure_log = std::time::Instant::now();
                }
                rewind_kafka_batch(&consumer, &batch);
                let shift = consecutive_failures.saturating_sub(1).min(6);
                let backoff_ms = (100u64 << shift).min(5_000);
                std::thread::sleep(Duration::from_millis(backoff_ms));
            }
        }
    }
}

pub(crate) fn kafka_consumer_config(brokers: &str, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set(
            "fetch.min.bytes",
            env_number("TC_KAFKA_FETCH_MIN_BYTES", 1usize).to_string(),
        )
        .set(
            "fetch.wait.max.ms",
            env_number("TC_KAFKA_FETCH_WAIT_MS", 10u64).to_string(),
        )
        .set(
            "fetch.message.max.bytes",
            env_number("TC_KAFKA_FETCH_MAX_BYTES", 52_428_800usize).to_string(),
        );
    config
}

/// Per-partition consumer lag, indexed by the router's global partition index
/// (`topic_pos * partitions_per_topic + partition`). Per-category backpressure
/// keys off this; the caller also sums it for the market-wide backlog switch.
pub(crate) fn read_consumer_group_lag(
    consumer: &BaseConsumer,
    topics: &[String],
    per_topic: i32,
) -> Result<Vec<u64>, String> {
    let mut requested = TopicPartitionList::new();
    for topic in topics {
        for partition in 0..per_topic {
            requested.add_partition(topic, partition);
        }
    }
    let committed = consumer
        .committed_offsets(requested, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    let mut committed_by_tp: HashMap<(String, i32), i64> = HashMap::new();
    for element in committed.elements() {
        let offset = match element.offset() {
            Offset::Offset(offset) => offset,
            _ => 0,
        };
        committed_by_tp.insert((element.topic().to_string(), element.partition()), offset);
    }
    let mut lags = Vec::with_capacity(topics.len() * per_topic as usize);
    for topic in topics {
        for partition in 0..per_topic {
            let offset = committed_by_tp
                .get(&(topic.clone(), partition))
                .copied()
                .unwrap_or(0);
            let (_, high) = consumer
                .fetch_watermarks(topic, partition, Duration::from_secs(2))
                .map_err(|error| error.to_string())?;
            lags.push(high.saturating_sub(offset).max(0) as u64);
        }
    }
    Ok(lags)
}

pub(crate) fn run_consumer_lag_monitor(
    kafka: KafkaIngress,
    group_id: String,
    stage: &'static str,
    topics: Vec<String>,
    per_topic: i32,
) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        .set("group.id", &group_id)
        .create()
        .expect("create Kafka lag monitor");
    // Partitions this stage watches; used to zero the per-partition breakdown
    // when the reading goes stale.
    let partition_count = topics.len() * per_topic as usize;
    // After this many consecutive failed reads (~STALE_AFTER * 500ms) the last
    // lag is treated as stale and cleared. Otherwise a Kafka coordinator/leader
    // hiccup freezes the gauge at its last value, and because try_reserve gates
    // on `inflight + observed_lag`, a frozen-high lag silently 429s every new
    // order off phantom backlog. Falling back to local inflight admission is
    // safe: if Kafka is genuinely down the publish path back-pressures at the
    // source, so no unbounded backlog can hide behind the cleared gauge.
    const STALE_AFTER: u32 = 6; // ~3s
    let mut consecutive_failures: u32 = 0;
    let mut last_error_log = std::time::Instant::now() - Duration::from_secs(30);
    loop {
        match read_consumer_group_lag(&consumer, &topics, per_topic) {
            Ok(lags) => {
                consecutive_failures = 0;
                let total: u64 = lags.iter().sum();
                // Global backlog master switch keeps the summed view; the
                // per-category tier machine keeps the per-partition breakdown.
                kafka.metrics.set_observed_lag(stage, total);
                kafka.backpressure.set_partition_lags(stage, &lags);
            }
            Err(error) => {
                kafka
                    .metrics
                    .lag_read_failures
                    .fetch_add(1, AtomicOrdering::Relaxed);
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures == STALE_AFTER {
                    // Drop the now-stale reading once, exactly as it goes stale.
                    kafka.metrics.set_observed_lag(stage, 0);
                    kafka
                        .backpressure
                        .set_partition_lags(stage, &vec![0u64; partition_count]);
                }
                if last_error_log.elapsed() >= Duration::from_secs(30) {
                    log_warn!("order-kafka", format_args!("{stage}-lag"); "event=lag_read_failed group={group_id} error={error} consecutive={consecutive_failures}");
                    last_error_log = std::time::Instant::now();
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}
