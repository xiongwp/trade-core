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
        let next = self
            .offsets
            .entry((topic.to_string(), partition))
            .or_insert(0);
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
        // Never move a committed offset back. A partition that went to
        // another consumer and came back keeps an old entry here, and the
        // other consumer may have committed further meanwhile: re-committing
        // ours would redeliver everything in between on the next restart.
        let mut probe = TopicPartitionList::new();
        for (topic, partition) in self.offsets.keys() {
            let _ = probe.add_partition(topic, *partition);
        }
        let committed: HashMap<(String, i32), i64> =
            match consumer.committed_offsets(probe, Duration::from_secs(5)) {
                Ok(list) => list
                    .elements()
                    .iter()
                    .filter_map(|element| match element.offset() {
                        Offset::Offset(offset) => {
                            Some(((element.topic().to_string(), element.partition()), offset))
                        }
                        _ => None,
                    })
                    .collect(),
                Err(_) => return, // retried on the next idle tick
            };
        self.offsets
            .retain(|key, offset| committed.get(key).is_none_or(|current| *offset > *current));
        let mut list = TopicPartitionList::new();
        for ((topic, partition), offset) in &self.offsets {
            if list
                .add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .is_err()
            {
                return;
            }
        }
        if list.count() == 0 {
            self.dirty = false;
            return;
        }
        match consumer.commit(&list, CommitMode::Sync) {
            Ok(()) => {
                // Committed: forget them, so no entry outlives its partition.
                self.offsets.clear();
                self.dirty = false;
            }
            Err(error) => {
                if self
                    .last_failure_log
                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(30))
                {
                    log_warn!(
                        "order-kafka",
                        "event=idle_commit_failed partitions={} error={error}",
                        self.offsets.len()
                    );
                    self.last_failure_log = Some(std::time::Instant::now());
                }
            }
        }
    }
}

pub(crate) fn rewind_kafka_batch(consumer: &BaseConsumer, records: &[KafkaRecord]) {
    rewind_to_earliest(
        consumer,
        records
            .iter()
            .map(|record| (record.topic.as_str(), record.partition, record.offset)),
    );
}

/// Seek every partition of a failed batch back to its first record. If a
/// seek is lost the consumer keeps fetching past the batch, and the next
/// successful batch commits an offset beyond it: the failed records would be
/// skipped for good. A partition that is no longer assigned is fine (its new
/// owner resumes from the committed offset, which is before this batch).
/// Otherwise, after retrying, stop the process: a restart resumes from the
/// committed offsets, which never pass an unprocessed batch.
pub(crate) fn rewind_to_earliest<'a>(
    consumer: &BaseConsumer,
    records: impl Iterator<Item = (&'a str, i32, i64)>,
) {
    let mut offsets: HashMap<(&str, i32), i64> = HashMap::new();
    for (topic, partition, offset) in records {
        offsets
            .entry((topic, partition))
            .and_modify(|first| *first = (*first).min(offset))
            .or_insert(offset);
    }
    for ((topic, partition), offset) in offsets {
        let mut attempt = 0;
        loop {
            let Err(error) = consumer.seek(
                topic,
                partition,
                Offset::Offset(offset),
                Duration::from_secs(1),
            ) else {
                break;
            };
            let assigned = consumer.assignment().map(|list| {
                list.elements()
                    .iter()
                    .any(|tp| tp.topic() == topic && tp.partition() == partition)
            });
            if assigned == Ok(false) {
                break;
            }
            attempt += 1;
            if attempt >= 5 {
                panic!(
                    "cannot rewind {topic}[{partition}] to offset {offset} after a failed batch \
                     ({error}); restarting from committed offsets instead of skipping it"
                );
            }
            log_warn!("order-kafka", "event=rewind_retry topic={topic} partition={partition} offset={offset} error={error}");
            std::thread::sleep(Duration::from_millis(200 * attempt));
        }
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

/// Refuse to start a consumer whose committed position would silently lose
/// or re-run data — `auto.offset.reset` hides both:
/// - committed offset below the partition's low watermark: retention deleted
///   messages this group never processed;
/// - no committed offset although the partition's history is already
///   truncated: the group's offsets were lost (e.g. expired while it was
///   down), and restarting from the earliest message would re-run commands
///   beyond the matching dedupe window.
/// `TC_KAFKA_OFFSET_PREFLIGHT=warn` only logs (for a deliberate reset).
pub(crate) fn preflight_offsets(consumer: &BaseConsumer, group: &str, partitions: &[(String, i32)]) {
    if partitions.is_empty() {
        return;
    }
    let enforce = std::env::var("TC_KAFKA_OFFSET_PREFLIGHT").map_or(true, |mode| mode != "warn");
    let mut list = TopicPartitionList::new();
    for (topic, partition) in partitions {
        list.add_partition(topic, *partition);
    }
    let committed = loop {
        match consumer.committed_offsets(list.clone(), Duration::from_secs(10)) {
            Ok(committed) => break committed,
            Err(error) => {
                log_warn!("order-kafka", "event=offset_preflight_retry group={group} error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    let mut problems = Vec::new();
    for element in committed.elements() {
        let (topic, partition) = (element.topic(), element.partition());
        let low = loop {
            match consumer.fetch_watermarks(topic, partition, Duration::from_secs(10)) {
                Ok((low, _high)) => break low,
                Err(error) => {
                    log_warn!("order-kafka", "event=offset_preflight_retry group={group} error={error}");
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        };
        match element.offset() {
            Offset::Offset(offset) if offset < low => problems.push(format!(
                "{topic}[{partition}]: committed {offset} < low watermark {low} ({} messages deleted unprocessed)",
                low - offset
            )),
            Offset::Offset(_) => {}
            _ if low > 0 => problems.push(format!(
                "{topic}[{partition}]: no committed offset but history starts at {low} (offsets lost)"
            )),
            _ => {}
        }
    }
    if problems.is_empty() {
        return;
    }
    for problem in &problems {
        log_error!("order-kafka", "event=offset_preflight_failed group={group} {problem}");
    }
    if enforce {
        panic!(
            "consumer group {group}: {} partition(s) would silently skip or re-run data (see the log); \
             set TC_KAFKA_OFFSET_PREFLIGHT=warn to start anyway",
            problems.len()
        );
    }
}

/// Every partition of `topics`, from broker metadata.
pub(crate) fn topic_partitions(consumer: &BaseConsumer, topics: &[&str]) -> Vec<(String, i32)> {
    let mut partitions = Vec::new();
    for topic in topics {
        let metadata = loop {
            match consumer.fetch_metadata(Some(topic), Duration::from_secs(10)) {
                Ok(metadata) => break metadata,
                Err(error) => {
                    log_warn!("order-kafka", "event=metadata_retry topic={topic} error={error}");
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        };
        for topic_metadata in metadata.topics() {
            for partition in topic_metadata.partitions() {
                partitions.push((topic_metadata.name().to_string(), partition.id()));
            }
        }
    }
    partitions
}

pub(crate) fn assigned_partitions(list: &TopicPartitionList) -> Vec<(String, i32)> {
    list.elements()
        .iter()
        .map(|element| (element.topic().to_string(), element.partition()))
        .collect()
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
    preflight_offsets(&consumer, &kafka.db_group, &assigned_partitions(&assignment));
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
    F: FnMut(&[KafkaRecord]) -> Result<PartitionFailures, String>,
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
    preflight_offsets(&consumer, &group_id, &topic_partitions(&consumer, &topics));
    consumer.subscribe(&topics).expect("subscribe order topics");
    log_info!(
        "order-kafka", format_args!("{stage}-{worker}");
        "group={group_id} subscribed to {} queue groups",
        topics.len(),
    );
    let mut consecutive_failures = 0u32;
    let mut last_failure_log = std::time::Instant::now() - Duration::from_secs(30);
    let mut partition_retries = HashMap::<PartitionKey, PartitionRetry>::new();

    let mut idle_commit = IdleCommit::default();
    loop {
        resume_due_partitions(&consumer, &mut partition_retries);
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
                Err(error) => dead_letter_command(stage, &message, &error),
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
                        Err(error) => dead_letter_command(stage, &message, &error),
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
        match process(&batch) {
            Ok(failures) if failures.is_empty() => {
                if let Err(error) =
                    commit_kafka_batch(&consumer, &batch, kafka.async_offset_commits)
                {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    log_retry(
                        stage,
                        worker,
                        consecutive_failures,
                        &mut last_failure_log,
                        &error,
                    );
                    rewind_kafka_batch(&consumer, &batch);
                    let shift = consecutive_failures.saturating_sub(1).min(6);
                    std::thread::sleep(Duration::from_millis((100u64 << shift).min(5_000)));
                    continue;
                }
                idle_commit.record(&batch);
                clear_partition_retries(&batch, &mut partition_retries);
                kafka.metrics.complete(stage, batch.len() as u64);
                consecutive_failures = 0;
            }
            Ok(failures) => {
                let (completed, failed): (Vec<_>, Vec<_>) =
                    batch.iter().cloned().partition(|record| {
                        !failures.contains_key(&(record.topic.clone(), record.partition))
                    });
                rewind_kafka_batch(&consumer, &failed);
                pause_failed_partitions(&consumer, &failures, &mut partition_retries);
                if !completed.is_empty() {
                    match commit_kafka_batch(&consumer, &completed, kafka.async_offset_commits) {
                        Ok(()) => {
                            idle_commit.record(&completed);
                            clear_partition_retries(&completed, &mut partition_retries);
                            kafka.metrics.complete(stage, completed.len() as u64);
                        }
                        Err(error) => {
                            // The healthy partitions were processed but their
                            // commit failed. Rewind them too; downstream
                            // command IDs make the replay idempotent.
                            rewind_kafka_batch(&consumer, &completed);
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            log_retry(
                                stage,
                                worker,
                                consecutive_failures,
                                &mut last_failure_log,
                                &error,
                            );
                        }
                    }
                }
                if last_failure_log.elapsed() >= Duration::from_secs(30) {
                    let first = failures
                        .values()
                        .next()
                        .map(String::as_str)
                        .unwrap_or("unknown");
                    log_warn!(
                        "order-kafka", format_args!("{stage}-{worker}");
                        "event=partitions_paused failed={} completed={} error={first}",
                        failures.len(),
                        completed.len()
                    );
                    last_failure_log = std::time::Instant::now();
                }
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                log_retry(
                    stage,
                    worker,
                    consecutive_failures,
                    &mut last_failure_log,
                    &error,
                );
                rewind_kafka_batch(&consumer, &batch);
                let shift = consecutive_failures.saturating_sub(1).min(6);
                let backoff_ms = (100u64 << shift).min(5_000);
                std::thread::sleep(Duration::from_millis(backoff_ms));
            }
        }
    }
}

fn log_retry(
    stage: &str,
    worker: usize,
    failures: u32,
    last_log: &mut std::time::Instant,
    error: &str,
) {
    if failures == 1 || last_log.elapsed() >= Duration::from_secs(30) {
        log_warn!(
            "order-kafka", format_args!("{stage}-{worker}");
            "event=batch_retained_for_retry failures={failures} error={error}"
        );
        *last_log = std::time::Instant::now();
    }
}

#[derive(Clone, Copy, Debug)]
struct PartitionRetry {
    attempts: u32,
    resume_at: Option<std::time::Instant>,
}

fn schedule_partition_retry(
    retries: &mut HashMap<PartitionKey, PartitionRetry>,
    key: PartitionKey,
    now: std::time::Instant,
) -> Duration {
    let attempt = retries
        .get(&key)
        .map_or(1, |state| state.attempts.saturating_add(1));
    let delay = Duration::from_millis((100u64 << attempt.saturating_sub(1).min(6)).min(5_000));
    retries.insert(
        key,
        PartitionRetry {
            attempts: attempt,
            resume_at: Some(now + delay),
        },
    );
    delay
}

fn clear_partition_retries(
    records: &[KafkaRecord],
    retries: &mut HashMap<PartitionKey, PartitionRetry>,
) {
    for record in records {
        retries.remove(&(record.topic.clone(), record.partition));
    }
}

fn pause_failed_partitions(
    consumer: &BaseConsumer,
    failures: &PartitionFailures,
    retries: &mut HashMap<PartitionKey, PartitionRetry>,
) {
    let mut partitions = TopicPartitionList::new();
    for ((topic, partition), _) in failures {
        partitions.add_partition(topic, *partition);
        schedule_partition_retry(
            retries,
            (topic.clone(), *partition),
            std::time::Instant::now(),
        );
    }
    if let Err(error) = consumer.pause(&partitions) {
        log_warn!("order-kafka", "event=partition_pause_failed error={error}");
    }
}

fn resume_due_partitions(
    consumer: &BaseConsumer,
    retries: &mut HashMap<PartitionKey, PartitionRetry>,
) {
    let now = std::time::Instant::now();
    let due = retries
        .iter()
        .filter(|(_, state)| state.resume_at.is_some_and(|resume_at| resume_at <= now))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    if due.is_empty() {
        return;
    }
    let mut partitions = TopicPartitionList::new();
    for (topic, partition) in &due {
        partitions.add_partition(topic, *partition);
    }
    if let Err(error) = consumer.resume(&partitions) {
        log_warn!("order-kafka", "event=partition_resume_failed error={error}");
        return;
    }
    for key in due {
        if let Some(state) = retries.get_mut(&key) {
            // Keep the attempt count until this partition is processed and
            // committed successfully. A persistent group failure therefore
            // backs off exponentially instead of restarting at 100 ms.
            state.resume_at = None;
        }
    }
}

#[cfg(test)]
mod partition_retry_tests {
    use super::*;

    #[test]
    fn retry_backoff_survives_resume_until_success() {
        let key = ("orders".to_string(), 7);
        let now = std::time::Instant::now();
        let mut retries = HashMap::new();

        assert_eq!(
            schedule_partition_retry(&mut retries, key.clone(), now),
            Duration::from_millis(100)
        );
        retries.get_mut(&key).unwrap().resume_at = None;
        assert_eq!(
            schedule_partition_retry(&mut retries, key.clone(), now),
            Duration::from_millis(200)
        );
        assert_eq!(retries[&key].attempts, 2);

        let record = KafkaRecord {
            topic: key.0.clone(),
            partition: key.1,
            offset: 1,
            category_id: 0,
            user: 1,
            frame: [0; wire::MSG_LEN],
        };
        clear_partition_retries(&[record], &mut retries);
        assert!(!retries.contains_key(&key));
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
                kafka.metrics.set_lag_monitor_ready(stage, true);
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
                    kafka.metrics.set_lag_monitor_ready(stage, false);
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
