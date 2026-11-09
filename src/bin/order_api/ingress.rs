//! Order intake: query/batch/WebSocket parsing and the Kafka producer (`KafkaIngress`).

use super::*;

pub(crate) const BATCH_RECORD_LEN: usize = 8 + wire::MSG_LEN;

/// Most orders accepted per WebSocket message (`TC_WS_MAX_BATCH`).
pub(crate) fn ws_max_batch() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| env_number("TC_WS_MAX_BATCH", 5_000usize).max(1))
}

/// Serve one `/ws/orders` connection. Each binary message is a batch of up
/// to `max_batch` records in the `/commands/batch` layout (user `u64` LE +
/// one wire frame); `submit` admits and publishes it exactly like the HTTP
/// batch path. Every message gets one JSON text acknowledgement, in order,
/// carrying its 1-based `seq` on this connection so clients may pipeline:
/// `{"seq":N,"accepted":true,"status":"PENDING","count":C}` (plus
/// `"failed":[{"index","order_id","error"}]` when some records of the batch
/// could not be published; `count` is then the published ones) or
/// `{"seq":N,"accepted":false,"code":429,"error":"..."}` (codes as HTTP).
/// A message above the size limit closes the connection (1009).
pub(crate) fn ws_order_session(
    r: &mut impl Read,
    w: &mut impl Write,
    max_batch: usize,
    mut submit: impl FnMut(&[u8]) -> Result<String, (&'static str, String)>,
) -> std::io::Result<()> {
    use trade_core::websocket::{self, Message};
    let max_len = max_batch * BATCH_RECORD_LEN;
    let mut seq = 0u64;
    loop {
        let message = match websocket::read_message(r, w, max_len) {
            Ok(Some(message)) => message,
            Ok(None) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                let code: u16 = if error.to_string().contains("too large") {
                    1009
                } else {
                    1002
                };
                let _ = websocket::write_frame(w, websocket::OP_CLOSE, &code.to_be_bytes());
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        seq += 1;
        let result = match message {
            Message::Binary(body) => submit(&body),
            Message::Text(_) => Err((
                "400 Bad Request",
                format!(
                    "binary batches only: up to {max_batch} records of {BATCH_RECORD_LEN} bytes"
                ),
            )),
        };
        let ack = match result {
            Ok(accepted) => format!("{{\"seq\":{seq},{accepted}}}"),
            Err((status, error)) => {
                let code = status.split_whitespace().next().unwrap_or("500");
                format!(
                    "{{\"seq\":{seq},\"accepted\":false,\"code\":{code},\"error\":\"{}\"}}",
                    json_escape(&error)
                )
            }
        };
        websocket::write_frame(w, websocket::OP_TEXT, ack.as_bytes())?;
        w.flush()?;
    }
}

/// Prefix for request errors that retrying cannot fix (malformed parameters,
/// instrument-rule violations): answered `400`, not the retryable `503`.
pub(crate) const INVALID_REQUEST: &str = "invalid-request: ";

pub(crate) fn ingress_error_status(error: &str) -> &'static str {
    if error.starts_with(INVALID_REQUEST) {
        "400 Bad Request"
    } else if error.starts_with("backpressure-emergency:") {
        // Emergency tier / lost quorum: stop writes for this category.
        "503 Service Unavailable"
    } else if error.starts_with("backpressure:") {
        // Hard tier or global backlog master switch: throttle.
        "429 Too Many Requests"
    } else {
        "503 Service Unavailable"
    }
}

/// Parse `POST /orders`. `type` is `limit` (default) or `market` (no `price`);
/// `tif` is `gtc` (default), `ioc`, `fok` or `post_only`.
pub(crate) fn order_from_query(query: &str) -> Result<Order, String> {
    let side = match query_param(query, "side") {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err("side must be buy or sell".into()),
    };
    let order_id = OrderId(required(query, "order_id")?);
    let qty = required(query, "qty")?;
    if order_id.0 == 0 {
        return Err("order_id must be non-zero".into());
    }
    if qty == 0 || qty > trade_core::exchange::MAX_ORDER_QTY {
        return Err(format!(
            "qty must be 1..={}",
            trade_core::exchange::MAX_ORDER_QTY
        ));
    }
    let mut order = match query_param(query, "type") {
        None | Some("limit") => Order::limit(order_id, side, required(query, "price")?, qty),
        Some("market") => Order::market(order_id, side, qty),
        Some(_) => return Err("type must be limit or market".into()),
    };
    order.tif = match query_param(query, "tif") {
        None | Some("gtc") => TimeInForce::Gtc,
        Some("ioc") => TimeInForce::Ioc,
        Some("fok") => TimeInForce::Fok,
        Some("post_only") => TimeInForce::PostOnly,
        Some(_) => return Err("tif must be gtc, ioc, fok or post_only".into()),
    };
    if order.tif == TimeInForce::PostOnly && order.order_type == OrderType::Market {
        return Err("post_only requires a limit order".into());
    }
    let order = order
        .on(InstrumentId(required(query, "instrument")?))
        .by(required(query, "user")?);
    // Fail fast on instrument-rule violations, like Binance/OKX answer
    // synchronously. The matching engine enforces the same table
    // authoritatively; this only spares the round trip through Kafka/Raft.
    if let Some(spec) = instrument_specs().get(order.instrument) {
        spec.check(&order)?;
    }
    Ok(order)
}

/// Parse `POST /orders` into the command to publish. With `stop_price` it is
/// a conditional order: `trigger` is `sl` (stop-loss, default) or `tp`
/// (take-profit), held by the matching engine until a trade prints at or
/// through `stop_price`, then entered as the `type`/`tif` order described.
pub(crate) fn order_command_from_query(query: &str) -> Result<trade_core::Command, String> {
    let order = order_from_query(query)?;
    let stop_price: Option<u64> = match query_param(query, "stop_price") {
        None => None,
        Some(_) => Some(required(query, "stop_price")?),
    };
    let Some(stop_price) = stop_price else {
        if query_param(query, "trigger").is_some() {
            return Err("trigger requires stop_price".into());
        }
        return Ok(trade_core::Command::New(order));
    };
    let kind = match query_param(query, "trigger") {
        None | Some("sl") => TriggerKind::StopLoss,
        Some("tp") => TriggerKind::TakeProfit,
        Some(_) => return Err("trigger must be sl or tp".into()),
    };
    if stop_price == 0 {
        return Err("stop_price must be positive".into());
    }
    if order.tif == TimeInForce::PostOnly {
        return Err("post_only cannot be combined with stop_price".into());
    }
    if let Some(spec) = instrument_specs().get(order.instrument) {
        spec.check_amend(stop_price, order.quantity)?;
    }
    Ok(trade_core::Command::NewConditional(
        order,
        Trigger {
            price: stop_price,
            kind,
        },
    ))
}

/// `TC_INSTRUMENT_SPECS` (same format and value as the matching nodes').
pub(crate) fn instrument_specs() -> &'static trade_core::InstrumentSpecs {
    static SPECS: std::sync::OnceLock<trade_core::InstrumentSpecs> = std::sync::OnceLock::new();
    SPECS.get_or_init(|| {
        std::env::var("TC_INSTRUMENT_SPECS")
            .ok()
            .map(|text| {
                trade_core::InstrumentSpecs::parse(&text).expect("valid TC_INSTRUMENT_SPECS")
            })
            .unwrap_or_default()
    })
}

pub(crate) fn cancel_from_query(query: &str) -> Result<(InstrumentId, OrderId, u64, u64), String> {
    Ok((
        InstrumentId(required(query, "instrument")?),
        OrderId(required(query, "order_id")?),
        required(query, "cmd_id")?,
        required(query, "user")?,
    ))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CategorySequence {
    pub(crate) category_id: u32,
    pub(crate) category_seq: u64,
}

#[derive(Clone)]
pub(crate) struct KafkaIngress {
    pub(crate) producer: FutureProducer,
    pub(crate) brokers: String,
    pub(crate) router: QueueRouter,
    pub(crate) partitions_per_topic: u32,
    pub(crate) db_consumers: usize,
    pub(crate) db_router_consumers: usize,
    pub(crate) matcher_consumers: usize,
    pub(crate) db_group: String,
    pub(crate) db_router_group: String,
    pub(crate) db_route_topic: String,
    pub(crate) db_route_partitions: u32,
    pub(crate) matcher_group: String,
    pub(crate) execution_topic: String,
    pub(crate) execution_partitions: u32,
    pub(crate) execution_group: String,
    pub(crate) execution_consumers: usize,
    /// Maximum execution events collected from Kafka before dispatching them
    /// to order-id-routed DB workers.
    pub(crate) execution_batch_size: usize,
    pub(crate) execution_linger: Duration,
    pub(crate) route_control: Arc<trade_core::cluster::RouteControlPlane>,
    /// Raft group id → matching-node admin address serving `/fingerprint`.
    /// When both a migration's source and target groups are present, the
    /// `/admin/routes/caught-up` handler fetches and compares real state
    /// fingerprints itself instead of trusting caller-supplied numbers.
    pub(crate) migration_endpoints: Arc<HashMap<usize, String>>,
    pub(crate) batch_size: usize,
    pub(crate) linger: Duration,
    pub(crate) async_offset_commits: bool,
    pub(crate) max_pipeline_backlog: u64,
    pub(crate) metrics: Arc<OrderPipelineMetrics>,
    /// Per-category three-tier ingress backpressure (production doc §5.3).
    pub(crate) backpressure: Arc<Backpressure>,
}

impl KafkaIngress {
    pub(crate) fn from_env(
        metrics: Arc<OrderPipelineMetrics>,
        raft_group_count: usize,
    ) -> Result<Option<Self>, String> {
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
        let partition_workers = topics.len().saturating_mul(partitions as usize).max(1);
        let raft_group_pins = Arc::new(
            std::env::var("TC_RAFT_CATEGORY_PINS")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(|entry| {
                            let (category, group) = entry.trim().split_once(':')?;
                            Some((category.parse().ok()?, group.parse().ok()?))
                        })
                        .collect::<HashMap<_, _>>()
                })
                .unwrap_or_default(),
        );
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", &brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("max.in.flight.requests.per.connection", "5")
            .set("delivery.timeout.ms", "10000")
            .set(
                "linger.ms",
                env_number("TC_ORDER_KAFKA_LINGER_MS", 1u64).to_string(),
            )
            .set(
                "batch.num.messages",
                env_number("TC_ORDER_KAFKA_BATCH_MESSAGES", 10_000usize).to_string(),
            )
            .set(
                "compression.type",
                std::env::var("TC_ORDER_KAFKA_COMPRESSION").unwrap_or_else(|_| "lz4".into()),
            )
            .set(
                "queue.buffering.max.kbytes",
                env_number("TC_ORDER_KAFKA_QUEUE_KBYTES", 1_048_576usize).to_string(),
            )
            // librdkafka's default (100k messages) is reached by 20 in-flight
            // 5,000-order batches long before the byte limit above.
            .set(
                "queue.buffering.max.messages",
                env_number("TC_ORDER_KAFKA_QUEUE_MESSAGES", 2_000_000usize).to_string(),
            );
        let producer = producer_config
            .create::<FutureProducer>()
            .map_err(|error| error.to_string())?;
        let topic_count = topics.len();
        let route_control = Arc::new(trade_core::cluster::RouteControlPlane::new(
            raft_group_count.max(1),
            (*raft_group_pins).clone(),
        ));
        let backpressure = Arc::new(Backpressure::from_env(
            topic_count,
            partitions,
            raft_group_count,
            (*route_control).clone(),
        ));
        // Optional: group→matching-node address map enabling automatic cutover
        // verification. Absent/empty leaves the legacy behavior where an
        // operator supplies fingerprints to /admin/routes/caught-up.
        let migration_endpoints = Arc::new(
            std::env::var("TC_MIGRATION_GROUP_ENDPOINTS")
                .ok()
                .filter(|spec| !spec.trim().is_empty())
                .map(|spec| {
                    trade_core::migration::parse_group_endpoints(&spec).unwrap_or_else(|error| {
                        log_warn!(
                            "order-api",
                            "ignoring TC_MIGRATION_GROUP_ENDPOINTS: {error}"
                        );
                        HashMap::new()
                    })
                })
                .unwrap_or_default(),
        );
        Ok(Some(Self {
            producer,
            brokers,
            router: QueueRouter::new(topics, partitions, version),
            partitions_per_topic: partitions,
            db_consumers: env_number(
                "TC_ORDER_KAFKA_DB_CONSUMERS",
                env_number("TC_ORDER_KAFKA_CONSUMERS", partition_workers),
            ),
            db_router_consumers: env_number(
                "TC_ORDER_KAFKA_DB_ROUTER_CONSUMERS",
                partition_workers,
            ),
            matcher_consumers: env_number(
                "TC_ORDER_KAFKA_MATCH_CONSUMERS",
                raft_group_count.max(1),
            ),
            db_group: std::env::var("TC_ORDER_KAFKA_DB_GROUP")
                .unwrap_or_else(|_| "trade-order-persist-v1".into()),
            db_router_group: std::env::var("TC_ORDER_KAFKA_DB_ROUTER_GROUP")
                .unwrap_or_else(|_| "trade-order-db-route-v1".into()),
            db_route_topic: std::env::var("TC_ORDER_DB_KAFKA_TOPIC")
                .unwrap_or_else(|_| "trade-order-db-v1".into()),
            db_route_partitions: env_number(
                "TC_ORDER_DB_KAFKA_PARTITIONS",
                env_number("TC_DB_COUNT", sharding::DB_COUNT as u32),
            ),
            matcher_group: std::env::var("TC_ORDER_KAFKA_MATCH_GROUP")
                .unwrap_or_else(|_| "trade-order-match-v1".into()),
            execution_topic: std::env::var("TC_EXECUTION_KAFKA_TOPIC")
                .unwrap_or_else(|_| "trade-executions-v1".into()),
            execution_partitions: env_number("TC_EXECUTION_KAFKA_PARTITIONS", 16u32),
            execution_group: std::env::var("TC_EXECUTION_KAFKA_MYSQL_GROUP")
                .unwrap_or_else(|_| "trade-order-execution-mysql-v1".into()),
            execution_consumers: env_number("TC_EXECUTION_KAFKA_MYSQL_CONSUMERS", 8usize),
            // The public knob is per physical DB. Collect one DB-wide target
            // from every shard before dispatch, so 10 DBs × 500 rows becomes a
            // roughly 5,000-event Kafka batch and each DB commits near 500.
            execution_batch_size: execution_consumer_batch_size(
                env_number("TC_EXECUTION_MYSQL_BATCH_PER_DB", 500usize),
                env_number("TC_DB_COUNT", sharding::DB_COUNT as usize),
            ),
            execution_linger: Duration::from_millis(env_number(
                "TC_EXECUTION_MYSQL_BATCH_LINGER_MS",
                10u64,
            )),
            route_control,
            migration_endpoints,
            batch_size: env_number("TC_ORDER_BATCH_SIZE", 1_000usize).max(1),
            linger: Duration::from_millis(env_number("TC_ORDER_BATCH_LINGER_MS", 2u64)),
            async_offset_commits: env_enabled("TC_KAFKA_OFFSET_COMMIT_ASYNC", true),
            max_pipeline_backlog: env_number("TC_ORDER_MAX_PIPELINE_BACKLOG", 50_000u64).max(1),
            metrics,
            backpressure,
        }))
    }

    pub(crate) fn reserve(&self, commands: usize) -> Result<(), String> {
        self.metrics
            .try_reserve(commands as u64, self.max_pipeline_backlog)
            .map_err(|backlog| {
                format!(
                    "backpressure: pipeline backlog {backlog} reached limit {}",
                    self.max_pipeline_backlog
                )
            })
    }

    /// Admit a batch by its worst-tier category. A batch is one HTTP request,
    /// so the client owns which categories it groups; per-request granularity
    /// still isolates other categories' *separate* requests, which is the
    /// isolation the design requires.
    pub(crate) fn admit_batch(&self, records: &[BatchRecord]) -> Result<(), String> {
        // Each distinct category once: the route checks take shared locks,
        // and a 5,000-record batch usually spans a handful of categories.
        let mut categories: Vec<u32> = records.iter().map(|record| record.category_id).collect();
        categories.sort_unstable();
        categories.dedup();
        if let Some(category) = categories
            .iter()
            .find(|category| !self.route_control.accepts(**category))
        {
            return Err(format!(
                "backpressure-emergency: category {category} is frozen for route migration"
            ));
        }
        let Some(worst) = categories
            .iter()
            .map(|category| (self.backpressure.tier_for_category(*category), *category))
            .max()
        else {
            return Ok(());
        };
        self.backpressure.admit(worst.1)
    }

    pub(crate) fn publish(
        &self,
        category_id: u32,
        user: u64,
        frame: &[u8; wire::MSG_LEN],
    ) -> Result<CategorySequence, String> {
        // Per-category tier first (soft slowdown / hard 429 / emergency 503),
        // then the market-wide backlog master switch.
        if !self.route_control.accepts(category_id) {
            return Err(format!(
                "backpressure-emergency: category {category_id} is frozen for route migration"
            ));
        }
        self.backpressure.admit(category_id)?;
        self.reserve(1)?;
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
        let offset = match delivery {
            Ok(delivery) => delivery.offset,
            Err((error, _)) => {
                self.metrics.rollback_reservation(1);
                return Err(error.to_string());
            }
        };
        self.metrics.finish_reservation(1, 1);
        Ok(CategorySequence {
            category_id,
            category_seq: (offset as u64).saturating_add(1),
        })
    }

    /// Publish a batch. Each record is delivered on its own, so some may
    /// fail while the rest are already on their way to matching: the batch is
    /// then answered as accepted with the records that did not make it, for
    /// the client to resend (resending one that did land is deduplicated by
    /// command id). `Err` means nothing was published.
    pub(crate) fn publish_batch(
        &self,
        records: &[BatchRecord],
    ) -> Result<Vec<FailedRecord>, String> {
        self.admit_batch(records)?;
        self.reserve(records.len())?;
        let prepared = records
            .iter()
            .map(|record| {
                let route = self.router.route(record.category_id);
                (
                    route.clone(),
                    record.category_id.to_be_bytes(),
                    encode_envelope(record.user, route.version, &record.frame),
                )
            })
            .collect::<Vec<_>>();
        let deliveries = futures::executor::block_on(futures::future::join_all(
            prepared.iter().map(|(route, key, envelope)| {
                self.producer.send(
                    FutureRecord::to(&route.topic)
                        .partition(route.partition)
                        .key(key)
                        .payload(envelope),
                    Duration::from_secs(5),
                )
            }),
        ));
        let failed = deliveries
            .into_iter()
            .enumerate()
            .filter_map(|(index, delivery)| {
                delivery.err().map(|(error, _)| FailedRecord {
                    index,
                    order_id: records[index].order_id,
                    error: error.to_string(),
                })
            })
            .collect::<Vec<_>>();
        self.metrics
            .finish_reservation(records.len() as u64, (records.len() - failed.len()) as u64);
        if failed.len() == records.len() {
            return Err(failed
                .into_iter()
                .next()
                .map_or_else(|| "Kafka batch publish failed".into(), |f| f.error));
        }
        Ok(failed)
    }
}

/// `text` as the inside of a JSON string literal.
pub(crate) fn json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// A batch record that was not published (see [`KafkaIngress::publish_batch`]).
#[derive(Debug)]
pub(crate) struct FailedRecord {
    /// 0-based position in the batch.
    pub(crate) index: usize,
    pub(crate) order_id: u64,
    pub(crate) error: String,
}

/// Acknowledgement body of a batch that was (at least partly) published:
/// `"count"` is how many were published; `"failed"` lists the rest, only
/// when there are any.
pub(crate) fn batch_accepted_json(published: usize, failed: &[FailedRecord]) -> String {
    let mut body = format!("\"accepted\":true,\"status\":\"PENDING\",\"count\":{published}");
    if !failed.is_empty() {
        body.push_str(",\"failed\":[");
        for (i, record) in failed.iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push_str(&format!(
                "{{\"index\":{},\"order_id\":{},\"error\":\"{}\"}}",
                record.index,
                record.order_id,
                json_escape(&record.error)
            ));
        }
        body.push(']');
    }
    body
}

#[derive(Debug)]
pub(crate) struct BatchRecord {
    pub(crate) category_id: u32,
    pub(crate) user: u64,
    pub(crate) order_id: u64,
    pub(crate) frame: [u8; wire::MSG_LEN],
}

pub(crate) fn decode_batch(body: &[u8], category_size: u32) -> Result<Vec<BatchRecord>, String> {
    if body.is_empty() || body.len() % BATCH_RECORD_LEN != 0 {
        return Err(format!(
            "batch body must contain one or more {BATCH_RECORD_LEN}-byte records"
        ));
    }
    let mut records = Vec::with_capacity(body.len() / BATCH_RECORD_LEN);
    for chunk in body.chunks_exact(BATCH_RECORD_LEN) {
        let user = u64::from_le_bytes(chunk[..8].try_into().expect("batch user bytes"));
        let frame: [u8; wire::MSG_LEN] = chunk[8..]
            .try_into()
            .expect("fixed-size batch command frame");
        let command = wire::WireView::parse(&frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| "batch contains an invalid command frame".to_string())?;
        if let trade_core::Command::New(order) | trade_core::Command::NewConditional(order, _) =
            &command
        {
            if order.id.0 == 0
                || order.quantity == 0
                || order.quantity > trade_core::exchange::MAX_ORDER_QTY
            {
                return Err(format!(
                    "{INVALID_REQUEST}order {} needs a non-zero id and a qty of 1..={}",
                    order.id.0,
                    trade_core::exchange::MAX_ORDER_QTY
                ));
            }
            if order.user != user {
                return Err(format!(
                    "batch user {user} does not match order {} user {}",
                    order.id.0, order.user
                ));
            }
        }
        records.push(BatchRecord {
            category_id: sharding::asset_category(command.instrument(), category_size),
            user,
            order_id: database_order_id(&command),
            frame,
        });
    }
    Ok(records)
}
