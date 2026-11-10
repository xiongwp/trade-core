//! Persistent order system: Kafka is the ordered source of truth and fans out
//! through independent consumer groups to the MySQL query projection and the
//! Raft-backed matcher.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{Pool, Transaction, TxOpts, Value};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use trade_core::journal;
use trade_core::metrics::LatencyHistogram;
use trade_core::order::Order;
use trade_core::order_queue::{encode_envelope, QueueEnvelope, QueueRouter};
use trade_core::sharding::{self, DEFAULT_ASSET_CATEGORY_SIZE};
use trade_core::types::{
    InstrumentId, OrderId, OrderType, Side, TimeInForce, Trigger, TriggerKind,
};
use trade_core::wire;
use trade_core::{log_error, log_info, log_warn};

mod admin;
mod backpressure;
mod command_projection;
mod dedupe_retention;
mod dlq;
mod execution_projection;
mod http;
mod ingress;
mod kafka_stage;
mod order_query;
mod pipeline_metrics;
mod raft_forward;
mod route_log;
mod schema;
mod store;
#[cfg(test)]
mod tests;

use admin::*;
use backpressure::*;
use command_projection::*;
use dedupe_retention::*;
use dlq::*;
use execution_projection::*;
use http::*;
use ingress::*;
use kafka_stage::*;
use order_query::*;
use pipeline_metrics::*;
use raft_forward::*;
use schema::*;
use store::*;

struct HttpConnectionGuard {
    metrics: Arc<OrderPipelineMetrics>,
}

impl Drop for HttpConnectionGuard {
    fn drop(&mut self) {
        self.metrics.close_http_connection();
    }
}

/// Set by the SIGTERM/SIGINT handler (async-signal-safe: a lone atomic store).
/// The HTTP accept loop observes it and stops taking new connections.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, AtomicOrdering::SeqCst);
}

fn install_signal_handlers() {
    let handler = on_signal as *const () as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
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

fn env_enabled(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(default)
}

fn category_size() -> u32 {
    std::env::var("TC_ORDER_CATEGORY_SIZE")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ASSET_CATEGORY_SIZE)
}

fn main() {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("order-api");
    install_signal_handlers();

    let routing = sharding::RouteConfig::from_env();
    let category_size = category_size();
    // Parse the instrument-rule table now: a bad value fails startup, not the
    // first order.
    let _ = instrument_specs();
    let _ = ingress::order_limits();
    let metrics = Arc::new(OrderPipelineMetrics::default());
    let matcher_groups = parse_matcher_groups();
    let token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let kafka = KafkaIngress::from_env(metrics.clone(), matcher_groups.len())
        .expect("configure Kafka order ingress")
        .expect("TC_ORDER_KAFKA_BROKERS is required");
    // Shared route state (pins, migration freezes) before any consumer runs.
    route_log::load_and_follow(&kafka);
    // API ingress and matcher-forwarder roles do not touch MySQL. Requiring
    // every replica to open a pool to every shard creates O(processes*shards)
    // connections and repeats all bootstrap DDL, defeating horizontal scaling.
    let needs_mysql = role_needs_mysql(kafka.db_consumers, kafka.execution_consumers);
    let owned_dbs = Arc::new(
        parse_owned_dbs(routing.db_count as u32)
            .unwrap_or_else(|error| panic!("invalid MySQL ownership: {error}")),
    );
    let store = needs_mysql.then(|| {
        let shard_urls = parse_shard_urls(routing.db_count as usize);
        if let Err(error) = validate_shard_urls(&shard_urls, routing.db_count as usize) {
            panic!("shard routing mismatch: {error}");
        }
        open_when_ready(
            &shard_urls,
            routing,
            metrics.clone(),
            owned_dbs.clone(),
            true,
        )
    });
    // Order queries read every physical DB (one user's orders span all of
    // them). Reuse the projection pools when this worker owns every DB;
    // otherwise open read pools to all of them without bootstrap DDL. Off by
    // default on workers that have no MySQL role, so ingress replicas keep
    // holding no shard connections.
    let query_store = env_enabled("TC_ORDER_QUERY_ENABLED", needs_mysql).then(|| match &store {
        Some(store) if store.owned_dbs.len() as u64 == routing.db_count => store.clone(),
        _ => {
            let shard_urls = parse_shard_urls(routing.db_count as usize);
            if let Err(error) = validate_shard_urls(&shard_urls, routing.db_count as usize) {
                panic!("shard routing mismatch: {error}");
            }
            let all_dbs = Arc::new((0..routing.db_count as u32).collect());
            open_when_ready(&shard_urls, routing, metrics.clone(), all_dbs, false)
        }
    });
    // When this process writes both commands and execution events for its
    // databases, only the execution writer drains pending events (see
    // `DrainHandoff`); a command-only worker keeps draining inline.
    let drain_handoff = (kafka.db_consumers > 0
        && kafka.execution_consumers > 0
        && env_enabled("TC_ORDER_DRAIN_HANDOFF", true))
    .then(|| Arc::new(DrainHandoff::new(routing)));
    let command_db_forwarder = (kafka.db_consumers > 0).then(|| {
        CommandDbForwarder::spawn(
            store.as_ref().expect("DB consumers require MySQL").clone(),
            drain_handoff.clone(),
        )
    });
    let execution_db_forwarder = (kafka.execution_consumers > 0).then(|| {
        ExecutionDbForwarder::spawn(
            store
                .as_ref()
                .expect("execution DB consumers require MySQL")
                .clone(),
            drain_handoff.clone(),
        )
    });
    let matcher_group_count = matcher_groups.len();
    let ingress_enabled = env_enabled("TC_ORDER_HTTP_INGRESS_ENABLED", true);
    let raft_forwarder = (kafka.matcher_consumers > 0)
        .then(|| RaftForwarder::spawn(matcher_groups, kafka.backpressure.clone()));
    let dlq_path =
        std::env::var("TC_ORDER_DLQ_PATH").unwrap_or_else(|_| "order-execution-dlq.wal".into());
    let dlq = Arc::new(Mutex::new(
        DlqWriter::open(Path::new(&dlq_path)).expect("open execution DLQ file"),
    ));
    log_info!(
        "order-api",
        "category_size={} db_count={} tables_per_db={} virtual_dbs={} virtual_tables={} route_version={} raft_groups={} db_router_consumers={} db_consumers={} matcher_consumers={} execution_consumers={} max_pipeline_backlog={} bp_soft={} bp_hard={} bp_emergency={} dlq={} db_group={} matcher_group={} execution_group={} kafka=true",
        category_size,
        routing.db_count,
        routing.tables_per_db,
        routing.virtual_db_count,
        routing.virtual_table_count,
        routing.route_version,
        matcher_group_count,
        kafka.db_router_consumers,
        kafka.db_consumers,
        kafka.matcher_consumers,
        kafka.execution_consumers,
        kafka.max_pipeline_backlog,
        kafka.backpressure.soft,
        kafka.backpressure.hard,
        kafka.backpressure.emergency,
        dlq_path,
        kafka.db_group,
        kafka.matcher_group,
        kafka.execution_group,
    );
    if ingress_enabled {
        // Each stage's consumers commit offsets on DIFFERENT topics: the match
        // group consumes the ingress order topics, the DB-projection group
        // consumes the routed DB topic. The monitor must read each group's lag
        // on the topic that group actually consumes. Reading the persist group
        // against the ingress topics (its committed offset there is always 0)
        // reports the whole ingress high-watermark as phantom MySQL lag, which
        // wedges backpressure permanently once it exceeds max_pipeline_backlog.
        let stages: [(&'static str, String, Vec<String>, i32); 2] = [
            (
                "mysql",
                kafka.db_group.clone(),
                vec![kafka.db_route_topic.clone()],
                kafka.db_route_partitions as i32,
            ),
            (
                "match",
                kafka.matcher_group.clone(),
                kafka.router.topics().to_vec(),
                kafka.partitions_per_topic as i32,
            ),
        ];
        for (stage, group_id, topics, per_topic) in stages {
            let monitor_kafka = kafka.clone();
            std::thread::Builder::new()
                .name(format!("order-kafka-{stage}-lag"))
                .spawn(move || {
                    run_consumer_lag_monitor(monitor_kafka, group_id, stage, topics, per_topic)
                })
                .expect("spawn Kafka lag monitor");
        }
    }
    let mut consumer_threads = Vec::new();
    for worker in 0..kafka.db_router_consumers {
        let router_kafka = kafka.clone();
        let group_id = kafka.db_router_group.clone();
        let handle = std::thread::Builder::new()
            .name(format!("order-kafka-db-route-{worker}"))
            .spawn(move || {
                let process_kafka = router_kafka.clone();
                run_kafka_stage(
                    router_kafka,
                    category_size,
                    group_id,
                    "db-route",
                    worker,
                    move |batch| {
                        route_command_db_batch(&process_kafka, routing, batch)
                            .map(|()| PartitionFailures::new())
                    },
                )
            })
            .expect("spawn command DB route consumer");
        consumer_threads.push(handle);
    }
    for worker in 0..kafka.db_consumers {
        let consumer_forwarder = command_db_forwarder
            .as_ref()
            .expect("DB consumers require shard forwarder")
            .clone();
        let consumer_kafka = kafka.clone();
        let handle = std::thread::Builder::new()
            .name(format!("order-kafka-mysql-{worker}"))
            .spawn(move || {
                run_db_kafka_stage(consumer_kafka, category_size, worker, consumer_forwarder)
            })
            .expect("spawn Kafka MySQL projection consumer");
        consumer_threads.push(handle);
    }
    for worker in 0..kafka.matcher_consumers {
        let consumer_kafka = kafka.clone();
        let group_id = kafka.matcher_group.clone();
        let worker_forwarder = raft_forwarder
            .as_ref()
            .expect("matcher consumers require Raft forwarder")
            .clone();
        let consumer_store_metrics = metrics.clone();
        let route_control = kafka.route_control.clone();
        let handle = std::thread::Builder::new()
            .name(format!("order-kafka-match-{worker}"))
            .spawn(move || {
                run_kafka_stage(
                    consumer_kafka,
                    category_size,
                    group_id,
                    "match",
                    worker,
                    move |batch| {
                        let started = std::time::Instant::now();
                        let result = forward_kafka_batch(&worker_forwarder, batch, &route_control);
                        consumer_store_metrics.record_raft(started.elapsed());
                        result
                    },
                )
            })
            .expect("spawn Kafka matching consumer");
        consumer_threads.push(handle);
    }
    for worker in 0..kafka.execution_consumers {
        let execution_forwarder = execution_db_forwarder
            .as_ref()
            .expect("execution DB consumers require shard forwarder")
            .clone();
        let execution_metrics = metrics.clone();
        let execution_kafka = kafka.clone();
        let execution_dlq = dlq.clone();
        let handle = std::thread::Builder::new()
            .name(format!("execution-kafka-mysql-{worker}"))
            .spawn(move || {
                run_execution_mysql_consumer(
                    execution_forwarder,
                    execution_metrics,
                    execution_kafka,
                    worker,
                    execution_dlq,
                )
            })
            .expect("spawn execution MySQL projection consumer");
        consumer_threads.push(handle);
    }
    let http_max_connections = env_number("TC_ORDER_MAX_CONNECTIONS", 4_096u64).max(1);
    let http_idle_timeout =
        Duration::from_millis(env_number("TC_ORDER_CONNECTION_IDLE_TIMEOUT_MS", 75_000u64).max(1));
    let http_write_timeout =
        Duration::from_millis(env_number("TC_ORDER_WRITE_TIMEOUT_MS", 30_000u64).max(1));
    let http_thread_stack = env_number("TC_ORDER_HTTP_THREAD_STACK_KB", 512usize)
        .max(128)
        .saturating_mul(1_024);
    log_info!(
        "order-api",
        "http max_connections={} idle_timeout_ms={} write_timeout_ms={} thread_stack_bytes={}",
        http_max_connections,
        http_idle_timeout.as_millis(),
        http_write_timeout.as_millis(),
        http_thread_stack,
    );
    let listener = TcpListener::bind("0.0.0.0:9200").expect("bind order API");
    // Non-blocking accept so a SIGTERM/SIGINT (e.g. `docker stop`) stops the API
    // taking new connections and lets the process exit cleanly instead of being
    // SIGKILLed. In-flight requests already commit Kafka offsets per batch, so
    // the durable pipeline state is crash-safe regardless.
    listener
        .set_nonblocking(true)
        .expect("set order API listener non-blocking");
    let shared_metrics = metrics;
    let shared_kafka = Arc::new(kafka);
    loop {
        if SHUTDOWN.load(AtomicOrdering::SeqCst) {
            log_info!(
                "order-api",
                "shutdown signal received, no longer accepting connections"
            );
            break;
        }
        match listener.accept() {
            Ok((mut stream, peer)) => {
                // Accepted sockets must be blocking for the handler's blocking reads.
                stream.set_nonblocking(false).ok();
                if stream.set_read_timeout(Some(http_idle_timeout)).is_err()
                    || stream.set_write_timeout(Some(http_write_timeout)).is_err()
                {
                    log_warn!("order-api", "event=http_socket_config_failed peer={peer}");
                    continue;
                }
                if !shared_metrics.try_open_http_connection(http_max_connections) {
                    respond(
                        &mut stream,
                        "503 Service Unavailable",
                        "{\"error\":\"connection limit reached\"}",
                        false,
                    );
                    continue;
                }
                let metrics = shared_metrics.clone();
                let kafka = shared_kafka.clone();
                let token = token.clone();
                let query_store = query_store.clone();
                let guard = HttpConnectionGuard {
                    metrics: metrics.clone(),
                };
                if let Err(error) = std::thread::Builder::new()
                    .name("order-http".into())
                    .stack_size(http_thread_stack)
                    .spawn(move || {
                        let _guard = guard;
                        handle(
                            stream,
                            category_size,
                            metrics,
                            kafka,
                            &token,
                            ingress_enabled,
                            query_store,
                        )
                    })
                {
                    log_warn!("order-api", "event=http_thread_spawn_failed error={error}");
                }
            }
            Err(ref error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    // Let every Kafka consumer finish its batch and leave its group before the
    // process exits (they check SHUTDOWN between batches).
    let consumers = consumer_threads.len();
    for handle in consumer_threads {
        let _ = handle.join();
    }
    log_info!("order-api", "stopped {consumers} Kafka consumers cleanly");
}
