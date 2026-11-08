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
//! Every event carries `event_id` = raft group:index:ordinal. Delivery is
//! at-least-once (Kafka redelivery), so clients dedupe on it.
//!
//! It is a live stream: an instance starts at the topic's tail and keeps no
//! history (reconcile through the order query API after a reconnect). Each
//! instance reads the whole topic and pushes only to its own connections;
//! beyond what one consumer can read, split the topic by user.
//!
//!   GET /ws/user?user=U   (Authorization: Bearer <TC_USER_STREAM_TOKEN>)
//!   GET /metrics
//!
//! Env: TC_USER_STREAM_ADDR (0.0.0.0:9300), TC_USER_STREAM_TOKEN (falls back
//! to TC_ORDER_API_TOKEN), TC_EXECUTION_KAFKA_BROKERS, TC_EXECUTION_KAFKA_TOPIC
//! (trade-executions-v1), TC_USER_STREAM_QUEUE (per-connection backlog before a
//! slow client is dropped, 10000), TC_USER_STREAM_MAX_CONNECTIONS_PER_USER (16).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
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
             tc_user_stream_slow_clients_dropped_total {}\n",
            get(&self.connections),
            get(&self.events_consumed),
            get(&self.events_pushed),
            get(&self.events_without_subscriber),
            get(&self.slow_clients_dropped),
        )
    }
}

/// user -> that user's live connections (id, outbound queue).
#[derive(Default)]
struct Registry {
    users: RwLock<HashMap<u64, Vec<(u64, SyncSender<Outgoing>)>>>,
    next_id: AtomicU64,
}

impl Registry {
    fn subscribe(&self, user: u64, tx: SyncSender<Outgoing>, max_per_user: usize) -> Option<u64> {
        let mut users = self.users.write().unwrap();
        let conns = users.entry(user).or_default();
        if conns.len() >= max_per_user {
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        conns.push((id, tx));
        Some(id)
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
        "\"order_id\":{order_id},\"user\":{user},\"instrument\":{},\"event_id\":\"{}:{}:{}\"",
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

fn consume(
    brokers: String,
    topic: String,
    group: String,
    registry: Arc<Registry>,
    metrics: Arc<Metrics>,
) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        // A live stream: start at the tail, never commit (each instance has
        // its own group and sees every event).
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "5")
        .create()
        .expect("create execution consumer");
    consumer
        .subscribe(&[&topic])
        .expect("subscribe execution topic");
    log_info!("user-stream", "consuming {topic} from {brokers} as {group}");
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                metrics.events_consumed.fetch_add(1, Ordering::Relaxed);
                let Some((user, json)) = message.payload().and_then(event_json) else {
                    continue;
                };
                let json: Arc<str> = json.into();
                if registry.push(user, &json, &metrics) > 0 {
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
    if method != "GET" || path != "/ws/user" {
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
    let (Some(key), true) = (key, upgrade) else {
        respond(&mut writer, "400 Bad Request", "websocket upgrade required");
        return;
    };
    let (tx, rx) = mpsc::sync_channel::<Outgoing>(config.queue);
    let Some(conn) = registry.subscribe(user, tx.clone(), config.max_per_user) else {
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
    let config = Arc::new(Config {
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
    let registry = Arc::new(Registry::default());
    let metrics = Arc::new(Metrics::default());
    {
        let (registry, metrics) = (registry.clone(), metrics.clone());
        std::thread::Builder::new()
            .name("execution-consumer".into())
            .spawn(move || consume(brokers, topic, group, registry, metrics))
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
                r#"{"e":"COMMITTED","order_id":1,"user":11,"instrument":5,"event_id":"7:42:3"}"#
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

        // No owner, nothing to push.
        let unowned = ExecReport::NotFound {
            instrument: sym,
            order_id: OrderId(9),
        };
        assert_eq!(event_json(&payload(&unowned, 9)), None);
        assert_eq!(event_json(b"garbage"), None);
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
