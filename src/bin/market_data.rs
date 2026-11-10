//! The **market-data** service: an independently deployed process that
//!
//! 1. subscribes to trade-core's market-data fanout port (TCP),
//! 2. aggregates trades into K-line candles (1s…1mo) and tracks the live
//!    **depth-of-market ladder** (top-5 bids/asks) per instrument,
//! 3. serves the chart frontend + JSON API over HTTP, **pushes updates over
//!    WebSocket** (poll fallback kept), and
//! 4. **persists candle history** to disk (atomic snapshot every 10 s, loaded
//!    on startup — a restart keeps the chart's history).
//!
//! Usage: market-data [EXCHANGE_MD_ADDR] [HTTP_ADDR] [DATA_DIR]
//!   EXCHANGE_MD_ADDR  default 127.0.0.1:9101
//!   HTTP_ADDR         default 0.0.0.0:8080
//!   DATA_DIR          default ./md-data   ("none" disables persistence)
//!
//! Endpoints:
//!   GET /                   chart UI
//!   GET /admin              operations dashboard
//!   GET /trade              authenticated order-entry terminal
//!   GET /api/admin/overview operational asset summary
//!   GET /api/symbols        [1,2,…]
//!   GET /api/candles?symbol=1&interval=1m&limit=120
//!   GET /api/depth?symbol=1 {"bids":[[p,q],…],"asks":[[p,q],…]}
//!   GET /ws?symbol=1&interval=1s   WebSocket: {"candles":…,"depth":…} pushes

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Message as KafkaMessage, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use trade_core::kline::KlineAggregator;
use trade_core::wire::{self, EXECUTION_EVENT_LEN, RT_DEPTH_END, RT_DEPTH_LEVEL};
use trade_core::{log_info, log_warn, InstrumentId, Order, OrderId, Price, Qty, Side};

const CHART_HTML: &str = include_str!("../../assets/kline.html");
const ADMIN_HTML: &str = include_str!("../../assets/admin.html");
const TRADE_HTML: &str = include_str!("../../assets/trade.html");
const RT_TRADE: u8 = 2;

/// Set by the SIGTERM/SIGINT handler so the main accept loop can break out and
/// run the candle-history flush before exiting.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Async-signal-safe handler: touches only an atomic flag, nothing else.
extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// (ts, price, qty, maker_fee, taker_fee), newest last.
type TradeRing = VecDeque<(u64, Price, Qty, u64, u64)>;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Depth {
    bids: Vec<(Price, Qty)>,
    asks: Vec<(Price, Qty)>,
}

#[derive(Default)]
struct DepthStore {
    /// Last complete snapshot (published on DepthEnd). Snapshots are
    /// assembled per feed connection: frames of two replicas' snapshots
    /// must not mix.
    live: HashMap<InstrumentId, Depth>,
}

struct State {
    agg: KlineAggregator,
    depth: DepthStore,
    /// Recent trades per instrument (newest last), bounded ring:
    /// (ts, price, qty, maker_fee, taker_fee).
    trades: HashMap<InstrumentId, TradeRing>,
    /// Per Raft group, the newest (raft index, ordinal) taken. Every replica
    /// of a group sends the same reports in the same order, and a restarted
    /// node replays its log tail: anything at or below the mark was already
    /// counted.
    applied: HashMap<u32, (u64, u32)>,
    /// Per Raft group, the incarnation its feeds announce
    /// (`wire::encode_md_hello`). A different one means the group was
    /// rebuilt from empty logs: its indexes restarted, so its mark resets.
    incarnations: HashMap<u32, u64>,
    /// Next Kafka offset to consume per market-topic partition. It is captured
    /// in the same atomic file as candles and watermarks, so restart never
    /// skips a trade whose candle was not durable yet.
    kafka_offsets: HashMap<i32, i64>,
    /// Newest timestamp given to a trade: candle time never goes backwards
    /// (feed threads race for the lock; wall clocks step back).
    last_ts: u64,
}

impl State {
    /// Whether a report of `group` at (`index`, `ordinal`) is new. Index 0:
    /// not from a Raft entry (a standalone exchange): always taken.
    fn take(&mut self, group: u32, index: u64, ordinal: u32) -> bool {
        if index == 0 {
            return true;
        }
        let mark = self.applied.entry(group).or_insert((0, 0));
        if (index, ordinal) <= *mark {
            // Far below the mark on every delivery is not a lagging replica
            // but a matching cluster whose Raft logs were wiped while this
            // state was kept: all its trades would be dropped as replays.
            if index.saturating_add(REPLAY_SUSPICION_GAP) < mark.0 {
                warn_far_below_mark(group, index, mark.0);
            }
            return false;
        }
        *mark = (index, ordinal);
        true
    }

    /// Whether a feed that last saw `index` of `group` is caught up with the
    /// group (so its depth is current, not a lagging replica's).
    fn current(&self, group: u32, index: u64) -> bool {
        self.applied
            .get(&group)
            .map_or(true, |mark| index >= mark.0)
    }

    /// A feed announced `group`'s incarnation (0: not assigned yet).
    fn on_incarnation(&mut self, group: u32, incarnation: u64) {
        if incarnation == 0 {
            return;
        }
        match self.incarnations.insert(group, incarnation) {
            Some(previous) if previous != incarnation => {
                let mark = self.applied.remove(&group);
                log_warn!(
                    "market-data",
                    "event=group_rebuilt group={group} incarnation={previous}->{incarnation} watermark_reset_from={mark:?}"
                );
            }
            _ => {}
        }
    }
}

/// A replica this far behind the newest delivered entry is implausible:
/// the group was more likely rebuilt from empty logs.
const REPLAY_SUSPICION_GAP: u64 = 10_000_000;

fn warn_far_below_mark(group: u32, index: u64, mark: u64) {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = now_sec();
    let last = LAST.load(Ordering::Relaxed);
    if now >= last + 60
        && LAST
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        log_warn!(
            "market-data",
            "event=trades_far_below_watermark group={group} index={index} watermark={mark}: dropped as replays; if the matching cluster was rebuilt from empty Raft logs, stop market-data and delete its klines.bin"
        );
    }
}

const MARKET_STATE_MAGIC: &[u8; 4] = b"TCM3";
const PREVIOUS_MARKET_STATE_MAGIC: &[u8; 4] = b"TCM2";
/// group u32 | index u64 | ordinal u32 | incarnation u64
const MARK_RECORD_LEN: usize = 24;

/// What a snapshot needs from the shared state: copied under the lock (the
/// candle encoding is the only real work), framed and checksummed after it
/// is released so the feed threads do not wait on that.
struct MarketStateParts {
    candles: Vec<u8>,
    applied: Vec<(u32, u64, u32, u64)>,
    kafka_offsets: Vec<(i32, i64)>,
    last_ts: u64,
}

fn market_state_parts(state: &State) -> MarketStateParts {
    let mut groups: Vec<u32> = state
        .applied
        .keys()
        .chain(state.incarnations.keys())
        .copied()
        .collect();
    groups.sort_unstable();
    groups.dedup();
    let applied = groups
        .into_iter()
        .map(|group| {
            let (index, ordinal) = state.applied.get(&group).copied().unwrap_or((0, 0));
            let incarnation = state.incarnations.get(&group).copied().unwrap_or(0);
            (group, index, ordinal, incarnation)
        })
        .collect();
    let mut kafka_offsets = state
        .kafka_offsets
        .iter()
        .map(|(&partition, &offset)| (partition, offset))
        .collect::<Vec<_>>();
    kafka_offsets.sort_unstable();
    MarketStateParts {
        candles: state.agg.encode(),
        applied,
        kafka_offsets,
        last_ts: state.last_ts,
    }
}

/// Candle history and its exact replay watermark must cross the durability
/// boundary together. Otherwise a crash after saving candles but before
/// saving the watermark counts the same execution again when any Raft replica
/// replays its tail.
#[cfg(test)]
fn encode_market_state(state: &State) -> Vec<u8> {
    frame_market_state(market_state_parts(state))
}

fn frame_market_state(parts: MarketStateParts) -> Vec<u8> {
    let MarketStateParts {
        candles,
        applied,
        kafka_offsets,
        last_ts,
    } = parts;
    let mut out = Vec::with_capacity(
        28 + candles.len() + applied.len() * MARK_RECORD_LEN + kafka_offsets.len() * 12,
    );
    out.extend_from_slice(MARKET_STATE_MAGIC);
    out.extend_from_slice(&(candles.len() as u64).to_le_bytes());
    out.extend_from_slice(&candles);
    out.extend_from_slice(&(applied.len() as u32).to_le_bytes());
    for (group, index, ordinal, incarnation) in applied {
        out.extend_from_slice(&group.to_le_bytes());
        out.extend_from_slice(&index.to_le_bytes());
        out.extend_from_slice(&ordinal.to_le_bytes());
        out.extend_from_slice(&incarnation.to_le_bytes());
    }
    out.extend_from_slice(&(kafka_offsets.len() as u32).to_le_bytes());
    for (partition, offset) in kafka_offsets {
        out.extend_from_slice(&partition.to_le_bytes());
        out.extend_from_slice(&offset.to_le_bytes());
    }
    out.extend_from_slice(&last_ts.to_le_bytes());
    let checksum = trade_core::journal::fnv1a(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

/// Candles, replay marks, group incarnations, last trade timestamp.
type RestoredMarket = (
    KlineAggregator,
    HashMap<u32, (u64, u32)>,
    HashMap<u32, u64>,
    HashMap<i32, i64>,
    u64,
);

fn decode_market_state(bytes: &[u8]) -> std::io::Result<RestoredMarket> {
    let bad =
        |message: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string());
    if bytes.starts_with(b"TCK2") {
        return Ok((
            KlineAggregator::decode(bytes)?,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            0,
        ));
    }
    let previous = bytes.get(..4) == Some(PREVIOUS_MARKET_STATE_MAGIC);
    if bytes.len() < 32 || (!previous && bytes.get(..4) != Some(MARKET_STATE_MAGIC)) {
        return Err(bad("bad market state magic"));
    }
    let body_end = bytes.len() - 8;
    let stored = u64::from_le_bytes(bytes[body_end..].try_into().unwrap());
    if trade_core::journal::fnv1a(&bytes[..body_end]) != stored {
        return Err(bad("market state checksum mismatch"));
    }
    let candle_len = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as usize;
    let candle_end = 12usize
        .checked_add(candle_len)
        .ok_or_else(|| bad("market state length overflow"))?;
    if candle_end.checked_add(4).map_or(true, |end| end > body_end) {
        return Err(bad("truncated market candle state"));
    }
    let agg = KlineAggregator::decode(&bytes[12..candle_end])?;
    let count = u32::from_le_bytes(bytes[candle_end..candle_end + 4].try_into().unwrap()) as usize;
    let marks_start = candle_end + 4;
    let marks_end = marks_start
        .checked_add(
            count
                .checked_mul(MARK_RECORD_LEN)
                .ok_or_else(|| bad("market watermark count overflow"))?,
        )
        .ok_or_else(|| bad("market watermark length overflow"))?;
    let mut applied = HashMap::with_capacity(count);
    let mut incarnations = HashMap::with_capacity(count);
    for record in bytes[marks_start..marks_end].chunks_exact(MARK_RECORD_LEN) {
        let group = u32::from_le_bytes(record[0..4].try_into().unwrap());
        let index = u64::from_le_bytes(record[4..12].try_into().unwrap());
        let ordinal = u32::from_le_bytes(record[12..16].try_into().unwrap());
        let incarnation = u64::from_le_bytes(record[16..24].try_into().unwrap());
        if applied.insert(group, (index, ordinal)).is_some() {
            return Err(bad("duplicate market watermark group"));
        }
        if incarnation != 0 {
            incarnations.insert(group, incarnation);
        }
    }
    let (kafka_offsets, last_ts_at) = if previous {
        if marks_end.checked_add(8) != Some(body_end) {
            return Err(bad("truncated or trailing market watermarks"));
        }
        (HashMap::new(), marks_end)
    } else {
        let count_end = marks_end
            .checked_add(4)
            .ok_or_else(|| bad("market offset count overflow"))?;
        if count_end > body_end {
            return Err(bad("truncated market offset count"));
        }
        let offset_count =
            u32::from_le_bytes(bytes[marks_end..count_end].try_into().unwrap()) as usize;
        let offsets_end = count_end
            .checked_add(
                offset_count
                    .checked_mul(12)
                    .ok_or_else(|| bad("market offset count overflow"))?,
            )
            .ok_or_else(|| bad("market offsets length overflow"))?;
        if offsets_end.checked_add(8) != Some(body_end) {
            return Err(bad("truncated or trailing market offsets"));
        }
        let mut offsets = HashMap::with_capacity(offset_count);
        for record in bytes[count_end..offsets_end].chunks_exact(12) {
            let partition = i32::from_le_bytes(record[..4].try_into().unwrap());
            let offset = i64::from_le_bytes(record[4..12].try_into().unwrap());
            if partition < 0 || offset < 0 || offsets.insert(partition, offset).is_some() {
                return Err(bad("invalid or duplicate market Kafka offset"));
            }
        }
        (offsets, offsets_end)
    };
    let last_ts = u64::from_le_bytes(bytes[last_ts_at..body_end].try_into().unwrap());
    Ok((agg, applied, incarnations, kafka_offsets, last_ts))
}

fn write_market_state(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    KlineAggregator::write_encoded(path, bytes)
}

fn now_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Feed: subscribe to trade-core's fanout, ingest trades + depth
// ---------------------------------------------------------------------------

fn feed_loop(md_addrs: String, state: Arc<Mutex<State>>) {
    for md_addr in md_addrs
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
    {
        let state = state.clone();
        let md_addr = md_addr.to_string();
        std::thread::spawn(move || feed_one(md_addr, state));
    }
}

/// Request line plus headers, at most.
const MAX_REQUEST_HEAD: u64 = 16 * 1024;
/// Concurrent HTTP/WebSocket connections.
const MAX_CONNECTIONS: usize = 4096;

/// Highest depth level accepted from a frame (the engine publishes 5).
const MAX_DEPTH_LEVELS: usize = 64;

fn idle_feed_read(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn apply_trade_event(state: &mut State, event: wire::ExecutionEvent, ts: u64) -> bool {
    let report = event.report;
    if report.type_code != RT_TRADE
        || !state.take(event.raft_group, event.raft_index, event.ordinal)
    {
        return false;
    }
    state.last_ts = state.last_ts.max(ts);
    let ts = state.last_ts;
    state
        .agg
        .on_trade(report.instrument, ts, report.price, report.qty);
    let trades = state.trades.entry(report.instrument).or_default();
    trades.push_back((
        ts,
        report.price,
        report.qty,
        report.maker_fee,
        report.taker_fee,
    ));
    if trades.len() > 1000 {
        trades.pop_front();
    }
    true
}

fn feed_one(md_addr: String, state: Arc<Mutex<State>>) {
    let mut last_connect_warning = std::time::Instant::now() - Duration::from_secs(30);
    let mut last_disconnect_warning = std::time::Instant::now() - Duration::from_secs(30);
    loop {
        let mut sock = match TcpStream::connect(&md_addr) {
            Ok(s) => s,
            Err(e) => {
                if last_connect_warning.elapsed() >= Duration::from_secs(30) {
                    log_warn!(
                        "market-data",
                        "waiting for trade-core fanout at {md_addr} ({e})"
                    );
                    last_connect_warning = std::time::Instant::now();
                }
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        // The fanout sends a heartbeat frame every idle second, so a quiet
        // market still delivers bytes. Two silent read timeouts in a row
        // mean the link is dead without a FIN (host loss, partition):
        // reconnect instead of blocking on it forever.
        sock.set_read_timeout(Some(Duration::from_secs(10))).ok();
        let mut silent_reads = 0u32;
        log_info!("market-data", "subscribed to {md_addr}");
        let mut buf = vec![0u8; EXECUTION_EVENT_LEN * 512];
        let mut filled = 0usize;
        // This connection's depth snapshots in progress, and the newest Raft
        // index it delivered (to tell whether its depth is current).
        let mut pending: HashMap<InstrumentId, Depth> = HashMap::new();
        let mut last_index = 0u64;
        loop {
            match sock.read(&mut buf[filled..]) {
                Ok(0) => break,
                Err(error) if idle_feed_read(&error) => {
                    silent_reads += 1;
                    if silent_reads < 2 {
                        continue;
                    }
                    log_warn!(
                        "market-data",
                        "event=feed_silent addr={md_addr}; reconnecting"
                    );
                    break;
                }
                Err(_) => break,
                Ok(n) => {
                    silent_reads = 0;
                    filled += n;
                    let mut off = 0;
                    let mut st = state.lock().unwrap();
                    let ts = now_sec().max(st.last_ts);
                    st.last_ts = ts;
                    while filled - off >= EXECUTION_EVENT_LEN {
                        let frame = &buf[off..off + EXECUTION_EVENT_LEN];
                        off += EXECUTION_EVENT_LEN;
                        if let Some((group, incarnation)) = wire::decode_md_hello(frame) {
                            st.on_incarnation(group, incarnation);
                            continue;
                        }
                        let Some(event) = wire::decode_execution_event(frame) else {
                            continue;
                        };
                        let r = event.report;
                        if event.raft_index != 0 {
                            last_index = last_index.max(event.raft_index);
                        }
                        match r.type_code {
                            RT_TRADE => {
                                apply_trade_event(&mut st, event, ts);
                            }
                            RT_DEPTH_LEVEL => {
                                let lvl = r.aux_id as usize;
                                if lvl >= MAX_DEPTH_LEVELS {
                                    continue; // malformed: never index or allocate by it
                                }
                                let d = pending.entry(r.instrument).or_default();
                                let side = if r.side == trade_core::Side::Buy {
                                    &mut d.bids
                                } else {
                                    &mut d.asks
                                };
                                if side.len() <= lvl {
                                    side.resize(lvl + 1, (0, 0));
                                }
                                side[lvl] = (r.price, r.qty);
                            }
                            RT_DEPTH_END => {
                                if let Some(mut d) = pending.remove(&r.instrument) {
                                    if st.current(event.raft_group, last_index) {
                                        d.bids.truncate(r.aux_id as usize & 0xFF);
                                        d.asks.truncate((r.aux_id as usize >> 8) & 0xFF);
                                        st.depth.live.insert(r.instrument, d);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    drop(st);
                    if off > 0 {
                        buf.copy_within(off..filled, 0);
                        filled -= off;
                    }
                }
            }
        }
        if last_disconnect_warning.elapsed() >= Duration::from_secs(30) {
            log_warn!("market-data", "feed {md_addr} disconnected, retrying");
            last_disconnect_warning = std::time::Instant::now();
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Durable candle source. The dedicated topic has one canonical event per
/// trade and is partitioned by Raft group, preserving the order required by
/// `State::take`. TCP fanout remains enabled for low-latency depth and may win
/// the race for a trade; the same event id makes the Kafka copy a no-op.
fn kafka_trade_loop(brokers: String, topic: String, state: Arc<Mutex<State>>) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "market-data-authoritative-replay")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "error")
        .set("isolation.level", "read_committed")
        .create()
        .expect("create market-data Kafka consumer");
    let metadata = consumer
        .fetch_metadata(Some(&topic), Duration::from_secs(10))
        .expect("fetch market-data topic metadata");
    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|metadata| metadata.name() == topic)
        .expect("market-data topic exists");
    assert!(
        topic_metadata.error().is_none(),
        "market-data topic metadata error: {:?}",
        topic_metadata.error()
    );
    let restored_offsets = state.lock().unwrap().kafka_offsets.clone();
    let mut assignment = TopicPartitionList::new();
    for partition in topic_metadata.partitions() {
        let partition = partition.id();
        let (low, high) = consumer
            .fetch_watermarks(&topic, partition, Duration::from_secs(5))
            .expect("fetch market-data partition watermarks");
        let offset = match restored_offsets.get(&partition).copied() {
            Some(offset) if offset < low => panic!(
                "market-data Kafka offset {offset} for partition {partition} is below retained low watermark {low}; restore a newer market snapshot before starting"
            ),
            Some(offset) if offset > high => panic!(
                "market-data Kafka offset {offset} for partition {partition} is beyond high watermark {high}; topic was likely recreated"
            ),
            Some(offset) => Offset::Offset(offset),
            None if low == 0 => Offset::Beginning,
            None => panic!(
                "market-data has no durable offset for partition {partition}, but Kafka history starts at {low}"
            ),
        };
        assignment
            .add_partition_offset(&topic, partition, offset)
            .expect("build market-data Kafka assignment");
    }
    consumer
        .assign(&assignment)
        .expect("assign every market-data Kafka partition");
    log_info!(
        "market-data",
        "authoritative trade replay from Kafka topic={topic} partitions={}",
        topic_metadata.partitions().len()
    );

    while !SHUTDOWN.load(Ordering::Acquire) {
        let Some(message) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        match message {
            Ok(message) => {
                let partition = message.partition();
                let next_offset = message.offset().saturating_add(1);
                let ts = match message.timestamp() {
                    Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) if ms >= 0 => {
                        ms as u64 / 1_000
                    }
                    _ => now_sec(),
                };
                let mut state = state.lock().unwrap();
                if let Some(event) = message.payload().and_then(wire::decode_execution_event) {
                    apply_trade_event(&mut state, event, ts);
                } else {
                    log_warn!(
                        "market-data",
                        "event=invalid_market_event partition={partition} offset={}",
                        message.offset()
                    );
                }
                state
                    .kafka_offsets
                    .entry(partition)
                    .and_modify(|offset| *offset = (*offset).max(next_offset))
                    .or_insert(next_offset);
            }
            Err(error) => {
                log_warn!("market-data", "event=kafka_trade_read_failed error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

fn candles_json(st: &State, sym: InstrumentId, interval: &str, limit: usize) -> String {
    let candles = st.agg.candles(sym, interval, limit.min(1000));
    let mut s = String::with_capacity(candles.len() * 48 + 2);
    s.push('[');
    for (i, c) in candles.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "[{},{},{},{},{},{}]",
            c.start, c.open, c.high, c.low, c.close, c.volume
        ));
    }
    s.push(']');
    s
}

/// Read-only operating summary. Commands are deliberately absent until the
/// control plane has authentication, authorization and an immutable audit log.
fn admin_overview_json(st: &State) -> String {
    let instruments = st.agg.instruments();
    let mut rows = Vec::with_capacity(instruments.len());
    let mut total_trades = 0usize;
    let mut total_volume = 0u64;
    for instrument in instruments {
        let trades = st.trades.get(&instrument);
        let trade_count = trades.map_or(0, |q| q.len());
        let volume = trades.map_or(0, |q| q.iter().map(|(_, _, qty, _, _)| *qty).sum());
        let last_trade = trades
            .and_then(|q| q.back().map(|(ts, _, _, _, _)| *ts))
            .unwrap_or(0);
        let depth = st.depth.live.get(&instrument);
        let bid_levels = depth.map_or(0, |d| d.bids.len());
        let ask_levels = depth.map_or(0, |d| d.asks.len());
        total_trades += trade_count;
        total_volume += volume;
        rows.push(format!(
            "{{\"instrument\":{},\"trades\":{trade_count},\"volume\":{volume},\"last_trade\":{last_trade},\"bid_levels\":{bid_levels},\"ask_levels\":{ask_levels}}}",
            instrument.0
        ));
    }
    format!(
        "{{\"assets\":{},\"recent_trades\":{total_trades},\"recent_volume\":{total_volume},\"instruments\":[{}]}}",
        rows.len(),
        rows.join(",")
    )
}

fn depth_json(st: &State, sym: InstrumentId) -> String {
    let d = st.depth.live.get(&sym).cloned().unwrap_or_default();
    let fmt = |v: &[(Price, Qty)]| {
        v.iter()
            .map(|(p, q)| format!("[{p},{q}]"))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{{\"bids\":[{}],\"asks\":[{}]}}",
        fmt(&d.bids),
        fmt(&d.asks)
    )
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

// ---------------------------------------------------------------------------
// WebSocket (RFC 6455, server side, text frames) — dependency-free
// ---------------------------------------------------------------------------

/// Write one unmasked text frame (server -> client).
fn ws_send_text(stream: &mut TcpStream, payload: &str) -> std::io::Result<()> {
    let p = payload.as_bytes();
    let mut hdr: Vec<u8> = vec![0x81]; // FIN + text
    match p.len() {
        0..=125 => hdr.push(p.len() as u8),
        126..=65535 => {
            hdr.push(126);
            hdr.extend_from_slice(&(p.len() as u16).to_be_bytes());
        }
        _ => {
            hdr.push(127);
            hdr.extend_from_slice(&(p.len() as u64).to_be_bytes());
        }
    }
    stream.write_all(&hdr)?;
    stream.write_all(p)
}

/// Consume any pending client frame; `Ok(false)` = client sent close.
fn ws_drain_client(stream: &mut TcpStream) -> std::io::Result<bool> {
    let mut hdr = [0u8; 2];
    match stream.read(&mut hdr[..1]) {
        Err(ref e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            return Ok(true); // nothing pending
        }
        Err(e) => return Err(e),
        Ok(0) => return Ok(false),
        Ok(_) => {}
    }
    stream.read_exact(&mut hdr[1..2])?;
    let opcode = hdr[0] & 0x0F;
    let masked = hdr[1] & 0x80 != 0;
    let mut len = (hdr[1] & 0x7F) as u64;
    if len == 126 {
        let mut b = [0u8; 2];
        stream.read_exact(&mut b)?;
        len = u16::from_be_bytes(b) as u64;
    } else if len == 127 {
        let mut b = [0u8; 8];
        stream.read_exact(&mut b)?;
        len = u64::from_be_bytes(b);
    }
    let skip = len + if masked { 4 } else { 0 };
    std::io::copy(&mut stream.take(skip), &mut std::io::sink())?;
    Ok(opcode != 0x8) // 0x8 = close
}

/// Upgrade and run one WS session: push candles+depth for the subscribed
/// symbol/interval every 250 ms until the client goes away.
fn ws_session(
    mut stream: TcpStream,
    key: &str,
    query: &str,
    state: &Arc<Mutex<State>>,
) -> std::io::Result<()> {
    stream.write_all(trade_core::websocket::handshake_response(key).as_bytes())?;

    let symbol = query_param(query, "symbol")
        .and_then(|v| v.parse().ok())
        .map(InstrumentId)
        .unwrap_or(InstrumentId(1));
    let interval = query_param(query, "interval").unwrap_or("1s").to_string();
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    // A client that stops reading is dropped instead of pinning this thread.
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    let mut last_push: Option<std::time::Instant> = None;
    loop {
        if !ws_drain_client(&mut stream)? {
            return Ok(()); // client closed
        }
        // One push per 250 ms however many frames the client sends: frames
        // used to each trigger a full candles+depth render under the lock.
        if last_push.is_some_and(|at| at.elapsed() < Duration::from_millis(250)) {
            continue;
        }
        last_push = Some(std::time::Instant::now());
        let msg = {
            let st = state.lock().unwrap();
            format!(
                "{{\"candles\":{},\"depth\":{}}}",
                candles_json(&st, symbol, &interval, 120),
                depth_json(&st, symbol)
            )
        };
        ws_send_text(&mut stream, &msg)?;
    }
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

fn respond(stream: &mut TcpStream, status: &str, ctype: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

fn handle_http(
    mut stream: TcpStream,
    state: &Arc<Mutex<State>>,
    admin_token: Option<&str>,
    trading_token: Option<&str>,
    order_api: &OrderApiPool,
) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(10))).ok();
    let Ok(read_half) = stream.try_clone() else {
        return; // out of file descriptors: drop this connection, not the process
    };
    // The request line and headers together are bounded: a client streaming
    // bytes without a newline must not grow a buffer until allocation fails.
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_HEAD);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || !line.ends_with('\n') {
        return;
    }
    let method = line.split_whitespace().next().unwrap_or("GET");
    let path_full = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let (path, query) = path_full
        .split_once('?')
        .unwrap_or((path_full.as_str(), ""));

    // Read headers (needed for the WS key).
    let mut ws_key = None;
    let mut authorization = None;
    let mut hl = String::new();
    while reader.read_line(&mut hl).is_ok() && hl.ends_with('\n') && hl.trim() != "" {
        if let Some((k, v)) = hl.split_once(':') {
            if k.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = Some(v.trim().to_string());
            }
            if k.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_string());
            }
        }
        hl.clear();
    }

    match path {
        "/" | "/index.html" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            CHART_HTML.as_bytes(),
        ),
        "/admin" | "/admin/" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            ADMIN_HTML.as_bytes(),
        ),
        "/trade" | "/trade/" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            TRADE_HTML.as_bytes(),
        ),
        "/ws" => {
            if let Some(key) = ws_key {
                let _ = ws_session(stream, &key, query, state);
            } else {
                respond(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    b"expected websocket",
                );
            }
        }
        "/api/symbols" => {
            let syms = state.lock().unwrap().agg.instruments();
            let body = format!(
                "[{}]",
                syms.iter()
                    .map(|s| s.0.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/admin/overview" => match admin_access(authorization.as_deref(), admin_token) {
            Ok(()) => {
                let body = admin_overview_json(&state.lock().unwrap());
                respond(&mut stream, "200 OK", "application/json", body.as_bytes());
            }
            Err((status, message)) => {
                respond(&mut stream, status, "application/json", message.as_bytes());
            }
        },
        "/api/trade/order" if method == "POST" => {
            match admin_access(authorization.as_deref(), trading_token) {
                Ok(()) => match new_order_from_query(query) {
                    Ok(order) => {
                        let _ = order;
                        match order_api.forward("/orders", query) {
                            Ok(()) => respond(
                                &mut stream,
                                "202 Accepted",
                                "application/json",
                                b"{\"accepted\":true}",
                            ),
                            Err(error) => respond(
                                &mut stream,
                                "503 Service Unavailable",
                                "application/json",
                                format!("{{\"error\":\"{error}\"}}").as_bytes(),
                            ),
                        }
                    }
                    Err(message) => respond(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        message.as_bytes(),
                    ),
                },
                Err((status, message)) => {
                    respond(&mut stream, status, "application/json", message.as_bytes())
                }
            }
        }
        "/api/trade/cancel" if method == "POST" => {
            match admin_access(authorization.as_deref(), trading_token) {
                Ok(()) => match cancel_from_query(query) {
                    Ok((instrument, order_id, cmd_id)) => {
                        let _ = (instrument, order_id, cmd_id);
                        match order_api.forward("/cancels", query) {
                            Ok(()) => respond(
                                &mut stream,
                                "202 Accepted",
                                "application/json",
                                b"{\"accepted\":true}",
                            ),
                            Err(error) => respond(
                                &mut stream,
                                "503 Service Unavailable",
                                "application/json",
                                format!("{{\"error\":\"{error}\"}}").as_bytes(),
                            ),
                        }
                    }
                    Err(message) => respond(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        message.as_bytes(),
                    ),
                },
                Err((status, message)) => {
                    respond(&mut stream, status, "application/json", message.as_bytes())
                }
            }
        }
        "/api/candles" => {
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let interval = query_param(query, "interval").unwrap_or("1m");
            let limit: usize = query_param(query, "limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(120);
            let body = candles_json(&state.lock().unwrap(), symbol, interval, limit);
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/trades" => {
            // Recent executions: [[ts,price,qty,maker_fee,taker_fee],...] oldest first.
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let limit: usize = query_param(query, "limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100);
            let st = state.lock().unwrap();
            let empty = TradeRing::new();
            let dq = st.trades.get(&symbol).unwrap_or(&empty);
            let body = format!(
                "[{}]",
                dq.iter()
                    .skip(dq.len().saturating_sub(limit.min(1000)))
                    .map(|(t, p, q, mf, tf)| format!("[{t},{p},{q},{mf},{tf}]"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        "/api/depth" => {
            let symbol = query_param(query, "symbol")
                .and_then(|v| v.parse().ok())
                .map(InstrumentId)
                .unwrap_or(InstrumentId(1));
            let body = depth_json(&state.lock().unwrap(), symbol);
            respond(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", b"not found"),
    }
}

fn required_query<T: std::str::FromStr>(query: &str, key: &str) -> Result<T, String> {
    query_param(query, key)
        .ok_or_else(|| format!("{{\"error\":\"missing {key}\"}}"))?
        .parse()
        .map_err(|_| format!("{{\"error\":\"invalid {key}\"}}"))
}

fn new_order_from_query(query: &str) -> Result<Order, String> {
    let side = match query_param(query, "side") {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err("{\"error\":\"side must be buy or sell\"}".into()),
    };
    let instrument = InstrumentId(required_query(query, "instrument")?);
    let order_id = OrderId(required_query(query, "order_id")?);
    let price = required_query(query, "price")?;
    let qty = required_query(query, "qty")?;
    let user = required_query(query, "user")?;
    Ok(Order::limit(order_id, side, price, qty)
        .on(instrument)
        .by(user))
}

fn cancel_from_query(query: &str) -> Result<(InstrumentId, OrderId, u64), String> {
    Ok((
        InstrumentId(required_query(query, "instrument")?),
        OrderId(required_query(query, "order_id")?),
        required_query(query, "cmd_id")?,
    ))
}

struct OrderApiPool {
    address: String,
    token: String,
    idle: Mutex<Vec<TcpStream>>,
    max_idle: usize,
}

impl OrderApiPool {
    fn new(address: String, token: String, max_idle: usize) -> Self {
        Self {
            address,
            token,
            idle: Mutex::new(Vec::with_capacity(max_idle)),
            max_idle,
        }
    }

    fn connect(&self) -> std::io::Result<TcpStream> {
        let stream = TcpStream::connect(&self.address)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        Ok(stream)
    }

    fn forward(&self, path: &str, query: &str) -> std::io::Result<()> {
        let request = format!(
            "POST {path}?{query} HTTP/1.1\r\nHost: order-api\r\nAuthorization: Bearer {}\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            self.token
        );
        let mut last_error = None;
        for _ in 0..2 {
            let mut stream = match self.idle.lock().unwrap().pop() {
                Some(stream) => stream,
                None => self.connect()?,
            };
            match send_order_api_request(&mut stream, request.as_bytes()) {
                Ok(accepted) => {
                    let mut idle = self.idle.lock().unwrap();
                    if idle.len() < self.max_idle {
                        idle.push(stream);
                    }
                    return if accepted {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "order API rejected request",
                        ))
                    };
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "order API unavailable")
        }))
    }
}

fn send_order_api_request(stream: &mut TcpStream, request: &[u8]) -> std::io::Result<bool> {
    stream.write_all(request)?;
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader.read_line(&mut status)?;
    if status.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "order API closed persistent connection",
        ));
    }
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid order API content length",
                    )
                })?;
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body)?;
    Ok(status.starts_with("HTTP/1.1 202"))
}

fn admin_access<'a>(
    authorization: Option<&str>,
    configured: Option<&'a str>,
) -> Result<(), (&'a str, String)> {
    let Some(token) = configured.filter(|token| !token.is_empty()) else {
        return Err((
            "503 Service Unavailable",
            "{\"error\":\"admin access is not configured\"}".into(),
        ));
    };
    let expected = format!("Bearer {token}");
    if authorization == Some(expected.as_str()) {
        Ok(())
    } else {
        Err((
            "401 Unauthorized",
            "{\"error\":\"admin authorization required\"}".into(),
        ))
    }
}

fn main() {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("market-data");

    // Install the shutdown handler up front: it only flips an atomic flag, which
    // is async-signal-safe. SIGTERM (orchestrator stop) and SIGINT (Ctrl-C) both
    // trigger a graceful drain + final candle-history flush.
    unsafe {
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
    }

    let mut args = std::env::args().skip(1);
    let md_addr = args.next().unwrap_or_else(|| "127.0.0.1:9101".to_string());
    let http_addr = args.next().unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let data_dir = args.next().unwrap_or_else(|| "./md-data".to_string());
    let admin_token = std::env::var("TC_ADMIN_TOKEN").ok();
    let trading_token = std::env::var("TC_TRADING_TOKEN").ok();
    let order_api =
        std::env::var("TC_ORDER_API_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let order_api_token = std::env::var("TC_ORDER_API_TOKEN").expect("TC_ORDER_API_TOKEN");
    let order_api_pool = Arc::new(OrderApiPool::new(
        order_api,
        order_api_token,
        std::env::var("TC_ORDER_API_MAX_IDLE_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(32)
            .clamp(1, 4_096),
    ));

    // Load persisted candle history, if any.
    let persist: Option<PathBuf> = (data_dir != "none").then(|| {
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        PathBuf::from(&data_dir).join("klines.bin")
    });
    let restored = persist
        .as_ref()
        .and_then(
            |p| match std::fs::read(p).and_then(|bytes| decode_market_state(&bytes)) {
                Ok(state) => {
                    log_info!(
                        "market-data",
                        "loaded durable market state from {}",
                        p.display()
                    );
                    Some(state)
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    // Keep the unreadable file for inspection; the next save
                    // would otherwise replace it.
                    let mut aside = p.as_os_str().to_owned();
                    aside.push(format!(".unreadable-{}", now_sec()));
                    let moved = std::fs::rename(p, &aside);
                    log_warn!(
                        "market-data",
                        "event=history_load_failed path={} error={error} moved_aside={}",
                        p.display(),
                        moved.is_ok()
                    );
                    None
                }
            },
        )
        .unwrap_or_else(|| {
            (
                KlineAggregator::new(2000),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                0,
            )
        });

    let state = Arc::new(Mutex::new(State {
        agg: restored.0,
        depth: DepthStore::default(),
        trades: HashMap::new(),
        applied: restored.1,
        incarnations: restored.2,
        kafka_offsets: restored.3,
        // A clock that once jumped ahead must not pin every later candle
        // to that future time across restarts.
        last_ts: restored.4.min(now_sec() + 60),
    }));

    // Feed thread.
    let feed_state = state.clone();
    std::thread::Builder::new()
        .name("md-feed".into())
        .spawn(move || feed_loop(md_addr, feed_state))
        .expect("spawn feed");

    let kafka_brokers = std::env::var("TC_MARKET_KAFKA_BROKERS")
        .or_else(|_| std::env::var("TC_EXECUTION_KAFKA_BROKERS"))
        .unwrap_or_default();
    if !kafka_brokers.trim().is_empty() {
        let kafka_topic = std::env::var("TC_MARKET_KAFKA_TOPIC")
            .unwrap_or_else(|_| "trade-market-events-v1".into());
        let kafka_state = state.clone();
        std::thread::Builder::new()
            .name("md-kafka-replay".into())
            .spawn(move || kafka_trade_loop(kafka_brokers, kafka_topic, kafka_state))
            .expect("spawn market-data Kafka replay");
    } else {
        log_warn!(
            "market-data",
            "TC_MARKET_KAFKA_BROKERS is empty: candle feed has no durable replay source"
        );
    }

    // Persistence thread: atomic snapshot every 10 s. Keep the path so the main
    // thread can take one final snapshot on graceful shutdown.
    if let Some(path) = persist.clone() {
        let save_state = state.clone();
        std::thread::Builder::new()
            .name("md-persist".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(10));
                // Encode under the lock, write and fsync after releasing it:
                // the feed threads must not wait on the disk.
                let parts = market_state_parts(&save_state.lock().unwrap());
                let bytes = frame_market_state(parts);
                if let Err(error) = write_market_state(&path, &bytes) {
                    log_warn!("market-data", "event=history_save_failed error={error}");
                }
            })
            .expect("spawn persist");
        log_info!("market-data", "persisting candle history every 10s");
    }

    let listener = TcpListener::bind(&http_addr).expect("bind http");
    // Non-blocking accept so the loop has a natural poll point to observe the
    // SHUTDOWN flag set by the signal handler.
    listener
        .set_nonblocking(true)
        .expect("set http listener non-blocking");
    log_info!("market-data", "chart UI + API + WS on http://{http_addr}/");
    let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            log_info!("market-data", "shutdown signal received, draining");
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                // One thread per connection, so bounded: past the limit (or
                // when the OS refuses a thread) the connection is closed.
                if connections.load(Ordering::Acquire) >= MAX_CONNECTIONS {
                    continue;
                }
                stream.set_nonblocking(false).ok();
                let state = state.clone();
                let admin_token = admin_token.clone();
                let trading_token = trading_token.clone();
                let order_api = order_api_pool.clone();
                let done = connections.clone();
                connections.fetch_add(1, Ordering::AcqRel);
                let spawned = std::thread::Builder::new()
                    .name("md-http".into())
                    .spawn(move || {
                        handle_http(
                            stream,
                            &state,
                            admin_token.as_deref(),
                            trading_token.as_deref(),
                            &order_api,
                        );
                        done.fetch_sub(1, Ordering::AcqRel);
                    });
                if spawned.is_err() {
                    connections.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection; nap briefly and re-check SHUTDOWN.
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }

    // Final flush of candle history so a graceful stop keeps the chart's
    // history up to the moment of shutdown (mirrors the periodic snapshot).
    if let Some(path) = persist {
        let parts = market_state_parts(&state.lock().unwrap());
        let _ = write_market_state(&path, &frame_market_state(parts));
        log_info!(
            "market-data",
            "market history and replay watermarks flushed on shutdown"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_feed_timeout_is_not_a_disconnect() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            assert!(idle_feed_read(&std::io::Error::from(kind)));
        }
        assert!(!idle_feed_read(&std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        )));
    }

    #[test]
    fn each_trade_counts_once_across_replicas_and_replays() {
        let mut st = State {
            agg: KlineAggregator::new(10),
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied: HashMap::new(),
            incarnations: HashMap::new(),
            last_ts: 0,
        };
        // Replica A delivers group 3's entries 10..=12; replica B, behind,
        // delivers 10 and 11 again; a restarted node replays 9..=12.
        let a: Vec<bool> = [(10, 0), (10, 1), (11, 0), (12, 0)]
            .iter()
            .map(|&(i, o)| st.take(3, i, o))
            .collect();
        assert_eq!(a, [true, true, true, true]);
        assert!(
            !st.take(3, 10, 1) && !st.take(3, 11, 0),
            "a lagging replica's copies"
        );
        assert!((9..=12).all(|i| !st.take(3, i, 0)), "a replayed tail");
        assert!(
            st.take(3, 13, 0) && st.take(4, 1, 0),
            "new entries, other groups"
        );
        assert!(
            st.take(0, 0, 0) && st.take(0, 0, 0),
            "not from a Raft entry: always"
        );
        // Depth from a feed behind the group is not current.
        assert!(!st.current(3, 12) && st.current(3, 13) && st.current(9, 0));
    }

    #[test]
    fn candle_history_and_replay_watermarks_restart_atomically() {
        let mut state = State {
            agg: KlineAggregator::new(10),
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied: HashMap::from([(3, (42, 7)), (9, (11, 2))]),
            incarnations: HashMap::from([(3, 555), (12, 777)]),
            last_ts: 1_700_000_123,
        };
        state
            .agg
            .on_trade(InstrumentId(17), state.last_ts, 1_005, 9);

        let bytes = encode_market_state(&state);
        let (agg, applied, incarnations, last_ts) = decode_market_state(&bytes).unwrap();
        // Group 12 has an incarnation but no trade yet: an empty mark.
        let mut expected = state.applied.clone();
        expected.insert(12, (0, 0));
        assert_eq!(applied, expected);
        assert_eq!(incarnations, state.incarnations);
        assert_eq!(last_ts, state.last_ts);
        assert_eq!(
            agg.candles(InstrumentId(17), "1s", 10),
            state.agg.candles(InstrumentId(17), "1s", 10)
        );

        let mut restarted = State {
            agg,
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied,
            incarnations,
            last_ts,
        };
        assert!(!restarted.take(3, 42, 7), "saved event is not recounted");
        assert!(
            restarted.take(3, 42, 8),
            "the next event remains admissible"
        );
    }

    #[test]
    fn a_rebuilt_group_restarts_its_watermark() {
        let mut st = State {
            agg: KlineAggregator::new(10),
            depth: DepthStore::default(),
            trades: HashMap::new(),
            applied: HashMap::new(),
            incarnations: HashMap::new(),
            last_ts: 0,
        };
        st.on_incarnation(3, 0); // not assigned yet: ignored
        st.on_incarnation(3, 111);
        assert!(st.take(3, 5_000, 0));
        st.on_incarnation(3, 111); // every replica, every heartbeat: no-op
        assert!(!st.take(3, 10, 0), "a replay of the same incarnation");
        // The group was rebuilt from empty logs: indexes restart at 1.
        st.on_incarnation(3, 222);
        assert!(st.take(3, 10, 0), "the new incarnation's trades count");
        assert!(st.current(3, 10));
    }

    #[test]
    fn order_api_pool_reuses_keep_alive_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            for _ in 0..2 {
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    assert!(!line.is_empty());
                    if line == "\r\n" {
                        break;
                    }
                }
                reader
                    .get_mut()
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\n{}",
                    )
                    .unwrap();
            }
        });

        let pool = OrderApiPool::new(address.to_string(), "token".into(), 1);
        pool.forward("/orders", "order_id=1").unwrap();
        pool.forward("/orders", "order_id=2").unwrap();
        server.join().unwrap();
        assert_eq!(pool.idle.lock().unwrap().len(), 1);
    }
}
