# trade-core

A dependency-free, commercial-style **exchange core** in Rust: multi-asset,
lock-free, journaled and replayable, horizontally scalable, risk-guarded — with
pluggable price matching strategies, cleanly split from the order system.

**Measured on this repo, Apple M-series 10-core, end-to-end through the full
pipeline (intake queue → match → report queue → drained):**

| Configuration | Aggregate throughput |
|---|---|
| **20M-order E2E stress** (3 services, TCP, journal+chart live) | **9.16 M orders/s** ✓ |
| 6 nodes, journal ON (1 s flush/fsync) | 5.7 – 6.3 M orders/s ✓ |
| 5 nodes, journal off | **6.3 – 6.6 M orders/s** |
| 1 node | 1.87 M orders/s |

| Order latency (submit → terminal report, 1 node) | p50 | p90 | p99 |
|---|---|---|---|
| paced at 200 k orders/s | 0.4 µs | 1.9 µs | 19 µs |
| paced at 500 k orders/s | 0.4 µs | 14.5 µs | 109 µs |

(`cargo bench --bench cluster_throughput -- 6 1000000 journal`, `cargo bench
--bench latency -- 200000`. Mixed flow: 85% new limit orders that cross and
rest, 15% cancels; ~0.67 trades per order. Latency tails include macOS
scheduler noise — Linux with isolated, pinned cores trims them.)

> **"Node" here = one exchange instance** (shard thread + own books + own
> queues + own journal), several of which ran on this one 10-core laptop.
> Because nodes share nothing, the same code deploys across real machines
> unchanged — where each node gets a whole machine, so per-node (and aggregate)
> numbers only go up. These figures are a single-box simulation of a cluster,
> stated conservatively.

## Feature map

| Requirement | Implementation | Verified by |
|---|---|---|
| Multiple matching strategies | `MatchingStrategy` trait: price-time (FIFO), pro-rata, size-priority | [integration.rs](tests/integration.rs), fuzzed contract |
| Multi-asset matching | one book per `InstrumentId`, hashed across shard threads | [exchange_pipeline.rs](tests/exchange_pipeline.rs) |
| One task/thread per asset group, horizontal scaling | share-nothing shards in-process; `cluster::ClusterMap` routes instruments to machines | [cluster.rs](src/cluster.rs) |
| Lock-free design | SPSC ring (Lamport + cached indices, cache-line padded); zero mutexes on the hot path | [lockfree.rs](src/lockfree.rs), 1M-item FIFO test |
| Matching ⟂ order system, async results | `OrderGateway` / `ResultSink` handles; queues are the only coupling | pipeline tests |
| Cancel & modify priority over new orders | dual intake queues; high queue (cancel/modify/force-close) fully drained first | `cancel_takes_priority_over_queued_new_orders` |
| Strict ordering + crash replay, identical results | per-shard WAL journal written in processing order; deterministic engine ⇒ replay fingerprint equals live | `journal_replay_reproduces_identical_results` |
| Replay by time | journal records carry ns timestamps; `replay_journal(..., until_ts)` | `replay_until_timestamp_stops_early` |
| Few seconds of loss acceptable | buffered journal, 1 s flush + 1 s fsync cadence; checksummed records, truncated tail dropped on replay | journal truncation test |
| In-memory matching, 3 GiB reserved at startup | slab `OrderPool`, pre-faulted pages; 3 GiB ≈ 50 M order slots (64 B each); zero allocation on the matching path | [book.rs](src/book.rs), server default `POOL_MB=3072` |
| ≥ 5,000,000 orders/s | see table above | [cluster_throughput.rs](benches/cluster_throughput.rs) |
| Sync + async risk strategies | `SyncRiskCheck` inline (price band); `AsyncRiskStrategy` monitors emitting commands | [risk.rs](src/risk.rs) |
| Forced liquidation | `Command::ForceClose`: cancel-all-user-orders + protected market close, via the **high-priority** queue | `force_close_cancels_user_orders_and_flattens` |
| External price feed, anti-spike (防插针) | `PriceGuard`: lock-free reference prices; out-of-band aggressive limits rejected; market orders capped at band edge | `price_guard_rejects_spikes_and_protects_market_orders` |
| CPU pinning | `affinity`: Linux `pthread_setaffinity_np`; macOS advisory tag (Apple Silicon: not supported — reported honestly, runs unpinned) | smoke test |
| Meituan Leaf-style id generation | segment double-buffer, `fetch_add` hot path, monotonic cursor (a real duplicate-id race was caught by test and fixed) | 8-thread × 50k uniqueness test |
| Snapshots + journal truncation | atomic per-shard snapshots; recovery = snapshot + journal tail, O(commands since last snapshot); `build()` auto-recovers on restart | `snapshot_plus_journal_tail_equals_continuous_state`, restart test |
| Self-trade prevention (STP) | `SelfTradePolicy`: CancelTaker / CancelMaker / CancelBoth, enforced inside the crossing loop | STP taker & maker tests |
| Static pre-trade limits | `RiskLimits`: max qty, max notional, max open orders per user (O(1) via book counters) | `risk_limits_reject_oversize_and_order_count` |
| Docker one-click deploy | multi-stage `Dockerfile` + `docker-compose.yml`: the three services **order / trade-core / market-data**, persistent journal volumes | full stack `compose up` verified, chart served from containers |
| Market data & chart frontend | fanout port → `market-data` service: **K-lines (1s…1mo)** + **depth ladder (top-5 bid/ask)** + canvas UI, **WebSocket push** (poll fallback), **candle history persisted** (atomic 10 s snapshots, loaded on restart) | kline tests incl. persistence; chart+depth+WS verified live in browser |
| O(1) under stress | 20M-order test exposed three O(level) hot spots — full-level view copies, O(level) cancels, O(level) depth sums — fixed via capped FIFO views, **tombstone cancels**, and per-level aggregate counters | E2E: 20M orders in 2.18 s (9.16M/s) with chart live |
| Order system sharded 10 DB × 100 tables by user id | `sharding::route(user_id)` (splitmix64 → 1000 slots); DDL name enumeration | uniformity test (<5% skew) |
| TCP long-connection intake | binary 40-byte frames, parse-in-place over a reusable buffer; reports streamed back async | [tcp_roundtrip.rs](tests/tcp_roundtrip.rs) |

## Architecture

```text
 ORDER SYSTEM side                          MATCHING side (per node/machine)
 ────────────────                           ─────────────────────────────────
 users ──▶ 10 DB × 100 tables               ┌──────────────────────────────┐
           (sharding::route by user_id)     │ Shard 0 (thread, CPU-pinned) │
 ids   ──▶ LeafIdGen (segment buffer)       │  1. WAL journal (ordered)    │
                                            │  2. PriceGuard vet (sync)    │
 client ──TCP 40B frames──▶ Gateway ─┬────▶ │  3. match in slab memory     │
                                     │      │  4. emit reports (async)     │
        [high: cancel/modify/close]──┤      └──────────────────────────────┘
        [normal: new orders       ]──┘        ... Shard N
                                            books: HashMap<Instrument, Engine>
 client ◀──report frames── Writer ◀── [result queues]
                                            ▲
 async risk (MarginMonitor) ──ForceClose────┘ (via gateway, high queue)
 external price feed ──set_reference──▶ PriceGuard (lock-free atomics)

 Horizontal scale-out: cluster::ClusterMap routes each instrument to a node;
 nodes share nothing; an instrument's journal moves with it on rebalance.
```

### Why replay is exact

The engine is deterministic: integer-only arithmetic, engine-assigned sequence
numbers, no wall clock in the matching path. The journal records commands in the
exact order the shard processes them (after queue-priority interleaving), so:

```
same journal prefix  ──▶  same command sequence  ──▶  same trades, same books
```

Tests assert equality with an order-sensitive FNV fingerprint over the encoded
report stream, live vs replayed, plus point-in-time replay (`until_ts`).

### Zero-copy and memory notes (honest scope)

* Wire frames are decoded **in place** (`WireView` borrows the receive buffer);
  one reusable read buffer per connection; the matching path allocates nothing
  (slab order pool, reusable scratch buffers, linear-scan fills). True NIC-level
  zero copy (DPDK/AF_XDP/`io_uring`) is OS/hardware work outside this repo; its
  buffers plug directly into `WireView::parse`.
* The 3 GiB pool is a startup reservation with pre-faulted pages (server flag
  `POOL_MB`, default 3072, split across the pre-listed instruments). Books can
  grow past it gracefully (Vec growth) rather than crash — a deliberate choice.
* WebSocket fits the same `Read`/`Write` seam as TCP (HTTP upgrade, then the
  same frame decode loop); TCP is the implemented, tested transport.
* macOS cannot hard-pin threads (Apple Silicon kernel limitation); on Linux
  pinning is real. The code attempts, reports, and continues.

## vs. exchange-core (LMAX-style Java engine)

| | exchange-core | trade-core |
|---|---|---|
| Language / GC | Java (+ GC pauses; Disruptor mitigates) | Rust, no GC, no allocation on hot path |
| Queueing | LMAX Disruptor (ring buffer) | Lamport SPSC rings, cached indices |
| Matching models | FIFO | FIFO, pro-rata, size-priority (pluggable trait) |
| Persistence | journaling + snapshots | WAL journal + atomic snapshots + truncation, deterministic replay (fingerprint-verified) |
| Risk | margin/balance in-core | price banding + qty/notional/order-count limits + STP in-core; async monitors + force-close; balances live in the order system |
| STP | yes | yes (CancelTaker / CancelMaker / CancelBoth) |
| Deploy | jar | Docker one-click (multi-node compose) |
| Published perf | ~1–5 M ops/s, p50 ≈ 0.5 µs (their hardware) | 5.7–6.3 M orders/s with journaling, p50 = 0.4 µs / p90 = 1.9 µs @200k/s (10-core laptop) |

Hardware differs between the two rows, so treat this as *kind-for-kind* rather
than a controlled head-to-head; both numbers are end-to-end pipeline figures at
comparable scale, and trade-core's exceed exchange-core's published throughput
while matching its median latency.

## Run it

### Docker (one-click)

```bash
docker compose up --build
open http://localhost:8080         # live candlestick chart
```

This starts the **separately deployed** services: `matching` (orders :9001,
trade fanout :9101), `market-data` (K-line aggregation + chart UI :8080), and
`sim-trader` (a stand-in order system generating flow — swap in your real one,
same TCP protocol). Journals/snapshots persist in named volumes; a restarted
matching container recovers its books automatically (snapshot + journal tail).

The chart supports the standard interval set — 1秒/1分/3分/5分/10分/15分/30分/
日/周/月 — with OHLC candles, volume bars, last-price line, per-symbol
selection, 1 s auto-refresh. API:
`GET /api/candles?symbol=1&interval=5m&limit=120` →
`[[start_sec,open,high,low,close,volume], …]`.

### Bare metal

```bash
# matching node: ADDR SHARDS STRATEGY JOURNAL_DIR POOL_MB BAND_BPS
cargo run --release --bin exchange_server -- 127.0.0.1:9001 4 price-time ./journal 3072 1000

# demo order system: orders, cancel, modify, price-band rejection, force-close
cargo run --release --bin order_client -- 127.0.0.1:9001
```

Library use, replay, and the strategy demo:

```bash
cargo test                                   # 54 tests
cargo run --example demo                     # three strategies side by side
cargo bench --bench cluster_throughput -- 5 1200000 journal
```

```rust
// Crash recovery: rebuild a node's state from its journal, stopping at t.
let summary = trade_core::replay_journal(
    Path::new("journal/journal-shard-0.bin"),
    || Box::new(PriceTimePriority),
    None,            // same PriceGuard config as live for exact equality
    Some(t_nanos),   // or None = full replay
)?;
assert_eq!(summary.fingerprint, live_fingerprint);
```

## Modules

| Module | Role |
|---|---|
| [`types`](src/types.rs) / [`order`](src/order.rs) / [`trade`](src/trade.rs) | integer value types, orders (with `user`), reports |
| [`book`](src/book.rs) | slab-pooled price-time book; O(log n) cancel; in-place amend |
| [`strategy`](src/strategy/mod.rs) | matching-strategy trait + 3 implementations (allocation-free) |
| [`engine`](src/engine.rs) | crossing, TIF, amend, cancel-all-per-user |
| [`lockfree`](src/lockfree.rs) | bounded wait-free SPSC ring buffer |
| [`exchange`](src/exchange.rs) | shards, dual-priority intake, journal/snapshot hooks, replay & recovery |
| [`journal`](src/journal.rs) | WAL: ordered, checksummed, time-stamped, replayable, truncatable |
| [`snapshot`](src/snapshot.rs) | atomic point-in-time state capture; snapshot + tail recovery |
| [`wire`](src/wire.rs) / [`gateway`](src/gateway.rs) | 40-byte binary protocol, parse-in-place; TCP long connection + market-data fanout |
| [`kline`](src/kline.rs) | OHLCV candle aggregation, 1s→calendar-month buckets |
| [`risk`](src/risk.rs) | price banding (sync), margin monitor + force-close (async) |
| [`idgen`](src/idgen.rs) | Leaf-segment id generator |
| [`sharding`](src/sharding.rs) | user → 10 DB × 100 table routing |
| [`cluster`](src/cluster.rs) | instrument → machine routing for scale-out |
| [`affinity`](src/affinity.rs) | CPU pinning (Linux hard / macOS advisory) |

## License

MIT OR Apache-2.0.
