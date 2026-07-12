//! The matching-engine node: sharded, lock-free, journaled, CPU-pinned,
//! listening for a long-lived order-system connection over TCP.
//!
//! Usage:
//!   cargo run --release --bin exchange_server -- \
//!       [ADDR] [SHARDS] [STRATEGY] [JOURNAL_DIR] [POOL_MB] [BAND_BPS] [MD_ADDR]
//!
//!   ADDR         default 127.0.0.1:9001
//!   MD_ADDR      market-data fanout port, default <ADDR host>:9101 ("none" off)
//!   SHARDS       default 4
//!   STRATEGY     price-time (default) | pro-rata | size-priority
//!   JOURNAL_DIR  default ./journal  ("none" disables journaling)
//!   POOL_MB      order-pool memory reserved at startup, MB; default 3072 (3 GiB)
//!   BAND_BPS     anti-spike band half-width in bps; default 1000 (±10%)
//!
//! In a horizontally-scaled deployment, run one such node per machine and give
//! each a disjoint instrument set via a `cluster::ClusterMap` on the order side.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use trade_core::exchange::{build, ExchangeConfig, StrategyFactory};
use trade_core::gateway;
use trade_core::risk::PriceGuard;
use trade_core::strategy::{PriceTimePriority, ProRata, SizePriority};
use trade_core::types::InstrumentId;
use trade_core::OrderPool;

fn strategy_by_name(name: &str) -> Option<StrategyFactory> {
    match name {
        "price-time" => Some(|| Box::new(PriceTimePriority)),
        "pro-rata" => Some(|| Box::new(ProRata)),
        "size-priority" => Some(|| Box::new(SizePriority)),
        _ => None,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:9001".to_string());
    let shards: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let strat_name = args.next().unwrap_or_else(|| "price-time".to_string());
    let journal_dir = args.next().unwrap_or_else(|| "./journal".to_string());
    let pool_mb: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(3072);
    let band_bps: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(1000);
    let md_addr = args.next().unwrap_or_else(|| {
        let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or("127.0.0.1");
        format!("{host}:9101")
    });

    let Some(strategy) = strategy_by_name(&strat_name) else {
        eprintln!("unknown strategy '{strat_name}' (price-time | pro-rata | size-priority)");
        std::process::exit(2);
    };

    // Pre-listed instruments: their books and memory pools are reserved at
    // startup. The pool budget is split evenly across them.
    let instruments: Vec<InstrumentId> = (0..16).map(InstrumentId).collect();
    let slot = OrderPool::slot_bytes();
    let pool_orders_per_book = (pool_mb * 1024 * 1024) / slot / instruments.len().max(1);
    eprintln!(
        "[server] shards={shards} strategy={strat_name} | pool: {pool_mb} MB = \
         {} instruments x {pool_orders_per_book} orders x {slot} B (prefaulted)",
        instruments.len()
    );

    // Anti-spike guard, fed by an external price source. Here a placeholder
    // reference of 1000 ticks per instrument is installed; in production a feed
    // client thread calls `guard.set_reference` on every index-price update.
    let guard = Arc::new(PriceGuard::new(band_bps, &instruments));
    for &inst in &instruments {
        guard.set_reference(inst, 1000);
    }

    let journal = if journal_dir == "none" {
        eprintln!("[server] journaling DISABLED");
        None
    } else {
        eprintln!("[server] journaling to {journal_dir} (flush 1s, fsync 1s)");
        Some(journal_dir.into())
    };

    let (gw, sink, handle) = build(ExchangeConfig {
        shards,
        queue_capacity: 1 << 16,
        strategy,
        instruments,
        pool_orders_per_book,
        prefault: true,
        journal_dir: journal,
        journal_flush: Duration::from_secs(1),
        journal_fsync: Duration::from_secs(1),
        price_guard: Some(guard),
        pin_cpus: true,
        // Snapshot every 30 s: recovery replays at most 30 s of journal, and the
        // journal file never grows past one snapshot interval.
        snapshot_every: Some(Duration::from_secs(30)),
        // Reject an order that would trade against the same user's resting order.
        stp: trade_core::SelfTradePolicy::CancelTaker,
        risk_limits: Some(trade_core::RiskLimits {
            max_order_qty: 1_000_000,
            max_notional: 10_000_000_000,
            max_user_orders: 10_000,
        }),
    });

    let running = Arc::new(AtomicBool::new(true));
    let listener = std::net::TcpListener::bind(&addr).expect("bind order port");
    let md_listener = (md_addr != "none")
        .then(|| std::net::TcpListener::bind(&md_addr).expect("bind market-data port"));
    match gateway::serve_with_md(listener, md_listener, gw, sink, running) {
        Ok(()) => eprintln!("[server] connection closed, shutting down"),
        Err(e) => eprintln!("[server] error: {e}"),
    }
    handle.shutdown();
}
