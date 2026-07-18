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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use trade_core::exchange::{build, ExchangeConfig, StrategyFactory};
use trade_core::gateway;
use trade_core::risk::PriceGuard;
use trade_core::strategy::{PriceTimePriority, ProRata, SizePriority};
use trade_core::types::InstrumentId;
use trade_core::OrderPool;
use trade_core::{log_error, log_info};

/// Set by the SIGTERM/SIGINT handler (async-signal-safe: a lone atomic store).
/// A watcher thread observes it and drives the normal drain path.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, std::sync::atomic::Ordering::SeqCst);
}

fn install_signal_handlers() {
    let handler = on_signal as *const () as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

fn configured_instruments() -> Vec<InstrumentId> {
    let spec = std::env::var("TC_INSTRUMENTS").unwrap_or_else(|_| "0-15".to_string());
    let mut instruments = Vec::new();
    for token in spec.split(',').filter(|s| !s.is_empty()) {
        let (start, end) = match token.split_once('-') {
            Some((start, end)) => (
                start
                    .parse::<u32>()
                    .expect("invalid TC_INSTRUMENTS range start"),
                end.parse::<u32>()
                    .expect("invalid TC_INSTRUMENTS range end"),
            ),
            None => {
                let value = token
                    .parse::<u32>()
                    .expect("invalid TC_INSTRUMENTS instrument id");
                (value, value)
            }
        };
        assert!(start <= end, "TC_INSTRUMENTS range start exceeds end");
        instruments.extend((start..=end).map(InstrumentId));
    }
    instruments.sort_unstable();
    instruments.dedup();
    assert!(
        !instruments.is_empty(),
        "TC_INSTRUMENTS must select at least one instrument"
    );
    instruments
}

fn strategy_by_name(name: &str) -> Option<StrategyFactory> {
    match name {
        "price-time" => Some(|| Box::new(PriceTimePriority)),
        "pro-rata" => Some(|| Box::new(ProRata)),
        "size-priority" => Some(|| Box::new(SizePriority)),
        _ => None,
    }
}

fn main() {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("trade-core");
    install_signal_handlers();

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
        log_error!("trade-core", "unknown strategy '{strat_name}' (price-time | pro-rata | size-priority)");
        std::process::exit(2);
    };

    // Pre-listed instruments: their books and memory pools are reserved at
    // startup. Configure a machine's ownership with TC_INSTRUMENTS, e.g.
    // `1-5000` on node A and `5001-10000` on node B.
    let instruments = configured_instruments();
    let slot = OrderPool::slot_bytes();
    let pool_orders_per_book = (pool_mb * 1024 * 1024) / slot / instruments.len().max(1);
    log_info!(
        "trade-core",
        "shards={shards} strategy={strat_name} | pool: {pool_mb} MB = \
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
        log_info!("trade-core", "journaling DISABLED");
        None
    } else {
        log_info!("trade-core", "journaling to {journal_dir} (flush 1s, fsync 1s)");
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
        raft_wal_authoritative: false,
        journal_flush: Duration::from_secs(1),
        journal_fsync: Duration::from_secs(1),
        price_guard: Some(guard),
        pin_cpus: true,
        // Snapshot every 30 s: recovery replays at most 30 s of journal, and the
        // journal file never grows past one snapshot interval.
        snapshot_every: Some(Duration::from_secs(30)),
        // Maker/taker fees: 10/20 bps demo schedule (account system settles these).
        fees: trade_core::fees::FeeSchedule {
            maker_bps: 10,
            taker_bps: 20,
        },
        // Reject an order that would trade against the same user's resting order.
        stp: trade_core::SelfTradePolicy::CancelTaker,
        // Order-system re-sends after reconnect are deduped by the id cursor.
        dedup_commands: true,
        risk_limits: Some(trade_core::RiskLimits {
            max_order_qty: 1_000_000,
            max_notional: 10_000_000_000,
            max_user_orders: 10_000,
        }),
        execution_outbox_dir: None,
        raft_group_id: 0,
        execution_outbox_sync_every: 1,
    });

    // Standalone mode becomes ready once recovery/build has completed. The
    // clustered runtime will hold this false until the node has a current
    // quorum and a fully replayed committed log.
    handle.metrics.set_ready(true);

    trade_core::metrics::serve(
        format!(
            "{}:9102",
            addr.rsplit_once(':').map(|(h, _)| h).unwrap_or("0.0.0.0")
        ),
        handle.metrics.clone(),
    );
    let running = Arc::new(AtomicBool::new(true));
    let listener = std::net::TcpListener::bind(&addr).expect("bind order port");
    let md_listener = (md_addr != "none")
        .then(|| std::net::TcpListener::bind(&md_addr).expect("bind market-data port"));
    // Intake throttle: 0 = unlimited (tune per deployment).
    let rate: u64 = std::env::var("TC_MAX_CMDS_PER_SEC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    // Graceful stop: a watcher thread turns a SIGTERM/SIGINT (e.g. `docker
    // stop`) into the normal drain path — clear `running` so the gateway's
    // serve loop returns, then nudge a possibly-blocked `accept()` by opening a
    // throwaway self-connection. `handle.shutdown()` below then drains and does
    // the final journal flush instead of the process being SIGKILLed.
    let watch_running = running.clone();
    let watch_addr = addr.clone();
    std::thread::spawn(move || {
        while !SHUTDOWN.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
        }
        log_info!("trade-core", "shutdown signal received, draining");
        watch_running.store(false, Ordering::Release);
        let _ = std::net::TcpStream::connect(&watch_addr);
    });

    match gateway::serve_forever(listener, md_listener, gw, sink, running, rate) {
        Ok(()) => log_info!("trade-core", "connection closed, shutting down"),
        Err(e) => log_error!("trade-core", "error: {e}"),
    }
    handle.shutdown();
}
