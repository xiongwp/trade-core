//! Acceptance tests for snapshot recovery, self-trade prevention, and static
//! pre-trade risk limits.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use trade_core::exchange::{build, recover_into, ExchangeConfig, ExecReport, Processor};
use trade_core::prelude::*;
use trade_core::{Command, InstrumentId, RiskLimits, SelfTradePolicy};

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tc-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn drain_all(sink: &trade_core::ResultSink) -> Vec<ExecReport> {
    let mut reports = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut idle = 0;
    while Instant::now() < deadline && idle < 2000 {
        if sink.poll(|r| reports.push(r)) == 0 {
            idle += 1;
            std::thread::yield_now();
        } else {
            idle = 0;
        }
    }
    reports
}

fn flow(seed: u64, id_base: u64, n: u64) -> Vec<Command> {
    let mut state = seed;
    let mut next = move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545F4914F6CDD1D)
    };
    (1..=n)
        .map(|i| {
            let r = next();
            let side = if r & 1 == 0 { Side::Buy } else { Side::Sell };
            Command::New(Order::limit(OrderId(id_base + i), side, 990 + r % 21, 1 + r % 50))
        })
        .collect()
}

/// The core snapshot property: (snapshot at time T) + (journal after T) must
/// reconstruct exactly the state that a continuous run reaches.
#[test]
fn snapshot_plus_journal_tail_equals_continuous_state() {
    let dir = temp_dir("snap-rec");
    let cfg = ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(5),
        // Manual snapshots only (via snapshot_now), so the test controls timing.
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // Phase A, then snapshot (which also truncates the journal).
    for cmd in flow(0xAAAA, 0, 2000) {
        gw.submit(cmd).expect("queue sized for the test");
    }
    let _ = drain_all(&sink);
    handle.snapshot_now();
    std::thread::sleep(Duration::from_millis(100)); // let the shard snapshot

    // Phase B lands in the (now truncated) journal after the snapshot.
    for cmd in flow(0xBBBB, 10_000, 1500) {
        gw.submit(cmd).expect("queue sized for the test");
    }
    let _ = drain_all(&sink);
    handle.shutdown(); // "crash" point: journal flushed by shutdown

    // Ground truth: one continuous processor over the identical A+B commands.
    let mut truth = Processor::new(|| Box::new(PriceTimePriority), None);
    for cmd in flow(0xAAAA, 0, 2000).into_iter().chain(flow(0xBBBB, 10_000, 1500)) {
        truth.process(cmd, &mut |_| {});
    }

    // Recover: snapshot + journal tail.
    let mut recovered = Processor::new(|| Box::new(PriceTimePriority), None);
    let applied = recover_into(
        &mut recovered,
        &dir.join("snapshot-shard-0.bin"),
        &dir.join("journal-shard-0.bin"),
    )
    .unwrap();

    assert!(
        applied <= 1500,
        "journal tail must only contain phase B ({applied} applied) — truncation worked"
    );
    assert_eq!(
        recovered.state_fingerprint(),
        truth.state_fingerprint(),
        "snapshot + tail must equal continuous execution"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Restart-in-place: build() itself must pick up snapshot + journal from disk.
#[test]
fn exchange_recovers_state_on_restart() {
    let dir = temp_dir("snap-restart");
    let mk = || ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(5),
        snapshot_every: Some(Duration::from_secs(3600)), // final-snapshot on shutdown
        ..ExchangeConfig::default()
    };

    // First life: rest an order book, then shut down cleanly.
    let (gw, sink, handle) = build(mk());
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 105, 7)).unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 95, 3)).unwrap();
    let _ = drain_all(&sink);
    handle.shutdown();

    // Second life: the resting orders must be back; a crossing buy proves it.
    let (gw, sink, handle) = build(mk());
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 105, 7)).unwrap();
    let reports = drain_all(&sink);
    handle.shutdown();

    assert!(
        reports.iter().any(|r| matches!(
            r, ExecReport::Trade { taker, maker, price, qty, .. }
            if *taker == OrderId(3) && *maker == OrderId(1) && *price == 105 && *qty == 7)),
        "restart must restore the resting ask; got {reports:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn stp_cancel_taker_blocks_self_trade() {
    let cfg = ExchangeConfig { stp: SelfTradePolicy::CancelTaker, ..ExchangeConfig::default() };
    let (gw, sink, handle) = build(cfg);

    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 5).by(7)).unwrap();
    // Same user crosses own order: taker cancelled, maker survives.
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 100, 5).by(7)).unwrap();
    // A different user CAN trade with it.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 100, 5).by(8)).unwrap();

    let reports = drain_all(&sink);
    handle.shutdown();

    assert!(!reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, .. } if *taker == OrderId(2))),
        "self-trade must not print: {reports:?}");
    assert!(reports.contains(&ExecReport::Cancelled {
        instrument: InstrumentId(0),
        order_id: OrderId(2),
    }));
    assert!(reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, maker, .. }
        if *taker == OrderId(3) && *maker == OrderId(1))));
}

#[test]
fn stp_cancel_maker_pulls_resting_and_matches_rest() {
    let cfg = ExchangeConfig { stp: SelfTradePolicy::CancelMaker, ..ExchangeConfig::default() };
    let (gw, sink, handle) = build(cfg);

    // User 7's stale quote sits ahead of user 8's at the same price.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 5).by(7)).unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Sell, 100, 5).by(8)).unwrap();
    // User 7 crosses: own maker #1 is cancelled, taker fills against #2.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 100, 5).by(7)).unwrap();

    let reports = drain_all(&sink);
    handle.shutdown();

    assert!(reports.contains(&ExecReport::Cancelled {
        instrument: InstrumentId(0),
        order_id: OrderId(1),
    }), "self maker must be cancelled: {reports:?}");
    assert!(reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, maker, qty, .. }
        if *taker == OrderId(3) && *maker == OrderId(2) && *qty == 5)),
        "taker must fill against the other user: {reports:?}");
}

#[test]
fn risk_limits_reject_oversize_and_order_count() {
    let cfg = ExchangeConfig {
        risk_limits: Some(RiskLimits {
            max_order_qty: 100,
            max_notional: 50_000,
            max_user_orders: 2,
        }),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);
    let sym = InstrumentId(0);

    gw.new_order(Order::limit(OrderId(1), Side::Buy, 10, 101)).unwrap(); // qty > 100
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 1000, 100)).unwrap(); // notional 100k > 50k
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 10, 100).by(9)).unwrap(); // ok
    gw.new_order(Order::limit(OrderId(4), Side::Buy, 11, 100).by(9)).unwrap(); // ok
    gw.new_order(Order::limit(OrderId(5), Side::Buy, 12, 100).by(9)).unwrap(); // 3rd open order

    let reports = drain_all(&sink);
    handle.shutdown();

    let rejected_with = |id: u64, why: &str| {
        reports.iter().any(|r| matches!(
            r, ExecReport::Rejected { order_id, reason, .. }
            if *order_id == OrderId(id) && *reason == why))
    };
    assert!(rejected_with(1, "max-qty"), "{reports:?}");
    assert!(rejected_with(2, "max-notional"), "{reports:?}");
    assert!(rejected_with(5, "max-user-orders"), "{reports:?}");
    assert!(reports.contains(&ExecReport::Resting { instrument: sym, order_id: OrderId(4) }));
}
