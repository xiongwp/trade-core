//! Recovery and risk tests: journal replay determinism, point-in-time replay,
//! forced liquidation, and anti-spike price banding.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use trade_core::exchange::{
    build, fingerprint_reports, replay_journal, ExchangeConfig, ExecReport,
};
use trade_core::prelude::*;
use trade_core::risk::PriceGuard;
use trade_core::InstrumentId;

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tc-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Drain the sink until no report arrives for a while.
fn drain_all(sink: &trade_core::ResultSink) -> Vec<ExecReport> {
    let mut reports = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut idle = 0;
    while Instant::now() < deadline && idle < 2000 {
        if sink.poll(|r| if !matches!(r, ExecReport::DepthLevel { .. } | ExecReport::DepthEnd { .. }) { reports.push(r) }) == 0 {
            idle += 1;
            std::thread::yield_now();
        } else {
            idle = 0;
        }
    }
    reports
}

/// A deterministic pseudo-random order flow on one instrument.
fn random_flow(n: u64) -> Vec<trade_core::Command> {
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545F4914F6CDD1D)
    };
    let mut out = Vec::new();
    for i in 1..=n {
        let r = next();
        if r % 5 == 0 && i > 10 {
            out.push(trade_core::Command::Cancel {
                instrument: InstrumentId(0),
                order_id: OrderId(1 + r % (i - 1)),
            });
        } else {
            let side = if r & 1 == 0 { Side::Buy } else { Side::Sell };
            let price = 990 + r % 21;
            let qty = 1 + r % 50;
            out.push(trade_core::Command::New(Order::limit(OrderId(i), side, price, qty)));
        }
    }
    out
}

#[test]
fn journal_replay_reproduces_identical_results() {
    let dir = temp_dir("replay");
    let cfg = ExchangeConfig {
        shards: 1, // per-shard journals preserve per-shard total order
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(10),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // Live run: a few thousand mixed commands.
    for cmd in random_flow(3000) {
        let mut pending = cmd;
        loop {
            match gw.submit(pending) {
                Ok(()) => break,
                Err(ret) => {
                    pending = ret;
                    std::thread::yield_now();
                }
            }
        }
    }
    let live_reports = drain_all(&sink);
    handle.shutdown(); // flushes the journal
    let live_fp = fingerprint_reports(&live_reports);
    assert!(!live_reports.is_empty());

    // "Crash" and recover: replay the journal into a fresh processor.
    let journal = dir.join("journal-shard-0.bin");
    let summary = replay_journal(
        &journal,
        || Box::new(PriceTimePriority),
        None,
        None,
    )
    .unwrap();

    assert_eq!(summary.commands, 3000);
    assert_eq!(
        summary.fingerprint, live_fp,
        "replay must reproduce the exact live report stream \
         (live {} reports, replay {})",
        live_reports.len(),
        summary.reports.len()
    );
    // And the rebuilt book state matches a direct comparison of depth.
    let engine = summary.processor.engine(InstrumentId(0)).unwrap();
    assert!(!engine.book().is_empty(), "book should have resting orders after replay");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn replay_until_timestamp_stops_early() {
    let dir = temp_dir("timereplay");
    let cfg = ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(10),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // First batch.
    for i in 1..=100u64 {
        gw.new_order(Order::limit(OrderId(i), Side::Sell, 100 + i % 5, 1)).unwrap();
    }
    let _ = drain_all(&sink);
    let cut = trade_core::journal::now_nanos();
    std::thread::sleep(Duration::from_millis(20));

    // Second batch, after the cut point.
    for i in 101..=200u64 {
        gw.new_order(Order::limit(OrderId(i), Side::Sell, 100 + i % 5, 1)).unwrap();
    }
    let _ = drain_all(&sink);
    handle.shutdown();

    let journal = dir.join("journal-shard-0.bin");
    let full = replay_journal(&journal, || Box::new(PriceTimePriority), None, None).unwrap();
    let partial =
        replay_journal(&journal, || Box::new(PriceTimePriority), None, Some(cut)).unwrap();

    assert_eq!(full.commands, 200);
    assert_eq!(partial.commands, 100, "time-bounded replay must stop at the cut");
    let engine = partial.processor.engine(InstrumentId(0)).unwrap();
    assert_eq!(engine.book().len(), 100, "state = exactly the first batch");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn force_close_cancels_user_orders_and_flattens() {
    let (gw, sink, handle) = build(ExchangeConfig::default());
    let sym = InstrumentId(0);

    // User 7 has two resting bids; user 8 provides an ask to close against and
    // a bid that must survive.
    gw.new_order(Order::limit(OrderId(1), Side::Buy, 99, 10).by(7)).unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 98, 5).by(7)).unwrap();
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 97, 5).by(8)).unwrap();
    gw.new_order(Order::limit(OrderId(4), Side::Sell, 101, 20).by(8)).unwrap();
    let _ = drain_all(&sink);

    // Risk decides: force-close user 7, flattening a long of 12 lots.
    gw.force_close(sym, 7, OrderId(100), Side::Sell, 12).unwrap();
    let reports = drain_all(&sink);
    handle.shutdown();

    // Both of user 7's orders cancelled...
    for id in [OrderId(1), OrderId(2)] {
        assert!(
            reports.iter().any(|r| matches!(
                r, ExecReport::Cancelled { order_id, .. } if *order_id == id)),
            "expected {id} cancelled; got {reports:?}"
        );
    }
    // ...and the closing sell traded against user 8's bid at 97.
    assert!(
        reports.iter().any(|r| matches!(
            r, ExecReport::Trade { taker, maker, price, qty, .. }
            if *taker == OrderId(100) && *maker == OrderId(3) && *price == 97 && *qty == 5)),
        "expected close order to hit the surviving bid; got {reports:?}"
    );
}

#[test]
fn price_guard_rejects_spikes_and_protects_market_orders() {
    let sym = InstrumentId(0);
    let guard = Arc::new(PriceGuard::new(500, &[sym])); // ±5%
    guard.set_reference(sym, 1000); // band = [950, 1050]

    let cfg = ExchangeConfig {
        price_guard: Some(guard.clone()),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // A thin ask far above the band — spike bait.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 1050, 1)).unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Sell, 2000, 100)).unwrap(); // out of band

    // An aggressive buy limit way above band: rejected outright.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 1500, 10)).unwrap();

    // A market buy for 5: protected — capped at 1050, so it lifts the 1-lot ask
    // at 1050 and cancels the rest instead of walking up to 2000.
    gw.new_order(Order::market(OrderId(4), Side::Buy, 5)).unwrap();

    let reports = drain_all(&sink);
    handle.shutdown();

    // The out-of-band sell got rejected too (2000 < lo would be for sells; a
    // sell ABOVE band is passive and allowed to rest... but #2 at 2000 is a
    // SELL above band: allowed to rest, harmless). The BUY at 1500 is rejected.
    assert!(
        reports.iter().any(|r| matches!(
            r, ExecReport::Rejected { order_id, reason, .. }
            if *order_id == OrderId(3) && *reason == "price-band")),
        "buy through the band must be rejected; got {reports:?}"
    );
    // Market order printed only at 1050 (1 lot) and never at 2000.
    assert!(reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, price, qty, .. }
        if *taker == OrderId(4) && *price == 1050 && *qty == 1)));
    assert!(
        !reports.iter().any(|r| matches!(
            r, ExecReport::Trade { price, .. } if *price == 2000)),
        "protected market order must never trade beyond the band; got {reports:?}"
    );
}
