//! Integration tests for the sharded, lock-free exchange runtime.

use std::time::{Duration, Instant};
use trade_core::exchange::{build, ExchangeConfig, ExecReport};
use trade_core::prelude::*;
use trade_core::InstrumentId;

/// Collect reports until `want` of them arrive or a timeout elapses.
fn collect(sink: &trade_core::ResultSink, want: usize) -> Vec<ExecReport> {
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < want && Instant::now() < deadline {
        let n = sink.poll(|r| got.push(r));
        if n == 0 {
            std::thread::yield_now();
        }
    }
    got
}

#[test]
fn matches_across_multiple_assets_independently() {
    let cfg = ExchangeConfig {
        shards: 4,
        queue_capacity: 1024,
        strategy: || Box::new(PriceTimePriority),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    let aapl = InstrumentId(101);
    let btc = InstrumentId(202);

    // Resting asks on two different instruments.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 10).on(aapl)).unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Sell, 5000, 3).on(btc)).unwrap();
    // Aggressors on each.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 100, 10).on(aapl)).unwrap();
    gw.new_order(Order::limit(OrderId(4), Side::Buy, 5000, 3).on(btc)).unwrap();

    let reports = collect(&sink, 8); // 4 Accepted + 2 Trade + 2 Filled at least
    handle.shutdown();

    // A trade printed on each instrument, isolated from the other.
    let aapl_trade = reports.iter().any(|r| matches!(
        r, ExecReport::Trade { instrument, price, qty, .. }
        if *instrument == aapl && *price == 100 && *qty == 10));
    let btc_trade = reports.iter().any(|r| matches!(
        r, ExecReport::Trade { instrument, price, qty, .. }
        if *instrument == btc && *price == 5000 && *qty == 3));
    assert!(aapl_trade, "expected an AAPL trade; got {reports:?}");
    assert!(btc_trade, "expected a BTC trade; got {reports:?}");
}

#[test]
fn async_results_report_full_lifecycle() {
    let (gw, sink, handle) = build(ExchangeConfig::default());
    let sym = InstrumentId(0);

    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 5)).unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 100, 5)).unwrap();

    let reports = collect(&sink, 5);
    handle.shutdown();

    assert!(reports.contains(&ExecReport::Accepted { instrument: sym, order_id: OrderId(1) }));
    assert!(reports.contains(&ExecReport::Resting { instrument: sym, order_id: OrderId(1) }));
    assert!(reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, maker, qty, .. }
        if *taker == OrderId(2) && *maker == OrderId(1) && *qty == 5)));
    assert!(reports.contains(&ExecReport::Filled { instrument: sym, order_id: OrderId(2) }));
}

#[test]
fn cancel_takes_priority_over_queued_new_orders() {
    let cfg = ExchangeConfig {
        shards: 1,
        queue_capacity: 1 << 14,
        strategy: || Box::new(PriceTimePriority),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);
    let sym = InstrumentId(0);

    // Rest a maker and confirm it is actually on the book.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 1_000)).unwrap();
    let acked = collect(&sink, 2); // Accepted + Resting
    assert!(acked.contains(&ExecReport::Resting { instrument: sym, order_id: OrderId(1) }));

    // Quiesce the shard, THEN stage a wall of crossing buys (normal queue) and a
    // cancel of the resting maker (high queue). Nothing is draining yet.
    handle.pause();
    for i in 0..500u64 {
        gw.new_order(Order::limit(OrderId(1000 + i), Side::Buy, 100, 1)).unwrap();
    }
    gw.cancel(sym, OrderId(1)).unwrap();

    // Resume: the shard drains HIGH first, so the maker is cancelled before any
    // queued buy runs -> the whole wall of buys finds an empty book.
    handle.resume();

    // Drain reports until quiescent.
    std::thread::sleep(Duration::from_millis(50));
    let mut reports = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut idle_rounds = 0;
    while Instant::now() < deadline && idle_rounds < 1000 {
        if sink.poll(|r| reports.push(r)) == 0 {
            idle_rounds += 1;
            std::thread::yield_now();
        } else {
            idle_rounds = 0;
        }
    }
    handle.shutdown();
    sink.poll(|r| reports.push(r));

    let maker_cancelled = reports
        .iter()
        .any(|r| matches!(r, ExecReport::Cancelled { order_id, .. } if *order_id == OrderId(1)));
    let any_trade = reports.iter().any(|r| matches!(r, ExecReport::Trade { .. }));
    assert!(maker_cancelled, "maker #1 should have been cancelled");
    assert!(
        !any_trade,
        "cancel must be processed before queued new orders, so nothing should trade; got {reports:?}"
    );
}

#[test]
fn modify_reduce_keeps_priority_and_reports_modified() {
    let (gw, sink, handle) = build(ExchangeConfig::default());
    let sym = InstrumentId(0);

    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 10)).unwrap();
    let _ = collect(&sink, 2);
    // Reduce 10 -> 4 at same price: keeps priority, reports Modified.
    gw.modify(sym, OrderId(1), 100, 4).unwrap();

    let reports = collect(&sink, 1);
    handle.shutdown();
    assert!(reports.contains(&ExecReport::Modified {
        instrument: sym,
        order_id: OrderId(1),
        remaining: 4,
    }));
}
