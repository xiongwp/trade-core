//! End-to-end tests exercising every strategy and engine feature.

use trade_core::prelude::*;

/// Rest three asks at one price: #1=10, #2=30, #3=60 (total 100), oldest first.
fn book_with_level(strategy: Box<dyn trade_core::MatchingStrategy>) -> MatchingEngine {
    let mut e = MatchingEngine::new(strategy);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 10));
    e.submit(Order::limit(OrderId(2), Side::Sell, 100, 30));
    e.submit(Order::limit(OrderId(3), Side::Sell, 100, 60));
    e
}

fn fill_of(report: &SubmitReport, maker: OrderId) -> Qty {
    report
        .trades
        .iter()
        .filter(|t| t.maker == maker)
        .map(|t| t.quantity)
        .sum()
}

#[test]
fn price_time_fills_oldest_first() {
    let mut e = book_with_level(Box::new(PriceTimePriority));
    let r = e.submit(Order::limit(OrderId(9), Side::Buy, 100, 50));
    assert_eq!(r.filled, 50);
    // FIFO: #1 fully (10), #2 fully (30), #3 partially (10).
    assert_eq!(fill_of(&r, OrderId(1)), 10);
    assert_eq!(fill_of(&r, OrderId(2)), 30);
    assert_eq!(fill_of(&r, OrderId(3)), 10);
    // #3 has 50 left resting.
    assert_eq!(e.book().depth(Side::Sell), vec![(100, 50, 1)]);
}

#[test]
fn size_priority_fills_largest_first() {
    let mut e = book_with_level(Box::new(SizePriority));
    let r = e.submit(Order::limit(OrderId(9), Side::Buy, 100, 50));
    assert_eq!(r.filled, 50);
    // Largest first: #3 (60) absorbs the whole 50; others untouched.
    assert_eq!(fill_of(&r, OrderId(3)), 50);
    assert_eq!(fill_of(&r, OrderId(1)), 0);
    assert_eq!(fill_of(&r, OrderId(2)), 0);
    assert_eq!(e.book().depth(Side::Sell), vec![(100, 50, 3)]);
}

#[test]
fn pro_rata_allocates_proportionally() {
    let mut e = book_with_level(Box::new(ProRata));
    let r = e.submit(Order::limit(OrderId(9), Side::Buy, 100, 50));
    assert_eq!(r.filled, 50);
    // 50% of the level: 5, 15, 30 — exact, no rounding needed.
    assert_eq!(fill_of(&r, OrderId(1)), 5);
    assert_eq!(fill_of(&r, OrderId(2)), 15);
    assert_eq!(fill_of(&r, OrderId(3)), 30);
}

#[test]
fn pro_rata_largest_remainder_is_exact() {
    // Three equal orders of 10 (total 30); take 10 -> 3.33 each. Floors to 3 each
    // (=9), one leftover lot goes to the earliest by time priority.
    let mut e = MatchingEngine::with_strategy(ProRata);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 10));
    e.submit(Order::limit(OrderId(2), Side::Sell, 100, 10));
    e.submit(Order::limit(OrderId(3), Side::Sell, 100, 10));
    let r = e.submit(Order::limit(OrderId(9), Side::Buy, 100, 10));
    assert_eq!(r.filled, 10, "must allocate the exact aggressor quantity");
    assert_eq!(fill_of(&r, OrderId(1)), 4); // earliest gets the leftover lot
    assert_eq!(fill_of(&r, OrderId(2)), 3);
    assert_eq!(fill_of(&r, OrderId(3)), 3);
}

#[test]
fn price_time_priority_across_arrival_order() {
    // Same price, staggered arrival — earlier order must fill first.
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Buy, 100, 5)); // oldest bid
    e.submit(Order::limit(OrderId(2), Side::Buy, 100, 5));
    let r = e.submit(Order::limit(OrderId(3), Side::Sell, 100, 5));
    assert_eq!(fill_of(&r, OrderId(1)), 5);
    assert_eq!(fill_of(&r, OrderId(2)), 0);
}

#[test]
fn limit_remainder_rests_and_reports_partial() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 3));
    let r = e.submit(Order::limit(OrderId(2), Side::Buy, 100, 10));
    assert_eq!(r.status, OrderStatus::PartiallyFilled);
    assert_eq!(r.filled, 3);
    assert!(r.resting);
    // 7 lots rest as a bid @100.
    assert_eq!(e.book().depth(Side::Buy), vec![(100, 7, 1)]);
    assert_eq!(e.book().best_bid(), Some(100));
}

#[test]
fn ioc_cancels_remainder() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 3));
    let r = e.submit(Order::limit(OrderId(2), Side::Buy, 100, 10).with_tif(TimeInForce::Ioc));
    assert_eq!(r.status, OrderStatus::PartiallyFilled);
    assert_eq!(r.filled, 3);
    assert!(!r.resting);
    assert!(e.book().is_empty());
}

#[test]
fn fok_all_or_nothing() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 3));

    // Not enough liquidity -> rejected, nothing filled, book untouched.
    let rej = e.submit(Order::limit(OrderId(2), Side::Buy, 100, 10).with_tif(TimeInForce::Fok));
    assert_eq!(rej.status, OrderStatus::Rejected);
    assert_eq!(rej.filled, 0);
    assert_eq!(e.book().depth(Side::Sell), vec![(100, 3, 1)]);

    // Enough liquidity -> fully filled.
    e.submit(Order::limit(OrderId(3), Side::Sell, 100, 7));
    let ok = e.submit(Order::limit(OrderId(4), Side::Buy, 100, 10).with_tif(TimeInForce::Fok));
    assert_eq!(ok.status, OrderStatus::Filled);
    assert_eq!(ok.filled, 10);
}

#[test]
fn market_order_sweeps_levels_and_never_rests() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 5));
    e.submit(Order::limit(OrderId(2), Side::Sell, 101, 5));
    e.submit(Order::limit(OrderId(3), Side::Sell, 102, 5));

    let r = e.submit(Order::market(OrderId(9), Side::Buy, 12));
    assert_eq!(r.filled, 12);
    // 5@100, 5@101, 2@102.
    assert_eq!(r.trades.len(), 3);
    assert_eq!(r.trades[0].price, 100);
    assert_eq!(r.trades[2].price, 102);
    assert!(!r.resting);
    assert_eq!(e.book().depth(Side::Sell), vec![(102, 3, 1)]);
}

#[test]
fn market_order_partial_when_book_thin() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 4));
    let r = e.submit(Order::market(OrderId(9), Side::Buy, 10));
    assert_eq!(r.status, OrderStatus::PartiallyFilled);
    assert_eq!(r.filled, 4);
    assert!(e.book().is_empty());
}

#[test]
fn no_cross_when_prices_do_not_meet() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 101, 5));
    // Bid below the ask -> no trade, bid rests.
    let r = e.submit(Order::limit(OrderId(2), Side::Buy, 100, 5));
    assert_eq!(r.status, OrderStatus::Resting);
    assert_eq!(r.filled, 0);
    assert_eq!(e.book().best_bid(), Some(100));
    assert_eq!(e.book().best_ask(), Some(101));
}

#[test]
fn cancel_removes_resting_order() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 5));
    e.submit(Order::limit(OrderId(2), Side::Sell, 100, 5));
    assert!(e.cancel(OrderId(1)));
    assert!(!e.cancel(OrderId(1)), "double cancel is a no-op");
    // Only #2 remains; an incoming buy hits it, not the cancelled #1.
    let r = e.submit(Order::limit(OrderId(3), Side::Buy, 100, 5));
    assert_eq!(r.trades.len(), 1);
    assert_eq!(r.trades[0].maker, OrderId(2));
    assert!(e.book().is_empty());
}

#[test]
fn trade_prints_at_maker_price_giving_taker_improvement() {
    // Resting ask at 99; aggressive buy limit 100 should trade at 99 (maker price).
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 99, 5));
    let r = e.submit(Order::limit(OrderId(2), Side::Buy, 100, 5));
    assert_eq!(r.trades[0].price, 99);
}

#[test]
fn strategies_agree_when_clearing_whole_level() {
    // If the aggressor takes >= the whole level, every strategy fills all makers
    // fully — only the intra-level split differs, and here there is none.
    for strat in [
        Box::new(PriceTimePriority) as Box<dyn trade_core::MatchingStrategy>,
        Box::new(ProRata),
        Box::new(SizePriority),
    ] {
        let mut e = book_with_level(strat);
        let r = e.submit(Order::limit(OrderId(9), Side::Buy, 100, 100));
        assert_eq!(r.filled, 100);
        assert_eq!(fill_of(&r, OrderId(1)), 10);
        assert_eq!(fill_of(&r, OrderId(2)), 30);
        assert_eq!(fill_of(&r, OrderId(3)), 60);
        assert!(e.book().is_empty());
    }
}

/// Regression: cancel and in-place reduce must keep the level's aggregate
/// quantity truthful — depth, top-of-book and FOK all read the aggregate, so
/// a stale counter means phantom liquidity (FOK accepting unfillable orders).
#[test]
fn aggregates_stay_truthful_after_cancel_and_reduce() {
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Sell, 100, 10));
    e.submit(Order::limit(OrderId(2), Side::Sell, 100, 7));
    assert_eq!(e.book().depth(Side::Sell), vec![(100, 17, 2)]);

    // Cancel #1 (tombstoned lazily): depth must drop immediately.
    assert!(e.cancel(OrderId(1)));
    assert_eq!(e.book().depth(Side::Sell), vec![(100, 7, 1)]);
    assert_eq!(e.book().top_levels(Side::Sell, 5), vec![(100, 7)]);

    // Reduce #2 from 7 -> 3 in place: aggregate follows.
    e.modify(OrderId(2), 100, 3);
    assert_eq!(e.book().depth(Side::Sell), vec![(100, 3, 1)]);

    // FOK reads the aggregate: 5 lots must be rejected, 3 must fill fully.
    let r = e.submit(Order::limit(OrderId(3), Side::Buy, 100, 5).with_tif(TimeInForce::Fok));
    assert_eq!(
        r.status,
        OrderStatus::Rejected,
        "phantom liquidity must not fool FOK"
    );
    let r = e.submit(Order::limit(OrderId(4), Side::Buy, 100, 3).with_tif(TimeInForce::Fok));
    assert_eq!(r.status, OrderStatus::Filled);
    assert!(e.book().is_empty());
}

/// exchange-core parity: budget (notional-capped) order types.
#[test]
fn budget_orders_ioc_and_fok() {
    // Asks: 5@100 then 5@110.
    let mk = || {
        let mut e = MatchingEngine::with_strategy(PriceTimePriority);
        e.submit(Order::limit(OrderId(1), Side::Sell, 100, 5));
        e.submit(Order::limit(OrderId(2), Side::Sell, 110, 5));
        e
    };

    // IOC_BUDGET buy, budget 750 for qty 10: 5@100 (500) + floor(250/110)=2@110.
    let mut e = mk();
    let mut o = Order::limit(OrderId(9), Side::Buy, 750, 10).with_tif(TimeInForce::IocBudget);
    o.price = 750; // price field = TOTAL budget
    let r = e.submit(o);
    assert_eq!(r.filled, 7, "budget caps spend: {r:?}");
    let spend: u64 = r.trades.iter().map(|t| t.price * t.quantity).sum();
    assert!(spend <= 750, "spend {spend} must fit budget");
    assert!(!r.resting, "budget orders never rest");

    // FOK_BUDGET buy: full qty 10 costs 1050 — budget 900 rejects, 1100 fills.
    let mut e = mk();
    let mut o = Order::limit(OrderId(9), Side::Buy, 900, 10).with_tif(TimeInForce::FokBudget);
    o.price = 900;
    assert_eq!(e.submit(o).status, OrderStatus::Rejected);
    let mut o = Order::limit(OrderId(9), Side::Buy, 1100, 10).with_tif(TimeInForce::FokBudget);
    o.price = 1100;
    let r = e.submit(o);
    assert_eq!(r.status, OrderStatus::Filled);
    assert_eq!(r.filled, 10);

    // SELL IOC_BUDGET: bids 5@100, 5@90; qty 10, budget 950 -> implied floor 95:
    // fills only the 100-level, remainder cancelled.
    let mut e = MatchingEngine::with_strategy(PriceTimePriority);
    e.submit(Order::limit(OrderId(1), Side::Buy, 100, 5));
    e.submit(Order::limit(OrderId(2), Side::Buy, 90, 5));
    let mut o = Order::limit(OrderId(9), Side::Sell, 950, 10).with_tif(TimeInForce::IocBudget);
    o.price = 950;
    let r = e.submit(o);
    assert_eq!(r.filled, 5);
    assert!(r.trades.iter().all(|t| t.price >= 95));
    assert!(!r.resting);
}
