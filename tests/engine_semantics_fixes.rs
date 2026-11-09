//! Regression tests for the 2026-11-08 matching-semantics audit findings.

use trade_core::exchange::{ExecReport, Processor};
use trade_core::prelude::*;
use trade_core::types::{Trigger, TriggerKind};
use trade_core::{Command, SelfTradePolicy};

const SYM: InstrumentId = InstrumentId(5);

fn processor() -> Processor {
    Processor::new(|| Box::new(PriceTimePriority), None)
}

fn run(p: &mut Processor, cmd: Command) -> Vec<ExecReport> {
    let mut out = Vec::new();
    p.process(cmd, &mut |r| out.push(r));
    out
}

fn limit(id: u64, user: u64, side: Side, price: Price, qty: Qty) -> Command {
    Command::New(Order::limit(OrderId(id), side, price, qty).on(SYM).by(user))
}

fn modify(cmd_id: u64, order: u64, price: Price, qty: Qty) -> Command {
    Command::Modify {
        instrument: SYM,
        order_id: OrderId(order),
        new_price: price,
        new_qty: qty,
        cmd_id,
    }
}

fn rejected_reason(reports: &[ExecReport]) -> Option<&'static str> {
    reports.iter().find_map(|r| match r {
        ExecReport::CommandRejected { reason, .. } | ExecReport::Rejected { reason, .. } => Some(*reason),
        _ => None,
    })
}

fn trades(reports: &[ExecReport]) -> usize {
    reports.iter().filter(|r| matches!(r, ExecReport::Trade { .. })).count()
}

#[test]
fn a_crossing_amend_is_refused_while_halted_but_a_reduce_is_allowed() {
    let mut p = processor();
    run(&mut p, limit(1, 10, Side::Sell, 101, 5));
    run(&mut p, limit(2, 20, Side::Buy, 100, 5));
    run(&mut p, Command::Halt { instrument: SYM, cmd_id: 3 });
    let out = run(&mut p, modify(4, 2, 101, 5)); // would take the ask
    assert_eq!(rejected_reason(&out), Some("halted"));
    assert_eq!(trades(&out), 0);
    let out = run(&mut p, modify(5, 2, 100, 3)); // reduce in place
    assert!(out.iter().any(|r| matches!(r, ExecReport::Modified { remaining: 3, .. })), "{out:?}");
}

#[test]
fn a_crossing_amend_by_a_suspended_user_is_refused() {
    let mut p = processor();
    run(&mut p, limit(1, 10, Side::Sell, 101, 5));
    run(&mut p, limit(2, 20, Side::Buy, 100, 5));
    run(&mut p, Command::HaltUser { instrument: SYM, user: 20, cmd_id: 3 });
    let out = run(&mut p, modify(4, 2, 101, 5));
    assert_eq!(rejected_reason(&out), Some("user-suspended"));
    assert_eq!(trades(&out), 0);
}

#[test]
fn an_amend_that_trades_updates_positions_and_reports_an_stp_stop_as_cancelled() {
    let mut p = processor().with_stp(SelfTradePolicy::CancelTaker);
    // User 20: a bid at 100 and an ask at 102. User 30 asks 5 @ 101.
    run(&mut p, limit(1, 20, Side::Buy, 100, 10));
    run(&mut p, limit(2, 20, Side::Sell, 102, 5));
    run(&mut p, limit(3, 30, Side::Sell, 101, 5));
    // Amend the bid to 102 x 10: 5 fill against user 30, then STP stops the
    // taker at its own ask.
    let out = run(&mut p, modify(4, 1, 102, 10));
    assert_eq!(trades(&out), 1);
    assert_eq!(p.position(20, SYM), 5, "amend trades must move positions");
    assert_eq!(p.position(30, SYM), -5);
    assert!(
        out.iter().any(|r| matches!(r, ExecReport::Cancelled { order_id: OrderId(1), .. })),
        "a stopped taker is gone, not Modified: {out:?}"
    );
    assert!(!out.iter().any(|r| matches!(r, ExecReport::Modified { .. })), "{out:?}");
}

#[test]
fn fill_or_kill_never_partially_fills_under_self_trade_prevention() {
    for stp in [SelfTradePolicy::CancelTaker, SelfTradePolicy::CancelMaker, SelfTradePolicy::CancelBoth] {
        let mut p = processor().with_stp(stp);
        run(&mut p, limit(1, 30, Side::Sell, 100, 5));
        run(&mut p, limit(2, 20, Side::Sell, 101, 5)); // the taker's own ask
        let fok = Order::limit(OrderId(3), Side::Buy, 101, 10).on(SYM).by(20).with_tif(TimeInForce::Fok);
        let out = run(&mut p, Command::New(fok));
        assert_eq!(trades(&out), 0, "{stp:?}: {out:?}");
        assert!(out.iter().any(|r| matches!(r, ExecReport::Rejected { .. })), "{stp:?}: {out:?}");
    }
}

#[test]
fn zero_quantities_never_report_a_fill() {
    let mut p = processor();
    let out = run(&mut p, limit(1, 10, Side::Buy, 100, 0));
    assert_eq!(rejected_reason(&out), Some("min-qty"));
    run(&mut p, limit(2, 10, Side::Buy, 100, 5));
    let out = run(&mut p, modify(3, 2, 99, 0)); // zero with a price change
    assert!(out.iter().any(|r| matches!(r, ExecReport::Cancelled { order_id: OrderId(2), .. })), "{out:?}");
    assert!(!out.iter().any(|r| matches!(r, ExecReport::Filled { .. })), "{out:?}");
}

#[test]
fn a_stop_fires_on_any_print_of_a_sweep_not_only_the_last() {
    let mut p = processor();
    // Sell stop at 101 (fires when a trade prints at or below 101).
    let stop = Command::NewConditional(
        Order::market(OrderId(1), Side::Sell, 1).on(SYM).by(1),
        Trigger { price: 101, kind: TriggerKind::StopLoss },
    );
    run(&mut p, stop);
    run(&mut p, limit(2, 2, Side::Sell, 100, 1));
    run(&mut p, limit(3, 3, Side::Sell, 101, 1));
    run(&mut p, limit(4, 4, Side::Sell, 102, 1));
    run(&mut p, limit(5, 5, Side::Buy, 90, 5)); // liquidity for the fired stop
    // One buy sweeps 100, 101, 102; the last print (102) is above the stop.
    let out = run(&mut p, limit(6, 6, Side::Buy, 102, 3));
    assert!(
        out.iter().any(|r| matches!(r, ExecReport::Triggered { order_id: OrderId(1), .. })),
        "{out:?}"
    );
}

#[test]
fn a_huge_sell_budget_does_not_wrap_its_price_floor() {
    let mut p = processor();
    run(&mut p, limit(1, 1, Side::Buy, 100, 5));
    let order = Order::limit(OrderId(2), Side::Sell, u64::MAX, 2)
        .on(SYM)
        .by(2)
        .with_tif(TimeInForce::IocBudget);
    let out = run(&mut p, Command::New(order));
    assert_eq!(trades(&out), 0, "a near-u64::MAX proceeds floor must not sell at 100: {out:?}");
}

fn force_close(close_id: u64, user: u64, side: Side, qty: Qty) -> Command {
    Command::ForceClose {
        instrument: SYM,
        user,
        close_order_id: OrderId(close_id),
        close_side: side,
        close_qty: qty,
    }
}

#[test]
fn a_liquidation_flattens_a_suspended_user_beyond_the_order_caps() {
    let mut p = Processor::new(|| Box::new(PriceTimePriority), None).with_limits(Some(
        trade_core::RiskLimits {
            max_order_qty: 5,
            max_notional: 0,
            max_user_orders: 1,
        },
    ));
    // User 20 builds a 10-lot long in two fills; makers 10/11 rest asks.
    run(&mut p, limit(1, 10, Side::Sell, 100, 5));
    run(&mut p, limit(2, 20, Side::Buy, 100, 5));
    run(&mut p, limit(3, 11, Side::Sell, 100, 5));
    run(&mut p, limit(4, 20, Side::Buy, 100, 5));
    assert_eq!(p.position(20, SYM), 10);
    run(&mut p, Command::HaltUser { instrument: SYM, user: 20, cmd_id: 5 });
    // Bids for the close: 10 lots, more than one order may carry.
    run(&mut p, limit(6, 30, Side::Buy, 99, 5));
    run(&mut p, limit(7, 31, Side::Buy, 99, 5));

    // Suspended, close above max-qty: still must go through.
    let out = run(&mut p, force_close(8, 20, Side::Sell, 10));
    assert_eq!(rejected_reason(&out), None, "{out:?}");
    assert_eq!(trades(&out), 2);
    assert_eq!(p.position(20, SYM), 0);

    // An ordinary order from the suspended user is still refused.
    let out = run(&mut p, limit(9, 20, Side::Sell, 99, 1));
    assert_eq!(rejected_reason(&out), Some("user-suspended"));
}

#[test]
fn malformed_orders_are_rejected_not_panicked_on() {
    let mut p = processor();
    // Order id 0 twice: used to bypass the duplicate checks and then trip the
    // book's uniqueness assertion.
    assert_eq!(rejected_reason(&run(&mut p, limit(0, 10, Side::Buy, 90, 1))), Some("invalid-order-id"));
    assert_eq!(rejected_reason(&run(&mut p, limit(0, 10, Side::Buy, 91, 1))), Some("invalid-order-id"));
    // A quantity whose level total would overflow.
    assert_eq!(rejected_reason(&run(&mut p, limit(1, 10, Side::Sell, 100, u64::MAX))), Some("max-qty"));
    run(&mut p, limit(2, 10, Side::Sell, 100, trade_core::exchange::MAX_ORDER_QTY));
    run(&mut p, limit(3, 11, Side::Sell, 100, trade_core::exchange::MAX_ORDER_QTY));
    let out = run(&mut p, modify(4, 3, 100, u64::MAX));
    assert_eq!(rejected_reason(&out), Some("max-qty"));
    // A market order cannot carry a budget.
    let mut budget = Order::market(OrderId(5), Side::Buy, 5).on(SYM).by(12);
    budget.tif = TimeInForce::IocBudget;
    assert_eq!(rejected_reason(&run(&mut p, Command::New(budget))), Some("invalid-tif"));
}
