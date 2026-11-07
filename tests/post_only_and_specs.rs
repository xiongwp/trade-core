//! Post-only orders and per-instrument trading rules (tick / lot / qty /
//! price range / min notional), end to end through the deterministic
//! `Processor`, the wire format and snapshots.

use trade_core::exchange::{ExecReport, Processor};
use trade_core::prelude::*;
use trade_core::snapshot::{self, SnapshotData};
use trade_core::wire;
use trade_core::{Command, InstrumentId, InstrumentSpec, InstrumentSpecs};

const SYM: InstrumentId = InstrumentId(1);

fn processor() -> Processor {
    Processor::new(|| Box::new(PriceTimePriority), None)
}

fn run(p: &mut Processor, cmd: Command) -> Vec<ExecReport> {
    let mut out = Vec::new();
    p.process(cmd, &mut |r| out.push(r));
    out
}

fn new(id: u64, side: Side, price: Price, qty: Qty) -> Command {
    Command::New(Order::limit(OrderId(id), side, price, qty).on(SYM).by(id))
}

fn post_only(id: u64, side: Side, price: Price, qty: Qty) -> Command {
    Command::New(
        Order::limit(OrderId(id), side, price, qty)
            .with_tif(TimeInForce::PostOnly)
            .on(SYM)
            .by(id),
    )
}

fn rejected_reason(reports: &[ExecReport]) -> Option<&'static str> {
    reports.iter().find_map(|r| match r {
        ExecReport::Rejected { reason, .. } => Some(*reason),
        _ => None,
    })
}

fn has_trade(reports: &[ExecReport]) -> bool {
    reports
        .iter()
        .any(|r| matches!(r, ExecReport::Trade { .. }))
}

#[test]
fn post_only_rests_when_passive_and_is_rejected_when_it_would_take() {
    let mut p = processor();
    run(&mut p, new(1, Side::Sell, 100, 10));

    // A bid below the best ask adds liquidity: it rests.
    let reports = run(&mut p, post_only(2, Side::Buy, 99, 5));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecReport::Resting {
            order_id: OrderId(2),
            ..
        }
    )));
    assert_eq!(p.engine(SYM).unwrap().book().best_bid(), Some(99));

    // A bid at the ask would take: rejected whole, nothing trades, the ask is
    // untouched.
    let reports = run(&mut p, post_only(3, Side::Buy, 100, 5));
    assert_eq!(rejected_reason(&reports), Some("post-only"));
    assert!(!has_trade(&reports));
    assert_eq!(p.engine(SYM).unwrap().book().best_ask(), Some(100));
    assert!(p.engine(SYM).unwrap().book().get(OrderId(3)).is_none());

    // Same on the sell side.
    let reports = run(&mut p, post_only(4, Side::Sell, 99, 1));
    assert_eq!(rejected_reason(&reports), Some("post-only"));
    let reports = run(&mut p, post_only(5, Side::Sell, 101, 1));
    assert_eq!(rejected_reason(&reports), None);
}

#[test]
fn post_only_ignores_a_level_holding_only_cancelled_orders() {
    // Cancels are O(1) tombstones reclaimed lazily by the matching path; a
    // level that is all tombstones must not make a post-only look marketable.
    let mut p = processor();
    run(&mut p, new(1, Side::Sell, 100, 10));
    run(&mut p, new(2, Side::Sell, 105, 10));
    let reports = run(
        &mut p,
        Command::Cancel {
            instrument: SYM,
            order_id: OrderId(1),
            cmd_id: 0,
        },
    );
    assert!(matches!(reports[0], ExecReport::Cancelled { .. }));

    let reports = run(&mut p, post_only(3, Side::Buy, 100, 5));
    assert_eq!(rejected_reason(&reports), None, "{reports:?}");
    assert!(p.engine(SYM).unwrap().book().get(OrderId(3)).is_some());

    // ...but a live level inside the limit still rejects.
    let reports = run(&mut p, post_only(4, Side::Buy, 105, 5));
    assert_eq!(rejected_reason(&reports), Some("post-only"));
}

#[test]
fn market_post_only_is_rejected() {
    let mut p = processor();
    run(&mut p, new(1, Side::Sell, 100, 10));
    let order = Order::market(OrderId(2), Side::Buy, 5)
        .with_tif(TimeInForce::PostOnly)
        .on(SYM)
        .by(2);
    let reports = run(&mut p, Command::New(order));
    assert_eq!(rejected_reason(&reports), Some("post-only"));
    assert!(!has_trade(&reports));
}

#[test]
fn post_only_amend_that_would_take_cancels_instead_of_trading() {
    let mut p = processor();
    run(&mut p, new(1, Side::Sell, 100, 10));
    run(&mut p, post_only(2, Side::Buy, 98, 5));

    // Repricing passively keeps it post-only and resting.
    let reports = run(
        &mut p,
        Command::Modify {
            instrument: SYM,
            order_id: OrderId(2),
            new_price: 99,
            new_qty: 5,
            cmd_id: 0,
        },
    );
    assert!(
        matches!(reports[..], [ExecReport::Modified { .. }]),
        "{reports:?}"
    );
    assert_eq!(
        p.engine(SYM).unwrap().book().get(OrderId(2)).unwrap().tif,
        TimeInForce::PostOnly
    );

    // Repricing through the ask must not turn it into a taker.
    let reports = run(
        &mut p,
        Command::Modify {
            instrument: SYM,
            order_id: OrderId(2),
            new_price: 100,
            new_qty: 5,
            cmd_id: 0,
        },
    );
    assert!(!has_trade(&reports), "{reports:?}");
    assert!(matches!(
        reports[..],
        [ExecReport::Cancelled {
            order_id: OrderId(2),
            ..
        }]
    ));
    assert_eq!(p.engine(SYM).unwrap().book().best_ask(), Some(100));
}

fn spec_processor() -> Processor {
    let specs = InstrumentSpecs::parse(
        "*:tick=5,lot=10,min_qty=10,max_qty=1000,min_price=50,max_price=500,min_notional=2000",
    )
    .unwrap();
    processor().with_specs(specs)
}

#[test]
fn instrument_rules_reject_each_violation_with_its_reason() {
    let mut p = spec_processor();
    let cases = [
        (new(1, Side::Buy, 101, 20), Some("tick-size")),
        (new(2, Side::Buy, 100, 25), Some("lot-size")),
        (new(3, Side::Buy, 100, 0), Some("min-qty")),
        (new(4, Side::Buy, 100, 1010), Some("max-qty")),
        (new(5, Side::Buy, 45, 100), Some("price-range")),
        (new(6, Side::Buy, 505, 10), Some("price-range")),
        (new(7, Side::Buy, 100, 10), Some("min-notional")), // 1000 < 2000
        (new(8, Side::Buy, 100, 20), None),                 // 2000: ok
    ];
    for (cmd, want) in cases {
        let label = format!("{cmd:?}");
        let reports = run(&mut p, cmd);
        assert_eq!(rejected_reason(&reports), want, "{label} -> {reports:?}");
    }
    // A rejected order never reached the book.
    let book = p.engine(SYM).unwrap().book();
    assert_eq!(book.len(), 1);
    assert!(book.get(OrderId(8)).is_some());
}

#[test]
fn instrument_rules_skip_price_checks_for_market_and_use_budget_as_notional() {
    let mut p = spec_processor();
    run(&mut p, new(1, Side::Sell, 100, 20));

    // Market orders have no price: only quantity rules apply.
    let market = |id, qty| Command::New(Order::market(OrderId(id), Side::Buy, qty).on(SYM).by(id));
    assert_eq!(
        rejected_reason(&run(&mut p, market(2, 15))),
        Some("lot-size")
    );
    let reports = run(&mut p, market(3, 10));
    assert_eq!(rejected_reason(&reports), None);
    assert!(has_trade(&reports));

    // Budget orders: `price` is the total budget, checked against min notional
    // and exempt from tick / price range.
    let budget = |id, total| {
        Command::New(
            Order::limit(OrderId(id), Side::Buy, total, 10)
                .with_tif(TimeInForce::IocBudget)
                .on(SYM)
                .by(id),
        )
    };
    assert_eq!(
        rejected_reason(&run(&mut p, budget(4, 1999))),
        Some("min-notional")
    );
    assert_eq!(rejected_reason(&run(&mut p, budget(5, 2001))), None);
}

#[test]
fn per_instrument_spec_overrides_the_default() {
    let specs = InstrumentSpecs::parse("*:tick=5; 2:tick=1").unwrap();
    let mut p = processor().with_specs(specs);
    let order = |id, inst| {
        Command::New(
            Order::limit(OrderId(id), Side::Buy, 101, 1)
                .on(InstrumentId(inst))
                .by(id),
        )
    };
    assert_eq!(
        rejected_reason(&run(&mut p, order(1, 1))),
        Some("tick-size")
    );
    assert_eq!(rejected_reason(&run(&mut p, order(2, 2))), None);
}

#[test]
fn amend_breaking_a_rule_is_refused_and_leaves_the_order_untouched() {
    let mut p = spec_processor();
    run(&mut p, new(1, Side::Buy, 100, 20));
    let amend = |price, qty, cmd_id| Command::Modify {
        instrument: SYM,
        order_id: OrderId(1),
        new_price: price,
        new_qty: qty,
        cmd_id,
    };

    for (cmd, reason) in [
        (amend(101, 20, 900), "tick-size"),
        (amend(100, 25, 901), "lot-size"),
        (amend(600, 20, 902), "price-range"),
    ] {
        let reports = run(&mut p, cmd);
        // Keyed by the command id: the order itself was not rejected.
        assert!(
            matches!(reports[..], [ExecReport::Rejected { order_id, reason: r, .. }] if r == reason && order_id.0 >= 900),
            "{reports:?}"
        );
        let resting = *p.engine(SYM).unwrap().book().get(OrderId(1)).unwrap();
        assert_eq!((resting.price, resting.remaining), (100, 20));
    }

    // A remainder below min_qty / min_notional is legal (it may be what is
    // left after partial fills), and qty 0 is a cancel.
    let reports = run(&mut p, amend(100, 10, 903));
    assert!(matches!(
        reports[..],
        [ExecReport::Modified { remaining: 10, .. }]
    ));
    let reports = run(&mut p, amend(100, 0, 904));
    assert!(matches!(reports[..], [ExecReport::Cancelled { .. }]));
}

#[test]
fn spec_parse_rejects_malformed_input() {
    assert!(InstrumentSpecs::parse("").unwrap().is_empty());
    assert!(InstrumentSpecs::parse("tick=5").is_err());
    assert!(InstrumentSpecs::parse("*:tick").is_err());
    assert!(InstrumentSpecs::parse("*:tick=x").is_err());
    assert!(InstrumentSpecs::parse("*:bogus=1").is_err());
    assert!(InstrumentSpecs::parse("abc:tick=1").is_err());
    let specs = InstrumentSpecs::parse("7:tick=5,lot=10,min_notional=1000").unwrap();
    assert_eq!(
        specs.get(InstrumentId(7)),
        Some(&InstrumentSpec {
            tick_size: 5,
            lot_size: 10,
            min_notional: 1000,
            ..InstrumentSpec::default()
        })
    );
    assert_eq!(specs.get(InstrumentId(8)), None);
}

#[test]
fn post_only_and_reject_reasons_survive_the_wire() {
    let order = Order::limit(OrderId(9), Side::Sell, 123, 4)
        .with_tif(TimeInForce::PostOnly)
        .on(SYM)
        .by(77);
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_new(&order, &mut frame);
    match wire::WireView::parse(&frame).unwrap().to_command().unwrap() {
        Command::New(decoded) => assert_eq!(decoded.tif, TimeInForce::PostOnly),
        other => panic!("expected New, got {other:?}"),
    }

    for &reason in wire::REJECT_REASONS {
        let mut report = [0u8; wire::REPORT_LEN];
        wire::encode_report(
            &ExecReport::Rejected {
                instrument: SYM,
                order_id: OrderId(9),
                reason,
            },
            &mut report,
        );
        let decoded = wire::decode_report(&report).unwrap();
        assert_eq!(wire::reject_reason_name(decoded.aux_id), reason);
    }
    assert_eq!(wire::reject_reason_code("not-a-reason"), 0);
    assert_eq!(wire::reject_reason_name(10_000), "unknown");
}

#[test]
fn resting_post_only_survives_snapshot_and_replay_is_deterministic() {
    let flow = || {
        vec![
            new(1, Side::Sell, 100, 10),
            post_only(2, Side::Buy, 99, 5),
            post_only(3, Side::Buy, 100, 5), // rejected
            post_only(4, Side::Sell, 101, 5),
            new(5, Side::Buy, 101, 12), // takes 100 and part of the post-only 101
        ]
    };
    let mut a = processor();
    let mut b = processor();
    let (mut ra, mut rb) = (Vec::new(), Vec::new());
    for cmd in flow() {
        ra.extend(run(&mut a, cmd));
    }
    for cmd in flow() {
        rb.extend(run(&mut b, cmd));
    }
    assert_eq!(ra, rb);
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());

    let dir = std::env::temp_dir().join(format!("tc-post-only-snap-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    let engines = a.export_state();
    snapshot::write(
        &path,
        SnapshotData {
            journal_seq: 0,
            raft_applied_index: 0,
            max_cmd_id: 0,
            max_admin_id: 0,
            halted: &[],
            suspended: &[],
            positions: &[],
            engines: &engines,
        },
    )
    .unwrap();
    let mut restored = processor();
    restored.restore_state(&snapshot::load(&path).unwrap());
    assert_eq!(restored.state_fingerprint(), a.state_fingerprint());
    let book = restored.engine(SYM).unwrap().book();
    assert_eq!(book.get(OrderId(2)).unwrap().tif, TimeInForce::PostOnly);
    assert_eq!(book.get(OrderId(4)).unwrap().tif, TimeInForce::PostOnly);
    std::fs::remove_dir_all(&dir).ok();
}
