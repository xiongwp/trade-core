//! Conditional (stop-loss / take-profit) orders inside the matching engine:
//! trigger semantics, cascades, lifecycle, persistence and replay determinism.

use std::time::Duration;

use trade_core::exchange::{replay_journal, ExecReport, Processor};
use trade_core::journal::JournalWriter;
use trade_core::prelude::*;
use trade_core::snapshot::{self, SnapshotData};
use trade_core::types::{Trigger, TriggerKind};
use trade_core::wire;
use trade_core::{Command, InstrumentSpecs};

const SYM: InstrumentId = InstrumentId(5);

fn processor() -> Processor {
    Processor::new(|| Box::new(PriceTimePriority), None)
}

fn run(p: &mut Processor, cmd: Command) -> Vec<ExecReport> {
    let mut out = Vec::new();
    p.process(cmd, &mut |r| out.push(r));
    out
}

fn limit(id: u64, side: Side, price: Price, qty: Qty) -> Command {
    Command::New(Order::limit(OrderId(id), side, price, qty).on(SYM).by(id))
}

fn stop_market(id: u64, side: Side, qty: Qty, trigger: Price, kind: TriggerKind) -> Command {
    Command::NewConditional(
        Order::market(OrderId(id), side, qty).on(SYM).by(id),
        Trigger {
            price: trigger,
            kind,
        },
    )
}

fn stop_limit(
    id: u64,
    side: Side,
    price: Price,
    qty: Qty,
    trigger: Price,
    kind: TriggerKind,
) -> Command {
    Command::NewConditional(
        Order::limit(OrderId(id), side, price, qty).on(SYM).by(id),
        Trigger {
            price: trigger,
            kind,
        },
    )
}

fn triggered(reports: &[ExecReport]) -> Vec<u64> {
    reports
        .iter()
        .filter_map(|r| match r {
            ExecReport::Triggered { order_id, .. } => Some(order_id.0),
            _ => None,
        })
        .collect()
}

fn reason(reports: &[ExecReport]) -> Option<&'static str> {
    reports.iter().find_map(|r| match r {
        ExecReport::Rejected { reason, .. } | ExecReport::CommandRejected { reason, .. } => {
            Some(*reason)
        }
        _ => None,
    })
}

#[test]
fn sell_stop_loss_waits_off_book_then_fires_on_a_print_at_its_trigger() {
    let mut p = processor();
    run(&mut p, limit(1, Side::Buy, 100, 10));
    run(&mut p, limit(2, Side::Buy, 95, 10));

    let reports = run(
        &mut p,
        stop_market(3, Side::Sell, 4, 98, TriggerKind::StopLoss),
    );
    assert!(
        matches!(
            reports[..],
            [ExecReport::StopAccepted {
                order_id: OrderId(3),
                ..
            }]
        ),
        "{reports:?}"
    );
    // Not on the book: the best ask is still empty.
    assert_eq!(p.engine(SYM).unwrap().book().best_ask(), None);

    // A print at 100 (above the trigger) leaves it armed.
    let reports = run(&mut p, limit(4, Side::Sell, 100, 10));
    assert!(triggered(&reports).is_empty(), "{reports:?}");

    // A print at 95 (at/below 98) fires it: Triggered, then the ordinary
    // market-order lifecycle, all inside the same command.
    let reports = run(&mut p, limit(5, Side::Sell, 95, 2));
    assert_eq!(triggered(&reports), vec![3]);
    let at = reports
        .iter()
        .position(|r| matches!(r, ExecReport::Triggered { .. }))
        .unwrap();
    assert!(matches!(
        reports[at + 1],
        ExecReport::Accepted {
            order_id: OrderId(3),
            ..
        }
    ));
    assert!(reports[at + 1..].iter().any(|r| matches!(
        r,
        ExecReport::Trade {
            taker: OrderId(3),
            price: 95,
            qty: 4,
            ..
        }
    )));
}

#[test]
fn trigger_directions_follow_side_and_kind() {
    // (side, kind, fires on a print at 110 after placement at a market of 100?)
    for (side, kind, fires_high) in [
        (Side::Buy, TriggerKind::StopLoss, true),
        (Side::Sell, TriggerKind::TakeProfit, true),
        (Side::Sell, TriggerKind::StopLoss, false),
        (Side::Buy, TriggerKind::TakeProfit, false),
    ] {
        let mut p = processor();
        run(&mut p, limit(1, Side::Sell, 110, 50));
        run(&mut p, limit(2, Side::Buy, 90, 50));
        run(&mut p, stop_limit(3, side, 100, 1, 105, kind));
        assert_eq!(Trigger { price: 105, kind }.fires_on_rise(side), fires_high);

        let up = run(&mut p, limit(4, Side::Buy, 110, 1)); // prints 110
        let down = run(&mut p, limit(5, Side::Sell, 90, 1)); // prints 90
        let fired = if fires_high { up } else { down };
        assert_eq!(triggered(&fired), vec![3], "{side:?} {kind:?}");
    }
}

#[test]
fn a_fired_stop_can_cascade_into_the_next_within_one_command() {
    let mut p = processor();
    for (id, px) in [(1, 100), (2, 99), (3, 98), (4, 97), (5, 96), (6, 95)] {
        run(&mut p, limit(id, Side::Buy, px, 1));
    }
    // 10 (trigger 99) sells 2 -> prints 98, 97 -> arms 11 (98), which sells 1
    // -> prints 96 -> arms 12 (96), which sells 1 -> prints 95. 13 (94) never
    // sees a print at or below 94.
    run(
        &mut p,
        stop_market(10, Side::Sell, 2, 99, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_market(11, Side::Sell, 1, 98, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_market(12, Side::Sell, 1, 96, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_market(13, Side::Sell, 1, 94, TriggerKind::StopLoss),
    );

    let reports = run(&mut p, limit(20, Side::Sell, 100, 1)); // prints 100 > 99
    assert!(triggered(&reports).is_empty());
    let reports = run(&mut p, limit(21, Side::Sell, 99, 1)); // prints 99
    assert_eq!(triggered(&reports), vec![10, 11, 12], "{reports:?}");
    for (taker, price) in [(10, 98), (10, 97), (11, 96), (12, 95)] {
        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecReport::Trade { taker: OrderId(t), price: px, .. } if *t == taker && *px == price
            )),
            "taker {taker} @ {price}: {reports:?}"
        );
    }
    // 13 is still armed and cancellable.
    let reports = run(
        &mut p,
        Command::Cancel {
            instrument: SYM,
            order_id: OrderId(13),
            cmd_id: 0,
        },
    );
    assert!(matches!(
        reports[..],
        [ExecReport::Cancelled {
            order_id: OrderId(13),
            ..
        }]
    ));
}

#[test]
fn several_armed_stops_fire_in_trigger_then_arrival_order() {
    let mut p = processor();
    run(&mut p, limit(1, Side::Buy, 90, 100));
    // Falling triggers fire highest first; equal triggers by arrival.
    run(
        &mut p,
        stop_limit(10, Side::Sell, 50, 1, 95, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_limit(11, Side::Sell, 50, 1, 97, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_limit(12, Side::Sell, 50, 1, 97, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_limit(13, Side::Sell, 50, 1, 96, TriggerKind::StopLoss),
    );
    let reports = run(&mut p, limit(2, Side::Sell, 90, 1));
    assert_eq!(triggered(&reports), vec![11, 12, 13, 10]);
}

#[test]
fn untriggered_stops_cancel_refuse_amends_and_go_with_force_close() {
    let mut p = processor();
    run(
        &mut p,
        stop_market(1, Side::Sell, 1, 90, TriggerKind::StopLoss),
    );
    let reports = run(
        &mut p,
        Command::Modify {
            instrument: SYM,
            order_id: OrderId(1),
            new_price: 0,
            new_qty: 2,
            cmd_id: 50,
        },
    );
    assert!(matches!(
        reports[..],
        [ExecReport::CommandRejected {
            command_id: 50,
            reason: "stop-not-amendable",
            ..
        }]
    ));
    let cancel = |id, cmd_id| Command::Cancel {
        instrument: SYM,
        order_id: OrderId(id),
        cmd_id,
    };
    assert!(matches!(
        run(&mut p, cancel(1, 51))[..],
        [ExecReport::Cancelled { .. }]
    ));
    assert!(matches!(
        run(&mut p, cancel(1, 52))[..],
        [ExecReport::NotFound { .. }]
    ));

    // ForceClose pulls the user's stops along with their resting orders.
    run(
        &mut p,
        stop_market(7, Side::Sell, 1, 90, TriggerKind::StopLoss),
    );
    run(
        &mut p,
        stop_market(8, Side::Buy, 1, 120, TriggerKind::StopLoss),
    );
    let reports = run(
        &mut p,
        Command::ForceClose {
            instrument: SYM,
            user: 7,
            close_order_id: OrderId(60),
            close_side: Side::Sell,
            close_qty: 0,
        },
    );
    assert!(matches!(
        reports[..],
        [ExecReport::Cancelled {
            order_id: OrderId(7),
            ..
        }]
    ));
    // User 8's stop survives and still fires.
    run(&mut p, limit(9, Side::Sell, 120, 1));
    let reports = run(&mut p, limit(10, Side::Buy, 120, 1));
    assert_eq!(triggered(&reports), vec![8]);
}

#[test]
fn placement_is_validated_and_deduplicated() {
    let mut p = processor().with_specs(InstrumentSpecs::parse("*:tick=5").unwrap());
    let budget = Command::NewConditional(
        Order::limit(OrderId(1), Side::Buy, 1_000, 1)
            .with_tif(TimeInForce::IocBudget)
            .on(SYM),
        Trigger {
            price: 100,
            kind: TriggerKind::StopLoss,
        },
    );
    assert_eq!(reason(&run(&mut p, budget)), Some("invalid-stop"));
    assert_eq!(
        reason(&run(
            &mut p,
            stop_market(2, Side::Buy, 1, 0, TriggerKind::StopLoss)
        )),
        Some("invalid-stop")
    );
    // Trading rules apply to the trigger price too.
    assert_eq!(
        reason(&run(
            &mut p,
            stop_market(3, Side::Buy, 1, 101, TriggerKind::StopLoss)
        )),
        Some("tick-size")
    );
    assert_eq!(
        reason(&run(
            &mut p,
            stop_market(4, Side::Buy, 1, 105, TriggerKind::StopLoss)
        )),
        None
    );
    assert_eq!(
        reason(&run(
            &mut p,
            stop_market(4, Side::Buy, 1, 105, TriggerKind::StopLoss)
        )),
        Some("duplicate")
    );
    run(
        &mut p,
        Command::Halt {
            instrument: SYM,
            cmd_id: 90,
        },
    );
    assert_eq!(
        reason(&run(
            &mut p,
            stop_market(5, Side::Buy, 1, 105, TriggerKind::StopLoss)
        )),
        Some("halted")
    );
}

#[test]
fn conditional_orders_survive_the_wire() {
    let cmd = stop_limit(9, Side::Sell, 120, 3, 115, TriggerKind::TakeProfit);
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_command(&cmd, &mut frame);
    let decoded = wire::WireView::parse(&frame).unwrap().to_command().unwrap();
    assert_eq!(format!("{decoded:?}"), format!("{cmd:?}"));

    // An ordinary order's frame is unchanged: zero tail, no flags.
    let mut plain = [0u8; wire::MSG_LEN];
    wire::encode_command(&limit(9, Side::Sell, 120, 3), &mut plain);
    assert!(plain[40..].iter().all(|&b| b == 0));
    assert_eq!(plain[2] & 0xF0, 0);

    for (report, code) in [
        (
            ExecReport::StopAccepted {
                instrument: SYM,
                order_id: OrderId(9),
                user: 0,
            },
            wire::RT_STOP_ACCEPTED,
        ),
        (
            ExecReport::Triggered {
                instrument: SYM,
                order_id: OrderId(9),
                user: 0,
            },
            wire::RT_TRIGGERED,
        ),
    ] {
        let mut out = [0u8; wire::REPORT_LEN];
        wire::encode_report(&report, &mut out);
        let decoded = wire::decode_report(&out).unwrap();
        assert_eq!((decoded.type_code, decoded.order_id), (code, OrderId(9)));
    }
}

fn flow() -> Vec<Command> {
    vec![
        limit(1, Side::Buy, 100, 5),
        limit(2, Side::Buy, 99, 5),
        stop_market(3, Side::Sell, 3, 100, TriggerKind::StopLoss),
        stop_limit(4, Side::Buy, 130, 2, 120, TriggerKind::StopLoss),
        limit(5, Side::Sell, 100, 1), // fires 3, which sells into 100/99
        limit(6, Side::Sell, 120, 4),
        limit(7, Side::Buy, 120, 1), // fires 4, which lifts 120
    ]
}

fn write_snapshot(p: &Processor, path: &std::path::Path) {
    let engines = p.export_state();
    snapshot::write(
        path,
        SnapshotData {
            journal_seq: 0,
            raft_applied_index: 0,
            max_cmd_id: 0,
            max_admin_id: 0,
            halted: &[],
            suspended: &[],
            positions: &[],
            engines: &engines,
            recent_command_ids: &[],
            sealed: &[],
            recent_command_instruments: &[],
            migration_epochs: &[],
        },
    )
    .unwrap();
}

#[test]
fn untriggered_stops_survive_snapshots_and_restore_continues_identically() {
    let dir = std::env::temp_dir().join(format!("tc-stops-snap-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    let split = 4; // both stops placed, none fired yet

    let mut live = processor();
    for cmd in flow().into_iter().take(split) {
        run(&mut live, cmd);
    }
    write_snapshot(&live, &path);
    let mut restored = processor();
    restored.restore_state(&snapshot::load(&path).unwrap());
    assert_eq!(restored.state_fingerprint(), live.state_fingerprint());

    let (mut a, mut b) = (Vec::new(), Vec::new());
    for cmd in flow().into_iter().skip(split) {
        a.extend(run(&mut live, cmd));
    }
    for cmd in flow().into_iter().skip(split) {
        b.extend(run(&mut restored, cmd));
    }
    assert_eq!(triggered(&a), vec![3, 4], "{a:?}");
    assert_eq!(a, b);
    assert_eq!(live.state_fingerprint(), restored.state_fingerprint());

    // Restored ids are deduplicated like resting orders.
    let mut again = processor();
    again.restore_state(&snapshot::load(&path).unwrap());
    assert_eq!(
        reason(&run(
            &mut again,
            stop_market(3, Side::Sell, 3, 100, TriggerKind::StopLoss)
        )),
        Some("duplicate")
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_cancelled_stop_leaves_no_trace_in_the_fingerprint() {
    let mut p = processor();
    run(&mut p, limit(1, Side::Buy, 100, 5));
    let before = p.state_fingerprint();

    // Placing then cancelling a stop leaves no trace in the fingerprint.
    run(
        &mut p,
        stop_market(2, Side::Sell, 1, 90, TriggerKind::StopLoss),
    );
    assert_ne!(p.state_fingerprint(), before);
    run(
        &mut p,
        Command::Cancel {
            instrument: SYM,
            order_id: OrderId(2),
            cmd_id: 3,
        },
    );
    assert_eq!(p.state_fingerprint(), before);
}

#[test]
fn journal_replay_of_conditional_orders_is_deterministic() {
    let dir = std::env::temp_dir().join(format!("tc-stops-replay-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("journal.bin");
    let mut writer = JournalWriter::open(&path, Duration::from_millis(1)).unwrap();
    for (i, cmd) in flow().iter().enumerate() {
        let mut frame = [0u8; wire::MSG_LEN];
        wire::encode_command(cmd, &mut frame);
        writer.append(1_000 + i as u64, &frame).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let mut live = processor();
    let mut live_reports = Vec::new();
    for cmd in flow() {
        live_reports.extend(run(&mut live, cmd));
    }
    let strategy = || Box::new(PriceTimePriority) as Box<dyn MatchingStrategy>;
    let replay = replay_journal(&path, strategy, None, None).unwrap();
    assert_eq!(replay.reports, live_reports);
    assert_eq!(
        replay.processor.state_fingerprint(),
        live.state_fingerprint()
    );
    std::fs::remove_dir_all(&dir).ok();
}
