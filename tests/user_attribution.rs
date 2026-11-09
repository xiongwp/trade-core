//! Execution results name the user who owns each order, end to end: engine
//! reports -> durable outbox -> Kafka execution events. This is what lets a
//! result be pushed straight to its user with no database lookup.

use std::time::Duration;

use trade_core::exchange::{fingerprint_reports, proof_digest, ExecReport, Processor};
use trade_core::execution_outbox::{
    ExecutionOutboxReader, ExecutionOutboxWriter,
};
use trade_core::prelude::*;
use trade_core::types::{Trigger, TriggerKind};
use trade_core::wire;
use trade_core::{Command, SelfTradePolicy};

const SYM: InstrumentId = InstrumentId(4);

fn run(p: &mut Processor, cmd: Command) -> Vec<ExecReport> {
    let mut out = Vec::new();
    p.process(cmd, &mut |r| out.push(r));
    out
}

fn limit(id: u64, side: Side, price: Price, qty: Qty, user: u64) -> Command {
    Command::New(Order::limit(OrderId(id), side, price, qty).on(SYM).by(user))
}

#[test]
fn every_order_report_names_its_owner() {
    let mut p = Processor::new(|| Box::new(PriceTimePriority), None);
    let resting = run(&mut p, limit(1, Side::Sell, 100, 5, 501));
    assert!(resting.contains(&ExecReport::Accepted {
        instrument: SYM,
        order_id: OrderId(1),
        user: 501
    }));
    assert!(resting.contains(&ExecReport::Resting {
        instrument: SYM,
        order_id: OrderId(1),
        user: 501
    }));

    // A trade names both sides; each side's own reports name its owner.
    let taker = run(&mut p, limit(2, Side::Buy, 100, 3, 502));
    let trade = taker
        .iter()
        .find(|r| matches!(r, ExecReport::Trade { .. }))
        .unwrap();
    assert_eq!(trade.users(), [502, 501]);
    assert!(taker.contains(&ExecReport::Filled {
        instrument: SYM,
        order_id: OrderId(2),
        user: 502
    }));

    // Cancelling a resting order reports its owner, not the canceller's guess.
    let cancel = run(
        &mut p,
        Command::Cancel {
            instrument: SYM,
            order_id: OrderId(1),
            cmd_id: 0,
        },
    );
    assert_eq!(cancel[0].users(), [501, 0]);

    // Rejections and conditional orders too.
    let halted = {
        let mut q = Processor::new(|| Box::new(PriceTimePriority), None);
        run(
            &mut q,
            Command::Halt {
                instrument: SYM,
                cmd_id: 0,
            },
        );
        run(&mut q, limit(9, Side::Buy, 100, 1, 509))
    };
    assert_eq!(halted[0].users(), [509, 0]);

    let stop = run(
        &mut p,
        Command::NewConditional(
            Order::market(OrderId(5), Side::Sell, 1).on(SYM).by(505),
            Trigger {
                price: 90,
                kind: TriggerKind::StopLoss,
            },
        ),
    );
    assert_eq!(stop[0].users(), [505, 0]);
    run(&mut p, limit(6, Side::Buy, 90, 5, 506));
    let fired = run(&mut p, limit(7, Side::Sell, 90, 1, 507));
    let triggered = fired
        .iter()
        .find(|r| matches!(r, ExecReport::Triggered { .. }))
        .unwrap();
    assert_eq!(triggered.users(), [505, 0]);

    // Unknown orders have no owner to report.
    let missing = run(
        &mut p,
        Command::Cancel {
            instrument: SYM,
            order_id: OrderId(999),
            cmd_id: 0,
        },
    );
    assert_eq!(missing[0].users(), [0, 0]);
}

#[test]
fn self_trade_prevention_cancels_name_the_maker_owner() {
    let mut p =
        Processor::new(|| Box::new(PriceTimePriority), None).with_stp(SelfTradePolicy::CancelMaker);
    run(&mut p, limit(1, Side::Sell, 100, 5, 77));
    let reports = run(&mut p, limit(2, Side::Buy, 100, 5, 77));
    assert!(reports.contains(&ExecReport::Cancelled {
        instrument: SYM,
        order_id: OrderId(1),
        user: 77
    }));
}

#[test]
fn report_fingerprints_do_not_depend_on_owners() {
    // Raft application proofs persist report fingerprints computed before
    // owners existed; owners must not change them.
    let with = ExecReport::Accepted {
        instrument: SYM,
        order_id: OrderId(1),
        user: 42,
    };
    let without = ExecReport::Accepted {
        instrument: SYM,
        order_id: OrderId(1),
        user: 0,
    };
    assert_eq!(
        fingerprint_reports(&[with]),
        fingerprint_reports(&[without])
    );
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("tc-users-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn outbox_persists_owners_and_publishes_each_side_to_its_own_user() {
    let dir = temp_dir("outbox");
    let base = dir.join("outbox-shard-0.bin");
    let trade = ExecReport::Trade {
        instrument: SYM,
        taker: OrderId(2),
        maker: OrderId(1),
        aggressor: Side::Buy,
        price: 100,
        qty: 3,
        maker_fee: 1,
        taker_fee: 2,
        taker_user: 502,
        maker_user: 501,
    };
    let mut writer = ExecutionOutboxWriter::open(&base, Duration::from_millis(1), 1).unwrap();
    writer.append(7, 10, 0, &trade).unwrap();
    writer.sync_data().unwrap();
    drop(writer);

    let records = ExecutionOutboxReader::open(base.clone())
        .unwrap()
        .read_batch(10)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].users, [502, 501]);
    assert_eq!(records[0].target_order_ids(), vec![2, 1]);
    for (target, user) in [(2u64, 502u64), (1, 501)] {
        let payload = records[0].kafka_payload_for(target);
        let event = wire::decode_execution_event(&payload).unwrap();
        assert_eq!(event.target_order_id, target);
        assert_eq!(wire::execution_event_target_user(&payload), user);
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// A format-v1 segment (records without owners), byte for byte.
#[test]
fn an_execution_event_carries_its_owner() {
    let report = ExecReport::Filled {
        instrument: SYM,
        order_id: OrderId(8),
        user: 808,
    };
    let mut v3 = [0u8; wire::EXECUTION_EVENT_LEN];
    wire::encode_execution_event(7, 33, 1, &report, &mut v3);
    assert_eq!(wire::execution_event_target_user(&v3), 808);
    // Anything shorter is not an event.
    assert!(wire::decode_execution_event(&v3[..96]).is_none());
    assert_eq!(wire::execution_event_target_user(&v3[..96]), 0);
}

#[test]
fn a_maker_filled_completely_is_reported_to_its_owner_but_not_proved() {
    let mut p = Processor::new(|| Box::new(PriceTimePriority), None);
    run(&mut p, limit(1, Side::Sell, 100, 5, 601));
    run(&mut p, limit(2, Side::Sell, 101, 5, 602));

    // Takes all of #1 and part of #2: only #1 is reported filled.
    let reports = run(&mut p, limit(3, Side::Buy, 101, 7, 603));
    let maker_filled: Vec<_> = reports
        .iter()
        .filter_map(|r| match r {
            ExecReport::MakerFilled { order_id, user, .. } => Some((order_id.0, *user)),
            _ => None,
        })
        .collect();
    assert_eq!(maker_filled, vec![(1, 601)], "{reports:?}");
    // It follows the trade that filled the maker.
    let at = reports
        .iter()
        .position(|r| matches!(r, ExecReport::MakerFilled { .. }))
        .unwrap();
    assert!(matches!(
        reports[at - 1],
        ExecReport::Trade {
            maker: OrderId(1),
            ..
        }
    ));

    // Raft application proofs written before MakerFilled existed must still
    // verify: the digest ignores it.
    let legacy: Vec<ExecReport> = reports
        .iter()
        .filter(|r| !matches!(r, ExecReport::MakerFilled { .. }))
        .cloned()
        .collect();
    assert_eq!(
        proof_digest(&reports),
        (legacy.len() as u32, fingerprint_reports(&legacy))
    );
}
