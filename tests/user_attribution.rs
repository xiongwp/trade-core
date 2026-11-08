//! Execution results name the user who owns each order, end to end: engine
//! reports -> durable outbox -> Kafka execution events. This is what lets a
//! result be pushed straight to its user with no database lookup.

use std::time::Duration;

use trade_core::exchange::{fingerprint_reports, ExecReport, Processor};
use trade_core::execution_outbox::{
    segment_paths, ExecutionOutboxReader, ExecutionOutboxWriter, OUTBOX_RECORD_LEN_V1,
};
use trade_core::journal::fnv1a;
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
fn write_v1_segment(path: &std::path::Path, frames: &[[u8; wire::REPORT_LEN]]) {
    let mut bytes = b"TCEX\x01\0\0\0".to_vec();
    for (i, frame) in frames.iter().enumerate() {
        let mut rec = vec![0u8; OUTBOX_RECORD_LEN_V1];
        rec[0..4].copy_from_slice(&7u32.to_le_bytes());
        rec[4..12].copy_from_slice(&(10 + i as u64).to_le_bytes());
        rec[16..16 + wire::REPORT_LEN].copy_from_slice(frame);
        let n = rec.len();
        let sum = fnv1a(&rec[..n - 8]);
        rec[n - 8..].copy_from_slice(&sum.to_le_bytes());
        bytes.extend_from_slice(&rec);
    }
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn v1_outbox_segments_still_publish_and_the_writer_moves_past_them() {
    let dir = temp_dir("v1");
    let base = dir.join("outbox-shard-0.bin");
    let mut frame = [0u8; wire::REPORT_LEN];
    wire::encode_report(
        &ExecReport::Accepted {
            instrument: SYM,
            order_id: OrderId(1),
            user: 0,
        },
        &mut frame,
    );
    write_v1_segment(&base, &[frame, frame]);

    // Read in place: records decode, owners unknown (0).
    let reader = ExecutionOutboxReader::open(base.clone()).unwrap();
    assert_eq!(reader.record_len(), OUTBOX_RECORD_LEN_V1);
    assert_eq!(reader.pending_records().unwrap(), 2);
    let records = reader.read_batch(10).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].users, [0, 0]);
    let payload = records[0].kafka_payload_for(1);
    assert_eq!(
        wire::decode_execution_event(&payload)
            .unwrap()
            .target_order_id,
        1
    );
    assert_eq!(wire::execution_event_target_user(&payload), 0);

    // Recovery trimming understands the old layout.
    trade_core::execution_outbox::truncate_after_applied(&base, Some(10)).unwrap();
    assert_eq!(
        ExecutionOutboxReader::open(base.clone())
            .unwrap()
            .pending_records()
            .unwrap(),
        1
    );

    // Opening the writer seals the v1 segment and appends to a fresh one.
    let mut writer = ExecutionOutboxWriter::open(&base, Duration::from_millis(1), 1).unwrap();
    writer
        .append(
            7,
            20,
            0,
            &ExecReport::Accepted {
                instrument: SYM,
                order_id: OrderId(3),
                user: 303,
            },
        )
        .unwrap();
    writer.sync_data().unwrap();
    drop(writer);
    let segments = segment_paths(&base).unwrap();
    assert_eq!(segments.len(), 2, "{segments:?}");
    assert_eq!(std::fs::read(&segments[0]).unwrap()[..5], *b"TCEX\x01");
    let fresh = ExecutionOutboxReader::open(segments[1].clone())
        .unwrap()
        .read_batch(10)
        .unwrap();
    assert_eq!(fresh.len(), 1);
    assert_eq!(fresh[0].users, [303, 0]);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn v2_execution_events_still_decode_without_an_owner() {
    let report = ExecReport::Filled {
        instrument: SYM,
        order_id: OrderId(8),
        user: 808,
    };
    let mut v3 = [0u8; wire::EXECUTION_EVENT_LEN];
    wire::encode_execution_event(7, 33, 1, &report, &mut v3);
    assert_eq!(wire::execution_event_target_user(&v3), 808);

    // The same event as a v2 producer wrote it: version 2, 96 bytes.
    let mut v2 = v3[..wire::EXECUTION_EVENT_V2_LEN].to_vec();
    v2[4..8].copy_from_slice(&2u32.to_le_bytes());
    let decoded = wire::decode_execution_event(&v2).unwrap();
    assert_eq!(decoded, wire::decode_execution_event(&v3).unwrap());
    assert_eq!(wire::execution_event_target_user(&v2), 0);
}
