//! Raft-authoritative recovery: a durable in-memory state snapshot plus the
//! committed Raft tail rebuilds the same book and reproduces the exact result
//! proof without writing duplicate Outbox events.

use std::time::{Duration, Instant};

use trade_core::asset_log::load_applied_batch_proofs;
use trade_core::exchange::{build, Command, ExchangeConfig, ExecReport};
use trade_core::execution_outbox::{ExecutionOutboxReader, ExecutionOutboxWriter};
use trade_core::order::Order;
use trade_core::types::{InstrumentId, OrderId, Side};

fn collect(sink: &trade_core::ResultSink, wanted: usize) -> Vec<trade_core::ExecutionReportEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    while events.len() < wanted && Instant::now() < deadline {
        if sink.poll_events(|event| events.push(event)) == 0 {
            std::thread::yield_now();
        }
    }
    events
}

fn tail_command(instrument: InstrumentId) -> Command {
    Command::Batch(vec![Command::New(
        Order::limit(OrderId(3), Side::Buy, 90, 4).on(instrument),
    )])
}

#[test]
fn snapshot_plus_raft_tail_replay_is_identical_and_emits_no_duplicate_results() {
    let root = std::env::temp_dir().join(format!("tc-raft-snapshot-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let journal = root.join("journal");
    let outbox = root.join("execution-outbox");
    let instrument = InstrumentId(0);
    let config = ExchangeConfig {
        journal_dir: Some(journal.clone()),
        raft_wal_authoritative: true,
        execution_outbox_dir: Some(outbox),
        raft_group_id: 7,
        ..ExchangeConfig::default()
    };

    {
        let (gateway, sink, handle) = build(config.clone());
        gateway
            .submit_committed(
                88,
                Command::Batch(vec![
                    Command::New(Order::limit(OrderId(1), Side::Sell, 100, 5).on(instrument)),
                    Command::New(Order::limit(OrderId(2), Side::Buy, 100, 5).on(instrument)),
                ]),
            )
            .unwrap();
        assert_eq!(collect(&sink, 5).len(), 5);

        handle.snapshot_now();
        let snapshot_path = journal.join("snapshot-shard-0.bin");
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if trade_core::snapshot::load(&snapshot_path)
                .is_ok_and(|snapshot| snapshot.raft_applied_index == 88)
            {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(
            trade_core::snapshot::load(&snapshot_path)
                .unwrap()
                .raft_applied_index,
            88
        );

        gateway
            .submit_committed(89, tail_command(instrument))
            .unwrap();
        assert_eq!(collect(&sink, 2).len(), 2);
        handle.shutdown();
    }

    assert!(journal.join("snapshot-shard-0.bin").exists());
    assert!(!journal.join("journal-shard-0.bin").exists());
    assert_eq!(
        std::fs::read_dir(journal.join("assets"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
            .count(),
        0,
        "Raft-authoritative mode must not create per-asset command WALs"
    );

    let proofs = load_applied_batch_proofs(&journal.join("assets")).unwrap();
    let proof = *proofs.get(&89).expect("tail batch has exact replay proof");
    {
        let (gateway, sink, handle) = build(config);
        gateway
            .submit_recovered(proof, tail_command(instrument))
            .unwrap();
        let deadline = Instant::now() + Duration::from_millis(100);
        let mut duplicates = 0;
        while Instant::now() < deadline {
            duplicates += sink.poll_events(|_| {});
            std::thread::yield_now();
        }
        assert_eq!(duplicates, 0, "recovery must not re-emit durable results");

        gateway
            .submit_committed(
                90,
                Command::Batch(vec![Command::Cancel {
                    instrument,
                    order_id: OrderId(3),
                    cmd_id: 4,
                }]),
            )
            .unwrap();
        let cancelled = collect(&sink, 1);
        assert!(matches!(
            cancelled.as_slice(),
            [trade_core::ExecutionReportEvent {
                report: ExecReport::Cancelled { order_id, .. },
                ..
            }] if *order_id == OrderId(3)
        ));
        handle.shutdown();
    }

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn outbox_tail_without_durable_application_proof_is_trimmed_then_regenerated() {
    let root = std::env::temp_dir().join(format!("tc-raft-unproved-outbox-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let journal = root.join("journal");
    let outbox_dir = root.join("execution-outbox");
    let outbox_path = outbox_dir.join("outbox-shard-0.bin");
    std::fs::create_dir_all(&outbox_dir).unwrap();

    // Crash window: an execution reached durable Outbox storage, but the
    // application proof was never persisted. Recovery must discard this
    // unproved tail and regenerate it from the committed Raft command.
    {
        let mut writer =
            ExecutionOutboxWriter::open(&outbox_path, Duration::from_secs(1), 1).unwrap();
        writer
            .append_deferred(
                3,
                55,
                0,
                &ExecReport::Accepted {
                    instrument: InstrumentId(0),
                    order_id: OrderId(55),
                },
            )
            .unwrap();
        writer.sync_data().unwrap();
    }
    assert_eq!(
        ExecutionOutboxReader::open(outbox_path.clone())
            .unwrap()
            .read_batch(10)
            .unwrap()
            .len(),
        1
    );

    let config = ExchangeConfig {
        journal_dir: Some(journal.clone()),
        raft_wal_authoritative: true,
        execution_outbox_dir: Some(outbox_dir),
        raft_group_id: 3,
        ..ExchangeConfig::default()
    };
    let (gateway, sink, handle) = build(config);
    assert_eq!(
        ExecutionOutboxReader::open(outbox_path.clone())
            .unwrap()
            .read_batch(10)
            .unwrap()
            .len(),
        0,
        "unproved Outbox tail must be removed before Raft replay"
    );
    gateway
        .submit_committed(
            55,
            Command::Batch(vec![Command::New(
                Order::limit(OrderId(55), Side::Buy, 90, 1).on(InstrumentId(0)),
            )]),
        )
        .unwrap();
    assert_eq!(collect(&sink, 2).len(), 2);
    handle.shutdown();

    let records = ExecutionOutboxReader::open(outbox_path)
        .unwrap()
        .read_batch(10)
        .unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record.raft_index == 55));
    assert!(load_applied_batch_proofs(&journal.join("assets"))
        .unwrap()
        .contains_key(&55));
    std::fs::remove_dir_all(root).ok();
}
