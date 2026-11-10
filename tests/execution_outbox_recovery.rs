//! P0-1 crash-order recovery: the execution outbox is made durable *before*
//! the Raft application watermark advances, so a committed batch's execution
//! events can never be lost, and recovery replays them exactly once.

use std::path::Path;
use std::time::{Duration, Instant};

use trade_core::asset_log::load_applied_batches;
use trade_core::exchange::{build, Command, ExchangeConfig, ExecReport, ExecutionReportEvent};
use trade_core::execution_outbox::{truncate_after_applied, ExecutionOutboxReader};
use trade_core::prelude::*;
use trade_core::InstrumentId;

/// The durable outbox records reduced to their deterministic event ids.
fn read_outbox_ids(path: &Path) -> Vec<(u64, u32)> {
    let mut reader = ExecutionOutboxReader::open(path.to_path_buf()).unwrap();
    let mut ids = Vec::new();
    reader
        .read_available(|record| ids.push((record.raft_index, record.ordinal)))
        .unwrap();
    ids
}

fn collect_events(sink: &trade_core::ResultSink, want: usize) -> Vec<ExecutionReportEvent> {
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < want && Instant::now() < deadline {
        let n = sink.poll_events(|event| {
            if !matches!(
                event.report,
                ExecReport::DepthLevel { .. } | ExecReport::DepthEnd { .. }
            ) {
                got.push(event);
            }
        });
        if n == 0 {
            std::thread::yield_now();
        }
    }
    got
}

#[test]
fn committed_batch_is_durable_in_outbox_before_watermark_and_survives_restart() {
    let root = std::env::temp_dir().join(format!("tc-outbox-recovery-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let journal_dir = root.join("journal");
    let outbox_dir = root.join("execution-outbox");
    let outbox_path = outbox_dir.join("outbox-shard-0.bin");
    let assets_dir = journal_dir.join("assets");
    let instrument = InstrumentId(0);

    // A crossing batch produces five execution reports (Accepted + Resting +
    // Trade + Filled variants); every one must land in the outbox.
    {
        let (gw, sink, handle) = build(ExchangeConfig {
            journal_dir: Some(journal_dir.clone()),
            execution_outbox_dir: Some(outbox_dir.clone()),
            raft_group_id: 4,
            ..ExchangeConfig::default()
        });
        let commands = vec![
            Command::New(Order::limit(OrderId(1), Side::Sell, 100, 5).on(instrument)),
            Command::New(Order::limit(OrderId(2), Side::Buy, 100, 5).on(instrument)),
        ];
        gw.submit_committed(88, Command::Batch(commands)).unwrap();
        let events = collect_events(&sink, 6);
        assert_eq!(
            events.len(),
            6,
            "the crossing batch emits six reports (incl. MakerFilled)"
        );
        // Reports are delivered to the order system only after the batch is
        // durable, so seeing all six means the watermark has advanced too.
        handle.shutdown();
    }

    // The watermark records batch 88 as applied...
    let applied = load_applied_batches(&assets_dir).unwrap();
    assert!(
        applied.contains(&88),
        "batch 88 must be watermarked as applied"
    );

    // ...and because the outbox fsync precedes the watermark, all six events
    // are already durable with their deterministic (raft_index, ordinal) ids.
    let durable = read_outbox_ids(&outbox_path);
    assert_eq!(
        durable,
        vec![(88, 0), (88, 1), (88, 2), (88, 3), (88, 4), (88, 5)],
        "every committed fill is durable, in deterministic order"
    );

    // Recovery trims the outbox to the last durable batch. Batch 88 is
    // watermarked, so nothing is dropped: no loss, no truncation of committed
    // fills.
    let max_applied = applied.iter().max().copied();
    truncate_after_applied(&outbox_path, max_applied).unwrap();
    assert_eq!(read_outbox_ids(&outbox_path).len(), 6);

    // Restarting the exchange runs the same recovery trim during build(); the
    // committed fills remain intact and the outbox stays appendable.
    {
        let (_gw, _sink, handle) = build(ExchangeConfig {
            journal_dir: Some(journal_dir.clone()),
            execution_outbox_dir: Some(outbox_dir.clone()),
            raft_group_id: 4,
            ..ExchangeConfig::default()
        });
        handle.shutdown();
    }
    assert_eq!(read_outbox_ids(&outbox_path).len(), 6);

    std::fs::remove_dir_all(root).ok();
}

/// Audit R1: replicas follow the leader's publish progress by logical
/// position. A replica that installed a Raft snapshot lacks the records it
/// covers, so the leader's byte offset pointed past records it had never
/// published; the logical position lands on the same record everywhere.
#[test]
fn replicas_follow_the_publish_position_not_the_byte_offset() {
    use trade_core::exchange::ExecReport;
    use trade_core::execution_outbox::{ExecutionOutboxReader, ExecutionOutboxWriter};
    let dir = std::env::temp_dir().join(format!("tc-outbox-logical-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let report = |id: u64| ExecReport::Cancelled {
        instrument: trade_core::InstrumentId(1),
        order_id: trade_core::OrderId(id),
        user: 0,
    };
    // Leader: entries 1..=10, two records each. Follower: installed a
    // snapshot at index 5, so it holds only entries 6..=10.
    let write = |path: &std::path::Path, from: u64| {
        let mut writer = ExecutionOutboxWriter::open(path, Duration::from_secs(60), 1_000).unwrap();
        for index in from..=10 {
            for ordinal in 0..2 {
                writer
                    .append(0, index, ordinal, &report(index * 10 + ordinal as u64))
                    .unwrap();
            }
        }
        writer.sync_data().unwrap();
    };
    let leader_path = dir.join("leader").join("outbox-shard-0.bin");
    let follower_path = dir.join("follower").join("outbox-shard-0.bin");
    write(&leader_path, 1);
    write(&follower_path, 6);

    // The leader published through entry 7, ordinal 1.
    let mut leader = ExecutionOutboxReader::open(leader_path).unwrap();
    let mut follower = ExecutionOutboxReader::open(follower_path).unwrap();
    leader.acknowledge_through((7, 1)).unwrap();
    follower.acknowledge_through((7, 1)).unwrap();
    let next = |reader: &ExecutionOutboxReader| {
        let record = reader.read_batch(1).unwrap()[0];
        (record.raft_index, record.ordinal)
    };
    assert_eq!(next(&leader), (8, 0));
    assert_eq!(
        next(&follower),
        (8, 0),
        "follower skipped unpublished records"
    );
    assert_eq!(leader.pending_records().unwrap(), 6);
    assert_eq!(follower.pending_records().unwrap(), 6);

    // Mid-entry positions, and positions not reached yet, also land exactly.
    follower.acknowledge_through((8, 0)).unwrap();
    assert_eq!(next(&follower), (8, 1));
    follower.acknowledge_through((3, 0)).unwrap(); // never moves backwards
    assert_eq!(next(&follower), (8, 1));
    follower.acknowledge_through((99, 0)).unwrap();
    assert_eq!(follower.pending_records().unwrap(), 0);
    std::fs::remove_dir_all(&dir).unwrap();
}
