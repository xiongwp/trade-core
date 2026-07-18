//! P0-1 crash-order recovery: the execution outbox is made durable *before*
//! the Raft application watermark advances, so a committed batch's execution
//! events can never be lost, and recovery replays them exactly once.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use trade_core::asset_log::load_applied_batches;
use trade_core::exchange::{build, Command, ExchangeConfig, ExecReport, ExecutionReportEvent};
use trade_core::execution_outbox::{truncate_after_applied, ExecutionOutboxReader};
use trade_core::prelude::*;
use trade_core::InstrumentId;

/// The durable outbox records reduced to their deterministic event ids.
fn read_outbox_ids(path: &PathBuf) -> Vec<(u64, u32)> {
    let mut reader = ExecutionOutboxReader::open(path.clone()).unwrap();
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
    let root =
        std::env::temp_dir().join(format!("tc-outbox-recovery-{}", std::process::id()));
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
        let events = collect_events(&sink, 5);
        assert_eq!(events.len(), 5, "the crossing batch emits five reports");
        // Reports are delivered to the order system only after the batch is
        // durable, so seeing all five means the watermark has advanced too.
        handle.shutdown();
    }

    // The watermark records batch 88 as applied...
    let applied = load_applied_batches(&assets_dir).unwrap();
    assert!(
        applied.contains(&88),
        "batch 88 must be watermarked as applied"
    );

    // ...and because the outbox fsync precedes the watermark, all five events
    // are already durable with their deterministic (raft_index, ordinal) ids.
    let durable = read_outbox_ids(&outbox_path);
    assert_eq!(
        durable,
        vec![(88, 0), (88, 1), (88, 2), (88, 3), (88, 4)],
        "every committed fill is durable, in deterministic order"
    );

    // Recovery trims the outbox to the last durable batch. Batch 88 is
    // watermarked, so nothing is dropped: no loss, no truncation of committed
    // fills.
    let max_applied = applied.iter().max().copied();
    truncate_after_applied(&outbox_path, max_applied).unwrap();
    assert_eq!(read_outbox_ids(&outbox_path).len(), 5);

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
    assert_eq!(read_outbox_ids(&outbox_path).len(), 5);

    std::fs::remove_dir_all(root).ok();
}
