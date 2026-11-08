//! The exact command-id dedupe window is replicated state: a replica that
//! restarted from a snapshot must accept and reject retries exactly like
//! one that never stopped (audit E1). Before, the window was an unbounded
//! set rebuilt from resting orders only, so a late redelivery of a filled
//! order was matched again after a restart.

use trade_core::exchange::{ExecReport, Processor};
use trade_core::snapshot::{self, SnapshotData};
use trade_core::strategy::PriceTimePriority;
use trade_core::{Command, InstrumentId, Order, OrderId, Side};

const INSTRUMENT: InstrumentId = InstrumentId(3);

fn processor(window: usize) -> Processor {
    Processor::new(|| Box::new(PriceTimePriority), None).with_command_id_window(window)
}

fn new(id: u64, side: Side, price: u64, qty: u64) -> Command {
    Command::New(Order::limit(OrderId(id), side, price, qty).on(INSTRUMENT).by(id))
}

fn run(p: &mut Processor, cmd: Command) -> Vec<String> {
    let mut out = Vec::new();
    p.process(cmd, &mut |report: ExecReport| out.push(format!("{report:?}")));
    out
}

/// Write `p`'s snapshot the way the matching shard does and restore it into
/// a fresh processor.
fn restart(p: &Processor, window: usize, tag: &str) -> Processor {
    let dir = std::env::temp_dir().join(format!("tc-dedupe-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    p.write_snapshot(&path, 0, 7).unwrap();
    let snap = snapshot::load(&path).unwrap();
    assert_eq!(snap.format_version, 6);
    let mut restored = processor(window);
    restored.restore_state(&snap);
    std::fs::remove_dir_all(&dir).unwrap();
    restored
}

#[test]
fn a_restarted_replica_rejects_a_retry_of_a_filled_order_like_a_live_one() {
    let mut live = processor(1_000);
    run(&mut live, new(1, Side::Sell, 100, 5));
    run(&mut live, new(2, Side::Buy, 100, 5)); // fills order 1 completely
    let mut restarted = restart(&live, 1_000, "filled");

    // A Kafka redelivery of order 1 arrives after the fill.
    let a = run(&mut live, new(1, Side::Sell, 100, 5));
    let b = run(&mut restarted, new(1, Side::Sell, 100, 5));
    assert_eq!(a, b, "replicas diverged on a retry");
    assert!(a[0].contains("duplicate"), "{a:?}");
    assert_eq!(live.state_fingerprint(), restarted.state_fingerprint());
}

#[test]
fn eviction_is_by_log_order_and_survives_snapshots() {
    let window = 3;
    let mut live = processor(window);
    for id in 10..15 {
        run(&mut live, new(id, Side::Buy, 90 + id, 1));
    }
    // Window now holds 12, 13, 14; 10 and 11 were evicted in log order.
    let mut restarted = restart(&live, window, "evict");
    for id in [10, 12, 14, 11, 13] {
        let a = run(&mut live, new(id, Side::Sell, 200, 1));
        let b = run(&mut restarted, new(id, Side::Sell, 200, 1));
        assert_eq!(a, b, "id {id}");
    }
    assert_eq!(live.state_fingerprint(), restarted.state_fingerprint());
}

#[test]
fn memory_is_bounded_by_the_window() {
    let mut p = processor(100);
    for id in 1..=10_000 {
        run(&mut p, new(id, Side::Buy, 1 + id % 50, 1));
    }
    let dir = std::env::temp_dir().join(format!("tc-dedupe-bound-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    p.write_snapshot(&path, 0, 1).unwrap();
    let snap = snapshot::load(&path).unwrap();
    assert_eq!(snap.recent_command_ids.as_ref().unwrap().len(), 100);
    assert_eq!(snap.recent_command_ids.unwrap().first(), Some(&9_901));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_pre_window_snapshot_restores_with_the_legacy_seed() {
    // A v4 snapshot (no window): resting ids plus the ids the node recovered
    // from its log are treated as seen, as before the upgrade.
    let mut old = processor(1_000);
    run(&mut old, new(1, Side::Sell, 100, 5));
    let dir = std::env::temp_dir().join(format!("tc-dedupe-legacy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    snapshot::write(
        &path,
        SnapshotData {
            journal_seq: 0,
            raft_applied_index: 3,
            max_cmd_id: 0,
            max_admin_id: 0,
            halted: &[],
            suspended: &[],
            positions: &[],
            engines: &old.export_state(),
            recent_command_ids: None,
        },
    )
    .unwrap();
    let snap = snapshot::load(&path).unwrap();
    assert_eq!(snap.format_version, 4);
    let mut restored = processor(1_000).with_legacy_command_ids(vec![7]);
    restored.restore_state(&snap);
    assert!(run(&mut restored, new(1, Side::Sell, 100, 5))[0].contains("duplicate"));
    assert!(run(&mut restored, new(7, Side::Sell, 100, 5))[0].contains("duplicate"));
    assert!(!run(&mut restored, new(8, Side::Sell, 100, 5))[0].contains("duplicate"));
    std::fs::remove_dir_all(&dir).unwrap();
}
