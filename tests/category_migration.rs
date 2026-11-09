//! Moving an asset category between Raft groups at the engine level: seal
//! on the source, export from its snapshot, install on the target, drop on
//! the source. Each step is a replicated command, so these are the state
//! transitions every replica of a group makes.

use trade_core::exchange::{CategoryOp, ExecReport, Processor};
use trade_core::migration::{encode_category_state, export_category};
use trade_core::prelude::*;
use trade_core::snapshot;
use trade_core::types::{Trigger, TriggerKind};
use trade_core::Command;

const SIZE: u32 = 1_000;
/// Category 0 (instruments 1..=1000) moves; category 1 stays.
const MOVING: InstrumentId = InstrumentId(1);
const STAYING: InstrumentId = InstrumentId(1_001);

fn processor() -> Processor {
    Processor::new(|| Box::new(PriceTimePriority), None)
}

fn run(p: &mut Processor, cmd: Command) -> Vec<ExecReport> {
    let mut out = Vec::new();
    p.process(cmd, &mut |r| out.push(r));
    out
}

fn limit(id: u64, user: u64, instrument: InstrumentId, side: Side, price: Price, qty: Qty) -> Command {
    Command::New(Order::limit(OrderId(id), side, price, qty).on(instrument).by(user))
}

fn op(p: &mut Processor, op: CategoryOp) -> Vec<ExecReport> {
    run(p, Command::Migrate(Box::new(op)))
}

fn rejected(reports: &[ExecReport]) -> Option<&'static str> {
    reports.iter().find_map(|r| match r {
        ExecReport::CommandRejected { reason, .. } => Some(*reason),
        _ => None,
    })
}

/// Snapshot `p` and cut out category 0 the way a source node serves it.
fn export(p: &Processor, tag: &str) -> (Vec<snapshot::EngineState>, Vec<(u64, u32, i64)>) {
    let dir = std::env::temp_dir().join(format!("tc-migrate-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    p.write_snapshot(&path, 0, 50).unwrap();
    let snap = snapshot::load(&path).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    // Through the transfer encoding, as between groups.
    let (engines, positions) = export_category(&snap, 0, SIZE);
    let wire = encode_category_state(snap.raft_applied_index, &engines, &positions);
    let received = snapshot::decode(&wire).unwrap();
    assert_eq!(received.raft_applied_index, 50);
    (received.engines, received.positions)
}

#[test]
fn a_category_moves_with_its_book_stops_and_positions() {
    let mut source = processor();
    run(&mut source, limit(1, 10, MOVING, Side::Sell, 101, 5));
    run(&mut source, limit(2, 20, MOVING, Side::Buy, 99, 5));
    run(&mut source, limit(3, 30, MOVING, Side::Buy, 101, 2)); // trades 2: positions
    run(
        &mut source,
        Command::NewConditional(
            Order::market(OrderId(4), Side::Sell, 1).on(MOVING).by(40),
            Trigger { price: 95, kind: TriggerKind::StopLoss },
        ),
    );
    run(&mut source, limit(5, 50, STAYING, Side::Buy, 10, 1));
    let before = source.category_fingerprint(0, SIZE);

    // 1. Seal: the category takes no more commands on the source.
    op(&mut source, CategoryOp::Seal { category: 0, category_size: SIZE });
    let late = run(&mut source, limit(6, 60, MOVING, Side::Buy, 99, 1));
    assert_eq!(rejected(&late), Some("category-moved"));
    assert!(rejected(&run(&mut source, limit(7, 70, STAYING, Side::Buy, 11, 1))).is_none());

    // 2. Export from the source snapshot, 3. install on the target.
    let (engines, positions) = export(&source, "move");
    let mut target = processor();
    let installed = op(
        &mut target,
        CategoryOp::Install { category: 0, category_size: SIZE, engines, positions },
    );
    assert!(rejected(&installed).is_none(), "{installed:?}");
    assert_eq!(target.category_fingerprint(0, SIZE), before, "book moved exactly");
    assert_eq!(target.position(30, MOVING), 2);
    assert_eq!(target.position(10, MOVING), -2);

    // The moved book trades and cancels on the target; the stop came along.
    let out = run(&mut target, limit(8, 80, MOVING, Side::Buy, 101, 3));
    assert!(out.iter().any(|r| matches!(r, ExecReport::Trade { maker: OrderId(1), qty: 3, .. })), "{out:?}");
    let out = run(&mut target, Command::Cancel { instrument: MOVING, order_id: OrderId(2), cmd_id: 9 });
    assert!(out.iter().any(|r| matches!(r, ExecReport::Cancelled { order_id: OrderId(2), .. })), "{out:?}");
    let out = run(&mut target, limit(10, 100, MOVING, Side::Buy, 90, 1));
    let out2 = run(&mut target, limit(11, 110, MOVING, Side::Sell, 90, 1)); // prints 90: fires the stop
    assert!(
        out.iter().chain(&out2).any(|r| matches!(r, ExecReport::Triggered { order_id: OrderId(4), .. })),
        "{out2:?}"
    );

    // 4. Drop on the source: the state is gone and the category stays sealed.
    op(&mut source, CategoryOp::Drop { category: 0, category_size: SIZE });
    assert_eq!(source.category_fingerprint(0, SIZE), processor().category_fingerprint(0, SIZE));
    assert_eq!(source.position(30, MOVING), 0);
    assert_eq!(rejected(&run(&mut source, limit(12, 120, MOVING, Side::Buy, 99, 1))), Some("category-moved"));
}

#[test]
fn the_seal_survives_a_snapshot_restore() {
    let mut source = processor();
    op(&mut source, CategoryOp::Seal { category: 0, category_size: SIZE });
    let dir = std::env::temp_dir().join(format!("tc-migrate-seal-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    source.write_snapshot(&path, 0, 7).unwrap();
    let snap = snapshot::load(&path).unwrap();
    assert_eq!(snap.format_version, 7);
    assert_eq!(snap.sealed, vec![(0, SIZE)]);
    let mut restored = processor();
    restored.restore_state(&snap);
    assert_eq!(rejected(&run(&mut restored, limit(1, 10, MOVING, Side::Buy, 99, 1))), Some("category-moved"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn install_is_refused_whole_when_the_target_already_holds_the_category() {
    let mut source = processor();
    run(&mut source, limit(1, 10, MOVING, Side::Sell, 101, 5));
    let (engines, positions) = export(&source, "occupied");
    let mut target = processor();
    run(&mut target, limit(2, 20, MOVING, Side::Buy, 90, 1)); // a stray order already here
    let before = target.category_fingerprint(0, SIZE);
    let out = op(&mut target, CategoryOp::Install { category: 0, category_size: SIZE, engines, positions });
    assert_eq!(rejected(&out), Some("category-not-empty"));
    assert_eq!(target.category_fingerprint(0, SIZE), before, "nothing installed");
}

#[test]
fn a_category_can_move_back_to_a_group_that_sealed_it() {
    let mut a = processor();
    run(&mut a, limit(1, 10, MOVING, Side::Sell, 101, 5));
    op(&mut a, CategoryOp::Seal { category: 0, category_size: SIZE });
    let (engines, positions) = export(&a, "back1");
    let mut b = processor();
    op(&mut b, CategoryOp::Install { category: 0, category_size: SIZE, engines, positions });
    op(&mut a, CategoryOp::Drop { category: 0, category_size: SIZE });
    // ... and back again.
    op(&mut b, CategoryOp::Seal { category: 0, category_size: SIZE });
    let (engines, positions) = export(&b, "back2");
    let out = op(&mut a, CategoryOp::Install { category: 0, category_size: SIZE, engines, positions });
    assert!(rejected(&out).is_none(), "{out:?}");
    let out = run(&mut a, limit(2, 20, MOVING, Side::Buy, 101, 1));
    assert!(out.iter().any(|r| matches!(r, ExecReport::Trade { maker: OrderId(1), .. })), "{out:?}");
}
