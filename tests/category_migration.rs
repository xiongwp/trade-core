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

/// Snapshot `p` and cut out category 0 the way a source node serves it,
/// as the install step a target receives for `epoch`.
fn export(p: &Processor, tag: &str, epoch: u64) -> CategoryOp {
    let dir = std::env::temp_dir().join(format!("tc-migrate-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    p.write_snapshot(&path, 0, 50).unwrap();
    let snap = snapshot::load(&path).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    // Through the transfer encoding, as between groups.
    let export = export_category(&snap, 0, SIZE);
    let wire = encode_category_state(
        snap.raft_applied_index,
        &export.engines,
        &export.positions,
        &export.halted,
        &export.command_ids,
    );
    let received = snapshot::decode(&wire).unwrap();
    assert_eq!(received.raft_applied_index, 50);
    let command_ids = received
        .recent_command_ids
        .into_iter()
        .zip(received.recent_command_instruments)
        .collect();
    CategoryOp::Install {
        category: 0,
        category_size: SIZE,
        epoch,
        engines: received.engines,
        positions: received.positions,
        halted: received.halted,
        command_ids,
    }
}

fn seal(epoch: u64) -> CategoryOp {
    CategoryOp::Seal { category: 0, category_size: SIZE, epoch }
}

fn drop_(epoch: u64) -> CategoryOp {
    CategoryOp::Drop { category: 0, category_size: SIZE, epoch }
}

fn unseal(epoch: u64) -> CategoryOp {
    CategoryOp::Unseal { category: 0, category_size: SIZE, epoch }
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
    op(&mut source, seal(1));
    let late = run(&mut source, limit(6, 60, MOVING, Side::Buy, 99, 1));
    assert_eq!(rejected(&late), Some("category-moved"));
    assert!(rejected(&run(&mut source, limit(7, 70, STAYING, Side::Buy, 11, 1))).is_none());

    // 2. Export from the source snapshot, 3. install on the target.
    let install = export(&source, "move", 1);
    let mut target = processor();
    let installed = op(&mut target, install);
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
    op(&mut source, drop_(1));
    assert_eq!(source.category_fingerprint(0, SIZE), processor().category_fingerprint(0, SIZE));
    assert_eq!(source.position(30, MOVING), 0);
    assert_eq!(rejected(&run(&mut source, limit(12, 120, MOVING, Side::Buy, 99, 1))), Some("category-moved"));
}

#[test]
fn a_command_redelivered_after_the_move_is_still_a_duplicate() {
    let mut source = processor();
    run(&mut source, limit(1, 10, MOVING, Side::Sell, 100, 5));
    run(&mut source, limit(2, 20, MOVING, Side::Buy, 100, 5)); // fully filled: no longer resting
    op(&mut source, seal(1));
    let mut target = processor();
    op(&mut target, export(&source, "dupe", 1));
    // Kafka redelivers order 2 after the move: it must not trade again.
    let again = run(&mut target, limit(2, 20, MOVING, Side::Buy, 100, 5));
    assert_eq!(rejected(&again), Some("duplicate"), "{again:?}");
    assert_eq!(target.position(20, MOVING), 5);
}

#[test]
fn a_halted_instrument_stays_halted_after_the_move() {
    let mut source = processor();
    run(&mut source, limit(1, 10, MOVING, Side::Sell, 100, 5));
    run(&mut source, Command::Halt { instrument: MOVING, cmd_id: 2 });
    op(&mut source, seal(1));
    let mut target = processor();
    op(&mut target, export(&source, "halt", 1));
    let out = run(&mut target, limit(3, 20, MOVING, Side::Buy, 100, 1));
    assert_eq!(
        out.iter().find_map(|r| match r {
            ExecReport::Rejected { reason, .. } => Some(*reason),
            _ => None,
        }),
        Some("halted")
    );
}

#[test]
fn the_seal_survives_a_snapshot_restore() {
    let mut source = processor();
    op(&mut source, seal(3));
    let dir = std::env::temp_dir().join(format!("tc-migrate-seal-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bin");
    source.write_snapshot(&path, 0, 7).unwrap();
    let snap = snapshot::load(&path).unwrap();
    assert_eq!(snap.sealed, vec![(0, SIZE)]);
    assert_eq!(snap.migration_epochs, vec![(0, 3)]);
    let mut restored = processor();
    restored.restore_state(&snap);
    assert_eq!(rejected(&run(&mut restored, limit(1, 10, MOVING, Side::Buy, 99, 1))), Some("category-moved"));
    // The epoch came back too: an older migration's unseal is ignored.
    assert_eq!(rejected(&op(&mut restored, unseal(2))), Some("stale-migration"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn install_is_refused_whole_when_the_target_already_holds_the_category() {
    let mut source = processor();
    run(&mut source, limit(1, 10, MOVING, Side::Sell, 101, 5));
    let install = export(&source, "occupied", 1);
    let mut target = processor();
    run(&mut target, limit(2, 20, MOVING, Side::Buy, 90, 1)); // a stray order already here
    let before = target.category_fingerprint(0, SIZE);
    let out = op(&mut target, install);
    assert_eq!(rejected(&out), Some("category-not-empty"));
    assert_eq!(target.category_fingerprint(0, SIZE), before, "nothing installed");
}

#[test]
fn leftover_positions_alone_refuse_an_install() {
    let mut source = processor();
    run(&mut source, limit(1, 10, MOVING, Side::Sell, 100, 2));
    run(&mut source, limit(2, 20, MOVING, Side::Buy, 100, 2)); // books empty, positions open
    let install = export(&source, "positions", 1);
    let mut target = processor();
    run(&mut target, limit(3, 30, MOVING, Side::Sell, 100, 1));
    run(&mut target, limit(4, 40, MOVING, Side::Buy, 100, 1)); // empty book, positions left
    let out = op(&mut target, install);
    assert_eq!(rejected(&out), Some("category-not-empty"), "positions would have been added up");
    assert_eq!(target.position(20, MOVING), 0);
}

#[test]
fn a_category_can_move_back_to_a_group_that_sealed_it() {
    let mut a = processor();
    run(&mut a, limit(1, 10, MOVING, Side::Sell, 101, 5));
    op(&mut a, seal(1));
    let install = export(&a, "back1", 1);
    let mut b = processor();
    op(&mut b, install);
    op(&mut a, drop_(1));
    // ... and back again (a newer migration).
    op(&mut b, seal(2));
    let install = export(&b, "back2", 2);
    let out = op(&mut a, install);
    assert!(rejected(&out).is_none(), "{out:?}");
    let out = run(&mut a, limit(2, 20, MOVING, Side::Buy, 101, 1));
    assert!(out.iter().any(|r| matches!(r, ExecReport::Trade { maker: OrderId(1), .. })), "{out:?}");
    // A delayed drop of the first migration must not wipe the books now here.
    assert_eq!(rejected(&op(&mut a, drop_(1))), Some("stale-migration"));
    assert_eq!(a.position(20, MOVING), 1);
}

#[test]
fn steps_of_a_stale_or_concurrent_migration_cannot_undo_a_newer_one() {
    // Migration 5 moved the category A -> B.
    let mut a = processor();
    run(&mut a, limit(1, 10, MOVING, Side::Sell, 101, 5));
    op(&mut a, seal(5));
    let install = export(&a, "fence", 5);
    let mut b = processor();
    op(&mut b, install);
    op(&mut a, drop_(5));
    let live = b.category_fingerprint(0, SIZE);
    // Migration 9, started with a stale route (it thinks the category is on
    // A), seals A again — refused: A is sealed by migration 5 ...
    assert_eq!(rejected(&op(&mut a, seal(9))), Some("stale-migration"));
    // ... and its rollback must not touch B's live books or unseal A.
    assert_eq!(rejected(&op(&mut b, drop_(9))), Some("stale-migration"));
    assert_eq!(b.category_fingerprint(0, SIZE), live);
    assert_eq!(rejected(&op(&mut a, unseal(9))), Some("stale-migration"));
    assert_eq!(rejected(&run(&mut a, limit(2, 20, MOVING, Side::Buy, 101, 1))), Some("category-moved"));
    // An install older than the category's last step here is refused too.
    let mut c = processor();
    op(&mut c, seal(7));
    assert_eq!(rejected(&op(&mut c, export(&a, "fence2", 6))), Some("stale-migration"));
}

#[test]
fn a_failed_migration_rolls_back_cleanly_whether_or_not_its_steps_applied() {
    let mut a = processor();
    run(&mut a, limit(1, 10, MOVING, Side::Sell, 101, 5));
    let before = a.category_fingerprint(0, SIZE);
    op(&mut a, seal(4));
    let mut b = processor();
    // The install timed out at the caller but committed anyway; the rollback
    // drops it and unseals the source.
    op(&mut b, export(&a, "rollback", 4));
    assert!(rejected(&op(&mut b, drop_(4))).is_none());
    assert!(rejected(&op(&mut a, unseal(4))).is_none());
    assert_eq!(a.category_fingerprint(0, SIZE), before);
    assert!(rejected(&run(&mut a, limit(2, 20, MOVING, Side::Buy, 100, 1))).is_none(), "trading again");
    // Rolling back a target that never got the install changes nothing.
    let mut c = processor();
    assert_eq!(rejected(&op(&mut c, drop_(4))), Some("stale-migration"));
}

#[test]
fn a_malformed_install_is_rejected_on_every_replica_instead_of_crashing() {
    let mut target = processor();
    let order = Order::limit(OrderId(1), Side::Sell, 101, 5).on(MOVING).by(10);
    let mut duplicate = order;
    duplicate.remaining = 2; // same id twice in one book
    let bad = CategoryOp::Install {
        category: 0,
        category_size: SIZE,
        epoch: 1,
        engines: vec![snapshot::EngineState {
            instrument: MOVING,
            engine_seq: 3,
            orders: vec![order, duplicate],
            stops: Vec::new(),
        }],
        positions: Vec::new(),
        halted: Vec::new(),
        command_ids: Vec::new(),
    };
    assert_eq!(rejected(&op(&mut target, bad)), Some("invalid-install"));
    // Nothing was installed and the processor still works.
    assert!(rejected(&run(&mut target, limit(2, 20, MOVING, Side::Buy, 90, 1))).is_none());
}

#[test]
fn cancels_for_unknown_instruments_leave_no_state_behind() {
    let mut p = processor();
    let before = p.state_fingerprint();
    for instrument in 50_000..50_100u32 {
        let out = run(&mut p, Command::Cancel { instrument: InstrumentId(instrument), order_id: OrderId(9), cmd_id: instrument as u64 });
        assert!(out.iter().any(|r| matches!(r, ExecReport::NotFound { .. })), "{out:?}");
        assert!(p.engine(InstrumentId(instrument)).is_none());
    }
    assert_eq!(p.state_fingerprint(), before);
}
