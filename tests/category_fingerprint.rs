//! Per-category matching-state fingerprint (route-migration verification).
//!
//! A category route migration is only safe to cut over once the source and
//! target Raft groups hold byte-identical book state for that category.
//! `Processor::category_fingerprint` is the value both sides must agree on, so
//! `RouteControlPlane::caught_up` can verify equality against real state
//! rather than a caller-supplied number. These tests pin the properties that
//! makes it a trustworthy verification input.

use trade_core::exchange::{Command, Processor};
use trade_core::prelude::*;
use trade_core::sharding::{asset_category, DEFAULT_ASSET_CATEGORY_SIZE};
use trade_core::strategy::PriceTimePriority;
use trade_core::InstrumentId;

const CAT_SIZE: u32 = DEFAULT_ASSET_CATEGORY_SIZE;

fn processor() -> Processor {
    Processor::new(|| Box::new(PriceTimePriority), None)
}

fn rest(p: &mut Processor, id: u64, instrument: u32, side: Side, price: u64, qty: u64) {
    p.process(
        Command::New(Order::limit(OrderId(id), side, price, qty).on(InstrumentId(instrument))),
        &mut |_| {},
    );
}

/// An instrument in category `c` (categories are `(instrument-1)/CAT_SIZE`).
fn instrument_in_category(c: u32, nth: u32) -> u32 {
    let base = c * CAT_SIZE + 1; // first instrument of the category
    let instrument = base + nth;
    assert_eq!(asset_category(InstrumentId(instrument), CAT_SIZE), c);
    instrument
}

#[test]
fn same_category_commands_produce_equal_fingerprints() {
    let cat = 3;
    let inst = instrument_in_category(cat, 7);

    let mut a = processor();
    let mut b = processor();
    for p in [&mut a, &mut b] {
        rest(p, 1, inst, Side::Buy, 100, 5);
        rest(p, 2, inst, Side::Sell, 105, 4);
    }

    assert_eq!(
        a.category_fingerprint(cat, CAT_SIZE),
        b.category_fingerprint(cat, CAT_SIZE),
        "identical resting state for the category must fingerprint identically"
    );
    // Sanity: a non-trivial book does not fingerprint to the empty seed.
    assert_ne!(
        a.category_fingerprint(cat, CAT_SIZE),
        processor().category_fingerprint(cat, CAT_SIZE),
        "a populated category must differ from an empty one"
    );
}

#[test]
fn a_divergent_book_changes_the_category_fingerprint() {
    let cat = 1;
    let inst = instrument_in_category(cat, 0);

    let mut a = processor();
    let mut b = processor();
    rest(&mut a, 1, inst, Side::Buy, 100, 5);
    rest(&mut b, 1, inst, Side::Buy, 100, 6); // different quantity

    assert_ne!(
        a.category_fingerprint(cat, CAT_SIZE),
        b.category_fingerprint(cat, CAT_SIZE),
        "a different resting quantity must change the fingerprint"
    );
}

#[test]
fn other_categories_do_not_affect_a_categorys_fingerprint() {
    let target_cat = 2;
    let target_inst = instrument_in_category(target_cat, 3);
    let other_inst = instrument_in_category(9, 3);

    // `base` rests only the target category; `noisy` additionally rests a
    // large book in an unrelated category. The target category's fingerprint
    // must be unchanged by the unrelated activity.
    let mut base = processor();
    let mut noisy = processor();
    for p in [&mut base, &mut noisy] {
        rest(p, 1, target_inst, Side::Buy, 100, 5);
        rest(p, 2, target_inst, Side::Sell, 110, 3);
    }
    for id in 10..30 {
        rest(&mut noisy, id, other_inst, Side::Buy, 90 + id, id);
    }

    assert_eq!(
        base.category_fingerprint(target_cat, CAT_SIZE),
        noisy.category_fingerprint(target_cat, CAT_SIZE),
        "unrelated categories' books must not leak into a category's fingerprint"
    );
    // But they DO change the whole-shard fingerprint, proving the filter is
    // actually narrowing scope (not a no-op).
    assert_ne!(
        base.state_fingerprint(),
        noisy.state_fingerprint(),
        "the whole-state fingerprint still reflects every category"
    );
}

#[test]
fn empty_category_fingerprint_is_the_bare_seed_and_stable() {
    let a = processor();
    let b = processor();
    // No orders anywhere: every category is empty and equal across processors.
    assert_eq!(
        a.category_fingerprint(42, CAT_SIZE),
        b.category_fingerprint(42, CAT_SIZE)
    );
}
