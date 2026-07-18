//! Pro-rata matching — allocate proportionally to resting size.

use std::cell::RefCell;

use super::{Allocation, MatchingStrategy, RestingOrder};
use crate::types::Qty;

/// **Pro-rata**: at a price level, the aggressor's quantity is split among all
/// resting orders in proportion to their size. Used by several short-term
/// interest-rate futures, where it discourages the "race to the front of the
/// queue" that pure FIFO encourages.
///
/// # Rounding
///
/// A proportional split rarely lands on whole lots, yet we must allocate an exact
/// integer total. We use the **largest-remainder method**:
///
/// 1. Give each order its floored proportional share
///    `floor(fillable * sizeᵢ / total)`.
/// 2. Distribute the leftover lots one at a time to the orders with the largest
///    fractional remainder, breaking ties by time priority (earlier first).
///
/// This is deterministic, allocates the exact total, and never over-fills an
/// order (a floored share of `fillable < total` is always `< sizeᵢ`, so a single
/// +1 can never exceed `sizeᵢ`).
///
/// # Cost
///
/// O(level) per cross. Selecting the leftover recipients uses
/// `select_nth_unstable_by` (expected O(n)) rather than a full sort: the chosen
/// *set* is identical to the sorted prefix because the comparator is a strict
/// total order (remainder desc, then timestamp asc, then slice position asc —
/// exactly the order the previous stable sort produced), so the allocation —
/// and therefore the replay fingerprint — is unchanged. Scratch buffers are
/// thread-local: the hot path allocates nothing after warm-up.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProRata;

thread_local! {
    /// (floored shares, (idx, fractional remainder)) reused across crosses.
    static SCRATCH: RefCell<(Vec<Qty>, Vec<(u32, u128)>)> =
        const { RefCell::new((Vec::new(), Vec::new())) };
}

impl MatchingStrategy for ProRata {
    fn name(&self) -> &'static str {
        "pro-rata"
    }

    fn allocate_into(&self, resting: &[RestingOrder], incoming: Qty, out: &mut Vec<Allocation>) {
        let total: Qty = resting.iter().map(|r| r.remaining).sum();
        let fillable = incoming.min(total);
        if fillable == 0 {
            return;
        }
        // Aggressor can clear the whole level: everyone fills fully.
        if fillable == total {
            out.extend(resting.iter().enumerate().map(|(i, r)| Allocation {
                id: r.id,
                qty: r.remaining,
                idx: i as u32,
            }));
            return;
        }
        // Single resting order: the proportional share is just the fillable.
        if let [r] = resting {
            out.push(Allocation {
                id: r.id,
                qty: fillable,
                idx: 0,
            });
            return;
        }

        SCRATCH.with(|scratch| {
            let (base, fracs) = &mut *scratch.borrow_mut();
            base.clear();
            fracs.clear();

            // Floored proportional shares plus fractional remainders (u128 to
            // avoid overflow on `fillable * size`).
            let total128 = total as u128;
            let fillable128 = fillable as u128;
            let mut assigned: Qty = 0;

            for (i, r) in resting.iter().enumerate() {
                let numer = fillable128 * r.remaining as u128;
                let share = (numer / total128) as Qty;
                let rem = numer % total128;
                base.push(share);
                fracs.push((i as u32, rem));
                assigned += share;
            }

            // Leftover lots go to the largest fractional remainders; ties by
            // time priority (earlier timestamp), then slice position for a
            // strict total order (mirrors the stable sort this replaces).
            let leftover = (fillable - assigned) as usize;
            debug_assert!(leftover < fracs.len(), "largest-remainder leftover bound");
            if leftover > 0 && leftover < fracs.len() {
                let by_remainder =
                    |&(ia, fa): &(u32, u128), &(ib, fb): &(u32, u128)| -> std::cmp::Ordering {
                        fb.cmp(&fa)
                            .then_with(|| {
                                resting[ia as usize]
                                    .timestamp
                                    .cmp(&resting[ib as usize].timestamp)
                            })
                            .then(ia.cmp(&ib))
                    };
                fracs.select_nth_unstable_by(leftover - 1, by_remainder);
                for &(i, _) in &fracs[..leftover] {
                    base[i as usize] += 1;
                }
            }

            // Emit in time order for a stable, auditable trade tape.
            out.extend(
                resting
                    .iter()
                    .enumerate()
                    .filter(|&(i, _)| base[i] > 0)
                    .map(|(i, r)| Allocation {
                        id: r.id,
                        qty: base[i],
                        idx: i as u32,
                    }),
            );
        });
    }
}
