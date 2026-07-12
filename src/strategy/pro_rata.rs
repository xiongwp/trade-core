//! Pro-rata matching — allocate proportionally to resting size.

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
#[derive(Clone, Copy, Debug, Default)]
pub struct ProRata;

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
            out.extend(resting.iter().map(|r| Allocation {
                id: r.id,
                qty: r.remaining,
            }));
            return;
        }

        // Floored proportional shares plus fractional remainders (u128 to avoid
        // overflow on `fillable * size`).
        let total128 = total as u128;
        let fillable128 = fillable as u128;
        let mut base: Vec<Qty> = Vec::with_capacity(resting.len());
        // (index, fractional remainder) for leftover distribution.
        let mut fracs: Vec<(usize, u128)> = Vec::with_capacity(resting.len());
        let mut assigned: Qty = 0;

        for (i, r) in resting.iter().enumerate() {
            let numer = fillable128 * r.remaining as u128;
            let share = (numer / total128) as Qty;
            let rem = numer % total128;
            base.push(share);
            fracs.push((i, rem));
            assigned += share;
        }

        // Leftover lots to hand out via largest remainder.
        let mut leftover = fillable - assigned;
        // Largest fractional remainder first; ties broken by time priority
        // (earlier timestamp, i.e. earlier position in the time-ordered slice).
        fracs.sort_by(|&(ia, fa), &(ib, fb)| {
            fb.cmp(&fa)
                .then(resting[ia].timestamp.cmp(&resting[ib].timestamp))
        });
        for &(i, _) in &fracs {
            if leftover == 0 {
                break;
            }
            base[i] += 1;
            leftover -= 1;
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
                }),
        );
    }
}
