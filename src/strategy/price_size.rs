//! Size-priority matching — largest resting order first.

use super::{Allocation, MatchingStrategy, RestingOrder};
use crate::types::Qty;

/// **Size priority**: at a price level, the *largest* resting order is filled
/// first; ties are broken by time (earlier first). This rewards posting size and
/// is seen in some dealer/market-maker driven venues.
///
/// The resting slice is not mutated; we sort a lightweight index of references.
#[derive(Clone, Copy, Debug, Default)]
pub struct SizePriority;

impl MatchingStrategy for SizePriority {
    fn name(&self) -> &'static str {
        "size-priority"
    }

    fn allocate_into(&self, resting: &[RestingOrder], incoming: Qty, out: &mut Vec<Allocation>) {
        // Order indices by (remaining desc, timestamp asc).
        let mut idx: Vec<usize> = (0..resting.len()).collect();
        idx.sort_by(|&a, &b| {
            resting[b]
                .remaining
                .cmp(&resting[a].remaining)
                .then(resting[a].timestamp.cmp(&resting[b].timestamp))
        });

        let mut remaining = incoming;
        for &i in &idx {
            if remaining == 0 {
                break;
            }
            let r = &resting[i];
            let fill = remaining.min(r.remaining);
            if fill > 0 {
                out.push(Allocation { id: r.id, qty: fill });
                remaining -= fill;
            }
        }
    }
}
