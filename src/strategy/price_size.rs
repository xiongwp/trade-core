//! Size-priority matching — largest resting order first.

use std::cell::RefCell;
use std::collections::BinaryHeap;

use super::{Allocation, MatchingStrategy, RestingOrder};
use crate::types::{Qty, Timestamp};

/// **Size priority**: at a price level, the *largest* resting order is filled
/// first; ties are broken by time (earlier first). This rewards posting size and
/// is seen in some dealer/market-maker driven venues.
///
/// # Cost
///
/// The previous implementation sorted the whole level per cross — O(n log n)
/// even for a 1-lot aggressor against a 100k-order level. This version heapifies
/// once (O(n)) and pops only until the aggressor is satisfied (O(k log n) for k
/// fills). The pop sequence follows the exact comparator the sort used
/// (remaining desc, timestamp asc, slice position asc), so allocations — and the
/// replay fingerprint — are unchanged. The heap's backing storage is
/// thread-local and reused across crosses: no hot-path allocation after warm-up.
#[derive(Clone, Copy, Debug, Default)]
pub struct SizePriority;

/// Heap entry ordered so that `BinaryHeap` (a max-heap) pops the level's
/// resting orders largest-first, ties to the earlier timestamp, then the
/// earlier slice position — a strict total order.
#[derive(Clone, Copy, PartialEq, Eq)]
struct BySize {
    remaining: Qty,
    timestamp: Timestamp,
    idx: u32,
}

impl Ord for BySize {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.remaining
            .cmp(&other.remaining)
            .then_with(|| other.timestamp.cmp(&self.timestamp))
            .then_with(|| other.idx.cmp(&self.idx))
    }
}

impl PartialOrd for BySize {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

thread_local! {
    /// Backing storage for the per-cross heap, reused to avoid allocation.
    static SCRATCH: RefCell<Vec<BySize>> = const { RefCell::new(Vec::new()) };
}

impl MatchingStrategy for SizePriority {
    fn name(&self) -> &'static str {
        "size-priority"
    }

    fn allocate_into(&self, resting: &[RestingOrder], incoming: Qty, out: &mut Vec<Allocation>) {
        if incoming == 0 {
            return;
        }
        // Single resting order: nothing to prioritise.
        if let [r] = resting {
            let fill = incoming.min(r.remaining);
            if fill > 0 {
                out.push(Allocation {
                    id: r.id,
                    qty: fill,
                    idx: 0,
                });
            }
            return;
        }

        SCRATCH.with(|scratch| {
            let mut storage = std::mem::take(&mut *scratch.borrow_mut());
            storage.clear();
            storage.extend(resting.iter().enumerate().map(|(i, r)| BySize {
                remaining: r.remaining,
                timestamp: r.timestamp,
                idx: i as u32,
            }));

            // O(n) heapify, then pop only as many entries as the aggressor
            // actually consumes.
            let mut heap = BinaryHeap::from(storage);
            let mut remaining = incoming;
            while remaining > 0 {
                let Some(top) = heap.pop() else { break };
                let fill = remaining.min(top.remaining);
                if fill > 0 {
                    out.push(Allocation {
                        id: resting[top.idx as usize].id,
                        qty: fill,
                        idx: top.idx,
                    });
                    remaining -= fill;
                }
            }

            // Hand the heap's buffer back for reuse.
            *scratch.borrow_mut() = heap.into_vec();
        });
    }
}
