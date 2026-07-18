//! Price-time priority (FIFO) — the most common matching model.

use super::{Allocation, MatchingStrategy, RestingOrder};
use crate::types::Qty;

/// **Price-time priority** ("FIFO"): at a price level, the order that arrived
/// first is filled first. This rewards being early and is the model used by most
/// cash equity and futures markets.
///
/// Because the resting slice is already time-ordered, allocation is a simple
/// left-to-right sweep.
#[derive(Clone, Copy, Debug, Default)]
pub struct PriceTimePriority;

impl MatchingStrategy for PriceTimePriority {
    fn name(&self) -> &'static str {
        "price-time"
    }

    /// FIFO only consumes the front of a level: a capped view suffices.
    fn full_level_required(&self) -> bool {
        false
    }

    fn allocate_into(&self, resting: &[RestingOrder], incoming: Qty, out: &mut Vec<Allocation>) {
        let mut remaining = incoming;
        for (i, r) in resting.iter().enumerate() {
            if remaining == 0 {
                break;
            }
            let fill = remaining.min(r.remaining);
            if fill > 0 {
                out.push(Allocation {
                    id: r.id,
                    qty: fill,
                    idx: i as u32,
                });
                remaining -= fill;
            }
        }
    }
}
