//! Pluggable price-matching strategies.
//!
//! # The core abstraction
//!
//! Crossing the spread — walking best price to worse — is common to every venue.
//! What actually distinguishes matching models is a *single, local* decision:
//!
//! > When an aggressor's quantity meets the resting orders sitting **at one price
//! > level**, how is that quantity distributed among those resting orders?
//!
//! [`MatchingStrategy::allocate`] captures exactly that decision, and nothing else.
//! The engine handles price/time bookkeeping, TIF, resting and cancels; a strategy
//! only answers "who at this level gets filled, and by how much?".
//!
//! This keeps each strategy tiny, independently testable, and swappable at runtime
//! (`Box<dyn MatchingStrategy>`), which is what "supports multiple matching
//! strategies" means in practice: one engine, one order book, configurable model.

mod price_size;
mod price_time;
mod pro_rata;

pub use price_size::SizePriority;
pub use price_time::PriceTimePriority;
pub use pro_rata::ProRata;

use crate::types::*;

/// A read-only view of one resting order, as presented to a strategy.
///
/// Strategies never mutate the book; they only decide allocations. The slice
/// handed to [`MatchingStrategy::allocate`] is always in **time-priority order**
/// (oldest resting order first), so a strategy that wants time as a tiebreak can
/// rely on position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestingOrder {
    pub id: OrderId,
    pub remaining: Qty,
    pub timestamp: Timestamp,
    /// Owning user (0 = unattributed) — used for self-trade prevention.
    pub user: u64,
}

/// A strategy's decision to fill `qty` from resting maker order `id`.
///
/// `idx` is the order's position in the `resting` slice the strategy was
/// handed. It lets the engine resolve the maker in O(1) instead of scanning
/// the level view per allocation (which is quadratic when a deep level is
/// swept). The contract check in [`MatchingStrategy::validate`] enforces that
/// `resting[idx].id == id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Allocation {
    pub id: OrderId,
    pub qty: Qty,
    pub idx: u32,
}

/// Defines how liquidity at a single price level is consumed.
///
/// # Contract
///
/// Given `resting` (time-ordered, all at the same price) and the aggressor's
/// `incoming` quantity, an implementation must return allocations such that:
///
/// * every `qty` is `> 0`;
/// * no allocation exceeds that order's `remaining`;
/// * the sum of allocated quantities equals `min(incoming, total_resting)`.
///
/// The default [`MatchingStrategy::validate`] checks these invariants and is used
/// by the test suite; violating the contract is a bug in the strategy, and the
/// engine's debug builds assert on it.
pub trait MatchingStrategy: Send + Sync {
    /// Human-readable strategy name, for logging and diagnostics.
    fn name(&self) -> &'static str;

    /// Allocate up to `incoming` lots among the `resting` orders at one level,
    /// appending to `out` (cleared by the caller). Allocation-free on the hot
    /// path: the engine reuses one output buffer across all crosses.
    fn allocate_into(&self, resting: &[RestingOrder], incoming: Qty, out: &mut Vec<Allocation>);

    /// Convenience wrapper returning a fresh `Vec` (tests, tooling).
    fn allocate(&self, resting: &[RestingOrder], incoming: Qty) -> Vec<Allocation> {
        let mut out = Vec::new();
        self.allocate_into(resting, incoming, &mut out);
        out
    }

    /// Whether the strategy needs to see the *entire* price level to allocate.
    /// Pro-rata and size-priority do (they weigh every resting order); FIFO
    /// does not — it only ever consumes the front, so the engine hands it a
    /// view capped at the aggressor's quantity. On deep levels (tens of
    /// thousands of orders) this is the difference between O(fill) and
    /// O(level) per cross.
    fn full_level_required(&self) -> bool {
        true
    }

    /// Verify the allocation contract for a given input/output. Returns `Err`
    /// with a description on the first violation.
    fn validate(
        &self,
        resting: &[RestingOrder],
        incoming: Qty,
        out: &[Allocation],
    ) -> Result<(), String> {
        let total: Qty = resting.iter().map(|r| r.remaining).sum();
        let expected = incoming.min(total);
        let mut allocated: Qty = 0;
        for a in out {
            if a.qty == 0 {
                return Err(format!("zero-qty allocation for {}", a.id));
            }
            match resting.get(a.idx as usize) {
                None => {
                    return Err(format!(
                        "allocation for {} has out-of-range idx {}",
                        a.id, a.idx
                    ));
                }
                Some(r) if r.id != a.id => {
                    return Err(format!(
                        "allocation idx {} points at order {}, not {}",
                        a.idx, r.id, a.id
                    ));
                }
                Some(r) if a.qty > r.remaining => {
                    return Err(format!(
                        "over-allocated {}: {} > {}",
                        a.id, a.qty, r.remaining
                    ));
                }
                _ => {}
            }
            allocated += a.qty;
        }
        if allocated != expected {
            return Err(format!(
                "allocated {allocated} but expected {expected} (incoming={incoming}, total={total})"
            ));
        }
        Ok(())
    }
}
