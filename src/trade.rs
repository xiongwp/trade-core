//! Execution results produced by the engine.

use crate::types::*;

/// A single execution between an aggressing (taker) order and a resting (maker) order.
///
/// The trade always prints at the **maker's** resting price — the aggressor
/// receives price improvement, which is the standard convention on continuous
/// limit-order-book venues.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Trade {
    /// The aggressing order that removed liquidity.
    pub taker: OrderId,
    /// The resting order that provided liquidity.
    pub maker: OrderId,
    /// Side of the aggressor.
    pub aggressor: Side,
    /// Execution price in ticks (the maker's resting price).
    pub price: Price,
    /// Executed quantity in lots.
    pub quantity: Qty,
    /// Engine sequence number at which the trade occurred.
    pub timestamp: Timestamp,
}

/// The terminal (or resting) state of a submitted order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderStatus {
    /// Fully filled; nothing rests.
    Filled,
    /// Partially filled; remainder either rests or was cancelled per TIF.
    PartiallyFilled,
    /// Nothing filled; the whole order now rests on the book.
    Resting,
    /// Nothing rested and nothing (further) will fill (e.g. IOC/market remainder).
    Cancelled,
    /// Rejected before any execution (e.g. a FOK that could not fully fill).
    Rejected,
}

/// The outcome of submitting an order.
#[derive(Clone, Debug)]
pub struct SubmitReport {
    pub order_id: OrderId,
    pub status: OrderStatus,
    /// Total quantity filled by this submission.
    pub filled: Qty,
    /// The executions generated, in the order they occurred.
    pub trades: Vec<Trade>,
    /// Whether an unfilled remainder now rests on the book.
    pub resting: bool,
}

/// The outcome of a modify (amend) request.
#[derive(Clone, Debug)]
pub enum ModifyOutcome {
    /// No resting order with that id was found.
    NotFound,
    /// Quantity was reduced in place; **time priority is preserved**.
    Reduced { order_id: OrderId, remaining: Qty },
    /// The amend reduced quantity to zero, cancelling the order.
    Cancelled { order_id: OrderId },
    /// A price change or quantity increase re-entered the order at the back of
    /// the queue (**priority forfeited**); it may have crossed on re-entry.
    Requoted(SubmitReport),
}

impl SubmitReport {
    /// Volume-weighted average fill price, or `None` if nothing filled.
    pub fn avg_price(&self) -> Option<f64> {
        if self.filled == 0 {
            return None;
        }
        let notional: u128 = self
            .trades
            .iter()
            .map(|t| t.price as u128 * t.quantity as u128)
            .sum();
        Some(notional as f64 / self.filled as f64)
    }
}
