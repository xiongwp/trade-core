//! Core value types shared across the matching engine.
//!
//! Prices and quantities are represented as **integers**. Matching engines must
//! never use floating point for price/size arithmetic: rounding error would make
//! fills non-deterministic and un-auditable. Prices are expressed in *ticks*
//! (the smallest tradable price increment) and quantities in *lots* (the smallest
//! tradable size increment). Callers are responsible for the scaling convention
//! (e.g. price tick = 0.01 => a price of 100 means 1.00).

use std::fmt;

/// The side of an order.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Side {
    /// A bid: an intent to buy.
    Buy,
    /// An offer/ask: an intent to sell.
    Sell,
}

impl Side {
    /// The opposite (resting) side that an aggressor of this side matches against.
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => f.write_str("BUY"),
            Side::Sell => f.write_str("SELL"),
        }
    }
}

/// A globally unique order identifier. A newtype so it can never be confused
/// with a price or quantity in a function signature.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct OrderId(pub u64);

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Identifies a tradable instrument (asset/symbol). The exchange routes each
/// instrument to a matching shard, so a single engine deployment matches many
/// assets concurrently and independently.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct InstrumentId(pub u32);

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sym:{}", self.0)
    }
}

/// A price expressed in integer ticks.
pub type Price = u64;

/// A quantity expressed in integer lots.
pub type Qty = u64;

/// A monotonically increasing sequence number assigned by the engine on accept.
/// Used as the time-priority key so ordering is deterministic and independent of
/// wall-clock resolution.
pub type Timestamp = u64;

/// Whether an order rests on the book at a limit price or sweeps the book.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderType {
    /// Executes only at the stated limit price or better; any remainder may rest.
    Limit,
    /// Executes against the best available prices; never rests on the book.
    Market,
}

/// How long an order remains active.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TimeInForce {
    /// Good-till-cancel: any unfilled remainder rests on the book.
    Gtc,
    /// Immediate-or-cancel: fill what is possible now, cancel the remainder.
    Ioc,
    /// Fill-or-kill: fill the entire quantity immediately, or reject the whole order.
    Fok,
}
