//! Maker/taker fee schedule, computed inside the matching path so every trade
//! report carries authoritative fees — the account system settles from these
//! numbers, never recomputes them (one source of truth, like the journal).
//!
//! Fees are integer ticks, floored: `fee = notional * bps / 10_000` with
//! `notional = price * qty` in u128 (no overflow, no floats, deterministic —
//! replay reproduces identical fees).

use crate::types::{Price, Qty};

/// Basis-point fee rates. Maker adds liquidity (resting side), taker removes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeeSchedule {
    pub maker_bps: u64,
    pub taker_bps: u64,
}

impl FeeSchedule {
    /// Fees for one execution: `(maker_fee, taker_fee)` in ticks.
    #[inline]
    pub fn fees(&self, price: Price, qty: Qty) -> (u64, u64) {
        let notional = price as u128 * qty as u128;
        (
            (notional * self.maker_bps as u128 / 10_000) as u64,
            (notional * self.taker_bps as u128 / 10_000) as u64,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fees_floor_deterministically() {
        // 10 bps maker, 20 bps taker on notional 1000*3 = 3000.
        let f = FeeSchedule { maker_bps: 10, taker_bps: 20 };
        assert_eq!(f.fees(1000, 3), (3, 6));
        // Flooring: notional 999 * 1 bps = 0.0999 -> 0.
        let f = FeeSchedule { maker_bps: 1, taker_bps: 1 };
        assert_eq!(f.fees(999, 1), (0, 0));
        // Zero schedule = free trading (default).
        assert_eq!(FeeSchedule::default().fees(1000, 50), (0, 0));
    }
}
