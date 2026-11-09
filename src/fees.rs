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
        // Saturate rather than wrap: `as u64` used to truncate a fee above
        // u64::MAX to its low bits (a tiny fee for a giant trade), and a
        // rate times a near-u128 notional could wrap the product itself.
        let fee = |bps: u64| (notional.saturating_mul(bps as u128) / 10_000).min(u64::MAX as u128) as u64;
        (fee(self.maker_bps), fee(self.taker_bps))
    }

    /// Rates above 100% are a configuration error, not a fee.
    pub fn validate(&self) -> Result<(), String> {
        if self.maker_bps > 10_000 || self.taker_bps > 10_000 {
            return Err(format!(
                "fee rates must be at most 10000 bps (100%): maker {} taker {}",
                self.maker_bps, self.taker_bps
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fees_floor_deterministically() {
        // 10 bps maker, 20 bps taker on notional 1000*3 = 3000.
        let f = FeeSchedule {
            maker_bps: 10,
            taker_bps: 20,
        };
        assert_eq!(f.fees(1000, 3), (3, 6));
        // Flooring: notional 999 * 1 bps = 0.0999 -> 0.
        let f = FeeSchedule {
            maker_bps: 1,
            taker_bps: 1,
        };
        assert_eq!(f.fees(999, 1), (0, 0));
        // Zero schedule = free trading (default).
        assert_eq!(FeeSchedule::default().fees(1000, 50), (0, 0));
    }

    #[test]
    fn a_fee_beyond_u64_saturates_instead_of_wrapping() {
        let f = FeeSchedule { maker_bps: 10_000, taker_bps: 10_000 };
        assert_eq!(f.fees(u64::MAX, u64::MAX), (u64::MAX, u64::MAX));
        assert!(FeeSchedule { maker_bps: 10_001, taker_bps: 0 }.validate().is_err());
        assert!(f.validate().is_ok());
    }
}
