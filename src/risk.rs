//! Risk controls: synchronous pre-trade checks and asynchronous monitors.
//!
//! Two kinds of strategy, matching the two places risk can act:
//!
//! * **Synchronous** ([`SyncRiskCheck`]): runs inline on the matching path,
//!   before an order reaches the book. Must be O(1)-ish. The built-in
//!   [`PriceGuard`] (anti price-spike banding) is one.
//! * **Asynchronous** ([`AsyncRiskStrategy`]): runs on its own thread/schedule,
//!   observing positions/prices, and emits commands — typically
//!   [`Command::ForceClose`] — that are funnelled into the gateway thread and
//!   enter the matching shard through the **high-priority queue** (ahead of all
//!   queued new orders).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::exchange::Command;
use crate::order::Order;
use crate::types::*;

/// A synchronous, inline pre-trade check. Return `Err(reason)` to reject.
pub trait SyncRiskCheck: Send + Sync {
    fn check(&self, order: &Order) -> Result<(), &'static str>;
}

/// Anti-spike price banding driven by an **external reference price** (e.g. an
/// index price aggregated from other venues).
///
/// * Limit orders priced *through* the band on the aggressive side are rejected
///   (a buy above `ref * (1 + band)`, a sell below `ref * (1 - band)`): these
///   are the orders that would print artificial spikes ("插针").
/// * Market orders are converted into **protected marketable limits** capped at
///   the band edge (IOC), so a market order can never sweep a thin book beyond
///   the band.
/// * Instruments with no reference price (feed down / not registered) are not
///   restricted — fail-open is a policy choice; flip to fail-closed by
///   registering a 0-tolerance default.
///
/// Reference prices live in pre-registered `AtomicU64`s so the feed thread
/// updates them lock-free while shards read them lock-free.
pub struct PriceGuard {
    band_bps: u64,
    refs: HashMap<InstrumentId, AtomicU64>,
}

impl PriceGuard {
    /// `band_bps` is the half-width of the allowed band in basis points
    /// (e.g. 500 = ±5%). `instruments` pre-registers the reference slots.
    pub fn new(band_bps: u64, instruments: &[InstrumentId]) -> Self {
        PriceGuard {
            band_bps,
            refs: instruments
                .iter()
                .map(|&i| (i, AtomicU64::new(0)))
                .collect(),
        }
    }

    /// Update an instrument's reference price (called by the external feed).
    /// Unregistered instruments are ignored.
    pub fn set_reference(&self, instrument: InstrumentId, price: Price) {
        if let Some(slot) = self.refs.get(&instrument) {
            slot.store(price, Ordering::Release);
        }
    }

    /// Current reference price (0 = unknown).
    pub fn reference(&self, instrument: InstrumentId) -> Price {
        self.refs
            .get(&instrument)
            .map(|s| s.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn band(&self, reference: Price) -> (Price, Price) {
        let delta = reference.saturating_mul(self.band_bps) / 10_000;
        (
            reference.saturating_sub(delta),
            reference.saturating_add(delta),
        )
    }

    /// Vet (and possibly adjust) an order before matching.
    ///
    /// Returns `Err` to reject; may mutate a market order into a protected
    /// marketable limit.
    pub fn vet(&self, order: &mut Order) -> Result<(), &'static str> {
        // Budget orders: `price` is a TOTAL budget, not a per-unit price — the
        // band check does not apply (they only lift existing in-band liquidity).
        if matches!(order.tif, TimeInForce::IocBudget | TimeInForce::FokBudget) {
            return Ok(());
        }
        let reference = self.reference(order.instrument);
        if reference == 0 {
            return Ok(()); // no feed: fail-open (see type-level docs)
        }
        let (lo, hi) = self.band(reference);
        match order.order_type {
            OrderType::Limit => match order.side {
                Side::Buy if order.price > hi => Err("price-band"),
                Side::Sell if order.price < lo => Err("price-band"),
                _ => Ok(()),
            },
            OrderType::Market => {
                // Protected market order: cap at the band edge, never rest.
                order.order_type = OrderType::Limit;
                order.price = match order.side {
                    Side::Buy => hi,
                    Side::Sell => lo,
                };
                if order.tif == TimeInForce::Gtc {
                    order.tif = TimeInForce::Ioc;
                }
                Ok(())
            }
        }
    }
}

impl SyncRiskCheck for PriceGuard {
    fn check(&self, order: &Order) -> Result<(), &'static str> {
        // Immutable variant for use as a plain check (no market conversion).
        let mut probe = *order;
        self.vet(&mut probe)
    }
}

/// Static pre-trade limits, enforced synchronously before an order reaches the
/// book. A field of `0` means "unlimited".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RiskLimits {
    /// Maximum quantity per order.
    pub max_order_qty: Qty,
    /// Maximum notional (`price * qty`) per limit order. Market orders are not
    /// notional-checked here (no price yet); the price guard caps their reach.
    pub max_notional: u128,
    /// Maximum simultaneously resting orders per user (user 0 exempt).
    pub max_user_orders: u32,
}

impl RiskLimits {
    /// Order-shape checks (no book state needed). `Err(reason)` rejects.
    pub fn check_static(&self, order: &Order) -> Result<(), &'static str> {
        if self.max_order_qty > 0 && order.quantity > self.max_order_qty {
            return Err("max-qty");
        }
        if self.max_notional > 0
            && order.order_type == OrderType::Limit
            && (order.price as u128) * (order.quantity as u128) > self.max_notional
        {
            return Err("max-notional");
        }
        Ok(())
    }
}

/// An asynchronous risk strategy: evaluated periodically off the matching path,
/// emitting commands (typically force-closes) to inject via the gateway.
pub trait AsyncRiskStrategy: Send {
    fn evaluate(&mut self) -> Vec<Command>;
}

/// A minimal margin monitor: tracks per-user positions marked against the
/// reference price, and force-closes users whose loss exceeds their margin.
///
/// Positions/margins come from the order system; this demonstrates the shape of
/// an async strategy and produces [`Command::ForceClose`] with a caller-supplied
/// id allocator (so ids stay unique system-wide, e.g. a [`crate::idgen`] source).
pub struct MarginMonitor<F: FnMut() -> OrderId + Send> {
    pub instrument: InstrumentId,
    /// (user, signed position qty, entry price, margin) — positive = long.
    pub accounts: Vec<(u64, i64, Price, u64)>,
    pub mark_price: Price,
    pub next_order_id: F,
}

impl<F: FnMut() -> OrderId + Send> AsyncRiskStrategy for MarginMonitor<F> {
    fn evaluate(&mut self) -> Vec<Command> {
        let mut out = Vec::new();
        for &(user, pos, entry, margin) in &self.accounts {
            if pos == 0 {
                continue;
            }
            // Unrealised PnL in ticks*lots; loss when negative.
            let pnl = (self.mark_price as i128 - entry as i128) * pos as i128;
            if pnl < -(margin as i128) {
                out.push(Command::ForceClose {
                    instrument: self.instrument,
                    user,
                    close_order_id: (self.next_order_id)(),
                    close_side: if pos > 0 { Side::Sell } else { Side::Buy },
                    close_qty: pos.unsigned_abs(),
                });
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn margin_monitor_flags_underwater_longs() {
        let mut next = 100u64;
        let mut mon = MarginMonitor {
            instrument: InstrumentId(1),
            accounts: vec![
                (7, 10, 1000, 500), // long 10 @1000, margin 500
                (8, -5, 1000, 500), // short 5 @1000, margin 500
            ],
            mark_price: 900, // longs lose 100*10=1000 > 500; shorts gain
            next_order_id: move || {
                next += 1;
                OrderId(next)
            },
        };
        let cmds = mon.evaluate();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::ForceClose {
                user,
                close_side,
                close_qty,
                ..
            } => {
                assert_eq!(*user, 7);
                assert_eq!(*close_side, Side::Sell);
                assert_eq!(*close_qty, 10);
            }
            other => panic!("expected ForceClose, got {other:?}"),
        }
    }
}
