//! The matching engine: order intake, crossing, TIF handling and reporting.

use crate::book::OrderBook;
use crate::order::Order;
use crate::strategy::MatchingStrategy;
use crate::trade::{ModifyOutcome, OrderStatus, SubmitReport, Trade};
use crate::types::*;

/// A single-instrument continuous matching engine.
///
/// The engine owns an [`OrderBook`] and a boxed [`MatchingStrategy`]. Crossing
/// logic (best-price-first, limit checks, TIF) lives here and is identical for
/// every strategy; the strategy only decides intra-level allocation.
/// What to do when an incoming order would trade against a resting order of
/// the **same user** (self-trade prevention, STP).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SelfTradePolicy {
    /// No prevention (default; user 0 = unattributed is never matched as self).
    #[default]
    Allow,
    /// Cancel the incoming (taker) remainder; resting orders stay.
    CancelTaker,
    /// Cancel the user's resting (maker) orders at the touched level and keep
    /// matching the incoming order against others.
    CancelMaker,
    /// Cancel both the resting self orders and the incoming remainder.
    CancelBoth,
}

pub struct MatchingEngine {
    book: OrderBook,
    strategy: Box<dyn MatchingStrategy>,
    seq: Timestamp,
    stp: SelfTradePolicy,
    // Reusable scratch buffers: the crossing loop allocates nothing.
    view_buf: Vec<crate::strategy::RestingOrder>,
    alloc_buf: Vec<crate::strategy::Allocation>,
    fills_buf: Vec<(OrderId, Qty)>,
    /// Maker orders cancelled by STP during the last submit (for reporting).
    stp_cancelled: Vec<OrderId>,
}

impl MatchingEngine {
    /// Build an engine with the given matching strategy and a small default
    /// order pool (grows on demand). Use [`MatchingEngine::with_pool`] to
    /// pre-reserve production-sized memory.
    pub fn new(strategy: Box<dyn MatchingStrategy>) -> Self {
        Self::with_pool(strategy, 1024, false)
    }

    /// Build an engine whose book pre-reserves `max_orders` pool slots (and
    /// pre-faults the pages when `prefault` is set) — in-memory matching with an
    /// up-front memory budget.
    pub fn with_pool(
        strategy: Box<dyn MatchingStrategy>,
        max_orders: usize,
        prefault: bool,
    ) -> Self {
        MatchingEngine {
            book: OrderBook::with_capacity(max_orders, prefault),
            strategy,
            seq: 0,
            stp: SelfTradePolicy::Allow,
            view_buf: Vec::new(),
            alloc_buf: Vec::new(),
            fills_buf: Vec::new(),
            stp_cancelled: Vec::new(),
        }
    }

    /// Set the self-trade prevention policy (builder style).
    pub fn with_stp(mut self, stp: SelfTradePolicy) -> Self {
        self.stp = stp;
        self
    }

    /// Maker orders cancelled by STP during the most recent `submit`/
    /// `submit_into` call (drain for reporting).
    pub fn stp_cancelled(&self) -> &[OrderId] {
        &self.stp_cancelled
    }

    /// The engine's monotonic sequence counter (time-priority stamp source).
    pub fn seq(&self) -> Timestamp {
        self.seq
    }

    /// Export every resting order, sorted by time priority — with [`seq`](Self::seq)
    /// this is the engine's complete state (used by snapshots).
    pub fn export_orders(&self) -> Vec<Order> {
        let mut orders: Vec<Order> = self.book.iter_orders().copied().collect();
        orders.sort_by_key(|o| o.timestamp);
        orders
    }

    /// Restore state exported by [`export_orders`](Self::export_orders): sets the
    /// sequence counter and re-inserts the orders (which must be sorted by
    /// timestamp) without matching. Only valid on a fresh engine.
    pub fn restore(&mut self, seq: Timestamp, orders: &[Order]) {
        debug_assert!(self.book.is_empty(), "restore() requires a fresh engine");
        self.seq = seq;
        for o in orders {
            self.book.insert(*o);
        }
    }

    /// Convenience constructor for a concrete strategy value.
    pub fn with_strategy<S: MatchingStrategy + 'static>(strategy: S) -> Self {
        Self::new(Box::new(strategy))
    }

    /// Cancel every resting order owned by `user` (forced-liquidation support).
    /// Returns the cancelled order ids.
    pub fn cancel_all_for_user(&mut self, user: u64) -> Vec<OrderId> {
        let ids = self.book.orders_of_user(user);
        for id in &ids {
            self.book.cancel(*id);
        }
        ids
    }

    /// The active strategy's name.
    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }

    /// Read-only access to the order book.
    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    /// Cancel a resting order. Returns `true` if an order was removed.
    pub fn cancel(&mut self, id: OrderId) -> bool {
        self.book.cancel(id).is_some()
    }

    /// Amend a resting order.
    ///
    /// Exchange amend semantics for queue priority:
    /// * **Reducing** quantity at the **same price** keeps the order's place in
    ///   line (reduced in place).
    /// * A **price change** or a **quantity increase** forfeits priority: the
    ///   order is cancelled and re-entered at the back of the queue, and may
    ///   cross the book on re-entry.
    pub fn modify(&mut self, id: OrderId, new_price: Price, new_qty: Qty) -> ModifyOutcome {
        let existing = match self.book.get(id) {
            Some(o) => o,
            None => return ModifyOutcome::NotFound,
        };

        // Fast path: same price, not increasing size -> reduce in place.
        if new_price == existing.price && new_qty <= existing.remaining {
            if new_qty == 0 {
                self.book.cancel(id);
                return ModifyOutcome::Cancelled { order_id: id };
            }
            self.book.reduce(id, new_qty);
            return ModifyOutcome::Reduced {
                order_id: id,
                remaining: new_qty,
            };
        }

        // Slow path: price change or size increase -> re-enter (loses priority).
        let mut order = self.book.cancel(id).expect("located above");
        order.price = new_price;
        order.quantity = new_qty;
        order.remaining = new_qty;
        order.tif = TimeInForce::Gtc;
        ModifyOutcome::Requoted(self.submit(order))
    }

    /// Submit an order and match it against the book, returning an owned report.
    pub fn submit(&mut self, order: Order) -> SubmitReport {
        let mut trades = Vec::new();
        let (order_id, status, filled, resting) = self.submit_into(order, &mut trades);
        SubmitReport {
            order_id,
            status,
            filled,
            trades,
            resting,
        }
    }

    /// Allocation-free submit: trades are **appended** to the caller's reusable
    /// buffer instead of a fresh `Vec`. This is the hot path used by the
    /// exchange shards. Returns `(order_id, status, filled, resting)`.
    pub fn submit_into(
        &mut self,
        mut order: Order,
        trades: &mut Vec<Trade>,
    ) -> (OrderId, OrderStatus, Qty, bool) {
        self.seq += 1;
        order.timestamp = self.seq;
        order.remaining = order.quantity;
        self.stp_cancelled.clear();

        // Budget orders: `price` carries the TOTAL notional budget.
        let mut budget_left: u128 = u128::MAX;
        if matches!(order.tif, TimeInForce::IocBudget | TimeInForce::FokBudget) {
            let budget = order.price;
            match order.side {
                // Sells: a linear proceeds floor is exactly a limit at
                // ceil(budget/qty) — convert (see TimeInForce docs).
                Side::Sell => {
                    let q = order.quantity.max(1);
                    order.price = (budget + q - 1) / q; // ceil (MSRV 1.70)
                    order.tif = if order.tif == TimeInForce::FokBudget {
                        TimeInForce::Fok
                    } else {
                        TimeInForce::Ioc
                    };
                }
                // Buys: genuinely notional-capped — cross at any price while
                // cumulative spend fits the budget.
                Side::Buy => {
                    budget_left = budget as u128;
                    order.price = Price::MAX;
                    if order.tif == TimeInForce::FokBudget {
                        let (fillable, cost) = self.book.cost_to_fill(Side::Buy, order.quantity);
                        if fillable < order.quantity || cost > budget_left {
                            return (order.id, OrderStatus::Rejected, 0, false);
                        }
                        order.tif = TimeInForce::IocBudget; // precheck passed: will fill fully
                    }
                }
            }
        }

        // Fill-or-kill: reject up front unless the whole quantity can fill now.
        if order.tif == TimeInForce::Fok
            && self.book.crossable_qty(order.side, order.price) < order.remaining
        {
            return (order.id, OrderStatus::Rejected, 0, false);
        }

        let taker_stopped = self.cross(&mut order, trades, &mut budget_left);
        if taker_stopped {
            // STP cancelled the incoming remainder: never rests, reports as
            // partially filled / cancelled depending on what already traded.
            let filled = order.quantity - order.remaining;
            let status = if filled > 0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Cancelled
            };
            return (order.id, status, filled, false);
        }
        self.finalize(order)
    }

    /// Walk the opposite side best-price-first, delegating intra-level allocation
    /// to the strategy, until the aggressor is exhausted or no crossable price
    /// remains. Mutates `order.remaining`; appends executions to `trades`.
    /// Returns `true` if STP cancelled the taker's remainder.
    fn cross(
        &mut self,
        order: &mut Order,
        trades: &mut Vec<Trade>,
        budget_left: &mut u128,
    ) -> bool {
        let taker = order.side;
        let maker_side = taker.opposite();

        while order.remaining > 0 {
            let px = match self.book.best(maker_side) {
                Some(p) => p,
                None => break,
            };

            // Limit orders only cross at their price or better.
            if order.order_type == OrderType::Limit {
                let crossable = match taker {
                    Side::Buy => order.price >= px,
                    Side::Sell => order.price <= px,
                };
                if !crossable {
                    break;
                }
            }

            // Budget cap: lots affordable at this level (u128 / px, saturated).
            let affordable =
                (*budget_left / (px.max(1) as u128)).min(order.remaining as u128) as Qty;
            if affordable == 0 {
                break; // budget exhausted
            }
            // FIFO-style strategies only consume the level's front: cap the
            // view at the aggressor's quantity so deep levels stay O(fill).
            if self.strategy.full_level_required() {
                self.book
                    .level_view_into(maker_side, px, &mut self.view_buf);
            } else {
                self.book
                    .level_view_capped(maker_side, px, affordable, &mut self.view_buf);
            }

            // Self-trade prevention: does this level hold the taker's own order?
            if self.stp != SelfTradePolicy::Allow && order.user != 0 {
                let self_resting = self.view_buf.iter().any(|r| r.user == order.user);
                if self_resting {
                    match self.stp {
                        SelfTradePolicy::Allow => unreachable!(),
                        SelfTradePolicy::CancelTaker => return true,
                        SelfTradePolicy::CancelMaker | SelfTradePolicy::CancelBoth => {
                            // Pull the user's own makers off this level, then
                            // re-read it (each pass removes >=1 order, so this
                            // terminates).
                            for i in 0..self.view_buf.len() {
                                let r = self.view_buf[i];
                                if r.user == order.user {
                                    self.book.cancel(r.id);
                                    self.stp_cancelled.push(r.id);
                                }
                            }
                            if self.stp == SelfTradePolicy::CancelBoth {
                                return true;
                            }
                            continue;
                        }
                    }
                }
            }

            // The level may have held only tombstones (cancelled orders): the
            // view reclaimed them and swept the level away. Re-read the best
            // price — progress is guaranteed because the level's bit is gone.
            if self.view_buf.is_empty() {
                continue;
            }

            self.alloc_buf.clear();
            self.strategy
                .allocate_into(&self.view_buf, affordable, &mut self.alloc_buf);
            debug_assert!(
                self.strategy
                    .validate(&self.view_buf, affordable, &self.alloc_buf)
                    .is_ok(),
                "strategy {} violated the allocation contract: {:?}",
                self.strategy.name(),
                self.strategy
                    .validate(&self.view_buf, affordable, &self.alloc_buf)
            );
            if self.alloc_buf.is_empty() {
                break;
            }

            self.fills_buf.clear();
            for a in &self.alloc_buf {
                if a.qty == 0 {
                    continue;
                }
                let maker_user = self
                    .view_buf
                    .iter()
                    .find(|r| r.id == a.id)
                    .map_or(0, |r| r.user);
                trades.push(Trade {
                    taker: order.id,
                    maker: a.id,
                    aggressor: taker,
                    price: px,
                    quantity: a.qty,
                    timestamp: self.seq,
                    maker_user,
                    taker_user: order.user,
                });
                order.remaining -= a.qty;
                *budget_left = budget_left.saturating_sub(px as u128 * a.qty as u128);
                self.fills_buf.push((a.id, a.qty));
            }

            if self.fills_buf.is_empty() {
                break;
            }
            let fills = std::mem::take(&mut self.fills_buf);
            self.book.apply_fills(maker_side, px, &fills);
            self.fills_buf = fills; // hand the buffer back for reuse
        }
        false
    }

    /// Decide the resting/cancel outcome for any remainder.
    fn finalize(&mut self, order: Order) -> (OrderId, OrderStatus, Qty, bool) {
        let filled = order.quantity - order.remaining;
        let id = order.id;

        let (status, resting) = if order.remaining == 0 {
            (OrderStatus::Filled, false)
        } else {
            match (order.order_type, order.tif) {
                // Market orders and IOC/FOK limits never rest.
                (OrderType::Market, _)
                | (OrderType::Limit, TimeInForce::Ioc)
                | (OrderType::Limit, TimeInForce::Fok)
                | (OrderType::Limit, TimeInForce::IocBudget)
                | (OrderType::Limit, TimeInForce::FokBudget) => {
                    let status = if filled > 0 {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Cancelled
                    };
                    (status, false)
                }
                // GTC limit remainder rests on the book.
                (OrderType::Limit, TimeInForce::Gtc) => {
                    self.book.insert(order);
                    let status = if filled > 0 {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Resting
                    };
                    (status, true)
                }
            }
        };

        (id, status, filled, resting)
    }
}
