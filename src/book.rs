//! The limit order book: resting liquidity organised by price and time,
//! **backed by a pre-allocated slab pool**.
//!
//! # Memory model
//!
//! Orders are stored in a slab ([`OrderPool`]) — one contiguous allocation of
//! order slots reserved (and optionally pre-faulted) at construction time. Price
//! levels hold only `u32` slot indices. This gives:
//!
//! * **no per-order heap allocation** on the matching path (slots are recycled
//!   through a free list);
//! * **cache-friendly** iteration (orders are 64-byte-ish records in one array);
//! * a hard, up-front memory budget: e.g. a 3 GiB pool ≈ 50 million resting
//!   orders, reserved once at startup (see [`OrderPool::with_capacity`]).

use std::collections::{BTreeMap, HashMap};

use crate::order::Order;
use crate::strategy::RestingOrder;
use crate::types::*;

/// A slab of order slots with a free list. `alloc` reuses freed slots before
/// growing; with a sufficient initial capacity it never allocates after startup.
#[derive(Debug, Default)]
pub struct OrderPool {
    slots: Vec<Order>,
    free: Vec<u32>,
}

impl OrderPool {
    /// Reserve `capacity` order slots up front. When `prefault` is true the
    /// backing pages are touched immediately so first use never page-faults —
    /// this is what "allocate N GiB at startup" means in practice.
    pub fn with_capacity(capacity: usize, prefault: bool) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        if prefault {
            // Touch every page, then logically empty the slab (capacity kept).
            slots.resize(capacity, Order::limit(OrderId(0), Side::Buy, 0, 0));
            slots.clear();
        }
        OrderPool { slots, free: Vec::with_capacity(1024) }
    }

    /// Bytes one order slot occupies.
    pub fn slot_bytes() -> usize {
        std::mem::size_of::<Order>()
    }

    /// Slots currently reserved (grows only if the initial budget is exceeded).
    pub fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    /// Slots currently holding live orders.
    pub fn in_use(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    #[inline]
    fn alloc(&mut self, order: Order) -> u32 {
        match self.free.pop() {
            Some(i) => {
                self.slots[i as usize] = order;
                i
            }
            None => {
                self.slots.push(order);
                (self.slots.len() - 1) as u32
            }
        }
    }

    #[inline]
    fn release(&mut self, idx: u32) {
        self.free.push(idx);
    }

    #[inline]
    fn get(&self, idx: u32) -> &Order {
        &self.slots[idx as usize]
    }

    #[inline]
    fn get_mut(&mut self, idx: u32) -> &mut Order {
        &mut self.slots[idx as usize]
    }
}

/// Where a resting order lives: which side, which price level, which slot.
#[derive(Clone, Copy, Debug)]
struct Loc {
    side: Side,
    price: Price,
    slot: u32,
}

// ---------------------------------------------------------------------------
// Price-level index: dense direct-index window + hierarchical bitmap
// ---------------------------------------------------------------------------

/// Ticks covered by the dense window (first insert centres it). Kept small on
/// purpose: the price guard confines active prices to a band, and a compact
/// window (levels array 24 KiB/side, bitmap 128 B) stays cache-resident —
/// a larger window measurably *lost* throughput to cache misses.
const WINDOW: usize = 1 << 10; // 1024 ticks
const W_WORDS: usize = WINDOW / 64; // 16 x u64 = 128 B, L1-resident
const L1_WORDS: usize = (W_WORDS + 63) / 64; // 1

/// A price-level index with **O(1)** lookup/insert/remove and O(1) best-price.
///
/// This plays the role exchange-core's Adaptive Radix Tree plays: replacing the
/// O(log N), pointer-chasing tree walk with constant-time, cache-resident
/// operations. For integer-tick books whose active prices sit in a band (which
/// the price guard enforces), a *dense* window indexed by `price - base` plus a
/// two-level occupancy bitmap is strictly better than a radix tree: level
/// access is one array index, best-price is two `trailing_zeros`/`leading_zeros`
/// scans over 1 KiB of bitmap that lives in L1. Prices that leave the window
/// (rare by construction) fall back to a `BTreeMap`, so correctness never
/// depends on the window.
#[derive(Debug)]
struct LevelIndex {
    /// Window start price; `Price::MAX` until the first insert centres it.
    base: Price,
    /// Dense per-tick levels; `levels[price - base]` — allocated lazily.
    levels: Vec<Vec<u32>>,
    /// Occupancy bitmap: bit i set <=> levels[i] non-empty.
    l0: [u64; W_WORDS],
    /// Summary bitmap: bit w set <=> l0[w] != 0.
    l1: [u64; L1_WORDS],
    /// Occupied levels inside the window.
    in_window: usize,
    /// Out-of-window levels (rare; unbounded prices stay correct).
    overflow: BTreeMap<Price, Vec<u32>>,
}

impl Default for LevelIndex {
    fn default() -> Self {
        LevelIndex {
            base: Price::MAX,
            levels: Vec::new(),
            l0: [0; W_WORDS],
            l1: [0; L1_WORDS],
            in_window: 0,
            overflow: BTreeMap::new(),
        }
    }
}

impl LevelIndex {
    #[inline]
    fn idx(&self, price: Price) -> Option<usize> {
        if price >= self.base {
            let i = (price - self.base) as usize;
            if i < WINDOW {
                return Some(i);
            }
        }
        None
    }

    #[inline]
    fn set_bit(&mut self, i: usize) {
        self.l0[i / 64] |= 1u64 << (i % 64);
        self.l1[i / 64 / 64] |= 1u64 << ((i / 64) % 64);
    }

    #[inline]
    fn clear_bit(&mut self, i: usize) {
        let w = i / 64;
        self.l0[w] &= !(1u64 << (i % 64));
        if self.l0[w] == 0 {
            self.l1[w / 64] &= !(1u64 << (w % 64));
        }
    }

    /// Append `slot` to the level at `price` (creating the level if needed).
    fn push(&mut self, price: Price, slot: u32) {
        if self.base == Price::MAX {
            // Centre the window on the first price seen.
            self.base = price.saturating_sub((WINDOW / 2) as u64);
            self.levels.resize(WINDOW, Vec::new());
        }
        match self.idx(price) {
            Some(i) => {
                if self.levels[i].is_empty() {
                    self.set_bit(i);
                    self.in_window += 1;
                }
                self.levels[i].push(slot);
            }
            None => self.overflow.entry(price).or_default().push(slot),
        }
    }

    /// The (non-empty) level at `price`.
    #[inline]
    fn get(&self, price: Price) -> Option<&Vec<u32>> {
        match self.idx(price) {
            Some(i) => {
                let lv = self.levels.get(i)?;
                (!lv.is_empty()).then_some(lv)
            }
            None => self.overflow.get(&price),
        }
    }

    /// Mutable access; pair with [`Self::sweep`] after possibly emptying it.
    #[inline]
    fn get_mut(&mut self, price: Price) -> Option<&mut Vec<u32>> {
        match self.idx(price) {
            Some(i) => {
                let lv = self.levels.get_mut(i)?;
                (!lv.is_empty()).then_some(lv)
            }
            None => self.overflow.get_mut(&price),
        }
    }

    /// Reconcile bookkeeping after mutating the level at `price`.
    fn sweep(&mut self, price: Price) {
        match self.idx(price) {
            Some(i) => {
                if self.levels.get(i).is_some_and(|lv| lv.is_empty()) && self.bit(i) {
                    self.clear_bit(i);
                    self.in_window -= 1;
                }
            }
            None => {
                if self.overflow.get(&price).is_some_and(|lv| lv.is_empty()) {
                    self.overflow.remove(&price);
                }
            }
        }
    }

    #[inline]
    fn bit(&self, i: usize) -> bool {
        self.l0[i / 64] >> (i % 64) & 1 == 1
    }

    /// Lowest occupied window index, via two bit scans (O(1)).
    fn window_min(&self) -> Option<usize> {
        for (wi, &s) in self.l1.iter().enumerate() {
            if s != 0 {
                let w = wi * 64 + s.trailing_zeros() as usize;
                return Some(w * 64 + self.l0[w].trailing_zeros() as usize);
            }
        }
        None
    }

    /// Highest occupied window index (O(1)).
    fn window_max(&self) -> Option<usize> {
        for (wi, &s) in self.l1.iter().enumerate().rev() {
            if s != 0 {
                let w = wi * 64 + 63 - s.leading_zeros() as usize;
                return Some(w * 64 + 63 - self.l0[w].leading_zeros() as usize);
            }
        }
        None
    }

    /// Lowest occupied price across window and overflow.
    fn min_price(&self) -> Option<Price> {
        let w = self.window_min().map(|i| self.base + i as u64);
        let o = self.overflow.keys().next().copied();
        match (w, o) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Highest occupied price across window and overflow.
    fn max_price(&self) -> Option<Price> {
        let w = self.window_max().map(|i| self.base + i as u64);
        let o = self.overflow.keys().next_back().copied();
        match (w, o) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    /// Visit occupied levels in ascending price order until `f` returns false.
    fn walk_asc(&self, mut f: impl FnMut(Price, &Vec<u32>) -> bool) {
        for (px, lv) in self.overflow.range(..self.base) {
            if !f(*px, lv) {
                return;
            }
        }
        for w in 0..W_WORDS {
            let mut bits = self.l0[w];
            while bits != 0 {
                let i = w * 64 + bits.trailing_zeros() as usize;
                if !f(self.base + i as u64, &self.levels[i]) {
                    return;
                }
                bits &= bits - 1;
            }
        }
        if self.base != Price::MAX {
            for (px, lv) in self.overflow.range(self.base + WINDOW as u64..) {
                if !f(*px, lv) {
                    return;
                }
            }
        }
    }

    /// Visit occupied levels in descending price order until `f` returns false.
    fn walk_desc(&self, mut f: impl FnMut(Price, &Vec<u32>) -> bool) {
        if self.base != Price::MAX {
            for (px, lv) in self.overflow.range(self.base + WINDOW as u64..).rev() {
                if !f(*px, lv) {
                    return;
                }
            }
        }
        for w in (0..W_WORDS).rev() {
            let mut bits = self.l0[w];
            while bits != 0 {
                let i = w * 64 + 63 - bits.leading_zeros() as usize;
                if !f(self.base + i as u64, &self.levels[i]) {
                    return;
                }
                bits &= !(1u64 << (i % 64));
            }
        }
        for (px, lv) in self.overflow.range(..self.base).rev() {
            if !f(*px, lv) {
                return;
            }
        }
    }
}

/// A price-time ordered limit order book over a slab pool.
///
/// Each side maps price -> time-ordered `Vec<u32>` of pool slots. Bids are read
/// highest-first, asks lowest-first. A location index gives O(log n) cancels.
#[derive(Debug, Default)]
pub struct OrderBook {
    pool: OrderPool,
    bids: LevelIndex,
    asks: LevelIndex,
    locate: HashMap<OrderId, Loc>,
    /// Open-order count per user (user 0 not tracked) — O(1) pre-trade checks.
    user_counts: HashMap<u64, u32>,
}

impl OrderBook {
    /// A book with a small default pool (grows on demand). Prefer
    /// [`OrderBook::with_capacity`] for production sizing.
    pub fn new() -> Self {
        Self::with_capacity(1024, false)
    }

    /// A book whose pool reserves `max_orders` slots up front; see
    /// [`OrderPool::with_capacity`] for `prefault`.
    pub fn with_capacity(max_orders: usize, prefault: bool) -> Self {
        OrderBook {
            pool: OrderPool::with_capacity(max_orders, prefault),
            bids: LevelIndex::default(),
            asks: LevelIndex::default(),
            locate: HashMap::new(),
            user_counts: HashMap::new(),
        }
    }

    /// Number of orders `user` currently has resting (0 for user 0).
    pub fn user_open_orders(&self, user: u64) -> u32 {
        self.user_counts.get(&user).copied().unwrap_or(0)
    }

    fn count_user(user_counts: &mut HashMap<u64, u32>, user: u64, delta: i32) {
        if user == 0 {
            return;
        }
        let e = user_counts.entry(user).or_insert(0);
        *e = e.saturating_add_signed(delta);
        if *e == 0 {
            user_counts.remove(&user);
        }
    }

    /// Every resting order (arbitrary iteration order; sort by `timestamp` to
    /// reconstruct queue priority — used by snapshots).
    pub fn iter_orders(&self) -> impl Iterator<Item = &Order> + '_ {
        self.locate.values().map(|loc| self.pool.get(loc.slot))
    }

    /// Pool statistics: (reserved slots, live orders).
    pub fn pool_stats(&self) -> (usize, usize) {
        (self.pool.capacity(), self.pool.in_use())
    }

    /// Highest bid price, if any. O(1) via the occupancy bitmap.
    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.max_price()
    }

    /// Lowest ask price, if any. O(1) via the occupancy bitmap.
    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.min_price()
    }

    /// Best price on the given resting side.
    #[inline]
    pub fn best(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.best_bid(),
            Side::Sell => self.best_ask(),
        }
    }

    fn side(&self, side: Side) -> &LevelIndex {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut LevelIndex {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// True if the book holds no resting orders.
    pub fn is_empty(&self) -> bool {
        self.locate.is_empty()
    }

    /// Number of resting orders.
    pub fn len(&self) -> usize {
        self.locate.len()
    }

    /// A time-ordered, read-only view of the orders resting at `(side, price)`,
    /// for handing to a matching strategy.
    pub fn level_view(&self, side: Side, price: Price) -> Vec<RestingOrder> {
        let mut out = Vec::new();
        self.level_view_into(side, price, &mut out);
        out
    }

    /// Allocation-free variant of [`level_view`](Self::level_view): clears and
    /// refills `out` (the matching engine reuses one buffer across all crosses).
    pub fn level_view_into(&self, side: Side, price: Price, out: &mut Vec<RestingOrder>) {
        out.clear();
        if let Some(idxs) = self.side(side).get(price) {
            out.extend(idxs.iter().map(|&i| {
                let o = self.pool.get(i);
                RestingOrder {
                    id: o.id,
                    remaining: o.remaining,
                    timestamp: o.timestamp,
                    user: o.user,
                }
            }));
        }
    }

    /// Total resting quantity reachable by an aggressor with limit `limit`
    /// (pass `Price::MAX`/`MIN` for market orders).
    pub fn crossable_qty(&self, aggressor: Side, limit: Price) -> Qty {
        let levels = self.side(aggressor.opposite());
        let pool = &self.pool;
        let mut sum: Qty = 0;
        match aggressor {
            Side::Buy => levels.walk_asc(|px, idxs| {
                if px > limit {
                    return false;
                }
                sum += idxs.iter().map(|&i| pool.get(i).remaining).sum::<Qty>();
                true
            }),
            Side::Sell => levels.walk_desc(|px, idxs| {
                if px < limit {
                    return false;
                }
                sum += idxs.iter().map(|&i| pool.get(i).remaining).sum::<Qty>();
                true
            }),
        }
        sum
    }

    /// Insert a resting order into its price level (allocating a pool slot).
    pub fn insert(&mut self, order: Order) {
        debug_assert!(
            !self.locate.contains_key(&order.id),
            "duplicate resting order {}",
            order.id
        );
        let (id, side, price, user) = (order.id, order.side, order.price, order.user);
        let slot = self.pool.alloc(order);
        self.locate.insert(id, Loc { side, price, slot });
        self.side_mut(side).push(price, slot);
        Self::count_user(&mut self.user_counts, user, 1);
    }

    /// Apply fills produced by a strategy against the resting `side` at `price`:
    /// reduce each maker, drop fully-filled makers (recycling their slots), and
    /// remove the level if it empties.
    pub fn apply_fills(&mut self, side: Side, price: Price, fills: &[(OrderId, Qty)]) {
        let pool = &mut self.pool;
        let locate = &mut self.locate;
        let user_counts = &mut self.user_counts;
        let index = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if let Some(idxs) = index.get_mut(price) {
            idxs.retain(|&i| {
                let o = pool.get_mut(i);
                // Fills per level are few (often 1-2): a linear scan beats
                // building a HashMap — and allocates nothing.
                if let Some(&(_, q)) = fills.iter().find(|(id, _)| *id == o.id) {
                    debug_assert!(q <= o.remaining, "over-fill of {}", o.id);
                    o.remaining -= q;
                }
                if o.remaining == 0 {
                    locate.remove(&o.id);
                    Self::count_user(user_counts, o.user, -1);
                    pool.release(i);
                    false
                } else {
                    true
                }
            });
        }
        index.sweep(price);
    }

    /// Borrow a resting order by id, or `None` if it is not on the book.
    pub fn get(&self, id: OrderId) -> Option<&Order> {
        let loc = self.locate.get(&id)?;
        Some(self.pool.get(loc.slot))
    }

    /// Reduce a resting order's remaining quantity **in place**, preserving its
    /// queue position. Reducing to zero cancels it. Returns `false` if the order
    /// is not resting or `new_remaining` exceeds the current remaining.
    pub fn reduce(&mut self, id: OrderId, new_remaining: Qty) -> bool {
        let Some(loc) = self.locate.get(&id).copied() else {
            return false;
        };
        if new_remaining == 0 {
            return self.cancel(id).is_some();
        }
        let o = self.pool.get_mut(loc.slot);
        if new_remaining > o.remaining {
            return false;
        }
        o.remaining = new_remaining;
        true
    }

    /// Cancel a resting order by id. Returns the removed order, or `None` if it
    /// was not resting. The slot is recycled.
    pub fn cancel(&mut self, id: OrderId) -> Option<Order> {
        let loc = self.locate.remove(&id)?;
        let order = *self.pool.get(loc.slot);
        let index = self.side_mut(loc.side);
        if let Some(idxs) = index.get_mut(loc.price) {
            if let Some(pos) = idxs.iter().position(|&i| i == loc.slot) {
                idxs.remove(pos);
            }
        }
        index.sweep(loc.price);
        self.pool.release(loc.slot);
        Self::count_user(&mut self.user_counts, order.user, -1);
        Some(order)
    }

    /// All resting order ids belonging to `user` (for forced liquidation).
    /// O(resting orders); force-close is a rare administrative action.
    pub fn orders_of_user(&self, user: u64) -> Vec<OrderId> {
        self.locate
            .iter()
            .filter(|(_, loc)| self.pool.get(loc.slot).user == user)
            .map(|(&id, _)| id)
            .collect()
    }

    /// The top `n` levels of `side` as `(price, total_qty)`, best price first —
    /// the depth-of-market feed for market data.
    pub fn top_levels(&self, side: Side, n: usize) -> Vec<(Price, Qty)> {
        let pool = &self.pool;
        let mut rows = Vec::with_capacity(n);
        let mut push = |px: Price, idxs: &Vec<u32>| {
            rows.push((px, idxs.iter().map(|&i| pool.get(i).remaining).sum::<Qty>()));
            rows.len() < n
        };
        match side {
            Side::Buy => self.side(side).walk_desc(&mut push),
            Side::Sell => self.side(side).walk_asc(&mut push),
        }
        rows
    }

    /// A snapshot of the book as `(price, total_qty, order_count)` rows, best
    /// price first, for display and diagnostics.
    pub fn depth(&self, side: Side) -> Vec<(Price, Qty, usize)> {
        let pool = &self.pool;
        let mut rows = Vec::new();
        let mut push = |px: Price, idxs: &Vec<u32>| {
            rows.push((
                px,
                idxs.iter().map(|&i| pool.get(i).remaining).sum::<Qty>(),
                idxs.len(),
            ));
            true
        };
        match side {
            // Best (highest) bid first / best (lowest) ask first.
            Side::Buy => self.side(side).walk_desc(&mut push),
            Side::Sell => self.side(side).walk_asc(&mut push),
        }
        rows
    }
}
