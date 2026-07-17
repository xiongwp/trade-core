//! # trade-core
//!
//! A compact, dependency-free **order matching engine** core, built around one
//! idea: the part of a trading venue that varies between markets — *how liquidity
//! at a price level is allocated* — is isolated behind a single trait, while the
//! invariant part — price-time bookkeeping, time-in-force, resting and cancels —
//! is written once.
//!
//! ## Components
//!
//! * [`types`] — integer price/quantity value types (no floating point in the
//!   matching path).
//! * [`Order`] — an order submitted to the engine.
//! * [`OrderBook`] — price-time ordered resting liquidity with O(log n) cancels.
//! * [`strategy`] — the [`MatchingStrategy`] trait and three implementations:
//!   [`PriceTimePriority`] (FIFO), [`ProRata`], and [`SizePriority`].
//! * [`MatchingEngine`] — intake, crossing, TIF, and execution reporting.
//!
//! ## Example
//!
//! ```
//! use trade_core::prelude::*;
//!
//! let mut engine = MatchingEngine::with_strategy(PriceTimePriority);
//!
//! // Rest two asks at price 100.
//! engine.submit(Order::limit(OrderId(1), Side::Sell, 100, 5));
//! engine.submit(Order::limit(OrderId(2), Side::Sell, 100, 5));
//!
//! // A buyer lifts 8 lots: order #1 fills fully (FIFO), #2 fills 3.
//! let report = engine.submit(Order::limit(OrderId(3), Side::Buy, 100, 8));
//! assert_eq!(report.status, OrderStatus::Filled);
//! assert_eq!(report.filled, 8);
//! assert_eq!(report.trades[0].maker, OrderId(1));
//! assert_eq!(report.trades[0].quantity, 5);
//! ```

pub mod affinity;
pub mod asset_log;
pub mod book;
pub mod cluster;
pub mod engine;
pub mod exchange;
pub mod fees;
pub mod gateway;
pub mod idgen;
pub mod journal;
pub mod kline;
pub mod lockfree;
pub mod metrics;
pub mod order;
pub mod order_queue;
pub mod raft_log;
pub mod replication;
pub mod risk;
pub mod sharding;
pub mod snapshot;
pub mod strategy;
pub mod trade;
pub mod types;
pub mod wire;

pub use book::{OrderBook, OrderPool};
pub use engine::{MatchingEngine, SelfTradePolicy};
pub use exchange::{
    build as build_exchange, recover_into, replay_journal, Command, ExchangeConfig, ExchangeHandle,
    ExecReport, ExecutionReportEvent, OrderGateway, ResultSink,
};
pub use order::Order;
pub use risk::{PriceGuard, RiskLimits};
pub use strategy::{
    Allocation, MatchingStrategy, PriceTimePriority, ProRata, RestingOrder, SizePriority,
};
pub use trade::{ModifyOutcome, OrderStatus, SubmitReport, Trade};
pub use types::{InstrumentId, OrderId, OrderType, Price, Qty, Side, TimeInForce, Timestamp};

/// Glob-importable common items.
pub mod prelude {
    pub use crate::engine::MatchingEngine;
    pub use crate::order::Order;
    pub use crate::strategy::{MatchingStrategy, PriceTimePriority, ProRata, SizePriority};
    pub use crate::trade::{ModifyOutcome, OrderStatus, SubmitReport, Trade};
    pub use crate::types::{InstrumentId, OrderId, OrderType, Price, Qty, Side, TimeInForce};
}
