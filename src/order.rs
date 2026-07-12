//! Orders submitted to the engine.

use crate::types::*;

/// An order as it lives inside the engine.
///
/// `quantity` is the original submitted size and never changes; `remaining`
/// tracks the unfilled portion and is mutated as the order fills. `timestamp`
/// is assigned by the engine on accept and is the time-priority key.
#[derive(Clone, Copy, Debug)]
pub struct Order {
    pub id: OrderId,
    /// The instrument this order trades. Defaults to `InstrumentId(0)`; set it
    /// with [`Order::on`] for multi-asset routing.
    pub instrument: InstrumentId,
    /// Owning user/account id. Used for risk actions such as forced liquidation
    /// (cancel-all + close) and for order-system DB sharding. 0 = unattributed.
    pub user: u64,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    /// Limit price in ticks. Ignored for [`OrderType::Market`].
    pub price: Price,
    /// Original submitted quantity.
    pub quantity: Qty,
    /// Unfilled quantity; equals `quantity` until the order starts filling.
    pub remaining: Qty,
    /// Engine-assigned sequence number (time priority). Zero until accepted.
    pub timestamp: Timestamp,
}

impl Order {
    /// A good-till-cancel limit order (instrument 0 by default).
    pub fn limit(id: OrderId, side: Side, price: Price, quantity: Qty) -> Self {
        Order {
            id,
            instrument: InstrumentId(0),
            user: 0,
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            price,
            quantity,
            remaining: quantity,
            timestamp: 0,
        }
    }

    /// A market order (sweeps the book; never rests). Defaults to IOC semantics.
    pub fn market(id: OrderId, side: Side, quantity: Qty) -> Self {
        Order {
            id,
            instrument: InstrumentId(0),
            user: 0,
            side,
            order_type: OrderType::Market,
            tif: TimeInForce::Ioc,
            // Sentinel price so any resting order is considered crossable.
            price: match side {
                Side::Buy => Price::MAX,
                Side::Sell => Price::MIN,
            },
            quantity,
            remaining: quantity,
            timestamp: 0,
        }
    }

    /// Override the time-in-force (builder style).
    pub fn with_tif(mut self, tif: TimeInForce) -> Self {
        self.tif = tif;
        self
    }

    /// Route this order to a specific instrument (builder style).
    pub fn on(mut self, instrument: InstrumentId) -> Self {
        self.instrument = instrument;
        self
    }

    /// Attribute this order to a user/account (builder style).
    pub fn by(mut self, user: u64) -> Self {
        self.user = user;
        self
    }

    /// Quantity already filled.
    #[inline]
    pub fn filled(&self) -> Qty {
        self.quantity - self.remaining
    }
}
