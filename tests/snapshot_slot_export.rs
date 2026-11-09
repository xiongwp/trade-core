//! Snapshots copy each book's raw slot pool on the matching thread and turn
//! it into resting orders later. That export must equal the book's live
//! orders exactly, whatever mix of fills, cancels and slot reuse came before.

use trade_core::book::live_orders_by_time;
use trade_core::{Order, OrderBook, OrderId, Side};

#[test]
fn slot_copy_exports_exactly_the_live_orders_in_time_order() {
    let mut book = OrderBook::new();
    let mut live: Vec<OrderId> = Vec::new();
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for step in 1..=20_000u64 {
        let roll = next() % 10;
        if roll < 6 || live.is_empty() {
            let side = if next() % 2 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = 1_000 + next() % 16;
            let mut order = Order::limit(OrderId(step), side, price, 1 + next() % 5);
            order.timestamp = step;
            book.insert(order);
            live.push(order.id);
        } else if roll < 8 {
            let id = live.swap_remove((next() % live.len() as u64) as usize);
            book.cancel(id).expect("live order cancels");
        } else {
            // Partially or fully fill one resting order in place.
            let id = live[(next() % live.len() as u64) as usize];
            let order = *book.iter_orders().find(|order| order.id == id).unwrap();
            let qty = 1 + next() % order.remaining;
            book.apply_fills(order.side, order.price, &[(id, qty)]);
            if qty == order.remaining {
                live.retain(|&other| other != id);
            }
        }
        if step % 997 == 0 {
            let mut expected: Vec<Order> = book.iter_orders().copied().collect();
            expected.sort_by_key(|order| order.timestamp);
            assert_eq!(
                format!("{:?}", live_orders_by_time(book.copy_slots())),
                format!("{expected:?}"),
                "step {step}"
            );
            assert_eq!(expected.len(), live.len());
        }
    }
}
