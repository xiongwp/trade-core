//! A dependency-free throughput micro-benchmark. Run with `cargo bench`.
//!
//! It replays a synthetic order flow (mixed adds, crosses and cancels) through
//! the engine for each strategy and reports orders/second. This is a smoke-level
//! benchmark for relative comparison, not a latency-grade measurement.

use std::time::Instant;
use trade_core::prelude::*;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

fn run(name: &str, mut engine: MatchingEngine, orders: u64) {
    let mut rng = Rng(0xDEADBEEFCAFEF00D);
    let mut id = 0u64;
    let mut trades = 0u64;

    let start = Instant::now();
    for _ in 0..orders {
        id += 1;
        let side = if rng.next() & 1 == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        // Prices cluster around 1000 so orders actually cross and rest.
        let price = rng.range(990, 1010);
        let qty = rng.range(1, 50);
        let report = engine.submit(Order::limit(OrderId(id), side, price, qty));
        trades += report.trades.len() as u64;

        // Occasionally cancel an older resting order to keep the book bounded.
        if id > 64 && rng.next() % 8 == 0 {
            engine.cancel(OrderId(rng.range(1, id)));
        }
    }
    let elapsed = start.elapsed();
    let ops = orders as f64 / elapsed.as_secs_f64();
    println!(
        "{name:<14} {orders} orders in {:>7.2?}  ->  {:>10.0} orders/s  ({trades} trades, book depth {})",
        elapsed,
        ops,
        engine.book().len()
    );
}

fn main() {
    let orders: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(500_000);

    println!("trade-core throughput ({orders} orders/strategy)\n");
    run(
        "price-time",
        MatchingEngine::with_strategy(PriceTimePriority),
        orders,
    );
    run("pro-rata", MatchingEngine::with_strategy(ProRata), orders);
    run(
        "size-priority",
        MatchingEngine::with_strategy(SizePriority),
        orders,
    );
}
