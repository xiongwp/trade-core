//! Randomised contract checks: for many random price levels and aggressor sizes,
//! every strategy must satisfy the allocation invariants (exact total, no
//! over-fill, no zero allocations). Uses a tiny deterministic PRNG so failures
//! reproduce; no external dependencies.

use trade_core::strategy::RestingOrder;
use trade_core::{MatchingStrategy, OrderId, PriceTimePriority, ProRata, SizePriority};

/// Deterministic xorshift64* PRNG.
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

fn check(strategy: &dyn MatchingStrategy) {
    let mut rng = Rng(0x9E3779B97F4A7C15 ^ strategy.name().len() as u64);
    for _ in 0..5_000 {
        let n = rng.range(1, 8) as usize;
        let resting: Vec<RestingOrder> = (0..n)
            .map(|i| RestingOrder {
                id: OrderId(i as u64 + 1),
                remaining: rng.range(1, 1000),
                timestamp: i as u64 + 1,
                user: 0,
            })
            .collect();
        let total: u64 = resting.iter().map(|r| r.remaining).sum();
        // Probe under-, exact-, and over-sized aggressors.
        let incoming = rng.range(0, total + 50);

        let out = strategy.allocate(&resting, incoming);
        if let Err(e) = strategy.validate(&resting, incoming, &out) {
            panic!(
                "{} violated contract: {e}\n resting={resting:?}\n incoming={incoming}\n out={out:?}",
                strategy.name()
            );
        }
    }
}

#[test]
fn price_time_respects_contract() {
    check(&PriceTimePriority);
}

#[test]
fn pro_rata_respects_contract() {
    check(&ProRata);
}

#[test]
fn size_priority_respects_contract() {
    check(&SizePriority);
}
