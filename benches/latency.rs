//! End-to-end order latency: submit → terminal report, measured under a paced
//! load through the full pipeline (intake queue → match → result queue).
//!
//! This mirrors how exchange-core publishes its numbers (percentile latency at
//! a fixed target rate), so results are comparable in kind — hardware differs,
//! which the README states plainly.
//!
//! Run: `cargo bench --bench latency [-- RATE_PER_SEC ORDERS]`

use std::time::{Duration, Instant};

use trade_core::exchange::{build, ExchangeConfig, ExecReport};
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

fn is_terminal(r: &ExecReport) -> bool {
    matches!(
        r,
        ExecReport::Filled { .. }
            | ExecReport::PartiallyFilled { .. }
            | ExecReport::Resting { .. }
            | ExecReport::Cancelled { .. }
            | ExecReport::Rejected { .. }
            | ExecReport::NotFound { .. }
    )
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rate: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let orders: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);

    println!("latency: {orders} orders paced at {rate}/s through the full pipeline");

    let (gw, sink, handle) = build(ExchangeConfig {
        shards: 1,
        queue_capacity: 1 << 16,
        pin_cpus: true,
        pool_orders_per_book: 1 << 20,
        prefault: true,
        ..ExchangeConfig::default()
    });

    // send_at[id] = submit instant; terminal report closes the measurement.
    let mut send_at: Vec<Option<Instant>> = vec![None; orders as usize + 1];
    let mut lat_ns: Vec<u64> = Vec::with_capacity(orders as usize);
    let mut rng = Rng(0xC0FFEE);

    let tick = Duration::from_nanos(1_000_000_000 / rate);
    let start = Instant::now();
    let mut next_send = start;

    let mut sent = 0u64;
    let mut done = 0u64;
    while done < orders {
        // Paced submission.
        if sent < orders && Instant::now() >= next_send {
            sent += 1;
            let side = if rng.next() & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let order = Order::limit(OrderId(sent), side, rng.range(990, 1010), rng.range(1, 50));
            send_at[sent as usize] = Some(Instant::now());
            if gw.new_order(order).is_err() {
                // Backpressure at this rate means the rate is beyond capacity;
                // record it as an unmeasured drop rather than blocking the pacer.
                send_at[sent as usize] = None;
                done += 1;
            }
            next_send += tick;
        }
        // Drain reports; terminal report completes an order's measurement.
        sink.poll(|r| {
            if is_terminal(&r) {
                let id = match r {
                    ExecReport::Filled { order_id, .. }
                    | ExecReport::PartiallyFilled { order_id, .. }
                    | ExecReport::Resting { order_id, .. }
                    | ExecReport::Cancelled { order_id, .. }
                    | ExecReport::Rejected { order_id, .. }
                    | ExecReport::NotFound { order_id, .. } => order_id,
                    _ => unreachable!(),
                };
                if let Some(Some(t0)) = send_at.get(id.0 as usize) {
                    lat_ns.push(t0.elapsed().as_nanos() as u64);
                }
                done += 1;
            }
        });
    }
    let elapsed = start.elapsed();
    handle.shutdown();

    lat_ns.sort_unstable();
    let pct = |p: f64| lat_ns[((lat_ns.len() as f64 * p) as usize).min(lat_ns.len() - 1)];
    println!(
        "measured {} orders in {elapsed:.2?} (effective {:.0}/s)",
        lat_ns.len(),
        lat_ns.len() as f64 / elapsed.as_secs_f64()
    );
    println!(
        "latency  p50={:.1}µs  p90={:.1}µs  p99={:.1}µs  p99.9={:.1}µs  max={:.1}µs",
        pct(0.50) as f64 / 1000.0,
        pct(0.90) as f64 / 1000.0,
        pct(0.99) as f64 / 1000.0,
        pct(0.999) as f64 / 1000.0,
        *lat_ns.last().unwrap() as f64 / 1000.0,
    );
}
