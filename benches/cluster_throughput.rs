//! Aggregate throughput of a horizontally-scaled deployment: N independent
//! share-nothing matching nodes (1 shard each), measured **end-to-end** —
//! commands cross the lock-free intake queue, match, and every execution report
//! crosses the result queue and is drained.
//!
//! Run: `cargo bench --bench cluster_throughput [-- NODES ORDERS_PER_NODE journal]`

use std::time::Instant;

use trade_core::exchange::{build, ExchangeConfig, ExecReport};
use trade_core::prelude::*;
use trade_core::Command;

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

/// Pre-generate a realistic mixed flow: 85% new limit orders clustered so they
/// cross and rest, 15% cancels of earlier ids.
fn generate(node: u64, n: u64) -> Vec<Command> {
    let mut rng = Rng(0xDEADBEEF ^ node.wrapping_mul(0x9E3779B97F4A7C15));
    let mut out = Vec::with_capacity(n as usize);
    let base = node << 32; // node-unique id space
    for i in 1..=n {
        if i > 64 && rng.next() % 7 == 0 {
            out.push(Command::Cancel {
                instrument: trade_core::InstrumentId(0),
                order_id: OrderId(base + rng.range(1, i - 1)),
                cmd_id: base + i,
            });
        } else {
            let side = if rng.next() & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = rng.range(990, 1010);
            let qty = rng.range(1, 50);
            out.push(Command::New(Order::limit(
                OrderId(base + i),
                side,
                price,
                qty,
            )));
        }
    }
    out
}

/// Terminal reports: exactly one per command in a New/Cancel-only flow.
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
    let nodes: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let per_node: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_500_000);
    let journal = args.next().as_deref() == Some("journal");

    println!(
        "cluster throughput: {nodes} share-nothing nodes x {per_node} orders \
         (journal: {})",
        if journal { "ON, 1s flush" } else { "off" }
    );

    // Pre-generate all flows outside the timed section.
    let flows: Vec<Vec<Command>> = (0..nodes).map(|n| generate(n, per_node)).collect();

    let mut clients = Vec::new();
    let mut handles = Vec::new();

    let journal_dir = std::env::temp_dir().join(format!("tc-bench-{}", std::process::id()));

    let start = Instant::now();
    for (node, flow) in flows.into_iter().enumerate() {
        let cfg = ExchangeConfig {
            shards: 1,
            queue_capacity: 1 << 16,
            pin_cpus: true,
            journal_dir: journal.then(|| journal_dir.join(format!("node-{node}"))),
            pool_orders_per_book: 1 << 20,
            prefault: true,
            ..ExchangeConfig::default()
        };
        let (gw, sink, handle) = build(cfg);
        handles.push(handle);

        // One client thread per node, like a real order-system IO thread:
        // interleave feeding commands and draining reports (2 threads per node
        // total, so the bench does not oversubscribe cores with spin-waiters).
        clients.push(std::thread::spawn(move || {
            let mut terminals = 0u64;
            let mut trades = 0u64;
            fn drain(
                sink: &trade_core::ResultSink,
                terminals: &mut u64,
                trades: &mut u64,
            ) -> usize {
                sink.poll(|r| {
                    if is_terminal(&r) {
                        *terminals += 1;
                    } else if matches!(r, ExecReport::Trade { .. }) {
                        *trades += 1;
                    }
                })
            }
            for cmd in flow {
                let mut pending = cmd;
                loop {
                    match gw.submit(pending) {
                        Ok(()) => break,
                        Err(ret) => {
                            pending = ret;
                            // Intake full: drain reports while we wait.
                            drain(&sink, &mut terminals, &mut trades);
                        }
                    }
                }
                drain(&sink, &mut terminals, &mut trades);
            }
            while terminals < per_node {
                if drain(&sink, &mut terminals, &mut trades) == 0 {
                    std::hint::spin_loop();
                }
            }
            trades
        }));
    }

    let mut total_trades = 0u64;
    for c in clients {
        total_trades += c.join().unwrap();
    }
    let elapsed = start.elapsed();

    for h in handles {
        h.shutdown();
    }
    let _ = std::fs::remove_dir_all(&journal_dir);

    let total = nodes * per_node;
    let rate = total as f64 / elapsed.as_secs_f64();
    println!(
        "processed {total} orders ({total_trades} trades) in {elapsed:.2?}  ->  \
         {rate:.0} orders/s aggregate  ({:.0} per node)",
        rate / nodes as f64
    );
    let target = 5_000_000.0;
    println!(
        "target 5,000,000 orders/s: {}",
        if rate >= target {
            "MET ✓"
        } else {
            "NOT MET ✗"
        }
    );
}
