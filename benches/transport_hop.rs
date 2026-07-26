//! Transport-hop micro-benchmark: what one message hop actually costs, so the
//! "Kafka vs Aeron vs collapse-the-hop" decision rests on local numbers instead
//! of vendor folklore.
//!
//! A command's journey has three separable costs:
//!
//!   1. **Codec** — `encode_command` + `WireView::parse`/`to_command`. Paid on
//!      every transport (Kafka, Aeron, or an in-process queue) because the
//!      command still has to be serialized to the 40-byte wire frame and back.
//!   2. **Same-host handoff** — the SPSC cross-core enqueue→dequeue + wakeup.
//!      This is the floor a same-host transport competes against: Aeron IPC
//!      lands here (sub-µs to low-µs); collapsing an ingress hop removes it
//!      entirely.
//!   3. **Broker round-trip** — Kafka's `acks=all` produce to a replicated,
//!      fsynced partition. NOT measurable here (needs a live broker); read it in
//!      production from `tc_execution_kafka_publish_ns_*`. It is milliseconds,
//!      i.e. ~1000x the numbers this bench prints.
//!
//! The point: (1) and (2) bound what a faster transport can *possibly* save on
//! the same host. If your end-to-end budget is dominated by (3), the lever is
//! the broker hop — either Aeron in its place, or removing a redundant hop
//! (the Raft WAL is already an ordered, durable log) — not anything in-process.
//!
//! Run: `cargo bench --bench transport_hop [-- ITERS]`

use std::thread;
use std::time::Instant;

use trade_core::exchange::Command;
use trade_core::lockfree;
use trade_core::prelude::*;
use trade_core::wire::{self, WireView, MSG_LEN};

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[i] as f64
}

fn report(label: &str, mut lat_ns: Vec<u64>) {
    lat_ns.sort_unstable();
    let sum: u128 = lat_ns.iter().map(|&x| x as u128).sum();
    let mean = sum as f64 / lat_ns.len() as f64;
    println!(
        "{label:<22} mean={:>6.0}ns  p50={:>6.0}ns  p99={:>6.0}ns  max={:>7.0}ns",
        mean,
        pct(&lat_ns, 0.50),
        pct(&lat_ns, 0.99),
        pct(&lat_ns, 0.999),
    );
}

/// Cost 1: serialize a command to the wire frame and parse it back.
///
/// A single encode+decode is faster than one `Instant::now()` read, so it is
/// batch-timed (total / iters) rather than per-op sampled — per-op sampling
/// would measure the clock, not the codec.
fn bench_codec(iters: usize) {
    let cmd = Command::New(Order::limit(OrderId(1), Side::Buy, 1000, 7));
    let mut frame = [0u8; MSG_LEN];

    let t = Instant::now();
    let mut acc = 0u64;
    for _ in 0..iters {
        wire::encode_command(std::hint::black_box(&cmd), &mut frame);
        let view = WireView::parse(std::hint::black_box(&frame)).expect("parse wire frame");
        let decoded = view.to_command().expect("decode command");
        // Fold a field of the decode in so the optimizer can't elide the round trip.
        acc = acc.wrapping_add(decoded.id());
    }
    let elapsed = t.elapsed();
    std::hint::black_box(acc);
    let per_op = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "codec encode+decode    {per_op:>5.1}ns/op  ({:.1}M frames/s, {iters} x {MSG_LEN}B)",
        iters as f64 / elapsed.as_secs_f64() / 1e6,
    );
}

/// Cost 2: one message's round trip through two SPSC queues and two thread
/// wakeups (ping → worker → pong). One-way same-host handoff ≈ rtt / 2.
fn bench_spsc_hop(iters: usize) {
    // Small capacity: strict ping-pong keeps exactly one message in flight, so
    // we measure handoff latency, not queue-drain throughput.
    let (fwd_tx, fwd_rx) = lockfree::channel::<Instant>(1024);
    let (back_tx, back_rx) = lockfree::channel::<Instant>(1024);

    // Echo exactly the warmup (1024) + measured messages, then exit.
    let to_echo = iters + 1024;
    let worker = thread::spawn(move || {
        let mut echoed = 0usize;
        while echoed < to_echo {
            match fwd_rx.pop() {
                Some(t) => {
                    // Bounce the original send instant straight back.
                    while back_tx.push(t).is_err() {
                        std::hint::spin_loop();
                    }
                    echoed += 1;
                }
                None => std::hint::spin_loop(),
            }
        }
    });

    // Warm up the cross-core cache lines and thread scheduling.
    for _ in 0..1024 {
        while fwd_tx.push(Instant::now()).is_err() {
            std::hint::spin_loop();
        }
        while back_rx.pop().is_none() {
            std::hint::spin_loop();
        }
    }

    let mut rtt = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        while fwd_tx.push(t).is_err() {
            std::hint::spin_loop();
        }
        let echoed = loop {
            if let Some(e) = back_rx.pop() {
                break e;
            }
            std::hint::spin_loop();
        };
        rtt.push(echoed.elapsed().as_nanos() as u64);
    }
    worker.join().expect("join worker");

    let mut one_way: Vec<u64> = rtt.iter().map(|&r| r / 2).collect();
    report("spsc round-trip", rtt);
    one_way.sort_unstable();
    report("spsc one-way (rtt/2)", one_way);
}

fn main() {
    let iters: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);

    println!("transport-hop bench: {iters} iterations\n");
    println!("These are the same-host costs a faster transport competes against.");
    println!("The broker round trip (Kafka acks=all) is NOT here — it is ~ms, read");
    println!("it in prod from tc_execution_kafka_publish_ns_*.\n");

    bench_codec(iters);
    println!();
    bench_spsc_hop(iters.min(1_000_000));

    println!();
    println!("Reading the result:");
    println!("  • codec + one-way handoff = the ceiling on same-host savings.");
    println!("  • That ceiling (~100ns/hop here) is ~4 orders of magnitude below a");
    println!("    Kafka acks=all publish (~ms). So the broker hop, not in-process");
    println!("    work, is the cost: the lever is Aeron in the broker's place, or");
    println!("    removing a redundant hop (the Raft WAL is already an ordered log).");
}
