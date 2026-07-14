//! Runs the *same* order flow through all three matching strategies so the
//! behavioural difference is visible side by side.
//!
//! Run with: `cargo run --example demo`

use trade_core::prelude::*;
use trade_core::MatchingStrategy;

fn build_engine(strategy: Box<dyn MatchingStrategy>) -> MatchingEngine {
    let mut engine = MatchingEngine::new(strategy);
    // Three resting asks at the SAME price 100, different sizes and arrival order:
    //   #1 size 10 (oldest), #2 size 30, #3 size 60 (newest). Total 100 @ 100.
    engine.submit(Order::limit(OrderId(1), Side::Sell, 100, 10));
    engine.submit(Order::limit(OrderId(2), Side::Sell, 100, 30));
    engine.submit(Order::limit(OrderId(3), Side::Sell, 100, 60));
    engine
}

fn run(label: &str, strategy: Box<dyn MatchingStrategy>) {
    let mut engine = build_engine(strategy);
    println!("\n=== {label} ===");
    println!("Resting asks @100: #1=10  #2=30  #3=60  (total 100)");

    // A single aggressive buy for 50 lots @ 100 — smaller than the level, so the
    // allocation policy fully determines who gets filled.
    let report = engine.submit(Order::limit(OrderId(99), Side::Buy, 100, 50));
    println!("Aggressor: BUY 50 @100  ->  filled {}", report.filled);
    for t in &report.trades {
        println!(
            "   fill: maker {} qty {:>3} @ {}",
            t.maker, t.quantity, t.price
        );
    }
    println!("Remaining resting depth (asks):");
    for (px, qty, n) in engine.book().depth(Side::Sell) {
        println!("   price {px}: qty {qty} across {n} order(s)");
    }
}

fn main() {
    println!("trade-core — multi-strategy matching demo");
    run("Price-Time (FIFO)", Box::new(PriceTimePriority));
    run("Pro-Rata", Box::new(ProRata));
    run("Size-Priority", Box::new(SizePriority));

    // A separate scenario: crossing multiple price levels + IOC remainder.
    println!("\n=== Multi-level sweep + IOC (price-time) ===");
    let mut engine = MatchingEngine::with_strategy(PriceTimePriority);
    engine.submit(Order::limit(OrderId(10), Side::Sell, 100, 5));
    engine.submit(Order::limit(OrderId(11), Side::Sell, 101, 5));
    engine.submit(Order::limit(OrderId(12), Side::Sell, 102, 5));
    let report =
        engine.submit(Order::limit(OrderId(20), Side::Buy, 101, 20).with_tif(TimeInForce::Ioc));
    println!(
        "BUY 20 @101 IOC -> status {:?}, filled {}",
        report.status, report.filled
    );
    for t in &report.trades {
        println!(
            "   fill: maker {} qty {} @ {}",
            t.maker, t.quantity, t.price
        );
    }
    println!(
        "VWAP: {:.2} (only prices <=101 were lifted; the 102 ask survived, remainder cancelled)",
        report.avg_price().unwrap_or(0.0)
    );
    for (px, qty, n) in engine.book().depth(Side::Sell) {
        println!("   remaining ask price {px}: qty {qty} across {n} order(s)");
    }
}
