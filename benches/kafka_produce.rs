//! Kafka write capacity with the matching nodes' producer settings
//! (idempotent, acks=all, lz4, 104-byte execution events): for calibrating
//! the broker count (docs/deployment-2m-tps.md section 9.3).
//!
//! Run: cargo bench --bench kafka_produce -- BROKERS TOPIC [SECONDS] [THREADS] [BATCH]
//!
//! The benchmark refuses known production topics. Set
//! `TC_ALLOW_PRODUCTION_BENCHMARK_TOPIC=1` only for an intentionally isolated
//! environment whose business data may be destroyed.
//!
//! Each thread sends batches of BATCH messages to rotating partitions and
//! waits for every acknowledgement before the next batch, as the outbox
//! publisher does. Reports acknowledged messages/s and MB/s (uncompressed).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

const MESSAGE_BYTES: usize = 104;
const DEFAULT_TOPIC: &str = "trade-benchmark-v1";
const PRODUCTION_TOPICS: &[&str] = &[
    "trade-executions-v1",
    "trade-market-events-v1",
    "trade-order-db-v1",
    "trade-user-events-v1",
    "trade-execution-outbox-cursors-v1",
    "trade-route-control-v1",
];

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let brokers = args
        .first()
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:19092".into());
    let topic = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| DEFAULT_TOPIC.into());
    let production_override = std::env::var("TC_ALLOW_PRODUCTION_BENCHMARK_TOPIC")
        .is_ok_and(|value| value == "1");
    assert!(
        production_override
            || (!PRODUCTION_TOPICS.contains(&topic.as_str()) && !topic.starts_with("orders-v2-g")),
        "refusing to publish benchmark payloads to production topic {topic}; use {DEFAULT_TOPIC}"
    );
    let seconds: u64 = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(60);
    let threads: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(8);
    let batch: usize = args.get(4).and_then(|v| v.parse().ok()).unwrap_or(4096);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("compression.type", "lz4")
        .set("linger.ms", "2")
        .set("batch.num.messages", "10000")
        .set("queue.buffering.max.messages", "2000000")
        .set("queue.buffering.max.kbytes", "1048576")
        .create()
        .expect("create producer");
    let partitions = {
        use rdkafka::producer::Producer;
        producer
            .client()
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .expect("topic metadata")
            .topics()[0]
            .partitions()
            .len() as i32
    };
    assert!(partitions > 0, "topic {topic} has no partitions");
    println!("{brokers} {topic}: {partitions} partitions, {threads} threads x {batch} per batch, {seconds} s");

    let acked = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (producer, topic, acked, failed) = (
                producer.clone(),
                topic.clone(),
                acked.clone(),
                failed.clone(),
            );
            std::thread::spawn(move || {
                // Realistic but incompressible-ish payloads: ids and prices vary.
                let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ thread as u64;
                let mut payloads = Vec::with_capacity(batch);
                let mut sent = 0u64;
                while Instant::now() < deadline {
                    payloads.clear();
                    for _ in 0..batch {
                        let mut payload = [0u8; MESSAGE_BYTES];
                        for chunk in payload.chunks_mut(8) {
                            seed ^= seed << 13;
                            seed ^= seed >> 7;
                            seed ^= seed << 17;
                            let n = chunk.len();
                            chunk.copy_from_slice(&(seed % 1_000_000).to_le_bytes()[..n]);
                        }
                        payloads.push(payload);
                    }
                    let keys: Vec<[u8; 8]> = (0..batch as u64)
                        .map(|k| (sent + k).to_be_bytes())
                        .collect();
                    let results = futures::executor::block_on(futures::future::join_all(
                        payloads
                            .iter()
                            .zip(&keys)
                            .enumerate()
                            .map(|(k, (payload, key))| {
                                producer.send(
                                    FutureRecord::to(&topic)
                                        .partition(((sent + k as u64) % partitions as u64) as i32)
                                        .key(key)
                                        .payload(&payload[..]),
                                    Duration::from_secs(10),
                                )
                            }),
                    ));
                    let ok = results.iter().filter(|r| r.is_ok()).count() as u64;
                    acked.fetch_add(ok, Ordering::Relaxed);
                    failed.fetch_add(results.len() as u64 - ok, Ordering::Relaxed);
                    sent += batch as u64;
                }
            })
        })
        .collect();
    let started = Instant::now();
    let mut last = 0;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_secs(5));
        let now = acked.load(Ordering::Relaxed);
        let rate = (now - last) as f64 / 5.0;
        println!(
            "  +{:4.0}s {:>12.0} msg/s {:>8.1} MB/s",
            started.elapsed().as_secs_f64(),
            rate,
            rate * MESSAGE_BYTES as f64 / 1e6
        );
        last = now;
    }
    for worker in workers {
        worker.join().unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64();
    let total = acked.load(Ordering::Relaxed);
    println!(
        "acknowledged {total} messages in {elapsed:.0} s: {:.0} msg/s, {:.1} MB/s; failed {}",
        total as f64 / elapsed,
        total as f64 * MESSAGE_BYTES as f64 / elapsed / 1e6,
        failed.load(Ordering::Relaxed)
    );
}
