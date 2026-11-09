//! Inspect and replay order-api dead-letter files
//! (`TC_ORDER_DLQ_PATH`, `TC_ORDER_COMMAND_DLQ_PATH`).
//!
//! A message is dead-lettered when it cannot be applied — undecodable, or
//! routed for another configuration (a route version or partition count
//! mismatch during a deploy) — instead of being skipped silently. Once the
//! cause is fixed, replaying puts each one back on the topic and partition it
//! came from, where the consumer applies it like any redelivery (projection
//! and matching both deduplicate, so replaying twice is harmless).
//!
//!   dlq-tool list   FILE                                one line per record
//!   dlq-tool replay FILE BROKERS [--commands] [--yes]   re-publish (dry run without --yes)
//!
//! Executions are idempotent downstream. A dead-lettered *command* is matched
//! again if its id has left the engine's dedupe window, so replaying command
//! records (anything not from the execution topic, `TC_EXECUTION_KAFKA_TOPIC`) needs `--commands`: use
//! it only for commands known never to have been applied.
//!
//! Replay writes a `FILE.replayed` marker with the count, so a second replay
//! of the same file is refused unless the marker is removed.

use std::io::{self, Read};
use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

const DLQ_HEADER: [u8; 8] = *b"TCDLQ02\0";

struct Record {
    at_nanos: u64,
    topic: String,
    partition: i32,
    offset: i64,
    reason: String,
    payload: Vec<u8>,
}

fn fnv1a(bytes: &[u8]) -> u64 {
    trade_core::journal::fnv1a(bytes)
}

/// Every complete record; a torn tail (a crash mid-append) ends the list.
fn read(path: &str) -> io::Result<Vec<Record>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < 8 || bytes[..8] != DLQ_HEADER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a TCDLQ02 dead-letter file",
        ));
    }
    let mut records = Vec::new();
    let mut at = 8;
    loop {
        let Some(len) = bytes
            .get(at..at + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
        else {
            break;
        };
        let Some(body) = bytes.get(at + 4..at + 4 + len) else {
            break;
        };
        let Some(sum) = bytes.get(at + 4 + len..at + 12 + len) else {
            break;
        };
        if fnv1a(body) != u64::from_le_bytes(sum.try_into().unwrap()) {
            eprintln!("record at byte {at}: checksum mismatch; stopping here");
            break;
        }
        at += 12 + len;
        let mut cursor = 0usize;
        let mut take = |n: usize| -> io::Result<&[u8]> {
            let slice = body
                .get(cursor..cursor + n)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short record"))?;
            cursor += n;
            Ok(slice)
        };
        let at_nanos = u64::from_le_bytes(take(8)?.try_into().unwrap());
        let partition = i32::from_le_bytes(take(4)?.try_into().unwrap());
        let offset = i64::from_le_bytes(take(8)?.try_into().unwrap());
        let topic_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let topic = String::from_utf8_lossy(take(topic_len)?).into_owned();
        let reason_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let reason = String::from_utf8_lossy(take(reason_len)?).into_owned();
        let payload_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let payload = take(payload_len)?.to_vec();
        records.push(Record {
            at_nanos,
            topic,
            partition,
            offset,
            reason,
            payload,
        });
    }
    Ok(records)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: dlq-tool list FILE | dlq-tool replay FILE BROKERS [--commands] [--yes]";
    let (Some(command), Some(file)) = (args.first(), args.get(1)) else {
        eprintln!("{usage}");
        std::process::exit(2);
    };
    let records = read(file).unwrap_or_else(|error| {
        eprintln!("{file}: {error}");
        std::process::exit(1);
    });
    match command.as_str() {
        "list" => {
            for record in &records {
                println!(
                    "{} {}[{}]@{} {} bytes: {}",
                    record.at_nanos / 1_000_000_000,
                    record.topic,
                    record.partition,
                    record.offset,
                    record.payload.len(),
                    record.reason
                );
            }
            println!("{} record(s)", records.len());
        }
        "replay" => {
            let Some(brokers) = args.get(2) else {
                eprintln!("{usage}");
                std::process::exit(2);
            };
            let confirmed = args.iter().any(|arg| arg == "--yes");
            let execution_topic = std::env::var("TC_EXECUTION_KAFKA_TOPIC")
                .unwrap_or_else(|_| "trade-executions-v1".into());
            let commands = records
                .iter()
                .filter(|record| record.topic != execution_topic)
                .count();
            if commands > 0 && !args.iter().any(|arg| arg == "--commands") {
                eprintln!(
                    "{commands} record(s) are commands: replaying one whose id left the dedupe window \
                     executes it again. Add --commands only if they were never applied."
                );
                std::process::exit(1);
            }
            let marker = format!("{file}.replayed");
            if std::path::Path::new(&marker).exists() {
                eprintln!("{marker} exists: this file was replayed already (remove the marker to replay again)");
                std::process::exit(1);
            }
            if !confirmed {
                for record in &records {
                    println!(
                        "would replay {}[{}] (from offset {})",
                        record.topic, record.partition, record.offset
                    );
                }
                println!(
                    "{} record(s); dry run — add --yes to publish",
                    records.len()
                );
                return;
            }
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("acks", "all")
                .set("enable.idempotence", "true")
                .create()
                .expect("create producer");
            let mut replayed = 0usize;
            for record in &records {
                let sent = futures::executor::block_on(
                    producer.send(
                        FutureRecord::<(), _>::to(&record.topic)
                            .partition(record.partition)
                            .payload(&record.payload),
                        Duration::from_secs(10),
                    ),
                );
                match sent {
                    Ok(delivery) => {
                        replayed += 1;
                        println!(
                            "replayed {}[{}]@{} -> @{}",
                            record.topic, record.partition, record.offset, delivery.offset
                        );
                    }
                    Err((error, _)) => {
                        eprintln!(
                            "failed at record {replayed}: {error}; nothing after it was sent"
                        );
                        break;
                    }
                }
            }
            std::fs::write(
                &marker,
                format!("{replayed} of {} records replayed\n", records.len()),
            )
            .expect("write replay marker");
            println!("{replayed} of {} record(s) replayed", records.len());
            if replayed != records.len() {
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    }
}
