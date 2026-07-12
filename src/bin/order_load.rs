//! The order-system load generator for end-to-end stress testing:
//!
//! 1. **Generate** N orders (default 20,000,000) for ~100k users and shard
//!    them into the order system's **10 databases × 100 tables** by user id
//!    (in-memory buckets standing in for the MySQL tables — the routing is the
//!    production `sharding::route`).
//! 2. **Stream** them to the matching node over one long-lived TCP connection
//!    (frames pre-encoded, written in large batches), while a reader thread
//!    counts execution reports.
//! 3. **Measure** end-to-end: submit → match → report received.
//!
//! While this runs, the market-data service keeps aggregating candles from the
//! fanout port, so the K-line chart shows the stress flow live.
//!
//! Usage: order_load [ADDR] [ORDERS]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use trade_core::prelude::*;
use trade_core::sharding::{self, DB_COUNT, TABLES_PER_DB};
use trade_core::wire::{self, MSG_LEN, REPORT_LEN};
use trade_core::InstrumentId;

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

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:9001".to_string());
    let total: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(20_000_000);

    // ---- Phase 1: generate and shard into 10 DBs x 100 tables --------------
    println!("[load] phase 1: generating {total} orders into 10 DBs x 100 tables…");
    let t0 = Instant::now();
    let slots = (DB_COUNT * TABLES_PER_DB) as usize;
    // Tables hold row COUNTS (the sharded-storage demo); frames stream in
    // SUBMISSION order — ids are allocated at send time, so each shard sees a
    // monotonic id stream (the dedup contract).
    let mut tables: Vec<u64> = vec![0; slots];
    let mut flat: Vec<[u8; MSG_LEN]> = Vec::with_capacity(total as usize);

    let mut rng = Rng(0xE2E_2024);
    let mut frame = [0u8; MSG_LEN];
    for i in 1..=total {
        let user = rng.range(1, 100_000);
        let route = sharding::route(user);
        let slot = route.db as usize * TABLES_PER_DB as usize + route.table as usize;

        let sym = InstrumentId(1 + (rng.next() % 4) as u32);
        if i > 1000 && rng.next() % 7 == 0 {
            // Cancel an earlier order (may be already gone -> NotFound, fine).
            wire::encode_cancel(sym, OrderId(rng.range(1, i - 1)), i, &mut frame);
        } else {
            let side = if rng.next() & 1 == 0 { Side::Buy } else { Side::Sell };
            let order = Order::limit(OrderId(i), side, rng.range(990, 1010), rng.range(1, 50))
                .on(sym)
                .by(user);
            wire::encode_new(&order, &mut frame);
        }
        tables[slot] += 1;
        flat.push(frame);
    }
    let per_db: Vec<u64> = (0..DB_COUNT as usize)
        .map(|db| (0..TABLES_PER_DB as usize).map(|t| tables[db * TABLES_PER_DB as usize + t]).sum())
        .collect();
    let (tmin, tmax) = tables.iter().fold((u64::MAX, 0u64), |(lo, hi), &t| (lo.min(t), hi.max(t)));
    println!(
        "[load] generated in {:.1?}; per-DB rows: {:?}",
        t0.elapsed(),
        per_db
    );
    println!(
        "[load] table skew: min {tmin} / max {tmax} rows over {slots} tables ({:.1}% dev)",
        (tmax - tmin) as f64 / (total as f64 / slots as f64) * 100.0 / 2.0
    );

    // ---- Phase 2: stream via TCP, count reports ----------------------------
    println!("[load] phase 2: streaming to {addr}…");
    let mut sock = TcpStream::connect(&addr).expect("connect exchange");
    sock.set_nodelay(true).ok();

    let terminals = Arc::new(AtomicU64::new(0));
    let trades = Arc::new(AtomicU64::new(0));
    let (t_term, t_trade) = (terminals.clone(), trades.clone());
    let mut rsock = sock.try_clone().expect("clone");
    let reader = std::thread::spawn(move || {
        let mut buf = vec![0u8; REPORT_LEN * 4096];
        let mut filled = 0usize;
        let mut term = 0u64;
        let mut trd = 0u64;
        loop {
            match rsock.read(&mut buf[filled..]) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    filled += n;
                    let mut off = 0;
                    while filled - off >= REPORT_LEN {
                        // type codes: 2=Trade; 3,4,5,6,7,9 are terminal.
                        match buf[off] {
                            2 => trd += 1,
                            3..=7 | 9 => term += 1,
                            _ => {}
                        }
                        off += REPORT_LEN;
                    }
                    if off > 0 {
                        buf.copy_within(off..filled, 0);
                        filled -= off;
                    }
                    if term / 1_000_000 != t_term.load(Ordering::Relaxed) / 1_000_000 {
                        eprintln!("[load] progress: {term} terminals, {trd} trades");
                    }
                    t_term.store(term, Ordering::Release);
                    t_trade.store(trd, Ordering::Release);
                }
            }
        }
    });

    // Stream in submission (id) order, in 4096-frame (160 KiB) batches.
    let t1 = Instant::now();
    let mut batch: Vec<u8> = Vec::with_capacity(MSG_LEN * 4096);
    let mut sent = 0u64;
    for f in &flat {
        batch.extend_from_slice(f);
        if batch.len() >= MSG_LEN * 4096 {
            sock.write_all(&batch).expect("stream orders");
            sent += (batch.len() / MSG_LEN) as u64;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        sock.write_all(&batch).expect("stream orders");
        sent += (batch.len() / MSG_LEN) as u64;
    }
    let send_elapsed = t1.elapsed();
    println!(
        "[load] all {sent} commands written in {send_elapsed:.1?} \
         ({:.0}/s submit side)",
        sent as f64 / send_elapsed.as_secs_f64()
    );

    // Wait until every command's terminal report has come back.
    while terminals.load(Ordering::Acquire) < total {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let e2e = t1.elapsed();
    println!(
        "[load] END-TO-END: {total} orders in {e2e:.2?} -> {:.0} orders/s \
         ({} trades printed)",
        total as f64 / e2e.as_secs_f64(),
        trades.load(Ordering::Acquire)
    );
    // Unblock the reader (it shares the socket) and exit.
    let _ = sock.shutdown(std::net::Shutdown::Both);
    let _ = reader.join();
}
