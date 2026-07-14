//! A simulated order system: connects to the matching node over the standard
//! TCP order port and generates a continuous random-walk order flow across a
//! few instruments, so the market-data service has live trades to chart.
//!
//! Usage: sim_trader [ADDR] [ORDERS_PER_SEC]
//!   ADDR            default 127.0.0.1:9001
//!   ORDERS_PER_SEC  default 300

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use trade_core::prelude::*;
use trade_core::wire::{self, MSG_LEN};
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
    let rate: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(300);

    let mut sock = loop {
        match TcpStream::connect(&addr) {
            Ok(s) => break s,
            Err(e) => {
                eprintln!("[sim] waiting for exchange at {addr} ({e})");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    sock.set_nodelay(true).ok();
    eprintln!("[sim] connected to {addr}, streaming ~{rate} orders/s");

    // Drain and discard the report stream on a separate thread so the server's
    // writer never blocks on a full socket buffer.
    let mut reader = sock.try_clone().expect("clone socket");
    std::thread::spawn(move || {
        let mut buf = [0u8; 16384];
        while reader.read(&mut buf).map(|n| n > 0).unwrap_or(false) {}
    });

    // Per-instrument mid prices random-walking inside the server's demo price
    // band (reference 1000, ±10% => stay well inside [920, 1080]).
    let mut rng = Rng(0x51D0_CAFE ^ std::process::id() as u64);
    let instruments: Vec<InstrumentId> = (1..=4).map(InstrumentId).collect();
    let mut mids: Vec<i64> = vec![1000; instruments.len()];

    let tick = Duration::from_nanos(1_000_000_000 / rate.max(1));
    let mut next_send = Instant::now();
    let mut id: u64 = (std::process::id() as u64) << 40; // unique-ish id space
    let mut frame = [0u8; MSG_LEN];

    loop {
        std::thread::sleep(next_send.saturating_duration_since(Instant::now()));
        next_send += tick;

        let k = (rng.next() % instruments.len() as u64) as usize;
        let sym = instruments[k];
        // Random walk, softly pulled back towards 1000.
        mids[k] += (rng.range(0, 6) as i64 - 3) + (1000 - mids[k]).signum();
        mids[k] = mids[k].clamp(925, 1075);
        let mid = mids[k] as u64;

        id += 1;
        let user = rng.range(1, 50);
        let side = if rng.next() & 1 == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let qty = rng.range(1, 20);

        // 70% passive quotes around the mid, 30% marketable orders that cross
        // the spread and print trades.
        let order = if rng.next() % 10 < 7 {
            let off = rng.range(1, 5);
            let price = match side {
                Side::Buy => mid - off,
                Side::Sell => mid + off,
            };
            Order::limit(OrderId(id), side, price, qty).on(sym).by(user)
        } else {
            let price = match side {
                Side::Buy => mid + rng.range(0, 3),
                Side::Sell => mid - rng.range(0, 3),
            };
            Order::limit(OrderId(id), side, price, qty)
                .on(sym)
                .by(user)
                .with_tif(TimeInForce::Ioc)
        };

        wire::encode_new(&order, &mut frame);
        if sock.write_all(&frame).is_err() {
            eprintln!("[sim] exchange connection lost, exiting");
            return;
        }

        // Occasionally cancel something old to keep the books tidy.
        if rng.next() % 16 == 0 {
            id += 1;
            wire::encode_cancel(
                sym,
                OrderId(id - rng.range(2, 200).min(id - 1)),
                id,
                &mut frame,
            );
            let _ = sock.write_all(&frame);
        }
    }
}
