//! A demo order system: connects to the exchange server over a persistent TCP
//! connection, streams a scripted sequence of orders across two instruments
//! (including a cancel and a modify), and prints the execution reports streamed
//! back asynchronously.
//!
//! Usage:
//!   cargo run --release --bin order_client -- [ADDR]
//!     ADDR  default 127.0.0.1:9001
//!
//! Start `exchange_server` first.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use trade_core::prelude::*;
use trade_core::wire::{self, MSG_LEN, REPORT_LEN};
use trade_core::InstrumentId;

fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9001".to_string());
    let mut sock = TcpStream::connect(&addr).unwrap_or_else(|e| {
        eprintln!("cannot connect to {addr}: {e}");
        std::process::exit(1);
    });
    sock.set_nodelay(true).ok();
    println!("[client] connected to {addr}");

    let aapl = InstrumentId(1);
    let btc = InstrumentId(2);
    let mut frame = [0u8; MSG_LEN];

    // Helper closures to send framed commands.
    let send = |bytes: &[u8; MSG_LEN], sock: &mut TcpStream| sock.write_all(bytes).unwrap();

    // --- Scripted flow ---------------------------------------------------
    // AAPL: rest two asks, cancel one, then a buy crosses the survivor.
    wire::encode_new(&Order::limit(OrderId(1), Side::Sell, 100, 5).on(aapl), &mut frame);
    send(&frame, &mut sock);
    wire::encode_new(&Order::limit(OrderId(2), Side::Sell, 100, 5).on(aapl), &mut frame);
    send(&frame, &mut sock);
    wire::encode_cancel(aapl, OrderId(1), 4, &mut frame); // cancel (admin cmd_id 4)
    send(&frame, &mut sock);
    wire::encode_new(&Order::limit(OrderId(3), Side::Buy, 100, 5).on(aapl), &mut frame);
    send(&frame, &mut sock); // should trade against #2, not the cancelled #1

    // BTC: rest a bid, modify it up (loses priority / re-quote), then a sell hits.
    // (The demo server's price guard references 1000, so stay in-band.)
    wire::encode_new(&Order::limit(OrderId(10), Side::Buy, 1000, 2).on(btc), &mut frame);
    send(&frame, &mut sock);
    wire::encode_modify(btc, OrderId(10), 1001, 2, 14, &mut frame); // raise price (admin cmd_id 14)
    send(&frame, &mut sock);
    wire::encode_new(&Order::limit(OrderId(11), Side::Sell, 1000, 2).on(btc), &mut frame);
    send(&frame, &mut sock);

    // Deliberately fire a buy far through the price band: the anti-spike guard
    // must reject it (watch for REJECTED in the report stream).
    wire::encode_new(&Order::limit(OrderId(12), Side::Buy, 999_999, 1).on(btc), &mut frame);
    send(&frame, &mut sock);

    // Risk demo: force-close user 42 on BTC (no position: pure cancel sweep).
    // The brief pause lets #13 rest first — force-close rides the high-priority
    // queue and would otherwise overtake it (that priority is the point).
    wire::encode_new(&Order::limit(OrderId(13), Side::Buy, 990, 3).on(btc).by(42), &mut frame);
    send(&frame, &mut sock);
    std::thread::sleep(Duration::from_millis(50));
    wire::encode_force_close(btc, 42, OrderId(14), Side::Sell, 0, &mut frame);
    send(&frame, &mut sock);

    // --- Read reports for a short while ----------------------------------
    sock.set_read_timeout(Some(Duration::from_millis(300))).ok();
    let mut buf = [0u8; REPORT_LEN * 32];
    let mut filled = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_millis(1500);
    println!("[client] execution reports:");
    while std::time::Instant::now() < deadline {
        match sock.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => {
                filled += n;
                let mut off = 0;
                while filled - off >= REPORT_LEN {
                    if let Some(r) = wire::decode_report(&buf[off..off + REPORT_LEN]) {
                        println!("   {r}");
                    }
                    off += REPORT_LEN;
                }
                if off > 0 {
                    buf.copy_within(off..filled, 0);
                    filled -= off;
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => {
                eprintln!("[client] read error: {e}");
                break;
            }
        }
    }
    println!("[client] done");
}
