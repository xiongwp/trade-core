//! End-to-end test of the network path: a client streams binary order frames
//! over TCP into the gateway, and receives execution-report frames back.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use trade_core::exchange::{build, ExchangeConfig};
use trade_core::gateway;
use trade_core::prelude::*;
use trade_core::wire::{self, DecodedReport, MSG_LEN, REPORT_LEN};
use trade_core::InstrumentId;

#[test]
fn client_streams_orders_and_receives_reports_over_tcp() {
    // Bind an ephemeral port and start the exchange + gateway server thread.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (gw, sink, handle) = build(ExchangeConfig {
        shards: 2,
        queue_capacity: 4096,
        strategy: || Box::new(PriceTimePriority),
        ..ExchangeConfig::default()
    });
    let running = Arc::new(AtomicBool::new(true));
    let server_running = running.clone();
    let server = std::thread::spawn(move || {
        let _ = gateway::serve_on(listener, gw, sink, server_running);
    });

    // Client connects (the long-lived order-system connection).
    let mut client = TcpStream::connect(addr).unwrap();
    client.set_nodelay(true).unwrap();

    // Send: rest an ask, then a crossing buy, on instrument 7.
    let sym = InstrumentId(7);
    let mut frame = [0u8; MSG_LEN];
    wire::encode_new(
        &Order::limit(OrderId(1), Side::Sell, 100, 5).on(sym),
        &mut frame,
    );
    client.write_all(&frame).unwrap();
    wire::encode_new(
        &Order::limit(OrderId(2), Side::Buy, 100, 5).on(sym),
        &mut frame,
    );
    client.write_all(&frame).unwrap();

    // Read report frames back until we see the taker Filled or time out.
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut reports: Vec<DecodedReport> = Vec::new();
    let mut buf = [0u8; REPORT_LEN * 16];
    let mut filled = 0usize;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_fill = false;

    while Instant::now() < deadline && !saw_fill {
        match client.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => {
                filled += n;
                let mut off = 0;
                while filled - off >= REPORT_LEN {
                    let r = wire::decode_report(&buf[off..off + REPORT_LEN]).unwrap();
                    if r.type_code == 3 && r.order_id == OrderId(2) {
                        saw_fill = true; // taker filled
                    }
                    reports.push(r);
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
            Err(_) => break,
        }
    }

    // Shut everything down.
    drop(client);
    handle.shutdown();
    let _ = server.join();

    assert!(
        saw_fill,
        "expected a Filled report for taker #2; got {reports:?}"
    );
    // A trade at price 100, qty 5, on instrument 7.
    let trade = reports
        .iter()
        .any(|r| r.type_code == 2 && r.instrument == sym && r.price == 100 && r.qty == 5);
    assert!(trade, "expected a TRADE report; got {reports:?}");
}
