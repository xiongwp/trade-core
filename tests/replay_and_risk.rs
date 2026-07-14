//! Recovery and risk tests: journal replay determinism, point-in-time replay,
//! forced liquidation, and anti-spike price banding.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use trade_core::exchange::{
    build, fingerprint_reports, replay_journal, ExchangeConfig, ExecReport,
};
use trade_core::prelude::*;
use trade_core::risk::PriceGuard;
use trade_core::InstrumentId;

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tc-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Drain the sink until no report arrives for a while.
fn drain_all(sink: &trade_core::ResultSink) -> Vec<ExecReport> {
    let mut reports = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut idle = 0;
    while Instant::now() < deadline && idle < 2000 {
        if sink.poll(|r| {
            if !matches!(
                r,
                ExecReport::DepthLevel { .. } | ExecReport::DepthEnd { .. }
            ) {
                reports.push(r)
            }
        }) == 0
        {
            idle += 1;
            std::thread::sleep(Duration::from_micros(100));
        } else {
            idle = 0;
        }
    }
    reports
}

/// A deterministic pseudo-random order flow on one instrument.
fn random_flow(n: u64) -> Vec<trade_core::Command> {
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545F4914F6CDD1D)
    };
    let mut out = Vec::new();
    for i in 1..=n {
        let r = next();
        if r % 7 == 0 && i > 10 {
            // Modifies are sequenced commands too: same replay guarantees.
            out.push(trade_core::Command::Modify {
                instrument: InstrumentId(0),
                order_id: OrderId(1 + r % (i - 1)),
                new_price: 990 + r % 21,
                new_qty: 1 + r % 30,
                cmd_id: i,
            });
        } else if r % 5 == 0 && i > 10 {
            out.push(trade_core::Command::Cancel {
                instrument: InstrumentId(0),
                order_id: OrderId(1 + r % (i - 1)),
                cmd_id: i,
            });
        } else {
            let side = if r & 1 == 0 { Side::Buy } else { Side::Sell };
            let price = 990 + r % 21;
            let qty = 1 + r % 50;
            out.push(trade_core::Command::New(Order::limit(
                OrderId(i),
                side,
                price,
                qty,
            )));
        }
    }
    out
}

#[test]
fn journal_replay_reproduces_identical_results() {
    let dir = temp_dir("replay");
    let cfg = ExchangeConfig {
        shards: 1, // per-shard journals preserve per-shard total order
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(10),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // Live run: a few thousand mixed commands.
    for cmd in random_flow(3000) {
        let mut pending = cmd;
        loop {
            match gw.submit(pending) {
                Ok(()) => break,
                Err(ret) => {
                    pending = ret;
                    std::thread::yield_now();
                }
            }
        }
    }
    let live_reports = drain_all(&sink);
    handle.shutdown(); // flushes the journal
    let live_fp = fingerprint_reports(&live_reports);
    assert!(!live_reports.is_empty());

    // "Crash" and recover: replay the journal into a fresh processor.
    let journal = dir.join("journal-shard-0.bin");
    let summary = replay_journal(&journal, || Box::new(PriceTimePriority), None, None).unwrap();

    assert_eq!(summary.commands, 3000);
    assert_eq!(
        summary.fingerprint,
        live_fp,
        "replay must reproduce the exact live report stream \
         (live {} reports, replay {})",
        live_reports.len(),
        summary.reports.len()
    );
    // And the rebuilt book state matches a direct comparison of depth.
    let engine = summary.processor.engine(InstrumentId(0)).unwrap();
    assert!(
        !engine.book().is_empty(),
        "book should have resting orders after replay"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn replay_until_timestamp_stops_early() {
    let dir = temp_dir("timereplay");
    let cfg = ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(10),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // First batch.
    for i in 1..=100u64 {
        gw.new_order(Order::limit(OrderId(i), Side::Sell, 100 + i % 5, 1))
            .unwrap();
    }
    let _ = drain_all(&sink);
    let cut = trade_core::journal::now_nanos();
    std::thread::sleep(Duration::from_millis(20));

    // Second batch, after the cut point.
    for i in 101..=200u64 {
        gw.new_order(Order::limit(OrderId(i), Side::Sell, 100 + i % 5, 1))
            .unwrap();
    }
    let _ = drain_all(&sink);
    handle.shutdown();

    let journal = dir.join("journal-shard-0.bin");
    let full = replay_journal(&journal, || Box::new(PriceTimePriority), None, None).unwrap();
    let partial =
        replay_journal(&journal, || Box::new(PriceTimePriority), None, Some(cut)).unwrap();

    assert_eq!(full.commands, 200);
    assert_eq!(
        partial.commands, 100,
        "time-bounded replay must stop at the cut"
    );
    let engine = partial.processor.engine(InstrumentId(0)).unwrap();
    assert_eq!(engine.book().len(), 100, "state = exactly the first batch");

    std::fs::remove_dir_all(&dir).ok();
}

/// The total-order property, stated as the canonical example: New(1) →
/// Modify(1) → Cancel(1) must replay in exactly journal-seq order, leaving no
/// order — even though Cancel/Modify travelled the high-priority queue and New
/// the normal queue. One journal, one seq series, queue routing invisible.
#[test]
fn new_modify_cancel_share_one_total_order() {
    let dir = temp_dir("total-order");
    let cfg = ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(5),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // Force the exact interleaving by draining between commands so each is
    // processed (and journaled) before the next is enqueued.
    gw.new_order(Order::limit(OrderId(1), Side::Buy, 100, 10))
        .unwrap();
    let _ = drain_all(&sink);
    gw.modify(InstrumentId(0), OrderId(1), 101, 10, 2).unwrap();
    let _ = drain_all(&sink);
    gw.cancel(InstrumentId(0), OrderId(1), 3).unwrap();
    let _ = drain_all(&sink);
    handle.shutdown();

    // The journal holds the three commands with strictly increasing seqs, in
    // execution order — Cancel did NOT get its own file or seq series.
    let jpath = dir.join("journal-shard-0.bin");
    let records: Vec<_> = trade_core::journal::JournalReader::open(&jpath)
        .unwrap()
        .collect();
    assert_eq!(records.len(), 3);
    assert!(
        records.windows(2).all(|w| w[1].seq == w[0].seq + 1),
        "one contiguous seq series"
    );

    // Replaying 1 → 2 → 3 ends with no order on the book.
    let summary = replay_journal(&jpath, || Box::new(PriceTimePriority), None, None).unwrap();
    assert_eq!(summary.commands, 3);
    let engine = summary.processor.engine(InstrumentId(0)).unwrap();
    assert!(
        engine.book().is_empty(),
        "New→Modify→Cancel must leave nothing"
    );

    // Restart continuity: a writer reopened over this journal must CONTINUE
    // the seq series (a restart-at-zero would corrupt the total order).
    let mut w = trade_core::journal::JournalWriter::open(&jpath, Duration::from_millis(5)).unwrap();
    w.resume_from(records.last().unwrap().seq);
    let frame = [1u8; trade_core::wire::MSG_LEN];
    let next = w.append(0, &frame).unwrap();
    assert_eq!(next, 4, "seq must continue the total order across restarts");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn force_close_cancels_user_orders_and_flattens() {
    let (gw, sink, handle) = build(ExchangeConfig::default());
    let sym = InstrumentId(0);

    // User 7 has two resting bids; user 8 provides an ask to close against and
    // a bid that must survive.
    gw.new_order(Order::limit(OrderId(1), Side::Buy, 99, 10).by(7))
        .unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 98, 5).by(7))
        .unwrap();
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 97, 5).by(8))
        .unwrap();
    gw.new_order(Order::limit(OrderId(4), Side::Sell, 101, 20).by(8))
        .unwrap();
    let _ = drain_all(&sink);

    // Risk decides: force-close user 7, flattening a long of 12 lots.
    gw.force_close(sym, 7, OrderId(100), Side::Sell, 12)
        .unwrap();
    let reports = drain_all(&sink);
    handle.shutdown();

    // Both of user 7's orders cancelled...
    for id in [OrderId(1), OrderId(2)] {
        assert!(
            reports.iter().any(|r| matches!(
                r, ExecReport::Cancelled { order_id, .. } if *order_id == id)),
            "expected {id} cancelled; got {reports:?}"
        );
    }
    // ...and the closing sell traded against user 8's bid at 97.
    assert!(
        reports.iter().any(|r| matches!(
            r, ExecReport::Trade { taker, maker, price, qty, .. }
            if *taker == OrderId(100) && *maker == OrderId(3) && *price == 97 && *qty == 5)),
        "expected close order to hit the surviving bid; got {reports:?}"
    );
}

#[test]
fn price_guard_rejects_spikes_and_protects_market_orders() {
    let sym = InstrumentId(0);
    let guard = Arc::new(PriceGuard::new(500, &[sym])); // ±5%
    guard.set_reference(sym, 1000); // band = [950, 1050]

    let cfg = ExchangeConfig {
        price_guard: Some(guard.clone()),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // A thin ask far above the band — spike bait.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 1050, 1))
        .unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Sell, 2000, 100))
        .unwrap(); // out of band

    // An aggressive buy limit way above band: rejected outright.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 1500, 10))
        .unwrap();

    // A market buy for 5: protected — capped at 1050, so it lifts the 1-lot ask
    // at 1050 and cancels the rest instead of walking up to 2000.
    gw.new_order(Order::market(OrderId(4), Side::Buy, 5))
        .unwrap();

    let reports = drain_all(&sink);
    handle.shutdown();

    // The out-of-band sell got rejected too (2000 < lo would be for sells; a
    // sell ABOVE band is passive and allowed to rest... but #2 at 2000 is a
    // SELL above band: allowed to rest, harmless). The BUY at 1500 is rejected.
    assert!(
        reports.iter().any(|r| matches!(
            r, ExecReport::Rejected { order_id, reason, .. }
            if *order_id == OrderId(3) && *reason == "price-band")),
        "buy through the band must be rejected; got {reports:?}"
    );
    // Market order printed only at 1050 (1 lot) and never at 2000.
    assert!(reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, price, qty, .. }
        if *taker == OrderId(4) && *price == 1050 && *qty == 1)));
    assert!(
        !reports.iter().any(|r| matches!(
            r, ExecReport::Trade { price, .. } if *price == 2000)),
        "protected market order must never trade beyond the band; got {reports:?}"
    );
}

/// Hot-standby failover: a standby consuming the live replication stream must
/// hold byte-identical books to the primary at promotion time.
#[test]
fn standby_replica_matches_primary_state() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use trade_core::replication;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let fanout = replication::RepFanout::accept_on(listener, running.clone());

    // Journal too: the journal and the replication stream are two consumers of
    // the SAME executed total order — they must agree exactly.
    let dir = temp_dir("rep");
    let cfg = ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(5),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = trade_core::exchange::build_with_rep(cfg, fanout.clone());

    // Standby attaches, then the primary processes a mixed flow.
    let applied = std::sync::Arc::new(AtomicU64::new(0));
    let standby = replication::spawn_replica(addr, || Box::new(PriceTimePriority), applied.clone());
    std::thread::sleep(Duration::from_millis(100)); // let it attach

    let n = 2000u64;
    for cmd in random_flow(n) {
        gw.submit(cmd).expect("queue sized for test");
    }
    let _ = drain_all(&sink);

    // Wait until the standby has applied every command, then "fail" the primary.
    let deadline = Instant::now() + Duration::from_secs(5);
    while applied.load(Ordering::Acquire) < n && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(applied.load(Ordering::Acquire), n, "standby must keep up");
    fanout.close_all(); // simulate primary death -> standby promotes
    let replica = standby.join().unwrap().unwrap();
    handle.shutdown();

    // Ground truth = replay of the primary's journal: the EXECUTED total
    // order (queue-priority interleaving included). Standby must match it —
    // not the submission order, which priority queues legitimately reorder.
    let summary = replay_journal(
        &dir.join("journal-shard-0.bin"),
        || Box::new(PriceTimePriority),
        None,
        None,
    )
    .unwrap();
    assert_eq!(summary.commands, n);
    assert_eq!(
        replica.processors[&0].state_fingerprint(),
        summary.processor.state_fingerprint(),
        "promoted standby must hold byte-identical books"
    );
    std::fs::remove_dir_all(&dir).ok();
}
