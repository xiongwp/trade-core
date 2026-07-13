//! Acceptance tests for snapshot recovery, self-trade prevention, and static
//! pre-trade risk limits.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use trade_core::exchange::{build, recover_into, ExchangeConfig, ExecReport, Processor};
use trade_core::prelude::*;
use trade_core::{Command, InstrumentId, RiskLimits, SelfTradePolicy};

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tc-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

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
            std::thread::yield_now();
        } else {
            idle = 0;
        }
    }
    reports
}

fn flow(seed: u64, id_base: u64, n: u64) -> Vec<Command> {
    let mut state = seed;
    let mut next = move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545F4914F6CDD1D)
    };
    (1..=n)
        .map(|i| {
            let r = next();
            let side = if r & 1 == 0 { Side::Buy } else { Side::Sell };
            Command::New(Order::limit(
                OrderId(id_base + i),
                side,
                990 + r % 21,
                1 + r % 50,
            ))
        })
        .collect()
}

/// The core snapshot property: (snapshot at time T) + (journal after T) must
/// reconstruct exactly the state that a continuous run reaches.
#[test]
fn snapshot_plus_journal_tail_equals_continuous_state() {
    let dir = temp_dir("snap-rec");
    let cfg = ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(5),
        // Manual snapshots only (via snapshot_now), so the test controls timing.
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // Phase A, then snapshot (which also truncates the journal).
    for cmd in flow(0xAAAA, 0, 2000) {
        gw.submit(cmd).expect("queue sized for the test");
    }
    let _ = drain_all(&sink);
    handle.snapshot_now();
    std::thread::sleep(Duration::from_millis(100)); // let the shard snapshot

    // Phase B lands in the (now truncated) journal after the snapshot.
    for cmd in flow(0xBBBB, 10_000, 1500) {
        gw.submit(cmd).expect("queue sized for the test");
    }
    let _ = drain_all(&sink);
    handle.shutdown(); // "crash" point: journal flushed by shutdown

    // Ground truth: one continuous processor over the identical A+B commands.
    let mut truth = Processor::new(|| Box::new(PriceTimePriority), None);
    for cmd in flow(0xAAAA, 0, 2000)
        .into_iter()
        .chain(flow(0xBBBB, 10_000, 1500))
    {
        truth.process(cmd, &mut |_| {});
    }

    // Recover: snapshot + journal tail.
    let mut recovered = Processor::new(|| Box::new(PriceTimePriority), None);
    let applied = recover_into(
        &mut recovered,
        &dir.join("snapshot-shard-0.bin"),
        &dir.join("journal-shard-0.bin"),
    )
    .unwrap();

    assert!(
        applied <= 1500,
        "journal tail must only contain phase B ({applied} applied) — truncation worked"
    );
    assert_eq!(
        recovered.state_fingerprint(),
        truth.state_fingerprint(),
        "snapshot + tail must equal continuous execution"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Restart-in-place: build() itself must pick up snapshot + journal from disk.
#[test]
fn exchange_recovers_state_on_restart() {
    let dir = temp_dir("snap-restart");
    let mk = || ExchangeConfig {
        shards: 1,
        journal_dir: Some(dir.clone()),
        journal_flush: Duration::from_millis(5),
        snapshot_every: Some(Duration::from_secs(3600)), // final-snapshot on shutdown
        ..ExchangeConfig::default()
    };

    // First life: rest an order book, then shut down cleanly.
    let (gw, sink, handle) = build(mk());
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 105, 7))
        .unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 95, 3))
        .unwrap();
    let _ = drain_all(&sink);
    handle.shutdown();

    // Second life: the resting orders must be back; a crossing buy proves it.
    let (gw, sink, handle) = build(mk());
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 105, 7))
        .unwrap();
    let reports = drain_all(&sink);
    handle.shutdown();

    assert!(
        reports.iter().any(|r| matches!(
            r, ExecReport::Trade { taker, maker, price, qty, .. }
            if *taker == OrderId(3) && *maker == OrderId(1) && *price == 105 && *qty == 7)),
        "restart must restore the resting ask; got {reports:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn stp_cancel_taker_blocks_self_trade() {
    let cfg = ExchangeConfig {
        stp: SelfTradePolicy::CancelTaker,
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 5).by(7))
        .unwrap();
    // Same user crosses own order: taker cancelled, maker survives.
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 100, 5).by(7))
        .unwrap();
    // A different user CAN trade with it.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 100, 5).by(8))
        .unwrap();

    let reports = drain_all(&sink);
    handle.shutdown();

    assert!(
        !reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, .. } if *taker == OrderId(2))),
        "self-trade must not print: {reports:?}"
    );
    assert!(reports.contains(&ExecReport::Cancelled {
        instrument: InstrumentId(0),
        order_id: OrderId(2),
    }));
    assert!(reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, maker, .. }
        if *taker == OrderId(3) && *maker == OrderId(1))));
}

#[test]
fn stp_cancel_maker_pulls_resting_and_matches_rest() {
    let cfg = ExchangeConfig {
        stp: SelfTradePolicy::CancelMaker,
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);

    // User 7's stale quote sits ahead of user 8's at the same price.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 5).by(7))
        .unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Sell, 100, 5).by(8))
        .unwrap();
    // User 7 crosses: own maker #1 is cancelled, taker fills against #2.
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 100, 5).by(7))
        .unwrap();

    let reports = drain_all(&sink);
    handle.shutdown();

    assert!(
        reports.contains(&ExecReport::Cancelled {
            instrument: InstrumentId(0),
            order_id: OrderId(1),
        }),
        "self maker must be cancelled: {reports:?}"
    );
    assert!(
        reports.iter().any(|r| matches!(
        r, ExecReport::Trade { taker, maker, qty, .. }
        if *taker == OrderId(3) && *maker == OrderId(2) && *qty == 5)),
        "taker must fill against the other user: {reports:?}"
    );
}

#[test]
fn risk_limits_reject_oversize_and_order_count() {
    let cfg = ExchangeConfig {
        risk_limits: Some(RiskLimits {
            max_order_qty: 100,
            max_notional: 50_000,
            max_user_orders: 2,
        }),
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);
    let sym = InstrumentId(0);

    gw.new_order(Order::limit(OrderId(1), Side::Buy, 10, 101))
        .unwrap(); // qty > 100
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 1000, 100))
        .unwrap(); // notional 100k > 50k
    gw.new_order(Order::limit(OrderId(3), Side::Buy, 10, 100).by(9))
        .unwrap(); // ok
    gw.new_order(Order::limit(OrderId(4), Side::Buy, 11, 100).by(9))
        .unwrap(); // ok
    gw.new_order(Order::limit(OrderId(5), Side::Buy, 12, 100).by(9))
        .unwrap(); // 3rd open order

    let reports = drain_all(&sink);
    handle.shutdown();

    let rejected_with = |id: u64, why: &str| {
        reports.iter().any(|r| {
            matches!(
            r, ExecReport::Rejected { order_id, reason, .. }
            if *order_id == OrderId(id) && *reason == why)
        })
    };
    assert!(rejected_with(1, "max-qty"), "{reports:?}");
    assert!(rejected_with(2, "max-notional"), "{reports:?}");
    assert!(rejected_with(5, "max-user-orders"), "{reports:?}");
    assert!(reports.contains(&ExecReport::Resting {
        instrument: sym,
        order_id: OrderId(4)
    }));
}

/// Idempotent dedup: a re-sent command (same id) must be rejected, never
/// applied twice; the cursor survives a snapshot round-trip.
#[test]
fn duplicate_commands_are_rejected_and_cursor_persists() {
    use trade_core::exchange::Processor;
    let mut p = Processor::new(|| Box::new(PriceTimePriority), None).with_dedup(true);
    let mut reports = Vec::new();

    // New #5 applies; a re-send of #5 is rejected as duplicate.
    p.process(
        Command::New(Order::limit(OrderId(5), Side::Sell, 100, 3)),
        &mut |r| reports.push(r),
    );
    p.process(
        Command::New(Order::limit(OrderId(5), Side::Sell, 100, 3)),
        &mut |r| reports.push(r),
    );
    assert!(reports.iter().any(|r| matches!(r,
        ExecReport::Rejected { reason, .. } if *reason == "duplicate")));
    assert_eq!(
        p.engine(InstrumentId(0)).unwrap().book().len(),
        1,
        "applied once"
    );

    // A cancel with a fresh cmd_id works; re-sending it is rejected.
    reports.clear();
    p.process(
        Command::Cancel {
            instrument: InstrumentId(0),
            order_id: OrderId(5),
            cmd_id: 6,
        },
        &mut |r| reports.push(r),
    );
    p.process(
        Command::Cancel {
            instrument: InstrumentId(0),
            order_id: OrderId(5),
            cmd_id: 6,
        },
        &mut |r| reports.push(r),
    );
    assert!(matches!(reports[0], ExecReport::Cancelled { .. }));
    assert!(matches!(
        reports[1],
        ExecReport::Rejected {
            reason: "duplicate",
            ..
        }
    ));

    // Cursor persists through snapshot: a fresh processor restored from it
    // still rejects old ids.
    let dir = temp_dir("dedup");
    let path = dir.join("s.bin");
    trade_core::snapshot::write(
        &path,
        trade_core::snapshot::SnapshotData {
            journal_seq: 0,
            max_cmd_id: 6,
            max_admin_id: 6,
            halted: &[],
            suspended: &[],
            positions: &[],
            engines: &p.export_state(),
        },
    )
    .unwrap();
    let snap = trade_core::snapshot::load(&path).unwrap();
    let mut p2 = Processor::new(|| Box::new(PriceTimePriority), None).with_dedup(true);
    p2.restore_state(&snap);
    reports.clear();
    p2.process(
        Command::New(Order::limit(OrderId(4), Side::Buy, 99, 1)),
        &mut |r| reports.push(r),
    );
    assert!(
        matches!(
            reports[0],
            ExecReport::Rejected {
                reason: "duplicate",
                ..
            }
        ),
        "restored cursor must still gate old ids"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Fees are computed in the matching path and carried on every trade report.
#[test]
fn trade_reports_carry_authoritative_fees() {
    let cfg = ExchangeConfig {
        fees: trade_core::fees::FeeSchedule {
            maker_bps: 10,
            taker_bps: 20,
        },
        ..ExchangeConfig::default()
    };
    let (gw, sink, handle) = build(cfg);
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 1000, 3))
        .unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 1000, 3))
        .unwrap();
    let reports = drain_all(&sink);
    handle.shutdown();
    // notional 3000: maker 10bps = 3, taker 20bps = 6.
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecReport::Trade {
                maker_fee: 3,
                taker_fee: 6,
                qty: 3,
                ..
            }
        )),
        "expected fee-bearing trade; got {reports:?}"
    );
}

/// Circuit breaker: Halt rejects new orders (cancels still work), Resume
/// restores trading; positions accrue from the trade stream.
#[test]
fn halt_resume_and_position_ledger() {
    let (gw, sink, handle) = build(ExchangeConfig::default());
    let sym = InstrumentId(0);

    // Trade 5 lots: user 7 sells to user 8 -> positions -5 / +5.
    gw.new_order(Order::limit(OrderId(1), Side::Sell, 100, 5).by(7))
        .unwrap();
    gw.new_order(Order::limit(OrderId(2), Side::Buy, 100, 5).by(8))
        .unwrap();
    gw.new_order(Order::limit(OrderId(3), Side::Sell, 101, 4).by(7))
        .unwrap(); // rests
    let _ = drain_all(&sink);

    // Halt: new orders rejected, cancel of the resting quote still allowed.
    // (Drain between steps: admin/cancel ride the high-priority queue and would
    // otherwise overtake the queued New — that priority is by design.)
    gw.halt(sym, 100).unwrap();
    let mut reports = drain_all(&sink);
    gw.new_order(Order::limit(OrderId(4), Side::Buy, 101, 1).by(8))
        .unwrap();
    gw.cancel(sym, OrderId(3), 101).unwrap();
    reports.extend(drain_all(&sink));
    // Resume: trading works again.
    gw.resume(sym, 102).unwrap();
    gw.new_order(Order::limit(OrderId(5), Side::Buy, 100, 1).by(8))
        .unwrap();
    reports.extend(drain_all(&sink));
    handle.shutdown();

    assert!(reports.contains(&ExecReport::Halted { instrument: sym }));
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecReport::Rejected {
                order_id: OrderId(4),
                reason: "halted",
                ..
            }
        )),
        "halted instrument must reject new orders: {reports:?}"
    );
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecReport::Cancelled {
                order_id: OrderId(3),
                ..
            }
        )),
        "cancels must still work while halted"
    );
    assert!(reports.contains(&ExecReport::Resumed { instrument: sym }));
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecReport::Resting {
                order_id: OrderId(5),
                ..
            }
        )),
        "trading must work after resume"
    );

    // Position ledger (processor-level check).
    let mut p = trade_core::exchange::Processor::new(|| Box::new(PriceTimePriority), None);
    p.process(
        Command::New(Order::limit(OrderId(1), Side::Sell, 100, 5).by(7)),
        &mut |_| {},
    );
    p.process(
        Command::New(Order::limit(OrderId(2), Side::Buy, 100, 5).by(8)),
        &mut |_| {},
    );
    assert_eq!(p.position(7, sym), -5);
    assert_eq!(p.position(8, sym), 5);
}

/// exchange-core parity: user suspension and atomic command batches.
#[test]
fn user_suspend_and_atomic_batch() {
    let (gw, sink, handle) = build(ExchangeConfig::default());
    let sym = InstrumentId(0);

    // Suspend user 7: their new orders reject, others trade normally.
    gw.submit(Command::HaltUser {
        instrument: sym,
        user: 7,
        cmd_id: 1,
    })
    .unwrap();
    let mut reports = drain_all(&sink);
    gw.new_order(Order::limit(OrderId(2), Side::Sell, 100, 5).by(7))
        .unwrap();
    gw.new_order(Order::limit(OrderId(3), Side::Sell, 100, 5).by(8))
        .unwrap();
    reports.extend(drain_all(&sink));
    gw.submit(Command::ResumeUser {
        instrument: sym,
        user: 7,
        cmd_id: 4,
    })
    .unwrap();
    reports.extend(drain_all(&sink));
    gw.new_order(Order::limit(OrderId(5), Side::Sell, 101, 5).by(7))
        .unwrap();
    reports.extend(drain_all(&sink));

    assert!(reports.contains(&ExecReport::UserHalted {
        instrument: sym,
        user: 7
    }));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecReport::Rejected {
            order_id: OrderId(2),
            reason: "user-suspended",
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecReport::Resting {
            order_id: OrderId(3),
            ..
        }
    )));
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecReport::Resting {
                order_id: OrderId(5),
                ..
            }
        )),
        "resumed user must trade again: {reports:?}"
    );

    // Atomic batch: quote replace (cancel old + place two new) as one group —
    // a marketable order queued behind it sees the FINISHED quote, never the gap.
    gw.submit(Command::Batch(vec![
        Command::Cancel {
            instrument: sym,
            order_id: OrderId(3),
            cmd_id: 6,
        },
        Command::New(Order::limit(OrderId(7), Side::Sell, 100, 5).by(8)),
        Command::New(Order::limit(OrderId(8), Side::Buy, 99, 5).by(8)),
    ]))
    .unwrap();
    let reports = drain_all(&sink);
    handle.shutdown();
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecReport::Cancelled {
            order_id: OrderId(3),
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecReport::Resting {
            order_id: OrderId(7),
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecReport::Resting {
            order_id: OrderId(8),
            ..
        }
    )));
}
