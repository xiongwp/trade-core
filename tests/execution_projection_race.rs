//! Real-MySQL regression test for the "execution result before order row"
//! race: `order-api` writes `asset_orders_*` rows from a separate Kafka
//! consumer than the one applying execution reports, with independent lag,
//! so an execution event can be applied before its order's row exists.
//! `apply_execution_report` must reject that (not silently no-op) so the
//! caller's retry loop re-delivers it until the order row is visible.
//!
//! Requires a real MySQL reachable at `TC_TEST_MYSQL_URL`
//! (e.g. `mysql://root@127.0.0.1:13306`); skips itself otherwise since most
//! dev/CI environments don't have one running.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{params, Pool, TxOpts};

use trade_core::execution_projection::{
    apply_execution_report, asset_orders_ddl, processed_execution_events_ddl,
    processed_executions_ddl,
};
use trade_core::types::{InstrumentId, OrderId};
use trade_core::wire::{self, DecodedReport, ExecutionEvent};

fn pool() -> Option<Pool> {
    let url = std::env::var("TC_TEST_MYSQL_URL").ok()?;
    Some(Pool::new(url.as_str()).expect("connect to TC_TEST_MYSQL_URL"))
}

fn setup_schema(pool: &Pool, db: &str, table: &str) {
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("DROP DATABASE IF EXISTS {db}")).unwrap();
    conn.query_drop(format!("CREATE DATABASE {db}")).unwrap();
    conn.query_drop(processed_executions_ddl(db)).unwrap();
    conn.query_drop(processed_execution_events_ddl(db)).unwrap();
    conn.query_drop(asset_orders_ddl(db, table)).unwrap();
}

fn insert_order_row(
    pool: &Pool,
    db: &str,
    table: &str,
    order_id: u64,
    instrument: u32,
    qty: u64,
) {
    pool.get_conn()
        .unwrap()
        .exec_drop(
            format!(
                "INSERT INTO {db}.{table} (order_id,category_id,user_id,instrument,side,price,qty,status) \
                 VALUES (:id,0,1,:instrument,0,100,:qty,'OPEN')"
            ),
            params! {"id" => order_id, "instrument" => instrument, "qty" => qty},
        )
        .unwrap();
}

fn trade_report(order_id: u64, instrument: u32, fill_qty: u64) -> ExecutionEvent {
    ExecutionEvent {
        raft_group: 1,
        raft_index: 42,
        ordinal: 0,
        target_order_id: OrderId(order_id),
        report: DecodedReport {
            type_code: wire::RT_TRADE,
            maker_fee: 0,
            taker_fee: 0,
            instrument: InstrumentId(instrument),
            order_id: OrderId(order_id),
            aux_id: 0,
            price: 100,
            qty: fill_qty,
            side: trade_core::types::Side::Buy,
        },
    }
}

fn selected_status(pool: &Pool, db: &str, table: &str, order_id: u64) -> Option<(String, u64)> {
    pool.get_conn()
        .unwrap()
        .exec_first(
            format!("SELECT status, filled_qty FROM {db}.{table} WHERE order_id=:id"),
            params! {"id" => order_id},
        )
        .unwrap()
}

/// The core bug: applying a Trade before its order row exists must fail (not
/// silently succeed), and once the order row lands, a retry of the *same*
/// event must converge — this is what makes the consumer's retry-with-
/// backoff loop a correct fix rather than a no-op.
#[test]
fn execution_report_before_order_row_fails_then_converges_on_retry() {
    let Some(pool) = pool() else {
        eprintln!("skipping: TC_TEST_MYSQL_URL not set");
        return;
    };
    let db = "tc_race_basic";
    let table = "asset_orders_000";
    setup_schema(&pool, db, table);

    let event = trade_report(7, 1, 3);
    let mut conn = pool.get_conn().unwrap();

    // Order row for id=7 does not exist yet: applying the fill must fail, not
    // silently succeed (the old bug: INSERT IGNORE into the idempotency table
    // committed regardless, so this event would never be retried).
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = apply_execution_report(&mut tx, db, table, 0, 0, event, 7);
    assert!(
        result.is_err(),
        "applying a fill for a nonexistent order row must fail so the caller retries"
    );
    tx.rollback().unwrap();
    assert_eq!(
        selected_status(&pool, db, table, 7),
        None,
        "the failed attempt must not have left a partial idempotency row behind"
    );

    // Now the order-intake pipeline catches up...
    insert_order_row(&pool, db, table, 7, 1, 10);

    // ...and a retry of the *same* event succeeds and applies correctly.
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    apply_execution_report(&mut tx, db, table, 0, 0, event, 7).unwrap();
    tx.commit().unwrap();
    assert_eq!(
        selected_status(&pool, db, table, 7),
        Some(("PARTIAL".to_string(), 3)),
        "the retried fill must land once the order row is visible"
    );

    // A second retry of the identical event (e.g. because the consumer
    // crashed after commit but before offset commit) must be a no-op, not a
    // double fill: the idempotency table's primary key already covers it.
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    apply_execution_report(&mut tx, db, table, 0, 0, event, 7).unwrap();
    tx.commit().unwrap();
    assert_eq!(
        selected_status(&pool, db, table, 7),
        Some(("PARTIAL".to_string(), 3)),
        "re-delivering an already-applied event must not double-apply the fill"
    );
}

/// Concurrent version: many orders' intake INSERTs race against their own
/// execution-report applies in genuinely random relative order across
/// threads, each execution side driven by the same bounded retry-with-
/// backoff a real consumer uses. Every order must converge to the correct
/// filled state regardless of which pipeline happened to run first.
#[test]
fn concurrent_intake_and_execution_converge_regardless_of_arrival_order() {
    let Some(pool) = pool() else {
        eprintln!("skipping: TC_TEST_MYSQL_URL not set");
        return;
    };
    let db = "tc_race_concurrent";
    let table = "asset_orders_000";
    setup_schema(&pool, db, table);

    const ORDERS: u64 = 40;
    let pool = Arc::new(pool);
    let mut handles = Vec::new();

    for order_id in 1..=ORDERS {
        let intake_pool = Arc::clone(&pool);
        // Order-intake side: inserts the order row after a small, varied
        // delay so some executions race ahead of it and some land after.
        handles.push(thread::spawn(move || {
            thread::sleep(Duration::from_micros((order_id % 7) * 200));
            insert_order_row(&intake_pool, db, table, order_id, 1, 10);
        }));

        let execution_pool = Arc::clone(&pool);
        // Execution side: retries with backoff until the order row is
        // visible, exactly like `run_execution_mysql_consumer`.
        handles.push(thread::spawn(move || {
            let event = trade_report(order_id, 1, 10);
            let mut conn = execution_pool.get_conn().unwrap();
            for attempt in 0..50 {
                let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
                match apply_execution_report(&mut tx, db, table, 0, 0, event, order_id) {
                    Ok(()) => {
                        tx.commit().unwrap();
                        return;
                    }
                    Err(_) => {
                        tx.rollback().unwrap();
                        thread::sleep(Duration::from_micros(200 * (attempt + 1)));
                    }
                }
            }
            panic!("order {order_id} never converged after 50 retries");
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for order_id in 1..=ORDERS {
        assert_eq!(
            selected_status(&pool, db, table, order_id),
            Some(("FILLED".to_string(), 10)),
            "order {order_id} must be fully filled regardless of intake/execution arrival order"
        );
    }
}
