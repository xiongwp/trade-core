//! order-api unit tests and the MySQL 8 integration tests (`--ignored`).

use super::*;
use mysql::params;

fn forward_job(frames: std::ops::Range<u64>) -> RaftForwardJob {
    let (reply, _) = std::sync::mpsc::sync_channel(1);
    RaftForwardJob {
        frames: frames
            .map(|id| {
                let mut frame = [0u8; wire::MSG_LEN];
                frame[8..16].copy_from_slice(&id.to_le_bytes());
                frame
            })
            .collect(),
        reply,
    }
}

fn ids(pack: &[[u8; wire::MSG_LEN]]) -> Vec<u64> {
    pack.iter()
        .map(|frame| u64::from_le_bytes(frame[8..16].try_into().unwrap()))
        .collect()
}

fn ws_batch(count: u64) -> Vec<u8> {
    (0..count)
        .flat_map(|i| batch_order_with_id(100_000 + i, 42_001, 1 + i))
        .collect()
}

fn ws_acks(out: &[u8]) -> Vec<(u8, String)> {
    let mut r = out;
    let mut frames = Vec::new();
    while !r.is_empty() {
        let (op, payload) = trade_core::websocket::read_server_frame(&mut r).unwrap();
        frames.push((op, String::from_utf8_lossy(&payload).into_owned()));
    }
    frames
}

#[test]
fn websocket_session_acknowledges_each_batch_in_order() {
    use trade_core::websocket::{write_client_frame, OP_BINARY, OP_CLOSE, OP_TEXT};
    let mut wire = Vec::new();
    write_client_frame(&mut wire, OP_BINARY, &ws_batch(3), [1, 2, 3, 4]).unwrap();
    write_client_frame(&mut wire, OP_BINARY, &ws_batch(5), [5, 6, 7, 8]).unwrap(); // backpressured
    write_client_frame(&mut wire, 0x9, b"hi", [0, 0, 0, 0]).unwrap(); // ping
    write_client_frame(&mut wire, OP_TEXT, b"{}", [9, 9, 9, 9]).unwrap();
    write_client_frame(&mut wire, OP_BINARY, &ws_batch(5), [1, 1, 1, 1]).unwrap();
    write_client_frame(&mut wire, OP_CLOSE, &1000u16.to_be_bytes(), [0, 0, 0, 0]).unwrap();

    let mut submitted = Vec::new();
    let mut out = Vec::new();
    let mut calls = 0;
    ws_order_session(&mut &wire[..], &mut out, 5, |body| {
        calls += 1;
        let records = decode_batch(body, 1_000).map_err(|e| ("400 Bad Request", e))?;
        if calls == 2 {
            return Err(("429 Too Many Requests", "backpressure: hard".into()));
        }
        submitted.push(records.len());
        Ok(batch_accepted_json(records.len(), &[]))
    })
    .unwrap();

    assert_eq!(submitted, vec![3, 5]);
    let acks = ws_acks(&out);
    let texts: Vec<&str> = acks
        .iter()
        .filter(|(op, _)| *op == OP_TEXT)
        .map(|(_, t)| t.as_str())
        .collect();
    assert_eq!(
        texts[0],
        r#"{"seq":1,"accepted":true,"status":"PENDING","count":3}"#
    );
    assert_eq!(
        texts[1],
        r#"{"seq":2,"accepted":false,"code":429,"error":"backpressure: hard"}"#
    );
    assert!(
        texts[2].starts_with(r#"{"seq":3,"accepted":false,"code":400,"#),
        "{}",
        texts[2]
    );
    assert_eq!(
        texts[3],
        r#"{"seq":4,"accepted":true,"status":"PENDING","count":5}"#
    );
    assert!(
        acks.iter().any(|(op, t)| *op == 0xA && t == "hi"),
        "ping answered"
    );
    assert_eq!(acks.last().unwrap().0, OP_CLOSE);
}

#[test]
fn websocket_session_closes_on_a_batch_above_the_limit() {
    use trade_core::websocket::{write_client_frame, OP_BINARY, OP_CLOSE};
    let mut wire = Vec::new();
    write_client_frame(&mut wire, OP_BINARY, &ws_batch(6), [1, 2, 3, 4]).unwrap();
    let mut out = Vec::new();
    let result = ws_order_session(&mut &wire[..], &mut out, 5, |_| {
        panic!("an oversized batch must not be submitted")
    });
    assert!(result.is_err());
    let mut r = &out[..];
    let (op, payload) = trade_core::websocket::read_server_frame(&mut r).unwrap();
    assert_eq!((op, payload), (OP_CLOSE, 1009u16.to_be_bytes().to_vec()));
    assert!(r.is_empty(), "nothing else was sent");
}

#[test]
fn unfinished_migration_inventory_is_stable_and_actionable() {
    let frozen = vec![
        (2, 9, route_log::RouteRecord::Freeze { from: 0, to: 1 }),
        (19, 41, route_log::RouteRecord::Freeze { from: 3, to: 7 }),
    ];
    assert_eq!(
        unfinished_migrations_json_from(frozen),
        "{\"count\":2,\"migrations\":[{\"category\":2,\"from\":0,\"to\":1,\"epoch\":10,\"freeze_offset\":9},{\"category\":19,\"from\":3,\"to\":7,\"epoch\":42,\"freeze_offset\":41}]}"
    );
}

#[test]
fn queued_jobs_are_packed_in_order_up_to_the_pack_size() {
    let (tx, rx) = std::sync::mpsc::sync_channel(8);
    tx.send(forward_job(3..6)).unwrap();
    tx.send(forward_job(6..9)).unwrap();
    tx.send(forward_job(9..20)).unwrap();
    // The first job has 3; a queued job is taken while the total is
    // still under the pack size (3 < 5), which brings it to 6 and stops.
    let jobs = drain_queued_jobs(forward_job(0..3), &rx, 5);
    assert_eq!(jobs.len(), 2);
    let packed: Vec<Vec<u64>> = packs(&jobs, 5).map(|pack| ids(&pack)).collect();
    assert_eq!(packed, vec![vec![0, 1, 2, 3, 4], vec![5]]);
    // The rest stays queued, in order, for the next round.
    let rest = drain_queued_jobs(rx.recv().unwrap(), &rx, 5);
    assert_eq!(rest.len(), 2);
    let packed: Vec<Vec<u64>> = packs(&rest, 5).map(|pack| ids(&pack)).collect();
    assert_eq!(
        packed,
        vec![
            vec![6, 7, 8, 9, 10],
            vec![11, 12, 13, 14, 15],
            vec![16, 17, 18, 19]
        ]
    );
    // A full first job takes nothing else; an empty one yields no pack.
    assert_eq!(drain_queued_jobs(forward_job(0..5), &rx, 5).len(), 1);
    assert_eq!(packs(&[forward_job(0..0)], 5).count(), 0);
}

#[test]
fn raft_group_failure_only_retries_its_kafka_partitions() {
    let mut groups = Vec::new();
    for group in 0..2 {
        let (tx, rx) = std::sync::mpsc::sync_channel::<RaftForwardJob>(1);
        std::thread::spawn(move || {
            let job = rx.recv().unwrap();
            let result = if group == 0 {
                Ok(())
            } else {
                Err("group 1 lost quorum".to_string())
            };
            job.reply.send(result).unwrap();
        });
        groups.push(tx);
    }
    let forwarder = RaftForwarder {
        groups: Arc::new(groups),
    };
    let records = [
        KafkaRecord {
            topic: "orders".into(),
            partition: 4,
            offset: 10,
            category_id: 0,
            user: 1,
            frame: [0; wire::MSG_LEN],
        },
        KafkaRecord {
            topic: "orders".into(),
            partition: 7,
            offset: 20,
            category_id: 1,
            user: 2,
            frame: [0; wire::MSG_LEN],
        },
    ];
    let routes = trade_core::cluster::RouteControlPlane::new(2, HashMap::new());

    let failures = forwarder.submit(&records, &routes).unwrap();

    assert_eq!(failures.len(), 1);
    assert!(!failures.contains_key(&("orders".to_string(), 4)));
    assert!(failures
        .get(&("orders".to_string(), 7))
        .is_some_and(|error| error.contains("lost quorum")));
}

#[test]
fn order_query_parses_type_and_tif() {
    let base = "order_id=1&side=buy&qty=10&instrument=3&user=9";
    let gtc = order_from_query(&format!("{base}&price=100")).unwrap();
    assert_eq!(
        (gtc.order_type, gtc.tif, gtc.price),
        (OrderType::Limit, TimeInForce::Gtc, 100)
    );
    let maker = order_from_query(&format!("{base}&price=100&tif=post_only")).unwrap();
    assert_eq!(maker.tif, TimeInForce::PostOnly);
    let ioc = order_from_query(&format!("{base}&type=market&tif=ioc")).unwrap();
    assert_eq!(
        (ioc.order_type, ioc.tif),
        (OrderType::Market, TimeInForce::Ioc)
    );

    assert!(order_from_query(base).is_err(), "limit needs a price");
    assert!(order_from_query(&format!("{base}&type=market&tif=post_only")).is_err());
    assert!(order_from_query(&format!("{base}&price=100&tif=gtd")).is_err());
    assert!(order_from_query(&format!("{base}&price=100&type=stop")).is_err());
}

#[test]
fn order_query_with_stop_price_is_a_conditional_order() {
    let base = "order_id=1&side=sell&qty=10&instrument=3&user=9";
    match order_command_from_query(&format!("{base}&type=market&stop_price=95")).unwrap() {
        trade_core::Command::NewConditional(order, trigger) => {
            assert_eq!(order.order_type, OrderType::Market);
            assert_eq!(
                trigger,
                Trigger {
                    price: 95,
                    kind: TriggerKind::StopLoss
                }
            );
        }
        other => panic!("expected a conditional order, got {other:?}"),
    }
    match order_command_from_query(&format!("{base}&price=120&stop_price=118&trigger=tp")).unwrap()
    {
        trade_core::Command::NewConditional(order, trigger) => {
            assert_eq!((order.price, trigger.price), (120, 118));
            assert_eq!(trigger.kind, TriggerKind::TakeProfit);
        }
        other => panic!("expected a conditional order, got {other:?}"),
    }
    assert!(matches!(
        order_command_from_query(&format!("{base}&price=100")).unwrap(),
        trade_core::Command::New(_)
    ));
    for bad in [
        "&price=100&trigger=tp",              // trigger without stop_price
        "&price=100&stop_price=0",            // zero trigger
        "&price=100&stop_price=x",            // malformed
        "&price=100&stop_price=95&trigger=x", // unknown kind
        "&price=100&stop_price=95&tif=post_only",
    ] {
        assert!(
            order_command_from_query(&format!("{base}{bad}")).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn only_order_state_reports_are_staged_for_projection() {
    for code in [
        wire::RT_ACCEPTED,
        wire::RT_TRADE,
        wire::RT_FILLED,
        wire::RT_PARTIAL,
        wire::RT_RESTING,
        wire::RT_CANCELLED,
        wire::RT_REJECTED,
        wire::RT_MODIFIED,
    ] {
        assert!(projects_order_state(code), "type {code}");
    }
    // NotFound, depth, halt/resume acks (12..=15), command rejects,
    // stop-fired markers.
    for code in [
        wire::RT_NOTFOUND,
        wire::RT_DEPTH_LEVEL,
        wire::RT_DEPTH_END,
        12,
        13,
        14,
        15,
        wire::RT_CMD_REJECTED,
        wire::RT_TRIGGERED,
    ] {
        assert!(!projects_order_state(code), "type {code}");
    }
    // A stop firing is followed by the fired order's own reports; the
    // Triggered marker itself changes no row.
    assert!(projects_order_state(wire::RT_STOP_ACCEPTED));
    assert!(!projects_order_state(wire::RT_TRIGGERED));
}

#[test]
fn unfixable_request_errors_are_400_not_retryable_503() {
    assert_eq!(
        ingress_error_status(&format!("{INVALID_REQUEST}tick-size")),
        "400 Bad Request"
    );
    assert_eq!(
        ingress_error_status("backpressure: soft"),
        "429 Too Many Requests"
    );
    assert_eq!(
        ingress_error_status("kafka unavailable"),
        "503 Service Unavailable"
    );
}

fn mysql_test_store() -> Option<OrderStore> {
    let url = std::env::var("TC_TEST_MYSQL_URL").ok()?;
    let routing = sharding::RouteConfig::with_virtual(1, 8, 1_000, 10_000, 1);
    let pool = Pool::new(url.as_str()).expect("open isolated MySQL 8 test server");
    pool.get_conn()
        .expect("connect isolated MySQL 8 test server");
    let store = OrderStore {
        shards: Arc::new(vec![Some(pool)]),
        owned_dbs: Arc::new([0u32].into_iter().collect()),
        routing,
        metrics: Arc::new(OrderPipelineMetrics::default()),
    };
    bootstrap(&store).expect("bootstrap isolated MySQL test schema");
    Some(store)
}

fn kafka_new(order_id: u64, user: u64, instrument: u32, qty: u64, offset: i64) -> KafkaRecord {
    let order = Order::limit(OrderId(order_id), Side::Buy, 1_000, qty)
        .on(InstrumentId(instrument))
        .by(user);
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_new(&order, &mut frame);
    KafkaRecord {
        topic: "mysql-it".into(),
        partition: 0,
        offset,
        category_id: sharding::asset_category(InstrumentId(instrument), 1_000),
        user,
        frame,
    }
}

fn kafka_cancel(
    order_id: u64,
    cmd_id: u64,
    user: u64,
    instrument: u32,
    offset: i64,
) -> KafkaRecord {
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_cancel(
        InstrumentId(instrument),
        OrderId(order_id),
        cmd_id,
        &mut frame,
    );
    KafkaRecord {
        topic: "mysql-it".into(),
        partition: 0,
        offset,
        category_id: sharding::asset_category(InstrumentId(instrument), 1_000),
        user,
        frame,
    }
}

fn report_event(
    type_code: u8,
    target_order_id: u64,
    instrument: u32,
    raft_index: u64,
    offset: i64,
) -> ExecutionKafkaRecord {
    let mut record = trade_event(target_order_id, 0, instrument, 0, raft_index, 0, offset);
    record.event.report.type_code = type_code;
    record
}

fn trade_event(
    target_order_id: u64,
    other_order_id: u64,
    instrument: u32,
    qty: u64,
    raft_index: u64,
    ordinal: u32,
    offset: i64,
) -> ExecutionKafkaRecord {
    ExecutionKafkaRecord {
        topic: "execution-it".into(),
        partition: 0,
        offset,
        event: wire::ExecutionEvent {
            raft_group: 7,
            raft_index,
            ordinal,
            target_order_id,
            report: wire::DecodedReport {
                type_code: wire::RT_TRADE,
                maker_fee: 0,
                taker_fee: 0,
                instrument: InstrumentId(instrument),
                order_id: OrderId(target_order_id),
                aux_id: other_order_id,
                price: 1_000,
                qty,
                side: Side::Buy,
            },
        },
    }
}

fn persist_events(store: &OrderStore, records: Vec<ExecutionKafkaRecord>) -> Result<(), String> {
    let mut tables = ExecutionShardTables::new();
    for record in records {
        let target = record.event.target_order_id;
        let route = store.routing.route_order_id(target);
        assert_eq!(route.db, 0);
        tables
            .entry(route.table)
            .or_default()
            .push((record, target));
    }
    persist_execution_shard(store, 0, tables, 500)
}

fn batch_order(user: u64, instrument: u32) -> Vec<u8> {
    batch_order_with_id(user, instrument, 99)
}

fn batch_order_with_id(user: u64, instrument: u32, order_id: u64) -> Vec<u8> {
    let order = Order::limit(OrderId(order_id), Side::Buy, 1_000, 2)
        .on(InstrumentId(instrument))
        .by(user);
    let mut frame = [0u8; wire::MSG_LEN];
    wire::encode_new(&order, &mut frame);
    let mut body = user.to_le_bytes().to_vec();
    body.extend_from_slice(&frame);
    body
}

#[test]
fn execution_batch_target_scales_with_physical_db_count() {
    assert_eq!(execution_consumer_batch_size(100, 10), 1_000);
    assert_eq!(execution_consumer_batch_size(500, 10), 5_000);
    assert_eq!(execution_consumer_batch_size(0, 0), 1);
}

/// Run explicitly against a disposable MySQL 8 instance:
/// `TC_TEST_MYSQL_URL=mysql://root:test@127.0.0.1:33306/mysql cargo test
///  --bin order-api mysql8_projection_is_race_safe_and_idempotent -- --ignored`
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_projection_is_race_safe_and_idempotent() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let taker = seed | 1;
    let maker = seed | 2;
    let raced = seed | 3;
    let user = 55_001;
    let instrument = 42_001;

    // Result-before-order: staging remains durable, then the command-side
    // transaction drains it as soon as the order row becomes visible.
    persist_events(
        &store,
        vec![trade_event(taker, maker, instrument, 3, seed, 0, 1)],
    )
    .unwrap();
    persist_mysql_shard(&store, 0, &[kafka_new(taker, user, instrument, 10, 1)]).unwrap();

    // One Trade has two independently keyed target events. The idempotency
    // key includes target_order_id, so maker and taker cannot collapse.
    persist_mysql_shard(&store, 0, &[kafka_new(maker, user + 1, instrument, 10, 2)]).unwrap();
    let dual = vec![
        trade_event(taker, maker, instrument, 2, seed + 1, 0, 2),
        trade_event(maker, taker, instrument, 2, seed + 1, 0, 3),
    ];
    persist_events(&store, dual.clone()).unwrap();
    persist_events(&store, dual).unwrap(); // Kafka replay must not double-fill.

    // Deliberately race the command and execution transactions. Whichever
    // commits first, either the command drain, execution UPDATE, or sweeper
    // must converge to the same projection.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let command_store = store.clone();
    let command_barrier = barrier.clone();
    let command = std::thread::spawn(move || {
        command_barrier.wait();
        persist_mysql_shard(
            &command_store,
            0,
            &[kafka_new(raced, user + 2, instrument, 5, 4)],
        )
    });
    let execution_store = store.clone();
    let execution = std::thread::spawn(move || {
        barrier.wait();
        persist_events(
            &execution_store,
            vec![trade_event(raced, maker, instrument, 5, seed + 2, 0, 4)],
        )
    });
    command.join().unwrap().unwrap();
    execution.join().unwrap().unwrap();
    sweep_pending_execution_shard(&store, 0).unwrap();

    let mut conn = store.shard(0).get_conn().unwrap();
    for (order_id, expected_fill, expected_status) in [
        (taker, 5u64, "PARTIAL"),
        (maker, 2u64, "PARTIAL"),
        (raced, 5u64, "FILLED"),
    ] {
        let route = store.routing.route_order_id(order_id);
        let row: Option<(u64, String)> = conn
            .exec_first(
                format!(
                    "SELECT filled_qty,status FROM order_db_0.{} WHERE order_id=:id",
                    route.table_name()
                ),
                params! {"id" => order_id},
            )
            .unwrap();
        assert_eq!(row, Some((expected_fill, expected_status.into())));
    }
    let targets: u64 = conn
        .exec_first(
            "SELECT (SELECT COUNT(*) FROM order_db_0.processed_execution_events_0 WHERE raft_group=7 AND raft_index=:idx AND report_ordinal=0) + (SELECT COUNT(*) FROM order_db_0.processed_execution_events_1 WHERE raft_group=7 AND raft_index=:idx AND report_ordinal=0) + (SELECT COUNT(*) FROM order_db_0.processed_execution_events_2 WHERE raft_group=7 AND raft_index=:idx AND report_ordinal=0)",
            params! {"idx" => seed + 1},
        )
        .unwrap()
        .unwrap();
    assert_eq!(targets, 2, "maker and taker use distinct idempotency keys");
    let pending: u64 = conn
        .query_first("SELECT COUNT(*) FROM order_db_0.pending_execution_events")
        .unwrap()
        .unwrap();
    assert_eq!(pending, 0);
}

#[test]
fn dedupe_tables_rotate_through_three_names() {
    assert_eq!(dedupe_table(0), "processed_execution_events_0");
    assert_eq!(dedupe_table(4), "processed_execution_events_1");
    // The table emptied during a period is never the current or previous one.
    for period in [1u64, 2, 3, 1_000_000] {
        let next = dedupe_table(period + 1);
        assert_ne!(next, dedupe_table(period));
        assert_ne!(next, dedupe_table(period - 1));
    }
}

/// Same disposable MySQL 8 as above. Rows land in the current period's
/// table; only the next period's table (two periods old) is emptied.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_dedupe_rotation_truncates_only_the_stale_table() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let mut conn = store.shard(0).get_conn().unwrap();
    let period = dedupe_period(&mut conn).unwrap();
    for table in 0..DEDUPE_TABLES {
        conn.query_drop(format!(
            "TRUNCATE TABLE order_db_0.processed_execution_events_{table}"
        ))
        .unwrap();
    }
    // (raft_index, age in periods): current, previous, and two periods old.
    for (index, age) in [(1u64, 0u64), (2, 1), (3, 2)] {
        conn.exec_drop(
            format!(
                "INSERT INTO order_db_0.{} (raft_group,raft_index,report_ordinal,target_order_id,shard_table,instrument,report_type,kafka_partition,kafka_offset) VALUES (9,?,0,1,0,0,12,0,0)",
                dedupe_table(period + DEDUPE_TABLES - age)
            ),
            (index,),
        )
        .unwrap();
    }
    let left = |conn: &mut mysql::PooledConn| -> Vec<u64> {
        conn.query(
            "SELECT raft_index FROM order_db_0.processed_execution_events_0 UNION ALL SELECT raft_index FROM order_db_0.processed_execution_events_1 UNION ALL SELECT raft_index FROM order_db_0.processed_execution_events_2 ORDER BY 1",
        )
        .unwrap()
    };
    let mut ring = DedupeRing::new();
    assert_eq!(ring.advance(&mut conn, 0, period).unwrap(), 1);
    assert_eq!(
        left(&mut conn),
        vec![1, 2],
        "only the two-period-old row goes"
    );
    // Same period again, or the clock stepping back: nothing more.
    assert_eq!(ring.advance(&mut conn, 0, period).unwrap(), 0);
    assert_eq!(ring.advance(&mut conn, 0, period - 1).unwrap(), 0);
    assert_eq!(left(&mut conn), vec![1, 2]);

    // A redelivery of an event applied in the previous period is discarded.
    conn.query_drop(
        "INSERT INTO order_db_0.pending_execution_events (raft_group,raft_index,report_ordinal,target_order_id,shard_table,instrument,report_type,event_qty,kafka_partition,kafka_offset) VALUES (9,2,0,1,0,0,12,0,0,0)",
    )
    .unwrap();
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    discard_redelivered_events(&mut tx, "order_db_0", period).unwrap();
    tx.commit().unwrap();
    let pending: u64 = conn
        .query_first("SELECT COUNT(*) FROM order_db_0.pending_execution_events WHERE raft_group=9")
        .unwrap()
        .unwrap();
    assert_eq!(pending, 0);
    for table in 0..DEDUPE_TABLES {
        conn.query_drop(format!(
            "TRUNCATE TABLE order_db_0.processed_execution_events_{table}"
        ))
        .unwrap();
    }
}

/// Same disposable MySQL 8 as above. A pre-ring dedupe table is renamed,
/// not rebuilt, and still stops a redelivery of an event it recorded.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_projection_ignores_reports_that_change_no_order() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let order = seed | 1;
    let instrument = 42_002;
    let status = |conn: &mut mysql::PooledConn| -> Option<String> {
        let route = store.routing.route_order_id(order);
        conn.exec_first(
            format!(
                "SELECT status FROM order_db_0.{} WHERE order_id=:id",
                route.table_name()
            ),
            params! {"id" => order},
        )
        .unwrap()
    };
    let pending = |conn: &mut mysql::PooledConn| -> u64 {
        conn.query_first("SELECT COUNT(*) FROM order_db_0.pending_execution_events")
            .unwrap()
            .unwrap()
    };

    persist_mysql_shard(&store, 0, &[kafka_new(order, 66_001, instrument, 10, 1)]).unwrap();
    persist_events(
        &store,
        vec![
            report_event(wire::RT_ACCEPTED, order, instrument, seed, 1),
            report_event(wire::RT_CANCELLED, order, instrument, seed + 1, 2),
        ],
    )
    .unwrap();
    let mut conn = store.shard(0).get_conn().unwrap();
    assert_eq!(status(&mut conn).as_deref(), Some("CANCELLED"));

    // A repeated cancel answers NotFound; it must not reopen the order.
    persist_events(
        &store,
        vec![report_event(
            wire::RT_NOTFOUND,
            order,
            instrument,
            seed + 2,
            3,
        )],
    )
    .unwrap();
    assert_eq!(status(&mut conn).as_deref(), Some("CANCELLED"));

    // A duplicate New's command reject carries the original order's id and
    // must not mark it REJECTED; halt acks carry order id 0 and command
    // rejects of cancels a command id — neither may linger in pending.
    persist_events(
        &store,
        vec![
            report_event(wire::RT_CMD_REJECTED, order, instrument, seed + 3, 4),
            report_event(12, 0, instrument, seed + 4, 5),
            report_event(
                wire::RT_CMD_REJECTED,
                seed | 0xffff,
                instrument,
                seed + 5,
                6,
            ),
        ],
    )
    .unwrap();
    assert_eq!(status(&mut conn).as_deref(), Some("CANCELLED"));
    assert_eq!(pending(&mut conn), 0);
}

/// Same disposable MySQL 8 as above.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_projection_tracks_a_conditional_order_from_untriggered_to_filled() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let order = seed | 1;
    let instrument = 42_003;
    let row = |conn: &mut mysql::PooledConn| -> Option<(u64, String)> {
        let route = store.routing.route_order_id(order);
        conn.exec_first(
            format!(
                "SELECT filled_qty,status FROM order_db_0.{} WHERE order_id=:id",
                route.table_name()
            ),
            params! {"id" => order},
        )
        .unwrap()
    };
    persist_mysql_shard(&store, 0, &[kafka_new(order, 66_002, instrument, 4, 1)]).unwrap();
    persist_events(
        &store,
        vec![report_event(
            wire::RT_STOP_ACCEPTED,
            order,
            instrument,
            seed,
            1,
        )],
    )
    .unwrap();
    let mut conn = store.shard(0).get_conn().unwrap();
    assert_eq!(row(&mut conn), Some((0, "UNTRIGGERED".into())));

    // The trigger prints: Triggered (not staged), then the fired order's
    // ordinary lifecycle in the same command.
    persist_events(
        &store,
        vec![
            // Distinct raft indices: (group, index, ordinal, target) is
            // the idempotency key, and report_event uses ordinal 0.
            report_event(wire::RT_TRIGGERED, order, instrument, seed + 1, 2),
            report_event(wire::RT_ACCEPTED, order, instrument, seed + 2, 3),
            trade_event(order, seed | 2, instrument, 4, seed + 3, 0, 4),
            report_event(wire::RT_FILLED, order, instrument, seed + 4, 5),
        ],
    )
    .unwrap();
    assert_eq!(row(&mut conn), Some((4, "FILLED".into())));
    let pending: u64 = conn
        .query_first("SELECT COUNT(*) FROM order_db_0.pending_execution_events")
        .unwrap()
        .unwrap();
    assert_eq!(pending, 0);
}

/// With the drain handed off, the command writer only inserts the order
/// row and marks its table; the event that arrived first stays pending
/// until the execution writer drains the marked table.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_command_writer_hands_the_pending_drain_to_the_execution_writer() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let order = seed | 1;
    let instrument = 42_004;
    let route = store.routing.route_order_id(order);
    let row = |conn: &mut mysql::PooledConn| -> Option<(u64, String)> {
        conn.exec_first(
            format!(
                "SELECT filled_qty,status FROM order_db_0.{} WHERE order_id=:id",
                route.table_name()
            ),
            params! {"id" => order},
        )
        .unwrap()
    };
    // The fill reaches MySQL before the order row.
    persist_events(
        &store,
        vec![
            report_event(wire::RT_ACCEPTED, order, instrument, seed, 1),
            trade_event(order, seed | 2, instrument, 4, seed + 1, 0, 2),
            report_event(wire::RT_FILLED, order, instrument, seed + 2, 3),
        ],
    )
    .unwrap();
    let handoff = DrainHandoff::new(store.routing);
    handoff.take(0); // drop the start-up "everything" mark
    persist_mysql_shard_with(
        &store,
        0,
        &[kafka_new(order, 66_003, instrument, 4, 1)],
        Some(&handoff),
    )
    .unwrap();
    let mut conn = store.shard(0).get_conn().unwrap();
    assert_eq!(
        row(&mut conn),
        Some((0, "PENDING".into())),
        "drain ran inline"
    );
    let marked = handoff.take(0);
    assert_eq!(marked, std::iter::once(route.table).collect());
    drain_marked_tables(&store, 0, &marked).unwrap();
    assert_eq!(row(&mut conn), Some((4, "FILLED".into())));
    let pending: u64 = conn
        .exec_first(
            "SELECT COUNT(*) FROM order_db_0.pending_execution_events WHERE target_order_id=?",
            (order,),
        )
        .unwrap()
        .unwrap();
    assert_eq!(pending, 0);
}

/// Same disposable MySQL 8 as above. Found by the 10k-user stress run:
/// 129 orders were stuck in CANCEL_PENDING.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_cancel_request_never_overwrites_order_status() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let (filled, open) = (seed | 1, seed | 2);
    let (user, instrument) = (77_001, 42_003);
    let status = |order: u64| -> Option<String> {
        let route = store.routing.route_order_id(order);
        store
            .shard(0)
            .get_conn()
            .unwrap()
            .exec_first(
                format!(
                    "SELECT status FROM order_db_0.{} WHERE order_id=:id",
                    route.table_name()
                ),
                params! {"id" => order},
            )
            .unwrap()
    };

    persist_mysql_shard(
        &store,
        0,
        &[
            kafka_new(filled, user, instrument, 3, 1),
            kafka_new(open, user, instrument, 3, 2),
        ],
    )
    .unwrap();
    persist_events(
        &store,
        vec![
            trade_event(filled, open ^ 0xff, instrument, 3, seed, 0, 1),
            report_event(wire::RT_FILLED, filled, instrument, seed + 1, 2),
            report_event(wire::RT_RESTING, open, instrument, seed + 2, 3),
        ],
    )
    .unwrap();
    assert_eq!(status(filled).as_deref(), Some("FILLED"));
    assert_eq!(status(open).as_deref(), Some("OPEN"));

    // A late cancel of the filled order (the engine answers NotFound) and a
    // cancel of the open one: neither request changes a status by itself.
    persist_mysql_shard(
        &store,
        0,
        &[
            kafka_cancel(filled, seed + 10, user, instrument, 3),
            kafka_cancel(open, seed + 11, user, instrument, 4),
        ],
    )
    .unwrap();
    persist_events(
        &store,
        vec![report_event(
            wire::RT_NOTFOUND,
            filled,
            instrument,
            seed + 3,
            4,
        )],
    )
    .unwrap();
    assert_eq!(status(filled).as_deref(), Some("FILLED"));
    assert_eq!(status(open).as_deref(), Some("OPEN"));

    // Only the engine's confirmation cancels.
    persist_events(
        &store,
        vec![report_event(
            wire::RT_CANCELLED,
            open,
            instrument,
            seed + 4,
            5,
        )],
    )
    .unwrap();
    assert_eq!(status(open).as_deref(), Some("CANCELLED"));
}

/// Same disposable MySQL 8 as above.
#[test]
fn decodes_batch_and_routes_asset_category() {
    let mut body = batch_order(100_000, 1);
    body.extend_from_slice(&batch_order(100_001, 2_501));

    let records = decode_batch(&body, 1_000).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].category_id, 0);
    assert_eq!(records[1].category_id, 2);
    assert_eq!(records[1].user, 100_001);
}

#[test]
fn same_asset_routes_to_same_topic_and_partition_for_different_orders() {
    let mut body = batch_order_with_id(100_000, 42_001, 7);
    body.extend_from_slice(&batch_order_with_id(200_000, 42_001, 9_999_999));
    let records = decode_batch(&body, 1_000).unwrap();
    let router = QueueRouter::new(
        vec!["orders-0".into(), "orders-1".into(), "orders-2".into()],
        8,
        1,
    );

    assert_eq!(records[0].category_id, records[1].category_id);
    assert_eq!(
        router.route(records[0].category_id),
        router.route(records[1].category_id)
    );
}

#[test]
fn rejects_partial_batch_record() {
    assert!(decode_batch(&[0; BATCH_RECORD_LEN - 1], 1_000).is_err());
}

#[test]
fn rejects_user_that_differs_from_order() {
    let mut body = batch_order(100_000, 1);
    body[..8].copy_from_slice(&100_001u64.to_le_bytes());

    assert!(decode_batch(&body, 1_000)
        .unwrap_err()
        .contains("does not match"));
}

#[test]
fn ingress_backpressure_uses_shared_group_lag_and_local_inflight() {
    let metrics = OrderPipelineMetrics::default();
    metrics.try_reserve(3, 5).unwrap();
    assert_eq!(metrics.backlog(), 3);
    metrics.finish_reservation(3, 3);
    assert_eq!(metrics.backlog(), 0);

    metrics.set_observed_lag("mysql", 2);
    metrics.set_observed_lag("match", 3);
    assert_eq!(metrics.backlog(), 3);
    metrics.try_reserve(2, 5).unwrap();
    assert!(metrics.try_reserve(1, 5).is_err());
    assert_eq!(
        ingress_error_status("backpressure: full"),
        "429 Too Many Requests"
    );
}

fn test_backpressure(
    soft: u64,
    hard: u64,
    emergency: u64,
    partitions: usize,
    groups: usize,
) -> Backpressure {
    Backpressure {
        soft,
        hard,
        emergency,
        soft_delay: Duration::ZERO,
        topic_count: 1,
        partitions_per_topic: partitions as u32,
        routes: trade_core::cluster::RouteControlPlane::new(groups.max(1), HashMap::new()),
        mysql_lag: AtomicU64::new(0),
        match_lag: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
        group_unhealthy: (0..groups).map(|_| AtomicU64::new(0)).collect(),
        soft_events: AtomicU64::new(0),
        hard_events: AtomicU64::new(0),
        emergency_events: AtomicU64::new(0),
    }
}

#[test]
fn backpressure_tiers_climb_by_threshold_and_recover() {
    let bp = test_backpressure(100, 250, 750, 1, 1);
    // Climb through every tier as lag crosses each threshold.
    assert_eq!(bp.classify(0, false), BackpressureTier::Normal);
    assert_eq!(bp.classify(99, false), BackpressureTier::Normal);
    assert_eq!(bp.classify(100, false), BackpressureTier::Soft);
    assert_eq!(bp.classify(249, false), BackpressureTier::Soft);
    assert_eq!(bp.classify(250, false), BackpressureTier::Hard);
    assert_eq!(bp.classify(749, false), BackpressureTier::Hard);
    assert_eq!(bp.classify(750, false), BackpressureTier::Emergency);
    // Lost quorum forces emergency regardless of lag.
    assert_eq!(bp.classify(0, true), BackpressureTier::Emergency);
    // Recovery falls back down as lag drains.
    assert_eq!(bp.classify(300, false), BackpressureTier::Hard);
    assert_eq!(bp.classify(120, false), BackpressureTier::Soft);
    assert_eq!(bp.classify(10, false), BackpressureTier::Normal);
}

#[test]
fn backpressure_is_isolated_per_category_and_takes_group_max() {
    // 4 order partitions; category N maps to partition N (topic_count 1).
    let bp = test_backpressure(100, 250, 750, 4, 2);
    // A hot order partition only throttles the categories on it.
    bp.set_partition_lags("match", &[300, 0, 0, 0]);
    assert_eq!(bp.tier_for_category(0), BackpressureTier::Hard);
    assert_eq!(bp.tier_for_category(1), BackpressureTier::Normal);
    assert_eq!(bp.tier_for_category(4), BackpressureTier::Hard); // 4 -> partition 0
                                                                 // MySQL lag is per database, and every category writes to every
                                                                 // database: one slow database throttles every category.
    bp.set_partition_lags("match", &[0, 0, 0, 0]);
    bp.set_partition_lags("mysql", &[0, 0, 800, 0, 0, 0]);
    for category in 0..4 {
        assert_eq!(bp.tier_for_category(category), BackpressureTier::Emergency);
    }
    assert_eq!(bp.max_partition_lag(), 800);
    // Drain both -> everything recovers to normal.
    bp.set_partition_lags("mysql", &[0, 0, 0, 0, 0, 0]);
    assert_eq!(bp.tier_for_category(0), BackpressureTier::Normal);
    assert_eq!(bp.tier_for_category(1), BackpressureTier::Normal);
}

#[test]
fn backpressure_quorum_loss_is_per_group_and_recoverable() {
    let bp = test_backpressure(100, 250, 750, 2, 2);
    // Category 0 -> group 0, category 1 -> group 1.
    bp.set_group_health(0, false);
    assert_eq!(bp.tier_for_category(0), BackpressureTier::Emergency);
    assert_eq!(bp.tier_for_category(1), BackpressureTier::Normal);
    // Group recovers on the next healthy forward.
    bp.set_group_health(0, true);
    assert_eq!(bp.tier_for_category(0), BackpressureTier::Normal);
}

#[test]
fn backpressure_admit_maps_tiers_to_ingress_status() {
    let bp = test_backpressure(100, 250, 750, 1, 1);
    // Soft accepts (slowdown only).
    bp.set_partition_lags("mysql", &[150]);
    assert!(bp.admit(0).is_ok());
    // Hard -> 429.
    bp.set_partition_lags("mysql", &[300]);
    let hard = bp.admit(0).unwrap_err();
    assert_eq!(ingress_error_status(&hard), "429 Too Many Requests");
    // Emergency -> 503.
    bp.set_partition_lags("mysql", &[900]);
    let emergency = bp.admit(0).unwrap_err();
    assert_eq!(ingress_error_status(&emergency), "503 Service Unavailable");
}

#[test]
fn shard_url_count_must_match_db_count() {
    assert!(validate_shard_urls(&["db0".to_string()], 1).is_ok());
    let three = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    assert!(validate_shard_urls(&three, 3).is_ok());
    let err = validate_shard_urls(&three, 10).unwrap_err();
    assert!(err.contains("10"));
    assert!(validate_shard_urls(&[], 1).is_err());
}

#[test]
fn ingress_and_match_only_roles_do_not_require_mysql() {
    assert!(!role_needs_mysql(0, 0));
    assert!(role_needs_mysql(1, 0));
    assert!(role_needs_mysql(0, 1));
}

#[test]
fn worker_completion_is_not_clamped_by_process_local_publishes() {
    let metrics = OrderPipelineMetrics::default();
    metrics.complete("mysql", 500);
    metrics.complete("match", 700);
    assert_eq!(
        metrics
            .mysql_completed_commands
            .load(AtomicOrdering::Relaxed),
        500
    );
    assert_eq!(
        metrics
            .match_completed_commands
            .load(AtomicOrdering::Relaxed),
        700
    );
}

fn dlq_temp_path(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    std::env::temp_dir().join(format!("tc-dlq-{}-{tag}-{n}.wal", std::process::id()))
}

fn read_dlq(path: &Path) -> Vec<(i32, i64, String, Vec<u8>)> {
    let mut file = File::open(path).unwrap();
    let mut header = [0u8; 8];
    file.read_exact(&mut header).unwrap();
    assert_eq!(header, DLQ_HEADER);
    let mut out = Vec::new();
    loop {
        let mut len_bytes = [0u8; 4];
        if file.read_exact(&mut len_bytes).is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut body = vec![0u8; len];
        file.read_exact(&mut body).unwrap();
        let mut checksum = [0u8; 8];
        file.read_exact(&mut checksum).unwrap();
        assert_eq!(journal::fnv1a(&body), u64::from_le_bytes(checksum));
        let partition = i32::from_le_bytes(body[8..12].try_into().unwrap());
        let offset = i64::from_le_bytes(body[12..20].try_into().unwrap());
        let reason_len = u32::from_le_bytes(body[20..24].try_into().unwrap()) as usize;
        let reason = String::from_utf8(body[24..24 + reason_len].to_vec()).unwrap();
        let mut cursor = 24 + reason_len;
        let payload_len = u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let payload = body[cursor..cursor + payload_len].to_vec();
        out.push((partition, offset, reason, payload));
    }
    out
}

#[test]
fn persist_retry_dead_letters_after_exhausting_retries() {
    let path = dlq_temp_path("poison");
    let _ = std::fs::remove_file(&path);
    let dlq = Mutex::new(DlqWriter::open(&path).unwrap());
    let metrics = OrderPipelineMetrics::default();

    let attempts = AtomicU64::new(0);
    let outcome = persist_with_retry(
        || {
            attempts.fetch_add(1, AtomicOrdering::Relaxed);
            Err("poison message".to_string())
        },
        &dlq,
        &metrics,
        3,
        Duration::ZERO,
        7,
        42,
        b"raw-envelope",
    );

    assert_eq!(outcome, PersistOutcome::DeadLettered);
    assert_eq!(
        attempts.load(AtomicOrdering::Relaxed),
        3,
        "tries up to the retry cap"
    );
    assert_eq!(metrics.dlq_total.load(AtomicOrdering::Relaxed), 1);

    let records = read_dlq(&path);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, 7);
    assert_eq!(records[0].1, 42);
    assert!(records[0].2.contains("poison"));
    assert_eq!(records[0].3, b"raw-envelope");
    std::fs::remove_file(&path).ok();
}

#[test]
fn persist_retry_commits_and_skips_dlq_on_eventual_success() {
    let path = dlq_temp_path("recover");
    let _ = std::fs::remove_file(&path);
    let dlq = Mutex::new(DlqWriter::open(&path).unwrap());
    let metrics = OrderPipelineMetrics::default();

    let attempts = AtomicU64::new(0);
    let outcome = persist_with_retry(
        || {
            // Fail twice, then succeed on the third attempt.
            if attempts.fetch_add(1, AtomicOrdering::Relaxed) < 2 {
                Err("transient".to_string())
            } else {
                Ok(())
            }
        },
        &dlq,
        &metrics,
        5,
        Duration::ZERO,
        1,
        2,
        b"x",
    );

    assert_eq!(outcome, PersistOutcome::Committed);
    assert_eq!(metrics.dlq_total.load(AtomicOrdering::Relaxed), 0);
    assert!(read_dlq(&path).is_empty(), "no dead-letter on success");
    std::fs::remove_file(&path).ok();
}

#[test]
fn order_query_parses_routes_filters_and_limits() {
    let parse = |method: &str, path: &str, query: &str| parse_order_query(method, path, query);
    assert_eq!(
        parse("GET", "/orders/42", ""),
        Some(Ok(OrderQuery::Order(42)))
    );
    assert!(matches!(parse("GET", "/orders/x", ""), Some(Err(_))));
    // Not queries: they fall through to ingress / metrics / 404.
    assert_eq!(parse("POST", "/orders", "user=1"), None);
    assert_eq!(parse("GET", "/metrics", ""), None);
    assert_eq!(parse("GET", "/ws/orders", ""), None);

    assert_eq!(
        parse("GET", "/orders", "user=7"),
        Some(Ok(OrderQuery::UserOrders {
            user: 7,
            open_only: false,
            limit: 100
        }))
    );
    assert_eq!(
        parse("GET", "/orders", "user=7&status=open&limit=1000"),
        Some(Ok(OrderQuery::UserOrders {
            user: 7,
            open_only: true,
            limit: 1000
        }))
    );
    assert_eq!(
        parse("GET", "/orders", "status=all&user=7&limit=1"),
        Some(Ok(OrderQuery::UserOrders {
            user: 7,
            open_only: false,
            limit: 1
        }))
    );
    for bad in [
        "",
        "user=x",
        "user=7&status=filled",
        "user=7&limit=0",
        "user=7&limit=1001",
        "user=7&limit=-1",
    ] {
        assert!(
            matches!(parse("GET", "/orders", bad), Some(Err(_))),
            "{bad}"
        );
    }

    assert_eq!(
        parse("GET", "/fills", "order_id=3"),
        Some(Ok(OrderQuery::OrderFills {
            order_id: 3,
            limit: 100
        }))
    );
    assert_eq!(
        parse("GET", "/fills", "user=4&limit=50"),
        Some(Ok(OrderQuery::UserFills { user: 4, limit: 50 }))
    );
    for bad in ["", "order_id=3&user=4", "order_id=x", "user=4&limit=2000"] {
        assert!(matches!(parse("GET", "/fills", bad), Some(Err(_))), "{bad}");
    }
    // A parse error is a 400 even on a worker without MySQL.
    let (status, body) = order_query_response(None, Err("invalid order_id".into()));
    assert_eq!(
        (status, body.as_str()),
        ("400 Bad Request", "{\"error\":\"invalid order_id\"}")
    );
    assert_eq!(
        order_query_response(None, Ok(OrderQuery::Order(1))).0,
        "503 Service Unavailable"
    );
}

#[test]
fn order_lookups_route_where_the_projections_write() {
    let routing = sharding::RouteConfig::with_virtual(10, 100, 1_000, 10_000, 1);
    let mut dbs = std::collections::HashSet::new();
    for id in (1..2_000u64).map(|n| n.wrapping_mul(0x9e37_79b9_7f4a_7c15)) {
        // The command projection places the row by database_order_id; the
        // execution projection places fills by target order id.
        let order = Order::limit(OrderId(id), Side::Buy, 1_000, 1)
            .on(InstrumentId(1))
            .by(9);
        let written = routing.route_order_id(database_order_id(&trade_core::Command::New(order)));
        let at = order_location(&routing, id);
        assert_eq!(at.db, written.db);
        assert_eq!(
            at.orders_table,
            format!("order_db_{}.asset_orders_{:03}", written.db, written.table)
        );
        assert_eq!(
            at.fills_table,
            format!("order_db_{}.order_fills", written.db)
        );
        dbs.insert(at.db);
    }
    // One user's orders land in every DB, hence the per-user fan-out.
    assert_eq!(dbs.len(), 10);
}

#[test]
fn average_fill_price_is_notional_over_filled_qty() {
    assert_eq!(average_price("0", 0), "0");
    assert_eq!(average_price("3000", 3), "1000");
    assert_eq!(average_price("10006", 10), "1000.6");
    assert_eq!(average_price("10", 3), "3.33333333");
    // Beyond u64: price * qty products are summed as DECIMAL(38,0).
    assert_eq!(
        average_price(&(u64::MAX as u128 * 2).to_string(), 2),
        u64::MAX.to_string()
    );
}

#[test]
fn trade_fill_takes_role_and_fee_from_the_target_side() {
    let report = wire::DecodedReport {
        type_code: wire::RT_TRADE,
        maker_fee: 3,
        taker_fee: 7,
        instrument: InstrumentId(1),
        order_id: OrderId(10),
        aux_id: 20,
        price: 1_001,
        qty: 6,
        side: Side::Buy,
    };
    assert_eq!(
        trade_fill(&report, 10),
        Some(TradeFill {
            price: 1_001,
            fee: 7,
            role: FILL_ROLE_TAKER
        })
    );
    assert_eq!(
        trade_fill(&report, 20),
        Some(TradeFill {
            price: 1_001,
            fee: 3,
            role: FILL_ROLE_MAKER
        })
    );
    let filled = wire::DecodedReport {
        type_code: wire::RT_FILLED,
        ..report
    };
    assert_eq!(trade_fill(&filled, 10), None);
}

fn kafka_order(
    order_id: u64,
    user: u64,
    instrument: u32,
    side: Side,
    price: u64,
    qty: u64,
    offset: i64,
) -> KafkaRecord {
    let mut record = kafka_new(order_id, user, instrument, qty, offset);
    let order = Order::limit(OrderId(order_id), side, price, qty)
        .on(InstrumentId(instrument))
        .by(user);
    wire::encode_new(&order, &mut record.frame);
    record
}

/// Both per-side events of one Trade, as the engine publishes them.
#[allow(clippy::too_many_arguments)]
fn trade_pair(
    taker: u64,
    maker: u64,
    instrument: u32,
    price: u64,
    qty: u64,
    fees: (u64, u64),
    raft_index: u64,
    ordinal: u32,
) -> Vec<ExecutionKafkaRecord> {
    [taker, maker]
        .into_iter()
        .map(|target| {
            let mut record = trade_event(
                target,
                0,
                instrument,
                qty,
                raft_index,
                ordinal,
                raft_index as i64,
            );
            record.event.report.order_id = OrderId(taker);
            record.event.report.aux_id = maker;
            record.event.report.price = price;
            (record.event.report.maker_fee, record.event.report.taker_fee) = fees;
            record
        })
        .collect()
}

fn query_ok(store: &OrderStore, method_path: &str, query: &str) -> (&'static str, String) {
    let request = parse_order_query("GET", method_path, query).expect("an order query route");
    order_query_response(Some(store), request)
}

/// Same disposable MySQL 8 as above.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_avg_price_and_fills_after_partial_then_full_fill() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let (taker, maker1, maker2) = (seed | 1, seed | 2, seed | 3);
    let (taker_user, maker_user) = (seed, seed + 1);
    let instrument = 42_010;
    persist_mysql_shard(
        &store,
        0,
        &[
            kafka_order(taker, taker_user, instrument, Side::Buy, 1_005, 10, 1),
            kafka_order(maker1, maker_user, instrument, Side::Sell, 1_000, 4, 2),
            kafka_order(maker2, maker_user, instrument, Side::Sell, 1_001, 6, 3),
        ],
    )
    .unwrap();

    // Partial: 4 @ 1000 against maker1 (maker fee 2, taker fee 5).
    let mut first = trade_pair(taker, maker1, instrument, 1_000, 4, (2, 5), seed, 0);
    first.push(report_event(wire::RT_FILLED, maker1, instrument, seed, 1));
    let mut partial = report_event(wire::RT_PARTIAL, taker, instrument, seed, 1);
    (partial.event.ordinal, partial.event.report.qty) = (2, 4);
    first.push(partial);
    persist_events(&store, first).unwrap();

    let (status, body) = query_ok(&store, &format!("/orders/{taker}"), "");
    assert_eq!(status, "200 OK");
    assert!(body.contains(&format!("\"order_id\":{taker},\"user_id\":{taker_user},\"instrument\":{instrument},\"side\":\"BUY\",\"price\":1005,\"qty\":10,\"filled_qty\":4,\"avg_fill_price\":1000,\"status\":\"PARTIAL\"")), "{body}");
    let (_, fills) = query_ok(&store, "/fills", &format!("order_id={taker}"));
    assert_eq!(fills.matches("\"order_id\"").count(), 1, "{fills}");
    assert!(
        fills.contains("\"price\":1000,\"qty\":4,\"fee\":5,\"role\":\"taker\""),
        "{fills}"
    );

    // Full: 6 @ 1001 against maker2 (maker fee 3, taker fee 7).
    let mut second = trade_pair(taker, maker2, instrument, 1_001, 6, (3, 7), seed + 1, 0);
    second.push(report_event(
        wire::RT_FILLED,
        maker2,
        instrument,
        seed + 1,
        1,
    ));
    let mut filled = report_event(wire::RT_FILLED, taker, instrument, seed + 1, 1);
    filled.event.ordinal = 2;
    second.push(filled);
    persist_events(&store, second.clone()).unwrap();
    persist_events(&store, second).unwrap(); // Kafka replay: no double fill.

    let (_, body) = query_ok(&store, &format!("/orders/{taker}"), "");
    // (4 * 1000 + 6 * 1001) / 10
    assert!(
        body.contains("\"filled_qty\":10,\"avg_fill_price\":1000.6,\"status\":\"FILLED\""),
        "{body}"
    );
    let (_, body) = query_ok(&store, &format!("/orders/{maker2}"), "");
    assert!(body.contains("\"side\":\"SELL\",\"price\":1001,\"qty\":6,\"filled_qty\":6,\"avg_fill_price\":1001,\"status\":\"FILLED\""), "{body}");

    let (_, fills) = query_ok(&store, "/fills", &format!("order_id={taker}"));
    assert_eq!(fills.matches("\"order_id\"").count(), 2, "{fills}");
    let fill = |price: u64, qty: u64, fee: u64, role: &str, index: u64| {
        format!("\"price\":{price},\"qty\":{qty},\"fee\":{fee},\"role\":\"{role}\"")
            + &format!("|{index}")
    };
    // Newest first; the two fills may share a created_at second, so the
    // tie breaks on raft_index descending.
    let shape = |fills: &str| -> Vec<String> {
        fills
            .split("},{")
            .map(|row| {
                let start = row.find("\"price\"").unwrap();
                let end = row.find(",\"time\"").unwrap();
                let index = row.split("\"raft_index\":").nth(1).unwrap();
                let index: String = index.chars().take_while(char::is_ascii_digit).collect();
                format!("{}|{index}", &row[start..end])
            })
            .collect()
    };
    assert_eq!(
        shape(&fills),
        vec![
            fill(1_001, 6, 7, "taker", seed + 1),
            fill(1_000, 4, 5, "taker", seed)
        ]
    );
    let (_, fills) = query_ok(&store, "/fills", &format!("order_id={maker2}"));
    assert_eq!(shape(&fills), vec![fill(1_001, 6, 3, "maker", seed + 1)]);
    assert!(
        fills.contains(&format!(
            "\"user_id\":{maker_user},\"instrument\":{instrument},\"side\":\"SELL\""
        )),
        "{fills}"
    );

    // Per user, across orders: the maker's two fills, newest first.
    let (_, fills) = query_ok(&store, "/fills", &format!("user={maker_user}"));
    assert_eq!(
        shape(&fills),
        vec![
            fill(1_001, 6, 3, "maker", seed + 1),
            fill(1_000, 4, 2, "maker", seed)
        ]
    );
    let (_, fills) = query_ok(&store, "/fills", &format!("user={maker_user}&limit=1"));
    assert_eq!(shape(&fills), vec![fill(1_001, 6, 3, "maker", seed + 1)]);
    let (_, fills) = query_ok(&store, "/fills", &format!("user={taker_user}"));
    assert_eq!(fills.matches("\"role\":\"taker\"").count(), 2, "{fills}");
    assert_eq!(query_ok(&store, "/fills", "user=1&limit=5").1, "[]");
}

/// Same disposable MySQL 8 as above.
#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_order_queries_get_list_open_filter_and_404() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let seed = (std::process::id() as u64) << 32 | journal::now_nanos() as u64 & 0xffff_ffff;
    let user = seed;
    let instrument = 42_011;
    // 12 orders spread over the test layout's 8 tables.
    let orders: Vec<u64> = (1..=12).map(|n| (seed << 4) | n).collect();
    let records: Vec<KafkaRecord> = orders
        .iter()
        .enumerate()
        .map(|(n, &id)| {
            kafka_order(
                id,
                user,
                instrument,
                Side::Buy,
                1_000 + n as u64,
                5,
                n as i64,
            )
        })
        .collect();
    persist_mysql_shard(&store, 0, &records).unwrap();
    assert!(
        orders
            .iter()
            .map(|&id| store.routing.route_order_id(id).table)
            .collect::<std::collections::HashSet<_>>()
            .len()
            > 1,
        "the list must merge several tables"
    );
    // Distinct creation times (order n created at base + n) make
    // newest-first deterministic.
    let mut conn = store.shard(0).get_conn().unwrap();
    for (n, &id) in orders.iter().enumerate() {
        conn.exec_drop(
            format!(
                "UPDATE order_db_0.{} SET created_at=FROM_UNIXTIME(1700000000+:n) WHERE order_id=:id",
                store.routing.route_order_id(id).table_name()
            ),
            params! {"n" => n as u64, "id" => id},
        )
        .unwrap();
    }
    // Terminal states for the 4 newest: two cancelled, one filled, one
    // rejected; one older order partially filled stays open.
    persist_events(
        &store,
        vec![
            report_event(wire::RT_CANCELLED, orders[11], instrument, seed, 1),
            report_event(wire::RT_CANCELLED, orders[10], instrument, seed + 1, 2),
            report_event(wire::RT_FILLED, orders[9], instrument, seed + 2, 3),
            report_event(wire::RT_REJECTED, orders[8], instrument, seed + 3, 4),
            report_event(wire::RT_RESTING, orders[7], instrument, seed + 4, 5),
            trade_event(orders[6], 0, instrument, 2, seed + 5, 0, 6),
        ],
    )
    .unwrap();

    let ids = |body: &str| -> Vec<u64> {
        body.split("\"order_id\":")
            .skip(1)
            .map(|rest| {
                rest.chars()
                    .take_while(char::is_ascii_digit)
                    .collect::<String>()
                    .parse()
                    .unwrap()
            })
            .collect()
    };
    let newest_first: Vec<u64> = orders.iter().rev().copied().collect();
    let (status, body) = query_ok(&store, "/orders", &format!("user={user}"));
    assert_eq!(status, "200 OK");
    assert_eq!(ids(&body), newest_first);
    let (_, body) = query_ok(
        &store,
        "/orders",
        &format!("user={user}&status=all&limit=3"),
    );
    assert_eq!(ids(&body), newest_first[..3]);
    let (_, body) = query_ok(&store, "/orders", &format!("user={user}&status=open"));
    assert_eq!(ids(&body), newest_first[4..]);
    assert!(body.contains(&format!("\"order_id\":{},\"user_id\":{user},\"instrument\":{instrument},\"side\":\"BUY\",\"price\":1006,\"qty\":5,\"filled_qty\":2,\"avg_fill_price\":1000,\"status\":\"PARTIAL\",\"created_at\":1700000006000,", orders[6])), "{body}");
    assert!(
        body.contains(&format!("\"order_id\":{},", orders[7]))
            && body.contains("\"status\":\"OPEN\""),
        "{body}"
    );
    assert!(body.contains("\"status\":\"PENDING\""), "{body}");
    let (_, body) = query_ok(
        &store,
        "/orders",
        &format!("user={user}&status=open&limit=2"),
    );
    assert_eq!(ids(&body), newest_first[4..6]);
    assert_eq!(
        query_ok(&store, "/orders", &format!("user={}", user + 1)).1,
        "[]"
    );

    let (status, body) = query_ok(&store, &format!("/orders/{}", orders[11]), "");
    assert_eq!(status, "200 OK");
    assert!(
        body.contains("\"status\":\"CANCELLED\",\"created_at\":1700000011000,\"updated_at\":"),
        "{body}"
    );
    let (status, body) = query_ok(&store, &format!("/orders/{}", seed << 4), "");
    assert_eq!(
        (status, body.as_str()),
        ("404 Not Found", "{\"error\":\"order not found\"}")
    );
    assert_eq!(
        query_ok(&store, "/fills", &format!("order_id={}", orders[0])).1,
        "[]"
    );
}

/// Same disposable MySQL 8 as above.
#[test]
fn a_partly_published_batch_is_accepted_with_the_failed_records() {
    assert_eq!(
        batch_accepted_json(3, &[]),
        r#""accepted":true,"status":"PENDING","count":3"#
    );
    let failed = [
        FailedRecord {
            index: 1,
            order_id: 42,
            error: "Message timed out".into(),
        },
        FailedRecord {
            index: 4,
            order_id: 45,
            error: "a \"quoted\" error".into(),
        },
    ];
    assert_eq!(
        batch_accepted_json(3, &failed),
        r#""accepted":true,"status":"PENDING","count":3,"failed":[{"index":1,"order_id":42,"error":"Message timed out"},{"index":4,"order_id":45,"error":"a \"quoted\" error"}]"#
    );
}

#[test]
fn backpressure_follows_a_migrated_category_to_its_new_group() {
    let bp = test_backpressure(100, 250, 750, 4, 4);
    bp.set_group_health(1, false); // group 1 lost quorum
                                   // Category 5 starts on group 1 (5 mod 4).
    assert_eq!(bp.tier_for_category(5), BackpressureTier::Emergency);
    // Moved to group 2 (as order-api applies a migration), it is writable.
    bp.routes.restore_pin(5, 2);
    assert_eq!(bp.raft_group(5), 2);
    assert_eq!(bp.tier_for_category(5), BackpressureTier::Normal);
}

#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_a_failing_later_statement_fails_the_batch() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let mut conn = store.shard(0).get_conn().unwrap();
    let batch = "SELECT 1;\nSELECT * FROM order_db_0.no_such_table_for_this_test";
    // What `query_drop` did: the second statement's error is lost.
    assert!(conn.query_drop(batch).is_ok());
    assert!(exec_batch(&mut conn, batch).is_err());
    assert!(exec_batch(&mut conn, "SELECT 1;\nSELECT 2").is_ok());
    // The connection is usable afterwards.
    assert!(exec_batch(&mut conn, "SELECT 3").is_ok());
}

#[test]
#[ignore = "requires disposable MySQL 8 via TC_TEST_MYSQL_URL"]
fn mysql8_a_different_dedupe_period_refuses_to_start() {
    let store = mysql_test_store().expect("TC_TEST_MYSQL_URL must be set");
    let mut conn = store.shard(0).get_conn().unwrap();
    conn.query_drop("CREATE DATABASE IF NOT EXISTS order_db_period_test")
        .unwrap();
    conn.query_drop("DROP TABLE IF EXISTS order_db_period_test.dedupe_settings")
        .unwrap();
    check_dedupe_period(&mut conn, "order_db_period_test").unwrap();
    check_dedupe_period(&mut conn, "order_db_period_test").unwrap(); // same: fine
    conn.query_drop("UPDATE order_db_period_test.dedupe_settings SET value=value*2")
        .unwrap();
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_dedupe_period(&mut conn, "order_db_period_test")
    }));
    assert!(refused.is_err(), "a changed period must refuse");
    let mut conn = store.shard(0).get_conn().unwrap();
    conn.query_drop("DROP DATABASE order_db_period_test")
        .unwrap();
}

#[test]
fn json_escape_keeps_acks_valid_json() {
    assert_eq!(json_escape("plain"), "plain");
    assert_eq!(json_escape("a\"b\\c\nd\u{1}"), "a\\\"b\\\\c\\u000ad\\u0001");
}

#[test]
fn http_connection_admission_is_bounded_and_observable() {
    let metrics = OrderPipelineMetrics::default();
    assert!(metrics.try_open_http_connection(2));
    assert!(metrics.try_open_http_connection(2));
    assert!(!metrics.try_open_http_connection(2));
    assert_eq!(
        metrics
            .http_active_connections
            .load(AtomicOrdering::Relaxed),
        2
    );
    assert_eq!(
        metrics
            .http_rejected_connections
            .load(AtomicOrdering::Relaxed),
        1
    );
    metrics.close_http_connection();
    assert!(metrics.try_open_http_connection(2));
    metrics.close_http_connection();
    metrics.close_http_connection();
    assert!(metrics
        .render()
        .contains("tc_order_http_active_connections 0"));
}

#[test]
fn lag_monitor_readiness_requires_both_pipeline_stages() {
    let metrics = OrderPipelineMetrics::default();
    assert!(!metrics.lag_monitors_ready());
    metrics.set_lag_monitor_ready("mysql", true);
    assert!(!metrics.lag_monitors_ready());
    metrics.set_lag_monitor_ready("match", true);
    assert!(metrics.lag_monitors_ready());
    metrics.set_lag_monitor_ready("mysql", false);
    assert!(!metrics.lag_monitors_ready());
}
