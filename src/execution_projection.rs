//! MySQL projection of execution reports onto `asset_orders_*` rows.
//!
//! Lives in the library (rather than the `order-api` binary that drives it
//! from Kafka) so it can be exercised directly against a real MySQL instance
//! in integration tests without standing up Kafka — see
//! `tests/execution_projection_race.rs`. `order-api`'s bootstrap and
//! `apply_execution_report` call site use the same DDL/apply functions, so
//! there is one definition of the schema and the retry contract, not two
//! that can drift apart.

use mysql::prelude::Queryable;
use mysql::{params, Transaction};

use crate::wire;

/// Idempotency + projection table for the durable execution stream. A `Trade`
/// is delivered as two records sharing `(raft_group, raft_index,
/// report_ordinal)` — one per side (see
/// [`wire::ExecutionEvent::target_order_id`]) — so `order_id` must be part of
/// the primary key or the second side's `INSERT IGNORE` collides with the
/// first and its corresponding `UPDATE` silently never runs.
pub fn processed_execution_events_ddl(db_name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db_name}.processed_execution_events (\
            raft_group INT UNSIGNED NOT NULL, \
            raft_index BIGINT UNSIGNED NOT NULL, \
            report_ordinal INT UNSIGNED NOT NULL, \
            instrument INT UNSIGNED NOT NULL, \
            order_id BIGINT UNSIGNED NOT NULL, \
            report_type TINYINT UNSIGNED NOT NULL, \
            kafka_partition INT NOT NULL, \
            kafka_offset BIGINT UNSIGNED NOT NULL, \
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
            PRIMARY KEY(raft_group,raft_index,report_ordinal,order_id), \
            KEY idx_instrument_index (instrument,raft_index), \
            KEY idx_order_event (order_id,raft_index)\
        ) ENGINE=InnoDB"
    )
}

/// Legacy idempotency table for execution events with no Raft coordinates
/// (`raft_index == 0`).
pub fn processed_executions_ddl(db_name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db_name}.processed_executions (\
            kafka_partition INT NOT NULL, \
            kafka_offset BIGINT UNSIGNED NOT NULL, \
            instrument INT UNSIGNED NOT NULL, \
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
            PRIMARY KEY(kafka_partition,kafka_offset), \
            KEY idx_instrument_offset (instrument,kafka_offset)\
        ) ENGINE=InnoDB"
    )
}

/// The sharded order-row table an execution report's `UPDATE`s target.
pub fn asset_orders_ddl(db_name: &str, table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db_name}.{table} (\
            row_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, \
            order_id BIGINT UNSIGNED NOT NULL UNIQUE, \
            category_id INT UNSIGNED NOT NULL, \
            user_id BIGINT UNSIGNED NOT NULL, \
            instrument INT UNSIGNED NOT NULL, \
            side TINYINT NOT NULL, \
            price BIGINT UNSIGNED NOT NULL, \
            qty BIGINT UNSIGNED NOT NULL, \
            filled_qty BIGINT UNSIGNED NOT NULL DEFAULT 0, \
            status VARCHAR(16) NOT NULL, \
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
            KEY idx_category_row (category_id,row_seq), \
            KEY idx_user_created (user_id,created_at)\
        ) ENGINE=InnoDB"
    )
}

/// Apply one execution event to its target order's row within `tx`.
///
/// The idempotency insert and the order-row update happen in the same
/// transaction: if the `asset_orders_*` row for `target_order_id` doesn't
/// exist yet — the order-intake pipeline (a separate Kafka consumer with its
/// own lag) hasn't caught up — the `UPDATE` matches zero rows, which MySQL
/// does not treat as an error. Returning `Ok(())` in that case would let the
/// idempotency row commit anyway, so this event would never be retried and
/// the fill/status update would be lost for good. Instead this returns `Err`,
/// which the caller must use to abort (not commit) the transaction; the
/// consumer's batch retry then re-delivers the event until the order row is
/// visible.
pub fn apply_execution_report(
    tx: &mut Transaction<'_>,
    db: &str,
    table: &str,
    partition: i32,
    offset: i64,
    event: wire::ExecutionEvent,
    target_order_id: u64,
) -> Result<(), String> {
    let report = event.report;
    if event.raft_index == 0 {
        tx.exec_drop(
            format!("INSERT IGNORE INTO {db}.processed_executions (kafka_partition,kafka_offset,instrument) VALUES (:partition,:offset,:instrument)"),
            params! {
                "partition" => partition,
                "offset" => offset as u64,
                "instrument" => report.instrument.0,
            },
        )
        .map_err(|error| error.to_string())?;
    } else {
        tx.exec_drop(
            format!("INSERT IGNORE INTO {db}.processed_execution_events (raft_group,raft_index,report_ordinal,instrument,order_id,report_type,kafka_partition,kafka_offset) VALUES (:raft_group,:raft_index,:ordinal,:instrument,:order_id,:report_type,:partition,:offset)"),
            params! {
                "raft_group" => event.raft_group,
                "raft_index" => event.raft_index,
                "ordinal" => event.ordinal,
                "instrument" => report.instrument.0,
                "order_id" => target_order_id,
                "report_type" => report.type_code,
                "partition" => partition,
                "offset" => offset as u64,
            },
        )
        .map_err(|error| error.to_string())?;
    }
    if tx.affected_rows() == 0 {
        return Ok(());
    }
    let applied = match report.type_code {
        wire::RT_TRADE => {
            tx.exec_drop(
                format!("UPDATE {db}.{table} SET status=IF(LEAST(qty,filled_qty+:fill)>=qty,'FILLED','PARTIAL'), filled_qty=LEAST(qty,filled_qty+:fill) WHERE order_id=:id"),
                params! {"fill" => report.qty, "id" => target_order_id},
            )
            .map_err(|error| error.to_string())?;
            tx.affected_rows()
        }
        wire::RT_FILLED => {
            tx.exec_drop(
                format!(
                    "UPDATE {db}.{table} SET status='FILLED',filled_qty=qty WHERE order_id=:id"
                ),
                params! {"id" => target_order_id},
            )
            .map_err(|error| error.to_string())?;
            tx.affected_rows()
        }
        wire::RT_PARTIAL => update_execution_status(
            tx,
            db,
            table,
            target_order_id,
            "PARTIAL",
            Some(report.qty),
        )?,
        wire::RT_RESTING | wire::RT_ACCEPTED => {
            update_execution_status(tx, db, table, target_order_id, "OPEN", None)?
        }
        wire::RT_CANCELLED => {
            update_execution_status(tx, db, table, target_order_id, "CANCELLED", None)?
        }
        wire::RT_REJECTED => {
            update_execution_status(tx, db, table, target_order_id, "REJECTED", None)?
        }
        wire::RT_MODIFIED => {
            update_execution_status(tx, db, table, target_order_id, "OPEN", None)?
        }
        _ => return Ok(()),
    };
    if applied == 0 {
        return Err(format!(
            "order row order_id={target_order_id} not yet visible in {db}.{table} for report_type={} (order-intake pipeline lagging behind execution pipeline) — retrying",
            report.type_code
        ));
    }
    Ok(())
}

fn update_execution_status(
    tx: &mut Transaction<'_>,
    db: &str,
    table: &str,
    order_id: u64,
    status: &str,
    filled: Option<u64>,
) -> Result<u64, String> {
    match filled {
        Some(filled) => tx.exec_drop(
            format!("UPDATE {db}.{table} SET status=:status,filled_qty=GREATEST(filled_qty,:filled) WHERE order_id=:id"),
            params! {"status" => status, "filled" => filled, "id" => order_id},
        ),
        None => tx.exec_drop(
            format!("UPDATE {db}.{table} SET status=:status WHERE order_id=:id"),
            params! {"status" => status, "id" => order_id},
        ),
    }
    .map_err(|error| error.to_string())?;
    Ok(tx.affected_rows())
}
