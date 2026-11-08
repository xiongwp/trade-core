//! Bootstrap DDL: databases, order/fill/dedupe tables, index and column migrations.

use super::*;

pub(crate) fn bootstrap(store: &OrderStore) -> mysql::Result<()> {
    for db in 0..store.routing.db_count {
        if !store.owns(db as u32) {
            continue;
        }
        let mut conn = store.shard(db as u32).get_conn()?;
        let db_name = format!("order_db_{db}");
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db_name}"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_commands (category_id INT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, command_id BIGINT UNSIGNED NOT NULL UNIQUE, user_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(category_id,kafka_offset)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_executions (kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(kafka_partition,kafka_offset), KEY idx_instrument_offset (instrument,kafka_offset)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_execution_events (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, target_order_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,target_order_id)) ENGINE=InnoDB"))?;
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.pending_execution_events (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, target_order_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, event_qty BIGINT UNSIGNED NOT NULL, event_price BIGINT UNSIGNED NOT NULL DEFAULT 0, event_fee BIGINT UNSIGNED NOT NULL DEFAULT 0, fill_role TINYINT UNSIGNED NOT NULL DEFAULT 0, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,target_order_id), KEY idx_pending_table_order (shard_table,target_order_id,raft_index,report_ordinal)) ENGINE=InnoDB"))?;
        // One row per (fill, side): a Trade is materialized once for the taker
        // and once for the maker, keyed exactly like processed_execution_events.
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.order_fills (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, order_id BIGINT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, fee BIGINT UNSIGNED NOT NULL, fill_role TINYINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,order_id), KEY idx_fill_order (order_id,created_at), KEY idx_fill_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
        // Rows staged before `projects_order_state` existed (NotFound, halt
        // acks) can never be promoted; drop them once at startup.
        conn.query_drop(format!(
            "DELETE FROM {db_name}.pending_execution_events WHERE report_type NOT IN ({})",
            projected_report_types_sql()
        ))?;
        for table in 0..store.routing.tables_per_db {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.asset_orders_{table:03} (row_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id BIGINT UNSIGNED NOT NULL UNIQUE, category_id INT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, filled_qty BIGINT UNSIGNED NOT NULL DEFAULT 0, filled_notional DECIMAL(38,0) NOT NULL DEFAULT 0, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_category_row (category_id,row_seq), KEY idx_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
        }
        add_missing_columns(&mut conn, &db_name, store.routing.tables_per_db)?;
        drop_unused_indexes(&mut conn, &db_name)?;
    }
    Ok(())
}

/// Secondary indexes that no query reads (both tables are only looked up by
/// primary key, and per-order fills are served from `order_fills`) but that
/// every command paid for: ~7 index writes per command on the MySQL
/// bottleneck (one per command, two per execution event x ~3.2 events).
pub(crate) const UNUSED_INDEXES: &[(&str, &str)] = &[
    ("processed_commands", "idx_partition_offset"),
    ("processed_execution_events", "idx_instrument_index"),
    ("processed_execution_events", "idx_order_event"),
];

/// Drop [`UNUSED_INDEXES`] from tables created before they were removed from
/// the DDL. Dropping a secondary index is an in-place, metadata-only change.
pub(crate) fn drop_unused_indexes(conn: &mut mysql::PooledConn, db_name: &str) -> mysql::Result<()> {
    for (table, index) in UNUSED_INDEXES {
        let present: Option<u8> = conn.exec_first(
            "SELECT 1 FROM information_schema.statistics WHERE table_schema=:db AND table_name=:t AND index_name=:i LIMIT 1",
            params! {"db" => db_name, "t" => *table, "i" => *index},
        )?;
        if present.is_some() {
            conn.query_drop(format!("ALTER TABLE {db_name}.{table} DROP INDEX {index}"))?;
        }
    }
    Ok(())
}

/// Columns added after the first deployments. `CREATE TABLE IF NOT EXISTS`
/// leaves existing tables alone, and MySQL 8.4 has no `ADD COLUMN IF NOT
/// EXISTS`, so add whatever information_schema reports missing. Each is
/// trailing with a constant default, which 8.4 applies as an INSTANT change.
pub(crate) const PENDING_EVENT_COLUMNS: &[(&str, &str)] = &[
    ("event_price", "BIGINT UNSIGNED NOT NULL DEFAULT 0"),
    ("event_fee", "BIGINT UNSIGNED NOT NULL DEFAULT 0"),
    ("fill_role", "TINYINT UNSIGNED NOT NULL DEFAULT 0"),
];
pub(crate) const ASSET_ORDER_COLUMNS: &[(&str, &str)] =
    &[("filled_notional", "DECIMAL(38,0) NOT NULL DEFAULT 0")];

pub(crate) fn add_missing_columns(
    conn: &mut mysql::PooledConn,
    db_name: &str,
    tables_per_db: u64,
) -> mysql::Result<()> {
    let present: std::collections::HashSet<(String, String)> = conn
        .exec::<(String, String), _, _>(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=?",
            (db_name,),
        )?
        .into_iter()
        .collect();
    let mut wanted = vec![("pending_execution_events".to_string(), PENDING_EVENT_COLUMNS)];
    wanted.extend((0..tables_per_db).map(|table| (format!("asset_orders_{table:03}"), ASSET_ORDER_COLUMNS)));
    for (table, columns) in wanted {
        let missing = columns
            .iter()
            .filter(|(column, _)| !present.contains(&(table.clone(), column.to_string())))
            .map(|(column, ddl)| format!("ADD COLUMN {column} {ddl}"))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            conn.query_drop(format!("ALTER TABLE {db_name}.{table} {}", missing.join(", ")))?;
        }
    }
    Ok(())
}
