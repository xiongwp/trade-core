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
        check_dedupe_period(&mut conn, &db_name)?;
        for table in 0..DEDUPE_TABLES {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.processed_execution_events_{table} (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, target_order_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,target_order_id)) ENGINE=InnoDB"))?;
        }
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.pending_execution_events (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, target_order_id BIGINT UNSIGNED NOT NULL, shard_table INT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, report_type TINYINT UNSIGNED NOT NULL, event_qty BIGINT UNSIGNED NOT NULL, event_price BIGINT UNSIGNED NOT NULL DEFAULT 0, event_fee BIGINT UNSIGNED NOT NULL DEFAULT 0, fill_role TINYINT UNSIGNED NOT NULL DEFAULT 0, kafka_partition INT NOT NULL, kafka_offset BIGINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,target_order_id), KEY idx_pending_table_order (shard_table,target_order_id,raft_index,report_ordinal)) ENGINE=InnoDB"))?;
        // One row per (fill, side): a Trade is materialized once for the taker
        // and once for the maker, keyed exactly like processed_execution_events.
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.order_fills (raft_group INT UNSIGNED NOT NULL, raft_index BIGINT UNSIGNED NOT NULL, report_ordinal INT UNSIGNED NOT NULL, order_id BIGINT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, fee BIGINT UNSIGNED NOT NULL, fill_role TINYINT UNSIGNED NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(raft_group,raft_index,report_ordinal,order_id), KEY idx_fill_order (order_id,created_at), KEY idx_fill_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
        for table in 0..store.routing.tables_per_db {
            conn.query_drop(format!("CREATE TABLE IF NOT EXISTS {db_name}.asset_orders_{table:03} (row_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id BIGINT UNSIGNED NOT NULL UNIQUE, category_id INT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL, instrument INT UNSIGNED NOT NULL, side TINYINT NOT NULL, price BIGINT UNSIGNED NOT NULL, qty BIGINT UNSIGNED NOT NULL, filled_qty BIGINT UNSIGNED NOT NULL DEFAULT 0, filled_notional DECIMAL(38,0) NOT NULL DEFAULT 0, status VARCHAR(16) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_category_row (category_id,row_seq), KEY idx_user_created (user_id,created_at)) ENGINE=InnoDB"))?;
        }
    }
    Ok(())
}

/// The dedupe tables are keyed by `UNIX_TIMESTAMP() DIV period`: a different
/// `TC_ORDER_DEDUPE_RETENTION_HOURS` renumbers the periods, maps the live
/// history to the "next" table, and the first rotation truncates it. So the
/// period is recorded with the data, and a start with another one refuses.
pub(crate) fn check_dedupe_period(conn: &mut mysql::PooledConn, db_name: &str) -> mysql::Result<()> {
    let period_secs = dedupe_retention_hours() * 3600;
    conn.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {db_name}.dedupe_settings (name VARCHAR(32) NOT NULL PRIMARY KEY, value BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB"
    ))?;
    conn.query_drop(format!(
        "INSERT IGNORE INTO {db_name}.dedupe_settings (name,value) VALUES ('period_secs',{period_secs})"
    ))?;
    let stored: Option<u64> = conn.query_first(format!(
        "SELECT value FROM {db_name}.dedupe_settings WHERE name='period_secs'"
    ))?;
    if let Some(stored) = stored.filter(|stored| *stored != period_secs) {
        panic!(
            "{db_name}: TC_ORDER_DEDUPE_RETENTION_HOURS is {} h but its dedupe history was \
             written with {} h; changing it would truncate live history. Keep {} h, or \
             stop the projection, wait one full period, empty the processed_execution_events_* \
             tables and delete the dedupe_settings row",
            period_secs / 3600,
            stored / 3600,
            stored / 3600,
        );
    }
    Ok(())
}
