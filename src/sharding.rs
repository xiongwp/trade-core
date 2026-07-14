//! Order-system database sharding: **10 databases × 100 tables** by asset
//! category.
//!
//! Orders for one category stay together so Kafka partition order, the MySQL
//! outbox and the matching route all share the same ownership boundary.
//!
//! * **deterministic** — every service instance routes the same user to the
//!   same table, forever (resharding is a data migration, not a code change);
//! * **stable** — changing category ownership is an explicit migration rather
//!   than a side effect of adding users.
//!
//! This module is the *routing layer* only — it computes where a row lives and
//! generates the DDL names; actual SQL execution belongs to the order-system
//! service and its connection pools.

use crate::types::InstrumentId;

/// Number of physical databases.
pub const DB_COUNT: u64 = 10;
/// Tables per database.
pub const TABLES_PER_DB: u64 = 100;
/// Total shard slots.
pub const SLOTS: u64 = DB_COUNT * TABLES_PER_DB;
/// Default number of instruments in one ordering category. With the default,
/// instruments 1..=1000 share one ordered stream, 1001..=2000 the next, etc.
pub const DEFAULT_ASSET_CATEGORY_SIZE: u32 = 1_000;

/// Where an asset category's rows live.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ShardRoute {
    /// 0..10
    pub db: u32,
    /// 0..100 (within the database)
    pub table: u32,
}

impl ShardRoute {
    /// Physical database name, e.g. `order_db_3`.
    pub fn db_name(&self) -> String {
        format!("order_db_{}", self.db)
    }

    /// Physical table name, e.g. `orders_042`.
    pub fn table_name(&self) -> String {
        format!("asset_orders_{:03}", self.table)
    }
}

/// Route a category to one of 1,000 stable slots. Consecutive categories are
/// striped across databases first, then tables, spreading early deployments
/// that have fewer than 1,000 categories across all ten MySQL instances.
#[inline]
pub fn route_category(category_id: u32) -> ShardRoute {
    let slot = category_id as u64 % SLOTS;
    ShardRoute {
        db: (slot % DB_COUNT) as u32,
        table: (slot / DB_COUNT) as u32,
    }
}

/// Enumerate every `(db_name, table_name)` pair — handy for generating DDL.
pub fn all_tables() -> impl Iterator<Item = (String, String)> {
    (0..DB_COUNT).flat_map(|db| {
        (0..TABLES_PER_DB).map(move |table| {
            let r = ShardRoute {
                db: db as u32,
                table: table as u32,
            };
            (r.db_name(), r.table_name())
        })
    })
}

/// Route an instrument into the ordering category used by the order outbox.
#[inline]
pub fn asset_category(instrument: InstrumentId, category_size: u32) -> u32 {
    let size = category_size.max(1);
    instrument.0.saturating_sub(1) / size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_routing_is_deterministic_and_in_range() {
        for category in [0u32, 1, 42, 999, u32::MAX] {
            let a = route_category(category);
            let b = route_category(category);
            assert_eq!(a, b);
            assert!(a.db < DB_COUNT as u32);
            assert!(a.table < TABLES_PER_DB as u32);
        }
        assert_eq!(route_category(7).db_name().len(), "order_db_X".len());
        assert_eq!(
            route_category(7).table_name().len(),
            "asset_orders_XXX".len()
        );
    }

    #[test]
    fn sequential_categories_stripe_evenly_across_databases() {
        let mut per_db = [0u64; DB_COUNT as usize];
        let n = 1_000u32;
        for category in 0..n {
            per_db[route_category(category).db as usize] += 1;
        }
        assert!(per_db.iter().all(|count| *count == 100));
        assert_eq!(route_category(0), ShardRoute { db: 0, table: 0 });
        assert_eq!(route_category(10), ShardRoute { db: 0, table: 1 });
    }

    #[test]
    fn table_enumeration_is_complete() {
        assert_eq!(all_tables().count() as u64, SLOTS);
    }

    #[test]
    fn instruments_route_to_ordering_categories() {
        assert_eq!(asset_category(InstrumentId(1), 1_000), 0);
        assert_eq!(asset_category(InstrumentId(1_000), 1_000), 0);
        assert_eq!(asset_category(InstrumentId(1_001), 1_000), 1);
        assert_eq!(asset_category(InstrumentId(0), 1_000), 0);
    }
}
