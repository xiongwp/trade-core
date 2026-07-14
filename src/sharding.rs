//! Order-system database sharding: **10 databases × 100 tables** by user id.
//!
//! The order system persists orders/positions per user. To spread load it
//! shards by `user_id` into 1000 slots mapped onto 10 physical databases of 100
//! tables each. The mapping must be:
//!
//! * **deterministic** — every service instance routes the same user to the
//!   same table, forever (resharding is a data migration, not a code change);
//! * **uniform** — user ids may be sequential (Leaf-style), so the slot is
//!   taken from a mixed hash rather than raw modulo of possibly-skewed ids.
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

/// Where a user's rows live.
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
        format!("orders_{:03}", self.table)
    }
}

/// Mix the user id so sequential ids spread uniformly (splitmix64 finalizer).
#[inline]
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Route a user id to its database and table.
#[inline]
pub fn route(user_id: u64) -> ShardRoute {
    let slot = mix(user_id) % SLOTS;
    ShardRoute {
        db: (slot / TABLES_PER_DB) as u32,
        table: (slot % TABLES_PER_DB) as u32,
    }
}

/// Enumerate every `(db_name, table_name)` pair — handy for generating DDL.
pub fn all_tables() -> impl Iterator<Item = (String, String)> {
    (0..DB_COUNT).flat_map(|db| {
        (0..TABLES_PER_DB).map(move |table| {
            let r = ShardRoute { db: db as u32, table: table as u32 };
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
    fn routing_is_deterministic_and_in_range() {
        for uid in [0u64, 1, 42, 1_000_000, u64::MAX] {
            let a = route(uid);
            let b = route(uid);
            assert_eq!(a, b);
            assert!(a.db < DB_COUNT as u32);
            assert!(a.table < TABLES_PER_DB as u32);
        }
        assert_eq!(route(7).db_name().len(), "order_db_X".len());
        assert_eq!(route(7).table_name().len(), "orders_XXX".len());
    }

    #[test]
    fn sequential_users_spread_uniformly() {
        // 100k sequential user ids (the Leaf-id worst case for raw modulo)
        // should land near-uniformly across the 1000 slots.
        let mut per_db = [0u64; DB_COUNT as usize];
        let n = 100_000u64;
        for uid in 0..n {
            per_db[route(uid).db as usize] += 1;
        }
        let expect = n / DB_COUNT;
        for (db, &count) in per_db.iter().enumerate() {
            let dev = count.abs_diff(expect) as f64 / expect as f64;
            assert!(dev < 0.05, "db {db} skewed: {count} vs {expect}");
        }
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
