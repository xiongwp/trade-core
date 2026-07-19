//! Order-system database sharding: a stable virtual consistent-hash space
//! mapped onto physical MySQL databases and tables.
//!
//! Orders for one category stay together so Kafka partition order, the MySQL
//! projection and the matching route all share the same ownership boundary.
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

/// Number of physical databases (compile-time default / cold-start value).
pub const DB_COUNT: u64 = 10;
/// Tables per database (compile-time default / cold-start value).
pub const TABLES_PER_DB: u64 = 100;
/// Total shard slots for the default configuration.
pub const SLOTS: u64 = DB_COUNT * TABLES_PER_DB;
/// Stable logical database buckets. These do not represent MySQL instances.
pub const VIRTUAL_DB_COUNT: u64 = 1_000;
/// Stable logical table buckets across all virtual databases.
pub const VIRTUAL_TABLE_COUNT: u64 = 10_000;
/// Default number of instruments in one ordering category. With the default,
/// instruments 1..=1000 share one ordered stream, 1001..=2000 the next, etc.
pub const DEFAULT_ASSET_CATEGORY_SIZE: u32 = 1_000;
/// Default route version stamped on a [`RouteConfig`] that was not given one.
pub const DEFAULT_ROUTE_VERSION: u32 = 1;

/// Versioned routing record: the shard fan-out parameters *plus* the route
/// version they belong to. Every service that persists or routes order rows
/// carries the same record, so changing a parameter is an explicit, versioned
/// migration rather than a silent code edit.
///
/// # Changing `db_count` / `tables_per_db` REQUIRES a data migration
///
/// These two numbers drive [`RouteConfig::route_category`]'s modulo striping.
/// Changing either remaps existing categories onto different `(db, table)`
/// slots, so rows written under an old record become unreadable under a new
/// one **unless the data is physically migrated**. The safe procedure is:
/// bump `route_version`, freeze writes for the affected categories, copy rows
/// to their new slots, verify the fingerprint, then atomically switch the
/// version. Never mutate `db_count`/`tables_per_db` in place on a live dataset.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RouteConfig {
    /// Number of physical databases. See the type docs: changing this needs a
    /// data migration, not just a config edit.
    pub db_count: u64,
    /// Tables per database. Same migration caveat as `db_count`.
    pub tables_per_db: u64,
    /// Stable logical database buckets used before physical placement.
    pub virtual_db_count: u64,
    /// Stable logical table buckets used before physical placement.
    pub virtual_table_count: u64,
    /// Version this parameter set belongs to; part of the routing record so a
    /// parameter change is a fenced, versioned migration.
    pub route_version: u32,
}

impl Default for RouteConfig {
    fn default() -> Self {
        Self {
            db_count: DB_COUNT,
            tables_per_db: TABLES_PER_DB,
            virtual_db_count: VIRTUAL_DB_COUNT,
            virtual_table_count: VIRTUAL_TABLE_COUNT,
            route_version: DEFAULT_ROUTE_VERSION,
        }
    }
}

impl RouteConfig {
    /// Build an explicit routing record. Panics on zero parameters — a shard
    /// map with no databases or no tables cannot route anything.
    pub fn new(db_count: u64, tables_per_db: u64, route_version: u32) -> Self {
        Self::with_virtual(
            db_count,
            tables_per_db,
            VIRTUAL_DB_COUNT,
            VIRTUAL_TABLE_COUNT,
            route_version,
        )
    }

    pub fn with_virtual(
        db_count: u64,
        tables_per_db: u64,
        virtual_db_count: u64,
        virtual_table_count: u64,
        route_version: u32,
    ) -> Self {
        assert!(db_count > 0, "at least one physical database is required");
        assert!(
            tables_per_db > 0,
            "at least one table per database is required"
        );
        assert!(
            virtual_db_count > 0,
            "at least one virtual database is required"
        );
        assert!(
            virtual_table_count >= virtual_db_count && virtual_table_count % virtual_db_count == 0,
            "virtual table count must be a multiple of virtual database count"
        );
        Self {
            db_count,
            tables_per_db,
            virtual_db_count,
            virtual_table_count,
            route_version,
        }
    }

    /// Read the routing record from the environment: `TC_DB_COUNT` and
    /// `TC_TABLES_PER_DB` (defaults 10 / 100, preserving the historical shard
    /// map) and `TC_SHARD_ROUTE_VERSION` (default 1). Invalid or zero values
    /// fall back to the defaults.
    pub fn from_env() -> Self {
        let db_count = env_u64("TC_DB_COUNT").unwrap_or(DB_COUNT);
        let tables_per_db = env_u64("TC_TABLES_PER_DB").unwrap_or(TABLES_PER_DB);
        let virtual_db_count = env_u64("TC_VIRTUAL_DB_COUNT").unwrap_or(VIRTUAL_DB_COUNT);
        let virtual_table_count = env_u64("TC_VIRTUAL_TABLE_COUNT").unwrap_or(VIRTUAL_TABLE_COUNT);
        let route_version = std::env::var("TC_SHARD_ROUTE_VERSION")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_ROUTE_VERSION);
        Self::with_virtual(
            db_count,
            tables_per_db,
            virtual_db_count,
            virtual_table_count,
            route_version,
        )
    }

    /// Total shard slots (`db_count * tables_per_db`).
    #[inline]
    pub fn slots(&self) -> u64 {
        self.db_count.saturating_mul(self.tables_per_db)
    }

    /// Route a category to one stable slot. Same striped-modulo algorithm as
    /// the free [`route_category`] function, with the parameters injected:
    /// consecutive categories stripe across databases first, then tables.
    #[inline]
    pub fn route_category(&self, category_id: u32) -> ShardRoute {
        let slot = category_id as u64 % self.slots();
        self.route_slot(slot)
    }

    /// Route an order row using only its order id. Asset/category is
    /// deliberately not part of database placement.
    #[inline]
    pub fn route_order_id(&self, order_id: u64) -> ShardRoute {
        self.route_order_id_full(order_id).physical
    }

    /// Resolve the stable virtual buckets and their current physical
    /// placement. Jump consistent hash moves only about `1/(N+1)` keys when a
    /// physical bucket is added, unlike modulo routing which remaps nearly all
    /// rows. The virtual 1000/10000 space remains fixed across expansions.
    #[inline]
    pub fn route_order_id_full(&self, order_id: u64) -> ConsistentRoute {
        let primary = mix64(order_id);
        let virtual_db = jump_consistent_hash(primary, self.virtual_db_count as u32);
        let virtual_tables_per_db = self.virtual_table_count / self.virtual_db_count;
        let virtual_table_local = jump_consistent_hash(
            mix64(primary ^ 0x9e37_79b9_7f4a_7c15),
            virtual_tables_per_db as u32,
        );
        let virtual_table = virtual_db as u64 * virtual_tables_per_db + virtual_table_local as u64;
        let db = jump_consistent_hash(
            mix64(virtual_db as u64 ^ 0xa076_1d64_78bd_642f),
            self.db_count as u32,
        );
        // A second independent jump hash places rows inside the selected
        // physical database. Hashing the order key (rather than only ten child
        // virtual tables) keeps all 100 physical tables evenly loaded while
        // retaining minimal movement when table capacity is expanded.
        let table = jump_consistent_hash(
            mix64(primary ^ virtual_table ^ 0xe703_7ed1_a0b4_28db),
            self.tables_per_db as u32,
        );
        ConsistentRoute {
            virtual_db,
            virtual_table: virtual_table as u32,
            physical: ShardRoute { db, table },
        }
    }

    /// Partition an order-targeted execution event so every partition belongs
    /// to exactly one physical DB (`partition % db_count == db`). Multiple
    /// lanes per DB preserve parallelism without reintroducing cross-DB work.
    #[inline]
    pub fn execution_partition(&self, order_id: u64, partition_count: u32) -> u32 {
        assert!(
            partition_count >= self.db_count as u32,
            "execution partition count must cover every physical DB"
        );
        let route = self.route_order_id_full(order_id);
        let lanes = (partition_count as u64 / self.db_count).max(1);
        let lane = route.virtual_table as u64 % lanes;
        (route.physical.db as u64 + lane * self.db_count) as u32
    }

    #[inline]
    fn route_slot(&self, slot: u64) -> ShardRoute {
        ShardRoute {
            db: (slot % self.db_count) as u32,
            table: (slot / self.db_count) as u32,
        }
    }

    /// Enumerate every `(db_name, table_name)` pair for this configuration.
    pub fn all_tables(&self) -> impl Iterator<Item = (String, String)> {
        let db_count = self.db_count;
        let tables_per_db = self.tables_per_db;
        (0..db_count).flat_map(move |db| {
            (0..tables_per_db).map(move |table| {
                let r = ShardRoute {
                    db: db as u32,
                    table: table as u32,
                };
                (r.db_name(), r.table_name())
            })
        })
    }
}

/// Full logical and physical location of one order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ConsistentRoute {
    pub virtual_db: u32,
    pub virtual_table: u32,
    pub physical: ShardRoute,
}

/// SplitMix64 finalizer: deterministic, fast and sufficiently avalanche-like
/// for distributing sequential Leaf order ids before consistent hashing.
#[inline]
pub fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Google's jump consistent hash, expressed without floating point so every
/// architecture produces exactly the same route.
#[inline]
pub fn jump_consistent_hash(key: u64, buckets: u32) -> u32 {
    assert!(buckets > 0, "consistent hash needs at least one bucket");
    let mut key = key;
    let mut current: i64 = -1;
    let mut next: i64 = 0;
    while next < buckets as i64 {
        current = next;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        next = (((current + 1) as u128 * (1u128 << 31)) / (((key >> 33) + 1) as u128)) as i64;
    }
    current as u32
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
}

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

/// Route an instrument into its ordered Kafka and persistence category.
#[inline]
pub fn asset_category(instrument: InstrumentId, category_size: u32) -> u32 {
    let size = category_size.max(1);
    instrument.0.saturating_sub(1) / size
}

/// Assign a category to one independent Raft group. This is deliberately the
/// same striping rule used by the Kafka topic router.
#[inline]
pub fn raft_group_for_category(category_id: u32, group_count: usize) -> usize {
    assert!(group_count > 0, "at least one Raft group is required");
    category_id as usize % group_count
}

/// Route with an explicit hot-category override. Control-plane configuration
/// can pin a busy category to a dedicated group while every other category
/// keeps deterministic modulo routing.
#[inline]
pub fn raft_group_for_category_pinned(
    category_id: u32,
    group_count: usize,
    pinned: &std::collections::HashMap<u32, usize>,
) -> usize {
    pinned
        .get(&category_id)
        .copied()
        .filter(|group| *group < group_count)
        .unwrap_or_else(|| raft_group_for_category(category_id, group_count))
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

    #[test]
    fn categories_stripe_across_independent_raft_groups() {
        assert_eq!(raft_group_for_category(0, 4), 0);
        assert_eq!(raft_group_for_category(1, 4), 1);
        assert_eq!(raft_group_for_category(4, 4), 0);
        for category in 0..10_000 {
            assert!(raft_group_for_category(category, 4) < 4);
        }
    }

    #[test]
    fn hot_category_can_own_a_dedicated_group() {
        let pinned = std::collections::HashMap::from([(42, 7)]);
        assert_eq!(raft_group_for_category_pinned(42, 8, &pinned), 7);
        assert_eq!(raft_group_for_category_pinned(43, 8, &pinned), 3);
    }

    #[test]
    fn default_route_config_matches_the_const_routing() {
        let cfg = RouteConfig::default();
        assert_eq!(cfg.db_count, DB_COUNT);
        assert_eq!(cfg.tables_per_db, TABLES_PER_DB);
        assert_eq!(cfg.slots(), SLOTS);
        for category in [0u32, 1, 42, 999, 12_345, u32::MAX] {
            assert_eq!(cfg.route_category(category), route_category(category));
        }
    }

    #[test]
    fn order_ids_stripe_evenly_across_databases_and_tables() {
        let cfg = RouteConfig::default();
        let mut slots = vec![0u64; cfg.slots() as usize];
        for order_id in 0..100_000u64 {
            let route = cfg.route_order_id(order_id);
            let slot = route.table as usize * cfg.db_count as usize + route.db as usize;
            slots[slot] += 1;
        }
        let min = *slots.iter().min().unwrap();
        let max = *slots.iter().max().unwrap();
        assert!(min > 50, "minimum physical slot load {min}");
        assert!(max < 170, "maximum physical slot load {max}");
        assert_eq!(cfg.route_order_id(42), cfg.route_order_id(42));
        let full = cfg.route_order_id_full(42);
        assert!(full.virtual_db < VIRTUAL_DB_COUNT as u32);
        assert!(full.virtual_table < VIRTUAL_TABLE_COUNT as u32);
    }

    #[test]
    fn route_config_is_deterministic_and_in_range_for_any_parameters() {
        for (db_count, tables_per_db) in [(1u64, 1u64), (10, 100), (100, 100), (7, 13)] {
            let cfg = RouteConfig::new(db_count, tables_per_db, 5);
            for category in [0u32, 1, 3, 999, 1_000, 100_000, u32::MAX] {
                let a = cfg.route_category(category);
                // Stable: the same category always lands on the same slot.
                assert_eq!(a, cfg.route_category(category));
                assert!((a.db as u64) < db_count, "db in range for {db_count}");
                assert!((a.table as u64) < tables_per_db, "table in range");
            }
            assert_eq!(cfg.all_tables().count() as u64, cfg.slots());
        }
    }

    #[test]
    fn different_db_count_reshards_slots_as_expected() {
        let before = RouteConfig::new(10, 100, 1);
        let after = RouteConfig::new(11, 100, 2);
        let moved = (1..=100_000u64)
            .filter(|order_id| {
                before.route_order_id(*order_id).db != after.route_order_id(*order_id).db
            })
            .count();
        // Adding one physical DB should move roughly 1/11 of virtual buckets,
        // not remap nearly every order as modulo routing would.
        assert!((7_000..=12_000).contains(&moved), "moved={moved}");
    }

    #[test]
    fn default_virtual_space_is_1000_databases_and_10000_tables() {
        let cfg = RouteConfig::default();
        assert_eq!(cfg.virtual_db_count, 1_000);
        assert_eq!(cfg.virtual_table_count, 10_000);
        assert_eq!(cfg.db_count, 10);
        assert_eq!(cfg.tables_per_db, 100);
    }

    #[test]
    fn execution_partitions_are_stable_and_owned_by_one_database() {
        let cfg = RouteConfig::default();
        for order_id in 1..100_000u64 {
            let partition = cfg.execution_partition(order_id, 40);
            assert_eq!(
                partition % cfg.db_count as u32,
                cfg.route_order_id(order_id).db
            );
            assert_eq!(partition, cfg.execution_partition(order_id, 40));
        }
    }
}
