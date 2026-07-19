//! Horizontal scaling: routing instruments to matching **nodes** (machines).
//!
//! The share-nothing design extends naturally across machines: a matching node
//! is just a process running some shards, owning a disjoint set of instruments.
//! Nothing is shared between nodes — no distributed locks, no cross-node order
//! flow — so scaling out is purely a routing concern: *which node owns which
//! instrument?*
//!
//! [`ClusterMap`] is that routing table. The order system holds one, opens one
//! long-lived TCP connection per node, and sends each command down the
//! connection owned by the command's instrument. Rebalancing instruments means
//! updating the map and draining/replaying the moved instrument's journal on its
//! new node — the journal (see [`crate::journal`]) is per-shard, so an
//! instrument's command history moves with it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::types::InstrumentId;

/// A static instrument → node routing table.
///
/// Explicit assignments take priority; unassigned instruments fall back to
/// `instrument.0 % nodes.len()`. Keep the map identical on every client — it is
/// versioned configuration, not runtime state.
#[derive(Clone, Debug)]
pub struct ClusterMap {
    /// Node addresses, e.g. `"10.0.0.5:9001"`. Index = node id.
    nodes: Vec<String>,
    /// Explicit overrides: (instrument, node index).
    pinned: Vec<(InstrumentId, usize)>,
    /// Inclusive instrument ranges assigned to a node. Ranges take precedence
    /// over hash fallback and allow deterministic machine ownership such as
    /// node A = 1..=5000, node B = 5001..=10000.
    ranges: Vec<(InstrumentId, InstrumentId, usize)>,
}

impl ClusterMap {
    /// A cluster of `nodes` with pure hash routing.
    pub fn new(nodes: Vec<String>) -> Self {
        assert!(!nodes.is_empty(), "cluster needs at least one node");
        ClusterMap {
            nodes,
            pinned: Vec::new(),
            ranges: Vec::new(),
        }
    }

    /// Pin an instrument to a specific node (e.g. keep a hot symbol alone).
    pub fn pin(mut self, instrument: InstrumentId, node: usize) -> Self {
        assert!(node < self.nodes.len(), "node {node} out of range");
        self.pinned.push((instrument, node));
        self
    }

    /// Assign an inclusive instrument-id range to one node. Later explicit
    /// single-instrument pins still take precedence, which is useful for
    /// moving a hot asset without rewriting the wider allocation.
    pub fn pin_range(mut self, start: InstrumentId, end: InstrumentId, node: usize) -> Self {
        assert!(start <= end, "instrument range start must not exceed end");
        assert!(node < self.nodes.len(), "node {node} out of range");
        assert!(
            !self
                .ranges
                .iter()
                .any(|&(lo, hi, _)| start <= hi && lo <= end),
            "instrument ranges must not overlap"
        );
        self.ranges.push((start, end, node));
        self
    }

    /// Number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// True if the cluster has no nodes (never, by construction).
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The node index that owns `instrument`.
    pub fn node_for(&self, instrument: InstrumentId) -> usize {
        self.pinned
            .iter()
            .find(|(i, _)| *i == instrument)
            .map(|&(_, n)| n)
            .or_else(|| {
                self.ranges
                    .iter()
                    .find(|&&(start, end, _)| start <= instrument && instrument <= end)
                    .map(|&(_, _, node)| node)
            })
            .unwrap_or(instrument.0 as usize % self.nodes.len())
    }

    /// The address of the node that owns `instrument`.
    pub fn addr_for(&self, instrument: InstrumentId) -> &str {
        &self.nodes[self.node_for(instrument)]
    }

    /// All node addresses (to open one connection per node).
    pub fn addrs(&self) -> &[String] {
        &self.nodes
    }
}

/// Dynamically replaceable routing configuration held by order gateways.
///
/// Each replacement gets a monotonically increasing version. Clients should
/// tag a command stream with the version they observed; the migration control
/// plane activates a replacement only after the affected books are frozen,
/// copied and verified on the new owner.
#[derive(Clone)]
pub struct ClusterRouter {
    map: Arc<RwLock<ClusterMap>>,
    version: Arc<AtomicU64>,
}

impl ClusterRouter {
    pub fn new(map: ClusterMap) -> Self {
        Self {
            map: Arc::new(RwLock::new(map)),
            version: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn node_for(&self, instrument: InstrumentId) -> (usize, u64) {
        let version = self.version();
        let node = self.map.read().unwrap().node_for(instrument);
        (node, version)
    }

    /// Atomically install a fully validated replacement map and return its
    /// version. This changes client routing only; it intentionally does not
    /// bypass the book-migration fence described above.
    pub fn replace(&self, map: ClusterMap) -> u64 {
        *self.map.write().unwrap() = map;
        self.version.fetch_add(1, Ordering::AcqRel) + 1
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationState {
    Freezing,
    CatchingUp,
    Verified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CategoryMigration {
    pub category_id: u32,
    pub route_version: u64,
    pub from_group: usize,
    pub to_group: usize,
    pub state: MigrationState,
    pub frozen_index: u64,
    pub target_index: u64,
    pub fingerprint: u64,
}

/// Fenced category migration state machine. Data movement may replay Kafka or
/// install a matching snapshot, but routing cannot activate the destination
/// until both sides report the same durable index and state fingerprint.
#[derive(Clone)]
pub struct RouteControlPlane {
    group_count: usize,
    pins: Arc<RwLock<HashMap<u32, usize>>>,
    migrations: Arc<RwLock<HashMap<u32, CategoryMigration>>>,
    version: Arc<AtomicU64>,
}

impl RouteControlPlane {
    pub fn new(group_count: usize, pins: HashMap<u32, usize>) -> Self {
        assert!(group_count > 0, "route control plane needs a Raft group");
        assert!(pins.values().all(|group| *group < group_count));
        Self {
            group_count,
            pins: Arc::new(RwLock::new(pins)),
            migrations: Arc::new(RwLock::new(HashMap::new())),
            version: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn group_for(&self, category_id: u32) -> usize {
        self.pins
            .read()
            .unwrap()
            .get(&category_id)
            .copied()
            .unwrap_or(category_id as usize % self.group_count)
    }

    pub fn accepts(&self, category_id: u32) -> bool {
        !self.migrations.read().unwrap().contains_key(&category_id)
    }

    pub fn begin(&self, category_id: u32, to_group: usize) -> Result<CategoryMigration, String> {
        if to_group >= self.group_count {
            return Err(format!("target group {to_group} is out of range"));
        }
        let from_group = self.group_for(category_id);
        if from_group == to_group {
            return Err("category already belongs to target group".into());
        }
        let mut migrations = self.migrations.write().unwrap();
        if migrations.contains_key(&category_id) {
            return Err("category migration already active".into());
        }
        let migration = CategoryMigration {
            category_id,
            route_version: self.version.fetch_add(1, Ordering::AcqRel) + 1,
            from_group,
            to_group,
            state: MigrationState::Freezing,
            frozen_index: 0,
            target_index: 0,
            fingerprint: 0,
        };
        migrations.insert(category_id, migration);
        Ok(migration)
    }

    pub fn frozen(&self, category_id: u32, index: u64) -> Result<CategoryMigration, String> {
        let mut migrations = self.migrations.write().unwrap();
        let migration = migrations
            .get_mut(&category_id)
            .ok_or("category migration is not active")?;
        if migration.state != MigrationState::Freezing || index == 0 {
            return Err("migration is not waiting for a valid freeze index".into());
        }
        migration.frozen_index = index;
        migration.state = MigrationState::CatchingUp;
        Ok(*migration)
    }

    pub fn caught_up(
        &self,
        category_id: u32,
        target_index: u64,
        source_fingerprint: u64,
        target_fingerprint: u64,
    ) -> Result<CategoryMigration, String> {
        let mut migrations = self.migrations.write().unwrap();
        let migration = migrations
            .get_mut(&category_id)
            .ok_or("category migration is not active")?;
        if migration.state != MigrationState::CatchingUp || target_index < migration.frozen_index {
            return Err("target has not caught up to the freeze index".into());
        }
        if source_fingerprint != target_fingerprint {
            return Err("source/target matching fingerprints differ".into());
        }
        migration.target_index = target_index;
        migration.fingerprint = source_fingerprint;
        migration.state = MigrationState::Verified;
        Ok(*migration)
    }

    pub fn activate(&self, category_id: u32) -> Result<u64, String> {
        let mut migrations = self.migrations.write().unwrap();
        let migration = migrations
            .get(&category_id)
            .copied()
            .ok_or("category migration is not active")?;
        if migration.state != MigrationState::Verified {
            return Err("migration fingerprint is not verified".into());
        }
        self.pins
            .write()
            .unwrap()
            .insert(category_id, migration.to_group);
        migrations.remove(&category_id);
        Ok(self.version.fetch_add(1, Ordering::AcqRel) + 1)
    }

    pub fn abort(&self, category_id: u32) -> bool {
        self.migrations
            .write()
            .unwrap()
            .remove(&category_id)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_deterministically_with_pins() {
        let map = ClusterMap::new(vec![
            "10.0.0.1:9001".into(),
            "10.0.0.2:9001".into(),
            "10.0.0.3:9001".into(),
        ])
        .pin(InstrumentId(42), 0);

        // Hash fallback.
        assert_eq!(map.node_for(InstrumentId(0)), 0);
        assert_eq!(map.node_for(InstrumentId(1)), 1);
        assert_eq!(map.node_for(InstrumentId(5)), 2);
        // Pinned override (42 % 3 == 0 anyway; use 43 to prove the override).
        let map = map.pin(InstrumentId(43), 0);
        assert_eq!(map.node_for(InstrumentId(43)), 0);
        assert_eq!(map.addr_for(InstrumentId(43)), "10.0.0.1:9001");
    }

    #[test]
    fn routes_large_disjoint_asset_ranges_to_their_owners() {
        let map = ClusterMap::new(vec!["10.0.0.1:9001".into(), "10.0.0.2:9001".into()])
            .pin_range(InstrumentId(1), InstrumentId(5_000), 0)
            .pin_range(InstrumentId(5_001), InstrumentId(10_000), 1);

        assert_eq!(map.node_for(InstrumentId(1)), 0);
        assert_eq!(map.node_for(InstrumentId(5_000)), 0);
        assert_eq!(map.node_for(InstrumentId(5_001)), 1);
        assert_eq!(map.node_for(InstrumentId(10_000)), 1);
    }

    #[test]
    fn router_swaps_ownership_atomically_and_versions_the_change() {
        let router = ClusterRouter::new(
            ClusterMap::new(vec!["a:9001".into(), "b:9001".into()]).pin_range(
                InstrumentId(1),
                InstrumentId(5_000),
                0,
            ),
        );
        assert_eq!(router.node_for(InstrumentId(42)), (0, 1));

        let version = router.replace(
            ClusterMap::new(vec!["a:9001".into(), "b:9001".into()]).pin_range(
                InstrumentId(1),
                InstrumentId(5_000),
                1,
            ),
        );
        assert_eq!(version, 2);
        assert_eq!(router.node_for(InstrumentId(42)), (1, 2));
    }

    #[test]
    fn category_migration_freezes_verifies_and_switches_atomically() {
        let control = RouteControlPlane::new(4, HashMap::new());
        assert_eq!(control.group_for(1), 1);
        let started = control.begin(1, 3).unwrap();
        assert_eq!(started.state, MigrationState::Freezing);
        assert!(!control.accepts(1));
        assert!(control.activate(1).is_err());
        control.frozen(1, 90).unwrap();
        assert!(control.caught_up(1, 89, 7, 7).is_err());
        assert!(control.caught_up(1, 90, 7, 8).is_err());
        control.caught_up(1, 90, 7, 7).unwrap();
        control.activate(1).unwrap();
        assert!(control.accepts(1));
        assert_eq!(control.group_for(1), 3);
    }
}
