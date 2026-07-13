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
}
