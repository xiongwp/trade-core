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
}

impl ClusterMap {
    /// A cluster of `nodes` with pure hash routing.
    pub fn new(nodes: Vec<String>) -> Self {
        assert!(!nodes.is_empty(), "cluster needs at least one node");
        ClusterMap { nodes, pinned: Vec::new() }
    }

    /// Pin an instrument to a specific node (e.g. keep a hot symbol alone).
    pub fn pin(mut self, instrument: InstrumentId, node: usize) -> Self {
        assert!(node < self.nodes.len(), "node {node} out of range");
        self.pinned.push((instrument, node));
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
}
