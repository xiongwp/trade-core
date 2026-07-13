//! Raft-backed command-log core.
//!
//! This module deliberately owns **only** consensus-log concerns. Matching
//! remains outside Raft: callers submit an encoded command, route outbound
//! Raft messages to peers, and feed committed entries to their local matching
//! shards. Consequently, no order may be matched before its log entry appears
//! in [`RaftNode::take_committed`].
//!
//! The core is transport-neutral: the production transport persists and sends
//! [`raft::prelude::Message`] values; the in-memory storage used here makes the
//! consensus state machine directly unit-testable.

use raft::eraftpb::ConfState;
use raft::prelude::{Entry, Message};
use raft::storage::MemStorage;
use raft::{Config, RawNode, StateRole};

/// The commercial deployment topology: one elected leader and four followers.
pub const CLUSTER_SIZE: usize = 5;
/// Number of durable replicas required for a committed command in this topology.
pub const QUORUM: usize = CLUSTER_SIZE / 2 + 1;

/// Static membership for a five-node trading cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub voters: [u64; CLUSTER_SIZE],
    /// Logical tick count before a follower starts an election.
    pub election_tick: usize,
    /// Logical tick count between leader heartbeats.
    pub heartbeat_tick: usize,
}

impl ClusterConfig {
    pub fn new(node_id: u64, voters: [u64; CLUSTER_SIZE]) -> Result<Self, &'static str> {
        if node_id == 0 || !voters.contains(&node_id) {
            return Err("node id must be a member of the five-node cluster");
        }
        let mut sorted = voters;
        sorted.sort_unstable();
        if sorted[0] == 0 || sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err("raft voters must be five distinct non-zero ids");
        }
        Ok(Self {
            node_id,
            voters,
            election_tick: 10,
            heartbeat_tick: 2,
        })
    }
}

/// Why a client command was not proposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
    Raft,
}

/// A transport-neutral Raft command-log node.
///
/// `MemStorage` is intentionally private. The runtime adapter persists every
/// `Ready` record before sending `take_outbound()` messages; keeping that
/// adapter separate prevents consensus code from reaching into matching state.
pub struct RaftNode {
    node: RawNode<MemStorage>,
    store: MemStorage,
    outbound: Vec<Message>,
    committed: Vec<(u64, Vec<u8>)>,
}

impl RaftNode {
    pub fn new(cluster: ClusterConfig) -> Result<Self, raft::Error> {
        let store =
            MemStorage::new_with_conf_state(ConfState::from((cluster.voters.to_vec(), vec![])));
        let cfg = Config {
            id: cluster.node_id,
            election_tick: cluster.election_tick,
            heartbeat_tick: cluster.heartbeat_tick,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };
        let node = RawNode::with_default_logger(&cfg, store.clone())?;
        Ok(Self {
            node,
            store,
            outbound: Vec::new(),
            committed: Vec::new(),
        })
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.node.raft.id
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.node.raft.leader_id
    }

    /// Starts an election. Production nodes normally reach this through ticks;
    /// this method is also useful for deterministic bootstrap tests.
    pub fn campaign(&mut self) -> Result<(), raft::Error> {
        self.node.campaign()?;
        self.drive();
        Ok(())
    }

    /// Propose one already-encoded command. It becomes observable to matching
    /// only after quorum commit, via [`Self::take_committed`].
    pub fn propose(&mut self, command: Vec<u8>) -> Result<(), ProposeError> {
        if !self.is_leader() {
            return Err(ProposeError::NotLeader);
        }
        self.node
            .propose(Vec::new(), command)
            .map_err(|_| ProposeError::Raft)?;
        self.drive();
        Ok(())
    }

    /// Advance logical time and produce heartbeat/election traffic.
    pub fn tick(&mut self) {
        self.node.tick();
        self.drive();
    }

    /// Deliver one peer message received from the cluster transport.
    pub fn step(&mut self, message: Message) -> Result<(), raft::Error> {
        self.node.step(message)?;
        self.drive();
        Ok(())
    }

    /// Raft messages to be sent to their `to` peer. The caller must preserve
    /// each message exactly; TCP framing and mTLS live in the runtime adapter.
    pub fn take_outbound(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.outbound)
    }

    /// Commands that have reached the cluster commit index, in Raft index order.
    pub fn take_committed(&mut self) -> Vec<(u64, Vec<u8>)> {
        std::mem::take(&mut self.committed)
    }

    fn drive(&mut self) {
        while self.node.has_ready() {
            let mut ready = self.node.ready();
            if !ready.entries().is_empty() {
                self.store
                    .wl()
                    .append(ready.entries())
                    .expect("raft entries stay contiguous");
            }
            if let Some(hs) = ready.hs() {
                self.store.wl().set_hardstate(hs.clone());
            }
            self.outbound.extend(ready.take_messages());
            self.apply_entries(ready.take_committed_entries());
            self.outbound.extend(ready.take_persisted_messages());

            let mut light = self.node.advance(ready);
            if let Some(commit) = light.commit_index() {
                self.store.wl().mut_hard_state().set_commit(commit);
            }
            self.outbound.extend(light.take_messages());
            self.apply_entries(light.take_committed_entries());
            self.node.advance_apply();
        }
    }

    fn apply_entries(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            if !entry.data.is_empty() {
                self.committed.push((entry.index, entry.data.to_vec()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VOTERS: [u64; CLUSTER_SIZE] = [1, 2, 3, 4, 5];

    fn node(id: u64) -> RaftNode {
        RaftNode::new(ClusterConfig::new(id, VOTERS).unwrap()).unwrap()
    }

    fn pump(nodes: &mut [RaftNode]) {
        for _ in 0..200 {
            let mut messages = Vec::new();
            for n in nodes.iter_mut() {
                messages.extend(n.take_outbound());
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                let target = nodes.iter_mut().find(|n| n.id() == message.to).unwrap();
                target.step(message).unwrap();
            }
        }
        panic!("raft message pump did not quiesce");
    }

    #[test]
    fn five_voters_commit_only_after_majority_replication() {
        let mut nodes = (1..=5).map(node).collect::<Vec<_>>();
        nodes[0].campaign().unwrap();
        pump(&mut nodes);
        assert!(nodes[0].is_leader());

        nodes[0].propose(b"encoded-order".to_vec()).unwrap();
        assert!(nodes.iter_mut().all(|n| n.take_committed().is_empty()));
        pump(&mut nodes);
        for n in &mut nodes {
            assert_eq!(n.take_committed(), vec![(2, b"encoded-order".to_vec())]);
        }
    }

    #[test]
    fn follower_cannot_accept_client_commands() {
        let mut follower = node(2);
        assert_eq!(follower.propose(vec![1]), Err(ProposeError::NotLeader));
    }
}
