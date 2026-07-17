//! Raft-backed command-log core.
//!
//! This module deliberately owns **only** consensus-log concerns. Matching
//! remains outside Raft: callers submit an encoded command, route outbound
//! Raft messages to peers, and feed committed entries to their local matching
//! shards. Consequently, no order may be matched before its log entry appears
//! in [`RaftNode::take_committed`].
//!
//! The core is transport-neutral: [`RaftNode::open`] persists and restores its
//! consensus state, while [`RaftNode::new`] retains an in-memory mode for
//! deterministic unit tests.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use protobuf::Message as PbMessage;
use raft::eraftpb::{ConfState, HardState};
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

const STORAGE_HEADER: [u8; 8] = *b"TCRF\x01\0\0\0";
const HARD_STATE_RECORD: u8 = 1;
const ENTRY_RECORD: u8 = 2;

/// Append-only durable state for one Raft member.
///
/// Records are checksummed and each [`RaftNode::drive`] synchronizes the batch
/// before its messages are exposed to the transport. A corrupt or torn tail is
/// rejected at startup: consensus must not guess at a vote or commit index.
struct DurableRaftLog {
    file: File,
}

impl DurableRaftLog {
    fn open(path: &Path) -> io::Result<(Self, Vec<Entry>, HardState)> {
        let mut read = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if read.metadata()?.len() < STORAGE_HEADER.len() as u64 {
            read.set_len(0)?;
            read.seek(SeekFrom::Start(0))?;
            read.write_all(&STORAGE_HEADER)?;
            read.sync_all()?;
        }
        read.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; STORAGE_HEADER.len()];
        read.read_exact(&mut header)?;
        if header != STORAGE_HEADER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raft state header/version mismatch",
            ));
        }

        let mut entries = Vec::new();
        let mut hard_state = HardState::default();
        loop {
            let record_start = read.stream_position()?;
            let mut kind = [0u8; 1];
            match read.read_exact(&mut kind) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            let mut length_bytes = [0u8; 4];
            if !read_or_truncate_torn_tail(&mut read, &mut length_bytes, record_start)? {
                break;
            }
            let length = u32::from_le_bytes(length_bytes) as usize;
            if length > 16 * 1024 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raft state record is too large",
                ));
            }
            let mut payload = vec![0; length];
            if !read_or_truncate_torn_tail(&mut read, &mut payload, record_start)? {
                break;
            }
            let mut checksum = [0u8; 8];
            if !read_or_truncate_torn_tail(&mut read, &mut checksum, record_start)? {
                break;
            }
            let mut protected = Vec::with_capacity(5 + payload.len());
            protected.push(kind[0]);
            protected.extend_from_slice(&length_bytes);
            protected.extend_from_slice(&payload);
            if crate::journal::fnv1a(&protected) != u64::from_le_bytes(checksum) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raft state checksum mismatch",
                ));
            }
            match kind[0] {
                HARD_STATE_RECORD => {
                    hard_state = HardState::parse_from_bytes(&payload).map_err(proto_error)?
                }
                ENTRY_RECORD => {
                    let entry = Entry::parse_from_bytes(&payload).map_err(proto_error)?;
                    append_recovered_entry(&mut entries, entry)?;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown raft state record",
                    ))
                }
            }
        }
        if hard_state.commit > entries.last().map(|entry| entry.index).unwrap_or(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raft commit index is ahead of the durable log",
            ));
        }
        let file = OpenOptions::new().append(true).open(path)?;
        Ok((Self { file }, entries, hard_state))
    }

    fn persist(&mut self, entries: &[Entry], hard_state: Option<&HardState>) -> io::Result<()> {
        for entry in entries {
            self.write_record(ENTRY_RECORD, &entry.write_to_bytes().map_err(proto_error)?)?;
        }
        // A committed HardState may reference these entries, so it must never
        // precede them in the crash-recoverable record stream.
        if let Some(hard_state) = hard_state {
            self.write_record(
                HARD_STATE_RECORD,
                &hard_state.write_to_bytes().map_err(proto_error)?,
            )?;
        }
        if hard_state.is_some() || !entries.is_empty() {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn write_record(&mut self, kind: u8, payload: &[u8]) -> io::Result<()> {
        let length = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "raft record too large"))?;
        let mut protected = Vec::with_capacity(5 + payload.len());
        protected.push(kind);
        protected.extend_from_slice(&length.to_le_bytes());
        protected.extend_from_slice(payload);
        self.file.write_all(&protected)?;
        self.file
            .write_all(&crate::journal::fnv1a(&protected).to_le_bytes())
    }
}

fn read_or_truncate_torn_tail(
    file: &mut File,
    bytes: &mut [u8],
    record_start: u64,
) -> io::Result<bool> {
    match file.read_exact(bytes) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            file.set_len(record_start)?;
            file.sync_all()?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn append_recovered_entry(entries: &mut Vec<Entry>, entry: Entry) -> io::Result<()> {
    if entry.index == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft log contains index zero",
        ));
    }
    if let Some(position) = entries
        .iter()
        .position(|existing| existing.index >= entry.index)
    {
        entries.truncate(position);
    }
    if let Some(previous) = entries.last() {
        if entry.index != previous.index + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raft log contains an index gap",
            ));
        }
    }
    entries.push(entry);
    Ok(())
}

fn proto_error(error: protobuf::ProtobufError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// A transport-neutral Raft command-log node.
///
/// `MemStorage` is intentionally private. [`RaftNode::open`] persists every
/// `Ready` record before exposing `take_outbound()` messages; keeping matching
/// outside this type prevents consensus code from reaching into book state.
pub struct RaftNode {
    node: RawNode<MemStorage>,
    store: MemStorage,
    outbound: Vec<Message>,
    committed: Vec<(u64, Vec<u8>)>,
    durable: Option<DurableRaftLog>,
}

impl RaftNode {
    pub fn new(cluster: ClusterConfig) -> Result<Self, raft::Error> {
        Self::with_storage(cluster, None, Vec::new(), HardState::default())
    }

    /// Open a real member from a durable state file. The file holds Raft's
    /// term, vote, commit index and all un-compacted entries, not just a copy
    /// of application commands.
    pub fn open(cluster: ClusterConfig, path: impl AsRef<Path>) -> io::Result<Self> {
        let (durable, entries, hard_state) = DurableRaftLog::open(path.as_ref())?;
        // Raft's durable commit index can be ahead of the local matching
        // state machine when a process dies after quorum commit but before it
        // reaches a shard. Re-expose that committed prefix on startup; the
        // application dispatcher is responsible for idempotent application.
        let recovered_committed = entries
            .iter()
            .filter(|entry| entry.index <= hard_state.commit && !entry.data.is_empty())
            .map(|entry| (entry.index, entry.data.to_vec()))
            .collect();
        let mut node = Self::with_storage(cluster, Some(durable), entries, hard_state)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        node.committed = recovered_committed;
        Ok(node)
    }

    fn with_storage(
        cluster: ClusterConfig,
        durable: Option<DurableRaftLog>,
        entries: Vec<Entry>,
        hard_state: HardState,
    ) -> Result<Self, raft::Error> {
        let store =
            MemStorage::new_with_conf_state(ConfState::from((cluster.voters.to_vec(), vec![])));
        if !entries.is_empty() {
            store.wl().append(&entries)?;
        }
        let applied = hard_state.commit;
        if hard_state != HardState::default() {
            store.wl().set_hardstate(hard_state);
        }
        let cfg = Config {
            id: cluster.node_id,
            election_tick: cluster.election_tick,
            heartbeat_tick: cluster.heartbeat_tick,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            applied,
            ..Default::default()
        };
        let node = RawNode::with_default_logger(&cfg, store.clone())?;
        Ok(Self {
            node,
            store,
            outbound: Vec::new(),
            committed: Vec::new(),
            durable,
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

    #[inline]
    pub fn term(&self) -> u64 {
        self.node.raft.term
    }

    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.node.raft.raft_log.committed
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.node.raft.raft_log.last_index()
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
        self.propose_batch(std::iter::once(command))
    }

    /// Append a bounded group of commands and persist the resulting Ready once.
    /// The runtime uses this to amortize fsync without weakening durability:
    /// replication messages are still exposed only after the whole batch is on
    /// stable storage.
    pub fn propose_batch<I>(&mut self, commands: I) -> Result<(), ProposeError>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        if !self.is_leader() {
            return Err(ProposeError::NotLeader);
        }
        for command in commands {
            self.node
                .propose(Vec::new(), command)
                .map_err(|_| ProposeError::Raft)?;
        }
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
    /// each message exactly; TCP framing lives in the runtime adapter.
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
            if let Some(durable) = &mut self.durable {
                durable
                    .persist(ready.entries(), ready.hs())
                    .expect("persist raft Ready before transport");
            }
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

    fn log_entry(index: u64, term: u64, data: &[u8]) -> Entry {
        Entry {
            index,
            term,
            data: data.to_vec().into(),
            ..Default::default()
        }
    }

    fn pump(nodes: &mut [RaftNode]) {
        pump_active(nodes, &[true; CLUSTER_SIZE]);
    }

    fn pump_active(nodes: &mut [RaftNode], active: &[bool; CLUSTER_SIZE]) {
        for _ in 0..200 {
            let mut messages = Vec::new();
            for (index, n) in nodes.iter_mut().enumerate() {
                if active[index] {
                    messages.extend(n.take_outbound());
                }
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if let Some((index, target)) = nodes
                    .iter_mut()
                    .enumerate()
                    .find(|(_, n)| n.id() == message.to)
                {
                    if active[index] {
                        target.step(message).unwrap();
                    }
                }
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

    #[test]
    fn recovery_truncates_an_overwritten_uncommitted_suffix() {
        let mut entries = vec![
            log_entry(1, 1, b"one"),
            log_entry(2, 1, b"old-two"),
            log_entry(3, 1, b"old-three"),
        ];
        append_recovered_entry(&mut entries, log_entry(2, 2, b"new-two")).unwrap();
        append_recovered_entry(&mut entries, log_entry(3, 2, b"new-three")).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.index, entry.term, entry.data.to_vec()))
                .collect::<Vec<_>>(),
            vec![
                (1, 1, b"one".to_vec()),
                (2, 2, b"new-two".to_vec()),
                (3, 2, b"new-three".to_vec()),
            ]
        );
    }

    #[test]
    fn surviving_quorum_elects_a_new_leader_and_commits_after_leader_failure() {
        let mut nodes = (1..=5).map(node).collect::<Vec<_>>();
        nodes[0].campaign().unwrap();
        pump(&mut nodes);
        nodes[0].propose(b"before-failure".to_vec()).unwrap();
        pump(&mut nodes);
        for node in &mut nodes {
            assert_eq!(node.take_committed(), vec![(2, b"before-failure".to_vec())]);
        }

        // Node 1 is a hard machine failure: no tick, no outbound delivery and
        // no inbound delivery. Four surviving voters still exceed quorum.
        let active = [false, true, true, true, true];
        for _ in 0..100 {
            for (index, node) in nodes.iter_mut().enumerate() {
                if active[index] {
                    node.tick();
                }
            }
            pump_active(&mut nodes, &active);
            if nodes
                .iter()
                .enumerate()
                .any(|(index, n)| active[index] && n.is_leader())
            {
                break;
            }
        }
        let leader = nodes
            .iter()
            .enumerate()
            .find(|(index, n)| active[*index] && n.is_leader())
            .map(|(index, _)| index)
            .expect("surviving quorum must elect a leader");
        nodes[leader].propose(b"after-failure".to_vec()).unwrap();
        pump_active(&mut nodes, &active);
        for (index, node) in nodes.iter_mut().enumerate() {
            if active[index] {
                let committed = node.take_committed();
                assert_eq!(committed.len(), 1);
                assert!(committed[0].0 > 2, "new leader may append a no-op first");
                assert_eq!(committed[0].1, b"after-failure".to_vec());
            }
        }
    }

    #[test]
    fn durable_member_restores_term_commit_and_entries_after_restart() {
        let path = std::env::temp_dir().join(format!(
            "tc-raft-state-{}-{}.bin",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = std::fs::remove_file(&path);
        let config = ClusterConfig::new(1, VOTERS).unwrap();
        let mut leader = RaftNode::open(config.clone(), &path).unwrap();
        let mut followers = (2..=5).map(node).collect::<Vec<_>>();

        leader.campaign().unwrap();
        for _ in 0..20 {
            let messages = leader.take_outbound();
            if messages.is_empty() {
                break;
            }
            for message in messages {
                let target = followers.iter_mut().find(|n| n.id() == message.to).unwrap();
                target.step(message).unwrap();
            }
            for follower in &mut followers {
                for message in follower.take_outbound() {
                    if message.to == leader.id() {
                        leader.step(message).unwrap();
                    }
                }
            }
        }
        assert!(leader.is_leader());
        leader.propose(b"durable-order".to_vec()).unwrap();
        for _ in 0..20 {
            let messages = leader.take_outbound();
            if messages.is_empty() {
                break;
            }
            for message in messages {
                let target = followers.iter_mut().find(|n| n.id() == message.to).unwrap();
                target.step(message).unwrap();
            }
            for follower in &mut followers {
                for message in follower.take_outbound() {
                    if message.to == leader.id() {
                        leader.step(message).unwrap();
                    }
                }
            }
        }
        let term = leader.term();
        let commit = leader.commit_index();
        assert!(term > 0);
        assert!(commit >= 2);
        drop(leader);

        let restored = RaftNode::open(config, &path).unwrap();
        assert_eq!(restored.term(), term);
        assert_eq!(restored.commit_index(), commit);
        assert_eq!(
            restored.committed,
            vec![(2, b"durable-order".to_vec())],
            "a committed entry must be re-delivered to the matching state machine after restart"
        );
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn recovery_truncates_a_torn_final_record() {
        let path = std::env::temp_dir().join(format!(
            "tc-raft-torn-tail-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&STORAGE_HEADER).unwrap();
        file.write_all(&[ENTRY_RECORD]).unwrap();
        file.write_all(&100u32.to_le_bytes()).unwrap();
        file.write_all(b"partial").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let (_durable, entries, hard_state) = DurableRaftLog::open(&path).unwrap();
        assert!(entries.is_empty());
        assert_eq!(hard_state, HardState::default());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            STORAGE_HEADER.len() as u64
        );
        std::fs::remove_file(path).ok();
    }
}
