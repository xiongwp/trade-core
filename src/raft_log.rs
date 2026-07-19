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
//!
//! ## Snapshots and compaction
//!
//! The consensus log cannot grow without bound. When the application has
//! durably captured its state machine up to some applied index it calls
//! [`RaftNode::compact`], handing the module an opaque snapshot blob. The
//! module then (1) records that blob as the member's most recent snapshot so a
//! lagging follower can be caught up over the wire, (2) discards the in-memory
//! log prefix, and (3) atomically rewrites the durable log so only the snapshot
//! plus the surviving tail remain. Because the rewrite lands via a temp-file
//! rename, a crash either leaves the full pre-compaction log or the compacted
//! one — never a torn mixture. A follower that receives a `MsgSnapshot`
//! installs it in [`RaftNode::drive`] and exposes the blob through
//! [`RaftNode::take_installed_snapshot`] so the application can rebuild its
//! state machine.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use protobuf::Message as PbMessage;
use raft::eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Snapshot};
use raft::prelude::Message;
use raft::storage::MemStorage;
use raft::{Config, GetEntriesContext, RaftState, RawNode, StateRole, Storage};

/// The default commercial deployment topology: one elected leader and four
/// followers. Membership is now configurable (see [`ClusterConfig::new`]); this
/// constant only seeds defaults and tests.
pub const CLUSTER_SIZE: usize = 5;
/// Number of durable replicas required for a committed command in the default
/// five-node topology.
pub const QUORUM: usize = CLUSTER_SIZE / 2 + 1;
/// Hard ceiling on voter count. Raft quorum latency degrades past this and it
/// guards against fat-fingered configuration.
pub const MAX_CLUSTER_SIZE: usize = 9;

/// Membership for a trading cluster of three, five, seven or nine voters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: u64,
    /// Distinct, non-zero voter ids. Length is the replication factor.
    pub voters: Vec<u64>,
    /// Logical tick count before a follower starts an election.
    pub election_tick: usize,
    /// Logical tick count between leader heartbeats.
    pub heartbeat_tick: usize,
}

impl ClusterConfig {
    /// Build a config for `node_id` with an explicit voter set. The voter set
    /// may hold 1..=[`MAX_CLUSTER_SIZE`] distinct non-zero ids and must contain
    /// `node_id`. Odd sizes (3/5/7/9) are recommended for clean quorums but not
    /// enforced, so single-node development clusters remain possible.
    pub fn new(node_id: u64, voters: impl Into<Vec<u64>>) -> Result<Self, &'static str> {
        let voters = voters.into();
        if node_id == 0 || !voters.contains(&node_id) {
            return Err("node id must be a member of the cluster");
        }
        if voters.is_empty() || voters.len() > MAX_CLUSTER_SIZE {
            return Err("raft voters must number between 1 and MAX_CLUSTER_SIZE");
        }
        let mut sorted = voters.clone();
        sorted.sort_unstable();
        if sorted[0] == 0 || sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err("raft voters must be distinct non-zero ids");
        }
        Ok(Self {
            node_id,
            voters,
            election_tick: 10,
            heartbeat_tick: 2,
        })
    }

    /// Durable replicas required for a commit in this membership.
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

/// Why a client command was not proposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
    Raft,
}

/// A committed log record delivered to the local matching state machine,
/// carrying the fencing metadata needed to reject stale application.
///
/// `term` is the Raft term the entry was committed under and is guaranteed
/// monotonically non-decreasing across the stream from a single member, so an
/// application can refuse to apply a batch whose term regressed (a stale
/// leader). `leader_id` records who produced it (0 for entries recovered from
/// disk, where the producing leader is not retained). `route_version` is
/// reserved for the future split-matching topology; the consensus layer does
/// not own routing, so it is always 0 here and set by the application if used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    pub index: u64,
    pub term: u64,
    pub leader_id: u64,
    pub route_version: u64,
    pub data: Vec<u8>,
}

const STORAGE_HEADER: [u8; 8] = *b"TCRF\x01\0\0\0";
const HARD_STATE_RECORD: u8 = 1;
const ENTRY_RECORD: u8 = 2;
const SNAPSHOT_RECORD: u8 = 3;

/// Append-only durable state for one Raft member.
///
/// Records are checksummed and each [`RaftNode::drive`] synchronizes the batch
/// before its messages are exposed to the transport. A corrupt or torn tail is
/// rejected at startup: consensus must not guess at a vote or commit index.
///
/// A compaction rewrites the whole file atomically (temp file + rename) so the
/// snapshot record always precedes any tail entries and the prefix is only
/// dropped once the snapshot is durably in the replacement file.
struct DurableRaftLog {
    file: File,
    path: PathBuf,
}

/// The durable state recovered from disk at startup.
struct Recovered {
    entries: Vec<Entry>,
    hard_state: HardState,
    snapshot: Option<Snapshot>,
}

impl DurableRaftLog {
    fn open(path: &Path) -> io::Result<(Self, Recovered)> {
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
        let mut snapshot: Option<Snapshot> = None;
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
                SNAPSHOT_RECORD => {
                    let snap = Snapshot::parse_from_bytes(&payload).map_err(proto_error)?;
                    // A later snapshot supersedes an earlier one, and every log
                    // entry it covers is now redundant.
                    let index = snap.get_metadata().index;
                    entries.retain(|entry| entry.index > index);
                    snapshot = Some(snap);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown raft state record",
                    ))
                }
            }
        }
        // Entries below the snapshot boundary are compacted; drop any that a
        // pre-compaction record left behind before the snapshot record.
        if let Some(snap) = &snapshot {
            let index = snap.get_metadata().index;
            entries.retain(|entry| entry.index > index);
        }
        let durable_last = entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or_else(|| snapshot.as_ref().map_or(0, |s| s.get_metadata().index));
        if hard_state.commit > durable_last {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raft commit index is ahead of the durable log",
            ));
        }
        let file = OpenOptions::new().append(true).open(path)?;
        Ok((
            Self {
                file,
                path: path.to_path_buf(),
            },
            Recovered {
                entries,
                hard_state,
                snapshot,
            },
        ))
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

    /// Persist a freshly installed snapshot (follower side). The snapshot
    /// invalidates the entire prior log, so the file is rewritten to hold only
    /// the header, the snapshot and the committed hard state.
    fn install_snapshot(&mut self, snapshot: &Snapshot, hard_state: &HardState) -> io::Result<()> {
        self.rewrite(Some(snapshot), &[], hard_state)
    }

    /// Atomically rewrite the durable log after a leader-side compaction: the
    /// snapshot, then the surviving tail entries, then the hard state.
    fn compact(
        &mut self,
        snapshot: &Snapshot,
        tail: &[Entry],
        hard_state: &HardState,
    ) -> io::Result<()> {
        self.rewrite(Some(snapshot), tail, hard_state)
    }

    fn rewrite(
        &mut self,
        snapshot: Option<&Snapshot>,
        entries: &[Entry],
        hard_state: &HardState,
    ) -> io::Result<()> {
        let tmp = self.path.with_extension("compacting");
        {
            let mut writer = Writer {
                file: OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)?,
            };
            writer.file.write_all(&STORAGE_HEADER)?;
            if let Some(snapshot) = snapshot {
                writer.write_record(
                    SNAPSHOT_RECORD,
                    &snapshot.write_to_bytes().map_err(proto_error)?,
                )?;
            }
            for entry in entries {
                writer.write_record(ENTRY_RECORD, &entry.write_to_bytes().map_err(proto_error)?)?;
            }
            if *hard_state != HardState::default() {
                writer.write_record(
                    HARD_STATE_RECORD,
                    &hard_state.write_to_bytes().map_err(proto_error)?,
                )?;
            }
            writer.file.sync_all()?;
        }
        // Atomic on POSIX: readers see either the old full log or the compacted
        // one. The snapshot is durable in `tmp` before it can become "the" log.
        std::fs::rename(&tmp, &self.path)?;
        self.file = OpenOptions::new().append(true).open(&self.path)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn write_record(&mut self, kind: u8, payload: &[u8]) -> io::Result<()> {
        let mut writer = Writer {
            file: self.file.try_clone()?,
        };
        writer.write_record(kind, payload)
    }
}

/// A thin record writer shared by the append path and the compaction rewrite.
struct Writer {
    file: File,
}

impl Writer {
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

/// Storage backing one member: an in-memory Raft log plus the most recent
/// application snapshot.
///
/// `MemStorage` alone returns an *empty* snapshot payload (it stores only Raft
/// logs, not applied data), so a lagging follower could never be caught up over
/// the wire. This wrapper keeps the last application snapshot blob and serves
/// it from [`Storage::snapshot`], which is exactly where Raft reads when a
/// follower's next index has been compacted away on the leader.
#[derive(Clone)]
struct SnapStorage {
    mem: MemStorage,
    app: Arc<RwLock<Option<Snapshot>>>,
}

impl SnapStorage {
    fn new(conf_state: ConfState) -> Self {
        Self {
            mem: MemStorage::new_with_conf_state(conf_state),
            app: Arc::new(RwLock::new(None)),
        }
    }

    fn mem(&self) -> &MemStorage {
        &self.mem
    }

    fn set_app_snapshot(&self, snapshot: Snapshot) {
        *self.app.write().unwrap() = Some(snapshot);
    }
}

impl Storage for SnapStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.mem.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.mem.entries(low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.mem.term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.mem.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.mem.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        if let Some(snapshot) = self.app.read().unwrap().as_ref() {
            if snapshot.get_metadata().index >= request_index {
                return Ok(snapshot.clone());
            }
        }
        self.mem.snapshot(request_index, to)
    }
}

/// A transport-neutral Raft command-log node.
///
/// The storage is intentionally private. [`RaftNode::open`] persists every
/// `Ready` record before exposing `take_outbound()` messages; keeping matching
/// outside this type prevents consensus code from reaching into book state.
pub struct RaftNode {
    node: RawNode<SnapStorage>,
    store: SnapStorage,
    outbound: Vec<Message>,
    committed: Vec<Committed>,
    durable: Option<DurableRaftLog>,
    /// Application snapshot blob installed from a peer, awaiting the state
    /// machine's attention.
    installed_snapshot: Option<Snapshot>,
}

impl RaftNode {
    pub fn new(cluster: ClusterConfig) -> Result<Self, raft::Error> {
        Self::with_storage(
            cluster,
            None,
            Recovered {
                entries: Vec::new(),
                hard_state: HardState::default(),
                snapshot: None,
            },
        )
    }

    /// Open a real member from a durable state file. The file holds Raft's
    /// term, vote, commit index, the last application snapshot and all
    /// un-compacted entries — not just a copy of application commands.
    pub fn open(cluster: ClusterConfig, path: impl AsRef<Path>) -> io::Result<Self> {
        let (durable, recovered) = DurableRaftLog::open(path.as_ref())?;
        let snapshot_index = recovered
            .snapshot
            .as_ref()
            .map_or(0, |s| s.get_metadata().index);
        // Raft's durable commit index can be ahead of the local matching
        // state machine when a process dies after quorum commit but before it
        // reaches a shard. Re-expose that committed prefix on startup; the
        // application dispatcher is responsible for idempotent application.
        // Entries at or below the snapshot boundary are already reflected in the
        // snapshot blob, so they are not re-delivered as individual commands.
        let recovered_committed = recovered
            .entries
            .iter()
            .filter(|entry| {
                entry.index <= recovered.hard_state.commit
                    && entry.index > snapshot_index
                    && entry.get_entry_type() == EntryType::EntryNormal
                    && !entry.data.is_empty()
            })
            .map(|entry| Committed {
                index: entry.index,
                term: entry.term,
                leader_id: 0,
                route_version: 0,
                data: entry.data.to_vec(),
            })
            .collect();
        let installed = recovered.snapshot.clone();
        let mut node = Self::with_storage(cluster, Some(durable), recovered)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        node.committed = recovered_committed;
        node.installed_snapshot = installed;
        Ok(node)
    }

    fn with_storage(
        cluster: ClusterConfig,
        durable: Option<DurableRaftLog>,
        recovered: Recovered,
    ) -> Result<Self, raft::Error> {
        let store = SnapStorage::new(ConfState::from((cluster.voters.clone(), vec![])));
        if let Some(snapshot) = &recovered.snapshot {
            store.mem().wl().apply_snapshot(snapshot.clone())?;
            store.set_app_snapshot(snapshot.clone());
        }
        if !recovered.entries.is_empty() {
            store.mem().wl().append(&recovered.entries)?;
        }
        let snapshot_index = recovered
            .snapshot
            .as_ref()
            .map_or(0, |s| s.get_metadata().index);
        let applied = recovered.hard_state.commit.max(snapshot_index);
        if recovered.hard_state != HardState::default() {
            store.mem().wl().set_hardstate(recovered.hard_state.clone());
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
        let mut node = RawNode::with_default_logger(&cfg, store.clone())?;
        // Replay committed membership changes so a restarted member restores
        // the live voter set rather than reverting to its bootstrap topology.
        Self::restore_membership(&mut node, &store, &recovered, snapshot_index);
        Ok(Self {
            node,
            store,
            outbound: Vec::new(),
            committed: Vec::new(),
            durable,
            installed_snapshot: None,
        })
    }

    fn restore_membership(
        node: &mut RawNode<SnapStorage>,
        store: &SnapStorage,
        recovered: &Recovered,
        snapshot_index: u64,
    ) {
        let commit = recovered.hard_state.commit;
        for entry in &recovered.entries {
            if entry.index <= snapshot_index || entry.index > commit {
                continue;
            }
            if let Some(cc) = decode_conf_change(entry) {
                if let Ok(conf_state) = node.apply_conf_change(&cc) {
                    store.mem().wl().set_conf_state(conf_state);
                }
            }
        }
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
    pub fn applied_index(&self) -> u64 {
        self.node.raft.raft_log.applied
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.node.raft.raft_log.last_index()
    }

    /// The first index still present in the log; everything below it has been
    /// folded into a snapshot. Equal to snapshot index + 1.
    #[inline]
    pub fn first_log_index(&self) -> u64 {
        self.store.first_index().unwrap_or(1)
    }

    /// Index of the most recent snapshot this member can serve, or 0.
    pub fn snapshot_index(&self) -> u64 {
        self.store
            .app
            .read()
            .unwrap()
            .as_ref()
            .map_or(0, |s| s.get_metadata().index)
    }

    /// The current voter set, sorted.
    pub fn voters(&self) -> Vec<u64> {
        let mut voters: Vec<u64> = self.node.raft.prs().conf().voters().ids().iter().collect();
        voters.sort_unstable();
        voters
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

    /// Propose adding a voter to the cluster. Committed and applied through the
    /// normal log path, so it is quorum-durable before it takes effect.
    pub fn add_node(&mut self, node_id: u64) -> Result<(), ProposeError> {
        self.propose_conf_change(node_id, ConfChangeType::AddNode)
    }

    /// Propose removing a voter from the cluster.
    pub fn remove_node(&mut self, node_id: u64) -> Result<(), ProposeError> {
        self.propose_conf_change(node_id, ConfChangeType::RemoveNode)
    }

    fn propose_conf_change(
        &mut self,
        node_id: u64,
        change: ConfChangeType,
    ) -> Result<(), ProposeError> {
        if !self.is_leader() {
            return Err(ProposeError::NotLeader);
        }
        let mut cc = ConfChange::default();
        cc.set_change_type(change);
        cc.node_id = node_id;
        self.node
            .propose_conf_change(Vec::new(), cc)
            .map_err(|_| ProposeError::Raft)?;
        self.drive();
        Ok(())
    }

    /// Fold the log prefix up to `index` into a snapshot carrying the opaque
    /// application blob `app_data`, then discard that prefix from memory and
    /// rewrite the durable log. `index` is clamped to the applied index — the
    /// application must have durably captured its state machine at least that
    /// far before calling. A no-op if `index` is not beyond the current
    /// snapshot boundary.
    pub fn compact(&mut self, index: u64, app_data: Vec<u8>) -> io::Result<bool> {
        let index = index.min(self.applied_index());
        if index <= self.snapshot_index() || index == 0 {
            return Ok(false);
        }
        let term = self
            .store
            .term(index)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let conf_state = self
            .store
            .initial_state()
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?
            .conf_state;

        let mut snapshot = Snapshot::default();
        {
            let meta = snapshot.mut_metadata();
            meta.index = index;
            meta.term = term;
            meta.set_conf_state(conf_state);
        }
        snapshot.set_data(app_data.into());

        // Serve this blob to lagging followers, then drop the in-memory prefix.
        self.store.set_app_snapshot(snapshot.clone());
        self.store
            .mem()
            .wl()
            .compact(index)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

        if let Some(durable) = &mut self.durable {
            let last = self.node.raft.raft_log.last_index();
            let tail = if last > index {
                self.store
                    .entries(index + 1, last + 1, None, GetEntriesContext::empty(false))
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?
            } else {
                Vec::new()
            };
            let hard_state = self.store.mem().rl().hard_state().clone();
            durable.compact(&snapshot, &tail, &hard_state)?;
        }
        Ok(true)
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

    /// Commands that have reached the cluster commit index, in Raft index order,
    /// each tagged with the fencing metadata described on [`Committed`].
    pub fn take_committed(&mut self) -> Vec<Committed> {
        std::mem::take(&mut self.committed)
    }

    /// The application snapshot blob most recently installed from a peer (or
    /// recovered from disk at open), consumed once. The application rebuilds its
    /// state machine from this before resuming apply of the committed stream.
    pub fn take_installed_snapshot(&mut self) -> Option<Vec<u8>> {
        self.take_installed_snapshot_with_index()
            .map(|(_, data)| data)
    }

    pub fn take_installed_snapshot_with_index(&mut self) -> Option<(u64, Vec<u8>)> {
        self.installed_snapshot
            .take()
            .map(|snapshot| (snapshot.get_metadata().index, snapshot.get_data().to_vec()))
    }

    fn drive(&mut self) {
        while self.node.has_ready() {
            let mut ready = self.node.ready();
            let snapshot = if *ready.snapshot() != Snapshot::default() {
                Some(ready.snapshot().clone())
            } else {
                None
            };
            if let Some(durable) = &mut self.durable {
                durable
                    .persist(ready.entries(), ready.hs())
                    .expect("persist raft Ready before transport");
            }
            // A received snapshot supersedes the whole log: install it in the
            // store, remember its blob for the state machine, and rewrite the
            // durable log so recovery starts from the snapshot.
            if let Some(snapshot) = &snapshot {
                self.store
                    .mem()
                    .wl()
                    .apply_snapshot(snapshot.clone())
                    .expect("install received raft snapshot");
                self.store.set_app_snapshot(snapshot.clone());
                self.installed_snapshot = Some(snapshot.clone());
                if let Some(durable) = &mut self.durable {
                    let hard_state = self.store.mem().rl().hard_state().clone();
                    durable
                        .install_snapshot(snapshot, &hard_state)
                        .expect("persist installed raft snapshot");
                }
                // Anything queued below the snapshot boundary is now redundant.
                let index = snapshot.get_metadata().index;
                self.committed.retain(|entry| entry.index > index);
            }
            if !ready.entries().is_empty() {
                self.store
                    .mem()
                    .wl()
                    .append(ready.entries())
                    .expect("raft entries stay contiguous");
            }
            if let Some(hs) = ready.hs() {
                self.store.mem().wl().set_hardstate(hs.clone());
            }
            self.outbound.extend(ready.take_messages());
            self.apply_entries(ready.take_committed_entries());
            self.outbound.extend(ready.take_persisted_messages());

            let mut light = self.node.advance(ready);
            if let Some(commit) = light.commit_index() {
                self.store.mem().wl().mut_hard_state().set_commit(commit);
            }
            self.outbound.extend(light.take_messages());
            self.apply_entries(light.take_committed_entries());
            self.node.advance_apply();
        }
    }

    fn apply_entries(&mut self, entries: Vec<Entry>) {
        let leader_id = self.node.raft.leader_id;
        for entry in entries {
            match entry.get_entry_type() {
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    if let Some(cc) = decode_conf_change(&entry) {
                        let conf_state = self
                            .node
                            .apply_conf_change(&cc)
                            .expect("apply committed conf change");
                        self.store.mem().wl().set_conf_state(conf_state);
                    }
                }
                EntryType::EntryNormal => {
                    if !entry.data.is_empty() {
                        self.committed.push(Committed {
                            index: entry.index,
                            term: entry.term,
                            leader_id,
                            route_version: 0,
                            data: entry.data.to_vec(),
                        });
                    }
                }
            }
        }
    }
}

/// Decode a committed conf-change entry into an applicable single-step change.
fn decode_conf_change(entry: &Entry) -> Option<ConfChange> {
    if entry.data.is_empty() {
        return None;
    }
    match entry.get_entry_type() {
        EntryType::EntryConfChange => ConfChange::parse_from_bytes(&entry.data).ok(),
        // V2 is not proposed by this module, but tolerate a single-change V2 for
        // forward compatibility by only reading the change we understand.
        EntryType::EntryConfChangeV2 => None,
        EntryType::EntryNormal => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters() -> Vec<u64> {
        vec![1, 2, 3, 4, 5]
    }

    fn node(id: u64) -> RaftNode {
        RaftNode::new(ClusterConfig::new(id, voters()).unwrap()).unwrap()
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
        let mask = vec![true; nodes.len()];
        pump_active(nodes, &mask);
    }

    fn pump_active(nodes: &mut [RaftNode], active: &[bool]) {
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
                        // A real transport silently drops messages to/from a
                        // peer this member does not (yet / any longer) track,
                        // which happens transiently around membership changes.
                        match target.step(message) {
                            Ok(()) | Err(raft::Error::StepPeerNotFound) => {}
                            Err(error) => panic!("unexpected raft step error: {error:?}"),
                        }
                    }
                }
            }
        }
        panic!("raft message pump did not quiesce");
    }

    fn elect(nodes: &mut [RaftNode], leader: usize) {
        nodes[leader].campaign().unwrap();
        pump(nodes);
        assert!(nodes[leader].is_leader());
    }

    #[test]
    fn five_voters_commit_only_after_majority_replication() {
        let mut nodes = (1..=5).map(node).collect::<Vec<_>>();
        elect(&mut nodes, 0);

        nodes[0].propose(b"encoded-order".to_vec()).unwrap();
        assert!(nodes.iter_mut().all(|n| n.take_committed().is_empty()));
        pump(&mut nodes);
        for n in &mut nodes {
            let committed = n.take_committed();
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].index, 2);
            assert_eq!(committed[0].data, b"encoded-order".to_vec());
            assert!(committed[0].term >= 1);
        }
    }

    #[test]
    fn follower_cannot_accept_client_commands() {
        let mut follower = node(2);
        assert_eq!(follower.propose(vec![1]), Err(ProposeError::NotLeader));
    }

    #[test]
    fn variable_size_cluster_configs_are_validated() {
        assert!(ClusterConfig::new(1, vec![1, 2, 3]).is_ok());
        assert!(ClusterConfig::new(1, [1u64]).is_ok());
        assert!(ClusterConfig::new(6, vec![1, 2, 3, 4, 5]).is_err());
        assert!(ClusterConfig::new(1, vec![1, 1, 2]).is_err());
        assert!(ClusterConfig::new(1, Vec::<u64>::new()).is_err());
        assert_eq!(ClusterConfig::new(1, vec![1, 2, 3]).unwrap().quorum(), 2);
        assert_eq!(
            ClusterConfig::new(1, vec![1, 2, 3, 4, 5]).unwrap().quorum(),
            3
        );
    }

    #[test]
    fn three_node_cluster_commits_with_two_replicas() {
        let mut nodes = (1..=3).map(node_of_three).collect::<Vec<_>>();
        elect(&mut nodes, 0);
        nodes[0].propose(b"three-node".to_vec()).unwrap();
        pump(&mut nodes);
        for n in &mut nodes {
            let committed = n.take_committed();
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].data, b"three-node".to_vec());
        }
    }

    fn node_of_three(id: u64) -> RaftNode {
        RaftNode::new(ClusterConfig::new(id, vec![1, 2, 3]).unwrap()).unwrap()
    }

    #[test]
    fn committed_stream_carries_monotonic_fencing_terms() {
        let mut nodes = (1..=5).map(node).collect::<Vec<_>>();
        elect(&mut nodes, 0);
        let leader_term = nodes[0].term();
        nodes[0].propose(b"a".to_vec()).unwrap();
        nodes[0].propose(b"b".to_vec()).unwrap();
        pump(&mut nodes);
        let committed = nodes[0].take_committed();
        assert_eq!(committed.len(), 2);
        assert!(committed.windows(2).all(|w| w[1].term >= w[0].term));
        assert!(committed.iter().all(|c| c.term == leader_term));
        assert!(committed.iter().all(|c| c.leader_id == nodes[0].id()));
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
        elect(&mut nodes, 0);
        nodes[0].propose(b"before-failure".to_vec()).unwrap();
        pump(&mut nodes);
        for node in &mut nodes {
            let committed = node.take_committed();
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].data, b"before-failure".to_vec());
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
                assert!(
                    committed[0].index > 2,
                    "new leader may append a no-op first"
                );
                assert_eq!(committed[0].data, b"after-failure".to_vec());
            }
        }
    }

    #[test]
    fn conf_change_adds_and_removes_a_voter_and_shifts_quorum() {
        // Start as a three-node cluster, grow to include node 4, then drop 2.
        let mut nodes = vec![
            RaftNode::new(ClusterConfig::new(1, vec![1, 2, 3]).unwrap()).unwrap(),
            RaftNode::new(ClusterConfig::new(2, vec![1, 2, 3]).unwrap()).unwrap(),
            RaftNode::new(ClusterConfig::new(3, vec![1, 2, 3]).unwrap()).unwrap(),
            // Node 4 must be bootstrapped knowing the *current* membership so it
            // votes and replicates coherently once added.
            RaftNode::new(ClusterConfig::new(4, vec![1, 2, 3, 4]).unwrap()).unwrap(),
        ];
        elect(&mut nodes[..3], 0);

        nodes[0].add_node(4).unwrap();
        pump(&mut nodes);
        assert_eq!(nodes[0].voters(), vec![1, 2, 3, 4]);
        // The new voter learns the membership through replication.
        assert_eq!(nodes[3].voters(), vec![1, 2, 3, 4]);

        nodes[0].remove_node(2).unwrap();
        pump(&mut nodes);
        assert_eq!(nodes[0].voters(), vec![1, 3, 4]);

        // A proposal still commits on the new membership (quorum 2 of {1,3,4}).
        nodes[0].propose(b"post-membership".to_vec()).unwrap();
        pump(&mut nodes);
        for index in [0usize, 2, 3] {
            let committed = nodes[index].take_committed();
            assert!(committed.iter().any(|c| c.data == b"post-membership"));
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
        let config = ClusterConfig::new(1, voters()).unwrap();
        let mut leader = RaftNode::open(config.clone(), &path).unwrap();
        let mut followers = (2..=5).map(node).collect::<Vec<_>>();

        leader.campaign().unwrap();
        drive_open_leader(&mut leader, &mut followers);
        assert!(leader.is_leader());
        leader.propose(b"durable-order".to_vec()).unwrap();
        drive_open_leader(&mut leader, &mut followers);
        let term = leader.term();
        let commit = leader.commit_index();
        assert!(term > 0);
        assert!(commit >= 2);
        drop(leader);

        let mut restored = RaftNode::open(config, &path).unwrap();
        assert_eq!(restored.term(), term);
        assert_eq!(restored.commit_index(), commit);
        let committed = restored.take_committed();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].index, 2);
        assert_eq!(committed[0].data, b"durable-order".to_vec());
        std::fs::remove_file(path).ok();
    }

    fn drive_open_leader(leader: &mut RaftNode, followers: &mut [RaftNode]) {
        for _ in 0..40 {
            let messages = leader.take_outbound();
            let mut worked = !messages.is_empty();
            for message in messages {
                let target = followers.iter_mut().find(|n| n.id() == message.to).unwrap();
                target.step(message).unwrap();
            }
            for follower in &mut *followers {
                for message in follower.take_outbound() {
                    if message.to == leader.id() {
                        leader.step(message).unwrap();
                        worked = true;
                    }
                }
            }
            if !worked {
                return;
            }
        }
    }

    #[test]
    fn compaction_then_restart_recovers_from_snapshot_plus_tail() {
        let path = std::env::temp_dir().join(format!(
            "tc-raft-compact-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let config = ClusterConfig::new(1, voters()).unwrap();
        let mut leader = RaftNode::open(config.clone(), &path).unwrap();
        let mut followers = (2..=5).map(node).collect::<Vec<_>>();
        leader.campaign().unwrap();
        drive_open_leader(&mut leader, &mut followers);

        for i in 0..5u8 {
            leader.propose(vec![b'a' + i]).unwrap();
            drive_open_leader(&mut leader, &mut followers);
        }
        let all = leader.take_committed();
        assert_eq!(all.len(), 5);
        let compact_at = all[2].index;
        // Snapshot blob = the app-state reference the runtime would persist.
        let blob = b"engine-snapshot-ref".to_vec();
        assert!(leader.compact(compact_at, blob.clone()).unwrap());
        assert_eq!(leader.snapshot_index(), compact_at);
        // MemStorage retains the snapshot-boundary entry as its term anchor, so
        // the prefix strictly below `compact_at` is discarded.
        assert!(leader.first_log_index() >= compact_at);
        assert!(leader.first_log_index() > all[1].index);
        let commit = leader.commit_index();
        drop(leader);

        // Restart: the durable log now begins with the snapshot record; the
        // recovered member must restore commit and the surviving tail without
        // re-delivering the compacted prefix as commands.
        let mut restored = RaftNode::open(config, &path).unwrap();
        assert_eq!(restored.commit_index(), commit);
        assert_eq!(restored.snapshot_index(), compact_at);
        assert_eq!(restored.take_installed_snapshot(), Some(blob));
        let tail = restored.take_committed();
        assert!(tail.iter().all(|c| c.index > compact_at));
        assert_eq!(tail.len(), 2, "only entries above the snapshot re-deliver");
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn lagging_follower_catches_up_through_an_installed_snapshot() {
        let mut nodes = (1..=5).map(node).collect::<Vec<_>>();
        // Node 5 is partitioned off while the rest make progress and compact.
        let active = [true, true, true, true, false];
        nodes[0].campaign().unwrap();
        pump_active(&mut nodes, &active);
        assert!(nodes[0].is_leader());

        for i in 0..4u8 {
            nodes[0].propose(vec![b'x' + i]).unwrap();
            pump_active(&mut nodes, &active);
        }
        let committed = nodes[0].take_committed();
        let compact_at = committed.last().unwrap().index;
        let blob = b"catch-up-engine-state".to_vec();
        assert!(nodes[0].compact(compact_at, blob.clone()).unwrap());
        // Drain the other survivors so only node 5 lags.
        for n in &mut nodes[1..4] {
            n.take_committed();
        }

        // Reconnect node 5 (now all members active). Its next index is below
        // the compacted prefix, so the leader must ship a snapshot to catch up.
        for _ in 0..50 {
            for n in nodes.iter_mut() {
                n.tick();
            }
            pump(&mut nodes);
            if nodes[4].snapshot_index() >= compact_at {
                break;
            }
        }
        assert_eq!(
            nodes[4].take_installed_snapshot(),
            Some(blob),
            "the lagging follower installs the leader's snapshot blob"
        );
        assert!(nodes[4].commit_index() >= compact_at);
        assert!(nodes[4].snapshot_index() >= compact_at);
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

        let (_durable, recovered) = DurableRaftLog::open(&path).unwrap();
        assert!(recovered.entries.is_empty());
        assert_eq!(recovered.hard_state, HardState::default());
        assert!(recovered.snapshot.is_none());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            STORAGE_HEADER.len() as u64
        );
        std::fs::remove_file(path).ok();
    }
}
