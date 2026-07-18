//! Hot-standby replication: the journal's total order, streamed live.
//!
//! **DEPRECATED — single-machine development only.** This module predates the
//! Raft consensus path ([`crate::raft_log`]) and provides *no* quorum, leader
//! election or split-brain fencing: it manually streams one primary's journal
//! to passive standbys and relies on an external actor to promote one. Running
//! two "primaries" concurrently silently forks state. It is retained only
//! because existing tests depend on it and because it is a convenient
//! single-box mirror for development.
//!
//! **Production high availability MUST use the Raft path** (the
//! [`crate::raft_log`] module and the `raft-node` binary), which is the only
//! supported topology with durable quorum commit, automatic failover and
//! fencing. Do not deploy this module. `accept_on` and `run_replica` emit a
//! runtime warning to make the deprecation visible in logs.
//!
//! Every command a shard journals is simultaneously pushed (shard id + seq +
//! frame) to a replication fanout. A **standby** node consumes the stream and
//! applies each command through the same deterministic [`Processor`] — so its
//! books are byte-identical to the primary's at every seq (fingerprint-verified
//! by test). Failover = promote the standby: it already holds the state.
//!
//! Record: `[shard_id u32][seq u64][frame MSG_LEN]` = 52 bytes, little-endian.
//!
//! Honest scope: a standby must start **empty and attach from seq 1** (or be
//! bootstrapped by copying the primary's snapshot directory first — the
//! `seq <= snapshot.journal_seq` filter then skips replayed prefixes exactly as
//! in crash recovery). Automatic leader election / split-brain fencing is a
//! deployment concern (e.g. keepalived / k8s lease), not implemented here.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::exchange::Processor;
use crate::wire::{self, MSG_LEN};

/// One replicated command record.
pub const REC_LEN: usize = 4 + 8 + MSG_LEN;

/// Fans replication records out to every attached standby (write-only feed;
/// dead standbys are dropped on first failed write). Shards push through a
/// lock only when at least one standby is attached.
#[derive(Clone, Default)]
pub struct RepFanout {
    subs: Arc<Mutex<Vec<TcpStream>>>,
    attached: Arc<AtomicBool>,
}

impl RepFanout {
    /// Accept standby connections on `listener` in a background thread.
    pub fn accept_on(listener: TcpListener, running: Arc<AtomicBool>) -> RepFanout {
        eprintln!(
            "[replication] WARNING: hot-standby replication is DEPRECATED and \
             for single-machine development only (no quorum / no fencing). Use \
             the Raft path (raft-node) for production high availability."
        );
        let fanout = RepFanout::default();
        let subs = fanout.subs.clone();
        let attached = fanout.attached.clone();
        std::thread::Builder::new()
            .name("rep-accept".into())
            .spawn(move || {
                for stream in listener.incoming() {
                    if !running.load(Ordering::Acquire) {
                        break;
                    }
                    if let Ok(s) = stream {
                        s.set_nodelay(true).ok();
                        eprintln!("[replication] standby attached");
                        subs.lock().unwrap().push(s);
                        attached.store(true, Ordering::Release);
                    }
                }
            })
            .expect("spawn rep acceptor");
        fanout
    }

    /// Disconnect every standby (tests/controlled failover).
    pub fn close_all(&self) {
        let mut subs = self.subs.lock().unwrap();
        for s in subs.drain(..) {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
        self.attached.store(false, Ordering::Release);
    }

    /// Publish one command record. Cheap no-op while no standby is attached.
    pub fn publish(&self, shard_id: u32, seq: u64, frame: &[u8; MSG_LEN]) {
        if !self.attached.load(Ordering::Acquire) {
            return;
        }
        let mut rec = [0u8; REC_LEN];
        rec[0..4].copy_from_slice(&shard_id.to_le_bytes());
        rec[4..12].copy_from_slice(&seq.to_le_bytes());
        rec[12..].copy_from_slice(frame);
        let mut subs = self.subs.lock().unwrap();
        subs.retain_mut(|s| s.write_all(&rec).is_ok());
        if subs.is_empty() {
            self.attached.store(false, Ordering::Release);
        }
    }
}

/// A standby's state: one deterministic processor per primary shard.
pub struct Replica {
    pub processors: HashMap<u32, Processor>,
    /// Highest seq applied per shard (resume/bootstrap filter).
    pub applied_seq: HashMap<u32, u64>,
}

/// Run a standby: consume the replication stream from `primary_addr`, applying
/// every record (skipping `seq <= skip_seq` per shard, for snapshot-bootstrapped
/// standbys). Returns when the primary disconnects — at which point the returned
/// [`Replica`] holds the promoted state. `applied` counts records applied.
pub fn run_replica(
    primary_addr: &str,
    factory: crate::exchange::StrategyFactory,
    skip_seq: &HashMap<u32, u64>,
    applied: &AtomicU64,
) -> std::io::Result<Replica> {
    eprintln!(
        "[replication] WARNING: run_replica is DEPRECATED (development-only hot \
         standby, no quorum / no fencing). Production HA uses the Raft path."
    );
    let mut sock = TcpStream::connect(primary_addr)?;
    sock.set_nodelay(true).ok();
    let mut replica = Replica {
        processors: HashMap::new(),
        applied_seq: skip_seq.clone(),
    };

    let mut buf = vec![0u8; REC_LEN * 1024];
    let mut filled = 0usize;
    loop {
        match sock.read(&mut buf[filled..]) {
            Ok(0) | Err(_) => break, // primary gone: promote
            Ok(n) => {
                filled += n;
                let mut off = 0;
                while filled - off >= REC_LEN {
                    let shard_id = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                    let seq = u64::from_le_bytes(buf[off + 4..off + 12].try_into().unwrap());
                    let skip = replica.applied_seq.get(&shard_id).copied().unwrap_or(0);
                    if seq > skip {
                        if let Some(view) =
                            wire::WireView::parse(&buf[off + 12..off + 12 + MSG_LEN])
                        {
                            if let Some(cmd) = view.to_command() {
                                replica
                                    .processors
                                    .entry(shard_id)
                                    .or_insert_with(|| Processor::new(factory, None))
                                    .process(cmd, &mut |_| {});
                                applied.fetch_add(1, Ordering::Release);
                            }
                        }
                        replica.applied_seq.insert(shard_id, seq);
                    }
                    off += REC_LEN;
                }
                if off > 0 {
                    buf.copy_within(off..filled, 0);
                    filled -= off;
                }
            }
        }
    }
    Ok(replica)
}

/// Spawn [`run_replica`] on a thread (test/deploy convenience).
pub fn spawn_replica(
    primary_addr: String,
    factory: crate::exchange::StrategyFactory,
    applied: Arc<AtomicU64>,
) -> JoinHandle<std::io::Result<Replica>> {
    std::thread::Builder::new()
        .name("replica".into())
        .spawn(move || run_replica(&primary_addr, factory, &HashMap::new(), &applied))
        .expect("spawn replica")
}
