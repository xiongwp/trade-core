//! The exchange runtime: multi-asset, sharded, lock-free, journaled,
//! order-system-decoupled.
//!
//! # Architecture
//!
//! ```text
//!   order system (IO thread)                     matching side (share-nothing)
//!   ────────────────────────                     ─────────────────────────────
//!                                 lock-free SPSC
//!   OrderGateway.submit(New) ───▶ [normal queue] ─┐
//!   OrderGateway.submit(Cancel)─▶ [ high queue  ] ─┼─▶ Shard thread N (CPU-pinned)
//!   OrderGateway.submit(Modify)─▶ [ high queue  ] ─┤     1. establish durable order
//!   OrderGateway.submit(FClose)─▶ [ high queue  ] ─┘     2. price-guard vet
//!                                                        3. match in memory
//!   ResultSink.poll(...)     ◀─── [result queue] ◀────── 4. emit ExecReports (async)
//! ```
//!
//! * **Multi-asset**: instruments are hashed to shards; each shard owns the books
//!   for its instruments outright — the reason no locks are needed. Across
//!   machines, the same routing extends via [`crate::cluster::ClusterMap`].
//! * **Ordering & recovery**: standalone mode journals every command in exact
//!   processing order before it touches a book. Raft mode instead treats the
//!   quorum-replicated Raft WAL as the sole command log and restores an atomic
//!   memory snapshot plus the committed tail. Both paths use the same
//!   deterministic processor, so replay reproduces identical results.
//! * **Cancel/modify/force-close priority**: the high-priority queue is fully
//!   drained before, and between, new orders.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use std::time::Instant;

use crate::asset_log::{AppliedBatchProof, AssetJournalSet};
use crate::engine::{MatchingEngine, SelfTradePolicy};
use crate::journal::{self, JournalReader, JournalWriter};
use crate::lockfree::{self, Consumer, Producer};
use crate::order::Order;
use crate::risk::{InstrumentSpecs, PriceGuard, RiskLimits};
use crate::snapshot::{self, EngineState, Snapshot, StopOrder};
use crate::strategy::MatchingStrategy;
use crate::trade::{ModifyOutcome, OrderStatus};
use crate::types::*;
use crate::wire;

/// A command from the order system to the matching side.
#[derive(Debug)]
pub enum Command {
    /// Submit a new order (low-priority queue).
    New(Order),
    /// Submit a conditional (stop-loss / take-profit) order (low-priority
    /// queue). It is held outside the book until a trade prints at or through
    /// the trigger price, then enters as the given ordinary order.
    NewConditional(Order, Trigger),
    /// Cancel a resting order (high-priority queue). `cmd_id` is the
    /// order-system-assigned **unique increasing command id** (Leaf-style, the
    /// same series as new-order ids): cancels and modifies are first-class
    /// sequenced commands, so a crash replay reproduces them exactly and
    /// re-sent commands are attributable end-to-end.
    Cancel {
        instrument: InstrumentId,
        order_id: OrderId,
        cmd_id: u64,
    },
    /// Amend a resting order (high-priority queue); `cmd_id` as on `Cancel`.
    Modify {
        instrument: InstrumentId,
        order_id: OrderId,
        new_price: Price,
        new_qty: Qty,
        cmd_id: u64,
    },
    /// Forced liquidation (high-priority queue): cancel every resting order of
    /// `user` on `instrument`, then, if `close_qty > 0`, submit a protected
    /// market order to flatten the position.
    ForceClose {
        instrument: InstrumentId,
        user: u64,
        close_order_id: OrderId,
        close_side: Side,
        close_qty: Qty,
    },
    /// Admin: halt trading on an instrument (circuit breaker / 停牌). New
    /// orders are rejected while halted; cancels/modifies still work. Journaled
    /// like every command, so replay reproduces halt windows exactly — price
    /// triggers live in the (external) risk monitor, which SENDS this command.
    Halt {
        instrument: InstrumentId,
        cmd_id: u64,
    },
    /// Admin: resume trading on a halted instrument.
    Resume {
        instrument: InstrumentId,
        cmd_id: u64,
    },
    /// Admin: suspend a **user** (all their new orders rejected; cancels still
    /// work). exchange-core's user-suspend, command-driven and journaled.
    HaltUser {
        instrument: InstrumentId,
        user: u64,
        cmd_id: u64,
    },
    /// Admin: lift a user suspension.
    ResumeUser {
        instrument: InstrumentId,
        user: u64,
        cmd_id: u64,
    },
    /// A group of commands applied **atomically** (no other command from any
    /// queue interleaves): all must target this shard's instruments. Each inner
    /// command is journaled/replicated individually, preserving the total order.
    Batch(Vec<Command>),
}

/// Intake metadata carried alongside a command. Raft entries retain their
/// consensus index through the queue so a shard can durably advance its local
/// recovery watermark only after both WALs contain the command.
struct QueuedCommand {
    command: Command,
    raft_index: Option<u64>,
    /// Present only during Raft recovery for a batch whose Outbox/result proof
    /// is already durable. The shard rebuilds memory, verifies the exact result
    /// stream, and suppresses duplicate side effects.
    replay_proof: Option<AppliedBatchProof>,
}

impl Command {
    /// Globally unique id used for exact retry suppression and replay repair.
    pub fn id(&self) -> u64 {
        match self {
            Command::New(order) | Command::NewConditional(order, _) => order.id.0,
            Command::Cancel { cmd_id, .. }
            | Command::Modify { cmd_id, .. }
            | Command::Halt { cmd_id, .. }
            | Command::Resume { cmd_id, .. }
            | Command::HaltUser { cmd_id, .. }
            | Command::ResumeUser { cmd_id, .. } => *cmd_id,
            Command::ForceClose { close_order_id, .. } => close_order_id.0,
            Command::Batch(commands) => commands.first().map(Command::id).unwrap_or(0),
        }
    }

    /// The instrument this command targets (used for shard routing).
    pub fn instrument(&self) -> InstrumentId {
        match self {
            Command::New(o) | Command::NewConditional(o, _) => o.instrument,
            Command::Cancel { instrument, .. } => *instrument,
            Command::Modify { instrument, .. } => *instrument,
            Command::ForceClose { instrument, .. } => *instrument,
            Command::Halt { instrument, .. } => *instrument,
            Command::Resume { instrument, .. } => *instrument,
            Command::HaltUser { instrument, .. } => *instrument,
            Command::ResumeUser { instrument, .. } => *instrument,
            Command::Batch(cmds) => cmds.first().map_or(InstrumentId(0), |c| c.instrument()),
        }
    }

    /// Whether this command belongs on the high-priority queue.
    fn is_high_priority(&self) -> bool {
        !matches!(self, Command::New(_) | Command::NewConditional(..))
    }
}

/// An asynchronous execution notification from the matching side.
///
/// `user` / `taker_user` / `maker_user` name the account that owns the order
/// (0 = unattributed), so results can be pushed straight to that user. They
/// travel beside the fixed 56-byte report frame (outbox record, execution
/// event envelope), not inside it: the frame — and with it the report
/// fingerprints persisted in Raft application proofs — is unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecReport {
    Accepted {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    Trade {
        instrument: InstrumentId,
        taker: OrderId,
        maker: OrderId,
        aggressor: Side,
        price: Price,
        qty: Qty,
        /// Authoritative fees (ticks), computed in the matching path.
        maker_fee: u64,
        taker_fee: u64,
        taker_user: u64,
        maker_user: u64,
    },
    Filled {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    PartiallyFilled {
        instrument: InstrumentId,
        order_id: OrderId,
        filled: Qty,
        user: u64,
    },
    Resting {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    Cancelled {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    Rejected {
        instrument: InstrumentId,
        order_id: OrderId,
        reason: &'static str,
        user: u64,
    },
    /// A conditional order was accepted and is waiting for its trigger (it is
    /// not on the book yet).
    StopAccepted {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    /// A conditional order's trigger printed: it now enters as an ordinary
    /// order, and the usual Accepted / Trade / Filled ... reports follow.
    Triggered {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    /// A resting (maker) order was filled completely by the trade just
    /// reported, so its owner can be told without tracking quantities.
    /// Informational: it changes no state the projection does not already
    /// derive from the trade, and it is left out of Raft application proofs
    /// ([`proof_digest`]) so entries proved before it existed still verify.
    MakerFilled {
        instrument: InstrumentId,
        order_id: OrderId,
        user: u64,
    },
    /// A command was refused without changing any order: a re-sent duplicate,
    /// or an amend that breaks the instrument's trading rules. `command_id` is
    /// the refused command's own id — for a duplicate `New` that is also the
    /// original order's id, whose state must NOT change. Consumers projecting
    /// order state ignore this report; it exists so the sender learns why.
    CommandRejected {
        instrument: InstrumentId,
        command_id: u64,
        reason: &'static str,
    },
    Modified {
        instrument: InstrumentId,
        order_id: OrderId,
        remaining: Qty,
        user: u64,
    },
    NotFound {
        instrument: InstrumentId,
        order_id: OrderId,
    },
    /// One level of a depth-of-market snapshot (market-data feed). `level` is
    /// 0 = best. A snapshot is a run of `DepthLevel`s closed by `DepthEnd`.
    DepthLevel {
        instrument: InstrumentId,
        side: Side,
        level: u8,
        price: Price,
        qty: Qty,
    },
    /// Terminates a depth snapshot; carries how many levels each side sent.
    DepthEnd {
        instrument: InstrumentId,
        bid_levels: u8,
        ask_levels: u8,
    },
    /// Trading halted on the instrument (ack of [`Command::Halt`]).
    Halted {
        instrument: InstrumentId,
    },
    /// Trading resumed (ack of [`Command::Resume`]).
    Resumed {
        instrument: InstrumentId,
    },
    /// User suspended / unsuspended (acks of HaltUser/ResumeUser).
    UserHalted {
        instrument: InstrumentId,
        user: u64,
    },
    UserResumed {
        instrument: InstrumentId,
        user: u64,
    },
}

impl ExecReport {
    /// `[owner of the report's order, maker's owner for a trade]`; 0 where
    /// unknown (`NotFound`, command rejects, market-data and admin acks).
    pub fn users(&self) -> [u64; 2] {
        match *self {
            ExecReport::Trade {
                taker_user,
                maker_user,
                ..
            } => [taker_user, maker_user],
            ExecReport::Accepted { user, .. }
            | ExecReport::Filled { user, .. }
            | ExecReport::PartiallyFilled { user, .. }
            | ExecReport::Resting { user, .. }
            | ExecReport::Cancelled { user, .. }
            | ExecReport::Rejected { user, .. }
            | ExecReport::StopAccepted { user, .. }
            | ExecReport::Triggered { user, .. }
            | ExecReport::MakerFilled { user, .. }
            | ExecReport::Modified { user, .. } => [user, 0],
            _ => [0, 0],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionReportEvent {
    pub raft_index: Option<u64>,
    pub ordinal: u32,
    pub report: ExecReport,
}

/// A function that produces a fresh matching strategy for each new instrument.
pub type StrategyFactory = fn() -> Box<dyn MatchingStrategy>;

/// Configuration for [`build`].
#[derive(Clone)]
pub struct ExchangeConfig {
    /// Number of matching shards (threads). Instruments are hashed across them.
    pub shards: usize,
    /// Capacity of each intake and result queue (rounded up to a power of two).
    pub queue_capacity: usize,
    /// Strategy used for every instrument's book.
    pub strategy: StrategyFactory,
    /// Instruments whose books (and order pools) are created **at startup**.
    /// Unlisted instruments still get books on demand.
    pub instruments: Vec<InstrumentId>,
    /// Order-pool slots reserved per book. Total startup reservation =
    /// `instruments.len() * pool_orders_per_book * size_of::<Order>()`.
    pub pool_orders_per_book: usize,
    /// Pre-fault pool pages at startup (touch memory now, not on first order).
    pub prefault: bool,
    /// Directory for per-shard command journals (`None` = no journaling).
    pub journal_dir: Option<PathBuf>,
    /// Treat the replicated Raft WAL as the only command log. `journal_dir`
    /// then stores state snapshots and application proofs only; per-asset and
    /// per-shard command journals are not written on the matching hot path.
    pub raft_wal_authoritative: bool,
    /// User-space flush cadence for the journal (loss window, roughly).
    pub journal_flush: Duration,
    /// fsync cadence (OS buffers → disk). The full loss window is about
    /// `journal_flush + journal_fsync`.
    pub journal_fsync: Duration,
    /// Anti-spike price banding, shared with the external price feed.
    pub price_guard: Option<Arc<PriceGuard>>,
    /// Pin shard `i` to CPU core `i` (best effort; see [`crate::affinity`]).
    pub pin_cpus: bool,
    /// Periodic state snapshots (requires `journal_dir`): each shard writes
    /// `snapshot-shard-N.bin`. Standalone mode truncates its covered command
    /// journal; authoritative-Raft mode stores the covered Raft applied index.
    /// `None` = manual only (via [`ExchangeHandle::snapshot_now`]).
    pub snapshot_every: Option<Duration>,
    /// Self-trade prevention policy applied to every book.
    pub stp: SelfTradePolicy,
    /// Static pre-trade limits (max qty / notional / per-user open orders).
    pub risk_limits: Option<RiskLimits>,
    /// Per-instrument trading rules (tick / lot / min-max qty / price range /
    /// min notional). Empty = unrestricted, and the hot path skips the check.
    pub instrument_specs: InstrumentSpecs,
    /// Maker/taker fee schedule (default = zero fees).
    pub fees: crate::fees::FeeSchedule,
    /// Idempotent command dedup: reject any command whose id is <= the
    /// high-water mark (exactly-once under order-system re-send; cursor is
    /// snapshot-persisted). Requires globally increasing ids (Leaf).
    pub dedup_commands: bool,
    /// Exact command-id dedupe window, in ids (replicated state: every
    /// replica of a group must use the same value).
    pub command_id_window: usize,
    /// Command ids recovered from the consensus log at or below the engine
    /// snapshot, used only when that snapshot predates the persisted window.
    pub legacy_command_ids: Vec<u64>,
    /// Durable execution outbox directory. When enabled, every non-depth report
    /// produced from a Raft-committed command is appended before it leaves the
    /// matching shard.
    pub execution_outbox_dir: Option<PathBuf>,
    /// Raft group id used in deterministic execution event ids.
    pub raft_group_id: u32,
    /// How many outbox records may be grouped per durability sync.
    pub execution_outbox_sync_every: usize,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        ExchangeConfig {
            shards: 1,
            queue_capacity: 1 << 16,
            strategy: || Box::new(crate::strategy::PriceTimePriority),
            instruments: Vec::new(),
            pool_orders_per_book: 4096,
            prefault: false,
            journal_dir: None,
            raft_wal_authoritative: false,
            journal_flush: Duration::from_secs(1),
            journal_fsync: Duration::from_secs(1),
            price_guard: None,
            pin_cpus: false,
            snapshot_every: None,
            stp: SelfTradePolicy::Allow,
            risk_limits: None,
            instrument_specs: InstrumentSpecs::default(),
            fees: crate::fees::FeeSchedule::default(),
            dedup_commands: false,
            command_id_window: DEFAULT_COMMAND_ID_WINDOW,
            legacy_command_ids: Vec::new(),
            execution_outbox_dir: None,
            raft_group_id: 0,
            execution_outbox_sync_every: 1,
        }
    }
}

/// How many new orders a shard processes before it loops back to re-drain the
/// high-priority queue.
const NORMAL_BATCH: usize = 32;

// ---------------------------------------------------------------------------
// Order-system-facing handles
// ---------------------------------------------------------------------------

struct ShardTx {
    high: Producer<QueuedCommand>,
    normal: Producer<QueuedCommand>,
}

/// The two priority lanes dedicated to one configured instrument. A shard
/// owns many of these mailboxes, but no mailbox is shared by two instruments.
struct InstrumentRx {
    high: Consumer<QueuedCommand>,
    normal: Consumer<QueuedCommand>,
}

/// The order system's handle for sending commands into the matching side.
/// Single-producer: drive it from one gateway/IO thread.
pub struct OrderGateway {
    shards: Vec<ShardTx>,
    /// Pre-configured assets get an independent pair of SPSC queues. The
    /// fallback shard queues retain backwards compatibility for dynamically
    /// created books in library users; production servers configure every
    /// locally owned asset at startup.
    instruments: HashMap<InstrumentId, ShardTx>,
}

impl OrderGateway {
    #[inline]
    fn shard_of(&self, instrument: InstrumentId) -> usize {
        instrument.0 as usize % self.shards.len()
    }

    /// Route a command to its shard. Cancels, modifies and force-closes go to
    /// the high-priority queue; new orders to the normal queue. Returns
    /// `Err(cmd)` if the target queue is full (backpressure).
    pub fn submit(&self, cmd: Command) -> Result<(), Command> {
        self.submit_with_raft_index(cmd, None, None)
    }

    /// Submit a command that has already crossed the Raft quorum. Its index is
    /// persisted with the local application watermark before matching.
    pub fn submit_committed(&self, raft_index: u64, cmd: Command) -> Result<(), Command> {
        self.submit_with_raft_index(cmd, Some(raft_index), None)
    }

    /// Rebuild in-memory state for an entry whose exact result stream is
    /// already durable. No report is appended or emitted; the supplied proof
    /// must match deterministic replay exactly or the shard fails closed.
    pub fn submit_recovered(&self, proof: AppliedBatchProof, cmd: Command) -> Result<(), Command> {
        self.submit_with_raft_index(cmd, Some(proof.raft_index), Some(proof))
    }

    fn submit_with_raft_index(
        &self,
        cmd: Command,
        raft_index: Option<u64>,
        replay_proof: Option<AppliedBatchProof>,
    ) -> Result<(), Command> {
        let queued = QueuedCommand {
            command: cmd,
            raft_index,
            replay_proof,
        };
        let instrument = queued.command.instrument();
        if let Some(tx) = self.instruments.get(&instrument) {
            return if queued.command.is_high_priority() {
                tx.high.push(queued).map_err(|queued| queued.command)
            } else {
                tx.normal.push(queued).map_err(|queued| queued.command)
            };
        }
        let idx = self.shard_of(instrument);
        let tx = &self.shards[idx];
        if queued.command.is_high_priority() {
            tx.high.push(queued).map_err(|queued| queued.command)
        } else {
            tx.normal.push(queued).map_err(|queued| queued.command)
        }
    }

    /// Convenience: submit a new order.
    pub fn new_order(&self, order: Order) -> Result<(), Command> {
        self.submit(Command::New(order))
    }

    /// Convenience: cancel a resting order.
    pub fn cancel(
        &self,
        instrument: InstrumentId,
        order_id: OrderId,
        cmd_id: u64,
    ) -> Result<(), Command> {
        self.submit(Command::Cancel {
            instrument,
            order_id,
            cmd_id,
        })
    }

    /// Convenience: amend a resting order.
    pub fn modify(
        &self,
        instrument: InstrumentId,
        order_id: OrderId,
        new_price: Price,
        new_qty: Qty,
        cmd_id: u64,
    ) -> Result<(), Command> {
        self.submit(Command::Modify {
            instrument,
            order_id,
            new_price,
            new_qty,
            cmd_id,
        })
    }

    /// Convenience: halt trading on an instrument (admin/risk monitor).
    pub fn halt(&self, instrument: InstrumentId, cmd_id: u64) -> Result<(), Command> {
        self.submit(Command::Halt { instrument, cmd_id })
    }

    /// Convenience: resume a halted instrument.
    pub fn resume(&self, instrument: InstrumentId, cmd_id: u64) -> Result<(), Command> {
        self.submit(Command::Resume { instrument, cmd_id })
    }

    /// Convenience: force-close a user on an instrument.
    pub fn force_close(
        &self,
        instrument: InstrumentId,
        user: u64,
        close_order_id: OrderId,
        close_side: Side,
        close_qty: Qty,
    ) -> Result<(), Command> {
        self.submit(Command::ForceClose {
            instrument,
            user,
            close_order_id,
            close_side,
            close_qty,
        })
    }
}

/// The order system's handle for receiving asynchronous execution reports.
pub struct ResultSink {
    results: Vec<Consumer<ExecutionReportEvent>>,
}

impl ResultSink {
    /// Drain every currently-available report across all shards, invoking `f`
    /// for each. Non-blocking; returns the number of reports delivered.
    pub fn poll(&self, mut f: impl FnMut(ExecReport)) -> usize {
        self.poll_events(|event| f(event.report))
    }

    /// Drain report events with their deterministic Raft metadata.
    pub fn poll_events(&self, mut f: impl FnMut(ExecutionReportEvent)) -> usize {
        let mut count = 0;
        for rx in &self.results {
            while let Some(event) = rx.pop() {
                f(event);
                count += 1;
            }
        }
        count
    }
}

/// Controls the lifecycle of the running matching shards.
pub struct ExchangeHandle {
    /// Operational counters (share with a metrics endpoint).
    pub metrics: Arc<crate::metrics::Metrics>,
    running: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    parked: Arc<AtomicUsize>,
    snap_requests: Vec<Arc<AtomicBool>>,
    num_shards: usize,
    threads: Vec<JoinHandle<()>>,
}

impl ExchangeHandle {
    /// Release shards (built paused, or previously [`pause`](Self::pause)d).
    pub fn resume(&self) {
        self.started.store(true, Ordering::Release);
    }

    /// Alias for [`resume`](Self::resume).
    pub fn start(&self) {
        self.resume();
    }

    /// Ask every shard to take a snapshot at its next opportunity. Standalone
    /// shards also truncate their covered journal. No-op without snapshot
    /// configuration.
    pub fn snapshot_now(&self) {
        for r in &self.snap_requests {
            r.store(true, Ordering::Release);
        }
    }

    /// Pause matching and block until every shard has quiesced.
    pub fn pause(&self) {
        self.started.store(false, Ordering::Release);
        while self.parked.load(Ordering::Acquire) < self.num_shards {
            thread::yield_now();
        }
    }

    /// Signal all shards to drain and stop, then join their threads.
    pub fn shutdown(self) {
        self.started.store(true, Ordering::Release);
        self.running.store(false, Ordering::Release);
        for t in self.threads {
            let _ = t.join();
        }
    }
}

/// Build and start an exchange.
pub fn build(config: ExchangeConfig) -> (OrderGateway, ResultSink, ExchangeHandle) {
    build_inner(config, true, None)
}

/// Like [`build`] with a live replication fanout: every journaled command is
/// simultaneously streamed to attached standbys (see [`crate::replication`]).
pub fn build_with_rep(
    config: ExchangeConfig,
    rep: crate::replication::RepFanout,
) -> (OrderGateway, ResultSink, ExchangeHandle) {
    build_inner(config, true, Some(rep))
}

/// Like [`build`], but shards start **paused** (accepting commands into queues
/// without processing) until [`ExchangeHandle::start`].
pub fn build_paused(config: ExchangeConfig) -> (OrderGateway, ResultSink, ExchangeHandle) {
    build_inner(config, false, None)
}

fn build_inner(
    config: ExchangeConfig,
    start_now: bool,
    rep: Option<crate::replication::RepFanout>,
) -> (OrderGateway, ResultSink, ExchangeHandle) {
    assert!(config.shards >= 1, "need at least one shard");
    assert!(
        !config.raft_wal_authoritative || config.shards == 1,
        "Raft authoritative matching currently requires exactly one state-machine shard per group"
    );
    let metrics = Arc::new(crate::metrics::Metrics::default());
    let running = Arc::new(AtomicBool::new(true));
    let started = Arc::new(AtomicBool::new(start_now));
    let parked = Arc::new(AtomicUsize::new(0));
    let mut shard_tx = Vec::with_capacity(config.shards);
    let mut instrument_tx = HashMap::with_capacity(config.instruments.len());
    let mut result_rx = Vec::with_capacity(config.shards);
    let mut threads = Vec::with_capacity(config.shards);
    let mut snap_requests = Vec::with_capacity(config.shards);

    for shard_id in 0..config.shards {
        let (high_tx, high_rx) = lockfree::channel::<QueuedCommand>(config.queue_capacity);
        let (normal_tx, normal_rx) = lockfree::channel::<QueuedCommand>(config.queue_capacity);
        let (res_tx, res_rx) = lockfree::channel::<ExecutionReportEvent>(config.queue_capacity);

        shard_tx.push(ShardTx {
            high: high_tx,
            normal: normal_tx,
        });
        result_rx.push(res_rx);

        // Each configured asset has its own two-lane intake mailbox. The
        // number of matching threads stays bounded by `shards`; a worker
        // multiplexes the mailboxes it owns, preserving isolation without one
        // thread per asset.
        let mut instrument_rx = Vec::new();
        for &inst in &config.instruments {
            if inst.0 as usize % config.shards == shard_id {
                let (inst_high_tx, inst_high_rx) =
                    lockfree::channel::<QueuedCommand>(config.queue_capacity);
                let (inst_normal_tx, inst_normal_rx) =
                    lockfree::channel::<QueuedCommand>(config.queue_capacity);
                instrument_tx.insert(
                    inst,
                    ShardTx {
                        high: inst_high_tx,
                        normal: inst_normal_tx,
                    },
                );
                instrument_rx.push(InstrumentRx {
                    high: inst_high_rx,
                    normal: inst_normal_rx,
                });
            }
        }

        // Pre-create books (and their memory pools) for this shard's share of
        // the configured instrument list — the startup memory reservation.
        let mut processor = Processor::new(config.strategy, config.price_guard.clone())
            .with_stp(config.stp)
            .with_limits(config.risk_limits)
            .with_specs(config.instrument_specs.clone())
            .with_dedup(config.dedup_commands)
            .with_command_id_window(config.command_id_window)
            .with_legacy_command_ids(config.legacy_command_ids.clone())
            .with_fees(config.fees);
        for &inst in &config.instruments {
            if inst.0 as usize % config.shards == shard_id {
                processor.create_book(inst, config.pool_orders_per_book, config.prefault);
            }
        }

        // Persistence for this shard. Standalone mode opens its command/asset
        // journals; authoritative-Raft mode opens only snapshot, application
        // proof and result-outbox state.
        let mut journal_w = None;
        let mut asset_journal = None;
        let mut execution_outbox = None;
        let mut snapshot_path = None;
        let mut applied_root = None;
        let mut initial_raft_index = 0u64;
        if let Some(dir) = &config.journal_dir {
            std::fs::create_dir_all(dir).expect("create journal dir");
            let jpath = dir.join(format!("journal-shard-{shard_id}.bin"));
            let spath = dir.join(format!("snapshot-shard-{shard_id}.bin"));
            let proof_root = dir.join("assets");
            std::fs::create_dir_all(&proof_root).expect("create Raft application proof dir");
            if config.raft_wal_authoritative {
                initial_raft_index =
                    recover_raft_authoritative(&mut processor, &spath, &jpath, &proof_root)
                        .expect("recover matching snapshot for Raft WAL replay");
                applied_root = Some(proof_root);
                snapshot_path = Some(spath);
            } else {
                let (_, last_seq) =
                    recover_stats(&mut processor, &spath, &jpath).expect("recover shard state");
                let mut w =
                    JournalWriter::open(&jpath, config.journal_flush).expect("open journal");
                // The journal seq IS the total order: continue it, never restart it.
                w.resume_from(last_seq);
                if let Ok(fh) = w.file_handle() {
                    journal::spawn_fsyncer(
                        fh,
                        config.journal_fsync,
                        running.clone(),
                        metrics.clone(),
                    );
                }
                journal_w = Some(w);
                asset_journal = Some(
                    AssetJournalSet::open(proof_root, config.journal_flush)
                        .expect("open per-asset journals"),
                );
                snapshot_path = Some(spath);
            }
        }
        if let Some(dir) = &config.execution_outbox_dir {
            let outbox_path = dir.join(format!("outbox-shard-{shard_id}.bin"));
            // Recovery barrier for the execution outbox. A batch's reports are
            // fsynced before its Raft application watermark is persisted, and
            // recovery re-applies (and re-appends) any batch past that
            // watermark. Trim every record beyond the last durably applied
            // batch so replay is exactly-once here: nothing lost, nothing
            // duplicated. Records are ordered by ascending raft_index and
            // segments rotate only after a watermark advance, so only the
            // newest segment can hold unwatermarked records.
            let max_applied = config.journal_dir.as_ref().and_then(|journal_dir| {
                crate::asset_log::load_applied_batches(&journal_dir.join("assets"))
                    .ok()
                    .and_then(|batches| batches.into_iter().max())
            });
            let newest =
                crate::execution_outbox::latest_segment(&outbox_path).unwrap_or_else(|error| {
                    panic!("scan execution outbox segments {}: {error}", dir.display())
                });
            crate::execution_outbox::truncate_after_applied(&newest, max_applied).unwrap_or_else(
                |error| {
                    panic!(
                        "trim execution outbox {} to last durable Raft batch: {error}",
                        newest.display()
                    )
                },
            );
            // Bound outbox disk growth: rotate to a new segment once the
            // current one reaches this size; fully published segments are then
            // garbage collected. 0 disables rotation.
            let rotate_bytes = std::env::var("TC_EXECUTION_OUTBOX_ROTATE_BYTES")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(128 * 1024 * 1024);
            execution_outbox = Some(
                crate::execution_outbox::ExecutionOutboxWriter::open(
                    &outbox_path,
                    config.journal_flush,
                    config.execution_outbox_sync_every,
                )
                .unwrap_or_else(|error| {
                    panic!("open execution outbox {}: {error}", outbox_path.display())
                })
                .with_rotate_bytes(Some(rotate_bytes)),
            );
        }

        let snap_request = Arc::new(AtomicBool::new(false));
        snap_requests.push(snap_request.clone());

        let mut shard = Shard {
            shard_id: shard_id as u32,
            rep: rep.clone(),
            rep_seq: 0,
            metrics: metrics.clone(),
            processor,
            high_rx,
            normal_rx,
            instrument_rx,
            next_instrument: 0,
            result_tx: res_tx,
            journal: journal_w,
            asset_journal,
            execution_outbox,
            applied_root,
            last_raft_index: initial_raft_index,
            raft_wal_authoritative: config.raft_wal_authoritative,
            raft_group_id: config.raft_group_id,
            snapshot_path,
            snapshot_every: config.snapshot_every,
            last_snapshot: Instant::now(),
            snapshot_writer: None,
            snap_request,
            running: running.clone(),
            started: started.clone(),
            parked: parked.clone(),
            default_pool: (config.pool_orders_per_book, config.prefault),
            pin_core: config.pin_cpus.then_some(shard_id),
            last_depth: Instant::now(),
            dirty: HashSet::new(),
            depth_buf: Vec::new(),
            idle_streak: 0,
        };
        threads.push(
            thread::Builder::new()
                .name(format!("match-shard-{shard_id}"))
                .spawn(move || shard.run())
                .expect("spawn shard"),
        );
        metrics.set_raft_enqueued_index(initial_raft_index);
        metrics.set_raft_applied_index(initial_raft_index);
    }

    (
        OrderGateway {
            shards: shard_tx,
            instruments: instrument_tx,
        },
        ResultSink { results: result_rx },
        ExchangeHandle {
            metrics,
            running,
            started,
            parked,
            snap_requests,
            num_shards: config.shards,
            threads,
        },
    )
}

// ---------------------------------------------------------------------------
// The command processor (shared by live shards and journal replay)
// ---------------------------------------------------------------------------

/// Applies commands to per-instrument engines and emits execution reports.
///
/// This is the deterministic core: given the same command sequence it produces
/// the same report sequence, which is what makes journal replay exact.
pub struct Processor {
    engines: HashMap<InstrumentId, MatchingEngine>,
    factory: StrategyFactory,
    guard: Option<Arc<PriceGuard>>,
    default_pool: (usize, bool),
    stp: SelfTradePolicy,
    limits: Option<RiskLimits>,
    specs: InstrumentSpecs,
    /// Idempotency: highest command id applied; duplicates are rejected.
    /// 0-ids bypass (legacy/tests). Persisted in snapshots (exactly-once
    /// across crash + order-system re-send).
    dedup: bool,
    /// Exact retry protection also closes the crash window between replaying
    /// the shard journal and advancing the separate Raft application marker.
    seen_command_ids: RecentCommandIds,
    /// Ids to seed the window with when restoring a pre-v6 snapshot, which
    /// has no window (see `ExchangeConfig::legacy_command_ids`).
    legacy_command_ids: Vec<u64>,
    /// Dual dedup cursors — one per intake queue. The high-priority queue
    /// legitimately reorders admin commands ahead of News, so a single cursor
    /// would be poisoned by a jumping cancel and dup-reject every later New.
    /// Each queue is FIFO, so per-stream monotonic ids hold.
    max_new_id: u64,
    max_admin_id: u64,
    fees: crate::fees::FeeSchedule,
    /// Halted instruments (circuit breaker); command-driven, snapshot-persisted.
    halted: std::collections::HashSet<InstrumentId>,
    /// Suspended users (new orders rejected).
    suspended: std::collections::HashSet<u64>,
    /// Net positions per (user, instrument), built from the trade stream —
    /// feeds margin monitoring/forced liquidation. Deterministic under replay.
    positions: HashMap<(u64, InstrumentId), i64>,
    /// Reusable trade buffer: the New-order hot path allocates nothing.
    trades_buf: Vec<crate::trade::Trade>,
    /// Untriggered conditional orders per instrument.
    stops: HashMap<InstrumentId, TriggerBook>,
    /// Total untriggered conditional orders: when 0 (the common case) the
    /// trigger check after a trade costs one comparison.
    pending_stops: usize,
    /// Arrival counter ordering stops that share a trigger price. Only its
    /// relative order matters, so it is not persisted (restore renumbers).
    stop_seq: u64,
}

/// Exact command-id dedupe over the most recent `cap` distinct ids.
///
/// Part of the replicated state machine: ids enter in log order and the
/// oldest is evicted first, and the whole window is written into every
/// snapshot (format v6). So a replica that restarted, or installed a
/// snapshot from the leader, makes exactly the same accept/reject decision
/// as one that never stopped — the previous unbounded set was rebuilt from
/// resting orders only, so a late Kafka redelivery of a filled order was
/// rejected on some replicas and matched again on others. Bounded: memory
/// is `cap` ids no matter how long the node runs. All replicas of a group
/// must use the same `cap` (`TC_DEDUP_WINDOW_IDS`).
#[derive(Clone)]
pub(crate) struct RecentCommandIds {
    set: std::collections::HashSet<u64>,
    order: std::collections::VecDeque<u64>,
    cap: usize,
}

/// Default exact-dedupe window: 2M ids (~50 MB per group).
pub const DEFAULT_COMMAND_ID_WINDOW: usize = 2_000_000;

impl RecentCommandIds {
    fn new(cap: usize) -> Self {
        Self {
            set: std::collections::HashSet::new(),
            order: std::collections::VecDeque::new(),
            cap: cap.max(1),
        }
    }

    /// Record `id`; false when it is already in the window (a duplicate).
    fn insert(&mut self, id: u64) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        self.order.push_back(id);
        while self.order.len() > self.cap {
            if let Some(expired) = self.order.pop_front() {
                self.set.remove(&expired);
            }
        }
        true
    }

    fn clear(&mut self) {
        self.set.clear();
        self.order.clear();
    }

    /// Oldest first: inserting them in this order rebuilds the same window.
    fn to_vec(&self) -> Vec<u64> {
        self.order.iter().copied().collect()
    }
}

/// Untriggered conditional orders of one instrument, in firing priority:
/// rising triggers lowest-first, falling triggers highest-first, ties by
/// arrival. Determinism: the order in which a single trade print fires
/// several stops is a pure function of this state.
#[derive(Default)]
struct TriggerBook {
    /// Fire when a trade prints at or above the price.
    rises: std::collections::BTreeMap<(Price, u64), StopOrder>,
    /// Fire when a trade prints at or below the price.
    falls: std::collections::BTreeMap<(Price, u64), StopOrder>,
    /// order id -> (rises?, key) for cancel lookups.
    index: HashMap<OrderId, (bool, (Price, u64))>,
}

impl TriggerBook {
    fn insert(&mut self, stop: StopOrder, seq: u64) {
        let rises = stop.trigger.fires_on_rise(stop.order.side);
        let key = (stop.trigger.price, seq);
        self.index.insert(stop.order.id, (rises, key));
        if rises {
            self.rises.insert(key, stop);
        } else {
            self.falls.insert(key, stop);
        }
    }

    fn remove(&mut self, id: OrderId) -> Option<StopOrder> {
        let (rises, key) = self.index.remove(&id)?;
        if rises {
            self.rises.remove(&key)
        } else {
            self.falls.remove(&key)
        }
    }

    fn contains(&self, id: OrderId) -> bool {
        self.index.contains_key(&id)
    }

    /// Remove and return the next stop armed by a trade at `price`: rising
    /// triggers first (lowest price, earliest arrival), then falling ones
    /// (highest price, earliest arrival).
    fn pop_triggered(&mut self, price: Price) -> Option<StopOrder> {
        let key = match self.rises.first_key_value() {
            Some((&key, _)) if key.0 <= price => Some((true, key)),
            _ => self
                .falls
                .iter()
                .rev()
                .find(|(key, _)| key.0 >= price)
                .and_then(|(_, stop)| {
                    // Highest price first; among equal prices, earliest arrival.
                    self.falls
                        .range((stop.trigger.price, 0)..=(stop.trigger.price, u64::MAX))
                        .next()
                        .map(|(&key, _)| (false, key))
                }),
        }?;
        let (rises, key) = key;
        let stop = if rises {
            self.rises.remove(&key)
        } else {
            self.falls.remove(&key)
        }?;
        self.index.remove(&stop.order.id);
        Some(stop)
    }

    fn remove_user(&mut self, user: u64) -> Vec<OrderId> {
        let mut ids: Vec<(Price, u64, OrderId)> = self
            .rises
            .iter()
            .chain(self.falls.iter())
            .filter(|(_, stop)| stop.order.user == user)
            .map(|(&(price, seq), stop)| (price, seq, stop.order.id))
            .collect();
        ids.sort_by_key(|&(_, seq, _)| seq); // arrival order: deterministic
        ids.into_iter()
            .filter_map(|(_, _, id)| self.remove(id).map(|stop| stop.order.id))
            .collect()
    }

    /// Canonical export order: rising book, then falling book, each in key
    /// order (so restore can renumber arrivals and keep tie order).
    fn export(&self) -> Vec<StopOrder> {
        self.rises.values().chain(self.falls.values()).copied().collect()
    }

    fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

impl Processor {
    pub fn new(factory: StrategyFactory, guard: Option<Arc<PriceGuard>>) -> Self {
        Processor {
            engines: HashMap::new(),
            factory,
            guard,
            default_pool: (4096, false),
            stp: SelfTradePolicy::Allow,
            limits: None,
            specs: InstrumentSpecs::default(),
            dedup: false,
            seen_command_ids: RecentCommandIds::new(DEFAULT_COMMAND_ID_WINDOW),
            legacy_command_ids: Vec::new(),
            max_new_id: 0,
            max_admin_id: 0,
            fees: crate::fees::FeeSchedule::default(),
            halted: std::collections::HashSet::new(),
            suspended: std::collections::HashSet::new(),
            positions: HashMap::new(),
            trades_buf: Vec::new(),
            stops: HashMap::new(),
            pending_stops: 0,
            stop_seq: 0,
        }
    }

    /// Set the self-trade prevention policy for every book (builder style).
    pub fn with_stp(mut self, stp: SelfTradePolicy) -> Self {
        self.stp = stp;
        self
    }

    /// Set static pre-trade limits (builder style).
    pub fn with_limits(mut self, limits: Option<RiskLimits>) -> Self {
        self.limits = limits;
        self
    }

    /// Set per-instrument trading rules (builder style).
    pub fn with_specs(mut self, specs: InstrumentSpecs) -> Self {
        self.specs = specs;
        self
    }

    /// A new order whose id is still resting (or waiting as a stop) is a
    /// retry even after its id has aged out of the bounded window; letting
    /// it through would insert the id twice (the book asserts on that, on
    /// every replica alike).
    fn would_duplicate_live(&self, cmd: &Command) -> bool {
        let (Command::New(order) | Command::NewConditional(order, _)) = cmd else {
            return false;
        };
        self.engines
            .get(&order.instrument)
            .is_some_and(|engine| engine.book().get(order.id).is_some())
            || (self.pending_stops > 0
                && self
                    .stops
                    .get(&order.instrument)
                    .is_some_and(|book| book.contains(order.id)))
    }

    /// Capture and atomically write this processor's snapshot, exactly as a
    /// matching shard does (format v6, including the dedupe window).
    pub fn write_snapshot(
        &self,
        path: &Path,
        journal_seq: u64,
        raft_applied_index: u64,
    ) -> std::io::Result<()> {
        persist_processor_snapshot(self, path, journal_seq, raft_applied_index)
    }

    /// Size of the exact command-id dedupe window (builder style).
    pub fn with_command_id_window(mut self, cap: usize) -> Self {
        self.seen_command_ids = RecentCommandIds::new(cap);
        self
    }

    /// Ids that seed the dedupe window when a pre-v6 snapshot (without one)
    /// is restored (builder style).
    pub fn with_legacy_command_ids(mut self, ids: Vec<u64>) -> Self {
        self.legacy_command_ids = ids;
        self
    }

    /// Enable idempotent command dedup (builder style).
    pub fn with_dedup(mut self, dedup: bool) -> Self {
        self.dedup = dedup;
        self
    }

    /// Set the fee schedule (builder style).
    pub fn with_fees(mut self, fees: crate::fees::FeeSchedule) -> Self {
        self.fees = fees;
        self
    }

    fn create_book(&mut self, instrument: InstrumentId, pool_orders: usize, prefault: bool) {
        let factory = self.factory;
        let stp = self.stp;
        self.engines.entry(instrument).or_insert_with(|| {
            MatchingEngine::with_pool(factory(), pool_orders, prefault).with_stp(stp)
        });
    }

    fn engine_for(&mut self, instrument: InstrumentId) -> &mut MatchingEngine {
        let factory = self.factory;
        let (pool, prefault) = self.default_pool;
        let stp = self.stp;
        self.engines
            .entry(instrument)
            .or_insert_with(|| MatchingEngine::with_pool(factory(), pool, prefault).with_stp(stp))
    }

    /// Read-only view of an instrument's engine (diagnostics, tests).
    pub fn engine(&self, instrument: InstrumentId) -> Option<&MatchingEngine> {
        self.engines.get(&instrument)
    }

    /// Net position of `user` on `instrument` (buys +, sells -).
    pub fn position(&self, user: u64, instrument: InstrumentId) -> i64 {
        self.positions
            .get(&(user, instrument))
            .copied()
            .unwrap_or(0)
    }

    /// Whether the instrument is currently halted.
    pub fn is_halted(&self, instrument: InstrumentId) -> bool {
        self.halted.contains(&instrument)
    }

    /// Export the full state of every engine (snapshot capture).
    pub fn export_state(&self) -> Vec<EngineState> {
        let mut states: Vec<EngineState> = self
            .engines
            .iter()
            .map(|(&instrument, e)| EngineState {
                instrument,
                engine_seq: e.seq(),
                orders: e.export_orders(),
                stops: self
                    .stops
                    .get(&instrument)
                    .map(TriggerBook::export)
                    .unwrap_or_default(),
            })
            .collect();
        states.sort_by_key(|s| s.instrument);
        states
    }

    /// Restore engines from a snapshot. Only valid on a fresh processor.
    pub fn restore_state(&mut self, snap: &Snapshot) {
        self.max_new_id = self.max_new_id.max(snap.max_cmd_id);
        self.max_admin_id = self.max_admin_id.max(snap.max_admin_id);
        self.halted
            .extend(snap.halted.iter().map(|&i| InstrumentId(i)));
        self.suspended.extend(snap.suspended.iter().copied());
        for &(u, i, q) in &snap.positions {
            *self.positions.entry((u, InstrumentId(i))).or_insert(0) += q;
        }
        // Exact command-id dedupe. A v6 snapshot carries the window itself.
        // Older ones do not: fall back to what the previous code used (the
        // ids of every order the snapshot holds, plus the ids the caller
        // recovered from its uncompacted log), so the first restart after an
        // upgrade decides retries exactly as before.
        if let Some(ids) = &snap.recent_command_ids {
            self.seen_command_ids.clear();
            for &id in ids {
                self.seen_command_ids.insert(id);
            }
        } else {
            for id in std::mem::take(&mut self.legacy_command_ids) {
                self.seen_command_ids.insert(id);
            }
            for e in &snap.engines {
                for id in e.orders.iter().map(|order| order.id.0) {
                    self.seen_command_ids.insert(id);
                }
                for id in e.stops.iter().map(|stop| stop.order.id.0) {
                    self.seen_command_ids.insert(id);
                }
            }
        }
        for e in &snap.engines {
            let engine = self.engine_for(e.instrument);
            engine.restore(e.engine_seq, &e.orders);
            for stop in &e.stops {
                self.stop_seq += 1;
                self.stops
                    .entry(e.instrument)
                    .or_default()
                    .insert(*stop, self.stop_seq);
                self.pending_stops += 1;
            }
        }
    }

    /// Order-sensitive fingerprint of the complete matching state (books +
    /// sequence counters). Equal fingerprints = identical state.
    pub fn state_fingerprint(&self) -> u64 {
        self.fingerprint_state(|_| true)
    }

    /// Order-sensitive fingerprint of just the instruments in one asset
    /// category (`asset_category(instrument, category_size) == category_id`).
    ///
    /// A route migration moves a category's *matching ownership* from one Raft
    /// group to another; the cutover is only safe once the source and target
    /// groups have byte-identical book state for that category. This computes
    /// the value both sides must agree on, so [`crate::cluster::RouteControlPlane::caught_up`]
    /// can verify equality against real state instead of trusting a
    /// caller-supplied number. Determinism holds because [`export_state`] sorts
    /// by instrument and the category filter preserves that order.
    ///
    /// [`export_state`]: Self::export_state
    pub fn category_fingerprint(&self, category_id: u32, category_size: u32) -> u64 {
        self.fingerprint_state(|instrument| {
            crate::sharding::asset_category(instrument, category_size) == category_id
        })
    }

    /// Shared fingerprint core: fold every selected engine's sequence and
    /// resting orders into one FNV-1a hash. Delegates to the free
    /// [`fingerprint_engine_states`] so a fingerprint computed from a durable
    /// snapshot (see [`crate::migration`]) is bit-identical to one computed
    /// from the live processor — a migration cutover compares the two.
    fn fingerprint_state(&self, select: impl Fn(InstrumentId) -> bool) -> u64 {
        fingerprint_engine_states(&self.export_state(), select)
    }

    /// Apply one command, emitting every resulting report through `emit`.
    pub fn process(&mut self, cmd: Command, emit: &mut dyn FnMut(ExecReport)) {
        let exact_id = cmd.id();
        if exact_id != 0 && (!self.seen_command_ids.insert(exact_id) || self.would_duplicate_live(&cmd)) {
            emit(ExecReport::CommandRejected {
                instrument: cmd.instrument(),
                command_id: exact_id,
                reason: "duplicate",
            });
            return;
        }
        // Idempotency gate: every command carries a unique increasing id (New =
        // order id; Cancel/Modify = cmd_id; ForceClose = close-order id). A
        // re-sent duplicate (id <= high-water mark) is rejected, never applied
        // twice. Deterministic under replay: duplicates are journaled too, and
        // the same gate skips them again.
        if self.dedup {
            // (id, instrument, is_admin_stream)
            let (id, inst, admin) = match &cmd {
                Command::New(o) | Command::NewConditional(o, _) => {
                    (o.id.0, o.instrument, false)
                }
                Command::Cancel {
                    cmd_id, instrument, ..
                } => (*cmd_id, *instrument, true),
                Command::Modify {
                    cmd_id, instrument, ..
                } => (*cmd_id, *instrument, true),
                Command::ForceClose {
                    close_order_id,
                    instrument,
                    ..
                } => (close_order_id.0, *instrument, true),
                Command::Halt { cmd_id, instrument } => (*cmd_id, *instrument, true),
                Command::Resume { cmd_id, instrument } => (*cmd_id, *instrument, true),
                Command::HaltUser {
                    cmd_id, instrument, ..
                } => (*cmd_id, *instrument, true),
                Command::ResumeUser {
                    cmd_id, instrument, ..
                } => (*cmd_id, *instrument, true),
                // Batches are gated per inner command (recursive process call).
                Command::Batch(_) => (0, InstrumentId(0), false),
            };
            if id != 0 {
                let cursor = if admin {
                    &mut self.max_admin_id
                } else {
                    &mut self.max_new_id
                };
                if id <= *cursor {
                    emit(ExecReport::CommandRejected {
                        instrument: inst,
                        command_id: id,
                        reason: "duplicate",
                    });
                    return;
                }
                *cursor = id;
            }
        }
        match cmd {
            Command::Batch(cmds) => {
                // Atomic: the shard is single-threaded, so processing the group
                // inside one call admits no interleaving.
                for c in cmds {
                    self.process(c, emit);
                }
            }
            Command::HaltUser {
                instrument, user, ..
            } => {
                self.suspended.insert(user);
                emit(ExecReport::UserHalted { instrument, user });
            }
            Command::ResumeUser {
                instrument, user, ..
            } => {
                self.suspended.remove(&user);
                emit(ExecReport::UserResumed { instrument, user });
            }
            Command::Halt { instrument, .. } => {
                self.halted.insert(instrument);
                emit(ExecReport::Halted { instrument });
            }
            Command::Resume { instrument, .. } => {
                self.halted.remove(&instrument);
                emit(ExecReport::Resumed { instrument });
            }
            Command::New(order) => {
                let instrument = order.instrument;
                let last_price = self.process_new(order, emit);
                if self.pending_stops > 0 {
                    self.fire_stops(instrument, last_price, emit);
                }
            }
            Command::NewConditional(order, trigger) => self.place_stop(order, trigger, emit),
            Command::Cancel {
                instrument,
                order_id,
                ..
            } => {
                if let Some(cancelled) = self.engine_for(instrument).cancel_order(order_id) {
                    emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
                        user: cancelled.user,
                    });
                } else if let Some(stop) = (self.pending_stops > 0)
                    .then(|| self.stops.get_mut(&instrument))
                    .flatten()
                    .and_then(|book| book.remove(order_id))
                {
                    self.pending_stops -= 1;
                    emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
                        user: stop.order.user,
                    });
                } else {
                    emit(ExecReport::NotFound {
                        instrument,
                        order_id,
                    });
                }
            }
            Command::Modify {
                instrument,
                order_id,
                new_price,
                new_qty,
                cmd_id,
            } => {
                // An untriggered conditional order is not amendable (cancel
                // and re-place it); refusing leaves it untouched.
                if self.pending_stops > 0
                    && self
                        .stops
                        .get(&instrument)
                        .is_some_and(|book| book.contains(order_id))
                {
                    emit(ExecReport::CommandRejected {
                        instrument,
                        command_id: cmd_id,
                        reason: "stop-not-amendable",
                    });
                    return;
                }
                // Trading rules apply to the amended price/size too. A refused
                // amend leaves the order untouched: a command-level rejection.
                if !self.specs.is_empty() {
                    if let Some(spec) = self.specs.get(instrument) {
                        if let Err(reason) = spec.check_amend(new_price, new_qty) {
                            emit(ExecReport::CommandRejected {
                                instrument,
                                command_id: cmd_id,
                                reason,
                            });
                            return;
                        }
                    }
                }
                let engine = self.engine_for(instrument);
                let user = engine.book().get(order_id).map_or(0, |order| order.user);
                match engine.modify(order_id, new_price, new_qty) {
                    ModifyOutcome::NotFound => emit(ExecReport::NotFound {
                        instrument,
                        order_id,
                    }),
                    ModifyOutcome::Reduced {
                        order_id,
                        remaining,
                    } => emit(ExecReport::Modified {
                        instrument,
                        order_id,
                        remaining,
                        user,
                    }),
                    ModifyOutcome::Cancelled { order_id } => emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
                        user,
                    }),
                    ModifyOutcome::Requoted(report) => {
                        let last_price = report.trades.last().map(|t| t.price);
                        for t in &report.trades {
                            let (maker_fee, taker_fee) = self.fees.fees(t.price, t.quantity);
                            emit(ExecReport::Trade {
                                instrument,
                                taker: t.taker,
                                maker: t.maker,
                                aggressor: t.aggressor,
                                price: t.price,
                                qty: t.quantity,
                                maker_fee,
                                taker_fee,
                                taker_user: t.taker_user,
                                maker_user: t.maker_user,
                            });
                            if t.maker_remaining == 0 {
                                emit(ExecReport::MakerFilled {
                                    instrument,
                                    order_id: t.maker,
                                    user: t.maker_user,
                                });
                            }
                        }
                        let remaining = new_qty.saturating_sub(report.filled);
                        match report.status {
                            OrderStatus::Filled => emit(ExecReport::Filled {
                                instrument,
                                order_id,
                                user,
                            }),
                            OrderStatus::Resting | OrderStatus::PartiallyFilled => {
                                emit(ExecReport::Modified {
                                    instrument,
                                    order_id,
                                    remaining,
                                    user,
                                })
                            }
                            _ => emit(ExecReport::Cancelled {
                                instrument,
                                order_id,
                                user,
                            }),
                        }
                        if self.pending_stops > 0 {
                            self.fire_stops(instrument, last_price, emit);
                        }
                    }
                }
            }
            Command::ForceClose {
                instrument,
                user,
                close_order_id,
                close_side,
                close_qty,
            } => {
                // 1. Pull every resting order of the user, and every
                //    untriggered conditional one (it would re-open exposure).
                let mut cancelled = self.engine_for(instrument).cancel_all_for_user(user);
                if self.pending_stops > 0 {
                    if let Some(book) = self.stops.get_mut(&instrument) {
                        let pulled = book.remove_user(user);
                        self.pending_stops -= pulled.len();
                        cancelled.extend(pulled);
                    }
                }
                for order_id in cancelled {
                    emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
                        user,
                    });
                }
                // 2. Flatten the position with a protected market order.
                if close_qty > 0 {
                    let mut close = Order::market(close_order_id, close_side, close_qty)
                        .on(instrument)
                        .by(user);
                    close.tif = TimeInForce::Ioc;
                    let last_price = self.process_new(close, emit);
                    if self.pending_stops > 0 {
                        self.fire_stops(instrument, last_price, emit);
                    }
                }
            }
        }
    }

    /// Admit and match one ordinary order. Returns the price of the last trade
    /// it printed (the input to [`Self::fire_stops`]), if any.
    fn process_new(&mut self, mut order: Order, emit: &mut dyn FnMut(ExecReport)) -> Option<Price> {
        let instrument = order.instrument;
        let id = order.id;
        let user = order.user;

        // Circuit breaker: a halted instrument accepts no new orders (cancels
        // and modifies still work so users can pull quotes).
        if self.halted.contains(&instrument) {
            emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                user,
                reason: "halted",
            });
            return None;
        }
        if order.user != 0 && self.suspended.contains(&order.user) {
            emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                user,
                reason: "user-suspended",
            });
            return None;
        }

        // Order-shape checks, cheapest first:
        // 0. post-only must be a limit (a market order always takes), and the
        //    instrument's trading rules (tick / lot / qty / price / notional);
        let post_only = order.tif == TimeInForce::PostOnly;
        if post_only && order.order_type == OrderType::Market {
            emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                user,
                reason: "post-only",
            });
            return None;
        }
        if !self.specs.is_empty() {
            if let Some(spec) = self.specs.get(instrument) {
                if let Err(reason) = spec.check(&order) {
                    emit(ExecReport::Rejected {
                        instrument,
                        order_id: id,
                        user,
                        reason,
                    });
                    return None;
                }
            }
        }
        // Synchronous pre-trade risk:
        // 1. static limits (order shape);
        if let Some(limits) = &self.limits {
            if let Err(reason) = limits.check_static(&order) {
                emit(ExecReport::Rejected {
                    instrument,
                    order_id: id,
                    user,
                    reason,
                });
                return None;
            }
        }
        // 2. anti-spike price banding (may convert market orders into
        //    protected marketable limits);
        if let Some(guard) = &self.guard {
            if let Err(reason) = guard.vet(&mut order) {
                emit(ExecReport::Rejected {
                    instrument,
                    order_id: id,
                    user,
                    reason,
                });
                return None;
            }
        }
        // 3. per-user open-order cap (needs book state).
        if let Some(limits) = self.limits {
            if limits.max_user_orders > 0 && order.user != 0 {
                let open = self
                    .engine_for(instrument)
                    .book()
                    .user_open_orders(order.user);
                if open >= limits.max_user_orders {
                    emit(ExecReport::Rejected {
                        instrument,
                        order_id: id,
                        user,
                        reason: "max-user-orders",
                    });
                    return None;
                }
            }
        }

        emit(ExecReport::Accepted {
            instrument,
            order_id: id,
            user,
        });
        // Zero-allocation submit: trades land in the processor's scratch buffer.
        let mut trades = std::mem::take(&mut self.trades_buf);
        trades.clear();
        let (_, status, filled, _) = self.engine_for(instrument).submit_into(order, &mut trades);
        // Report makers cancelled by self-trade prevention, if any. Skipped
        // entirely under `Allow` — no extra lookup on the default hot path.
        if self.stp != SelfTradePolicy::Allow {
            let engine = self.engines.get(&instrument).expect("engine exists");
            for &(order_id, user) in engine.stp_cancelled() {
                emit(ExecReport::Cancelled {
                    instrument,
                    order_id,
                    user,
                });
            }
        }
        for t in &trades {
            let (maker_fee, taker_fee) = self.fees.fees(t.price, t.quantity);
            // Position ledger: buyer +qty, seller -qty (net, per user/instrument).
            let q = t.quantity as i64;
            let (buyer, seller) = match t.aggressor {
                Side::Buy => (t.taker_user, t.maker_user),
                Side::Sell => (t.maker_user, t.taker_user),
            };
            if buyer != 0 {
                *self.positions.entry((buyer, instrument)).or_insert(0) += q;
            }
            if seller != 0 {
                *self.positions.entry((seller, instrument)).or_insert(0) -= q;
            }
            emit(ExecReport::Trade {
                instrument,
                taker: t.taker,
                maker: t.maker,
                aggressor: t.aggressor,
                price: t.price,
                qty: t.quantity,
                maker_fee,
                taker_fee,
                taker_user: t.taker_user,
                maker_user: t.maker_user,
            });
            if t.maker_remaining == 0 {
                emit(ExecReport::MakerFilled {
                    instrument,
                    order_id: t.maker,
                    user: t.maker_user,
                });
            }
        }
        let last_price = trades.last().map(|t| t.price);
        self.trades_buf = trades;
        match status {
            OrderStatus::Filled => emit(ExecReport::Filled {
                instrument,
                order_id: id,
                user,
            }),
            OrderStatus::PartiallyFilled => emit(ExecReport::PartiallyFilled {
                instrument,
                order_id: id,
                user,
                filled,
            }),
            OrderStatus::Resting => emit(ExecReport::Resting {
                instrument,
                order_id: id,
                user,
            }),
            OrderStatus::Cancelled => emit(ExecReport::Cancelled {
                instrument,
                order_id: id,
                user,
            }),
            OrderStatus::Rejected => emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                user,
                // The engine rejects a post-only that would take liquidity,
                // and a FOK that cannot fill whole.
                reason: if post_only { "post-only" } else { "unfillable" },
            }),
        }
        last_price
    }

    /// Admit a conditional order into the instrument's trigger book. Static
    /// checks run now (halt, suspension, trading rules, limits) so a bad
    /// order fails at placement; the dynamic ones (price band, book state)
    /// run again when it fires and enters through [`Self::process_new`].
    // Kept out of line: conditional orders are rare next to plain orders, and
    // their code must not grow the hot `process` / `process_new` paths.
    #[cold]
    #[inline(never)]
    fn place_stop(&mut self, order: Order, trigger: Trigger, emit: &mut dyn FnMut(ExecReport)) {
        let instrument = order.instrument;
        let id = order.id;
        let user = order.user;
        let reject = |reason, emit: &mut dyn FnMut(ExecReport)| {
            emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                user,
                reason,
            })
        };
        if self.halted.contains(&instrument) {
            return reject("halted", emit);
        }
        if order.user != 0 && self.suspended.contains(&order.user) {
            return reject("user-suspended", emit);
        }
        // Budget orders carry a total in `price` and post-only must rest on
        // arrival; neither is meaningful as a triggered order.
        if trigger.price == 0
            || matches!(
                order.tif,
                TimeInForce::IocBudget | TimeInForce::FokBudget | TimeInForce::PostOnly
            )
        {
            return reject("invalid-stop", emit);
        }
        if !self.specs.is_empty() {
            if let Some(spec) = self.specs.get(instrument) {
                let checked = spec
                    .check(&order)
                    .and_then(|()| spec.check_amend(trigger.price, order.quantity));
                if let Err(reason) = checked {
                    return reject(reason, emit);
                }
            }
        }
        if let Some(limits) = &self.limits {
            if let Err(reason) = limits.check_static(&order) {
                return reject(reason, emit);
            }
        }
        self.engine_for(instrument); // the instrument now has state to snapshot
        self.stop_seq += 1;
        self.stops
            .entry(instrument)
            .or_default()
            .insert(StopOrder { order, trigger }, self.stop_seq);
        self.pending_stops += 1;
        emit(ExecReport::StopAccepted {
            instrument,
            order_id: id,
            user,
        });
    }

    /// Fire every conditional order armed by trades on `instrument`, starting
    /// from the price of the trade that just printed. A fired order that
    /// itself trades moves the price on, so cascades resolve here, in one
    /// command, in the trigger book's deterministic priority order. Callers
    /// check `pending_stops > 0` first, so a venue with no stops never calls it.
    #[cold]
    #[inline(never)]
    fn fire_stops(
        &mut self,
        instrument: InstrumentId,
        mut last_price: Option<Price>,
        emit: &mut dyn FnMut(ExecReport),
    ) {
        while self.pending_stops > 0 {
            let Some(price) = last_price else { return };
            let Some(stop) = self
                .stops
                .get_mut(&instrument)
                .and_then(|book| book.pop_triggered(price))
            else {
                return;
            };
            self.pending_stops -= 1;
            if self.stops.get(&instrument).is_some_and(TriggerBook::is_empty) {
                self.stops.remove(&instrument);
            }
            emit(ExecReport::Triggered {
                instrument,
                order_id: stop.order.id,
                user: stop.order.user,
            });
            if let Some(price) = self.process_new(stop.order, emit) {
                last_price = Some(price);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Matching-side shard
// ---------------------------------------------------------------------------

struct Shard {
    shard_id: u32,
    /// Live replication fanout (standby hot-sync); `None` = standalone.
    rep: Option<crate::replication::RepFanout>,
    /// Command seq when journaling is off (mirrors the journal seq otherwise).
    rep_seq: u64,
    metrics: Arc<crate::metrics::Metrics>,
    processor: Processor,
    high_rx: Consumer<QueuedCommand>,
    normal_rx: Consumer<QueuedCommand>,
    instrument_rx: Vec<InstrumentRx>,
    next_instrument: usize,
    result_tx: Producer<ExecutionReportEvent>,
    journal: Option<JournalWriter>,
    /// Portable WALs written per asset in standalone/legacy mode. Production
    /// authoritative-Raft mode leaves this unset so the consensus WAL remains
    /// the single command log.
    asset_journal: Option<AssetJournalSet>,
    execution_outbox: Option<crate::execution_outbox::ExecutionOutboxWriter>,
    /// Directory containing durable Raft application proofs.
    applied_root: Option<PathBuf>,
    /// Highest Raft index represented by the live in-memory state.
    last_raft_index: u64,
    raft_wal_authoritative: bool,
    raft_group_id: u32,
    snapshot_path: Option<PathBuf>,
    snapshot_every: Option<Duration>,
    last_snapshot: Instant,
    /// In-flight background snapshot write (Raft mode), at most one.
    snapshot_writer: Option<thread::JoinHandle<()>>,
    snap_request: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    parked: Arc<AtomicUsize>,
    default_pool: (usize, bool),
    pin_core: Option<usize>,
    last_depth: Instant,
    /// Instruments whose book changed since the last depth publish. Depth is
    /// only recomputed for these, so a shard hosting thousands of mostly-idle
    /// books doesn't rescan every ladder every cadence tick.
    dirty: HashSet<InstrumentId>,
    /// Reusable scratch for the per-tick depth batch (no periodic allocation).
    depth_buf: Vec<ExecReport>,
    /// Consecutive idle loop iterations, used to back off the poll interval.
    idle_streak: u32,
}

/// Depth-of-market publish cadence and ladder size.
const DEPTH_EVERY: Duration = Duration::from_millis(200);
const DEPTH_LEVELS: usize = 5;

/// Idle poll interval as a function of how many consecutive idle iterations the
/// shard has seen. Tight (50µs) through short gaps between bursts, then two
/// steps out to 500µs once the shard has been quiet long enough that latency no
/// longer matters (nothing is arriving anyway).
#[inline]
fn idle_backoff(idle_streak: u32) -> Duration {
    // At 50µs/iter, ~128 iters ≈ 6ms of continuous silence before the first
    // step; ~1024 ≈ tens of ms before the widest interval.
    match idle_streak {
        0..=128 => Duration::from_micros(50),
        129..=1024 => Duration::from_micros(200),
        _ => Duration::from_micros(500),
    }
}

impl Shard {
    fn run(&mut self) {
        self.processor.default_pool = self.default_pool;
        if let Some(core) = self.pin_core {
            if let Err(reason) = crate::affinity::pin_current_thread(core) {
                crate::log_warn!("shard", self.shard_id; "CPU pin to core {core} not applied: {reason}");
            }
        }

        let mut is_parked = false;
        loop {
            if !self.started.load(Ordering::Acquire) {
                if !is_parked {
                    self.parked.fetch_add(1, Ordering::Release);
                    is_parked = true;
                }
                if !self.running.load(Ordering::Acquire) {
                    break;
                }
                thread::park_timeout(Duration::from_micros(100));
                continue;
            }
            if is_parked {
                self.parked.fetch_sub(1, Ordering::Release);
                is_parked = false;
            }

            // Always fully drain high-priority (cancel/modify/force-close) first.
            let mut worked = self.drain_high();

            // Then a bounded batch of new orders, re-checking high in between.
            let mut n = 0;
            while n < NORMAL_BATCH {
                let cmd = self.pop_next_normal();
                match cmd {
                    Some(cmd) => {
                        self.handle(cmd);
                        worked = true;
                        n += 1;
                        self.drain_high();
                    }
                    None => break,
                }
            }

            if worked {
                // Busy: reset the idle backoff so the first quiet tick polls
                // tightly again, and publish depth on cadence.
                self.idle_streak = 0;
                self.maybe_publish_depth();
            } else {
                if !self.running.load(Ordering::Acquire)
                    && self.high_rx.is_empty()
                    && self.normal_rx.is_empty()
                    && self
                        .instrument_rx
                        .iter()
                        .all(|rx| rx.high.is_empty() && rx.normal.is_empty())
                {
                    break;
                }
                // Idle: bound the journal loss window and honour snapshots.
                if let Some(j) = &mut self.journal {
                    let _ = j.tick();
                }
                if self.snapshot_due() {
                    self.take_snapshot();
                }
                self.maybe_publish_depth();
                // Adaptive backoff: bursty traffic with micro-gaps keeps polling
                // tightly (50µs) so the first order after a lull is picked up
                // fast; only *sustained* silence widens the interval, so a truly
                // idle shard stops spinning a pinned core down to a 500µs poll.
                // There is no producer-side unpark, so the cap also bounds the
                // worst-case wake latency after quiet: 500µs.
                self.idle_streak = self.idle_streak.saturating_add(1);
                let backoff = idle_backoff(self.idle_streak);
                thread::park_timeout(backoff);
            }
        }
        // Final snapshot on clean shutdown makes the next start instant — but
        // only when periodic snapshotting is enabled: journal-only deployments
        // keep their full journal (e.g. for audit/time replay).
        if let Some(writer) = self.snapshot_writer.take() {
            let _ = writer.join();
        }
        if self.snapshot_every.is_some() {
            self.take_snapshot_sync();
        }
        // Shutdown flush failures must be visible: an operator relying on a
        // clean stop for durability needs to know the tail didn't make it.
        if let Some(j) = &mut self.journal {
            if let Err(e) = j.flush() {
                crate::log_error!("shard", self.shard_id; "event=shutdown_flush_failed target=journal error={e}");
            }
        }
        if let Some(j) = &mut self.asset_journal {
            if let Err(e) = j.flush_all() {
                crate::log_error!("shard", self.shard_id; "event=shutdown_flush_failed target=asset-journal error={e}");
            }
        }
        if is_parked {
            self.parked.fetch_sub(1, Ordering::Release);
        }
    }

    /// Publish a top-N depth snapshot per instrument onto the result stream
    /// (mirrored to market-data subscribers by the gateway), on a cadence.
    fn maybe_publish_depth(&mut self) {
        if self.last_depth.elapsed() < DEPTH_EVERY {
            return;
        }
        if self.dirty.is_empty() {
            return; // nothing changed since the last publish
        }
        self.last_depth = Instant::now();
        // Collect first (immutable borrow of the engines), then emit. The buffer
        // is a reusable member, so a steady publish cadence allocates nothing.
        let mut out = std::mem::take(&mut self.depth_buf);
        out.clear();
        for &instrument in self.dirty.iter() {
            let Some(engine) = self.processor.engines.get(&instrument) else {
                continue; // command touched no book (e.g. rejected admin action)
            };
            let book = engine.book();
            if book.is_empty() {
                continue;
            }
            let bids = book.top_levels(Side::Buy, DEPTH_LEVELS);
            let asks = book.top_levels(Side::Sell, DEPTH_LEVELS);
            for (i, &(price, qty)) in bids.iter().enumerate() {
                out.push(ExecReport::DepthLevel {
                    instrument,
                    side: Side::Buy,
                    level: i as u8,
                    price,
                    qty,
                });
            }
            for (i, &(price, qty)) in asks.iter().enumerate() {
                out.push(ExecReport::DepthLevel {
                    instrument,
                    side: Side::Sell,
                    level: i as u8,
                    price,
                    qty,
                });
            }
            out.push(ExecReport::DepthEnd {
                instrument,
                bid_levels: bids.len() as u8,
                ask_levels: asks.len() as u8,
            });
        }
        self.dirty.clear();
        for r in out.drain(..) {
            self.emit_report(r);
        }
        self.depth_buf = out; // hand the buffer (and its capacity) back
    }

    fn emit_report(&self, report: ExecReport) {
        push_result_event(
            &self.result_tx,
            &self.metrics,
            ExecutionReportEvent {
                raft_index: None,
                ordinal: 0,
                report,
            },
        );
    }

    fn snapshot_due(&mut self) -> bool {
        if self.snapshot_path.is_none() {
            return false;
        }
        if self.snap_request.swap(false, Ordering::AcqRel) {
            return true;
        }
        match self.snapshot_every {
            Some(every) => self.last_snapshot.elapsed() >= every,
            None => false,
        }
    }

    /// Periodic snapshot. In authoritative-Raft mode the matching thread only
    /// copies its state; writing and fsyncing the file happen on a background
    /// thread, so matching no longer pauses for the disk (measured up to
    /// ~0.9 s per group under load with ~9 MB books). At most one write is in
    /// flight; a snapshot that comes due while one is running is skipped and
    /// retried on the next idle tick. Standalone mode stays synchronous: it
    /// truncates its command journal after the snapshot, which is only safe
    /// when nothing was appended in between.
    fn take_snapshot(&mut self) {
        if !(self.raft_wal_authoritative && self.journal.is_none()) {
            self.take_snapshot_sync();
            return;
        }
        let Some(path) = self.snapshot_path.clone() else {
            return;
        };
        if self
            .snapshot_writer
            .as_ref()
            .is_some_and(|writer| !writer.is_finished())
        {
            return; // previous write still running; try again later
        }
        if let Some(writer) = self.snapshot_writer.take() {
            let _ = writer.join();
        }
        let started = Instant::now();
        let captured = capture_processor_snapshot(&self.processor, 0, self.last_raft_index);
        let paused_ns = started.elapsed().as_nanos() as u64;
        self.metrics.snapshots.fetch_add(1, Ordering::Relaxed);
        self.metrics.snapshot_ns_last.store(paused_ns, Ordering::Relaxed);
        self.metrics.snapshot_ns_max.fetch_max(paused_ns, Ordering::Relaxed);
        self.last_snapshot = Instant::now();
        let metrics = self.metrics.clone();
        self.snapshot_writer = Some(
            thread::Builder::new()
                .name(format!("snapshot-{}", self.shard_id))
                .spawn(move || {
                    let started = Instant::now();
                    if let Err(error) = captured.write(&path) {
                        crate::log_error!("snapshot", "event=snapshot_write_failed error={error}");
                        return;
                    }
                    let took = started.elapsed();
                    metrics
                        .snapshot_write_ns_last
                        .store(took.as_nanos() as u64, Ordering::Relaxed);
                    if let Ok(meta) = std::fs::metadata(&path) {
                        metrics.snapshot_bytes.store(meta.len(), Ordering::Relaxed);
                    }
                })
                .expect("spawn snapshot writer"),
        );
    }

    /// Capture state and persist it atomically on the matching thread.
    /// Standalone mode then truncates its covered journal; authoritative-Raft
    /// mode retains the consensus WAL and stores its applied index in the
    /// snapshot.
    fn take_snapshot_sync(&mut self) {
        let Some(path) = self.snapshot_path.as_ref() else {
            return;
        };
        let journal_seq = match self.journal.as_mut() {
            Some(journal) => {
                if journal.flush().is_err() {
                    return;
                }
                journal.seq()
            }
            None => 0,
        };
        let started = Instant::now();
        let persisted =
            persist_processor_snapshot(&self.processor, path, journal_seq, self.last_raft_index);
        // The shard matches nothing while it snapshots, so this is a pause in
        // matching that grows with the books.
        let took = started.elapsed();
        let took_ns = took.as_nanos() as u64;
        self.metrics.snapshots.fetch_add(1, Ordering::Relaxed);
        self.metrics.snapshot_ns_last.store(took_ns, Ordering::Relaxed);
        self.metrics.snapshot_ns_max.fetch_max(took_ns, Ordering::Relaxed);
        if let Ok(meta) = std::fs::metadata(path) {
            self.metrics.snapshot_bytes.store(meta.len(), Ordering::Relaxed);
        }
        if took > Duration::from_millis(100) {
            crate::log_warn!(
                "snapshot",
                "event=slow_snapshot took_ms={} bytes={}",
                took.as_millis(),
                self.metrics.snapshot_bytes.load(Ordering::Relaxed)
            );
        }
        if persisted.is_ok() {
            if let Some(journal) = self.journal.as_mut() {
                let _ = journal.truncate();
            }
        }
        self.last_snapshot = Instant::now();
    }

    fn drain_high(&mut self) -> bool {
        let mut worked = false;
        while let Some(cmd) = self.high_rx.pop() {
            self.handle(cmd);
            worked = true;
        }
        for index in 0..self.instrument_rx.len() {
            while let Some(cmd) = self.instrument_rx[index].high.pop() {
                self.handle(cmd);
                worked = true;
            }
        }
        worked
    }

    /// Fair round-robin across configured asset queues, then the legacy shard
    /// queue. A hot instrument therefore cannot monopolize a worker's intake
    /// budget and starve unrelated assets on the same machine.
    fn pop_next_normal(&mut self) -> Option<QueuedCommand> {
        let len = self.instrument_rx.len();
        for _ in 0..len {
            let index = self.next_instrument % len;
            self.next_instrument = (self.next_instrument + 1) % len;
            if let Some(cmd) = self.instrument_rx[index].normal.pop() {
                return Some(cmd);
            }
        }
        self.normal_rx.pop()
    }

    /// Journal the command (in processing order), replicate it, then apply it.
    /// One serialization point, one seq series: journal and standbys see the
    /// identical total order.
    fn handle(&mut self, queued: QueuedCommand) {
        let started = Instant::now();
        let QueuedCommand {
            command: cmd,
            raft_index,
            replay_proof,
        } = queued;
        // Batches flatten HERE: each inner command is journaled/replicated as
        // its own record (the total order stays flat and replayable); atomicity
        // holds because this thread pops nothing until the loop finishes.
        if let Command::Batch(cmds) = cmd {
            if let Some(index) = raft_index {
                self.handle_committed_batch(index, cmds, replay_proof, started);
                return;
            }
            for c in cmds {
                self.handle(QueuedCommand {
                    command: c,
                    raft_index: None,
                    replay_proof: None,
                });
            }
            return;
        }
        if let Some(asset_journal) = &mut self.asset_journal {
            // The asset WAL is intentionally appended before matching. A
            // failure is fail-closed: processing a command that cannot be
            // replayed on a destination node would make migration unsafe.
            let append = match raft_index {
                Some(index) => asset_journal.append_committed(index, &cmd),
                None => asset_journal.append(&cmd),
            };
            if let Err(error) = append {
                self.metrics
                    .asset_wal_errors
                    .fetch_add(1, Ordering::Relaxed);
                crate::log_error!(
                    "shard", self.shard_id;
                    "event=append_failed target=asset-journal action=reject instrument={} raft_index={} error={error}",
                    cmd.instrument().0,
                    raft_index.map(|i| i as i64).unwrap_or(-1)
                );
                return;
            }
        }
        if self.journal.is_some() || self.rep.is_some() {
            let mut frame = [0u8; wire::MSG_LEN];
            wire::encode_command(&cmd, &mut frame);
            let seq = match &mut self.journal {
                Some(j) => j
                    .append(journal::now_nanos(), &frame)
                    .unwrap_or_else(|_| self.rep_seq + 1),
                None => self.rep_seq + 1,
            };
            self.rep_seq = seq;
            if let Some(raft_index) = raft_index {
                if let Some(journal) = &mut self.journal {
                    journal.sync_data().expect("sync raft command journal");
                }
                if let Some(asset_journal) = &self.asset_journal {
                    asset_journal
                        .mark_raft_applied(cmd.instrument(), raft_index)
                        .expect("persist raft application watermark");
                }
            }
            if let Some(rep) = &self.rep {
                rep.publish(self.shard_id, seq, &frame);
            }
        }
        if self.rep_seq > 0 {
            self.metrics
                .journal_seq
                .fetch_max(self.rep_seq, Ordering::Relaxed);
        }
        let mut ordinal = 0;
        // Mark the touched book so the next cadence republishes just this
        // instrument's depth (admin commands rarely move the book, but the
        // resulting single redundant publish is cheaper than tracking intent).
        self.dirty.insert(cmd.instrument());
        self.process_and_emit(cmd, raft_index, &mut ordinal);
        if let Some(index) = raft_index {
            self.metrics.set_raft_applied_index(index);
        }
        let command_ns = started.elapsed().as_nanos() as u64;
        self.metrics.record_command_latency(command_ns);
        self.metrics
            .record_latency_hist(crate::metrics::LatencyMetric::Command, command_ns);
    }

    fn process_and_emit(&mut self, command: Command, raft_index: Option<u64>, ordinal: &mut u32) {
        let started = Instant::now();
        let result_tx = &self.result_tx;
        let metrics = &self.metrics;
        let outbox = &mut self.execution_outbox;
        let raft_group_id = self.raft_group_id;
        self.processor.process(command, &mut |report| {
            if let (Some(index), Some(outbox)) = (raft_index, outbox.as_mut()) {
                // Fail loud: a durable-outbox append failure (disk full, IO
                // error) must never silently drop an execution event. The
                // committed-batch path is fail-fast via match_and_record_batch;
                // this single-command path is reachable only for non-batch Raft
                // commands, and it too must halt rather than lose a fill.
                if let Err(error) = outbox.append(raft_group_id, index, *ordinal, &report) {
                    metrics.asset_wal_errors.fetch_add(1, Ordering::Relaxed);
                    panic!("append execution report to durable outbox: {error}");
                }
            }
            metrics.record(&report);
            let event = ExecutionReportEvent {
                raft_index,
                ordinal: *ordinal,
                report,
            };
            *ordinal = ordinal.saturating_add(1);
            // Backpressure: spin briefly, then park (and count) until the order
            // system drains — see `push_result_event`.
            push_result_event(result_tx, metrics, event);
        });
        let match_ns = started.elapsed().as_nanos() as u64;
        self.metrics.record_match_latency(match_ns);
        self.metrics
            .record_latency_hist(crate::metrics::LatencyMetric::Match, match_ns);
    }

    fn handle_committed_batch(
        &mut self,
        raft_index: u64,
        commands: Vec<Command>,
        replay_proof: Option<AppliedBatchProof>,
        started: Instant,
    ) {
        if commands.is_empty() {
            return;
        }
        if let Some(proof) = replay_proof {
            assert_eq!(
                proof.raft_index, raft_index,
                "Raft replay proof index does not match queued entry"
            );
            let mut reports = Vec::new();
            for command in commands {
                self.dirty.insert(command.instrument());
                self.processor
                    .process(command, &mut |report| reports.push(report));
            }
            let (count, fingerprint) = proof_digest(&reports);
            crate::asset_log::verify_applied_batch(proof, count, fingerprint)
                .unwrap_or_else(|error| panic!("{error}"));
            self.last_raft_index = raft_index;
            self.metrics.set_raft_applied_index(raft_index);
            return;
        }
        let wal_started = Instant::now();
        // Persistence failures on the quorum-committed path are fail-stop:
        // skipping a committed batch (while later batches keep applying) would
        // silently fork this replica's book state away from Raft and the other
        // members. Halting instead leaves a clean replay point.
        let touched = match &mut self.asset_journal {
            Some(asset_journal) => match asset_journal.append_committed_batch(&commands) {
                Ok(touched) => touched,
                Err(error) => {
                    self.metrics
                        .asset_wal_errors
                        .fetch_add(1, Ordering::Relaxed);
                    panic!(
                        "[shard {}] append committed Raft batch (index {raft_index}) to asset WAL: {error}",
                        self.shard_id
                    );
                }
            },
            None => Vec::new(),
        };
        let shard_id = self.shard_id;
        let mut replicated = Vec::with_capacity(commands.len());
        for command in &commands {
            let mut frame = [0u8; wire::MSG_LEN];
            wire::encode_command(command, &mut frame);
            let seq = match &mut self.journal {
                // A fabricated sequence after a failed append would desync the
                // durable journal from replication; committed commands must
                // fail-stop here too.
                Some(journal) => journal.append(journal::now_nanos(), &frame).unwrap_or_else(
                    |error| {
                        panic!(
                            "[shard {shard_id}] append committed Raft command (index {raft_index}) to shard journal: {error}"
                        )
                    },
                ),
                None => self.rep_seq + 1,
            };
            self.rep_seq = seq;
            replicated.push((seq, frame));
        }
        if let Some(journal) = &mut self.journal {
            journal
                .flush_to_os()
                .expect("flush committed Raft command batch journal");
        }
        // Match the batch and append every execution report to the outbox
        // *before* the durability barrier, so the outbox shares the batch's
        // single group-commit fsync and is guaranteed on disk before the
        // application watermark advances. This closes the crash window where a
        // watermarked-but-unwritten batch would be skipped on recovery and its
        // execution events lost forever. Reports are held back and delivered to
        // the order system only after the batch is durable (see below).
        for command in &commands {
            self.dirty.insert(command.instrument());
        }
        let emitted = self
            .match_and_record_batch(commands, raft_index)
            .expect("append committed batch execution reports to durable outbox");
        // The Linux asset-WAL barrier below is filesystem-wide. Flush the
        // outbox's userspace buffer first so WAL, shard journal and outbox are
        // all covered by that single syncfs instead of paying a second fsync.
        if let Some(outbox) = &mut self.execution_outbox {
            outbox
                .flush_to_os()
                .expect("flush committed execution outbox to OS");
        }
        // Group-commit barrier: command WAL(s) + execution outbox in one shot.
        // Timed on its own so a slow durability barrier can be alerted without
        // conflating it with the batch's matching/append work.
        let fsync_started = Instant::now();
        let used_filesystem_barrier = if let Some(asset_journal) = &self.asset_journal {
            asset_journal
                .sync_committed_batch(&touched)
                .expect("group commit asset and shard WAL files");
            cfg!(target_os = "linux")
        } else if let Some(journal) = &mut self.journal {
            journal
                .sync_data()
                .expect("sync committed Raft command batch journal");
            false
        } else {
            false
        };
        if let Some(outbox) = &mut self.execution_outbox {
            if used_filesystem_barrier {
                outbox.mark_synced();
            } else {
                outbox
                    .sync_data()
                    .expect("group commit execution outbox with WAL barrier");
            }
        }
        let fsync_elapsed = fsync_started.elapsed();
        let wal_ns = wal_started.elapsed().as_nanos() as u64;
        self.metrics.record_wal_fsync_latency(wal_ns);
        self.metrics
            .record_latency_hist(crate::metrics::LatencyMetric::WalFsync, wal_ns);
        // Slow-fsync alert: the durability window widens whenever the group
        // commit barrier stalls (disk pressure, contended fsync). Threshold is
        // read once and cached; the timing above is unconditional, so this adds
        // only a compare on the hot path.
        let slow = crate::oblog::slow_fsync_threshold();
        if fsync_elapsed >= slow && crate::oblog::enabled(crate::oblog::Level::Warn) {
            crate::log_warn!(
                "shard", self.shard_id;
                "slow group-commit fsync: {:.3}ms >= {:.3}ms threshold (raft_index={raft_index}, wal_files={})",
                fsync_elapsed.as_secs_f64() * 1e3,
                slow.as_secs_f64() * 1e3,
                touched.len()
            );
        }
        // Only now, with WAL and outbox both durable, is it safe to advance the
        // application watermark: a crash before this point re-applies the batch
        // and regenerates its (idempotent) outbox records; a crash after it
        // finds those records already present.
        if self.raft_wal_authoritative {
            let reports = emitted
                .iter()
                .map(|event| event.report.clone())
                .collect::<Vec<_>>();
            let (count, fingerprint) = proof_digest(&reports);
            crate::asset_log::mark_applied_batch(
                self.applied_root
                    .as_deref()
                    .expect("Raft authoritative matching requires an application proof dir"),
                raft_index,
                count,
                fingerprint,
            )
            .expect("persist exact Raft application result proof");
        } else if let Some(asset_journal) = &self.asset_journal {
            asset_journal
                .mark_raft_batch_applied(&touched, raft_index)
                .expect("persist Raft batch application watermarks");
        }
        // Safe rotation point: the watermark now covers everything in the
        // current outbox segment, so a sealed segment never holds records that
        // recovery would need to trim, and fully published segments can be
        // garbage collected (bounding disk growth).
        if let Some(outbox) = &mut self.execution_outbox {
            outbox
                .maybe_rotate()
                .expect("rotate execution outbox segment");
        }
        if let Some(rep) = &self.rep {
            for (seq, frame) in replicated {
                rep.publish(self.shard_id, seq, &frame);
            }
        }
        self.metrics
            .journal_seq
            .fetch_max(self.rep_seq, Ordering::Relaxed);
        for event in emitted {
            self.emit_report_event(event);
        }
        self.last_raft_index = raft_index;
        self.metrics.set_raft_applied_index(raft_index);
        let command_ns = started.elapsed().as_nanos() as u64;
        self.metrics.record_command_latency(command_ns);
        self.metrics
            .record_latency_hist(crate::metrics::LatencyMetric::Command, command_ns);
    }

    /// Match every command in a committed batch, appending each resulting
    /// execution report to the durable outbox in deterministic
    /// `(raft_index, ordinal)` order. The reports are returned rather than
    /// delivered immediately: the caller folds the outbox fsync into the batch
    /// group-commit barrier and only then delivers them downstream, so the
    /// order system never observes a fill that is not yet durable.
    ///
    /// An outbox append failure (disk full, IO error) is surfaced as an error
    /// so the caller fails the whole batch without advancing the watermark —
    /// execution events are never silently dropped.
    fn match_and_record_batch(
        &mut self,
        commands: Vec<Command>,
        raft_index: u64,
    ) -> std::io::Result<Vec<ExecutionReportEvent>> {
        let started = Instant::now();
        let metrics = &self.metrics;
        let outbox = &mut self.execution_outbox;
        let raft_group_id = self.raft_group_id;
        let mut emitted: Vec<ExecutionReportEvent> = Vec::new();
        let mut ordinal: u32 = 0;
        let mut append_error: Option<std::io::Error> = None;
        for command in commands {
            self.processor.process(command, &mut |report| {
                // Keep matching to completion even after a failure so book
                // state stays deterministic; the recorded error aborts the
                // batch before the watermark advances.
                if let Some(outbox) = outbox.as_mut() {
                    if append_error.is_none() {
                        if let Err(error) =
                            outbox.append_deferred(raft_group_id, raft_index, ordinal, &report)
                        {
                            metrics.asset_wal_errors.fetch_add(1, Ordering::Relaxed);
                            append_error = Some(error);
                        }
                    }
                }
                metrics.record(&report);
                emitted.push(ExecutionReportEvent {
                    raft_index: Some(raft_index),
                    ordinal,
                    report,
                });
                ordinal = ordinal.saturating_add(1);
            });
        }
        let match_ns = started.elapsed().as_nanos() as u64;
        self.metrics.record_match_latency(match_ns);
        self.metrics
            .record_latency_hist(crate::metrics::LatencyMetric::Match, match_ns);
        match append_error {
            Some(error) => Err(error),
            None => Ok(emitted),
        }
    }

    /// Deliver one already-durable execution report event to the order system,
    /// applying backpressure (spin+yield) until the consumer drains.
    fn emit_report_event(&self, event: ExecutionReportEvent) {
        push_result_event(&self.result_tx, &self.metrics, event);
    }
}

/// Push one execution event onto the result stream, applying backpressure when
/// the order system can't keep up.
///
/// A short spin absorbs the common case (the consumer drains within a few
/// hundred nanoseconds) without a syscall. Past the spin budget the thread
/// **parks** instead of spinning forever: a matching thread is CPU-pinned, so
/// burning the core while a downstream stall persists steals cycles from every
/// other shard sharing the socket and inflates tail latency. Each park bumps
/// `result_backpressure`, turning an otherwise invisible downstream stall into
/// an observable, alertable signal.
fn push_result_event(
    result_tx: &Producer<ExecutionReportEvent>,
    metrics: &crate::metrics::Metrics,
    mut event: ExecutionReportEvent,
) {
    // Enough spins to ride out a consumer that is scheduled on another core and
    // draining continuously; short enough that a genuine stall parks quickly.
    const SPIN_LIMIT: u32 = 128;
    let mut spins: u32 = 0;
    loop {
        match result_tx.push(event) {
            Ok(()) => return,
            Err(returned) => {
                event = returned;
                if spins < SPIN_LIMIT {
                    spins += 1;
                    std::hint::spin_loop();
                } else {
                    metrics.result_backpressure.fetch_add(1, Ordering::Relaxed);
                    thread::park_timeout(Duration::from_micros(20));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Journal replay
// ---------------------------------------------------------------------------

/// A snapshot's state copied out of the processor, so it can be written
/// after matching has moved on. Capture is kept to flat copies (each book's
/// slot pool is one memcpy, positions one table clone); filtering live
/// orders, sorting them into time priority and encoding happen in
/// [`write`](Self::write), off the matching thread in Raft mode.
struct CapturedSnapshot {
    journal_seq: u64,
    raft_applied_index: u64,
    max_cmd_id: u64,
    max_admin_id: u64,
    halted: Vec<u32>,
    suspended: Vec<u64>,
    positions: HashMap<(u64, InstrumentId), i64>,
    engines: Vec<CapturedEngine>,
    recent_command_ids: Vec<u64>,
}

struct CapturedEngine {
    instrument: InstrumentId,
    engine_seq: Timestamp,
    slots: Vec<Order>,
    stops: Vec<StopOrder>,
}

impl CapturedSnapshot {
    fn write(self, path: &Path) -> std::io::Result<()> {
        let positions: Vec<(u64, u32, i64)> = self
            .positions
            .iter()
            .filter(|(_, &quantity)| quantity != 0)
            .map(|(&(user, instrument), &quantity)| (user, instrument.0, quantity))
            .collect();
        let mut engines: Vec<EngineState> = self
            .engines
            .into_iter()
            .map(|engine| EngineState {
                instrument: engine.instrument,
                engine_seq: engine.engine_seq,
                orders: crate::book::live_orders_by_time(engine.slots),
                stops: engine.stops,
            })
            .collect();
        engines.sort_by_key(|state| state.instrument);
        snapshot::write(
            path,
            snapshot::SnapshotData {
                journal_seq: self.journal_seq,
                raft_applied_index: self.raft_applied_index,
                max_cmd_id: self.max_cmd_id,
                max_admin_id: self.max_admin_id,
                halted: &self.halted,
                suspended: &self.suspended,
                positions: &positions,
                engines: &engines,
                recent_command_ids: Some(&self.recent_command_ids),
            },
        )
    }
}

fn capture_processor_snapshot(
    processor: &Processor,
    journal_seq: u64,
    raft_applied_index: u64,
) -> CapturedSnapshot {
    CapturedSnapshot {
        journal_seq,
        raft_applied_index,
        max_cmd_id: processor.max_new_id,
        max_admin_id: processor.max_admin_id,
        halted: processor.halted.iter().map(|instrument| instrument.0).collect(),
        suspended: processor.suspended.iter().copied().collect(),
        positions: processor.positions.clone(),
        recent_command_ids: processor.seen_command_ids.to_vec(),
        engines: processor
            .engines
            .iter()
            .map(|(&instrument, engine)| CapturedEngine {
                instrument,
                engine_seq: engine.seq(),
                slots: engine.copy_slots(),
                stops: processor
                    .stops
                    .get(&instrument)
                    .map(TriggerBook::export)
                    .unwrap_or_default(),
            })
            .collect(),
    }
}

fn persist_processor_snapshot(
    processor: &Processor,
    path: &Path,
    journal_seq: u64,
    raft_applied_index: u64,
) -> std::io::Result<()> {
    capture_processor_snapshot(processor, journal_seq, raft_applied_index).write(path)
}

/// Restore a version-4 matching snapshot whose Raft index is explicit. Legacy
/// snapshot+journal deployments are upgraded once, using their already-durable
/// application watermark as the exact Raft boundary. The old files are kept
/// for rollback but are no longer appended after this succeeds.
fn recover_raft_authoritative(
    processor: &mut Processor,
    snapshot_path: &Path,
    legacy_journal_path: &Path,
    applied_root: &Path,
) -> std::io::Result<u64> {
    if snapshot_path.exists() {
        let snap = snapshot::load(snapshot_path)?;
        if snap.format_version >= 4 {
            let index = snap.raft_applied_index;
            processor.restore_state(&snap);
            return Ok(index);
        }
    }

    let has_legacy_state = snapshot_path.exists() || legacy_journal_path.exists();
    if !has_legacy_state {
        return Ok(0);
    }
    let (_, last_seq) = recover_stats(processor, snapshot_path, legacy_journal_path)?;
    let applied = crate::asset_log::load_applied_batches(applied_root)?;
    let raft_applied_index = applied.iter().max().copied().unwrap_or(0);
    if last_seq != 0 && raft_applied_index == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "legacy matching state has no durable Raft application watermark",
        ));
    }
    persist_processor_snapshot(processor, snapshot_path, 0, raft_applied_index)?;
    Ok(raft_applied_index)
}

/// The outcome of replaying one shard journal.
pub struct ReplaySummary {
    /// Commands applied.
    pub commands: u64,
    /// Every report emitted, in order.
    pub reports: Vec<ExecReport>,
    /// FNV-1a fingerprint over the encoded report stream — equal fingerprints
    /// mean identical matching results.
    pub fingerprint: u64,
    /// The rebuilt processor (books restored to post-replay state).
    pub processor: Processor,
}

/// Replay a shard journal through a fresh [`Processor`], reproducing the
/// original matching results exactly (same strategy and guard required).
///
/// `until_ts` (nanoseconds since epoch) enables point-in-time replay: records
/// after it are ignored.
pub fn replay_journal(
    path: &Path,
    strategy: StrategyFactory,
    guard: Option<Arc<PriceGuard>>,
    until_ts: Option<u64>,
) -> std::io::Result<ReplaySummary> {
    let mut processor = Processor::new(strategy, guard);
    let mut reports = Vec::new();
    let mut commands = 0u64;
    let mut seen = HashMap::new();

    for record in JournalReader::open(path)? {
        if let Some(limit) = until_ts {
            if record.ts_nanos > limit {
                break;
            }
        }
        if let Some(view) = wire::WireView::parse(&record.frame) {
            if let Some(cmd) = view.to_command() {
                if !accept_replay_command(&mut seen, &cmd, &record.frame)? {
                    continue;
                }
                processor.process(cmd, &mut |r| reports.push(r));
                commands += 1;
            }
        }
    }

    let fingerprint = fingerprint_reports(&reports);
    Ok(ReplaySummary {
        commands,
        reports,
        fingerprint,
        processor,
    })
}

/// Fast crash recovery: **snapshot + journal tail**.
///
/// Loads the snapshot (if present), then applies only the journal records not
/// yet covered by it (`seq > snapshot.journal_seq`). Recovery cost is
/// proportional to commands since the last snapshot, not since genesis.
/// Returns the number of journal records applied. Missing files are treated as
/// empty (cold start).
pub fn recover_into(
    processor: &mut Processor,
    snapshot_path: &Path,
    journal_path: &Path,
) -> std::io::Result<u64> {
    Ok(recover_stats(processor, snapshot_path, journal_path)?.0)
}

/// Like [`recover_into`], additionally returning the **highest journal seq
/// observed** (snapshot's or any record's) — the writer must resume from it so
/// the total order stays strictly increasing across restarts.
pub fn recover_stats(
    processor: &mut Processor,
    snapshot_path: &Path,
    journal_path: &Path,
) -> std::io::Result<(u64, u64)> {
    let mut skip_seq = 0;
    if snapshot_path.exists() {
        let snap = snapshot::load(snapshot_path)?;
        processor.restore_state(&snap);
        skip_seq = snap.journal_seq;
    }
    let mut applied = 0u64;
    let mut last_seq = skip_seq;
    let mut seen = HashMap::new();
    if journal_path.exists() {
        for record in JournalReader::open(journal_path)? {
            last_seq = last_seq.max(record.seq);
            if record.seq <= skip_seq {
                continue; // already covered by the snapshot
            }
            if let Some(view) = wire::WireView::parse(&record.frame) {
                if let Some(cmd) = view.to_command() {
                    if !accept_replay_command(&mut seen, &cmd, &record.frame)? {
                        continue;
                    }
                    // Recovery restores *state*; reports were already delivered
                    // to the order system before the crash, so drop them here.
                    processor.process(cmd, &mut |_| {});
                    applied += 1;
                }
            }
        }
    }
    Ok((applied, last_seq))
}

fn accept_replay_command(
    seen: &mut HashMap<u64, [u8; wire::MSG_LEN]>,
    command: &Command,
    frame: &[u8; wire::MSG_LEN],
) -> std::io::Result<bool> {
    let id = command.id();
    if id == 0 {
        return Ok(true);
    }
    match seen.get(&id) {
        None => {
            seen.insert(id, *frame);
            Ok(true)
        }
        Some(original) if original == frame => Ok(false),
        Some(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("command id {id} has conflicting journal payloads"),
        )),
    }
}

/// Order-sensitive FNV-1a fingerprint of a set of engine states, keeping only
/// the instruments for which `select` returns true. This is the shared core
/// behind [`Processor::state_fingerprint`]/[`Processor::category_fingerprint`]
/// and the durable-snapshot fingerprint in [`crate::migration`], so a value
/// computed from live processor state and one recomputed from a snapshot of
/// that same state are bit-identical. Callers must pass states in a stable
/// order (`Processor::export_state` sorts by instrument; a snapshot preserves
/// that capture order).
pub fn fingerprint_engine_states(
    states: &[EngineState],
    select: impl Fn(InstrumentId) -> bool,
) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut mix = |v: u64| {
        for b in v.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    };
    for s in states {
        if !select(s.instrument) {
            continue;
        }
        mix(s.instrument.0 as u64);
        mix(s.engine_seq);
        for o in &s.orders {
            mix(o.id.0);
            mix(o.price);
            mix(o.remaining);
            mix(o.timestamp);
            mix(o.user);
        }
        // Only mixed in when present, so state without conditional orders
        // fingerprints exactly as it did before they existed.
        for stop in &s.stops {
            mix(stop.order.id.0);
            mix(stop.order.price);
            mix(stop.order.quantity);
            mix(stop.order.user);
            mix(stop.trigger.price);
            mix(stop.trigger.fires_on_rise(stop.order.side) as u64);
        }
    }
    h
}

/// `(count, fingerprint)` of a Raft entry's result stream as recorded in and
/// checked against application proofs. [`ExecReport::MakerFilled`] is left
/// out: it is derived from the trade before it, and entries proved before it
/// existed must replay to the same digest.
pub fn proof_digest(reports: &[ExecReport]) -> (u32, u64) {
    if !reports
        .iter()
        .any(|r| matches!(r, ExecReport::MakerFilled { .. }))
    {
        return (reports.len() as u32, fingerprint_reports(reports));
    }
    let proved: Vec<ExecReport> = reports
        .iter()
        .filter(|r| !matches!(r, ExecReport::MakerFilled { .. }))
        .cloned()
        .collect();
    (proved.len() as u32, fingerprint_reports(&proved))
}

/// FNV-1a fingerprint of a report stream (order-sensitive). Two runs with equal
/// fingerprints produced identical trades, fills, rests and cancels.
///
/// Reject frames are hashed in their original layout — type `RT_REJECTED`,
/// zero `aux` — even though the wire now carries a reason code there and
/// splits off `RT_CMD_REJECTED`. Raft application proofs persist this value,
/// so a node replaying a WAL tail proved by an older build must reach the same
/// fingerprint or it refuses to start.
pub fn fingerprint_reports(reports: &[ExecReport]) -> u64 {
    let mut frame = [0u8; wire::REPORT_LEN];
    let mut h: u64 = 0xcbf29ce484222325;
    for r in reports {
        wire::encode_report(r, &mut frame);
        if frame[0] == wire::RT_REJECTED || frame[0] == wire::RT_CMD_REJECTED {
            frame[0] = wire::RT_REJECTED;
            frame[16..24].fill(0);
        }
        for &b in &frame {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}
