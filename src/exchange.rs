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
//!   OrderGateway.submit(Modify)─▶ [ high queue  ] ─┤     1. journal command (WAL)
//!   OrderGateway.submit(FClose)─▶ [ high queue  ] ─┘     2. price-guard vet
//!                                                        3. match in memory
//!   ResultSink.poll(...)     ◀─── [result queue] ◀────── 4. emit ExecReports (async)
//! ```
//!
//! * **Multi-asset**: instruments are hashed to shards; each shard owns the books
//!   for its instruments outright — the reason no locks are needed. Across
//!   machines, the same routing extends via [`crate::cluster::ClusterMap`].
//! * **Ordering & recovery**: every command is journaled in exact processing
//!   order before it touches a book; the engine is deterministic, so replaying
//!   the journal (see [`replay_journal`]) reproduces identical results. Loss
//!   window is bounded by the journal flush cadence (seconds, by design).
//! * **Cancel/modify/force-close priority**: the high-priority queue is fully
//!   drained before, and between, new orders.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use std::time::Instant;

use crate::engine::{MatchingEngine, SelfTradePolicy};
use crate::journal::{self, JournalReader, JournalWriter};
use crate::lockfree::{self, Consumer, Producer};
use crate::order::Order;
use crate::risk::{PriceGuard, RiskLimits};
use crate::snapshot::{self, EngineState, Snapshot};
use crate::strategy::MatchingStrategy;
use crate::trade::{ModifyOutcome, OrderStatus};
use crate::types::*;
use crate::wire;

/// A command from the order system to the matching side.
#[derive(Debug)]
pub enum Command {
    /// Submit a new order (low-priority queue).
    New(Order),
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

impl Command {
    /// The instrument this command targets (used for shard routing).
    pub fn instrument(&self) -> InstrumentId {
        match self {
            Command::New(o) => o.instrument,
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
        !matches!(self, Command::New(_))
    }
}

/// An asynchronous execution notification from the matching side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecReport {
    Accepted {
        instrument: InstrumentId,
        order_id: OrderId,
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
    },
    Filled {
        instrument: InstrumentId,
        order_id: OrderId,
    },
    PartiallyFilled {
        instrument: InstrumentId,
        order_id: OrderId,
        filled: Qty,
    },
    Resting {
        instrument: InstrumentId,
        order_id: OrderId,
    },
    Cancelled {
        instrument: InstrumentId,
        order_id: OrderId,
    },
    Rejected {
        instrument: InstrumentId,
        order_id: OrderId,
        reason: &'static str,
    },
    Modified {
        instrument: InstrumentId,
        order_id: OrderId,
        remaining: Qty,
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
    /// `snapshot-shard-N.bin` and truncates its journal, bounding recovery time
    /// to the commands since the last snapshot. `None` = manual only
    /// (via [`ExchangeHandle::snapshot_now`]).
    pub snapshot_every: Option<Duration>,
    /// Self-trade prevention policy applied to every book.
    pub stp: SelfTradePolicy,
    /// Static pre-trade limits (max qty / notional / per-user open orders).
    pub risk_limits: Option<RiskLimits>,
    /// Maker/taker fee schedule (default = zero fees).
    pub fees: crate::fees::FeeSchedule,
    /// Idempotent command dedup: reject any command whose id is <= the
    /// high-water mark (exactly-once under order-system re-send; cursor is
    /// snapshot-persisted). Requires globally increasing ids (Leaf).
    pub dedup_commands: bool,
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
            journal_flush: Duration::from_secs(1),
            journal_fsync: Duration::from_secs(1),
            price_guard: None,
            pin_cpus: false,
            snapshot_every: None,
            stp: SelfTradePolicy::Allow,
            risk_limits: None,
            fees: crate::fees::FeeSchedule::default(),
            dedup_commands: false,
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
    high: Producer<Command>,
    normal: Producer<Command>,
}

/// The two priority lanes dedicated to one configured instrument. A shard
/// owns many of these mailboxes, but no mailbox is shared by two instruments.
struct InstrumentRx {
    high: Consumer<Command>,
    normal: Consumer<Command>,
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
        if let Some(tx) = self.instruments.get(&cmd.instrument()) {
            return if cmd.is_high_priority() {
                tx.high.push(cmd)
            } else {
                tx.normal.push(cmd)
            };
        }
        let idx = self.shard_of(cmd.instrument());
        let tx = &self.shards[idx];
        if cmd.is_high_priority() {
            tx.high.push(cmd)
        } else {
            tx.normal.push(cmd)
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
    results: Vec<Consumer<ExecReport>>,
}

impl ResultSink {
    /// Drain every currently-available report across all shards, invoking `f`
    /// for each. Non-blocking; returns the number of reports delivered.
    pub fn poll(&self, mut f: impl FnMut(ExecReport)) -> usize {
        let mut count = 0;
        for rx in &self.results {
            while let Some(r) = rx.pop() {
                f(r);
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

    /// Ask every shard to take a snapshot (and truncate its journal) at its
    /// next opportunity. No-op for shards without journal/snapshot config.
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
        let (high_tx, high_rx) = lockfree::channel::<Command>(config.queue_capacity);
        let (normal_tx, normal_rx) = lockfree::channel::<Command>(config.queue_capacity);
        let (res_tx, res_rx) = lockfree::channel::<ExecReport>(config.queue_capacity);

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
                    lockfree::channel::<Command>(config.queue_capacity);
                let (inst_normal_tx, inst_normal_rx) =
                    lockfree::channel::<Command>(config.queue_capacity);
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
            .with_dedup(config.dedup_commands)
            .with_fees(config.fees);
        for &inst in &config.instruments {
            if inst.0 as usize % config.shards == shard_id {
                processor.create_book(inst, config.pool_orders_per_book, config.prefault);
            }
        }

        // Journal + snapshot + fsync thread for this shard. On startup, any
        // existing snapshot + journal is recovered into the processor first.
        let mut journal_w = None;
        let mut snapshot_path = None;
        if let Some(dir) = &config.journal_dir {
            std::fs::create_dir_all(dir).expect("create journal dir");
            let jpath = dir.join(format!("journal-shard-{shard_id}.bin"));
            let spath = dir.join(format!("snapshot-shard-{shard_id}.bin"));
            let (_, last_seq) =
                recover_stats(&mut processor, &spath, &jpath).expect("recover shard state");
            let mut w = JournalWriter::open(&jpath, config.journal_flush).expect("open journal");
            // The journal seq IS the total order: continue it, never restart it.
            w.resume_from(last_seq);
            if let Ok(fh) = w.file_handle() {
                journal::spawn_fsyncer(fh, config.journal_fsync, running.clone());
            }
            journal_w = Some(w);
            snapshot_path = Some(spath);
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
            snapshot_path,
            snapshot_every: config.snapshot_every,
            last_snapshot: Instant::now(),
            snap_request,
            running: running.clone(),
            started: started.clone(),
            parked: parked.clone(),
            default_pool: (config.pool_orders_per_book, config.prefault),
            pin_core: config.pin_cpus.then_some(shard_id),
            last_depth: Instant::now(),
        };
        threads.push(
            thread::Builder::new()
                .name(format!("match-shard-{shard_id}"))
                .spawn(move || shard.run())
                .expect("spawn shard"),
        );
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
    /// Idempotency: highest command id applied; duplicates are rejected.
    /// 0-ids bypass (legacy/tests). Persisted in snapshots (exactly-once
    /// across crash + order-system re-send).
    dedup: bool,
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
            dedup: false,
            max_new_id: 0,
            max_admin_id: 0,
            fees: crate::fees::FeeSchedule::default(),
            halted: std::collections::HashSet::new(),
            suspended: std::collections::HashSet::new(),
            positions: HashMap::new(),
            trades_buf: Vec::new(),
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
        for e in &snap.engines {
            let engine = self.engine_for(e.instrument);
            engine.restore(e.engine_seq, &e.orders);
        }
    }

    /// Order-sensitive fingerprint of the complete matching state (books +
    /// sequence counters). Equal fingerprints = identical state.
    pub fn state_fingerprint(&self) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        let mut mix = |v: u64| {
            for b in v.to_le_bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
        };
        for s in self.export_state() {
            mix(s.instrument.0 as u64);
            mix(s.engine_seq);
            for o in &s.orders {
                mix(o.id.0);
                mix(o.price);
                mix(o.remaining);
                mix(o.timestamp);
                mix(o.user);
            }
        }
        h
    }

    /// Apply one command, emitting every resulting report through `emit`.
    pub fn process(&mut self, cmd: Command, emit: &mut dyn FnMut(ExecReport)) {
        // Idempotency gate: every command carries a unique increasing id (New =
        // order id; Cancel/Modify = cmd_id; ForceClose = close-order id). A
        // re-sent duplicate (id <= high-water mark) is rejected, never applied
        // twice. Deterministic under replay: duplicates are journaled too, and
        // the same gate skips them again.
        if self.dedup {
            // (id, instrument, is_admin_stream)
            let (id, inst, admin) = match &cmd {
                Command::New(o) => (o.id.0, o.instrument, false),
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
                    emit(ExecReport::Rejected {
                        instrument: inst,
                        order_id: OrderId(id),
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
            Command::New(order) => self.process_new(order, emit),
            Command::Cancel {
                instrument,
                order_id,
                ..
            } => {
                if self.engine_for(instrument).cancel(order_id) {
                    emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
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
                ..
            } => {
                match self
                    .engine_for(instrument)
                    .modify(order_id, new_price, new_qty)
                {
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
                    }),
                    ModifyOutcome::Cancelled { order_id } => emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
                    }),
                    ModifyOutcome::Requoted(report) => {
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
                            });
                        }
                        let remaining = new_qty.saturating_sub(report.filled);
                        match report.status {
                            OrderStatus::Filled => emit(ExecReport::Filled {
                                instrument,
                                order_id,
                            }),
                            OrderStatus::Resting | OrderStatus::PartiallyFilled => {
                                emit(ExecReport::Modified {
                                    instrument,
                                    order_id,
                                    remaining,
                                })
                            }
                            _ => emit(ExecReport::Cancelled {
                                instrument,
                                order_id,
                            }),
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
                // 1. Pull every resting order of the user.
                let cancelled = self.engine_for(instrument).cancel_all_for_user(user);
                for order_id in cancelled {
                    emit(ExecReport::Cancelled {
                        instrument,
                        order_id,
                    });
                }
                // 2. Flatten the position with a protected market order.
                if close_qty > 0 {
                    let mut close = Order::market(close_order_id, close_side, close_qty)
                        .on(instrument)
                        .by(user);
                    close.tif = TimeInForce::Ioc;
                    self.process_new(close, emit);
                }
            }
        }
    }

    fn process_new(&mut self, mut order: Order, emit: &mut dyn FnMut(ExecReport)) {
        let instrument = order.instrument;
        let id = order.id;

        // Circuit breaker: a halted instrument accepts no new orders (cancels
        // and modifies still work so users can pull quotes).
        if self.halted.contains(&instrument) {
            emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                reason: "halted",
            });
            return;
        }
        if order.user != 0 && self.suspended.contains(&order.user) {
            emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                reason: "user-suspended",
            });
            return;
        }

        // Synchronous pre-trade risk, cheapest checks first:
        // 1. static limits (order shape);
        if let Some(limits) = &self.limits {
            if let Err(reason) = limits.check_static(&order) {
                emit(ExecReport::Rejected {
                    instrument,
                    order_id: id,
                    reason,
                });
                return;
            }
        }
        // 2. anti-spike price banding (may convert market orders into
        //    protected marketable limits);
        if let Some(guard) = &self.guard {
            if let Err(reason) = guard.vet(&mut order) {
                emit(ExecReport::Rejected {
                    instrument,
                    order_id: id,
                    reason,
                });
                return;
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
                        reason: "max-user-orders",
                    });
                    return;
                }
            }
        }

        emit(ExecReport::Accepted {
            instrument,
            order_id: id,
        });
        // Zero-allocation submit: trades land in the processor's scratch buffer.
        let mut trades = std::mem::take(&mut self.trades_buf);
        trades.clear();
        let (_, status, filled, _) = self.engine_for(instrument).submit_into(order, &mut trades);
        // Report makers cancelled by self-trade prevention, if any. Skipped
        // entirely under `Allow` — no extra lookup on the default hot path.
        if self.stp != SelfTradePolicy::Allow {
            let engine = self.engines.get(&instrument).expect("engine exists");
            for &order_id in engine.stp_cancelled() {
                emit(ExecReport::Cancelled {
                    instrument,
                    order_id,
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
            });
        }
        self.trades_buf = trades;
        match status {
            OrderStatus::Filled => emit(ExecReport::Filled {
                instrument,
                order_id: id,
            }),
            OrderStatus::PartiallyFilled => emit(ExecReport::PartiallyFilled {
                instrument,
                order_id: id,
                filled,
            }),
            OrderStatus::Resting => emit(ExecReport::Resting {
                instrument,
                order_id: id,
            }),
            OrderStatus::Cancelled => emit(ExecReport::Cancelled {
                instrument,
                order_id: id,
            }),
            OrderStatus::Rejected => emit(ExecReport::Rejected {
                instrument,
                order_id: id,
                reason: "unfillable",
            }),
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
    high_rx: Consumer<Command>,
    normal_rx: Consumer<Command>,
    instrument_rx: Vec<InstrumentRx>,
    next_instrument: usize,
    result_tx: Producer<ExecReport>,
    journal: Option<JournalWriter>,
    snapshot_path: Option<PathBuf>,
    snapshot_every: Option<Duration>,
    last_snapshot: Instant,
    snap_request: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    parked: Arc<AtomicUsize>,
    default_pool: (usize, bool),
    pin_core: Option<usize>,
    last_depth: Instant,
}

/// Depth-of-market publish cadence and ladder size.
const DEPTH_EVERY: Duration = Duration::from_millis(200);
const DEPTH_LEVELS: usize = 5;

impl Shard {
    fn run(&mut self) {
        self.processor.default_pool = self.default_pool;
        if let Some(core) = self.pin_core {
            if let Err(reason) = crate::affinity::pin_current_thread(core) {
                eprintln!("[shard] CPU pin to core {core} not applied: {reason}");
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
                thread::yield_now();
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
                // Under sustained load, still publish depth on cadence.
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
                thread::yield_now();
            }
        }
        // Final snapshot on clean shutdown makes the next start instant — but
        // only when periodic snapshotting is enabled: journal-only deployments
        // keep their full journal (e.g. for audit/time replay).
        if self.snapshot_every.is_some() {
            self.take_snapshot();
        }
        if let Some(j) = &mut self.journal {
            let _ = j.flush();
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
        self.last_depth = Instant::now();
        // Collect first (immutable borrow), then emit.
        let mut out: Vec<ExecReport> = Vec::new();
        for (&instrument, engine) in self.processor.engines.iter() {
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
        for r in out {
            self.emit_report(r);
        }
    }

    fn emit_report(&self, report: ExecReport) {
        let mut pending = report;
        loop {
            match self.result_tx.push(pending) {
                Ok(()) => return,
                Err(returned) => {
                    pending = returned;
                    thread::yield_now();
                }
            }
        }
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

    /// Capture state, persist it atomically, then truncate the journal. Safe
    /// against a crash at any point in between (recovery skips journal records
    /// already covered by the snapshot's sequence number).
    fn take_snapshot(&mut self) {
        let (Some(path), Some(j)) = (self.snapshot_path.as_ref(), self.journal.as_mut()) else {
            return;
        };
        if j.flush().is_err() {
            return;
        }
        let states = self.processor.export_state();
        let halted: Vec<u32> = self.processor.halted.iter().map(|i| i.0).collect();
        let suspended: Vec<u64> = self.processor.suspended.iter().copied().collect();
        let positions: Vec<(u64, u32, i64)> = self
            .processor
            .positions
            .iter()
            .filter(|(_, &q)| q != 0)
            .map(|(&(u, i), &q)| (u, i.0, q))
            .collect();
        if snapshot::write(
            path,
            snapshot::SnapshotData {
                journal_seq: j.seq(),
                max_cmd_id: self.processor.max_new_id,
                max_admin_id: self.processor.max_admin_id,
                halted: &halted,
                suspended: &suspended,
                positions: &positions,
                engines: &states,
            },
        )
        .is_ok()
        {
            let _ = j.truncate();
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
    fn pop_next_normal(&mut self) -> Option<Command> {
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
    fn handle(&mut self, cmd: Command) {
        // Batches flatten HERE: each inner command is journaled/replicated as
        // its own record (the total order stays flat and replayable); atomicity
        // holds because this thread pops nothing until the loop finishes.
        if let Command::Batch(cmds) = cmd {
            for c in cmds {
                self.handle(c);
            }
            return;
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
            if let Some(rep) = &self.rep {
                rep.publish(self.shard_id, seq, &frame);
            }
        }
        if self.rep_seq > 0 {
            self.metrics
                .journal_seq
                .fetch_max(self.rep_seq, Ordering::Relaxed);
        }
        let result_tx = &self.result_tx;
        let metrics = &self.metrics;
        self.processor.process(cmd, &mut |report| {
            metrics.record(&report);
            // Backpressure: spin+yield until the order system drains.
            let mut pending = report;
            loop {
                match result_tx.push(pending) {
                    Ok(()) => return,
                    Err(returned) => {
                        pending = returned;
                        thread::yield_now();
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Journal replay
// ---------------------------------------------------------------------------

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

    for record in JournalReader::open(path)? {
        if let Some(limit) = until_ts {
            if record.ts_nanos > limit {
                break;
            }
        }
        if let Some(view) = wire::WireView::parse(&record.frame) {
            if let Some(cmd) = view.to_command() {
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
    if journal_path.exists() {
        for record in JournalReader::open(journal_path)? {
            last_seq = last_seq.max(record.seq);
            if record.seq <= skip_seq {
                continue; // already covered by the snapshot
            }
            if let Some(view) = wire::WireView::parse(&record.frame) {
                if let Some(cmd) = view.to_command() {
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

/// FNV-1a fingerprint of a report stream (order-sensitive). Two runs with equal
/// fingerprints produced identical trades, fills, rests and cancels.
pub fn fingerprint_reports(reports: &[ExecReport]) -> u64 {
    let mut frame = [0u8; wire::REPORT_LEN];
    let mut h: u64 = 0xcbf29ce484222325;
    for r in reports {
        wire::encode_report(r, &mut frame);
        for &b in &frame {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}
