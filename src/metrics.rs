//! Operational metrics, Prometheus text exposition — counters incremented at
//! the shard's single emit point (lock-free atomics; ~1 ns per report).
//!
//! # Latency histograms
//!
//! The `*_ns_total`/`*_ns_max`/`*_samples` triples cannot answer the p99 SLO
//! questions the runbooks are written against (a total and a max give you the
//! mean and the worst case, never a tail percentile). Alongside each triple we
//! keep a parallel [`LatencyHistogram`]: fixed logarithmic bucket boundaries at
//! `{1..9} x 10^k` nanoseconds (`MIN_EXP..=MAX_EXP`, i.e. 1µs to 90s), 9 buckets
//! per decade, plus a `+Inf` overflow bucket — 73 `AtomicU64` counters total.
//!
//! Design tradeoffs:
//!
//! * **Fixed buckets, no dependency.** A real HDR histogram would give tighter
//!   tails, but the house rule is no new crates. Log buckets keep relative error
//!   bounded (~10–25% within a decade) at trivial cost, which is enough to alarm
//!   on a p99 SLO. We deliberately do *not* pull in `hdrhistogram`.
//! * **O(1), branch-cheap record.** `record` computes the bucket with one
//!   `ilog10` and a few integer ops (no loop over boundaries), then two relaxed
//!   `fetch_add`s. The scrape-time `render`/`quantile` paths may walk buckets and
//!   do floating-point interpolation; scrape frequency is low so that is free.
//! * **Quantiles by interpolation.** `render` emits Prometheus histogram series
//!   (`_bucket{le=...}`/`_sum`/`_count`) plus estimated `p50`/`p90`/`p99` gauges
//!   computed by linear interpolation within the containing bucket, so accuracy
//!   is bounded by bucket width.
//!
//! # Label dimension
//!
//! [`LabeledHistogram`] fans a family out across a *pre-registered, fixed* set
//! of label values (e.g. one per Raft group), rendered as
//! `tc_raft_commit_ns_bucket{group="3",le="..."}`. Registration happens once at
//! startup; the hot path addresses a series by integer index, so there is no
//! runtime `HashMap` insertion and no lock. All added capability is additive —
//! the existing public API and its `render` output are unchanged.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::exchange::ExecReport;

#[derive(Default)]
pub struct Metrics {
    pub orders_accepted: AtomicU64,
    pub trades: AtomicU64,
    pub volume: AtomicU64,
    pub cancels: AtomicU64,
    pub rejects: AtomicU64,
    pub modifies: AtomicU64,
    pub journal_seq: AtomicU64,
    /// Readiness is controlled by the process lifecycle (and, in clustered
    /// mode, by the Raft runtime once it has joined a quorum).
    ready: AtomicBool,
    pub raft_role: AtomicU64,
    pub raft_term: AtomicU64,
    pub raft_leader_id: AtomicU64,
    pub raft_commit_index: AtomicU64,
    pub raft_enqueued_index: AtomicU64,
    pub raft_applied_index: AtomicU64,
    pub raft_transport_reconnects: AtomicU64,
    pub raft_transport_dropped: AtomicU64,
    pub command_latency_ns_total: AtomicU64,
    pub command_latency_ns_max: AtomicU64,
    pub command_latency_samples: AtomicU64,
    pub asset_wal_errors: AtomicU64,
    pub raft_commit_ns_total: AtomicU64,
    pub raft_commit_ns_max: AtomicU64,
    pub raft_commit_samples: AtomicU64,
    pub wal_fsync_ns_total: AtomicU64,
    pub wal_fsync_ns_max: AtomicU64,
    pub wal_fsync_samples: AtomicU64,
    pub match_ns_total: AtomicU64,
    pub match_ns_max: AtomicU64,
    pub match_samples: AtomicU64,
    pub execution_outbox_pending: AtomicU64,
    pub execution_outbox_published: AtomicU64,
    pub execution_outbox_publish_failures: AtomicU64,
    pub execution_outbox_publish_healthy: AtomicU64,
    pub execution_kafka_publish_ns_total: AtomicU64,
    pub execution_kafka_publish_ns_max: AtomicU64,
    pub execution_kafka_publish_samples: AtomicU64,
    /// Log-bucketed latency distributions running in parallel with the
    /// `*_ns_total`/`*_ns_max`/`*_samples` triples above. The triples are kept
    /// for backward compatibility; these back the p50/p90/p99 SLO gauges.
    /// Populated via [`Metrics::record_latency_hist`] (wired in later).
    latency_hists: LatencyHistograms,
    /// Optional per-Raft-group commit-latency histogram. Pre-registered once at
    /// startup via [`Metrics::register_raft_commit_groups`]; addressed by group
    /// index on the hot path (no runtime map insertion). Demonstrates the label
    /// dimension (`tc_raft_commit_ns_bucket{group="3",le="..."}`).
    raft_commit_by_group: OnceLock<LabeledHistogram>,
}

impl Metrics {
    /// Tally one execution report (called from the shard emit path).
    #[inline]
    pub fn record(&self, r: &ExecReport) {
        match r {
            ExecReport::Accepted { .. } => {
                self.orders_accepted.fetch_add(1, Ordering::Relaxed);
            }
            ExecReport::Trade { qty, .. } => {
                self.trades.fetch_add(1, Ordering::Relaxed);
                self.volume.fetch_add(*qty, Ordering::Relaxed);
            }
            ExecReport::Cancelled { .. } => {
                self.cancels.fetch_add(1, Ordering::Relaxed);
            }
            ExecReport::Rejected { .. } => {
                self.rejects.fetch_add(1, Ordering::Relaxed);
            }
            ExecReport::Modified { .. } => {
                self.modifies.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Prometheus text-format exposition.
    pub fn render(&self) -> String {
        let c = |n: &str, h: &str, v: u64| {
            format!("# HELP tc_{n} {h}\n# TYPE tc_{n} counter\ntc_{n} {v}\n")
        };
        let mut out = [
            c("orders_accepted", "Orders accepted", self.orders_accepted.load(Ordering::Relaxed)),
            c("trades", "Trades printed", self.trades.load(Ordering::Relaxed)),
            c("volume", "Lots traded", self.volume.load(Ordering::Relaxed)),
            c("cancels", "Orders cancelled", self.cancels.load(Ordering::Relaxed)),
            c("rejects", "Orders rejected", self.rejects.load(Ordering::Relaxed)),
            c("modifies", "Orders modified", self.modifies.load(Ordering::Relaxed)),
            c("journal_seq", "Max journal seq (total order head)",
              self.journal_seq.load(Ordering::Relaxed)),
            format!("# HELP tc_ready Service readiness (1 = ready)\n# TYPE tc_ready gauge\ntc_ready {}\n",
                self.ready.load(Ordering::Relaxed) as u8),
            format!("# HELP tc_raft_role Raft role (0 = follower, 1 = candidate, 2 = leader)\n# TYPE tc_raft_role gauge\ntc_raft_role {}\n",
                self.raft_role.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_term Current Raft term\n# TYPE tc_raft_term gauge\ntc_raft_term {}\n",
                self.raft_term.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_leader_id Current Raft leader id (0 = unknown)\n# TYPE tc_raft_leader_id gauge\ntc_raft_leader_id {}\n",
                self.raft_leader_id.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_commit_index Highest Raft entry committed by quorum\n# TYPE tc_raft_commit_index gauge\ntc_raft_commit_index {}\n",
                self.raft_commit_index.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_enqueued_index Highest committed command entry admitted to matching\n# TYPE tc_raft_enqueued_index gauge\ntc_raft_enqueued_index {}\n",
                self.raft_enqueued_index.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_applied_index Highest committed command entry fully applied by matching\n# TYPE tc_raft_applied_index gauge\ntc_raft_applied_index {}\n",
                self.raft_applied_index.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_apply_lag Committed command entries queued but not fully applied\n# TYPE tc_raft_apply_lag gauge\ntc_raft_apply_lag {}\n",
                self.raft_apply_lag()),
            c("raft_transport_reconnects", "Raft peer transport TCP reconnects", self.raft_transport_reconnects.load(Ordering::Relaxed)),
            c("raft_transport_dropped", "Raft peer messages dropped after bounded transport retries or queue saturation", self.raft_transport_dropped.load(Ordering::Relaxed)),
            c("command_latency_ns_total", "Total matching command processing time in nanoseconds", self.command_latency_ns_total.load(Ordering::Relaxed)),
            c("command_latency_samples", "Matching command latency samples", self.command_latency_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_command_latency_ns_max Maximum matching command processing time in nanoseconds\n# TYPE tc_command_latency_ns_max gauge\ntc_command_latency_ns_max {}\n", self.command_latency_ns_max.load(Ordering::Relaxed)),
            c("asset_wal_errors", "Command/application durability failures", self.asset_wal_errors.load(Ordering::Relaxed)),
            c("raft_commit_ns_total", "Total Raft quorum commit time in nanoseconds", self.raft_commit_ns_total.load(Ordering::Relaxed)),
            c("raft_commit_samples", "Raft quorum commit batches", self.raft_commit_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_commit_ns_max Maximum Raft quorum commit time in nanoseconds\n# TYPE tc_raft_commit_ns_max gauge\ntc_raft_commit_ns_max {}\n", self.raft_commit_ns_max.load(Ordering::Relaxed)),
            c("wal_fsync_ns_total", "Total post-match durability barrier time in nanoseconds", self.wal_fsync_ns_total.load(Ordering::Relaxed)),
            c("wal_fsync_samples", "Post-match durability barrier batches", self.wal_fsync_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_wal_fsync_ns_max Maximum post-match durability barrier time in nanoseconds\n# TYPE tc_wal_fsync_ns_max gauge\ntc_wal_fsync_ns_max {}\n", self.wal_fsync_ns_max.load(Ordering::Relaxed)),
            c("match_ns_total", "Total in-memory matching time in nanoseconds", self.match_ns_total.load(Ordering::Relaxed)),
            c("match_samples", "Commands matched", self.match_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_match_ns_max Maximum in-memory matching time in nanoseconds\n# TYPE tc_match_ns_max gauge\ntc_match_ns_max {}\n", self.match_ns_max.load(Ordering::Relaxed)),
            format!("# HELP tc_execution_outbox_pending Execution events durably recorded but not broker-acknowledged\n# TYPE tc_execution_outbox_pending gauge\ntc_execution_outbox_pending {}\n", self.execution_outbox_pending.load(Ordering::Relaxed)),
            c("execution_outbox_published", "Execution events acknowledged by Kafka", self.execution_outbox_published.load(Ordering::Relaxed)),
            c("execution_outbox_publish_failures", "Execution outbox batches rejected or not acknowledged by Kafka", self.execution_outbox_publish_failures.load(Ordering::Relaxed)),
            format!("# HELP tc_execution_outbox_publish_healthy Execution publisher health (1 = initialized and last batch acknowledged)\n# TYPE tc_execution_outbox_publish_healthy gauge\ntc_execution_outbox_publish_healthy {}\n", self.execution_outbox_publish_healthy.load(Ordering::Relaxed)),
            c("execution_kafka_publish_ns_total", "Total execution Kafka publish acknowledgement latency in nanoseconds", self.execution_kafka_publish_ns_total.load(Ordering::Relaxed)),
            c("execution_kafka_publish_samples", "Execution Kafka publish batches acknowledged", self.execution_kafka_publish_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_execution_kafka_publish_ns_max Maximum execution Kafka publish acknowledgement latency in nanoseconds\n# TYPE tc_execution_kafka_publish_ns_max gauge\ntc_execution_kafka_publish_ns_max {}\n", self.execution_kafka_publish_ns_max.load(Ordering::Relaxed)),
        ]
        .concat();
        // Append the parallel latency histograms and their derived quantile
        // gauges. Render cost is irrelevant (scrape frequency is low).
        self.latency_hists.render_into(&mut out);
        if let Some(g) = self.raft_commit_by_group.get() {
            g.render_into(&mut out);
        }
        out
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Update the Raft gauges from the runtime's single event-loop thread.
    pub fn set_raft_state(&self, role: u64, term: u64, leader_id: u64, commit_index: u64) {
        self.raft_role.store(role, Ordering::Relaxed);
        self.raft_term.store(term, Ordering::Relaxed);
        self.raft_leader_id.store(leader_id, Ordering::Relaxed);
        self.raft_commit_index
            .store(commit_index, Ordering::Relaxed);
    }

    pub fn set_raft_enqueued_index(&self, index: u64) {
        self.raft_enqueued_index.fetch_max(index, Ordering::Release);
    }

    pub fn set_raft_applied_index(&self, index: u64) {
        self.raft_applied_index.fetch_max(index, Ordering::Release);
    }

    pub fn raft_apply_lag(&self) -> u64 {
        self.raft_enqueued_index
            .load(Ordering::Acquire)
            .saturating_sub(self.raft_applied_index.load(Ordering::Acquire))
    }

    pub fn record_command_latency(&self, elapsed_ns: u64) {
        self.command_latency_ns_total
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.command_latency_ns_max
            .fetch_max(elapsed_ns, Ordering::Relaxed);
        self.command_latency_samples.fetch_add(1, Ordering::Relaxed);
    }

    fn record_stage(total: &AtomicU64, max: &AtomicU64, samples: &AtomicU64, elapsed_ns: u64) {
        total.fetch_add(elapsed_ns, Ordering::Relaxed);
        max.fetch_max(elapsed_ns, Ordering::Relaxed);
        samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_raft_commit_latency(&self, elapsed_ns: u64) {
        Self::record_stage(
            &self.raft_commit_ns_total,
            &self.raft_commit_ns_max,
            &self.raft_commit_samples,
            elapsed_ns,
        );
    }

    pub fn record_wal_fsync_latency(&self, elapsed_ns: u64) {
        Self::record_stage(
            &self.wal_fsync_ns_total,
            &self.wal_fsync_ns_max,
            &self.wal_fsync_samples,
            elapsed_ns,
        );
    }

    pub fn record_match_latency(&self, elapsed_ns: u64) {
        Self::record_stage(
            &self.match_ns_total,
            &self.match_ns_max,
            &self.match_samples,
            elapsed_ns,
        );
    }

    /// Record a latency sample into the parallel log-bucketed histogram for
    /// `metric`. O(1), lock-free, no branch penalty on the hot path (a single
    /// `ilog10`, a few integer ops, and two relaxed `fetch_add`s). Wired in
    /// alongside the existing `record_*_latency` calls in a later stage.
    #[inline]
    pub fn record_latency_hist(&self, metric: LatencyMetric, elapsed_ns: u64) {
        self.latency_hists.hists[metric as usize].record(elapsed_ns);
    }

    /// Pre-register the per-group Raft commit-latency histogram at startup, one
    /// series per group. Fixed capacity: the hot path then addresses groups by
    /// index with no map insertion. Idempotent — a second call is a no-op.
    pub fn register_raft_commit_groups(&self, group_count: usize) {
        let labels = (0..group_count).map(|g| g.to_string()).collect();
        let _ = self.raft_commit_by_group.set(LabeledHistogram::new(
            "raft_commit_ns",
            "Raft quorum commit latency by group (nanoseconds)",
            "group",
            labels,
        ));
    }

    /// Record a Raft commit-latency sample for `group` (index into the set
    /// registered by [`register_raft_commit_groups`]). O(1); silently ignored
    /// if groups were never registered or `group` is out of range.
    #[inline]
    pub fn record_raft_commit_latency_group(&self, group: usize, elapsed_ns: u64) {
        if let Some(h) = self.raft_commit_by_group.get() {
            h.record_at(group, elapsed_ns);
        }
    }
}

// ===========================================================================
// Log-bucketed latency histograms
// ===========================================================================
//
// Design (see module docs): fixed logarithmic bucket boundaries at
// `{1,2,..,9} x 10^k` nanoseconds for k in `MIN_EXP..=MAX_EXP`, i.e. 1µs up to
// 9e10 ns (90s), 9 buckets per decade, plus a final `+Inf` overflow bucket.
// That is 8 decades x 9 = 72 finite buckets + 1 = 73 `AtomicU64` counters,
// matching Prometheus cumulative-histogram semantics on render.
//
// `record` is O(1) and branch-cheap: one `ilog10`, a handful of integer ops,
// then two relaxed `fetch_add`s — no loop over bucket boundaries. `render` and
// quantile estimation walk the buckets but run only at scrape time.

/// Lowest bucketed decade exponent: `10^3 ns = 1µs`.
const MIN_EXP: u32 = 3;
/// Highest bucketed decade exponent: `10^10 ns` (finite boundaries reach
/// `9 x 10^10 ns = 90s`; larger samples fall into `+Inf`).
const MAX_EXP: u32 = 10;
/// Buckets per decade: leading digits `1..=9`.
const PER_DECADE: usize = 9;
/// Number of finite `le` buckets.
const FINITE_BUCKETS: usize = ((MAX_EXP - MIN_EXP + 1) as usize) * PER_DECADE;
/// Total counters including the trailing `+Inf` bucket.
const BUCKET_COUNT: usize = FINITE_BUCKETS + 1;

/// `10^i` for `i` up to 19 (covers every exponent we index, including the
/// `digit == 10` rollover to `MAX_EXP + 1`).
const POW10: [u64; 20] = {
    let mut t = [1u64; 20];
    let mut i = 1;
    while i < 20 {
        t[i] = t[i - 1] * 10;
        i += 1;
    }
    t
};

/// Upper (`le`) boundary in nanoseconds for finite bucket `i` (`0..FINITE_BUCKETS`).
#[inline]
fn bucket_le(i: usize) -> u64 {
    let digit = (i % PER_DECADE + 1) as u64;
    let exp = MIN_EXP as usize + i / PER_DECADE;
    digit * POW10[exp]
}

/// Map a nanosecond sample to its bucket index (`0..=FINITE_BUCKETS`, the last
/// being `+Inf`). O(1), no boundary loop.
#[inline]
fn bucket_index(v: u64) -> usize {
    // Everything at or below the first boundary (1µs) lands in bucket 0; also
    // guards `ilog10(0)`, which would panic.
    if v < 1000 {
        return 0;
    }
    let exp = v.ilog10(); // >= 3
    if exp > MAX_EXP {
        return FINITE_BUCKETS; // >= 1e11 ns → +Inf
    }
    let pow = POW10[exp as usize];
    let m = v / pow; // leading digit, 1..=9
    let has_rem = (v % pow != 0) as u64;
    let digit = m + has_rem; // 1..=10; a nonzero remainder rounds up to the next `le`
    let (exp, digit) = if digit == 10 {
        (exp + 1, 1) // carry into the next decade
    } else {
        (exp, digit)
    };
    let idx = (exp - MIN_EXP) as usize * PER_DECADE + (digit as usize - 1);
    idx.min(FINITE_BUCKETS)
}

/// A single lock-free log-bucketed latency distribution.
pub struct LatencyHistogram {
    buckets: [AtomicU64; BUCKET_COUNT],
    /// Sum of all recorded nanoseconds (for the `_sum` series and the mean).
    sum_ns: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one nanosecond latency sample. O(1), lock-free.
    #[inline]
    pub fn record(&self, ns: u64) {
        self.buckets[bucket_index(ns)].fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Snapshot the raw (non-cumulative) bucket counts.
    fn snapshot(&self) -> [u64; BUCKET_COUNT] {
        std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed))
    }

    fn count(counts: &[u64; BUCKET_COUNT]) -> u64 {
        counts.iter().sum()
    }

    /// Estimate the `q`-quantile (0.0..=1.0) in nanoseconds via linear
    /// interpolation within the bucket that contains the target rank. Accuracy
    /// is bounded by bucket width. Returns 0 when empty.
    fn quantile_from(counts: &[u64; BUCKET_COUNT], q: f64) -> u64 {
        let total = Self::count(counts);
        if total == 0 {
            return 0;
        }
        let rank = q * total as f64;
        let mut cum: u64 = 0;
        for i in 0..BUCKET_COUNT {
            let c = counts[i];
            let cum_before = cum;
            cum += c;
            if cum as f64 >= rank && c > 0 {
                if i >= FINITE_BUCKETS {
                    // +Inf bucket: unbounded above, report the last finite edge.
                    return bucket_le(FINITE_BUCKETS - 1);
                }
                let lower = if i == 0 { 0.0 } else { bucket_le(i - 1) as f64 };
                let upper = bucket_le(i) as f64;
                let frac = (rank - cum_before as f64) / c as f64;
                return (lower + frac * (upper - lower)).round() as u64;
            }
        }
        bucket_le(FINITE_BUCKETS - 1)
    }

    /// Public quantile estimate (nanoseconds); snapshots then interpolates.
    pub fn quantile(&self, q: f64) -> u64 {
        Self::quantile_from(&self.snapshot(), q)
    }

    /// Render this histogram as a self-contained Prometheus block under `base`
    /// (metric name without the `tc_` prefix): `# HELP`/`# TYPE` headers, the
    /// cumulative `_bucket`/`_sum`/`_count` series, and the derived
    /// `p50`/`p90`/`p99` gauges. For callers outside this module that keep their
    /// own [`LatencyHistogram`] (e.g. the order API's MySQL-commit latency) and
    /// splice the block into their own exposition text.
    pub fn render_standalone(&self, base: &str, help: &str) -> String {
        let mut out = String::new();
        let _ = write!(out, "# HELP tc_{base} {help}\n# TYPE tc_{base} histogram\n");
        self.render_into(&mut out, base, None);
        for suffix in ["p50", "p90", "p99"] {
            let _ = write!(out, "# TYPE tc_{base}_{suffix} gauge\n");
        }
        out
    }

    /// Render Prometheus histogram series plus derived p50/p90/p99 gauges.
    /// `base` is the metric base name (without the `tc_` prefix), e.g.
    /// `command_latency_ns`. When `extra_label` is set, every bucket line and
    /// gauge carries it (used for the `{group="..."}` dimension).
    fn render_into(&self, out: &mut String, base: &str, extra_label: Option<&str>) {
        let counts = self.snapshot();
        let total = Self::count(&counts);
        let sum_ns = self.sum_ns.load(Ordering::Relaxed);
        // Comment lines are emitted once per base name by the caller for the
        // labeled case; here we always emit them (harmless duplication is
        // avoided by the labeled renderer, which prints them itself).
        // Prometheus convention places `le` last, so the extra dimension is a
        // prefix on bucket lines: `{group="3",le="..."}`. Gauges just carry the
        // dimension: `{group="3"}`.
        let (bucket_prefix, gauge_extra) = match extra_label {
            Some(l) => (format!("{l},"), format!("{{{l}}}")),
            None => (String::new(), String::new()),
        };
        let mut cum: u64 = 0;
        for i in 0..FINITE_BUCKETS {
            cum += counts[i];
            let _ = write!(
                out,
                "tc_{base}_bucket{{{}le=\"{}\"}} {}\n",
                bucket_prefix,
                bucket_le(i),
                cum
            );
        }
        cum += counts[FINITE_BUCKETS];
        let _ = write!(
            out,
            "tc_{base}_bucket{{{}le=\"+Inf\"}} {}\n",
            bucket_prefix, cum
        );
        let _ = write!(out, "tc_{base}_sum{} {}\n", gauge_extra, sum_ns);
        let _ = write!(out, "tc_{base}_count{} {}\n", gauge_extra, total);
        // Estimated quantile gauges (nanoseconds).
        for (q, suffix) in [(0.5, "p50"), (0.9, "p90"), (0.99, "p99")] {
            let _ = write!(
                out,
                "tc_{base}_{suffix}{} {}\n",
                gauge_extra,
                Self::quantile_from(&counts, q)
            );
        }
    }
}

/// The four parallel latency histograms, one per existing `*_ns_total` family.
/// Indexed by [`LatencyMetric`].
struct LatencyHistograms {
    hists: [LatencyHistogram; 4],
}

impl Default for LatencyHistograms {
    fn default() -> Self {
        Self {
            hists: std::array::from_fn(|_| LatencyHistogram::new()),
        }
    }
}

impl LatencyHistograms {
    fn render_into(&self, out: &mut String) {
        const SPECS: [(&str, &str); 4] = [
            (
                "command_latency_ns",
                "Matching command processing latency (nanoseconds)",
            ),
            ("raft_commit_ns", "Raft quorum commit latency (nanoseconds)"),
            (
                "wal_fsync_ns",
                "Post-match durability barrier latency (nanoseconds)",
            ),
            ("match_ns", "In-memory matching latency (nanoseconds)"),
        ];
        for (i, (base, help)) in SPECS.iter().enumerate() {
            let _ = write!(out, "# HELP tc_{base} {help}\n# TYPE tc_{base} histogram\n");
            self.hists[i].render_into(out, base, None);
            // Quantile gauges share the base name with a suffix; declare their
            // TYPE so scrapers treat them as gauges rather than histogram parts.
            for suffix in ["p50", "p90", "p99"] {
                let _ = write!(out, "# TYPE tc_{base}_{suffix} gauge\n");
            }
        }
    }
}

/// Identifies one of the parallel latency histogram families.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum LatencyMetric {
    Command = 0,
    RaftCommit = 1,
    WalFsync = 2,
    Match = 3,
}

/// A latency histogram fanned out across a fixed, pre-registered set of label
/// values (e.g. one per Raft group). The hot path addresses a series by index;
/// there is no runtime map insertion.
pub struct LabeledHistogram {
    base: String,
    help: String,
    label_key: String,
    labels: Vec<String>,
    hists: Vec<LatencyHistogram>,
}

impl LabeledHistogram {
    /// Pre-register one histogram per label value. `base` is the metric base
    /// name (without `tc_`), `label_key` the dimension name (e.g. `group`).
    pub fn new(
        base: impl Into<String>,
        help: impl Into<String>,
        label_key: impl Into<String>,
        labels: Vec<String>,
    ) -> Self {
        let hists = labels.iter().map(|_| LatencyHistogram::new()).collect();
        Self {
            base: base.into(),
            help: help.into(),
            label_key: label_key.into(),
            labels,
            hists,
        }
    }

    /// Number of registered label series.
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// Resolve a label value to its index (setup-time helper, not hot-path).
    pub fn index_of(&self, label: &str) -> Option<usize> {
        self.labels.iter().position(|l| l == label)
    }

    /// Record a sample into series `idx`. O(1); out-of-range is ignored.
    #[inline]
    pub fn record_at(&self, idx: usize, ns: u64) {
        if let Some(h) = self.hists.get(idx) {
            h.record(ns);
        }
    }

    fn render_into(&self, out: &mut String) {
        let base = &self.base;
        let _ = write!(
            out,
            "# HELP tc_{base} {}\n# TYPE tc_{base} histogram\n",
            self.help
        );
        for (i, label) in self.labels.iter().enumerate() {
            let kv = format!("{}=\"{}\"", self.label_key, label);
            self.hists[i].render_into(out, base, Some(&kv));
        }
        for suffix in ["p50", "p90", "p99"] {
            let _ = write!(out, "# TYPE tc_{base}_{suffix} gauge\n");
        }
    }
}

/// Serve `GET /metrics` on `addr` in a background thread (thread per request;
/// scrape traffic is tiny).
pub fn serve(addr: String, metrics: Arc<Metrics>) {
    std::thread::Builder::new()
        .name("metrics".into())
        .spawn(move || {
            let Ok(listener) = TcpListener::bind(&addr) else {
                crate::log_error!("metrics", "cannot bind {addr}");
                return;
            };
            crate::log_info!("metrics", "Prometheus on http://{addr}/metrics");
            for mut s in listener.incoming().flatten() {
                let mut request = [0u8; 1024];
                let n = s.read(&mut request).unwrap_or(0);
                let line = std::str::from_utf8(&request[..n])
                    .ok()
                    .and_then(|r| r.lines().next())
                    .unwrap_or("");
                let (status, content_type, body) = if line.starts_with("GET /healthz ") {
                    ("200 OK", "text/plain", "ok\n".to_string())
                } else if line.starts_with("GET /readyz ") {
                    if metrics.is_ready() {
                        ("200 OK", "text/plain", "ready\n".to_string())
                    } else {
                        (
                            "503 Service Unavailable",
                            "text/plain",
                            "not ready\n".to_string(),
                        )
                    }
                } else {
                    ("200 OK", "text/plain; version=0.0.4", metrics.render())
                };
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            }
        })
        .expect("spawn metrics");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InstrumentId, OrderId};

    #[test]
    fn counters_tally_and_render() {
        let m = Metrics::default();
        m.record(&ExecReport::Accepted {
            instrument: InstrumentId(1),
            order_id: OrderId(1),
        });
        m.record(&ExecReport::Cancelled {
            instrument: InstrumentId(1),
            order_id: OrderId(1),
        });
        let text = m.render();
        assert!(text.contains("tc_orders_accepted 1"));
        assert!(text.contains("tc_cancels 1"));
        assert!(text.contains("# TYPE tc_trades counter"));
        assert!(text.contains("tc_raft_commit_index 0"));
        assert!(text.contains("tc_raft_apply_lag 0"));
    }

    #[test]
    fn latency_and_asset_wal_metrics_render() {
        let m = Metrics::default();
        m.record_command_latency(100);
        m.record_command_latency(250);
        m.record_raft_commit_latency(300);
        m.record_wal_fsync_latency(400);
        m.record_match_latency(50);
        m.execution_kafka_publish_ns_total
            .store(700, Ordering::Relaxed);
        m.execution_kafka_publish_ns_max
            .store(700, Ordering::Relaxed);
        m.execution_kafka_publish_samples
            .store(1, Ordering::Relaxed);
        m.asset_wal_errors.fetch_add(1, Ordering::Relaxed);
        m.set_raft_enqueued_index(12);
        m.set_raft_applied_index(9);
        let text = m.render();
        assert!(text.contains("tc_command_latency_ns_total 350"));
        assert!(text.contains("tc_command_latency_samples 2"));
        assert!(text.contains("tc_command_latency_ns_max 250"));
        assert!(text.contains("tc_asset_wal_errors 1"));
        assert!(text.contains("tc_raft_commit_ns_total 300"));
        assert!(text.contains("tc_wal_fsync_ns_max 400"));
        assert!(text.contains("tc_match_ns_total 50"));
        assert!(text.contains("tc_execution_kafka_publish_ns_total 700"));
        assert!(text.contains("tc_execution_kafka_publish_samples 1"));
        assert!(text.contains("tc_execution_kafka_publish_ns_max 700"));
        assert!(text.contains("tc_raft_apply_lag 3"));
    }

    #[test]
    fn bucket_index_maps_edges_and_rollover() {
        // Sub-1µs and zero collapse into bucket 0 (le = 1000).
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(999), 0);
        assert_eq!(bucket_index(1000), 0);
        assert_eq!(bucket_le(0), 1000);
        // Just over a boundary rounds up to the next `le`.
        assert_eq!(bucket_index(1001), 1);
        assert_eq!(bucket_le(1), 2000);
        // Exact boundary stays in its own bucket.
        assert_eq!(bucket_index(2000), 1);
        assert_eq!(bucket_index(9000), 8);
        assert_eq!(bucket_le(8), 9000);
        // Rollover across a decade: 9001 → le 10000 = 1e4 (first bucket of next decade).
        assert_eq!(bucket_index(9001), 9);
        assert_eq!(bucket_le(9), 10_000);
        // Beyond the last finite edge → +Inf overflow bucket.
        assert_eq!(bucket_index(u64::MAX), FINITE_BUCKETS);
        assert_eq!(bucket_index(100_000_000_000), FINITE_BUCKETS); // 1e11 ns
    }

    #[test]
    fn quantile_estimate_within_bucket_granularity() {
        // Feed a known distribution: 1000 samples uniformly in [1µs, 100µs).
        // With log buckets the p50/p99 estimate must land within the width of
        // the bucket that contains the true quantile.
        let h = LatencyHistogram::new();
        let n = 100_000u64;
        for i in 0..n {
            // 1_000 .. 100_000 ns
            let ns = 1_000 + (i * 99_000) / n;
            h.record(ns);
        }
        let true_p50 = 1_000 + 99_000 / 2; // ~50_500 ns
        let true_p99 = 1_000 + (99 * 99_000) / 100; // ~99_010 ns
        let est_p50 = h.quantile(0.5);
        let est_p99 = h.quantile(0.99);
        // Bucket width around 50µs is 10µs (le 50000 spans 40000..50000);
        // interpolation should keep us well inside a bucket's width.
        let bucket_w = 10_000i64; // widest bucket in this range
        assert!(
            (est_p50 as i64 - true_p50 as i64).abs() <= bucket_w,
            "p50 est {est_p50} vs true {true_p50}"
        );
        assert!(
            (est_p99 as i64 - true_p99 as i64).abs() <= bucket_w,
            "p99 est {est_p99} vs true {true_p99}"
        );
    }

    #[test]
    fn empty_histogram_quantile_is_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.quantile(0.5), 0);
        assert_eq!(h.quantile(0.99), 0);
    }

    #[test]
    fn histogram_renders_prometheus_shape() {
        let m = Metrics::default();
        m.record_latency_hist(LatencyMetric::Command, 1_500);
        m.record_latency_hist(LatencyMetric::Command, 42_000);
        let text = m.render();
        assert!(text.contains("# TYPE tc_command_latency_ns histogram"));
        assert!(text.contains("tc_command_latency_ns_bucket{le=\"2000\"} "));
        assert!(text.contains("tc_command_latency_ns_bucket{le=\"+Inf\"} 2"));
        assert!(text.contains("tc_command_latency_ns_sum 43500"));
        assert!(text.contains("tc_command_latency_ns_count 2"));
        assert!(text.contains("# TYPE tc_command_latency_ns_p99 gauge"));
        assert!(text.contains("tc_command_latency_ns_p50 "));
        // Cumulative: the +Inf bucket equals the count.
        assert!(text.contains("tc_match_ns_bucket{le=\"+Inf\"} 0"));
    }

    #[test]
    fn labeled_histogram_render_format() {
        let m = Metrics::default();
        m.register_raft_commit_groups(4);
        // Idempotent second call is a no-op.
        m.register_raft_commit_groups(4);
        m.record_raft_commit_latency_group(3, 5_000);
        m.record_raft_commit_latency_group(0, 5_000);
        // Out-of-range group is silently ignored (no panic).
        m.record_raft_commit_latency_group(99, 5_000);
        let text = m.render();
        assert!(text.contains("# TYPE tc_raft_commit_ns histogram"));
        assert!(text.contains("tc_raft_commit_ns_bucket{group=\"3\",le=\"5000\"} 1"));
        assert!(text.contains("tc_raft_commit_ns_bucket{group=\"3\",le=\"+Inf\"} 1"));
        assert!(text.contains("tc_raft_commit_ns_count{group=\"0\"} 1"));
        assert!(text.contains("tc_raft_commit_ns_count{group=\"3\"} 1"));
        assert!(text.contains("tc_raft_commit_ns_p99{group=\"3\"} "));
        // A group that received nothing still renders, with a zero count.
        assert!(text.contains("tc_raft_commit_ns_count{group=\"2\"} 0"));
    }

    #[test]
    fn labeled_index_of_resolves_setup_labels() {
        let h = LabeledHistogram::new("x_ns", "help", "group", vec!["0".into(), "7".into()]);
        assert_eq!(h.len(), 2);
        assert_eq!(h.index_of("7"), Some(1));
        assert_eq!(h.index_of("nope"), None);
    }

    #[test]
    fn concurrent_record_is_lossless() {
        use std::sync::Arc;
        let h = Arc::new(LatencyHistogram::new());
        let threads = 8;
        let per = 50_000u64;
        let mut handles = Vec::new();
        for _ in 0..threads {
            let h = Arc::clone(&h);
            handles.push(std::thread::spawn(move || {
                for i in 0..per {
                    // Spread across several decades.
                    h.record(1_000 + (i % 7) * 3_000);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let counts = h.snapshot();
        let total: u64 = counts.iter().sum();
        assert_eq!(total, threads as u64 * per);
        // sum_ns must equal the arithmetic sum of every recorded value.
        let expected_sum: u64 =
            (0..per).map(|i| 1_000 + (i % 7) * 3_000).sum::<u64>() * threads as u64;
        assert_eq!(h.sum_ns.load(Ordering::Relaxed), expected_sum);
    }
}
