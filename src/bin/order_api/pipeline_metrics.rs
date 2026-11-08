//! Commit-latency, backlog and lag counters behind `/metrics`.

use super::*;

#[derive(Default)]
pub(crate) struct OrderPipelineMetrics {
    pub(crate) mysql_commit_ns_total: AtomicU64,
    pub(crate) mysql_commit_ns_max: AtomicU64,
    pub(crate) mysql_commit_samples: AtomicU64,
    /// Parallel log-bucketed distribution of MySQL commit latency, backing the
    /// p50/p90/p99 SLO gauges the `*_ns_total`/`*_ns_max` triple cannot answer.
    pub(crate) mysql_commit_hist: LatencyHistogram,
    pub(crate) raft_forward_ns_total: AtomicU64,
    pub(crate) raft_forward_ns_max: AtomicU64,
    pub(crate) raft_forward_samples: AtomicU64,
    pub(crate) execution_mysql_commit_ns_total: AtomicU64,
    pub(crate) execution_mysql_commit_ns_max: AtomicU64,
    pub(crate) execution_mysql_commit_samples: AtomicU64,
    pub(crate) execution_mysql_completed_events: AtomicU64,
    pub(crate) execution_mysql_commit_hist: LatencyHistogram,
    /// Commands currently waiting for Kafka delivery in this API process.
    /// Durable downstream backlog is read from the shared consumer groups.
    pub(crate) inflight_publish_commands: AtomicU64,
    pub(crate) published_commands: AtomicU64,
    pub(crate) mysql_completed_commands: AtomicU64,
    pub(crate) match_completed_commands: AtomicU64,
    pub(crate) backpressure_rejections: AtomicU64,
    pub(crate) observed_mysql_lag: AtomicU64,
    pub(crate) observed_match_lag: AtomicU64,
    pub(crate) dlq_total: AtomicU64,
    /// Consumer-group lag reads that failed (Kafka metadata/coordinator errors).
    /// While these persist the observed-lag gauges would otherwise freeze at a
    /// stale value and drive backpressure off phantom backlog; the monitor
    /// clears the lag after a short staleness window, and this counter makes the
    /// condition observable instead of silent.
    pub(crate) lag_read_failures: AtomicU64,
    /// Rows the dedupe retention has deleted (see `spawn_dedupe_retention`).
    pub(crate) dedupe_purged_rows: AtomicU64,
}

impl OrderPipelineMetrics {
    pub(crate) fn record(total: &AtomicU64, max: &AtomicU64, samples: &AtomicU64, elapsed: Duration) {
        let ns = elapsed.as_nanos() as u64;
        total.fetch_add(ns, AtomicOrdering::Relaxed);
        max.fetch_max(ns, AtomicOrdering::Relaxed);
        samples.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn record_mysql(&self, elapsed: Duration) {
        Self::record(
            &self.mysql_commit_ns_total,
            &self.mysql_commit_ns_max,
            &self.mysql_commit_samples,
            elapsed,
        );
        self.mysql_commit_hist.record(elapsed.as_nanos() as u64);
    }

    pub(crate) fn record_raft(&self, elapsed: Duration) {
        Self::record(
            &self.raft_forward_ns_total,
            &self.raft_forward_ns_max,
            &self.raft_forward_samples,
            elapsed,
        );
    }

    pub(crate) fn record_execution_mysql_batch(&self, elapsed: Duration, events: u64) {
        Self::record(
            &self.execution_mysql_commit_ns_total,
            &self.execution_mysql_commit_ns_max,
            &self.execution_mysql_commit_samples,
            elapsed,
        );
        self.execution_mysql_commit_hist
            .record(elapsed.as_nanos() as u64);
        self.execution_mysql_completed_events
            .fetch_add(events, AtomicOrdering::Relaxed);
    }

    pub(crate) fn try_reserve(&self, commands: u64, max_backlog: u64) -> Result<(), u64> {
        loop {
            let inflight = self.inflight_publish_commands.load(AtomicOrdering::Acquire);
            let backlog = inflight.saturating_add(
                self.observed_mysql_lag
                    .load(AtomicOrdering::Acquire)
                    .max(self.observed_match_lag.load(AtomicOrdering::Acquire)),
            );
            if backlog.saturating_add(commands) > max_backlog {
                self.backpressure_rejections
                    .fetch_add(commands, AtomicOrdering::Relaxed);
                return Err(backlog);
            }
            if self
                .inflight_publish_commands
                .compare_exchange_weak(
                    inflight,
                    inflight.saturating_add(commands),
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub(crate) fn rollback_reservation(&self, commands: u64) {
        self.inflight_publish_commands
            .fetch_sub(commands, AtomicOrdering::AcqRel);
    }

    pub(crate) fn finish_reservation(&self, reserved: u64, published: u64) {
        self.inflight_publish_commands
            .fetch_sub(reserved, AtomicOrdering::AcqRel);
        self.published_commands
            .fetch_add(published, AtomicOrdering::Relaxed);
    }

    pub(crate) fn complete(&self, stage: &str, commands: u64) {
        let counter = match stage {
            "mysql" => &self.mysql_completed_commands,
            "match" => &self.match_completed_commands,
            _ => return,
        };
        // Worker roles publish no HTTP commands in their own process, so this
        // counter cannot be clamped to the process-local published counter.
        // Kafka consumer-group lag is the durable shared backlog; this metric
        // reports work completed by this replica and is aggregated by role.
        counter.fetch_add(commands, AtomicOrdering::Relaxed);
    }

    pub(crate) fn set_observed_lag(&self, stage: &str, lag: u64) {
        let counter = match stage {
            "mysql" => &self.observed_mysql_lag,
            "match" => &self.observed_match_lag,
            _ => return,
        };
        counter.store(lag, AtomicOrdering::Release);
    }

    pub(crate) fn backlog(&self) -> u64 {
        self.inflight_publish_commands
            .load(AtomicOrdering::Acquire)
            .saturating_add(
                self.observed_mysql_lag
                    .load(AtomicOrdering::Acquire)
                    .max(self.observed_match_lag.load(AtomicOrdering::Acquire)),
            )
    }

    pub(crate) fn render(&self) -> String {
        format!(
            "# TYPE tc_order_mysql_commit_ns_total counter\ntc_order_mysql_commit_ns_total {}\n\
# TYPE tc_order_mysql_commit_ns_max gauge\ntc_order_mysql_commit_ns_max {}\n\
# TYPE tc_order_mysql_commit_samples counter\ntc_order_mysql_commit_samples {}\n\
# TYPE tc_order_raft_forward_ns_total counter\ntc_order_raft_forward_ns_total {}\n\
# TYPE tc_order_raft_forward_ns_max gauge\ntc_order_raft_forward_ns_max {}\n\
# TYPE tc_order_raft_forward_samples counter\ntc_order_raft_forward_samples {}\n",
            self.mysql_commit_ns_total.load(AtomicOrdering::Relaxed),
            self.mysql_commit_ns_max.load(AtomicOrdering::Relaxed),
            self.mysql_commit_samples.load(AtomicOrdering::Relaxed),
            self.raft_forward_ns_total.load(AtomicOrdering::Relaxed),
            self.raft_forward_ns_max.load(AtomicOrdering::Relaxed),
            self.raft_forward_samples.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_execution_mysql_commit_ns_total counter\ntc_execution_mysql_commit_ns_total {}\n\
# TYPE tc_execution_mysql_commit_ns_max gauge\ntc_execution_mysql_commit_ns_max {}\n\
# TYPE tc_execution_mysql_commit_samples counter\ntc_execution_mysql_commit_samples {}\n\
# TYPE tc_execution_mysql_completed_events counter\ntc_execution_mysql_completed_events {}\n",
            self.execution_mysql_commit_ns_total.load(AtomicOrdering::Relaxed),
            self.execution_mysql_commit_ns_max.load(AtomicOrdering::Relaxed),
            self.execution_mysql_commit_samples.load(AtomicOrdering::Relaxed),
            self.execution_mysql_completed_events.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_order_ingress_backlog gauge\ntc_order_ingress_backlog {}\n\
# TYPE tc_order_publish_inflight gauge\ntc_order_publish_inflight {}\n\
# TYPE tc_order_published_commands counter\ntc_order_published_commands {}\n\
# TYPE tc_order_mysql_completed_commands counter\ntc_order_mysql_completed_commands {}\n\
# TYPE tc_order_match_completed_commands counter\ntc_order_match_completed_commands {}\n\
# TYPE tc_order_backpressure_rejections counter\ntc_order_backpressure_rejections {}\n",
            self.backlog(),
            self.inflight_publish_commands.load(AtomicOrdering::Relaxed),
            self.published_commands.load(AtomicOrdering::Relaxed),
            self.mysql_completed_commands.load(AtomicOrdering::Relaxed),
            self.match_completed_commands.load(AtomicOrdering::Relaxed),
            self.backpressure_rejections.load(AtomicOrdering::Relaxed),
        ) + &format!(
            "# TYPE tc_order_mysql_consumer_lag gauge\ntc_order_mysql_consumer_lag {}\n\
# TYPE tc_order_match_consumer_lag gauge\ntc_order_match_consumer_lag {}\n\
# TYPE tc_order_dlq_total counter\ntc_order_dlq_total {}\n\
# TYPE tc_order_lag_read_failures counter\ntc_order_lag_read_failures {}\n\
# TYPE tc_order_dedupe_purged_rows counter\ntc_order_dedupe_purged_rows {}\n",
            self.observed_mysql_lag.load(AtomicOrdering::Relaxed),
            self.observed_match_lag.load(AtomicOrdering::Relaxed),
            self.dlq_total.load(AtomicOrdering::Relaxed),
            self.lag_read_failures.load(AtomicOrdering::Relaxed),
            self.dedupe_purged_rows.load(AtomicOrdering::Relaxed),
        ) + &self.mysql_commit_hist.render_standalone(
            "order_mysql_commit_ns",
            "Order API MySQL commit latency (nanoseconds)",
        ) + &self.execution_mysql_commit_hist.render_standalone(
            "execution_mysql_commit_ns",
            "Execution Kafka to MySQL commit processing latency (nanoseconds)",
        )
    }
}
