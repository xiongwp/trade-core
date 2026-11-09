//! Ingress admission control driven by Kafka consumer lag.

use super::*;

/// Ingress admission tier for one category, from the document's §5.3 ladder.
/// Ordered by severity so a batch spanning categories can take the worst.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub(crate) enum BackpressureTier {
    /// Below the soft limit — accept normally.
    Normal,
    /// >= soft limit (default 2 s of design traffic): slow the caller down.
    Soft,
    /// >= hard limit (default 5 s): reject with HTTP 429.
    Hard,
    /// >= emergency limit (default 15 s) or the category's Raft quorum is lost:
    /// stop writes for this category with HTTP 503. Independently recoverable.
    Emergency,
}

/// Per-category, three-tier ingress backpressure (production doc §5.3).
///
/// Lag is observed **per Kafka partition** (a category maps to exactly one
/// partition) as the max of the persist and match consumer groups, so a hot
/// partition throttles only the categories that share it — the global
/// [`OrderPipelineMetrics`] backlog stays as the market-wide master switch.
/// Quorum health is tracked per Raft group and clears on the next successful
/// forward, making the emergency tier independently recoverable.
pub(crate) struct Backpressure {
    pub(crate) soft: u64,
    pub(crate) hard: u64,
    pub(crate) emergency: u64,
    pub(crate) soft_delay: Duration,
    pub(crate) topic_count: u32,
    pub(crate) partitions_per_topic: u32,
    /// The live routing (shared with intake): after a category migrates its
    /// quorum health must come from the group that now owns it.
    pub(crate) routes: trade_core::cluster::RouteControlPlane,
    /// The MySQL projection's lag: the largest over the DB-route partitions.
    /// Every category's orders spread over every database, so a database
    /// that falls behind is everyone's lag, not one order partition's.
    pub(crate) mysql_lag: AtomicU64,
    pub(crate) match_lag: Vec<AtomicU64>,
    /// Per group: until when (ms since the epoch) it counts as unhealthy
    /// after a failed forward. It expires, so an instance that stops
    /// forwarding to a group (its partitions moved away) does not keep
    /// refusing that group's categories forever.
    pub(crate) group_unhealthy: Vec<AtomicU64>,
    pub(crate) soft_events: AtomicU64,
    pub(crate) hard_events: AtomicU64,
    pub(crate) emergency_events: AtomicU64,
}

impl Backpressure {
    pub(crate) fn from_env(
        topic_count: usize,
        partitions_per_topic: u32,
        raft_group_count: usize,
        routes: trade_core::cluster::RouteControlPlane,
    ) -> Self {
        // Defaults express ~2 s / 5 s / 15 s of a 50k commands/s/partition
        // design load; each is overridable directly for the machine's tuning.
        let soft = env_number("TC_ORDER_BP_SOFT", 100_000u64).max(1);
        let hard = env_number("TC_ORDER_BP_HARD", 250_000u64).max(soft);
        let emergency = env_number("TC_ORDER_BP_EMERGENCY", 750_000u64).max(hard);
        let partitions = topic_count * partitions_per_topic as usize;
        Self {
            soft,
            hard,
            emergency,
            soft_delay: Duration::from_millis(env_number("TC_ORDER_BP_SOFT_DELAY_MS", 2u64)),
            topic_count: topic_count.max(1) as u32,
            partitions_per_topic: partitions_per_topic.max(1),
            routes,
            mysql_lag: AtomicU64::new(0),
            match_lag: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            group_unhealthy: (0..raft_group_count.max(1))
                .map(|_| AtomicU64::new(0))
                .collect(),
            soft_events: AtomicU64::new(0),
            hard_events: AtomicU64::new(0),
            emergency_events: AtomicU64::new(0),
        }
    }

    /// Pure tier decision — the state machine unit-tested in isolation.
    pub(crate) fn classify(&self, lag: u64, quorum_lost: bool) -> BackpressureTier {
        if quorum_lost || lag >= self.emergency {
            BackpressureTier::Emergency
        } else if lag >= self.hard {
            BackpressureTier::Hard
        } else if lag >= self.soft {
            BackpressureTier::Soft
        } else {
            BackpressureTier::Normal
        }
    }

    pub(crate) fn partition_index(&self, category_id: u32) -> usize {
        let topic = (category_id % self.topic_count) as usize;
        let partition = ((category_id / self.topic_count) % self.partitions_per_topic) as usize;
        topic * self.partitions_per_topic as usize + partition
    }

    pub(crate) fn raft_group(&self, category_id: u32) -> usize {
        self.routes.group_for(category_id)
    }

    pub(crate) fn partition_lag(&self, index: usize) -> u64 {
        let mysql = self.mysql_lag.load(AtomicOrdering::Acquire);
        let matcher = self
            .match_lag
            .get(index)
            .map_or(0, |v| v.load(AtomicOrdering::Acquire));
        mysql.max(matcher)
    }

    pub(crate) fn set_partition_lags(&self, stage: &str, lags: &[u64]) {
        match stage {
            "mysql" => self
                .mysql_lag
                .store(lags.iter().copied().max().unwrap_or(0), AtomicOrdering::Release),
            "match" => {
                for (slot, lag) in self.match_lag.iter().zip(lags.iter()) {
                    slot.store(*lag, AtomicOrdering::Release);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn set_group_health(&self, group: usize, healthy: bool) {
        if let Some(until) = self.group_unhealthy.get(group) {
            let value = if healthy { 0 } else { now_ms() + GROUP_UNHEALTHY_MS };
            until.store(value, AtomicOrdering::Release);
        }
    }

    pub(crate) fn group_unhealthy(&self, group: usize) -> bool {
        self.group_unhealthy
            .get(group)
            .is_some_and(|until| now_ms() < until.load(AtomicOrdering::Acquire))
    }

    /// Current tier for a category from its partition lag and quorum health.
    pub(crate) fn tier_for_category(&self, category_id: u32) -> BackpressureTier {
        let lag = self.partition_lag(self.partition_index(category_id));
        self.classify(lag, self.group_unhealthy(self.raft_group(category_id)))
    }

    pub(crate) fn note(&self, tier: BackpressureTier) {
        match tier {
            BackpressureTier::Soft => &self.soft_events,
            BackpressureTier::Hard => &self.hard_events,
            BackpressureTier::Emergency => &self.emergency_events,
            BackpressureTier::Normal => return,
        }
        .fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Admit one category's write: apply the soft-tier slowdown inline, or map
    /// hard/emergency to an ingress error. Emergency is 503, hard is 429.
    pub(crate) fn admit(&self, category_id: u32) -> Result<(), String> {
        let tier = self.tier_for_category(category_id);
        self.note(tier);
        match tier {
            BackpressureTier::Normal => Ok(()),
            BackpressureTier::Soft => {
                if !self.soft_delay.is_zero() {
                    std::thread::sleep(self.soft_delay);
                }
                Ok::<(), String>(())
            }
            BackpressureTier::Hard => Err(format!(
                "backpressure: category {category_id} throttled at hard limit {}",
                self.hard
            )),
            BackpressureTier::Emergency => Err(format!(
                "backpressure-emergency: category {category_id} writes stopped (lag/quorum)"
            )),
        }
    }

    pub(crate) fn max_partition_lag(&self) -> u64 {
        (0..self.match_lag.len())
            .map(|i| self.partition_lag(i))
            .max()
            .unwrap_or(0)
            .max(self.mysql_lag.load(AtomicOrdering::Acquire))
    }

    pub(crate) fn unhealthy_groups(&self) -> u64 {
        self.group_unhealthy
            .iter()
            .filter(|until| now_ms() < until.load(AtomicOrdering::Acquire))
            .count() as u64
    }

    pub(crate) fn render(&self) -> String {
        format!(
            "# TYPE tc_order_bp_soft_limit gauge\ntc_order_bp_soft_limit {}\n\
# TYPE tc_order_bp_hard_limit gauge\ntc_order_bp_hard_limit {}\n\
# TYPE tc_order_bp_emergency_limit gauge\ntc_order_bp_emergency_limit {}\n\
# TYPE tc_order_bp_soft_total counter\ntc_order_bp_soft_total {}\n\
# TYPE tc_order_bp_hard_total counter\ntc_order_bp_hard_total {}\n\
# TYPE tc_order_bp_emergency_total counter\ntc_order_bp_emergency_total {}\n\
# TYPE tc_order_bp_max_partition_lag gauge\ntc_order_bp_max_partition_lag {}\n\
# TYPE tc_order_bp_quorum_unhealthy_groups gauge\ntc_order_bp_quorum_unhealthy_groups {}\n",
            self.soft,
            self.hard,
            self.emergency,
            self.soft_events.load(AtomicOrdering::Relaxed),
            self.hard_events.load(AtomicOrdering::Relaxed),
            self.emergency_events.load(AtomicOrdering::Relaxed),
            self.max_partition_lag(),
            self.unhealthy_groups(),
        )
    }
}

/// How long a failed forward marks its group unhealthy (a later failure
/// renews it; a success clears it).
const GROUP_UNHEALTHY_MS: u64 = 10_000;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
