//! Operational metrics, Prometheus text exposition — counters incremented at
//! the shard's single emit point (lock-free atomics; ~1 ns per report).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

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
        [
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
            c("command_latency_ns_total", "Total matching command processing time in nanoseconds", self.command_latency_ns_total.load(Ordering::Relaxed)),
            c("command_latency_samples", "Matching command latency samples", self.command_latency_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_command_latency_ns_max Maximum matching command processing time in nanoseconds\n# TYPE tc_command_latency_ns_max gauge\ntc_command_latency_ns_max {}\n", self.command_latency_ns_max.load(Ordering::Relaxed)),
            c("asset_wal_errors", "Per-asset WAL append failures", self.asset_wal_errors.load(Ordering::Relaxed)),
            c("raft_commit_ns_total", "Total Raft quorum commit time in nanoseconds", self.raft_commit_ns_total.load(Ordering::Relaxed)),
            c("raft_commit_samples", "Raft quorum commit batches", self.raft_commit_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_raft_commit_ns_max Maximum Raft quorum commit time in nanoseconds\n# TYPE tc_raft_commit_ns_max gauge\ntc_raft_commit_ns_max {}\n", self.raft_commit_ns_max.load(Ordering::Relaxed)),
            c("wal_fsync_ns_total", "Total asset WAL group fsync time in nanoseconds", self.wal_fsync_ns_total.load(Ordering::Relaxed)),
            c("wal_fsync_samples", "Asset WAL group fsync batches", self.wal_fsync_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_wal_fsync_ns_max Maximum asset WAL group fsync time in nanoseconds\n# TYPE tc_wal_fsync_ns_max gauge\ntc_wal_fsync_ns_max {}\n", self.wal_fsync_ns_max.load(Ordering::Relaxed)),
            c("match_ns_total", "Total in-memory matching time in nanoseconds", self.match_ns_total.load(Ordering::Relaxed)),
            c("match_samples", "Commands matched", self.match_samples.load(Ordering::Relaxed)),
            format!("# HELP tc_match_ns_max Maximum in-memory matching time in nanoseconds\n# TYPE tc_match_ns_max gauge\ntc_match_ns_max {}\n", self.match_ns_max.load(Ordering::Relaxed)),
        ]
        .concat()
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
}

/// Serve `GET /metrics` on `addr` in a background thread (thread per request;
/// scrape traffic is tiny).
pub fn serve(addr: String, metrics: Arc<Metrics>) {
    std::thread::Builder::new()
        .name("metrics".into())
        .spawn(move || {
            let Ok(listener) = TcpListener::bind(&addr) else {
                eprintln!("[metrics] cannot bind {addr}");
                return;
            };
            eprintln!("[metrics] Prometheus on http://{addr}/metrics");
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
    }

    #[test]
    fn latency_and_asset_wal_metrics_render() {
        let m = Metrics::default();
        m.record_command_latency(100);
        m.record_command_latency(250);
        m.record_raft_commit_latency(300);
        m.record_wal_fsync_latency(400);
        m.record_match_latency(50);
        m.asset_wal_errors.fetch_add(1, Ordering::Relaxed);
        let text = m.render();
        assert!(text.contains("tc_command_latency_ns_total 350"));
        assert!(text.contains("tc_command_latency_samples 2"));
        assert!(text.contains("tc_command_latency_ns_max 250"));
        assert!(text.contains("tc_asset_wal_errors 1"));
        assert!(text.contains("tc_raft_commit_ns_total 300"));
        assert!(text.contains("tc_wal_fsync_ns_max 400"));
        assert!(text.contains("tc_match_ns_total 50"));
    }
}
