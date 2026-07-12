//! Operational metrics, Prometheus text exposition — counters incremented at
//! the shard's single emit point (lock-free atomics; ~1 ns per report).

use std::io::Write;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
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
        ]
        .concat()
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
                let body = metrics.render();
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
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
        m.record(&ExecReport::Accepted { instrument: InstrumentId(1), order_id: OrderId(1) });
        m.record(&ExecReport::Cancelled { instrument: InstrumentId(1), order_id: OrderId(1) });
        let text = m.render();
        assert!(text.contains("tc_orders_accepted 1"));
        assert!(text.contains("tc_cancels 1"));
        assert!(text.contains("# TYPE tc_trades counter"));
    }
}
