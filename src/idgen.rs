//! Segment-based distributed ID generation (Meituan **Leaf-segment** style).
//!
//! Ids are allocated from *segments* — contiguous ranges handed out by a central
//! allocator (in Leaf, a `max_id = max_id + step` row update in a database).
//! Each process holds the current segment and pre-fetches the next one when the
//! current segment is half consumed (Leaf's double-buffer), so the hot path is a
//! single `fetch_add` — lock-free — and the allocator is touched once per `step`
//! ids rather than once per id.
//!
//! Properties: ids are unique and *trend-increasing* (monotonic within a
//! process; gaps can occur across segment boundaries and restarts, which is the
//! standard Leaf trade-off).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Hands out contiguous id ranges. In production this is a database row per biz
/// tag (`UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = ?`); the
/// in-memory implementation below simulates it for tests and single-node runs.
pub trait SegmentProvider: Send + Sync {
    /// Reserve the next `step`-sized range; returns its starting id.
    fn next_segment(&self, step: u64) -> u64;
}

/// An in-process allocator standing in for the Leaf database table.
#[derive(Debug)]
pub struct MemorySegmentProvider {
    max_id: AtomicU64,
}

impl MemorySegmentProvider {
    pub fn starting_at(start: u64) -> Self {
        MemorySegmentProvider { max_id: AtomicU64::new(start) }
    }
}

impl SegmentProvider for MemorySegmentProvider {
    fn next_segment(&self, step: u64) -> u64 {
        self.max_id.fetch_add(step, Ordering::Relaxed)
    }
}

/// Leaf-style id generator: lock-free `fetch_add` hot path, double-buffered
/// segment refill on the cold path.
pub struct LeafIdGen<P: SegmentProvider> {
    provider: P,
    step: u64,
    /// Next id to hand out.
    cursor: AtomicU64,
    /// Exclusive end of the current segment.
    end: AtomicU64,
    /// Pre-fetched next segment start (the "double buffer").
    next: Mutex<Option<u64>>,
}

impl<P: SegmentProvider> LeafIdGen<P> {
    pub fn new(provider: P, step: u64) -> Self {
        assert!(step >= 2, "step must be >= 2");
        let start = provider.next_segment(step);
        LeafIdGen {
            provider,
            step,
            cursor: AtomicU64::new(start),
            end: AtomicU64::new(start + step),
            next: Mutex::new(None),
        }
    }

    /// Allocate one id. Hot path: one `fetch_add` + one load.
    pub fn next_id(&self) -> u64 {
        loop {
            let id = self.cursor.fetch_add(1, Ordering::Relaxed);
            let end = self.end.load(Ordering::Acquire);
            if id < end {
                // Past the half-way mark: make sure the next segment is staged.
                if id + self.step / 2 >= end {
                    self.stage_next();
                }
                return id;
            }
            // Segment exhausted: install the staged (or a fresh) segment. Only
            // one thread wins the install; losers retry the loop.
            self.install_next(end);
        }
    }

    fn stage_next(&self) {
        if let Ok(mut next) = self.next.try_lock() {
            if next.is_none() {
                *next = Some(self.provider.next_segment(self.step));
            }
        }
    }

    fn install_next(&self, exhausted_end: u64) {
        let mut next = self.next.lock().unwrap();
        // Someone else may have already installed a newer segment.
        if self.end.load(Ordering::Acquire) != exhausted_end {
            return;
        }
        let start = next.take().unwrap_or_else(|| self.provider.next_segment(self.step));
        // The cursor is a *forever-monotonic* counter: `fetch_max` (never a
        // plain store) so it can only move forward. Threads spinning on an
        // exhausted segment may have pushed the cursor past `start`; resetting
        // it backwards would re-issue those overrun values once the new `end`
        // became visible — a duplicate-id race. Monotonicity makes every
        // `fetch_add` result unique unconditionally; overruns just become gaps,
        // the standard Leaf trade-off.
        self.cursor.fetch_max(start, Ordering::AcqRel);
        self.end.store(start + self.step, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn ids_are_unique_across_threads() {
        let gen = Arc::new(LeafIdGen::new(MemorySegmentProvider::starting_at(1), 1000));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let g = gen.clone();
            handles.push(std::thread::spawn(move || {
                (0..50_000).map(|_| g.next_id()).collect::<Vec<u64>>()
            }));
        }
        let mut all = HashSet::new();
        for h in handles {
            for id in h.join().unwrap() {
                assert!(all.insert(id), "duplicate id {id}");
            }
        }
        assert_eq!(all.len(), 8 * 50_000);
    }

    #[test]
    fn ids_trend_increasing_single_thread() {
        let gen = LeafIdGen::new(MemorySegmentProvider::starting_at(100), 10);
        let ids: Vec<u64> = (0..100).map(|_| gen.next_id()).collect();
        for w in ids.windows(2) {
            assert!(w[1] > w[0], "single-thread ids must increase: {w:?}");
        }
        assert_eq!(ids[0], 100);
    }
}
