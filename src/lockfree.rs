//! A bounded, wait-free **single-producer / single-consumer (SPSC)** ring buffer.
//!
//! This is the backbone of the engine's lock-free design. The whole exchange is
//! built as *share-nothing* actors (one matching shard owns its books outright)
//! that communicate **only** through these queues. No mutex is ever taken on the
//! hot path: the producer touches only `tail`, the consumer only `head`, and they
//! observe each other through a single `Acquire`/`Release` pair per operation.
//!
//! The algorithm is the classic Lamport SPSC queue with monotonically increasing
//! indices and a power-of-two mask, plus cache-line padding to avoid false
//! sharing between the producer and consumer counters.

use std::cell::{Cell, UnsafeCell};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Pads a value to a full cache line so the producer's and consumer's atomics
/// never live in the same line (which would cause false sharing).
#[repr(align(64))]
struct CachePadded<T>(T);

struct Ring<T> {
    /// Slots; only ever accessed by the owning end for a given index, so the
    /// `UnsafeCell` aliasing is disciplined by the head/tail protocol.
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// Next index to read (owned by the consumer).
    head: CachePadded<AtomicUsize>,
    /// Next index to write (owned by the producer).
    tail: CachePadded<AtomicUsize>,
}

// Safe to share the `Ring` across the two ends because each atomic has a single
// writer and the slot protocol prevents concurrent access to the same slot.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(2);
        let mut v = Vec::with_capacity(cap);
        for _ in 0..cap {
            v.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Ring {
            slots: v.into_boxed_slice(),
            mask: cap - 1,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // Drop any items still queued between head and tail.
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        let mut i = head;
        while i != tail {
            let slot = &self.slots[i & self.mask];
            unsafe { (*slot.get()).assume_init_drop() };
            i = i.wrapping_add(1);
        }
    }
}

/// The producing end. `!Sync`: exactly one thread may push.
///
/// Caches the last-observed consumer position so the common (non-full) push
/// touches **no** shared cache line except its own tail: the consumer's `head`
/// is re-loaded only when the queue *looks* full against the cached value.
/// This avoids head/tail cache-line ping-pong and is worth several× throughput.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    cached_head: Cell<usize>,
}

/// The consuming end. `!Sync`: exactly one thread may pop. Mirrors the
/// producer's optimisation by caching the last-observed `tail`.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    cached_tail: Cell<usize>,
}

// Sendable to the thread that will own the end; not Sync (single owner each).
unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}

/// Create a bounded SPSC queue. `capacity` is rounded up to a power of two
/// (minimum 2). Returns the producer and consumer halves.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::with_capacity(capacity));
    (
        Producer {
            ring: ring.clone(),
            cached_head: Cell::new(0),
        },
        Consumer {
            ring,
            cached_tail: Cell::new(0),
        },
    )
}

impl<T> Producer<T> {
    /// Try to enqueue `item`. Returns `Err(item)` if the queue is full.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        let ring = &*self.ring;
        let tail = ring.tail.0.load(Ordering::Relaxed);
        // Fast path: judge fullness against the cached head; only on apparent
        // fullness pay the Acquire load of the consumer's real position.
        if tail.wrapping_sub(self.cached_head.get()) == ring.slots.len() {
            self.cached_head.set(ring.head.0.load(Ordering::Acquire));
            if tail.wrapping_sub(self.cached_head.get()) == ring.slots.len() {
                return Err(item); // truly full
            }
        }
        let slot = &ring.slots[tail & ring.mask];
        unsafe { (*slot.get()).write(item) };
        // Release so the consumer sees the slot write once it sees this tail.
        ring.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Number of items currently queued (approximate under concurrency).
    pub fn len(&self) -> usize {
        let ring = &*self.ring;
        let tail = ring.tail.0.load(Ordering::Relaxed);
        let head = ring.head.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Whether the queue currently holds no items.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Consumer<T> {
    /// Try to dequeue an item. Returns `None` if the queue is empty.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let ring = &*self.ring;
        let head = ring.head.0.load(Ordering::Relaxed);
        // Fast path: judge emptiness against the cached tail; only on apparent
        // emptiness pay the Acquire load of the producer's real position.
        if head == self.cached_tail.get() {
            self.cached_tail.set(ring.tail.0.load(Ordering::Acquire));
            if head == self.cached_tail.get() {
                return None; // truly empty
            }
        }
        let slot = &ring.slots[head & ring.mask];
        let item = unsafe { (*slot.get()).assume_init_read() };
        // Release so the producer sees the slot freed once it sees this head.
        ring.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    /// True if the queue is currently empty.
    pub fn is_empty(&self) -> bool {
        let ring = &*self.ring;
        ring.head.0.load(Ordering::Relaxed) == ring.tail.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn single_threaded_fifo_and_full() {
        let (tx, rx) = channel::<u32>(4); // rounds to 4
        assert!(rx.pop().is_none());
        for i in 0..4 {
            assert!(tx.push(i).is_ok());
        }
        assert_eq!(tx.push(99), Err(99)); // full
        for i in 0..4 {
            assert_eq!(rx.pop(), Some(i));
        }
        assert!(rx.pop().is_none());
    }

    #[test]
    fn spsc_transfers_all_items_in_order() {
        const N: u64 = 1_000_000;
        let (tx, rx) = channel::<u64>(1024);
        let producer = thread::spawn(move || {
            let mut i = 0;
            while i < N {
                if tx.push(i).is_ok() {
                    i += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        let mut expected = 0u64;
        while expected < N {
            match rx.pop() {
                Some(v) => {
                    assert_eq!(v, expected, "SPSC must preserve FIFO order");
                    expected += 1;
                }
                None => std::hint::spin_loop(),
            }
        }
        producer.join().unwrap();
        assert_eq!(expected, N);
    }

    #[test]
    fn drops_unconsumed_items() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let (tx, _rx) = channel::<Counted>(8);
            for _ in 0..5 {
                tx.push(Counted(drops.clone())).ok();
            }
        } // ring dropped here with 5 items still queued
        assert_eq!(drops.load(Ordering::SeqCst), 5);
    }
}
