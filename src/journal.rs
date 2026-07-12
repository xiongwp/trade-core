//! Write-ahead command journal: durable, ordered, replayable.
//!
//! # Design
//!
//! Matching is **in-memory**; durability comes from journaling every inbound
//! command *in the exact order the shard processes it*, before it touches the
//! book. Because the engine is deterministic (integer arithmetic, engine-assigned
//! sequence numbers, no wall-clock in the matching path), replaying the journal
//! reproduces the identical trade tape and book state.
//!
//! * **Ordering.** One journal file per shard; records carry a per-shard
//!   monotonically increasing `seq`. Replay preserves that total order exactly.
//!   (Instruments on different shards are independent, so per-shard order is the
//!   only order that affects results.)
//! * **Loss window.** Records are buffered in user space and flushed on a time /
//!   count cadence; a separate fsync thread pushes OS buffers to disk about once
//!   a second. A crash therefore loses at most the last few seconds of commands —
//!   the accepted trade-off — and never a *prefix*: whatever is on disk replays
//!   to a consistent state.
//! * **Torn writes.** Every record ends with an FNV-1a checksum; replay stops at
//!   the first corrupt/truncated record instead of applying garbage.
//! * **Time replay.** Records carry a wall-clock nanosecond timestamp, so replay
//!   can stop at any point in time (`until_ts`).
//!
//! ## Record layout (little-endian, 64 bytes)
//!
//! | off | size | field                          |
//! |-----|------|--------------------------------|
//! | 0   | 8    | seq (per-shard, from 1)        |
//! | 8   | 8    | wall-clock timestamp (ns)      |
//! | 16  | 40   | wire frame ([`wire::MSG_LEN`]) |
//! | 56  | 8    | FNV-1a-64 over bytes 0..56     |

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::wire::MSG_LEN;

/// On-disk record size.
pub const RECORD_LEN: usize = 8 + 8 + MSG_LEN + 8;

/// FNV-1a 64-bit — tiny, dependency-free, adequate for torn-write detection
/// (not for adversarial integrity).
#[inline]
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Current wall clock in nanoseconds since the Unix epoch.
#[inline]
pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Appends command records to a journal file with buffered, cadenced flushing.
pub struct JournalWriter {
    w: BufWriter<File>,
    seq: u64,
    written_since_flush: usize,
    last_flush: Instant,
    flush_interval: Duration,
}

impl JournalWriter {
    /// Open (create/append) a journal at `path`. `flush_interval` bounds the
    /// user-space buffering delay; pair with [`spawn_fsyncer`] for OS-level
    /// durability cadence.
    pub fn open(path: &Path, flush_interval: Duration) -> io::Result<JournalWriter> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(JournalWriter {
            w: BufWriter::with_capacity(1 << 20, file),
            seq: 0,
            written_since_flush: 0,
            last_flush: Instant::now(),
            flush_interval,
        })
    }

    /// A clone of the underlying file handle, for an fsync thread.
    pub fn file_handle(&self) -> io::Result<File> {
        self.w.get_ref().try_clone()
    }

    /// The sequence number of the most recently appended record.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Resume the sequence counter after recovery. The journal seq is **the**
    /// total order that determines replay results, so across restarts it must
    /// stay strictly increasing: a writer restarting at 0 would append records
    /// whose seqs duplicate earlier ones, corrupting the
    /// `seq <= snapshot.journal_seq` skip filter on the next recovery.
    pub fn resume_from(&mut self, seq: u64) {
        debug_assert!(self.seq == 0, "resume_from is for freshly opened writers");
        self.seq = self.seq.max(seq);
    }

    /// Truncate the journal file to zero length. Call **only after** a snapshot
    /// covering every record has been durably written; the sequence counter
    /// keeps counting from where it was, so recovery's `seq > snapshot_seq`
    /// filter stays correct whether or not the truncation happened.
    pub fn truncate(&mut self) -> io::Result<()> {
        self.w.flush()?;
        let f = self.w.get_ref();
        f.set_len(0)?;
        f.sync_all()?;
        self.written_since_flush = 0;
        Ok(())
    }

    /// Append one command frame. Assigns and returns the record's `seq`.
    #[inline]
    pub fn append(&mut self, ts_nanos: u64, frame: &[u8; MSG_LEN]) -> io::Result<u64> {
        self.seq += 1;
        let mut rec = [0u8; RECORD_LEN];
        rec[0..8].copy_from_slice(&self.seq.to_le_bytes());
        rec[8..16].copy_from_slice(&ts_nanos.to_le_bytes());
        rec[16..16 + MSG_LEN].copy_from_slice(frame);
        let h = fnv1a(&rec[..RECORD_LEN - 8]);
        rec[RECORD_LEN - 8..].copy_from_slice(&h.to_le_bytes());
        self.w.write_all(&rec)?;

        self.written_since_flush += 1;
        if self.written_since_flush >= 8192 || self.last_flush.elapsed() >= self.flush_interval {
            self.flush()?;
        }
        Ok(self.seq)
    }

    /// Flush user-space buffer to the OS. Called automatically on cadence; call
    /// on idle to bound the loss window.
    pub fn flush(&mut self) -> io::Result<()> {
        self.w.flush()?;
        self.written_since_flush = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Flush if the cadence interval has elapsed (cheap to call in idle loops).
    pub fn tick(&mut self) -> io::Result<()> {
        if self.written_since_flush > 0 && self.last_flush.elapsed() >= self.flush_interval {
            self.flush()?;
        }
        Ok(())
    }
}

/// Spawn a background thread fsyncing `file` every `interval` until `running`
/// clears. Bounds the crash-loss window at roughly `interval`.
pub fn spawn_fsyncer(file: File, interval: Duration, running: Arc<AtomicBool>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("journal-fsync".into())
        .spawn(move || {
            while running.load(Ordering::Acquire) {
                std::thread::sleep(interval);
                let _ = file.sync_data();
            }
            let _ = file.sync_data();
        })
        .expect("spawn fsyncer")
}

/// One record read back from a journal.
#[derive(Clone, Copy, Debug)]
pub struct Record {
    pub seq: u64,
    pub ts_nanos: u64,
    pub frame: [u8; MSG_LEN],
}

/// Read a journal file, yielding records in order and stopping cleanly at the
/// first truncated or checksum-corrupt record (crash tolerance).
pub struct JournalReader {
    r: BufReader<File>,
}

impl JournalReader {
    pub fn open(path: &Path) -> io::Result<JournalReader> {
        Ok(JournalReader { r: BufReader::with_capacity(1 << 20, File::open(path)?) })
    }
}

impl Iterator for JournalReader {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        let mut rec = [0u8; RECORD_LEN];
        // read_exact returns Err on EOF/short read -> stop (truncated tail).
        self.r.read_exact(&mut rec).ok()?;
        let stored = u64::from_le_bytes(rec[RECORD_LEN - 8..].try_into().unwrap());
        if fnv1a(&rec[..RECORD_LEN - 8]) != stored {
            return None; // torn/corrupt record: stop at the consistent prefix
        }
        let mut frame = [0u8; MSG_LEN];
        frame.copy_from_slice(&rec[16..16 + MSG_LEN]);
        Some(Record {
            seq: u64::from_le_bytes(rec[0..8].try_into().unwrap()),
            ts_nanos: u64::from_le_bytes(rec[8..16].try_into().unwrap()),
            frame,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_truncation_tolerance() {
        let dir = std::env::temp_dir().join(format!("tc-journal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("j.bin");
        let _ = std::fs::remove_file(&path);

        let mut w = JournalWriter::open(&path, Duration::from_millis(1)).unwrap();
        for i in 0..10u8 {
            let mut frame = [0u8; MSG_LEN];
            frame[0] = 1;
            frame[8] = i; // vary the payload
            w.append(1000 + i as u64, &frame).unwrap();
        }
        w.flush().unwrap();
        drop(w);

        let recs: Vec<Record> = JournalReader::open(&path).unwrap().collect();
        assert_eq!(recs.len(), 10);
        assert_eq!(recs[0].seq, 1);
        assert_eq!(recs[9].seq, 10);
        assert_eq!(recs[3].frame[8], 3);
        assert_eq!(recs[3].ts_nanos, 1003);

        // Simulate a crash mid-write: truncate the file into the last record.
        let full = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 13).unwrap();
        drop(f);
        let recs: Vec<Record> = JournalReader::open(&path).unwrap().collect();
        assert_eq!(recs.len(), 9, "truncated tail must be dropped, prefix kept");

        std::fs::remove_dir_all(&dir).ok();
    }
}
