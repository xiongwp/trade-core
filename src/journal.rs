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

/// File header: magic + format version. Written on create/truncate, verified
/// on open — future format changes bump the version instead of silently
/// misparsing old files (migration tooling hooks in here).
pub const JOURNAL_HEADER: [u8; 8] = *b"TCJR\x00\x00\x00";

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
        Self::open_with_capacity(path, flush_interval, 1 << 20)
    }

    /// Open a journal with an explicit userspace buffer size. Per-asset WALs
    /// use a small buffer because a node can own thousands of sparse assets;
    /// allocating the shard-journal default for every open asset wastes memory.
    pub fn open_with_capacity(
        path: &Path,
        flush_interval: Duration,
        buffer_bytes: usize,
    ) -> io::Result<JournalWriter> {
        let mut probe = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        let len = probe.metadata()?.len();
        if len == 0 {
            probe.write_all(&JOURNAL_HEADER)?;
            probe.sync_data()?;
        } else {
            let mut hdr = [0u8; 8];
            probe.read_exact(&mut hdr)?;
            if hdr != JOURNAL_HEADER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal header/version mismatch (migration required)",
                ));
            }
        }
        let file = OpenOptions::new().append(true).open(path)?;
        if let Some(bytes) = std::env::var("TC_WAL_PREALLOCATE_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|bytes| *bytes > len)
        {
            preallocate_keep_size(&file, bytes)?;
        }
        Ok(JournalWriter {
            w: BufWriter::with_capacity(buffer_bytes.max(RECORD_LEN), file),
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
        // Re-stamp the header so the truncated file stays a valid v2 journal.
        self.w.write_all(&JOURNAL_HEADER)?;
        self.w.flush()?;
        self.w.get_ref().sync_all()?;
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

    /// Make every appended record durable before an external commit checkpoint
    /// is advanced. Raft dispatch uses this to avoid acknowledging an applied
    /// index whose command is still only in a userspace buffer.
    pub fn sync_data(&mut self) -> io::Result<()> {
        self.w.flush()?;
        self.w.get_ref().sync_data()?;
        self.written_since_flush = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Flush buffered records from userspace without issuing a durability
    /// barrier. A caller coordinating several WAL files can flush all of them
    /// first and then use [`Self::sync_filesystem`] for one group commit.
    pub fn flush_to_os(&mut self) -> io::Result<()> {
        self.w.flush()?;
        self.written_since_flush = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Make all pending writes on this WAL's filesystem durable. On Linux this
    /// is one `syncfs(2)` call, allowing a batch spread across many per-asset
    /// files to share one storage barrier.
    pub fn sync_filesystem(&self) -> io::Result<()> {
        sync_filesystem(self.w.get_ref())
    }
}

#[cfg(target_os = "linux")]
fn sync_filesystem(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    if unsafe { libc::syncfs(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_filesystem(file: &File) -> io::Result<()> {
    file.sync_data()
}

#[cfg(target_os = "linux")]
fn preallocate_keep_size(file: &File, bytes: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let result = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_KEEP_SIZE,
            0,
            bytes as libc::off_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn preallocate_keep_size(_file: &File, _bytes: u64) -> io::Result<()> {
    // KEEP_SIZE allocation is Linux-specific in the Docker runtime. Other
    // platforms retain buffered append semantics without changing logical EOF.
    Ok(())
}

/// Spawn a background thread fsyncing `file` every `interval` until `running`
/// clears. Bounds the crash-loss window at roughly `interval`.
pub fn spawn_fsyncer(
    file: File,
    interval: Duration,
    running: Arc<AtomicBool>,
    metrics: Arc<crate::metrics::Metrics>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("journal-fsync".into())
        .spawn(move || {
            // A failing durability barrier must not be silent: the advertised
            // "crash loses at most ~interval" guarantee is void while fsync
            // errors persist (ENOSPC/EIO). Count every failure, log the first
            // and every 60th thereafter to avoid flooding, and log recovery.
            let mut consecutive_failures: u64 = 0;
            let slow = crate::oblog::slow_fsync_threshold();
            let mut sync = |file: &File| {
                let started = Instant::now();
                match file.sync_data() {
                    Ok(()) => {
                        let elapsed = started.elapsed();
                        if consecutive_failures > 0 {
                            crate::log_info!(
                                "journal-fsync",
                                "event=fsync_recovered after_failures={consecutive_failures}"
                            );
                        }
                        consecutive_failures = 0;
                        if elapsed >= slow {
                            crate::log_warn!(
                                "journal-fsync",
                                "event=slow_fsync elapsed_ms={:.3} threshold_ms={:.3}",
                                elapsed.as_secs_f64() * 1e3,
                                slow.as_secs_f64() * 1e3
                            );
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        metrics.asset_wal_errors.fetch_add(1, Ordering::Relaxed);
                        if consecutive_failures == 1 || consecutive_failures % 60 == 0 {
                            crate::log_error!(
                                "journal-fsync",
                                "event=fsync_failed consecutive={consecutive_failures} retry_in={interval:?} error={e} — durability window is growing"
                            );
                        }
                    }
                }
            };
            while running.load(Ordering::Acquire) {
                std::thread::sleep(interval);
                sync(&file);
            }
            sync(&file);
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
        let mut r = BufReader::with_capacity(1 << 20, File::open(path)?);
        let mut hdr = [0u8; 8];
        match r.read_exact(&mut hdr) {
            Ok(()) if hdr == JOURNAL_HEADER => {}
            Ok(()) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal header/version mismatch (migration required)",
                ))
            }
            Err(_) => {} // empty/short file: yields no records
        }
        Ok(JournalReader { r })
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
