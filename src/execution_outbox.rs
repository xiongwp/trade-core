use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::exchange::ExecReport;
use crate::journal::fnv1a;
use crate::wire::{self, EXECUTION_EVENT_LEN, REPORT_LEN};

pub const OUTBOX_HEADER: [u8; 8] = *b"TCEX\x01\0\0\0";
pub const OUTBOX_RECORD_LEN: usize = 4 + 8 + 4 + REPORT_LEN + 8;
const CURSOR_HEADER: [u8; 8] = *b"TCEC\x01\0\0\0";
const CURSOR_LEN: usize = CURSOR_HEADER.len() + 8 + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxRecord {
    pub raft_group: u32,
    pub raft_index: u64,
    pub ordinal: u32,
    pub report_frame: [u8; REPORT_LEN],
}

impl OutboxRecord {
    pub fn kafka_key(&self, _category_size: u32) -> [u8; 8] {
        let report = wire::decode_report(&self.report_frame).expect("valid outbox report");
        report.order_id.0.to_be_bytes()
    }

    pub fn kafka_payload(&self) -> [u8; EXECUTION_EVENT_LEN] {
        let report = wire::decode_report(&self.report_frame).expect("valid outbox report");
        let mut payload = [0u8; EXECUTION_EVENT_LEN];
        payload[..4].copy_from_slice(b"EX01");
        payload[4..8].copy_from_slice(&1u32.to_le_bytes());
        payload[8..12].copy_from_slice(&self.raft_group.to_le_bytes());
        payload[16..24].copy_from_slice(&self.raft_index.to_le_bytes());
        payload[24..28].copy_from_slice(&self.ordinal.to_le_bytes());
        payload[32..32 + REPORT_LEN].copy_from_slice(&self.report_frame);
        debug_assert_eq!(
            wire::decode_execution_event(&payload).unwrap().report,
            report
        );
        payload
    }
}

pub struct ExecutionOutboxWriter {
    writer: BufWriter<File>,
    /// Path of the segment currently being appended.
    path: PathBuf,
    /// The base path (`outbox-shard-N.bin`); rotated segments derive their
    /// names from it.
    base: PathBuf,
    next_segment_seq: u64,
    /// Logical size of the current segment (header + records written).
    segment_bytes: u64,
    /// Rotate to a fresh segment once the current one reaches this size.
    /// `None` disables rotation (single-file behavior).
    rotate_bytes: Option<u64>,
    flush_interval: Duration,
    last_flush: Instant,
    written_since_flush: usize,
    sync_every: usize,
}

/// The path of rotation segment `seq` for a given base outbox path. Segment 0
/// is the base path itself. The naming (`outbox-shard-N-seg-…​.bin`) is chosen
/// so the publisher's directory scan (`outbox-shard-` prefix + `.bin`
/// extension) discovers new segments automatically, each with its own
/// `.published.cursor`.
fn segment_path(base: &Path, seq: u64) -> PathBuf {
    if seq == 0 {
        return base.to_path_buf();
    }
    let stem = base
        .file_stem()
        .and_then(|stem| stem.to_str())
        .expect("outbox base path has a utf-8 file stem");
    base.with_file_name(format!("{stem}-seg-{seq:010}.bin"))
}

/// Parse the segment sequence out of a path produced by [`segment_path`].
fn segment_seq_of(base: &Path, path: &Path) -> Option<u64> {
    if path == base {
        return Some(0);
    }
    let base_stem = base.file_stem()?.to_str()?;
    let stem = path.file_stem()?.to_str()?;
    if path.extension()?.to_str()? != "bin" || path.parent() != base.parent() {
        return None;
    }
    stem.strip_prefix(base_stem)?
        .strip_prefix("-seg-")?
        .parse()
        .ok()
}

/// Every existing segment of an outbox, ascending by segment sequence (oldest
/// records first). The base file, when present, is always the oldest.
pub fn segment_paths(base: &Path) -> io::Result<Vec<PathBuf>> {
    let Some(parent) = base.parent() else {
        return Ok(if base.exists() {
            vec![base.to_path_buf()]
        } else {
            Vec::new()
        });
    };
    let mut segments = Vec::new();
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let path = entry?.path();
        if let Some(seq) = segment_seq_of(base, &path) {
            segments.push((seq, path));
        }
    }
    segments.sort_by_key(|(seq, _)| *seq);
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

/// The newest segment of an outbox — where recovery trimming and appending
/// happen. Falls back to the (possibly not-yet-created) base path.
pub fn latest_segment(base: &Path) -> io::Result<PathBuf> {
    Ok(segment_paths(base)?
        .pop()
        .unwrap_or_else(|| base.to_path_buf()))
}

/// Create-or-validate one segment file and return an append handle plus its
/// current logical length.
fn open_segment(path: &Path) -> io::Result<(File, u64)> {
    let mut file = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(path)?;
    let mut len = file.metadata()?.len();
    if len == 0 {
        file.write_all(&OUTBOX_HEADER)?;
        file.sync_data()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        len = OUTBOX_HEADER.len() as u64;
    } else {
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; OUTBOX_HEADER.len()];
        file.read_exact(&mut header)?;
        if header != OUTBOX_HEADER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "execution outbox header/version mismatch in {}",
                    path.display()
                ),
            ));
        }
    }
    Ok((OpenOptions::new().append(true).open(path)?, len))
}

impl ExecutionOutboxWriter {
    /// Open (resuming the newest rotation segment, if any). Rotation is off by
    /// default; enable it with [`with_rotate_bytes`](Self::with_rotate_bytes).
    pub fn open(
        path: &Path,
        flush_interval: Duration,
        sync_every: usize,
    ) -> io::Result<ExecutionOutboxWriter> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let base = path.to_path_buf();
        let current = latest_segment(&base)?;
        let current_seq = segment_seq_of(&base, &current).unwrap_or(0);
        let (file, len) = open_segment(&current)?;
        Ok(ExecutionOutboxWriter {
            writer: BufWriter::with_capacity(64 * 1024, file),
            path: current,
            base,
            next_segment_seq: current_seq + 1,
            segment_bytes: len,
            rotate_bytes: None,
            flush_interval,
            last_flush: Instant::now(),
            written_since_flush: 0,
            sync_every: sync_every.max(1),
        })
    }

    /// Enable size-based segment rotation. `None` or `Some(0)` disables it.
    pub fn with_rotate_bytes(mut self, rotate_bytes: Option<u64>) -> Self {
        self.rotate_bytes = rotate_bytes.filter(|bytes| *bytes > 0);
        self
    }

    /// Rotate to a fresh segment when the current one has reached the
    /// configured size, then garbage-collect older segments the publisher has
    /// fully acknowledged. Must be called only at a batch boundary **after**
    /// the application watermark has been persisted, so every record in a
    /// sealed (non-newest) segment is watermark-covered and recovery trimming
    /// only ever needs to examine the newest segment.
    ///
    /// Returns `true` if a rotation happened.
    pub fn maybe_rotate(&mut self) -> io::Result<bool> {
        let Some(limit) = self.rotate_bytes else {
            return Ok(false);
        };
        if self.segment_bytes < limit {
            return Ok(false);
        }
        // Seal the old segment: everything buffered must be durable before the
        // writer moves on (normally a no-op — the group-commit barrier already
        // synced it).
        if self.written_since_flush > 0 {
            self.sync_data()?;
        }
        let next = segment_path(&self.base, self.next_segment_seq);
        let (file, len) = open_segment(&next)?;
        self.writer = BufWriter::with_capacity(64 * 1024, file);
        self.path = next;
        self.next_segment_seq += 1;
        self.segment_bytes = len;
        // Best-effort space reclamation; failure must not take down matching.
        if let Err(error) = self.remove_published_segments() {
            crate::log_warn!(
                "execution-outbox",
                "event=segment_gc_failed base={} error={error}",
                self.base.display()
            );
        }
        Ok(true)
    }

    /// Delete every sealed segment whose publisher cursor shows all of its
    /// records were acknowledged by the broker. The cursor is only advanced
    /// after a whole publish batch succeeded and is persisted atomically, so a
    /// cursor equal to the segment length proves the segment is fully
    /// published and safe to remove.
    fn remove_published_segments(&self) -> io::Result<usize> {
        let mut removed = 0;
        for path in segment_paths(&self.base)? {
            if path == self.path {
                continue; // the active segment is never collected
            }
            let cursor_path = path.with_extension("published.cursor");
            let Some(cursor) = read_cursor_offset(&cursor_path)? else {
                continue; // never published (or cursor unreadable): keep
            };
            let len = match std::fs::metadata(&path) {
                Ok(meta) => meta.len(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if cursor < len {
                continue; // records still pending publication
            }
            std::fs::remove_file(&path)?;
            std::fs::remove_file(&cursor_path).ok();
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            removed += 1;
        }
        Ok(removed)
    }

    /// Path of the segment currently being appended (the newest one).
    pub fn current_segment(&self) -> &Path {
        &self.path
    }

    pub fn append(
        &mut self,
        raft_group: u32,
        raft_index: u64,
        ordinal: u32,
        report: &ExecReport,
    ) -> io::Result<()> {
        let mut frame = [0u8; REPORT_LEN];
        wire::encode_report(report, &mut frame);
        let record = OutboxRecord {
            raft_group,
            raft_index,
            ordinal,
            report_frame: frame,
        };
        write_record(&mut self.writer, &record)?;
        self.segment_bytes += OUTBOX_RECORD_LEN as u64;
        self.written_since_flush += 1;
        if self.written_since_flush >= self.sync_every
            || self.last_flush.elapsed() >= self.flush_interval
        {
            self.sync_data()?;
        }
        Ok(())
    }

    /// Append one record without forcing a durability barrier. The caller is
    /// responsible for issuing exactly one [`sync_data`](Self::sync_data) per
    /// group-commit batch, so a whole Raft batch shares a single fsync instead
    /// of paying one fsync per execution event (the dominant apply-pipeline
    /// cost). Crash consistency is preserved because the caller folds this
    /// sync into the same barrier as the command WAL and only advances the
    /// application watermark afterwards.
    pub fn append_deferred(
        &mut self,
        raft_group: u32,
        raft_index: u64,
        ordinal: u32,
        report: &ExecReport,
    ) -> io::Result<()> {
        let mut frame = [0u8; REPORT_LEN];
        wire::encode_report(report, &mut frame);
        let record = OutboxRecord {
            raft_group,
            raft_index,
            ordinal,
            report_frame: frame,
        };
        write_record(&mut self.writer, &record)?;
        self.segment_bytes += OUTBOX_RECORD_LEN as u64;
        self.written_since_flush += 1;
        Ok(())
    }

    pub fn sync_data(&mut self) -> io::Result<()> {
        self.flush_to_os()?;
        self.writer.get_ref().sync_data()?;
        self.mark_synced();
        Ok(())
    }

    /// Move buffered outbox records into the kernel before a filesystem-wide
    /// group-commit barrier. On Linux the asset WAL coordinator then uses one
    /// `syncfs` for the WAL, shard journal and this outbox together.
    pub fn flush_to_os(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Record that an external filesystem-wide barrier made the flushed
    /// records durable. This only updates batching bookkeeping; callers must
    /// invoke it strictly after a successful `syncfs`/equivalent barrier.
    pub fn mark_synced(&mut self) {
        self.written_since_flush = 0;
        self.last_flush = Instant::now();
    }
}

/// Trim the outbox on recovery so it ends exactly at the last Raft batch whose
/// application watermark was durably persisted.
///
/// Records are appended in ascending `(raft_index, ordinal)` order and a
/// batch's records are fsynced (see [`ExecutionOutboxWriter::sync_data`])
/// *before* its watermark is written, so:
///   * every record for a watermarked batch is guaranteed present, and
///   * any record beyond `max_applied_index` belongs to a batch that recovery
///     re-applies and re-appends.
///
/// Dropping that tail makes replay exactly-once at the outbox: no execution
/// event is lost (the watermark can only advance after the record is durable)
/// and none is duplicated (re-applied batches start from a clean tail). A
/// torn trailing record left by a crash mid-append is also discarded.
pub fn truncate_after_applied(path: &Path, max_applied_index: Option<u64>) -> io::Result<()> {
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let len = file.metadata()?.len();
    if len < OUTBOX_HEADER.len() as u64 {
        return Ok(());
    }
    let mut header = [0u8; OUTBOX_HEADER.len()];
    file.read_exact(&mut header)?;
    if header != OUTBOX_HEADER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "execution outbox header/version mismatch",
        ));
    }
    let mut reader = BufReader::new(&file);
    let mut keep = OUTBOX_HEADER.len() as u64;
    loop {
        let mut bytes = [0u8; OUTBOX_RECORD_LEN];
        match reader.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let expected =
            u64::from_le_bytes(bytes[OUTBOX_RECORD_LEN - 8..].try_into().expect("checksum"));
        if fnv1a(&bytes[..OUTBOX_RECORD_LEN - 8]) != expected {
            break; // torn/partial trailing record: discard from here on
        }
        let raft_index = u64::from_le_bytes(bytes[4..12].try_into().expect("index"));
        match max_applied_index {
            Some(max) if raft_index <= max => keep += OUTBOX_RECORD_LEN as u64,
            _ => break, // first record past the durable watermark
        }
    }
    drop(reader);
    if keep < len {
        file.set_len(keep)?;
        file.sync_all()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
    }
    Ok(())
}

fn write_record(mut writer: impl Write, record: &OutboxRecord) -> io::Result<()> {
    let mut bytes = [0u8; OUTBOX_RECORD_LEN];
    bytes[0..4].copy_from_slice(&record.raft_group.to_le_bytes());
    bytes[4..12].copy_from_slice(&record.raft_index.to_le_bytes());
    bytes[12..16].copy_from_slice(&record.ordinal.to_le_bytes());
    bytes[16..16 + REPORT_LEN].copy_from_slice(&record.report_frame);
    let checksum = fnv1a(&bytes[..OUTBOX_RECORD_LEN - 8]);
    bytes[OUTBOX_RECORD_LEN - 8..].copy_from_slice(&checksum.to_le_bytes());
    writer.write_all(&bytes)
}

pub struct ExecutionOutboxReader {
    path: PathBuf,
    cursor_path: Option<PathBuf>,
    offset: u64,
}

impl ExecutionOutboxReader {
    pub fn open(path: PathBuf) -> io::Result<ExecutionOutboxReader> {
        let mut file = File::open(&path)?;
        let mut header = [0u8; OUTBOX_HEADER.len()];
        file.read_exact(&mut header)?;
        if header != OUTBOX_HEADER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "execution outbox header/version mismatch",
            ));
        }
        Ok(ExecutionOutboxReader {
            path,
            cursor_path: None,
            offset: OUTBOX_HEADER.len() as u64,
        })
    }

    /// Open a publisher reader at its last broker-acknowledged offset. The
    /// cursor is advanced atomically only after an entire publish batch has
    /// succeeded, so a crash can duplicate a batch but can never skip one.
    pub fn open_with_cursor(path: PathBuf, cursor_path: PathBuf) -> io::Result<Self> {
        let mut reader = Self::open(path)?;
        reader.cursor_path = Some(cursor_path.clone());
        if cursor_path.exists() {
            let offset = read_cursor_offset(&cursor_path)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution outbox cursor header/version mismatch",
                )
            })?;
            if offset < OUTBOX_HEADER.len() as u64
                || (offset - OUTBOX_HEADER.len() as u64) % OUTBOX_RECORD_LEN as u64 != 0
                || offset > std::fs::metadata(&reader.path)?.len()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution outbox cursor is corrupt or ahead of WAL",
                ));
            }
            reader.offset = offset;
        }
        Ok(reader)
    }

    pub fn read_batch(&self, max_records: usize) -> io::Result<Vec<OutboxRecord>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            // A fully published rotation segment may have been garbage
            // collected by the writer: nothing left to read is not an error.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;
        let mut records = Vec::with_capacity(max_records.min(1024));
        while records.len() < max_records {
            let mut bytes = [0u8; OUTBOX_RECORD_LEN];
            match reader.read_exact(&mut bytes) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            let expected =
                u64::from_le_bytes(bytes[OUTBOX_RECORD_LEN - 8..].try_into().expect("checksum"));
            if fnv1a(&bytes[..OUTBOX_RECORD_LEN - 8]) != expected {
                break;
            }
            let mut frame = [0u8; REPORT_LEN];
            frame.copy_from_slice(&bytes[16..16 + REPORT_LEN]);
            records.push(OutboxRecord {
                raft_group: u32::from_le_bytes(bytes[0..4].try_into().expect("group")),
                raft_index: u64::from_le_bytes(bytes[4..12].try_into().expect("index")),
                ordinal: u32::from_le_bytes(bytes[12..16].try_into().expect("ordinal")),
                report_frame: frame,
            });
        }
        Ok(records)
    }

    pub fn acknowledge(&mut self, records: usize) -> io::Result<()> {
        if records == 0 {
            return Ok(());
        }
        let advance = (records as u64)
            .checked_mul(OUTBOX_RECORD_LEN as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cursor overflow"))?;
        let next = self
            .offset
            .checked_add(advance)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cursor overflow"))?;
        if next > std::fs::metadata(&self.path)?.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot acknowledge records beyond outbox end",
            ));
        }
        if let Some(path) = &self.cursor_path {
            persist_cursor(path, next)?;
        }
        self.offset = next;
        Ok(())
    }

    pub fn pending_records(&self) -> io::Result<u64> {
        let len = match std::fs::metadata(&self.path) {
            Ok(meta) => meta.len(),
            // Garbage-collected segment: it was fully published, so nothing
            // is pending.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };
        Ok(len.saturating_sub(self.offset) / OUTBOX_RECORD_LEN as u64)
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn read_available(&mut self, mut emit: impl FnMut(OutboxRecord)) -> io::Result<usize> {
        let records = self.read_batch(usize::MAX)?;
        for record in records.iter().copied() {
            emit(record);
        }
        self.acknowledge(records.len())?;
        Ok(records.len())
    }
}

/// Read a publisher cursor file, returning `Ok(None)` when it does not exist
/// or fails validation (header/length/checksum) — callers treating the cursor
/// as an optimization (segment GC) must not fail hard on a corrupt cursor.
fn read_cursor_offset(path: &Path) -> io::Result<Option<u64>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if bytes.len() != CURSOR_LEN || bytes[..CURSOR_HEADER.len()] != CURSOR_HEADER {
        return Ok(None);
    }
    let offset = u64::from_le_bytes(
        bytes[CURSOR_HEADER.len()..CURSOR_HEADER.len() + 8]
            .try_into()
            .expect("cursor offset"),
    );
    let checksum = u64::from_le_bytes(
        bytes[CURSOR_HEADER.len() + 8..]
            .try_into()
            .expect("cursor checksum"),
    );
    if fnv1a(&bytes[..CURSOR_HEADER.len() + 8]) != checksum {
        return Ok(None);
    }
    Ok(Some(offset))
}

fn persist_cursor(path: &Path, offset: u64) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut bytes = [0u8; CURSOR_LEN];
    bytes[..CURSOR_HEADER.len()].copy_from_slice(&CURSOR_HEADER);
    bytes[CURSOR_HEADER.len()..CURSOR_HEADER.len() + 8].copy_from_slice(&offset.to_le_bytes());
    let checksum = fnv1a(&bytes[..CURSOR_HEADER.len() + 8]);
    bytes[CURSOR_HEADER.len() + 8..].copy_from_slice(&checksum.to_le_bytes());
    let temp = path.with_extension("cursor.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InstrumentId, OrderId};

    #[test]
    fn outbox_persists_records_and_rebuilds_kafka_payloads() {
        let root = std::env::temp_dir().join(format!(
            "tc-execution-outbox-{}",
            crate::journal::now_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("outbox-shard-0.bin");
        {
            let mut writer =
                ExecutionOutboxWriter::open(&path, Duration::from_secs(60), 1).unwrap();
            writer
                .append(
                    3,
                    9,
                    2,
                    &ExecReport::Accepted {
                        instrument: InstrumentId(42),
                        order_id: OrderId(7),
                    },
                )
                .unwrap();
        }

        let mut reader = ExecutionOutboxReader::open(path).unwrap();
        let mut got = Vec::new();
        assert_eq!(reader.read_available(|record| got.push(record)).unwrap(), 1);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].raft_group, 3);
        assert_eq!(got[0].raft_index, 9);
        assert_eq!(got[0].ordinal, 2);
        assert_eq!(got[0].kafka_key(10), 7u64.to_be_bytes());
        let event = wire::decode_execution_event(&got[0].kafka_payload()).unwrap();
        assert_eq!(event.raft_group, 3);
        assert_eq!(event.raft_index, 9);
        assert_eq!(event.ordinal, 2);
        assert_eq!(event.report.instrument, InstrumentId(42));
        assert_eq!(event.report.order_id, OrderId(7));

        std::fs::remove_dir_all(root).ok();
    }

    // ---- P0-1 crash-order recovery contract ---------------------------------

    fn write_batches(path: &Path, batches: &[(u64, u32)]) {
        // batches: (raft_index, record_count) written in ascending index order,
        // each fsynced as its own group-commit barrier (as handle_committed_batch
        // does before advancing the watermark).
        let mut writer = ExecutionOutboxWriter::open(path, Duration::from_secs(60), 1).unwrap();
        for &(index, count) in batches {
            for ordinal in 0..count {
                writer
                    .append_deferred(
                        7,
                        index,
                        ordinal,
                        &ExecReport::Accepted {
                            instrument: InstrumentId(index as u32),
                            order_id: OrderId(ordinal as u64),
                        },
                    )
                    .unwrap();
            }
            writer.sync_data().unwrap();
        }
    }

    fn read_indices(path: &Path) -> Vec<u64> {
        let mut reader = ExecutionOutboxReader::open(path.to_path_buf()).unwrap();
        let mut indices = Vec::new();
        reader
            .read_available(|record| indices.push(record.raft_index))
            .unwrap();
        indices
    }

    #[test]
    fn rotation_seals_segments_and_gc_reclaims_fully_published_ones() {
        let root = std::env::temp_dir()
            .join(format!("tc-outbox-rotate-{}", crate::journal::now_nanos()));
        std::fs::create_dir_all(&root).unwrap();
        let base = root.join("outbox-shard-0.bin");
        // Rotate once a segment holds two records.
        let rotate = (OUTBOX_HEADER.len() + 2 * OUTBOX_RECORD_LEN) as u64;
        let report = |index: u64| ExecReport::Accepted {
            instrument: InstrumentId(index as u32),
            order_id: OrderId(index),
        };

        let mut writer = ExecutionOutboxWriter::open(&base, Duration::from_secs(60), usize::MAX)
            .unwrap()
            .with_rotate_bytes(Some(rotate));
        // Batch 1 (two records) fills the base segment; after its (simulated)
        // watermark advance the writer rotates to a fresh segment.
        writer.append_deferred(1, 1, 0, &report(1)).unwrap();
        writer.append_deferred(1, 1, 1, &report(1)).unwrap();
        writer.sync_data().unwrap();
        assert!(writer.maybe_rotate().unwrap());
        let seg1 = writer.current_segment().to_path_buf();
        assert_ne!(seg1, base);
        assert_eq!(segment_paths(&base).unwrap(), vec![base.clone(), seg1.clone()]);
        // Segment names must be discoverable by the publisher's scan rules.
        let stem = seg1.file_stem().unwrap().to_str().unwrap();
        assert!(stem.starts_with("outbox-shard-"));
        assert_eq!(seg1.extension().unwrap(), "bin");

        // Not-yet-published sealed segments survive rotation.
        writer.append_deferred(1, 2, 0, &report(2)).unwrap();
        writer.append_deferred(1, 2, 1, &report(2)).unwrap();
        writer.sync_data().unwrap();
        assert!(writer.maybe_rotate().unwrap());
        assert!(base.exists(), "unpublished sealed segment must be retained");

        // Publish the base segment fully (cursor == len); the next rotation
        // garbage-collects it and only it.
        let cursor = base.with_extension("published.cursor");
        let mut reader =
            ExecutionOutboxReader::open_with_cursor(base.clone(), cursor.clone()).unwrap();
        let mut published = Vec::new();
        reader
            .read_available(|record| published.push((record.raft_index, record.ordinal)))
            .unwrap();
        assert_eq!(published, vec![(1, 0), (1, 1)]);
        writer.append_deferred(1, 3, 0, &report(3)).unwrap();
        writer.append_deferred(1, 3, 1, &report(3)).unwrap();
        writer.sync_data().unwrap();
        assert!(writer.maybe_rotate().unwrap());
        assert!(!base.exists(), "fully published segment must be reclaimed");
        assert!(!cursor.exists(), "its cursor is removed with it");
        assert!(seg1.exists(), "unpublished segment is still retained");

        // A deleted segment reads as empty (not an error) for a stale reader.
        assert!(reader.read_batch(10).unwrap().is_empty());
        assert_eq!(reader.pending_records().unwrap(), 0);

        // Records in surviving segments are intact, and reopening resumes the
        // newest segment.
        let mut seg1_reader = ExecutionOutboxReader::open(seg1.clone()).unwrap();
        let mut seg1_records = Vec::new();
        seg1_reader
            .read_available(|record| seg1_records.push(record.raft_index))
            .unwrap();
        assert_eq!(seg1_records, vec![2, 2]);
        let newest = writer.current_segment().to_path_buf();
        drop(writer);
        let reopened =
            ExecutionOutboxWriter::open(&base, Duration::from_secs(60), usize::MAX).unwrap();
        assert_eq!(reopened.current_segment(), newest.as_path());
        assert_eq!(latest_segment(&base).unwrap(), newest);

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn recovery_drops_reports_past_the_durable_watermark() {
        // Crash after outbox append+fsync of batch 3 but before its watermark
        // was persisted: the durable max applied index is 2. Recovery must trim
        // batch 3 so its re-application does not duplicate execution events.
        let root = std::env::temp_dir()
            .join(format!("tc-outbox-trim-{}", crate::journal::now_nanos()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("outbox-shard-0.bin");
        write_batches(&path, &[(1, 1), (2, 2), (3, 2)]);
        assert_eq!(read_indices(&path), vec![1, 2, 2, 3, 3]);

        truncate_after_applied(&path, Some(2)).unwrap();

        // Batch 3 is gone; batches 1 and 2 (watermark-covered) are intact, so
        // replay re-appends batch 3 exactly once: no loss, no duplication.
        assert_eq!(read_indices(&path), vec![1, 2, 2]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn recovery_keeps_every_watermarked_batch() {
        // Watermark reached batch 3, so batch 3's reports must already be on
        // disk (they were fsynced before the watermark advanced): trimming to
        // the same index removes nothing.
        let root = std::env::temp_dir()
            .join(format!("tc-outbox-keep-{}", crate::journal::now_nanos()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("outbox-shard-0.bin");
        write_batches(&path, &[(1, 1), (2, 2), (3, 2)]);

        truncate_after_applied(&path, Some(3)).unwrap();

        assert_eq!(read_indices(&path), vec![1, 2, 2, 3, 3]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn recovery_with_no_watermark_and_torn_tail_trims_cleanly() {
        let root = std::env::temp_dir()
            .join(format!("tc-outbox-torn-{}", crate::journal::now_nanos()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("outbox-shard-0.bin");
        write_batches(&path, &[(1, 2)]);

        // Simulate a crash mid-append: a partial (torn) record at the tail.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0xAB; OUTBOX_RECORD_LEN / 2]).unwrap();
            file.sync_data().unwrap();
        }

        // No batch was ever watermarked: everything (including the torn tail)
        // is trimmed back to an empty, header-only outbox.
        truncate_after_applied(&path, None).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            OUTBOX_HEADER.len() as u64
        );
        assert!(read_indices(&path).is_empty());

        // The trimmed file is a valid, appendable outbox again.
        write_batches(&path, &[(5, 1)]);
        assert_eq!(read_indices(&path), vec![5]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn publisher_cursor_only_advances_acknowledged_batches() {
        let root = std::env::temp_dir().join(format!(
            "tc-execution-cursor-{}",
            crate::journal::now_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("outbox-shard-0.bin");
        let cursor = root.join("outbox-shard-0.published.cursor");
        {
            let mut writer =
                ExecutionOutboxWriter::open(&path, Duration::from_secs(60), 1).unwrap();
            for index in 1..=3 {
                writer
                    .append(
                        2,
                        index,
                        0,
                        &ExecReport::Accepted {
                            instrument: InstrumentId(5),
                            order_id: OrderId(index),
                        },
                    )
                    .unwrap();
            }
        }

        let mut reader =
            ExecutionOutboxReader::open_with_cursor(path.clone(), cursor.clone()).unwrap();
        assert_eq!(reader.pending_records().unwrap(), 3);
        assert_eq!(reader.read_batch(2).unwrap().len(), 2);
        assert_eq!(reader.pending_records().unwrap(), 3);
        reader.acknowledge(2).unwrap();
        assert_eq!(reader.pending_records().unwrap(), 1);
        drop(reader);

        let mut restored = ExecutionOutboxReader::open_with_cursor(path, cursor).unwrap();
        let tail = restored.read_batch(10).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].raft_index, 3);
        restored.acknowledge(1).unwrap();
        assert_eq!(restored.pending_records().unwrap(), 0);

        std::fs::remove_dir_all(root).ok();
    }
}
