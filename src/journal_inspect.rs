//! Offline inspection of durable journal and execution-outbox files.
//!
//! The matching node writes two kinds of append-only, checksummed files: the
//! command **journal** (`journal::JournalReader`, one `Record` per command
//! frame) and the execution **outbox** (`execution_outbox`, one `OutboxRecord`
//! per emitted execution report). When a replica diverges or a recovery looks
//! wrong, an operator needs to read those files back without a running node.
//!
//! This module holds the reusable, side-effect-free core (decode + filter +
//! compare, writing to any [`Write`]); the `journal-inspect` binary is a thin
//! argument-parsing shell over it. Everything here reuses the same
//! `JournalReader`/`ExecutionOutboxReader` decoders the node uses, so the human
//! view can never drift from the on-disk format.

use std::io::{self, Write};
use std::path::Path;

use crate::execution_outbox::{ExecutionOutboxReader, OutboxRecord};
use crate::journal::{JournalReader, Record};
use crate::wire;

/// Inclusive record filter shared by the journal `dump` sub-command. A `None`
/// bound is open-ended.
#[derive(Clone, Copy, Debug, Default)]
pub struct DumpFilter {
    pub from_seq: Option<u64>,
    pub to_seq: Option<u64>,
    pub from_ts: Option<u64>,
    pub to_ts: Option<u64>,
}

impl DumpFilter {
    fn accepts(&self, seq: u64, ts_nanos: u64) -> bool {
        self.from_seq.map_or(true, |v| seq >= v)
            && self.to_seq.map_or(true, |v| seq <= v)
            && self.from_ts.map_or(true, |v| ts_nanos >= v)
            && self.to_ts.map_or(true, |v| ts_nanos <= v)
    }
}

/// One human-readable line for a journal record: sequence, timestamp, and the
/// decoded command (or a marker if the frame cannot be decoded — the reader
/// already guards checksums, so this only happens on a format change).
pub fn format_record(record: &Record) -> String {
    match wire::decode_raft_entry(&record.frame).and_then(|mut c| c.pop()) {
        Some(command) => format!(
            "seq={} ts_nanos={} {:?}",
            record.seq, record.ts_nanos, command
        ),
        None => format!(
            "seq={} ts_nanos={} <undecodable frame>",
            record.seq, record.ts_nanos
        ),
    }
}

/// Dump every journal record matching `filter` to `out`, one per line. Returns
/// the number of records written. Stops cleanly at the first torn/corrupt
/// record (inherited from [`JournalReader`]).
pub fn dump_journal(path: &Path, filter: &DumpFilter, out: &mut impl Write) -> io::Result<usize> {
    let reader = JournalReader::open(path)?;
    let mut written = 0;
    for record in reader {
        if filter.accepts(record.seq, record.ts_nanos) {
            writeln!(out, "{}", format_record(&record))?;
            written += 1;
        }
    }
    Ok(written)
}

/// Result of verifying a journal's on-disk integrity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyReport {
    /// Records read before EOF or the first corruption.
    pub records: u64,
    pub first_seq: Option<u64>,
    /// Sequence of the last record that read back valid. This is the safe
    /// replay head — everything after it is torn or missing.
    pub last_valid_seq: Option<u64>,
    /// True when sequences increase by exactly 1 with no gap.
    pub contiguous: bool,
    /// Sequence after which the first gap appears (only set when not
    /// contiguous).
    pub first_gap_after: Option<u64>,
}

/// Read a journal to its first inconsistency, reporting the last valid sequence
/// and whether the sequence numbers are gap-free. [`JournalReader`] stops at the
/// first torn/corrupt record, so a short `records` count relative to the file
/// size means the tail was truncated (a normal post-crash state).
pub fn verify_journal(path: &Path) -> io::Result<VerifyReport> {
    let reader = JournalReader::open(path)?;
    let mut records = 0u64;
    let mut first_seq = None;
    let mut last_seq: Option<u64> = None;
    let mut contiguous = true;
    let mut first_gap_after = None;
    for record in reader {
        records += 1;
        if first_seq.is_none() {
            first_seq = Some(record.seq);
        }
        if let Some(prev) = last_seq {
            if contiguous && record.seq != prev + 1 {
                contiguous = false;
                first_gap_after = Some(prev);
            }
        }
        last_seq = Some(record.seq);
    }
    Ok(VerifyReport {
        records,
        first_seq,
        last_valid_seq: last_seq,
        contiguous,
        first_gap_after,
    })
}

/// Outcome of diffing two journals record-by-record in sequence order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalDiff {
    /// Both files decode to the same records (same length, same frames).
    Identical,
    /// The record frames differ at this shared sequence.
    FrameDiffers { seq: u64 },
    /// The two files carry different sequence numbers at the same position.
    SeqMismatch { a_seq: u64, b_seq: u64 },
    /// One file has records the other does not; `at_seq` is the first extra
    /// record's sequence and `longer_is_a` says which side has it.
    LengthMismatch { at_seq: u64, longer_is_a: bool },
}

/// Find the first point at which two journals diverge, comparing position by
/// position. Reuses [`JournalReader`]'s crash-tolerant decode, so a truncated
/// tail on one side surfaces as a [`JournalDiff::LengthMismatch`].
pub fn diff_journals(a_path: &Path, b_path: &Path) -> io::Result<JournalDiff> {
    let mut a = JournalReader::open(a_path)?;
    let mut b = JournalReader::open(b_path)?;
    loop {
        match (a.next(), b.next()) {
            (Some(ra), Some(rb)) => {
                if ra.seq != rb.seq {
                    return Ok(JournalDiff::SeqMismatch {
                        a_seq: ra.seq,
                        b_seq: rb.seq,
                    });
                }
                if ra.frame != rb.frame {
                    return Ok(JournalDiff::FrameDiffers { seq: ra.seq });
                }
            }
            (Some(ra), None) => {
                return Ok(JournalDiff::LengthMismatch {
                    at_seq: ra.seq,
                    longer_is_a: true,
                })
            }
            (None, Some(rb)) => {
                return Ok(JournalDiff::LengthMismatch {
                    at_seq: rb.seq,
                    longer_is_a: false,
                })
            }
            (None, None) => return Ok(JournalDiff::Identical),
        }
    }
}

/// One human-readable line for an execution-outbox record: Raft coordinates
/// plus the decoded execution report (via [`wire::DecodedReport`]'s `Display`).
pub fn format_outbox_record(record: &OutboxRecord) -> String {
    match wire::decode_report(&record.report_frame) {
        Some(report) => format!(
            "raft_group={} raft_index={} ordinal={} {}",
            record.raft_group, record.raft_index, record.ordinal, report
        ),
        None => format!(
            "raft_group={} raft_index={} ordinal={} <undecodable report>",
            record.raft_group, record.raft_index, record.ordinal
        ),
    }
}

/// Dump execution-outbox records to `out`, one per line. `from_seq`/`to_seq` in
/// the shared [`DumpFilter`] are interpreted as an inclusive `raft_index` range
/// (the outbox has no monotonic per-record sequence of its own); the timestamp
/// bounds are ignored (outbox records carry no timestamp). Returns the number
/// of records written.
pub fn dump_outbox(path: &Path, filter: &DumpFilter, out: &mut impl Write) -> io::Result<usize> {
    let mut reader = ExecutionOutboxReader::open(path.to_path_buf())?;
    let mut records = Vec::new();
    reader.read_available(|record| records.push(record))?;
    let mut written = 0;
    for record in &records {
        let index = record.raft_index;
        let in_range = filter.from_seq.map_or(true, |v| index >= v)
            && filter.to_seq.map_or(true, |v| index <= v);
        if in_range {
            writeln!(out, "{}", format_outbox_record(record))?;
            written += 1;
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::JournalWriter;
    use crate::types::{InstrumentId, OrderId};
    use crate::wire::MSG_LEN;
    use crate::Command;
    use std::time::Duration;

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tc-jinspect-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_journal(path: &Path, commands: &[(u64, Command)]) {
        let mut w = JournalWriter::open(path, Duration::from_millis(1)).unwrap();
        for (ts, command) in commands {
            let mut frame = [0u8; MSG_LEN];
            wire::encode_command(command, &mut frame);
            w.append(*ts, &frame).unwrap();
        }
        w.flush().unwrap();
    }

    fn cancel(order_id: u64, cmd_id: u64) -> Command {
        Command::Cancel {
            instrument: InstrumentId(7),
            order_id: OrderId(order_id),
            cmd_id,
        }
    }

    #[test]
    fn dump_filters_by_seq_and_ts() {
        let dir = scratch_dir("dump");
        let path = dir.join("j.bin");
        write_journal(
            &path,
            &[
                (1_000, cancel(10, 1)),
                (2_000, cancel(11, 2)),
                (3_000, cancel(12, 3)),
            ],
        );
        // No filter -> all three.
        let mut buf = Vec::new();
        assert_eq!(
            dump_journal(&path, &DumpFilter::default(), &mut buf).unwrap(),
            3
        );
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text.lines().count(), 3);
        assert!(text.contains("seq=1 ts_nanos=1000"));
        assert!(text.contains("Cancel"));

        // Seq range [2,3].
        let mut buf = Vec::new();
        let filter = DumpFilter {
            from_seq: Some(2),
            to_seq: Some(3),
            ..Default::default()
        };
        assert_eq!(dump_journal(&path, &filter, &mut buf).unwrap(), 2);

        // Timestamp lower bound excludes the first record.
        let mut buf = Vec::new();
        let filter = DumpFilter {
            from_ts: Some(2_500),
            ..Default::default()
        };
        assert_eq!(dump_journal(&path, &filter, &mut buf).unwrap(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn verify_reports_contiguous_and_last_valid_seq() {
        let dir = scratch_dir("verify");
        let path = dir.join("j.bin");
        write_journal(
            &path,
            &[(1, cancel(1, 1)), (2, cancel(2, 2)), (3, cancel(3, 3))],
        );
        let report = verify_journal(&path).unwrap();
        assert_eq!(report.records, 3);
        assert_eq!(report.first_seq, Some(1));
        assert_eq!(report.last_valid_seq, Some(3));
        assert!(report.contiguous);
        assert_eq!(report.first_gap_after, None);

        // A truncated tail (crash mid-write) leaves a shorter, still-contiguous
        // prefix. Cut into the last record like the journal's own test.
        let full = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 13).unwrap();
        drop(f);
        let report = verify_journal(&path).unwrap();
        assert_eq!(report.records, 2);
        assert_eq!(report.last_valid_seq, Some(2));
        assert!(report.contiguous);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn diff_detects_identical_frame_and_length() {
        let dir = scratch_dir("diff");
        let base = [(1, cancel(1, 1)), (2, cancel(2, 2)), (3, cancel(3, 3))];
        let a = dir.join("a.bin");
        write_journal(&a, &base);

        // Identical copy.
        let b = dir.join("b.bin");
        write_journal(&b, &base);
        assert_eq!(diff_journals(&a, &b).unwrap(), JournalDiff::Identical);

        // Frame differs at seq 2 (different order id in the same slot).
        let c = dir.join("c.bin");
        write_journal(
            &c,
            &[(1, cancel(1, 1)), (2, cancel(999, 2)), (3, cancel(3, 3))],
        );
        assert_eq!(
            diff_journals(&a, &c).unwrap(),
            JournalDiff::FrameDiffers { seq: 2 }
        );

        // Length mismatch: `d` is a proper prefix of `a`.
        let d = dir.join("d.bin");
        write_journal(&d, &base[..2]);
        assert_eq!(
            diff_journals(&a, &d).unwrap(),
            JournalDiff::LengthMismatch {
                at_seq: 3,
                longer_is_a: true,
            }
        );
        assert_eq!(
            diff_journals(&d, &a).unwrap(),
            JournalDiff::LengthMismatch {
                at_seq: 3,
                longer_is_a: false,
            }
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
