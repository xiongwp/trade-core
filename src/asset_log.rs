//! Independent, portable write-ahead logs for individual instruments.
//!
//! A machine may own thousands of instruments, but each instrument writes to
//! `asset-<id>.wal` and can therefore be replayed or migrated without reading
//! unrelated markets. Raft's committed index is the authority for which
//! commands may enter these logs; this module is the durable per-asset view.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::exchange::{Command, Processor, StrategyFactory};
use crate::journal::{self, JournalReader, JournalWriter};
use crate::types::InstrumentId;
use crate::wire::{self, MSG_LEN};

/// Summary used to verify a copied asset log before activating it on another
/// machine. `fingerprint` covers every durable record in sequence order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AssetLogMeta {
    pub instrument: InstrumentId,
    pub records: u64,
    pub last_seq: u64,
    pub fingerprint: u64,
}

/// Deterministic result of replaying a single asset WAL. The fingerprint is
/// over the emitted matching reports, so it detects a book that appears valid
/// but produced a different fill sequence.
pub struct AssetReplaySummary {
    pub meta: AssetLogMeta,
    pub reports: Vec<crate::exchange::ExecReport>,
    pub fingerprint: u64,
    pub processor: Processor,
}

/// Durable proof that a Raft entry was applied and produced an exact result
/// stream. Recovery uses it to rebuild memory without re-emitting already
/// durable Outbox records, and fails closed if replay differs by one byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedBatchProof {
    pub raft_index: u64,
    pub report_count: u32,
    pub report_fingerprint: u64,
}

const APPLIED_PROOF_HEADER: [u8; 8] = *b"TCAP\x01\0\0\0";
const APPLIED_PROOF_RECORD_LEN: usize = 32;

/// Owns lazily-opened WAL writers for the instruments local to one machine.
pub struct AssetJournalSet {
    root: PathBuf,
    flush_interval: Duration,
    writers: HashMap<InstrumentId, CachedWriter>,
    max_open_writers: usize,
    writer_buffer_bytes: usize,
    access_clock: u64,
}

struct CachedWriter {
    writer: JournalWriter,
    last_access: u64,
}

impl AssetJournalSet {
    pub fn open(root: impl Into<PathBuf>, flush_interval: Duration) -> io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            flush_interval,
            writers: HashMap::new(),
            // One Raft entry can touch at most this many assets. Keeping at
            // least that many writers guarantees no writer from the current
            // batch is evicted before the group durability barrier.
            max_open_writers: std::env::var("TC_ASSET_WAL_MAX_OPEN_WRITERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1024usize)
                .max(wire::RAFT_BATCH_MAX_COMMANDS),
            writer_buffer_bytes: std::env::var("TC_ASSET_WAL_BUFFER_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8 << 10),
            access_clock: 0,
        })
    }

    fn writer_for(&mut self, instrument: InstrumentId) -> io::Result<&mut JournalWriter> {
        self.access_clock = self.access_clock.wrapping_add(1);
        let access = self.access_clock;
        if self.writers.contains_key(&instrument) {
            let cached = self.writers.get_mut(&instrument).expect("writer exists");
            cached.last_access = access;
            return Ok(&mut cached.writer);
        }

        if self.writers.len() >= self.max_open_writers {
            let victim = self
                .writers
                .iter()
                .min_by_key(|(_, cached)| cached.last_access)
                .map(|(instrument, _)| *instrument)
                .expect("non-empty writer cache");
            let mut evicted = self.writers.remove(&victim).expect("victim exists");
            evicted.writer.flush_to_os()?;
        }

        let path = asset_path(&self.root, instrument);
        let mut writer = JournalWriter::open_with_capacity(
            &path,
            self.flush_interval,
            self.writer_buffer_bytes,
        )?;
        writer.resume_from(last_seq(&path)?);
        self.writers.insert(
            instrument,
            CachedWriter {
                writer,
                last_access: access,
            },
        );
        Ok(&mut self
            .writers
            .get_mut(&instrument)
            .expect("writer inserted")
            .writer)
    }

    pub fn path_for(&self, instrument: InstrumentId) -> PathBuf {
        asset_path(&self.root, instrument)
    }

    /// Append a command to exactly one asset log. Cross-asset batches are not
    /// portable units and are therefore rejected here.
    pub fn append(&mut self, command: &Command) -> io::Result<u64> {
        let instrument = command.instrument();
        if matches!(command, Command::Batch(commands) if commands.iter().any(|c| c.instrument() != instrument))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "per-asset log cannot contain a cross-asset batch",
            ));
        }
        let mut frame = [0u8; MSG_LEN];
        wire::encode_command(command, &mut frame);
        let writer = self.writer_for(instrument)?;
        writer.append(journal::now_nanos(), &frame)
    }

    pub fn flush_all(&mut self) -> io::Result<()> {
        for cached in self.writers.values_mut() {
            cached.writer.flush()?;
        }
        Ok(())
    }

    /// Persist a Raft-committed asset command before the matching engine
    /// mutates memory. The caller advances the application watermark only
    /// after its shard journal is durable too.
    pub fn append_committed(&mut self, raft_index: u64, command: &Command) -> io::Result<u64> {
        let instrument = command.instrument();
        let seq = self.append(command)?;
        let writer = self
            .writers
            .get_mut(&instrument)
            .expect("writer opened by append");
        writer.writer.sync_data()?;
        let _ = raft_index;
        Ok(seq)
    }

    /// Append one quorum-committed Raft entry containing many commands and
    /// flush every touched asset WAL into the kernel. The caller issues one
    /// filesystem group commit after its shard journal has also been flushed.
    pub fn append_committed_batch(
        &mut self,
        commands: &[Command],
    ) -> io::Result<Vec<InstrumentId>> {
        let mut touched = Vec::new();
        for command in commands {
            let instrument = command.instrument();
            self.append(command)?;
            if !touched.contains(&instrument) {
                touched.push(instrument);
            }
        }
        for instrument in &touched {
            self.writers
                .get_mut(instrument)
                .expect("writer opened by batch append")
                .writer
                .flush_to_os()?;
        }
        Ok(touched)
    }

    /// One durability barrier for all asset WALs and the shard journal on the
    /// same filesystem. Must be called only after every userspace buffer has
    /// been flushed.
    pub fn sync_committed_batch(&self, touched: &[InstrumentId]) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            // A single syncfs(2) on any one writer flushes every dirty file on
            // the shared filesystem, so the whole batch (and the co-located
            // shard journal) is made durable by one barrier.
            let Some(instrument) = touched.first() else {
                return Ok(());
            };
            self.writers
                .get(instrument)
                .expect("writer opened by batch append")
                .writer
                .sync_filesystem()
        }
        #[cfg(not(target_os = "linux"))]
        {
            // No filesystem-wide barrier available: fsync every touched writer
            // individually. Slower, but each committed asset WAL is genuinely
            // durable rather than only the first one.
            for instrument in touched {
                self.writers
                    .get(instrument)
                    .expect("writer opened by batch append")
                    .writer
                    .sync_filesystem()?;
            }
            Ok(())
        }
    }

    pub fn mark_raft_applied(&self, instrument: InstrumentId, raft_index: u64) -> io::Result<()> {
        write_applied_index(&self.root, instrument, raft_index)
    }

    pub fn mark_raft_batch_applied(
        &self,
        _instruments: &[InstrumentId],
        raft_index: u64,
    ) -> io::Result<()> {
        append_applied_batch(&self.root, raft_index)
    }
}

/// Load the durable set of fully applied multi-command Raft entries. Each
/// record represents a whole batch whose asset WALs and shard journal were
/// synchronized before the marker was appended.
pub fn load_applied_batches(root: &Path) -> io::Result<HashSet<u64>> {
    let path = root.join("raft-batches.applied");
    let mut applied = HashSet::new();
    match File::open(path) {
        Ok(mut file) => {
            let mut record = [0u8; 16];
            loop {
                match file.read_exact(&mut record) {
                    Ok(()) => {
                        let index = u64::from_le_bytes(record[..8].try_into().unwrap());
                        if journal::fnv1a(&record[..8])
                            != u64::from_le_bytes(record[8..].try_into().unwrap())
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "applied Raft batch watermark checksum mismatch",
                            ));
                        }
                        applied.insert(index);
                    }
                    Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(error) => return Err(error),
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    applied.extend(load_applied_batch_proofs(root)?.into_keys());
    Ok(applied)
}

/// Append one fail-closed application proof after the result Outbox has been
/// synchronized. A complete proof means both state application and its exact
/// deterministic result stream crossed the durability boundary.
pub fn mark_applied_batch(
    root: &Path,
    raft_index: u64,
    report_count: u32,
    report_fingerprint: u64,
) -> io::Result<()> {
    std::fs::create_dir_all(root)?;
    let path = root.join("raft-batches.applied.v2");
    let new_file = !path.exists();
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    if new_file {
        file.write_all(&APPLIED_PROOF_HEADER)?;
    }
    let mut record = [0u8; APPLIED_PROOF_RECORD_LEN];
    record[0..8].copy_from_slice(&raft_index.to_le_bytes());
    record[8..12].copy_from_slice(&report_count.to_le_bytes());
    record[16..24].copy_from_slice(&report_fingerprint.to_le_bytes());
    let checksum = journal::fnv1a(&record[..24]);
    record[24..32].copy_from_slice(&checksum.to_le_bytes());
    file.write_all(&record)?;
    file.sync_data()
}

/// Load and validate every exact replay proof. A torn final record is ignored
/// because its batch has no durable application proof and will be replayed as
/// a normal committed Raft entry. Corruption in a complete record is fatal.
pub fn load_applied_batch_proofs(root: &Path) -> io::Result<HashMap<u64, AppliedBatchProof>> {
    let path = root.join("raft-batches.applied.v2");
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error),
    };
    let mut header = [0u8; APPLIED_PROOF_HEADER.len()];
    file.read_exact(&mut header)?;
    if header != APPLIED_PROOF_HEADER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "applied Raft proof header/version mismatch",
        ));
    }
    let mut proofs = HashMap::new();
    let mut record = [0u8; APPLIED_PROOF_RECORD_LEN];
    loop {
        match file.read_exact(&mut record) {
            Ok(()) => {
                let expected = u64::from_le_bytes(record[24..32].try_into().unwrap());
                if journal::fnv1a(&record[..24]) != expected {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "applied Raft proof checksum mismatch",
                    ));
                }
                let proof = AppliedBatchProof {
                    raft_index: u64::from_le_bytes(record[0..8].try_into().unwrap()),
                    report_count: u32::from_le_bytes(record[8..12].try_into().unwrap()),
                    report_fingerprint: u64::from_le_bytes(record[16..24].try_into().unwrap()),
                };
                if let Some(previous) = proofs.insert(proof.raft_index, proof) {
                    if previous != proof {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "conflicting applied Raft proofs for one index",
                        ));
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
    }
    Ok(proofs)
}

/// Validate deterministic replay against the durable proof. Callers must stop
/// recovery on error; continuing would serve a book different from the one
/// that produced the already-published executions.
pub fn verify_applied_batch(
    proof: AppliedBatchProof,
    report_count: u32,
    report_fingerprint: u64,
) -> io::Result<()> {
    if proof.report_count != report_count || proof.report_fingerprint != report_fingerprint {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Raft replay mismatch at index {}: expected count={} fingerprint={:#018x}, got count={} fingerprint={:#018x}",
                proof.raft_index,
                proof.report_count,
                proof.report_fingerprint,
                report_count,
                report_fingerprint
            ),
        ));
    }
    Ok(())
}

/// Load exact command fingerprints from the per-asset WALs, but only when the
/// WAL record count proves that every record is also represented by the shard
/// snapshot+journal state recovered by the matching engine.
///
/// This is an upgrade bridge for nodes written before batch application
/// watermarks existed.  Asset WAL append precedes the shard journal append, so
/// membership in an asset WAL alone is not enough to claim that a command was
/// applied.  Equality with the sum of the shard journal sequence heads closes
/// that crash window: there can be no WAL-only tail record.
pub fn recovered_command_fingerprints(
    root: &Path,
    journal_root: &Path,
    wanted: &HashSet<u64>,
) -> io::Result<Option<HashMap<u64, u64>>> {
    let mut shard_ids = HashSet::new();
    for entry in std::fs::read_dir(journal_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        for (prefix, suffix) in [("journal-shard-", ".bin"), ("snapshot-shard-", ".bin")] {
            if let Some(id) = name
                .strip_prefix(prefix)
                .and_then(|value| value.strip_suffix(suffix))
                .and_then(|value| value.parse::<usize>().ok())
            {
                shard_ids.insert(id);
            }
        }
    }
    let mut recovered_records = 0u64;
    for shard_id in shard_ids {
        let snapshot_path = journal_root.join(format!("snapshot-shard-{shard_id}.bin"));
        let journal_path = journal_root.join(format!("journal-shard-{shard_id}.bin"));
        let snapshot_seq = if snapshot_path.exists() {
            crate::snapshot::load(&snapshot_path)?.journal_seq
        } else {
            0
        };
        let journal_seq = if journal_path.exists() {
            last_seq(&journal_path)?
        } else {
            0
        };
        recovered_records = recovered_records.saturating_add(snapshot_seq.max(journal_seq));
    }

    let mut wal_records = 0u64;
    let mut fingerprints = HashMap::new();
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("wal") {
            continue;
        }
        for record in JournalReader::open(&path)? {
            wal_records = wal_records.saturating_add(1);
            let Some(command) =
                wire::WireView::parse(&record.frame).and_then(|view| view.to_command())
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("asset WAL {} contains an invalid command", path.display()),
                ));
            };
            let id = command.id();
            if id == 0 || !wanted.contains(&id) {
                continue;
            }
            let fingerprint = journal::fnv1a(&record.frame);
            match fingerprints.insert(id, fingerprint) {
                Some(previous) if previous != fingerprint => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("command id {id} has conflicting asset WAL payloads"),
                    ));
                }
                _ => {}
            }
        }
    }
    if wal_records != recovered_records {
        return Ok(None);
    }
    Ok(Some(fingerprints))
}

/// Highest Raft entry durably represented in the local asset and shard logs.
/// A missing watermark means that recovery must re-submit every committed
/// entry for this asset.
pub fn applied_raft_index(root: &Path, instrument: InstrumentId) -> io::Result<u64> {
    let path = applied_path(root, instrument);
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut bytes = [0u8; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

/// Read and validate an asset WAL. Sequence gaps or malformed commands are a
/// hard error; a torn final journal record is handled by `JournalReader` as a
/// safe prefix, consistent with normal crash recovery.
pub fn inspect(root: &Path, instrument: InstrumentId) -> io::Result<AssetLogMeta> {
    let path = asset_path(root, instrument);
    let mut records = 0u64;
    let mut fingerprint = 0xcbf29ce484222325u64;
    for (expected, record) in (1u64..).zip(JournalReader::open(&path)?) {
        if record.seq != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "asset log sequence gap",
            ));
        }
        let view = wire::WireView::parse(&record.frame).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "asset log contains invalid frame",
            )
        })?;
        let command = view.to_command().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "asset log contains unknown command",
            )
        })?;
        if command.instrument() != instrument {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "asset log contains another instrument",
            ));
        }
        fingerprint = record_hash(fingerprint, record.seq, record.ts_nanos, &record.frame);
        records += 1;
    }
    Ok(AssetLogMeta {
        instrument,
        records,
        last_seq: records,
        fingerprint,
    })
}

/// Replay all durable records after `after_seq` into a fresh or snapshot-loaded
/// processor. The caller compares the returned metadata/fingerprint with the
/// source machine before making this machine the asset owner.
pub fn replay_into(
    root: &Path,
    instrument: InstrumentId,
    after_seq: u64,
    processor: &mut Processor,
) -> io::Result<AssetLogMeta> {
    let meta = inspect(root, instrument)?;
    let mut seen = HashMap::new();
    for record in JournalReader::open(&asset_path(root, instrument))? {
        if record.seq <= after_seq {
            continue;
        }
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid asset command"))?;
        if !accept_replay_command(&mut seen, &command, &record.frame)? {
            continue;
        }
        processor.process(command, &mut |_| {});
    }
    Ok(meta)
}

/// Convenience helper for a destination node that has no state for this asset.
pub fn replay_fresh(
    root: &Path,
    instrument: InstrumentId,
    strategy: StrategyFactory,
) -> io::Result<(Processor, AssetLogMeta)> {
    let mut processor = Processor::new(strategy, None);
    let meta = replay_into(root, instrument, 0, &mut processor)?;
    Ok((processor, meta))
}

/// Replay one asset and retain the matching output for recovery verification.
pub fn replay_with_reports(
    root: &Path,
    instrument: InstrumentId,
    strategy: StrategyFactory,
) -> io::Result<AssetReplaySummary> {
    let mut processor = Processor::new(strategy, None);
    let mut reports = Vec::new();
    let mut seen = HashMap::new();
    for record in JournalReader::open(&asset_path(root, instrument))? {
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid asset command"))?;
        if !accept_replay_command(&mut seen, &command, &record.frame)? {
            continue;
        }
        processor.process(command, &mut |report| reports.push(report));
    }
    let mut bytes = Vec::with_capacity(reports.len() * crate::wire::REPORT_LEN);
    for report in &reports {
        let mut frame = [0u8; crate::wire::REPORT_LEN];
        wire::encode_report(report, &mut frame);
        bytes.extend_from_slice(&frame);
    }
    Ok(AssetReplaySummary {
        meta: inspect(root, instrument)?,
        fingerprint: journal::fnv1a(&bytes),
        reports,
        processor,
    })
}

fn accept_replay_command(
    seen: &mut HashMap<u64, [u8; MSG_LEN]>,
    command: &Command,
    frame: &[u8; MSG_LEN],
) -> io::Result<bool> {
    let id = command.id();
    if id == 0 {
        return Ok(true);
    }
    match seen.get(&id) {
        None => {
            seen.insert(id, *frame);
            Ok(true)
        }
        Some(original) if original == frame => Ok(false),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("command id {id} has conflicting asset WAL payloads"),
        )),
    }
}

fn asset_path(root: &Path, instrument: InstrumentId) -> PathBuf {
    root.join(format!("asset-{:010}.wal", instrument.0))
}

fn applied_path(root: &Path, instrument: InstrumentId) -> PathBuf {
    root.join(format!("asset-{:010}.applied", instrument.0))
}

fn write_applied_index(root: &Path, instrument: InstrumentId, raft_index: u64) -> io::Result<()> {
    let path = applied_path(root, instrument);
    let current = applied_raft_index(root, instrument)?;
    if raft_index <= current {
        return Ok(());
    }
    let temp = path.with_extension("applied.tmp");
    let mut file = std::fs::File::create(&temp)?;
    file.write_all(&raft_index.to_le_bytes())?;
    file.sync_all()?;
    std::fs::rename(temp, path)
}

fn append_applied_batch(root: &Path, raft_index: u64) -> io::Result<()> {
    let mut record = [0u8; 16];
    record[..8].copy_from_slice(&raft_index.to_le_bytes());
    let checksum = journal::fnv1a(&record[..8]);
    record[8..].copy_from_slice(&checksum.to_le_bytes());
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("raft-batches.applied"))?;
    file.write_all(&record)?;
    file.sync_data()
}

fn last_seq(path: &Path) -> io::Result<u64> {
    Ok(JournalReader::open(path)?
        .last()
        .map(|record| record.seq)
        .unwrap_or(0))
}

fn record_hash(previous: u64, seq: u64, ts_nanos: u64, frame: &[u8; MSG_LEN]) -> u64 {
    let mut bytes = [0u8; 8 + 8 + 8 + MSG_LEN];
    bytes[0..8].copy_from_slice(&previous.to_le_bytes());
    bytes[8..16].copy_from_slice(&seq.to_le_bytes());
    bytes[16..24].copy_from_slice(&ts_nanos.to_le_bytes());
    bytes[24..].copy_from_slice(frame);
    journal::fnv1a(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Order;
    use crate::strategy::PriceTimePriority;

    #[test]
    fn exact_applied_proof_round_trips_and_is_visible_as_watermark() {
        let root = std::env::temp_dir().join(format!(
            "tc-applied-proof-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        mark_applied_batch(&root, 91, 5, 0xfeed_beef).unwrap();
        let proofs = load_applied_batch_proofs(&root).unwrap();
        assert_eq!(
            proofs.get(&91),
            Some(&AppliedBatchProof {
                raft_index: 91,
                report_count: 5,
                report_fingerprint: 0xfeed_beef,
            })
        );
        assert!(load_applied_batches(&root).unwrap().contains(&91));
        assert!(verify_applied_batch(proofs[&91], 5, 0xfeed_beef).is_ok());
        assert!(verify_applied_batch(proofs[&91], 4, 0xfeed_beef).is_err());
        assert!(verify_applied_batch(proofs[&91], 5, 7).is_err());
        std::fs::remove_dir_all(root).ok();
    }
    use crate::types::{OrderId, Side};

    #[test]
    fn an_asset_log_replays_portably_without_other_assets() {
        let root = std::env::temp_dir().join(format!("tc-asset-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let btc = InstrumentId(5000);
        let eth = InstrumentId(5001);
        let mut logs = AssetJournalSet::open(&root, Duration::from_millis(1)).unwrap();
        logs.append(&Command::New(
            Order::limit(OrderId(1), Side::Sell, 100, 3).on(btc),
        ))
        .unwrap();
        logs.append(&Command::New(
            Order::limit(OrderId(2), Side::Buy, 100, 1).on(eth),
        ))
        .unwrap();
        logs.flush_all().unwrap();

        let meta = inspect(&root, btc).unwrap();
        assert_eq!(meta.records, 1);
        let (processor, replayed) =
            replay_fresh(&root, btc, || Box::new(PriceTimePriority)).unwrap();
        assert_eq!(replayed, meta);
        assert_eq!(processor.engine(btc).unwrap().book().len(), 1);
        assert!(processor.engine(eth).is_none());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn replay_skips_only_byte_identical_duplicate_commands() {
        let root = std::env::temp_dir().join(format!(
            "tc-asset-log-dedup-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let instrument = InstrumentId(42);
        let command = Command::New(Order::limit(OrderId(900), Side::Buy, 100, 1).on(instrument));
        let mut logs = AssetJournalSet::open(&root, Duration::from_millis(1)).unwrap();
        logs.append(&command).unwrap();
        logs.append(&command).unwrap();
        logs.flush_all().unwrap();

        let (processor, meta) =
            replay_fresh(&root, instrument, || Box::new(PriceTimePriority)).unwrap();
        assert_eq!(meta.records, 2, "repair does not rewrite the audit WAL");
        assert_eq!(processor.engine(instrument).unwrap().book().len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn replay_rejects_a_command_id_with_different_payloads() {
        let root = std::env::temp_dir().join(format!(
            "tc-asset-log-conflict-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let instrument = InstrumentId(43);
        let mut logs = AssetJournalSet::open(&root, Duration::from_millis(1)).unwrap();
        logs.append(&Command::New(
            Order::limit(OrderId(901), Side::Buy, 100, 1).on(instrument),
        ))
        .unwrap();
        logs.append(&Command::New(
            Order::limit(OrderId(901), Side::Buy, 101, 1).on(instrument),
        ))
        .unwrap();
        logs.flush_all().unwrap();

        let error = replay_fresh(&root, instrument, || Box::new(PriceTimePriority))
            .err()
            .expect("conflicting command id must fail recovery");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn cross_asset_batch_is_rejected_before_it_can_reach_a_wal() {
        let root = std::env::temp_dir().join(format!("tc-asset-log-batch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut logs = AssetJournalSet::open(&root, Duration::from_millis(1)).unwrap();
        let btc = InstrumentId(1);
        let eth = InstrumentId(2);
        let command = Command::Batch(vec![
            Command::New(Order::limit(OrderId(1), Side::Buy, 100, 1).on(btc)),
            Command::New(Order::limit(OrderId(2), Side::Sell, 100, 1).on(eth)),
        ]);
        let error = logs.append(&command).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!logs.path_for(btc).exists());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn batch_watermarks_group_commit_and_reload() {
        let root = std::env::temp_dir().join(format!(
            "tc-asset-log-watermark-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let logs = AssetJournalSet::open(&root, Duration::from_millis(1)).unwrap();

        logs.mark_raft_batch_applied(&[InstrumentId(1), InstrumentId(2)], 77)
            .unwrap();

        let applied = load_applied_batches(&root).unwrap();
        assert_eq!(applied.len(), 1);
        assert!(applied.contains(&77));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn legacy_recovery_fingerprints_require_complete_shard_journal_coverage() {
        let base = std::env::temp_dir().join(format!(
            "tc-asset-log-legacy-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        let assets = base.join("assets");
        std::fs::create_dir_all(&assets).unwrap();
        let instrument = InstrumentId(77);
        let command = Command::New(
            Order::limit(OrderId(7_700), Side::Buy, 100, 1).on(instrument),
        );
        let mut asset_logs = AssetJournalSet::open(&assets, Duration::from_millis(1)).unwrap();
        asset_logs.append(&command).unwrap();
        asset_logs.flush_all().unwrap();

        let mut frame = [0u8; MSG_LEN];
        wire::encode_command(&command, &mut frame);
        let mut shard_log = JournalWriter::open(
            &base.join("journal-shard-0.bin"),
            Duration::from_millis(1),
        )
        .unwrap();
        shard_log.append(1, &frame).unwrap();
        shard_log.sync_data().unwrap();

        let wanted = HashSet::from([command.id()]);
        let recovered = recovered_command_fingerprints(&assets, &base, &wanted)
            .unwrap()
            .expect("equal WAL and shard sequence counts prove full coverage");
        assert_eq!(recovered[&command.id()], journal::fnv1a(&frame));

        asset_logs.append(&Command::New(
            Order::limit(OrderId(7_701), Side::Sell, 101, 1).on(instrument),
        ))
        .unwrap();
        asset_logs.flush_all().unwrap();
        assert!(recovered_command_fingerprints(&assets, &base, &wanted)
            .unwrap()
            .is_none());
        std::fs::remove_dir_all(&base).ok();
    }
}
