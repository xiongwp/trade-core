//! Independent, portable write-ahead logs for individual instruments.
//!
//! A machine may own thousands of instruments, but each instrument writes to
//! `asset-<id>.wal` and can therefore be replayed or migrated without reading
//! unrelated markets. Raft's committed index is the authority for which
//! commands may enter these logs; this module is the durable per-asset view.

use std::collections::HashMap;
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

/// Owns lazily-opened WAL writers for the instruments local to one machine.
pub struct AssetJournalSet {
    root: PathBuf,
    flush_interval: Duration,
    writers: HashMap<InstrumentId, JournalWriter>,
}

impl AssetJournalSet {
    pub fn open(root: impl Into<PathBuf>, flush_interval: Duration) -> io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            flush_interval,
            writers: HashMap::new(),
        })
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
        let path = asset_path(&self.root, instrument);
        let writer = match self.writers.entry(instrument) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let mut writer = JournalWriter::open(&path, self.flush_interval)?;
                writer.resume_from(last_seq(&path)?);
                entry.insert(writer)
            }
        };
        writer.append(journal::now_nanos(), &frame)
    }

    pub fn flush_all(&mut self) -> io::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Persist a Raft-committed asset command before the matching engine
    /// mutates memory. The caller advances the application watermark only
    /// after its shard journal is durable too.
    pub fn append_committed(&mut self, raft_index: u64, command: &Command) -> io::Result<u64> {
        let instrument = command.instrument();
        let seq = self.append(command)?;
        let writer = self.writers.get_mut(&instrument).expect("writer opened by append");
        writer.sync_data()?;
        let _ = raft_index;
        Ok(seq)
    }

    pub fn mark_raft_applied(&self, instrument: InstrumentId, raft_index: u64) -> io::Result<()> {
        write_applied_index(&self.root, instrument, raft_index)
    }
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
    for record in JournalReader::open(&asset_path(root, instrument))? {
        if record.seq <= after_seq {
            continue;
        }
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid asset command"))?;
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
    for record in JournalReader::open(&asset_path(root, instrument))? {
        let command = wire::WireView::parse(&record.frame)
            .and_then(|view| view.to_command())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid asset command"))?;
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
}
