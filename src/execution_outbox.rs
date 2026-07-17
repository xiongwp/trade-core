use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::exchange::ExecReport;
use crate::journal::fnv1a;
use crate::wire::{self, EXECUTION_EVENT_LEN, REPORT_LEN};

pub const OUTBOX_HEADER: [u8; 8] = *b"TCEX\x01\0\0\0";
pub const OUTBOX_RECORD_LEN: usize = 4 + 8 + 4 + REPORT_LEN + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxRecord {
    pub raft_group: u32,
    pub raft_index: u64,
    pub ordinal: u32,
    pub report_frame: [u8; REPORT_LEN],
}

impl OutboxRecord {
    pub fn kafka_key(&self, category_size: u32) -> [u8; 4] {
        let instrument = crate::InstrumentId(u32::from_le_bytes(
            self.report_frame[4..8]
                .try_into()
                .expect("execution instrument"),
        ));
        crate::sharding::asset_category(instrument, category_size).to_be_bytes()
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
    flush_interval: Duration,
    last_flush: Instant,
    written_since_flush: usize,
    sync_every: usize,
}

impl ExecutionOutboxWriter {
    pub fn open(
        path: &Path,
        flush_interval: Duration,
        sync_every: usize,
    ) -> io::Result<ExecutionOutboxWriter> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            file.write_all(&OUTBOX_HEADER)?;
            file.sync_data()?;
        } else {
            file.seek(SeekFrom::Start(0))?;
            let mut header = [0u8; OUTBOX_HEADER.len()];
            file.read_exact(&mut header)?;
            if header != OUTBOX_HEADER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution outbox header/version mismatch",
                ));
            }
        }
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(ExecutionOutboxWriter {
            writer: BufWriter::with_capacity(64 * 1024, file),
            flush_interval,
            last_flush: Instant::now(),
            written_since_flush: 0,
            sync_every: sync_every.max(1),
        })
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
        self.written_since_flush += 1;
        if self.written_since_flush >= self.sync_every
            || self.last_flush.elapsed() >= self.flush_interval
        {
            self.sync_data()?;
        }
        Ok(())
    }

    pub fn sync_data(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.written_since_flush = 0;
        self.last_flush = Instant::now();
        Ok(())
    }
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
            offset: OUTBOX_HEADER.len() as u64,
        })
    }

    pub fn read_available(&mut self, mut emit: impl FnMut(OutboxRecord)) -> io::Result<usize> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;
        let mut count = 0;
        loop {
            let mut bytes = [0u8; OUTBOX_RECORD_LEN];
            match reader.read_exact(&mut bytes) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(count),
                Err(error) => return Err(error),
            }
            let expected =
                u64::from_le_bytes(bytes[OUTBOX_RECORD_LEN - 8..].try_into().expect("checksum"));
            if fnv1a(&bytes[..OUTBOX_RECORD_LEN - 8]) != expected {
                return Ok(count);
            }
            let mut frame = [0u8; REPORT_LEN];
            frame.copy_from_slice(&bytes[16..16 + REPORT_LEN]);
            emit(OutboxRecord {
                raft_group: u32::from_le_bytes(bytes[0..4].try_into().expect("group")),
                raft_index: u64::from_le_bytes(bytes[4..12].try_into().expect("index")),
                ordinal: u32::from_le_bytes(bytes[12..16].try_into().expect("ordinal")),
                report_frame: frame,
            });
            self.offset += OUTBOX_RECORD_LEN as u64;
            count += 1;
        }
    }
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
        assert_eq!(got[0].kafka_key(10), 4u32.to_be_bytes());
        let event = wire::decode_execution_event(&got[0].kafka_payload()).unwrap();
        assert_eq!(event.raft_group, 3);
        assert_eq!(event.raft_index, 9);
        assert_eq!(event.ordinal, 2);
        assert_eq!(event.report.instrument, InstrumentId(42));
        assert_eq!(event.report.order_id, OrderId(7));

        std::fs::remove_dir_all(root).ok();
    }
}
