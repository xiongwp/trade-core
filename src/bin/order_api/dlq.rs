//! Dead-letter file for records that keep failing, and the bounded retry wrapper.

use super::*;

/// Header stamped on a fresh dead-letter file; a version bump changes it so old
/// files are rejected rather than silently misparsed.
pub(crate) const DLQ_HEADER: [u8; 8] = *b"TCDLQ01\0";

/// Append-only dead-letter log for order-execution messages that exhausted
/// their retry budget. Mirrors the journal WAL style: a magic header, one
/// length-framed record per poison message carrying `(partition, offset,
/// reason, original payload)`, each closed by an FNV-1a checksum so a torn
/// tail is detected and dropped on read.
pub(crate) struct DlqWriter {
    pub(crate) file: File,
}

impl DlqWriter {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let mut probe = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        if probe.metadata()?.len() == 0 {
            probe.write_all(&DLQ_HEADER)?;
            probe.sync_data()?;
        } else {
            let mut header = [0u8; 8];
            probe.read_exact(&mut header)?;
            if header != DLQ_HEADER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DLQ header/version mismatch (migration required)",
                ));
            }
        }
        Ok(Self {
            file: OpenOptions::new().append(true).open(path)?,
        })
    }

    pub(crate) fn append(
        &mut self,
        partition: i32,
        offset: i64,
        reason: &str,
        payload: &[u8],
    ) -> io::Result<()> {
        let reason = reason.as_bytes();
        let mut body = Vec::with_capacity(28 + reason.len() + payload.len());
        body.extend_from_slice(&journal::now_nanos().to_le_bytes());
        body.extend_from_slice(&partition.to_le_bytes());
        body.extend_from_slice(&offset.to_le_bytes());
        body.extend_from_slice(&(reason.len() as u32).to_le_bytes());
        body.extend_from_slice(reason);
        body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        body.extend_from_slice(payload);
        let checksum = journal::fnv1a(&body);
        let mut frame = Vec::with_capacity(4 + body.len() + 8);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        frame.extend_from_slice(&checksum.to_le_bytes());
        self.file.write_all(&frame)?;
        self.file.sync_data()
    }
}

/// Outcome of driving one message through the persist-retry-or-dead-letter
/// loop. Both variants advance the Kafka offset — a poison message must not
/// wedge the partition forever.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum PersistOutcome {
    Committed,
    DeadLettered,
}

/// Persist one message with bounded exponential-backoff retries; on exhausting
/// the budget, dead-letter it and report `DeadLettered` so the caller advances
/// the offset. A DLQ write failure is *not* treated as success — we keep
/// retrying rather than silently drop the record.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn persist_with_retry<F>(
    mut persist: F,
    dlq: &Mutex<DlqWriter>,
    metrics: &OrderPipelineMetrics,
    max_retries: u32,
    base_backoff: Duration,
    partition: i32,
    offset: i64,
    payload: &[u8],
) -> PersistOutcome
where
    F: FnMut() -> Result<(), String>,
{
    let mut attempt = 1u32;
    loop {
        match persist() {
            Ok(()) => return PersistOutcome::Committed,
            Err(error) if attempt >= max_retries => {
                let written = dlq
                    .lock()
                    .map_err(|_| "DLQ mutex poisoned".to_string())
                    .and_then(|mut writer| {
                        writer
                            .append(partition, offset, &error, payload)
                            .map_err(|e| e.to_string())
                    });
                match written {
                    Ok(()) => {
                        metrics.dlq_total.fetch_add(1, AtomicOrdering::Relaxed);
                        return PersistOutcome::DeadLettered;
                    }
                    Err(dlq_error) => {
                        log_error!(
                            "execution-mysql",
                            "event=dlq_write_failed partition={partition} offset={offset} error={dlq_error} — retrying"
                        );
                        if !base_backoff.is_zero() {
                            std::thread::sleep(base_backoff);
                        }
                        // Keep the same attempt count: stay in the dead-letter
                        // branch until the record is durably captured.
                    }
                }
            }
            Err(_) => {
                let shift = (attempt - 1).min(9);
                let backoff = base_backoff.saturating_mul(1u32 << shift);
                if !backoff.is_zero() {
                    std::thread::sleep(backoff);
                }
                attempt += 1;
            }
        }
    }
}

/// Dead-letter a command-topic message that cannot be decoded or is routed
/// to the wrong place (e.g. produced under another `route_version` during a
/// rolling deploy) before its offset is committed past. Skipping it with
/// only a log line silently lost an order whose client had already been
/// told ACCEPTED (audit I1); now the raw payload is kept durably
/// (`TC_ORDER_COMMAND_DLQ_PATH`) for an operator to replay. If even the
/// dead-letter write fails, the process stops before the offset moves, so
/// the message is redelivered rather than lost.
pub(crate) fn dead_letter_command(
    stage: &str,
    message: &rdkafka::message::BorrowedMessage<'_>,
    error: &str,
) {
    static DLQ: std::sync::OnceLock<Mutex<DlqWriter>> = std::sync::OnceLock::new();
    static DEAD_LETTERED: AtomicU64 = AtomicU64::new(0);
    let dlq = DLQ.get_or_init(|| {
        let path = std::env::var("TC_ORDER_COMMAND_DLQ_PATH")
            .unwrap_or_else(|_| "order-command-dlq.wal".into());
        Mutex::new(DlqWriter::open(Path::new(&path)).expect("open command DLQ file"))
    });
    let reason = format!("{stage}: {error}");
    let payload = rdkafka::message::Message::payload(message).unwrap_or_default();
    let partition = rdkafka::message::Message::partition(message);
    let offset = rdkafka::message::Message::offset(message);
    dlq.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .append(partition, offset, &reason, payload)
        .unwrap_or_else(|io| {
            panic!("command DLQ append failed ({io}); refusing to skip offset {offset}")
        });
    let total = DEAD_LETTERED.fetch_add(1, AtomicOrdering::Relaxed) + 1;
    log_error!("order-kafka", stage; "event=command_dead_lettered partition={partition} offset={offset} total={total} error={error}");
}
