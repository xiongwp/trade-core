//! Kafka -> Raft matcher forwarding: leader discovery, packing, matcher connections.

use super::*;

#[derive(Clone)]
pub(crate) struct MatcherTarget {
    pub(crate) order_addr: String,
    pub(crate) metrics_addr: Option<String>,
}

pub(crate) struct RaftForwardJob {
    /// This group's frames from one Kafka batch, each category's run in Kafka
    /// order (categories are independent books, so their relative order is
    /// free).
    pub(crate) frames: Vec<[u8; wire::MSG_LEN]>,
    pub(crate) reply: std::sync::mpsc::SyncSender<Result<(), String>>,
}

#[derive(Clone)]
pub(crate) struct RaftForwarder {
    pub(crate) groups: Arc<Vec<std::sync::mpsc::SyncSender<RaftForwardJob>>>,
}

impl RaftForwarder {
    pub(crate) fn spawn(target_groups: Vec<Vec<MatcherTarget>>, backpressure: Arc<Backpressure>) -> Self {
        let queue_capacity = env_number("TC_RAFT_FORWARD_QUEUE", 64usize).max(1);
        // Each request blocks until its Raft entry commits — one WAL fsync
        // plus a replication round trip — so the entry is the unit of cost.
        // Pack up to this many commands per entry, across categories and
        // across the jobs queued behind each other.
        let pack_size = env_number("TC_RAFT_PACK_SIZE", 5_000usize)
            .clamp(1, wire::RAFT_BATCH_MAX_COMMANDS);
        let mut groups = Vec::with_capacity(target_groups.len());
        for (group, targets) in target_groups.into_iter().enumerate() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<RaftForwardJob>(queue_capacity);
            let worker_backpressure = backpressure.clone();
            std::thread::Builder::new()
                .name(format!("raft-forward-{group}"))
                .spawn(move || {
                    let mut matcher = MatcherConnection::new(targets);
                    while let Ok(first) = rx.recv() {
                        let jobs = drain_queued_jobs(first, &rx, pack_size);
                        let result = packs(&jobs, pack_size).try_for_each(|pack| {
                            matcher.send_frames(&pack).map_err(|error| {
                                format!("Raft group {group} batch commit failed: {error}")
                            })
                        });
                        worker_backpressure.set_group_health(group, result.is_ok());
                        for job in jobs {
                            let _ = job.reply.send(result.clone());
                        }
                    }
                })
                .expect("spawn Raft forwarding worker");
            groups.push(tx);
        }
        Self {
            groups: Arc::new(groups),
        }
    }

    pub(crate) fn submit(
        &self,
        records: &[KafkaRecord],
        control: &trade_core::cluster::RouteControlPlane,
    ) -> Result<(), String> {
        if self.groups.is_empty() {
            return Err("no Raft groups configured".into());
        }
        let mut grouped = (0..self.groups.len())
            .map(|_| BTreeMap::<u32, Vec<[u8; wire::MSG_LEN]>>::new())
            .collect::<Vec<_>>();
        for record in records {
            if !control.accepts(record.category_id) {
                return Err(format!(
                    "category {} is frozen for migration",
                    record.category_id
                ));
            }
            let group = control.group_for(record.category_id);
            grouped[group]
                .entry(record.category_id)
                .or_default()
                .push(record.frame);
        }

        let mut replies = Vec::new();
        for (group, categories) in grouped.into_iter().enumerate() {
            if categories.is_empty() {
                continue;
            }
            let frames = categories.into_values().flatten().collect();
            let (reply, result) = std::sync::mpsc::sync_channel(1);
            self.groups[group]
                .send(RaftForwardJob { frames, reply })
                .map_err(|_| format!("Raft group {group} forwarding worker stopped"))?;
            replies.push(result);
        }
        for reply in replies {
            reply
                .recv()
                .map_err(|_| "Raft forwarding worker dropped acknowledgement".to_string())??;
        }
        Ok(())
    }
}

/// Take `first` plus whatever jobs are already queued behind it, while they
/// fit in one pack — no waiting, so an idle pipeline adds no latency. The
/// channel is FIFO and each job keeps its categories' Kafka order, so
/// concatenating jobs in arrival order keeps every category in order.
pub(crate) fn drain_queued_jobs(
    first: RaftForwardJob,
    rx: &std::sync::mpsc::Receiver<RaftForwardJob>,
    pack_size: usize,
) -> Vec<RaftForwardJob> {
    let mut total = first.frames.len();
    let mut jobs = vec![first];
    while total < pack_size {
        let Ok(job) = rx.try_recv() else { break };
        total += job.frames.len();
        jobs.push(job);
    }
    jobs
}

/// Cut the merged jobs into Raft entries of at most `pack_size` commands,
/// in order. Each pack is committed before the next is sent, so a category
/// spanning two packs still applies in order.
pub(crate) fn packs(
    jobs: &[RaftForwardJob],
    pack_size: usize,
) -> impl Iterator<Item = Vec<[u8; wire::MSG_LEN]>> + '_ {
    let mut frames = jobs.iter().flat_map(|job| job.frames.iter().copied()).peekable();
    std::iter::from_fn(move || {
        frames.peek()?;
        Some(frames.by_ref().take(pack_size).collect())
    })
}

pub(crate) fn forward_kafka_batch(
    forwarder: &RaftForwarder,
    records: &[KafkaRecord],
    control: &trade_core::cluster::RouteControlPlane,
) -> Result<(), String> {
    if forwarder.groups.is_empty() {
        return Err("no Raft groups configured".into());
    }
    forwarder.submit(records, control)
}

pub(crate) fn is_leader(metrics_addr: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(metrics_addr) else {
        return false;
    };
    let timeout = Duration::from_millis(env_number("TC_RAFT_LEADER_PROBE_TIMEOUT_MS", 1_000u64));
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: raft\r\nConnection: close\r\n\r\n");
    let mut response = String::new();
    if std::io::Read::read_to_string(&mut stream, &mut response).is_err() {
        return false;
    }
    let has = |metric: &str, expected: &str| {
        response.lines().any(|line| {
            line.split_once(' ')
                .is_some_and(|(name, value)| name == metric && value.trim() == expected)
        })
    };
    has("tc_raft_role", "2") && has("tc_ready", "1")
}

pub(crate) struct MatcherConnection {
    pub(crate) targets: Vec<MatcherTarget>,
    pub(crate) stream: Option<TcpStream>,
    pub(crate) active_target: Option<usize>,
}

impl MatcherConnection {
    pub(crate) fn new(targets: Vec<MatcherTarget>) -> Self {
        Self {
            targets,
            stream: None,
            active_target: None,
        }
    }

    pub(crate) fn connect(&mut self) -> std::io::Result<()> {
        for (index, target) in self.targets.iter().enumerate() {
            if target
                .metrics_addr
                .as_deref()
                .is_some_and(|metrics| !is_leader(metrics))
            {
                continue;
            }
            let Ok(stream) = TcpStream::connect(&target.order_addr) else {
                continue;
            };
            stream.set_nodelay(true)?;
            let timeout =
                Duration::from_millis(env_number("TC_RAFT_MATCHER_IO_TIMEOUT_MS", 5_000u64));
            stream.set_read_timeout(Some(timeout))?;
            stream.set_write_timeout(Some(timeout))?;
            self.stream = Some(stream);
            self.active_target = Some(index);
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no reachable Raft leader",
        ))
    }

    pub(crate) fn send_frames(&mut self, frames: &[[u8; wire::MSG_LEN]]) -> std::io::Result<()> {
        if frames.is_empty() || frames.len() > wire::RAFT_BATCH_MAX_COMMANDS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid matcher batch size",
            ));
        }
        let mut payload = Vec::with_capacity(8 + frames.len() * wire::MSG_LEN);
        payload.extend_from_slice(b"TCB1");
        payload.extend_from_slice(&(frames.len() as u32).to_be_bytes());
        for frame in frames {
            payload.extend_from_slice(frame);
        }
        for _ in 0..2 {
            if self.stream.is_none() {
                self.connect()?;
            }
            let stream = self.stream.as_mut().expect("connected matcher stream");
            let mut ack = [0u8; 9];
            if stream.write_all(&payload).is_ok()
                && stream.read_exact(&mut ack).is_ok()
                && ack[0] == 1
                && u64::from_be_bytes(ack[1..].try_into().expect("Raft ACK index")) > 0
            {
                return Ok(());
            }
            self.stream = None;
            self.active_target = None;
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "Raft leader connection failed before quorum commit acknowledgement",
        ))
    }
}

pub(crate) fn parse_matcher_list(value: &str) -> Vec<MatcherTarget> {
    value
        .split(',')
        .filter_map(|item| {
            let (order_addr, metrics_addr) = item.trim().split_once('@')?;
            Some(MatcherTarget {
                order_addr: order_addr.to_string(),
                metrics_addr: Some(metrics_addr.to_string()),
            })
        })
        .collect()
}

pub(crate) fn parse_matcher_groups() -> Vec<Vec<MatcherTarget>> {
    if let Ok(value) = std::env::var("TC_RAFT_GROUP_MATCHERS") {
        let groups = value.split(';').map(parse_matcher_list).collect::<Vec<_>>();
        if !groups.is_empty() && groups.iter().all(|group| !group.is_empty()) {
            return groups;
        }
        log_warn!("order-api", "ignored invalid TC_RAFT_GROUP_MATCHERS");
    }
    if let Ok(value) = std::env::var("TC_RAFT_MATCHERS") {
        let targets = parse_matcher_list(&value);
        if !targets.is_empty() {
            return vec![targets];
        }
    }
    vec![vec![MatcherTarget {
        order_addr: std::env::var("TC_MATCHER_ADDR").unwrap_or_else(|_| "trade-core:9001".into()),
        metrics_addr: None,
    }]]
}

// ---------------------------------------------------------------------------
// Order queries: Binance-style reads served purely from the MySQL projection.
// They lag the engine by the projection delay; real-time state is the user
// push stream's job, which does not go through MySQL at all.
// ---------------------------------------------------------------------------
