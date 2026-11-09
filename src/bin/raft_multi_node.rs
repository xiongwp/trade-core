//! Hosts multiple independent five-replica Raft groups on one physical node.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use trade_core::{log_error, log_info, log_warn};

/// Default replication factor. Override with `TC_RAFT_REPLICAS` (3, 5, 7, 9).
const DEFAULT_REPLICAS: u16 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupPlacement {
    group: u16,
    local_node: u16,
    peers: String,
    raft_port: u16,
}

/// Default Raft port of `node` in `group` (implicit placements):
///
/// | groups     | port              | example          |
/// |------------|-------------------|------------------|
/// | 0-9        | `7000 + 10g + n`  | G0 N1 = 7001     |
/// | 10-2552    | `40000 + 10g + n` | G10 N1 = 40101   |
///
/// Groups 0-9 keep the original layout; from group 10 on it ran into the
/// service ports (group 200 node 1 = 9001 = group 0's order port). Each
/// group owns ten ports, so `node` must be 1-9: node 10 would be the next
/// group's node 0. `None` for such a node, and when the port would not fit
/// a u16 (every node of a group fits only up to group 2552: G2552 N9 =
/// 65529, while G2553 N9 would be 65539).
fn raft_port(group: u16, node: u16) -> Option<u16> {
    if !(1..=9).contains(&node) {
        return None;
    }
    let base: u32 = if group < 10 { 7_000 } else { 40_000 };
    u16::try_from(base + 10 * group as u32 + node as u32).ok()
}

/// (order, market-data, metrics) ports of `group`. Groups 0-9 keep the
/// original layout (the compose files map those). It made group g+10's order
/// port equal group g's market-data port, so groups from 10 on get three
/// consecutive ports from 20000 instead.
fn service_ports(group: u16) -> Option<(u16, u16, u16)> {
    if group < 10 {
        let g = group * 10;
        return Some((9_001 + g, 9_101 + g, 9_102 + g));
    }
    let base = u16::try_from(20_000 + 3 * group as u32).ok()?;
    Some((base, base.checked_add(1)?, base.checked_add(2)?))
}

/// Every port this node will bind must be unique and in range; a collision
/// used to surface as a bind panic that crash-looped the whole node.
fn check_ports(placements: &[GroupPlacement]) -> Result<(), String> {
    let mut used = std::collections::HashMap::new();
    for placement in placements {
        let (order, md, metrics) = service_ports(placement.group)
            .ok_or_else(|| format!("group {} has no port layout (too large)", placement.group))?;
        for (port, role) in [
            (placement.raft_port, "raft"),
            (order, "order"),
            (md, "market-data"),
            (metrics, "metrics"),
        ] {
            if port == 0 {
                return Err(format!("group {} {role} port is 0", placement.group));
            }
            if let Some((group, other)) = used.insert(port, (placement.group, role)) {
                return Err(format!(
                    "port {port}: group {} {role} collides with group {group} {other}",
                    placement.group
                ));
            }
        }
    }
    Ok(())
}

/// Parse `group|local_node|id@host:port,id@host:port;...`. Each machine gets
/// only the entries assigned to it, allowing hundreds of groups to be spread
/// over many physical hosts instead of starting every group everywhere.
fn parse_placements(value: &str) -> Result<Vec<GroupPlacement>, String> {
    value
        .split(';')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let mut fields = entry.trim().splitn(3, '|');
            let group = fields
                .next()
                .ok_or("missing group")?
                .parse::<u16>()
                .map_err(|_| format!("invalid group in {entry}"))?;
            let local_node = fields
                .next()
                .ok_or("missing local node")?
                .parse::<u16>()
                .map_err(|_| format!("invalid local node in {entry}"))?;
            let peers = fields.next().ok_or("missing peers")?.to_string();
            let local = peers
                .split(',')
                .find_map(|peer| {
                    let (id, addr) = peer.split_once('@')?;
                    (id.parse::<u16>().ok()? == local_node).then_some(addr)
                })
                .ok_or_else(|| format!("local node {local_node} absent from group {group}"))?;
            let raft_port = local
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
                .ok_or_else(|| format!("invalid local peer address {local}"))?;
            Ok(GroupPlacement {
                group,
                local_node,
                peers,
                raft_port,
            })
        })
        .collect::<Result<Vec<_>, String>>()
        .and_then(|placements: Vec<GroupPlacement>| {
            // Two entries for one group would start two processes on the same
            // data directory.
            let mut seen = std::collections::HashSet::new();
            for placement in &placements {
                if !seen.insert(placement.group) {
                    return Err(format!("group {} is placed twice", placement.group));
                }
            }
            Ok(placements)
        })
}

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Delay before restarting a child that exited with `code` after running for
/// `ran`, given `restarts` recent consecutive restarts. A clean exit (0) is a
/// deliberate restart — e.g. after installing a Raft snapshot — and comes back
/// at once unless it keeps repeating; a crash backs off 1 s, 2 s, 4 s ... up to 30 s, and the streak
/// resets once a child has stayed up for a minute. A config mismatch (78)
/// waits the maximum: it will not fix itself.
fn restart_delay(code: Option<i32>, ran: Duration, restarts: u32) -> Duration {
    match code {
        // A clean exit that keeps repeating (a follower that is sent a new
        // snapshot on every start, or a child stopped from outside) backs off
        // like a crash instead of respawning ten times a second.
        Some(0) if ran < Duration::from_secs(60) && restarts >= 3 => {
            Duration::from_secs(1u64 << (restarts - 2).min(5)).min(Duration::from_secs(30))
        }
        Some(0) => Duration::from_millis(100),
        Some(78) => Duration::from_secs(30),
        _ if ran >= Duration::from_secs(60) => Duration::from_secs(1),
        _ => Duration::from_secs(1u64 << restarts.min(5)).min(Duration::from_secs(30)),
    }
}

/// One supervised Raft group process.
struct Slot {
    placement: GroupPlacement,
    data_dir: PathBuf,
    ports: (u16, u16, u16),
    child: Option<Child>,
    started: Instant,
    restarts: u32,
    restart_at: Option<Instant>,
    probe_failures: u32,
    next_probe: Instant,
}

impl Slot {
    fn spawn(&mut self, binary: &str) {
        let (order_port, md_port, metrics_port) = self.ports;
        let group = self.placement.group;
        log_info!(
            "raft-multi-node",
            "node={} group={group} raft={} order={order_port} metrics={metrics_port} data={}",
            self.placement.local_node,
            self.placement.raft_port,
            self.data_dir.display()
        );
        match Command::new(binary)
            .env("TC_RAFT_GROUP_ID", group.to_string())
            .arg(self.placement.local_node.to_string())
            // Bind on all interfaces: binding to the service hostname needs a
            // DNS round-trip at startup and panics the node when the embedded
            // DNS is slow/unready (observed on fresh compose networks). Peers
            // still *dial* raft-{n} by name, and that path retries.
            .arg(format!("0.0.0.0:{}", self.placement.raft_port))
            .arg(&self.placement.peers)
            .arg(format!("0.0.0.0:{order_port}"))
            .arg(&self.data_dir)
            .arg(format!("0.0.0.0:{md_port}"))
            .arg(format!("0.0.0.0:{metrics_port}"))
            .spawn()
        {
            Ok(child) => self.child = Some(child),
            Err(error) => {
                log_error!("raft-multi-node", "group={group} event=spawn_failed error={error}");
                self.restart_at = Some(Instant::now() + Duration::from_secs(5));
            }
        }
        self.started = Instant::now();
        self.probe_failures = 0;
        self.next_probe = Instant::now();
    }
}

/// Liveness: does the child answer `GET /metrics` on its metrics port?
fn probe(port: u16) -> bool {
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_secs(2),
    ) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    if stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut head = [0u8; 12];
    stream.read_exact(&mut head).is_ok() && head.starts_with(b"HTTP/1.1 200")
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() -> ExitCode {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("raft-multi-node");
    let mut args = std::env::args().skip(1);
    let node: u16 = args
        .next()
        .expect("node id")
        .parse()
        .expect("numeric node id");
    let groups: u16 = args
        .next()
        .expect("raft group count")
        .parse()
        .expect("numeric raft group count");
    let data_root = PathBuf::from(args.next().expect("data root"));
    let replicas: u16 = std::env::var("TC_RAFT_REPLICAS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_REPLICAS);
    let explicit_placements = std::env::var("TC_RAFT_GROUP_PLACEMENTS").ok();
    assert!(
        (1..=9).contains(&replicas),
        "replication factor must be 1..=9"
    );
    if explicit_placements.is_none() {
        assert!(
            (1..=replicas).contains(&node),
            "node id must be 1..={replicas}"
        );
    }
    assert!(groups > 0, "at least one Raft group is required");

    let placements = match explicit_placements {
        Some(value) => parse_placements(&value).expect("valid TC_RAFT_GROUP_PLACEMENTS"),
        None => (0..groups)
            .map(|group| GroupPlacement {
                group,
                local_node: node,
                peers: (1..=replicas)
                    .map(|peer| {
                        let port = raft_port(group, peer).expect("Raft port fits u16");
                        format!("{peer}@raft-{peer}:{port}")
                    })
                    .collect::<Vec<_>>()
                    .join(","),
                raft_port: raft_port(group, node).expect("Raft port fits u16"),
            })
            .collect(),
    };
    assert!(
        !placements.is_empty(),
        "at least one local Raft replica is required"
    );
    if let Err(error) = check_ports(&placements) {
        panic!("invalid Raft group port layout: {error}");
    }
    let binary = std::env::var("TC_RAFT_NODE_BIN").unwrap_or_else(|_| "raft-node".into());
    // Liveness: a child whose metrics port stops answering for this many
    // consecutive 5 s probes (after a start-up grace for WAL recovery) is
    // killed and restarted like a crashed one.
    let liveness_grace = Duration::from_secs(env_u64("TC_SUPERVISOR_LIVENESS_GRACE_SECS", 120));
    let liveness_failures = env_u64("TC_SUPERVISOR_LIVENESS_FAILURES", 6).max(1) as u32;
    unsafe {
        let handler = on_signal as *const () as libc::sighandler_t;
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }

    let now = Instant::now();
    let mut slots: Vec<Slot> = placements
        .into_iter()
        .map(|placement| {
            let group = placement.group;
            // One isolated directory per group (state, journal, per-asset WAL
            // tree), side by side, so a replica can be rebuilt by moving just
            // its own directory aside (scripts/ops/rebuild-replica.sh).
            let data_dir = data_root.join(format!("group-{group}"));
            std::fs::create_dir_all(&data_dir).expect("create Raft group data directory");
            Slot {
                ports: service_ports(group).expect("checked above"),
                placement,
                data_dir,
                child: None,
                started: now,
                restarts: 0,
                restart_at: Some(now),
                probe_failures: 0,
                next_probe: now,
            }
        })
        .collect();

    // Each group is supervised on its own: one group crashing (or exiting on
    // purpose to load an installed snapshot) used to SIGKILL every group on
    // the machine and exit.
    while !SHUTDOWN.load(Ordering::SeqCst) {
        let now = Instant::now();
        for slot in &mut slots {
            let group = slot.placement.group;
            if let Some(child) = slot.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let ran = slot.started.elapsed();
                        if ran >= Duration::from_secs(60) {
                            slot.restarts = 0;
                        }
                        let delay = restart_delay(status.code(), ran, slot.restarts);
                        slot.restarts += 1;
                        slot.child = None;
                        slot.restart_at = Some(now + delay);
                        log_warn!(
                            "raft-multi-node",
                            "group={group} event=child_exited status={status} ran_ms={} restart_in_ms={}",
                            ran.as_millis(),
                            delay.as_millis()
                        );
                    }
                    Ok(None) => {
                        if slot.started.elapsed() >= liveness_grace && now >= slot.next_probe {
                            slot.next_probe = now + Duration::from_secs(5);
                            if probe(slot.ports.2) {
                                slot.probe_failures = 0;
                            } else {
                                slot.probe_failures += 1;
                                if slot.probe_failures >= liveness_failures {
                                    log_error!(
                                        "raft-multi-node",
                                        "group={group} event=child_unresponsive probes={}; killing",
                                        slot.probe_failures
                                    );
                                    let _ = child.kill();
                                }
                            }
                        }
                    }
                    Err(error) => {
                        log_error!("raft-multi-node", "group={group} event=inspect_failed error={error}");
                    }
                }
            } else if slot.restart_at.is_some_and(|at| now >= at) {
                slot.restart_at = None;
                slot.spawn(&binary);
            }
        }
        thread::sleep(Duration::from_millis(250));
    }

    // Graceful stop: forward SIGTERM so every group flushes and snapshots,
    // then force whatever has not exited after 20 s.
    log_info!("raft-multi-node", "event=shutdown children={}", slots.len());
    for slot in &slots {
        if let Some(child) = &slot.child {
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    for slot in &mut slots {
        if let Some(child) = slot.child.as_mut() {
            while Instant::now() < deadline {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_assigns_only_explicit_local_replicas() {
        let placements = parse_placements(
            "7|2|1@rack-a:7071,2@rack-b:7072,3@rack-c:7073;19|1|1@rack-b:7191,2@rack-d:7192,3@rack-e:7193",
        )
        .unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].group, 7);
        assert_eq!(placements[0].local_node, 2);
        assert_eq!(placements[0].raft_port, 7072);
        assert_eq!(placements[1].group, 19);
        assert_eq!(placements[1].raft_port, 7191);
    }

    #[test]
    fn a_group_placed_twice_is_rejected() {
        assert!(parse_placements("7|1|1@a:7071;7|2|1@a:7071,2@b:7072").is_err());
    }

    #[test]
    fn groups_below_ten_keep_their_ports() {
        assert_eq!(raft_port(0, 1), Some(7_001));
        assert_eq!(raft_port(3, 5), Some(7_035));
        assert_eq!(service_ports(0), Some((9_001, 9_101, 9_102)));
        assert_eq!(service_ports(3), Some((9_031, 9_131, 9_132)));
    }

    #[test]
    fn ports_never_collide_for_every_group_that_fits() {
        let placements: Vec<GroupPlacement> = (0..2_553)
            .map(|group| GroupPlacement {
                group,
                local_node: 9,
                peers: String::new(),
                raft_port: raft_port(group, 9).unwrap(),
            })
            .collect();
        check_ports(&placements).unwrap();
        // Every node id of a group shares the service ports but not the Raft
        // one, and other nodes' Raft ports must not hit this node's services.
        let mut all = std::collections::HashSet::new();
        for group in 0..2_553u16 {
            let (order, md, metrics) = service_ports(group).unwrap();
            assert!(all.insert(order) && all.insert(md) && all.insert(metrics), "group {group}");
            for node in 1..=9 {
                assert!(all.insert(raft_port(group, node).unwrap()), "group {group} node {node}");
            }
        }
    }

    #[test]
    fn raft_ports_reject_node_ids_outside_one_to_nine_and_oversized_groups() {
        assert_eq!(raft_port(0, 0), None);
        assert_eq!(raft_port(0, 10), None, "node 10 would be group 1 node 0");
        assert_eq!(raft_port(10, 1), Some(40_101));
        assert_eq!(raft_port(2_552, 9), Some(65_529));
        assert_eq!(raft_port(2_553, 9), None, "65539 does not fit a u16");
    }

    #[test]
    fn colliding_explicit_ports_are_refused() {
        let placements = parse_placements("0|1|1@a:9101;1|1|1@a:7011").unwrap();
        assert!(check_ports(&placements).unwrap_err().contains("collides"));
    }

    #[test]
    fn restarts_back_off_and_a_clean_exit_comes_back_at_once() {
        let short = Duration::from_secs(1);
        assert_eq!(restart_delay(Some(0), short, 0), Duration::from_millis(100));
        assert_eq!(restart_delay(Some(0), short, 9), Duration::from_secs(30), "a clean-exit loop backs off");
        assert_eq!(restart_delay(Some(0), Duration::from_secs(90), 9), Duration::from_millis(100));
        assert_eq!(restart_delay(Some(101), short, 0), Duration::from_secs(1));
        assert_eq!(restart_delay(Some(101), short, 3), Duration::from_secs(8));
        assert_eq!(restart_delay(None, short, 20), Duration::from_secs(30));
        assert_eq!(restart_delay(Some(78), short, 0), Duration::from_secs(30));
        assert_eq!(restart_delay(Some(101), Duration::from_secs(90), 7), Duration::from_secs(1));
    }

    #[test]
    fn placement_requires_the_local_node_in_peer_set() {
        assert!(parse_placements("7|3|1@rack-a:7071,2@rack-b:7072").is_err());
    }
}
