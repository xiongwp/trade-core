//! Hosts multiple independent five-replica Raft groups on one physical node.

use std::path::PathBuf;
use std::process::{Child, Command, ExitCode};
use std::thread;
use std::time::Duration;

use trade_core::{log_error, log_info};

/// Default replication factor. Override with `TC_RAFT_REPLICAS` (3, 5, 7, 9).
const DEFAULT_REPLICAS: u16 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupPlacement {
    group: u16,
    local_node: u16,
    peers: String,
    raft_port: u16,
}

fn group_port(base: u16, group: u16, node: u16) -> u16 {
    base + group * 10 + node
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
        .collect()
}

fn stop(children: &mut [Child]) {
    for child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
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
                    .map(|peer| format!("{peer}@raft-{peer}:{}", group_port(7000, group, peer)))
                    .collect::<Vec<_>>()
                    .join(","),
                raft_port: group_port(7000, group, node),
            })
            .collect(),
    };
    assert!(
        !placements.is_empty(),
        "at least one local Raft replica is required"
    );
    let binary = std::env::var("TC_RAFT_NODE_BIN").unwrap_or_else(|_| "raft-node".into());
    let mut children = Vec::with_capacity(placements.len());
    for placement in placements {
        let group = placement.group;
        let local_node = placement.local_node;
        let raft_port = placement.raft_port;
        let order_port = 9001 + group * 10;
        let md_port = 9101 + group * 10;
        let metrics_port = 9102 + group * 10;
        // Keep the existing single-group directory compatible. Every added
        // group gets a fully isolated state, journal and per-asset WAL tree.
        let data_dir = if group == 0 {
            data_root.clone()
        } else {
            data_root.join(format!("group-{group}"))
        };
        std::fs::create_dir_all(&data_dir).expect("create Raft group data directory");
        log_info!(
            "raft-multi-node",
            "node={local_node} group={group} raft={raft_port} order={order_port} metrics={metrics_port} data={}",
            data_dir.display()
        );
        let child = Command::new(&binary)
            .env("TC_RAFT_GROUP_ID", group.to_string())
            .arg(local_node.to_string())
            // Bind on all interfaces: binding to the service hostname needs a
            // DNS round-trip at startup and panics the node when the embedded
            // DNS is slow/unready (observed on fresh compose networks). Peers
            // still *dial* raft-{n} by name, and that path retries.
            .arg(format!("0.0.0.0:{raft_port}"))
            .arg(placement.peers)
            .arg(format!("0.0.0.0:{order_port}"))
            .arg(data_dir)
            .arg(format!("0.0.0.0:{md_port}"))
            .arg(format!("0.0.0.0:{metrics_port}"))
            .spawn()
            .expect("start Raft group process");
        children.push(child);
    }

    loop {
        for index in 0..children.len() {
            match children[index].try_wait() {
                Ok(Some(status)) => {
                    log_error!(
                        "raft-multi-node",
                        "child group {index} exited with {status}; stopping node"
                    );
                    stop(&mut children);
                    return ExitCode::FAILURE;
                }
                Ok(None) => {}
                Err(error) => {
                    log_error!(
                        "raft-multi-node",
                        "failed to inspect group {index}: {error}"
                    );
                    stop(&mut children);
                    return ExitCode::FAILURE;
                }
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
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
    fn placement_requires_the_local_node_in_peer_set() {
        assert!(parse_placements("7|3|1@rack-a:7071,2@rack-b:7072").is_err());
    }
}
