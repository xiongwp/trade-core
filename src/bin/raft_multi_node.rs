//! Hosts multiple independent five-replica Raft groups on one physical node.

use std::path::PathBuf;
use std::process::{Child, Command, ExitCode};
use std::thread;
use std::time::Duration;

use trade_core::{log_error, log_info};

/// Default replication factor. Override with `TC_RAFT_REPLICAS` (3, 5, 7, 9).
const DEFAULT_REPLICAS: u16 = 5;

fn group_port(base: u16, group: u16, node: u16) -> u16 {
    base + group * 10 + node
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
    assert!(
        (1..=9).contains(&replicas),
        "replication factor must be 1..=9"
    );
    assert!(
        (1..=replicas).contains(&node),
        "node id must be 1..={replicas}"
    );
    assert!(groups > 0, "at least one Raft group is required");

    let binary = std::env::var("TC_RAFT_NODE_BIN").unwrap_or_else(|_| "raft-node".into());
    let mut children = Vec::with_capacity(groups as usize);
    for group in 0..groups {
        let raft_port = group_port(7000, group, node);
        let order_port = 9001 + group * 10;
        let md_port = 9101 + group * 10;
        let metrics_port = 9102 + group * 10;
        let peers = (1..=replicas)
            .map(|peer| format!("{peer}@raft-{peer}:{}", group_port(7000, group, peer)))
            .collect::<Vec<_>>()
            .join(",");
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
            "node={node} group={group} raft={raft_port} order={order_port} metrics={metrics_port} data={}",
            data_dir.display()
        );
        let child = Command::new(&binary)
            .env("TC_RAFT_GROUP_ID", group.to_string())
            .arg(node.to_string())
            .arg(format!("raft-{node}:{raft_port}"))
            .arg(peers)
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
                    log_error!("raft-multi-node", "failed to inspect group {index}: {error}");
                    stop(&mut children);
                    return ExitCode::FAILURE;
                }
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}
