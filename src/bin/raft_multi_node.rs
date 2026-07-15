//! Hosts multiple independent five-replica Raft groups on one physical node.

use std::path::PathBuf;
use std::process::{Child, Command, ExitCode};
use std::thread;
use std::time::Duration;

const REPLICAS: u16 = 5;

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
    assert!((1..=REPLICAS).contains(&node), "node id must be 1..=5");
    assert!(groups > 0, "at least one Raft group is required");

    let binary = std::env::var("TC_RAFT_NODE_BIN").unwrap_or_else(|_| "raft-node".into());
    let mut children = Vec::with_capacity(groups as usize);
    for group in 0..groups {
        let raft_port = group_port(7000, group, node);
        let order_port = 9001 + group * 10;
        let md_port = 9101 + group * 10;
        let metrics_port = 9102 + group * 10;
        let peers = (1..=REPLICAS)
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
        eprintln!(
            "[raft-multi-node] node={node} group={group} raft={raft_port} order={order_port} metrics={metrics_port} data={}",
            data_dir.display()
        );
        let child = Command::new(&binary)
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
                    eprintln!(
                        "[raft-multi-node] child group {index} exited with {status}; stopping node"
                    );
                    stop(&mut children);
                    return ExitCode::FAILURE;
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!("[raft-multi-node] failed to inspect group {index}: {error}");
                    stop(&mut children);
                    return ExitCode::FAILURE;
                }
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}
