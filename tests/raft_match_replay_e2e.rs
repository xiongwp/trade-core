use std::time::Duration;

use trade_core::asset_log::{replay_with_reports, AssetJournalSet};
use trade_core::exchange::{Command, Processor};
use trade_core::order::Order;
use trade_core::raft_log::{ClusterConfig, RaftNode, CLUSTER_SIZE};
use trade_core::strategy::PriceTimePriority;
use trade_core::types::{InstrumentId, OrderId, Side};
use trade_core::wire::{self, MSG_LEN, REPORT_LEN};

const VOTERS: [u64; CLUSTER_SIZE] = [1, 2, 3, 4, 5];

fn pump(leader: &mut RaftNode, followers: &mut [RaftNode]) {
    for _ in 0..100 {
        let mut worked = false;
        for message in leader.take_outbound() {
            followers
                .iter_mut()
                .find(|node| node.id() == message.to)
                .unwrap()
                .step(message)
                .unwrap();
            worked = true;
        }
        for follower in &mut *followers {
            for message in follower.take_outbound() {
                if message.to == leader.id() {
                    leader.step(message).unwrap();
                    worked = true;
                }
            }
        }
        if !worked {
            return;
        }
    }
    panic!("raft traffic did not quiesce");
}

fn report_fingerprint(reports: &[trade_core::ExecReport]) -> u64 {
    let mut bytes = Vec::with_capacity(reports.len() * REPORT_LEN);
    for report in reports {
        let mut frame = [0u8; REPORT_LEN];
        wire::encode_report(report, &mut frame);
        bytes.extend_from_slice(&frame);
    }
    trade_core::journal::fnv1a(&bytes)
}

#[test]
fn quorum_committed_orders_replay_to_the_identical_match_result_after_restart() {
    let root = std::env::temp_dir().join(format!("tc-raft-match-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let state = root.join("raft.state");
    let config = ClusterConfig::new(1, VOTERS).unwrap();
    let mut leader = RaftNode::open(config.clone(), &state).unwrap();
    let mut followers = (2..=5)
        .map(|id| RaftNode::new(ClusterConfig::new(id, VOTERS).unwrap()).unwrap())
        .collect::<Vec<_>>();
    leader.campaign().unwrap();
    pump(&mut leader, &mut followers);
    assert!(leader.is_leader());

    let instrument = InstrumentId(7001);
    for order in [
        Order::limit(OrderId(101), Side::Sell, 100, 5).on(instrument),
        Order::limit(OrderId(102), Side::Buy, 100, 5).on(instrument),
    ] {
        let mut frame = [0u8; MSG_LEN];
        wire::encode_command(&Command::New(order), &mut frame);
        leader.propose(frame.to_vec()).unwrap();
        pump(&mut leader, &mut followers);
    }
    drop(leader);

    // Simulates a process dying after quorum commit. Restart exposes the
    // committed entries so the matching state machine can complete recovery.
    let mut restarted = RaftNode::open(config, &state).unwrap();
    let committed = restarted.take_committed();
    assert_eq!(committed.len(), 2);

    let mut asset_logs = AssetJournalSet::open(root.join("assets"), Duration::from_millis(1)).unwrap();
    let mut live = Processor::new(|| Box::new(PriceTimePriority), None);
    let mut live_reports = Vec::new();
    for (_, payload) in committed {
        let command = wire::WireView::parse(&payload)
            .and_then(|view| view.to_command())
            .expect("valid committed order frame");
        asset_logs.append(&command).unwrap();
        live.process(command, &mut |report| live_reports.push(report));
    }
    asset_logs.flush_all().unwrap();

    let replay = replay_with_reports(&root.join("assets"), instrument, || Box::new(PriceTimePriority)).unwrap();
    assert_eq!(report_fingerprint(&live_reports), replay.fingerprint);
    assert_eq!(replay.meta.records, 2);
    assert_eq!(replay.processor.engine(instrument).unwrap().book().len(), 0);
    std::fs::remove_dir_all(root).ok();
}
