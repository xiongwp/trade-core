//! Data written before wire frames grew from 40 to 48 bytes (Raft batch v1,
//! journal format v2) must still decode and replay exactly as before. The
//! fixtures here are built byte-for-byte in the old layouts rather than with
//! today's encoders.

use std::path::PathBuf;
use std::time::Duration;

use trade_core::exchange::{replay_journal, Command};
use trade_core::journal::{fnv1a, JournalReader, JournalWriter};
use trade_core::prelude::*;
use trade_core::wire::{self, MSG_LEN, MSG_LEN_V1};

fn commands() -> Vec<Command> {
    let sym = InstrumentId(3);
    vec![
        Command::New(Order::limit(OrderId(1), Side::Sell, 100, 10).on(sym).by(7)),
        Command::New(Order::limit(OrderId(2), Side::Sell, 101, 5).on(sym).by(8)),
        Command::New(Order::limit(OrderId(3), Side::Buy, 101, 12).on(sym).by(9)),
        Command::Modify {
            instrument: sym,
            order_id: OrderId(2),
            new_price: 101,
            new_qty: 2,
            cmd_id: 4,
        },
        Command::Cancel {
            instrument: sym,
            order_id: OrderId(2),
            cmd_id: 5,
        },
    ]
}

/// Today's frames, cut to the old 40-byte width (the tail is zero for every
/// existing command, so this is exactly what the old encoder produced).
fn v1_frames() -> Vec<[u8; MSG_LEN_V1]> {
    commands()
        .iter()
        .map(|command| {
            let mut frame = [0u8; MSG_LEN];
            wire::encode_command(command, &mut frame);
            assert!(frame[MSG_LEN_V1..].iter().all(|&b| b == 0));
            frame[..MSG_LEN_V1].try_into().unwrap()
        })
        .collect()
}

fn debug(commands: &[Command]) -> Vec<String> {
    commands.iter().map(|c| format!("{c:?}")).collect()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tc-frame-v1-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn raft_wal_entries_written_with_40_byte_frames_still_decode() {
    let old = v1_frames();
    // Batch version 1: magic, version 1, count, 40-byte frames.
    let mut entry = b"RB01".to_vec();
    entry.extend_from_slice(&1u32.to_le_bytes());
    entry.extend_from_slice(&(old.len() as u32).to_le_bytes());
    for frame in &old {
        entry.extend_from_slice(frame);
    }
    let decoded = wire::decode_raft_entry(&entry).expect("v1 batch decodes");
    assert_eq!(debug(&decoded), debug(&commands()));

    // A pre-batch single-command entry is a bare 40-byte frame.
    let single = wire::decode_raft_entry(&old[0]).expect("legacy single entry");
    assert_eq!(debug(&single), debug(&commands()[..1]));

    // New entries are version 2 and round-trip.
    let frames: Vec<[u8; MSG_LEN]> = commands()
        .iter()
        .map(|c| {
            let mut f = [0u8; MSG_LEN];
            wire::encode_command(c, &mut f);
            f
        })
        .collect();
    let v2 = wire::encode_raft_batch(&frames).unwrap();
    assert_eq!(u32::from_le_bytes(v2[4..8].try_into().unwrap()), 2);
    assert_eq!(
        debug(&wire::decode_raft_entry(&v2).unwrap()),
        debug(&commands())
    );

    // A v1 header with v2-sized frames (or any other mismatch) is refused.
    let mut bad = entry.clone();
    bad[4..8].copy_from_slice(&2u32.to_le_bytes());
    assert!(wire::decode_raft_entry(&bad).is_none());
    let mut unknown = entry;
    unknown[4..8].copy_from_slice(&3u32.to_le_bytes());
    assert!(wire::decode_raft_entry(&unknown).is_none());
}

/// Write a format-v2 journal: header `TCJR\x02`, 64-byte records
/// (seq, ts, 40-byte frame, FNV-1a over the first 56 bytes).
fn write_v2_journal(path: &PathBuf, frames: &[[u8; MSG_LEN_V1]], torn_tail: bool) {
    let mut bytes = b"TCJR\x02\0\0\0".to_vec();
    for (i, frame) in frames.iter().enumerate() {
        let mut rec = [0u8; 64];
        rec[0..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
        rec[8..16].copy_from_slice(&(1_000 + i as u64).to_le_bytes());
        rec[16..56].copy_from_slice(frame);
        let h = fnv1a(&rec[..56]);
        rec[56..].copy_from_slice(&h.to_le_bytes());
        bytes.extend_from_slice(&rec);
    }
    if torn_tail {
        bytes.extend_from_slice(&[0xAB; 30]); // a crash mid-record
    }
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn v2_journal_is_read_widened_and_migrated_on_append() {
    let dir = temp_dir("journal");
    let path = dir.join("journal-shard-0.bin");
    let old = v1_frames();
    write_v2_journal(&path, &old, true);

    // Read in place: every record, frames widened with a zero tail.
    let records: Vec<_> = JournalReader::open(&path).unwrap().collect();
    assert_eq!(records.len(), old.len(), "torn tail dropped, prefix kept");
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.seq, i as u64 + 1);
        assert_eq!(record.ts_nanos, 1_000 + i as u64);
        assert_eq!(&record.frame[..MSG_LEN_V1], &old[i][..]);
        assert!(record.frame[MSG_LEN_V1..].iter().all(|&b| b == 0));
    }

    // Opening for append rewrites the file as v3, then appends normally.
    let mut writer = JournalWriter::open(&path, Duration::from_millis(1)).unwrap();
    writer.resume_from(old.len() as u64);
    let mut extra = [0u8; MSG_LEN];
    wire::encode_command(
        &Command::Cancel {
            instrument: InstrumentId(3),
            order_id: OrderId(1),
            cmd_id: 6,
        },
        &mut extra,
    );
    writer.append(9_999, &extra).unwrap();
    writer.flush().unwrap();
    drop(writer);

    let header = std::fs::read(&path).unwrap()[..8].to_vec();
    assert_eq!(&header, b"TCJR\x03\0\0\0");
    let migrated: Vec<_> = JournalReader::open(&path).unwrap().collect();
    assert_eq!(migrated.len(), old.len() + 1);
    for (before, after) in records.iter().zip(&migrated) {
        assert_eq!(
            (before.seq, before.ts_nanos, before.frame),
            (after.seq, after.ts_nanos, after.frame)
        );
    }
    assert_eq!(migrated.last().unwrap().seq, old.len() as u64 + 1);
    assert_eq!(migrated.last().unwrap().frame, extra);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn replaying_a_v2_journal_matches_replaying_the_same_commands_natively() {
    let dir = temp_dir("replay");
    let old_path = dir.join("old.bin");
    write_v2_journal(&old_path, &v1_frames(), false);

    let new_path = dir.join("new.bin");
    let mut writer = JournalWriter::open(&new_path, Duration::from_millis(1)).unwrap();
    for (i, command) in commands().iter().enumerate() {
        let mut frame = [0u8; MSG_LEN];
        wire::encode_command(command, &mut frame);
        writer.append(1_000 + i as u64, &frame).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let strategy = || Box::new(PriceTimePriority) as Box<dyn MatchingStrategy>;
    let old = replay_journal(&old_path, strategy, None, None).unwrap();
    let new = replay_journal(&new_path, strategy, None, None).unwrap();
    assert_eq!(old.commands, commands().len() as u64);
    assert!(
        old.reports.len() > old.commands as usize,
        "trades were produced"
    );
    assert_eq!(old.reports, new.reports);
    assert_eq!(old.fingerprint, new.fingerprint);
    assert_eq!(
        old.processor.state_fingerprint(),
        new.processor.state_fingerprint()
    );
    std::fs::remove_dir_all(&dir).ok();
}
