//! Point-in-time state snapshots — turning "replay everything since genesis"
//! into "load snapshot + replay the tail".
//!
//! A snapshot captures, per shard: the shard's **journal sequence** at capture
//! time, and every engine's state (its sequence counter plus all resting
//! orders in time-priority order). Recovery is then:
//!
//! ```text
//! state = load(snapshot)                  // books restored instantly
//! for rec in journal where rec.seq > snapshot.journal_seq { apply(rec) }
//! ```
//!
//! Because the engine is deterministic and the journal is ordered, this equals
//! a full-history replay — verified by test against exactly that.
//!
//! **Journal truncation.** After a snapshot is durably written (temp file +
//! fsync + rename), the journal can be truncated to zero: everything up to
//! `journal_seq` is now redundant. If the process crashes *between* snapshot
//! and truncation, recovery still works — replay skips records with
//! `seq <= journal_seq`. Recovery time becomes O(commands since last snapshot)
//! instead of O(all commands ever).
//!
//! ## File format (little-endian)
//!
//! ```text
//! header : magic "TCS1" | version u32 | journal_seq u64 | engine_count u32
//! engine : instrument u32 | engine_seq u64 | order_count u64 | orders...
//! order  : 56 bytes (id, instrument, user, side, type, tif, price, qty,
//!          remaining, timestamp)
//! footer : FNV-1a-64 over all preceding bytes
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::journal::fnv1a;
use crate::order::Order;
use crate::types::*;

const MAGIC: &[u8; 4] = b"TCS1";
const VERSION: u32 = 4; // v4: + last durably applied Raft index
const LEGACY_VERSION: u32 = 3;
const ORDER_ENC: usize = 56;

fn encode_order(o: &Order, out: &mut [u8; ORDER_ENC]) {
    out[0..8].copy_from_slice(&o.id.0.to_le_bytes());
    out[8..12].copy_from_slice(&o.instrument.0.to_le_bytes());
    out[12..20].copy_from_slice(&o.user.to_le_bytes());
    out[20] = match o.side {
        Side::Buy => 0,
        Side::Sell => 1,
    };
    out[21] = match o.order_type {
        OrderType::Limit => 0,
        OrderType::Market => 1,
    };
    out[22] = match o.tif {
        TimeInForce::Gtc => 0,
        TimeInForce::Ioc => 1,
        TimeInForce::Fok => 2,
        TimeInForce::IocBudget => 3,
        TimeInForce::FokBudget => 4,
    };
    out[23] = 0; // pad
    out[24..32].copy_from_slice(&o.price.to_le_bytes());
    out[32..40].copy_from_slice(&o.quantity.to_le_bytes());
    out[40..48].copy_from_slice(&o.remaining.to_le_bytes());
    out[48..56].copy_from_slice(&o.timestamp.to_le_bytes());
}

fn decode_order(b: &[u8; ORDER_ENC]) -> Order {
    Order {
        id: OrderId(u64::from_le_bytes(b[0..8].try_into().unwrap())),
        instrument: InstrumentId(u32::from_le_bytes(b[8..12].try_into().unwrap())),
        user: u64::from_le_bytes(b[12..20].try_into().unwrap()),
        side: if b[20] == 0 { Side::Buy } else { Side::Sell },
        order_type: if b[21] == 1 {
            OrderType::Market
        } else {
            OrderType::Limit
        },
        tif: match b[22] {
            1 => TimeInForce::Ioc,
            2 => TimeInForce::Fok,
            3 => TimeInForce::IocBudget,
            4 => TimeInForce::FokBudget,
            _ => TimeInForce::Gtc,
        },
        price: u64::from_le_bytes(b[24..32].try_into().unwrap()),
        quantity: u64::from_le_bytes(b[32..40].try_into().unwrap()),
        remaining: u64::from_le_bytes(b[40..48].try_into().unwrap()),
        timestamp: u64::from_le_bytes(b[48..56].try_into().unwrap()),
    }
}

/// One engine's state inside a snapshot.
pub struct EngineState {
    pub instrument: InstrumentId,
    pub engine_seq: Timestamp,
    /// Resting orders in time-priority order.
    pub orders: Vec<Order>,
}

/// A decoded shard snapshot.
pub struct Snapshot {
    /// On-disk format version. Version 3 snapshots can be upgraded from their
    /// legacy shard journal; version 4 snapshots are self-describing for Raft
    /// WAL replay.
    pub format_version: u32,
    /// Journal sequence at capture time; replay records with seq > this.
    pub journal_seq: u64,
    /// Highest Raft entry represented by this exact in-memory state image.
    /// Recovery replays committed entries strictly above this index.
    pub raft_applied_index: u64,
    /// Idempotency high-water marks (dual dedup cursors: New / admin streams).
    pub max_cmd_id: u64,
    pub max_admin_id: u64,
    /// Halted instruments (circuit breaker state must survive snapshots).
    pub halted: Vec<u32>,
    /// Suspended users.
    pub suspended: Vec<u64>,
    /// Position ledger entries (user, instrument, net qty).
    pub positions: Vec<(u64, u32, i64)>,
    pub engines: Vec<EngineState>,
}

/// Borrowed state supplied when atomically writing a snapshot.
pub struct SnapshotData<'a> {
    pub journal_seq: u64,
    pub raft_applied_index: u64,
    pub max_cmd_id: u64,
    pub max_admin_id: u64,
    pub halted: &'a [u32],
    pub suspended: &'a [u64],
    pub positions: &'a [(u64, u32, i64)],
    pub engines: &'a [EngineState],
}

/// Write a snapshot **atomically**: temp file, fsync, rename over `path`.
pub fn write(path: &Path, data: SnapshotData<'_>) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&data.journal_seq.to_le_bytes());
    buf.extend_from_slice(&data.raft_applied_index.to_le_bytes());
    buf.extend_from_slice(&data.max_cmd_id.to_le_bytes());
    buf.extend_from_slice(&data.max_admin_id.to_le_bytes());
    buf.extend_from_slice(&(data.halted.len() as u32).to_le_bytes());
    for h in data.halted {
        buf.extend_from_slice(&h.to_le_bytes());
    }
    buf.extend_from_slice(&(data.suspended.len() as u32).to_le_bytes());
    for s in data.suspended {
        buf.extend_from_slice(&s.to_le_bytes());
    }
    buf.extend_from_slice(&(data.positions.len() as u32).to_le_bytes());
    for (u, i, q) in data.positions {
        buf.extend_from_slice(&u.to_le_bytes());
        buf.extend_from_slice(&i.to_le_bytes());
        buf.extend_from_slice(&q.to_le_bytes());
    }
    buf.extend_from_slice(&(data.engines.len() as u32).to_le_bytes());
    let mut rec = [0u8; ORDER_ENC];
    for e in data.engines {
        buf.extend_from_slice(&e.instrument.0.to_le_bytes());
        buf.extend_from_slice(&e.engine_seq.to_le_bytes());
        buf.extend_from_slice(&(e.orders.len() as u64).to_le_bytes());
        for o in &e.orders {
            encode_order(o, &mut rec);
            buf.extend_from_slice(&rec);
        }
    }
    let h = fnv1a(&buf);
    buf.extend_from_slice(&h.to_le_bytes());

    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?; // durable before it can become "the" snapshot
    }
    fs::rename(&tmp, path)?; // atomic on POSIX filesystems
                             // fsync the parent directory so the rename itself survives a crash;
                             // otherwise the directory entry may still point at the old (or no)
                             // snapshot after power loss even though the file data is durable.
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("fsync snapshot parent dir {}: {e}", parent.display()),
                )
            })?;
    }
    Ok(())
}

/// Atomically install an application snapshot received through Raft. The blob
/// is fully decoded and its embedded applied index is verified before rename,
/// so a corrupt or mismatched consensus snapshot can never replace the live
/// recovery point.
pub fn install_bytes(path: &Path, bytes: &[u8], expected_index: u64) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("installing");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    let decoded = load(&tmp)?;
    if decoded.format_version < 4 || decoded.raft_applied_index != expected_index {
        let _ = fs::remove_file(&tmp);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "matching snapshot index {} does not match Raft snapshot index {expected_index}",
                decoded.raft_applied_index
            ),
        ));
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Load a snapshot; `Err` on missing file, corruption, or version mismatch.
pub fn load(path: &Path) -> io::Result<Snapshot> {
    let mut buf = Vec::new();
    File::open(path)?.read_to_end(&mut buf)?;
    let corrupt = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());

    if buf.len() < 4 + 4 + 8 + 4 + 8 || &buf[0..4] != MAGIC {
        return Err(corrupt("bad magic/size"));
    }
    let body_len = buf.len() - 8;
    let stored = u64::from_le_bytes(buf[body_len..].try_into().unwrap());
    if fnv1a(&buf[..body_len]) != stored {
        return Err(corrupt("checksum mismatch"));
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != VERSION && version != LEGACY_VERSION {
        return Err(corrupt("unsupported version"));
    }
    let journal_seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let (raft_applied_index, mut pos) = if version >= 4 {
        (u64::from_le_bytes(buf[16..24].try_into().unwrap()), 24)
    } else {
        (0, 16)
    };
    let max_cmd_id = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
    pos += 8;
    let max_admin_id = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
    pos += 8;
    let n_halt = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut halted = Vec::with_capacity(n_halt);
    for _ in 0..n_halt {
        halted.push(u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()));
        pos += 4;
    }
    let n_susp = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut suspended = Vec::with_capacity(n_susp);
    for _ in 0..n_susp {
        suspended.push(u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap()));
        pos += 8;
    }
    let n_pos = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut positions = Vec::with_capacity(n_pos);
    for _ in 0..n_pos {
        positions.push((
            u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap()),
            u32::from_le_bytes(buf[pos + 8..pos + 12].try_into().unwrap()),
            i64::from_le_bytes(buf[pos + 12..pos + 20].try_into().unwrap()),
        ));
        pos += 20;
    }
    let engine_count = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut engines = Vec::with_capacity(engine_count);
    for _ in 0..engine_count {
        if pos + 20 > body_len {
            return Err(corrupt("truncated engine header"));
        }
        let instrument = InstrumentId(u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()));
        let engine_seq = u64::from_le_bytes(buf[pos + 4..pos + 12].try_into().unwrap());
        let n = u64::from_le_bytes(buf[pos + 12..pos + 20].try_into().unwrap()) as usize;
        pos += 20;
        if pos + n * ORDER_ENC > body_len {
            return Err(corrupt("truncated orders"));
        }
        let mut orders = Vec::with_capacity(n);
        for _ in 0..n {
            let rec: &[u8; ORDER_ENC] = buf[pos..pos + ORDER_ENC].try_into().unwrap();
            orders.push(decode_order(rec));
            pos += ORDER_ENC;
        }
        engines.push(EngineState {
            instrument,
            engine_seq,
            orders,
        });
    }
    Ok(Snapshot {
        format_version: version,
        journal_seq,
        raft_applied_index,
        max_cmd_id,
        max_admin_id,
        halted,
        suspended,
        positions,
        engines,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trips_and_detects_corruption() {
        let dir = std::env::temp_dir().join(format!("tc-snap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.bin");

        let engines = vec![EngineState {
            instrument: InstrumentId(7),
            engine_seq: 42,
            orders: vec![
                Order::limit(OrderId(1), Side::Buy, 99, 5)
                    .on(InstrumentId(7))
                    .by(11),
                Order::limit(OrderId(2), Side::Sell, 101, 3)
                    .on(InstrumentId(7))
                    .by(22),
            ],
        }];
        write(
            &path,
            SnapshotData {
                journal_seq: 1000,
                raft_applied_index: 777,
                max_cmd_id: 555,
                max_admin_id: 556,
                halted: &[7],
                suspended: &[42],
                positions: &[(9, 7, -3)],
                engines: &engines,
            },
        )
        .unwrap();

        let snap = load(&path).unwrap();
        assert_eq!(snap.journal_seq, 1000);
        assert_eq!(snap.raft_applied_index, 777);
        assert_eq!(snap.max_cmd_id, 555);
        assert_eq!(snap.max_admin_id, 556);
        assert_eq!(snap.halted, vec![7]);
        assert_eq!(snap.suspended, vec![42]);
        assert_eq!(snap.positions, vec![(9, 7, -3)]);
        assert_eq!(snap.engines.len(), 1);
        let e = &snap.engines[0];
        assert_eq!(e.instrument, InstrumentId(7));
        assert_eq!(e.engine_seq, 42);
        assert_eq!(e.orders.len(), 2);
        assert_eq!(e.orders[0].id, OrderId(1));
        assert_eq!(e.orders[0].user, 11);
        assert_eq!(e.orders[1].price, 101);

        // Version 3 had no explicit Raft index. It remains readable so an
        // existing snapshot+journal node can perform the one-time safe upgrade
        // to the Raft-authoritative format.
        let current = std::fs::read(&path).unwrap();
        let current_body = current.len() - 8;
        let mut legacy = Vec::with_capacity(current.len() - 8);
        legacy.extend_from_slice(MAGIC);
        legacy.extend_from_slice(&LEGACY_VERSION.to_le_bytes());
        legacy.extend_from_slice(&current[8..16]);
        legacy.extend_from_slice(&current[24..current_body]);
        legacy.extend_from_slice(&fnv1a(&legacy).to_le_bytes());
        std::fs::write(&path, &legacy).unwrap();
        let legacy_snap = load(&path).unwrap();
        assert_eq!(legacy_snap.format_version, 3);
        assert_eq!(legacy_snap.raft_applied_index, 0);
        assert_eq!(legacy_snap.journal_seq, 1000);
        assert_eq!(legacy_snap.engines[0].orders.len(), 2);

        // Flip one byte: load must fail, not deliver silent garbage.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[30] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(load(&path).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }
}
