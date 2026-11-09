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
//! magic "TCS1" | version u32 | journal_seq u64 | raft_applied_index u64
//! max_cmd_id u64 | max_admin_id u64
//! halted:    count u32, instrument u32 ...
//! suspended: count u32, user u64 ...
//! positions: count u32, (user u64, instrument u32, qty i64) ...
//! engines:   count u32, per engine: instrument u32 | engine_seq u64 |
//!            order count u64, orders (56 bytes) | stop count u32, stops
//! dedupe window: count u64, ids u64 ..., then their instruments u32 ...
//! sealed:    count u32, (category u32, category size u32) ...
//! epochs:    count u32, (category u32, epoch u64) ...
//! footer:    FNV-1a-64 over all preceding bytes
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::journal::fnv1a;
use crate::order::Order;
use crate::types::*;

const MAGIC: &[u8; 4] = b"TCS1";
const VERSION: u32 = 8;
const ORDER_ENC: usize = 56;
/// Encoded stop: the order, then trigger price (u64) and kind (u8).
const STOP_ENC: usize = ORDER_ENC + 8 + 1;

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
        TimeInForce::PostOnly => 5,
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
            5 => TimeInForce::PostOnly,
            _ => TimeInForce::Gtc,
        },
        price: u64::from_le_bytes(b[24..32].try_into().unwrap()),
        quantity: u64::from_le_bytes(b[32..40].try_into().unwrap()),
        remaining: u64::from_le_bytes(b[40..48].try_into().unwrap()),
        timestamp: u64::from_le_bytes(b[48..56].try_into().unwrap()),
    }
}

/// One engine's state inside a snapshot.
#[derive(Debug, Clone)]
pub struct EngineState {
    pub instrument: InstrumentId,
    pub engine_seq: Timestamp,
    /// Resting orders in time-priority order.
    pub orders: Vec<Order>,
    /// Untriggered conditional orders, in firing-priority order within each
    /// direction (see `Processor`'s trigger book).
    pub stops: Vec<StopOrder>,
}

/// An untriggered conditional order: held outside the book until a trade
/// prints at or through its trigger.
#[derive(Clone, Copy, Debug)]
pub struct StopOrder {
    pub order: Order,
    pub trigger: Trigger,
}

fn encode_stop(stop: &StopOrder, out: &mut [u8; STOP_ENC]) {
    let mut order = [0u8; ORDER_ENC];
    encode_order(&stop.order, &mut order);
    out[..ORDER_ENC].copy_from_slice(&order);
    out[ORDER_ENC..ORDER_ENC + 8].copy_from_slice(&stop.trigger.price.to_le_bytes());
    out[ORDER_ENC + 8] = match stop.trigger.kind {
        TriggerKind::StopLoss => 0,
        TriggerKind::TakeProfit => 1,
    };
}

fn decode_stop(b: &[u8; STOP_ENC]) -> StopOrder {
    StopOrder {
        order: decode_order(b[..ORDER_ENC].try_into().unwrap()),
        trigger: Trigger {
            price: u64::from_le_bytes(b[ORDER_ENC..ORDER_ENC + 8].try_into().unwrap()),
            kind: if b[ORDER_ENC + 8] == 1 {
                TriggerKind::TakeProfit
            } else {
                TriggerKind::StopLoss
            },
        },
    }
}

/// A decoded shard snapshot.
pub struct Snapshot {
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
    /// The exact command-id dedupe window, oldest first.
    pub recent_command_ids: Vec<u64>,
    /// Instrument of each `recent_command_ids` entry, same order.
    pub recent_command_instruments: Vec<u32>,
    /// Categories sealed by a migration: (category, category size).
    pub sealed: Vec<(u32, u32)>,
    /// Epoch of the last migration step applied per category.
    pub migration_epochs: Vec<(u32, u64)>,
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
    /// Dedupe window, oldest first, and each id's instrument (same length).
    pub recent_command_ids: &'a [u64],
    pub recent_command_instruments: &'a [u32],
    pub sealed: &'a [(u32, u32)],
    pub migration_epochs: &'a [(u32, u64)],
}

/// Write a snapshot **atomically**: temp file, fsync, rename over `path`.
pub fn write(path: &Path, data: SnapshotData<'_>) -> io::Result<()> {
    let buf = encode(data);
    write_bytes(path, &buf)
}

/// The on-disk encoding of a snapshot (also how a category's state travels
/// between Raft groups during a migration; see [`crate::migration`]).
pub fn encode(data: SnapshotData<'_>) -> Vec<u8> {
    assert_eq!(
        data.recent_command_ids.len(),
        data.recent_command_instruments.len(),
        "one instrument per dedupe-window id"
    );
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
    let mut stop_rec = [0u8; STOP_ENC];
    for e in data.engines {
        buf.extend_from_slice(&e.instrument.0.to_le_bytes());
        buf.extend_from_slice(&e.engine_seq.to_le_bytes());
        buf.extend_from_slice(&(e.orders.len() as u64).to_le_bytes());
        for o in &e.orders {
            encode_order(o, &mut rec);
            buf.extend_from_slice(&rec);
        }
        buf.extend_from_slice(&(e.stops.len() as u32).to_le_bytes());
        for stop in &e.stops {
            encode_stop(stop, &mut stop_rec);
            buf.extend_from_slice(&stop_rec);
        }
    }
    buf.extend_from_slice(&(data.recent_command_ids.len() as u64).to_le_bytes());
    for id in data.recent_command_ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    for instrument in data.recent_command_instruments {
        buf.extend_from_slice(&instrument.to_le_bytes());
    }
    buf.extend_from_slice(&(data.sealed.len() as u32).to_le_bytes());
    for (category, size) in data.sealed {
        buf.extend_from_slice(&category.to_le_bytes());
        buf.extend_from_slice(&size.to_le_bytes());
    }
    buf.extend_from_slice(&(data.migration_epochs.len() as u32).to_le_bytes());
    for (category, epoch) in data.migration_epochs {
        buf.extend_from_slice(&category.to_le_bytes());
        buf.extend_from_slice(&epoch.to_le_bytes());
    }
    let h = fnv1a(&buf);
    buf.extend_from_slice(&h.to_le_bytes());
    buf
}

fn write_bytes(path: &Path, buf: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(buf)?;
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
    if decoded.raft_applied_index != expected_index {
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

/// The Raft applied index recorded in snapshot `bytes`, after checking the
/// magic and checksum (without decoding the books).
pub fn raft_applied_index_of(bytes: &[u8]) -> io::Result<u64> {
    let corrupt = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    if bytes.len() < 32 || &bytes[0..4] != MAGIC {
        return Err(corrupt("bad magic/size"));
    }
    let body_len = bytes.len() - 8;
    if fnv1a(&bytes[..body_len]) != u64::from_le_bytes(bytes[body_len..].try_into().unwrap()) {
        return Err(corrupt("checksum mismatch"));
    }
    if bytes[4..8] != VERSION.to_le_bytes() {
        return Err(corrupt("unsupported version"));
    }
    Ok(u64::from_le_bytes(bytes[16..24].try_into().unwrap()))
}

/// Load a snapshot; `Err` on missing file, corruption, or version mismatch.
pub fn load(path: &Path) -> io::Result<Snapshot> {
    let mut buf = Vec::new();
    File::open(path)?.read_to_end(&mut buf)?;
    decode(&buf)
}

/// Bounds-checked little-endian reads over a snapshot body: a malformed
/// buffer (a category state fetched over the network, say) is an error,
/// never a panic.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.buf.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated snapshot"))?;
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// A count of `size`-byte items that must fit in what is left.
    fn count(&mut self, n: u64, size: usize) -> io::Result<usize> {
        let left = (self.buf.len() - self.pos) / size.max(1);
        usize::try_from(n)
            .ok()
            .filter(|n| *n <= left)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "snapshot count exceeds its data"))
    }
}

/// Decode [`encode`]d bytes; `Err` on corruption or version mismatch.
pub fn decode(buf: &[u8]) -> io::Result<Snapshot> {
    let corrupt = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    if buf.len() < 16 || &buf[0..4] != MAGIC {
        return Err(corrupt("bad magic/size"));
    }
    let body_len = buf.len() - 8;
    if fnv1a(&buf[..body_len]) != u64::from_le_bytes(buf[body_len..].try_into().unwrap()) {
        return Err(corrupt("checksum mismatch"));
    }
    let mut c = Cursor { buf: &buf[..body_len], pos: 4 };
    if c.u32()? != VERSION {
        return Err(corrupt("unsupported version"));
    }
    let journal_seq = c.u64()?;
    let raft_applied_index = c.u64()?;
    let max_cmd_id = c.u64()?;
    let max_admin_id = c.u64()?;
    let n = c.u32()?;
    let halted = (0..c.count(n as u64, 4)?).map(|_| c.u32()).collect::<io::Result<_>>()?;
    let n = c.u32()?;
    let suspended = (0..c.count(n as u64, 8)?).map(|_| c.u64()).collect::<io::Result<_>>()?;
    let n = c.u32()?;
    let positions = (0..c.count(n as u64, 20)?)
        .map(|_| Ok((c.u64()?, c.u32()?, c.i64()?)))
        .collect::<io::Result<_>>()?;
    let n = c.u32()?;
    let mut engines = Vec::with_capacity(c.count(n as u64, 24)?);
    for _ in 0..n {
        let instrument = InstrumentId(c.u32()?);
        let engine_seq = c.u64()?;
        let n = c.u64()?;
        let orders = (0..c.count(n, ORDER_ENC)?)
            .map(|_| Ok(decode_order(c.take(ORDER_ENC)?.try_into().unwrap())))
            .collect::<io::Result<_>>()?;
        let n = c.u32()?;
        let stops = (0..c.count(n as u64, STOP_ENC)?)
            .map(|_| Ok(decode_stop(c.take(STOP_ENC)?.try_into().unwrap())))
            .collect::<io::Result<_>>()?;
        engines.push(EngineState { instrument, engine_seq, orders, stops });
    }
    let n = c.u64()?;
    let n = c.count(n, 12)?;
    let recent_command_ids = (0..n).map(|_| c.u64()).collect::<io::Result<_>>()?;
    let recent_command_instruments = (0..n).map(|_| c.u32()).collect::<io::Result<_>>()?;
    let n = c.u32()?;
    let sealed = (0..c.count(n as u64, 8)?)
        .map(|_| Ok((c.u32()?, c.u32()?)))
        .collect::<io::Result<_>>()?;
    let n = c.u32()?;
    let migration_epochs = (0..c.count(n as u64, 12)?)
        .map(|_| Ok((c.u32()?, c.u64()?)))
        .collect::<io::Result<_>>()?;
    if c.pos != body_len {
        return Err(corrupt("trailing bytes"));
    }
    Ok(Snapshot {
        journal_seq,
        raft_applied_index,
        max_cmd_id,
        max_admin_id,
        halted,
        suspended,
        positions,
        engines,
        recent_command_ids,
        recent_command_instruments,
        sealed,
        migration_epochs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_index_is_read_from_the_same_bytes() {
        let dir = std::env::temp_dir().join(format!("tc-snap-index-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.bin");
        let data = |index| SnapshotData {
            journal_seq: 0,
            raft_applied_index: index,
            max_cmd_id: 0,
            max_admin_id: 0,
            halted: &[],
            suspended: &[],
            positions: &[],
            engines: &[],
            recent_command_ids: &[1, 2, 3],
            recent_command_instruments: &[7, 7, 8],
            sealed: &[],
            migration_epochs: &[],
        };
        write(&path, data(41)).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        write(&path, data(42)).unwrap(); // a newer snapshot replaces the file
        assert_eq!(raft_applied_index_of(&bytes).unwrap(), 41);
        let mut torn = bytes.clone();
        torn[20] ^= 1;
        assert!(raft_applied_index_of(&torn).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

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
            stops: Vec::new(),
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
                recent_command_ids: &[3, 4],
                recent_command_instruments: &[7, 9],
                sealed: &[(1, 1000)],
                migration_epochs: &[(1, 12)],
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

        assert_eq!(snap.recent_command_ids, vec![3, 4]);
        assert_eq!(snap.recent_command_instruments, vec![7, 9]);
        assert_eq!(snap.sealed, vec![(1, 1000)]);
        assert_eq!(snap.migration_epochs, vec![(1, 12)]);

        // Truncated or padded data is an error, not a panic.
        let bytes = std::fs::read(&path).unwrap();
        for cut in [10, 40, bytes.len() / 2, bytes.len() - 9] {
            let mut short = bytes[..cut].to_vec();
            short.extend_from_slice(&fnv1a(&short).to_le_bytes());
            assert!(decode(&short).is_err(), "cut at {cut}");
        }
        let mut huge = bytes[..bytes.len() - 8].to_vec();
        huge[40..44].copy_from_slice(&u32::MAX.to_le_bytes()); // halted count
        huge.extend_from_slice(&fnv1a(&huge).to_le_bytes());
        assert!(decode(&huge).is_err());

        // Flip one byte: load must fail, not deliver silent garbage.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[30] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(load(&path).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }
}
