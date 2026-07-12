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
const VERSION: u32 = 1;
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
        order_type: if b[21] == 1 { OrderType::Market } else { OrderType::Limit },
        tif: match b[22] {
            1 => TimeInForce::Ioc,
            2 => TimeInForce::Fok,
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
    /// Journal sequence at capture time; replay records with seq > this.
    pub journal_seq: u64,
    pub engines: Vec<EngineState>,
}

/// Write a snapshot **atomically**: temp file, fsync, rename over `path`.
pub fn write(path: &Path, journal_seq: u64, engines: &[EngineState]) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&journal_seq.to_le_bytes());
    buf.extend_from_slice(&(engines.len() as u32).to_le_bytes());
    let mut rec = [0u8; ORDER_ENC];
    for e in engines {
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
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?; // durable before it can become "the" snapshot
    }
    fs::rename(&tmp, path)?; // atomic on POSIX filesystems
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
    if version != VERSION {
        return Err(corrupt("unsupported version"));
    }
    let journal_seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let engine_count = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;

    let mut pos = 20;
    let mut engines = Vec::with_capacity(engine_count);
    for _ in 0..engine_count {
        if pos + 20 > body_len {
            return Err(corrupt("truncated engine header"));
        }
        let instrument =
            InstrumentId(u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()));
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
        engines.push(EngineState { instrument, engine_seq, orders });
    }
    Ok(Snapshot { journal_seq, engines })
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
                Order::limit(OrderId(1), Side::Buy, 99, 5).on(InstrumentId(7)).by(11),
                Order::limit(OrderId(2), Side::Sell, 101, 3).on(InstrumentId(7)).by(22),
            ],
        }];
        write(&path, 1000, &engines).unwrap();

        let snap = load(&path).unwrap();
        assert_eq!(snap.journal_seq, 1000);
        assert_eq!(snap.engines.len(), 1);
        let e = &snap.engines[0];
        assert_eq!(e.instrument, InstrumentId(7));
        assert_eq!(e.engine_seq, 42);
        assert_eq!(e.orders.len(), 2);
        assert_eq!(e.orders[0].id, OrderId(1));
        assert_eq!(e.orders[0].user, 11);
        assert_eq!(e.orders[1].price, 101);

        // Flip one byte: load must fail, not deliver silent garbage.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[30] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(load(&path).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }
}
