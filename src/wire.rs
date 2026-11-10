//! A fixed-size binary wire protocol with **zero-copy, parse-in-place** decoding.
//!
//! Every inbound command is a fixed [`MSG_LEN`]-byte, little-endian frame. The
//! decoder ([`WireView`]) *borrows* the network receive buffer and reads fields
//! directly from it — no intermediate owned message struct, no heap allocation on
//! the parse path. The same frame format doubles as the **journal record body**,
//! so a replayed journal goes through the identical decode path as live traffic.
//!
//! ## A note on "zero-copy from the NIC"
//!
//! True kernel-bypass ingest (reading frames straight out of NIC RX rings) needs
//! DPDK / AF_XDP / `io_uring` with registered buffers — OS/hardware specific.
//! This module is the software half: frames parsed in place over reusable
//! buffers. A bypass driver plugs in by handing its RX slices to
//! [`WireView::parse`]; the decode path does not change.
//!
//! ## Frame layout (little-endian, 48 bytes)
//!
//! | off | size | field                                                    |
//! |-----|------|----------------------------------------------------------|
//! | 0   | 1    | msg type (1=New, 2=Cancel, 3=Modify, 4=ForceClose)       |
//! | 1   | 1    | side (0=Buy, 1=Sell) — close side for ForceClose         |
//! | 2   | 1    | order type (0=Limit, 1=Market); New only: bit 7 = conditional, bit 6 = take-profit |
//! | 3   | 1    | time-in-force (0=GTC, 1=IOC, 2=FOK)                      |
//! | 4   | 4    | instrument id (u32)                                      |
//! | 8   | 8    | order id (u64) — close-order id for ForceClose           |
//! | 16  | 8    | price (u64) — new price for Modify; **cmd_id for Cancel** |
//! | 24  | 8    | quantity (u64) — new qty for Modify, close qty for FC    |
//! | 32  | 8    | user id (u64) — **cmd_id for Modify**                    |
//! | 40  | 8    | trigger price (u64) for conditional orders; 0 = none      |
//!
//! Every command carries a unique increasing id: New = order id; Cancel/Modify
//! = `cmd_id` from the same Leaf-style series. Combined with the journal seq
//! this makes cancels and modifies first-class sequenced commands — crash
//! replay reproduces the full command stream, not just the new orders.

use crate::exchange::{Command, ExecReport};
use crate::order::Order;
use crate::types::*;
use std::fmt;

/// Fixed wire frame length in bytes.
pub const MSG_LEN: usize = 48;
pub const RAFT_BATCH_HEADER_LEN: usize = 12;
/// High-throughput Raft application batch.  At 48 bytes per command the
/// maximum entry is about 480 KiB, small enough for replication while
/// amortizing quorum WAL and application durability barriers over 10x more
/// commands than the original 1,000-command limit.
pub const RAFT_BATCH_MAX_COMMANDS: usize = 10_000;
const RAFT_BATCH_MAGIC: [u8; 4] = *b"RB01";

/// Encode multiple command frames into one Raft application entry. Consensus
/// persists and replicates this byte string atomically, so a batch consumes one
/// log index and one Ready fsync rather than one of each per command.
pub fn encode_raft_batch(frames: &[[u8; MSG_LEN]]) -> Option<Vec<u8>> {
    if frames.is_empty() || frames.len() > RAFT_BATCH_MAX_COMMANDS {
        return None;
    }
    let mut out = Vec::with_capacity(RAFT_BATCH_HEADER_LEN + frames.len() * MSG_LEN);
    out.extend_from_slice(&RAFT_BATCH_MAGIC);
    out.extend_from_slice(&2u32.to_le_bytes());
    out.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    for frame in frames {
        WireView::parse(frame)?;
        out.extend_from_slice(frame);
    }
    Some(out)
}

const RAFT_CONFIG_MAGIC: [u8; 4] = *b"TCCF";

/// A Raft entry announcing the proposing leader's deterministic-config
/// fingerprint (and a readable description of what was hashed). It carries no
/// commands: every replica that applies it compares the fingerprint with its
/// own and refuses to go on applying on a mismatch, instead of silently
/// computing different results.
pub fn encode_raft_config_entry(fingerprint: u64, description: &str) -> Vec<u8> {
    let description = &description.as_bytes()[..description.len().min(4096)];
    let mut out = Vec::with_capacity(20 + description.len());
    out.extend_from_slice(&RAFT_CONFIG_MAGIC);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&fingerprint.to_le_bytes());
    out.extend_from_slice(&(description.len() as u32).to_le_bytes());
    out.extend_from_slice(description);
    out
}

/// `(fingerprint, description)` of a config entry; `None` for any other entry.
pub fn decode_raft_config_entry(payload: &[u8]) -> Option<(u64, String)> {
    if payload.len() < 20 || payload[..4] != RAFT_CONFIG_MAGIC {
        return None;
    }
    let fingerprint = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let len = u32::from_le_bytes(payload[16..20].try_into().ok()?) as usize;
    let description = payload.get(20..20 + len)?;
    Some((
        fingerprint,
        String::from_utf8_lossy(description).into_owned(),
    ))
}

const RAFT_MIGRATION_MAGIC: [u8; 4] = *b"TCMG";

/// A Raft entry carrying one category-migration step
/// ([`crate::exchange::CategoryOp`]). An install carries the category's
/// exported state in the snapshot encoding, so it can be large; it travels
/// as its own entry rather than as command frames. Layout: magic, version
/// (2), step kind, category, category size, epoch, then an install's state.
pub fn encode_raft_migration_entry(op: &crate::exchange::CategoryOp) -> Vec<u8> {
    use crate::exchange::CategoryOp;
    let (kind, category, size, epoch) = match op {
        CategoryOp::Seal {
            category,
            category_size,
            epoch,
        } => (1u8, *category, *category_size, *epoch),
        CategoryOp::Install {
            category,
            category_size,
            epoch,
            ..
        } => (2u8, *category, *category_size, *epoch),
        CategoryOp::Drop {
            category,
            category_size,
            epoch,
        } => (3u8, *category, *category_size, *epoch),
        CategoryOp::Unseal {
            category,
            category_size,
            epoch,
        } => (4u8, *category, *category_size, *epoch),
    };
    let mut out = Vec::with_capacity(25);
    out.extend_from_slice(&RAFT_MIGRATION_MAGIC);
    out.extend_from_slice(&2u32.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&category.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    if let CategoryOp::Install {
        engines,
        positions,
        halted,
        command_ids,
        ..
    } = op
    {
        out.extend_from_slice(&crate::migration::encode_category_state(
            0,
            engines,
            positions,
            halted,
            command_ids,
        ));
    }
    out
}

fn decode_raft_migration_entry(payload: &[u8]) -> Option<crate::exchange::CategoryOp> {
    use crate::exchange::CategoryOp;
    if payload.len() < 17 || payload[..4] != RAFT_MIGRATION_MAGIC {
        return None;
    }
    if u32::from_le_bytes(payload[4..8].try_into().ok()?) != 2 {
        return None;
    }
    let category = u32::from_le_bytes(payload[9..13].try_into().ok()?);
    let category_size = u32::from_le_bytes(payload[13..17].try_into().ok()?);
    let epoch = u64::from_le_bytes(payload.get(17..25)?.try_into().ok()?);
    let body = &payload[25..];
    match payload[8] {
        1 => Some(CategoryOp::Seal {
            category,
            category_size,
            epoch,
        }),
        3 => Some(CategoryOp::Drop {
            category,
            category_size,
            epoch,
        }),
        4 => Some(CategoryOp::Unseal {
            category,
            category_size,
            epoch,
        }),
        2 => {
            let state = crate::snapshot::decode(body).ok()?;
            let command_ids = state
                .recent_command_ids
                .iter()
                .copied()
                .zip(state.recent_command_instruments.iter().copied())
                .collect();
            Some(CategoryOp::Install {
                category,
                category_size,
                epoch,
                engines: state.engines,
                positions: state.positions,
                halted: state.halted,
                command_ids,
            })
        }
        _ => None,
    }
}

const RAFT_INCARNATION_MAGIC: [u8; 4] = *b"TCIN";

/// A Raft entry assigning the group's incarnation ([`Command::Incarnation`]):
/// magic, version (1), id (non-zero).
pub fn encode_raft_incarnation_entry(id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&RAFT_INCARNATION_MAGIC);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&id.to_le_bytes());
    out
}

/// Decode a command batch entry. A category-migration entry decodes to one
/// [`Command::Migrate`], an incarnation entry to one [`Command::Incarnation`].
pub fn decode_raft_entry(payload: &[u8]) -> Option<Vec<Command>> {
    if payload.starts_with(&RAFT_INCARNATION_MAGIC) {
        if payload.len() != 16 || payload[4..8] != 1u32.to_le_bytes() {
            return None;
        }
        let id = u64::from_le_bytes(payload[8..16].try_into().ok()?);
        return (id != 0).then(|| vec![Command::Incarnation(id)]);
    }
    if payload.starts_with(&RAFT_MIGRATION_MAGIC) {
        return decode_raft_migration_entry(payload).map(|op| vec![Command::Migrate(Box::new(op))]);
    }
    if payload.len() < RAFT_BATCH_HEADER_LEN
        || payload[..4] != RAFT_BATCH_MAGIC
        || u32::from_le_bytes(payload[4..8].try_into().ok()?) != 2
    {
        return None;
    }
    let count = u32::from_le_bytes(payload[8..12].try_into().ok()?) as usize;
    if count == 0
        || count > RAFT_BATCH_MAX_COMMANDS
        || payload.len() != RAFT_BATCH_HEADER_LEN + count * MSG_LEN
    {
        return None;
    }
    payload[RAFT_BATCH_HEADER_LEN..]
        .chunks_exact(MSG_LEN)
        .map(|frame| WireView::parse(frame)?.to_command())
        .collect()
}

/// Fixed execution-report frame length in bytes (v2: 56 — adds fee fields).
pub const REPORT_LEN: usize = 56;
/// An execution event: magic, version (3), raft group, raft index, ordinal,
/// the target order id (a trade is published once for the taker and once
/// for the maker, each keyed and partitioned by it), the report, and the
/// target order's owner (`u64` at 96..104, so a result can be pushed to its
/// user without any lookup; see [`execution_event_target_user`]).
pub const EXECUTION_EVENT_LEN: usize = 104;
const EXECUTION_EVENT_USER_AT: usize = 96;
const EXECUTION_EVENT_MAGIC: [u8; 4] = *b"EX01";
const MD_HELLO_MAGIC: [u8; 4] = *b"TCMH";

/// A market-data fanout frame announcing which Raft group, in which
/// incarnation, the feed carries (sent on connect, when the incarnation is
/// assigned, and as the idle heartbeat). Same length as an execution event,
/// so a feed stays frame-aligned; readers that do not know it skip it.
pub fn encode_md_hello(group: u32, incarnation: u64) -> [u8; EXECUTION_EVENT_LEN] {
    let mut frame = [0u8; EXECUTION_EVENT_LEN];
    frame[..4].copy_from_slice(&MD_HELLO_MAGIC);
    frame[4..8].copy_from_slice(&1u32.to_le_bytes());
    frame[8..12].copy_from_slice(&group.to_le_bytes());
    frame[16..24].copy_from_slice(&incarnation.to_le_bytes());
    frame
}

/// `(group, incarnation)` of a fanout hello frame.
pub fn decode_md_hello(frame: &[u8]) -> Option<(u32, u64)> {
    if frame.len() < EXECUTION_EVENT_LEN
        || frame[..4] != MD_HELLO_MAGIC
        || frame[4..8] != 1u32.to_le_bytes()
    {
        return None;
    }
    Some((
        u32::from_le_bytes(frame[8..12].try_into().ok()?),
        u64::from_le_bytes(frame[16..24].try_into().ok()?),
    ))
}

const MT_NEW: u8 = 1;
const MT_CANCEL: u8 = 2;
const MT_MODIFY: u8 = 3;
const MT_FORCE_CLOSE: u8 = 4;
const MT_HALT: u8 = 5;
const MT_RESUME: u8 = 6;
const MT_HALT_USER: u8 = 7;
const MT_RESUME_USER: u8 = 8;

// Report type codes.
pub const RT_ACCEPTED: u8 = 1;
pub const RT_TRADE: u8 = 2;
pub const RT_FILLED: u8 = 3;
pub const RT_PARTIAL: u8 = 4;
pub const RT_RESTING: u8 = 5;
pub const RT_CANCELLED: u8 = 6;
pub const RT_REJECTED: u8 = 7;
pub const RT_MODIFIED: u8 = 8;
pub const RT_NOTFOUND: u8 = 9;
/// One depth ladder level (market-data): aux = level index, price/qty set.
pub const RT_DEPTH_LEVEL: u8 = 10;
/// Depth snapshot terminator: aux = bid_levels | ask_levels << 8.
pub const RT_DEPTH_END: u8 = 11;
/// A refused command that changed no order ([`ExecReport::CommandRejected`]):
/// order_id slot = the command's id, aux = reject reason code. (12..=15 are
/// the halt / resume / user-halt / user-resume acks.)
pub const RT_CMD_REJECTED: u8 = 16;
/// A conditional order accepted into the trigger book (not on the book yet).
pub const RT_STOP_ACCEPTED: u8 = 17;
/// A conditional order fired; its ordinary-order reports follow.
pub const RT_TRIGGERED: u8 = 18;
/// A maker order filled completely by the preceding trade (informational).
pub const RT_MAKER_FILLED: u8 = 19;

/// A borrowed view over one wire frame. Reads fields in place; copies nothing.
#[derive(Clone, Copy)]
pub struct WireView<'a> {
    buf: &'a [u8; MSG_LEN],
}

impl<'a> WireView<'a> {
    /// Interpret the first [`MSG_LEN`] bytes of `buf` as a frame, or `None` if it
    /// is too short. Borrows `buf`; performs no copy or allocation.
    #[inline]
    pub fn parse(buf: &'a [u8]) -> Option<WireView<'a>> {
        let arr: &[u8; MSG_LEN] = buf.get(..MSG_LEN)?.try_into().ok()?;
        Some(WireView { buf: arr })
    }

    #[inline]
    fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
    }
    #[inline]
    fn u64_at(&self, off: usize) -> u64 {
        u64::from_le_bytes(self.buf[off..off + 8].try_into().unwrap())
    }

    #[inline]
    pub fn msg_type(&self) -> u8 {
        self.buf[0]
    }
    #[inline]
    pub fn instrument(&self) -> InstrumentId {
        InstrumentId(self.u32_at(4))
    }
    #[inline]
    pub fn order_id(&self) -> OrderId {
        OrderId(self.u64_at(8))
    }
    #[inline]
    pub fn price(&self) -> Price {
        self.u64_at(16)
    }
    #[inline]
    pub fn qty(&self) -> Qty {
        self.u64_at(24)
    }
    #[inline]
    pub fn user(&self) -> u64 {
        self.u64_at(32)
    }

    fn side(&self) -> Side {
        match self.buf[1] {
            0 => Side::Buy,
            _ => Side::Sell,
        }
    }
    fn order_type(&self) -> OrderType {
        match self.buf[2] & ORDER_TYPE_MASK {
            1 => OrderType::Market,
            _ => OrderType::Limit,
        }
    }
    #[inline]
    pub fn trigger_price(&self) -> Price {
        self.u64_at(40)
    }
    /// The trigger of a conditional `New` (bit 7 of the order-type byte).
    fn trigger(&self) -> Option<Trigger> {
        (self.buf[2] & FLAG_CONDITIONAL != 0).then(|| Trigger {
            price: self.trigger_price(),
            kind: if self.buf[2] & FLAG_TAKE_PROFIT != 0 {
                TriggerKind::TakeProfit
            } else {
                TriggerKind::StopLoss
            },
        })
    }
    fn tif(&self) -> TimeInForce {
        match self.buf[3] {
            1 => TimeInForce::Ioc,
            2 => TimeInForce::Fok,
            3 => TimeInForce::IocBudget,
            4 => TimeInForce::FokBudget,
            5 => TimeInForce::PostOnly,
            _ => TimeInForce::Gtc,
        }
    }

    /// Materialise the frame into an engine [`Command`].
    pub fn to_command(&self) -> Option<Command> {
        match self.msg_type() {
            MT_NEW => {
                let mut order = match self.order_type() {
                    OrderType::Market => Order::market(self.order_id(), self.side(), self.qty()),
                    OrderType::Limit => {
                        Order::limit(self.order_id(), self.side(), self.price(), self.qty())
                    }
                };
                order.instrument = self.instrument();
                order.user = self.user();
                order.tif = self.tif();
                Some(match self.trigger() {
                    Some(trigger) => Command::NewConditional(order, trigger),
                    None => Command::New(order),
                })
            }
            MT_CANCEL => Some(Command::Cancel {
                instrument: self.instrument(),
                order_id: self.order_id(),
                cmd_id: self.price(), // rides in the price slot
            }),
            MT_MODIFY => Some(Command::Modify {
                instrument: self.instrument(),
                order_id: self.order_id(),
                new_price: self.price(),
                new_qty: self.qty(),
                cmd_id: self.user(), // rides in the user slot
            }),
            MT_FORCE_CLOSE => Some(Command::ForceClose {
                instrument: self.instrument(),
                user: self.user(),
                close_order_id: self.order_id(),
                close_side: self.side(),
                close_qty: self.qty(),
            }),
            MT_HALT => Some(Command::Halt {
                instrument: self.instrument(),
                cmd_id: self.order_id().0,
            }),
            MT_RESUME => Some(Command::Resume {
                instrument: self.instrument(),
                cmd_id: self.order_id().0,
            }),
            MT_HALT_USER => Some(Command::HaltUser {
                instrument: self.instrument(),
                user: self.user(),
                cmd_id: self.order_id().0,
            }),
            MT_RESUME_USER => Some(Command::ResumeUser {
                instrument: self.instrument(),
                user: self.user(),
                cmd_id: self.order_id().0,
            }),
            _ => None,
        }
    }
}

/// Encode a Halt/Resume admin frame (cmd_id rides the order-id slot).
pub fn encode_admin(halt: bool, instrument: InstrumentId, cmd_id: u64, out: &mut [u8; MSG_LEN]) {
    out.fill(0);
    out[0] = if halt { MT_HALT } else { MT_RESUME };
    out[4..8].copy_from_slice(&instrument.0.to_le_bytes());
    out[8..16].copy_from_slice(&cmd_id.to_le_bytes());
}

/// Order-type byte: low bits carry the type, high bits the conditional flags.
const ORDER_TYPE_MASK: u8 = 0x0F;
const FLAG_CONDITIONAL: u8 = 0x80;
const FLAG_TAKE_PROFIT: u8 = 0x40;

/// Encode a conditional (stop-loss / take-profit) `New` into a frame: the
/// order as [`encode_new`] writes it, flagged, with the trigger price in the
/// frame's tail.
pub fn encode_new_conditional(order: &Order, trigger: &Trigger, out: &mut [u8; MSG_LEN]) {
    encode_new(order, out);
    out[2] |= FLAG_CONDITIONAL;
    if trigger.kind == TriggerKind::TakeProfit {
        out[2] |= FLAG_TAKE_PROFIT;
    }
    out[40..48].copy_from_slice(&trigger.price.to_le_bytes());
}

/// Encode a `New` order into a frame.
pub fn encode_new(order: &Order, out: &mut [u8; MSG_LEN]) {
    out.fill(0);
    out[0] = MT_NEW;
    out[1] = match order.side {
        Side::Buy => 0,
        Side::Sell => 1,
    };
    out[2] = match order.order_type {
        OrderType::Limit => 0,
        OrderType::Market => 1,
    };
    out[3] = match order.tif {
        TimeInForce::Gtc => 0,
        TimeInForce::Ioc => 1,
        TimeInForce::Fok => 2,
        TimeInForce::IocBudget => 3,
        TimeInForce::FokBudget => 4,
        TimeInForce::PostOnly => 5,
    };
    out[4..8].copy_from_slice(&order.instrument.0.to_le_bytes());
    out[8..16].copy_from_slice(&order.id.0.to_le_bytes());
    out[16..24].copy_from_slice(&order.price.to_le_bytes());
    out[24..32].copy_from_slice(&order.quantity.to_le_bytes());
    out[32..40].copy_from_slice(&order.user.to_le_bytes());
}

/// Encode a `Cancel` frame. `cmd_id` (unique, increasing — same id series as
/// new orders) rides in the otherwise-unused price slot.
pub fn encode_cancel(
    instrument: InstrumentId,
    order_id: OrderId,
    cmd_id: u64,
    out: &mut [u8; MSG_LEN],
) {
    out.fill(0);
    out[0] = MT_CANCEL;
    out[4..8].copy_from_slice(&instrument.0.to_le_bytes());
    out[8..16].copy_from_slice(&order_id.0.to_le_bytes());
    out[16..24].copy_from_slice(&cmd_id.to_le_bytes());
}

/// Encode a `Modify` frame.
pub fn encode_modify(
    instrument: InstrumentId,
    order_id: OrderId,
    new_price: Price,
    new_qty: Qty,
    cmd_id: u64,
    out: &mut [u8; MSG_LEN],
) {
    out.fill(0);
    out[0] = MT_MODIFY;
    out[4..8].copy_from_slice(&instrument.0.to_le_bytes());
    out[8..16].copy_from_slice(&order_id.0.to_le_bytes());
    out[16..24].copy_from_slice(&new_price.to_le_bytes());
    out[24..32].copy_from_slice(&new_qty.to_le_bytes());
    out[32..40].copy_from_slice(&cmd_id.to_le_bytes());
}

/// Encode a `ForceClose` frame: cancel all of `user`'s resting orders on
/// `instrument`, then (if `close_qty > 0`) submit a protected market order of
/// `close_qty` on `close_side` with id `close_order_id`.
pub fn encode_force_close(
    instrument: InstrumentId,
    user: u64,
    close_order_id: OrderId,
    close_side: Side,
    close_qty: Qty,
    out: &mut [u8; MSG_LEN],
) {
    out.fill(0);
    out[0] = MT_FORCE_CLOSE;
    out[1] = match close_side {
        Side::Buy => 0,
        Side::Sell => 1,
    };
    out[4..8].copy_from_slice(&instrument.0.to_le_bytes());
    out[8..16].copy_from_slice(&close_order_id.0.to_le_bytes());
    out[24..32].copy_from_slice(&close_qty.to_le_bytes());
    out[32..40].copy_from_slice(&user.to_le_bytes());
}

/// Encode any [`Command`] into a frame (used by the journal so that replay goes
/// through the same decode path as live traffic).
pub fn encode_command(cmd: &Command, out: &mut [u8; MSG_LEN]) {
    match cmd {
        Command::New(order) => encode_new(order, out),
        Command::NewConditional(order, trigger) => encode_new_conditional(order, trigger, out),
        Command::Cancel {
            instrument,
            order_id,
            cmd_id,
        } => encode_cancel(*instrument, *order_id, *cmd_id, out),
        Command::Modify {
            instrument,
            order_id,
            new_price,
            new_qty,
            cmd_id,
        } => encode_modify(*instrument, *order_id, *new_price, *new_qty, *cmd_id, out),
        Command::ForceClose {
            instrument,
            user,
            close_order_id,
            close_side,
            close_qty,
        } => encode_force_close(
            *instrument,
            *user,
            *close_order_id,
            *close_side,
            *close_qty,
            out,
        ),
        Command::Halt { instrument, cmd_id } => encode_admin(true, *instrument, *cmd_id, out),
        Command::Resume { instrument, cmd_id } => encode_admin(false, *instrument, *cmd_id, out),
        Command::HaltUser {
            instrument,
            user,
            cmd_id,
        } => encode_user_admin(true, *instrument, *user, *cmd_id, out),
        Command::ResumeUser {
            instrument,
            user,
            cmd_id,
        } => encode_user_admin(false, *instrument, *user, *cmd_id, out),
        // Batches are flattened at the shard: encode_command is called per inner
        // command for journal/replication; a Batch itself never reaches here.
        Command::Batch(_) => out.fill(0),
        // Category migration travels as its own Raft entry (it carries whole
        // books), never as a command frame; standalone journals do not
        // support it.
        Command::Migrate(_) | Command::Incarnation(_) => out.fill(0),
    }
}

/// Encode a user-suspend admin frame.
pub fn encode_user_admin(
    halt: bool,
    instrument: InstrumentId,
    user: u64,
    cmd_id: u64,
    out: &mut [u8; MSG_LEN],
) {
    out.fill(0);
    out[0] = if halt { MT_HALT_USER } else { MT_RESUME_USER };
    out[4..8].copy_from_slice(&instrument.0.to_le_bytes());
    out[8..16].copy_from_slice(&cmd_id.to_le_bytes());
    out[32..40].copy_from_slice(&user.to_le_bytes());
}

/// Encode an execution report into a fixed 40-byte frame for the return path.
///
/// Layout (little-endian): `[0]=type, [1]=side, [4..8]=instrument,
/// [8..16]=order_id, [16..24]=aux_id (maker for trades), [24..32]=price,
/// [32..40]=qty` (qty carries trade size / filled / remaining per type).
pub fn encode_report(r: &ExecReport, out: &mut [u8; REPORT_LEN]) {
    out.fill(0);
    let mut put =
        |ty: u8, inst: InstrumentId, oid: OrderId, aux: u64, price: u64, qty: u64, side: u8| {
            out[0] = ty;
            out[1] = side;
            out[4..8].copy_from_slice(&inst.0.to_le_bytes());
            out[8..16].copy_from_slice(&oid.0.to_le_bytes());
            out[16..24].copy_from_slice(&aux.to_le_bytes());
            out[24..32].copy_from_slice(&price.to_le_bytes());
            out[32..40].copy_from_slice(&qty.to_le_bytes());
        };
    match *r {
        ExecReport::Accepted {
            instrument,
            order_id,
            ..
        } => put(RT_ACCEPTED, instrument, order_id, 0, 0, 0, 0),
        ExecReport::Trade {
            instrument,
            taker,
            maker,
            aggressor,
            price,
            qty,
            maker_fee,
            taker_fee,
            ..
        } => {
            put(
                RT_TRADE,
                instrument,
                taker,
                maker.0,
                price,
                qty,
                match aggressor {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
            );
            out[40..48].copy_from_slice(&maker_fee.to_le_bytes());
            out[48..56].copy_from_slice(&taker_fee.to_le_bytes());
        }
        ExecReport::Filled {
            instrument,
            order_id,
            ..
        } => put(RT_FILLED, instrument, order_id, 0, 0, 0, 0),
        ExecReport::PartiallyFilled {
            instrument,
            order_id,
            filled,
            ..
        } => put(RT_PARTIAL, instrument, order_id, 0, 0, filled, 0),
        ExecReport::Resting {
            instrument,
            order_id,
            ..
        } => put(RT_RESTING, instrument, order_id, 0, 0, 0, 0),
        ExecReport::Cancelled {
            instrument,
            order_id,
            ..
        } => put(RT_CANCELLED, instrument, order_id, 0, 0, 0, 0),
        ExecReport::Rejected {
            instrument,
            order_id,
            reason,
            ..
        } => put(
            RT_REJECTED,
            instrument,
            order_id,
            reject_reason_code(reason),
            0,
            0,
            0,
        ),
        ExecReport::StopAccepted {
            instrument,
            order_id,
            ..
        } => put(RT_STOP_ACCEPTED, instrument, order_id, 0, 0, 0, 0),
        ExecReport::Triggered {
            instrument,
            order_id,
            ..
        } => put(RT_TRIGGERED, instrument, order_id, 0, 0, 0, 0),
        ExecReport::MakerFilled {
            instrument,
            order_id,
            ..
        } => put(RT_MAKER_FILLED, instrument, order_id, 0, 0, 0, 0),
        ExecReport::CommandRejected {
            instrument,
            command_id,
            reason,
        } => put(
            RT_CMD_REJECTED,
            instrument,
            OrderId(command_id),
            reject_reason_code(reason),
            0,
            0,
            0,
        ),
        ExecReport::Modified {
            instrument,
            order_id,
            price,
            remaining,
            ..
        } => put(RT_MODIFIED, instrument, order_id, 0, price, remaining, 0),
        ExecReport::NotFound {
            instrument,
            order_id,
        } => put(RT_NOTFOUND, instrument, order_id, 0, 0, 0, 0),
        ExecReport::DepthLevel {
            instrument,
            side,
            level,
            price,
            qty,
        } => put(
            RT_DEPTH_LEVEL,
            instrument,
            OrderId(0),
            level as u64,
            price,
            qty,
            match side {
                Side::Buy => 0,
                Side::Sell => 1,
            },
        ),
        ExecReport::DepthEnd {
            instrument,
            bid_levels,
            ask_levels,
        } => put(
            RT_DEPTH_END,
            instrument,
            OrderId(0),
            bid_levels as u64 | (ask_levels as u64) << 8,
            0,
            0,
            0,
        ),
        ExecReport::Halted { instrument } => put(12, instrument, OrderId(0), 0, 0, 0, 0),
        ExecReport::Resumed { instrument } => put(13, instrument, OrderId(0), 0, 0, 0, 0),
        ExecReport::UserHalted { instrument, user } => {
            put(14, instrument, OrderId(0), user, 0, 0, 0)
        }
        ExecReport::UserResumed { instrument, user } => {
            put(15, instrument, OrderId(0), user, 0, 0, 0)
        }
    }
}

/// A decoded execution report (owned; the return path is not latency-critical).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedReport {
    pub type_code: u8,
    pub maker_fee: u64,
    pub taker_fee: u64,
    pub instrument: InstrumentId,
    pub order_id: OrderId,
    pub aux_id: u64,
    pub price: Price,
    pub qty: Qty,
    pub side: Side,
}

/// Reject reasons as carried on the wire: a `Rejected` report puts the index
/// of its reason in the frame's `aux` slot (unused by that type), so clients
/// see *why* — the Binance/OKX error-code role. Append only: the index is the
/// wire code. 0 = unknown (a reason missing from this table).
pub const REJECT_REASONS: &[&str] = &[
    "unknown",
    "duplicate",
    "halted",
    "user-suspended",
    "max-qty",
    "max-notional",
    "price-band",
    "max-user-orders",
    "unfillable",
    "post-only",
    "tick-size",
    "lot-size",
    "min-qty",
    "price-range",
    "min-notional",
    "invalid-stop",
    "stop-not-amendable",
    "category-moved",
    "category-not-empty",
    "stale-migration",
    "invalid-order-id",
    "invalid-tif",
    "invalid-install",
];

/// Wire code of a reject reason (0 if it is not in [`REJECT_REASONS`]).
pub fn reject_reason_code(reason: &str) -> u64 {
    REJECT_REASONS
        .iter()
        .position(|known| *known == reason)
        .unwrap_or(0) as u64
}

/// Reject reason for a wire code (`"unknown"` if out of range).
pub fn reject_reason_name(code: u64) -> &'static str {
    REJECT_REASONS
        .get(code as usize)
        .copied()
        .unwrap_or(REJECT_REASONS[0])
}

/// A durable execution event envelope. The report remains the old fixed frame;
/// the envelope gives consumers a deterministic id that survives Kafka
/// republish, leader failover and consumer offset rewind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionEvent {
    pub raft_group: u32,
    pub raft_index: u64,
    pub ordinal: u32,
    pub target_order_id: u64,
    pub report: DecodedReport,
}

pub fn encode_execution_event(
    raft_group: u32,
    raft_index: u64,
    ordinal: u32,
    report: &ExecReport,
    out: &mut [u8; EXECUTION_EVENT_LEN],
) {
    let target_order_id = match report {
        ExecReport::Trade { taker, .. } => taker.0,
        _ => {
            let mut frame = [0u8; REPORT_LEN];
            encode_report(report, &mut frame);
            decode_report(&frame).map_or(0, |decoded| decoded.order_id.0)
        }
    };
    encode_execution_event_for_target(
        raft_group,
        raft_index,
        ordinal,
        target_order_id,
        report,
        out,
    );
}

pub fn encode_execution_event_for_target(
    raft_group: u32,
    raft_index: u64,
    ordinal: u32,
    target_order_id: u64,
    report: &ExecReport,
    out: &mut [u8; EXECUTION_EVENT_LEN],
) {
    let mut frame = [0u8; REPORT_LEN];
    encode_report(report, &mut frame);
    let [user, maker_user] = report.users();
    let target_user = match report {
        ExecReport::Trade { maker, .. } if maker.0 == target_order_id => maker_user,
        _ => user,
    };
    encode_execution_event_frame(
        raft_group,
        raft_index,
        ordinal,
        target_order_id,
        target_user,
        &frame,
        out,
    );
}

/// Build a v3 envelope around an already-encoded report frame.
pub fn encode_execution_event_frame(
    raft_group: u32,
    raft_index: u64,
    ordinal: u32,
    target_order_id: u64,
    target_user: u64,
    frame: &[u8; REPORT_LEN],
    out: &mut [u8; EXECUTION_EVENT_LEN],
) {
    out.fill(0);
    out[..4].copy_from_slice(&EXECUTION_EVENT_MAGIC);
    out[4..8].copy_from_slice(&3u32.to_le_bytes());
    out[8..12].copy_from_slice(&raft_group.to_le_bytes());
    out[16..24].copy_from_slice(&raft_index.to_le_bytes());
    out[24..28].copy_from_slice(&ordinal.to_le_bytes());
    out[32..40].copy_from_slice(&target_order_id.to_le_bytes());
    out[40..40 + REPORT_LEN].copy_from_slice(frame);
    out[EXECUTION_EVENT_USER_AT..EXECUTION_EVENT_LEN].copy_from_slice(&target_user.to_le_bytes());
}

fn is_execution_event(buf: &[u8]) -> bool {
    buf.len() >= EXECUTION_EVENT_LEN
        && buf[..4] == EXECUTION_EVENT_MAGIC
        && buf[4..8] == 3u32.to_le_bytes()
}

/// Owner of an execution event's target order (0 for a malformed buffer, or
/// an event with no owner, such as a command-level reject).
pub fn execution_event_target_user(buf: &[u8]) -> u64 {
    if !is_execution_event(buf) {
        return 0;
    }
    u64::from_le_bytes(
        buf[EXECUTION_EVENT_USER_AT..EXECUTION_EVENT_LEN]
            .try_into()
            .unwrap(),
    )
}

pub fn decode_execution_event(buf: &[u8]) -> Option<ExecutionEvent> {
    if !is_execution_event(buf) {
        return None;
    }
    Some(ExecutionEvent {
        raft_group: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        raft_index: u64::from_le_bytes(buf[16..24].try_into().ok()?),
        ordinal: u32::from_le_bytes(buf[24..28].try_into().ok()?),
        target_order_id: u64::from_le_bytes(buf[32..40].try_into().ok()?),
        report: decode_report(&buf[40..40 + REPORT_LEN])?,
    })
}

/// Decode a 40-byte report frame, or `None` if the buffer is too short.
pub fn decode_report(buf: &[u8]) -> Option<DecodedReport> {
    let b: &[u8; REPORT_LEN] = buf.get(..REPORT_LEN)?.try_into().ok()?;
    Some(DecodedReport {
        type_code: b[0],
        maker_fee: u64::from_le_bytes(b[40..48].try_into().unwrap()),
        taker_fee: u64::from_le_bytes(b[48..56].try_into().unwrap()),
        side: if b[1] == 0 { Side::Buy } else { Side::Sell },
        instrument: InstrumentId(u32::from_le_bytes(b[4..8].try_into().unwrap())),
        order_id: OrderId(u64::from_le_bytes(b[8..16].try_into().unwrap())),
        aux_id: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        price: u64::from_le_bytes(b[24..32].try_into().unwrap()),
        qty: u64::from_le_bytes(b[32..40].try_into().unwrap()),
    })
}

impl fmt::Display for DecodedReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_code {
            RT_ACCEPTED => write!(f, "ACCEPTED   {} {}", self.instrument, self.order_id),
            RT_TRADE => write!(
                f,
                "TRADE      {} taker {} x maker #{} : {} @ {} ({})",
                self.instrument, self.order_id, self.aux_id, self.qty, self.price, self.side
            ),
            RT_FILLED => write!(f, "FILLED     {} {}", self.instrument, self.order_id),
            RT_PARTIAL => write!(
                f,
                "PARTIAL    {} {} filled {}",
                self.instrument, self.order_id, self.qty
            ),
            RT_RESTING => write!(f, "RESTING    {} {}", self.instrument, self.order_id),
            RT_CANCELLED => write!(f, "CANCELLED  {} {}", self.instrument, self.order_id),
            RT_REJECTED => write!(
                f,
                "REJECTED   {} {} ({})",
                self.instrument,
                self.order_id,
                reject_reason_name(self.aux_id)
            ),
            RT_STOP_ACCEPTED => write!(f, "STOP       {} {}", self.instrument, self.order_id),
            RT_TRIGGERED => write!(f, "TRIGGERED  {} {}", self.instrument, self.order_id),
            RT_MAKER_FILLED => write!(f, "MKRFILLED  {} {}", self.instrument, self.order_id),
            RT_CMD_REJECTED => write!(
                f,
                "CMDREJECT  {} command {} ({})",
                self.instrument,
                self.order_id.0,
                reject_reason_name(self.aux_id)
            ),
            RT_MODIFIED => write!(
                f,
                "MODIFIED   {} {} remaining {} @ {}",
                self.instrument, self.order_id, self.qty, self.price
            ),
            RT_NOTFOUND => write!(f, "NOTFOUND   {} {}", self.instrument, self.order_id),
            other => write!(f, "UNKNOWN({other})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_entries_round_trip_including_the_installed_state() {
        use crate::exchange::CategoryOp;
        let seal = encode_raft_migration_entry(&CategoryOp::Seal {
            category: 7,
            category_size: 1000,
            epoch: 42,
        });
        match decode_raft_entry(&seal).unwrap().as_slice() {
            [Command::Migrate(op)] => {
                assert!(matches!(
                    **op,
                    CategoryOp::Seal {
                        category: 7,
                        category_size: 1000,
                        epoch: 42
                    }
                ))
            }
            other => panic!("{other:?}"),
        }
        let unseal = encode_raft_migration_entry(&CategoryOp::Unseal {
            category: 7,
            category_size: 1000,
            epoch: 42,
        });
        assert!(matches!(
            decode_raft_entry(&unseal).unwrap().as_slice(),
            [Command::Migrate(op)] if matches!(**op, CategoryOp::Unseal { epoch: 42, .. })
        ));
        let order = Order::limit(OrderId(9), Side::Buy, 10, 2).on(InstrumentId(7_001));
        let install = encode_raft_migration_entry(&CategoryOp::Install {
            category: 7,
            category_size: 1000,
            epoch: 42,
            engines: vec![crate::snapshot::EngineState {
                instrument: InstrumentId(7_001),
                engine_seq: 3,
                orders: vec![order],
                stops: Vec::new(),
            }],
            positions: vec![(5, 7_001, -2)],
            halted: vec![7_001],
            command_ids: vec![(9, 7_001), (11, 7_002)],
        });
        match decode_raft_entry(&install).unwrap().as_slice() {
            [Command::Migrate(op)] => match &**op {
                CategoryOp::Install {
                    engines,
                    positions,
                    halted,
                    command_ids,
                    epoch,
                    ..
                } => {
                    assert_eq!(engines[0].orders[0].id, OrderId(9));
                    assert_eq!(positions, &vec![(5, 7_001, -2)]);
                    assert_eq!(halted, &vec![7_001]);
                    assert_eq!(command_ids, &vec![(9, 7_001), (11, 7_002)]);
                    assert_eq!(*epoch, 42);
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
        assert!(decode_raft_config_entry(&seal).is_none());
    }

    #[test]
    fn config_entry_round_trips_and_is_never_a_command_batch() {
        let entry = encode_raft_config_entry(0xfeed, "specs=*:tick=1;window=500000");
        assert_eq!(
            decode_raft_config_entry(&entry),
            Some((0xfeed, "specs=*:tick=1;window=500000".to_string()))
        );
        assert!(decode_raft_entry(&entry).is_none());
        let mut frame = [0u8; MSG_LEN];
        encode_new(&Order::limit(OrderId(1), Side::Buy, 10, 1), &mut frame);
        let batch = encode_raft_batch(&[frame]).unwrap();
        assert!(decode_raft_config_entry(&batch).is_none());
    }

    #[test]
    fn report_round_trips() {
        let r = ExecReport::Trade {
            instrument: InstrumentId(2),
            taker: OrderId(10),
            maker: OrderId(20),
            aggressor: Side::Sell,
            price: 999,
            qty: 5,
            maker_fee: 49,
            taker_fee: 99,
            taker_user: 0,
            maker_user: 0,
        };
        let mut frame = [0u8; REPORT_LEN];
        encode_report(&r, &mut frame);
        let d = decode_report(&frame).unwrap();
        assert_eq!(d.type_code, RT_TRADE);
        assert_eq!(d.order_id, OrderId(10));
        assert_eq!(d.aux_id, 20);
        assert_eq!(d.price, 999);
        assert_eq!(d.qty, 5);
        assert_eq!(d.side, Side::Sell);
        assert_eq!(d.maker_fee, 49, "fees must survive the wire");
        assert_eq!(d.taker_fee, 99);
    }

    #[test]
    fn execution_event_round_trips() {
        let r = ExecReport::Resting {
            instrument: InstrumentId(42),
            order_id: OrderId(99),
            user: 0,
        };
        let mut payload = [0u8; EXECUTION_EVENT_LEN];
        encode_execution_event(7, 1234, 5, &r, &mut payload);
        let event = decode_execution_event(&payload).unwrap();
        assert_eq!(event.raft_group, 7);
        assert_eq!(event.raft_index, 1234);
        assert_eq!(event.ordinal, 5);
        assert_eq!(event.report.type_code, RT_RESTING);
        assert_eq!(event.report.instrument, InstrumentId(42));
        assert_eq!(event.report.order_id, OrderId(99));

        // A bare report is not an event.
        let mut bare = [0u8; REPORT_LEN];
        encode_report(&r, &mut bare);
        assert!(decode_execution_event(&bare).is_none());
    }

    #[test]
    fn new_order_round_trips_zero_copy() {
        let order = Order::limit(OrderId(42), Side::Buy, 1234, 77)
            .on(InstrumentId(7))
            .by(555)
            .with_tif(TimeInForce::Ioc);
        let mut frame = [0u8; MSG_LEN];
        encode_new(&order, &mut frame);

        let view = WireView::parse(&frame).unwrap();
        assert_eq!(view.msg_type(), MT_NEW);
        assert_eq!(view.user(), 555);
        match view.to_command().unwrap() {
            Command::New(o) => {
                assert_eq!(o.id, OrderId(42));
                assert_eq!(o.price, 1234);
                assert_eq!(o.quantity, 77);
                assert_eq!(o.instrument, InstrumentId(7));
                assert_eq!(o.user, 555);
                assert_eq!(o.tif, TimeInForce::Ioc);
            }
            other => panic!("expected New, got {other:?}"),
        }
    }

    #[test]
    fn cancel_modify_force_close_round_trip() {
        let mut frame = [0u8; MSG_LEN];
        encode_cancel(InstrumentId(3), OrderId(9), 77, &mut frame);
        assert!(matches!(
            WireView::parse(&frame).unwrap().to_command().unwrap(),
            Command::Cancel {
                instrument: InstrumentId(3),
                order_id: OrderId(9),
                cmd_id: 77
            }
        ));

        encode_modify(InstrumentId(1), OrderId(5), 500, 12, 78, &mut frame);
        assert!(matches!(
            WireView::parse(&frame).unwrap().to_command().unwrap(),
            Command::Modify {
                order_id: OrderId(5),
                new_price: 500,
                new_qty: 12,
                cmd_id: 78,
                ..
            }
        ));

        encode_force_close(
            InstrumentId(4),
            777,
            OrderId(88),
            Side::Sell,
            250,
            &mut frame,
        );
        match WireView::parse(&frame).unwrap().to_command().unwrap() {
            Command::ForceClose {
                instrument,
                user,
                close_order_id,
                close_side,
                close_qty,
            } => {
                assert_eq!(instrument, InstrumentId(4));
                assert_eq!(user, 777);
                assert_eq!(close_order_id, OrderId(88));
                assert_eq!(close_side, Side::Sell);
                assert_eq!(close_qty, 250);
            }
            other => panic!("expected ForceClose, got {other:?}"),
        }
    }

    #[test]
    fn short_buffer_is_rejected() {
        let short = [0u8; MSG_LEN - 1];
        assert!(WireView::parse(&short).is_none());
    }

    #[test]
    fn raft_batch_is_one_entry() {
        let orders = [
            Order::limit(OrderId(101), Side::Sell, 100, 5).on(InstrumentId(7)),
            Order::limit(OrderId(102), Side::Buy, 100, 5).on(InstrumentId(7)),
        ];
        let frames = orders.map(|order| {
            let mut frame = [0; MSG_LEN];
            encode_new(&order, &mut frame);
            frame
        });

        let payload = encode_raft_batch(&frames).unwrap();
        let commands = decode_raft_entry(&payload).unwrap();

        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].id(), 101);
        assert_eq!(commands[1].id(), 102);
        assert!(
            decode_raft_entry(&frames[0]).is_none(),
            "a bare frame is not an entry"
        );
    }

    #[test]
    fn incarnation_entries_and_fanout_hellos_round_trip() {
        let entry = encode_raft_incarnation_entry(42);
        assert!(matches!(
            decode_raft_entry(&entry).as_deref(),
            Some([Command::Incarnation(42)])
        ));
        assert!(decode_raft_entry(&encode_raft_incarnation_entry(0)).is_none());
        let hello = encode_md_hello(7, 99);
        assert_eq!(decode_md_hello(&hello), Some((7, 99)));
        assert!(
            decode_execution_event(&hello).is_none(),
            "readers that only know execution events skip a hello"
        );
    }
}
