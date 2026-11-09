//! Minimal, dependency-free WebSocket (RFC 6455) server side: the handshake
//! accept key, reading client messages (masked, fragmented, with control
//! frames interleaved) and writing server frames.
//!
//! Shared by the order API (binary order batches in, acknowledgements out)
//! and market-data (pushes).

use std::io::{self, Read, Write};

/// RFC 6455 handshake GUID.
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const OP_CONTINUATION: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

/// A complete client data message.
#[derive(Debug, PartialEq, Eq)]
pub enum Message {
    Binary(Vec<u8>),
    Text(Vec<u8>),
}

/// `Sec-WebSocket-Accept` for a client's `Sec-WebSocket-Key`.
pub fn accept_key(client_key: &str) -> String {
    base64(&sha1(format!("{}{WS_GUID}", client_key.trim()).as_bytes()))
}

/// The `101 Switching Protocols` response completing a handshake.
pub fn handshake_response(client_key: &str) -> String {
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(client_key)
    )
}

/// Write one unmasked, unfragmented server frame.
pub fn write_frame(w: &mut impl Write, opcode: u8, payload: &[u8]) -> io::Result<()> {
    let mut header = Vec::with_capacity(10);
    header.push(0x80 | opcode); // FIN
    match payload.len() {
        0..=125 => header.push(payload.len() as u8),
        126..=65535 => {
            header.push(126);
            header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            header.push(127);
            header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    w.write_all(&header)?;
    w.write_all(payload)
}

/// One thing a client sent: a complete data message, or a control frame the
/// caller must answer itself (when several threads share the socket, only
/// the writer may write, so control replies cannot be sent from here).
#[derive(Debug, PartialEq, Eq)]
pub enum ClientEvent {
    Message(Message),
    Ping(Vec<u8>),
    /// The client closed; carries its status code bytes (if any) to echo.
    Close(Vec<u8>),
}

/// Reads [`ClientEvent`]s from one connection. Keeps a fragmented message's
/// reassembly state between calls, so a ping arriving between fragments is
/// returned without losing the partial message.
#[derive(Default)]
pub struct EventReader {
    message: Option<(u8, Vec<u8>)>,
}

impl EventReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the next [`ClientEvent`]: data messages are reassembled from
    /// fragments, pongs are skipped. `Ok(None)` = the stream ended cleanly
    /// between messages.
    ///
    /// Rejects (with `InvalidData`) what a server must not accept: unmasked
    /// client frames, fragmented or oversized control frames, a continuation
    /// without a start, a new message inside a fragmented one, reserved bits,
    /// and messages larger than `max_len` bytes (checked before reading them).
    pub fn next(&mut self, r: &mut impl Read, max_len: usize) -> io::Result<Option<ClientEvent>> {
        read_event_into(&mut self.message, r, max_len)
    }
}

fn read_event_into(
    message: &mut Option<(u8, Vec<u8>)>,
    r: &mut impl Read,
    max_len: usize,
) -> io::Result<Option<ClientEvent>> {
    let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    loop {
        let mut head = [0u8; 2];
        match r.read_exact(&mut head[..1]) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && message.is_none() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        r.read_exact(&mut head[1..])?;
        let fin = head[0] & 0x80 != 0;
        if head[0] & 0x70 != 0 {
            return Err(invalid("reserved bits set"));
        }
        let opcode = head[0] & 0x0F;
        if head[1] & 0x80 == 0 {
            return Err(invalid("client frames must be masked"));
        }
        let len = match head[1] & 0x7F {
            126 => {
                let mut b = [0u8; 2];
                r.read_exact(&mut b)?;
                u16::from_be_bytes(b) as u64
            }
            127 => {
                let mut b = [0u8; 8];
                r.read_exact(&mut b)?;
                u64::from_be_bytes(b)
            }
            n => n as u64,
        };
        let control = opcode & 0x8 != 0;
        if control && (!fin || len > 125) {
            return Err(invalid("invalid control frame"));
        }
        let buffered = message.as_ref().map_or(0, |(_, data)| data.len() as u64);
        // Compare without adding: a 64-bit length near u64::MAX made
        // `buffered + len` wrap, pass, and then abort on the allocation.
        if !control && len > (max_len as u64).saturating_sub(buffered) {
            return Err(invalid("message too large"));
        }
        let mut mask = [0u8; 4];
        r.read_exact(&mut mask)?;
        let mut payload = vec![0u8; len as usize];
        r.read_exact(&mut payload)?;
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[i % 4];
        }
        match opcode {
            OP_PING => return Ok(Some(ClientEvent::Ping(payload))),
            OP_PONG => {}
            OP_CLOSE => {
                payload.truncate(2);
                return Ok(Some(ClientEvent::Close(payload)));
            }
            OP_TEXT | OP_BINARY => {
                if message.is_some() {
                    return Err(invalid("new message inside a fragmented one"));
                }
                *message = Some((opcode, payload));
            }
            OP_CONTINUATION => match message.as_mut() {
                Some((_, data)) => data.extend_from_slice(&payload),
                None => return Err(invalid("continuation without a message")),
            },
            _ => return Err(invalid("unknown opcode")),
        }
        if fin && !control {
            if let Some((opcode, data)) = message.take() {
                return Ok(Some(ClientEvent::Message(if opcode == OP_TEXT {
                    Message::Text(data)
                } else {
                    Message::Binary(data)
                })));
            }
        }
    }
}

/// Read the next complete data message from a client, answering pings with
/// pongs and a close with a close on `w` (single-threaded sessions). Returns
/// `Ok(None)` once the client closed or the stream ended between frames.
/// Validation as for [`read_event`]. A ping inside a fragmented message is
/// answered after that message completes.
pub fn read_message(
    r: &mut impl Read,
    w: &mut impl Write,
    max_len: usize,
) -> io::Result<Option<Message>> {
    let mut events = EventReader::new();
    loop {
        match events.next(r, max_len)? {
            Some(ClientEvent::Message(message)) => return Ok(Some(message)),
            Some(ClientEvent::Ping(payload)) => write_frame(w, OP_PONG, &payload)?,
            Some(ClientEvent::Close(code)) => {
                let _ = write_frame(w, OP_CLOSE, &code);
                return Ok(None);
            }
            None => return Ok(None),
        }
    }
}

/// Write one *masked* client frame (for clients, tests and benches).
pub fn write_client_frame(
    w: &mut impl Write,
    opcode: u8,
    payload: &[u8],
    mask: [u8; 4],
) -> io::Result<()> {
    let mut header = Vec::with_capacity(14);
    header.push(0x80 | opcode);
    match payload.len() {
        0..=125 => header.push(0x80 | payload.len() as u8),
        126..=65535 => {
            header.push(0x80 | 126);
            header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            header.push(0x80 | 127);
            header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    header.extend_from_slice(&mask);
    w.write_all(&header)?;
    let masked: Vec<u8> = payload
        .iter()
        .enumerate()
        .map(|(i, b)| b ^ mask[i % 4])
        .collect();
    w.write_all(&masked)
}

/// Read one unmasked server frame: `(opcode, payload)` (for clients/tests).
pub fn read_server_frame(r: &mut impl Read) -> io::Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 2];
    r.read_exact(&mut head)?;
    let len = match head[1] & 0x7F {
        126 => {
            let mut b = [0u8; 2];
            r.read_exact(&mut b)?;
            u16::from_be_bytes(b) as usize
        }
        127 => {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            u64::from_be_bytes(b) as usize
        }
        n => n as usize,
    };
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok((head[0] & 0x0F, payload))
}

/// SHA-1 (needed only for the handshake accept key — not for security).
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let (mut h0, mut h1, mut h2, mut h3, mut h4) = (
        0x67452301u32,
        0xEFCDAB89u32,
        0x98BADCFEu32,
        0x10325476u32,
        0xC3D2E1F0u32,
    );
    let ml = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&ml.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, h) in [h0, h1, h2, h3, h4].iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&h.to_be_bytes());
    }
    out
}

/// Standard base64 with padding.
pub fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::new();
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        s.push(T[(n >> 18 & 63) as usize] as char);
        s.push(T[(n >> 12 & 63) as usize] as char);
        s.push(if c.len() > 1 {
            T[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        s.push(if c.len() > 2 {
            T[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_matches_the_rfc_example() {
        // RFC 6455 section 1.3.
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn reads_masked_fragmented_messages_and_answers_pings() {
        let mut wire = Vec::new();
        // "hel" + ping + "lo" as a fragmented binary message.
        let mut first = Vec::new();
        write_client_frame(&mut first, OP_BINARY, b"hel", [1, 2, 3, 4]).unwrap();
        first[0] &= 0x7F; // not FIN
        wire.extend_from_slice(&first);
        write_client_frame(&mut wire, OP_PING, b"p", [9, 9, 9, 9]).unwrap();
        write_client_frame(&mut wire, OP_CONTINUATION, b"lo", [5, 6, 7, 8]).unwrap();
        // A large binary message (64-bit length).
        let big = vec![7u8; 70_000];
        write_client_frame(&mut wire, OP_BINARY, &big, [1, 1, 1, 1]).unwrap();
        write_client_frame(&mut wire, OP_CLOSE, &1000u16.to_be_bytes(), [0, 0, 0, 0]).unwrap();

        let mut r = &wire[..];
        let mut out = Vec::new();
        assert_eq!(
            read_message(&mut r, &mut out, 100_000).unwrap(),
            Some(Message::Binary(b"hello".to_vec()))
        );
        let (op, payload) = read_server_frame(&mut &out[..]).unwrap();
        assert_eq!((op, payload), (OP_PONG, b"p".to_vec()));
        assert_eq!(
            read_message(&mut r, &mut out, 100_000).unwrap(),
            Some(Message::Binary(big))
        );
        assert_eq!(read_message(&mut r, &mut out, 100_000).unwrap(), None);
    }

    #[test]
    fn rejects_unmasked_and_oversized_messages() {
        let mut unmasked = Vec::new();
        write_frame(&mut unmasked, OP_BINARY, b"x").unwrap();
        assert!(read_message(&mut &unmasked[..], &mut Vec::new(), 100).is_err());

        let mut big = Vec::new();
        write_client_frame(&mut big, OP_BINARY, &[0u8; 200], [1, 2, 3, 4]).unwrap();
        assert!(read_message(&mut &big[..], &mut Vec::new(), 100).is_err());

        // A clean end of stream between messages is not an error.
        assert_eq!(
            read_message(&mut &[][..], &mut Vec::new(), 100).unwrap(),
            None
        );
    }
}

#[cfg(test)]
mod length_overflow {
    use super::*;

    #[test]
    fn a_continuation_with_a_huge_length_is_rejected_not_allocated() {
        let mut wire = Vec::new();
        // A 10-byte non-final text fragment...
        wire.extend_from_slice(&[0x01, 0x80 | 10, 0, 0, 0, 0]);
        wire.extend_from_slice(&[b'x'; 10]);
        // ...then a continuation claiming u64::MAX - 9 bytes (sum wraps to 0).
        wire.extend_from_slice(&[0x80, 0x80 | 127]);
        wire.extend_from_slice(&(u64::MAX - 9).to_be_bytes());
        wire.extend_from_slice(&[0, 0, 0, 0]);
        let mut events = EventReader::new();
        let error = loop {
            match events.next(&mut &wire[..], 1024) {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("stream ended without an error"),
                Err(error) => break error,
            }
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    }
}
