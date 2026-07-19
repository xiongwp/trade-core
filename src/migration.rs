//! Route-migration verification: computing the per-category matching-state
//! fingerprint from a durable snapshot, so a cutover can be verified against
//! real state on both the source and target Raft groups.
//!
//! A category route migration ([`crate::cluster::RouteControlPlane`]) freezes a
//! category on its source group at some Raft index, replays it onto the target
//! group up to that index, and may only cut over once both groups hold
//! byte-identical book state for that category. The value both sides must agree
//! on is [`crate::exchange::Processor::category_fingerprint`].
//!
//! The live processor lives on the matching thread, but every matching node
//! also writes a durable state snapshot (`snapshot-shard-N.bin`) on a cadence,
//! stamped with the exact `raft_applied_index` the image represents. Computing
//! the fingerprint from that snapshot lets a status endpoint answer a
//! verification query without reaching into the matching hot path, and pins the
//! answer to a specific applied index so the control plane can require both
//! groups to report the *same* index as well as the same fingerprint.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use crate::exchange::fingerprint_engine_states;
use crate::sharding::asset_category;
use crate::snapshot;
use crate::types::InstrumentId;

/// A category fingerprint pinned to the applied index it was taken at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CategoryFingerprint {
    /// Raft applied index of the snapshot this was computed from. Both sides of
    /// a migration must report the same index for a comparison to be meaningful.
    pub raft_applied_index: u64,
    /// Order-sensitive fingerprint of the category's resting book state.
    pub fingerprint: u64,
}

/// Compute a category's matching-state fingerprint from a durable snapshot.
///
/// Returns `Ok(None)` when the snapshot file does not exist yet (a freshly
/// started node that has not snapshotted). Any other I/O or decode error
/// (corrupt/torn snapshot) is surfaced as `Err` so a verifier fails closed
/// rather than comparing against a bogus value.
///
/// The result is bit-identical to [`crate::exchange::Processor::category_fingerprint`]
/// computed on the live processor that produced the snapshot, because both fold
/// the same `EngineState`s through [`fingerprint_engine_states`].
pub fn snapshot_category_fingerprint(
    snapshot_path: &Path,
    category_id: u32,
    category_size: u32,
) -> io::Result<Option<CategoryFingerprint>> {
    match snapshot::load(snapshot_path) {
        Ok(snap) => {
            let fingerprint = fingerprint_engine_states(&snap.engines, |instrument| {
                asset_category(instrument, category_size) == category_id
            });
            Ok(Some(CategoryFingerprint {
                raft_applied_index: snap.raft_applied_index,
                fingerprint,
            }))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Verify that a source and target fingerprint agree closely enough to cut a
/// migration over: both must be pinned to an applied index at least
/// `frozen_index` (the target has fully caught up) and carry the same
/// fingerprint (identical book state for the category).
///
/// Returns the agreed `(raft_applied_index, fingerprint)` on success, or a
/// human-readable reason on mismatch — the reason is safe to surface to an
/// operator driving `/admin/routes/caught-up`.
pub fn verify_cutover(
    frozen_index: u64,
    source: CategoryFingerprint,
    target: CategoryFingerprint,
) -> Result<CategoryFingerprint, String> {
    if source.raft_applied_index < frozen_index {
        return Err(format!(
            "source snapshot index {} is behind the freeze index {frozen_index}",
            source.raft_applied_index
        ));
    }
    if target.raft_applied_index < frozen_index {
        return Err(format!(
            "target snapshot index {} has not caught up to the freeze index {frozen_index}",
            target.raft_applied_index
        ));
    }
    if source.raft_applied_index != target.raft_applied_index {
        return Err(format!(
            "source index {} and target index {} differ; snapshot both sides at the same applied index before comparing",
            source.raft_applied_index, target.raft_applied_index
        ));
    }
    if source.fingerprint != target.fingerprint {
        return Err(format!(
            "source/target matching fingerprints differ at index {} ({:#018x} vs {:#018x})",
            source.raft_applied_index, source.fingerprint, target.fingerprint
        ));
    }
    Ok(source)
}

/// The instrument's owning category under the default asset-category striping.
pub fn category_of(instrument: InstrumentId, category_size: u32) -> u32 {
    asset_category(instrument, category_size)
}

/// Parse the `category` query parameter out of an HTTP request line of the form
/// `GET /fingerprint?category=<u32> HTTP/1.1`. Returns `None` when the path is
/// not the fingerprint route (so the caller falls through to other handlers).
/// Returns `Some(Err(..))` when the route matches but the parameter is missing
/// or malformed, so the server can answer 400 rather than silently 404.
pub fn parse_fingerprint_request(line: &str) -> Option<Result<u32, String>> {
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    let target = parts.next()?;
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    if path != "/fingerprint" {
        return None;
    }
    let category = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("category="));
    Some(match category {
        Some(value) => value
            .parse::<u32>()
            .map_err(|_| format!("category must be a u32, got {value:?}")),
        None => Err("missing category query parameter".to_string()),
    })
}

/// Render the JSON body a `/fingerprint?category=N` query answers with. Kept
/// tiny and dependency-free (no serde) — the consumer is
/// [`parse_fingerprint_response`].
pub fn fingerprint_response_json(category_id: u32, fp: CategoryFingerprint) -> String {
    format!(
        "{{\"category\":{category_id},\"raft_applied_index\":{},\"fingerprint\":{}}}",
        fp.raft_applied_index, fp.fingerprint
    )
}

/// Parse a `group:host:port` migration-endpoint map from an environment-style
/// spec, e.g. `"0=10.0.0.1:9102,1=10.0.0.2:9102"`. Each entry maps a Raft group
/// id to the admin/metrics address serving that group's `/fingerprint` route.
/// Order-system control planes use it to reach a category's source and target
/// groups during a cutover.
pub fn parse_group_endpoints(spec: &str) -> Result<HashMap<usize, String>, String> {
    let mut map = HashMap::new();
    for token in spec.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        let (group, addr) = token
            .split_once('=')
            .ok_or_else(|| format!("endpoint {token:?} is not group=host:port"))?;
        let group: usize = group
            .trim()
            .parse()
            .map_err(|_| format!("invalid group id in {token:?}"))?;
        let addr = addr.trim();
        if addr.is_empty() {
            return Err(format!("endpoint {token:?} has an empty address"));
        }
        if map.insert(group, addr.to_string()).is_some() {
            return Err(format!("duplicate endpoint for group {group}"));
        }
    }
    if map.is_empty() {
        return Err("no migration endpoints configured".to_string());
    }
    Ok(map)
}

/// Fetch a group's per-category fingerprint over HTTP from its `/fingerprint`
/// route (served by [`crate::metrics::serve_with_extra`] on the matching node).
///
/// `Ok(None)` means the node answered 503 — it has no durable snapshot yet, so
/// the caller must wait and retry rather than treat the absence as agreement.
/// A non-200/503 status, a connection failure, or an unparseable body is an
/// `Err`, so verification fails closed.
pub fn fetch_category_fingerprint(
    addr: &str,
    category_id: u32,
    timeout: Duration,
) -> io::Result<Option<CategoryFingerprint>> {
    let (status, body) = http_get(addr, &format!("/fingerprint?category={category_id}"), timeout)?;
    // 503: no durable snapshot yet — caller must wait, not treat as agreement.
    if status == 503 {
        return Ok(None);
    }
    if status != 200 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("fingerprint endpoint {addr} returned HTTP {status}"),
        ));
    }
    parse_fingerprint_response(&body).map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unparseable fingerprint response from {addr}"),
        )
    })
}

/// Minimal dependency-free HTTP/1.1 GET returning `(status_code, body)`.
///
/// Tolerates a connection reset *after* bytes arrive: a `Connection: close`
/// peer that closes without draining our request can make the OS deliver an
/// RST, surfacing as ConnectionReset on the trailing read even though the full
/// response is already in hand. Only an error with no bytes received is fatal.
fn http_get(addr: &str, path: &str, timeout: Duration) -> io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )?;
    let mut raw = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&chunk[..n]),
            Err(ref error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                if raw.is_empty() {
                    return Err(error);
                }
                break;
            }
        }
    }
    let text = String::from_utf8_lossy(&raw);
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP status line"))?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

/// Fetch a matching node's current live Raft applied index from its
/// `/applied-index` route. Used to auto-capture a migration's freeze index from
/// the source group instead of having an operator hand-supply it.
///
/// Correctness note: the caller must have already frozen writes for the
/// category (`RouteControlPlane::begin`) and allowed the source's in-flight
/// command pipeline to drain, so the returned index is the *final* index for
/// that category. This function only reads the value; it does not enforce the
/// drain.
pub fn fetch_applied_index(addr: &str, timeout: Duration) -> io::Result<u64> {
    let (status, body) = http_get(addr, "/applied-index", timeout)?;
    if status != 200 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("applied-index endpoint {addr} returned HTTP {status}"),
        ));
    }
    parse_applied_index_response(&body).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unparseable applied-index response from {addr}"),
        )
    })
}

/// True when `line` is the `GET /applied-index` route (query ignored).
pub fn is_applied_index_request(line: &str) -> bool {
    let mut parts = line.split_whitespace();
    let _method = parts.next();
    parts
        .next()
        .map(|target| target.split('?').next() == Some("/applied-index"))
        .unwrap_or(false)
}

/// Render the `/applied-index` JSON body.
pub fn applied_index_response_json(raft_applied_index: u64) -> String {
    format!("{{\"raft_applied_index\":{raft_applied_index}}}")
}

/// Parse the body produced by [`applied_index_response_json`].
pub fn parse_applied_index_response(body: &str) -> Option<u64> {
    let key = "\"raft_applied_index\"";
    let start = body.find(key)? + key.len();
    let rest = body[start..].trim_start_matches([':', ' ']);
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// Parse the JSON body produced by [`fingerprint_response_json`] back into a
/// [`CategoryFingerprint`]. A hand-rolled parser (three unsigned fields) keeps
/// the crate dependency-free; it tolerates whitespace but not reordering, which
/// is fine because it only ever reads our own emitter's output.
pub fn parse_fingerprint_response(body: &str) -> Option<CategoryFingerprint> {
    fn field(body: &str, key: &str) -> Option<u64> {
        let start = body.find(key)? + key.len();
        let rest = body[start..].trim_start_matches([':', ' ']);
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].parse().ok()
    }
    Some(CategoryFingerprint {
        raft_applied_index: field(body, "\"raft_applied_index\"")?,
        fingerprint: field(body, "\"fingerprint\"")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::Processor;
    use crate::order::Order;
    use crate::sharding::DEFAULT_ASSET_CATEGORY_SIZE;
    use crate::snapshot::SnapshotData;
    use crate::strategy::PriceTimePriority;
    use crate::types::{OrderId, Side};

    const CAT_SIZE: u32 = DEFAULT_ASSET_CATEGORY_SIZE;

    fn instrument_in_category(c: u32, nth: u32) -> u32 {
        c * CAT_SIZE + 1 + nth
    }

    fn write_snapshot(path: &Path, processor: &Processor, raft_applied_index: u64) {
        let engines = processor.export_state();
        let data = SnapshotData {
            journal_seq: 0,
            raft_applied_index,
            max_cmd_id: 0,
            max_admin_id: 0,
            halted: &[],
            suspended: &[],
            positions: &[],
            engines: &engines,
        };
        snapshot::write(path, data).unwrap();
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tc-migration-{tag}-{}-{}",
            std::process::id(),
            crate::journal::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn snapshot_fingerprint_matches_the_live_processor() {
        let cat = 4;
        let inst = InstrumentId(instrument_in_category(cat, 2));
        let mut p = Processor::new(|| Box::new(PriceTimePriority), None);
        p.process(
            crate::exchange::Command::New(Order::limit(OrderId(1), Side::Buy, 100, 5).on(inst)),
            &mut |_| {},
        );
        p.process(
            crate::exchange::Command::New(Order::limit(OrderId(2), Side::Sell, 110, 4).on(inst)),
            &mut |_| {},
        );

        let dir = tmp("live-eq");
        let path = dir.join("snapshot-shard-0.bin");
        write_snapshot(&path, &p, 77);

        let got = snapshot_category_fingerprint(&path, cat, CAT_SIZE)
            .unwrap()
            .expect("snapshot exists");
        assert_eq!(got.raft_applied_index, 77);
        assert_eq!(
            got.fingerprint,
            p.category_fingerprint(cat, CAT_SIZE),
            "snapshot-derived fingerprint must equal the live processor's"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn missing_snapshot_is_none_not_error() {
        let dir = tmp("missing");
        let path = dir.join("does-not-exist.bin");
        assert_eq!(snapshot_category_fingerprint(&path, 0, CAT_SIZE).unwrap(), None);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn applied_index_request_and_response_round_trip() {
        assert!(is_applied_index_request("GET /applied-index HTTP/1.1"));
        assert!(is_applied_index_request("GET /applied-index?x=1 HTTP/1.1"));
        assert!(!is_applied_index_request("GET /fingerprint?category=1 HTTP/1.1"));
        let body = applied_index_response_json(9_876);
        assert_eq!(parse_applied_index_response(&body), Some(9_876));
        assert_eq!(parse_applied_index_response("garbage"), None);
    }

    #[test]
    fn fetch_applied_index_reads_a_served_value() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 512];
            let _ = s.read(&mut buf);
            let body = applied_index_response_json(4_242);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(response.as_bytes());
        });
        assert_eq!(
            fetch_applied_index(&addr, Duration::from_secs(2)).unwrap(),
            4_242
        );
        handle.join().unwrap();
    }

    #[test]
    fn group_endpoint_parsing() {
        let map = parse_group_endpoints("0=host-a:9102, 1=host-b:9102").unwrap();
        assert_eq!(map.get(&0).map(String::as_str), Some("host-a:9102"));
        assert_eq!(map.get(&1).map(String::as_str), Some("host-b:9102"));
        assert!(parse_group_endpoints("").is_err());
        assert!(parse_group_endpoints("garbage").is_err());
        assert!(parse_group_endpoints("0=a:1,0=b:2").is_err(), "duplicate group rejected");
    }

    #[test]
    fn fetch_category_fingerprint_reads_a_served_response() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let fp = CategoryFingerprint {
            raft_applied_index: 555,
            fingerprint: 0x0123_4567_89ab_cdef,
        };
        // 200 server: replies with a well-formed fingerprint body.
        let ok = TcpListener::bind("127.0.0.1:0").unwrap();
        let ok_addr = ok.local_addr().unwrap().to_string();
        let ok_body = fingerprint_response_json(9, fp);
        let ok_handle = std::thread::spawn(move || {
            let (mut s, _) = ok.accept().unwrap();
            let mut buf = [0u8; 512];
            let _ = s.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ok_body}",
                ok_body.len()
            );
            let _ = s.write_all(response.as_bytes());
        });
        let got = fetch_category_fingerprint(&ok_addr, 9, Duration::from_secs(2))
            .unwrap()
            .expect("200 yields Some");
        assert_eq!(got, fp);
        ok_handle.join().unwrap();

        // 503 server: no snapshot yet → Ok(None), not an error.
        let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
        let un_addr = unavailable.local_addr().unwrap().to_string();
        let un_handle = std::thread::spawn(move || {
            let (mut s, _) = unavailable.accept().unwrap();
            let mut buf = [0u8; 512];
            let _ = s.read(&mut buf);
            let body = "no durable snapshot yet\n";
            let response = format!(
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(response.as_bytes());
        });
        assert_eq!(
            fetch_category_fingerprint(&un_addr, 9, Duration::from_secs(2)).unwrap(),
            None
        );
        un_handle.join().unwrap();
    }

    #[test]
    fn verify_cutover_accepts_equal_state_at_the_same_index() {
        let fp = CategoryFingerprint {
            raft_applied_index: 100,
            fingerprint: 0xdead_beef,
        };
        assert_eq!(verify_cutover(100, fp, fp), Ok(fp));
        // Target ahead of the freeze index but at the same index as source is
        // fine (both caught up to exactly the frozen point).
        assert!(verify_cutover(90, fp, fp).is_ok());
    }

    #[test]
    fn fingerprint_request_parsing() {
        assert_eq!(
            parse_fingerprint_request("GET /fingerprint?category=42 HTTP/1.1"),
            Some(Ok(42))
        );
        // Not our route.
        assert_eq!(parse_fingerprint_request("GET /metrics HTTP/1.1"), None);
        // Our route, bad/missing param → 400-worthy error, not a fall-through.
        assert!(matches!(
            parse_fingerprint_request("GET /fingerprint HTTP/1.1"),
            Some(Err(_))
        ));
        assert!(matches!(
            parse_fingerprint_request("GET /fingerprint?category=abc HTTP/1.1"),
            Some(Err(_))
        ));
    }

    #[test]
    fn fingerprint_response_round_trips_over_the_wire_format() {
        let fp = CategoryFingerprint {
            raft_applied_index: 12_345,
            fingerprint: 0xfeed_face_dead_beef,
        };
        let body = fingerprint_response_json(7, fp);
        assert_eq!(parse_fingerprint_response(&body), Some(fp));
    }

    #[test]
    fn verify_cutover_rejects_lagging_or_divergent_sides() {
        let source = CategoryFingerprint {
            raft_applied_index: 100,
            fingerprint: 1,
        };
        // Target behind the freeze index.
        let behind = CategoryFingerprint {
            raft_applied_index: 99,
            fingerprint: 1,
        };
        assert!(verify_cutover(100, source, behind).is_err());
        // Same index, different fingerprint (state diverged).
        let diverged = CategoryFingerprint {
            raft_applied_index: 100,
            fingerprint: 2,
        };
        assert!(verify_cutover(100, source, diverged).is_err());
        // Both caught up but at different indices.
        let ahead = CategoryFingerprint {
            raft_applied_index: 101,
            fingerprint: 1,
        };
        assert!(verify_cutover(100, source, ahead).is_err());
    }
}
