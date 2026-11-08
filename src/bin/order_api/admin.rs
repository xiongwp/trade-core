//! Route-migration admin endpoints: freeze a category, verify consumers caught up.

use super::*;

/// Handle `/admin/routes/caught-up`. When `TC_MIGRATION_GROUP_ENDPOINTS` maps
/// both the migration's source and target Raft groups, this fetches each side's
/// per-category matching-state fingerprint from that group's node and verifies
/// the cutover against real state — the operator no longer supplies (and the
/// control plane no longer trusts) fingerprints as query parameters. Without an
/// endpoint map, it falls back to the legacy caller-supplied fingerprints.
pub(crate) fn verify_caught_up(kafka: &KafkaIngress, category: u32, query: &str) -> Result<String, String> {
    let timeout = Duration::from_millis(env_number("TC_MIGRATION_FINGERPRINT_TIMEOUT_MS", 2_000u64));
    verify_caught_up_inner(
        &kafka.route_control,
        &kafka.migration_endpoints,
        category,
        query,
        timeout,
    )
}

/// Handle `/admin/routes/frozen`. The freeze index may be supplied explicitly
/// (`?raft_index=N`); otherwise, when the source group has a configured
/// endpoint, it is auto-captured from that node's current live applied index so
/// an operator no longer hand-supplies it. The caller must have let the source
/// pipeline drain first — `begin` already stopped accepting the category's
/// writes — so the captured index is the category's final index.
pub(crate) fn freeze_migration(kafka: &KafkaIngress, category: u32, query: &str) -> Result<String, String> {
    let timeout = Duration::from_millis(env_number("TC_MIGRATION_FINGERPRINT_TIMEOUT_MS", 2_000u64));
    freeze_migration_inner(
        &kafka.route_control,
        &kafka.migration_endpoints,
        category,
        query,
        timeout,
    )
}

/// Testable core of [`freeze_migration`], decoupled from `KafkaIngress`.
pub(crate) fn freeze_migration_inner(
    route_control: &trade_core::cluster::RouteControlPlane,
    endpoints: &HashMap<usize, String>,
    category: u32,
    query: &str,
    timeout: Duration,
) -> Result<String, String> {
    let migration = route_control
        .active(category)
        .ok_or("category migration is not active")?;

    let (index, captured_from) = match query_param(query, "raft_index") {
        Some(_) => (required::<u64>(query, "raft_index")?, "caller"),
        None => {
            let source_addr = endpoints.get(&migration.from_group).ok_or_else(|| {
                format!(
                    "no endpoint for source group {}; supply raft_index explicitly",
                    migration.from_group
                )
            })?;
            let index = trade_core::migration::fetch_applied_index(source_addr, timeout)
                .map_err(|error| {
                    format!("applied-index fetch from {source_addr} failed: {error}")
                })?;
            (index, "source-node")
        }
    };

    route_control.frozen(category, index).map(|_| {
        format!(
            "{{\"accepted\":true,\"state\":\"CATCHING_UP\",\"frozen_index\":{index},\"captured_from\":\"{captured_from}\"}}"
        )
    })
}

/// Testable core of [`verify_caught_up`], decoupled from `KafkaIngress` so it
/// can be driven against stub fingerprint servers. `endpoints` maps a Raft
/// group id to the address serving its `/fingerprint` route.
pub(crate) fn verify_caught_up_inner(
    route_control: &trade_core::cluster::RouteControlPlane,
    endpoints: &HashMap<usize, String>,
    category: u32,
    query: &str,
    timeout: Duration,
) -> Result<String, String> {
    let migration = route_control
        .active(category)
        .ok_or("category migration is not active")?;

    let source_addr = endpoints.get(&migration.from_group);
    let target_addr = endpoints.get(&migration.to_group);
    let (Some(source_addr), Some(target_addr)) = (source_addr, target_addr) else {
        // Legacy path: no endpoint map for these groups, trust the caller.
        return route_control
            .caught_up(
                category,
                required::<u64>(query, "raft_index")?,
                required::<u64>(query, "source_fingerprint")?,
                required::<u64>(query, "target_fingerprint")?,
            )
            .map(|_| "{\"accepted\":true,\"state\":\"VERIFIED\",\"verified_by\":\"caller\"}".into());
    };

    let fetch = |addr: &str| -> Result<trade_core::migration::CategoryFingerprint, String> {
        trade_core::migration::fetch_category_fingerprint(addr, category, timeout)
            .map_err(|error| format!("fingerprint fetch from {addr} failed: {error}"))?
            .ok_or_else(|| format!("group node {addr} has no durable snapshot yet; retry"))
    };
    let source = fetch(source_addr)?;
    let target = fetch(target_addr)?;

    let verified = trade_core::migration::verify_cutover(migration.frozen_index, source, target)?;
    // Feed the verified, node-reported values into the fenced state machine;
    // the equality check inside caught_up is now backed by real state.
    route_control
        .caught_up(
            category,
            verified.raft_applied_index,
            verified.fingerprint,
            verified.fingerprint,
        )
        .map(|_| {
            format!(
                "{{\"accepted\":true,\"state\":\"VERIFIED\",\"verified_by\":\"nodes\",\"raft_applied_index\":{},\"fingerprint\":{}}}",
                verified.raft_applied_index, verified.fingerprint
            )
        })
}
