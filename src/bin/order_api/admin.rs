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
            let source_addr = group_leader(endpoints, migration.from_group).map_err(|error| {
                format!("{error}; supply raft_index explicitly")
            })?;
            let source_addr = &source_addr;
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

    let source_addr = group_leader(endpoints, migration.from_group).ok();
    let target_addr = group_leader(endpoints, migration.to_group).ok();
    let (Some(source_addr), Some(target_addr)) = (source_addr.as_ref(), target_addr.as_ref()) else {
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

/// A group's admin address for migration calls. `TC_MIGRATION_GROUP_ENDPOINTS`
/// may list every replica of a group (`0=raft-1:9102|raft-2:9102|...`); the
/// steps that propose through the group's log must reach its current leader,
/// found by asking each replica's `/metrics` for `tc_raft_role 2`.
pub(crate) fn group_leader(endpoints: &HashMap<usize, String>, group: usize) -> Result<String, String> {
    let spec = endpoints
        .get(&group)
        .ok_or_else(|| format!("no TC_MIGRATION_GROUP_ENDPOINTS entry for group {group}"))?;
    let replicas: Vec<&str> = spec.split('|').map(str::trim).filter(|addr| !addr.is_empty()).collect();
    if replicas.len() == 1 {
        return Ok(replicas[0].to_string());
    }
    for addr in &replicas {
        if let Ok((200, body)) = trade_core::migration::http_get(addr, "/metrics", Duration::from_secs(2)) {
            if body.lines().any(|line| line.trim() == "tc_raft_role 2") {
                return Ok(addr.to_string());
            }
        }
    }
    Err(format!("group {group} has no reachable leader among {spec}"))
}

fn admin_call(addr: &str, path: &str) -> Result<String, String> {
    match trade_core::migration::http_get(addr, path, Duration::from_secs(20)) {
        Ok((200, body)) => Ok(body),
        Ok((status, body)) => Err(format!("{addr}{path}: HTTP {status}: {}", body.trim())),
        Err(error) => Err(format!("{addr}{path}: {error}")),
    }
}

fn json_number(body: &str, key: &str) -> Option<u64> {
    body.split(&format!("\"{key}\":"))
        .nth(1)?
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

/// Wait until `addr`'s durable snapshot covers `index`, asking it to take one
/// every few seconds; answers that snapshot's category fingerprint.
fn snapshot_fingerprint_at(
    addr: &str,
    category: u32,
    index: u64,
    timeout: Duration,
) -> Result<trade_core::migration::CategoryFingerprint, String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut next_request = std::time::Instant::now();
    loop {
        if std::time::Instant::now() >= next_request {
            admin_call(addr, "/migration/snapshot")?;
            next_request = std::time::Instant::now() + Duration::from_secs(5);
        }
        if let Ok(Some(fingerprint)) =
            trade_core::migration::fetch_category_fingerprint(addr, category, Duration::from_secs(5))
        {
            if fingerprint.raft_applied_index >= index {
                return Ok(fingerprint);
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("{addr}: no snapshot at or after index {index} within {timeout:?}"));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// Move `category` to Raft group `to_group`, end to end (`POST
/// /admin/routes/migrate?category=C&to=G`):
///
/// 1. freeze it here and in the shared route log (every instance holds its
///    commands from now on), and let in-flight forwards finish;
/// 2. seal it on the source group (a straggler is rejected "category-moved",
///    never applied after the export);
/// 3. wait for a source snapshot covering the seal, and for the source
///    outbox to drain (so its last events for these orders reach Kafka
///    before the target's first);
/// 4. install it on the target (the target pulls the state from the source);
/// 5. verify the target's category fingerprint equals the source's;
/// 6. route it to the target (shared route log first), releasing the held
///    commands, then drop it from the source.
///
/// A failure before step 6 undoes what was done: the target's partial
/// install is dropped, the source is unsealed, and the route stays.
pub(crate) fn migrate_category(kafka: &KafkaIngress, category: u32, to_group: usize) -> Result<String, String> {
    let started = std::time::Instant::now();
    let endpoints = &kafka.migration_endpoints;
    let control = &kafka.route_control;
    let wait = Duration::from_secs(env_number("TC_MIGRATION_STEP_TIMEOUT_SECS", 60u64));
    let migration = control.begin(category, to_group)?;
    let from_group = migration.from_group;
    if let Err(error) = route_log::publish(kafka, category, route_log::RouteRecord::Freeze(to_group)) {
        control.abort(category);
        return Err(error);
    }
    log_info!("migration", category; "event=frozen from={from_group} to={to_group}");
    std::thread::sleep(Duration::from_millis(env_number("TC_MIGRATION_DRAIN_MS", 1_500u64)));

    let mut sealed_on: Option<String> = None;
    let mut installed_on: Option<String> = None;
    let result = (|| -> Result<(u64, u64, u64), String> {
        let source = group_leader(endpoints, from_group)?;
        let seal_index = json_number(&admin_call(&source, &format!("/migration/seal?category={category}"))?, "index")
            .ok_or("seal answered no index")?;
        sealed_on = Some(source.clone());
        log_info!("migration", category; "event=sealed source={source} index={seal_index}");
        let source_fingerprint = snapshot_fingerprint_at(&source, category, seal_index, wait)?;
        let deadline = std::time::Instant::now() + wait;
        loop {
            let pending = admin_call(&source, "/metrics")?
                .lines()
                .find_map(|line| line.strip_prefix("tc_execution_outbox_pending "))
                .and_then(|value| value.trim().parse::<u64>().ok());
            if pending == Some(0) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!("source outbox still has {pending:?} unpublished events"));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let target = group_leader(endpoints, to_group)?;
        let install_index = json_number(
            &admin_call(
                &target,
                &format!("/migration/install?category={category}&from={source}&min_index={seal_index}"),
            )?,
            "index",
        )
        .ok_or("install answered no index")?;
        installed_on = Some(target.clone());
        let target_fingerprint = snapshot_fingerprint_at(&target, category, install_index, wait)?;
        if target_fingerprint.fingerprint != source_fingerprint.fingerprint {
            return Err(format!(
                "target fingerprint {} differs from source {} (install refused or incomplete)",
                target_fingerprint.fingerprint, source_fingerprint.fingerprint
            ));
        }
        control.verified(category, source_fingerprint.fingerprint)?;
        Ok((seal_index, install_index, source_fingerprint.fingerprint))
    })();

    let (seal_index, install_index, fingerprint) = match result {
        Ok(done) => done,
        Err(error) => {
            log_error!("migration", category; "event=failed error={error}; rolling back");
            if let Some(target) = installed_on {
                let _ = admin_call(&target, &format!("/migration/drop?category={category}"));
            }
            if sealed_on.is_some() {
                if let Ok(source) = group_leader(endpoints, from_group) {
                    let _ = admin_call(&source, &format!("/migration/unseal?category={category}"));
                }
            }
            let _ = route_log::publish(kafka, category, route_log::RouteRecord::Pin(from_group));
            control.abort(category);
            return Err(error);
        }
    };
    route_log::publish(kafka, category, route_log::RouteRecord::Pin(to_group))?;
    // The route log follower may apply our own pin first, which already
    // routes the category to the target and ends the migration here.
    let version = control.activate(category).or_else(|error| {
        if control.group_for(category) == to_group {
            Ok(control.version())
        } else {
            Err(error)
        }
    })?;
    log_info!("migration", category; "event=activated to={to_group} route_version={version}");
    let dropped = group_leader(endpoints, from_group)
        .and_then(|source| admin_call(&source, &format!("/migration/drop?category={category}")));
    if let Err(error) = &dropped {
        // Harmless for correctness (the source stays sealed); retry later.
        log_warn!("migration", category; "event=drop_failed error={error}");
    }
    Ok(format!(
        "{{\"migrated\":true,\"category\":{category},\"from\":{from_group},\"to\":{to_group},\"seal_index\":{seal_index},\"install_index\":{install_index},\"fingerprint\":{fingerprint},\"route_version\":{version},\"source_dropped\":{},\"elapsed_ms\":{}}}",
        dropped.is_ok(),
        started.elapsed().as_millis()
    ))
}
