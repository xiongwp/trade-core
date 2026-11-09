//! Category migration: `POST /admin/routes/migrate` and `/admin/routes/abort`.

use super::*;

/// A group's admin address for migration calls. `TC_MIGRATION_GROUP_ENDPOINTS`
/// may list every replica of a group (`0=raft-1:9102|raft-2:9102|...`); the
/// steps that propose through the group's log must reach its current leader,
/// found by asking each replica's `/metrics` for `tc_raft_role 2`.
pub(crate) fn group_leader(
    endpoints: &HashMap<usize, String>,
    group: usize,
) -> Result<String, String> {
    let spec = endpoints
        .get(&group)
        .ok_or_else(|| format!("no TC_MIGRATION_GROUP_ENDPOINTS entry for group {group}"))?;
    let replicas: Vec<&str> = spec
        .split('|')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .collect();
    if replicas.len() == 1 {
        return Ok(replicas[0].to_string());
    }
    for addr in &replicas {
        if let Ok((200, body)) =
            trade_core::migration::http_get(addr, "/metrics", Duration::from_secs(2))
        {
            if body.lines().any(|line| line.trim() == "tc_raft_role 2") {
                return Ok(addr.to_string());
            }
        }
    }
    Err(format!(
        "group {group} has no reachable leader among {spec}"
    ))
}

fn admin_call(addr: &str, path: &str) -> Result<String, String> {
    admin_call_within(addr, path, Duration::from_secs(20))
}

fn admin_call_within(addr: &str, path: &str, timeout: Duration) -> Result<String, String> {
    match trade_core::migration::http_get(addr, path, timeout) {
        Ok((200, body)) => Ok(body),
        Ok((status, body)) => Err(format!("{addr}{path}: HTTP {status}: {}", body.trim())),
        Err(error) => Err(format!("{addr}{path}: {error}")),
    }
}

/// Propose a step through `group`'s current leader until it commits (the
/// leader is resolved again for every attempt). Steps are idempotent per
/// epoch, so a retry after an unknown outcome is safe.
fn propose_until_committed(
    endpoints: &HashMap<usize, String>,
    group: usize,
    path: &str,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let attempt = group_leader(endpoints, group).and_then(|leader| admin_call(&leader, path));
        match attempt {
            Ok(body) => return Ok(body),
            Err(error) if std::time::Instant::now() >= deadline => return Err(error),
            Err(error) => {
                log_warn!("migration", group; "event=step_retry path={path} error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

/// `(sealed, epoch)` of `category` in `addr`'s durable snapshot, once that
/// snapshot covers `index`.
fn migration_status_at(
    addr: &str,
    category: u32,
    index: u64,
    timeout: Duration,
) -> Result<(bool, u64), String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut next_request = std::time::Instant::now();
    loop {
        if std::time::Instant::now() >= next_request {
            admin_call(addr, "/migration/snapshot")?;
            next_request = std::time::Instant::now() + Duration::from_secs(5);
        }
        if let Ok(body) = admin_call(addr, &format!("/migration/status?category={category}")) {
            if json_number(&body, "raft_applied_index").is_some_and(|applied| applied >= index) {
                return Ok((
                    body.contains("\"sealed\":true"),
                    json_number(&body, "epoch").unwrap_or(0),
                ));
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "{addr}: no snapshot at or after index {index} within {timeout:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(300));
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
        if let Ok(Some(fingerprint)) = trade_core::migration::fetch_category_fingerprint(
            addr,
            category,
            Duration::from_secs(5),
        ) {
            if fingerprint.raft_applied_index >= index {
                return Ok(fingerprint);
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "{addr}: no snapshot at or after index {index} within {timeout:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// Move `category` to Raft group `to_group`, end to end (`POST
/// /admin/routes/migrate?category=C&to=G`):
///
/// 1. freeze it in the shared route log (every instance holds its commands
///    from now on); the freeze record's position is the migration's epoch,
///    and every later step carries it and proceeds only while that freeze
///    is still the category's latest record — a concurrent migration or an
///    abort stops this one, and the groups ignore its stale steps;
/// 2. seal it on the source group and check, in the source's snapshot, that
///    the seal applied with this epoch (a straggler is rejected
///    "category-moved", never applied after the export);
/// 3. wait for the source outbox to drain (its last events for these orders
///    reach Kafka before the target's first);
/// 4. install it on the target (books, stops, positions, halts and the
///    category's recent command ids, pulled from the source snapshot);
/// 5. verify the target applied it with this epoch and that its category
///    fingerprint (books and positions) equals the source's;
/// 6. route it to the target (shared route log first), releasing the held
///    commands, then drop it from the source.
///
/// A failure before step 6 undoes every step that was attempted — whether
/// or not its reply arrived, as a timed-out step may still commit — until
/// each undo commits: the target drops what this epoch installed, the
/// source unseals what this epoch sealed, and the route returns to the
/// source. The same rollback runs from `/admin/routes/abort` for a
/// migration whose orchestrator stopped mid-way.
pub(crate) fn migrate_category(
    kafka: &KafkaIngress,
    category: u32,
    to_group: usize,
) -> Result<String, String> {
    let started = std::time::Instant::now();
    let endpoints = &kafka.migration_endpoints;
    let control = &kafka.route_control;
    let wait = Duration::from_secs(env_number("TC_MIGRATION_STEP_TIMEOUT_SECS", 60u64));
    let migration = control.begin(category, to_group)?;
    let from_group = migration.from_group;
    let freeze = route_log::RouteRecord::Freeze {
        to: to_group,
        from: from_group,
    };
    let freeze_offset = match route_log::publish(kafka, category, freeze) {
        Ok(offset) => offset,
        Err(error) => {
            control.abort(category);
            return Err(error);
        }
    };
    let epoch = freeze_offset as u64 + 1;
    // Ours only if no other record for the category landed before ours was
    // read back... and nothing newer since: checked before every step.
    let still_ours = || -> Result<(), String> {
        match route_log::latest_after(category, freeze_offset, Duration::from_secs(10))? {
            Some((offset, _)) if offset == freeze_offset => Ok(()),
            other => Err(format!(
                "route log moved on (latest record {other:?}, ours at {freeze_offset}): another migration or an abort"
            )),
        }
    };
    log_info!("migration", category; "event=frozen from={from_group} to={to_group} epoch={epoch}");
    std::thread::sleep(Duration::from_millis(env_number(
        "TC_MIGRATION_DRAIN_MS",
        1_500u64,
    )));

    let mut seal_attempted = false;
    let mut install_attempted = false;
    let result = (|| -> Result<(u64, u64, u64), String> {
        still_ours()?;
        let source = group_leader(endpoints, from_group)?;
        seal_attempted = true;
        let seal_index = json_number(
            &admin_call(
                &source,
                &format!("/migration/seal?category={category}&epoch={epoch}"),
            )?,
            "index",
        )
        .ok_or("seal answered no index")?;
        let (sealed, applied_epoch) = migration_status_at(&source, category, seal_index, wait)?;
        if !sealed || applied_epoch != epoch {
            return Err(format!(
                "seal not applied on group {from_group} (sealed={sealed}, epoch {applied_epoch}, ours {epoch}): another migration holds it"
            ));
        }
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
                return Err(format!(
                    "source outbox still has {pending:?} unpublished events"
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        still_ours()?;
        let target = group_leader(endpoints, to_group)?;
        install_attempted = true;
        // The target fetches the state (up to 10 s) and commits it (up to 15 s).
        let install_index = json_number(
            &admin_call_within(
                &target,
                &format!("/migration/install?category={category}&epoch={epoch}&from={source}&min_index={seal_index}"),
                Duration::from_secs(60),
            )?,
            "index",
        )
        .ok_or("install answered no index")?;
        let (sealed, applied_epoch) = migration_status_at(&target, category, install_index, wait)?;
        if sealed || applied_epoch != epoch {
            return Err(format!(
                "install not applied on group {to_group} (sealed={sealed}, epoch {applied_epoch}, ours {epoch})"
            ));
        }
        let target_fingerprint = snapshot_fingerprint_at(&target, category, install_index, wait)?;
        if target_fingerprint.fingerprint != source_fingerprint.fingerprint {
            return Err(format!(
                "target fingerprint {} differs from source {} (install refused or incomplete)",
                target_fingerprint.fingerprint, source_fingerprint.fingerprint
            ));
        }
        control.verified(category, source_fingerprint.fingerprint)?;
        still_ours()?;
        Ok((seal_index, install_index, source_fingerprint.fingerprint))
    })();

    let (seal_index, install_index, fingerprint) = match result {
        Ok(done) => done,
        Err(error) => {
            log_error!("migration", category; "event=failed error={error}; rolling back");
            let undone = roll_back(
                kafka,
                category,
                from_group,
                to_group,
                epoch,
                freeze_offset,
                seal_attempted,
                install_attempted,
            );
            control.abort(category);
            return Err(match undone {
                Ok(()) => error,
                Err(undo) => format!("{error}; ROLLBACK INCOMPLETE ({undo}): retry POST /admin/routes/abort?category={category}"),
            });
        }
    };
    // The cutover. Retried: until this record lands every instance holds the
    // category's commands.
    let pin_deadline = std::time::Instant::now() + wait;
    loop {
        match route_log::publish(kafka, category, route_log::RouteRecord::Pin(to_group)) {
            Ok(_) => break,
            Err(error) if std::time::Instant::now() < pin_deadline => {
                log_warn!("migration", category; "event=pin_retry error={error}");
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(error) => {
                return Err(format!(
                    "cutover record not written ({error}); the category stays frozen and verified: \
                     retry POST /admin/routes/abort?category={category} to roll back"
                ))
            }
        }
    }
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
    let dropped = propose_until_committed(
        endpoints,
        from_group,
        &format!("/migration/drop?category={category}&epoch={epoch}"),
        wait,
    );
    if let Err(error) = &dropped {
        // The source stays sealed (a late command is rejected there); its
        // leftover state only costs memory. Fenced by the epoch, a later
        // drop cannot touch a newer install.
        log_warn!("migration", category; "event=drop_failed error={error}");
    }
    Ok(format!(
        "{{\"migrated\":true,\"category\":{category},\"from\":{from_group},\"to\":{to_group},\"epoch\":{epoch},\"seal_index\":{seal_index},\"install_index\":{install_index},\"fingerprint\":{fingerprint},\"route_version\":{version},\"source_dropped\":{},\"elapsed_ms\":{}}}",
        dropped.is_ok(),
        started.elapsed().as_millis()
    ))
}

/// Undo a migration's attempted steps, each until it commits: drop what
/// `epoch` installed on the target, unseal what it sealed on the source
/// (the groups ignore either if `epoch` never applied there, or a newer
/// migration has since), then route the category back to the source —
/// only if this migration's freeze is still the latest record.
#[allow(clippy::too_many_arguments)]
fn roll_back(
    kafka: &KafkaIngress,
    category: u32,
    from_group: usize,
    to_group: usize,
    epoch: u64,
    freeze_offset: i64,
    seal_attempted: bool,
    install_attempted: bool,
) -> Result<(), String> {
    let endpoints = &kafka.migration_endpoints;
    let wait = Duration::from_secs(env_number("TC_MIGRATION_STEP_TIMEOUT_SECS", 60u64));
    if install_attempted {
        propose_until_committed(
            endpoints,
            to_group,
            &format!("/migration/drop?category={category}&epoch={epoch}"),
            wait,
        )?;
    }
    if seal_attempted {
        propose_until_committed(
            endpoints,
            from_group,
            &format!("/migration/unseal?category={category}&epoch={epoch}"),
            wait,
        )?;
    }
    match route_log::latest_after(category, freeze_offset, Duration::from_secs(10))? {
        Some((offset, _)) if offset == freeze_offset => {
            route_log::publish(kafka, category, route_log::RouteRecord::Pin(from_group))?;
        }
        _ => {} // someone else's record is newer: leave the route to them
    }
    Ok(())
}

/// `POST /admin/routes/abort?category=C`: roll back a migration whose
/// orchestrator stopped (crash, restart, failed cutover write). Works from
/// the route log alone, on any instance; a no-op unless the category's
/// latest record is a freeze.
pub(crate) fn abort_migration(kafka: &KafkaIngress, category: u32) -> Result<String, String> {
    // What this instance has read (the follower is near-live).
    let latest = route_log::latest_after(category, -1, Duration::from_secs(10))?;
    let Some((offset, route_log::RouteRecord::Freeze { to, from })) = latest else {
        kafka.route_control.abort(category);
        return Ok(format!("{{\"aborted\":false,\"reason\":\"no unfinished migration\",\"latest\":\"{latest:?}\"}}"));
    };
    let epoch = offset as u64 + 1;
    log_warn!("migration", category; "event=abort from={from} to={to} epoch={epoch}");
    roll_back(kafka, category, from, to, epoch, offset, true, true)?;
    kafka.route_control.abort(category);
    Ok(format!("{{\"aborted\":true,\"category\":{category},\"from\":{from},\"to\":{to},\"epoch\":{epoch}}}"))
}

/// Stable, operator-facing inventory of migrations that still hold their
/// category frozen. The epoch is the durable freeze offset plus one, matching
/// every idempotent migration command sent to the Raft groups.
pub(crate) fn unfinished_migrations_json() -> String {
    unfinished_migrations_json_from(route_log::frozen_categories())
}

pub(crate) fn unfinished_migrations_json_from(
    frozen: Vec<(u32, i64, route_log::RouteRecord)>,
) -> String {
    let mut body = format!("{{\"count\":{},\"migrations\":[", frozen.len());
    for (position, (category, offset, record)) in frozen.into_iter().enumerate() {
        if position > 0 {
            body.push(',');
        }
        let route_log::RouteRecord::Freeze { to, from } = record else {
            continue;
        };
        body.push_str(&format!(
            "{{\"category\":{category},\"from\":{from},\"to\":{to},\"epoch\":{},\"freeze_offset\":{offset}}}",
            offset + 1
        ));
    }
    body.push_str("]}");
    body
}
