//! HTTP connection handler and routing for every order-api endpoint.

use super::*;

/// At most one rejection log line per 5 s: under overload every request is
/// rejected, and logging each one would make the overload worse. Monotonic,
/// so a wall-clock step back cannot silence it.
fn should_log_ingress_rejection() -> bool {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    static NEXT_LOG_SEC: AtomicU64 = AtomicU64::new(0);
    let now = START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs();
    let next = NEXT_LOG_SEC.load(AtomicOrdering::Relaxed);
    now >= next
        && NEXT_LOG_SEC
            .compare_exchange(next, now + 5, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
            .is_ok()
}

pub(crate) fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|part| {
        let (k, v) = part.split_once('=')?;
        (k == key).then_some(v)
    })
}

pub(crate) fn required<T: std::str::FromStr>(query: &str, key: &str) -> Result<T, String> {
    query_param(query, key)
        .ok_or_else(|| format!("missing {key}"))?
        .parse()
        .map_err(|_| format!("invalid {key}"))
}

pub(crate) fn respond(stream: &mut TcpStream, status: &str, body: &str, keep_alive: bool) {
    respond_content(stream, status, "application/json", body, keep_alive);
}

pub(crate) fn respond_content(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
    keep_alive: bool,
) {
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let _ = stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{body}", body.len()).as_bytes());
}

/// Longest request or header line, and most headers, of one request.
const MAX_HEAD_LINE: u64 = 16 * 1024;
const MAX_HEADERS: usize = 100;

/// `read_line`, bounded: a line without a newline within the limit is an
/// error, not a buffer that grows until allocation fails.
fn read_head_line(reader: &mut BufReader<TcpStream>, line: &mut String) -> io::Result<usize> {
    let read = reader.by_ref().take(MAX_HEAD_LINE).read_line(line)?;
    if read > 0 && !line.ends_with('\n') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "request line too long"));
    }
    Ok(read)
}

pub(crate) fn handle(
    stream: TcpStream,
    category_size: u32,
    metrics: Arc<OrderPipelineMetrics>,
    kafka: Arc<KafkaIngress>,
    token: &str,
    ingress_enabled: bool,
    query_store: Option<OrderStore>,
) {
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream);
    loop {
        let mut first = String::new();
        match read_head_line(&mut reader, &mut first) {
            Ok(0) | Err(_) => return,
            Ok(_) if first.trim().is_empty() => continue,
            Ok(_) => {}
        }
        let mut parts = first.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let uri = parts.next().unwrap_or("/").to_string();
        let version = parts.next().unwrap_or("HTTP/1.0");
        let mut authorized = false;
        let mut keep_alive = version == "HTTP/1.1";
        let mut content_length = 0usize;
        let mut upgrade_websocket = false;
        let mut websocket_key: Option<String> = None;
        let mut headers = 0;
        loop {
            let mut header = String::new();
            headers += 1;
            if headers > MAX_HEADERS {
                return;
            }
            match read_head_line(&mut reader, &mut header) {
                Ok(0) | Err(_) => return,
                Ok(_) if header.trim().is_empty() => break,
                Ok(_) => {}
            }
            if let Some((key, value)) = header.split_once(':') {
                let value = value.trim();
                authorized |=
                    key.eq_ignore_ascii_case("authorization") && value == format!("Bearer {token}");
                if key.eq_ignore_ascii_case("connection") {
                    keep_alive = value.eq_ignore_ascii_case("keep-alive");
                }
                if key.eq_ignore_ascii_case("content-length") {
                    content_length = value.parse().unwrap_or(usize::MAX);
                }
                if key.eq_ignore_ascii_case("upgrade") {
                    upgrade_websocket = value.eq_ignore_ascii_case("websocket");
                }
                if key.eq_ignore_ascii_case("sec-websocket-key") {
                    websocket_key = Some(value.to_string());
                }
            }
        }
        if content_length > 1 << 20 {
            respond(
                reader.get_mut(),
                "413 Payload Too Large",
                "{\"error\":\"payload too large\"}",
                false,
            );
            return;
        }
        let body = if content_length > 0 {
            let mut body = vec![0; content_length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
            body
        } else {
            Vec::new()
        };
        let (path, query) = uri.split_once('?').unwrap_or((&uri, ""));
        if method == "GET" && path == "/healthz" {
            respond(
                reader.get_mut(),
                "200 OK",
                "{\"status\":\"alive\"}",
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if method == "GET" && path == "/readyz" {
            let backlog = metrics.backlog();
            let unhealthy_groups = kafka.backpressure.unhealthy_groups();
            let lag_monitors_ready = !ingress_enabled || metrics.lag_monitors_ready();
            // Process-local only. Backlog and Raft health are the same on
            // every instance: gating on them would pull all instances out of
            // a load balancer at once (and the admin abort endpoint with
            // them), while 429 backpressure already sheds that load per
            // category. They are reported, not enforced.
            let ready = !SHUTDOWN.load(AtomicOrdering::Acquire) && lag_monitors_ready;
            let status = if ready {
                "200 OK"
            } else {
                "503 Service Unavailable"
            };
            respond(
                reader.get_mut(),
                status,
                &format!(
                    "{{\"ready\":{ready},\"lag_monitors_ready\":{lag_monitors_ready},\"backlog\":{backlog},\"backlog_limit\":{},\"unhealthy_raft_groups\":{unhealthy_groups}}}",
                    kafka.max_pipeline_backlog,
                ),
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if method == "GET" && path == "/metrics" {
            respond_content(
                reader.get_mut(),
                "200 OK",
                "text/plain; version=0.0.4",
                &(metrics.render() + &kafka.backpressure.render()),
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if !authorized {
            respond(
                reader.get_mut(),
                "401 Unauthorized",
                "{\"error\":\"unauthorized\"}",
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if let Some(request) = parse_order_query(&method, path, query) {
            let (status, body) = order_query_response(query_store.as_ref(), request);
            respond(reader.get_mut(), status, &body, keep_alive);
            if !keep_alive {
                return;
            }
            continue;
        }
        if method == "GET" && path == "/admin/routes/unfinished" {
            respond(
                reader.get_mut(),
                "200 OK",
                &unfinished_migrations_json(),
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }
        if method == "POST" && path.starts_with("/admin/routes/") {
            let category = required::<u32>(query, "category");
            let result: Result<String, String> = category.and_then(|category| match path {
                "/admin/routes/migrate" => required::<usize>(query, "to")
                    .and_then(|to| migrate_category(&kafka, category, to)),
                "/admin/routes/abort" => abort_migration(&kafka, category),
                _ => Err("unknown route migration action".into()),
            });
            match result {
                Ok(body) => respond(reader.get_mut(), "200 OK", &body, keep_alive),
                Err(error) => respond(
                    reader.get_mut(),
                    "409 Conflict",
                    &format!("{{\"error\":\"{error}\"}}"),
                    keep_alive,
                ),
            }
            if !keep_alive {
                return;
            }
            continue;
        }
        if method == "GET" && path == "/ws/orders" {
            let key = websocket_key.take().filter(|_| upgrade_websocket);
            let (Some(key), true) = (key, ingress_enabled) else {
                let (status, error) = if ingress_enabled {
                    ("400 Bad Request", "websocket upgrade required")
                } else {
                    (
                        "503 Service Unavailable",
                        "ingress disabled for this worker role",
                    )
                };
                respond(
                    reader.get_mut(),
                    status,
                    &format!("{{\"error\":\"{error}\"}}"),
                    false,
                );
                return;
            };
            let Ok(mut writer) = reader.get_ref().try_clone() else {
                return;
            };
            if writer
                .write_all(trade_core::websocket::handshake_response(&key).as_bytes())
                .is_err()
            {
                return;
            }
            // `reader` may already hold frames the client pipelined behind
            // the handshake, so it stays the read side.
            let result =
                ws_order_session(
                    &mut reader,
                    &mut writer,
                    ws_max_batch(),
                    |body| match decode_batch(body, category_size) {
                        Ok(records) => match kafka.publish_batch(&records) {
                            Ok(failed) => {
                                Ok(batch_accepted_json(records.len() - failed.len(), &failed))
                            }
                            Err(error) => Err((ingress_error_status(&error), error)),
                        },
                        Err(error) => Err(("400 Bad Request", error)),
                    },
                );
            if let Err(error) = result {
                log_warn!("order-api", "event=ws_session_closed error={error}");
            }
            return;
        }
        if !ingress_enabled && method == "POST" {
            respond(
                reader.get_mut(),
                "503 Service Unavailable",
                "{\"error\":\"HTTP ingress disabled for this worker role\"}",
                keep_alive,
            );
            if !keep_alive {
                return;
            }
            continue;
        }

        if method == "POST" && path == "/commands/batch" {
            match decode_batch(&body, category_size) {
                Ok(records) => match kafka.publish_batch(&records) {
                    Ok(failed) => {
                        if !failed.is_empty() {
                            log_warn!(
                                "order-api",
                                "event=batch_partially_published count={} failed={} error={}",
                                records.len(),
                                failed.len(),
                                failed[0].error
                            );
                        }
                        respond(
                            reader.get_mut(),
                            "202 Accepted",
                            &format!(
                                "{{{}}}",
                                batch_accepted_json(records.len() - failed.len(), &failed)
                            ),
                            keep_alive,
                        )
                    }
                    Err(error) => {
                        let status = ingress_error_status(&error);
                        // Rejected ingress is an abnormal path: keep it
                        // correlatable (success stays silent — the metrics
                        // carry it), but rate-limited.
                        if should_log_ingress_rejection() {
                            log_warn!(
                                "order-api",
                                "event=batch_rejected status=\"{status}\" count={} error={error}",
                                records.len()
                            );
                        }
                        respond(
                            reader.get_mut(),
                            status,
                            &format!("{{\"error\":\"{error}\"}}"),
                            keep_alive,
                        )
                    }
                },
                Err(error) => {
                    if should_log_ingress_rejection() {
                        log_warn!(
                            "order-api",
                            "event=batch_rejected status=\"400 Bad Request\" error={error}"
                        );
                    }
                    respond(
                        reader.get_mut(),
                        "400 Bad Request",
                        &format!("{{\"error\":\"{error}\"}}"),
                        keep_alive,
                    )
                }
            }
            if !keep_alive {
                return;
            }
            continue;
        }
        // (order_id, category) of the request, for correlating a rejection.
        let mut req_ctx: Option<(u64, u32)> = None;
        let persisted = match (method.as_str(), path) {
            ("POST", "/orders") => order_command_from_query(query)
                .map_err(|error| format!("{INVALID_REQUEST}{error}"))
                .and_then(|command| {
                    let mut frame = [0u8; wire::MSG_LEN];
                    wire::encode_command(&command, &mut frame);
                    let category = sharding::asset_category(command.instrument(), category_size);
                    let (trade_core::Command::New(order)
                    | trade_core::Command::NewConditional(order, _)) = &command
                    else {
                        unreachable!("order_command_from_query yields orders");
                    };
                    req_ctx = Some((order.id.0, category));
                    kafka.publish(category, order.user, &frame)
                }),
            ("POST", "/cancels") => cancel_from_query(query)
                .map_err(|error| format!("{INVALID_REQUEST}{error}"))
                .and_then(|(i, o, c, u)| {
                    let mut frame = [0u8; wire::MSG_LEN];
                    wire::encode_cancel(i, o, c, &mut frame);
                    let category = sharding::asset_category(i, category_size);
                    req_ctx = Some((o.0, category));
                    kafka.publish(category, u, &frame)
                }),
            _ => {
                respond(
                    reader.get_mut(),
                    "404 Not Found",
                    "{\"error\":\"not found\"}",
                    keep_alive,
                );
                if !keep_alive {
                    return;
                }
                continue;
            }
        };
        match persisted {
            Ok(seq) => respond(
                reader.get_mut(),
                "202 Accepted",
                &format!(
                    "{{\"accepted\":true,\"status\":\"PENDING\",\"category_id\":{},\"category_seq\":{}}}",
                    seq.category_id, seq.category_seq
                ),
                keep_alive,
            ),
            Err(error) => {
                let status = ingress_error_status(&error);
                if should_log_ingress_rejection() {
                    match req_ctx {
                        Some((order_id, category)) => log_warn!(
                            "order-api",
                            "event=order_rejected status=\"{status}\" order_id={order_id} category={category} error={error}"
                        ),
                        None => log_warn!(
                            "order-api",
                            "event=order_rejected status=\"{status}\" error={error}"
                        ),
                    }
                }
                respond(
                    reader.get_mut(),
                    status,
                    &format!("{{\"error\":\"{error}\"}}"),
                    keep_alive,
                )
            }
        }
        if !keep_alive {
            return;
        }
    }
}
