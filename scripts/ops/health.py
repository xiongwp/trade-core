#!/usr/bin/env python3
"""Whole-system health check for a trade-core deployment.

    python3 scripts/ops/health.py                    # the local docker stack
    python3 scripts/ops/health.py out/health.json    # a deployment (gen_config.py writes it)
    add --quiet to print only problems

health.json:
{
  "raft_groups":  {"0": ["mx01:9102", "mx02:9102", "mx03:9102"], ...},  # every replica's metrics
  "order_api":    ["in01:9200", "pj01:9200", ...],
  "user_stream":  ["ps01:9300", ...],
  "market_data":  ["md01:8080", ...],
  "token":        "...",            # order-api admin token (or env TC_ORDER_API_TOKEN)
  "limits": {"apply_spread": 10000, "outbox_pending": 50000, "consumer_lag": 500000,
             "snapshot_bytes": 536870912}
}

Prints one line per finding with what to do about it. Exit code: 0 all
healthy, 1 warnings, 2 failures (e.g. a group without quorum or leader).
"""
import json
import os
import sys
import time
import urllib.request

LOCAL = {
    "raft_groups": {str(g): [f"localhost:{9200 + g * 10 + n}" for n in range(1, 6)] for g in range(4)},
    "order_api": ["localhost:9200"],
    "user_stream": ["localhost:9300", "localhost:9310"],
    "market_data": ["localhost:8081"],
}
LIMITS = {"apply_spread": 10_000, "outbox_pending": 50_000, "consumer_lag": 500_000,
          "snapshot_bytes": 512 * 2**20}

status = 0
findings = []


def report(level, where, message, action=""):
    global status
    status = max(status, {"OK": 0, "WARN": 1, "FAIL": 2}[level])
    findings.append((level, where, message, action))


def get(addr, path="/metrics", token=None, timeout=3):
    url = addr if addr.startswith("http") else f"http://{addr}{path}"
    request = urllib.request.Request(url)
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode()


def metrics(addr):
    values = {}
    for line in get(addr).splitlines():
        if line and not line.startswith("#"):
            name, _, value = line.rpartition(" ")
            name = name.split("{", 1)[0]
            try:
                values[name] = values.get(name, 0.0) + float(value)
            except ValueError:
                pass
    return values


def check_group(group, replicas, limits):
    seen = {}
    for addr in replicas:
        try:
            seen[addr] = metrics(addr)
        except OSError as error:
            report("WARN", f"group {group} {addr}", f"unreachable ({error})",
                   "restart that replica; if its data is lost: scripts/ops/rebuild-replica.sh")
    majority = len(replicas) // 2 + 1
    if len(seen) < majority:
        report("FAIL", f"group {group}", f"only {len(seen)}/{len(replicas)} replicas reachable: no quorum",
               "bring replicas back (runbook §2); the group's categories are refused until then")
        return
    leaders = [a for a, m in seen.items() if m.get("tc_raft_role") == 2]
    if not leaders:
        report("FAIL", f"group {group}", "no leader", "check peer connectivity and exit-78 config mismatches in the logs")
        return
    if len(leaders) > 1:
        terms = {a: seen[a].get("tc_raft_term") for a in leaders}
        report("WARN", f"group {group}", f"several replicas report leadership {terms}", "an old leader not yet stepped down; recheck in a few seconds")
    leader = max(leaders, key=lambda a: seen[a].get("tc_raft_term", 0))
    lm = seen[leader]
    heartbeat = lm.get("tc_raft_runtime_heartbeat_unix_ms")
    heartbeat_age = time.time() * 1000 - heartbeat if heartbeat else 0
    if heartbeat and heartbeat_age > 5_000:
        report("FAIL", f"group {group} leader {leader}",
               f"Raft runtime heartbeat stale by {heartbeat_age / 1000:,.1f}s",
               "capture thread stacks; restart only this group process")
    applied = lm.get("tc_raft_applied_index", 0)
    for addr, m in seen.items():
        behind = applied - m.get("tc_raft_applied_index", 0)
        if behind > limits["apply_spread"]:
            report("WARN", f"group {group} {addr}", f"{behind:,.0f} entries behind the leader", "catching up; if it does not shrink, check its disk and logs")
        if m.get("tc_asset_wal_errors", 0) > 0:
            report("FAIL", f"group {group} {addr}", "WAL write errors (fail-stop)", "disk full or failing: runbook §5")
        if m.get("tc_storage_healthy", 1) != 1:
            report("FAIL", f"group {group} {addr}", "local WAL or snapshot storage unhealthy",
                   "isolate this replica, check free space and media errors, then restore or rebuild it")
    if lm.get("tc_ready", 0) != 1:
        report("WARN", f"group {group} leader {leader}", "not ready (apply lag, outbox backlog or publisher unhealthy)", "see the outbox lines below and Kafka health")
    for addr, m in seen.items():
        size = m.get("tc_snapshot_bytes", 0)
        if size > limits["snapshot_bytes"]:
            report("WARN", f"group {group} {addr}", f"engine snapshot {size / 2**20:,.0f} MiB",
                   "a follower behind the compaction point needs the whole snapshot within "
                   "TC_RAFT_MAX_MESSAGE_BYTES (default 1 GiB): migrate categories away or raise the limit")
    pending = lm.get("tc_execution_outbox_pending", 0)
    if pending > limits["outbox_pending"]:
        report("WARN", f"group {group} leader {leader}", f"{pending:,.0f} execution events not on Kafka yet", "Kafka slow or down: runbook §4")
    if lm.get("tc_execution_outbox_publish_healthy", 1) != 1:
        report("WARN", f"group {group} leader {leader}", "execution publisher unhealthy", "check brokers; it recovers by itself once Kafka answers")
    report("OK", f"group {group}", f"leader {leader} term {lm.get('tc_raft_term', 0):.0f} applied {applied:,.0f}, {len(seen)}/{len(replicas)} replicas up")


def check_order_api(addrs, token, limits):
    unfinished_checked = False
    for addr in addrs:
        try:
            m = metrics(addr)
        except OSError as error:
            report("FAIL", f"order-api {addr}", f"unreachable ({error})", "restart it; it resumes from committed Kafka offsets")
            continue
        for gauge, what in (("tc_order_match_consumer_lag", "matching"), ("tc_order_mysql_consumer_lag", "MySQL projection")):
            lag = m.get(gauge, 0)
            if lag > limits["consumer_lag"]:
                report("WARN", f"order-api {addr}", f"{what} lag {lag:,.0f}", "overloaded or a stage is down; rates.py shows whether it drains")
        for counter, path in (("tc_order_dlq_total", "TC_ORDER_DLQ_PATH"),
                              ("tc_order_command_dlq_total", "TC_ORDER_COMMAND_DLQ_PATH")):
            if m.get(counter, 0) > 0:
                report("WARN", f"order-api {addr}", f"{m[counter]:.0f} dead-lettered messages ({counter})",
                       f"dlq-tool list <{path}>; fix the cause, then dlq-tool replay (runbook §3)")
        if m.get("tc_order_bp_quorum_unhealthy_groups", 0) > 0:
            report("WARN", f"order-api {addr}", f"{m['tc_order_bp_quorum_unhealthy_groups']:.0f} Raft group(s) failing forwards", "see the group lines above")
        if not unfinished_checked and token:
            try:
                rows = json.loads(get(addr, "/admin/routes/unfinished", token))["migrations"]
                unfinished_checked = True
                for row in rows:
                    report("WARN", f"category {row['category']}",
                           f"migration {row['from']}->{row['to']} unfinished (epoch {row['epoch']}): frozen",
                           f"scripts/ops/abort-migrations.sh (or POST /admin/routes/abort?category={row['category']})")
            except (OSError, ValueError, KeyError) as error:
                report("WARN", f"order-api {addr}", f"cannot list unfinished migrations ({error})")
        report("OK", f"order-api {addr}", f"published {m.get('tc_order_published_commands', 0):,.0f}, "
               f"match lag {m.get('tc_order_match_consumer_lag', 0):,.0f}, mysql lag {m.get('tc_order_mysql_consumer_lag', 0):,.0f}")


def check_up(kind, addrs):
    for addr in addrs:
        path = "/metrics" if kind == "user-stream" else "/readyz"
        try:
            get(addr, path)
            report("OK", f"{kind} {addr}", "ready")
        except OSError as error:
            report("FAIL", f"{kind} {addr}", f"not ready ({error})",
                   "check Kafka replay and local snapshot storage before routing traffic")


def main():
    paths = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    config = json.load(open(paths[0])) if paths else LOCAL
    limits = {**LIMITS, **config.get("limits", {})}
    token = config.get("token") or os.environ.get("TC_ORDER_API_TOKEN", "local-order-api-token")
    for group, replicas in sorted(config.get("raft_groups", {}).items(), key=lambda kv: int(kv[0])):
        check_group(group, replicas, limits)
    check_order_api(config.get("order_api", []), token, limits)
    check_up("user-stream", config.get("user_stream", []))
    check_up("market-data", config.get("market_data", []))
    quiet = "--quiet" in sys.argv
    for level, where, message, action in findings:
        if quiet and level == "OK":
            continue
        print(f"{level:4} {where}: {message}" + (f"\n       -> {action}" if action else ""))
    print({0: "HEALTHY", 1: "WARNINGS", 2: "FAILURES"}[status])
    sys.exit(status)


if __name__ == "__main__":
    main()
