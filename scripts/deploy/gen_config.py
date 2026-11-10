#!/usr/bin/env python3
"""Generate per-host environment files for a large trade-core deployment.

Hand-writing 400 MySQL URLs, 64 Raft groups' replica placements and every
group's ports is where deployments go wrong; this derives them all from one
inventory. See docs/deployment-2m-tps.md for what each value means.

    python3 scripts/deploy/gen_config.py inventory.json out/

inventory.json:
{
  "mysql_hosts":      ["db01", "db02", ...],      # primaries, in order
  "dbs_per_mysql":    8,                           # -> TC_DB_COUNT = hosts x this
  "mysql_password_env": "MYSQL_PASSWORD",          # referenced, never inlined
  "raft_hosts":       ["mx01", ...],               # matching machines
  "raft_failure_domains": {"mx01":"rack-a", ...}, # required for rack-aware placement
  "raft_groups":      64,
  "replicas":         3,
  "projection_hosts": ["pj01", ...],               # order-api DB workers
  "ingress_hosts":    ["in01", ...],               # order-api intake
  "push_hosts":       ["ps01", ...],               # user-stream (optional)
  "market_data_hosts":["md01", ...],               # market API/read models (optional)
  "route_version":    1,                           # bump with any change below
  "virtual_db_count": 400000,                      # fixed for the life of the data
  "kafka_brokers":    ["kf01:9092", ...]
}

Writes out/<host>.env for every host and out/topics.sh with the Kafka topic
creation commands. Each push host gets two files: <host>-router.env and
<host>-pusher.env (user-stream router and pusher shard, one process each).
"""
import json
import os
import sys


def raft_port(group, node):
    """Same layout as src/bin/raft_multi_node.rs `raft_port`."""
    assert 1 <= node <= 9, "node ids are 1-9"
    base = 7000 if group < 10 else 40000
    port = base + 10 * group + node
    assert port <= 65535, f"group {group} does not fit the port layout"
    return port


def service_ports(group):
    """(order, market-data, metrics), as raft_multi_node.rs `service_ports`."""
    if group < 10:
        return 9001 + 10 * group, 9101 + 10 * group, 9102 + 10 * group
    base = 20000 + 3 * group
    return base, base + 1, base + 2


def main(inventory_path, out_dir):
    inv = json.load(open(inventory_path))
    os.makedirs(out_dir, exist_ok=True)
    mysql_hosts = inv["mysql_hosts"]
    dbs_per = inv["dbs_per_mysql"]
    db_count = len(mysql_hosts) * dbs_per
    groups = inv["raft_groups"]
    replicas = inv["replicas"]
    raft_hosts = inv["raft_hosts"]
    assert 1 <= replicas <= 9 and replicas <= len(raft_hosts)
    brokers = ",".join(inv["kafka_brokers"])
    pw = "${" + inv.get("mysql_password_env", "MYSQL_PASSWORD") + "}"

    # The order -> DB/table mapping depends on these; changing any of them
    # (or the DB count) moves orders between databases, so it needs a new
    # route version. Keep the virtual space fixed across expansions: set it
    # once in the inventory (>= 1000 x the largest DB count planned).
    route_version = inv.get("route_version", 1)
    virtual_dbs = inv.get("virtual_db_count", db_count * 1000)
    assert virtual_dbs >= db_count * 1000, "virtual_db_count must be >= 1000 x the DB count"
    state_path = os.path.join(out_dir, "routing.json")
    if os.path.exists(state_path):
        previous = json.load(open(state_path))
        changed = (previous["db_count"], previous["virtual_db_count"]) != (db_count, virtual_dbs)
        assert not changed or previous["route_version"] != route_version, (
            f"routing changed ({previous} -> db_count={db_count}, virtual_db_count={virtual_dbs}) "
            "without a new route_version in the inventory"
        )
    json.dump({"db_count": db_count, "virtual_db_count": virtual_dbs, "route_version": route_version},
              open(state_path, "w"))

    # Globally identical routing/determinism parameters.
    common = {
        "TC_DB_COUNT": db_count,
        "TC_TABLES_PER_DB": 100,
        # >= 1000 virtual DBs per physical DB keeps the hottest DB within ~10%
        # of the average (1000 total for 400 DBs left some DBs empty and one
        # at 2.8x the average).
        "TC_VIRTUAL_DB_COUNT": virtual_dbs,
        "TC_VIRTUAL_TABLE_COUNT": virtual_dbs * 10,
        "TC_SHARD_ROUTE_VERSION": route_version,
        "TC_ORDER_CATEGORY_SIZE": 1,
        "TC_EXECUTION_KAFKA_PARTITIONS": db_count,
        "TC_ORDER_DB_KAFKA_PARTITIONS": db_count,
        "TC_ORDER_KAFKA_TOPIC_COUNT": groups,
        "TC_ORDER_KAFKA_PARTITIONS_PER_TOPIC": 16,
        "TC_ORDER_KAFKA_BROKERS": brokers,
        "TC_EXECUTION_KAFKA_BROKERS": brokers,
        "TC_MARKET_KAFKA_BROKERS": brokers,
    }

    # Raft placement. With failure domains, replicas rotate across distinct
    # racks/AZs and each domain's assignments round-robin over its hosts. The
    # generator refuses a topology that cannot place a group's replicas in
    # distinct domains. Without the optional map it retains the local/dev
    # consecutive-host behavior.
    placements = {host: [] for host in raft_hosts}
    members = {}
    failure_domains = inv.get("raft_failure_domains")
    if failure_domains:
        missing = [host for host in raft_hosts if host not in failure_domains]
        unknown = [host for host in failure_domains if host not in raft_hosts]
        assert not missing and not unknown, (
            f"raft_failure_domains must name exactly raft_hosts; missing={missing}, unknown={unknown}"
        )
        domains = []
        hosts_by_domain = {}
        for host in raft_hosts:
            domain = failure_domains[host]
            assert domain, f"empty failure domain for {host}"
            if domain not in hosts_by_domain:
                domains.append(domain)
                hosts_by_domain[domain] = []
            hosts_by_domain[domain].append(host)
        assert len(domains) >= replicas, (
            f"{replicas} replicas need at least {replicas} failure domains; got {len(domains)}"
        )
        domain_cursor = {domain: 0 for domain in domains}
        for g in range(groups):
            selected = [domains[(g + r) % len(domains)] for r in range(replicas)]
            hosts = []
            for domain in selected:
                candidates = hosts_by_domain[domain]
                cursor = domain_cursor[domain]
                hosts.append(candidates[cursor % len(candidates)])
                domain_cursor[domain] = cursor + 1
            assert len(set(hosts)) == replicas
            members[g] = [(node + 1, host) for node, host in enumerate(hosts)]
    else:
        for g in range(groups):
            hosts = [raft_hosts[(g + r) % len(raft_hosts)] for r in range(replicas)]
            members[g] = [(node + 1, host) for node, host in enumerate(hosts)]
    # Existing groups must keep their members: a node's data directory holds
    # one group's log for one (node id, peer set), so moving a group to other
    # hosts or ids by regenerating (e.g. after adding raft_failure_domains)
    # would boot those directories into the wrong membership. Placement is
    # recorded on first generation and may only grow by new groups.
    placement_path = os.path.join(out_dir, "placement.json")
    current = {str(g): [[node, host] for node, host in nodes] for g, nodes in members.items()}
    if os.path.exists(placement_path):
        previous = json.load(open(placement_path))
        moved = sorted(
            (int(g) for g in previous if g in current and previous[g] != current[g]), key=int
        )
        assert not moved or inv.get("allow_placement_change"), (
            f"raft groups {moved} would change members ({placement_path}); "
            "migrate them deliberately and set allow_placement_change in the inventory"
        )
    json.dump(current, open(placement_path, "w"), indent=1)
    for g, nodes in members.items():
        peers = ",".join(f"{node}@{host}:{raft_port(g, node)}" for node, host in nodes)
        for node, host in nodes:
            placements[host].append(f"{g}|{node}|{peers}")
    matchers = ";".join(
        ",".join(
            f"{host}:{service_ports(g)[0]}@{host}:{service_ports(g)[2]}" for _, host in members[g]
        )
        for g in range(groups)
    )
    migration = ",".join(
        f"{g}=" + "|".join(f"{host}:{service_ports(g)[2]}" for _, host in members[g])
        for g in range(groups)
    )
    shard_urls = ",".join(
        f"mysql://trade:{pw}@{mysql_hosts[db // dbs_per]}:3306/mysql" for db in range(db_count)
    )

    def write(host, env):
        with open(os.path.join(out_dir, f"{host}.env"), "w") as f:
            for key, value in {**common, **env}.items():
                f.write(f"{key}={value}\n")

    # Recommended per-role values (docs/deployment-2m-tps.md sections 6-7).
    raft_defaults = {
        "TC_DEDUP_WINDOW_IDS": 2_000_000,
        "TC_RAFT_DEDUP_MAX_IDS": 2_000_000,
        "TC_SNAPSHOT_EVERY_SECS": 30,
        "TC_RAFT_COMPACT_APPLIED_THRESHOLD": 1000,
        "TC_RAFT_CONFIG_CHECK": 1,
        "TC_RAFT_CONFIG_ANNOUNCE_SECS": 30,
        "TC_RAFT_CONFIG_ADOPT": 0,
        "TC_RAFT_READY_MAX_APPLY_LAG": 32,
        "TC_RAFT_READY_MAX_OUTBOX_PENDING": 50_000,
        "TC_RAFT_MIN_FREE_BYTES": 10 * 1024**3,
        "TC_RAFT_TRANSPORT_QUEUE": 8192,
        "TC_MATCH_POOL_PER_ASSET": 4096,
        "TC_EXECUTION_OUTBOX_SYNC_EVERY": 1,
        "TC_EXECUTION_PUBLISH_BATCH": 4096,
        "TC_EXECUTION_KAFKA_LINGER_MS": 2,
        "TC_EXECUTION_KAFKA_BATCH_MESSAGES": 10_000,
        "TC_EXECUTION_KAFKA_COMPRESSION": "lz4",
        "TC_EXECUTION_KAFKA_DELIVERY_TIMEOUT_MS": 15000,
        # One cursor partition per group (topics.sh creates exactly that
        # many); the node default (16) would map groups onto partitions that
        # do not exist or leave created ones unused.
        "TC_EXECUTION_CURSOR_PARTITIONS": groups,
        "TC_MARKET_KAFKA_TOPIC": "trade-market-events-v1",
        "TC_MARKET_KAFKA_PARTITIONS": groups,
        "TC_SUPERVISOR_LIVENESS_GRACE_SECS": 300,
        "TC_SUPERVISOR_LIVENESS_FAILURES": 6,
        "TC_SUPERVISOR_START_CONCURRENCY": 16,
        "TC_SUPERVISOR_STARTUP_JITTER_MS": 2000,
        "TC_SUPERVISOR_STARTUP_SLOT_TIMEOUT_SECS": 300,
        "TC_SUPERVISOR_MAX_GROUPS": 1024,
        "TC_RAFT_CHILD_NOFILE": 16384,
        "TC_RAFT_CHILD_MAX_ADDRESS_SPACE_MB": 0,
    }
    partitions = groups * 16
    per_partition = max(1, 2_000_000 // partitions)  # design commands/s/partition
    ingress_defaults = {
        "TC_RAFT_PACK_SIZE": 1000,
        "TC_RAFT_FORWARD_QUEUE": 64,
        "TC_RAFT_MATCHER_IO_TIMEOUT_MS": 5000,
        "TC_WS_MAX_BATCH": 5000,
        "TC_ORDER_KAFKA_LINGER_MS": 1,
        "TC_ORDER_KAFKA_BATCH_MESSAGES": 10_000,
        "TC_ORDER_KAFKA_COMPRESSION": "lz4",
        "TC_ORDER_MAX_PIPELINE_BACKLOG": 4_000_000,
        # 2 s / 5 s / 15 s of the design load of one partition.
        "TC_ORDER_BP_SOFT": per_partition * 2,
        "TC_ORDER_BP_HARD": per_partition * 5,
        "TC_ORDER_BP_EMERGENCY": per_partition * 15,
        "TC_KAFKA_OFFSET_COMMIT_ASYNC": "true",
        "TC_ROUTE_CONTROL_TOPIC": "trade-route-control-v1",
        "TC_ORDER_QUERY_ENABLED": "false",
        "TC_ORDER_MAX_CONNECTIONS": 4096,
        "TC_ORDER_CONNECTION_IDLE_TIMEOUT_MS": 75000,
        "TC_ORDER_WRITE_TIMEOUT_MS": 30000,
        "TC_ORDER_HTTP_THREAD_STACK_KB": 512,
    }
    projection_defaults = {
        "TC_EXECUTION_MYSQL_BATCH_PER_DB": 500,
        "TC_EXECUTION_MYSQL_BATCH_LINGER_MS": 10,
        "TC_MYSQL_SHARD_QUEUE": 64,
        "TC_MYSQL_SHARD_COALESCE_JOBS": 16,
        "TC_ORDER_DRAIN_HANDOFF": "true",
        "TC_ORDER_DEDUPE_RETENTION_HOURS": 24,
        "TC_ORDER_COMMAND_LOG": 0,
        "TC_ORDER_PERSIST_MAX_RETRIES": 10,
        "TC_ORDER_DLQ_PATH": "/data/order-execution-dlq.wal",
        "TC_ORDER_COMMAND_DLQ_PATH": "/data/order-command-dlq.wal",
        "TC_ORDER_QUERY_ENABLED": "true",
        "TC_ORDER_QUERY_DB_PARALLELISM": 4,
        "TC_ORDER_QUERY_MAX_DB_WORKERS": 32,
    }

    # Raft ports of groups >= 10 (40000 + 10g + n) sit inside Linux's default
    # ephemeral range: reserve them, or an outbound socket can take one and a
    # restarted raft-node fails to bind. Install on every matching host:
    #   cp out/sysctl-raft.conf /etc/sysctl.d/90-trade-core.conf && sysctl --system
    if groups > 10:
        top = raft_port(groups - 1, replicas)
        with open(os.path.join(out_dir, "sysctl-raft.conf"), "w") as f:
            f.write(f"net.ipv4.ip_local_reserved_ports = 40000-{top}\n")
    for host in raft_hosts:
        write(host, {
            **raft_defaults,
            "TC_RAFT_GROUP_PLACEMENTS": ";".join(placements[host]),
            "TC_RAFT_REPLICAS": replicas,
        })
    for host in inv["ingress_hosts"]:
        write(host, {
            **ingress_defaults,
            "TC_ORDER_KAFKA_DB_CONSUMERS": 0,
            "TC_EXECUTION_KAFKA_MYSQL_CONSUMERS": 0,
            "TC_ORDER_KAFKA_DB_ROUTER_CONSUMERS": 4,
            "TC_ORDER_KAFKA_MATCH_CONSUMERS": max(1, groups // len(inv["ingress_hosts"])),
            "TC_RAFT_GROUP_MATCHERS": matchers,
            "TC_MIGRATION_GROUP_ENDPOINTS": migration,
        })
    projection = inv["projection_hosts"]
    per = -(-db_count // len(projection))  # ceil
    for i, host in enumerate(projection):
        first, last = i * per, min(db_count, (i + 1) * per) - 1
        if first > last:
            continue
        owned = last - first + 1
        write(host, {
            **projection_defaults,
            "TC_ORDER_HTTP_INGRESS_ENABLED": "false",
            "TC_MYSQL_OWNED_DBS": f"{first}-{last}",
            "TC_ORDER_MYSQL_SHARD_URLS": shard_urls,
            "TC_ORDER_KAFKA_DB_CONSUMERS": owned,
            "TC_EXECUTION_KAFKA_MYSQL_CONSUMERS": owned,
            "TC_ORDER_KAFKA_DB_ROUTER_CONSUMERS": 0,
            "TC_ORDER_KAFKA_MATCH_CONSUMERS": 0,
        })

    # user-stream: every push host runs one router (the routers share the
    # execution topic as a consumer group) and one pusher shard (the users
    # whose user-topic partition p has p mod shards == its index).
    push_hosts = inv.get("push_hosts", [])
    user_partitions = max(16, len(push_hosts) * 16)
    push_common = {
        "TC_USER_STREAM_TOPIC": "trade-user-events-v1",
        "TC_USER_STREAM_SHARDS": len(push_hosts),
        "TC_USER_STREAM_PEERS": ",".join(f"{host}:9300" for host in push_hosts),
        "TC_USER_STREAM_REPLAY_EVENTS": 512,
        "TC_USER_STREAM_REPLAY_SECS": 300,
        "TC_USER_STREAM_REPLAY_MAX_TOTAL": 20_000_000,
        "TC_EXECUTION_KAFKA_TOPIC": "trade-executions-v1",
    }
    for i, host in enumerate(push_hosts):
        write(f"{host}-router", {
            **push_common,
            "TC_USER_STREAM_ROLE": "router",
            "TC_USER_STREAM_ADDR": "0.0.0.0:9310",
            "TC_USER_STREAM_GROUP": "user-stream-router",
        })
        write(f"{host}-pusher", {
            **push_common,
            "TC_USER_STREAM_ROLE": "pusher",
            "TC_USER_STREAM_ADDR": "0.0.0.0:9300",
            "TC_USER_STREAM_SHARD": i,
        })

    for host in inv.get("market_data_hosts", []):
        write(host, {
            "TC_MARKET_KAFKA_TOPIC": "trade-market-events-v1",
            "TC_MARKET_KAFKA_REQUIRED": "true",
            "TC_MARKET_ALLOW_LEGACY_BOOTSTRAP": "false",
            "TC_ORDER_API_ADDR": f"{inv['ingress_hosts'][0]}:9200",
        })

    # Endpoints for scripts/ops/health.py: every replica's metrics port per
    # group, and every service's.
    health = {
        "raft_groups": {str(g): [f"{host}:{service_ports(g)[2]}" for _, host in members[g]] for g in range(groups)},
        "order_api": [f"{host}:9200" for host in inv["ingress_hosts"] + projection],
        "user_stream": [f"{host}:9300" for host in push_hosts],
        "market_data": [f"{host}:8080" for host in inv.get("market_data_hosts", [])],
    }
    with open(os.path.join(out_dir, "health.json"), "w") as f:
        json.dump(health, f, indent=1)

    with open(os.path.join(out_dir, "topics.sh"), "w") as f:
        # Redpanda's acks=all already waits for a majority; on Apache Kafka add
        # `-c min.insync.replicas=2` to every topic.
        # Explicit retention: a consumer down for longer than this loses
        # data, which the order-api offset preflight then refuses to start
        # over. Size it well above the longest outage you would ride out.
        retention_ms = int(inv.get("kafka_retention_hours", 168)) * 3_600_000
        f.write("#!/bin/sh\nset -eu\nB=" + inv["kafka_brokers"][0] + f"\nR={retention_ms}\n")
        f.write(f"for g in $(seq 0 {groups - 1}); do rpk topic create --brokers=$B -p 16 -r 3 -c retention.ms=$R \"orders-v2-g$g\"; done\n")
        f.write(f"rpk topic create --brokers=$B -p {db_count} -r 3 -c retention.ms=$R trade-order-db-v1\n")
        f.write(f"rpk topic create --brokers=$B -p {db_count} -r 3 -c retention.ms=$R trade-executions-v1\n")
        f.write(f"rpk topic create --brokers=$B -p {groups} -r 3 -c retention.ms=$R trade-market-events-v1\n")
        f.write(f"rpk topic create --brokers=$B -p {db_count} -r 3 -c retention.ms=3600000 trade-benchmark-v1\n")
        f.write(f"rpk topic create --brokers=$B -p {groups} -r 3 -c cleanup.policy=compact trade-execution-outbox-cursors-v1\n")
        f.write("rpk topic create --brokers=$B -p 1 -r 3 -c cleanup.policy=compact trade-route-control-v1\n")
        if push_hosts:
            f.write(f"rpk topic create --brokers=$B -p {user_partitions} -r 3 -c retention.ms=$R trade-user-events-v1\n")
    print(f"db_count={db_count} groups={groups} replicas={replicas}: wrote {out_dir}/")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
