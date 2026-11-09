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
  "raft_groups":      64,
  "replicas":         3,
  "projection_hosts": ["pj01", ...],               # order-api DB workers
  "ingress_hosts":    ["in01", ...],               # order-api intake
  "kafka_brokers":    ["kf01:9092", ...]
}

Writes out/<host>.env for every host and out/topics.sh with the Kafka topic
creation commands.
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

    # Globally identical routing/determinism parameters.
    common = {
        "TC_DB_COUNT": db_count,
        "TC_TABLES_PER_DB": 100,
        # >= 1000 virtual DBs per physical DB keeps the hottest DB within ~10%
        # of the average (1000 total for 400 DBs left some DBs empty and one
        # at 2.8x the average).
        "TC_VIRTUAL_DB_COUNT": db_count * 1000,
        "TC_VIRTUAL_TABLE_COUNT": db_count * 10000,
        "TC_SHARD_ROUTE_VERSION": 1,
        "TC_ORDER_CATEGORY_SIZE": 1,
        "TC_EXECUTION_KAFKA_PARTITIONS": db_count,
        "TC_ORDER_DB_KAFKA_PARTITIONS": db_count,
        "TC_ORDER_KAFKA_TOPIC_COUNT": groups,
        "TC_ORDER_KAFKA_PARTITIONS_PER_TOPIC": 16,
        "TC_ORDER_KAFKA_BROKERS": brokers,
        "TC_EXECUTION_KAFKA_BROKERS": brokers,
    }

    # Raft placement: group g's replicas on consecutive hosts starting at
    # g mod hosts, so every host carries the same number of replicas and no
    # host holds two replicas of one group.
    placements = {host: [] for host in raft_hosts}
    members = {}
    for g in range(groups):
        hosts = [raft_hosts[(g + r) % len(raft_hosts)] for r in range(replicas)]
        members[g] = [(node + 1, host) for node, host in enumerate(hosts)]
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
        "TC_RAFT_TRANSPORT_QUEUE": 8192,
        "TC_MATCH_POOL_PER_ASSET": 4096,
        "TC_EXECUTION_OUTBOX_SYNC_EVERY": 1,
        "TC_EXECUTION_PUBLISH_BATCH": 4096,
        "TC_EXECUTION_KAFKA_LINGER_MS": 2,
        "TC_EXECUTION_KAFKA_BATCH_MESSAGES": 10_000,
        "TC_EXECUTION_KAFKA_COMPRESSION": "lz4",
        "TC_EXECUTION_KAFKA_DELIVERY_TIMEOUT_MS": 60000,
        "TC_SUPERVISOR_LIVENESS_GRACE_SECS": 300,
        "TC_SUPERVISOR_LIVENESS_FAILURES": 6,
    }
    partitions = groups * 16
    per_partition = max(1, 2_000_000 // partitions)  # design commands/s/partition
    ingress_defaults = {
        "TC_RAFT_PACK_SIZE": 5000,
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
    }

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

    with open(os.path.join(out_dir, "topics.sh"), "w") as f:
        # Redpanda's acks=all already waits for a majority; on Apache Kafka add
        # `-c min.insync.replicas=2` to every topic.
        f.write("#!/bin/sh\nset -eu\nB=" + inv["kafka_brokers"][0] + "\n")
        f.write(f"for g in $(seq 0 {groups - 1}); do rpk topic create --brokers=$B -p 16 -r 3 \"orders-v2-g$g\"; done\n")
        f.write(f"rpk topic create --brokers=$B -p {db_count} -r 3 trade-order-db-v1\n")
        f.write(f"rpk topic create --brokers=$B -p {db_count} -r 3 trade-executions-v1\n")
        f.write(f"rpk topic create --brokers=$B -p {groups} -r 3 -c cleanup.policy=compact trade-execution-outbox-cursors-v1\n")
        f.write("rpk topic create --brokers=$B -p 1 -r 3 -c cleanup.policy=compact trade-route-control-v1\n")
    print(f"db_count={db_count} groups={groups} replicas={replicas}: wrote {out_dir}/")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
