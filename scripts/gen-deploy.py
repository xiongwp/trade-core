#!/usr/bin/env python3
"""Deployment manifest generator for trade-core Raft/MySQL topologies.

Generates per-node docker-compose shard files plus per-node .env files for an
N-group / M-replica / K-node Raft matching cluster and an S-shard MySQL order
store, following the topology in docs/production-deployment-5m-tps.md.

Standard library only. Python 3.8+.

Usage:
    python3 scripts/gen-deploy.py --groups 4   --replicas-per-group 5 --nodes 5  --mysql-shards 10  --output deploy/local
    python3 scripts/gen-deploy.py --groups 100 --replicas-per-group 5 --nodes 50 --mysql-shards 100 --output deploy/prod
    python3 scripts/gen-deploy.py --groups 100 --nodes 50 --mysql-shards 100 --check

Placement invariants (validated in --check and at generation time):
    * two replicas of the same group never land on the same node
      (generation fails when nodes < replicas-per-group);
    * per-node replica counts differ by at most one (balanced);
    * every port assigned on a node is unique on that node.

Port scheme (per node, slot s = index of hosted replica ordered by group id,
replica id rid = 1..M).  Chosen so the generated 4-group/5-node output is
port-for-port identical to the hand-written docker-compose.raft.yml:
    raft transport : 7000 + 10*s + rid
    order ingress  : 9001 + 10*s
    market data    : 9101 + 10*s
    metrics        : 9102 + 10*s
Nodes hosting more than 10 replicas switch order/md/metrics to a packed
20000+ range to avoid cross-slot collisions; raft ports are unchanged.
"""

import argparse
import json
import os
import sys

# The current raft-node binary asserts CLUSTER_SIZE == 5 peers
# (src/raft_log.rs / src/bin/raft_node.rs). Other replica counts can be
# generated for planning with --allow-nonstandard-replicas but will not boot.
SUPPORTED_REPLICAS = 5

RAFT_PORT_BASE = 7000
ORDER_PORT_BASE = 9001
MD_PORT_BASE = 9101
METRICS_PORT_BASE = 9102
PACKED_PORT_BASE = 20000  # used when a node hosts > 10 replicas
LEGACY_SLOTS_PER_NODE = 10

RAFT_IMAGE = "trade-core-node"
MYSQL_IMAGE = "mysql:8.4"

# Tunables copied from the x-raft-environment anchor in docker-compose.raft.yml
# so generated nodes keep the same knobs; each stays overridable per node .env.
RAFT_ENV_KEYS = [
    ("TC_MATCH_POOL_PER_ASSET", "2"),
    ("TC_WAL_PREALLOCATE_BYTES", "0"),
    ("TC_ASSET_WAL_MAX_OPEN_WRITERS", "1024"),
    ("TC_ASSET_WAL_BUFFER_BYTES", "8192"),
    ("TC_RAFT_TRANSPORT_QUEUE", "8192"),
    ("TC_RAFT_READY_MAX_APPLY_LAG", "32"),
    ("TC_RAFT_READY_MAX_OUTBOX_PENDING", "10000"),
    ("TC_ORDER_CATEGORY_SIZE", "1000"),
    ("TC_EXECUTION_OUTBOX_SYNC_EVERY", "1"),
    ("TC_EXECUTION_PUBLISH_BATCH", "512"),
    ("TC_EXECUTION_KAFKA_DELIVERY_TIMEOUT_MS", "5000"),
    ("TC_EXECUTION_KAFKA_TOPIC", "trade-executions-v1"),
    ("TC_EXECUTION_CURSOR_TOPIC", "trade-execution-outbox-cursors-v1"),
    ("TC_EXECUTION_CURSOR_PARTITIONS", "16"),
    ("TC_EXECUTION_CURSOR_EVERY_BATCHES", "16"),
]


class TopologyError(Exception):
    pass


# --------------------------------------------------------------------------
# Placement
# --------------------------------------------------------------------------

def place(groups, replicas, nodes):
    """Return {group: [node_index (0-based) per replica slot]}.

    Round-robin stripe: replica r of group g lands on node (g*replicas + r)
    mod nodes.  This is balanced (per-node counts differ by <= 1) and, because
    replicas <= nodes, the replicas of one group always hit distinct nodes.
    For 4 groups / 5 replicas / 5 nodes it reproduces docker-compose.raft.yml
    exactly: node i hosts replica i of every group.
    """
    if replicas > nodes:
        raise TopologyError(
            f"replicas-per-group={replicas} needs at least {replicas} nodes "
            f"to keep same-group replicas on distinct failure domains, "
            f"got nodes={nodes}"
        )
    placement = {}
    for g in range(groups):
        placement[g] = [(g * replicas + r) % nodes for r in range(replicas)]
    return placement


def node_slots(placement, nodes):
    """Return {node: [(group, replica_index)] ordered by group id}."""
    hosted = {n: [] for n in range(nodes)}
    for g in sorted(placement):
        for r, n in enumerate(placement[g]):
            hosted[n].append((g, r))
    for n in hosted:
        hosted[n].sort()
    return hosted


def ports_for(slot, replica_index, packed):
    rid = replica_index + 1
    raft = RAFT_PORT_BASE + 10 * slot + rid
    if packed:
        order = PACKED_PORT_BASE + 10 * slot + 1
        md = PACKED_PORT_BASE + 10 * slot + 3
        metrics = PACKED_PORT_BASE + 10 * slot + 4
    else:
        order = ORDER_PORT_BASE + 10 * slot
        md = MD_PORT_BASE + 10 * slot
        metrics = METRICS_PORT_BASE + 10 * slot
    return raft, order, md, metrics


def build_topology(groups, replicas, nodes, mysql_shards):
    placement = place(groups, replicas, nodes)
    hosted = node_slots(placement, nodes)
    # replica -> port map, keyed (group, replica_index)
    ports = {}
    for n, slots in hosted.items():
        packed = len(slots) > LEGACY_SLOTS_PER_NODE
        for slot, (g, r) in enumerate(slots):
            ports[(g, r)] = ports_for(slot, r, packed)
    return {
        "groups": groups,
        "replicas_per_group": replicas,
        "nodes": nodes,
        "mysql_shards": mysql_shards,
        "placement": placement,
        "hosted": hosted,
        "ports": ports,
    }


# --------------------------------------------------------------------------
# Validation (--check and post-generation)
# --------------------------------------------------------------------------

def validate(topo, out=sys.stdout):
    errors = []
    warnings = []
    placement = topo["placement"]
    hosted = topo["hosted"]
    ports = topo["ports"]
    replicas = topo["replicas_per_group"]
    nodes = topo["nodes"]

    if replicas != SUPPORTED_REPLICAS:
        warnings.append(
            f"replicas-per-group={replicas}: the current raft-node binary "
            f"asserts exactly {SUPPORTED_REPLICAS} peers; this manifest is "
            f"planning-only until the binary supports other cluster sizes"
        )

    # 1. failure-domain constraint: same-group replicas on distinct nodes
    for g, node_list in placement.items():
        if len(set(node_list)) != len(node_list):
            errors.append(f"group {g}: replicas share a node: {node_list}")
        if len(node_list) != replicas:
            errors.append(f"group {g}: expected {replicas} replicas, got {len(node_list)}")

    # 2. balance: per-node replica counts differ by at most one
    counts = [len(hosted[n]) for n in range(nodes)]
    if counts and max(counts) - min(counts) > 1:
        errors.append(f"unbalanced placement: per-node replica counts {min(counts)}..{max(counts)}")

    # 3. per-node port uniqueness
    for n in range(nodes):
        seen = {}
        for (g, r) in hosted[n]:
            for label, port in zip(("raft", "order", "md", "metrics"), ports[(g, r)]):
                key = port
                if key in seen:
                    errors.append(
                        f"node {n + 1}: port {port} ({label}, group {g}) "
                        f"collides with {seen[key]}"
                    )
                seen[key] = f"{label} of group {g}"

    for w in warnings:
        print(f"WARN  {w}", file=out)
    for e in errors:
        print(f"ERROR {e}", file=out)
    if not errors:
        print(
            f"OK    {topo['groups']} groups x {replicas} replicas on {nodes} nodes "
            f"({min(counts)}-{max(counts)} replicas/node), {topo['mysql_shards']} MySQL shards: "
            f"failure-domain, balance and port constraints hold",
            file=out,
        )
    return not errors


# --------------------------------------------------------------------------
# Emission helpers
# --------------------------------------------------------------------------

def node_name(n):
    return f"raft-{n + 1}"


def peer_string(topo, g):
    parts = []
    for r, n in enumerate(topo["placement"][g]):
        raft_port = topo["ports"][(g, r)][0]
        parts.append(f"{r + 1}@{node_name(n)}:{raft_port}")
    return ",".join(parts)


def render_node_compose(topo, n):
    """One docker-compose file per physical node."""
    hosted = topo["hosted"][n]
    nn = node_name(n)
    lines = [
        f"# Generated by scripts/gen-deploy.py — node {nn}",
        f"# topology: {topo['groups']} groups x {topo['replicas_per_group']} replicas / {topo['nodes']} nodes",
        f"# Deploy on the host that other nodes reach as '{nn}'. Run with:",
        f"#   docker compose --env-file {nn}.env -f docker-compose.{nn}.yml up -d",
        "# Cross-node hostnames raft-1..raft-N resolve through the extra_hosts",
        f"# entries below; set RAFT_HOST_* in {nn}.env to the real node IPs.",
        "",
        "x-raft-hosts: &raft-hosts",
    ]
    for peer in range(topo["nodes"]):
        pn = node_name(peer)
        lines.append(f'  - "{pn}:${{RAFT_HOST_{peer + 1}:?set RAFT_HOST_{peer + 1} in {nn}.env}}"')
    lines += [
        "",
        "x-raft-environment: &raft-environment",
    ]
    for key, default in RAFT_ENV_KEYS:
        lines.append(f'  {key}: "${{{key}:-{default}}}"')
    lines.append(
        '  TC_EXECUTION_KAFKA_BROKERS: "${TC_EXECUTION_KAFKA_BROKERS'
        ':-redpanda:9092,redpanda-2:9092,redpanda-3:9092}"'
    )
    lines.append('  TC_EXECUTION_PUBLISH_ENABLED: "${TC_EXECUTION_PUBLISH_ENABLED:-true}"')
    lines += ["", "services:"]

    for (g, r) in hosted:
        raft_port, order_port, md_port, metrics_port = topo["ports"][(g, r)]
        svc = f"raft-g{g}-r{r + 1}"
        peers = peer_string(topo, g)
        lines += [
            f"  {svc}:",
            f"    image: {RAFT_IMAGE}",
            f'    command: ["exec raft-node {r + 1} 0.0.0.0:{raft_port} {peers} '
            f'0.0.0.0:{order_port} /data 0.0.0.0:{md_port} 0.0.0.0:{metrics_port}"]',
            "    environment:",
            "      <<: *raft-environment",
            f'      TC_RAFT_GROUP_ID: "{g}"',
            "    extra_hosts: *raft-hosts",
            "    ports:",
            f'      - "{raft_port}:{raft_port}"',
            f'      - "{order_port}:{order_port}"',
            f'      - "{md_port}:{md_port}"',
            f'      - "{metrics_port}:{metrics_port}"',
            "    volumes:",
            f'      - "{svc}-data:/data"',
            "    healthcheck:",
            f'      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/{metrics_port} && exec 3>&- 3<&-"]',
            "      interval: 5s",
            "      timeout: 3s",
            "      retries: 40",
            "      start_period: 20s",
            "    restart: unless-stopped",
        ]

    lines += ["", "volumes:"]
    for (g, r) in hosted:
        lines.append(f"  raft-g{g}-r{r + 1}-data:")
    lines.append("")
    return "\n".join(lines)


def render_node_env(topo, n):
    nn = node_name(n)
    lines = [
        f"# Generated by scripts/gen-deploy.py — env for {nn}",
        f"# Pass with: docker compose --env-file {nn}.env -f docker-compose.{nn}.yml up -d",
        "",
        "# --- cross-node addressing (CHANGE-ME: real node IPs) ---",
    ]
    for peer in range(topo["nodes"]):
        lines.append(f"RAFT_HOST_{peer + 1}=10.100.0.{peer + 1}  # CHANGE-ME")
    lines += [
        "",
        "# --- Kafka execution fanout (CHANGE-ME: production broker list) ---",
        "TC_EXECUTION_KAFKA_BROKERS=redpanda:9092,redpanda-2:9092,redpanda-3:9092",
        "",
        "# --- per-machine tuning; see docs/production-deployment-5m-tps.md §3.1 ---",
        "# 128 GiB Raft-node tier baseline (uncomment and calibrate after load test):",
        "#TC_ASSET_WAL_MAX_OPEN_WRITERS=8192",
        "#TC_ASSET_WAL_BUFFER_BYTES=32768",
        "#TC_WAL_PREALLOCATE_BYTES=67108864",
    ]
    for key, default in RAFT_ENV_KEYS:
        lines.append(f"#{key}={default}")
    lines.append("")
    return "\n".join(lines)


def render_mysql_compose(topo):
    shards = topo["mysql_shards"]
    lines = [
        "# Generated by scripts/gen-deploy.py — MySQL order shards",
        f"# {shards} write shards. In production each shard runs on its own host",
        "# (docs/production-deployment-5m-tps.md §8); this file is the per-shard",
        "# service template and doubles as a single-host test harness.",
        "#   docker compose --env-file mysql.env -f docker-compose.mysql.yml up -d",
        "",
        "services:",
    ]
    for i in range(shards):
        lines += [
            f"  mysql-order-{i}:",
            f"    image: {MYSQL_IMAGE}",
            '    command: ["--innodb-buffer-pool-size=${MYSQL_BUFFER_POOL:-64M}", '
            '"--performance-schema=OFF", "--max-connections=${MYSQL_MAX_CONNECTIONS:-100}"]',
            "    environment:",
            '      MYSQL_ROOT_PASSWORD: "${MYSQL_ROOT_PASSWORD:?set MYSQL_ROOT_PASSWORD in mysql.env}"',
            "    volumes:",
            f'      - "order-mysql-{i}:/var/lib/mysql"',
            "    healthcheck:",
            '      test: ["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 -uroot -p$$MYSQL_ROOT_PASSWORD --silent"]',
            "      interval: 3s",
            "      timeout: 3s",
            "      retries: 40",
            "      start_period: 10s",
            "    restart: unless-stopped",
        ]
    lines += ["", "volumes:"]
    for i in range(shards):
        lines.append(f"  order-mysql-{i}:")
    lines.append("")
    return "\n".join(lines)


def render_mysql_env(topo):
    return "\n".join(
        [
            "# Generated by scripts/gen-deploy.py — env for docker-compose.mysql.yml",
            "MYSQL_ROOT_PASSWORD=CHANGE-ME",
            "# Production shard hosts run with large buffer pools; the 64M default",
            "# is only for single-host smoke tests.",
            "#MYSQL_BUFFER_POOL=192G",
            "#MYSQL_MAX_CONNECTIONS=500",
            "",
        ]
    )


def render_gateway_compose(topo):
    """order-api + market-data with the full generated wiring strings."""
    groups = topo["groups"]
    matchers = []
    md_endpoints = []
    for g in range(groups):
        replica_parts = []
        for r, n in enumerate(topo["placement"][g]):
            _, order_port, md_port, metrics_port = topo["ports"][(g, r)]
            replica_parts.append(f"{node_name(n)}:{order_port}@{node_name(n)}:{metrics_port}")
            md_endpoints.append(f"{node_name(n)}:{md_port}")
        matchers.append(",".join(replica_parts))
    matcher_string = ";".join(matchers)
    md_string = ",".join(md_endpoints)
    shard_urls = ",".join(
        "mysql://root:${MYSQL_ROOT_PASSWORD:?set in gateway.env}"
        f"@mysql-order-{i}:3306/mysql"
        for i in range(topo["mysql_shards"])
    )

    lines = [
        "# Generated by scripts/gen-deploy.py — order API + market-data gateway",
        "# Hostnames raft-1..raft-N and mysql-order-* must resolve on this host",
        "# (DNS, /etc/hosts, or an extra_hosts overlay).",
        "#   docker compose --env-file gateway.env -f docker-compose.gateway.yml up -d",
        "",
        "services:",
        "  order:",
        f"    image: {RAFT_IMAGE}",
        '    command: ["exec order-api"]',
        "    environment:",
        f'      TC_RAFT_GROUP_MATCHERS: "{matcher_string}"',
        "      TC_ORDER_API_TOKEN: ${TC_ORDER_API_TOKEN:?set in gateway.env}",
        "      TC_ORDER_KAFKA_BROKERS: ${TC_ORDER_KAFKA_BROKERS:?set in gateway.env}",
        f"      TC_ORDER_KAFKA_TOPIC_COUNT: ${{TC_ORDER_KAFKA_TOPIC_COUNT:-{groups}}}",
        "      TC_ORDER_KAFKA_TOPIC_PREFIX: ${TC_ORDER_KAFKA_TOPIC_PREFIX:-orders-v2-g}",
        "      TC_ORDER_CATEGORY_SIZE: ${TC_ORDER_CATEGORY_SIZE:-1000}",
        "      TC_ORDER_KAFKA_DB_CONSUMERS: 0",
        "      TC_ORDER_KAFKA_MATCH_CONSUMERS: 0",
        "      TC_EXECUTION_KAFKA_MYSQL_CONSUMERS: 0",
        "      TC_ORDER_HTTP_INGRESS_ENABLED: true",
        "    ports:",
        '      - "9200:9200"',
        "    healthcheck:",
        '      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9200 && exec 3>&- 3<&-"]',
        "      interval: 5s",
        "      timeout: 3s",
        "      retries: 40",
        "      start_period: 20s",
        "    restart: unless-stopped",
        "",
        "  command-db-worker:",
        f"    image: {RAFT_IMAGE}",
        '    command: ["exec order-api"]',
        "    environment:",
        f"      TC_ORDER_MYSQL_SHARD_URLS: {shard_urls}",
        f'      TC_RAFT_GROUP_MATCHERS: "{matcher_string}"',
        "      TC_ORDER_API_TOKEN: ${TC_ORDER_API_TOKEN:?set in gateway.env}",
        "      TC_ORDER_KAFKA_BROKERS: ${TC_ORDER_KAFKA_BROKERS:?set in gateway.env}",
        f"      TC_ORDER_KAFKA_TOPIC_COUNT: ${{TC_ORDER_KAFKA_TOPIC_COUNT:-{groups}}}",
        "      TC_ORDER_KAFKA_DB_CONSUMERS: ${TC_COMMAND_DB_CONSUMERS_PER_REPLICA:-8}",
        "      TC_ORDER_KAFKA_MATCH_CONSUMERS: 0",
        "      TC_EXECUTION_KAFKA_MYSQL_CONSUMERS: 0",
        "      TC_ORDER_HTTP_INGRESS_ENABLED: false",
        "    restart: unless-stopped",
        "",
        "  match-worker:",
        f"    image: {RAFT_IMAGE}",
        '    command: ["exec order-api"]',
        "    environment:",
        f'      TC_RAFT_GROUP_MATCHERS: "{matcher_string}"',
        "      TC_ORDER_API_TOKEN: ${TC_ORDER_API_TOKEN:?set in gateway.env}",
        "      TC_ORDER_KAFKA_BROKERS: ${TC_ORDER_KAFKA_BROKERS:?set in gateway.env}",
        f"      TC_ORDER_KAFKA_TOPIC_COUNT: ${{TC_ORDER_KAFKA_TOPIC_COUNT:-{groups}}}",
        "      TC_ORDER_KAFKA_DB_CONSUMERS: 0",
        "      TC_ORDER_KAFKA_MATCH_CONSUMERS: ${TC_MATCH_CONSUMERS_PER_REPLICA:-4}",
        "      TC_ORDER_MATCH_BATCH_SIZE: ${TC_ORDER_MATCH_BATCH_SIZE:-10000}",
        "      TC_ORDER_MATCH_BATCH_LINGER_MS: ${TC_ORDER_MATCH_BATCH_LINGER_MS:-2}",
        "      TC_EXECUTION_KAFKA_MYSQL_CONSUMERS: 0",
        "      TC_ORDER_HTTP_INGRESS_ENABLED: false",
        "    restart: unless-stopped",
        "",
        "  execution-db-worker:",
        f"    image: {RAFT_IMAGE}",
        '    command: ["exec order-api"]',
        "    environment:",
        f"      TC_ORDER_MYSQL_SHARD_URLS: {shard_urls}",
        f'      TC_RAFT_GROUP_MATCHERS: "{matcher_string}"',
        "      TC_ORDER_API_TOKEN: ${TC_ORDER_API_TOKEN:?set in gateway.env}",
        "      TC_ORDER_KAFKA_BROKERS: ${TC_ORDER_KAFKA_BROKERS:?set in gateway.env}",
        f"      TC_ORDER_KAFKA_TOPIC_COUNT: ${{TC_ORDER_KAFKA_TOPIC_COUNT:-{groups}}}",
        "      TC_ORDER_KAFKA_DB_CONSUMERS: 0",
        "      TC_ORDER_KAFKA_MATCH_CONSUMERS: 0",
        "      TC_EXECUTION_KAFKA_MYSQL_CONSUMERS: ${TC_EXECUTION_DB_CONSUMERS_PER_REPLICA:-8}",
        "      TC_ORDER_HTTP_INGRESS_ENABLED: false",
        "    restart: unless-stopped",
        "",
        "  market-data:",
        f"    image: {RAFT_IMAGE}",
        f'    command: ["exec market-data {md_string} 0.0.0.0:8080 /data/md"]',
        "    environment:",
        "      TC_ADMIN_TOKEN: ${TC_ADMIN_TOKEN:?set in gateway.env}",
        "      TC_TRADING_TOKEN: ${TC_TRADING_TOKEN:?set in gateway.env}",
        "      TC_ORDER_API_ADDR: order:9200",
        "      TC_ORDER_API_TOKEN: ${TC_ORDER_API_TOKEN:?set in gateway.env}",
        "    ports:",
        '      - "8081:8080"',
        "    volumes:",
        '      - "market-data-history:/data/md"',
        "    healthcheck:",
        '      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080 && exec 3>&- 3<&-"]',
        "      interval: 5s",
        "      timeout: 3s",
        "      retries: 40",
        "      start_period: 20s",
        "    depends_on:",
        "      order:",
        "        condition: service_healthy",
        "    restart: unless-stopped",
        "",
        "volumes:",
        "  market-data-history:",
        "",
    ]
    return "\n".join(lines)


def render_gateway_env():
    return "\n".join(
        [
            "# Generated by scripts/gen-deploy.py — env for docker-compose.gateway.yml",
            "MYSQL_ROOT_PASSWORD=CHANGE-ME",
            "TC_ORDER_API_TOKEN=CHANGE-ME",
            "TC_ADMIN_TOKEN=CHANGE-ME",
            "TC_TRADING_TOKEN=CHANGE-ME",
            "TC_ORDER_KAFKA_BROKERS=CHANGE-ME-broker-1:9092,CHANGE-ME-broker-2:9092",
            "TC_COMMAND_DB_CONSUMERS_PER_REPLICA=8",
            "TC_MATCH_CONSUMERS_PER_REPLICA=4",
            "TC_EXECUTION_DB_CONSUMERS_PER_REPLICA=8",
            "",
        ]
    )


def topology_json(topo):
    return json.dumps(
        {
            "groups": topo["groups"],
            "replicas_per_group": topo["replicas_per_group"],
            "nodes": topo["nodes"],
            "mysql_shards": topo["mysql_shards"],
            "placement": {str(g): v for g, v in topo["placement"].items()},
            "ports": {
                f"g{g}-r{r + 1}": {
                    "node": node_name(topo["placement"][g][r]),
                    "raft": p[0],
                    "order": p[1],
                    "market_data": p[2],
                    "metrics": p[3],
                }
                for (g, r), p in sorted(topo["ports"].items())
            },
        },
        indent=2,
    )


# --------------------------------------------------------------------------
# Comparison against the hand-written compose (best effort)
# --------------------------------------------------------------------------

def compare_with_compose(topo, path, out=sys.stdout):
    """Sanity-compare against docker-compose.raft.yml's raft-multi-node layout.

    That file runs `raft-multi-node <node> ${TC_RAFT_GROUPS:-G} /data` on
    services raft-1..raft-K, i.e. every node hosts every group. We check node
    count, group count and that our placement also puts every group on every
    node (only meaningful when groups*replicas == nodes*groups).
    """
    import re

    try:
        text = open(path, encoding="utf-8").read()
    except OSError as error:
        print(f"WARN  cannot read {path}: {error}", file=out)
        return True
    nodes = sorted(set(int(m) for m in re.findall(r"^  raft-(\d+):", text, re.M)))
    groups = re.findall(r"raft-multi-node \d+ \$\{TC_RAFT_GROUPS:-(\d+)\}", text)
    if not nodes or not groups:
        print(f"WARN  {path}: unrecognized structure, skipping comparison", file=out)
        return True
    ref_nodes, ref_groups = len(nodes), int(groups[0])
    ok = True
    if topo["nodes"] != ref_nodes or topo["groups"] != ref_groups:
        print(
            f"ERROR reference {path} is {ref_groups} groups / {ref_nodes} nodes; "
            f"generated topology is {topo['groups']} groups / {topo['nodes']} nodes",
            file=out,
        )
        ok = False
    else:
        for g in range(topo["groups"]):
            if sorted(topo["placement"][g]) != list(range(ref_nodes)):
                print(f"ERROR group {g} not present on every node as in {path}", file=out)
                ok = False
    if ok:
        print(
            f"OK    placement matches {path}: every one of {ref_groups} groups "
            f"replicated on all {ref_nodes} nodes",
            file=out,
        )
    return ok


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--groups", type=int, required=True, help="Raft group count")
    parser.add_argument("--replicas-per-group", type=int, default=SUPPORTED_REPLICAS)
    parser.add_argument("--nodes", type=int, required=True, help="physical Raft node count")
    parser.add_argument("--mysql-shards", type=int, default=10)
    parser.add_argument("--output", help="output directory for generated manifests")
    parser.add_argument("--check", action="store_true",
                        help="validate placement constraints only; no files written")
    parser.add_argument("--compare", metavar="COMPOSE_YML",
                        help="compare placement with a hand-written raft compose file")
    parser.add_argument("--allow-nonstandard-replicas", action="store_true",
                        help="permit replicas-per-group != 5 (planning only; "
                             "the raft-node binary requires exactly 5 peers)")
    args = parser.parse_args(argv)

    if args.groups < 1 or args.nodes < 1 or args.replicas_per_group < 1 or args.mysql_shards < 0:
        parser.error("counts must be positive")
    if args.replicas_per_group != SUPPORTED_REPLICAS and not args.allow_nonstandard_replicas:
        parser.error(
            f"replicas-per-group must be {SUPPORTED_REPLICAS} (raft-node asserts "
            f"a five-peer cluster); pass --allow-nonstandard-replicas for planning output"
        )
    if not args.check and not args.output:
        parser.error("--output DIR is required unless --check is given")

    try:
        topo = build_topology(args.groups, args.replicas_per_group, args.nodes, args.mysql_shards)
    except TopologyError as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 2

    ok = validate(topo)
    if args.compare:
        ok = compare_with_compose(topo, args.compare) and ok
    if not ok:
        return 2
    if args.check:
        return 0

    outdir = args.output
    os.makedirs(outdir, exist_ok=True)
    written = []

    for n in range(topo["nodes"]):
        nn = node_name(n)
        compose_path = os.path.join(outdir, f"docker-compose.{nn}.yml")
        env_path = os.path.join(outdir, f"{nn}.env")
        with open(compose_path, "w", encoding="utf-8") as fh:
            fh.write(render_node_compose(topo, n))
        with open(env_path, "w", encoding="utf-8") as fh:
            fh.write(render_node_env(topo, n))
        written += [compose_path, env_path]

    with open(os.path.join(outdir, "docker-compose.mysql.yml"), "w", encoding="utf-8") as fh:
        fh.write(render_mysql_compose(topo))
    with open(os.path.join(outdir, "mysql.env"), "w", encoding="utf-8") as fh:
        fh.write(render_mysql_env(topo))
    with open(os.path.join(outdir, "docker-compose.gateway.yml"), "w", encoding="utf-8") as fh:
        fh.write(render_gateway_compose(topo))
    with open(os.path.join(outdir, "gateway.env"), "w", encoding="utf-8") as fh:
        fh.write(render_gateway_env())
    with open(os.path.join(outdir, "topology.json"), "w", encoding="utf-8") as fh:
        fh.write(topology_json(topo) + "\n")
    written += [
        os.path.join(outdir, "docker-compose.mysql.yml"),
        os.path.join(outdir, "mysql.env"),
        os.path.join(outdir, "docker-compose.gateway.yml"),
        os.path.join(outdir, "gateway.env"),
        os.path.join(outdir, "topology.json"),
    ]

    total_replicas = topo["groups"] * topo["replicas_per_group"]
    per_node = sorted(len(s) for s in topo["hosted"].values())
    print(
        f"wrote {len(written)} files to {outdir}: {topo['nodes']} node compose shards, "
        f"{total_replicas} raft replicas ({per_node[0]}-{per_node[-1]} per node), "
        f"{topo['mysql_shards']} MySQL shards, gateway, topology.json"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
