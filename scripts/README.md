# scripts

## bench/staged.sh — two-axis staged performance test

Runs the local staged benchmark: horizontal scale-out
(`cluster_throughput` over a node-count list) and vertical offered-load ramp
(`latency` over a target-rate list), printing markdown tables. A single peak
number hides the SLO knee; staging exposes it.

```sh
scripts/bench/staged.sh                                   # defaults
NODES="1 2 4 6 8" scripts/bench/staged.sh                 # scale-out axis
RATES="200000 1000000 2000000" scripts/bench/staged.sh    # load axis
ORDERS_PER_NODE=1000000 JOURNAL=1 scripts/bench/staged.sh # add durable stage
```

Stop the docker stack and confirm a low `load1` first — contention depresses
the numbers (the script warns). Absolute peaks and durable/fsync numbers need
a dedicated idle NVMe machine; use `scripts/acceptance/run-ladder.py` there.
A recorded local baseline lives in `docs/stage-performance-local-baseline.md`.

## gen-deploy.py — deployment manifest generator

Generates the sharded docker-compose deployment described in
`docs/production-deployment-5m-tps.md` (N Raft groups, 5 replicas per group,
K physical nodes, S MySQL write shards). Python 3.8+ standard library only.

```sh
# local-equivalent topology (matches the hand-written docker-compose.raft.yml)
python3 scripts/gen-deploy.py --groups 4 --nodes 5 --mysql-shards 10 \
    --output deploy/local --compare docker-compose.raft.yml

# production topology from the 5M-TPS deployment doc
python3 scripts/gen-deploy.py --groups 100 --nodes 50 --mysql-shards 100 \
    --output deploy/prod

# constraint check only, no files written
python3 scripts/gen-deploy.py --groups 100 --nodes 50 --mysql-shards 100 --check
```

Output per run:

| File | Purpose |
|---|---|
| `docker-compose.raft-K.yml` | one compose shard per physical node; runs one `raft-node` container per hosted group replica |
| `raft-K.env` | per-node env: `RAFT_HOST_*` peer IPs (CHANGE-ME), Kafka brokers, per-machine WAL/backlog tuning |
| `docker-compose.mysql.yml` + `mysql.env` | S MySQL order shards (one host per shard in production) |
| `docker-compose.gateway.yml` + `gateway.env` | order-api + market-data with the full generated `TC_RAFT_GROUP_MATCHERS` / shard-URL / market-data endpoint wiring |
| `topology.json` | machine-readable placement and port map |

Deploy a node with `docker compose --env-file raft-K.env -f docker-compose.raft-K.yml up -d`.

Guarantees (enforced at generation time and by `--check`):

- two replicas of one group never share a node; generation fails when
  `--nodes` < `--replicas-per-group`;
- per-node replica counts are balanced (differ by at most one);
- every port on a node is unique; the 4-group/5-node output is
  port-for-port identical to `docker-compose.raft.yml`
  (`--compare docker-compose.raft.yml` verifies the placement);
- `--replicas-per-group` must be 5 — the `raft-node` binary asserts a
  five-peer cluster (`--allow-nonstandard-replicas` emits planning-only
  manifests).

## protoc-compat.sh

Shim that reports a fixed `libprotoc 3.21.12` version and forwards to the
system `protoc`, for toolchains that reject newer protoc version strings.
