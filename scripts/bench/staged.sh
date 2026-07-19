#!/usr/bin/env bash
#
# Two-axis staged performance test for the trade-core matching pipeline.
#
#   Axis 1 (horizontal / scale-out): cluster_throughput over a node-count list
#     — aggregate throughput and per-node degradation as share-nothing nodes
#     are added. Answers "how far does it scale on this machine".
#
#   Axis 2 (vertical / offered load): latency over a target-rate list — the
#     single-shard saturation curve (achieved rate + latency percentiles per
#     stage). Answers "what TPS can one shard sustain within SLO".
#
# A single peak number hides the SLO knee; staging exposes it. This is the
# LOCAL methodology — absolute peaks and durable (journal/fsync) numbers need a
# dedicated idle NVMe machine; use scripts/acceptance/run-ladder.py there.
#
# Usage:
#   scripts/bench/staged.sh                 # defaults
#   NODES="1 2 4 6 8" scripts/bench/staged.sh
#   RATES="200000 1000000 2000000" ORDERS_PER_STAGE=2000000 scripts/bench/staged.sh
#   ORDERS_PER_NODE=1000000 JOURNAL=1 scripts/bench/staged.sh
#
# Env knobs (all optional):
#   NODES             node counts for axis 1     (default "1 2 4 6")
#   ORDERS_PER_NODE   orders per node, axis 1     (default 1000000)
#   JOURNAL           1 = also run a journaled (durable) axis-1 stage
#   RATES             offered rates for axis 2    (default "200000 500000 1000000 2000000 3000000 4000000")
#   ORDERS_PER_STAGE  orders per rate stage       (default 1000000)
#
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

NODES="${NODES:-1 2 4 6}"
ORDERS_PER_NODE="${ORDERS_PER_NODE:-1000000}"
RATES="${RATES:-200000 500000 1000000 2000000 3000000 4000000}"
ORDERS_PER_STAGE="${ORDERS_PER_STAGE:-1000000}"
JOURNAL="${JOURNAL:-0}"

# ---- environment sanity (contention invalidates absolute numbers) -----------
cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?')"
load1="$(uptime | sed -E 's/.*load averages?: ([0-9.]+).*/\1/')"
echo "# trade-core staged benchmark"
echo "cores=${cores}  load1=${load1}  $(date '+%Y-%m-%d %H:%M:%S')"
if command -v docker >/dev/null 2>&1; then
  running="$(docker ps -q 2>/dev/null | wc -l | tr -d ' ')"
  [ "${running:-0}" -gt 0 ] && echo "WARNING: ${running} docker container(s) running — CPU contention will depress numbers"
fi
awk "BEGIN{ if (\"${load1}\"+0 > ${cores}/2) print \"WARNING: load1 ${load1} is high for ${cores} cores — results are contended, not a clean baseline\" }" 2>/dev/null || true
echo

# ---- build benches once (release) -------------------------------------------
source "$HOME/.cargo/env" 2>/dev/null || true
echo "building release benches (cluster_throughput, latency)…" >&2
cargo build --release --bench cluster_throughput --bench latency >&2

bench_bin() { ls -t "target/release/deps/$1-"* 2>/dev/null | grep -v '\.d$' | head -1; }
CLUSTER="$(bench_bin cluster_throughput)"
LATENCY="$(bench_bin latency)"
[ -x "$CLUSTER" ] || { echo "cluster_throughput binary not found" >&2; exit 1; }
[ -x "$LATENCY" ] || { echo "latency binary not found" >&2; exit 1; }

# ---- axis 1: scale-out ------------------------------------------------------
echo "## Axis 1 — scale-out (cluster_throughput, ${ORDERS_PER_NODE} orders/node)"
echo
printf '| nodes | aggregate orders/s | per-node orders/s | journal |\n'
printf '|------:|-------------------:|------------------:|:-------:|\n'
run_cluster() { # nodes journal_flag journal_label
  local out agg per
  out="$("$CLUSTER" "$1" "$ORDERS_PER_NODE" ${2:+journal} 2>/dev/null | grep processed)"
  agg="$(echo "$out" | sed -nE 's/.*-> *([0-9]+) orders\/s aggregate.*/\1/p')"
  per="$(echo "$out" | sed -nE 's/.*\(([0-9]+) per node\).*/\1/p')"
  printf '| %5s | %18s | %17s | %7s |\n' "$1" "${agg:-?}" "${per:-?}" "$3"
}
for n in $NODES; do run_cluster "$n" "" "off"; done
if [ "$JOURNAL" = "1" ]; then
  # durable stage at the largest node count
  last="$(echo $NODES | tr ' ' '\n' | sort -n | tail -1)"
  run_cluster "$last" "1" "ON"
fi
echo

# ---- axis 2: offered-load ramp ----------------------------------------------
echo "## Axis 2 — offered-load ramp (latency, single shard, ${ORDERS_PER_STAGE} orders/stage)"
echo
printf '| offered/s | achieved/s | p50 | p90 | p99 | p99.9 |\n'
printf '|----------:|-----------:|----:|----:|----:|------:|\n'
for rate in $RATES; do
  out="$("$LATENCY" "$rate" "$ORDERS_PER_STAGE" 2>/dev/null)"
  eff="$(echo "$out"  | sed -nE 's/.*effective ([0-9]+)\/s.*/\1/p')"
  p50="$(echo "$out"  | sed -nE 's/.*p50=([0-9.]+µs).*/\1/p')"
  p90="$(echo "$out"  | sed -nE 's/.*p90=([0-9.]+µs).*/\1/p')"
  p99="$(echo "$out"  | sed -nE 's/.*[^.]p99=([0-9.]+µs).*/\1/p')"
  p999="$(echo "$out" | sed -nE 's/.*p99\.9=([0-9.]+µs).*/\1/p')"
  printf '| %9s | %10s | %s | %s | %s | %s |\n' "$rate" "${eff:-?}" "${p50:-?}" "${p90:-?}" "${p99:-?}" "${p999:-?}"
done
echo
echo "Note: p99+ tails are dominated by OS scheduling noise on an unpinned shard"
echo "(Apple Silicon can't pin cores). Real hardware with core pinning tightens them"
echo "substantially. Durable/fsync and absolute peak numbers require a dedicated"
echo "idle NVMe machine — see scripts/acceptance/run-ladder.py."
