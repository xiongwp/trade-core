#!/usr/bin/env bash
# End-to-end stress test on the local Docker stack, always from clean data.
#
# Every run wipes the stack (all MySQL, Raft and Kafka volumes), starts it,
# waits until all Raft replicas are ready and order is healthy, then runs
# benches/user_sim_e2e.rs. Data left by earlier runs makes end-to-end TPS fall
# run after run (measured: ~11k -> ~5k cmd/s), so runs are only comparable
# when each one starts empty.
#
#   scripts/bench/e2e-clean.sh                         # 300k commands, defaults
#   scripts/bench/e2e-clean.sh 1000000 10000 100 16 500
#   IMAGE=trade-core-node:candidate scripts/bench/e2e-clean.sh
#   TC_SIM_TRANSPORT=ws scripts/bench/e2e-clean.sh 300000 10000 100 16 5000
#
# Args after the first are passed through as user_sim_e2e's
# COMMANDS USERS INSTRUMENTS CONCURRENCY BATCH (see benches/user_sim_e2e.rs).
# IMAGE (optional) is tagged as trade-core-node:latest before the stack
# starts, to compare builds (A/B: alternate them, two runs each).
#
# Destroys local test data by design. Never point it at a real deployment.
set -euo pipefail

# A Mac that sleeps mid-run pauses the Docker VM and the client: Kafka
# sessions and timers break and the run hangs or reports garbage. Keep the
# machine awake for the whole run (macOS only; re-exec once).
if command -v caffeinate >/dev/null 2>&1 && [ -z "${TC_CAFFEINATED:-}" ]; then
    exec env TC_CAFFEINATED=1 caffeinate -dimsu "$0" "$@"
fi

cd "$(dirname "$0")/../.."
COMMANDS="${1:-300000}"
USERS="${2:-10000}"
INSTRUMENTS="${3:-100}"
CONCURRENCY="${4:-16}"
BATCH="${5:-500}"
RAFT_GROUPS="${TC_RAFT_GROUPS:-4}"
NODES=5
COMPOSE=(docker compose -p trade-core -f docker-compose.yml -f docker-compose.raft.yml)

if [ -n "${IMAGE:-}" ]; then
    docker tag "$IMAGE" trade-core-node:latest
fi

echo "== wiping the local stack (volumes included)"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
echo "== starting"
"${COMPOSE[@]}" up -d >/dev/null 2>&1

ready_replicas() {
    local n=0 node group value
    for node in $(seq 1 "$NODES"); do
        for group in $(seq 0 $((RAFT_GROUPS - 1))); do
            value=$(curl -s --max-time 2 "localhost:$((9200 + group * 10 + node))/metrics" \
                | awk '/^tc_ready /{print $2}')
            [ "$value" = "1" ] && n=$((n + 1))
        done
    done
    echo "$n"
}

want=$((NODES * RAFT_GROUPS))
deadline=$((SECONDS + 300))
until [ "$(ready_replicas)" = "$want" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "Raft replicas not ready after 300 s" >&2; exit 1; }
    sleep 3
done
until docker ps --format '{{.Names}} {{.Status}}' | grep -q 'trade-core-order-1 .*(healthy)'; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "order not healthy after 300 s" >&2; exit 1; }
    sleep 2
done
# Let the Kafka consumer groups finish their first assignment.
sleep 10
echo "== ready: $want/$want replicas, image $(docker inspect trade-core-order-1 --format '{{.Image}}' | cut -c8-19), load $(uptime | awk -F'averages?: ' '{print $2}')"

# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
cargo bench --bench user_sim_e2e -q -- \
    127.0.0.1:9200 "${TC_ORDER_API_TOKEN:-local-order-api-token}" \
    "$COMMANDS" "$USERS" "$INSTRUMENTS" "$CONCURRENCY" "$BATCH" 2>&1 \
    | grep -E 'transport=|accepted=|HTTP accept|end-to-end|batch HTTP|matched|order -> MySQL|peak lag'
