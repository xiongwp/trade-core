#!/usr/bin/env bash
# Crash test from clean data: kill nodes with SIGKILL while orders flow, bring
# them back, then check end-to-end consistency (benches/chaos_e2e.rs).
#
#   scripts/bench/chaos.sh                 # 1M commands, default schedule
#   IMAGE=trade-core-node:candidate scripts/bench/chaos.sh 2000000
#
# Faults (seconds after load starts), each a hard kill like a power cut:
#   +15  raft-1 (a whole matching machine: every group's replica on it) ... +40 back
#   +55  order (order-api: intake, Raft forwarding, projections)        ... +65 back
#   +80  raft-2                                                         ... +100 back
#   +110 redpanda-2 (a Kafka broker)                                    ... +130 back
#
# Destroys local test data by design. Never point it at a real deployment.
set -euo pipefail

cd "$(dirname "$0")/../.."
COMMANDS="${1:-1000000}"
RAFT_GROUPS="${TC_RAFT_GROUPS:-4}"
NODES=5
COMPOSE=(docker compose -p trade-core -f docker-compose.yml -f docker-compose.raft.yml)

[ -n "${IMAGE:-}" ] && docker tag "$IMAGE" trade-core-node:latest
echo "== wiping the local stack (volumes included)"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
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
deadline=$((SECONDS + 300))
until [ "$(ready_replicas)" = "$((NODES * RAFT_GROUPS))" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "replicas not ready" >&2; exit 1; }
    sleep 3
done
until docker ps --format '{{.Names}} {{.Status}}' | grep -q 'trade-core-order-1 .*(healthy)'; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "order not healthy" >&2; exit 1; }
    sleep 2
done
sleep 10
echo "== ready, image $(docker inspect trade-core-order-1 --format '{{.Image}}' | cut -c8-19)"

# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
cargo bench --bench chaos_e2e -q -- 127.0.0.1:9200 "${TC_ORDER_API_TOKEN:-local-order-api-token}" \
    "$COMMANDS" 2>&1 | sed 's/^/   /' &
CLIENT=$!

fault() { # at-second action container
    local at=$1 action=$2 name=$3
    sleep $((at - (SECONDS - T0))) 2>/dev/null || true
    kill -0 "$CLIENT" 2>/dev/null || return 0
    echo "== +$((SECONDS - T0))s $action $name"
    docker "$action" "$name" >/dev/null 2>&1 || true
}
T0=$SECONDS
(
    fault 15 kill trade-core-raft-1-1
    fault 40 start trade-core-raft-1-1
    fault 55 kill trade-core-order-1
    fault 65 start trade-core-order-1
    fault 80 kill trade-core-raft-2-1
    fault 100 start trade-core-raft-2-1
    fault 110 kill trade-core-redpanda-2-1
    fault 130 start trade-core-redpanda-2-1
) &
FAULTS=$!
STATUS=0
wait "$CLIENT" || STATUS=$?
kill "$FAULTS" 2>/dev/null || true
# Bring everything back for inspection whatever happened.
"${COMPOSE[@]}" up -d >/dev/null 2>&1 || true
exit "$STATUS"
