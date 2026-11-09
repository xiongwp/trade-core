#!/usr/bin/env bash
# Live category migration from clean data: move categories between Raft
# groups while orders flow, then check end-to-end consistency with
# benches/chaos_e2e.rs (no lost / double-applied / over-filled orders,
# identical replicas).
#
#   scripts/bench/migrate.sh                 # 2M commands
#   IMAGE=trade-core-node:candidate scripts/bench/migrate.sh 1000000
#
# Destroys local test data by design. Never point it at a real deployment.
set -euo pipefail

cd "$(dirname "$0")/../.."
COMMANDS="${1:-2000000}"
RAFT_GROUPS="${TC_RAFT_GROUPS:-4}"
NODES=5
TOKEN="${TC_ORDER_API_TOKEN:-local-order-api-token}"
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
cargo bench --bench chaos_e2e -q -- 127.0.0.1:9200 "$TOKEN" "$COMMANDS" 2>&1 | sed -l 's/^/   /' &
CLIENT=$!

migrate() { # at-second category to-group
    local at=$1 category=$2 to=$3
    sleep $((at - (SECONDS - T0))) 2>/dev/null || true
    kill -0 "$CLIENT" 2>/dev/null || return 0
    echo "== +$((SECONDS - T0))s migrate category $category -> group $to"
    curl -s --max-time 300 -X POST -H "Authorization: Bearer $TOKEN" \
        "localhost:9200/admin/routes/migrate?category=$category&to=$to" | sed 's/^/   /'
    echo
}
T0=$SECONDS
(
    migrate 20 1 2
    migrate 45 2 3
    migrate 70 1 1
) &
MIGRATIONS=$!
STATUS=0
wait "$CLIENT" || STATUS=$?
wait "$MIGRATIONS" 2>/dev/null || true
exit "$STATUS"
