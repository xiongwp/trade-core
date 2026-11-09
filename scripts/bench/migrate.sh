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

# A Mac that sleeps mid-run pauses the Docker VM and the client: Kafka
# sessions and timers break and the run hangs or reports garbage. Keep the
# machine awake for the whole run (macOS only; re-exec once).
if command -v caffeinate >/dev/null 2>&1 && [ -z "${TC_CAFFEINATED:-}" ]; then
    exec env TC_CAFFEINATED=1 caffeinate -dimsu "$0" "$@"
fi

cd "$(dirname "$0")/../.."
COMMANDS="${1:-2000000}"
RAFT_GROUPS="${TC_RAFT_GROUPS:-4}"
NODES=5
TOKEN="${TC_ORDER_API_TOKEN:-local-order-api-token}"
COMPOSE=(docker compose -p trade-core -f docker-compose.yml -f docker-compose.raft.yml)
# MULTI=1: three order-api instances (docker-compose.multi.yml); the client
# spreads over all of them and keeps going while one is down.
ORDER_ADDRS=127.0.0.1:9200
ORDERS=(trade-core-order-1)
if [ -n "${MULTI:-}" ]; then
    COMPOSE+=(-f docker-compose.multi.yml)
    ORDER_ADDRS=127.0.0.1:9200,127.0.0.1:9250,127.0.0.1:9260
    ORDERS=(trade-core-order-1 trade-core-order-2-1 trade-core-order-3-1)
fi

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
for order in "${ORDERS[@]}"; do
    until docker ps --format '{{.Names}} {{.Status}}' | grep -q "$order .*(healthy)"; do
        [ "$SECONDS" -lt "$deadline" ] || { echo "$order not healthy" >&2; exit 1; }
        sleep 2
    done
done
sleep 10
echo "== ready, image $(docker inspect trade-core-order-1 --format '{{.Image}}' | cut -c8-19)"

# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
cargo bench --bench chaos_e2e -q -- "$ORDER_ADDRS" "$TOKEN" "$COMMANDS" 2>&1 | sed -l 's/^/   /' &
CLIENT=$!

# Admin port of order-api instance N; always the single instance without MULTI.
api_port() { case "${MULTI:+$1}" in 2) echo 9250 ;; 3) echo 9260 ;; *) echo 9200 ;; esac; }

migrate() { # at-second category to-group [instance]
    local at=$1 category=$2 to=$3 port
    port=$(api_port "${4:-1}")
    sleep $((at - (SECONDS - T0))) 2>/dev/null || true
    kill -0 "$CLIENT" 2>/dev/null || return 0
    echo "== +$((SECONDS - T0))s migrate category $category -> group $to (via :$port)"
    curl -s --max-time 300 -X POST -H "Authorization: Bearer $TOKEN" \
        "localhost:$port/admin/routes/migrate?category=$category&to=$to" | sed 's/^/   /'
    echo
}
# An orchestrator crash mid-migration: order-api is killed about a second
# into moving category 3, so the category stays frozen (and possibly sealed
# or installed) until /admin/routes/abort on the restarted instance rolls it
# back. Held orders for it are resent by the client until it thaws.
crash_mid_migration() { # at-second category to-group
    local at=$1 category=$2 to=$3
    sleep $((at - (SECONDS - T0))) 2>/dev/null || true
    kill -0 "$CLIENT" 2>/dev/null || return 0
    echo "== +$((SECONDS - T0))s migrate category $category -> group $to, kill -9 order-api mid-way"
    curl -s --max-time 120 -X POST -H "Authorization: Bearer $TOKEN" \
        "localhost:9200/admin/routes/migrate?category=$category&to=$to" >/dev/null 2>&1 &
    sleep "${CRASH_AFTER:-1}"
    docker kill -s KILL trade-core-order-1 >/dev/null
    "${COMPOSE[@]}" start order >/dev/null 2>&1
    until docker ps --format '{{.Names}} {{.Status}}' | grep -q 'trade-core-order-1 .*(healthy)'; do sleep 1; done
    # With MULTI, another instance rolls it back, from the route log alone.
    local abort_port
    abort_port=$(api_port 2)
    echo "== +$((SECONDS - T0))s abort category $category (via :$abort_port)"
    curl -s --max-time 300 -X POST -H "Authorization: Bearer $TOKEN" \
        "localhost:$abort_port/admin/routes/abort?category=$category" | sed 's/^/   /'
    echo
}
T0=$SECONDS
(
    migrate 20 1 2 2
    migrate 45 2 3 3
    crash_mid_migration 55 3 0
    migrate 80 1 1 3
) &
MIGRATIONS=$!
STATUS=0
wait "$CLIENT" || STATUS=$?
wait "$MIGRATIONS" 2>/dev/null || true
exit "$STATUS"
