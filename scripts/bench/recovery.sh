#!/usr/bin/env bash
# Recovery drill from clean data: while orders flow, use the operator scripts
# in scripts/ops to recover from a lost replica and an abandoned migration,
# then check end-to-end consistency (benches/chaos_e2e.rs) and health.
#
#   scripts/bench/recovery.sh [COMMANDS]      (default 1M)
#
#   +20s  group 1 on raft-2: its data directory is wiped, the replica is
#         rebuilt with scripts/ops/rebuild-replica.sh (snapshot + log tail)
#   +45s  a migration of category 3 starts and order-api is killed 2.6 s in;
#         scripts/ops/abort-migrations.sh rolls it back after the restart
#   end   scripts/ops/health.py must report no failures
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
COMMANDS="${1:-1000000}"
RAFT_GROUPS="${TC_RAFT_GROUPS:-4}"
NODES=5
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
cargo bench --bench chaos_e2e -q -- "$ORDER_ADDRS" "${TC_ORDER_API_TOKEN:-local-order-api-token}" \
    "$COMMANDS" 2>&1 | sed -l 's/^/   /' &
CLIENT=$!

fault() { # at-second action container
    local at=$1 action=$2 name=$3
    sleep $((at - (SECONDS - T0))) 2>/dev/null || true
    kill -0 "$CLIENT" 2>/dev/null || return 0
    echo "== +$((SECONDS - T0))s $action $name"
    docker "$action" "$name" >/dev/null 2>&1 || true
}
T0=$SECONDS
at() { sleep $(($1 - (SECONDS - T0))) 2>/dev/null || true; }
(
    at 20
    echo "== +$((SECONDS - T0))s group 1 on raft-2 loses its data: rebuild-replica.sh"
    scripts/ops/rebuild-replica.sh 1 /data localhost:9212 localhost:9211 localhost:9213 localhost:9214 localhost:9215 \
        --docker trade-core-raft-2-1 --yes 2>&1 | sed 's/^/   /' | grep -v "^   *applied" || true
    at 45
    echo "== +$((SECONDS - T0))s migrate category 3 -> group 0, kill order-api mid-way"
    curl -s --max-time 120 -X POST -H "Authorization: Bearer ${TC_ORDER_API_TOKEN:-local-order-api-token}" \
        "localhost:9200/admin/routes/migrate?category=3&to=0" >/dev/null 2>&1 &
    sleep 2.6
    docker kill -s KILL trade-core-order-1 >/dev/null
    "${COMPOSE[@]}" start order >/dev/null 2>&1
    until docker ps --format '{{.Names}} {{.Status}}' | grep -q 'trade-core-order-1 .*(healthy)'; do sleep 1; done
    echo "== +$((SECONDS - T0))s abort-migrations.sh"
    scripts/ops/abort-migrations.sh localhost:9200 --yes 2>&1 | sed 's/^/   /'
) &
DRILL=$!
STATUS=0
wait "$CLIENT" || STATUS=$?
wait "$DRILL" 2>/dev/null || true
echo "== health"
# Warnings (exit 1, e.g. a lagging replica still catching up) are reported;
# only failures (exit 2) fail the drill.
health=0
python3 scripts/ops/health.py --quiet > /tmp/tc-health.$$ 2>&1 || health=$?
sed 's/^/   /' /tmp/tc-health.$$; rm -f /tmp/tc-health.$$
[ "$health" -lt 2 ] || STATUS=1
exit "$STATUS"
