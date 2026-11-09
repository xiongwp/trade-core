#!/usr/bin/env bash
# Roll back every unfinished category migration (its orchestrating order-api
# stopped mid-way, so the category stays frozen and refuses orders). Works
# through any order-api instance, from the shared route log; safe to repeat.
#
#   abort-migrations.sh [ORDER_API_ADDR] [--yes]      (default localhost:9200)
#   token: TC_ORDER_API_TOKEN (default the local stack's)
set -euo pipefail
ADDR=localhost:9200 YES=0
for arg in "$@"; do
    case "$arg" in --yes) YES=1 ;; *) ADDR=$arg ;; esac
done
TOKEN=${TC_ORDER_API_TOKEN:-local-order-api-token}
# The instance may be restarting (the orchestrator that died is often this
# one): wait for it rather than failing at once.
rows=""
for _ in $(seq 1 ${ABORT_WAIT_SECS:-60}); do
    rows=$(curl -sf --max-time 10 -H "Authorization: Bearer $TOKEN" "http://$ADDR/admin/routes/unfinished") && break
    rows=""
    sleep 1
done
[ -n "$rows" ] || { echo "cannot reach $ADDR/admin/routes/unfinished" >&2; exit 1; }
categories=$(printf '%s' "$rows" | python3 -c 'import json,sys; print(" ".join(str(r["category"]) for r in json.load(sys.stdin)["migrations"]))')
if [ -z "$categories" ]; then
    echo "no unfinished migrations"
    exit 0
fi
printf '%s\n' "$rows" | python3 -c 'import json,sys
for r in json.load(sys.stdin)["migrations"]: print("category %(category)s: %(from)s -> %(to)s, epoch %(epoch)s" % r)'
if [ "$YES" -ne 1 ]; then
    echo "dry run: add --yes to roll these back"
    exit 0
fi
status=0
for category in $categories; do
    echo "== abort category $category"
    curl -s --max-time 300 -X POST -H "Authorization: Bearer $TOKEN" \
        "http://$ADDR/admin/routes/abort?category=$category" || status=1
    echo
done
exit $status
