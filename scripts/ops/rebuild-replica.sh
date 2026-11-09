#!/usr/bin/env bash
# Rebuild one Raft replica whose data is lost or corrupt (it crash-loops on
# a checksum error, its disk was replaced, ...): it rejoins empty, marked as
# a same-id disk replacement (raft-rejoin.required), so it neither votes nor
# campaigns until the leader has sent it a snapshot plus the log tail; the
# node removes the marker itself once caught up.
#
#   rebuild-replica.sh GROUP DATA_ROOT SELF_METRICS PEER_METRICS... [--docker CONTAINER] [--yes]
#
#   GROUP         Raft group id
#   DATA_ROOT     raft-multi-node's data root on that host (the group's data is
#                 DATA_ROOT/group-GROUP)
#   SELF_METRICS  this replica's metrics address (host:port)
#   PEER_METRICS  the group's other replicas' metrics addresses
#   --docker C    run the host steps inside container C (local compose stack)
#   --yes         do it (without: check and print the plan only)
#
# Example (local stack, group 2 on raft-3):
#   scripts/ops/rebuild-replica.sh 2 /data localhost:9223 localhost:9221 localhost:9222 \
#       localhost:9224 localhost:9225 --docker trade-core-raft-3-1 --yes
#
# Safety: only while the OTHER replicas have a leader and a quorum, and only
# one replica of a group at a time. The old directory is kept as
# group-GROUP.broken-<time> for inspection; delete it once the replica is
# healthy again.
set -euo pipefail

[ $# -ge 4 ] || { sed -n '2,22p' "$0"; exit 2; }
GROUP=$1 DATA_ROOT=$2 SELF=$3
shift 3
PEERS=() CONTAINER="" YES=0
while [ $# -gt 0 ]; do
    case "$1" in
        --docker) CONTAINER=$2; shift 2 ;;
        --yes) YES=1; shift ;;
        *) PEERS+=("$1"); shift ;;
    esac
done
[ ${#PEERS[@]} -ge 1 ] || { echo "need the other replicas' metrics addresses" >&2; exit 2; }

metric() { # addr name -> value (empty when unreachable)
    curl -s --max-time 3 "http://$1/metrics" 2>/dev/null | awk -v n="$2" '$1 == n { print $2; exit }'
}

# 1. The rest of the group must be healthy without this replica.
total=$(( ${#PEERS[@]} + 1 ))
majority=$(( total / 2 + 1 ))
up=0 leader="" leader_applied=0
for peer in "${PEERS[@]}"; do
    role=$(metric "$peer" tc_raft_role)
    [ -n "$role" ] || continue
    up=$((up + 1))
    if [ "${role%.*}" = "2" ]; then
        leader=$peer
        leader_applied=$(metric "$peer" tc_raft_applied_index)
    fi
done
echo "group $GROUP: $up of ${#PEERS[@]} other replicas up, leader ${leader:-none}"
[ "$up" -ge "$majority" ] || { echo "refusing: the others have no quorum without this replica ($up < $majority)" >&2; exit 1; }
[ -n "$leader" ] || { echo "refusing: no leader among the other replicas" >&2; exit 1; }

STAMP=$(date +%Y%m%d-%H%M%S)
DIR="$DATA_ROOT/group-$GROUP"
# Move the data aside first (the running process keeps its open files in the
# moved directory), then kill this group's raft-node: the supervisor
# restarts it on an empty directory. No pgrep in the runtime image: scan /proc.
HOST_STEP="
set -e
[ -d '$DIR' ] || { echo 'no $DIR'; exit 1; }
pid=''
for p in /proc/[0-9]*; do
    if tr '\\0' ' ' < \"\$p/cmdline\" 2>/dev/null | grep -q -- 'raft-node .* $DIR '; then pid=\${p#/proc/}; fi
done
[ -n \"\$pid\" ] || { echo 'no raft-node running on $DIR'; exit 1; }
mv '$DIR' '$DIR.broken-$STAMP'
mkdir '$DIR'
: > '$DIR/raft-rejoin.required'
sync
kill -9 \"\$pid\"
echo \"moved $DIR to $DIR.broken-$STAMP, killed raft-node \$pid\"
"
if [ "$YES" -ne 1 ]; then
    echo "plan: move $DIR aside, restart group $GROUP's raft-node empty${CONTAINER:+ (in $CONTAINER)}, wait for it to catch up with $leader"
    echo "dry run: add --yes to do it"
    exit 0
fi
if [ -n "$CONTAINER" ]; then
    docker exec "$CONTAINER" sh -c "$HOST_STEP"
else
    sh -c "$HOST_STEP"
fi

# 2. Wait for it to come back and catch up, and for the node to clear its
#    rejoin marker (only then does it vote again).
marker_present() {
    if [ -n "$CONTAINER" ]; then
        docker exec "$CONTAINER" test -f "$DIR/raft-rejoin.required"
    else
        test -f "$DIR/raft-rejoin.required"
    fi
}
deadline=$((SECONDS + ${REBUILD_TIMEOUT_SECS:-900}))
while :; do
    sleep 5
    applied=$(metric "$SELF" tc_raft_applied_index)
    leader_applied=$(metric "$leader" tc_raft_applied_index)
    ready=$(metric "$SELF" tc_ready)
    rejoining=no
    marker_present && rejoining=yes
    echo "  applied ${applied:-down} / leader ${leader_applied:-?}, rejoining=$rejoining"
    if [ "$rejoining" = no ] && [ -n "$applied" ] && [ -n "$leader_applied" ] \
        && awk -v a="$applied" -v l="$leader_applied" 'BEGIN { exit !(a >= l - 100) }'; then
        echo "group $GROUP replica $SELF rebuilt and caught up (ready=${ready:-?})"
        echo "the old data is in $DIR.broken-$STAMP: delete it when no longer needed"
        exit 0
    fi
    [ "$SECONDS" -lt "$deadline" ] || { echo "not caught up within the timeout; check its logs" >&2; exit 1; }
done
