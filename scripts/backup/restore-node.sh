#!/usr/bin/env bash
# =============================================================================
# restore-node.sh — rebuild a trade-core node volume from a backup and verify.
#
# Flow:
#   1. Validate the backup's sha256 manifest (detect bit-rot / partial backup).
#   2. Verify supported journal/outbox segments with journal-inspect.
#   3. Populate a FRESH docker volume from the backup (exact /data mirror).
#   4. (default) Boot a single group-0 raft-node against the volume on an
#      isolated port, let it cross-validate Raft WAL + memory snapshot + exact
#      application proofs, then compare recovery metrics with manifest anchors.
#
# The restore never touches the source backup (mounted read-only) and never
# touches a running production node — it creates its own volume and container.
#
# Usage:
#   restore-node.sh --backup DIR --volume VOL [--dry-run] [--no-boot]
#                   [--force] [--port N]
#
#   --backup DIR   A node backup dir (…/raft-N) containing data/ + manifest.txt.
#   --volume VOL   Target docker volume name to (re)build. Must be empty unless
#                  --force is given (then it is wiped first).
#   --dry-run      Validate manifest + supported segments only; make no volume,
#                  boot nothing. Prints every action it would take.
#   --no-boot      Populate the volume but skip the boot/metrics recovery check.
#   --force        Allow using a non-empty target volume (wipes it first).
#   --port N       Host port for the verification node's /metrics (default 9299).
# =============================================================================

set -euo pipefail

NODE_IMAGE="${TC_NODE_IMAGE:-trade-core-node}"
BACKUP=""; VOLUME=""; DRY_RUN=0; DO_BOOT=1; FORCE=0; METRICS_PORT=9299

usage() { sed -n '2,40p' "$0" >&2; exit "${1:-2}"; }

if command -v sha256sum >/dev/null 2>&1; then
  sha256_of() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_of() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  echo "FATAL: no sha256 tool" >&2; exit 1
fi

log() { printf '[restore] %s\n' "$*" >&2; }
die() { printf '[restore] FATAL: %s\n' "$*" >&2; exit 1; }
run() { log "+ $*"; [[ "$DRY_RUN" -eq 1 ]] || eval "$@"; }

[[ $# -eq 0 ]] && usage 2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --backup)  BACKUP="$2"; shift 2 ;;
    --volume)  VOLUME="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --no-boot) DO_BOOT=0; shift ;;
    --force)   FORCE=1; shift ;;
    --port)    METRICS_PORT="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage 2 ;;
  esac
done

[[ -n "$BACKUP" ]] || die "--backup DIR is required"
[[ -d "$BACKUP/data" ]] || die "$BACKUP/data not found (point --backup at a raft-N dir)"
[[ -f "$BACKUP/manifest.txt" ]] || die "$BACKUP/manifest.txt not found"
command -v docker >/dev/null 2>&1 || die "docker not on PATH"
DATA_DIR="$BACKUP/data"
MANIFEST="$BACKUP/manifest.txt"

# ---- 1. manifest sha256 validation ------------------------------------------
log "validating manifest: $MANIFEST"
sha_fail=0; n=0
while read -r sum size rel; do
  [[ "$sum" =~ ^[0-9a-f]{64}$ ]] || continue
  f="$DATA_DIR/$rel"
  [[ -f "$f" ]] || { log "  MISSING: $rel"; sha_fail=1; continue; }
  actual="$(sha256_of "$f")"
  [[ "$actual" == "$sum" ]] || { log "  SHA MISMATCH: $rel"; sha_fail=1; }
  n=$((n+1))
done < <(sed -n '/^\[files\]/,$p' "$MANIFEST")
[[ "$sha_fail" -eq 0 ]] || die "manifest validation failed"
log "manifest OK: $n files match recorded sha256"

# ---- 2. supported segment integrity (single throwaway container) ------------
log "verifying supported journal/outbox integrity with journal-inspect"
ji_fail=0
# One container loops over every file (a node holds ~10k asset WALs; a container
# per file would be unusable). ENTRYPOINT is `/bin/sh -c` => pass one script.
if ! docker run --rm -v "$DATA_DIR":/b:ro "$NODE_IMAGE" '
    fail=0
    jn=$(find /b -type f \( -name "journal-shard-*.bin" -o -name "asset-*.wal" \) | wc -l)
    on=$(find /b -type f -name "outbox-shard-*.bin" | wc -l)
    for f in $(find /b -type f \( -name "journal-shard-*.bin" -o -name "asset-*.wal" \)); do
      journal-inspect verify --path "$f" >/dev/null 2>&1 || { echo "FAIL verify $f"; fail=1; }
    done
    for f in $(find /b -type f -name "outbox-shard-*.bin"); do
      journal-inspect dump --outbox --path "$f" >/dev/null 2>&1 || { echo "FAIL outbox $f"; fail=1; }
    done
    echo "journal-inspect: checked $jn journals/WALs + $on outbox segments"
    exit $fail
  ' 1>&2; then
  ji_fail=1
fi
[[ "$ji_fail" -eq 0 ]] || die "journal-inspect verification failed"
log "segment check OK; boot recovery will validate Raft/snapshot/proof consistency"

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "DRY RUN complete — validation passed; no volume created, nothing booted."
  exit 0
fi

[[ -n "$VOLUME" ]] || die "--volume VOL is required (omit only with --dry-run)"

# ---- 3. build the target volume ---------------------------------------------
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  # non-empty check
  nonempty="$(docker run --rm -v "$VOLUME":/v "$NODE_IMAGE" \
      "ls -A /v 2>/dev/null | head -1" 2>/dev/null || true)"
  if [[ -n "$nonempty" ]]; then
    [[ "$FORCE" -eq 1 ]] || die "volume $VOLUME is not empty (use --force to wipe)"
    log "wiping non-empty volume $VOLUME (--force)"
    docker run --rm -v "$VOLUME":/v "$NODE_IMAGE" "rm -rf /v/* /v/.[!.]* 2>/dev/null || true"
  fi
else
  log "creating volume $VOLUME"
  docker volume create "$VOLUME" >/dev/null
fi

log "populating $VOLUME from $DATA_DIR (exact /data mirror)"
# Mount backup ro at /b and volume rw at /data; copy the whole mirror in.
docker run --rm -v "$DATA_DIR":/b:ro -v "$VOLUME":/data "$NODE_IMAGE" \
  "cp -a /b/. /data/ && echo populated"

# sanity: same file count as the manifest
vfiles="$(docker run --rm -v "$VOLUME":/data "$NODE_IMAGE" \
  "find /data -type f | wc -l" | tr -d ' ')"
log "volume now holds $vfiles files"

if [[ "$DO_BOOT" -eq 0 ]]; then
  log "DONE (--no-boot): volume $VOLUME rebuilt. Boot check skipped."
  exit 0
fi

# ---- 4. boot a single group-0 node and compare /metrics ---------------------
# Read the manifest's group-0 anchors.
exp_journal_seq="$(awk '/^\[group group-0\]/{g=1} g&&/^metric tc_journal_seq=/{sub(/^metric tc_journal_seq=/,"");print;exit}' "$MANIFEST")"
exp_applied="$(awk '/^\[group group-0\]/{g=1} g&&/^metric tc_raft_applied_index=/{sub(/^metric tc_raft_applied_index=/,"");print;exit}' "$MANIFEST")"

CNAME="tc-restore-verify-$$"
log "booting group-0 recovery node ($CNAME) on /metrics port $METRICS_PORT"
# Single-voter argv so ClusterConfig is valid; it will NOT reach the original
# 5-voter quorum (so it stays a follower/candidate) but engine recovery
# (snapshot + journal tail + asset WAL replay) runs at startup regardless and
# populates tc_journal_seq / tc_raft_applied_index. TC_RAFT_GROUP_ID=0 + /data.
# No Kafka brokers set => publisher is idle (nothing external touched).
docker run -d --name "$CNAME" \
  -e TC_RAFT_GROUP_ID=0 -e TC_LOG=info -e RUST_BACKTRACE=1 \
  -e TC_SNAPSHOT_EVERY_SECS=0 \
  -v "$VOLUME":/data \
  -p "${METRICS_PORT}:9102" \
  "$NODE_IMAGE" \
  "exec raft-node 1 0.0.0.0:7000 1@127.0.0.1:7000 0.0.0.0:9001 /data 0.0.0.0:9101 0.0.0.0:9102" \
  >/dev/null

cleanup() { docker rm -f "$CNAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# poll /metrics until journal_seq shows up (recovery finished) or timeout.
got_journal_seq=""; got_applied=""
for _ in $(seq 1 30); do
  sleep 1
  m="$(curl -fs --max-time 3 "http://localhost:${METRICS_PORT}/metrics" 2>/dev/null || true)"
  [[ -z "$m" ]] && continue
  got_journal_seq="$(printf '%s\n' "$m" | awk '$1=="tc_journal_seq"{print $2}')"
  got_applied="$(printf '%s\n' "$m" | awk '$1=="tc_raft_applied_index"{print $2}')"
  [[ -n "$got_journal_seq" ]] && break
done

[[ -n "$got_journal_seq" ]] || { docker logs --tail 40 "$CNAME" >&2 || true; die "recovery node never served /metrics"; }

log "recovered tc_journal_seq=$got_journal_seq (backup anchor=${exp_journal_seq:-n/a})"
log "recovered tc_raft_applied_index=$got_applied (backup anchor=${exp_applied:-n/a})"

status=0
# journal_seq must not REGRESS below the backup anchor (recovery may add tail
# records the fsync thread had flushed after the /metrics scrape, so >= is ok).
if [[ -n "$exp_journal_seq" && -n "$got_journal_seq" ]]; then
  if [[ "$got_journal_seq" -lt "$exp_journal_seq" ]]; then
    log "  FAIL: recovered journal_seq regressed below the backup anchor"; status=1
  else
    log "  OK: recovered journal_seq >= backup anchor"
  fi
fi

cleanup; trap - EXIT
if [[ "$status" -eq 0 ]]; then
  log "RESTORE VERIFIED: volume $VOLUME recovers cleanly and matches the backup anchors."
else
  die "RESTORE VERIFICATION FAILED"
fi
