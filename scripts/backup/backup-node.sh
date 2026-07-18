#!/usr/bin/env bash
# =============================================================================
# backup-node.sh — consistent HOT backup of a trade-core Raft matching node.
#
# Produces a point-in-time-consistent copy of one (or --all) raft node's
# persistent volume, plus an optional mysqldump of the 10 order shards, together
# with a sha256 manifest and the source /metrics anchors used to verify a
# restore.
#
# -----------------------------------------------------------------------------
# CONSISTENCY MODEL (why a hot copy is safe — no pause/stop required)
# -----------------------------------------------------------------------------
# A node's /data tree holds several INDEPENDENTLY crash-recoverable file
# families. Each already tolerates the exact conditions a live `docker cp`
# reproduces — a reader observing bytes mid-append:
#
#   * journal-shard-N.bin  — per-shard command WAL. JournalReader stops at the
#     first torn/checksum-bad record (journal.rs), so a half-copied tail record
#     is dropped, never applied. Recovery = load snapshot, then replay journal
#     records with seq > snapshot.journal_seq (exchange.rs recover_stats).
#   * snapshot-shard-N.bin — engine snapshot; embeds the journal_seq it covers,
#     protected by an FNV footer validated by snapshot::load (corrupt => the
#     node refuses to start, never loads garbage).
#   * journal/assets/asset-*.wal — per-instrument WALs. Replay is idempotent:
#     byte-identical duplicate command ids are skipped, and a torn tail is a
#     safe prefix (asset_log.rs). Independent of the shard journal.
#   * journal/assets/raft-batches.applied + asset-*.applied — append-only
#     watermarks; a short/torn read is treated as EOF, so at worst recovery
#     re-applies one already-idempotent batch.
#   * execution-outbox/* — recovery trims every record past the last durable
#     applied watermark and drops a torn trailing record
#     (execution_outbox.rs truncate_after_applied); the .published.cursor is an
#     optimization (a corrupt cursor is treated as "unpublished").
#   * raft.state — consensus log + last application-snapshot reference. A torn
#     tail record is truncated at open (raft_log.rs read_or_truncate_torn_tail).
#
# The ONE ordering constraint that matters is between a shard's journal and its
# snapshot, because the snapshot's journal_seq acts as the replay cut point:
#
#   take_snapshot() (exchange.rs) writes snapshot-shard-N.bin ATOMICALLY
#   (temp + fsync + rename) and ONLY THEN truncates journal-shard-N.bin.
#
# Therefore we copy the JOURNAL FIRST, then the SNAPSHOT:
#
#   - If a snapshot+truncation cycle races our copy window, the snapshot we read
#     afterward is the NEW one, whose journal_seq >= the head of the journal we
#     already copied. Recovery loads that snapshot (state complete through its
#     journal_seq) and the older journal records (all <= journal_seq) are simply
#     skipped by the seq filter. No gap.
#   - If no cycle happens, the snapshot is the same/older pre-existing one and
#     the journal tail we copied is a contiguous run above snapshot.journal_seq.
#     No gap.
#
#   Copying SNAPSHOT-first would be UNSAFE: an old snapshot paired with a
#   post-truncation journal (which now starts ABOVE the old snapshot's seq)
#   leaves a hole between them. journal-first structurally cannot produce that.
#
# The other families carry no cross-file ordering requirement (each reconciles
# itself at recovery via watermark / dedup / torn-tail handling), so we copy
# them after the snapshot, and raft.state last so the consensus view is at least
# as new as the engine state it commits.
#
# PRODUCTION NOTE (freezing): for an audit-grade, byte-identical replica you may
# instead briefly `docker pause` the container around the copy (a few hundred ms
# — matching is in-memory, so no client sees more than added latency), or take a
# filesystem/LVM/ZFS snapshot of the volume and copy from that. This script does
# NOT pause: on this shared bench another agent is running an acceptance load and
# a pause would pollute its measurements. The hot copy above is provably
# consistent for recovery, and --verify proves it after the fact with
# journal-inspect.
#
# MySQL: --mysql uses `mysqldump --single-transaction` per shard, which takes a
# consistent InnoDB snapshot without locking writers. PRODUCTION: prefer
# Percona XtraBackup (physical, non-blocking, far faster to restore for large
# shards) or a read replica; mysqldump is fine for these bench-sized shards.
# =============================================================================

set -euo pipefail

# ---- configuration ----------------------------------------------------------
PROJECT="${TC_PROJECT:-kaishi-29a4a3}"
NODE_IMAGE="${TC_NODE_IMAGE:-trade-core-node}"
DEFAULT_OUT_ROOT="${TC_BACKUP_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/backups}"
MYSQL_SHARDS="${TC_MYSQL_SHARDS:-10}"
MYSQL_USER="${TC_MYSQL_USER:-root}"
MYSQL_PASSWORD="${TC_MYSQL_PASSWORD:-root}"

usage() {
  cat >&2 <<'EOF'
Usage:
  backup-node.sh --node N   [--out DIR] [--mysql] [--verify]
  backup-node.sh --all      [--out DIR] [--mysql] [--verify]
  backup-node.sh --mysql-only [--out DIR] [--verify]

Options:
  --node N       Back up raft node N (1..5).
  --all          Back up all five raft nodes.
  --mysql        Also mysqldump the order shards (--single-transaction).
  --mysql-only   Back up only the MySQL shards, no raft nodes.
  --out DIR      Destination directory (default: <repo>/backups/<UTC-timestamp>).
  --verify       After copying, run journal-inspect verify on every journal /
                 asset WAL / outbox in the backup and re-check the sha256 manifest.
  -h, --help     This help.

Environment overrides:
  TC_PROJECT (compose project prefix, default kaishi-29a4a3),
  TC_NODE_IMAGE, TC_BACKUP_ROOT, TC_MYSQL_SHARDS,
  TC_MYSQL_USER, TC_MYSQL_PASSWORD.
EOF
  exit "${1:-2}"
}

# ---- sha256 helper (darwin ships shasum; linux ships sha256sum) --------------
if command -v sha256sum >/dev/null 2>&1; then
  sha256_of() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_of() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  echo "FATAL: neither sha256sum nor shasum found" >&2; exit 1
fi

log()  { printf '[backup] %s\n' "$*" >&2; }
die()  { printf '[backup] FATAL: %s\n' "$*" >&2; exit 1; }

# host metrics port for (node, group): compose maps 920N.. / 921N.. etc.
# internal metrics_port = 9102 + group*10 (raft_multi_node.rs);
# host port            = 9200 + group*10 + node (docker-compose.raft.yml).
metrics_host_port() { echo $(( 9200 + $2 * 10 + $1 )); }

# ---- argument parsing --------------------------------------------------------
NODES=()
DO_MYSQL=0
MYSQL_ONLY=0
DO_VERIFY=0
OUT=""

[[ $# -eq 0 ]] && usage 2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --node)       NODES+=("$2"); shift 2 ;;
    --all)        NODES=(1 2 3 4 5); shift ;;
    --mysql)      DO_MYSQL=1; shift ;;
    --mysql-only) MYSQL_ONLY=1; DO_MYSQL=1; shift ;;
    --verify)     DO_VERIFY=1; shift ;;
    --out)        OUT="$2"; shift 2 ;;
    -h|--help)    usage 0 ;;
    *)            echo "unknown argument: $1" >&2; usage 2 ;;
  esac
done

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT:-$DEFAULT_OUT_ROOT/$TS}"
mkdir -p "$OUT"
log "backup destination: $OUT"

command -v docker >/dev/null 2>&1 || die "docker not on PATH"

container_running() { docker ps --format '{{.Names}}' | grep -qx "$1"; }

# =============================================================================
# Raft node backup
# =============================================================================
backup_raft_node() {
  local node="$1"
  local container="${PROJECT}-raft-${node}-1"
  container_running "$container" || die "container $container is not running"

  local node_dir="$OUT/raft-${node}"
  local data_dir="$node_dir/data"          # exact mirror of the container /data
  local manifest="$node_dir/manifest.txt"
  mkdir -p "$data_dir"
  log "node $node: source=$container -> $data_dir"

  # Discover groups. Group 0 lives at /data root; group M at /data/group-M.
  # (raft_multi_node.rs: group 0 uses data_root, others data_root/group-M.)
  local groups=(0)
  while IFS= read -r g; do
    [[ -n "$g" ]] && groups+=("$g")
  done < <(docker exec "$container" sh -c \
      'ls -d /data/group-* 2>/dev/null | sed "s#.*/group-##"' | sort -n)

  {
    echo "# trade-core node backup manifest"
    echo "schema=1"
    echo "backup_started_utc=$TS"
    echo "project=$PROJECT"
    echo "node=$node"
    echo "source_container=$container"
    echo "groups=${groups[*]}"
  } > "$manifest"

  local group
  for group in "${groups[@]}"; do
    local src dst tag
    if [[ "$group" == "0" ]]; then
      src="/data"; dst="$data_dir"; tag="group-0"
    else
      src="/data/group-${group}"; dst="$data_dir/group-${group}"; tag="group-${group}"
    fi
    mkdir -p "$dst/journal/assets" "$dst/execution-outbox"
    log "node $node $tag: copying (journal -> snapshot -> assets -> outbox -> raft.state)"

    # ---- 1. journal shards FIRST (see consistency model above) --------------
    local f base
    for f in $(docker exec "$container" sh -c "ls $src/journal/journal-shard-*.bin 2>/dev/null || true"); do
      base="$(basename "$f")"
      docker cp -q "$container:$f" "$dst/journal/$base" 2>/dev/null \
        || docker cp "$container:$f" "$dst/journal/$base"
    done
    # ---- 2. snapshots AFTER the journal (snapshot.journal_seq >= journal head)
    for f in $(docker exec "$container" sh -c "ls $src/journal/snapshot-shard-*.bin 2>/dev/null || true"); do
      base="$(basename "$f")"
      docker cp "$container:$f" "$dst/journal/$base"
    done
    # ---- 3. per-asset WALs + watermarks (self-reconciling on replay) --------
    #        (journal/assets also holds raft-batches.applied and *.applied)
    if docker exec "$container" sh -c "[ -d $src/journal/assets ]"; then
      docker cp "$container:$src/journal/assets/." "$dst/journal/assets/"
    fi
    # ---- 4. execution outbox (recovery trims past the durable watermark) ----
    if docker exec "$container" sh -c "[ -d $src/execution-outbox ]"; then
      docker cp "$container:$src/execution-outbox/." "$dst/execution-outbox/"
    fi
    # ---- 5. raft.state LAST (consensus view >= committed engine state) ------
    if docker exec "$container" sh -c "[ -f $src/raft.state ]"; then
      docker cp "$container:$src/raft.state" "$dst/raft.state"
    fi

    # ---- record the /metrics recovery anchors for this group ----------------
    local port; port="$(metrics_host_port "$node" "$group")"
    {
      echo ""
      echo "[group $tag]"
      echo "metrics_host_port=$port"
    } >> "$manifest"
    local metrics
    if metrics="$(curl -fs --max-time 5 "http://localhost:${port}/metrics" 2>/dev/null)"; then
      local m
      for m in tc_journal_seq tc_raft_applied_index tc_raft_commit_index tc_raft_enqueued_index; do
        local v; v="$(printf '%s\n' "$metrics" | awk -v k="$m" '$1==k {print $2}')"
        [[ -n "$v" ]] && echo "metric ${m}=${v}" >> "$manifest"
      done
    else
      echo "metric _unavailable=1  # /metrics not reachable at backup time" >> "$manifest"
    fi
  done

  # ---- sha256 every copied file, paths relative to data/ --------------------
  {
    echo ""
    echo "[files]  # <sha256>  <bytes>  <relpath-under-data>"
  } >> "$manifest"
  local rel size sum
  while IFS= read -r -d '' f; do
    rel="${f#"$data_dir"/}"
    size="$(wc -c < "$f" | tr -d ' ')"
    sum="$(sha256_of "$f")"
    echo "$sum  $size  $rel" >> "$manifest"
  done < <(find "$data_dir" -type f -print0 | sort -z)

  local nfiles; nfiles="$(find "$data_dir" -type f | wc -l | tr -d ' ')"
  log "node $node: $nfiles files backed up, manifest -> $manifest"
}

# =============================================================================
# MySQL shard backup (mysqldump --single-transaction)
# =============================================================================
backup_mysql() {
  local out_dir="$OUT/mysql"
  mkdir -p "$out_dir"
  local i container
  for (( i=0; i<MYSQL_SHARDS; i++ )); do
    container="${PROJECT}-mysql-order-${i}-1"
    if ! container_running "$container"; then
      log "mysql shard $i: $container not running, SKIP"
      continue
    fi
    local dump="$out_dir/order-shard-${i}.sql.gz"
    log "mysql shard $i: mysqldump --single-transaction -> $dump"
    # --single-transaction => consistent InnoDB snapshot without blocking writers.
    # --routines/--triggers/--events for a complete schema; --source-data=2 pins
    # the binlog position as a comment for point-in-time recovery alignment.
    docker exec "$container" sh -c \
      "exec mysqldump --single-transaction --routines --triggers --events \
         --source-data=2 --all-databases \
         -u'${MYSQL_USER}' -p'${MYSQL_PASSWORD}' 2>/dev/null" \
      | gzip > "$dump" \
      || { log "mysql shard $i: dump FAILED (check credentials TC_MYSQL_USER/PASSWORD)"; rm -f "$dump"; continue; }
    local sum size
    size="$(wc -c < "$dump" | tr -d ' ')"
    sum="$(sha256_of "$dump")"
    printf '%s  %s  order-shard-%s.sql.gz\n' "$sum" "$size" "$i" >> "$out_dir/manifest.txt"
  done
  log "mysql: manifest -> $out_dir/manifest.txt"
}

# =============================================================================
# Verification: journal-inspect over the backup + manifest re-check
# =============================================================================
verify_backup() {
  local node="$1"
  local node_dir="$OUT/raft-${node}"
  local data_dir="$node_dir/data"
  log "node $node: verifying journal integrity (journal-inspect) + sha256 manifest"

  # journal-inspect runs inside ONE throwaway container (the node image ships
  # the binary) that loops over every journal/asset/outbox file internally —
  # spawning a container per file would mean 10k+ launches. ENTRYPOINT is
  # `/bin/sh -c`, so we hand it a single script string. A node's WALs are
  # journal-format (`verify` = checksum + contiguity); outbox segments are
  # validated by range-scanning them with `dump --outbox`.
  local ji_fail=0
  if ! docker run --rm -v "$data_dir":/b:ro "$NODE_IMAGE" '
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

  # sha256 manifest re-check.
  local manifest="$node_dir/manifest.txt" sum size rel2 actual sha_fail=0 n=0
  while read -r sum size rel2; do
    [[ "$sum" =~ ^[0-9a-f]{64}$ ]] || continue
    actual="$(sha256_of "$data_dir/$rel2")"
    if [[ "$actual" != "$sum" ]]; then
      log "  SHA MISMATCH: $rel2"; sha_fail=1
    fi
    n=$((n+1))
  done < <(sed -n '/^\[files\]/,$p' "$manifest")

  if [[ "$ji_fail" -eq 0 && "$sha_fail" -eq 0 ]]; then
    log "node $node: VERIFY OK ($n files hashed, all journals contiguous)"
  else
    die "node $node: VERIFY FAILED (see messages above)"
  fi
}

# =============================================================================
# main
# =============================================================================
if [[ "$MYSQL_ONLY" -eq 0 ]]; then
  [[ ${#NODES[@]} -eq 0 ]] && { echo "no --node/--all given" >&2; usage 2; }
  for n in "${NODES[@]}"; do
    backup_raft_node "$n"
    [[ "$DO_VERIFY" -eq 1 ]] && verify_backup "$n"
  done
fi
[[ "$DO_MYSQL" -eq 1 ]] && backup_mysql

log "DONE. Backup at: $OUT"
