# trade-core Backup & Restore

Tools, consistency model, and procedures for backing up and restoring a
trade-core matching node's durable volume (and the MySQL order shards).

Scripts: `scripts/backup/backup-node.sh`, `scripts/backup/restore-node.sh`.

---

## What lives on a node volume

Each raft node volume (`${PREFIX}_raft-N-data`, mounted at `/data`) holds **all
four Raft groups** for that node. Group 0 is at `/data`; groups 1..3 at
`/data/group-N`. Per group:

```
<group root>/
  journal/
    journal-shard-0.bin      # command WAL, per-shard monotonic seq, FNV per record
    snapshot-shard-0.bin      # engine snapshot; embeds journal_seq it covers; FNV footer
    assets/
      asset-<id>.wal          # per-instrument WAL (portable, idempotent replay)
      asset-<id>.applied       # per-asset raft-index watermark
      raft-batches.applied     # applied multi-command raft-batch watermarks
  execution-outbox/
    outbox-shard-0.bin         # durable execution events (+ rotation segments)
    outbox-shard-0.published.cursor   # publisher progress (optimization only)
  raft.state                   # consensus log + hard state + last snapshot reference
```

All families are checksummed (FNV-1a) and their readers **stop at the first
torn/short record**, treating it as a clean prefix — exactly the condition a
live copy of an actively-appended file produces.

---

## Consistency model — why a HOT copy is safe (no stop/pause)

A running node is copied without pausing it. Correctness rests on two facts
proven in the source:

1. **Each file family self-reconciles at recovery.**
   - *journal-shard* — replay applies only records with `seq >
     snapshot.journal_seq` and stops at the first bad record
     (`exchange.rs::recover_stats`, `journal.rs::JournalReader`).
   - *asset WALs* — replay is idempotent: byte-identical duplicate command ids
     are skipped, conflicting ones hard-fail, torn tail = safe prefix
     (`asset_log.rs`).
   - *watermarks* (`*.applied`, `raft-batches.applied`) — append-only; a short
     read is EOF, so at worst one already-idempotent batch is re-applied.
   - *execution-outbox* — recovery **trims** every record past the last durable
     applied watermark and drops a torn trailing record
     (`execution_outbox.rs::truncate_after_applied`); a corrupt
     `.published.cursor` is treated as "unpublished" and the segment is kept.
   - *raft.state* — a torn tail record is truncated at open
     (`raft_log.rs::read_or_truncate_torn_tail`).

2. **Copy order: JOURNAL before SNAPSHOT (per shard).**
   `take_snapshot()` (`exchange.rs`) writes `snapshot-shard-N.bin` atomically
   (temp + fsync + rename) and **only then** truncates the journal. The
   snapshot's `journal_seq` is the replay cut point. Copying the journal first
   guarantees the snapshot we copy afterward is **≥** the journal head we
   already have:
   - If a snapshot+truncation cycle races the copy window, the snapshot read
     afterward is the *new* one (journal_seq ≥ our journal head); recovery loads
     it (state complete through journal_seq) and the older journal records are
     skipped by the seq filter. **No gap.**
   - If no cycle happens, the snapshot is the same/older pre-existing one and the
     copied journal tail is a contiguous run above `snapshot.journal_seq`.
     **No gap.**
   - Copying snapshot-first would be **unsafe**: an old snapshot paired with a
     post-truncation journal (which now starts *above* the old snapshot's seq)
     leaves a hole. Journal-first structurally cannot produce that.

   The remaining families are copied after the snapshot (order among them is
   irrelevant — each reconciles itself), and `raft.state` last so the consensus
   view is at least as new as the engine state it commits.

`backup-node.sh` implements exactly this order and then **proves** consistency
after the fact: `--verify` runs `journal-inspect verify` on every journal/WAL
(contiguous, checksum-clean) and re-checks the sha256 manifest.

**Production hardening.** For an audit-grade byte-identical replica, copy from a
filesystem/LVM/ZFS volume snapshot, or briefly `docker pause` the container
around the copy (matching is in-memory; clients see only added latency). Both
avoid even the (already-safe) hot-copy reasoning above. The provided script uses
hot copy so it can run against a busy node without perturbing it.

---

## backup-node.sh

```bash
export TC_PROJECT=kaishi-29a4a3          # compose project prefix (default)

# One node (all 4 groups), verified:
scripts/backup/backup-node.sh --node 1 --verify

# All five nodes + MySQL shards, verified, to a chosen dir:
scripts/backup/backup-node.sh --all --mysql --verify --out /backups/$(date -u +%FT%TZ)

# MySQL only:
scripts/backup/backup-node.sh --mysql-only
```

Output layout:
```
<out>/
  raft-1/
    data/            # exact mirror of the container /data (all groups)
    manifest.txt     # metadata + per-group /metrics anchors + sha256 of every file
  raft-2/ ...
  mysql/
    order-shard-0.sql.gz ...
    manifest.txt
```

`manifest.txt` records, per group, the recovery anchors captured at copy time
(`tc_journal_seq`, `tc_raft_applied_index`, `tc_raft_commit_index`,
`tc_raft_enqueued_index`) and a `sha256  bytes  relpath` line per file. These
anchors are what the restore compares against.

MySQL uses `mysqldump --single-transaction --source-data=2` (consistent InnoDB
snapshot, records binlog position, non-blocking). Credentials via
`TC_MYSQL_USER`/`TC_MYSQL_PASSWORD` (default root/root). **Production: prefer
Percona XtraBackup** for physical, fast-to-restore shard backups.

---

## restore-node.sh

Rebuilds a node volume from a backup and verifies it, **without touching any
running node** (fresh volume + throwaway container, backup mounted read-only).

```bash
# 1. Validation-only (safe anytime; make a habit of it on every backup):
scripts/backup/restore-node.sh --backup /backups/…/raft-1 --dry-run

# 2. Full restore to a NEW volume + boot-verify recovery:
scripts/backup/restore-node.sh --backup /backups/…/raft-1 --volume raft-1-data-restored

# 3. Populate a volume but skip the boot check:
scripts/backup/restore-node.sh --backup /backups/…/raft-1 --volume VOL --no-boot

# --force wipes a non-empty target volume; --port sets the verify /metrics port.
```

What it does:
1. **Manifest sha256 validation** — re-hashes every file, fails on mismatch/missing.
2. **journal-inspect verification** — one throwaway container loops over all
   journals/WALs (`verify`) and outbox segments (`dump --outbox`); fails on any
   gap or malformed record.
3. **Volume populate** — `cp -a` the data mirror into the target volume (must be
   empty unless `--force`).
4. **Boot verify** (default) — starts a single group-0 `raft-node` against the
   volume on an isolated port, waits for engine recovery, then reads `/metrics`
   and checks `tc_journal_seq` did not regress below the backup anchor. Stops and
   removes the container. *(A single-voter boot will not reach the original
   5-voter quorum, so it stays a follower; engine recovery — snapshot + journal
   tail + asset WAL replay — still runs and populates the metrics, which is what
   we assert. In production the restored volume is attached to its real member,
   which rejoins the live quorum.)*

### Restoring into the live cluster

To return a repaired/replaced node to service:
```bash
# a) Restore its volume from the latest verified backup:
scripts/backup/restore-node.sh --backup /backups/…/raft-3 --volume ${PREFIX}_raft-3-data --force --no-boot
# b) Start the member; it rejoins as a follower and the leader reconciles it
#    (log replication, or a shipped snapshot if its log was compacted away).
docker start ${PREFIX}-raft-3-1
```
For a brand-new member (hardware replacement) seed the volume the same way to
shorten catch-up, then do the ConfChange add/remove dance (runbook §7).

### Restoring MySQL

```bash
gunzip -c /backups/…/mysql/order-shard-3.sql.gz \
  | docker exec -i ${PREFIX}-mysql-order-3-1 mysql -uroot -proot
```
For point-in-time recovery, replay binlog from the `--source-data=2` position
recorded in the dump.

---

## Verifying a backup is real

A backup you have never restored is a hypothesis. Practice:
- `--verify` on **every** backup (journal-inspect + sha256).
- `restore-node.sh --dry-run` on every backup (independent re-validation).
- A full boot-verify restore to a scratch volume at least **weekly**, off the
  primary host.
