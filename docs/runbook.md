# trade-core Operations Runbook

Incident handling for the trade-core matching cluster. Organized by alert /
symptom. Each entry: **symptom** (which alert or `event=` log line fires),
**impact**, **procedure** (concrete commands), **verify recovery**.

## Topology & conventions

- **5 raft nodes** `kaishi-29a4a3-raft-{1..5}-1`, each hosting **4 independent
  Raft groups** (0..3). Group 0 uses `/data`; groups 1..3 use `/data/group-N`.
  Quorum per group = 3 of 5.
- **10 MySQL order shards** `kaishi-29a4a3-mysql-order-{0..9}-1` (external
  account/settlement system; trade-core only writes executions to them via the
  order-api consumer).
- **3 Redpanda (Kafka) brokers** `kaishi-29a4a3-redpanda[-2,-3]-1`.
- Per-group `/metrics` host port = `9200 + group*10 + node`
  (e.g. node 1 group 0 = `9201`, node 5 group 3 = `9235`).
- Set `PREFIX=kaishi-29a4a3` for the commands below.
- Persistence per group: `journal/journal-shard-N.bin` (command WAL, seq-ordered),
  `journal/snapshot-shard-N.bin` (engine snapshot, embeds `journal_seq`),
  `journal/assets/asset-*.wal` (+ `raft-batches.applied`, `asset-*.applied`
  watermarks), `execution-outbox/outbox-shard-N.bin` (+ `.published.cursor`),
  `raft.state` (consensus log + last snapshot ref). All are checksummed and
  torn-tail tolerant; see `docs/backup-restore.md` for the consistency model.

### Key metrics & their meaning
| metric | meaning |
|---|---|
| `tc_raft_role` | 0 follower, 1 candidate, 2 leader |
| `tc_raft_leader_id` | current leader (0 = no leader / election in progress) |
| `tc_raft_term` | consensus term (rising fast = repeated elections) |
| `tc_raft_commit_index` | highest quorum-committed entry |
| `tc_raft_applied_index` | highest entry fully applied to matching |
| `tc_raft_apply_lag` | committed-but-not-applied backlog |
| `tc_journal_seq` | per-shard command WAL head (total-order cursor) |
| `tc_execution_outbox_pending` | execution events durable but not yet acked by Kafka |
| `tc_execution_outbox_publish_healthy` | 1 = publisher healthy, 0 = failing |
| `tc_asset_wal_errors` | durability (fsync/append) failures — should stay 0 |
| `tc_order_dlq_total` | order-api messages dead-lettered (poison) |

### Fail-stop philosophy
The node is **fail-stop on durability loss**: a failed journal `sync_data`, an
outbox append error (disk full / EIO), or a corrupt file at startup **panics**
(`panic=abort` → container exit → `restart: unless-stopped` relaunches it). On
relaunch, recovery loads the last snapshot and replays the journal tail,
stopping at the first torn/checksum-bad record — so a fail-stop never corrupts
state, it only interrupts availability. Alerts below tell you which case you're in.

---

## 1. Raft minority failure (1–2 nodes down, quorum intact)

**Symptom.** 1 or 2 raft containers `Exited`/unreachable; surviving nodes still
elect a leader. `tc_raft_leader_id != 0` on the survivors; a peer shows
`tc_raft_transport_reconnects` climbing; `docker ps` shows the dead container.

**Impact.** **None to clients** — quorum (3/5) holds, matching continues.
Reduced fault tolerance (one more failure per affected group loses quorum).

**Procedure.**
```bash
# 1. Confirm quorum is healthy on a survivor (any group port).
curl -s localhost:9201/metrics | grep -E 'tc_raft_(role|leader_id|commit_index|apply_lag)'
# 2. Identify & inspect the dead node.
docker ps -a --filter "name=${PREFIX}-raft" --format '{{.Names}}\t{{.Status}}'
docker logs --tail 100 ${PREFIX}-raft-3-1
# 3. If it's a transient crash, just restart it — it rejoins and catches up
#    from the leader (log replication or a shipped snapshot).
docker start ${PREFIX}-raft-3-1
```

**Verify recovery.** On the restarted node, all four group `/metrics`
(`92X3` ports) show `tc_raft_leader_id != 0` and `tc_raft_commit_index`
converging to the leader's; `tc_raft_apply_lag → 0`. If the machine is dead and
won't return, treat it as **node replacement** (§7).

---

## 2. Raft majority failure (quorum lost)

**Symptom.** 3+ nodes down for a group. Survivors are stuck `candidate`
(`tc_raft_role=1`), `tc_raft_leader_id=0`, `tc_raft_term` rising, no commits
advancing. Clients see order acks stall/timeout.

**Impact.** **Full matching outage for affected groups.** No new commits until
quorum returns. Committed data is safe (durable on ≥3 or recoverable from disk).

**Procedure.** *Restoring the original members is strongly preferred over forcing
a new quorum — forcing can lose committed entries only present on down nodes.*
```bash
# 1. Determine which nodes are recoverable (disk intact?).
docker ps -a --filter "name=${PREFIX}-raft"
# 2. Bring back enough original members to reach 3/5. If their volumes survived,
#    just restart — durable raft.state + journals recover in place.
docker start ${PREFIX}-raft-2-1 ${PREFIX}-raft-4-1
# 3. If a node's DISK is lost, restore its volume from the latest backup FIRST
#    (§ docs/backup-restore.md), then start it. A restored member rejoins as a
#    follower and is reconciled by the leader.
```
**Do NOT** attempt to force a single survivor into a 1-node cluster unless you
accept losing any commits that only reached the down majority; that is a
last-resort data-loss recovery and must be signed off.

**Verify recovery.** A leader is elected (`tc_raft_leader_id != 0`),
`tc_raft_commit_index` advances on a test order, `tc_raft_apply_lag → 0`.

---

## 3. Single MySQL order shard failure

**Symptom.** `${PREFIX}-mysql-order-K-1` unhealthy/exited. order-api logs
`event=dlq_write_failed` or a spike in `tc_order_dlq_total`, and execution-persist
workers for shard K retry/back off. **Matching is unaffected** (MySQL is
downstream of the Kafka execution stream).

**Impact.** Executions for shard K are not persisted to MySQL. They remain
durable in the raft **execution-outbox** and on the Kafka topic; the order-api
consumer is at-least-once, so nothing is lost — persistence is delayed. Poison
messages that exhaust retries are appended to the **DLQ file** (fail-safe).

**Procedure.**
```bash
# 1. Confirm scope: only shard K, matching healthy.
docker ps --filter "name=${PREFIX}-mysql-order" --format '{{.Names}}\t{{.Status}}'
docker logs --tail 100 ${PREFIX}-mysql-order-K-1
# 2. Restart / repair the shard (or restore it from backup — see backup-restore.md).
docker start ${PREFIX}-mysql-order-K-1
# 3. The order-api consumer resumes from its committed Kafka offset and drains
#    the backlog automatically. Watch it catch up.
```

**Draining the DLQ.** The DLQ is an append-only file (env `TC_ORDER_DLQ_PATH`,
default `order-execution-dlq.wal`) written by order-api's `DlqWriter`. Format:
8-byte magic `TCDLQ01\0`, then per record `len:u32 | body | fnv1a:u64`, where
`body = ts_ns:u64 | partition:i32 | offset:i64 | reason_len:u32 | reason |
payload_len:u32 | payload`. A torn tail is dropped on read (checksum). The
`payload` is the original Kafka execution-event frame. To replay after fixing
the shard: parse the file, filter records whose `partition` maps to shard K, and
re-submit each `payload` to the order-api persist path (or hand to the
reprocessing tool). After a successful drain, archive the DLQ file and confirm
`tc_order_dlq_total` stops rising.

**Verify recovery.** Shard K `healthy`; consumer lag returns to ~0; row counts
for shard K resume advancing; no new `event=dlq_write_failed`.

---

## 4. Kafka (Redpanda) unavailable

**Symptom.** `tc_execution_outbox_publish_healthy=0` and
`tc_execution_outbox_pending` rising on raft nodes; execution-outbox logs
`event=read_failed` / publish failures; order-api consumers idle (no new
executions to persist).

**Impact.** **Matching is unaffected** — execution events keep being written
durably to the per-group execution-outbox on disk. Downstream (Kafka →
order-api → MySQL) is stalled. Risk is **outbox disk growth** if the outage is
long (segments rotate at `TC_EXECUTION_OUTBOX_ROTATE_BYTES`, default 128 MiB,
and are GC'd only once fully published).

**Procedure.**
```bash
# 1. Check brokers.
docker ps --filter "name=${PREFIX}-redpanda" --format '{{.Names}}\t{{.Status}}'
docker logs --tail 100 ${PREFIX}-redpanda-1
# 2. Restart the failed broker(s); Redpanda re-forms its own quorum.
docker start ${PREFIX}-redpanda-2-1
# 3. Watch outbox pending drain once brokers are back — the publisher resumes
#    from its .published.cursor (at-least-once; duplicates are deduped downstream
#    by (raft_group, raft_index, ordinal)).
watch -n2 "curl -s localhost:9201/metrics | grep -E 'tc_execution_outbox_(pending|publish_healthy)'"
# 4. If disk pressure is urgent before Kafka returns, see §5 (disk full).
```

**Verify recovery.** `tc_execution_outbox_publish_healthy=1`,
`tc_execution_outbox_pending → 0`; order-api consumer lag drains; MySQL row
counts resume.

---

## 5. Disk full (write path fail-stop)

**Symptom.** Journal-fsync logs `event=fsync_failed ... error=... — durability
window is growing` (repeated at 1st and every 60th failure) and/or
`tc_asset_wal_errors > 0`. If a synchronous write hits `ENOSPC`, the node
**panics** (`sync raft command journal` / `append execution report to durable
outbox`) and the container restart-loops (crash → `restart: unless-stopped` →
recover → same full disk → crash). `docker ps` shows a node `Restarting`.

**Impact.** Affected node/group is **down** (fail-stop protects against silent
data loss). Quorum may still hold if only one node's disk is full (see §1).

**Procedure.**
```bash
# 1. Confirm the volume that is full.
docker system df -v | grep raft
docker exec ${PREFIX}-raft-3-1 df -h /data   # if it stays up long enough
docker logs --tail 50 ${PREFIX}-raft-3-1 | grep -E 'fsync_failed|ENOSPC|No space'
# 2. Reclaim space on the Docker host (named volumes live under the docker root):
#    - prune dead containers/images:  docker system prune
#    - the biggest reclaimable in-app source is a long Kafka outage's outbox
#      backlog (see §4) — restoring Kafka lets fully-published segments GC.
# 3. If the app itself can't free enough, grow the underlying disk/volume, then
#    let the node restart-recover (or `docker start` it).
```
Recovery is safe: on restart the node replays snapshot + journal tail and stops
at the first torn record; the last (failed) write is simply not present.

**Verify recovery.** `df -h /data` has headroom; node stays `Up` (no restart
loop); `tc_asset_wal_errors` stops rising; `event=fsync_recovered` appears in
logs; commits advance again.

---

## 6. Data corruption (startup checksum panic)

**Symptom.** A node crash-loops at startup with a panic such as
`recover shard state`, `raft state checksum mismatch`, `raft commit index is
ahead of the durable log`, `unsupported version`, or `checksum mismatch` from
snapshot load. `docker logs` shows the panic + backtrace immediately after start.

**Impact.** That node/group cannot start. Others are unaffected if quorum holds.
This means a durable file is genuinely corrupt (bad sector, truncated snapshot,
version skew) — *not* a torn tail, which is handled transparently.

**Procedure.**
```bash
# 1. Read the exact panic to identify the file family.
docker logs --tail 60 ${PREFIX}-raft-3-1
# 2. Triage the suspect files offline with journal-inspect (runs in the image):
docker run --rm -v ${PREFIX}_raft-3-data:/data trade-core-node \
  'journal-inspect verify --path /data/journal/journal-shard-0.bin'
#   (a reported gap / non-contiguous => that WAL is damaged.)
# 3. Preferred fix: this node is one replica of an intact quorum. REBUILD it
#    from a peer/backup rather than hand-editing files:
#      - restore its volume from the latest good backup (docs/backup-restore.md),
#        OR wipe the volume and let it re-sync from the leader as a fresh member
#        (leader ships a snapshot + tail). Then `docker start` it.
# 4. NEVER delete individual records to "fix" a checksum error — that breaks the
#    total order. Replace the whole file family from a consistent source.
```
If the corruption is a snapshot but the journal is intact, restoring from backup
(snapshot + journal tail) reproduces identical state deterministically.

**Verify recovery.** Node starts without panic; `journal-inspect verify` is
clean; `tc_raft_commit_index`/`tc_journal_seq` converge to peers.

---

## 7. Node replacement (ConfChange add/remove)

**Symptom.** A machine is permanently lost, or you are rotating hardware. You
need to swap raft member X for a new member Y.

**Impact.** Planned. Done one single-step ConfChange at a time so quorum math is
always well-defined (raft_log.rs `add_node` / `remove_node` — committed through
the normal log, so each change is quorum-durable before it takes effect; the
operator endpoint is provided by the order/admin API).

**Procedure.** *Membership changes are single-step; never add and remove in one
shot.* To replace a dead node, **add the replacement first (restore quorum
strength), then remove the dead one** — or, to keep 5 voters, remove-then-add.
```bash
# 1. Provision the new host and bootstrap its container knowing the CURRENT
#    membership (so it votes/replicates coherently once added). Optionally seed
#    its volume from a recent backup to shorten catch-up (docs/backup-restore.md).
# 2. Add the new voter (single ConfChange) via the admin API on the leader:
#      add_node(new_id)   # RaftNode::add_node
#    Wait until it appears in the voter set and catches up (leader ships a
#    snapshot if its log was compacted past the follower's next index).
# 3. Remove the dead voter (single ConfChange):
#      remove_node(dead_id)   # RaftNode::remove_node
# 4. Decommission the old host.
```

**Verify recovery.** New member shows `tc_raft_commit_index` tracking the
leader and `tc_raft_apply_lag → 0` on all its groups; the dead id no longer
appears in the voter set; a test order commits under the new membership.

---

## 8. Version upgrade / rollback (rolling)

**Symptom.** Planned deploy of a new `trade-core-node` image.

**Impact.** Handled as a **rolling restart** one node at a time so quorum (and
therefore availability) is never lost. On-disk formats are versioned
(`JOURNAL_HEADER` TCJR, snapshot `TCS1` v3, outbox `TCEX`, raft `TCRF`); a
version bump is rejected at open with a "migration required" error rather than
misparsed — so verify format compatibility before rolling.

**Procedure.**
```bash
# 0. PRE-FLIGHT: take a fresh backup of every node (docs/backup-restore.md) and
#    the MySQL shards. Note current image id for rollback.
docker inspect --format '{{.Image}}' ${PREFIX}-raft-1-1

# 1. Build/pull the new image tagged trade-core-node.
# 2. Roll ONE node at a time; wait for full convergence before the next:
for n in 1 2 3 4 5; do
  docker compose -f docker-compose.raft.yml up -d --no-deps raft-$n   # recreate with new image
  # wait until this node's groups rejoin and lag drains before continuing:
  until curl -s localhost:920${n}/metrics | grep -q 'tc_raft_leader_id [1-9]'; do sleep 2; done
  # (repeat the check on 921$n/922$n/923$n for the other groups)
done

# ROLLBACK: if a node misbehaves on the new image, redeploy the previous image
# id for that node the same one-at-a-time way. Because state is on the durable
# volume (not the image), rollback is just running the old binary against the
# same /data.
```
**Fingerprint verification** (cross-version state equivalence): after upgrading a
node, confirm it recovered identical matching state by comparing per-asset WAL
fingerprints with a peer. `journal-inspect verify` proves each WAL is contiguous;
`journal-inspect diff --path A --path2 B` compares two copies of the same shard
journal for byte-level divergence. A replica whose replayed report fingerprint
(`AssetLogMeta.fingerprint`) matches its peers is confirmed equivalent.

**Verify recovery.** After each node: all 4 groups have a leader, `apply_lag→0`,
`tc_journal_seq` non-decreasing across the restart. After the full roll: a test
order commits and produces an execution end-to-end (matching → outbox → Kafka →
MySQL).

---

## 9. Backup strategy (recommendation)

See **`docs/backup-restore.md`** for the tools, the hot-copy consistency model,
and restore/verification steps. Recommended policy:

- **Frequency.** Per-node consistent hot backup (`backup-node.sh --all --mysql
  --verify`) **every 6 h**; MySQL shards additionally lean on binlog for
  point-in-time recovery between full dumps. The engine snapshot cadence
  (`TC_SNAPSHOT_EVERY_SECS=30`) already bounds journal-tail replay, so backups
  are cheap and recovery is fast.
- **Retention.** Keep hourly/6-hourly for 48 h, daily for 14 days, weekly for
  90 days (adjust to compliance requirements). Always keep the last **verified**
  backup (one that passed `--verify`) as the restore floor.
- **Off-site / 3-2-1.** Ship each backup to a second medium and an off-site /
  different-region object store (the backup directory is self-contained: data
  mirror + `manifest.txt` with sha256). Restores must be tested off the primary
  host. **A backup is not real until a restore drill has recovered from it** —
  run `restore-node.sh --dry-run` on every backup and a full boot-verify restore
  at least weekly.
- **What to back up.** All 5 raft volumes (each covers its 4 groups) + all 10
  MySQL shards. The raft volume is the source of truth for matching; MySQL is
  the downstream account/settlement store (also externally owned).
- **Production hardening.** For audit-grade physical copies, back up from a
  filesystem/LVM/ZFS volume snapshot or a brief `docker pause`, and use Percona
  XtraBackup for MySQL. The provided scripts use hot copy + `mysqldump
  --single-transaction`, which is consistent and non-blocking for this scale.
