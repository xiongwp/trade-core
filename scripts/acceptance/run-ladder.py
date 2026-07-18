#!/usr/bin/env python3
"""One-shot acceptance harness for the trade-core capacity ladder.

Drives the load generator (`order_batch_e2e`), scrapes `/metrics` from the
order API and every Raft-group replica, waits for backlog to drain after each
stage, judges PASS/FAIL against the conditions in
`docs/production-deployment-5m-tps.md` §12, and emits per-stage CSV plus a
Markdown report. Python 3.8+ standard library only.

Stage targets, durations and pass conditions are table-driven (`LADDER`) and
mirror the production doc. `--scale` / `--duration-scale` shrink targets and
durations for local smoke runs; `--smoke` sets sensible small defaults so the
whole ladder (including the leader-kill fault stage) runs on one machine in a
few minutes and exercises every code path in the harness.

See docs/acceptance-runbook.md for the real-machine procedure and how to read
the report.
"""

import argparse
import csv
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# --------------------------------------------------------------------------
# SLO thresholds (production doc §1). Latencies in milliseconds.
# --------------------------------------------------------------------------
SLO = {
    "raft_commit_p99_ms": 20.0,      # Raft quorum commit p99 <= 20 ms
    "kafka_to_match_p99_ms": 100.0,  # Kafka -> match complete p99 <= 100 ms
    "kafka_to_mysql_p99_ms": 500.0,  # Kafka -> MySQL visible p99 <= 500 ms
    "rto_seconds": 10.0,             # single-node failover RTO <= 10 s
}


@dataclass
class Stage:
    name: str
    tps: int                     # production target commands/s
    minutes: float               # production sustain minutes
    resource_max: Optional[float]  # fraction (0..1) or None (not gated)
    enforce_latency: bool        # gate latency SLOs this stage
    kind: str = "normal"         # "normal" | "capacity" | "fault"
    note: str = ""


# Production capacity ladder, doc §12. Order is authoritative for a full run.
LADDER: List[Stage] = [
    Stage("warmup", 500_000, 15, None, False, "normal",
          "lag drains to zero, no errors"),
    Stage("stage1", 1_000_000, 30, 0.50, True, "normal",
          "p99 SLOs met, resource < 50%"),
    Stage("stage2", 3_000_000, 30, 0.65, True, "normal",
          "no sustained lag, resource < 65%"),
    Stage("stage3", 5_000_000, 60, 0.70, True, "normal",
          "all SLOs met, resource < 70%"),
    Stage("capacity", 7_150_000, 15, None, False, "capacity",
          "no dropped orders, no OOM, full catch-up after"),
    Stage("fault", 5_000_000, 15, None, False, "fault",
          "RPO=0, RTO <= 10 s on leader kill"),
]
LADDER_BY_NAME = {s.name: s for s in LADDER}


# --------------------------------------------------------------------------
# Prometheus text scraping
# --------------------------------------------------------------------------
_METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)")


class Sample:
    """Parsed Prometheus exposition text: name -> list of (labels, value)."""

    def __init__(self, text: str):
        self.series: Dict[str, List[Tuple[str, float]]] = {}
        for line in text.splitlines():
            if not line or line[0] == "#":
                continue
            m = _METRIC_RE.match(line)
            if not m:
                continue
            name, labels, value = m.group(1), m.group(2) or "", m.group(3)
            try:
                self.series.setdefault(name, []).append((labels, float(value)))
            except ValueError:
                pass

    def first(self, name: str, default: float = math.nan) -> float:
        vals = self.series.get(name)
        return vals[0][1] if vals else default

    def max(self, name: str, default: float = math.nan) -> float:
        vals = self.series.get(name)
        return max(v for _, v in vals) if vals else default


def scrape(url: str, timeout: float = 4.0) -> Optional[Sample]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return Sample(resp.read().decode("utf-8", "replace"))
    except (urllib.error.URLError, OSError, ValueError):
        return None


@dataclass
class Endpoint:
    url: str
    node: int   # physical node index (1-based); 0 if unknown/order-api
    group: int  # raft group index; -1 for the order API


def build_raft_endpoints(host: str, nodes: int, groups: int, base: int) -> List[Endpoint]:
    """Host port = base + group*10 + node (matches docker-compose.raft.yml)."""
    eps = []
    for n in range(1, nodes + 1):
        for g in range(groups):
            port = base + g * 10 + n
            eps.append(Endpoint(f"http://{host}:{port}/metrics", n, g))
    return eps


def parse_explicit_endpoints(spec: str) -> List[Endpoint]:
    """Comma list of `url` or `url@node:group` entries."""
    out = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        node, group = 0, -1
        if "@" in item:
            url, meta = item.rsplit("@", 1)
            if ":" in meta:
                n, g = meta.split(":", 1)
                node, group = int(n), int(g)
        else:
            url = item
        if not url.endswith("/metrics"):
            url = url.rstrip("/") + "/metrics"
        out.append(Endpoint(url, node, group))
    return out


# --------------------------------------------------------------------------
# Cluster-wide observation snapshot
# --------------------------------------------------------------------------
@dataclass
class Snapshot:
    t: float
    # order API counters / gauges
    published: float = math.nan
    mysql_completed: float = math.nan
    match_completed: float = math.nan
    mysql_lag: float = math.nan
    match_lag: float = math.nan
    ingress_backlog: float = math.nan
    bp_rejections: float = math.nan
    dlq: float = math.nan
    # raft aggregate
    max_apply_lag: float = math.nan
    leader_outbox_pending: float = math.nan   # summed over group leaders
    groups_with_leader: int = 0
    groups_total: int = 0
    endpoints_up: int = 0
    endpoints_total: int = 0
    # latency p99 (ms), max across raft endpoints
    raft_commit_p99_ms: float = math.nan
    match_p99_ms: float = math.nan
    command_p99_ms: float = math.nan
    wal_fsync_p99_ms: float = math.nan
    # per-group leader node index (group -> node), for fault analysis
    leaders: Dict[int, int] = field(default_factory=dict)


def observe(order_metrics_url: str, raft_eps: List[Endpoint]) -> Snapshot:
    snap = Snapshot(t=time.time())
    o = scrape(order_metrics_url)
    if o:
        snap.published = o.first("tc_order_published_commands")
        snap.mysql_completed = o.first("tc_order_mysql_completed_commands")
        snap.match_completed = o.first("tc_order_match_completed_commands")
        snap.mysql_lag = o.first("tc_order_mysql_consumer_lag")
        snap.match_lag = o.first("tc_order_match_consumer_lag")
        snap.ingress_backlog = o.first("tc_order_ingress_backlog")
        snap.bp_rejections = o.first("tc_order_backpressure_rejections")
        snap.dlq = o.first("tc_order_dlq_total")

    # per group, remember every replica's outbox + who is leader
    per_group_leader_outbox: Dict[int, float] = {}
    group_has_leader: Dict[int, bool] = {}
    apply_lags: List[float] = []
    commit_p99: List[float] = []
    match_p99: List[float] = []
    cmd_p99: List[float] = []
    fsync_p99: List[float] = []
    groups_seen = set()
    up = 0
    for ep in raft_eps:
        groups_seen.add(ep.group)
        s = scrape(ep.url)
        if s is None:
            continue
        up += 1
        role = s.first("tc_raft_role", 0.0)
        outbox = s.first("tc_execution_outbox_pending", math.nan)
        apply_lag = s.first("tc_raft_apply_lag", math.nan)
        if not math.isnan(apply_lag):
            apply_lags.append(apply_lag)
        if role >= 2.0:  # leader
            group_has_leader[ep.group] = True
            snap.leaders[ep.group] = ep.node
            if not math.isnan(outbox):
                per_group_leader_outbox[ep.group] = outbox
        # p99 gauges are nanoseconds
        for name, bucket in (
            ("tc_raft_commit_ns_p99", commit_p99),
            ("tc_match_ns_p99", match_p99),
            ("tc_command_latency_ns_p99", cmd_p99),
            ("tc_wal_fsync_ns_p99", fsync_p99),
        ):
            v = s.max(name, math.nan)
            if not math.isnan(v):
                bucket.append(v / 1e6)  # ns -> ms

    snap.endpoints_total = len(raft_eps)
    snap.endpoints_up = up
    snap.groups_total = len(groups_seen)
    snap.groups_with_leader = sum(1 for g in groups_seen if group_has_leader.get(g))
    snap.max_apply_lag = max(apply_lags) if apply_lags else math.nan
    snap.leader_outbox_pending = (
        sum(per_group_leader_outbox.values()) if per_group_leader_outbox else math.nan
    )
    snap.raft_commit_p99_ms = max(commit_p99) if commit_p99 else math.nan
    snap.match_p99_ms = max(match_p99) if match_p99 else math.nan
    snap.command_p99_ms = max(cmd_p99) if cmd_p99 else math.nan
    snap.wal_fsync_p99_ms = max(fsync_p99) if fsync_p99 else math.nan
    return snap


def backlog_drained(snap: Snapshot) -> bool:
    """All order-API lags, ingress backlog, raft apply lag and every group
    leader's execution outbox are zero. NaN (unreachable) does not count as
    drained."""
    checks = [
        snap.mysql_lag, snap.match_lag, snap.ingress_backlog,
        snap.max_apply_lag, snap.leader_outbox_pending,
    ]
    for c in checks:
        if math.isnan(c) or c > 0:
            return False
    # every group must currently have a leader publishing
    return snap.groups_total > 0 and snap.groups_with_leader == snap.groups_total


# --------------------------------------------------------------------------
# Load generation
# --------------------------------------------------------------------------
@dataclass
class RoundResult:
    accepted: int = 0
    errors: int = 0
    throughput: float = 0.0
    p50: float = math.nan
    p95: float = math.nan
    p99: float = math.nan
    exit_code: int = 0


_ACC_RE = re.compile(r"accepted=(\d+),\s*errors=(\d+).*?throughput=([\d.]+)")
_LAT_RE = re.compile(r"p50=([\d.]+)ms\s+p95=([\d.]+)ms\s+p99=([\d.]+)ms")


def run_bench_round(bench_bin: str, addr: str, token: str, orders: int,
                    conc: int, assets: int, batch: int,
                    timeout: float) -> RoundResult:
    cmd = [bench_bin, addr, token, str(orders), str(conc), str(assets), str(batch)]
    res = RoundResult()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        res.exit_code = -1
        res.errors = orders
        sys.stderr.write(f"    [load] round timed out after {timeout:.0f}s\n")
        if e.stdout:
            _parse_bench_stdout(e.stdout, res)
        return res
    res.exit_code = proc.returncode
    _parse_bench_stdout(proc.stdout, res)
    if proc.returncode != 0 and res.accepted == 0 and res.errors == 0:
        # bench crashed before printing counters
        res.errors = orders
        tail = (proc.stderr or "").strip().splitlines()[-2:]
        sys.stderr.write(f"    [load] bench exit {proc.returncode}: {' '.join(tail)}\n")
    return res


def _parse_bench_stdout(text: str, res: RoundResult) -> None:
    m = _ACC_RE.search(text)
    if m:
        res.accepted = int(m.group(1))
        res.errors = int(m.group(2))
        res.throughput = float(m.group(3))
    m = _LAT_RE.search(text)
    if m:
        res.p50, res.p95, res.p99 = (float(m.group(i)) for i in (1, 2, 3))


# --------------------------------------------------------------------------
# Resource sampling (optional, coarse; docker stats)
# --------------------------------------------------------------------------
def sample_resource(prefix: str) -> Tuple[float, float]:
    """Return (max_cpu_pct_per_core, max_mem_pct) over containers whose name
    starts with `prefix`. CPU% can exceed 100 (per-core). Best-effort."""
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}} {{.CPUPerc}} {{.MemPerc}}"],
            capture_output=True, text=True, timeout=15,
        ).stdout
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return (math.nan, math.nan)
    max_cpu = max_mem = math.nan
    for line in out.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[0].startswith(prefix):
            continue
        try:
            cpu = float(parts[1].rstrip("%"))
            mem = float(parts[2].rstrip("%"))
        except ValueError:
            continue
        max_cpu = cpu if math.isnan(max_cpu) else max(max_cpu, cpu)
        max_mem = mem if math.isnan(max_mem) else max(max_mem, mem)
    return (max_cpu, max_mem)


# --------------------------------------------------------------------------
# Stage result
# --------------------------------------------------------------------------
@dataclass
class Check:
    name: str
    status: str          # PASS | FAIL | WARN | NA
    detail: str = ""


@dataclass
class StageResult:
    stage: Stage
    target_tps: float
    duration_s: float
    orders_budget: int
    accepted: int = 0
    errors: int = 0
    load_throughput: float = 0.0
    load_p99_ms: float = math.nan
    catchup_s: float = math.nan
    checks: List[Check] = field(default_factory=list)
    # fault-specific
    rto_s: float = math.nan
    reelected_groups: int = 0
    max_cpu: float = math.nan
    max_mem: float = math.nan
    published_delta: float = math.nan
    mysql_delta: float = math.nan
    match_delta: float = math.nan

    @property
    def verdict(self) -> str:
        if any(c.status == "FAIL" for c in self.checks):
            return "FAIL"
        if any(c.status == "WARN" for c in self.checks):
            return "PASS (warnings)"
        return "PASS"


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------
class Harness:
    def __init__(self, args):
        self.args = args
        self.order_metrics_url = args.metrics_order
        if args.metrics_endpoints:
            self.raft_eps = parse_explicit_endpoints(args.metrics_endpoints)
        else:
            self.raft_eps = build_raft_endpoints(
                args.raft_host, args.raft_nodes, args.raft_groups, args.raft_port_base)
        os.makedirs(args.output, exist_ok=True)
        self.results: List[StageResult] = []

    # ---- scaling ----------------------------------------------------------
    def stage_plan(self, stage: Stage) -> Tuple[float, float, int]:
        tps = stage.tps * self.args.scale
        duration = stage.minutes * 60.0 * self.args.duration_scale
        if self.args.smoke:
            duration = self.args.smoke_seconds
        orders = max(1, int(round(tps * duration)))
        return tps, duration, orders

    # ---- CSV --------------------------------------------------------------
    def _csv_writer(self, stage_name: str):
        path = os.path.join(self.args.output, f"metrics-{stage_name}.csv")
        f = open(path, "w", newline="")
        w = csv.writer(f)
        w.writerow([
            "wall_ts", "elapsed_s", "phase",
            "published", "mysql_completed", "match_completed",
            "mysql_lag", "match_lag", "ingress_backlog",
            "bp_rejections", "dlq",
            "max_apply_lag", "leader_outbox_pending",
            "groups_with_leader", "groups_total", "endpoints_up", "endpoints_total",
            "raft_commit_p99_ms", "match_p99_ms", "command_p99_ms", "wal_fsync_p99_ms",
            "max_cpu_pct", "max_mem_pct",
        ])
        return f, w, path

    def _csv_row(self, w, start: float, snap: Snapshot, phase: str,
                 cpu: float, mem: float):
        w.writerow([
            f"{snap.t:.3f}", f"{snap.t - start:.1f}", phase,
            _n(snap.published), _n(snap.mysql_completed), _n(snap.match_completed),
            _n(snap.mysql_lag), _n(snap.match_lag), _n(snap.ingress_backlog),
            _n(snap.bp_rejections), _n(snap.dlq),
            _n(snap.max_apply_lag), _n(snap.leader_outbox_pending),
            snap.groups_with_leader, snap.groups_total,
            snap.endpoints_up, snap.endpoints_total,
            _f(snap.raft_commit_p99_ms), _f(snap.match_p99_ms),
            _f(snap.command_p99_ms), _f(snap.wal_fsync_p99_ms),
            _f(cpu), _f(mem),
        ])

    # ---- one stage --------------------------------------------------------
    def run_stage(self, stage: Stage) -> StageResult:
        tps, duration, orders_budget = self.stage_plan(stage)
        r = StageResult(stage=stage, target_tps=tps, duration_s=duration,
                        orders_budget=orders_budget)
        print(f"\n=== stage {stage.name}: target {tps:,.0f} cmd/s for "
              f"{duration:.0f}s (~{orders_budget:,} orders) ===")

        base = observe(self.order_metrics_url, self.raft_eps)
        if base.endpoints_up == 0:
            print("  WARNING: no raft metrics endpoints reachable")
        pub0, my0, mt0 = base.published, base.mysql_completed, base.match_completed
        dlq0 = base.dlq if not math.isnan(base.dlq) else 0.0

        f, w, csv_path = self._csv_writer(stage.name)
        start = time.time()
        deadline = start + duration
        round_secs = self.args.round_seconds
        round_orders = max(1, int(round(tps * round_secs)))
        emitted = 0
        last_sample = 0.0
        max_cpu = max_mem = math.nan
        fault_done = False
        fault_at = None
        reelect_deadline = None

        # fault target selection
        killed_node = None
        fault_cmd = None
        recover_cmd = None
        pre_fault_leaders: Dict[int, int] = {}
        if stage.kind == "fault":
            killed_node, fault_cmd, recover_cmd, pre_fault_leaders = \
                self._plan_fault(base)
            print(f"  fault: will run `{fault_cmd}` at stage midpoint "
                  f"(pre-fault leaders: {pre_fault_leaders})")

        def sample(phase: str):
            nonlocal max_cpu, max_mem
            snap = observe(self.order_metrics_url, self.raft_eps)
            cpu = mem = math.nan
            if self.args.resource_prefix:
                cpu, mem = sample_resource(self.args.resource_prefix)
                max_cpu = _nanmax(max_cpu, cpu)
                max_mem = _nanmax(max_mem, mem)
            self._csv_row(w, start, snap, phase, cpu, mem)
            f.flush()
            return snap

        # -------- load phase --------
        while time.time() < deadline and emitted < orders_budget:
            now = time.time()
            if now - last_sample >= self.args.sample_interval:
                sample("load")
                last_sample = now
            # fault injection at midpoint
            if stage.kind == "fault" and not fault_done and now >= start + duration / 2:
                print(f"  >>> injecting fault: {fault_cmd}")
                self._run_cmd(fault_cmd)
                fault_at = time.time()
                fault_done = True
                reelect_deadline = fault_at + max(60.0, SLO["rto_seconds"] * 6)

            chunk = min(round_orders, orders_budget - emitted)
            rr = run_bench_round(self.args.bench_bin, self.args.order_api,
                                 self.args.token, chunk, self.args.concurrency,
                                 self.args.assets, self.args.batch,
                                 timeout=round_secs * 20 + 60)
            r.accepted += rr.accepted
            r.errors += rr.errors
            emitted += chunk
            if not math.isnan(rr.throughput):
                r.load_throughput = rr.throughput
            if not math.isnan(rr.p99):
                r.load_p99_ms = rr.p99
            # pace to constant rate
            spent = time.time() - now
            if spent < round_secs:
                time.sleep(round_secs - spent)

        # -------- re-election timing (fault) --------
        if stage.kind == "fault" and fault_at is not None:
            r.rto_s, r.reelected_groups = self._await_reelection(
                fault_at, reelect_deadline, pre_fault_leaders, killed_node, sample)

        # -------- catch-up phase --------
        print("  load done; waiting for backlog to drain...")
        catchup_start = time.time()
        catchup_deadline = catchup_start + self.args.catchup_timeout
        drained = False
        while time.time() < catchup_deadline:
            snap = sample("catchup")
            if backlog_drained(snap):
                drained = True
                break
            time.sleep(self.args.sample_interval)
        if drained:
            r.catchup_s = time.time() - catchup_start

        final = sample("final")
        f.close()
        print(f"  metrics CSV -> {csv_path}")

        # -------- recover phase (fault) --------
        if stage.kind == "fault" and recover_cmd:
            print(f"  >>> recovering node: {recover_cmd}")
            self._run_cmd(recover_cmd)
            rejoined = self._await_rejoin(killed_node)
            r.checks.append(Check(
                "node rejoin",
                "PASS" if rejoined else "WARN",
                "killed node endpoints reachable again"
                if rejoined else "killed node did not rejoin within timeout"))

        r.max_cpu, r.max_mem = max_cpu, max_mem
        r.published_delta = _delta(final.published, pub0)
        r.mysql_delta = _delta(final.mysql_completed, my0)
        r.match_delta = _delta(final.match_completed, mt0)

        self._judge(stage, r, final, drained, dlq0)
        return r

    # ---- fault helpers ----------------------------------------------------
    def _plan_fault(self, base: Snapshot):
        # pick node leading the most groups unless overridden
        if self.args.fault_node:
            node = self.args.fault_node
        else:
            counts: Dict[int, int] = {}
            for g, n in base.leaders.items():
                counts[n] = counts.get(n, 0) + 1
            node = max(counts, key=counts.get) if counts else 1
        prefix = self.args.fault_container_prefix
        fault_cmd = self.args.fault_cmd or f"docker kill {prefix}{node}"
        recover_cmd = self.args.fault_recover_cmd or f"docker start {prefix}{node}"
        # if an explicit fault-cmd names a container, infer node for exclusion
        if self.args.fault_cmd and not self.args.fault_node:
            m = re.search(r"raft-(\d+)", self.args.fault_cmd)
            if m:
                node = int(m.group(1))
        return node, fault_cmd, recover_cmd, dict(base.leaders)

    def _await_reelection(self, fault_at, deadline, pre_leaders, killed_node,
                          sample) -> Tuple[float, int]:
        """Wait until every group again has a leader among reachable replicas.
        RTO = time from fault to full leadership restoration."""
        # groups whose leader was on the killed node need a new leader
        affected = {g for g, n in pre_leaders.items() if n == killed_node}
        print(f"  measuring RTO; groups needing re-election: "
              f"{sorted(affected) if affected else 'none (killed node led no group)'}")
        while time.time() < deadline:
            snap = observe(self.order_metrics_url, self.raft_eps)
            if snap.groups_total > 0 and snap.groups_with_leader == snap.groups_total:
                return (time.time() - fault_at, len(affected))
            time.sleep(1.0)
        return (math.nan, len(affected))

    def _await_rejoin(self, killed_node) -> bool:
        eps = [e for e in self.raft_eps if e.node == killed_node]
        if not eps:
            return True
        deadline = time.time() + self.args.catchup_timeout
        while time.time() < deadline:
            if all(scrape(e.url) is not None for e in eps):
                return True
            time.sleep(2.0)
        return False

    def _run_cmd(self, cmd: str):
        if self.args.dry_run_faults:
            print(f"    [dry-run] would run: {cmd}")
            return
        try:
            subprocess.run(cmd, shell=True, timeout=30,
                           capture_output=True, text=True)
        except subprocess.SubprocessError as e:
            sys.stderr.write(f"    fault command failed: {e}\n")

    # ---- judgement --------------------------------------------------------
    def _judge(self, stage: Stage, r: StageResult, final: Snapshot,
               drained: bool, dlq0: float):
        # 1. no errors
        r.checks.append(Check(
            "no load errors", "PASS" if r.errors == 0 else "FAIL",
            f"accepted={r.accepted:,}, errors={r.errors:,}"))

        # 2. backlog drained
        if drained:
            r.checks.append(Check(
                "backlog drained", "PASS",
                f"catch-up {r.catchup_s:.1f}s (lag/apply/outbox -> 0)"))
        else:
            r.checks.append(Check(
                "backlog drained", "FAIL",
                f"not drained within {self.args.catchup_timeout:.0f}s: "
                f"mysql_lag={_n(final.mysql_lag)} match_lag={_n(final.match_lag)} "
                f"apply_lag={_n(final.max_apply_lag)} "
                f"leader_outbox={_n(final.leader_outbox_pending)} "
                f"groups_led={final.groups_with_leader}/{final.groups_total}"))

        # 3. RPO / consistency: published == match == mysql after drain
        pub, my, mt = final.published, final.mysql_completed, final.match_completed
        consistent = (not any(math.isnan(x) for x in (pub, my, mt))
                      and pub == my == mt)
        r.checks.append(Check(
            "RPO=0 / consistency",
            "PASS" if consistent else "FAIL",
            f"published={_n(pub)} mysql={_n(my)} match={_n(mt)} "
            f"(this stage +{_n(r.published_delta)}/"
            f"+{_n(r.mysql_delta)}/+{_n(r.match_delta)})"))

        # 4. DLQ (dropped/poisoned commands)
        dlq_now = final.dlq if not math.isnan(final.dlq) else 0.0
        r.checks.append(Check(
            "no DLQ growth",
            "PASS" if dlq_now <= dlq0 else "FAIL",
            f"dlq {dlq0:.0f} -> {dlq_now:.0f}"))

        # 5. latency SLOs
        if stage.enforce_latency:
            self._latency_check(r, final, "Raft commit p99",
                                 final.raft_commit_p99_ms, SLO["raft_commit_p99_ms"])
            self._latency_check(r, final, "Kafka->match p99 (command latency)",
                                 final.command_p99_ms, SLO["kafka_to_match_p99_ms"])
            # WAL fsync is environment-distorted on Docker Desktop: informational
            r.checks.append(Check(
                "WAL fsync p99 (informational)", "NA",
                f"{_f(final.wal_fsync_p99_ms)} ms — not gated "
                "(Docker Desktop fsync is not representative of NVMe)"))

        # 6. resource utilisation
        if stage.resource_max is not None:
            if self.args.resource_prefix and not math.isnan(r.max_mem):
                thr = stage.resource_max * 100.0
                status = "PASS" if r.max_mem <= thr else (
                    "WARN" if self.args.slo_mode == "warn" else "FAIL")
                r.checks.append(Check(
                    f"resource < {thr:.0f}% (mem)", status,
                    f"max mem {r.max_mem:.0f}%, max cpu {_f(r.max_cpu)}% "
                    "(cpu per-core, informational)"))
            else:
                r.checks.append(Check(
                    f"resource < {stage.resource_max*100:.0f}%", "NA",
                    "no --resource-prefix / docker stats unavailable; "
                    "gate on external node metrics (Prometheus) in production"))

        # 7. capacity stage: no dropped orders, no OOM (no endpoint dropout)
        if stage.kind == "capacity":
            no_drop = (r.errors == 0 and consistent and drained)
            r.checks.append(Check(
                "no dropped orders", "PASS" if no_drop else "FAIL",
                "errors=0, counters consistent, fully caught up"))
            oom = final.endpoints_up < final.endpoints_total
            r.checks.append(Check(
                "no OOM / endpoint dropout",
                "WARN" if oom else "PASS",
                f"{final.endpoints_up}/{final.endpoints_total} raft endpoints up "
                "at end"))

        # 8. fault stage: RTO
        if stage.kind == "fault":
            if math.isnan(r.rto_s):
                r.checks.append(Check(
                    "RTO <= 10s", "FAIL",
                    "leadership not restored within re-election window"))
            else:
                status = "PASS" if r.rto_s <= SLO["rto_seconds"] else (
                    "WARN" if self.args.slo_mode == "warn" else "FAIL")
                r.checks.append(Check(
                    f"RTO <= {SLO['rto_seconds']:.0f}s", status,
                    f"leadership restored in {r.rto_s:.1f}s "
                    f"({r.reelected_groups} group(s) re-elected)"))

        print(f"  verdict: {r.verdict}")
        for c in r.checks:
            print(f"    [{c.status:>4}] {c.name}: {c.detail}")

    def _latency_check(self, r, final, label, value, threshold):
        if math.isnan(value):
            r.checks.append(Check(label, "NA", "no samples"))
            return
        ok = value <= threshold
        status = "PASS" if ok else ("WARN" if self.args.slo_mode == "warn" else "FAIL")
        r.checks.append(Check(
            f"{label} <= {threshold:.0f}ms", status, f"{value:.1f} ms"))

    # ---- report -----------------------------------------------------------
    def write_report(self):
        path = os.path.join(self.args.output, "acceptance-report.md")
        lines = []
        lines.append("# trade-core capacity acceptance report\n")
        lines.append(f"- generated: {time.strftime('%Y-%m-%d %H:%M:%S %z')}")
        lines.append(f"- mode: {'SMOKE' if self.args.smoke else 'ACCEPTANCE'} "
                     f"(scale={self.args.scale}, duration_scale={self.args.duration_scale}, "
                     f"slo_mode={self.args.slo_mode})")
        lines.append(f"- order API: `{self.args.order_api}`  metrics: `{self.order_metrics_url}`")
        lines.append(f"- raft metrics endpoints: {len(self.raft_eps)}")
        lines.append("")
        overall = "PASS"
        if any(r.verdict == "FAIL" for r in self.results):
            overall = "FAIL"
        elif any("warning" in r.verdict for r in self.results):
            overall = "PASS (warnings)"
        lines.append(f"## Overall: **{overall}**\n")
        lines.append("| Stage | Target cmd/s | Dur (s) | Accepted | Errors | "
                     "Load p99 (ms) | Catch-up (s) | Verdict |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
        for r in self.results:
            lines.append(
                f"| {r.stage.name} | {r.target_tps:,.0f} | {r.duration_s:.0f} | "
                f"{r.accepted:,} | {r.errors:,} | {_f(r.load_p99_ms)} | "
                f"{_f(r.catchup_s)} | {r.verdict} |")
        lines.append("")
        for r in self.results:
            lines.append(f"### {r.stage.name} — {r.verdict}")
            lines.append(f"_{r.stage.note}_\n")
            if r.stage.kind == "fault" and not math.isnan(r.rto_s):
                lines.append(f"- measured RTO: **{r.rto_s:.1f}s** "
                             f"({r.reelected_groups} group(s) re-elected)\n")
            lines.append("| Check | Status | Detail |")
            lines.append("|---|---|---|")
            for c in r.checks:
                lines.append(f"| {c.name} | {c.status} | {c.detail} |")
            lines.append("")
        with open(path, "w") as fh:
            fh.write("\n".join(lines))
        print(f"\nreport -> {path}")
        return path, overall

    def run(self, stages: List[Stage]) -> str:
        for stage in stages:
            self.results.append(self.run_stage(stage))
        _, overall = self.write_report()
        return overall


# --------------------------------------------------------------------------
# formatting helpers
# --------------------------------------------------------------------------
def _n(x: float) -> str:
    if isinstance(x, float) and math.isnan(x):
        return "n/a"
    return f"{x:.0f}"


def _f(x: float) -> str:
    if isinstance(x, float) and math.isnan(x):
        return "n/a"
    return f"{x:.1f}"


def _delta(a: float, b: float) -> float:
    if math.isnan(a) or math.isnan(b):
        return math.nan
    return a - b


def _nanmax(a: float, b: float) -> float:
    if math.isnan(a):
        return b
    if math.isnan(b):
        return a
    return max(a, b)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def parse_args(argv):
    p = argparse.ArgumentParser(
        description="trade-core capacity acceptance ladder harness",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--order-api", default="127.0.0.1:9200",
                   help="host:port of the order API (bench connects here)")
    p.add_argument("--token", default="local-order-api-token")
    p.add_argument("--bench-bin", required=True,
                   help="path to the compiled order_batch_e2e binary")
    p.add_argument("--metrics-order", default=None,
                   help="order API /metrics URL (default derived from --order-api)")
    # raft endpoint auto-discovery
    p.add_argument("--raft-host", default="127.0.0.1")
    p.add_argument("--raft-nodes", type=int, default=5)
    p.add_argument("--raft-groups", type=int, default=4)
    p.add_argument("--raft-port-base", type=int, default=9200,
                   help="host port = base + group*10 + node (docker-compose.raft.yml)")
    p.add_argument("--metrics-endpoints", default=None,
                   help="explicit comma list of raft metrics URLs "
                        "(url or url@node:group); overrides auto-discovery")
    # ladder selection / scaling
    p.add_argument("--stage", default=None,
                   help="run a single stage by name "
                        f"({', '.join(s.name for s in LADDER)})")
    p.add_argument("--scale", type=float, default=1.0,
                   help="multiply target TPS (local smoke uses <<1)")
    p.add_argument("--duration-scale", type=float, default=1.0,
                   help="multiply stage sustain durations")
    # load params
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--assets", type=int, default=10_000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--round-seconds", type=float, default=5.0,
                   help="load sub-round length for constant-rate pacing")
    # observation
    p.add_argument("--sample-interval", type=float, default=5.0)
    p.add_argument("--catchup-timeout", type=float, default=120.0)
    p.add_argument("--resource-prefix", default=None,
                   help="docker container name prefix for coarse resource "
                        "sampling via `docker stats` (e.g. kaishi-29a4a3-raft-)")
    p.add_argument("--output", default="acceptance-out")
    p.add_argument("--slo-mode", choices=["enforce", "warn"], default="enforce",
                   help="latency/RTO/resource breaches FAIL (enforce) or WARN")
    # fault stage
    p.add_argument("--fault-cmd", default=None,
                   help="command run at fault-stage midpoint "
                        "(default: docker kill the busiest leader node)")
    p.add_argument("--fault-recover-cmd", default=None,
                   help="command run at fault-stage end (default: docker start it)")
    p.add_argument("--fault-node", type=int, default=None,
                   help="physical node index being killed (for endpoint exclusion)")
    p.add_argument("--fault-container-prefix", default="kaishi-29a4a3-raft-",
                   help="prefix used to build default fault/recover commands")
    p.add_argument("--dry-run-faults", action="store_true",
                   help="print fault commands instead of executing them")
    # smoke
    p.add_argument("--smoke", action="store_true",
                   help="local full-ladder smoke: small scale, short stages, "
                        "warn-mode SLOs")
    p.add_argument("--smoke-seconds", type=float, default=20.0,
                   help="per-stage load duration in --smoke mode")
    args = p.parse_args(argv)

    if args.metrics_order is None:
        host = args.order_api.split(":")[0]
        port = args.order_api.split(":")[1] if ":" in args.order_api else "9200"
        args.metrics_order = f"http://{host}:{port}/metrics"
    if args.smoke:
        if args.scale == 1.0:
            args.scale = 0.0006
        args.slo_mode = "warn"
        if args.catchup_timeout == 120.0:
            args.catchup_timeout = 90.0
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.stage:
        if args.stage not in LADDER_BY_NAME:
            sys.exit(f"unknown stage {args.stage!r}; choose from "
                     f"{', '.join(LADDER_BY_NAME)}")
        stages = [LADDER_BY_NAME[args.stage]]
    else:
        stages = LADDER
    harness = Harness(args)
    overall = harness.run(stages)
    sys.exit(0 if overall != "FAIL" else 1)


if __name__ == "__main__":
    main()
