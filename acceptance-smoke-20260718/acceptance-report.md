# trade-core capacity acceptance report

- generated: 2026-07-18 11:43:32 +0900
- mode: SMOKE (scale=0.0006, duration_scale=1.0, slo_mode=warn)
- order API: `127.0.0.1:9200`  metrics: `http://127.0.0.1:9200/metrics`
- raft metrics endpoints: 20

## Overall: **PASS (warnings)**

| Stage | Target cmd/s | Dur (s) | Accepted | Errors | Load p99 (ms) | Catch-up (s) | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| warmup | 300 | 10 | 3,000 | 0 | 102.2 | 2.2 | PASS |
| stage1 | 600 | 10 | 6,000 | 0 | 82.3 | 1.2 | PASS (warnings) |
| stage2 | 1,800 | 10 | 18,000 | 0 | 109.8 | 2.2 | PASS (warnings) |
| stage3 | 3,000 | 10 | 30,000 | 0 | 111.4 | 2.2 | PASS (warnings) |
| capacity | 4,290 | 10 | 42,900 | 0 | 114.8 | 2.2 | PASS |
| fault | 3,000 | 10 | 30,000 | 0 | 102.4 | 9.4 | PASS |

### warmup — PASS
_lag drains to zero, no errors_

| Check | Status | Detail |
|---|---|---|
| no load errors | PASS | accepted=3,000, errors=0 |
| backlog drained | PASS | catch-up 2.2s (lag/apply/outbox -> 0) |
| RPO=0 / consistency | PASS | published=3000 mysql=3000 match=3000 (this stage +3000/+3000/+3000) |
| no DLQ growth | PASS | dlq 0 -> 0 |

### stage1 — PASS (warnings)
_p99 SLOs met, resource < 50%_

| Check | Status | Detail |
|---|---|---|
| no load errors | PASS | accepted=6,000, errors=0 |
| backlog drained | PASS | catch-up 1.2s (lag/apply/outbox -> 0) |
| RPO=0 / consistency | PASS | published=9000 mysql=9000 match=9000 (this stage +6000/+6000/+6000) |
| no DLQ growth | PASS | dlq 0 -> 0 |
| Raft commit p99 <= 20ms | WARN | 79.4 ms |
| Kafka->match p99 (command latency) <= 100ms | WARN | 1980.0 ms |
| WAL fsync p99 (informational) | NA | 1980.0 ms — not gated (Docker Desktop fsync is not representative of NVMe) |
| resource < 50% (mem) | PASS | max mem 2%, max cpu 13.1% (cpu per-core, informational) |

### stage2 — PASS (warnings)
_no sustained lag, resource < 65%_

| Check | Status | Detail |
|---|---|---|
| no load errors | PASS | accepted=18,000, errors=0 |
| backlog drained | PASS | catch-up 2.2s (lag/apply/outbox -> 0) |
| RPO=0 / consistency | PASS | published=27000 mysql=27000 match=27000 (this stage +18000/+18000/+18000) |
| no DLQ growth | PASS | dlq 0 -> 0 |
| Raft commit p99 <= 20ms | WARN | 292.0 ms |
| Kafka->match p99 (command latency) <= 100ms | WARN | 1965.0 ms |
| WAL fsync p99 (informational) | NA | 1965.0 ms — not gated (Docker Desktop fsync is not representative of NVMe) |
| resource < 65% (mem) | PASS | max mem 3%, max cpu 19.1% (cpu per-core, informational) |

### stage3 — PASS (warnings)
_all SLOs met, resource < 70%_

| Check | Status | Detail |
|---|---|---|
| no load errors | PASS | accepted=30,000, errors=0 |
| backlog drained | PASS | catch-up 2.2s (lag/apply/outbox -> 0) |
| RPO=0 / consistency | PASS | published=57000 mysql=57000 match=57000 (this stage +30000/+30000/+30000) |
| no DLQ growth | PASS | dlq 0 -> 0 |
| Raft commit p99 <= 20ms | WARN | 295.3 ms |
| Kafka->match p99 (command latency) <= 100ms | WARN | 1932.5 ms |
| WAL fsync p99 (informational) | NA | 1932.5 ms — not gated (Docker Desktop fsync is not representative of NVMe) |
| resource < 70% (mem) | PASS | max mem 3%, max cpu 9.9% (cpu per-core, informational) |

### capacity — PASS
_no dropped orders, no OOM, full catch-up after_

| Check | Status | Detail |
|---|---|---|
| no load errors | PASS | accepted=42,900, errors=0 |
| backlog drained | PASS | catch-up 2.2s (lag/apply/outbox -> 0) |
| RPO=0 / consistency | PASS | published=99900 mysql=99900 match=99900 (this stage +42900/+42900/+42900) |
| no DLQ growth | PASS | dlq 0 -> 0 |
| no dropped orders | PASS | errors=0, counters consistent, fully caught up |
| no OOM / endpoint dropout | PASS | 20/20 raft endpoints up at end |

### fault — PASS
_RPO=0, RTO <= 10 s on leader kill_

- measured RTO: **2.6s** (2 group(s) re-elected)

| Check | Status | Detail |
|---|---|---|
| node rejoin | PASS | killed node endpoints reachable again |
| no load errors | PASS | accepted=30,000, errors=0 |
| backlog drained | PASS | catch-up 9.4s (lag/apply/outbox -> 0) |
| RPO=0 / consistency | PASS | published=129900 mysql=129900 match=129900 (this stage +30000/+30000/+30000) |
| no DLQ growth | PASS | dlq 0 -> 0 |
| RTO <= 10s | PASS | leadership restored in 2.6s (2 group(s) re-elected) |
