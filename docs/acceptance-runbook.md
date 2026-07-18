# 容量验收操作手册 (acceptance runbook)

本手册说明如何在真机（Linux + 本地企业级 NVMe）上执行
`docs/production-deployment-5m-tps.md` §12 定义的容量验收阶梯，以及如何用
`scripts/acceptance/run-ladder.py` 一键跑 ladder、解读报告。所有 TPS 均按业务
命令数统计（下单/撤单/改单各计一条），不按 HTTP 请求或 Kafka batch 计。

---

## 0. 前置条件

- 40 broker Kafka、100 MySQL 写分片、50 Raft 节点 / 100 group、matching pool、
  Order API 已按 §11 上线并通过各自的组件级验收。
- 负载发生机与被测集群同网段、独立机器；单机无法产生 5M cmd/s，需要多台负载机
  并行（见 §5 扩展说明）。
- 观测：Prometheus/Thanos 已抓取所有 `/metrics`，资源利用率（CPU/mem/磁盘/网络）
  由 node exporter 采集——harness 的 `docker stats` 采样只用于本机冒烟，生产资源
  门限（<50/65/70%）以 Prometheus 为准。

## 1. 用 gen-deploy.py 铺环境

生成 100 group / 50 node / 100 MySQL 分片的部署清单：

```sh
python3 scripts/gen-deploy.py --groups 100 --nodes 50 --mysql-shards 100 \
    --output deploy/prod
```

产出（详见 `scripts/README.md`）：每节点一个 `docker-compose.raft-K.yml` +
`raft-K.env`（把 `RAFT_HOST_*` 的 CHANGE-ME 换成真实对端 IP）、MySQL/gateway
清单，以及 `topology.json`（机器可读的放置与端口映射）。

逐节点部署：

```sh
docker compose --env-file deploy/prod/raft-K.env \
    -f deploy/prod/docker-compose.raft-K.yml up -d
```

`topology.json` 里每个 group replica 的 metrics 端口即 harness 的
`--metrics-endpoints` 输入来源（生产端口不一定符合本机
`base+group*10+node` 公式，用 topology.json 显式列出更稳妥）。

## 2. 预热与就绪校验

1. Order API 先关闭外部流量，确认 persist / match 两个 consumer group lag=0
   （`tc_order_mysql_consumer_lag` / `tc_order_match_consumer_lag`）。
2. 每个 group 五副本 `tc_ready=1`、`tc_raft_role` 恰有一个 leader、
   `tc_raft_apply_lag=0`。
3. 单跑一轮小负载（如 `--stage warmup --scale 0.2`）确认链路通、无 DLQ、
   backlog 能归零，再进入正式 ladder。

## 3. 跑 ladder（生产）

```sh
python3 scripts/acceptance/run-ladder.py \
    --order-api ORDER_API_HOST:9200 \
    --token "$TC_ORDER_API_TOKEN" \
    --bench-bin /path/to/order_batch_e2e \
    --metrics-order http://ORDER_API_HOST:9200/metrics \
    --metrics-endpoints "$(python3 - <<'PY'
import json;d=json.load(open("deploy/prod/topology.json"))
# emit url@node:group,... from topology (adapt keys to your topology.json)
PY
)" \
    --concurrency 256 --assets 100000 --batch 1000 \
    --catchup-timeout 600 \
    --resource-prefix "" \
    --fault-cmd "ssh raft-node-K docker kill <leader-container>" \
    --fault-recover-cmd "ssh raft-node-K docker start <leader-container>" \
    --slo-mode enforce \
    --output acceptance-$(date +%Y%m%d)
```

要点：

- 不带 `--stage` 即按顺序跑完整阶梯（热身 → 1M → 3M → 5M → 容量 7.15M →
  故障）。带 `--stage stage3` 可只跑单阶段重测。
- 目标 TPS、时长、通过条件全部表驱动（脚本内 `LADDER`，取自文档 §12）。
  `--scale` / `--duration-scale` 只在冒烟/缩比时使用；**生产验收用 1.0**。
- `--slo-mode enforce`：延迟 SLO、RTO、资源门限超标即判 FAIL。
- 恒速投放：harness 把每阶段切成 `--round-seconds` 的小轮，每轮按目标 TPS
  计算批量并补偿间歇，逼近恒定入口速率；记录接受吞吐、错误数、批延迟分位。

## 4. 解读报告

输出目录含：

- `metrics-<stage>.csv`：每 5s（`--sample-interval`）一行，含 published /
  mysql / match 计数、两个 consumer lag、ingress backlog、backpressure、DLQ、
  raft `max_apply_lag`、**group leader 的** execution outbox pending 之和、
  各组 leader 覆盖数、端点存活数、以及 p99 gauge（raft commit / match /
  command latency / wal fsync，单位 ms）。相位列 `phase` 区分 load / catchup /
  final。
- `acceptance-report.md`：总览表（每阶段目标、接受数、错误、负载 p99、追平
  时间、判定）+ 每阶段 check 明细表。

每阶段的通过判定（对应 §12）：

| Check | 含义 | 数据来源 |
|---|---|---|
| no load errors | 入口零错误 | bench `errors=` |
| backlog drained | 两 lag + ingress + apply_lag + **各组 leader outbox** 全部归零并计时 | /metrics 轮询 |
| RPO=0 / consistency | published == mysql == match（追平后三计数相等） | order API 计数 |
| no DLQ growth | 无投毒/丢单进 DLQ | `tc_order_dlq_total` |
| Raft commit p99 <= 20ms | Raft quorum commit SLO | `tc_raft_commit_ns_p99` |
| Kafka->match p99 <= 100ms | 命令端到端延迟 SLO | `tc_command_latency_ns_p99` |
| resource < 50/65/70% | 资源余量 | node exporter（本机为 docker stats mem%） |
| no dropped orders / no OOM | 容量阶段：不丢单、端点无掉线 | 计数一致 + 端点存活 |
| RTO <= 10s | 故障阶段：全组恢复 leader 用时 | 轮询各组 `tc_raft_role` |

**判定语义**：任一 FAIL → 阶段 FAIL → 整体 FAIL（进程退出码 1）。
`--slo-mode warn` 下延迟/RTO/资源超标降级为 WARN（整体仍 PASS，标注 warnings），
用于本机冒烟；生产必须 `enforce`。

### backlog 判定的一个关键点

`tc_execution_outbox_pending` 是**每副本**指标：只有 group 的 **leader** 会把
执行事件发到 execution Kafka 并推进发布水位，follower 只做持久化 outbox、不发布，
其 pending 会一直很高直到自己成为 leader。因此 harness 只统计**每组 leader** 的
outbox pending 作为“积压归零”依据，而不是所有副本求和——否则永远归不了零。
解读 CSV 时同理：`leader_outbox_pending` 列已是各组 leader 之和。

## 5. 单机产不出目标 TPS 时

`order_batch_e2e` 单进程受连接/CPU 限制，达不到百万级。生产 ladder 需：

- 多台负载机各跑一份 harness 的 load，或用同一 harness 更大的
  `--concurrency`（256+）与多进程封装；每份 harness 观测同一集群 /metrics。
- 判定以集群侧 published/lag/p99 为准，入口接受数只是其中一项——文档 §12
  明确“只报告入口接受数不算通过”。
- 若用多负载机，选一台作为“判定主”跑完整 harness，其余只投放负载。

## 6. 故障阶段细节

`--stage fault` 在阶段中点执行 `--fault-cmd`（默认自动选当前领导 group 最多的
节点并 `docker kill`），随后：

1. **重选时间（RTO）**：轮询所有 group 的 `tc_raft_role`，从注入到“每个 group
   都重新有 leader”的用时；对照 10s 门限。被杀节点若当时未领导任何 group，
   RTO≈0（无需重选，合法通过）。
2. **追平时间**：leader 恢复后等 backlog 归零并计时。
3. **RPO=0**：追平后校验 published == match == mysql。
4. **归队**：阶段末执行 `--fault-recover-cmd`（默认 `docker start`），验证被杀
   节点端点重新可达、副本重新追平。

`--dry-run-faults` 只打印不执行，用于演练命令串。

## 7. 已知环境差异（重要）

- **Docker Desktop fsync 失真**：macOS/Docker Desktop 的 fsync 语义与生产
  Linux + 本地 NVMe 完全不同，本机 `wal_fsync p99` 可达 ~2.1s，并会把
  `tc_command_latency_ns_p99` 一起拖高。因此 harness 把 **WAL fsync p99 列为
  informational（不判定 FAIL）**，本机冒烟一律 `--slo-mode warn`。
  **本机的 wal_fsync / command latency p99 不代表生产**，生产必须在真机 NVMe 上
  用 `--slo-mode enforce` 重测。
- 本机资源采样用 `docker stats`（CPU% 为 per-core 之和、可 >100%），只作参考；
  生产资源门限以 node exporter 为准。harness 本机只用 mem% 粗判并降级为 WARN。
- 本机单栈只有 4 group / 5 node，端口按 `docker-compose.raft.yml` 的
  `base+group*10+node`（base=9200）映射；生产用 topology.json 显式列端点。

## 8. 本机冒烟（验证 harness 本身）

栈已在跑（`docker compose up -d` 起 `docker-compose.yml` +
`docker-compose.raft.yml`）时：

```sh
python3 scripts/acceptance/run-ladder.py \
    --bench-bin target/release/deps/order_batch_e2e-<hash> \
    --smoke --smoke-seconds 15 \
    --resource-prefix kaishi-29a4a3-raft- \
    --output acceptance-smoke
```

`--smoke` 自动设小 `--scale`（默认 0.0006）、每阶段 `--smoke-seconds` 短时长、
`--slo-mode warn`，几分钟跑完全阶梯（含 leader kill 故障阶段），用于验证 harness
的负载、采集、追平、判定、报告与故障逻辑，而非验证生产容量。
