# 分段性能测试报告（2026-07-18）

## 环境与负载

- 环境：macOS Docker Desktop，本机 3 Redpanda、10 MySQL、5 Raft 节点 / 4 group
- 输入：100,000 条订单，32 HTTP 并发，500 条/批，10,000 个资产
- 输出：288,000 个撮合结果事件
- 完整性：命令 MySQL lag、命令撮合 lag、结果 Kafka lag 均归零；DLQ 为 0

## 分段结果

| 阶段 | 测量边界 | 吞吐/服务率 | 延迟 |
|---|---|---:|---:|
| HTTP → 命令 Kafka | HTTP 写入至 Kafka ACK | 45,060 cmd/s | batch p50 87.76ms，p95 1208.53ms，p99 1335.93ms |
| 命令 Kafka → MySQL | 消费批次的 MySQL 事务 | 约 160k cmd/s（32 worker 服务时间折算） | batch avg 93.89ms，p50 79.46ms，p90 177.07ms，p99 362.33ms |
| 命令 Kafka → 撮合 | 消费批次转发至 Raft/撮合完成 | 约 84k cmd/s（32 worker 服务时间折算） | batch avg 163.15ms；command p50 114–156ms，p99 837–958ms |
| 撮合 → 结果 Kafka | outbox 批次发送至 Kafka ACK | 约 65,100 events/s（累计服务时间折算） | batch avg 7.76ms，max 233.27ms |
| 结果 Kafka → MySQL | 单执行事件的 DB 事务提交 | 约 2,638 events/s（4 worker 服务时间折算） | event avg 1.516ms，p50 1.25ms，p90 2.92ms，p99 6.30ms |

## 结论

当前瓶颈是“结果 Kafka → MySQL”。4 个消费者逐事件开启并提交事务，理论服务率约
2.6k events/s；实测结果积压峰值 169,034，随后从 169,034 → 130,571 → 70,469
→ 12,028 → 0，观察到的排空速度约 1.2k–3.8k events/s，与事务服务时间折算一致。

HTTP 阶段虽然最终零错误，但发生 3,575,000 command-equivalent 的 429 重试计数，
因此 p95/p99 包含背压等待。入口能够以 45k cmd/s 接受，不代表下游能持续承载该速率。

下一步应优先把结果落库改成按分片批量事务（每批多事件），再增加 execution consumer；
只增加消费者会很快受 MySQL 连接、事务日志和热点订单行更新限制。

> Docker Desktop 的 fsync 与调度延迟不代表 Linux + NVMe 生产环境。本报告适合定位
> 相对瓶颈，不应作为生产容量承诺。

## 批量结果落库优化复测

全量清空 Kafka、MySQL 和 Raft 数据后重新测试 100,000 条订单：

- HTTP → Kafka：60,927 cmd/s，零错误。
- 撮合输出：200,000 个结果事件。
- 结果 Kafka → MySQL：420 个批量事务，平均约 476 events/批。
- 批次延迟：p50 78.24ms，p90 223.33ms，p99 398.67ms。
- 按 4 worker 累计服务时间折算约 18,224 events/s，较逐事件事务的约
  2,638 events/s 提升约 **6.9 倍**。
- 测试结束后约 5 秒内结果 Kafka lag 归零；命令 MySQL、撮合、结果 DB 计数一致，
  DLQ 为 0。

最终路由分为两条：HTTP 命令 Kafka 使用 `asset_category(instrument)`，保证同资产命令
进入相同 topic/partition；撮合结果 Kafka 使用 `order_id`，保证同订单事件进入相同
topic/partition。MySQL 只按 `order_id % 1000` 分到 10 DB × 100 table，不考虑资产；
相同 `(db, table)` 的结果事件合并成一笔事务提交。

## Kafka 配置调优复测

在三个下游消费者均关闭、`TC_ORDER_MAX_PIPELINE_BACKLOG=2000000` 的隔离口径下，
启用 LZ4、可配置 producer batch/linger/queue 后写入 1,000,000 条命令：

- 32 并发 × 500 条/批：1,267,009 cmd/s，错误 0；p50 10.77ms、p95
  18.52ms、p99 106.47ms。
- 64 并发 × 1,000 条/批：1,276,216 cmd/s，错误 0；p50 41.39ms、p95
  119.73ms、p99 129.61ms。
- 64 并发已进入吞吐平台且延迟显著上升，因此本机入口推荐 32 并发 × 500 条/批。
- 相比先前 545,389 cmd/s 的短基准，最高入口吞吐提升约 2.34 倍。

Kafka → 撮合隔离采样表明，32 个消费者争抢 4 个有序 Raft group 会造成大量 leader
重试，吞吐仅约万级；降为 4 个消费者后，500 条批次采样约 66,000 cmd/s。批次提高到
1,000 后采样约 97,000 cmd/s，但开发机上的 4-group Raft 集群仍出现 leader 重试，
后续提升必须增加 Raft group 和物理节点。默认撮合消费者因此改为 4，默认消费批次改为
1,000。

## 水平扩展改建后分阶段复测

使用独立 `trade-core-stage` Compose project 和全新 Kafka/MySQL/Raft 数据卷，按阶段只开启
目标 consumer。负载为 1,000,000 条命令、32 HTTP 长连接、500 条/请求、10,000 个资产。

| 阶段 | 数据量 | 实测吞吐 | 延迟/说明 |
|---|---:|---:|---|
| HTTP → 命令 Kafka | 1,000,000 commands | **1,152,232 cmd/s** | 0错误；p50 10.58ms，p95 29.07ms，p99 73.18ms |
| 命令 Kafka → MySQL | 1,000,000 commands | 约 **110k–160k cmd/s** | lag采样窗口6–9s；事务p50 31.43ms，p90 80.92ms，p99 297.18ms |
| 命令 Kafka → Raft/撮合 | 1,000,000 commands | 约 **22k cmd/s** | 1030批，平均转发约233ms，max 5.49s |
| 撮合 Outbox → 结果 Kafka | 2,000,000 events | 不低于 **44k events/s** | 与45s撮合并行完成；leader pending=0、失败=0 |
| 结果 Kafka → MySQL | 2,000,000 events | 约 **60.6k events/s** | 约33s；p50 94.13ms，p90 340.49ms，p99 498.68ms |

一致性检查：三个consumer group总lag均为0；execution topic 16个partition各125,000条；
10个MySQL分片各100,000条命令、200,000条execution event，分布完全均匀。

结果落库从最初逐事件事务的约2,638 events/s提升到约60.6k events/s，约**23倍**。
本轮撮合低于此前约97k cmd/s基线，采样显示20个Raft replica共享Docker Desktop虚拟磁盘时，
WAL fsync p99为1.49–2.00s，leader Raft commit p99为94.75–698ms；这是本机磁盘同步瓶颈，
不是CPU或Kafka consumer不足。生产扩展必须把同group副本分散到独立本地NVMe故障域，增加
物理group后再做500万TPS验收。

## 纯内存撮合能力（修正后的撮合口径）

撮合计算与可恢复性链路分开计量：生产数据流仍为
`Kafka（同资产有序）→ Raft/WAL → 内存撮合 → 结果Outbox日志`；Raft/WAL负责进程崩溃后通过
快照和命令日志重放恢复订单簿，不计入“纯内存撮合TPS”。结果Outbox也单独计量。

使用 `cluster_throughput` 的无journal模式测试锁无关输入队列、内存订单簿撮合和结果队列排空。
负载为85%新订单、15%撤单，价格会真实交叉成交；同一逻辑节点内的同资产命令严格串行。

| 独立撮合节点 | 命令数 | 耗时 | 聚合纯内存撮合吞吐 | 单节点折算 |
|---:|---:|---:|---:|---:|
| 1 | 2,000,000 | 774.23ms | **2,583,211 orders/s** | 2,583,211/s |
| 2 | 3,000,000 | 700.41ms | **4,283,195 orders/s** | 2,141,598/s |
| 3 | 3,600,000 | 722.82ms | **4,980,513 orders/s** | 1,660,171/s |
| 4 | 4,000,000 | 623.96ms | **6,410,653 orders/s** | 1,602,663/s |

因此纯内存撮合在本机使用4个share-nothing节点已经超过500万TPS。此前约22k cmd/s是
`Kafka → 5副本Raft → WAL/Outbox落盘 → 撮合完成`的强持久化端到端吞吐，瓶颈为20个
Raft replica共享Docker Desktop虚拟磁盘的fsync，不能用于代表撮合算法性能。

500万TPS生产容量应同时满足两个独立条件：至少4个逻辑撮合分片可提供计算能力，并把Raft
group副本分布到独立NVMe故障域，使WAL提交能力也达到入口速率；否则纯撮合达到500万，系统
端到端仍会在WAL阶段积压。结果Outbox日志及其Kafka发布同样需要按group水平分片。

## Raft/WAL 持久化撮合批量优化复测

针对原先约22k cmd/s的强持久化链路，将撮合消费批次与MySQL批次解耦，并把单个撮合微批上限
从1,000提高到10,000。资产路由与顺序语义不变：同资产仍进入同一命令topic/partition，
Raft group内仍串行应用；只是把更多命令合并到一次Raft quorum WAL、应用WAL、结果Outbox和
applied-watermark持久化窗口中。

隔离环境使用4个Raft group、每组5副本、4个撮合consumer，输入1,000,000条命令。关闭命令DB
consumer和结果DB consumer；设置`TC_EXECUTION_PUBLISH_ENABLED=false`只暂停结果Kafka发布，
结果Outbox仍正常写入并参与fsync，因此测试边界为
`HTTP/Kafka → 5副本Raft/WAL → 内存撮合 → durable result Outbox`。稳态测试关闭周期快照，避免
30秒快照风暴污染吞吐；生产不能永久关闭快照，应按group错峰。

- HTTP → Kafka：963,651 cmd/s，错误0；p50 14.33ms、p95 28.52ms、p99 83.00ms。
- 100万命令从开始写入到撮合计数达到100万、consumer lag归零：17.41秒，保守端到端吞吐
  **57,438 cmd/s**。
- 相比原先约22k cmd/s提升约**2.61倍**。
- Docker Desktop共享虚拟盘的重复轮次约46k–57k cmd/s，曾观察到约137k cmd/s热态突发；
  fsync争用导致波动，容量规划采用57,438 cmd/s以下的可重复保守值，不采用突发峰值。

新增两个独立配置：`TC_ORDER_MATCH_BATCH_SIZE`（默认10,000）和
`TC_ORDER_MATCH_BATCH_LINGER_MS`（默认2ms）。`TC_EXECUTION_PUBLISH_ENABLED=false`仅用于分阶段
测试或故障隔离，不会关闭结果日志；默认仍为true。

本轮确认主要瓶颈是每个Raft应用批次还要同步写大量per-asset WAL，以及20个副本共享同一Docker
虚拟盘的fsync竞争。下一节已据此移除生产Raft热路径中的重复命令WAL，并保留Raft日志重放和
结果Outbox恢复语义。

## Raft单一WAL + 内存快照改造复测

按“Raft WAL是唯一命令真相源、内存订单簿定期快照、结果Outbox持久化”的结构完成改造。
生产Raft热路径不再同步写per-asset WAL和per-shard command journal。每个已应用Raft批次额外写入
32字节应用证明，绑定`raft_index + result_count + result_fingerprint`；恢复时先加载快照中的
`raft_applied_index`，再重放Raft尾部。对已有应用证明的批次重新计算结果数量和指纹，完全一致
才抑制重复结果；不一致直接失败停止。Outbox中没有应用证明的尾部会被截断并由Raft WAL重建。

隔离环境仍为4个Raft group、每组5副本、4个撮合consumer。关闭命令DB、结果DB以及结果Kafka
publisher，但结果Outbox仍写入并执行持久化屏障；关闭周期快照仅用于避免测试时暂停，生产默认
30秒。输入1,000,000条命令，32 HTTP并发、500条/请求、10,000个资产：

- HTTP → Kafka：**662,167 cmd/s**，错误0；p50 19.67ms、p95 46.27ms、p99 127.17ms。
- 从开始写入到`tc_order_match_completed_commands=1,000,000`：**3.53秒**，即约
  **283,286 cmd/s**强持久化端到端吞吐。
- 相比重复WAL版本的57,438 cmd/s提升约**4.93倍**；相比最初约22k cmd/s提升约**12.9倍**。
- 压测后单个300,000命令group副本约产生12MiB `raft.state`、46MiB结果Outbox和4KiB应用证明。
  数据卷检查没有`journal-shard-*.bin`或`asset-*.wal`。
- 重启一个承载4个group的Raft容器并回放100万订单数据，1.83秒内4个group均恢复为
  `ready=1`、`commit_index=applied_index`、`apply_lag=0`，日志无指纹不一致或持久化错误。

单元/集成恢复测试还覆盖两个崩溃边界：快照后Raft尾部重放不会重复发布结果；Outbox已落盘但
应用证明未落盘时，重启会截断该尾部并从Raft命令生成完全相同结果。完整`cargo test`通过。

当前安全限制：在撮合状态快照与Raft consensus snapshot实现原子传输/安装之前，
`TC_RAFT_COMPACT_APPLIED_THRESHOLD`必须为0，因此Raft WAL会持续增长。这不会丢数据，但需要磁盘
容量监控；下一步工程重点是原子快照传输、验证、安装完成后再安全压缩日志。性能瓶颈已从重复
命令WAL转为20个副本共享Docker虚拟盘上的Raft WAL、结果Outbox和应用证明fsync。生产500万TPS
仍需增加独立Raft group，并把每个副本放到独立本地NVMe故障域做线性扩展验收。
