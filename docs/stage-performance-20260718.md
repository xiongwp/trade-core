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
