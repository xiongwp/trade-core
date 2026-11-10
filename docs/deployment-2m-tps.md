# 200 万 TPS 部署方案（含全部参数）

> 对应代码：main `5e0bdf6` 及之后。各层容量是从本机实测外推出来的。本机实测条件：10 核 Mac，Docker 里跑完整套系统，每轮先清空数据，30 万指令端到端约 2.1 万笔/秒。**采购前每层都要先用 1 台物理机做单机压测**，校准第 9 节列的 4 个数字。
> 更大规模（500 万）的架构原则见 [production-deployment-5m-tps.md](production-deployment-5m-tps.md)。本文给出 200 万这个目标的具体台数和每个参数的取值。参数以本文为准，两者冲突时，以代码当前的行为为准。

## 1. 目标与口径

- **TPS 的含义**：每秒被受理、撮合，并且结果投影进 MySQL 的指令数（新单、撤单、改单都算），持续 10 分钟以上。
- **数据要求**：0 丢单、0 重复撮合、0 超量成交，同一组的所有副本状态一致。用 `scripts/bench/chaos.sh` 的对账口径来判定。
- **单品种上限**：200 万是所有品种加起来的总量。单个品种只在一个 Raft 组里撮合，上限约 20–30 万笔/秒。热门品种用品种迁移（第 8 节）挪到独占的组上。

## 2. 负载模型

| 项目 | 2,000,000 指令/秒时 | 依据 |
|---|---|---|
| 成交事件 | 约 700 万条/秒 | 实测每笔指令约 3.5 条 |
| Kafka 消息 | 约 1,100 万条/秒 | 指令 200 万 + 落库路由 200 万 + 成交 700 万 |
| Kafka 写入 | 约 1.3 GB/s，3 副本后约 4 GB/s | 每条平均约 120 B |
| MySQL 行写入 | 约 1,600 万行/秒 | 每笔约 8 行 |
| 单库负载 | 约 5,000 笔/秒（400 个库） | 估计单库上限约 8,000，见 9.1 |

## 3. 硬件清单（约 187 台）

| 层 | 台数 | 单机配置 | 部署内容 |
|---|---|---|---|
| MySQL 主库 | 50 | 48 核、256 GB、4×NVMe（RAID10）、25GbE | 每台 1 个 mysqld，承载 8 个逻辑库（`order_db_N`） |
| MySQL 从库 | 50 | 同主库，可低一档 | 一主一从（半同步） |
| 撮合 | 16 | 32 核、128 GB、2×NVMe、25GbE | `raft-multi-node`，每台 12 个 Raft 副本（64 组 × 3 副本） |
| Kafka（Redpanda） | 15 | 32 核、128 GB、4×NVMe、2×25GbE | 3 副本，`acks=all`（Redpanda 等多数副本确认） |
| 接入 order-api | 16 | 32 核、64 GB、25GbE | HTTP/WS 收单 + 转发撮合 + 指令路由到落库 topic |
| 落库 order-api | 16 | 16 核、64 GB、25GbE | 每台负责 25 个库的指令写入和成交写入 |
| 用户推送 user-stream | 16 | 16 核、64 GB、25GbE | 每台 1 个 router + 1 个 pusher 分片，见 7.5 |
| 行情 market-data | 4 | 16 核、32 GB | K 线、深度 |
| 运维 | 4 | 16 核、64 GB、大盘 | Prometheus、Grafana、日志 |

**网络**：同一机房，25GbE 接入，100GbE leaf-spine 骨干，机器间往返时延小于 0.1 ms。同一个 Raft 组的 3 个副本分布在 3 个机架上。

## 4. 生成每台机器的配置

参数之间有很多联动关系，比如 400 个库地址、64 组的副本分布和端口、每台落库机负责哪些库。**不要手写**，用配置生成脚本：

```bash
python3 scripts/deploy/gen_config.py inventory.json out/
```

`inventory.json` 是机器清单（格式见脚本开头的说明）。脚本为每台机器生成 `out/<主机名>.env`，同时生成 `out/topics.sh`（创建 Kafka topic 的命令）。

脚本的规则：
- 第 g 组的 3 个副本，放在从第 `g mod 撮合机台数` 台开始的连续 3 台撮合机上。这样每台机器的副本数一样多，而且同一组的副本不会落在同一台机器上；
- 400 个库按连续区间平均分给各台落库机；
- 推送机（`push_hosts`）每台生成 `<主机名>-router.env` 和 `<主机名>-pusher.env` 两个文件，第 i 台是第 i 个分片；
- 端口布局和 `raft-multi-node` 完全一致：
  - 0–9 组：Raft 端口 `7000+10g+n`，服务端口 `9001/9101/9102+10g`；
  - 10–2552 组：Raft 端口 `40000+10g+n`，服务端口 `20000+3g` 起的连续 3 个。

## 5. 全局一致参数（所有服务、所有机器必须相同）

下面这些参数决定订单的路由、撮合结果和事件分区。**首次部署时定好，上线后不能修改**，改动等于数据迁移。

| 参数 | 取值 | 说明 |
|---|---|---|
| `TC_DB_COUNT` | **400** | 物理库数量 = 50 台主库 × 每台 8 个 |
| `TC_TABLES_PER_DB` | 100 | 每个库的订单分表数 |
| `TC_VIRTUAL_DB_COUNT` | **400000** | **每个物理库至少 1,000 个逻辑库**，见下表 |
| `TC_VIRTUAL_TABLE_COUNT` | 4000000 | 必须是 `TC_VIRTUAL_DB_COUNT` 的整数倍 |
| `TC_SHARD_ROUTE_VERSION` | 1 | 路由版本。库数或逻辑库数一变，订单所在的库就变了，必须同时升级这个版本号；配置脚本发现库数变了而版本号没变会直接报错（在 `inventory.json` 里设 `route_version`、`virtual_db_count`） |
| `TC_ORDER_CATEGORY_SIZE` | **1** | 每个交易对单独成为一个品种，路由、迁移、负载均衡都以它为单位；order-api 和 raft-node 必须相同 |
| `TC_EXECUTION_KAFKA_PARTITIONS` | **400** | 等于 `TC_DB_COUNT`，每个库对应一个分区 |
| `TC_ORDER_DB_KAFKA_PARTITIONS` | **400** | 同上 |
| `TC_ORDER_KAFKA_TOPIC_COUNT` | 64 | 指令 topic 数，等于 Raft 组数 |
| `TC_ORDER_KAFKA_PARTITIONS_PER_TOPIC` | 16 | 每个指令 topic 16 个分区，共 1,024 个 |
| `TC_INSTRUMENT_SPECS` | 业务配置 | 交易规则，同一组的副本必须一致，见 6.2 |
| `TC_DEDUP_WINDOW_IDS` | 2000000 | 去重窗口，同一组的副本必须一致，见 6.2 |

**为什么 `TC_VIRTUAL_DB_COUNT` 必须很大**：订单先哈希到逻辑库，逻辑库再映射到物理库。下表用和代码完全相同的哈希算法，算出 400 个物理库的负载分布：

| `TC_VIRTUAL_DB_COUNT` | 最热的库 | 最冷的库 | 后果 |
|---|---|---|---|
| 1,000（默认值） | 平均的 2.8 倍 | 0（空库） | 整体吞吐被拖到约 1/3 |
| 40,000 | 1.27 倍 | 0.73 倍 | 浪费约 25% 容量 |
| **400,000** | **1.10 倍** | **0.91 倍** | 推荐 |

## 6. 撮合层（16 台）

### 6.1 启动

**先预留端口**：10 号以后的组用 `40000+10g+n` 端口，落在 Linux 默认的临时端口范围内，可能被别的出站连接占用，导致 raft-node 重启时绑定失败。每台撮合机安装配置脚本生成的 `out/sysctl-raft.conf`：

```bash
cp out/sysctl-raft.conf /etc/sysctl.d/90-trade-core.conf && sysctl --system
```

```bash
# 每台撮合机：加载 out/<主机名>.env，然后
raft-multi-node 1 64 /data
# 以 TC_RAFT_GROUP_PLACEMENTS 为准，上面第一个参数（节点号）会被忽略
```

`raft-multi-node` 会为本机的每个副本启动一个 `raft-node` 子进程：
- 每个子进程单独重启，按 1、2、4…30 秒退避；
- 每个数据目录加独占锁；
- 启动恢复受并发闸门和抖动控制，不会同时拉起全部 group；
- `/healthz` 连续失败才会杀掉子进程；无 Leader、追平中或存储降级只会让
  `/readyz` 返回 503，不会触发重启风暴；
- 启动前检查成员数、重复 ID/地址、主机名、端口和本机端口冲突。

### 6.2 参数

| 参数 | 取值 | 说明 |
|---|---|---|
| `TC_RAFT_GROUP_PLACEMENTS` | 由脚本生成 | 本机负责的副本，格式 `组|本机节点号|1@主机:端口,2@...;...` |
| `TC_RAFT_REPLICAS` | 3 | 每组副本数；要扛住 2 台同时宕机就用 5，撮合机需要 26 台 |
| `TC_DEDUP_WINDOW_IDS` | 2000000 | 撮合引擎的去重窗口，属于复制状态，同组副本必须相同。容量按"单组峰值 × Kafka 最长重投秒数 × 2"估算：3 万/秒 × 30 秒 × 2 ≈ 200 万。热门品种独占的组按它自己的峰值另算（30 万/秒时约 2,000 万）。每 100 万个 ID 约占 24 MB 内存、8 MB 快照 |
| `TC_RAFT_DEDUP_MAX_IDS` | 2000000 | leader 判断"已提交过、无需再提"的窗口，和上一项一致 |
| `TC_SNAPSHOT_EVERY_SECS` | 30 | 快照间隔；快照在后台写盘，撮合线程只停一次内存拷贝（毫秒级） |
| `TC_RAFT_COMPACT_APPLIED_THRESHOLD` | 1000 | 每应用这么多条日志压缩一次 Raft 日志；只会压缩到 outbox 已全部发出的位置 |
| `TC_RAFT_CONFIG_CHECK` | 1 | 配置指纹检查：同组副本的配置不一致时，配置不一致的副本拒绝继续应用（退出码 78） |
| `TC_RAFT_CONFIG_ANNOUNCE_SECS` | 30 | leader 每隔这么久重新公布一次配置指纹 |
| `TC_RAFT_CONFIG_ADOPT` | 0 | 平时为 0；全组统一修改上面那些确定性参数时，所有副本临时设为 1 |
| `TC_RAFT_READY_MAX_APPLY_LAG` | 32 | 应用滞后超过这么多条时，节点不接新指令 |
| `TC_RAFT_READY_MAX_OUTBOX_PENDING` | 50000 | outbox 待发事件超过这么多时，节点不接新指令（不要太小，防止成交高峰时误判） |
| `TC_RAFT_TRANSPORT_QUEUE` | 8192 | 副本之间的消息队列长度 |
| `TC_MATCH_POOL_PER_ASSET` | 4096 | 每个品种预分配的订单槽数，热门品种可以调大 |
| `TC_EXECUTION_OUTBOX_SYNC_EVERY` | 1 | 每个 Raft 条目刷盘一次 outbox，**不要调大** |
| `TC_EXECUTION_PUBLISH_BATCH` | 4096 | outbox 每次发布的最大条数 |
| `TC_EXECUTION_KAFKA_LINGER_MS` | 2 | 发布成交时 Kafka producer 的攒批时间 |
| `TC_EXECUTION_KAFKA_BATCH_MESSAGES` | 10000 | 同上，每批最多条数 |
| `TC_EXECUTION_KAFKA_COMPRESSION` | lz4 | 压缩算法 |
| `TC_EXECUTION_KAFKA_DELIVERY_TIMEOUT_MS` | 15000 | 发布超时。要长于 broker 切主时间：超时的消息会被跳过而后面的照样写入，重发后同一订单的事件会乱序。也不能太长：等待期间发布线程阻塞，健康状态和积压指标停在旧值 |
| `TC_SUPERVISOR_LIVENESS_GRACE_SECS` | 300 | 子进程启动后多久才开始做存活探测，要大于最长的 WAL 恢复时间 |
| `TC_SUPERVISOR_LIVENESS_FAILURES` | 6 | 连续这么多次探测失败（每 5 秒一次）就杀掉重启 |
| `TC_SUPERVISOR_START_CONCURRENCY` | 16 | 同一物理机同时处于 WAL/快照恢复阶段的最大 group 数 |
| `TC_SUPERVISOR_STARTUP_JITTER_MS` | 2000 | group/node 维度的确定性启动抖动上限，分散整批机器重启的连接风暴 |
| `TC_SUPERVISOR_STARTUP_SLOT_TIMEOUT_SECS` | 300 | 单个恢复任务占用启动并发名额的最长时间；超时只释放名额，不杀进程 |
| `TC_SUPERVISOR_MAX_GROUPS` | 1024 | 单机配置的 group 硬上限，超出时启动前拒绝配置 |
| `TC_RAFT_CHILD_NOFILE` | 16384 | 每个 group 子进程的文件描述符软上限（不超过宿主硬上限） |
| `TC_RAFT_CHILD_MAX_ADDRESS_SPACE_MB` | 0 | 可选的每 group 地址空间硬上限；0 表示交给 systemd/cgroup 管理 |

每个 Raft 管理端口提供四层检查：`/livez`（等同 `/healthz`，只看进程）、
`/readyz`（初始化、Leader、apply lag、outbox、WAL/快照）、`/consensusz`
（Leader 和 Raft event-loop 心跳）以及 `/storagez`（WAL/快照写状态）。Kubernetes/
systemd 只能用 `/healthz` 做自动重启依据；`/readyz` 只用于摘流。

## 7. order-api：一个镜像，两种角色

### 7.1 接入角色（16 台）

| 参数 | 取值 | 说明 |
|---|---|---|
| `TC_ORDER_API_TOKEN` | 密钥 | 接口鉴权 |
| `TC_ORDER_KAFKA_MATCH_CONSUMERS` | 4 | 每台 4 个撮合转发消费者，16 台共 64 个，对应 64 个 Raft 组 |
| `TC_ORDER_KAFKA_DB_ROUTER_CONSUMERS` | 4 | 每台 4 个，把指令路由到落库 topic |
| `TC_ORDER_KAFKA_DB_CONSUMERS` | 0 | 接入机不写 MySQL |
| `TC_EXECUTION_KAFKA_MYSQL_CONSUMERS` | 0 | 同上 |
| `TC_RAFT_GROUP_MATCHERS` | 由脚本生成 | 每组所有副本的"收单地址@管理地址" |
| `TC_RAFT_PACK_SIZE` | 5000 | 每个 Raft 条目最多打包的指令数 |
| `TC_RAFT_FORWARD_QUEUE` | 64 | 每组转发队列长度，满了会对 Kafka 消费形成背压 |
| `TC_RAFT_MATCHER_IO_TIMEOUT_MS` | 5000 | 转发超时。必须大于撮合节点的等待时间之和（等 leader 1 秒 + 等提交 3.5 秒），否则接入机会在撮合节点答复前放弃并重发同一批指令 |
| `TC_WS_MAX_BATCH` | 5000 | WebSocket 每条消息最多订单数 |
| `TC_ORDER_KAFKA_LINGER_MS` | 1 | 写指令 topic 时 producer 的攒批时间 |
| `TC_ORDER_KAFKA_BATCH_MESSAGES` | 10000 | 每批最多条数 |
| `TC_ORDER_KAFKA_COMPRESSION` | lz4 | 压缩算法 |
| `TC_ORDER_MAX_PIPELINE_BACKLOG` | 4000000 | 全链路积压上限（各消费组积压之和），超过后限流；取 2 秒的设计流量（200 万/秒 × 2） |
| `TC_ORDER_BP_SOFT` / `_HARD` / `_EMERGENCY` | 4000 / 10000 / 30000 | **按单个分区的积压**分三档：轻度延迟受理 → 拒绝 → 紧急拒绝。默认值（10 万 / 25 万 / 75 万）是按每分区 5 万笔/秒设计的；这里每分区约 2,000 笔/秒（200 万 ÷ 1,024 个分区），取 2 秒 / 5 秒 / 15 秒的量 |
| `TC_KAFKA_OFFSET_COMMIT_ASYNC` | true | 位点异步提交；消费者空闲时会自动同步补提交 |
| `TC_MIGRATION_GROUP_ENDPOINTS` | 由脚本生成 | 品种迁移用：每组所有副本的管理地址，leader 自动识别 |
| `TC_ROUTE_CONTROL_TOPIC` | trade-route-control-v1 | 路由状态（品种归属哪个组、迁移冻结）的持久化 topic，所有 order-api 共享 |
| `TC_ORDER_QUERY_ENABLED` | false | 查询接口放到落库机上；如果要在接入机上提供，需要配置只读库地址 |

### 7.2 落库角色（16 台，每台 25 个库）

| 参数 | 取值 | 说明 |
|---|---|---|
| `TC_ORDER_HTTP_INGRESS_ENABLED` | false | 不对外收单 |
| `TC_MYSQL_OWNED_DBS` | 例如 `0-24` | 本机负责的库，由脚本生成 |
| `TC_ORDER_MYSQL_SHARD_URLS` | 400 个地址 | 第 N 个地址是第 N 个库所在的主库，由脚本生成 |
| `TC_ORDER_KAFKA_DB_CONSUMERS` | **等于负责的库数（25）** | 每个库一个分区、一个消费者 |
| `TC_EXECUTION_KAFKA_MYSQL_CONSUMERS` | **等于负责的库数（25）** | 同上。本机实测：消费者数从 4 个加到 8 个快了 23%，加到和库数一样多又快了约 3% |
| `TC_EXECUTION_MYSQL_BATCH_PER_DB` | 500 | 每个库一个事务最多处理的事件数；**不要调大**，实测调到 2000 反而更慢 |
| `TC_EXECUTION_MYSQL_BATCH_LINGER_MS` | 10 | 攒批时间 |
| `TC_MYSQL_SHARD_QUEUE` | 64 | 每个库写入线程的任务队列长度 |
| `TC_MYSQL_SHARD_COALESCE_JOBS` | 16 | 每个库的写入线程一次最多合并的任务数 |
| `TC_ORDER_DRAIN_HANDOFF` | true | 由成交写入线程统一合并待处理事件；指令和成交两个写入线程要在同一个进程里 |
| `TC_ORDER_DEDUPE_RETENTION_HOURS` | 24 | 成交事件去重保留 24–48 小时，3 张表按周期轮转、整表清空，必须大于 Kafka 可能重投的最长时间 |
| `TC_ORDER_COMMAND_LOG` | 0 | `processed_commands` 审计表默认不写 |
| `TC_ORDER_PERSIST_MAX_RETRIES` | 10 | 一批写入失败时最多重试次数，用完之后整批回退重新消费 |
| `TC_ORDER_DLQ_PATH`、`TC_ORDER_COMMAND_DLQ_PATH` | 本地持久盘路径 | 死信文件：无法解析的成交事件和指令写到这里，不会被静默丢弃 |
| `TC_ORDER_QUERY_ENABLED` | true | 查询接口（`/orders`、`/fills`） |

### 7.3 MySQL 服务端（每台主库 1 个 mysqld，8 个库）

```ini
[mysqld]
innodb_buffer_pool_size        = 180G      # 内存的 70%
innodb_buffer_pool_instances   = 16
innodb_redo_log_capacity       = 16G
innodb_log_buffer_size         = 256M
innodb_flush_log_at_trx_commit = 1         # 每次提交都刷盘，不能改
sync_binlog                    = 1
innodb_change_buffering        = all       # 订单表的二级索引是随机插入
innodb_io_capacity             = 20000
innodb_io_capacity_max         = 40000
innodb_flush_method            = O_DIRECT
max_connections                = 2000
binlog_format                  = ROW
transaction_isolation          = REPEATABLE-READ   # 投影的正确性依赖它，不能改成 READ-COMMITTED
performance_schema             = OFF
rpl_semi_sync_source_enabled   = 1         # 一主一从，半同步
```

### 7.4 Kafka（15 台）

用脚本生成的 `out/topics.sh` 创建 topic：

| Topic | 分区 | 说明 |
|---|---|---|
| `orders-v2-g0` ~ `orders-v2-g63` | 每个 16 个 | 指令；品种 c 进入第 `c mod 64` 个 topic |
| `trade-order-db-v1` | 400 | 指令按库路由后的落库 topic |
| `trade-executions-v1` | 400 | 成交事件，按订单号对应到库 |
| `trade-execution-outbox-cursors-v1` | 64，压缩 topic | 各组 outbox 的发布进度 |
| `trade-route-control-v1` | 1，压缩 topic | 品种路由和迁移状态 |
| `trade-user-events-v1` | 256 | 推送事件，按用户哈希分区（见 7.5） |

所有 topic 都是 3 副本。Redpanda 在 `acks=all` 下天然要求多数副本确认；如果改用 Apache Kafka，所有 topic 都要额外设 `min.insync.replicas=2`。

### 7.5 用户推送 user-stream（16 台）

每台跑两个进程，都由配置脚本生成：

- **router**（`<host>-router.env`）：16 个 router 组成一个消费者组，分摊读取 `trade-executions-v1` 的 400 个分区，把每条事件原样转发到 `trade-user-events-v1`，分区 = 用户哈希 mod 256，并保留原始时间戳，所以 `since` 续传不受影响。如果单个 router 跟不上（看 `tc_user_stream_events_routed_total` 的增长速度和消费积压），每台可以再多起一个 router，消费者组会自动重新分摊分区。
- **pusher**（`<host>-pusher.env`）：第 i 台只读 `p mod 16 == i` 的分区，只保存和推送这部分用户的事件。客户端连到不负责自己的分片时，返回 307，指向 `TC_USER_STREAM_PEERS` 里对应的地址；负载均衡也可以直接按同样的哈希把用户路由到对应分片。

| 参数 | 值 | 说明 |
|---|---|---|
| `TC_USER_STREAM_ROLE` | router / pusher | 不设置时为单实例模式，读整个成交 topic，只适合小规模 |
| `TC_USER_STREAM_SHARDS` / `TC_USER_STREAM_SHARD` | 16 / 0–15 | 分片总数和本分片编号。user topic 的分区数要 ≥ 分片数，最好是它的整数倍 |
| `TC_USER_STREAM_PEERS` | 16 个 `host:9300` | 按分片顺序排列，客户端能访问到的地址，用于 307 跳转 |
| `TC_USER_STREAM_REPLAY_EVENTS` | 512 | 每个用户保留的事件数，供断线重连补发 |
| `TC_USER_STREAM_REPLAY_SECS` | 300 | 补发窗口 |
| `TC_USER_STREAM_REPLAY_MAX_TOTAL` | 20,000,000 | 单个 pusher 保留的事件总数上限，按每条约 350 字节估算约 7 GB；超过时先淘汰最早的事件 |
| `TC_USER_STREAM_MAX_CONNECTIONS` | 50,000 | 单个 pusher 的连接数上限（每个连接 2 个线程），超过时新连接直接关闭 |
| `TC_USER_STREAM_SINCE_GRACE_MS` | 10000 | 用 `since=` 续传时多往前回放的时间 |

**断线续传**：pusher 推送的每条事件都带 `cursor`（该用户分区内的位点）。客户端重连时带 `after=<最后收到的 cursor>`，精确续传，pusher 重启后也有效。如果续传点之后的事件已经不全（超出窗口、总量淘汰、刚重启还没加载到），第一帧会是 `{"e":"RESYNC"}`，客户端要通过订单查询接口对账。服务端每 30 秒发一次 ping，75 秒收不到客户端任何数据就断开。

**改分片数**：分片数和 user topic 的分区数都不能在线修改，要整体重新部署推送层。router 和 pusher 必须使用同一个分区数（启动时都从 Kafka 读取，不需要单独配置）。

## 8. 品种迁移（热门品种独占一个组）

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://<任一接入机>:9200/admin/routes/migrate?category=<品种>&to=<组>"
```

迁移会自动依次完成：冻结 → 源组封存 → 等源组快照和 outbox 发完 → 目标组装入 → 比对指纹 → 切换路由 → 源组删除。

- 每次约 2–3 秒，期间该品种的指令会暂停，切换后继续处理，不会丢失；
- 同一个消费者负责的其他品种，也会跟着停顿约 2 秒；
- 品种的订单簿、条件单、持仓、熔断状态，以及最近的指令号（用于重投去重）都会一起迁走；
- 任何一步失败都会自动回滚：凡是**发出过**的步骤都会撤销（即使请求超时，实际也可能已经提交），并且一直重试到撤销提交为止；
- 每次迁移有一个纪元号（冻结记录在路由日志里的位置），每一步都带着它。撮合组会拒绝旧迁移或并发迁移的步骤（回报 `stale-migration`），所以两台接入机同时发起迁移、或者迟到的旧步骤，都不会改动正在使用的订单簿；
- 装入数据超过 Raft 单条消息上限的一半（128 MiB）时直接拒绝迁移，不会卡住目标组；
- 本机压测中迁移 3 次（含一次迁回原组），对账结果为 0 丢单、0 重复撮合。

**迁移中途接入机宕机**：品种会一直保持冻结（接入机启动时会在日志里打印 `event=unfinished_migration`）。在任一接入机上执行：

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://<任一接入机>:9200/admin/routes/abort?category=<品种>"
```

它根据路由日志里的冻结记录完成回滚：目标组删除本次装入的数据，源组解封，路由切回源组。重复执行是安全的。

**路由日志不可用**：接入机启动时如果读不到路由日志，会一直重试，不会用旧路由接单。

## 9. 上线前的单机校准（必须做）

| 要校准的数字 | 方法 | 本文假设的值 |
|---|---|---|
| 9.1 单库每秒能落多少笔 | 1 台物理 MySQL + 1 台落库机，`TC_DB_COUNT=8`，压到 MySQL CPU 或刷盘饱和 | 约 8,000 笔/秒 |
| 9.2 单个 Raft 组的吞吐 | 3 台撮合机，1 个组 | 约 20–30 万笔/秒 |
| 9.3 单台 Kafka 的写入带宽 | 3 台 broker | 约 300 MB/s（留 50% 余量） |
| 9.4 单台接入机的受理速度 | 1 台接入机，用 WebSocket 批量发单 | 约 15 万笔/秒 |

具体步骤、命令和测量方法见 [`scripts/calibrate/README.md`](../scripts/calibrate/README.md)。

**台数换算方法**：各层台数 = 目标负载 ÷（实测值 × 0.7）。以 MySQL 为例，假设实测单库只有 5,000 笔/秒：库数 = 200 万 ÷（5,000 × 0.7）≈ 570，向上取整到每台 8 个库的整数倍，即 576 个库（72 台主库）。

## 10. 上线前必须完成的代码改动

1. ~~user-stream 按用户分片~~：已完成，见 7.5。上线前用第 9 节的方法实测单个 router 每秒能转发多少条，以此决定每台起几个 router。
2. 按第 9 节的校准结果调整 `TC_DB_COUNT` 和组数，然后重新运行配置生成脚本。

## 11. 上线步骤

1. 按第 3 节准备机器，所有机器 NTP 时间同步。
2. 用 `gen_config.py` 生成配置，执行 `topics.sh` 创建 topic。
3. 启动 MySQL 主从 → Kafka → 撮合（等 64 个组都选出 leader，`/readyz` 返回 200）→ 落库机（启动时自动建库建表）→ 接入机 → 推送 → 行情。
4. 验收（每次都从空数据开始）：
   - `scripts/bench/chaos.sh`：压测中途 kill -9 撮合机、order-api 和 Kafka，要求 0 丢单、0 重复撮合、副本状态一致；
   - `scripts/bench/migrate.sh`：压测过程中迁移品种；
   - 容量阶梯：20 万 → 50 万 → 100 万 → 200 万 TPS，每档持续 30 分钟，看延迟 p99、MySQL 积压和撮合积压。
5. 监控重点：

| 指标 | 含义 |
|---|---|
| `tc_order_mysql_consumer_lag` | 指令落库积压 |
| `tc_order_match_consumer_lag` | 撮合积压 |
| `tc_execution_outbox_pending` | outbox 未发布事件数 |
| `tc_snapshot_ns_max` | 快照导致的撮合停顿 |
| `tc_order_dedupe_truncated_partitions` | 去重表轮转清理次数 |
| 各 Raft 组的 apply lag 和选举次数 | 撮合健康状况 |
| MySQL 复制延迟 | 从库是否跟得上 |

### 11.1 升级（换新版本）

上线前不保留任何旧格式的兼容代码（快照、日志、消息格式只认当前版本），换版本时清空数据重新部署即可。上线后的升级规则：


- 新版本如果改了撮合结果（`DETERMINISM_VERSION` 变了，例如 2026-11-09 的强平改动），同一个组的副本必须**一起升级**；混跑时配置指纹不一致，副本会拒绝当 leader。
- 升级前每个节点都要**先做一次完整快照**：用 SIGTERM 正常停机（停机时会写最终快照）。不要在 kill -9 或崩溃之后直接换新版本：重启时要按老版本的结果去校验重放的日志，结果不同会触发 "Raft replay mismatch"，节点起不来。崩溃过的节点先用老版本启动一次，再正常停机升级。
