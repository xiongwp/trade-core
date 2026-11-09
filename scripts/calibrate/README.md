# 上线前的单机校准

部署文档第 9 节里的 4 个数字（单库落库速度、单组撮合吞吐、Kafka 写入带宽、单台接入机受理速度）都是估算值，必须在真实机器上实测后，再用实测值换算各层台数。

## 工具

- `scripts/calibrate/rates.py`：采样任意服务的 Prometheus 指标，计算吞吐。只有在积压指标不涨的前提下，这个吞吐才算可持续：
  - 输出 `SUSTAINABLE`：说明还没压满，加大压力再测一轮；
  - 输出 `SATURATED`：说明已经压满，可持续吞吐低于本轮的平均值。
- `cargo bench --bench user_sim_e2e`：模拟用户下单的压测客户端（参数见文件开头的说明）。用 `TC_SIM_TRANSPORT=ws` 走 WebSocket 批量下单。
- `cargo bench --bench kafka_produce`：Kafka 写入带宽测试。生产者参数和撮合节点的发布线程完全相同。

**所有压测都从空数据开始，每档至少持续 5 分钟。** 压测客户端要单独放在一台机器上，避免和被测服务抢 CPU。

## 9.1 单库每秒能落多少笔

**部署**：
- 1 台 MySQL 主库，`TC_DB_COUNT=8`；
- 1 台落库机，配置 `TC_MYSQL_OWNED_DBS=0-7`；
- 撮合组和接入机的数量要足够多，确保它们不是瓶颈（不够就先加，直到它们的积压保持不涨）。

**加压**：在压测机上逐档提高并发，例如 16、32、64、128：

```bash
TC_SIM_TRANSPORT=ws cargo bench --bench user_sim_e2e -- in01:9200 $TOKEN 20000000 100000 1000 64 5000 mx01:9102,mx02:9112
```

倒数第二个参数是每个组任选一个副本的 metrics 地址，用逗号分隔。

**测量**：

```bash
python3 scripts/calibrate/rates.py --seconds 300 \
  --endpoint pj01:9200 \
  --counter tc_execution_mysql_completed_events \
  --endpoint in01:9200 --gauge tc_order_mysql_consumer_lag
```

积压指标由接入机上报，所以 `--gauge` 要配接入机的地址。

**换算**：成交事件写入速度 ÷ 每笔指令平均产生的事件数（本机实测约 3.2）÷ 8，就是单库每秒能落多少笔指令。

## 9.2 单个 Raft 组的吞吐

**部署**：3 台撮合机，只放 1 个组。落库机先不启动（`TC_ORDER_KAFKA_DB_CONSUMERS=0`、`TC_EXECUTION_KAFKA_MYSQL_CONSUMERS=0`），只测撮合本身。

**加压**：同 9.1，但所有品种都要落在这个组里（`STRIDE=1`）：

```bash
TC_SIM_TRANSPORT=ws cargo bench --bench user_sim_e2e -- in01:9200 $TOKEN 20000000 100000 1000 64 5000 mx01:9102 1
```

**测量**：在 leader 上测量。leader 是 `/metrics` 里 `tc_raft_role 2` 的那台。指令数等于受理、撤单、拒绝、改单四个计数之和（`tc_command_latency_samples` 是按批计数的，不能用）。

```bash
python3 scripts/calibrate/rates.py --seconds 300 \
  --endpoint mx01:9102 \
  --counter tc_orders_accepted --counter tc_cancels --counter tc_rejects --counter tc_modifies \
  --endpoint in01:9200 --gauge tc_order_match_consumer_lag
```

## 9.3 Kafka 写入带宽

**部署**：3 台 broker。建一个和生产配置相同的 topic（3 副本，分区数同 `TC_EXECUTION_KAFKA_PARTITIONS`）。

**测量**：在 2–4 台客户端机器上同时运行下面的命令，把各台的 MB/s 加起来：

```bash
cargo bench --bench kafka_produce -- kf01:9092,kf02:9092,kf03:9092 trade-executions-v1 300 8 4096
```

逐步增加客户端数量或线程数，直到总带宽不再上升。这时 broker 的磁盘或网卡应该已经接近饱和，用 `iostat` 和 `sar -n DEV` 确认。

**换算**：取总带宽的一半作为单台 broker 的可用带宽（留 50% 余量）。

## 9.4 单台接入机的受理速度

**部署**：1 台接入机。后面的撮合组、Kafka、落库都要足够多，不能成为瓶颈。

**加压**：用 2–3 台压测机同时向这台接入机发单。

**测量**：

```bash
python3 scripts/calibrate/rates.py --seconds 300 \
  --endpoint in01:9200 --counter tc_order_published_commands \
  --gauge tc_order_publish_inflight
```

同时看这台接入机的 CPU 和网卡流量：先打满的那个就是它的上限。

## 换算台数

各层台数 = 目标负载 ÷（实测值 × 0.7）。把结果填进 `inventory.json`，然后重新运行 `scripts/deploy/gen_config.py`。注意：库数一旦改变，必须同时升级 `route_version`。
