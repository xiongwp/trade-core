# Prometheus for trade-core

`trade-core-alerts.yml` holds the alerting rules. They need four scrape jobs
with these exact names; `scripts/deploy/gen_config.py` writes the matching
target files (`prometheus-raft.json`, `prometheus-order-api.json`,
`prometheus-user-stream.json`, `prometheus-market-data.json`) next to the
per-host `.env` files.

```yaml
rule_files:
  - /etc/prometheus/trade-core-alerts.yml

scrape_configs:
  - job_name: trade-core-raft          # every replica of every Raft group
    scrape_interval: 5s
    file_sd_configs: [{files: [/etc/prometheus/targets/prometheus-raft.json]}]
  - job_name: trade-core-order-api
    file_sd_configs: [{files: [/etc/prometheus/targets/prometheus-order-api.json]}]
  - job_name: trade-core-user-stream
    file_sd_configs: [{files: [/etc/prometheus/targets/prometheus-user-stream.json]}]
  - job_name: trade-core-market-data
    file_sd_configs: [{files: [/etc/prometheus/targets/prometheus-market-data.json]}]
```

Each Raft target carries a `raft_group` label. Most matching-node metrics
have no group label of their own, and the per-group alerts (no leader, lost
majority, replica lag) aggregate by it — keep it if you write targets by hand.

Thresholds follow `scripts/ops/health.py` and the SLOs in
`docs/production-deployment-5m-tps.md`; change them in one place together.
`critical` means trading on some categories has stopped or data is at risk
(page someone); `warning` means degraded.
