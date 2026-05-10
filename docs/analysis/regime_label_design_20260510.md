# market.regime_label 设计与计算脚本（T10）

> **作者**：Claude Code 战略 session 2026-05-10
> **状态**：DRAFT 骨架（T10 prep），DDL 未跑、脚本未接入 cron
> **关联文档**：`docs/architecture/data_warehouse_extension_design_20260510.md` v2.1 §8

## §1 工作面归属

- **schema**：`market`（不在 qe_archive，**不需 Codex 协商**）
- **数据源**：`market.index_daily`（外部 Tushare 注入，已存在）
- **消费方**：`qe_archive.paper_v2_daily_snapshot.regime` 字段在 ETL 时 join `market.regime_label` 拉值
- **触发方式**：每日盘后定时（不走 outbox），独立 Python 脚本

## §2 文件清单（本批次新增，仅 draft）

| 路径 | 用途 | 状态 |
|---|---|---|
| `backend/db/init_market_regime_label_20260510.sql` | DDL | DRAFT，未应用 |
| `scripts/regime_label_daily.py` | 计算脚本 | SKELETON，未接入 cron |
| `docs/analysis/regime_label_design_20260510.md` | 本设计说明 | DRAFT |

## §3 simple_quadrant 第一版方法

```
input:  CSI300 daily close
        过去 5 年历史

steps:
  1. ret_6m = close[T] / close[T-126] - 1
  2. vol_60d = stddev(log_return[T-60..T]) * sqrt(252)
  3. ret_pct_5y = percentile_rank(ret_6m, history='5y')
  4. vol_pct_5y = percentile_rank(vol_60d, history='5y')
  5. classify:
     ret_pct > 0.6 AND vol_pct < 0.4  → bull
     ret_pct < 0.4 AND vol_pct > 0.6  → bear
     vol_pct > 0.6                     → high_vol
     vol_pct < 0.4                     → low_vol
     else                              → oscillation
  6. confidence = sqrt((ret_pct - 0.5)^2 + (vol_pct - 0.5)^2) / 0.5  -- 距象限中心
output: (trade_date, regime, confidence, simple_quadrant, raw signals JSON)
```

## §4 多方法并存（PRIMARY KEY 设计）

`PRIMARY KEY (trade_date, source_method)` 允许同一交易日多种方法标签共存：
- `simple_quadrant`（本批次实现，第一版）
- `hmm_viterbi`（P2，复用 `hmm_viterbi_forward_filter_fix` 修复版）
- `bbq`（P2，Bry-Boschan 季度周期）
- `ensemble`（P3，跨方法多数加权）

ETL 时默认取 `source_method='simple_quadrant'`（可在 ETL 配置里切换）。

## §5 接入 paper_v2 的关联

```sql
-- ETL 写入 paper_v2_daily_snapshot 时同步打 regime
UPDATE qe_archive.paper_v2_daily_snapshot AS pds
SET regime = rl.regime
FROM market.regime_label AS rl
WHERE pds.trade_date = rl.trade_date
  AND rl.source_method = 'simple_quadrant'
  AND pds.regime IS NULL;
```

或在 handler 写入时直接 LEFT JOIN 拉值。

## §6 缺口 / TODO

1. `fetch_percentile` 函数当前 `NotImplementedError`，需实现 5 年历史 percentile rank（窗口查询或 in-memory）
2. `market.index_daily` 的实际列名 / index_code 约定需与 Tushare 注入脚本对齐
3. cron 接入：建议每日 16:00 后跑（数据落地后）
4. backfill：脚本 `--backfill --start 2024-01-01 --end 2026-05-10` 一次性补历史

## §7 启动条件

dw-foundation worktree 启动后（等 Codex D5 答复）由数仓团队接手。本批次的 DDL + skeleton 是预备工作。**不需要等 Codex**——可以独立推进 fetch_percentile 实现 + cron 接入。

下一步可派给数仓团队或战略 session 续做。
