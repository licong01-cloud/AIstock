# P0 Qlib 权威分钟线导出工具与验证记录（2026-05-03）

## 结论

本次已经把 AIstock 的 Qlib 日线 / 1min 分钟线 bin 导出链路改为可复现、可审计、字段完整、无静默兜底的权威链路：AIstock DB -> per-stock CSV -> Microsoft/RD-Agent `dump_bin.py` -> Qlib bin。

已完成并验证：

- 新增权威导出模块：`backend/qlib_exporter/authoritative_bin_exporter.py`。
- 新增 CLI 导出工具：`scripts/qlib_authoritative_bin_export.py`。
- 新增 CSV-vs-bin 全量覆盖审计：`scripts/qlib_authoritative_csv_bin_audit.py`。
- 新增 Qlib 1min smoke 回测工具：`scripts/qlib_authoritative_smoke_backtest.py`。
- 更新 FastAPI Qlib bin 导出接口，股票日线和 1min 分钟线都改为权威 per-stock CSV -> `dump_bin.py`。
- 更新 Qlib 页面 UI，支持在 `Bin 数据集` 中选择 `股票分钟线 1min`。
- 完成小规模日线 / 分钟线 CSV -> bin -> 逐字段 DB 对比验证，最大误差为 0。
- 完成全量 1min CSV 导出、官方 dump_bin 生成、全量 CSV-vs-bin 覆盖审计、重点样本逐字段 DB 对比、Qlib 1min NestedExecutor 回测。

## 之前导出缺失的原因定位

从已有文档和脚本记录可以证明：

```text
证据项                                      结论
------------------------------------------  ------------------------------------------------------------
qlib_bin_20260428_shsz_candidate/meta       该目录仅包含 daily，freq_types=["daily"]，不是分钟线全量版本
scripts/export_qe_qlib_candidate.py         只执行 dump_bin.py --freq day，用于日线候选数据
reports/qlib_minute_bin_incremental_*.json  生产分钟线是旧 full base + 2026-03-20~2026-04-28 增量追加
P0_qe_*_qlib_minute_export_lineage 文档      2025-07-08~2025-07-16 缺口位于旧历史 base，不是 4/29 增量产生
qlib_full_factor_minute_chain_validate.py   10 股票样本证明当前 DB + 正确公式可导出有效 2025-07 分钟数据
```

因此，之前缺失不是 DB 完全缺数据，而是旧生产 Qlib 分钟线历史 base 中 OHLCV/factor 字段不完整；旧 full base 的原始 CSV、日志和备份没有找到，不能继续猜测具体操作原因。

## 旧导出程序的关键问题

```text
文件/接口                                   问题
------------------------------------------  ------------------------------------------------------------
scripts/export_minute_prod.py               UTC/时区风险；缺 prev_close/up_limit/down_limit；缺严格校验；只打印 dump 命令
router._export_minute_to_csv_for_dump_bin   曾经导出单个 minute_1m_all.csv；freq 口径为 1m；字段不满足 QE/V25
旧日线 bin API                              只导出 OHLCV/factor，缺 QE/V25 需要的涨跌停与 prev_close 字段
旧增量 stock bin                            未记录 qfq basis；如果随 end 延长会和全量重建产生复权口径不一致风险
```

## 新权威导出公式

```text
字段                    公式/来源
----------------------  ------------------------------------------------------------
date                    trade_time AT TIME ZONE 'Asia/Shanghai'，格式 YYYY-MM-DD HH:MM:SS
symbol                  ts_code，例如 000001.SZ
factor                  market.adj_factor.adj_factor / 该股票 basis 窗口 max(adj_factor)
open/high/low/close     kline_*_raw 的厘价格 / 1000 * factor
volume                  volume_hand * 100 / factor
amount                  amount_li / 1000
prev_close              market.stk_limit.pre_close；缺失时仅按明确规则补齐并记录计数
up_limit_price          market.stk_limit.up_limit
down_limit_price        market.stk_limit.down_limit
limit_up                raw close >= up_limit_price - 1e-4
limit_down              raw close <= down_limit_price + 1e-4
```

`prev_close` 补齐规则不是静默兜底，导出摘要会记录计数：

```text
场景                                      规则
----------------------------------------  ------------------------------------------------------------
确认 suspend_d 停牌且分钟/日线成交量为 0  使用当日 raw close 补齐 prev_close，记录 suspended_prev_close_filled_rows
stk_limit.pre_close 缺失但有历史日线 close 使用该股票该日期之前最近一个有效 daily close，记录 previous_daily_prev_close_filled_rows
其他复权/涨跌停/字段缺失                  直接失败，不导出成功结果
```

## 小规模验证结果

```text
验证项                                      结果
------------------------------------------  ------------------------------------------------------------
分钟线样本                                  000001.SZ, 000063.SZ, 600519.SH；2025-07-08~2025-07-16
分钟线 CSV 行数                             5,040
分钟线逐字段 DB vs Qlib bin 最大误差         open/high/low/close/volume/amount/factor/limit 字段全部为 0
分钟线缺失字段                              0
日线样本                                    同上
日线 CSV 行数                               21
日线逐字段 DB vs Qlib bin 最大误差           全部为 0
Qlib 1min smoke 回测                         NestedExecutor + SimulatorExecutor(1min) 正常返回 portfolio report
Smoke 回测分钟字段 NaN                       0
Smoke 回测 portfolio rows                    6
Smoke 回测 last_account                      1,004,474.9876913929
```

## 全量导出结果

```text
项目                                      值
----------------------------------------  ------------------------------------------------------------
Snapshot ID                               qlib_minute_authoritative_full_20260428
CSV 目录                                  /home/lc999/data/qlib_csv_authoritative/qlib_minute_authoritative_full_20260428/stock_minute_1min
Bin 目录                                  /home/lc999/data/qlib_minute_authoritative_full_20260428
日期范围                                  2024-01-02 ~ 2026-04-28
交易所                                    SH, SZ, BJ
复权 basis                                2024-01-02 ~ 2026-04-28
CSV 文件数                                5,515
CSV 行数                                  700,457,459
CSV 体积                                  73G
Bin features 文件数                       66,180 = 5,515 * 12
Bin 体积                                  32G
1min calendar 行数                        134,807
instrument 行数                           5,515
skipped_no_price_rows                     0
suspended_prev_close_filled_rows          151,847
previous_daily_prev_close_filled_rows     241
```

全量导出期间发现并修复了一个新的数据边界：`688766.SH` 在 `2025-11-27` 和 `2025-12-02` 有零成交分钟记录，`stk_limit.up_limit/down_limit` 存在但 `pre_close` 为空；代码现在用该股票该日期之前最近一个有效日线 close 补齐 `prev_close`，并记录在 `previous_daily_prev_close_filled_rows` 中。针对该边界的逐字段验证通过，最大误差为 0。

## 全量验证结果

```text
验证层级                                  结果
----------------------------------------  ------------------------------------------------------------
CSV 生成阶段                              5,515 股票、700,457,459 行全部通过严格字段非空检查
官方 dump_bin                             returncode=0，生成 calendars/1min.txt、instruments/all.txt、features
CSV-vs-bin 全量覆盖审计                   5,515 股票、8,405,489,508 个字段值，error_count=0
重点缺口样本 DB-vs-bin                    2025-07-08~2025-07-16，3 股票，60,480 字段值，最大误差 0
688766 prev_close 边界 DB-vs-bin          2025-11-27~2025-12-02，2 stock-date，2,892 字段值，最大误差 0
301449 上市日期边界 DB-vs-bin             2025-12-23~2025-12-24，2 stock-date，5,760 字段值，最大误差 0
最新日期样本 DB-vs-bin                    2026-04-28，5 股票，14,400 字段值，最大误差 0
Qlib 1min NestedExecutor smoke 回测        2025-07-08~2025-07-15，minute_nan 全部为 0，portfolio rows=6
```

验证报告文件（本地 `reports/` 目录被 gitignore，不随代码提交）：

- `reports/qlib_authoritative_export/qlib_minute_authoritative_full_20260428_stock_minute_export.json`
- `reports/qlib_authoritative_export/qlib_minute_authoritative_full_20260428_stock_minute_dump.json`
- `reports/qlib_authoritative_export/qlib_minute_authoritative_full_20260428_csv_bin_audit.json`
- `reports/qlib_authoritative_export/qlib_minute_authoritative_full_20260428_stock_minute_validate.json`
- `reports/qlib_authoritative_export/qlib_minute_authoritative_full_20260428_backtest.json`

## UI 与 API 行为

- UI：`frontend/src/app/qlib/page.tsx` 的 Qlib bin 导出区域新增 `股票分钟线 1min` 选择项。
- API：`/api/v1/qlib/bin/unified_export_v2` 支持 `datasets` 包含 `stock_minute`。
- 输出：股票分钟线统一使用 Qlib 官方频率 `1min`，不再使用 `1m` 作为 bin freq。
- 元数据：`meta_export.json` 记录 `basis_start/basis_end/freq_types/last_end_dates/required_minute_fields`。
- 增量：stock 日线 / 分钟线如果需要扩展 `basis_end`，接口会失败并要求全量重建，避免复权口径混合。
- CSV 暂存：UI/API 每次导出会清理该 dataset 的临时 CSV 目录，避免旧 CSV 残留混入新 dump。

## 关键命令

```bash
# 全量 CSV 导出（本次先 overwrite，失败后用 resume_csv 从已有 CSV 安全续跑）
PYTHONPATH=/mnt/f/Dev/AIstock TDX_DB_PASSWORD=*** PYTHONWARNINGS=ignore \
python scripts/qlib_authoritative_bin_export.py \
  --dataset stock_minute \
  --stage export \
  --snapshot-id qlib_minute_authoritative_full_20260428 \
  --start 2024-01-02 \
  --end 2026-04-28 \
  --basis-start 2024-01-02 \
  --basis-end 2026-04-28 \
  --exchanges sh,sz,bj \
  --csv-root /home/lc999/data/qlib_csv_authoritative \
  --bin-root /home/lc999/data \
  --minute-chunked-export \
  --minute-code-batch-size 100 \
  --minute-chunk-months 3 \
  --resume-csv

# 官方 dump_bin 生成 1min bin
PYTHONPATH=/mnt/f/Dev/AIstock TDX_DB_PASSWORD=*** PYTHONWARNINGS=ignore \
python scripts/qlib_authoritative_bin_export.py \
  --dataset stock_minute \
  --stage dump \
  --snapshot-id qlib_minute_authoritative_full_20260428 \
  --start 2024-01-02 \
  --end 2026-04-28 \
  --basis-start 2024-01-02 \
  --basis-end 2026-04-28 \
  --exchanges sh,sz,bj \
  --csv-root /home/lc999/data/qlib_csv_authoritative \
  --bin-root /home/lc999/data \
  --dump-workers 16

# 全量 CSV-vs-bin 覆盖审计
python scripts/qlib_authoritative_csv_bin_audit.py \
  --csv-dir /home/lc999/data/qlib_csv_authoritative/qlib_minute_authoritative_full_20260428/stock_minute_1min \
  --qlib-dir /home/lc999/data/qlib_minute_authoritative_full_20260428 \
  --freq 1min \
  --workers 8
```

## 剩余说明

- 官方 `check_data_health.py --qlib_dir ... --freq 1min` 在 Fire 默认调用下会加载数据但不自动执行 `check_data`，因此本次最终以更直接的 `CSV-vs-bin` 全量审计、重点 DB-vs-bin 逐字段验证和 Qlib 回测作为有效验收依据。
- 由于 `reports/` 被 gitignore，最终 GitHub 提交包含代码、UI、分析文档和测试记录；大型导出产物保留在 WSL `/home/lc999/data`。
