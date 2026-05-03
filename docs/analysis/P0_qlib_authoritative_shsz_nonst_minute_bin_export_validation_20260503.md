# P0 权威 SH/SZ non-ST 分钟线 Qlib Bin 导出与验证报告（2026-05-03）

## 结论

本次已按最新规则在 WSL 本地生成新的权威分钟线 Qlib bin：`/home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428`。截至本报告记录，导出、dump、股票池规则、全量 CSV-vs-bin、抽样 DB-vs-bin、小规模 Qlib 回测、v25 涨跌停读取和 UI/API 配置验证均通过，未发现本次新导出数据存在字段缺失、涨跌停口径不一致、北交所/ST/退市股票混入、IPO 过滤误删 feature bin 的问题。

当前没有删除任何候选旧数据；旧数据删除候选记录见 `docs/analysis/backup_and_legacy_data_cleanup_candidates_20260503.md`。

## 本次权威导出规则

```text
Rule                         Result
---------------------------  ------------------------------------------------------------
交易所范围                   只导出 SH/SZ；BJ/BSE fail-fast，不导出
退市/暂停上市                不导出；stock_basic.list_status 必须为 L
ST 股票                      不导出；只要 market.stock_st 有 ST 记录即排除
新股上市未满一年             feature bin 保留上市后全部数据；只在 instruments/all.txt 过滤
all.txt 新股过滤规则          start = max(data_start, list_date + 365 days)
复权 OHLC                    使用 DB raw OHLC / 1000 * qfq_factor
qfq_factor                   使用对应股票、对应日期的 market.adj_factor / basis window max(adj_factor)
涨跌停价/pre_close            使用 market.stk_limit 原始价格口径，不做复权
limit_up/limit_down 标记      使用 raw close 与 raw up_limit/down_limit 比较
缺失关键字段                 strict_limit=True；缺失复权或涨跌停数据直接失败
```

## 导出产物

```text
Item                         Value
---------------------------  ------------------------------------------------------------
Snapshot ID                  qlib_minute_authoritative_shsz_nonst_full_20260428
Qlib bin dir                 /home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428
CSV dir                      /home/lc999/data/qlib_csv_authoritative/qlib_minute_authoritative_shsz_nonst_full_20260428/stock_minute_1min
Start                        2024-01-02
End                          2026-04-28
Basis start                  2024-01-02
Basis end                    2026-04-28
CSV files                    4692
CSV rows                     618578712
Feature stock dirs           4692
Feature bin files            56304
Calendar 1min rows           134807
all.txt rows after IPO rule  4612
Bin size                     28G
CSV size                     65G
```

导出报告：

```text
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_export.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_dump.json
/home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428/meta_export.json
/home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428/instruments/all_ipo_filter_summary.json
```

## 股票池规则审计

```text
Audit                        Result
---------------------------  ------------------------------------------------------------
Script                       scripts/qlib_stock_universe_policy_audit.py
Output                       reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_universe_policy_audit.json
OK                           true
Feature stock count          4692
all.txt stock count          4612
IPO-young feature count      80
Missing from all.txt count   80
BJ/BSE feature dirs          0 errors
ST-record feature dirs       0 errors
Non-active listing dirs      0 errors
Warnings                     0
Errors                       0
```

样例新股（feature bin 存在，但因上市未满一年从 `all.txt` 过滤）：`001312.SZ`、`301449.SZ`、`301513.SZ`。这证明新股数据没有从 bin 中删除，回测可交易股票池限制通过 `all.txt` 实现。

## 全量 CSV-vs-bin 审计

```text
Audit                        Result
---------------------------  ------------------------------------------------------------
Script                       scripts/qlib_authoritative_csv_bin_audit.py
Output                       reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_csv_bin_audit.json
OK                           true
CSV files                    4692
Checked stocks               4692
Checked CSV rows             618578712
Checked field values         7422944544
Calendar rows                134807
Instrument rows              4612
Error count                  0
```

该审计逐字段确认 `open/high/low/close/volume/amount/factor/up_limit_price/down_limit_price/prev_close/limit_up/limit_down` 从 CSV dump 到 Qlib bin 后无差异。

## DB-vs-bin 抽样审计

```text
Sample                       Rows/Values Checked                              Result
---------------------------  ------------------------------------------------  ----------------
Mature stocks                5 stocks, 35 stock-days, 8400 rows, 100800 values  OK, max diff 0
IPO young 301449.SZ          2 stock-days, 480 rows, 5760 values               OK, max diff 0
IPO young 001312.SZ          6 stock-days, 1440 rows, 17280 values             OK, max diff 0
Limit event sample           4 stocks, 4 stock-days, 964 rows, 11568 values    OK, max diff 0
```

报告路径：

```text
reports/qlib_authoritative_export/db_validate_mature/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/db_validate_ipo_301449/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/db_validate_ipo_001312/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/db_validate_limit_events/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
```

## 涨跌停与价格口径验证

本次使用“DB raw price -> Qlib qfq price + Qlib factor -> v25 转回 raw price”的链路验证。验证结论：OHLC 与 factor 使用前复权口径；`prev_close/up_limit_price/down_limit_price` 使用 raw 口径；v25 通过 `close_qfq / factor` 转回 raw 后与 raw 涨跌停价比较，未发现标记不一致。

```text
Validation                   Result
---------------------------  ------------------------------------------------------------
Limit event DB-vs-bin        000153.SZ,001236.SZ,002080.SZ,600110.SH on 2025-07-01; max diff 0
V25 limit event smoke        ok=true; rows_checked=964; data_error_count=0; flag_mismatch_count=0
Limit-up states observed     limit_up_buy_blocked=209; p0_limit_sell_at_up_limit=209
Limit-down states observed   limit_down_sell_blocked=112; p0_limit_buy_at_down_limit=112
Regular V25 smoke            ok=true; rows_checked=4320; data_error_count=0; flag_mismatch_count=0
```

报告路径：

```text
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_v25_limit_state_smoke.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_v25_limit_event_smoke.json
```

## 小规模 Qlib 回测验证

```text
Item                         Value
---------------------------  ------------------------------------------------------------
Script                       scripts/qlib_authoritative_smoke_backtest.py
Output                       reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_backtest.json
OK                           true
Codes                        000001.SZ, 000063.SZ, 600519.SH
Window                       2025-07-08 to 2025-07-15
Minute rows                  4320
Minute NaN count             all required fields = 0
Portfolio report             found
Report rows                  6
First account                1000000.0
Last account                 1004474.9876913929
Return NaN                   0
Indicator keys               1day, 1min
```

## AIstock UI/API 验证

```text
Validation                   Result
---------------------------  ------------------------------------------------------------
Frontend payload             Qlib bin 导出固定发送 exclude_st=true, exclude_delisted_or_paused=true
Frontend exchange UI         BJ 选项禁用并固定排除
Frontend sample filters      ST 与退市/暂停上市复选框禁用并固定 true
API smoke                    unified_export_v2 monkeypatch smoke passed
API smoke output             reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_ui_api_payload_smoke.json
TypeScript                   npm exec tsc -- --noEmit passed
Backend tests                python -m pytest backend/tests/test_qlib_export_stock_universe_filters.py -q passed
```

API smoke 验证了 UI/API 传入 full stock_minute 导出时，后端收到 `exclude_st=true`、`exclude_delisted_or_paused=true`、`exchanges=[sh,sz]`，并且输出 meta 包含：

```text
exclude_st                               true
exclude_delisted_or_paused               true
exclude_bj                               true
ipo_filter_mode                          instruments_all_txt
bin_contains_pre_ipo_filter_data         true
stock_universe_min_listed_days_prefilter false
```

## 代码级修复与保护点

```text
File                                                Change
--------------------------------------------------  ------------------------------------------------------------
backend/qlib_exporter/authoritative_bin_exporter.py 股票 universe 不再按 IPO 365 天预过滤；新增 all.txt IPO rewrite；ST/退市过滤 fail-fast
backend/qlib_exporter/router.py                     bin export/unified export 调用 IPO all.txt rewrite；meta 标记权威规则
scripts/qlib_authoritative_bin_export.py            CLI dump 后调用 IPO all.txt rewrite 并写入 meta
frontend/src/app/qlib/page.tsx                      UI 固定排除 BJ/ST/退市暂停上市，payload 固定传 true
backend/tests/test_qlib_export_stock_universe_filters.py 覆盖 SH/SZ、BJ fail-fast、ST/退市 fail-fast、IPO all.txt rewrite
scripts/qlib_stock_universe_policy_audit.py         新增股票池规则审计脚本
scripts/qlib_v25_limit_state_smoke.py               新增 v25 raw price 涨跌停读取 smoke 脚本
```

## 本次执行命令摘要

```text
Command/Check                                                    Result
---------------------------------------------------------------  ----------------
python -m py_compile backend/qlib_exporter/... scripts/...       PASS
python -m pytest backend/tests/test_qlib_export_stock_universe_filters.py -q  PASS, 6 passed
npm exec tsc -- --noEmit                                        PASS
scripts/qlib_stock_universe_policy_audit.py                     PASS
scripts/qlib_authoritative_csv_bin_audit.py                     PASS
scripts/qlib_authoritative_bin_export.py --stage validate       PASS on all samples
scripts/qlib_authoritative_smoke_backtest.py                    PASS
scripts/qlib_v25_limit_state_smoke.py                           PASS
```

## 可信度与剩余风险

```text
Area                         Current status
---------------------------  ------------------------------------------------------------
本次新分钟线 bin              高可信；已完成全量 CSV-vs-bin 和多类 DB-vs-bin 抽样
全量 DB-vs-bin                未跑全量 6.18 亿分钟行直接 DB 对比；原因是 IO/DB 成本较高
源 DB 数据                    本次验证假设 DB 为权威源；未重新审计 Tushare/TDX 入库链路
生产 active provider          未切换；当前仅生成新 WSL 本地权威候选目录
旧无效数据                    未删除；需用户确认后按清理候选文档逐项删除
远端节点                      本次未同步远端；远端若要使用需单独 rsync 后跑同类审计
```

建议下一步在用户确认后执行：

```text
Priority  Action
--------  ------------------------------------------------------------
P0        如需作为 QE active provider，先把该目录切换/同步到 QE 使用路径并跑一次真实小规模 QE smoke
P0        确认后删除旧 BJ/ST 混入版本与 CSV 备份，释放 WSL 空间
P1        对远端节点同步相同 bin 后运行 stock universe + CSV/bin spot check + v25 smoke
P1        如需更高审计强度，再执行分月份/分股票批次 DB-vs-bin 抽样扩大覆盖
```
