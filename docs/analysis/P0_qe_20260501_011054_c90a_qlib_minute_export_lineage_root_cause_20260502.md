# P0 Qlib 分钟导出链路与数据丢失定位

- 实验：`qe_20260501_011054_c90a`，重点关联 Loop19-28 的 `QLIB_MINUTE_CLOSE_MISSING` 现象。
- 目标：查找历史 Qlib minute bin 导出脚本、保留 CSV、设计/验证文档，并用现有数据确认分钟数据丢失发生在哪一层。
- 约束：本次只读分析；未重跑 QE；未修改 Qlib bin；未补策略日志。

## 结论

当前可以用代码和数据确认：缺失不是当前 DB 分钟线缺失、不是停牌、不是股票池/日历缺失、也不是涨跌停价格精度问题；缺失发生在正式生产目录 `/home/lc999/data/qlib_minute_bin` 的分钟 OHLCV/factor bin 快照层。

更精确地说，`open/high/low/close/volume/amount/factor` 在 2025-07-08 至 2025-07-16 的一批股票上全为 NaN，但同一股票/日期的 DB 分钟线、DB 日线、DB 涨跌停、Qlib 1min calendar、Qlib instrument、以及后续写入的 `prev_close/up_limit_price/down_limit_price` bin 都存在。因此这是一个“不完整的生产 Qlib 分钟 OHLCV/factor 历史基线快照”。

历史验证文档显示正式 `/home/lc999/data/qlib_minute_bin` 是在“原 2024-01-02~2026-03-19 全市场分钟 bin 基础上增量追加到 2026-04-28”。本次缺失日期是 2025-07-08~2025-07-16，早于 2026-03-20 的增量追加区间，所以这些洞不是 2026-03-20~2026-04-28 增量追加造成的，而是已存在于原始历史基线快照里。随后 `dump_limit_price_minute_bins.py` 在 2026-04-30 左右只补写了 limit/pre_close 三类字段，所以才出现“limit/pre_close 有值，但 close/factor 全空”的表象。

当前未找到正式全市场生产导出时保留的 `qlib_minute_prod/csv`、`qlib_minute_full/csv` 或 `/home/lc999/data/qlib_minute_bin_backup_20260429_205315`，因此无法进一步证明最初那次历史基线快照为什么不完整，例如无法区分当时导出 CSV 就已缺行、dump_bin 输入不完整、拷贝/覆盖中断、还是当时 DB 尚未补齐。但可以证明：用当前 DB 和现有导出逻辑生成的候选 10 股 CSV/bin，在同一日期同一股票上是完整的，正式生产 bin 的缺失不是当前源 DB 问题。

## 搜索到的导出脚本与工具

```text
Artifact / Script                                                    Role                                  Current Evidence
-------------------------------------------------------------------  ------------------------------------  ------------------------------------------------------------
scripts/export_minute_full.py                                        早期分钟线 DB->CSV->dump_bin 脚本       目标 `qlib_minute_full`，范围 2025-11~2026-02；目录当前为空
scripts/export_minute_prod.py                                        计划型全市场分钟 DB->CSV->dump_bin 脚本  目标 `qlib_minute_prod/csv` 与 `bin`；产物目录当前未找到
scripts/qlib_full_factor_minute_chain_validate.py                    10股候选链路验证导出脚本                保留 CSV/bin；证明当前 DB 可导出 2025-07 问题日期有效分钟线
scripts/dump_limit_price_minute_bins.py                              分钟 limit/pre_close overlay 工具       只写 `prev_close/up_limit_price/down_limit_price`，不写 OHLCV/factor
.codex_tmp/fast_dump_limit_bins.py                                   快速 limit/pre_close overlay 临时工具   与上面同类；解释 limit 字段为何晚于 close/factor 被补齐
backend/qlib_exporter/router.py::_export_minute_to_csv_for_dump_bin  API 风格 DB->CSV->dump_bin 分支         使用 `QLIB_CSV_ROOT_WIN`；未发现它生成当前正式 minute bin 的证据
backend/qlib_exporter/exporter.py / snapshot_writer.py               HDF5 snapshot 全量/增量写入工具          用于 snapshot API；不是当前正式 Qlib 1min bin 缺口的直接证据
RD-Agent `scripts/dump_bin.py dump_all`                              CSV->Qlib bin 转换工具                  多个脚本/验证报告均调用它生成 Qlib bin
```

## 搜索到的保留 CSV / bin 产物

```text
Path                                                                                       Scope / Count                         Status
-----------------------------------------------------------------------------------------  ------------------------------------  ------------------------------------------------------------
qlib_minute_validation/full_factor_minute_chain_20260428_candidate/csv                     10 CSV，1,347,640 rows                存在；10股候选验证产物
qlib_minute_validation/full_factor_minute_chain_20260428_candidate/bin                     10股 Qlib minute bin                 存在；2025-07 问题日期可读且无 close NaN
qlib_test_minute/csv                                                                       4 CSV，小型旧测试                    存在；范围/股票太小，不能解释本次正式缺口
qlib_minute_full/csv                                                                       早期 2025-11~2026-02 目标目录         不存在；`qlib_minute_full` 根目录当前为空
qlib_minute_prod/csv                                                                       计划型全市场生产 CSV 目录             不存在；未找到正式生产 CSV 快照
/home/lc999/data/qlib_minute_bin_backup_20260429_205315                                    正式数据替换前备份                   历史文档提到，但本机 `/home/lc999/data` 未找到
qlib_csv/qlib_bin_20260428_shsz_candidate                                                  日频候选 CSV                         存在；不是分钟 CSV
```

补充：`.gitignore` 明确忽略 `qlib_minute_full/`、`qlib_minute_prod/`、`qlib_test_minute/`、`qlib_bin/`、`qlib_csv/`，所以这些大数据产物不会随 Git 保存；本地不存在即无法从仓库恢复。

## 历史设计 / 验证文档证据

```text
Document                                                                                         Relevant Evidence
------------------------------------------------------------------------------------------------  ------------------------------------------------------------
tests/aistock_validation/history/qlib_data/20260429_211000_l4-official-qlib-rdagent-dataset-promotion-20260428.md  正式 minute bin = 原 2024-01-02~2026-03-19 基线 + 2026-03-20~2026-04-28 增量追加；仅抽样验证 2026-04-28 和 2026-04-20~2026-04-27 official smoke
tests/aistock_validation/history/qlib_data/20260429_192821_l4_full-factor-minute-execution-chain-validation-20260428.md  10股候选链路验证通过；明确 out of scope: full-market minute bin export 与 production dataset replacement
reports/qlib_full_factor_minute_chain_20260428/minute_export_summary.json                         10股、560交易日、1,347,640分钟行，missing_stock_dates=0，bad_bar_count=0，bad_time_range=0
reports/qlib_full_factor_minute_chain_20260428/report.json                                        候选 1min bin required fields NaN 全为 0；NestedExecutor minute backtest 通过
reports/qlib_official_minute_chain_smoke_20260428.json                                            正式数据 smoke 只覆盖 2026-04-20~2026-04-27，未覆盖 2025-07-08~2025-07-16
```

## 直接对比证据：当前 DB / 正式 bin / 保留候选 CSV

说明：`NN` 表示非空分钟 bar 数；2025-07-08 正常交易日预期为 240。`Official` 是 `/home/lc999/data/qlib_minute_bin`；`Candidate` 是保留的 10股候选 CSV/bin。

```text
Code       Date        DBMin  DBSusp  OfficialCloseNN  OfficialFactorNN  OfficialPrevNN  CandidateCsvNN  CandidateBinNN  Verdict
---------  ----------  -----  ------  ---------------  ----------------  --------------  --------------  --------------  ---------------------------------------------
000001.SZ  2025-07-08  240    0       240              240               240             240             240             基准正常，正式 bin 与候选 bin 都完整
000063.SZ  2025-07-08  240    0       0                0                 240             240             240             正式 OHLCV/factor 缺失；当前 DB 与候选导出完整
000651.SZ  2025-07-08  240    0       0                0                 240             240             240             正式 OHLCV/factor 缺失；当前 DB 与候选导出完整
603185.SH  2025-07-08  240    0       0                0                 240             N/A             N/A             正式 OHLCV/factor 缺失；当前 DB 完整；候选10股未包含
```

样本原始证据保存于 `.codex_tmp/qe_export_gap_lineage_sample_20260502.json`。

## 缺口范围

`docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_gap_all_db_present_stock_dates_20260502.csv` 统计显示，当前 DB 存在分钟线但正式 Qlib 1min `$close` 全空的 stock-date 对集中在 7 个交易日。

```text
Date        DBPresentQlibCloseAllNullPairs
----------  ------------------------------
2025-07-08  2691
2025-07-09  2320
2025-07-10  1559
2025-07-11  1270
2025-07-14  923
2025-07-15  590
2025-07-16  302
```

前序 QE warning 影响范围为 486 个 stock-date pair、157 只股票、同样集中在这 7 个交易日。全市场 DB-present 缺口更大，说明 QE 只是命中了其中一部分股票。

## 文件时间戳证据

```text
Artifact                                                          SizeBytes  MTimeUTC                         Interpretation
----------------------------------------------------------------  ---------  -------------------------------  ------------------------------------------------------------
Official calendars/1min.txt                                      2696140    2026-04-29T13:00:06+00:00      正式 1min calendar 在 2026-04-29 生成/更新
Official instruments/all.txt                                     275750     2026-04-29T13:00:06+00:00      正式 instruments 在 2026-04-29 生成/更新
Official features/000063.sz/close.1min.bin                       539232     2026-04-29T12:56:50+00:00      正式 close bin 先生成；问题日期为 NaN
Official features/000063.sz/prev_close.1min.bin                  539232     2026-04-30T01:00:39+00:00      prev_close 后补写；问题日期有值
Candidate csv/000063.SZ.csv                                      16027116   2026-04-29T11:19:47+00:00      候选 CSV 保留；问题日期有 240 个 close
Candidate bin/features/000063.sz/close.1min.bin                  539060     2026-04-29T11:20:09+00:00      候选 close bin 保留；问题日期有 240 个 close
```

这个时间顺序与代码功能一致：正式 OHLCV/factor bin 先存在缺口，后续 limit/pre_close overlay 工具只补了 `prev_close/up_limit_price/down_limit_price`，没有修复 `close/factor`。

## 为什么当时验证没有发现

```text
Validation Item                                  Coverage                                  Why It Missed The Gap
-----------------------------------------------  ----------------------------------------  ------------------------------------------------------------
正式 WSL minute sample                            000001/000333/300750/600519 on 2026-04-28  只抽样最新一天，未覆盖 2025-07 历史区间
官方 day+minute NestedExecutor smoke              4 stocks, 2026-04-20~2026-04-27           覆盖的是增量尾部，不覆盖 2025-07 历史基线缺口
10股 full-factor minute chain validation           10 stocks, 2024-01-02~2026-04-28         候选 CSV/bin 通过，但明确不是 full-market production replacement
production CSV / export log                        未找到                                    无法回放生产全市场 dump_bin 输入是否完整
```

## 当前可证实与不可证实的边界

```text
Question                                                         Answer
----------------------------------------------------------------  ------------------------------------------------------------
是否 DB 分钟线缺失？                                             否。样本与全市场审计均显示 DB minute rows 和 close_li 存在
是否股票停牌导致？                                               否。DB-present 缺口样本 suspend_d=0，且有日线成交量/分钟线
是否股票池或 calendar 缺失？                                     否。正式 Qlib 返回 240 个 1min offset，instrument/calendar 存在
是否涨跌停/prev_close 字段缺失？                                 否。这三类字段在问题 offset 上非空，且是后续 overlay 写入
是否可确认生产 OHLCV/factor bin 不完整？                         是。正式 close/factor 全 NaN，候选 CSV/bin 和当前 DB 同日同股完整
是否可确认最初那次历史基线快照为何不完整的操作原因？             否。正式生产 CSV、导出日志、备份快照未找到，不能臆测中断/复制/源数据时点问题
```

## 后续最高优先级建议

```text
Priority  Action                                                        Purpose
--------  ------------------------------------------------------------  ------------------------------------------------------------
P0        对当前正式 `qlib_minute_bin` 跑全市场 DB-vs-Qlib 1min coverage gate  在任何修复/重跑前形成完整缺口清单和基准
P0        从当前 DB 全量重建 2024-01-02~2026-04-28 minute bin，并保留 CSV/log/checksum  用当前完整 DB 修复历史基线洞，避免只修补局部后留下未知缺口
P0        重建后验证 2025-07-08~2025-07-16 全市场 OHLCV/factor 非空覆盖        证明本次问题日期已修复
P0        增加 production promotion 前置门禁：DB stock-date rows vs Qlib close non-null  防止抽样 smoke 再次漏掉历史区间缺口
P1        smoke 增加历史随机日期、涨跌停日、高成交/低成交股票、近期增量尾部组合         提高发现局部历史缺口的概率
P1        如果不全量重建，至少对缺口 CSV 中 9,655 个 stock-date 做定向 patch 后再全量审计  降低修复成本，但风险是未扫描日期仍可能有洞
```

## 本次未执行的操作

- 未重跑 QE loop。
- 未修改 `/home/lc999/data/qlib_minute_bin`。
- 未生成新的生产 Qlib bin。
- 未修改策略或新增回测日志。
