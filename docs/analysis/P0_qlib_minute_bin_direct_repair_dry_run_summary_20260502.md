# P0 Qlib Minute Bin Direct Repair Dry-Run Summary - 2026-05-02

本报告记录本轮“只读 dry-run”的执行结果。当前没有写入、覆盖或重建任何 Qlib bin 文件。

## Scope

```text
Item                         Value
---------------------------  ------------------------------------------------------------
QlibMinuteUri                /home/lc999/data/qlib_minute_bin
InputUniverse                docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_gap_all_db_present_stock_dates_20260502.csv
Mutation                     none
Commands                     scan, build-plan, verify-plan
WritePhase                   not implemented in this step
SkillTool                    qe_qlib_minute_bin_repair.py
```

## Confirmed Gap

```text
Metric                       Value
---------------------------  ----------------------------
GapStockDatePairs            9655
UniqueStocks                 2696
UniqueDates                  7
DateStart                    2025-07-08
DateEnd                      2025-07-16
PatchableCandidates          9655
SkippedInPlan                0
UniqueFieldFilesInPlan       24264
```

缺失字段不是 `prev_close/up_limit_price/down_limit_price`，而是正式分钟 OHLCV/factor 交易字段。历史 overlay 只补过涨跌停价格类字段，没有补齐下面这些字段。

```text
Field                        MissingStockDatePairs
---------------------------  ---------------------
open                         9655
high                         9655
low                          9655
close                        9655
volume                       9655
amount                       9655
factor                       9655
limit_up                     9655
limit_down                   9655
```

## Factor Basis Verification

最初用“当前 DB max(adj_factor)”直接验证 Qlib `$factor` 会出现 33 只股票不一致；这不是修复计划失败，而是证明当前 DB 的最大复权因子与官方 Qlib minute bin 已写入的 factor denominator 存在漂移。后续验证改为从相邻非空 Qlib `$factor` 和当前 DB `adj_factor` 反推官方 denominator，并检查 denominator 在 gap 前后是否稳定。

```text
Metric                                      Value
------------------------------------------  ----------------------------------------------------------------------
VerifyOK                                    True
CheckedRecords                              9655
CheckedStocks                               2696
AdjacentFactorSamples                       84628
Failures                                    0
Warnings                                    0
FactorBasisMethod                           infer_official_denominator_from_adjacent_qlib_factor_and_db_adj_factor
DbMaxDiffersFromInferredStocks              33
PlannedMissingDateFactorMin                 0.6711399555206299
PlannedMissingDateFactorMax                 1.0
```

33 只股票的含义：如果修复时直接用当前 DB max(adj_factor) 作为 denominator，会造成与现有 Qlib bin 前后不一致；因此后续写入阶段必须使用已验证的官方 inferred denominator，不能静默 fallback 到 DB max(adj_factor)。

## Generated Evidence

```text
FileType                     Path
---------------------------  --------------------------------------------------------------------------------
ScanMarkdown                 docs/analysis/P0_qlib_minute_bin_gap_scan_20260502.md
ScanCSV                      docs/analysis/P0_qlib_minute_bin_gap_scan_20260502.csv
FieldMatrixCSV               docs/analysis/P0_qlib_minute_bin_gap_field_matrix_20260502.csv
DryRunPlanMarkdown           docs/analysis/P0_qlib_minute_bin_patch_plan_dry_run_20260502.md
VerifyMarkdown               docs/analysis/P0_qlib_minute_bin_patch_plan_verify_20260502.md
FactorBasisCSV               docs/analysis/P0_qlib_minute_bin_factor_basis_20260502.csv
LocalPlanJSONIgnoredByGit    docs/analysis/P0_qlib_minute_bin_patch_plan_dry_run_20260502.json
LocalVerifyJSONIgnoredByGit  docs/analysis/P0_qlib_minute_bin_patch_plan_verify_20260502.json
```

## Next Write-Phase Gates

只有在用户明确确认写入 Qlib bin 后，才进入下一阶段。下一阶段必须继续 fail-fast，不能有静默兜底。

```text
Priority  Gate                         Requirement
--------  ---------------------------  ------------------------------------------------------------
P0        BackupBeforeWrite             backup affected files or whole dataset before any mutation
P0        PreWriteChecksum              validate calendar SHA and field-file SHA before patching
P0        ExactValuePreview             compute per-field patch values and abort on NaN/Inf/default fill
P0        FactorDenominator             use inferred official denominator, not current DB max adj_factor
P0        AtomicPatch                   write only verified offsets and fields, no unrelated offsets
P0        PostWriteReadback             reread every patched offset and compare with expected values
P0        PostRepairAudit               rerun gap scan and close-none root-cause audit without QE rerun
```

## Actual Repair Execution

用户已确认继续执行分钟线数据修复。本轮写入阶段只修改 dry-run plan 中验证过的 Qlib 1min bin offset，没有重新运行 QE，也没有修改 Qlib 源码。

```text
Metric                       Value
---------------------------  ---------------------------------------------------------------
ApplyStatus                  completed
RecordsPatched               9655
StocksPatched                2696
FilesPatched                 24264
ValuesWritten                20854800
ReadbackMaxAbsDiff           0.0
BackupRoot                   /home/lc999/data/qlib_minute_bin_backup_direct_repair_20260502_
BackupFiles                  24264
BackupBytes                  12962997064
ApplyReport                  docs/analysis/P0_qlib_minute_bin_repair_apply_20260502.md
```

## Post-Repair Verification

```text
Check                        Result
---------------------------  ------------------------------------------------------------
PostScanPatchableCandidates  0
PostScanMissingFields        none
PostVerifyOK                 True
PostVerifyFailures           0
PostVerifyWarnings           0
PostVerifyFactorSamples      94283
```

修复后，原 9,655 个 DB-present stock-date pair 的 `open/high/low/close/volume/amount/factor/limit_up/limit_down` 均已不再缺失。`verify-plan` 对同一批记录回读通过，factor denominator 仍然稳定；33 只 DB max adj 与官方 inferred denominator 不一致的股票仍作为数据口径漂移证据保留，不影响本次修复，因为写入使用的是 inferred official denominator。

```text
EvidenceType                 Path
---------------------------  --------------------------------------------------------------------------------
ApplyMarkdown                docs/analysis/P0_qlib_minute_bin_repair_apply_20260502.md
PostScanMarkdown             docs/analysis/P0_qlib_minute_bin_post_repair_scan_20260502.md
PostVerifyMarkdown           docs/analysis/P0_qlib_minute_bin_post_repair_verify_20260502.md
PostFactorBasisCSV           docs/analysis/P0_qlib_minute_bin_post_repair_factor_basis_20260502.csv
```
