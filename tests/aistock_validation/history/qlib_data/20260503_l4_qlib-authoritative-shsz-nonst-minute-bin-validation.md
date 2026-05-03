# L4 Qlib 权威 SH/SZ non-ST 分钟线 Bin 验证记录（2026-05-03）

## Scope

验证新导出的 WSL 本地权威分钟线 Qlib bin：`/home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428`。

## Business Oracles

```text
Oracle                                       Result
-------------------------------------------  ----------------
BJ/BSE 不导出                                PASS
ST 股票不导出                                PASS
退市/暂停上市股票不导出                      PASS
新股 feature bin 保留，all.txt 执行 365D 过滤 PASS
CSV dump 到 bin 后逐字段一致                 PASS
Qlib 可读取全部 QE/V25 必需字段              PASS
v25 使用 raw price 口径读取涨跌停状态        PASS
无静默 fallback / fake success              PASS
```

## Commands And Results

```text
Command                                                                   Result
------------------------------------------------------------------------  ----------------
python -m py_compile backend/qlib_exporter/... scripts/...                PASS
python -m pytest backend/tests/test_qlib_export_stock_universe_filters.py -q  PASS, 6 passed
npm exec tsc -- --noEmit                                                  PASS
scripts/qlib_stock_universe_policy_audit.py                               PASS, error_count=0
scripts/qlib_authoritative_csv_bin_audit.py                               PASS, 618578712 rows, 7422944544 values
scripts/qlib_authoritative_bin_export.py --stage validate mature sample    PASS, max diff 0
scripts/qlib_authoritative_bin_export.py --stage validate IPO samples      PASS, max diff 0
scripts/qlib_authoritative_bin_export.py --stage validate limit events     PASS, max diff 0
scripts/qlib_authoritative_smoke_backtest.py                              PASS
scripts/qlib_v25_limit_state_smoke.py regular sample                      PASS
scripts/qlib_v25_limit_state_smoke.py limit-event sample                  PASS
unified_export_v2 monkeypatch UI/API payload smoke                        PASS
```

## Evidence Files

```text
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_export.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_dump.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_universe_policy_audit.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_csv_bin_audit.json
reports/qlib_authoritative_export/db_validate_mature/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/db_validate_ipo_301449/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/db_validate_ipo_001312/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/db_validate_limit_events/qlib_minute_authoritative_shsz_nonst_full_20260428_stock_minute_validate.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_backtest.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_v25_limit_state_smoke.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_v25_limit_event_smoke.json
reports/qlib_authoritative_export/qlib_minute_authoritative_shsz_nonst_full_20260428_ui_api_payload_smoke.json
docs/analysis/P0_qlib_authoritative_shsz_nonst_minute_bin_export_validation_20260503.md
```

## Dataset Summary

```text
Metric                                       Value
-------------------------------------------  ----------------
Feature stock dirs                           4692
Feature bin files                            56304
CSV rows                                     618578712
all.txt rows                                 4612
IPO-young feature dirs omitted from all.txt  80
CSV-vs-bin errors                            0
DB-vs-bin sample errors                      0
V25 data errors                              0
V25 limit flag mismatches                    0
Backtest minute NaN fields                   0
```

## Residual Risks

```text
Risk                                         Status
-------------------------------------------  ------------------------------------------------------------
Full DB-vs-bin across all 618M rows          Not executed; full CSV-vs-bin plus representative DB-vs-bin passed
Source DB ingestion chain                    Not re-audited in this run
Production active provider                   Not switched in this run
Remote node copy                             Not synced or validated in this run
Old data deletion                            Not performed after user requested confirmation-first deletion
```
