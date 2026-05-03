# Qlib H5 + Daily Bin Candidate Export - 2026-05-04

本次按当前过渡方案生成非生产候选数据集；没有替换 WSL 生产目录，也没有同步到远端节点。候选数据用于后续验证通过后再决定是否替换生产。

## Export Scope

```
Item                    Value
----------------------  ------------------------------------------------------------
Snapshot ID             qlib_20260430_shsz_current_candidate
Daily Bin ID            qlib_bin_20260430_shsz_current_candidate
Date Range              2018-08-01 ~ 2026-04-30
H5 Snapshot             F:\Dev\AIstock\qlib_snapshots\qlib_20260430_shsz_current_candidate
Daily Bin               F:\Dev\AIstock\qlib_bin\qlib_bin_20260430_shsz_current_candidate
Bin CSV Source          F:\Dev\AIstock\qlib_csv\qlib_bin_20260430_shsz_current_candidate
Validation Report       F:\Dev\AIstock\reports\qlib_candidate_export\qlib_20260430_shsz_current_candidate_daily_validate_20260504.json
Production Replaced     No
WSL Copy                No
Remote Copy             No
Minute Bin              Not included in this export; export separately if needed
```

## Export Command

```powershell
$env:TDX_DB_PASSWORD='***'
python scripts/export_qe_qlib_candidate.py `
  --start 2018-08-01 `
  --end 2026-04-30 `
  --snapshot-id qlib_20260430_shsz_current_candidate `
  --bin-id qlib_bin_20260430_shsz_current_candidate `
  --wsl-copy-dir /home/lc999/data/qlib_bin_20260430_shsz_current_candidate `
  --skip-rdagent-copy `
  --skip-wsl-copy `
  --overwrite-candidate
```

## Current Universe Rules

```
Rule                    Status
----------------------  ------------------------------------------------------------
Exchange Scope          SH/SZ only; BJ/BSE excluded
ST Handling             Current transition rule: exclude stocks with ST ann_date <= 2026-04-30
Listing Status          Exclude delisted/paused by stock_basic list_status D/P
IPO 365D Rule           Enforced in instruments/all.txt eligibility spans
H5 Feature Rows         Keep full available post-listing rows for exported instruments
Daily Bin Feature Rows  Keep full available post-listing rows for exported instruments
Stock Pool Mode         Still transition mode; final PIT StockPoolResolver not implemented here
```

## Export Counts

```
Metric                  Value
----------------------  ------------------------------------------------------------
H5 Universe             4664 instruments with feature rows
Official all.txt        4583 instruments after ST/listing/IPO eligibility rules
Daily Rows              7264909
Daily Instruments       4664
Daily Bin CSV Files     4664
Daily Bin CSV Rows      7264909
Daily Bin Feature Dirs  4665 including 000300.SH index
Static Factor Rows      7264601
Static Factor Columns   112
Elapsed Seconds         1482.154
Generated At            2026-05-04 03:04:14
```

## H5 Validation Summary

```
File                    Rows       Cols  Start       End         Instruments
----------------------  ---------  ----  ----------  ----------  -----------
daily_pv.h5             7264909    7     2018-08-01  2026-04-30  4664
daily_basic.h5          7264601    16    2018-08-01  2026-04-30  4664
moneyflow.h5            7261806    18    2018-08-01  2026-04-30  4664
bak_basic.h5            6765276    15    2018-08-01  2026-04-30  4583
cyq_perf.h5             7264601    9     2018-08-01  2026-04-30  4664
sector_data.h5          7295212    22    2018-08-01  2026-04-30  4664
margin_detail.h5        4237319    8     2018-08-01  2026-04-30  3355
static_factors.parquet  7264601    112   2018-08-01  2026-04-30  4664
```

## all.txt Policy Audit

```
Path     Lines  DB Rows  BJ  Non SH/SZ  Not L  ST <= End  Result
-------  -----  -------  --  ----------  -----  ---------  ------
H5       4583   4583     0   0           0      0          PASS
DailyBin 4583   4583     0   0           0      0          PASS
```

Notes:

- `all.txt` contains 4583 eligible instruments; 603056.SH has an eligibility end date of 2026-03-30, so active rows on 2026-04-30 are 4582.
- H5 feature files intentionally contain full feature rows for 4664 exported instruments; `all.txt` is the eligibility filter.

## Daily Bin Value Validation

Validation compared Qlib `D.features` values read from the generated Bin against expected values reconstructed from H5 `daily_pv.h5` plus DB `market.stk_limit` for two dates. The comparison allows float32 storage rounding only; no row mismatch or semantic field mismatch was found.

```
Date        Active all.txt  Expected Rows  Qlib Rows  DB Limit Rows  Missing Limit  Result
----------  --------------  -------------  ---------  -------------  -------------  ------
2026-04-30  4582            4582           4582       4582           0              PASS
2025-07-10  4509            4501           4501       4509           0              PASS
```

For 2025-07-10, the 8 active `all.txt` instruments without `daily_pv` rows were checked against DB `market.suspend_d`; all 8 are suspension rows (`suspend_type='S'`) on that exact date, and Qlib Bin output matches H5 sparsity exactly.

```
Suspended Missing Daily Rows on 2025-07-10
------------------------------------------
000545.SZ
300897.SZ
300950.SZ
301505.SZ
603758.SH
605008.SH
605389.SH
688313.SH
```

Maximum observed absolute differences were due to Qlib Bin float32 storage: adjusted OHLC max `5.8594e-05`, `factor` max `2.9776e-08`, raw limit prices and limit flags exact, `volume` max `28`, `amount` max `944`. These passed with `abs_tol=1e-4` and `rel_tol=1e-6`.

## Code Change Required For This Export

`scripts/export_qe_qlib_candidate.py` was adjusted so the candidate matches the current transition design:

- ST exclusion uses `ann_date <= end`, not `< end`.
- H5 and Daily Bin feature rows keep full available history.
- IPO 365D restriction is written into `instruments/all.txt` instead of deleting feature rows.
- H5 `instruments/all.txt` is rewritten from the same official IPO-filtered universe as Daily Bin.

## Residual Scope

- This is not a production replacement and not a WSL local deployment.
- This does not regenerate the large minute Bin dataset used by V25 minute execution. If minute Bin validation/replacement is needed, run the authoritative `stock_minute` export separately as a new candidate.
- The current universe rule still globally excludes ST stocks by `ann_date <= 2026-04-30`; the final no-future-leakage solution should use PIT stock-pool spans instead of global exclusion.
