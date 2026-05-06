# ST PIT Active Full Candidate Overnight Validation

- generated_at: `2026-05-05T08:41:06`
- production_replaced: `False`
- minute_h5_full_exported: `False`
- minute_h5_policy: `skipped; no 5min/10min/minute H5 export in this run`
- universe_key: `shsz_st_pit_active_v1`
- rule_version: `st_pub_next_trade_restore_active_l_v1`

## Status Matrix

| Check | Status | Evidence |
|---|---:|---|
| Daily/aux H5 validation | `PASS` | `F:\Dev\AIstock\reports\qlib_st_pit_active_h5_daily_candidate_validate.json` |
| Daily Bin assembled validation | `PASS` | `F:\Dev\AIstock\reports\qlib_authoritative_export\qlib_bin_st_pit_active_daily_candidate_20180801_20260430_daily_assembled_validation_summary.json` |
| Minute Bin full coverage validation | `PASS` | `F:\Dev\AIstock\reports\qlib_authoritative_export\qlib_bin_st_pit_active_minute_candidate_20240102_20260430_stock_minute_all.json` |
| Minute Bin targeted value validation | `PASS` | `F:\Dev\AIstock\reports\qlib_authoritative_export\qlib_bin_st_pit_active_minute_candidate_20240102_20260430_stock_minute_validate.json` |
| All dataset integrity | `PASS` | `F:\Dev\AIstock\reports\qlib_authoritative_export\st_pit_all_dataset_integrity_summary.json` |
| Daily Alpha158 LGB smoke | `PASS` | `F:\Dev\AIstock\reports\qlib_authoritative_export\st_pit_active_daily_lgb_smoke_result.json` |
| Daily Bin + H5 multi-dataset smoke | `PASS` | `F:\Dev\AIstock\reports\qlib_multi_dataset_smoke_st_pit_active_20180801_20260430\report.json` |
| Day+1min NestedExecutor minute smoke | `PASS` | `F:\Dev\AIstock\reports\qlib_authoritative_export\st_pit_active_minute_chain_smoke\report.json` |

## Minute Bin Evidence

- full coverage stocks_in_universe: `5083`
- full coverage db_stock_dates: `2835050`
- full coverage db_rows: `682238616`
- full coverage checked_stock_dates: `2835050`
- full coverage checked_field_values: `8189648292`
- full coverage error_count: `0`
- targeted value stocks_in_universe: `15`
- targeted value db_rows: `2028660`
- targeted value checked_field_values: `24343920`
- targeted value error_count: `0`

## Dataset Integrity

- daily_bin: all_txt_rows=`5372`, instruments=`5117`, features=`5122`, bj_rows=`0`, overlap_count=`0`
- minute_bin: all_txt_rows=`5130`, instruments=`5083`, features=`5083`, bj_rows=`0`, overlap_count=`0`

## Residual Risks

- Production datasets were not replaced in this run.
- Full minute H5 / 5min H5 / 10min H5 were intentionally not exported.
- Final production replacement still requires user approval after reviewing this evidence.
