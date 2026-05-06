# ST-only PIT Candidate Manual QE Validation - 2026-05-05

## Scope

- Goal: validate the non-production ST-only PIT candidate datasets with a real manual QE-style LGB training and 1min execution backtest before any production replacement.
- Universe: `shsz_st_pit_active_v1`, rule `st_pub_next_trade_restore_active_l_v1`.
- Production replacement: **not performed**. Production WSL datasets `/home/lc999/data/qlib_bin` and `/home/lc999/data/qlib_minute_bin` were not modified.

## Candidate Paths

- H5/daily factor data: `/mnt/f/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260430`
- Daily Qlib Bin: `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_daily_candidate_20180801_20260430`
- 1min Qlib Bin: `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_minute_candidate_20240102_20260430`
- Experiment workspace: `/mnt/f/Dev/AIstock/rdagent_assets/qe_workspace_st_pit_validation/qe_manual_st_pit_candidate_20260505`

## Commands

```powershell
$env:TDX_DB_PASSWORD='lc78080808'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile debug_tools/st_pit_manual_qe_validation.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe debug_tools/st_pit_manual_qe_validation.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe debug_tools/st_pit_manual_qe_validation.py --run
```

Additional post-run PIT membership check was executed against `pred.pkl`, daily `instruments/all.txt`, minute `instruments/all.txt`, and actual buy orders from `qlib_results_enhanced.json`.

## QE Run Result

- Return code: `0`
- Model: `__builtin_LGBModel__`
- Strategy: `topk_dropout_conservative`
- Execution algorithm: `TWAP` via 1min nested executor
- Train: `2026-01-02` to `2026-02-27`
- Valid: `2026-03-02` to `2026-03-31`
- Test/backtest: `2026-04-20` to `2026-04-28`

## Metrics

- IC: `0.007660577055641423`
- ICIR: `0.15865818120490838`
- Rank IC: `-0.009060589552451054`
- Rank ICIR: `-0.13407700893065957`
- Final NAV: `1.007224` over `7` trading days
- Annualized return with cost: `0.25822949210240576`
- Information ratio with cost: `1.4592265774900897`
- Max drawdown with cost: `-0.01794902364583023`
- Fill factor ratio: `0.975609756097561`
- Average turnover: `0.1161`
- Final stock count: `30`

## PIT Membership Oracle

- Prediction rows: `39425` across `8` dates and `4939` instruments.
- Pred rows outside daily PIT `all.txt`: `0`.
- Pred rows outside minute PIT `all.txt`: `0`.
- Buy orders: `35` across `35` instruments.
- Buy orders outside daily PIT `all.txt`: `0`.
- Buy orders outside minute PIT `all.txt`: `0`.

## Evidence

- Run result: `reports/qlib_authoritative_export/manual_qe_st_pit_validation/manual_qe_candidate_result.json`
- Run log: `reports/qlib_authoritative_export/manual_qe_st_pit_validation/manual_qe_candidate_run.log`
- Summary: `reports/qlib_authoritative_export/manual_qe_st_pit_validation/manual_qe_candidate_summary.json`
- PIT membership check: `reports/qlib_authoritative_export/manual_qe_st_pit_validation/manual_qe_candidate_pit_membership_check.json`
- Enhanced QE results: `rdagent_assets/qe_workspace_st_pit_validation/qe_manual_st_pit_candidate_20260505/qlib_results_enhanced.json`
- Prediction artifact: `rdagent_assets/qe_workspace_st_pit_validation/qe_manual_st_pit_candidate_20260505/mlruns/519504845184244372/2f3927e0d9d44c37a80fbbe825ce9e61/artifacts/pred.pkl`
- Portfolio artifact: `rdagent_assets/qe_workspace_st_pit_validation/qe_manual_st_pit_candidate_20260505/mlruns/519504845184244372/2f3927e0d9d44c37a80fbbe825ce9e61/artifacts/portfolio_analysis/port_analysis_1day.pkl`

## Residual Risks

- This is a short-window manual QE validation, not a full multi-month/multi-factor QE replacement run.
- The generated `benchmark_sh000300.parquet` ended at 2026-03-10, so benchmark metrics used fallback/zero benchmark for the 2026-04 test window. This does not invalidate candidate H5/Bin readability or PIT membership, but benchmark-relative performance is not authoritative.
- The run log contains a Qlib `StaticDataLoader` instruments warning; post-run membership checks found zero prediction or buy-order violations against both candidate daily and minute `all.txt` files.

## Decision

Manual QE validation **passed** for candidate data usability: H5 factor preparation, daily Bin Alpha158 loading, LGB training, `pred.pkl` creation, and 1min nested backtest all completed using the non-production ST-only PIT candidate paths. Replacement should still wait for user confirmation and, ideally, one longer benchmark-covered validation window.
