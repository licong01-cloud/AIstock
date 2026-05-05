# ST PIT Production Replacement Validation - 2026-05-05

## Scope

- User request: replace generated local Bin and H5 files without backup; do not replace remote machine data.
- Replacement source universe: `shsz_st_pit_active_v1`.
- Rule version: `st_pub_next_trade_restore_active_l_v1`.
- Scope intentionally remains ST-only PIT for current active SH/SZ stocks; delisting PIT and paused-listing PIT remain out of scope because related raw/minute/auxiliary datasets are incomplete for delisted stocks.

## Local Targets Replaced

- Daily Qlib Bin: `/home/lc999/data/qlib_bin`.
- 1min Qlib Bin: `/home/lc999/data/qlib_minute_bin`.
- WSL factor/H5 data: `/home/lc999/data/factor_data`.
- AIstock H5 snapshot target: `F:\Dev\AIstock\qlib_snapshots\qlib_test`.
- RD-Agent full factor source data: `F:\Dev\RD-Agent-main\git_ignore_folder\factor_implementation_source_data`.
- Remote machine data: not touched.
- Backup: not created, per user instruction.

## Replacement Integrity

Commands:

```powershell
wsl -d Ubuntu bash -lc "du -sh /home/lc999/data/qlib_bin /home/lc999/data/qlib_minute_bin /home/lc999/data/factor_data /mnt/f/Dev/AIstock/qlib_snapshots/qlib_test /mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data; wc -l /home/lc999/data/qlib_bin/instruments/all.txt /home/lc999/data/qlib_minute_bin/instruments/all.txt /home/lc999/data/factor_data/instruments/all.txt /mnt/f/Dev/AIstock/qlib_snapshots/qlib_test/instruments/all.txt /mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data/instruments/all.txt"
wsl -d Ubuntu bash -lc "rsync -ani --delete /mnt/f/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_daily_candidate_20180801_20260430/ /home/lc999/data/qlib_bin/ | head -20; rsync -ani --delete /mnt/f/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_minute_candidate_20240102_20260430/ /home/lc999/data/qlib_minute_bin/ | head -20; rsync -ani --delete /mnt/f/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260430/ /mnt/f/Dev/AIstock/qlib_snapshots/qlib_test/ | head -20"
```

Results:

- `/home/lc999/data/qlib_bin`: `470M`; `instruments/all.txt` rows: `5372`.
- `/home/lc999/data/qlib_minute_bin`: `31G`; `instruments/all.txt` rows: `5130`.
- `/home/lc999/data/factor_data`: `6.2G`; `instruments/all.txt` rows: `5372`.
- `F:\Dev\AIstock\qlib_snapshots\qlib_test`: `6.2G`; `instruments/all.txt` rows: `5372`.
- `F:\Dev\RD-Agent-main\git_ignore_folder\factor_implementation_source_data`: `6.2G`; `instruments/all.txt` rows: `5372`.
- `rsync -ani --delete` dry-run output was empty for daily Bin, 1min Bin, and `qlib_test`, so these targets match the ST PIT candidate sources by rsync quick-check.

## Qlib/H5 Smoke

Report: `reports/qlib_authoritative_export/st_pit_production_replacement_smoke.json`.

Key results:

- `ok`: `true`.
- Day calendar: `2018-08-01` to `2026-04-30`, `1878` rows.
- 1min calendar: `2024-01-02 09:30:00` to `2026-04-30 15:00:00`, `135289` rows.
- Daily feature sample: shape `[21, 5]`, NaN count `0`.
- Minute feature sample: shape `[723, 2]`, NaN count `6`.
- H5/parquet shapes:
  - `daily_pv.h5`: `[8230158, 7]`
  - `daily_basic.h5`: `[8235236, 16]`
  - `moneyflow.h5`: `[8226801, 18]`
  - `bak_basic.h5`: `[8186850, 15]`
  - `cyq_perf.h5`: `[8235236, 9]`
  - `margin_detail.h5`: `[4516090, 8]`
  - `sector_data.h5`: `[8267307, 22]`
  - `static_factors.parquet`: `[8229661, 120]`

## Data Doctor

Command:

```powershell
$env:TDX_DB_PASSWORD='lc78080808'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe C:/Users/lc999/.codex/skills/rdagent-data-doctor/check_rdagent_data.py --check --json
```

Report: `reports/qlib_authoritative_export/st_pit_production_data_doctor_after_metadata.json`.

Results after metadata schema sync:

- PASS: static factor columns (`122` parquet fields, `22` `sw2_*` columns).
- PASS: `sw2_close` NaN rate `0.09%`.
- PASS: debug parquet size/rows (`12.2MB`, `33,944` rows, `114` columns).
- PASS: H5 freshness through `2026-04-30`.
- PASS: `sw_index_member` coverage.
- PASS: `sector_data` coverage (`sw2_close NULL 0.09%`).
- PASS: WSL data paths exist.
- PASS: metadata schema sync (`schema 120` non-index columns; missing schema columns `0`).
- Known non-blocking FAIL: `qlib_bin` version heuristic still marks WSL as stale because the skill sorts legacy Windows folder names and prefers `qlib_bin_20260430_shsz_current_candidate`; dry-run rsync and meta checks show the WSL target is the intended ST PIT dataset. Do not run `rdagent-data-doctor --fix` until that heuristic is updated.

## Production-Path Manual QE

Command:

```powershell
$env:TDX_DB_PASSWORD='lc78080808'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe debug_tools/st_pit_manual_qe_production_validation.py --run
```

Evidence:

- Run result: `reports/qlib_authoritative_export/manual_qe_st_pit_production_validation/manual_qe_production_result.json`.
- Run log: `reports/qlib_authoritative_export/manual_qe_st_pit_production_validation/manual_qe_production_run.log`.
- PIT membership check: `reports/qlib_authoritative_export/manual_qe_st_pit_production_validation/manual_qe_production_pit_membership_check.json`.
- Workspace: `rdagent_assets/qe_workspace_st_pit_production_validation/qe_manual_st_pit_production_20260505`.

Manual QE results:

- `returncode`: `0`, `ok`: `true`.
- Provider paths in generated run:
  - day: `/home/lc999/data/qlib_bin`
  - 1min: `/home/lc999/data/qlib_minute_bin`
  - H5/factor: `/home/lc999/data/factor_data`
- Factor preparation succeeded using `m_intraday_range_ratio_5d`.
- LGB training and `pred.pkl` generation succeeded.
- 1min nested backtest completed.
- IC: `0.01105362705665435`.
- Rank IC: `-0.013227066195059334`.
- Final NAV: `1.001855` over `7` trading days.
- FFR: `1.0`.
- Buy orders: `36`.

PIT membership oracle:

- Prediction rows: `39417` across `4938` instruments, dates `2026-04-20` to `2026-04-29`.
- Prediction rows outside daily PIT `all.txt`: `0`.
- Prediction rows outside minute PIT `all.txt`: `0`.
- Buy orders outside daily PIT `all.txt`: `0`.
- Buy orders outside minute PIT `all.txt`: `0`.

## Residual Risks

- Remote machine still has old files and was intentionally not updated.
- No local backup was created per user instruction.
- Data Doctor `qlib_bin` FAIL is a stale-folder-selection heuristic, not an observed data mismatch; avoid using `--fix` until corrected.
- `factor_implementation_source_data_debug` remains the existing debug subset; full production H5/Bin replacement and manual QE used the full production paths.
- Benchmark file in the manual QE window still ends before the April 2026 test window, so benchmark-relative performance is not authoritative; this does not block data readability, training, minute execution, or PIT membership validation.

## Conclusion

Local ST-only PIT Bin/H5 production replacement is complete and usable for local QE paths. Daily Bin, 1min Bin, H5/parquet data, production factor data, manual LGB training, `pred.pkl`, 1min backtest, and PIT membership checks passed. Remote data was not replaced.


