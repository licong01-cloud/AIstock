# Remote RD-Agent Data/API Sync Validation - 2026-04-30

## Scope

- Synced WSL/backtest datasets to remote node `rdagent-node1` (`192.168.50.215`).
- Updated remote RD-Agent API/source/templates from the WSL RD-Agent tree.
- Verified remote QE/RD-Agent active template windows use latest signal/backtest dates: signal/data `2026-04-28`, portfolio backtest `2026-04-27`.
- Restarted remote RD-Agent Results API on port `9000` after code sync.

## Data Synced

- `/home/lc999/data/qlib_bin/` -> remote `/home/lc999/data/qlib_bin/`
- `/home/lc999/data/qlib_minute_bin/` -> remote `/home/lc999/data/qlib_minute_bin/`
- `/mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data/` -> remote project and `/home/lc999/data/factor_data/`
- `/mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data_debug/` -> remote project and `/home/lc999/data/factor_data_debug/`
- `/mnt/f/Dev/AIstock/qlib_snapshots/qlib_test/` -> remote `/home/lc999/data/qlib_snapshots/qlib_test/`

Rsync backup roots on remote:

- `/home/lc999/data/_sync_backups/20260430_142304/`
- `/home/lc999/data/_sync_backups/20260430_143337_continue/`
- `/home/lc999/projects/_sync_backups/20260430_143337_continue/`

## Commands

```bash
python "C:/Users/lc999/.codex/skills/rdagent-data-doctor/check_rdagent_data.py" --check --json
wsl -d Ubuntu -- bash /mnt/f/Dev/AIstock/.codex_tmp/sync_remote_dataset_api.sh
wsl -d Ubuntu -- bash /mnt/f/Dev/AIstock/.codex_tmp/sync_remote_dataset_api_continue.sh
# remote restart, via ssh stdin script:
cd /home/lc999/projects/RD-Agent-main
export PATH="/home/lc999/miniconda3/envs/rdagent-gpu/bin:$PATH"
nohup /home/lc999/miniconda3/envs/rdagent-gpu/bin/python -m rdagent.app.cli results_api --host 0.0.0.0 --port 9000 > log/results_api_restart_20260430_143957.log 2>&1 &
```

Local sync logs:

- `.codex_tmp/remote_dataset_api_sync_20260430_142304.log`
- `.codex_tmp/remote_dataset_api_sync_20260430_143337_continue.log`

Remote API restart log:

- `/home/lc999/projects/RD-Agent-main/log/results_api_restart_20260430_143957.log`

## Validation Results

### Local/WSL Data Doctor

`rdagent-data-doctor` returned PASS for all 9 checks:

- `static_factors.parquet`: 114 columns, 22 `sw2_*` columns.
- Debug parquet: 33,944 rows, 114 columns, about 12.2 MB.
- Qlib bin version: `qlib_bin_20260428_shsz_candidate`, end `2026-04-28`.
- WSL paths and metadata/schema/README/debug synchronization: PASS.

### Remote Dataset Freshness

Remote tails after sync:

- Day calendar tail: `2026-04-22`, `2026-04-23`, `2026-04-24`, `2026-04-27`, `2026-04-28`.
- Minute calendar tail: `2026-04-28 14:56:00` through `2026-04-28 15:00:00`.
- Remote factor data now includes `margin_detail.h5`, 6 core H5 files, `static_factors.parquet`, README, CSV schema and JSON schema in both full and debug directories.

Remote Qlib smoke in `rdagent-gpu`:

- `D.features(..., freq="day", 2026-04-28)` returned shape `[1, 2]`, non-null close count `1`.
- `D.features(..., freq="1min", 2026-04-28 14:56:00~15:00:00)` returned shape `[5, 2]`, non-null close count `5`.
- Remote `static_factors.parquet`: 7,313,383 rows and 114 columns.

### Remote API Consistency

- Remote `/health`: `{"status":"ok"}` after restart.
- WSL and remote `/openapi.json` both have 76 paths and identical SHA256 `c44a14bedb7a36dd057902a2f70a73de62b4818b5dd880da8f789881277d312f`.
- `rsync -anic` for `rdagent/app/` with pycache/backup exclusions returned no differences.
- Remote `/api/v1/qe_workspace/config` uses remote paths:
  - workspace: `/home/lc999/projects/RD-Agent-main/qe_workspace`
  - factor data: `/home/lc999/data/factor_data`
  - qlib data: `/home/lc999/data/qlib_bin`

### Backtest Window

Active remote RD-Agent/QE templates checked after sync:

- `rdagent/scenarios/qlib/experiment/factor_template/conf_baseline.yaml`: data/test end `2026-04-28`, portfolio end `2026-04-27`.
- `app_tpl/all/v4/.../conf_baseline.yaml`: data/test end `2026-04-28`, portfolio end `2026-04-27`.
- `rdagent/scenarios/qlib/experiment/factor_template/conf_v25_example.yaml`: end `2026-04-28`.

A grep excluding historical backup files found no active `2026-03-10`, `2026-03-09`, or `2025-12-31` references under remote active qlib templates checked.

### Targeted AIstock QE Config Test

```bash
conda run -n AIstock pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
# 19 passed in 33.73s
```

## Residual Risks / Notes

- Historical backup template files with old dates were intentionally not treated as active runtime configuration.
- Remote `/system/metrics` reports `running_tasks: ["186"]`; PID `186` is a kernel worker, not an RD-Agent process, so the node appears `busy` in AIstock although API health and probe are OK.
- Production AIstock backend `8001` was not restarted. It was only used for a dispatch node probe.
