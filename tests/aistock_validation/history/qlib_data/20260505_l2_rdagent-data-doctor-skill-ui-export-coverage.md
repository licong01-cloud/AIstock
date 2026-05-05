# RDAgent Data Doctor Skill + UI Export Coverage Check - 2026-05-05

## Scope

- Updated local `rdagent-data-doctor` skill documentation for the current ST-only PIT production dataset and Qlib export UI coverage.
- Revalidated the skill script against current local production data.
- Performed static code/DB review to determine whether future raw-data, H5, Bin, and ST PIT updates can be completed from UI.

## Commands

```powershell
Get-Content -Path docs/codex_project_memory.md -TotalCount 220
Get-Content -Path C:/Users/lc999/.codex/skills/rdagent-data-doctor/SKILL.md -TotalCount 260
Get-Content -Path F:/Dev/AIstock/.codex/skills/verify-aistock-feature/SKILL.md -TotalCount 220

$env:TDX_DB_PASSWORD='lc78080808'
$env:PYTHONIOENCODING='utf-8'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile C:/Users/lc999/.codex/skills/rdagent-data-doctor/check_rdagent_data.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe C:/Users/lc999/.codex/skills/rdagent-data-doctor/check_rdagent_data.py --check --json | Tee-Object -FilePath reports/qlib_authoritative_export/rdagent_data_doctor_st_pit_skill_update_20260505_rerun.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/qlib_exporter/router.py backend/qlib_exporter/exporter.py backend/routers/stock_universe.py backend/services/stock_universe_pit_service.py backend/services/tushare_sync_engine.py backend/routers/ingestion.py
```

DB/UI coverage snapshot:

```powershell
$env:TDX_DB_PASSWORD='lc78080808'
$env:PYTHONIOENCODING='utf-8'
# Queried market.data_stats, market.stock_universe_pit_state, and market.ingestion_schedules.
# Result saved to reports/qlib_authoritative_export/ui_data_export_coverage_db_snapshot_20260505.json
```

## Skill Update

- `C:/Users/lc999/.codex/skills/rdagent-data-doctor/check_rdagent_data.py` now validates the current ST-only PIT authority for both daily and 1min Qlib Bin.
- `--fix` is safe-gated to sync only verified ST PIT candidates and must not fall back to legacy non-PIT Bin datasets.
- `C:/Users/lc999/.codex/skills/rdagent-data-doctor/SKILL.md` now documents:
  - daily Bin `/home/lc999/data/qlib_bin`,
  - 1min Bin `/home/lc999/data/qlib_minute_bin`,
  - WSL factor data `/home/lc999/data/factor_data`,
  - `shsz_st_pit_active_v1` / `st_pub_next_trade_restore_active_l_v1`,
  - `margin_detail.h5`,
  - expected 122 parquet fields / 120 schema fields,
  - Qlib export UI coverage and known non-UI promotion gap.

## Validation Results

- Data Doctor check: PASS for all checks.
- Current production Data Doctor highlights:
  - `static_factors.parquet`: 122 fields, 22 `sw2_*` fields.
  - H5 freshness: all tracked H5 files, including `margin_detail.h5`, end at `2026-04-30`.
  - daily Qlib Bin: `qlib_bin_st_pit_active_daily_candidate_20180801_20260430`, 5,372 WSL `all.txt` rows.
  - 1min Qlib Bin: `qlib_bin_st_pit_active_minute_candidate_20240102_20260430`, 5,130 WSL `all.txt` rows.
  - metadata sync: schema 120 non-index columns, missing 0.
- Backend syntax check: PASS for Qlib export router/exporter, stock-universe PIT router/service, Tushare sync engine, and ingestion router.

## UI Coverage Findings

- Raw data updates: covered by local-data init/incremental forms and ingestion schedules for the required TDX/Tushare datasets, including `stock_basic`, `stock_st`, `stock_st_events`, daily/minute K-line, `daily_basic`, moneyflow, `margin_detail`, `stk_limit`, `suspend_d`, sector, and CYQ datasets.
- ST PIT cache: covered by dashboard status/rebuild UI and by strict ensure in H5/Bin export paths. Tushare sync of `stock_basic`, `stock_st`, or `stock_st_events` marks PIT dirty and attempts non-strict rebuild.
- H5 snapshot export: UI supports full and per-dataset incremental export for daily/aux H5 and `minute_1min.h5`, plus `static_factors.parquet` and field map generation.
- H5 one-click incremental: does not include `minute_1min.h5`; it updates daily/aux datasets only.
- Qlib Bin export: UI supports full/incremental stock daily Bin and stock 1min Bin through `unified_export_v2`; stock Bin incremental can fail fast if qfq basis extension requires a full rebuild.
- Production promotion: not covered by UI. Replacing `/home/lc999/data/qlib_bin`, `/home/lc999/data/qlib_minute_bin`, and `/home/lc999/data/factor_data` remains manual/scripted until a dedicated promote API/UI with Data Doctor validation gates exists.

## Decision

Most future source-data updates and export-candidate generation can be done from UI. It is not yet a complete UI-only production data lifecycle because candidate-to-production promotion is not exposed in UI, and H5 one-click incremental excludes minute H5.
