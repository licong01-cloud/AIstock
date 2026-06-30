# StrategyPackage ???????????? - 2026-07-01

## ????

- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-prod-asset-backfill-20260701`
- Branch: `ops/strategy-package-prod-asset-backfill-20260701`
- Main HEAD: `cfd0d277ab02ee69334c855d22ec2825a6505d1a`
- ????: ????????? DML????? 1 ?????? + ? 13 ??????????????
- ?? DDL: ????
- ????: ??????????????
- ?????: `F:\Dev\AIstock\rdagent_assets\package_assets`
- ?? DB env: `F:\Dev\AIstock\.env` ? `TDX_DB_*`?

## A. ?? pkg_b4ce634c

- Package: `pkg_b4ce634c24bd470fac2c7b581a4e106f`
- Source: `qe_20260520_005113_1785` / `Loop2`
- Reason: `source_experiment_permanently_lost_cannot_freeze`
- ????: `BACKTEST_APPROVED` -> `RETIRED`
- ????: `event_id=393`, reason=`source_experiment_permanently_lost_cannot_freeze`
- ?????: `authorized_by_user=true`, `operator=codex_strategy_package_asset_backfill_20260701`, `source_loss_evidence=[db_missing, qe_node_215_missing, backup_missing]`
- ??: `tmp/strategy_package_asset_backfill_20260701/retire_apply_verify.json`

## B. ?? dry-run ? apply

- ?? dry-run ??: `counts={"planned_freeze": 13, "unrecoverable": 2}`??????? 2???? full scan ?????????/???????????????????
- ?? dry-run 13 ?????: `planned_freeze=13`, `asset_count=605`, `unrecoverable_count=0`?
- ?? apply 13 ?????: `applied=13`, `asset_count=605`, `unrecoverable_count=0`?
- ?? apply ??: ??????????????????? full-scan fail-loud ???
- Apply gate: `--apply --confirm-production-dml` + `STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY=I_UNDERSTAND_PRODUCTION_DML`?
- ????:

```powershell
$env:AISTOCK_PACKAGE_ASSET_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\package_assets'
$env:STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY='I_UNDERSTAND_PRODUCTION_DML'
python scripts/strategy_package_asset_backfill.py --env-file 'F:\Dev\AIstock\.env' --target-db prod --package-id pkg_006a42323f7c4e81a468fdaad2cb16a3 --package-id pkg_09750b4944ca434db03efd399ccf2144 --package-id pkg_1de32357724a4c5b874f2abd90f22da5 --package-id pkg_2563063e544f4d1fa601e740d019f8c7 --package-id pkg_2a9fccb83da840c9a27a2d7a4118af9a --package-id pkg_378eb9c91e104c64935404e257e932ee --package-id pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27 --package-id pkg_99142cb1440c40a7824e83902f4e7da9 --package-id pkg_a2f53f3f2f3e4095a910b939464c35e6 --package-id pkg_b2faccade8d549af9621c51d285bdc06 --package-id pkg_b668f8a633c44b72a5d557a2cb8970e3 --package-id pkg_c4703dfc2fdf4e548cf8dd3027ef228b --package-id pkg_cfa3c5b4068d4db1ad06db352bfece93 --apply --confirm-production-dml --operator codex_strategy_package_asset_backfill_20260701 --output tmp/strategy_package_asset_backfill_20260701/backfill_apply_13.json
```

- ??:
  - `tmp/strategy_package_asset_backfill_20260701/backfill_dry_run_after_retire.json`
  - `tmp/strategy_package_asset_backfill_20260701/backfill_targeted_dry_run_13.json`
  - `tmp/strategy_package_asset_backfill_20260701/backfill_apply_13.json`

## C. ??? Package IDs

- `pkg_006a42323f7c4e81a468fdaad2cb16a3`
- `pkg_09750b4944ca434db03efd399ccf2144`
- `pkg_1de32357724a4c5b874f2abd90f22da5`
- `pkg_2563063e544f4d1fa601e740d019f8c7`
- `pkg_2a9fccb83da840c9a27a2d7a4118af9a`
- `pkg_378eb9c91e104c64935404e257e932ee`
- `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27`
- `pkg_99142cb1440c40a7824e83902f4e7da9`
- `pkg_a2f53f3f2f3e4095a910b939464c35e6`
- `pkg_b2faccade8d549af9621c51d285bdc06`
- `pkg_b668f8a633c44b72a5d557a2cb8970e3`
- `pkg_c4703dfc2fdf4e548cf8dd3027ef228b`
- `pkg_cfa3c5b4068d4db1ad06db352bfece93`

## D. ????

- `package_asset` ?? blob SHA: 13 ??? expected runtime assets ?????blob SHA ?????
- Manifest ????: 13 ?? `manifest_has_frozen_runtime_assets=true`?
- Manifest ???: `total_scanned=15`, `clean_count=15`, `drifted_count=0`?
- ??????: 13 ???? `strategy_package_asset_backfill_freeze` event?
- ???: `pkg_b4ce634c24bd470fac2c7b581a4e106f` ?? `RETIRED`?
- Oracle ?????: `ok=True`, `oracle_artifact_rows_in_prod=0`??? oracle ???? `selection_score_artifact`?
- ??:
  - `tmp/strategy_package_asset_backfill_20260701/independent_asset_manifest_verify.json`
  - `tmp/strategy_package_asset_backfill_20260701/final_readonly_verify_after_oracle.json`

## E. package_asset ??

- `pkg_006a42323f7c4e81a468fdaad2cb16a3`: `factor_code`=32
- `pkg_006a42323f7c4e81a468fdaad2cb16a3`: `model_weight`=1
- `pkg_09750b4944ca434db03efd399ccf2144`: `factor_code`=26
- `pkg_09750b4944ca434db03efd399ccf2144`: `model_weight`=1
- `pkg_1de32357724a4c5b874f2abd90f22da5`: `factor_code`=57
- `pkg_1de32357724a4c5b874f2abd90f22da5`: `model_weight`=1
- `pkg_2563063e544f4d1fa601e740d019f8c7`: `factor_code`=57
- `pkg_2563063e544f4d1fa601e740d019f8c7`: `model_weight`=1
- `pkg_2a9fccb83da840c9a27a2d7a4118af9a`: `factor_code`=57
- `pkg_2a9fccb83da840c9a27a2d7a4118af9a`: `model_weight`=1
- `pkg_378eb9c91e104c64935404e257e932ee`: `factor_code`=57
- `pkg_378eb9c91e104c64935404e257e932ee`: `model_weight`=1
- `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27`: `factor_code`=57
- `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27`: `model_weight`=1
- `pkg_99142cb1440c40a7824e83902f4e7da9`: `factor_code`=50
- `pkg_99142cb1440c40a7824e83902f4e7da9`: `model_weight`=1
- `pkg_a2f53f3f2f3e4095a910b939464c35e6`: `factor_code`=23
- `pkg_a2f53f3f2f3e4095a910b939464c35e6`: `model_weight`=1
- `pkg_b2faccade8d549af9621c51d285bdc06`: `factor_code`=57
- `pkg_b2faccade8d549af9621c51d285bdc06`: `model_weight`=1
- `pkg_b668f8a633c44b72a5d557a2cb8970e3`: `factor_code`=50
- `pkg_b668f8a633c44b72a5d557a2cb8970e3`: `model_weight`=1
- `pkg_c4703dfc2fdf4e548cf8dd3027ef228b`: `factor_code`=12
- `pkg_c4703dfc2fdf4e548cf8dd3027ef228b`: `model_weight`=1
- `pkg_cfa3c5b4068d4db1ad06db352bfece93`: `factor_code`=57
- `pkg_cfa3c5b4068d4db1ad06db352bfece93`: `model_weight`=1

## F. ???????? Oracle

??: ?? `generate_from_live_inference` ? `2026-06-30` ????????package repository ? `source_id/loop_id/run_id` ??? `simulated_deleted_*`?runtime resolver ? QE source DB `conn_factory` ?????? `AssertionError`????????? `source_workspace_type=strategy_package_asset_store` ? `model_params_origin=package_asset`?artifact repository ? no-write??????? artifact?

???:

- `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27` (PAPER_ENABLED): score_count=1032, top=002437.SZ, source_workspace_type=`strategy_package_asset_store`, model_params_origin=`package_asset`, dynamic_factors=57, strict_kept=1032, artifact_repo=`no_write`.
- `pkg_b668f8a633c44b72a5d557a2cb8970e3` (SELECTION_ENABLED): score_count=1359, top=002701.SZ, source_workspace_type=`strategy_package_asset_store`, model_params_origin=`package_asset`, dynamic_factors=50, strict_kept=1359, artifact_repo=`no_write`.

?????????????:

- `self_contained_oracle_wsl_stub_2pkg.json`: `DataUnavailableError` / Can't get attribute 'LSTM_10D_hs64_d02'; workspace=`rdagent_assets\strategy_package_runtime\oracle_20260701_wsl\pkg_c4703dfc2fdf4e548cf8dd3027ef228b\39d921334a1544c6`.
- `self_contained_oracle_wsl_stub_lgb_selection_enabled.json`: `DataUnavailableError` / strict inference model feature count mismatch; refusing to pad or truncate features: expected=70, actual=50; workspace=`rdagent_assets\strategy_package_runtime\oracle_20260701_wsl\pkg_99142cb1440c40a7824e83902f4e7da9\cc20f64d31d4e259`.
- `self_contained_oracle_wsl_stub_pkg006a.json`: `DataUnavailableError` / strict inference model feature count mismatch; refusing to pad or truncate features: expected=63, actual=32; workspace=`rdagent_assets\strategy_package_runtime\oracle_20260701_wsl\pkg_006a42323f7c4e81a468fdaad2cb16a3\ee8d8fa694c5225d`.

??:

- WSL `rdagent-gpu` ??? `aiofiles`?oracle ??????? `PYTHONPATH` ?? import-only stub???????????????? QE workspace API???????????????
- ?? LSTM ? feature-count ????? live inference ?? fail-loud????????/???????????????????????????? runtime workspace ?????????????? QE ??
- ??:
  - `tmp/strategy_package_asset_backfill_20260701/self_contained_oracle_summary_20260701.json`
  - `tmp/strategy_package_asset_backfill_20260701/self_contained_oracle_wsl_stub_2pkg.json`
  - `tmp/strategy_package_asset_backfill_20260701/self_contained_oracle_wsl_stub_pkgb668.json`

## G. ????

- `production_dml_gate=applied_and_verified`????? `pkg_b4ce634c24bd470fac2c7b581a4e106f` + ???? 13 ??
- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- ???/?? backend?frontend?TDX ? QE ?????
- SSH/QE ??????????????????????

## H. ??????

- `pkg_95523262439644e49ae52f9b5087165d` ?? full dry-run ?????????????????????????? apply?
- LSTM/model feature-count ???????????????????????????? runtime/model ???
