# StrategyPackage 资产回填双源恢复 Batch 3 验证记录

- Module: `strategy_package` / `selection_center` / `paper_trading_v2`
- Level: L2
- Date: 2026-06-30
- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-asset-backfill-dualsource-20260630`
- Branch: `feature/strategy-package-asset-backfill-dualsource-20260630`
- Stacked base: `feature/strategy-package-asset-backfill-batch3-20260630` / PR #1771
- Design: `docs/architecture/strategy_package_asset_backfill_dualsource_batch3_f2_design_20260630.md`

## Scope

本批只扩展 Batch 3 回填固化源解析：在 PR #1771 的 dry-run / gated apply / CAS / ledger / audit event 基础上，为 `StrategyPackageAssetSource` 增加中心库 -> QE 节点 API -> WSL/local workspace 的双源/三源恢复。生产逻辑复用 `QEWorkspaceClient`，SSH 仅用于第 0 步只读取证。

不变边界：不启/重启服务；不执行生产 DML；无 DDL；不修改 `qe_archive`；不把 `pred.pkl` / `combined_prediction.pkl` 当作包、运行时或数仓权威资产；不改 PaperPortfolio 单 `package_id` 契约。

## Step 0 三源核查结论

Evidence files retained under local `tmp/` only:

- Step-0 probe: `tmp/strategy_package_dualsource_step0_probe_v2_no_hardcoded_roots.json`
- SSH 215 direct find probe: `tmp/strategy_package_dualsource_ssh215_probe_v2.json`
- Dual-source dry-run: `tmp/strategy_package_asset_backfill_dualsource_dry_run_v4_no_hardcoded_roots.json`
- Self-contained runtime materialization oracle: `tmp/strategy_package_dualsource_self_contained_oracle.json`

Summary:

- 生产 15 包三源核查完成：`central_any=2`、`qe_node_any=11`、`wsl_local_any=11`。
- 优先恢复源：`central_store=2`、`qe_node=9`、`wsl_workspace=2`、`unrecoverable=2`。
- 215 SSH direct `find` 是只读取证：`found_count=1`；生产恢复逻辑不依赖 SSH，而依赖 `QEWorkspaceClient` API。SSH direct find 与 QE API 结果不完全一致时，以 API/dry-run 为可执行回填依据，并在报告中保留两者差异。

| package_id | status | source | qe_task_id | qe_loop_id | experiment_id/run_id | central | qe_node_api | wsl/local | final_source |
|---|---|---|---|---|---|---|---|---|---|
| `pkg_c4703dfc2fdf4e548cf8dd3027ef228b` | `BACKTEST_APPROVED` | `qe_experiment` | `qe_20260614_022643_edaf` | `Loop13` | `qe_20260614_022643_edaf_L13` | yes | yes | yes | `central_store` |
| `pkg_09750b4944ca434db03efd399ccf2144` | `BACKTEST_APPROVED` | `candidate_strategy_package` | `qe_20260607_093306_1f70` | `Loop2` | `qe_20260607_093306_1f70_L2` | yes | yes | yes | `central_store` |
| `pkg_a2f53f3f2f3e4095a910b939464c35e6` | `BACKTEST_APPROVED` | `candidate_strategy_package` | `qe_20260601_172505_fe17` | `Loop2` | `qe_20260601_172505_fe17_L2` | no | yes | yes | `qe_node` |
| `pkg_378eb9c91e104c64935404e257e932ee` | `BACKTEST_APPROVED` | `candidate_strategy_package` | `qe_20260520_215627_abbc` | `Loop16` | `qe_20260520_215627_abbc_L16` | no | yes | yes | `qe_node` |
| `pkg_b4ce634c24bd470fac2c7b581a4e106f` | `BACKTEST_APPROVED` | `qe_evolution_loop` | `qe_20260520_005113_1785` | `Loop2` | `qe_20260520_005113_1785_L2` | no | no | no | `unrecoverable` |
| `pkg_95523262439644e49ae52f9b5087165d` | `BACKTEST_APPROVED` | `candidate_strategy_package` | `qe_20260520_005113_1785` | `Loop2` | `qe_20260520_005113_1785_L2` | no | no | no | `unrecoverable` |
| `pkg_cfa3c5b4068d4db1ad06db352bfece93` | `SELECTION_ENABLED` | `qe_evolution_loop` | `qe_20260512_113610_b19c` | `Loop1` | `qe_20260512_113610_b19c_L1` | no | yes | yes | `qe_node` |
| `pkg_2a9fccb83da840c9a27a2d7a4118af9a` | `SELECTION_ENABLED` | `qe_evolution_loop` | `qe_20260513_151128_12ea` | `Loop1` | `qe_20260513_151128_12ea_L1` | no | yes | yes | `qe_node` |
| `pkg_2563063e544f4d1fa601e740d019f8c7` | `BACKTEST_APPROVED` | `candidate_strategy_package` | `qe_20260513_151128_12ea` | `Loop1` | `qe_20260513_151128_12ea_L1` | no | yes | yes | `qe_node` |
| `pkg_b2faccade8d549af9621c51d285bdc06` | `BACKTEST_APPROVED` | `qe_evolution_loop` | `qe_20260512_113610_b19c` | `Loop2` | `qe_20260512_113610_b19c_L2` | no | yes | yes | `qe_node` |
| `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27` | `PAPER_ENABLED` | `qe_evolution_loop` | `qe_20260508_060509_1268` | `Loop2` | `qe_20260508_060509_1268_L2` | no | yes | no | `qe_node` |
| `pkg_1de32357724a4c5b874f2abd90f22da5` | `BACKTEST_APPROVED` | `qe_evolution_loop` | `qe_20260502_231229_0565` | `Loop1` | `qe_20260502_231229_0565_L1` | no | yes | no | `qe_node` |
| `pkg_99142cb1440c40a7824e83902f4e7da9` | `SELECTION_ENABLED` | `qe_experiment` | `qe_20260416_082012` | `Loop1` | `qe_20260416_082012` | no | no | yes | `wsl_workspace` |
| `pkg_006a42323f7c4e81a468fdaad2cb16a3` | `SELECTION_ENABLED` | `qe_experiment` | `qe_20260413_084216` | `Loop1` | `qe_20260413_084216` | no | yes | yes | `qe_node` |
| `pkg_b668f8a633c44b72a5d557a2cb8970e3` | `SELECTION_ENABLED` | `qe_experiment` | `qe_20260416_002701` | `Loop1` | `qe_20260416_002701` | no | no | yes | `wsl_workspace` |

## Unrecoverable 升级清单

以下 2 个包同源到 `qe_20260520_005113_1785/Loop2`，中心库、节点 API 与本机/WSL workspace 均未找到 `params.pkl`，需人工裁决：重跑/重建实验、重建包，或退役包。脚本不会伪造资产，不会建半包。

| package_id | package_status | source | resolved_qe_coord | central miss | qe_node miss | wsl/local miss | decision |
|---|---|---|---|---|---|---|---|
| `pkg_b4ce634c24bd470fac2c7b581a4e106f` | `BACKTEST_APPROVED` | `qe_evolution_loop` | `qe_20260520_005113_1785/Loop2/qe_20260520_005113_1785_L2` | 3 attempts | 2 attempts / 404 | no candidate path | `requires_manual_decision` |
| `pkg_95523262439644e49ae52f9b5087165d` | `BACKTEST_APPROVED` | `candidate_strategy_package` | `qe_20260520_005113_1785/Loop2/qe_20260520_005113_1785_L2` | 2 attempts | 2 attempts / 404 | no candidate path | `requires_manual_decision` |

## Design Compliance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `tests/aistock_validation/history/strategy_package/20260630_l2_strategy-package-asset-backfill-dualsource.md`；`tmp/strategy_package_dualsource_step0_probe_v2_no_hardcoded_roots.json` | 15 包三源核查表完整；candidate 源解析到底层 QE task/loop/experiment；2 个不可恢复包升级 | verified | - |
| F-002 | `backend/services/strategy_package/package_asset_freeze.py` `model_params_bytes()` / `_model_params_from_qe_sources()` | `test_central_model_hit_does_not_call_qe_node`、`test_central_miss_qe_node_model_params_archive_hit`、`test_node_miss_local_workspace_model_params_hit`；dry-run `planned_freeze=13` | verified | - |
| F-003 | `backend/services/strategy_package/package_asset_freeze.py` `factor_code_bytes()` / `_factor_code_from_qe_sources()` | `test_factor_catalog_miss_qe_node_factor_file_hit`、`test_node_miss_local_workspace_factor_code_hit`、`test_factor_catalog_ambiguous_qe_node_factor_file_wins` | verified | - |
| F-004 | `QEWorkspaceClient.for_node()` / `download_mlruns_params()` / `download_workspace_file_bytes()` | 生产代码无 SSH/paramiko；mock client 断言节点 API 调用；SSH 只用于第 0 步只读证据 | verified | - |
| F-005 | `attempted_sources` error context；`DataUnavailableError` / `StrategyPackageValidationError` context | `test_all_sources_miss_reports_central_node_and_wsl_attempts`；dry-run unrecoverable 逐项含 `central_store`、`qe_node`、`wsl_workspace` miss reason | verified | - |
| F-006 | `scripts/strategy_package_asset_backfill.py` dry-run report；本验证记录 runbook | dry-run scanned 15：`planned_freeze=13`、`unrecoverable=2`、`asset_count=605`；生产 apply 未执行，DML gate 保持 pending | verified | - |
| F-007 | Batch 3 backfill service/repository/CLI contracts；本批只替换 source bytes resolver | StrategyPackage 全模块、selection/paper targeted 回归已通过；multi-alpha recursion / CAS / ledger / audit event 由 PR #1771 测试继续覆盖 | verified | - |

## Commands And Results

```bash
python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_backfill_dualsource_batch3_f2_design_20260630.md --tier F2
# PASS tier=F2 design_items=7 matrix_rows=7 warnings=0

python -m compileall -q backend/services/strategy_package scripts/strategy_package_asset_backfill.py
# PASS: exit 0

python -m pytest backend/tests/strategy_package/test_package_asset_backfill_dualsource_batch3.py -q
# PASS: 19 passed

python -m pytest backend/tests/strategy_package/test_package_asset_backfill_batch3.py backend/tests/strategy_package/test_package_asset_freeze_batch1.py backend/tests/strategy_package/test_package_asset_backfill_dualsource_batch3.py -q
# PASS: 42 passed

python -m pytest backend/tests/strategy_package/test_package_asset_freeze_batch1.py backend/tests/strategy_package/test_package_asset_backfill_dualsource_batch3.py --cov=backend.services.strategy_package.package_asset_freeze --cov-branch --cov-report=term-missing --cov-report=json:tmp/package_asset_freeze_dualsource_coverage.json -q
# PASS: 26 passed
# Coverage package_asset_freeze.py: line/statements 86%, branch 76%

python -m pytest backend/tests/strategy_package -q
# PASS: 317 passed

python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/selection_center/test_live_inference_preflight_wiring.py backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py backend/tests/paper_trading_v2/test_runtime_enable_paper_strict_gate_compat.py -q
# PASS: 56 passed, 1 skipped

python -m ruff check backend/services/strategy_package/package_asset_freeze.py backend/tests/strategy_package/test_package_asset_backfill_dualsource_batch3.py scripts/strategy_package_asset_backfill.py
# PASS: All checks passed

git diff --check
# PASS
```

Note: full `backend/tests/strategy_package` with coverage collection once hit an environment/pydantic coverage import issue (`ValueError: tuple.index(x): x not in tuple`); the same suite without coverage passed and is the accepted regression evidence.

## Production Read-only Dry-run Evidence

```powershell
$env:AISTOCK_PREDICTION_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\prediction_store'
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --limit 500 --output tmp\strategy_package_asset_backfill_dualsource_dry_run_v4_no_hardcoded_roots.json
# expected exit code: 2, because unrecoverable packages remain
# counts: planned_freeze=13, unrecoverable=2, asset_count=605
# source_resolution: resolved_count=13, unrecoverable_count=2, resolution_rate=0.866667
```

Recoverable package count by priority source:

| source | package_count |
|---|---:|
| central_store | 2 |
| qe_node | 9 |
| wsl_workspace | 2 |
| unrecoverable | 2 |

Dry-run planned freeze packages: `13`. Unrecoverable packages: `2`.

## Self-contained Oracle

`tmp/strategy_package_dualsource_self_contained_oracle.json` verifies one planned package materializes runtime workspace from package-owned assets without QE DB access:

```json
{
  "package_id": "pkg_c4703dfc2fdf4e548cf8dd3027ef228b",
  "source_workspace_type": "strategy_package_asset_store",
  "model_params_origin": "package_asset",
  "prepared_model_params_exists": true,
  "prepared_model_params_size": 612675,
  "dynamic_factors_count": 12,
  "alpha158_factors_count": 0,
  "qe_db_conn_forbidden": true
}
```

This is a workspace-materialization oracle, not a live market scoring run. A full production selection signal for all 13 recoverable packages requires user-authorized apply first; until apply runs, the dry-run report is intentionally non-mutating.

## Asset-safety / No-silent Review

- No production code uses SSH/paramiko; SSH 215 evidence is external read-only probe only.
- No `qe_archive` code path changed.
- No `pred.pkl` / `combined_prediction.pkl` package/runtime authority added.
- `params.pkl` tar extraction rejects symlink/hardlink, absolute/path traversal, and drive-qualified names.
- All-source miss returns `unrecoverable` with `reason_code=strategy_package_model_params_missing` and a complete `attempted_sources` list; factor miss/ambiguity also carries explicit context.
- Production DB was read only for dry-run/probe; no DML/DDL was executed.

## Production Backfill Runbook

1. Merge the stacked PR only after Tier2 review accepts this evidence and the two unrecoverable package decisions are acknowledged.
2. User-authorized window: run dry-run again on production runtime checkout and save report.

```powershell
$env:AISTOCK_PREDICTION_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\prediction_store'
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --output <approved_dry_run_report>.json
```

3. If `counts.unrecoverable > 0`, do not apply. Escalate listed packages for manual decision.
4. If operator accepts partial apply for recoverable packages or all blockers are resolved, run production DML only with both apply flag and explicit token:

```powershell
$env:STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY='I_UNDERSTAND_PRODUCTION_DML'
$env:AISTOCK_PREDICTION_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\prediction_store'
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --target-db prod --apply --confirm-production-dml --operator <operator> --limit 500 --output <approved_apply_report>.json
```

5. Re-run dry-run. Expected for applied packages: `skipped_already_frozen`; unresolved packages remain explicit `unrecoverable` until manually resolved.
6. Rollback remains a separate user-authorized DML action using PR #1771 audit event old/new manifest sha evidence；do not auto-delete package asset blobs.

## Gates

- `production_ddl_gate=noop`：无 schema/migration/comment/index/constraint 变更。
- `production_dml_gate=pending_user_authorized_window`：本批只 dry-run，不执行生产写入。
- `production_backend_dependency_gate=noop`：无 Python dependency 变更。
- `production_frontend_dependency_gate=noop`：无 frontend 变更。
- Services：backend/frontend/TDX/QE 均未启动或重启。
