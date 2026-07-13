# StrategyPackage Runtime Read Package Assets Batch 2 验证记录

- Module: strategy_package / selection_center / simulation_runtime
- Level: L2
- Date: 2026-06-30
- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-runtime-read-assets-batch2-20260630`
- Branch: `feature/strategy-package-runtime-read-assets-batch2-20260630`
- Base: `origin/feature/strategy-package-asset-freeze-batch1-20260630`
- Design: `docs/architecture/strategy_package_runtime_read_assets_batch2_f2_design_20260630.md`

## Scope

- Batch 2 only: 已固化 StrategyPackage 运行时从 package-owned `PackageAssetStore` 读取 `params.pkl` 与因子 `.py`，不回 QE node、不查询 `qe_experiments`。
- 未固化存量包保留 legacy QE source 解析，等待 Batch 3 回填。
- MultiAlpha child package 使用同一 StrategyPackage runtime asset resolver，不另起 alpha 分叉。
- No DDL/DML；未修改 `qe_archive`；未启停 backend/frontend/TDX；未读写 `pred.pkl` / `combined_prediction.pkl` 作为 runtime authority。

## Design Compliance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/live_inference.py`; `backend/services/strategy_package/selection_artifact.py` | `test_frozen_package_runtime_materializes_assets_without_qe_db` 注入 forbidden conn；`prepared.factor_order_path` 记录 `strategy_package_manifest.factor_set` | verified | - |
| F-002 | `backend/services/strategy_package/live_inference.py`; `backend/tests/strategy_package/test_runtime_package_assets_batch2.py` | `test_frozen_package_runtime_rejects_asset_sha_mismatch`; `test_frozen_package_runtime_rejects_missing_asset_blob`; `test_frozen_package_runtime_rejects_explicit_model_override` | verified | - |
| F-003 | `backend/services/strategy_package/live_inference.py` | `test_unfrozen_package_keeps_legacy_qe_source_resolution` 证明未固化包仍走 legacy `load_source` | verified | - |
| F-004 | `backend/services/strategy_package/multi_alpha_live.py` | `test_multi_alpha_frozen_child_runtime_does_not_require_seed_run_id_binding`; `test_multi_alpha_live_selection.py` 13 passed | verified | - |
| F-005 | changed-file grep / git diff | 无 `backend/migrations/` 变更；无新增 `qe_archive` / prediction artifact runtime references；生产门禁均 noop | verified | - |

## Commands And Results

```bash
python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_runtime_read_assets_batch2_f2_design_20260630.md --tier F2
# PASS: tier=F2 design_items=5 matrix_rows=5 warnings=0

python -m compileall -q backend/services/strategy_package backend/services/selection_center backend/services/simulation_runtime
# PASS: exit 0

python -m pytest backend/tests/strategy_package/test_runtime_package_assets_batch2.py -q
# PASS: 7 passed

python -m pytest backend/tests/strategy_package/test_multi_alpha_live_selection.py -q
# PASS: 13 passed

python -m pytest backend/tests/strategy_package/test_live_inference_preflight.py -q
# PASS: 12 passed

python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_artifact_service_resolves_qe_evolution_loop_source backend/tests/selection_center/test_live_inference_preflight_wiring.py -q
# PASS: 5 passed

python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_artifact_service_resolves_qe_evolution_loop_source backend/tests/selection_center/test_live_inference_preflight_wiring.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q
# PASS: 21 passed

python -m pytest backend/tests/strategy_package -q
# PASS: 266 passed

python -m pytest backend/tests/selection_center -q
# PASS: 86 passed

python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q
# PASS: 16 passed

python -m ruff check backend/services/strategy_package/live_inference.py backend/services/strategy_package/selection_artifact.py backend/services/strategy_package/multi_alpha_live.py backend/services/selection_center/service.py backend/services/simulation_runtime/selection.py backend/tests/strategy_package/test_runtime_package_assets_batch2.py backend/tests/strategy_package/test_multi_alpha_live_selection.py
# PASS: All checks passed!

git diff --check
# PASS: exit 0

git diff --unified=0 -- backend/services backend/tests | rg "^\\+.*(qe_archive|pred\\.pkl|combined_prediction\\.pkl)"
# PASS: no added qe_archive/prediction-artifact runtime references

git diff --name-only -- backend/migrations; git ls-files --others --exclude-standard backend/migrations
# PASS: no backend/migrations changes
```

## Baseline Notes

- `python -m pytest backend/tests/simulation_runtime -q` 当前 4 个失败：`test_tail_policy.py` 2 个、`test_target_rebalance_shared.py` 2 个。
- 同 4 个用例在 detached baseline `origin/feature/strategy-package-asset-freeze-batch1-20260630` 上逐项复跑同样失败，判定为既有 baseline drift，非 Batch 2 新增回归；Batch 2 相关 simulation runtime 入口已用 `test_strategy_package_selection_service.py` + `test_selection_artifact_hmm_preflight.py` 覆盖并通过。

## Business Outcome

- 已固化包的 runtime source 标记 `source_workspace_type=strategy_package_asset_store`、`model_params_origin=package_asset`，并将因子代码与模型权重物化到 runtime cache 后再交给现有 inference workspace。
- 每个读取到的 package asset blob 都按 manifest `sha256` 重算校验；缺失、sha mismatch、已固化包显式 `model_params_path` 覆盖均 fail-loud。
- Selection Center / simulation runtime preflight 传入当前 manifest + package_id；已固化包的 QE node check PASS 信息明确为 package-owned assets 不需要 QE node。
- MultiAlpha child package 复用同一路径；已固化 child 不再要求 `child_record.run_id == seed_run_id`。

## Production Gates

- production_ddl_gate: noop.
- production_dml_gate: noop.
- production_backend_dependency_gate: noop.
- production_frontend_dependency_gate: noop.
- Services: backend/frontend/TDX not started or restarted.
- Production DB: no writes, no DDL.

## Remaining Work

- Batch 3 仍需回填固化 15 个存量包，并执行删源/源不可达场景的 self-contained 核验。
- Batch 5/6 未开始；candidate retirement 与删 `prediction_ref` 不在本批范围。
