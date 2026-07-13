# 多Alpha父包自包含运行时 L2/F2 验证记录

- Module: `strategy_package` / `multi_alpha`
- Level: L2 + F2 design workflow + real WSL oracle
- Date: 2026-07-02
- Worktree: `F:\Dev\AIstock_worktrees\multi-alpha-parent-self-contained-runtime-20260702`
- Branch: `feature/multi-alpha-parent-self-contained-runtime-20260702`
- Design: `docs/analysis/multi_alpha_parent_self_contained_runtime_design_20260702.md`
- Rebase base: `origin/main=225e9c90 chore(issue): close-sync BUG-577 after merge (#1829)`; includes BUG-573 `2a11ad15` / close-sync `024203b2`, LocalSim #1814 `c9e8ee75`, BUG-575/571/574/577 mainline commits.

## Scope

本轮验证覆盖四阶段实现：

1. Runtime 按腿从父包 `model_asset`、`factor_set`、`runtime_assets` 切片，不读 `child_package`。
2. `FrozenRuntimeSelfCheckService` 与 `MultiAlphaPaperDryRunValidator` 改为 parent package asset evidence。
3. Promotion 改为 parent-only：拒绝 `component_package_ids`，不创建 single-alpha component 子包，不写 child edge。
4. Legacy `child_package:` lineage ref 仅作为 metadata ignored；已建子包退役仅列后续步骤，本 PR 不执行 DML/DDL。

## Rebase / BUG-573 Evidence

```powershell
rtk git fetch origin --prune
rtk git merge-base --is-ancestor origin/main HEAD
# PASS: origin/main is ancestor of HEAD
rtk git log --oneline -5 origin/main
# 225e9c90 chore(issue): close-sync BUG-577 after merge (#1829)
# e6462e85 BUG-577 issue workflow fix (#1826)
# f7e68b5b chore(issue): close-sync BUG-574 after merge (#1828)
# 5403f000 chore(issue): finalize BUG-571 closed metadata (#1827)
# 28479806 Fix BUG-574 simulation lifecycle scheduler binding isolation (#1818)
rtk git log --oneline --ancestry-path 2a11ad15..HEAD
# Confirms BUG-573 `2a11ad15` is in ancestry before this feature branch.
```

`backend/services/strategy_package/package_asset_freeze.py` 当前包含 BUG-573 完整逻辑：`pickled_model_code_references_from_params_bytes()`、`_freeze_model_code_assets()`、`manifest_has_frozen_runtime_assets()` 对 `model_code_required/model_code_assets` 的检查。收尾补充 `_model_code_assets_from_existing_closure()`，用于 parent 二次 freeze 时验证并继承已冻结的 per-leg model_code closure，避免父包因没有 QE source coordinates 而重新发现失败。

## Parity Oracle: Scratch Complete Parent View

旧生产父包 `pkg_ma_0c796d57d216ebbd1daf0412` 仍然是 legacy 状态：`manifest.runtime_assets=null` 且父包 ledger 缺 `factor_schema`。本 PR 没有 backfill、没有 DML、没有改写该 manifest。Tier2 授权的 parity 方式为 scratch/in-memory complete parent view：只在 debug oracle 内从 legacy child manifest 读取 Alpha158 runtime schema mapping，补入临时父包视图，再跑 production parent-self-contained path 与 test-only legacy child oracle 的逐值对比。

```powershell
$env:AISTOCK_PACKAGE_ASSET_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\package_assets'
rtk python debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py --package-id pkg_ma_0c796d57d216ebbd1daf0412 --trade-date 2024-07-02 --backend wsl --read-only --output debug_tools/strategy_package/multi_alpha_parent_self_contained/parent_vs_legacy_child_pkg_ma_0c79_20240702.json
# PASS
```

Evidence summary:

```json
{
  "ok": true,
  "package_id": "pkg_ma_0c796d57d216ebbd1daf0412",
  "trade_date": "2024-07-02",
  "row_count": 25,
  "topk": 25,
  "max_abs_combined_score_diff": 0.0,
  "max_abs_leg_normalized_diff": {
    "a1_plus3_LSTM_h20": 0.0,
    "new_FUNDGROWTH_h20": 0.0
  },
  "weights": {
    "a1_plus3_LSTM_h20": 0.6966591521,
    "new_FUNDGROWTH_h20": 0.3033408479
  },
  "parent_runtime_source": "parent_package_asset",
  "parent_model_params_origin": "package_asset",
  "scratch_parent_runtime_view": {
    "scratch_only": true,
    "writes_db": false,
    "mutates_manifest": false,
    "reason": "legacy_parent_missing_runtime_assets_factor_schema"
  }
}
```

结论：在“完整父包视图”下，parent-self-contained combined selection 与 legacy child-based oracle 对 `instrument/combined score/rank/topK/每腿 normalized/weights` 逐值一致，容差 `<=1e-12`；实际 diff 为 0.0。该验证不证明 legacy 生产父包已经被持久化补齐，只证明新 promotion 产出的完整父包能保持行为 parity。

## Forward Completeness Smoke

使用真实 combine run 作为只读输入，在 scratch package asset store 与 in-memory StrategyPackage repository 中执行 parent-only promotion；不写生产 DB、不写 dev DB、不启动服务。

```powershell
$env:AISTOCK_PACKAGE_ASSET_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\package_assets'
rtk python debug_tools/strategy_package/multi_alpha_parent_self_contained/forward_parent_only_smoke.py --run-id macb_7738e811293948eb_20240702_20260310_20260625T184334308696Z --weighting-scheme ic_weighted --trade-date 2024-07-02 --backend wsl --read-only-production-db --output debug_tools/strategy_package/multi_alpha_parent_self_contained/forward_parent_only_smoke_macb_7738_20240702.json
# PASS
```

Evidence summary:

```json
{
  "ok": true,
  "scope": {
    "scratch_only": true,
    "production_db": "read_only_session",
    "writes_production_db": false,
    "writes_dev_db": false,
    "mutates_existing_package": false
  },
  "promotion": {
    "package_id": "pkg_ma_54fa75c9edf2d4749fa51756",
    "alpha_mode": "multi_alpha",
    "record_count": 1,
    "multi_alpha_parent_count": 1,
    "single_alpha_child_count": 0,
    "component_edge_count": 0,
    "result_component_count": 0,
    "package_status": "ASSET_VALIDATED"
  },
  "manifest": {
    "runtime_assets_alpha158_enabled": true,
    "runtime_assets_alpha158_sha256": "3bde7a5534d0934dadd764277d462a21c676b51f7b175d3fbaf1d396396bcb26",
    "factor_count": 33,
    "model_count": 2,
    "source_evidence_has_child_refs": false
  },
  "asset_ledger": {
    "total_count": 37,
    "counts_by_type": {
      "factor_code": 33,
      "factor_schema": 1,
      "model_code": 1,
      "model_weight": 2
    }
  },
  "runtime_signal": {
    "pass": true,
    "score_count": 25,
    "universe_count": 1279,
    "runtime_source": "parent_package_asset",
    "model_params_origin": "package_asset"
  }
}
```

Self-check evidence: two per-leg package-asset self-checks PASS; parent multi-alpha self-check PASS with `combined_signal_smoke.schema_version=multi_alpha_parent_combined_signal_smoke_v1`, `leg_count=2`, `deterministic_replay=true`; feature counts match (`46/46` for LSTM leg, `7/7` for FUND leg). BUG-573 path exercised: LSTM model has `model_code_required=true` and `model_code_assets=[model.py]`; parent ledger contains `model_code_count=1`。

## Commands And Results

```powershell
rtk python -m py_compile backend/services/strategy_package/multi_alpha_live.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/frozen_runtime_self_check.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/package_asset_freeze.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_freeze_completeness_build_gate.py debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py debug_tools/strategy_package/multi_alpha_parent_self_contained/forward_parent_only_smoke.py
# PASS

rtk python -m pytest -q backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py -p no:cacheprovider
# PASS: 52 passed

rtk python -m pytest -q backend/tests/strategy_package/test_freeze_completeness_build_gate.py backend/tests/strategy_package/test_package_asset_freeze_batch1.py -p no:cacheprovider
# PASS: 17 passed; includes refreeze existing model_code closure regression

rtk python -m pytest -q backend/tests/strategy_package -p no:cacheprovider
# PASS: 349 passed

rtk python -m ruff check backend/services/strategy_package/frozen_runtime_self_check.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/multi_alpha_live.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/package_asset_freeze.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_freeze_completeness_build_gate.py debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py debug_tools/strategy_package/multi_alpha_parent_self_contained/forward_parent_only_smoke.py
# PASS

rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_parent_self_contained_runtime_design_20260702.md --tier F2
# PASS after matrix updated with scratch-complete parity and explicit approved_by_user scope notes

rtk python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
# PASS: changed_only files=6, findings=0, blocking=0

rtk git diff --check
# PASS
```

## Design Compliance Summary

| item | status | evidence |
|---|---|---|
| F-001/F-002 runtime parent leg slice | PASS | child repository sentinel, per-leg cache namespace, metadata `runtime_source=parent_package_asset` |
| F-003/F-011 parity | PASS | WSL scratch-complete parent view oracle: row_count=25, score/rank/topK/per-leg normalized/weights diff 0.0 |
| F-004 dry-run | PASS | dry-run evidence writes `parent_asset_runtime` |
| F-005/F-006 promotion parent-only | PASS | forward smoke and tests: 1 multi-alpha parent, 0 single-alpha child, 0 child edge, explicit child ids rejected |
| F-007/F-008 freeze/self-check | PASS | per-leg model/factor/schema strict self-check, BUG-573 model_code closure, combined signal smoke |
| F-009 legacy compatibility | PASS_WITH_APPROVED_SCOPE | legacy `child_package:` ref ignored; parity uses scratch in-memory schema completion; persisted legacy parent remains unmodified |
| F-010 cleanup readiness | PASS | only lists retire follow-up steps; no DML |
| F-012 single-alpha regression | PASS | `backend/tests/strategy_package` module regression |

## Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_runtime_gate=pending_user_activation`
- 未启动/重启 backend、frontend、TDX 或其他服务。
- 未执行生产 DML/DDL；生产 DB 访问仅只读查询/只读 session。
- 未 backfill 或改写生产 `pkg_ma_0c796d57d216ebbd1daf0412`、`pkg_mac_6e48c4963846f7bf4f16a5f9`、`pkg_mac_a889a92ef523d91a1c103dc1`。
