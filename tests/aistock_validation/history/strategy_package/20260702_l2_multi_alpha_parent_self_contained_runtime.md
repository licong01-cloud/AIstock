# 多Alpha父包自包含运行时 L2/F2 验证记录

- Module: `strategy_package` / `multi_alpha`
- Level: L2 + F2 design workflow + real DB read-only oracle
- Date: 2026-07-02
- Worktree: `F:\Dev\AIstock_worktrees\multi-alpha-parent-self-contained-runtime-20260702`
- Branch: `feature/multi-alpha-parent-self-contained-runtime-20260702`
- Design: `docs/analysis/multi_alpha_parent_self_contained_runtime_design_20260702.md`

## Scope

本轮验证覆盖四阶段实现：

1. Runtime 按腿从父包 `model_asset`、`factor_set`、`runtime_assets` 切片，不读 `child_package`。
2. `FrozenRuntimeSelfCheckService` 与 `MultiAlphaPaperDryRunValidator` 改为 parent package asset evidence。
3. Promotion 改为 parent-only：拒绝 `component_package_ids`，不创建 single-alpha component 子包，不写 child edge。
4. Legacy `child_package:` lineage ref 仅作为 metadata ignored；已建子包退役仅列后续步骤，本 PR 不执行 DML/DDL。

## Commands And Results

```powershell
rtk python -m py_compile backend/services/strategy_package/multi_alpha_live.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/frozen_runtime_self_check.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/package_asset_freeze.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py
# PASS

rtk python -m pytest -q backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py -p no:cacheprovider
# PASS: 49 passed in 4.60s

rtk python -m pytest -q backend/tests/strategy_package/test_freeze_completeness_build_gate.py backend/tests/strategy_package/test_package_asset_freeze_batch1.py -p no:cacheprovider
# PASS: 14 passed in 1.47s

rtk python -m pytest -q backend/tests/strategy_package -p no:cacheprovider
# PASS: 338 passed in 13.54s

rtk python -m ruff check backend/services/strategy_package/frozen_runtime_self_check.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/multi_alpha_live.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/package_asset_freeze.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_multi_alpha_promotion.py debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py
# PASS: All checks passed!

rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_parent_self_contained_runtime_design_20260702.md --tier F2
# FAIL-CLOSED: F-003/F-009/F-011 intentionally blocked by unapproved legacy DB schema gap

rtk python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
# PASS: findings=0, blocking=0

rtk git diff --check
# PASS
```

## Real DB Read-Only Oracle

生产 DB 只读复核 `pkg_ma_0c796d57d216ebbd1daf0412`：

```json
{
  "package_id": "pkg_ma_0c796d57d216ebbd1daf0412",
  "manifest_runtime_assets_is_null": true,
  "package_asset_counts": {
    "factor_code": 33,
    "model_code": 1,
    "model_weight": 2
  },
  "leg_child_ids": [
    "pkg_mac_6e48c4963846f7bf4f16a5f9",
    "pkg_mac_a889a92ef523d91a1c103dc1"
  ],
  "child_runtime_assets": {
    "pkg_mac_6e48c4963846f7bf4f16a5f9": {
      "alpha_mode": "single_alpha",
      "runtime_assets_is_null": false,
      "alpha158_enabled": true,
      "alpha158_sha256": "3bde7a5534d0934dadd764277d462a21c676b51f7b175d3fbaf1d396396bcb26"
    },
    "pkg_mac_a889a92ef523d91a1c103dc1": {
      "alpha_mode": "single_alpha",
      "runtime_assets_is_null": false,
      "alpha158_enabled": false,
      "alpha158_sha256": null
    }
  }
}
```

真实 WSL parity 命令按 fail-closed 退出，未伪造 child/QE fallback：

```powershell
$env:PYTHONPATH='.'
$env:AISTOCK_PACKAGE_ASSET_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\package_assets'
rtk python debug_tools/strategy_package/multi_alpha_parent_self_contained/compare_parent_vs_legacy_child.py --package-id pkg_ma_0c796d57d216ebbd1daf0412 --trade-date 2024-07-02 --backend wsl --read-only
# FAIL-CLOSED: exit 1
```

```json
{
  "ok": false,
  "error_type": "DataUnavailableError",
  "error": "MULTI_ALPHA parent leg is missing runtime asset mapping",
  "context": {
    "reason_code": "multi_alpha_parent_alpha158_schema_missing",
    "package_id": "pkg_ma_0c796d57d216ebbd1daf0412",
    "leg_id": "a1_plus3_LSTM_h20",
    "model_id": "__seed_LSTM_10D_hs64_d02__"
  }
}
```

结论：现存父包缺 `runtime_assets` 与 `factor_schema` ledger row，不能满足“现存 `pkg_ma_0c79` 直接 parent-self-contained parity 通过”。本 PR 选择正确 fail-closed；后续需要 Tier2 批准 refreeze/manifest migration/backfill 后再复跑 parity。本 PR 不执行该 DML。

## Design Compliance Summary

| item | status | evidence |
|---|---|---|
| F-001/F-002 runtime parent leg slice | PASS | child repository sentinel、per-leg cache namespace、metadata `runtime_source=parent_package_asset` |
| F-003/F-011 parity | BLOCKED_BY_LEGACY_DB_GAP | debug oracle 已提交；真实 `pkg_ma_0c79` 因缺 Alpha158 schema fail-closed |
| F-004 dry-run | PASS | dry-run evidence 写入 `parent_asset_runtime` |
| F-005/F-006 promotion parent-only | PASS | 0 single-alpha 子包、0 child edge、非空 `component_package_ids` 拒绝 |
| F-007/F-008 freeze/self-check | PASS | per-leg model/factor/schema 严格 self-check 与 combined smoke |
| F-009 legacy compatibility | PARTIAL_WITH_EXPLICIT_GAP | legacy `child_package:` ref 被 ignored；但该历史父包缺 schema，不能无迁移跑通 |
| F-010 cleanup readiness | PASS | 仅列 retire 后续步骤；未执行 DML |
| F-012 single-alpha regression | PASS | `backend/tests/strategy_package` 338 passed |

Feature workflow gate is intentionally not marked PASS after the real DB finding, because the approved design required existing `pkg_ma_0c79` parity and the existing parent package lacks the schema asset needed to run without child fallback. This PR is suitable for Tier2 review as a draft/blocked implementation, not for merge as complete, until Tier2 approves a refreeze/migration/backfill path or revises the acceptance scope.

## Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_runtime_gate=pending_user_activation`
- 未启动/重启 backend、frontend、TDX 或其他服务。
- 未执行生产 DML/DDL；生产 DB 访问仅只读查询。
