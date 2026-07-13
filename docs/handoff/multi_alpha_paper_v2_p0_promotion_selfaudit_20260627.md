# Multi Alpha -> Paper v2 P0 Promotion Self-Audit (2026-06-27)

## Scope

- Branch: `feature/multi-alpha-paper-v2-p0-promotion-20260627`
- Worktree: `F:\Dev\AIstock_worktrees\multi-alpha-paper-v2-p0-promotion-20260627`
- Design: `docs/analysis/multi_alpha_paper_v2_route_architecture_20260626.md`
- GitHub issue: `#1648`
- This PR implements P0 only: `MultiAlphaPackagePromotionService`, `POST /api/v1/strategy-packages/from-multi-alpha-combine-run`, and frozen `MULTI_ALPHA` manifest creation.

## Design Compliance

| Design Item | Implementation Evidence | Validation Evidence | Status |
|---|---|---|---|
| F-001: Paper v2 keeps one `package_id`; multi-alpha is one StrategyPackage | `backend/services/strategy_package/multi_alpha_promotion.py` creates a parent `alpha_mode=MULTI_ALPHA` manifest; no `PaperPortfolio.package_id` change | `test_promote_target_two_leg_run_freezes_deterministic_multi_alpha_package` | PASS |
| F-002: validate run, scheme, roster, weights, metrics, seed, child package, prediction ref | Promotion service fail-loud checks each item with `reason_code` | `test_promote_fails_loud_with_reason_codes`, `test_router_endpoint_negative_paths_are_loud` | PASS |
| F-003: freeze legs, seed ids, child sha, weight policy, topk, execution policy, prediction ref, backtest evidence | Evidence is stored in `source_evidence.multi_alpha` and `backtest_context`; existing `freeze_manifest` computes `manifest_sha256` | Same input produces the same `package_id` and `manifest_sha256` | PASS |
| F-004/F-005/F-006: live inference / rolling IC / selection artifact deferred to P1a | P0 does not implement live inference, weight service, or selection artifact generation | `git diff --name-only` does not touch runtime/selection/Paper execution paths | PASS |
| F-007: missing leg/seed/weight/ref or weak metrics fail loudly | All failure paths raise `TradingCoreError` with `context.reason_code` | Negative tests cover missing child, missing seed, missing scheme, missing prediction ref, and metrics gate | PASS |
| F-008: SINGLE_ALPHA compatibility and Paper v2 package main contract unchanged | Only new endpoint/service plus multi-alpha admission blocker; no single-alpha flow edits | `backend/tests/strategy_package` full suite passes | PASS |
| F-009/F-010: dry-run and activation remain separated | Parent package defaults to `ASSET_VALIDATED`, but asset eligibility blocks with `multi_alpha_runtime_not_validated_until_dry_run`; no automatic `PAPER_ENABLED` | `test_asset_eligibility_blocks_multi_alpha_until_dry_run` | PASS |

## Boundary Evidence

- Execution layer untouched: no edits under `simulation_runtime`, `miniqmt_execution_runtime`, `qmt_strategy_ledger`, `paper_trading_v2/broker`, scheduler, or bridges.
- Single-package main contract untouched: no `PaperPortfolio.package_id` change and no new `AlphaMode` enum.
- P1a intentionally not implemented: no live inference, weight service, or selection artifact generation.
- Research Assistant untouched: no `assistant_` / `research_` path in `git diff --name-only`.
- No DDL/DML: no migration added and no production DB write executed.
- No services started or restarted.
- `production_ddl_gate=noop`.

## Validation Log

```text
rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_paper_v2_route_architecture_20260626.md --tier F2
Feature workflow validation: PASS
tier=F2 design_items=10 matrix_rows=10 warnings=0

rtk python -X utf8 -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py -q
15 passed

rtk python -X utf8 -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_base_schema.py backend/tests/strategy_package/test_enable_paper_invariants.py backend/tests/strategy_package/test_enable_paper_router_409.py backend/tests/strategy_package/test_repository_service.py -q
61 passed

rtk python -X utf8 -m pytest backend/tests/strategy_package -q
217 passed

rtk python -X utf8 -m pytest backend/tests/test_multi_alpha_combine_backtest.py -q
36 passed

rtk python -m nox -s l0
Session l0 was successful

rtk ruff check backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/asset_eligibility.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_multi_alpha_promotion.py
Ruff: No issues found

rtk python -X utf8 -m py_compile backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/asset_eligibility.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_multi_alpha_promotion.py
PASS

rtk git diff --check
PASS
```

## Tier2 Review Notes

1. P0 accepts only `weight_policy.mode=frozen_backtest_terminal_weights`; `live_rolling_ic_weighted` is fail-loud and deferred to P1a.
2. Parent `source_type` reuses `candidate_strategy_package` to avoid P0 DDL; true provenance is frozen under `source_evidence.multi_alpha.source_type=multi_alpha_combine_backtest`.
3. After merge, user-owned backend restart is required for the new endpoint to load. This window does not restart services, merge, or close-sync.
