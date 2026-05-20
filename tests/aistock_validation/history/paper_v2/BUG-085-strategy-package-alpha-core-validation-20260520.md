# BUG-085 StrategyPackage alpha-core boundary validation

- Module: strategy_package / paper_v2 / selection_center
- Level: L3 backend + guardrail
- Date: 2026-05-20T17:55:00+08:00
- Branch: bug/BUG-085-strategy-package-alpha-core
- Worktree: F:\Dev\AIstock_worktrees\bug-085-strategy-package-alpha-core
- Git commit: pre-commit validation record; final commit hash will be written back after commit
- Operator: codex-app

## Scope

- Bug: BUG-085 / GitHub #107, StrategyPackage manifest schema still binds platform runtime policies instead of alpha core only.
- Changed modules: StrategyPackage manifest/QE resolver/runtime contract/runtime config/candidate snapshot/model asset resolver/selection artifact, Selection Center runtime config, Paper v2 portfolio service/day runner/readiness/live session, manifest contract docs, file ownership catalog, and targeted regression tests.
- Business goal: new StrategyPackage manifests bind only factor/model alpha core and audit/source evidence. Daily strategy, top_k, universe/ST PIT, HMM, risk/event, minute execution, broker/account, and approval remain platform runtime/profile/policy capabilities.
- Out of scope: production backend/frontend restart, production DB/migrations, live MiniQMT submit/cancel, and UI E2E.
- Protected assets reviewed: no QE workspaces, model artifacts, factor artifacts, HMM snapshots, production DB rows, or broker ledgers were modified.

## Design Compliance Matrix

| Requirement / closure item | Implementation refs | Validation evidence | Status | Gap / exception |
|---|---|---|---|---|
| New StrategyPackage manifest must be alpha-core only and not require platform runtime fields | `backend/services/strategy_package/models.py`, `backend/tests/strategy_package/test_manifest_alpha_core_boundary.py` | `test_alpha_core_manifest_accepts_factor_model_core_without_platform_runtime_fields`; `test_alpha_core_manifest_rejects_bound_platform_runtime_fields`; `python -m pytest backend/tests/strategy_package -q` -> 172 passed | PASS | Legacy v1 runtime fields remain parseable only under `manifest_version="1.0"` |
| QE resolver must not copy QE runtime/platform policy into frozen manifest authority | `backend/services/strategy_package/qe_source_resolver.py`, `backend/services/strategy_package/backtest_contract.py` | `test_qe_source_resolver_emits_alpha_core_manifest_with_audit_only_runtime_evidence`; `test_qe_source_resolver.py`; strategy package suite 172 passed | PASS | QE historical runtime settings are retained only as `source_evidence` / `backtest_context` audit context |
| Selection/Paper/MiniQMT runtime behavior must use platform profile or validated execution policy, not manifest runtime fields | `backend/services/selection_center/service.py`, `backend/services/paper_trading_v2/service.py`, `backend/services/paper_trading_v2/day_runner.py`, `backend/services/paper_trading_v2/readiness.py`, `backend/services/paper_trading_v2/live_session.py` | `test_alpha_core_paper_portfolio_requires_explicit_validated_execution_policy`; `test_selection_center_uses_runtime_profile_or_source_evidence_not_manifest_runtime_fields`; `python -m nox -s paper_v2_backend` -> 448 passed, 1 skipped, 2 xfailed | PASS | Old single-order/local daemon helper paths still read legacy manifest minute policy; they are not the Paper v2 Selection/MiniQMT portfolio path and should be covered by a future legacy-deprecation issue if retained |
| Runtime config/semantics hash must exclude platform runtime policies | `backend/services/strategy_package/runtime_config.py`, `backend/tests/strategy_package/test_runtime_config_contract.py` | `test_strategy_semantics_hash_is_shared_across_qe_and_paper_adapters`; `test_strategy_semantics_hash_ignores_platform_runtime_policy_changes`; strategy package suite 172 passed | PASS | None |
| Candidate snapshot must not expose executable runtime policy fields as strategy manifest authority | `backend/services/strategy_package/candidate.py`, `backend/tests/strategy_package/test_candidate_strategy_package.py` | candidate tests inside strategy package suite 172 passed | PASS | Candidate snapshot records alpha-core hash/source evidence/backtest context only |
| Legacy v1 contract must be marked superseded to prevent future boundary confusion | `docs/contracts/strategy_package_manifest_v1.md`, `docs/architecture/strategy_package_platform_boundary_contract_20260520.md` | manual doc review plus changed-file ownership guardrail; `nox -s guardrail_changed_files -- --changed-only` passed | PASS | Old sections remain for historical v1 compatibility but top notice is authoritative |
| All affected code paths either consume platform profile/policy or fail fast with migration-required error | `backend/services/paper_trading_v2/service.py`, `backend/services/paper_trading_v2/day_runner.py`, `backend/services/strategy_package/model_asset_resolver.py` | `test_alpha_core_paper_portfolio_requires_explicit_validated_execution_policy`; paper v2 backend 448 passed; focused Paper v2/MiniQMT suite 65 passed | PASS | Legacy v1 auto-default policy path remains only for `manifest_version="1.0"` compatibility |
| Development-standard guardrails and ownership mapping must pass for changed files | `tests/aistock_validation/catalog/file_ownership.yaml` | `python -m nox -s l0`; `python -m nox -s guardrail_changed_files -- --changed-only`; `git diff --check` | PASS | `l0` reports existing non-blocking P2/UI raw JSON and baseline findings, no blocking count |

## Commands And Results

```bash
python -m pytest backend/tests/strategy_package/test_manifest_alpha_core_boundary.py -q
# 5 passed in 1.70s

python -m pytest backend/tests/strategy_package/test_manifest_v1.py backend/tests/strategy_package/test_qe_source_resolver.py backend/tests/strategy_package/test_runtime_config_contract.py backend/tests/strategy_package/test_candidate_strategy_package.py backend/tests/strategy_package/test_manifest_alpha_core_boundary.py -q
# 34 passed in 14.78s

python -m pytest backend/tests/strategy_package/test_backtest_contract.py backend/tests/strategy_package/test_score_weighted_capacity_contract.py backend/tests/strategy_package/test_model_asset_resolver.py backend/tests/strategy_package/test_repository_service.py -q
# 40 passed in 1.38s

python -m pytest backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q
# 37 passed in 1.25s

python -m pytest backend/tests/paper_trading_v2/test_day_runner.py -q
# 22 passed in 1.14s

python -m pytest backend/tests/selection_center/test_runtime_selection.py -q
# 41 passed in 1.42s

python -m pytest backend/tests/paper_trading_v2/test_runtime_profile.py -q
# first run failed because source daily_strategy defaults were persisted into profile versions; fixed by disabling source-default inheritance for profile-version creation and not persisting unset daily strategy placeholders
# rerun: 6 passed in 1.31s

python -m pytest backend/tests/strategy_package -q
# 172 passed in 10.59s

python -m pytest backend/tests/selection_center/test_runtime_selection.py -q
# 41 passed in 1.52s

python -m pytest backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_runtime_profile.py -q
# 65 passed in 1.30s

python -m nox -s paper_v2_backend
# first run failed 1 test in test_runtime_profile.py; fixed and reran
# final: 448 passed, 1 skipped, 2 xfailed in 18.31s; session successful in 21 seconds

python -m nox -s l0
# session successful; guardrail scan showed existing non-blocking P2/UI raw JSON and baseline findings; blocking=0

python -m nox -s guardrail_changed_files -- --changed-only
# first run failed on unmapped docs/contracts/strategy_package_manifest_v1.md; fixed ownership catalog
# final: guardrail blocking=0; module ownership files=24, mapped=24, unmapped=0, ambiguous=0; session successful

git diff --check
# passed
```

## Failure History And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `test_runtime_profile_version_hash_and_audit_are_persisted` failed in `paper_v2_backend` | source `backtest_context.daily_strategy` defaults were being persisted into runtime profile versions, which would make audit/profile versions look package-derived | `_normalize_runtime_profile_config()` now calls `normalize_runtime_config_with_backtest_contract(..., inherit_source_defaults=False)` and drops unset daily-strategy placeholders | `backend/tests/paper_trading_v2/test_runtime_profile.py -q` -> 6 passed; `nox -s paper_v2_backend` -> 448 passed |
| `guardrail_changed_files` failed on unmapped docs contract | `docs/contracts/strategy_package_manifest_v1.md` had no file ownership rule though it is a StrategyPackage contract | Added `docs/contracts/strategy_package*.md` to `strategy_package_services` ownership rule | `nox -s guardrail_changed_files -- --changed-only` passed |
| P0 guardrail false-positive on legacy `MinuteFallbackPolicy` line drift | BUG-085 added lines before an existing historical fallback-policy pattern, making the fingerprint appear new | Converted the legacy fallback policy class to a typed fail-only dict default while preserving serialized JSON contract | Strategy package suite passed; changed-file guardrail blocking=0 |

## Business Outcome

- New alpha-core manifests default to `manifest_version="alpha_core_v1"` and reject bound `strategy_config`, `universe_policy`, `portfolio_policy`, `execution_policy`, `minute_execution_policy`, and `risk_policy`.
- QE source resolution emits alpha-core manifests; QE daily/execution/HMM/ST PIT/risk context is audit-only source/backtest evidence, not executable runtime authority.
- Paper v2 portfolio creation for alpha-core packages requires an explicit backtest-validated execution policy id; no `manifest_default_execution_policy` is auto-created from an alpha-core manifest.
- Selection Center and Paper v2 use runtime profile / source contract defaults / validated execution policy snapshots and fail fast when runtime authority is missing.
- Candidate StrategyPackage snapshot exposes alpha-core identity and audit context, not platform runtime policy fields.

## Residual Risks

- Legacy `manifest_version="1.0"` compatibility remains intentionally parseable; old helper paths such as `paper_trading_v2/runner.py`, `broker/localsim.py`, and daemon demo/test utilities still read legacy `manifest.minute_execution_policy`. They are not the current Paper v2 Selection/MiniQMT portfolio path, but they should be revisited under a separate deprecation/legacy-path issue if product policy requires removing all legacy manifest execution reads.
- UI E2E, production API smoke, and live MiniQMT submit/cancel were not run because this issue is backend contract/manifest boundary work and production ports were not touched.
- Branch is behind latest `origin/main` by two unrelated BUG-079/QE archive commits at validation time; touched files do not overlap those commits based on `git log --stat HEAD..origin/main`.

## Production Impact

- Production backend `8001`: not started or restarted.
- Frontend `3000`: not started or restarted.
- Production DB/migrations: not touched.
- MiniQMT process/account: not touched.
- Runtime activation after merge: backend restart will be needed for code to take effect, but was intentionally not performed here.

## Result

- Final local validation status: PASS for BUG-085 pre-commit backend/guardrail evidence.
- Recommended issue state after commit: fixed-pending-review / GitHub label `status:fixed-pending-review`.
