# Phase 7 Validation Modes Foundation

Date: 2026-05-09
Branch: `codex/qe-phase-7-validation-modes-20260509`
Worktree: `F:\Dev\AIstock_worktrees\qe-phase-7-validation-modes-20260509`
Base: `origin/codex/qe-governance-integration-20260509` at `a86fe21`

## Scope

Add the StrategyPackage validation-run foundation for latest-data fixed-weight validation, latest-data retrain validation, walk-forward rolling validation, and runtime-variant validation evidence.

This phase is additive only:

- Adds `strategy_pkg.package_validation_run` DDL with PostgreSQL comments.
- Adds backend model/service/repository/router support for append-only validation runs.
- Keeps original QE metrics and frozen StrategyPackage manifest/core unchanged.
- Does not execute live QE, Paper v2, broker, or production backend `8001`.
- Does not write any production DB rows.

## Documents Read

- `docs/codex_project_memory.md`
- `docs/standards/aistock_development_standard_v1.1_20260504.md`
- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`
- `tests/aistock_validation/modules/qe_governance.md`
- `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`

## Gates Run

- `python -m py_compile backend/services/strategy_package/validation_run.py backend/services/strategy_package/repository.py backend/services/strategy_package/service.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_validation_runs.py` - passed.
- `python -m pytest backend/tests/strategy_package/test_validation_runs.py -q -p no:cacheprovider` - `8 passed`.
- `python -m pytest backend/tests/strategy_package -q -p no:cacheprovider` - `75 passed`.
- `python -m pytest backend/tests -q -p no:cacheprovider -k "validation_run or runtime_variant or strategy_package or seed_contract or promotion_review"` - `82 passed, 898 deselected`.
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` - passed with `blocking=0`; one P2 complexity review note on the existing large repository module.
- `git diff --check` - passed; Git emitted CRLF normalization warnings only.

## Business Assertions

- Latest-data validations require an explicit `target_data_version`.
- Retrain validations require a non-empty seed policy.
- Walk-forward validations require `rolling_retrain` and non-empty window evidence.
- Runtime-variant validations require an existing variant and record the variant hash.
- Passed validation runs require metrics, artifact manifest evidence, and `completed_at`.
- Validation runs append evidence to `strategy_pkg.package_validation_run`; they do not mutate frozen manifests, protected assets, source QE metrics, or runtime variants.

## Isolation

- Production `8001`: not touched.
- Protected assets: not touched.
- DB writes: none during this code/test phase.
- `main`: not touched; this branch must merge only into `codex/qe-governance-integration-20260509` until user approval.
