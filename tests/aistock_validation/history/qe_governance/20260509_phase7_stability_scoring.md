# Phase 7 Stability Scoring

Date: 2026-05-09
Branch: `codex/qe-phase-7-stability-scoring-20260509`
Worktree: `F:\Dev\AIstock_worktrees\qe-phase-7-stability-scoring-20260509`
Base: `origin/codex/qe-governance-integration-20260509` at `46bcdda`

## Scope

Add read-only seed-stability and regime-stability scoring on top of Phase 7 `strategy_pkg.package_validation_run` evidence.

This phase:

- Computes stability summaries from validation-run metrics/evidence.
- Marks seed/regime fragile only when enough samples exist.
- Reports `INSUFFICIENT_EVIDENCE` explicitly instead of silently treating missing samples as stable.
- Adds a read-only API endpoint for the summary.
- Does not write DB rows, mutate validation runs, mutate StrategyPackage manifests, or change Paper state.

## Documents Read

- `docs/codex_project_memory.md`
- `docs/standards/aistock_development_standard_v1.1_20260504.md`
- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`
- `tests/aistock_validation/modules/qe_governance.md`
- `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`

## Gates Run

- `python -m py_compile backend/services/strategy_package/validation_stability.py backend/services/strategy_package/service.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_validation_stability.py` - passed.
- `python -m pytest backend/tests/strategy_package/test_validation_stability.py -q -p no:cacheprovider` - `6 passed`.
- `python -m pytest backend/tests/strategy_package -q -p no:cacheprovider` - `81 passed`.
- `python -m pytest backend/tests -q -p no:cacheprovider -k "validation_stability or validation_run or runtime_variant or strategy_package"` - `88 passed, 898 deselected`.
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` - passed with `blocking=0`.
- `git diff --check` - passed; Git emitted CRLF normalization warnings only.

## Dev-Port Note

Before this phase, a readonly smoke was attempted against `http://127.0.0.1:8011`; it did not touch production `8001`, but the target returned 404 for Validation Center endpoints, so it is not accepted as a successful dev-port business smoke.

## Isolation

- Production `8001`: not touched.
- Protected assets: not touched.
- DB writes: none.
- `main`: not touched; this branch must merge only into `codex/qe-governance-integration-20260509` until user approval.
