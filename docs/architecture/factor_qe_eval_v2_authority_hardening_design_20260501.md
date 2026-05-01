# Factor qe_eval_v2 Authority Hardening Design - 2026-05-01

## Scope

This document is the implementation design for hardening AIstock factor independent metrics, factor classification, and official factor rating authority. It intentionally excludes running full-library independent metric calculation, full-library classification, or full-library official rating. Those full-library operations must be triggered manually from the UI by the operator.

## Current Authority Model

- `aistock_factor_metrics` and `aistock_factor_monthly_ic` are the official independent metrics tables. Runtime reads must always scope to `calc_engine = 'qe_eval_v2'`.
- `qe_factor_official_ratings` is the official factor rating table. `official_grade` and `official_score` are the only production rating fields.
- `qe_factor_classification` is still the official factor classification/profile metadata table. It is not an official rating table.
- `qe_factor_classification.grade`, `ic_value`, `sharpe_value`, and `ann_ret_value` are legacy mirror fields. They must not be used for production filtering, sorting, scoring, prompts, or UI authority display.

## Baseline Alignment

The current codebase has one production official rating service:

- `backend/services/quantevolver/factor_rating_service.py` owns official factor rating writes and reads.
- `backend/rating_rules/factor/index.json` and `qe_rating_rule_versions` own rule-version discovery and activation state.
- `backend/services/quantevolver/factor_analyst.py` owns classification/profile metadata writes into `qe_factor_classification`.
- `backend/routers/quantevolver.py` exposes the UI-facing factor list, selected-scope metric calculation, selected-scope classification, and selected-scope official rating APIs.
- `frontend/src/app/quantevolver/components/FactorList.tsx` is the current UI entry point for selecting factors and triggering selected-scope operations.

The deprecated v1 rating rule is retained only as archived historical metadata. Normal operator flows, API flows, and service-layer entry points must only execute v2 rules. RD-Agent native metric code may still exist for RD-Agent's own runtime, but AIstock official `qe_eval_v2` metrics should become self-contained in a later phase without deleting RD-Agent-owned code.

## Implementation Checklist

This phase implements the items that can be safely changed without running full-library operations:

1. Archive v1 factor rating by default in file index and DB state.
2. Reject archived or non-v2 rating rule execution in the service layer.
3. Require exactly one active rating rule when listing rules.
4. Require v2 rating inputs to be complete before writing official ratings.
5. Replace classification, monthly IC, and dedup silent fallbacks with explicit errors.
6. Replace unknown rule-only classification default `TECH` with fail-fast.
7. Add static/unit tests that prevent regression to v1 execution and legacy classification rating fields.
8. Make the factor-library UI show archived rules as audit-only and disable activation of archived or non-v2 rules.
9. Validate only a selected five-factor UI flow; do not trigger full-library metric calculation, classification, or official rating.

## Implemented In This Phase

### V1 Rating Rule Archive Guard

- `backend/rating_rules/factor/index.json` is now version-controlled even though JSON files are generally ignored.
- The only default and active rule is `v2.0.0`.
- `v1.0.0` is archived in both the file index and `qe_rating_rule_versions`.
- `FactorRatingService` now rejects non-v2 rule execution before any DB-backed scoring code can run.
- Archived rules cannot be activated through the service layer.

### V2 Rating Fail-Fast Inputs

`FactorRatingService` must fail explicitly when official rating inputs are unavailable or incomplete:

- Missing required `qe_eval_v2` metric windows: `full`, `out_sample`, `recent_6m`, `recent_3m`.
- Missing required full-window metric fields used by v2 scoring and hard gates.
- Missing `qe_factor_classification` row or required v2 metadata fields.
- Missing latest monthly IC row or missing `sign_consistency_12m` / `oos_is_ratio`.
- Missing `aistock_factor_catalog.is_dedup_primary`.

No neutral defaults, empty dictionaries, implicit primary dedup status, or fake categories are allowed in official v2 rating.

### Classification Fail-Fast

Rule-only classification must not silently classify unknown factors as `TECH`. If no rule or LLM output produces a category, the classification operation fails with an explicit error. The unified pipeline must treat that as Step A failure and skip Step B rating for that factor.

## Implemented Follow-up - 2026-05-01

### AIstock-owned qe_eval_v2 Metrics Kernel

- Added `backend/services/quantevolver/qe_eval_v2_metric_engine.py` as the AIstock-owned official independent metrics kernel.
- Added `backend/services/quantevolver/qe_eval_v2_qlib_reader.py` as the local Qlib close-price reader used by the AIstock-owned kernel.
- `backend/services/quantevolver/factor_official_evaluation_service.py` now imports `prepare_shared_context` and `compute_single_factor_metrics` from the AIstock-owned kernel.
- `scripts/compute_factor_metrics_unified.py` now bootstraps the AIstock repository root onto `sys.path` and imports `backend.services.quantevolver.qe_eval_v2_metric_engine` instead of `rdagent.app.factor_metrics.engine`.
- RD-Agent native files are untouched and remain available for RD-Agent's own runtime. AIstock official metric paths are guarded by tests against importing `rdagent.app.factor_metrics`.

### PIT Coverage Semantics

The existing `aistock_factor_metrics.coverage` column is redefined as:

```text
coverage = finite_factor_value_count / pit_eligible_non_warmup_sample_count
```

The denominator must include only point-in-time eligible samples:

- listed at the evaluation date;
- not delisted at the evaluation date;
- normal trading date in the evaluation calendar;
- not suspended when suspension state is part of the official PIT eligibility source;
- outside the factor's deterministic rolling-window warm-up period.

The numerator counts only finite factor values. NaN, inf, missing, and calculation failures remain invalid values. This coverage is intended to measure real factor data availability for tradable PIT samples, not raw matrix density across never-listed or unavailable securities.

Implementation details:

- The official kernel reports `coverage_semantics = 'pit_listed_tradable_non_warmup_v1'`.
- Listed/trading eligibility is derived from the aligned close-price and forward-return matrices.
- Suspension exclusion is loaded from `market.suspend_d` by default. If that official source cannot be read, metric preparation fails explicitly.
- Rolling-window warm-up is detected on the full factor time series per instrument, then sliced into each evaluation window. A NaN appearing after the first finite value remains a real missing value and is counted against coverage.
- Non-official isolated tests may pass `load_suspend_d=False`; official UI/API flows keep the fail-fast default.

### Legacy Classification Field Cleanup

- `FactorAnalyst._upsert_classification` writes `NULL` to legacy metric mirror fields `ic_value`, `sharpe_value`, and `ann_ret_value`; official metric values remain in `aistock_factor_metrics`.
- UI-facing factor-list code reads production grade/score from `qe_factor_official_ratings`.
- Backward-compatible classification fields are exposed only as `legacy_*` where still needed for audit display, and are excluded from sorting/filtering/prompts.
- Static tests prevent Quantevolver UI paths from reading legacy classification rating fields as authority fields.

### Duplicate Classification Rows

Current data contains two duplicate classification rows by `factor_catalog_id`, caused by name-case variants. Clean-up should be executed only after a backup or manual review:

- keep the row matching `aistock_factor_catalog.factor_name`;
- merge useful metadata if needed;
- delete the duplicate row;
- add a guard to prevent future one-catalog-to-many classification rows.

Implementation guard:

- New classification writes require an exact `aistock_factor_catalog` row for `(factor_name, factor_source)`.
- If another `qe_factor_classification` row already maps to the same `factor_catalog_id`, the write fails with an explicit duplicate-row error and requires manual cleanup.
- No historical duplicate rows are deleted by Codex in this phase.

## Remaining Manual / Later Work

### Parity Audit Against RD-Agent Metric Outputs

- The new AIstock kernel is copied from the current RD-Agent metric implementation and then changed only for authority imports, PIT coverage semantics, and fail-fast source ownership.
- A small in-memory coverage test exists, but a real DB/Qlib parity audit comparing old RD-Agent metric output versus the AIstock-owned kernel on a fixed audited factor sample still remains a later manual/diagnostic task.
- That parity audit must not modify RD-Agent assets or StrategyPackage/QE artifacts.

### Full-Library Operations

The following operations are intentionally not executed by Codex in this phase:

- full-library independent metric recalculation;
- full-library factor classification rerun;
- full-library official rating rerun;
- full-library deletion/cleanup decisions.

They should be executed manually from the UI after the operator confirms the implementation and desired run scope.

## Validation Completed - 2026-05-01

- `conda run -n AIstock python -m py_compile backend/services/quantevolver/qe_eval_v2_metric_engine.py backend/services/quantevolver/qe_eval_v2_qlib_reader.py backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/factor_analyst.py scripts/compute_factor_metrics_unified.py backend/tests/test_factor_metrics_authority_static.py`
- `conda run -n AIstock pytest backend/tests/test_manual_factor_service_wsl_output.py backend/tests/test_factor_cache_wsl_env.py backend/tests/test_factor_metrics_authority_static.py -q -p no:cacheprovider` -> 22 passed.
- `npm exec tsc -- --noEmit` in `frontend` -> passed.
- `conda run -n AIstock python -m nox -s l0` -> passed; existing MEDIUM guardrail findings remain in Paper v2 tests and are outside this factor qe_eval_v2 change scope.
- Synthetic in-memory `_compute_factor_metrics_impl` smoke with 160 business days and 20 instruments -> all five eval windows returned `ok`, `coverage_full = 1.0`; expected NumPy warnings were emitted for warm-up/terminal all-NaN slices.
- Real UI validation on dev ports `8012/3012`: selected snapshot `20260410` and factor `WVMA5` from `alpha158`, clicked the selected-factor metrics button, backend inserted 5 rows successfully; DB before/after comparison showed max absolute difference `0.0` and mismatch count `0` for every field except `coverage`, `calculated_at`, and `calc_batch_id`. Coverage changed from old raw-density values to PIT coverage as expected.
- Static scans confirmed official Quantevolver metric paths no longer import `rdagent.app.factor_metrics`; `scripts/quick_ic_screen.py` remains a non-official diagnostic script and still imports the RD-Agent reader.
- Full-library independent metric calculation, full-library classification, and full-library official rating were not executed.

## Validation Requirements

### Automated Tests

- Static tests must prove that v1 is archived and v2 is the only default active rule.
- Static tests must reject production reads of legacy classification rating fields.
- Unit tests must verify non-v2 rating rules cannot execute.
- Existing factor authority tests must continue to pass.

### UI Smoke Validation

Use non-production ports only:

- backend: `8011` or `8012`;
- frontend: `3011` or `3012`.

The UI validation should select five known-good factors and verify:

1. classification can run or already displays valid classification metadata;
2. official independent metrics are visible with `calc_engine = qe_eval_v2`;
3. official rating uses v2 only;
4. v1 cannot be selected as a default executable path;
5. failure messages are explicit if any required v2 input is missing.

This validation is a small-sample flow check only. It must not trigger full-library recalculation, full-library classification, or full-library rating.
