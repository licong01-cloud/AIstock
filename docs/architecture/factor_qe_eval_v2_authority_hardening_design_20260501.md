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

## Still To Implement In Later Phases

### AIstock-owned qe_eval_v2 Metrics Kernel

- Move the official metric calculation kernel into AIstock-owned code under the `qe_eval_v2` authority boundary.
- Keep RD-Agent native files untouched; do not delete `rdagent.app.factor_metrics.engine`.
- Replace runtime imports of `rdagent.app.factor_metrics.engine.compute_single_factor_metrics` in official AIstock metric calculation.
- Add parity tests that compare the migrated AIstock kernel against the current RD-Agent/Qlib metric outputs on a small audited sample.
- Add a static guard that prevents official metric paths from importing `rdagent.app.factor_metrics`.

### PIT Coverage Semantics

Keep the existing `aistock_factor_metrics.coverage` column but redefine its meaning:

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

### Legacy Classification Field Cleanup

- Stop exposing `qe_factor_classification.grade`, `ic_value`, `sharpe_value`, and `ann_ret_value` as normal business fields.
- If backward compatibility requires returning them, they must be named `legacy_*` and excluded from sorting/filtering/prompts.
- Inventory scripts that still read or write those legacy fields and classify them as production, migration, diagnostic, or deprecated.
- Production paths must read official ratings and official metrics instead.

### Duplicate Classification Rows

Current data contains two duplicate classification rows by `factor_catalog_id`, caused by name-case variants. Clean-up should be executed only after a backup or manual review:

- keep the row matching `aistock_factor_catalog.factor_name`;
- merge useful metadata if needed;
- delete the duplicate row;
- add a guard to prevent future one-catalog-to-many classification rows.

### Full-Library Operations

The following operations are intentionally not executed by Codex in this phase:

- full-library independent metric recalculation;
- full-library factor classification rerun;
- full-library official rating rerun;
- full-library deletion/cleanup decisions.

They should be executed manually from the UI after the operator confirms the implementation and desired run scope.

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
