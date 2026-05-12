# R7 Retrain Pipeline and Regime Metrics Automation Design

**Date**: 2026-05-12
**Purpose**: Convert the manual Task 9 evidence preparation path into a repeatable R7 sprint pipeline.

## Problem Statement

Task 9 proved that a file-only evidence bundle ETL can produce the governance bundle shape, but the `pkg_5a5c` preview exposed real evidence gaps:

- Model-weight artifact discovery is not guaranteed.
- DB-verified `manifest_sha256` has no file-only export step.
- Daily backtest rows are not bundled with the QE artifact snapshot.
- `market.regime_label` is not exported with the same date range as the backtest.
- ORIGINAL_RETRAIN fixed-seed outputs are not produced or collected automatically.
- Production backfill expects an operator-ready bundle and plan, but current evidence collection is manual.

R7 should automate evidence generation and make manual gaps explicit before a package reaches paper enablement.

## Goals

1. Produce original fixed-weight validation evidence for a frozen StrategyPackage manifest.
2. Run at least two fixed-seed ORIGINAL_RETRAIN validations on the same manifest.
3. Export daily backtest returns and market regime labels for the exact same date range.
4. Compute per-regime metrics deterministically from file inputs.
5. Collect model-weight artifacts and SHA256 values from the QE workspace or MLflow artifact store.
6. Build an evidence bundle accepted by the existing governance planner/executor chain.
7. Emit operator artifacts: bundle JSON, plan JSON, preview JSON, SOP, checksums, and immutable source manifest.

## Non-Goals

- No live broker or real paper trade execution in R7 evidence generation.
- No production DB writes from the automation runner.
- No bypass of `enable_paper` governance gates.
- No silent fallback to latest package, latest model, or latest QE run.
- No mutation of existing StrategyPackage manifest rows.

## Proposed Pipeline

### Stage 1: Package and Manifest Lock

Inputs:
- Explicit package id list.
- DB-readonly export of package id, package status, `manifest_sha256`, runtime variant candidate state, and existing evidence rows.

Output:
- `r7_package_manifest_lock.json`
- Stable package list and manifest hashes.

Guardrails:
- Refuse package selectors such as latest/current/auto.
- Refuse mixed manifests for the same package.
- Refuse package statuses outside `BACKTEST_APPROVED`, `SELECTION_ENABLED`, `PAPER_ENABLED`.

### Stage 2: QE Artifact Harvest

Inputs:
- QE experiment directory or MLflow recorder id.
- Expected model artifact path (`params.pkl`, `model.pkl`, or configured equivalent).

Output:
- `r7_qe_artifact_manifest.json`
- Model-weight SHA256, size, source URI, metrics file hashes, and recorder metadata.

Implementation candidates:
- Extend `scripts/qe_to_evidence_bundle_etl.py` discovery into a reusable module.
- Add `scripts/qe_artifact_export.py` for MLflow/WSL artifact export into a local evidence vault.

### Stage 3: Original Fixed-Weight Retest

Inputs:
- Frozen manifest and original model weight.
- Backtest config and date range.

Output:
- `original_fixed_weight_metrics.json`
- `original_fixed_weight_daily_returns.csv`

Guardrails:
- Same manifest and same model weight SHA256.
- No retrain in this stage.
- Store source config and data version.

### Stage 4: Fixed-Seed Retrain Matrix

Inputs:
- Frozen factor/config set.
- Seed list, default `[101, 202]`.

Output:
- `seed101_metrics.json`, `seed202_metrics.json`
- Optional daily returns for each seed.
- Seed fragility summary.

Guardrails:
- Require explicit seed list.
- Refuse fewer than two successful seed runs.
- Preserve failed seed outputs for audit but do not mark as passed evidence.

### Stage 5: Regime Label Export

Inputs:
- Backtest date range.
- `market.regime_label` source method, default `simple_quadrant` unless strategy overrides.

Output:
- `market_regime_label_export.csv`
- Export metadata with source method, date range, row count, and SHA256.

Implementation candidates:
- Add `scripts/market_regime_label_export.py` with readonly DB mode and file-only test fixtures.
- Add API-compatible export path later if backend route is safer.

### Stage 6: Regime Metric Join

Inputs:
- Daily returns CSV.
- Regime label CSV.

Output:
- `regime_metrics.json` with bull/bear/other regime buckets.

Rules:
- Join by trade date.
- Require at least two regime buckets unless package is explicitly exempted by strategy.
- Report sample_count, mean_daily_return, annual_return, total_return, and optional drawdown/volatility if returns allow.

### Stage 7: Bundle Build and Planner Preview

Inputs:
- Package manifest lock.
- Artifact manifest.
- Original fixed-weight metrics.
- Fixed-seed metrics.
- Regime metrics.

Output:
- `r7_real_evidence_bundle.json`
- `r7_real_evidence_backfill_plan.json`
- `r7_real_evidence_executor_preview.json`

Use:
- `scripts/qe_to_evidence_bundle_etl.py --require-prod-ready`
- `scripts/strategy_package_governance_evidence_backfill_plan.py`
- `scripts/strategy_package_governance_evidence_backfill_prod_executor.py` in preview mode only.

### Stage 8: Operator Review Handoff

Output:
- Human-readable SOP.
- Machine-readable checksums.
- Cross-tool review drawer.
- Go/no-go checklist for release commander and DB operator.

## File / Module Plan

Candidate new files:
- `scripts/qe_artifact_export.py`
- `scripts/qe_fixed_seed_retrain_runner.py`
- `scripts/market_regime_label_export.py`
- `scripts/qe_evidence_bundle_pipeline.py`
- `backend/tests/scripts/test_qe_artifact_export.py`
- `backend/tests/scripts/test_qe_fixed_seed_retrain_runner.py`
- `backend/tests/scripts/test_market_regime_label_export.py`
- `backend/tests/scripts/test_qe_evidence_bundle_pipeline.py`

Candidate extensions:
- `scripts/qe_to_evidence_bundle_etl.py`: convert discovery helpers into reusable functions and support multi-package evidence vault manifests.
- `docs/operations/task11_real_evidence_backfill_sop_20260512.md`: keep as operator handoff template.
- Validation Center UI: add a read-only evidence readiness panel after backend API exists.

## Test Strategy

Unit tests:
- Artifact discovery checksum determinism.
- Missing model weight blocks.
- Explicit package id only; no latest fallback.
- Two-seed minimum enforcement.
- Regime label join and sample-count gate.
- Manifest mismatch refusal.
- Output accepted by existing governance planner.
- Production executor preview remains no DB connect.

Integration tests:
- File-only end-to-end pipeline with synthetic fixture artifacts.
- Dev DB readonly regime label export using a temporary test schema or fake cursor.
- Planner + executor preview with four-package fixture bundle.

Guardrail tests:
- No production DB writes in R7 generator scripts.
- No `--apply` path in evidence generation scripts.
- No imports of live broker, paper daemon, or Paper runtime execution path.
- No mutation of StrategyPackage manifest/package status.

## Risks

| Risk | Mitigation |
|---|---|
| MLflow artifacts only exist in WSL/remote paths | Add artifact export stage with checksum manifest. |
| Regime labels are absent for a date range | Export/generate labels before bundle build; block if join has no samples. |
| Fixed-seed retrain is expensive | Run background queue and cache by manifest/config/seed hash. |
| Single-package real evidence conflicts with four-package R6 executor | Either gather all four package bundles or create a separately reviewed one-package executor in a future decision. |
| Synthetic evidence remains after real backfill | Make rollback verification a release-commanded prerequisite before daemon enablement. |

## R7 Exit Criteria

- One command can generate a complete evidence vault from explicit package ids and QE run ids.
- The generated bundle passes the governance planner with `blocked_packages={}`.
- Executor preview returns `status=passed`, `db_connection_opened=false`, and `db_writes_executed=false`.
- Missing artifacts produce explicit blockers and SOP, not synthetic evidence.
- Release commander has a deterministic checklist for production apply, separate from the generator.
