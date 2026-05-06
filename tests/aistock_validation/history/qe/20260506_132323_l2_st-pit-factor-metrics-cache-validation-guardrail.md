# AIstock Guardrail Baseline Scan

- Generated at: 2026-05-06T05:34:18.646195+00:00
- Mode: `changed_only`
- Files scanned: 18
- Total findings: 107

## Summary By Baseline Status

| Status | Count |
|---|---:|
| `new` | 107 |

## Summary By Severity

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 6 |
| P2 | 101 |
| P3 | 0 |

## Summary By Rule

| Rule | Count |
|---|---:|
| `ALGO-COMPLEXITY-001` | 100 |
| `DOC-LOCATION-001` | 3 |
| `RESOURCE-TIMEOUT-001` | 1 |
| `ROOT-POLLUTION-001` | 3 |

## Interpretation

This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.
New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.
Historical findings should be triaged by module and burned down with regression tests.

## First 107 Findings

| Severity | Status | Rule | File | Line | Remediation |
|---|---|---|---|---:|---|
| P2 | `new` | `RESOURCE-TIMEOUT-001` | `backend/routers/quantevolver.py` | 3807 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 99 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 131 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 144 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 145 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 448 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 496 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 498 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 1039 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 1048 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 1051 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 1063 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 1081 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 1518 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 2070 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 2264 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 2437 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 2857 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 3035 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 3054 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 3500 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 3728 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 3869 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 4297 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 4311 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 4327 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 4347 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 5011 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 5263 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 5271 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 5394 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 5812 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 5853 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 6156 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 6221 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 7164 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 7197 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/routers/quantevolver.py` | 7218 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/correlation_compute_service.py` | 388 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/correlation_compute_service.py` | 483 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/correlation_engine.py` | 43 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/correlation_engine.py` | 376 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/correlation_engine.py` | 482 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/correlation_engine.py` | 492 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 33 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 35 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 79 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 82 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 85 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 88 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 91 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 389 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 393 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/data_snapshot_manager.py` | 403 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 460 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 467 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 766 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 781 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 807 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_official_evaluation_service.py` | 823 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 30 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 36 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 75 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 93 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 270 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 322 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 361 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 391 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 399 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 419 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 539 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 630 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 770 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_loader.py` | 789 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 39 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 41 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 42 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 161 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 391 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 395 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 418 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 545 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 582 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 719 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 746 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 998 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1014 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1019 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1116 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1135 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1165 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1181 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1251 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1335 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `backend/services/quantevolver/factor_value_pipeline.py` | 1347 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P1 | `new` | `ROOT-POLLUTION-001` | `findings.md` | 1 | Move one-off scripts to debug_tools, analysis docs to docs/analysis, design docs to docs/architecture, or reusable tools to scripts. |
| P1 | `new` | `DOC-LOCATION-001` | `findings.md` | 1 | Move standards to docs/standards, designs to docs/architecture, analysis/research to docs/analysis, operations to docs/operations, user guides to docs/user_guides, and releases to docs/releases. |
| P1 | `new` | `ROOT-POLLUTION-001` | `progress.md` | 1 | Move one-off scripts to debug_tools, analysis docs to docs/analysis, design docs to docs/architecture, or reusable tools to scripts. |
| P1 | `new` | `DOC-LOCATION-001` | `progress.md` | 1 | Move standards to docs/standards, designs to docs/architecture, analysis/research to docs/analysis, operations to docs/operations, user guides to docs/user_guides, and releases to docs/releases. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `scripts/backfill_factor_cache.py` | 325 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `scripts/backfill_factor_cache.py` | 332 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `scripts/backfill_factor_cache.py` | 333 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `scripts/backfill_factor_cache.py` | 508 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `scripts/backfill_factor_cache.py` | 520 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `new` | `ALGO-COMPLEXITY-001` | `scripts/backfill_factor_cache.py` | 539 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P1 | `new` | `ROOT-POLLUTION-001` | `task_plan.md` | 1 | Move one-off scripts to debug_tools, analysis docs to docs/analysis, design docs to docs/architecture, or reusable tools to scripts. |
| P1 | `new` | `DOC-LOCATION-001` | `task_plan.md` | 1 | Move standards to docs/standards, designs to docs/architecture, analysis/research to docs/analysis, operations to docs/operations, user guides to docs/user_guides, and releases to docs/releases. |
