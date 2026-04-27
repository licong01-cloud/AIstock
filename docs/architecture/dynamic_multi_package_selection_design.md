# Dynamic Multi-Package Selection Design

Status: implementation design  
Date: 2026-04-26  
Scope: StrategyPackage Center, Selection Center, Paper Trading v2 discovery UI  
Out of scope: multi-package Paper v2 execution, QMT, Shadow, live trading, V25 adapter

## 1. Decision Summary

Multi-package selection is an exploratory selection-layer feature. It should not
freeze a new trading source and should not enter Paper Trading v2 execution in
this phase.

The intended workflow is:

```text
StrategyPackage A single-package selection run
StrategyPackage B single-package selection run
StrategyPackage C single-package selection run
        -> dynamic UI picks packages or existing runs
        -> intersection / union / weighted_fusion
        -> persisted aggregate selection.run
        -> analyst review and export
```

Direct conversion to a Paper v2 portfolio remains disabled until a future
Combined StrategyPackage or SelectionBundle contract exists and has backtest
validation.

## 2. Source-Backed Current Findings

- `backend/services/selection_center/service.py` already supports
  `single_package`, `intersection`, `union`, and `weighted_fusion`.
- `weighted_fusion` uses rank-normalized package scores and stores package
  ranks, raw scores, normalized rank scores, weights, and normalized weights in
  `SelectionCandidate.component_scores`.
- `backend/services/selection_center/repository.py` persists:
  - `selection.run.package_ids`
  - `selection.package_result`
  - `selection.aggregate_result`
  - `selection.excluded_result`
  - `selection.paper_portfolio_link`
- `backend/routers/selection_center.py` exposes run/list/get/aggregate-result
  endpoints and rejects multi-package selection-to-paper with
  `UnsupportedFeatureError`.
- `StrategyPackageManifest.backtest_summary` stores structured `ic`, `rank_ic`,
  `icir`, `annual_return`, `max_drawdown`, `final_nav`, `n_trading_days`, and
  `raw_metrics`.
- `StrategyPackageManifest.alpha_components[].metrics_snapshot` stores
  component-level metrics such as `ic`, `rank_ic`, `annual_return`,
  `max_drawdown`, and `turnover`.
- Sharpe is not currently a first-class manifest field. If QE emitted Sharpe,
  it can be read from `backtest_summary.raw_metrics`.

## 3. Invariants

- Do not mutate frozen StrategyPackage manifests to add display metrics.
- Do not change `manifest_sha256` for display-only fields.
- Do not re-enter old RD-Agent or old paper trading selection paths.
- Do not convert a multi-package aggregate selection into a Paper v2 portfolio.
- Do not return empty aggregate results as success unless
  `valid_no_candidate` is explicitly modeled with a reason.
- Aggregating existing runs must require:
  - all source runs are `SUCCEEDED`;
  - all source runs are `single_package`;
  - all source runs have the same `trade_date`;
  - all source runs have the same `data_source`;
  - source packages are unique;
  - each source run has aggregate rows and a package manifest hash.

## 4. Metrics Summary

Display metrics are derived at API/service time from the frozen manifest and do
not become part of the manifest hash.

Minimum response shape:

```json
{
  "package_id": "pkg_x",
  "manifest_sha256": "sha...",
  "ic": 0.05,
  "rank_ic": 0.04,
  "icir": 1.2,
  "sharpe": 1.35,
  "annual_return": 0.18,
  "max_drawdown": -0.12,
  "final_nav": 1.2,
  "turnover": 0.35,
  "n_trading_days": 252,
  "sample_start": "2024-01-01",
  "sample_end": "2025-12-31",
  "missing_metrics": ["sharpe"]
}
```

Sharpe aliases should include:

- `sharpe`
- `Sharpe`
- `sharpe_ratio`
- `Sharpe Ratio`
- `annualized_sharpe`

Annual return and max drawdown remain decimals when manifest values are
decimals. The UI formats them as percentages.

## 5. Selectable Packages API

Selection Center should expose a package discovery endpoint for the dynamic
multi-package UI:

```text
GET /api/v1/selection-center/selectable-packages
```

Response includes:

- package id/name/version/status/source;
- manifest hash;
- alpha mode and alpha count;
- portfolio top-k;
- metrics summary;
- model staleness state;
- latest selection run summary when available.

Default eligible package statuses:

- `BACKTEST_APPROVED`
- `SELECTION_ENABLED`
- `PAPER_ENABLED`

## 6. Aggregate Existing Runs API

The UI can either run selected StrategyPackages directly or aggregate existing
single-package runs. Existing-run aggregation is useful when every strategy has
already completed selection for a shared trade date and data source.

```text
POST /api/v1/selection-center/aggregate-runs
```

Request:

```json
{
  "source_run_ids": ["sel_a", "sel_b"],
  "mode": "weighted_fusion",
  "runtime_config": {
    "package_weights": {
      "pkg_a": 0.6,
      "pkg_b": 0.4
    }
  }
}
```

The created aggregate run:

- uses the same `trade_date` and `data_source` as the source runs;
- stores `runtime_config.source_run_ids`;
- stores package-level rows copied from each source run's aggregate rows;
- stores aggregate rows produced by the same aggregation engine used for direct
  package runs;
- adds source-run trace metadata to aggregate explanations.

## 7. Frontend Plan

Add a new Paper Trading v2 discovery page:

```text
frontend/src/app/paper-trading/package-selection/page.tsx
```

The page should:

- load selectable packages;
- display metrics cards/table for IC, Rank IC, ICIR, Sharpe, annual return, and
  max drawdown;
- allow dynamic package selection;
- support `intersection`, `union`, and `weighted_fusion`;
- allow weight editing when `weighted_fusion` is selected;
- run direct multi-package selection through `/selection-center/runs`;
- show backend fail-fast errors verbatim;
- show aggregate results with source package trace;
- not offer a "create Paper portfolio" action for multi-package runs.

The page may use a test backend by setting:

```text
NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1
```

## 8. Verification Plan

Backend:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/strategy_package backend/tests/selection_center -q -p no:cacheprovider
pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/test_tushare_sync_engine.py -q -p no:cacheprovider
```

Temporary backend validation:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
```

Then verify:

```text
GET /docs
GET /openapi.json
GET /api/v1/selection-center/selectable-packages
```

Frontend:

```powershell
cd frontend
npm run lint
```

If the repository lint config is incomplete or unrelated legacy pages fail,
run TypeScript checks or a targeted compile-quality check and document the
limitation.

## 9. Backend Trace Update - 2026-04-26

The dynamic multi-package backend now also exposes:

```text
GET /api/v1/selection-center/runs/{run_id}/excluded-results
```

This returns the persisted `selection.excluded_result` rows grouped by
`package_id`, including suspended and industry-blacklist exclusions. The dynamic
selection UI can use this endpoint to explain why a top-ranked raw signal was
not present in the final aggregate result. Multi-package Paper v2 execution
remains disabled; this trace is for selection review only.
