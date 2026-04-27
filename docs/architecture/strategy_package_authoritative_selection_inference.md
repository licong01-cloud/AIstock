# StrategyPackage Authoritative Selection Inference

Date: 2026-04-26  
Status: implementation note / Paper v2 selection baseline

## Background

When a `StrategyPackage` enters Selection Center or Paper Trading v2, AIstock must not treat QE backtest `pred.pkl` files as current tradable selection signals.

`pred.pkl` only represents a historical test segment from a QE backtest. It does not prove that the package can recompute factors from the latest DB data, load the saved model, and rank the current universe at the requested `trade_date`.

## Rules

- Authoritative selection must start from the StrategyPackage QE lineage, factor definitions, model artifact, and feature order.
- Each authoritative selection artifact must recompute factors from DB-backed data for the requested `trade_date` / cutoff date.
- Each artifact must persist `package_id`, `manifest_sha256`, `trade_date`, `data_source`, score-production runtime hash, model path/hash, factor order, inference backend, row count, and reference price status.
- QE `pred.pkl` may only create `diagnostic_backtest_only` artifacts; Selection Center and Paper v2 must reject those artifacts by default.
- `runtime_config.selection_scores`, manifest embedded `strategy_config.selection_runtime.scores`, and `scores_path` are not authoritative signal inputs and must fail fast.
- Missing model, missing factor code, missing data, mismatched inference date, empty/non-finite scores, feature shape mismatch, feature padding, zero fill, or stale date fallback must fail fast.

## Current Implementation

### Authoritative endpoint

```text
POST /api/v1/strategy-packages/{package_id}/selection-artifacts/generate
```

Behavior:

- Generates `source_type=live_qe_model_inference_v1` and `authority_scope=authoritative_selection` artifacts.
- Uses a temporary StrategyPackage inference workspace containing `manifest.json`, `factor_order.json`, `strategy_package_factor_entry.py`, and copied model parameters.
- Runs strict inference with `AISTOCK_STRICT_INFERENCE=1`.
- Rejects feature-count mismatch, missing feature padding, factor-date fallback, insufficient data windows, and missing required DB datasets.
- Persists reference prices for selected rows when requested; missing selected reference price fails instead of using a default price.

### Diagnostic endpoint

```text
POST /api/v1/strategy-packages/{package_id}/selection-artifacts/generate-diagnostic-backtest
```

Behavior:

- Explicitly reads QE backtest `pred.pkl` for diagnostics only.
- Persists `authority_scope=diagnostic_backtest_only`.
- Authoritative runtime rejects this artifact unless a future diagnostic-only caller explicitly opts into non-trading analysis.

## Data Window

Authoritative inference resolves the factor rolling window from factor definitions. For example, a `250d` factor needs roughly 260 trading days of input history.

The WSL runner uses the DB-backed QE static factor builder so it can load full windows from local tables such as `daily_basic`, `moneyflow`, and `sector_data` without changing global `backend/data_service` semantics.

## Verified Baseline

On 2026-04-26, DB_HISTORICAL live-inference artifacts and single-package Selection Center runs were verified for 2026-04-24 for:

- `qe_20260416_002701` / `pkg_b668f8a633c44b72a5d557a2cb8970e3`
- `qe_20260413_084216` / `pkg_006a42323f7c4e81a468fdaad2cb16a3`
- `qe_20260416_082012` / `pkg_99142cb1440c40a7824e83902f4e7da9`

## Remaining Gaps

- `TDX_REALTIME` intraday factor inference is not yet the authoritative selection data source; current authoritative selection artifacts use DB_HISTORICAL latest ingested daily/factor data.
- Multi-alpha StrategyPackage component runtime is not implemented yet; current multi-strategy support is multi-package aggregation, not one multi-alpha package runtime.
- Paper v2 execution still needs separate readiness for minute bars, limit prices, suspend status, `pre_close`, validated execution policy, execution algo dependencies, and ledger persistence.
