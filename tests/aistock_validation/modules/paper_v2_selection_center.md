# Paper v2 + Selection Center First-Stage Validation Matrix

Selection Center is treated as part of the Paper Trading v2 first-stage validation
slice because Paper v2 depends on package-based selection output and the UI flows are
strongly coupled.

## L0 Guardrails

- No hardcoded local paths, WSL UNC paths, distro names, secrets, or production ports.
- No silent fallback or fake success for selection, market data, execution, ledger, or UI.
- No raw JSON display to operators.
- No protected asset writes during framework validation.

## Backend L1/L2

- StrategyPackage repository/service tests needed by Paper v2 package selection.
- Selection Center runtime/API tests:
  - single-package selection;
  - HMM disabled/enabled readiness;
  - suspension and industry blacklist filtering with backfill;
  - multi-package aggregation paths that are UI-supported;
  - fail-fast for missing score/model/HMM/industry/suspend artifacts.
- Paper v2 tests:
  - portfolio lifecycle;
  - replay policy and reset confirmation;
  - runtime profile validation;
  - market data readiness;
  - V25 day_features readiness;
  - order/fill/cash/position/snapshot persistence;
  - performance report from snapshots only.

## API L2

- List selectable StrategyPackages and metrics.
- Run or load Selection Center result for one package.
- Create Paper v2 portfolio from a valid package/selection context.
- Check readiness without persisting a run.
- Execute DB historical replay where data exists.
- Query runs, run events, orders, fills, positions, cash, snapshots, performance, and errors.
- Verify reset rejects wrong confirmation and records audit on correct confirmation.

## UI L3

- Navigate to Paper v2 and Selection Center pages.
- Select package, runtime profile, topK, HMM, industry blacklist, suspend filter, and execution policy options.
- Execute selection and open existing selection records.
- Add selected symbols to watchlist when UI supports the operation.
- Create or inspect Paper v2 portfolio.
- Run historical replay where backend data is available.
- Inspect running portfolio list and details: funds, holdings, orders, fills, NAV, errors, config audit.
- Refresh/reopen pages and verify persisted state.
- Fail test on pageerror, console error, requestfailed, unexpected 4xx/5xx, or raw JSON operator display.

## Business Oracles

- Selection must be authoritative live/latest inference or a declared live artifact, not QE backtest pred.pkl.
- Selection result is traceable by package_id, manifest_sha256, trade_date, data_source, runtime_config_hash.
- Paper v2 uses minute data only; no daily fallback.
- Ledger state changes only from fills.
- NAV/performance is computed from persisted snapshots and explains insufficient data.
- Missing calendar, pre_close, limit, suspend, minute bars, day_features, HMM coefficient, or strategy output fails fast with context.

## First-Stage Command Targets

```bash
conda run -n AIstock python -m nox -s l0
conda run -n AIstock python -m nox -s paper_v2_backend
conda run -n AIstock python -m nox -s paper_v2_ui -- 8012 3012
conda run -n AIstock python -m nox -s paper_v2_l3
```
