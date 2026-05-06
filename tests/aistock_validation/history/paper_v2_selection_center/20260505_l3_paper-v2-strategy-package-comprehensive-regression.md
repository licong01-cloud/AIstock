# Paper v2 / StrategyPackage Comprehensive Regression

Date: 2026-05-05

## Scope

- Trigger: verify whether issues similar to the V25 model-path failure still
  exist before reporting the StrategyPackage/Paper v2 flow as usable.
- Production port `8001` was not restarted or used for service validation.
- Temporary ports:
  - backend: `8012`
  - frontend: `3012`
  - TDX probe: existing `19080`

## Pipeline Results

```powershell
conda run -n AIstock python -m nox -s l0
```

Result: PASS. Skill metadata and guardrails passed. Existing non-blocking
findings remained:

- `RAW_JSON_UI` medium review findings in validation/Paper v2 tests.
- historical baseline findings from `tmp/validation/guardrails/baseline_20260504.json`.

```powershell
conda run -n AIstock python -m nox -s paper_v2_backend
```

Result: PASS, `128 passed in 9.24s`.

```powershell
conda run -n AIstock python -m nox -s paper_v2_data_quality
```

Result: FAIL. Read-only smoke reported stale datasets:

- `kline_daily_raw`: latest success `2026-04-28`, minimum required `2026-04-29`
- `stock_moneyflow_ts`: latest success `2026-04-28`, minimum required `2026-04-29`
- `sector_data`: latest success `2026-04-28`, minimum required `2026-04-29`
- `index_daily`: latest success `2026-04-28`, minimum required `2026-04-29`

Other data checks passed: required schemas/tables, trading calendar, package
catalog, selection result traceability, and Paper v2 run traceability.

```powershell
python scripts/aistock_validate.py services --backend-port 8012 --tdx-port 19080 --timeout 10
```

Result: PASS. Temporary backend `8012` and TDX `19080` responded with HTTP 200.

```powershell
conda run -n AIstock python -m nox -s paper_v2_ui
```

Environment:

```text
BACKEND_PORT=8012
FRONTEND_PORT=3012
PAPER_V2_SKIP_REALTIME=1
PAPER_V2_E2E_SKIP_REALTIME=1
```

Result: FAIL. `5 passed`, `1 failed`, `6 did not run`.

Failed test:

```text
Selection Center validates weighted fusion, HMM, blacklist backfill, and TopK guard through UI
```

Failure evidence:

- Screenshot:
  `tmp/playwright-results/tests-paper-v2-paper-v2-re-752c5-l-and-TopK-guard-through-UI-chromium/test-failed-1.png`
- Trace:
  `tmp/playwright-results/tests-paper-v2-paper-v2-re-752c5-l-and-TopK-guard-through-UI-chromium/trace.zip`
- Error context:
  `tmp/playwright-results/tests-paper-v2-paper-v2-re-752c5-l-and-TopK-guard-through-UI-chromium/error-context.md`

## Real API Smoke

Temporary backend `8012`, read-only HTTP checks:

```text
strategy_packages_list PASS {"status": 200, "ok": true, "packages": 3}
qe_sources_list PASS {"status": 200, "ok": true, "sources": 20}
v25_qe_paper_readiness PASS {"status": 200, "ok": true, "algo": "V25_TWO_STAGE", "early_path_cached": true, "late_path_cached": true, "cache_status": {"early_model_path": "copied", "late_model_path": "copied"}}
selection_selectable_packages PASS {"status": 200, "ok": true, "packages": 3}
paper_v2_defaults PASS {"status": 200, "ok": true}
paper_v2_running_summary PASS {"status": 200, "ok": true, "items": 0}
```

## Similar Runtime-Asset Problems Found

The V25 model path issue is fixed, but Selection Center still exposes packages
whose QE runtime assets cannot be materialized through the node API.

Runtime asset audit of the three selectable packages:

```text
FAIL pkg_99142cb1440c40a7824e83902f4e7da9 qe_20260416_082012
  DATA_UNAVAILABLE: download_mlruns_params 404 for qe_20260416_082012/Loop1

PASS pkg_006a42323f7c4e81a468fdaad2cb16a3 qe_20260413_084216
  factors=52, model params.pkl materialized

FAIL pkg_b668f8a633c44b72a5d557a2cb8970e3 qe_20260416_002701
  DATA_UNAVAILABLE: download_mlruns_params 404 for qe_20260416_002701/Loop1
```

The failed UI E2E created a failed selection run:

```text
run_id=sel_3630ce405e2542afb2d225a86b7a965e
status=FAILED
trade_date=2026-04-27
error_code=DATA_UNAVAILABLE
message=failed to materialize QE runtime assets through the node API
node API URL=http://127.0.0.1:9000/api/v1/qe_workspace/tasks/qe_20260416_002701/loops/Loop1/mlruns-params
HTTP=404
```

## Business Conclusion

- The specific `V25_TWO_STAGE early_model_path` StrategyPackage readiness error
  is fixed on the temporary backend.
- The broader Paper v2/Selection Center flow is not release-clean yet:
  - market data refresh audit is stale for four required datasets;
  - two selectable StrategyPackages cannot generate authoritative selection
    runtime assets because QE node API lacks `mlruns-params`;
  - UI E2E fails when a selectable package later fails runtime materialization.
- Production `8001` remained untouched.
