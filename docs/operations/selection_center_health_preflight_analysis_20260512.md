# Selection Center Health Preflight Failure Analysis - 2026-05-12

## Verdict

Likely root cause is an intentional fail-fast gate, not a cleanup-branch
regression. The Selection Center/Paper v2 UI now sends
`st_pit_authoritative=true`; the backend health preflight rejects legacy
StrategyPackages whose frozen QE backtest contract did not enable the ST PIT
risk policy. Historical repo docs record the currently selectable packages as
legacy/non-ST-PIT packages, so a direct `POST /api/v1/selection-center/runs`
or a stale UI that bypasses the disabled package guard is expected to fail with
`strategy package is blocked by Selection Center health preflight`.

This blocks using the existing legacy packages as successful ST PIT
authoritative Selection/Paper v2 cutover evidence. It does not by itself block
the documentation/cleanup branch, provided cleanup does not claim a green
Selection/Paper v2 cutover and does not delete the health-gate behavior. The
safe cutover path is to create/rebuild a genuinely ST PIT StrategyPackage, or
to define an explicit legacy-only mode that does not claim ST PIT authority.

## Scope And Boundaries

- Worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`.
- Branch: `codex/qe-cleanup-and-pr005-prep-20260512`.
- Branch HEAD inspected: `7eaee9f`.
- `origin/main` reference inspected: `da64806`.
- Owned output: `docs/operations/selection_center_health_preflight_analysis_20260512.md`.
- Not touched: `main`, services, ports, production/dev DB, Paper daemon, live broker, Claude worktrees, commits, pushes.
- Pre-existing unrelated untracked docs left untouched:
  `docs/baseline/stage6_baseline_post_cleanup_20260512.md`,
  `docs/cross_tool/20260512_codex_verify_synthetic_rollback.md`,
  `docs/handoff/archive_branches_register_20260512.md`.

## Exact Failing Path

### Normal UI path

1. `frontend/src/app/paper-v2/selection/page.tsx` loads packages from
   `GET /api/v1/selection-center/selectable-packages`.
2. The page reads each row's `selection_health` and treats the package as
   selectable only when `selection_health.runnable === true`.
3. `runtimeConfig()` always sends:
   - `st_pit_authoritative: true`
   - `display_top_n`
   - `selection_artifact_config.auto_generate: true`
   - `runtime_profile` with selection/tradability/HMM settings.
4. The UI blocks selected non-runnable packages before the API call. If an old
   frontend/backend process omits `selection_health`, or an operator/API client
   bypasses the UI guard, the backend rejects the same request.

### Backend path

1. `POST /api/v1/selection-center/runs`
   enters `backend/routers/selection_center.py::run_selection`.
2. `SelectionCenterService.run_packages()` normalizes the top-level runtime
   config and calls `_prepare_package_runtime_configs()`.
3. `_prepare_package_runtime_configs()` loads each StrategyPackage, builds the
   effective per-package runtime config, and calls
   `SelectionPackageHealthService.require_runnable()`.
4. In ST PIT authoritative mode, `require_runnable()` raises
   `StrategyPackageValidationError` when `health["runnable"]` is false.
5. The router maps that validation error to HTTP 400 with structured
   `detail` containing at least `package_id`, `health_status`, and `checks`.

The inferable failing API shape is:

```http
POST /api/v1/selection-center/runs
```

```json
{
  "package_ids": ["<legacy package id>"],
  "trade_date": "2026-05-12",
  "data_source": "DB_HISTORICAL",
  "mode": "single_package",
  "runtime_config": {
    "st_pit_authoritative": true,
    "display_top_n": 20,
    "selection_artifact_config": {
      "auto_generate": true,
      "inference_backend": "wsl"
    },
    "runtime_profile": {
      "selection": {"top_k": 20},
      "tradability": {"exclude_suspended": true}
    }
  }
}
```

Expected failure payload includes:

```text
strategy package is blocked by Selection Center health preflight
```

with a blocked check such as:

```text
st_pit_contract_required
ST PIT authoritative selection requires a StrategyPackage created from a ST PIT QE backtest
```

## Evidence From Code

| Area | Evidence | Impact |
|---|---|---|
| UI request contract | `frontend/src/app/paper-v2/selection/page.tsx:191` builds `runtimeConfig()` with `st_pit_authoritative=true` and `selection_artifact_config.auto_generate=true`. | Selection Center runs from Paper v2 UI are ST PIT authoritative by default. |
| UI package gate | `frontend/src/app/paper-v2/selection/page.tsx:42`, `frontend/src/app/paper-v2/selection/page.tsx:246`, and `frontend/src/app/paper-v2/selection/page.tsx:386` require `selection_health.runnable === true`. | A current UI should prevent the click before the backend call. |
| API route | `backend/routers/selection_center.py:63` calls `SelectionCenterService.run_packages()`, and `_raise_http()` maps validation errors to HTTP 400. | Backend should return a structured 400, not a silent success. |
| Package preparation | `backend/services/selection_center/service.py:117` and `backend/services/selection_center/service.py:245` prepare per-package runtime configs before any Selection run executes. | Health gate runs before signal generation. |
| ST PIT contract merge | `backend/services/selection_center/service.py:285` uses `normalize_runtime_config_with_backtest_contract(...)` when `st_pit_authoritative` is true. | Engine runtime inherits the frozen QE contract instead of UI overrides. |
| Health gate | `backend/services/selection_center/package_health.py:53` checks the frozen contract; `backend/services/selection_center/package_health.py:76` adds `st_pit_contract_required` for legacy packages in ST PIT mode; `backend/services/selection_center/package_health.py:104` raises the health-preflight validation error. | Legacy/non-ST-PIT packages are intentionally blocked. |
| Artifact/live inference preflight | `backend/services/selection_center/service.py:422` runs `_require_live_inference_preflight()` before heavy live artifact generation; `backend/services/strategy_package/live_inference.py:517` defines the five checks. | After health passes, cold-start failures still fail fast on source/node/conf/factors/model params. |
| HMM preflight | `backend/services/selection_center/package_health.py:307` checks HMM coefficient artifacts when HMM is enabled. | HMM-on selection can separately block on missing stock-sector mapping. |

## Evidence From Existing Docs

- `docs/analysis/selection_paper_st_pit_alignment_and_blockers_20260506.md`
  records four selectable packages from 2026-05-06 as legacy from the ST PIT
  risk-contract perspective:
  - `pkg_1de32357724a4c5b874f2abd90f22da5`
  - `pkg_99142cb1440c40a7824e83902f4e7da9`
  - `pkg_006a42323f7c4e81a468fdaad2cb16a3`
  - `pkg_b668f8a633c44b72a5d557a2cb8970e3`
- The same doc says those packages should remain explicitly marked legacy or
  be recreated from new ST PIT QE backtests; frozen manifests should not be
  mutated to add missing ST PIT policy.
- It also records package-specific follow-on blockers:
  - `pkg_1de...`: strict live inference kept `0 / 4636` fully scored rows.
  - `pkg_006...`: model expected 63 features but live runtime prepared 52.
  - `pkg_991...` and `pkg_b668...`: current local cache could run, but cold
    cache materialization failed when node `mlruns-params` returned 404.
  - HMM-enabled selection had historical missing stock-sector-map failures.
- `docs/analysis/paper_v2_architecture_flow_and_confirmed_defects_20260507.md`
  repeats that the current selectable packages are legacy/non-ST-PIT and notes
  that an old test backend lacking `selection_health` cannot be used as final
  validation evidence.
- `tests/aistock_validation/history/paper_v2_selection_center/20260507_012146_l3_paper-v2-selection-center-l3-regression.md`
  states the business goal explicitly: fail before live Selection/Paper v2
  execution when ST PIT/HMM/runtime artifacts are not authoritative.

## Tested Locally Under Safe Constraints

Executed only isolated unit tests with bytecode and pytest cache disabled; no
services, ports, DB, nox, npm, Playwright, Paper daemon, or broker path was
started.

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m pytest `
  backend/tests/selection_center/test_runtime_selection.py::test_selection_center_authoritative_mode_blocks_legacy_non_st_pit_package `
  backend/tests/selection_center/test_runtime_selection.py::test_selection_center_health_blocks_hmm_missing_stock_sector_map_before_inference `
  backend/tests/selection_center/test_runtime_selection.py::test_selection_center_health_passes_hmm_artifact_preflight `
  backend/tests/strategy_package/test_live_inference_preflight.py::test_preflight_blocks_when_conf_yaml_missing `
  backend/tests/strategy_package/test_live_inference_preflight.py::test_preflight_blocks_when_model_params_missing `
  -q -p no:cacheprovider
```

Result:

```text
5 passed in 1.61s
```

These tests confirm:

- ST PIT authoritative mode blocks a legacy non-ST-PIT package with the health
  preflight error.
- HMM health preflight blocks missing stock-sector mapping before inference.
- HMM health preflight passes when coefficient artifacts include sector map
  coverage.
- Live-inference cold-start preflight blocks missing `conf.yaml`.
- Live-inference cold-start preflight blocks missing `params.pkl`.

## Root Cause Classification

| Candidate root cause | Likelihood | Evidence | Cutover impact |
|---|---:|---|---|
| Legacy/non-ST-PIT package selected in ST PIT authoritative mode | High | Current docs list available packages as legacy; code explicitly blocks them via `st_pit_contract_required`. | Blocks ST PIT authoritative Selection/Paper v2 success until new ST PIT packages exist. |
| HMM coefficient artifact missing stock-sector mapping | Medium when HMM enabled; low when HMM disabled | Health service calls `hmm_runtime.preflight_coefficients()` and test covers missing `stock_sector_map`. | Blocks only HMM-on selection. |
| Live inference cold-start asset missing (`qe_source`, node, `conf.yaml`, factors, `params.pkl`) | Medium after health passes or when auto-generate is enabled | Preflight checks are wired before artifact generation; historical docs record node 404/model schema/strict-score failures. | Blocks individual packages; good fail-fast behavior. |
| Stale backend/frontend process without `selection_health` | Medium operational caveat, not proven in this task | Historical docs observed old port `8011` missing `selection_health`; current code returns it. | Can mislead UI verification; restart non-prod/prod service only when explicitly authorized. |
| Cleanup branch code regression | Low | Focused unit tests pass; inspected path matches previously documented behavior. | No direct cleanup blocker found. |

## Safe Next Steps

1. Do not patch legacy frozen StrategyPackage manifests to add ST PIT policy.
2. For cutover evidence, build or identify a StrategyPackage created from a QE
   run whose frozen manifest already contains ST PIT `risk_policy.enabled=true`
   with provider `st_pit` and hard actions `block_buy` + `force_exit`.
3. Run `GET /api/v1/selection-center/selectable-packages` only on an authorized
   non-production dev backend and confirm rows include `selection_health`.
4. For the chosen package, inspect `selection_health.checks` before attempting
   `POST /selection-center/runs`; do not use UI success as the only signal.
5. If HMM is enabled, verify coefficient artifacts cover the trade date and
   include `stock_sector_map` before selection.
6. If `selection_artifact_config.auto_generate=true`, verify the live-inference
   cold-start prerequisites: source resolves, `execution_node_id`, `conf.yaml`,
   `factors/*.py`, and `mlruns/**/artifacts/params.pkl`.
7. Treat this as a cutover-data/package-readiness blocker, not a reason to
   weaken the health gate.

## Commands Run

```powershell
git status --short --branch
git rev-parse --short HEAD; git rev-parse --short origin/main; git branch --show-current
rg -n "Selection Center|selection center|SelectionCenter|selection_center|selection-center|health preflight|health_preflight|preflight" -S --glob '!*.pyc' --glob '!node_modules/**' --glob '!*.pkl' --glob '!*.parquet'
rg -n "health preflight|Selection Center health|selection_health|selectable-packages|st_pit_authoritative|legacy/non-ST-PIT|legacy package|HMM artifact health|preflight failed|health gate|blocked by Selection Center" docs tests backend -S --glob '*.md' --glob '*.log' --glob '*.txt' --glob '*.py'
rg -n "st_pit_authoritative|selection_health|selectable-packages|health preflight|LEGACY_NON_ST_PIT|blocked" frontend backend/tests/paper_trading_v2 frontend/tests -S --glob '!node_modules/**'
$env:PYTHONDONTWRITEBYTECODE='1'; C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_center_authoritative_mode_blocks_legacy_non_st_pit_package backend/tests/selection_center/test_runtime_selection.py::test_selection_center_health_blocks_hmm_missing_stock_sector_map_before_inference backend/tests/selection_center/test_runtime_selection.py::test_selection_center_health_passes_hmm_artifact_preflight backend/tests/strategy_package/test_live_inference_preflight.py::test_preflight_blocks_when_conf_yaml_missing backend/tests/strategy_package/test_live_inference_preflight.py::test_preflight_blocks_when_model_params_missing -q -p no:cacheprovider
```

## Not Run

- No backend/frontend service start or restart.
- No production backend `8001` or frontend `3000` access.
- No dev/prod DB queries or writes.
- No Paper daemon, broker, live broker, miniQMT, or TDX access.
- No nox, npm, Playwright, or live API smoke.
- No commits or pushes.

## Final Recommendation

Mark the Selection Center health preflight failure as an expected ST PIT
package-readiness/cutover blocker. Keep the health gate. Cleanup can continue
only if release notes remain explicit that legacy packages are not valid
ST PIT authoritative cutover evidence and that a new/rebuilt ST PIT package is
required before green Selection/Paper v2 cutover.
