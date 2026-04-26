# Paper Trading v2 UI Detailed Design

Date: 2026-04-26
Status: implemented and route/API verified
Scope: new frontend UI for `StrategyPackage -> Selection Center -> Paper Trading v2`
Out of scope: QMT, Shadow, live trading, V25 adapter, legacy `/paper-trading` refactor

## 1. Decision Summary

Paper Trading v2 needs a new standalone UI under `/paper-v2`.

The existing `/paper-trading/*` pages mostly call legacy `/api/v1/paper-trading/*`
APIs. Reusing them directly would mix legacy paper-trading concepts with the new
StrategyPackage-based Paper v2 flow and could hide backend fail-fast errors. The
new UI therefore uses a separate route tree, a separate API client, and explicit
workflow pages.

Canonical user flow:

```text
StrategyPackage
  -> Selection Center
  -> Paper v2 Portfolio
  -> Readiness
  -> Run Day / Historical Replay
  -> Orders / Fills / Positions / Cash / Snapshots
  -> Performance / Errors / Model & HMM maintenance
```

## 2. UI Route Map

```text
/paper-v2
/paper-v2/packages
/paper-v2/selection
/paper-v2/portfolios
/paper-v2/portfolios/[portfolioId]
/paper-v2/portfolios/[portfolioId]/run-console
/paper-v2/portfolios/[portfolioId]/ledger
/paper-v2/portfolios/[portfolioId]/performance
/paper-v2/model-hmm
/paper-v2/settings
```

Global Sidebar gets a new "Paper Trading v2" group. Legacy `/paper-trading/*`
remains available but is not the v2 authority.

## 3. Visual System

Paper v2 uses a "research trading console" visual language:

- warm off-white workspace background;
- charcoal/navy text and navigation;
- teal as primary action color;
- amber for stale/warning;
- red for blocking errors;
- green for ready/success;
- dense but readable cards, tables, badges, and timeline panels.

The UI must avoid generic success states. Empty data, unsupported features,
not-run-yet states, valid no-candidate, and backend failures must be visually
distinct.

## 4. Shared Frontend Modules

```text
frontend/src/lib/paper-v2/
  api.ts       # typed fetch wrappers and backend error parsing
  types.ts     # shared v2 API payload types
  format.ts    # number/date/status formatting helpers

frontend/src/components/paper-v2/
  StatusBadge.tsx
  ErrorPanel.tsx
  JsonPanel.tsx
  ConfirmAction.tsx
  MetricCard.tsx
  PaperTable.tsx
  SectionCard.tsx
```

All pages call backend through `frontend/src/lib/paper-v2/api.ts`; raw `fetch`
should not be scattered through page components.

## 5. API Mapping

### 5.1 Strategy Package

```text
GET  /api/v1/strategy-packages
GET  /api/v1/strategy-packages/{package_id}
GET  /api/v1/strategy-packages/{package_id}/metrics-summary
GET  /api/v1/strategy-packages/{package_id}/status-events
GET  /api/v1/strategy-packages/{package_id}/execution-policies
POST /api/v1/strategy-packages/{package_id}/enable-selection
POST /api/v1/strategy-packages/{package_id}/enable-paper
POST /api/v1/strategy-packages/{package_id}/retire
GET  /api/v1/strategy-packages/{package_id}/model-state
POST /api/v1/strategy-packages/{package_id}/model-retrain/preview
POST /api/v1/strategy-packages/{package_id}/model-retrain/start
GET  /api/v1/strategy-packages/{package_id}/model-retrain/jobs
```

### 5.2 Selection Center

```text
GET  /api/v1/selection-center/selectable-packages
POST /api/v1/selection-center/runs
GET  /api/v1/selection-center/runs
GET  /api/v1/selection-center/runs/{run_id}
GET  /api/v1/selection-center/runs/{run_id}/aggregate-results
GET  /api/v1/selection-center/runs/{run_id}/excluded-results
POST /api/v1/selection-center/aggregate-runs
POST /api/v1/selection-center/runs/{run_id}/create-paper-portfolio
GET  /api/v1/selection-center/runs/{run_id}/paper-portfolio-links
```

### 5.3 Paper Trading v2

```text
GET  /api/v1/paper-v2/portfolios
POST /api/v1/paper-v2/portfolios
GET  /api/v1/paper-v2/portfolios/{portfolio_id}
POST /api/v1/paper-v2/portfolios/{portfolio_id}/pause
POST /api/v1/paper-v2/portfolios/{portfolio_id}/resume
POST /api/v1/paper-v2/portfolios/{portfolio_id}/complete
POST /api/v1/paper-v2/portfolios/{portfolio_id}/retire
POST /api/v1/paper-v2/portfolios/{portfolio_id}/readiness
POST /api/v1/paper-v2/portfolios/{portfolio_id}/run-day
POST /api/v1/paper-v2/portfolios/{portfolio_id}/replay
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policies
POST /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policy-activations
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policy-activations
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/orders
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/fills
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/cash-ledger
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/positions
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/daily-snapshots
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/performance-report
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/runs
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/run-events
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/errors
```

### 5.4 HMM Training

```text
GET  /api/v1/hmm-training/configs
POST /api/v1/hmm-training/configs/{config_id}/rolling-training/preview
POST /api/v1/hmm-training/configs/{config_id}/rolling-training/trigger
GET  /api/v1/hmm-training/configs/{config_id}/jobs
GET  /api/v1/hmm-training/configs/{config_id}/snapshots
```

## 6. Page Design

### 6.1 Overview

Purpose: show portfolio health and the next required actions.

Layout:

```text
Hero summary
  metric cards: packages, selections, portfolios, today runs, blocking errors
Workflow board
  package enabled -> selection done -> portfolio ready -> readiness pass
Active portfolio table
  portfolio, status, package, model state, data source, last run, actions
Blocking errors table
  trade_date, portfolio, stage, error_code, message, link to run console
```

Primary actions:

- refresh all;
- open packages;
- open selection;
- create portfolio;
- open run console.

### 6.2 Packages

Purpose: inspect StrategyPackages, metrics, model freshness, validated execution
policies, and status transitions.

Layout:

```text
Package filters
Package table
Selected package detail drawer/card
Tabs: manifest, metrics, model state, execution policies, status events
```

Actions:

- enable selection;
- enable paper;
- retire;
- create paper portfolio;
- preview model retrain.

### 6.3 Selection Center

Purpose: run package-based selection and dynamic multi-package aggregation.

Layout:

```text
Mode controls: single_package / intersection / union / weighted_fusion
Trade date + data source + top_k
Runtime profile editor:
  industry blacklist
  HMM enabled/model_snapshot_id/signal_preset/coefficients_path
  exclude suspended
Package picker with metrics and model state
Result table
Excluded result trace table
Create Paper portfolio panel
```

Rules:

- single-package selection result can create Paper v2 portfolio;
- multi-package aggregate result shows a clear disabled paper-execution banner;
- weighted fusion requires positive package weights;
- backend errors are shown with error context JSON.

### 6.4 Portfolio Center

Purpose: create and manage Paper v2 portfolios.

Create wizard:

```text
1. Select source StrategyPackage
2. Choose initial cash, start date, data source
3. Select backtest-validated execution policy
4. Review frozen contract and create
```

Portfolio list columns:

```text
name, status, package, manifest hash, initial cash, start date, data source,
last snapshot, last run, quick actions
```

Actions:

- pause/resume/complete/retire;
- open detail;
- open run console;
- open ledger;
- open performance.

### 6.5 Portfolio Detail

Purpose: show frozen contract and operational links.

Sections:

- frozen manifest and `manifest_sha256`;
- frozen fee/risk/execution policy;
- active execution policy activations;
- recent runs;
- recent errors;
- navigation cards to Run Console, Ledger, Performance.

### 6.6 Run Console

Purpose: run readiness, run-day, replay, reset, and inspect run timeline/errors.

Layout:

```text
Trade date controls
Runtime profile JSON editor
Readiness checklist
Action bar: Run readiness, Run day, Replay, Reset & replay
Run timeline from run-events
Error detail panel
```

Rules:

- readiness failure keeps run-day visually blocked;
- reset requires `confirm_text == portfolio_id`;
- replay default is `reject_existing`;
- no raw paper-only execution config is accepted here.

### 6.7 Ledger

Purpose: inspect persisted trading artifacts.

Tabs:

- orders;
- fills;
- positions;
- daily snapshots;
- run events;
- errors.

The UI must distinguish "no run yet" from "run succeeded but no artifact" from
"backend failed".

### 6.8 Performance

Purpose: show persisted snapshot performance.

Sections:

- return, annualized return, volatility, Sharpe, win-day ratio;
- NAV table/compact curve;
- insufficient-data reasons;
- daily returns.

### 6.9 Model & HMM

Purpose: model freshness and HMM rolling-training control.

Sections:

- package model states and retrain previews;
- HMM configs, snapshots, jobs;
- HMM rolling-training preview and trigger.

Rules:

- HMM training is manual-confirmation driven;
- Paper v2 runtime never trains HMM;
- runtime only consumes completed snapshots and coefficient artifacts.

## 7. Error Handling Contract

The API client parses backend responses in this order:

1. `detail.error_code`, `detail.message`, `detail.context`;
2. string `detail`;
3. `error`;
4. HTTP status + raw response.

Every page includes an `ErrorPanel` that shows the human-readable message and an
expandable JSON context. Pages must not turn failures into empty tables.

## 8. Verification Plan

Automated verification:

- `npm run lint` where supported by the project;
- `npm run build` or `npx next build` if dependency state allows it;
- TypeScript compile through Next build;
- backend import smoke: `python -c "from backend.main import app; print(len(app.routes))"`;
- optional temporary backend smoke on a non-8001 port:

```powershell
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
```

UI-based verification options:

- If Playwright or another browser test runner is installed, add smoke tests for
  navigation and key button flows.
- If no browser runner is installed, verify by:
  - Next build/static route compilation;
  - HTTP smoke tests against temporary backend;
  - optional manual browser check by the user.

Codex must not restart the existing 8001 production backend for this work.

## 9. Implementation Priority

1. shared API/types/components;
2. `/paper-v2` shell and navigation;
3. overview/packages/selection;
4. portfolios/detail/run console;
5. ledger/performance/model-HMM;
6. validation and documentation update.

## 10. Implementation Status

Implemented frontend route tree:

```text
frontend/src/app/paper-v2/page.tsx
frontend/src/app/paper-v2/packages/page.tsx
frontend/src/app/paper-v2/selection/page.tsx
frontend/src/app/paper-v2/portfolios/page.tsx
frontend/src/app/paper-v2/portfolios/[portfolioId]/page.tsx
frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx
frontend/src/app/paper-v2/portfolios/[portfolioId]/ledger/page.tsx
frontend/src/app/paper-v2/portfolios/[portfolioId]/performance/page.tsx
frontend/src/app/paper-v2/model-hmm/page.tsx
frontend/src/app/paper-v2/settings/page.tsx
```

Implemented API alignment additions:

- StrategyPackage creation from QE experiment or QE evolution loop is exposed on
  the Packages page.
- Dynamic multi-package direct selection and existing-run aggregation are exposed
  on the Selection page.
- Single-package selection-to-Paper-v2 portfolio creation remains enabled; multi-
  package Paper execution is explicitly blocked in the UI.
- Portfolio Center and Run Console use validated execution policy ids or manifest
  defaults; no raw Paper-only execution algorithm config is exposed.
- Run Console supports dated execution policy activation before run-day.
- Ledger page reads orders, fills, cash ledger, positions, daily snapshots, run
  events, and errors.
- Added backend endpoint `/api/v1/paper-v2/portfolios/{portfolio_id}/cash-ledger`
  to close the cash-ledger UI/API gap.

Verification completed:

- `npm run build` passed and compiled all `/paper-v2` routes.
- Backend import smoke passed and confirmed the new cash-ledger route exists.
- Relevant backend pytest suite passed with 94 tests.
- Temporary backend smoke on port 8011 returned 200 for OpenAPI and core v2 list endpoints.
- Temporary frontend production-start smoke on port 3011 returned 200 for all Paper v2 routes after a clean rebuild.
- `npm run lint` is not usable non-interactively in the current project because `next lint` prompts to create an ESLint config; build-based type/validity checks passed.
- Browser click automation was not run because Playwright is not installed.
