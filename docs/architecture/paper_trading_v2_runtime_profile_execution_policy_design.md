# Paper Trading v2 Runtime Profile / Validated Execution Policy Design

Status: implementation design for the next Paper Trading v2 iteration  
Date: 2026-04-26  
Scope: StrategyPackage -> Selection Center -> Paper Trading v2  
Out of scope: QMT, Shadow, live trading, V25 adapter implementation, new daily-frequency fallback

## 1. Decision Summary

Paper Trading v2 must not treat every StrategyPackage field as a permanent runtime decision. The following fields are runtime-operational choices and must be configurable for future paper/live runs without mutating the frozen StrategyPackage manifest:

- industry blacklist;
- HMM enabled flag, HMM model snapshot, and HMM signal preset;
- active model version used for signal generation;
- validated minute execution policy chosen for a paper portfolio or a specific trade date;
- replay/reset policy.

However, execution policy flexibility has a hard boundary: Paper Trading v2 cannot expose any paper-only execution option. Every minute execution algorithm and tail-unfilled handling mode must first be represented in the backtest contract, run through backtest validation, persisted as a validated execution policy, and only then become selectable by Paper Trading v2.

## 2. Current Code Findings

Source-backed facts from the current workspace:

- `StrategyPackageRuntime` must not read raw `runtime_config.selection_scores` or manifest `strategy_config.selection_runtime.scores/scores_path` as signal input. It loads only persisted authoritative live/latest-data selection artifacts (`source_type=live_qe_model_inference_v1`, `authority_scope=authoritative_selection`), while explicit QE `pred.pkl` artifacts remain diagnostic-only.
- `SelectionCenterService` supports `single_package`, `intersection`, `union`, and `weighted_fusion`. Weighted fusion uses rank-normalized package scores and stores source package/rank/weight trace data.
- Selection Center API exposes multi-package runs and aggregate results. Multi-package selection-to-paper remains explicitly unsupported until a combined package or selection bundle contract exists.
- Paper Trading v2 resolves a backtest-validated execution policy and stores the frozen policy snapshot/hash in the run context; raw paper-only execution overrides are rejected.
- `PaperTradingHistoricalReplay` supports safe default `rerun_policy="reject_existing"` and explicit audited `rerun_policy="reset_portfolio"` with confirmation.
- `StrategyPackage` manifest hash is frozen, and package status is mutable outside the hash.
- `model_train_configs`, `model_train_snapshots`, and HMM training APIs exist, but automatic monthly retraining is not required for this phase.

## 3. Invariants

### 3.1 Frozen StrategyPackage

The frozen manifest remains the immutable proof that a QE single experiment or QE evolution loop produced a strategy package.

The manifest may keep backtest-time values such as `custom_params.enable_sector_hmm`, `custom_params.hmm_model_version_id`, and `custom_params.sector_blacklist` for lineage, but Paper Trading v2 must not blindly enforce those values as immutable runtime configuration.

### 3.2 Runtime Config Must Be Versioned

Any runtime choice that can affect selected stocks, orders, fills, cash, position, NAV, or performance must be versioned and copied into the run record.

Changing a runtime profile affects only future runs. Historical selection runs and paper runs are never silently rewritten.

### 3.3 Backtest-Validated Execution Only

Paper Trading v2 must never offer a minute execution or tail-unfilled option that the backtest path has not validated.

Allowed source for paper execution policy:

```text
QE/backtest execution config
  -> validated_execution_policy
  -> paper execution policy activation
  -> paper run frozen execution policy snapshot
```

Not allowed:

```text
Paper runtime_config raw algo_code
Paper-only tail handling mode
Unknown algo fallback to TWAP
V25 placeholder pretending to run
```

## 4. Domain Concepts

### 4.1 Runtime Profile

Runtime Profile covers configurable selection/signal settings that are not part of the frozen StrategyPackage identity.

Minimum v1 fields:

```json
{
  "industry_blacklist": [],
  "hmm": {
    "enabled": false,
    "model_snapshot_id": null,
    "signal_preset": null
  },
  "tradability": {
    "exclude_suspended": true
  },
  "selection": {
    "top_k": null
  }
}
```

Rules:

- Profile changes create a new version.
- A selection run stores the exact profile version and JSON payload used.
- HMM enabled without a valid snapshot must fail fast.
- Industry blacklist filtering must record exclusions and must continue selecting from lower ranked candidates when the signal source provides a full ranked universe.
- Unknown profile keys must fail validation unless explicitly marked unsupported.

### 4.2 Validated Execution Policy

Validated Execution Policy is the only execution policy Paper Trading v2 may activate.

Minimum v1 fields:

```text
policy_id
package_id
manifest_sha256
policy_name
policy_json
policy_sha256
algo_code
algo_config
unfilled_handler
unfilled_handler_params
source_backtest_id
source_backtest_status
validation_status
validated_at
paper_enabled
created_at
updated_at
```

Rules:

- `validation_status` must be `BACKTEST_VALIDATED`.
- `paper_enabled` must be true.
- `policy_sha256` must match canonical `policy_json`.
- `package_id` and `manifest_sha256` must match the portfolio frozen package.
- `policy_json` must contain only fields accepted by the backtest contract.
- V25 policy cannot be paper-enabled until V25 adapter and backtest contract are stable.

### 4.3 Execution Policy Activation

Paper portfolios may switch among validated execution policies before a trade-date run starts.

Minimum v1 fields:

```text
activation_id
portfolio_id
trade_date
policy_id
policy_sha256
status
activated_at
activated_by
reason
```

Rules:

- If a paper run already exists for `(portfolio_id, trade_date)`, activation for that date must fail.
- If no trade-date activation exists, Paper v2 may use the portfolio default validated policy.
- A paper run stores `validated_execution_policy_id`, `policy_sha256`, and full `policy_json` in run runtime context.

### 4.4 Multi-Package Selection

Multi-package selection means multiple independent StrategyPackages used in Selection Center aggregation. It is not multi-alpha StrategyPackage runtime.

Supported modes:

- `single_package`
- `intersection`
- `union`
- `weighted_fusion`

Default weighted fusion must use rank-normalized scores, not raw model scores:

```text
normalized_rank_score = 1 - (rank - 1) / max(candidate_count - 1, 1)
fusion_score = sum(package_weight * normalized_rank_score)
```

Rules:

- Weights are required and must be positive.
- Weights are normalized by total positive weight.
- Result rows must store source package ids, source ranks, raw scores, normalized rank scores, and weights.
- Empty aggregation fails unless explicitly modeled as `valid_no_candidate`.

### 4.5 Selection Bundle

Multi-package selection results cannot be silently converted into a single StrategyPackage. A future Paper v2 portfolio should reference a `SelectionBundle` when paper trading a multi-package aggregate.

This phase implements weighted fusion result persistence first. Direct multi-package selection-to-paper remains unsupported until `SelectionBundle` persistence and Paper v2 source resolution are implemented.

Selection run records are the current traceable artifact for multi-package selection. They store `package_ids`, per-package manifest hashes, exact runtime config, package-level rows, aggregate rows, and exclusion rows. A future `SelectionBundle` should freeze the aggregate run as a reusable package-like source before Paper v2 can trade it.

### 4.6 Model Freshness

Every StrategyPackage needs model freshness metadata outside the manifest hash.

Minimum fields:

```text
package_id
active_model_version_id
train_start_date
train_end_date
trained_at
last_retrain_job_id
last_retrained_at
stale_after_days
staleness_status
last_checked_at
```

Rules:

- Backtest-origin models enter paper as stale or warning state by default unless an explicit current model version exists.
- Stale model state does not block paper trading by itself, but API/UI must surface the warning.
- Manual retraining and rolling retraining are user-triggered flows, not automatic cron jobs.
- A rolling-training preview API should compute recommended training dates and wait for user confirmation before starting.

### 4.7 Replay Reset

Replay reset is allowed but must be explicit and auditable.

Default behavior stays safe:

```text
rerun_policy = reject_existing
```

Additional reset behavior:

```text
rerun_policy = reset_portfolio
confirm_reset = true
confirm_text = portfolio_id
```

Rules:

- Reset must never be implicit.
- Reset must have a durable audit record independent from deleted run events.
- Reset deletes paper artifacts in dependency-safe order.
- Reset can clear a portfolio's runs and replay from a historical date range to latest completed trade date.
- After replay, the portfolio remains ready for realtime Paper v2 runs. Historical replay uses `DB_HISTORICAL`; realtime runs use the configured realtime data source/run mode.

### 4.8 Multi-Package Selection vs QE Multi-Alpha

Multi-package Selection Center aggregation and QE multi-alpha are different layers:

- Multi-package selection combines already-created StrategyPackages at selection time. It is best for UI exploration, analyst review, comparing independent strategies, and quickly producing intersection/union/weighted candidate lists.
- QE multi-alpha combines alpha components inside one experiment and one StrategyPackage lineage. It is better for production-grade paper/live validation because weights, conflicts, turnover, risk, and backtest metrics can be jointly trained and frozen into one manifest.
- Multi-package paper execution should not be enabled by directly stacking packages because the aggregate result is not yet a frozen StrategyPackage and has no unified execution/risk/backtest proof.
- The preferred long-term path is: use multi-package selection for discovery, then promote the chosen combination into QE multi-alpha or a future combined package contract, run backtest validation, and only then enable Paper v2 execution.

## 5. Schema Plan

### 5.1 `strategy_pkg.model_state`

Tracks model freshness outside manifest hash.

### 5.2 `strategy_pkg.model_version`

Tracks model artifacts and training ranges generated by manual retrain or rolling retrain.

### 5.3 `strategy_pkg.model_retrain_job`

Tracks user-triggered retraining jobs and preview/confirmation payloads.

### 5.4 `strategy_pkg.validated_execution_policy`

Stores backtest-validated minute execution policy records.

### 5.5 `paper_v2.execution_policy_activation`

Stores per-portfolio/per-date policy activation.

### 5.6 `paper_v2.reset_audit`

Stores reset/replay delete counts, confirmation payload, requested date range, and status.

## 6. API Plan

### 6.1 Strategy Package Model State

```text
GET  /api/v1/strategy-packages/{package_id}/model-state
POST /api/v1/strategy-packages/{package_id}/model-retrain/preview
POST /api/v1/strategy-packages/{package_id}/model-retrain/start
GET  /api/v1/strategy-packages/{package_id}/model-retrain/jobs
```

### 6.2 Validated Execution Policy

```text
POST /api/v1/strategy-packages/{package_id}/execution-policies
GET  /api/v1/strategy-packages/{package_id}/execution-policies
POST /api/v1/strategy-packages/{package_id}/execution-policies/{policy_id}/enable-paper
```

`POST` must reject unvalidated policies unless explicitly importing a backtest-validated policy with required proof fields.

### 6.3 Selection Center

```text
POST /api/v1/selection-center/run
```

For weighted fusion:

```json
{
  "mode": "weighted_fusion",
  "package_ids": ["pkg_a", "pkg_b"],
  "runtime_config": {
    "package_weights": {
      "pkg_a": 0.6,
      "pkg_b": 0.4
    }
  }
}
```

### 6.4 Paper Trading v2 Execution Policy

```text
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policies
POST /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policy-activations
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policy-activations
```

### 6.5 Replay Reset

```text
POST /api/v1/paper-v2/portfolios/{portfolio_id}/replay
```

New payload fields:

```json
{
  "rerun_policy": "reset_portfolio",
  "confirm_reset": true,
  "confirm_text": "paper_xxx"
}
```

## 7. Implementation Phases

### Phase A: Documentation and Test Baseline

- Add this document.
- Run current relevant tests before invasive changes when practical.
- Do not restart port 8001.

### Phase B: Weighted Fusion

- Implement weighted rank fusion in `SelectionCenterService`.
- Add tests for required weights, rank-normalized scoring, trace metadata, and no raw-score silent fusion.
- Run `backend/tests/selection_center`.

### Phase C: Validated Execution Policy Foundation

- Add Pydantic models and repository/service methods for validated execution policies.
- Add in-memory repository support for tests.
- Add API endpoints under Strategy Package Center.
- Add tests for policy hash, package/manifest matching, validation status, and V25 unsupported behavior.

### Phase D: Paper v2 Policy Resolution

- Modify Paper v2 creation/run path so paper execution uses a validated policy when provided/activated.
- Keep manifest `minute_execution_policy` as the default imported policy only if it is represented as a validated policy.
- Reject raw paper-only execution overrides.
- Add tests for policy resolution, mismatch rejection, and run context trace.

### Phase E: Replay Reset

- Add reset audit model/repository operations.
- Add `reset_portfolio` rerun policy with confirmation.
- Delete artifacts in dependency order.
- Add tests for confirmation required, delete counts, replay after reset, and reject-existing default.

### Phase F: Model Freshness Metadata

- Add model state models/repository/API.
- Mark initial backtest model as stale/warning unless retrain metadata exists.
- Add retrain preview skeleton that returns recommended dates but does not auto-run training.
- Add tests for stale calculation and manual-confirm requirement.

## 8. Verification

Per feature:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest <target tests> -q -p no:cacheprovider
```

Final relevant suite:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/test_tushare_sync_engine.py -q -p no:cacheprovider
```

Temporary backend verification must use a non-8001 port, for example:

```powershell
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
```

Do not restart or stop the existing 8001 backend during this work.

## 9. Implementation Update - 2026-04-26

Backend/service implementation now includes:

- `SelectionRuntimeProfile` parsing under `backend/services/selection_center/runtime_profile.py`.
- Normalized `runtime_config.runtime_profile` persisted on selection and paper runs.
- Industry blacklist filtering in the shared `TradabilityFilter`, including lower-rank backfill and explicit `industry_blacklisted` exclusions.
- HMM runtime fail-fast boundary was initially added there; it is now upgraded in Section 10 to consume precomputed HMM coefficient artifacts when explicitly enabled.
- StrategyPackage model retrain job tracking:
  - `POST /api/v1/strategy-packages/{package_id}/model-retrain/start`;
  - `GET /api/v1/strategy-packages/{package_id}/model-retrain/jobs`;
  - `strategy_pkg.model_retrain_job`;
  - model state is marked `RETRAINING` only after explicit confirmation and is never marked current until a future training executor reports success.
- Paper v2 execution policy activation:
  - `GET /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policies`;
  - `POST /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policy-activations`;
  - `GET /api/v1/paper-v2/portfolios/{portfolio_id}/execution-policy-activations`;
  - `paper_v2.execution_policy_activation`;
  - day runner/readiness resolve a trade-date activation first and otherwise use the portfolio default policy.
- Selection trace API:
  - `GET /api/v1/selection-center/runs/{run_id}/excluded-results`.
- Paper performance report now returns annualized return, annualized volatility, Sharpe, average daily return, win-day ratio, and explicit insufficient-data reasons instead of fabricating unavailable metrics.

Still intentionally not implemented:

- V25 adapter.
- Multi-package Paper v2 execution.
- Paper-only execution or tail-unfilled options that do not exist in the backtest-validated execution policy contract.

## 10. Industry Data / HMM Runtime Update - 2026-04-26

Additional backend implementation now includes:

- Runtime industry blacklist uses PIT Shenwan mapping from `market.sw_index_member`.
  - `sw_index_member` is populated by the existing Tushare `index_member_all` dataset.
  - Blacklist matching is exact against L1/L2/L3 industry code and name.
  - Missing PIT mapping and missing candidate industry metadata fail fast when the blacklist is enabled.
  - Excluded rows persist the matched blacklist item, matched level, industry source, and full L1/L2/L3 context.
- The existing data layer already contains the required sector datasets:
  - `market.sw_index_classify` from Tushare `index_classify`;
  - `market.sw_index_member` from Tushare `index_member_all`;
  - `market.sw_daily` from Tushare `sw_daily`;
  - `market.sector_data` as the post-processed stock-level sector factor view.
- `backend/data_service` already exposes `sector_data` for QE/factor features, but it does not expose the authoritative PIT stock-to-industry metadata needed by StrategyPackage runtime filters. Selection/Paper v2 therefore adds a local `DbSwIndustryLookupProvider` instead of changing legacy `backend/data_service` semantics.
- HMM runtime now applies precomputed coefficient artifacts when explicitly enabled:
  - `runtime_profile.hmm.enabled=true` requires `model_snapshot_id` and `signal_preset`;
  - the snapshot must exist, be in a ready status, and have an existing local model artifact;
  - coefficients are loaded from `coefficients_{signal_preset}_*.json` beside the model artifact, or from an explicit `coefficients_path`;
  - the artifact must contain `daily_coefficients` for the trade date and `stock_sector_map` for every candidate;
  - candidate scores are adjusted as `raw_score * sector_coefficient`, re-ranked, and traced in `component_scores.hmm`.

Still intentionally not implemented:

- HMM training/retraining execution from Selection/Paper v2; HMM runtime consumes existing validated snapshots and precomputed coefficient artifacts only.
- Any silent neutral coefficient fallback for missing HMM sector mappings or dates.

## 11. HMM Rolling Training Update - 2026-04-26

HMM rolling training remains outside Selection Center and Paper Trading v2
runtime execution. It is implemented as a manual HMM Training Center flow that
produces completed snapshots and precomputed coefficient artifacts for runtime
consumption.

Implemented decisions:

- The authoritative executor remains WSL with the `rdagent-gpu` conda
  environment. The backend runs `scripts/hmm_train_script.py` through WSL and
  does not install or run Torch on the Windows backend path.
- The rolling preview uses the latest completed common data date across
  `market.sector_data`, `market.sw_daily`, and `market.index_daily`
  (`000300.SH`), bounded by optional `as_of_date`.
- The recommended validation window among 1-3 months is the latest **3 calendar
  months**. One- or two-month windows are accepted for diagnostics but return
  warnings because HMM state validation is usually too noisy with about one
  month of observations.
- The default training window is 3 years ending on the trading day immediately
  before validation starts. The validation end is the latest completed data date.
- The rolling plan writes `train_start`, `train_end`, `val_start`, `val_end`,
  `coefficient_start`, and `coefficient_end` into the effective training config.
- `scripts/hmm_train_script.py` forwards those split dates into RD-Agent
  `HMMTrainConfig`, so the previewed split and actual WSL training split match.
- Coefficient precompute is mandatory before a snapshot is marked completed. If
  precompute fails, the training job fails and no ready snapshot is inserted.

API:

```text
POST /api/v1/hmm-training/configs/{config_id}/rolling-training/preview
POST /api/v1/hmm-training/configs/{config_id}/rolling-training/trigger
```

The trigger endpoint requires `confirm_text == config_id`; rolling training is
manual-confirmation driven and does not run from Paper v2 or Selection Center.

Still intentionally not implemented:

- Daily automatic coefficient refresh for future trade dates after a rolling
  snapshot is produced. Runtime continues to fail fast if no coefficient
  artifact covers the requested trade date.
- Automatic model promotion into StrategyPackage model-state `CURRENT`; model
  freshness updates still require a future explicit bridge from completed HMM
  snapshots to package model-state records.
