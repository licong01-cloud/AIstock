# Strategy Package Manifest v1 Contract

> Status: legacy contract. This document is retained only for historical v1
> manifest compatibility and migration context.
> Superseded-by: `docs/architecture/strategy_package_platform_boundary_contract_20260520.md`.
> Current rule: new StrategyPackage manifests must use `manifest_version:
> "alpha_core_v1"` and must not bind platform runtime policies such as
> `strategy_config`, `universe_policy`, `portfolio_policy`,
> `execution_policy`, `minute_execution_policy`, or `risk_policy`.
> Scope of this document after 2026-05-20: reading old v1 manifests only; it is
> not the implementation authority for new Selection Center, Paper Trading v2,
> MiniQMT SIM, or future live-trading runtime behavior.

## 0. Legacy Notice

The original v1 contract required StrategyPackage to carry daily strategy,
portfolio, universe, minute execution, and risk settings. That boundary is now
deprecated because those settings are platform runtime capabilities, not
alpha-core assets. New development must follow the alpha-core boundary:

- StrategyPackage owns factor/model alpha core, source lineage, hashes, and
  training/backtest evidence.
- RuntimeProfile owns mutable daily selection settings, HMM, ST PIT, risk and
  tradability choices.
- ValidatedExecutionPolicy owns minute execution algorithm and tail/unfilled
  handling references.
- Paper v2 and MiniQMT must consume platform runtime/profile/policy versions or
  fail fast; they must not auto-promote manifest minute policy into a validated
  execution policy.

The sections below describe the legacy v1 shape only.

## 1. Purpose

`StrategyPackage` is the only standard asset that can enter the new Selection Center and Paper Trading v2.

It must represent both:

- current single-alpha QE results;
- future multi-alpha QE results.

It must also include the minute-level execution policy, because current QE backtests already use minute-line execution.

## 2. Allowed Sources

Allowed:

```text
QEExperiment -> StrategyPackage
QEEvolutionLoop -> StrategyPackage
```

Not allowed:

```text
RD-Agent Task -> StrategyPackage
RD-Agent Loop -> StrategyPackage
Old selection config -> StrategyPackage
Manual ad-hoc factor/model/strategy mix -> StrategyPackage
```

Manual combinations must first become a QE experiment and pass QE backtest validation before package generation.

## 3. Required Manifest Fields

```yaml
manifest_version: "1.0"
package_id: string
package_name: string
package_version: string
source:
  source_type: "qe_experiment" | "qe_evolution_loop"
  source_id: string
  loop_id: string | null
  run_id: string | null
  created_at: datetime
alpha_mode: "single_alpha" | "multi_alpha"
alpha_components: AlphaComponent[]
alpha_combination_policy: AlphaCombinationPolicy
factor_set: FactorAsset[]
model_asset: ModelAsset | ModelAsset[]
strategy_config: object
universe_policy: UniversePolicy
portfolio_policy: PortfolioPolicy
execution_policy: ExecutionPolicy
minute_execution_policy: MinuteExecutionPolicy
risk_policy: RiskPolicy
backtest_summary: BacktestSummary
asset_checks: AssetCheck[]
manifest_sha256: string
package_status: PackageStatus
```

## 4. Alpha Model

### 4.1 Single Alpha

Single-alpha packages use the same structure as multi-alpha packages:

```yaml
alpha_mode: "single_alpha"
alpha_components:
  - alpha_id: "alpha_001"
    component_weight: 1.0
```

Rules:

- `alpha_components` length must be exactly 1.
- `alpha_combination_policy.method` should be `identity`.
- Selection result attribution still records the component id.

### 4.2 Multi Alpha

Multi-alpha packages are first-class v1 objects, not a later extension.

```yaml
alpha_mode: "multi_alpha"
alpha_components:
  - alpha_id: "quality_value_alpha"
    component_weight: 0.35
  - alpha_id: "momentum_reversal_alpha"
    component_weight: 0.40
  - alpha_id: "liquidity_risk_alpha"
    component_weight: 0.25
```

Rules:

- `alpha_components` length must be greater than 1.
- Each component must preserve factor/model lineage from QE.
- Selection Center and Paper Trading v2 must store component-level contribution.
- Attribution must support package-level and component-level drilldown.

### 4.3 AlphaComponent

```yaml
alpha_id: string
alpha_name: string
component_weight: float
factor_ids: string[]
model_id: string | null
model_ref: string | null
holding_period: string
rebalance_frequency: string
score_direction: "higher_better" | "lower_better"
score_normalization: "zscore" | "rank" | "none" | string
risk_tags: string[]
metrics_snapshot:
  ic: float | null
  rank_ic: float | null
  annual_return: float | null
  max_drawdown: float | null
  turnover: float | null
  sample_start: date | null
  sample_end: date | null
lineage:
  qe_artifact_id: string | null
  factor_artifact_refs: string[]
  model_artifact_ref: string | null
```

## 5. AlphaCombinationPolicy

```yaml
method: "identity" | "weighted_score" | "rank_fusion" | "vote" | "risk_budget"
weights:
  alpha_id: float
score_clip: object | null
normalization_scope: "universe" | "industry" | "market_cap_bucket" | string
conflict_resolution: "highest_score" | "weighted_sum" | "exclude_conflict" | string
explainability:
  store_component_scores: true
  store_component_rank: true
  store_component_reason: true
```

For `single_alpha`, `method = identity` and the only component weight is `1.0`.

## 6. MinuteExecutionPolicy

`minute_execution_policy` is required in v1.

```yaml
execution_level: "minute"
bar_freq: "1m" | "5m"
algo_code: "TWAP" | "VWAP" | "POV" | "CLOSE_PRICE" | string
algo_config:
  max_participation_rate: float | null
  split_count: int | null
  force_complete_before_close: bool
  allow_partial_fill: bool
fallback_algo_code: null
data_requirements:
  requires_minute_bar: true
  requires_limit_price: true
  requires_trade_calendar: true
  requires_suspend_status: true
fallback_policy:
  on_missing_minute_bar: "fail"
  on_algo_error: "fail"
quality_report:
  record_slippage: true
  record_participation_rate: true
  record_unfilled_reason: true
```

Rules:

- Paper Trading v2 uses minute execution as the MVP main path.
- Daily matching is disabled for authoritative paper trading and package validation.
- Authoritative paper validation runs must fail when required minute data is missing.
- Silent fallback is not allowed.
- Execution algorithms may emit `StepFill` / `OrderEvent`; they must not directly mutate cash, positions, NAV, or ledger state.
- Existing `backend/execution_algos` can be adapted behind an `ExecutionAlgoAdapter`.

## 7. Fail-Fast And No Silent Success

The trading path must never return success when a required business function is missing.

Hard rules:

- Missing required data must raise an explicit domain error and persist a failed run status.
- Unsupported alpha mode, combination method, execution algo, order type, or A-share rule must raise `UnsupportedFeatureError`.
- Placeholder implementations must raise `NotImplementedError` or `UnsupportedFeatureError`; they must not return empty results, zero fills, default prices, or success states.
- Diagnostic tools may exist outside the authoritative trading path, but they must not provide alternate fills, alternate PnL, or alternate promotion metrics.
- Exceptions must include `error_code`, `message`, `context`, and `run_id` or `package_id` when available.
- Batch jobs and schedulers must mark the affected item as failed instead of swallowing exceptions and continuing as successful.

## 8. Runtime Flow

```text
StrategyPackage
  -> StrategyPackageRuntime
  -> SignalSnapshot
  -> TargetPosition
  -> OrderIntent
  -> RiskEngine
  -> OMS / Order
  -> SimBrokerAdapter
  -> MinuteExecutionEngine
  -> StepFill / Fill / OrderEvent
  -> Ledger
  -> DailySnapshot / Attribution
```

The package manifest and `manifest_sha256` must be frozen when a paper portfolio is created. Package upgrades require a new package version or an explicit new portfolio run.

## 9. Validation Gates

Package generation must fail if any of these are missing:

- valid QE source;
- factor/model/strategy artifact references;
- `alpha_mode`;
- `alpha_components`;
- `alpha_combination_policy`;
- `minute_execution_policy`;
- backtest summary;
- manifest hash.

Additional validation:

- single alpha: exactly one component;
- multi alpha: at least two components;
- all component weights are finite;
- combination policy references only existing component ids;
- minute execution algo exists in registry or has a declared custom adapter;
- fallback behavior is disabled for authoritative validation.

## 10. Data Service Boundary

This contract does not require changing `backend/data_service`.

Paper Trading v2 may add an internal adapter that reads existing market data and converts it into minute bars required by `MinuteExecutionEngine`. Any change to `backend/data_service` must follow the separate evaluation process defined in `docs/architecture/paper_trading_v2_implementation_plan.md`.

## 11. Acceptance Checklist

- A current single-alpha QE experiment can generate a valid package.
- A future multi-alpha QE loop can generate a valid package without schema changes.
- The package detail page can display component lineage and weights.
- The Selection Center can store package-level and component-level selection explanations.
- A paper portfolio can run a one-day minute replay from the package.
- Missing minute data causes explicit failure in authoritative paper validation.
- Missing data or unsupported execution behavior fails the authoritative run.
- Cash, positions, and NAV change only through OMS / Fill / Ledger.
