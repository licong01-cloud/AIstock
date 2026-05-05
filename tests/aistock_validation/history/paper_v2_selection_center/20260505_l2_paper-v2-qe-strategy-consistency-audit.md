# Paper v2 / QE Backtest Strategy Consistency Audit

Date: 2026-05-05
Module: Paper v2 + Selection Center + StrategyPackage + QE
Level: L2 audit / backend + DB + test-coverage review

## Objective

Verify whether Paper v2 currently executes the same strategy contract as the QE backtest that produced the StrategyPackage.

User-level business rule being audited:

- The simulated portfolio may freeze factors and models from the StrategyPackage.
- Portfolio construction strategy, minute execution strategy, HMM, industry blacklist, risk rules, and other trading-affecting choices must stay consistent with a QE-backtested contract.
- A Paper-only strategy or Paper-only override that was not validated in the same QE/backtest context must fail fast rather than silently produce a simulated trading result.

## Summary

The current implementation is not fully consistent with the QE backtest contract.

The largest problem is not a single missing field. Paper v2 currently reconstructs target positions with an `equal_weight_topk` target engine, while the audited QE packages were produced by `score_weighted_topk_v1/v2` strategies. Existing Paper portfolios also use a separate `V25_TWO_STAGE` execution policy even though the frozen package manifests declare `V24_PLAN`.

This means current Paper v2 results cannot be treated as backtest-faithful simulated trading results for those packages.

## Evidence Commands

Read-only/static commands used:

```powershell
git status --short
rg -n "equal_weight_topk|target_weight|execution_policy|validated_execution_policy_id|V24_PLAN|V25_TWO_STAGE|top_k|hmm|industry_blacklist" backend/tests/selection_center backend/tests/paper_trading_v2 backend/tests/strategy_package backend/tests/unified_engine/test_qe_config_truth.py
```

Read-only DB audit used `backend.db.pg_pool.get_conn()` after loading `.env`.

Backend regression command:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/selection_center backend/tests/paper_trading_v2 backend/tests/strategy_package backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
```

Result:

```text
178 passed in 10.93s
```

Interpretation: the current tests pass, but they do not assert QE-vs-Paper portfolio-strategy equivalence. Some tests actively encode the current divergent behavior.

## DB Audit Snapshot

Current local DB snapshot:

```text
paper_v2.portfolio by status:
  FAILED 6
  READY 111
  RETIRED 30

paper_v2.run by status:
  FAILED 6
  SUCCEEDED 115

strategy_pkg.validated_execution_policy:
  V25_TWO_STAGE paper_enabled=true count=1

strategy_pkg.selection_score_artifact target_weight_policy:
  equal_weight_topk + qe_mlruns_pred_pkl_v1 count=30
  equal_weight_topk + live_qe_model_inference_v1 count=13

paper_v2.portfolio manifest minute algo vs Paper default policy algo:
  V24_PLAN -> V25_TWO_STAGE count=147

manifest/paper algo mismatch count:
  147

paper_v2.execution_policy_activation active:
  V25_TWO_STAGE count=42

paper_v2.runtime_profile_version top_k counts:
  21 count=46
  20 count=35
  5 count=15

runtime HMM enabled:
  false count=96

runtime industry blacklist nonempty:
  0
```

Recent sample:

```text
portfolio_id: paper_eaabf2a868f74dc39235d7f97bb0e8d1
portfolio_name: E2E-Console-1777517271330
package_id: pkg_b668f8a633c44b72a5d557a2cb8970e3
manifest algo: V24_PLAN
paper algo: V25_TWO_STAGE
validated policy: execpol_8e96a3ec3d4d414f9581c66fbf405830
source_backtest_id: execution_algorithm_catalog:V25_TWO_STAGE:2026-04-26
```

StrategyPackage rows sampled:

```text
pkg_99142cb1440c40a7824e83902f4e7da9:
  strategy_id=score_weighted_topk_v2
  topk=50
  n_drop=5
  enable_sector_hmm=true
  hmm_signal_preset=preset_A
  stock_pool=filtered_pool_20260416
  execution_algo=V24_PLAN

pkg_006a42323f7c4e81a468fdaad2cb16a3:
  strategy_id=score_weighted_topk_v1
  topk=50
  n_drop=5
  enable_sector_hmm=true
  hmm_signal_preset=preset_A
  stock_pool=filtered_pool_20260413
  execution_algo=V24_PLAN

pkg_b668f8a633c44b72a5d557a2cb8970e3:
  strategy_id=score_weighted_topk_v2
  topk=50
  n_drop=5
  enable_sector_hmm=true
  hmm_signal_preset=preset_A
  stock_pool=filtered_pool_20260416
  execution_algo=V24_PLAN
```

## Code Findings

### P0: Paper target construction is not QE ScoreWeightedTopkStrategyV1/V2

QE authority path:

- `backend/services/quantevolver/config_composer.py`
- `port_analysis_config.strategy` emits the outer portfolio strategy.
- `port_analysis_config.executor.kwargs.inner_strategy` emits the minute execution strategy.
- `ScoreWeightedTopkStrategyV1/V2` includes score weighting, dynamic n_drop, hold threshold, max/min weight, max position ratio, ghost holding forced sells, HMM score adjustment, and backup-candidate behavior.

Paper path:

- `backend/services/strategy_package/selection_artifact.py` stores `target_weight_policy = "equal_weight_topk"`.
- `backend/services/strategy_package/selection_artifact.py` assigns `target_weight = 1.0 / topk`.
- `backend/services/strategy_package/runtime.py` selects top_k candidates by rank and converts `candidate.target_weight` to target shares.
- `backend/services/strategy_package/runtime.py` rebalances by simple target-vs-current quantity diff.
- `backend/services/paper_trading_v2/day_runner.py` calls that target engine.

Missing from Paper target semantics:

- `weight_method` such as softmax/linear/rank/equal.
- `temperature`, `score_clip_quantile`, `max_weight`, `min_weight`, `max_position_ratio`.
- `enable_dynamic_ndrop`, `max_n_drop`, `min_n_drop`, threshold method and thresholds.
- `hold_thresh` sell blocking.
- V2 ghost-holding forced-sell semantics.
- ScoreWeighted buy/sell candidate pairing and fill-to-topk behavior.
- Tail-substitute backup-candidate semantics coupled to the outer strategy.
- Exact order of HMM/risk/suspend filtering relative to ranking/weighting/rebalance.

Conclusion: even when factor scores are correct, Paper v2 is not executing the same portfolio construction strategy as the QE backtest.

### P0: Existing Paper portfolios use a different minute execution policy

The frozen manifests in current Paper portfolios declare `V24_PLAN`, but Paper portfolios use `V25_TWO_STAGE`.

Relevant code:

- `backend/services/paper_trading_v2/service.py` allows a requested validated execution policy at portfolio creation.
- `backend/services/paper_trading_v2/service.py` skips full manifest execution-policy validation when an explicit policy is supplied.
- `backend/services/paper_trading_v2/day_runner.py` uses portfolio default policy or a per-date activation policy.

This was an intentional flexibility design in previous architecture docs, but it conflicts with the current business rule that Paper must execute only the same backtest-validated package contract unless a new package/version is created from that exact validation.

### P0/P1: Runtime profile can alter trading-affecting strategy fields

Runtime profile fields currently include:

- `selection.top_k`
- `industry_blacklist`
- `hmm.enabled/model_snapshot_id/signal_preset/coefficients_path`
- `tradability.exclude_suspended`
- `risk_policy`

DB shows runtime top_k values 5, 20, and 21, while current package manifests have `topk=50`.

This changes the paper portfolio strategy compared with the backtest unless those profile variants were separately backtested and versioned as valid runtime contracts.

### P1: HMM is implemented but not tied to the QE manifest contract

QE ScoreWeighted applies HMM adjustment inside the strategy before rank/weight logic.

Paper runtime can also multiply/adjust scores and rerank candidates through `SectorHMMRuntime`, but it is selected through runtime profile and then followed by equal-weight target construction.

Therefore the current Paper HMM path is not enough to claim QE equivalence.

### P1: Industry blacklist semantics differ from QE stock_pool execution

QE config currently treats blacklist metadata as UI/detail traceability; executable restriction is represented through `stock_pool` and/or generated runtime artifacts.

Paper v2 applies `industry_blacklist` dynamically with PIT industry lookup and backfill.

This may be a useful runtime feature, but enabling it independently is a strategy change unless it matches a backtest-validated runtime contract.

### P1: Stock universe is a known gap but not the main fix for this audit

The live inference path still uses a default DB universe excluding historical ST stocks, not the frozen package `stock_pool`.

The user has stated that stock pool freezing is still under QE development and should be applied later. Therefore this audit records it as a known future consistency gap rather than the immediate P0 remediation.

## Test Coverage Findings

The current tests do not protect QE-vs-Paper strategy equivalence.

Examples:

- `backend/tests/selection_center/test_runtime_selection.py` asserts `target_weight == 1.0 / manifest.portfolio_policy.topk`, which encodes equal-weight topk behavior.
- `backend/tests/paper_trading_v2/test_day_runner.py` contains cases where a manifest with `V24_PLAN` uses a separate Paper policy such as `TWAP` or an activated policy.
- `backend/tests/unified_engine/test_qe_config_truth.py` validates QE YAML truth but does not compare Paper target/order generation against QE strategy output.

Required missing tests:

- A QE/Paper oracle test for `ScoreWeightedTopkStrategyV2`: same scores, holdings, cash, reference prices, and constraints must produce equivalent target/order intents.
- API tests rejecting Paper portfolio creation when requested execution policy differs from the frozen QE backtest contract unless it references a package-specific backtest-validated runtime contract.
- Runtime profile tests rejecting top_k/HMM/blacklist changes that are not present in the frozen/validated contract.
- DB audit test/report for existing portfolios whose manifest algo differs from Paper policy algo.

## Corrective Direction

Recommended remediation order:

1. Add a normalized backtest runtime contract to StrategyPackage manifests or a companion immutable package contract. It must include the portfolio strategy class/module, all resolved strategy kwargs/defaults, HMM contract, blacklist/stock-pool/risk policy, and minute execution policy.
2. Add a Paper portfolio construction adapter for the QE portfolio strategy, starting with `ScoreWeightedTopkStrategyV2`. The adapter must reproduce sell/buy selection, weighting, n_drop, hold_thresh, forced exits, and ordering semantics.
3. Change Paper v2 creation/activation validation to reject execution policy mismatches by default. A catalog-level `V25_TWO_STAGE` validation is not enough; the policy must be tied to the same package/backtest contract or a new StrategyPackage version.
4. Lock runtime-profile fields that change strategy behavior unless the values exactly match the validated contract. Custom HMM/top_k/blacklist profiles should create a new backtest-validated contract before Paper execution.
5. Update UI to display the frozen QE contract and disable controls that would break consistency, with Chinese explanations.
6. Add regression tests and validation records before reporting Paper v2 strategy fidelity as complete.

## Asset Safety

No protected assets were intentionally modified during this audit:

- No StrategyPackage manifests.
- No QE workspaces.
- No model weights.
- No HMM snapshots or coefficient artifacts.
- No validated execution policies.
- No Paper ledger/order/fill/position rows.

This audit added only this Markdown record.

## Residual Risk

The audit proves that current code and local DB state are inconsistent with the user-stated quant backtest-to-paper principle. It does not implement the corrective code. Until the remediation is implemented, Paper v2 results from current packages should be considered diagnostic framework output, not backtest-faithful simulated portfolio performance.
