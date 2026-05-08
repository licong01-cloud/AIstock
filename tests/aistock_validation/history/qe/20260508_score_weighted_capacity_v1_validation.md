# ScoreWeighted V2 Capacity v1 Validation

- Task: Agent B capacity-parameterized ScoreWeighted V2 strategy asset.
- Branch: `codex/qe-score-weighted-capacity-v1-20260508`.
- Commit: uncommitted, pending integrator validation.
- Strategy ID: `score_weighted_topk_v2_capacity_v1`.
- New source file: `scripts/score_weighted_strategy_v2_capacity_v1.py`.
- Registration script: `scripts/register_score_weighted_strategy_v2_capacity_v1.py` defaults to dry-run; no DB write was performed.

## Changed Files

- `scripts/score_weighted_strategy_v2_capacity_v1.py`
- `scripts/register_score_weighted_strategy_v2_capacity_v1.py`
- `scripts/qe_suspend_filter_score_weighted_strategy.py`
- `backend/services/quantevolver/config_composer.py`
- `backend/services/strategy_package/backtest_contract.py`
- `backend/services/strategy_package/runtime.py`
- `frontend/src/app/quantevolver/evolution/page.tsx`
- `frontend/src/app/quantevolver/evolution/components/ParamSchemaForm.tsx`
- `backend/tests/strategy_package/test_score_weighted_capacity_contract.py`
- `backend/tests/unified_engine/test_qe_config_truth.py`
- `backend/tests/unified_engine/test_score_weighted_capacity_registration.py`

## Validation Commands

- `python -m py_compile scripts/register_score_weighted_strategy_v2_capacity_v1.py scripts/score_weighted_strategy_v2_capacity_v1.py scripts/qe_suspend_filter_score_weighted_strategy.py backend/services/quantevolver/config_composer.py backend/services/strategy_package/backtest_contract.py backend/services/strategy_package/runtime.py` passed.
- `python scripts/register_score_weighted_strategy_v2_capacity_v1.py` passed in dry-run mode; output included `max_single_order_value=1000000000.0` and no DB write.
- `pytest backend/tests -q -p no:cacheprovider -k "strategy and (capacity or score_weighted or package)"` passed: 47 passed, 845 deselected.
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1 --fail-new-only --baseline-json tmp/validation/guardrails/baseline_20260504.json` passed: blocking=0, P2 review findings only.
- `cd frontend; npm exec tsc -- --noEmit --incremental false` passed after `npm ci` installed local dependencies.
- `git diff --check` passed with line-ending warnings only.

## Business Checks

- New strategy defaults include `max_single_order_value=1000000000.0`, `max_weight=0.05`, and `max_position_ratio=0.95`.
- New strategy param schema exposes `max_single_order_value`, `max_weight`, and `max_position_ratio`.
- StrategyPackage contract keeps the family as `score_weighted_topk_v2` and records the new `strategy_id` with `capacity_profile=capacity_parameterized_v1`.
- Legacy `score_weighted_topk_v2` contract still uses `max_single_order_value=5000000.0` when the manifest lacks an explicit capacity value.
- Frontend evolution forms render strategy catalog schemas, label legacy `score_weighted_topk_v2` as `legacy_5m_cap`, and prefill only the required capacity fields for `score_weighted_topk_v2_capacity_v1` instead of copying every catalog `default_kwargs` key into requested configs.
- Production backend `8001`, model weights, HMM snapshots, QE/RD-Agent artifacts, Paper ledger, and production DB were not touched.
