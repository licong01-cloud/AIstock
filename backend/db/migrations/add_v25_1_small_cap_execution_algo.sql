-- Register the V25.1 small-cap board+cost-aware execution algorithm in execution_algorithm_catalog.
-- V25.1 inherits the V25 two-stage plan generator (88.79/11.21 weights), but
-- transforms the per-minute plan into a board-aware, cost-aware bucket schedule
-- before slicing. Validated against V25 baseline over 1-year minute backtest
-- (2025-05-06 ~ 2026-04-24, 5215 stocks, 10M RMB, topk=50): +2.07pp total return,
-- +0.10 Sharpe, +1.12pp fill_rate, 0 board-rule violations, 0 silent drops.

INSERT INTO public.execution_algorithm_catalog
    (
        algo_code,
        algo_name,
        source,
        description,
        source_code,
        default_config,
        param_schema,
        supported_freqs,
        min_bars,
        sort_order,
        is_enabled
    )
VALUES
    (
        'V25_1_SMALL_CAP',
        'V25.1 Small-Cap Execution (board+cost-aware)',
        'custom',
        'V25.1 small-cap minute execution: reuses V25 two-stage plan generator (early 30min @ 88.79% + late 210min @ 11.21%), then transforms per-minute plan into a board-aware, cost-aware bucket schedule. Honours main board / ChiNext (00/60/300/301/302) 100-share lots and STAR market (688/689) 200-min/1-increment. Cost floor: min_slice_amount = min_cost / (commission_rate + tolerance_bps/1e4). Authoritative algo class lives in backend/execution_algos/v25_1_small_cap_algo.py; QE wrapper in scripts/tail_twap_v25_1_strategy.py. Missing models or invalid devices must fail fast.',
        $$pred_early = early_model(gap_bucket, gap_ratio, day_features)
pred_late = late_model(gap_bucket, gap_ratio, is_buy, early_weight, early_peak, early_conc)
plan = concat([pred_early * 0.8879, pred_late * 0.1121])
plan = plan / plan.sum()
# Board-aware bucket schedule:
min_lot, increment = board_lot_rule(stock_id)
min_slice_amount = min_cost / (commission_rate + tolerance_bps / 1e4)
bucket_qty = bucket_schedule(plan, min_lot, increment, min_slice_amount, max_buckets)
# Parent's scale-invariant base_delta formula then emits bucket_qty[step] per bar.$$,
        '{"early_model_path": "/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt", "late_model_path": "/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt", "device": "cuda", "min_cost": 5.0, "commission_rate": 0.00025, "tolerance_bps": 10.0, "max_buckets": 12}'::jsonb,
        '{"type": "object", "properties": {"early_model_path": {"type": "string", "description": "V25 early model path (shared with V25_TWO_STAGE)"}, "late_model_path": {"type": "string", "description": "V25 late model path (shared with V25_TWO_STAGE)"}, "device": {"type": "string", "enum": ["cpu", "cuda"], "default": "cuda"}, "min_cost": {"type": "number", "minimum": 0, "default": 5.0, "description": "minimum commission per fill (RMB)"}, "commission_rate": {"type": "number", "exclusiveMinimum": 0, "default": 0.00025, "description": "broker commission rate"}, "tolerance_bps": {"type": "number", "minimum": 0, "default": 10.0, "description": "commission overshoot tolerance in bps"}, "max_buckets": {"type": "integer", "minimum": 1, "maximum": 240, "default": 12, "description": "max bucket count per side"}}, "required": ["early_model_path", "late_model_path"]}'::jsonb,
        ARRAY['1m'],
        240,
        5,
        TRUE
    )
ON CONFLICT (algo_code) DO UPDATE SET
    algo_name = EXCLUDED.algo_name,
    source = EXCLUDED.source,
    description = EXCLUDED.description,
    source_code = EXCLUDED.source_code,
    default_config = EXCLUDED.default_config,
    param_schema = EXCLUDED.param_schema,
    supported_freqs = EXCLUDED.supported_freqs,
    min_bars = EXCLUDED.min_bars,
    sort_order = EXCLUDED.sort_order,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = NOW();
