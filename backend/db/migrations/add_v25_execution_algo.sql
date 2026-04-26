-- Register the fixed V25 two-stage execution algorithm in execution_algorithm_catalog.

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
        'V25_TWO_STAGE',
        'V25 Two-Stage execution plan (oracle weights)',
        'custom',
        'V25 two-stage minute execution: early model covers first 30 minutes with 88.79% weight; late model covers the remaining 210 minutes with 11.21% weight. Missing models or invalid devices must fail fast.',
        $$pred_early = early_model(gap_bucket, gap_ratio, day_features)
pred_late = late_model(gap_bucket, gap_ratio, is_buy, early_weight, early_peak, early_conc)
plan = concat([pred_early * 0.8879, pred_late * 0.1121])
plan = plan / plan.sum()$$,
        '{"early_model_path": "/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt", "late_model_path": "/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt", "device": "cuda"}'::jsonb,
        '{"type": "object", "properties": {"early_model_path": {"type": "string", "description": "V25 early model path"}, "late_model_path": {"type": "string", "description": "V25 late model path"}, "device": {"type": "string", "enum": ["cpu", "cuda"], "default": "cuda"}}, "required": ["early_model_path", "late_model_path"]}'::jsonb,
        ARRAY['1m'],
        30,
        4,
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
