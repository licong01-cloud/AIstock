-- Register ScoreWeightedTopkStrategy into aistock_strategy_catalog
-- Created: 2026-04-06
-- Purpose: T1.1+T1.3 - Make new strategy selectable in QE experiment creation UI
--
-- NOTE: This SQL only inserts metadata. The source_code column must be
-- populated separately via the Python loader script:
--   scripts/register_score_weighted_strategy.py
-- because source_code is read from score_weighted_strategy.py file.
--
-- The config_composer._get_strategy_info() reads source_code to build
-- custom_strategy.py in workspace. Without source_code, experiments WILL FAIL.

INSERT INTO aistock_strategy_catalog (
    strategy_id,
    catalog_version,
    generated_at_utc,
    catalog_source,
    scenario,
    strategy_type,
    display_name,
    description,
    source_code_relpath,
    python_implementation,
    portfolio_config,
    default_kwargs,
    param_schema,
    in_selection_center
) VALUES (
    'score_weighted_topk_v1',
    '1.0',
    '2026-04-06T00:00:00Z',
    'custom',
    'qlib_factor',
    'daily',
    'ScoreWeightedTopk (Softmax+DynamicNDrop)',
    'Configurable weighted strategy with controlled dynamic n_drop. 4 weighting methods (softmax/linear/rank/equal) + 3 threshold methods (adaptive/fixed/percentile). Preserves max_n_drop hard cap for turnover control.',
    'score_weighted_strategy.py',
    '{"module_path": "rdagent.scenarios.qlib.experiment.factor_template.score_weighted_strategy", "class_name": "ScoreWeightedTopkStrategy"}'::jsonb,
    '{
        "class": "ScoreWeightedTopkStrategy",
        "kwargs": {
            "topk": 50, "n_drop": 5, "signal": "<PRED>",
            "weight_method": "softmax", "temperature": 1.0,
            "score_clip_quantile": 0.0, "max_weight": 0.05,
            "min_weight": 0.005, "max_position_ratio": 0.95,
            "enable_dynamic_ndrop": true, "max_n_drop": 5, "min_n_drop": 0,
            "threshold_method": "adaptive", "min_improvement": 0.01,
            "adaptive_multiplier": 0.5, "threshold_floor": 0.005,
            "hold_thresh": 2, "only_tradable": true, "forbid_all_trade_at_limit": false
        },
        "module_path": "rdagent.scenarios.qlib.experiment.factor_template.score_weighted_strategy"
    }'::jsonb,
    '{
        "topk": 50, "n_drop": 5,
        "weight_method": "softmax", "temperature": 1.0,
        "score_clip_quantile": 0.0, "max_weight": 0.05,
        "min_weight": 0.005, "max_position_ratio": 0.95,
        "enable_dynamic_ndrop": true, "max_n_drop": 5, "min_n_drop": 0,
        "threshold_method": "adaptive", "min_improvement": 0.01,
        "adaptive_multiplier": 0.5, "threshold_floor": 0.005,
        "hold_thresh": 2, "only_tradable": true, "forbid_all_trade_at_limit": false
    }'::jsonb,
    '[
        {"name": "weight_method", "type": "enum", "options": ["softmax", "linear", "rank", "equal"], "default": "softmax", "desc": "Weighting method"},
        {"name": "temperature", "type": "float", "min": 0.1, "max": 5.0, "default": 1.0, "desc": "Softmax temperature"},
        {"name": "max_weight", "type": "float", "min": 0.01, "max": 0.2, "default": 0.05, "desc": "Max single stock weight"},
        {"name": "min_weight", "type": "float", "min": 0.0, "max": 0.05, "default": 0.005, "desc": "Min single stock weight"},
        {"name": "max_position_ratio", "type": "float", "min": 0.5, "max": 1.0, "default": 0.95, "desc": "Max portfolio ratio"},
        {"name": "enable_dynamic_ndrop", "type": "bool", "default": true, "desc": "Enable dynamic n_drop"},
        {"name": "max_n_drop", "type": "int", "min": 1, "max": 20, "default": 5, "desc": "Max n_drop (critical risk cap)"},
        {"name": "min_n_drop", "type": "int", "min": 0, "max": 10, "default": 0, "desc": "Min n_drop"},
        {"name": "threshold_method", "type": "enum", "options": ["adaptive", "fixed", "percentile"], "default": "adaptive", "desc": "Threshold method"},
        {"name": "adaptive_multiplier", "type": "float", "min": 0.1, "max": 2.0, "default": 0.5, "desc": "Adaptive multiplier"},
        {"name": "threshold_floor", "type": "float", "min": 0.0, "max": 0.05, "default": 0.005, "desc": "Adaptive threshold floor"},
        {"name": "min_improvement", "type": "float", "min": 0.0, "max": 0.1, "default": 0.01, "desc": "Fixed threshold value"},
        {"name": "hold_thresh", "type": "int", "min": 1, "max": 10, "default": 2, "desc": "Min holding days"}
    ]'::jsonb,
    TRUE
) ON CONFLICT (strategy_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    strategy_type = EXCLUDED.strategy_type,
    source_code_relpath = EXCLUDED.source_code_relpath,
    python_implementation = EXCLUDED.python_implementation,
    portfolio_config = EXCLUDED.portfolio_config,
    default_kwargs = EXCLUDED.default_kwargs,
    param_schema = EXCLUDED.param_schema,
    in_selection_center = EXCLUDED.in_selection_center,
    updated_at = NOW();
