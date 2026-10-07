"""One held-path information hypothesis; all original Exit label/action clocks stay fixed."""
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import (
    AUDIT, FEATURES, POLICY_SHA256 as LABEL_POLICY_SHA256,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

HYPOTHESIS = "EXIT5-HELD-PATH-CONTEXT-1"
NEW_FEATURES = ("held_mark_return_bps", "held_peak_drawdown_fraction", "held_range_fraction")
MODEL_FEATURES = (*FEATURES, *NEW_FEATURES, "remaining_session_fraction")
FEATURE_CONTRACT = dict(hypothesis=HYPOTHESIS, features=NEW_FEATURES,
    price_basis="S_ANCHOR_RAW_TIMES_HISTORICAL_FACTOR_OVER_S_FACTOR",
    visible_window="ORIGINAL_ENTRY_T_THROUGH_S_INCLUSIVE_COMPLETE_SESSIONS",
    mark="S_NET_LIQUIDATION_OVER_ORIGINAL_ENTRY_COST_NOT_ACTUAL_S_SALE",
    peak="S_CLOSE_OVER_MAX_HIGH_T_TO_S_MINUS_ONE", range="MAX_HIGH_OVER_MIN_LOW_T_TO_S_MINUS_ONE",
    missing="PER_FIELD_UNKNOWN_NO_FILL_NO_DROP_NO_WINDOW_SHORTENING", label_policy_sha256=LABEL_POLICY_SHA256)
FEATURE_SHA256 = sha(FEATURE_CONTRACT)
RECIPE = {**AUDIT, "hypothesis": HYPOTHESIS, "features": MODEL_FEATURES,
    "missing_flag_features": (*FEATURES, *NEW_FEATURES), "model_inputs": 25,
    "control": "REUSE_ORIGINAL_CROSS_FITTED_PREDICTIONS_ZERO_CONTROL_REFITS"}
RECIPE_SHA256 = sha(RECIPE)
PRICE_FIELDS = ("trade_date", "instrument", "raw_open_cny", "raw_high_cny", "raw_low_cny", "raw_close_cny", "adj_factor")
FEATURE_KEY = ("episode_id", "s_date")
