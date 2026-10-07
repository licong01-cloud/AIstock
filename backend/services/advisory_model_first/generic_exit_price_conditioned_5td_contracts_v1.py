"""One net-price scenario hypothesis; no raw-price product or package gate."""
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

HYPOTHESIS = "EXIT5-PRICE-CONDITIONED-CONTINUATION-1"
KEY = ("episode_id", "s_date")
S_FIELDS = (*KEY, *FEATURES, "remaining_session_fraction")
TRAIN_FIELDS = (*S_FIELDS, "entry_date", "held", "y_hold_bps", "sell_scenario_bps")
QUERY_FIELDS = (*KEY, "sell_scenario_bps")
RECIPE = dict(alpha=1., max_physical_fits=4, model_inputs=20,
    unique_variable="HYPOTHETICAL_NET_PRICE_CONDITIONING", query_scale=10000.,
    query_role="HYPOTHETICAL_S_EQUIVALENT_NET_LIQUIDATION_RELATIVE_TO_S_REFERENCE",
    support_quantiles=[.025, .975], support_bucket_bps=100.,
    support_min_decisions=30, support_min_entry_cohorts=5,
    label_policy_sha256=POLICY_SHA256)
RECIPE_SHA256 = sha(RECIPE)
CURVE_FIELDS = (*KEY, "remaining_sessions", "a_bps", "b_per_bps", "support_json",
    "curve_status", "model_sha256", "s_input_sha256", "policy_sha256", "curve_sha256")

__all__ = ["CURVE_FIELDS", "FEATURES", "HYPOTHESIS", "KEY", "POLICY_SHA256",
           "QUERY_FIELDS", "RECIPE", "RECIPE_SHA256", "S_FIELDS", "TRAIN_FIELDS"]
