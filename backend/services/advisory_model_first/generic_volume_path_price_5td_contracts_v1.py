"""D volume-path information block; unchanged fixed-five-trading-session business policy."""
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import (
    ARMS, FEATURES, FIELDS, KEY, MINUTE_FEATURES as OLD_MINUTE_FEATURES,
    PARAMETERS, PINS, POLICY, POLICY_SHA256, GenericPrice5TDConfigurationV1,
    MinuteSourceIdentityV1, check_resource_budget_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

BASE_MINUTE_FEATURES = tuple(name for name in OLD_MINUTE_FEATURES if name != "close_to_vwap_bps")
VOLUME_FEATURES = (
    "close_to_volume_weighted_bar_close_bps", "signed_volume_pressure", "closing_30m_volume_share",
)
MINUTE_FEATURES = (*BASE_MINUTE_FEATURES, *VOLUME_FEATURES)
SCHEMA = "generic_volume_path_price_5td_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, daily=FEATURES, matched=BASE_MINUTE_FEATURES,
    volume=VOLUME_FEATURES, matched_dimensions=33, candidate_dimensions=39,
    coverage_denominator="ORIGINAL_D_CALENDAR", minimum_joint_volume_coverage=.8,
    weighted_close="SUM_CLOSE_TIMES_VOLUME_OVER_SUM_VOLUME_NOT_TRADE_VWAP",
    signed_pressure="EXACT_1MIN_ADJACENT_PAIRS_ONLY",
    closing_volume="ORIGINAL_1430_END_WINDOW_ALL_KNOWN"))

__all__ = ["ARMS", "FEATURES", "FIELDS", "KEY", "BASE_MINUTE_FEATURES", "MINUTE_FEATURES",
    "VOLUME_FEATURES", "PARAMETERS", "PINS", "POLICY", "POLICY_SHA256", "SCHEMA", "SCHEMA_SHA256",
    "GenericPrice5TDConfigurationV1", "MinuteSourceIdentityV1", "check_resource_budget_v1"]
