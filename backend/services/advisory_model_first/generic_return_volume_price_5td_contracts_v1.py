"""Only new D-past cross-day volume/return information, not a package gate."""
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    FEATURES, KEY, PARAMETERS, POLICY, POLICY_SHA256, GenericPrice5TDConfigurationV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

LAG_FEATURES = ("lag_volume_next_return_cov18", "volume_innovation_return_cov18", "downside_pressure_next_return18")
MODEL_FEATURES = (*FEATURES, *LAG_FEATURES)
SCHEMA_SHA256 = sha(dict(schema="generic_return_volume_price_5td_v1", features=MODEL_FEATURES,
    quantity="volume_hand_over_D_past_factor_proxy", sessions=20, return_count=19, pairs=18,
    missing="PER_FIELD_TRAIN_MEDIAN_PLUS_FLAGS", policy_sha256=POLICY_SHA256, physical_fit_budget=2))

__all__ = ["FEATURES", "KEY", "PARAMETERS", "POLICY", "POLICY_SHA256", "GenericPrice5TDConfigurationV1",
           "LAG_FEATURES", "MODEL_FEATURES", "SCHEMA_SHA256"]
