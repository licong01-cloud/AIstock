"""One new D-clock information block; unchanged joint learner and fixed 5TD policy."""
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import (
    FEATURES, KEY, MINUTE_FEATURES, PINS, POLICY, POLICY_SHA256,
    GenericPrice5TDConfigurationV1, MinuteSourceIdentityV1, check_resource_budget_v1,
)
from backend.services.advisory_model_first.generic_joint_distribution_price_5td_model_v1 import PARAMETERS
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

FIELDS = ("open", "high", "low", "close", "volume")
ORDERED_FEATURES = tuple(f"d_bin_{i:02d}_{kind}" for i in range(16) for kind in ("return_bps", "volume_share"))
RAW_FEATURES = (*FEATURES, *MINUTE_FEATURES, *ORDERED_FEATURES)
DIMENSIONS = 103
SCHEMA = "generic_ordered_path_price_5td_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, fields=RAW_FEATURES, dimensions=DIMENSIONS, parameters=PARAMETERS,
    clocks="FIXED_ENDPOINTS_WITH_OPTIONAL_0930_1300_BOUNDARY_ANCHORS_NO_COMPRESSION",
    returns="EXACT_BIN_ENDPOINTS", volume="ALL_DECLARED_D_CLOCKS_AND_240_CORE_CLOCKS_KNOWN_POSITIVE_TOTAL",
    medians="ORIGINAL_19_FROZEN_NEW_32_STRUCTURE_ONLY", policy=POLICY_SHA256))
ARMS = ("candidate", "matched")
__all__ = ["FEATURES", "KEY", "MINUTE_FEATURES", "ORDERED_FEATURES", "RAW_FEATURES", "FIELDS",
    "PINS", "POLICY", "POLICY_SHA256", "PARAMETERS", "SCHEMA", "SCHEMA_SHA256", "DIMENSIONS", "ARMS",
    "GenericPrice5TDConfigurationV1", "MinuteSourceIdentityV1", "check_resource_budget_v1"]
