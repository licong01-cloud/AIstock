from __future__ import annotations

from typing import Final, Mapping

from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    IDENTITY_COLUMNS,
    MARKET_FEATURE_COLUMNS as V1_MARKET_FEATURE_COLUMNS,
    PARENT_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.suspension_aware_bar_policy import (
    BAR_POLICY_PAYLOAD,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256

FEATURE_SCHEMA_VERSION: Final = "advisory_feature_schema_v2_suspension_aware"

SUSPENSION_FEATURE_COLUMNS: Final = (
    "suspend_session_count_5",
    "suspend_session_count_20",
    "suspend_session_count_60",
    "suspend_fraction_20",
    "suspend_fraction_60",
    "sessions_since_last_suspend",
    "current_bar_synthetic",
    "zero_liquidity_window_5",
    "zero_liquidity_window_20",
)

MARKET_FEATURE_COLUMNS: Final = (*V1_MARKET_FEATURE_COLUMNS, *SUSPENSION_FEATURE_COLUMNS)

SUSPENSION_CONDITIONAL_OPTIONAL_COLUMNS: Final = (
    "volume_ratio_5",
    "volume_ratio_20",
    "amount_ratio_5",
    "amount_ratio_20",
    "decision_limit_up",
    "decision_limit_down",
    "distance_to_limit_up",
    "distance_to_limit_down",
)

_REQUIRED_MARKET_COLUMNS: Final = (
    "ret_1",
    "ret_3",
    "ret_5",
    "ret_10",
    "ret_20",
    "drawdown_20",
    "drawdown_60",
    "atr14_close",
    "intraday_range",
    "open_gap",
    "csi300_ret_1",
    "csi300_ret_5",
    "csi300_ret_20",
    "market_up_ratio",
    "market_limit_up_ratio",
    "market_cross_section_vol",
    "decision_is_suspended",
    *SUSPENSION_FEATURE_COLUMNS,
)
REQUIRED_FEATURE_COLUMNS: Final = (*PARENT_FEATURE_COLUMNS, *_REQUIRED_MARKET_COLUMNS)
OPTIONAL_FEATURE_COLUMNS: Final = tuple(
    column for column in MARKET_FEATURE_COLUMNS if column not in REQUIRED_FEATURE_COLUMNS
)
MISSING_INDICATOR_COLUMNS: Final = tuple(f"{column}__missing" for column in OPTIONAL_FEATURE_COLUMNS)
MODEL_FEATURE_COLUMNS: Final = (
    *PARENT_FEATURE_COLUMNS,
    *MARKET_FEATURE_COLUMNS,
    *MISSING_INDICATOR_COLUMNS,
)

FEATURE_SCHEMA_BASE_PAYLOAD: Final = {
    "schema_version": FEATURE_SCHEMA_VERSION,
    "identity_columns": IDENTITY_COLUMNS,
    "model_feature_columns": MODEL_FEATURE_COLUMNS,
    "required_feature_columns": REQUIRED_FEATURE_COLUMNS,
    "optional_feature_columns": OPTIONAL_FEATURE_COLUMNS,
    "categorical_feature_columns": CATEGORICAL_FEATURE_COLUMNS,
    "missing_indicator_columns": MISSING_INDICATOR_COLUMNS,
    "bar_policy": BAR_POLICY_PAYLOAD,
}


def build_feature_schema_payload(
    *,
    market_calendar_identity: Mapping[str, object],
    suspend_sidecar_identity: Mapping[str, object],
) -> dict[str, object]:
    if not market_calendar_identity or not suspend_sidecar_identity:
        raise ValueError("feature schema v2 requires calendar and suspend sidecar identities")
    return {
        **FEATURE_SCHEMA_BASE_PAYLOAD,
        "market_calendar_identity": dict(market_calendar_identity),
        "suspend_sidecar_identity": dict(suspend_sidecar_identity),
    }


def feature_schema_hash(
    *,
    market_calendar_identity: Mapping[str, object],
    suspend_sidecar_identity: Mapping[str, object],
) -> str:
    return canonical_json_sha256(
        build_feature_schema_payload(
            market_calendar_identity=market_calendar_identity,
            suspend_sidecar_identity=suspend_sidecar_identity,
        )
    )
