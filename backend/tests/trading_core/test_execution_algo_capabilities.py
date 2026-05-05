from __future__ import annotations

import pytest

from backend.services.trading_core.errors import AlgoRealtimeUnsupportedError, StrategyPackageValidationError
from backend.services.trading_core.execution_algo_capabilities import (
    get_execution_algo_capability,
    require_execution_algo_supports_mode,
    required_minute_bars_for_policy,
)


def test_required_minute_bars_use_registered_capability_defaults() -> None:
    assert required_minute_bars_for_policy({"algo_code": "TWAP", "algo_config": {}}, package_id="pkg") == 1
    assert required_minute_bars_for_policy({"algo_code": "V24_PLAN", "algo_config": {}}, package_id="pkg") == 31
    assert required_minute_bars_for_policy({"algo_code": "V25_TWO_STAGE", "algo_config": {}}, package_id="pkg") == 240
    assert required_minute_bars_for_policy({"algo_code": "V25_1_SMALL_CAP", "algo_config": {}}, package_id="pkg") == 240


def test_required_minute_bars_support_validated_policy_override() -> None:
    assert required_minute_bars_for_policy(
        {"algo_code": "TWAP", "algo_config": {"min_required_bars": 15}},
        package_id="pkg",
    ) == 15


def test_required_minute_bars_reject_invalid_override() -> None:
    with pytest.raises(StrategyPackageValidationError, match="positive"):
        required_minute_bars_for_policy(
            {"algo_code": "TWAP", "algo_config": {"min_required_bars": 0}},
            package_id="pkg",
        )


def test_capabilities_split_historical_and_live_requirements() -> None:
    v25 = get_execution_algo_capability("V25_TWO_STAGE")
    assert v25.historical_min_required_bars == 240
    assert v25.plan_horizon_bars == 240
    assert v25.live_min_start_bars == 1
    assert v25.live_supported is True
    assert v25.live_step_mode == "persisted_plan"


def test_v25_1_small_cap_capability_matches_v25_horizon() -> None:
    cap = get_execution_algo_capability("V25_1_SMALL_CAP")
    assert cap.historical_min_required_bars == 240
    assert cap.plan_horizon_bars == 240
    assert cap.live_min_start_bars == 1
    assert cap.live_supported is True
    assert cap.live_step_mode == "persisted_plan"
    assert "early_model_path" in cap.runtime_asset_keys
    assert "late_model_path" in cap.runtime_asset_keys


def test_realtime_v25_1_small_cap_is_declared_streaming_without_fallback() -> None:
    capability = require_execution_algo_supports_mode(
        {"algo_code": "V25_1_SMALL_CAP", "algo_config": {}},
        mode="LIVE_ONLY",
        package_id="pkg",
    )
    assert capability.algo_code == "V25_1_SMALL_CAP"
    assert capability.live_step_mode == "persisted_plan"


def test_realtime_v25_is_declared_streaming_without_fallback() -> None:
    capability = require_execution_algo_supports_mode(
        {"algo_code": "V25_TWO_STAGE", "algo_config": {}},
        mode="LIVE_ONLY",
        package_id="pkg",
    )
    assert capability.algo_code == "V25_TWO_STAGE"
    assert capability.live_step_mode == "persisted_plan"


def test_unknown_algorithm_is_not_declared_live_safe_by_default() -> None:
    with pytest.raises(AlgoRealtimeUnsupportedError):
        require_execution_algo_supports_mode(
            {"algo_code": "CUSTOM_RESEARCH_ALGO", "algo_config": {}},
            mode="LIVE_ONLY",
            package_id="pkg",
        )
