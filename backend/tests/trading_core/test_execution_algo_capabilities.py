from __future__ import annotations

import pytest

from backend.services.trading_core.errors import AlgoRealtimeUnsupportedError, StrategyPackageValidationError
from backend.services.trading_core.execution_algo_capabilities import (
    get_execution_algo_capability,
    require_execution_algo_supports_mode,
    required_minute_bars_for_policy,
)
from backend.services.trading_core.execution_algo_retirement import ExecutionAlgoRetiredError


def test_required_minute_bars_use_registered_capability_defaults() -> None:
    assert required_minute_bars_for_policy({"algo_code": "TWAP", "algo_config": {}}, package_id="pkg") == 1
    assert required_minute_bars_for_policy({"algo_code": "V24_PLAN", "algo_config": {}}, package_id="pkg") == 31


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


def test_v25_capability_is_retired_from_active_factory() -> None:
    with pytest.raises(ExecutionAlgoRetiredError):
        get_execution_algo_capability("V25_TWO_STAGE")


def test_v25_1_small_cap_capability_is_retired_from_active_factory() -> None:
    with pytest.raises(ExecutionAlgoRetiredError):
        get_execution_algo_capability("V25_1_SMALL_CAP")


def test_realtime_v25_1_small_cap_is_retired_without_fallback() -> None:
    with pytest.raises(ExecutionAlgoRetiredError):
        require_execution_algo_supports_mode(
            {"algo_code": "V25_1_SMALL_CAP", "algo_config": {}},
            mode="LIVE_ONLY",
            package_id="pkg",
        )


def test_realtime_v25_is_retired_without_fallback() -> None:
    with pytest.raises(ExecutionAlgoRetiredError):
        require_execution_algo_supports_mode(
            {"algo_code": "V25_TWO_STAGE", "algo_config": {}},
            mode="LIVE_ONLY",
            package_id="pkg",
        )


def test_unknown_algorithm_is_not_declared_live_safe_by_default() -> None:
    with pytest.raises(AlgoRealtimeUnsupportedError):
        require_execution_algo_supports_mode(
            {"algo_code": "CUSTOM_RESEARCH_ALGO", "algo_config": {}},
            mode="LIVE_ONLY",
            package_id="pkg",
        )
