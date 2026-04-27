from __future__ import annotations

import pytest

from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.trading_core.execution_algo_capabilities import required_minute_bars_for_policy


def test_required_minute_bars_use_registered_capability_defaults() -> None:
    assert required_minute_bars_for_policy({"algo_code": "TWAP", "algo_config": {}}, package_id="pkg") == 1
    assert required_minute_bars_for_policy({"algo_code": "V24_PLAN", "algo_config": {}}, package_id="pkg") == 31
    assert required_minute_bars_for_policy({"algo_code": "V25_TWO_STAGE", "algo_config": {}}, package_id="pkg") == 240


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
