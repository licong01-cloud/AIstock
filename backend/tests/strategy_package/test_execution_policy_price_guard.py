from __future__ import annotations

import pytest

from backend.services.strategy_package.execution_policy import (
    compute_execution_policy_sha256,
    normalize_execution_policy_json,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


def _base_policy() -> dict:
    return {"algo_code": "TWAP", "algo_config": {"max_participation_rate": 0.1}}


def test_s1_3_policy_hash_stable_and_disabled_key_is_distinct() -> None:
    base = normalize_execution_policy_json(_base_policy())
    disabled = normalize_execution_policy_json({**_base_policy(), "price_guard": {"enabled": False}})

    assert compute_execution_policy_sha256(base) == compute_execution_policy_sha256(base)
    assert compute_execution_policy_sha256(base) != compute_execution_policy_sha256(disabled)
    assert "price_guard" not in base
    assert disabled["price_guard"] == {"enabled": False}


def test_s1_3_validator_allows_guard_contract_keys_and_rejects_unknown() -> None:
    policy = normalize_execution_policy_json(
        {
            **_base_policy(),
            "price_guard": {"enabled": True, "mode": "rule_v1", "price_basis": "raw"},
            "exit_guard": {"enabled": True, "mode": "rule_v1", "t1_handling": "defer_to_next_tradable_day"},
        }
    )

    assert policy["price_guard"]["mode"] == "rule_v1"
    assert policy["exit_guard"]["mode"] == "rule_v1"

    with pytest.raises(RuntimeConfigInvalidError, match="unsupported fields"):
        normalize_execution_policy_json({**_base_policy(), "price_guard": {"unknown": 1}})


def test_s1_3_validator_rejects_algo_config_guard_parameter_smuggling() -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="algo_config must not carry"):
        normalize_execution_policy_json(
            {
                "algo_code": "TWAP",
                "algo_config": {
                    "max_participation_rate": 0.1,
                    "max_open_gap_bps": 500,
                },
            }
        )

    with pytest.raises(RuntimeConfigInvalidError, match="algo_config must not carry"):
        normalize_execution_policy_json(
            {
                "algo_code": "TWAP",
                "algo_config": {"nested": {"stop_loss": {"enabled": True}}},
            }
        )
