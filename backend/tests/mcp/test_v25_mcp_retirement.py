from __future__ import annotations

import pytest

from backend.routers.execution_policy import (
    ExecutionPolicyBindingPlanRequest,
    _algo_catalog,
    _validate_policy_contract,
    binding_plan,
    get_algo,
)
from backend.services.trading_core.execution_algo_retirement import (
    RETIRED_EXECUTION_ALGO_CODES,
    V25_EXECUTION_ALGO_RETIRED,
)


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_execution_policy_catalog_projects_single_retirement_authority(algo_code: str) -> None:
    catalog = _algo_catalog()
    assert catalog[algo_code]["retired"] is True
    assert catalog[algo_code]["selectable"] is False
    assert catalog[algo_code]["activatable"] is False
    assert catalog[algo_code]["retirement_reason_code"] == V25_EXECUTION_ALGO_RETIRED
    assert get_algo(algo_code)["algo"]["algo_code"] == algo_code


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_mcp_binding_plan_cannot_create_or_activate_v25(algo_code: str) -> None:
    policy = {"algo_code": algo_code, "algo_config": {}}
    assert _validate_policy_contract(policy) == [V25_EXECUTION_ALGO_RETIRED]
    result = binding_plan(
        ExecutionPolicyBindingPlanRequest(
            package_id="db_must_not_be_read",
            policy_name="retired",
            policy_json=policy,
            source_backtest_id="historical",
            source_backtest_status="SUCCEEDED",
        )
    )
    assert result["blockers"] == [V25_EXECUTION_ALGO_RETIRED]
    assert result["will_create_policy"] is False
    assert result["will_enable_for_paper"] is False


def test_mcp_non_execution_labels_do_not_trigger_retirement() -> None:
    blockers = _validate_policy_contract(
        {
            "algo_code": "TWAP",
            "algo_config": {"report_label": "V25_TWO_STAGE", "universe": "V25_1_SMALL_CAP"},
            "data_requirements": {
                "requires_minute_bar": True,
                "requires_pre_close": True,
                "requires_limit_price": True,
                "requires_suspend_status": True,
            },
        }
    )
    assert V25_EXECUTION_ALGO_RETIRED not in blockers
