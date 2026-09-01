from __future__ import annotations

import pytest

from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_context
from backend.services.trading_core.execution_algo_adapter import ExecutionAlgoAdapter
from backend.services.trading_core.execution_algo_capabilities import required_minute_bars_for_policy
from backend.services.trading_core.execution_algo_retirement import (
    RETIRED_EXECUTION_ALGO_CODES,
    V25_EXECUTION_ALGO_RETIRED,
    ExecutionAlgoRetiredError,
)
from backend.services.trading_core.models import Order, OrderSide, OrderType


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_paper_preflight_rejects_v25_even_with_min_bar_override(algo_code: str) -> None:
    with pytest.raises(ExecutionAlgoRetiredError) as exc_info:
        required_minute_bars_for_policy(
            {"algo_code": algo_code, "algo_config": {"min_required_bars": 1}},
            package_id="pkg_retired",
        )
    assert exc_info.value.context["reason_code"] == V25_EXECUTION_ALGO_RETIRED


def test_paper_adapter_rejects_before_runtime_construction() -> None:
    order = Order(
        order_id="order_retired",
        intent_id="intent_retired",
        package_id="pkg_retired",
        portfolio_id="portfolio_retired",
        symbol="600000.SH",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100,
    )
    with pytest.raises(ExecutionAlgoRetiredError):
        ExecutionAlgoAdapter().create_state(order, "V25_TWO_STAGE", {})


def test_localsim_existing_v25_source_policy_still_compiles_to_explicit_twap() -> None:
    from backend.services.strategy_package.execution_policy import (
        compute_execution_policy_sha256,
        normalize_execution_policy_json,
    )

    requested = normalize_execution_policy_json({"algo_code": "V25_TWO_STAGE", "algo_config": {}})
    context = local_sim_twap_only_policy_context(
        {
            "validated_execution_policy_id": "historical_v25_policy",
            "policy_sha256": compute_execution_policy_sha256(requested),
            "policy_json": requested,
            "activation_source": "historical_read_only",
        }
    )
    assert context["algo_code"] == "TWAP"
    assert context["runtime_policy_selection"]["requested_algo_code"] == "V25_TWO_STAGE"
    assert context["runtime_policy_selection"]["fallback_used"] is False
