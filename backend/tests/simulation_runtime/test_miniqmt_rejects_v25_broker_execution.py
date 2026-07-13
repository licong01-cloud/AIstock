from __future__ import annotations

import pytest

from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.simulation_runtime import (
    ExecutionPathNotCanonicalError,
    MiniQMTExecutionBridge,
    MiniQMTUnsupportedExecutionAlgoError,
    SimulationBrokerBackend,
)
from backend.tests.simulation_runtime.test_target_rebalance_shared import _compiled_plan_for_bridge


@pytest.mark.parametrize("algo_code", ["V25_1_SMALL_CAP", "V25_TWO_STAGE"])
def test_miniqmt_bridge_rejects_v25_broker_execution_before_request_build(algo_code: str) -> None:
    _release, binding, plan = _compiled_plan_for_bridge(backend=SimulationBrokerBackend.MINIQMT_SIM)
    plan = plan.model_copy(
        update={
            "execution_policy_version_id": f"exec_policy_{algo_code.lower()}",
            "plan_payload_json": {
                **plan.plan_payload_json,
                "execution_policy": {
                    **plan.plan_payload_json["execution_policy"],
                    "version_id": f"exec_policy_{algo_code.lower()}",
                    "payload": {"algo_code": algo_code, "schedule_window": {"mode": "open_to_close"}},
                },
            },
        }
    )
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    with pytest.raises(MiniQMTUnsupportedExecutionAlgoError) as exc_info:
        bridge.build_managed_order_requests(plan=plan, binding=binding)

    assert exc_info.value.error_code == "MINIQMT_UNSUPPORTED_EXECUTION_ALGO"
    assert exc_info.value.context["broker_backend"] == "minqmt_sim"
    assert exc_info.value.context["inferred_algo_code"] == algo_code
    assert "SNIPER_MINIQMT" in exc_info.value.context["required_action"]


def test_miniqmt_bridge_rejects_v25_policy_id_without_silent_direct_order_fallback() -> None:
    _release, binding, plan = _compiled_plan_for_bridge(backend=SimulationBrokerBackend.MINIQMT_SIM)
    plan = plan.model_copy(
        update={
            "execution_policy_version_id": "legacy:V25_1_SMALL_CAP:paper_v2",
            "plan_payload_json": {
                **plan.plan_payload_json,
                "execution_policy": {
                    "version_id": "legacy:V25_1_SMALL_CAP:paper_v2",
                    "sha256": "sha_v25_policy_id_only",
                    "payload": {"schedule_window": {"mode": "open_to_close"}},
                },
            },
        }
    )
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    with pytest.raises(MiniQMTUnsupportedExecutionAlgoError) as exc_info:
        bridge.build_managed_order_requests(plan=plan, binding=binding)

    assert exc_info.value.context["inferred_algo_code"] == "V25_1_SMALL_CAP"
    assert exc_info.value.context["payload_has_policy_json"] is False


def test_miniqmt_bridge_rejects_non_vnpy_policy_before_preview_or_submit() -> None:
    _release, binding, plan = _compiled_plan_for_bridge(backend=SimulationBrokerBackend.MINIQMT_SIM)
    plan = plan.model_copy(
        update={
            "execution_policy_version_id": "exec_policy_close_price",
            "plan_payload_json": {
                **plan.plan_payload_json,
                "execution_policy": {
                    **plan.plan_payload_json["execution_policy"],
                    "version_id": "exec_policy_close_price",
                    "payload": {"algo_code": "CLOSE_PRICE", "policy_json": {"algo_code": "CLOSE_PRICE", "algo_config": {}}},
                },
            },
        }
    )
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    for method_name in ("build_managed_order_requests", "preview_plan", "submit_plan"):
        with pytest.raises(ExecutionPathNotCanonicalError) as exc_info:
            getattr(bridge, method_name)(plan=plan, binding=binding)
        assert exc_info.value.error_code == "EXECUTION_PATH_NOT_CANONICAL"
        assert exc_info.value.context["inferred_algo_code"] == "CLOSE_PRICE"
