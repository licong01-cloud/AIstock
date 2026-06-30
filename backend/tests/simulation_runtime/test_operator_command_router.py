from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntimeClient,
    MiniQMTOperatorCommandStatus,
)
from backend.services.simulation_runtime import InMemorySimulationRuntimeRepository, SimulationRuntimeOpsService


def _client(
    *,
    gateway: FakeMiniQMTGateway | None = None,
    runtime_client: MiniQMTExecutionRuntimeClient | None = None,
    ops_service: SimulationRuntimeOpsService | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    broker_gateway = gateway or FakeMiniQMTGateway()
    app.dependency_overrides[simulation_runtime.get_miniqmt_gateway] = lambda: broker_gateway
    app.dependency_overrides[simulation_runtime.get_miniqmt_runtime_client] = (
        lambda: runtime_client
        or MiniQMTExecutionRuntimeClient(repository=InMemoryMiniQMTExecutionRuntimeRepository())
    )
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = (
        lambda: ops_service or SimulationRuntimeOpsService(repository=InMemorySimulationRuntimeRepository())
    )
    return TestClient(app)


def _payload(command_type: str) -> dict:
    return {
        "command_id": f"opcmd_router_{command_type.lower()}",
        "command_type": command_type,
        "account_group_id": "ag_minqmt_main_sim",
        "trade_date": "2026-06-09",
        "runtime_config_hash": "runtime_hash_router_operator",
        "runtime_id": "mqrt_router_operator",
        "strategy_slot_id": "slot_alpha_router",
        "reason": "router operator command regression",
    }


def test_operator_command_router_requires_confirmation_for_destructive_commands() -> None:
    gateway = FakeMiniQMTGateway()
    client = _client(gateway=gateway)

    response = client.post(
        "/api/v1/simulation-runtime/miniqmt/operator-commands",
        json=_payload("FLATTEN_ALL_POSITIONS"),
    )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["error_code"] == "MINIQMT_OPERATOR_CONFIRMATION_REQUIRED"
    assert detail["context"]["expected_confirm_text"] == "EXECUTE FLATTEN_ALL_POSITIONS"
    assert gateway.submitted_orders == []
    assert gateway.cancelled_orders == []


def test_operator_command_router_flattens_positions_through_runtime_gateway() -> None:
    gateway = FakeMiniQMTGateway(
        positions=[
            {
                "symbol": "000001.SZ",
                "quantity": 1000,
                "available_quantity": 600,
                "last_price": 10.25,
                "strategy_slot_id": "slot_alpha_router",
            }
        ]
    )
    client = _client(gateway=gateway)
    payload = _payload("FLATTEN_ALL_POSITIONS")
    payload["confirm_text"] = "EXECUTE FLATTEN_ALL_POSITIONS"

    response = client.post("/api/v1/simulation-runtime/miniqmt/operator-commands", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["result"]["status"] == MiniQMTOperatorCommandStatus.EXECUTED.value
    assert body["result"]["submitted_child_order_ids"]
    assert body["runtime_evidence"]["runtime_owner"] == "MiniQMTExecutionRuntime"
    assert body["runtime_evidence"]["submitted_child_count"] == 1
    assert gateway.submitted_orders[0].symbol == "000001.SZ"
    assert gateway.submitted_orders[0].quantity == 600


def test_operator_command_router_replace_alpha_is_runtime_audit_not_broker_submit() -> None:
    gateway = FakeMiniQMTGateway()
    client = _client(gateway=gateway)
    payload = _payload("REPLACE_ALPHA_SIGNAL_BOOK")
    payload["alpha_signal_book_id"] = "asb_router_20260609_v2"

    response = client.post("/api/v1/simulation-runtime/miniqmt/operator-commands", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["result"]["status"] == MiniQMTOperatorCommandStatus.EXECUTED.value
    assert body["result"]["alpha_signal_book_id"] == "asb_router_20260609_v2"
    assert body["result"]["metadata"]["execution_layer_mutated"] is False
    assert body["runtime_evidence"]["submitted_child_count"] == 0
    assert gateway.submitted_orders == []


def test_operator_command_router_requires_confirmation_and_run_id_for_stale_recovery() -> None:
    gateway = FakeMiniQMTGateway()
    client = _client(gateway=gateway)
    payload = _payload("RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT")

    missing_confirmation = client.post("/api/v1/simulation-runtime/miniqmt/operator-commands", json=payload)
    assert missing_confirmation.status_code == 409
    assert (
        missing_confirmation.json()["detail"]["context"]["expected_confirm_text"]
        == "EXECUTE RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT"
    )

    payload["confirm_text"] = "EXECUTE RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT"
    missing_run = client.post("/api/v1/simulation-runtime/miniqmt/operator-commands", json=payload)
    assert missing_run.status_code == 422
    assert missing_run.json()["detail"]["error_code"] == "MINIQMT_OPERATOR_RUN_ID_REQUIRED"
