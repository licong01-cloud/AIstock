from __future__ import annotations

from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime
from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
)
from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository


class _StaticScheduler:
    def status(self) -> dict:
        return {
            "scheduler": "simulation_lifecycle_scheduler",
            "running": True,
            "thread_alive": True,
            "last_run_at": "2026-07-02T13:47:00+08:00",
            "last_result": {
                "started_at": "2026-07-02T13:47:00+08:00",
                "errors": [
                    {
                        "type": "LiveInferencePreflightError",
                        "message": "strategy package model code missing",
                        "context": {"reason_code": "strategy_package_model_code_missing"},
                    }
                ],
            },
            "selection_inference": {"authority": "unit_test_static_scheduler"},
            "binding_watchdog": {"authority": "unit_test_static_scheduler"},
            "miniqmt_sim_runtime": {"runtime_route": "A_EVENT_LOOP"},
            "miniqmt_quote_context": {"authority": "unit_test_static_scheduler"},
            "miniqmt_quote_ingress_activation": {"authority": "unit_test_static_scheduler"},
            "b0_quote_v2_controllers": {"controllers": {}},
        }


def _runtime(runtime_id: str = "mqrt_event_loop_scope", *, metadata: dict | None = None) -> MiniQMTExecutionRuntimeRecord:
    return MiniQMTExecutionRuntimeRecord(
        runtime_id=runtime_id,
        account_group_id="ag_minqmt_sim",
        trade_date=date(2026, 6, 25),
        event_loop_state=MiniQMTExecutionRuntimeState.READY,
        runtime_config_hash="runtime_hash_event_loop_scope",
        metadata=metadata or {},
    )


def _runtime_repo() -> InMemoryMiniQMTExecutionRuntimeRepository:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime(metadata={"compiler_route_retired": True, "runtime_route": "A_EVENT_LOOP"}))
    return repo


def _client(
    *,
    runtime_repo: InMemoryMiniQMTExecutionRuntimeRepository | JsonFileMiniQMTExecutionRuntimeRepository | None = None,
    simulation_repo: InMemorySimulationRuntimeRepository | None = None,
    service: SimulationRuntimeOpsService | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    runtime_repo = runtime_repo or InMemoryMiniQMTExecutionRuntimeRepository()
    sim_repo = simulation_repo or InMemorySimulationRuntimeRepository()
    ops_service = service or SimulationRuntimeOpsService(repository=sim_repo)
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = lambda: ops_service
    app.dependency_overrides[simulation_runtime.get_miniqmt_runtime_repository] = lambda: runtime_repo
    return TestClient(app)


def test_scheduler_status_omits_retired_miniqmt_shadow_flag() -> None:
    service = SimulationRuntimeOpsService(repository=InMemorySimulationRuntimeRepository(), scheduler=_StaticScheduler())
    client = _client(service=service)

    response = client.get("/api/v1/simulation-runtime/scheduler/status")

    assert response.status_code == 200
    scheduler = response.json()["scheduler"]
    assert "miniqmt_shadow" not in scheduler
    assert scheduler["running"] is True
    assert scheduler["thread_alive"] is True
    assert scheduler["last_run_at"] == "2026-07-02T13:47:00+08:00"
    assert scheduler["last_result"]["errors"][0]["type"] == "LiveInferencePreflightError"
    assert scheduler["last_result_errors"][0]["context"]["reason_code"] == "strategy_package_model_code_missing"
    assert scheduler["last_error_count"] == 1
