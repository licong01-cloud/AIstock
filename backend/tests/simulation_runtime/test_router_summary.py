from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime
from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
)
from backend.services.simulation_runtime.models import (
    ExecutionPlan,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    canonical_json_sha256,
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
            "miniqmt_shadow": {
                "env_var": "MINIQMT_SHADOW_ENABLED",
                "enabled": True,
                "default": False,
                "mode": "dry_run_no_broker_mutation",
            },
        }


def _shadow_payload(*, runtime_id: str = "mqrt_shadow_scope", fatal: bool = False) -> dict:
    difference = {
        "severity": "FATAL" if fatal else "WARNING",
        "reason_code": "MINIQMT_SHADOW_CHILD_ORDER_COUNT_DRIFT" if fatal else "MINIQMT_SHADOW_PRICE_ROUNDING_DRIFT",
        "message": "shadow difference for router summary regression",
        "context": {"symbol": "000001.SZ"},
    }
    return {
        "report_id": f"mqrt_shadow_{runtime_id}_delay",
        "runtime_id": runtime_id,
        "durable_event_id": "mqrtevt_shadow_scope_1",
        "scenario": "delay",
        "differences": [difference],
        "metadata": {
            "portfolio_id": "portfolio_l16",
            "strategy_slot_id": "slot_l16",
            "binding_id": "binding_l16",
            "run_id": "run_l16",
            "trade_date": "2026-06-25",
            "execution_plan_id": "plan_l16",
            "account_group_id": "ag_minqmt_sim",
        },
    }


def _runtime(runtime_id: str = "mqrt_shadow_scope", *, metadata: dict | None = None) -> MiniQMTExecutionRuntimeRecord:
    return MiniQMTExecutionRuntimeRecord(
        runtime_id=runtime_id,
        account_group_id="ag_minqmt_sim",
        trade_date=date(2026, 6, 25),
        event_loop_state=MiniQMTExecutionRuntimeState.READY,
        runtime_config_hash="runtime_hash_shadow_scope",
        metadata=metadata or {},
    )


def _shadow_repo(*, fatal: bool = False) -> InMemoryMiniQMTExecutionRuntimeRepository:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    payload = _shadow_payload(fatal=fatal)
    repo.upsert_runtime(
        _runtime(
            metadata={
                "gray_runtime_overrides": {"portfolio_l16::slot_l16": "event_loop"},
                "last_shadow_reconciliation": payload,
            }
        )
    )
    repo.append_event(
        MiniQMTExecutionEvent(
            event_id="mqrtevt_shadow_scope_1",
            runtime_id="mqrt_shadow_scope",
            sequence=1,
            event_type=MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED,
            event_time=datetime(2026, 6, 25, 9, 30, tzinfo=UTC),
            source="shadow",
            payload=payload,
        )
    )
    return repo


def _client(
    *,
    shadow_repo: InMemoryMiniQMTExecutionRuntimeRepository | JsonFileMiniQMTExecutionRuntimeRepository | None = None,
    simulation_repo: InMemorySimulationRuntimeRepository | None = None,
    service: SimulationRuntimeOpsService | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    runtime_repo = shadow_repo or InMemoryMiniQMTExecutionRuntimeRepository()
    sim_repo = simulation_repo or InMemorySimulationRuntimeRepository()
    ops_service = service or SimulationRuntimeOpsService(repository=sim_repo)
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = lambda: ops_service
    app.dependency_overrides[simulation_runtime.get_miniqmt_runtime_repository] = lambda: runtime_repo
    return TestClient(app)


def test_miniqmt_shadow_evidence_endpoint_returns_scope_severity_and_fatal_marker() -> None:
    client = _client(shadow_repo=_shadow_repo(fatal=True))

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["count"] == 1
    assert body["fatal_count"] == 1
    row = body["evidence"][0]
    assert row["event_id"] == "mqrtevt_shadow_scope_1"
    assert row["severity"] == "FATAL"
    assert row["has_fatal_difference"] is True
    assert row["scope"]["portfolio_id"] == "portfolio_l16"
    assert row["scope"]["strategy_slot_id"] == "slot_l16"
    assert row["scope"]["execution_plan_id"] == "plan_l16"


def test_miniqmt_shadow_evidence_endpoint_returns_empty_count_when_no_evidence() -> None:
    client = _client()

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    )

    assert response.status_code == 200
    assert response.json()["count"] == 0
    assert response.json()["evidence"] == []


def test_miniqmt_shadow_evidence_endpoint_includes_failed_observation_only_run_payload() -> None:
    simulation_repo = InMemorySimulationRuntimeRepository()
    plan_payload = {"schema_version": "test_execution_plan_v1", "case": "shadow_failure"}
    plan_hash = canonical_json_sha256(plan_payload)
    plan_id = f"plan_{plan_hash[:16]}"
    simulation_repo.execution_plans[plan_id] = ExecutionPlan(
        plan_id=plan_id,
        strategy_id="strategy_l16",
        portfolio_id="portfolio_l16",
        package_id="pkg_l16",
        release_id="release_l16",
        release_hash="release_hash_l16",
        binding_id="binding_l16",
        binding_hash="binding_hash_l16",
        account_group_id="ag_minqmt_sim",
        strategy_slot_id="slot_l16",
        selection_evidence_id="evidence_l16",
        selection_evidence_hash="evidence_hash_l16",
        target_trade_date=date(2026, 6, 25),
        execution_policy_version_id="policy_v1",
        execution_policy_sha256="policy_hash",
        tail_policy_version_id="tail_v1",
        tail_policy_sha256="tail_hash",
        intents=[],
        trading_rule_decisions=[],
        plan_payload_json=plan_payload,
        plan_hash=plan_hash,
    )
    simulation_repo.daily_runs["run_l16"] = SimulationDailyRun(
        run_id="run_l16",
        trade_date=date(2026, 6, 25),
        strategy_id="strategy_l16",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        package_id="pkg_l16",
        manifest_sha256="manifest_hash_l16",
        release_id="release_l16",
        release_hash="release_hash_l16",
        binding_id="binding_l16",
        binding_hash="binding_hash_l16",
        account_group_id="ag_minqmt_sim",
        strategy_slot_id="slot_l16",
        execution_plan_id=plan_id,
        execution_plan_hash=plan_hash,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        run_payload_json={
            "miniqmt_shadow_reconciliation": {
                "status": "FAILED_OBSERVATION_ONLY",
                "reason_code": "MINIQMT_SHADOW_RECONCILIATION_FAILED",
                "run_id": "run_l16",
                "binding_id": "binding_l16",
                "trade_date": "2026-06-25",
                "execution_plan_id": plan_id,
                "account_group_id": "ag_minqmt_sim",
                "message": "shadow failed loud but B submit continued",
            }
        },
    )
    client = _client(simulation_repo=simulation_repo)

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    row = body["evidence"][0]
    assert row["source"] == "simulation_run_payload"
    assert row["status"] == "FAILED_OBSERVATION_ONLY"
    assert row["reason_code"] == "MINIQMT_SHADOW_RECONCILIATION_FAILED"
    assert row["scope"]["portfolio_id"] == "portfolio_l16"
    assert row["scope"]["strategy_slot_id"] == "slot_l16"


def test_miniqmt_shadow_evidence_endpoint_rejects_missing_required_scope() -> None:
    client = _client()

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16"},
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["error_code"] == "MINIQMT_RUNTIME_QUERY_PARAMETER_REQUIRED"
    assert detail["context"]["field"] == "strategy_slot_id"


def test_miniqmt_runtime_events_endpoint_is_loud_when_runtime_missing() -> None:
    client = _client()

    response = client.get("/api/v1/simulation-runtime/miniqmt/runtime-events", params={"runtime_id": "missing_runtime"})

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["error_code"] == "DATA_UNAVAILABLE"
    assert detail["context"]["reason_code"] == "MINIQMT_RUNTIME_NOT_FOUND"
    assert detail["context"]["runtime_id"] == "missing_runtime"


def test_miniqmt_gray_state_reflects_override_and_last_shadow_reconciliation() -> None:
    client = _client(shadow_repo=_shadow_repo(fatal=False))

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/gray-state",
        params={"portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["override_runtime_kind"] == "event_loop"
    assert body["effective_runtime_kind"] == "event_loop"
    assert body["last_shadow_reconciliation"]["durable_event_id"] == "mqrtevt_shadow_scope_1"
    assert body["last_shadow_reconciliation"]["severity"] == "WARNING"


def test_miniqmt_runtime_readonly_endpoints_do_not_mutate_json_store(tmp_path) -> None:
    store_path = tmp_path / "runtime-state.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    payload = _shadow_payload()
    repo.upsert_runtime(_runtime(metadata={"last_shadow_reconciliation": payload}))
    repo.append_event(
        MiniQMTExecutionEvent(
            event_id="mqrtevt_shadow_scope_1",
            runtime_id="mqrt_shadow_scope",
            sequence=1,
            event_type=MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED,
            event_time=datetime(2026, 6, 25, 9, 30, tzinfo=UTC),
            source="shadow",
            payload=payload,
        )
    )
    before = store_path.read_text(encoding="utf-8")
    client = _client(shadow_repo=repo)

    assert client.get("/api/v1/simulation-runtime/miniqmt/runtime-events", params={"runtime_id": "mqrt_shadow_scope"}).status_code == 200
    assert client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    ).status_code == 200
    assert client.get(
        "/api/v1/simulation-runtime/miniqmt/gray-state",
        params={"portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    ).status_code == 200

    assert store_path.read_text(encoding="utf-8") == before


def test_scheduler_status_exposes_miniqmt_shadow_flag() -> None:
    service = SimulationRuntimeOpsService(repository=InMemorySimulationRuntimeRepository(), scheduler=_StaticScheduler())
    client = _client(service=service)

    response = client.get("/api/v1/simulation-runtime/scheduler/status")

    assert response.status_code == 200
    scheduler = response.json()["scheduler"]
    shadow = scheduler["miniqmt_shadow"]
    assert shadow["env_var"] == "MINIQMT_SHADOW_ENABLED"
    assert shadow["enabled"] is True
    assert scheduler["running"] is True
    assert scheduler["thread_alive"] is True
    assert scheduler["last_run_at"] == "2026-07-02T13:47:00+08:00"
    assert scheduler["last_result"]["errors"][0]["type"] == "LiveInferencePreflightError"
    assert scheduler["last_result_errors"][0]["context"]["reason_code"] == "strategy_package_model_code_missing"
    assert scheduler["last_error_count"] == 1
