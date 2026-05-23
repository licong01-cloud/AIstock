from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime
from backend.services.selection_center.models import SelectionCandidate
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    InMemorySimulationRuntimeRepository,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRunStatus,
    SimulationLifecycleScheduler,
    SimulationRunContext,
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    StrategyPackageSelectionResult,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256

TRADE_DATE = date(2026, 5, 21)


def _candidate_rows() -> list[SelectionCandidate]:
    return [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_quantity=1000,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        )
    ]


def _evidence(release: Any, *, candidates: list[SelectionCandidate]) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": TRADE_DATE.isoformat(),
        "cutoff_date": "2026-05-20",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile_version_id": release.runtime_profile_version_id,
        "runtime_profile_hash": release.runtime_profile_sha256,
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "selected_candidates": [item.model_dump(mode="json") for item in candidates],
        "excluded_candidates": [],
        "valid_no_candidate": False,
        "no_candidate_reason": None,
    }
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=TRADE_DATE,
        cutoff_date=date(2026, 5, 20),
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        runtime_profile_version_id=release.runtime_profile_version_id,
        runtime_profile_hash=release.runtime_profile_sha256,
        source_type="live_inference",
        data_source="DB_HISTORICAL",
        candidate_count=len(candidates),
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
        created_by="unit-test",
    )


class FakeSelectionService:
    def __init__(self, release: Any) -> None:
        self.release = release

    def run_selection(self, **kwargs: Any) -> StrategyPackageSelectionResult:
        candidates = _candidate_rows()
        evidence = _evidence(self.release, candidates=candidates)
        return StrategyPackageSelectionResult(
            runtime_config={
                "runtime_profile": {
                    "selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}
                }
            },
            package_results={self.release.package_id: candidates},
            aggregate_results=candidates,
            excluded_results={self.release.package_id: []},
            manifest_sha256_by_package={self.release.package_id: self.release.manifest_sha256},
            evidence_by_package={self.release.package_id: evidence},
            valid_no_candidate=False,
            no_candidate_reason=None,
        )


@pytest.fixture()
def repo_with_plan() -> tuple[InMemorySimulationRuntimeRepository, str, str]:
    repo = InMemorySimulationRuntimeRepository()
    release_service = StrategyRuntimeReleaseService(repository=repo)
    release = release_service.create_release(
        package_id="pkg_ops",
        manifest_sha256="manifest_ops",
        runtime_profile_id="runtime_profile_ops",
        runtime_profile_version_id="runtime_profile_ops_v1",
        runtime_profile_sha256="runtime_profile_hash_ops",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="ops api test",
    )
    binding = release_service.create_binding(
        strategy_id="strategy_ops",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="ops api test",
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_ops",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0},
                )
            }
        ),
    )
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 25, tzinfo=UTC),
    )
    assert result.planned_count == 1
    run = result.results[0].run
    plan = result.results[0].execution_plan
    assert run is not None
    assert plan is not None
    return repo, run.run_id, plan.plan_id


@pytest.fixture()
def client(repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str]) -> TestClient:
    repo, _, _ = repo_with_plan
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = (
        lambda: SimulationRuntimeOpsService(repository=repo)
    )
    return TestClient(app)


def test_scheduler_status_is_read_only_and_does_not_claim_autostart(client: TestClient) -> None:
    response = client.get("/api/v1/simulation-runtime/scheduler/status")

    assert response.status_code == 200
    scheduler = response.json()["scheduler"]
    assert scheduler["autostart"] is False
    assert scheduler["default_submit"] is False
    assert scheduler["read_only_ops_api"] is True
    assert scheduler["manual_tick_endpoint_enabled"] is False
    assert scheduler["restart_recovery_mode"] == "persisted_state_only"
    assert [window["window_id"] for window in scheduler["schedule_windows"]] == [
        "pre_open",
        "selection",
        "planning",
        "execution",
    ]


def test_list_runs_returns_business_summary_and_filters(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    run_id = repo_with_plan[1]

    response = client.get(
        "/api/v1/simulation-runtime/runs",
        params={"trade_date": TRADE_DATE.isoformat(), "broker_backend": "local_sim"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["run_count"] == 1
    assert payload["summary"]["by_broker_backend"] == {"local_sim": 1}
    assert payload["runs"][0]["run_id"] == run_id
    assert payload["runs"][0]["execution_plan_id"]
    assert payload["runs"][0]["stage_counts"]["execution_plan_intent_count"] == 1
    assert payload["runs"][0]["strategy_performance"]["nav"] == 1.0


def test_run_and_execution_plan_detail_include_traceability(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    _, run_id, plan_id = repo_with_plan

    run_response = client.get(f"/api/v1/simulation-runtime/runs/{run_id}")
    plan_response = client.get(f"/api/v1/simulation-runtime/execution-plans/{plan_id}")

    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["run"]["release_id"].startswith("srr_")
    assert run_payload["selection_evidence"]["evidence_id"].startswith("dse_")
    assert run_payload["execution_plan"]["plan_id"] == plan_id
    assert run_payload["execution_plan"]["buy_intent_count"] == 1
    assert run_payload["run"]["orders"] == []
    assert run_payload["run"]["fills"] == []
    assert run_payload["run"]["errors"] == []

    assert plan_response.status_code == 200
    plan_payload = plan_response.json()["execution_plan"]
    assert plan_payload["plan_id"] == plan_id
    assert plan_payload["intent_count"] == 1
    assert plan_payload["intents"][0]["symbol"] == "000001.SZ"


def test_missing_run_maps_to_404(client: TestClient) -> None:
    response = client.get("/api/v1/simulation-runtime/runs/missing")

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["error_code"] == "DATA_UNAVAILABLE"
    assert detail["context"]["run_id"] == "missing"


def test_invalid_filter_values_are_rejected(client: TestClient) -> None:
    response = client.get("/api/v1/simulation-runtime/runs", params={"broker_backend": "paper"})
    assert response.status_code == 422
    assert response.json()["detail"]["error_code"] == "INVALID_BROKER_BACKEND"

    response = client.get("/api/v1/simulation-runtime/runs", params={"status": "RUNNING"})
    assert response.status_code == 422
    assert response.json()["detail"]["error_code"] == "INVALID_SIMULATION_RUN_STATUS"


def test_live_admission_evidence_requires_successful_dual_simulation_runs() -> None:
    repo = InMemorySimulationRuntimeRepository()
    release_service = StrategyRuntimeReleaseService(repository=repo)
    release = release_service.create_release(
        package_id="pkg_live_admission",
        manifest_sha256="manifest_live_admission",
        runtime_profile_id="runtime_profile_live_admission",
        runtime_profile_version_id="runtime_profile_live_admission_v1",
        runtime_profile_sha256="runtime_profile_live_admission_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="live admission evidence test",
    )
    local_binding = release_service.create_binding(
        strategy_id="strategy_live_admission_local",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="live admission local sim evidence",
    )
    qmt_binding = release_service.create_binding(
        strategy_id="strategy_live_admission_qmt",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="LiveAdmissionQMT",
        order_remark_prefix="live-admission-qmt",
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="live admission miniqmt evidence",
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_live_admission_local",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0},
                ),
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_live_admission_qmt",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0},
                ),
            }
        ),
    )
    local = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    ).results[0]
    qmt = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    ).results[0]
    assert local.run is not None
    assert qmt.run is not None
    repo.update_simulation_daily_run(local.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)
    repo.update_simulation_daily_run(qmt.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)

    service = SimulationRuntimeOpsService(repository=repo)
    evidence = service.build_live_admission_evidence(
        paper_v2_run_id=local.run.run_id,
        miniqmt_sim_run_id=qmt.run.run_id,
        target_broker_backend="minqmt_live",
    )

    assert evidence["sim_validation_evidence"]["paper_v2"]["status"] == "VERIFIED"
    assert evidence["sim_validation_evidence"]["paper_v2"]["runtime_release_sha256"] == release.release_hash
    assert evidence["sim_validation_evidence"]["miniqmt_sim"]["binding_id"] == qmt_binding.binding_id
    assert evidence["broker_compatibility"]["status"] == "VERIFIED"
    assert evidence["broker_compatibility"]["target_broker_backend"] == "minqmt_live"
    assert evidence["broker_compatibility"]["simulation_binding_id"] == qmt_binding.binding_id


def test_live_admission_evidence_api_returns_actionable_payload(client: TestClient) -> None:
    repo = client.app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service]().repository
    # The fixture has only LocalSim evidence, so the API must fail rather than
    # fabricating MiniQMT evidence from an unrelated run.
    only_run = next(iter(repo.daily_runs.values()))
    repo.update_simulation_daily_run(only_run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)
    response = client.get(
        "/api/v1/simulation-runtime/live-admission/evidence",
        params={"paper_v2_run_id": only_run.run_id, "miniqmt_sim_run_id": only_run.run_id},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error_code"] == "DATA_UNAVAILABLE"


def test_live_admission_evidence_api_returns_standardized_dual_sim_payload(client: TestClient) -> None:
    repo = InMemorySimulationRuntimeRepository()
    release_service = StrategyRuntimeReleaseService(repository=repo)
    release = release_service.create_release(
        package_id="pkg_ops_api_live",
        manifest_sha256="manifest_ops_api_live",
        runtime_profile_id="runtime_profile_ops_api_live",
        runtime_profile_version_id="runtime_profile_ops_api_live_v1",
        runtime_profile_sha256="runtime_profile_ops_api_live_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="ops api live admission evidence",
    )
    local_binding = release_service.create_binding(
        strategy_id="strategy_ops_api_local",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="ops api live admission local",
    )
    qmt_binding = release_service.create_binding(
        strategy_id="strategy_ops_api_qmt",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="OpsApiLiveQmt",
        order_remark_prefix="ops-api-live-qmt",
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="ops api live admission qmt",
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_ops_api_local",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0},
                ),
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_ops_api_qmt",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0},
                ),
            }
        ),
    )
    local = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    ).results[0]
    qmt = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    ).results[0]
    assert local.run is not None
    assert qmt.run is not None
    repo.update_simulation_daily_run(local.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)
    repo.update_simulation_daily_run(qmt.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)

    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    repo_for_app = repo
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = (
        lambda: SimulationRuntimeOpsService(repository=repo_for_app)
    )
    local_client = TestClient(app)

    response = local_client.get(
        "/api/v1/simulation-runtime/live-admission/evidence",
        params={
            "paper_v2_run_id": local.run.run_id,
            "miniqmt_sim_run_id": qmt.run.run_id,
            "target_broker_backend": "minqmt_live",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["sim_validation_evidence"]["paper_v2"]["status"] == "VERIFIED"
    assert payload["sim_validation_evidence"]["miniqmt_sim"]["status"] == "VERIFIED"
    assert payload["broker_compatibility"]["status"] == "VERIFIED"


def test_ops_service_start_stop_scheduler_requires_background_scheduler():
    """start_scheduler and stop_scheduler raise when scheduler is not background."""
    from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
    from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler
    from backend.services.trading_core.errors import DataUnavailableError

    svc = SimulationRuntimeOpsService(scheduler=SimulationLifecycleScheduler())
    with pytest.raises(DataUnavailableError, match="SimulationLifecycleBackgroundScheduler"):
        svc.start_scheduler()
    with pytest.raises(DataUnavailableError, match="SimulationLifecycleBackgroundScheduler"):
        svc.stop_scheduler()


def test_ops_service_scheduler_tick_with_lifecycle_scheduler():
    """scheduler_tick works with plain SimulationLifecycleScheduler."""
    from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
    from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
    from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler
    from backend.services.simulation_runtime.models import SimulationBindingApprovalState

    repo = InMemorySimulationRuntimeRepository()
    # Use an approval state that won't match any bindings, so the tick completes cleanly
    scheduler = SimulationLifecycleScheduler(repository=repo)
    svc = SimulationRuntimeOpsService(repository=repo, scheduler=scheduler)
    result = svc.scheduler_tick()
    assert result["ok"] is True
    assert result["action"] == "scheduler_tick"
    assert "total_bindings" in result
    assert result["total_bindings"] == 0
    assert result["failed_count"] == 0
