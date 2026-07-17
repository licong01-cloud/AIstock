from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
)
from backend.services.miniqmt_execution_runtime.repository import InMemoryMiniQMTExecutionRuntimeRepository
from backend.services.selection_center.models import SelectionCandidate
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    InMemorySimulationRuntimeRepository,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationLifecycleScheduler,
    SimulationRunContext,
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    StrategyPackageSelectionResult,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.models import (
    LocalSimExecutionRuntimeStatus,
    LocalSimExecutionStateV1,
    canonical_json_sha256,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide
from backend.services.strategy_package.models import PackageStatus

TRADE_DATE = date(2026, 5, 21)
MINIQMT_B0_QUOTE_CONTROL = {
    "schema_version": "miniqmt_quote_control_binding_v1",
    "control_revision": "B0_QUOTE_V2",
}


def _scheduler_component_status() -> dict[str, object]:
    return {
        "selection_inference": {
            "mode": "artifact_hit_sync_else_background",
            "in_flight_count": 0,
            "in_flight": [],
        },
        "binding_watchdog": {"timeout_seconds": 30.0},
        "miniqmt_sim_runtime": {
            "sim_runtime_kind": "event_loop",
            "compiler_route_retired": True,
        },
        "miniqmt_quote_context": {"status": "READY"},
    }


class _PlatformDiagnosticsBackgroundScheduler:
    def __init__(self, *, last_run_at: datetime, market_phase: str = "OPEN_AM") -> None:
        self.last_run_at = last_run_at
        self.market_phase = market_phase

    def status(self) -> dict[str, object]:
        return {
            "scheduler": "simulation_lifecycle_scheduler",
            "default_submit": True,
            "autostart": True,
            "running": True,
            "thread_alive": True,
            "interval_seconds": 30,
            "scheduler_control_api_enabled": True,
            "manual_tick_endpoint_enabled": False,
            "last_run_at": self.last_run_at.isoformat(),
            "last_result": {
                "trade_date": TRADE_DATE.isoformat(),
                "market_phase": self.market_phase,
                "errors": [],
            },
            "last_blocking_result": None,
            "scheduler_loop_health": {
                "schema_version": "simulation_background_scheduler_loop_health_v1",
                "status": "HEALTHY",
                "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_OK",
                "active_failure": None,
                "last_failure": None,
                "last_successful_tick_at": self.last_run_at.isoformat(),
                "consecutive_failure_count": 0,
                "total_failure_count": 0,
                "total_success_count": 1,
                "execution_gate": False,
                "auto_clears_on_success": True,
            },
            "miniqmt_quote_ingress_activation": {"status": "NOT_CONFIGURED"},
            "b0_quote_v2_controllers": {"status": "NOT_CONFIGURED", "controller_count": 0},
            **_scheduler_component_status(),
        }


def _b0_quote_policy() -> dict[str, Any]:
    benchmark_policy = {
        "benchmark_max_age_ms": 10_000,
        "arrival_forward_window_ms": 2_000,
        "clock_skew_tolerance_ms": 1_000,
        "benchmark_max_transport_latency_ms": 3_000,
        "policy_version": "miniqmt_execution_tca_benchmark_v1",
    }
    return {
        "algo_code": "SNIPER_MINIQMT",
        "algo_config": {"tca": {"benchmark_policy": benchmark_policy}},
        "quote_contract": {
            "schema_version": "miniqmt_quote_contract_policy_v2",
            "control_revision": "B0_QUOTE_V2",
            "required_capabilities": [
                "CALENDAR",
                "DEPTH_UNIT_SHARES",
                "EXCHANGE_TIMESTAMP",
                "FIVE_LEVEL_DEPTH",
                "RAW_PRICE_BASIS",
                "TRADABILITY",
            ],
            "max_receive_age_ms": 20_000,
            "max_source_lag_ms": 20_000,
            "max_exchange_age_ms": 20_000,
            "max_negative_skew_ms": 1_000,
            "max_clock_age_divergence_ms": 1_000,
            "max_dependency_group_skew_ms": 20_000,
            "auction_mode": "OBSERVE_ONLY",
        },
        "quote_evidence": {
            "schema_version": "miniqmt_quote_evidence_policy_v1",
            "benchmark_policy_version": benchmark_policy["policy_version"],
            "mark_policy_version": "miniqmt_execution_tca_mark_selector_v1",
            "markout_max_lag_ms": 10_000,
        },
    }


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
        self.package_repository = SimpleNamespace(
            get=lambda package_id: SimpleNamespace(
                package_id=package_id,
                manifest_sha256=release.manifest_sha256,
                package_status=PackageStatus.SELECTION_ENABLED,
            )
        )

    def run_selection(self, **kwargs: Any) -> StrategyPackageSelectionResult:
        candidates = _candidate_rows()
        evidence = _evidence(self.release, candidates=candidates)
        return StrategyPackageSelectionResult(
            runtime_config={
                "runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}
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
    setattr(repo, "_ops_test_scheduler", scheduler)
    return repo, run.run_id, plan.plan_id


@pytest.fixture()
def client(repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str]) -> TestClient:
    repo, _, _ = repo_with_plan
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = lambda: (
        SimulationRuntimeOpsService(
            repository=repo,
            scheduler=getattr(repo, "_ops_test_scheduler"),
        )
    )
    app.dependency_overrides[simulation_runtime.get_miniqmt_runtime_repository] = lambda: SimpleNamespace()
    return TestClient(app)


def _mark_localsim_run_succeeded_with_persistence(
    repo: InMemorySimulationRuntimeRepository,
    run_id: str,
    *,
    intent_count: int = 1,
) -> None:
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.SUCCEEDED,
        payload_patch={
            "broker_called": True,
            "submitted_intents": intent_count,
            "last_stage": "SUCCEEDED",
            "local_sim_persistence": {
                "schema_version": "local_sim_persistence_v1",
                "status": "PERSISTED",
                "paper_v2_run_id": run_id,
                "order_count": intent_count,
                "fill_count": intent_count,
                "order_event_count": intent_count,
                "cash_ledger_count": intent_count,
                "position_count": intent_count,
                "snapshot_time": datetime(2026, 5, 21, 9, 31, tzinfo=UTC).isoformat(),
                "cash": 99000.0,
                "nav": 100000.0,
            },
        },
        payload_unset=("submit_failure",),
    )


def test_scheduler_status_reports_controlled_ops_and_does_not_claim_autostart(client: TestClient) -> None:
    response = client.get("/api/v1/simulation-runtime/scheduler/status")

    assert response.status_code == 200
    scheduler = response.json()["scheduler"]
    assert scheduler["autostart"] is False
    assert scheduler["default_submit"] is False
    assert scheduler["sim_binding_selection_policy"] == "all_non_retired"
    assert scheduler["read_only_status_api"] is True
    assert scheduler["read_only_ops_api"] is False
    assert scheduler["controlled_ops_api"] is True
    assert scheduler["manual_tick_endpoint_enabled"] is True
    assert scheduler["scheduler_control_api_enabled"] is False
    assert scheduler["effective_runtime_health"] == "SCHEDULER_INACTIVE"
    assert scheduler["scheduler_loop_health"]["status"] == "NOT_APPLICABLE"
    assert scheduler["scheduler_loop_health"]["execution_gate"] is False
    assert scheduler["account_slot_persistence"]["enabled"] is True
    assert scheduler["account_slot_persistence"]["miniqmt_unified_binding_mode"] == "account_group_slots"
    assert scheduler["context_provider_mode"] == "StaticSimulationRunContextProvider"
    assert scheduler["restart_recovery_mode"] == "persisted_state_only"
    assert scheduler["selection_inference"]["mode"] == "artifact_hit_sync_else_background"
    assert scheduler["binding_watchdog"]["timeout_seconds"] > 0
    assert scheduler["miniqmt_sim_runtime"]["sim_runtime_kind"] == "event_loop"
    assert isinstance(scheduler["miniqmt_quote_context"], dict)
    assert scheduler["miniqmt_quote_ingress_activation"] == {
        "schema_version": "miniqmt_quote_ingress_activation_v1",
        "status": "UNCONFIGURED",
        "factory_available": False,
    }
    assert scheduler["b0_quote_v2_controllers"] == {
        "status": "DISABLED",
        "controller_count": 0,
    }
    assert scheduler["summary"]["safety_note"].endswith("default_submit is disabled.")
    assert [window["window_id"] for window in scheduler["schedule_windows"]] == [
        "pre_open",
        "selection",
        "planning",
        "opening_auction_observe",
        "execution",
        "lunch_recess",
        "execution_afternoon",
        "closing_auction_observe",
        "post_close_reconcile",
    ]


def test_scheduler_status_summary_reports_enabled_submit_mode() -> None:
    class _EnabledSubmitScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "running": True,
                "thread_alive": True,
                "default_submit": True,
                "sim_binding_selection_policy": "all_non_retired",
                "miniqmt_quote_ingress_activation": {
                    "schema_version": "miniqmt_quote_ingress_activation_v1",
                    "status": "RUNNING",
                    "factory_available": True,
                },
                "b0_quote_v2_controllers": {
                    "status": "RUNNING",
                    "controller_count": 2,
                },
                **_scheduler_component_status(),
            }

    scheduler = SimulationRuntimeOpsService(
        repository=InMemorySimulationRuntimeRepository(),
        scheduler=_EnabledSubmitScheduler(),
    ).scheduler_status()

    assert scheduler["default_submit"] is True
    assert scheduler["sim_binding_selection_policy"] == "all_non_retired"
    assert scheduler["miniqmt_quote_ingress_activation"]["status"] == "RUNNING"
    assert scheduler["b0_quote_v2_controllers"]["controller_count"] == 2
    assert scheduler["selection_inference"]["in_flight_count"] == 0
    assert scheduler["miniqmt_sim_runtime"]["compiler_route_retired"] is True
    assert scheduler["effective_runtime_health"] == "NO_CURRENT_DAY_BLOCKER"
    assert scheduler["summary"]["safety_note"].endswith("default_submit is enabled.")


def test_scheduler_status_projects_active_run_loop_exception_as_current_blocker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_TRADE_DATE", TRADE_DATE.isoformat())

    class _LoopBlockedScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "running": True,
                "thread_alive": True,
                "default_submit": True,
                "scheduler_control_api_enabled": True,
                "miniqmt_quote_ingress_activation": {"status": "RUNNING"},
                "b0_quote_v2_controllers": {"status": "RUNNING", "controller_count": 1},
                "scheduler_loop_health": {
                    "schema_version": "simulation_background_scheduler_loop_health_v1",
                    "status": "BLOCKED",
                    "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
                    "active_failure": {
                        "schema_version": "simulation_background_scheduler_loop_failure_v1",
                        "status": "BLOCKED",
                        "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
                        "stage": "BACKGROUND_SCHEDULER_RUN_LOOP",
                        "exception_type": "DataUnavailableError",
                        "exception_message": "dependency failed",
                        "underlying_reason_code": "SIMULATION_DEPENDENCY_UNAVAILABLE",
                        "underlying_stage": "DEPENDENCY_READ",
                        "trade_date": TRADE_DATE.isoformat(),
                        "first_failure_at": "2026-05-21T01:20:00+00:00",
                        "failure_at": "2026-05-21T01:21:00+00:00",
                        "context": {},
                        "execution_gate": False,
                        "auto_clears_on_success": True,
                    },
                    "last_failure": None,
                    "last_successful_tick_at": "2026-05-21T01:19:00+00:00",
                    "consecutive_failure_count": 2,
                    "total_failure_count": 3,
                    "total_success_count": 5,
                    "execution_gate": False,
                    "auto_clears_on_success": True,
                },
                **_scheduler_component_status(),
            }

    projected = SimulationRuntimeOpsService(
        repository=InMemorySimulationRuntimeRepository(),
        scheduler=_LoopBlockedScheduler(),  # type: ignore[arg-type]
    ).scheduler_status()

    assert projected["thread_alive"] is True
    assert projected["effective_runtime_health"] == "BLOCKED"
    assert projected["scheduler_loop_health"]["status"] == "BLOCKED"
    blockers = projected["current_trade_date_blockers"]
    assert blockers["status"] == "BLOCKED"
    assert blockers["blocker_count"] == 1
    assert blockers["observed_blocker_count"] == 1
    assert blockers["source"] == "scheduler_loop_health+simulation_daily_run_readback"
    assert blockers["execution_gate"] is False
    assert blockers["blockers"][0] == {
        "component": "simulation_background_scheduler_run_loop",
        "status": "BLOCKED",
        "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
        "stage": "BACKGROUND_SCHEDULER_RUN_LOOP",
        "exception_type": "DataUnavailableError",
        "exception_message": "dependency failed",
        "underlying_reason_code": "SIMULATION_DEPENDENCY_UNAVAILABLE",
        "underlying_stage": "DEPENDENCY_READ",
        "failure_trade_date": TRADE_DATE.isoformat(),
        "first_failure_at": "2026-05-21T01:20:00+00:00",
        "failure_at": "2026-05-21T01:21:00+00:00",
        "consecutive_failure_count": 2,
        "execution_gate": False,
    }


def test_scheduler_status_rejects_missing_loop_health_from_background_scheduler() -> None:
    class _MissingLoopHealthScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "running": True,
                "thread_alive": True,
                "scheduler_control_api_enabled": True,
                "miniqmt_quote_ingress_activation": {"status": "RUNNING"},
                "b0_quote_v2_controllers": {"status": "RUNNING", "controller_count": 1},
                **_scheduler_component_status(),
            }

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationRuntimeOpsService(
            repository=InMemorySimulationRuntimeRepository(),
            scheduler=_MissingLoopHealthScheduler(),  # type: ignore[arg-type]
        ).scheduler_status()

    assert exc_info.value.context["reason_code"] == "SIMULATION_SCHEDULER_LOOP_HEALTH_MISSING"


def test_scheduler_status_rejects_malformed_active_loop_failure() -> None:
    class _MalformedLoopHealthScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "running": True,
                "thread_alive": True,
                "scheduler_control_api_enabled": True,
                "scheduler_loop_health": {
                    "schema_version": "simulation_background_scheduler_loop_health_v1",
                    "status": "BLOCKED",
                    "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
                    "active_failure": {
                        "schema_version": "simulation_background_scheduler_loop_failure_v1",
                        "status": "BLOCKED",
                        "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
                        "stage": "BACKGROUND_SCHEDULER_RUN_LOOP",
                        "exception_message": "x" * 2049,
                        "context": {},
                        "execution_gate": False,
                        "auto_clears_on_success": True,
                    },
                    "last_failure": None,
                    "last_successful_tick_at": None,
                    "consecutive_failure_count": 1,
                    "total_failure_count": 1,
                    "total_success_count": 0,
                    "execution_gate": False,
                    "auto_clears_on_success": True,
                },
                "miniqmt_quote_ingress_activation": {"status": "RUNNING"},
                "b0_quote_v2_controllers": {"status": "RUNNING", "controller_count": 1},
                **_scheduler_component_status(),
            }

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationRuntimeOpsService(
            repository=InMemorySimulationRuntimeRepository(),
            scheduler=_MalformedLoopHealthScheduler(),  # type: ignore[arg-type]
        ).scheduler_status()

    assert exc_info.value.context["reason_code"] == "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID"
    assert exc_info.value.context["field"] == "active_failure.exception_message"


@pytest.mark.parametrize(
    "status_patch",
    [
        {"miniqmt_quote_ingress_activation": None, "b0_quote_v2_controllers": {}},
        {"miniqmt_quote_ingress_activation": {}, "b0_quote_v2_controllers": "invalid"},
    ],
)
def test_scheduler_status_rejects_missing_or_invalid_quote_health(status_patch: dict[str, object]) -> None:
    class _InvalidQuoteHealthScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "default_submit": False,
                **_scheduler_component_status(),
                **status_patch,
            }

    with pytest.raises(RuntimeConfigInvalidError, match="scheduler status .* must be a mapping"):
        SimulationRuntimeOpsService(
            repository=InMemorySimulationRuntimeRepository(),
            scheduler=_InvalidQuoteHealthScheduler(),
        ).scheduler_status()


def test_scheduler_status_keeps_current_day_failed_runs_visible_after_noop_window(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_TRADE_DATE", TRADE_DATE.isoformat())
    repo, run_id, _plan_id = repo_with_plan
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "PRE_RUN_FAILED",
            "pre_run_failure": {
                "reason_code": "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE",
                "message": "required dataset refresh status is missing",
            },
        },
    )

    class _PostCloseNoopScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "running": True,
                "thread_alive": True,
                "default_submit": True,
                "last_result": {
                    "trade_date": TRADE_DATE.isoformat(),
                    "reason": "post_close_reconcile",
                    "processed": [],
                    "errors": [],
                    "alerts": [],
                    "has_blocking_result": False,
                },
                "last_blocking_result": {
                    "trade_date": TRADE_DATE.isoformat(),
                    "reason": "submit",
                    "has_blocking_result": True,
                    "summary": {"failed_count": 1},
                },
                "miniqmt_quote_ingress_activation": {
                    "schema_version": "miniqmt_quote_ingress_activation_v1",
                    "status": "INACTIVE",
                    "factory_available": True,
                },
                "b0_quote_v2_controllers": {"status": "IDLE", "controller_count": 0},
                **_scheduler_component_status(),
            }

    projected = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=_PostCloseNoopScheduler(),  # type: ignore[arg-type]
    ).scheduler_status()

    assert projected["last_error_count"] == 0
    assert projected["last_result"]["has_blocking_result"] is False
    assert projected["last_blocking_result"]["has_blocking_result"] is True
    assert projected["effective_runtime_health"] == "BLOCKED"
    blockers = projected["current_trade_date_blockers"]
    assert blockers["status"] == "BLOCKED"
    assert blockers["blocker_count"] == 1
    assert blockers["blockers"][0]["run_id"] == run_id
    assert blockers["blockers"][0]["reason_code"] == "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE"
    assert blockers["execution_gate"] is False


def test_scheduler_status_reads_current_day_blockers_before_first_scheduler_tick(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_TRADE_DATE", TRADE_DATE.isoformat())
    repo, run_id, _plan_id = repo_with_plan
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_TERMINAL,
        payload_patch={
            "last_stage": "PRE_RUN_FAILED",
            "pre_run_failure": {
                "reason_code": "SIMULATION_PACKAGE_ASSET_UNAVAILABLE",
            },
        },
    )

    class _BeforeFirstTickScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "running": True,
                "thread_alive": True,
                "default_submit": True,
                "last_result": None,
                "last_blocking_result": None,
                "miniqmt_quote_ingress_activation": {
                    "schema_version": "miniqmt_quote_ingress_activation_v1",
                    "status": "INACTIVE",
                    "factory_available": True,
                },
                "b0_quote_v2_controllers": {"status": "IDLE", "controller_count": 0},
                **_scheduler_component_status(),
            }

    projected = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=_BeforeFirstTickScheduler(),  # type: ignore[arg-type]
    ).scheduler_status()

    assert projected["effective_runtime_health"] == "BLOCKED"
    blockers = projected["current_trade_date_blockers"]
    assert blockers["trade_date"] == TRADE_DATE.isoformat()
    assert blockers["blocker_count"] == 1
    assert blockers["blockers"][0]["run_id"] == run_id
    assert blockers["last_observed_trade_dates"] == []


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
    run = payload["runs"][0]
    assert run["run_id"] == run_id
    assert run["execution_plan_id"]
    assert run["stage_counts"]["execution_plan_intent_count"] == 1
    assert run["strategy_performance"]["nav"] == 1.0
    assert run["display"]["status_label"] == "\u6267\u884c\u8ba1\u5212\u5df2\u751f\u6210"
    assert run["display"]["broker_label"] == "LocalSim \u672c\u5730\u6a21\u62df"
    assert run["display"]["strategy_label"] == "Ops"
    assert run["display"]["selection_label"] == "\u9009\u51fa 1 \u53ea\u5019\u9009"
    assert (
        run["display"]["execution_plan_label"] == "\u4ea4\u6613\u610f\u56fe 1 / \u5df2\u63d0\u4ea4 0 / \u5931\u8d25 0"
    )


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


def test_run_detail_projection_rejects_truthy_string_booleans(
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    run = repo.get_simulation_daily_run(run_id)
    invalid_run = run.model_copy(
        update={
            "run_payload_json": {
                **run.run_payload_json,
                "no_rebalance_required": "false",
            }
        }
    )

    with pytest.raises(DataUnavailableError) as payload_exc:
        SimulationRuntimeOpsService._broker_context(invalid_run)

    assert payload_exc.value.context["reason_code"] == "SIMULATION_RUN_PAYLOAD_BOOLEAN_INVALID"

    with pytest.raises(DataUnavailableError) as result_exc:
        SimulationRuntimeOpsService._orders_projection(
            run,
            {
                "qmt_batch_result": {
                    "results": [
                        {
                            "success": "false",
                            "broker_called": False,
                        }
                    ]
                }
            },
        )

    assert result_exc.value.context["reason_code"] == "SIMULATION_RUN_PAYLOAD_BOOLEAN_INVALID"


def test_runs_api_surfaces_miniqmt_succeeded_with_capacity_residual(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo = repo_with_plan[0]
    release_service = StrategyRuntimeReleaseService(repository=repo)
    release = release_service.create_release(
        package_id="pkg_ops_qmt",
        manifest_sha256="manifest_ops_qmt",
        runtime_profile_id="runtime_profile_ops_qmt",
        runtime_profile_version_id="runtime_profile_ops_qmt_v1",
        runtime_profile_sha256="runtime_profile_hash_ops_qmt",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="ops api capacity residual test",
    )
    binding = release_service.create_binding(
        strategy_id="strategy_qmt_ops",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        account_group_id="ag_ops",
        strategy_slot_id="slot_ops",
        strategy_name="OpsQmtCapacityResidual",
        order_remark_prefix="ops-qmt-capacity-residual",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="ops api capacity residual test",
    )
    repo.save_simulation_daily_run(
        SimulationDailyRun(
            run_id="simrun_qmt_capacity_residual",
            trade_date=TRADE_DATE,
            strategy_id=binding.strategy_id,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
            release_id=release.release_id,
            release_hash=release.release_hash,
            binding_id=binding.binding_id,
            binding_hash=binding.binding_hash,
            account_group_id=binding.account_group_id,
            strategy_slot_id=binding.strategy_slot_id,
            status=SimulationDailyRunStatus.SUCCEEDED,
            run_payload_json={
                "last_stage": "SUCCEEDED",
                "broker_called": True,
                "submitted_intents": 1,
                "failed_intents": 1,
                "qmt_batch_id": "batch_ops_capacity",
                "qmt_batch_status": "PARTIAL",
                "qmt_batch_result": {
                    "batch_status": "PARTIAL",
                    "failed": 1,
                    "results": [
                        {
                            "success": False,
                            "broker_called": False,
                            "preflight": {
                                "primary_error_code": "SKIPPED_INSUFFICIENT_CAPITAL",
                                "primary_error": {"message": "capacity residual"},
                                "errors": [
                                    {
                                        "code": "SKIPPED_INSUFFICIENT_CAPITAL",
                                        "message": "capacity residual",
                                        "context": {},
                                    }
                                ],
                            },
                        }
                    ],
                },
                "reconcile_after_submit": {
                    "submit_result_gate": {
                        "schema_version": "miniqmt_reconcile_submit_result_gate_v2",
                        "status": "SUCCEEDED",
                        "reason": "miniqmt_capacity_residual_skipped_and_reconciled",
                        "terminal_capacity_residual": True,
                    },
                    "qmt_batch_residual_summary": {
                        "schema_version": "miniqmt_batch_residual_summary_v1",
                        "noncompensating_residual": True,
                        "capacity_residual_count": 1,
                        "dependent_buy_count": 0,
                        "failed_result_count": 1,
                    },
                    "run": {"status": "SUCCEEDED"},
                },
            },
        )
    )

    response = client.get(
        "/api/v1/simulation-runtime/runs",
        params={"trade_date": TRADE_DATE.isoformat(), "broker_backend": "minqmt_sim"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["succeeded_with_capacity_residual_count"] == 1
    assert payload["summary"]["capacity_residual_count"] == 1
    assert payload["summary"]["capacity_residual_failed_intents"] == 1
    run = payload["runs"][0]
    assert run["succeeded_with_capacity_residual"] is True
    assert run["capacity_residual_count"] == 1
    assert run["capacity_residual_failed_intents"] == 1
    assert run["miniqmt_capacity_residual_observability"]["alert"]["reason_code"] == (
        "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
    )


def test_scheduler_tick_api_is_controlled_dry_run_by_default(client: TestClient) -> None:
    response = client.post(
        "/api/v1/simulation-runtime/scheduler/tick",
        json="2026-05-21T09:22:00+00:00",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "scheduler_tick"
    assert payload["trade_date"] == "2026-05-21"
    assert payload["submit"] is False
    assert payload["total_bindings"] >= 0
    assert "results" in payload


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
    execution_policy = _b0_quote_policy()
    release = release_service.create_release(
        package_id="pkg_live_admission",
        manifest_sha256="manifest_live_admission",
        runtime_profile_id="runtime_profile_live_admission",
        runtime_profile_version_id="runtime_profile_live_admission_v1",
        runtime_profile_sha256="runtime_profile_live_admission_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="vnpy_asset:SNIPER_MINIQMT",
        execution_policy_sha256=canonical_json_sha256(execution_policy),
        execution_policy_json=execution_policy,
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
        account_group_id="ag_minqmt_qmt_sim_account_sim",
        strategy_slot_id="slot_live_admission_qmt",
        strategy_name="LiveAdmissionQMT",
        order_remark_prefix="live-admission-qmt",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
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
    assert qmt.run.account_group_id == "ag_minqmt_qmt_sim_account_sim"
    assert qmt.run.strategy_slot_id == "slot_live_admission_qmt"
    repo.update_simulation_daily_run(local.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)
    repo.update_simulation_daily_run(qmt.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)

    service = SimulationRuntimeOpsService(repository=repo)
    with pytest.raises(DataUnavailableError, match="durable Paper v2"):
        service.build_live_admission_evidence(
            paper_v2_run_id=local.run.run_id,
            miniqmt_sim_run_id=qmt.run.run_id,
            target_broker_backend="minqmt_live",
        )

    _mark_localsim_run_succeeded_with_persistence(repo, local.run.run_id)
    evidence = service.build_live_admission_evidence(
        paper_v2_run_id=local.run.run_id,
        miniqmt_sim_run_id=qmt.run.run_id,
        target_broker_backend="minqmt_live",
    )

    assert evidence["sim_validation_evidence"]["paper_v2"]["status"] == "VERIFIED"
    assert evidence["sim_validation_evidence"]["paper_v2"]["runtime_release_sha256"] == release.release_hash
    assert evidence["sim_validation_evidence"]["miniqmt_sim"]["binding_id"] == qmt_binding.binding_id
    assert evidence["sim_validation_evidence"]["miniqmt_sim"]["account_group_id"] == "ag_minqmt_qmt_sim_account_sim"
    assert evidence["sim_validation_evidence"]["miniqmt_sim"]["strategy_slot_id"] == "slot_live_admission_qmt"
    assert evidence["broker_compatibility"]["status"] == "VERIFIED"
    assert evidence["broker_compatibility"]["target_broker_backend"] == "minqmt_live"
    assert evidence["broker_compatibility"]["simulation_binding_id"] == qmt_binding.binding_id
    assert evidence["broker_compatibility"]["account_group_id"] == "ag_minqmt_qmt_sim_account_sim"
    assert evidence["broker_compatibility"]["strategy_slot_id"] == "slot_live_admission_qmt"


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
    execution_policy = _b0_quote_policy()
    release = release_service.create_release(
        package_id="pkg_ops_api_live",
        manifest_sha256="manifest_ops_api_live",
        runtime_profile_id="runtime_profile_ops_api_live",
        runtime_profile_version_id="runtime_profile_ops_api_live_v1",
        runtime_profile_sha256="runtime_profile_ops_api_live_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="vnpy_asset:SNIPER_MINIQMT",
        execution_policy_sha256=canonical_json_sha256(execution_policy),
        execution_policy_json=execution_policy,
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
        account_group_id="ag_minqmt_qmt_sim_account_sim",
        strategy_slot_id="slot_ops_api_qmt",
        strategy_name="OpsApiLiveQmt",
        order_remark_prefix="ops-api-live-qmt",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
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
    assert qmt.run.account_group_id == "ag_minqmt_qmt_sim_account_sim"
    assert qmt.run.strategy_slot_id == "slot_ops_api_qmt"
    _mark_localsim_run_succeeded_with_persistence(repo, local.run.run_id)
    repo.update_simulation_daily_run(qmt.run.run_id, status=SimulationDailyRunStatus.SUCCEEDED)

    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    repo_for_app = repo
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = lambda: (
        SimulationRuntimeOpsService(repository=repo_for_app)
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
    assert payload["sim_validation_evidence"]["miniqmt_sim"]["account_group_id"] == "ag_minqmt_qmt_sim_account_sim"
    assert payload["sim_validation_evidence"]["miniqmt_sim"]["strategy_slot_id"] == "slot_ops_api_qmt"
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


def test_platform_diagnostics_is_read_only_layered_and_low_cardinality(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    before = repo.get_simulation_daily_run(run_id).model_dump(mode="json")

    response = client.get(
        "/api/v1/simulation-runtime/platform-diagnostics",
        params={"trade_date": TRADE_DATE.isoformat()},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "simulation_platform_diagnostics_v1"
    assert payload["query"]["trade_date"] == TRADE_DATE.isoformat()
    assert payload["query"]["returned_count"] == 1
    assert set(payload["layers"]) == {
        "process",
        "lifecycle",
        "bindings",
        "backends",
        "durability",
        "business",
    }
    assert payload["side_effect_contract"] == {
        "read_only": True,
        "starts_feed": False,
        "writes_database": False,
        "calls_broker": False,
        "replays_order": False,
        "execution_gate": False,
    }
    assert payload["alerts"]["acknowledge_required"] is False
    assert payload["alerts"]["execution_gate"] is False
    assert payload["runbook"]["read_only"] is True
    assert payload["runbook"]["ordered_steps"] == [
        "process",
        "lifecycle",
        "binding",
        "data_backend",
        "durable_facts",
        "broker_reconcile",
        "tca",
    ]
    allowed_labels = set(payload["metrics"]["label_allowlist"])
    forbidden_labels = set(payload["metrics"]["forbidden_high_cardinality_labels"])
    assert payload["metrics"]["series_count"] == len(payload["metrics"]["series"])
    assert payload["metrics"]["truncated"] is False
    assert all(set(metric["labels"]) <= allowed_labels for metric in payload["metrics"]["series"])
    assert all(not set(metric["labels"]).intersection(forbidden_labels) for metric in payload["metrics"]["series"])
    assert repo.get_simulation_daily_run(run_id).model_dump(mode="json") == before


def test_platform_diagnostics_filters_exact_run_binding_and_plan_identity(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, plan_id = repo_with_plan
    run = repo.get_simulation_daily_run(run_id)

    response = client.get(
        "/api/v1/simulation-runtime/platform-diagnostics",
        params={
            "trade_date": TRADE_DATE.isoformat(),
            "run_id": run_id,
            "binding_id": run.binding_id,
            "plan_id": plan_id,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["query"]["observed_match_count"] == 1
    assert payload["query"]["truncated"] is False
    assert payload["layers"]["bindings"][0]["identity"] == {
        "trade_date": TRADE_DATE.isoformat(),
        "binding_id": run.binding_id,
        "run_id": run_id,
        "runtime_id": None,
        "plan_id": plan_id,
    }


def test_platform_diagnostics_rejects_scheduler_boolean_coercion() -> None:
    class _StringBooleanScheduler:
        def status(self) -> dict[str, object]:
            return {
                "scheduler": "simulation_lifecycle_scheduler",
                "default_submit": False,
                "running": "false",
                "thread_alive": True,
                "scheduler_control_api_enabled": False,
                "miniqmt_quote_ingress_activation": {"status": "NOT_CONFIGURED"},
                "b0_quote_v2_controllers": {"status": "NOT_CONFIGURED", "controller_count": 0},
                **_scheduler_component_status(),
            }

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationRuntimeOpsService(
            repository=InMemorySimulationRuntimeRepository(),
            scheduler=_StringBooleanScheduler(),  # type: ignore[arg-type]
        ).scheduler_status()

    assert exc_info.value.context["reason_code"] == "SIMULATION_SCHEDULER_STATUS_INVALID"
    assert exc_info.value.context["field"] == "running"


def test_platform_diagnostics_rejects_invalid_business_count_without_coercion(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    repo.update_simulation_daily_run(run_id, payload_patch={"submitted_intents": "1"})

    response = client.get(
        "/api/v1/simulation-runtime/platform-diagnostics",
        params={"run_id": run_id},
    )

    assert response.status_code == 400
    assert "SIMULATION_PLATFORM_OBSERVABILITY_PAYLOAD_INVALID" in response.text
    assert "run_payload_json.submitted_intents" in response.text


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    [
        ("broker_called", "false", "SIMULATION_LIVE_EVIDENCE_BOOLEAN_INVALID"),
        ("submitted_intents", "1", "SIMULATION_LIVE_EVIDENCE_COUNT_INVALID"),
        ("local_sim_persistence.order_count", "1", "SIMULATION_LIVE_EVIDENCE_COUNT_INVALID"),
        ("local_sim_persistence.status", True, "SIMULATION_LIVE_EVIDENCE_PERSISTENCE_STATUS_INVALID"),
    ],
)
def test_platform_diagnostics_live_evidence_rejects_truthy_bool_and_safe_int_coercion(
    field: str,
    value: Any,
    expected_reason: str,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    _mark_localsim_run_succeeded_with_persistence(repo, run_id)
    run = repo.get_simulation_daily_run(run_id)
    if field.startswith("local_sim_persistence."):
        persistence = dict(run.run_payload_json["local_sim_persistence"])
        persistence[field.rsplit(".", 1)[-1]] = value
        repo.update_simulation_daily_run(run_id, payload_patch={"local_sim_persistence": persistence})
    else:
        repo.update_simulation_daily_run(run_id, payload_patch={field: value})

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationRuntimeOpsService._require_localsim_persisted_effects_for_live_evidence(
            repo.get_simulation_daily_run(run_id)
        )

    assert exc_info.value.context["reason_code"] == expected_reason


def test_platform_diagnostics_supports_exact_runtime_query_before_daily_run_exists() -> None:
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime_repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id="runtime_platform_diagnostics_only",
                account_group_id="account_platform_diagnostics_only",
                trade_date=TRADE_DATE,
                runtime_config_hash="a" * 64,
            ).model_dump()
        )
    )
    service = SimulationRuntimeOpsService(
        repository=InMemorySimulationRuntimeRepository(),
        scheduler=SimulationLifecycleScheduler(repository=InMemorySimulationRuntimeRepository()),
    )

    payload = service.platform_diagnostics(
        runtime_id="runtime_platform_diagnostics_only",
        runtime_repository=runtime_repository,
    )

    assert payload["query"]["runtime_id"] == "runtime_platform_diagnostics_only"
    assert payload["query"]["returned_count"] == 0
    backend_layers = payload["layers"]["backends"]
    assert any(layer["identity"]["backend"] == "minqmt_sim" for layer in backend_layers), backend_layers
    miniqmt_backend = next(layer for layer in backend_layers if layer["identity"]["backend"] == "minqmt_sim")
    assert miniqmt_backend["facts"]["runtime_id"] == "runtime_platform_diagnostics_only"
    assert miniqmt_backend["facts"]["quote_health_status"] == "DEGRADED"


def test_platform_diagnostics_emits_scheduler_tick_lag_metric_and_auto_clear_alert() -> None:
    observed_at = datetime(2026, 5, 21, 2, 0, tzinfo=UTC)
    repository = InMemorySimulationRuntimeRepository()
    lagged_service = SimulationRuntimeOpsService(
        repository=repository,
        scheduler=_PlatformDiagnosticsBackgroundScheduler(
            last_run_at=observed_at - timedelta(minutes=2),
        ),  # type: ignore[arg-type]
    )
    lagged = lagged_service.platform_diagnostics(
        trade_date=TRADE_DATE,
        generated_at=observed_at,
    )
    assert lagged["layers"]["process"]["reason_code"] == "SIMULATION_SCHEDULER_TICK_LAG_EXCEEDED"
    assert any(alert["alert_type"] == "SIMULATION_SCHEDULER_TICK_LAG" for alert in lagged["alerts"]["items"])
    lag_metric = next(
        metric for metric in lagged["metrics"]["series"] if metric["name"] == "simulation_scheduler_tick_lag_seconds"
    )
    assert lag_metric["value"] == 120.0

    recovered_service = SimulationRuntimeOpsService(
        repository=repository,
        scheduler=_PlatformDiagnosticsBackgroundScheduler(last_run_at=observed_at),  # type: ignore[arg-type]
    )
    recovered = recovered_service.platform_diagnostics(
        trade_date=TRADE_DATE,
        generated_at=observed_at,
    )
    assert not any(alert["alert_type"] == "SIMULATION_SCHEDULER_TICK_LAG" for alert in recovered["alerts"]["items"])


def test_platform_diagnostics_projects_exact_localsim_state_partial_and_bar_lag(
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, plan_id = repo_with_plan
    run = repo.get_simulation_daily_run(run_id)
    last_bar = datetime(2026, 5, 21, 1, 30, tzinfo=UTC)
    state = LocalSimExecutionStateV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=plan_id,
        intent_id="intent_platform_observability",
        algo_instance_id="algo_platform_observability",
        portfolio_id="portfolio_platform_observability",
        order_id="order_platform_observability",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        total_quantity=100,
        filled_quantity=20,
        remaining_quantity=80,
        algo_code="V25",
        order_status="PARTIAL",
        runtime_status=LocalSimExecutionRuntimeStatus.ACTIVE,
        schedule_version="v25_platform_observability",
        causality_cursor=last_bar.replace(minute=35),
        last_processed_bar_time=last_bar,
        idempotency_key="localsim-platform-observability",
    )
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={
            "last_stage": "INTRADAY_RUNNING",
            "local_sim_execution_states_v1": [state.model_dump(mode="json")],
        },
    )
    observed_at = datetime(2026, 5, 21, 1, 35, tzinfo=UTC)
    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=_PlatformDiagnosticsBackgroundScheduler(last_run_at=observed_at),  # type: ignore[arg-type]
    )

    payload = service.platform_diagnostics(
        run_id=run_id,
        generated_at=observed_at,
    )

    business = payload["layers"]["business"][0]
    assert business["facts"]["active_algo_count"] == 1
    assert business["facts"]["partial_count"] == 1
    assert business["facts"]["max_bar_lag_seconds"] == 300.0
    assert any(alert["alert_type"] == "LOCAL_SIM_CAUSAL_BAR_NOT_PROGRESSING" for alert in payload["alerts"]["items"])
    metrics = {metric["name"]: metric["value"] for metric in payload["metrics"]["series"]}
    assert metrics["simulation_localsim_active_algo_count"] == 1
    assert metrics["simulation_localsim_partial_count"] == 1
    assert metrics["simulation_localsim_causal_bar_lag_seconds"] == 300.0


def test_platform_diagnostics_projects_localsim_outbox_backlog_terminal_failure_and_recovery(
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={
            "last_stage": "INTRADAY_RUNNING",
            "local_sim_projection_outbox_v1": {
                "status": "PENDING",
                "attempt_count": 0,
                "readback_attempt_count": 0,
                "generation": 1,
            },
        },
    )
    pending_run = repo.get_simulation_daily_run(run_id)
    observed_at = pending_run.updated_at + timedelta(seconds=121)
    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=_PlatformDiagnosticsBackgroundScheduler(last_run_at=observed_at),  # type: ignore[arg-type]
    )

    backlog = service.platform_diagnostics(run_id=run_id, generated_at=observed_at)
    durability = backlog["layers"]["durability"][0]
    assert durability["status"] == "DEGRADED"
    assert durability["reason_code"] == "LOCAL_SIM_PROJECTION_BACKLOG"
    assert any(alert["alert_type"] == "SIMULATION_DURABILITY_FAILURE" for alert in backlog["alerts"]["items"])

    repo.update_simulation_daily_run(
        run_id,
        payload_patch={
            "local_sim_projection_terminal_failure": {
                "reason_code": "LOCAL_SIM_PROJECTION_TERMINAL_TEST",
                "stage": "LOCAL_SIM_PROJECTION",
            }
        },
    )
    terminal = service.platform_diagnostics(
        run_id=run_id,
        generated_at=repo.get_simulation_daily_run(run_id).updated_at + timedelta(seconds=1),
    )
    assert terminal["layers"]["durability"][0]["status"] == "BLOCKED"
    assert terminal["layers"]["durability"][0]["reason_code"] == "LOCAL_SIM_PROJECTION_TERMINAL_TEST"

    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.SUCCEEDED,
        payload_patch={
            "last_stage": "SUCCEEDED",
            "submitted_intents": 0,
            "failed_intents": 0,
            "pending_intents": 0,
            "local_sim_persistence": {
                "schema_version": "local_sim_persistence_v2",
                "status": "PERSISTED",
                "order_count": 0,
                "fill_count": 0,
                "order_event_count": 0,
                "cash_ledger_count": 0,
                "position_count": 0,
            },
            "local_sim_projection_outbox_v1": {
                "status": "PROJECTED",
                "attempt_count": 1,
                "readback_attempt_count": 1,
                "generation": 1,
            },
        },
        payload_unset=("local_sim_projection_terminal_failure",),
    )
    recovered_run = repo.get_simulation_daily_run(run_id)
    recovered = service.platform_diagnostics(
        run_id=run_id,
        generated_at=recovered_run.updated_at + timedelta(seconds=1),
    )
    assert recovered["layers"]["durability"][0]["status"] == "HEALTHY"
    assert not any(alert["alert_type"] == "SIMULATION_DURABILITY_FAILURE" for alert in recovered["alerts"]["items"])


def test_platform_diagnostics_projects_valid_miniqmt_pending_batch_and_rejects_counter_conflict(
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    original = repo.get_simulation_daily_run(run_id)
    release = repo.get_strategy_runtime_release(original.release_id)
    binding = StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id="strategy_platform_valid_miniqmt",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        account_group_id="ag_platform_valid_miniqmt",
        strategy_slot_id="slot_platform_valid_miniqmt",
        strategy_name="PlatformValidMiniQMT",
        order_remark_prefix="platform-valid-miniqmt",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="platform diagnostics valid MiniQMT batch",
    )
    qmt_run = original.model_copy(
        update={
            "run_id": "simrun_platform_valid_miniqmt",
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "binding_hash": binding.binding_hash,
            "account_group_id": binding.account_group_id,
            "strategy_slot_id": binding.strategy_slot_id,
            "broker_backend": SimulationBrokerBackend.MINIQMT_SIM,
            "execution_plan_id": None,
            "execution_plan_hash": None,
            "status": SimulationDailyRunStatus.INTRADAY_RUNNING,
            "run_payload_json": {
                "last_stage": "INTRADAY_RUNNING",
                "submitted_intents": 0,
                "failed_intents": 0,
                "pending_intents": 1,
                "qmt_batch_result": {
                    "batch_id": "batch_platform_valid_miniqmt",
                    "batch_status": "PARTIAL",
                    "results": [{"success": False, "broker_called": False}],
                    "total": 1,
                    "success": False,
                    "succeeded": 0,
                    "failed": 0,
                    "pending": 1,
                    "runtime_evidence": {
                        "runtime_id": "runtime_platform_valid_miniqmt",
                        "source": "simulation_runtime_event_loop_tick_driver",
                    },
                },
            },
        }
    )
    repo.save_simulation_daily_run(qmt_run)
    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=getattr(repo, "_ops_test_scheduler"),
    )

    payload = service.platform_diagnostics(run_id=qmt_run.run_id)
    assert payload["layers"]["durability"][0]["status"] == "HEALTHY"
    assert payload["layers"]["business"][0]["facts"]["pending_intents"] == 1
    pending_metric = next(
        metric for metric in payload["metrics"]["series"] if metric["name"] == "simulation_miniqmt_pending_algo_count"
    )
    assert pending_metric["value"] == 1

    repo.update_simulation_daily_run(qmt_run.run_id, payload_patch={"pending_intents": 0})
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        service.platform_diagnostics(run_id=qmt_run.run_id)
    assert exc_info.value.context["reason_code"] == "SIMULATION_PLATFORM_BUSINESS_COUNT_CONFLICT"


def test_platform_diagnostics_rejects_malformed_localsim_durable_state(
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={"local_sim_execution_states_v1": [{"schema_version": "local_sim_execution_state_v1"}]},
    )
    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=getattr(repo, "_ops_test_scheduler"),
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        service.platform_diagnostics(run_id=run_id)

    assert exc_info.value.context["reason_code"] == "SIMULATION_PLATFORM_LOCAL_SIM_STATE_INVALID"


def test_platform_diagnostics_keeps_binding_failure_isolated_and_alert_auto_clears(
    client: TestClient,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    original = repo.get_simulation_daily_run(run_id)
    release = repo.get_strategy_runtime_release(original.release_id)
    isolated_binding = StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id="strategy_ops_failed_isolated",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="platform diagnostics binding isolation",
    )
    failed_run = original.model_copy(
        update={
            "run_id": "simrun_ops_failed_isolated",
            "strategy_id": isolated_binding.strategy_id,
            "binding_id": isolated_binding.binding_id,
            "binding_hash": isolated_binding.binding_hash,
            "status": SimulationDailyRunStatus.FAILED_RETRYABLE,
            "run_payload_json": {
                "last_stage": "SUBMITTING",
                "submit_failure": {
                    "reason_code": "SIMULATION_TEST_BINDING_FAILURE",
                    "stage": "SUBMITTING",
                },
            },
        }
    )
    repo.save_simulation_daily_run(failed_run)

    blocked = client.get(
        "/api/v1/simulation-runtime/platform-diagnostics",
        params={"trade_date": TRADE_DATE.isoformat()},
    ).json()
    binding_alerts = [
        alert for alert in blocked["alerts"]["items"] if alert["alert_type"] == "SIMULATION_BINDING_BLOCKED"
    ]
    assert [alert["identity"]["run_id"] for alert in binding_alerts] == [failed_run.run_id]
    assert any(layer["identity"]["run_id"] == original.run_id for layer in blocked["layers"]["bindings"])

    repo.update_simulation_daily_run(
        failed_run.run_id,
        status=SimulationDailyRunStatus.SUCCEEDED,
        payload_patch={
            "last_stage": "SUCCEEDED",
            "submitted_intents": 0,
            "failed_intents": 0,
            "pending_intents": 0,
            "local_sim_persistence": {
                "schema_version": "local_sim_persistence_v1",
                "status": "PERSISTED",
                "order_count": 0,
                "fill_count": 0,
                "order_event_count": 0,
                "cash_ledger_count": 0,
                "position_count": 0,
            },
        },
        payload_unset=("submit_failure",),
    )
    recovered = client.get(
        "/api/v1/simulation-runtime/platform-diagnostics",
        params={"trade_date": TRADE_DATE.isoformat()},
    ).json()
    recovered_binding_alerts = [
        alert for alert in recovered["alerts"]["items"] if alert["alert_type"] == "SIMULATION_BINDING_BLOCKED"
    ]
    assert all(alert["identity"].get("run_id") != failed_run.run_id for alert in recovered_binding_alerts)
    recovered_layer = next(
        layer for layer in recovered["layers"]["bindings"] if layer["identity"]["run_id"] == failed_run.run_id
    )
    assert recovered_layer["status"] == "HEALTHY"


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        (
            {
                "miniqmt_runtime_id": "runtime_a",
                "qmt_batch_result": {
                    "batch_id": "batch_runtime_conflict",
                    "batch_status": "SUCCEEDED",
                    "results": [],
                    "total": 0,
                    "success": True,
                    "succeeded": 0,
                    "failed": 0,
                    "pending": 0,
                    "runtime_evidence": {"runtime_id": "runtime_b"},
                },
            },
            "SIMULATION_PLATFORM_RUNTIME_IDENTITY_CONFLICT",
        ),
        (
            {
                "qmt_batch_result": {
                    "batch_id": "batch_cardinality_conflict",
                    "batch_status": "SUCCEEDED",
                    "results": [],
                    "total": 1,
                    "success": True,
                    "succeeded": 1,
                    "failed": 0,
                    "pending": 0,
                    "runtime_evidence": {"runtime_id": "runtime_cardinality"},
                },
            },
            "SIMULATION_PLATFORM_DURABLE_BATCH_CARDINALITY_MISMATCH",
        ),
    ],
)
def test_platform_diagnostics_fails_loud_on_miniqmt_identity_or_cardinality_conflict(
    payload: dict[str, Any],
    expected_reason: str,
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, run_id, _plan_id = repo_with_plan
    original = repo.get_simulation_daily_run(run_id)
    release = repo.get_strategy_runtime_release(original.release_id)
    qmt_binding = StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id=f"strategy_{expected_reason.lower()}",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        account_group_id=f"ag_{expected_reason.lower()}",
        strategy_slot_id=f"slot_{expected_reason.lower()}",
        strategy_name="PlatformDiagnosticsQmtNegative",
        order_remark_prefix=f"platform-diagnostics-{expected_reason.lower()}",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="platform diagnostics durable evidence negative",
    )
    qmt_run = original.model_copy(
        update={
            "run_id": f"simrun_{expected_reason.lower()}",
            "strategy_id": qmt_binding.strategy_id,
            "binding_id": qmt_binding.binding_id,
            "binding_hash": qmt_binding.binding_hash,
            "account_group_id": qmt_binding.account_group_id,
            "strategy_slot_id": qmt_binding.strategy_slot_id,
            "broker_backend": SimulationBrokerBackend.MINIQMT_SIM,
            "execution_plan_id": None,
            "execution_plan_hash": None,
            "status": SimulationDailyRunStatus.SUCCEEDED,
            "run_payload_json": {"last_stage": "SUCCEEDED", **payload},
        }
    )
    repo.save_simulation_daily_run(qmt_run)
    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=getattr(repo, "_ops_test_scheduler"),
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        service.platform_diagnostics(run_id=qmt_run.run_id)

    assert exc_info.value.context["reason_code"] == expected_reason
