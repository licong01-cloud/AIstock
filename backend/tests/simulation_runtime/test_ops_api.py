from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
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
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationLifecycleScheduler,
    SimulationRunContext,
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    StrategyPackageSelectionResult,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
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
    setattr(repo, "_ops_test_scheduler", scheduler)
    return repo, run.run_id, plan.plan_id


@pytest.fixture()
def client(repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str]) -> TestClient:
    repo, _, _ = repo_with_plan
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    app.dependency_overrides[simulation_runtime.get_simulation_runtime_ops_service] = (
        lambda: SimulationRuntimeOpsService(
            repository=repo,
            scheduler=getattr(repo, "_ops_test_scheduler"),
        )
    )
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
    assert scheduler["summary"]["safety_note"].endswith("default_submit is enabled.")


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
    assert run["display"]["execution_plan_label"] == "\u4ea4\u6613\u610f\u56fe 1 / \u5df2\u63d0\u4ea4 0 / \u5931\u8d25 0"


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
