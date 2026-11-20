"""Archived MiniQMT status, kernel and read-side contracts."""


from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.services.miniqmt_execution_runtime import JsonFileMiniQMTExecutionRuntimeRepository
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
)
from backend.services.miniqmt_execution_runtime.repository import InMemoryMiniQMTExecutionRuntimeRepository
from backend.services.paper_trading_v2.live_dashboard import PaperTradingLiveDashboardService
from backend.services.paper_trading_v2.models import (
    PaperRun,
    PaperSessionDay,
    PaperSessionMode,
    PaperSessionPhase,
    PaperSessionStatus,
    PaperTradingSession,
)
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    InMemorySimulationRuntimeRepository,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationRuntimeOpsService,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime import ops as simulation_ops
from backend.services.strategy_package.selection_artifact import InMemorySelectionScoreArtifactRepository
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide, RunStatus
from backend.tests.paper_trading_v2.test_live_dashboard import (
    _portfolio,
)
from backend.tests.simulation_runtime.test_ops_api import (
    MINIQMT_B0_QUOTE_CONTROL,
    TRADE_DATE,
    _PlatformDiagnosticsBackgroundScheduler,
    client as _client_fixture,
    repo_with_plan as _repo_with_plan_fixture,
)
from backend.tests.simulation_runtime.test_router_summary import (
    _client,
    _runtime,
    _runtime_repo,
)

client = _client_fixture
repo_with_plan = _repo_with_plan_fixture


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


def test_platform_diagnostics_projects_k2_kernel_facts_read_only_and_auto_clears() -> None:
    repo = InMemorySimulationRuntimeRepository()
    scheduler = _PlatformDiagnosticsBackgroundScheduler(
        last_run_at=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
        market_phase="OPEN_AM",
        kernel_product_runtimes=[
            {
                "runtime_id": "runtime_k2d_diagnostics",
                "binding_id": "binding_k6d_diagnostics",
                "trade_date": TRADE_DATE.isoformat(),
                "source_capability_sha256": "8" * 64,
            }
        ],
    )
    durable_payload = {
        "schema_version": "miniqmt_kernel_diagnostics_v1",
        "schema_status": "READY",
        "runtime_id": "runtime_k2d_diagnostics",
        "trade_date": TRADE_DATE.isoformat(),
        "missing_tables": [],
        "event_type_counts": {"TIMER": 2},
        "delivery_status_counts": {"APPLIED": 2},
        "outbox_status_counts": {"OUTCOME_UNKNOWN": 1},
        "outbox_command_type_counts": {"SUBMIT_LIMIT": 1},
        "timer_status_counts": {"EMITTED": 2},
        "timer_occurrence_status_counts": {"EVENT_COMMITTED": 2},
        "diagnostic_reason_family_counts": {"OUTCOME_UNKNOWN": 1},
        "predecessor_gap_count": 0,
        "mapping_lineage_pending_count": 0,
        "expired_dispatching_lease_count": 0,
        "oldest_delivery_lag_seconds": 0,
        "oldest_due_timer_lag_seconds": 0,
        "runtime_status": "ACTIVE",
        "product_route": {
            "schema_version": "miniqmt_k6d_product_route_diagnostics_v1",
            "status": "ACTIVE",
            "runtime_id": "runtime_k2d_diagnostics",
            "binding_id": "binding_k6d_diagnostics",
            "trade_date": TRADE_DATE.isoformat(),
            "route_owner": "KERNEL_V2",
            "route_epoch": 1,
            "effective_new_instance_sequence": 1,
            "owner_row_version": 1,
            "owner_sha256": "1" * 64,
            "current_receipt_sha256": "2" * 64,
            "legacy_active_instance_count": 0,
            "kernel_active_instance_count": 1,
            "cutover_legacy_active_instance_count": 0,
            "cutover_kernel_active_instance_count": 0,
            "catalog_sha256": "3" * 64,
            "gateway_capability_catalog_sha256": "4" * 64,
            "exchange_session_authority_sha256": "5" * 64,
            "migration_readback_sha256": "6" * 64,
            "product_authority_schema_sha256": "7" * 64,
            "coordination_status_counts": {},
            "read_only": True,
        },
        "recent_command_chains": [],
        "limit": 100,
        "truncated": False,
        "next_cursor": None,
        "read_only": True,
    }
    calls: list[dict[str, Any]] = []
    read_failure = False

    def reader(**values: Any) -> dict[str, Any]:
        calls.append(dict(values))
        if read_failure:
            raise RuntimeError("injected kernel scalar readback drift")
        return dict(durable_payload)

    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=scheduler,  # type: ignore[arg-type]
        kernel_diagnostics_reader=reader,
    )
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime_repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id="runtime_k2d_diagnostics",
                account_group_id="account_k2d_diagnostics",
                trade_date=TRADE_DATE,
                runtime_config_hash="d" * 64,
            ).model_dump()
        )
    )
    degraded = service.platform_diagnostics(
        trade_date=TRADE_DATE,
        runtime_id="runtime_k2d_diagnostics",
        runtime_repository=runtime_repository,
        generated_at=datetime(2026, 7, 27, 1, 30, 10, tzinfo=UTC),
    )
    assert calls == [{"runtime_id": "runtime_k2d_diagnostics", "trade_date": TRADE_DATE, "limit": 100, "cursor": None}]
    assert degraded["layers"]["miniqmt_kernel"]["status"] == "BLOCKED"
    assert degraded["layers"]["miniqmt_k6d"]["status"] == "HEALTHY"
    assert degraded["layers"]["miniqmt_k6d"]["execution_gate"] is False
    assert any(item["alert_type"] == "MINIQMT_KERNEL_DURABLE_HEALTH" for item in degraded["alerts"]["items"])
    assert degraded["side_effect_contract"]["read_only"] is True
    assert degraded["alerts"]["acknowledge_required"] is False
    assert all(
        not ({"runtime_id", "algo_instance_id", "command_id", "symbol"} & set(item["labels"]))
        for item in degraded["metrics"]["series"]
    )

    durable_payload["outbox_status_counts"] = {"ACKED": 1}
    durable_payload["diagnostic_reason_family_counts"] = {}
    recovered = service.platform_diagnostics(
        trade_date=TRADE_DATE,
        runtime_id="runtime_k2d_diagnostics",
        runtime_repository=runtime_repository,
        generated_at=datetime(2026, 7, 27, 1, 30, 20, tzinfo=UTC),
    )
    assert recovered["layers"]["miniqmt_kernel"]["status"] == "HEALTHY"
    assert not any(item["alert_type"] == "MINIQMT_KERNEL_DURABLE_HEALTH" for item in recovered["alerts"]["items"])

    read_failure = True
    failed = service.platform_diagnostics(
        trade_date=TRADE_DATE,
        runtime_id="runtime_k2d_diagnostics",
        runtime_repository=runtime_repository,
        generated_at=datetime(2026, 7, 27, 1, 30, 30, tzinfo=UTC),
    )
    assert failed["layers"]["miniqmt_kernel"]["status"] == "BLOCKED"
    assert any(
        item["reason_code"] == "MINIQMT_KERNEL_READBACK_FAILED" and item["status"] == "CRITICAL"
        for item in failed["alerts"]["items"]
    )


def test_runtime_projection_does_not_treat_pre_broker_child_rejection_as_broker_call() -> None:
    runtime_id = "runtime_projection_pre_broker_rejection"
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime_repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id=runtime_id,
                account_group_id="ag_projection_pre_broker_rejection",
                trade_date=TRADE_DATE,
                runtime_config_hash="c" * 64,
            ).model_dump()
        )
    )
    child = runtime_repository.upsert_child_order(
        MiniQMTChildOrder(
            child_order_id="child_projection_pre_broker_rejection",
            runtime_id=runtime_id,
            algo_instance_id="algo_projection_pre_broker_rejection",
            parent_intent_id="parent_projection_pre_broker_rejection",
            strategy_slot_id="slot_projection_pre_broker_rejection",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            price=10.0,
            status=MiniQMTChildOrderStatus.REJECTED,
            metadata={"gateway_ack": {"broker_called": False, "error_code": "QMT_PLACE_ORDER_UNAVAILABLE"}},
        )
    )
    runtime_repository.append_event(
        MiniQMTExecutionEvent(
            event_id="event_projection_pre_broker_rejection",
            runtime_id=runtime_id,
            sequence=1,
            event_type=MiniQMTExecutionEventType.CHILD_ORDER_REJECTED,
            source="gateway",
            payload={
                "child_order_id": child.child_order_id,
                "broker_order_id": None,
                "accepted": False,
                "broker_called": False,
            },
        )
    )

    result = simulation_ops._miniqmt_runtime_projection_consistency(
        run=None,
        runtime_repository=runtime_repository,
        runtime_id=runtime_id,
    )

    assert result["actual"]["broker_called"] is False


def test_platform_diagnostics_degrades_stale_miniqmt_projection_and_auto_clears(
    repo_with_plan: tuple[InMemorySimulationRuntimeRepository, str, str],
) -> None:
    repo, source_run_id, _plan_id = repo_with_plan
    source_run = repo.get_simulation_daily_run(source_run_id)
    release = repo.get_strategy_runtime_release(source_run.release_id)
    binding = StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id="strategy_platform_projection_stale",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        account_group_id="ag_platform_projection_stale",
        strategy_slot_id="slot_platform_projection_stale",
        strategy_name="PlatformProjectionStale",
        order_remark_prefix="platform-projection-stale",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        created_by="unit-test",
        created_reason="platform diagnostics runtime projection consistency",
    )
    runtime_id = "runtime_platform_projection_stale"
    batch_id = "batch_platform_projection_stale"
    pending_result = {
        "success": False,
        "intent_id": "parent_platform_projection_stale",
        "qmt_order_id": None,
        "broker_message": "event_loop algo dispatched and running; child order pending tick trigger",
        "broker_called": False,
        "preflight": {"allowed": True},
    }
    run = source_run.model_copy(
        update={
            "run_id": "simrun_platform_projection_stale",
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
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": 0,
                "pending_intents": 1,
                "qmt_batch_result": {
                    "batch_id": batch_id,
                    "batch_status": "SUBMITTING",
                    "results": [pending_result],
                    "total": 1,
                    "success": True,
                    "succeeded": 0,
                    "failed": 0,
                    "pending": 1,
                    "runtime_evidence": {
                        "runtime_id": runtime_id,
                        "child_order_ids": [],
                        "submitted_child_count": 0,
                        "rejected_child_count": 0,
                        "trade_event_count": 0,
                    },
                },
            },
        }
    )
    repo.save_simulation_daily_run(run)
    runtime_repository = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime_repository.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id=runtime_id,
                account_group_id=binding.account_group_id or "ag_platform_projection_stale",
                trade_date=TRADE_DATE,
                runtime_config_hash="b" * 64,
            ).model_dump()
        )
    )
    child = runtime_repository.upsert_child_order(
        MiniQMTChildOrder(
            child_order_id="child_platform_projection_stale",
            runtime_id=runtime_id,
            algo_instance_id="algo_platform_projection_stale",
            parent_intent_id="parent_platform_projection_stale",
            strategy_slot_id=binding.strategy_slot_id or "slot_platform_projection_stale",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            price=10.0,
            status=MiniQMTChildOrderStatus.SUBMITTED,
            broker_order_id="100001",
        )
    )
    runtime_repository.append_event(
        MiniQMTExecutionEvent(
            event_id="event_platform_projection_stale",
            runtime_id=runtime_id,
            sequence=1,
            event_type=MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
            source="gateway",
            payload={
                "child_order_id": child.child_order_id,
                "broker_order_id": child.broker_order_id,
                "accepted": True,
            },
        )
    )
    service = SimulationRuntimeOpsService(
        repository=repo,
        scheduler=_PlatformDiagnosticsBackgroundScheduler(last_run_at=datetime.now(UTC)),  # type: ignore[arg-type]
    )

    stale = service.platform_diagnostics(
        trade_date=TRADE_DATE,
        runtime_id=runtime_id,
        runtime_repository=runtime_repository,
    )

    assert stale["runtime_projection_consistency"]["status"] == "STALE"
    durability = stale["layers"]["durability"][0]
    assert durability["status"] == "DEGRADED"
    assert durability["reason_code"] == "MINIQMT_RUNTIME_PROJECTION_STALE"
    assert stale["overall_health"]["status"] != "HEALTHY"
    assert any(alert["alert_type"] == "MINIQMT_RUNTIME_PROJECTION_STALE" for alert in stale["alerts"]["items"])

    accepted_result = {
        **pending_result,
        "success": True,
        "qmt_order_id": child.broker_order_id,
        "broker_message": "accepted",
        "broker_called": True,
    }
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={
            "broker_called": True,
            "submitted_intents": 1,
            "pending_intents": 0,
            "qmt_batch_result": {
                "batch_id": batch_id,
                "batch_status": "SUCCEEDED",
                "results": [accepted_result],
                "total": 1,
                "success": True,
                "succeeded": 1,
                "failed": 0,
                "pending": 0,
                "runtime_evidence": {
                    "runtime_id": runtime_id,
                    "child_order_ids": [child.child_order_id],
                    "submitted_child_count": 1,
                    "rejected_child_count": 0,
                    "trade_event_count": 0,
                },
            },
        },
    )
    consistent = service.platform_diagnostics(
        trade_date=TRADE_DATE,
        runtime_id=runtime_id,
        runtime_repository=runtime_repository,
    )
    assert consistent["runtime_projection_consistency"]["status"] == "CONSISTENT"
    assert consistent["layers"]["durability"][0]["reason_code"] == "MINIQMT_DURABLE_BATCH_VALID"
    assert not any(alert["alert_type"] == "MINIQMT_RUNTIME_PROJECTION_STALE" for alert in consistent["alerts"]["items"])


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
                        "submitted_child_count": 0,
                        "rejected_child_count": 0,
                        "pending_algo_count": 1,
                        "trade_event_count": 0,
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


def test_miniqmt_shadow_evidence_endpoint_is_retired() -> None:
    client = _client(runtime_repo=_runtime_repo())

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    )

    assert response.status_code == 404


def test_miniqmt_runtime_events_endpoint_is_loud_when_runtime_missing() -> None:
    client = _client()

    response = client.get("/api/v1/simulation-runtime/miniqmt/runtime-events", params={"runtime_id": "missing_runtime"})

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["error_code"] == "DATA_UNAVAILABLE"
    assert detail["context"]["reason_code"] == "MINIQMT_RUNTIME_NOT_FOUND"
    assert detail["context"]["runtime_id"] == "missing_runtime"


def test_miniqmt_gray_state_endpoint_is_retired() -> None:
    client = _client(runtime_repo=_runtime_repo())

    response = client.get(
        "/api/v1/simulation-runtime/miniqmt/gray-state",
        params={"portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    )

    assert response.status_code == 404


def test_miniqmt_runtime_readonly_endpoints_do_not_mutate_json_store(tmp_path) -> None:
    store_path = tmp_path / "runtime-state.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    repo.upsert_runtime(_runtime(metadata={"compiler_route_retired": True, "runtime_route": "A_EVENT_LOOP"}))
    repo._write_snapshot(reason="readonly_test_seed")
    before = store_path.read_text(encoding="utf-8")
    client = _client(runtime_repo=repo)

    assert client.get("/api/v1/simulation-runtime/miniqmt/runtime-events", params={"runtime_id": "mqrt_event_loop_scope"}).status_code == 200
    assert client.get(
        "/api/v1/simulation-runtime/miniqmt/shadow-evidence",
        params={"trade_date": "2026-06-25", "portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    ).status_code == 404
    assert client.get(
        "/api/v1/simulation-runtime/miniqmt/gray-state",
        params={"portfolio_id": "portfolio_l16", "strategy_slot_id": "slot_l16"},
    ).status_code == 404

    assert store_path.read_text(encoding="utf-8") == before


def test_live_dashboard_surfaces_miniqmt_capacity_residual_warning() -> None:
    repo = InMemoryPaperTradingV2Repository()
    portfolio, _manifest_sha = _portfolio(repo)
    session = PaperTradingSession(
        session_id="psess_capacity_residual_dash",
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        status=PaperSessionStatus.SUCCEEDED,
        phase=PaperSessionPhase.LIVE_INTRADAY,
        start_date=date(2024, 1, 3),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    repo.create_session(session)
    observability = {
        "schema_version": "miniqmt_capacity_residual_observability_v1",
        "succeeded_with_capacity_residual": True,
        "reason": "miniqmt_capacity_residual_skipped_and_reconciled",
        "source": "simulation_runtime",
        "capacity_residual_count": 2,
        "failed_intents": 2,
        "qmt_batch_id": "batch_dash_capacity",
        "qmt_batch_status": "PARTIAL",
    }
    run = PaperRun(
        run_id="prun_capacity_residual_dash",
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
        status=RunStatus.SUCCEEDED,
        data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"miniqmt_capacity_residual_observability": observability},
    )
    repo.create_run(run)
    repo.save_session_day(
        PaperSessionDay(
            session_id=session.session_id,
            portfolio_id=portfolio.portfolio_id,
            trade_date=run.trade_date,
            run_id=run.run_id,
            status=PaperSessionStatus.SUCCEEDED,
            phase=PaperSessionPhase.LIVE_INTRADAY,
            data_source=MinuteDataSource.TDX_REALTIME,
        )
    )

    dashboard = PaperTradingLiveDashboardService(
        repository=repo,
        artifact_repository=InMemorySelectionScoreArtifactRepository(),
    ).get_dashboard(portfolio.portfolio_id)

    assert dashboard["miniqmt_capacity_residual_observability"]["succeeded_with_capacity_residual"] is True
    assert dashboard["miniqmt_capacity_residual_observability"]["capacity_residual_count"] == 2
    warning = next(item for item in dashboard["warnings"] if item["code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL")
    assert warning["failed_intents"] == 2
