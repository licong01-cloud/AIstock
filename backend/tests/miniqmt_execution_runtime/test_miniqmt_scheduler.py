"""Retired MiniQMT scheduler, tick and broker execution contracts."""


from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from datetime import time as wall_time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import backend.services.simulation_runtime.bridges as simulation_bridges
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    quote_contract_error,
)
from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_REJECTED,
    OrderBatchStatus,
    OrderLedgerRecord,
    PositionLotRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.selection_center.models import SelectionCandidate
from backend.services.simulation_data.daily_context import SimulationBrokerBackend
from backend.services.simulation_runtime import (
    InMemorySimulationRuntimeRepository,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationLifecycleBackgroundScheduler,
    SimulationRunContext,
    SimulationSchedulerBindingResult,
    SimulationSchedulerRunOnceResult,
    StaticSimulationRunContextProvider,
)
from backend.services.simulation_runtime.lifecycle import (
    MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW,
    compute_schedule_windows,
)
from backend.services.simulation_runtime.miniqmt_quote_activation import MiniQMTKernelProductSyncError
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import PositionLot
from backend.tests.simulation_runtime.test_lifecycle_scheduler import (
    TRADE_DATE,
    FakeManagedOrderBroker,
    FakePreTradeTradabilityProvider,
    FakeSelectionService,
    SimulationLifecycleScheduler,
    _candidate_rows,
    _CapturingDailyTradingContextProvider,
    _deterministic_scheduler_now,  # noqa: F401 - retain the shared autouse fixture
    _frozen_manifest,
    _make_test_binding,
    _make_test_release,
    _managed_buy_request,
    _miniqmt_event_loop_test_scheduler,
    _miniqmt_runtime_repository_test_only,  # noqa: F401 - retain the shared autouse fixture
    _miniqmt_two_strategy_scheduler,
    _position_context,
    _qmt_account,
    _release_and_bindings,
    _stage_exact_kernel_failure,
    _tick_driver_batch_result_row,
    _tick_driver_persist_fixture,
    _tick_driver_result_payload,
)


def test_scheduler_miniqmt_uses_only_kernel_v2_product_root_and_is_restart_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, repo, broker, _binding = _miniqmt_event_loop_test_scheduler()
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME", "compiler")

    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "submit_event_loop_plan")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "drive_event_loop_ticks")
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    assert first.results[0].status == "MINIQMT_KERNEL_V2_ACTIVE"
    assert second.results[0].status == "REUSED_EXISTING_PLAN"
    assert len(scheduler._test_k6d_start_calls) == 1
    payload = repo.get_simulation_daily_run(first.results[0].run.run_id).run_payload_json
    assert payload["miniqmt_runtime_route"]["route"] == "KERNEL_V2"
    assert payload["miniqmt_runtime_route"]["quote_source"] == "B0_QUOTE_V2"
    assert payload["broker_called"] is False
    assert payload["submitted_intents"] == 0
    assert broker.place_order_payloads == []


def test_scheduler_miniqmt_product_root_failure_uses_durable_backoff_without_plan_rebuild() -> None:
    scheduler, repo, broker, _binding = _miniqmt_event_loop_test_scheduler()
    product_root_calls = 0
    full_context_calls = 0
    existing_context_calls = 0
    retry_claim_calls = 0
    base_provider = scheduler.context_provider
    original_retry_claim = repo.claim_simulation_retry_attempt

    def count_retry_claim(**values: Any) -> Any:
        nonlocal retry_claim_calls
        retry_claim_calls += 1
        return original_retry_claim(**values)

    repo.claim_simulation_retry_attempt = count_retry_claim  # type: ignore[method-assign]

    class PhaseAwareProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            nonlocal full_context_calls
            full_context_calls += 1
            return base_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        def load_existing_plan_context(self, *, runtime_release, binding, plan, trade_date, as_of_time):
            nonlocal existing_context_calls
            existing_context_calls += 1
            return base_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

    scheduler.context_provider = PhaseAwareProvider()

    def fail_product_runtime(**_values: Any) -> Any:
        nonlocal product_root_calls
        product_root_calls += 1
        raise RuntimeError('column "release_hash" does not exist')

    scheduler.orchestrator.miniqmt_product_runtime_factory = fail_product_runtime
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(seconds=30),
    )
    third = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    assert first.results[0].status == "FAILED_RETRYABLE"
    assert second.results[0].status == "RETRY_BACKOFF"
    assert second.results[0].error is None
    assert second.results[0].lifecycle_diagnostic["reason_code"] == "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE"
    assert third.results[0].status == "FAILED_RETRYABLE"
    assert third.results[0].run is not None
    run = repo.get_simulation_daily_run(third.results[0].run.run_id)
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert run.run_payload_json["last_stage"] == "FAILED_RETRYABLE"
    assert run.run_payload_json["broker_called"] is False
    assert run.run_payload_json["submitted_intents"] == 0
    assert run.run_payload_json["failed_intents"] == 0
    assert run.run_payload_json["submit_failure"] == {
        "stage": "MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        "outer_stage": "MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        "type": "RuntimeError",
        "message": 'column "release_hash" does not exist',
        "context": None,
    }
    retry_control = run.run_payload_json["simulation_scheduler_retry_control_v1"]
    retry_entry = retry_control["entries"]["BINDING_FAILED_RETRYABLE"]
    assert retry_entry["failure_stage"] == "MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED"
    assert retry_entry["consecutive_failure_count"] == 2
    assert retry_entry["attempt_count"] == 2
    assert retry_entry["next_retry_at"] == (observed + timedelta(minutes=3)).astimezone(UTC).isoformat()
    assert product_root_calls == 2
    assert full_context_calls == 1
    assert existing_context_calls == 1
    assert retry_claim_calls == 1
    assert first.results[0].run.execution_plan_id == second.results[0].execution_plan.plan_id
    assert second.results[0].execution_plan.plan_id == third.results[0].run.execution_plan_id
    assert broker.place_order_payloads == []


@pytest.mark.parametrize(
    ("failure_mode", "expected_stage", "expected_type"),
    (
        (
            "coordinator_missing",
            "MINIQMT_KERNEL_V2_PRODUCT_ROOT_UNAVAILABLE",
            "BrokerUnavailableError",
        ),
        (
            "worker_incarnation_missing",
            "MINIQMT_KERNEL_V2_WORKER_INCARNATION_MISSING",
            "RuntimeConfigInvalidError",
        ),
    ),
)
def test_scheduler_miniqmt_invalid_product_root_is_persisted_before_broker(
    failure_mode: str,
    expected_stage: str,
    expected_type: str,
) -> None:
    scheduler, repo, broker, _binding = _miniqmt_event_loop_test_scheduler()
    if failure_mode == "coordinator_missing":
        product_runtime = SimpleNamespace(coordinator=None, worker_incarnation_id="worker_k6d")
    else:
        product_runtime = SimpleNamespace(
            coordinator=SimpleNamespace(start_execution_plan_v1=lambda **_values: None),
            worker_incarnation_id="",
        )
    scheduler.orchestrator.miniqmt_product_runtime_factory = lambda **_values: product_runtime
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )

    assert result.results[0].status == "FAILED_RETRYABLE"
    assert result.results[0].run is not None
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert run.run_payload_json["broker_called"] is False
    assert run.run_payload_json["submitted_intents"] == 0
    assert run.run_payload_json["failed_intents"] == 0
    failure = run.run_payload_json["submit_failure"]
    assert failure["stage"] == expected_stage
    assert failure["outer_stage"] == expected_stage
    assert failure["type"] == expected_type
    assert broker.place_order_payloads == []


def test_scheduler_miniqmt_kernel_v2_keeps_two_bindings_independent() -> None:
    scheduler, _repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )

    assert {item.binding_id for item in result.results} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in result.results} == {"MINIQMT_KERNEL_V2_ACTIVE"}
    assert {item["binding_id"] for item in scheduler._test_k6d_start_calls} == {
        binding_a.binding_id,
        binding_b.binding_id,
    }
    assert broker.place_order_payloads == []


def test_scheduler_kernel_product_tick_failure_isolated_to_owning_binding() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    first_by_binding = {item.binding_id: item for item in first.results}
    activation = scheduler._miniqmt_quote_ingress_activation
    original, _plan, runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
    )

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    by_binding = {item.binding_id: item for item in result.results}
    assert by_binding[binding_a.binding_id].status == original.status.value
    assert by_binding[binding_a.binding_id].error["context"]["reason_code"] == (
        "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED"
    )
    assert by_binding[binding_a.binding_id].error["context"]["broker_side_effect_state"] == "UNKNOWN"
    assert "broker_called" not in by_binding[binding_a.binding_id].error["context"]
    failed_run = repo.get_simulation_daily_run(by_binding[binding_a.binding_id].run.run_id)
    assert failed_run.run_payload_json["broker_side_effect_state"] == "UNKNOWN"
    assert "broker_called" not in failed_run.run_payload_json
    assert "submitted_intents" not in failed_run.run_payload_json
    assert "failed_intents" not in failed_run.run_payload_json
    assert failed_run.run_payload_json["pre_run_failure"]["broker_side_effect_state"] == "UNKNOWN"
    assert "reconcile broker and durable outbox state" in failed_run.run_payload_json["pre_run_failure"]["next_action"]
    assert scheduler._run_has_broker_side_effect_evidence(failed_run) is True
    assert by_binding[binding_b.binding_id].status == "REUSED_EXISTING_PLAN"
    assert failed_run.execution_plan_id == original.execution_plan_id
    assert runtime_id in failed_run.run_payload_json["pre_run_failure"]["context"]["ordered_failures"][0]["runtime_id"]
    assert first_by_binding[binding_b.binding_id].run.run_id == by_binding[binding_b.binding_id].run.run_id
    assert broker.place_order_payloads == []


def test_scheduler_unmatched_kernel_product_tick_failure_is_not_silently_successful() -> None:
    scheduler, _repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError(
            (
                {
                    "runtime_id": "runtime_not_in_current_binding_page",
                    "binding_id": "simbind_not_in_current_binding_page",
                    "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_SYNC_FAILED",
                    "exception_type": "RuntimeError",
                    "exception_message": "injected failure outside the current binding page",
                },
            )
        )

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    # Every current peer is attempted before the bounded unmatched aggregate is
    # surfaced.  A stale/limit-excluded activation failure must not turn this
    # scheduler tick into failed_count=0.
    assert {item.binding_id for item in result.results[:2]} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in result.results[:2]} == {"MINIQMT_KERNEL_V2_ACTIVE"}
    assert result.failed_count == 1
    unmatched = result.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    assert unmatched.error["type"] == "MiniQMTKernelProductSyncError"
    assert unmatched.error["context"]["reason_code"] == "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_UNMATCHED"
    assert unmatched.error["context"]["failure_count"] == 1
    assert unmatched.error["context"]["ordered_failures"][0]["runtime_id"] == ("runtime_not_in_current_binding_page")
    assert unmatched.error["context"]["ordered_failures"][0]["binding_id"] == ("simbind_not_in_current_binding_page")
    assert broker.place_order_payloads == []


def test_scheduler_expired_kernel_product_failure_is_aggregated_after_current_peers() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    _run, _plan, runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        runtime_trade_date=TRADE_DATE - timedelta(days=1),
    )

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    current = result.results[:-1]
    assert {item.binding_id for item in current} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in current} == {"REUSED_EXISTING_PLAN"}
    assert result.failed_count == 1
    unmatched = result.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    failure = unmatched.error["context"]["ordered_failures"][0]
    assert failure["runtime_id"] == runtime_id
    assert failure["binding_id"] == binding_a.binding_id
    assert failure["scheduler_match_state"] == "RUNTIME_TRADE_DATE_STALE"
    current_run = repo.get_simulation_daily_run_by_key(
        strategy_id=binding_a.strategy_id,
        binding_id=binding_a.binding_id,
        trade_date=TRADE_DATE,
    )
    assert current_run.execution_plan_id
    assert "pre_run_failure" not in current_run.run_payload_json
    assert broker.place_order_payloads == []


@pytest.mark.parametrize(
    ("runtime_generation", "runtime_attempt", "failure_generation", "failure_attempt", "expected_state"),
    [
        (2, 1, 1, 1, "RUNTIME_LIFECYCLE_GENERATION_STALE"),
        (1, 2, 1, 1, "RUNTIME_ATTEMPT_TOKEN_STALE"),
    ],
)
def test_scheduler_stale_kernel_attempt_never_pollutes_or_releases_current_plan(
    runtime_generation: int,
    runtime_attempt: int,
    failure_generation: int,
    failure_attempt: int,
    expected_state: str,
) -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    original = {item.binding_id: item for item in first.results}[binding_a.binding_id].run
    assert original is not None
    _run, _plan, runtime_id, failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        lifecycle_generation=runtime_generation,
        attempt_token=runtime_attempt,
    )
    failure["lifecycle_generation"] = failure_generation
    failure["attempt_token"] = failure_attempt

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    latest = repo.get_simulation_daily_run(original.run_id)
    unmatched = result.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    assert unmatched.error["context"]["ordered_failures"][0]["scheduler_match_state"] == expected_state
    assert latest == original
    assert scheduler._miniqmt_quote_ingress_activation.released_runtime_ids == []
    assert {item.binding_id: item for item in result.results}[binding_b.binding_id].error is None
    assert runtime_id not in scheduler._miniqmt_quote_ingress_activation.released_runtime_ids
    assert broker.place_order_payloads == []


def test_miniqmt_shadow_bridge_api_is_removed_from_scheduler_path() -> None:
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "run_shadow_reconciliations")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "run_shadow_reconciliation")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "_shadow_scenarios")


def test_miniqmt_event_loop_bridge_product_methods_are_physically_retired() -> None:
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "submit_event_loop_plan")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "drive_event_loop_ticks")
    assert not hasattr(SimulationLifecycleScheduler, "_drive_miniqmt_event_loop_ticks")


def test_miniqmt_compiler_submit_route_rejects_loudly() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    bridge = simulation_bridges.MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=qmt_repo, broker=FakeManagedOrderBroker()),  # type: ignore[arg-type]
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(repository=qmt_repo, broker=FakeManagedOrderBroker()),  # type: ignore[arg-type]
                    qmt_ledger_repository=qmt_repo,
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        bridge.submit_plan(
            plan=planned.results[0].execution_plan,
            binding=qmt_binding,
            mode="SIM",
            price_by_symbol={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_SIM_COMPILER_ROUTE_RETIRED"
    assert exc_info.value.context["stage"] == "MINIQMT_COMPILER_SUBMIT_REJECTED"
    assert exc_info.value.context["broker_called"] is False


def test_miniqmt_managed_order_disconnect_freezes_until_reconnect_reconcile() -> None:
    account_id = "QMT_DISCONNECT_ACCOUNT"
    strategy_name = "DisconnectFreezeStrategy"
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    _qmt_account(qmt_repo, account_id=account_id, strategy_name=strategy_name)
    broker = FakeManagedOrderBroker(fail_next_place=True, connect_ok=False)
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    first = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-first")]
    )

    assert first.success is False
    assert first.results[0].broker_called is True
    assert first.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"
    assert service.broker_disconnect_freeze_status()["frozen"] is True
    assert len(broker.place_order_payloads) == 1

    frozen = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-blocked")]
    )

    assert frozen.success is False
    assert frozen.results[0].broker_called is False
    assert frozen.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"
    assert frozen.results[0].preflight.primary_error.context["recovery"]["stage"] == "BROKER_STILL_DISCONNECTED"
    assert len(broker.place_order_payloads) == 1

    broker.connect_ok = True
    recovered = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-recovered")]
    )

    assert recovered.success is True
    assert recovered.results[0].broker_called is True
    assert broker.order_query_calls >= 1
    assert broker.trade_query_calls >= 1
    assert len(broker.place_order_payloads) == 2
    status = service.broker_disconnect_freeze_status()
    assert status["frozen"] is False
    assert status["last_recovery"]["reason_code"] == "MINIQMT_BROKER_RECONNECTED_RECONCILED"


def test_miniqmt_managed_order_reconnect_reconcile_failure_keeps_freeze_without_submit() -> None:
    account_id = "QMT_RECONNECT_FAIL_ACCOUNT"
    strategy_name = "DisconnectFreezeReconcileFailStrategy"
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    _qmt_account(qmt_repo, account_id=account_id, strategy_name=strategy_name)
    broker = FakeManagedOrderBroker(fail_next_place=True, connect_ok=True, fail_order_query=True)
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    first = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-first")]
    )
    assert first.success is False
    assert first.results[0].broker_called is True
    assert first.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"

    frozen = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-blocked")]
    )

    assert frozen.success is False
    assert frozen.results[0].broker_called is False
    error = frozen.results[0].preflight.primary_error
    assert error.code == "MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED"
    assert error.context["recovery"]["stage"] == "RECONNECT_RECONCILE_FAILED"
    assert service.broker_disconnect_freeze_status()["frozen"] is True
    assert broker.order_query_calls == 1
    assert broker.trade_query_calls == 0
    assert len(broker.place_order_payloads) == 1


def test_scheduler_sizes_miniqmt_targets_from_dynamic_strategy_slot_equity() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            frozen_cash=Decimal("10000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    candidates = [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        )
    ]
    context = SimulationRunContext(
        portfolio_id=qmt_binding.strategy_id,
        current_positions={
            "000003.SZ": PositionLot(
                portfolio_id=qmt_binding.strategy_id,
                symbol="000003.SZ",
                quantity=1000,
                available_quantity=1000,
                avg_cost=9.0,
                trade_date=date(2026, 5, 20),
            )
        },
        current_prices={"000003.SZ": 20.0},
        qmt_ledger_repository=qmt_repo,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=candidates),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    plan = result.results[0].execution_plan
    assert plan is not None
    buy = next(intent for intent in plan.intents if intent.symbol == "000001.SZ")
    assert buy.order_quantity == 1300
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    basis = run.run_payload_json["target_equity_basis"]
    assert basis["source"] == "miniqmt_strategy_slot_dynamic_equity"
    assert basis["cash"] == 100_000.0
    assert basis["frozen_cash"] == 10_000.0
    assert basis["market_value"] == 20_000.0
    assert basis["total_equity"] == 130_000.0
    assert basis["capital_allocation"] == 100_000.0


def test_scheduler_rolls_forward_expired_miniqmt_binding_for_unattended_daily_runs() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    qmt_binding = qmt_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[qmt_binding.binding_id] = qmt_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_miniqmt_roll_forward")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    rerun = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    rolled_run = result.results[0].run
    assert rolled_run is not None
    assert rolled_run.binding_id != qmt_binding.binding_id
    assert rolled_run.account_group_id == qmt_binding.account_group_id
    assert rolled_run.strategy_slot_id == qmt_binding.strategy_slot_id
    new_binding = repo.get_simulation_release_binding(rolled_run.binding_id)
    assert new_binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
    assert new_binding.account_group_id == qmt_binding.account_group_id
    assert new_binding.strategy_slot_id == qmt_binding.strategy_slot_id
    assert new_binding.strategy_name == qmt_binding.strategy_name
    assert new_binding.order_remark_prefix == qmt_binding.order_remark_prefix
    assert new_binding.effective_from == next_trade_day
    assert new_binding.effective_to == next_trade_day
    assert new_binding.binding_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert new_binding.binding_config_json["metadata"]["extends_binding_id"] == qmt_binding.binding_id
    new_release = repo.get_strategy_runtime_release(new_binding.release_id)
    assert new_release.base_release_id == release.release_id
    assert new_release.effective_from == next_trade_day
    assert new_release.effective_to == next_trade_day
    assert new_release.execution_policy_version_id == release.execution_policy_version_id
    assert new_release.execution_policy_sha256 == release.execution_policy_sha256
    assert new_release.release_config_json["execution_policy"] == release.release_config_json["execution_policy"]
    assert new_release.release_config_json["metadata"]["execution_policy_authority"] == {
        "schema_version": "simulation_roll_forward_execution_policy_authority_v1",
        "source_policy_version_id": release.execution_policy_version_id,
        "source_policy_sha256": release.execution_policy_sha256,
        "effective_policy_version_id": release.execution_policy_version_id,
        "effective_policy_sha256": release.execution_policy_sha256,
        "authority_source": "source_runtime_release",
        "source_policy_consulted_for_execution": True,
        "fallback_used": False,
    }
    assert new_release.release_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == new_binding.binding_id


def test_scheduler_miniqmt_open_order_evidence_excludes_terminal_xtquant_statuses() -> None:
    _release, _, qmt_binding, _repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for status in (STATUS_CANCELLED, STATUS_FILLED, STATUS_REJECTED):
        qmt_repo.upsert_order_ledger(
            OrderLedgerRecord(
                intent_id=f"intent_terminal_{status}",
                strategy_id=qmt_binding.strategy_id,
                strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
                qmt_order_id=f"terminal_{status}",
                symbol="000003.SZ",
                order_type=SELL_ORDER_TYPE,
                order_volume=100,
                traded_volume=50 if status != STATUS_FILLED else 100,
                order_status=status,
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                status_msg=f"terminal xtquant status {status}",
                order_remark=f"remark_terminal_{status}",
            )
        )

    evidence = SimulationLifecycleScheduler._miniqmt_open_order_evidence(
        binding=qmt_binding,
        run=SimpleNamespace(trade_date=TRADE_DATE, run_payload_json={}),
        context=SimulationRunContext(current_positions={}, qmt_ledger_repository=qmt_repo),
    )

    assert evidence["open_order_count"] == 0
    assert evidence["open_orders"] == []


def test_scheduler_rejects_fresh_miniqmt_submit_outside_shared_window_without_broker_call() -> None:
    scheduler, repo, broker, _qmt_binding = _miniqmt_event_loop_test_scheduler()

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    gate = run.run_payload_json["submit_window_gate"]

    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["active_window"] is None
    blocked_plan = repo.get_execution_plan(run.execution_plan_id)
    assert gate["blocked_intent_count"] == len(blocked_plan.intents)
    assert gate["schedule_windows"] == list(
        compute_schedule_windows(trade_date=TRADE_DATE, as_of_time=datetime(2026, 5, 21, 16, 33))
    )
    assert run.run_payload_json["submit_failure"]["stage"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []


def test_scheduler_terminalizes_stale_historical_miniqmt_planning_runs_before_today_tick() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=InMemoryQmtStrategyLedgerRepository(),
                        broker=FakeManagedOrderBroker(),  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
                    ),
                )
            }
        ),
    )
    stale = scheduler.run_once(
        trade_date=date(2026, 5, 20),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        created_by="codex_final_minqmt_multistrategy_dry_run_20260603",
    )
    stale_run = repo.get_simulation_daily_run(stale.results[0].run.run_id)

    today = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    terminalized = repo.get_simulation_daily_run(stale_run.run_id)

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["run_id"] == stale_run.run_id
    assert terminalized.status == SimulationDailyRunStatus.CANCELLED
    assert terminalized.run_payload_json["stale_active_terminalization"]["previous_status"] == "PLANNING_EXECUTION"
    assert terminalized.run_payload_json["stale_active_terminalization"]["had_broker_side_effect"] is False


def test_scheduler_publishes_b0_context_before_miniqmt_submit_callable() -> None:
    order: list[str] = []

    class _Activation:
        controller_factory = None
        quote_context_adapter = None

        def prepare_runtime_context(self, **kwargs: Any) -> dict[str, object]:
            order.append("context")
            assert kwargs["recovering_active"] is False
            return {"context_id": "context-before-submit"}

    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=_Activation(),  # type: ignore[arg-type]
    )
    binding = SimpleNamespace(
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="binding-context-order",
        strategy_id="strategy-context-order",
    )
    plan = SimpleNamespace(
        plan_id="plan-context-order",
        target_trade_date=TRADE_DATE,
        plan_payload_json={"quote_control": {"binding": {}, "revision": {}, "assignments": []}},
        intents=(SimpleNamespace(symbol="000001.SZ"),),
    )
    run = SimpleNamespace(run_id="run-context-order")
    expected = SimpleNamespace(status="SUBMITTED")

    actual = scheduler._submit_execution_plan_with_timeout(
        build_result=None,
        binding=binding,
        run=run,
        plan=plan,
        context=SimpleNamespace(),
        mode="SIM",
        as_of_time=datetime(2026, 5, 21, 10, 0, tzinfo=UTC),
        submit_callable=lambda: order.append("submit") or expected,
    )

    assert actual is expected
    assert order == ["context", "submit"]


def test_scheduler_lifecycle_tick_pumps_kernel_product_callback_watchdog() -> None:
    calls: list[str] = []

    class _Activation:
        controller_factory = None
        quote_context_adapter = None

        def begin_lifecycle_epoch(self) -> None:
            calls.append("begin")

        def watchdog_tick(self) -> None:
            calls.append("watchdog")

    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=_Activation(),  # type: ignore[arg-type]
    )
    scheduler._advance_miniqmt_quote_ingress_lifecycle()
    assert calls == ["begin", "watchdog"]


def test_scheduler_persists_b0_context_prepare_failure_before_broker_callable() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)

    class _FailingActivation:
        controller_factory = None
        quote_context_adapter = None

        def begin_lifecycle_epoch(self) -> None:
            return None

        def watchdog_tick(self) -> dict[str, object]:
            return {"status": "READY"}

        def prepare_runtime_context(self, **_kwargs: Any) -> dict[str, object]:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "authoritative MiniQMT context unavailable",
                context={"reason_code": "ADAPTIVE_IS_TRADABILITY_DATA_INVALID", "stage": "TRADABILITY"},
            )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: _position_context(portfolio_id="portfolio_context_failure")}
        ),
        miniqmt_quote_ingress_activation=_FailingActivation(),  # type: ignore[arg-type]
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert run is not None
    assert plan is not None
    plan = plan.model_copy(
        update={
            "plan_payload_json": {
                **plan.plan_payload_json,
                "quote_control": {"binding": {}, "revision": {}, "assignments": []},
            }
        }
    )
    repo.execution_plans[plan.plan_id] = plan
    submit_called = False

    def submit_callable() -> Any:
        nonlocal submit_called
        submit_called = True
        raise AssertionError("broker callable must not run after context preparation failure")

    with pytest.raises(QuoteContractError, match="authoritative MiniQMT context unavailable"):
        scheduler._submit_execution_plan_with_timeout(
            build_result=None,
            binding=qmt_binding,
            run=run,
            plan=plan,
            context=SimpleNamespace(),
            mode="SIM",
            as_of_time=datetime(2026, 5, 21, 10, 0, tzinfo=UTC),
            submit_callable=submit_callable,
        )

    latest = repo.get_simulation_daily_run(run.run_id)
    assert submit_called is False
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.run_payload_json["last_stage"] == "MINIQMT_QUOTE_CONTEXT_PREPARE_FAILED"
    diagnostic = latest.run_payload_json["miniqmt_quote_context_prepare_failure"]
    assert diagnostic["broker_callable_invoked"] is False
    assert diagnostic["broker_side_effect_evidence_before_attempt"] is False
    assert diagnostic["exception"]["reason_code"] == "ADAPTIVE_IS_TRADABILITY_DATA_INVALID"
    assert latest.run_payload_json["submit_failure"]["stage"] == "MINIQMT_QUOTE_CONTEXT_PREPARE_FAILED"


def test_miniqmt_reconciliation_warning_keeps_durable_event_loop_pending() -> None:
    run = SimpleNamespace(
        run_payload_json={
            "qmt_batch_id": "qmtbatch_bug628",
            "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
            "broker_called": True,
            "submitted_intents": 13,
            "failed_intents": 0,
            "pending_intents": 16,
            "qmt_batch_result": {
                "batch_id": "qmtbatch_bug628",
                "batch_status": OrderBatchStatus.SUBMITTING.value,
                "succeeded": 13,
                "failed": 0,
                "pending": 16,
                "pending_child_trigger_count": 16,
                "runtime_evidence": {
                    "source": "simulation_runtime_event_loop_submit",
                    "runtime_id": "mqrt_bug628",
                    "active_algo_count": 29,
                    "pending_algo_count": 16,
                    "submitted_child_count": 13,
                    "rejected_child_count": 0,
                },
            },
        }
    )

    gate = SimulationLifecycleScheduler._miniqmt_submit_result_gate(
        run=run,
        run_status_gate={"status": "WARNING", "reason": "strategy_scope_has_blocking_issues"},
        batch_residual_summary={},
        open_order_evidence={"open_order_count": 0},
        side_effect_evidence={"broker_side_effect_count": 13},
    )

    assert gate["status"] == "PENDING"
    assert gate["reason"] == "miniqmt_event_loop_pending_after_reconciliation_warning"
    assert gate["pending_event_loop"] == {
        "schema_version": "miniqmt_pending_event_loop_evidence_v1",
        "eligible": True,
        "batch_id": "qmtbatch_bug628",
        "runtime_id": "mqrt_bug628",
        "runtime_evidence_source": "simulation_runtime_event_loop_submit",
        "batch_status": OrderBatchStatus.SUBMITTING.value,
        "active_algo_count": 29,
        "pending_algo_count": 16,
        "failed_or_rejected_count": 0,
        "identity_sources": {
            "payload_batch_id": "qmtbatch_bug628",
            "result_batch_id": "qmtbatch_bug628",
            "payload_batch_status": OrderBatchStatus.SUBMITTING.value,
            "result_batch_status": OrderBatchStatus.SUBMITTING.value,
        },
        "pending_count_sources": {
            "payload_pending_intents": 16,
            "result_pending": 16,
            "result_pending_child_trigger_count": 16,
            "runtime_pending_algo_count": 16,
        },
        "failure_count_sources": {
            "payload_failed_intents": 0,
            "result_failed": 0,
            "runtime_rejected_child_count": 0,
        },
        "conflicts": [],
    }


def test_miniqmt_post_close_pending_algos_are_retryable_not_fake_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "qmt_batch_id": "qmtbatch_bug633",
        "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
        "broker_called": True,
        "submitted_intents": 26,
        "failed_intents": 0,
        "pending_intents": 3,
        "qmt_batch_result": {
            "batch_id": "qmtbatch_bug633",
            "batch_status": OrderBatchStatus.SUBMITTING.value,
            "succeeded": 26,
            "failed": 0,
            "pending": 3,
            "pending_child_trigger_count": 3,
            "runtime_evidence": {
                "source": "simulation_runtime_event_loop_tick_driver",
                "runtime_id": "mqrt_bug633",
                "active_algo_count": 29,
                "pending_algo_count": 3,
                "submitted_child_count": 26,
                "rejected_child_count": 0,
            },
        },
        "miniqmt_event_loop_tick_driver": {
            "pending_parent_intent_ids": ["intent_a", "intent_b", "intent_c"],
        },
    }
    run = SimpleNamespace(
        run_id="simrun_bug633",
        trade_date=TRADE_DATE,
        strategy_id="strategy_bug633",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        run_payload_json=payload,
    )

    class _Repository:
        status: SimulationDailyRunStatus | None = None
        payload_patch: dict[str, Any] | None = None

        def update_simulation_daily_run(self, run_id: str, *, status, payload_patch, payload_unset=None):  # noqa: ANN001, ANN201, ARG002
            assert run_id == run.run_id
            self.status = status
            self.payload_patch = payload_patch
            return SimpleNamespace(
                run_id=run.run_id,
                trade_date=run.trade_date,
                strategy_id=run.strategy_id,
                broker_backend=run.broker_backend,
                status=status,
                run_payload_json={**payload, **payload_patch},
            )

    repository = _Repository()
    scheduler = object.__new__(SimulationLifecycleScheduler)
    scheduler.repository = repository
    monkeypatch.setattr(
        scheduler,
        "_fresh_miniqmt_post_close_payload",
        lambda **_kwargs: (payload, {"schema_version": "miniqmt_post_close_fresh_reconcile_v1"}),
    )

    result = scheduler._post_close_terminalize_miniqmt_run(  # noqa: SLF001
        run=run,
        as_of_time=datetime(2026, 5, 21, 15, 1),
    )

    assert result["status"] == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result["reason"] == "miniqmt_post_close_event_loop_pending_algos_untriggered"
    assert repository.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    terminalization = repository.payload_patch["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "failed_retryable_after_close"
    assert terminalization["event_loop_pending_after_close"] == {
        "schema_version": "miniqmt_event_loop_pending_after_close_v1",
        "reason_code": "MINIQMT_EVENT_LOOP_PENDING_ALGOS_MARKET_CLOSED",
        "stage": "MINIQMT_POST_CLOSE_TERMINALIZATION",
        "reason": "event_loop_algorithms_remained_running_without_child_order_until_market_close",
        "pending_intents": 3,
        "pending_parent_intent_ids": ["intent_a", "intent_b", "intent_c"],
        "qmt_batch_id": "qmtbatch_bug633",
        "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
    }


def test_miniqmt_reconciliation_warning_rejects_unproven_pending_event_loop() -> None:
    run = SimpleNamespace(
        run_payload_json={
            "qmt_batch_id": "qmtbatch_bug628_invalid",
            "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
            "broker_called": True,
            "failed_intents": 0,
            "pending_intents": 16,
            "qmt_batch_result": {
                "batch_id": "qmtbatch_bug628_invalid",
                "batch_status": OrderBatchStatus.SUBMITTING.value,
                "failed": 0,
                "pending": 16,
                "pending_child_trigger_count": 16,
                "runtime_evidence": {
                    "source": "simulation_runtime_event_loop_submit",
                    "active_algo_count": 29,
                    "pending_algo_count": 16,
                    "rejected_child_count": 0,
                },
            },
        }
    )

    gate = SimulationLifecycleScheduler._miniqmt_submit_result_gate(
        run=run,
        run_status_gate={"status": "WARNING", "reason": "strategy_scope_has_blocking_issues"},
        batch_residual_summary={},
        open_order_evidence={"open_order_count": 0},
        side_effect_evidence={"broker_side_effect_count": 13},
    )

    assert gate["status"] == "blocked"
    assert gate["pending_event_loop"]["eligible"] is False
    assert gate["pending_event_loop"]["conflicts"] == ["runtime_id_missing"]


def test_miniqmt_reconciliation_warning_rejects_conflicting_durable_evidence() -> None:
    run = SimpleNamespace(
        run_payload_json={
            "qmt_batch_id": "qmtbatch_bug628_payload",
            "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
            "broker_called": True,
            "failed_intents": 0,
            "pending_intents": 16,
            "qmt_batch_result": {
                "batch_id": "qmtbatch_bug628_result",
                "batch_status": OrderBatchStatus.FAILED.value,
                "failed": 0,
                "pending": 15,
                "pending_child_trigger_count": 16,
                "runtime_evidence": {
                    "source": "simulation_runtime_event_loop_submit",
                    "runtime_id": "mqrt_bug628_conflict",
                    "active_algo_count": 29,
                    "pending_algo_count": 16,
                    "rejected_child_count": 0,
                },
            },
        }
    )

    gate = SimulationLifecycleScheduler._miniqmt_submit_result_gate(
        run=run,
        run_status_gate={"status": "WARNING", "reason": "strategy_scope_has_blocking_issues"},
        batch_residual_summary={},
        open_order_evidence={"open_order_count": 0},
        side_effect_evidence={"broker_side_effect_count": 13},
    )

    assert gate["status"] == "blocked"
    assert gate["pending_event_loop"]["eligible"] is False
    assert gate["pending_event_loop"]["conflicts"] == [
        "batch_id_conflict",
        "result_batch_not_submitting",
        "batch_status_conflict",
        "pending_count_conflict",
    ]


def test_miniqmt_tick_driver_persists_exact_valid_batch_without_defaulting() -> None:
    scheduler, repository, run, result = _tick_driver_persist_fixture()
    try:
        updated = scheduler._persist_miniqmt_tick_driver_result(
            binding=SimpleNamespace(binding_id=run.binding_id),
            run=run,
            plan=SimpleNamespace(plan_id="plan_bug683"),
            result=result,
        )
    finally:
        scheduler.shutdown_selection_inference(wait=True)

    assert updated.run_payload_json["qmt_batch_result"]["total"] == 1
    assert updated.run_payload_json["qmt_batch_result"]["batch_status"] == OrderBatchStatus.SUCCEEDED.value
    assert updated.run_payload_json["qmt_batch_result"]["results"] == [_tick_driver_batch_result_row()]
    assert updated.run_payload_json["qmt_batch_result"]["succeeded"] == 1
    assert updated.run_payload_json["qmt_batch_result"]["triggered_child_order_count"] == 13
    assert updated.run_payload_json["qmt_batch_result"]["runtime_evidence"]["trade_event_count"] == 2
    assert updated.run_payload_json["submitted_intents"] == 1
    assert updated.run_payload_json["failed_intents"] == 0
    assert updated.run_payload_json["pending_intents"] == 0
    assert repository.get_simulation_daily_run(run.run_id) == updated


@pytest.mark.parametrize(
    ("case", "reason_code"),
    [
        ("batch_id_conflict", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT"),
        ("invalid_status", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID"),
        ("invalid_result_json", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("invalid_metadata", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("invalid_result_boolean", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("invalid_foreign_batch", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("string_total", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_INVALID"),
        ("total_conflict", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_CONFLICT"),
        ("durable_status_invalid", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID"),
        ("broker_called_string", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("durable_batch_result_list", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("missing_durable_batch_id", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT"),
        ("missing_durable_status", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID"),
        ("missing_durable_results", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("missing_durable_total", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_INVALID"),
        ("missing_durable_success", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
    ],
)
def test_miniqmt_tick_driver_rejects_malformed_batch_before_durable_overwrite(
    case: str,
    reason_code: str,
) -> None:
    scheduler, repository, run, result = _tick_driver_persist_fixture(case=case)
    before = repository.get_simulation_daily_run(run.run_id)
    try:
        with pytest.raises(RuntimeConfigInvalidError) as exc_info:
            scheduler._persist_miniqmt_tick_driver_result(
                binding=SimpleNamespace(binding_id=run.binding_id),
                run=run,
                plan=SimpleNamespace(plan_id="plan_bug683"),
                result=result,
            )
    finally:
        scheduler.shutdown_selection_inference(wait=True)

    assert exc_info.value.context["reason_code"] == reason_code
    assert repository.get_simulation_daily_run(run.run_id) == before


def test_miniqmt_tick_driver_result_requires_exact_schema_identity_and_counts() -> None:
    evidence, submitted, rejected, pending = SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(
        _tick_driver_result_payload()
    )

    assert evidence["runtime_id"] == "mqrt_bug680"
    assert (submitted, rejected, pending) == (13, 0, 16)

    missing = _tick_driver_result_payload()
    missing["runtime_evidence"].pop("submitted_child_count")
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(missing)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_MISSING"

    invalid = _tick_driver_result_payload()
    invalid["pending_algo_count"] = "invalid"
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(invalid)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_COUNTER_INVALID"

    conflict = _tick_driver_result_payload()
    conflict["runtime_evidence"]["rejected_child_count"] = 1
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(conflict)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_CONFLICT"

    identity_conflict = _tick_driver_result_payload()
    identity_conflict["runtime_evidence"]["runtime_id"] = "mqrt_other"
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(identity_conflict)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_TICK_DRIVER_IDENTITY_CONFLICT"


def test_miniqmt_pending_and_submitted_evidence_rejects_malformed_counters_instead_of_defaulting() -> None:
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._mini_qmt_event_loop_has_submitted_children({"submitted_intents": "invalid"})
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_COUNTER_INVALID"

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos({"pending_intents": float("nan")})
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_COUNTER_INVALID"

    assert (
        SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos(
            {"qmt_batch_status": OrderBatchStatus.SUBMITTING.value}
        )
        is False
    )


def test_background_scheduler_result_surfaces_miniqmt_capacity_residual_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ResidualLifecycleScheduler:
        def status(self) -> dict[str, Any]:
            return {"ok": True, "scheduler": "residual_lifecycle_scheduler"}

        def run_once(self, **kwargs: Any) -> SimulationSchedulerRunOnceResult:
            run = SimulationDailyRun(
                run_id="simrun_background_capacity_residual",
                trade_date=TRADE_DATE,
                strategy_id="strategy_background_qmt",
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                package_id="pkg_background",
                manifest_sha256="manifest_background",
                release_id="srr_background",
                release_hash="release_hash_background",
                binding_id="simbind_background",
                binding_hash="binding_hash_background",
                account_group_id="ag_background",
                strategy_slot_id="slot_background",
                status=SimulationDailyRunStatus.SUCCEEDED,
                run_payload_json={
                    "last_stage": "SUCCEEDED",
                    "broker_called": True,
                    "failed_intents": 3,
                    "qmt_batch_id": "batch_background_capacity",
                    "qmt_batch_status": "PARTIAL",
                    "qmt_batch_result": {
                        "batch_status": "PARTIAL",
                        "failed": 3,
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
                        }
                    },
                },
            )
            return SimulationSchedulerRunOnceResult(
                trade_date=TRADE_DATE,
                data_source="DB_HISTORICAL",
                submit=True,
                total_bindings=1,
                results=(
                    SimulationSchedulerBindingResult(
                        binding_id=run.binding_id,
                        strategy_id=run.strategy_id,
                        broker_backend=run.broker_backend,
                        status="RECONCILED",
                        run=run,
                        data_source="MINIQMT_REALTIME",
                    ),
                ),
                as_of_time=kwargs.get("as_of_time"),
            )

        def post_close_reconcile_once(self, **kwargs: Any) -> SimulationSchedulerRunOnceResult:
            raise AssertionError("execution window should call run_once")

    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "true")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=ResidualLifecycleScheduler(),  # type: ignore[arg-type]
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 2, 0, tzinfo=UTC))

    assert result["submit"] is True
    assert result["summary"]["succeeded_with_capacity_residual_count"] == 1
    assert result["summary"]["capacity_residual_count"] == 1
    assert result["summary"]["capacity_residual_failed_intents"] == 3
    assert result["processed"][0]["succeeded_with_capacity_residual"] is True
    assert result["processed"][0]["capacity_residual_failed_intents"] == 3
    assert result["alerts"][0]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"


def test_production_context_provider_miniqmt_context():
    """Production provider wires MiniQMT services when backend is MINIQMT_SIM."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1",
            symbol="000001.XSHE",
            quantity=1000,
            available_quantity=1000,
            avg_cost=12.50,
            trade_date=date.today(),
        ),
    }
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="strat1",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        position_loader=lambda strategy_id, trade_date: positions,
        price_loader=lambda symbols, trade_date: {symbol: 12.6 for symbol in symbols},
        managed_order_service_factory=lambda: "fake_mos",
        qmt_sync_service_factory=lambda: "fake_sync",
        qmt_reconciliation_service_factory=lambda: "fake_recon",
        qmt_ledger_repository=qmt_repo,
        package_manifest_loader=lambda package_id: manifest,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
    assert ctx.current_positions == positions
    assert ctx.current_prices == {"000001.XSHE": 12.6}
    assert ctx.manifest == manifest
    assert ctx.managed_order_service == "fake_mos"
    assert ctx.qmt_sync_service == "fake_sync"
    assert ctx.qmt_reconciliation_service == "fake_recon"
    assert ctx.qmt_ledger_repository is qmt_repo


def test_existing_miniqmt_plan_context_uses_broker_marks_without_database_market_reload() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    planning_scheduler, _planning_repo, _planning_broker, _planning_binding = _miniqmt_event_loop_test_scheduler()
    planned = planning_scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(9, 25), tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    plan = planned.results[0].execution_plan
    assert plan is not None

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1",
            symbol="000001.XSHE",
            quantity=1000,
            available_quantity=1000,
            avg_cost=12.50,
            trade_date=TRADE_DATE,
        )
    }
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="strat1",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    quote_broker = FakeManagedOrderBroker(
        positions=[],
        quotes={"000001.XSHE": {"lastPrice": 12.7, "time": "20260521100000"}},
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    price_loader_calls: list[tuple[list[str], date]] = []
    tradability = FakePreTradeTradabilityProvider()

    def forbidden_price_loader(symbols: list[str], trade_date: date) -> dict[str, float]:
        price_loader_calls.append((list(symbols), trade_date))
        raise AssertionError("existing MiniQMT plan must not read database market prices")

    provider = ProductionSimulationRunContextProvider(
        position_loader=lambda strategy_id, trade_date: positions,
        price_loader=forbidden_price_loader,
        qmt_client_factory=lambda: quote_broker,
        managed_order_service_factory=lambda: "fake_mos",
        qmt_sync_service_factory=lambda: "fake_sync",
        qmt_reconciliation_service_factory=lambda: "fake_recon",
        qmt_ledger_repository=qmt_repo,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    context = provider.load_existing_plan_context(
        runtime_release=release,
        binding=binding,
        plan=plan,
        trade_date=TRADE_DATE,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert price_loader_calls == []
    assert tradability.calls == []
    assert context.current_prices == {"000001.XSHE": 12.7}
    assert context.price_by_symbol == {"000001.XSHE": 12.7}
    assert context.target_total_equity is None
    assert context.target_equity_context["planning_market_data_reloaded"] is False
    assert quote_broker.full_tick_calls == [["000001.XSHE"]]


def test_production_context_provider_loads_miniqmt_positions_from_virtual_ledger_without_submit_broker():
    """Default MiniQMT production context reads strategy lots and keeps submission disabled unless explicitly enabled."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            frozen_cash=Decimal("123"),
            realized_pnl=Decimal("45"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prod_context",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_prod_context",
            open_date=TRADE_DATE,
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    qmt_client = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 1000, "can_sell": 1000}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: qmt_client,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["000001.SZ"].quantity == 1000
    assert ctx.current_prices == {"000001.SZ": 10.5}
    assert ctx.manifest == manifest
    assert ctx.cash == 900000
    assert ctx.frozen_cash == 123
    assert ctx.realized_pnl == 45
    assert ctx.qmt_ledger_repository is qmt_repo
    assert ctx.qmt_sync_service is not None
    assert ctx.qmt_reconciliation_service is not None
    assert getattr(ctx.managed_order_service, "_broker") is qmt_client


def test_production_context_provider_uses_miniqmt_quote_for_pre_trade_gate_today() -> None:
    """MiniQMT same-day gate must use broker quotes and must not call the TDX provider."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_suspended_no_quote",
            strategy_id="strat1",
            symbol="688689.SH",
            open_trade_id="trade_suspended_no_quote",
            open_date=date.today() - timedelta(days=1),
            quantity=878,
            available_quantity=878,
            remaining_quantity=878,
            avg_cost=Decimal("46.82"),
            cost_amount=Decimal("41111.96"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "688689.SH", "quantity": 878, "can_sell": 878}],
        quotes={
            "688689.SH": {
                "bidPrice": [0.0],
                "askPrice": [0.0],
                "bidVol": [0],
                "askVol": [0],
                "lastPrice": 46.82,
                "lastClose": 46.8,
                "openPrice": 0.0,
                "highPrice": 0.0,
                "lowPrice": 0.0,
                "volume": 0,
                "amount": 0,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 46.82 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    assert ctx.pre_trade_tradability["688689.SH"]["reason_code"] == "NO_TRADABLE_REALTIME_QUOTE"
    assert ctx.pre_trade_tradability["688689.SH"]["source"] == "MINIQMT_REALTIME.broker_quote"
    assert ctx.context_diagnostics["pre_trade_tradability"]["blocked_symbols"] == [
        {"symbol": "688689.SH", "reason_code": "NO_TRADABLE_REALTIME_QUOTE", "source": "MINIQMT_REALTIME.broker_quote"}
    ]
    assert tradability.calls == []
    assert broker.full_tick_calls == [["688689.SH"]]


def test_production_context_provider_miniqmt_quote_yuan_limits_do_not_fail_pre_run() -> None:
    """MiniQMT broker quotes are yuan-denominated and must not use TDX raw-li rounding."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_miniqmt_quote",
            strategy_id="strat1",
            symbol="603303.SH",
            open_trade_id="trade_miniqmt_quote",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("30.14"),
            cost_amount=Decimal("3014.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "603303.SH", "quantity": 100, "can_sell": 100}],
        quotes={
            "603303.SH": {
                "bidPrice": [30.23],
                "askPrice": [30.26],
                "bidVol": [108],
                "askVol": [17],
                "lastPrice": 30.23,
                "lastClose": 30.14,
                "openPrice": 30.27,
                "highPrice": 31.8,
                "lowPrice": 29.01,
                "volume": 66974,
                "amount": 203373536,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 30.23 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["603303.SH"]
    assert status["is_tradable"] is True
    assert status["quote_evidence"]["quote_price_basis"] == "yuan"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(33.15)
    assert status["quote_evidence"]["limit_down"] == pytest.approx(27.13)
    assert tradability.calls == []
    assert broker.full_tick_calls == [["603303.SH"]]


def test_production_context_provider_miniqmt_degenerate_raw_limit_metadata_passes_pre_run() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_miniqmt_degenerate_limit_metadata",
            strategy_id="strat1",
            symbol="000048.SZ",
            open_trade_id="trade_miniqmt_degenerate_limit_metadata",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("20.75"),
            cost_amount=Decimal("2075.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000048.SZ", "quantity": 100, "can_sell": 100}],
        quotes={
            "000048.SZ": {
                "price_basis": "raw_li",
                "bidPrice": [20.65],
                "askPrice": [20.66],
                "bidVol": [100],
                "askVol": [100],
                "lastPrice": 20.66,
                "lastClose": 20.75,
                "openPrice": 20.7,
                "highPrice": 20.88,
                "lowPrice": 20.3,
                "limit_up": 20.0,
                "limit_down": 20.0,
                "volume": 12345,
                "amount": 25432100,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 20.66 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["000048.SZ"]
    assert status["is_tradable"] is True
    assert status["quote_evidence"]["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert status["quote_evidence"]["quote_price_basis"] == "yuan"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(22.83)
    assert status["quote_evidence"]["limit_down"] == pytest.approx(18.68)
    assert broker.full_tick_calls == [["000048.SZ"]]


def test_production_context_provider_miniqmt_quote_missing_is_visible_without_tdx_fallback() -> None:
    """MiniQMT quote outages must surface as MiniQMT evidence, not TDX timestamp failures."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_quote_missing",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_quote_missing",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("1000.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000001.SZ", "quantity": 100, "can_sell": 100}],
        quotes={},
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["000001.SZ"]
    assert status["is_tradable"] is False
    assert status["reason_code"] == "REALTIME_QUOTE_MISSING"
    assert status["source"] == "MINIQMT_REALTIME.broker_quote"
    assert status["quote_evidence"] == {
        "quote_source": "MINIQMT_REALTIME.broker_quote",
        "quote_present": False,
    }
    assert tradability.calls == []
    assert broker.full_tick_calls == [["000001.SZ"]]


def test_production_context_provider_drops_miniqmt_stale_lots_missing_from_broker():
    """MiniQMT current_positions must not emit impossible sells for lots absent from broker can_sell."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_stale_not_in_broker",
            strategy_id="strat1",
            symbol="000636.SZ",
            open_trade_id="trade_stale_not_in_broker",
            open_date=date(2026, 5, 20),
            quantity=900,
            available_quantity=900,
            remaining_quantity=900,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("9000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    seen_price_symbols: list[tuple[str, ...]] = []

    def price_loader(symbols, trade_date):
        seen_price_symbols.append(tuple(symbols))
        return {symbol: 10.5 for symbol in symbols}

    broker = FakeManagedOrderBroker(positions=[])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=price_loader,
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions == {}
    assert ctx.broker_positions == []
    assert seen_price_symbols == [()]
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["dropped_position_count"] == 1
    assert diagnostics["dropped_positions"][0]["symbol"] == "000636.SZ"


def test_production_context_provider_caps_miniqmt_lots_to_broker_quantity_and_can_sell():
    """Strategy-lot context is capped by broker-authoritative quantity/can_sell before planning."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_cap_to_broker",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_cap_to_broker",
            open_date=date(2026, 5, 20),
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 500, "can_sell": 300}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    position = ctx.current_positions["000001.SZ"]
    assert position.quantity == 500
    assert position.available_quantity == 300
    assert ctx.broker_positions == [{"stock_code": "000001.SZ", "quantity": 500, "can_sell": 300}]
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["capped_position_count"] == 1
    assert diagnostics["capped_positions"][0]["reconciled_quantity"] == 500
    assert diagnostics["capped_positions"][0]["reconciled_available_quantity"] == 300


def test_production_context_provider_projects_miniqmt_strategy_slot_from_account_broker_authority():
    """One slot's local lots cannot consume another slot's broker-backed attribution."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for strategy_id, strategy_name, quantity in (
        ("strat1", "StrategyA", 7600),
        ("strat_b", "StrategyB", 6600),
    ):
        qmt_repo.create_virtual_account(
            VirtualAccount(
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                display_name=strategy_name,
                account_id="QMT_SIM_ACCOUNT",
                mode="SIM",
                initial_cash=Decimal("1000000"),
                cash=Decimal("900000"),
                status=VirtualAccountStatus.ENABLED,
            )
        )
        qmt_repo.create_position_lot(
            PositionLotRecord(
                lot_id=f"lot_{strategy_id}",
                strategy_id=strategy_id,
                symbol="001358.SZ",
                open_trade_id=f"trade_{strategy_id}",
                open_date=date(2026, 5, 20),
                quantity=quantity,
                available_quantity=quantity,
                remaining_quantity=quantity,
                avg_cost=Decimal("29.88"),
                cost_amount=Decimal(quantity) * Decimal("29.88"),
                account_id="QMT_SIM_ACCOUNT",
            )
        )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "001358.SZ", "quantity": 13200, "can_sell": 13200}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 30.0 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["001358.SZ"].quantity == 7100
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["position_authority"] == "broker_positions"
    assert diagnostics["account_strategy_count"] == 2
    assert diagnostics["capped_position_count"] == 1
    assert diagnostics["projection_adjustments"][0]["issue_type"] == "UNBACKED_STRATEGY_POSITION"
    assert diagnostics["projection_adjustments"][0]["projected_strategy_quantities"] == {
        "strat1": 7100,
        "strat_b": 6100,
    }


def test_production_context_provider_miniqmt_preview_checks_broker_can_sell_without_submit():
    """Preview-only MiniQMT path can read account sellable quantity but never places orders."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_preview_sell_check",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_preview_sell_check",
            open_date=date(2026, 5, 20),
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 1000, "can_sell": 100}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.manifest == manifest

    result = ctx.managed_order_service.submit_batch(
        [
            ManagedOrderRequest(
                account_id="QMT_SIM_ACCOUNT",
                strategy_name="StrategyOne",
                symbol="000001.SZ",
                side="SELL",
                order_type=24,
                quantity=200,
                price_type=5,
                price=Decimal("0"),
                order_remark="preview-sell-check",
                trade_date=TRADE_DATE,
                mode="SIM",
            )
        ]
    )

    assert result.success is False
    assert result.results[0].broker_called is False
    assert result.results[0].preflight.broker_can_sell == 100
    assert result.results[0].preflight.primary_error.code in {
        "INSUFFICIENT_BROKER_CAN_SELL",
        "BATCH_INSUFFICIENT_BROKER_CAN_SELL",
    }
    assert broker.place_order_payloads == []


def test_daily_context_wires_direct_instrument_authority_for_miniqmt() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    daily_provider = _CapturingDailyTradingContextProvider()
    instrument_calls: list[list[str]] = []

    def get_instrument_details(symbols: list[str]) -> dict[str, dict[str, Any]]:
        instrument_calls.append(list(symbols))
        return {
            symbol: {
                "InstrumentID": symbol.split(".", 1)[0],
                "ExchangeID": symbol.split(".", 1)[1],
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.01,
                "InstrumentStatus": 0,
                "IsTrading": True,
                "TradingDay": TRADE_DATE.strftime("%Y%m%d"),
                "OpenDate": "20100101",
                "DayCountFromIPO": 1000,
            }
            for symbol in symbols
        }

    qmt_client = SimpleNamespace(get_instrument_details=get_instrument_details)
    qmt_factory_calls: list[bool] = []

    def qmt_client_factory() -> SimpleNamespace:
        qmt_factory_calls.append(True)
        return qmt_client

    provider = ProductionSimulationRunContextProvider(
        daily_trading_context_provider=daily_provider,
        qmt_client_factory=qmt_client_factory,
    )
    binding = SimpleNamespace(
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="bind-qmt",
        binding_hash="binding-hash",
        strategy_id="strategy-qmt",
        broker_account_id="account-qmt",
        binding_config_json={
            "miniqmt_quote_control": {
                "schema_version": "miniqmt_quote_control_binding_v1",
                "control_revision": "B0_QUOTE_V2",
            }
        },
    )

    result = provider.load_daily_trading_context(
        symbols=["000001.SZ"],
        trade_date=TRADE_DATE,
        binding=binding,
        runtime_release=SimpleNamespace(
            package_id="pkg-qmt",
            manifest_sha256="manifest-hash",
            release_id="release-qmt",
            release_hash="release-hash",
        ),
        as_of_time=datetime(TRADE_DATE.year, TRADE_DATE.month, TRADE_DATE.day, 9, 10),
        calendar_service_snapshot={"is_trading_day": True},
    )

    assert qmt_factory_calls == [True]
    assert instrument_calls == [["000001.SZ"]]
    assert daily_provider.kwargs == {}
    assert result["000001.SZ"]["daily_trading_context"]["limit_authority"] == ("MINIQMT_INSTRUMENT_DETAIL_V1")
