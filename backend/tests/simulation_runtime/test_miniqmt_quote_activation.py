from __future__ import annotations

from datetime import date, datetime, UTC
from types import SimpleNamespace
from typing import Any

import pytest

from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeClient,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2RevisionV1,
    ParentQuoteControlAssignmentV1,
    source_build_manifest,
)
from backend.services.miniqmt_execution_runtime.client import _b0_quote_v2_recovering_active
from backend.services.miniqmt_execution_runtime.models import MiniQMTAlgoInstanceStatus
from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.simulation_runtime.miniqmt_quote_activation import (
    MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
    DrainOnlyB0QuoteV2ControllerFactory,
    MiniQMTQuoteIngressActivation,
    _build_bootstrap_fetcher,
    build_miniqmt_quote_ingress_activation_from_env,
)
from backend.services.simulation_runtime.models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
)
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler
from backend.services.simulation_runtime.service import StrategyRuntimeReleaseService
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType


class _Subscriber:
    def phase_one_health(self, *, data_session_key: str) -> dict[str, object]:
        return {"status": "INACTIVE", "data_session_key": data_session_key, "lease_count": 0}

    def shutdown_phase_one_leases(self, *, data_session_key: str) -> None:
        return None


class _QmtClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_full_tick(
        self,
        symbols: list[str],
        *,
        ensure_subscription: bool,
        ensure_fresh: bool,
    ) -> dict[str, dict[str, object]]:
        self.calls.append(
            {
                "symbols": list(symbols),
                "ensure_subscription": ensure_subscription,
                "ensure_fresh": ensure_fresh,
            }
        )
        return {symbol: {"lastPrice": 10.0} for symbol in symbols}


class _LifecycleActivation:
    controller_factory = None

    def __init__(self) -> None:
        self.epoch_count = 0
        self.shutdown_count = 0

    def health(self) -> dict[str, object]:
        return {"schema_version": "miniqmt_quote_ingress_activation_v1", "status": "DISABLED"}

    def begin_lifecycle_epoch(self) -> dict[str, object]:
        self.epoch_count += 1
        return self.health()

    def shutdown(self) -> dict[str, object]:
        self.shutdown_count += 1
        return self.health()


def _enabled_env() -> dict[str, str]:
    return {"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "true"}


def _quote_control_policy_context(*, parent_intent_id: str, trade_date: date) -> dict[str, object]:
    manifest = source_build_manifest()
    quote_contract = {
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
        "max_receive_age_ms": 2_000,
        "max_source_lag_ms": 2_000,
        "max_exchange_age_ms": 2_000,
        "max_negative_skew_ms": 20,
        "max_clock_age_divergence_ms": 20,
        "max_dependency_group_skew_ms": 100,
        "auction_mode": "OBSERVE_ONLY",
    }
    revision = B0QuoteV2RevisionV1.build(
        execution_policy={"quote_contract": quote_contract},
        execution_policy_version_id="policy-b0-activation",
        execution_policy_sha256="a" * 64,
        adapter_version=manifest.adapter_version,
        adapter_sha256=manifest.adapter_sha256,
        code_revision=manifest.code_revision,
        code_sha256=manifest.code_sha256,
        evidence_schema_version=manifest.evidence_schema_version,
        evidence_schema_sha256=manifest.evidence_schema_sha256,
        benchmark_policy_version="benchmark-b0-activation",
        mark_policy_version="mark-b0-activation",
        markout_max_lag_ms=5_000,
    )
    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-b0-activation",
        binding_hash="b" * 64,
        trade_date=trade_date,
        parent_intent_id=parent_intent_id,
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=revision,
    )
    return {
        "policy_json": {"algo_code": "SNIPER_MINIQMT", "algo_config": {}},
        "validated_execution_policy_id": "policy-b0-activation",
        "policy_sha256": "a" * 64,
        "quote_control": {
            "binding": {
                "schema_version": "miniqmt_quote_control_binding_v1",
                "control_revision": "B0_QUOTE_V2",
            },
            "revision": revision.canonical_payload(),
            "assignments": [assignment.canonical_payload()],
        },
    }


def _event_loop_dependencies() -> tuple[
    InMemoryMiniQMTExecutionRuntimeRepository,
    InMemoryQmtStrategyLedgerRepository,
]:
    repository = InMemoryMiniQMTExecutionRuntimeRepository()
    ledger_repository = InMemoryQmtStrategyLedgerRepository()
    ledger_repository.create_virtual_account(
        VirtualAccount(
            strategy_id="strategy-b0-activation",
            strategy_name="strategy-b0-activation",
            display_name="B0 activation strategy",
            account_id="account-b0-activation",
            mode="SIM",
            initial_cash=100_000,
            cash=100_000,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    return repository, ledger_repository


class _NoCallQmtClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def _unexpected(self, name: str) -> None:
        self.calls.append(name)
        raise AssertionError(f"fresh B0 drain rejection reached qmt method {name}")

    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, object]]:  # noqa: ARG002
        self._unexpected("get_orders")
        return []

    def get_trades(self) -> list[dict[str, object]]:
        self._unexpected("get_trades")
        return []

    def get_positions(self) -> list[dict[str, object]]:
        self._unexpected("get_positions")
        return []

    def get_full_tick(self, symbols: list[str]) -> dict[str, dict[str, object]]:  # noqa: ARG002
        self._unexpected("get_full_tick")
        return {}

    def place_order(self, **kwargs: object) -> tuple[int, str]:  # noqa: ARG002
        self._unexpected("place_order")
        return 0, "unreachable"


class _RecordingRecoveryController:
    def __init__(self, runtime_id: str) -> None:
        self.runtime_id = runtime_id
        self.lifecycle_ticks: list[datetime] = []

    def lifecycle_tick(self, *, now_utc: datetime) -> None:
        self.lifecycle_ticks.append(now_utc)


class _RecordingRecoveryDelegate:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []
        self.controller: _RecordingRecoveryController | None = None

    def get(self, runtime_id: str) -> None:  # noqa: ARG002
        return None

    def create(
        self,
        *,
        runtime: MiniQMTExecutionRuntime,
        assignments: dict[str, Any],
        symbols: tuple[str, ...],
        recovering_active: bool = False,
    ) -> _RecordingRecoveryController:
        self.create_calls.append(
            {
                "runtime_id": runtime.config.runtime_id,
                "assignment_ids": tuple(sorted(assignments)),
                "symbols": symbols,
                "recovering_active": recovering_active,
            }
        )
        self.controller = _RecordingRecoveryController(runtime.config.runtime_id)
        return self.controller

    def health(self) -> dict[str, object]:
        return {"accept_new_assignments": False}

    def set_accept_new_assignments(self, enabled: bool) -> None:
        if enabled:
            raise AssertionError("drain delegate cannot accept new assignments")


def test_switch_disabled_defers_schema_and_runtime_dependencies_until_durable_recovery() -> None:
    calls: list[str] = []

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: calls.append("schema") or "pending",
        subscriber_factory=lambda: calls.append("subscriber"),
        qmt_client_factory=lambda: calls.append("qmt"),
    )

    health = activation.health()
    assert health["status"] == "DRAINING"
    assert health["production_ddl_gate"] == "deferred_until_durable_recovery"
    assert activation.controller_factory is not None
    assert health["drain_factory"]["factory_initialized"] is False
    assert calls == []


def test_client_fresh_b0_submit_is_rejected_before_runtime_gateway_or_qmt_side_effect() -> None:
    trade_date = date(2026, 7, 13)
    runtime_id = "runtime-b0-fresh-rejected"
    parent_intent_id = "parent-b0-fresh-rejected"
    repository, ledger_repository = _event_loop_dependencies()
    dependency_calls: list[str] = []
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: dependency_calls.append("schema") or "applied_and_verified",
        subscriber_factory=lambda: dependency_calls.append("subscriber") or _Subscriber(),
        qmt_client_factory=lambda: dependency_calls.append("qmt") or _QmtClient(),
    )
    assert activation.controller_factory is not None
    qmt_client = _NoCallQmtClient()
    client = MiniQMTExecutionRuntimeClient(
        repository=repository,
        strategy_ledger_repository=ledger_repository,
        runtime_kind="event_loop",
        b0_quote_v2_controller_factory=activation.controller_factory,
    )
    intent = OrderIntent(
        intent_id=parent_intent_id,
        package_id="package-b0-activation",
        portfolio_id="portfolio-b0-activation",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        order_type=OrderType.LIMIT,
        limit_price=10.0,
        target_trade_date=trade_date,
        metadata={"strategy_id": "strategy-b0-activation"},
    )

    with pytest.raises(QuoteContractError, match="only durable active runtimes may drain") as exc_info:
        client.submit_event_loop_vnpy_parent_intents(
            parent_intents=[intent],
            policy_context=_quote_control_policy_context(
                parent_intent_id=parent_intent_id,
                trade_date=trade_date,
            ),
            account_group_id="account-b0-activation",
            trade_date=trade_date,
            runtime_config_hash="runtime-config-b0-fresh-rejected",
            runtime_id=runtime_id,
            strategy_slot_id="slot-b0-activation",
            qmt_client=qmt_client,
            strategy_name="strategy-b0-activation",
            order_remark_prefix="b0activation",
            account_id="account-b0-activation",
        )

    assert exc_info.value.context["recovering_active"] is False
    assert repository.get_runtime(runtime_id) is None
    assert qmt_client.calls == []
    assert dependency_calls == []
    assert activation.health()["drain_factory"]["factory_initialized"] is False


def test_client_tick_driver_recovers_durable_active_b0_through_drain_delegate() -> None:
    trade_date = date(2026, 7, 13)
    runtime_id = "runtime-b0-durable-recovery"
    parent_intent_id = "parent-b0-durable-recovery"
    runtime_config_hash = "runtime-config-b0-durable-recovery"
    repository, ledger_repository = _event_loop_dependencies()
    durable_runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id=runtime_id,
            account_group_id="account-b0-activation",
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
        ),
        repository=repository,
        gateway=FakeMiniQMTGateway(),
    )
    durable_runtime.start()
    durable_runtime.create_vnpy_algo_instance(
        parent_intent_id=parent_intent_id,
        strategy_slot_id="slot-b0-activation",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.0,
        metadata={"runtime_child_context": {"price_type": 5}},
    )
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
        subscriber_factory=_Subscriber,
        qmt_client_factory=_QmtClient,
    )
    drain_factory = activation.controller_factory
    assert isinstance(drain_factory, DrainOnlyB0QuoteV2ControllerFactory)
    delegate = _RecordingRecoveryDelegate()
    drain_factory._delegate = delegate  # type: ignore[assignment]
    qmt_client = _NoCallQmtClient()
    client = MiniQMTExecutionRuntimeClient(
        repository=repository,
        strategy_ledger_repository=ledger_repository,
        runtime_kind="event_loop",
        b0_quote_v2_controller_factory=drain_factory,
    )
    tick_time = datetime(2026, 7, 13, 1, 31, tzinfo=UTC)

    result = client.drive_event_loop_ticks(
        account_group_id="account-b0-activation",
        trade_date=trade_date,
        runtime_config_hash=runtime_config_hash,
        runtime_id=runtime_id,
        qmt_client=qmt_client,
        strategy_name="strategy-b0-activation",
        order_remark_prefix="b0activation",
        account_id="account-b0-activation",
        policy_context=_quote_control_policy_context(
            parent_intent_id=parent_intent_id,
            trade_date=trade_date,
        ),
        as_of_time=tick_time,
    )

    assert result.runtime_id == runtime_id
    assert result.pending_parent_intent_ids == (parent_intent_id,)
    assert delegate.create_calls == [
        {
            "runtime_id": runtime_id,
            "assignment_ids": (parent_intent_id,),
            "symbols": ("000001.SZ",),
            "recovering_active": True,
        }
    ]
    assert delegate.controller is not None
    assert delegate.controller.lifecycle_ticks == [tick_time]
    assert qmt_client.calls == []


def test_enabled_pending_or_failed_schema_readback_is_loud_and_exposes_no_factory(caplog) -> None:  # type: ignore[no-untyped-def]
    dependency_calls: list[str] = []
    pending = build_miniqmt_quote_ingress_activation_from_env(
        environ=_enabled_env(),
        schema_gate_reader=lambda: "pending",
        subscriber_factory=lambda: dependency_calls.append("subscriber"),
        qmt_client_factory=lambda: dependency_calls.append("qmt"),
    )
    pending_health = pending.health()
    assert pending_health["status"] == "BLOCKED"
    assert pending_health["enabled"] is True
    assert pending_health["production_ddl_gate"] == "pending"
    assert pending_health["reason_code"] == "MINIQMT_QUOTE_EVENT_SCHEMA_NOT_APPLIED"
    assert pending_health["factory_available"] is False
    assert dependency_calls == []

    def fail_readback() -> str:
        raise ConnectionError("schema readback unavailable")

    failed = build_miniqmt_quote_ingress_activation_from_env(
        environ=_enabled_env(),
        schema_gate_reader=fail_readback,
        subscriber_factory=lambda: dependency_calls.append("subscriber"),
        qmt_client_factory=lambda: dependency_calls.append("qmt"),
    )
    health = failed.health()
    assert health["status"] == "BLOCKED"
    assert health["production_ddl_gate"] == "readback_failed"
    assert health["reason_code"] == "MINIQMT_QUOTE_EVENT_SCHEMA_READBACK_FAILED"
    assert health["failure"] == {
        "exception_type": "ConnectionError",
        "message": "schema readback unavailable",
    }
    assert "B0_QUOTE_V2 remains unavailable" in caplog.text
    assert dependency_calls == []


def test_applied_schema_builds_single_scheduler_factory_and_bootstrap_avoids_legacy_subscription() -> None:
    subscriber = _Subscriber()
    qmt_client = _QmtClient()
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ=_enabled_env(),
        schema_gate_reader=lambda: "applied_and_verified",
        subscriber_factory=lambda: subscriber,
        qmt_client_factory=lambda: qmt_client,
    )

    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=activation,
    )
    assert activation.health()["status"] == "READY"
    assert activation.data_session_key == MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY
    assert scheduler._b0_quote_v2_controller_factory is activation.controller_factory
    assert scheduler.orchestrator.b0_quote_v2_controller_factory is activation.controller_factory
    with pytest.raises(ValueError, match="requires process switch=false"):
        DrainOnlyB0QuoteV2ControllerFactory(
            requested_config=activation.config,
            schema_gate_reader=lambda: "applied_and_verified",
            subscriber_factory=lambda: subscriber,
            qmt_client_factory=lambda: qmt_client,
        )
    assert activation.begin_lifecycle_epoch()["status"] == "READY"
    assert activation.watchdog_tick()["status"] == "READY"

    bootstrap = _build_bootstrap_fetcher(qmt_client)
    assert bootstrap(["000001.SZ"]) == {"000001.SZ": {"lastPrice": 10.0}}
    assert qmt_client.calls == [
        {
            "symbols": ["000001.SZ"],
            "ensure_subscription": False,
            "ensure_fresh": False,
        }
    ]
    scheduler.shutdown_miniqmt_quote_ingress()
    scheduler.shutdown_selection_inference()
    assert activation.health()["status"] == "STOPPED"
    assert activation.shutdown()["status"] == "STOPPED"
    with pytest.raises(RuntimeError, match="cannot begin a lifecycle epoch"):
        activation.begin_lifecycle_epoch()
    with pytest.raises(RuntimeError, match="cannot run watchdog"):
        activation.watchdog_tick()


def test_activation_rejects_impossible_component_states_and_missing_bootstrap_method() -> None:
    disabled = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "pending",
    )
    with pytest.raises(ValueError, match="requires applied_and_verified"):
        MiniQMTQuoteIngressActivation(
            config=disabled.config,
            status="READY",
            production_ddl_gate="pending",
            process_switch_enabled=True,
        )
    with pytest.raises(ValueError, match="requires supervisor and controller factory"):
        MiniQMTQuoteIngressActivation(
            config=disabled.config,
            status="READY",
            production_ddl_gate="applied_and_verified",
            process_switch_enabled=True,
        )
    with pytest.raises(ValueError, match="cannot expose runtime components"):
        MiniQMTQuoteIngressActivation(
            config=disabled.config,
            status="BLOCKED",
            production_ddl_gate="pending",
            process_switch_enabled=True,
            supervisor=object(),  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="callable get_full_tick"):
        _build_bootstrap_fetcher(object())


def test_switch_disabled_with_applied_schema_builds_drain_only_factory_for_restart_recovery() -> None:
    calls: list[str] = []
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: calls.append("schema") or "applied_and_verified",
        subscriber_factory=lambda: calls.append("subscriber") or _Subscriber(),
        qmt_client_factory=lambda: calls.append("qmt") or _QmtClient(),
    )

    health = activation.health()
    assert health["status"] == "DRAINING"
    assert health["enabled"] is False
    assert health["runtime_config_enabled"] is True
    assert health["process_config_sha256"] != health["runtime_config_sha256"]
    assert health["factory_available"] is True
    assert activation.controller_factory is not None
    assert activation.controller_factory.health()["accept_new_assignments"] is False
    assert calls == []
    with pytest.raises(QuoteContractError, match="only durable active runtimes may drain"):
        activation.controller_factory.assert_accepts_new_assignments()
    activation.controller_factory._ensure_delegate()
    assert calls == ["schema", "subscriber", "qmt"]
    initialized = activation.health()
    assert initialized["production_ddl_gate"] == "applied_and_verified"
    assert initialized["drain_factory"]["factory_initialized"] is True
    activation.shutdown()


def test_drain_recovery_schema_failure_is_typed_loud_and_constructs_no_runtime_dependencies(caplog) -> None:  # type: ignore[no-untyped-def]
    calls: list[str] = []
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: calls.append("schema") or "pending",
        subscriber_factory=lambda: calls.append("subscriber"),
        qmt_client_factory=lambda: calls.append("qmt"),
    )
    assert activation.controller_factory is not None

    with pytest.raises(QuoteContractError, match="requires applied_and_verified") as exc_info:
        activation.controller_factory._ensure_delegate()

    assert exc_info.value.context["production_ddl_gate"] == "pending"
    assert activation.health()["drain_factory"]["last_failure"]["reason_code"] == (
        "MINIQMT_QUOTE_EVENT_SCHEMA_NOT_APPLIED"
    )
    assert calls == ["schema"]
    assert "schema readback failed" not in caplog.text


def test_scheduler_owns_activation_tick_and_shutdown_lifecycle_exactly_once() -> None:
    activation = _LifecycleActivation()
    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=activation,
    )

    scheduler.run_once(trade_date=date(2026, 7, 13), data_source="DB_HISTORICAL")
    scheduler.post_close_reconcile_once(trade_date=date(2026, 7, 13), data_source="DB_HISTORICAL")
    scheduler.shutdown_miniqmt_quote_ingress()
    scheduler.shutdown_selection_inference()

    assert activation.epoch_count == 2
    assert activation.shutdown_count == 1


def test_drain_recovery_requires_durable_active_algo_or_child_fact() -> None:
    completed = SimpleNamespace(status=MiniQMTAlgoInstanceStatus.COMPLETED)
    active = SimpleNamespace(status=MiniQMTAlgoInstanceStatus.ACTIVE)
    paused = SimpleNamespace(status=MiniQMTAlgoInstanceStatus.PAUSED)
    child = SimpleNamespace(child_order_id="child-1")

    assert _b0_quote_v2_recovering_active(algo_instances=(completed,), active_child_orders=()) is False
    assert _b0_quote_v2_recovering_active(algo_instances=(active,), active_child_orders=()) is True
    assert _b0_quote_v2_recovering_active(algo_instances=(paused,), active_child_orders=()) is True
    assert _b0_quote_v2_recovering_active(algo_instances=(completed,), active_child_orders=(child,)) is True


def _release_and_binding(
    repository: InMemorySimulationRuntimeRepository,
    *,
    backend: SimulationBrokerBackend,
    quote_control: dict[str, str] | None,
):  # type: ignore[no-untyped-def]
    service = StrategyRuntimeReleaseService(repository=repository)
    release = service.create_release(
        package_id="pkg-b0-activation",
        manifest_sha256="manifest-sha",
        runtime_profile_id="runtime-profile",
        runtime_profile_version_id="runtime-profile-v1",
        runtime_profile_sha256="runtime-profile-sha",
        daily_strategy_profile_version_id="daily-v1",
        execution_policy_version_id="execution-v1",
        execution_policy_sha256="execution-sha",
        tail_policy_version_id="tail-v1",
        tail_policy_sha256="tail-sha",
        execution_policy_json={"algo_code": "SNIPER_MINIQMT", "algo_config": {}},
        validation_state=RuntimeReleaseValidationState.SIM_PASSED,
        effective_from=date(2026, 7, 13),
        effective_to=date(2026, 7, 13),
    )
    binding = service.create_binding(
        strategy_id="strategy-b0-activation",
        release=release,
        broker_backend=backend,
        broker_account_id="sim-account" if backend == SimulationBrokerBackend.MINIQMT_SIM else None,
        capital_allocation=100000.0,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        miniqmt_quote_control=quote_control,
        effective_from=date(2026, 7, 13),
        effective_to=date(2026, 7, 13),
    )
    return release, binding


def test_binding_creation_requires_exact_explicit_quote_control_and_rejects_non_miniqmt() -> None:
    repository = InMemorySimulationRuntimeRepository()
    quote_control = {
        "schema_version": "miniqmt_quote_control_binding_v1",
        "control_revision": "B0_QUOTE_V2",
    }
    _release, binding = _release_and_binding(
        repository,
        backend=SimulationBrokerBackend.MINIQMT_SIM,
        quote_control=quote_control,
    )
    assert binding.binding_config_json["miniqmt_quote_control"] == quote_control

    with pytest.raises(RuntimeConfigInvalidError, match="miniqmt_quote_control is valid only for MiniQMT SIM bindings"):
        _release_and_binding(
            InMemorySimulationRuntimeRepository(),
            backend=SimulationBrokerBackend.LOCAL_SIM,
            quote_control=quote_control,
        )


def test_unattended_roll_forward_preserves_b0_quote_control_without_revision_drift() -> None:
    repository = InMemorySimulationRuntimeRepository()
    quote_control = {
        "schema_version": "miniqmt_quote_control_binding_v1",
        "control_revision": "B0_QUOTE_V2",
    }
    _release, source = _release_and_binding(
        repository,
        backend=SimulationBrokerBackend.MINIQMT_SIM,
        quote_control=quote_control,
    )
    scheduler = SimulationLifecycleScheduler(repository=repository)

    rolled = scheduler._roll_forward_unattended_binding(source=source, trade_date=date(2026, 7, 14))

    assert rolled.binding_id != source.binding_id
    assert rolled.binding_config_json["miniqmt_quote_control"] == quote_control
    scheduler.shutdown_selection_inference()
