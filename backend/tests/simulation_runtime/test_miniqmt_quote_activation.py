from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time as monotonic_time
from datetime import date, datetime, timedelta, UTC
from threading import Event, RLock, Thread
from types import SimpleNamespace
from typing import Any

import pytest

import backend.services.simulation_runtime.miniqmt_quote_activation as activation_module
from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.miniqmt_quote_contract_config import QuoteContractPolicy
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
from backend.services.miniqmt_execution_runtime.kernel_repository_schema import KERNEL_SCHEMA_PREFLIGHT_KEYS
from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.simulation_runtime.miniqmt_quote_activation import (
    MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
    DrainOnlyB0QuoteV2ControllerFactory,
    MiniQMTKernelProductIngressPending,
    MiniQMTKernelProductIngressSuppression,
    MiniQMTKernelProductLifecycleError,
    MiniQMTQuoteIngressActivation,
    MiniQMTKernelProductRegistryRollbackError,
    MiniQMTKernelProductSyncError,
    _build_bootstrap_fetcher,
    build_miniqmt_quote_ingress_activation_from_env,
)
from backend.services.simulation_runtime.models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
)
from backend.services.simulation_data.daily_context import (
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

    def get_instrument_detail(self, symbol: str) -> dict[str, object]:
        code, exchange = symbol.split(".", 1)
        return {
            "InstrumentID": code,
            "ExchangeID": exchange,
            "PriceTick": 0.01,
            "MinLimitOrderVolume": 100,
            "IsTrading": True,
            "InstrumentStatus": 0,
        }


class _LifecycleActivation:
    controller_factory = None

    def __init__(self) -> None:
        self.epoch_count = 0
        self.watchdog_count = 0
        self.shutdown_count = 0

    def health(self) -> dict[str, object]:
        return {"schema_version": "miniqmt_quote_ingress_activation_v1", "status": "DISABLED"}

    def begin_lifecycle_epoch(self) -> dict[str, object]:
        self.epoch_count += 1
        return self.health()

    def watchdog_tick(self) -> dict[str, object]:
        self.watchdog_count += 1
        return self.health()

    def shutdown(self) -> dict[str, object]:
        self.shutdown_count += 1
        return self.health()


class _StaticExactLeaseOwnerMixin:
    """Supply the exact physical-lease authority for tests focused on another concern."""

    def get_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> object | None:
        del consumer_id, symbols
        active_sink = getattr(self, "active_sink", None)
        return active_sink if active_sink is not None else getattr(self, "sink", None)

    @staticmethod
    def consumer_lease_owner_snapshot(
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> dict[str, object]:
        lease = {
            "lease_id": f"lease:{consumer_id}",
            "data_session_key": MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
            "owner": "simulation_scheduler",
            "consumer_id": consumer_id,
            "symbols": list(symbols),
            "generation": 1,
            "status": "ACTIVE",
            "physical_subscription_id": 1001,
        }
        identity_sha256 = hashlib.sha256(
            json.dumps(lease, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {
            "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
            "consumer_id": consumer_id,
            "requested_symbols": list(symbols),
            "readback_current": True,
            "exact_owner": True,
            "state": "ACTIVE",
            "reason_code": None,
            "registration_generation": 1,
            "expected_owner_identity_sha256": identity_sha256,
            "actual_owner_identity_sha256": identity_sha256,
            "expected_lease": dict(lease),
            "actual_lease": dict(lease),
        }


class _KernelLeaseSupervisor:
    def __init__(self) -> None:
        self.sinks: dict[str, object] = {}
        self.sink_symbols: dict[str, tuple[str, ...]] = {}
        self.consumers: dict[str, dict[str, object]] = {}
        self.expected_consumers: dict[str, dict[str, object]] = {}
        self.lease_generation = 0

    def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
        if consumer_id in self.sinks:
            raise RuntimeError("duplicate sink")
        self.sinks[consumer_id] = sink
        self.sink_symbols[consumer_id] = symbols

    def get_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...]) -> object | None:
        if consumer_id in self.sinks and self.sink_symbols.get(consumer_id) != symbols:
            raise RuntimeError("sink symbol ownership drift")
        return self.sinks.get(consumer_id)

    def unregister_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        sink: object,
    ) -> bool:
        current = self.get_observation_sink(consumer_id=consumer_id, symbols=symbols)
        if current is None:
            return False
        if current is not sink:
            raise RuntimeError("sink callable ownership drift")
        del self.sinks[consumer_id]
        del self.sink_symbols[consumer_id]
        return True

    def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
        if consumer_id in self.consumers:
            raise RuntimeError("duplicate consumer")
        self.lease_generation += 1
        consumer = {
            "lease_id": f"lease:{consumer_id}",
            "lease_status": "ACTIVE",
            "symbols": list(symbols),
            "lease_generation": self.lease_generation,
            "physical_subscription_id": 1000 + self.lease_generation,
        }
        self.consumers[consumer_id] = dict(consumer)
        self.expected_consumers[consumer_id] = dict(consumer)

    def release_consumer(self, *, consumer_id: str) -> bool:
        released = self.consumers.pop(consumer_id, None) is not None
        self.expected_consumers.pop(consumer_id, None)
        return released

    def watchdog_tick(self) -> None:
        return None

    def health(self) -> dict[str, object]:
        return {"status": "READY", "consumers": dict(self.consumers)}

    def consumer_lease_owner_snapshot(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> dict[str, object]:
        expected_consumer = self.expected_consumers.get(consumer_id)
        actual_consumer = self.consumers.get(consumer_id)
        if expected_consumer is None:
            return {
                "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
                "consumer_id": consumer_id,
                "requested_symbols": list(symbols),
                "readback_current": True,
                "exact_owner": False,
                "state": "ABSENT",
                "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_OWNER_ABSENT",
                "registration_generation": None,
                "expected_owner_identity_sha256": None,
                "actual_owner_identity_sha256": None,
                "expected_lease": None,
                "actual_lease": None,
            }
        expected_lease = {
            "lease_id": expected_consumer["lease_id"],
            "data_session_key": MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
            "owner": "simulation_scheduler",
            "consumer_id": consumer_id,
            "symbols": list(expected_consumer["symbols"]),
            "generation": expected_consumer["lease_generation"],
            "status": expected_consumer["lease_status"],
            "physical_subscription_id": expected_consumer["physical_subscription_id"],
        }
        actual_lease = (
            {
                "lease_id": actual_consumer["lease_id"],
                "data_session_key": MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
                "owner": "simulation_scheduler",
                "consumer_id": consumer_id,
                "symbols": list(actual_consumer["symbols"]),
                "generation": actual_consumer["lease_generation"],
                "status": actual_consumer["lease_status"],
                "physical_subscription_id": actual_consumer["physical_subscription_id"],
            }
            if actual_consumer is not None
            else None
        )
        exact = bool(
            actual_lease is not None
            and tuple(expected_consumer["symbols"]) == symbols
            and actual_lease == expected_lease
            and actual_lease["status"] == "ACTIVE"
        )
        expected_identity = hashlib.sha256(
            json.dumps(expected_lease, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        actual_identity = (
            hashlib.sha256(json.dumps(actual_lease, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            if actual_lease is not None
            else None
        )
        return {
            "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
            "consumer_id": consumer_id,
            "requested_symbols": list(symbols),
            "readback_current": True,
            "exact_owner": exact,
            "state": "ACTIVE" if exact else ("LEASE_MISSING" if actual_lease is None else "LEASE_OWNER_DRIFT"),
            "reason_code": None if exact else "MINIQMT_QUOTE_CONSUMER_LEASE_OWNER_DRIFT",
            "registration_generation": expected_consumer["lease_generation"],
            "expected_owner_identity_sha256": expected_identity,
            "actual_owner_identity_sha256": actual_identity,
            "expected_lease": dict(expected_lease),
            "actual_lease": dict(actual_lease) if actual_lease is not None else None,
        }


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
        "policy_json": {
            "algo_code": "SNIPER_MINIQMT",
            "algo_config": {},
            "quote_contract": quote_contract,
        },
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
        self.lifecycle_ticks: list[datetime | None] = []

    def lifecycle_tick(self, *, now_utc: datetime | None = None) -> None:
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
    assert delegate.controller.lifecycle_ticks == [None]
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


def test_enabled_schema_readback_failure_recovers_once_on_scheduler_lifecycle() -> None:
    dependency_calls: list[str] = []
    readback_outcomes: list[str | Exception] = [
        ConnectionError("startup readback unavailable"),
        "applied_and_verified",
    ]

    def schema_gate_reader() -> str:
        outcome = readback_outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ=_enabled_env(),
        schema_gate_reader=schema_gate_reader,
        subscriber_factory=lambda: dependency_calls.append("subscriber") or _Subscriber(),
        qmt_client_factory=lambda: dependency_calls.append("qmt") or _QmtClient(),
    )

    blocked = activation.health()
    assert blocked["status"] == "BLOCKED"
    assert blocked["production_ddl_gate"] == "readback_failed"
    assert blocked["factory_available"] is False
    assert dependency_calls == []

    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=activation,
    )
    assert scheduler._b0_quote_v2_controller_factory is None
    assert scheduler.orchestrator.b0_quote_v2_controller_factory is None

    assert scheduler._advance_miniqmt_quote_ingress_lifecycle() == ()
    recovered = activation.health()
    assert recovered["status"] == "READY"
    assert recovered["production_ddl_gate"] == "applied_and_verified"
    assert recovered["reason_code"] is None
    assert recovered["failure"] is None
    assert recovered["factory_available"] is True
    assert dependency_calls == ["subscriber", "qmt"]
    assert scheduler._b0_quote_v2_controller_factory is activation.controller_factory
    assert scheduler.orchestrator.b0_quote_v2_controller_factory is activation.controller_factory
    assert scheduler._miniqmt_quote_context_adapter is activation.quote_context_adapter

    assert scheduler._advance_miniqmt_quote_ingress_lifecycle() == ()
    assert dependency_calls == ["subscriber", "qmt"]
    assert readback_outcomes == []


def test_enabled_schema_startup_recovery_constructs_one_owner_under_concurrency() -> None:
    readback_calls = 0
    dependency_calls: list[str] = []
    call_lock = RLock()

    def schema_gate_reader() -> str:
        nonlocal readback_calls
        with call_lock:
            readback_calls += 1
            if readback_calls == 1:
                raise ConnectionError("startup readback unavailable")
        return "applied_and_verified"

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ=_enabled_env(),
        schema_gate_reader=schema_gate_reader,
        subscriber_factory=lambda: dependency_calls.append("subscriber") or _Subscriber(),
        qmt_client_factory=lambda: dependency_calls.append("qmt") or _QmtClient(),
    )
    threads = [Thread(target=activation._recover_enabled_startup_if_needed) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert all(not thread.is_alive() for thread in threads)
    assert activation.health()["status"] == "READY"
    assert readback_calls == 2
    assert dependency_calls == ["subscriber", "qmt"]


def test_production_schema_gate_uses_full_postgres_kernel_preflight(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[str] = []

    class _Repository:
        @staticmethod
        def preflight_schema() -> dict[str, bool]:
            calls.append("preflight_schema")
            return {key: True for key in KERNEL_SCHEMA_PREFLIGHT_KEYS}

    monkeypatch.setattr(activation_module, "PostgresMiniQMTKernelRepository", _Repository)
    assert activation_module._production_quote_event_schema_gate() == "applied_and_verified"
    assert calls == ["preflight_schema"]

    complete = {key: True for key in KERNEL_SCHEMA_PREFLIGHT_KEYS}
    invalid_readbacks = [
        {key: value for key, value in complete.items() if key != missing_key} for missing_key in sorted(complete)
    ]
    invalid_readbacks.extend(
        [
            {**complete, "unexpected_schema_key": True},
            {**complete, "event_contract_schema": False},
            {},
        ]
    )
    for invalid in invalid_readbacks:

        class _IncompleteRepository:
            @staticmethod
            def preflight_schema() -> dict[str, bool]:
                return invalid

        monkeypatch.setattr(activation_module, "PostgresMiniQMTKernelRepository", _IncompleteRepository)
        with pytest.raises(RuntimeError, match="full PostgreSQL schema preflight"):
            activation_module._production_quote_event_schema_gate()


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
    assert activation.quote_context_adapter is not None
    assert activation.quote_context_adapter.context_store is activation.supervisor.context_store
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


def test_watchdog_syncs_every_kernel_product_runtime_and_raises_aggregate_after_isolation() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    calls: list[str] = []

    class _Runtime:
        def __init__(self, runtime_id: str, *, fail: bool) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.trade_date = date(2026, 8, 11)
            self.symbols = ("600000.SH",) if runtime_id == "runtime_a" else ("000001.SZ",)
            self.source_capability_sha256 = runtime_id * 8
            self.fail = fail

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, *, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            assert observed_at.tzinfo is not None
            assert monotonic_ns > 0
            calls.append(self.runtime_id)
            if self.fail:
                raise RuntimeError(f"snapshot failure for {self.runtime_id}")
            return ()

    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (_Runtime("runtime_a", fail=True), _Runtime("runtime_b", fail=False)):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(MiniQMTKernelProductSyncError) as caught:
        activation.watchdog_tick()
    assert calls == ["runtime_a", "runtime_b"]
    assert caught.value.reason_code == "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED"
    assert caught.value.context["ordered_failures"][0]["runtime_id"] == "runtime_a"


def test_missing_retry_owner_is_loud_and_does_not_block_healthy_peer_watchdog() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    calls: list[str] = []

    class _Runtime:
        def __init__(self, runtime_id: str, symbol: str) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding:{runtime_id}"
            self.trade_date = date(2026, 8, 11)
            self.symbols = (symbol,)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, *, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            assert observed_at.tzinfo is not None and monotonic_ns > 0
            calls.append(self.runtime_id)
            return ()

    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    runtime_a = _Runtime("runtime_missing_owner", "600000.SH")
    runtime_b = _Runtime("runtime_healthy_peer", "000001.SZ")
    activation.register_kernel_product_runtime(runtime=runtime_a, symbols=runtime_a.symbols)
    activation.register_kernel_product_runtime(runtime=runtime_b, symbols=runtime_b.symbols)
    activation._kernel_retry_states.pop(runtime_a.runtime_id)

    health = activation.health()
    by_runtime = {item["runtime_id"]: item["ingress_retry"] for item in health["kernel_product_runtimes"]}
    assert by_runtime[runtime_a.runtime_id]["state"] == "OWNER_DRIFT"
    assert by_runtime[runtime_b.runtime_id]["state"] == "HEALTHY"
    with pytest.raises(MiniQMTKernelProductSyncError) as caught:
        activation.watchdog_tick()
    assert calls == [runtime_b.runtime_id]
    assert caught.value.context["ordered_failures"][0]["reason_code"] == ("MINIQMT_K6_PRODUCT_RETRY_STATE_MISSING")


def test_missing_operation_retry_owner_is_not_recreated_or_silently_suppressed() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_missing_callback_owner"
        binding_id = "binding_missing_callback_owner"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            raise AssertionError("missing callback owner reached business sink")

        @staticmethod
        def scheduler_tick_v1(*, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            raise AssertionError((observed_at, monotonic_ns))

    runtime = _Runtime()
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    state = activation._kernel_retry_states[runtime.runtime_id]
    state.operations.pop("CALLBACK")

    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "OWNER_DRIFT"
    sink = supervisor.sinks[f"k6d-kernel-v2:{runtime.runtime_id}"]
    frame = SimpleNamespace(symbol="600000.SH", ingress_generation=1, ingress_sequence=1)
    observation = SimpleNamespace(
        frame=frame,
        market_data_id="market_data_missing_operation",
        context_id="context_missing_operation",
    )
    with pytest.raises(MiniQMTKernelProductLifecycleError) as caught:
        sink(observation, object())  # type: ignore[operator]
    assert caught.value.reason_code == "MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING"
    assert set(state.operations) == {"WATCHDOG"}

    with pytest.raises(MiniQMTKernelProductSyncError) as watchdog:
        activation.watchdog_tick()
    assert watchdog.value.context["ordered_failures"][0]["reason_code"] == (
        "MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING"
    )
    assert set(state.operations) == {"WATCHDOG"}


def test_watchdog_completion_generation_drift_isolated_without_mutating_successor_or_peer() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    calls: list[str] = []

    class _Runtime:
        def __init__(self, runtime_id: str, symbol: str, *, drift: bool) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding:{runtime_id}"
            self.trade_date = date(2026, 8, 11)
            self.symbols = (symbol,)
            self.drift = drift

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, *, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            assert observed_at.tzinfo is not None and monotonic_ns > 0
            calls.append(self.runtime_id)
            if self.drift:
                current = activation._kernel_retry_states[self.runtime_id]
                activation._kernel_retry_states[self.runtime_id] = activation._new_retry_state(
                    runtime_id=self.runtime_id,
                    runtime=self,
                    lifecycle_generation=current.lifecycle_generation + 1,
                )
            return ()

    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    drifted = _Runtime("runtime_completion_drift", "600000.SH", drift=True)
    healthy = _Runtime("runtime_completion_peer", "000001.SZ", drift=False)
    activation.register_kernel_product_runtime(runtime=drifted, symbols=drifted.symbols)
    activation.register_kernel_product_runtime(runtime=healthy, symbols=healthy.symbols)

    with pytest.raises(MiniQMTKernelProductSyncError) as caught:
        activation.watchdog_tick()
    assert calls == [drifted.runtime_id, healthy.runtime_id]
    assert caught.value.context["ordered_failures"][0]["reason_code"] == ("MINIQMT_K6_PRODUCT_ATTEMPT_LIFECYCLE_DRIFT")
    successor = activation._kernel_retry_states[drifted.runtime_id]
    assert successor.operations["WATCHDOG"].last_success_at_utc is None


def test_callback_completion_generation_drift_fails_loud_without_successor_mutation() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_callback_completion_drift"
        binding_id = "binding_callback_completion_drift"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def observe_b0_quote_v1(self, *_values: object) -> None:
            current = activation._kernel_retry_states[self.runtime_id]
            activation._kernel_retry_states[self.runtime_id] = activation._new_retry_state(
                runtime_id=self.runtime_id,
                runtime=self,
                lifecycle_generation=current.lifecycle_generation + 1,
            )

    runtime = _Runtime()
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    sink = supervisor.sinks[f"k6d-kernel-v2:{runtime.runtime_id}"]
    frame = SimpleNamespace(symbol="600000.SH", ingress_generation=2, ingress_sequence=4)
    observation = SimpleNamespace(
        frame=frame,
        market_data_id="market_data_callback_drift",
        context_id="context_callback_drift",
    )

    with pytest.raises(MiniQMTKernelProductLifecycleError) as caught:
        sink(observation, object())  # type: ignore[operator]
    assert caught.value.reason_code == "MINIQMT_K6_PRODUCT_ATTEMPT_LIFECYCLE_DRIFT"
    successor = activation._kernel_retry_states[runtime.runtime_id]
    assert successor.operations["CALLBACK"].last_success_at_utc is None
    assert activation._kernel_product_in_flight == {}


@pytest.mark.parametrize(
    "constraint_name",
    (
        "ck_miniqmt_event_id",
        "ck_miniqmt_event_sequence",
        "ck_miniqmt_event_type",
        "ck_miniqmt_event_source",
        "ck_miniqmt_k2_event_composite",
        "ck_miniqmt_k2_event_contract",
    ),
)
def test_kernel_product_schema_failure_backoff_suppresses_quote_burst_and_recovers_automatically(
    constraint_name: str,
) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    now_utc = [datetime(2026, 8, 11, 2, 5, tzinfo=UTC)]
    now_monotonic_ns = [10_000_000_000]
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]

    class _Diag:
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    _Diag.constraint_name = constraint_name

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_schema_backoff"
        binding_id = "binding_schema_backoff"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)
        source_capability_sha256 = "a" * 64

        def __init__(self) -> None:
            self.quote_attempts = 0
            self.clock_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.quote_attempts += 1
            if self.quote_attempts == 1:
                raise _SchemaFailure(f"violates check constraint {constraint_name}")

        def scheduler_tick_v1(self, *, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            self.clock_attempts += 1
            assert observed_at.tzinfo is not None
            assert monotonic_ns == now_monotonic_ns[0]
            return ()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        def __init__(self) -> None:
            self.sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            assert consumer_id == "k6d-kernel-v2:runtime_schema_backoff"
            assert symbols == ("600000.SH",)
            self.sink = sink

        def get_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...]) -> object | None:
            assert consumer_id == "k6d-kernel-v2:runtime_schema_backoff"
            assert symbols == ("600000.SH",)
            return self.sink

        @staticmethod
        def acquire_consumer(*, consumer_id: str, symbols: list[str]) -> None:
            assert consumer_id == "k6d-kernel-v2:runtime_schema_backoff"
            assert symbols == ["600000.SH"]

        @staticmethod
        def watchdog_tick() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    supervisor = _Supervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert callable(supervisor.sink)

    with pytest.raises(QuoteContractError) as first_failure:
        supervisor.sink(object(), object())
    assert first_failure.value.reason_code.value == "ADAPTIVE_IS_QUOTE_CONSUMER_FAILURE"
    assert first_failure.value.context["constraint_name"] == constraint_name
    suppressions = [supervisor.sink(object(), object()) for _ in range(100)]
    assert all(
        isinstance(item, MiniQMTKernelProductIngressSuppression)
        and item.disposition == "RETRY_BACKOFF_SUPPRESSED"
        and item.as_dict()["business_success"] is False
        for item in suppressions
    )
    assert runtime.quote_attempts == 1

    degraded = activation.health()
    assert degraded["status"] == "DEGRADED"
    retry = degraded["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "RETRY_BACKOFF"
    assert retry["suppressed_callback_count"] == 100
    assert retry["automatic_retry"] is True
    assert retry["manual_ack_required"] is False
    assert retry["business_gate"] is False
    assert retry["broker_side_effect_state"] == "UNKNOWN"
    assert len(retry["operations"]["CALLBACK"]["pending"]) == 1

    activation.watchdog_tick()
    assert runtime.clock_attempts == 1
    assert activation.health()["status"] == "DEGRADED"
    now_utc[0] = datetime(2026, 8, 11, 2, 6, tzinfo=UTC)
    now_monotonic_ns[0] += 60_000_000_000
    watchdog_recovered = activation.watchdog_tick()
    assert runtime.quote_attempts == 1
    assert runtime.clock_attempts == 2
    assert watchdog_recovered["status"] == "DEGRADED"
    supervisor.sink(object(), object())
    recovered = activation.health()
    assert runtime.quote_attempts == 2
    assert recovered["status"] == "DRAINING"
    retry = recovered["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "HEALTHY"
    assert retry["active_failure"] is None
    assert retry["last_failure"]["constraint_name"] == constraint_name
    assert retry["operations"]["CALLBACK"]["pending"] == []
    supervisor.sink(object(), object())
    assert runtime.quote_attempts == 3


def test_fresh_live_quote_resolves_only_its_symbol_without_dropping_newer_or_peer_pending_ticks() -> None:
    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_multi_symbol_recovery"
        binding_id = "binding_multi_symbol_recovery"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH", "000001.SZ")

        def __init__(self) -> None:
            self.fail_schema = True
            self.observed_symbols: list[str] = []

        def observe_b0_quote_v1(self, observation: object, *_values: object) -> None:
            self.observed_symbols.append(observation.quote.symbol)  # type: ignore[attr-defined]
            if self.fail_schema:
                raise _SchemaFailure("schema conflict")

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        def unregister_observation_sink(self, **_values: object) -> bool:
            self.sink = None
            return True

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            return True

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    def observation(symbol: str, sequence: int) -> SimpleNamespace:
        frame = SimpleNamespace(
            symbol=symbol,
            ingress_generation=4,
            ingress_sequence=sequence,
        )
        return SimpleNamespace(
            frame=frame,
            quote=frame,
            market_data_id=f"market_data_{symbol}_{sequence}",
            context_id="context_multi_symbol",
        )

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    now_utc = [datetime(2026, 8, 11, 1, 30, tzinfo=UTC)]
    now_monotonic_ns = [1_000_000_000]
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]
    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    with pytest.raises(QuoteContractError):
        supervisor.sink(observation("600000.SH", 1), object())
    assert isinstance(
        supervisor.sink(observation("000001.SZ", 2), object()),
        MiniQMTKernelProductIngressSuppression,
    )
    pending = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]["pending"]
    assert {item["symbol"] for item in pending} == {"600000.SH", "000001.SZ"}

    runtime.fail_schema = False
    now_utc[0] += timedelta(seconds=60)
    now_monotonic_ns[0] += 60_000_000_000
    supervisor.sink(observation("600000.SH", 3), object())
    operation = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]
    assert runtime.observed_symbols == ["600000.SH", "600000.SH"]
    assert [item["symbol"] for item in operation["pending"]] == ["000001.SZ"]
    assert operation["not_replayed_pending_count"] == 1
    assert operation["last_pending_resolution"]["disposition"] == ("SUPERSEDED_BY_FRESH_LIVE_QUOTE_NOT_REPLAYED")
    supervisor.sink(observation("000001.SZ", 4), object())
    operation = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]
    assert operation["pending"] == []
    assert operation["not_replayed_pending_count"] == 2


def test_older_in_flight_quote_success_does_not_discard_a_newer_suppressed_quote() -> None:
    entered = Event()
    allow_first = Event()

    class _Runtime:
        runtime_id = "runtime_newer_pending_preserved"
        binding_id = "binding_newer_pending_preserved"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.observed_sequences: list[int] = []

        def observe_b0_quote_v1(self, observation: object, *_values: object) -> None:
            sequence = observation.quote.ingress_sequence  # type: ignore[attr-defined]
            self.observed_sequences.append(sequence)
            if sequence == 1:
                entered.set()
                assert allow_first.wait(timeout=5)

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    def observation(sequence: int) -> SimpleNamespace:
        frame = SimpleNamespace(
            symbol="600000.SH",
            ingress_generation=5,
            ingress_sequence=sequence,
        )
        return SimpleNamespace(
            frame=frame,
            quote=frame,
            market_data_id=f"market_data_pending_{sequence}",
            context_id="context_pending_order",
        )

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    worker = Thread(target=lambda: supervisor.sink(observation(1), object()))
    worker.start()
    assert entered.wait(timeout=5)
    suppressed = supervisor.sink(observation(2), object())
    assert isinstance(suppressed, MiniQMTKernelProductIngressSuppression)
    assert suppressed.consumer_id == "k6d-kernel-v2:runtime_newer_pending_preserved"
    assert suppressed.symbol == "600000.SH"
    assert suppressed.ingress_generation == 5
    assert suppressed.ingress_sequence == 2
    allow_first.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    operation = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]
    assert [item["ingress_sequence"] for item in operation["pending"]] == [2]
    assert operation["state"] == "RECOVERY_PENDING"

    supervisor.sink(observation(3), object())
    operation = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]
    assert operation["pending"] == []
    assert operation["not_replayed_pending_count"] == 1
    assert runtime.observed_sequences == [1, 3]


def test_prior_day_watchdog_expires_quote_retry_without_replay_before_exact_release() -> None:
    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_prior_day_retry"
        binding_id = "binding_prior_day_retry"
        trade_date = date(2026, 8, 10)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.quote_attempts = 0
            self.clock_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.quote_attempts += 1
            raise _SchemaFailure("schema conflict")

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def __init__(self) -> None:
            self.operations: list[str] = []

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink
            self.operations.append(f"register:{consumer_id}")

        def unregister_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> bool:
            self.operations.append(f"unregister:{consumer_id}")
            assert self.sink is sink
            self.sink = None
            return True

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        def acquire_consumer(self, **_values: object) -> None:
            self.operations.append("acquire")

        def release_consumer(self, *, consumer_id: str) -> bool:
            self.operations.append(f"release:{consumer_id}")
            return True

        @staticmethod
        def watchdog_tick() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    observed = datetime(2026, 8, 11, 1, 30, tzinfo=UTC)
    activation._kernel_retry_clock_utc = lambda: observed
    activation._kernel_retry_monotonic_ns = lambda: 61_000_000_000
    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(QuoteContractError):
        supervisor.sink(object(), object())
    state = activation._kernel_retry_states[runtime.runtime_id]

    health = activation.watchdog_tick()

    assert runtime.quote_attempts == 1
    assert runtime.clock_attempts == 1
    assert state.operations["CALLBACK"].last_pending_resolution == {
        "disposition": "EXPIRED_PRIOR_SESSION_NOT_REPLAYED",
        "resolved_at_utc": observed.isoformat(),
        "pending_count": 1,
    }
    assert activation.get_kernel_product_runtime(runtime.runtime_id) is None
    assert health["kernel_product_runtimes"] == []
    assert supervisor.operations[-2:] == [
        f"unregister:k6d-kernel-v2:{runtime.runtime_id}",
        f"release:k6d-kernel-v2:{runtime.runtime_id}",
    ]


def test_kernel_product_schema_backoff_is_per_runtime_and_does_not_reclassify_other_failures() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Diag:
        constraint_name = "ck_miniqmt_k2_event_composite"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _WrongRelationDiag:
        constraint_name = "ck_miniqmt_k2_event_composite"
        schema_name = "other_schema"
        table_name = "execution_runtime_event"

    class _WrongRelationFailure(RuntimeError):
        pgcode = "23514"
        diag = _WrongRelationDiag()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        def __init__(self) -> None:
            self.sinks: dict[str, object] = {}

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sinks[consumer_id] = sink

        def get_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...]) -> object | None:
            return self.sinks.get(consumer_id)

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    class _Runtime:
        symbols = ("600000.SH",)
        trade_date = date(2026, 8, 11)

        def __init__(self, runtime_id: str, failure: Exception | None) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.failure = failure
            self.calls = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.calls += 1
            if self.failure is not None:
                raise self.failure

    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    schema_runtime = _Runtime("runtime_schema", _SchemaFailure("schema conflict"))
    healthy_runtime = _Runtime("runtime_healthy", None)
    other_runtime = _Runtime(
        "runtime_other",
        RuntimeError("violates check constraint ck_miniqmt_event_source without SQLSTATE"),
    )
    wrong_relation_runtime = _Runtime("runtime_wrong_relation", _WrongRelationFailure("same name, wrong table"))
    for runtime in (schema_runtime, healthy_runtime, other_runtime, wrong_relation_runtime):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    with pytest.raises(QuoteContractError):
        supervisor.sinks["k6d-kernel-v2:runtime_schema"](object(), object())
    supervisor.sinks["k6d-kernel-v2:runtime_schema"](object(), object())
    supervisor.sinks["k6d-kernel-v2:runtime_healthy"](object(), object())
    with pytest.raises(RuntimeError, match="without SQLSTATE"):
        supervisor.sinks["k6d-kernel-v2:runtime_other"](object(), object())
    with pytest.raises(_WrongRelationFailure, match="wrong table"):
        supervisor.sinks["k6d-kernel-v2:runtime_wrong_relation"](object(), object())
    with pytest.raises(RuntimeError, match="without SQLSTATE"):
        supervisor.sinks["k6d-kernel-v2:runtime_other"](object(), object())

    assert schema_runtime.calls == 1
    assert healthy_runtime.calls == 1
    assert other_runtime.calls == 2
    assert wrong_relation_runtime.calls == 1
    health_by_runtime = {
        item["runtime_id"]: item["ingress_retry"] for item in activation.health()["kernel_product_runtimes"]
    }
    assert health_by_runtime["runtime_schema"]["state"] == "RETRY_BACKOFF"
    assert health_by_runtime["runtime_healthy"]["state"] == "HEALTHY"
    assert health_by_runtime["runtime_other"]["state"] == "RETRY_READY"
    assert health_by_runtime["runtime_other"]["failure_class"] == "RUNTIME_OPERATION_FAILURE"
    assert health_by_runtime["runtime_wrong_relation"]["state"] == "RETRY_READY"
    assert health_by_runtime["runtime_wrong_relation"]["failure_class"] == "RUNTIME_OPERATION_FAILURE"


def test_kernel_product_schema_retry_is_single_flight_per_operation_without_cross_path_gate() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 11, 2, 0, tzinfo=UTC)
    entered = Event()
    release = Event()
    thread_failures: list[Exception] = []

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_single_flight"
        binding_id = "binding_single_flight"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.quote_attempts = 0
            self.clock_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.quote_attempts += 1
            entered.set()
            assert release.wait(timeout=5)
            raise _SchemaFailure("schema conflict")

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def watchdog_tick() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    supervisor = _Supervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert callable(supervisor.sink)

    def invoke() -> None:
        try:
            supervisor.sink(object(), object())
        except Exception as exc:  # noqa: BLE001 - direct assertion captures the typed failure from the worker thread.
            thread_failures.append(exc)

    worker = Thread(target=invoke)
    worker.start()
    assert entered.wait(timeout=5)
    in_flight_health = activation.health()
    assert in_flight_health["status"] == "DRAINING"
    assert in_flight_health["kernel_product_runtimes"][0]["ingress_retry"]["state"] == "HEALTHY"
    callback_suppression = supervisor.sink(object(), object())
    assert isinstance(callback_suppression, MiniQMTKernelProductIngressSuppression)
    assert callback_suppression.disposition == "SINGLE_FLIGHT_SUPPRESSED"
    watchdog_health = activation.watchdog_tick()
    assert watchdog_health["status"] == "DEGRADED"
    watchdog_retry = watchdog_health["kernel_product_runtimes"][0]["ingress_retry"]
    assert watchdog_retry["state"] == "RECOVERY_PENDING"
    assert watchdog_retry["operations"]["WATCHDOG"]["last_success_at_utc"] is not None
    release.set()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert runtime.quote_attempts == 1
    assert runtime.clock_attempts == 1
    assert len(thread_failures) == 1 and isinstance(thread_failures[0], QuoteContractError)
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["suppressed_callback_count"] == 1
    assert retry["suppressed_watchdog_count"] == 0


def test_blocked_watchdog_runtime_keeps_one_owner_without_starving_peer_runtime() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 11, 2, 0, tzinfo=UTC)
    activation._kernel_watchdog_peer_wait_seconds = 0.01
    entered = Event()
    release = Event()

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str, *, blocked: bool) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.blocked = blocked
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            if self.blocked:
                entered.set()
                assert release.wait(timeout=5)
            return ()

    supervisor = _KernelLeaseSupervisor()
    blocked = _Runtime("runtime_a_blocked", blocked=True)
    peer = _Runtime("runtime_b_peer", blocked=False)
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (blocked, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    first = activation.watchdog_tick()
    assert entered.wait(timeout=1)
    assert first["status"] == "DRAINING"
    assert blocked.clock_attempts == 1
    assert peer.clock_attempts == 1
    assert tuple(activation._kernel_watchdog_workers) == ("runtime_a_blocked",)

    second = activation.watchdog_tick()
    assert second["status"] == "DRAINING"
    assert blocked.clock_attempts == 1
    assert peer.clock_attempts == 2

    release.set()
    for _attempt in range(50):
        if not activation._kernel_watchdog_workers:
            break
        activation.watchdog_tick()
        monotonic_time.sleep(0.01)
    assert activation._kernel_watchdog_workers == {}
    assert activation._kernel_product_in_flight == {}


def test_blocked_callback_runtime_does_not_starve_peer_quote_sink() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    entered = Event()
    release = Event()
    peer_completed = Event()

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str, *, blocked: bool) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.blocked = blocked
            self.quote_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.quote_attempts += 1
            if self.blocked:
                entered.set()
                assert release.wait(timeout=5)
            else:
                peer_completed.set()

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    supervisor = _KernelLeaseSupervisor()
    blocked = _Runtime("runtime_callback_blocked", blocked=True)
    peer = _Runtime("runtime_callback_peer", blocked=False)
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (blocked, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    observation = SimpleNamespace(
        frame=SimpleNamespace(symbol="600000.SH", ingress_generation=1, ingress_sequence=1),
        market_data_id="market_data_callback_peer",
        context_id="context_callback_peer",
    )
    context = SimpleNamespace(context_id="context_callback_peer")

    started = monotonic_time.monotonic()
    pending = supervisor.sinks["k6d-kernel-v2:runtime_callback_blocked"](observation, context)
    assert isinstance(pending, MiniQMTKernelProductIngressPending)
    assert pending.as_dict()["business_success"] is None
    assert entered.wait(timeout=1)
    assert monotonic_time.monotonic() - started < 0.25
    assert supervisor.sinks["k6d-kernel-v2:runtime_callback_peer"](observation, context) is None
    assert peer_completed.wait(timeout=1)
    assert blocked.quote_attempts == 1 and peer.quote_attempts == 1
    health = activation.health()
    assert health["kernel_callback_worker_count"] == 2
    assert health["kernel_callback_active_count"] == 1
    assert (
        next(item for item in health["kernel_callback_workers"] if item["runtime_id"] == blocked.runtime_id)[
            "active_attempt_token"
        ]
        == pending.attempt_token
    )

    blocked_worker = activation._kernel_callback_workers[blocked.runtime_id]
    blocked_thread = blocked_worker.thread
    release.set()
    for _attempt in range(100):
        if not activation._kernel_product_in_flight:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_product_in_flight == {}
    assert activation._kernel_callback_workers[blocked.runtime_id] is blocked_worker
    assert activation._kernel_callback_workers[blocked.runtime_id].thread is blocked_thread
    assert blocked_thread.is_alive()
    assert supervisor.sinks["k6d-kernel-v2:runtime_callback_blocked"](observation, context) is None
    assert blocked.quote_attempts == 2
    assert activation._kernel_callback_workers[blocked.runtime_id].thread is blocked_thread
    activation.release_kernel_product_runtime(blocked.runtime_id)
    activation.release_kernel_product_runtime(peer.runtime_id)
    assert activation._kernel_callback_workers == {}


def test_async_callback_failure_retains_loud_runtime_and_worker_evidence() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    entered = Event()
    release = Event()

    class _Runtime:
        runtime_id = "runtime_async_callback_failure"
        binding_id = "binding_async_callback_failure"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.attempts += 1
            if self.attempts == 1:
                entered.set()
                assert release.wait(timeout=5)
                raise RuntimeError("late callback failure")

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    supervisor = _KernelLeaseSupervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    observation = SimpleNamespace(
        frame=SimpleNamespace(symbol="600000.SH", ingress_generation=1, ingress_sequence=1),
        market_data_id="market_data_async_failure",
        context_id="context_async_failure",
    )
    pending = supervisor.sinks[f"k6d-kernel-v2:{runtime.runtime_id}"](observation, object())
    assert isinstance(pending, MiniQMTKernelProductIngressPending)
    assert entered.wait(timeout=1)
    release.set()
    for _attempt in range(100):
        if not activation._kernel_product_in_flight:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_product_in_flight == {}
    health = activation.health()
    retry = health["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "RETRY_READY"
    assert retry["automatic_retry"] is True
    assert retry["manual_ack_required"] is False
    assert retry["business_gate"] is False
    assert retry["active_failure"]["reason_code"] == "MINIQMT_K6_PRODUCT_OPERATION_FAILED"
    assert retry["last_failure"]["reason_code"] == "MINIQMT_K6_PRODUCT_OPERATION_FAILED"
    worker = next(item for item in health["kernel_callback_workers"] if item["runtime_id"] == runtime.runtime_id)
    assert worker["last_failure"]["reason_code"] == "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_TASK_FAILED"
    assert worker["last_failure"]["exception_message"] == "late callback failure"
    assert supervisor.sinks[f"k6d-kernel-v2:{runtime.runtime_id}"](observation, object()) is None
    assert runtime.attempts == 2
    recovered = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert recovered["state"] == "HEALTHY"
    assert recovered["active_failure"] is None
    activation.release_kernel_product_runtime(runtime.runtime_id)


def test_async_callback_non_none_result_retries_loudly_then_recovers_on_live_none() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 11, 2, 0, tzinfo=UTC)
    entered = Event()
    release = Event()

    class _Runtime:
        runtime_id = "runtime_async_callback_result_invalid"
        binding_id = "binding_async_callback_result_invalid"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> object | None:
            self.attempts += 1
            if self.attempts == 1:
                entered.set()
                assert release.wait(timeout=5)
                return {"invalid": "late non-None callback result"}
            return None

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    supervisor = _KernelLeaseSupervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    observation = SimpleNamespace(
        frame=SimpleNamespace(symbol="600000.SH", ingress_generation=1, ingress_sequence=1),
        market_data_id="market_data_async_result_invalid",
        context_id="context_async_result_invalid",
    )
    sink = supervisor.sinks[f"k6d-kernel-v2:{runtime.runtime_id}"]

    pending = sink(observation, object())  # type: ignore[operator]
    assert isinstance(pending, MiniQMTKernelProductIngressPending)
    assert pending.as_dict()["business_success"] is None
    completions: list[object] = []
    pending.completion_signal.subscribe(completions.append)
    assert entered.wait(timeout=1)
    release.set()
    for _attempt in range(100):
        if not activation._kernel_product_in_flight:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_product_in_flight == {}
    monotonic_time.sleep(0.02)

    failed = activation.health()
    retry = failed["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "RETRY_READY"
    assert retry["active_failure"]["reason_code"] == "MINIQMT_K6_PRODUCT_CALLBACK_RESULT_INVALID"
    worker = next(item for item in failed["kernel_callback_workers"] if item["runtime_id"] == runtime.runtime_id)
    assert worker["last_failure"]["reason_code"] == "MINIQMT_K6_PRODUCT_CALLBACK_RESULT_INVALID"
    assert len(completions) == 1
    completion = completions[0].as_dict()  # type: ignore[attr-defined]
    assert completion["pending_identity_sha256"] == pending.pending_identity_sha256
    assert completion["attempt_token"] == pending.attempt_token
    assert completion["disposition"] == "ASYNC_FAILED"
    assert completion["failure"]["reason_code"] == "MINIQMT_K6_PRODUCT_CALLBACK_RESULT_INVALID"

    assert sink(observation, object()) is None  # type: ignore[operator]
    assert runtime.attempts == 2
    recovered = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert recovered["state"] == "HEALTHY"
    assert recovered["active_failure"] is None
    activation.release_kernel_product_runtime(runtime.runtime_id)


def test_async_schema_failure_after_pending_enters_backoff_without_blocking_peer() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    entered = Event()
    release = Event()

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _BlockedRuntime:
        runtime_id = "runtime_async_schema_failure"
        binding_id = "binding_async_schema_failure"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.attempts += 1
            entered.set()
            assert release.wait(timeout=5)
            raise _SchemaFailure("late schema callback failure")

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    class _PeerRuntime:
        runtime_id = "runtime_async_schema_peer"
        binding_id = "binding_async_schema_peer"
        trade_date = date(2026, 8, 11)
        symbols = ("000001.SZ",)

        def __init__(self) -> None:
            self.attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.attempts += 1

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    supervisor = _KernelLeaseSupervisor()
    blocked = _BlockedRuntime()
    peer = _PeerRuntime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=blocked, symbols=blocked.symbols)
    activation.register_kernel_product_runtime(runtime=peer, symbols=peer.symbols)
    blocked_observation = SimpleNamespace(
        frame=SimpleNamespace(symbol="600000.SH", ingress_generation=1, ingress_sequence=1),
        market_data_id="market_data_async_schema_failure",
        context_id="context_async_schema_failure",
    )
    peer_observation = SimpleNamespace(
        frame=SimpleNamespace(symbol="000001.SZ", ingress_generation=1, ingress_sequence=1),
        market_data_id="market_data_async_schema_peer",
        context_id="context_async_schema_peer",
    )
    pending = supervisor.sinks[f"k6d-kernel-v2:{blocked.runtime_id}"](blocked_observation, object())
    assert isinstance(pending, MiniQMTKernelProductIngressPending)
    assert entered.wait(timeout=1)
    assert supervisor.sinks[f"k6d-kernel-v2:{peer.runtime_id}"](peer_observation, object()) is None
    assert peer.attempts == 1
    release.set()
    for _attempt in range(100):
        if not activation._kernel_product_in_flight:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_product_in_flight == {}
    health_by_runtime = {
        item["runtime_id"]: item["ingress_retry"] for item in activation.health()["kernel_product_runtimes"]
    }
    assert health_by_runtime[blocked.runtime_id]["state"] == "RETRY_BACKOFF"
    assert health_by_runtime[blocked.runtime_id]["active_failure"]["constraint_name"] == ("ck_miniqmt_event_source")
    assert health_by_runtime[peer.runtime_id]["state"] == "HEALTHY"
    suppressed = supervisor.sinks[f"k6d-kernel-v2:{blocked.runtime_id}"](blocked_observation, object())
    assert isinstance(suppressed, MiniQMTKernelProductIngressSuppression)
    assert suppressed.disposition == "RETRY_BACKOFF_SUPPRESSED"
    assert blocked.attempts == 1
    assert supervisor.sinks[f"k6d-kernel-v2:{peer.runtime_id}"](peer_observation, object()) is None
    assert peer.attempts == 2
    activation.release_kernel_product_runtime(blocked.runtime_id)
    activation.release_kernel_product_runtime(peer.runtime_id)


def test_completed_old_watchdog_result_is_consumed_before_same_id_successor_registration() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_watchdog_peer_wait_seconds = 0.0
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 11, 2, 0, tzinfo=UTC)
    entered = Event()
    release = Event()

    class _Runtime:
        symbols = ("600000.SH",)

        def __init__(self, *, trade_date_value: date, blocked: bool) -> None:
            self.runtime_id = "runtime_watchdog_successor"
            self.binding_id = f"binding_{trade_date_value.isoformat()}"
            self.trade_date = trade_date_value
            self.blocked = blocked
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            if self.blocked:
                entered.set()
                assert release.wait(timeout=5)
            return ()

    supervisor = _KernelLeaseSupervisor()
    old = _Runtime(trade_date_value=date(2026, 8, 10), blocked=True)
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=old, symbols=old.symbols)

    activation.watchdog_tick()
    assert entered.wait(timeout=1)
    release.set()
    for _attempt in range(100):
        worker = activation._kernel_watchdog_workers.get(old.runtime_id)
        if worker is not None and not worker.thread.is_alive():
            break
        monotonic_time.sleep(0.01)
    assert old.runtime_id in activation._kernel_watchdog_workers

    activation.release_kernel_product_runtime(old.runtime_id)
    assert activation._kernel_watchdog_workers == {}
    successor = _Runtime(trade_date_value=date(2026, 8, 11), blocked=False)
    activation.register_kernel_product_runtime(runtime=successor, symbols=successor.symbols)
    activation.watchdog_tick()
    assert successor.clock_attempts == 1
    assert activation.get_kernel_product_runtime(successor.runtime_id) is successor


def test_completed_release_worker_is_consumed_before_same_id_successor_registration() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        symbols = ("600000.SH",)
        trade_date = date(2026, 8, 10)

        def __init__(self, marker: str) -> None:
            self.runtime_id = "runtime_release_worker_successor"
            self.binding_id = f"binding_{marker}"

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    old = _Runtime("old")
    activation.register_kernel_product_runtime(runtime=old, symbols=old.symbols)
    old_generation = activation._kernel_retry_states[old.runtime_id].lifecycle_generation
    old_worker_thread = activation._kernel_callback_workers[old.runtime_id].thread
    activation._start_kernel_release_worker(
        runtime_id=old.runtime_id,
        runtime=old,
        lifecycle_generation=old_generation,
        observed_at=datetime(2026, 8, 11, 2, 0, tzinfo=UTC),
        operation="PRIOR_DAY_RELEASE",
        failure_reason_code="MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_FAILED",
    )
    release_worker = activation._kernel_release_workers[old.runtime_id]
    release_worker.thread.join(timeout=5)
    assert not release_worker.thread.is_alive()
    assert activation.get_kernel_product_runtime(old.runtime_id) is None
    assert old.runtime_id in activation._kernel_release_workers

    successor = _Runtime("successor")
    with pytest.raises(MiniQMTKernelProductLifecycleError) as blocked:
        activation.register_kernel_product_runtime(runtime=successor, symbols=successor.symbols)
    assert blocked.value.reason_code == "MINIQMT_K6_PRODUCT_RELEASE_WORKER_NOT_CONSUMED"

    assert activation._consume_kernel_auxiliary_workers(wait_seconds=0.0) == []
    activation.register_kernel_product_runtime(runtime=successor, symbols=successor.symbols)
    successor_state = activation._kernel_retry_states[successor.runtime_id]
    assert successor_state.lifecycle_generation > old_generation
    assert activation._kernel_callback_workers[successor.runtime_id].thread is not old_worker_thread
    activation.release_kernel_product_runtime(successor.runtime_id)


def test_shared_supervisor_watchdog_failure_is_loud_after_runtime_peers_continue() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Supervisor(_KernelLeaseSupervisor):
        def watchdog_tick(self) -> None:
            raise RuntimeError("shared feed watchdog failed")

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    runtimes = (_Runtime("runtime_shared_a"), _Runtime("runtime_shared_b"))
    activation.controller_factory = None
    activation.supervisor = _Supervisor()  # type: ignore[assignment]
    for runtime in runtimes:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    with pytest.raises(MiniQMTKernelProductSyncError) as caught:
        activation.watchdog_tick()
    assert [runtime.clock_attempts for runtime in runtimes] == [1, 1]
    failure = caught.value.context["ordered_failures"][0]
    assert failure["reason_code"] == "MINIQMT_SHARED_QUOTE_SUPERVISOR_WATCHDOG_FAILED"
    assert failure["broker_side_effect_state"] == "UNKNOWN"


def test_blocked_shared_supervisor_watchdog_does_not_starve_runtime_peers() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 11, 2, 0, tzinfo=UTC)
    activation._kernel_watchdog_peer_wait_seconds = 0.01
    entered = Event()
    release = Event()

    class _Supervisor(_KernelLeaseSupervisor):
        def watchdog_tick(self) -> None:
            entered.set()
            assert release.wait(timeout=5)

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    runtimes = (_Runtime("runtime_blocked_shared_a"), _Runtime("runtime_blocked_shared_b"))
    activation.controller_factory = None
    activation.supervisor = _Supervisor()  # type: ignore[assignment]
    for runtime in runtimes:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    first = activation.watchdog_tick()
    assert entered.wait(timeout=1)
    assert [runtime.clock_attempts for runtime in runtimes] == [1, 1]
    assert first["kernel_auxiliary_workers"][0]["operation"] == "SUPERVISOR_WATCHDOG"
    activation.watchdog_tick()
    assert [runtime.clock_attempts for runtime in runtimes] == [2, 2]

    release.set()
    for _attempt in range(100):
        activation.watchdog_tick()
        if activation._kernel_supervisor_watchdog_worker is None:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_supervisor_watchdog_worker is None


def test_blocked_release_unknown_reconciliation_does_not_starve_healthy_peer_tick() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_watchdog_peer_wait_seconds = 0.01
    activation._kernel_lifecycle_drain_timeout_seconds = 0.25
    entered = Event()
    release = Event()

    class _Runtime:
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str, *, blocked_callback: bool, trade_date: date) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.blocked_callback = blocked_callback
            self.trade_date = trade_date
            self.clock_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            if self.blocked_callback:
                entered.set()
                assert release.wait(timeout=5)

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    supervisor = _KernelLeaseSupervisor()
    unknown = _Runtime(
        "runtime_release_unknown_blocked",
        blocked_callback=True,
        trade_date=date(2026, 8, 11),
    )
    peer = _Runtime(
        "runtime_release_unknown_peer",
        blocked_callback=False,
        trade_date=date(2026, 8, 10),
    )
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (unknown, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    observation = SimpleNamespace(
        frame=SimpleNamespace(symbol="600000.SH", ingress_generation=1, ingress_sequence=1),
        market_data_id="market_data_release_unknown",
        context_id="context_release_unknown",
    )
    supervisor.sinks[f"k6d-kernel-v2:{unknown.runtime_id}"](observation, object())
    assert entered.wait(timeout=1)
    with activation._kernel_retry_condition:
        activation._kernel_retry_states[unknown.runtime_id].lifecycle_state = "RELEASE_UNKNOWN"

    started = monotonic_time.monotonic()
    health = activation.watchdog_tick()
    assert monotonic_time.monotonic() - started < 0.15
    assert peer.clock_attempts == 1
    assert unknown.runtime_id in activation._kernel_release_workers
    assert any(item["operation"] == "RELEASE_UNKNOWN_RECONCILIATION" for item in health["kernel_auxiliary_workers"])
    for _attempt in range(100):
        activation._consume_kernel_auxiliary_workers(wait_seconds=0.01)
        if activation.get_kernel_product_runtime(peer.runtime_id) is None:
            break
        monotonic_time.sleep(0.01)
    assert activation.get_kernel_product_runtime(peer.runtime_id) is None
    assert unknown.runtime_id in activation._kernel_release_workers

    release.set()
    for _attempt in range(100):
        activation.watchdog_tick()
        if unknown.runtime_id not in activation._kernel_release_workers:
            break
        monotonic_time.sleep(0.01)
    assert unknown.runtime_id not in activation._kernel_release_workers
    assert activation.get_kernel_product_runtime(unknown.runtime_id) is None


def test_blocked_release_worker_does_not_block_repeated_watchdog_health_or_peer_cadence() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_watchdog_peer_wait_seconds = 0.01
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 12, 1, 30, tzinfo=UTC)
    release_entered = Event()
    allow_release = Event()

    class _Runtime:
        trade_date = date(2026, 8, 12)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    class _BlockingReleaseSupervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self._health_lock = RLock()

        def release_consumer(self, *, consumer_id: str) -> bool:
            with self._health_lock:
                release_entered.set()
                assert allow_release.wait(timeout=5)
                return super().release_consumer(consumer_id=consumer_id)

        def health(self) -> dict[str, object]:
            with self._health_lock:
                return super().health()

    blocked = _Runtime("runtime_release_health_blocked")
    peer = _Runtime("runtime_release_health_peer")
    supervisor = _BlockingReleaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (blocked, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with activation._kernel_retry_condition:
        activation._kernel_retry_states[blocked.runtime_id].lifecycle_state = "RELEASE_UNKNOWN"

    receipts: list[dict[str, object]] = []
    failures: list[Exception] = []

    def run_watchdog() -> None:
        try:
            receipts.append(activation.watchdog_tick())
        except Exception as exc:  # noqa: BLE001 - thread carries the exact cadence failure.
            failures.append(exc)

    first = Thread(target=run_watchdog)
    second: Thread | None = None
    first.start()
    assert release_entered.wait(timeout=1)
    first.join(timeout=0.2)
    first_returned_while_release_blocked = not first.is_alive()
    if first_returned_while_release_blocked:
        second = Thread(target=run_watchdog)
        second.start()
        second.join(timeout=0.2)
    second_returned_while_release_blocked = second is not None and not second.is_alive()

    allow_release.set()
    first.join(timeout=5)
    if second is not None:
        second.join(timeout=5)

    assert first_returned_while_release_blocked is True
    assert second_returned_while_release_blocked is True
    assert failures == []
    assert peer.clock_attempts == 2
    assert len(receipts) == 2
    for receipt in receipts:
        assert receipt["ingress"]["status"] == "READBACK_DEFERRED"  # type: ignore[index]
        assert receipt["ingress"]["readback_current"] is False  # type: ignore[index]
        assert any(
            item["runtime_id"] == blocked.runtime_id and item["operation"] == "RELEASE_UNKNOWN_RECONCILIATION"
            for item in receipt["kernel_auxiliary_workers"]  # type: ignore[union-attr]
        )


@pytest.mark.parametrize(
    "drift",
    ("missing", "failed", "empty_lease_id", "generation", "symbols", "physical_subscription"),
)
def test_exact_physical_lease_owner_drift_degrades_only_owner_and_preserves_peer_cadence(
    drift: str,
) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_retry_clock_utc = lambda: datetime(2026, 8, 12, 1, 30, tzinfo=UTC)

    class _Runtime:
        trade_date = date(2026, 8, 12)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    broken = _Runtime(f"runtime_lease_drift_{drift}")
    peer = _Runtime(f"runtime_lease_peer_{drift}")
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (broken, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    consumer_id = f"k6d-kernel-v2:{broken.runtime_id}"
    if drift == "missing":
        supervisor.consumers.pop(consumer_id)
    elif drift == "failed":
        supervisor.consumers[consumer_id]["lease_status"] = "FAILED"
    elif drift == "empty_lease_id":
        supervisor.consumers[consumer_id]["lease_id"] = ""
    elif drift == "generation":
        supervisor.consumers[consumer_id]["lease_generation"] = (
            int(supervisor.consumers[consumer_id]["lease_generation"]) + 1
        )
    elif drift == "symbols":
        supervisor.consumers[consumer_id]["symbols"] = ["000001.SZ"]
    else:
        supervisor.consumers[consumer_id]["physical_subscription_id"] = None

    health_by_runtime = {
        item["runtime_id"]: item["ingress_retry"] for item in activation.health()["kernel_product_runtimes"]
    }
    assert health_by_runtime[broken.runtime_id]["state"] == "OWNER_DRIFT"
    assert health_by_runtime[broken.runtime_id]["reason_code"] == "MINIQMT_K6_PRODUCT_CONSUMER_LEASE_OWNER_DRIFT"
    assert health_by_runtime[peer.runtime_id]["state"] == "HEALTHY"
    with pytest.raises(MiniQMTKernelProductLifecycleError, match="exact active owner graph"):
        activation.get_kernel_product_runtime(broken.runtime_id)
    with pytest.raises(MiniQMTKernelProductLifecycleError, match="exact active owner graph"):
        activation.register_kernel_product_runtime(runtime=broken, symbols=broken.symbols)
    with pytest.raises(MiniQMTKernelProductSyncError) as failure:
        activation.watchdog_tick()
    assert any(
        item["runtime_id"] == broken.runtime_id
        and item["reason_code"] == "MINIQMT_K6_PRODUCT_CONSUMER_LEASE_OWNER_DRIFT"
        for item in failure.value.context["ordered_failures"]
    )
    assert broken.clock_attempts == 0
    assert peer.clock_attempts == 1


def test_kernel_product_schema_retry_uses_bounded_automatic_exponential_cadence() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    now_utc = [datetime(2026, 8, 11, 1, 30, tzinfo=UTC)]
    now_monotonic_ns = [1_000_000_000]
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_retry_cadence"
        binding_id = "binding_retry_cadence"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.quote_attempts = 0
            self.clock_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.quote_attempts += 1
            raise _SchemaFailure("schema conflict")

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def watchdog_tick() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    supervisor = _Supervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(QuoteContractError):
        supervisor.sink(object(), object())

    for delay_seconds in (60, 120, 240, 480, 960, 1920, 3600, 3600):
        now_utc[0] = datetime.fromtimestamp(now_utc[0].timestamp() + delay_seconds, tz=UTC)
        now_monotonic_ns[0] += delay_seconds * 1_000_000_000
        with pytest.raises(QuoteContractError) as caught:
            supervisor.sink(object(), object())
        assert caught.value.context["reason_code"] == "MINIQMT_KERNEL_EVENT_SCHEMA_CONSTRAINT_FAILED"
        assert caught.value.context["operation"] == "CALLBACK"
        assert caught.value.context["broker_side_effect_state"] == "UNKNOWN"
        activation.watchdog_tick()

    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert runtime.quote_attempts == 9
    assert runtime.clock_attempts == 8
    assert retry["attempt_count"] == 9
    assert retry["consecutive_failure_count"] == 9
    assert (datetime.fromisoformat(retry["next_retry_at_utc"]) - now_utc[0]).total_seconds() == 3600


def test_schema_retry_cadence_starts_when_the_failed_database_attempt_finishes() -> None:
    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    now_utc = [datetime(2026, 8, 11, 1, 30, tzinfo=UTC)]
    now_monotonic_ns = [1_000_000_000]

    class _Runtime:
        runtime_id = "runtime_failure_completion_cadence"
        binding_id = "binding_failure_completion_cadence"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)
        attempts = 0

        @classmethod
        def observe_b0_quote_v1(cls, *_values: object) -> None:
            cls.attempts += 1
            now_utc[0] += timedelta(seconds=90)
            now_monotonic_ns[0] += 90_000_000_000
            raise _SchemaFailure("slow schema conflict")

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]
    supervisor = _Supervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    with pytest.raises(QuoteContractError):
        supervisor.sink(object(), object())
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]
    assert retry["last_failure_at_utc"] == datetime(2026, 8, 11, 1, 31, 30, tzinfo=UTC).isoformat()
    assert retry["next_retry_at_utc"] == datetime(2026, 8, 11, 1, 32, 30, tzinfo=UTC).isoformat()
    assert isinstance(supervisor.sink(object(), object()), MiniQMTKernelProductIngressSuppression)
    assert runtime.attempts == 1


def test_non_schema_retry_failure_preserves_active_schema_backoff_until_same_path_succeeds() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    now_utc = [datetime(2026, 8, 11, 1, 30, tzinfo=UTC)]
    now_monotonic_ns = [1_000_000_000]
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_secondary_failure"
        binding_id = "binding_secondary_failure"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.failure: Exception | None = _SchemaFailure("schema conflict")
            self.advance_seconds = 0
            self.attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.attempts += 1
            if self.advance_seconds:
                now_utc[0] += timedelta(seconds=self.advance_seconds)
                now_monotonic_ns[0] += self.advance_seconds * 1_000_000_000
            if self.failure is not None:
                raise self.failure

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def watchdog_tick() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(QuoteContractError):
        supervisor.sink(object(), object())

    runtime.failure = RuntimeError("temporary callback read failure")
    runtime.advance_seconds = 90
    now_utc[0] += timedelta(seconds=60)
    now_monotonic_ns[0] += 60_000_000_000
    with pytest.raises(RuntimeError, match="temporary callback read failure"):
        supervisor.sink(object(), object())
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "RETRY_BACKOFF"
    assert retry["failure_operation"] == "CALLBACK"
    assert retry["operations"]["CALLBACK"]["secondary_failure_count"] == 1
    assert retry["operations"]["CALLBACK"]["last_secondary_failure"]["observed_at_utc"] == now_utc[0].isoformat()
    assert retry["next_retry_at_utc"] == (now_utc[0] + timedelta(seconds=60)).isoformat()
    assert retry["active_failure"]["constraint_name"] == "ck_miniqmt_event_source"
    attempts_after_failure = runtime.attempts
    immediate = supervisor.sink(object(), object())
    assert isinstance(immediate, MiniQMTKernelProductIngressSuppression)
    assert immediate.disposition == "RETRY_BACKOFF_SUPPRESSED"
    assert runtime.attempts == attempts_after_failure

    runtime.failure = None
    runtime.advance_seconds = 45
    now_utc[0] += timedelta(seconds=60)
    now_monotonic_ns[0] += 60_000_000_000
    supervisor.sink(object(), object())
    recovered = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert recovered["state"] == "HEALTHY"
    assert recovered["last_success_at_utc"] == now_utc[0].isoformat()


def test_watchdog_secondary_failure_backoff_and_success_use_attempt_completion_clocks() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    now_utc = [datetime(2026, 8, 11, 1, 30, tzinfo=UTC)]
    now_monotonic_ns = [1_000_000_000]
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_watchdog_secondary_failure"
        binding_id = "binding_watchdog_secondary_failure"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.failure: Exception | None = _SchemaFailure("watchdog schema conflict")
            self.advance_seconds = 0
            self.attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.attempts += 1
            if self.advance_seconds:
                now_utc[0] += timedelta(seconds=self.advance_seconds)
                now_monotonic_ns[0] += self.advance_seconds * 1_000_000_000
            if self.failure is not None:
                raise self.failure
            return ()

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        def __init__(self) -> None:
            self.sink: object | None = None

        def register_observation_sink(self, *, sink: object, **_values: object) -> None:
            self.sink = sink

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def watchdog_tick() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = _Supervisor()  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(MiniQMTKernelProductSyncError):
        activation.watchdog_tick()

    runtime.failure = RuntimeError("temporary watchdog read failure")
    runtime.advance_seconds = 90
    now_utc[0] += timedelta(seconds=60)
    now_monotonic_ns[0] += 60_000_000_000
    with pytest.raises(MiniQMTKernelProductSyncError):
        activation.watchdog_tick()
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["operations"]["WATCHDOG"]["last_secondary_failure"]["observed_at_utc"] == now_utc[0].isoformat()
    assert retry["next_retry_at_utc"] == (now_utc[0] + timedelta(seconds=60)).isoformat()
    attempts_after_failure = runtime.attempts
    with pytest.raises(MiniQMTKernelProductSyncError) as suppressed:
        activation.watchdog_tick()
    suppression = suppressed.value.context["ordered_failures"][0]["suppression"]
    assert suppression["schema_version"] == "miniqmt_kernel_product_watchdog_suppression_v1"
    assert "pending_identity_sha256" not in suppression
    assert runtime.attempts == attempts_after_failure

    runtime.failure = None
    runtime.advance_seconds = 45
    now_utc[0] += timedelta(seconds=60)
    now_monotonic_ns[0] += 60_000_000_000
    activation.watchdog_tick()
    recovered = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert recovered["state"] == "HEALTHY"
    assert recovered["last_success_at_utc"] == now_utc[0].isoformat()


def test_fresh_process_retry_state_resets_explicitly_and_starts_one_new_bounded_series() -> None:
    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_fresh_process_reset"
        binding_id = "binding_fresh_process_reset"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            raise _SchemaFailure("schema conflict")

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        def unregister_observation_sink(self, **_values: object) -> bool:
            self.sink = None
            return True

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            return True

        @staticmethod
        def shutdown() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    def build(now: datetime, monotonic_ns: int):  # type: ignore[no-untyped-def]
        activation = build_miniqmt_quote_ingress_activation_from_env(
            environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
            schema_gate_reader=lambda: "applied_and_verified",
        )
        supervisor = _Supervisor()
        runtime = _Runtime()
        activation._kernel_retry_clock_utc = lambda: now
        activation._kernel_retry_monotonic_ns = lambda: monotonic_ns
        activation.controller_factory = None
        activation.supervisor = supervisor  # type: ignore[assignment]
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
        return activation, supervisor

    first, first_supervisor = build(datetime(2026, 8, 11, 1, 30, tzinfo=UTC), 1_000_000_000)
    with pytest.raises(QuoteContractError):
        first_supervisor.sink(object(), object())
    first_retry = first.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert first_retry["attempt_count"] == 1
    first.shutdown()

    second, second_supervisor = build(datetime(2026, 8, 11, 1, 31, tzinfo=UTC), 61_000_000_000)
    clean = second.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert clean["state"] == "HEALTHY"
    assert clean["attempt_count"] == 0
    assert clean["automatic_retry"] is False
    with pytest.raises(QuoteContractError):
        second_supervisor.sink(object(), object())
    reset_retry = second.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert reset_retry["attempt_count"] == 1
    assert (
        datetime.fromisoformat(reset_retry["next_retry_at_utc"]) - datetime(2026, 8, 11, 1, 31, tzinfo=UTC)
    ).total_seconds() == 60


def test_fresh_process_imports_current_activation_source_with_empty_process_local_retry_state() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    source_path = Path(activation_module.__file__).resolve()
    expected_source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    code = """
import hashlib
import json
import os
from pathlib import Path
import backend.services.simulation_runtime.miniqmt_quote_activation as module

activation = module.build_miniqmt_quote_ingress_activation_from_env(
    environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
    schema_gate_reader=lambda: "pending",
)
health = activation.health()
source_path = Path(module.__file__).resolve()
print(json.dumps({
    "pid": os.getpid(),
    "source_path": str(source_path),
    "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
    "kernel_product_runtimes": health["kernel_product_runtimes"],
    "status": health["status"],
}, sort_keys=True))
"""
    child_env = dict(os.environ)
    child_env["MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED"] = "false"
    child_env["ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER"] = "false"
    child_env["SIMULATION_RUNTIME_CONTEXT_PROVIDER"] = ""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=repo_root,
        check=True,
        capture_output=True,
        env=child_env,
        text=True,
        timeout=30,
    )
    receipt = json.loads(completed.stdout.strip())
    assert receipt["pid"] != 0
    assert Path(receipt["source_path"]).resolve() == source_path
    assert receipt["source_sha256"] == expected_source_sha256
    assert receipt["kernel_product_runtimes"] == []
    assert receipt["status"] == "DRAINING"


def test_order_and_trade_callback_entrypoints_do_not_claim_unimplemented_automatic_replay() -> None:
    class _Diag:
        constraint_name = "ck_miniqmt_k2_event_composite"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_direct_callbacks"
        binding_id = "binding_direct_callbacks"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def ingest_order_callback_v1(**_values: object) -> None:
            raise _SchemaFailure("order callback schema conflict")

        @staticmethod
        def ingest_trade_callback_v1(**_values: object) -> str:
            return "trade_event"

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, **values: object) -> None:
            self.sink = values["sink"]

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = _Supervisor()  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(_SchemaFailure, match="order callback schema conflict"):
        activation.ingest_kernel_order_callback_v1(
            runtime_id=runtime.runtime_id,
            broker_order_id="broker_order",
            raw_payload={"status": 48},
            observed_at=datetime(2026, 8, 11, 1, 30, tzinfo=UTC),
        )
    with pytest.raises(_SchemaFailure, match="order callback schema conflict"):
        activation.ingest_kernel_order_callback_v1(
            runtime_id=runtime.runtime_id,
            broker_order_id="broker_order",
            raw_payload={"status": 48},
            observed_at=datetime(2026, 8, 11, 1, 30, tzinfo=UTC),
        )
    assert (
        activation.ingest_kernel_trade_callback_v1(
            runtime_id=runtime.runtime_id,
            broker_order_id="broker_order",
            trade_quantity=10,
            trade_price_decimal="10.25",
            cumulative_quantity=10,
            raw_payload={"trade_id": "trade"},
            observed_at=datetime(2026, 8, 11, 1, 31, tzinfo=UTC),
        )
        == "trade_event"
    )
    operations = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]
    assert set(operations) == {"CALLBACK", "WATCHDOG"}
    assert all(operation["automatic_retry"] is False for operation in operations.values())


def test_watchdog_releases_successful_prior_day_kernel_runtime_after_final_clock_tick() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    operations: list[tuple[str, str]] = []

    class _Supervisor(_KernelLeaseSupervisor):
        def release_consumer(self, *, consumer_id: str) -> bool:
            operations.append(("release", consumer_id))
            return super().release_consumer(consumer_id=consumer_id)

        def unregister_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> bool:
            operations.append(("unregister", consumer_id))
            return super().unregister_observation_sink(
                consumer_id=consumer_id,
                symbols=symbols,
                sink=sink,
            )

    class _Runtime:
        runtime_id = "runtime_prior_day"
        binding_id = "binding_prior_day"
        trade_date = date(2026, 8, 3)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, *, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            assert observed_at.tzinfo is not None
            assert monotonic_ns > 0
            operations.append(("tick", self.runtime_id))
            return ()

    activation.controller_factory = None
    activation.supervisor = _Supervisor()  # type: ignore[assignment]
    runtime = _Runtime()
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    activation.watchdog_tick()
    assert operations == [
        ("tick", "runtime_prior_day"),
        ("unregister", "k6d-kernel-v2:runtime_prior_day"),
        ("release", "k6d-kernel-v2:runtime_prior_day"),
    ]
    assert activation._kernel_product_runtimes == {}


def test_kernel_product_registry_is_strict_and_preserves_primary_and_rollback_failures() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class Runtime:
        runtime_id = "runtime_registry"
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values):
            return None

    runtime = Runtime()
    with pytest.raises(ValueError, match="exact unique symbol tuple"):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=("600000.SH", "600000.SH"))
    with pytest.raises(RuntimeError, match="frozen symbol owner"):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=("000001.SZ",))

    class RegisterSupervisor(_KernelLeaseSupervisor):
        def acquire_consumer(self, **_values: object) -> None:
            raise RuntimeError("primary acquire failure")

        def unregister_observation_sink(self, **_values: object) -> bool:
            raise RuntimeError("rollback unregister failure")

    activation.controller_factory = None
    activation.supervisor = RegisterSupervisor()  # type: ignore[assignment]
    with pytest.raises(MiniQMTKernelProductRegistryRollbackError) as register_failure:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert register_failure.value.context["operation"] == "REGISTER_ACQUIRE_CONSUMER"
    assert register_failure.value.context["primary_failure"]["exception_message"] == "primary acquire failure"
    assert register_failure.value.context["rollback_failure"]["exception_message"] == "rollback unregister failure"
    retained = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retained["lifecycle_state"] == "RELEASE_UNKNOWN"
    assert activation._kernel_guarded_sinks[runtime.runtime_id] is not None

    class ReleaseSupervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.unregister_attempts = 0

        def unregister_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> bool:
            self.unregister_attempts += 1
            if self.unregister_attempts == 1:
                self.sinks[consumer_id] = object()
                raise RuntimeError("primary unregister failure")
            return super().unregister_observation_sink(
                consumer_id=consumer_id,
                symbols=symbols,
                sink=sink,
            )

    release_supervisor = ReleaseSupervisor()
    release_activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    release_activation.controller_factory = None
    release_activation.supervisor = release_supervisor  # type: ignore[assignment]
    release_activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(MiniQMTKernelProductRegistryRollbackError) as release_failure:
        release_activation.release_kernel_product_runtime(runtime.runtime_id)
    assert release_failure.value.context["operation"] == "RELEASE_UNREGISTER_SINK"
    assert release_failure.value.context["primary_failure"]["exception_message"] == "primary unregister failure"
    assert release_failure.value.context["rollback_failure"]["exception_message"] == (
        "quote supervisor observation-sink owner changed during release rollback"
    )
    retry = release_activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["lifecycle_state"] == "RELEASE_UNKNOWN"
    consumer_id = f"k6d-kernel-v2:{runtime.runtime_id}"
    release_supervisor.sinks[consumer_id] = release_activation._kernel_guarded_sinks[runtime.runtime_id]
    release_activation.watchdog_tick()
    assert release_supervisor.unregister_attempts == 2
    assert release_activation.health()["kernel_product_runtimes"] == []


def test_registration_rollback_drain_timeout_reconciles_automatically_after_callback_exit() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_lifecycle_drain_timeout_seconds = 0.01
    entered = Event()
    release = Event()

    class _Runtime:
        runtime_id = "runtime_registration_drain_timeout"
        binding_id = "binding_registration_drain_timeout"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            entered.set()
            assert release.wait(timeout=5)

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    class _Supervisor(_KernelLeaseSupervisor):
        callback_thread: Thread | None = None

        def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
            assert symbols == ["600000.SH"]
            sink = self.sinks[consumer_id]
            assert callable(sink)
            self.callback_thread = Thread(target=lambda: sink(object(), object()))
            self.callback_thread.start()
            assert entered.wait(timeout=5)
            raise RuntimeError("acquire failed after callback publication")

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    with pytest.raises(MiniQMTKernelProductRegistryRollbackError) as caught:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert caught.value.context["operation"] == "REGISTER_ROLLBACK_DRAIN"
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["lifecycle_state"] == "RELEASE_UNKNOWN"

    release.set()
    assert supervisor.callback_thread is not None
    supervisor.callback_thread.join(timeout=5)
    assert not supervisor.callback_thread.is_alive()
    for _attempt in range(100):
        if not activation._kernel_product_in_flight:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_product_in_flight == {}
    activation.watchdog_tick()
    assert activation.health()["kernel_product_runtimes"] == []


def test_registration_failure_releases_explicitly_retained_acquire_rollback_owner() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _RetainedAcquireFailure(RuntimeError):
        context = {
            "consumer_lease_retained": True,
            "release_outcome": "UNKNOWN",
        }

    class _Supervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.release_count = 0

        def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
            super().acquire_consumer(consumer_id=consumer_id, symbols=symbols)
            raise _RetainedAcquireFailure("non-exact acquire cleanup remains unknown")

        def release_consumer(self, *, consumer_id: str) -> bool:
            self.release_count += 1
            return super().release_consumer(consumer_id=consumer_id)

    class _Runtime:
        runtime_id = "runtime_retained_acquire_rollback"
        binding_id = "binding_retained_acquire_rollback"
        trade_date = date(2026, 8, 12)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    supervisor = _Supervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]

    with pytest.raises(_RetainedAcquireFailure, match="cleanup remains unknown"):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    assert supervisor.release_count == 1
    assert supervisor.consumers == {}
    assert supervisor.sinks == {}
    assert activation.health()["kernel_product_runtimes"] == []


def test_kernel_product_idempotent_registration_requires_exact_active_sink_lease_and_operations() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_idempotent_registration"
        binding_id = "binding_idempotent_registration"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    runtime = _Runtime()
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    assert activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols) is runtime
    assert activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols) is runtime
    assert len(supervisor.sinks) == 1
    assert len(supervisor.consumers) == 1

    state = activation._kernel_retry_states[runtime.runtime_id]
    state.lifecycle_state = "RELEASE_UNKNOWN"
    with pytest.raises(MiniQMTKernelProductLifecycleError) as unknown:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert unknown.value.reason_code == "MINIQMT_K6_PRODUCT_REGISTRATION_OWNER_INCOMPLETE"

    state.lifecycle_state = "ACTIVE"
    state.operations.pop("CALLBACK")
    with pytest.raises(MiniQMTKernelProductLifecycleError) as operation_drift:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert operation_drift.value.reason_code == "MINIQMT_K6_PRODUCT_REGISTRATION_OWNER_INCOMPLETE"


@pytest.mark.parametrize("replacement", [None, lambda *_values: None])
def test_kernel_product_readback_rejects_missing_or_replaced_observation_sink_owner(
    replacement: object | None,
) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_sink_owner_drift"
        binding_id = "binding_sink_owner_drift"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    runtime = _Runtime()
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    consumer_id = f"k6d-kernel-v2:{runtime.runtime_id}"
    if replacement is None:
        supervisor.sinks.pop(consumer_id)
    else:
        supervisor.sinks[consumer_id] = replacement

    health = activation.health()
    assert health["status"] == "DEGRADED"
    retry = health["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "OWNER_DRIFT"
    assert retry["reason_code"] == "MINIQMT_K6_PRODUCT_SINK_OWNER_DRIFT"
    assert retry["manual_ack_required"] is False
    assert retry["business_gate"] is False
    with pytest.raises(MiniQMTKernelProductLifecycleError) as registration:
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert registration.value.reason_code == "MINIQMT_K6_PRODUCT_REGISTRATION_SINK_OWNER_MISSING"
    with pytest.raises(MiniQMTKernelProductLifecycleError) as readback:
        activation.get_kernel_product_runtime(runtime.runtime_id)
    assert readback.value.reason_code == "MINIQMT_K6_PRODUCT_RUNTIME_READBACK_SINK_OWNER_MISSING"


@pytest.mark.parametrize("drift", ["sink", "actor"])
def test_watchdog_fails_loud_for_exact_owner_drift_without_starving_peer(drift: str) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    fixed_now = datetime(2026, 8, 11, 2, 0, tzinfo=UTC)
    activation._kernel_retry_clock_utc = lambda: fixed_now

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.clock_attempts = 0

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        def scheduler_tick_v1(self, **_values: object) -> tuple[str, ...]:
            self.clock_attempts += 1
            return ()

    supervisor = _KernelLeaseSupervisor()
    broken = _Runtime(f"runtime_owner_drift_{drift}")
    peer = _Runtime(f"runtime_owner_peer_{drift}")
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (broken, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    consumer_id = f"k6d-kernel-v2:{broken.runtime_id}"
    guarded_sink = activation._kernel_guarded_sinks[broken.runtime_id]
    original_worker_sink: object | None = None
    if drift == "sink":
        supervisor.sinks.pop(consumer_id)
    else:
        state = activation._kernel_retry_states[broken.runtime_id]
        original_worker_sink = activation._kernel_callback_workers[broken.runtime_id].sink
        activation._stop_kernel_callback_worker(
            runtime_id=broken.runtime_id,
            lifecycle_generation=state.lifecycle_generation,
            operation="TEST_OWNER_DRIFT",
        )

    with pytest.raises(MiniQMTKernelProductSyncError) as caught:
        activation.watchdog_tick()
    assert broken.clock_attempts == 0
    assert peer.clock_attempts == 1
    failures = caught.value.context["ordered_failures"]
    assert len(failures) == 1
    assert failures[0]["runtime_id"] == broken.runtime_id
    assert failures[0]["binding_id"] == broken.binding_id
    assert failures[0]["reason_code"] == (
        "MINIQMT_K6_PRODUCT_SINK_OWNER_DRIFT" if drift == "sink" else "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT"
    )
    assert failures[0]["broker_side_effect_state"] == "UNKNOWN"

    if drift == "sink":
        supervisor.sinks[consumer_id] = guarded_sink
    else:
        assert callable(original_worker_sink)
        state = activation._kernel_retry_states[broken.runtime_id]
        activation._start_kernel_callback_worker(
            runtime_id=broken.runtime_id,
            runtime=broken,
            lifecycle_generation=state.lifecycle_generation,
            sink=original_worker_sink,
            started_at_utc=fixed_now,
        )
    activation.release_kernel_product_runtime(broken.runtime_id)
    activation.release_kernel_product_runtime(peer.runtime_id)


@pytest.mark.parametrize("runtime_id", [123, " runtime_with_space", "runtime_with_space ", ""])
def test_kernel_product_registration_rejects_non_exact_runtime_identity(runtime_id: object) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    runtime = _Runtime()
    runtime.runtime_id = runtime_id  # type: ignore[attr-defined]
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]

    with pytest.raises(TypeError, match="exact canonical string identity"):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert activation._kernel_product_runtimes == {}
    assert activation._kernel_retry_states == {}
    assert activation._kernel_callback_workers == {}
    assert supervisor.sinks == {}
    assert supervisor.consumers == {}


def test_retry_pending_replacement_records_exact_drop_reason_count_and_identity() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    now_utc = datetime(2026, 8, 11, 1, 30, tzinfo=UTC)
    activation._kernel_retry_clock_utc = lambda: now_utc
    activation._kernel_retry_monotonic_ns = lambda: 1_000_000_000

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _SchemaFailure(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_pending_drop_identity"
        binding_id = "binding_pending_drop_identity"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            raise _SchemaFailure("schema conflict")

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    def observation(sequence: int) -> SimpleNamespace:
        return SimpleNamespace(
            frame=SimpleNamespace(symbol="600000.SH", ingress_generation=4, ingress_sequence=sequence),
            market_data_id=f"market_data_pending_drop_{sequence}",
            context_id="context_pending_drop",
        )

    supervisor = _KernelLeaseSupervisor()
    runtime = _Runtime()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    sink = supervisor.sinks[f"k6d-kernel-v2:{runtime.runtime_id}"]
    with pytest.raises(QuoteContractError):
        sink(observation(1), object())  # type: ignore[operator]
    for sequence in (2, 3, 2):
        suppressed = sink(observation(sequence), object())  # type: ignore[operator]
        assert isinstance(suppressed, MiniQMTKernelProductIngressSuppression)

    operation = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["operations"]["CALLBACK"]
    assert operation["pending"][0]["ingress_sequence"] == 3
    assert operation["pending_drop_count_by_reason"] == {
        "PENDING_COALESCED_SUPERSEDED": 2,
        "PENDING_ORDERING_REJECTED": 1,
    }
    assert operation["last_pending_drop"] == {
        "reason": "PENDING_ORDERING_REJECTED",
        "symbol": "600000.SH",
        "market_data_id": "market_data_pending_drop_2",
        "ingress_generation": 4,
        "ingress_sequence": 2,
        "pending_identity_sha256": suppressed.pending_identity_sha256,
        "replacement_market_data_id": "market_data_pending_drop_3",
        "replacement_ingress_generation": 4,
        "replacement_ingress_sequence": 3,
        "replacement_pending_identity_sha256": operation["pending"][0]["pending_identity_sha256"],
    }
    activation.release_kernel_product_runtime(runtime.runtime_id)


@pytest.mark.parametrize(
    ("failure_stage", "expected_reason"),
    (
        ("unregister", "MINIQMT_K6_PRODUCT_RUNTIME_UNREGISTER_NOT_EXACT"),
        ("release", "MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_NOT_EXACT"),
    ),
)
def test_release_requires_exact_true_and_restores_active_sink(
    failure_stage: str,
    expected_reason: str,
) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = f"runtime_release_{failure_stage}"
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    class _Supervisor(_KernelLeaseSupervisor):
        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            super().register_observation_sink(consumer_id=consumer_id, symbols=symbols, sink=sink)

        def unregister_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> bool:
            super().unregister_observation_sink(consumer_id=consumer_id, symbols=symbols, sink=sink)
            return failure_stage != "unregister"

        def release_consumer(self, *, consumer_id: str) -> bool:
            if failure_stage == "release":
                return False
            return super().release_consumer(consumer_id=consumer_id)

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(MiniQMTKernelProductLifecycleError) as caught:
        activation.release_kernel_product_runtime(runtime.runtime_id)
    assert caught.value.reason_code == expected_reason
    assert (
        supervisor.get_observation_sink(
            consumer_id=f"k6d-kernel-v2:{runtime.runtime_id}",
            symbols=runtime.symbols,
        )
        is not None
    )
    assert activation.get_kernel_product_runtime(runtime.runtime_id) is runtime
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["lifecycle_state"] == "ACTIVE"


def test_release_active_exception_restores_exact_sink_without_duplicate_consumer_acquire() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_release_exception"
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    class _ActiveReleaseError(RuntimeError):
        context = {"release_outcome": "ACTIVE"}

    class _Supervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.acquire_count = 0

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            super().register_observation_sink(consumer_id=consumer_id, symbols=symbols, sink=sink)

        def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
            self.acquire_count += 1
            if self.acquire_count > 1:
                raise AssertionError("duplicate supervisor consumer acquire")
            super().acquire_consumer(consumer_id=consumer_id, symbols=symbols)

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            raise _ActiveReleaseError("release remained active")

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(_ActiveReleaseError, match="release remained active"):
        activation.release_kernel_product_runtime(runtime.runtime_id)
    assert supervisor.acquire_count == 1
    assert (
        supervisor.get_observation_sink(
            consumer_id=f"k6d-kernel-v2:{runtime.runtime_id}",
            symbols=runtime.symbols,
        )
        is not None
    )
    assert activation.get_kernel_product_runtime(runtime.runtime_id) is runtime
    assert activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["lifecycle_state"] == "ACTIVE"


def test_release_unknown_exception_keeps_sink_fenced_and_watchdog_reconciles_automatically() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_release_unknown"
        binding_id = "binding_release_unknown"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def scheduler_tick_v1(*, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
            raise AssertionError((observed_at, monotonic_ns))

    class _Supervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.release_attempts = 0

        def release_consumer(self, *, consumer_id: str) -> bool:
            self.release_attempts += 1
            if self.release_attempts == 1:
                raise RuntimeError("release outcome unknown")
            return super().release_consumer(consumer_id=consumer_id)

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    with pytest.raises(RuntimeError, match="release outcome unknown"):
        activation.release_kernel_product_runtime(runtime.runtime_id)

    consumer_id = f"k6d-kernel-v2:{runtime.runtime_id}"
    assert supervisor.get_observation_sink(consumer_id=consumer_id, symbols=runtime.symbols) is None
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["lifecycle_state"] == "RELEASE_UNKNOWN"
    assert retry["automatic_retry"] is True
    with pytest.raises(MiniQMTKernelProductLifecycleError) as readback:
        activation.get_kernel_product_runtime(runtime.runtime_id)
    assert readback.value.reason_code == "MINIQMT_K6_PRODUCT_RUNTIME_READBACK_OWNER_INCOMPLETE"

    activation.watchdog_tick()
    assert supervisor.release_attempts == 2
    assert activation.health()["kernel_product_runtimes"] == []


def test_stale_callback_carrier_retains_its_closed_lifecycle_generation_after_same_id_reregister() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_same_id_generation"
        binding_id = "binding_same_id_generation"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        def __init__(self) -> None:
            self.sinks: list[object] = []
            self.active_sink: object | None = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sinks.append(sink)
            self.active_sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        def unregister_observation_sink(self, **_values: object) -> bool:
            self.active_sink = None
            return True

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            return True

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    first_runtime = _Runtime()
    activation.register_kernel_product_runtime(runtime=first_runtime, symbols=first_runtime.symbols)
    stale_sink = supervisor.sinks[0]
    activation.release_kernel_product_runtime(first_runtime.runtime_id)
    second_runtime = _Runtime()
    activation.register_kernel_product_runtime(runtime=second_runtime, symbols=second_runtime.symbols)

    frame = SimpleNamespace(symbol="600000.SH", ingress_generation=9, ingress_sequence=1)
    observation = SimpleNamespace(
        frame=frame,
        quote=frame,
        market_data_id="market_data_stale_generation",
        context_id="context_stale_generation",
    )
    late = stale_sink(observation, object())  # type: ignore[operator]
    current_generation = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["lifecycle_generation"]
    assert isinstance(late, MiniQMTKernelProductIngressSuppression)
    assert late.disposition == "LIFECYCLE_FENCED"
    assert late.lifecycle_generation == 1
    assert current_generation == 2
    assert late.lifecycle_generation < current_generation
    assert late.consumer_id == "k6d-kernel-v2:runtime_same_id_generation"
    assert late.ingress_generation == 9
    assert late.ingress_sequence == 1


def test_release_fences_new_callbacks_and_waits_for_exact_in_flight_generation() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    entered = Event()
    allow_completion = Event()
    unregistered = Event()
    release_errors: list[Exception] = []

    class _Runtime:
        runtime_id = "runtime_release_fence"
        binding_id = "binding_release_fence"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self) -> None:
            self.calls = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.calls += 1
            entered.set()
            assert allow_completion.wait(timeout=5)

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def unregister_observation_sink(**_values: object) -> bool:
            unregistered.set()
            return True

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            return True

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    guarded_sink = supervisor.sink
    assert callable(guarded_sink)
    callback_thread = Thread(target=lambda: guarded_sink(object(), object()))
    callback_thread.start()
    assert entered.wait(timeout=5)

    def release_runtime() -> None:
        try:
            activation.release_kernel_product_runtime(runtime.runtime_id)
        except Exception as exc:  # noqa: BLE001 - thread transports exact release failure.
            release_errors.append(exc)

    release_thread = Thread(target=release_runtime)
    release_thread.start()
    assert unregistered.wait(timeout=5)
    assert release_thread.is_alive()
    late = supervisor.sink(object(), object())
    assert isinstance(late, MiniQMTKernelProductIngressSuppression)
    assert late.disposition == "LIFECYCLE_FENCED"
    allow_completion.set()
    callback_thread.join(timeout=5)
    release_thread.join(timeout=5)

    assert not callback_thread.is_alive() and not release_thread.is_alive()
    assert release_errors == []
    assert runtime.calls == 1
    assert activation.get_kernel_product_runtime(runtime.runtime_id) is None


def test_shutdown_fences_new_callbacks_and_waits_for_in_flight_attempt() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    entered = Event()
    allow_completion = Event()
    supervisor_stopped = Event()
    sink_unregistered = Event()
    shutdown_errors: list[Exception] = []

    class _Runtime:
        runtime_id = "runtime_shutdown_fence"
        binding_id = "binding_shutdown_fence"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            entered.set()
            assert allow_completion.wait(timeout=5)

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        def unregister_observation_sink(self, **_values: object) -> bool:
            self.sink = None
            sink_unregistered.set()
            return True

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            return True

        @staticmethod
        def shutdown() -> None:
            supervisor_stopped.set()

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    stale_sink = supervisor.sink
    callback_thread = Thread(target=lambda: stale_sink(object(), object()))
    callback_thread.start()
    assert entered.wait(timeout=5)

    def shutdown() -> None:
        try:
            activation.shutdown()
        except Exception as exc:  # noqa: BLE001 - the thread result is asserted below.
            shutdown_errors.append(exc)

    shutdown_thread = Thread(target=shutdown)
    shutdown_thread.start()
    assert sink_unregistered.wait(timeout=5)
    assert not supervisor_stopped.is_set()
    assert shutdown_thread.is_alive()
    late = stale_sink(object(), object())
    assert isinstance(late, MiniQMTKernelProductIngressSuppression)
    assert late.disposition == "LIFECYCLE_FENCED"
    allow_completion.set()
    callback_thread.join(timeout=5)
    shutdown_thread.join(timeout=5)

    assert not callback_thread.is_alive() and not shutdown_thread.is_alive()
    assert shutdown_errors == []
    assert supervisor_stopped.is_set()
    assert activation.health()["status"] == "STOPPED"
    assert activation.health()["kernel_product_runtimes"] == []


def test_release_drain_timeout_is_loud_and_restores_active_sink_without_releasing_lease() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_lifecycle_drain_timeout_seconds = 0.01
    entered = Event()
    allow_completion = Event()

    class _Runtime:
        runtime_id = "runtime_release_timeout"
        binding_id = "binding_release_timeout"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            entered.set()
            assert allow_completion.wait(timeout=5)

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def __init__(self) -> None:
            self.register_count = 0
            self.release_count = 0
            self.consumers: dict[str, dict[str, object]] = {}

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.register_count += 1
            self.sink = sink

        def get_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...]) -> object | None:  # noqa: ARG002
            return self.sink

        def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
            self.consumers[consumer_id] = {
                "lease_id": f"lease:{consumer_id}",
                "lease_status": "ACTIVE",
                "symbols": symbols,
            }

        def unregister_observation_sink(self, **_values: object) -> bool:
            self.sink = None
            return True

        def release_consumer(self, **_values: object) -> bool:
            self.release_count += 1
            return True

        def health(self) -> dict[str, object]:
            return {"status": "READY", "consumers": dict(self.consumers)}

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    callback_thread = Thread(target=lambda: supervisor.sink(object(), object()))
    callback_thread.start()
    assert entered.wait(timeout=5)

    with pytest.raises(MiniQMTKernelProductLifecycleError) as timeout:
        activation.release_kernel_product_runtime(runtime.runtime_id)
    assert timeout.value.reason_code == "MINIQMT_K6_PRODUCT_RUNTIME_DRAIN_TIMEOUT"
    assert supervisor.register_count == 2
    assert supervisor.release_count == 0
    assert activation.get_kernel_product_runtime(runtime.runtime_id) is runtime
    assert activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["lifecycle_state"] == "ACTIVE"

    allow_completion.set()
    callback_thread.join(timeout=5)
    assert not callback_thread.is_alive()


def test_shutdown_drain_timeout_is_loud_and_second_attempt_finishes_after_in_flight_exit() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    activation._kernel_lifecycle_drain_timeout_seconds = 0.01
    entered = Event()
    allow_completion = Event()

    class _Runtime:
        runtime_id = "runtime_shutdown_timeout"
        binding_id = "binding_shutdown_timeout"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            entered.set()
            assert allow_completion.wait(timeout=5)

    class _Supervisor(_StaticExactLeaseOwnerMixin):
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        def unregister_observation_sink(self, **_values: object) -> bool:
            self.sink = None
            return True

        def get_observation_sink(self, **_values: object) -> object | None:
            return self.sink

        @staticmethod
        def release_consumer(**_values: object) -> bool:
            return True

        @staticmethod
        def shutdown() -> None:
            return None

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    guarded_sink = supervisor.sink
    assert callable(guarded_sink)
    callback_thread = Thread(target=lambda: guarded_sink(object(), object()))
    callback_thread.start()
    assert entered.wait(timeout=5)

    with pytest.raises(MiniQMTKernelProductLifecycleError) as timeout:
        activation.shutdown()
    assert timeout.value.reason_code == "MINIQMT_K6_PRODUCT_SHUTDOWN_RUNTIME_RELEASE_FAILED"
    assert timeout.value.context["ordered_failures"][0]["reason_code"] == (
        "MINIQMT_K6_PRODUCT_RUNTIME_DRAIN_TIMEOUT"
    ), timeout.value.context
    assert activation.health()["status"] == "SHUTDOWN_UNKNOWN"
    assert len(activation.health()["kernel_product_runtimes"]) == 1
    assert activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["lifecycle_state"] == "ACTIVE"

    allow_completion.set()
    callback_thread.join(timeout=5)
    assert not callback_thread.is_alive()
    for _attempt in range(100):
        if not activation._kernel_product_in_flight:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_product_in_flight == {}
    assert activation.shutdown()["kernel_product_runtimes"] == []


def test_shutdown_release_unknown_is_fenced_and_only_stops_after_exact_retry_readback() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_shutdown_release_unknown"
        binding_id = "binding_shutdown_release_unknown"
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    class _Supervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.release_attempts = 0

        def release_consumer(self, *, consumer_id: str) -> bool:
            self.release_attempts += 1
            if self.release_attempts == 1:
                raise RuntimeError("physical lease release outcome unknown")
            return super().release_consumer(consumer_id=consumer_id)

        @staticmethod
        def shutdown() -> None:
            return None

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    with pytest.raises(MiniQMTKernelProductLifecycleError) as first:
        activation.shutdown()
    assert first.value.reason_code == "MINIQMT_K6_PRODUCT_SHUTDOWN_RUNTIME_RELEASE_FAILED"
    assert activation._shutdown is False
    health = activation.health()
    assert health["status"] == "SHUTDOWN_UNKNOWN"
    retry = health["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["lifecycle_state"] == "RELEASE_UNKNOWN"
    assert retry["broker_side_effect_state"] == "UNKNOWN"
    assert activation._kernel_callback_workers[runtime.runtime_id].thread.is_alive()

    closed = activation.shutdown()
    assert closed["status"] == "STOPPED"
    assert closed["kernel_product_runtimes"] == []
    assert activation._kernel_product_runtimes == {}
    assert activation._kernel_retry_states == {}
    assert activation._kernel_guarded_sinks == {}
    assert activation._kernel_callback_workers == {}


def test_shutdown_fences_every_runtime_before_waiting_for_a_blocked_peer() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    entered = Event()
    release = Event()

    class _Runtime:
        trade_date = date(2026, 8, 11)
        symbols = ("600000.SH",)

        def __init__(self, runtime_id: str, *, blocked: bool) -> None:
            self.runtime_id = runtime_id
            self.binding_id = f"binding_{runtime_id}"
            self.blocked = blocked
            self.quote_attempts = 0

        def observe_b0_quote_v1(self, *_values: object) -> None:
            self.quote_attempts += 1
            if self.blocked:
                entered.set()
                assert release.wait(timeout=5)

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    class _Supervisor(_KernelLeaseSupervisor):
        @staticmethod
        def shutdown() -> None:
            return None

    supervisor = _Supervisor()
    blocked = _Runtime("runtime_a_shutdown_blocked", blocked=True)
    peer = _Runtime("runtime_b_shutdown_peer", blocked=False)
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    for runtime in (blocked, peer):
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    blocked_sink = supervisor.sinks[f"k6d-kernel-v2:{blocked.runtime_id}"]
    peer_sink = supervisor.sinks[f"k6d-kernel-v2:{peer.runtime_id}"]
    callback_thread = Thread(target=lambda: blocked_sink(object(), object()))
    callback_thread.start()
    assert entered.wait(timeout=5)

    shutdown_errors: list[Exception] = []

    def run_shutdown() -> None:
        try:
            activation.shutdown()
        except Exception as exc:  # noqa: BLE001 - thread transports the exact lifecycle failure.
            shutdown_errors.append(exc)

    shutdown_thread = Thread(target=run_shutdown)
    shutdown_thread.start()
    for _attempt in range(100):
        if activation._shutdown_requested:
            break
        monotonic_time.sleep(0.01)
    assert activation._shutdown_requested is True
    late = peer_sink(object(), object())
    assert isinstance(late, MiniQMTKernelProductIngressSuppression)
    assert late.disposition == "LIFECYCLE_FENCED"
    assert peer.quote_attempts == 0

    release.set()
    callback_thread.join(timeout=5)
    shutdown_thread.join(timeout=5)
    assert not callback_thread.is_alive() and not shutdown_thread.is_alive()
    assert shutdown_errors == []
    assert activation.health()["status"] == "STOPPED"
    assert activation._kernel_product_runtimes == {}


def test_activation_parses_frozen_plan_and_publishes_exact_runtime_context() -> None:
    captured: dict[str, object] = {}

    class _ContextAdapter:
        def __init__(self, context_store) -> None:  # type: ignore[no-untyped-def]
            self.context_store = context_store

        def health(self) -> dict[str, object]:
            return {"status": "READY"}

        def release_runtime_context(self, runtime_id: str) -> None:
            captured["released_runtime_id"] = runtime_id

        def advance_clock(self, **kwargs: Any) -> SimpleNamespace:
            captured["advanced_clock"] = dict(kwargs)
            return SimpleNamespace(context_id="advanced-context")

        def prepare_runtime_context(self, **kwargs: Any) -> SimpleNamespace:
            captured.update(kwargs)
            captured["symbols"] = tuple(kwargs["symbols"])
            policy = QuoteContractPolicy.from_execution_policy(kwargs["execution_policy"])
            return SimpleNamespace(
                context_id="context-from-frozen-plan",
                policy=policy,
                symbols={"000001.SZ": object()},
            )

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ=_enabled_env(),
        schema_gate_reader=lambda: "applied_and_verified",
        subscriber_factory=_Subscriber,
        qmt_client_factory=_QmtClient,
        context_adapter_factory=lambda store, _qmt: _ContextAdapter(store),  # type: ignore[arg-type,return-value]
    )
    policy_context = _quote_control_policy_context(parent_intent_id="parent-context", trade_date=date(2026, 7, 14))
    plan = SimpleNamespace(
        plan_id="plan-context-publication",
        plan_payload_json={
            "execution_policy": {"payload": policy_context},
            "quote_control": policy_context["quote_control"],
        },
        intents=(SimpleNamespace(symbol="000001.SZ"),),
    )

    receipt = activation.prepare_runtime_context(
        runtime_id="runtime-context-publication",
        plan=plan,
        recovering_active=False,
        clock_at_utc=datetime(2026, 7, 14, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=100_000,
    )

    captured_execution_policy = captured["execution_policy"]
    assert isinstance(captured_execution_policy, dict)
    expected_policy_sha256 = QuoteContractPolicy.from_execution_policy(captured_execution_policy).policy_sha256
    assert receipt == {
        "runtime_id": "runtime-context-publication",
        "context_id": "context-from-frozen-plan",
        "policy_sha256": expected_policy_sha256,
        "symbol_count": 1,
        "recovering_active": False,
    }
    assert captured["runtime_id"] == "runtime-context-publication"
    assert captured["symbols"] == ("000001.SZ",)


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
    release, source = _release_and_binding(
        repository,
        backend=SimulationBrokerBackend.MINIQMT_SIM,
        quote_control=quote_control,
    )
    package_repository = SimpleNamespace(
        get=lambda package_id: SimpleNamespace(
            package_id=package_id,
            manifest_sha256=release.manifest_sha256,
        )
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repository,
        selection_service=SimpleNamespace(package_repository=package_repository),
    )

    rolled = scheduler._roll_forward_unattended_binding(source=source, trade_date=date(2026, 7, 14))

    assert rolled.binding_id != source.binding_id
    assert rolled.binding_config_json["miniqmt_quote_control"] == quote_control
    scheduler.shutdown_selection_inference()


def test_completed_watchdog_owner_drift_is_aggregated_without_starving_peer_result() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    def completed_entry(runtime_id: str, attempt_token: int) -> Any:
        result_queue: activation_module.queue.Queue[tuple[str, dict[str, object]]] = activation_module.queue.Queue(
            maxsize=1
        )
        claim = activation_module._KernelProductAttemptClaim(
            runtime_id=runtime_id,
            operation="WATCHDOG",
            lifecycle_generation=1,
            attempt_token=attempt_token,
        )
        entry = activation_module._KernelWatchdogWorker(
            runtime_id=runtime_id,
            binding_id=f"binding_{runtime_id}",
            runtime=object(),
            claim=claim,
            thread=Thread(),
            result_queue=result_queue,
            started_at_utc=datetime(2026, 8, 12, tzinfo=UTC),
        )
        result_queue.put_nowait(
            (
                "success",
                {
                    "runtime_id": runtime_id,
                    "lifecycle_generation": 1,
                    "attempt_token": attempt_token,
                    "release_after_success": False,
                },
            )
        )
        return entry

    drifted = completed_entry("runtime_watchdog_drift", 1)
    peer = completed_entry("runtime_watchdog_peer", 2)
    replacement = completed_entry("runtime_watchdog_drift", 3)

    class _DriftingResultQueue:
        def get_nowait(self) -> tuple[str, dict[str, object]]:
            activation._kernel_watchdog_workers[drifted.runtime_id] = replacement
            return (
                "success",
                {
                    "runtime_id": drifted.runtime_id,
                    "lifecycle_generation": 1,
                    "attempt_token": 1,
                    "release_after_success": False,
                },
            )

    drifted.result_queue = _DriftingResultQueue()  # type: ignore[assignment]
    activation._kernel_watchdog_workers[drifted.runtime_id] = drifted
    activation._kernel_watchdog_workers[peer.runtime_id] = peer

    failures, releases, consumed = activation._consume_kernel_watchdog_workers(wait_seconds=0.0)

    assert releases == []
    assert consumed == {drifted.runtime_id, peer.runtime_id}
    assert failures == [
        {
            "runtime_id": drifted.runtime_id,
            "binding_id": drifted.binding_id,
            "reason_code": "MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_OWNER_DRIFT",
            "broker_side_effect_state": "UNKNOWN",
            "expected_lifecycle_generation": 1,
            "expected_attempt_token": 1,
            "exception_type": None,
            "exception_message": "watchdog worker owner changed before result consumption",
        }
    ]
    assert activation._kernel_watchdog_workers == {drifted.runtime_id: replacement}
    assert peer.result_queue.empty()


def test_auxiliary_owner_drift_and_non_string_runtime_identity_are_aggregated_per_peer() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    def completed_entry(runtime_id: object, operation: str) -> Any:
        result_queue: activation_module.queue.Queue[tuple[str, dict[str, object]]] = activation_module.queue.Queue(
            maxsize=1
        )
        entry = activation_module._KernelAuxiliaryWorker(
            owner_key=runtime_id,
            operation=operation,
            runtime_id=runtime_id,
            binding_id=f"binding_{runtime_id}",
            lifecycle_generation=1,
            thread=Thread(),
            result_queue=result_queue,
            started_at_utc=datetime(2026, 8, 12, tzinfo=UTC),
        )
        result_queue.put_nowait(
            (
                "success",
                {
                    "runtime_id": runtime_id,
                    "binding_id": f"binding_{runtime_id}",
                    "operation": operation,
                    "lifecycle_generation": 1,
                },
            )
        )
        return entry

    invalid_identity = completed_entry(123, "PRIOR_DAY_RELEASE")
    drifted = completed_entry("runtime_aux_drift", "PRIOR_DAY_RELEASE")
    peer = completed_entry("runtime_aux_peer", "PRIOR_DAY_RELEASE")
    replacement = completed_entry("runtime_aux_drift", "PRIOR_DAY_RELEASE")

    class _DriftingResultQueue:
        def get_nowait(self) -> tuple[str, dict[str, object]]:
            activation._kernel_release_workers[drifted.runtime_id] = replacement
            return (
                "success",
                {
                    "runtime_id": drifted.runtime_id,
                    "binding_id": drifted.binding_id,
                    "operation": drifted.operation,
                    "lifecycle_generation": drifted.lifecycle_generation,
                },
            )

    drifted.result_queue = _DriftingResultQueue()  # type: ignore[assignment]
    activation._kernel_release_workers["123"] = invalid_identity
    activation._kernel_release_workers[drifted.runtime_id] = drifted
    activation._kernel_release_workers[peer.runtime_id] = peer

    failures = activation._consume_kernel_auxiliary_workers(wait_seconds=0.0)

    assert [failure["reason_code"] for failure in failures] == [
        "MINIQMT_K6_PRODUCT_AUXILIARY_WORKER_RUNTIME_IDENTITY_DRIFT",
        "MINIQMT_K6_PRODUCT_RELEASE_WORKER_OWNER_DRIFT",
    ]
    assert failures[0]["runtime_id"] == 123
    assert failures[0]["runtime_id_type"] == "builtins.int"
    assert failures[1]["runtime_id"] == drifted.runtime_id
    assert activation._kernel_release_workers == {drifted.runtime_id: replacement}
    assert peer.result_queue.empty()


@pytest.mark.parametrize("lifecycle_action", ["release", "shutdown"])
def test_runtime_release_records_and_clears_each_detached_pending_quote_identity(
    lifecycle_action: str,
) -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )

    class _Runtime:
        runtime_id = "runtime_pending_release_drop"
        binding_id = "binding_pending_release_drop"
        trade_date = date(2026, 8, 12)
        symbols = ("600000.SH",)

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    class _Supervisor(_KernelLeaseSupervisor):
        @staticmethod
        def shutdown() -> None:
            return None

    runtime = _Runtime()
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    state = activation._kernel_retry_states[runtime.runtime_id]
    operation_state = state.operations["CALLBACK"]
    pending = activation_module._KernelPendingQuoteV1(
        symbol="600000.SH",
        pending_identity_sha256="a" * 64,
        market_data_id="market_data_pending_release",
        ingress_generation=7,
        ingress_sequence=9,
        values=(object(),),
    )
    operation_state.pending_by_symbol[pending.symbol] = pending

    if lifecycle_action == "release":
        activation.release_kernel_product_runtime(runtime.runtime_id)
    else:
        activation.shutdown()

    assert operation_state.pending_by_symbol == {}
    assert operation_state.pending_drop_count_by_reason == {"PENDING_RUNTIME_RELEASED": 1}
    assert operation_state.not_replayed_pending_count == 1
    assert operation_state.last_pending_drop == {
        "reason": "PENDING_RUNTIME_RELEASED",
        "symbol": pending.symbol,
        "market_data_id": pending.market_data_id,
        "ingress_generation": pending.ingress_generation,
        "ingress_sequence": pending.ingress_sequence,
        "pending_identity_sha256": pending.pending_identity_sha256,
        "replacement_market_data_id": None,
        "replacement_ingress_generation": None,
        "replacement_ingress_sequence": None,
        "replacement_pending_identity_sha256": None,
    }
    health = activation.health()
    assert health["kernel_pending_drop_count_by_reason"] == {"PENDING_RUNTIME_RELEASED": 1}
    assert health["last_kernel_pending_drop"] == {
        "reason": "PENDING_RUNTIME_RELEASED",
        "registration_drop_reason": "RUNTIME_RELEASED",
        "runtime_id": runtime.runtime_id,
        "binding_id": runtime.binding_id,
        "operation": "CALLBACK",
        "lifecycle_generation": state.lifecycle_generation,
        "symbol": pending.symbol,
        "market_data_id": pending.market_data_id,
        "ingress_generation": pending.ingress_generation,
        "ingress_sequence": pending.ingress_sequence,
        "pending_identity_sha256": pending.pending_identity_sha256,
    }


def test_runtime_lifecycle_locks_are_reclaimed_after_each_exact_release() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    supervisor = _KernelLeaseSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]

    class _Runtime:
        trade_date = date(2026, 8, 12)
        symbols = ("600000.SH",)

        def __init__(self, index: int) -> None:
            self.runtime_id = f"runtime_lifecycle_lock_{index:02d}"
            self.binding_id = f"binding_lifecycle_lock_{index:02d}"

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    for index in range(8):
        runtime = _Runtime(index)
        activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
        activation.release_kernel_product_runtime(runtime.runtime_id)

    assert activation._kernel_runtime_lifecycle_locks == {}
    assert activation._kernel_runtime_lifecycle_lock_users == {}
    assert activation.health()["kernel_runtime_lifecycle_lock_count"] == 0


def test_runtime_lifecycle_lock_reclamation_preserves_concurrent_same_id_fence() -> None:
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    release_entered = Event()
    allow_release = Event()

    class _BlockingSupervisor(_KernelLeaseSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.release_count = 0

        def release_consumer(self, *, consumer_id: str) -> bool:
            self.release_count += 1
            if self.release_count == 1:
                release_entered.set()
                assert allow_release.wait(timeout=5)
            return super().release_consumer(consumer_id=consumer_id)

    class _Runtime:
        runtime_id = "runtime_concurrent_release_fence"
        trade_date = date(2026, 8, 12)
        symbols = ("600000.SH",)

        def __init__(self, binding_id: str) -> None:
            self.binding_id = binding_id

        @staticmethod
        def observe_b0_quote_v1(*_values: object) -> None:
            return None

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    supervisor = _BlockingSupervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    original = _Runtime("binding_concurrent_release_original")
    activation.register_kernel_product_runtime(runtime=original, symbols=original.symbols)
    failures: list[Exception] = []

    def release_runtime() -> None:
        try:
            activation.release_kernel_product_runtime(original.runtime_id)
        except Exception as exc:  # noqa: BLE001 - assert the exact duplicate-release loser below.
            failures.append(exc)

    first = Thread(target=release_runtime)
    second = Thread(target=release_runtime)
    first.start()
    assert release_entered.wait(timeout=5)
    second.start()
    for _attempt in range(100):
        if activation._kernel_runtime_lifecycle_lock_users.get(original.runtime_id) == 2:
            break
        monotonic_time.sleep(0.01)
    assert activation._kernel_runtime_lifecycle_lock_users[original.runtime_id] == 2

    allow_release.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive() and not second.is_alive()
    assert len(failures) == 1 and isinstance(failures[0], KeyError)
    assert activation._kernel_runtime_lifecycle_locks == {}
    assert activation._kernel_runtime_lifecycle_lock_users == {}

    successor = _Runtime("binding_concurrent_release_successor")
    activation.register_kernel_product_runtime(runtime=successor, symbols=successor.symbols)
    assert activation.get_kernel_product_runtime(successor.runtime_id) is successor
    activation.release_kernel_product_runtime(successor.runtime_id)
    assert activation._kernel_runtime_lifecycle_locks == {}
