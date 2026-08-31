"""Production composition root for the Phase 1 MiniQMT quote ingress.

This module owns construction only.  It never applies DDL, persists process
configuration, creates a simulation binding, or calls the broker.  A schema-
blocked composition exposes no B0 controller factory.  With an exact schema,
switch=false exposes a drain-only factory solely for durable active recovery;
new B0_QUOTE_V2 assignments still fail closed while historical LEGACY_B0
bindings continue on their unchanged path.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from hashlib import sha256
import logging
import os
import queue
import threading
import time as monotonic_time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping
from zoneinfo import ZoneInfo

from backend.miniqmt_quote_contract_config import QuoteContractPolicy, QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2RevisionV1,
    B0QuoteV2ControllerFactory,
    ParentQuoteControlAssignmentV1,
    QuoteControlBindingV1,
    quote_ingress_config_sha256,
)
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractReasonCode,
    quote_contract_error,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import QuoteEvaluationContextStore
from backend.services.miniqmt_execution_runtime.plugin_contracts import bounded_exception_summary_v1
from backend.services.miniqmt_execution_runtime.kernel_repository import (
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.kernel_repository_schema import (
    KernelRepositorySchemaError,
    validate_kernel_schema_preflight_readback,
)
from backend.services.miniqmt_execution_runtime.quote_ingress import (
    MiniQMTKernelProductIngressCompletionSignal,
    MiniQMTKernelProductIngressPending,
    MiniQMTKernelProductIngressSuppression,
    kernel_product_pending_identity_sha256_v1,
)
from backend.services.simulation_data.daily_context_provider import (
    DbEquityInstrumentMetadataProvider,
    DbSuspendStatusProvider,
)
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.limit_price_provider import StkLimitPriceProvider
from .miniqmt_quote_context import MiniQMTInstrumentQuoteSpecProvider, MiniQMTQuoteContextAuthorityAdapter

if TYPE_CHECKING:
    from backend.services.miniqmt_execution_runtime.quote_ingress import QuoteIngressSupervisor


logger = logging.getLogger("aistock.simulation_runtime.miniqmt_quote_activation")

MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY = "SIM:B0_QUOTE_V2:simulation_scheduler"
MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED = "applied_and_verified"
_CHINA_TZ = ZoneInfo("Asia/Shanghai")
_KERNEL_EVENT_SCHEMA_CONSTRAINTS = frozenset(
    {
        "ck_miniqmt_event_id",
        "ck_miniqmt_event_sequence",
        "ck_miniqmt_event_type",
        "ck_miniqmt_event_source",
        "ck_miniqmt_k2_event_contract",
        "ck_miniqmt_k2_event_composite",
    }
)
_KERNEL_SCHEMA_RETRY_SECONDS = (60, 120, 240, 480, 960, 1920, 3600)
_KERNEL_CALLBACK_OPERATION = "CALLBACK"
_KERNEL_WATCHDOG_OPERATION = "WATCHDOG"
_KERNEL_RETRY_OPERATIONS = frozenset({_KERNEL_CALLBACK_OPERATION, _KERNEL_WATCHDOG_OPERATION})
_KERNEL_RELEASE_OPERATIONS = frozenset({"PRIOR_DAY_RELEASE", "RELEASE_UNKNOWN_RECONCILIATION"})
_KERNEL_ACTIVE_LIFECYCLE = "ACTIVE"
_KERNEL_RELEASING_LIFECYCLE = "RELEASING"
_KERNEL_RELEASE_UNKNOWN_LIFECYCLE = "RELEASE_UNKNOWN"
_KERNEL_RELEASED_LIFECYCLE = "RELEASED"
_KERNEL_WATCHDOG_PEER_WAIT_SECONDS = 0.25
_KERNEL_CALLBACK_PEER_WAIT_SECONDS = 0.01


class MiniQMTKernelProductSyncError(RuntimeError):
    def __init__(self, failures: tuple[dict[str, Any], ...]) -> None:
        self.reason_code = "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED"
        self.context = {
            "ordered_failures": list(failures),
            "broker_side_effect_state": "UNKNOWN",
        }
        super().__init__("one or more KERNEL_V2 runtimes failed callback or exchange-clock ingress")


class MiniQMTKernelProductRegistryRollbackError(RuntimeError):
    def __init__(self, *, operation: str, primary: Exception, rollback: Exception) -> None:
        self.reason_code = "MINIQMT_K6_PRODUCT_REGISTRY_ROLLBACK_FAILED"
        self.context = {
            "operation": operation,
            "primary_failure": bounded_exception_summary_v1(primary),
            "rollback_failure": bounded_exception_summary_v1(rollback),
            "broker_side_effect_state": "UNKNOWN",
        }
        super().__init__("KERNEL_V2 product registry operation and its exact rollback both failed")


class MiniQMTKernelProductLifecycleError(RuntimeError):
    def __init__(self, *, reason_code: str, message: str, context: Mapping[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = {**dict(context), "broker_side_effect_state": "UNKNOWN"}
        super().__init__(message)


@dataclass(frozen=True)
class _KernelProductWatchdogSuppression:
    runtime_id: str
    disposition: str
    lifecycle_generation: int
    failure_fingerprint_sha256: str | None
    next_retry_at_utc: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_kernel_product_watchdog_suppression_v1",
            "runtime_id": self.runtime_id,
            "operation": _KERNEL_WATCHDOG_OPERATION,
            "disposition": self.disposition,
            "lifecycle_generation": self.lifecycle_generation,
            "failure_fingerprint_sha256": self.failure_fingerprint_sha256,
            "next_retry_at_utc": self.next_retry_at_utc,
            "executed": False,
            "business_success": False,
            "broker_side_effect_state": "UNKNOWN",
        }


@dataclass
class _KernelPendingQuoteV1:
    symbol: str
    pending_identity_sha256: str
    market_data_id: str | None
    ingress_generation: int | None
    ingress_sequence: int | None
    values: tuple[Any, ...] = field(repr=False)

    def as_health(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "pending_identity_sha256": self.pending_identity_sha256,
            "market_data_id": self.market_data_id,
            "ingress_generation": self.ingress_generation,
            "ingress_sequence": self.ingress_sequence,
        }


@dataclass
class _KernelOperationRetryState:
    operation: str
    state: str = "HEALTHY"
    failure_class: str | None = None
    failure_fingerprint_sha256: str | None = None
    reason_code: str | None = None
    sqlstate: str | None = None
    constraint_name: str | None = None
    first_failure_at_utc: datetime | None = None
    last_failure_at_utc: datetime | None = None
    last_attempt_at_utc: datetime | None = None
    next_retry_at_utc: datetime | None = None
    next_retry_monotonic_ns: int | None = None
    attempt_count: int = 0
    consecutive_failure_count: int = 0
    suppressed_count: int = 0
    secondary_failure_count: int = 0
    not_replayed_pending_count: int = 0
    last_success_at_utc: datetime | None = None
    active_failure: dict[str, Any] | None = None
    last_failure: dict[str, Any] | None = None
    last_secondary_failure: dict[str, Any] | None = None
    last_pending_resolution: dict[str, Any] | None = None
    pending_drop_count_by_reason: dict[str, int] = field(default_factory=dict)
    last_pending_drop: dict[str, Any] | None = None
    pending_by_symbol: dict[str, _KernelPendingQuoteV1] = field(default_factory=dict, repr=False)

    def effective_state(self) -> str:
        if self.active_failure is not None:
            return "RETRY_BACKOFF" if self.next_retry_monotonic_ns is not None else "RETRY_READY"
        if self.pending_by_symbol:
            return "RECOVERY_PENDING"
        return "HEALTHY"

    def as_health(self) -> dict[str, Any]:
        state = self.effective_state()
        return {
            "operation": self.operation,
            "state": state,
            "failure_class": self.failure_class,
            "failure_fingerprint_sha256": self.failure_fingerprint_sha256,
            "reason_code": self.reason_code,
            "sqlstate": self.sqlstate,
            "constraint_name": self.constraint_name,
            "first_failure_at_utc": _iso_utc(self.first_failure_at_utc),
            "last_failure_at_utc": _iso_utc(self.last_failure_at_utc),
            "last_attempt_at_utc": _iso_utc(self.last_attempt_at_utc),
            "next_retry_at_utc": _iso_utc(self.next_retry_at_utc),
            "attempt_count": self.attempt_count,
            "consecutive_failure_count": self.consecutive_failure_count,
            "suppressed_count": self.suppressed_count,
            "secondary_failure_count": self.secondary_failure_count,
            "not_replayed_pending_count": self.not_replayed_pending_count,
            "last_success_at_utc": _iso_utc(self.last_success_at_utc),
            "active_failure": dict(self.active_failure) if self.active_failure is not None else None,
            "last_failure": dict(self.last_failure) if self.last_failure is not None else None,
            "last_secondary_failure": (
                dict(self.last_secondary_failure) if self.last_secondary_failure is not None else None
            ),
            "last_pending_resolution": (
                dict(self.last_pending_resolution) if self.last_pending_resolution is not None else None
            ),
            "pending_drop_count_by_reason": dict(sorted(self.pending_drop_count_by_reason.items())),
            "last_pending_drop": dict(self.last_pending_drop) if self.last_pending_drop is not None else None,
            "pending": [pending.as_health() for _, pending in sorted(self.pending_by_symbol.items())],
            "automatic_retry": self.active_failure is not None or bool(self.pending_by_symbol),
            "recovery_trigger": (
                "NEXT_LIVE_QUOTE_OR_SESSION_EXPIRY"
                if self.operation == _KERNEL_CALLBACK_OPERATION
                else "WATCHDOG_CADENCE"
            ),
        }


@dataclass
class _KernelProductIngressRetryState:
    runtime_id: str
    binding_id: str | None
    trade_date: str | None
    source_capability_sha256: str | None
    symbols: tuple[str, ...]
    lifecycle_generation: int
    lifecycle_state: str = _KERNEL_ACTIVE_LIFECYCLE
    operations: dict[str, _KernelOperationRetryState] = field(
        default_factory=lambda: {
            _KERNEL_CALLBACK_OPERATION: _KernelOperationRetryState(_KERNEL_CALLBACK_OPERATION),
            _KERNEL_WATCHDOG_OPERATION: _KernelOperationRetryState(_KERNEL_WATCHDOG_OPERATION),
        },
        repr=False,
    )
    last_failure: dict[str, Any] | None = None

    def as_health(self) -> dict[str, Any]:
        operation_health = {operation: state.as_health() for operation, state in sorted(self.operations.items())}
        active_states = [state for state in self.operations.values() if state.active_failure is not None]
        pending_states = [state for state in self.operations.values() if state.pending_by_symbol]
        primary = active_states[0] if active_states else (pending_states[0] if pending_states else None)
        if self.lifecycle_state != _KERNEL_ACTIVE_LIFECYCLE:
            state = self.lifecycle_state
        elif active_states:
            state = (
                "RETRY_BACKOFF"
                if any(item.next_retry_monotonic_ns is not None for item in active_states)
                else "RETRY_READY"
            )
        elif pending_states:
            state = "RECOVERY_PENDING"
        else:
            state = "HEALTHY"
        return {
            "schema_version": "miniqmt_kernel_product_ingress_retry_v1",
            "runtime_id": self.runtime_id,
            "binding_id": self.binding_id,
            "trade_date": self.trade_date,
            "source_capability_sha256": self.source_capability_sha256,
            "symbols": list(self.symbols),
            "lifecycle_generation": self.lifecycle_generation,
            "lifecycle_state": self.lifecycle_state,
            "state": state,
            "failure_operation": primary.operation if primary is not None else None,
            "failure_class": primary.failure_class if primary is not None else None,
            "failure_fingerprint_sha256": (primary.failure_fingerprint_sha256 if primary is not None else None),
            "reason_code": primary.reason_code if primary is not None else None,
            "sqlstate": primary.sqlstate if primary is not None else None,
            "constraint_name": primary.constraint_name if primary is not None else None,
            "first_failure_at_utc": _iso_utc(primary.first_failure_at_utc) if primary is not None else None,
            "last_failure_at_utc": _iso_utc(primary.last_failure_at_utc) if primary is not None else None,
            "last_attempt_at_utc": _iso_utc(primary.last_attempt_at_utc) if primary is not None else None,
            "next_retry_at_utc": _iso_utc(primary.next_retry_at_utc) if primary is not None else None,
            "attempt_count": sum(item.attempt_count for item in self.operations.values()),
            "consecutive_failure_count": (primary.consecutive_failure_count if primary is not None else 0),
            "suppressed_callback_count": self.operations[_KERNEL_CALLBACK_OPERATION].suppressed_count,
            "suppressed_watchdog_count": self.operations[_KERNEL_WATCHDOG_OPERATION].suppressed_count,
            "last_success_at_utc": max(
                (item.last_success_at_utc for item in self.operations.values() if item.last_success_at_utc is not None),
                default=None,
            ).isoformat()
            if any(item.last_success_at_utc is not None for item in self.operations.values())
            else None,
            "active_failure": (
                dict(primary.active_failure) if primary is not None and primary.active_failure is not None else None
            ),
            "last_failure": dict(self.last_failure) if self.last_failure is not None else None,
            "operations": operation_health,
            "automatic_retry": bool(
                active_states or pending_states or self.lifecycle_state == _KERNEL_RELEASE_UNKNOWN_LIFECYCLE
            ),
            "manual_ack_required": False,
            "business_gate": False,
            "broker_side_effect_state": (
                str(primary.active_failure.get("broker_side_effect_state"))
                if primary is not None and primary.active_failure is not None
                else ("UNKNOWN" if self.lifecycle_state == _KERNEL_RELEASE_UNKNOWN_LIFECYCLE else "NOT_APPLICABLE")
            ),
        }


@dataclass(frozen=True)
class _KernelProductAttemptClaim:
    runtime_id: str
    operation: str
    lifecycle_generation: int
    attempt_token: int


@dataclass
class _KernelWatchdogWorker:
    runtime_id: str
    binding_id: str | None
    runtime: Any
    claim: _KernelProductAttemptClaim
    thread: threading.Thread
    result_queue: queue.Queue[tuple[str, dict[str, Any]]]
    started_at_utc: datetime


@dataclass
class _KernelCallbackTask:
    claim: _KernelProductAttemptClaim
    values: tuple[Any, ...]
    pending: _KernelPendingQuoteV1
    completion_signal: MiniQMTKernelProductIngressCompletionSignal
    result_queue: queue.Queue[tuple[str, Any]]
    result_observed: threading.Event


@dataclass
class _KernelCallbackWorker:
    runtime_id: str
    binding_id: str | None
    runtime: Any
    lifecycle_generation: int
    sink: Callable[..., Any]
    task_queue: queue.Queue[_KernelCallbackTask | None]
    stop_event: threading.Event
    stopped_event: threading.Event
    thread: threading.Thread
    started_at_utc: datetime
    active_attempt_token: int | None = None
    processed_count: int = 0
    last_failure: dict[str, Any] | None = None


@dataclass(frozen=True)
class _KernelProductGuardedSink:
    activation: Any = field(repr=False, compare=False)
    runtime_id: str
    consumer_id: str
    runtime: Any = field(repr=False, compare=False)
    sink: Callable[..., Any] = field(repr=False, compare=False)
    lifecycle_generation: int

    def __call__(self, *values: Any) -> Any:
        return self.activation._invoke_kernel_product_callback(
            runtime_id=self.runtime_id,
            consumer_id=self.consumer_id,
            runtime=self.runtime,
            sink=self.sink,
            lifecycle_generation=self.lifecycle_generation,
            values=tuple(values),
        )

    def enqueue_kernel_product_callback_v1(self, *values: Any) -> Any:
        return self.activation._enqueue_kernel_product_callback(
            runtime_id=self.runtime_id,
            consumer_id=self.consumer_id,
            runtime=self.runtime,
            sink=self.sink,
            lifecycle_generation=self.lifecycle_generation,
            values=tuple(values),
        )

    def await_kernel_product_callback_v1(self, *, dispatch: Any, timeout_seconds: float) -> Any:
        return self.activation._await_kernel_product_callback(
            dispatch=dispatch,
            timeout_seconds=timeout_seconds,
        )


@dataclass
class _KernelAuxiliaryWorker:
    owner_key: str
    operation: str
    runtime_id: str | None
    binding_id: str | None
    lifecycle_generation: int | None
    thread: threading.Thread
    result_queue: queue.Queue[tuple[str, dict[str, Any]]]
    started_at_utc: datetime


def _iso_utc(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def _production_quote_event_schema_gate() -> str:
    readback = PostgresMiniQMTKernelRepository().preflight_schema()
    try:
        validate_kernel_schema_preflight_readback(readback)
    except KernelRepositorySchemaError as exc:
        raise RuntimeError("MiniQMT KERNEL_V2 full PostgreSQL schema preflight did not close exactly") from exc
    return MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED


def _production_subscriber() -> Any:
    from backend.infra.realtime_quote_subscriber import get_realtime_quote_subscriber

    return get_realtime_quote_subscriber()


def _production_qmt_client() -> Any:
    from backend.infra.qmt_client import get_qmt_client_singleton

    return get_qmt_client_singleton()


def _build_bootstrap_fetcher(qmt_client: Any) -> Callable[[list[str]], Mapping[str, Mapping[str, Any]]]:
    get_full_tick = getattr(qmt_client, "get_full_tick", None)
    if not callable(get_full_tick):
        raise TypeError("MiniQMT quote activation requires a callable get_full_tick broker read")

    def fetch(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
        # The Phase 1 lease has already established the physical whole-quote
        # subscription.  Reusing the LEGACY managed-subscription path here
        # would create a second owner and violate the single-feed contract.
        return get_full_tick(
            symbols,
            ensure_subscription=False,
            ensure_fresh=False,
        )

    return fetch


def _production_quote_context_adapter(
    context_store: QuoteEvaluationContextStore,
    qmt_client: Any,
) -> MiniQMTQuoteContextAuthorityAdapter:
    instrument_reader = getattr(qmt_client, "get_instrument_detail", None)
    if not callable(instrument_reader):
        raise TypeError("MiniQMT quote activation requires a callable instrument-detail authority")
    instrument_provider = MiniQMTInstrumentQuoteSpecProvider(instrument_reader)
    return MiniQMTQuoteContextAuthorityAdapter(
        context_store=context_store,
        trading_calendar_service=TradingCalendarStatusService(),
        suspend_status_provider=DbSuspendStatusProvider(),
        limit_price_provider=StkLimitPriceProvider(),
        equity_metadata_provider=DbEquityInstrumentMetadataProvider(),
        runtime_symbol_spec_provider=instrument_provider.get_symbol_spec,
    )


class DrainOnlyB0QuoteV2ControllerFactory:
    """Lazy factory that permits only durable-active restart recovery.

    Default LEGACY startup therefore performs no schema read, QMT construction,
    or subscriber construction.  The exact production CHECK gate is evaluated
    only when a durable active B0 runtime actually asks to recover.
    """

    def __init__(
        self,
        *,
        requested_config: QuoteIngressRuntimeConfig,
        schema_gate_reader: Callable[[], str],
        subscriber_factory: Callable[[], Any],
        qmt_client_factory: Callable[[], Any],
        context_adapter_factory: Callable[
            [QuoteEvaluationContextStore, Any], MiniQMTQuoteContextAuthorityAdapter
        ] = _production_quote_context_adapter,
    ) -> None:
        if requested_config.enabled:
            raise ValueError("drain-only factory requires process switch=false")
        self.requested_config = requested_config
        self.runtime_config = replace(requested_config, enabled=True)
        self._schema_gate_reader = schema_gate_reader
        self._subscriber_factory = subscriber_factory
        self._qmt_client_factory = qmt_client_factory
        self._context_adapter_factory = context_adapter_factory
        self._delegate: B0QuoteV2ControllerFactory | None = None
        self._supervisor: QuoteIngressSupervisor | None = None
        self._context_adapter: MiniQMTQuoteContextAuthorityAdapter | None = None
        self._lock = threading.RLock()
        self._last_failure: dict[str, Any] | None = None
        self._production_ddl_gate = "deferred_until_durable_recovery"
        self._shutdown = False

    @property
    def supervisor(self) -> QuoteIngressSupervisor | None:
        return self._supervisor

    @property
    def production_ddl_gate(self) -> str:
        return self._production_ddl_gate

    @property
    def context_adapter(self) -> MiniQMTQuoteContextAuthorityAdapter | None:
        return self._context_adapter

    def assert_accepts_new_assignments(self) -> None:
        raise quote_contract_error(
            QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
            "B0_QUOTE_V2 switch is disabled for new assignments; only durable active runtimes may drain",
            context={"data_session_key": MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY, "recovering_active": False},
        )

    def get(self, runtime_id: str) -> Any | None:
        return self._delegate.get(runtime_id) if self._delegate is not None else None

    def create(
        self,
        *,
        runtime: Any,
        assignments: Mapping[str, Any],
        symbols: tuple[str, ...],
        recovering_active: bool = False,
    ) -> Any:
        if not recovering_active:
            self.assert_accepts_new_assignments()
        delegate = self._ensure_delegate()
        return delegate.create(
            runtime=runtime,
            assignments=assignments,
            symbols=symbols,
            recovering_active=True,
        )

    def release(self, runtime_id: str) -> None:
        if self._delegate is not None:
            self._delegate.release(runtime_id)

    def set_accept_new_assignments(self, enabled: bool) -> None:
        if enabled:
            raise ValueError("drain-only factory cannot enable new assignments without process restart")
        if self._delegate is not None:
            self._delegate.set_accept_new_assignments(False)

    def health(self) -> dict[str, Any]:
        delegate_health = self._delegate.health() if self._delegate is not None else None
        return {
            "schema_version": "b0_quote_v2_drain_only_factory_v1",
            "lifecycle_state": "STOPPED" if self._shutdown else "DRAINING",
            "accept_new_assignments": False,
            "factory_initialized": self._delegate is not None,
            "data_session_key": MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
            "production_ddl_gate": self._production_ddl_gate,
            "last_failure": dict(self._last_failure) if self._last_failure is not None else None,
            "delegate": delegate_health,
        }

    def shutdown(self) -> None:
        if self._shutdown:
            return
        if self._delegate is not None:
            self._delegate.set_accept_new_assignments(False)
        if self._supervisor is not None:
            self._supervisor.shutdown()
        self._shutdown = True

    def _ensure_delegate(self) -> B0QuoteV2ControllerFactory:
        with self._lock:
            if self._shutdown:
                raise RuntimeError("stopped drain-only B0_QUOTE_V2 factory cannot recover a runtime")
            if self._delegate is not None:
                return self._delegate
            try:
                gate = str(self._schema_gate_reader())
            except Exception as exc:  # noqa: BLE001 - convert to a stable, visible recovery failure
                self._production_ddl_gate = "readback_failed"
                self._last_failure = {"exception_type": type(exc).__name__, "message": str(exc)}
                logger.error("B0_QUOTE_V2 drain recovery schema readback failed", exc_info=True)
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                    "B0_QUOTE_V2 drain recovery requires exact production schema readback",
                    context={
                        "production_ddl_gate": self._production_ddl_gate,
                        **self._last_failure,
                        "recovering_active": True,
                    },
                ) from exc
            self._production_ddl_gate = gate
            if gate != MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED:
                self._last_failure = {
                    "reason_code": "MINIQMT_QUOTE_EVENT_SCHEMA_NOT_APPLIED",
                    "production_ddl_gate": gate,
                }
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                    "B0_QUOTE_V2 drain recovery requires applied_and_verified production DDL",
                    context={"production_ddl_gate": gate, "recovering_active": True},
                )
            supervisor, delegate, context_adapter = _build_runtime_components(
                runtime_config=self.runtime_config,
                subscriber_factory=self._subscriber_factory,
                qmt_client_factory=self._qmt_client_factory,
                context_adapter_factory=self._context_adapter_factory,
            )
            delegate.set_accept_new_assignments(False)
            self._supervisor = supervisor
            self._delegate = delegate
            self._context_adapter = context_adapter
            self._last_failure = None
            return delegate


def _build_runtime_components(
    *,
    runtime_config: QuoteIngressRuntimeConfig,
    subscriber_factory: Callable[[], Any],
    qmt_client_factory: Callable[[], Any],
    context_adapter_factory: Callable[[QuoteEvaluationContextStore, Any], MiniQMTQuoteContextAuthorityAdapter],
) -> tuple[QuoteIngressSupervisor, B0QuoteV2ControllerFactory, MiniQMTQuoteContextAuthorityAdapter]:
    from backend.services.miniqmt_execution_runtime.quote_ingress import QuoteIngressSupervisor

    subscriber = subscriber_factory()
    qmt_client = qmt_client_factory()
    context_store = QuoteEvaluationContextStore()
    context_adapter = context_adapter_factory(context_store, qmt_client)
    if context_adapter.context_store is not context_store:
        raise ValueError("MiniQMT quote context adapter must share the ingress context store")
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=runtime_config,
        data_session_key=MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
        owner=runtime_config.owner_mode,
        bootstrap_fetcher=_build_bootstrap_fetcher(qmt_client),
        context_store=context_store,
    )
    controller_factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=runtime_config,
        data_session_key=MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
        context_release_callback=context_adapter.release_runtime_context,
        context_advance_callback=context_adapter.advance_clock,
        clock_sample_provider=lambda: (datetime.now(UTC), monotonic_time.monotonic_ns()),
    )
    return supervisor, controller_factory, context_adapter


@dataclass
class MiniQMTQuoteIngressActivation:
    """Scheduler-owned activation state and lifecycle delegation."""

    config: QuoteIngressRuntimeConfig
    status: str
    production_ddl_gate: str
    process_switch_enabled: bool
    data_session_key: str = MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY
    reason_code: str | None = None
    failure: dict[str, Any] | None = None
    supervisor: QuoteIngressSupervisor | None = None
    controller_factory: Any | None = None
    context_adapter: MiniQMTQuoteContextAuthorityAdapter | None = None
    _startup_schema_gate_reader: Callable[[], str] | None = field(default=None, repr=False)
    _startup_subscriber_factory: Callable[[], Any] | None = field(default=None, repr=False)
    _startup_qmt_client_factory: Callable[[], Any] | None = field(default=None, repr=False)
    _startup_context_adapter_factory: (
        Callable[[QuoteEvaluationContextStore, Any], MiniQMTQuoteContextAuthorityAdapter] | None
    ) = field(default=None, repr=False)
    _startup_recovery_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _shutdown: bool = False
    _shutdown_requested: bool = field(default=False, repr=False)
    _kernel_product_runtimes: dict[str, Any] = field(default_factory=dict)
    _kernel_retry_states: dict[str, _KernelProductIngressRetryState] = field(default_factory=dict, repr=False)
    _kernel_product_in_flight: dict[tuple[str, str], _KernelProductAttemptClaim] = field(
        default_factory=dict,
        repr=False,
    )
    _kernel_watchdog_workers: dict[str, _KernelWatchdogWorker] = field(default_factory=dict, repr=False)
    _kernel_callback_workers: dict[str, _KernelCallbackWorker] = field(default_factory=dict, repr=False)
    _kernel_release_workers: dict[str, _KernelAuxiliaryWorker] = field(default_factory=dict, repr=False)
    _kernel_supervisor_watchdog_worker: _KernelAuxiliaryWorker | None = field(default=None, repr=False)
    _kernel_guarded_sinks: dict[str, Callable[..., Any]] = field(default_factory=dict, repr=False)
    _kernel_registration_drop_count_by_reason: dict[str, int] = field(default_factory=dict, repr=False)
    _last_kernel_registration_drop: dict[str, Any] | None = field(default=None, repr=False)
    _kernel_pending_drop_count_by_reason: dict[str, int] = field(default_factory=dict, repr=False)
    _last_kernel_pending_drop: dict[str, Any] | None = field(default=None, repr=False)
    _kernel_retry_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _kernel_lifecycle_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _kernel_runtime_lifecycle_locks: dict[str, threading.RLock] = field(default_factory=dict, repr=False)
    _kernel_runtime_lifecycle_lock_users: dict[str, int] = field(default_factory=dict, repr=False)
    _kernel_retry_condition: threading.Condition = field(init=False, repr=False)
    _kernel_lifecycle_generation: int = field(default=0, repr=False)
    _kernel_attempt_token: int = field(default=0, repr=False)
    _kernel_lifecycle_drain_timeout_seconds: float = field(default=30.0, repr=False)
    _kernel_watchdog_peer_wait_seconds: float = field(
        default=_KERNEL_WATCHDOG_PEER_WAIT_SECONDS,
        repr=False,
    )
    _kernel_retry_clock_utc: Callable[[], datetime] = field(default=lambda: datetime.now(UTC), repr=False)
    _kernel_retry_monotonic_ns: Callable[[], int] = field(default=monotonic_time.monotonic_ns, repr=False)

    def __post_init__(self) -> None:
        self._kernel_retry_condition = threading.Condition(self._kernel_retry_lock)
        if (
            isinstance(self._kernel_lifecycle_drain_timeout_seconds, bool)
            or not isinstance(self._kernel_lifecycle_drain_timeout_seconds, (int, float))
            or self._kernel_lifecycle_drain_timeout_seconds <= 0
        ):
            raise ValueError("kernel lifecycle drain timeout must be a positive number of seconds")
        if (
            isinstance(self._kernel_watchdog_peer_wait_seconds, bool)
            or not isinstance(self._kernel_watchdog_peer_wait_seconds, (int, float))
            or self._kernel_watchdog_peer_wait_seconds <= 0
        ):
            raise ValueError("kernel watchdog peer wait must be a positive number of seconds")
        if self.status == "READY":
            if self.production_ddl_gate != MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED:
                raise ValueError("operational quote activation requires applied_and_verified production DDL")
            if self.supervisor is None or self.controller_factory is None:
                raise ValueError("operational quote activation requires supervisor and controller factory")
            if self.context_adapter is None:
                raise ValueError("operational quote activation requires an authoritative context adapter")
        elif self.status == "DRAINING":
            if (
                not isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory)
                or self.supervisor is not None
                or self.context_adapter is not None
            ):
                raise ValueError("DRAINING quote activation requires only the lazy drain factory")
        elif self.supervisor is not None or self.controller_factory is not None or self.context_adapter is not None:
            raise ValueError("disabled or blocked quote activation cannot expose runtime components")

    def _kernel_runtime_owner_failure(
        self,
        *,
        runtime_id: str,
        runtime: Any,
        state: _KernelProductIngressRetryState,
        guarded_sink: Callable[..., Any] | None,
        callback_worker: _KernelCallbackWorker | None,
        supervisor: Any | None,
    ) -> dict[str, Any] | None:
        binding_id = getattr(runtime, "binding_id", None)
        consumer_id = f"k6d-kernel-v2:{runtime_id}"
        if (
            callback_worker is None
            or callback_worker.runtime is not runtime
            or callback_worker.lifecycle_generation != state.lifecycle_generation
            or not callable(callback_worker.sink)
            or callback_worker.stop_event.is_set()
            or callback_worker.stopped_event.is_set()
            or not callback_worker.thread.is_alive()
        ):
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "expected_lifecycle_generation": state.lifecycle_generation,
                "actual_lifecycle_generation": (
                    callback_worker.lifecycle_generation if callback_worker is not None else None
                ),
                "worker_present": callback_worker is not None,
                "thread_alive": callback_worker.thread.is_alive() if callback_worker is not None else False,
                "stop_requested": callback_worker.stop_event.is_set() if callback_worker is not None else None,
                "broker_side_effect_state": "UNKNOWN",
            }
        if supervisor is None:
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_SINK_OWNER_READBACK_FAILED",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "exception_type": "builtins.RuntimeError",
                "exception_message": "registered KERNEL_V2 runtime has no quote supervisor",
                "broker_side_effect_state": "UNKNOWN",
            }
        lease_snapshot_reader = getattr(supervisor, "consumer_lease_owner_snapshot", None)
        if not callable(lease_snapshot_reader):
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_CONSUMER_LEASE_READBACK_UNAVAILABLE",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "broker_side_effect_state": "UNKNOWN",
                "exception_type": "builtins.RuntimeError",
                "exception_message": "quote supervisor lacks nonblocking exact consumer lease authority",
            }
        try:
            lease_snapshot = lease_snapshot_reader(
                consumer_id=consumer_id,
                symbols=state.symbols,
            )
        except Exception as exc:  # noqa: BLE001 - exact physical lease readback must fail loud per runtime.
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_CONSUMER_LEASE_READBACK_FAILED",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "broker_side_effect_state": "UNKNOWN",
                **bounded_exception_summary_v1(exc),
            }
        expected_lease = lease_snapshot.get("expected_lease") if isinstance(lease_snapshot, Mapping) else None
        actual_lease = lease_snapshot.get("actual_lease") if isinstance(lease_snapshot, Mapping) else None
        expected_identity = (
            lease_snapshot.get("expected_owner_identity_sha256") if isinstance(lease_snapshot, Mapping) else None
        )
        actual_identity = (
            lease_snapshot.get("actual_owner_identity_sha256") if isinstance(lease_snapshot, Mapping) else None
        )
        exact_physical_owner = bool(
            isinstance(lease_snapshot, Mapping)
            and lease_snapshot.get("schema_version") == "miniqmt_quote_consumer_lease_owner_snapshot_v1"
            and lease_snapshot.get("readback_current") is True
            and lease_snapshot.get("exact_owner") is True
            and lease_snapshot.get("state") == "ACTIVE"
            and type(lease_snapshot.get("registration_generation")) is int
            and lease_snapshot["registration_generation"] > 0
            and type(expected_identity) is str
            and len(expected_identity) == 64
            and expected_identity == actual_identity
            and isinstance(expected_lease, Mapping)
            and isinstance(actual_lease, Mapping)
            and dict(expected_lease) == dict(actual_lease)
            and actual_lease.get("consumer_id") == consumer_id
            and actual_lease.get("data_session_key") == self.data_session_key
            and actual_lease.get("owner") == self.config.owner_mode
            and tuple(actual_lease.get("symbols") or ()) == state.symbols
            and type(actual_lease.get("lease_id")) is str
            and bool(actual_lease["lease_id"])
            and actual_lease["lease_id"] == actual_lease["lease_id"].strip()
            and type(actual_lease.get("generation")) is int
            and actual_lease["generation"] > 0
            and actual_lease.get("status") == "ACTIVE"
            and type(actual_lease.get("physical_subscription_id")) is int
            and actual_lease["physical_subscription_id"] > 0
        )
        if not exact_physical_owner:
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_CONSUMER_LEASE_OWNER_DRIFT",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "lease_owner_state": (
                    lease_snapshot.get("state") if isinstance(lease_snapshot, Mapping) else "INVALID_CARRIER"
                ),
                "lease_owner_reason_code": (
                    lease_snapshot.get("reason_code") if isinstance(lease_snapshot, Mapping) else None
                ),
                "lease_owner_readback_current": (
                    lease_snapshot.get("readback_current") if isinstance(lease_snapshot, Mapping) else False
                ),
                "lease_owner_registration_generation": (
                    lease_snapshot.get("registration_generation") if isinstance(lease_snapshot, Mapping) else None
                ),
                "expected_lease": dict(expected_lease) if isinstance(expected_lease, Mapping) else None,
                "actual_lease": dict(actual_lease) if isinstance(actual_lease, Mapping) else None,
                "broker_side_effect_state": "UNKNOWN",
            }
        try:
            current_sink = self._read_exact_observation_sink(
                supervisor=supervisor,
                consumer_id=consumer_id,
                symbols=state.symbols,
            )
        except Exception as exc:  # noqa: BLE001 - preserve exact owner readback failure.
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_SINK_OWNER_READBACK_FAILED",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "broker_side_effect_state": "UNKNOWN",
                **bounded_exception_summary_v1(exc),
            }
        if not callable(guarded_sink) or current_sink is not guarded_sink:
            return {
                "reason_code": "MINIQMT_K6_PRODUCT_SINK_OWNER_DRIFT",
                "runtime_id": runtime_id,
                "binding_id": binding_id,
                "consumer_id": consumer_id,
                "expected_sink_present": callable(guarded_sink),
                "actual_sink_present": current_sink is not None,
                "broker_side_effect_state": "UNKNOWN",
            }
        return None

    def health(self) -> dict[str, Any]:
        return self._health(include_dependency_health=True)

    def _health(self, *, include_dependency_health: bool) -> dict[str, Any]:
        with self._startup_recovery_lock:
            status = self.status
            production_ddl_gate_snapshot = self.production_ddl_gate
            reason_code = self.reason_code
            failure = dict(self.failure) if self.failure is not None else None
            controller_factory = self.controller_factory
            process_config = replace(self.config, enabled=self.process_switch_enabled)
            supervisor = self._current_supervisor()
        runtime_config = (
            controller_factory.runtime_config
            if isinstance(controller_factory, DrainOnlyB0QuoteV2ControllerFactory)
            else self.config
        )
        production_ddl_gate = (
            controller_factory.production_ddl_gate
            if isinstance(controller_factory, DrainOnlyB0QuoteV2ControllerFactory)
            else production_ddl_gate_snapshot
        )
        with self._kernel_retry_lock:
            runtime_snapshot = tuple(sorted(self._kernel_product_runtimes.items()))
            guarded_sink_snapshot = dict(self._kernel_guarded_sinks)
            retry_state_snapshot = dict(self._kernel_retry_states)
            watchdog_worker_snapshot = tuple(sorted(self._kernel_watchdog_workers.items(), key=lambda item: item[0]))
            callback_worker_snapshot = tuple(sorted(self._kernel_callback_workers.items(), key=lambda item: item[0]))
            callback_worker_by_runtime = dict(callback_worker_snapshot)
            release_worker_snapshot = tuple(sorted(self._kernel_release_workers.items(), key=lambda item: item[0]))
            supervisor_watchdog_worker = self._kernel_supervisor_watchdog_worker
            in_flight_owner_snapshot = tuple(sorted(self._kernel_product_in_flight.items()))
            in_flight_by_key = dict(in_flight_owner_snapshot)
            registration_drop_count_snapshot = dict(self._kernel_registration_drop_count_by_reason)
            last_registration_drop_snapshot = (
                dict(self._last_kernel_registration_drop) if self._last_kernel_registration_drop is not None else None
            )
            pending_drop_count_snapshot = dict(self._kernel_pending_drop_count_by_reason)
            last_pending_drop_snapshot = (
                dict(self._last_kernel_pending_drop) if self._last_kernel_pending_drop is not None else None
            )
            lifecycle_lock_count = len(self._kernel_runtime_lifecycle_locks)
            lifecycle_lock_user_count = sum(self._kernel_runtime_lifecycle_lock_users.values())
            retry_health: dict[str, dict[str, Any]] = {}
            for runtime_id, runtime in runtime_snapshot:
                state = self._kernel_retry_states.get(runtime_id)
                if state is None:
                    retry_health[runtime_id] = self._missing_retry_state_health(
                        runtime_id=runtime_id,
                        runtime=runtime,
                    )
                elif frozenset(state.operations) != _KERNEL_RETRY_OPERATIONS:
                    retry_health[runtime_id] = self._operation_retry_state_drift_health(
                        runtime_id=runtime_id,
                        runtime=runtime,
                        state=state,
                    )
                else:
                    retry_health[runtime_id] = state.as_health()
        for runtime_id, runtime in runtime_snapshot:
            guarded_sink = guarded_sink_snapshot.get(runtime_id)
            callback_worker = callback_worker_by_runtime.get(runtime_id)
            runtime_state = retry_state_snapshot.get(runtime_id)
            if runtime_state is None or retry_health[runtime_id].get("lifecycle_state") != _KERNEL_ACTIVE_LIFECYCLE:
                continue
            sink_failure = self._kernel_runtime_owner_failure(
                runtime_id=runtime_id,
                runtime=runtime,
                state=runtime_state,
                guarded_sink=guarded_sink,
                callback_worker=callback_worker,
                supervisor=supervisor,
            )
            if sink_failure is not None:
                with self._kernel_retry_lock:
                    still_active_owner = (
                        self._kernel_product_runtimes.get(runtime_id) is runtime
                        and self._kernel_retry_states.get(runtime_id) is retry_state_snapshot.get(runtime_id)
                        and retry_state_snapshot.get(runtime_id) is not None
                        and retry_state_snapshot[runtime_id].lifecycle_state == _KERNEL_ACTIVE_LIFECYCLE
                        and self._kernel_guarded_sinks.get(runtime_id) is guarded_sink
                    )
                if not still_active_owner:
                    continue
                retry_health[runtime_id] = self._sink_owner_drift_health(
                    retry_health=retry_health[runtime_id],
                    failure=sink_failure,
                )
        with self._kernel_retry_lock:
            retry_degraded = any(item["state"] != "HEALTHY" for item in retry_health.values())
        effective_status = (
            "STOPPED"
            if self._shutdown
            else ("SHUTDOWN_UNKNOWN" if self._shutdown_requested else ("DEGRADED" if retry_degraded else status))
        )
        payload: dict[str, Any] = {
            "schema_version": "miniqmt_quote_ingress_activation_v1",
            "status": effective_status,
            "enabled": self.process_switch_enabled,
            "data_session_key": self.data_session_key,
            "owner_mode": self.config.owner_mode,
            "process_config_sha256": quote_ingress_config_sha256(process_config),
            "runtime_config_sha256": quote_ingress_config_sha256(runtime_config),
            "runtime_config_enabled": runtime_config.enabled,
            "evidence_cadence_seconds": runtime_config.evidence_cadence_seconds,
            "production_ddl_gate": production_ddl_gate,
            "reason_code": reason_code,
            "factory_available": (
                controller_factory is not None and not self._shutdown and not self._shutdown_requested
            ),
            "failure": failure,
            "kernel_retry_active_count": sum(item["state"] != "HEALTHY" for item in retry_health.values()),
            "kernel_registration_drop_count_by_reason": dict(sorted(registration_drop_count_snapshot.items())),
            "last_kernel_registration_drop": last_registration_drop_snapshot,
            "kernel_pending_drop_count_by_reason": dict(sorted(pending_drop_count_snapshot.items())),
            "last_kernel_pending_drop": last_pending_drop_snapshot,
            "kernel_runtime_lifecycle_lock_count": lifecycle_lock_count,
            "kernel_runtime_lifecycle_lock_user_count": lifecycle_lock_user_count,
            "kernel_watchdog_active_count": len(watchdog_worker_snapshot),
            "kernel_watchdog_workers": [
                {
                    "runtime_id": runtime_id,
                    "binding_id": entry.binding_id,
                    "operation": entry.claim.operation,
                    "lifecycle_generation": entry.claim.lifecycle_generation,
                    "attempt_token": entry.claim.attempt_token,
                    "started_at_utc": entry.started_at_utc.astimezone(UTC).isoformat(),
                    "thread_alive": entry.thread.is_alive(),
                }
                for runtime_id, entry in watchdog_worker_snapshot
            ],
            "kernel_callback_worker_count": len(callback_worker_snapshot),
            "kernel_callback_active_count": sum(
                1
                for (_runtime_id, operation), _claim in in_flight_owner_snapshot
                if operation == _KERNEL_CALLBACK_OPERATION
            ),
            "kernel_callback_workers": [
                {
                    "runtime_id": runtime_id,
                    "binding_id": entry.binding_id,
                    "operation": _KERNEL_CALLBACK_OPERATION,
                    "lifecycle_generation": entry.lifecycle_generation,
                    "attempt_token": (
                        in_flight_by_key[(runtime_id, _KERNEL_CALLBACK_OPERATION)].attempt_token
                        if (runtime_id, _KERNEL_CALLBACK_OPERATION) in in_flight_by_key
                        else None
                    ),
                    "started_at_utc": entry.started_at_utc.astimezone(UTC).isoformat(),
                    "thread_alive": entry.thread.is_alive(),
                    "stop_requested": entry.stop_event.is_set(),
                    "stopped": entry.stopped_event.is_set(),
                    "queued_task_count": entry.task_queue.qsize(),
                    "active_attempt_token": entry.active_attempt_token,
                    "processed_count": entry.processed_count,
                    "last_failure": dict(entry.last_failure) if entry.last_failure is not None else None,
                }
                for runtime_id, entry in callback_worker_snapshot
            ],
            "kernel_auxiliary_workers": [
                {
                    "owner_key": entry.owner_key,
                    "runtime_id": entry.runtime_id,
                    "binding_id": entry.binding_id,
                    "operation": entry.operation,
                    "lifecycle_generation": entry.lifecycle_generation,
                    "started_at_utc": entry.started_at_utc.astimezone(UTC).isoformat(),
                    "thread_alive": entry.thread.is_alive(),
                }
                for entry in (
                    tuple(item[1] for item in release_worker_snapshot)
                    + ((supervisor_watchdog_worker,) if supervisor_watchdog_worker is not None else ())
                )
            ],
            "kernel_in_flight_attempts": [
                {
                    "runtime_id": runtime_id,
                    "operation": operation,
                    "lifecycle_generation": claim.lifecycle_generation,
                    "attempt_token": claim.attempt_token,
                }
                for (runtime_id, operation), claim in in_flight_owner_snapshot
            ],
        }
        if supervisor is not None and not self._shutdown:
            if include_dependency_health:
                payload["ingress"] = supervisor.health()
            else:
                payload["ingress"] = {
                    "schema_version": "miniqmt_quote_ingress_dependency_readback_v1",
                    "status": "READBACK_DEFERRED",
                    "readback_current": False,
                    "reason_code": "MINIQMT_QUOTE_INGRESS_CADENCE_DEPENDENCY_READBACK_DEFERRED",
                    "release_worker_runtime_ids": [runtime_id for runtime_id, _entry in release_worker_snapshot],
                    "supervisor_watchdog_worker_active": supervisor_watchdog_worker is not None,
                }
        context_adapter = self._current_context_adapter()
        if context_adapter is not None and not self._shutdown:
            payload["quote_context"] = context_adapter.health()
        if isinstance(controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            payload["drain_factory"] = controller_factory.health()
        payload["kernel_product_runtimes"] = [
            {
                "runtime_id": runtime_id,
                "binding_id": getattr(runtime, "binding_id", None),
                "trade_date": (
                    getattr(runtime, "trade_date").isoformat()
                    if getattr(runtime, "trade_date", None) is not None
                    else None
                ),
                "symbols": list(getattr(runtime, "symbols", ())),
                "source_capability_sha256": getattr(runtime, "source_capability_sha256", None),
                "ingress_retry": retry_health[runtime_id],
            }
            for runtime_id, runtime in runtime_snapshot
        ]
        return payload

    def begin_lifecycle_epoch(self) -> dict[str, Any]:
        if self._shutdown or self._shutdown_requested:
            raise RuntimeError("stopped or shutdown-fenced MiniQMT quote activation cannot begin a lifecycle epoch")
        self._recover_enabled_startup_if_needed()
        supervisor = self._current_supervisor()
        if supervisor is not None:
            supervisor.begin_lifecycle_epoch()
        return self._health(include_dependency_health=False)

    def _recover_enabled_startup_if_needed(self) -> None:
        if self.status != "BLOCKED" or not self.process_switch_enabled:
            return
        with self._startup_recovery_lock:
            if self.status != "BLOCKED" or not self.process_switch_enabled:
                return
            if (
                self._startup_schema_gate_reader is None
                or self._startup_subscriber_factory is None
                or self._startup_qmt_client_factory is None
                or self._startup_context_adapter_factory is None
            ):
                return
            try:
                production_ddl_gate = str(self._startup_schema_gate_reader())
            except Exception as exc:  # noqa: BLE001 - keep startup recovery fail-closed and visible
                self.production_ddl_gate = "readback_failed"
                self.reason_code = "MINIQMT_QUOTE_EVENT_SCHEMA_READBACK_FAILED"
                self.failure = {"exception_type": type(exc).__name__, "message": str(exc)}
                logger.error(
                    "MiniQMT quote ingress startup recovery schema readback failed; B0_QUOTE_V2 remains unavailable",
                    exc_info=True,
                )
                return
            self.production_ddl_gate = production_ddl_gate
            if production_ddl_gate != MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED:
                self.reason_code = "MINIQMT_QUOTE_EVENT_SCHEMA_NOT_APPLIED"
                self.failure = None
                return
            try:
                supervisor, controller_factory, context_adapter = _build_runtime_components(
                    runtime_config=self.config,
                    subscriber_factory=self._startup_subscriber_factory,
                    qmt_client_factory=self._startup_qmt_client_factory,
                    context_adapter_factory=self._startup_context_adapter_factory,
                )
            except Exception as exc:  # noqa: BLE001 - dependency construction remains typed and retryable
                self.reason_code = "MINIQMT_QUOTE_INGRESS_ACTIVATION_BUILD_FAILED"
                self.failure = {"exception_type": type(exc).__name__, "message": str(exc)}
                logger.error(
                    "MiniQMT quote ingress startup recovery dependency construction failed",
                    exc_info=True,
                )
                return
            self.supervisor = supervisor
            self.controller_factory = controller_factory
            self.context_adapter = context_adapter
            self.status = "READY"
            self.reason_code = None
            self.failure = None

    def _execute_kernel_watchdog_worker(
        self,
        *,
        entry: _KernelWatchdogWorker,
        tick: Callable[..., Any],
        observed_at: datetime,
        monotonic_ns: int,
    ) -> None:
        runtime_id = entry.runtime_id
        runtime = entry.runtime
        claim = entry.claim
        try:
            tick(observed_at=observed_at, monotonic_ns=monotonic_ns)
            completed_at_utc = self._kernel_retry_clock_utc()
            completed_at_monotonic_ns = self._kernel_retry_monotonic_ns()
            self._complete_kernel_product_attempt(
                claim=claim,
                succeeded=True,
                now_utc=completed_at_utc,
                now_monotonic_ns=completed_at_monotonic_ns,
            )
            runtime_trade_date = getattr(runtime, "trade_date", None)
            with self._kernel_retry_lock:
                retry_state = self._kernel_retry_states.get(runtime_id)
                if (
                    retry_state is not None
                    and runtime_trade_date is not None
                    and runtime_trade_date < completed_at_utc.astimezone(_CHINA_TZ).date()
                ):
                    self._expire_prior_day_quote_retry_locked(
                        retry_state,
                        resolved_at_utc=completed_at_utc,
                    )
                fully_healthy = retry_state is not None and retry_state.as_health()["state"] == "HEALTHY"
            entry.result_queue.put_nowait(
                (
                    "success",
                    {
                        "runtime_id": runtime_id,
                        "lifecycle_generation": claim.lifecycle_generation,
                        "attempt_token": claim.attempt_token,
                        "release_after_success": bool(
                            fully_healthy
                            and runtime_trade_date is not None
                            and runtime_trade_date < completed_at_utc.astimezone(_CHINA_TZ).date()
                        ),
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001 - worker preserves exact failure and releases its claim.
            with self._kernel_retry_lock:
                attempt_owner_is_claim = self._kernel_product_in_flight.get((runtime_id, claim.operation)) == claim
            if not attempt_owner_is_claim:
                failure_payload = {
                    "runtime_id": runtime_id,
                    "binding_id": entry.binding_id,
                    "reason_code": getattr(
                        exc,
                        "reason_code",
                        "MINIQMT_K6_PRODUCT_ATTEMPT_FINALIZATION_FAILED",
                    ),
                    "broker_side_effect_state": "UNKNOWN",
                    **bounded_exception_summary_v1(exc),
                }
            else:
                schema_failure = self._kernel_event_schema_failure(exc)
                failed_at_utc = self._kernel_retry_clock_utc()
                failed_at_monotonic_ns = self._kernel_retry_monotonic_ns()
                if schema_failure is not None:
                    evidence = self._record_kernel_schema_failure(
                        claim=claim,
                        runtime_id=runtime_id,
                        runtime=runtime,
                        operation=_KERNEL_WATCHDOG_OPERATION,
                        failure=schema_failure,
                        exception=exc,
                        now_utc=failed_at_utc,
                        now_monotonic_ns=failed_at_monotonic_ns,
                    )
                    failure_payload = {
                        "runtime_id": runtime_id,
                        "binding_id": entry.binding_id,
                        **evidence,
                    }
                else:
                    self._complete_kernel_product_attempt(
                        claim=claim,
                        succeeded=False,
                        now_utc=failed_at_utc,
                        now_monotonic_ns=failed_at_monotonic_ns,
                        exception=exc,
                    )
                    failure_payload = {
                        "runtime_id": runtime_id,
                        "binding_id": entry.binding_id,
                        "reason_code": getattr(
                            exc,
                            "reason_code",
                            "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED",
                        ),
                        "broker_side_effect_state": "UNKNOWN",
                        **bounded_exception_summary_v1(exc),
                    }
            failure_payload.update(
                {
                    "lifecycle_generation": claim.lifecycle_generation,
                    "attempt_token": claim.attempt_token,
                }
            )
            entry.result_queue.put_nowait(("failure", failure_payload))

    def _consume_kernel_watchdog_workers(
        self,
        *,
        wait_seconds: float,
        runtime_ids: frozenset[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[_KernelWatchdogWorker], set[str]]:
        deadline = monotonic_time.monotonic() + max(0.0, float(wait_seconds))
        with self._kernel_retry_lock:
            entries = tuple(
                entry
                for runtime_id, entry in self._kernel_watchdog_workers.items()
                if runtime_ids is None or runtime_id in runtime_ids
            )
        for entry in entries:
            remaining = deadline - monotonic_time.monotonic()
            if remaining > 0 and entry.thread.is_alive():
                entry.thread.join(timeout=remaining)
        failures: list[dict[str, Any]] = []
        release_after_success: list[_KernelWatchdogWorker] = []
        consumed_runtime_ids: set[str] = set()
        for entry in entries:
            try:
                outcome, payload = entry.result_queue.get_nowait()
            except queue.Empty:
                if entry.thread.is_alive():
                    continue
                missing = RuntimeError("KERNEL_V2 watchdog worker ended without an outcome carrier")
                try:
                    self._complete_kernel_product_attempt(
                        claim=entry.claim,
                        succeeded=False,
                        now_utc=self._kernel_retry_clock_utc(),
                        now_monotonic_ns=self._kernel_retry_monotonic_ns(),
                        exception=missing,
                    )
                except Exception as finalization_error:  # noqa: BLE001 - preserve both missing and owner drift.
                    missing = MiniQMTKernelProductRegistryRollbackError(
                        operation="WATCHDOG_WORKER_OUTCOME_MISSING",
                        primary=missing,
                        rollback=finalization_error,
                    )
                payload = {
                    "runtime_id": entry.runtime_id,
                    "binding_id": entry.binding_id,
                    "lifecycle_generation": entry.claim.lifecycle_generation,
                    "attempt_token": entry.claim.attempt_token,
                    "reason_code": "MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_OUTCOME_MISSING",
                    "broker_side_effect_state": "UNKNOWN",
                    **bounded_exception_summary_v1(missing),
                }
                outcome = "failure"
            with self._kernel_retry_lock:
                owner_drift = self._kernel_watchdog_workers.get(entry.runtime_id) is not entry
                if not owner_drift:
                    self._kernel_watchdog_workers.pop(entry.runtime_id, None)
            consumed_runtime_ids.add(entry.runtime_id)
            if owner_drift:
                failures.append(
                    {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "reason_code": "MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_OWNER_DRIFT",
                        "broker_side_effect_state": "UNKNOWN",
                        "expected_lifecycle_generation": entry.claim.lifecycle_generation,
                        "expected_attempt_token": entry.claim.attempt_token,
                        "exception_type": None,
                        "exception_message": "watchdog worker owner changed before result consumption",
                    }
                )
                continue
            if (
                payload.get("runtime_id") != entry.runtime_id
                or payload.get("lifecycle_generation") != entry.claim.lifecycle_generation
                or payload.get("attempt_token") != entry.claim.attempt_token
            ):
                outcome = "failure"
                payload = {
                    "runtime_id": entry.runtime_id,
                    "binding_id": entry.binding_id,
                    "reason_code": "MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_IDENTITY_DRIFT",
                    "broker_side_effect_state": "UNKNOWN",
                    "expected_lifecycle_generation": entry.claim.lifecycle_generation,
                    "actual_lifecycle_generation": payload.get("lifecycle_generation"),
                    "expected_attempt_token": entry.claim.attempt_token,
                    "actual_attempt_token": payload.get("attempt_token"),
                    "exception_type": None,
                    "exception_message": "watchdog worker outcome does not match its exact lifecycle claim",
                }
            if outcome == "failure":
                failures.append(payload)
            elif outcome == "success":
                if payload.get("release_after_success") is True:
                    release_after_success.append(entry)
            else:
                failures.append(
                    {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "reason_code": "MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_OUTCOME_INVALID",
                        "broker_side_effect_state": "UNKNOWN",
                        "outcome": outcome,
                        "exception_type": None,
                        "exception_message": "watchdog worker returned an unregistered outcome",
                    }
                )
        return failures, release_after_success, consumed_runtime_ids

    def _consume_runtime_watchdog_before_release(self, *, runtime_id: str) -> None:
        failures, _release_after_success, _consumed = self._consume_kernel_watchdog_workers(
            wait_seconds=float(self._kernel_watchdog_peer_wait_seconds),
            runtime_ids=frozenset({runtime_id}),
        )
        with self._kernel_retry_lock:
            worker_remains = runtime_id in self._kernel_watchdog_workers
        if worker_remains:
            raise MiniQMTKernelProductLifecycleError(
                reason_code="MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_DRAIN_TIMEOUT",
                message="KERNEL_V2 runtime release could not consume its completed watchdog owner",
                context={
                    "runtime_id": runtime_id,
                    "timeout_seconds": float(self._kernel_watchdog_peer_wait_seconds),
                },
            )
        if failures:
            raise MiniQMTKernelProductSyncError(tuple(failures))

    @staticmethod
    def _execute_kernel_auxiliary_worker(
        *,
        entry: _KernelAuxiliaryWorker,
        operation: Callable[[], Any],
        failure_reason_code: str,
    ) -> None:
        try:
            operation()
        except Exception as exc:  # noqa: BLE001 - exact auxiliary failure crosses the result carrier.
            entry.result_queue.put_nowait(
                (
                    "failure",
                    {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "operation": entry.operation,
                        "lifecycle_generation": entry.lifecycle_generation,
                        "reason_code": failure_reason_code,
                        "broker_side_effect_state": "UNKNOWN",
                        **bounded_exception_summary_v1(exc),
                    },
                )
            )
        else:
            entry.result_queue.put_nowait(
                (
                    "success",
                    {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "operation": entry.operation,
                        "lifecycle_generation": entry.lifecycle_generation,
                    },
                )
            )

    def _start_supervisor_watchdog_worker(self, *, supervisor: Any, observed_at: datetime) -> None:
        with self._kernel_retry_lock:
            if self._kernel_supervisor_watchdog_worker is not None:
                return
            result_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(maxsize=1)
            entry = _KernelAuxiliaryWorker(
                owner_key="shared-supervisor",
                operation="SUPERVISOR_WATCHDOG",
                runtime_id=None,
                binding_id=None,
                lifecycle_generation=None,
                thread=threading.current_thread(),
                result_queue=result_queue,
                started_at_utc=observed_at,
            )
            worker = threading.Thread(
                target=self._execute_kernel_auxiliary_worker,
                kwargs={
                    "entry": entry,
                    "operation": supervisor.watchdog_tick,
                    "failure_reason_code": "MINIQMT_SHARED_QUOTE_SUPERVISOR_WATCHDOG_FAILED",
                },
                name="miniqmt-shared-quote-supervisor-watchdog",
                daemon=True,
            )
            entry.thread = worker
            self._kernel_supervisor_watchdog_worker = entry
        worker.start()

    def _start_kernel_release_worker(
        self,
        *,
        runtime_id: str,
        runtime: Any,
        lifecycle_generation: int,
        observed_at: datetime,
        operation: str,
        failure_reason_code: str,
    ) -> None:
        with self._kernel_retry_lock:
            existing = self._kernel_release_workers.get(runtime_id)
            if existing is not None:
                if (
                    existing.lifecycle_generation != lifecycle_generation
                    or existing.runtime_id != runtime_id
                    or existing.operation != operation
                ):
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RELEASE_WORKER_OWNER_DRIFT",
                        message="KERNEL_V2 release worker differs from the requested lifecycle owner",
                        context={
                            "runtime_id": runtime_id,
                            "expected_lifecycle_generation": lifecycle_generation,
                            "actual_lifecycle_generation": existing.lifecycle_generation,
                            "expected_operation": operation,
                            "actual_operation": existing.operation,
                        },
                    )
                return
            result_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(maxsize=1)
            entry = _KernelAuxiliaryWorker(
                owner_key=runtime_id,
                operation=operation,
                runtime_id=runtime_id,
                binding_id=getattr(runtime, "binding_id", None),
                lifecycle_generation=lifecycle_generation,
                thread=threading.current_thread(),
                result_queue=result_queue,
                started_at_utc=observed_at,
            )
            worker = threading.Thread(
                target=self._execute_kernel_auxiliary_worker,
                kwargs={
                    "entry": entry,
                    "operation": lambda: self.release_kernel_product_runtime(runtime_id),
                    "failure_reason_code": failure_reason_code,
                },
                name=f"miniqmt-kernel-release-{operation.lower()}-{runtime_id}",
                daemon=True,
            )
            entry.thread = worker
            self._kernel_release_workers[runtime_id] = entry
        worker.start()

    def _start_release_unknown_worker(
        self,
        *,
        runtime_id: str,
        runtime: Any,
        lifecycle_generation: int,
        observed_at: datetime,
    ) -> None:
        self._start_kernel_release_worker(
            runtime_id=runtime_id,
            runtime=runtime,
            lifecycle_generation=lifecycle_generation,
            observed_at=observed_at,
            operation="RELEASE_UNKNOWN_RECONCILIATION",
            failure_reason_code="MINIQMT_K6_PRODUCT_RELEASE_RECONCILIATION_FAILED",
        )

    def _consume_kernel_auxiliary_workers(self, *, wait_seconds: float) -> list[dict[str, Any]]:
        deadline = monotonic_time.monotonic() + max(0.0, float(wait_seconds))
        with self._kernel_retry_lock:
            entries = tuple(self._kernel_release_workers.values()) + (
                ((self._kernel_supervisor_watchdog_worker,))
                if self._kernel_supervisor_watchdog_worker is not None
                else ()
            )
        for entry in entries:
            remaining = deadline - monotonic_time.monotonic()
            if remaining > 0 and entry.thread.is_alive():
                entry.thread.join(timeout=remaining)
        failures: list[dict[str, Any]] = []
        for entry in entries:
            try:
                outcome, payload = entry.result_queue.get_nowait()
            except queue.Empty:
                if entry.thread.is_alive():
                    continue
                outcome = "failure"
                payload = {
                    "runtime_id": entry.runtime_id,
                    "binding_id": entry.binding_id,
                    "operation": entry.operation,
                    "lifecycle_generation": entry.lifecycle_generation,
                    "reason_code": "MINIQMT_K6_PRODUCT_AUXILIARY_WORKER_OUTCOME_MISSING",
                    "broker_side_effect_state": "UNKNOWN",
                    "exception_type": None,
                    "exception_message": "auxiliary worker ended without an outcome carrier",
                }
            owner_failure: dict[str, Any] | None = None
            exact_runtime_id: str | None = None
            with self._kernel_retry_lock:
                if entry.operation == "SUPERVISOR_WATCHDOG":
                    if self._kernel_supervisor_watchdog_worker is entry:
                        self._kernel_supervisor_watchdog_worker = None
                    else:
                        owner_failure = {
                            "runtime_id": entry.runtime_id,
                            "binding_id": entry.binding_id,
                            "operation": entry.operation,
                            "lifecycle_generation": entry.lifecycle_generation,
                            "reason_code": "MINIQMT_SHARED_QUOTE_SUPERVISOR_WATCHDOG_OWNER_DRIFT",
                            "broker_side_effect_state": "UNKNOWN",
                            "exception_type": None,
                            "exception_message": "shared supervisor watchdog worker owner changed before result consumption",
                        }
                elif entry.operation in _KERNEL_RELEASE_OPERATIONS:
                    runtime_id_is_exact = (
                        type(entry.runtime_id) is str
                        and bool(entry.runtime_id)
                        and entry.runtime_id == entry.runtime_id.strip()
                        and type(entry.owner_key) is str
                        and entry.owner_key == entry.runtime_id
                    )
                    if runtime_id_is_exact:
                        exact_runtime_id = entry.runtime_id
                        if self._kernel_release_workers.get(exact_runtime_id) is entry:
                            self._kernel_release_workers.pop(exact_runtime_id)
                        else:
                            owner_failure = {
                                "runtime_id": entry.runtime_id,
                                "binding_id": entry.binding_id,
                                "operation": entry.operation,
                                "lifecycle_generation": entry.lifecycle_generation,
                                "reason_code": "MINIQMT_K6_PRODUCT_RELEASE_WORKER_OWNER_DRIFT",
                                "broker_side_effect_state": "UNKNOWN",
                                "exception_type": None,
                                "exception_message": "release worker owner changed before result consumption",
                            }
                    else:
                        for registry_runtime_id, candidate in tuple(self._kernel_release_workers.items()):
                            if candidate is entry:
                                self._kernel_release_workers.pop(registry_runtime_id)
                                if (
                                    type(registry_runtime_id) is str
                                    and bool(registry_runtime_id)
                                    and registry_runtime_id == registry_runtime_id.strip()
                                ):
                                    self._maybe_drop_kernel_runtime_lifecycle_lock_locked(registry_runtime_id)
                        owner_failure = {
                            "runtime_id": entry.runtime_id,
                            "binding_id": entry.binding_id,
                            "operation": entry.operation,
                            "lifecycle_generation": entry.lifecycle_generation,
                            "reason_code": "MINIQMT_K6_PRODUCT_AUXILIARY_WORKER_RUNTIME_IDENTITY_DRIFT",
                            "runtime_id_type": (
                                f"{type(entry.runtime_id).__module__}.{type(entry.runtime_id).__qualname__}"
                            ),
                            "broker_side_effect_state": "UNKNOWN",
                            "exception_type": None,
                            "exception_message": "release worker runtime identity is not an exact canonical string",
                        }
                else:
                    for registry_runtime_id, candidate in tuple(self._kernel_release_workers.items()):
                        if candidate is entry:
                            self._kernel_release_workers.pop(registry_runtime_id)
                            if (
                                type(registry_runtime_id) is str
                                and bool(registry_runtime_id)
                                and registry_runtime_id == registry_runtime_id.strip()
                            ):
                                self._maybe_drop_kernel_runtime_lifecycle_lock_locked(registry_runtime_id)
                    owner_failure = {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "operation": entry.operation,
                        "lifecycle_generation": entry.lifecycle_generation,
                        "reason_code": "MINIQMT_K6_PRODUCT_AUXILIARY_WORKER_OPERATION_INVALID",
                        "broker_side_effect_state": "UNKNOWN",
                        "exception_type": None,
                        "exception_message": "auxiliary worker operation is not registered",
                    }
                if owner_failure is not None:
                    outcome = "failure"
                    payload = owner_failure
                elif (
                    payload.get("operation") != entry.operation
                    or payload.get("lifecycle_generation") != entry.lifecycle_generation
                    or (entry.operation == "SUPERVISOR_WATCHDOG" and payload.get("runtime_id") is not None)
                    or (
                        entry.operation in _KERNEL_RELEASE_OPERATIONS
                        and (
                            type(payload.get("runtime_id")) is not str or payload.get("runtime_id") != exact_runtime_id
                        )
                    )
                ):
                    outcome = "failure"
                    payload = {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "operation": entry.operation,
                        "lifecycle_generation": entry.lifecycle_generation,
                        "reason_code": "MINIQMT_K6_PRODUCT_AUXILIARY_WORKER_IDENTITY_DRIFT",
                        "broker_side_effect_state": "UNKNOWN",
                        "exception_type": None,
                        "exception_message": "auxiliary worker outcome differs from its exact owner",
                    }
                if outcome == "success" and exact_runtime_id is not None:
                    release_closed = (
                        self._kernel_product_runtimes.get(exact_runtime_id) is None
                        and self._kernel_retry_states.get(exact_runtime_id) is None
                        and self._kernel_guarded_sinks.get(exact_runtime_id) is None
                        and self._kernel_callback_workers.get(exact_runtime_id) is None
                        and self._kernel_watchdog_workers.get(exact_runtime_id) is None
                        and not any(
                            owner_runtime_id == exact_runtime_id
                            for owner_runtime_id, _ in self._kernel_product_in_flight
                        )
                    )
                    if not release_closed:
                        outcome = "failure"
                        payload = {
                            "runtime_id": entry.runtime_id,
                            "binding_id": entry.binding_id,
                            "operation": entry.operation,
                            "lifecycle_generation": entry.lifecycle_generation,
                            "reason_code": "MINIQMT_K6_PRODUCT_RELEASE_RECONCILIATION_NOT_CLOSED",
                            "broker_side_effect_state": "UNKNOWN",
                            "exception_type": None,
                            "exception_message": "release worker returned without removing its exact owner graph",
                        }
                if exact_runtime_id is not None:
                    self._maybe_drop_kernel_runtime_lifecycle_lock_locked(exact_runtime_id)
            if outcome == "failure":
                failures.append(payload)
            elif outcome != "success":
                failures.append(
                    {
                        "runtime_id": entry.runtime_id,
                        "binding_id": entry.binding_id,
                        "operation": entry.operation,
                        "lifecycle_generation": entry.lifecycle_generation,
                        "reason_code": "MINIQMT_K6_PRODUCT_AUXILIARY_WORKER_OUTCOME_INVALID",
                        "broker_side_effect_state": "UNKNOWN",
                        "outcome": outcome,
                        "exception_type": None,
                        "exception_message": "auxiliary worker returned an unregistered outcome",
                    }
                )
        return failures

    def watchdog_tick(self) -> dict[str, Any]:
        if self._shutdown or self._shutdown_requested:
            raise RuntimeError("stopped or shutdown-fenced MiniQMT quote activation cannot run watchdog")
        failures: list[dict[str, Any]] = []
        failures.extend(self._consume_kernel_auxiliary_workers(wait_seconds=0.0))
        observed_at = self._kernel_retry_clock_utc()
        monotonic_ns = self._kernel_retry_monotonic_ns()
        supervisor = self._current_supervisor()
        if supervisor is not None:
            self._start_supervisor_watchdog_worker(supervisor=supervisor, observed_at=observed_at)
        with self._kernel_retry_lock:
            release_unknown_snapshot = tuple(
                sorted(
                    (runtime_id, self._kernel_product_runtimes.get(runtime_id), state.lifecycle_generation)
                    for runtime_id, state in self._kernel_retry_states.items()
                    if state.lifecycle_state == _KERNEL_RELEASE_UNKNOWN_LIFECYCLE
                )
            )
        for runtime_id, runtime, lifecycle_generation in release_unknown_snapshot:
            if runtime is None:
                failures.append(
                    {
                        "runtime_id": runtime_id,
                        "binding_id": None,
                        "reason_code": "MINIQMT_K6_PRODUCT_RELEASE_RECONCILIATION_OWNER_MISSING",
                        "broker_side_effect_state": "UNKNOWN",
                        "exception_type": None,
                        "exception_message": "release-unknown lifecycle has no exact runtime owner",
                    }
                )
                continue
            self._start_release_unknown_worker(
                runtime_id=runtime_id,
                runtime=runtime,
                lifecycle_generation=lifecycle_generation,
                observed_at=observed_at,
            )
        completed_failures, release_after_success, consumed_runtime_ids = self._consume_kernel_watchdog_workers(
            wait_seconds=0.0
        )
        failures.extend(completed_failures)
        with self._kernel_retry_lock:
            candidate_snapshot = tuple(sorted(self._kernel_product_runtimes.items()))
            runtime_snapshot: list[
                tuple[
                    str,
                    Any,
                    _KernelProductIngressRetryState,
                    Callable[..., Any] | None,
                    _KernelCallbackWorker | None,
                ]
            ] = []
            lifecycle_generations: dict[str, int] = {}
            for runtime_id, runtime in candidate_snapshot:
                state = self._kernel_retry_states.get(runtime_id)
                if state is None:
                    failures.append(
                        {
                            "runtime_id": runtime_id,
                            "binding_id": getattr(runtime, "binding_id", None),
                            "reason_code": "MINIQMT_K6_PRODUCT_RETRY_STATE_MISSING",
                            "broker_side_effect_state": "UNKNOWN",
                            "exception_type": "MiniQMTKernelProductLifecycleError",
                            "exception_message": "registered runtime has no retry/lifecycle owner",
                        }
                    )
                    continue
                if frozenset(state.operations) != _KERNEL_RETRY_OPERATIONS:
                    failures.append(
                        {
                            "runtime_id": runtime_id,
                            "binding_id": getattr(runtime, "binding_id", None),
                            "reason_code": "MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING",
                            "expected_operations": sorted(_KERNEL_RETRY_OPERATIONS),
                            "actual_operations": sorted(state.operations),
                            "broker_side_effect_state": "UNKNOWN",
                            "exception_type": "MiniQMTKernelProductLifecycleError",
                            "exception_message": "registered runtime lost a mandatory operation retry owner",
                        }
                    )
                    continue
                if state.lifecycle_state == _KERNEL_RELEASE_UNKNOWN_LIFECYCLE:
                    continue
                runtime_snapshot.append(
                    (
                        runtime_id,
                        runtime,
                        state,
                        self._kernel_guarded_sinks.get(runtime_id),
                        self._kernel_callback_workers.get(runtime_id),
                    )
                )
                lifecycle_generations[runtime_id] = state.lifecycle_generation
        for runtime_id, runtime, state, guarded_sink, callback_worker in runtime_snapshot:
            if state.lifecycle_state == _KERNEL_ACTIVE_LIFECYCLE:
                owner_failure = self._kernel_runtime_owner_failure(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    state=state,
                    guarded_sink=guarded_sink,
                    callback_worker=callback_worker,
                    supervisor=supervisor,
                )
                if owner_failure is not None:
                    with self._kernel_retry_lock:
                        still_active_owner = (
                            self._kernel_product_runtimes.get(runtime_id) is runtime
                            and self._kernel_retry_states.get(runtime_id) is state
                            and state.lifecycle_state == _KERNEL_ACTIVE_LIFECYCLE
                            and self._kernel_guarded_sinks.get(runtime_id) is guarded_sink
                            and self._kernel_callback_workers.get(runtime_id) is callback_worker
                        )
                    if still_active_owner:
                        failures.append(owner_failure)
                    continue
            with self._kernel_retry_lock:
                worker_in_progress = runtime_id in self._kernel_watchdog_workers
            if runtime_id in consumed_runtime_ids or worker_in_progress:
                continue
            binding_id = getattr(runtime, "binding_id", None)
            tick = getattr(runtime, "scheduler_tick_v1", None)
            if not callable(tick):
                failures.append(
                    {
                        "runtime_id": runtime_id,
                        "binding_id": binding_id,
                        "reason_code": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_MISSING",
                        "exception_type": None,
                        "exception_message": "registered runtime lacks callback and exchange-clock tick ingress",
                    }
                )
                continue
            try:
                claim = self._claim_kernel_product_attempt(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    operation=_KERNEL_WATCHDOG_OPERATION,
                    lifecycle_generation=lifecycle_generations[runtime_id],
                    now_utc=observed_at,
                    now_monotonic_ns=monotonic_ns,
                )
            except MiniQMTKernelProductLifecycleError as exc:
                failures.append(
                    {
                        "runtime_id": runtime_id,
                        "binding_id": binding_id,
                        "reason_code": exc.reason_code,
                        **bounded_exception_summary_v1(exc),
                    }
                )
                continue
            if isinstance(claim, _KernelProductWatchdogSuppression):
                failures.append(
                    {
                        "runtime_id": runtime_id,
                        "binding_id": binding_id,
                        "reason_code": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_SUPPRESSED",
                        "broker_side_effect_state": "UNKNOWN",
                        "suppression": claim.as_dict(),
                        "exception_type": None,
                        "exception_message": "KERNEL_V2 scheduler tick was not executed by the retry owner",
                    }
                )
                continue
            result_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(maxsize=1)
            entry = _KernelWatchdogWorker(
                runtime_id=runtime_id,
                binding_id=binding_id,
                runtime=runtime,
                claim=claim,
                thread=threading.current_thread(),
                result_queue=result_queue,
                started_at_utc=observed_at,
            )
            worker = threading.Thread(
                target=self._execute_kernel_watchdog_worker,
                kwargs={
                    "entry": entry,
                    "tick": tick,
                    "observed_at": observed_at,
                    "monotonic_ns": monotonic_ns,
                },
                name=f"miniqmt-kernel-watchdog-{runtime_id}",
                daemon=True,
            )
            entry.thread = worker
            with self._kernel_retry_lock:
                if runtime_id in self._kernel_watchdog_workers:
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_WATCHDOG_WORKER_DUPLICATE",
                        message="KERNEL_V2 watchdog worker already owns this runtime",
                        context={"runtime_id": runtime_id},
                    )
                self._kernel_watchdog_workers[runtime_id] = entry
            worker.start()
        completed_failures, completed_releases, _consumed_runtime_ids = self._consume_kernel_watchdog_workers(
            wait_seconds=float(self._kernel_watchdog_peer_wait_seconds),
        )
        failures.extend(completed_failures)
        release_after_success.extend(completed_releases)
        failures.extend(
            self._consume_kernel_auxiliary_workers(wait_seconds=float(self._kernel_watchdog_peer_wait_seconds))
        )
        for release_entry in release_after_success:
            runtime_id = release_entry.runtime_id
            with self._kernel_retry_lock:
                runtime = self._kernel_product_runtimes.get(runtime_id)
                retry_state = self._kernel_retry_states.get(runtime_id)
            if (
                runtime is not release_entry.runtime
                or retry_state is None
                or retry_state.lifecycle_generation != release_entry.claim.lifecycle_generation
            ):
                failures.append(
                    {
                        "runtime_id": runtime_id,
                        "binding_id": release_entry.binding_id,
                        "reason_code": "MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_RACE",
                        "broker_side_effect_state": "UNKNOWN",
                        "expected_lifecycle_generation": release_entry.claim.lifecycle_generation,
                        "actual_lifecycle_generation": (
                            retry_state.lifecycle_generation if retry_state is not None else None
                        ),
                        "exception_type": None,
                        "exception_message": ("runtime owner changed before exact prior-day watchdog release"),
                    }
                )
                continue
            try:
                self._start_kernel_release_worker(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    lifecycle_generation=release_entry.claim.lifecycle_generation,
                    observed_at=observed_at,
                    operation="PRIOR_DAY_RELEASE",
                    failure_reason_code="MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_FAILED",
                )
            except Exception as exc:  # noqa: BLE001 - retain peer isolation and surface exact release failure.
                failures.append(
                    {
                        "runtime_id": runtime_id,
                        "binding_id": getattr(runtime, "binding_id", None),
                        "reason_code": "MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_FAILED",
                        "broker_side_effect_state": "UNKNOWN",
                        **bounded_exception_summary_v1(exc),
                    }
                )
        failures.extend(
            self._consume_kernel_auxiliary_workers(wait_seconds=float(self._kernel_watchdog_peer_wait_seconds))
        )
        if failures:
            raise MiniQMTKernelProductSyncError(tuple(failures))
        return self._health(include_dependency_health=False)

    def shutdown(self) -> dict[str, Any]:
        with self._kernel_retry_condition:
            if self._shutdown and not self._kernel_product_runtimes:
                return self.health()
            self._shutdown_requested = True
            self._kernel_retry_condition.notify_all()
        auxiliary_failures = self._consume_kernel_auxiliary_workers(
            wait_seconds=float(self._kernel_lifecycle_drain_timeout_seconds)
        )
        with self._kernel_retry_lock:
            active_auxiliary_workers = sorted(self._kernel_release_workers)
            supervisor_worker_active = self._kernel_supervisor_watchdog_worker is not None
        if auxiliary_failures or active_auxiliary_workers or supervisor_worker_active:
            raise MiniQMTKernelProductLifecycleError(
                reason_code="MINIQMT_K6_PRODUCT_SHUTDOWN_AUXILIARY_OWNER_ACTIVE",
                message="KERNEL_V2 activation shutdown requires every auxiliary worker outcome to close first",
                context={
                    "ordered_failures": auxiliary_failures,
                    "release_worker_runtime_ids": active_auxiliary_workers,
                    "supervisor_watchdog_worker_active": supervisor_worker_active,
                },
            )
        with self._kernel_lifecycle_lock:
            with self._kernel_retry_condition:
                runtime_ids = tuple(sorted(self._kernel_product_runtimes))
            if self.controller_factory is not None and not isinstance(
                self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory
            ):
                self.controller_factory.set_accept_new_assignments(False)
            release_failures: list[dict[str, Any]] = []
            for runtime_id in runtime_ids:
                try:
                    self.release_kernel_product_runtime(runtime_id)
                except Exception as exc:  # noqa: BLE001 - attempt every owned runtime before failing shutdown.
                    release_failures.append(
                        {
                            "runtime_id": runtime_id,
                            "reason_code": getattr(
                                exc,
                                "reason_code",
                                "MINIQMT_K6_PRODUCT_SHUTDOWN_RUNTIME_RELEASE_FAILED",
                            ),
                            "broker_side_effect_state": "UNKNOWN",
                            **bounded_exception_summary_v1(exc),
                        }
                    )
            if release_failures:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_SHUTDOWN_RUNTIME_RELEASE_FAILED",
                    message="KERNEL_V2 activation shutdown could not exactly release every runtime",
                    context={"ordered_failures": release_failures},
                )
            if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
                self.controller_factory.shutdown()
            elif self.supervisor is not None:
                self.supervisor.shutdown()
            with self._kernel_retry_condition:
                if (
                    self._kernel_product_runtimes
                    or self._kernel_retry_states
                    or self._kernel_guarded_sinks
                    or self._kernel_product_in_flight
                    or self._kernel_watchdog_workers
                    or self._kernel_callback_workers
                    or self._kernel_release_workers
                    or self._kernel_supervisor_watchdog_worker is not None
                    or self._kernel_runtime_lifecycle_locks
                    or self._kernel_runtime_lifecycle_lock_users
                ):
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_SHUTDOWN_OWNER_DRIFT",
                        message="KERNEL_V2 activation retained an in-flight owner after exact runtime release",
                        context={
                            "in_flight_runtime_ids": sorted(
                                {runtime_id for runtime_id, _operation in self._kernel_product_in_flight}
                            ),
                            "in_flight_attempts": [
                                {"runtime_id": runtime_id, "operation": operation}
                                for runtime_id, operation in sorted(self._kernel_product_in_flight)
                            ],
                            "watchdog_worker_runtime_ids": sorted(self._kernel_watchdog_workers),
                            "callback_worker_runtime_ids": sorted(self._kernel_callback_workers),
                            "release_worker_runtime_ids": sorted(self._kernel_release_workers),
                            "supervisor_watchdog_worker_active": (self._kernel_supervisor_watchdog_worker is not None),
                            "runtime_lifecycle_lock_ids": sorted(self._kernel_runtime_lifecycle_locks),
                            "runtime_lifecycle_lock_users": dict(
                                sorted(self._kernel_runtime_lifecycle_lock_users.items())
                            ),
                        },
                    )
                self._shutdown = True
        return self.health()

    def register_kernel_product_runtime(self, *, runtime: Any, symbols: tuple[str, ...]) -> Any:
        """Attach one final KERNEL_V2 source publisher to the shared physical feed."""

        raw_runtime_id = getattr(runtime, "runtime_id", None)
        if type(raw_runtime_id) is not str or not raw_runtime_id or raw_runtime_id != raw_runtime_id.strip():
            raise TypeError("kernel product runtime_id must be an exact canonical string identity")
        runtime_id = raw_runtime_id
        sink = getattr(runtime, "observe_b0_quote_v1", None)
        if not runtime_id or not callable(sink):
            raise TypeError("kernel product runtime must expose identity and observe_b0_quote_v1")
        if (
            type(symbols) is not tuple
            or not symbols
            or len(symbols) != len(set(symbols))
            or any(type(item) is not str or not item or item != item.strip() for item in symbols)
        ):
            raise ValueError("kernel product runtime requires a non-empty exact unique symbol tuple")
        normalized_symbols = symbols
        if tuple(getattr(runtime, "symbols", ())) != normalized_symbols:
            raise RuntimeError("kernel product runtime registration differs from its frozen symbol owner")
        with self._kernel_lifecycle_lock:
            with self._kernel_retry_condition:
                if self._shutdown or self._shutdown_requested:
                    raise RuntimeError(
                        "stopped or shutdown-fenced MiniQMT quote activation cannot register a product runtime"
                    )
                existing = self._kernel_product_runtimes.get(runtime_id)
                if existing is None and runtime_id in self._kernel_release_workers:
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RELEASE_WORKER_NOT_CONSUMED",
                        message="KERNEL_V2 runtime id cannot be reused before its release outcome is consumed",
                        context={"runtime_id": runtime_id},
                    )
                if existing is not None:
                    if existing is not runtime:
                        raise RuntimeError("kernel product runtime identity is already registered to another owner")
                    if tuple(getattr(existing, "symbols", normalized_symbols)) != normalized_symbols:
                        raise RuntimeError("kernel product runtime registration symbol set drifted")
                    state = self._retry_state_for(runtime_id, existing)
                    guarded_sink = self._kernel_guarded_sinks.get(runtime_id)
                    callback_worker = self._kernel_callback_workers.get(runtime_id)
                    if (
                        state.lifecycle_state != _KERNEL_ACTIVE_LIFECYCLE
                        or frozenset(state.operations) != _KERNEL_RETRY_OPERATIONS
                        or not callable(guarded_sink)
                        or callback_worker is None
                        or callback_worker.runtime is not existing
                        or callback_worker.lifecycle_generation != state.lifecycle_generation
                        or callback_worker.stop_event.is_set()
                        or callback_worker.stopped_event.is_set()
                        or not callback_worker.thread.is_alive()
                    ):
                        raise MiniQMTKernelProductLifecycleError(
                            reason_code="MINIQMT_K6_PRODUCT_REGISTRATION_OWNER_INCOMPLETE",
                            message="existing KERNEL_V2 runtime registration is not an exact ACTIVE sink owner",
                            context={
                                "runtime_id": runtime_id,
                                "lifecycle_generation": state.lifecycle_generation,
                                "lifecycle_state": state.lifecycle_state,
                                "expected_operations": sorted(_KERNEL_RETRY_OPERATIONS),
                                "actual_operations": sorted(state.operations),
                                "guarded_sink_present": callable(guarded_sink),
                                "callback_worker_present": callback_worker is not None,
                                "callback_worker_alive": (
                                    callback_worker.thread.is_alive() if callback_worker is not None else False
                                ),
                            },
                        )
                    supervisor = self._current_supervisor()
                    if supervisor is None:
                        raise MiniQMTKernelProductLifecycleError(
                            reason_code="MINIQMT_K6_PRODUCT_REGISTRATION_LEASE_MISSING",
                            message="existing KERNEL_V2 runtime registration has no quote supervisor",
                            context={"runtime_id": runtime_id, "consumer_id": f"k6d-kernel-v2:{runtime_id}"},
                        )
                    consumer_id = f"k6d-kernel-v2:{runtime_id}"
                    owner_failure = self._kernel_runtime_owner_failure(
                        runtime_id=runtime_id,
                        runtime=existing,
                        state=state,
                        guarded_sink=guarded_sink,
                        callback_worker=callback_worker,
                        supervisor=supervisor,
                    )
                    if owner_failure is not None:
                        owner_reason_code = owner_failure.get("reason_code")
                        raise MiniQMTKernelProductLifecycleError(
                            reason_code=(
                                "MINIQMT_K6_PRODUCT_REGISTRATION_SINK_OWNER_MISSING"
                                if owner_reason_code == "MINIQMT_K6_PRODUCT_SINK_OWNER_DRIFT"
                                else "MINIQMT_K6_PRODUCT_REGISTRATION_OWNER_INCOMPLETE"
                            ),
                            message="existing KERNEL_V2 registration lacks its exact active owner graph",
                            context={
                                "runtime_id": runtime_id,
                                "consumer_id": consumer_id,
                                "owner_failure": owner_failure,
                            },
                        )
                    return existing
                self._kernel_lifecycle_generation += 1
                lifecycle_generation = self._kernel_lifecycle_generation
                self._kernel_product_runtimes[runtime_id] = runtime
                self._kernel_retry_states[runtime_id] = self._new_retry_state(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    lifecycle_generation=lifecycle_generation,
                )
            supervisor = self._current_supervisor()
            if supervisor is None:
                with self._kernel_retry_condition:
                    self._drop_kernel_registration_locked(
                        runtime_id,
                        lifecycle_generation,
                        expected_runtime=runtime,
                        expected_lifecycle_state=_KERNEL_ACTIVE_LIFECYCLE,
                        reason="REGISTRATION_SUPERVISOR_MISSING",
                    )
                raise RuntimeError("KERNEL_V2 product publisher requires the active B0 quote supervisor")
            consumer_id = f"k6d-kernel-v2:{runtime_id}"

            guarded_sink = _KernelProductGuardedSink(
                activation=self,
                runtime_id=runtime_id,
                consumer_id=consumer_id,
                runtime=runtime,
                sink=sink,
                lifecycle_generation=lifecycle_generation,
            )

            with self._kernel_retry_condition:
                self._kernel_guarded_sinks[runtime_id] = guarded_sink
            sink_registered = False
            callback_worker_started = False
            consumer_acquired = False
            try:
                self._start_kernel_callback_worker(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    lifecycle_generation=lifecycle_generation,
                    sink=sink,
                    started_at_utc=self._kernel_retry_clock_utc(),
                )
                callback_worker_started = True
                supervisor.register_observation_sink(
                    consumer_id=consumer_id,
                    symbols=normalized_symbols,
                    sink=guarded_sink,
                )
                sink_registered = True
                try:
                    supervisor.acquire_consumer(consumer_id=consumer_id, symbols=list(normalized_symbols))
                except Exception as acquire_failure:
                    failure_context = getattr(acquire_failure, "context", None)
                    consumer_acquired = bool(
                        isinstance(failure_context, Mapping) and failure_context.get("consumer_lease_retained") is True
                    )
                    raise
                else:
                    consumer_acquired = True
                with self._kernel_retry_lock:
                    state = self._retry_state_for(runtime_id, runtime)
                    callback_worker = self._kernel_callback_workers.get(runtime_id)
                owner_failure = self._kernel_runtime_owner_failure(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    state=state,
                    guarded_sink=guarded_sink,
                    callback_worker=callback_worker,
                    supervisor=supervisor,
                )
                if owner_failure is not None:
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_REGISTRATION_OWNER_INCOMPLETE",
                        message="new KERNEL_V2 registration did not close its exact active owner graph",
                        context={"runtime_id": runtime_id, "owner_failure": owner_failure},
                    )
            except Exception as primary:
                rollback_failure: Exception | None = None
                if sink_registered:
                    try:
                        unregistered = supervisor.unregister_observation_sink(
                            consumer_id=consumer_id,
                            symbols=normalized_symbols,
                            sink=guarded_sink,
                        )
                        if unregistered is not True:
                            current_sink = self._read_exact_observation_sink(
                                supervisor=supervisor,
                                consumer_id=consumer_id,
                                symbols=normalized_symbols,
                            )
                            if current_sink is not None:
                                raise RuntimeError(
                                    "registration rollback did not unregister the exact observation sink"
                                )
                    except Exception as exc:  # noqa: BLE001 - preserve primary and rollback failures.
                        try:
                            current_sink = self._read_exact_observation_sink(
                                supervisor=supervisor,
                                consumer_id=consumer_id,
                                symbols=normalized_symbols,
                            )
                        except Exception as readback_error:  # noqa: BLE001 - retain both rollback failures.
                            rollback_failure = MiniQMTKernelProductRegistryRollbackError(
                                operation="REGISTER_ROLLBACK_SINK_READBACK",
                                primary=exc,
                                rollback=readback_error,
                            )
                        else:
                            rollback_failure = None if current_sink is None else exc
                if consumer_acquired:
                    try:
                        released = supervisor.release_consumer(consumer_id=consumer_id)
                        if released is not True:
                            snapshot_reader = getattr(supervisor, "consumer_lease_owner_snapshot", None)
                            snapshot = (
                                snapshot_reader(consumer_id=consumer_id, symbols=normalized_symbols)
                                if callable(snapshot_reader)
                                else None
                            )
                            if not isinstance(snapshot, Mapping) or snapshot.get("state") != "ABSENT":
                                raise RuntimeError(
                                    "registration rollback did not release the exact physical consumer lease"
                                )
                    except Exception as lease_rollback_error:  # noqa: BLE001 - aggregate exact graph rollback.
                        if rollback_failure is None:
                            rollback_failure = lease_rollback_error
                        else:
                            rollback_failure = MiniQMTKernelProductRegistryRollbackError(
                                operation="REGISTER_ROLLBACK_OWNER_GRAPH",
                                primary=rollback_failure,
                                rollback=lease_rollback_error,
                            )
                if rollback_failure is not None:
                    rollback_error = MiniQMTKernelProductRegistryRollbackError(
                        operation="REGISTER_ACQUIRE_CONSUMER",
                        primary=primary,
                        rollback=rollback_failure,
                    )
                    self._mark_release_unknown(
                        state=self._retry_state_for(runtime_id, runtime),
                        runtime_id=runtime_id,
                        lifecycle_generation=lifecycle_generation,
                        exception=rollback_error,
                    )
                    raise rollback_error from primary
                with self._kernel_retry_condition:
                    state = self._kernel_retry_states.get(runtime_id)
                    if state is not None:
                        state.lifecycle_state = _KERNEL_RELEASED_LIFECYCLE
                    try:
                        self._wait_for_runtime_drain_locked(
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            operation="REGISTER_ROLLBACK",
                        )
                    except MiniQMTKernelProductLifecycleError as drain_failure:
                        rollback_error = MiniQMTKernelProductRegistryRollbackError(
                            operation="REGISTER_ROLLBACK_DRAIN",
                            primary=primary,
                            rollback=drain_failure,
                        )
                        self._mark_release_unknown(
                            state=state,
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            exception=rollback_error,
                        )
                        raise rollback_error from primary
                if callback_worker_started:
                    try:
                        self._stop_kernel_callback_worker(
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            operation="REGISTER_ROLLBACK",
                        )
                    except Exception as worker_stop_failure:
                        rollback_error = MiniQMTKernelProductRegistryRollbackError(
                            operation="REGISTER_ROLLBACK_CALLBACK_WORKER",
                            primary=primary,
                            rollback=worker_stop_failure,
                        )
                        self._mark_release_unknown(
                            state=state,
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            exception=rollback_error,
                        )
                        raise rollback_error from primary
                with self._kernel_retry_condition:
                    self._drop_kernel_registration_locked(
                        runtime_id,
                        lifecycle_generation,
                        expected_runtime=runtime,
                        expected_lifecycle_state=_KERNEL_RELEASED_LIFECYCLE,
                        reason="REGISTRATION_ROLLBACK_CLOSED",
                    )
                raise
            return runtime

    def get_kernel_product_runtime(self, runtime_id: str) -> Any | None:
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise TypeError("runtime_id must be a canonical identity")
        with self._kernel_lifecycle_lock:
            with self._kernel_retry_lock:
                runtime = self._kernel_product_runtimes.get(runtime_id)
                if runtime is None:
                    return None
                state = self._retry_state_for(runtime_id, runtime)
                guarded_sink = self._kernel_guarded_sinks.get(runtime_id)
                callback_worker = self._kernel_callback_workers.get(runtime_id)
                if (
                    state.lifecycle_state != _KERNEL_ACTIVE_LIFECYCLE
                    or frozenset(state.operations) != _KERNEL_RETRY_OPERATIONS
                    or not callable(guarded_sink)
                    or callback_worker is None
                    or callback_worker.runtime is not runtime
                    or callback_worker.lifecycle_generation != state.lifecycle_generation
                    or callback_worker.stop_event.is_set()
                    or callback_worker.stopped_event.is_set()
                    or not callback_worker.thread.is_alive()
                ):
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RUNTIME_READBACK_OWNER_INCOMPLETE",
                        message="KERNEL_V2 runtime readback is not an exact ACTIVE sink owner",
                        context={
                            "runtime_id": runtime_id,
                            "lifecycle_state": state.lifecycle_state,
                            "expected_operations": sorted(_KERNEL_RETRY_OPERATIONS),
                            "actual_operations": sorted(state.operations),
                            "guarded_sink_present": callable(guarded_sink),
                            "callback_worker_present": callback_worker is not None,
                            "callback_worker_alive": (
                                callback_worker.thread.is_alive() if callback_worker is not None else False
                            ),
                        },
                    )
            supervisor = self._current_supervisor()
            if supervisor is None:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_RUNTIME_READBACK_LEASE_MISSING",
                    message="KERNEL_V2 runtime readback has no quote supervisor",
                    context={"runtime_id": runtime_id},
                )
            consumer_id = f"k6d-kernel-v2:{runtime_id}"
            owner_failure = self._kernel_runtime_owner_failure(
                runtime_id=runtime_id,
                runtime=runtime,
                state=state,
                guarded_sink=guarded_sink,
                callback_worker=callback_worker,
                supervisor=supervisor,
            )
            if owner_failure is not None:
                owner_reason_code = owner_failure.get("reason_code")
                raise MiniQMTKernelProductLifecycleError(
                    reason_code=(
                        "MINIQMT_K6_PRODUCT_RUNTIME_READBACK_SINK_OWNER_MISSING"
                        if owner_reason_code == "MINIQMT_K6_PRODUCT_SINK_OWNER_DRIFT"
                        else "MINIQMT_K6_PRODUCT_RUNTIME_READBACK_OWNER_INCOMPLETE"
                    ),
                    message="KERNEL_V2 runtime readback lacks its exact active owner graph",
                    context={
                        "runtime_id": runtime_id,
                        "consumer_id": consumer_id,
                        "owner_failure": owner_failure,
                    },
                )
            return runtime

    def _kernel_runtime_lifecycle_lock(self, runtime_id: str) -> threading.RLock:
        with self._kernel_retry_lock:
            lifecycle_lock = self._kernel_runtime_lifecycle_locks.get(runtime_id)
            if lifecycle_lock is None:
                lifecycle_lock = threading.RLock()
                self._kernel_runtime_lifecycle_locks[runtime_id] = lifecycle_lock
            return lifecycle_lock

    def _maybe_drop_kernel_runtime_lifecycle_lock_locked(self, runtime_id: str) -> None:
        lifecycle_lock = self._kernel_runtime_lifecycle_locks.get(runtime_id)
        if lifecycle_lock is None or self._kernel_runtime_lifecycle_lock_users.get(runtime_id, 0) > 0:
            return
        if (
            runtime_id in self._kernel_product_runtimes
            or runtime_id in self._kernel_retry_states
            or runtime_id in self._kernel_guarded_sinks
            or runtime_id in self._kernel_callback_workers
            or runtime_id in self._kernel_watchdog_workers
            or runtime_id in self._kernel_release_workers
            or any(owner_runtime_id == runtime_id for owner_runtime_id, _operation in self._kernel_product_in_flight)
        ):
            return
        if self._kernel_runtime_lifecycle_locks.get(runtime_id) is lifecycle_lock:
            self._kernel_runtime_lifecycle_locks.pop(runtime_id)

    @contextmanager
    def _kernel_runtime_lifecycle_guard(self, runtime_id: str) -> Iterator[None]:
        with self._kernel_retry_lock:
            lifecycle_lock = self._kernel_runtime_lifecycle_lock(runtime_id)
            self._kernel_runtime_lifecycle_lock_users[runtime_id] = (
                self._kernel_runtime_lifecycle_lock_users.get(runtime_id, 0) + 1
            )
        lifecycle_lock.acquire()
        try:
            yield
        finally:
            lifecycle_lock.release()
            with self._kernel_retry_lock:
                users = self._kernel_runtime_lifecycle_lock_users.get(runtime_id, 0)
                if users <= 1:
                    self._kernel_runtime_lifecycle_lock_users.pop(runtime_id, None)
                else:
                    self._kernel_runtime_lifecycle_lock_users[runtime_id] = users - 1
                self._maybe_drop_kernel_runtime_lifecycle_lock_locked(runtime_id)

    def release_kernel_product_runtime(self, runtime_id: str) -> None:
        """Release one completed prior-day product runtime from the shared feed."""

        with self._kernel_retry_lock:
            expected_runtime = self._kernel_product_runtime(runtime_id)
            expected_state = self._retry_state_for(runtime_id, expected_runtime)
            expected_lifecycle_generation = expected_state.lifecycle_generation
        with self._kernel_runtime_lifecycle_guard(runtime_id):
            with self._kernel_retry_condition:
                runtime = self._kernel_product_runtime(runtime_id)
                state = self._retry_state_for(runtime_id, runtime)
                if (
                    runtime is not expected_runtime
                    or state is not expected_state
                    or state.lifecycle_generation != expected_lifecycle_generation
                ):
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_OWNER_DRIFT",
                        message="KERNEL_V2 release request no longer owns its captured runtime generation",
                        context={
                            "runtime_id": runtime_id,
                            "expected_lifecycle_generation": expected_lifecycle_generation,
                            "actual_lifecycle_generation": state.lifecycle_generation,
                        },
                    )
                retrying_unknown_release = state.lifecycle_state == _KERNEL_RELEASE_UNKNOWN_LIFECYCLE
                if state.lifecycle_state not in {
                    _KERNEL_ACTIVE_LIFECYCLE,
                    _KERNEL_RELEASE_UNKNOWN_LIFECYCLE,
                }:
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_CONFLICT",
                        message="KERNEL_V2 product runtime is not eligible for exact release",
                        context={"runtime_id": runtime_id, "lifecycle_state": state.lifecycle_state},
                    )
                if not retrying_unknown_release:
                    state.lifecycle_state = _KERNEL_RELEASING_LIFECYCLE
                lifecycle_generation = state.lifecycle_generation
                guarded_sink = self._kernel_guarded_sinks.get(runtime_id)
            supervisor = self._current_supervisor()
            if supervisor is None:
                with self._kernel_retry_condition:
                    state.lifecycle_state = (
                        _KERNEL_RELEASE_UNKNOWN_LIFECYCLE if retrying_unknown_release else _KERNEL_ACTIVE_LIFECYCLE
                    )
                raise RuntimeError("KERNEL_V2 product runtime release requires the active B0 quote supervisor")
            consumer_id = f"k6d-kernel-v2:{runtime_id}"
            if retrying_unknown_release:
                try:
                    current_sink = self._read_exact_observation_sink(
                        supervisor=supervisor,
                        consumer_id=consumer_id,
                        symbols=state.symbols,
                    )
                    if current_sink is not None:
                        if current_sink is not guarded_sink:
                            raise RuntimeError(
                                "quote supervisor observation-sink owner changed during release reconciliation"
                            )
                        unregistered = supervisor.unregister_observation_sink(
                            consumer_id=consumer_id,
                            symbols=state.symbols,
                            sink=guarded_sink,
                        )
                        if unregistered is not True:
                            remaining_sink = self._read_exact_observation_sink(
                                supervisor=supervisor,
                                consumer_id=consumer_id,
                                symbols=state.symbols,
                            )
                            if remaining_sink is not None:
                                raise RuntimeError(
                                    "release reconciliation did not unregister the exact observation sink"
                                )
                    with self._kernel_retry_condition:
                        self._wait_for_runtime_drain_locked(
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            operation="RELEASE_UNKNOWN_RECONCILIATION",
                        )
                    self._consume_runtime_watchdog_before_release(runtime_id=runtime_id)
                except Exception as primary:
                    self._mark_release_unknown(
                        state=state,
                        runtime_id=runtime_id,
                        lifecycle_generation=lifecycle_generation,
                        exception=primary,
                    )
                    raise
            else:
                try:
                    unregistered = supervisor.unregister_observation_sink(
                        consumer_id=consumer_id,
                        symbols=state.symbols,
                        sink=guarded_sink,
                    )
                except Exception as primary:
                    self._rollback_release_sink(
                        supervisor=supervisor,
                        consumer_id=consumer_id,
                        guarded_sink=guarded_sink,
                        state=state,
                        primary=primary,
                        operation="RELEASE_UNREGISTER_SINK",
                    )
                    raise
                if unregistered is not True:
                    primary = MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RUNTIME_UNREGISTER_NOT_EXACT",
                        message="KERNEL_V2 product runtime sink was not unregistered exactly",
                        context={"runtime_id": runtime_id, "consumer_id": consumer_id},
                    )
                    self._rollback_release_sink(
                        supervisor=supervisor,
                        consumer_id=consumer_id,
                        guarded_sink=guarded_sink,
                        state=state,
                        primary=primary,
                        operation="RELEASE_UNREGISTER_SINK",
                    )
                    raise primary
                try:
                    with self._kernel_retry_condition:
                        self._wait_for_runtime_drain_locked(
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            operation="RELEASE",
                        )
                    self._consume_runtime_watchdog_before_release(runtime_id=runtime_id)
                except Exception as primary:
                    self._rollback_release_sink(
                        supervisor=supervisor,
                        consumer_id=consumer_id,
                        guarded_sink=guarded_sink,
                        state=state,
                        primary=primary,
                        operation="RELEASE_DRAIN_OR_WORKER_OUTCOME",
                    )
                    raise
            try:
                released = supervisor.release_consumer(consumer_id=consumer_id)
            except Exception as primary:
                if self._release_exception_outcome(primary) == "ACTIVE" and not retrying_unknown_release:
                    self._rollback_release_sink(
                        supervisor=supervisor,
                        consumer_id=consumer_id,
                        guarded_sink=guarded_sink,
                        state=state,
                        primary=primary,
                        operation="RELEASE_CONSUMER",
                    )
                else:
                    self._mark_release_unknown(
                        state=state,
                        runtime_id=runtime_id,
                        lifecycle_generation=lifecycle_generation,
                        exception=primary,
                    )
                raise
            if released is not True:
                if retrying_unknown_release and self._release_consumer_is_absent(
                    supervisor=supervisor,
                    consumer_id=consumer_id,
                ):
                    released = True
                else:
                    primary = MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_RUNTIME_RELEASE_NOT_EXACT",
                        message="KERNEL_V2 product runtime physical lease was not released exactly",
                        context={"runtime_id": runtime_id, "consumer_id": consumer_id},
                    )
                    if retrying_unknown_release:
                        self._mark_release_unknown(
                            state=state,
                            runtime_id=runtime_id,
                            lifecycle_generation=lifecycle_generation,
                            exception=primary,
                        )
                    else:
                        self._rollback_release_sink(
                            supervisor=supervisor,
                            consumer_id=consumer_id,
                            guarded_sink=guarded_sink,
                            state=state,
                            primary=primary,
                            operation="RELEASE_CONSUMER",
                        )
                    raise primary
            try:
                self._stop_kernel_callback_worker(
                    runtime_id=runtime_id,
                    lifecycle_generation=lifecycle_generation,
                    operation=("RELEASE_UNKNOWN_RECONCILIATION" if retrying_unknown_release else "RELEASE"),
                )
            except Exception as primary:
                self._mark_release_unknown(
                    state=state,
                    runtime_id=runtime_id,
                    lifecycle_generation=lifecycle_generation,
                    exception=primary,
                )
                raise
            with self._kernel_retry_condition:
                state.lifecycle_state = _KERNEL_RELEASED_LIFECYCLE
                if self._kernel_product_runtimes.get(runtime_id) is not runtime:
                    raise RuntimeError("KERNEL_V2 product runtime registry changed during exact release")
                self._drop_kernel_registration_locked(
                    runtime_id,
                    lifecycle_generation,
                    expected_runtime=runtime,
                    expected_lifecycle_state=_KERNEL_RELEASED_LIFECYCLE,
                    reason=("RELEASE_UNKNOWN_RECONCILED" if retrying_unknown_release else "RUNTIME_RELEASED"),
                )

    @staticmethod
    def _release_exception_outcome(exception: Exception) -> str:
        context = getattr(exception, "context", None)
        if isinstance(context, Mapping) and context.get("release_outcome") == "ACTIVE":
            return "ACTIVE"
        return "UNKNOWN"

    def _mark_release_unknown(
        self,
        *,
        state: _KernelProductIngressRetryState,
        runtime_id: str,
        lifecycle_generation: int,
        exception: Exception,
    ) -> None:
        with self._kernel_retry_condition:
            current = self._kernel_retry_states.get(runtime_id)
            if current is not state or state.lifecycle_generation != lifecycle_generation:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_RELEASE_UNKNOWN_OWNER_DRIFT",
                    message="KERNEL_V2 release-unknown owner changed before fencing",
                    context={"runtime_id": runtime_id, "lifecycle_generation": lifecycle_generation},
                ) from exception
            state.lifecycle_state = _KERNEL_RELEASE_UNKNOWN_LIFECYCLE
            state.last_failure = {
                "reason_code": "MINIQMT_K6_PRODUCT_RELEASE_OUTCOME_UNKNOWN",
                "runtime_id": runtime_id,
                "lifecycle_generation": lifecycle_generation,
                "observed_at_utc": self._kernel_retry_clock_utc().isoformat(),
                "broker_side_effect_state": "UNKNOWN",
                **bounded_exception_summary_v1(exception),
            }
            self._kernel_retry_condition.notify_all()

    def _retry_state_for(self, runtime_id: str, runtime: Any) -> _KernelProductIngressRetryState:
        state = self._kernel_retry_states.get(runtime_id)
        if state is None:
            raise MiniQMTKernelProductLifecycleError(
                reason_code="MINIQMT_K6_PRODUCT_RETRY_STATE_MISSING",
                message="registered KERNEL_V2 runtime has no exact retry/lifecycle owner",
                context={
                    "runtime_id": runtime_id,
                    "binding_id": getattr(runtime, "binding_id", None),
                },
            )
        return state

    @staticmethod
    def _new_retry_state(
        *,
        runtime_id: str,
        runtime: Any,
        lifecycle_generation: int,
    ) -> _KernelProductIngressRetryState:
        trade_date = getattr(runtime, "trade_date", None)
        return _KernelProductIngressRetryState(
            runtime_id=runtime_id,
            binding_id=getattr(runtime, "binding_id", None),
            trade_date=trade_date.isoformat() if trade_date is not None else None,
            source_capability_sha256=getattr(runtime, "source_capability_sha256", None),
            symbols=tuple(getattr(runtime, "symbols", ())),
            lifecycle_generation=lifecycle_generation,
        )

    @staticmethod
    def _missing_retry_state_health(*, runtime_id: str, runtime: Any) -> dict[str, Any]:
        failure = {
            "reason_code": "MINIQMT_K6_PRODUCT_RETRY_STATE_MISSING",
            "runtime_id": runtime_id,
            "binding_id": getattr(runtime, "binding_id", None),
            "broker_side_effect_state": "UNKNOWN",
        }
        return {
            "schema_version": "miniqmt_kernel_product_ingress_retry_v1",
            "runtime_id": runtime_id,
            "binding_id": getattr(runtime, "binding_id", None),
            "trade_date": (
                getattr(runtime, "trade_date").isoformat() if getattr(runtime, "trade_date", None) is not None else None
            ),
            "source_capability_sha256": getattr(runtime, "source_capability_sha256", None),
            "lifecycle_generation": None,
            "lifecycle_state": "OWNER_DRIFT",
            "state": "OWNER_DRIFT",
            "failure_operation": None,
            "failure_class": "LIFECYCLE_OWNER_DRIFT",
            "failure_fingerprint_sha256": None,
            "reason_code": failure["reason_code"],
            "sqlstate": None,
            "constraint_name": None,
            "first_failure_at_utc": None,
            "last_failure_at_utc": None,
            "last_attempt_at_utc": None,
            "next_retry_at_utc": None,
            "attempt_count": 0,
            "consecutive_failure_count": 0,
            "suppressed_callback_count": 0,
            "suppressed_watchdog_count": 0,
            "last_success_at_utc": None,
            "active_failure": failure,
            "last_failure": failure,
            "operations": {},
            "automatic_retry": False,
            "manual_ack_required": False,
            "business_gate": False,
            "broker_side_effect_state": "UNKNOWN",
        }

    @staticmethod
    def _operation_retry_state_drift_health(
        *,
        runtime_id: str,
        runtime: Any,
        state: _KernelProductIngressRetryState,
    ) -> dict[str, Any]:
        payload = MiniQMTQuoteIngressActivation._missing_retry_state_health(
            runtime_id=runtime_id,
            runtime=runtime,
        )
        reason_code = "MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING"
        failure = {
            "reason_code": reason_code,
            "runtime_id": runtime_id,
            "binding_id": getattr(runtime, "binding_id", None),
            "expected_operations": sorted(_KERNEL_RETRY_OPERATIONS),
            "actual_operations": sorted(state.operations),
            "broker_side_effect_state": "UNKNOWN",
        }
        payload.update(
            {
                "lifecycle_generation": state.lifecycle_generation,
                "lifecycle_state": "OWNER_DRIFT",
                "state": "OWNER_DRIFT",
                "reason_code": reason_code,
                "active_failure": failure,
                "last_failure": failure,
                "operations": {
                    operation: operation_state.as_health()
                    for operation, operation_state in sorted(state.operations.items())
                },
            }
        )
        return payload

    @staticmethod
    def _sink_owner_drift_health(
        *,
        retry_health: Mapping[str, Any],
        failure: Mapping[str, Any],
    ) -> dict[str, Any]:
        payload = dict(retry_health)
        payload.update(
            {
                "lifecycle_state": "OWNER_DRIFT",
                "state": "OWNER_DRIFT",
                "reason_code": failure.get("reason_code"),
                "active_failure": dict(failure),
                "last_failure": dict(failure),
                "automatic_retry": True,
                "manual_ack_required": False,
                "business_gate": False,
                "broker_side_effect_state": "UNKNOWN",
            }
        )
        return payload

    def _drop_kernel_registration_locked(
        self,
        runtime_id: str,
        lifecycle_generation: int,
        *,
        expected_runtime: Any,
        expected_lifecycle_state: str,
        reason: str,
    ) -> None:
        state = self._kernel_retry_states.get(runtime_id)
        runtime = self._kernel_product_runtimes.get(runtime_id)
        guarded_sink = self._kernel_guarded_sinks.get(runtime_id)
        if (
            runtime is not expected_runtime
            or state is None
            or state.lifecycle_generation != lifecycle_generation
            or state.lifecycle_state != expected_lifecycle_state
        ):
            raise RuntimeError("KERNEL_V2 exact registry cleanup owner or lifecycle state drifted")
        if reason != "REGISTRATION_SUPERVISOR_MISSING" and not callable(guarded_sink):
            raise RuntimeError("KERNEL_V2 exact registry cleanup lost its guarded sink owner")
        if self._runtime_generation_in_flight(runtime_id, lifecycle_generation):
            raise RuntimeError("KERNEL_V2 registry cleanup cannot discard an in-flight operation owner")
        if runtime_id in self._kernel_watchdog_workers:
            raise RuntimeError("KERNEL_V2 registry cleanup cannot discard an unconsumed watchdog worker")
        if runtime_id in self._kernel_callback_workers:
            raise RuntimeError("KERNEL_V2 registry cleanup cannot discard an active callback worker")
        pending_drop_reason = f"PENDING_{reason}"
        for operation, operation_state in sorted(state.operations.items()):
            for symbol, pending in sorted(operation_state.pending_by_symbol.items()):
                if symbol != pending.symbol:
                    raise RuntimeError("KERNEL_V2 pending quote registry key differs from its exact symbol owner")
                self._record_operation_pending_drop_locked(
                    operation_state,
                    pending=pending,
                    reason=pending_drop_reason,
                )
                self._kernel_pending_drop_count_by_reason[pending_drop_reason] = (
                    self._kernel_pending_drop_count_by_reason.get(pending_drop_reason, 0) + 1
                )
                self._last_kernel_pending_drop = {
                    "reason": pending_drop_reason,
                    "registration_drop_reason": reason,
                    "runtime_id": runtime_id,
                    "binding_id": getattr(expected_runtime, "binding_id", None),
                    "operation": operation,
                    "lifecycle_generation": lifecycle_generation,
                    "symbol": pending.symbol,
                    "market_data_id": pending.market_data_id,
                    "ingress_generation": pending.ingress_generation,
                    "ingress_sequence": pending.ingress_sequence,
                    "pending_identity_sha256": pending.pending_identity_sha256,
                }
                operation_state.not_replayed_pending_count += 1
            operation_state.pending_by_symbol.clear()
        removed_runtime = self._kernel_product_runtimes.pop(runtime_id)
        removed_state = self._kernel_retry_states.pop(runtime_id)
        if removed_runtime is not expected_runtime or removed_state is not state:
            raise RuntimeError("KERNEL_V2 exact registry cleanup removed a different owner")
        self._kernel_guarded_sinks.pop(runtime_id, None)
        self._kernel_registration_drop_count_by_reason[reason] = (
            self._kernel_registration_drop_count_by_reason.get(reason, 0) + 1
        )
        self._last_kernel_registration_drop = {
            "reason": reason,
            "runtime_id": runtime_id,
            "binding_id": getattr(expected_runtime, "binding_id", None),
            "lifecycle_generation": lifecycle_generation,
        }

    def _runtime_generation_in_flight(self, runtime_id: str, lifecycle_generation: int) -> bool:
        claim_in_flight = any(
            owner.lifecycle_generation == lifecycle_generation
            for (owner_runtime_id, _operation), owner in self._kernel_product_in_flight.items()
            if owner_runtime_id == runtime_id
        )
        callback_worker = self._kernel_callback_workers.get(runtime_id)
        return claim_in_flight or (
            callback_worker is not None
            and callback_worker.lifecycle_generation == lifecycle_generation
            and (callback_worker.active_attempt_token is not None or not callback_worker.task_queue.empty())
        )

    def _wait_for_runtime_drain_locked(
        self,
        *,
        runtime_id: str,
        lifecycle_generation: int,
        operation: str,
    ) -> None:
        drained = self._kernel_retry_condition.wait_for(
            lambda: not self._runtime_generation_in_flight(runtime_id, lifecycle_generation),
            timeout=float(self._kernel_lifecycle_drain_timeout_seconds),
        )
        if not drained:
            raise MiniQMTKernelProductLifecycleError(
                reason_code="MINIQMT_K6_PRODUCT_RUNTIME_DRAIN_TIMEOUT",
                message="KERNEL_V2 runtime lifecycle timed out waiting for in-flight work",
                context={
                    "runtime_id": runtime_id,
                    "lifecycle_generation": lifecycle_generation,
                    "operation": operation,
                    "timeout_seconds": float(self._kernel_lifecycle_drain_timeout_seconds),
                },
            )

    def _rollback_release_sink(
        self,
        *,
        supervisor: Any,
        consumer_id: str,
        guarded_sink: Callable[..., Any] | None,
        state: _KernelProductIngressRetryState,
        primary: Exception,
        operation: str,
    ) -> None:
        try:
            if guarded_sink is None:
                raise RuntimeError("exact guarded sink is unavailable for release rollback")
            sink_reader = getattr(supervisor, "get_observation_sink", None)
            if not callable(sink_reader):
                raise RuntimeError("quote supervisor lacks exact observation-sink readback")
            current_sink = sink_reader(consumer_id=consumer_id, symbols=state.symbols)
            if current_sink is None:
                supervisor.register_observation_sink(
                    consumer_id=consumer_id,
                    symbols=state.symbols,
                    sink=guarded_sink,
                )
            elif current_sink is not guarded_sink:
                raise RuntimeError("quote supervisor observation-sink owner changed during release rollback")
        except Exception as rollback:
            self._mark_release_unknown(
                state=state,
                runtime_id=state.runtime_id,
                lifecycle_generation=state.lifecycle_generation,
                exception=rollback,
            )
            raise MiniQMTKernelProductRegistryRollbackError(
                operation=operation,
                primary=primary,
                rollback=rollback,
            ) from primary
        with self._kernel_retry_condition:
            state.lifecycle_state = _KERNEL_ACTIVE_LIFECYCLE
            self._kernel_retry_condition.notify_all()

    @staticmethod
    def _read_exact_observation_sink(
        *,
        supervisor: Any,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> Callable[..., Any] | None:
        sink_reader = getattr(supervisor, "get_observation_sink", None)
        if not callable(sink_reader):
            raise RuntimeError("quote supervisor lacks exact observation-sink readback")
        current_sink = sink_reader(consumer_id=consumer_id, symbols=symbols)
        if current_sink is not None and not callable(current_sink):
            raise RuntimeError("quote supervisor returned an invalid observation-sink owner")
        return current_sink

    @staticmethod
    def _release_consumer_is_absent(*, supervisor: Any, consumer_id: str) -> bool:
        health = supervisor.health()
        consumers = health.get("consumers") if isinstance(health, Mapping) else None
        if not isinstance(consumers, Mapping):
            raise RuntimeError("quote supervisor health lacks exact consumer lease readback")
        consumer_health = consumers.get(consumer_id)
        if consumer_health is None:
            return True
        if not isinstance(consumer_health, Mapping):
            raise RuntimeError("quote supervisor returned an invalid consumer lease readback")
        return consumer_health.get("lease_status") == "RELEASED" and not consumer_health.get("lease_id")

    def _claim_kernel_product_attempt(
        self,
        *,
        runtime_id: str,
        runtime: Any,
        operation: str,
        lifecycle_generation: int,
        now_utc: datetime,
        now_monotonic_ns: int,
        consumer_id: str | None = None,
        pending: _KernelPendingQuoteV1 | None = None,
    ) -> _KernelProductAttemptClaim | MiniQMTKernelProductIngressSuppression | _KernelProductWatchdogSuppression:
        with self._kernel_retry_condition:
            state = self._kernel_retry_states.get(runtime_id)
            registered_runtime = self._kernel_product_runtimes.get(runtime_id)
            if registered_runtime is runtime and state is None:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_RETRY_STATE_MISSING",
                    message="registered KERNEL_V2 runtime lost its retry/lifecycle owner",
                    context={"runtime_id": runtime_id, "operation": operation},
                )
            if (
                self._shutdown
                or self._shutdown_requested
                or registered_runtime is not runtime
                or state is None
                or state.lifecycle_generation != lifecycle_generation
                or state.lifecycle_state != _KERNEL_ACTIVE_LIFECYCLE
            ):
                return self._suppression(
                    runtime_id=runtime_id,
                    consumer_id=consumer_id,
                    operation=operation,
                    disposition="LIFECYCLE_FENCED",
                    lifecycle_generation=lifecycle_generation,
                    operation_state=None,
                    pending=pending,
                )
            operation_state = state.operations.get(operation)
            if operation_state is None:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING",
                    message="KERNEL_V2 runtime lost a mandatory operation retry owner",
                    context={
                        "runtime_id": runtime_id,
                        "operation": operation,
                        "lifecycle_generation": lifecycle_generation,
                    },
                )
            attempt_key = (runtime_id, operation)
            if attempt_key in self._kernel_product_in_flight:
                operation_state.suppressed_count += 1
                if pending is not None:
                    self._remember_pending_locked(operation_state, pending)
                return self._suppression(
                    runtime_id=runtime_id,
                    consumer_id=consumer_id,
                    operation=operation,
                    disposition="SINGLE_FLIGHT_SUPPRESSED",
                    lifecycle_generation=lifecycle_generation,
                    operation_state=operation_state,
                    pending=pending,
                )
            if (
                operation_state.active_failure is not None
                and operation_state.next_retry_monotonic_ns is not None
                and now_monotonic_ns < operation_state.next_retry_monotonic_ns
            ):
                operation_state.suppressed_count += 1
                if pending is not None:
                    self._remember_pending_locked(operation_state, pending)
                return self._suppression(
                    runtime_id=runtime_id,
                    consumer_id=consumer_id,
                    operation=operation,
                    disposition="RETRY_BACKOFF_SUPPRESSED",
                    lifecycle_generation=lifecycle_generation,
                    operation_state=operation_state,
                    pending=pending,
                )
            self._kernel_attempt_token += 1
            claim = _KernelProductAttemptClaim(
                runtime_id=runtime_id,
                operation=operation,
                lifecycle_generation=lifecycle_generation,
                attempt_token=self._kernel_attempt_token,
            )
            self._kernel_product_in_flight[attempt_key] = claim
            operation_state.last_attempt_at_utc = now_utc
            if operation_state.active_failure is not None or operation_state.pending_by_symbol:
                operation_state.attempt_count += 1
            return claim

    @staticmethod
    def _suppression(
        *,
        runtime_id: str,
        consumer_id: str | None,
        operation: str,
        disposition: str,
        lifecycle_generation: int,
        operation_state: _KernelOperationRetryState | None,
        pending: _KernelPendingQuoteV1 | None,
    ) -> MiniQMTKernelProductIngressSuppression | _KernelProductWatchdogSuppression:
        failure_fingerprint_sha256 = operation_state.failure_fingerprint_sha256 if operation_state is not None else None
        next_retry_at_utc = _iso_utc(operation_state.next_retry_at_utc) if operation_state is not None else None
        if operation == _KERNEL_WATCHDOG_OPERATION:
            return _KernelProductWatchdogSuppression(
                runtime_id=runtime_id,
                disposition=disposition,
                lifecycle_generation=lifecycle_generation,
                failure_fingerprint_sha256=failure_fingerprint_sha256,
                next_retry_at_utc=next_retry_at_utc,
            )
        if operation != _KERNEL_CALLBACK_OPERATION or consumer_id is None or pending is None:
            raise MiniQMTKernelProductLifecycleError(
                reason_code="MINIQMT_K6_PRODUCT_SUPPRESSION_OWNER_INVALID",
                message="KERNEL_V2 callback suppression lacks its exact consumer or frame owner",
                context={"runtime_id": runtime_id, "operation": operation},
            )
        return MiniQMTKernelProductIngressSuppression(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            operation=operation,
            disposition=disposition,
            lifecycle_generation=lifecycle_generation,
            symbol=pending.symbol,
            ingress_generation=pending.ingress_generation,
            ingress_sequence=pending.ingress_sequence,
            market_data_id=pending.market_data_id,
            failure_fingerprint_sha256=failure_fingerprint_sha256,
            next_retry_at_utc=next_retry_at_utc,
            pending_identity_sha256=pending.pending_identity_sha256,
        )

    def _complete_kernel_product_attempt(
        self,
        *,
        claim: _KernelProductAttemptClaim,
        succeeded: bool,
        now_utc: datetime,
        now_monotonic_ns: int,
        completed_pending: _KernelPendingQuoteV1 | None = None,
        exception: Exception | None = None,
    ) -> None:
        with self._kernel_retry_condition:
            attempt_key = (claim.runtime_id, claim.operation)
            owner = self._kernel_product_in_flight.get(attempt_key)
            if owner != claim:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_ATTEMPT_OWNER_DRIFT",
                    message="KERNEL_V2 retry attempt owner changed before completion",
                    context={"runtime_id": claim.runtime_id, "operation": claim.operation},
                )
            state = self._kernel_retry_states.get(claim.runtime_id)
            registered_runtime = self._kernel_product_runtimes.get(claim.runtime_id)
            lifecycle_matches = (
                state is not None
                and registered_runtime is not None
                and state.lifecycle_generation == claim.lifecycle_generation
            )
            operation_state = state.operations.get(claim.operation) if lifecycle_matches else None
            self._kernel_product_in_flight.pop(attempt_key, None)
            self._kernel_retry_condition.notify_all()
            if not lifecycle_matches:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_ATTEMPT_LIFECYCLE_DRIFT",
                    message="KERNEL_V2 attempt completion differs from its lifecycle owner",
                    context={
                        "runtime_id": claim.runtime_id,
                        "operation": claim.operation,
                        "lifecycle_generation": claim.lifecycle_generation,
                        "actual_lifecycle_generation": (state.lifecycle_generation if state is not None else None),
                        "runtime_owner_present": registered_runtime is not None,
                    },
                )
            if operation_state is None:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING",
                    message="KERNEL_V2 attempt completed without its mandatory operation retry owner",
                    context={
                        "runtime_id": claim.runtime_id,
                        "operation": claim.operation,
                        "lifecycle_generation": claim.lifecycle_generation,
                    },
                )
            if succeeded:
                operation_state.active_failure = None
                operation_state.failure_class = None
                operation_state.failure_fingerprint_sha256 = None
                operation_state.reason_code = None
                operation_state.sqlstate = None
                operation_state.constraint_name = None
                operation_state.first_failure_at_utc = None
                operation_state.last_failure_at_utc = None
                operation_state.next_retry_at_utc = None
                operation_state.next_retry_monotonic_ns = None
                operation_state.consecutive_failure_count = 0
                operation_state.last_success_at_utc = now_utc
                if completed_pending is not None:
                    self._resolve_pending_after_live_success_locked(
                        operation_state,
                        completed=completed_pending,
                        resolved_at_utc=now_utc,
                    )
                return
            if (
                operation_state.active_failure is not None
                and operation_state.failure_class == "POSTGRES_CHECK_VIOLATION"
            ):
                operation_state.secondary_failure_count += 1
                operation_state.last_secondary_failure = {
                    "observed_at_utc": now_utc.isoformat(),
                    "broker_side_effect_state": "UNKNOWN",
                    **(bounded_exception_summary_v1(exception) if exception is not None else {}),
                }
                delay_index = min(
                    max(operation_state.consecutive_failure_count - 1, 0),
                    len(_KERNEL_SCHEMA_RETRY_SECONDS) - 1,
                )
                delay_seconds = _KERNEL_SCHEMA_RETRY_SECONDS[delay_index]
                operation_state.next_retry_at_utc = now_utc + timedelta(seconds=delay_seconds)
                operation_state.next_retry_monotonic_ns = now_monotonic_ns + delay_seconds * 1_000_000_000
                return
            reason_code = getattr(
                exception,
                "reason_code",
                "MINIQMT_K6_PRODUCT_OPERATION_FAILED",
            )
            exception_summary = bounded_exception_summary_v1(exception) if exception is not None else {}
            fingerprint = sha256(
                (
                    f"{claim.runtime_id}|{claim.operation}|{reason_code}|"
                    f"{exception_summary.get('exception_type')}|{exception_summary.get('exception_message')}"
                ).encode("utf-8")
            ).hexdigest()
            if operation_state.active_failure is None or operation_state.failure_fingerprint_sha256 != fingerprint:
                operation_state.first_failure_at_utc = now_utc
                operation_state.consecutive_failure_count = 1
            else:
                operation_state.consecutive_failure_count += 1
            operation_state.attempt_count = max(operation_state.attempt_count, 1)
            failure_evidence = {
                "reason_code": reason_code,
                "runtime_id": claim.runtime_id,
                "operation": claim.operation,
                "lifecycle_generation": claim.lifecycle_generation,
                "attempt_token": claim.attempt_token,
                "observed_at_utc": now_utc.isoformat(),
                "failure_class": "RUNTIME_OPERATION_FAILURE",
                "failure_fingerprint_sha256": fingerprint,
                "attempt_count": operation_state.attempt_count,
                "consecutive_failure_count": operation_state.consecutive_failure_count,
                "automatic_retry": True,
                "retry_trigger": (
                    "NEXT_LIVE_QUOTE" if claim.operation == _KERNEL_CALLBACK_OPERATION else "WATCHDOG_CADENCE"
                ),
                "manual_ack_required": False,
                "business_gate": False,
                "broker_side_effect_state": "UNKNOWN",
                **exception_summary,
            }
            operation_state.failure_class = "RUNTIME_OPERATION_FAILURE"
            operation_state.failure_fingerprint_sha256 = fingerprint
            operation_state.reason_code = str(reason_code)
            operation_state.sqlstate = None
            operation_state.constraint_name = None
            operation_state.last_failure_at_utc = now_utc
            operation_state.next_retry_at_utc = None
            operation_state.next_retry_monotonic_ns = None
            operation_state.active_failure = dict(failure_evidence)
            operation_state.last_failure = dict(failure_evidence)
            operation_state.secondary_failure_count += 1
            operation_state.last_secondary_failure = dict(failure_evidence)
            state.last_failure = dict(failure_evidence)

    def _record_kernel_schema_failure(
        self,
        *,
        claim: _KernelProductAttemptClaim,
        runtime_id: str,
        runtime: Any,
        operation: str,
        failure: tuple[str, str, str, str],
        exception: Exception,
        now_utc: datetime,
        now_monotonic_ns: int,
        pending: _KernelPendingQuoteV1 | None = None,
    ) -> dict[str, Any]:
        sqlstate, constraint_name, schema_name, table_name = failure
        fingerprint = sha256(
            f"{runtime_id}|{operation}|{schema_name}|{table_name}|{sqlstate}|{constraint_name}".encode("utf-8")
        ).hexdigest()
        with self._kernel_retry_condition:
            attempt_key = (runtime_id, operation)
            owner = self._kernel_product_in_flight.get(attempt_key)
            if owner != claim:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_ATTEMPT_OWNER_DRIFT",
                    message="KERNEL_V2 retry attempt owner changed before schema failure recording",
                    context={"runtime_id": runtime_id, "operation": operation},
                )
            state = self._kernel_retry_states.get(runtime_id)
            if (
                state is None
                or state.lifecycle_generation != claim.lifecycle_generation
                or self._kernel_product_runtimes.get(runtime_id) is not runtime
            ):
                self._kernel_product_in_flight.pop(attempt_key, None)
                self._kernel_retry_condition.notify_all()
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_ATTEMPT_LIFECYCLE_DRIFT",
                    message="KERNEL_V2 retry attempt differs from its lifecycle owner",
                    context={"runtime_id": runtime_id, "operation": operation},
                )
            operation_state = state.operations.get(operation)
            if operation_state is None:
                self._kernel_product_in_flight.pop(attempt_key, None)
                self._kernel_retry_condition.notify_all()
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_OPERATION_RETRY_STATE_MISSING",
                    message="KERNEL_V2 schema failure has no mandatory operation retry owner",
                    context={
                        "runtime_id": runtime_id,
                        "operation": operation,
                        "lifecycle_generation": claim.lifecycle_generation,
                    },
                )
            self._kernel_product_in_flight.pop(attempt_key, None)
            self._kernel_retry_condition.notify_all()
            if operation_state.active_failure is None or operation_state.failure_fingerprint_sha256 != fingerprint:
                operation_state.first_failure_at_utc = now_utc
                operation_state.attempt_count = 1
                operation_state.consecutive_failure_count = 1
            else:
                operation_state.consecutive_failure_count += 1
            delay_index = min(
                operation_state.consecutive_failure_count - 1,
                len(_KERNEL_SCHEMA_RETRY_SECONDS) - 1,
            )
            delay_seconds = _KERNEL_SCHEMA_RETRY_SECONDS[delay_index]
            next_retry_at_utc = now_utc + timedelta(seconds=delay_seconds)
            evidence = {
                "runtime_id": runtime_id,
                "binding_id": getattr(runtime, "binding_id", None),
                "trade_date": (
                    getattr(runtime, "trade_date").isoformat()
                    if getattr(runtime, "trade_date", None) is not None
                    else None
                ),
                "source_capability_sha256": getattr(runtime, "source_capability_sha256", None),
                "operation": operation,
                "failure_class": "POSTGRES_CHECK_VIOLATION",
                "failure_fingerprint_sha256": fingerprint,
                "reason_code": "MINIQMT_KERNEL_EVENT_SCHEMA_CONSTRAINT_FAILED",
                "sqlstate": sqlstate,
                "constraint_name": constraint_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "exception": bounded_exception_summary_v1(exception),
                "attempt_count": operation_state.attempt_count,
                "consecutive_failure_count": operation_state.consecutive_failure_count,
                "next_retry_at_utc": next_retry_at_utc.isoformat(),
                "automatic_retry": True,
                "manual_ack_required": False,
                "business_gate": False,
                "broker_side_effect_state": "UNKNOWN",
            }
            operation_state.failure_class = "POSTGRES_CHECK_VIOLATION"
            operation_state.failure_fingerprint_sha256 = fingerprint
            operation_state.reason_code = "MINIQMT_KERNEL_EVENT_SCHEMA_CONSTRAINT_FAILED"
            operation_state.sqlstate = sqlstate
            operation_state.constraint_name = constraint_name
            operation_state.last_failure_at_utc = now_utc
            operation_state.next_retry_at_utc = next_retry_at_utc
            operation_state.next_retry_monotonic_ns = now_monotonic_ns + delay_seconds * 1_000_000_000
            operation_state.active_failure = dict(evidence)
            operation_state.last_failure = dict(evidence)
            if pending is not None:
                self._remember_pending_locked(operation_state, pending)
            state.last_failure = dict(evidence)
        log_schema_failure = logger.warning if operation == _KERNEL_CALLBACK_OPERATION else logger.error
        log_schema_failure(
            "KERNEL_V2 runtime entered automatic event-schema retry backoff: runtime_id=%s constraint=%s "
            "next_retry_at_utc=%s",
            runtime_id,
            constraint_name,
            next_retry_at_utc.isoformat(),
            exc_info=exception,
        )
        return evidence

    def _start_kernel_callback_worker(
        self,
        *,
        runtime_id: str,
        runtime: Any,
        lifecycle_generation: int,
        sink: Callable[..., Any],
        started_at_utc: datetime,
    ) -> _KernelCallbackWorker:
        with self._kernel_retry_condition:
            if runtime_id in self._kernel_callback_workers:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_DUPLICATE",
                    message="KERNEL_V2 callback worker already owns this runtime",
                    context={"runtime_id": runtime_id, "lifecycle_generation": lifecycle_generation},
                )
            state = self._kernel_retry_states.get(runtime_id)
            if (
                self._shutdown
                or self._shutdown_requested
                or self._kernel_product_runtimes.get(runtime_id) is not runtime
                or state is None
                or state.lifecycle_generation != lifecycle_generation
                or state.lifecycle_state != _KERNEL_ACTIVE_LIFECYCLE
            ):
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_INVALID",
                    message="KERNEL_V2 callback worker cannot start without its exact ACTIVE runtime owner",
                    context={"runtime_id": runtime_id, "lifecycle_generation": lifecycle_generation},
                )
            entry = _KernelCallbackWorker(
                runtime_id=runtime_id,
                binding_id=getattr(runtime, "binding_id", None),
                runtime=runtime,
                lifecycle_generation=lifecycle_generation,
                sink=sink,
                task_queue=queue.Queue(maxsize=1),
                stop_event=threading.Event(),
                stopped_event=threading.Event(),
                thread=threading.current_thread(),
                started_at_utc=started_at_utc,
            )
            worker = threading.Thread(
                target=self._execute_kernel_callback_worker,
                kwargs={"entry": entry},
                name=f"miniqmt-kernel-callback-{runtime_id}-g{lifecycle_generation}",
                daemon=True,
            )
            entry.thread = worker
            self._kernel_callback_workers[runtime_id] = entry
            try:
                worker.start()
            except Exception:
                self._kernel_callback_workers.pop(runtime_id, None)
                entry.stopped_event.set()
                self._kernel_retry_condition.notify_all()
                raise
            return entry

    def _stop_kernel_callback_worker(
        self,
        *,
        runtime_id: str,
        lifecycle_generation: int,
        operation: str,
    ) -> None:
        with self._kernel_retry_condition:
            entry = self._kernel_callback_workers.get(runtime_id)
            if entry is None:
                return
            if entry.lifecycle_generation != lifecycle_generation:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT",
                    message="KERNEL_V2 callback worker generation changed before stop",
                    context={
                        "runtime_id": runtime_id,
                        "operation": operation,
                        "expected_lifecycle_generation": lifecycle_generation,
                        "actual_lifecycle_generation": entry.lifecycle_generation,
                    },
                )
            if self._runtime_generation_in_flight(runtime_id, lifecycle_generation):
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_STOP_IN_FLIGHT",
                    message="KERNEL_V2 callback worker cannot stop before its exact task drains",
                    context={"runtime_id": runtime_id, "operation": operation},
                )
            entry.stop_event.set()
            try:
                entry.task_queue.put_nowait(None)
            except queue.Full as exc:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_STOP_QUEUE_FULL",
                    message="KERNEL_V2 callback worker stop signal could not close an unexpectedly occupied queue",
                    context={"runtime_id": runtime_id, "operation": operation},
                ) from exc
        entry.thread.join(timeout=float(self._kernel_lifecycle_drain_timeout_seconds))
        with self._kernel_retry_condition:
            if entry.thread.is_alive() or not entry.stopped_event.is_set():
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_STOP_UNKNOWN",
                    message="KERNEL_V2 callback worker did not stop within the bounded lifecycle timeout",
                    context={
                        "runtime_id": runtime_id,
                        "operation": operation,
                        "lifecycle_generation": lifecycle_generation,
                        "timeout_seconds": float(self._kernel_lifecycle_drain_timeout_seconds),
                    },
                )
            if not entry.task_queue.empty() or entry.active_attempt_token is not None:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_STOP_NOT_EMPTY",
                    message="KERNEL_V2 callback worker stopped with an unclosed task owner",
                    context={"runtime_id": runtime_id, "operation": operation},
                )
            if self._kernel_callback_workers.get(runtime_id) is not entry:
                raise MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT",
                    message="KERNEL_V2 callback worker owner changed before exact removal",
                    context={"runtime_id": runtime_id, "operation": operation},
                )
            self._kernel_callback_workers.pop(runtime_id)
            self._kernel_retry_condition.notify_all()

    def _execute_kernel_callback_task(
        self,
        *,
        entry: _KernelCallbackWorker,
        task: _KernelCallbackTask,
    ) -> tuple[str, Any]:
        claim = task.claim
        outcome: tuple[str, Any]
        try:
            try:
                result = entry.sink(*task.values)
                if result is not None:
                    raise MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_CALLBACK_RESULT_INVALID",
                        message="KERNEL_V2 product callback must return None",
                        context={
                            "runtime_id": entry.runtime_id,
                            "lifecycle_generation": claim.lifecycle_generation,
                            "attempt_token": claim.attempt_token,
                            "result_type": type(result).__qualname__,
                        },
                    )
            except Exception as exc:
                schema_failure = self._kernel_event_schema_failure(exc)
                failed_at_utc = self._kernel_retry_clock_utc()
                failed_at_monotonic_ns = self._kernel_retry_monotonic_ns()
                if schema_failure is None:
                    self._complete_kernel_product_attempt(
                        claim=claim,
                        succeeded=False,
                        now_utc=failed_at_utc,
                        now_monotonic_ns=failed_at_monotonic_ns,
                        exception=exc,
                    )
                    failure: Exception = exc
                else:
                    evidence = self._record_kernel_schema_failure(
                        claim=claim,
                        runtime_id=entry.runtime_id,
                        runtime=entry.runtime,
                        operation=_KERNEL_CALLBACK_OPERATION,
                        failure=schema_failure,
                        exception=exc,
                        now_utc=failed_at_utc,
                        now_monotonic_ns=failed_at_monotonic_ns,
                        pending=task.pending,
                    )
                    failure = quote_contract_error(
                        QuoteContractReasonCode.CONSUMER_FAILURE,
                        "KERNEL_V2 quote consumer entered automatic schema retry backoff",
                        context=evidence,
                    )
                outcome = ("failure", failure)
            else:
                self._complete_kernel_product_attempt(
                    claim=claim,
                    succeeded=True,
                    now_utc=self._kernel_retry_clock_utc(),
                    now_monotonic_ns=self._kernel_retry_monotonic_ns(),
                    completed_pending=task.pending,
                )
                outcome = ("success", result)
        except Exception as internal:
            with self._kernel_retry_lock:
                claim_is_active = self._kernel_product_in_flight.get((claim.runtime_id, claim.operation)) == claim
            if claim_is_active:
                try:
                    self._complete_kernel_product_attempt(
                        claim=claim,
                        succeeded=False,
                        now_utc=self._kernel_retry_clock_utc(),
                        now_monotonic_ns=self._kernel_retry_monotonic_ns(),
                        exception=internal,
                    )
                except Exception as finalization_error:  # noqa: BLE001 - preserve both worker failures.
                    internal = MiniQMTKernelProductRegistryRollbackError(
                        operation="CALLBACK_WORKER_FINALIZATION",
                        primary=internal,
                        rollback=finalization_error,
                    )
            outcome = ("failure", internal)
        return outcome

    def _execute_kernel_callback_worker(self, *, entry: _KernelCallbackWorker) -> None:
        try:
            while True:
                if entry.stop_event.is_set() and entry.task_queue.empty():
                    return
                try:
                    task = entry.task_queue.get(timeout=_KERNEL_CALLBACK_PEER_WAIT_SECONDS)
                except queue.Empty:
                    continue
                if task is None:
                    entry.task_queue.task_done()
                    return
                with self._kernel_retry_condition:
                    if self._kernel_callback_workers.get(entry.runtime_id) is not entry:
                        entry.last_failure = {
                            "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT",
                            "runtime_id": entry.runtime_id,
                            "lifecycle_generation": entry.lifecycle_generation,
                            "attempt_token": task.claim.attempt_token,
                        }
                    entry.active_attempt_token = task.claim.attempt_token
                    self._kernel_retry_condition.notify_all()
                outcome = self._execute_kernel_callback_task(entry=entry, task=task)
                completion_failure = outcome[1] if outcome[0] == "failure" else None
                task.completion_signal.resolve(
                    business_success=outcome[0] == "success",
                    completed_at_utc=self._kernel_retry_clock_utc(),
                    failure=(
                        {
                            "reason_code": getattr(
                                completion_failure,
                                "reason_code",
                                "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_TASK_FAILED",
                            ),
                            "runtime_id": entry.runtime_id,
                            "lifecycle_generation": entry.lifecycle_generation,
                            "attempt_token": task.claim.attempt_token,
                            "broker_side_effect_state": "UNKNOWN",
                            **bounded_exception_summary_v1(completion_failure),
                        }
                        if isinstance(completion_failure, Exception)
                        else None
                    ),
                )
                with self._kernel_retry_condition:
                    entry.active_attempt_token = None
                    entry.processed_count += 1
                    if outcome[0] == "failure":
                        failure = outcome[1]
                        entry.last_failure = {
                            "reason_code": getattr(
                                failure,
                                "reason_code",
                                "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_TASK_FAILED",
                            ),
                            "runtime_id": entry.runtime_id,
                            "lifecycle_generation": entry.lifecycle_generation,
                            "attempt_token": task.claim.attempt_token,
                            "broker_side_effect_state": "UNKNOWN",
                            **bounded_exception_summary_v1(failure),
                        }
                    self._kernel_retry_condition.notify_all()
                task.result_queue.put_nowait(outcome)
                entry.task_queue.task_done()
                if outcome[0] == "failure" and not task.result_observed.wait(
                    timeout=_KERNEL_CALLBACK_PEER_WAIT_SECONDS
                ):
                    failure = outcome[1]
                    logger.error(
                        "KERNEL_V2 callback worker failed: runtime_id=%s lifecycle_generation=%s attempt_token=%s",
                        entry.runtime_id,
                        task.claim.lifecycle_generation,
                        task.claim.attempt_token,
                        exc_info=(type(failure), failure, failure.__traceback__),
                    )
        except Exception as exc:  # noqa: BLE001 - retain an unexpected persistent-worker failure in health.
            with self._kernel_retry_condition:
                entry.last_failure = {
                    "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_WORKER_FAILED",
                    "runtime_id": entry.runtime_id,
                    "lifecycle_generation": entry.lifecycle_generation,
                    "broker_side_effect_state": "UNKNOWN",
                    **bounded_exception_summary_v1(exc),
                }
                self._kernel_retry_condition.notify_all()
            logger.error(
                "KERNEL_V2 persistent callback worker failed: runtime_id=%s lifecycle_generation=%s",
                entry.runtime_id,
                entry.lifecycle_generation,
                exc_info=True,
            )
        finally:
            entry.stopped_event.set()
            with self._kernel_retry_condition:
                self._kernel_retry_condition.notify_all()

    def _invoke_kernel_product_callback(
        self,
        *,
        runtime_id: str,
        consumer_id: str,
        runtime: Any,
        sink: Callable[..., Any],
        lifecycle_generation: int,
        values: tuple[Any, ...],
    ) -> Any:
        dispatch = self._enqueue_kernel_product_callback(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            runtime=runtime,
            sink=sink,
            lifecycle_generation=lifecycle_generation,
            values=values,
        )
        return self._await_kernel_product_callback(
            dispatch=dispatch,
            timeout_seconds=_KERNEL_CALLBACK_PEER_WAIT_SECONDS,
        )

    def _enqueue_kernel_product_callback(
        self,
        *,
        runtime_id: str,
        consumer_id: str,
        runtime: Any,
        sink: Callable[..., Any],
        lifecycle_generation: int,
        values: tuple[Any, ...],
    ) -> _KernelCallbackTask | MiniQMTKernelProductIngressSuppression:
        now_utc = self._kernel_retry_clock_utc()
        now_monotonic_ns = self._kernel_retry_monotonic_ns()
        pending = self._pending_quote(runtime_id=runtime_id, runtime=runtime, values=values)
        claim = self._claim_kernel_product_attempt(
            runtime_id=runtime_id,
            runtime=runtime,
            operation=_KERNEL_CALLBACK_OPERATION,
            lifecycle_generation=lifecycle_generation,
            now_utc=now_utc,
            now_monotonic_ns=now_monotonic_ns,
            consumer_id=consumer_id,
            pending=pending,
        )
        if isinstance(claim, MiniQMTKernelProductIngressSuppression):
            return claim
        result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)
        completion_signal = MiniQMTKernelProductIngressCompletionSignal(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            operation=_KERNEL_CALLBACK_OPERATION,
            lifecycle_generation=lifecycle_generation,
            attempt_token=claim.attempt_token,
            symbol=pending.symbol,
            ingress_generation=pending.ingress_generation,
            ingress_sequence=pending.ingress_sequence,
            market_data_id=pending.market_data_id,
            pending_identity_sha256=pending.pending_identity_sha256,
        )
        task = _KernelCallbackTask(
            claim=claim,
            values=values,
            pending=pending,
            completion_signal=completion_signal,
            result_queue=result_queue,
            result_observed=threading.Event(),
        )
        with self._kernel_retry_condition:
            entry = self._kernel_callback_workers.get(runtime_id)
            if (
                entry is None
                or entry.runtime is not runtime
                or entry.lifecycle_generation != lifecycle_generation
                or entry.sink is not sink
                or entry.stop_event.is_set()
                or entry.stopped_event.is_set()
                or not entry.thread.is_alive()
            ):
                failure = MiniQMTKernelProductLifecycleError(
                    reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OWNER_DRIFT",
                    message="KERNEL_V2 callback cannot enqueue without its exact live persistent worker",
                    context={
                        "runtime_id": runtime_id,
                        "lifecycle_generation": lifecycle_generation,
                        "worker_present": entry is not None,
                    },
                )
            else:
                failure = None
                try:
                    entry.task_queue.put_nowait(task)
                except queue.Full:
                    failure = MiniQMTKernelProductLifecycleError(
                        reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_QUEUE_FULL",
                        message="KERNEL_V2 callback worker queue is full despite exact single-flight ownership",
                        context={
                            "runtime_registry_ids": sorted(self._kernel_product_runtimes),
                            "retry_state_ids": sorted(self._kernel_retry_states),
                            "guarded_sink_ids": sorted(self._kernel_guarded_sinks),
                            "runtime_id": runtime_id,
                            "lifecycle_generation": lifecycle_generation,
                            "attempt_token": claim.attempt_token,
                        },
                    )
        if failure is not None:
            self._complete_kernel_product_attempt(
                claim=claim,
                succeeded=False,
                now_utc=self._kernel_retry_clock_utc(),
                now_monotonic_ns=self._kernel_retry_monotonic_ns(),
                exception=failure,
            )
            raise failure
        return task

    @staticmethod
    def _await_kernel_product_callback(
        *,
        dispatch: _KernelCallbackTask | MiniQMTKernelProductIngressSuppression,
        timeout_seconds: float,
    ) -> Any:
        if isinstance(dispatch, MiniQMTKernelProductIngressSuppression):
            return dispatch
        if type(dispatch) is not _KernelCallbackTask:
            raise MiniQMTKernelProductLifecycleError(
                reason_code="MINIQMT_K6_PRODUCT_CALLBACK_DISPATCH_INVALID",
                message="KERNEL_V2 callback await requires its exact dispatch owner",
                context={"dispatch_type": type(dispatch).__qualname__},
            )
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds < 0:
            raise ValueError("kernel product callback wait budget must be a non-negative number")
        try:
            outcome, payload = dispatch.result_queue.get(timeout=float(timeout_seconds))
        except queue.Empty:
            return MiniQMTKernelProductIngressPending(
                runtime_id=dispatch.claim.runtime_id,
                consumer_id=f"k6d-kernel-v2:{dispatch.claim.runtime_id}",
                operation=_KERNEL_CALLBACK_OPERATION,
                lifecycle_generation=dispatch.claim.lifecycle_generation,
                attempt_token=dispatch.claim.attempt_token,
                symbol=dispatch.pending.symbol,
                ingress_generation=dispatch.pending.ingress_generation,
                ingress_sequence=dispatch.pending.ingress_sequence,
                market_data_id=dispatch.pending.market_data_id,
                pending_identity_sha256=dispatch.pending.pending_identity_sha256,
                completion_signal=dispatch.completion_signal,
            )
        dispatch.result_observed.set()
        if outcome == "success":
            return payload
        if outcome == "failure" and isinstance(payload, Exception):
            raise payload
        raise MiniQMTKernelProductLifecycleError(
            reason_code="MINIQMT_K6_PRODUCT_CALLBACK_WORKER_OUTCOME_INVALID",
            message="KERNEL_V2 callback worker returned an invalid outcome carrier",
            context={
                "runtime_id": dispatch.claim.runtime_id,
                "lifecycle_generation": dispatch.claim.lifecycle_generation,
                "attempt_token": dispatch.claim.attempt_token,
                "outcome": outcome,
            },
        )

    @staticmethod
    def _pending_quote(
        *,
        runtime_id: str,
        runtime: Any,
        values: tuple[Any, ...],
    ) -> _KernelPendingQuoteV1:
        observation = values[0] if values else None
        frame = getattr(observation, "frame", None)
        raw_symbol = getattr(frame, "symbol", None)
        owned_symbols = tuple(getattr(runtime, "symbols", ()))
        symbol = raw_symbol if type(raw_symbol) is str and raw_symbol in owned_symbols else "__invalid__"
        market_data_id = getattr(observation, "market_data_id", None)
        if type(market_data_id) is not str or not market_data_id:
            market_data_id = None
        ingress_generation = getattr(frame, "ingress_generation", None)
        if type(ingress_generation) is not int:
            ingress_generation = None
        ingress_sequence = getattr(frame, "ingress_sequence", None)
        if type(ingress_sequence) is not int:
            ingress_sequence = None
        context_id = getattr(observation, "context_id", None)
        return _KernelPendingQuoteV1(
            symbol=symbol,
            pending_identity_sha256=kernel_product_pending_identity_sha256_v1(
                runtime_id=runtime_id,
                symbol=symbol,
                market_data_id=market_data_id,
                ingress_generation=ingress_generation,
                ingress_sequence=ingress_sequence,
                context_id=context_id,
                values=values,
            ),
            market_data_id=market_data_id,
            ingress_generation=ingress_generation,
            ingress_sequence=ingress_sequence,
            values=values,
        )

    @staticmethod
    def _remember_pending_locked(
        operation_state: _KernelOperationRetryState,
        pending: _KernelPendingQuoteV1,
    ) -> None:
        existing = operation_state.pending_by_symbol.get(pending.symbol)
        if existing is not None and existing.pending_identity_sha256 == pending.pending_identity_sha256:
            return
        if (
            existing is not None
            and existing.ingress_generation is not None
            and existing.ingress_sequence is not None
            and pending.ingress_generation is not None
            and pending.ingress_sequence is not None
            and (pending.ingress_generation, pending.ingress_sequence)
            < (existing.ingress_generation, existing.ingress_sequence)
        ):
            MiniQMTQuoteIngressActivation._record_operation_pending_drop_locked(
                operation_state,
                pending=pending,
                reason="PENDING_ORDERING_REJECTED",
                replacement=existing,
            )
            return
        if existing is not None:
            MiniQMTQuoteIngressActivation._record_operation_pending_drop_locked(
                operation_state,
                pending=existing,
                reason="PENDING_COALESCED_SUPERSEDED",
                replacement=pending,
            )
        operation_state.pending_by_symbol[pending.symbol] = pending

    @staticmethod
    def _record_operation_pending_drop_locked(
        operation_state: _KernelOperationRetryState,
        *,
        pending: _KernelPendingQuoteV1,
        reason: str,
        replacement: _KernelPendingQuoteV1 | None = None,
    ) -> None:
        operation_state.pending_drop_count_by_reason[reason] = (
            operation_state.pending_drop_count_by_reason.get(reason, 0) + 1
        )
        operation_state.last_pending_drop = {
            "reason": reason,
            "symbol": pending.symbol,
            "market_data_id": pending.market_data_id,
            "ingress_generation": pending.ingress_generation,
            "ingress_sequence": pending.ingress_sequence,
            "pending_identity_sha256": pending.pending_identity_sha256,
            "replacement_market_data_id": replacement.market_data_id if replacement is not None else None,
            "replacement_ingress_generation": (replacement.ingress_generation if replacement is not None else None),
            "replacement_ingress_sequence": replacement.ingress_sequence if replacement is not None else None,
            "replacement_pending_identity_sha256": (
                replacement.pending_identity_sha256 if replacement is not None else None
            ),
        }

    @staticmethod
    def _resolve_pending_after_live_success_locked(
        operation_state: _KernelOperationRetryState,
        *,
        completed: _KernelPendingQuoteV1,
        resolved_at_utc: datetime,
    ) -> None:
        pending = operation_state.pending_by_symbol.get(completed.symbol)
        if pending is None:
            return
        comparable_order = (
            pending.ingress_generation is not None
            and pending.ingress_sequence is not None
            and completed.ingress_generation is not None
            and completed.ingress_sequence is not None
        )
        superseded = pending.pending_identity_sha256 == completed.pending_identity_sha256 or (
            comparable_order
            and (pending.ingress_generation, pending.ingress_sequence)
            <= (completed.ingress_generation, completed.ingress_sequence)
        )
        if not superseded:
            return
        operation_state.pending_by_symbol.pop(completed.symbol, None)
        MiniQMTQuoteIngressActivation._record_operation_pending_drop_locked(
            operation_state,
            pending=pending,
            reason="PENDING_SUPERSEDED_BY_FRESH_LIVE_QUOTE",
            replacement=completed,
        )
        operation_state.not_replayed_pending_count += 1
        operation_state.last_pending_resolution = {
            "disposition": "SUPERSEDED_BY_FRESH_LIVE_QUOTE_NOT_REPLAYED",
            "resolved_at_utc": resolved_at_utc.isoformat(),
            "pending_count": 1,
            "recovery_symbol": completed.symbol,
            "recovery_pending_identity_sha256": completed.pending_identity_sha256,
        }

    @staticmethod
    def _expire_prior_day_quote_retry_locked(
        state: _KernelProductIngressRetryState,
        *,
        resolved_at_utc: datetime,
    ) -> None:
        operation_state = state.operations[_KERNEL_CALLBACK_OPERATION]
        if operation_state.active_failure is None and not operation_state.pending_by_symbol:
            return
        pending_count = len(operation_state.pending_by_symbol)
        for pending in operation_state.pending_by_symbol.values():
            MiniQMTQuoteIngressActivation._record_operation_pending_drop_locked(
                operation_state,
                pending=pending,
                reason="PENDING_PRIOR_SESSION_EXPIRED",
            )
        operation_state.not_replayed_pending_count += pending_count
        operation_state.last_pending_resolution = {
            "disposition": "EXPIRED_PRIOR_SESSION_NOT_REPLAYED",
            "resolved_at_utc": resolved_at_utc.isoformat(),
            "pending_count": pending_count,
        }
        operation_state.pending_by_symbol.clear()
        operation_state.active_failure = None
        operation_state.failure_class = None
        operation_state.failure_fingerprint_sha256 = None
        operation_state.reason_code = None
        operation_state.sqlstate = None
        operation_state.constraint_name = None
        operation_state.first_failure_at_utc = None
        operation_state.last_failure_at_utc = None
        operation_state.next_retry_at_utc = None
        operation_state.next_retry_monotonic_ns = None
        operation_state.consecutive_failure_count = 0

    @staticmethod
    def _kernel_event_schema_failure(exception: Exception) -> tuple[str, str, str, str] | None:
        current: BaseException | None = exception
        visited: set[int] = set()
        for _ in range(16):
            if current is None or id(current) in visited:
                break
            visited.add(id(current))
            sqlstate = str(getattr(current, "pgcode", "") or getattr(current, "sqlstate", "") or "").strip()
            diag = getattr(current, "diag", None)
            constraint_name = str(getattr(diag, "constraint_name", "") or "").strip()
            schema_name = str(getattr(diag, "schema_name", "") or "").strip()
            table_name = str(getattr(diag, "table_name", "") or "").strip()
            if (
                sqlstate == "23514"
                and constraint_name in _KERNEL_EVENT_SCHEMA_CONSTRAINTS
                and schema_name == "qmt_strategy"
                and table_name == "execution_runtime_event"
            ):
                return sqlstate, constraint_name, schema_name, table_name
            current = current.__cause__ or current.__context__
        return None

    def ingest_kernel_order_callback_v1(
        self,
        *,
        runtime_id: str,
        broker_order_id: str,
        raw_payload: dict[str, Any],
        observed_at: datetime,
    ) -> Any:
        runtime = self._kernel_product_runtime(runtime_id)
        ingress = getattr(runtime, "ingest_order_callback_v1", None)
        if not callable(ingress):
            raise TypeError("registered kernel product runtime lacks order callback ingress")
        return ingress(
            broker_order_id=broker_order_id,
            raw_payload=raw_payload,
            observed_at=observed_at,
        )

    def ingest_kernel_trade_callback_v1(
        self,
        *,
        runtime_id: str,
        broker_order_id: str,
        trade_quantity: int,
        trade_price_decimal: Any,
        cumulative_quantity: int,
        raw_payload: dict[str, Any],
        observed_at: datetime,
    ) -> Any:
        runtime = self._kernel_product_runtime(runtime_id)
        ingress = getattr(runtime, "ingest_trade_callback_v1", None)
        if not callable(ingress):
            raise TypeError("registered kernel product runtime lacks trade callback ingress")
        return ingress(
            broker_order_id=broker_order_id,
            trade_quantity=trade_quantity,
            trade_price_decimal=trade_price_decimal,
            cumulative_quantity=cumulative_quantity,
            raw_payload=raw_payload,
            observed_at=observed_at,
        )

    def _kernel_product_runtime(self, runtime_id: str) -> Any:
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise TypeError("runtime_id must be a canonical identity")
        with self._kernel_retry_lock:
            try:
                return self._kernel_product_runtimes[runtime_id]
            except KeyError as exc:
                raise KeyError(f"no registered KERNEL_V2 product runtime for {runtime_id}") from exc

    def prepare_runtime_context(
        self,
        *,
        runtime_id: str,
        plan: Any,
        recovering_active: bool,
        clock_at_utc: datetime,
        clock_monotonic_ns: int,
    ) -> dict[str, Any] | None:
        quote_control = plan.plan_payload_json.get("quote_control")
        if quote_control is None:
            return None
        if not isinstance(quote_control, Mapping) or set(quote_control) != {"binding", "revision", "assignments"}:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "execution plan quote_control payload is not exact for context publication",
                context={"plan_id": plan.plan_id, "legacy_fallback": False},
            )
        binding_payload = quote_control.get("binding")
        if not isinstance(binding_payload, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "execution plan quote_control binding is missing for context publication",
                context={"plan_id": plan.plan_id, "legacy_fallback": False},
            )
        binding = QuoteControlBindingV1.from_binding_config({"miniqmt_quote_control": binding_payload})
        if binding.control_revision.value == "LEGACY_B0":
            return None
        if self._shutdown or self._shutdown_requested:
            raise RuntimeError("stopped or shutdown-fenced MiniQMT quote activation cannot publish runtime context")
        if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            if not recovering_active:
                self.controller_factory.assert_accepts_new_assignments()
            self.controller_factory._ensure_delegate()
        adapter = self._current_context_adapter()
        if adapter is None:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "B0_QUOTE_V2 production composition has no authoritative context adapter",
                context={"plan_id": plan.plan_id, "runtime_id": runtime_id, "legacy_fallback": False},
            )
        execution_policy = _execution_policy_snapshot(plan)
        policy = QuoteContractPolicy.from_execution_policy(execution_policy)
        revision_payload = quote_control.get("revision")
        if not isinstance(revision_payload, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 context publication requires a frozen revision",
                context={"plan_id": plan.plan_id, "runtime_id": runtime_id, "legacy_fallback": False},
            )
        revision = B0QuoteV2RevisionV1.from_payload(revision_payload)
        if revision.quote_policy_sha256 != policy.policy_sha256:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 context policy differs from the frozen plan revision",
                context={
                    "plan_id": plan.plan_id,
                    "runtime_id": runtime_id,
                    "revision_quote_policy_sha256": revision.quote_policy_sha256,
                    "context_quote_policy_sha256": policy.policy_sha256,
                    "legacy_fallback": False,
                },
            )
        assignment_payloads = quote_control.get("assignments")
        if not isinstance(assignment_payloads, list) or not assignment_payloads:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 context publication requires exact parent assignments",
                context={"plan_id": plan.plan_id, "runtime_id": runtime_id, "legacy_fallback": False},
            )
        assignments = {
            assignment.parent_intent_id: assignment
            for payload in assignment_payloads
            if isinstance(payload, Mapping)
            for assignment in (ParentQuoteControlAssignmentV1.from_plan_payload(payload, revision=revision),)
        }
        if len(assignments) != len(assignment_payloads):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 context publication assignments are malformed or duplicated",
                context={"plan_id": plan.plan_id, "runtime_id": runtime_id, "legacy_fallback": False},
            )
        prepare_transition = getattr(self.controller_factory, "prepare_assignment_transition", None)
        if not callable(prepare_transition):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "B0_QUOTE_V2 controller factory lacks the assignment transition contract",
                context={"plan_id": plan.plan_id, "runtime_id": runtime_id, "legacy_fallback": False},
            )
        prepare_transition(runtime_id=runtime_id, assignments=assignments)
        context = adapter.prepare_runtime_context(
            runtime_id=runtime_id,
            symbols=(intent.symbol for intent in plan.intents),
            execution_policy=execution_policy,
            clock_at_utc=clock_at_utc.astimezone(UTC),
            clock_monotonic_ns=clock_monotonic_ns,
        )
        return {
            "runtime_id": runtime_id,
            "context_id": context.context_id,
            "policy_sha256": context.policy.policy_sha256,
            "symbol_count": len(context.symbols),
            "recovering_active": bool(recovering_active),
        }

    def _current_supervisor(self) -> QuoteIngressSupervisor | None:
        if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            return self.controller_factory.supervisor
        return self.supervisor

    def _current_context_adapter(self) -> MiniQMTQuoteContextAuthorityAdapter | None:
        if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            return self.controller_factory.context_adapter
        return self.context_adapter

    @property
    def quote_context_adapter(self) -> MiniQMTQuoteContextAuthorityAdapter | None:
        return self._current_context_adapter()


def _execution_policy_snapshot(plan: Any) -> dict[str, Any]:
    policy_container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(policy_container, Mapping):
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            "B0_QUOTE_V2 execution plan has no immutable execution policy",
            context={"plan_id": plan.plan_id},
        )
    payload = policy_container.get("payload")
    if not isinstance(payload, Mapping):
        raise quote_contract_error(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            "B0_QUOTE_V2 execution plan policy payload is missing",
            context={"plan_id": plan.plan_id},
        )
    policy_json = payload.get("policy_json")
    return dict(policy_json) if isinstance(policy_json, Mapping) else dict(payload)


def build_miniqmt_quote_ingress_activation_from_env(
    *,
    environ: Mapping[str, Any] | None = None,
    schema_gate_reader: Callable[[], str] = _production_quote_event_schema_gate,
    subscriber_factory: Callable[[], Any] | None = None,
    qmt_client_factory: Callable[[], Any] | None = None,
    context_adapter_factory: Callable[
        [QuoteEvaluationContextStore, Any], MiniQMTQuoteContextAuthorityAdapter
    ] = _production_quote_context_adapter,
) -> MiniQMTQuoteIngressActivation:
    """Build the one scheduler-owned production composition, without side effects.

    Schema readback is always read-only.  switch=true verifies it eagerly;
    switch=false builds a lazy drain-only factory and verifies it only if a
    durable active B0 runtime actually recovers, preserving pure LEGACY startup
    behavior.  Application startup never runs the migration and never converts
    a failed B0 activation into a legacy assignment.
    """

    requested_config = QuoteIngressRuntimeConfig.from_mapping(environ if environ is not None else os.environ)
    effective_subscriber_factory = subscriber_factory or _production_subscriber
    effective_qmt_client_factory = qmt_client_factory or _production_qmt_client

    if not requested_config.enabled:
        drain_factory = DrainOnlyB0QuoteV2ControllerFactory(
            requested_config=requested_config,
            schema_gate_reader=schema_gate_reader,
            subscriber_factory=effective_subscriber_factory,
            qmt_client_factory=effective_qmt_client_factory,
            context_adapter_factory=context_adapter_factory,
        )
        return MiniQMTQuoteIngressActivation(
            config=requested_config,
            status="DRAINING",
            production_ddl_gate="deferred_until_durable_recovery",
            process_switch_enabled=False,
            reason_code="MINIQMT_QUOTE_INGRESS_SWITCH_DISABLED_DRAIN_ONLY",
            controller_factory=drain_factory,
        )

    try:
        production_ddl_gate = str(schema_gate_reader())
    except Exception as exc:  # noqa: BLE001 - preserve LEGACY while making the ingress failure explicit
        logger.error(
            "MiniQMT quote ingress production schema readback failed; B0_QUOTE_V2 remains unavailable",
            exc_info=True,
        )
        return MiniQMTQuoteIngressActivation(
            config=requested_config,
            status="BLOCKED" if requested_config.enabled else "DISABLED",
            production_ddl_gate="readback_failed",
            process_switch_enabled=requested_config.enabled,
            reason_code="MINIQMT_QUOTE_EVENT_SCHEMA_READBACK_FAILED",
            failure={"exception_type": type(exc).__name__, "message": str(exc)},
            _startup_schema_gate_reader=schema_gate_reader,
            _startup_subscriber_factory=effective_subscriber_factory,
            _startup_qmt_client_factory=effective_qmt_client_factory,
            _startup_context_adapter_factory=context_adapter_factory,
        )
    if production_ddl_gate != MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED:
        logger.error(
            "MiniQMT quote ingress blocked by production DDL readback: gate=%s",
            production_ddl_gate,
        )
        return MiniQMTQuoteIngressActivation(
            config=requested_config,
            status="BLOCKED" if requested_config.enabled else "DISABLED",
            production_ddl_gate=production_ddl_gate,
            process_switch_enabled=requested_config.enabled,
            reason_code="MINIQMT_QUOTE_EVENT_SCHEMA_NOT_APPLIED",
            _startup_schema_gate_reader=schema_gate_reader,
            _startup_subscriber_factory=effective_subscriber_factory,
            _startup_qmt_client_factory=effective_qmt_client_factory,
            _startup_context_adapter_factory=context_adapter_factory,
        )

    supervisor, controller_factory, context_adapter = _build_runtime_components(
        runtime_config=requested_config,
        subscriber_factory=effective_subscriber_factory,
        qmt_client_factory=effective_qmt_client_factory,
        context_adapter_factory=context_adapter_factory,
    )
    return MiniQMTQuoteIngressActivation(
        config=requested_config,
        status="READY",
        production_ddl_gate=production_ddl_gate,
        process_switch_enabled=True,
        supervisor=supervisor,
        controller_factory=controller_factory,
        context_adapter=context_adapter,
    )


__all__ = [
    "MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY",
    "MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED",
    "DrainOnlyB0QuoteV2ControllerFactory",
    "MiniQMTKernelProductIngressPending",
    "MiniQMTKernelProductIngressSuppression",
    "MiniQMTKernelProductLifecycleError",
    "MiniQMTQuoteIngressActivation",
    "build_miniqmt_quote_ingress_activation_from_env",
]
