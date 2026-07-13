"""Production composition root for the Phase 1 MiniQMT quote ingress.

This module owns construction only.  It never applies DDL, persists process
configuration, creates a simulation binding, or calls the broker.  A schema-
blocked composition exposes no B0 controller factory.  With an exact schema,
switch=false exposes a drain-only factory solely for durable active recovery;
new B0_QUOTE_V2 assignments still fail closed while historical LEGACY_B0
bindings continue on their unchanged path.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import logging
import os
import threading
from typing import TYPE_CHECKING, Any, Callable, Mapping

from backend.miniqmt_quote_contract_config import QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2ControllerFactory,
    quote_ingress_config_sha256,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractReasonCode, quote_contract_error
from backend.services.miniqmt_execution_runtime.repository import (
    default_miniqmt_execution_runtime_repository,
)

if TYPE_CHECKING:
    from backend.services.miniqmt_execution_runtime.quote_ingress import QuoteIngressSupervisor


logger = logging.getLogger("aistock.simulation_runtime.miniqmt_quote_activation")

MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY = "SIM:B0_QUOTE_V2:simulation_scheduler"
MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED = "applied_and_verified"


def _production_quote_event_schema_gate() -> str:
    repository = default_miniqmt_execution_runtime_repository()
    return str(repository.quote_event_schema_gate())


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
    ) -> None:
        if requested_config.enabled:
            raise ValueError("drain-only factory requires process switch=false")
        self.requested_config = requested_config
        self.runtime_config = replace(requested_config, enabled=True)
        self._schema_gate_reader = schema_gate_reader
        self._subscriber_factory = subscriber_factory
        self._qmt_client_factory = qmt_client_factory
        self._delegate: B0QuoteV2ControllerFactory | None = None
        self._supervisor: QuoteIngressSupervisor | None = None
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
            supervisor, delegate = _build_runtime_components(
                runtime_config=self.runtime_config,
                subscriber_factory=self._subscriber_factory,
                qmt_client_factory=self._qmt_client_factory,
            )
            delegate.set_accept_new_assignments(False)
            self._supervisor = supervisor
            self._delegate = delegate
            self._last_failure = None
            return delegate


def _build_runtime_components(
    *,
    runtime_config: QuoteIngressRuntimeConfig,
    subscriber_factory: Callable[[], Any],
    qmt_client_factory: Callable[[], Any],
) -> tuple[QuoteIngressSupervisor, B0QuoteV2ControllerFactory]:
    from backend.services.miniqmt_execution_runtime.quote_ingress import QuoteIngressSupervisor

    subscriber = subscriber_factory()
    qmt_client = qmt_client_factory()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=runtime_config,
        data_session_key=MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
        owner=runtime_config.owner_mode,
        bootstrap_fetcher=_build_bootstrap_fetcher(qmt_client),
    )
    controller_factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=runtime_config,
        data_session_key=MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY,
    )
    return supervisor, controller_factory


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
    _shutdown: bool = False

    def __post_init__(self) -> None:
        if self.status == "READY":
            if self.production_ddl_gate != MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED:
                raise ValueError("operational quote activation requires applied_and_verified production DDL")
            if self.supervisor is None or self.controller_factory is None:
                raise ValueError("operational quote activation requires supervisor and controller factory")
        elif self.status == "DRAINING":
            if not isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory) or self.supervisor is not None:
                raise ValueError("DRAINING quote activation requires only the lazy drain factory")
        elif self.supervisor is not None or self.controller_factory is not None:
            raise ValueError("disabled or blocked quote activation cannot expose runtime components")

    def health(self) -> dict[str, Any]:
        process_config = replace(self.config, enabled=self.process_switch_enabled)
        runtime_config = (
            self.controller_factory.runtime_config
            if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory)
            else self.config
        )
        production_ddl_gate = (
            self.controller_factory.production_ddl_gate
            if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory)
            else self.production_ddl_gate
        )
        payload: dict[str, Any] = {
            "schema_version": "miniqmt_quote_ingress_activation_v1",
            "status": "STOPPED" if self._shutdown else self.status,
            "enabled": self.process_switch_enabled,
            "data_session_key": self.data_session_key,
            "owner_mode": self.config.owner_mode,
            "process_config_sha256": quote_ingress_config_sha256(process_config),
            "runtime_config_sha256": quote_ingress_config_sha256(runtime_config),
            "runtime_config_enabled": runtime_config.enabled,
            "production_ddl_gate": production_ddl_gate,
            "reason_code": self.reason_code,
            "factory_available": self.controller_factory is not None and not self._shutdown,
            "failure": dict(self.failure) if self.failure is not None else None,
        }
        supervisor = self._current_supervisor()
        if supervisor is not None and not self._shutdown:
            payload["ingress"] = supervisor.health()
        if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            payload["drain_factory"] = self.controller_factory.health()
        return payload

    def begin_lifecycle_epoch(self) -> dict[str, Any]:
        if self._shutdown:
            raise RuntimeError("stopped MiniQMT quote activation cannot begin a lifecycle epoch")
        supervisor = self._current_supervisor()
        if supervisor is not None:
            supervisor.begin_lifecycle_epoch()
        return self.health()

    def watchdog_tick(self) -> dict[str, Any]:
        if self._shutdown:
            raise RuntimeError("stopped MiniQMT quote activation cannot run watchdog")
        supervisor = self._current_supervisor()
        if supervisor is not None:
            supervisor.watchdog_tick()
        return self.health()

    def shutdown(self) -> dict[str, Any]:
        if self._shutdown:
            return self.health()
        if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            self.controller_factory.shutdown()
        else:
            if self.controller_factory is not None:
                self.controller_factory.set_accept_new_assignments(False)
            if self.supervisor is not None:
                self.supervisor.shutdown()
        self._shutdown = True
        return self.health()

    def _current_supervisor(self) -> QuoteIngressSupervisor | None:
        if isinstance(self.controller_factory, DrainOnlyB0QuoteV2ControllerFactory):
            return self.controller_factory.supervisor
        return self.supervisor


def build_miniqmt_quote_ingress_activation_from_env(
    *,
    environ: Mapping[str, Any] | None = None,
    schema_gate_reader: Callable[[], str] = _production_quote_event_schema_gate,
    subscriber_factory: Callable[[], Any] | None = None,
    qmt_client_factory: Callable[[], Any] | None = None,
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
        )

    supervisor, controller_factory = _build_runtime_components(
        runtime_config=requested_config,
        subscriber_factory=effective_subscriber_factory,
        qmt_client_factory=effective_qmt_client_factory,
    )
    return MiniQMTQuoteIngressActivation(
        config=requested_config,
        status="READY",
        production_ddl_gate=production_ddl_gate,
        process_switch_enabled=True,
        supervisor=supervisor,
        controller_factory=controller_factory,
    )


__all__ = [
    "MINIQMT_B0_QUOTE_V2_SIM_DATA_SESSION_KEY",
    "MINIQMT_QUOTE_EVENT_SCHEMA_GATE_APPLIED",
    "DrainOnlyB0QuoteV2ControllerFactory",
    "MiniQMTQuoteIngressActivation",
    "build_miniqmt_quote_ingress_activation_from_env",
]
