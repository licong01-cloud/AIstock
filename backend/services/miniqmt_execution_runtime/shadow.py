"""Phase 5 shadow reconciliation for MiniQMT event-loop rollout."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field

from backend.services.qmt_strategy_ledger.order_service import ManagedOrderRequest
from backend.services.qmt_strategy_ledger.models import (
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_OPEN_LIKE,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
    is_open_like_order_status,
    is_partial_order_status,
    is_terminal_order_status,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

from .client import MiniQMTExecutionRuntimeClient
from .config import MiniQMTExecutionRuntimeKind
from .gateway import MiniQMTGatewayCancelAck, MiniQMTGatewayOrderAck
from .models import (
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
)
from .repository import InMemoryMiniQMTExecutionRuntimeRepository, MiniQMTExecutionRuntimeRepository
from .runtime import MiniQMTExecutionRuntime


class MiniQMTShadowSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    FATAL = "FATAL"


class MiniQMTShadowScenario(str, Enum):
    FULL_FILL = "full_fill"
    PARTIAL_55_STREAM = "partial_55_stream"
    DELAY = "delay"
    REJECT = "reject"
    CANCEL = "cancel"
    DISCONNECT = "disconnect"
    RESTART_RECOVERY = "restart_recovery"


class MiniQMTShadowLedgerSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    child_orders: list[dict[str, Any]] = Field(default_factory=list)
    trades: list[dict[str, Any]] = Field(default_factory=list)
    cash: dict[str, Any] = Field(default_factory=dict)
    positions: dict[str, Any] = Field(default_factory=dict)


class MiniQMTShadowInputEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)


class MiniQMTShadowRuntimeSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime_id: str
    runtime_kind: str
    ledger: MiniQMTShadowLedgerSnapshot = Field(default_factory=MiniQMTShadowLedgerSnapshot)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MiniQMTShadowDifference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    severity: MiniQMTShadowSeverity
    reason_code: str
    message: str
    a_value: Any = None
    b_value: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MiniQMTShadowReconciliationReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report_id: str
    runtime_id: str
    scenario: MiniQMTShadowScenario
    a_runtime: MiniQMTShadowRuntimeSnapshot
    b_runtime: MiniQMTShadowRuntimeSnapshot
    differences: list[MiniQMTShadowDifference] = Field(default_factory=list)
    durable_event_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def fatal_differences(self) -> list[MiniQMTShadowDifference]:
        return [item for item in self.differences if item.severity == MiniQMTShadowSeverity.FATAL]

    @property
    def is_fatal(self) -> bool:
        return bool(self.fatal_differences)


class MiniQMTShadowDryRunGateway(Protocol):
    @property
    def submitted_orders(self) -> list[Any]:
        ...

    def submit_child_order(self, *_args: Any, **_kwargs: Any) -> Any:
        ...


class MiniQMTShadowRuntimeAdapter(Protocol):
    def compute_shadow_snapshot(
        self,
        *,
        runtime_id: str,
        input_events: tuple[MiniQMTShadowInputEvent, ...],
        metadata: dict[str, Any],
    ) -> MiniQMTShadowRuntimeSnapshot:
        ...


@dataclass
class NoBrokerMutationMiniQMTShadowGateway:
    """SIM dry-run gateway that fails loud if shadow code tries broker mutation."""

    reason_code: str = "MINIQMT_SHADOW_BROKER_MUTATION_BLOCKED"
    submitted_orders: list[Any] = field(default_factory=list)
    cancelled_orders: list[Any] = field(default_factory=list)

    def connect(self, *, runtime_id: str) -> None:
        self.runtime_id = runtime_id

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        raise RuntimeError(
            "MiniQMT shadow dry-run gateway has no broker order snapshot; "
            "reason_code=MINIQMT_SHADOW_BROKER_SYNC_UNAVAILABLE"
        )

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        raise RuntimeError(
            "MiniQMT shadow dry-run gateway has no broker trade snapshot; "
            "reason_code=MINIQMT_SHADOW_BROKER_SYNC_UNAVAILABLE"
        )

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        raise RuntimeError(
            "MiniQMT shadow dry-run gateway has no broker position snapshot; "
            "reason_code=MINIQMT_SHADOW_BROKER_SYNC_UNAVAILABLE"
        )

    def submit_child_order(self, *_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError(
            "MiniQMT shadow runtime is dry-run only and cannot mutate broker orders; "
            f"reason_code={self.reason_code}"
        )

    def cancel_child_order(self, *_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError(
            "MiniQMT shadow runtime is dry-run only and cannot mutate broker orders; "
            f"reason_code={self.reason_code}"
        )


class MiniQMTShadowEventLoopAdapter:
    """Replay shadow input events through the event-loop runtime without broker mutation."""

    def __init__(
        self,
        *,
        repository: MiniQMTExecutionRuntimeRepository | None = None,
        runtime_config_hash: str = "shadow_event_loop",
        account_group_id: str = "shadow",
        trade_date: date | None = None,
    ) -> None:
        self.repository = repository or InMemoryMiniQMTExecutionRuntimeRepository()
        self.runtime_config_hash = runtime_config_hash
        self.account_group_id = account_group_id
        self.trade_date = trade_date or datetime.now(UTC).date()

    def compute_shadow_snapshot(
        self,
        *,
        runtime_id: str,
        input_events: tuple[MiniQMTShadowInputEvent, ...],
        metadata: dict[str, Any],
    ) -> MiniQMTShadowRuntimeSnapshot:
        gateway = _RecordingShadowGateway()
        runtime = MiniQMTExecutionRuntime(
            config=_runtime_config(
                runtime_id=f"{runtime_id}_a",
                account_group_id=str(metadata.get("account_group_id") or self.account_group_id),
                trade_date=_trade_date_from_metadata(metadata) or self.trade_date,
                runtime_config_hash=str(metadata.get("runtime_config_hash") or self.runtime_config_hash),
                source="shadow_event_loop",
            ),
            repository=self.repository,
            gateway=gateway,
        )
        runtime.start()
        _drive_event_loop_runtime(runtime=runtime, input_events=input_events, metadata=metadata)
        return _snapshot_from_runtime_repository(
            repository=self.repository,
            runtime_id=runtime.config.runtime_id,
            runtime_kind="event_loop",
            metadata={
                "broker_called": False,
                "broker_mutated": False,
                "shadow_adapter": type(self).__name__,
                "source_runtime_id": runtime_id,
            },
        )


class MiniQMTShadowCompilerAdapter:
    """Replay the same events through the legacy compiler-style runtime client in preview mode."""

    def __init__(
        self,
        *,
        repository: MiniQMTExecutionRuntimeRepository | None = None,
        runtime_config_hash: str = "shadow_compiler",
        account_group_id: str = "shadow",
        trade_date: date | None = None,
    ) -> None:
        self.repository = repository or InMemoryMiniQMTExecutionRuntimeRepository()
        self.runtime_config_hash = runtime_config_hash
        self.account_group_id = account_group_id
        self.trade_date = trade_date or datetime.now(UTC).date()

    def compute_shadow_snapshot(
        self,
        *,
        runtime_id: str,
        input_events: tuple[MiniQMTShadowInputEvent, ...],
        metadata: dict[str, Any],
    ) -> MiniQMTShadowRuntimeSnapshot:
        client = MiniQMTExecutionRuntimeClient(
            repository=self.repository,
            runtime_kind=MiniQMTExecutionRuntimeKind.COMPILER,
        )
        trade_date = _trade_date_from_metadata(metadata) or self.trade_date
        parent_intents = _parent_intents_from_shadow_events(input_events, trade_date=trade_date)
        if not parent_intents:
            raise RuntimeError(
                "MiniQMT shadow compiler adapter requires a parent_intent event before replay; "
                f"reason_code=MINIQMT_SHADOW_PARENT_INTENT_MISSING, runtime_id={runtime_id}"
            )
        policy_context = _policy_context_from_shadow_events(input_events, metadata=metadata)
        quote_provider = _quote_provider_from_shadow_events(input_events)
        client.build_managed_vnpy_order_requests(
            parent_intents=parent_intents,
            policy_context=policy_context,
            account_group_id=str(metadata.get("account_group_id") or self.account_group_id),
            trade_date=trade_date,
            runtime_config_hash=str(metadata.get("runtime_config_hash") or self.runtime_config_hash),
            runtime_id=f"{runtime_id}_b",
            strategy_slot_id=str(metadata.get("strategy_slot_id") or _DEFAULT_STRATEGY_SLOT_ID),
            managed_request_factory=_shadow_managed_request_factory(trade_date=trade_date),
            quote_provider=quote_provider,
            source="miniqmt_phase5_shadow_compiler_preview",
        )
        _apply_terminal_shadow_events_to_repository(
            repository=self.repository,
            runtime_id=f"{runtime_id}_b",
            input_events=input_events,
        )
        return _snapshot_from_runtime_repository(
            repository=self.repository,
            runtime_id=f"{runtime_id}_b",
            runtime_kind="compiler",
            metadata={
                "broker_called": False,
                "broker_mutated": False,
                "shadow_adapter": type(self).__name__,
                "source_runtime_id": runtime_id,
            },
        )


class MiniQMTShadowParallelRunner:
    """Feed identical input events to A/B shadow adapters and reconcile output."""

    def __init__(self, *, reconciler: MiniQMTShadowReconciler) -> None:
        self.reconciler = reconciler

    def run(
        self,
        *,
        runtime_id: str,
        scenario: MiniQMTShadowScenario | str,
        input_events: Sequence[MiniQMTShadowInputEvent | dict[str, Any]],
        event_loop_adapter: MiniQMTShadowRuntimeAdapter,
        compiler_adapter: MiniQMTShadowRuntimeAdapter,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTShadowReconciliationReport:
        materialized_events = tuple(
            event if isinstance(event, MiniQMTShadowInputEvent) else MiniQMTShadowInputEvent.model_validate(event)
            for event in input_events
        )
        run_metadata = {
            **dict(metadata or {}),
            "input_event_count": len(materialized_events),
            "shadow_mode": "dry_run_no_broker_mutation",
        }
        a_snapshot = event_loop_adapter.compute_shadow_snapshot(
            runtime_id=runtime_id,
            input_events=materialized_events,
            metadata={**run_metadata, "runtime_kind": "event_loop"},
        )
        b_snapshot = compiler_adapter.compute_shadow_snapshot(
            runtime_id=runtime_id,
            input_events=materialized_events,
            metadata={**run_metadata, "runtime_kind": "compiler"},
        )
        a_snapshot = _snapshot(a_snapshot, runtime_kind="event_loop")
        b_snapshot = _snapshot(b_snapshot, runtime_kind="compiler")
        _assert_shadow_snapshot_is_dry_run(a_snapshot)
        _assert_shadow_snapshot_is_dry_run(b_snapshot)
        return self.reconciler.reconcile(
            runtime_id=runtime_id,
            scenario=scenario,
            a_runtime=a_snapshot,
            b_runtime=b_snapshot,
            metadata=run_metadata,
        )


class MiniQMTShadowReconciler:
    """Persist A/B shadow reconciliation reports as runtime events."""

    def __init__(self, *, repository: MiniQMTExecutionRuntimeRepository) -> None:
        self.repository = repository

    def reconcile(
        self,
        *,
        runtime_id: str,
        scenario: MiniQMTShadowScenario | str,
        a_runtime: MiniQMTShadowRuntimeSnapshot | dict[str, Any],
        b_runtime: MiniQMTShadowRuntimeSnapshot | dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTShadowReconciliationReport:
        scenario_value = scenario if isinstance(scenario, MiniQMTShadowScenario) else MiniQMTShadowScenario(str(scenario))
        a_snapshot = _snapshot(a_runtime, runtime_kind="event_loop")
        b_snapshot = _snapshot(b_runtime, runtime_kind="compiler")
        report_id = f"mqrt_shadow_{runtime_id}_{scenario_value.value}"
        differences = _diff_snapshots(a_snapshot, b_snapshot)
        report = MiniQMTShadowReconciliationReport(
            report_id=report_id,
            runtime_id=runtime_id,
            scenario=scenario_value,
            a_runtime=a_snapshot,
            b_runtime=b_snapshot,
            differences=differences,
            metadata=dict(metadata or {}),
        )
        runtime = self.repository.get_runtime(runtime_id)
        if runtime is None:
            runtime = MiniQMTExecutionRuntimeRecord(
                runtime_id=runtime_id,
                account_group_id=str(report.metadata.get("account_group_id") or "shadow"),
                trade_date=_trade_date_from_metadata(report.metadata),
                event_loop_state=MiniQMTExecutionRuntimeState.READY,
                runtime_config_hash=str(report.metadata.get("runtime_config_hash") or "shadow_reconciliation"),
                metadata={},
            )
            self.repository.upsert_runtime(runtime)
        event = self.repository.append_event(
            self._event(runtime_id=runtime_id, report=report)
        )
        durable_report = report.model_copy(update={"durable_event_id": event.event_id})
        runtime = self.repository.get_runtime(runtime_id) or runtime
        self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "metadata": {
                        **dict(runtime.metadata),
                        "last_shadow_reconciliation": durable_report.model_dump(mode="json"),
                    }
                }
            )
        )
        if durable_report.is_fatal:
            reason_codes = sorted({item.reason_code for item in durable_report.fatal_differences})
            raise RuntimeError(
                "MiniQMT shadow reconciliation found fatal A/B drift; "
                f"reason_code=MINIQMT_SHADOW_RECONCILIATION_FATAL, runtime_id={runtime_id}, "
                f"scenario={scenario_value.value}, difference_reason_codes={reason_codes}"
            )
        return durable_report

    def _event(
        self,
        *,
        runtime_id: str,
        report: MiniQMTShadowReconciliationReport,
    ):
        from .models import MiniQMTExecutionEvent

        return MiniQMTExecutionEvent(
            runtime_id=runtime_id,
            sequence=self.repository.next_event_sequence(runtime_id),
            event_type=MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED,
            source="shadow",
            payload=report.model_dump(mode="json"),
        )


def _snapshot(value: MiniQMTShadowRuntimeSnapshot | dict[str, Any], *, runtime_kind: str) -> MiniQMTShadowRuntimeSnapshot:
    if isinstance(value, MiniQMTShadowRuntimeSnapshot):
        return value
    payload = dict(value)
    payload.setdefault("runtime_kind", runtime_kind)
    return MiniQMTShadowRuntimeSnapshot.model_validate(payload)


_DEFAULT_STRATEGY_SLOT_ID = "shadow_strategy_slot"
_DEFAULT_PACKAGE_ID = "shadow_package"
_DEFAULT_PORTFOLIO_ID = "shadow_portfolio"
_DEFAULT_SYMBOL = "000001.SZ"
_DEFAULT_PRICE = 10.0


@dataclass
class _RecordingShadowGateway:
    """Runtime gateway used by shadow replay; records intent and never mutates a broker."""

    submitted_orders: list[Any] = field(default_factory=list)
    cancelled_orders: list[Any] = field(default_factory=list)
    order_snapshots: list[dict[str, Any]] = field(default_factory=list)
    trade_snapshots: list[dict[str, Any]] = field(default_factory=list)
    position_snapshots: list[dict[str, Any]] = field(default_factory=list)

    def connect(self, *, runtime_id: str) -> None:
        self.runtime_id = runtime_id

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        if not self.order_snapshots and not self.submitted_orders:
            raise RuntimeError(
                "MiniQMT shadow event-loop replay has no recorded broker order snapshots; "
                "reason_code=MINIQMT_SHADOW_BROKER_SYNC_UNAVAILABLE"
            )
        return [dict(item) for item in self.order_snapshots]

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return [dict(item) for item in self.trade_snapshots]

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return [dict(item) for item in self.position_snapshots]

    def submit_child_order(self, order: Any) -> MiniQMTGatewayOrderAck:
        self.submitted_orders.append(order)
        broker_order_id = str(order.metadata.get("shadow_broker_order_id") or order.child_order_id)
        return MiniQMTGatewayOrderAck(
            accepted=True,
            broker_order_id=broker_order_id,
            message="shadow event-loop dry-run accepted child order intent",
            raw={
                "gateway": "miniqmt_shadow_event_loop_replay",
                "broker_called": False,
                "broker_mutated": False,
            },
        )

    def cancel_child_order(self, order: Any, *, reason: str) -> MiniQMTGatewayCancelAck:
        self.cancelled_orders.append(order)
        return MiniQMTGatewayCancelAck(
            accepted=True,
            broker_order_id=order.broker_order_id,
            message="shadow event-loop dry-run accepted cancel intent",
            raw={
                "gateway": "miniqmt_shadow_event_loop_replay",
                "broker_called": False,
                "broker_mutated": False,
                "reason": reason,
            },
        )


def _runtime_config(
    *,
    runtime_id: str,
    account_group_id: str,
    trade_date: date,
    runtime_config_hash: str,
    source: str,
):
    from .models import MiniQMTExecutionRuntimeConfig

    return MiniQMTExecutionRuntimeConfig(
        runtime_id=runtime_id,
        account_group_id=account_group_id,
        trade_date=trade_date,
        runtime_config_hash=runtime_config_hash,
        metadata={"source": source, "shadow_mode": "dry_run_no_broker_mutation"},
    )


def _drive_event_loop_runtime(
    *,
    runtime: MiniQMTExecutionRuntime,
    input_events: tuple[MiniQMTShadowInputEvent, ...],
    metadata: dict[str, Any],
) -> None:
    intent_by_id: dict[str, OrderIntent] = {}
    algo_by_parent: dict[str, str] = {}
    child_by_parent: dict[str, Any] = {}
    policy_context = _policy_context_from_shadow_events(input_events, metadata=metadata)
    for event in input_events:
        event_type = event.event_type.strip().lower()
        payload = dict(event.payload)
        if event_type == "parent_intent":
            intent = _parent_intent_from_payload(payload, trade_date=runtime.config.trade_date)
            intent_by_id[intent.intent_id] = intent
            algo = runtime.create_vnpy_algo_instance(
                parent_intent_id=intent.intent_id,
                strategy_slot_id=str(payload.get("strategy_slot_id") or metadata.get("strategy_slot_id") or _DEFAULT_STRATEGY_SLOT_ID),
                symbol=intent.symbol,
                side=intent.side,
                target_quantity=int(intent.quantity),
                algo_code=str(policy_context["policy_json"]["algo_code"]),
                limit_price=float(payload.get("limit_price") or intent.limit_price or _DEFAULT_PRICE),
                algo_config=dict(policy_context["policy_json"].get("algo_config") or {}),
                metadata={
                    "source": "miniqmt_phase5_shadow_event_loop",
                    "runtime_child_context": {
                        "parent_intent_metadata": dict(intent.metadata),
                        "target_trade_date": runtime.config.trade_date.isoformat(),
                        "package_id": intent.package_id,
                        "portfolio_id": intent.portfolio_id,
                        "strategy_id": str(payload.get("strategy_id") or _DEFAULT_STRATEGY_SLOT_ID),
                        "strategy_name": str(payload.get("strategy_name") or _DEFAULT_STRATEGY_SLOT_ID),
                    },
                },
            )
            algo_by_parent[intent.intent_id] = algo.algo_instance_id
            continue
        if event_type == "tick":
            runtime.on_tick(
                symbol=str(payload.get("symbol") or _DEFAULT_SYMBOL),
                price=float(payload.get("price") or payload.get("last_price") or _DEFAULT_PRICE),
                payload=_normalized_tick_payload(payload),
            )
            child_by_parent.update(_latest_child_by_parent(runtime))
            continue
        if event_type in {"timer", "algo_timer"}:
            runtime.on_timer(timer_name=str(payload.get("timer_name") or f"shadow_timer_{len(runtime.repository.list_events(runtime.config.runtime_id)) + 1}"))
            child_by_parent.update(_latest_child_by_parent(runtime))
            continue
        if event_type in {"trade_fill", "partial_fill_55"}:
            parent_intent_id = str(payload.get("parent_intent_id") or next(iter(algo_by_parent), ""))
            child = _child_for_event(runtime, parent_intent_id=parent_intent_id, child_by_parent=child_by_parent)
            if child is None:
                continue
            quantity = int(payload.get("quantity") or payload.get("traded_volume") or payload.get("filled_quantity") or child.quantity)
            price = float(payload.get("price") or payload.get("traded_price") or child.price or _DEFAULT_PRICE)
            runtime.record_trade_event(
                broker_order_id=child.broker_order_id or child.child_order_id,
                quantity=quantity,
                price=price,
                payload={
                    "trade_id": str(payload.get("trade_id") or f"shadow_trade_{child.child_order_id}_{quantity}"),
                    "cumulative_quantity": int(payload.get("cumulative_quantity") or payload.get("filled_quantity") or quantity),
                    **payload,
                },
            )
            continue
        if event_type == "reject":
            _record_shadow_order_status(runtime, payload=payload, status=STATUS_REJECTED)
            continue
        if event_type == "cancel":
            _record_shadow_order_status(runtime, payload=payload, status=STATUS_CANCELLED)
            continue
        if event_type == "disconnect":
            runtime.record_disconnect_event(
                reason=str(payload.get("reason") or "shadow_disconnect"),
                payload=payload,
            )
            continue
        if event_type == "restart_recovery":
            gateway = runtime.gateway
            if isinstance(gateway, _RecordingShadowGateway):
                _refresh_shadow_gateway_snapshots(runtime=runtime, gateway=gateway)
            runtime.recover()


def _apply_terminal_shadow_events_to_repository(
    *,
    repository: MiniQMTExecutionRuntimeRepository,
    runtime_id: str,
    input_events: tuple[MiniQMTShadowInputEvent, ...],
) -> None:
    for event in input_events:
        event_type = event.event_type.strip().lower()
        if event_type not in {"trade_fill", "partial_fill_55", "reject", "cancel"}:
            continue
        child = _latest_repo_child(repository, runtime_id=runtime_id, parent_intent_id=str(event.payload.get("parent_intent_id") or ""))
        if child is None:
            continue
        if event_type in {"trade_fill", "partial_fill_55"}:
            quantity = int(event.payload.get("quantity") or event.payload.get("traded_volume") or child.quantity)
            cumulative = int(event.payload.get("cumulative_quantity") or event.payload.get("filled_quantity") or quantity)
            status = (
                MiniQMTChildOrderStatus.FILLED
                if cumulative >= int(child.quantity)
                else MiniQMTChildOrderStatus.PARTIALLY_FILLED
            )
            repository.upsert_child_order(
                child.model_copy(
                    update={
                        "status": status,
                        "metadata": {
                            **dict(child.metadata),
                            "last_trade_price": float(event.payload.get("price") or event.payload.get("traded_price") or child.price),
                            "cumulative_quantity": cumulative,
                        },
                    }
                )
            )
            continue
        status = MiniQMTChildOrderStatus.REJECTED if event_type == "reject" else MiniQMTChildOrderStatus.CANCELLED
        repository.upsert_child_order(child.model_copy(update={"status": status}))


def _record_shadow_order_status(runtime: MiniQMTExecutionRuntime, *, payload: dict[str, Any], status: int) -> None:
    parent_intent_id = str(payload.get("parent_intent_id") or "")
    child = _child_for_event(runtime, parent_intent_id=parent_intent_id, child_by_parent=_latest_child_by_parent(runtime))
    if child is None:
        return
    runtime.record_order_event(
        broker_order_id=child.broker_order_id or child.child_order_id,
        status=str(status),
        payload={
            "order_status": status,
            "traded": int(payload.get("traded") or payload.get("filled_quantity") or 0),
            "status_msg": str(payload.get("status_msg") or "shadow terminal order update"),
            **payload,
        },
    )


def _parent_intents_from_shadow_events(
    input_events: tuple[MiniQMTShadowInputEvent, ...],
    *,
    trade_date: date,
) -> list[OrderIntent]:
    return [
        _parent_intent_from_payload(event.payload, trade_date=trade_date)
        for event in input_events
        if event.event_type.strip().lower() == "parent_intent"
    ]


def _parent_intent_from_payload(payload: dict[str, Any], *, trade_date: date) -> OrderIntent:
    side = payload.get("side") or OrderSide.BUY.value
    order_type = payload.get("order_type") or OrderType.LIMIT.value
    return OrderIntent(
        intent_id=str(payload.get("intent_id") or payload.get("parent_intent_id") or "shadow_parent_intent"),
        package_id=str(payload.get("package_id") or _DEFAULT_PACKAGE_ID),
        portfolio_id=str(payload.get("portfolio_id") or _DEFAULT_PORTFOLIO_ID),
        symbol=str(payload.get("symbol") or _DEFAULT_SYMBOL),
        side=side if isinstance(side, OrderSide) else OrderSide(str(side).upper()),
        quantity=int(payload.get("quantity") or payload.get("target_quantity") or 100),
        order_type=order_type if isinstance(order_type, OrderType) else OrderType(str(order_type).upper()),
        limit_price=float(payload.get("limit_price") or payload.get("price") or _DEFAULT_PRICE),
        target_trade_date=trade_date,
        metadata=dict(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}),
    )


def _policy_context_from_shadow_events(
    input_events: tuple[MiniQMTShadowInputEvent, ...],
    *,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    policy_json = dict(metadata.get("policy_json") if isinstance(metadata.get("policy_json"), dict) else {})
    for event in input_events:
        if event.event_type.strip().lower() != "policy":
            continue
        policy_json.update(dict(event.payload.get("policy_json") if isinstance(event.payload.get("policy_json"), dict) else event.payload))
    policy_json.setdefault("algo_code", "SNIPER_MINIQMT")
    policy_json.setdefault("algo_config", {})
    return {
        "policy_json": policy_json,
        "validated_execution_policy_id": str(metadata.get("validated_execution_policy_id") or "shadow_policy"),
        "policy_sha256": str(metadata.get("policy_sha256") or "shadow_policy_sha256"),
    }


def _quote_provider_from_shadow_events(
    input_events: tuple[MiniQMTShadowInputEvent, ...],
):
    quotes: dict[str, dict[str, Any]] = {}
    for event in input_events:
        if event.event_type.strip().lower() != "tick":
            continue
        payload = _normalized_tick_payload(event.payload)
        quotes[str(payload.get("symbol") or _DEFAULT_SYMBOL)] = payload

    def quote_provider(symbol: str) -> dict[str, Any] | None:
        return quotes.get(symbol)

    return quote_provider


def _normalized_tick_payload(payload: dict[str, Any]) -> dict[str, Any]:
    price = float(payload.get("price") or payload.get("last_price") or _DEFAULT_PRICE)
    return {
        "symbol": str(payload.get("symbol") or _DEFAULT_SYMBOL),
        "price": price,
        "last_price": price,
        "bid_price_1": float(payload.get("bid_price_1") or price),
        "bid_volume_1": int(payload.get("bid_volume_1") or payload.get("volume") or 1000),
        "ask_price_1": float(payload.get("ask_price_1") or price),
        "ask_volume_1": int(payload.get("ask_volume_1") or payload.get("volume") or 1000),
        **dict(payload),
    }


def _shadow_managed_request_factory(*, trade_date: date):
    def factory(child: Any, index: int) -> ManagedOrderRequest:
        return ManagedOrderRequest(
            account_id="shadow_account",
            strategy_name=str(child.strategy_slot_id),
            symbol=child.symbol,
            side=child.side.value,
            order_type=1 if child.side == OrderSide.BUY else 2,
            quantity=int(child.quantity),
            price_type=int(child.price_type),
            price=Decimal(str(child.price or 0)),
            order_remark=f"shadow-{index:02d}-{child.symbol[:6]}-{child.side.value[0]}",
            trade_date=trade_date,
            mode="SIM",
            metadata={
                **dict(child.metadata),
                "broker_called": False,
                "broker_mutated": False,
            },
        )

    return factory


def _latest_child_by_parent(runtime: MiniQMTExecutionRuntime) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for child in runtime.repository.list_child_orders(runtime.config.runtime_id, active_only=False):
        result[child.parent_intent_id] = child
    return result


def _child_for_event(
    runtime: MiniQMTExecutionRuntime,
    *,
    parent_intent_id: str,
    child_by_parent: dict[str, Any],
) -> Any | None:
    if parent_intent_id and parent_intent_id in child_by_parent:
        return child_by_parent[parent_intent_id]
    children = runtime.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
    return children[-1] if children else None


def _latest_repo_child(
    repository: MiniQMTExecutionRuntimeRepository,
    *,
    runtime_id: str,
    parent_intent_id: str,
) -> Any | None:
    children = [
        child
        for child in repository.list_child_orders(runtime_id, active_only=False)
        if not parent_intent_id or child.parent_intent_id == parent_intent_id
    ]
    return children[-1] if children else None


def _snapshot_from_runtime_repository(
    *,
    repository: MiniQMTExecutionRuntimeRepository,
    runtime_id: str,
    runtime_kind: str,
    metadata: dict[str, Any],
) -> MiniQMTShadowRuntimeSnapshot:
    children = repository.list_child_orders(runtime_id, active_only=False)
    child_orders = [
        _shadow_child_order_payload(child, shadow_key=_shadow_child_order_key(child, index))
        for index, child in enumerate(children, start=1)
    ]
    trades = []
    cash: dict[str, Any] = {"available_cash": 0.0}
    positions: dict[str, Any] = {}
    for index, child in enumerate(children, start=1):
        cumulative = int(child.metadata.get("cumulative_quantity") or 0)
        if child.status == MiniQMTChildOrderStatus.FILLED and cumulative <= 0:
            cumulative = int(child.quantity)
        if child.status == MiniQMTChildOrderStatus.PARTIALLY_FILLED and cumulative <= 0:
            cumulative = max(1, int(child.quantity) // 2)
        if cumulative > 0:
            trade_price = float(child.metadata.get("last_trade_price") or child.price or 0)
            trades.append(
                {
                    "shadow_key": f"trade_{_shadow_child_order_key(child, index)}",
                    "symbol": child.symbol,
                    "side": child.side.value,
                    "quantity": cumulative,
                    "price": trade_price,
                    "child_order_id": child.child_order_id,
                }
            )
            signed_quantity = cumulative if child.side == OrderSide.BUY else -cumulative
            position = positions.setdefault(
                child.symbol.upper(),
                {"quantity": 0, "market_value": 0.0},
            )
            position["quantity"] = int(position["quantity"]) + signed_quantity
            position["market_value"] = round(float(position["quantity"]) * trade_price, 6)
            cash["available_cash"] = round(
                float(cash["available_cash"])
                + (trade_price * cumulative * (-1 if child.side == OrderSide.BUY else 1)),
                6,
            )
    return MiniQMTShadowRuntimeSnapshot(
        runtime_id=runtime_id,
        runtime_kind=runtime_kind,
        ledger=MiniQMTShadowLedgerSnapshot(
            child_orders=child_orders,
            trades=trades,
            cash=cash,
            positions=positions,
        ),
        metadata=dict(metadata),
    )


def _shadow_child_order_payload(child: Any, *, shadow_key: str) -> dict[str, Any]:
    payload = child.model_dump(mode="json")
    payload["shadow_key"] = shadow_key
    return payload


def _shadow_child_order_key(child: Any, index: int) -> str:
    return str(
        child.metadata.get("shadow_key")
        or child.metadata.get("managed_parent_intent_id")
        or child.metadata.get("paper_parent_intent_id")
        or child.parent_intent_id
        or f"child_{index}"
    )


def _refresh_shadow_gateway_snapshots(
    *,
    runtime: MiniQMTExecutionRuntime,
    gateway: _RecordingShadowGateway,
) -> None:
    gateway.order_snapshots = [
        _broker_order_snapshot_from_child(child)
        for child in runtime.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
    ]
    gateway.trade_snapshots = [
        {
            "trade_id": f"shadow_trade_{child.child_order_id}",
            "qmt_order_id": child.broker_order_id or child.child_order_id,
            "order_id": child.broker_order_id or child.child_order_id,
            "stock_code": child.symbol,
            "symbol": child.symbol,
            "order_side": child.side.value,
            "traded_volume": int(child.metadata.get("cumulative_quantity") or child.quantity),
            "traded_price": float(child.metadata.get("last_trade_price") or child.price or _DEFAULT_PRICE),
        }
        for child in runtime.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
        if child.status in {MiniQMTChildOrderStatus.PARTIALLY_FILLED, MiniQMTChildOrderStatus.FILLED}
    ]


def _broker_order_snapshot_from_child(child: Any) -> dict[str, Any]:
    if child.status == MiniQMTChildOrderStatus.FILLED:
        status = STATUS_FILLED
    elif child.status == MiniQMTChildOrderStatus.PARTIALLY_FILLED:
        status = STATUS_PART_SUCC
    elif child.status == MiniQMTChildOrderStatus.CANCELLED:
        status = STATUS_CANCELLED
    elif child.status == MiniQMTChildOrderStatus.REJECTED:
        status = STATUS_REJECTED
    else:
        status = STATUS_OPEN_LIKE
    return {
        "qmt_order_id": child.broker_order_id or child.child_order_id,
        "order_id": child.broker_order_id or child.child_order_id,
        "stock_code": child.symbol,
        "symbol": child.symbol,
        "order_side": child.side.value,
        "order_volume": int(child.quantity),
        "traded_volume": int(child.metadata.get("cumulative_quantity") or 0),
        "order_status": status,
        "price": float(child.price or 0),
        "status_msg": "shadow restart recovery snapshot",
    }


def _assert_shadow_snapshot_is_dry_run(snapshot: MiniQMTShadowRuntimeSnapshot) -> None:
    if snapshot.metadata.get("broker_mutated") is True:
        raise RuntimeError(
            "MiniQMT shadow runtime reported broker mutation; shadow mode must be dry-run only; "
            f"reason_code=MINIQMT_SHADOW_BROKER_MUTATION_DETECTED, runtime_id={snapshot.runtime_id}, "
            f"runtime_kind={snapshot.runtime_kind}"
        )


def _diff_snapshots(
    a_snapshot: MiniQMTShadowRuntimeSnapshot,
    b_snapshot: MiniQMTShadowRuntimeSnapshot,
) -> list[MiniQMTShadowDifference]:
    differences: list[MiniQMTShadowDifference] = []
    _append_count_diff(differences, a_snapshot=a_snapshot, b_snapshot=b_snapshot)
    _append_child_order_diffs(differences, a_snapshot=a_snapshot, b_snapshot=b_snapshot)
    _append_trade_diffs(differences, a_snapshot=a_snapshot, b_snapshot=b_snapshot)
    _append_mapping_diff(
        differences,
        field="cash",
        reason_code="MINIQMT_SHADOW_CASH_DRIFT",
        a_value=_normalize_money_map(a_snapshot.ledger.cash),
        b_value=_normalize_money_map(b_snapshot.ledger.cash),
    )
    _append_mapping_diff(
        differences,
        field="positions",
        reason_code="MINIQMT_SHADOW_POSITION_DRIFT",
        a_value=_normalize_positions(a_snapshot.ledger.positions),
        b_value=_normalize_positions(b_snapshot.ledger.positions),
    )
    return differences


def _append_count_diff(
    differences: list[MiniQMTShadowDifference],
    *,
    a_snapshot: MiniQMTShadowRuntimeSnapshot,
    b_snapshot: MiniQMTShadowRuntimeSnapshot,
) -> None:
    a_count = len(a_snapshot.ledger.child_orders)
    b_count = len(b_snapshot.ledger.child_orders)
    if a_count != b_count:
        differences.append(
            MiniQMTShadowDifference(
                field="child_order_count",
                severity=MiniQMTShadowSeverity.FATAL,
                reason_code="MINIQMT_SHADOW_CHILD_ORDER_COUNT_DRIFT",
                message="A/B child order counts diverged",
                a_value=a_count,
                b_value=b_count,
            )
        )


def _append_child_order_diffs(
    differences: list[MiniQMTShadowDifference],
    *,
    a_snapshot: MiniQMTShadowRuntimeSnapshot,
    b_snapshot: MiniQMTShadowRuntimeSnapshot,
) -> None:
    a_orders = _orders_by_key(a_snapshot.ledger.child_orders)
    b_orders = _orders_by_key(b_snapshot.ledger.child_orders)
    for key in sorted(set(a_orders) | set(b_orders)):
        a_order = a_orders.get(key)
        b_order = b_orders.get(key)
        if a_order is None or b_order is None:
            differences.append(
                MiniQMTShadowDifference(
                    field=f"child_orders[{key}]",
                    severity=MiniQMTShadowSeverity.FATAL,
                    reason_code="MINIQMT_SHADOW_CHILD_ORDER_MISSING",
                    message="child order missing from one runtime",
                    a_value=a_order,
                    b_value=b_order,
                )
            )
            continue
        for field_name in ("symbol", "side", "quantity", "price", "status"):
            a_value = _normalized_order_field(a_order, field_name)
            b_value = _normalized_order_field(b_order, field_name)
            if a_value != b_value:
                differences.append(
                    MiniQMTShadowDifference(
                        field=f"child_orders[{key}].{field_name}",
                        severity=_order_diff_severity(field_name, a_value=a_value, b_value=b_value),
                        reason_code=f"MINIQMT_SHADOW_CHILD_ORDER_{field_name.upper()}_DRIFT",
                        message="A/B child order field diverged",
                        a_value=a_value,
                        b_value=b_value,
                    )
                )


def _append_trade_diffs(
    differences: list[MiniQMTShadowDifference],
    *,
    a_snapshot: MiniQMTShadowRuntimeSnapshot,
    b_snapshot: MiniQMTShadowRuntimeSnapshot,
) -> None:
    a_trades = _trades_by_key(a_snapshot.ledger.trades)
    b_trades = _trades_by_key(b_snapshot.ledger.trades)
    for key in sorted(set(a_trades) | set(b_trades)):
        a_trade = a_trades.get(key)
        b_trade = b_trades.get(key)
        if a_trade is None or b_trade is None:
            differences.append(
                MiniQMTShadowDifference(
                    field=f"trades[{key}]",
                    severity=MiniQMTShadowSeverity.FATAL,
                    reason_code="MINIQMT_SHADOW_TRADE_MISSING",
                    message="trade missing from one runtime",
                    a_value=a_trade,
                    b_value=b_trade,
                )
            )
            continue
        for field_name in ("symbol", "side", "quantity", "price"):
            a_value = _normalized_trade_field(a_trade, field_name)
            b_value = _normalized_trade_field(b_trade, field_name)
            if a_value != b_value:
                differences.append(
                    MiniQMTShadowDifference(
                        field=f"trades[{key}].{field_name}",
                        severity=MiniQMTShadowSeverity.FATAL,
                        reason_code=f"MINIQMT_SHADOW_TRADE_{field_name.upper()}_DRIFT",
                        message="A/B trade field diverged",
                        a_value=a_value,
                        b_value=b_value,
                    )
                )


def _append_mapping_diff(
    differences: list[MiniQMTShadowDifference],
    *,
    field: str,
    reason_code: str,
    a_value: dict[str, Any],
    b_value: dict[str, Any],
) -> None:
    if a_value != b_value:
        differences.append(
            MiniQMTShadowDifference(
                field=field,
                severity=MiniQMTShadowSeverity.FATAL,
                reason_code=reason_code,
                message=f"A/B {field} ledger diverged",
                a_value=a_value,
                b_value=b_value,
            )
        )


def _orders_by_key(orders: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_order_key(item): dict(item) for item in orders}


def _trades_by_key(trades: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_trade_key(item): dict(item) for item in trades}


def _order_key(order: dict[str, Any]) -> str:
    return str(
        order.get("shadow_key")
        or order.get("order_remark")
        or order.get("child_order_id")
        or order.get("broker_order_id")
        or order.get("qmt_order_id")
    )


def _trade_key(trade: dict[str, Any]) -> str:
    return str(trade.get("shadow_key") or trade.get("trade_id") or trade.get("fill_id") or _order_key(trade))


def _normalized_order_field(order: dict[str, Any], field_name: str) -> Any:
    if field_name == "status":
        return _normalize_status(order.get("status") or order.get("order_status") or order.get("state"))
    if field_name == "quantity":
        return int(order.get("quantity") or order.get("order_volume") or order.get("volume") or 0)
    if field_name == "price":
        return _money(order.get("price") or order.get("limit_price") or 0)
    if field_name == "symbol":
        return str(order.get("symbol") or order.get("stock_code") or "").strip().upper()
    if field_name == "side":
        return str(order.get("side") or order.get("order_side") or order.get("order_type") or "").strip().upper()
    return order.get(field_name)


def _normalized_trade_field(trade: dict[str, Any], field_name: str) -> Any:
    if field_name == "quantity":
        return int(trade.get("quantity") or trade.get("traded_volume") or trade.get("volume") or 0)
    if field_name == "price":
        return _money(trade.get("price") or trade.get("traded_price") or 0)
    if field_name == "symbol":
        return str(trade.get("symbol") or trade.get("stock_code") or "").strip().upper()
    if field_name == "side":
        return str(trade.get("side") or trade.get("order_side") or "").strip().upper()
    return trade.get(field_name)


def _normalize_status(value: Any) -> str:
    raw = str(value or "").strip().upper()
    mapped = {
        str(MiniQMTChildOrderStatus.SUBMITTING.value): "OPEN",
        str(MiniQMTChildOrderStatus.SUBMITTED.value): "OPEN",
        str(MiniQMTChildOrderStatus.PARTIALLY_FILLED.value): "PARTIAL",
        str(MiniQMTChildOrderStatus.FILLED.value): "FILLED",
        str(MiniQMTChildOrderStatus.CANCELLED.value): "CANCELLED",
        str(MiniQMTChildOrderStatus.REJECTED.value): "REJECTED",
    }
    if raw in mapped:
        return mapped[raw]
    status = _int_or_none(raw)
    if is_partial_order_status(status):
        return "PARTIAL"
    if is_terminal_order_status(status):
        if status == STATUS_CANCELLED:
            return "CANCELLED"
        if status == STATUS_FILLED:
            return "FILLED"
        if status == STATUS_REJECTED:
            return "REJECTED"
    if is_open_like_order_status(status):
        return "OPEN"
    return raw


def _order_diff_severity(field_name: str, *, a_value: Any, b_value: Any) -> MiniQMTShadowSeverity:
    if field_name == "status" and {a_value, b_value} <= {"OPEN", "PARTIAL"}:
        return MiniQMTShadowSeverity.WARNING
    return MiniQMTShadowSeverity.FATAL


def _normalize_money_map(value: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _money(item) for key, item in sorted(dict(value or {}).items())}


def _normalize_positions(value: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for symbol, item in sorted(dict(value or {}).items()):
        if isinstance(item, dict):
            normalized[str(symbol).upper()] = {
                key: _money(raw) if key in {"quantity", "available_quantity", "market_value", "cost"} else raw
                for key, raw in sorted(item.items())
            }
        else:
            normalized[str(symbol).upper()] = _money(item)
    return normalized


def _money(value: Any) -> float:
    return round(float(value or 0), 6)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _trade_date_from_metadata(metadata: dict[str, Any]):
    from datetime import UTC, date, datetime

    raw = metadata.get("trade_date")
    if raw:
        return date.fromisoformat(str(raw))
    return datetime.now(UTC).date()
