"""MiniQMTExecutionRuntime durable event loop and runtime-owned algo adapter."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any, Callable

from backend.execution_algos.vnpy_style import (
    VnpyAction,
    VnpyActionType,
    VnpyAlgoStatus,
    VnpyOrderUpdate,
    VnpyTick,
    VnpyTradeUpdate,
    create_vnpy_style_core,
    get_vnpy_style_asset,
    is_vnpy_style_algo,
)
from backend.execution_algos.vnpy_style.base import VnpyAlgoTemplate
from backend.services.qmt_strategy_ledger.models import (
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_REJECTED,
    is_open_like_order_status,
    is_partial_order_status,
    is_terminal_order_status,
)
from backend.services.trading_core.models import OrderSide

from .gateway import MiniQMTGateway
from .models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
    MiniQMTGatewayState,
    MiniQMTOmsState,
    MiniQMTOperatorCommandResult,
    MiniQMTOperatorCommandStatus,
    MiniQMTRuntimeRecoverySnapshot,
)
from .oms import MiniQMTOmsLedger
from .repository import MiniQMTExecutionRuntimeRepository
from .risk import MiniQMTRiskDecisionAction, MiniQMTRiskEngine, NoopMiniQMTRiskEngine


class MiniQMTExecutionEventLoop:
    """Persist-first event loop for fake-broker Phase 2 validation."""

    def __init__(self, *, repository: MiniQMTExecutionRuntimeRepository) -> None:
        self._repository = repository

    def append(
        self,
        *,
        runtime_id: str,
        event_type: MiniQMTExecutionEventType,
        source: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        event = MiniQMTExecutionEvent(
            runtime_id=runtime_id,
            sequence=self._repository.next_event_sequence(runtime_id),
            event_type=event_type,
            source=source,
            payload=dict(payload or {}),
        )
        return self._repository.append_event(event)


class MiniQMTExecutionRuntime:
    """Single execution owner for future MiniQMT product paths.

    Phase 2 provides durable runtime/event/gateway/OMS behavior with a fake
    broker. Phase 3 attaches vn.py-derived algo cores while keeping gateway
    submission/cancel ownership inside this runtime. Phase 4 will route Paper
    v2/simulation_runtime clients.
    """

    def __init__(
        self,
        *,
        config: MiniQMTExecutionRuntimeConfig,
        repository: MiniQMTExecutionRuntimeRepository,
        gateway: MiniQMTGateway,
        strategy_ledger_repository: Any | None = None,
        account_id: str | None = None,
        risk_engine: MiniQMTRiskEngine | None = None,
    ) -> None:
        self.config = config
        self.repository = repository
        self.gateway = gateway
        self.events = MiniQMTExecutionEventLoop(repository=repository)
        self.oms = MiniQMTOmsLedger(
            repository,
            strategy_ledger_repository=strategy_ledger_repository,
            account_id=account_id or config.account_group_id,
            trade_date=config.trade_date,
        )
        self.risk_engine = risk_engine or NoopMiniQMTRiskEngine()
        self._kill_switch_active = False
        self._vnpy_cores: dict[str, VnpyAlgoTemplate] = {}
        self._vnpy_random_volume_providers: dict[str, Callable[[int, int], float]] = {}
        event_sink_binder = getattr(gateway, "bind_event_sink", None)
        if callable(event_sink_binder):
            event_sink_binder(self)

    def start(self) -> MiniQMTExecutionRuntimeRecord:
        runtime = self.repository.get_runtime(self.config.runtime_id)
        if runtime is None:
            runtime = MiniQMTExecutionRuntimeRecord(
                runtime_id=self.config.runtime_id,
                account_group_id=self.config.account_group_id,
                trade_date=self.config.trade_date,
                mode=self.config.mode,
                event_loop_state=MiniQMTExecutionRuntimeState.CREATED,
                runtime_config_hash=self.config.runtime_config_hash,
                metadata=self.config.metadata,
            )
            runtime = self.repository.upsert_runtime(runtime)
            self.events.append(
                runtime_id=runtime.runtime_id,
                event_type=MiniQMTExecutionEventType.RUNTIME_CREATED,
                source="runtime",
                payload={"account_group_id": runtime.account_group_id, "mode": runtime.mode.value},
            )

        self.gateway.connect(runtime_id=runtime.runtime_id)
        runtime = self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.READY,
                    "gateway_state": MiniQMTGatewayState.CONNECTED,
                }
            )
        )
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.GATEWAY_CONNECTED,
            source="gateway",
            payload={"gateway_state": runtime.gateway_state.value},
        )
        return runtime

    def recover(self) -> MiniQMTRuntimeRecoverySnapshot:
        runtime = self.repository.get_runtime(self.config.runtime_id)
        if runtime is None:
            runtime = self.start()
        runtime = self.repository.upsert_runtime(
            runtime.model_copy(update={"event_loop_state": MiniQMTExecutionRuntimeState.RECOVERING})
        )
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.BROKER_SYNC_STARTED,
            source="recovery",
            payload={"reason": "process_restart"},
        )
        broker_orders = self.gateway.sync_orders(runtime_id=runtime.runtime_id)
        broker_trades = self.gateway.sync_trades(runtime_id=runtime.runtime_id)
        broker_positions = self.gateway.sync_positions(runtime_id=runtime.runtime_id)
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.BROKER_SYNCED,
            source="gateway",
            payload={
                "orders": broker_orders,
                "trades": broker_trades,
                "positions": broker_positions,
                "sync_before_new_orders": True,
            },
        )
        self.oms.reconcile_child_orders_from_ledger(runtime.runtime_id)
        self._reconcile_child_orders_from_broker_snapshot(
            runtime.runtime_id,
            broker_orders=broker_orders,
            broker_trades=broker_trades,
            source="recovery",
        )
        orphaned_algo_ids = self._terminalize_orphaned_active_algos(
            runtime.runtime_id,
            reason="process_restart_recovery",
        )
        latest_runtime = self.repository.get_runtime(runtime.runtime_id) or runtime
        runtime = self.repository.upsert_runtime(
            latest_runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.READY,
                    "gateway_state": MiniQMTGatewayState.CONNECTED,
                    "oms_state": MiniQMTOmsState.RECONCILED,
                    "metadata": {
                        **dict(latest_runtime.metadata),
                        "last_recovery_terminalized_orphaned_algo_instance_ids": orphaned_algo_ids,
                    },
                }
            )
        )
        return MiniQMTRuntimeRecoverySnapshot(
            runtime=runtime,
            events=self.repository.list_events(runtime.runtime_id),
            active_algo_instances=self.repository.list_algo_instances(runtime.runtime_id, active_only=True),
            active_child_orders=self.repository.list_child_orders(runtime.runtime_id, active_only=True),
            broker_orders=broker_orders,
            broker_trades=broker_trades,
            broker_positions=broker_positions,
        )

    def create_algo_instance(
        self,
        *,
        parent_intent_id: str,
        strategy_slot_id: str,
        symbol: str,
        side: OrderSide,
        target_quantity: int,
        algo_code: str,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionAlgoInstance:
        runtime = self._require_runtime()
        instance = MiniQMTExecutionAlgoInstance(
            runtime_id=runtime.runtime_id,
            parent_intent_id=parent_intent_id,
            strategy_slot_id=strategy_slot_id,
            symbol=symbol,
            side=side,
            target_quantity=target_quantity,
            remaining_quantity=target_quantity,
            algo_code=algo_code,
            metadata=dict(metadata or {}),
        )
        instance = self.oms.record_algo_instance(instance)
        self.repository.upsert_runtime(runtime.model_copy(update={"event_loop_state": MiniQMTExecutionRuntimeState.RUNNING}))
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.ALGO_INSTANCE_CREATED,
            source="algo",
            payload={
                "algo_instance_id": instance.algo_instance_id,
                "parent_intent_id": parent_intent_id,
                "strategy_slot_id": strategy_slot_id,
                "algo_code": algo_code,
            },
        )
        return instance

    def create_vnpy_algo_instance(
        self,
        *,
        parent_intent_id: str,
        strategy_slot_id: str,
        symbol: str,
        side: OrderSide,
        target_quantity: int,
        algo_code: str,
        limit_price: float,
        algo_config: dict[str, Any] | None = None,
        min_volume: int = 100,
        volume_increment: int = 100,
        random_volume_provider: Callable[[int, int], float] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionAlgoInstance:
        """Create a runtime-owned vn.py-style algo instance.

        Phase 3 keeps the algo core broker-neutral: the core emits actions,
        while this runtime owns gateway submission, event persistence, and OMS
        mapping.
        """

        normalized_algo_code = str(algo_code or "").strip().upper()
        if not is_vnpy_style_algo(normalized_algo_code):
            raise RuntimeError(f"unsupported runtime-owned vn.py-style algo: {algo_code}")
        spec = get_vnpy_style_asset(normalized_algo_code)
        instance_metadata = {
            **dict(metadata or {}),
            "runtime_algo_family": "vnpy_style",
            "limit_price": float(limit_price),
            "algo_config": dict(algo_config or {}),
            "min_volume": int(min_volume),
            "volume_increment": int(volume_increment),
            "execution_asset_version": spec.version,
            "source_attribution": spec.metadata()["source_attribution"],
        }
        instance = self.create_algo_instance(
            parent_intent_id=parent_intent_id,
            strategy_slot_id=strategy_slot_id,
            symbol=symbol,
            side=side,
            target_quantity=target_quantity,
            algo_code=normalized_algo_code,
            metadata=instance_metadata,
        )
        if random_volume_provider is not None:
            self._vnpy_random_volume_providers[instance.algo_instance_id] = random_volume_provider
        core = self._ensure_vnpy_core(instance)
        actions = core.start()
        self._persist_vnpy_core_state(instance, core)
        self._handle_vnpy_actions(instance, actions)
        return self._find_algo_instance(instance.runtime_id, instance.algo_instance_id) or instance

    def on_timer(self, *, timer_name: str, payload: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        event = self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.TIMER,
            source="runtime",
            payload={"timer_name": timer_name, **dict(payload or {})},
        )
        if self._evaluate_risk_after_event(runtime.runtime_id, event_type=event.event_type, payload=event.payload):
            return event
        self._dispatch_timer_to_vnpy_algos(runtime.runtime_id)
        return event

    def on_tick(self, *, symbol: str, price: float, payload: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        tick_payload = {"symbol": symbol, "price": price, **dict(payload or {})}
        event = self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.TICK,
            source="gateway",
            payload=tick_payload,
        )
        if self._evaluate_risk_after_event(runtime.runtime_id, event_type=event.event_type, payload=event.payload):
            return event
        self._dispatch_tick_to_vnpy_algos(runtime.runtime_id, tick_payload=tick_payload)
        return event

    def submit_child_order(
        self,
        *,
        algo_instance_id: str,
        quantity: int,
        price: float,
        price_type: int = 11,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTChildOrder:
        runtime = self._require_runtime()
        self._raise_if_kill_switch_blocks_submit(runtime, algo_instance_id)
        instance = self._require_algo_instance(runtime.runtime_id, algo_instance_id)
        self._raise_if_kill_switch_active(runtime.runtime_id, instance)
        order = MiniQMTChildOrder(
            runtime_id=runtime.runtime_id,
            algo_instance_id=instance.algo_instance_id,
            parent_intent_id=instance.parent_intent_id,
            strategy_slot_id=instance.strategy_slot_id,
            symbol=instance.symbol,
            side=instance.side,
            quantity=quantity,
            price=price,
            price_type=price_type,
            metadata=dict(metadata or {}),
        )
        self.oms.record_child_order(order)
        ack = self.gateway.submit_child_order(order)
        submitted = order.model_copy(
            update={
                "broker_order_id": ack.broker_order_id,
                "status": MiniQMTChildOrderStatus.SUBMITTED if ack.accepted else MiniQMTChildOrderStatus.REJECTED,
                "submitted_at": datetime.now(UTC) if ack.accepted else None,
                "metadata": {**order.metadata, "gateway_ack": ack.raw, "gateway_message": ack.message},
            }
        )
        submitted = self.oms.record_child_order(submitted)
        runtime = self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.RUNNING,
                    "oms_state": MiniQMTOmsState.OPEN,
                }
            )
        )
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=(
                MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED
                if ack.accepted
                else MiniQMTExecutionEventType.CHILD_ORDER_REJECTED
            ),
            source="gateway",
            payload={
                "child_order_id": submitted.child_order_id,
                "algo_instance_id": algo_instance_id,
                "broker_order_id": submitted.broker_order_id,
                "accepted": ack.accepted,
                "message": ack.message,
            },
        )
        return submitted

    def record_external_child_order(
        self,
        *,
        algo_instance_id: str,
        quantity: int,
        price: float,
        price_type: int = 11,
        status: MiniQMTChildOrderStatus = MiniQMTChildOrderStatus.SUBMITTED,
        broker_order_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTChildOrder:
        """Attach an externally-submitted child order to the runtime ledger.

        Phase 4 product clients may still use legacy broker/managed-order
        gateways while the canonical owner records algo/child-order evidence.
        """

        runtime = self._require_runtime()
        instance = self._require_algo_instance(runtime.runtime_id, algo_instance_id)
        order = MiniQMTChildOrder(
            runtime_id=runtime.runtime_id,
            algo_instance_id=instance.algo_instance_id,
            parent_intent_id=instance.parent_intent_id,
            strategy_slot_id=instance.strategy_slot_id,
            symbol=instance.symbol,
            side=instance.side,
            quantity=quantity,
            price=price,
            price_type=price_type,
            status=status,
            broker_order_id=broker_order_id,
            submitted_at=datetime.now(UTC) if status == MiniQMTChildOrderStatus.SUBMITTED else None,
            metadata=dict(metadata or {}),
        )
        order = self.oms.record_child_order(order)
        runtime = self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.RUNNING,
                    "oms_state": MiniQMTOmsState.OPEN,
                }
            )
        )
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=(
                MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED
                if status == MiniQMTChildOrderStatus.SUBMITTED
                else MiniQMTExecutionEventType.CHILD_ORDER_REJECTED
            ),
            source="gateway",
            payload={
                "child_order_id": order.child_order_id,
                "algo_instance_id": algo_instance_id,
                "broker_order_id": broker_order_id,
                "status": status.value,
                "external_gateway_record": True,
            },
        )
        return order

    def record_operator_command(
        self,
        *,
        command_id: str,
        command_type: str,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        return self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.OPERATOR_COMMAND_RECEIVED,
            source="operator",
            payload={
                "command_id": command_id,
                "command_type": command_type,
                "reason": reason,
                **dict(payload or {}),
            },
        )

    def execute_operator_command(
        self,
        *,
        command_id: str,
        command_type: str,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTOperatorCommandResult:
        """Execute a user/operator command inside the canonical runtime owner.

        ``record_operator_command`` remains audit-only for preview paths. This
        method is the product boundary that mutates gateway/OMS state.
        """

        command_payload = dict(payload or {})
        self.record_operator_command(
            command_id=command_id,
            command_type=command_type,
            reason=reason,
            payload=command_payload,
        )
        normalized = str(command_type or "").strip().upper()
        if normalized == "CANCEL_ALL_OPEN_ORDERS":
            return self._execute_cancel_all_open_orders(
                command_id=command_id,
                command_type=normalized,
                reason=reason,
                payload=command_payload,
            )
        if normalized in {"FLATTEN_ALL_POSITIONS", "FLATTEN_STRATEGY_SLOT"}:
            return self._execute_flatten_positions(
                command_id=command_id,
                command_type=normalized,
                reason=reason,
                payload=command_payload,
            )
        if normalized == "RESET_STRATEGY_SLOT":
            return self._execute_reset_strategy_slot(
                command_id=command_id,
                command_type=normalized,
                reason=reason,
                payload=command_payload,
            )
        if normalized == "REPLACE_ALPHA_SIGNAL_BOOK":
            return self._execute_replace_alpha_signal_book(
                command_id=command_id,
                command_type=normalized,
                reason=reason,
                payload=command_payload,
            )
        return self._operator_result(
            command_id=command_id,
            command_type=normalized or command_type,
            status=MiniQMTOperatorCommandStatus.REJECTED,
            reason=reason,
            payload=command_payload,
            errors=[
                {
                    "error_code": "MINIQMT_OPERATOR_COMMAND_UNSUPPORTED",
                    "message": "unsupported MiniQMT operator command",
                    "context": {"command_type": command_type},
                }
            ],
        )

    def record_order_event(
        self,
        *,
        broker_order_id: str,
        status: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        event = self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.ORDER_EVENT,
            source="gateway",
            payload={"broker_order_id": broker_order_id, "status": status, **dict(payload or {})},
        )
        risk_triggered = False
        child = self._find_child_order(runtime.runtime_id, broker_order_id=broker_order_id)
        if child is not None:
            broker_status_payload = {**dict(payload or {}), "order_status": status}
            broker_status_payload.setdefault("status", status)
            child_status = _child_status_from_broker_order_snapshot(
                broker_status_payload,
                child_quantity=child.quantity,
                broker_trades=[],
            )
            child_metadata = {
                **dict(child.metadata),
                "status_msg": str((payload or {}).get("status_msg") or child.metadata.get("status_msg") or ""),
                "broker_order_event": dict(payload or {}),
            }
            child = self.oms.record_child_order(
                child.model_copy(update={"status": child_status, "metadata": child_metadata})
            )
            risk_triggered = self._evaluate_risk_after_event(
                runtime.runtime_id,
                event_type=event.event_type,
                payload=event.payload,
            )
            instance = self._find_algo_instance(runtime.runtime_id, child.algo_instance_id)
            if instance is not None and self._is_vnpy_instance(instance) and not risk_triggered:
                core = self._ensure_vnpy_core(instance)
                actions = core.update_order(
                    VnpyOrderUpdate(
                        vt_orderid=str(child.metadata.get("vnpy_vt_orderid") or child.child_order_id),
                        active=child_status not in _TERMINAL_CHILD_ORDER_STATUSES,
                        traded=int((payload or {}).get("traded") or (payload or {}).get("filled_quantity") or 0),
                        price=_optional_float((payload or {}).get("price") or child.price),
                        raw_status=str(status),
                        status_msg=str((payload or {}).get("status_msg") or ""),
                        raw={"broker_order_id": broker_order_id, **dict(payload or {})},
                    )
                )
                self._persist_vnpy_core_state(instance, core)
                self._handle_vnpy_actions(instance, actions)
            if child.status in _TERMINAL_CHILD_ORDER_STATUSES:
                self._terminalize_algo_if_all_children_terminal(
                    runtime.runtime_id,
                    child.algo_instance_id,
                    reason=f"broker_order_{child.status.value.lower()}",
                )
        else:
            self._evaluate_risk_after_event(runtime.runtime_id, event_type=event.event_type, payload=event.payload)
        return event

    def record_trade_event(
        self,
        *,
        broker_order_id: str,
        quantity: int,
        price: float,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        event = self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.TRADE_EVENT,
            source="gateway",
            payload={"broker_order_id": broker_order_id, "quantity": quantity, "price": price, **dict(payload or {})},
        )
        risk_triggered = False
        child = self._find_child_order(runtime.runtime_id, broker_order_id=broker_order_id)
        if child is not None:
            cumulative_quantity = int(
                (payload or {}).get("cumulative_quantity") or (payload or {}).get("filled_quantity") or quantity
            )
            updated_child = child.model_copy(
                update={
                    "status": MiniQMTChildOrderStatus.FILLED
                    if cumulative_quantity >= child.quantity
                    else MiniQMTChildOrderStatus.PARTIALLY_FILLED,
                    "metadata": {
                        **dict(child.metadata),
                        "last_trade_event": {
                            "broker_order_id": broker_order_id,
                            "quantity": quantity,
                            "price": price,
                            **dict(payload or {}),
                        },
                        "last_trade_price": price,
                        "cumulative_quantity": cumulative_quantity,
                    },
                }
            )
            self.oms.record_trade_fill(updated_child, quantity=quantity, price=price, payload=payload)
            child = self.oms.record_child_order(updated_child)
            risk_triggered = self._evaluate_risk_after_event(
                runtime.runtime_id,
                event_type=event.event_type,
                payload=event.payload,
            )
            instance = self._find_algo_instance(runtime.runtime_id, child.algo_instance_id)
            if instance is not None and self._is_vnpy_instance(instance) and not risk_triggered:
                core = self._ensure_vnpy_core(instance)
                actions = []
                if cumulative_quantity >= child.quantity:
                    actions.extend(
                        core.update_order(
                            VnpyOrderUpdate(
                                vt_orderid=str(child.metadata.get("vnpy_vt_orderid") or child.child_order_id),
                                active=False,
                                traded=cumulative_quantity,
                                price=float(price),
                                raw_status="FILLED",
                                raw={"broker_order_id": broker_order_id, **dict(payload or {})},
                            )
                        )
                    )
                actions.extend(
                    core.update_trade(
                        VnpyTradeUpdate(
                            vt_orderid=str(child.metadata.get("vnpy_vt_orderid") or child.child_order_id),
                            volume=int(quantity),
                            price=float(price),
                            raw={"broker_order_id": broker_order_id, **dict(payload or {})},
                        )
                    )
                )
                self._persist_vnpy_core_state(instance, core)
                self._handle_vnpy_actions(instance, actions)
            if child.status in _TERMINAL_CHILD_ORDER_STATUSES:
                self._terminalize_algo_if_all_children_terminal(
                    runtime.runtime_id,
                    child.algo_instance_id,
                    reason=f"broker_trade_{child.status.value.lower()}",
                )
        else:
            self._evaluate_risk_after_event(runtime.runtime_id, event_type=event.event_type, payload=event.payload)
        return event

    def record_account_event(self, *, payload: dict[str, Any]) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        event = self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.ACCOUNT_EVENT,
            source="gateway",
            payload=dict(payload),
        )
        self._evaluate_risk_after_event(runtime.runtime_id, event_type=event.event_type, payload=event.payload)
        return event

    def record_disconnect_event(
        self,
        *,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        event_payload = {
            "reason_code": "MINIQMT_GATEWAY_DISCONNECTED",
            "reason": reason,
            **dict(payload or {}),
        }
        event = self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.GATEWAY_DISCONNECTED,
            source="gateway",
            payload=event_payload,
        )
        self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.PAUSED,
                    "gateway_state": MiniQMTGatewayState.DISCONNECTED,
                    "metadata": {
                        **dict(runtime.metadata),
                        "last_disconnect_reason_code": event_payload["reason_code"],
                        "last_disconnect_reason": reason,
                    },
                }
            )
        )
        self._evaluate_risk_after_event(runtime.runtime_id, event_type=event.event_type, payload=event.payload)
        return event

    def _reconcile_child_orders_from_broker_snapshot(
        self,
        runtime_id: str,
        *,
        broker_orders: list[dict[str, Any]],
        broker_trades: list[dict[str, Any]],
        source: str,
    ) -> None:
        """Backfill durable child statuses from broker truth after sync."""

        trades_by_order_id: dict[str, list[dict[str, Any]]] = {}
        for trade in broker_trades:
            broker_order_id = _broker_order_id(trade) or _optional_text(trade.get("order_id"))
            if not broker_order_id:
                continue
            trades_by_order_id.setdefault(broker_order_id, []).append(dict(trade))

        for broker_order in broker_orders:
            broker_order_id = _broker_order_id(broker_order)
            if not broker_order_id:
                continue
            child = self._find_child_order(runtime_id, broker_order_id=broker_order_id)
            if child is None:
                continue
            status = _child_status_from_broker_order_snapshot(
                broker_order,
                child_quantity=child.quantity,
                broker_trades=trades_by_order_id.get(broker_order_id, []),
            )
            if status is None:
                continue
            metadata = {
                **dict(child.metadata),
                "broker_reconciled_status": status.value,
                "broker_reconcile_source": source,
                "broker_reconcile_order": dict(broker_order),
            }
            if trades_by_order_id.get(broker_order_id):
                metadata["broker_reconcile_trades"] = [dict(item) for item in trades_by_order_id[broker_order_id]]
            updated = self.oms.record_child_order(
                child.model_copy(
                    update={
                        "status": status,
                        "metadata": metadata,
                    }
                )
            )
            if updated.status in _TERMINAL_CHILD_ORDER_STATUSES:
                self._terminalize_algo_if_all_children_terminal(
                    runtime_id,
                    updated.algo_instance_id,
                    reason=f"broker_snapshot_{updated.status.value.lower()}",
                )

    def reconcile(self) -> MiniQMTRuntimeRecoverySnapshot:
        runtime = self._require_runtime()
        self.repository.upsert_runtime(runtime.model_copy(update={"event_loop_state": MiniQMTExecutionRuntimeState.RECONCILING}))
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.RECONCILE_STARTED,
            source="runtime",
            payload={},
        )
        snapshot = self.recover()
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.RECONCILE_COMPLETED,
            source="runtime",
            payload={
                "active_algo_instances": len(snapshot.active_algo_instances),
                "active_child_orders": len(snapshot.active_child_orders),
            },
        )
        return MiniQMTRuntimeRecoverySnapshot(
            runtime=self.repository.get_runtime(runtime.runtime_id) or snapshot.runtime,
            events=self.repository.list_events(runtime.runtime_id),
            active_algo_instances=self.repository.list_algo_instances(runtime.runtime_id, active_only=True),
            active_child_orders=self.repository.list_child_orders(runtime.runtime_id, active_only=True),
            broker_orders=snapshot.broker_orders,
            broker_trades=snapshot.broker_trades,
            broker_positions=snapshot.broker_positions,
        )

    def _execute_cancel_all_open_orders(
        self,
        *,
        command_id: str,
        command_type: str,
        reason: str,
        payload: dict[str, Any],
    ) -> MiniQMTOperatorCommandResult:
        runtime = self._require_runtime()
        strategy_slot_id = _optional_text(payload.get("strategy_slot_id"))
        active_children = [
            child
            for child in self.repository.list_child_orders(runtime.runtime_id, active_only=True)
            if strategy_slot_id is None or child.strategy_slot_id == strategy_slot_id
        ]
        try:
            imported_children = self._import_active_broker_orders_for_cancel(
                command_id=command_id,
                runtime_id=runtime.runtime_id,
                strategy_slot_id=strategy_slot_id,
            )
        except Exception as exc:  # noqa: BLE001
            return self._operator_result(
                command_id=command_id,
                command_type=command_type,
                status=MiniQMTOperatorCommandStatus.REJECTED,
                reason=reason,
                payload=payload,
                errors=[
                    {
                        "error_code": "MINIQMT_OPERATOR_BROKER_SYNC_FAILED",
                        "message": "MiniQMT broker open-order sync failed before operator cancel",
                        "context": {"reason": f"{type(exc).__name__}: {exc}"},
                    }
                ],
            )
        active_children.extend(imported_children)
        cancelled_child_order_ids: list[str] = []
        broker_packets: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for child in active_children:
            ack = self.gateway.cancel_child_order(child, reason=reason)
            broker_packets.append(
                {
                    "action": "cancel_child_order",
                    "child_order_id": child.child_order_id,
                    "broker_order_id": child.broker_order_id,
                    "accepted": ack.accepted,
                    "message": ack.message,
                    "raw": dict(ack.raw),
                }
            )
            self.events.append(
                runtime_id=runtime.runtime_id,
                event_type=MiniQMTExecutionEventType.CHILD_ORDER_CANCEL_REQUESTED,
                source="operator",
                payload={
                    "command_id": command_id,
                    "child_order_id": child.child_order_id,
                    "broker_order_id": child.broker_order_id,
                    "accepted": ack.accepted,
                    "message": ack.message,
                    "reason": reason,
                },
            )
            if ack.accepted:
                cancelled_child_order_ids.append(child.child_order_id)
                self.oms.record_child_order(
                    child.model_copy(
                        update={
                            "status": MiniQMTChildOrderStatus.CANCELLED,
                            "metadata": {
                                **dict(child.metadata),
                                "operator_command_id": command_id,
                                "operator_cancel_ack": dict(ack.raw),
                            },
                        }
                    )
                )
                self._terminalize_algo_if_all_children_terminal(
                    runtime.runtime_id,
                    child.algo_instance_id,
                    reason="operator_cancel_all_open_orders",
                    command_id=command_id,
                )
            else:
                errors.append(
                    {
                        "error_code": "MINIQMT_OPERATOR_CANCEL_REJECTED",
                        "message": ack.message,
                        "context": {"child_order_id": child.child_order_id, "broker_order_id": child.broker_order_id},
                    }
                )
        status = MiniQMTOperatorCommandStatus.EXECUTED if not errors else MiniQMTOperatorCommandStatus.REJECTED
        return self._operator_result(
            command_id=command_id,
            command_type=command_type,
            status=status,
            reason=reason,
            payload=payload,
            cancelled_child_order_ids=cancelled_child_order_ids,
            affected_algo_instance_ids=_unique(child.algo_instance_id for child in active_children),
            broker_packets=broker_packets,
            errors=errors,
            metadata={"active_child_count": len(active_children), "strategy_slot_id": strategy_slot_id},
        )

    def _evaluate_risk_after_event(
        self,
        runtime_id: str,
        *,
        event_type: MiniQMTExecutionEventType,
        payload: dict[str, Any],
    ) -> bool:
        decision = self.risk_engine.evaluate_event(
            runtime_id=runtime_id,
            event_type=event_type.value,
            payload=dict(payload),
        )
        if decision.action != MiniQMTRiskDecisionAction.KILL_SWITCH:
            return False
        self._trigger_kill_switch(
            runtime_id,
            reason_code=decision.reason_code,
            reason=decision.reason,
            metadata=decision.metadata,
            source_event_type=event_type.value,
        )
        return True

    def _trigger_kill_switch(
        self,
        runtime_id: str,
        *,
        reason_code: str,
        reason: str,
        metadata: dict[str, Any],
        source_event_type: str,
    ) -> None:
        self._kill_switch_active = True
        runtime = self.repository.get_runtime(runtime_id)
        if runtime is not None:
            self.repository.upsert_runtime(
                runtime.model_copy(
                    update={
                        "event_loop_state": MiniQMTExecutionRuntimeState.PAUSED,
                        "metadata": {
                            **dict(runtime.metadata),
                            "kill_switch_active": True,
                            "kill_switch_reason_code": reason_code,
                            "kill_switch_reason": reason,
                            "kill_switch_source_event_type": source_event_type,
                        },
                    }
                )
            )
        cancelled_child_order_ids: list[str] = []
        broker_packets: list[dict[str, Any]] = []
        for child in self.repository.list_child_orders(runtime_id, active_only=True):
            ack = self.gateway.cancel_child_order(child, reason=reason)
            broker_packets.append(
                {
                    "child_order_id": child.child_order_id,
                    "broker_order_id": child.broker_order_id,
                    "accepted": ack.accepted,
                    "message": ack.message,
                    "raw": dict(ack.raw),
                }
            )
            if not ack.accepted:
                continue
            cancelled_child_order_ids.append(child.child_order_id)
            self.oms.record_child_order(
                child.model_copy(
                    update={
                        "status": MiniQMTChildOrderStatus.CANCELLED,
                        "metadata": {
                            **dict(child.metadata),
                            "risk_kill_switch_reason_code": reason_code,
                            "risk_kill_switch_reason": reason,
                            "risk_kill_switch_ack": dict(ack.raw),
                        },
                    }
                )
            )
            self._terminalize_algo_if_all_children_terminal(
                runtime_id,
                child.algo_instance_id,
                reason="risk_kill_switch",
                command_id=f"risk:{reason_code}",
            )
        self.events.append(
            runtime_id=runtime_id,
            event_type=MiniQMTExecutionEventType.RISK_KILL_SWITCH_TRIGGERED,
            source="runtime",
            payload={
                "reason_code": reason_code,
                "reason": reason,
                "source_event_type": source_event_type,
                "cancelled_child_order_ids": cancelled_child_order_ids,
                "broker_packets": broker_packets,
                "metadata": dict(metadata),
            },
        )

    def _raise_if_kill_switch_active(
        self,
        runtime_id: str,
        instance: MiniQMTExecutionAlgoInstance,
    ) -> None:
        self._raise_if_kill_switch_blocks_submit(runtime_id, instance.algo_instance_id)

    def _raise_if_kill_switch_blocks_submit(self, runtime: MiniQMTExecutionRuntimeRecord | str, algo_instance_id: str) -> None:
        runtime_record = runtime if isinstance(runtime, MiniQMTExecutionRuntimeRecord) else self.repository.get_runtime(runtime)
        runtime_id = runtime_record.runtime_id if runtime_record is not None else str(runtime)
        metadata = dict(runtime_record.metadata) if runtime_record is not None else {}
        active = self._kill_switch_active or bool(metadata.get("kill_switch_active"))
        if not active:
            return
        reason_code = str(metadata.get("kill_switch_reason_code") or "MINIQMT_RISK_KILL_SWITCH_ACTIVE")
        raise RuntimeError(
            "MiniQMT risk kill-switch is active; new child orders are blocked; "
            f"reason_code={reason_code}, runtime_id={runtime_id}, algo_instance_id={algo_instance_id}"
        )

    def _terminalize_algo_if_all_children_terminal(
        self,
        runtime_id: str,
        algo_instance_id: str,
        *,
        reason: str,
        command_id: str | None = None,
    ) -> MiniQMTExecutionAlgoInstance | None:
        instance = self._find_algo_instance(runtime_id, algo_instance_id)
        if instance is None or instance.status != MiniQMTAlgoInstanceStatus.ACTIVE:
            return None
        if self._is_vnpy_instance(instance) and command_id is None:
            return None
        children = [
            child
            for child in self.repository.list_child_orders(runtime_id, active_only=False)
            if child.algo_instance_id == algo_instance_id
        ]
        if not children or any(child.status not in _TERMINAL_CHILD_ORDER_STATUSES for child in children):
            return None
        terminal_status = _algo_terminal_status_from_child_orders(children)
        updated = self.oms.record_algo_instance(
            instance.model_copy(
                update={
                    "status": terminal_status,
                    "remaining_quantity": 0 if terminal_status == MiniQMTAlgoInstanceStatus.COMPLETED else instance.remaining_quantity,
                    "metadata": {
                        **dict(instance.metadata),
                        "terminalized_by_runtime": True,
                        "terminalized_reason": reason,
                        "terminal_child_order_statuses": sorted({child.status.value for child in children}),
                        **({"operator_command_id": command_id} if command_id else {}),
                    },
                }
            )
        )
        self.events.append(
            runtime_id=runtime_id,
            event_type=MiniQMTExecutionEventType.ALGO_ACTION_EMITTED,
            source="oms",
            payload={
                "algo_instance_id": algo_instance_id,
                "action_type": "TERMINALIZE_ORPHANED_ALGO",
                "reason": reason,
                "status": updated.status.value,
                "terminal_child_order_ids": [child.child_order_id for child in children],
            },
        )
        return updated

    def _terminalize_orphaned_active_algos(self, runtime_id: str, *, reason: str) -> list[str]:
        terminalized: list[str] = []
        for instance in list(self.repository.list_algo_instances(runtime_id, active_only=True)):
            updated = self._terminalize_algo_if_all_children_terminal(
                runtime_id,
                instance.algo_instance_id,
                reason=reason,
            )
            if updated is not None:
                terminalized.append(updated.algo_instance_id)
        return terminalized

    def _import_active_broker_orders_for_cancel(
        self,
        *,
        command_id: str,
        runtime_id: str,
        strategy_slot_id: str | None,
    ) -> list[MiniQMTChildOrder]:
        known_broker_ids = {
            child.broker_order_id
            for child in self.repository.list_child_orders(runtime_id, active_only=True)
            if child.broker_order_id
        }
        imported: list[MiniQMTChildOrder] = []
        for broker_order in self.gateway.sync_orders(runtime_id=runtime_id):
            broker_order_id = _broker_order_id(broker_order)
            broker_slot_id = _position_strategy_slot_id(broker_order) or _optional_text(broker_order.get("strategy_name"))
            if not broker_order_id or broker_order_id in known_broker_ids:
                continue
            if strategy_slot_id is not None and broker_slot_id != strategy_slot_id:
                continue
            if not _is_open_broker_order(broker_order):
                continue
            symbol = _position_symbol(broker_order)
            side = _order_side(broker_order)
            quantity = _open_order_remaining_quantity(broker_order)
            if not symbol or side is None or quantity <= 0:
                continue
            instance = self.create_algo_instance(
                parent_intent_id=f"{command_id}_broker_{broker_order_id}",
                strategy_slot_id=broker_slot_id or strategy_slot_id or "broker_open_order",
                symbol=symbol,
                side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
                target_quantity=quantity,
                algo_code="OPERATOR_IMPORT_BROKER_ORDER",
                metadata={
                    "operator_command_id": command_id,
                    "source": "operator_command_broker_sync",
                    "broker_order": dict(broker_order),
                },
            )
            child = self.record_external_child_order(
                algo_instance_id=instance.algo_instance_id,
                quantity=quantity,
                price=_position_price(broker_order),
                price_type=_order_price_type(broker_order),
                status=MiniQMTChildOrderStatus.SUBMITTED,
                broker_order_id=broker_order_id,
                metadata={
                    "operator_command_id": command_id,
                    "source": "operator_command_broker_sync",
                    "broker_order": dict(broker_order),
                },
            )
            known_broker_ids.add(broker_order_id)
            imported.append(child)
        return imported

    def _execute_flatten_positions(
        self,
        *,
        command_id: str,
        command_type: str,
        reason: str,
        payload: dict[str, Any],
    ) -> MiniQMTOperatorCommandResult:
        runtime = self._require_runtime()
        strategy_slot_id = _optional_text(payload.get("strategy_slot_id"))
        if command_type == "FLATTEN_STRATEGY_SLOT" and not strategy_slot_id:
            return self._operator_result(
                command_id=command_id,
                command_type=command_type,
                status=MiniQMTOperatorCommandStatus.REJECTED,
                reason=reason,
                payload=payload,
                errors=[
                    {
                        "error_code": "MINIQMT_OPERATOR_STRATEGY_SLOT_REQUIRED",
                        "message": "FLATTEN_STRATEGY_SLOT requires strategy_slot_id",
                        "context": {"command_id": command_id},
                    }
                ],
            )
        cancel_result = self._execute_cancel_all_open_orders(
            command_id=f"{command_id}_cancel",
            command_type="CANCEL_ALL_OPEN_ORDERS",
            reason=f"{reason}; flatten pre-cancel",
            payload={"strategy_slot_id": strategy_slot_id} if strategy_slot_id else {},
        )
        if cancel_result.errors:
            return self._operator_result(
                command_id=command_id,
                command_type=command_type,
                status=MiniQMTOperatorCommandStatus.REJECTED,
                reason=reason,
                payload=payload,
                cancelled_child_order_ids=list(cancel_result.cancelled_child_order_ids),
                broker_packets=list(cancel_result.broker_packets),
                errors=list(cancel_result.errors),
                metadata={"strategy_slot_id": strategy_slot_id, "pre_cancel_required": True},
            )
        try:
            positions = [
                item
                for item in self.gateway.sync_positions(runtime_id=runtime.runtime_id)
                if _position_quantity(item) > 0
                and (strategy_slot_id is None or _position_strategy_slot_id(item) == strategy_slot_id)
            ]
        except Exception as exc:  # noqa: BLE001
            return self._operator_result(
                command_id=command_id,
                command_type=command_type,
                status=MiniQMTOperatorCommandStatus.REJECTED,
                reason=reason,
                payload=payload,
                errors=[
                    {
                        "error_code": "MINIQMT_OPERATOR_BROKER_SYNC_FAILED",
                        "message": "MiniQMT broker position sync failed before operator flatten",
                        "context": {"reason": f"{type(exc).__name__}: {exc}"},
                    }
                ],
            )
        if (
            command_type == "FLATTEN_ALL_POSITIONS"
            and strategy_slot_id is None
            and payload.get("allow_open_sell_order_fallback") is True
        ):
            positions.extend(
                _open_sell_positions_from_broker_orders(
                    self.gateway.sync_orders(runtime_id=runtime.runtime_id),
                    known_symbols={_position_symbol(item) for item in positions},
                )
            )
        submitted_child_order_ids: list[str] = []
        broker_packets = list(cancel_result.broker_packets)
        errors = list(cancel_result.errors)
        for position in positions:
            position_slot_id = _position_strategy_slot_id(position) or strategy_slot_id or "broker_position"
            symbol = _position_symbol(position)
            quantity = _position_sellable_quantity(position)
            if not symbol or quantity <= 0:
                errors.append(
                    {
                        "error_code": "MINIQMT_OPERATOR_POSITION_NOT_SELLABLE",
                        "message": "position cannot be flattened because symbol or sellable quantity is missing",
                        "context": {"position": dict(position)},
                    }
                )
                continue
            instance = self.create_algo_instance(
                parent_intent_id=command_id,
                strategy_slot_id=position_slot_id,
                symbol=symbol,
                side=OrderSide.SELL,
                target_quantity=quantity,
                algo_code="OPERATOR_FLATTEN",
                metadata={
                    "operator_command_id": command_id,
                    "operator_command_type": command_type,
                    "source": "operator_command",
                    "broker_position": dict(position),
                },
            )
            price = _position_price(position)
            child = self.submit_child_order(
                algo_instance_id=instance.algo_instance_id,
                quantity=quantity,
                price=price,
                metadata={
                    "source": "operator_command",
                    "operator_command_id": command_id,
                    "operator_command_type": command_type,
                    "broker_position": dict(position),
                },
            )
            submitted_child_order_ids.append(child.child_order_id)
            broker_packets.append(
                {
                    "action": "submit_flatten_sell",
                    "child_order_id": child.child_order_id,
                    "broker_order_id": child.broker_order_id,
                    "symbol": symbol,
                    "quantity": quantity,
                    "price": price,
                    "status": child.status.value,
                }
            )
            if child.status == MiniQMTChildOrderStatus.REJECTED:
                errors.append(
                    {
                        "error_code": "MINIQMT_OPERATOR_FLATTEN_SELL_REJECTED",
                        "message": "flatten sell order was rejected by gateway",
                        "context": {"child_order_id": child.child_order_id, "symbol": symbol},
                    }
                )
        status = MiniQMTOperatorCommandStatus.EXECUTED if not errors else MiniQMTOperatorCommandStatus.REJECTED
        return self._operator_result(
            command_id=command_id,
            command_type=command_type,
            status=status,
            reason=reason,
            payload=payload,
            cancelled_child_order_ids=list(cancel_result.cancelled_child_order_ids),
            submitted_child_order_ids=submitted_child_order_ids,
            affected_algo_instance_ids=_unique(
                item.algo_instance_id for item in self.repository.list_algo_instances(runtime.runtime_id, active_only=False)
            ),
            broker_packets=broker_packets,
            errors=errors,
            metadata={"position_count": len(positions), "strategy_slot_id": strategy_slot_id},
        )

    def _execute_reset_strategy_slot(
        self,
        *,
        command_id: str,
        command_type: str,
        reason: str,
        payload: dict[str, Any],
    ) -> MiniQMTOperatorCommandResult:
        runtime = self._require_runtime()
        strategy_slot_id = _optional_text(payload.get("strategy_slot_id"))
        if not strategy_slot_id:
            return self._operator_result(
                command_id=command_id,
                command_type=command_type,
                status=MiniQMTOperatorCommandStatus.REJECTED,
                reason=reason,
                payload=payload,
                errors=[
                    {
                        "error_code": "MINIQMT_OPERATOR_STRATEGY_SLOT_REQUIRED",
                        "message": "RESET_STRATEGY_SLOT requires strategy_slot_id",
                        "context": {"command_id": command_id},
                    }
                ],
            )
        affected_instances = [
            instance.algo_instance_id
            for instance in self.repository.list_algo_instances(runtime.runtime_id, active_only=True)
            if instance.strategy_slot_id == strategy_slot_id
        ]
        cancel_result = self._execute_cancel_all_open_orders(
            command_id=f"{command_id}_cancel",
            command_type="CANCEL_ALL_OPEN_ORDERS",
            reason=f"{reason}; reset pre-cancel",
            payload={"strategy_slot_id": strategy_slot_id},
        )
        affected_instances.extend(cancel_result.affected_algo_instance_ids)
        for instance in self.repository.list_algo_instances(runtime.runtime_id, active_only=True):
            if instance.strategy_slot_id != strategy_slot_id:
                continue
            affected_instances.append(instance.algo_instance_id)
            self.oms.record_algo_instance(
                instance.model_copy(
                    update={
                        "status": MiniQMTAlgoInstanceStatus.CANCELLED,
                        "metadata": {
                            **dict(instance.metadata),
                            "operator_command_id": command_id,
                            "operator_reset_reason": reason,
                        },
                    }
                )
            )
        status = MiniQMTOperatorCommandStatus.EXECUTED if not cancel_result.errors else MiniQMTOperatorCommandStatus.REJECTED
        return self._operator_result(
            command_id=command_id,
            command_type=command_type,
            status=status,
            reason=reason,
            payload=payload,
            cancelled_child_order_ids=list(cancel_result.cancelled_child_order_ids),
            affected_algo_instance_ids=_unique(affected_instances),
            broker_packets=list(cancel_result.broker_packets),
            errors=list(cancel_result.errors),
            metadata={"strategy_slot_id": strategy_slot_id, "settlement_snapshot_required": True},
        )

    def _execute_replace_alpha_signal_book(
        self,
        *,
        command_id: str,
        command_type: str,
        reason: str,
        payload: dict[str, Any],
    ) -> MiniQMTOperatorCommandResult:
        strategy_slot_id = _optional_text(payload.get("strategy_slot_id"))
        alpha_signal_book_id = _optional_text(payload.get("alpha_signal_book_id"))
        errors = []
        if not strategy_slot_id:
            errors.append(
                {
                    "error_code": "MINIQMT_OPERATOR_STRATEGY_SLOT_REQUIRED",
                    "message": "REPLACE_ALPHA_SIGNAL_BOOK requires strategy_slot_id",
                    "context": {"command_id": command_id},
                }
            )
        if not alpha_signal_book_id:
            errors.append(
                {
                    "error_code": "MINIQMT_OPERATOR_ALPHA_SIGNAL_BOOK_REQUIRED",
                    "message": "REPLACE_ALPHA_SIGNAL_BOOK requires alpha_signal_book_id",
                    "context": {"command_id": command_id},
                }
            )
        status = MiniQMTOperatorCommandStatus.REJECTED if errors else MiniQMTOperatorCommandStatus.EXECUTED
        return self._operator_result(
            command_id=command_id,
            command_type=command_type,
            status=status,
            reason=reason,
            payload=payload,
            errors=errors,
            metadata={
                "strategy_slot_id": strategy_slot_id,
                "alpha_signal_book_id": alpha_signal_book_id,
                "execution_layer_mutated": False,
            },
        )

    def _operator_result(
        self,
        *,
        command_id: str,
        command_type: str,
        status: MiniQMTOperatorCommandStatus,
        reason: str,
        payload: dict[str, Any],
        cancelled_child_order_ids: list[str] | None = None,
        submitted_child_order_ids: list[str] | None = None,
        affected_algo_instance_ids: list[str] | None = None,
        broker_packets: list[dict[str, Any]] | None = None,
        errors: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTOperatorCommandResult:
        runtime = self._require_runtime()
        result = MiniQMTOperatorCommandResult(
            command_id=command_id,
            command_type=command_type,
            runtime_id=runtime.runtime_id,
            status=status,
            reason=reason,
            strategy_slot_id=_optional_text(payload.get("strategy_slot_id")),
            alpha_signal_book_id=_optional_text(payload.get("alpha_signal_book_id")),
            cancelled_child_order_ids=list(cancelled_child_order_ids or []),
            submitted_child_order_ids=list(submitted_child_order_ids or []),
            affected_algo_instance_ids=list(affected_algo_instance_ids or []),
            broker_packets=list(broker_packets or []),
            errors=list(errors or []),
            metadata=dict(metadata or {}),
        )
        self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.READY
                    if status == MiniQMTOperatorCommandStatus.EXECUTED
                    else MiniQMTExecutionRuntimeState.FAILED,
                    "oms_state": MiniQMTOmsState.RECONCILED
                    if not self.repository.list_child_orders(runtime.runtime_id, active_only=True)
                    else runtime.oms_state,
                    "metadata": {
                        **dict(runtime.metadata),
                        "last_operator_command": result.model_dump(mode="json"),
                    },
                }
            )
        )
        self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=(
                MiniQMTExecutionEventType.OPERATOR_COMMAND_EXECUTED
                if status == MiniQMTOperatorCommandStatus.EXECUTED
                else MiniQMTExecutionEventType.OPERATOR_COMMAND_REJECTED
            ),
            source="operator",
            payload=result.model_dump(mode="json"),
        )
        return result

    def _require_runtime(self) -> MiniQMTExecutionRuntimeRecord:
        runtime = self.repository.get_runtime(self.config.runtime_id)
        if runtime is None:
            raise RuntimeError("MiniQMTExecutionRuntime must be started before use")
        return runtime

    def _require_algo_instance(self, runtime_id: str, algo_instance_id: str) -> MiniQMTExecutionAlgoInstance:
        for instance in self.repository.list_algo_instances(runtime_id, active_only=True):
            if instance.algo_instance_id == algo_instance_id:
                return instance
        raise RuntimeError(f"active algo instance not found: {algo_instance_id}")

    def _find_algo_instance(self, runtime_id: str, algo_instance_id: str) -> MiniQMTExecutionAlgoInstance | None:
        for active_only in (False, True):
            for instance in self.repository.list_algo_instances(runtime_id, active_only=active_only):
                if instance.algo_instance_id == algo_instance_id:
                    return instance
        return None

    def _find_child_order(
        self,
        runtime_id: str,
        *,
        broker_order_id: str | None = None,
        child_order_id: str | None = None,
    ) -> MiniQMTChildOrder | None:
        for active_only in (False, True):
            for child in self.repository.list_child_orders(runtime_id, active_only=active_only):
                if broker_order_id and child.broker_order_id == broker_order_id:
                    return child
                if child_order_id and child.child_order_id == child_order_id:
                    return child
        return None

    def _is_vnpy_instance(self, instance: MiniQMTExecutionAlgoInstance) -> bool:
        return (
            str(instance.metadata.get("runtime_algo_family") or "") == "vnpy_style"
            and is_vnpy_style_algo(instance.algo_code)
        )

    def _ensure_vnpy_core(self, instance: MiniQMTExecutionAlgoInstance) -> VnpyAlgoTemplate:
        core = self._vnpy_cores.get(instance.algo_instance_id)
        if core is not None:
            return core
        metadata = dict(instance.metadata or {})
        core = create_vnpy_style_core(
            algo_code=instance.algo_code,
            symbol=instance.symbol,
            side=instance.side.value,
            price=float(metadata["limit_price"]),
            volume=int(instance.target_quantity),
            algo_config=dict(metadata.get("algo_config") or {}),
            algo_name=f"{instance.algo_code}_{instance.algo_instance_id}",
            min_volume=int(metadata.get("min_volume") or 100),
            volume_increment=int(metadata.get("volume_increment") or 100),
            random_volume_provider=self._vnpy_random_volume_providers.get(instance.algo_instance_id),
        )
        self._restore_vnpy_core_from_metadata(core, metadata)
        self._vnpy_cores[instance.algo_instance_id] = core
        return core

    def _restore_vnpy_core_from_metadata(self, core: VnpyAlgoTemplate, metadata: dict[str, Any]) -> None:
        algo_state = metadata.get("vnpy_algo_state")
        if not isinstance(algo_state, dict):
            return
        snapshot = algo_state.get("snapshot")
        if not isinstance(snapshot, dict):
            return
        status = str(snapshot.get("status") or "")
        if status:
            core.status = VnpyAlgoStatus(status)
        core.traded = int(snapshot.get("traded") or 0)
        core.traded_price = float(snapshot.get("traded_price") or 0.0)
        variables = snapshot.get("variables")
        if isinstance(variables, dict):
            for name, value in variables.items():
                if hasattr(core, name):
                    setattr(core, name, value)
        for vt_orderid in snapshot.get("active_order_ids") or []:
            core.active_orders[str(vt_orderid)] = VnpyOrderUpdate(vt_orderid=str(vt_orderid), active=True)

    def _persist_vnpy_core_state(
        self,
        instance: MiniQMTExecutionAlgoInstance,
        core: VnpyAlgoTemplate,
    ) -> MiniQMTExecutionAlgoInstance:
        snapshot = core.get_data()
        status = (
            MiniQMTAlgoInstanceStatus.COMPLETED
            if snapshot.status == "finished"
            else instance.status
        )
        updated = instance.model_copy(
            update={
                "remaining_quantity": max(0, int(snapshot.left)),
                "status": status,
                "metadata": {
                    **dict(instance.metadata),
                    "vnpy_algo_state": core.audit_metadata(),
                },
            }
        )
        return self.oms.record_algo_instance(updated)

    def _dispatch_tick_to_vnpy_algos(self, runtime_id: str, *, tick_payload: dict[str, Any]) -> None:
        for instance in self.repository.list_algo_instances(runtime_id, active_only=True):
            if instance.symbol != tick_payload.get("symbol") or not self._is_vnpy_instance(instance):
                continue
            tick = self._vnpy_tick_from_payload(tick_payload)
            core = self._ensure_vnpy_core(instance)
            actions = core.update_tick(tick)
            self._persist_vnpy_core_state(instance, core)
            self._handle_vnpy_actions(instance, actions)

    def _dispatch_timer_to_vnpy_algos(self, runtime_id: str) -> None:
        for instance in self.repository.list_algo_instances(runtime_id, active_only=True):
            if not self._is_vnpy_instance(instance):
                continue
            core = self._ensure_vnpy_core(instance)
            actions = core.update_timer()
            self._persist_vnpy_core_state(instance, core)
            self._handle_vnpy_actions(instance, actions)

    def _handle_vnpy_actions(self, instance: MiniQMTExecutionAlgoInstance, actions: list[VnpyAction]) -> None:
        if not actions:
            return
        cancelled_child_ids: set[str] = set()
        for action in actions:
            if action.action_type == VnpyActionType.SUBMIT:
                child_metadata = {
                    **dict(instance.metadata.get("runtime_child_context") or {}),
                    "source": "runtime_owned_vnpy_algo",
                    "vnpy_action_id": action.action_id,
                    "vnpy_vt_orderid": action.vt_orderid,
                    "vnpy_action_type": action.action_type.value,
                    "vnpy_reason": action.reason,
                    "source_attribution": instance.metadata.get("source_attribution"),
                    "execution_algo_code": instance.algo_code,
                }
                child = self.submit_child_order(
                    algo_instance_id=instance.algo_instance_id,
                    quantity=int(action.volume or 0),
                    price=float(action.price or 0),
                    metadata=child_metadata,
                )
                core = self._ensure_vnpy_core(instance)
                follow_up_actions = core.update_order(
                    VnpyOrderUpdate(
                        vt_orderid=action.vt_orderid or child.child_order_id,
                        active=child.status == MiniQMTChildOrderStatus.SUBMITTED,
                        traded=0,
                        price=child.price,
                        raw_status=child.status.value,
                        status_msg=str(child.metadata.get("gateway_message") or ""),
                        raw={"child_order_id": child.child_order_id, "broker_order_id": child.broker_order_id},
                    )
                )
                self._persist_vnpy_core_state(instance, core)
                self._handle_vnpy_actions(instance, follow_up_actions)
            elif action.action_type in {VnpyActionType.CANCEL, VnpyActionType.CANCEL_ALL}:
                cancel_acks = []
                for child in self._find_vnpy_active_children(instance, vt_orderid=action.vt_orderid):
                    if child.child_order_id in cancelled_child_ids:
                        continue
                    ack = self.gateway.cancel_child_order(child, reason=action.reason or "vnpy_cancel")
                    cancelled_child_ids.add(child.child_order_id)
                    cancel_acks.append(
                        {
                            "child_order_id": child.child_order_id,
                            "broker_order_id": child.broker_order_id,
                            "accepted": ack.accepted,
                            "message": ack.message,
                            "raw": dict(ack.raw),
                        }
                    )
                self.events.append(
                    runtime_id=instance.runtime_id,
                    event_type=MiniQMTExecutionEventType.CHILD_ORDER_CANCEL_REQUESTED,
                    source="algo",
                    payload={
                        "algo_instance_id": instance.algo_instance_id,
                        "vt_orderid": action.vt_orderid,
                        "action_type": action.action_type.value,
                        "reason": action.reason,
                        "metadata": dict(action.metadata),
                        "cancel_acks": cancel_acks,
                    },
                )
            elif action.action_type == VnpyActionType.FINISH:
                latest = self._find_algo_instance(instance.runtime_id, instance.algo_instance_id) or instance
                self.oms.record_algo_instance(latest.model_copy(update={"status": MiniQMTAlgoInstanceStatus.COMPLETED}))
                self.events.append(
                    runtime_id=instance.runtime_id,
                    event_type=MiniQMTExecutionEventType.ALGO_ACTION_EMITTED,
                    source="algo",
                    payload={
                        "algo_instance_id": instance.algo_instance_id,
                        "action_type": action.action_type.value,
                        "reason": action.reason,
                    },
                )
            else:
                self.events.append(
                    runtime_id=instance.runtime_id,
                    event_type=MiniQMTExecutionEventType.ALGO_ACTION_EMITTED,
                    source="algo",
                    payload={
                        "algo_instance_id": instance.algo_instance_id,
                        "action_type": action.action_type.value,
                        "reason": action.reason,
                    },
                )

    def _vnpy_tick_from_payload(self, payload: dict[str, Any]) -> VnpyTick:
        required_quote_fields = ("bid_price_1", "bid_volume_1", "ask_price_1", "ask_volume_1")
        missing_fields = [
            field
            for field in required_quote_fields
            if field not in payload or payload.get(field) is None or payload.get(field) == ""
        ]
        if missing_fields:
            raise RuntimeError(
                "runtime-owned vn.py MiniQMT algo requires broker best-quote fields; "
                f"missing={missing_fields}, symbol={payload.get('symbol')}"
            )
        bid = _required_positive_float(payload.get("bid_price_1"), "bid_price_1")
        ask = _required_positive_float(payload.get("ask_price_1"), "ask_price_1")
        bid_volume = _required_positive_int(payload.get("bid_volume_1"), "bid_volume_1")
        ask_volume = _required_positive_int(payload.get("ask_volume_1"), "ask_volume_1")
        timestamp = payload.get("datetime")
        if not isinstance(timestamp, datetime):
            timestamp = datetime.now(UTC)
        return VnpyTick(
            symbol=str(payload["symbol"]),
            datetime=timestamp,
            bid_price_1=bid,
            bid_volume_1=bid_volume,
            ask_price_1=ask,
            ask_volume_1=ask_volume,
            raw=dict(payload),
        )

    def _find_vnpy_active_children(
        self,
        instance: MiniQMTExecutionAlgoInstance,
        *,
        vt_orderid: str | None,
    ) -> list[MiniQMTChildOrder]:
        children = []
        for child in self.repository.list_child_orders(instance.runtime_id, active_only=True):
            if child.algo_instance_id != instance.algo_instance_id:
                continue
            if vt_orderid and child.metadata.get("vnpy_vt_orderid") != vt_orderid:
                continue
            children.append(child)
        return children


_TERMINAL_CHILD_ORDER_STATUSES = frozenset(
    {
        MiniQMTChildOrderStatus.FILLED,
        MiniQMTChildOrderStatus.CANCELLED,
        MiniQMTChildOrderStatus.REJECTED,
    }
)


def _algo_terminal_status_from_child_orders(children: list[MiniQMTChildOrder]) -> MiniQMTAlgoInstanceStatus:
    statuses = {child.status for child in children}
    if statuses == {MiniQMTChildOrderStatus.FILLED}:
        return MiniQMTAlgoInstanceStatus.COMPLETED
    if MiniQMTChildOrderStatus.REJECTED in statuses:
        return MiniQMTAlgoInstanceStatus.FAILED
    return MiniQMTAlgoInstanceStatus.CANCELLED


def _child_status_from_broker_status_strict(status: str, *, broker_order_id: str | None) -> MiniQMTChildOrderStatus:
    normalized = str(status or "").strip().upper()
    if normalized in {"FILLED", "ALL_TRADED"}:
        return MiniQMTChildOrderStatus.FILLED
    if normalized in {"PARTIALLY_FILLED", "PARTIAL_FILLED", "PART_TRADED"}:
        return MiniQMTChildOrderStatus.PARTIALLY_FILLED
    if normalized in {"OPEN", "SUBMITTED", "PENDING", "CANCEL_REQUESTED", "ACTIVE", "ACCEPTED"}:
        return MiniQMTChildOrderStatus.SUBMITTED
    if normalized in {"CANCELLED", "CANCELED"}:
        return MiniQMTChildOrderStatus.CANCELLED
    if normalized in {"REJECTED", "BROKER_REJECTED"}:
        return MiniQMTChildOrderStatus.REJECTED
    raise RuntimeError(
        "MiniQMT broker snapshot contains unknown child order text status; "
        f"reason_code=MINIQMT_RUNTIME_UNKNOWN_BROKER_ORDER_STATUS, broker_order_id={broker_order_id}, "
        f"raw_status={status!r}"
    )


def _child_status_from_broker_order_snapshot(
    order: dict[str, Any],
    *,
    child_quantity: int,
    broker_trades: list[dict[str, Any]],
) -> MiniQMTChildOrderStatus | None:
    raw_status = order.get("order_status")
    filled_quantity = _broker_filled_quantity(order, broker_trades=broker_trades)
    if is_terminal_order_status(raw_status):
        return _child_status_from_broker_terminal_status(raw_status)
    if is_partial_order_status(raw_status) or filled_quantity > 0:
        if filled_quantity >= max(int(child_quantity or 0), 1):
            return MiniQMTChildOrderStatus.FILLED
        return MiniQMTChildOrderStatus.PARTIALLY_FILLED
    if is_open_like_order_status(raw_status):
        return MiniQMTChildOrderStatus.SUBMITTED
    text_status = str(order.get("status") or order.get("raw_status") or "").strip().upper()
    if text_status:
        return _child_status_from_broker_status_strict(text_status, broker_order_id=_broker_order_id(order))
    raise RuntimeError(
        "MiniQMT broker snapshot is missing usable child order status; "
        f"reason_code=MINIQMT_RUNTIME_MISSING_BROKER_ORDER_STATUS, broker_order_id={_broker_order_id(order)}, "
        f"raw_order_status={raw_status!r}"
    )


def _child_status_from_broker_terminal_status(raw_status: Any) -> MiniQMTChildOrderStatus:
    try:
        status = int(raw_status)
    except (TypeError, ValueError):
        return MiniQMTChildOrderStatus.SUBMITTED
    if status == STATUS_CANCELLED:
        return MiniQMTChildOrderStatus.CANCELLED
    if status == STATUS_FILLED:
        return MiniQMTChildOrderStatus.FILLED
    if status == STATUS_REJECTED:
        return MiniQMTChildOrderStatus.REJECTED
    return MiniQMTChildOrderStatus.SUBMITTED


def _broker_filled_quantity(order: dict[str, Any], *, broker_trades: list[dict[str, Any]]) -> int:
    for key in ("traded_volume", "filled_quantity", "filled_volume", "cumulative_quantity", "traded_quantity"):
        if order.get(key) is None:
            continue
        try:
            return max(int(order[key]), 0)
        except (TypeError, ValueError):
            continue
    total = 0
    for trade in broker_trades:
        for key in ("traded_volume", "quantity", "volume", "filled_quantity"):
            if trade.get(key) is None:
                continue
            try:
                total += max(int(trade[key]), 0)
                break
            except (TypeError, ValueError):
                continue
    return total


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _unique(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _position_symbol(position: dict[str, Any]) -> str | None:
    for key in ("symbol", "stock_code", "instrument", "code"):
        value = _optional_text(position.get(key))
        if value:
            return value
    return None


def _position_strategy_slot_id(position: dict[str, Any]) -> str | None:
    for key in ("strategy_slot_id", "slot_id"):
        value = _optional_text(position.get(key))
        if value:
            return value
    metadata = position.get("metadata")
    if isinstance(metadata, dict):
        return _optional_text(metadata.get("strategy_slot_id"))
    return None


def _open_sell_positions_from_broker_orders(
    broker_orders: list[dict[str, Any]],
    *,
    known_symbols: set[str | None],
) -> list[dict[str, Any]]:
    """Fallback for brokers/tests that expose open orders but no positions."""

    positions: list[dict[str, Any]] = []
    seen_symbols = {symbol for symbol in known_symbols if symbol}
    for order in broker_orders:
        symbol = _position_symbol(order)
        if not symbol or symbol in seen_symbols:
            continue
        if _order_side(order) != "SELL" or not _is_open_broker_order(order):
            continue
        quantity = _open_order_remaining_quantity(order)
        if quantity <= 0:
            continue
        seen_symbols.add(symbol)
        positions.append(
            {
                "symbol": symbol,
                "quantity": quantity,
                "available_quantity": quantity,
                "price": _position_price(order),
                "strategy_slot_id": _position_strategy_slot_id(order) or _optional_text(order.get("strategy_name")),
                "source": "open_sell_order_fallback",
                "broker_order": dict(order),
            }
        )
    return positions


def _order_side(order: dict[str, Any]) -> str | None:
    raw = str(order.get("side") or "").strip().upper()
    if raw in {"BUY", "SELL"}:
        return raw
    try:
        order_type = int(order.get("order_type"))
    except (TypeError, ValueError):
        return None
    if order_type == 23:
        return "BUY"
    if order_type == 24:
        return "SELL"
    return None


def _is_open_broker_order(order: dict[str, Any]) -> bool:
    diagnostic = order.get("diagnostic")
    if isinstance(diagnostic, dict) and diagnostic.get("cancelable_stale_warning") is True:
        return False
    order_status = order.get("order_status")
    if is_terminal_order_status(order_status):
        return False
    if is_open_like_order_status(order_status):
        return True
    raw_status = str(order.get("status") or order.get("raw_status") or "").strip().upper()
    if raw_status in {"OPEN", "SUBMITTED", "PARTIALLY_FILLED", "PENDING", "CANCEL_REQUESTED", "ACTIVE", "ACCEPTED"}:
        return True
    return False


def _open_order_remaining_quantity(order: dict[str, Any]) -> int:
    for key in ("remaining_volume", "remaining_quantity"):
        if order.get(key) is None:
            continue
        try:
            return max(int(order[key]), 0)
        except (TypeError, ValueError):
            continue
    try:
        order_volume = int(order.get("order_volume") or order.get("quantity") or 0)
        traded_volume = int(order.get("traded_volume") or 0)
    except (TypeError, ValueError):
        return 0
    return max(order_volume - traded_volume, 0)


def _position_quantity(position: dict[str, Any]) -> int:
    for key in ("quantity", "volume", "current_amount", "position_quantity"):
        if position.get(key) is None:
            continue
        try:
            return max(int(position[key]), 0)
        except (TypeError, ValueError):
            continue
    return 0


def _position_sellable_quantity(position: dict[str, Any]) -> int:
    for key in ("available_quantity", "available", "can_sell", "can_sell_quantity", "sellable_quantity"):
        if position.get(key) is None:
            continue
        try:
            return max(int(position[key]), 0)
        except (TypeError, ValueError):
            continue
    return _position_quantity(position)


def _position_price(position: dict[str, Any]) -> float:
    for key in ("price", "last_price", "current_price", "market_price", "avg_price", "avg_cost", "cost_price", "open_price"):
        if position.get(key) in (None, ""):
            continue
        try:
            return max(float(position[key]), 0.0)
        except (TypeError, ValueError):
            continue
    return 0.0


def _broker_order_id(order: dict[str, Any]) -> str | None:
    for key in ("broker_order_id", "order_id", "qmt_order_id", "native_order_id"):
        value = _optional_text(order.get(key))
        if value:
            return value
    return None


def _order_price_type(order: dict[str, Any]) -> int:
    try:
        return int(order.get("price_type") or 11)
    except (TypeError, ValueError):
        return 11


def _required_positive_float(value: Any, name: str) -> float:
    parsed = _optional_float(value)
    if parsed is None or parsed <= 0 or not math.isfinite(parsed):
        raise RuntimeError(f"runtime-owned vn.py MiniQMT tick field {name} must be positive")
    return parsed


def _required_positive_int(value: Any, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"runtime-owned vn.py MiniQMT tick field {name} must be an integer") from exc
    if parsed <= 0:
        raise RuntimeError(f"runtime-owned vn.py MiniQMT tick field {name} must be positive")
    return parsed
