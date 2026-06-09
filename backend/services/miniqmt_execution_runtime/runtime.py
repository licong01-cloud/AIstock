"""MiniQMTExecutionRuntime Phase 2 skeleton."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from backend.services.trading_core.models import OrderSide

from .gateway import MiniQMTGateway
from .models import (
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
    MiniQMTRuntimeRecoverySnapshot,
)
from .oms import MiniQMTOmsLedger
from .repository import MiniQMTExecutionRuntimeRepository


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

    The Phase 2 implementation provides durable runtime/event/gateway/OMS
    skeleton behavior with a fake broker. Phase 3+ will attach vn.py-derived
    algo behavior and Phase 4 will route Paper v2/simulation_runtime clients.
    """

    def __init__(
        self,
        *,
        config: MiniQMTExecutionRuntimeConfig,
        repository: MiniQMTExecutionRuntimeRepository,
        gateway: MiniQMTGateway,
    ) -> None:
        self.config = config
        self.repository = repository
        self.gateway = gateway
        self.events = MiniQMTExecutionEventLoop(repository=repository)
        self.oms = MiniQMTOmsLedger(repository)

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
        runtime = self.repository.upsert_runtime(
            runtime.model_copy(
                update={
                    "event_loop_state": MiniQMTExecutionRuntimeState.READY,
                    "gateway_state": MiniQMTGatewayState.CONNECTED,
                    "oms_state": MiniQMTOmsState.RECONCILED,
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

    def on_timer(self, *, timer_name: str, payload: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        return self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.TIMER,
            source="runtime",
            payload={"timer_name": timer_name, **dict(payload or {})},
        )

    def on_tick(self, *, symbol: str, price: float, payload: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        return self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.TICK,
            source="gateway",
            payload={"symbol": symbol, "price": price, **dict(payload or {})},
        )

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

    def record_order_event(
        self,
        *,
        broker_order_id: str,
        status: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        return self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.ORDER_EVENT,
            source="gateway",
            payload={"broker_order_id": broker_order_id, "status": status, **dict(payload or {})},
        )

    def record_trade_event(
        self,
        *,
        broker_order_id: str,
        quantity: int,
        price: float,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        runtime = self._require_runtime()
        return self.events.append(
            runtime_id=runtime.runtime_id,
            event_type=MiniQMTExecutionEventType.TRADE_EVENT,
            source="gateway",
            payload={"broker_order_id": broker_order_id, "quantity": quantity, "price": price, **dict(payload or {})},
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
