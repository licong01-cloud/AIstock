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
    ) -> None:
        self.config = config
        self.repository = repository
        self.gateway = gateway
        self.events = MiniQMTExecutionEventLoop(repository=repository)
        self.oms = MiniQMTOmsLedger(repository)
        self._vnpy_cores: dict[str, VnpyAlgoTemplate] = {}
        self._vnpy_random_volume_providers: dict[str, Callable[[int, int], float]] = {}

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
        child = self._find_child_order(runtime.runtime_id, broker_order_id=broker_order_id)
        if child is not None:
            child = self.oms.record_child_order(
                child.model_copy(update={"status": _child_status_from_broker_status(status)})
            )
            instance = self._find_algo_instance(runtime.runtime_id, child.algo_instance_id)
            if instance is not None and self._is_vnpy_instance(instance):
                core = self._ensure_vnpy_core(instance)
                actions = core.update_order(
                    VnpyOrderUpdate(
                        vt_orderid=str(child.metadata.get("vnpy_vt_orderid") or child.child_order_id),
                        active=_broker_status_is_active(status),
                        traded=int((payload or {}).get("traded") or (payload or {}).get("filled_quantity") or 0),
                        price=_optional_float((payload or {}).get("price") or child.price),
                        raw_status=str(status),
                        status_msg=str((payload or {}).get("status_msg") or ""),
                        raw={"broker_order_id": broker_order_id, **dict(payload or {})},
                    )
                )
                self._persist_vnpy_core_state(instance, core)
                self._handle_vnpy_actions(instance, actions)
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
        child = self._find_child_order(runtime.runtime_id, broker_order_id=broker_order_id)
        if child is not None:
            cumulative_quantity = int(
                (payload or {}).get("cumulative_quantity") or (payload or {}).get("filled_quantity") or quantity
            )
            child = self.oms.record_child_order(
                child.model_copy(
                    update={
                        "status": MiniQMTChildOrderStatus.FILLED
                        if cumulative_quantity >= child.quantity
                        else MiniQMTChildOrderStatus.PARTIALLY_FILLED
                    }
                )
            )
            instance = self._find_algo_instance(runtime.runtime_id, child.algo_instance_id)
            if instance is not None and self._is_vnpy_instance(instance):
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
        return event

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


def _broker_status_is_active(status: str) -> bool:
    return str(status or "").strip().upper() in {"PENDING", "SUBMITTED", "PARTIALLY_FILLED", "ACTIVE", "ACCEPTED"}


def _child_status_from_broker_status(status: str) -> MiniQMTChildOrderStatus:
    normalized = str(status or "").strip().upper()
    if normalized in {"FILLED", "ALL_TRADED"}:
        return MiniQMTChildOrderStatus.FILLED
    if normalized in {"PARTIALLY_FILLED", "PARTIAL_FILLED", "PART_TRADED"}:
        return MiniQMTChildOrderStatus.PARTIALLY_FILLED
    if normalized in {"CANCELLED", "CANCELED"}:
        return MiniQMTChildOrderStatus.CANCELLED
    if normalized in {"REJECTED", "BROKER_REJECTED"}:
        return MiniQMTChildOrderStatus.REJECTED
    return MiniQMTChildOrderStatus.SUBMITTED


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
