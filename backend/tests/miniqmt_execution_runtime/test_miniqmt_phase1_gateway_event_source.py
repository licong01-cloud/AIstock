from __future__ import annotations

from datetime import date

import pytest

from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeState,
    MiniQMTGatewayEventSourceError,
    MiniQMTGatewayState,
    QmtClientMiniQMTEventLoopGateway,
)
from backend.services.qmt_strategy_ledger.models import STATUS_OPEN_LIKE, STATUS_PART_SUCC
from backend.services.trading_core.models import OrderSide


class _CallbackQmtClient:
    def __init__(self) -> None:
        self.orders = [{"order_id": "900001", "stock_code": "000001.SZ", "order_status": STATUS_OPEN_LIKE}]
        self.trades = [{"traded_id": "trade_1", "order_id": "900001", "traded_volume": 100, "traded_price": 10.2}]
        self.positions = [{"stock_code": "000001.SZ", "quantity": 100, "can_sell": 100}]

    def get_orders(self, cancelable_only: bool = False) -> list[dict]:  # noqa: ARG002
        return list(self.orders)

    def get_trades(self) -> list[dict]:
        return list(self.trades)

    def get_positions(self) -> list[dict]:
        return list(self.positions)

    def place_order(self, **kwargs):
        return 900001, "accepted"

    def cancel_order(self, order_id: str):
        return True, f"cancelled {order_id}"


def _runtime() -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, QmtClientMiniQMTEventLoopGateway]:
    repository = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = QmtClientMiniQMTEventLoopGateway(qmt_client=_CallbackQmtClient())
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase1_gateway_events",
            account_group_id="ag_phase1",
            trade_date=date(2026, 6, 23),
            runtime_config_hash="hash_phase1_gateway",
        ),
        repository=repository,
        gateway=gateway,
    )
    return runtime, repository, gateway


def test_event_loop_gateway_converts_real_callbacks_to_runtime_events() -> None:
    runtime, repository, gateway = _runtime()
    runtime.start()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_phase1_000001",
        strategy_slot_id="slot_phase1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.2)

    gateway.on_tick(
        {
            "stock_code": "000001.SZ",
            "last_price": 10.2,
            "bid_price_1": 10.19,
            "bid_volume_1": 1000,
            "ask_price_1": 10.2,
            "ask_volume_1": 1000,
        }
    )
    gateway.on_order({"order_id": child.broker_order_id, "order_status": STATUS_PART_SUCC, "status_msg": "partial"})
    gateway.on_trade({"order_id": child.broker_order_id, "traded_volume": 100, "traded_price": 10.2})
    gateway.on_account({"available_cash": 1000000, "market_value": 1020})

    events = repository.list_events(runtime.config.runtime_id)
    assert MiniQMTExecutionEventType.TICK in {event.event_type for event in events}
    assert MiniQMTExecutionEventType.ORDER_EVENT in {event.event_type for event in events}
    assert MiniQMTExecutionEventType.TRADE_EVENT in {event.event_type for event in events}
    assert MiniQMTExecutionEventType.ACCOUNT_EVENT in {event.event_type for event in events}
    stored_child = repository.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.child_order_id == child.child_order_id
    assert stored_child.status == MiniQMTChildOrderStatus.FILLED


def test_event_loop_gateway_disconnect_is_loud_and_marks_runtime_disconnected() -> None:
    runtime, repository, gateway = _runtime()
    runtime.start()

    event = gateway.on_disconnect({"reason": "xtquant callback disconnected"})

    assert event.event_type == MiniQMTExecutionEventType.GATEWAY_DISCONNECTED
    assert event.payload["reason_code"] == "MINIQMT_GATEWAY_DISCONNECTED"
    runtime_record = repository.get_runtime(runtime.config.runtime_id)
    assert runtime_record is not None
    assert runtime_record.gateway_state == MiniQMTGatewayState.DISCONNECTED
    assert runtime_record.event_loop_state == MiniQMTExecutionRuntimeState.PAUSED


def test_event_loop_gateway_sync_methods_require_real_broker_snapshot() -> None:
    gateway = QmtClientMiniQMTEventLoopGateway(qmt_client=object())

    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_SYNC_ORDERS_UNAVAILABLE"):
        gateway.sync_orders(runtime_id="mqrt_missing_query")

    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_SYNC_TRADES_UNAVAILABLE"):
        gateway.sync_trades(runtime_id="mqrt_missing_query")

    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_SYNC_POSITIONS_UNAVAILABLE"):
        gateway.sync_positions(runtime_id="mqrt_missing_query")


def test_event_loop_gateway_rejects_malformed_callbacks_loudly() -> None:
    runtime, _repository, gateway = _runtime()
    runtime.start()

    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_ORDER_ID_MISSING"):
        gateway.on_order({"order_status": STATUS_OPEN_LIKE})

    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_TICK_SYMBOL_MISSING"):
        gateway.on_tick({"last_price": 10.2})
