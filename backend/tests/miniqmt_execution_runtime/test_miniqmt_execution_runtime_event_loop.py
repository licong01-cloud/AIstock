from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeState,
    MiniQMTGatewayState,
    MiniQMTOmsState,
)
from backend.services.trading_core.models import OrderSide


def _runtime() -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, FakeMiniQMTGateway]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase2_event_loop",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase2",
        ),
        repository=repo,
        gateway=gateway,
    )
    return runtime, repo, gateway


def test_runtime_event_loop_persists_tick_timer_algo_order_trade_and_reconcile_events() -> None:
    runtime, repo, gateway = _runtime()

    record = runtime.start()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_buy_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
    )
    runtime.on_timer(timer_name="open_call_auction")
    runtime.on_tick(symbol="000001.SZ", price=10.2)
    runtime.record_operator_command(
        command_id="opcmd_audit_only_001",
        command_type="AUDIT_RUNTIME_STATE",
        reason="phase2_operator_event_persistence",
    )
    child_order = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=1000, price=10.2)
    runtime.record_order_event(broker_order_id=child_order.broker_order_id or "", status="SUBMITTED")
    runtime.record_trade_event(broker_order_id=child_order.broker_order_id or "", quantity=300, price=10.2)
    snapshot = runtime.reconcile()

    events = repo.list_events(record.runtime_id)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert {
        MiniQMTExecutionEventType.RUNTIME_CREATED,
        MiniQMTExecutionEventType.GATEWAY_CONNECTED,
        MiniQMTExecutionEventType.ALGO_INSTANCE_CREATED,
        MiniQMTExecutionEventType.TIMER,
        MiniQMTExecutionEventType.TICK,
        MiniQMTExecutionEventType.OPERATOR_COMMAND_RECEIVED,
        MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
        MiniQMTExecutionEventType.ORDER_EVENT,
        MiniQMTExecutionEventType.TRADE_EVENT,
        MiniQMTExecutionEventType.BROKER_SYNC_STARTED,
        MiniQMTExecutionEventType.BROKER_SYNCED,
        MiniQMTExecutionEventType.RECONCILE_STARTED,
        MiniQMTExecutionEventType.RECONCILE_COMPLETED,
    }.issubset({event.event_type for event in events})
    assert child_order.status == MiniQMTChildOrderStatus.SUBMITTED
    assert child_order.broker_order_id == "fake_qmt_000001"
    assert gateway.submitted_orders
    assert snapshot.active_algo_instances[0].algo_instance_id == algo.algo_instance_id
    assert snapshot.active_child_orders[0].child_order_id == child_order.child_order_id

    updated = repo.get_runtime(record.runtime_id)
    assert updated is not None
    assert updated.gateway_state == MiniQMTGatewayState.CONNECTED
    assert updated.oms_state == MiniQMTOmsState.RECONCILED
    assert updated.event_loop_state == MiniQMTExecutionRuntimeState.READY
