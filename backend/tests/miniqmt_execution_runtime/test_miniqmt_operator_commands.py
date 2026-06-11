from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeClient,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOperatorCommandStatus,
)
from backend.services.trading_core.models import OrderSide


def _runtime(
    *,
    gateway: FakeMiniQMTGateway | None = None,
) -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, FakeMiniQMTGateway]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    broker_gateway = gateway or FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase6_operator",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase6_operator",
        ),
        repository=repo,
        gateway=broker_gateway,
    )
    runtime.start()
    return runtime, repo, broker_gateway


def test_cancel_all_open_orders_executes_through_gateway_and_terminalizes_oms() -> None:
    runtime, repo, gateway = _runtime()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_buy_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=1000, price=10.2)

    result = runtime.execute_operator_command(
        command_id="opcmd_cancel_all_001",
        command_type="CANCEL_ALL_OPEN_ORDERS",
        reason="operator pre-open cleanup",
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.cancelled_child_order_ids == [child.child_order_id]
    assert result.broker_packets[0]["accepted"] is True
    assert gateway.cancelled_orders[0].child_order_id == child.child_order_id
    stored = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    assert stored.status == MiniQMTChildOrderStatus.CANCELLED
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=True) == []
    assert [
        event.event_type
        for event in repo.list_events(runtime.config.runtime_id)
        if event.payload.get("command_id") == "opcmd_cancel_all_001"
    ] == [
        MiniQMTExecutionEventType.OPERATOR_COMMAND_RECEIVED,
        MiniQMTExecutionEventType.CHILD_ORDER_CANCEL_REQUESTED,
        MiniQMTExecutionEventType.OPERATOR_COMMAND_EXECUTED,
    ]


def test_cancel_all_open_orders_imports_active_broker_orders_before_cancel() -> None:
    runtime, repo, gateway = _runtime(
        gateway=FakeMiniQMTGateway(
            orders=[
                {
                    "order_id": "900001",
                    "stock_code": "000001.SZ",
                    "order_type": 23,
                    "order_volume": 1000,
                    "traded_volume": 0,
                    "order_status": 50,
                    "price_type": 5,
                    "price": 10.1,
                    "strategy_name": "slot_alpha_001",
                }
            ]
        )
    )

    result = runtime.execute_operator_command(
        command_id="opcmd_cancel_synced_001",
        command_type="CANCEL_ALL_OPEN_ORDERS",
        reason="operator broker cleanup",
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.cancelled_child_order_ids
    assert gateway.cancelled_orders[0].broker_order_id == "900001"
    stored = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    assert stored.status == MiniQMTChildOrderStatus.CANCELLED
    assert stored.metadata["broker_order"]["order_id"] == "900001"


def test_cancel_all_open_orders_skips_stale_cancelable_broker_orders() -> None:
    runtime, repo, gateway = _runtime(
        gateway=FakeMiniQMTGateway(
            orders=[
                {
                    "order_id": "900002",
                    "stock_code": "000001.SZ",
                    "order_type": 23,
                    "order_volume": 1000,
                    "traded_volume": 0,
                    "order_status": 50,
                    "price_type": 5,
                    "price": 10.1,
                    "strategy_name": "slot_alpha_001",
                    "diagnostic": {
                        "cancelable_stale_warning": True,
                        "cancelable_stale_reason": "historical_cancelable_order_reported_by_broker",
                    },
                }
            ]
        )
    )

    result = runtime.execute_operator_command(
        command_id="opcmd_cancel_stale_001",
        command_type="CANCEL_ALL_OPEN_ORDERS",
        reason="operator broker cleanup",
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.cancelled_child_order_ids == []
    assert gateway.cancelled_orders == []
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False) == []


def test_flatten_all_positions_pre_cancels_then_submits_sell_orders_with_slot_attribution() -> None:
    runtime, repo, gateway = _runtime(
        gateway=FakeMiniQMTGateway(
            positions=[
                {
                    "symbol": "000001.SZ",
                    "quantity": 1000,
                    "available_quantity": 700,
                    "last_price": 10.1,
                    "strategy_slot_id": "slot_alpha_001",
                },
                {
                    "symbol": "000002.SZ",
                    "quantity": 500,
                    "available_quantity": 500,
                    "last_price": 11.2,
                    "strategy_slot_id": "slot_alpha_002",
                },
            ]
        )
    )
    open_algo = runtime.create_algo_instance(
        parent_intent_id="intent_open_buy",
        strategy_slot_id="slot_alpha_001",
        symbol="000003.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    open_child = runtime.submit_child_order(algo_instance_id=open_algo.algo_instance_id, quantity=100, price=9.9)

    result = runtime.execute_operator_command(
        command_id="opcmd_flatten_all_001",
        command_type="FLATTEN_ALL_POSITIONS",
        reason="replace alpha universe",
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert open_child.child_order_id in result.cancelled_child_order_ids
    assert len(result.submitted_child_order_ids) == 2
    submitted_sells = [
        child
        for child in repo.list_child_orders(runtime.config.runtime_id, active_only=False)
        if child.child_order_id in result.submitted_child_order_ids
    ]
    assert [child.side for child in submitted_sells] == [OrderSide.SELL, OrderSide.SELL]
    assert [(child.strategy_slot_id, child.quantity) for child in submitted_sells] == [
        ("slot_alpha_001", 700),
        ("slot_alpha_002", 500),
    ]
    assert len(gateway.cancelled_orders) == 1
    assert len(gateway.submitted_orders) == 3


def test_reset_strategy_slot_cancels_only_that_slot_and_marks_algos_cancelled() -> None:
    runtime, repo, gateway = _runtime()
    slot_a_algo = runtime.create_algo_instance(
        parent_intent_id="intent_slot_a",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    slot_b_algo = runtime.create_algo_instance(
        parent_intent_id="intent_slot_b",
        strategy_slot_id="slot_alpha_002",
        symbol="000002.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    runtime.submit_child_order(algo_instance_id=slot_a_algo.algo_instance_id, quantity=100, price=10)
    runtime.submit_child_order(algo_instance_id=slot_b_algo.algo_instance_id, quantity=100, price=11)

    result = runtime.execute_operator_command(
        command_id="opcmd_reset_slot_001",
        command_type="RESET_STRATEGY_SLOT",
        reason="bad alpha source",
        payload={"strategy_slot_id": "slot_alpha_001"},
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.affected_algo_instance_ids == [slot_a_algo.algo_instance_id]
    assert len(gateway.cancelled_orders) == 1
    slot_a = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    slot_b = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[1]
    assert slot_a.status == MiniQMTAlgoInstanceStatus.CANCELLED
    assert slot_b.status == MiniQMTAlgoInstanceStatus.ACTIVE
    active_children = repo.list_child_orders(runtime.config.runtime_id, active_only=True)
    assert [child.strategy_slot_id for child in active_children] == ["slot_alpha_002"]


def test_replace_alpha_signal_book_records_binding_without_execution_layer_mutation() -> None:
    runtime, repo, _gateway = _runtime()

    result = runtime.execute_operator_command(
        command_id="opcmd_replace_alpha_001",
        command_type="REPLACE_ALPHA_SIGNAL_BOOK",
        reason="switch to morning signal book",
        payload={"strategy_slot_id": "slot_alpha_001", "alpha_signal_book_id": "asb_20260609_v2"},
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.strategy_slot_id == "slot_alpha_001"
    assert result.alpha_signal_book_id == "asb_20260609_v2"
    assert result.metadata["execution_layer_mutated"] is False
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False) == []
    runtime_record = repo.get_runtime(runtime.config.runtime_id)
    assert runtime_record is not None
    assert runtime_record.metadata["last_operator_command"]["alpha_signal_book_id"] == "asb_20260609_v2"


def test_runtime_client_executes_operator_command_with_evidence() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    client = MiniQMTExecutionRuntimeClient(repository=repo)
    gateway = FakeMiniQMTGateway(
        positions=[
            {
                "symbol": "000001.SZ",
                "quantity": 100,
                "available_quantity": 100,
                "last_price": 10.0,
                "strategy_slot_id": "slot_alpha_001",
            }
        ]
    )

    result, evidence = client.execute_operator_command(
        account_group_id="ag_minqmt_main_sim",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="runtime_hash_phase6_operator_client",
        command_id="opcmd_client_flatten_001",
        command_type="FLATTEN_ALL_POSITIONS",
        reason="operator smoke",
        gateway=gateway,
        runtime_id="mqrt_phase6_client",
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.submitted_child_order_ids
    assert evidence.runtime_owner == "MiniQMTExecutionRuntime"
    assert evidence.submitted_child_count == 1
