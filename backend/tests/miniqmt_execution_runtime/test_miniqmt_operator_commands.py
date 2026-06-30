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


def test_operator_cancel_terminalizes_runtime_owned_vnpy_instance() -> None:
    runtime, repo, gateway = _runtime()
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_vnpy_cancel_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.0,
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=9.99,
        payload={"bid_price_1": 9.98, "bid_volume_1": 1000, "ask_price_1": 9.99, "ask_volume_1": 1000},
    )
    child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]

    result = runtime.execute_operator_command(
        command_id="opcmd_cancel_vnpy_001",
        command_type="CANCEL_ALL_OPEN_ORDERS",
        reason="operator stop runtime-owned algo",
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.cancelled_child_order_ids == [child.child_order_id]
    assert result.affected_algo_instance_ids == [algo.algo_instance_id]
    assert gateway.cancelled_orders[0].child_order_id == child.child_order_id
    stored_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.status == MiniQMTChildOrderStatus.CANCELLED
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.CANCELLED
    assert stored_algo.metadata["operator_command_id"] == "opcmd_cancel_vnpy_001"
    assert stored_algo.metadata["terminal_vnpy_active_order_ids"]
    assert stored_algo.metadata["terminal_vnpy_active_orders_ignored"] is True


def test_normal_terminalization_guard_keeps_vnpy_active_when_core_still_has_active_order() -> None:
    runtime, repo, _gateway = _runtime()
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_vnpy_guard_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.0,
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=9.99,
        payload={"bid_price_1": 9.98, "bid_volume_1": 1000, "ask_price_1": 9.99, "ask_volume_1": 1000},
    )
    child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    runtime.record_order_event(
        broker_order_id=child.broker_order_id or child.child_order_id,
        status="50",
        payload={"order_status": 50, "status_msg": "still active"},
    )
    algo_after_tick = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert algo_after_tick.metadata["vnpy_algo_state"]["snapshot"]["active_order_ids"]

    repo.upsert_child_order(child.model_copy(update={"status": MiniQMTChildOrderStatus.CANCELLED}))
    updated = runtime._terminalize_algo_if_all_children_terminal(  # noqa: SLF001
        runtime.config.runtime_id,
        algo.algo_instance_id,
        reason="unit_normal_guard",
    )

    assert updated is None
    guarded_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert guarded_algo.status == MiniQMTAlgoInstanceStatus.ACTIVE


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


def test_stale_runtime_recovery_terminalizes_only_when_broker_empty_without_cancel() -> None:
    runtime, repo, gateway = _runtime()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_stale_runtime",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    gateway._orders.clear()

    result = runtime.execute_operator_command(
        command_id="opcmd_recover_empty_broker_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="broker empty stale runtime recovery",
        payload={"run_id": "simrun_stale_runtime"},
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert result.metadata["broker_evidence"]["broker_open_order_count"] == 0
    assert result.metadata["runtime_only_cleanup"]["terminalized_child_order_ids"] == [child.child_order_id]
    assert result.metadata["runtime_only_cleanup_mutated"] is True
    assert gateway.cancelled_orders == []
    stored_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.status == MiniQMTChildOrderStatus.REJECTED
    assert stored_child.metadata["broker_cancel_called"] is False
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.FAILED
    assert stored_algo.metadata["operator_command_id"] == "opcmd_recover_empty_broker_001"
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=True) == []


def test_stale_runtime_recovery_executes_when_status_50_order_has_production_stale_evidence() -> None:
    runtime, repo, gateway = _runtime()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_stale_runtime_order_time",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    gateway._orders[:] = [
        {
            "order_id": "900004",
            "stock_code": "000001.SZ",
            "order_type": 23,
            "order_volume": 100,
            "traded_volume": 0,
            "order_status": 50,
            "order_time_iso": "2026-06-08T10:00:00+08:00",
        }
    ]

    result = runtime.execute_operator_command(
        command_id="opcmd_recover_stale_order_time_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="broker stale order-time evidence recovery",
        payload={"run_id": "simrun_stale_runtime"},
    )

    assert result.status == MiniQMTOperatorCommandStatus.EXECUTED
    broker_evidence = result.metadata["broker_evidence"]
    assert broker_evidence["broker_order_count"] == 1
    assert broker_evidence["broker_open_order_count"] == 0
    assert broker_evidence["excluded_stale_order_count"] == 1
    assert broker_evidence["excluded_stale_order_ids"] == ["900004"]
    assert broker_evidence["excluded_stale_orders"][0]["reason"] == "historical_open_like_order_reported_by_broker_snapshot"
    assert result.broker_packets[0]["excluded_stale_order_ids"] == ["900004"]
    assert gateway.cancelled_orders == []
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0].child_order_id == child.child_order_id
    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0].status == MiniQMTAlgoInstanceStatus.FAILED


def test_stale_runtime_recovery_rejects_status_50_order_without_stale_evidence() -> None:
    runtime, repo, gateway = _runtime()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_live_runtime_status_50",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    gateway._orders[:] = [
        {
            "order_id": "900005",
            "stock_code": "000001.SZ",
            "order_type": 23,
            "order_volume": 100,
            "traded_volume": 0,
            "order_status": 50,
        }
    ]

    result = runtime.execute_operator_command(
        command_id="opcmd_recover_live_order_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="broker status 50 without stale evidence must block",
        payload={"run_id": "simrun_stale_runtime"},
    )

    assert result.status == MiniQMTOperatorCommandStatus.REJECTED
    assert result.errors[0]["error_code"] == "MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT"
    broker_evidence = result.metadata["broker_evidence"]
    assert broker_evidence["broker_open_order_ids"] == ["900005"]
    assert broker_evidence["excluded_stale_order_count"] == 0
    assert broker_evidence["excluded_stale_order_ids"] == []
    assert gateway.cancelled_orders == []
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0].child_order_id == child.child_order_id
    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0].status == MiniQMTAlgoInstanceStatus.ACTIVE


def test_stale_runtime_recovery_rejects_when_broker_has_open_order_without_mutation() -> None:
    runtime, repo, gateway = _runtime(
        gateway=FakeMiniQMTGateway(
            orders=[
                {
                    "order_id": "900003",
                    "stock_code": "000001.SZ",
                    "order_type": 23,
                    "order_volume": 100,
                    "traded_volume": 0,
                    "order_status": 50,
                }
            ]
        )
    )
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_live_broker_order",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)

    result = runtime.execute_operator_command(
        command_id="opcmd_recover_broker_nonempty_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="broker nonempty must reject runtime-only cleanup",
        payload={"run_id": "simrun_stale_runtime"},
    )

    assert result.status == MiniQMTOperatorCommandStatus.REJECTED
    assert result.errors[0]["error_code"] == "MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT"
    assert gateway.cancelled_orders == []
    stored_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.child_order_id == child.child_order_id
    assert stored_child.status == MiniQMTChildOrderStatus.SUBMITTED
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.ACTIVE


def test_stale_runtime_recovery_is_idempotent_when_already_clean() -> None:
    runtime, repo, gateway = _runtime()

    first = runtime.execute_operator_command(
        command_id="opcmd_recover_clean_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="already clean recovery",
        payload={"run_id": "simrun_stale_runtime"},
    )
    second = runtime.execute_operator_command(
        command_id="opcmd_recover_clean_002",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="already clean recovery repeat",
        payload={"run_id": "simrun_stale_runtime"},
    )

    assert first.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert first.metadata["already_clean"] is True
    assert second.status == MiniQMTOperatorCommandStatus.EXECUTED
    assert second.metadata["already_clean"] is True
    assert first.metadata["runtime_only_cleanup_mutated"] is False
    assert second.metadata["runtime_only_cleanup_mutated"] is False
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False) == []
    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=False) == []
    assert gateway.cancelled_orders == []


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
