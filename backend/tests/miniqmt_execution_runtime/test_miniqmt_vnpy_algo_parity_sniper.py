from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    MiniQMTAlgoInstanceStatus,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.qmt_strategy_ledger.models import STATUS_REJECTED
from backend.services.trading_core.models import OrderSide


def _runtime() -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, FakeMiniQMTGateway]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_sniper",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_sniper",
        ),
        repository=repo,
        gateway=gateway,
    )
    return runtime, repo, gateway


def test_runtime_owned_sniper_preserves_vnpy_tick_submit_and_trade_finish_semantics() -> None:
    runtime, repo, gateway = _runtime()
    runtime.start()

    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_sniper_buy_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.0,
    )
    assert algo.metadata["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.99,
        payload={"bid_price_1": 9.98, "bid_volume_1": 1000, "ask_price_1": 9.99, "ask_volume_1": 1000},
    )

    child = repo.list_child_orders(runtime.config.runtime_id)[0]
    assert child.quantity == 1000
    assert child.price == 10.0
    assert child.metadata["source"] == "runtime_owned_vnpy_algo"
    assert child.metadata["vnpy_reason"] == "sniper_ask_crossed_limit"
    assert gateway.submitted_orders and gateway.submitted_orders[0].algo_instance_id == algo.algo_instance_id

    runtime.record_trade_event(broker_order_id=child.broker_order_id or "", quantity=1000, price=9.99)

    completed = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    event_types = [event.event_type for event in repo.list_events(runtime.config.runtime_id)]
    assert completed.status.value == "COMPLETED"
    assert completed.remaining_quantity == 0
    assert MiniQMTExecutionEventType.TRADE_EVENT in event_types
    assert MiniQMTExecutionEventType.ALGO_ACTION_EMITTED in event_types

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.98,
        payload={"bid_price_1": 9.97, "bid_volume_1": 1000, "ask_price_1": 9.98, "ask_volume_1": 1000},
    )

    assert len(repo.list_child_orders(runtime.config.runtime_id, active_only=False)) == 1
    assert len(gateway.submitted_orders) == 1


def test_runtime_owned_sniper_active_order_requests_cancel_before_requote_without_second_submit() -> None:
    runtime, repo, gateway = _runtime()
    runtime.start()
    runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_sniper_requote_000001",
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
    runtime.on_tick(
        symbol="000001.SZ",
        price=9.98,
        payload={"bid_price_1": 9.97, "bid_volume_1": 1000, "ask_price_1": 9.98, "ask_volume_1": 1000},
    )

    event_types = [event.event_type for event in repo.list_events(runtime.config.runtime_id)]
    assert len(gateway.submitted_orders) == 1
    assert MiniQMTExecutionEventType.CHILD_ORDER_CANCEL_REQUESTED in event_types


def test_runtime_owned_sniper_reject_terminalizes_and_later_tick_does_not_resubmit() -> None:
    runtime, repo, gateway = _runtime()
    runtime.start()
    runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_sniper_reject_000001",
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
        status=str(STATUS_REJECTED),
        payload={"order_status": STATUS_REJECTED, "status_msg": "shadow reject parity"},
    )
    rejected_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert rejected_algo.status == MiniQMTAlgoInstanceStatus.FAILED
    assert rejected_algo.metadata["terminalized_by_runtime"] is True
    assert rejected_algo.metadata["terminal_child_order_statuses"] == ["REJECTED"]
    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=True) == []

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.98,
        payload={"bid_price_1": 9.97, "bid_volume_1": 1000, "ask_price_1": 9.98, "ask_volume_1": 1000},
    )

    assert len(repo.list_child_orders(runtime.config.runtime_id, active_only=False)) == 1
    assert len(gateway.submitted_orders) == 1


def test_runtime_owned_sniper_broker_ack_reject_terminalizes_without_resubmit() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway(accept_orders=False)
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_sniper_ack_reject",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_sniper_ack_reject",
        ),
        repository=repo,
        gateway=gateway,
    )
    runtime.start()
    runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_sniper_ack_reject_000001",
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

    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0].status.value == "REJECTED"
    rejected_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert rejected_algo.status == MiniQMTAlgoInstanceStatus.FAILED
    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=True) == []

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.98,
        payload={"bid_price_1": 9.97, "bid_volume_1": 1000, "ask_price_1": 9.98, "ask_volume_1": 1000},
    )

    assert len(repo.list_child_orders(runtime.config.runtime_id, active_only=False)) == 1
    assert len(gateway.submitted_orders) == 1
