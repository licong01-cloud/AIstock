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
from backend.services.trading_core.models import OrderSide


def test_runtime_owned_twap_uses_runtime_timer_not_synchronous_for_loop() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_twap",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_twap",
        ),
        repository=repo,
        gateway=gateway,
    )
    runtime.start()
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_twap_buy_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="TWAP_LITE_MINIQMT",
        limit_price=10.0,
        algo_config={"time": 4, "interval": 2},
    )
    assert algo.metadata["source_attribution"]["upstream_source_file"].endswith("twap_algo.py")

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.99,
        payload={"bid_price_1": 9.98, "bid_volume_1": 1000, "ask_price_1": 9.99, "ask_volume_1": 1000},
    )
    runtime.on_timer(timer_name="phase3_twap_second_1")
    assert repo.list_child_orders(runtime.config.runtime_id) == []

    runtime.on_timer(timer_name="phase3_twap_second_2")

    child = repo.list_child_orders(runtime.config.runtime_id)[0]
    event_types = [event.event_type for event in repo.list_events(runtime.config.runtime_id)]
    assert child.quantity == 500
    assert child.price == 10.0
    assert child.metadata["vnpy_reason"] == "twap_lite_interval_buy"
    assert event_types.count(MiniQMTExecutionEventType.TIMER) == 2
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED in event_types


def test_runtime_owned_twap_survives_child_fill_until_window_end_and_next_timer_slice() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_twap_window_lifecycle",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_twap_window_lifecycle",
        ),
        repository=repo,
        gateway=gateway,
    )
    runtime.start()
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_twap_window_buy_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="TWAP_LITE_MINIQMT",
        limit_price=10.0,
        algo_config={"time": 6, "interval": 2},
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=9.99,
        payload={"bid_price_1": 9.98, "bid_volume_1": 1000, "ask_price_1": 9.99, "ask_volume_1": 1000},
    )
    runtime.on_timer(timer_name="twap_window_second_1")
    runtime.on_timer(timer_name="twap_window_second_2")
    first_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]

    runtime.record_trade_event(broker_order_id=first_child.broker_order_id or "", quantity=300, price=9.99)

    after_fill = repo.list_algo_instances(runtime.config.runtime_id, active_only=True)[0]
    assert after_fill.algo_instance_id == algo.algo_instance_id
    assert after_fill.status == MiniQMTAlgoInstanceStatus.ACTIVE
    assert after_fill.remaining_quantity == 700

    runtime.on_timer(timer_name="twap_window_second_3")
    runtime.on_timer(timer_name="twap_window_second_4")

    children = repo.list_child_orders(runtime.config.runtime_id, active_only=False)
    assert len(children) == 2
    assert children[1].quantity == 300
    assert children[1].metadata["vnpy_reason"] == "twap_lite_interval_buy"

    runtime.on_timer(timer_name="twap_window_second_5")
    runtime.on_timer(timer_name="twap_window_second_6")

    terminal = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert terminal.status == MiniQMTAlgoInstanceStatus.COMPLETED
    assert terminal.metadata["vnpy_algo_state"]["finished_reason"] == "twap_lite_total_time_exhausted"
