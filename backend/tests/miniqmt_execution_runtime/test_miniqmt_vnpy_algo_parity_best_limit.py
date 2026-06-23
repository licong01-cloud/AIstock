from __future__ import annotations

from datetime import date

import pytest

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.qmt_strategy_ledger.models import STATUS_CANCELLED
from backend.services.trading_core.models import OrderSide


def test_runtime_owned_best_limit_submits_at_best_bid_and_cancel_replaces_on_quote_change() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_best_limit",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_best_limit",
        ),
        repository=repo,
        gateway=gateway,
    )
    runtime.start()
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_best_limit_buy_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="BEST_LIMIT_MINIQMT",
        limit_price=10.0,
        algo_config={"min_volume": 100, "max_volume": 500},
        random_volume_provider=lambda _min, _max: 350,
    )
    assert algo.metadata["source_attribution"]["upstream_source_file"].endswith("best_limit_algo.py")

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.88,
        payload={"bid_price_1": 9.88, "bid_volume_1": 1000, "ask_price_1": 10.12, "ask_volume_1": 1000},
    )

    child = repo.list_child_orders(runtime.config.runtime_id)[0]
    assert child.price == 9.88
    assert child.quantity == 300
    assert child.metadata["vnpy_reason"] == "best_limit_buy_at_bid_price_1"

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.89,
        payload={"bid_price_1": 9.89, "bid_volume_1": 1000, "ask_price_1": 10.12, "ask_volume_1": 1000},
    )

    event_types = [event.event_type for event in repo.list_events(runtime.config.runtime_id)]
    assert len(gateway.submitted_orders) == 1
    assert MiniQMTExecutionEventType.CHILD_ORDER_CANCEL_REQUESTED in event_types


def test_runtime_owned_best_limit_stays_alive_after_algo_cancel_until_real_cancel_callback_then_requotes() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_best_limit_cancel_requote",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_best_limit_cancel_requote",
        ),
        repository=repo,
        gateway=gateway,
    )
    runtime.start()
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_best_limit_cancel_requote_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="BEST_LIMIT_MINIQMT",
        limit_price=10.0,
        algo_config={"min_volume": 100, "max_volume": 500},
        random_volume_provider=lambda _min, _max: 350,
    )

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.88,
        payload={"bid_price_1": 9.88, "bid_volume_1": 1000, "ask_price_1": 10.12, "ask_volume_1": 1000},
    )
    first_child = repo.list_child_orders(runtime.config.runtime_id)[0]
    runtime.on_tick(
        symbol="000001.SZ",
        price=9.89,
        payload={"bid_price_1": 9.89, "bid_volume_1": 1000, "ask_price_1": 10.12, "ask_volume_1": 1000},
    )

    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=True)[0].algo_instance_id == algo.algo_instance_id
    assert len(gateway.cancelled_orders) == 1
    runtime.record_order_event(
        broker_order_id=first_child.broker_order_id or "",
        status=str(STATUS_CANCELLED),
        payload={"order_status": STATUS_CANCELLED, "status_msg": "broker cancel confirmed"},
    )

    stored_first = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    assert stored_first.status == MiniQMTChildOrderStatus.CANCELLED
    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=True)[0].status == MiniQMTAlgoInstanceStatus.ACTIVE

    runtime.on_tick(
        symbol="000001.SZ",
        price=9.89,
        payload={"bid_price_1": 9.89, "bid_volume_1": 1000, "ask_price_1": 10.12, "ask_volume_1": 1000},
    )

    children = repo.list_child_orders(runtime.config.runtime_id, active_only=False)
    assert len(children) == 2
    assert children[1].price == 9.89
    assert children[1].metadata["vnpy_reason"] == "best_limit_buy_at_bid_price_1"


def test_runtime_owned_vnpy_algo_fails_fast_without_broker_best_quote_fields() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase3_best_limit_missing_quote",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase3_best_limit_missing_quote",
        ),
        repository=repo,
        gateway=gateway,
    )
    runtime.start()
    runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_best_limit_missing_quote_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="BEST_LIMIT_MINIQMT",
        limit_price=10.0,
        algo_config={"min_volume": 100, "max_volume": 500},
    )

    with pytest.raises(RuntimeError, match="requires broker best-quote fields"):
        runtime.on_tick(symbol="000001.SZ", price=9.88, payload={"ask_price_1": 10.12, "ask_volume_1": 1000})
