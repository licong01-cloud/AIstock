from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from backend.services.miniqmt_execution_runtime import (
    ConfigurableMiniQMTRiskEngine,
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeState,
    MiniQMTRiskDecision,
)
from backend.services.trading_core.models import OrderSide


class _KillOnDisconnectRiskEngine:
    def evaluate_event(self, *, runtime_id: str, event_type: str, payload: dict[str, Any]) -> MiniQMTRiskDecision:
        if event_type == MiniQMTExecutionEventType.GATEWAY_DISCONNECTED.value:
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_DISCONNECT_KILL_SWITCH",
                reason="disconnect kill-switch",
                metadata={"runtime_id": runtime_id, "payload": dict(payload)},
            )
        return MiniQMTRiskDecision.pass_()

    def evaluate_pre_submit(self, **_: Any) -> MiniQMTRiskDecision:
        return MiniQMTRiskDecision.pass_()


class _KillOnTickPriceRiskEngine:
    def __init__(self, *, max_price: float) -> None:
        self.max_price = float(max_price)

    def evaluate_event(self, *, runtime_id: str, event_type: str, payload: dict[str, Any]) -> MiniQMTRiskDecision:
        if event_type != MiniQMTExecutionEventType.TICK.value:
            return MiniQMTRiskDecision.pass_()
        if float(payload.get("price") or 0) <= self.max_price:
            return MiniQMTRiskDecision.pass_()
        return MiniQMTRiskDecision.kill_switch(
            reason_code="MINIQMT_RISK_PRICE_LIMIT_KILL_SWITCH",
            reason="tick price exceeded realtime risk limit",
            metadata={"runtime_id": runtime_id, "price": payload.get("price"), "max_price": self.max_price},
        )

    def evaluate_pre_submit(self, **_: Any) -> MiniQMTRiskDecision:
        return MiniQMTRiskDecision.pass_()


def _runtime(risk_engine: object) -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, FakeMiniQMTGateway]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase4_risk",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase4_risk",
        ),
        repository=repo,
        gateway=gateway,
        risk_engine=risk_engine,
    )
    runtime.start()
    return runtime, repo, gateway


def test_disconnect_risk_kill_switch_cancels_active_children_terminalizes_algos_and_blocks_new_orders() -> None:
    runtime, repo, gateway = _runtime(_KillOnDisconnectRiskEngine())
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_disconnect_risk_000001",
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

    runtime.record_disconnect_event(reason="xtquant disconnected", payload={"transport": "xtquant"})

    event_types = [event.event_type for event in repo.list_events(runtime.config.runtime_id)]
    stored_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    runtime_record = repo.get_runtime(runtime.config.runtime_id)
    assert runtime_record is not None
    assert event_types[-1] == MiniQMTExecutionEventType.RISK_KILL_SWITCH_TRIGGERED
    assert stored_child.child_order_id == child.child_order_id
    assert stored_child.status == MiniQMTChildOrderStatus.CANCELLED
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.CANCELLED
    assert gateway.cancelled_orders[0].child_order_id == child.child_order_id
    assert runtime_record.event_loop_state == MiniQMTExecutionRuntimeState.PAUSED
    assert runtime_record.metadata["kill_switch_reason_code"] == "MINIQMT_RISK_DISCONNECT_KILL_SWITCH"

    with pytest.raises(RuntimeError, match="MINIQMT_RISK_DISCONNECT_KILL_SWITCH"):
        runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)


def test_tick_risk_kill_switch_runs_before_algo_can_submit_new_child_order() -> None:
    runtime, repo, gateway = _runtime(_KillOnTickPriceRiskEngine(max_price=10.0))
    runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_tick_risk_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.2,
    )

    runtime.on_tick(
        symbol="000001.SZ",
        price=10.1,
        payload={"bid_price_1": 10.0, "bid_volume_1": 1000, "ask_price_1": 10.1, "ask_volume_1": 1000},
    )

    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False) == []
    assert gateway.submitted_orders == []
    event_types = [event.event_type for event in repo.list_events(runtime.config.runtime_id)]
    assert MiniQMTExecutionEventType.RISK_KILL_SWITCH_TRIGGERED in event_types


def test_configurable_pre_submit_quantity_limit_blocks_order_loudly_and_terminalizes_instance() -> None:
    runtime, repo, gateway = _runtime(
        ConfigurableMiniQMTRiskEngine({"max_child_order_quantity": 100, "kill_on_disconnect": True})
    )
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_pre_submit_risk_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=200,
        algo_code="SNIPER_MINIQMT",
    )

    with pytest.raises(RuntimeError, match="MINIQMT_RISK_ORDER_QUANTITY_LIMIT_BREACH"):
        runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=200, price=10.0)

    assert gateway.submitted_orders == []
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False) == []
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    runtime_record = repo.get_runtime(runtime.config.runtime_id)
    assert runtime_record is not None
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.CANCELLED
    assert runtime_record.event_loop_state == MiniQMTExecutionRuntimeState.PAUSED
    assert runtime_record.metadata["kill_switch_reason_code"] == "MINIQMT_RISK_ORDER_QUANTITY_LIMIT_BREACH"
    risk_events = [
        event for event in repo.list_events(runtime.config.runtime_id)
        if event.event_type == MiniQMTExecutionEventType.RISK_KILL_SWITCH_TRIGGERED
    ]
    assert risk_events[-1].payload["source_event_type"] == "PRE_SUBMIT"
    assert risk_events[-1].payload["affected_algo_instance_ids"] == [algo.algo_instance_id]


def test_configurable_loss_threshold_uses_account_event_and_blocks_follow_on_orders() -> None:
    runtime, repo, gateway = _runtime(ConfigurableMiniQMTRiskEngine({"max_loss": 100}))

    runtime.record_account_event(payload={"realized_pnl": -120, "unrealized_pnl": 0, "cash": 100000})

    runtime_record = repo.get_runtime(runtime.config.runtime_id)
    assert runtime_record is not None
    assert runtime_record.metadata["kill_switch_reason_code"] == "MINIQMT_RISK_LOSS_LIMIT_BREACH"
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_loss_risk_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    with pytest.raises(RuntimeError, match="MINIQMT_RISK_LOSS_LIMIT_BREACH"):
        runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    assert gateway.submitted_orders == []


def test_configurable_exposure_rule_combines_account_positions_and_pending_buy_notional() -> None:
    runtime, repo, gateway = _runtime(
        ConfigurableMiniQMTRiskEngine({"max_symbol_exposure": {"000001.SZ": 1000}})
    )
    runtime.record_account_event(
        payload={"positions": [{"symbol": "000001.SZ", "market_value": 500.0}], "cash": 100000}
    )
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_exposure_risk_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )

    with pytest.raises(RuntimeError, match="MINIQMT_RISK_EXPOSURE_LIMIT_BREACH"):
        runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=6.0)

    assert gateway.submitted_orders == []
    runtime_record = repo.get_runtime(runtime.config.runtime_id)
    assert runtime_record is not None
    assert runtime_record.metadata["kill_switch_reason_code"] == "MINIQMT_RISK_EXPOSURE_LIMIT_BREACH"

