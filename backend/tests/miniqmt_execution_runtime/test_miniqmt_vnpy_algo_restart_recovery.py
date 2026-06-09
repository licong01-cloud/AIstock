from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.trading_core.models import OrderSide


def _config() -> MiniQMTExecutionRuntimeConfig:
    return MiniQMTExecutionRuntimeConfig(
        runtime_id="mqrt_phase3_restart_vnpy",
        account_group_id="ag_minqmt_main_sim",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="runtime_hash_phase3_restart_vnpy",
    )


def test_runtime_owned_vnpy_algo_restores_active_order_state_after_restart(tmp_path) -> None:
    store_path = tmp_path / "runtime-vnpy-store.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    first_gateway = FakeMiniQMTGateway()
    first_runtime = MiniQMTExecutionRuntime(config=_config(), repository=repo, gateway=first_gateway)
    first_runtime.start()
    first_runtime.create_vnpy_algo_instance(
        parent_intent_id="intent_restart_sniper_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.0,
    )
    first_runtime.on_tick(
        symbol="000001.SZ",
        price=9.99,
        payload={"bid_price_1": 9.98, "bid_volume_1": 1000, "ask_price_1": 9.99, "ask_volume_1": 1000},
    )
    assert len(first_gateway.submitted_orders) == 1

    recovered_repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    recovery_gateway = FakeMiniQMTGateway()
    restarted_runtime = MiniQMTExecutionRuntime(config=_config(), repository=recovered_repo, gateway=recovery_gateway)
    restarted_runtime.recover()
    restarted_runtime.on_tick(
        symbol="000001.SZ",
        price=9.98,
        payload={"bid_price_1": 9.97, "bid_volume_1": 1000, "ask_price_1": 9.98, "ask_volume_1": 1000},
    )

    event_types = [event.event_type for event in recovered_repo.list_events(_config().runtime_id)]
    assert len(recovery_gateway.submitted_orders) == 0
    assert len(recovery_gateway.cancelled_orders) == 1
    assert MiniQMTExecutionEventType.CHILD_ORDER_CANCEL_REQUESTED in event_types
