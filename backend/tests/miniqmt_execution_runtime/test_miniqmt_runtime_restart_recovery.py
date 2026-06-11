from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOmsState,
)
from backend.services.trading_core.models import OrderSide


def _config() -> MiniQMTExecutionRuntimeConfig:
    return MiniQMTExecutionRuntimeConfig(
        runtime_id="mqrt_phase2_restart_recovery",
        account_group_id="ag_minqmt_main_sim",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="runtime_hash_phase2_restart",
    )


def test_restart_recovery_rebuilds_active_state_and_syncs_broker_before_new_orders(tmp_path) -> None:
    store_path = tmp_path / "runtime-store.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    first_gateway = FakeMiniQMTGateway()
    first_runtime = MiniQMTExecutionRuntime(config=_config(), repository=repo, gateway=first_gateway)
    first_runtime.start()
    algo = first_runtime.create_algo_instance(
        parent_intent_id="intent_sell_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=1000,
        algo_code="BEST_LIMIT_MINIQMT",
    )
    child = first_runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=500, price=10.1)
    submitted_before_restart = len(first_gateway.submitted_orders)

    recovery_gateway = FakeMiniQMTGateway(
        orders=[
            {
                "broker_order_id": child.broker_order_id,
                "stock_code": "000001.SZ",
                "status": "SUBMITTED",
                "order_volume": 500,
            }
        ],
        trades=[],
        positions=[{"stock_code": "000001.SZ", "can_sell": 500}],
    )
    recovered_repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    restarted_runtime = MiniQMTExecutionRuntime(config=_config(), repository=recovered_repo, gateway=recovery_gateway)

    snapshot = restarted_runtime.recover()

    assert len(recovery_gateway.submitted_orders) == 0
    assert submitted_before_restart == 1
    assert snapshot.runtime.oms_state == MiniQMTOmsState.RECONCILED
    assert [item.algo_instance_id for item in snapshot.active_algo_instances] == [algo.algo_instance_id]
    assert [item.child_order_id for item in snapshot.active_child_orders] == [child.child_order_id]
    assert snapshot.broker_orders[0]["broker_order_id"] == child.broker_order_id
    assert snapshot.broker_synced_before_new_orders is True

    event_types = [event.event_type for event in snapshot.events]
    broker_synced_index = event_types.index(MiniQMTExecutionEventType.BROKER_SYNCED)
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED in event_types[:broker_synced_index]
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED not in event_types[broker_synced_index + 1 :]
