from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.trading_core.models import OrderSide


def test_submit_rejection_remains_terminal_after_reconcile_success() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway(accept_orders=False)
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase2_submit_reject",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase2_submit_reject",
        ),
        repository=repo,
        gateway=gateway,
    )

    runtime.start()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_reject_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=1000,
        algo_code="SNIPER_MINIQMT",
    )
    rejected = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=1000, price=10.2)

    snapshot = runtime.reconcile()

    stored_orders = repo.list_child_orders(runtime.config.runtime_id, active_only=False)
    assert rejected.status == MiniQMTChildOrderStatus.REJECTED
    assert stored_orders[0].status == MiniQMTChildOrderStatus.REJECTED
    assert snapshot.active_child_orders == []
    assert MiniQMTExecutionEventType.CHILD_ORDER_REJECTED in {event.event_type for event in snapshot.events}
    assert len(gateway.submitted_orders) == 1
