from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeClient,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    OrderLedgerRecord,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
)
from backend.services.qmt_strategy_ledger.order_service import (
    ManagedOrderSubmitResult,
    OrderPreflightResult,
)
from backend.services.trading_core.models import OrderSide


TRADE_DATE = date(2026, 6, 22)


def _runtime(
    *,
    gateway: FakeMiniQMTGateway | None = None,
) -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, FakeMiniQMTGateway]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    broker_gateway = gateway or FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_bug470_order_lifecycle",
            account_group_id="ag_bug470",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_bug470",
        ),
        repository=repo,
        gateway=broker_gateway,
    )
    runtime.start()
    return runtime, repo, broker_gateway


def _submit_child(runtime: MiniQMTExecutionRuntime):
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_bug470",
        strategy_slot_id="slot_bug470",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    assert child.broker_order_id is not None
    return algo, child


def _preflight() -> OrderPreflightResult:
    return OrderPreflightResult(
        allowed=True,
        errors=(),
        strategy_id="strategy_bug470",
        estimated_notional=Decimal("1000"),
        estimated_fee=Decimal("0"),
        freeze_amount=Decimal("1000"),
        available_cash=Decimal("100000"),
        strategy_available_sell_quantity=None,
        pending_sell_quantity=None,
        broker_can_sell=None,
    )


def _ledger_order(*, qmt_order_id: str, order_status: int, traded_volume: int, order_volume: int = 100) -> OrderLedgerRecord:
    return OrderLedgerRecord(
        intent_id="intent_bug470",
        strategy_id="strategy_bug470",
        strategy_name="slot_bug470",
        qmt_order_id=qmt_order_id,
        symbol="000001.SZ",
        order_type=BUY_ORDER_TYPE,
        order_volume=order_volume,
        traded_volume=traded_volume,
        order_status=order_status,
        account_id="QMT_SIM_ACCOUNT",
        trade_date=TRADE_DATE,
        price=Decimal("10.0"),
        status_msg=f"xtquant status {order_status}",
        order_remark="remark_bug470",
    )


def test_runtime_recover_backfills_partial_fill_child_status_from_broker_status_55() -> None:
    runtime, repo, gateway = _runtime()
    _algo, child = _submit_child(runtime)
    gateway._orders[0].update(
        {
            "order_status": STATUS_PART_SUCC,
            "order_volume": 100,
            "traded_volume": 60,
            "status": "PARTIALLY_FILLED",
        }
    )

    snapshot = runtime.recover()

    stored = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    assert stored.status == MiniQMTChildOrderStatus.PARTIALLY_FILLED
    assert stored.metadata["broker_reconciled_status"] == MiniQMTChildOrderStatus.PARTIALLY_FILLED.value
    assert stored.metadata["broker_reconcile_order"]["order_status"] == STATUS_PART_SUCC
    assert [item.child_order_id for item in snapshot.active_child_orders] == [child.child_order_id]


@pytest.mark.parametrize(
    ("order_status", "expected_child_status", "expected_algo_status", "traded_volume"),
    [
        (STATUS_CANCELLED, MiniQMTChildOrderStatus.CANCELLED, MiniQMTAlgoInstanceStatus.CANCELLED, 60),
        (STATUS_FILLED, MiniQMTChildOrderStatus.FILLED, MiniQMTAlgoInstanceStatus.COMPLETED, 0),
        (STATUS_REJECTED, MiniQMTChildOrderStatus.REJECTED, MiniQMTAlgoInstanceStatus.FAILED, 20),
    ],
)
def test_runtime_recover_treats_xtquant_terminal_statuses_as_terminal(
    order_status: int,
    expected_child_status: MiniQMTChildOrderStatus,
    expected_algo_status: MiniQMTAlgoInstanceStatus,
    traded_volume: int,
) -> None:
    runtime, repo, gateway = _runtime(gateway=FakeMiniQMTGateway())
    algo, _child = _submit_child(runtime)
    gateway._orders[0].update(
        {
            "order_status": order_status,
            "order_volume": 100,
            "traded_volume": traded_volume,
            "status": "SUBMITTED",
        }
    )

    snapshot = runtime.recover()

    stored_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.status == expected_child_status
    assert stored_algo.algo_instance_id == algo.algo_instance_id
    assert stored_algo.status == expected_algo_status
    assert snapshot.active_child_orders == []


def test_runtime_client_managed_child_sync_uses_ledger_partial_status() -> None:
    runtime, repo, _gateway = _runtime()
    _algo, child = _submit_child(runtime)
    client = MiniQMTExecutionRuntimeClient(repository=repo)
    result = ManagedOrderSubmitResult(
        success=True,
        intent_id="intent_bug470",
        qmt_order_id=child.broker_order_id,
        broker_message="accepted",
        preflight=_preflight(),
        broker_called=True,
    )

    updated = client._sync_managed_child_result(
        runtime_id=runtime.config.runtime_id,
        child_order_id=child.child_order_id,
        managed_result=result,
        ledger_order=_ledger_order(qmt_order_id=child.broker_order_id or "", order_status=STATUS_PART_SUCC, traded_volume=60),
        source="bug470_unit",
    )

    assert updated is not None
    assert updated.status == MiniQMTChildOrderStatus.PARTIALLY_FILLED
    assert updated.metadata["broker_synced_child_status"] == MiniQMTChildOrderStatus.PARTIALLY_FILLED.value
    assert updated.metadata["broker_order_ledger"]["order_status"] == STATUS_PART_SUCC
