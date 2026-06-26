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
    PositionLotRecord,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import (
    ManagedOrderSubmitResult,
    OrderPreflightResult,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
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


def test_dependent_buy_released_by_sell_trade_event_after_ledger_cash_sufficient() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )
    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=100,
        price=10.0,
        payload={"trade_id": "trade_d35_full_sell", "cumulative_quantity": 100},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL, OrderSide.BUY]
    released = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert released.algo_instance_id == buy_algo.algo_instance_id
    assert released.metadata["dependent_buy_status"] == "RELEASED_SUBMITTED"
    assert released.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_RELEASED_AFTER_SELL_TRADE"
    assert released.metadata["dependent_buy_last_context"]["cash_source"] == "qmt_strategy_ledger.virtual_account.cash"
    assert qmt_repo.get_virtual_account("strategy_bug528").cash == Decimal("1000.000000")


def test_dependent_buy_inferred_from_same_runtime_sell_and_released_by_trade_event() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_inferred_dependent_buy(runtime, price=10.0, quantity=100)
    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=100,
        price=10.0,
        payload={"trade_id": "trade_d35_inferred_sell", "cumulative_quantity": 100},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL, OrderSide.BUY]
    released = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert released.algo_instance_id == buy_algo.algo_instance_id
    assert released.metadata["dependent_buy_inferred"] is True
    assert released.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_RELEASED_AFTER_SELL_TRADE"


def test_dependent_buy_partial_sell_keeps_deferred_when_ledger_cash_insufficient() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=50,
        price=10.0,
        payload={"trade_id": "trade_d35_partial_sell", "cumulative_quantity": 50},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    deferred = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert deferred.algo_instance_id == buy_algo.algo_instance_id
    assert deferred.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    context = deferred.metadata["dependent_buy_last_context"]
    assert deferred.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_CASH_STILL_INSUFFICIENT"
    assert context["available_cash"] == "500.000000"
    assert context["cash_shortfall"] == "500.000000"
    assert context["cash_source"] == "qmt_strategy_ledger.virtual_account.cash"


def test_dependent_buy_blocked_when_dependent_sell_cancelled_without_proceeds() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )

    runtime.record_order_event(
        broker_order_id=sell_child.broker_order_id or "",
        status="CANCELLED",
        payload={"status": "CANCELLED", "status_msg": "cancelled before any fill"},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    blocked = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert blocked.algo_instance_id == buy_algo.algo_instance_id
    assert blocked.status == MiniQMTAlgoInstanceStatus.FAILED
    assert blocked.metadata["dependent_buy_status"] == "BLOCKED_SELL_PROCEEDS_UNAVAILABLE"
    assert (
        blocked.metadata["dependent_buy_reason_code"]
        == "MINIQMT_DEPENDENT_BUY_DEPENDENT_SELL_TERMINAL_WITHOUT_PROCEEDS"
    )


def test_dependent_buy_eod_residual_records_shortfall_without_broker_submit() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("200"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )

    runtime.on_timer(timer_name="EOD_DEPENDENT_BUY_SWEEP", payload={"source": "unit_test"})

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    residual = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert residual.algo_instance_id == buy_algo.algo_instance_id
    assert residual.status == MiniQMTAlgoInstanceStatus.FAILED
    assert residual.metadata["dependent_buy_status"] == "EOD_RESIDUAL"
    assert residual.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_EOD_RESIDUAL"
    assert residual.metadata["dependent_buy_last_context"]["available_cash"] == "200"
    assert residual.metadata["dependent_buy_last_context"]["cash_shortfall"] == "800"


def test_dependent_buy_release_requires_qmt_strategy_ledger_authority_not_runtime_json_estimate() -> None:
    runtime, repo, gateway = _runtime(gateway=FakeMiniQMTGateway())
    sell_algo = runtime.create_algo_instance(
        parent_intent_id="sell_no_ledger",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    sell_child = runtime.submit_child_order(
        algo_instance_id=sell_algo.algo_instance_id,
        quantity=100,
        price=10.0,
        metadata={"strategy_id": "strategy_bug528", "order_remark": "sell_no_ledger"},
    )
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
        extra_metadata={"estimated_sell_proceeds": "999999999"},
    )

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=100,
        price=10.0,
        payload={"trade_id": "trade_no_ledger", "cumulative_quantity": 100},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    deferred = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert deferred.algo_instance_id == buy_algo.algo_instance_id
    assert deferred.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    assert deferred.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_LEDGER_AUTHORITY_MISSING"
    assert "estimated_sell_proceeds" not in deferred.metadata["dependent_buy_last_context"]


def _dependent_runtime(
    *,
    cash: Decimal,
) -> tuple[
    MiniQMTExecutionRuntime,
    InMemoryMiniQMTExecutionRuntimeRepository,
    FakeMiniQMTGateway,
    InMemoryQmtStrategyLedgerRepository,
]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strategy_bug528",
            strategy_name="slot_bug528",
            display_name="BUG-528 strategy",
            account_id="ag_bug528",
            mode="SIM",
            initial_cash=Decimal("1"),
            cash=cash,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_bug528_dependent_buy",
            account_group_id="ag_bug528",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_bug528",
        ),
        repository=repo,
        gateway=gateway,
        strategy_ledger_repository=qmt_repo,
        account_id="ag_bug528",
    )
    runtime.start()
    return runtime, repo, gateway, qmt_repo


def _submit_dependent_sell(
    runtime: MiniQMTExecutionRuntime,
    *,
    qmt_repo: InMemoryQmtStrategyLedgerRepository,
    quantity: int,
    price: Decimal,
):
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id=f"lot_bug528_{quantity}_{price}",
            strategy_id="strategy_bug528",
            symbol="000001.SZ",
            open_trade_id="open_trade_bug528",
            open_date=TRADE_DATE,
            quantity=quantity,
            available_quantity=quantity,
            remaining_quantity=quantity,
            avg_cost=Decimal("8"),
            cost_amount=Decimal("8") * Decimal(quantity),
            account_id="ag_bug528",
        )
    )
    algo = runtime.create_algo_instance(
        parent_intent_id="sell_parent_bug528",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=quantity,
        algo_code="SNIPER_MINIQMT",
    )
    return runtime.submit_child_order(
        algo_instance_id=algo.algo_instance_id,
        quantity=quantity,
        price=float(price),
        metadata={
            "strategy_id": "strategy_bug528",
            "strategy_name": "slot_bug528",
            "order_remark": "sell_remark_bug528",
        },
    )


def _create_deferred_dependent_buy(
    runtime: MiniQMTExecutionRuntime,
    *,
    sell_child,
    required_cash: Decimal,
    price: float,
    quantity: int,
    extra_metadata: dict | None = None,
):
    metadata = {
        "dependent_buy": True,
        "dependent_buy_required_cash": str(required_cash),
        "dependent_buy_strategy_id": "strategy_bug528",
        "dependent_sell_child_order_ids": [sell_child.child_order_id],
        "dependent_sell_parent_intent_ids": [sell_child.parent_intent_id],
        "dependent_sell_symbols": [sell_child.symbol],
        "runtime_child_context": {
            "strategy_id": "strategy_bug528",
            "strategy_name": "slot_bug528",
            "order_remark": "buy_remark_bug528",
            "parent_intent_metadata": {
                "strategy_id": "strategy_bug528",
                "dependent_buy": True,
            },
        },
        **dict(extra_metadata or {}),
    }
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="buy_parent_bug528",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=quantity,
        algo_code="SNIPER_MINIQMT",
        limit_price=price,
        metadata=metadata,
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=price,
        payload={
            "bid_price_1": price - 0.01,
            "bid_volume_1": quantity,
            "ask_price_1": price,
            "ask_volume_1": quantity,
        },
    )
    stored = runtime.repository.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert stored.algo_instance_id == algo.algo_instance_id
    assert stored.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    assert stored.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_DEFERRED_WAITING_SELL_PROCEEDS"
    return stored


def _create_inferred_dependent_buy(
    runtime: MiniQMTExecutionRuntime,
    *,
    price: float,
    quantity: int,
):
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="buy_parent_bug528_inferred",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=quantity,
        algo_code="SNIPER_MINIQMT",
        limit_price=price,
        metadata={
            "runtime_child_context": {
                "strategy_id": "strategy_bug528",
                "strategy_name": "slot_bug528",
                "order_remark": "buy_inferred_bug528",
            },
        },
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=price,
        payload={
            "bid_price_1": price - 0.01,
            "bid_volume_1": quantity,
            "ask_price_1": price,
            "ask_volume_1": quantity,
        },
    )
    stored = runtime.repository.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert stored.algo_instance_id == algo.algo_instance_id
    assert stored.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    assert stored.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_DEFERRED_WAITING_SELL_PROCEEDS"
    assert stored.metadata["dependent_buy_inferred"] is True
    return stored
