from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionRuntimeClient,
)
from backend.services.miniqmt_execution_runtime.config import MiniQMTExecutionRuntimeKind
from backend.services.qmt_strategy_ledger.models import (
    STATUS_FILLED,
    STATUS_PART_SUCC,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.trading_core.errors import BrokerSubmitError
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType


TRADE_DATE = date(2026, 6, 23)
ACCOUNT_ID = "QMT_PHASE2_ACCOUNT"


def _event_loop_runtime(
    *,
    runtime_repo: InMemoryMiniQMTExecutionRuntimeRepository | None = None,
    ledger_repo: InMemoryQmtStrategyLedgerRepository | None = None,
):
    strategy_ledger = ledger_repo or InMemoryQmtStrategyLedgerRepository()
    _seed_virtual_account(strategy_ledger)
    client = MiniQMTExecutionRuntimeClient(
        repository=runtime_repo or InMemoryMiniQMTExecutionRuntimeRepository(),
        strategy_ledger_repository=strategy_ledger,
        runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
    )
    runtime = client._runtime(
        account_group_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        runtime_config_hash="phase2_qmt_strategy_oms",
        runtime_id="mqrt_phase2_qmt_strategy_oms",
        gateway=FakeMiniQMTGateway(),
    )
    return runtime, client


def _seed_virtual_account(ledger_repo: InMemoryQmtStrategyLedgerRepository) -> None:
    ledger_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strategy_phase2",
            strategy_name="slot_phase2",
            display_name="Phase 2 slot",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("1000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )


def _create_child(runtime):
    runtime.start()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_phase2_qmt_strategy",
        strategy_slot_id="slot_phase2",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    return runtime.submit_child_order(
        algo_instance_id=algo.algo_instance_id,
        quantity=100,
        price=10.2,
        metadata={
            "strategy_id": "strategy_phase2",
            "strategy_name": "slot_phase2",
            "order_remark": "remark_phase2_child",
        },
    )


def test_event_loop_oms_writes_child_order_and_trade_facts_to_qmt_strategy_ledger_once() -> None:
    ledger_repo = InMemoryQmtStrategyLedgerRepository()
    runtime, _client = _event_loop_runtime(ledger_repo=ledger_repo)

    child = _create_child(runtime)
    order_facts = ledger_repo.list_order_ledger(account_id=ACCOUNT_ID, trade_date=TRADE_DATE)
    assert len(order_facts) == 1
    assert order_facts[0].qmt_order_id == child.broker_order_id
    assert order_facts[0].order_status is None
    assert order_facts[0].raw_json["qmt_strategy_ledger_authority"] is True

    runtime.record_order_event(
        broker_order_id=child.broker_order_id or "",
        status=str(STATUS_PART_SUCC),
        payload={"traded_volume": 60, "status_msg": "partial fill from callback"},
    )
    partial = ledger_repo.get_order_ledger(ACCOUNT_ID, child.broker_order_id or "")
    assert partial is not None
    assert partial.order_status == STATUS_PART_SUCC
    assert partial.traded_volume == 60
    assert partial.status_msg == "partial fill from callback"
    projection = runtime.oms.active_projection(runtime.config.runtime_id)
    assert projection.active_child_orders[0].status == MiniQMTChildOrderStatus.PARTIALLY_FILLED
    assert projection.order_ledger_facts[0].qmt_order_id == child.broker_order_id

    runtime.record_trade_event(
        broker_order_id=child.broker_order_id or "",
        quantity=100,
        price=10.2,
        payload={"trade_id": "trade_phase2_001", "cumulative_quantity": 100},
    )
    runtime.record_trade_event(
        broker_order_id=child.broker_order_id or "",
        quantity=100,
        price=10.2,
        payload={"trade_id": "trade_phase2_001", "cumulative_quantity": 100},
    )

    filled = ledger_repo.get_order_ledger(ACCOUNT_ID, child.broker_order_id or "")
    assert filled is not None
    assert filled.order_status == STATUS_FILLED
    assert filled.traded_volume == 100
    assert len(ledger_repo._trade_ledgers) == 1
    assert runtime.repository.list_child_orders(runtime.config.runtime_id, active_only=True) == []


def test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_runtime_is_retired() -> None:
    ledger_repo = InMemoryQmtStrategyLedgerRepository()
    runtime_repo = InMemoryMiniQMTExecutionRuntimeRepository()
    event_loop_runtime, _event_loop_client = _event_loop_runtime(
        runtime_repo=runtime_repo,
        ledger_repo=ledger_repo,
    )

    assert event_loop_runtime.oms.uses_qmt_strategy_authority is True
    _create_child(event_loop_runtime)
    assert ledger_repo.list_order_ledger(account_id=ACCOUNT_ID, trade_date=TRADE_DATE)

    compiler_ledger = InMemoryQmtStrategyLedgerRepository()
    with pytest.raises(BrokerSubmitError) as exc_info:
        MiniQMTExecutionRuntimeClient(
            repository=InMemoryMiniQMTExecutionRuntimeRepository(),
            strategy_ledger_repository=compiler_ledger,
            runtime_kind=MiniQMTExecutionRuntimeKind.COMPILER,
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_SIM_COMPILER_ROUTE_RETIRED"
    assert exc_info.value.context["stage"] == "MINIQMT_RUNTIME_KIND_REJECTED"
    assert compiler_ledger.list_order_ledger(account_id=ACCOUNT_ID, trade_date=TRADE_DATE) == []


def test_event_loop_recovery_uses_qmt_strategy_ledger_facts_without_resubmitting() -> None:
    ledger_repo = InMemoryQmtStrategyLedgerRepository()
    runtime_repo = InMemoryMiniQMTExecutionRuntimeRepository()
    first_runtime, _client = _event_loop_runtime(runtime_repo=runtime_repo, ledger_repo=ledger_repo)
    child = _create_child(first_runtime)
    order = ledger_repo.get_order_ledger(ACCOUNT_ID, child.broker_order_id or "")
    assert order is not None
    ledger_repo.upsert_order_ledger(replace(order, order_status=STATUS_FILLED, traded_volume=100))

    recovery_gateway = FakeMiniQMTGateway(orders=[], trades=[], positions=[])
    restarted_client = MiniQMTExecutionRuntimeClient(
        repository=runtime_repo,
        strategy_ledger_repository=ledger_repo,
        runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
    )
    restarted_runtime = restarted_client._runtime(
        account_group_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        runtime_config_hash="phase2_qmt_strategy_oms",
        runtime_id=first_runtime.config.runtime_id,
        gateway=recovery_gateway,
    )

    snapshot = restarted_runtime.recover()

    assert recovery_gateway.submitted_orders == []
    assert snapshot.active_child_orders == []
    stored_child = runtime_repo.list_child_orders(first_runtime.config.runtime_id, active_only=False)[0]
    stored_algo = runtime_repo.list_algo_instances(first_runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.status == MiniQMTChildOrderStatus.FILLED
    assert stored_child.metadata["qmt_strategy_ledger_authority"] is True
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.COMPLETED


def test_event_loop_rejects_compiler_style_managed_vnpy_timer_building() -> None:
    client = MiniQMTExecutionRuntimeClient(
        repository=InMemoryMiniQMTExecutionRuntimeRepository(),
        strategy_ledger_repository=InMemoryQmtStrategyLedgerRepository(),
        runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
    )

    with pytest.raises(BrokerSubmitError, match="MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS"):
        client.build_managed_vnpy_order_requests(
            parent_intents=[],
            policy_context={},
            account_group_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            runtime_config_hash="event_loop_refuses_compiler_timer_build",
            runtime_id="mqrt_event_loop_refuses_compiler_timer_build",
            strategy_slot_id="slot_phase2",
            managed_request_factory=lambda child, index: None,  # type: ignore[arg-type]
        )


def test_event_loop_rejects_all_compiler_style_submit_paths_before_sync_lifecycle() -> None:
    client = MiniQMTExecutionRuntimeClient(
        repository=InMemoryMiniQMTExecutionRuntimeRepository(),
        strategy_ledger_repository=InMemoryQmtStrategyLedgerRepository(),
        runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
    )
    intent = OrderIntent(
        package_id="pkg_phase2",
        portfolio_id="portfolio_phase2",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        order_type=OrderType.LIMIT,
        limit_price=10.2,
        target_trade_date=TRADE_DATE,
    )

    with pytest.raises(BrokerSubmitError, match="MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS") as managed_exc:
        client.submit_managed_order_requests(
            managed_order_service=object(),  # type: ignore[arg-type]
            requests=[],
            account_group_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            runtime_config_hash="event_loop_refuses_managed_submit",
            runtime_id="mqrt_event_loop_refuses_managed_submit",
        )
    assert managed_exc.value.context["operation"] == "submit_managed_order_requests"

    with pytest.raises(BrokerSubmitError, match="MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS") as paper_exc:
        client.execute_paper_vnpy_intent(
            portfolio=type("Portfolio", (), {"portfolio_id": "portfolio_phase2"})(),
            run=type("Run", (), {"run_id": "run_phase2"})(),
            trade_date=TRADE_DATE,
            intent=intent,
            broker=object(),  # type: ignore[arg-type]
            execution_policy_context={"policy_json": {"algo_code": "SNIPER_MINIQMT", "algo_config": {}}},
            runtime_config_hash="event_loop_refuses_paper_submit",
            account_group_id=ACCOUNT_ID,
            strategy_slot_id="slot_phase2",
        )
    assert paper_exc.value.context["operation"] == "execute_paper_vnpy_intent"
