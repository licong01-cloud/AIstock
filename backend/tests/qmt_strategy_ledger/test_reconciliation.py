from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.qmt_strategy_ledger.models import (
    MiniQmtAccountGroup,
    MiniQmtStrategySlot,
    PositionLotRecord,
    UnattributedOrderRecord,
    UnattributedTradeRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


def _repo_with_overlap_lots() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    for strategy_id, strategy_name in [("strat_a", "poc_strategy_a"), ("strat_b", "poc_strategy_b")]:
        repo.create_virtual_account(
            VirtualAccount(
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                display_name=strategy_name,
                account_id=ACCOUNT_ID,
                mode="SIM",
                initial_cash=Decimal("10000000"),
                cash=Decimal("10000000"),
                status=VirtualAccountStatus.ENABLED,
            )
        )
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_a",
            strategy_id="strat_a",
            symbol="001358.SZ",
            open_trade_id="trade_a",
            open_date=TRADE_DATE,
            quantity=7600,
            available_quantity=0,
            remaining_quantity=7600,
            avg_cost=Decimal("29.88"),
            cost_amount=Decimal("227088"),
            account_id=ACCOUNT_ID,
        )
    )
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_b",
            strategy_id="strat_b",
            symbol="001358.SZ",
            open_trade_id="trade_b",
            open_date=TRADE_DATE,
            quantity=6600,
            available_quantity=0,
            remaining_quantity=6600,
            avg_cost=Decimal("29.88"),
            cost_amount=Decimal("197208"),
            account_id=ACCOUNT_ID,
        )
    )
    return repo


def test_reconciliation_accepts_overlap_strategy_lots_when_broker_quantity_matches_sum() -> None:
    repo = _repo_with_overlap_lots()

    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        broker_positions=[{"stock_code": "001358.SZ", "quantity": 14200}],
    )

    assert report.run.status == "SUCCEEDED"
    assert report.run.completed_at is not None
    assert report.run.summary_json["issue_count"] == 0
    assert report.issues == ()
    assert report.overlap_symbols == ("001358.SZ",)
    assert report.strategy_lot_quantities["poc_strategy_a"]["001358.SZ"] == 7600
    assert report.strategy_lot_quantities["poc_strategy_b"]["001358.SZ"] == 6600


def test_reconciliation_reports_position_mismatch_and_unattributed_trade() -> None:
    repo = _repo_with_overlap_lots()
    repo.upsert_unattributed_trade(
        UnattributedTradeRecord(
            unattributed_id="ut_a",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            trade_id="trade_unknown",
            qmt_order_id="order_unknown",
            symbol="001358.SZ",
            reason="UNKNOWN_ORDER_INTENT",
            order_remark="manual_order",
        )
    )

    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        broker_positions=[{"stock_code": "001358.SZ", "quantity": 13200}],
    )

    issue_types = [issue.issue_type for issue in report.issues]
    assert report.run.status == "WARNING"
    assert report.run.completed_at is not None
    assert report.run.summary_json["issue_count"] == 2
    assert issue_types == ["POSITION_MISMATCH", "UNATTRIBUTED_TRADE"]
    mismatch = report.issues[0]
    assert mismatch.context == {"strategy_quantity": 14200, "broker_quantity": 13200}
    assert report.unattributed_trades == 1


def test_reconciliation_strategy_scope_keeps_cross_slot_mismatch_account_level() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_account_group_slots(
        MiniQmtAccountGroup(
            account_group_id="ag_test",
            broker_account_id=ACCOUNT_ID,
            slots=(
                MiniQmtStrategySlot(
                    account_group_id="ag_test",
                    strategy_slot_id="slot_current",
                    strategy_id="strat_current",
                    strategy_name="StrategyCurrent",
                    display_name="Strategy Current",
                    account_id=ACCOUNT_ID,
                    allocated_cash=Decimal("100000"),
                    order_remark_prefix="cur",
                ),
                MiniQmtStrategySlot(
                    account_group_id="ag_test",
                    strategy_slot_id="slot_stale",
                    strategy_id="strat_stale",
                    strategy_name="StrategyStale",
                    display_name="Strategy Stale",
                    account_id=ACCOUNT_ID,
                    allocated_cash=Decimal("100000"),
                    order_remark_prefix="stale",
                ),
            ),
        )
    )
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_current_ok",
            strategy_id="strat_current",
            symbol="000001.SZ",
            open_trade_id="trade_current_ok",
            open_date=TRADE_DATE,
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("1000.00"),
            account_id=ACCOUNT_ID,
        )
    )
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_stale_missing",
            strategy_id="strat_stale",
            symbol="000002.SZ",
            open_trade_id="trade_stale_missing",
            open_date=TRADE_DATE,
            quantity=200,
            available_quantity=200,
            remaining_quantity=200,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("2000.00"),
            account_id=ACCOUNT_ID,
        )
    )

    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        broker_positions=[{"stock_code": "000001.SZ", "quantity": 100}],
    )
    scope = report.strategy_scope(strategy_id="strat_current", strategy_name="StrategyCurrent")

    assert report.run.status == "WARNING"
    assert report.run.summary_json["issue_count"] == 1
    assert scope["matched"] is True
    assert scope["status"] == "SUCCEEDED"
    assert scope["issue_count"] == 0
    assert scope["account_level_issue_count"] == 1
    assert scope["order_remark_prefix"] == "cur"

    missing_scope = report.strategy_scope(strategy_id="strat_missing", strategy_name="StrategyMissing")
    assert missing_scope["matched"] is False
    assert missing_scope["status"] == "WARNING"


def test_reconciliation_strategy_scope_keeps_unknown_unattributed_order_account_level() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_account_group_slots(
        MiniQmtAccountGroup(
            account_group_id="ag_unattributed",
            broker_account_id=ACCOUNT_ID,
            slots=(
                MiniQmtStrategySlot(
                    account_group_id="ag_unattributed",
                    strategy_slot_id="slot_current",
                    strategy_id="strat_current",
                    strategy_name="StrategyCurrent",
                    display_name="Strategy Current",
                    account_id=ACCOUNT_ID,
                    allocated_cash=Decimal("100000"),
                    order_remark_prefix="cur",
                ),
            ),
        )
    )
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_current_ok",
            strategy_id="strat_current",
            symbol="000001.SZ",
            open_trade_id="trade_current_ok",
            open_date=TRADE_DATE,
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("1000.00"),
            account_id=ACCOUNT_ID,
        )
    )
    repo.upsert_unattributed_order(
        UnattributedOrderRecord(
            unattributed_id="uo_manual",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            qmt_order_id="manual_order",
            symbol="000999.SZ",
            reason="UNKNOWN_ORDER_INTENT",
            order_remark="manual_order",
        )
    )

    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        broker_positions=[{"stock_code": "000001.SZ", "quantity": 100}],
    )
    scope = report.strategy_scope(strategy_id="strat_current", strategy_name="StrategyCurrent")

    assert report.run.status == "WARNING"
    assert scope["matched"] is True
    assert scope["status"] == "SUCCEEDED"
    assert scope["issue_count"] == 0
    assert scope["account_level_issue_count"] == 1
    assert scope["account_level_issue_types"] == ["UNATTRIBUTED_ORDER"]


def test_reconciliation_strategy_scope_blocks_prefixed_unattributed_order() -> None:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_account_group_slots(
        MiniQmtAccountGroup(
            account_group_id="ag_unattributed",
            broker_account_id=ACCOUNT_ID,
            slots=(
                MiniQmtStrategySlot(
                    account_group_id="ag_unattributed",
                    strategy_slot_id="slot_current",
                    strategy_id="strat_current",
                    strategy_name="StrategyCurrent",
                    display_name="Strategy Current",
                    account_id=ACCOUNT_ID,
                    allocated_cash=Decimal("100000"),
                    order_remark_prefix="cur",
                ),
            ),
        )
    )
    repo.upsert_unattributed_order(
        UnattributedOrderRecord(
            unattributed_id="uo_current",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
            qmt_order_id="current_order",
            symbol="000999.SZ",
            reason="UNKNOWN_ORDER_INTENT",
            order_remark="cur-20260612-001",
        )
    )

    report = QmtStrategyLedgerReconciliationService(repository=repo).reconcile_snapshot(
        account_id=ACCOUNT_ID,
        trade_date=TRADE_DATE,
        broker_positions=[],
    )
    scope = report.strategy_scope(strategy_id="strat_current", strategy_name="StrategyCurrent")

    assert scope["matched"] is True
    assert scope["status"] == "WARNING"
    assert scope["issue_count"] == 1
    assert scope["issue_types"] == ["UNATTRIBUTED_ORDER"]
