from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import qmt_strategy_ledger
from backend.services.qmt_strategy_ledger.models import (
    BindingStatus,
    CashEntryType,
    CashLedgerEntry,
    PositionLotRecord,
    StrategyPackageBinding,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


def _account(strategy_id: str, strategy_name: str) -> VirtualAccount:
    return VirtualAccount(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        display_name=strategy_name.replace("_", " "),
        account_id=ACCOUNT_ID,
        mode="SIM",
        initial_cash=Decimal("10000000"),
        cash=Decimal("9900000"),
        frozen_cash=Decimal("50000"),
        market_value=Decimal("100000"),
        realized_pnl=Decimal("1200"),
        unrealized_pnl=Decimal("800"),
        status=VirtualAccountStatus.ENABLED,
    )


def _lot(strategy_id: str, lot_id: str, quantity: int) -> PositionLotRecord:
    return PositionLotRecord(
        lot_id=lot_id,
        strategy_id=strategy_id,
        symbol="300604.SZ",
        open_trade_id=f"trade_{lot_id}",
        open_date=TRADE_DATE,
        quantity=quantity,
        available_quantity=0,
        remaining_quantity=quantity,
        avg_cost=Decimal("10.00"),
        cost_amount=Decimal(quantity * 10),
        account_id=ACCOUNT_ID,
    )


def _repo() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account("strat_a", "poc_strategy_a"))
    repo.create_virtual_account(_account("strat_b", "poc_strategy_b"))
    repo.create_position_lot(_lot("strat_a", "lot_a", 1000))
    repo.create_position_lot(_lot("strat_b", "lot_b", 500))
    repo.append_cash_entry(
        CashLedgerEntry(
            cash_id="cash_a",
            strategy_id="strat_a",
            entry_type=CashEntryType.INITIAL_ALLOCATE,
            cash_delta=Decimal("10000000"),
            cash_after=Decimal("10000000"),
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
        )
    )
    repo.create_package_binding(
        StrategyPackageBinding(
            binding_id="bind_a",
            strategy_id="strat_a",
            package_id="pkg_a",
            manifest_sha256="sha_a",
            selection_run_id="sel_a",
            trade_date=TRADE_DATE,
            target_weight=Decimal("0.02"),
            top_k=20,
            binding_status=BindingStatus.ACTIVE,
        )
    )
    return repo


def _client(repo: InMemoryQmtStrategyLedgerRepository) -> TestClient:
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: object())
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    return TestClient(app)


def test_virtual_strategy_summary_exposes_accounts_lots_and_overlap() -> None:
    response = _client(_repo()).get(
        "/api/v1/qmt/virtual-strategies/summary",
        params={"account_id": ACCOUNT_ID, "trade_date": TRADE_DATE.isoformat()},
    )

    assert response.status_code == 200
    summary = response.json()["summary"]
    assert summary["strategy_count"] == 2
    assert summary["overlap_symbols"] == ["300604.SZ"]
    assert summary["unattributed_orders"] == 0
    assert summary["unattributed_trades"] == 0

    strat_a = next(row for row in summary["strategies"] if row["strategy_id"] == "strat_a")
    assert strat_a["active_binding"]["package_id"] == "pkg_a"
    assert strat_a["active_binding"]["selection_run_id"] == "sel_a"
    assert strat_a["positions"] == [
        {
            "symbol": "300604.SZ",
            "quantity": 1000,
            "available_quantity": 0,
            "remaining_quantity": 1000,
            "cost_amount": 10000.0,
            "avg_cost": 10.0,
            "realized_pnl": 0.0,
            "lot_count": 1,
        }
    ]
