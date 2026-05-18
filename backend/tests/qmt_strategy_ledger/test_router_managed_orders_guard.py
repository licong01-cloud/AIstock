from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import qmt_strategy_ledger
from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"


def _repo() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    return repo


def _client(repo: InMemoryQmtStrategyLedgerRepository) -> TestClient:
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: object())
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    return TestClient(app)


def _payload() -> dict:
    return {
        "account_id": ACCOUNT_ID,
        "strategy_name": "poc_strategy_a",
        "stock_code": "300604.SZ",
        "order_type": 23,
        "order_volume": 1000,
        "price_type": 5,
        "price": 10,
        "order_remark": "remark_router",
        "trade_date": date(2026, 5, 18).isoformat(),
        "mode": "SIM",
    }


def test_preview_router_is_available_without_real_order_switch(monkeypatch) -> None:
    monkeypatch.delenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", raising=False)
    monkeypatch.delenv("AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST", raising=False)

    response = _client(_repo()).post("/api/v1/qmt/virtual-strategies/orders/preview", json=_payload())

    assert response.status_code == 200
    assert response.json()["preflight"]["allowed"] is True


def test_submit_router_is_disabled_without_explicit_real_order_switch(monkeypatch) -> None:
    monkeypatch.delenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", raising=False)
    monkeypatch.delenv("AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST", raising=False)

    response = _client(_repo()).post("/api/v1/qmt/virtual-strategies/orders", json=_payload())

    assert response.status_code == 403
    assert "disabled by default" in response.json()["detail"]


def test_submit_router_blocks_live_mode_even_when_sim_switch_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", "1")
    monkeypatch.delenv("AISTOCK_ALLOW_MINIQMT_LIVE_MANAGED_ORDERS", raising=False)
    payload = _payload()
    payload["mode"] = "LIVE"

    response = _client(_repo()).post("/api/v1/qmt/virtual-strategies/orders", json=payload)

    assert response.status_code == 403
    assert "allows SIM only" in response.json()["detail"]
