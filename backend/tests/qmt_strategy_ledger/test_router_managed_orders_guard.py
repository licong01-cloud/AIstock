from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import qmt, qmt_strategy_ledger
from backend.services.qmt_strategy_ledger.models import BUY_ORDER_TYPE, IntentSubmitStatus, VirtualAccount, VirtualAccountStatus
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


class FakeQmtClient:
    def __init__(self) -> None:
        self.place_order_calls: list[dict] = []

    def place_order(self, **kwargs) -> tuple[int, str]:
        self.place_order_calls.append(kwargs)
        return 10001, "accepted"


def _raw_client(fake_client: FakeQmtClient, monkeypatch) -> TestClient:
    app = FastAPI()
    app.include_router(qmt.router, prefix="/api/v1")
    monkeypatch.setattr(qmt, "_get_client", lambda: fake_client)
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


def _raw_payload() -> dict:
    return {
        "stock_code": "300604.SZ",
        "order_type": 23,
        "order_volume": 1000,
        "price_type": 5,
        "price": 10,
        "strategy_name": "",
        "order_remark": "",
        "trade_password": "secret",
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


def test_raw_order_router_is_disabled_by_default_and_does_not_call_broker(monkeypatch) -> None:
    monkeypatch.setenv("QMT_TRADE_PASSWORD", "secret")
    monkeypatch.delenv("AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS", raising=False)
    fake_client = FakeQmtClient()

    response = _raw_client(fake_client, monkeypatch).post("/api/v1/qmt/order", json=_raw_payload())

    assert response.status_code == 403
    assert "administrator/POC diagnostics only" in response.json()["detail"]
    assert "/api/v1/qmt/virtual-strategies/orders" in response.json()["detail"]
    assert fake_client.place_order_calls == []


def test_raw_batch_order_router_is_disabled_by_default_and_does_not_call_broker(monkeypatch) -> None:
    monkeypatch.setenv("QMT_TRADE_PASSWORD", "secret")
    monkeypatch.delenv("AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS", raising=False)
    fake_client = FakeQmtClient()

    response = _raw_client(fake_client, monkeypatch).post(
        "/api/v1/qmt/order/batch",
        json={"trade_password": "secret", "orders": [_raw_payload(), {**_raw_payload(), "stock_code": "300054.SZ"}]},
    )

    assert response.status_code == 403
    assert "administrator/POC diagnostics only" in response.json()["detail"]
    assert "/api/v1/qmt/virtual-strategies/orders" in response.json()["detail"]
    assert fake_client.place_order_calls == []


def test_raw_order_router_requires_explicit_diagnostic_switch(monkeypatch) -> None:
    monkeypatch.setenv("QMT_TRADE_PASSWORD", "secret")
    monkeypatch.setenv("AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS", "1")
    fake_client = FakeQmtClient()

    response = _raw_client(fake_client, monkeypatch).post("/api/v1/qmt/order", json=_raw_payload())

    assert response.status_code == 200
    assert response.json()["success"] is True
    assert response.json()["order_id"] == 10001
    assert "administrator/POC diagnostics only" in response.json()["diagnostic_warning"]
    assert fake_client.place_order_calls == [
        {
            "stock_code": "300604.SZ",
            "order_type": 23,
            "order_volume": 1000,
            "price_type": 5,
            "price": 10.0,
            "strategy_name": "",
            "order_remark": "",
        }
    ]


def test_raw_batch_order_router_requires_explicit_diagnostic_switch(monkeypatch) -> None:
    monkeypatch.setenv("QMT_TRADE_PASSWORD", "secret")
    monkeypatch.setenv("AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS", "1")
    fake_client = FakeQmtClient()
    order_a = _raw_payload()
    order_b = {**_raw_payload(), "stock_code": "300054.SZ", "strategy_name": "manual_b", "order_remark": "remark_b"}

    response = _raw_client(fake_client, monkeypatch).post(
        "/api/v1/qmt/order/batch",
        json={"trade_password": "secret", "orders": [order_a, order_b]},
    )

    assert response.status_code == 200
    assert response.json()["success"] is True
    assert response.json()["succeeded"] == 2
    assert "administrator/POC diagnostics only" in response.json()["diagnostic_warning"]
    assert [call["strategy_name"] for call in fake_client.place_order_calls] == ["", "manual_b"]
    assert [call["order_remark"] for call in fake_client.place_order_calls] == ["", "remark_b"]


def test_managed_submit_records_intent_before_broker_call(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", "1")
    fake_client = FakeQmtClient()
    repo = _repo()
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: fake_client)
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")

    response = TestClient(app).post("/api/v1/qmt/virtual-strategies/orders", json=_payload())

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["broker_called"] is True
    intent_id = body["result"]["intent_id"]
    intent = repo.get_order_intent(intent_id)
    assert intent.strategy_name == "poc_strategy_a"
    assert intent.order_type == BUY_ORDER_TYPE
    assert intent.order_remark == "remark_router"
    assert intent.submit_status == IntentSubmitStatus.ACCEPTED
    assert fake_client.place_order_calls[0]["strategy_name"] == "poc_strategy_a"
    assert fake_client.place_order_calls[0]["order_remark"] == "remark_router"


def test_managed_batch_submit_returns_batch_preflight_contract(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", "1")
    fake_client = FakeQmtClient()
    repo = _repo()
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: fake_client)
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")

    payload = {
        "orders": [
            {**_payload(), "order_remark": "remark_batch_a"},
            {**_payload(), "stock_code": "300054.SZ", "order_remark": "remark_batch_b"},
        ]
    }
    response = TestClient(app).post("/api/v1/qmt/virtual-strategies/orders/batch", json=payload)

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["success"] is True
    assert result["batch_id"].startswith("qmtbatch_")
    assert result["batch_status"] == "SUCCEEDED"
    assert result["preflight_passed"] is True
    assert result["retry_of_batch_id"] is None
    assert result["compensation_actions"] == []
    assert len(fake_client.place_order_calls) == 2
    batch = repo.get_order_batch(result["batch_id"])
    assert batch is not None
    assert batch.batch_status.value == "SUCCEEDED"


def test_managed_batch_submit_preflight_failure_skips_broker(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", "1")
    fake_client = FakeQmtClient()
    repo = _repo()
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: fake_client)
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")

    payload = {"orders": [{**_payload(), "order_remark": "dup"}, {**_payload(), "order_remark": "dup"}]}
    response = TestClient(app).post("/api/v1/qmt/virtual-strategies/orders/batch", json=payload)

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["success"] is False
    assert result["batch_status"] == "PREFLIGHT_FAILED"
    assert result["preflight_passed"] is False
    assert fake_client.place_order_calls == []
    assert "BATCH_DUPLICATE_ORDER_REMARK" in {
        error["code"]
        for item in result["results"]
        for error in item["preflight"]["errors"]
    }
