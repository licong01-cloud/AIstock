from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import qmt, qmt_strategy_ledger
from backend.services.qmt_strategy_ledger.models import BUY_ORDER_TYPE, IntentSubmitStatus, VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"


def _repo(*, mode: str = "SIM") -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode=mode,
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    return repo


class _UnexpectedLiveApprovalService:
    def require_live_approval(self, **_kwargs):
        raise AssertionError("SIM managed order path must not call live approval service")


def _client(
    repo: InMemoryQmtStrategyLedgerRepository,
    *,
    live_approval_service_factory=lambda: _UnexpectedLiveApprovalService(),
) -> TestClient:
    qmt_strategy_ledger.configure_dependencies(
        repository_factory=lambda: repo,
        client_factory=lambda: object(),
        live_approval_service_factory=live_approval_service_factory,
    )
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    return TestClient(app)


class FakeQmtClient:
    def __init__(self) -> None:
        self.place_order_calls: list[dict] = []
        self.cancel_order_calls: list[str] = []
        self.last_order_diagnostic: dict | None = None
        self.last_cancel_diagnostic: dict | None = None

    def place_order(self, **kwargs) -> tuple[int, str]:
        self.place_order_calls.append(kwargs)
        self.last_order_diagnostic = {
            "schema_version": "qmt_order_submit_diagnostic_v1",
            "accepted": True,
            "raw_return_code": 10001,
            "classification": "accepted",
        }
        return 10001, "accepted"

    def get_last_order_diagnostic(self) -> dict | None:
        return dict(self.last_order_diagnostic) if self.last_order_diagnostic else None

    def cancel_order(self, order_id: str) -> tuple[bool, str]:
        self.cancel_order_calls.append(str(order_id))
        self.last_cancel_diagnostic = {
            "schema_version": "qmt_cancel_diagnostic_v1",
            "operation": "cancel_order_stock",
            "cancel_method": "order_id",
            "accepted": False,
            "raw_return_code": -1,
            "order_id": str(order_id),
            "classification": "xtquant_nonzero_return",
        }
        return False, "cancel failed: raw_return_code=-1"

    def get_last_cancel_diagnostic(self) -> dict | None:
        return dict(self.last_cancel_diagnostic) if self.last_cancel_diagnostic else None


class FakeLiveApprovalService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def require_live_approval(self, **kwargs):
        self.calls.append(kwargs)
        return object()


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


def test_submit_router_blocks_live_mode_without_approval_metadata(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", "1")
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_LIVE_MANAGED_ORDERS", "1")
    payload = _payload()
    payload["mode"] = "LIVE"
    fake_service = FakeLiveApprovalService()

    response = _client(
        _repo(mode="LIVE"),
        live_approval_service_factory=lambda: fake_service,
    ).post("/api/v1/qmt/virtual-strategies/orders", json=payload)

    assert response.status_code == 403
    detail = response.json()["detail"]
    assert detail["code"] == "MINIQMT_LIVE_APPROVAL_REQUIRED"
    assert detail["context"] == {
        "package_id_present": False,
        "live_approval_id_present": False,
        "runtime_release_sha256_present": False,
    }
    assert fake_service.calls == []


def test_submit_router_allows_live_mode_only_after_live_approval_gate(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", "1")
    monkeypatch.setenv("AISTOCK_ALLOW_MINIQMT_LIVE_MANAGED_ORDERS", "1")
    fake_client = FakeQmtClient()
    fake_service = FakeLiveApprovalService()
    repo = _repo(mode="LIVE")
    qmt_strategy_ledger.configure_dependencies(
        repository_factory=lambda: repo,
        client_factory=lambda: fake_client,
        live_approval_service_factory=lambda: fake_service,
    )
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    payload = {
        **_payload(),
        "mode": "LIVE",
        "package_id": "pkg_live_unit_test",
        "metadata": {
            "live_approval_id": "liveappr_unit_test",
            "runtime_release_sha256": "sha256:runtime-release-unit-test",
        },
    }

    response = TestClient(app).post("/api/v1/qmt/virtual-strategies/orders", json=payload)

    assert response.status_code == 200
    assert response.json()["result"]["broker_called"] is True
    assert fake_service.calls == [
        {
            "package_id": "pkg_live_unit_test",
            "approval_id": "liveappr_unit_test",
            "runtime_release_sha256": "sha256:runtime-release-unit-test",
            "target_broker_backend": "minqmt_live",
        }
    ]
    assert fake_client.place_order_calls[0]["strategy_name"] == "poc_strategy_a"


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
    assert response.json()["diagnostic"]["raw_return_code"] == 10001
    assert response.json()["diagnostic"]["classification"] == "accepted"
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


def test_raw_cancel_router_returns_broker_diagnostic(monkeypatch) -> None:
    monkeypatch.setenv("QMT_TRADE_PASSWORD", "secret")
    fake_client = FakeQmtClient()

    response = _raw_client(fake_client, monkeypatch).post(
        "/api/v1/qmt/cancel",
        json={"trade_password": "secret", "order_id": "1090519216"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert "raw_return_code=-1" in body["message"]
    assert body["diagnostic"]["schema_version"] == "qmt_cancel_diagnostic_v1"
    assert body["diagnostic"]["cancel_method"] == "order_id"
    assert body["diagnostic"]["order_id"] == "1090519216"
    assert body["diagnostic"]["raw_return_code"] == -1
    assert body["diagnostic"]["classification"] == "xtquant_nonzero_return"
    assert fake_client.cancel_order_calls == ["1090519216"]


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
