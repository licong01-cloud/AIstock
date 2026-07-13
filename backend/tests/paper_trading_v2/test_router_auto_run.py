from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import paper_trading_v2
from backend.services.paper_trading_v2.market_data import MinuteDataSource


class _FakeMutationWindowService:
    def require_non_trading_operation_window(self, *, action: str) -> None:
        self.action = action


class _FakePortfolio:
    def __init__(self, **payload: Any) -> None:
        self.payload = payload

    def model_dump(self, *, mode: str = "json") -> dict[str, Any]:
        assert mode == "json"
        return dict(self.payload)


class _FakePortfolioService:
    calls: list[dict[str, Any]] = []

    def create_portfolio(self, **kwargs: Any) -> _FakePortfolio:
        self.calls.append(kwargs)
        return _FakePortfolio(portfolio_id="paper_local_router_guard", **kwargs)


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    _FakePortfolioService.calls = []
    monkeypatch.setattr(paper_trading_v2, "_session_service_for_mutation", lambda: _FakeMutationWindowService())
    monkeypatch.setattr(paper_trading_v2, "PaperTradingV2PortfolioService", _FakePortfolioService)
    app = FastAPI()
    app.include_router(paper_trading_v2.router, prefix="/api/v1")
    return TestClient(app)


def test_minqmt_auto_run_path_p_endpoint_rejects_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(monkeypatch)

    response = client.post(
        "/api/v1/paper-v2/auto-run/miniqmt-portfolios",
        json={
            "package_id": "pkg_unit",
            "portfolio_name": "legacy MiniQMT Path P",
            "initial_cash": 1000000,
            "start_date": "2026-07-03",
            "broker_account_id": "62266303",
            "top_k": 25,
        },
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason_code"] == "MINIQMT_PATH_P_AUTO_RUN_DEPRECATED"
    assert detail["error_code"] == "MINIQMT_PATH_P_AUTO_RUN_DEPRECATED"
    assert detail["context"]["endpoint"] == "/api/v1/paper-v2/auto-run/miniqmt-portfolios"
    assert detail["context"]["broker_backend"] == "minqmt_sim"
    assert detail["context"]["required_path"] == "simulation-runtime Path S release+binding"
    assert "A/event_loop" in detail["message"]
    assert _FakePortfolioService.calls == []


def test_minqmt_broker_backend_on_portfolios_endpoint_rejects_path_p(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(monkeypatch)

    response = client.post(
        "/api/v1/paper-v2/portfolios",
        json={
            "package_id": "pkg_unit",
            "portfolio_name": "manual MiniQMT Path P",
            "initial_cash": 1000000,
            "start_date": "2026-07-03",
            "data_source": MinuteDataSource.MINIQMT_REALTIME.value,
            "broker_backend": "minqmt_sim",
        },
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason_code"] == "MINIQMT_PATH_P_AUTO_RUN_DEPRECATED"
    assert detail["context"]["endpoint"] == "/api/v1/paper-v2/portfolios"
    assert detail["context"]["broker_backend"] == "minqmt_sim"
    assert _FakePortfolioService.calls == []


def test_local_sim_portfolio_creation_still_uses_path_p(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(monkeypatch)

    response = client.post(
        "/api/v1/paper-v2/portfolios",
        json={
            "package_id": "pkg_unit",
            "portfolio_name": "LocalSim Path P",
            "initial_cash": 1000000,
            "start_date": "2026-07-03",
            "data_source": MinuteDataSource.TDX_REALTIME.value,
            "broker_backend": "local_sim",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["portfolio"]["portfolio_id"] == "paper_local_router_guard"
    assert payload["portfolio"]["broker_backend"] == "local_sim"
    assert _FakePortfolioService.calls[0]["broker_backend"] == "local_sim"
