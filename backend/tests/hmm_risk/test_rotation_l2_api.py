from __future__ import annotations

from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import hmm_risk
from backend.services.hmm_risk.rotation_l2_prediction import REASON_NOT_FOUND, RotationL2PredictionError


class _Repository:
    def overview(self, *, run_id: str):
        return {"run_id": run_id, "sector_count": 131, "available_count": 119}

    def read_date(self, trade_date: date, *, run_id: str):
        if trade_date == date(2026, 1, 1):
            raise RotationL2PredictionError(REASON_NOT_FOUND, "missing date")
        return {"run_id": run_id, "trade_date": trade_date.isoformat(), "rows": [{}] * 131}


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(hmm_risk.router, prefix="/api/v1")
    app.dependency_overrides[hmm_risk.get_rotation_l2_repository] = _Repository
    return TestClient(app)


def test_l2_routes_require_explicit_run_and_return_full_catalog() -> None:
    run_id = "a" * 64
    missing_run = _client().get("/api/v1/hmm-risk/rotation-l2/overview")
    overview = _client().get("/api/v1/hmm-risk/rotation-l2/overview", params={"run_id": run_id})
    detail = _client().get(
        "/api/v1/hmm-risk/rotation-l2",
        params={"run_id": run_id, "trade_date": "2026-01-02"},
    )

    assert missing_run.status_code == 422
    assert overview.status_code == 200
    assert overview.json()["data"]["sector_count"] == 131
    assert detail.status_code == 200
    assert len(detail.json()["data"]["rows"]) == 131


def test_l2_missing_date_is_typed_404() -> None:
    response = _client().get(
        "/api/v1/hmm-risk/rotation-l2",
        params={"run_id": "a" * 64, "trade_date": "2026-01-01"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["reason_code"] == REASON_NOT_FOUND


def test_main_app_registers_l2_routes() -> None:
    from backend.main import create_app

    paths = {route.path for route in create_app().routes}

    assert "/api/v1/hmm-risk/rotation-l2/overview" in paths
    assert "/api/v1/hmm-risk/rotation-l2" in paths
