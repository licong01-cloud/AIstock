from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import hmm_risk
from backend.services.hmm_risk.rotation_l1_prediction import REASON_NOT_FOUND, RotationL1PredictionError


class _Repository:
    def overview(self, *, model_hash=None):
        return {"model_hash": model_hash or "a" * 64, "sector_count": 31}

    def read_date(self, trade_date: date, *, model_hash=None):
        if trade_date == date(2026, 1, 1):
            raise RotationL1PredictionError(REASON_NOT_FOUND, "missing date")
        return {"model_hash": model_hash or "a" * 64, "trade_date": trade_date.isoformat(), "rows": [{}] * 31}


def _client():
    app = FastAPI()
    app.include_router(hmm_risk.router, prefix="/api/v1")
    app.dependency_overrides[hmm_risk.get_rotation_l1_repository] = _Repository
    return TestClient(app)


def test_hmm_risk_read_apis_return_model_bound_31_row_payload() -> None:
    client = _client()
    overview = client.get("/api/v1/hmm-risk/overview", params={"model_hash": "a" * 64})
    detail = client.get(
        "/api/v1/hmm-risk/rotation-l1",
        params={"trade_date": "2026-01-02", "model_hash": "a" * 64},
    )

    assert overview.status_code == 200
    assert overview.json()["data"]["sector_count"] == 31
    assert detail.status_code == 200
    assert len(detail.json()["data"]["rows"]) == 31


def test_hmm_risk_rotation_date_not_found_is_typed_404() -> None:
    response = _client().get("/api/v1/hmm-risk/rotation-l1", params={"trade_date": "2026-01-01"})

    assert response.status_code == 404
    assert response.json()["detail"]["reason_code"] == REASON_NOT_FOUND


def test_main_app_registers_both_hmm_risk_read_routes() -> None:
    from backend.main import create_app

    paths = {route.path for route in create_app().routes}

    assert "/api/v1/hmm-risk/overview" in paths
    assert "/api/v1/hmm-risk/rotation-l1" in paths
