from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.routers import hmm_risk
from backend.services.hmm_risk.risk_l2_prediction import (
    REASON_CONFLICT,
    REASON_NOT_FOUND,
    REASON_READBACK,
    RiskL2PredictionError,
)


@pytest.mark.parametrize("reason,status", [(REASON_NOT_FOUND, 404), (REASON_CONFLICT, 409), (REASON_READBACK, 500)])
def test_risk_l2_typed_http_failure_not_empty_success(reason, status):
    class Repository:
        def overview(self, **_kw):
            raise RiskL2PredictionError(reason, "explicit failure")

        def read_date(self, *_a, **_kw):
            raise RiskL2PredictionError(reason, "explicit failure")

    app = FastAPI()
    app.include_router(hmm_risk.router, prefix="/api/v1")
    app.dependency_overrides[hmm_risk.get_risk_l2_repository] = Repository
    with TestClient(app) as client:
        for suffix in ("/overview?", "?trade_date=2026-03-31&"):
            response = client.get("/api/v1/hmm-risk/risk-l2" + suffix + "run_id=" + "a" * 64)
            assert response.status_code == status and response.json()["detail"]["reason_code"] == reason


def test_risk_l2_explicit_input_and_full_day_route():
    class Repository:
        def read_date(self, day, *, run_id):
            assert day == date(2026, 3, 31) and run_id == "a" * 64
            return {"rows": [{"sector_code": str(i)} for i in range(131)]}

    app = FastAPI()
    app.include_router(hmm_risk.router, prefix="/api/v1")
    app.dependency_overrides[hmm_risk.get_risk_l2_repository] = Repository
    with TestClient(app) as client:
        assert client.get("/api/v1/hmm-risk/risk-l2").status_code == 422
        assert client.get("/api/v1/hmm-risk/risk-l2/overview?run_id=" + "x" * 64).status_code == 422
        assert client.get("/api/v1/hmm-risk/risk-l2?trade_date=invalid&run_id=" + "a" * 64).status_code == 422
        assert (
            len(client.get("/api/v1/hmm-risk/risk-l2?trade_date=2026-03-31&run_id=" + "a" * 64).json()["data"]["rows"])
            == 131
        )
