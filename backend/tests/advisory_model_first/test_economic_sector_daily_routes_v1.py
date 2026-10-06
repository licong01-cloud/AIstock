"""Actual isolated ASGI GET; no production startup or database/service operations."""

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import psycopg2
import pytest

from backend.routers.advisory import get_advisory_sector_daily_service, router
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def client_for(reader):
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_advisory_sector_daily_service] = lambda: SimpleNamespace(read_day=reader)
    return TestClient(app)


def test_get_passes_real_parameters_without_mutation_routes():
    seen = []

    def read(**kwargs):
        seen.append(kwargs)
        return {
            "schema_version": "economic_sector_daily_service_v1",
            "status": "NOT_CONFIGURED",
            "database_written": False,
        }

    response = client_for(read).get(
        "/api/v1/advisory/programs/p/sector-entry-price?target_trade_date=2026-08-28&list_version_id=l"
    )
    assert response.status_code == 200 and response.json()["status"] == "NOT_CONFIGURED"
    assert (
        str(seen[0]["target_date"]) == "2026-08-28"
        and seen[0]["list_version_id"] == "l"
        and seen[0]["program_id"] == "p"
    )


@pytest.mark.parametrize(
    "reason,status",
    [
        ("ADVISORY_SECTOR_FROZEN_LIST_NOT_READY", "FROZEN_LIST_NOT_READY"),
        ("ADVISORY_SECTOR_MODEL_INPUT_INCOMPATIBLE", "MODEL_INPUT_INCOMPATIBLE"),
        ("ADVISORY_SECTOR_FROZEN_LIST_INVALID", "INPUT_UNAVAILABLE"),
    ],
)
def test_input_errors_are_conflict_not_empty_success(reason, status):
    def read(**_):
        raise AdvisoryModelFirstError("private diagnostic must not enter HTTP output", reason_code=reason)

    response = client_for(read).get("/api/v1/advisory/programs/p/sector-entry-price")
    assert response.status_code == 409 and response.json()["detail"]["status"] == status
    assert "private diagnostic" not in response.text


def test_database_unavailable_is_503_not_a_profitability_decision():
    def read(**_):
        raise psycopg2.OperationalError("connection diagnostic must not be displayed")

    response = client_for(read).get("/api/v1/advisory/programs/p/sector-entry-price")
    assert response.status_code == 503 and response.json()["detail"]["deployable"] is False
    assert "connection diagnostic" not in response.text


def test_bad_date_does_not_reach_consumer():
    def forbidden(**_):
        pytest.fail("invalid HTTP date must not read consumer inputs")

    assert (
        client_for(forbidden).get("/api/v1/advisory/programs/p/sector-entry-price?target_trade_date=bad").status_code
        == 422
    )
