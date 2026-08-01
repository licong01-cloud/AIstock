from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import health as subject


def test_health_contract_reports_configured_application_identity(monkeypatch) -> None:
    monkeypatch.setattr(subject, "get_app_settings", lambda: SimpleNamespace(app_name="AIstock-test"))
    app = FastAPI()
    app.include_router(subject.router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "app": "AIstock-test"}
