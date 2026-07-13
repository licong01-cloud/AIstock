from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import quantevolver_evolution as router_module
from backend.services.quantevolver.qe_resource_phase_service import (
    AUTH_FAILED_REASON,
    QEResourcePhaseError,
)


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router_module.router, prefix="/api/v1")
    return TestClient(app)


def _payload() -> dict:
    return {
        "session_id": "qers_1",
        "source_run_key": "qe_task_L1",
        "task_id": "qe_task",
        "loop_id": "Loop1",
        "loop_index": 1,
        "node_id": "wsl2-5080",
        "sequence_no": 1,
        "phase": "bootstrap",
        "phase_status": "completed",
        "sample_count": 1,
    }


def test_resource_phase_webhook_requires_scoped_token():
    response = _client().post("/api/v1/quantevolver/evolution/webhook/loop-resource-phase", json=_payload())
    assert response.status_code == 403
    assert response.json()["detail"]["reason_code"] == AUTH_FAILED_REASON


def test_resource_phase_webhook_passes_structured_payload(monkeypatch):
    captured = {}

    class FakeService:
        def ingest_event(self, *, token, payload):  # type: ignore[no-untyped-def]
            captured.update({"token": token, "payload": payload})
            return {"status": "accepted", "phase": payload["phase"]}

    monkeypatch.setattr(router_module, "QEResourcePhaseService", lambda: FakeService())
    response = _client().post(
        "/api/v1/quantevolver/evolution/webhook/loop-resource-phase",
        json=_payload(),
        headers={"X-QE-Resource-Token": "scoped-token"},
    )

    assert response.status_code == 200
    assert response.json()["data"]["phase"] == "bootstrap"
    assert captured["token"] == "scoped-token"
    assert captured["payload"]["sequence_no"] == 1


def test_resource_phase_webhook_maps_auth_failure_to_403(monkeypatch):
    class FakeService:
        def ingest_event(self, **_kwargs):  # type: ignore[no-untyped-def]
            raise QEResourcePhaseError(AUTH_FAILED_REASON, "bad token")

    monkeypatch.setattr(router_module, "QEResourcePhaseService", lambda: FakeService())
    response = _client().post(
        "/api/v1/quantevolver/evolution/webhook/loop-resource-phase",
        json=_payload(),
        headers={"X-QE-Resource-Token": "wrong"},
    )

    assert response.status_code == 403
    assert response.json()["detail"]["reason_code"] == AUTH_FAILED_REASON
