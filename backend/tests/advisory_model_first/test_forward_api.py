from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.routers import advisory as advisory_router


class _ForwardService:
    def status(self):
        return {"schema_version": "advisory_forward_status_v1", "run_count": 1}

    def run_once(self):
        return {
            "schema_version": "advisory_forward_run_once_v1",
            "publication_due": True,
            "results": [
                {
                    "program_id": "advp_test",
                    "forward_run_id": "advfwd_test",
                    "status": "PUBLISHED",
                    "model_status": "UNAVAILABLE",
                }
            ],
        }

    def list_runs(self, *, program_id: str | None = None, limit: int = 100):
        assert (program_id, limit) == ("advp_test", 20)
        return [{"forward_run_id": "advfwd_test", "publication_status": "PUBLISHED"}]

    def detail(self, forward_run_id: str):
        assert forward_run_id == "advfwd_test"
        return {
            "forward_run": {"forward_run_id": forward_run_id, "publication_status": "PUBLISHED"},
            "model_observation": {"status": "UNAVAILABLE"},
            "model_outcome": None,
        }

    def model_metrics(self, program_id: str):
        assert program_id == "advp_test"
        return {
            "schema_version": "advisory_forward_model_metrics_response_v1",
            "program_id": program_id,
            "status": "EVIDENCE_IMMATURE",
            "observation_count": 1,
            "due_observation_count": 0,
            "evaluation": None,
        }


def test_forward_api_exposes_status_run_once_history_and_detail(monkeypatch) -> None:
    service = _ForwardService()
    app.dependency_overrides[advisory_router.get_advisory_forward_service] = lambda: service
    monkeypatch.setattr(
        advisory_router.advisory_forward_scheduler,
        "status",
        lambda: {"configured_enabled": False, "running": False},
    )
    monkeypatch.setattr(advisory_router.advisory_forward_scheduler, "run_once", service.run_once)
    client = TestClient(app)
    try:
        status = client.get("/api/v1/advisory/forward/status")
        run = client.post("/api/v1/advisory/forward/run-once")
        history = client.get("/api/v1/advisory/programs/advp_test/forward-runs", params={"limit": 20})
        detail = client.get("/api/v1/advisory/forward-runs/advfwd_test")
        metrics = client.get("/api/v1/advisory/programs/advp_test/forward-model-metrics")
    finally:
        app.dependency_overrides.pop(advisory_router.get_advisory_forward_service, None)

    assert status.status_code == 200
    assert status.json()["scheduler"] == {"configured_enabled": False, "running": False}
    assert run.status_code == 200
    assert run.json()["results"][0]["model_status"] == "UNAVAILABLE"
    assert history.status_code == 200
    assert history.json()["forward_runs"][0]["publication_status"] == "PUBLISHED"
    assert detail.status_code == 200
    assert detail.json()["model_observation"]["status"] == "UNAVAILABLE"
    assert metrics.status_code == 200
    assert metrics.json()["status"] == "EVIDENCE_IMMATURE"
