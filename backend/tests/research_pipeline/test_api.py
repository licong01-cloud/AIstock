"""API contract tests for the Research Pipeline router."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import research_pipeline
from backend.services.research_pipeline import (
    RESEARCH_PROMOTE_CONFIRM,
    RESEARCH_RETRY_STAGE_CONFIRM,
    RESEARCH_RUN_STAGE_CONFIRM,
)
from backend.services.research_pipeline.service import ResearchPipelineNotFoundError


class FakeResearchPipelineService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))

    def create_experiment(self, **kwargs: Any) -> dict[str, Any]:
        self._record("create_experiment", **kwargs)
        return {"experiment_id": "exp_1", "status": "draft", **kwargs}

    def list_experiments(self, **kwargs: Any) -> list[dict[str, Any]]:
        self._record("list_experiments", **kwargs)
        return [{"experiment_id": "exp_1", "status": "draft", "pipeline_type": "hmm_research"}]

    def get_experiment(self, experiment_id: str) -> dict[str, Any]:
        self._record("get_experiment", experiment_id)
        if experiment_id == "missing":
            raise ResearchPipelineNotFoundError("experiment not found: missing")
        return {"experiment_id": experiment_id, "status": "draft"}

    def run_stage(self, experiment_id: str, stage_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("run_stage", experiment_id, stage_name, payload)
        return {"stage": {"stage_name": stage_name}, "attempt": {"attempt_no": 1}}

    def retry_stage(self, experiment_id: str, stage_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("retry_stage", experiment_id, stage_name, payload)
        return {"stage": {"stage_name": stage_name}, "attempt": {"attempt_no": 2}}

    def get_stage_result(self, experiment_id: str, stage_name: str) -> dict[str, Any]:
        self._record("get_stage_result", experiment_id, stage_name)
        if stage_name == "missing_stage":
            raise ResearchPipelineNotFoundError(f"stage not found: {experiment_id}/{stage_name}")
        return {"stage": {"stage_name": stage_name}, "attempts": []}

    def compare_baseline(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("compare_baseline", experiment_id, payload)
        return {"experiment_id": experiment_id, "verdict": payload["verdict"]}

    def list_artifact_refs(self, experiment_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        self._record("list_artifact_refs", experiment_id, **kwargs)
        return [{"experiment_id": experiment_id, "domain_type": kwargs.get("domain_type") or "model"}]

    def get_pipeline_types(self) -> dict[str, Any]:
        self._record("get_pipeline_types")
        return {"hmm_research": {"stages": ["artifact_gen"]}}

    def create_issue(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("create_issue", payload)
        return {"external_issue_created": False, "issue_request": payload}

    def promote(self, experiment_id: str, *, issue_url: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("promote", experiment_id, issue_url=issue_url, payload=payload)
        return {"experiment": {"experiment_id": experiment_id, "status": "promotion_requested"}}

    def reject(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("reject", experiment_id, payload)
        return {"experiment": {"experiment_id": experiment_id, "status": "rejected"}}


@pytest.fixture()
def fake_service() -> FakeResearchPipelineService:
    return FakeResearchPipelineService()


@pytest.fixture()
def client(fake_service: FakeResearchPipelineService) -> TestClient:
    app = FastAPI()
    app.include_router(research_pipeline.router, prefix="/api/v1")
    app.dependency_overrides[research_pipeline.get_research_pipeline_service] = lambda: fake_service
    return TestClient(app)


def assert_success_envelope(response: Any) -> Any:
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert "data" in payload
    return payload["data"]


def test_health_list_get_create_return_success_envelopes(
    client: TestClient,
    fake_service: FakeResearchPipelineService,
) -> None:
    health = assert_success_envelope(client.get("/api/v1/research-pipeline/health"))
    assert health == {"service": "research-pipeline", "status": "ok"}

    listed = assert_success_envelope(
        client.get(
            "/api/v1/research-pipeline/experiments",
            params={"status": "draft", "pipeline_type": "hmm_research", "search": "gate", "limit": 7, "offset": 3},
        )
    )
    assert listed == [{"experiment_id": "exp_1", "status": "draft", "pipeline_type": "hmm_research"}]
    assert fake_service.calls[-1] == (
        "list_experiments",
        (),
        {"status": "draft", "pipeline_type": "hmm_research", "search": "gate", "limit": 7, "offset": 3},
    )

    experiment = assert_success_envelope(client.get("/api/v1/research-pipeline/experiments/exp_1"))
    assert experiment == {"experiment_id": "exp_1", "status": "draft"}

    created = assert_success_envelope(
        client.post(
            "/api/v1/research-pipeline/experiments",
            json={"pipeline_type": "hmm_research", "title": "HMM gate", "created_by": "pytest"},
        )
    )
    assert created["experiment_id"] == "exp_1"
    assert fake_service.calls[-1][0] == "create_experiment"
    assert fake_service.calls[-1][2]["title"] == "HMM gate"


@pytest.mark.parametrize(
    ("path", "expected_detail"),
    [
        ("/api/v1/research-pipeline/experiments/missing", "experiment not found: missing"),
        (
            "/api/v1/research-pipeline/experiments/exp_1/stages/missing_stage",
            "stage not found: exp_1/missing_stage",
        ),
    ],
)
def test_missing_experiment_or_stage_maps_to_404(client: TestClient, path: str, expected_detail: str) -> None:
    response = client.get(path)

    assert response.status_code == 404
    assert response.json() == {"detail": expected_detail}


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("post", "/api/v1/research-pipeline/experiments", {"json": {"pipeline_type": "hmm_research"}}),
        ("get", "/api/v1/research-pipeline/experiments", {"params": {"limit": 0}}),
        ("post", "/api/v1/research-pipeline/experiments/exp_1/promote", {"json": {"confirm": RESEARCH_PROMOTE_CONFIRM}}),
    ],
)
def test_request_validation_returns_422(client: TestClient, method: str, path: str, kwargs: dict[str, Any]) -> None:
    response = getattr(client, method)(path, **kwargs)

    assert response.status_code == 422
    assert response.json()["detail"]


@pytest.mark.parametrize(
    ("path", "body", "expected_confirm"),
    [
        (
            "/api/v1/research-pipeline/experiments/exp_1/stages/offline_validation/run",
            {"payload": {"reason": "smoke"}, "confirm": "WRONG"},
            RESEARCH_RUN_STAGE_CONFIRM,
        ),
        (
            "/api/v1/research-pipeline/experiments/exp_1/stages/qe_shadow/retry",
            {"payload": {"reason": "retry"}, "confirm": "WRONG"},
            RESEARCH_RETRY_STAGE_CONFIRM,
        ),
        (
            "/api/v1/research-pipeline/experiments/exp_1/promote",
            {"issue_url": "https://github.com/example/repo/issues/1", "payload": {"target": "candidate"}, "confirm": "WRONG"},
            RESEARCH_PROMOTE_CONFIRM,
        ),
    ],
)
def test_confirm_guard_rejects_run_retry_promote_before_service_call(
    client: TestClient,
    fake_service: FakeResearchPipelineService,
    path: str,
    body: dict[str, Any],
    expected_confirm: str,
) -> None:
    response = client.post(path, json=body)

    assert response.status_code == 400
    assert response.json() == {"detail": f"confirm must equal {expected_confirm}"}
    assert fake_service.calls == []


def test_phase2_mcp_route_paths_are_accepted_by_router(client: TestClient) -> None:
    calls = [
        client.post(
            "/api/v1/research-pipeline/experiments",
            json={"pipeline_type": "hmm_research", "name": "HMM gate"},
        ),
        client.get("/api/v1/research-pipeline/experiments"),
        client.get("/api/v1/research-pipeline/experiments/exp_1"),
        client.post(
            "/api/v1/research-pipeline/experiments/exp_1/stages/offline_validation/run",
            json={"reason": "dogfood", "confirm": RESEARCH_RUN_STAGE_CONFIRM},
        ),
        client.post(
            "/api/v1/research-pipeline/experiments/exp_1/stages/qe_shadow/retry",
            json={"reason": "transient", "confirm": RESEARCH_RETRY_STAGE_CONFIRM},
        ),
        client.get("/api/v1/research-pipeline/experiments/exp_1/stages/qe_shadow"),
        client.post("/api/v1/research-pipeline/experiments/exp_1/compare", json={"baseline": "v25_1", "verdict": "pass"}),
        client.get("/api/v1/research-pipeline/experiments/exp_1/artifact-refs"),
        client.get("/api/v1/research-pipeline/pipeline-types"),
        client.post("/api/v1/research-pipeline/issues", json={"title": "Research blocked"}),
        client.post(
            "/api/v1/research-pipeline/experiments/exp_1/promote",
            json={"issue_url": "https://github.com/example/repo/issues/1", "target": "candidate", "confirm": RESEARCH_PROMOTE_CONFIRM},
        ),
        client.post("/api/v1/research-pipeline/experiments/exp_1/reject", json={"reason": "underperformed"}),
    ]

    assert [response.status_code for response in calls] == [200] * len(calls)
    for response in calls:
        assert response.json()["status"] == "success"
