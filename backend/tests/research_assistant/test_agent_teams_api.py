from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import research_assistant
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService


class FakeLlmClient:
    def complete(self, **_kwargs: object) -> LlmCallResult:
        return LlmCallResult(content="summary", provider="fake", model="fake", duration_ms=1, usage={})


def _client() -> tuple[TestClient, ResearchAssistantService]:
    repository = InMemoryResearchAssistantRepository()
    service = ResearchAssistantService(repository=repository, llm_client=FakeLlmClient())
    service.seed_catalogs()
    app = FastAPI()
    app.include_router(research_assistant.router, prefix="/api/v1")
    app.dependency_overrides[research_assistant.get_research_assistant_service] = lambda: service
    return TestClient(app), service


def test_agent_runs_facade_lists_and_reads_existing_agent_team_runs() -> None:
    client, service = _client()
    task = service.create_task({"title": "Phase7 Agent Teams facade"})
    service.repository.create_record(
        "agent_runs",
        {
            "agent_run_id": "agent_orchestrator_phase7",
            "parent_task_id": task["task_id"],
            "agent_key": "orchestrator",
            "role": "orchestrator",
            "status": "succeeded",
            "input_json": {"objective": "600584 是否值得买入", "context_pack_id": "ctx_phase7"},
            "result_json": {
                "summary": "orchestrator reduced two workers",
                "reduce_summary": "evidence supported but action remains blocked",
                "evidence_refs": [{"source": "mcp://stock/fundamental", "as_of": "2026-06-02", "provenance": {"tool": "stock_fundamental"}}],
            },
            "model_profile_id": "model_primary",
            "trace_id": "trace_orchestrator_phase7",
        },
    )
    service.repository.create_record(
        "agent_runs",
        {
            "agent_run_id": "agent_worker_phase7",
            "parent_task_id": task["task_id"],
            "agent_key": "stock_evidence_worker",
            "role": "worker",
            "status": "approval_required",
            "input_json": {"context_pack_id": "ctx_phase7", "allowed_tools": ["stock_readonly"]},
            "result_json": {
                "summary": "high risk buy/sell judgement blocked",
                "blocker_cards": [
                    {
                        "blocker_id": "blk_phase7_risk",
                        "status": "approval_required",
                        "reason": "investment advice requires explicit user confirmation",
                        "next_step": "show evidence and wait",
                        "provenance": {"policy": "high_risk_gate"},
                        "as_of": "2026-06-02",
                    }
                ],
            },
            "model_profile_id": "model_worker",
            "trace_id": "trace_worker_phase7",
        },
    )

    listed = client.get("/api/v1/research-assistant/agent-runs", params={"parent_task_id": task["task_id"]}).json()["data"]

    assert listed["total"] == 2
    assert {item["agent_run_id"] for item in listed["items"]} == {"agent_orchestrator_phase7", "agent_worker_phase7"}
    assert listed["items"][0]["input_json"]["context_pack_id"] == "ctx_phase7"
    assert any(item["status"] == "approval_required" for item in listed["items"])

    detail = client.get("/api/v1/research-assistant/agent-runs/agent_worker_phase7").json()["data"]

    assert detail["role"] == "worker"
    assert detail["result_json"]["blocker_cards"][0]["status"] == "approval_required"


def test_agent_run_facade_missing_run_is_explicit_404() -> None:
    client, _service = _client()

    response = client.get("/api/v1/research-assistant/agent-runs/missing-agent-run")

    assert response.status_code == 404
    assert "missing-agent-run" in response.json()["detail"]
