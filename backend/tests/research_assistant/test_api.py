from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import research_assistant
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ASSISTANT_APPROVAL_CONFIRM, LlmCallResult, ResearchAssistantService


class FakeLlmClient:
    def complete(self, **_kwargs: object) -> LlmCallResult:
        return LlmCallResult(
            content="我理解你要创建 QE 10 loop 实验。本轮只生成计划和确认问题，不执行。",
            provider="fake",
            model="fake-primary",
            duration_ms=9,
            usage={"prompt_tokens": 10, "completion_tokens": 8},
        )


def _client(*, seed: bool = True) -> TestClient:
    repository = InMemoryResearchAssistantRepository()
    service = ResearchAssistantService(repository=repository, llm_client=FakeLlmClient())
    if seed:
        service.seed_catalogs()
    app = FastAPI()
    app.include_router(research_assistant.router, prefix="/api/v1")
    app.dependency_overrides[research_assistant.get_research_assistant_service] = lambda: service
    return TestClient(app)


def test_research_assistant_catalog_readiness_api_is_explicit() -> None:
    client = _client(seed=False)

    health = client.get("/api/v1/research-assistant/health").json()["data"]
    assert health["status"] == "catalog_not_ready"
    assert health["catalog_readiness"]["ready"] is False
    assert "prompt_nodes" in health["catalog_readiness"]["missing_catalogs"]

    readiness = client.get("/api/v1/research-assistant/catalogs/readiness").json()["data"]
    assert readiness["operator_action"] == "POST /api/v1/research-assistant/catalogs/seed"

    chat_resp = client.post(
        "/api/v1/research-assistant/chat/turn",
        json={"message": "帮我创建一个 QE 10 loop 实验，先不要执行。", "allow_execute": False},
    )
    assert chat_resp.status_code == 409
    detail = chat_resp.json()["detail"]
    assert detail["code"] == "research_assistant_catalog_not_ready"
    assert detail["readiness"]["ready"] is False

    seed_result = client.post("/api/v1/research-assistant/catalogs/seed").json()["data"]
    assert seed_result["seeded"]["prompt_nodes"] >= 1
    assert client.get("/api/v1/research-assistant/health").json()["data"]["status"] == "ok"
    assert client.post(
        "/api/v1/research-assistant/chat/turn",
        json={"message": "帮我创建一个 QE 10 loop 实验，先不要执行。", "allow_execute": False},
    ).status_code == 200


def test_research_assistant_api_phase1_smoke() -> None:
    client = _client()

    assert client.get("/api/v1/research-assistant/health").json()["data"]["status"] == "ok"
    assert client.get("/api/v1/research-assistant/overview").status_code == 200

    task_resp = client.post("/api/v1/research-assistant/tasks", json={"title": "API task", "idempotency_key": "api-idem"}).json()
    task_id = task_resp["data"]["task_id"]
    assert task_resp["data"]["status"] == "planned"
    assert client.post(f"/api/v1/research-assistant/tasks/{task_id}/events", json={"event_type": "mcp_started", "message": "running"}).status_code == 200
    detail = client.get(f"/api/v1/research-assistant/tasks/{task_id}").json()["data"]
    assert detail["task"]["status"] == "running"
    assert detail["events"]

    memory_resp = client.post(
        "/api/v1/research-assistant/memories",
        json={"memory_type": "core", "subject_key": "api.memory", "title": "API Memory", "content_text": "fact", "source_ref": "api-test"},
    ).json()
    memory_id = memory_resp["data"]["memory_id"]
    mem_approval = client.post(
        "/api/v1/research-assistant/approvals",
        json={
            "approval_type": "memory.approve",
            "plan_digest": "digest-memory-api",
            "summary": "approve api.memory",
            "required_confirmation_text": ASSISTANT_APPROVAL_CONFIRM,
        },
    ).json()["data"]
    assert client.post(f"/api/v1/research-assistant/memories/{memory_id}/status", json={"status": "approved", "approved_by": "pytest"}).status_code == 400
    assert client.post(
        f"/api/v1/research-assistant/memories/{memory_id}/status",
        json={"status": "approved", "approved_by": "pytest", "approval_id": mem_approval["approval_id"], "confirmation_text": ASSISTANT_APPROVAL_CONFIRM},
    ).status_code == 200
    context_resp = client.post("/api/v1/research-assistant/context-packs", json={"task_id": task_id, "token_budget": 4000}).json()
    assert context_resp["data"]["context_pack_id"].startswith("ctx_")

    prompt_nodes = client.get("/api/v1/research-assistant/prompt-nodes", params={"phase": "planning", "search": "QE"}).json()["data"]
    assert prompt_nodes["total"] >= 1
    prompt_bundle = client.post(
        "/api/v1/research-assistant/prompt-bundles",
        json={"user_message": "帮我创建一个 QE 10 loop 实验，先不要执行。", "phase": "planning"},
    ).json()["data"]
    assert "domain.qe_experiment" in [node["prompt_key"] for node in prompt_bundle["node_refs"]]

    chat_resp = client.post(
        "/api/v1/research-assistant/chat/turn",
        json={"message": "帮我创建一个 QE 10 loop 实验，先不要执行。", "allow_execute": False},
    ).json()["data"]
    assert chat_resp["assistant_message"]["content_text"].startswith("我理解你要创建 QE 10 loop")
    assert chat_resp["cards"]["status_rail"][3]["label"] == "等待确认"
    assert chat_resp["cards"]["safety"]["no_materialize_before_confirmation"] is True
    assert client.post(
        "/api/v1/research-assistant/chat/turn",
        json={"message": "确认执行 QE materialize", "allow_execute": True},
    ).status_code == 400

    preflight_resp = client.post(
        "/api/v1/research-assistant/mcp/preflight",
        json={"task_id": task_id, "server_key": "research-assistant", "tool_name": "assistant_create_issue_candidate", "payload_json": {"title": "bug"}},
    ).json()
    assert preflight_resp["data"]["approval_required"] is False
    assert preflight_resp["data"]["passed"] is False
    assert preflight_resp["data"]["failed_checks"][0]["check"] == "input_schema"
    assert "github_formal_issue_blocked" in preflight_resp["data"]["preflight_checks"]

    approval_resp = client.post(
        "/api/v1/research-assistant/approvals",
        json={
            "task_id": task_id,
            "approval_type": "mcp.high_risk",
            "plan_digest": "digest-abcdef",
            "summary": "approve",
            "required_confirmation_text": ASSISTANT_APPROVAL_CONFIRM,
        },
    ).json()
    approval_id = approval_resp["data"]["approval_id"]
    assert client.post(f"/api/v1/research-assistant/approvals/{approval_id}/approve", json={"confirmation_text": "WRONG"}).status_code == 400
    assert client.post(f"/api/v1/research-assistant/approvals/{approval_id}/approve", json={"confirmation_text": ASSISTANT_APPROVAL_CONFIRM}).json()["data"]["status"] == "approved"

    issue_resp = client.post(
        "/api/v1/research-assistant/issue-candidates",
        json={"title": "Candidate", "severity": "P1", "problem_statement": "review first"},
    ).json()
    assert issue_resp["data"]["status"] == "needs_review"
    assert issue_resp["data"]["github_sync_status"] == "not_requested"

    assert client.get("/api/v1/research-assistant/skills").json()["data"]["total"] >= 5
    assert client.get("/api/v1/research-assistant/mcp/tools").json()["data"]["total"] >= 6
    assert client.get("/api/v1/research-assistant/models/profiles").json()["data"]["total"] >= 3
    model_route = client.post("/api/v1/research-assistant/models/route", json={"role": "cheap_worker", "risk_level": "low"}).json()["data"]
    assert model_route["route_status"] == "fallback_selected"
    assert model_route["model_profile"]["status"] == "enabled"
    assert client.post("/api/v1/research-assistant/temp-memories", json={"task_id": task_id, "content_text": "progress"}).status_code == 200
    assert client.get("/api/v1/research-assistant/notifications/summary").json()["data"]["unread"] >= 1
    entity_a = client.post("/api/v1/research-assistant/graph/entities", json={"entity_type": "module", "entity_key": "api.a", "title": "API A", "source_refs": ["doc#a"]}).json()["data"]
    entity_b = client.post("/api/v1/research-assistant/graph/entities", json={"entity_type": "module", "entity_key": "api.b", "title": "API B", "source_refs": ["doc#b"]}).json()["data"]
    rel = client.post(
        "/api/v1/research-assistant/graph/relations",
        json={"source_entity_id": entity_a["entity_id"], "target_entity_id": entity_b["entity_id"], "relation_type": "depends_on", "evidence_refs": ["doc#rel"]},
    ).json()["data"]
    assert client.get(f"/api/v1/research-assistant/graph/relations/{rel['relation_id']}").json()["data"]["relation_type"] == "depends_on"
    path = client.post("/api/v1/research-assistant/graph/evolution-paths", json={"stream_id": "api", "objective": "test", "evidence_refs": ["doc#path"]}).json()["data"]
    assert path["path_id"].startswith("evopath_")

    assert client.post("/api/v1/research-assistant/skills/qe-evolution-diagnostics/disable").json()["data"]["status"] == "blocked"
    assert client.post("/api/v1/research-assistant/skills/qe-evolution-diagnostics/enable").json()["data"]["status"] == "approved"
    skill_event = client.post("/api/v1/research-assistant/skills/usage-events", json={"skill_key": "qe-evolution-diagnostics", "task_id": task_id}).json()["data"]
    assert skill_event["skill_event_id"].startswith("skillev_")

    session = client.post("/api/v1/research-assistant/external-agent/sessions", json={"agent_type": "codex", "agent_name": "api-worker"}).json()["data"]
    external_event = client.post("/api/v1/research-assistant/external-agent/events", json={"session_id": session["session_id"], "event_type": "evidence_written", "evidence_refs": ["doc#e"]}).json()["data"]
    assert external_event["external_event_id"].startswith("extev_")

    trace = client.post("/api/v1/research-assistant/trace-events", json={"task_id": task_id, "event_type": "mcp", "component": "api", "status": "ok"}).json()["data"]
    assert trace["trace_id"].startswith("trace_")
    assert client.get("/api/v1/research-assistant/trace-events", params={"task_id": task_id}).json()["data"]["total"] >= 1

    dry_run = client.post("/api/v1/research-assistant/workbench/dry-run-execute", json={"task_id": task_id, "server_key": "research-assistant", "tool_name": "assistant_create_task", "payload_json": {"title": "x"}}).json()["data"]
    assert dry_run["tool_result"]["executed"] is False
    assert client.get("/api/v1/research-assistant/mcp/tool-events", params={"task_id": task_id}).json()["data"]["total"] >= 1

    capabilities = client.get("/api/v1/research-assistant/capabilities", params={"status": "approved"}).json()["data"]
    assert capabilities["total"] >= 10
    action_task = client.post("/api/v1/research-assistant/tasks", json={"title": "QE action closure"}).json()["data"]
    action = client.post(
        "/api/v1/research-assistant/actions/propose",
        json={
            "task_id": action_task["task_id"],
            "capability_key": "qe.create_experiment_draft",
            "proposal_type": "workflow_pack",
            "title": "生成 QE 草案",
            "summary": "只生成草案，不 materialize/run",
            "input_json": {
                "template_kind": "custom_evo",
                "title": "QE draft",
                "config_json": {
                    "loops": [{"factor_keys": ["alpha001"], "model_id": "lightgbm"}],
                    "stock_pool": "fixed_pit_pool",
                    "backtest_window": {"start": "2023-01-01", "end": "2024-12-31"},
                },
            },
        },
    ).json()["data"]
    assert action["status"] == "proposed"
    assert client.post(f"/api/v1/research-assistant/actions/{action['action_proposal_id']}/confirm", json={"confirmation_text": "CONFIRM_QE_DRAFT"}).json()["data"]["status"] == "confirmed"
    assert client.post(f"/api/v1/research-assistant/actions/{action['action_proposal_id']}/preflight", json={}).json()["data"]["proposal"]["status"] == "preflight_passed"
    executed = client.post(f"/api/v1/research-assistant/actions/{action['action_proposal_id']}/execute", json={}).json()["data"]
    assert executed["executed"] is True
    assert executed["tool_event"]["result_card_json"]["title"] == "QE template 草案已生成"
    action_events = client.get(f"/api/v1/research-assistant/actions/{action['action_proposal_id']}/events").json()["data"]
    assert action_events["mcp_tool_events"]
    assert action_events["trace_events"]

    sync = client.post(f"/api/v1/research-assistant/issue-candidates/{issue_resp['data']['candidate_id']}/github-sync", json={"mode": "formal"}).json()["data"]
    assert sync["github_sync_status"] == "approval_required"
    assert sync["github_sync_json"]["direct_github_create_performed"] is False

    assert client.get("/api/v1/research-assistant/validation-discovery/summary").status_code == 200


def test_research_assistant_api_errors_are_explicit() -> None:
    client = _client()

    assert client.get("/api/v1/research-assistant/tasks/rat_missing").status_code == 404
    assert client.post("/api/v1/research-assistant/temp-memories", json={"content_text": "missing scope"}).status_code == 400
