from __future__ import annotations

from backend.services.research_assistant.models import McpPreflightRequest, TaskCreate
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    svc.seed_catalogs()
    return svc


def test_mcp_preflight_event_records_profile_approval_and_evidence_audit() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="Phase 5 MCP audit"))

    result = svc.preflight_mcp_tool(
        McpPreflightRequest(
            server_key="aistock-external-research",
            tool_name="external_research_save_evidence",
            task_id=task["task_id"],
            payload_json={},
            idempotency_key="phase5-audit-test",
        )
    )

    assert result["approval_required"] is True
    assert result["audit"]["profile"] == "external_research"
    assert result["audit"]["tool_name"] == "external_research_save_evidence"
    assert result["audit"]["preflight"]["status"] == "approval_required"
    assert result["audit"]["approval"]["required"] is True
    assert "manifest:external_research_save_evidence" in result["audit"]["evidence_refs"]

    event = svc.repository.get_record("mcp_tool_events", result["tool_event_id"])
    assert event is not None
    assert event["status"] == "approval_required"
    assert event["response_json"]["audit"]["profile"] == "external_research"
    assert event["result_card_json"]["approval_required"] is True
    assert event["result_card_json"]["status"] == "approval_required"
    assert event["artifact_refs"] == result["evidence_refs"]

    task_detail = svc.get_task(task["task_id"])
    audit_events = [item for item in task_detail["events"] if item["event_type"] == "approval_required"]
    assert audit_events
    payload = audit_events[-1]["payload_json"]
    assert payload["mcp_preflight_audit"]["profile"] == "external_research"
    assert payload["mcp_preflight_audit"]["approval"]["required"] is True
    assert payload["mcp_preflight_audit"]["evidence_refs"] == result["evidence_refs"]


def test_readonly_preflight_event_records_passed_audit_without_approval() -> None:
    svc = _service()
    result = svc.preflight_mcp_tool(
        McpPreflightRequest(
            server_key="aistock-qe",
            tool_name="qe_archive_query_run_leaderboard",
            payload_json={"limit": 1},
        )
    )

    assert result["passed"] is True
    assert result["approval_required"] is False
    assert result["audit"]["preflight"]["status"] == "passed"
    assert result["audit"]["approval"]["required"] is False
    assert result["audit"]["profile"] == "qe"

    event = svc.repository.get_record("mcp_tool_events", result["tool_event_id"])
    assert event is not None
    assert event["result_card_json"]["status"] == "passed"
    assert event["result_card_json"]["approval_required"] is False
    assert "manifest:qe_archive_query_run_leaderboard" in event["artifact_refs"]
