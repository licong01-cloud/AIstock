from __future__ import annotations

from typing import Any, Mapping

from backend.services.research_assistant.models import ContextPackBuildRequest, TaskCreate, TaskEventCreate
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


class SpyRepository(InMemoryResearchAssistantRepository):
    def __init__(self) -> None:
        super().__init__()
        self.write_calls: list[tuple[str, str]] = []

    def create_record(self, kind: str, row: Mapping[str, Any]) -> dict[str, Any]:
        self.write_calls.append(("create_record", kind))
        return super().create_record(kind, row)

    def update_record(self, kind: str, record_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        self.write_calls.append(("update_record", kind))
        return super().update_record(kind, record_id, updates)


def _service(repo: SpyRepository | None = None) -> ResearchAssistantService:
    service = ResearchAssistantService(repository=repo or SpyRepository())
    service.seed_catalogs()
    if isinstance(service.repository, SpyRepository):
        service.repository.write_calls.clear()
    return service


def _assert_external_safe(*values: str) -> None:
    forbidden = ("chain of thought", "reasoning chain", "\u601d\u7ef4\u94fe", "\u63a8\u7406\u94fe")
    text = "\n".join(values).lower()
    for token in forbidden:
        assert token.lower() not in text


def test_failed_task_event_generates_reflection_card_memory_and_l1_recall() -> None:
    repo = SpyRepository()
    service = _service(repo)
    task = service.create_task(
        TaskCreate(
            title="Reflection failure loop",
            input_json={"user_message": "reflection failure evidence"},
        )
    )
    repo.write_calls.clear()

    event = service.add_task_event(
        task["task_id"],
        TaskEventCreate(
            event_type="mcp_failed",
            severity="error",
            message="MCP failed after evidence source returned timeout; do not reveal chain of thought",
            payload_json={"reason_code": "mcp_timeout"},
            evidence_refs=["mcp://timeout"],
        ),
    )

    cards = service.list_records("reflection_cards")["items"]
    memories = service.list_records("memory_items", filters={"source_type": "reflection_card"})["items"]
    assert len(cards) == 1
    assert len(memories) == 1
    card = cards[0]
    memory = memories[0]
    assert card["task_id"] == task["task_id"]
    assert card["trigger"] == "failure"
    assert card["memory_ref"] == memory["memory_id"]
    assert card["structured_json"]["source_event_id"] == event["event_id"]
    assert card["structured_json"]["reason_codes"] == ["reflection_card_failure"]
    assert card["structured_json"]["source_refs"] == [
        f"research_agent_tasks:{task['task_id']}",
        f"agent_task_events:{event['event_id']}",
    ]
    assert card["structured_json"]["safety"] == {
        "external_reasoning_hidden": True,
        "prompt_or_strategy_changed": False,
        "action_proposals_created": False,
        "next_strategy_is_proposal_only": True,
    }
    assert memory["memory_type"] == "episodic"
    assert memory["scope"] == "personal"
    assert memory["tree_path"].startswith("personal.episodic.reflection.")
    assert memory["approval_status"] == "approved"
    assert memory["risk_level"] == "low"
    assert memory["auto_created"] is True
    assert memory["source_type"] == "reflection_card"
    assert memory["provenance_json"]["card_id"] == card["card_id"]
    assert memory["content_json"]["reflection_card_id"] == card["card_id"]
    _assert_external_safe(card["lesson_md"], memory["content_text"], str(card["structured_json"]))

    pack = service.build_context_pack(
        ContextPackBuildRequest(
            task_id=task["task_id"],
            user_message=f"Use reflection failure lesson for {task['task_id']}",
            dialogue_intent="analysis",
            token_budget=4000,
        )
    )

    recalled_ids = {item["memory_id"] for item in pack["pack_json"]["memory_items"]}
    assert memory["memory_id"] in recalled_ids
    assert "personal.episodic" in pack["pack_json"]["memory_route"]["matched_branches"]
    assert ("create_record", "reflection_cards") in repo.write_calls
    assert ("create_record", "memory_items") in repo.write_calls
    assert all(kind not in {"action_proposals", "approvals", "mcp_tool_events"} for _, kind in repo.write_calls)


def test_correction_and_low_confidence_triggers_are_deterministic_without_action_proposals() -> None:
    repo = SpyRepository()
    service = _service(repo)
    correction_task = service.create_task(TaskCreate(title="Correction reflection", input_json={}))
    low_conf_task = service.create_task(TaskCreate(title="Low confidence reflection", input_json={}))
    repo.write_calls.clear()

    service.add_task_event(
        correction_task["task_id"],
        TaskEventCreate(event_type="triage_required", severity="warning", message="Manual correction required", payload_json={}),
    )
    service.add_task_event(
        low_conf_task["task_id"],
        TaskEventCreate(event_type="llm_done", message="Answer was weak", payload_json={"confidence": 0.41}),
    )

    cards = service.list_records("reflection_cards")["items"]
    triggers = {card["trigger"] for card in cards}
    assert triggers == {"correction", "low_confidence"}
    for card in cards:
        assert card["structured_json"]["warnings"] == []
        assert card["structured_json"]["safety"]["prompt_or_strategy_changed"] is False
        assert card["structured_json"]["safety"]["action_proposals_created"] is False
    assert all(kind != "action_proposals" for _, kind in repo.write_calls)
