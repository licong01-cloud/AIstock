from __future__ import annotations

from backend.services.research_assistant.memory_curator import MemoryCurator
from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService


class FakeLlmClient:
    def complete(self, **kwargs: object) -> LlmCallResult:
        return LlmCallResult(
            content="acknowledged",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={"prompt_tokens": 10, "completion_tokens": 2},
        )


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=FakeLlmClient())
    svc.seed_catalogs()
    return svc


def test_memory_curator_auto_creates_branch_and_fact_with_provenance() -> None:
    repo = InMemoryResearchAssistantRepository()
    curator = MemoryCurator(repo)

    result = curator.curate_turn(
        user_message="remember preference: answer in concise Chinese",
        assistant_message="acknowledged",
        conversation_id="conv_1",
        user_message_id="msg_user",
        assistant_message_id="msg_assistant",
        task_id="rat_1",
    )

    assert result.created_branch_ids
    assert result.created_memory_ids
    rows = repo.list_records("memory_items", filters={}, limit=20)["items"]
    branch = next(item for item in rows if item["node_type"] == "branch")
    fact = next(item for item in rows if item["node_type"] == "fact")
    assert branch["auto_created"] is True
    assert branch["tree_path"] == "personal.preference.response"
    assert fact["memory_type"] == "user_preference"
    assert fact["scope"] == "personal"
    assert fact["resident"] is True
    assert fact["approval_status"] == "approved"
    assert fact["provenance_json"]["conversation_id"] == "conv_1"
    assert fact["provenance_json"]["user_message_id"] == "msg_user"


def test_chat_turn_triggers_memory_curator_after_assistant_reply() -> None:
    svc = _service()

    result = svc.chat_turn(ChatTurnRequest(message="remember preference: answer with evidence first"))

    rows = svc.repository.list_records("memory_items", filters={"memory_type": "user_preference"}, limit=20)["items"]
    assert rows
    assert rows[0]["content_text"] == "answer with evidence first"
    assert rows[0]["provenance_json"]["conversation_id"] == result["conversation"]["conversation_id"]
    events = svc.list_records("task_events", filters={"task_id": result["task"]["task_id"]}, limit=20)["items"]
    assert any(event["event_type"] == "memory_written" for event in events)
