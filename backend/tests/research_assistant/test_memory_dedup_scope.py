from __future__ import annotations

from backend.services.research_assistant.memory_curator import MemoryCurator
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository


def test_memory_curator_self_edits_duplicate_personal_memory_in_same_scope() -> None:
    repo = InMemoryResearchAssistantRepository()
    curator = MemoryCurator(repo)

    for _ in range(2):
        curator.curate_turn(
            user_message="remember preference: answer in concise Chinese",
            assistant_message="acknowledged",
            conversation_id="conv_1",
            user_message_id="msg_user",
            assistant_message_id="msg_assistant",
            task_id="rat_1",
        )

    facts = [
        item
        for item in repo.list_records("memory_items", filters={"memory_type": "user_preference"}, limit=20)["items"]
        if item["node_type"] == "fact"
    ]
    assert len(facts) == 1
    assert facts[0]["use_count"] == 1
    assert facts[0]["scope"] == "personal"
    assert facts[0]["approval_status"] == "approved"


def test_project_directive_rewrite_requires_approval_and_does_not_become_resident() -> None:
    repo = InMemoryResearchAssistantRepository()
    curator = MemoryCurator(repo)

    result = curator.curate_turn(
        user_message="project directive: use issue workflow for bug fixes",
        assistant_message="acknowledged",
        conversation_id="conv_2",
        user_message_id="msg_user_2",
        assistant_message_id="msg_assistant_2",
        task_id="rat_2",
    )

    assert result.approval_required_ids
    rows = repo.list_records("memory_items", filters={"memory_type": "directive"}, limit=20)["items"]
    fact = next(item for item in rows if item["node_type"] == "fact")
    assert fact["scope"] == "project"
    assert fact["approval_status"] == "draft"
    assert fact["resident"] is False
    assert fact["trust_level"] == "user_stated"
    assert fact["provenance_json"]["source"] == "chat_turn"
