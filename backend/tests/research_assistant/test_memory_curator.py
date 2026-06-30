from __future__ import annotations

import pytest

from backend.services.research_assistant.memory_curator import MemoryCurator
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository


def test_t9_7_chinese_remember_intent_creates_memory_candidate_via_semantic_extractor() -> None:
    repo = InMemoryResearchAssistantRepository()
    semantic_calls: list[dict[str, str]] = []

    def semantic_extractor(*, user_message: str, assistant_message: str) -> list[dict[str, object]]:
        semantic_calls.append({"user_message": user_message, "assistant_message": assistant_message})
        return [
            {
                "memory_type": "task_state",
                "scope": "personal",
                "tree_path": "personal.task_state.todo",
                "title": "待办：明天复盘",
                "content_text": "明天要复盘",
                "trust_level": "user_stated",
                "resident": False,
                "requires_approval": True,
                "importance": 0.7,
            }
        ]

    result = MemoryCurator(repo, semantic_extractor=semantic_extractor).curate_turn(
        user_message="帮我记住明天要复盘",
        assistant_message="好的，我会先生成记忆候选，等你审批。",
        conversation_id="conv_t9_7",
        user_message_id="msg_user_t9_7",
        assistant_message_id="msg_assistant_t9_7",
        task_id="rat_t9_7",
    )

    assert semantic_calls == [
        {
            "user_message": "帮我记住明天要复盘",
            "assistant_message": "好的，我会先生成记忆候选，等你审批。",
        }
    ]
    assert result.created_branch_ids
    assert result.created_memory_ids
    assert result.approval_required_ids == result.created_memory_ids
    rows = repo.list_records("memory_items", filters={"memory_type": "task_state"}, limit=20)["items"]
    fact = next(item for item in rows if item["node_type"] == "fact")
    assert fact["content_text"] == "明天要复盘"
    assert fact["tree_path"] == "personal.task_state.todo"
    assert fact["scope"] == "personal"
    assert fact["approval_status"] == "draft"
    assert fact["risk_level"] == "medium"
    assert fact["resident"] is False
    assert fact["provenance_json"]["conversation_id"] == "conv_t9_7"


def test_t9_7_semantic_extractor_can_use_assistant_reply_context() -> None:
    repo = InMemoryResearchAssistantRepository()

    def semantic_extractor(*, user_message: str, assistant_message: str) -> list[dict[str, object]]:
        assert user_message == "这个项目以后都先走 issue workflow。"
        assert "项目指令" in assistant_message
        return [
            {
                "memory_type": "directive",
                "scope": "project",
                "tree_path": "project.directive.workflow",
                "title": "项目流程指令",
                "content_text": "这个项目以后都先走 issue workflow。",
                "trust_level": "user_stated",
                "resident": False,
                "requires_approval": True,
                "importance": 0.9,
            }
        ]

    result = MemoryCurator(repo, semantic_extractor=semantic_extractor).curate_turn(
        user_message="这个项目以后都先走 issue workflow。",
        assistant_message="理解，这是项目指令，我会按候选记忆处理。",
        conversation_id="conv_project",
        user_message_id="msg_project_user",
        assistant_message_id="msg_project_assistant",
        task_id="rat_project",
    )

    assert result.approval_required_ids
    rows = repo.list_records("memory_items", filters={"memory_type": "directive"}, limit=20)["items"]
    fact = next(item for item in rows if item["node_type"] == "fact")
    assert fact["scope"] == "project"
    assert fact["approval_status"] == "draft"
    assert fact["resident"] is False
    assert fact["trust_level"] == "user_stated"


def test_t9_7_project_directive_semantic_candidate_cannot_bypass_approval() -> None:
    repo = InMemoryResearchAssistantRepository()

    def semantic_extractor(**_kwargs: object) -> list[dict[str, object]]:
        return [
            {
                "memory_type": "directive",
                "scope": "project",
                "tree_path": "project.directive.workflow",
                "title": "项目流程指令",
                "content_text": "项目指令必须审批。",
                "trust_level": "user_stated",
                "resident": False,
                "requires_approval": False,
                "importance": 0.9,
            }
        ]

    with pytest.raises(ValueError, match="project semantic memory candidates require draft approval"):
        MemoryCurator(repo, semantic_extractor=semantic_extractor).curate_turn(
            user_message="这个项目以后都先走 issue workflow。",
            assistant_message="收到。",
            conversation_id="conv_project",
            user_message_id="msg_project_user",
            assistant_message_id="msg_project_assistant",
            task_id="rat_project",
        )


def test_t9_7_personal_preference_scope_policy_stays_approved_resident() -> None:
    repo = InMemoryResearchAssistantRepository()
    curator = MemoryCurator(repo)

    result = curator.curate_turn(
        user_message="remember preference: answer in concise Chinese",
        assistant_message="acknowledged",
        conversation_id="conv_pref",
        user_message_id="msg_pref_user",
        assistant_message_id="msg_pref_assistant",
        task_id="rat_pref",
    )

    assert not result.approval_required_ids
    facts = [
        item
        for item in repo.list_records("memory_items", filters={"memory_type": "user_preference"}, limit=20)["items"]
        if item["node_type"] == "fact"
    ]
    assert len(facts) == 1
    assert facts[0]["scope"] == "personal"
    assert facts[0]["approval_status"] == "approved"
    assert facts[0]["resident"] is True
