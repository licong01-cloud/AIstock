from __future__ import annotations

from typing import Any

from backend.services.research_assistant.memory_tree import select_memory_branches
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository


def _memory(memory_id: str, *, importance: float, last_used_at: str, content: str = "factor signal") -> dict[str, Any]:
    return {
        "memory_id": memory_id,
        "memory_type": "analysis_note",
        "namespace": "aistock",
        "subject_key": f"project.qe.factor.{memory_id}",
        "title": f"factor {memory_id}",
        "content_json": {"text": content},
        "content_text": content,
        "source_type": "test",
        "source_ref": f"test://{memory_id}",
        "confidence": 1.0,
        "approval_status": "approved",
        "risk_level": "low",
        "evidence_refs": [f"test://{memory_id}"],
        "checksum": f"checksum-{memory_id}",
        "created_by": "pytest",
        "tree_path": f"project.qe.factor.{memory_id}",
        "parent_key": "project.qe.factor",
        "node_type": "fact",
        "scope": "project",
        "importance": importance,
        "last_used_at": last_used_at,
        "use_count": 0,
        "auto_created": False,
        "trust_level": "user_stated",
        "provenance_json": {"source": "pytest"},
        "resident": False,
    }


def test_memory_tree_scores_importance_before_recency_noise() -> None:
    repo = InMemoryResearchAssistantRepository()
    repo.create_record("memory_items", _memory("low_recent", importance=0.2, last_used_at="2026-06-01T00:00:00+00:00"))
    repo.create_record("memory_items", _memory("high_older", importance=0.95, last_used_at="2026-05-01T00:00:00+00:00"))
    repo.create_record("memory_items", _memory("middle", importance=0.5, last_used_at="2026-05-31T00:00:00+00:00"))

    result = select_memory_branches(
        "factor signal analysis",
        "analysis",
        repo=repo,
        runtime_config={"memory_tree": {"candidate_limit": 10, "max_items": 3}},
    )

    assert [item["memory_id"] for item in result.memory_items][:3] == ["high_older", "middle", "low_recent"]


def test_memory_tree_omits_low_ranked_items_when_budget_is_exhausted() -> None:
    repo = InMemoryResearchAssistantRepository()
    repo.create_record("memory_items", _memory("keep", importance=1.0, last_used_at="2026-06-01T00:00:00+00:00", content="factor " * 5))
    repo.create_record("memory_items", _memory("omit", importance=0.1, last_used_at="2026-06-01T00:00:00+00:00", content="factor " * 100))

    result = select_memory_branches(
        "factor",
        "analysis",
        repo=repo,
        runtime_config={"memory_tree": {"candidate_limit": 10, "max_items": 10, "token_budget": 20}},
    )

    assert [item["memory_id"] for item in result.memory_items] == ["keep"]
    assert result.omitted_refs == ["omit"]
