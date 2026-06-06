from __future__ import annotations

from typing import Any

from backend.services.research_assistant.memory_tree import select_memory_branches
from backend.services.research_assistant.models import ContextPackBuildRequest, TaskCreate
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    svc.seed_catalogs()
    return svc


def _memory(memory_id: str, *, memory_type: str, tree_path: str, content: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "memory_id": memory_id,
        "memory_type": memory_type,
        "namespace": "aistock",
        "subject_key": tree_path,
        "title": tree_path,
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
        "tree_path": tree_path,
        "parent_key": ".".join(tree_path.split(".")[:-1]) or None,
        "node_type": "fact",
        "scope": "personal" if tree_path.startswith("personal.") else "project",
        "importance": 0.7,
        "last_used_at": "2026-06-01T00:00:00+00:00",
        "use_count": 0,
        "auto_created": False,
        "trust_level": "user_stated",
        "provenance_json": {"source": "pytest"},
        "resident": False,
    }
    row.update(overrides)
    return row


def test_select_memory_branches_collapses_matching_branches_and_includes_resident_items() -> None:
    svc = _service()
    svc.repository.create_record("memory_items", _memory("mem_factor", memory_type="experiment", tree_path="project.qe.factor.ic", content="factor IC diagnostics are required"))
    svc.repository.create_record("memory_items", _memory("mem_model", memory_type="architecture", tree_path="project.qe.model.training", content="model training uses fixed seeds"))
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_pref",
            memory_type="user_preference",
            tree_path="personal.preference.language",
            content="answer in Chinese",
            scope="personal",
            resident=True,
            importance=1.0,
        ),
    )

    result = select_memory_branches(
        "analyze the factor model experiment",
        "analysis",
        repo=svc.repository,
        runtime_config=svc.active_runtime_config(),
    )

    assert {item["memory_id"] for item in result.memory_items} >= {"mem_factor", "mem_model", "mem_pref"}
    assert "project.qe" in result.matched_branches
    assert "resident" in result.route_reason
    assert result.refs_by_type["user_preference"] == ["mem_pref"]


def test_build_context_pack_consumes_tree_route_and_records_branch_metadata() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="tree retrieval", input_json={"user_message": "analyze factor model experiment"}))
    svc.repository.create_record("memory_items", _memory("mem_factor", memory_type="experiment", tree_path="project.qe.factor.ic", content="factor IC diagnostics are required"))
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_pref",
            memory_type="directive",
            tree_path="personal.directive.response",
            content="show evidence before conclusion",
            scope="personal",
            resident=True,
            importance=1.0,
        ),
    )

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            task_id=task["task_id"],
            user_message="analyze factor model experiment",
            dialogue_intent="analysis",
            token_budget=4000,
        )
    )

    assert pack["pack_json"]["memory_route"]["route_reason"]
    assert "project.qe" in pack["pack_json"]["memory_route"]["matched_branches"]
    assert {item["memory_id"] for item in pack["pack_json"]["memory_items"]} >= {"mem_factor", "mem_pref"}
    assert pack["experiment_memory_refs"] == ["mem_factor"]
    assert "mem_pref" in pack["core_memory_refs"]
