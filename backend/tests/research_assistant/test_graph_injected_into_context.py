from __future__ import annotations

from typing import Any

from backend.services.research_assistant.models import ContextPackBuildRequest
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
        "importance": 0.8,
        "last_used_at": "2026-06-01T00:00:00+00:00",
        "use_count": 0,
        "auto_created": False,
        "trust_level": "user_stated",
        "provenance_json": {"source": "pytest"},
        "resident": False,
    }
    row.update(overrides)
    return row


def _entity(entity_id: str, entity_key: str, entity_type: str = "module") -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "namespace": "aistock",
        "entity_key": entity_key,
        "entity_type": entity_type,
        "title": entity_key,
        "summary": f"summary for {entity_key}",
        "source_refs": [f"test://{entity_id}"],
        "approval_status": "approved",
        "confidence": 1.0,
    }


def test_build_context_pack_injects_true_graph_neighbors_and_preserves_phase1_tree_route() -> None:
    svc = _service()
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_phase2_module",
            memory_type="architecture",
            tree_path="project.module.phase2_alpha",
            content="phase2 fixture alpha module depends on beta mcp neighbor",
            content_json={"entity_keys": ["module.phase2_alpha"]},
        ),
    )
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_phase2_directive",
            memory_type="directive",
            tree_path="personal.directive.response",
            content="show evidence before conclusion",
            scope="personal",
            resident=True,
            importance=1.0,
        ),
    )
    svc.repository.create_record("entities", _entity("ent_phase2_alpha", "module.phase2_alpha", "module"))
    svc.repository.create_record("entities", _entity("ent_phase2_beta", "mcp.phase2_beta", "mcp_server"))
    svc.repository.create_record(
        "relations",
        {
            "relation_id": "rel_phase2_alpha_beta",
            "source_entity_id": "ent_phase2_alpha",
            "target_entity_id": "ent_phase2_beta",
            "relation_type": "uses",
            "evidence_refs": ["test://rel_phase2_alpha_beta"],
            "approval_status": "approved",
            "confidence": 1.0,
        },
    )

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="analyze phase2 fixture alpha module and beta mcp dependency",
            dialogue_intent="analysis",
            token_budget=4000,
        )
    )

    assert pack["graph_relation_refs"] == ["rel_phase2_alpha_beta"]
    graph_context = pack["pack_json"]["graph_context"]
    assert graph_context["route_reason"]["selected_count"] == 1
    assert graph_context["relation_refs"][0]["neighbor_entity_key"] == "mcp.phase2_beta"
    assert graph_context["relation_refs"][0]["evidence_refs"] == ["test://rel_phase2_alpha_beta"]
    assert set(graph_context["relation_refs"][0]) <= {
        "relation_id",
        "relation_type",
        "source_entity_key",
        "source_entity_type",
        "source_title",
        "target_entity_key",
        "target_entity_type",
        "target_title",
        "neighbor_entity_key",
        "neighbor_entity_type",
        "neighbor_title",
        "neighbor_summary",
        "direction",
        "depth",
        "evidence_refs",
        "confidence",
    }
    assert pack["pack_json"]["memory_route"]["route_reason"]
    assert "mem_phase2_directive" in pack["core_memory_refs"]
    assert any(item["memory_id"] == "mem_phase2_directive" for item in pack["pack_json"]["memory_items"])


def test_build_context_pack_does_not_inject_graph_for_personal_only_query() -> None:
    svc = _service()
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_personal_only",
            memory_type="user_preference",
            tree_path="personal.preference.response_style",
            content="answer with concise evidence",
            scope="personal",
            resident=True,
            importance=1.0,
        ),
    )

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="remember preference response style",
            dialogue_intent="dialogue",
            token_budget=4000,
        )
    )

    assert pack["graph_relation_refs"] == []
    assert pack["pack_json"]["graph_context"]["relation_refs"] == []
    assert pack["pack_json"]["memory_route"]["route_reason"]
    assert "mem_personal_only" in pack["core_memory_refs"]
