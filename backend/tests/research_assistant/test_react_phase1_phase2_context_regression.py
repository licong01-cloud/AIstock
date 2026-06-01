from __future__ import annotations

from typing import Any

from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService


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
        "importance": 0.9,
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


class ContextRecordingLlm:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def complete(self, **kwargs: Any) -> LlmCallResult:
        self.calls.append(kwargs)
        return LlmCallResult(
            content="I used context evidence from the context pack.",
            provider="fake",
            model="fake",
            duration_ms=1,
            usage={},
        )


def test_react_prompt_consumes_phase1_memory_route_and_phase2_graph_refs() -> None:
    fake = ContextRecordingLlm()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)
    svc.seed_catalogs()
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_phase2_module",
            memory_type="architecture",
            tree_path="project.module.phase2_alpha",
            content="phase two alpha module depends on beta mcp neighbor",
            content_json={"entity_keys": ["module.phase2_alpha"]},
        ),
    )
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_directive",
            memory_type="directive",
            tree_path="personal.directive.response",
            content="show evidence before conclusion",
            scope="personal",
            resident=True,
            importance=1.0,
        ),
    )
    svc.repository.create_record(
        "memory_items",
        _memory(
            "mem_preference",
            memory_type="user_preference",
            tree_path="personal.preference.style",
            content="prefer concise sourced answers",
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

    svc.chat_turn(ChatTurnRequest(message="analyze phase two alpha module and beta mcp dependency"))

    assert fake.calls
    prompt = "\n".join(str(item.get("content", "")) for item in fake.calls[0]["messages"] if isinstance(item, dict))
    assert "rel_phase2_alpha_beta" in prompt
    assert "mcp.phase2_beta" in prompt
    assert "memory_route" in prompt
    assert "route_reason" in prompt
    assert "show evidence before conclusion" in prompt
    assert "prefer concise sourced answers" in prompt
