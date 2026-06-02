from __future__ import annotations

from typing import Any

from backend.services.research_assistant.code_intelligence_core import CodeContextQuery
from backend.services.research_assistant.models import CodeContextManifest, CodeContextRef, ContextPackBuildRequest, TaskCreate
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


class FakeCodeProvider:
    def __init__(self) -> None:
        self.requests: list[CodeContextQuery] = []

    def query_code_context(self, request: CodeContextQuery) -> CodeContextManifest:
        self.requests.append(request)
        return CodeContextManifest(
            provider="fixture",
            query=request.query,
            status="ok",
            as_of="2026-06-02T10:00:00Z",
            source_refs=["tmp/code-context.json"],
            refs=[
                CodeContextRef(
                    file_path="backend/services/research_assistant/service.py",
                    symbol="build_context_pack",
                    edge_refs=[{"edge_id": "edge-service-pack", "edge_type": "calls"}],
                    provenance={"source": "fixture", "edge": "edge-service-pack"},
                    as_of="2026-06-02T10:00:00Z",
                    summary="Context pack builder consumes code refs.",
                    summary_ref="tmp/summary.md",
                    detail_ref="tmp/detail.md",
                    call_chain=[{"from": "build_context_pack", "to": "AgentTeamsRuntime.decompose"}],
                    impact_radius={"affected_test_count": 1, "files": ["backend/services/research_assistant/service.py"]},
                    affected_tests=[
                        {
                            "test_path": "backend/tests/research_assistant/test_code_intel_context_injection.py",
                            "classification": "impacted",
                            "source_ref": "tmp/affected-tests.json",
                        }
                    ],
                )
            ],
        )


def _service(provider: Any) -> ResearchAssistantService:
    svc = ResearchAssistantService(
        repository=InMemoryResearchAssistantRepository(),
        code_intelligence_provider=provider,
    )
    svc.seed_catalogs()
    return svc


def test_code_query_injects_code_context_refs_into_context_pack_and_repository() -> None:
    provider = FakeCodeProvider()
    svc = _service(provider)
    task = svc.create_task(TaskCreate(title="code context", input_json={"module": "research_assistant"}))

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            task_id=task["task_id"],
            token_budget=3000,
            user_message="Explain backend/services/research_assistant/service.py call chain and affected tests",
        )
    )

    refs = pack["pack_json"]["code_context_refs"]
    assert provider.requests and provider.requests[0].changed_files == ("backend/services/research_assistant/service.py",)
    assert refs and refs[0]["file_path"] == "backend/services/research_assistant/service.py"
    assert refs[0]["call_chain"][0]["to"] == "AgentTeamsRuntime.decompose"
    assert refs[0]["impact_radius"]["affected_test_count"] == 1
    assert refs[0]["affected_tests"][0]["classification"] == "impacted"
    assert refs[0]["provenance"]["source"] == "fixture"
    stored = svc.repository.list_records("code_context_refs", filters={"context_pack_id": pack["context_pack_id"]}, limit=10)
    assert stored["total"] == 1
    assert stored["items"][0]["as_of"] == "2026-06-02T10:00:00Z"


def test_windows_style_code_path_is_normalized_before_provider_call() -> None:
    provider = FakeCodeProvider()
    svc = _service(provider)
    task = svc.create_task(TaskCreate(title="windows path code context"))

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            task_id=task["task_id"],
            token_budget=3000,
            user_message=r"Explain backend\services\research_assistant\service.py call chain",
        )
    )

    assert provider.requests and provider.requests[0].changed_files == ("backend/services/research_assistant/service.py",)
    assert pack["pack_json"]["code_context_refs"][0]["file_path"] == "backend/services/research_assistant/service.py"


def test_non_code_query_has_empty_refs_with_reason_code_and_does_not_call_provider() -> None:
    provider = FakeCodeProvider()
    svc = _service(provider)
    task = svc.create_task(TaskCreate(title="general market note"))

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            task_id=task["task_id"],
            token_budget=3000,
            user_message="Summarize today's agenda and meeting notes",
        )
    )

    assert provider.requests == []
    assert pack["pack_json"]["code_context_refs"] == []
    assert pack["pack_json"]["code_context_ref_reason_code"] == "non_code_query"
