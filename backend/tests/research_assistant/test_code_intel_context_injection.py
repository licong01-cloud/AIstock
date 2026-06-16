from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.research_assistant.code_intelligence import build_query_code_context
from backend.services.research_assistant.models import ContextPackBuildRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, RUNTIME_CONFIG_KEY, load_runtime_config
from backend.services.research_assistant.service import ResearchAssistantService


class FakeCodeIntelligenceAdapter:
    def __init__(self) -> None:
        self.context_calls: list[dict[str, Any]] = []
        self.affected_calls: list[dict[str, Any]] = []

    def build_context_artifacts(self, **kwargs: Any) -> dict[str, Any]:
        self.context_calls.append(kwargs)
        return {
            "schema_version": "aistock_code_intelligence_context_v1",
            "generated_at": "2026-06-16T09:00:00Z",
            "git_commit": "commit-phase8",
            "query": kwargs["query"],
            "changed_files": kwargs.get("changed_files") or [],
            "status": "ok",
            "context_markdown": "tmp/issue_workflow/phase8/codegraph-context.md",
            "manifest_path": "tmp/issue_workflow/phase8/code-intelligence.json",
            "context_quality": {"quality": "scoped", "broad_scan_required": False},
            "fallback": {"used": False, "reason": None},
            "codegraph_status": {"git_commit": "commit-phase8"},
        }

    def build_affected_tests_artifact(self, **kwargs: Any) -> dict[str, Any]:
        self.affected_calls.append(kwargs)
        return {
            "schema_version": "aistock_codegraph_affected_tests_v1",
            "generated_at": "2026-06-16T09:00:00Z",
            "artifact_path": "tmp/issue_workflow/phase8/affected-tests.json",
            "status": "ok",
            "quality": "ok",
            "suggested_tests": [
                "backend/tests/research_assistant/test_code_intel_context_injection.py",
                "backend/tests/research_assistant/test_code_intel_decomposition.py",
            ],
            "fallback": {"used": False, "reason": None},
        }


def test_code_query_injects_query_scoped_refs_with_complete_provenance() -> None:
    adapter = FakeCodeIntelligenceAdapter()
    payload = build_query_code_context(
        user_query="分析 backend/services/research_assistant/service.py 的 build_context_pack 调用链和影响测试",
        task_id="task-code-1",
        repo_root=Path.cwd(),
        token_budget=1800,
        adapter_module=adapter,
        skip_external=True,
    )

    refs = payload["code_context_refs"]
    assert payload["status"] == "ok"
    assert refs, "code query must inject code_context_refs"
    assert adapter.context_calls[0]["max_symbols"] == 8
    assert adapter.context_calls[0]["skip_external"] is True
    assert adapter.affected_calls[0]["skip_external"] is True
    for ref in refs:
        assert ref["source"] == "codegraph"
        assert ref["as_of"] == "2026-06-16T09:00:00Z"
        assert ref["provenance"]["commit"] == "commit-phase8"
        assert ref["provenance"]["file"]
        assert "generated_at" in ref["provenance"]
        assert ref["manifest_json"]["summary_first"] is True
        assert ref["manifest_json"]["raw_context_embedded"] is False
        assert len(ref["affected_tests"]) <= 8
    assert payload["adapter_contract"] == {
        "provider": "codegraph",
        "deterministic_ast_only": True,
        "embedding_used": False,
        "llm_repo_scan_used": False,
    }


def test_non_code_query_does_not_inject_or_pollute_context_pack() -> None:
    repo = InMemoryResearchAssistantRepository()
    runtime = load_runtime_config(DEFAULT_RUNTIME_CONFIG_PATH).config
    repo.create_record(
        "runtime_config_activations",
        {
            "activation_id": "runtime_phase8",
            "config_key": RUNTIME_CONFIG_KEY,
            "config_version": "test",
            "environment": "dev",
            "status": "active",
            "config_json": runtime,
        },
    )
    service = ResearchAssistantService(repository=repo, llm_client=object())
    repo.create_record(
        "tasks",
        {
            "task_id": "task-non-code",
            "title": "non code query",
            "task_type": "research",
            "status": "planned",
            "risk_level": "medium",
            "input_json": {},
            "result_json": {},
        },
    )

    pack = service.build_context_pack(
        ContextPackBuildRequest(
            task_id="task-non-code",
            agent_id="assistant",
            model_profile="model",
            user_message="目前 QE 数仓入仓健康状态怎么样？",
            token_budget=1200,
            dialogue_intent="qe_warehouse_request",
        )
    )

    pack_json = pack["pack_json"]
    assert pack_json["code_context_refs"] == []
    assert pack_json["code_context_route"]["reason_codes"] == ["non_code_query"]
    assert service.repository.list_records("code_context_refs", limit=10)["total"] == 0


def test_code_scope_parse_failure_is_explicit_degrade_not_error() -> None:
    payload = build_query_code_context(
        user_query="帮我看一下这段代码要怎么改",
        task_id="task-code-ambiguous",
        repo_root=Path.cwd(),
        adapter_module=FakeCodeIntelligenceAdapter(),
        skip_external=True,
    )

    assert payload["status"] == "skipped"
    assert payload["code_context_refs"] == []
    assert payload["reason_codes"] == ["no_code_scope_detected"]


def test_code_context_is_summary_first_and_has_no_embedding_dependency() -> None:
    text = Path("backend/services/research_assistant/code_intelligence.py").read_text(encoding="utf-8")
    forbidden = ("semantic_search", "vector_store", "embedding_model", "openai.embeddings")
    for marker in forbidden:
        assert marker not in text

    payload = build_query_code_context(
        user_query="检查 backend/services/research_assistant/code_intelligence.py 的 parse_code_query_scope 影响面",
        task_id="task-code-summary",
        repo_root=Path.cwd(),
        token_budget=5000,
        adapter_module=FakeCodeIntelligenceAdapter(),
        skip_external=True,
    )

    assert payload["limits"]["max_context_symbols"] == 16
    serialized_refs = str(payload["code_context_refs"])
    assert "Code Intelligence Context Guidance" not in serialized_refs
    assert "raw_context_embedded': True" not in serialized_refs
