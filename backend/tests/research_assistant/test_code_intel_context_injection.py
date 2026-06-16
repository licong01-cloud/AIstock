from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.research_assistant.code_intelligence import build_query_code_context, expected_code_context_ref_ids
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


def _service_with_runtime(repo: InMemoryResearchAssistantRepository) -> ResearchAssistantService:
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
    return ResearchAssistantService(repository=repo, llm_client=object())


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


def test_code_query_cache_hit_reuses_persisted_refs_without_adapter() -> None:
    adapter = FakeCodeIntelligenceAdapter()
    first = build_query_code_context(
        user_query="check backend/services/research_assistant/service.py build_context_pack impact",
        task_id="task-code-cache",
        repo_root=Path.cwd(),
        token_budget=1800,
        adapter_module=adapter,
        skip_external=True,
    )
    assert first["code_context_refs"]
    assert len(adapter.context_calls) == 1
    persisted_refs = list(first["code_context_refs"])

    def cache_lookup(payload: dict[str, Any]) -> dict[str, Any]:
        expected = set(payload["expected_ref_ids"])
        hits = [ref for ref in persisted_refs if ref["code_ref_id"] in expected]
        return {
            "status": "hit",
            "code_context_refs": hits,
            "reason_codes": ["code_context_cache_hit"],
            "warnings": [],
            "as_of": hits[0]["as_of"],
        }

    cached = build_query_code_context(
        user_query="check backend/services/research_assistant/service.py build_context_pack impact",
        task_id="task-code-cache",
        repo_root=Path.cwd(),
        token_budget=1800,
        adapter_module=adapter,
        skip_external=True,
        cache_lookup=cache_lookup,
    )

    assert cached["status"] == "ok"
    assert cached["cache"]["status"] == "hit"
    assert "code_context_cache_hit" in cached["reason_codes"]
    assert cached["code_context_refs"] == persisted_refs
    assert len(adapter.context_calls) == 1
    assert len(adapter.affected_calls) == 1


def test_non_code_query_does_not_inject_or_pollute_context_pack() -> None:
    repo = InMemoryResearchAssistantRepository()
    service = _service_with_runtime(repo)
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
    assert pack_json["code_context_route"]["warnings"]
    assert service.repository.list_records("code_context_refs", limit=10)["total"] == 0


def test_repository_cache_lookup_returns_persisted_code_context_refs_without_adapter() -> None:
    repo = InMemoryResearchAssistantRepository()
    service = _service_with_runtime(repo)
    adapter = FakeCodeIntelligenceAdapter()
    query = "check backend/services/research_assistant/service.py build_context_pack impact"
    refs_payload = build_query_code_context(
        user_query=query,
        task_id="task-code-cache-pack",
        repo_root=Path.cwd(),
        token_budget=1200,
        adapter_module=adapter,
        skip_external=True,
    )
    service._persist_code_context_refs(task_id="task-code-cache-pack", refs=list(refs_payload["code_context_refs"]))
    cache_payload = expected_code_context_ref_ids(
        user_query=query,
        task_id="task-code-cache-pack",
        repo_root=Path.cwd(),
        token_budget=1200,
    )
    assert len(adapter.context_calls) == 1
    assert len(adapter.affected_calls) == 1

    cached = service._lookup_code_context_cache(cache_payload)

    assert cached["status"] == "hit"
    assert cached["code_context_refs"]
    assert "code_context_cache_hit" in cached["reason_codes"]
    assert len(adapter.context_calls) == 1
    assert len(adapter.affected_calls) == 1


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
    assert payload["warnings"]


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


def test_catalog_parse_failure_is_explicit_and_keeps_default_limits(tmp_path: Path) -> None:
    catalog_dir = tmp_path / "tests" / "aistock_validation" / "catalog"
    catalog_dir.mkdir(parents=True)
    (catalog_dir / "code_intelligence.yaml").write_text("codegraph: [unterminated\n", encoding="utf-8")

    payload = build_query_code_context(
        user_query="check backend/services/research_assistant/code_intelligence.py parse_code_query_scope impact",
        task_id="task-code-bad-catalog",
        repo_root=tmp_path,
        token_budget=1800,
        adapter_module=FakeCodeIntelligenceAdapter(),
        skip_external=True,
    )

    assert payload["limits"]["max_context_symbols"] == 8
    assert "code_intelligence_catalog_parse_failed" in payload["reason_codes"]
    assert any("code intelligence catalog parse failed" in warning for warning in payload["warnings"])
