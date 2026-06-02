from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.research_assistant.code_intelligence_adapter_provider import (
    AistockCodeIntelligenceAdapterProvider,
)
from backend.services.research_assistant.code_intelligence_core import CodeContextQuery
import backend.services.research_assistant.code_intelligence_adapter_provider as provider_module


def test_adapter_provider_imports_and_calls_existing_adapter_symbols(tmp_path: Path, monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_status(root=None, *, skip_external=False):
        calls.append(("codegraph_status", {"root": root, "skip_external": skip_external}))
        return {
            "git_commit": "abc123",
            "graph_root_source": "current_worktree",
            "index_exists": True,
            "status": "ok",
        }

    def fake_context(*, item_id, query, changed_files, root, skip_external, **_kwargs):
        calls.append(
            (
                "build_context_artifacts",
                {
                    "changed_files": tuple(changed_files),
                    "item_id": item_id,
                    "query": query,
                    "root": root,
                    "skip_external": skip_external,
                },
            )
        )
        return {
            "generated_at": "2026-06-02T10:00:00Z",
            "context_markdown": "tmp/issue_workflow/task-code/codegraph-context.md",
            "manifest_path": "tmp/issue_workflow/task-code/code-intelligence.json",
            "graph_root_source": "current_worktree",
            "status": "ok",
        }

    def fake_affected(*, item_id, changed_files, root, skip_external, **_kwargs):
        calls.append(
            (
                "build_affected_tests_artifact",
                {
                    "changed_files": tuple(changed_files),
                    "item_id": item_id,
                    "root": root,
                    "skip_external": skip_external,
                },
            )
        )
        return {
            "generated_at": "2026-06-02T10:00:00Z",
            "artifact_path": "tmp/issue_workflow/task-code/affected-tests.json",
            "suggested_tests": ["backend/tests/research_assistant/test_code_intel_true_reuse.py"],
            "codegraph_suggested_tests": ["backend/tests/research_assistant/test_code_intel_true_reuse.py"],
            "graph_root_source": "current_worktree",
            "status": "ok",
        }

    def fake_ua_summary(*, module, root, **_kwargs):
        calls.append(("build_understand_anything_summary", {"module": module, "root": root}))
        return {
            "generated_at": "2026-06-02T10:00:00Z",
            "status": "ok",
            "summary_ref": f"tmp/validation/code-intelligence/ua-{module}-summary.md",
        }

    def fake_ua_manifest(*, modules, root, **_kwargs):
        calls.append(("build_understand_anything_summary_manifest", {"modules": tuple(modules), "root": root}))
        return {
            "generated_at": "2026-06-02T10:00:00Z",
            "modules": list(modules),
            "summary_refs": [],
        }

    monkeypatch.setattr(provider_module.adapter, "codegraph_status", fake_status)
    monkeypatch.setattr(provider_module.adapter, "build_context_artifacts", fake_context)
    monkeypatch.setattr(provider_module.adapter, "build_affected_tests_artifact", fake_affected)
    monkeypatch.setattr(provider_module.adapter, "build_understand_anything_summary", fake_ua_summary)
    monkeypatch.setattr(provider_module.adapter, "build_understand_anything_summary_manifest", fake_ua_manifest)

    provider = AistockCodeIntelligenceAdapterProvider(repo_root=tmp_path, skip_external=True)
    manifest = provider.query_code_context(
        CodeContextQuery(
            query="explain backend/services/research_assistant/service.py call chain",
            task_id="task-code",
            changed_files=("backend/services/research_assistant/service.py",),
            repo_root=tmp_path,
        )
    )

    called = [name for name, _payload in calls]
    for symbol in (
        "codegraph_status",
        "build_context_artifacts",
        "build_affected_tests_artifact",
        "build_understand_anything_summary",
        "build_understand_anything_summary_manifest",
    ):
        assert called.count(symbol) == 1
    context_call = dict(calls[1][1])
    assert context_call["item_id"] == "task-code"
    assert context_call["changed_files"] == ("backend/services/research_assistant/service.py",)
    assert manifest.refs[0].file_path == "backend/services/research_assistant/service.py"
    assert manifest.refs[0].provenance["adapter"] == "scripts.code_intelligence_adapter"
    assert manifest.refs[0].affected_tests[0].classification == "impacted"


def test_adapter_provider_failure_is_not_swallowed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(provider_module.adapter, "codegraph_status", lambda *args, **kwargs: {"status": "ok"})

    def fail_context(**_kwargs):
        raise RuntimeError("adapter failed")

    monkeypatch.setattr(provider_module.adapter, "build_context_artifacts", fail_context)

    provider = AistockCodeIntelligenceAdapterProvider(repo_root=tmp_path, skip_external=True)
    with pytest.raises(RuntimeError, match="adapter failed"):
        provider.query_code_context(
            CodeContextQuery(
                query="explain backend/services/research_assistant/service.py",
                task_id="task-fail",
                changed_files=("backend/services/research_assistant/service.py",),
            )
        )


def test_adapter_provider_has_direct_existing_adapter_import() -> None:
    source = Path(provider_module.__file__).read_text(encoding="utf-8")
    assert "import scripts.code_intelligence_adapter as adapter" in source
    assert "class CodeGraph" not in source
    assert "UnderstandAnything" not in source
