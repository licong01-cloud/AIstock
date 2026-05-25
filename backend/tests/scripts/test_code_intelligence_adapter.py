from __future__ import annotations

import json
from pathlib import Path

import scripts.code_intelligence_adapter as adapter


def test_code_intelligence_doctor_falls_back_without_codegraph(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(adapter, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_doctor_report(tmp_path, skip_external=True)

    assert payload["schema_version"] == "aistock_code_intelligence_doctor_v1"
    assert payload["workflow_gate"] == "warning"
    assert payload["codegraph"]["status"] == "unavailable"
    assert payload["understand_anything"]["blocking_for_issue_workflow"] is False


def test_context_and_affected_artifacts_use_fallback_when_index_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    context = adapter.build_context_artifacts(
        item_id="BUG-199",
        query="workflow regression",
        changed_files=["scripts/aistock_issue_workflow.py"],
        root=tmp_path,
        skip_external=True,
    )
    affected = adapter.build_affected_tests_artifact(
        item_id="BUG-199",
        changed_files=["scripts/aistock_issue_workflow.py"],
        root=tmp_path,
        skip_external=True,
    )

    assert context["status"] == "fallback"
    assert context["fallback"]["used"] is True
    assert (tmp_path / context["context_markdown"]).exists()
    assert affected["status"] == "fallback"
    assert (tmp_path / affected["artifact_path"]).exists()
    assert json.loads((tmp_path / affected["artifact_path"]).read_text(encoding="utf-8"))["changed_files"] == [
        "scripts/aistock_issue_workflow.py"
    ]


def test_build_summary_links_context_and_affected_refs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_summary(
        item_id="BUG-199",
        query="workflow regression",
        changed_files=["scripts/aistock_issue_workflow.py"],
        root=tmp_path,
        skip_external=True,
    )

    assert payload["schema_version"] == "aistock_code_intelligence_summary_v1"
    assert payload["status"] == "fallback"
    assert payload["context_ref"].endswith("codegraph-context.md")
    assert payload["affected_tests_ref"].endswith("affected-tests.json")


def test_doctor_reads_code_intelligence_catalog(tmp_path: Path, monkeypatch) -> None:
    catalog = tmp_path / "tests" / "aistock_validation" / "catalog" / "code_intelligence.yaml"
    catalog.parent.mkdir(parents=True)
    catalog.write_text('schema_version: aistock_code_intelligence_catalog_v1\ncodegraph:\n  version: "0.9.4"\n', encoding="utf-8")
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_doctor_report(tmp_path, skip_external=True)

    assert payload["catalog"]["schema_version"] == "aistock_code_intelligence_catalog_v1"
    assert payload["catalog"]["codegraph"]["version"] == "0.9.4"


def test_summary_markdown_contains_warning_only_artifact_refs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_summary(
        item_id="PR-226",
        query="PR impact",
        changed_files=["scripts/code_intelligence_adapter.py"],
        root=tmp_path,
        skip_external=True,
    )
    markdown = adapter.render_summary_markdown(payload)

    assert "## Code Intelligence Summary" in markdown
    assert "scripts/code_intelligence_adapter.py" in markdown
    assert "affected-tests.json" in markdown
    assert "warning-only" in markdown


def test_understand_anything_summary_reads_graph_without_blocking(tmp_path: Path, monkeypatch) -> None:
    graph_path = tmp_path / ".understand-anything" / "knowledge-graph.json"
    graph_path.parent.mkdir(parents=True)
    graph_path.write_text(
        json.dumps(
            {
                "nodes": [
                    {"id": "paper_v2_service", "label": "paper_v2 service"},
                    {"id": "qe_service", "label": "qe service"},
                ],
                "edges": [{"source": "paper_v2_service", "target": "qe_service", "type": "depends_on"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_understand_anything_summary(module="paper_v2", root=tmp_path)

    assert payload["schema_version"] == "aistock_understand_anything_summary_v1"
    assert payload["status"] == "ok"
    assert payload["blocking_for_issue_workflow"] is False
    assert payload["nodes_used"] == 1
    assert (tmp_path / payload["artifact_path"]).exists()
    assert "Understand Anything Summary" in (tmp_path / payload["summary_ref"]).read_text(encoding="utf-8")


def test_understand_anything_summary_manifest_uses_standard_modules(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_understand_anything_summary_manifest(
        modules=["issue_workflow", "paper_v2"],
        root=tmp_path,
    )

    assert payload["schema_version"] == "aistock_understand_anything_summary_manifest_v1"
    assert payload["blocking_for_issue_workflow"] is False
    assert [item["module"] for item in payload["summary_refs"]] == ["issue_workflow", "paper_v2"]
    assert (tmp_path / "tmp" / "validation" / "code-intelligence" / "ua-summary-manifest.json").exists()

