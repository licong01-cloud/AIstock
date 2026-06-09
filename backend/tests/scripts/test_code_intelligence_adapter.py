from __future__ import annotations

import json
from pathlib import Path

import scripts.code_intelligence_adapter as adapter


def test_emit_dash_writes_stdout_without_dash_file(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    adapter._emit({"ok": True}, "-")

    assert json.loads(capsys.readouterr().out) == {"ok": True}
    assert not (tmp_path / "-").exists()


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
    assert payload["understand_anything"]["status"] in {"not_configured", "configured_missing_graph"}


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


def test_codegraph_status_reuses_canonical_index_for_worktree(tmp_path: Path, monkeypatch) -> None:
    canonical = tmp_path / "canonical"
    worktree = tmp_path / "worktree"
    (canonical / ".codegraph").mkdir(parents=True)
    (canonical / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    worktree.mkdir()
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(adapter, "_canonical_repo_root", lambda root: canonical if root == worktree else root)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    calls: list[list[str]] = []

    def fake_run(args, cwd=None, timeout=30):
        calls.append(args)
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "0.9.4", "stderr": ""}
        assert args == ["codegraph", "status", str(canonical)]
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "Files: 1\nNodes: 2\nEdges: 3\nIndex is up to date",
            "stderr": "",
        }

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.codegraph_status(worktree)

    assert payload["status"] == "ok"
    assert payload["index_exists"] is True
    assert payload["graph_root"] == str(canonical)
    assert payload["graph_root_source"] == "canonical_worktree_root"
    assert calls[-1] == ["codegraph", "status", str(canonical)]


def test_context_uses_repo_index_when_detail_context_fails(tmp_path: Path, monkeypatch) -> None:
    canonical = tmp_path / "canonical"
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    monkeypatch.setattr(
        adapter,
        "codegraph_status",
        lambda root, skip_external=False: {
            "available": True,
            "index_exists": True,
            "command": "codegraph",
            "version": "0.9.4",
            "git_commit": "abc123",
            "working_tree_dirty": False,
            "graph_root": str(canonical),
            "graph_root_source": "canonical_worktree_root",
            "index_summary": {"files": 10, "nodes": 20, "edges": 30},
        },
    )

    def fake_run(args, cwd=None, timeout=30):
        assert args[:5] == ["codegraph", "context", "workflow bug", "--path", str(canonical)]
        return {"ok": False, "returncode": 2, "stdout": "", "stderr": "no matching detail"}

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.build_context_artifacts(
        item_id="BUG-199",
        query="workflow bug",
        changed_files=["scripts/aistock_issue_workflow.py"],
        root=worktree,
    )

    assert payload["status"] == "repo_index_ready"
    assert payload["fallback"]["used"] is False
    assert payload["channel"] == "repo_index"
    assert payload["graph_root"] == str(canonical)
    context_text = (worktree / payload["context_markdown"]).read_text(encoding="utf-8")
    assert "repo_index_ready" in context_text


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


def test_build_summary_includes_ua_module_ref_without_inlining_graph(tmp_path: Path, monkeypatch) -> None:
    graph_path = tmp_path / ".understand-anything" / "knowledge-graph.json"
    graph_path.parent.mkdir(parents=True)
    graph_path.write_text(
        json.dumps(
            {
                "nodes": [{"id": "validation.workflow", "label": "validation workflow"}],
                "edges": [{"source": "validation.workflow", "target": "scripts/aistock_issue_workflow.py"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_summary(
        item_id="BUG-273",
        query="workflow validation",
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation",
        root=tmp_path,
        skip_external=True,
    )

    assert payload["understand_anything_summary_ref"].endswith("ua-validation-summary.md")
    assert payload["understand_anything_summary"]["nodes_used"] == 1
    assert "selected_nodes" not in payload["understand_anything_summary"]
    assert (tmp_path / payload["understand_anything_summary"]["artifact_path"]).exists()


def test_understand_anything_status_reports_configured_missing_graph(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "home"
    skill = home / ".agents" / "skills" / "understand"
    skill.mkdir(parents=True)
    monkeypatch.setenv("USERPROFILE", str(home))

    payload = adapter.understand_anything_status(tmp_path, skip_external=True)

    assert payload["status"] == "configured_missing_graph"
    assert payload["codex_skill_exists"] is True
    assert payload["graph_exists"] is False
    assert payload["generate_graph_command"].startswith("/understand")


def test_affected_tests_supplements_codegraph_with_repo_import_scan(
    tmp_path: Path,
    monkeypatch,
) -> None:
    script = tmp_path / "scripts" / "code_intelligence_adapter.py"
    test_file = tmp_path / "backend" / "tests" / "scripts" / "test_code_intelligence_adapter.py"
    script.parent.mkdir(parents=True)
    test_file.parent.mkdir(parents=True)
    script.write_text("def build_summary():\n    return {}\n", encoding="utf-8")
    test_file.write_text("import scripts.code_intelligence_adapter as adapter\n", encoding="utf-8")
    monkeypatch.setattr(
        adapter,
        "codegraph_status",
        lambda root, skip_external=False: {
            "available": True,
            "index_exists": True,
            "command": "codegraph",
            "version": "0.9.4",
        },
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {"ok": True, "stdout": "", "stderr": ""},
    )

    payload = adapter.build_affected_tests_artifact(
        item_id="BUG-199",
        changed_files=["scripts/code_intelligence_adapter.py"],
        root=tmp_path,
    )

    assert payload["status"] == "ok"
    assert payload["codegraph_suggested_tests"] == []
    assert payload["repo_fallback_suggested_tests"] == [
        "backend/tests/scripts/test_code_intelligence_adapter.py"
    ]
    assert payload["suggested_tests"] == ["backend/tests/scripts/test_code_intelligence_adapter.py"]
    assert payload["test_discovery_fallback"]["used"] is True
    assert payload["quality"] == "partial_codegraph_plus_repo_fallback"


def test_affected_tests_filter_applies_to_repo_import_scan(tmp_path: Path, monkeypatch) -> None:
    script = tmp_path / "scripts" / "aistock_issue_workflow.py"
    backend_test = tmp_path / "backend" / "tests" / "scripts" / "test_aistock_issue_workflow.py"
    other_test = tmp_path / "tests" / "smoke" / "test_aistock_issue_workflow.py"
    script.parent.mkdir(parents=True)
    backend_test.parent.mkdir(parents=True)
    other_test.parent.mkdir(parents=True)
    script.write_text("def build_run_plan():\n    return {}\n", encoding="utf-8")
    backend_test.write_text("import scripts.aistock_issue_workflow as workflow\n", encoding="utf-8")
    other_test.write_text("import scripts.aistock_issue_workflow as workflow\n", encoding="utf-8")
    monkeypatch.setattr(
        adapter,
        "codegraph_status",
        lambda root, skip_external=False: {
            "available": True,
            "index_exists": True,
            "command": "codegraph",
            "version": "0.9.4",
        },
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {"ok": True, "stdout": "", "stderr": ""},
    )

    payload = adapter.build_affected_tests_artifact(
        item_id="BUG-199",
        changed_files=["scripts/aistock_issue_workflow.py"],
        root=tmp_path,
        filter_glob="backend/tests/**/*.py",
    )

    assert payload["suggested_tests"] == ["backend/tests/scripts/test_aistock_issue_workflow.py"]
    assert payload["test_discovery_fallback"]["matched_tests"] == {
        "backend/tests/scripts/test_aistock_issue_workflow.py": ["scripts.aistock_issue_workflow"]
    }


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


def test_codegraph_status_sanitizes_successful_external_output(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    (tmp_path / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    def fake_run(args, cwd=None, timeout=30):
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "\x1b[32m0.9.4\x1b[0m", "stderr": ""}
        return {
            "ok": True,
            "returncode": 0,
            "stdout": (
                "\x1b[1mCodeGraph Status\x1b[0m\n"
                "  Files:     1,921\n"
                "  Nodes:     40,678\n"
                "  Edges:     109,343\n"
                "  DB Size:   102.57 MB\n"
                "\x1b[32m[OK]\x1b[0m Index is up to date"
            ),
            "stderr": "",
        }

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.codegraph_status(tmp_path)

    assert payload["version"] == "0.9.4"
    assert payload["version_check"] == {"ok": True, "returncode": 0, "stdout_summary": "0.9.4"}
    assert payload["status_check"] == {"ok": True, "returncode": 0, "stdout_summary": "Index is up to date"}
    assert payload["index_summary"] == {
        "files": 1921,
        "nodes": 40678,
        "edges": 109343,
        "db_size": "102.57 MB",
        "up_to_date": True,
    }
    assert "stdout" not in payload["status_check"]
    assert "\x1b" not in json.dumps(payload)


def test_codegraph_status_preserves_compact_failure_output(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    def fake_run(args, cwd=None, timeout=30):
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "0.9.4", "stderr": ""}
        return {"ok": False, "returncode": 2, "stdout": "\x1b[31mbad\x1b[0m", "stderr": "failed"}

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.codegraph_status(tmp_path)

    assert payload["status_check"]["ok"] is False
    assert payload["status_check"]["stdout"] == "bad"
    assert payload["status_check"]["stderr"] == "failed"
    assert "\x1b" not in json.dumps(payload)


def test_codegraph_freshness_ready_for_up_to_date_index(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    (tmp_path / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    def fake_run(args, cwd=None, timeout=30):
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "0.9.4", "stderr": ""}
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "Files: 10\nNodes: 20\nEdges: 30\nIndex is up to date",
            "stderr": "",
        }

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.build_codegraph_freshness_artifact(root=tmp_path, max_age_hours=36)

    assert payload["schema_version"] == "aistock_codegraph_freshness_v1"
    assert payload["workflow_gate"] == "ready"
    assert payload["freshness"] == "fresh"
    assert payload["blocking_for_issue_workflow"] is False
    assert (tmp_path / payload["artifact_path"]).exists()
    assert "CodeGraph Freshness" in (tmp_path / payload["summary_ref"]).read_text(encoding="utf-8")


def test_codegraph_freshness_warns_for_missing_index(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_codegraph_freshness_artifact(root=tmp_path, skip_external=True)

    assert payload["workflow_gate"] == "warning"
    assert payload["freshness"] == "unavailable"
    assert payload["warnings"]


def test_codegraph_freshness_warns_for_stale_index_when_status_unchecked(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    index = tmp_path / ".codegraph" / "codegraph.db"
    index.write_text("db", encoding="utf-8")
    old_time = index.stat().st_mtime - 10_000
    index.touch()
    import os

    os.utime(index, (old_time, old_time))
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {"ok": True, "returncode": 0, "stdout": "Index is up to date", "stderr": ""},
    )

    payload = adapter.build_codegraph_freshness_artifact(root=tmp_path, max_age_hours=1, skip_external=True)

    assert payload["workflow_gate"] == "warning"
    assert payload["freshness"] == "stale"
    assert payload["freshness_basis"] == "mtime"
    assert any("age exceeds" in item for item in payload["warnings"])


def test_codegraph_freshness_trusts_up_to_date_status_over_old_mtime(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    index = tmp_path / ".codegraph" / "codegraph.db"
    index.write_text("db", encoding="utf-8")
    old_time = index.stat().st_mtime - 10_000
    import os

    os.utime(index, (old_time, old_time))
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {
            "ok": True,
            "returncode": 0,
            "stdout": "Files: 10\nNodes: 20\nEdges: 30\nIndex is up to date",
            "stderr": "",
        },
    )

    payload = adapter.build_codegraph_freshness_artifact(root=tmp_path, max_age_hours=1)

    assert payload["workflow_gate"] == "ready"
    assert payload["freshness"] == "fresh"
    assert payload["freshness_basis"] == "codegraph_status"
    assert not payload["warnings"]
    assert any("mtime exceeds" in item for item in payload["notes"])
    assert "### Notes" in (tmp_path / payload["summary_ref"]).read_text(encoding="utf-8")


def test_latest_codegraph_freshness_reads_newest_artifact_without_external_call(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "nightly-1"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "old.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-03T00:00:00Z",
                "provider": "codegraph",
                "workflow_gate": "warning",
                "freshness": "stale",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/old.json",
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "new.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "provider": "codegraph",
                "workflow_gate": "ready",
                "freshness": "fresh",
                "freshness_basis": "codegraph_status",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/new.json",
                "summary_ref": "tmp/validation/code-intelligence/nightly-1/new.md",
                "index_summary": {"files": 10, "nodes": 20, "edges": 30},
            }
        ),
        encoding="utf-8",
    )

    payload = adapter.latest_codegraph_freshness(tmp_path)

    assert payload["schema_version"] == "aistock_codegraph_latest_freshness_v1"
    assert payload["workflow_gate"] == "ready"
    assert payload["latest"]["freshness"] == "fresh"
    assert payload["latest"]["artifact_path"].endswith("new.json")
    assert payload["latest"]["index_summary"]["nodes"] == 20


def test_latest_codegraph_freshness_uses_live_status_when_artifact_is_stale(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "nightly-1"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "stale.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "provider": "codegraph",
                "workflow_gate": "warning",
                "freshness": "stale",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/stale.json",
            }
        ),
        encoding="utf-8",
    )
    live_status = {
        "available": True,
        "index_exists": True,
        "status": "ok",
        "status_check": {"ok": True},
        "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
        "git_commit": "abc123",
        "graph_root": str(tmp_path),
        "graph_root_source": "current_worktree",
    }

    payload = adapter.latest_codegraph_freshness(tmp_path, live_status=live_status)

    assert payload["workflow_gate"] == "ready"
    assert payload["latest"]["freshness"] == "stale"
    assert payload["effective_source"] == "live_status"
    assert payload["effective"]["freshness"] == "fresh"
    assert payload["warnings"] == []
    assert payload["notes"]


def test_latest_codegraph_freshness_refreshes_missing_artifact_on_demand(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    (tmp_path / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    monkeypatch.setattr(
        adapter,
        "codegraph_status",
        lambda root, skip_external=False: {
            "available": True,
            "index_exists": True,
            "status": "ok",
            "status_check": {"ok": True},
            "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
            "git_commit": "abc123",
            "working_tree_dirty": False,
            "graph_root": str(tmp_path),
            "graph_root_source": "current_worktree",
            "version": "0.9.4",
        },
    )

    payload = adapter.latest_codegraph_freshness(
        tmp_path,
        refresh_if_stale=True,
        output_dir=tmp_path / "tmp" / "validation" / "code-intelligence" / "latest",
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["refreshed"] is True
    assert payload["effective_source"] == "refreshed_artifact"
    assert payload["effective"]["freshness"] == "fresh"
    assert (tmp_path / "tmp" / "validation" / "code-intelligence" / "latest" / "codegraph-freshness.json").exists()


def test_code_intelligence_run_manifest_points_agents_to_uploaded_artifact(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "12345"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "codegraph-freshness.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "workflow_gate": "ready",
                "freshness": "fresh",
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "codegraph-freshness.md").write_text("## CodeGraph Freshness\n", encoding="utf-8")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_code_intelligence_run_manifest(
        root=tmp_path,
        output_dir=artifact_dir,
        artifact_name="code-intelligence-12345",
        run_id="12345",
        run_url="https://github.com/licong01-cloud/AIstock/actions/runs/12345",
        branch="main",
        commit="abc123",
    )

    assert payload["schema_version"] == "aistock_code_intelligence_run_manifest_v1"
    assert payload["workflow_gate"] == "ready"
    assert payload["artifact_name"] == "code-intelligence-12345"
    assert payload["download"]["gh_command"] == (
        "gh run download 12345 --repo licong01-cloud/AIstock "
        "-n code-intelligence-12345 -D tmp/validation/code-intelligence/downloaded/12345"
    )
    assert payload["download"]["local_latest_freshness_command"] == "python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale"
    assert payload["consumable_refs"]["codegraph_freshness_json"].endswith("codegraph-freshness.json")
    assert (tmp_path / payload["artifact_path"]).exists()
    markdown = (tmp_path / payload["summary_ref"]).read_text(encoding="utf-8")
    assert "Code Intelligence Run Manifest" in markdown
    assert "Agent Consumption" in markdown


def test_code_intelligence_run_manifest_warns_without_freshness_json(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "missing"
    artifact_dir.mkdir(parents=True)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_code_intelligence_run_manifest(root=tmp_path, output_dir=artifact_dir, run_id="999")

    assert payload["workflow_gate"] == "warning"
    assert payload["blocking_for_issue_workflow"] is False
    assert payload["consumable_refs"]["codegraph_freshness_json"] is None
    assert payload["warnings"]


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
    assert "latest_freshness" in markdown
    assert "latest-freshness --refresh-if-stale" in markdown
    assert "affected_quality" in markdown
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


def test_understand_anything_summary_marks_ancestor_graph_as_base_current(tmp_path: Path, monkeypatch) -> None:
    graph_path = tmp_path / ".understand-anything" / "knowledge-graph.json"
    graph_path.parent.mkdir(parents=True)
    graph_path.write_text(
        json.dumps(
            {
                "project": {"gitCommitHash": "base123", "analyzedAt": "2026-06-06T00:00:00Z"},
                "nodes": [{"id": "validation.workflow", "label": "validation workflow"}],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "feature456", "dirty": False, "dirty_count": 0},
    )
    monkeypatch.setattr(adapter, "_git_commit_is_ancestor", lambda root, ancestor, descendant: True)

    payload = adapter.build_understand_anything_summary(module="validation", root=tmp_path)
    manifest = adapter.build_understand_anything_summary_manifest(modules=["validation"], root=tmp_path)

    assert payload["freshness"] == "base_current"
    assert payload["status"] == "ok"
    assert payload["graph_commit"] == "base123"
    assert payload["current_git_commit"] == "feature456"
    assert manifest["workflow_gate"] == "ready"
    assert manifest["summary_refs"][0]["freshness"] == "base_current"


def test_configure_understand_anything_writes_config_and_ignore(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "home"
    monkeypatch.setenv("USERPROFILE", str(home))

    payload = adapter.configure_understand_anything(root=tmp_path, language="zh", auto_update=False)

    assert payload["workflow_gate"] == "configured"
    config = json.loads((tmp_path / payload["config_path"]).read_text(encoding="utf-8"))
    assert config["outputLanguage"] == "zh"
    assert config["autoUpdate"] is False
    assert (tmp_path / payload["understandignore_path"]).exists()


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

