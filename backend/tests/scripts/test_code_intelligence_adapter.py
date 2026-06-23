from __future__ import annotations

import json
from pathlib import Path

import scripts.code_intelligence_adapter as adapter


def test_emit_dash_writes_stdout_without_dash_file(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    adapter._emit({"ok": True}, "-")

    assert json.loads(capsys.readouterr().out) == {"ok": True}
    assert not (tmp_path / "-").exists()


def test_freshness_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_codegraph_freshness_artifact",
        lambda **kwargs: {
            "workflow_gate": "ready",
            "freshness": "fresh",
            "freshness_basis": "codegraph_status",
            "git_commit": "abc123",
            "warnings": [],
            "summary_ref": "tmp/validation/code-intelligence/codegraph-freshness.md",
            "artifact_path": "tmp/validation/code-intelligence/codegraph-freshness.json",
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(["freshness", "--root", str(tmp_path)])
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS codegraph-freshness ")
    assert "artifact_path=" in stdout
    assert "selected_nodes" not in stdout


def test_latest_freshness_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "latest_codegraph_freshness",
        lambda **kwargs: {
            "workflow_gate": "ready",
            "current_git_commit": "abc123",
            "effective_source": "artifact",
            "stale_metadata_warning": False,
            "refreshed": False,
            "latest": {"git_commit": "abc123", "artifact_path": "tmp/validation/code-intelligence/latest.json"},
            "effective": {"freshness": "fresh", "artifact_path": "tmp/validation/code-intelligence/latest.json"},
            "warnings": [],
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(["latest-freshness", "--root", str(tmp_path)])
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS latest-freshness ")
    assert "effective=fresh" in stdout
    assert "stale_metadata_warning=false" in stdout
    assert "selected_nodes" not in stdout


def test_verify_clients_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_client_verification",
        lambda **kwargs: {
            "workflow_gate": "ready",
            "artifact_path": "tmp/validation/code-intelligence/client-verification.json",
            "codegraph": {"status": "ok"},
            "freshness": {"effective_freshness": "fresh"},
            "understand_anything": {"status": "available", "freshness": "base_current"},
            "clients": {"codex": {"status": "ready"}, "claude": {"status": "ready"}},
            "artifacts": {
                "context_ref": "tmp/issue_workflow/VERIFY/codegraph-context.md",
                "affected_tests_ref": "tmp/issue_workflow/VERIFY/affected-tests.json",
                "ua_summary_ref": "tmp/validation/code-intelligence/ua-validation-summary.md",
            },
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(["verify-clients", "--item-id", "VERIFY", "--module", "validation", "--root", str(tmp_path)])
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS verify-clients ")
    assert "clients_ready=2/2" in stdout
    assert "context_ref=" in stdout
    assert "selected_nodes" not in stdout


def test_doctor_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_doctor_report",
        lambda root, skip_external=False: {
            "workflow_gate": "ready",
            "warnings": [],
            "blocking": [],
            "codegraph": {"status": "ok", "index_exists": True},
            "codegraph_freshness": {"effective": {"freshness": "fresh"}},
            "understand_anything": {"status": "available", "freshness": "base_current"},
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(["doctor", "--root", str(tmp_path)])
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS code-intelligence-doctor ")
    assert "codegraph=ok" in stdout
    assert "effective=fresh" in stdout
    assert "selected_nodes" not in stdout


def test_readiness_command_full_json_is_explicit(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "latest_codegraph_freshness",
        lambda **kwargs: {
            "workflow_gate": "ready",
            "current_git_commit": "abc123",
            "effective_source": "artifact",
            "stale_metadata_warning": False,
            "refreshed": False,
            "latest": {"git_commit": "abc123"},
            "effective": {"freshness": "fresh"},
            "selected_nodes": [{"id": "explicit-json"}],
        },
    )

    result = adapter.main(["latest-freshness", "--root", str(tmp_path), "--output-format", "full-json"])
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["selected_nodes"][0]["id"] == "explicit-json"


def test_doctor_command_full_json_is_explicit(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_doctor_report",
        lambda root, skip_external=False: {
            "workflow_gate": "ready",
            "warnings": [],
            "blocking": [],
            "codegraph": {"status": "ok", "index_exists": True},
            "codegraph_freshness": {"effective": {"freshness": "fresh"}},
            "understand_anything": {"status": "available", "freshness": "base_current"},
            "selected_nodes": [{"id": "explicit-json"}],
        },
    )

    result = adapter.main(["doctor", "--root", str(tmp_path), "--output-format", "full-json"])
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["selected_nodes"][0]["id"] == "explicit-json"


def test_context_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_context_artifacts",
        lambda **kwargs: {
            "status": "ok",
            "changed_files": ["scripts/code_intelligence_adapter.py"],
            "context_markdown": "tmp/issue_workflow/VERIFY/codegraph-context.md",
            "context_quality": {"quality": "scoped"},
            "fallback": {"used": False},
            "graph_root_source": "current_worktree",
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(
        [
            "context",
            "--item-id",
            "VERIFY",
            "--query",
            "workflow",
            "--changed-file",
            "scripts/code_intelligence_adapter.py",
            "--root",
            str(tmp_path),
        ]
    )
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS codegraph-context ")
    assert "context_ref=" in stdout
    assert "scoped_fallback=false" in stdout
    assert "selected_nodes" not in stdout


def test_affected_tests_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_affected_tests_artifact",
        lambda **kwargs: {
            "status": "ok",
            "quality": "partial_codegraph_plus_repo_fallback",
            "changed_files": ["scripts/code_intelligence_adapter.py"],
            "suggested_tests": ["backend/tests/scripts/test_code_intelligence_adapter.py"],
            "artifact_path": "tmp/issue_workflow/VERIFY/affected-tests.json",
            "fallback": {"used": False},
            "graph_root_source": "current_worktree",
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(
        [
            "affected-tests",
            "--item-id",
            "VERIFY",
            "--changed-file",
            "scripts/code_intelligence_adapter.py",
            "--root",
            str(tmp_path),
        ]
    )
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS codegraph-affected-tests ")
    assert "suggested_tests=1" in stdout
    assert "selected_nodes" not in stdout


def test_summary_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_summary",
        lambda **kwargs: {
            "status": "ok",
            "latest_freshness": "fresh",
            "affected_tests_count": 1,
            "context_ref": "tmp/issue_workflow/VERIFY/codegraph-context.md",
            "affected_tests_ref": "tmp/issue_workflow/VERIFY/affected-tests.json",
            "understand_anything_summary_ref": "tmp/issue_workflow/VERIFY/ua-validation-summary.md",
            "fallback_used": False,
            "stale_metadata_warning": False,
            "selected_nodes": [{"id": "should-not-inline"}],
        },
    )

    result = adapter.main(
        [
            "summary",
            "--item-id",
            "VERIFY",
            "--query",
            "workflow",
            "--changed-file",
            "scripts/code_intelligence_adapter.py",
            "--module",
            "validation",
            "--root",
            str(tmp_path),
        ]
    )
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS code-intelligence-summary ")
    assert "affected_tests=1" in stdout
    assert "selected_nodes" not in stdout


def test_summary_command_sanitizes_changed_files_file(tmp_path: Path, monkeypatch, capsys) -> None:
    changed_file = tmp_path / "changed.txt"
    changed_file.write_text(
        "\ufeffChanges:\n"
        "+++ b/scripts/code_intelligence_adapter.py\n"
        "scripts/code_intelligence_adapter.py\n"
        "F:/Dev/AIstock/scripts/nightly_adaptive_scheduler.py\n",
        encoding="utf-8",
    )
    observed: dict[str, object] = {}

    def fake_build_summary(**kwargs):
        observed.update(kwargs)
        return {
            "status": "ok",
            "latest_freshness": "fresh",
            "affected_tests_count": 0,
            "fallback_used": False,
            "stale_metadata_warning": False,
        }

    monkeypatch.setattr(adapter, "build_summary", fake_build_summary)

    result = adapter.main(
        [
            "summary",
            "--item-id",
            "VERIFY",
            "--query",
            "workflow",
            "--changed-files-file",
            str(changed_file),
            "--root",
            str(tmp_path),
        ]
    )

    assert result == 0
    assert capsys.readouterr().out.startswith("PASS code-intelligence-summary ")
    assert observed["changed_files"] == ["scripts/code_intelligence_adapter.py"]


def test_summary_command_full_json_is_explicit(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "build_summary",
        lambda **kwargs: {
            "status": "ok",
            "latest_freshness": "fresh",
            "selected_nodes": [{"id": "explicit-json"}],
        },
    )

    result = adapter.main(
        [
            "summary",
            "--item-id",
            "VERIFY",
            "--query",
            "workflow",
            "--root",
            str(tmp_path),
            "--output-format",
            "full-json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["selected_nodes"][0]["id"] == "explicit-json"


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


def test_affected_tests_maps_scripts_to_matching_backend_script_tests(
    tmp_path: Path,
    monkeypatch,
) -> None:
    script = tmp_path / "scripts" / "nightly_adaptive_scheduler.py"
    test_file = tmp_path / "backend" / "tests" / "scripts" / "test_nightly_adaptive_scheduler.py"
    script.parent.mkdir(parents=True)
    test_file.parent.mkdir(parents=True)
    script.write_text("def build_report():\n    return {}\n", encoding="utf-8")
    test_file.write_text("def test_scheduler():\n    assert True\n", encoding="utf-8")
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
        item_id="BUG-313",
        changed_files=["scripts/nightly_adaptive_scheduler.py"],
        root=tmp_path,
    )

    assert payload["suggested_tests"] == ["backend/tests/scripts/test_nightly_adaptive_scheduler.py"]
    assert payload["test_discovery_fallback"]["direct_matches"] == {
        "backend/tests/scripts/test_nightly_adaptive_scheduler.py": [
            "direct:scripts/nightly_adaptive_scheduler.py"
        ]
    }
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


def test_codegraph_freshness_uses_configured_graph_source_root(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "canonical"
    workspace = tmp_path / "runner-workspace"
    source.mkdir()
    workspace.mkdir()
    (source / ".git").mkdir()
    (workspace / ".git").mkdir()
    (source / ".codegraph").mkdir()
    (source / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    for relative in (".github/workflows/nightly.yml", "scripts/code_intelligence_adapter.py"):
        (source / relative).parent.mkdir(parents=True, exist_ok=True)
        (source / relative).write_text("# indexed\n", encoding="utf-8")
        (workspace / relative).parent.mkdir(parents=True, exist_ok=True)
        (workspace / relative).write_text("# checkout\n", encoding="utf-8")
    monkeypatch.setenv(adapter.GRAPH_SOURCE_ROOT_ENV, str(source))
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(adapter, "_same_repo_remote", lambda left, right: True)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    def fake_run(args, cwd=None, timeout=30):
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "0.9.4", "stderr": ""}
        if args[:2] == ["codegraph", "status"]:
            assert args[2] == str(source)
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "Files: 10\nNodes: 20\nEdges: 30\nIndex is up to date",
                "stderr": "",
            }
        if args[:2] == ["codegraph", "files"]:
            assert args[args.index("--path") + 1] == str(source)
            pattern = args[args.index("--pattern") + 1]
            return {"ok": True, "returncode": 0, "stdout": json.dumps([{"path": pattern}]), "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.build_codegraph_freshness_artifact(
        root=workspace,
        output_dir=workspace / "tmp" / "validation" / "code-intelligence" / "run-1",
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["freshness"] == "fresh"
    assert payload["root"] == str(workspace)
    assert payload["graph_root"] == str(source)
    assert payload["graph_root_source"] == "configured_env"
    assert payload["graph_commit_relation"] == "same"
    assert payload["artifact_path"] == "tmp/validation/code-intelligence/run-1/codegraph-freshness.json"
    assert (workspace / payload["artifact_path"]).exists()


def test_configured_graph_source_accepts_git_worktree_with_local_clone_remote(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "canonical"
    workspace = tmp_path / "runner-workspace"
    source.mkdir()
    workspace.mkdir()
    (source / ".codegraph").mkdir()
    (source / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    monkeypatch.setenv(adapter.GRAPH_SOURCE_ROOT_ENV, str(source))
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")

    def fake_git(args, cwd=None, check=False):
        if args == ["config", "--get", "remote.origin.url"]:
            return {
                "ok": True,
                "stdout": str(source) if cwd == workspace else "git@github.com:licong01-cloud/AIstock.git",
            }
        if args == ["rev-parse", "--is-inside-work-tree"]:
            return {"ok": True, "stdout": "true"}
        return {"ok": False, "stdout": "", "stderr": "unexpected git"}

    monkeypatch.setattr(adapter, "_git", fake_git)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    def fake_run(args, cwd=None, timeout=30):
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "0.9.4", "stderr": ""}
        if args[:2] == ["codegraph", "status"]:
            assert args[2] == str(source)
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "Files: 10\nNodes: 20\nEdges: 30\nIndex is up to date",
                "stderr": "",
            }
        if args[:2] == ["codegraph", "files"]:
            return {"ok": True, "returncode": 0, "stdout": "[]", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    status = adapter.codegraph_status(workspace)

    assert status["graph_root"] == str(source)
    assert status["graph_root_source"] == "configured_env"
    assert status["index_exists"] is True
    assert status["graph_source_warnings"] == []


def test_codegraph_freshness_warns_when_configured_graph_source_is_not_related(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "canonical"
    workspace = tmp_path / "runner-workspace"
    source.mkdir()
    workspace.mkdir()
    (source / ".git").mkdir()
    (workspace / ".git").mkdir()
    (source / ".codegraph").mkdir()
    (source / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    monkeypatch.setenv(adapter.GRAPH_SOURCE_ROOT_ENV, str(source))
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(adapter, "_same_repo_remote", lambda left, right: True)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {
            "ok": True,
            "head": "source123" if root == source else "workspace456",
            "dirty": False,
            "dirty_count": 0,
        },
    )
    monkeypatch.setattr(adapter, "_git_commit_is_ancestor", lambda root, ancestor, descendant: False)
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

    payload = adapter.build_codegraph_freshness_artifact(root=workspace)

    assert payload["workflow_gate"] == "warning"
    assert payload["freshness"] == "stale"
    assert payload["freshness_basis"] == "configured_graph_root_commit"
    assert any("Configured CodeGraph root commit" in item for item in payload["warnings"])


def test_codegraph_freshness_warns_when_critical_file_missing_from_index(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    (tmp_path / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    for relative in ("scripts/code_intelligence_adapter.py", "scripts/llm_provider_adapter.py"):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# critical\n", encoding="utf-8")
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    def fake_run(args, cwd=None, timeout=30):
        if args == ["codegraph", "--version"]:
            return {"ok": True, "returncode": 0, "stdout": "0.9.4", "stderr": ""}
        if args[:2] == ["codegraph", "status"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "Files: 10\nNodes: 20\nEdges: 30\nIndex is up to date",
                "stderr": "",
            }
        if args[:2] == ["codegraph", "files"]:
            pattern = args[args.index("--pattern") + 1]
            rows = [{"path": pattern}] if pattern == "scripts/code_intelligence_adapter.py" else []
            return {"ok": True, "returncode": 0, "stdout": json.dumps(rows), "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(adapter, "_run_command", fake_run)

    payload = adapter.build_codegraph_freshness_artifact(root=tmp_path, max_age_hours=36)

    assert payload["workflow_gate"] == "warning"
    assert payload["freshness"] == "incomplete_index"
    assert payload["freshness_basis"] == "critical_file_coverage"
    assert payload["index_file_coverage"]["missing_files"] == ["scripts/llm_provider_adapter.py"]
    assert any("missing critical workflow files" in item for item in payload["warnings"])


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


def test_latest_codegraph_freshness_marks_live_incomplete_index(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".codegraph").mkdir()
    (tmp_path / ".codegraph" / "codegraph.db").write_text("db", encoding="utf-8")
    critical = tmp_path / "scripts" / "llm_provider_adapter.py"
    critical.parent.mkdir(parents=True)
    critical.write_text("# critical\n", encoding="utf-8")
    live_status = {
        "available": True,
        "index_exists": True,
        "command": "codegraph",
        "graph_root": str(tmp_path),
        "graph_root_source": "current_worktree",
        "git_commit": "new456",
        "status": "ok",
        "status_check": {"ok": True},
        "index_summary": {"up_to_date": True},
    }
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "new456", "dirty": False, "dirty_count": 0},
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {"ok": True, "returncode": 0, "stdout": "[]", "stderr": ""},
    )

    payload = adapter.latest_codegraph_freshness(tmp_path, live_status=live_status)

    assert payload["workflow_gate"] == "warning"
    assert payload["effective"]["freshness"] == "incomplete_index"
    assert payload["effective"]["index_file_coverage"]["missing_files"] == ["scripts/llm_provider_adapter.py"]


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


def test_latest_codegraph_freshness_persists_live_current_head_when_artifact_metadata_is_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "nightly-1"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "stale-metadata.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "provider": "codegraph",
                "workflow_gate": "ready",
                "freshness": "fresh",
                "git_commit": "old123",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/stale-metadata.json",
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
        "git_commit": "new456",
        "graph_root": str(tmp_path),
        "graph_root_source": "current_worktree",
    }
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "new456", "dirty": False, "dirty_count": 0},
    )
    monkeypatch.setattr(
        adapter,
        "_codegraph_critical_files",
        lambda root: [],
    )

    payload = adapter.latest_codegraph_freshness(
        tmp_path,
        live_status=live_status,
        persist_effective=True,
        output_dir=tmp_path / "tmp" / "validation" / "code-intelligence" / "latest",
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["effective_source"] == "persisted_effective_artifact"
    assert payload["refreshed"] is True
    assert payload["stale_metadata_warning"] is False
    assert payload["effective"]["git_commit"] == "new456"
    persisted = tmp_path / "tmp" / "validation" / "code-intelligence" / "latest" / "codegraph-freshness.json"
    assert persisted.exists()
    assert json.loads(persisted.read_text(encoding="utf-8"))["git_commit"] == "new456"


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


def test_llm_value_summary_renders_human_readable_evidence(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "12345"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "codegraph-freshness.json").write_text(
        json.dumps(
            {
                "workflow_gate": "ready",
                "freshness": "fresh",
                "status": "ok",
                "index_summary": {"files": 12, "nodes": 34, "edges": 56},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "codegraph-freshness.md").write_text("## CodeGraph Freshness\n", encoding="utf-8")
    (artifact_dir / "code-intelligence-summary.json").write_text(
        json.dumps(
            {
                "understand_anything_status": "available",
                "understand_anything_summary_ref": "tmp/validation/code-intelligence/12345/ua-validation-summary.md",
                "context_ref": "tmp/issue_workflow/nightly-12345/codegraph-context.md",
                "affected_tests_ref": "tmp/issue_workflow/nightly-12345/affected-tests.json",
                "context": {"context_quality": {"broad_scan_required": False}},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "ua-summary-manifest.json").write_text(
        json.dumps(
            {
                "summary_refs": [
                    {"module": "validation", "freshness": "base_current"},
                    {"module": "issue_workflow", "freshness": "base_current"},
                ]
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "llm-nightly-adaptive-scheduler.json").write_text(
        json.dumps(
            {
                "workflow_gate": "ready",
                "provider": "deepseek_api",
                "model": "deepseek-v4-pro",
                "llm_invoked": True,
                "llm_invocation_evidence": {"invoked": True, "fallback_used": False},
                "queue_summary": {"allowed_plan_keys": ["l0", "validation_module_registry_l0"]},
                "advice_consumption": {"advice_consumed": True},
                "issue_creation_policy": {"mode": "warning_only_advice"},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "llm-hypotheses.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_llm_discovery_hypothesis_v1",
                "provider": "deepseek_api",
                "model": "deepseek-v4-pro",
                "llm_invoked": True,
                "llm_invocation_evidence": {"invoked": True, "fallback_used": False},
                "rotation": {"selected_plan_keys": ["workflow_discovery_root_clean_guard"]},
                "hypotheses": [{"id": "H-001", "recommended_plan_keys": ["validation_catalog_integrity"]}],
                "selected_plan_keys": ["validation_catalog_integrity"],
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "selected-plans.json").write_text(
        json.dumps({"selected_plan_keys": ["validation_catalog_integrity"]}),
        encoding="utf-8",
    )
    discovery_dir = artifact_dir / "discovery-plans"
    discovery_dir.mkdir()
    (discovery_dir / "manifest.json").write_text(
        json.dumps({"summary": {"executed_count": 1, "anomaly_count": 0}}),
        encoding="utf-8",
    )
    bug_candidate_dir = artifact_dir / "bug-candidates"
    bug_candidate_dir.mkdir()
    (bug_candidate_dir / "manifest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "candidate_count": 2,
                    "high_value_candidate_count": 1,
                    "issue_payload_ready_count": 1,
                    "accepted_count": 1,
                    "rejected_count": 0,
                    "closed_count": 0,
                    "no_candidate_reason": "no_high_value_actionable_candidates",
                }
            }
        ),
        encoding="utf-8",
    )
    (bug_candidate_dir / "candidate-summary.md").write_text("## Nightly BugCandidate Queue\n", encoding="utf-8")
    (artifact_dir / "llm-prompt-evaluation.json").write_text(
        json.dumps(
            {
                "workflow_gate": "passed",
                "case_count": 20,
                "issue_body_completeness": 1.0,
                "false_positive_auto_file_rate": 0.0,
                "plan_recommendation_accuracy": 1.0,
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "llm-guarded-rollout-gate.json").write_text(
        json.dumps(
            {
                "workflow_gate": "warning",
                "mode": "warning_only",
                "auto_file_allowed": False,
                "llm_can_enhance_issue": True,
                "llm_enhancement_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "design-drift-audit.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_nightly_design_drift_audit_v1",
                "workflow_gate": "warning",
                "candidate_only": True,
                "manual_analysis_required_before_bug_registration": True,
                "llm_invoked": True,
                "summary": {"review_target_count": 5, "finding_count": 1},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "design-drift-audit.md").write_text("# Nightly LLM Design Drift Audit\n", encoding="utf-8")
    (artifact_dir / "silent-degradation-audit.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_nightly_silent_degradation_audit_v1",
                "workflow_gate": "warning",
                "candidate_only": True,
                "manual_analysis_required_before_bug_registration": True,
                "llm_invoked": True,
                "summary": {"review_target_count": 6, "finding_count": 2},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "silent-degradation-audit.md").write_text(
        "# Nightly LLM Silent Degradation Audit\n",
        encoding="utf-8",
    )

    payload = adapter.build_llm_value_summary(root=tmp_path, artifact_dir=artifact_dir)
    markdown = adapter.render_llm_value_summary_markdown(payload)

    assert payload["workflow_gate"] == "ready"
    assert payload["llm"]["llm_invoked"] is True
    assert payload["llm"]["allowed_plan_keys"] == ["l0", "validation_module_registry_l0"]
    assert payload["llm"]["discovery_hypothesis_count"] == 1
    assert payload["llm"]["selected_plan_count"] == 1
    assert payload["llm"]["bug_candidate_count"] == 2
    assert payload["llm"]["bug_candidate_issue_payload_count"] == 1
    assert payload["value_metrics"]["llm_advice_generated"] is True
    assert payload["value_metrics"]["llm_advice_consumed"] is True
    assert payload["value_metrics"]["llm_advice_changed_plan"] is True
    assert payload["value_metrics"]["codegraph_refs_used"] >= 2
    assert payload["value_metrics"]["ua_refs_used"] >= 1
    assert payload["value_metrics"]["broad_scan_avoided"] is True
    assert payload["value_metrics"]["high_value_candidates"] == 1
    assert payload["value_metrics"]["candidate_feedback"]["accepted_count"] == 1
    assert payload["value_metrics"]["candidate_feedback"]["no_candidate_reason"] == "no_high_value_actionable_candidates"
    assert payload["value_metrics"]["candidate_feedback"]["placeholders_present"] is True
    assert payload["design_drift_audit"]["candidate_only"] is True
    assert payload["design_drift_audit"]["finding_count"] == 1
    assert payload["silent_degradation_audit"]["candidate_only"] is True
    assert payload["silent_degradation_audit"]["finding_count"] == 2
    assert payload["understand_anything"]["summary_count"] == 2
    assert payload["understand_anything"]["manifest_freshness"] == "base_current"
    assert payload["understand_anything"]["base_current_summary_count"] == 2
    assert "LLM + Code Intelligence Value" in markdown
    assert "llm_provider: `deepseek_api`" in markdown
    assert "understand_anything: `available` freshness=`base_current` summaries=`2`" in markdown
    assert "allowed_plan_keys: `l0,validation_module_registry_l0`" in markdown
    assert "advice_changed_plan: `True`" in markdown
    assert "graph_refs: `codegraph=" in markdown
    assert "discovery_hypotheses: `hypotheses=1, selected_plans=1`" in markdown
    assert "discovery_plans: `executed=1, anomalies=0`" in markdown
    assert "bug_candidates: `candidates=2, high_value=1, issue_payload_drafts=1`" in markdown
    assert "candidate_feedback: `available=True, accepted=1, rejected=0, closed=0, pending=1`" in markdown
    assert "design_drift_audit: `targets=5, findings=1, candidate_only=True`" in markdown
    assert "silent_degradation_audit: `targets=6, findings=2, candidate_only=True`" in markdown
    assert "candidate_no_issue_reason: `no_high_value_actionable_candidates`" in markdown
    assert "bug-candidates/manifest.json" in markdown
    assert "selected-plans.json" in markdown
    assert "design-drift-audit.md" in markdown
    assert "silent-degradation-audit.md" in markdown
    assert "Raw JSON artifacts stay in the uploaded artifact bundle" in markdown


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


def test_ua_summary_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    graph_path = tmp_path / ".understand-anything" / "knowledge-graph.json"
    graph_path.parent.mkdir(parents=True)
    graph_path.write_text(
        json.dumps(
            {
                "nodes": [
                    {"id": "validation.workflow", "label": "validation workflow"},
                    {"id": "validation.runner", "label": "validation runner"},
                ],
                "edges": [{"source": "validation.workflow", "target": "validation.runner", "type": "calls"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    result = adapter.main(["ua-summary", "--module", "validation", "--root", str(tmp_path)])
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS ua-summary ")
    assert "summary_ref=" in stdout
    assert "artifact_path=" in stdout
    assert "selected_nodes" not in stdout
    assert "selected_edges" not in stdout


def test_ua_summary_command_full_json_is_explicit(tmp_path: Path, monkeypatch, capsys) -> None:
    graph_path = tmp_path / ".understand-anything" / "knowledge-graph.json"
    graph_path.parent.mkdir(parents=True)
    graph_path.write_text(
        json.dumps({"nodes": [{"id": "validation.workflow", "label": "validation workflow"}], "edges": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    result = adapter.main(["ua-summary", "--module", "validation", "--root", str(tmp_path), "--output-format", "full-json"])
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["schema_version"] == "aistock_understand_anything_summary_v1"
    assert payload["selected_nodes"][0]["id"] == "validation.workflow"


def test_ua_summary_all_command_defaults_to_compact_stdout(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )
    monkeypatch.setattr(
        adapter,
        "build_understand_anything_summary",
        lambda **kwargs: {
            "module": kwargs["module"],
            "status": "ok",
            "freshness": "fresh",
            "summary_ref": f"tmp/validation/code-intelligence/ua-{kwargs['module']}-summary.md",
            "artifact_path": f"tmp/validation/code-intelligence/ua-{kwargs['module']}-summary.json",
        },
    )

    result = adapter.main(["ua-summary-all", "--module", "validation", "--root", str(tmp_path)])
    stdout = capsys.readouterr().out

    assert result == 0
    assert stdout.startswith("PASS ua-summary-all ")
    assert "modules=1" in stdout
    assert "artifact_path=" in stdout
    assert "summary_refs" not in stdout


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


def test_pr_quality_runner_reports_artifact_fallback_not_local_misconfiguration(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(adapter, "_codegraph_command", lambda: None)
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.build_summary(
        item_id="PR-887",
        query="PR impact",
        changed_files=["scripts/code_intelligence_adapter.py"],
        root=tmp_path,
        skip_external=True,
    )
    markdown = adapter.render_summary_markdown(payload)

    assert payload["runner_context"] == "github_actions"
    assert payload["context"]["fallback"]["reason"] == "runner_artifact_unavailable"
    assert payload["affected_tests"]["fallback"]["reason"] == "runner_artifact_unavailable"
    assert payload["understand_anything"]["status"] == "runner_artifact_unavailable"
    assert payload["understand_anything"]["blocking_for_issue_workflow"] is False
    assert "runner_context: `github_actions`" in markdown
    assert "runner_artifact_unavailable" in markdown
    assert "not_configured" not in markdown
    assert len(markdown) < 4000


def test_pr_quality_runner_can_reference_ua_summary_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "latest"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "ua-summary-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_understand_anything_summary_manifest_v1",
                "generated_at": "2026-06-10T00:00:00Z",
                "workflow_gate": "ready",
                "blocking_for_issue_workflow": False,
                "summary_refs": [
                    {
                        "module": "validation",
                        "summary_ref": "tmp/validation/code-intelligence/latest/ua-validation-summary.md",
                        "freshness": "base_current",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.understand_anything_status(tmp_path, skip_external=True, runner_artifact_mode=True)

    assert payload["status"] == "runner_artifact_available"
    assert payload["latest_summary_manifest"]["artifact_path"].endswith("ua-summary-manifest.json")
    assert payload["latest_summary_manifest"]["summary_refs"][0]["module"] == "validation"


def test_understand_anything_status_falls_back_to_graph_freshness_for_legacy_summary_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "latest"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "ua-summary-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_understand_anything_summary_manifest_v1",
                "generated_at": "2026-06-10T00:00:00Z",
                "workflow_gate": "ready",
                "summary_refs": [{"module": "validation", "summary_ref": "ua-validation-summary.md"}],
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

    payload = adapter.understand_anything_status(tmp_path, skip_external=True)

    assert payload["freshness"] == "base_current"
    assert payload["latest_summary_manifest_freshness"] == "base_current"


def test_context_quality_flags_noisy_context_without_requiring_broad_scan(tmp_path: Path, monkeypatch) -> None:
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
            "graph_root": str(tmp_path),
            "graph_root_source": "current_worktree",
            "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
        },
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {
            "ok": True,
            "returncode": 0,
            "stdout": "unrelated frontend component and strategy package symbol",
            "stderr": "",
        },
    )

    payload = adapter.build_context_artifacts(
        item_id="BUG-308",
        query="issue workflow merge-finalizer cleanup sync root",
        changed_files=["scripts/aistock_issue_workflow.py"],
        root=tmp_path,
    )

    assert payload["status"] == "ok"
    assert payload["context_quality"]["quality"] == "scoped_fallback"
    assert payload["context_quality"]["matched_changed_files"] == ["scripts/aistock_issue_workflow.py"]
    assert payload["context_quality"]["scoped_fallback_inserted"] is True
    assert payload["context_quality"]["noisy_context_warning"] is False
    assert payload["context_quality"]["broad_scan_required"] is False
    markdown = (tmp_path / payload["context_markdown"]).read_text(encoding="utf-8")
    assert "Code Intelligence Context Guidance" in markdown
    assert "## Scoped Changed-File Context" in markdown


def test_context_inserts_scoped_changed_file_context_before_noisy_graph_hits(
    tmp_path: Path,
    monkeypatch,
) -> None:
    changed = tmp_path / "scripts" / "code_intelligence_adapter.py"
    changed.parent.mkdir(parents=True)
    changed.write_text(
        "class CodeIntelligenceError(ValueError):\n"
        "    pass\n\n"
        "def build_context_artifacts():\n"
        "    return {}\n",
        encoding="utf-8",
    )
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
            "graph_root": str(tmp_path),
            "graph_root_source": "current_worktree",
            "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
        },
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {
            "ok": True,
            "returncode": 0,
            "stdout": "unrelated qlib workflow symbol",
            "stderr": "",
        },
    )

    payload = adapter.build_context_artifacts(
        item_id="BUG-325",
        query="workflow compact output smoke",
        changed_files=["scripts/code_intelligence_adapter.py"],
        root=tmp_path,
    )

    quality = payload["context_quality"]
    assert payload["status"] == "ok"
    assert payload["channel"] == "scoped_fallback"
    assert quality["quality"] == "scoped_fallback"
    assert quality["matched_changed_files"] == ["scripts/code_intelligence_adapter.py"]
    assert quality["scoped_fallback_inserted"] is True
    assert quality["noisy_context_warning"] is False
    assert payload["scoped_file_context"]["enabled"] is True
    assert payload["scoped_file_context"]["outlines"][0]["line_count"] == 5
    markdown = (tmp_path / payload["context_markdown"]).read_text(encoding="utf-8")
    assert markdown.index("## Scoped Changed-File Context") < markdown.index("unrelated qlib workflow symbol")
    assert "scripts/code_intelligence_adapter.py" in markdown
    assert "`CodeIntelligenceError:1`" in markdown
    assert "`build_context_artifacts:4`" in markdown


def test_latest_freshness_marks_stale_metadata_warning_without_blocking(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "nightly-1"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "fresh.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "provider": "codegraph",
                "workflow_gate": "ready",
                "freshness": "fresh",
                "git_commit": "old123",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/fresh.json",
                "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "new456", "dirty": False, "dirty_count": 0},
    )

    payload = adapter.latest_codegraph_freshness(tmp_path)

    assert payload["workflow_gate"] == "warning"
    assert payload["blocking_for_issue_workflow"] is False
    assert payload["effective"]["freshness"] == "fresh"
    assert payload["stale_metadata_warning"] is True
    assert any("differs from current HEAD" in item for item in payload["warnings"])


def test_latest_freshness_can_persist_effective_fresh_artifact(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "nightly-1"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "fresh.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "provider": "codegraph",
                "workflow_gate": "ready",
                "freshness": "fresh",
                "git_commit": "old123",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/fresh.json",
                "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "_git_snapshot",
        lambda root: {"ok": True, "head": "new456", "dirty": False, "dirty_count": 0},
    )
    live_status = {
        "available": True,
        "index_exists": True,
        "status": "ok",
        "status_check": {"ok": True},
        "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
        "git_commit": "new456",
        "graph_root": str(tmp_path),
        "graph_root_source": "current_worktree",
    }

    payload = adapter.latest_codegraph_freshness(
        tmp_path,
        live_status=live_status,
        persist_effective=True,
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["refreshed"] is True
    assert payload["effective_source"] == "persisted_effective_artifact"
    assert payload["stale_metadata_warning"] is False
    assert payload["latest"]["git_commit"] == "new456"
    assert (tmp_path / "tmp" / "validation" / "code-intelligence" / "latest" / "codegraph-freshness.json").exists()


def test_verify_clients_produces_compact_warning_only_evidence(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    codex_skill = home / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md"
    claude_command = home / ".claude" / "commands" / "fix-aistock-issue.md"
    ua_skill = home / ".understand-anything" / "repo" / "understand-anything-plugin" / "skills" / "understand" / "SKILL.md"
    ua_chat = (
        home
        / ".understand-anything"
        / "repo"
        / "understand-anything-plugin"
        / "skills"
        / "understand-chat"
        / "SKILL.md"
    )
    for path in (codex_skill, claude_command, ua_skill, ua_chat):
        path.parent.mkdir(parents=True)
        path.write_text("graph-first Code Intelligence aistock_issue_workflow.py understand", encoding="utf-8")
    monkeypatch.setenv("USERPROFILE", str(home))
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
    artifact_dir = tmp_path / "tmp" / "validation" / "code-intelligence" / "nightly-1"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "fresh.json").write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "generated_at": "2026-06-04T00:00:00Z",
                "workflow_gate": "ready",
                "freshness": "fresh",
                "git_commit": "base123",
                "artifact_path": "tmp/validation/code-intelligence/nightly-1/fresh.json",
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
    monkeypatch.setattr(
        adapter,
        "codegraph_status",
        lambda root, skip_external=False: {
            "available": True,
            "index_exists": True,
            "command": "codegraph",
            "version": "0.9.4",
            "status": "ok",
            "status_check": {"ok": True},
            "git_commit": "feature456",
            "working_tree_dirty": False,
            "graph_root": str(tmp_path),
            "graph_root_source": "current_worktree",
            "index_summary": {"files": 10, "nodes": 20, "edges": 30, "up_to_date": True},
        },
    )
    monkeypatch.setattr(
        adapter,
        "_run_command",
        lambda args, cwd=None, timeout=30: {
            "ok": True,
            "returncode": 0,
            "stdout": "unrelated validation dashboard symbol"
            if args[1] == "context"
            else "backend/tests/scripts/test_code_intelligence_adapter.py",
            "stderr": "",
        },
    )

    payload = adapter.build_client_verification(
        item_id="BUG-308",
        query="issue workflow merge-finalizer cleanup sync root",
        changed_files=["scripts/code_intelligence_adapter.py"],
        module="validation",
        root=tmp_path,
    )
    markdown = adapter.render_client_verification_summary(payload)

    assert payload["schema_version"] == "aistock_code_intelligence_client_verification_v1"
    assert payload["workflow_gate"] == "ready"
    assert payload["freshness"]["stale_metadata_warning"] is False
    assert payload["freshness"]["effective_source"] == "persisted_effective_artifact"
    assert payload["context"]["noisy_context_warning"] is False
    assert payload["context"]["scoped_fallback_inserted"] is True
    assert payload["context"]["broad_scan_required"] is False
    assert payload["clients"]["codex_issue_skill"]["status"] == "ready"
    assert payload["clients"]["claude_issue_command"]["status"] == "ready"
    assert payload["understand_anything"]["stale_but_usable"] is True
    assert not any("Understand Anything graph freshness is base_current" in item for item in payload["warnings"])
    assert payload["efficiency"]["large_graph_payload_inlined"] is False
    assert "selected_nodes" not in json.dumps(payload["understand_anything"])
    assert "Code Intelligence Client Verification" in markdown
    assert len(markdown) < 3000




def test_understand_anything_summary_manifest_counts_freshness(tmp_path: Path, monkeypatch) -> None:
    def fake_summary(*, module, root=None, output_dir=None, max_nodes=None):
        return {
            "module": module,
            "status": "ok",
            "freshness": "fresh" if module == "validation" else "base_current",
            "graph_commit": "abc123",
            "current_git_commit": "abc123" if module == "validation" else "def456",
            "summary_ref": f"tmp/validation/code-intelligence/ua-{module}-summary.md",
            "artifact_path": f"tmp/validation/code-intelligence/ua-{module}-summary.json",
        }

    monkeypatch.setattr(adapter, "build_understand_anything_summary", fake_summary)

    payload = adapter.build_understand_anything_summary_manifest(
        modules=["validation", "qe"],
        root=tmp_path,
        output_dir=tmp_path / "tmp" / "validation" / "code-intelligence",
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["fresh_summary_count"] == 1
    assert payload["base_current_summary_count"] == 1
    assert payload["stale_summary_count"] == 0
