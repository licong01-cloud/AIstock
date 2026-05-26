from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

import scripts.aistock_issue_workflow as workflow


def _fake_code_intelligence_summary(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "aistock_code_intelligence_summary_v1",
        "provider": "codegraph",
        "status": "fallback",
        "context_ref": "tmp/issue_workflow/BUG-199/codegraph-context.md",
        "manifest_ref": "tmp/issue_workflow/BUG-199/code-intelligence.json",
        "affected_tests_ref": "tmp/issue_workflow/BUG-199/affected-tests.json",
        "fallback_used": True,
        "affected_tests": {"suggested_tests": []},
        "understand_anything": {"status": "not_required_missing"},
    }
    payload.update(overrides)
    return payload


def _bug(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "bug_id": "BUG-199",
        "title": "Workflow regression",
        "module": "validation.guardrails",
        "severity": "P1",
        "status": "open",
        "description": "The issue workflow should prepare a compact fix context.",
        "reproduce_command": "python scripts/aistock_issue_workflow.py --help",
        "allowed_write_scope": ["scripts/aistock_issue_workflow.py"],
        "required_verification": ["l0"],
        "closure_requirements": ["Generate a start pack.", "Generate a finish plan."],
        "non_goals": ["Do not touch production runtime services."],
        "github_issue_number": 199,
        "github_issue_url": "https://github.example/issues/199",
    }
    record.update(overrides)
    return record


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.fixture
def isolated_workflow_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(workflow, "BUGS_ROOT", tmp_path / "tests" / "aistock_validation" / "bugs")
    return tmp_path


def test_start_rejects_missing_github_linkage(isolated_workflow_root: Path) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(github_issue_number=None, github_issue_url=None))

    with pytest.raises(workflow.WorkflowError, match="missing GitHub linkage"):
        workflow.build_start_plan(
            bug_id=None,
            issue_json=str(issue),
            changed_files=[],
            create_worktree=False,
            dry_run=True,
            task_slug=None,
            allow_missing_linkage=False,
            allow_closed=False,
        )


def test_start_rejects_closed_status_by_default(isolated_workflow_root: Path) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="verified"))

    with pytest.raises(workflow.WorkflowError, match="only .* are fixable"):
        workflow.build_start_plan(
            bug_id=None,
            issue_json=str(issue),
            changed_files=[],
            create_worktree=False,
            dry_run=True,
            task_slug=None,
            allow_missing_linkage=False,
            allow_closed=False,
        )


def test_start_dry_run_returns_worktree_context_and_scope(isolated_workflow_root: Path) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

    payload = workflow.build_start_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=[],
        create_worktree=True,
        dry_run=True,
        task_slug="workflow-smoke",
        allow_missing_linkage=False,
        allow_closed=False,
    )

    assert payload["schema_version"] == "aistock_issue_workflow_start_v1"
    assert payload["bug_id"] == "BUG-199"
    assert payload["worktree_plan"]["create_worktree"] is True
    assert payload["worktree_plan"]["dry_run"] is True
    assert payload["worktree_plan"]["branch"].endswith(f"-{workflow._today_compact()}")
    assert payload["allowed_write_scope"] == ["scripts/aistock_issue_workflow.py"]
    assert "l0" in payload["required_verification"]
    assert payload["next_agent_steps"][0] == "switch_to_worktree_if_created"


def test_start_writes_fix_ready_and_context_pack(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    payload = workflow.build_start_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=[],
        create_worktree=False,
        dry_run=False,
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
    )

    fix_ready = isolated_workflow_root / payload["fix_ready_path"]
    context_json = isolated_workflow_root / payload["context_pack_json"]
    context_md = isolated_workflow_root / payload["context_pack_md"]
    assert fix_ready.exists()
    assert context_json.exists()
    context_payload = json.loads(context_json.read_text(encoding="utf-8"))
    assert context_md.read_text(encoding="utf-8").startswith("# AIstock Context Pack")
    assert context_payload["code_intelligence"]["provider"] == "codegraph"
    assert payload["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")
    assert payload["context_metrics"]["context_pack_md"]["estimated_tokens"] > 0
    assert payload["context_metrics"]["fix_ready_json"]["bytes"] > 0
    assert json.loads(fix_ready.read_text(encoding="utf-8"))["workflow_gate"] == "allowed"


def test_finish_plan_selects_validation_and_requires_evidence(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

    assert workflow.main([
        "finish",
        "--issue-json",
        str(issue),
        "--changed-file",
        "scripts/aistock_issue_workflow.py",
    ]) == 2
    missing = json.loads(capsys.readouterr().out)
    assert missing["workflow_gate"] == "validation_evidence_missing"
    assert missing["closure_ready"] is False

    assert workflow.main([
        "finish",
        "--issue-json",
        str(issue),
        "--changed-file",
        "scripts/aistock_issue_workflow.py",
        "--validation-evidence",
        "python -m nox -s l0 -> passed",
    ]) == 0
    ready = json.loads(capsys.readouterr().out)
    assert ready["workflow_gate"] == "ready_for_pr"
    assert "l0" in ready["required_verification"]
    assert ready["artifact_metrics"]["pr_body"]["estimated_tokens"] > 0
    assert (isolated_workflow_root / ready["pr_body_path"]).exists()


def test_finish_plan_only_can_draft_pr_body_without_evidence(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    monkeypatch.setattr(
        workflow,
        "_build_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(
            affected_tests={"suggested_tests": ["backend/tests/scripts/test_aistock_issue_workflow.py"]}
        ),
    )

    payload = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=[],
        plan_only=True,
        allow_missing_evidence=False,
    )

    assert payload["closure_ready"] is True
    assert payload["codegraph_suggested_tests"] == ["backend/tests/scripts/test_aistock_issue_workflow.py"]
    assert payload["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")
    pr_body = isolated_workflow_root / payload["pr_body_path"]
    pr_body_text = pr_body.read_text(encoding="utf-8")
    assert "Code intelligence" in pr_body_text
    assert "backend/tests/scripts/test_aistock_issue_workflow.py" in pr_body_text
    assert "missing - run required validation" in pr_body_text


def test_triage_p0_groups_open_issues_and_flags_missing_linkage(
    isolated_workflow_root: Path,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug(severity="P0"))
    _write_json(bugs_root / "bug200.json", _bug(bug_id="BUG-200", severity="P0", github_issue_url=None))
    _write_json(bugs_root / "bug201.json", _bug(bug_id="BUG-201", severity="P2"))
    _write_json(bugs_root / "bug202.json", _bug(bug_id="BUG-202", status="verified"))

    payload = workflow.build_triage_p0()

    assert payload["schema_version"] == "aistock_issue_workflow_triage_p0_v1"
    assert payload["count"] == 2
    assert {item["bug_id"] for item in payload["items"]} == {"BUG-199", "BUG-200"}
    missing = {item["bug_id"]: item["missing_github_linkage"] for item in payload["items"]}
    assert missing["BUG-200"] == ["github_issue_url"]
    assert payload["groups"][0]["can_batch"] is True
    assert payload["groups"][0]["suggested_branch"].startswith("bug/p0-validation-guardrails-batch-")


def test_run_p0_recommends_next_issue_command(isolated_workflow_root: Path) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug(severity="P0", module="paper_v2"))
    _write_json(bugs_root / "bug200.json", _bug(bug_id="BUG-200", severity="P0", module="validation.guardrails"))

    payload = workflow.build_run_p0_plan(module="paper_v2")

    assert payload["schema_version"] == "aistock_issue_workflow_run_p0_v1"
    assert payload["count"] == 1
    assert payload["recommended_first_issue"] == "BUG-199"
    assert "run --bug-id BUG-199" in payload["next_command"]


def test_start_batch_rejects_incompatible_modules(isolated_workflow_root: Path) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug())
    _write_json(bugs_root / "bug200.json", _bug(bug_id="BUG-200", module="paper_v2", github_issue_number=200))

    with pytest.raises(workflow.WorkflowError, match="share one module"):
        workflow.build_start_batch_plan(
            bug_ids=["BUG-199", "BUG-200"],
            create_worktree=False,
            dry_run=True,
            task_slug=None,
            allow_missing_linkage=False,
            allow_closed=False,
        )


def test_start_batch_writes_batch_state_and_contexts(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug())
    _write_json(bugs_root / "bug200.json", _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"))
    monkeypatch.setattr(workflow, "_build_batch_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary(item_id=kwargs["batch_id"]))

    payload = workflow.build_start_batch_plan(
        bug_ids=["BUG-199", "BUG-200"],
        create_worktree=False,
        dry_run=False,
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
    )

    assert payload["schema_version"] == "aistock_issue_workflow_start_batch_v1"
    assert payload["workflow_gate"] == "ready_for_batch_fix"
    assert payload["batch_id"].startswith("BATCH-validation-guardrails-")
    assert payload["bug_ids"] == ["BUG-199", "BUG-200"]
    assert payload["code_intelligence"]["context_ref"].endswith("codegraph-context.md")
    assert payload["context_metrics"]["BUG-199"]["context_md"]["estimated_tokens"] > 0
    assert (isolated_workflow_root / payload["batch_state_path"]).exists()
    assert (isolated_workflow_root / payload["context_dir"] / "BUG-199.md").exists()
    assert (isolated_workflow_root / payload["fix_ready_dir"] / "BUG-200.json").exists()
    context_pack = json.loads((isolated_workflow_root / payload["context_dir"] / "BUG-199.json").read_text(encoding="utf-8"))
    assert context_pack["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")


def test_finish_batch_plan_generates_per_issue_pr_body(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug())
    _write_json(bugs_root / "bug200.json", _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"))
    monkeypatch.setattr(
        workflow,
        "_build_batch_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(
            item_id=kwargs["batch_id"],
            affected_tests={"suggested_tests": ["backend/tests/scripts/test_aistock_issue_workflow.py"]},
        ),
    )

    payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> passed"],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["schema_version"] == "aistock_issue_workflow_finish_batch_v1"
    assert payload["workflow_gate"] == "ready_for_pr"
    assert payload["per_issue_commit_map"] == {"BUG-199": "abc1234", "BUG-200": "def5678"}
    assert payload["codegraph_suggested_tests"] == ["backend/tests/scripts/test_aistock_issue_workflow.py"]
    pr_body = (isolated_workflow_root / payload["pr_body_path"]).read_text(encoding="utf-8")
    assert "Closes #199" in pr_body
    assert "Closes #200" in pr_body
    assert "Per-issue closure map" in pr_body
    assert "Code intelligence" in pr_body
    assert "backend/tests/scripts/test_aistock_issue_workflow.py" in pr_body



def test_code_intelligence_doctor_reports_bootstrap_command(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow.code_intelligence, "_codegraph_command", lambda: "codegraph")
    monkeypatch.setattr(
        workflow.code_intelligence,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "head": "abc123", "dirty": False, "dirty_count": 0},
    )

    payload = workflow.code_intelligence.build_doctor_report(isolated_workflow_root, skip_external=True)

    assert payload["workflow_gate"] == "warning"
    assert payload["codegraph"]["bootstrap_command"] == "codegraph init -i"
    assert payload["bootstrap_commands"]["codegraph"] == "codegraph init -i"
    assert any("codegraph init -i" in item for item in payload["warnings"])

def test_doctor_reports_ready_when_client_entries_exist(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue").mkdir(parents=True)
    (isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / ".claude" / "commands").mkdir(parents=True)
    (isolated_workflow_root / ".claude" / "commands" / "fix-aistock-issue.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
    codex_home = isolated_workflow_root / "codex_home"
    (codex_home / "skills" / "fix-aistock-issue").mkdir(parents=True)
    (codex_home / "skills" / "fix-aistock-issue" / "SKILL.md").write_text("", encoding="utf-8")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {
            "ok": True,
            "branch": "main",
            "head": "abc1234",
            "origin_main": "abc1234",
            "dirty": False,
            "dirty_count": 0,
            "status": "## main...origin/main",
        },
    )
    monkeypatch.setattr(workflow, "_mcp_config_snapshot", lambda: {"files": [], "stale_worktree_config_files": []})
    monkeypatch.setattr(
        workflow.code_intelligence,
        "build_doctor_report",
        lambda root, skip_external=False: {
            "schema_version": "aistock_code_intelligence_doctor_v1",
            "workflow_gate": "ready",
            "warnings": [],
            "blocking": [],
            "codegraph": {"status": "ok"},
            "understand_anything": {"status": "available"},
        },
    )

    payload = workflow.build_doctor_report(skip_external=True)

    assert payload["schema_version"] == "aistock_issue_workflow_doctor_v1"
    assert payload["workflow_gate"] == "ready"
    assert payload["blocking"] == []
    assert payload["code_intelligence"]["codegraph"]["status"] == "ok"
    assert "run --bug-id BUG-XXX" in payload["next_command"]



def test_doctor_reports_stale_global_skill_manifest(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("cli", encoding="utf-8")
    repo_skill = isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue"
    repo_skill.mkdir(parents=True)
    (repo_skill / "SKILL.md").write_text("repo skill", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / ".claude" / "commands").mkdir(parents=True)
    (isolated_workflow_root / ".claude" / "commands" / "fix-aistock-issue.md").write_text("claude", encoding="utf-8")
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
    codex_home = isolated_workflow_root / "codex_home"
    global_skill = codex_home / "skills" / "fix-aistock-issue"
    global_skill.mkdir(parents=True)
    (global_skill / "SKILL.md").write_text("old skill", encoding="utf-8")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {
            "ok": True,
            "branch": "main",
            "head": "abc1234",
            "origin_main": "abc1234",
            "dirty": False,
            "dirty_count": 0,
            "status": "## main...origin/main",
        },
    )
    monkeypatch.setattr(workflow, "_mcp_config_snapshot", lambda: {"files": [], "stale_worktree_config_files": []})
    monkeypatch.setattr(
        workflow.code_intelligence,
        "build_doctor_report",
        lambda root, skip_external=False: {
            "schema_version": "aistock_code_intelligence_doctor_v1",
            "workflow_gate": "ready",
            "warnings": [],
            "blocking": [],
        },
    )

    payload = workflow.build_doctor_report(skip_external=True)

    assert payload["workflow_gate"] == "warning"
    assert payload["client_manifest"]["codex_skill_status"] == "stale"
    assert payload["restart_recommended"] is True
    assert "install-client --apply" in payload["install_client_next_command"]


def test_run_plan_existing_clean_active_worktree_returns_resume(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    active = isolated_workflow_root / "active-worktree"
    active.mkdir()
    state_dir = active / "tmp" / "issue_workflow" / "BUG-199"
    _write_json(
        state_dir / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "context_ready",
            "worktree": str(active),
            "branch": "bug/BUG-199-existing",
            "next_actions": [],
        },
    )
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [active])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "bug/BUG-199-existing", "dirty": False, "dirty_count": 0},
    )

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="plan",
        issue_json=str(issue),
        changed_files=[],
        create_worktree=True,
        dry_run=False,
        validation_evidence=[],
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
        base="origin/main",
        head="HEAD",
    )

    assert payload["workflow_gate"] == "resume"
    assert payload["active_decision"]["decision"] == "resume_existing"
    assert "resume --bug-id BUG-199" in payload["next_command"]


def test_run_plan_dirty_active_worktree_blocks_duplicate_creation(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    active = isolated_workflow_root / "active-worktree"
    active.mkdir()
    state_dir = active / "tmp" / "issue_workflow" / "BUG-199"
    _write_json(
        state_dir / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "context_ready",
            "worktree": str(active),
            "branch": "bug/BUG-199-existing",
            "next_actions": [],
        },
    )
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [active])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "bug/BUG-199-existing", "dirty": True, "dirty_count": 2},
    )

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="plan",
        issue_json=str(issue),
        changed_files=[],
        create_worktree=True,
        dry_run=False,
        validation_evidence=[],
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
        base="origin/main",
        head="HEAD",
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["active_decision"]["decision"] == "blocked_dirty_active"
    assert "inspect_git_status_without_reset_or_clean" in payload["active_decision"]["rescue_checklist"]


def test_pre_pr_gate_blocks_artifacts_and_scope_violation(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finish = {
        "changed_files": ["scripts/aistock_issue_workflow.py", "outside/scope.py"],
        "scope_check": {"status": "failed", "violations": ["outside/scope.py"]},
    }
    monkeypatch.setattr(
        workflow,
        "_git_status_paths",
        lambda root: [{"status": "??", "path": ".codex_tmp/debug.json"}],
    )
    monkeypatch.setattr(
        workflow,
        "_run_changed_file_lint",
        lambda changed_files, root: {"status": "passed", "python_files": changed_files, "commands": []},
    )

    payload = workflow._pre_pr_gate(
        finish=finish,
        validation_evidence=["python -m nox -s l0 -> passed"],
        root=isolated_workflow_root,
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["artifact_guard"]["status"] == "failed"
    assert any("scope check failed" in item for item in payload["blocking"])
    assert any("temporary/cache artifacts" in item for item in payload["blocking"])

def test_submit_bug_dry_run_requires_github_sync(isolated_workflow_root: Path) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})

    payload = workflow.build_submit_bug_plan(
        title="Paper v2 display regression",
        module="paper_v2",
        severity="P1",
        description="The view shows stale data.",
        expected="The view should show fresh data.",
        actual="The view shows stale data.",
        reproduce_command="n/a",
        evidence_refs=["screenshot:paper-v2"],
        changed_files=["frontend/src/app/paper-v2/page.tsx"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number=None,
        github_issue_url=None,
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        dry_run=False,
    )

    assert payload["schema_version"] == "aistock_issue_workflow_submit_bug_v1"
    assert payload["bug_id"] == "BUG-118"
    assert payload["workflow_gate"] == "needs_github_sync"
    assert payload["record"]["github_issue_number"] is None
    assert not (isolated_workflow_root / payload["bug_json_path"]).exists()


def test_submit_bug_apply_with_existing_github_link_writes_registry(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="Paper v2 display regression",
        module="paper_v2",
        severity="P1",
        description="The view shows stale data.",
        expected="The view should show fresh data.",
        actual="The view shows stale data.",
        reproduce_command="n/a",
        evidence_refs=["screenshot:paper-v2"],
        changed_files=["frontend/src/app/paper-v2/page.tsx"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number="188",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/188",
        create_github=False,
        apply=True,
        create_registry_worktree=False,
        dry_run=False,
    )

    assert payload["workflow_gate"] == "submitted"
    bug_path = isolated_workflow_root / payload["bug_json_path"]
    assert bug_path.exists()
    record = json.loads(bug_path.read_text(encoding="utf-8"))
    assert record["bug_id"] == "BUG-118"
    assert record["github_issue_number"] == 188
    assert record["production_ddl_gate"] == "noop"
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 118
    assert (isolated_workflow_root / payload["state_path"]).exists()


def test_submit_bug_apply_blocks_canonical_root_pollution(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    with pytest.raises(workflow.WorkflowError, match="canonical root"):
        workflow.build_submit_bug_plan(
            title="Paper v2 display regression",
            module="paper_v2",
            severity="P1",
            description="The view shows stale data.",
            expected="The view should show fresh data.",
            actual="The view shows stale data.",
            reproduce_command="n/a",
            evidence_refs=[],
            changed_files=[],
            plan_key=None,
            nox_session=None,
            candidate_type="bug",
            bug_id=None,
            github_issue_number="188",
            github_issue_url="https://github.com/licong01-cloud/AIstock/issues/188",
            create_github=False,
            apply=True,
            create_registry_worktree=False,
            dry_run=False,
        )


def test_submit_bug_apply_uses_registry_worktree_override(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry-worktree"
    allocator = registry / "tests" / "aistock_validation" / "bugs" / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})
    monkeypatch.setenv("AISTOCK_ISSUE_REGISTRY_ROOT", str(registry))
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "bug/registry", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    payload = workflow.build_submit_bug_plan(
        title="Paper v2 display regression",
        module="paper_v2",
        severity="P1",
        description="The view shows stale data.",
        expected="The view should show fresh data.",
        actual="The view shows stale data.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=[],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number="188",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/188",
        create_github=False,
        apply=True,
        create_registry_worktree=False,
        dry_run=False,
    )

    assert payload["workflow_gate"] == "submitted"
    assert payload["registry_root"] == str(registry)
    assert (registry / payload["bug_json_path"]).exists()
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 118


def test_submit_bug_can_plan_registry_worktree_without_writes(isolated_workflow_root: Path) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})

    payload = workflow.build_submit_bug_plan(
        title="Paper v2 display regression",
        module="paper_v2",
        severity="P1",
        description="The view shows stale data.",
        expected="The view should show fresh data.",
        actual="The view shows stale data.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=[],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number=None,
        github_issue_url=None,
        create_github=False,
        apply=False,
        create_registry_worktree=True,
        dry_run=True,
    )

    assert payload["workflow_gate"] == "needs_github_sync"
    assert payload["registry_worktree_plan"]["create_worktree"] is True
    assert payload["registry_worktree_plan"]["dry_run"] is True
    assert payload["registry_worktree_plan"]["branch"].startswith("bug/registry-paper-v2-")
    assert not (isolated_workflow_root / payload["bug_json_path"]).exists()


def test_install_client_plan_can_copy_global_codex_skill(
    isolated_workflow_root: Path,
) -> None:
    source = isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue"
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("skill", encoding="utf-8")
    claude = isolated_workflow_root / ".claude" / "commands"
    claude.mkdir(parents=True)
    (claude / "fix-aistock-issue.md").write_text("claude", encoding="utf-8")
    codex_home = isolated_workflow_root / "codex_home"

    dry = workflow.build_client_install_plan(codex_home=str(codex_home))
    assert dry["workflow_gate"] == "ready_for_install"
    assert dry["dry_run"] is True

    applied = workflow.build_client_install_plan(apply=True, codex_home=str(codex_home))
    assert applied["workflow_gate"] == "installed"
    assert (codex_home / "skills" / "fix-aistock-issue" / "SKILL.md").read_text(encoding="utf-8") == "skill"


def test_run_plan_writes_state_and_resume_reads_it(isolated_workflow_root: Path) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="plan",
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        create_worktree=False,
        dry_run=False,
        validation_evidence=[],
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
        base="origin/main",
        head="HEAD",
    )
    state_path = isolated_workflow_root / payload["start"]["state_path"]
    events_path = isolated_workflow_root / payload["start"]["events_path"]

    assert payload["workflow_gate"] == "planned"
    assert state_path.exists()
    assert events_path.exists()
    assert json.loads(state_path.read_text(encoding="utf-8"))["state"] == "context_ready"

    resume = workflow.build_resume_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root))
    assert resume["schema_version"] == "aistock_issue_workflow_resume_v1"
    assert resume["state"]["context_pack_md"].endswith("context-pack.md")
    assert "finish --bug-id BUG-199" in resume["next_command"]


def test_postmortem_reports_timing_context_and_duplicate_active_count(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "validation_passed",
            "branch": "bug/BUG-199-workflow",
            "worktree": str(isolated_workflow_root),
            "context_metrics": {
                "context_pack_md": {"estimated_tokens": 12},
                "fix_ready_json": {"estimated_tokens": 8},
            },
            "production_gates": {"production_ddl_gate": "noop"},
        },
    )
    events_path = isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "timestamp": "2026-05-26T00:00:00Z",
                        "event": "state:context_ready",
                        "state": "context_ready",
                        "duration_seconds": None,
                    }
                ),
                json.dumps(
                    {
                        "timestamp": "2026-05-26T00:00:05Z",
                        "event": "command:gh_pr_create",
                        "state": "pr_opened",
                        "duration_seconds": 2.5,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        workflow,
        "_active_workflows_for_bug",
        lambda bug_id: [
            {"bug_id": bug_id, "worktree": str(isolated_workflow_root), "dirty": False},
            {"bug_id": bug_id, "worktree": str(isolated_workflow_root / "other"), "dirty": False},
        ],
    )
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda bug_id: {"status": "checked", "open_prs": [], "merged_prs": []})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root))

    assert payload["schema_version"] == "aistock_issue_workflow_postmortem_v1"
    assert payload["timing_summary"]["event_count"] == 2
    assert payload["timing_summary"]["known_duration_seconds"] == 2.5
    assert payload["flow_overhead_estimate"]["context_estimated_tokens"] == 20
    assert payload["duplicate_active_count"] == 1
    assert (isolated_workflow_root / payload["postmortem_json_path"]).exists()
    assert (isolated_workflow_root / payload["postmortem_md_path"]).exists()


def test_run_pr_mode_drafts_pr_automation_without_side_effects(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    monkeypatch.setattr(workflow, "_current_branch", lambda root=None: "bug/BUG-199-workflow")
    monkeypatch.setattr(
        workflow,
        "_pr_worktree_guard",
        lambda root=None: {
            "blocking": [],
            "warnings": [],
            "root": str(isolated_workflow_root),
            "canonical_root": str(isolated_workflow_root.parent / "AIstock"),
        },
    )

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="pr",
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        create_worktree=False,
        dry_run=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
        base="origin/main",
        head="HEAD",
    )

    assert payload["workflow_gate"] == "ready_for_pr"
    assert payload["pr_automation"]["dry_run"] is True
    assert "gh pr create" in payload["pr_automation"]["next_commands"][1]


def test_run_pr_mode_blocks_pr_automation_from_canonical_root(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    with pytest.raises(workflow.WorkflowError, match="canonical root"):
        workflow.build_run_plan(
            bug_id="BUG-199",
            mode="pr",
            issue_json=str(issue),
            changed_files=["scripts/aistock_issue_workflow.py"],
            create_worktree=False,
            dry_run=False,
            validation_evidence=["python -m nox -s l0 -> passed"],
            task_slug=None,
            allow_missing_linkage=False,
            allow_closed=False,
            base="origin/main",
            head="HEAD",
        )


def test_close_sync_apply_skips_github_sync_when_github_check_is_disabled(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    called = False

    def fake_sync(record: dict[str, Any], evidence_payload: dict[str, Any]) -> dict[str, Any]:
        nonlocal called
        called = True
        return {"status": "synced"}

    monkeypatch.setattr(workflow, "_sync_github_issue_after_close", fake_sync)

    payload = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/1",
        apply=True,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        merge_commit="abc1234",
        production_gates={"production_ddl_gate": "noop"},
        skip_github_check=True,
    )

    assert payload["workflow_gate"] == "close_synced"
    assert payload["github_issue_sync"]["status"] == "skipped_github_check_disabled"
    assert called is False



def test_run_merge_mode_requires_explicit_authorization(isolated_workflow_root: Path) -> None:
    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="merge",
        issue_json=None,
        changed_files=[],
        create_worktree=False,
        dry_run=False,
        validation_evidence=[],
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
        base="origin/main",
        head="HEAD",
        pr_url="https://github.com/licong01-cloud/AIstock/pull/999",
        merge=False,
    )

    assert payload["workflow_gate"] == "merge_requires_explicit_flag"
    assert "--merge" in payload["next_command"]

def test_close_sync_is_dry_run_and_requires_pr_url(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="fixed"))

    assert workflow.main(["close-sync", "--issue-json", str(issue)]) == 2
    missing_pr = json.loads(capsys.readouterr().out)
    assert missing_pr["workflow_gate"] == "missing_pr_url"

    assert workflow.main([
        "close-sync",
        "--issue-json",
        str(issue),
        "--pr-url",
        "https://github.example/pull/1",
        "--validation-evidence",
        "python -m nox -s l0 -> passed",
    ]) == 0
    ready = json.loads(capsys.readouterr().out)
    assert ready["workflow_gate"] == "ready_for_apply"
    assert ready["dry_run"] is True
    assert (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "close-sync-plan.json").exists()

    assert workflow.main([
        "close-sync",
        "--issue-json",
        str(issue),
        "--pr-url",
        "https://github.example/pull/1",
        "--validation-evidence",
        "python -m nox -s l0 -> passed",
        "--merge-commit",
        "abc1234",
        "--skip-github-check",
        "--apply",
    ]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["workflow_gate"] == "close_synced"
    updated = json.loads(issue.read_text(encoding="utf-8"))
    assert updated["status"] == "fixed"
    assert updated["fix_commit"] == "abc1234"


def test_cleanup_after_merge_blocks_unmerged_branch(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return "bug/BUG-199-workflow"
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    payload = workflow.build_cleanup_after_merge_plan(branch="bug/BUG-199-workflow")

    assert payload["workflow_gate"] == "blocked"
    assert "not merged" in payload["blocking"][0]


def test_cleanup_after_merge_dry_run_ready_for_merged_branch(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return "bug/BUG-199-workflow"
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return "bug/BUG-199-workflow"
        if args[:2] == ["ls-remote", "--heads"]:
            return "ref"
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    payload = workflow.build_cleanup_after_merge_plan(branch="bug/BUG-199-workflow", sync_root=True)

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["dry_run"] is True
    assert {item["action"] for item in payload["actions"]} >= {"sync_root_main", "delete_local_branch", "delete_remote_branch"}


def test_cleanup_after_merge_allows_verified_squash_merge(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "feature/issue-workflow-phase2"

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(workflow, "_verify_pr_merged", lambda pr_url: {"checked": True, "merged": True, "pr": {"url": pr_url}})
    monkeypatch.setattr(workflow, "_run_command", lambda *args, **kwargs: {"ok": True, "stdout": "", "stderr": "", "returncode": 0})

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        pr_url="https://github.example/pull/195",
        sync_root=True,
    )

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["merged_into_origin_main"] is False
    assert payload["squash_merge_verified"] is True
    assert payload["tree_equivalent_to_origin_main"] is True


def test_repo_skill_and_quickstart_are_parseable() -> None:
    skill = Path(".codex/skills/fix-aistock-issue/SKILL.md").read_text(encoding="utf-8")
    assert skill.startswith("---\n")
    assert "scripts/aistock_issue_workflow.py" in skill
    assert "fix BUG-112 according to AIstock standards" in skill

    metadata = yaml.safe_load(Path(".codex/skills/fix-aistock-issue/agents/openai.yaml").read_text(encoding="utf-8"))
    assert metadata["interface"]["display_name"] == "Fix AIstock Issue"

    quickstart = Path("docs/standards/aistock_issue_workflow_quickstart.md").read_text(encoding="utf-8")
    assert "AIstock Issue Workflow Quickstart" in quickstart
    assert "按规范修复 BUG-112" in quickstart
    assert "????" not in quickstart
    assert "doctor" in quickstart
    assert "submit-bug" in quickstart
    assert "start-batch" in quickstart
    assert "resume" in quickstart
    assert "production_ddl_gate" in quickstart

    claude_command = Path(".claude/commands/fix-aistock-issue.md").read_text(encoding="utf-8")
    assert "Claude Code" in claude_command
    assert "submit-bug" in claude_command
    assert "aistock_issue_workflow.py doctor" in claude_command

    design = Path("docs/architecture/aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").read_text(encoding="utf-8")
    assert "智能验证平台设计实施方案 v2.0" in design
    assert "Codex / Claude Code / Cursor" in design
    assert "????" not in design


def test_triage_ci_issue_extracts_run_summary_and_recommends_promotion(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = {
        "number": 197,
        "title": "[P1] AIstock CI failed on main",
        "state": "OPEN",
        "url": "https://github.com/licong01-cloud/AIstock/issues/197",
        "body": "<!-- aistock-issue-on-test-fail:26378872481 -->",
        "labels": [],
    }
    summary = {
        "schema_version": "aistock_ci_failure_summary_v1",
        "diagnostic_status": "complete",
        "severity": "P1",
        "workflow": "AIstock CI",
        "run_id": "26378872481",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
        "branch": "main",
        "commit": "62dc1b1",
        "failed_jobs": [
            {
                "job_name": "Backend tests (paper_v2_backend)",
                "nox_session": "paper_v2_backend",
                "failed_tests": [
                    "backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_a_share_trading_window"
                ],
                "error_signature": "assert 200 == 409",
                "key_log_excerpt": ['relation "market.trading_calendar" does not exist'],
                "suspected_module": "paper_v2",
                "suspected_files": ["backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py"],
            }
        ],
        "suspected_modules": ["paper_v2"],
        "suspected_files": ["backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py"],
        "fingerprint": "ci-test",
        "issue_title": "[P1][paper_v2_backend] main CI failed: test_sentinel_endpoint_rejects_a_share_trading_window",
        "reproduce_command": "python -m pytest backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_a_share_trading_window -q -p no:cacheprovider",
    }

    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(
        workflow.ci_failure_summary,
        "summarize_actions_run",
        lambda **kwargs: summary,
    )

    payload = workflow.build_triage_ci_issue_plan(issue_number=197)

    assert payload["schema_version"] == "aistock_issue_workflow_triage_ci_issue_v1"
    assert payload["detected_run_id"] == "26378872481"
    assert payload["needs_bug_json"] is True
    assert payload["suggested_bug"]["module"] == "paper_v2"
    assert "promote-ci-issue --issue 197" in payload["next_command"]
    assert (isolated_workflow_root / "tmp" / "issue_workflow" / "ci-issue-197" / "triage-ci-issue.json").exists()


def test_promote_ci_issue_writes_bug_json_with_existing_github_issue(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 118})
    issue = {
        "number": 197,
        "title": "[P1] AIstock CI failed on main",
        "state": "OPEN",
        "url": "https://github.com/licong01-cloud/AIstock/issues/197",
        "body": "<!-- aistock-issue-on-test-fail:26378872481 -->",
        "labels": [],
    }
    summary = {
        "schema_version": "aistock_ci_failure_summary_v1",
        "diagnostic_status": "complete",
        "severity": "P1",
        "workflow": "AIstock CI",
        "run_id": "26378872481",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
        "branch": "main",
        "commit": "62dc1b1",
        "failed_jobs": [
            {
                "job_name": "Backend tests (paper_v2_backend)",
                "nox_session": "paper_v2_backend",
                "failed_tests": [
                    "backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_a_share_trading_window"
                ],
                "error_signature": "assert 200 == 409",
                "key_log_excerpt": ['relation "market.trading_calendar" does not exist'],
                "suspected_module": "paper_v2",
                "suspected_files": ["backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py"],
            }
        ],
        "suspected_modules": ["paper_v2"],
        "suspected_files": ["backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py"],
        "fingerprint": "ci-test",
        "issue_title": "[P1][paper_v2_backend] main CI failed: test_sentinel_endpoint_rejects_a_share_trading_window",
        "reproduce_command": "python -m pytest backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_a_share_trading_window -q -p no:cacheprovider",
    }

    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(
        workflow,
        "_validate_registry_apply_target",
        lambda root: {"blocking": [], "warnings": [], "target_root": str(root)},
    )
    monkeypatch.setattr(
        workflow.ci_failure_summary,
        "summarize_actions_run",
        lambda **kwargs: summary,
    )

    payload = workflow.build_promote_ci_issue_plan(issue_number=197, apply=True, bug_id=None)

    assert payload["workflow_gate"] == "promoted"
    assert payload["submit_bug"]["bug_id"] == "BUG-119"
    bug_path = isolated_workflow_root / payload["submit_bug"]["bug_json_path"]
    record = json.loads(bug_path.read_text(encoding="utf-8"))
    assert record["github_issue_number"] == 197
    assert record["github_issue_url"] == "https://github.com/licong01-cloud/AIstock/issues/197"
    assert record["module"] == "paper_v2"
    assert record["production_ddl_gate"] == "noop"
