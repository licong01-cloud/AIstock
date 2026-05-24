from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

import scripts.aistock_issue_workflow as workflow


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


def test_start_writes_fix_ready_and_context_pack(isolated_workflow_root: Path) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

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
    assert context_md.read_text(encoding="utf-8").startswith("# AIstock Context Pack")
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
    assert (isolated_workflow_root / ready["pr_body_path"]).exists()


def test_finish_plan_only_can_draft_pr_body_without_evidence(isolated_workflow_root: Path) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

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
    pr_body = isolated_workflow_root / payload["pr_body_path"]
    assert "missing - run required validation" in pr_body.read_text(encoding="utf-8")


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
    ]) == 0
    ready = json.loads(capsys.readouterr().out)
    assert ready["workflow_gate"] == "ready_for_mcp_sync"
    assert ready["dry_run"] is True
    assert (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "close-sync-plan.json").exists()

    assert workflow.main(["close-sync", "--issue-json", str(issue), "--apply"]) == 2
    assert "intentionally not implemented" in capsys.readouterr().err


def test_repo_skill_and_quickstart_are_parseable() -> None:
    skill = Path(".codex/skills/fix-aistock-issue/SKILL.md").read_text(encoding="utf-8")
    assert skill.startswith("---\n")
    assert "scripts/aistock_issue_workflow.py" in skill
    assert "fix BUG-112 according to AIstock standards" in skill

    metadata = yaml.safe_load(Path(".codex/skills/fix-aistock-issue/agents/openai.yaml").read_text(encoding="utf-8"))
    assert metadata["interface"]["display_name"] == "Fix AIstock Issue"

    quickstart = Path("docs/standards/aistock_issue_workflow_quickstart.md").read_text(encoding="utf-8")
    assert "AIstock Issue Workflow Quickstart" in quickstart
    assert "production_ddl_gate" in quickstart
