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
        "affected_tests_count": 0,
        "affected_quality": "codegraph_fallback",
        "latest_freshness": "fresh",
        "latest_freshness_ref": "tmp/validation/code-intelligence/codegraph-freshness.json",
        "consume_command": "python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale",
        "affected_tests": {"suggested_tests": []},
        "understand_anything": {"status": "not_required_missing"},
        "understand_anything_summary_ref": "tmp/issue_workflow/BUG-199/ua-validation-summary.md",
        "understand_anything_summary": {
            "status": "fallback",
            "graph_exists": False,
            "nodes_used": 0,
            "summary_ref": "tmp/issue_workflow/BUG-199/ua-validation-summary.md",
        },
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


def _fetched_origin_payload() -> dict[str, Any]:
    return {
        "status": "fetched",
        "command": "git fetch origin --prune",
        "result": {"ok": True, "stdout": "", "stderr": "", "returncode": 0},
    }


def test_emit_dash_writes_stdout_without_dash_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)

    workflow._emit({"ok": True}, "-", output_format="full-json")

    assert json.loads(capsys.readouterr().out) == {"ok": True}
    assert not (tmp_path / "-").exists()


def test_emit_defaults_to_compact_success_payload(capsys: pytest.CaptureFixture[str]) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_issue_workflow_smoke_v1",
            "workflow_gate": "passed",
            "bug_id": "BUG-199",
            "fast_path": {"task_tier": "T1", "module": "validation", "validation": {"skip_reasons": {"x": "noisy"}}},
            "postmortem_preview": {"recent_events": [{"event": "noisy"}], "timing_summary": {"event_count": 1}},
            "statusCheckRollup": [{"name": "noisy"}],
        }
    )

    out = capsys.readouterr().out
    compact = json.loads(out)
    assert compact["workflow_gate"] == "passed"
    assert compact["full_payload"].startswith("use --output-format full-json")
    assert "statusCheckRollup" not in out
    assert "recent_events" not in out
    assert "skip_reasons" not in out


def test_emit_summary_outputs_human_success_line(capsys: pytest.CaptureFixture[str]) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_issue_workflow_smoke_v1",
            "workflow_gate": "passed",
            "bug_id": "BUG-199",
            "statusCheckRollup": [{"name": "noisy"}],
            "recent_events": [{"event": "noisy"}],
        },
        output_format="summary",
    )

    out = capsys.readouterr().out.strip()
    assert out.startswith("PASS BUG-199")
    assert "workflow_gate=passed" in out
    assert "{" not in out
    assert "schema_version" not in out
    assert "statusCheckRollup" not in out
    assert "recent_events" not in out


def test_emit_summary_for_watch_ci_keeps_counts_without_rollup(capsys: pytest.CaptureFixture[str]) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_issue_workflow_watch_ci_v1",
            "workflow_gate": "checks_passed",
            "bug_id": "BUG-199",
            "pr_url": "https://github.example/pull/199",
            "check_summary": {
                "failed_count": 0,
                "pending_count": 0,
                "passed_count": 7,
                "non_blocking_count": 1,
            },
            "next_actions": ["merge_only_if_user_authorized"],
            "statusCheckRollup": [{"name": "noisy"}],
        },
        output_format="summary",
    )

    out = capsys.readouterr().out.strip()
    assert out.startswith("PASS BUG-199")
    assert "passed=7" in out
    assert "pending=0" in out
    assert "failed=0" in out
    assert "merge_only_if_user_authorized" in out
    assert "{" not in out
    assert "statusCheckRollup" not in out


def test_compact_merge_output_hides_verbose_finalizer_and_postmortem_lists(
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_issue_workflow_run_v1",
            "workflow_gate": "merged_close_synced",
            "bug_id": "BUG-203",
            "mode": "merge",
            "merge": {
                "already_merged": False,
                "check_summary": {
                    "failed": [],
                    "pending": [],
                    "non_blocking": [{"name": "neutral"}],
                    "passed": [{"name": "Static gate"}],
                },
                "verified": {
                    "checked": True,
                    "merged": True,
                    "pr": {
                        "state": "MERGED",
                        "mergedAt": "2026-06-02T00:00:00Z",
                        "mergeCommit": {"oid": "merge123"},
                        "headRefOid": "large-noisy-field",
                    },
                },
            },
            "finalizer": {
                "schema_version": "aistock_issue_workflow_merge_finalizer_v1",
                "workflow_gate": "complete",
                "source_merge_commit": "merge123",
                "next_actions": [],
                "close_sync": {
                    "workflow_gate": "already_close_synced",
                    "registry_root": "F:/Dev/AIstock",
                    "updated_bug_json": "origin/main:tests/aistock_validation/bugs/bug203.json",
                    "merge_commit": "merge123",
                    "stale_pr_check": {"merged_prs": [{"number": 1}]},
                },
                "close_sync_commit": {
                    "workflow_gate": "already_merged",
                    "branch": "chore/BUG-203-close-sync",
                    "pr_url": "https://github.example/pull/2",
                    "actions": [{"command": "noisy"}],
                },
                "postmortem": {
                    "schema_version": "aistock_issue_workflow_postmortem_v1",
                    "bug_id": "BUG-203",
                    "workflow_root": "F:/Dev/AIstock_worktrees/BUG-203",
                    "state": {"state": "complete", "recent_events": [{"event": "noisy"}]},
                    "phase_cost_table": [{"phase": "validation", "dominant_seconds": 12}],
                    "h6_summary": {
                        "event_count": 3,
                        "total_estimated_tokens": 100,
                        "phase_cost_table": [{"phase": "noisy"}],
                    },
                    "stale_pr_check": {
                        "status": "checked",
                        "open_prs": [{"number": 3, "title": "verbose"}],
                        "merged_prs": [{"number": 4, "title": "verbose"}],
                    },
                    "recent_events": [{"event": "noisy"}],
                },
            },
        }
    )

    out = capsys.readouterr().out
    compact = json.loads(out)
    assert compact["merge"]["merge_commit"] == "merge123"
    assert compact["merge"]["check_summary"]["passed_count"] == 1
    assert compact["finalizer"]["postmortem"]["stale_pr_check"] == {
        "status": "checked",
        "open_pr_count": 1,
        "merged_pr_count": 1,
    }
    assert "large-noisy-field" not in out
    assert "recent_events" not in out
    assert "open_prs" not in out
    assert "merged_prs" not in out
    assert '"command": "noisy"' not in out

def test_emit_full_json_preserves_payload(capsys: pytest.CaptureFixture[str]) -> None:
    payload = {
        "schema_version": "aistock_issue_workflow_smoke_v1",
        "workflow_gate": "passed",
        "statusCheckRollup": [{"name": "Static gate"}],
    }

    workflow._emit(payload, output_format="full-json")

    assert json.loads(capsys.readouterr().out)["statusCheckRollup"][0]["name"] == "Static gate"


def test_emit_output_file_keeps_full_payload_while_stdout_is_compact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    output = tmp_path / "tmp" / "issue_workflow" / "BUG-199" / "full.json"
    payload = {
        "schema_version": "aistock_issue_workflow_smoke_v1",
        "workflow_gate": "passed",
        "statusCheckRollup": [{"name": "Static gate"}],
    }

    workflow._emit(payload, str(output))

    assert "statusCheckRollup" not in capsys.readouterr().out
    assert json.loads(output.read_text(encoding="utf-8"))["statusCheckRollup"][0]["name"] == "Static gate"


def test_emit_rejects_format_token_without_creating_root_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)

    with pytest.raises(workflow.WorkflowError, match="JSON file path"):
        workflow._emit({"ok": True}, "json")

    assert not (tmp_path / "json").exists()


def test_emit_rejects_root_level_bare_output_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)

    with pytest.raises(workflow.WorkflowError, match="root-level bare file"):
        workflow._emit({"ok": True}, str(tmp_path / "workflow-output"))

    assert not (tmp_path / "workflow-output").exists()


def test_next_command_for_ci_running_uses_watch_ci() -> None:
    command = workflow._next_command_for_state(
        "BUG-199",
        {"state": "ci_running", "pr_url": "https://github.example/pull/199"},
    )

    assert "watch-ci" in command
    assert "--pr-url https://github.example/pull/199" in command


def test_watch_ci_updates_state_when_checks_pass(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_watch_pr_checks_compact",
        lambda bug_id, pr_url, attempts=1, delay_seconds=0: {
            "workflow_gate": "checks_passed",
            "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 3, "non_blocking_count": 1},
            "next_actions": ["merge_only_if_user_authorized"],
        },
    )

    payload = workflow.build_watch_ci_plan(
        bug_id="BUG-199",
        pr_url="https://github.example/pull/199",
    )

    assert payload["workflow_gate"] == "checks_passed"
    assert payload["state"] == "ci_green"
    assert payload["next_actions"] == ["merge_only_if_user_authorized"]
    state = json.loads(
        (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "state.json").read_text(encoding="utf-8")
    )
    assert state["state"] == "ci_green"
    assert "stop_reason" not in state


@pytest.fixture
def isolated_workflow_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(workflow, "BUGS_ROOT", tmp_path / "tests" / "aistock_validation" / "bugs")
    monkeypatch.setenv("AISTOCK_CANONICAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AISTOCK_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    monkeypatch.setenv("AISTOCK_BUG_ID_RESERVATION_ROOT", str(tmp_path / "bug-id-reservations"))
    monkeypatch.setattr(workflow, "_scan_github_bug_ids", lambda **_kwargs: ([], []))
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
    task_card_json = isolated_workflow_root / payload["task_card_json"]
    task_card_md = isolated_workflow_root / payload["task_card_md"]
    assert fix_ready.exists()
    assert context_json.exists()
    assert task_card_json.exists()
    assert task_card_md.exists()
    context_payload = json.loads(context_json.read_text(encoding="utf-8"))
    task_card = json.loads(task_card_json.read_text(encoding="utf-8"))
    assert context_md.read_text(encoding="utf-8").startswith("# AIstock Context Pack")
    assert task_card_md.read_text(encoding="utf-8").startswith("# AIstock Agent Task Card BUG-199")
    assert context_payload["code_intelligence"]["provider"] == "codegraph"
    assert task_card["schema_version"] == "aistock_agent_task_card_v1"
    assert task_card["supported_clients"] == ["Codex", "Claude Code", "Cursor", "CLI"]
    assert task_card["artifact_refs"]["context_pack_md"].endswith("context-pack.md")
    assert task_card["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")
    assert task_card["code_intelligence"]["latest_freshness"] == "fresh"
    assert task_card["code_intelligence"]["consume_command"].endswith("latest-freshness --refresh-if-stale")
    assert task_card["code_intelligence"]["understand_anything_summary_ref"].endswith("ua-validation-summary.md")
    assert task_card["code_intelligence"]["affected_tests_count"] == 0
    assert task_card["code_intelligence"]["blocking_for_issue_workflow"] is False
    assert task_card["token_budget"]["large_graph_payload_inlined"] is False
    assert "suggested_tests" not in json.dumps(task_card, ensure_ascii=False)
    assert "skip_reasons" not in json.dumps(task_card, ensure_ascii=False)
    assert payload["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")
    assert payload["task_card_md"].endswith("task-card.md")
    assert payload["context_metrics"]["context_pack_md"]["estimated_tokens"] > 0
    assert payload["context_metrics"]["task_card_md"]["estimated_tokens"] > 0
    assert payload["context_metrics"]["fix_ready_json"]["bytes"] > 0
    assert json.loads(fix_ready.read_text(encoding="utf-8"))["workflow_gate"] == "allowed"


def test_start_created_worktree_seeds_bug_json(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "registry" / "tests" / "aistock_validation" / "bugs" / "BUG-199.json", _bug())
    fix = isolated_workflow_root / "fix-worktree"
    monkeypatch.setattr(workflow, "_target_names", lambda record, bug_id, task_slug=None: ("bug/BUG-199-fix", fix))
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["worktree", "add"]:
            fix.mkdir(parents=True)
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)

    payload = workflow.build_start_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=[],
        create_worktree=True,
        dry_run=False,
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
    )

    seeded = fix / payload["target_bug_json"]
    assert payload["worktree_plan"]["seeded_issue_json"] == "tests/aistock_validation/bugs/BUG-199.json"
    assert seeded.exists()
    assert json.loads(seeded.read_text(encoding="utf-8"))["bug_id"] == "BUG-199"


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
    assert "artifact_metrics" not in ready
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
    assert payload["batch_selector"]["workflow_gate"] == "compatible"
    assert payload["batch_selector"]["shared_files"] == ["scripts/aistock_issue_workflow.py"]
    assert payload["context_metrics"]["BUG-199"]["context_md"]["estimated_tokens"] > 0
    assert (isolated_workflow_root / payload["batch_state_path"]).exists()
    assert (isolated_workflow_root / payload["context_dir"] / "BUG-199.md").exists()
    assert (isolated_workflow_root / payload["fix_ready_dir"] / "BUG-200.json").exists()
    context_pack = json.loads((isolated_workflow_root / payload["context_dir"] / "BUG-199.json").read_text(encoding="utf-8"))
    assert context_pack["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")


def test_start_batch_rejects_missing_scope(isolated_workflow_root: Path) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug(allowed_write_scope=[]))
    _write_json(
        bugs_root / "bug200.json",
        _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"),
    )

    with pytest.raises(workflow.WorkflowError, match="no allowed_write_scope"):
        workflow.build_start_batch_plan(
            bug_ids=["BUG-199", "BUG-200"],
            create_worktree=False,
            dry_run=True,
            task_slug=None,
            allow_missing_linkage=False,
            allow_closed=False,
        )


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
    assert payload["scope_check"]["status"] == "passed"
    assert payload["per_issue_commit_map"] == {"BUG-199": "abc1234", "BUG-200": "def5678"}
    assert payload["codegraph_suggested_tests"] == ["backend/tests/scripts/test_aistock_issue_workflow.py"]
    pr_body = (isolated_workflow_root / payload["pr_body_path"]).read_text(encoding="utf-8")
    assert "Closes #199" in pr_body
    assert "Closes #200" in pr_body
    assert "Per-issue closure map" in pr_body
    assert "Code intelligence" in pr_body
    assert "backend/tests/scripts/test_aistock_issue_workflow.py" in pr_body


def test_finish_batch_blocks_scope_expansion(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug())
    _write_json(
        bugs_root / "bug200.json",
        _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"),
    )
    monkeypatch.setattr(workflow, "_build_batch_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary(item_id=kwargs["batch_id"]))

    payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["frontend/src/app/page.tsx"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> passed"],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["scope_check"]["status"] == "failed"
    assert "exceed shared scope" in payload["error"]


def test_finish_batch_scope_check_accepts_glob_scope(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(
        bugs_root / "bug199.json",
        _bug(allowed_write_scope=["tests/aistock_validation/bugs/**"]),
    )
    _write_json(
        bugs_root / "bug200.json",
        _bug(
            bug_id="BUG-200",
            github_issue_number=200,
            github_issue_url="https://github.example/issues/200",
            allowed_write_scope=["tests/aistock_validation/bugs/**"],
        ),
    )
    monkeypatch.setattr(workflow, "_build_batch_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary(item_id=kwargs["batch_id"]))

    payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["tests/aistock_validation/bugs/BUG-199.json"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> passed"],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["workflow_gate"] == "ready_for_pr"
    assert payload["scope_check"]["status"] == "passed"


def test_fast_path_classifies_docs_only_as_t0(isolated_workflow_root: Path) -> None:
    payload = workflow.build_fast_path_plan(
        bug_id=None,
        issue_json=None,
        changed_files=["docs/standards/aistock_issue_workflow_quickstart.md"],
    )

    assert payload["schema_version"] == "aistock_issue_workflow_fast_path_v1"
    assert payload["task_tier"] == "T0"
    assert payload["context_strategy"]["max_initial_files"] == 4
    assert "archived standards" in payload["context_strategy"]["avoid_by_default"]
    assert payload["production_gates"]["ddl"] == "noop"
    assert "python -m nox -s l0" in payload["required_commands"]


def test_fast_path_classifies_workflow_script_as_t1(isolated_workflow_root: Path) -> None:
    payload = workflow.build_fast_path_plan(
        bug_id=None,
        issue_json=None,
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation.guardrails",
    )

    assert payload["task_tier"] == "T1"
    assert payload["file_categories"]["scripts/aistock_issue_workflow.py"] == "workflow"
    assert payload["context_strategy"]["goal"] == "single issue context pack plus targeted code snippets"
    assert payload["production_gates"] == {
        "ddl": "noop",
        "frontend_dependency": "noop",
        "backend_dependency": "noop",
    }
    assert "python -m nox -s guardrail_changed_files" in payload["required_commands"]
    assert payload["code_intelligence_hint"]["blocking_for_issue_workflow"] is False
    assert payload["code_intelligence_hint"]["consume_command"] == "python scripts/code_intelligence_adapter.py latest-freshness"


def test_start_and_finish_embed_fast_path(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    start = workflow.build_start_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        create_worktree=False,
        dry_run=True,
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
    )
    finish = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=[],
        plan_only=True,
        allow_missing_evidence=False,
    )

    assert start["fast_path"]["task_tier"] == "T1"
    assert finish["fast_path"]["task_tier"] == "T1"
    assert start["fast_path"]["next_command"].endswith("run --bug-id BUG-199 --mode plan --create-worktree")


def test_workflow_smoke_uses_synthetic_issue_and_no_unexpected_dirty_paths(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])

    payload = workflow.build_workflow_smoke_plan(
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation.guardrails",
    )

    assert payload["schema_version"] == "aistock_issue_workflow_smoke_v1"
    assert payload["workflow_gate"] == "passed"
    assert payload["dry_run"] is True
    assert payload["synthetic_record"] is True
    assert payload["unexpected_dirty_paths"] == []
    assert payload["client_manifest"]["codex_skill_status"] in {
        "current",
        "missing_repo_skill",
        "missing_global",
        "stale",
    }
    assert payload["h7_code_intelligence"]["workflow_gate"] in {"ready", "warning"}
    assert payload["fast_path"]["task_tier"] == "T1"
    assert payload["start"]["worktree_plan"]["dry_run"] is True
    assert payload["finish"]["workflow_gate"] == "ready_for_pr"
    assert payload["postmortem_preview"]["stale_pr_check"] == "skipped_in_smoke_to_avoid_external_github_reads"
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-000*.json"))


def test_nightly_intake_smoke_writes_only_tmp_artifacts_and_handoff(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])

    payload = workflow.build_nightly_intake_smoke_plan()

    assert payload["schema_version"] == "aistock_nightly_intake_smoke_v1"
    assert payload["workflow_gate"] == "passed"
    assert payload["github_writes"] is False
    assert payload["unexpected_dirty_paths"] == []
    assert payload["candidate_history_path"].startswith("tmp/validation/nightly_failure_issue/smoke/")
    assert "tests/aistock_validation/history" not in payload["candidate_history_path"]
    for path in payload["artifacts"].values():
        assert path.startswith("tmp/validation/nightly_failure_issue/smoke/")
        assert (isolated_workflow_root / path).exists()
    assert (isolated_workflow_root / payload["candidate_history_path"]).exists()
    assert "triage-ci-issue" in payload["handoff_entrypoints"]["triage"]
    assert "promote-ci-issue" in payload["handoff_entrypoints"]["promote"]
    assert payload["closed_loop_checks"]["agent_handoff_section"] is True
    assert payload["closed_loop_checks"]["promotion_requires_registry_worktree"] is True
    assert payload["closed_loop_checks"]["candidate_history_tmp_only"] is True
    context_pack = json.loads((isolated_workflow_root / payload["artifacts"]["context"]).read_text(encoding="utf-8"))
    assert context_pack["llm_triage_advice"]["workflow_gate"] == "ready"
    assert context_pack["llm_triage_advice"]["llm_invocation_evidence"]["invoked"] is False
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-*.json"))


def test_batch_workflow_smoke_writes_only_tmp_artifacts_and_per_issue_closure(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_build_batch_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(item_id=kwargs["batch_id"]),
    )

    payload = workflow.build_batch_workflow_smoke_plan()

    assert payload["schema_version"] == "aistock_batch_workflow_smoke_v1"
    assert payload["workflow_gate"] == "passed"
    assert payload["github_writes"] is False
    assert payload["unexpected_dirty_paths"] == []
    assert payload["finish"]["workflow_gate"] == "ready_for_pr"
    assert payload["finish"]["scope_check"]["status"] == "passed"
    assert payload["finish"]["per_issue_commit_map"] == {
        "BUG-000": "synthetic-shared-pr",
        "BUG-001": "synthetic-shared-pr",
    }
    assert set(payload["finish"]["per_issue_closure_map"]) == {"BUG-000", "BUG-001"}
    for path in payload["artifacts"].values():
        assert path.startswith("tmp/issue_workflow/")
        assert (isolated_workflow_root / path).exists()
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-*.json"))


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
    claude_home = isolated_workflow_root / "claude_home"
    (claude_home / "commands").mkdir(parents=True)
    (claude_home / "commands" / "fix-aistock-issue.md").write_text("", encoding="utf-8")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("CLAUDE_HOME", str(claude_home))
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
    assert payload["h7_code_intelligence"]["workflow_gate"] == "ready"
    assert "run --bug-id BUG-XXX" in payload["next_command"]


def test_doctor_warns_when_bug_allocator_lags_github(
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
    _write_json(workflow.BUGS_ROOT / ".bug_id_allocator.json", {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 216})
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
        },
    )
    monkeypatch.setattr(workflow, "_mcp_config_snapshot", lambda: {"files": [], "stale_worktree_config_files": []})
    monkeypatch.setattr(workflow, "_run_command", lambda *_args, **_kwargs: {"ok": True, "stdout": "{}", "stderr": ""})
    monkeypatch.setattr(
        workflow,
        "_scan_github_bug_ids",
        lambda **_kwargs: (
            [{"bug_id": "BUG-217", "number": 217, "kind": "github_issue", "source": "https://github.example/issues/588"}],
            [],
        ),
    )
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

    payload = workflow.build_doctor_report(skip_external=False)
    compact = workflow._compact_payload(payload)

    assert payload["workflow_gate"] == "warning"
    assert payload["bug_id_allocation"]["next_number"] == 218
    assert payload["bug_id_allocation"]["github_max_number"] == 217
    assert any("bug id allocation" in warning for warning in payload["warnings"])
    assert compact["bug_id_allocation"]["next_number"] == 218


def test_doctor_compact_reports_codegraph_bootstrap_next_command(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue").mkdir(parents=True)
    (isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
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
        },
    )
    monkeypatch.setattr(workflow, "_mcp_config_snapshot", lambda: {"files": [], "stale_worktree_config_files": []})
    monkeypatch.setattr(
        workflow.code_intelligence,
        "build_doctor_report",
        lambda root, skip_external=False: {
            "schema_version": "aistock_code_intelligence_doctor_v1",
            "workflow_gate": "warning",
            "warnings": ["CodeGraph index is missing; run codegraph init -i"],
            "blocking": [],
            "codegraph": {
                "status": "missing_index",
                "index_exists": False,
                "bootstrap_command": "codegraph init -i",
            },
            "understand_anything": {"status": "not_required_missing"},
            "bootstrap_commands": {"codegraph": "codegraph init -i"},
        },
    )

    payload = workflow.build_doctor_report(skip_external=True)
    compact = workflow._compact_payload(payload)

    assert payload["h7_code_intelligence"]["readiness_next_command"] == "codegraph init -i"
    assert payload["h7_code_intelligence"]["blocking_for_issue_workflow"] is False
    assert compact["h7_code_intelligence"]["readiness_next_command"] == "codegraph init -i"


def test_doctor_omits_codegraph_bootstrap_when_ready(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue").mkdir(parents=True)
    (isolated_workflow_root / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
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
            "codegraph": {
                "status": "ok",
                "index_exists": True,
                "bootstrap_command": "codegraph init -i",
            },
            "codegraph_freshness": {
                "latest": {
                    "freshness": "fresh",
                    "artifact_path": "tmp/validation/code-intelligence/codegraph-freshness.json",
                }
            },
            "understand_anything": {"status": "not_required_missing"},
            "bootstrap_commands": {"codegraph": "codegraph init -i"},
        },
    )

    payload = workflow.build_doctor_report(skip_external=True)
    compact = workflow._compact_payload(payload)

    assert payload["h7_code_intelligence"]["workflow_gate"] == "ready"
    assert payload["h7_code_intelligence"]["fallback_used"] is False
    assert payload["h7_code_intelligence"]["codegraph_freshness"] == "fresh"
    assert payload["h7_code_intelligence"]["codegraph_freshness_ref"].endswith("codegraph-freshness.json")
    assert payload["h7_code_intelligence"]["readiness_next_command"] is None
    assert compact["h7_code_intelligence"]["codegraph_freshness"] == "fresh"
    assert compact["h7_code_intelligence"]["readiness_next_command"] is None



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
    monkeypatch.setenv("CLAUDE_HOME", str(isolated_workflow_root / "claude_home"))
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


def test_run_plan_registry_intake_creates_separate_fix_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry-worktree"
    _write_json(registry / "tests" / "aistock_validation" / "bugs" / "BUG-199.json", _bug())
    _write_json(
        registry / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "discovered",
            "workflow_role": "registry_intake",
            "source_bug_json": "tests/aistock_validation/bugs/BUG-199.json",
            "branch": "bug/registry-validation-smoke",
            "worktree": str(registry),
        },
    )
    fix = isolated_workflow_root / "fix-worktree"
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [registry])
    monkeypatch.setattr(workflow, "_target_names", lambda record, bug_id, task_slug=None: ("bug/BUG-199-fix", fix))
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    def fake_git_snapshot(root: Path) -> dict[str, Any]:
        branch = "bug/registry-validation-smoke" if root == registry else "bug/BUG-199-fix"
        return {"ok": True, "branch": branch, "dirty": False, "dirty_count": 0}

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["worktree", "add"]:
            fix.mkdir(parents=True)
        return ""

    monkeypatch.setattr(workflow, "_git_snapshot", fake_git_snapshot)
    monkeypatch.setattr(workflow, "_git", fake_git)

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="plan",
        issue_json=None,
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

    assert payload["workflow_gate"] == "planned"
    assert payload["start"]["source_bug_json"].endswith("registry-worktree/tests/aistock_validation/bugs/BUG-199.json")
    assert payload["start"]["worktree_plan"]["created"] is True
    assert payload["start"]["active_decision"]["decision"] == "create_fix_from_registry_intake"
    state = json.loads((fix / payload["start"]["state_path"]).read_text(encoding="utf-8"))
    assert state["workflow_role"] == "fix"
    assert state["worktree"] == str(fix)


def test_run_plan_dirty_registry_intake_blocks_until_registry_commit(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry-worktree"
    issue = _write_json(registry / "tests" / "aistock_validation" / "bugs" / "BUG-199.json", _bug())
    _write_json(
        registry / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "discovered",
            "workflow_role": "registry_intake",
            "source_bug_json": str(issue),
            "branch": "bug/registry-validation-smoke",
            "worktree": str(registry),
        },
    )
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [registry])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "bug/registry-validation-smoke", "dirty": True, "dirty_count": 2},
    )

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="plan",
        issue_json=None,
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
    assert payload["active_decision"]["decision"] == "blocked_dirty_registry_intake"
    assert "git commit" in payload["next_command"]
    assert "tmp/issue_workflow" not in payload["next_command"]


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


def test_pre_pr_gate_requires_commit_only_for_push_or_pr(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finish = {
        "changed_files": ["scripts/aistock_issue_workflow.py"],
        "scope_check": {"status": "passed"},
    }
    monkeypatch.setattr(
        workflow,
        "_git_status_paths",
        lambda root: [{"status": " M", "path": "scripts/aistock_issue_workflow.py"}],
    )
    monkeypatch.setattr(
        workflow,
        "_run_changed_file_lint",
        lambda changed_files, root: {"status": "passed", "python_files": changed_files, "commands": []},
    )

    warning_only = workflow._pre_pr_gate(
        finish=finish,
        validation_evidence=["python -m nox -s l0 -> passed"],
        root=isolated_workflow_root,
        require_clean=False,
    )
    blocking = workflow._pre_pr_gate(
        finish=finish,
        validation_evidence=["python -m nox -s l0 -> passed"],
        root=isolated_workflow_root,
        require_clean=True,
    )

    assert warning_only["workflow_gate"] == "passed"
    assert warning_only["warnings"]
    assert blocking["workflow_gate"] == "blocked"
    assert any("commit_required before push/PR" in item for item in blocking["blocking"])
    assert blocking["next_actions"] == ['git add <task files> && git commit -m "fix: resolve issue"']


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
        registry_pr_only=False,
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
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["workflow_gate"] == "submitted"
    bug_path = isolated_workflow_root / payload["bug_json_path"]
    assert bug_path.exists()
    record = json.loads(bug_path.read_text(encoding="utf-8"))
    assert record["bug_id"] == "BUG-118"
    assert record["github_issue_number"] == 188
    assert record["production_ddl_gate"] == "noop"
    assert payload["bug_json_path"] in record["allowed_write_scope"]
    assert "tests/aistock_validation/bugs/.bug_id_allocator.json" in record["allowed_write_scope"]
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 118
    assert (isolated_workflow_root / payload["state_path"]).exists()
    assert payload["fix_chain"]["continue_to_fix_in_same_workflow"] is True
    assert "--issue-json" in payload["fix_chain"]["next_command"]
    assert "--create-worktree" in payload["fix_chain"]["next_command"]
    active_index = isolated_workflow_root / "tmp" / "issue_workflow" / "index" / "active_bugs.json"
    active_entry = json.loads(active_index.read_text(encoding="utf-8"))["active_bugs"]["BUG-118"]
    assert active_entry["active_state"] == "discovered"
    assert active_entry["branch"] is None
    assert "git add tests/aistock_validation/bugs" in payload["next_command"]
    assert "tmp/issue_workflow" not in payload["next_command"]


def test_submit_bug_registry_pr_only_stops_after_intake(
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
        registry_pr_only=True,
        dry_run=False,
    )

    assert payload["registry_pr_only"] is True
    assert payload["fix_chain"]["registry_pr_required"] is True
    assert payload["fix_chain"]["continue_to_fix_in_same_workflow"] is False
    assert "git commit" in payload["next_command"]
    assert "tmp/issue_workflow" not in payload["next_command"]


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
        registry_pr_only=False,
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
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["workflow_gate"] == "submitted"
    assert payload["registry_root"] == str(registry)
    assert (registry / payload["bug_json_path"]).exists()
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 118


def test_submit_bug_fast_chain_writes_registration_into_fix_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    fix_root = isolated_workflow_root / "worktrees" / "BUG-118-fast-fix"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})

    def fake_fix_worktree(**kwargs: Any) -> dict[str, Any]:
        fix_root.mkdir(parents=True)
        return {
            "create_worktree": kwargs["create"],
            "dry_run": kwargs["dry_run"],
            "branch": "bug/BUG-118-fast-fix",
            "worktree": str(fix_root),
            "base": "origin/main",
            "created": True,
            "registration_strategy": "fix_pr_persists_bug_registration",
        }

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        if args[:2] == ["git", "status"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "?? tests/aistock_validation/bugs/bug118.json\n?? tests/aistock_validation/bugs/.bug_id_allocator.json",
                "stderr": "",
            }
        if args[:3] == ["git", "rev-parse", "--short=12"]:
            return {"ok": True, "returncode": 0, "stdout": "abc123def456", "stderr": ""}
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_maybe_create_fix_chain_worktree", fake_fix_worktree)
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: {"ok": True, "branch": "bug/BUG-118-fast-fix", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"})
    monkeypatch.setattr(workflow, "_branch_for_path", lambda root: "bug/BUG-118-fast-fix")
    monkeypatch.setattr(workflow, "_run_command", fake_run)

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
        create_fix_worktree=True,
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["workflow_gate"] == "submitted"
    assert payload["registration_strategy"] == "fix_pr_persists_bug_registration"
    assert payload["fix_chain"]["default_path"] == "single_fix_branch_registration_and_fix"
    assert "run --bug-id BUG-118" in payload["fix_chain"]["next_command"]
    assert (fix_root / payload["bug_json_path"]).exists()
    assert (fix_root / "tests" / "aistock_validation" / "bugs" / ".bug_id_allocator.json").exists()
    assert not list(workflow.BUGS_ROOT.glob("*BUG-118*.json"))
    assert payload["fix_registration_commit"]["workflow_gate"] == "committed"


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
        registry_pr_only=False,
        dry_run=True,
    )

    assert payload["workflow_gate"] == "needs_github_sync"
    assert payload["registry_worktree_plan"]["create_worktree"] is True
    assert payload["registry_worktree_plan"]["dry_run"] is True
    assert payload["registry_worktree_plan"]["branch"].startswith("bug/registry-paper-v2-")
    assert not (isolated_workflow_root / payload["bug_json_path"]).exists()


def test_submit_bug_allocator_scans_stale_worktrees(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    stale = isolated_workflow_root / "worktrees" / "stale-registry" / "tests" / "aistock_validation" / "bugs"
    _write_json(stale / "20260528_BUG-136-other-window.json", {"bug_id": "BUG-136", "title": "Other window"})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="Duplicate allocator regression",
        module="validation",
        severity="P1",
        description="A stale worktree should not reuse an existing BUG id.",
        expected="The next BUG id should be globally unique.",
        actual="The stale allocator points at BUG-133.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=["scripts/aistock_issue_workflow.py"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number="264",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/264",
        create_github=False,
        apply=True,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["bug_id"] == "BUG-137"
    assert payload["bug_id_allocation"]["global_max_number"] == 136
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 137


def test_submit_bug_allocator_scans_github_only_bug_ids(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 216})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})
    monkeypatch.setattr(
        workflow,
        "_scan_github_bug_ids",
        lambda **_kwargs: (
            [
                {
                    "bug_id": "BUG-217",
                    "number": 217,
                    "kind": "github_issue",
                    "source": "https://github.example/issues/588",
                    "github_issue_number": 588,
                    "github_state": "OPEN",
                }
            ],
            [],
        ),
    )

    payload = workflow.build_submit_bug_plan(
        title="Allocator GitHub-only regression",
        module="validation",
        severity="P1",
        description="A GitHub-only BUG id should advance allocation.",
        expected="The next BUG id should be globally unique.",
        actual="The local allocator points at BUG-216 while GitHub has BUG-217.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=["scripts/aistock_issue_workflow.py"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number="592",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/592",
        create_github=False,
        apply=True,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["bug_id"] == "BUG-218"
    assert payload["bug_id_allocation"]["global_max_number"] == 217
    assert payload["bug_id_allocation"]["warnings"]
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 218


def test_submit_bug_explicit_duplicate_fails_before_github_create(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_json(workflow.BUGS_ROOT / "20260528_BUG-137-existing.json", {"bug_id": "BUG-137", "title": "Existing"})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    def fail_if_called(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("gh issue create must not run for duplicate explicit BUG id")

    monkeypatch.setattr(workflow, "_execute_checked", fail_if_called)

    with pytest.raises(workflow.WorkflowError, match="BUG-137 already exists"):
        workflow.build_submit_bug_plan(
            title="Duplicate explicit id",
            module="validation",
            severity="P1",
            description="Duplicate id should fail before GitHub creation.",
            expected="No GitHub issue is created.",
            actual="Duplicate id could be embedded in GitHub.",
            reproduce_command="n/a",
            evidence_refs=[],
            changed_files=["scripts/aistock_issue_workflow.py"],
            plan_key=None,
            nox_session=None,
            candidate_type="bug",
            bug_id="BUG-137",
            github_issue_number=None,
            github_issue_url=None,
            create_github=True,
            apply=True,
            create_registry_worktree=False,
            registry_pr_only=False,
            dry_run=False,
        )


def test_submit_bug_explicit_new_id_bumps_allocator(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="Explicit allocator bump",
        module="validation",
        severity="P1",
        description="Explicit new ids must advance future allocation.",
        expected="Allocator is bumped to explicit id.",
        actual="Allocator stayed stale.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=["scripts/aistock_issue_workflow.py"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id="BUG-137",
        github_issue_number="264",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/264",
        create_github=False,
        apply=True,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["bug_id"] == "BUG-137"
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 137


def test_submit_bug_offline_github_scan_warns_but_uses_local_scan(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    monkeypatch.setattr(workflow, "_scan_github_bug_ids", lambda **_kwargs: ([], ["github BUG id scan unavailable: offline"]))

    payload = workflow.build_submit_bug_plan(
        title="Offline GitHub scan",
        module="validation",
        severity="P1",
        description="Dry-run should not require GitHub.",
        expected="Local allocation still works with a warning.",
        actual="GitHub is offline.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=["scripts/aistock_issue_workflow.py"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id=None,
        github_issue_number="264",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/264",
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=False,
    )

    assert payload["bug_id"] == "BUG-133"
    assert payload["bug_id_allocation"]["warnings"]


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
    claude_home = isolated_workflow_root / "claude_home"

    dry = workflow.build_client_install_plan(codex_home=str(codex_home), claude_home=str(claude_home))
    assert dry["workflow_gate"] == "ready_for_install"
    assert dry["dry_run"] is True

    applied = workflow.build_client_install_plan(apply=True, codex_home=str(codex_home), claude_home=str(claude_home))
    assert applied["workflow_gate"] == "installed"
    assert (codex_home / "skills" / "fix-aistock-issue" / "SKILL.md").read_text(encoding="utf-8") == "skill"
    assert (claude_home / "commands" / "fix-aistock-issue.md").read_text(encoding="utf-8") == "claude"


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
    assert resume["task_card_md"].endswith("task-card.md")
    assert resume["state"]["task_card_json"].endswith("task-card.json")
    assert resume["worktree"] is None
    assert "run --bug-id BUG-199 --mode plan --create-worktree" in resume["next_command"]


def test_run_plan_without_create_worktree_records_planned_not_actual_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    planned = isolated_workflow_root.parent / "planned-worktree"
    monkeypatch.setattr(workflow, "_target_names", lambda record, bug_id, task_slug=None: ("bug/BUG-199-planned", planned))
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

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
    state = json.loads((isolated_workflow_root / payload["start"]["state_path"]).read_text(encoding="utf-8"))

    assert state.get("worktree") is None
    assert state["planned_worktree"] == str(planned)
    assert state.get("branch") is None
    assert state["planned_branch"] == "bug/BUG-199-planned"

    resume = workflow.build_resume_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root))
    assert resume["worktree"] is None
    assert resume["planned_worktree"] == str(planned)
    assert any("planned worktree has not been created" in item for item in resume["stop_conditions"])
    assert resume["next_command"].endswith("run --bug-id BUG-199 --mode plan --create-worktree")


def test_resume_missing_recorded_worktree_recovers_as_planned_state(isolated_workflow_root: Path) -> None:
    missing = isolated_workflow_root / "missing-worktree"
    _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "context_ready",
            "branch": "bug/BUG-199-missing",
            "worktree": str(missing),
            "context_pack_md": "tmp/issue_workflow/BUG-199/context-pack.md",
        },
    )

    resume = workflow.build_resume_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root))

    assert resume["worktree"] is None
    assert resume["planned_worktree"] == str(missing)
    assert any("planned worktree has not been created" in item for item in resume["stop_conditions"])
    assert resume["next_command"].endswith("run --bug-id BUG-199 --mode plan --create-worktree")


def test_start_plan_with_created_worktree_records_actual_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    actual = isolated_workflow_root / "actual-worktree"
    monkeypatch.setattr(workflow, "_target_names", lambda record, bug_id, task_slug=None: ("bug/BUG-199-actual", actual))
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["worktree", "add"]:
            actual.mkdir(parents=True)
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)

    payload = workflow.build_start_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        create_worktree=True,
        dry_run=False,
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
    )
    state = json.loads((actual / payload["state_path"]).read_text(encoding="utf-8"))

    assert payload["worktree_plan"]["created"] is True
    assert state["worktree"] == str(actual)
    assert "planned_worktree" not in state
    assert state["branch"] == "bug/BUG-199-actual"


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

    payload = workflow.build_postmortem_plan(
        bug_id="BUG-199",
        worktree=str(isolated_workflow_root),
        persist_artifacts=True,
    )

    assert payload["schema_version"] == "aistock_issue_workflow_postmortem_v1"
    assert payload["timing_summary"]["event_count"] == 2
    assert payload["timing_summary"]["known_duration_seconds"] == 2.5
    assert payload["flow_overhead_estimate"]["context_estimated_tokens"] == 20
    assert payload["h6_summary"]["top_phase"]["phase"] == "gh_pr_create"
    assert payload["phase_cost_table"]
    assert payload["h7_code_intelligence"]["workflow_gate"] == "ready"
    assert payload["duplicate_active_count"] == 1
    postmortem_md = isolated_workflow_root / payload["postmortem_md_path"]
    assert (isolated_workflow_root / payload["postmortem_json_path"]).exists()
    assert postmortem_md.exists()
    md_text = postmortem_md.read_text(encoding="utf-8")
    assert "## H6 Cost Summary" in md_text
    assert "## H7 Code Intelligence" in md_text


def test_postmortem_defaults_to_compact_success_without_artifacts(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_root = isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199"
    _write_json(
        workflow_root / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "validation_passed",
            "branch": "bug/BUG-199-workflow",
            "worktree": str(isolated_workflow_root),
        },
    )
    events_path = workflow_root / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text(
        json.dumps(
            {
                "timestamp": "2026-05-26T00:00:05Z",
                "event": "command:validation",
                "state": "validation_passed",
                "duration_seconds": 1.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(workflow, "_active_workflows_for_bug", lambda _bug_id: [])
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda _bug_id: {"status": "checked"})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root))

    assert payload["artifact_policy"] == "compact_success_no_artifact"
    assert payload["h6_summary"]["token_usage_status"] == "unknown"
    assert payload["h6_summary"]["total_estimated_tokens"] is None
    assert "postmortem_json_path" not in payload
    assert "postmortem_md_path" not in payload
    assert not (workflow_root / "postmortem.json").exists()
    assert not (workflow_root / "postmortem.md").exists()


def test_postmortem_prefers_fix_workflow_over_registry_intake(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry-worktree"
    fix = isolated_workflow_root / "fix-worktree"
    _write_json(
        registry / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "validation_planned",
            "workflow_role": "registry_intake",
            "worktree": str(registry),
            "branch": "bug/registry-validation-smoke",
        },
    )
    _write_json(
        fix / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "pr_opened",
            "workflow_role": "fix",
            "worktree": str(fix),
            "branch": "bug/BUG-199-fix",
            "pr_url": "https://github.example/pull/199",
        },
    )
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [registry, fix])
    monkeypatch.setattr(workflow, "_active_workflows_for_bug", lambda bug_id: [])
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda bug_id: {"status": "checked", "open_prs": [], "merged_prs": []})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199")

    assert payload["workflow_root"] == str(fix)
    assert payload["state"]["state"] == "pr_opened"
    assert payload["state"]["pr_url"] == "https://github.example/pull/199"


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


def test_pre_pr_gate_blocks_unmapped_ownership_before_heavy_pr_flow(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])
    finish = {
        "changed_files": [".claude/commands/fix-aistock-issue.md"],
        "scope_check": {"status": "passed"},
        "fast_path": {
            "ownership": {
                "unmapped_count": 1,
                "unmapped": [".claude/commands/fix-aistock-issue.md"],
                "ambiguous_count": 0,
            }
        },
    }

    payload = workflow._pre_pr_gate(
        finish=finish,
        validation_evidence=["python -m nox -s l0 -> passed"],
        root=isolated_workflow_root,
        run_lint=False,
    )

    assert payload["workflow_gate"] == "blocked"
    assert "ownership check failed" in payload["blocking"][0]
    assert payload["ownership_check"]["unmapped_count"] == 1


def test_pr_check_watch_treats_missing_checks_as_pending(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda args, **kwargs: {"ok": True, "returncode": 0, "stdout": json.dumps({"statusCheckRollup": []}), "stderr": ""},
    )
    monkeypatch.setattr(workflow.time, "sleep", lambda seconds: None)

    payload = workflow._watch_pr_checks_compact(
        "BUG-199",
        "https://github.example/pull/199",
        attempts=2,
        delay_seconds=0,
    )

    assert payload["workflow_gate"] == "checks_pending"
    assert payload["check_summary"]["pending_count"] == 0
    assert len(payload["attempts"]) == 2


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


def test_close_sync_apply_blocks_canonical_root_pollution(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    with pytest.raises(workflow.WorkflowError, match="canonical root"):
        workflow.build_close_sync_plan(
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


def test_close_sync_apply_skips_github_sync_when_github_check_is_disabled(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    called = False

    def fake_sync(record: dict[str, Any], evidence_payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return {"status": "synced"}

    monkeypatch.setattr(workflow, "_sync_github_issue_after_close", fake_sync)
    monkeypatch.setattr(workflow, "_validate_close_sync_apply_target", lambda root: {"blocking": [], "warnings": []})

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


def test_parse_git_porcelain_path_handles_stripped_modified_line() -> None:
    assert (
        workflow._parse_git_porcelain_path(
            "M tests/aistock_validation/bugs/bug199.json",
        )
        == "tests/aistock_validation/bugs/bug199.json"
    )
    assert (
        workflow._parse_git_porcelain_path(
            " M tests/aistock_validation/bugs/bug199.json",
        )
        == "tests/aistock_validation/bugs/bug199.json"
    )


def test_merge_recovers_when_remote_merge_succeeds_after_local_error(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "pr", "view"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "state": "OPEN",
                        "statusCheckRollup": [
                            {"name": "unit", "status": "COMPLETED", "conclusion": "SUCCESS"}
                        ],
                    }
                ),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "merge"]:
            return {
                "ok": False,
                "returncode": 1,
                "stdout": "",
                "stderr": "fatal: 'main' is already used by worktree at 'F:/Dev/AIstock'",
            }
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}},
        },
    )

    payload = workflow._merge_pr_if_ready_for_bug("BUG-199", "https://github.example/pull/199")

    assert payload["recovered_from_local_merge_error"] is True
    assert payload["verified"]["merged"] is True
    assert any(args[:3] == ["gh", "pr", "merge"] for args in commands)
    events = (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "events.jsonl").read_text(
        encoding="utf-8"
    )
    assert "merge_remote_verified_after_local_error" in events


def test_generic_merge_helper_recovers_when_remote_merge_succeeds_after_local_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "pr", "view"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "state": "OPEN",
                        "statusCheckRollup": [
                            {"name": "unit", "status": "COMPLETED", "conclusion": "SUCCESS"}
                        ],
                    }
                ),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "merge"]:
            return {
                "ok": False,
                "returncode": 1,
                "stdout": "",
                "stderr": "fatal: 'main' is already used by worktree at 'F:/Dev/AIstock'",
            }
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}},
        },
    )

    payload = workflow._merge_pr_if_ready("https://github.example/pull/199")

    assert payload["recovered_from_local_merge_error"] is True
    assert payload["verified"]["merged"] is True
    assert any(args[:3] == ["gh", "pr", "merge"] for args in commands)


def test_close_sync_pr_commit_only_stages_bug_registry_files(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry"
    bug_path = registry / "tests" / "aistock_validation" / "bugs" / "bug199.json"
    _write_json(bug_path, _bug(status="fixed"))
    (registry / "tmp" / "issue_workflow" / "BUG-199").mkdir(parents=True)
    (registry / "tmp" / "issue_workflow" / "BUG-199" / "close-sync-evidence.json").write_text(
        "local artifact",
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        calls.append(args)
        if args[:2] == ["git", "status"] and "--" in args:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "M tests/aistock_validation/bugs/bug199.json",
                "stderr": "",
            }
        if args[:2] == ["git", "status"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": " M tests/aistock_validation/bugs/bug199.json",
                "stderr": "",
            }
        if args[:3] == ["git", "rev-parse", "--short=12"]:
            return {"ok": True, "returncode": 0, "stdout": "abc123def456", "stderr": ""}
        if args[:2] == ["gh", "pr"]:
            return {"ok": True, "returncode": 0, "stdout": "https://github.example/pull/299", "stderr": ""}
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    payload = workflow._maybe_commit_and_pr_close_sync(
        bug_id="BUG-199",
        close_sync={
            "registry_root": str(registry),
            "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
            "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
            "merged_pr": "https://github.example/pull/199",
            "merge_commit": "merge123",
            "production_gates": {"production_ddl_gate": "noop"},
        },
        validation_evidence=["python -m nox -s l0 -> passed"],
    )

    assert payload["workflow_gate"] == "pr_opened"
    assert payload["commit"] == "abc123def456"
    assert payload["pr_url"] == "https://github.example/pull/299"
    assert ["git", "add", "tests/aistock_validation/bugs/bug199.json"] in calls
    assert not any("close-sync-evidence.json" in " ".join(args) for args in calls if args[:2] == ["git", "add"])


def test_close_sync_pr_commit_reuses_existing_branch_pr_without_duplicate_commit(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry"
    bug_path = registry / "tests" / "aistock_validation" / "bugs" / "bug199.json"
    _write_json(bug_path, _bug(status="fixed"))
    calls: list[list[str]] = []

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        calls.append(args)
        if args[:2] == ["git", "status"] and "--" in args:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "M tests/aistock_validation/bugs/bug199.json",
                "stderr": "",
            }
        if args[:2] == ["git", "status"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": " M tests/aistock_validation/bugs/bug199.json",
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "list"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps(
                    [
                        {
                            "number": 299,
                            "url": "https://github.example/pull/299",
                            "headRefName": "chore/BUG-199-close-sync",
                            "title": "chore(issue): close-sync BUG-199",
                        }
                    ]
                ),
                "stderr": "",
            }
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    payload = workflow._maybe_commit_and_pr_close_sync(
        bug_id="BUG-199",
        close_sync={
            "registry_root": str(registry),
            "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
            "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
            "merged_pr": "https://github.example/pull/199",
            "merge_commit": "merge123",
            "production_gates": {"production_ddl_gate": "noop"},
        },
        validation_evidence=["python -m nox -s l0 -> passed"],
    )

    assert payload["workflow_gate"] == "pr_opened"
    assert payload["reason"] == "existing_open_close_sync_pr_for_branch"
    assert payload["pr_url"] == "https://github.example/pull/299"
    assert not any(args[:2] == ["git", "add"] for args in calls)
    assert not any(args[:2] == ["git", "commit"] for args in calls)
    assert not any(args[:2] == ["git", "push"] for args in calls)


def test_close_sync_pr_commit_blocks_unexpected_dirty_files(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry"
    bug_path = registry / "tests" / "aistock_validation" / "bugs" / "bug199.json"
    _write_json(bug_path, _bug(status="fixed"))

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        if args[:2] == ["git", "status"] and "--" in args:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": " M tests/aistock_validation/bugs/bug199.json",
                "stderr": "",
            }
        if args[:2] == ["git", "status"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": " M tests/aistock_validation/bugs/bug199.json\n M scripts/aistock_issue_workflow.py",
                "stderr": "",
            }
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    with pytest.raises(workflow.WorkflowError, match="unexpected dirty files"):
        workflow._maybe_commit_and_pr_close_sync(
            bug_id="BUG-199",
            close_sync={
                "registry_root": str(registry),
                "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
                "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
            },
            validation_evidence=["python -m nox -s l0 -> passed"],
        )


def test_close_sync_batch_updates_multiple_bug_jsons(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bug_a = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(bug_id="BUG-199", github_issue_number=199, github_issue_url="https://github.example/issues/199"),
    )
    bug_b = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug200.json",
        _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"),
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url, skip_github_check=False: {"checked": True, "merged": True, "pr": {"mergeCommit": {"oid": "merge123"}}},
    )
    monkeypatch.setattr(workflow, "_sync_github_issue_after_close", lambda record, payload, root: {"status": "synced", "bug_id": record["bug_id"]})
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: {"ok": True, "branch": "bug/close-sync-batch", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"})

    payload = workflow.build_close_sync_batch_plan(
        bug_ids=["BUG-199", "BUG-200"],
        pr_url="https://github.example/pull/299",
        apply=True,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        merge_commit=None,
        production_gates={"production_ddl_gate": "noop"},
        skip_github_check=False,
        create_registry_worktree=False,
        allow_current_worktree=True,
    )

    assert payload["workflow_gate"] == "close_synced"
    assert payload["updated_bug_jsons"] == [
        "tests/aistock_validation/bugs/bug199.json",
        "tests/aistock_validation/bugs/bug200.json",
    ]
    assert json.loads(bug_a.read_text(encoding="utf-8"))["status"] == "fixed"
    assert json.loads(bug_b.read_text(encoding="utf-8"))["pr_url"] == "https://github.example/pull/299"
    assert set(payload["github_issue_sync"]) == {"BUG-199", "BUG-200"}


def test_close_sync_pr_commit_can_use_batch_title_and_body(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "registry"
    _write_json(registry / "tests" / "aistock_validation" / "bugs" / "bug199.json", _bug(bug_id="BUG-199", status="fixed"))
    _write_json(registry / "tests" / "aistock_validation" / "bugs" / "bug200.json", _bug(bug_id="BUG-200", status="fixed"))
    calls: list[list[str]] = []

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        calls.append(args)
        if args[:2] == ["git", "status"] and "--" in args:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "M tests/aistock_validation/bugs/bug199.json\nM tests/aistock_validation/bugs/bug200.json",
                "stderr": "",
            }
        if args[:2] == ["git", "status"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": " M tests/aistock_validation/bugs/bug199.json\n M tests/aistock_validation/bugs/bug200.json",
                "stderr": "",
            }
        if args[:3] == ["git", "rev-parse", "--short=12"]:
            return {"ok": True, "returncode": 0, "stdout": "abc123def456", "stderr": ""}
        if args[:2] == ["gh", "pr"]:
            return {"ok": True, "returncode": 0, "stdout": "https://github.example/pull/399", "stderr": ""}
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    payload = workflow._maybe_commit_and_pr_close_sync(
        bug_id="BUG-199",
        close_sync={
            "schema_version": "aistock_issue_workflow_close_sync_batch_v1",
            "batch_id": "BUG-199-BUG-200",
            "bug_ids": ["BUG-199", "BUG-200"],
            "registry_root": str(registry),
            "registry_worktree_plan": {"branch": "chore/BUG-199-BUG-200-close-sync"},
            "updated_bug_jsons": [
                "tests/aistock_validation/bugs/bug199.json",
                "tests/aistock_validation/bugs/bug200.json",
            ],
            "merged_pr": "https://github.example/pull/299",
            "merge_commit": "merge123",
            "production_gates": {"production_ddl_gate": "noop"},
        },
        validation_evidence=["python -m nox -s l0 -> passed"],
    )

    assert payload["workflow_gate"] == "pr_opened"
    assert ["git", "commit", "-m", "chore(issue): close-sync BUG-199-BUG-200 after merge"] in calls
    assert any(args[:7] == ["gh", "pr", "create", "--repo", workflow.GITHUB_REPO, "--base", "main"] for args in calls)


def test_run_merge_mode_continues_to_close_sync_pr_after_recovered_merge(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    close_sync_payload = {
        "workflow_gate": "close_synced",
        "registry_root": str(isolated_workflow_root / "registry"),
        "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
        "merge_commit": "merge123",
        "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
    }

    monkeypatch.setattr(
        workflow,
        "_merge_pr_if_ready_for_bug",
        lambda bug_id, pr_url: {
            "already_merged": True,
            "recovered_from_local_merge_error": True,
            "verified": {"checked": True, "merged": True, "pr": {"mergeCommit": {"oid": "merge123"}}},
        },
    )

    captured: dict[str, Any] = {}

    def fake_close_sync(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return close_sync_payload

    monkeypatch.setattr(workflow, "build_close_sync_plan", fake_close_sync)
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: {"workflow_gate": "pr_opened", "pr_url": "https://github.example/pull/299"},
    )
    monkeypatch.setattr(
        workflow,
        "build_cleanup_after_merge_plan",
        lambda **kwargs: {"workflow_gate": "ready_for_cleanup", "branch": kwargs["branch"]},
    )

    payload = workflow.build_run_plan(
        bug_id="BUG-199",
        mode="merge",
        issue_json=str(issue),
        changed_files=[],
        create_worktree=False,
        dry_run=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
        base="origin/main",
        head="HEAD",
        pr_url="https://github.example/pull/199",
        merge=True,
        branch="bug/BUG-199-workflow",
        worktree=str(isolated_workflow_root / "task"),
    )

    assert payload["workflow_gate"] == "merged_close_synced"
    assert payload["merge"]["recovered_from_local_merge_error"] is True
    assert captured["create_registry_worktree"] is True
    assert captured["merge_commit"] == "merge123"
    assert payload["close_sync_commit"]["pr_url"] == "https://github.example/pull/299"
    assert payload["cleanup"]["workflow_gate"] == "ready_for_cleanup"


def test_merge_finalizer_persists_close_sync_and_reports_postmortem(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    close_sync_payload = {
        "workflow_gate": "close_synced",
        "registry_root": str(isolated_workflow_root / "registry"),
        "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
        "merge_commit": "merge123",
        "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
    }

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    captured: dict[str, Any] = {}

    def fake_close_sync(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return close_sync_payload

    monkeypatch.setattr(workflow, "build_close_sync_plan", fake_close_sync)
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: {"workflow_gate": "pr_opened", "pr_url": "https://github.example/pull/299"},
    )
    monkeypatch.setattr(
        workflow,
        "build_postmortem_plan",
        lambda **kwargs: {"schema_version": "aistock_issue_workflow_postmortem_v1", "workflow_root": str(isolated_workflow_root)},
    )

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=False,
        cleanup=False,
        apply=True,
    )

    assert payload["workflow_gate"] == "close_sync_persisted"
    assert captured["create_registry_worktree"] is True
    assert captured["merge_commit"] == "merge123"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "ready_for_merge"
    assert payload["postmortem"]["schema_version"] == "aistock_issue_workflow_postmortem_v1"
    assert "merge_close_sync_pr_after_checks_are_green" in payload["next_actions"]


def test_merge_finalizer_uses_batch_close_sync_for_multiple_bug_ids(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(workflow, "_close_sync_is_complete", lambda **kwargs: None)
    monkeypatch.setattr(workflow, "_close_sync_pr_in_progress_marker", lambda **kwargs: None)
    monkeypatch.setattr(workflow, "build_close_sync_plan", lambda **kwargs: pytest.fail("single BUG close-sync should not run"))

    def fake_batch_close_sync(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "schema_version": "aistock_issue_workflow_close_sync_batch_v1",
            "workflow_gate": "close_synced",
            "batch_id": "BUG-266-BUG-267",
            "bug_ids": kwargs["bug_ids"],
            "registry_root": str(isolated_workflow_root / "registry"),
            "registry_worktree_plan": {"branch": "chore/BUG-266-BUG-267-close-sync"},
            "merged_pr": kwargs["pr_url"],
            "merge_commit": kwargs["merge_commit"],
            "updated_bug_jsons": [
                "tests/aistock_validation/bugs/bug266.json",
                "tests/aistock_validation/bugs/bug267.json",
            ],
        }

    monkeypatch.setattr(workflow, "build_close_sync_batch_plan", fake_batch_close_sync)
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: {
            "workflow_gate": "pr_opened",
            "root": str(isolated_workflow_root / "registry"),
            "branch": "chore/BUG-266-BUG-267-close-sync",
            "pr_url": "https://github.example/pull/299",
        },
    )
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id=["BUG-266", "BUG-267"],
        source_pr_url="https://github.example/pull/266",
        source_branch="bug/BUG-266-workflow-fast-lane",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=False,
        cleanup=False,
        apply=True,
    )

    assert payload["batch_mode"] is True
    assert payload["bug_ids"] == ["BUG-266", "BUG-267"]
    assert payload["close_sync"]["schema_version"] == "aistock_issue_workflow_close_sync_batch_v1"
    assert payload["close_sync_commit"]["pr_url"] == "https://github.example/pull/299"
    assert captured["bug_ids"] == ["BUG-266", "BUG-267"]
    assert captured["create_registry_worktree"] is True
    assert captured["merge_commit"] == "merge123"


def test_merge_finalizer_parser_accepts_repeated_bug_ids() -> None:
    args = workflow.build_parser().parse_args(
        [
            "merge-finalizer",
            "--bug-id",
            "BUG-266",
            "--bug-id",
            "BUG-267",
            "--source-pr-url",
            "https://github.example/pull/266",
        ]
    )

    assert args.bug_id == ["BUG-266", "BUG-267"]


def test_merge_finalizer_can_merge_close_sync_pr_and_cleanup(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    close_sync_root = isolated_workflow_root / "registry"
    cleanup_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {
            "workflow_gate": "close_synced",
            "registry_root": str(close_sync_root),
            "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
            "merge_commit": "merge123",
            "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: {
            "workflow_gate": "pr_opened",
            "root": str(close_sync_root),
            "branch": "chore/BUG-199-close-sync",
            "pr_url": "https://github.example/pull/299",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_merge_pr_if_ready_for_bug",
        lambda bug_id, pr_url: {"already_merged": False, "verified": {"pr": {"mergeCommit": {"oid": "syncmerge123"}}}},
    )
    def fake_cleanup(**kwargs: Any) -> dict[str, Any]:
        cleanup_calls.append(kwargs)
        return {
            "workflow_gate": "cleanup_done",
            "branch": kwargs["branch"],
            "worktree": kwargs.get("worktree"),
            "sync_root": kwargs.get("sync_root"),
        }

    monkeypatch.setattr(workflow, "build_cleanup_after_merge_plan", fake_cleanup)
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert payload["workflow_gate"] == "complete"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "merged"
    assert payload["close_sync_pr_merge"]["merge_commit"] == "syncmerge123"
    assert payload["cleanup"]["workflow_gate"] == "cleanup_done"
    assert payload["close_sync_cleanup"]["workflow_gate"] == "cleanup_done"
    assert cleanup_calls == [
        {
            "branch": "bug/BUG-199-workflow",
            "bug_id": "BUG-199",
            "worktree": str(isolated_workflow_root / "task"),
            "pr_url": "https://github.example/pull/199",
            "apply": True,
            "sync_root": True,
        },
        {
            "branch": "chore/BUG-199-close-sync",
            "bug_id": "BUG-199",
            "worktree": str(close_sync_root),
            "pr_url": "https://github.example/pull/299",
            "apply": True,
            "sync_root": False,
        },
    ]


def test_merge_finalizer_defers_source_cleanup_when_invoked_from_source_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    source_worktree = isolated_workflow_root / "task"
    close_sync_root = isolated_workflow_root / "registry"
    source_worktree.mkdir()
    close_sync_root.mkdir()
    cleanup_cwds: list[Path] = []

    monkeypatch.chdir(source_worktree)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {
            "workflow_gate": "close_synced",
            "registry_root": str(close_sync_root),
            "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
            "merge_commit": "merge123",
            "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: {
            "workflow_gate": "pr_opened",
            "root": str(close_sync_root),
            "branch": "chore/BUG-199-close-sync",
            "pr_url": "https://github.example/pull/299",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_merge_pr_if_ready_for_bug",
        lambda bug_id, pr_url: {"already_merged": False, "verified": {"pr": {"mergeCommit": {"oid": "syncmerge123"}}}},
    )

    def fake_cleanup(**kwargs: Any) -> dict[str, Any]:
        cleanup_cwds.append(Path.cwd())
        assert not workflow._cwd_is_inside(kwargs.get("worktree"))
        return {
            "workflow_gate": "cleanup_done",
            "branch": kwargs["branch"],
            "worktree": kwargs.get("worktree"),
            "sync_root": kwargs.get("sync_root"),
        }

    monkeypatch.setattr(workflow, "build_cleanup_after_merge_plan", fake_cleanup)
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(source_worktree),
        validation_evidence=["python -m nox -s l0 -> passed"],
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert payload["workflow_gate"] == "close_sync_persisted"
    assert payload["cleanup_cwd_relocation"] == {
        "from": str(source_worktree),
        "to": str(isolated_workflow_root),
        "reason": "cleanup_target_contains_current_cwd",
        "relocated": True,
    }
    assert payload["source_cleanup_deferred"] is True
    assert payload["cleanup"]["workflow_gate"] == "ready_for_cleanup"
    assert payload["cleanup"]["reason"] == "source_worktree_contains_invoking_cwd"
    assert "cleanup-after-merge" in payload["cleanup"]["next_command"]
    assert payload["next_commands"] == [payload["cleanup"]["next_command"]]
    assert cleanup_cwds == [isolated_workflow_root]
    assert Path.cwd() == isolated_workflow_root


def test_merge_finalizer_blocks_when_close_sync_cleanup_blocks(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(status="in_progress"))
    close_sync_root = isolated_workflow_root / "registry"

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {
            "workflow_gate": "close_synced",
            "registry_root": str(close_sync_root),
            "registry_worktree_plan": {"branch": "chore/BUG-199-close-sync"},
            "merge_commit": "merge123",
            "updated_bug_json": "tests/aistock_validation/bugs/bug199.json",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: {
            "workflow_gate": "pr_opened",
            "root": str(close_sync_root),
            "branch": "chore/BUG-199-close-sync",
            "pr_url": "https://github.example/pull/299",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_merge_pr_if_ready_for_bug",
        lambda bug_id, pr_url: {"already_merged": False, "verified": {"pr": {"mergeCommit": {"oid": "syncmerge123"}}}},
    )

    def fake_cleanup(**kwargs: Any) -> dict[str, Any]:
        if kwargs["branch"] == "chore/BUG-199-close-sync":
            return {"workflow_gate": "blocked", "branch": kwargs["branch"], "blocking": ["worktree is dirty"]}
        return {"workflow_gate": "cleanup_done", "branch": kwargs["branch"]}

    monkeypatch.setattr(workflow, "build_cleanup_after_merge_plan", fake_cleanup)
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["close_sync_cleanup"]["workflow_gate"] == "blocked"
    assert payload["blocking"] == ["worktree is dirty"]


def test_merge_finalizer_reuses_existing_close_sync_without_duplicate_pr(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="fixed", fix_commit="merge123", pr_url="https://github.example/pull/199"),
    )
    close_sync_calls: list[dict[str, Any]] = []
    commit_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "_stale_pr_check_for_bug",
        lambda bug_id: {
            "status": "checked",
            "open_prs": [],
            "merged_prs": [
                {
                    "number": 299,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/299",
                    "headRefName": "chore/BUG-199-close-sync",
                    "mergedAt": "2026-06-01T00:00:00Z",
                },
            ],
        },
    )

    def fail_close_sync(**kwargs: Any) -> dict[str, Any]:
        close_sync_calls.append(kwargs)
        raise AssertionError("finalizer must not rebuild close-sync for an already fixed BUG")

    def fail_commit(**kwargs: Any) -> dict[str, Any]:
        commit_calls.append(kwargs)
        raise AssertionError("finalizer must not create a duplicate close-sync PR")

    monkeypatch.setattr(workflow, "build_close_sync_plan", fail_close_sync)
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", fail_commit)
    monkeypatch.setattr(
        workflow,
        "build_cleanup_after_merge_plan",
        lambda **kwargs: {"workflow_gate": "ready_for_cleanup", "branch": kwargs["branch"]},
    )
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert close_sync_calls == []
    assert commit_calls == []
    assert payload["close_sync"]["workflow_gate"] == "already_close_synced"
    assert payload["close_sync_commit"]["workflow_gate"] == "already_merged"
    assert payload["close_sync_commit"]["pr_url"] == "https://github.example/pull/299"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "already_merged"
    assert payload["cleanup"]["workflow_gate"] == "ready_for_cleanup"
    assert payload["next_actions"] == ["rerun_cleanup_after_merge_with_apply"]


def test_merge_finalizer_tracks_open_close_sync_pr_without_duplicate_pr(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="fixed", fix_commit="merge123", pr_url="https://github.example/pull/199"),
    )
    close_sync_calls: list[dict[str, Any]] = []
    commit_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "_stale_pr_check_for_bug",
        lambda bug_id: {
            "status": "checked",
            "open_prs": [
                {
                    "number": 299,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/299",
                    "headRefName": "chore/BUG-199-close-sync",
                },
            ],
            "merged_prs": [],
        },
    )

    def fail_close_sync(**kwargs: Any) -> dict[str, Any]:
        close_sync_calls.append(kwargs)
        raise AssertionError("finalizer must not rebuild close-sync when an open close-sync PR exists")

    def fail_commit(**kwargs: Any) -> dict[str, Any]:
        commit_calls.append(kwargs)
        raise AssertionError("finalizer must not create a duplicate close-sync PR")

    monkeypatch.setattr(workflow, "build_close_sync_plan", fail_close_sync)
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", fail_commit)
    monkeypatch.setattr(workflow, "build_cleanup_after_merge_plan", lambda **kwargs: {"workflow_gate": "ready_for_cleanup"})
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=False,
        cleanup=True,
        apply=True,
    )

    assert close_sync_calls == []
    assert commit_calls == []
    assert payload["workflow_gate"] == "close_sync_persisted"
    assert payload["close_sync"]["workflow_gate"] == "already_close_synced"
    assert payload["close_sync"]["open_close_sync_pr"]["url"] == "https://github.example/pull/299"
    assert payload["close_sync_commit"]["workflow_gate"] == "pr_opened"
    assert payload["close_sync_commit"]["pr_url"] == "https://github.example/pull/299"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "ready_for_merge"
    assert payload["close_sync_pr_merge"]["pr_url"] == "https://github.example/pull/299"
    assert "merge_close_sync_pr_after_checks_are_green" in payload["next_actions"]


def test_merge_finalizer_reuses_open_close_sync_pr_before_bug_json_is_fixed(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="in_progress", fix_commit=None, pr_url=None),
    )
    close_sync_calls: list[dict[str, Any]] = []
    commit_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "_stale_pr_check_for_bug",
        lambda bug_id: {
            "status": "checked",
            "open_prs": [
                {
                    "number": 299,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/299",
                    "headRefName": "chore/BUG-199-close-sync",
                    "body": "- Source PR: https://github.example/pull/199",
                },
            ],
            "merged_prs": [],
        },
    )

    def fail_close_sync(**kwargs: Any) -> dict[str, Any]:
        close_sync_calls.append(kwargs)
        raise AssertionError("finalizer must not rebuild close-sync while an open close-sync PR is pending")

    def fail_commit(**kwargs: Any) -> dict[str, Any]:
        commit_calls.append(kwargs)
        raise AssertionError("finalizer must not append duplicate close-sync commits")

    monkeypatch.setattr(workflow, "build_close_sync_plan", fail_close_sync)
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", fail_commit)
    monkeypatch.setattr(workflow, "build_cleanup_after_merge_plan", lambda **kwargs: {"workflow_gate": "ready_for_cleanup"})
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=False,
        cleanup=True,
        apply=True,
    )

    assert close_sync_calls == []
    assert commit_calls == []
    assert payload["workflow_gate"] == "close_sync_persisted"
    assert payload["close_sync"]["workflow_gate"] == "close_sync_pr_open"
    assert payload["close_sync"]["open_close_sync_pr"]["url"] == "https://github.example/pull/299"
    assert payload["close_sync_commit"]["workflow_gate"] == "pr_opened"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "ready_for_merge"


def test_merge_finalizer_merges_existing_open_close_sync_pr(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="fixed", fix_commit="merge123", pr_url="https://github.example/pull/199"),
    )
    merged_prs: list[tuple[str, str]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "_stale_pr_check_for_bug",
        lambda bug_id: {
            "status": "checked",
            "open_prs": [
                {
                    "number": 299,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/299",
                    "headRefName": "chore/BUG-199-close-sync",
                },
            ],
            "merged_prs": [],
        },
    )
    monkeypatch.setattr(
        workflow,
        "_merge_pr_if_ready_for_bug",
        lambda bug_id, pr_url: (
            merged_prs.append((bug_id, pr_url))
            or {"already_merged": False, "verified": {"pr": {"mergeCommit": {"oid": "syncmerge123"}}}}
        ),
    )
    monkeypatch.setattr(workflow, "build_close_sync_plan", lambda **kwargs: pytest.fail("duplicate close-sync plan"))
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", lambda **kwargs: pytest.fail("duplicate close-sync PR"))
    monkeypatch.setattr(
        workflow,
        "build_cleanup_after_merge_plan",
        lambda **kwargs: {"workflow_gate": "cleanup_done", "branch": kwargs["branch"]},
    )
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert merged_prs == [("BUG-199", "https://github.example/pull/299")]
    assert payload["workflow_gate"] == "complete"
    assert payload["close_sync_commit"]["workflow_gate"] == "pr_opened"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "merged"
    assert payload["close_sync_pr_merge"]["merge_commit"] == "syncmerge123"
    assert payload["cleanup"]["workflow_gate"] == "cleanup_done"


def test_merge_finalizer_ignores_close_sync_pr_for_different_source_pr(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="fixed", fix_commit="merge123", pr_url="https://github.example/pull/199"),
    )
    merged_prs: list[tuple[str, str]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(
        workflow,
        "_stale_pr_check_for_bug",
        lambda bug_id: {
            "status": "checked",
            "open_prs": [
                {
                    "number": 299,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/299",
                    "headRefName": "chore/BUG-199-close-sync",
                    "body": "- Source PR: https://github.example/pull/199",
                },
            ],
            "merged_prs": [
                {
                    "number": 111,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/111",
                    "headRefName": "chore/BUG-111-close-sync",
                    "mergedAt": "2026-05-28T00:00:00Z",
                    "body": "- Source PR: https://github.example/pull/111",
                },
            ],
        },
    )
    monkeypatch.setattr(
        workflow,
        "_merge_pr_if_ready_for_bug",
        lambda bug_id, pr_url: (
            merged_prs.append((bug_id, pr_url))
            or {"already_merged": False, "verified": {"pr": {"mergeCommit": {"oid": "syncmerge123"}}}}
        ),
    )
    monkeypatch.setattr(workflow, "build_close_sync_plan", lambda **kwargs: pytest.fail("duplicate close-sync plan"))
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", lambda **kwargs: pytest.fail("duplicate close-sync PR"))
    monkeypatch.setattr(
        workflow,
        "build_cleanup_after_merge_plan",
        lambda **kwargs: {"workflow_gate": "cleanup_done", "branch": kwargs["branch"]},
    )
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert merged_prs == [("BUG-199", "https://github.example/pull/299")]
    assert payload["workflow_gate"] == "complete"
    assert payload["close_sync"]["open_close_sync_pr"]["url"] == "https://github.example/pull/299"
    assert payload["close_sync_commit"]["pr_url"] == "https://github.example/pull/299"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "merged"
    assert payload["close_sync_pr_merge"]["merge_commit"] == "syncmerge123"
    assert payload["cleanup"]["workflow_gate"] == "cleanup_done"


def test_merge_finalizer_detects_close_sync_from_origin_main_when_root_is_stale(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="in_progress", fix_commit=None, pr_url=None),
    )
    close_sync_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "mergeCommit": {"oid": "merge123"}, "headRefOid": "head123"},
        },
    )
    monkeypatch.setattr(workflow, "_fetch_origin_main_for_close_sync", lambda root: {"status": "fetched"})
    monkeypatch.setattr(
        workflow,
        "_find_bug_record_in_git_ref",
        lambda bug_id, **kwargs: (
            _bug(status="fixed", fix_commit="merge123", pr_url="https://github.example/pull/199"),
            "origin/main:tests/aistock_validation/bugs/bug199.json",
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_stale_pr_check_for_bug",
        lambda bug_id: {
            "status": "checked",
            "open_prs": [],
            "merged_prs": [
                {
                    "number": 299,
                    "title": "chore(issue): close-sync BUG-199",
                    "url": "https://github.example/pull/299",
                    "headRefName": "chore/BUG-199-close-sync",
                },
            ],
        },
    )

    def fail_close_sync(**kwargs: Any) -> dict[str, Any]:
        close_sync_calls.append(kwargs)
        raise AssertionError("stale local root must not force a duplicate close-sync PR")

    monkeypatch.setattr(workflow, "build_close_sync_plan", fail_close_sync)
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", fail_close_sync)
    monkeypatch.setattr(workflow, "build_cleanup_after_merge_plan", lambda **kwargs: {"workflow_gate": "ready_for_cleanup"})
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"schema_version": "postmortem"})

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        issue_json=str(stale_issue),
        source_pr_url="https://github.example/pull/199",
        source_branch="bug/BUG-199-workflow",
        source_worktree=str(isolated_workflow_root / "task"),
        validation_evidence=["python -m nox -s l0 -> passed"],
        production_gates={"production_ddl_gate": "noop"},
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=True,
    )

    assert close_sync_calls == []
    assert payload["close_sync"]["workflow_gate"] == "already_close_synced"
    assert payload["close_sync"]["snapshot_source"] == "origin_main_ref"
    assert payload["close_sync_commit"]["pr_url"] == "https://github.example/pull/299"


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
        "--allow-current-worktree",
        "--apply",
    ]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["workflow_gate"] == "close_synced"
    updated = json.loads(issue.read_text(encoding="utf-8"))
    assert updated["status"] == "fixed"
    assert updated["fix_commit"] == "abc1234"


def test_close_sync_apply_can_create_registry_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(status="in_progress"),
    )
    registry = isolated_workflow_root / "worktrees" / "BUG-199-close-sync"
    target = registry / "tests" / "aistock_validation" / "bugs" / "bug199.json"

    monkeypatch.setattr(workflow, "_close_sync_worktree_names", lambda bug_id: ("chore/BUG-199-close-sync", registry))

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["worktree", "add"]:
            _write_json(target, _bug(status="in_progress"))
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_validate_close_sync_apply_target", lambda root: {"blocking": [], "warnings": []})

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
        create_registry_worktree=True,
    )

    assert payload["workflow_gate"] == "close_synced"
    assert payload["registry_root"] == str(registry)
    assert payload["registry_worktree_plan"]["created"] is True
    assert json.loads(target.read_text(encoding="utf-8"))["fix_commit"] == "abc1234"


def test_worktree_creation_puts_branch_option_before_path(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_close_sync_worktree_names",
        lambda bug_id: ("chore/BUG-199-close-sync", isolated_workflow_root / "worktrees" / "BUG-199-close-sync"),
    )

    payload = workflow._maybe_create_close_sync_worktree(bug_id="BUG-199", create=True, dry_run=False)

    assert payload["created"] is True
    assert calls[-1] == [
        "worktree",
        "add",
        "-b",
        "chore/BUG-199-close-sync",
        str(isolated_workflow_root / "worktrees" / "BUG-199-close-sync"),
        "origin/main",
    ]


def test_close_sync_worktree_creation_reuses_clean_existing_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "worktrees" / "BUG-199-close-sync"
    registry.mkdir(parents=True)
    calls: list[list[str]] = []

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_close_sync_worktree_names",
        lambda bug_id: ("chore/BUG-199-close-sync", registry),
    )
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "chore/BUG-199-close-sync", "dirty": False, "dirty_count": 0},
    )

    payload = workflow._maybe_create_close_sync_worktree(bug_id="BUG-199", create=True, dry_run=False)

    assert payload["reused"] is True
    assert not any(args[:2] == ["worktree", "add"] for args in calls)


def test_pr_check_summary_treats_skipped_as_non_blocking() -> None:
    summary = workflow._classify_pr_checks(
        [
            {"name": "Static gate", "status": "COMPLETED", "conclusion": "SUCCESS"},
            {"name": "Auto-register CI failures as BUGs", "status": "COMPLETED", "conclusion": "SKIPPED"},
            {"name": "CodeQL", "status": "COMPLETED", "conclusion": "NEUTRAL"},
        ]
    )

    assert summary["failed"] == []
    assert summary["pending"] == []
    assert set(summary["non_blocking"]) == {"Auto-register CI failures as BUGs", "CodeQL"}
    compact = workflow._checks_summary_payload(summary)
    assert compact["failed"] == []
    assert "Auto-register CI failures as BUGs" in compact["non_blocking"]


def test_registry_intake_cleanup_removes_safe_persisted_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "worktrees" / "registry-validation-smoke"
    registry.mkdir(parents=True)
    _write_json(isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json", _bug())
    branch = "bug/registry-validation-smoke"
    commands: list[list[str]] = []

    monkeypatch.setattr(
        workflow,
        "_active_workflows_for_bug",
        lambda bug_id: [
            {
                "workflow_role": "registry_intake",
                "worktree": str(registry),
                "branch": branch,
                "issue_json": str(registry / "tests" / "aistock_validation" / "bugs" / "bug199.json"),
                "git": {"ok": True, "branch": branch, "dirty": False, "dirty_count": 0},
            }
        ],
    )

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_execute(args: list[str], **_kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)

    def fake_remove_worktree(root: Path, worktree_path: Path) -> dict[str, Any]:
        commands.append(["git", "worktree", "remove", str(worktree_path)])
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0, "fallback_used": False}

    monkeypatch.setattr(
        workflow,
        "_remove_worktree_with_reparse_fallback",
        fake_remove_worktree,
    )

    payload = workflow.build_registry_intake_cleanup_plan(
        bug_id="BUG-199",
        apply=True,
        canonical_root=str(isolated_workflow_root),
    )

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["candidates"][0]["safe"] is True
    assert ["git", "worktree", "remove", str(registry)] in commands
    assert ["git", "branch", "-D", branch] in commands


def test_registry_intake_cleanup_skips_dirty_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "worktrees" / "registry-validation-smoke"
    registry.mkdir(parents=True)
    _write_json(isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json", _bug())
    branch = "bug/registry-validation-smoke"

    monkeypatch.setattr(
        workflow,
        "_active_workflows_for_bug",
        lambda bug_id: [
            {
                "workflow_role": "registry_intake",
                "worktree": str(registry),
                "branch": branch,
                "git": {"ok": True, "branch": branch, "dirty": True, "dirty_count": 1},
            }
        ],
    )
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "")

    payload = workflow.build_registry_intake_cleanup_plan(
        bug_id="BUG-199",
        apply=True,
        canonical_root=str(isolated_workflow_root),
    )

    assert payload["workflow_gate"] == "skipped"
    assert payload["candidates"][0]["skip_reason"] == "worktree_dirty"
    assert payload["warnings"]


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


def test_cleanup_after_merge_apply_refreshes_origin_before_merge_check(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "chore/BUG-199-close-sync"
    calls: list[tuple[str, ...]] = []
    fetched = False

    def fake_run(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
        nonlocal fetched
        calls.append(tuple(args))
        if args == ["git", "fetch", "origin", "--prune"]:
            fetched = True
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        calls.append(("git", *args))
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return ""
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch if fetched else ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, sync_root=False, apply=True)

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["pre_cleanup_fetch"]["status"] == "fetched"
    assert payload["merged_into_origin_main"] is True
    assert calls.index(("git", "fetch", "origin", "--prune")) < calls.index(
        ("git", "branch", "--format=%(refname:short)", "--merged", "origin/main")
    )


def test_cleanup_after_merge_allows_origin_equivalent_root_dirty_files(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": True, "dirty_count": 1, "head": "a", "origin_main": "b"},
    )
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: ["tests/aistock_validation/bugs/bug199.json"])
    monkeypatch.setattr(workflow, "_origin_equivalent_dirty_files", lambda root, files: list(files))

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, sync_root=True)

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["origin_equivalent_dirty_files"] == ["tests/aistock_validation/bugs/bug199.json"]
    assert payload["warnings"]


def test_origin_equivalent_dirty_files_excludes_untracked_paths(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        calls.append(args)
        if args[:3] == ["git", "cat-file", "-e"]:
            exists = args[3] == "origin/main:tests/aistock_validation/bugs/bug199.json"
            return {"ok": exists, "returncode": 0 if exists else 1, "stdout": "", "stderr": ""}
        if args[:3] == ["git", "diff", "--quiet"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    equivalent = workflow._origin_equivalent_dirty_files(
        isolated_workflow_root,
        [
            "tests/aistock_validation/bugs/bug199.json",
            ".codex_tmp/qe_20260601_014515_310f_loop1_execution_truth_tmp.md",
        ],
    )

    assert equivalent == ["tests/aistock_validation/bugs/bug199.json"]
    assert ["git", "diff", "--quiet", "origin/main", "--", ".codex_tmp/qe_20260601_014515_310f_loop1_execution_truth_tmp.md"] not in calls


def test_cleanup_after_merge_apply_ignores_untracked_root_files(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    executed: list[list[str]] = []

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_execute(args: list[str], **kwargs: Any) -> dict[str, Any]:
        executed.append(args)
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": True, "dirty_count": 1, "head": "abc", "origin_main": "abc"},
    )
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [".codex_tmp/qe_20260601_014515_310f_loop1_execution_truth_tmp.md"])
    monkeypatch.setattr(workflow, "_origin_equivalent_dirty_files", lambda root, files: [])
    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, sync_root=True, apply=True)

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["unrelated_root_dirty_files"] == [".codex_tmp/qe_20260601_014515_310f_loop1_execution_truth_tmp.md"]
    assert not any(args[:2] == ["git", "restore"] for args in executed)


def test_cleanup_after_merge_uses_canonical_root_when_called_from_task_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    task_worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    task_worktree.mkdir(parents=True)
    calls: list[tuple[tuple[str, ...], Path | None]] = []
    executed: list[tuple[tuple[str, ...], Path | None]] = []

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        calls.append((tuple(args), cwd))
        if args[:2] == ["branch", "--show-current"]:
            return "main" if cwd == isolated_workflow_root else branch
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_run(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    def fake_execute(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
        executed.append((tuple(args), cwd))
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.chdir(task_worktree)
    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "abc", "origin_main": "abc"},
    )

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        worktree=str(task_worktree),
        sync_root=True,
        apply=True,
    )

    assert payload["workflow_gate"] == "cleanup_done"
    assert "currently checked-out branch" not in " ".join(payload["blocking"])
    assert payload["worktree_is_current_cwd"] is True
    assert any(item["action"] == "relocate_current_cwd" for item in payload["actions"])
    assert any(item["command"].startswith("chdir ") for item in payload["applied"])
    assert Path.cwd() == isolated_workflow_root
    assert (("branch", "--show-current"), isolated_workflow_root) in calls
    assert all(cwd == isolated_workflow_root for args, cwd in executed if args[:2] in {("git", "worktree"), ("git", "branch")})


def test_cleanup_after_merge_ignores_unrelated_dirty_files_when_root_synced(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": True, "dirty_count": 1, "head": "abc", "origin_main": "abc"},
    )
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: ["backend/services/paper_trading_v2/live_session.py"])
    monkeypatch.setattr(workflow, "_origin_equivalent_dirty_files", lambda root, files: [])

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, sync_root=True)

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["root_sync_safe_with_dirty"] is True
    assert payload["unrelated_root_dirty_files"] == ["backend/services/paper_trading_v2/live_session.py"]
    assert payload["blocking"] == []
    assert "ignore them" in " ".join(payload["warnings"])


def test_cleanup_after_merge_blocks_unrelated_dirty_files_when_root_behind(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": True, "dirty_count": 1, "head": "abc", "origin_main": "def"},
    )
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: ["backend/services/paper_trading_v2/live_session.py"])
    monkeypatch.setattr(workflow, "_origin_equivalent_dirty_files", lambda root, files: [])

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, sync_root=True)

    assert payload["workflow_gate"] == "blocked"
    assert payload["root_sync_safe_with_dirty"] is False
    assert payload["unrelated_root_dirty_files"] == ["backend/services/paper_trading_v2/live_session.py"]
    assert "dirty and not synced" in payload["blocking"][0]


def test_cleanup_after_merge_apply_can_mark_bug_complete(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    branch = "bug/BUG-199-workflow"

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(workflow, "_execute_checked", lambda *args, **kwargs: {"ok": True, "stdout": "", "stderr": "", "returncode": 0})
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())

    assert workflow.main(["cleanup-after-merge", "--branch", branch, "--bug-id", "BUG-199", "--apply"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["complete_state"]["state"] == "complete"
    assert json.loads((isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "state.json").read_text(encoding="utf-8"))["state"] == "complete"


def test_cleanup_after_merge_removes_empty_unregistered_worktree_dir(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    orphan = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    orphan.mkdir(parents=True)

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return ""
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_registered_worktree_paths", lambda cwd=None: set())
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {
                "url": pr_url,
                "headRefName": branch,
                "headRefOid": "feature123",
                "mergeCommit": {"oid": "merge123"},
            },
        },
    )
    monkeypatch.setattr(
        workflow,
        "_git_squash_head_equivalent_to_ref",
        lambda *args, **kwargs: {"verified": True, "reason": "changed_paths_equivalent", "changed_files": ["scripts/aistock_issue_workflow.py"]},
    )
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        worktree=str(orphan),
        pr_url="https://github.example/pull/199",
        sync_root=False,
        apply=True,
        canonical_root=str(isolated_workflow_root),
    )

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["worktree_exists"] is True
    assert payload["worktree_registered"] is False
    assert payload["worktree_empty"] is True
    assert not orphan.exists()
    assert any(item["command"].startswith("remove orphan worktree dir ") for item in payload["applied"])
    assert not any(item["command"].startswith("git worktree remove") for item in payload["applied"])


def test_cleanup_after_merge_defers_locked_empty_orphan_dir(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    orphan = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    orphan.mkdir(parents=True)

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return ""
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_registered_worktree_paths", lambda cwd=None: set())
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {
                "url": pr_url,
                "headRefName": branch,
                "headRefOid": "feature123",
                "mergeCommit": {"oid": "merge123"},
            },
        },
    )
    monkeypatch.setattr(
        workflow,
        "_git_squash_head_equivalent_to_ref",
        lambda *args, **kwargs: {"verified": True, "reason": "changed_paths_equivalent", "changed_files": ["scripts/aistock_issue_workflow.py"]},
    )
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())
    monkeypatch.setattr(
        workflow,
        "_remove_reparse_or_empty_tree",
        lambda path: {
            "ok": True,
            "returncode": 0,
            "stderr": "locked",
            "removed": [],
            "deferred": True,
            "deferred_reason": "empty_directory_locked_by_windows_handle",
            "profile": {"safe_reparse_or_empty_only": True},
        },
    )

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        worktree=str(orphan),
        pr_url="https://github.example/pull/199",
        sync_root=False,
        apply=True,
        canonical_root=str(isolated_workflow_root),
    )

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["deferred_cleanup"]["reason"] == "empty_directory_locked_by_windows_handle"
    assert payload["deferred_cleanup"]["safe_to_retry"] is True
    assert any("deferred empty worktree directory cleanup" in item for item in payload["warnings"])


def test_cleanup_after_merge_removes_orphan_reparse_only_worktree_dir(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    orphan = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    junction_like = orphan / "frontend" / "node_modules"
    junction_like.mkdir(parents=True)

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return ""
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return ""
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_registered_worktree_paths", lambda cwd=None: set())
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(workflow, "_is_reparse_or_symlink", lambda path: path == junction_like)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {"checked": True, "merged": True, "pr": {"url": pr_url, "headRefName": branch, "headRefOid": "feature123", "mergeCommit": {"oid": "merge123"}}},
    )
    monkeypatch.setattr(
        workflow,
        "_git_squash_head_equivalent_to_ref",
        lambda *args, **kwargs: {"verified": True, "reason": "changed_paths_equivalent", "changed_files": ["scripts/aistock_issue_workflow.py"]},
    )
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        worktree=str(orphan),
        pr_url="https://github.example/pull/199",
        sync_root=False,
        apply=True,
        canonical_root=str(isolated_workflow_root),
    )

    assert payload["workflow_gate"] == "cleanup_done"
    assert payload["worktree_orphan_profile"]["reparse_entries"] == ["frontend/node_modules"]
    assert not orphan.exists()
    assert any(item["command"].startswith("remove orphan worktree dir ") for item in payload["applied"])


def test_cleanup_after_merge_falls_back_when_git_worktree_remove_leaves_reparse(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    junction_like = worktree / "frontend" / "node_modules"
    junction_like.mkdir(parents=True)

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main" if cwd == isolated_workflow_root else branch
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        if args[:3] == ["git", "worktree", "remove"]:
            return {"ok": False, "returncode": 128, "stdout": "", "stderr": "Invalid argument: frontend/node_modules"}
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_registered_worktree_paths", lambda cwd=None: {worktree.resolve()})
    monkeypatch.setattr(workflow, "_is_reparse_or_symlink", lambda path: path == junction_like)
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        worktree=str(worktree),
        sync_root=False,
        apply=True,
        canonical_root=str(isolated_workflow_root),
    )

    remove_result = next(item["result"] for item in payload["applied"] if item["command"].startswith("git worktree remove"))
    assert payload["workflow_gate"] == "cleanup_done"
    assert remove_result["fallback_used"] is True
    assert not worktree.exists()


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
    monkeypatch.setattr(
        workflow,
        "_git_squash_head_equivalent_to_origin",
        lambda head_oid: {"verified": False, "reason": "missing_head_oid"},
    )
    monkeypatch.setattr(workflow, "_git_ref_exists", lambda ref, cwd=None: ref in {branch, "origin/main"})
    monkeypatch.setattr(workflow, "_git_refs_tree_equivalent", lambda left, right, cwd=None: left == branch and right == "origin/main")

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        pr_url="https://github.example/pull/195",
        sync_root=True,
    )

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["merged_into_origin_main"] is False
    assert payload["squash_merge_verified"] is True
    assert payload["tree_equivalent_to_origin_main"] is True
    assert payload["merge_verification"]["method"] == "squash_merge_branch_tree_equivalent"


def test_cleanup_after_merge_allows_squash_merge_when_remote_branch_deleted(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "feature/deleted-after-squash"

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
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "merge123", "origin_main": "merge123"},
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {
                "url": pr_url,
                "headRefName": branch,
                "headRefOid": "feature123",
                "mergeCommit": {"oid": "merge123"},
            },
        },
    )
    def fake_squash_equivalence(
        head_oid: str,
        target_ref: str,
        *,
        target_label: str | None = None,
        cwd: Path | None = None,
    ) -> dict[str, Any]:
        return {
            "head_ref": head_oid,
            "base_ref": target_label or target_ref,
            "target_ref": target_ref,
            "base": "base123",
            "changed_files": ["scripts/aistock_issue_workflow.py"],
            "verified": target_ref == "merge123",
            "reason": "changed_paths_equivalent" if target_ref == "merge123" else "changed_paths_differ",
        }

    monkeypatch.setattr(workflow, "_git_squash_head_equivalent_to_ref", fake_squash_equivalence)
    monkeypatch.setattr(workflow, "_git_ref_exists", lambda ref, cwd=None: ref in {"origin/main"})
    monkeypatch.setattr(workflow, "_git_refs_tree_equivalent", lambda left, right, cwd=None: False)

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        pr_url="https://github.example/pull/355",
        sync_root=True,
    )

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["merged_into_origin_main"] is False
    assert payload["squash_merge_verified"] is True
    assert payload["tree_equivalent_to_origin_main"] is False
    assert payload["merge_verification"]["method"] == "squash_merge_head_oid_changed_paths_equivalent_to_merge_commit"
    assert payload["merge_verification"]["tree_equivalence_ref"] == "feature123"
    assert payload["merge_verification"]["tree_equivalence_target"] == "merge123"
    assert payload["merge_verification"]["path_equivalence"]["changed_files"] == ["scripts/aistock_issue_workflow.py"]


def test_cleanup_after_merge_uses_pr_merge_commit_when_origin_drifted(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "feature/squash-with-close-sync-drift"

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

    def fake_squash_equivalence(
        head_oid: str,
        target_ref: str,
        *,
        target_label: str | None = None,
        cwd: Path | None = None,
    ) -> dict[str, Any]:
        return {
            "head_ref": head_oid,
            "base_ref": target_label or target_ref,
            "target_ref": target_ref,
            "base": "base123",
            "changed_files": [
                "scripts/aistock_issue_workflow.py",
                "tests/aistock_validation/bugs/bug172.json",
            ],
            "verified": target_ref == "merge123",
            "reason": "changed_paths_equivalent" if target_ref == "merge123" else "changed_paths_differ",
        }

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {
            "ok": True,
            "branch": "main",
            "dirty": False,
            "dirty_count": 0,
            "head": "close_sync_merge",
            "origin_main": "close_sync_merge",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {
                "url": pr_url,
                "headRefOid": "feature123",
                "mergeCommit": {"oid": "merge123"},
            },
        },
    )
    monkeypatch.setattr(workflow, "_git_squash_head_equivalent_to_ref", fake_squash_equivalence)
    monkeypatch.setattr(workflow, "_git_ref_exists", lambda ref, cwd=None: ref in {"feature123", "merge123", "origin/main"})
    monkeypatch.setattr(workflow, "_git_refs_tree_equivalent", lambda left, right, cwd=None: False)

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        pr_url="https://github.example/pull/418",
        sync_root=True,
    )

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["squash_merge_verified"] is True
    assert payload["tree_equivalent_to_origin_main"] is False
    assert payload["merge_verification"]["method"] == "squash_merge_head_oid_changed_paths_equivalent_to_merge_commit"
    assert payload["merge_verification"]["path_equivalence"]["target_ref"] == "merge123"
    assert payload["merge_verification"]["merge_commit_path_equivalence"]["verified"] is True


def test_cleanup_after_merge_blocks_when_squash_pr_head_tree_differs(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "feature/not-equivalent"

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
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "merge123", "origin_main": "merge123"},
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {
            "checked": True,
            "merged": True,
            "pr": {"url": pr_url, "headRefOid": "feature123", "mergeCommit": {"oid": "merge123"}},
        },
    )
    monkeypatch.setattr(
        workflow,
        "_git_squash_head_equivalent_to_ref",
        lambda head_oid, target_ref, **kwargs: {
            "verified": False,
            "reason": "changed_paths_differ",
            "changed_files": ["scripts/aistock_issue_workflow.py"],
            "target_ref": target_ref,
        },
    )
    monkeypatch.setattr(workflow, "_git_ref_exists", lambda ref, cwd=None: ref in {"feature123", branch, "origin/main"})
    monkeypatch.setattr(workflow, "_git_refs_tree_equivalent", lambda left, right, cwd=None: False)

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        pr_url="https://github.example/pull/356",
        sync_root=True,
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["squash_merge_verified"] is False
    assert payload["merge_verification"]["verified"] is False
    assert payload["blocking"] == [f"branch is not merged into origin/main: {branch}"]


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
    monkeypatch.setattr(workflow, "_find_superseding_main_success", lambda summary: None)

    payload = workflow.build_triage_ci_issue_plan(issue_number=197)

    assert payload["schema_version"] == "aistock_issue_workflow_triage_ci_issue_v1"
    assert payload["detected_run_id"] == "26378872481"
    assert payload["needs_bug_json"] is True
    assert payload["suggested_bug"]["module"] == "paper_v2"
    assert payload["next_command"] == (
        "python scripts/aistock_issue_workflow.py promote-ci-issue --issue 197 "
        "--create-registry-worktree --apply"
    )
    ci_dir = isolated_workflow_root / "tmp" / "issue_workflow" / "ci-issue-197"
    assert (ci_dir / "triage-ci-issue.json").exists()
    assert (ci_dir / "failure-event.json").exists()
    assert (ci_dir / "context-pack.json").exists()
    assert (ci_dir / "context-pack.md").exists()
    assert payload["failure_event"]["schema_version"] == "aistock_failure_event_v1"
    assert payload["context_pack"]["schema_version"] == "aistock_ci_failure_context_pack_v1"
    assert payload["context_pack"]["agent_handoff"]["workflow_entrypoints"]["promote"].endswith(
        "--issue 197 --create-registry-worktree --apply"
    )


def test_triage_ci_issue_incomplete_diagnostics_blocks_bug_promotion(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = {
        "number": 901,
        "title": "[P1] AIstock CI failed without run details",
        "state": "OPEN",
        "url": "https://github.com/licong01-cloud/AIstock/issues/901",
        "body": "CI failed, but the run summary is not available yet.",
        "labels": [],
    }
    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(workflow, "_find_superseding_main_success", lambda summary: None)

    payload = workflow.build_triage_ci_issue_plan(issue_number=901)

    assert payload["classification_recommendation"] == "needs_log_triage"
    assert payload["needs_bug_json"] is False
    assert payload["next_command"] == "triage_incomplete_collect_failure_diagnostics_before_bug_promotion"
    assert payload["context_pack"]["agent_handoff"]["needs_bug_json"] is False
    assert payload["context_pack"]["agent_handoff"]["workflow_entrypoints"]["promote"] == "blocked_until_diagnostic_status_complete"

    promote = workflow.build_promote_ci_issue_plan(issue_number=901, apply=True, bug_id=None)
    assert promote["workflow_gate"] == "blocked_triage_incomplete_not_code_bug"
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-*.json"))


def test_triage_ci_issue_preserves_issue_locator_and_marks_superseded_main_success(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = {
        "number": 584,
        "title": "[P1][l0] main CI failed: ##[error]Process completed with exit code 1.",
        "state": "OPEN",
        "url": "https://github.com/licong01-cloud/AIstock/issues/584",
        "body": """<!-- aistock-issue-on-test-fail:26819596553 -->

## Failure Summary

- Diagnostic status: `complete`
- Workflow/source: `AIstock CI`
- Run: https://github.com/licong01-cloud/AIstock/actions/runs/26819596553
- Branch: `main`
- Commit: `a80f327f93c3b4c235e11e0e5b25d714c4874e9e`

## Regression Locator

- last_green_status: `found`
- commit_range: `dae8bd170066..a80f327f93c3`
- previous_success_run: https://github.com/licong01-cloud/AIstock/actions/runs/26817889738

## Suggested Triage

- [ ] real_regression
- [ ] infra_flaky
""",
        "labels": [],
    }
    summary = {
        "schema_version": "aistock_ci_failure_summary_v1",
        "diagnostic_status": "complete",
        "severity": "P1",
        "workflow": "AIstock CI",
        "run_id": "26819596553",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26819596553",
        "branch": "main",
        "commit": "a80f327f93c3b4c235e11e0e5b25d714c4874e9e",
        "failed_jobs": [
            {
                "job_name": "Static gate (l0 + module registry)",
                "nox_session": "l0",
                "failed_tests": [],
                "error_signature": "##[error]Process completed with exit code 1.",
                "key_log_excerpt": ["nox > Session l0 failed."],
                "suspected_module": "validation",
                "suspected_files": [],
            }
        ],
        "suspected_modules": ["validation"],
        "suspected_files": [],
        "fingerprint": "ci-1b05b81d088257d5",
        "last_green_locator": {
            "schema_version": "aistock_ci_last_green_locator_v1",
            "status": "not_found",
            "commit_range": None,
            "previous_success_run": None,
        },
        "reproduce_command": "python -m nox -s l0",
    }

    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(workflow.ci_failure_summary, "summarize_actions_run", lambda **kwargs: summary)
    monkeypatch.setattr(
        workflow,
        "_find_superseding_main_success",
        lambda summary: {
            "run_id": "26825497246",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26825497246",
            "head_sha": "440e410af8a8e999cfc7e73a85482758a089e39f",
            "created_at": "2026-06-02T14:12:36Z",
        },
    )

    payload = workflow.build_triage_ci_issue_plan(issue_number=584)

    locator = payload["failure_event"]["last_green_locator"]
    assert locator["status"] == "found"
    assert locator["commit_range"] == "dae8bd170066..a80f327f93c3"
    assert locator["previous_success_run"]["run_id"] == "26817889738"
    assert payload["classification_recommendation"] == "superseded_by_later_main_success"
    assert payload["needs_bug_json"] is False
    assert payload["superseded_action"]["workflow_gate"] == "superseded_by_latest_main_success"
    assert payload["superseded_action"]["superseding_run"]["run_id"] == "26825497246"
    assert payload["next_command"].startswith("gh issue close 584")
    assert payload["context_pack"]["last_green_locator"]["source"] == "github_issue_body"
    assert payload["context_pack"]["agent_handoff"]["next_commands"] == [payload["next_command"]]
    assert "promote" not in payload["context_pack"]["agent_handoff"]["workflow_entrypoints"]
    assert payload["failure_event"]["candidate_status"] == "superseded_by_later_main_success"


def test_triage_ci_issue_classification_ignores_generic_infra_checklist() -> None:
    summary = {
        "diagnostic_status": "complete",
        "failed_jobs": [
            {
                "error_signature": "##[error]Process completed with exit code 1.",
                "key_log_excerpt": ["nox > Session l0 failed."],
            }
        ],
    }
    issue = {
        "title": "[P1][l0] main CI failed",
        "body": """## Failure Summary

- Diagnostic status: `complete`

## Suggested Triage

- [ ] infra_flaky
- [ ] real_regression
""",
    }

    assert workflow._classify_ci_issue(summary, issue) == "real_regression_candidate"


def test_triage_ci_issue_classification_ignores_nightly_runner_success_status() -> None:
    summary = {
        "diagnostic_status": "complete",
        "failed_jobs": [
            {
                "error_signature": "Nightly failed: nightly_l3=failure",
                "key_log_excerpt": [
                    "runner_preflight: success",
                    "nightly_l3: failure",
                    "paper_v2_live: skipped",
                ],
            }
        ],
    }
    issue = {
        "title": "P1 Nightly failed: runner=success dr=success/success l3=failure live=skipped code=success",
        "body": "Diagnostic status: `complete`",
    }

    assert workflow._classify_ci_issue(summary, issue) == "real_regression_candidate"


def test_ci_issue_janitor_dry_run_does_not_close_superseded_issue(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[int | str] = []

    def fake_triage(issue_number: int | str, **kwargs: Any) -> dict[str, Any]:
        return {
            "classification_recommendation": "superseded_by_later_main_success",
            "linked_bug": None,
            "github_issue": {"number": int(issue_number), "state": "OPEN"},
            "summary": {"workflow": "AIstock CI"},
            "superseded_action": {
                "workflow_gate": "superseded_by_latest_main_success",
                "superseding_run": {"run_id": "26899001365", "run_url": "https://github.example/runs/26899001365"},
            },
        }

    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", fake_triage)
    monkeypatch.setattr(workflow, "_close_superseded_ci_issue", lambda issue_number, *args, **kwargs: closed.append(issue_number))

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[642], apply=False)

    assert payload["workflow_gate"] == "ready_for_apply"
    assert payload["superseded_count"] == 1
    assert payload["closed_count"] == 0
    assert payload["issues"][0]["action"] == "close_superseded"
    assert payload["next_command"] == "python scripts/aistock_issue_workflow.py ci-issue-janitor --issue 642 --apply"
    assert closed == []


def test_ci_issue_janitor_apply_only_closes_superseded_unlinked_issues(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[int | str] = []

    def fake_triage(issue_number: int | str, **kwargs: Any) -> dict[str, Any]:
        issue = int(issue_number)
        if issue == 642:
            return {
                "classification_recommendation": "superseded_by_later_main_success",
                "linked_bug": None,
                "github_issue": {"number": issue, "state": "OPEN"},
                "summary": {"workflow": "AIstock CI"},
                "superseded_action": {
                    "workflow_gate": "superseded_by_latest_main_success",
                    "superseding_run": {"run_id": "26899001365", "run_url": "https://github.example/runs/26899001365"},
                },
            }
        if issue == 559:
            return {
                "classification_recommendation": "real_regression_candidate",
                "linked_bug": None,
                "github_issue": {"number": issue, "state": "OPEN"},
                "summary": {"workflow": "AIstock CI"},
                "superseded_action": None,
            }
        return {
            "classification_recommendation": "superseded_by_later_main_success",
            "linked_bug": {"bug_id": "BUG-199"},
            "github_issue": {"number": issue, "state": "OPEN"},
            "summary": {"workflow": "AIstock CI"},
            "superseded_action": {
                "workflow_gate": "superseded_by_latest_main_success",
                "superseding_run": {"run_id": "26899001365", "run_url": "https://github.example/runs/26899001365"},
            },
        }

    def fake_close(issue_number: int | str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        closed.append(issue_number)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", fake_triage)
    monkeypatch.setattr(workflow, "_close_superseded_ci_issue", fake_close)

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[642, 559, 548], apply=True)

    assert payload["workflow_gate"] == "closed"
    assert payload["superseded_count"] == 1
    assert payload["closed_issues"] == [642]
    assert payload["skipped_count"] == 2
    assert closed == [642]


def test_ci_issue_janitor_dry_run_marks_infra_issue_without_closing(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[int | str] = []

    def fake_triage(issue_number: int | str, **kwargs: Any) -> dict[str, Any]:
        issue = int(issue_number)
        return {
            "classification_recommendation": "infra_flaky",
            "needs_bug_json": False,
            "linked_bug": None,
            "github_issue": {"number": issue, "state": "OPEN"},
            "summary": {"workflow": "AIstock Nightly L3 + DR"},
            "infra_action": {
                "workflow_gate": "infra_action_required",
                "reason": "CI/Nightly failure is infrastructure, not a code regression.",
                "next_actions": ["restore runner", "rerun nightly"],
                "production_gates": {
                    "production_backend_dependency_gate": "noop",
                    "production_ddl_gate": "noop",
                    "production_frontend_dependency_gate": "noop",
                },
            },
        }

    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", fake_triage)
    monkeypatch.setattr(workflow, "_close_infra_ci_issue", lambda issue_number, *args, **kwargs: closed.append(issue_number))

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[683], apply=False)

    assert payload["workflow_gate"] == "ready_for_apply"
    assert payload["infra_count"] == 1
    assert payload["closed_count"] == 0
    assert payload["issues"][0]["action"] == "close_infra"
    assert payload["issues"][0]["infra_action"]["workflow_gate"] == "infra_action_required"
    assert payload["next_command"] == "python scripts/aistock_issue_workflow.py ci-issue-janitor --issue 683 --apply"
    assert closed == []


def test_ci_issue_janitor_apply_closes_infra_issue_without_bug_promotion(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[int | str] = []

    def fake_triage(issue_number: int | str, **kwargs: Any) -> dict[str, Any]:
        issue = int(issue_number)
        if issue == 683:
            return {
                "classification_recommendation": "infra_blocker",
                "needs_bug_json": False,
                "linked_bug": None,
                "github_issue": {"number": issue, "state": "OPEN"},
                "summary": {"workflow": "AIstock Nightly L3 + DR"},
                "infra_action": {
                    "workflow_gate": "infra_action_required",
                    "reason": "Self-hosted runner is unavailable.",
                    "next_actions": ["restore or register the self-hosted Windows GitHub Actions runner"],
                    "production_gates": {
                        "production_backend_dependency_gate": "noop",
                        "production_ddl_gate": "noop",
                        "production_frontend_dependency_gate": "noop",
                    },
                },
            }
        return {
            "classification_recommendation": "infra_flaky",
            "needs_bug_json": False,
            "linked_bug": {"bug_id": "BUG-199"},
            "github_issue": {"number": issue, "state": "OPEN"},
            "summary": {"workflow": "AIstock Nightly L3 + DR"},
            "infra_action": {"workflow_gate": "infra_action_required"},
        }

    def fake_close(issue_number: int | str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        closed.append(issue_number)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", fake_triage)
    monkeypatch.setattr(workflow, "_close_infra_ci_issue", fake_close)

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[683, 684], apply=True)

    assert payload["workflow_gate"] == "closed"
    assert payload["infra_count"] == 1
    assert payload["closed_issues"] == [683]
    assert payload["skipped_count"] == 1
    assert closed == [683]


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
        workflow,
        "_maybe_create_registry_worktree",
        lambda **kwargs: {
            "create_worktree": True,
            "dry_run": False,
            "branch": "bug/registry-ci-issue-test",
            "worktree": str(isolated_workflow_root),
            "created": False,
        },
    )
    monkeypatch.setattr(
        workflow.ci_failure_summary,
        "summarize_actions_run",
        lambda **kwargs: summary,
    )
    monkeypatch.setattr(workflow, "_find_superseding_main_success", lambda summary: None)

    payload = workflow.build_promote_ci_issue_plan(
        issue_number=197,
        apply=True,
        bug_id=None,
        create_registry_worktree=True,
    )

    assert payload["workflow_gate"] == "promoted"
    assert payload["submit_bug"]["bug_id"] == "BUG-119"
    bug_path = isolated_workflow_root / payload["submit_bug"]["bug_json_path"]
    record = json.loads(bug_path.read_text(encoding="utf-8"))
    assert record["github_issue_number"] == 197
    assert record["github_issue_url"] == "https://github.com/licong01-cloud/AIstock/issues/197"
    assert record["module"] == "paper_v2"
    assert "tmp/issue_workflow/ci-issue-197/context-pack.md" in record["evidence_uris"]
    assert record["production_ddl_gate"] == "noop"


def test_promote_ci_issue_apply_requires_registry_worktree_for_code_bug(
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
                "job_name": "Backend tests (validation_center_backend)",
                "nox_session": "validation_center_backend",
                "failed_tests": ["backend/tests/test_validation_center_api.py::test_runner"],
                "error_signature": "assert 500 == 200",
                "key_log_excerpt": ["AssertionError: assert 500 == 200"],
                "suspected_module": "validation",
                "suspected_files": ["backend/routers/validation.py"],
            }
        ],
        "suspected_modules": ["validation"],
        "suspected_files": ["backend/routers/validation.py"],
        "fingerprint": "ci-validation",
        "issue_title": "[P1][validation_center_backend] main CI failed: test_runner",
        "reproduce_command": "python -m pytest backend/tests/test_validation_center_api.py::test_runner -q -p no:cacheprovider",
    }

    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(workflow.ci_failure_summary, "summarize_actions_run", lambda **kwargs: summary)
    monkeypatch.setattr(workflow, "_find_superseding_main_success", lambda summary: None)

    payload = workflow.build_promote_ci_issue_plan(issue_number=197, apply=True, bug_id=None)

    assert payload["workflow_gate"] == "registry_worktree_required"
    assert "--create-registry-worktree --apply" in payload["next_command"]
    assert payload["triage"]["needs_bug_json"] is True
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-*.json"))


def test_triage_ci_issue_compact_output_keeps_actionable_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_issue_workflow_triage_ci_issue_v1",
            "detected_run_id": "26378872481",
            "next_command": "python scripts/aistock_issue_workflow.py promote-ci-issue --issue 197 --create-registry-worktree --apply",
            "classification_recommendation": "real_regression_candidate",
            "needs_bug_json": True,
            "failure_event_path": "tmp/issue_workflow/ci-issue-197/failure-event.json",
            "context_pack_md_path": "tmp/issue_workflow/ci-issue-197/context-pack.md",
            "github_issue": {
                "number": 197,
                "url": "https://github.com/licong01-cloud/AIstock/issues/197",
                "title": "[P1] AIstock CI failed on main",
                "state": "OPEN",
                "body": "verbose body should stay hidden",
            },
            "summary": {
                "diagnostic_status": "complete",
                "workflow": "AIstock CI",
                "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
                "suspected_modules": ["validation"],
                "suspected_files": ["scripts/aistock_issue_workflow.py"],
                "reproduce_command": "python -m nox -s validation_center_backend",
                "failed_jobs": [{"key_log_excerpt": ["large excerpt should stay hidden"]}],
            },
            "suggested_bug": {
                "module": "validation",
                "severity": "P1",
                "title": "CI failure requires triage",
                "risk_area": "ci_failure_intake",
                "allowed_write_scope": ["scripts/aistock_issue_workflow.py"],
                "required_verification": ["validation_center_backend"],
            },
        }
    )

    compact = json.loads(capsys.readouterr().out)
    assert compact["classification_recommendation"] == "real_regression_candidate"
    assert compact["diagnostic_status"] == "complete"
    assert compact["needs_bug_json"] is True
    assert compact["github_issue"]["number"] == 197
    assert compact["suggested_bug"]["module"] == "validation"
    assert compact["suggested_bug"]["allowed_write_scope_count"] == 1
    assert "body" not in compact["github_issue"]
    assert "summary" not in compact
    assert "large excerpt" not in json.dumps(compact)


def test_triage_ci_issue_compact_output_keeps_infra_action_without_full_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_issue_workflow_triage_ci_issue_v1",
            "next_command": "infra_action_required_no_code_bug",
            "classification_recommendation": "infra_blocker",
            "needs_bug_json": False,
            "github_issue": {
                "number": 257,
                "url": "https://github.com/licong01-cloud/AIstock/issues/257",
                "title": "P1 Nightly blocked: self-hosted Windows runner unavailable",
                "state": "OPEN",
            },
            "summary": {
                "diagnostic_status": "complete",
                "workflow": "AIstock Nightly L3 + DR",
                "failed_jobs": [
                    {
                        "error_signature": "runner outage",
                        "key_log_excerpt": ["full runner log should stay hidden"],
                    }
                ],
            },
            "infra_action": {
                "workflow_gate": "infra_action_required",
                "reason": "CI/Nightly failure is classified as infrastructure, not a code regression.",
                "next_actions": [
                    "restore or register the self-hosted Windows GitHub Actions runner",
                    "verify runner labels include: self-hosted, windows",
                ],
                "production_gates": {
                    "production_ddl_gate": "noop",
                    "production_frontend_dependency_gate": "noop",
                    "production_backend_dependency_gate": "noop",
                },
            },
        }
    )

    compact = json.loads(capsys.readouterr().out)
    assert compact["classification_recommendation"] == "infra_blocker"
    assert compact["needs_bug_json"] is False
    assert compact["infra_action"]["workflow_gate"] == "infra_action_required"
    assert compact["infra_action"]["next_actions"][0].startswith("restore or register")
    assert "summary" not in compact
    assert "full runner log" not in json.dumps(compact)


def test_promote_ci_issue_blocks_infra_runner_outage(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = {
        "number": 257,
        "title": "P1 Nightly blocked: self-hosted Windows runner unavailable",
        "state": "OPEN",
        "url": "https://github.com/licong01-cloud/AIstock/issues/257",
        "body": "<!-- aistock-nightly-failure:runner-preflight-unavailable -->",
        "labels": [],
    }
    summary = {
        "schema_version": "aistock_ci_failure_summary_v1",
        "diagnostic_status": "complete",
        "severity": "P1",
        "workflow": "AIstock Nightly L3 + DR",
        "run_id": "26602696543",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26602696543",
        "branch": "main",
        "commit": "9798a0b",
        "failed_jobs": [
            {
                "job_name": "Runner preflight",
                "failed_tests": [],
                "error_signature": "unable to query GitHub runner health; no online GitHub Actions runner matches required labels: self-hosted, windows",
                "key_log_excerpt": ["Process completed with exit code 2."],
                "suspected_module": "validation",
                "suspected_files": [],
            }
        ],
        "suspected_modules": ["validation"],
        "suspected_files": [],
        "fingerprint": "ci-runner",
        "issue_title": "P1 Nightly blocked: self-hosted Windows runner unavailable",
        "reproduce_command": "python scripts/aistock_runner_health.py doctor",
    }

    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(workflow.ci_failure_summary, "summarize_actions_run", lambda **kwargs: summary)
    monkeypatch.setattr(workflow, "_find_superseding_main_success", lambda summary: None)

    payload = workflow.build_promote_ci_issue_plan(issue_number=257, apply=True, bug_id=None)

    assert payload["workflow_gate"] == "blocked_infra_issue_not_code_bug"
    assert payload["triage"]["needs_bug_json"] is False
    assert payload["triage"]["next_command"] == "infra_action_required_no_code_bug"
    assert payload["infra_action"]["workflow_gate"] == "infra_action_required"
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-*.json"))












def test_submit_bug_ui_intake_hints_fill_scope_labels_and_compact_output(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 253})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="Advisory UI pagination sorting still shows raw JSON",
        module="advisory",
        severity="P1",
        description="/paper-v2/advisory page has UI controls that are hard to use.",
        expected="The UI should show structured controls and focused validation guidance.",
        actual="Raw JSON and table behavior confuse users.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=["frontend/src/app/paper-v2/advisory/page.tsx"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id="BUG-254",
        github_issue_number="714",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/714",
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=True,
    )

    record = payload["record"]
    assert payload["ui_intake_hints"]["ui_route"] == "/paper-v2/advisory"
    assert payload["ui_intake_hints"]["reproduce_required"] is True
    assert "frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts" in record["allowed_write_scope"]
    assert "frontend_tsc" in record["required_verification"]
    assert "paper_v2_ui" in record["required_verification"]
    assert payload["github_issue_labels"] == ["aistock:bug", "bug", "P1", "severity:p1", "module:paper_v2", "status:open", "paper-v2"]
    body = workflow._render_github_issue_body(record, {"candidate_id": "IC-test"})
    assert "## UI Intake Hints" in body
    assert "reproduce_required" in body

    workflow._emit(payload)
    compact = json.loads(capsys.readouterr().out)
    assert compact["ui_intake_hints"]["scope_count"] >= 3
    assert compact["workflow_efficiency_recommendations"]["compact_success_output"] is True
    assert "ui_component_scope" not in json.dumps(compact)


def test_submit_bug_does_not_infer_ui_hints_from_workflow_script_text(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 264})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="Issue workflow CI watch mentions statusCheckRollup",
        module="validation",
        severity="P1",
        description="statusCheckRollup output should stay compact for workflow scripts.",
        expected="No visual UI intake is created for non-frontend workflow files.",
        actual="Script text was previously inferred as a UI route.",
        reproduce_command="python scripts/aistock_issue_workflow.py watch-ci --bug-id BUG-1 --pr-url <url>",
        evidence_refs=[],
        changed_files=["scripts/aistock_issue_workflow.py", ".claude/commands/fix-aistock-issue.md"],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id="BUG-265",
        github_issue_number="765",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/765",
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=True,
    )

    assert payload["ui_intake_hints"] is None
    assert "ui_intake_hints" not in payload["record"]


def test_postmortem_reports_queue_time_from_bug_created_at(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _bug(created_at="2026-06-04T05:17:35Z", first_seen_at="2026-06-04T05:17:35Z"),
    )
    _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "validation_passed",
            "branch": "bug/BUG-199-workflow",
            "worktree": str(isolated_workflow_root),
            "source_bug_json": str(issue.relative_to(isolated_workflow_root)),
        },
    )
    events_path = isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text(
        json.dumps({"timestamp": "2026-06-04T07:47:45Z", "event": "state:context_ready", "state": "context_ready"}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(workflow, "_active_workflows_for_bug", lambda bug_id: [])
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda bug_id: {"status": "checked", "open_prs": [], "merged_prs": []})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root), output_markdown=False)

    assert payload["timing_summary"]["queue_seconds"] == 9010.0
    assert payload["timing_summary"]["issue_created_at"] == "2026-06-04T05:17:35Z"
    assert payload["h6_summary"]["queue_seconds"] == 9010.0


def test_postmortem_falls_back_to_prior_artifact_after_state_cleanup(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = {
        "schema_version": "aistock_issue_workflow_postmortem_v1",
        "bug_id": "BUG-199",
        "workflow_root": str(isolated_workflow_root / "removed-worktree"),
        "timing_summary": {"known_duration_seconds": 12.0},
    }
    _write_json(isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "postmortem.json", prior)
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [isolated_workflow_root / "removed-worktree"])

    payload = workflow.build_postmortem_plan(bug_id="BUG-199", output_markdown=False)

    assert payload["workflow_gate"] == "artifact_fallback"
    assert payload["artifact_fallback"]["reason"] == "workflow_state_missing_or_cleaned"
    assert payload["timing_summary"]["known_duration_seconds"] == 12.0


def test_postmortem_prefers_prior_phase_evidence_after_cleanup_state(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_dir = isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199"
    _write_json(
        workflow_dir / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "complete",
            "worktree": str(isolated_workflow_root),
        },
    )
    events_path = workflow_dir / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text(
        json.dumps({"timestamp": "2026-06-04T18:00:00Z", "event": "state:complete", "state": "complete"}) + "\n",
        encoding="utf-8",
    )
    _write_json(
        workflow_dir / "postmortem-pre-cleanup.json",
        {
            "schema_version": "aistock_issue_workflow_postmortem_v1",
            "bug_id": "BUG-199",
            "workflow_root": str(isolated_workflow_root),
            "timing_summary": {"event_count": 5, "known_duration_seconds": 42.0},
        },
    )
    monkeypatch.setattr(workflow, "_active_workflows_for_bug", lambda bug_id: [])
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda bug_id: {"status": "checked", "open_prs": [], "merged_prs": []})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root), output_markdown=False)

    assert payload["workflow_gate"] == "artifact_fallback"
    assert payload["artifact_fallback"]["reason"] == "prior_postmortem_has_more_phase_evidence_than_cleanup_state"
    assert payload["timing_summary"]["event_count"] == 5
    assert payload["timing_summary"]["known_duration_seconds"] == 42.0


def test_sync_github_issue_after_close_comment_uses_persisted_not_completed(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        calls.append(args)
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    record = _bug(github_issue_number=714)
    payload = {
        "workflow_gate": "close_synced",
        "merged_pr": "https://github.example/pull/730",
        "merge_commit": "abc123",
        "validation_evidence": ["python -m nox -s l0 -> passed"],
        "production_gates": {"production_ddl_gate": "noop"},
    }

    result = workflow._sync_github_issue_after_close(record, payload, root=isolated_workflow_root)

    assert result["status"] == "synced"
    comment_path = isolated_workflow_root / result["comment_path"]
    text = comment_path.read_text(encoding="utf-8")
    assert "close-sync persisted to the current registry worktree" in text
    assert "close-sync completed" not in text
    assert "`origin/main`" in text
    assert any(args[:3] == ["gh", "issue", "comment"] for args in calls)
