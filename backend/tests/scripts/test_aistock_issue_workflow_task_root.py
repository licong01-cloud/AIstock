from __future__ import annotations

import argparse
import json
import subprocess

import pytest

from scripts import aistock_issue_workflow as workflow


def git(root, *args):
    return subprocess.run(
        ["git", "-c", "user.name=WorkflowTest", "-c", "user.email=workflow@example.invalid", *args],
        cwd=root, check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def linked_roots(tmp_path, monkeypatch):
    canonical = tmp_path / "canonical"
    canonical.mkdir()
    git(canonical, "init")
    git(canonical, "commit", "--allow-empty", "-m", "base")
    task = tmp_path / "task"
    git(canonical, "worktree", "add", "-b", "test/task", str(task))
    nested = task / "nested"
    nested.mkdir()
    monkeypatch.setattr(workflow, "REPO_ROOT", canonical)
    monkeypatch.setattr(workflow, "SCRIPT_ROOT", canonical)
    monkeypatch.setattr(workflow, "BUGS_ROOT", canonical / "tests/aistock_validation/bugs")
    monkeypatch.chdir(nested)
    return canonical, task


def invoke(monkeypatch, callback, command="finish"):
    args = argparse.Namespace(command=command, func=callback)
    monkeypatch.setattr(workflow, "build_parser", lambda: argparse.Namespace(parse_args=lambda argv: args))
    return workflow.main([])


def write_state(task, payload):
    state = task / "tmp/issue_workflow/BUG-999/state.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps(payload), encoding="utf-8")


def test_canonical_cli_binds_task_state_and_helper_paths_then_restores(linked_roots, monkeypatch):
    canonical, task = linked_roots
    before = workflow.flow.REPO_ROOT
    write_state(task, {"worktree": "real-task"})

    def check(args):
        assert workflow.REPO_ROOT == task
        assert workflow.BUGS_ROOT == task / "tests/aistock_validation/bugs"
        assert workflow._load_state("BUG-999")["worktree"] == "real-task"
        assert workflow.flow.REPO_ROOT == task
        assert workflow.flow.FILE_OWNERSHIP == task / "tests/aistock_validation/catalog/file_ownership.yaml"
        assert workflow.flow.TEST_PLANS == task / "tests/aistock_validation/catalog/test_plans.yaml"
        assert git(workflow.REPO_ROOT, "branch", "--show-current") == "test/task"
        bug = task / "tests/aistock_validation/bugs/20261003_BUG-999.json"
        bug.parent.mkdir(parents=True)
        bug.write_text('{"bug_id":"BUG-999","status":"open"}', encoding="utf-8")
        assert workflow.find_bug_record("BUG-999")[1] == bug
        return 0

    assert invoke(monkeypatch, check) == 0
    assert workflow.REPO_ROOT == canonical
    assert workflow.flow.REPO_ROOT == before


@pytest.mark.parametrize("create", [False, True])
def test_registered_existing_task_is_reused_without_recreation(linked_roots, monkeypatch, create):
    _, task = linked_roots
    write_state(task, {"planned_worktree": str(task), "planned_branch": "test/task"})
    bug = task / "tests/aistock_validation/bugs/20261003_BUG-999.json"
    bug.parent.mkdir(parents=True)
    bug.write_text('{"bug_id":"BUG-999"}', encoding="utf-8")
    def check(args):
        plan = workflow._maybe_create_worktree(
            record={"bug_id": "BUG-999", "title": "task"}, bug_id="BUG-999",
            source_bug_json=bug, create=create, dry_run=False, task_slug=None,
        )
        assert plan["reused"] is True and not plan.get("created")
        assert plan["branch"] == "test/task"
        assert workflow._actual_and_planned_worktree(plan) == (str(task), None)
        return 0
    assert invoke(monkeypatch, check, "run") == 0


def test_reusing_existing_task_rejects_branch_identity_drift(linked_roots, monkeypatch):
    _, task = linked_roots
    write_state(task, {"worktree": str(task), "branch": "wrong/branch"})
    def check(args):
        workflow._maybe_create_worktree(
            record={"bug_id": "BUG-999"}, bug_id="BUG-999",
            source_bug_json=task / "tests/aistock_validation/bugs/BUG-999.json",
            create=False, dry_run=False, task_slug=None,
        )
        pytest.fail("branch drift must fail closed")
    assert invoke(monkeypatch, check, "run") == 2


def test_task_root_restored_after_workflow_error(linked_roots, monkeypatch):
    canonical, task = linked_roots
    def fail(args):
        assert workflow.REPO_ROOT == task
        raise workflow.WorkflowError("target validation failed")
    assert invoke(monkeypatch, fail) == 2
    assert workflow.REPO_ROOT == canonical


@pytest.mark.parametrize("command", ["doctor", "verify-clients", "install-client", "cleanup-after-merge"])
def test_non_task_execution_keeps_canonical_root(linked_roots, monkeypatch, tmp_path, command):
    canonical, _ = linked_roots
    if command is None:
        monkeypatch.chdir(tmp_path)
    def check(args):
        assert workflow.REPO_ROOT == canonical
        return 0
    assert invoke(monkeypatch, check, command or "finish") == 0


def test_foreign_repository_is_rejected_without_running_task(linked_roots, monkeypatch, tmp_path):
    canonical, _ = linked_roots
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    git(foreign, "init")
    monkeypatch.chdir(foreign)
    assert invoke(monkeypatch, lambda args: pytest.fail("foreign repository must not execute")) == 2
    assert workflow.REPO_ROOT == canonical


def test_task_root_regression_is_in_the_actual_workflow_ci_slice():
    from scripts.ci_change_classifier import classify_changed_files
    path = "backend/tests/scripts/test_aistock_issue_workflow_task_root.py"
    result = classify_changed_files(
        ["scripts/aistock_issue_workflow.py", path, "noxfile.py"], repo_root=workflow.REPO_ROOT,
    )
    assert result["workflow_gate"] == "passed"
    assert path in result["workflow_test_targets"]
    assert not result["backend_required"]
