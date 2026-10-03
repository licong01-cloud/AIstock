from __future__ import annotations

import argparse

import pytest

from scripts import aistock_issue_workflow as workflow
from backend.tests.scripts.test_aistock_issue_workflow_fast import cli_task_worktree  # noqa: F401


def invoke(monkeypatch, callback, command="finish"):
    args = argparse.Namespace(command=command, func=callback)
    monkeypatch.setattr(workflow, "build_parser", lambda: argparse.Namespace(parse_args=lambda argv: args))
    return workflow.main([])


@pytest.mark.parametrize("command", ["doctor", "verify-clients", "install-client", "cleanup-after-merge"])
def test_non_task_execution_keeps_canonical_root(cli_task_worktree, monkeypatch, command):  # noqa: F811
    canonical, _, _ = cli_task_worktree
    def check(args):
        assert workflow.REPO_ROOT == canonical
        return 0
    assert invoke(monkeypatch, check, command) == 0


def test_task_root_regression_is_in_the_actual_workflow_ci_slice():
    from scripts.ci_change_classifier import classify_changed_files
    path = "backend/tests/scripts/test_aistock_issue_workflow_task_root.py"
    result = classify_changed_files(
        ["scripts/aistock_issue_workflow.py", path, "noxfile.py"], repo_root=workflow.REPO_ROOT,
    )
    assert result["workflow_gate"] == "passed"
    assert path in result["workflow_test_targets"]
    assert not result["backend_required"]
