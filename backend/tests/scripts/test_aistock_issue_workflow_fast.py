from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

import scripts.aistock_issue_workflow as workflow
from scripts.aistock_bug_id_allocator import compact_terminal_reservation


def _result(*, ok: bool = True, stdout: str = "", stderr: str = "", returncode: int = 0) -> dict[str, Any]:
    return {"ok": ok, "stdout": stdout, "stderr": stderr, "returncode": returncode}


def test_merge_uses_only_github_required_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    monkeypatch.setattr(
        workflow,
        "_execute_checked",
        lambda args, **kwargs: _result(
            stdout=json.dumps(
                {
                    "state": "OPEN",
                    "statusCheckRollup": [
                        {"name": "CI verdict", "status": "COMPLETED", "conclusion": "SUCCESS"},
                        {"name": "advisory", "status": "COMPLETED", "conclusion": "FAILURE"},
                    ],
                }
            )
        ),
    )

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "pr", "checks"]:
            return _result(
                stdout=json.dumps(
                    [{"name": "CI verdict", "state": "SUCCESS", "bucket": "pass", "workflow": "AIstock CI"}]
                )
            )
        if args[:3] == ["gh", "pr", "merge"]:
            return _result()
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {"checked": True, "merged": True, "pr": {"mergeCommit": {"oid": "merge123"}}},
    )

    payload = workflow._merge_pr_if_ready("https://github.example/pull/199")

    assert payload["check_summary"]["passed"] == ["CI verdict"]
    assert any(args[:3] == ["gh", "pr", "merge"] for args in commands)


def test_required_check_unknown_bucket_fails_closed() -> None:
    summary = workflow._required_pr_check_summary(
        _result(stdout=json.dumps([{"name": "CI verdict", "bucket": "mystery"}]))
    )

    assert summary["failed"] == ["CI verdict"]
    assert summary["passed"] == []


def test_find_bug_record_parses_only_matching_or_opaque_filenames(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matching = tmp_path / "20260813_BUG-199-example.json"
    unrelated = tmp_path / "20260813_BUG-200-example.json"
    opaque = tmp_path / "legacy.json"
    matching.write_text(json.dumps({"bug_id": "BUG-199"}), encoding="utf-8")
    unrelated.write_text(json.dumps({"bug_id": "BUG-200"}), encoding="utf-8")
    opaque.write_text(json.dumps({"bug_id": "BUG-201"}), encoding="utf-8")
    loaded: list[Path] = []
    original = workflow._load_json

    def recording_load(path: Path) -> dict[str, Any]:
        loaded.append(path)
        return original(path)

    monkeypatch.setattr(workflow, "_bug_files", lambda: [matching, unrelated, opaque])
    monkeypatch.setattr(workflow, "_load_json", recording_load)

    record, path = workflow.find_bug_record("BUG-199")

    assert record["bug_id"] == "BUG-199"
    assert path == matching
    assert loaded == [matching, opaque]


def test_compact_terminal_reservation_is_exact_and_keeps_other_records(tmp_path: Path) -> None:
    target = tmp_path / "BUG-199.json"
    other = tmp_path / "BUG-200.json"
    target.write_text(json.dumps({"bug_id": "BUG-199", "status": "registered"}), encoding="utf-8")
    other.write_text(json.dumps({"bug_id": "BUG-200", "status": "registered"}), encoding="utf-8")

    removed = compact_terminal_reservation(tmp_path, "BUG-199", min_age_seconds=0)

    assert removed == str(target)
    assert not target.exists()
    assert other.exists()


def test_read_command_retry_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _result(ok=calls == 3, stderr="TLS EOF" if calls < 3 else "", returncode=0 if calls == 3 else 1)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow.time, "sleep", lambda seconds: None)

    result = workflow._run_read_command_with_retry(["gh", "pr", "view", "1"], attempts=3)

    assert result["ok"] is True
    assert result["attempts"] == 3
    assert calls == 3


def test_workflow_smoke_does_not_call_full_doctor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    issue = tmp_path / "bug.json"
    issue.write_text(json.dumps({"bug_id": "BUG-199"}), encoding="utf-8")
    monkeypatch.setattr(workflow, "build_doctor_report", lambda **kwargs: pytest.fail("full doctor called"))
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])
    monkeypatch.setattr(workflow, "build_fast_path_plan", lambda **kwargs: {"workflow_gate": "planned"})
    monkeypatch.setattr(workflow, "build_start_plan", lambda **kwargs: {"bug_id": "BUG-199"})
    monkeypatch.setattr(
        workflow,
        "build_finish_plan",
        lambda **kwargs: {"workflow_gate": "plan_ready", "artifact_metrics": {}},
    )
    monkeypatch.setattr(workflow, "_workflow_timing_summary", lambda *args, **kwargs: {"event_count": 0})

    payload = workflow.build_workflow_smoke_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation",
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["client_manifest"] is None


def test_runtime_pending_close_sync_does_not_create_intermediate_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: dict[str, Any] = {}
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {"bug_id": "BUG-199", "workflow_gate": "fixed_source_pending_user_restart"},
    )
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: pytest.fail("intermediate close-sync PR created"),
    )
    monkeypatch.setattr(workflow, "_production_gates_payload", lambda args=None: {})
    monkeypatch.setattr(workflow, "_emit_args", lambda payload, args: emitted.update(payload))
    args = argparse.Namespace(
        bug_id="BUG-199",
        issue_json=None,
        pr_url="https://github.example/pull/199",
        apply=True,
        allow_missing_linkage=False,
        validation_evidence=["pytest -> passed"],
        merge_commit="a" * 40,
        skip_github_check=False,
        create_registry_worktree=True,
        allow_current_worktree=False,
        post_restart_receipt=None,
        create_pr=True,
    )

    assert workflow.cmd_close_sync(args) == 0
    assert emitted["close_sync_commit"]["workflow_gate"] == "deferred_runtime_verification"


def test_windows_process_scan_builds_full_caller_ancestor_exclusion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(workflow.os, "name", "nt")

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="[]", stderr="")

    monkeypatch.setattr(workflow.shutil, "which", lambda name: "powershell.exe")
    monkeypatch.setattr(workflow.subprocess, "run", fake_run)

    profile = workflow._worktree_active_process_profile(tmp_path)

    assert profile["reference_count"] == 0
    assert "AISTOCK_CLEANUP_CALLER_PID" in captured["env"]
    assert "ParentProcessId" in captured["args"][-1]
    assert "AISTOCK_CLEANUP_EXCLUDE_PIDS" not in captured["env"]


def test_backend_lifespan_logs_are_transient_only_for_exact_bounded_format(tmp_path: Path) -> None:
    log_root = tmp_path / "backend" / "logs"
    log_root.mkdir(parents=True)
    (log_root / "aistock.log").write_text(
        "2026-08-13 03:10:31 INFO [backend.main] lifespan validation started\n",
        encoding="utf-8",
    )
    (log_root / "errors.log").write_text("", encoding="utf-8")

    accepted, reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log", "backend/logs/errors.log"],
        worktree_path=tmp_path,
    )

    assert accepted == {"backend/logs/aistock.log", "backend/logs/errors.log"}
    assert reason == "bounded_test_created_backend_lifespan_log"

    (log_root / "aistock.log.1").write_text("rotated evidence", encoding="utf-8")
    rejected, reject_reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log", "backend/logs/errors.log", "backend/logs/aistock.log.1"],
        worktree_path=tmp_path,
    )

    assert rejected == set()
    assert reject_reason == "backend_lifespan_log_inventory_mismatch"


def test_backend_lifespan_log_format_mismatch_stays_unknown(tmp_path: Path) -> None:
    log_root = tmp_path / "backend" / "logs"
    log_root.mkdir(parents=True)
    (log_root / "aistock.log").write_text("Traceback: retain this evidence\n", encoding="utf-8")

    accepted, reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log"],
        worktree_path=tmp_path,
    )

    assert accepted == set()
    assert reason == "backend_lifespan_log_format_mismatch"


def test_cleanup_discovers_registered_worktree_when_argument_is_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    task_worktree = tmp_path / "task-worktree"
    task_worktree.mkdir()

    def fake_git(args: list[str], **kwargs: Any) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        if args[:2] == ["git", "status"]:
            return _result()
        if args[:2] == ["git", "ls-files"]:
            return _result(stdout="")
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(workflow, "_registered_worktree_for_branch", lambda value, cwd=None: task_worktree)
    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_path_is_registered_worktree", lambda path, cwd=None: True)
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: {"branch": "main", "dirty": False})
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(workflow, "_cleanup_protected_receipt_paths", lambda bug_id: set())
    monkeypatch.setattr(
        workflow,
        "_cleanup_evidence_finalization",
        lambda bug_id: {"durable_receipt_present": True, "status": "finalized_structured_receipt"},
    )

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, apply=False)

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["worktree"] == str(task_worktree)
    assert payload["worktree_registered"] is True
