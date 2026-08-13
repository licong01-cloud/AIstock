from __future__ import annotations

import json
import os
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
        "verify_command": "python scripts/code_intelligence_adapter.py verify-clients --item-id BUG-199",
        "stale_metadata_warning": False,
        "context": {
            "context_quality": {
                "quality": "scoped",
                "noisy_context_warning": False,
            }
        },
        "affected_tests": {"suggested_tests": []},
        "understand_anything": {"status": "not_required_missing"},
        "understand_anything_summary_ref": "tmp/issue_workflow/BUG-199/ua-validation-summary.md",
        "understand_anything_summary": {
            "status": "fallback",
            "graph_exists": False,
            "nodes_used": 0,
            "summary_ref": "tmp/issue_workflow/BUG-199/ua-validation-summary.md",
            "freshness": "base_current",
            "graph_commit": "base123",
            "current_git_commit": "head456",
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


def _write_runtime_catalog(root: Path) -> Path:
    path = root / "docs" / "standards" / "aistock_runtime_targets_v1.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "aistock_runtime_target_catalog_v1",
                "targets": {
                    "backend-main": {
                        "runtime_kind": "backend",
                        "source_globs": ["backend/**/*.py", "requirements*.txt"],
                        "production_port": 8001,
                        "isolated_validation_ports": [8011, 8012],
                        "probe_origins": ["http://127.0.0.1:8001"],
                        "operator_runbook_ref": "bug_record.runtime_contract.operator_runbook_ref",
                        "expected_identity_ref": "merged_commit",
                        "probes": {
                            "health_ref": "bug_record.runtime_contract.health_ref",
                            "identity_ref": "bug_record.runtime_contract.identity_ref",
                            "business_smoke_ref": "bug_record.runtime_contract.business_smoke_ref",
                            "database_readback_ref": "bug_record.runtime_contract.database_readback_ref",
                        },
                    },
                    "worker-scheduler": {
                        "runtime_kind": "worker_scheduler",
                        "source_globs": [
                            "scripts/dataset_release_worker.py",
                            "scripts/dataset_release_source_stage.py",
                        ],
                        "probe_origins": ["http://127.0.0.1:8001"],
                        "operator_runbook_ref": "bug_record.runtime_contract.operator_runbook_ref",
                        "expected_identity_ref": "merged_commit",
                        "probes": {
                            "health_ref": "bug_record.runtime_contract.health_ref",
                            "identity_ref": "bug_record.runtime_contract.identity_ref",
                            "business_smoke_ref": "bug_record.runtime_contract.business_smoke_ref",
                            "database_readback_ref": "bug_record.runtime_contract.database_readback_ref",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _runtime_bug(root: Path, **overrides: Any) -> dict[str, Any]:
    runbook = root / "docs" / "operations" / "example_backend_restart.md"
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text("# User-owned backend restart\n", encoding="utf-8")
    payload = _bug(
        allowed_write_scope=["backend/services/example.py"],
        runtime_contract={
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "runtime_impact": "backend",
            "target_id": "backend-main",
            "target_ids": ["backend-main"],
            "persistence_basis": "git_tracked_source",
            "fresh_process_evidence": ["isolated port 8012 import smoke passed"],
            "operator_runbook_ref": "docs/operations/example_backend_restart.md",
            "health_ref": "http://127.0.0.1:8001/api/v1/health",
            "identity_ref": "http://127.0.0.1:8001/api/v1/runtime-identity",
            "business_smoke_ref": "http://127.0.0.1:8001/api/v1/example/smoke",
            "database_readback_ref": "not_required",
        },
    )
    payload.update(overrides)
    return payload


def _passed_runtime_receipt(root: Path, record: dict[str, Any], *, expected_identity: str) -> dict[str, Any]:
    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=record["allowed_write_scope"],
        root=root,
    )
    probes = [
        {
            "name": name,
            "url": contract["target"]["probes"][name],
            "status": "passed",
            "status_code": 200,
            "response_sha256": f"sha-{name}",
            "response_bytes": 20,
        }
        for name in ("health_ref", "identity_ref", "business_smoke_ref")
    ]
    identity_proof = {
        "schema_version": "aistock_runtime_identity_proof_v1",
        "mode": "exact",
        "expected_identity": expected_identity,
        "observed_identity": expected_identity,
        "origin_main_identity": None,
        "expected_is_ancestor": True,
        "observed_in_origin_main": True,
    }
    return {
        "schema_version": workflow.RUNTIME_VERIFY_RECEIPT_SCHEMA,
        "bug_id": record["bug_id"],
        "target_id": contract["target_id"],
        "expected_identity": expected_identity,
        "observed_identity": expected_identity,
        "runtime_identity_proof": identity_proof,
        "runtime_identity_proof_digest": workflow._runtime_identity_proof_digest(identity_proof),
        "mode": "read_only",
        "tracked_files_written": False,
        "contract_digest": workflow._runtime_contract_digest(contract),
        "catalog_sha256": workflow._runtime_catalog_sha256(root=root),
        "required_probe_names": ["health_ref", "identity_ref", "business_smoke_ref"],
        "post_restart_effective_gate": "passed",
        "runtime_identity_match": True,
        "process_control_performed": False,
        "blocking": [],
        "probes": probes,
        "probe_evidence_digest": workflow._probe_evidence_digest(probes),
    }


def _write_repo_client_entrypoints(root: Path) -> None:
    for _key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        (root / ".codex" / "skills" / skill_name).mkdir(parents=True)
        (root / ".codex" / "skills" / skill_name / "SKILL.md").write_text("", encoding="utf-8")
    (root / ".claude" / "commands").mkdir(parents=True)
    for _key, command_name in workflow.CLIENT_CLAUDE_COMMANDS:
        (root / ".claude" / "commands" / command_name).write_text("", encoding="utf-8")


def test_git_subprocess_env_unsets_powershell_shell_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow.os, "name", "nt")
    monkeypatch.setenv("SHELL", r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe")

    env = workflow._subprocess_env(["git", "ls-remote", "origin"])

    assert env is not None
    assert "SHELL" not in env


def test_git_subprocess_env_keeps_git_sh_shell_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow.os, "name", "nt")
    monkeypatch.setenv("SHELL", r"C:\Program Files\Git\bin\sh.exe")

    env = workflow._subprocess_env(["git", "ls-remote", "origin"])

    assert env is not None
    assert env["SHELL"] == r"C:\Program Files\Git\bin\sh.exe"


def test_subprocess_env_ignores_non_git_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow.os, "name", "nt")
    monkeypatch.setenv("SHELL", r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe")

    assert workflow._subprocess_env(["python", "--version"]) is None


@pytest.mark.parametrize(
    "command",
    [
        ["python", "scripts/_restart_backend.py"],
        ["python", "-m", "uvicorn", "backend.main:app", "--port", str(8000 + 1)],
        ["taskkill", "/F", "/PID", "123"],
        ["sc.exe", "stop", "backend-api"],
        ["docker", "compose", "restart", "backend"],
    ],
)
def test_workflow_command_runner_refuses_user_backend_process_control(
    command: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("blocked process-control command must not reach subprocess"),
    )

    with pytest.raises(workflow.WorkflowError, match="process control is forbidden"):
        workflow._run_command(command)


def test_git_decodes_output_as_utf8_with_replacement(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(args: list[str], **kwargs: Any) -> Any:
        captured.update(kwargs)
        return workflow.subprocess.CompletedProcess(
            args,
            0,
            stdout="准备 worktree — 完成\n",
            stderr="",
        )

    monkeypatch.setattr(workflow.subprocess, "run", fake_run)

    assert workflow._git(["worktree", "list"]) == "准备 worktree — 完成"
    assert captured["encoding"] == "utf-8"
    assert captured["errors"] == "replace"


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


def test_emit_summary_for_verify_clients_keeps_compact_readiness_line(capsys: pytest.CaptureFixture[str]) -> None:
    workflow._emit(
        {
            "schema_version": "aistock_code_intelligence_client_verification_v1",
            "workflow_gate": "ready",
            "codegraph": {"status": "ok"},
            "freshness": {"effective_freshness": "fresh"},
            "understand_anything": {"status": "available", "freshness": "base_current"},
            "clients": {
                "codex_issue_skill": {"status": "ready"},
                "claude_issue_command": {"status": "ready"},
            },
            "artifacts": {
                "context_ref": "tmp/issue_workflow/BUG-331/codegraph-context.md",
                "affected_tests_ref": "tmp/issue_workflow/BUG-331/affected-tests.json",
                "ua_summary_ref": "tmp/issue_workflow/BUG-331/ua-validation-summary.md",
            },
            "efficiency": {"next_actions": ["read_task_card_code_intelligence_refs"]},
            "selected_nodes": [{"id": "noisy"}],
        },
        output_format="summary",
    )

    out = capsys.readouterr().out.strip()
    assert out.startswith("PASS verify-clients")
    assert "workflow_gate=ready" in out
    assert "codegraph=ok" in out
    assert "clients_ready=2/2" in out
    assert "read_task_card_code_intelligence_refs" in out
    assert "{" not in out
    assert "selected_nodes" not in out


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
    monkeypatch.setenv("AISTOCK_BUG_ID_STATE_PATH", str(tmp_path / "bug-id-state.json"))
    monkeypatch.setattr(
        workflow,
        "_client_source_authority",
        lambda: {
            "ready": True,
            "source": "canonical_main",
            "root": str(tmp_path),
            "commit": "a" * 40,
            "origin_main_commit": "a" * 40,
            "blocking_reason": None,
        },
    )
    monkeypatch.setattr(workflow, "_client_checkout_relation", lambda _authority: "matches_authority")
    monkeypatch.setattr(workflow, "_client_checkout_root", lambda: tmp_path)
    monkeypatch.setattr(
        workflow,
        "_publish_changed_clients_after_merge",
        lambda **_kwargs: {
            "schema_version": "aistock_merge_aftercare_client_publish_v1",
            "workflow_gate": "not_required",
            "selected_lanes": [],
            "blocking": [],
        },
    )
    monkeypatch.setattr(workflow, "_scan_github_bug_ids", lambda **_kwargs: ([], []))
    monkeypatch.setattr(workflow, "_github_bug_issue_for_id", lambda _bug_id, **_kwargs: (None, []))
    monkeypatch.setattr(workflow, "_github_bug_issue_by_number", lambda _issue_number, **_kwargs: (None, []))

    def isolated_read_command(args: list[str], cwd: Path | None = None, timeout: int = 30, attempts: int = 3) -> dict[str, Any]:
        if args[:2] == ["git", "fetch"]:
            return {**workflow._run_command(args, cwd=cwd, timeout=timeout), "attempts": 1}
        if args and args[0] == "git":
            try:
                stdout = workflow._git(args[1:], cwd=cwd, check=False)
                return {"ok": True, "returncode": 0, "stdout": stdout, "stderr": "", "attempts": 1}
            except Exception as exc:
                return {"ok": False, "returncode": 1, "stdout": "", "stderr": str(exc), "attempts": 1}
        return {**workflow._run_command(args, cwd=cwd, timeout=timeout), "attempts": 1}

    monkeypatch.setattr(workflow, "_run_read_command_with_retry", isolated_read_command)
    submit_scope_root = tmp_path / "submit-scope"
    monkeypatch.setattr(workflow, "_submit_bug_file_root", lambda: submit_scope_root)
    for relative_path in (
        "scripts/aistock_issue_workflow.py",
        ".claude/commands/fix-aistock-issue.md",
        "frontend/src/app/paper-v2/page.tsx",
        "frontend/src/app/paper-v2/advisory/page.tsx",
        "backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py",
    ):
        path = submit_scope_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# existing workflow test fixture\n", encoding="utf-8")
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
    assert payload["required_verification"] == ["guardrail_changed_files"]
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
    assert task_card["code_intelligence"]["verify_command"].startswith(
        "python scripts/code_intelligence_adapter.py verify-clients"
    )
    assert task_card["code_intelligence"]["context_quality"] == "scoped"
    assert task_card["code_intelligence"]["stale_metadata_warning"] is False
    assert task_card["code_intelligence"]["noisy_context_warning"] is False
    assert task_card["code_intelligence"]["understand_anything_summary_ref"].endswith("ua-validation-summary.md")
    assert task_card["code_intelligence"]["understand_anything_freshness"] == "base_current"
    assert task_card["code_intelligence"]["graph_first_required"] is True
    assert task_card["code_intelligence"]["graph_first_refs_ready"] is True
    assert task_card["code_intelligence"]["broad_scan_requires_scoped_miss_reason"] is True
    assert task_card["code_intelligence"]["affected_tests_count"] == 0
    assert task_card["code_intelligence"]["blocking_for_issue_workflow"] is False
    assert task_card["token_budget"]["large_graph_payload_inlined"] is False
    assert task_card["machine_json_policy"]["debug_only"] == [
        "state.json",
        "events.jsonl",
        "finish-plan.json",
        "fix-ready.json",
    ]
    assert task_card["verification_budget"]["delegated_validation"]["skill"] == "aistock-validation-delegation"
    local_loop = task_card["verification_budget"]["local_loop_policy"]
    assert "pytest --lf -q" in local_loop["failure_resume_first"]
    assert local_loop["max_final_related_matrix_runs"] == 1
    assert "do not rerun broad" in local_loop["no_repeat_rule"]
    task_card_text = task_card_md.read_text(encoding="utf-8")
    assert "delegated_validation_skill: `aistock-validation-delegation`" in task_card_text
    assert "## Local Validation Loop Policy" in task_card_text
    assert "pytest --lf -q" in task_card_text
    assert "max_final_related_matrix_runs: `1`" in task_card_text
    assert "machine JSON policy: debug/resume only" in task_card_text
    assert "suggested_tests" not in json.dumps(task_card, ensure_ascii=False)
    assert "skip_reasons" not in json.dumps(task_card, ensure_ascii=False)
    assert payload["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")
    assert payload["task_card_md"].endswith("task-card.md")
    assert payload["context_metrics"]["context_pack_md"]["estimated_tokens"] > 0
    assert payload["context_metrics"]["task_card_md"]["estimated_tokens"] > 0
    assert payload["context_metrics"]["fix_ready_json"]["bytes"] > 0
    assert json.loads(fix_ready.read_text(encoding="utf-8"))["workflow_gate"] == "allowed"




def test_start_validation_budget_defers_broad_required_plans(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "bug.json",
        _bug(
            module="miniqmt_execution_runtime",
            allowed_write_scope=["backend/services/miniqmt_execution_runtime/gray.py"],
            required_verification=[
                "backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py",
                "paper_v2_backend",
                "simulation_core_l2",
                "miniqmt_sim_stub_l3",
            ],
        ),
    )
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

    assert payload["required_verification"] == [
        "backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py",
        "miniqmt_execution_runtime_l2",
    ]
    assert payload["deferred_nightly_plans"] == [
        "paper_v2_backend",
        "simulation_core_l2",
        "miniqmt_sim_stub_l3",
    ]
    task_card_text = (isolated_workflow_root / payload["task_card_md"]).read_text(encoding="utf-8")
    assert (
        "deferred_nightly_plans: `paper_v2_backend, simulation_core_l2, miniqmt_sim_stub_l3`"
    ) in task_card_text
    state = json.loads((isolated_workflow_root / payload["state_path"]).read_text(encoding="utf-8"))
    assert state["task_card_availability"]["available"] is True


def test_validation_budget_keeps_changed_file_primary_module_plan_and_drops_fixed_l0() -> None:
    budgeted = workflow._apply_validation_budget(
        record={"required_verification": ["l0", "hmm_risk_backend"]},
        validation={
            "required_plans": ["l0", "hmm_risk_backend"],
            "recommended_plans": [],
        },
    )

    assert budgeted["required_plans"] == ["hmm_risk_backend"]
    assert budgeted["recommended_plans"] == []
    assert budgeted["deferred_nightly_plans"] == []
    assert budgeted["validation_budget_gate"]["premerge_required"] == ["hmm_risk_backend"]
    record_budget = workflow._verification_budget_for_record(
        {"module": "hmm.risk", "required_verification": ["hmm_risk_backend"]},
        validation_budget=budgeted,
    )
    assert record_budget["premerge_required_plans"] == ["hmm_risk_backend"]
    assert record_budget["deferred_nightly_verification"]["plans"] == []


def test_start_code_intelligence_uses_allowed_scope_when_no_changed_files(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug(allowed_write_scope=["scripts/aistock_issue_workflow.py"]))
    captured: dict[str, Any] = {}

    def fake_summary(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _fake_code_intelligence_summary()

    monkeypatch.setattr(workflow.code_intelligence, "build_summary", fake_summary)

    workflow.build_start_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=[],
        create_worktree=False,
        dry_run=False,
        task_slug=None,
        allow_missing_linkage=False,
        allow_closed=False,
    )

    assert captured["changed_files"] == ["scripts/aistock_issue_workflow.py"]


def test_code_intelligence_scope_falls_back_to_allowed_write_scope() -> None:
    record = _bug(allowed_write_scope=["scripts/aistock_issue_workflow.py"])

    assert workflow._code_intelligence_scope(record, []) == ["scripts/aistock_issue_workflow.py"]
    assert workflow._code_intelligence_scope(record, ["scripts/code_intelligence_adapter.py"]) == [
        "scripts/code_intelligence_adapter.py"
    ]


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
        "python -m nox -s guardrail_changed_files -> passed",
    ]) == 0
    ready = json.loads(capsys.readouterr().out)
    assert ready["workflow_gate"] == "ready_for_pr"
    assert ready["required_verification"] == ["guardrail_changed_files"]
    assert ready["validation_receipts"][0]["schema_version"] == "aistock_validation_receipt_v1"
    assert ready["validation_receipts"][0]["evidence_kind"] == "nox"
    assert ready["validation_receipts"][0]["plan"] == "guardrail_changed_files"
    assert len(ready["validation_receipts"][0]["receipt_id"]) == 16
    assert "artifact_metrics" not in ready
    assert ready["artifact_policy"] == "compact_success_no_finish_plan_json"
    assert (isolated_workflow_root / ready["pr_body_path"]).exists()
    assert not (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "finish-plan.json").exists()


def test_finish_blocks_partial_required_plan_receipts(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "bug.json",
        _bug(required_verification=["l0", "guardrail_changed_files"]),
    )
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    payload = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m nox -s l0 -> passed"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["closure_ready"] is False
    assert payload["workflow_gate"] == "blocked"
    assert payload["validation_receipt_plan_coverage"]["missing_required_plans"] == [
        "guardrail_changed_files"
    ]
    assert "missing required validation plan receipts" in payload["error"]
    assert payload["pre_pr_gate"]["workflow_gate"] == "blocked"
    assert "finish plan is not closure-ready" in payload["pre_pr_gate"]["blocking"]

    allowed_payload = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m nox -s l0 -> passed"],
        plan_only=False,
        allow_missing_evidence=True,
    )

    assert allowed_payload["closure_ready"] is False
    assert allowed_payload["draft_ready"] is False
    assert allowed_payload["workflow_gate"] == "blocked"
    assert allowed_payload["validation_receipt_plan_coverage"]["missing_required_plans"] == [
        "guardrail_changed_files"
    ]


def test_finish_rejects_unstructured_or_failed_validation_evidence(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

    for evidence in [
        "arbitrary-text",
        "custom validator -> passed",
        "python -m pytest -> passed",
        "python -m nox -s l0 -> failed",
    ]:
        assert workflow.main([
            "finish",
            "--issue-json",
            str(issue),
            "--changed-file",
            "scripts/aistock_issue_workflow.py",
            "--validation-evidence",
            evidence,
        ]) == 2
        payload = json.loads(capsys.readouterr().out)
        assert payload["workflow_gate"] == "blocked"
        assert payload["closure_ready"] is False
        assert payload["validation_receipts"] == []
        assert payload["validation_evidence_errors"]


def test_finish_blocks_duplicate_required_plan_receipts(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "bug.json",
        _bug(required_verification=["l0", "guardrail_changed_files"]),
    )
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())

    payload = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=[
            "python -m nox -s l0 -> passed",
            "python -m nox -s l0 -> passed",
            "python -m nox -s guardrail_changed_files -> passed",
        ],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["closure_ready"] is False
    assert payload["workflow_gate"] == "blocked"
    assert list(payload["validation_receipt_plan_coverage"]["duplicate_plan_receipts"]) == ["l0"]
    assert "duplicate validation plan receipts are not allowed" in payload["error"]


def test_validation_receipt_binds_allowlisted_command_to_current_commit(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "abcdef1234567890abcdef1234567890abcdef12")

    receipts, errors = workflow._build_validation_receipts(
        ["python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> 30 passed"],
        root=isolated_workflow_root,
    )

    assert errors == []
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["schema_version"] == "aistock_validation_receipt_v1"
    assert receipt["commit"] == "abcdef1234567890abcdef1234567890abcdef12"
    assert receipt["command"] == "python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q"
    assert receipt["result"] == "30 passed"
    assert receipt["status"] == "passed"
    assert receipt["evidence_kind"] == "pytest"
    assert receipt["plan"] is None
    assert len(receipt["receipt_id"]) == 16


@pytest.mark.parametrize(
    ("evidence", "expected_kind", "expected_plan"),
    [
        ("rtk nox -s guardrail_changed_files -> passed", "nox", "guardrail_changed_files"),
        (
            "rtk pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> 44 passed",
            "pytest",
            None,
        ),
    ],
)
def test_validation_receipt_accepts_rtk_wrapped_commands(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence: str,
    expected_kind: str,
    expected_plan: str | None,
) -> None:
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "abcdef1234567890abcdef1234567890abcdef12")

    receipts, errors = workflow._build_validation_receipts([evidence], root=isolated_workflow_root)

    assert errors == []
    assert receipts[0]["evidence_kind"] == expected_kind
    assert receipts[0]["plan"] == expected_plan


def test_finish_changed_files_combines_branch_and_worktree_paths(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow.flow,
        "changed_files_from_git",
        lambda base, head: ["scripts/aistock_issue_workflow.py", "AGENTS.md", ".claude/commands/fix-aistock-issue.md"],
    )
    monkeypatch.setattr(
        workflow,
        "_dirty_files",
        lambda root: ["AGENTS.md", "docs/standards/README.md", "new-untracked.md"],
    )

    assert workflow._finish_changed_files("origin/main", "HEAD", root=isolated_workflow_root) == [
        "scripts/aistock_issue_workflow.py",
        "AGENTS.md",
        ".claude/commands/fix-aistock-issue.md",
        "docs/standards/README.md",
        "new-untracked.md",
    ]


def test_normalize_changed_files_removes_only_explicit_relative_prefix() -> None:
    assert workflow._normalize_changed_files(
        ["./scripts/tool.py", ".claude/commands/tool.md", ".codex/skills/tool/SKILL.md", ".github/workflows/test.yml"]
    ) == [
        "scripts/tool.py",
        ".claude/commands/tool.md",
        ".codex/skills/tool/SKILL.md",
        ".github/workflows/test.yml",
    ]


def test_validation_receipt_reuse_key_changes_with_identity_inputs(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "abcdef1234567890abcdef1234567890abcdef12")
    evidence = ["python -m nox -s l0 -> passed"]

    first, first_errors = workflow._build_validation_receipts(
        evidence,
        root=isolated_workflow_root,
        changed_files=["scripts/aistock_issue_workflow.py"],
    )
    reused, reused_errors = workflow._build_validation_receipts(
        evidence,
        root=isolated_workflow_root,
        changed_files=["scripts/aistock_issue_workflow.py"],
    )
    changed, changed_errors = workflow._build_validation_receipts(
        evidence,
        root=isolated_workflow_root,
        changed_files=["scripts/issue_flow.py"],
    )

    assert first_errors == reused_errors == changed_errors == []
    assert first[0]["reuse_key"] == reused[0]["reuse_key"]
    assert first[0]["reuse_key"] != changed[0]["reuse_key"]


def test_runtime_contract_is_lazy_fail_closed_and_restart_plan_never_controls_process(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    runtime_record = _runtime_bug(isolated_workflow_root)
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", runtime_record)
    monkeypatch.setattr(
        workflow.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("restart-plan must not invoke process control or subprocesses"),
    )

    plan = workflow.build_restart_plan(bug_id=None, issue_json=str(issue))

    assert plan["workflow_gate"] == "operator_action_required"
    assert plan["backend_restart_owner"] == "user"
    assert plan["process_control_performed"] is False
    assert plan["target_id"] == "backend-main"
    assert plan["operator_runbook_ref"] == "docs/operations/example_backend_restart.md"

    missing_runbook = workflow.build_runtime_contract(
        record=_bug(
            allowed_write_scope=["backend/services/example.py"],
            runtime_contract={
                "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
                "runtime_impact": "backend",
                "target_id": "backend-main",
                "persistence_basis": "git_tracked_source",
                "fresh_process_evidence": ["isolated port 8012 import smoke passed"],
                "health_ref": "http://127.0.0.1:8001/health",
                "identity_ref": "http://127.0.0.1:8001/runtime-identity",
                "business_smoke_ref": "http://127.0.0.1:8001/api/example/smoke",
            },
        ),
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )
    assert "runtime target backend-main operator runbook ref is incomplete" in missing_runbook["blocking"]

    legacy = workflow.build_runtime_contract(
        record=_bug(allowed_write_scope=["backend/services/example.py"]),
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )
    assert legacy["runtime_impact"] == "backend"
    assert legacy["pre_pr_ready"] is False
    assert "legacy or runtime BUG requires an explicit runtime_contract schema upgrade" in legacy["blocking"]

    unknown = workflow.build_runtime_contract(
        record=_bug(allowed_write_scope=["tools/unknown.bin"]),
        changed_files=["tools/unknown.bin"],
        root=isolated_workflow_root,
    )
    assert unknown["runtime_impact"] == "unknown"
    assert unknown["pre_pr_ready"] is False


def test_runtime_contract_cannot_downgrade_changed_files_or_hide_multiple_targets(
    isolated_workflow_root: Path,
) -> None:
    catalog_path = _write_runtime_catalog(isolated_workflow_root)
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    catalog["targets"]["tdx-go-backend"] = {
        "runtime_kind": "backend",
        "source_globs": ["tdx-api-main/**/*.go"],
        "production_port": 19080,
        "isolated_validation_ports": [19081],
        "probe_origins": ["http://127.0.0.1:19080"],
        "operator_runbook_ref": "bug_record.runtime_contract.operator_runbook_ref",
        "expected_identity_ref": "merged_commit",
        "probes": {
            "health_ref": "bug_record.runtime_contract.health_ref",
            "identity_ref": "bug_record.runtime_contract.identity_ref",
            "business_smoke_ref": "bug_record.runtime_contract.business_smoke_ref",
            "database_readback_ref": "bug_record.runtime_contract.database_readback_ref",
        },
    }
    catalog_path.write_text(yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8")

    downgraded = workflow.build_runtime_contract(
        record=_bug(runtime_contract={"schema_version": workflow.RUNTIME_CONTRACT_SCHEMA, "runtime_impact": "none"}),
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )
    assert downgraded["runtime_impact"] == "backend"
    assert downgraded["backend_restart_required"] is True
    assert downgraded["pre_pr_ready"] is False
    assert any("cannot downgrade" in item for item in downgraded["blocking"])

    multi_target_record = _runtime_bug(isolated_workflow_root)
    multiple = workflow.build_runtime_contract(
        record=multi_target_record,
        changed_files=["backend/services/example.py", "tdx-api-main/web/server.go"],
        root=isolated_workflow_root,
    )
    assert multiple["target_ids"] == ["backend-main", "tdx-go-backend"]
    assert multiple["target_id"] is None
    assert multiple["pre_pr_ready"] is False
    assert any("multiple runtime targets" in item for item in multiple["blocking"])


def test_runtime_catalog_globs_and_client_paths_drive_activation_classification(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)

    dependency = workflow._classify_runtime_impact(["requirements-dev.txt"], root=isolated_workflow_root)
    client = workflow._classify_runtime_impact([".codex/skills/fix-aistock-issue/SKILL.md"], root=isolated_workflow_root)
    mcp_client = workflow._classify_runtime_impact(["scripts/aistock_mcp_server.py"], root=isolated_workflow_root)
    allocator_tool = workflow._classify_runtime_impact(["scripts/aistock_bug_id_allocator.py"], root=isolated_workflow_root)
    bug_registry_metadata_tool = workflow._classify_runtime_impact(
        ["scripts/bug_registry_metadata_check.py"],
        root=isolated_workflow_root,
    )
    backend_test = workflow._classify_runtime_impact(
        ["backend/tests/scripts/test_aistock_issue_workflow.py"],
        root=isolated_workflow_root,
    )
    offline_hmm_preparation = workflow._classify_runtime_impact(
        ["scripts/hmm_risk/prepare_state_model_set.py"],
        root=isolated_workflow_root,
    )
    offline_hmm_d1 = workflow._classify_runtime_impact(
        ["backend/services/hmm_risk/b3_d1_inactive_dimension.py"],
        root=isolated_workflow_root,
    )
    offline_hmm_mixed_dimension = workflow._classify_runtime_impact(
        ["backend/services/hmm_risk/b3_mixed_dimension.py"],
        root=isolated_workflow_root,
    )
    offline_hmm_training = workflow._classify_runtime_impact(
        ["backend/services/hmm_risk/b3_training.py"],
        root=isolated_workflow_root,
    )
    offline_hmm_state_model_set = workflow._classify_runtime_impact(
        ["backend/services/hmm_risk/state_model_set.py"],
        root=isolated_workflow_root,
    )
    offline_advisory_phase0b = workflow._classify_runtime_impact(
        ["backend/services/advisory_phase0b/audit_service.py"],
        root=isolated_workflow_root,
    )
    offline_advisory_batch_b = workflow._classify_runtime_impact(
        ["scripts/advisory_short_rebound_batch_b.py"],
        root=isolated_workflow_root,
    )
    dataset_offline_tools = workflow._classify_runtime_impact(
        [
            "scripts/build_stock_universe_pit_spans.py",
            "scripts/dataset_release_control_store.py",
            "scripts/update_backtest_dataset_monthly.py",
        ],
        root=isolated_workflow_root,
    )
    dataset_worker = workflow._classify_runtime_impact(
        ["scripts/dataset_release_worker.py"],
        root=isolated_workflow_root,
    )
    dataset_source_stage = workflow._classify_runtime_impact(
        ["scripts/dataset_release_source_stage.py"],
        root=isolated_workflow_root,
    )
    mixed_advisory_and_backend = workflow._classify_runtime_impact(
        [
            "backend/services/advisory_phase0b/audit_service.py",
            "backend/services/example.py",
        ],
        root=isolated_workflow_root,
    )
    unmapped_script = workflow._classify_runtime_impact(
        ["scripts/hmm_risk/unmapped_runtime_candidate.py"],
        root=isolated_workflow_root,
    )
    nightly_intake = workflow._classify_runtime_impact(
        [
            ".github/workflows/nightly.yml",
            "scripts/ci_failure_issue_summary.py",
            "scripts/llm_provider_adapter.py",
            "scripts/nightly_session_runner.py",
        ],
        root=isolated_workflow_root,
    )

    assert dependency["runtime_impact"] == "backend"
    assert dependency["target_ids"] == ["backend-main"]
    assert client["runtime_impact"] == "client"
    assert mcp_client["runtime_impact"] == "client"
    assert allocator_tool["runtime_impact"] == "none"
    assert bug_registry_metadata_tool["runtime_impact"] == "none"
    assert bug_registry_metadata_tool["runtime_files"] == []
    assert backend_test["runtime_impact"] == "none"
    assert offline_hmm_preparation["runtime_impact"] == "none"
    assert offline_hmm_preparation["runtime_files"] == []
    assert offline_hmm_d1["runtime_impact"] == "none"
    assert offline_hmm_d1["runtime_files"] == []
    assert offline_hmm_mixed_dimension["runtime_impact"] == "none"
    assert offline_hmm_mixed_dimension["runtime_files"] == []
    assert offline_hmm_training["runtime_impact"] == "none"
    assert offline_hmm_training["runtime_files"] == []
    assert offline_hmm_state_model_set["runtime_impact"] == "none"
    assert offline_hmm_state_model_set["runtime_files"] == []
    assert offline_advisory_phase0b["runtime_impact"] == "none"
    assert offline_advisory_phase0b["runtime_files"] == []
    assert offline_advisory_batch_b["runtime_impact"] == "none"
    assert offline_advisory_batch_b["runtime_files"] == []
    assert dataset_offline_tools["runtime_impact"] == "none"
    assert dataset_offline_tools["runtime_files"] == []
    assert dataset_worker["runtime_impact"] == "worker_scheduler"
    assert dataset_worker["target_ids"] == ["worker-scheduler"]
    assert dataset_source_stage["runtime_impact"] == "worker_scheduler"
    assert dataset_source_stage["target_ids"] == ["worker-scheduler"]
    assert mixed_advisory_and_backend["runtime_impact"] == "backend"
    assert mixed_advisory_and_backend["runtime_files"] == ["backend/services/example.py"]
    assert mixed_advisory_and_backend["target_ids"] == ["backend-main"]
    assert unmapped_script["runtime_impact"] == "unknown"
    assert nightly_intake["runtime_impact"] == "none"
    assert nightly_intake["runtime_files"] == []

    offline_contract = workflow.build_runtime_contract(
        record=_bug(
            allowed_write_scope=[
                "backend/services/advisory_phase0b/audit_service.py",
                "backend/services/advisory_phase0b/snapshot_reader.py",
            ],
            runtime_contract={
                "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
                "runtime_impact": "none",
            },
        ),
        changed_files=[
            "backend/services/advisory_phase0b/audit_service.py",
            "backend/services/advisory_phase0b/snapshot_reader.py",
        ],
        root=isolated_workflow_root,
    )

    assert offline_contract["runtime_impact"] == "none"
    assert offline_contract["backend_restart_required"] is False
    assert offline_contract["pre_pr_ready"] is True
    assert offline_contract["blocking"] == []


def test_bug_1032_offline_hmm_state_model_set_preserves_real_backend_detection(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    changed_files = [
        "backend/services/hmm_risk/state_model_set.py",
        "backend/tests/hmm_risk/test_prepare_state_model_set_b3.py",
        "backend/tests/hmm_risk/test_stock_fact_observation.py",
        "scripts/hmm_risk/prepare_state_model_set.py",
    ]

    record = _bug(
        allowed_write_scope=[*changed_files, "backend/services/example.py"],
        file_scope_contract={"changed_files": changed_files},
        runtime_contract={
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "runtime_impact": "none",
        },
    )
    authoritative_files = workflow.resolve_record_runtime_changed_files(record)
    inference = workflow._classify_runtime_impact(authoritative_files, root=isolated_workflow_root)
    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=authoritative_files,
        root=isolated_workflow_root,
    )
    mixed = workflow._classify_runtime_impact(
        [*changed_files, "backend/services/example.py"],
        root=isolated_workflow_root,
    )

    assert authoritative_files == changed_files
    assert inference == {
        "runtime_impact": "none",
        "observed_impacts": ["none"],
        "runtime_files": [],
        "target_ids": [],
    }
    assert contract["runtime_impact"] == "none"
    assert contract["backend_restart_required"] is False
    assert contract["target_ids"] == []
    assert contract["pre_pr_ready"] is True
    assert contract["blocking"] == []
    assert mixed["runtime_impact"] == "backend"
    assert mixed["runtime_files"] == ["backend/services/example.py"]
    assert mixed["target_ids"] == ["backend-main"]


def test_runtime_contract_requires_schema_real_runbook_and_known_persistence_basis(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["runtime_contract"] = {
        **record["runtime_contract"],
        "schema_version": "legacy",
        "operator_runbook_ref": "docs/operations/missing.md",
        "persistence_basis": "magic",
    }

    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )

    assert contract["pre_pr_ready"] is False
    assert any("schema_version" in item for item in contract["blocking"])
    assert any("does not exist" in item for item in contract["blocking"])
    assert any("persistence_basis is invalid" in item for item in contract["blocking"])


def test_runtime_contract_rejects_non_executable_probe_refs_before_post_restart(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["runtime_contract"] = {
        **record["runtime_contract"],
        "identity_ref": f"GET {record['runtime_contract']['identity_ref']} after restart",
        "business_smoke_ref": "run the fresh-process smoke matrix",
        "database_readback_ref": "pending separately authorized database readback",
    }

    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )

    assert contract["pre_pr_ready"] is False
    assert contract["target"]["probes"]["health_ref"] == "http://127.0.0.1:8001/api/v1/health"
    assert any("identity_ref must be an executable absolute endpoint" in item for item in contract["blocking"])
    assert any("business_smoke_ref must be an executable absolute endpoint" in item for item in contract["blocking"])
    assert any("database_readback_ref must be an executable absolute endpoint" in item for item in contract["blocking"])


def test_runtime_contract_accepts_explicit_database_readback_not_required(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)

    contract = workflow.build_runtime_contract(
        record=_runtime_bug(isolated_workflow_root),
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )

    assert contract["pre_pr_ready"] is True
    assert contract["target"]["probes"]["database_readback_ref"] == "not_required"


def test_runtime_contract_rejects_unresolved_probe_url_template(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["runtime_contract"]["business_smoke_ref"] = (
        f"{record['runtime_contract']['business_smoke_ref']}/{{run_id}}"
    )

    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )

    assert contract["pre_pr_ready"] is False
    assert any("business_smoke_ref must be an executable absolute endpoint" in item for item in contract["blocking"])


def test_runtime_source_pr_uses_refs_until_post_restart_verification(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )

    body = workflow.render_pr_body(
        "BUG-199",
        record,
        ["backend/services/example.py"],
        {"required_plans": ["l0"], "production_gates": {}},
        {"scope_check": {"status": "passed"}},
        ["validation-receipt"],
        True,
        contract,
    )

    assert "Refs #199" in body
    assert "Closes #199" not in body


def test_finish_persists_fresh_process_evidence_in_bug_json_and_pr_body(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["runtime_contract"]["fresh_process_evidence"] = []
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", record)
    monkeypatch.setattr(
        workflow,
        "_build_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(item_id="BUG-199"),
    )

    payload = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["backend/services/example.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=[],
        plan_only=True,
        allow_missing_evidence=False,
        fresh_process_evidence=["isolated port 8012 import smoke passed"],
    )

    persisted = json.loads(issue.read_text(encoding="utf-8"))
    assert persisted["runtime_contract"]["fresh_process_evidence"] == [
        "isolated port 8012 import smoke passed"
    ]
    pr_body = (isolated_workflow_root / payload["pr_body_path"]).read_text(encoding="utf-8")
    assert "fresh_process_evidence: isolated port 8012 import smoke passed" in pr_body
    assert "Refs #199" in pr_body


def test_post_restart_verify_is_read_only_and_writes_only_ignored_receipt(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue_payload = _runtime_bug(isolated_workflow_root)
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", issue_payload)
    before = issue.read_text(encoding="utf-8")

    class _Response:
        status = 200

        def __init__(self, body: bytes) -> None:
            self.body = body

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return self.body

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        assert request.method == "GET"
        assert timeout == 3.0
        body = b'{"commit":"merge-abc123"}' if "runtime-identity" in request.full_url else b'{"status":"ok"}'
        return _Response(body)

    monkeypatch.setattr(
        workflow,
        "_open_read_only_url",
        lambda request, timeout_seconds: fake_urlopen(request, timeout_seconds),
    )
    payload = workflow.build_post_restart_verify(
        bug_id=None,
        issue_json=str(issue),
        target_id="backend-main",
        expected_identity="merge-abc123",
        timeout_seconds=3.0,
    )

    assert payload["workflow_gate"] == "verified"
    assert payload["runtime_identity_match"] is True
    assert payload["process_control_performed"] is False
    assert payload["tracked_files_written"] is False
    assert all("response_preview" not in probe for probe in payload["probes"])
    assert all("_response_body" not in probe for probe in payload["probes"])
    assert payload["probe_evidence_digest"]
    assert issue.read_text(encoding="utf-8") == before
    assert (isolated_workflow_root / payload["receipt_path"]).exists()


def test_post_restart_verify_accepts_deployed_origin_main_descendant_with_strict_git_proof(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue = _write_json(
        isolated_workflow_root / "runtime-bug.json",
        _runtime_bug(isolated_workflow_root),
    )
    expected = "a" * 40
    observed = "b" * 40
    origin_main = "c" * 40

    class _Response:
        status = 200

        def __init__(self, body: bytes) -> None:
            self.body = body

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return self.body

    def fake_open(request: Any, *, timeout_seconds: float) -> _Response:
        assert timeout_seconds == 3.0
        body = json.dumps({"merge_commit": observed}).encode() if "runtime-identity" in request.full_url else b'{"status":"ok"}'
        return _Response(body)

    def fake_run(args: list[str], *, cwd: Path, timeout: float = 120.0, env: dict[str, str] | None = None) -> dict[str, Any]:
        del timeout, env
        assert cwd == isolated_workflow_root
        if args == ["git", "rev-parse", "--verify", "origin/main^{commit}"]:
            return {"ok": True, "returncode": 0, "stdout": origin_main, "stderr": ""}
        if tuple(args) in {
            ("git", "merge-base", "--is-ancestor", expected, observed),
            ("git", "merge-base", "--is-ancestor", observed, origin_main),
            ("git", "merge-base", "--is-ancestor", origin_main, origin_main),
        }:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(workflow, "_open_read_only_url", fake_open)
    monkeypatch.setattr(workflow, "_run_command", fake_run)

    payload = workflow.build_post_restart_verify(
        bug_id=None,
        issue_json=str(issue),
        target_id="backend-main",
        expected_identity=expected,
        timeout_seconds=3.0,
    )

    assert payload["workflow_gate"] == "verified"
    assert payload["runtime_identity_match"] is True
    assert payload["observed_identity"] == observed
    assert payload["runtime_identity_proof"] == {
        "schema_version": "aistock_runtime_identity_proof_v1",
        "mode": "origin_main_descendant",
        "expected_identity": expected,
        "observed_identity": observed,
        "origin_main_identity": origin_main,
        "expected_is_ancestor": True,
        "observed_in_origin_main": True,
    }
    assert payload["runtime_identity_proof_digest"]

    close_sync = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        post_restart_receipt=str(isolated_workflow_root / payload["receipt_path"]),
    )
    assert close_sync["workflow_gate"] == "ready_for_apply"


@pytest.mark.parametrize(
    ("expected_is_ancestor", "observed_in_origin_main", "expected_error"),
    [
        (False, True, "not a deployed origin/main descendant"),
        (True, False, "not contained in the verified origin/main lineage"),
    ],
)
def test_post_restart_verify_rejects_unproven_deployed_commit(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    expected_is_ancestor: bool,
    observed_in_origin_main: bool,
    expected_error: str,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue = _write_json(
        isolated_workflow_root / "runtime-bug.json",
        _runtime_bug(isolated_workflow_root),
    )
    expected = "a" * 40
    observed = "b" * 40
    origin_main = "c" * 40

    class _Response:
        status = 200

        def __init__(self, body: bytes) -> None:
            self.body = body

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return self.body

    monkeypatch.setattr(
        workflow,
        "_open_read_only_url",
        lambda request, timeout_seconds: _Response(
            json.dumps({"merge_commit": observed}).encode()
            if "runtime-identity" in request.full_url
            else b'{"status":"ok"}'
        ),
    )

    def fake_run(args: list[str], **_kwargs: Any) -> dict[str, Any]:
        if args == ["git", "rev-parse", "--verify", "origin/main^{commit}"]:
            return {"ok": True, "returncode": 0, "stdout": origin_main, "stderr": ""}
        if args == ["git", "merge-base", "--is-ancestor", expected, observed]:
            return {"ok": expected_is_ancestor, "returncode": 0 if expected_is_ancestor else 1, "stdout": "", "stderr": ""}
        if args == ["git", "merge-base", "--is-ancestor", observed, origin_main]:
            return {"ok": observed_in_origin_main, "returncode": 0 if observed_in_origin_main else 1, "stdout": "", "stderr": ""}
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    payload = workflow.build_post_restart_verify(
        bug_id=None,
        issue_json=str(issue),
        target_id="backend-main",
        expected_identity=expected,
        timeout_seconds=3.0,
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["runtime_identity_match"] is False
    assert payload["observed_identity"] == observed
    assert any(expected_error in item for item in payload["blocking"])


_BUG_993_BUG989_CHANGED_FILES = [
    "aistock_models/aistock_models/gats_industry_provider.py",
    "backend/services/quantevolver/config_composer.py",
    "backend/services/quantevolver/qe_dataset_contract.py",
    "backend/tests/unified_engine/test_qe_config_truth.py",
    "backend/tests/unified_engine/test_qe_data_plane_zero_db.py",
    "backend/tests/unified_engine/test_qe_frozen_suspend_filter.py",
    "docs/analysis/sector_rotation_factors_develop_spec_20260710.md",
    "docs/architecture/qe_efficient_gats_l2_industry_embedding_f1_design_20260710.md",
    "docs/standards/aistock_runtime_targets_v1.yaml",
    "scripts/export_suspend_d_candidate.py",
    "scripts/qe_build_frozen_risk_policy.py",
    "scripts/qe_build_frozen_suspend_filter.py",
    "scripts/qrun_limit.py",
    "scripts/qrun_limit_minute.py",
    "tests/aistock_validation/bugs/20260806_BUG-989-qe-alpha-postgresql.json",
    "tests/aistock_validation/catalog/file_ownership.yaml",
    "tests/aistock_validation/history/qe/20260806_134857_l3_qe-read-only-workspace-access-regression.md",
]


def _write_bug993_runtime_catalog(root: Path) -> Path:
    path = root / "docs" / "standards" / "aistock_runtime_targets_v1.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "aistock_runtime_target_catalog_v1",
                "targets": {
                    "backend-main": {
                        "runtime_kind": "backend",
                        "source_globs": [
                            "backend/**/*.py",
                            "scripts/qe_build_frozen_risk_policy.py",
                            "scripts/qe_build_frozen_suspend_filter.py",
                            "scripts/qrun_limit.py",
                            "scripts/qrun_limit_minute.py",
                            "aistock_models/**/*.py",
                            "requirements*.txt",
                        ],
                        "production_port": 8001,
                        "isolated_validation_ports": [8011, 8012],
                        "probe_origins": ["http://127.0.0.1:8001"],
                        "operator_runbook_ref": "bug_record.runtime_contract.operator_runbook_ref",
                        "expected_identity_ref": "merged_commit",
                        "probes": {
                            "health_ref": "bug_record.runtime_contract.health_ref",
                            "identity_ref": "bug_record.runtime_contract.identity_ref",
                            "business_smoke_ref": "bug_record.runtime_contract.business_smoke_ref",
                            "database_readback_ref": "bug_record.runtime_contract.database_readback_ref",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _file_scope_contract(changed_files: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "aistock_submit_bug_file_scope_v1",
        "changed_files": list(changed_files),
        "added_files": [],
        "scope_files": list(changed_files),
        "ownership": {},
    }


def _bug993_bug989_record(root: Path) -> dict[str, Any]:
    record = _runtime_bug(root)
    record["bug_id"] = "BUG-989"
    record["allowed_write_scope"] = [
        *_BUG_993_BUG989_CHANGED_FILES,
        "backend/tests/unified_engine/test_qrun_mlflow_metric_retry.py",
        "tests/aistock_validation/bugs/.bug_id_allocator.json",
    ]
    record["file_scope_contract"] = _file_scope_contract(_BUG_993_BUG989_CHANGED_FILES)
    return record


def test_post_restart_verify_bug989_record_infers_backend_main_from_actual_changed_files(
    isolated_workflow_root: Path,
) -> None:
    _write_bug993_runtime_catalog(isolated_workflow_root)
    record = _bug993_bug989_record(isolated_workflow_root)

    resolved = workflow.resolve_record_runtime_changed_files(record)
    assert resolved == sorted(_BUG_993_BUG989_CHANGED_FILES)
    inference = workflow._classify_runtime_impact(resolved, root=isolated_workflow_root)
    assert inference["runtime_impact"] == "backend"
    assert inference["target_ids"] == ["backend-main"]
    assert "unknown" not in inference["observed_impacts"]

    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=resolved,
        root=isolated_workflow_root,
        fresh_process_evidence=["isolated port 8012 import smoke passed"],
    )
    assert contract["runtime_impact"] == "backend"
    assert contract["target_id"] == "backend-main"
    assert contract["target_ids"] == ["backend-main"]
    assert contract["backend_restart_required"] is True
    assert contract["blocking"] == []


def test_resolve_record_runtime_changed_files_prefers_actual_changed_files_over_allowed_scope(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["allowed_write_scope"] = [
        "backend/services/example.py",
        "docs/operations/example_backend_restart.md",
        "scripts/unmodified_unknown_tool.py",
    ]
    record["file_scope_contract"] = _file_scope_contract(
        ["backend/services/example.py", "docs/operations/example_backend_restart.md"]
    )

    resolved = workflow.resolve_record_runtime_changed_files(record)
    assert resolved == ["backend/services/example.py", "docs/operations/example_backend_restart.md"]
    assert "scripts/unmodified_unknown_tool.py" not in resolved
    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=resolved,
        root=isolated_workflow_root,
        fresh_process_evidence=["isolated port 8012 import smoke passed"],
    )
    assert contract["runtime_impact"] == "backend"
    assert contract["blocking"] == []


@pytest.mark.parametrize(
    "file_scope_contract",
    [
        None,
        {"schema_version": "aistock_submit_bug_file_scope_v1", "changed_files": []},
        {"schema_version": "aistock_submit_bug_file_scope_v1", "changed_files": ["", "  "]},
        {"schema_version": "aistock_submit_bug_file_scope_v1", "changed_files": "not-a-list"},
        "not-a-dict",
    ],
)
def test_resolve_record_runtime_changed_files_legacy_fallback_stays_fail_closed(
    isolated_workflow_root: Path,
    file_scope_contract: Any,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["allowed_write_scope"] = [
        "backend/services/example.py",
        "scripts/unclassified_executable_tool.py",
    ]
    if file_scope_contract is not None:
        record["file_scope_contract"] = file_scope_contract

    resolved = workflow.resolve_record_runtime_changed_files(record)
    assert resolved == ["backend/services/example.py", "scripts/unclassified_executable_tool.py"]
    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=resolved,
        root=isolated_workflow_root,
        fresh_process_evidence=["isolated port 8012 import smoke passed"],
    )
    assert contract["runtime_impact"] == "unknown"
    assert contract["pre_pr_ready"] is False
    assert any("conflicts with changed-file inference" in item for item in contract["blocking"])


def test_runtime_contract_keeps_unknown_executable_blocking_with_actual_changed_files(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    record = _runtime_bug(isolated_workflow_root)
    record["file_scope_contract"] = _file_scope_contract(
        ["backend/services/example.py", "scripts/brand_new_unclassified.py"]
    )

    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=workflow.resolve_record_runtime_changed_files(record),
        root=isolated_workflow_root,
        fresh_process_evidence=["isolated port 8012 import smoke passed"],
    )
    assert contract["runtime_impact"] == "unknown"
    assert contract["pre_pr_ready"] is False
    assert any("conflicts with changed-file inference" in item for item in contract["blocking"])


def test_export_suspend_d_candidate_classified_as_non_runtime_offline_tool(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    inference = workflow._classify_runtime_impact(
        ["scripts/export_suspend_d_candidate.py"],
        root=isolated_workflow_root,
    )
    assert inference["runtime_impact"] == "none"
    assert inference["target_ids"] == []
    assert inference["runtime_files"] == []

    record = _bug(
        allowed_write_scope=["scripts/export_suspend_d_candidate.py"],
        file_scope_contract=_file_scope_contract(["scripts/export_suspend_d_candidate.py"]),
        runtime_contract={
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "runtime_impact": "none",
            "persistence_basis": "not_required",
            "post_restart_effective_gate": "not_required",
            "target_id": None,
            "target_ids": [],
        },
    )
    contract = workflow.build_runtime_contract(
        record=record,
        changed_files=workflow.resolve_record_runtime_changed_files(record),
        root=isolated_workflow_root,
    )
    assert contract["runtime_impact"] == "none"
    assert contract["target_ids"] == []
    assert contract["backend_restart_required"] is False


def test_export_qe_qlib_candidate_classified_as_non_runtime_offline_tool(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    inference = workflow._classify_runtime_impact(
        ["scripts/export_qe_qlib_candidate.py"],
        root=isolated_workflow_root,
    )
    assert inference == {
        "runtime_impact": "none",
        "observed_impacts": ["none"],
        "runtime_files": [],
        "target_ids": [],
    }


def test_mixed_backend_main_file_and_exporter_infers_backend_main_only(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    inference = workflow._classify_runtime_impact(
        ["backend/services/example.py", "scripts/export_suspend_d_candidate.py"],
        root=isolated_workflow_root,
    )
    assert inference["runtime_impact"] == "backend"
    assert inference["target_ids"] == ["backend-main"]
    assert inference["runtime_files"] == ["backend/services/example.py"]


def test_post_restart_verify_executes_probes_for_bug989_contract(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_bug993_runtime_catalog(isolated_workflow_root)
    record = _bug993_bug989_record(isolated_workflow_root)
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", record)
    expected = "2ff0e0aed0670b2611cb91b5dedd587659dea4ad"
    requested_urls: list[str] = []

    class _Response:
        status = 200

        def __init__(self, body: bytes) -> None:
            self.body = body

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return self.body

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        assert request.method == "GET"
        requested_urls.append(request.full_url)
        if "runtime-identity" in request.full_url:
            return _Response(json.dumps({"status": "ready", "merge_commit": expected}).encode())
        return _Response(b'{"status":"ok"}')

    monkeypatch.setattr(
        workflow,
        "_open_read_only_url",
        lambda request, timeout_seconds: fake_urlopen(request, timeout_seconds),
    )
    payload = workflow.build_post_restart_verify(
        bug_id=None,
        issue_json=str(issue),
        target_id="backend-main",
        expected_identity=expected,
        timeout_seconds=3.0,
    )

    assert payload["workflow_gate"] == "verified"
    assert payload["blocking"] == []
    assert payload["target_id"] == "backend-main"
    assert len(requested_urls) == 3
    assert [probe["name"] for probe in payload["probes"]] == ["health_ref", "identity_ref", "business_smoke_ref"]
    assert all(probe["status"] == "passed" for probe in payload["probes"])
    assert payload["observed_identity"] == expected
    assert payload["runtime_identity_match"] is True
    assert payload["process_control_performed"] is False
    assert payload["tracked_files_written"] is False
    assert payload["required_probe_names"] == ["health_ref", "identity_ref", "business_smoke_ref"]
    assert payload["probe_evidence_digest"]
    assert payload["contract_digest"]
    assert payload["catalog_sha256"]
    receipt = json.loads((isolated_workflow_root / payload["receipt_path"]).read_text(encoding="utf-8"))
    assert receipt["runtime_identity_match"] is True
    assert receipt["probe_evidence_digest"] == payload["probe_evidence_digest"]
    assert receipt["process_control_performed"] is False
    assert receipt["tracked_files_written"] is False


def test_runtime_classification_matrix_unchanged_for_known_target_kinds(
    isolated_workflow_root: Path,
) -> None:
    catalog_path = _write_runtime_catalog(isolated_workflow_root)
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    catalog["targets"]["worker-scheduler"] = {
        "runtime_kind": "worker_scheduler",
        "source_globs": ["backend/services/**/*worker*.py", "scripts/**/*scheduler*.py"],
        "production_port": 8002,
        "isolated_validation_ports": [8013],
        "probe_origins": ["http://127.0.0.1:8002"],
        "operator_runbook_ref": "bug_record.runtime_contract.operator_runbook_ref",
        "expected_identity_ref": "merged_commit",
        "probes": {
            "health_ref": "bug_record.runtime_contract.health_ref",
            "identity_ref": "bug_record.runtime_contract.identity_ref",
            "business_smoke_ref": "bug_record.runtime_contract.business_smoke_ref",
            "database_readback_ref": "bug_record.runtime_contract.database_readback_ref",
        },
    }
    catalog_path.write_text(yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8")
    cases = [
        (["backend/services/example.py"], "backend", ["backend-main"]),
        (["backend/services/worker_scheduler_runner.py"], "worker_scheduler", ["worker-scheduler"]),
        (["tdx-api-main/web/server.go"], "backend", ["tdx-go-backend"]),
        (["frontend/src/app/paper-v2/page.tsx"], "frontend", []),
        (["migrations/20260807_example.sql"], "database", []),
        (["backend/migrations/20260807_example.sql"], "database", []),
        ([".claude/commands/fix-aistock-issue.md"], "client", []),
        (["scripts/other_unclassified_export.py"], "unknown", []),
    ]
    for files, expected_impact, expected_targets in cases:
        inference = workflow._classify_runtime_impact(files, root=isolated_workflow_root)
        assert inference["runtime_impact"] == expected_impact, files
        assert inference["target_ids"] == expected_targets, files

    downgraded = workflow.build_runtime_contract(
        record=_bug(runtime_contract={"schema_version": workflow.RUNTIME_CONTRACT_SCHEMA, "runtime_impact": "none"}),
        changed_files=["backend/services/example.py"],
        root=isolated_workflow_root,
    )
    assert downgraded["runtime_impact"] == "backend"
    assert any("cannot downgrade" in item for item in downgraded["blocking"])

    multi_target_record = _runtime_bug(isolated_workflow_root)
    multiple = workflow.build_runtime_contract(
        record=multi_target_record,
        changed_files=["backend/services/example.py", "backend/services/worker_scheduler_runner.py"],
        root=isolated_workflow_root,
    )
    assert multiple["target_ids"] == ["backend-main", "worker-scheduler"]
    assert any("multiple runtime targets" in item for item in multiple["blocking"])


def test_close_sync_rejects_forged_runtime_identity_proof(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue_payload = _runtime_bug(isolated_workflow_root)
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", issue_payload)
    receipt_payload = _passed_runtime_receipt(
        isolated_workflow_root,
        issue_payload,
        expected_identity="merge-abc123",
    )
    receipt_payload["runtime_identity_proof"]["observed_identity"] = "forged"
    receipt = _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "forged.json",
        receipt_payload,
    )

    payload = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        post_restart_receipt=str(receipt),
    )

    assert payload["workflow_gate"] == "fixed_source_pending_user_restart"
    assert any("identity proof digest mismatch" in item for item in payload["post_restart_receipt_errors"])
    assert any("observed identity does not match identity proof" in item for item in payload["post_restart_receipt_errors"])


def test_read_only_probe_rejects_non_catalog_origin_without_network_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_open_read_only_url",
        lambda *args, **kwargs: pytest.fail("disallowed probe origin must not perform network I/O"),
    )

    result = workflow._read_only_http_probe(
        "health_ref",
        "http://169.254.169.254/latest/meta-data",
        allowed_origins=["http://127.0.0.1:8001"],
    )

    assert result["status"] == "blocked"
    assert "not catalog-allowed" in result["error"]


def test_close_sync_runtime_bug_requires_passed_post_restart_receipt(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue_payload = _runtime_bug(isolated_workflow_root)
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", issue_payload)

    pending = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
    )
    assert pending["workflow_gate"] == "fixed_source_pending_user_restart"
    assert pending["post_restart_effective_gate"] == "pending_user_restart"
    assert pending["runtime_identity_match"] == "pending"

    receipt = _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "post-restart-verify.json",
        _passed_runtime_receipt(isolated_workflow_root, issue_payload, expected_identity="merge-abc123"),
    )
    ready = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        post_restart_receipt=str(receipt),
    )
    assert ready["workflow_gate"] == "ready_for_apply"
    assert ready["post_restart_effective_gate"] == "passed"


def test_close_sync_rejects_receipt_without_complete_probe_evidence(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue_payload = _runtime_bug(isolated_workflow_root)
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", issue_payload)
    incomplete = _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "incomplete.json",
        {
            "schema_version": workflow.RUNTIME_VERIFY_RECEIPT_SCHEMA,
            "bug_id": "BUG-199",
            "target_id": "backend-main",
            "expected_identity": "merge-abc123",
            "post_restart_effective_gate": "passed",
            "runtime_identity_match": True,
            "process_control_performed": False,
        },
    )

    payload = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        post_restart_receipt=str(incomplete),
    )

    assert payload["workflow_gate"] == "fixed_source_pending_user_restart"
    assert any("probe set mismatch" in item for item in payload["post_restart_receipt_errors"])
    assert any("contract digest mismatch" in item for item in payload["post_restart_receipt_errors"])


def test_close_sync_rejects_non_backend_runtime_contract_conflicts(
    isolated_workflow_root: Path,
) -> None:
    issue_payload = _bug(
        allowed_write_scope=[".codex/skills/fix-aistock-issue/SKILL.md"],
        runtime_contract={
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "runtime_impact": "none",
            "persistence_basis": "not_required",
        },
    )
    issue = _write_json(isolated_workflow_root / "client-runtime-bug.json", issue_payload)

    dry = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
    )

    assert dry["workflow_gate"] == "blocked_runtime_contract"
    assert any("cannot downgrade" in item for item in dry["runtime_contract_errors"])
    with pytest.raises(workflow.WorkflowError, match="runtime contract blocks close-sync"):
        workflow.build_close_sync_plan(
            bug_id=None,
            issue_json=str(issue),
            pr_url="https://github.example/pull/199",
            apply=True,
            allow_missing_linkage=False,
            validation_evidence=["python -m nox -s l0 -> passed"],
            allow_current_worktree=True,
        )


def test_close_sync_uses_merged_commit_files_instead_of_unmodified_allowed_scope(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    merge_commit = "a" * 40
    issue = _write_json(
        isolated_workflow_root / "offline-bug.json",
        _bug(
            allowed_write_scope=[
                "backend/services/hmm_risk/b3_d1_inactive_dimension.py",
                "backend/services/hmm_risk/stock_fact_repository.py",
            ],
            runtime_contract={
                "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
                "runtime_impact": "none",
                "persistence_basis": "not_required",
                "post_restart_effective_gate": "not_required",
                "target_id": None,
                "target_ids": [],
            },
        ),
    )

    def fake_run(args: list[str], **_kwargs: Any) -> dict[str, Any]:
        assert args == ["git", "diff", "--name-only", f"{merge_commit}^1", merge_commit, "--"]
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "backend/services/hmm_risk/b3_d1_inactive_dimension.py\nscripts/hmm_risk/prepare_state_model_set.py\n",
            "stderr": "",
        }

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    payload = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=False,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s hmm_risk_backend -> passed"],
        merge_commit=merge_commit,
    )
    assert payload["workflow_gate"] == "ready_for_apply"
    assert payload["backend_restart"]["required"] is False
    assert payload["runtime_changed_files_source"] == "merge_commit"
    assert "backend/services/hmm_risk/stock_fact_repository.py" not in payload["runtime_changed_files"]


def test_close_sync_rejects_declared_merge_commit_that_differs_from_verified_pr(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declared = "a" * 40
    verified = "b" * 40
    issue = _write_json(isolated_workflow_root / "offline-bug.json", _bug())
    monkeypatch.setattr(workflow, "_merged_commit_changed_files", lambda merge_commit: ["scripts/aistock_issue_workflow.py"])
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url, skip_github_check=False: {
            "checked": True,
            "merged": True,
            "pr": {"mergeCommit": {"oid": verified}, "mergedAt": "2026-08-05T00:00:00Z"},
        },
    )
    with pytest.raises(workflow.WorkflowError, match="differs from the verified source PR"):
        workflow.build_close_sync_plan(
            bug_id=None,
            issue_json=str(issue),
            pr_url="https://github.example/pull/199",
            apply=True,
            allow_missing_linkage=False,
            validation_evidence=["python -m nox -s validation_module_registry_l0 -> passed"],
            merge_commit=declared,
            allow_current_worktree=True,
        )


def test_runtime_close_sync_preserves_source_fixed_time_and_uses_verification_close_time(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    issue_payload = _runtime_bug(
        isolated_workflow_root,
        fixed_at="2026-07-31T10:00:00Z",
    )
    issue = _write_json(isolated_workflow_root / "runtime-bug.json", issue_payload)
    receipt = _write_json(
        isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "passed.json",
        _passed_runtime_receipt(isolated_workflow_root, issue_payload, expected_identity="merge-abc123"),
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url, skip_github_check=False: {
            "checked": True,
            "merged": True,
            "pr": {"mergeCommit": {"oid": "merge-abc123"}, "mergedAt": "2026-07-31T11:00:00Z"},
        },
    )
    monkeypatch.setattr(workflow, "_utc_now", lambda: "2026-08-01T02:00:00Z")

    payload = workflow.build_close_sync_plan(
        bug_id=None,
        issue_json=str(issue),
        pr_url="https://github.example/pull/199",
        apply=True,
        allow_missing_linkage=False,
        validation_evidence=["python -m nox -s l0 -> passed"],
        skip_github_check=True,
        allow_current_worktree=True,
        post_restart_receipt=str(receipt),
    )

    updated = json.loads(issue.read_text(encoding="utf-8"))
    assert payload["workflow_gate"] == "close_synced"
    assert updated["status"] == "verified"
    assert updated["fixed_at"] == "2026-07-31T10:00:00Z"
    assert updated["closed_at"] == "2026-08-01T02:00:00Z"
    assert updated["runtime_contract"]["runtime_identity_match"] is True
    summary = updated["runtime_contract"]["post_restart_receipt_summary"]
    assert summary["schema_version"] == workflow.RUNTIME_VERIFY_RECEIPT_SUMMARY_SCHEMA
    assert summary["receipt_sha256"]
    assert summary["post_restart_effective_gate"] == "passed"
    assert summary["response_content_persisted"] is False


def test_runtime_pending_sync_reopens_issue_and_aligns_status_label(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        calls.append(args)
        if args[:3] == ["gh", "issue", "view"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps({"state": "CLOSED", "labels": [{"name": "status:fixed"}]}),
                "stderr": "",
            }
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    result = workflow._sync_github_issue_runtime_pending(
        _bug(status="fixed_source_pending_user_restart"),
        {"merged_pr": "https://github.example/pull/199", "merge_commit": "merge-abc123"},
        root=isolated_workflow_root,
    )

    assert result["status"] == "reopened_runtime_pending"
    assert any(args[:3] == ["gh", "issue", "reopen"] for args in calls)
    edit = next(args for args in calls if args[:3] == ["gh", "issue", "edit"])
    assert ["--remove-label", "status:fixed"] == edit[6:8]
    assert ["--add-label", "status:in_progress"] == edit[8:10]


def test_close_sync_create_pr_persists_post_restart_state_in_one_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {
            "schema_version": "aistock_issue_workflow_close_sync_v1",
            "bug_id": "BUG-199",
            "workflow_gate": "close_synced",
            "registry_root": "F:/tmp/BUG-199-close-sync",
            "updated_bug_json": "tests/aistock_validation/bugs/BUG-199.json",
        },
    )

    def fake_commit_and_pr(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"workflow_gate": "pr_opened", "pr_url": "https://github.example/pull/200"}

    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", fake_commit_and_pr)

    exit_code = workflow.main(
        [
            "close-sync",
            "--bug-id",
            "BUG-199",
            "--pr-url",
            "https://github.example/pull/199",
            "--validation-evidence",
            "nox -s l0 -> passed",
            "--post-restart-receipt",
            "tmp/issue_workflow/BUG-199/post-restart-verify.json",
            "--apply",
            "--create-pr",
            "--output-format",
            "full-json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["close_sync_commit"]["workflow_gate"] == "pr_opened"
    assert captured["bug_id"] == "BUG-199"
    assert captured["validation_evidence"] == ["nox -s l0 -> passed"]


def test_finish_plan_only_can_draft_pr_body_without_evidence(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    suggested_tests = [f"backend/tests/scripts/test_aistock_issue_workflow_{index}.py" for index in range(12)]
    monkeypatch.setattr(
        workflow,
        "_build_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(
            affected_tests={"suggested_tests": suggested_tests}
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

    assert payload["closure_ready"] is False
    assert payload["draft_ready"] is True
    assert payload["workflow_gate"] == "plan_ready"
    assert payload["artifact_policy"] == "draft_finish_plan_persisted"
    assert payload["codegraph_suggested_tests"] == suggested_tests
    assert payload["code_intelligence"]["affected_tests_ref"].endswith("affected-tests.json")
    pr_body = isolated_workflow_root / payload["pr_body_path"]
    pr_body_text = pr_body.read_text(encoding="utf-8")
    assert "Code intelligence" in pr_body_text
    assert suggested_tests[0] in pr_body_text
    assert suggested_tests[9] in pr_body_text
    assert suggested_tests[10] not in pr_body_text
    assert "CodeGraph suggested tests omitted: `2` more" in pr_body_text
    assert "missing - run required validation" in pr_body_text
    assert (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "finish-plan.json").exists()


def test_finish_draft_flags_exit_success_without_claiming_pr_readiness(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())

    for flag, expected_gate in [
        ("--plan-only", "plan_ready"),
        ("--allow-missing-evidence", "validation_evidence_missing_allowed"),
    ]:
        assert workflow.main([
            "finish",
            "--issue-json",
            str(issue),
            "--changed-file",
            "scripts/aistock_issue_workflow.py",
            flag,
        ]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["closure_ready"] is False
        assert payload["draft_ready"] is True
        assert payload["workflow_gate"] == expected_gate
        assert payload["pre_pr_gate"]["workflow_gate"] == "blocked"


def test_finish_failure_persists_diagnostic_json(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(isolated_workflow_root / "bug.json", _bug())
    suggested_tests = [f"backend/tests/scripts/test_aistock_issue_workflow_{index}.py" for index in range(12)]
    monkeypatch.setattr(
        workflow,
        "_build_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(affected_tests={"suggested_tests": suggested_tests}),
    )

    payload = workflow.build_finish_plan(
        bug_id=None,
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=[],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["workflow_gate"] == "validation_evidence_missing"
    assert payload["artifact_policy"] == "diagnostic_json_persisted"
    finish_plan = json.loads(
        (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-199" / "finish-plan.json").read_text(
            encoding="utf-8"
        )
    )
    assert finish_plan["artifact_policy"] == (
        "compact_finish_plan_no_full_selected_validation_pr_quality_or_code_intelligence_payload"
    )
    assert finish_plan["code_intelligence"]["full_payload_inlined"] is False
    assert finish_plan["code_intelligence"]["suggested_tests_count"] == 12
    assert len(finish_plan["code_intelligence"]["suggested_tests_preview"]) == workflow.PR_BODY_CODEGRAPH_TEST_LIMIT
    assert finish_plan["selected_validation"]["full_payload_inlined"] is False
    assert finish_plan["selected_validation"]["codegraph_suggested_tests"]["omitted_count"] == 2
    finish_plan_text = json.dumps(finish_plan, ensure_ascii=False)
    assert "skip_reasons" not in finish_plan_text
    assert "matched_rules" not in finish_plan_text


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
    assert payload["source"] == "local"
    assert payload["mode"] == "plan"
    assert payload["count"] == 1
    assert payload["recommended_first_issue"] == "BUG-199"
    assert "run --bug-id BUG-199" in payload["next_command"]


def test_run_p0_accepts_documented_source_both_and_mode_plan(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug(severity="P0", module="validation"))
    monkeypatch.setattr(
        workflow,
        "_scan_github_bug_ids",
        lambda **_kwargs: (
            [
                {
                    "bug_id": "BUG-199",
                    "number": 199,
                    "kind": "github_issue",
                    "source": "https://github.example/issues/199",
                    "github_issue_number": 199,
                    "github_state": "OPEN",
                    "title": "BUG-199 P0: Local mirror already exists",
                    "labels": [{"name": "module:validation"}, {"name": "P0"}],
                },
                {
                    "bug_id": "BUG-200",
                    "number": 200,
                    "kind": "github_issue",
                    "source": "https://github.example/issues/200",
                    "github_issue_number": 200,
                    "github_state": "OPEN",
                    "title": "BUG-200 P0: GitHub-only issue",
                    "labels": [{"name": "module:validation"}, {"name": "severity:p0"}],
                },
            ],
            [],
        ),
    )

    payload = workflow.build_run_p0_plan(module="validation", source="both", mode="plan")

    assert payload["workflow_gate"] == "planned"
    assert payload["source"] == "both"
    assert payload["mode"] == "plan"
    assert [item["bug_id"] for item in payload["items"]] == ["BUG-199", "BUG-200"]
    github_only = payload["items"][1]
    assert github_only["missing_local_bug_json"] is True
    assert github_only["source_channel"] == "github"
    assert "--github-issue-number 200" in github_only["next_command"]
    assert "run --bug-id BUG-199" in payload["next_command"]


def test_run_p0_parser_accepts_documented_source_and_mode() -> None:
    args = workflow.build_parser().parse_args(["run-p0", "--module", "validation", "--source", "both", "--mode", "plan"])

    assert args.source == "both"
    assert args.mode == "plan"


def test_ci_issue_janitor_parser_accepts_legacy_stdout_format_alias() -> None:
    args = workflow.build_parser().parse_args(
        ["ci-issue-janitor", "--superseded-only", "--apply", "--stdout-format", "compact"]
    )

    assert args.command == "ci-issue-janitor"
    assert args.superseded_only is True
    assert args.apply is True
    assert args.output_format == "compact"


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
    suggested_tests = [f"backend/tests/scripts/test_aistock_issue_workflow_{index}.py" for index in range(11)]
    monkeypatch.setattr(
        workflow,
        "_build_batch_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(
            item_id=kwargs["batch_id"],
            affected_tests={"suggested_tests": suggested_tests},
        ),
    )

    payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=[
            "python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> passed",
            "python -m nox -s l0 -> passed",
            "python -m nox -s guardrail_changed_files -> passed",
        ],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["schema_version"] == "aistock_issue_workflow_finish_batch_v1"
    assert payload["workflow_gate"] == "ready_for_pr"
    assert payload["scope_check"]["status"] == "passed"
    assert payload["per_issue_commit_map"] == {"BUG-199": "abc1234", "BUG-200": "def5678"}
    assert payload["codegraph_suggested_tests"] == suggested_tests
    pr_body = (isolated_workflow_root / payload["pr_body_path"]).read_text(encoding="utf-8")
    assert "Closes #199" in pr_body
    assert "Closes #200" in pr_body
    assert "Per-issue closure map" in pr_body
    assert "Code intelligence" in pr_body
    assert suggested_tests[0] in pr_body
    assert suggested_tests[9] in pr_body
    assert suggested_tests[10] not in pr_body
    assert "CodeGraph suggested tests omitted: `1` more" in pr_body


def test_finish_batch_blocks_runtime_bugs_and_never_closes_their_issues(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _runtime_bug(isolated_workflow_root))
    _write_json(
        bugs_root / "bug200.json",
        _runtime_bug(
            isolated_workflow_root,
            bug_id="BUG-200",
            github_issue_number=200,
            github_issue_url="https://github.example/issues/200",
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_build_batch_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(item_id=kwargs["batch_id"]),
    )

    payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["backend/services/example.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m nox -s l0 -> passed"],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["workflow_gate"] == "blocked"
    assert "runtime BUGs require the single-issue" in "; ".join(payload["blocking"])
    pr_body = (isolated_workflow_root / payload["pr_body_path"]).read_text(encoding="utf-8")
    assert "Refs #199" in pr_body
    assert "Refs #200" in pr_body
    assert "Closes #199" not in pr_body
    assert "Closes #200" not in pr_body


def test_finish_batch_blocks_when_one_selected_required_plan_has_no_receipt(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bugs_root = workflow.BUGS_ROOT
    _write_json(bugs_root / "bug199.json", _bug())
    _write_json(
        bugs_root / "bug200.json",
        _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"),
    )
    monkeypatch.setattr(
        workflow,
        "_build_batch_code_intelligence_summary",
        lambda **kwargs: _fake_code_intelligence_summary(item_id=kwargs["batch_id"]),
    )
    original_select_validation = workflow.flow.select_validation

    def selected_validation(changed_files: list[str], module: str | None = None) -> dict[str, Any]:
        payload = original_select_validation(changed_files, module=module)
        payload["required_plans"] = ["l0", "validation_center_backend"]
        return payload

    monkeypatch.setattr(workflow.flow, "select_validation", selected_validation)

    payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m nox -s l0 -> passed"],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["closure_ready"] is False
    assert payload["workflow_gate"] == "blocked"
    assert payload["validation_receipt_plan_coverage"]["missing_required_plans"] == [
        "validation_center_backend"
    ]

    allowed_payload = workflow.build_finish_batch_plan(
        batch_id=None,
        bug_ids=["BUG-199", "BUG-200"],
        changed_files=["scripts/aistock_issue_workflow.py"],
        base="origin/main",
        head="HEAD",
        validation_evidence=["python -m nox -s l0 -> passed"],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=True,
    )

    assert allowed_payload["closure_ready"] is False
    assert allowed_payload["draft_ready"] is False
    assert allowed_payload["workflow_gate"] == "blocked"


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
        validation_evidence=[
            "python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> passed",
            "python -m nox -s l0 -> passed",
            "python -m nox -s validation_module_registry_l0 -> passed",
            "python -m nox -s guardrail_changed_files -> passed",
        ],
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
        validation_evidence=[
            "python -m pytest backend/tests/scripts/test_aistock_issue_workflow.py -q -> passed",
            "python -m nox -s l0 -> passed",
            "python -m nox -s validation_module_registry_l0 -> passed",
            "python -m nox -s guardrail_changed_files -> passed",
        ],
        issue_commit=["BUG-199=abc1234", "BUG-200=def5678"],
        plan_only=False,
        allow_missing_evidence=False,
    )

    assert payload["workflow_gate"] == "ready_for_pr"
    assert payload["scope_check"]["status"] == "passed"


def test_fast_path_classifies_ordinary_docs_as_docs_fast_t0(isolated_workflow_root: Path) -> None:
    payload = workflow.build_fast_path_plan(
        bug_id=None,
        issue_json=None,
        changed_files=["docs/analysis/example.md"],
    )

    assert payload["schema_version"] == "aistock_issue_workflow_fast_path_v1"
    assert payload["task_tier"] == "T0"
    assert payload["validation"]["docs_fast_tier"] == "docs_fast_update"
    assert payload["validation"]["required_plans"] == []
    assert payload["context_strategy"]["max_initial_files"] == 4
    assert "archived standards" in payload["context_strategy"]["avoid_by_default"]
    assert any("design documents" in item for item in payload["context_strategy"]["avoid_by_default"])
    assert payload["production_gates"]["ddl"] == "noop"
    assert payload["required_commands"] == []


def test_fast_path_classifies_controlled_docs_as_t1(isolated_workflow_root: Path) -> None:
    payload = workflow.build_fast_path_plan(
        bug_id=None,
        issue_json=None,
        changed_files=["docs/standards/aistock_issue_workflow_quickstart.md"],
    )

    assert payload["task_tier"] == "T1"
    assert payload["validation"]["docs_controlled_required"] is True
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
    assert payload["context_strategy"]["goal"] == "single issue Context Pack plus targeted code snippets; do not read design docs by default"
    assert any("design documents" in item for item in payload["context_strategy"]["avoid_by_default"])
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
    assert payload["client_manifest"] is None
    assert payload["h7_code_intelligence"] is None
    assert payload["fast_path"]["task_tier"] == "T1"
    assert payload["start"]["worktree_plan"]["dry_run"] is True
    assert payload["finish"]["workflow_gate"] == "plan_ready"
    assert payload["finish"]["closure_ready"] is False
    assert payload["finish"]["draft_ready"] is True
    assert payload["finish"]["artifact_policy"] == "draft_finish_plan_persisted"
    assert payload["postmortem_preview"]["stale_pr_check"] == "skipped_in_smoke_to_avoid_external_github_reads"
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-000*.json"))


def test_workflow_smoke_isolates_synthetic_timing_from_stale_bug_000_state(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_build_code_intelligence_summary", lambda **kwargs: _fake_code_intelligence_summary())
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])
    events_path = isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-000" / "events.jsonl"
    events_path.parent.mkdir(parents=True)
    events_path.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-01-01T00:00:00Z", "event": "state:context_ready", "state": "context_ready"}),
                json.dumps({"timestamp": "2026-06-01T00:00:00Z", "event": "state:validation_passed", "state": "validation_passed"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = workflow.build_workflow_smoke_plan(
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation.guardrails",
    )

    timing = payload["postmortem_preview"]["timing_summary"]
    assert timing["event_count"] <= 1
    assert timing["inferred_elapsed_seconds"] == 0.0
    assert "isolated synthetic BUG-000" in payload["warnings"][0]


def test_nightly_intake_smoke_writes_only_tmp_artifacts_and_handoff(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])

    payload = workflow.build_nightly_intake_smoke_plan()

    assert payload["schema_version"] == "aistock_nightly_intake_smoke_v1"
    assert payload["workflow_gate"] == "passed"
    assert payload["github_writes"] is False
    assert payload["failure_kind"] == "synthetic_smoke"
    assert payload["synthetic"] is True
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
    assert payload["closed_loop_checks"]["synthetic_marker_recorded"] is True
    context_pack = json.loads((isolated_workflow_root / payload["artifacts"]["context"]).read_text(encoding="utf-8"))
    issue_payload = json.loads((isolated_workflow_root / payload["artifacts"]["github_issue_payload"]).read_text(encoding="utf-8"))
    assert context_pack["llm_triage_advice"]["workflow_gate"] in {"ready", "warning"}
    assert context_pack["llm_triage_advice"]["llm_invocation_evidence"]["invoked"] is False
    assert issue_payload["synthetic"] is True
    assert issue_payload["failure_kind"] == "synthetic_smoke"
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
    monkeypatch.delenv("AISTOCK_CODE_INTELLIGENCE_GRAPH_SOURCE_ROOT", raising=False)
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


def test_verify_clients_command_bridges_code_intelligence_adapter(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_verification(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["item_id"] == "BUG-331"
        assert kwargs["module"] == "validation"
        assert kwargs["changed_files"] == ["scripts/aistock_issue_workflow.py"]
        assert kwargs["root"] == isolated_workflow_root
        return {
            "schema_version": "aistock_code_intelligence_client_verification_v1",
            "workflow_gate": "ready",
            "codegraph": {"status": "ok"},
            "freshness": {"effective_freshness": "fresh"},
            "understand_anything": {"status": "available", "freshness": "base_current"},
            "clients": {
                "codex_issue_skill": {"status": "ready"},
                "claude_issue_command": {"status": "ready"},
            },
            "artifacts": {
                "context_ref": "tmp/issue_workflow/BUG-331/codegraph-context.md",
                "affected_tests_ref": "tmp/issue_workflow/BUG-331/affected-tests.json",
                "ua_summary_ref": "tmp/issue_workflow/BUG-331/ua-validation-summary.md",
            },
            "efficiency": {"next_actions": ["read_task_card_code_intelligence_refs"]},
        }

    monkeypatch.setattr(workflow.code_intelligence, "build_client_verification", fake_verification)

    result = workflow.main(
        [
            "verify-clients",
            "--item-id",
            "BUG-331",
            "--module",
            "validation",
            "--changed-file",
            "scripts/aistock_issue_workflow.py",
            "--root",
            str(isolated_workflow_root),
            "--output-format",
            "summary",
        ]
    )

    out = capsys.readouterr().out
    assert result == 0
    assert "PASS verify-clients workflow_gate=ready" in out
    assert "codegraph=ok" in out
    assert "clients_ready=2/2" in out
    assert "{" not in out


def test_doctor_reports_ready_when_client_entries_exist(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "backend").mkdir()
    (isolated_workflow_root / "backend" / "validation_app.py").write_text("", encoding="utf-8")
    _write_repo_client_entrypoints(isolated_workflow_root)
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
    codex_home = isolated_workflow_root / "codex_home"
    for _key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        (codex_home / "skills" / skill_name).mkdir(parents=True)
        (codex_home / "skills" / skill_name / "SKILL.md").write_text("", encoding="utf-8")
    claude_home = isolated_workflow_root / "claude_home"
    (claude_home / "commands").mkdir(parents=True)
    for _key, command_name in workflow.CLIENT_CLAUDE_COMMANDS:
        (claude_home / "commands" / command_name).write_text("", encoding="utf-8")
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
    assert payload["validation_center_runtime_safety"]["workflow_gate"] == "ready"
    assert payload["validation_center_runtime_safety"]["safe_app_module"] == "backend.validation_app:app"
    assert payload["validation_center_runtime_safety"]["unsafe_app_module"] == "backend.main:app"
    assert "8012" in payload["validation_center_runtime_safety"]["safe_command"]
    compact = workflow._compact_payload(payload)
    assert compact["validation_center_runtime_safety"]["safe_app_module"] == "backend.validation_app:app"
    assert compact["worktree_hygiene"]["workflow_gate"] == "ready"
    assert compact["cleanup_janitor"]["workflow_gate"] == "ready"
    assert "run --bug-id BUG-XXX" in payload["next_command"]


def test_doctor_cleanup_janitor_reports_compact_cleanup_debt(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_git",
        lambda args, cwd=None, check=True: {
            ("for-each-ref", "--format=%(refname:short)", "refs/heads"): "\n".join(
                [
                    "main",
                    "bug/merged-clean",
                    "backup/pre-rebase-20260501",
                    "feature/checked-merged",
                ]
            ),
            ("branch", "--format=%(refname:short)", "--merged", "origin/main"): "\n".join(
                ["main", "bug/merged-clean", "feature/checked-merged"]
            ),
        }.get(tuple(args), ""),
    )
    monkeypatch.setattr(
        workflow,
        "_parse_worktree_list",
        lambda: [
            {"worktree": str(isolated_workflow_root), "branch": "refs/heads/main"},
            {"worktree": str(isolated_workflow_root / "wt"), "branch": "refs/heads/feature/checked-merged"},
        ],
    )

    payload = workflow._cleanup_janitor_report(isolated_workflow_root, sample_limit=1)
    compact = workflow._compact_payload({"schema_version": "aistock_issue_workflow_doctor_v1", "cleanup_janitor": payload})

    assert payload["workflow_gate"] == "warning"
    assert payload["safe_merged_local_branch_count"] == 1
    assert payload["stale_backup_or_temp_branch_count"] == 1
    assert payload["checked_out_merged_branch_count"] == 1
    assert payload["safe_merged_local_branch_samples"] == ["bug/merged-clean"]
    assert compact["cleanup_janitor"]["safe_merged_local_branch_count"] == 1
    assert compact["cleanup_janitor"]["stale_backup_or_temp_branch_samples"] == ["backup/pre-rebase-20260501"]
    assert "backup/pre-rebase-20260501" in json.dumps(compact["cleanup_janitor"], ensure_ascii=False)


def test_doctor_blocks_noncanonical_main_worktree_with_stale_index(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale = isolated_workflow_root / "worktrees" / "private-repo-doc-main-20260519"
    stale.mkdir(parents=True)
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "backend").mkdir()
    (isolated_workflow_root / "backend" / "validation_app.py").write_text("", encoding="utf-8")
    _write_repo_client_entrypoints(isolated_workflow_root)
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
    monkeypatch.setattr(workflow, "_canonical_root", lambda: isolated_workflow_root)
    monkeypatch.setattr(
        workflow,
        "_parse_worktree_list",
        lambda: [
            {"worktree": str(isolated_workflow_root), "HEAD": "abc1234", "branch": "refs/heads/main"},
            {"worktree": str(stale), "HEAD": "old1234", "branch": "refs/heads/main"},
        ],
    )

    def fake_git_snapshot(root: Path) -> dict[str, Any]:
        if root == stale:
            status = "## main...origin/main\n" + "\n".join(f"A  stale-{idx}.txt" for idx in range(120))
            return {
                "ok": True,
                "branch": "main",
                "head": "old1234",
                "origin_main": "abc1234",
                "dirty": True,
                "dirty_count": 120,
                "status": status,
            }
        return {
            "ok": True,
            "branch": "main",
            "head": "abc1234",
            "origin_main": "abc1234",
            "dirty": False,
            "dirty_count": 0,
            "status": "## main...origin/main",
        }

    monkeypatch.setattr(workflow, "_git_snapshot", fake_git_snapshot)
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
    compact = workflow._compact_payload(payload)

    assert payload["workflow_gate"] == "blocked"
    assert payload["worktree_hygiene"]["workflow_gate"] == "blocked"
    assert payload["worktree_hygiene"]["noncanonical_main_worktrees"][0]["staged_count"] == 120
    assert any("non-canonical worktree" in item for item in payload["blocking"])
    assert compact["worktree_hygiene"]["noncanonical_main_worktree_count"] == 1


def test_doctor_warns_when_bug_allocator_lags_github(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    _write_repo_client_entrypoints(isolated_workflow_root)
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
        "_github_bug_issue_for_id",
        lambda bug_id, **_kwargs: (
            {
                "bug_id": "BUG-217",
                "number": 217,
                "kind": "github_issue",
                "source": "https://github.example/issues/588",
                "github_issue_number": 588,
                "github_state": "OPEN",
                "title": "BUG-217 existing",
            },
            [],
        )
        if bug_id == "BUG-217"
        else (None, []),
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
    assert payload["bug_id_allocation"]["github_lookup_mode"] == "exact_candidate"
    assert any("bug id allocation" in warning for warning in payload["warnings"])
    assert compact["bug_id_allocation"]["next_number"] == 218


def test_doctor_ignores_invalid_unrelated_worktree_allocator(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    _write_repo_client_entrypoints(isolated_workflow_root)
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
    _write_json(workflow.BUGS_ROOT / ".bug_id_allocator.json", {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 328})
    stale_bugs_root = (
        isolated_workflow_root
        / "worktrees"
        / "BUG-326-other-window"
        / "tests"
        / "aistock_validation"
        / "bugs"
    )
    stale_bugs_root.mkdir(parents=True)
    (stale_bugs_root / ".bug_id_allocator.json").write_text("{", encoding="utf-8")
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
            "codegraph": {"status": "ok"},
            "understand_anything": {"status": "available"},
        },
    )

    payload = workflow.build_doctor_report(skip_external=True)
    compact = workflow._compact_payload(payload)

    assert payload["blocking"] == []
    assert payload["bug_id_allocation"]["next_number"] == 329
    assert not any("unrelated invalid BUG id allocator" in item for item in payload["bug_id_allocation"]["warnings"])
    assert not any("unrelated invalid BUG id allocator" in item for item in payload["warnings"])
    assert compact["bug_id_allocation"]["next_number"] == 329


def test_bug_allocation_report_keeps_canonical_allocator_strict(isolated_workflow_root: Path) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    allocator.parent.mkdir(parents=True, exist_ok=True)
    allocator.write_text("{", encoding="utf-8")

    with pytest.raises(workflow.WorkflowError, match="invalid bug id allocator"):
        workflow._bug_id_allocation_report(isolated_workflow_root)


def test_bug_id_scan_roots_do_not_enumerate_other_worktrees(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow.BUGS_ROOT.mkdir(parents=True)
    stale_root = isolated_workflow_root / "worktrees" / "stale" / "tests" / "aistock_validation" / "bugs"
    stale_root.mkdir(parents=True)
    monkeypatch.setattr(
        workflow,
        "_parse_worktree_list",
        lambda: pytest.fail("BUG allocation must not enumerate registered worktrees"),
    )

    roots = workflow._bug_id_scan_roots(isolated_workflow_root)

    assert stale_root not in roots
    assert roots == [workflow._bugs_root(isolated_workflow_root)]


def test_bug_registry_scan_uses_canonical_filename_without_parsing_json(
    isolated_workflow_root: Path,
) -> None:
    bug_path = workflow.BUGS_ROOT / "20260810_BUG-777-invalid-body.json"
    bug_path.parent.mkdir(parents=True, exist_ok=True)
    bug_path.write_text("{", encoding="utf-8")

    sources = workflow._scan_bug_registry_ids(isolated_workflow_root)

    assert any(item["kind"] == "bug_json" and item["number"] == 777 for item in sources)


def test_doctor_compact_reports_codegraph_bootstrap_next_command(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (isolated_workflow_root / "scripts").mkdir()
    (isolated_workflow_root / "scripts" / "aistock_issue_workflow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    _write_repo_client_entrypoints(isolated_workflow_root)
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
    _write_repo_client_entrypoints(isolated_workflow_root)
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
    for key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        repo_skill = isolated_workflow_root / ".codex" / "skills" / skill_name
        repo_skill.mkdir(parents=True)
        (repo_skill / "SKILL.md").write_text(f"repo {key} skill", encoding="utf-8")
    (isolated_workflow_root / "scripts" / "issue_flow.py").write_text("", encoding="utf-8")
    (isolated_workflow_root / ".claude" / "commands").mkdir(parents=True)
    for key, command_name in workflow.CLIENT_CLAUDE_COMMANDS:
        (isolated_workflow_root / ".claude" / "commands" / command_name).write_text(f"repo {key} command", encoding="utf-8")
    (isolated_workflow_root / "docs" / "standards").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md").write_text("", encoding="utf-8")
    (isolated_workflow_root / "docs" / "architecture").mkdir(parents=True)
    (isolated_workflow_root / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md").write_text("", encoding="utf-8")
    codex_home = isolated_workflow_root / "codex_home"
    for key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        global_skill = codex_home / "skills" / skill_name
        global_skill.mkdir(parents=True)
        content = "old skill" if key == "issue" else f"repo {key} skill"
        (global_skill / "SKILL.md").write_text(content, encoding="utf-8")
    claude_home = isolated_workflow_root / "claude_home"
    (claude_home / "commands").mkdir(parents=True)
    for key, command_name in workflow.CLIENT_CLAUDE_COMMANDS:
        (claude_home / "commands" / command_name).write_text(f"repo {key} command", encoding="utf-8")
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
        },
    )

    payload = workflow.build_doctor_report(skip_external=True)

    assert payload["workflow_gate"] == "warning"
    assert payload["client_manifest"]["codex_skill_status"] == "stale"
    assert payload["restart_recommended"] is False
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


def test_run_plan_missing_local_bug_json_reports_github_adopt_command(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_for_id",
        lambda _bug_id: (
            {
                "bug_id": "BUG-302",
                "number": 302,
                "kind": "github_issue",
                "source": "https://github.com/licong01-cloud/AIstock/issues/897",
                "github_issue_number": 897,
                "github_state": "OPEN",
                "title": "BUG-302 P1: missing registry record",
                "labels": [{"name": "module:paper_v2"}, {"name": "severity:p1"}],
            },
            [],
        ),
    )

    with pytest.raises(workflow.WorkflowPayloadError) as excinfo:
        workflow.build_run_plan(
            bug_id="BUG-302",
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

    payload = excinfo.value.payload
    assert payload["schema_version"] == "aistock_issue_workflow_missing_bug_record_v1"
    assert payload["workflow_gate"] == "missing_bug_record"
    assert payload["github_issue"]["number"] == 897
    assert payload["inferred_module"] == "paper_v2"
    assert payload["inferred_severity"] == "P1"
    assert "--bug-id BUG-302" in payload["next_command"]
    assert "--github-issue-number 897" in payload["next_command"]
    assert "--module paper_v2 --severity P1" in payload["next_command"]
    assert "--create-fix-worktree --apply" in payload["next_command"]


def test_run_cli_missing_local_bug_json_emits_compact_recovery_payload(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_for_id",
        lambda _bug_id: (
            {
                "bug_id": "BUG-302",
                "number": 302,
                "kind": "github_issue",
                "source": "https://github.com/licong01-cloud/AIstock/issues/897",
                "github_issue_number": 897,
                "github_state": "OPEN",
                "title": "BUG-302 P1: missing registry record",
                "labels": [{"name": "module:paper_v2"}, {"name": "severity:p1"}],
            },
            [],
        ),
    )

    assert workflow.main(["run", "--bug-id", "BUG-302", "--mode", "plan", "--create-worktree"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["workflow_gate"] == "missing_bug_record"
    assert payload["github_issue"]["number"] == 897
    assert payload["inferred_module"] == "paper_v2"
    assert payload["next_command"].startswith("python scripts/aistock_issue_workflow.py submit-bug")
    assert "BUG record not found" not in payload["next_command"]


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


def test_submit_bug_file_preflight_rejects_nonexistent_changed_file(
    isolated_workflow_root: Path,
) -> None:
    missing = "backend/services/simulation_runtime/lifecycle_scheduler.py"

    with pytest.raises(
        workflow.WorkflowError,
        match=r"--changed-file does not exist.*lifecycle_scheduler\.py",
    ):
        workflow._validate_submit_bug_file_inputs(
            changed_files=[missing],
            added_files=[],
            module="simulation_runtime",
            root=isolated_workflow_root,
        )


def test_submit_bug_file_preflight_accepts_existing_changed_and_explicit_added_file(
    isolated_workflow_root: Path,
) -> None:
    changed = isolated_workflow_root / "scripts" / "aistock_issue_workflow.py"
    changed.parent.mkdir(parents=True)
    changed.write_text("# existing workflow\n", encoding="utf-8")

    result = workflow._validate_submit_bug_file_inputs(
        changed_files=[
            r".\scripts\aistock_issue_workflow.py",
            "scripts/aistock_issue_workflow.py",
        ],
        added_files=[r"backend\tests\scripts\test_submit_bug_added_file.py"],
        module="validation.guardrails",
        root=isolated_workflow_root,
    )

    assert result["changed_files"] == ["scripts/aistock_issue_workflow.py"]
    assert result["added_files"] == ["backend/tests/scripts/test_submit_bug_added_file.py"]
    assert result["scope_files"] == [
        "scripts/aistock_issue_workflow.py",
        "backend/tests/scripts/test_submit_bug_added_file.py",
    ]
    assert result["ownership"]["unmatched_files"] == []


def test_submit_bug_file_preflight_rejects_existing_added_file(
    isolated_workflow_root: Path,
) -> None:
    existing = isolated_workflow_root / "backend" / "tests" / "scripts" / "test_existing.py"
    existing.parent.mkdir(parents=True)
    existing.write_text("# existing test\n", encoding="utf-8")

    with pytest.raises(
        workflow.WorkflowError,
        match=r"--added-file already exists.*use --changed-file",
    ):
        workflow._validate_submit_bug_file_inputs(
            changed_files=[],
            added_files=["backend/tests/scripts/test_existing.py"],
            module="validation.guardrails",
            root=isolated_workflow_root,
        )


def test_submit_bug_file_preflight_rejects_changed_directory(
    isolated_workflow_root: Path,
) -> None:
    directory = isolated_workflow_root / "scripts" / "not_a_file"
    directory.mkdir(parents=True)

    with pytest.raises(workflow.WorkflowError, match=r"--changed-file must identify a file.*not_a_file"):
        workflow._validate_submit_bug_file_inputs(
            changed_files=["scripts/not_a_file"],
            added_files=[],
            module="validation.guardrails",
            root=isolated_workflow_root,
        )


@pytest.mark.parametrize(
    "unsafe_path",
    [
        "../outside.py",
        "/absolute/path.py",
        r"C:\outside\path.py",
        r"\\server\share\path.py",
        "scripts/bad?.py",
        "scripts/NUL.py",
        "scripts/trailing-dot.",
    ],
)
def test_submit_bug_file_preflight_rejects_unsafe_paths(
    isolated_workflow_root: Path,
    unsafe_path: str,
) -> None:
    with pytest.raises(workflow.WorkflowError, match="repository-relative"):
        workflow._validate_submit_bug_file_inputs(
            changed_files=[],
            added_files=[unsafe_path],
            module="validation.guardrails",
            root=isolated_workflow_root,
        )


def test_submit_bug_file_preflight_rejects_cross_category_duplicate(
    isolated_workflow_root: Path,
) -> None:
    with pytest.raises(workflow.WorkflowError, match="both --changed-file and --added-file"):
        workflow._validate_submit_bug_file_inputs(
            changed_files=["scripts/aistock_issue_workflow.py"],
            added_files=[r"scripts\aistock_issue_workflow.py"],
            module="validation.guardrails",
            root=isolated_workflow_root,
        )


def test_submit_bug_file_preflight_rejects_unowned_added_file(
    isolated_workflow_root: Path,
) -> None:
    with pytest.raises(
        workflow.WorkflowError,
        match=r"file ownership catalog has no match.*unowned/new_file\.py",
    ):
        workflow._validate_submit_bug_file_inputs(
            changed_files=[],
            added_files=["unowned/new_file.py"],
            module="validation.guardrails",
            root=isolated_workflow_root,
        )


def test_submit_bug_parser_exposes_added_file_separately() -> None:
    args = workflow.build_parser().parse_args(
        [
            "submit-bug",
            "--title",
            "workflow file contract",
            "--module",
            "validation.guardrails",
            "--changed-file",
            "scripts/aistock_issue_workflow.py",
            "--added-file",
            "backend/tests/scripts/test_new_workflow_contract.py",
        ]
    )

    assert args.changed_file == ["scripts/aistock_issue_workflow.py"]
    assert args.added_file == ["backend/tests/scripts/test_new_workflow_contract.py"]


def test_submit_bug_cli_rejects_invalid_scope_before_allocator_or_registry_mutation(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})

    result = workflow.main(
        [
            "submit-bug",
            "--title",
            "invalid changed file",
            "--module",
            "validation.guardrails",
            "--severity",
            "P1",
            "--bug-id",
            "BUG-118",
            "--github-issue-number",
            "188",
            "--github-issue-url",
            "https://github.com/licong01-cloud/AIstock/issues/188",
            "--changed-file",
            "scripts/nonexistent_workflow.py",
            "--apply",
        ]
    )

    assert result == 2
    assert (
        "--changed-file does not exist: scripts/nonexistent_workflow.py"
        in capsys.readouterr().err
    )
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 117
    assert not list(workflow.BUGS_ROOT.glob("*BUG-118*.json"))
    assert not (isolated_workflow_root / "tmp" / "issue_workflow" / "BUG-118").exists()


def test_submit_bug_plan_persists_changed_and_added_file_scope_contract(
    isolated_workflow_root: Path,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 117})

    payload = workflow.build_submit_bug_plan(
        title="workflow file scope contract",
        module="validation.guardrails",
        severity="P1",
        description="Distinguish existing and planned files.",
        expected="Exact file categories are persisted.",
        actual="The old workflow merged both categories.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=["scripts/aistock_issue_workflow.py"],
        added_files=["backend/tests/scripts/test_new_workflow_contract.py"],
        plan_key="validation_workflow_automation",
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

    expected_contract = {
        "schema_version": "aistock_submit_bug_file_scope_v1",
        "changed_files": ["scripts/aistock_issue_workflow.py"],
        "added_files": ["backend/tests/scripts/test_new_workflow_contract.py"],
        "scope_files": [
            "scripts/aistock_issue_workflow.py",
            "backend/tests/scripts/test_new_workflow_contract.py",
        ],
        "ownership": workflow.flow.match_changed_files(
            [
                "scripts/aistock_issue_workflow.py",
                "backend/tests/scripts/test_new_workflow_contract.py",
            ]
        ),
    }
    assert payload["file_scope_contract"] == expected_contract
    assert payload["record"]["file_scope_contract"] == expected_contract
    assert set(payload["record"]["allowed_write_scope"]) >= set(expected_contract["scope_files"])


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


def test_registry_worktree_creation_uses_canonical_root_not_disposable_checkout(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = isolated_workflow_root / "canonical"
    canonical.mkdir(parents=True)
    calls: list[tuple[list[str], Path | None]] = []

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        calls.append((args, cwd))
        return ""

    monkeypatch.setattr(workflow, "_canonical_root", lambda: canonical)
    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_git_worktree_add_new_branch", lambda **_kwargs: calls.append((["worktree-add"], canonical)))

    plan = workflow._maybe_create_registry_worktree(
        title="Nightly candidate",
        module="validation.runner",
        severity="P1",
        create=True,
        dry_run=False,
    )

    assert plan["git_root"] == str(canonical)
    assert calls[0] == (["fetch", "origin", "main"], canonical)
    assert calls[1][0] == ["worktree-add"]


def _nightly_candidate_payload(path: Path, *, ready: bool = True, confidence: float = 0.91) -> Path:
    candidate = {
        "schema_version": "aistock_bug_candidate_v1",
        "candidate_id": "NC-20260618-workflow",
        "source": "nightly_discovery",
        "source_plan_key": "workflow_discovery_root_clean_guard",
        "module": "validation.runner",
        "severity": "P1",
        "confidence": confidence,
        "title": "Unexpected dirty path in nightly workspace",
        "summary": "Nightly discovered a root-cleanliness regression.",
        "expected": "Nightly workspace should stay clean except ignored tmp validation artifacts.",
        "actual": "A tracked workflow file was dirty before nightly execution.",
        "reproduce": ["python scripts/nightly_discovery_plans.py --json run --plan-key workflow_discovery_root_clean_guard"],
        "evidence_refs": ["scripts/aistock_issue_workflow.py"],
        "codegraph_refs": ["tmp/validation/code-intelligence/codegraph-freshness.json"],
        "ua_refs": ["tmp/validation/code-intelligence/ua-summary-manifest.json"],
        "dedupe_fingerprint": "nc-workflow-root-dirty",
        "fingerprint": "nc-workflow-root-dirty",
        "allowed_write_scope": ["scripts/aistock_issue_workflow.py"],
        "suggested_validation": ["python -m nox -s workflow_discovery_root_clean_guard"],
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
        "source_anomaly": {"synthetic": False},
        "quality_gate": {
            "schema_version": "aistock_bug_candidate_quality_gate_v1",
            "workflow_gate": "ready" if ready else "draft",
            "issue_payload_ready": ready,
            "threshold": 0.8,
            "reasons": [] if ready else ["confidence_below_threshold"],
            "auto_submit_allowed": False,
        },
    }
    payload = {
        "schema_version": "aistock_bug_candidate_github_issue_payload_v1",
        "mode": "draft_only",
        "repo": "licong01-cloud/AIstock",
        "candidate_id": candidate["candidate_id"],
        "title": "[P1] Unexpected dirty path in nightly workspace",
        "body": "## Failure / Anomaly Summary\nNightly discovered a root-cleanliness regression.\n",
        "labels": ["P1", "severity:p1", "module:validation.runner", "nightly-discovery"],
        "candidate": candidate,
        "dedupe": {
            "fingerprint": candidate["dedupe_fingerprint"],
            "marker": "<!-- aistock-nightly-bug-candidate:nc-workflow-root-dirty -->",
            "search_query": "repo:licong01-cloud/AIstock is:issue in:body nc-workflow-root-dirty",
        },
        "auto_submit_allowed": False,
        "production_gates": candidate["production_gates"],
    }
    return _write_json(path, payload)


def test_promote_nightly_candidate_blocks_apply_without_isolated_worktree(
    isolated_workflow_root: Path,
) -> None:
    payload_path = _nightly_candidate_payload(isolated_workflow_root / "tmp" / "candidate-payload.json")

    payload = workflow.build_promote_nightly_candidate_plan(
        issue_payload=[str(payload_path)],
        queue_manifest=None,
        apply=True,
        opt_in_auto_file=False,
        create_registry_worktree=False,
        create_fix_worktree=False,
        skip_dedupe_search=True,
    )

    assert payload["workflow_gate"] == "blocked"
    assert any("--create-registry-worktree" in item for item in payload["blocking"])
    assert not any("--opt-in-auto-file" in item for item in payload["blocking"])
    assert not list(workflow.BUGS_ROOT.glob("*BUG-*.json"))


def test_promote_nightly_candidate_dry_run_builds_complete_issue_workflow_handoff(
    isolated_workflow_root: Path,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 415})
    payload_path = _nightly_candidate_payload(isolated_workflow_root / "tmp" / "candidate-payload.json")

    payload = workflow.build_promote_nightly_candidate_plan(
        issue_payload=[str(payload_path)],
        queue_manifest=None,
        apply=False,
        opt_in_auto_file=False,
        create_registry_worktree=False,
        create_fix_worktree=False,
        skip_dedupe_search=True,
    )

    assert payload["workflow_gate"] == "ready_for_apply"
    assert payload["promotion_mode"] == "deterministic_quality_gate"
    assert payload["llm_enhancement_opt_in"] is False
    assert "--opt-in-auto-file" not in payload["next_command"]
    assert payload["submit_bug"]["bug_id"] == "BUG-416"
    body = workflow._render_github_issue_body(
        payload["submit_bug"]["record"],
        {"candidate_id": payload["submit_bug"]["candidate_id"]},
    )
    assert "## Expected" in body
    assert "## Actual" in body
    assert "## Next Step" in body
    assert "promote-nightly-candidate" not in body
    assert "CodeGraph / Understand Anything Refs" in body
    assert "aistock-nightly-bug-candidate:nc-workflow-root-dirty" in body
    assert payload["submit_bug"]["github"]["planned"] is True


def test_promote_nightly_candidate_apply_creates_github_linked_bug_in_registry_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "worktrees" / "registry-nightly"
    allocator = registry / "tests" / "aistock_validation" / "bugs" / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 415})
    payload_path = _nightly_candidate_payload(isolated_workflow_root / "tmp" / "candidate-payload.json")
    created_issue_body: dict[str, str] = {}

    def fake_registry_worktree(**kwargs: Any) -> dict[str, Any]:
        registry.mkdir(parents=True, exist_ok=True)
        return {
            "create_worktree": kwargs["create"],
            "dry_run": kwargs["dry_run"],
            "branch": "bug/registry-validation-nightly",
            "worktree": str(registry),
            "base": "origin/main",
            "created": kwargs["create"],
        }

    def fake_create_issue(**kwargs: Any) -> dict[str, Any]:
        body_path = Path(kwargs["body_path"])
        created_issue_body["body"] = body_path.read_text(encoding="utf-8")
        assert kwargs["cwd"] == registry
        return {
            "created": True,
            "url": "https://github.com/licong01-cloud/AIstock/issues/900",
            "number": 900,
            "recovered_after_transport_error": False,
            "warnings": [],
        }

    monkeypatch.setattr(workflow, "_maybe_create_registry_worktree", fake_registry_worktree)
    monkeypatch.setattr(workflow, "_create_github_issue_with_recovery", fake_create_issue)
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})
    monkeypatch.setattr(
        workflow,
        "_commit_bug_registration_in_fix_worktree",
        lambda root, bug_id: {
            "workflow_gate": "committed",
            "root": str(root),
            "branch": "bug/registry-validation-nightly",
            "commit": "abc123def456",
            "changed_files": ["tests/aistock_validation/bugs/20260618_BUG-416-unexpected-dirty-path-in-nightly-workspace.json"],
        },
    )

    payload = workflow.build_promote_nightly_candidate_plan(
        issue_payload=[str(payload_path)],
        queue_manifest=None,
        apply=True,
        opt_in_auto_file=False,
        create_registry_worktree=True,
        create_fix_worktree=False,
        skip_dedupe_search=True,
    )

    assert payload["workflow_gate"] == "promoted"
    assert payload["promotion_mode"] == "deterministic_quality_gate"
    assert payload["llm_enhancement_opt_in"] is False
    assert payload["github_issue_number"] == 900
    assert payload["submit_bug"]["bug_id"] == "BUG-416"
    assert payload["submit_bug"]["nightly_registry_commit"]["workflow_gate"] == "committed"
    bug_path = registry / payload["submit_bug"]["bug_json_path"]
    assert bug_path.exists()
    record = json.loads(bug_path.read_text(encoding="utf-8"))
    assert record["github_issue_number"] == 900
    assert record["github_issue_url"] == "https://github.com/licong01-cloud/AIstock/issues/900"
    assert record["production_ddl_gate"] == "noop"
    assert "scripts/aistock_issue_workflow.py" in record["allowed_write_scope"]
    assert "CodeGraph / Understand Anything Refs" in created_issue_body["body"]
    assert "python scripts/aistock_issue_workflow.py run --bug-id BUG-416 --mode plan --create-worktree" in created_issue_body["body"]
    assert str(bug_path) in payload["next_command"]


def test_submit_bug_allocator_uses_reservations_and_ignores_stale_worktrees(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    stale = isolated_workflow_root / "worktrees" / "stale-registry" / "tests" / "aistock_validation" / "bugs"
    _write_json(stale / "20260528_BUG-999-other-window.json", {"bug_id": "BUG-999", "title": "Stale worktree"})
    reservation = isolated_workflow_root / "bug-id-reservations" / "BUG-136.json"
    _write_json(reservation, {"schema_version": "aistock_bug_id_reservation_v1", "bug_id": "BUG-136"})
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


def test_submit_bug_allocator_skips_exact_github_candidate_collision(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 216})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})
    looked_up: list[str] = []

    def exact_lookup(bug_id: str, **_kwargs: object):
        looked_up.append(bug_id)
        if bug_id == "BUG-217":
            return (
                {
                    "bug_id": "BUG-217",
                    "number": 217,
                    "kind": "github_issue",
                    "source": "https://github.example/issues/588",
                    "github_issue_number": 588,
                    "github_state": "OPEN",
                    "title": "BUG-217 P1: Different existing issue",
                },
                [],
            )
        return None, []

    monkeypatch.setattr(workflow, "_github_bug_issue_for_id", exact_lookup)
    monkeypatch.setattr(
        workflow,
        "_scan_github_bug_ids",
        lambda **_kwargs: pytest.fail("automatic allocation must not run a full GitHub issue scan"),
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
    assert payload["bug_id_allocation"]["github_lookup_mode"] == "exact_candidate"
    assert payload["bug_id_allocation"]["github_scanned"] is False
    assert looked_up == ["BUG-217", "BUG-218"]
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 218


def test_reserve_bug_id_performs_exact_github_lookup_after_allocator_lock(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"locked": False}
    original_registry_scan = workflow._scan_bug_registry_ids
    _write_json(
        workflow.BUGS_ROOT / ".bug_id_allocator.json",
        {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 216},
    )

    class FakeLock:
        def __enter__(self):
            assert state["locked"] is False
            state["locked"] = True
            return self

        def __exit__(self, *_args: object) -> None:
            state["locked"] = False

    def exact_lookup(bug_id: str, **_kwargs: object):
        assert state["locked"] is False
        assert bug_id == "BUG-217"
        return None, []

    def registry_scan(*args: object, **kwargs: object):
        assert state["locked"] is True
        return original_registry_scan(*args, **kwargs)

    monkeypatch.setattr(workflow, "_GlobalBugIdAllocatorLock", FakeLock)
    monkeypatch.setattr(workflow, "_github_bug_issue_for_id", exact_lookup)
    monkeypatch.setattr(
        workflow,
        "_scan_github_bug_ids",
        lambda **_kwargs: pytest.fail("automatic allocation must not run a full GitHub issue scan"),
    )
    monkeypatch.setattr(workflow, "_scan_bug_registry_ids", registry_scan)

    bug_id, number, report, reservation = workflow._reserve_bug_id(
        isolated_workflow_root,
        bug_id=None,
        include_github=True,
        github_required=True,
        allowed_github_issue_number=None,
        reservation_title="New allocator issue",
        reservation_fingerprint="fingerprint-217",
    )
    try:
        assert bug_id == "BUG-217"
        assert number == 217
        assert report["github_scanned"] is False
        assert report["github_lookup_mode"] == "exact_candidate"
    finally:
        workflow._release_bug_id_reservation(reservation)


def test_submit_bug_retry_adopts_matching_open_github_issue(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})
    common = {
        "title": "Retry-safe allocator issue",
        "module": "validation",
        "severity": "P1",
        "description": "Retry must adopt the exact open GitHub Issue.",
        "expected": "No duplicate Issue is created.",
        "actual": "The first attempt lost transport confirmation.",
        "reproduce_command": "n/a",
        "evidence_refs": [],
        "changed_files": ["scripts/aistock_issue_workflow.py"],
        "plan_key": None,
        "nox_session": None,
        "candidate_type": "bug",
        "bug_id": None,
        "github_issue_number": None,
        "github_issue_url": None,
        "create_github": True,
        "create_registry_worktree": False,
        "registry_pr_only": False,
        "dry_run": False,
    }
    dry_run = workflow.build_submit_bug_plan(**common, apply=False)
    reservation = isolated_workflow_root / "bug-id-reservations" / "BUG-133.json"
    _write_json(
        reservation,
        {
            "schema_version": "aistock_bug_id_reservation_v1",
            "bug_id": "BUG-133",
            "reserved_at": "2026-08-10T00:00:00Z",
            "reserved_by": "aistock_issue_workflow.py",
            "root": str(isolated_workflow_root),
            "status": "github_create_outcome_unknown",
            "title": common["title"],
            "fingerprint": dry_run["record"]["trigger_condition"]["fingerprint"],
        },
    )
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_for_id",
        lambda bug_id, **_kwargs: (
            {
                "bug_id": "BUG-133",
                "number": 133,
                "kind": "github_issue",
                "source": "https://github.example/issues/588",
                "github_issue_number": 588,
                "github_state": "OPEN",
                "title": "BUG-133 P1: Retry-safe allocator issue",
            },
            [],
        )
        if bug_id == "BUG-133"
        else (None, []),
    )
    monkeypatch.setattr(
        workflow,
        "_create_github_issue_with_recovery",
        lambda **_kwargs: pytest.fail("matching orphan Issue must be adopted instead of created again"),
    )

    payload = workflow.build_submit_bug_plan(**common, apply=True)

    assert payload["bug_id"] == "BUG-133"
    assert payload["github"] == {
        "created": False,
        "recovered_existing": True,
        "url": "https://github.example/issues/588",
        "number": 588,
    }
    assert json.loads(reservation.read_text(encoding="utf-8"))["status"] == "registered"
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 133


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
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_by_number",
        lambda _issue_number: (None, ["linked GitHub Issue lookup unavailable: offline"]),
    )

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
    assert payload["bug_id_allocation"]["warnings"] == ["linked GitHub Issue lookup unavailable: offline"]
    assert json.loads(allocator.read_text(encoding="utf-8"))["last_allocated"] == 137


def test_submit_bug_offline_exact_github_lookup_warns_but_uses_local_scan(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_for_id",
        lambda bug_id, **_kwargs: (None, [f"GitHub lookup for {bug_id} unavailable: offline"]),
    )

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
    assert payload["bug_id_allocation"]["warnings"] == ["GitHub lookup for BUG-133 unavailable: offline"]
    assert payload["bug_id_allocation"]["github_lookup_mode"] == "exact_candidate"


def test_install_client_plan_can_copy_global_codex_skill(
    isolated_workflow_root: Path,
) -> None:
    for key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        source = isolated_workflow_root / ".codex" / "skills" / skill_name
        source.mkdir(parents=True)
        (source / "SKILL.md").write_text(f"{key} skill", encoding="utf-8")
    claude = isolated_workflow_root / ".claude" / "commands"
    claude.mkdir(parents=True)
    for key, command_name in workflow.CLIENT_CLAUDE_COMMANDS:
        (claude / command_name).write_text(f"{key} command", encoding="utf-8")
    codex_home = isolated_workflow_root / "codex_home"
    claude_home = isolated_workflow_root / "claude_home"

    dry = workflow.build_client_install_plan(codex_home=str(codex_home), claude_home=str(claude_home))
    assert dry["workflow_gate"] == "ready_for_install"
    assert dry["dry_run"] is True

    applied = workflow.build_client_install_plan(apply=True, codex_home=str(codex_home), claude_home=str(claude_home))
    assert applied["workflow_gate"] == "installed"
    for key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        assert (codex_home / "skills" / skill_name / "SKILL.md").read_text(encoding="utf-8") == f"{key} skill"
    for key, command_name in workflow.CLIENT_CLAUDE_COMMANDS:
        assert (claude_home / "commands" / command_name).read_text(encoding="utf-8") == f"{key} command"
    assert applied["client_manifest_after"]["codex_feature_skill_status"] == "current"
    assert applied["client_manifest_after"]["codex_router_skill_status"] == "current"
    assert applied["client_manifest_after"]["claude_feature_command_status"] == "current"
    assert applied["client_manifest_after"]["claude_router_command_status"] == "current"

    isolated_codex_home = isolated_workflow_root / "official_codex_home"
    untouched_claude_home = isolated_workflow_root / "untouched_claude_home"
    sentinel = untouched_claude_home / "commands" / "keep.md"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("keep", encoding="utf-8")
    codex_only = workflow.build_client_install_plan(
        apply=True,
        codex_home=str(isolated_codex_home),
        claude_home=str(untouched_claude_home),
        install_claude=False,
    )
    assert codex_only["install_codex"] is True
    assert codex_only["install_claude"] is False
    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert not (untouched_claude_home / "commands" / "fix-aistock-issue.md").exists()


def test_verify_clients_workflow_only_checks_every_lane(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    codex_home = isolated_workflow_root / "verified_codex_home"
    claude_home = isolated_workflow_root / "verified_claude_home"
    workflow.build_client_install_plan(
        apply=True,
        codex_home=str(codex_home),
        claude_home=str(claude_home),
    )

    result = workflow.main(
        [
            "verify-clients",
            "--workflow-only",
            "--codex-home",
            str(codex_home),
            "--claude-home",
            str(claude_home),
            "--output-format",
            "full-json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["workflow_gate"] == "ready"
    assert len(payload["client_manifest"]["codex_entries"]) == len(workflow.CLIENT_CODEX_SKILLS)
    assert len(payload["client_manifest"]["claude_entries"]) == len(workflow.CLIENT_CLAUDE_COMMANDS)
    assert all(item["status"] == "current" for item in payload["client_manifest"]["codex_entries"].values())
    assert all(item["status"] == "current" for item in payload["client_manifest"]["claude_entries"].values())


def test_verify_clients_selected_lane_warns_for_unrelated_stale_entry(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    codex_home = isolated_workflow_root / "selected_codex_home"
    workflow.build_client_install_plan(
        apply=True,
        codex_home=str(codex_home),
        install_claude=False,
    )
    (codex_home / "skills" / "verify-aistock-feature" / "SKILL.md").write_text("stale", encoding="utf-8")

    result = workflow.main(
        [
            "verify-clients",
            "--workflow-only",
            "--selected-lane",
            "issue",
            "--codex-home",
            str(codex_home),
            "--skip-claude",
            "--output-format",
            "full-json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["workflow_gate"] == "ready"
    assert payload["selected_lane_keys"] == ["issue", "router"]
    assert payload["blocking"] == []
    assert payload["restart_recommended"] is False
    assert any("unrelated codex lane feature is stale" in item for item in payload["warnings"])

    workflow._emit(payload, output_format="summary")
    summary = capsys.readouterr().out.strip()
    assert "workflow_gate=ready" in summary
    assert "lane=issue" in summary
    assert "blocking=0" in summary
    assert "warnings=1" in summary
    assert "restart_recommended=false" in summary


def test_verify_clients_selected_lane_blocks_when_selected_entry_is_stale(
    isolated_workflow_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    codex_home = isolated_workflow_root / "blocked_codex_home"
    workflow.build_client_install_plan(
        apply=True,
        codex_home=str(codex_home),
        install_claude=False,
    )
    (codex_home / "skills" / "verify-aistock-feature" / "SKILL.md").write_text("stale", encoding="utf-8")

    result = workflow.main(
        [
            "verify-clients",
            "--workflow-only",
            "--selected-lane",
            "feature",
            "--codex-home",
            str(codex_home),
            "--skip-claude",
            "--output-format",
            "full-json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert result == 2
    assert payload["workflow_gate"] == "blocked"
    assert payload["warnings"] == []
    assert payload["restart_recommended"] is False
    assert payload["blocking"] == ["codex lane feature is stale"]


def test_install_client_selected_lane_is_idempotent_and_leaves_unrelated_lane_untouched(
    isolated_workflow_root: Path,
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    codex_home = isolated_workflow_root / "targeted_codex_home"
    unrelated = codex_home / "skills" / "fix-aistock-issue" / "SKILL.md"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("preserve unrelated lane", encoding="utf-8")

    first = workflow.build_client_install_plan(
        apply=True,
        codex_home=str(codex_home),
        install_claude=False,
        selected_lane="feature",
    )

    assert first["selected_lane_keys"] == ["feature", "router"]
    assert first["installed_count"] == 2
    assert unrelated.read_text(encoding="utf-8") == "preserve unrelated lane"
    assert (codex_home / "skills" / "verify-aistock-feature" / "SKILL.md").exists()
    assert (codex_home / "skills" / "aistock-task-router" / "SKILL.md").exists()

    second = workflow.build_client_install_plan(
        apply=True,
        codex_home=str(codex_home),
        install_claude=False,
        selected_lane="feature",
    )

    assert second["installed_count"] == 0
    assert second["skipped_current_count"] == 2
    assert second["single_owner_required"] is False
    assert unrelated.read_text(encoding="utf-8") == "preserve unrelated lane"


def test_verify_clients_uses_merged_authority_instead_of_older_task_worktree(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    authority_root = isolated_workflow_root / "canonical"
    _write_repo_client_entrypoints(authority_root)
    for key, skill_name in workflow.CLIENT_CODEX_SKILLS:
        (isolated_workflow_root / ".codex" / "skills" / skill_name / "SKILL.md").write_text(
            f"older worktree {key}", encoding="utf-8"
        )
        (authority_root / ".codex" / "skills" / skill_name / "SKILL.md").write_text(
            f"merged authority {key}", encoding="utf-8"
        )
    authority = {
        "ready": True,
        "source": "canonical_main",
        "root": str(authority_root),
        "commit": "a" * 40,
        "origin_main_commit": "a" * 40,
        "blocking_reason": None,
    }
    monkeypatch.setattr(workflow, "_client_source_authority", lambda: authority)
    monkeypatch.setattr(workflow, "_client_checkout_relation", lambda _authority: "behind_authority")
    codex_home = isolated_workflow_root / "authority_codex_home"

    installed = workflow.build_client_install_plan(
        apply=True,
        codex_home=str(codex_home),
        install_claude=False,
        selected_lane="feature",
    )

    assert installed["workflow_gate"] == "installed"
    assert installed["source_authority"] == authority
    assert installed["task_worktree_is_install_source"] is False
    assert (codex_home / "skills" / "verify-aistock-feature" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "merged authority feature"

    result = workflow.main(
        [
            "verify-clients",
            "--workflow-only",
            "--selected-lane",
            "feature",
            "--codex-home",
            str(codex_home),
            "--skip-claude",
            "--output-format",
            "full-json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    feature = payload["client_manifest"]["codex_entries"]["feature"]
    assert result == 0
    assert payload["workflow_gate"] == "ready"
    assert payload["blocking"] == []
    assert payload["remediation"]["action"] == "continue_without_install"
    assert payload["remediation"]["owner_command"] is None
    assert str(authority_root / "scripts" / "aistock_issue_workflow.py") in payload["remediation"][
        "window_verify_command"
    ]
    assert payload["client_manifest"]["checkout_commit_relation"] == "behind_authority"
    assert payload["client_manifest"]["codex_feature_skill_sha256"] == feature["authority_sha256"]
    assert feature["status"] == "current"
    assert feature["checkout_status"] == "differs_from_authority"
    assert any("do not install from this task worktree" in item for item in payload["checkout_advisories"])


def test_install_client_blocks_when_merged_authority_is_unavailable(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    authority = {
        "ready": False,
        "source": "canonical_main",
        "root": str(isolated_workflow_root),
        "commit": "a" * 40,
        "origin_main_commit": "b" * 40,
        "blocking_reason": "canonical main is not aligned with origin/main",
    }
    monkeypatch.setattr(workflow, "_client_source_authority", lambda: authority)

    payload = workflow.build_client_install_plan(
        codex_home=str(isolated_workflow_root / "blocked_codex_home"),
        install_claude=False,
        selected_lane="feature",
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["task_worktree_is_install_source"] is False
    assert payload["blocking"] == [
        "merged client authority is unavailable: canonical main is not aligned with origin/main"
    ]


def test_install_client_rechecks_authority_identity_under_lock(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo_client_entrypoints(isolated_workflow_root)
    first = {
        "ready": True,
        "source": "canonical_main",
        "root": str(isolated_workflow_root),
        "commit": "a" * 40,
        "origin_main_commit": "a" * 40,
        "blocking_reason": None,
    }
    changed = {**first, "commit": "b" * 40, "origin_main_commit": "b" * 40}
    authorities = iter([first, first, changed])
    monkeypatch.setattr(workflow, "_client_source_authority", lambda: next(authorities))

    with pytest.raises(workflow.WorkflowError, match="authority changed before install"):
        workflow.build_client_install_plan(
            apply=True,
            codex_home=str(isolated_workflow_root / "racing_codex_home"),
            install_claude=False,
            selected_lane="feature",
        )


@pytest.mark.parametrize(
    ("snapshot", "head", "origin_main", "authority_status", "expected_reason"),
    [
        (
            {"ok": False, "branch": None, "dirty": False},
            "",
            None,
            {"ok": False, "stdout": ""},
            "not a readable Git checkout",
        ),
        (
            {"ok": True, "branch": "main", "dirty": True},
            "a" * 40,
            "a" * 40,
            {"ok": True, "stdout": " M .codex/skills/aistock-task-router/SKILL.md"},
            "canonical client-authority paths are dirty",
        ),
        (
            {"ok": True, "branch": "main", "dirty": False},
            "a" * 40,
            "b" * 40,
            {"ok": True, "stdout": ""},
            "canonical main is not aligned with origin/main",
        ),
    ],
)
def test_client_source_authority_fails_closed_without_clean_aligned_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    snapshot: dict[str, object],
    head: str,
    origin_main: str | None,
    authority_status: dict[str, object],
    expected_reason: str,
) -> None:
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(workflow, "_git_snapshot", lambda _root: snapshot)
    def fake_run(command: list[str], **_kwargs: object) -> dict[str, object]:
        if "status" in command:
            return authority_status
        return {"ok": bool(head), "stdout": head}

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_origin_main_commit", lambda **_kwargs: origin_main)

    authority = workflow._client_source_authority()

    assert authority["ready"] is False
    assert expected_reason in authority["blocking_reason"]


def test_client_source_authority_ignores_unrelated_canonical_dirty_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commit = "a" * 40
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda _root: {"ok": True, "branch": "main", "dirty": True},
    )

    def fake_run(command: list[str], **_kwargs: object) -> dict[str, object]:
        if "status" in command:
            return {"ok": True, "stdout": ""}
        return {"ok": True, "stdout": commit}

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_origin_main_commit", lambda **_kwargs: commit)

    authority = workflow._client_source_authority()

    assert authority["ready"] is True
    assert authority["authority_paths_clean"] is True


def test_explicit_linked_issue_reservation_uses_direct_lookup_without_global_scan(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_scan_github_bug_ids",
        lambda **_kwargs: pytest.fail("global GitHub Issue scan must not run for explicit BUG and Issue identity"),
    )
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_by_number",
        lambda issue_number: (
            {
                "bug_id": "BUG-952",
                "number": 952,
                "kind": "github_issue",
                "source": "https://github.com/licong01-cloud/AIstock/issues/3041",
                "github_issue_number": int(issue_number),
                "github_state": "OPEN",
                "title": "BUG-952 P1: Client workflow gate",
                "labels": [],
            },
            [],
        ),
    )

    bug_id, number, report, reservation = workflow._reserve_bug_id(
        isolated_workflow_root,
        bug_id="BUG-952",
        include_github=True,
        github_required=True,
        allowed_github_issue_number=3041,
    )
    try:
        assert bug_id == "BUG-952"
        assert number == 952
        assert report["github_scanned"] is False
        assert reservation.exists()
    finally:
        workflow._release_bug_id_reservation(reservation)


def test_explicit_linked_issue_resumes_matching_incomplete_reservation(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation = isolated_workflow_root / "bug-id-reservations" / "BUG-952.json"
    _write_json(
        reservation,
        {
            "schema_version": "aistock_bug_id_reservation_v1",
            "bug_id": "BUG-952",
            "status": "github_issue_confirmed_local_incomplete",
            "github_issue_number": 3041,
            "title": "Client workflow gate",
            "fingerprint": "fp-952",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_by_number",
        lambda issue_number: (
            {
                "bug_id": "BUG-952",
                "number": 952,
                "kind": "github_issue",
                "source": "https://github.com/licong01-cloud/AIstock/issues/3041",
                "github_issue_number": int(issue_number),
                "github_state": "OPEN",
                "title": "BUG-952 P1: Client workflow gate",
                "labels": [],
            },
            [],
        ),
    )

    bug_id, number, _report, reused = workflow._reserve_bug_id(
        isolated_workflow_root,
        bug_id="BUG-952",
        include_github=True,
        github_required=True,
        allowed_github_issue_number=3041,
        reservation_title="Client workflow gate",
        reservation_fingerprint="fp-952",
    )

    assert bug_id == "BUG-952"
    assert number == 952
    assert reused == reservation
    assert json.loads(reservation.read_text(encoding="utf-8"))["status"] == "reserved"


def test_github_issue_create_recovers_exact_issue_after_transport_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = tmp_path / "body.md"
    body.write_text("body", encoding="utf-8")
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *_args, **_kwargs: {"ok": False, "stdout": "", "stderr": "TLS handshake timeout"},
    )
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_for_id",
        lambda bug_id: (
            {
                "bug_id": bug_id,
                "github_issue_number": 3246,
                "source": "https://github.example/issues/3246",
                "title": "BUG-1009 P1: Retry-safe allocator",
            },
            [],
        ),
    )

    result = workflow._create_github_issue_with_recovery(
        bug_id="BUG-1009",
        title="BUG-1009 P1: Retry-safe allocator",
        body_path=body,
        labels=["bug"],
        cwd=tmp_path,
    )

    assert result["number"] == 3246
    assert result["recovered_after_transport_error"] is True


def test_github_issue_create_plain_eof_preserves_unknown_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = tmp_path / "body.md"
    body.write_text("body", encoding="utf-8")
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *_args, **_kwargs: {
            "ok": False,
            "stdout": "",
            "stderr": 'Post "https://api.github.com/graphql": EOF',
        },
    )
    monkeypatch.setattr(workflow, "_github_bug_issue_for_id", lambda _bug_id: (None, []))

    with pytest.raises(workflow.GitHubOutcomeUnknownError, match="automatic recreate blocked"):
        workflow._create_github_issue_with_recovery(
            bug_id="BUG-1009",
            title="BUG-1009 P1: Retry-safe allocator",
            body_path=body,
            labels=["bug"],
            cwd=tmp_path,
        )


def test_submit_bug_plain_eof_keeps_reservation_for_safe_retry(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 132})
    monkeypatch.setattr(
        workflow,
        "_validate_registry_apply_target",
        lambda root: {"blocking": [], "warnings": [], "target_root": str(root)},
    )
    monkeypatch.setattr(workflow, "_github_bug_issue_for_id", lambda _bug_id, **_kwargs: (None, []))
    monkeypatch.setattr(
        workflow,
        "_create_github_issue_with_recovery",
        lambda **_kwargs: (_ for _ in ()).throw(
            workflow.GitHubOutcomeUnknownError(
                'Post "https://api.github.com/graphql": EOF; reservation preserved and automatic recreate blocked'
            )
        ),
    )

    with pytest.raises(workflow.GitHubOutcomeUnknownError, match="automatic recreate blocked"):
        workflow.build_submit_bug_plan(
            title="Preserve EOF reservation",
            module="validation",
            severity="P1",
            description="GitHub may have accepted the Issue create.",
            expected="Retry cannot create a duplicate Issue.",
            actual="The create response ended with EOF.",
            reproduce_command="n/a",
            evidence_refs=[],
            changed_files=["scripts/aistock_issue_workflow.py"],
            plan_key=None,
            nox_session=None,
            candidate_type="bug",
            bug_id=None,
            github_issue_number=None,
            github_issue_url=None,
            create_github=True,
            apply=True,
            create_registry_worktree=False,
            registry_pr_only=False,
            dry_run=False,
        )

    reservation = isolated_workflow_root / "bug-id-reservations" / "BUG-133.json"
    payload = json.loads(reservation.read_text(encoding="utf-8"))
    assert payload["status"] == "github_create_outcome_unknown"
    assert payload["last_error_type"] == "GitHubOutcomeUnknownError"


def test_github_issue_create_does_not_retry_non_transport_label_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = tmp_path / "body.md"
    body.write_text("body", encoding="utf-8")
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *_args, **_kwargs: {"ok": False, "stdout": "", "stderr": "could not add label: module:missing"},
    )
    monkeypatch.setattr(
        workflow,
        "_github_bug_issue_for_id",
        lambda _bug_id: pytest.fail("non-transport errors must not trigger remote recovery lookup"),
    )

    with pytest.raises(workflow.WorkflowError, match="could not add label"):
        workflow._create_github_issue_with_recovery(
            bug_id="BUG-1009",
            title="BUG-1009 P1: Missing label",
            body_path=body,
            labels=["module:missing"],
            cwd=tmp_path,
        )


def test_global_allocator_lock_reclaims_dead_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = tmp_path / "bug-id-allocator.lock"
    lock_path.write_text("999999\n2026-01-01T00:00:00Z\n", encoding="ascii")
    monkeypatch.setenv("AISTOCK_BUG_ID_LOCK_PATH", str(lock_path))
    monkeypatch.setattr(workflow, "_process_id_is_alive", lambda _pid: False)

    with workflow._GlobalBugIdAllocatorLock(timeout=0.1):
        assert lock_path.exists()
        owner = json.loads(lock_path.read_text(encoding="utf-8"))
        assert owner["schema_version"] == "aistock_bug_id_lock_v2"
        assert owner["pid"] == os.getpid()
        assert owner["thread_id"]
        assert owner["token"]

    assert not lock_path.exists()


def test_global_allocator_lock_cleans_failed_owner_write(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = isolated_workflow_root / "global-allocator.lock"
    monkeypatch.setenv("AISTOCK_BUG_ID_LOCK_PATH", str(lock_path))
    monkeypatch.setattr(workflow.os, "write", lambda _fd, _payload: 0)

    with pytest.raises(workflow.WorkflowError, match="failed to initialize BUG id allocator lock"):
        with workflow._GlobalBugIdAllocatorLock(timeout=0.1):
            pass

    assert not lock_path.exists()


def test_reserve_bug_id_blocks_matching_active_fingerprint(
    isolated_workflow_root: Path,
) -> None:
    reservation = isolated_workflow_root / "bug-id-reservations" / "BUG-133.json"
    _write_json(
        reservation,
        {
            "schema_version": "aistock_bug_id_reservation_v1",
            "bug_id": "BUG-133",
            "status": "reserved",
            "title": "Concurrent logical bug",
            "fingerprint": "same-fingerprint",
        },
    )

    with pytest.raises(workflow.WorkflowError, match="resume the existing intake"):
        workflow._reserve_bug_id(
            isolated_workflow_root,
            bug_id=None,
            include_github=False,
            github_required=False,
            allowed_github_issue_number=None,
            reservation_title="Concurrent logical bug",
            reservation_fingerprint="same-fingerprint",
        )


def test_terminal_reservation_compaction_preserves_unknown_outcome(
    isolated_workflow_root: Path,
) -> None:
    root = isolated_workflow_root / "bug-id-reservations"
    registered = root / "BUG-133.json"
    unknown = root / "BUG-134.json"
    _write_json(registered, {"bug_id": "BUG-133", "status": "registered"})
    _write_json(unknown, {"bug_id": "BUG-134", "status": "github_create_outcome_unknown"})

    assert workflow.compact_terminal_reservations(root, {"BUG-133", "BUG-134"}) == []
    assert registered.exists()

    removed = workflow.compact_terminal_reservations(
        root,
        {"BUG-133", "BUG-134"},
        min_age_seconds=0,
    )

    assert removed == [str(registered)]
    assert not registered.exists()
    assert unknown.exists()


def test_allocator_steady_state_does_not_scan_registries_or_reservation_inventory(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow.write_allocator_state(
        Path(os.environ["AISTOCK_BUG_ID_STATE_PATH"]),
        last_allocated=300,
        updated_at="2026-08-11T00:00:00Z",
        updated_by="pytest",
        fingerprint_index_version=workflow.FINGERPRINT_INDEX_VERSION,
    )
    monkeypatch.setattr(
        workflow,
        "_scan_bug_registry_ids",
        lambda *_args, **_kwargs: pytest.fail("steady-state allocation must not scan BUG registries"),
    )
    monkeypatch.setattr(
        workflow,
        "_scan_bug_id_reservations",
        lambda: pytest.fail("steady-state allocation must not enumerate all reservations"),
    )
    monkeypatch.setattr(
        workflow,
        "_fingerprint_bootstrap_records",
        lambda *_args, **_kwargs: pytest.fail("steady-state allocation must not rebuild the fingerprint index"),
    )

    bug_id, number, report, reservation = workflow._reserve_bug_id(
        isolated_workflow_root,
        bug_id=None,
        include_github=False,
        github_required=False,
        allowed_github_issue_number=None,
        reservation_title="Steady-state allocator",
        reservation_fingerprint="steady-state-fingerprint",
    )
    try:
        assert (bug_id, number) == ("BUG-301", 301)
        assert report["allocator_state_bootstrap_required"] is False
        assert report["allocator_lock"]["wait_ms"] is not None
        assert report["allocator_lock"]["hold_ms"] is not None
    finally:
        workflow._release_bug_id_reservation(reservation)


def test_allocator_missing_state_bootstraps_once_then_uses_direct_state(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_json(
        workflow.BUGS_ROOT / ".bug_id_allocator.json",
        {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 140},
    )
    calls = {"registry": 0, "fingerprints": 0}
    original_registry_scan = workflow._scan_bug_registry_ids
    original_fingerprint_records = workflow._fingerprint_bootstrap_records

    def registry_scan(*args: object, **kwargs: object):
        calls["registry"] += 1
        return original_registry_scan(*args, **kwargs)

    def fingerprint_records(*args: object, **kwargs: object):
        calls["fingerprints"] += 1
        return original_fingerprint_records(*args, **kwargs)

    monkeypatch.setattr(workflow, "_scan_bug_registry_ids", registry_scan)
    monkeypatch.setattr(workflow, "_fingerprint_bootstrap_records", fingerprint_records)

    first = workflow._reserve_bug_id(
        isolated_workflow_root,
        bug_id=None,
        include_github=False,
        github_required=False,
        allowed_github_issue_number=None,
        reservation_title="Bootstrap once",
        reservation_fingerprint="bootstrap-once-1",
    )
    workflow._release_bug_id_reservation(first[3])
    assert calls == {"registry": 1, "fingerprints": 1}

    second = workflow._reserve_bug_id(
        isolated_workflow_root,
        bug_id=None,
        include_github=False,
        github_required=False,
        allowed_github_issue_number=None,
        reservation_title="No second bootstrap",
        reservation_fingerprint="bootstrap-once-2",
    )
    try:
        assert second[0] == "BUG-142"
        assert calls == {"registry": 1, "fingerprints": 1}
    finally:
        workflow._release_bug_id_reservation(second[3])


def test_legacy_reservation_compaction_only_removes_durable_statusless_record(
    isolated_workflow_root: Path,
) -> None:
    root = isolated_workflow_root / "bug-id-reservations"
    durable = root / "BUG-150.json"
    unmatched = root / "BUG-151.json"
    unknown = root / "BUG-152.json"
    _write_json(durable, {"bug_id": "BUG-150"})
    _write_json(unmatched, {"bug_id": "BUG-151"})
    _write_json(unknown, {"bug_id": "BUG-152", "status": "github_create_outcome_unknown"})

    removed = workflow.compact_terminal_reservations(
        root,
        {"BUG-150", "BUG-152"},
        min_age_seconds=0,
    )

    assert removed == [str(durable)]
    assert not durable.exists()
    assert unmatched.exists()
    assert unknown.exists()


def test_workflow_policy_sources_are_compact_and_semantically_consistent() -> None:
    root = workflow.REPO_ROOT
    standard_path = root / "docs/standards/aistock_development_standard_v1.5_20260523.md"
    catalog_path = root / "docs/standards/aistock_development_standard_v1.5_20260523.yaml"
    quickstart_path = root / "docs/standards/aistock_issue_workflow_quickstart.md"
    standard = standard_path.read_text(encoding="utf-8")
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    rules = {item["rule_id"]: item for item in catalog["rules"]}

    assert "必须使用 `rtk git`" in standard
    assert "不把 `doctor` 作为通用前置门禁" in standard
    assert "同一条用户指令可以明确打包源码合入" in standard
    assert "禁止再次索要同一授权" in standard
    assert "裸 `merge` 授权仍只覆盖源码合入" in standard
    assert rules["TOOL-RTK-001"]["effect"] == "warn"
    assert rules["TOOL-RTK-001"]["failure_policy"] == "non_blocking_visible_warning"
    assert rules["ISSUE-BATCH-CONTEXT-001"]["effect"] == "advisory"
    assert rules["SCRIPT-LOCATION-001"]["effect"] == "warn"
    assert rules["DOC-LOCATION-001"]["effect"] == "warn"
    ddl_evidence = rules["PROD-DDL-001"]["checker"]["required_evidence"]
    assert "authorization_may_be_bundled_with_merge_without_second_prompt" in ddl_evidence
    assert "immutable_merge_commit_confirmed_before_production_apply" in ddl_evidence

    lane_paths = [
        ".codex/skills/aistock-task-router/SKILL.md",
        ".codex/skills/fix-aistock-issue/SKILL.md",
        ".codex/skills/aistock-merge-aftercare/SKILL.md",
        ".codex/skills/aistock-readonly-triage/SKILL.md",
        ".codex/skills/aistock-docs-handoff/SKILL.md",
        ".codex/skills/aistock-validation-delegation/SKILL.md",
        ".codex/skills/verify-aistock-feature/SKILL.md",
        ".claude/commands/aistock-task-router.md",
        ".claude/commands/fix-aistock-issue.md",
        ".claude/commands/aistock-merge-aftercare.md",
        ".claude/commands/aistock-readonly-triage.md",
        ".claude/commands/aistock-docs-handoff.md",
        ".claude/commands/aistock-validation-delegation.md",
        ".claude/commands/aistock-feature-workflow.md",
    ]
    for relative in lane_paths:
        text = (root / relative).read_text(encoding="utf-8")
        assert "TOOL-RTK-001" in text, relative
        assert "must use RTK" in text, relative
        assert "never make RTK or telemetry a task/PR/CI gate" in text, relative

    for relative in (
        ".codex/skills/aistock-task-router/SKILL.md",
        ".codex/skills/fix-aistock-issue/SKILL.md",
        ".claude/commands/aistock-task-router.md",
        ".claude/commands/fix-aistock-issue.md",
    ):
        text = (root / relative).read_text(encoding="utf-8")
        assert "doctor` before repository mutation" not in text
        assert "Run `python scripts/aistock_issue_workflow.py doctor`" not in text

    for relative in (
        ".codex/skills/aistock-merge-aftercare/SKILL.md",
        ".claude/commands/aistock-merge-aftercare.md",
    ):
        text = (root / relative).read_text(encoding="utf-8")
        assert "Authorizations are action-scoped, not message-scoped" in text
        assert "do not ask for the same authorization a second time" in text
        assert "Bare merge authorization" in text
        assert "exact named cleanup targets" in text
        assert "confirm the immutable merge commit first" in text

    quickstart = quickstart_path.read_text(encoding="utf-8")
    assert len(quickstart.splitlines()) <= 130
    assert "普通任务直接使用" in quickstart
    assert "same explicit merge authorization requires" not in quickstart
    assert "full aftercare loop unless" not in quickstart
    assert "授权按动作和目标独立，但可以在一条用户指令中打包" in quickstart
    assert "不再二次询问" in quickstart

    agents = (root / "AGENTS.md").read_text(encoding="utf-8")
    assert len(agents.splitlines()) <= 80
    assert "Architecture Overview" not in agents
    assert "TOOL-RTK-001" in agents
    assert "docs/standards/aistock_development_standard_v1.5_20260523.md" in agents
    assert "Authorizations are action-scoped, not message-scoped" in agents
    assert "A complete bundle needs no second prompt after merge" in agents

    project_memory = (root / "docs/codex_project_memory.md").read_text(encoding="utf-8")
    assert "Active Multi-Alpha Strategy Evolution Snapshot" not in project_memory
    assert "Advisory Research Program Working Memory" not in project_memory
    assert "never add snapshots here" in project_memory
    assert "Authorization is action-scoped rather than message-scoped" in project_memory
    assert "with no second prompt after merge" in project_memory

    ownership = (root / "tests/aistock_validation/catalog/file_ownership.yaml").read_text(encoding="utf-8")
    assert ".claude/commands/aistock-issue-doctor.md" in ownership
    assert "docs/standards/aistock_development_standard_v1.5_20260523.md" in workflow.WORKFLOW_RULE_DIGEST_REFS
    assert "docs/standards/aistock_issue_workflow_quickstart.md" not in workflow.WORKFLOW_RULE_DIGEST_REFS


def test_rdagent_release_aftercare_is_part_of_every_client_and_resume_digest() -> None:
    assert ("merge_aftercare", "aistock-merge-aftercare") in workflow.CLIENT_CODEX_SKILLS
    assert ("merge_aftercare", "aistock-merge-aftercare.md") in workflow.CLIENT_CLAUDE_COMMANDS
    assert ".codex/skills/aistock-merge-aftercare/SKILL.md" in workflow.WORKFLOW_RULE_DIGEST_REFS
    assert ".claude/commands/aistock-merge-aftercare.md" in workflow.WORKFLOW_RULE_DIGEST_REFS

    codex_text = (workflow.REPO_ROOT / ".codex/skills/aistock-merge-aftercare/SKILL.md").read_text(
        encoding="utf-8"
    )
    claude_text = (workflow.REPO_ROOT / ".claude/commands/aistock-merge-aftercare.md").read_text(
        encoding="utf-8"
    )
    router_text = (workflow.REPO_ROOT / ".codex/skills/aistock-task-router/SKILL.md").read_text(
        encoding="utf-8"
    )
    for text in (codex_text, claude_text):
        assert "RDAGENT_STATE_ROOT" in text
        assert "blocked_not_implemented" in text
        assert "deployment receipt" in text.lower()
        assert "Copy-Item" in text
        assert "runtime_verified" in text
    assert "RD-Agent release/deploy/rollback" in router_text


def test_verification_budget_does_not_require_nightly_without_deferred_plans() -> None:
    budget = workflow._verification_budget_for_record(
        {
            "title": "Workflow validation issue",
            "description": "P1 workflow regression",
            "module": "validation",
            "severity": "P1",
            "required_verification": ["l0"],
        }
    )

    assert budget["deferred_nightly_verification"] == {
        "required": False,
        "modules": [],
        "plans": [],
        "scope": "deduplicate all merged BUG/PR changes for the day and run deep UI/API/business-flow validation once in nightly or delegated VC/CI runs",
    }
    catalog_budget = workflow._verification_budget_for_record(
        {
            "title": "Standards catalog synchronization",
            "description": "Validate the source digest and machine catalog.",
            "module": "validation",
            "severity": "P1",
            "required_verification": ["l0", "validation_catalog_integrity"],
        }
    )
    assert catalog_budget["premerge_required_plans"] == ["l0", "validation_catalog_integrity"]
    assert catalog_budget["deferred_nightly_verification"]["required"] is False


def test_verification_budget_does_not_treat_ddl_policy_wording_as_schema_work() -> None:
    policy_budget = workflow._verification_budget_for_record(
        {
            "title": "Clarify merge and DDL authorization policy",
            "description": "Workflow standards discuss production migration authorization without changing a database.",
            "module": "validation",
            "severity": "P1",
            "allowed_write_scope": ["docs/standards/aistock_development_standard_v1.5_20260523.md"],
            "production_ddl_gate": "noop",
            "required_verification": ["l0"],
        }
    )
    migration_budget = workflow._verification_budget_for_record(
        {
            "title": "Apply production DDL migration",
            "description": "Add a committed migration for the production DB.",
            "module": "database",
            "severity": "P2",
            "allowed_write_scope": ["backend/db/migrations/20260811_add_index.sql"],
            "production_ddl_gate": "pending",
            "required_verification": ["l0"],
        }
    )

    assert policy_budget["budget"] == "standard"
    assert migration_budget["budget"] == "deep"


def test_verification_budget_requires_nightly_for_explicit_broad_plan() -> None:
    budget = workflow._verification_budget_for_record(
        {
            "title": "Validation Center regression",
            "description": "Cross-module API flow needs delegated coverage",
            "module": "validation",
            "severity": "P1",
            "required_verification": ["l0", "validation_center_backend"],
        }
    )

    assert budget["deferred_nightly_verification"]["required"] is True
    assert budget["deferred_nightly_verification"]["plans"] == ["validation_center_backend"]
    assert budget["deferred_nightly_verification"]["modules"] == ["validation", "validation_center"]


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
    digest = resume["context_resume_digest"]
    assert digest["schema_version"] == "aistock_workflow_context_resume_digest_v1"
    assert "standards README" in digest["reuse_policy"][1]
    assert digest["exploration_command_budget"]["soft_limit"] == 40
    assert "pytest --lf -q" in digest["validation_loop_budget"]["failure_resume_first"]
    assert digest["validation_loop_budget"]["max_final_related_matrix_runs"] == 1
    assert "do not rerun broad suites" in digest["validation_loop_budget"]["rule"]
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
            "code_intelligence": {
                "status": "ready",
                "context_ref": "tmp/issue_workflow/BUG-199/codegraph-context.md",
                "affected_tests_ref": "tmp/issue_workflow/BUG-199/affected-tests.json",
                "understand_anything_summary_ref": "tmp/issue_workflow/BUG-199/ua-validation-summary.md",
                "fallback_used": False,
                "affected_tests_count": 2,
                "affected_tests": {"suggested_tests": ["backend/tests/scripts/test_issue_flow.py"]},
                "understand_anything": {"status": "ready", "graph_exists": True},
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
    assert payload["code_intelligence_efficiency"]["broad_scan_avoided"] is True
    assert payload["code_intelligence_efficiency"]["estimated_broad_scan_tokens_avoided"] == 8000
    assert payload["code_intelligence_efficiency"]["full_graph_payload_included"] is False
    assert payload["flow_overhead_estimate"]["code_intelligence_broad_scan_avoided"] is True
    assert payload["duplicate_active_count"] == 1
    postmortem_md = isolated_workflow_root / payload["postmortem_md_path"]
    assert (isolated_workflow_root / payload["postmortem_json_path"]).exists()
    assert postmortem_md.exists()
    md_text = postmortem_md.read_text(encoding="utf-8")
    assert "## H6 Cost Summary" in md_text
    assert "## H7 Code Intelligence" in md_text
    assert "broad_scan_avoided" in md_text


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
    assert payload["task_card_availability"]["available"] is False
    assert payload["validation_receipt_summary"]["broad_premerge_detected"] is False
    assert payload["code_intelligence_efficiency"]["broad_scan_avoided"] is False
    assert "postmortem_json_path" not in payload
    assert "postmortem_md_path" not in payload
    assert not (workflow_root / "postmortem.json").exists()
    assert not (workflow_root / "postmortem.md").exists()



def test_postmortem_prefers_fix_workflow_over_close_sync_state(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fix = isolated_workflow_root / "fix-worktree"
    close_sync = isolated_workflow_root / "BUG-199-close-sync"
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
            "context_metrics": {"task_card_md": {"exists": True, "estimated_tokens": 11}},
        },
    )
    _write_json(
        close_sync / "tmp" / "issue_workflow" / "BUG-199" / "state.json",
        {
            "schema_version": "aistock_issue_workflow_state_v1",
            "bug_id": "BUG-199",
            "state": "complete",
            "worktree": str(close_sync),
            "branch": "chore/BUG-199-close-sync",
            "pr_url": "https://github.example/pull/200",
        },
    )
    monkeypatch.setattr(workflow, "_state_roots_for_bug", lambda bug_id: [fix, close_sync])
    monkeypatch.setattr(workflow, "_active_workflows_for_bug", lambda bug_id: [])
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda bug_id: {"status": "checked", "open_prs": [], "merged_prs": []})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199")

    assert payload["workflow_root"] == str(fix)
    assert payload["state"]["branch"] == "bug/BUG-199-fix"
    assert payload["h6_summary"]["context_estimated_tokens"] == 11


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
    monkeypatch.setattr(
        workflow,
        "_merged_commit_changed_files",
        lambda _commit: ["scripts/aistock_issue_workflow.py"],
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
    monkeypatch.setattr(
        workflow,
        "_merged_commit_changed_files",
        lambda _commit: ["scripts/aistock_issue_workflow.py"],
    )

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
    updated = json.loads(issue.read_text(encoding="utf-8"))
    assert updated["closed_at"]
    assert updated["fixed_at"]



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


def test_merge_uses_cleanup_owned_branch_deletion(
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
                            {"name": "CI verdict", "status": "COMPLETED", "conclusion": "SUCCESS"},
                            {"name": "advisory", "status": "COMPLETED", "conclusion": "FAILURE"},
                        ],
                    }
                ),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "checks"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps(
                    [{"name": "CI verdict", "state": "SUCCESS", "bucket": "pass", "workflow": "AIstock CI"}]
                ),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "merge"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
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

    assert payload["merge_result"]["ok"] is True
    assert payload["verified"]["merged"] is True
    merge_commands = [args for args in commands if args[:3] == ["gh", "pr", "merge"]]
    assert merge_commands == [["gh", "pr", "merge", "https://github.example/pull/199", "--squash"]]


def test_generic_merge_helper_uses_cleanup_owned_branch_deletion(
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
                            {"name": "CI verdict", "status": "COMPLETED", "conclusion": "SUCCESS"},
                            {"name": "advisory", "status": "IN_PROGRESS", "conclusion": ""},
                        ],
                    }
                ),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "checks"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps(
                    [{"name": "CI verdict", "state": "SUCCESS", "bucket": "pass", "workflow": "AIstock CI"}]
                ),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "merge"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
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

    assert payload["merge_result"]["ok"] is True
    assert payload["verified"]["merged"] is True
    merge_commands = [args for args in commands if args[:3] == ["gh", "pr", "merge"]]
    assert merge_commands == [["gh", "pr", "merge", "https://github.example/pull/199", "--squash"]]


def test_generic_merge_helper_blocks_failed_required_check(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "pr", "view"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps({"state": "OPEN", "statusCheckRollup": []}),
                "stderr": "",
            }
        if args[:3] == ["gh", "pr", "checks"]:
            return {
                "ok": False,
                "returncode": 1,
                "stdout": json.dumps(
                    [{"name": "CI verdict", "state": "FAILURE", "bucket": "fail", "workflow": "AIstock CI"}]
                ),
                "stderr": "required checks failed",
            }
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)

    with pytest.raises(workflow.WorkflowError, match="CI verdict"):
        workflow._merge_pr_if_ready("https://github.example/pull/199")

    assert not any(args[:3] == ["gh", "pr", "merge"] for args in commands)


def test_generic_merge_helper_short_circuits_required_checks_for_merged_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_execute(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        assert args[:3] == ["gh", "pr", "view"]
        return {
            "ok": True,
            "returncode": 0,
            "stdout": json.dumps({"state": "MERGED", "statusCheckRollup": []}),
            "stderr": "",
        }

    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)

    payload = workflow._merge_pr_if_ready("https://github.example/pull/199")

    assert payload["already_merged"] is True
    assert commands == [
        [
            "gh",
            "pr",
            "view",
            "https://github.example/pull/199",
            "--json",
            "state,mergeStateStatus,statusCheckRollup,url",
        ]
    ]


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
        _bug(
            bug_id="BUG-199",
            github_issue_number=199,
            github_issue_url="https://github.example/issues/199",
            validation_evidence=["BUG-199 historical evidence", "python -m nox -s l0 -> passed"],
        ),
    )
    bug_b = _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug200.json",
        _bug(
            bug_id="BUG-200",
            github_issue_number=200,
            github_issue_url="https://github.example/issues/200",
            validation_evidence=["BUG-200 historical evidence"],
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url, skip_github_check=False: {
            "checked": True,
            "merged": True,
            "pr": {"mergeCommit": {"oid": "merge123"}, "mergedAt": "2026-07-17T21:00:19Z"},
        },
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
    updated_a = json.loads(bug_a.read_text(encoding="utf-8"))
    assert updated_a["status"] == "fixed"
    assert updated_a["validation_evidence"] == [
        "BUG-199 historical evidence",
        "python -m nox -s l0 -> passed",
    ]
    updated_b = json.loads(bug_b.read_text(encoding="utf-8"))
    assert updated_b["pr_url"] == "https://github.example/pull/299"
    assert updated_b["closed_at"] == "2026-07-17T21:00:19Z"
    assert updated_b["fixed_at"]
    assert updated_b["validation_evidence"] == [
        "BUG-200 historical evidence",
        "python -m nox -s l0 -> passed",
    ]
    assert set(payload["github_issue_sync"]) == {"BUG-199", "BUG-200"}


def test_close_sync_batch_rejects_runtime_bugs_without_per_issue_receipts(
    isolated_workflow_root: Path,
) -> None:
    _write_runtime_catalog(isolated_workflow_root)
    _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        _runtime_bug(isolated_workflow_root),
    )
    _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug200.json",
        _runtime_bug(
            isolated_workflow_root,
            bug_id="BUG-200",
            github_issue_number=200,
            github_issue_url="https://github.example/issues/200",
        ),
    )

    with pytest.raises(workflow.WorkflowError, match="cannot close runtime BUGs"):
        workflow.build_close_sync_batch_plan(
            bug_ids=["BUG-199", "BUG-200"],
            pr_url="https://github.example/pull/299",
            apply=False,
            allow_missing_linkage=False,
            validation_evidence=["python -m nox -s l0 -> passed"],
        )


def test_close_sync_batch_rejects_any_invalid_runtime_contract(
    isolated_workflow_root: Path,
) -> None:
    invalid = _bug(
        allowed_write_scope=[".codex/skills/fix-aistock-issue/SKILL.md"],
        runtime_contract={
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "runtime_impact": "none",
            "persistence_basis": "not_required",
        },
    )
    _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug199.json",
        invalid,
    )
    _write_json(
        isolated_workflow_root / "tests" / "aistock_validation" / "bugs" / "bug200.json",
        _bug(bug_id="BUG-200", github_issue_number=200, github_issue_url="https://github.example/issues/200"),
    )

    with pytest.raises(workflow.WorkflowError, match="runtime contracts block close-sync-batch"):
        workflow.build_close_sync_batch_plan(
            bug_ids=["BUG-199", "BUG-200"],
            pr_url="https://github.example/pull/299",
            apply=False,
            allow_missing_linkage=False,
            validation_evidence=["python -m nox -s l0 -> passed"],
        )


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
    body = (registry / workflow.WORKFLOW_ROOT / "BUG-199-BUG-200" / "close-sync-pr-body.md").read_text(encoding="utf-8")
    assert "BUG-199=fixed" in body
    assert "BUG-200=fixed" in body


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


def test_merge_finalizer_plan_preserves_bundled_cleanup_and_gate_actions() -> None:
    payload = workflow.build_merge_finalizer_plan(
        bug_id=["BUG-266", "BUG-267"],
        issue_json="F:/Dev/task/bug.json",
        source_pr_url="https://github.example/pull/266",
        source_branch="bug/BUG-266-workflow",
        source_worktree="F:/Dev/task worktree",
        validation_evidence=["rtk nox -s l0 -> passed"],
        allow_missing_linkage=True,
        production_gates={
            "production_ddl_gate": "pending_authorized_apply",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
        sync_root=True,
        merge_close_sync_pr=True,
        cleanup=True,
        apply=False,
    )

    assert payload["workflow_gate"] == "ready_for_apply"
    command = payload["next_command"]
    assert command.count("--bug-id") == 2
    assert '--source-branch "bug/BUG-266-workflow"' in command
    assert '--source-worktree "F:/Dev/task worktree"' in command
    assert '--validation-evidence "rtk nox -s l0 -> passed"' in command
    assert "--allow-missing-linkage" in command
    assert "--sync-root" in command
    assert "--merge-close-sync-pr" in command
    assert "--cleanup" in command
    assert '--production-ddl-gate "pending_authorized_apply"' in command
    assert command.endswith("--apply")


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


def test_merge_finalizer_apply_cleans_source_before_close_sync_pr_merge(
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

    def fake_cleanup(**kwargs: Any) -> dict[str, Any]:
        cleanup_calls.append(kwargs)
        return {
            "workflow_gate": "cleanup_done" if kwargs.get("apply") else "ready_for_cleanup",
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
        merge_close_sync_pr=False,
        cleanup=True,
        apply=True,
    )

    assert payload["workflow_gate"] == "close_sync_persisted"
    assert payload["close_sync_pr_merge"]["workflow_gate"] == "ready_for_merge"
    assert payload["cleanup"]["workflow_gate"] == "cleanup_done"
    assert payload["close_sync_cleanup"] is None
    assert cleanup_calls == [
        {
            "branch": "bug/BUG-199-workflow",
            "bug_id": "BUG-199",
            "worktree": str(isolated_workflow_root / "task"),
            "pr_url": "https://github.example/pull/199",
            "apply": True,
            "sync_root": True,
        }
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
    assert payload["close_sync_cleanup"]["workflow_gate"] == "cleanup_done"
    assert payload["close_sync_cleanup"]["sync_root"] is True
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


def test_merge_finalizer_defers_dirty_root_sync_without_blocking_cleanup(
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
        if kwargs.get("sync_root") and kwargs.get("apply"):
            raise workflow.WorkflowError(
                f"canonical root is dirty and not synced to origin/main: {isolated_workflow_root}"
            )
        if kwargs.get("sync_root"):
            return {
                "workflow_gate": "blocked",
                "branch": kwargs["branch"],
                "worktree": kwargs.get("worktree"),
                "sync_root": True,
                "canonical_root": str(isolated_workflow_root),
                "root_dirty_files": ["CLAUDE.md"],
                "unrelated_root_dirty_files": ["CLAUDE.md"],
                "origin_equivalent_dirty_files": [],
                "root_git": {"branch": "main", "dirty": True, "head": "old", "origin_main": "new"},
                "blocking": [f"canonical root is dirty and not synced to origin/main: {isolated_workflow_root}"],
            }
        return {
            "workflow_gate": "cleanup_done",
            "branch": kwargs["branch"],
            "worktree": kwargs.get("worktree"),
            "sync_root": False,
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
    assert payload["blocking"] == []
    assert payload["cleanup"]["workflow_gate"] == "cleanup_done"
    assert payload["cleanup"]["sync_root"] is False
    assert payload["close_sync_cleanup"]["workflow_gate"] == "cleanup_done"
    assert payload["close_sync_cleanup"]["sync_root"] is False
    assert payload["root_sync_deferred"]["workflow_gate"] == "deferred"
    assert payload["root_sync_deferred"]["unrelated_root_dirty_files"] == ["CLAUDE.md"]
    assert [item["phase"] for item in payload["root_sync_deferred"]["phases"]] == [
        "source_cleanup",
        "close_sync_cleanup",
    ]
    assert "sync_root_after_unrelated_dirty_files_are_resolved" in payload["next_actions"]
    assert [(call["sync_root"], call["apply"]) for call in cleanup_calls] == [
        (True, True),
        (True, False),
        (False, True),
        (True, True),
        (True, False),
        (False, True),
    ]


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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _write_json(
        isolated_workflow_root / "bug.json",
        _bug(
            status="fixed",
            validation_evidence=[
                "historical targeted test -> passed",
                "python -m nox -s l0 -> passed",
            ],
        ),
    )

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

    monkeypatch.setattr(
        workflow,
        "_merged_commit_changed_files",
        lambda _commit: ["scripts/aistock_issue_workflow.py"],
    )

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
    assert updated["validation_evidence"] == [
        "historical targeted test -> passed",
        "python -m nox -s l0 -> passed",
    ]


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
    monkeypatch.setattr(
        workflow,
        "_merged_commit_changed_files",
        lambda _commit: ["scripts/aistock_issue_workflow.py"],
    )

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
        "_run_read_command_with_retry",
        lambda args, **kwargs: {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "attempts": 1},
    )
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


def test_close_sync_worktree_fast_forwards_clean_stale_reuse(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "worktrees" / "BUG-199-close-sync"
    registry.mkdir(parents=True)
    calls: list[tuple[list[str], Path | None]] = []
    snapshots = iter(
        [
            {
                "ok": True,
                "branch": "chore/BUG-199-close-sync",
                "dirty": False,
                "dirty_count": 0,
                "head": "old",
                "origin_main": "new",
            },
            {
                "ok": True,
                "branch": "chore/BUG-199-close-sync",
                "dirty": False,
                "dirty_count": 0,
                "head": "new",
                "origin_main": "new",
            },
        ]
    )

    monkeypatch.setattr(
        workflow,
        "_close_sync_worktree_names",
        lambda bug_id: ("chore/BUG-199-close-sync", registry),
    )
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: next(snapshots))
    monkeypatch.setattr(
        workflow,
        "_git",
        lambda args, cwd=None, check=True: calls.append((args, cwd)) or "",
    )
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda args, cwd=None, **kwargs: {
            "ok": args[-2:] == ["HEAD", "origin/main"],
            "returncode": 0 if args[-2:] == ["HEAD", "origin/main"] else 1,
            "stdout": "",
            "stderr": "",
        },
    )

    payload = workflow._maybe_create_close_sync_worktree(
        bug_id="BUG-199",
        create=True,
        dry_run=False,
    )

    assert payload["reused"] is True
    assert payload["fast_forwarded"] is True
    assert (["merge", "--ff-only", "origin/main"], registry) in calls


def test_close_sync_worktree_reuses_clean_ahead_branch_after_push_failure(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = isolated_workflow_root / "worktrees" / "BUG-199-close-sync"
    registry.mkdir(parents=True)
    git_calls: list[list[str]] = []
    monkeypatch.setattr(
        workflow,
        "_close_sync_worktree_names",
        lambda bug_id: ("chore/BUG-199-close-sync", registry),
    )
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {
            "ok": True,
            "branch": "chore/BUG-199-close-sync",
            "dirty": False,
            "dirty_count": 0,
            "head": "task-commit",
            "origin_main": "main-commit",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda args, cwd=None, **kwargs: {
            "ok": args[-2:] == ["origin/main", "HEAD"],
            "returncode": 0 if args[-2:] == ["origin/main", "HEAD"] else 1,
            "stdout": "",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        workflow,
        "_git",
        lambda args, cwd=None, check=True: git_calls.append(args) or "",
    )

    payload = workflow._maybe_create_close_sync_worktree(
        bug_id="BUG-199",
        create=True,
        dry_run=False,
    )

    assert payload["reused"] is True
    assert payload["ahead_with_task_commits"] is True
    assert not any(args[:2] == ["merge", "--ff-only"] for args in git_calls)


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
        "_run_read_command_with_retry",
        lambda args, **kwargs: {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "attempts": 1},
    )
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


def test_worktree_transient_artifact_profile_and_purge_are_manifest_bound(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    temporary = worktree / "tmp" / "issue_workflow" / "BUG-199" / "context.json"
    cache = worktree / "frontend" / "node_modules" / "pkg" / "index.js"
    local_config = worktree / "proxy_config.json"
    canonical_config = isolated_workflow_root / "proxy_config.json"
    for path in (temporary, cache, local_config, canonical_config):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("same" if path.name == "proxy_config.json" else "artifact", encoding="utf-8")

    def fake_run(args: list[str], cwd: Path | None = None, **_kwargs: Any) -> dict[str, Any]:
        if args[:3] == ["git", "ls-files", "--others"]:
            paths = [
                rel
                for rel in (
                    "tmp/issue_workflow/BUG-199/context.json",
                    "frontend/node_modules/pkg/index.js",
                    "proxy_config.json",
                )
                if (worktree / rel).exists()
            ]
            return {"ok": True, "returncode": 0, "stdout": "\0".join(paths), "stderr": ""}
        if args[:3] == ["git", "ls-files", "-z"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    profile = workflow._worktree_ignored_artifact_profile(
        worktree,
        canonical_root=isolated_workflow_root,
    )

    assert profile["ignored_count"] == 3
    assert profile["transient_count"] == 3
    assert profile["protected_count"] == 0
    assert profile["unknown_count"] == 0
    assert set(profile["transient_roots"]) == {"tmp/issue_workflow", "frontend/node_modules", "proxy_config.json"}

    purge = workflow._purge_worktree_transient_artifacts(
        worktree,
        canonical_root=isolated_workflow_root,
        expected_profile=profile,
    )

    assert purge["ignored_count_before"] == 3
    assert purge["ignored_count_after"] == 0
    assert not temporary.exists()
    assert not cache.exists()
    assert not local_config.exists()
    assert canonical_config.exists()


def test_worktree_qe_live_log_ring_is_structurally_validated_and_purged(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    relative_paths = sorted(workflow.WORKTREE_QE_LIVE_LOG_PATHS)
    for index, relative_path in enumerate(relative_paths):
        target = worktree / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            {"schema_version": workflow.WORKTREE_QE_LIVE_LOG_SCHEMA, "slot": index}
            if index in {0, 4}
            else None
        )
        target.write_text(json.dumps(payload) + "\n" if payload else "", encoding="utf-8")

    def fake_run(args: list[str], cwd: Path | None = None, **_kwargs: Any) -> dict[str, Any]:
        if args[:3] == ["git", "ls-files", "--others"]:
            existing = [item for item in relative_paths if (worktree / item).exists()]
            return {"ok": True, "returncode": 0, "stdout": "\0".join(existing), "stderr": ""}
        if args[:3] == ["git", "ls-files", "-z"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    profile = workflow._worktree_ignored_artifact_profile(worktree, canonical_root=isolated_workflow_root)

    assert profile["ignored_count"] == 5
    assert profile["transient_count"] == 5
    assert profile["unknown_count"] == 0
    assert profile["transient_roots"] == [workflow.WORKTREE_QE_LIVE_LOG_ROOT]

    purge = workflow._purge_worktree_transient_artifacts(
        worktree,
        canonical_root=isolated_workflow_root,
        expected_profile=profile,
    )

    assert purge["ignored_count_before"] == 5
    assert purge["ignored_count_after"] == 0
    assert not (worktree / workflow.WORKTREE_QE_LIVE_LOG_ROOT).exists()


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("missing_slot", "qe_live_log_ring_inventory_mismatch"),
        ("extra_file", "qe_live_log_ring_inventory_mismatch"),
        ("wrong_schema", "qe_live_log_ring_schema_mismatch"),
        ("invalid_json", "qe_live_log_ring_invalid_jsonl"),
        ("oversized", "qe_live_log_ring_file_too_large"),
    ],
)
def test_worktree_qe_live_log_ring_remains_unknown_when_contract_is_not_exact(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    expected_reason: str,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    relative_paths = sorted(workflow.WORKTREE_QE_LIVE_LOG_PATHS)
    for relative_path in relative_paths:
        target = worktree / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps({"schema_version": workflow.WORKTREE_QE_LIVE_LOG_SCHEMA}) + "\n",
            encoding="utf-8",
        )

    if mutation == "missing_slot":
        (worktree / relative_paths[-1]).unlink()
    elif mutation == "extra_file":
        extra = worktree / workflow.WORKTREE_QE_LIVE_LOG_ROOT / "qe-live-5.jsonl"
        extra.write_text(json.dumps({"schema_version": workflow.WORKTREE_QE_LIVE_LOG_SCHEMA}) + "\n", encoding="utf-8")
    elif mutation == "wrong_schema":
        (worktree / relative_paths[0]).write_text('{"schema_version":"unexpected"}\n', encoding="utf-8")
    elif mutation == "invalid_json":
        (worktree / relative_paths[0]).write_text("not-json\n", encoding="utf-8")
    elif mutation == "oversized":
        monkeypatch.setattr(workflow, "WORKTREE_QE_LIVE_LOG_MAX_FILE_BYTES", 1)

    def fake_run(args: list[str], cwd: Path | None = None, **_kwargs: Any) -> dict[str, Any]:
        if args[:3] == ["git", "ls-files", "--others"]:
            root = worktree / workflow.WORKTREE_QE_LIVE_LOG_ROOT
            existing = sorted(path.relative_to(worktree).as_posix() for path in root.iterdir() if path.is_file())
            return {"ok": True, "returncode": 0, "stdout": "\0".join(existing), "stderr": ""}
        if args[:3] == ["git", "ls-files", "-z"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    profile = workflow._worktree_ignored_artifact_profile(worktree, canonical_root=isolated_workflow_root)

    assert profile["transient_count"] == 0
    assert profile["unknown_count"] >= 1
    assert profile["transient_roots"] == []
    assert {item["reason"] for item in profile["unknown_samples"]} == {expected_reason}


def test_worktree_qe_live_log_ring_does_not_widen_other_rdagent_assets(
    isolated_workflow_root: Path,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    worktree.mkdir(parents=True)

    root, reason = workflow._worktree_transient_root(
        "rdagent_assets/model.bin",
        worktree_path=worktree,
        canonical_root=isolated_workflow_root,
    )

    assert root is None
    assert reason == "unknown_ignored_artifact"


def test_worktree_transient_purge_stops_on_manifest_drift(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    first = worktree / "tmp" / "validation" / "first.json"
    second = worktree / "tmp" / "validation" / "second.json"
    first.parent.mkdir(parents=True)
    first.write_text("first", encoding="utf-8")

    def fake_run(args: list[str], cwd: Path | None = None, **_kwargs: Any) -> dict[str, Any]:
        if args[:3] == ["git", "ls-files", "--others"]:
            paths = [rel for rel in ("tmp/validation/first.json", "tmp/validation/second.json") if (worktree / rel).exists()]
            return {"ok": True, "returncode": 0, "stdout": "\0".join(paths), "stderr": ""}
        if args[:3] == ["git", "ls-files", "-z"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    profile = workflow._worktree_ignored_artifact_profile(worktree, canonical_root=isolated_workflow_root)
    second.write_text("second", encoding="utf-8")

    with pytest.raises(workflow.WorkflowError, match="manifest changed"):
        workflow._purge_worktree_transient_artifacts(
            worktree,
            canonical_root=isolated_workflow_root,
            expected_profile=profile,
        )
    assert first.exists()
    assert second.exists()


def test_worktree_ignored_artifact_profile_blocks_unknown_and_unfinalized_receipt(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    worktree.mkdir(parents=True)

    def fake_run(args: list[str], cwd: Path | None = None, **_kwargs: Any) -> dict[str, Any]:
        if args[:3] == ["git", "ls-files", "--others"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "notes/local-only.txt\0tmp/issue_workflow/BUG-199/post-restart-verify.json",
                "stderr": "",
            }
        if args[:3] == ["git", "ls-files", "-z"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    profile = workflow._worktree_ignored_artifact_profile(
        worktree,
        canonical_root=isolated_workflow_root,
        protected_paths={"tmp/issue_workflow/BUG-199/post-restart-verify.json"},
    )

    assert profile["unknown_count"] == 1
    assert profile["unknown_samples"] == [
        {"path": "notes/local-only.txt", "reason": "unknown_ignored_artifact"}
    ]
    assert profile["protected_count"] == 1
    assert profile["protected_samples"] == ["tmp/issue_workflow/BUG-199/post-restart-verify.json"]


def test_worktree_transient_classifier_rejects_parent_escape(
    isolated_workflow_root: Path,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    worktree.mkdir(parents=True)

    root, reason = workflow._worktree_transient_root(
        "../tmp/issue_workflow/escape.json",
        worktree_path=worktree,
        canonical_root=isolated_workflow_root,
    )

    assert root is None
    assert reason == "invalid_path"

    nested_root, nested_reason = workflow._worktree_transient_root(
        "tmp/../outside.txt",
        worktree_path=worktree,
        canonical_root=isolated_workflow_root,
    )
    assert nested_root is None
    assert nested_reason == "invalid_path"


def test_worktree_transient_cleanup_unlinks_external_symlink_without_following(
    isolated_workflow_root: Path,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    external = isolated_workflow_root / "outside-cache"
    link = worktree / "tmp" / "external-cache"
    external.mkdir(parents=True)
    (external / "keep.txt").write_text("keep", encoding="utf-8")
    link.parent.mkdir(parents=True)
    try:
        link.symlink_to(external, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink unavailable: {exc}")

    workflow._remove_exact_transient_root(worktree, "tmp/external-cache")

    assert not link.exists()
    assert (external / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_worktree_transient_cleanup_rejects_intermediate_reparse_before_deleting(
    isolated_workflow_root: Path,
) -> None:
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    external = isolated_workflow_root / "outside-cache"
    link = worktree / "tmp"
    external.mkdir(parents=True)
    (external / "keep.txt").write_text("keep", encoding="utf-8")
    worktree.mkdir(parents=True)
    try:
        link.symlink_to(external, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink unavailable: {exc}")

    with pytest.raises(workflow.WorkflowError, match="crosses reparse point"):
        workflow._validated_transient_root_target(worktree, "tmp/keep.txt")
    assert (external / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_cleanup_evidence_finalization_requires_structured_receipt(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = isolated_workflow_root / "bug.json"
    monkeypatch.setattr(
        workflow,
        "find_bug_record",
        lambda **_kwargs: (
            {
                "bug_id": "BUG-199",
                "validation_evidence": [
                    {
                        "schema_version": "aistock_validation_receipt_v1",
                        "receipt_id": "0123456789abcdef",
                        "commit": "abc1234",
                        "evidence_kind": "pytest",
                        "status": "passed",
                        "command": "pytest backend/tests/test_example.py -q",
                        "result": "1 passed",
                    }
                ],
            },
            issue,
        ),
    )

    finalized = workflow._cleanup_evidence_finalization("BUG-199")
    assert finalized["status"] == "finalized_structured_receipt"
    assert finalized["durable_receipt_present"] is True

    monkeypatch.setattr(
        workflow,
        "find_bug_record",
        lambda **_kwargs: ({"bug_id": "BUG-199", "validation_evidence": ["pytest -> passed"]}, issue),
    )
    missing = workflow._cleanup_evidence_finalization("BUG-199")
    assert missing["status"] == "missing_durable_receipt"
    assert missing["durable_receipt_present"] is False

    monkeypatch.setattr(
        workflow,
        "find_bug_record",
        lambda **_kwargs: (
            {
                "bug_id": "BUG-199",
                "status": "fixed",
                "fix_commit": "abc1234",
                "pr_url": "https://github.example/pull/199",
                "validation_evidence": ["legacy targeted test -> passed"],
            },
            issue,
        ),
    )
    legacy = workflow._cleanup_evidence_finalization("BUG-199")
    assert legacy["status"] == "finalized_legacy_closed_bug"
    assert legacy["legacy_closure_present"] is True


def test_merged_pr_validation_receipt_profile_is_compact(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = (
        "Validation\n"
        "validation-receipt: id=0123456789abcdef commit=abcdef1 kind=pytest "
        "plan=focused status=passed command=`pytest backend/tests/test_example.py -q` "
        "result=`1 passed`"
    )
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *args, **kwargs: {
            "ok": True,
            "returncode": 0,
            "stdout": json.dumps(
                {
                    "state": "MERGED",
                    "mergedAt": "2026-08-12T00:00:00Z",
                    "url": "https://github.example/pull/199",
                    "headRefOid": "abcdef1" + "0" * 33,
                    "body": body,
                }
            ),
            "stderr": "",
        },
    )

    profile = workflow._merged_pr_validation_receipt_profile("https://github.example/pull/199")

    assert profile["status"] == "finalized_merged_pr_receipt"
    assert profile["durable_receipt_present"] is True
    assert profile["receipt_commit"] == "abcdef1"
    assert "body" not in profile


def test_merged_pr_validation_receipt_rejects_stale_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = (
        "validation-receipt: id=0123456789abcdef commit=abcdef1 kind=pytest "
        "plan=focused status=passed command=`pytest backend/tests/test_example.py -q` result=`1 passed`"
    )
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *args, **kwargs: {
            "ok": True,
            "returncode": 0,
            "stdout": json.dumps(
                {
                    "state": "MERGED",
                    "mergedAt": "2026-08-12T00:00:00Z",
                    "url": "https://github.example/pull/199",
                    "headRefOid": "1234567" + "0" * 33,
                    "body": body,
                }
            ),
            "stderr": "",
        },
    )

    profile = workflow._merged_pr_validation_receipt_profile("https://github.example/pull/199")

    assert profile["status"] == "receipt_commit_mismatch"
    assert profile["durable_receipt_present"] is False
    assert profile["receipt_commit"] is None


def test_cleanup_uses_merged_pr_receipt_before_close_sync(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    artifact = worktree / "tmp" / "validation" / "focused.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("receipt detail", encoding="utf-8")

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

    def fake_run(args: list[str], cwd: Path | None = None, **_kwargs: Any) -> dict[str, Any]:
        if args[:2] == ["git", "status"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        if args[:3] == ["git", "ls-files", "--others"]:
            return {
                "ok": True,
                "returncode": 0,
                "stdout": "tmp/validation/focused.json",
                "stderr": "",
            }
        if args[:3] == ["git", "ls-files", "-z"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_registered_worktree_paths", lambda cwd=None: {worktree.resolve()})
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(
        workflow,
        "_cleanup_evidence_finalization",
        lambda bug_id: {
            "schema_version": "aistock_cleanup_evidence_finalization_v1",
            "status": "missing_durable_receipt",
            "durable_receipt_present": False,
        },
    )
    monkeypatch.setattr(workflow, "_cleanup_protected_receipt_paths", lambda bug_id: set())
    monkeypatch.setattr(
        workflow,
        "_merged_pr_validation_receipt_profile",
        lambda pr_url: {
            "schema_version": "aistock_merged_pr_validation_receipt_v1",
            "pr_url": pr_url,
            "checked": True,
            "merged": True,
            "durable_receipt_present": True,
            "status": "finalized_merged_pr_receipt",
        },
    )

    payload = workflow.build_cleanup_after_merge_plan(
        branch=branch,
        worktree=str(worktree),
        pr_url="https://github.example/pull/199",
        canonical_root=str(isolated_workflow_root),
    )

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["evidence_finalization"]["status"] == "finalized_merged_pr_receipt"
    assert payload["evidence_finalization"]["durable_receipt_present"] is True


def test_cleanup_protects_absolute_runtime_receipt_until_durable_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    receipt_ref = "/tmp/pytest-run/tmp/issue_workflow/BUG-199/post-restart-verify.json"
    record = {
        "bug_id": "BUG-199",
        "runtime_contract": {"post_restart_receipt_ref": receipt_ref},
    }
    monkeypatch.setattr(workflow, "find_bug_record", lambda **_kwargs: (record, tmp_path / "bug.json"))

    assert workflow._cleanup_protected_receipt_paths("BUG-199") == {
        "tmp/issue_workflow/BUG-199/post-restart-verify.json"
    }

    record["runtime_contract"]["post_restart_receipt_summary"] = {
        "schema_version": workflow.RUNTIME_VERIFY_RECEIPT_SUMMARY_SCHEMA,
        "receipt_sha256": "a" * 64,
        "expected_identity": "abc1234",
        "observed_identity": "abc1234",
        "runtime_identity_proof_digest": "b" * 64,
        "contract_digest": "c" * 64,
        "catalog_sha256": "d" * 64,
        "probe_evidence_digest": "e" * 64,
        "post_restart_effective_gate": "passed",
        "response_content_persisted": False,
    }
    assert workflow._cleanup_protected_receipt_paths("BUG-199") == set()


def test_cleanup_after_merge_blocks_active_process_reference(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    worktree = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    worktree.mkdir(parents=True)

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

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *args, **kwargs: {"ok": True, "returncode": 0, "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(workflow, "_registered_worktree_paths", lambda cwd=None: {worktree.resolve()})
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"ok": True, "branch": "main", "dirty": False, "dirty_count": 0, "head": "a", "origin_main": "a"},
    )
    monkeypatch.setattr(workflow, "_cleanup_preflight_fetch_origin", lambda root, apply: _fetched_origin_payload())
    monkeypatch.setattr(
        workflow,
        "_worktree_active_process_profile",
        lambda path: {
            "schema_version": "aistock_worktree_process_reference_v1",
            "scan_status": "complete",
            "reference_count": 1,
            "references": [{"ProcessId": 123, "Name": "python.exe"}],
        },
    )

    with pytest.raises(workflow.WorkflowError, match="active processes"):
        workflow.build_cleanup_after_merge_plan(
            branch=branch,
            worktree=str(worktree),
            apply=True,
            canonical_root=str(isolated_workflow_root),
        )


def test_remote_branch_delete_is_sha_lease_bound(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    sha = "a" * 40
    remote_ref = f"{sha}\trefs/heads/{branch}"
    executed: list[list[str]] = []
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: remote_ref)
    monkeypatch.setattr(
        workflow,
        "_execute_checked",
        lambda args, **kwargs: executed.append(args) or {"ok": True, "returncode": 0, "stdout": "", "stderr": ""},
    )

    result = workflow._delete_remote_branch_with_lease(
        root=isolated_workflow_root,
        branch=branch,
        expected_remote_ref=remote_ref,
    )

    assert result["expected_sha"] == sha
    assert executed == [
        [
            "git",
            "push",
            "origin",
            "--delete",
            f"--force-with-lease=refs/heads/{branch}:{sha}",
            branch,
        ]
    ]


def test_remote_branch_delete_stops_on_sha_drift(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    expected = f"{'a' * 40}\trefs/heads/{branch}"
    observed = f"{'b' * 40}\trefs/heads/{branch}"
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: observed)
    monkeypatch.setattr(
        workflow,
        "_execute_checked",
        lambda *args, **kwargs: pytest.fail("remote delete must not execute after SHA drift"),
    )

    with pytest.raises(workflow.WorkflowError, match="changed after cleanup preflight"):
        workflow._delete_remote_branch_with_lease(
            root=isolated_workflow_root,
            branch=branch,
            expected_remote_ref=expected,
        )


def test_remote_branch_delete_treats_concurrent_absence_as_idempotent(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    expected = f"{'a' * 40}\trefs/heads/{branch}"
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        workflow,
        "_execute_checked",
        lambda *args, **kwargs: pytest.fail("already absent branch must not be deleted again"),
    )

    result = workflow._delete_remote_branch_with_lease(
        root=isolated_workflow_root,
        branch=branch,
        expected_remote_ref=expected,
    )

    assert result == {"expected_sha": "a" * 40, "already_absent": True}


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
    branch_deleted = False

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return "" if branch_deleted else branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_execute(args: list[str], **kwargs: Any) -> dict[str, Any]:
        nonlocal branch_deleted
        executed.append(args)
        if args[:2] == ["git", "branch"]:
            branch_deleted = True
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
    branch_deleted = False

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        calls.append((tuple(args), cwd))
        if args[:2] == ["branch", "--show-current"]:
            return "main" if cwd == isolated_workflow_root else branch
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return "" if branch_deleted else branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_run(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        if args[:3] == ["git", "worktree", "remove"]:
            task_worktree.rmdir()
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    def fake_execute(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
        nonlocal branch_deleted
        executed.append((tuple(args), cwd))
        if args[:2] == ["git", "branch"]:
            branch_deleted = True
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
    branch_deleted = False

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "feature/current"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return "" if branch_deleted else branch
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
    def fake_execute(args: list[str], **_kwargs: Any) -> dict[str, Any]:
        nonlocal branch_deleted
        if args[:2] == ["git", "branch"]:
            branch_deleted = True
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)
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

    assert payload["workflow_gate"] == "cleanup_deferred"
    assert payload["deferred_cleanup"]["reason"] == "empty_directory_locked_by_windows_handle"
    assert payload["deferred_cleanup"]["safe_to_retry"] is True
    assert payload["cleanup_verification"]["path_absent"] is False
    assert payload["cleanup_verification"]["all_clear"] is False
    assert any("deferred empty worktree directory cleanup" in item for item in payload["warnings"])


def test_orphan_worktree_profile_and_refusal_message_stay_compact(
    isolated_workflow_root: Path,
) -> None:
    orphan = isolated_workflow_root / "worktrees" / "BUG-199-workflow"
    for index in range(60):
        path = orphan / "frontend" / "node_modules" / "pkg" / f"file-{index}.js"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
    deep_tail = orphan / "tests" / "aistock_validation" / "history" / "very-deep-tail.json"
    deep_tail.parent.mkdir(parents=True, exist_ok=True)
    deep_tail.write_text("{}", encoding="utf-8")

    profile = workflow._orphan_worktree_dir_profile(orphan)

    assert profile["regular_entry_count"] == 61
    assert len(profile["regular_entries"]) <= profile["sample_limit"]
    assert profile["regular_entries_truncated"] is True
    assert profile["top_regular_dirs"]["frontend"] == 60
    assert profile["safe_reparse_or_empty_only"] is False
    assert "very-deep-tail.json" not in json.dumps(profile, ensure_ascii=False)

    with pytest.raises(workflow.WorkflowError) as excinfo:
        workflow._remove_reparse_or_empty_tree(orphan)

    message = str(excinfo.value)
    assert "count=61" in message
    assert "top_dirs=" in message
    assert "full file list intentionally omitted" in message
    assert "very-deep-tail.json" not in message


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
    branch_deleted = False

    def fake_git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main" if cwd == isolated_workflow_root else branch
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return "" if branch_deleted else branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        if args[:2] == ["ls-remote", "--heads"]:
            return ""
        return ""

    def fake_run(args: list[str], cwd: Path | None = None, **kwargs: Any) -> dict[str, Any]:
        nonlocal branch_deleted
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        if args[:3] == ["git", "worktree", "remove"]:
            return {"ok": False, "returncode": 128, "stdout": "", "stderr": "Invalid argument: frontend/node_modules"}
        if args[:2] == ["git", "branch"]:
            branch_deleted = True
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(
        workflow,
        "_registered_worktree_paths",
        lambda cwd=None: {worktree.resolve()} if worktree.exists() else set(),
    )
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
    assert "AIstock Issue / Feature / CI-CD" in design
    assert "v2.0" in design
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


def test_triage_ci_issue_marks_superseded_same_branch_success(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = {
        "number": 853,
        "title": "P1 Nightly failed: runner=success dr=success/success l3=success live=failure code=success",
        "state": "OPEN",
        "url": "https://github.com/licong01-cloud/AIstock/issues/853",
        "body": "<!-- aistock-issue-on-test-fail:27185588632 -->",
        "labels": [],
    }
    summary = {
        "schema_version": "aistock_ci_failure_summary_v1",
        "diagnostic_status": "complete",
        "severity": "P1",
        "workflow": "AIstock Nightly L3 + DR",
        "run_id": "27185588632",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/27185588632",
        "branch": "feat/github-models-deepseek-r1-20260609",
        "commit": "030771c80f19e331093971a453949db9b742f3c0",
        "failed_jobs": [
            {
                "job_name": "AIstock Nightly status",
                "nox_session": "paper_v2_live",
                "failed_tests": [],
                "error_signature": "Nightly failed: paper_v2_live=failure",
                "key_log_excerpt": ["paper_v2_live: failure"],
                "suspected_module": "paper_v2",
                "suspected_files": [],
            }
        ],
        "suspected_modules": ["paper_v2"],
        "suspected_files": [],
        "fingerprint": "ci-07b8e8ccdbb3d76a",
        "reproduce_command": "python -m nox -s paper_v2_live",
    }

    monkeypatch.setattr(workflow, "_load_github_issue", lambda issue_number: issue)
    monkeypatch.setattr(workflow, "_find_bug_by_github_issue", lambda issue_number: None)
    monkeypatch.setattr(workflow.ci_failure_summary, "summarize_actions_run", lambda **kwargs: summary)
    monkeypatch.setattr(
        workflow,
        "_find_superseding_main_success",
        lambda summary: {
            "run_id": "27188538840",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/27188538840",
            "head_sha": "760cd2d26b445e2cbf2afe63380aa1d89b7dccef",
            "created_at": "2026-06-09T06:37:28Z",
            "branch": "feat/github-models-deepseek-r1-20260609",
            "supersede_scope": "same_branch",
        },
    )

    payload = workflow.build_triage_ci_issue_plan(issue_number=853)

    assert payload["classification_recommendation"] == "superseded_by_later_branch_success"
    assert payload["needs_bug_json"] is False
    assert payload["superseded_action"]["workflow_gate"] == "superseded_by_latest_branch_success"
    assert "same branch feat/github-models-deepseek-r1-20260609" in payload["next_command"]
    assert payload["failure_event"]["candidate_status"] == "superseded_by_later_branch_success"
    assert "promote" not in payload["context_pack"]["agent_handoff"]["workflow_entrypoints"]


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
    label_synced: list[int | str] = []

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
    monkeypatch.setattr(
        workflow,
        "_sync_closed_auto_filed_issue_labels",
        lambda issue_number: label_synced.append(issue_number) or {"ok": True, "returncode": 0},
    )

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[642, 559, 548], apply=True)

    assert payload["workflow_gate"] == "closed"
    assert payload["superseded_count"] == 1
    assert payload["closed_issues"] == [642]
    assert payload["skipped_count"] == 2
    assert closed == [642]
    assert label_synced == []


def test_ci_issue_janitor_superseded_only_leaves_infra_for_manual_ops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed: list[int | str] = []

    def fake_triage(issue_number: int | str, **kwargs: Any) -> dict[str, Any]:
        issue = int(issue_number)
        if issue == 642:
            return {
                "classification_recommendation": "superseded_by_later_main_success",
                "linked_bug": None,
                "github_issue": {"number": issue, "state": "OPEN"},
                "summary": {"workflow": "AIstock Nightly L3 + DR"},
                "superseded_action": {
                    "workflow_gate": "superseded_by_latest_main_success",
                    "superseding_run": {"run_id": "26899001365", "run_url": "https://github.example/runs/26899001365"},
                },
            }
        return {
            "classification_recommendation": "infra_blocker",
            "needs_bug_json": False,
            "linked_bug": None,
            "github_issue": {"number": issue, "state": "OPEN"},
            "summary": {"workflow": "AIstock Nightly L3 + DR"},
            "infra_action": {
                "workflow_gate": "infra_action_required",
                "reason": "Runner unavailable.",
                "next_actions": ["restore runner"],
            },
        }

    def fake_close(issue_number: int | str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        closed.append(issue_number)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", fake_triage)
    monkeypatch.setattr(workflow, "_close_superseded_ci_issue", fake_close)

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[642, 683], apply=True, close_infra=False)

    assert payload["workflow_gate"] == "closed"
    assert payload["close_infra"] is False
    assert payload["superseded_count"] == 1
    assert payload["infra_count"] == 0
    assert payload["closed_issues"] == [642]
    assert payload["issues"][1]["action"] == "skip"
    assert payload["issues"][1]["reason"] == "infra_closure_disabled"
    assert closed == [642]


def test_sync_closed_auto_filed_issue_labels_removes_status_open(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_execute(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "issue", "view"]:
            return {
                "ok": True,
                "stdout": json.dumps(
                    {
                        "state": "CLOSED",
                        "labels": [{"name": "auto-filed"}, {"name": "ci"}, {"name": "status:open"}],
                    }
                ),
            }
        if args[:3] == ["gh", "issue", "edit"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)

    result = workflow._sync_closed_auto_filed_issue_labels(853)

    assert result["ok"] is True
    assert any(command[:3] == ["gh", "issue", "edit"] and "--remove-label" in command for command in commands)


def test_sync_closed_issue_status_labels_marks_generic_bug_fixed(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_execute(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "issue", "view"]:
            return {
                "ok": True,
                "stdout": json.dumps(
                    {
                        "state": "CLOSED",
                        "labels": [{"name": "aistock:bug"}, {"name": "status:open"}],
                    }
                ),
            }
        if args[:3] == ["gh", "issue", "edit"]:
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)

    result = workflow._sync_closed_issue_status_labels(931)

    assert result["ok"] is True
    edit = [command for command in commands if command[:3] == ["gh", "issue", "edit"]][0]
    assert edit[edit.index("--remove-label") + 1] == "status:open"
    assert edit[edit.index("--add-label") + 1] == "status:fixed"


def test_sync_closed_auto_filed_issue_labels_skips_non_auto_or_open(monkeypatch: pytest.MonkeyPatch) -> None:
    edit_called = False

    def fake_execute(args: list[str], **kwargs: Any) -> dict[str, Any]:
        nonlocal edit_called
        if args[:3] == ["gh", "issue", "view"]:
            return {"ok": True, "stdout": json.dumps({"state": "OPEN", "labels": [{"name": "auto-filed"}]})}
        if args[:3] == ["gh", "issue", "edit"]:
            edit_called = True
        return {"ok": True, "returncode": 0}

    monkeypatch.setattr(workflow, "_execute_checked", fake_execute)

    result = workflow._sync_closed_auto_filed_issue_labels(853)

    assert result["ok"] is True
    assert result["skipped"] is True
    assert edit_called is False


def test_ci_issue_janitor_closes_superseded_same_branch_issue(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[int | str] = []

    def fake_triage(issue_number: int | str, **kwargs: Any) -> dict[str, Any]:
        return {
            "classification_recommendation": "superseded_by_later_branch_success",
            "linked_bug": None,
            "github_issue": {"number": int(issue_number), "state": "OPEN"},
            "summary": {"workflow": "AIstock Nightly L3 + DR"},
            "superseded_action": {
                "workflow_gate": "superseded_by_latest_branch_success",
                "superseding_run": {
                    "run_id": "27188538840",
                    "run_url": "https://github.example/runs/27188538840",
                    "branch": "feat/github-models-deepseek-r1-20260609",
                    "supersede_scope": "same_branch",
                },
            },
        }

    def fake_close(issue_number: int | str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        closed.append(issue_number)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", fake_triage)
    monkeypatch.setattr(workflow, "_close_superseded_ci_issue", fake_close)

    payload = workflow.build_ci_issue_janitor_plan(issue_numbers=[853], apply=True)

    assert payload["workflow_gate"] == "closed"
    assert payload["superseded_count"] == 1
    assert payload["closed_issues"] == [853]
    assert closed == [853]


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
        workflow,
        "_github_actions_registry_pr_capability",
        lambda: {"allowed": True, "source": "test"},
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


def test_promote_ci_issue_actions_defers_before_bug_allocation_when_registry_pr_is_blocked(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = {
        "linked_bug": None,
        "classification_recommendation": "real_regression_candidate",
        "needs_bug_json": True,
        "summary": {
            "failed_jobs": [{"nox_session": "advisory_historical_range_backend"}],
        },
        "suggested_bug": {
            "title": "Nightly advisory failure",
            "module": "advisory.historical_range",
            "severity": "P1",
            "allowed_write_scope": ["backend/services/advisory_historical_range"],
        },
    }
    monkeypatch.setattr(workflow, "build_triage_ci_issue_plan", lambda **_: triage)
    monkeypatch.setattr(
        workflow,
        "_github_actions_registry_pr_capability",
        lambda: {
            "allowed": False,
            "source": "github_actions_workflow_permissions",
            "reason": "repository Actions cannot create or approve pull requests",
        },
    )
    monkeypatch.setattr(
        workflow,
        "build_submit_bug_plan",
        lambda **_: pytest.fail("BUG allocation must not run before registry PR capability is available"),
    )

    payload = workflow.build_promote_ci_issue_plan(
        issue_number=197,
        apply=True,
        create_registry_worktree=True,
    )

    assert payload["workflow_gate"] == "deferred_registry_pr_capability"
    assert payload["registry_pr_capability"]["allowed"] is False
    assert payload["triage"]["needs_bug_json"] is True
    assert not list((isolated_workflow_root / "tests" / "aistock_validation" / "bugs").glob("*BUG-*.json"))


def test_github_actions_registry_pr_capability_reads_repository_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda *_, **__: {
            "ok": True,
            "stdout": json.dumps(
                {
                    "default_workflow_permissions": "read",
                    "can_approve_pull_request_reviews": False,
                }
            ),
            "stderr": "",
        },
    )

    payload = workflow._github_actions_registry_pr_capability()

    assert payload["allowed"] is False
    assert payload["default_workflow_permissions"] == "read"
    assert payload["can_approve_pull_request_reviews"] is False


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


def test_submit_bug_does_not_infer_ui_hints_from_bug_json_or_design_text(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 522})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="BUG fixes should not read design docs by default",
        module="validation",
        severity="P2",
        description=(
            "Ordinary BUG fixes start from BUG JSON and Context Pack. They should not read "
            "historical/design notes unless the BUG explicitly cites them or the user asks."
        ),
        expected="No visual UI route is inferred from workflow context policy wording.",
        actual="The description contains BUG JSON plus historical/design path-like text.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=[],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id="BUG-523",
        github_issue_number="1627",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/1627",
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=True,
    )

    assert payload["ui_intake_hints"] is None
    assert "ui_intake_hints" not in payload["record"]


def test_submit_bug_does_not_infer_ui_hints_from_cleanup_route_wording(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 578})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="cleanup docs scratch tasks are over-routed into full development workflow",
        module="validation_llm_pipeline",
        severity="P2",
        description=(
            "Root pollution cleanup should move docs/scratch files with cleanup-fast, "
            "not route into a full workflow that productizes scratch scripts."
        ),
        expected="No UI intake or visual validation is inferred from over-routed cleanup wording.",
        actual="The word routed previously matched the UI keyword route.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=[],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id="BUG-579",
        github_issue_number="1834",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/1834",
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=True,
    )

    assert payload["ui_intake_hints"] is None
    assert "ui_intake_hints" not in payload["record"]
    efficiency = payload["record"]["workflow_efficiency_recommendations"]
    assert efficiency["cleanup_fast_candidate"] is True
    assert any("cleanup-fast" in item for item in efficiency["recommendations"])


def test_submit_bug_workflow_budget_skips_nightly_without_explicit_broad_plan(
    isolated_workflow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = workflow.BUGS_ROOT / ".bug_id_allocator.json"
    _write_json(allocator, {"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 581})
    monkeypatch.setattr(workflow, "_validate_registry_apply_target", lambda root: {"blocking": [], "warnings": [], "target_root": str(root)})

    payload = workflow.build_submit_bug_plan(
        title="Issue workflow over-validates bug fixes instead of using risk-based verification budgets",
        module="validation_llm_pipeline",
        severity="P1",
        description="Workflow budget should defer broad UI/API/business-flow validation to nightly.",
        expected="No UI intake is inferred; verification_budget is recorded for compact PR validation.",
        actual="The word issue previously looked like a route and broad validation was encouraged.",
        reproduce_command="n/a",
        evidence_refs=[],
        changed_files=[],
        plan_key=None,
        nox_session=None,
        candidate_type="bug",
        bug_id="BUG-582",
        github_issue_number="1841",
        github_issue_url="https://github.com/licong01-cloud/AIstock/issues/1841",
        create_github=False,
        apply=False,
        create_registry_worktree=False,
        registry_pr_only=False,
        dry_run=True,
    )

    assert payload["ui_intake_hints"] is None
    record = payload["record"]
    assert "ui_intake_hints" not in record
    budget = record["verification_budget"]
    assert budget["budget"] == "standard"
    assert budget["deferred_nightly_verification"]["required"] is False
    assert budget["deferred_nightly_verification"]["modules"] == []
    assert budget["deferred_nightly_verification"]["plans"] == []
    assert budget["delegated_validation"]["receipt_default"] == "compact"
    assert any("nightly" in item for item in record["workflow_efficiency_recommendations"]["recommendations"])


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


def test_postmortem_uses_embedded_pre_cleanup_timing_after_cleanup_state(
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
            "pre_cleanup_postmortem": {
                "artifact_policy": "compact_success_no_artifact",
                "timing_summary": {
                    "event_count": 6,
                    "known_duration_seconds": 51.0,
                    "phases": {"validation_passed": {"event_count": 1, "known_duration_seconds": 9.0}},
                },
                "h6_summary": {"token_usage_status": "unknown"},
            },
        },
    )
    events_path = workflow_dir / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text(
        json.dumps({"timestamp": "2026-06-04T18:00:00Z", "event": "state:complete", "state": "complete"}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(workflow, "_active_workflows_for_bug", lambda bug_id: [])
    monkeypatch.setattr(workflow, "_stale_pr_check_for_bug", lambda bug_id: {"status": "checked", "open_prs": [], "merged_prs": []})

    payload = workflow.build_postmortem_plan(bug_id="BUG-199", worktree=str(isolated_workflow_root), output_markdown=False)

    assert payload["workflow_gate"] == "artifact_fallback"
    assert payload["artifact_fallback"]["reason"] == "pre_cleanup_postmortem_embedded_in_cleanup_state"
    assert payload["timing_summary"]["event_count"] == 6
    assert payload["timing_summary"]["known_duration_seconds"] == 51.0


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
    assert result["label_sync"]["ok"] is True
    comment_path = isolated_workflow_root / result["comment_path"]
    text = comment_path.read_text(encoding="utf-8")
    assert "close-sync persisted to the current registry worktree" in text
    assert "close-sync completed" not in text
    assert "`origin/main`" in text
    assert any(args[:3] == ["gh", "issue", "comment"] for args in calls)
