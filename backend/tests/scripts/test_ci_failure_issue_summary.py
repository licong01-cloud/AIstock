from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import ci_failure_issue_summary as summary


CI_LOG = """
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:38.5513821Z nox > Running session paper_v2_backend
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:38.5521005Z nox > python -m pytest backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package -q -p no:cacheprovider
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:53.0323500Z =================================== FAILURES ===================================
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:53.0330026Z backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py:281: AssertionError
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:53.0331360Z DEBUG: DB connection failed: relation "market.trading_calendar" does not exist
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:53.0333096Z FAILED backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_a_share_trading_window - assert 200 == 409
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:53.0334058Z 1 failed, 472 passed, 1 skipped, 1 deselected in 12.88s
Backend tests (paper_v2_backend)\tUNKNOWN STEP\t2026-05-25T01:43:53.6181164Z nox > Session paper_v2_backend failed.
"""


def test_parse_job_log_extracts_pytest_failure_details() -> None:
    parsed = summary.parse_job_log(CI_LOG, job_name="Backend tests (paper_v2_backend)")

    assert parsed["nox_session"] == "paper_v2_backend"
    assert parsed["pytest_summary"] == "1 failed, 472 passed, 1 skipped, 1 deselected in 12.88s"
    assert parsed["failed_tests"] == [
        "backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_a_share_trading_window"
    ]
    assert parsed["error_signature"] == "assert 200 == 409"
    assert any("market.trading_calendar" in line for line in parsed["key_log_excerpt"])
    assert parsed["key_log_excerpt_omitted_count"] == 0
    assert parsed["suspected_module"] == "paper_v2"


def test_finalize_summary_builds_fingerprint_title_and_reproduce_command() -> None:
    parsed = summary.parse_job_log(CI_LOG, job_name="Backend tests (paper_v2_backend)")
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "26378872481",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
            "branch": "main",
            "commit": "62dc1b12",
            "failed_jobs": [parsed],
            "extraction_errors": [],
        }
    )

    assert payload["diagnostic_status"] == "complete"
    assert payload["fingerprint"].startswith("ci-")
    assert "[P1][paper_v2_backend] main CI failed" in payload["issue_title"]
    assert "test_sentinel_endpoint_rejects_a_share_trading_window" in payload["reproduce_command"]
    assert payload["production_ddl_gate"] == "noop"
    assert payload["failure_event"]["schema_version"] == "aistock_failure_event_v1"
    assert payload["failure_event"]["module_guess"] == "paper_v2"
    assert payload["agent_handoff"]["schema_version"] == "aistock_ci_failure_agent_handoff_v1"
    assert "triage-ci-issue --issue <issue-number>" in payload["agent_handoff"]["next_commands"][0]
    assert "--create-registry-worktree" in payload["agent_handoff"]["workflow_entrypoints"]["promote"]


def test_render_issue_markdown_contains_actionable_sections() -> None:
    parsed = summary.parse_job_log(CI_LOG, job_name="Backend tests (paper_v2_backend)")
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "26378872481",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
            "branch": "main",
            "commit": "62dc1b12",
            "failed_jobs": [parsed],
            "extraction_errors": [],
        }
    )

    markdown = summary.render_issue_markdown(payload)

    assert "## Failure Summary" in markdown
    assert "## Failed Jobs" in markdown
    assert "## Failed Tests / Errors" in markdown
    assert "## Agent Handoff" in markdown
    assert "promote-ci-issue --issue <issue-number> --create-registry-worktree --apply" in markdown
    assert "assert 200 == 409" in markdown
    assert "production_ddl_gate" in markdown


def test_context_pack_is_agent_neutral_and_compact() -> None:
    parsed = summary.parse_job_log(CI_LOG, job_name="Backend tests (paper_v2_backend)")
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "26378872481",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
            "branch": "main",
            "commit": "62dc1b12",
            "failed_jobs": [parsed],
            "extraction_errors": [],
        }
    )

    context_pack = summary.build_context_pack(payload, github_issue_number=197)
    markdown = summary.render_context_pack_markdown(context_pack)

    assert context_pack["schema_version"] == "aistock_ci_failure_context_pack_v1"
    assert context_pack["agent_handoff"]["intended_clients"] == ["Codex", "Claude Code", "Cursor", "generic CLI/IDE agent"]
    assert context_pack["token_budget"]["full_logs_included"] is False
    assert context_pack["github_issue_url"] == "https://github.com/licong01-cloud/AIstock/issues/197"
    assert "promote-ci-issue --issue 197 --create-registry-worktree --apply" in markdown


def test_cli_log_file_outputs_json_and_markdown(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    log_path = tmp_path / "job.log"
    output_path = tmp_path / "summary.json"
    markdown_path = tmp_path / "summary.md"
    context_path = tmp_path / "context-pack.json"
    context_markdown_path = tmp_path / "context-pack.md"
    log_path.write_text(CI_LOG, encoding="utf-8")

    assert summary.main([
        "--log-file",
        str(log_path),
        "--job-name",
        "Backend tests (paper_v2_backend)",
        "--source-name",
        "AIstock CI",
        "--branch",
        "main",
        "--commit",
        "62dc1b12",
        "--output",
        str(output_path),
        "--markdown-output",
        str(markdown_path),
        "--context-output",
        str(context_path),
        "--context-markdown-output",
        str(context_markdown_path),
    ]) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads(output_path.read_text(encoding="utf-8"))
    context_payload = json.loads(context_path.read_text(encoding="utf-8"))
    assert stdout_payload["diagnostic_status"] == "complete"
    assert stdout_payload["llm_guarded_rollout_gate"]["fallback"] == "deterministic_issue_workflow"
    assert file_payload["llm_guarded_rollout_gate"]["workflow_gate"] == "warning"
    assert context_payload["llm_guarded_rollout_gate"]["fallback"] == "deterministic_issue_workflow"
    assert file_payload["failed_jobs"][0]["nox_session"] == "paper_v2_backend"
    assert "Failed Tests" in markdown_path.read_text(encoding="utf-8")
    assert context_payload["schema_version"] == "aistock_ci_failure_context_pack_v1"
    assert "Agent Handoff" in context_markdown_path.read_text(encoding="utf-8")


def test_github_issue_payload_contains_dedupe_marker_and_labels() -> None:
    parsed = summary.parse_job_log(CI_LOG, job_name="Backend tests (paper_v2_backend)")
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "26378872481",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/26378872481",
            "branch": "main",
            "commit": "62dc1b12",
            "failed_jobs": [parsed],
            "extraction_errors": [],
        }
    )

    issue_payload = summary.build_github_issue_payload(payload)

    assert issue_payload["schema_version"] == "aistock_ci_failure_github_issue_payload_v1"
    assert issue_payload["dedupe"]["marker"] in issue_payload["body"]
    assert issue_payload["dedupe"]["run_marker"] in issue_payload["body"]
    assert "## Failure Summary" in issue_payload["body"]
    assert "triage-ci-issue --issue <issue-number>" in issue_payload["body"]
    assert "P1" in issue_payload["labels"]
    assert "module:paper_v2" in issue_payload["labels"]
    assert "Latest run" in issue_payload["recurrence_comment"]
    assert payload["llm_guarded_rollout_gate"]["workflow_gate"] == "warning"
    assert payload["llm_guarded_rollout_gate"]["fallback"] == "deterministic_issue_workflow"
    assert issue_payload["llm_enhancement"]["allowed"] is False
    assert issue_payload["llm_enhancement"]["deterministic_issue_creation_unaffected"] is True
    assert "## LLM Guarded Rollout" in issue_payload["body"]


def test_cli_writes_github_issue_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    log_path = tmp_path / "job.log"
    issue_payload_path = tmp_path / "issue-payload.json"
    log_path.write_text(CI_LOG, encoding="utf-8")

    assert summary.main([
        "--log-file",
        str(log_path),
        "--job-name",
        "Backend tests (paper_v2_backend)",
        "--source-name",
        "AIstock CI",
        "--run-id",
        "26378872481",
        "--branch",
        "main",
        "--commit",
        "62dc1b12",
        "--github-issue-payload-output",
        str(issue_payload_path),
    ]) == 0

    capsys.readouterr()
    issue_payload = json.loads(issue_payload_path.read_text(encoding="utf-8"))
    assert issue_payload["dedupe"]["fingerprint"].startswith("ci-")
    assert "aistock-ci-failure-fingerprint" in issue_payload["body"]


def test_cli_llm_kill_switch_still_writes_deterministic_issue_payload(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    log_path = tmp_path / "job.log"
    issue_payload_path = tmp_path / "issue-payload.json"
    log_path.write_text(CI_LOG, encoding="utf-8")

    assert summary.main(
        [
            "--log-file",
            str(log_path),
            "--job-name",
            "Backend tests (paper_v2_backend)",
            "--source-name",
            "AIstock CI",
            "--run-id",
            "26378872481",
            "--branch",
            "main",
            "--commit",
            "62dc1b12",
            "--llm-triage-mode",
            "off",
            "--github-issue-payload-output",
            str(issue_payload_path),
            "--stdout-format",
            "compact",
        ]
    ) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    issue_payload = json.loads(issue_payload_path.read_text(encoding="utf-8"))
    assert stdout_payload["llm_guarded_rollout"]["workflow_gate"] == "off"
    assert stdout_payload["artifacts"]["github_issue_payload"] == str(issue_payload_path)
    assert issue_payload["llm_enhancement"]["allowed"] is False
    assert issue_payload["llm_enhancement"]["fallback"] == "deterministic_issue_workflow"
    assert issue_payload["llm_enhancement"]["deterministic_issue_creation_unaffected"] is True
    assert "## Failure Summary" in issue_payload["body"]


def test_cli_opt_in_rollout_allows_llm_issue_enhancement(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    log_path = tmp_path / "job.log"
    issue_payload_path = tmp_path / "issue-payload.json"
    log_path.write_text(CI_LOG, encoding="utf-8")

    assert summary.main(
        [
            "--log-file",
            str(log_path),
            "--job-name",
            "Backend tests (paper_v2_backend)",
            "--source-name",
            "AIstock CI",
            "--run-id",
            "26378872481",
            "--branch",
            "main",
            "--commit",
            "62dc1b12",
            "--llm-triage-mode",
            "opt_in_auto_file",
            "--llm-auto-file-opt-in",
            "--github-issue-payload-output",
            str(issue_payload_path),
            "--stdout-format",
            "compact",
        ]
    ) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    issue_payload = json.loads(issue_payload_path.read_text(encoding="utf-8"))
    assert stdout_payload["llm_guarded_rollout"]["workflow_gate"] == "ready"
    assert stdout_payload["llm_guarded_rollout"]["auto_file_allowed"] is True
    assert issue_payload["llm_enhancement"]["allowed"] is True
    assert issue_payload["llm_enhancement"]["mode"] == "opt_in_auto_file"


def test_cli_compact_stdout_keeps_details_in_artifact(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    status_path = tmp_path / "nightly-status.json"
    output_path = tmp_path / "summary.json"
    status_path.write_text(
        json.dumps(
            {
                "statuses": {
                    "runnerPreflight": "success",
                    "drSnapshot": "success",
                    "drValidate": "success",
                    "nightlyL3": "failure",
                    "paperV2Live": "skipped",
                },
                "run_id": "9001",
            }
        ),
        encoding="utf-8",
    )

    assert summary.main([
        "--nightly-status-json",
        str(status_path),
        "--output",
        str(output_path),
        "--stdout-format",
        "compact",
    ]) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    artifact_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert stdout_payload["schema_version"] == "aistock_ci_failure_summary_compact_v1"
    assert "failed_jobs" not in stdout_payload
    assert stdout_payload["nightly_failed_stages"] == ["nightly_l3"]
    assert artifact_payload["schema_version"] == "aistock_ci_failure_summary_v1"


def test_cli_persists_tmp_failure_candidate_history(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    status_path = tmp_path / "nightly-status.json"
    output_path = tmp_path / "tmp" / "validation" / "nightly_failure_issue" / "summary.json"
    history_dir = tmp_path / "tests" / "aistock_validation" / "history" / "issue_candidates"
    monkeypatch.setattr(summary, "_default_candidate_history_dir", lambda: history_dir)
    status_path.write_text(
        json.dumps(
            {
                "statuses": {
                    "runnerPreflight": "success",
                    "drSnapshot": "success",
                    "drValidate": "success",
                    "nightlyL3": "failure",
                    "paperV2Live": "skipped",
                },
                "run_id": "9001",
                "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/9001",
            }
        ),
        encoding="utf-8",
    )

    assert summary.main(
        [
            "--nightly-status-json",
            str(status_path),
            "--output",
            str(output_path),
            "--stdout-format",
            "compact",
        ]
    ) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    history_path = Path(stdout_payload["artifacts"]["candidate_history"])
    history_payload = json.loads(history_path.read_text(encoding="utf-8"))

    assert history_path.is_file()
    assert "tmp/validation/nightly_failure_issue/candidate_history" in history_path.as_posix()
    assert "tests/aistock_validation/history" not in history_path.as_posix()
    assert history_payload["schema_version"] == "aistock_ci_failure_candidate_history_v1"
    assert history_payload["candidate"]["fingerprint"] == stdout_payload["fingerprint"]
    assert history_payload["candidate"]["module"] == "paper_v2"
    assert history_payload["run_count"] == 1
    assert history_payload["observed_run_ids"] == ["9001"]
    assert history_payload["log_policy"]["full_log_embedded"] is False


def test_candidate_history_persistence_dedupes_by_fingerprint(tmp_path: Path) -> None:
    history_dir = tmp_path / "tests" / "aistock_validation" / "history" / "issue_candidates"
    first = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "generated_at": "2026-06-02T00:00:00Z",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "1001",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/1001",
            "branch": "main",
            "commit": "abc",
            "failed_jobs": [],
            "extraction_errors": [],
        }
    )
    second = dict(first)
    second["run_id"] = "1002"
    second["run_url"] = "https://github.com/licong01-cloud/AIstock/actions/runs/1002"
    second["generated_at"] = "2026-06-02T01:00:00Z"
    second["failure_event"] = summary.build_failure_event(second)
    second["agent_handoff"] = summary.build_agent_handoff(second)

    first_path = summary.persist_candidate_history(first, history_dir=str(history_dir))
    second_path = summary.persist_candidate_history(second, history_dir=str(history_dir))

    assert first_path == second_path
    payload = json.loads(second_path.read_text(encoding="utf-8"))  # type: ignore[union-attr]
    assert payload["run_count"] == 2
    assert payload["observed_run_ids"] == ["1001", "1002"]
    assert payload["last_seen_at"] == "2026-06-02T01:00:00Z"


def test_actions_run_wait_defers_issue_when_logs_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], **_: object) -> dict[str, object]:
        calls.append(args)
        return {
            "ok": True,
            "stdout": json.dumps(
                {
                    "databaseId": 300,
                    "workflowName": "AIstock CI",
                    "displayTitle": "CI",
                    "event": "pull_request",
                    "headBranch": "bug/example",
                    "headSha": "abcdef1234567890",
                    "status": "in_progress",
                    "conclusion": None,
                    "url": "https://github.com/licong01-cloud/AIstock/actions/runs/300",
                    "jobs": [{"databaseId": 301, "name": "Backend tests (paper_v2_backend)", "conclusion": "failure"}],
                }
            ),
            "stderr": "",
        }

    monkeypatch.setattr(summary, "_run", fake_run)
    payload = summary.summarize_actions_run(
        repo="licong01-cloud/AIstock",
        run_id="300",
        wait_for_completion=True,
        wait_attempts=1,
        wait_seconds=0,
    )

    assert payload["diagnostic_status"] == "deferred"
    assert payload["issue_creation_policy"]["allowed"] is False
    assert payload["issue_creation_policy"]["reason"] == summary.LOGS_NOT_READY_REASON
    next_command = payload["issue_creation_policy"]["next_command"]
    assert "--wait-for-completion" in next_command
    assert "--log-attempts 3" in next_command
    assert "--stdout-format compact" in next_command
    assert "CI run is still in progress" in payload["failure_event"]["normalized_error"]
    with pytest.raises(ValueError, match="not actionable yet"):
        summary.build_github_issue_payload(payload)
    assert all("--job" not in call for call in calls)


def test_actions_run_defers_issue_when_job_log_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args: list[str], **_: object) -> dict[str, object]:
        if "--job" in args:
            return {
                "ok": False,
                "stdout": "",
                "stderr": "run 300 is still in progress; logs will be available when it is complete",
            }
        return {
            "ok": True,
            "stdout": json.dumps(
                {
                    "databaseId": 300,
                    "workflowName": "AIstock CI",
                    "displayTitle": "CI",
                    "event": "pull_request",
                    "headBranch": "bug/example",
                    "headSha": "abcdef1234567890",
                    "status": "in_progress",
                    "conclusion": None,
                    "url": "https://github.com/licong01-cloud/AIstock/actions/runs/300",
                    "jobs": [{"databaseId": 301, "name": "Backend tests (paper_v2_backend)", "conclusion": "failure"}],
                }
            ),
            "stderr": "",
        }

    monkeypatch.setattr(summary, "_run", fake_run)
    payload = summary.summarize_actions_run(repo="licong01-cloud/AIstock", run_id="300")

    assert payload["diagnostic_status"] == "deferred"
    assert payload["failed_jobs"][0]["nox_session"] == "paper_v2_backend"
    assert payload["issue_creation_policy"]["allowed"] is False
    assert payload["issue_creation_policy"]["reason"] == summary.LOGS_NOT_READY_REASON


def test_actions_run_retries_job_log_until_available(monkeypatch: pytest.MonkeyPatch) -> None:
    job_log_calls = 0

    def fake_run(args: list[str], **_: object) -> dict[str, object]:
        nonlocal job_log_calls
        if "--job" in args:
            job_log_calls += 1
            if job_log_calls == 1:
                return {
                    "ok": False,
                    "stdout": "",
                    "stderr": "run 300 is still in progress; logs will be available when it is complete",
                }
            return {"ok": True, "stdout": CI_LOG, "stderr": ""}
        return {
            "ok": True,
            "stdout": json.dumps(
                {
                    "databaseId": 300,
                    "workflowName": "AIstock CI",
                    "displayTitle": "CI",
                    "event": "pull_request",
                    "headBranch": "bug/example",
                    "headSha": "abcdef1234567890",
                    "status": "completed",
                    "conclusion": "failure",
                    "url": "https://github.com/licong01-cloud/AIstock/actions/runs/300",
                    "jobs": [{"databaseId": 301, "name": "Backend tests (paper_v2_backend)", "conclusion": "failure"}],
                }
            ),
            "stderr": "",
        }

    monkeypatch.setattr(summary, "_run", fake_run)
    monkeypatch.setattr(summary.time, "sleep", lambda _: None)
    monkeypatch.setattr(summary, "locate_last_green_run", lambda payload, repo: summary._last_green_payload(payload, status="not_requested"))

    payload = summary.summarize_actions_run(
        repo="licong01-cloud/AIstock",
        run_id="300",
        log_attempts=2,
        log_wait_seconds=0,
    )

    assert job_log_calls == 2
    assert payload["diagnostic_status"] == "complete"
    assert payload["issue_creation_policy"]["allowed"] is True
    assert payload["failed_jobs"][0]["failed_tests"]


def test_cli_deferred_summary_skips_github_issue_payload(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "summary.json"
    issue_payload_path = tmp_path / "github-issue-payload.json"

    def fake_summary(**_: object) -> dict[str, object]:
        return summary.finalize_summary(
            {
                "schema_version": "aistock_ci_failure_summary_v1",
                "generated_at": "2026-06-02T00:00:00Z",
                "severity": "P1",
                "workflow": "AIstock CI",
                "run_id": "300",
                "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/300",
                "branch": "bug/example",
                "commit": "abcdef1234567890",
                "failed_jobs": [],
                "extraction_errors": [
                    "run 300 is still in progress after 1 check(s); logs will be available when it is complete"
                ],
                "defer_issue_creation": True,
            }
        )

    monkeypatch.setattr(summary, "summarize_actions_run", fake_summary)

    assert summary.main(
        [
            "--run-id",
            "300",
            "--output",
            str(output_path),
            "--github-issue-payload-output",
            str(issue_payload_path),
            "--stdout-format",
            "compact",
        ]
    ) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    artifact_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert stdout_payload["diagnostic_status"] == "deferred"
    assert stdout_payload["issue_creation_policy"]["allowed"] is False
    assert artifact_payload["issue_creation_policy"]["reason"] == summary.LOGS_NOT_READY_REASON
    assert not issue_payload_path.exists()


def test_partial_unactionable_summary_blocks_payload_and_bug_promotion() -> None:
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "generated_at": "2026-06-02T00:00:00Z",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "301",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/301",
            "branch": "main",
            "commit": "abcdef1234567890",
            "failed_jobs": [
                {
                    "job_name": "Backend tests (paper_v2_backend)",
                    "nox_session": "paper_v2_backend",
                    "failed_tests": [],
                    "error_signature": None,
                    "key_log_excerpt": [],
                    "suspected_module": "paper_v2",
                    "suspected_files": [],
                }
            ],
            "extraction_errors": ["job log was unavailable or incomplete"],
        }
    )

    assert payload["diagnostic_status"] == "partial"
    assert payload["issue_creation_policy"]["allowed"] is False
    assert payload["issue_creation_policy"]["reason"] == "diagnostics_not_actionable"
    assert "--wait-for-completion" in payload["issue_creation_policy"]["next_command"]
    assert "--log-attempts 3" in payload["issue_creation_policy"]["next_command"]
    assert payload["agent_handoff"]["handoff_mode"] == "triage_only"
    assert payload["agent_handoff"]["needs_bug_json"] is False
    assert payload["agent_handoff"]["next_commands"] == [
        "python scripts/aistock_issue_workflow.py triage-ci-issue --issue <issue-number>"
    ]
    assert payload["agent_handoff"]["workflow_entrypoints"]["promote"] == "not_applicable_infra_action_only"
    with pytest.raises(ValueError, match="not actionable yet"):
        summary.build_github_issue_payload(payload)


def test_manual_summary_issue_is_triage_only_not_bug_promotion(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    issue_payload_path = tmp_path / "github-issue-payload.json"

    assert summary.main(
        [
            "--run-id",
            "302",
            "--run-url",
            "https://github.com/licong01-cloud/AIstock/actions/runs/302",
            "--manual-summary",
            "manual P1 failure report needs triage",
            "--branch",
            "main",
            "--commit",
            "abcdef1234567890",
            "--github-issue-payload-output",
            str(issue_payload_path),
            "--stdout-format",
            "compact",
        ]
    ) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    issue_payload = json.loads(issue_payload_path.read_text(encoding="utf-8"))

    assert stdout_payload["issue_creation_policy"]["allowed"] is True
    assert stdout_payload["issue_creation_policy"]["reason"] == "manual_summary_triage"
    assert "triage-ci-issue --issue <issue-number>" in issue_payload["body"]
    assert "promote-ci-issue" not in issue_payload["body"]
    assert "needs_bug_json: `False`" in issue_payload["body"]


def test_parse_job_log_extracts_docker_pull_failure_signature() -> None:
    log = """
Backend tests (rl_execution_smoke)\tUNKNOWN STEP\t2026-06-02T07:40:00Z nox > Running session rl_execution_smoke
Backend tests (rl_execution_smoke)\tUNKNOWN STEP\t2026-06-02T07:40:01Z ##[error]Docker pull failed with exit code 1
Backend tests (rl_execution_smoke)\tUNKNOWN STEP\t2026-06-02T07:40:02Z nox > Session rl_execution_smoke failed.
"""

    parsed = summary.parse_job_log(log, job_name="Backend tests (rl_execution_smoke)")

    assert parsed["nox_session"] == "rl_execution_smoke"
    assert parsed["command"] is None
    assert parsed["error_signature"] == "Docker pull failed with exit code 1"



def test_locate_last_green_run_finds_previous_success() -> None:
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "200",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/200",
            "branch": "main",
            "commit": "abcdef1234567890",
            "failed_jobs": [],
            "extraction_errors": [],
        }
    )

    def fake_run(args: list[str], **_: object) -> dict[str, object]:
        assert args[:3] == ["gh", "run", "list"]
        return {
            "ok": True,
            "stdout": json.dumps(
                [
                    {
                        "databaseId": 200,
                        "workflowName": "AIstock CI",
                        "headSha": "abcdef1234567890",
                        "headBranch": "main",
                        "conclusion": "failure",
                        "url": "https://github.com/licong01-cloud/AIstock/actions/runs/200",
                    },
                    {
                        "databaseId": 198,
                        "workflowName": "AIstock CI",
                        "headSha": "1234567890abcdef",
                        "headBranch": "main",
                        "conclusion": "success",
                        "url": "https://github.com/licong01-cloud/AIstock/actions/runs/198",
                        "createdAt": "2026-06-01T00:00:00Z",
                    },
                ]
            ),
            "stderr": "",
        }

    locator = summary.locate_last_green_run(payload, repo="licong01-cloud/AIstock", run_provider=fake_run)

    assert locator["schema_version"] == "aistock_ci_last_green_locator_v1"
    assert locator["status"] == "found"
    assert locator["blocking_for_issue_workflow"] is False
    assert locator["commit_range"] == "1234567890ab..abcdef123456"
    assert locator["previous_success_run"]["run_id"] == "198"


def test_regression_locator_is_rendered_and_carried_to_context_pack() -> None:
    parsed = summary.parse_job_log(CI_LOG, job_name="Backend tests (paper_v2_backend)")
    payload = summary.finalize_summary(
        {
            "schema_version": "aistock_ci_failure_summary_v1",
            "severity": "P1",
            "workflow": "AIstock CI",
            "run_id": "200",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/200",
            "branch": "main",
            "commit": "abcdef1234567890",
            "failed_jobs": [parsed],
            "extraction_errors": [],
            "last_green_locator": {
                "schema_version": "aistock_ci_last_green_locator_v1",
                "status": "found",
                "blocking_for_issue_workflow": False,
                "commit_range": "1234567890ab..abcdef123456",
                "previous_success_run": {"run_id": "198", "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/198"},
                "warnings": [],
            },
        }
    )

    markdown = summary.render_issue_markdown(payload)
    context_pack = summary.build_context_pack(payload, github_issue_number=517)

    assert "## Regression Locator" in markdown
    assert "1234567890ab..abcdef123456" in markdown
    context_markdown = summary.render_context_pack_markdown(context_pack)

    assert context_pack["last_green_locator"]["status"] == "found"
    assert context_pack["agent_handoff"]["regression_locator"]["commit_range"] == "1234567890ab..abcdef123456"
    assert "## Regression Locator" in context_markdown


def test_nightly_status_summary_uses_shared_payload_and_markers() -> None:
    payload = summary.summarize_nightly_status(
        {
            "statuses": {
                "runnerPreflight": "success",
                "drSnapshot": "success",
                "drValidate": "success",
                "nightlyL3": "failure",
                "paperV2Live": "skipped",
                "codeIntelligence": "success",
            },
            "run_id": "9001",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/9001",
        },
        branch="main",
        commit="abcdef1234567890",
    )
    issue_payload = summary.build_github_issue_payload(payload)
    context_pack = summary.build_context_pack(payload, github_issue_number=519)

    assert payload["workflow"] == "AIstock Nightly L3 + DR"
    assert payload["nightly_failed_stages"] == ["nightly_l3"]
    assert payload["issue_title"].startswith("P1 Nightly failed:")
    assert payload["reproduce_command"] == "gh run view 9001 --repo licong01-cloud/AIstock"
    assert "paper_v2" in payload["suspected_modules"]
    assert "noxfile.py" in payload["suspected_files"]
    assert "scripts/aistock_data_quality_smoke.py" in payload["suspected_files"]
    assert payload["llm_triage_advice"]["workflow_gate"] in {"ready", "warning"}
    assert payload["llm_triage_advice"]["llm_invocation_evidence"]["invoked"] is False
    if not payload["llm_triage_advice"].get("fallback_used"):
        assert payload["llm_triage_advice"]["test_plan_advice_gate"]["workflow_gate"] == "ready"
        assert payload["llm_triage_advice"]["test_plan_advice_gate"]["shell_commands_allowed"] is False
        assert payload["llm_triage_advice"]["test_plan_advice_gate"]["llm_invoked"] is False
    assert "<!-- aistock-nightly-failure:nightly-success-success-success-failure-skipped-success -->" in issue_payload["body"]
    assert "<!-- aistock-failure-kind:real_github_actions -->" in issue_payload["body"]
    assert issue_payload["failure_kind"] == "real_github_actions"
    assert issue_payload["synthetic"] is False
    assert issue_payload["dedupe"]["nightly_marker"] in issue_payload["dedupe"]["search_query"]
    assert "source:nightly" in issue_payload["labels"]
    assert "module:validation.runner" in issue_payload["labels"]
    assert context_pack["schema_version"] == "aistock_ci_failure_context_pack_v1"
    assert context_pack["failure_event"]["source"] == "github_actions"
    assert context_pack["failure_event"]["failure_kind"] == "real_github_actions"
    assert context_pack["llm_triage_advice"]["provider"] == "github_models"
    assert context_pack["token_budget"]["full_logs_included"] is False


def test_nightly_smoke_payload_is_explicitly_synthetic() -> None:
    payload = summary.summarize_nightly_status(
        {
            "statuses": {
                "runnerPreflight": "success",
                "drSnapshot": "success",
                "drValidate": "success",
                "nightlyL3": "failure",
                "paperV2Live": "skipped",
                "codeIntelligence": "success",
            },
            "run_id": "999999999",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/999999999",
        },
        branch="main",
        commit="abcdef1234567890",
    )
    issue_payload = summary.build_github_issue_payload(payload)
    context_pack = summary.build_context_pack(payload, github_issue_number=519)

    assert issue_payload["failure_kind"] == "synthetic_smoke"
    assert issue_payload["synthetic"] is True
    assert "<!-- aistock-failure-kind:synthetic_smoke -->" in issue_payload["body"]
    assert "Failure kind: `synthetic_smoke`" in issue_payload["body"]
    assert context_pack["failure_event"]["failure_kind"] == "synthetic_smoke"


def test_nightly_status_summary_includes_code_intelligence_failure() -> None:
    payload = summary.summarize_nightly_status(
        {
            "statuses": {
                "runnerPreflight": "success",
                "drSnapshot": "success",
                "drValidate": "success",
                "nightlyL3": "success",
                "paperV2Live": "success",
                "codeIntelligence": "failure",
            },
            "run_id": "9003",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/9003",
        },
        branch="main",
        commit="abcdef1234567890",
    )
    issue_payload = summary.build_github_issue_payload(payload)

    assert payload["nightly_failed_stages"] == ["code_intelligence"]
    assert payload["diagnostic_status"] == "complete"
    assert payload["issue_creation_policy"]["allowed"] is True
    assert payload["issue_creation_policy"]["reason"] == "ready"
    assert payload["agent_handoff"]["handoff_mode"] == "bug_promotion"
    assert payload["agent_handoff"]["needs_bug_json"] is True
    assert payload["suspected_modules"] == ["validation.runner"]
    assert "module:validation.runner" in issue_payload["labels"]
    assert "code=failure" in payload["issue_title"]
    assert "- code_intelligence: `failure`" in issue_payload["body"]
    assert "diagnostics_not_actionable" not in json.dumps(issue_payload)


def test_nightly_status_summary_keeps_payload_when_code_intelligence_fails_with_actionable_stage() -> None:
    payload = summary.summarize_nightly_status(
        {
            "statuses": {
                "runnerPreflight": "success",
                "drSnapshot": "success",
                "drValidate": "success",
                "nightlyL3": "failure",
                "paperV2Live": "success",
                "codeIntelligence": "failure",
            },
            "run_id": "9004",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/9004",
        },
        branch="main",
        commit="abcdef1234567890",
    )
    issue_payload = summary.build_github_issue_payload(payload)

    assert payload["nightly_failed_stages"] == ["nightly_l3", "code_intelligence"]
    assert payload["issue_creation_policy"]["allowed"] is True
    assert "<!-- aistock-nightly-failure:nightly-success-success-success-failure-success-failure -->" in issue_payload["body"]
    assert "- code_intelligence: `failure`" in issue_payload["body"]


def test_nightly_context_pack_includes_code_intelligence_refs() -> None:
    payload = summary.summarize_nightly_status(
        {
            "statuses": {
                "runnerPreflight": "success",
                "drSnapshot": "success",
                "drValidate": "success",
                "nightlyL3": "failure",
                "paperV2Live": "success",
                "codeIntelligence": "success",
            },
            "run_id": "9005",
            "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/9005",
        },
        branch="main",
        commit="abcdef1234567890",
    )
    payload["code_intelligence_refs"] = {
        "context_ref": "tmp/validation/code-intelligence/9005/codegraph-context.md",
        "affected_tests_ref": "tmp/validation/code-intelligence/9005/affected-tests.json",
        "affected_tests_count": 1,
        "understand_anything_summary_ref": "tmp/validation/code-intelligence/9005/ua-validation-summary.md",
        "understand_anything_status": "available",
    }
    payload["llm_triage_advice"] = summary._build_nightly_llm_triage_advice(payload)

    context_pack = summary.build_context_pack(payload, github_issue_number=519)
    markdown = summary.render_context_pack_markdown(context_pack)

    assert context_pack["code_intelligence_refs"]["affected_tests_count"] == 1
    assert "## Code Intelligence Refs" in markdown
    assert "code_intelligence_context_ref" in markdown
    assert "code_intelligence_ua_summary_ref" in markdown


def test_nightly_runner_outage_preserves_existing_dedupe_title() -> None:
    payload = summary.summarize_nightly_status(
        {
            "runner-preflight": "failure",
            "dr-snapshot": "skipped",
            "dr-validate": "skipped",
            "nightly-l3": "skipped",
            "paper-v2-live": "skipped",
        },
        run_id="9002",
        run_url="https://github.com/licong01-cloud/AIstock/actions/runs/9002",
    )
    issue_payload = summary.build_github_issue_payload(payload)

    assert payload["issue_title"] == "P1 Nightly blocked: self-hosted Windows runner unavailable"
    assert payload["nightly_fingerprint"] == "runner-preflight-unavailable"
    assert "<!-- aistock-nightly-failure:runner-preflight-unavailable -->" in issue_payload["body"]
    assert "self-hosted Windows runner unavailable" in issue_payload["body"]
    assert payload["agent_handoff"]["handoff_mode"] == "infra_action_only"
    assert payload["agent_handoff"]["needs_bug_json"] is False
    assert payload["agent_handoff"]["next_commands"] == [
        "python scripts/aistock_issue_workflow.py triage-ci-issue --issue <issue-number>"
    ]
    assert payload["agent_handoff"]["workflow_entrypoints"]["promote"] == "not_applicable_infra_action_only"
    assert "promote-ci-issue" not in issue_payload["body"]
    assert "needs_bug_json: `False`" in issue_payload["body"]
    assert "BUG ID: not applicable for infra-only issue" in issue_payload["body"]


def test_nightly_runner_outage_context_pack_omits_bug_promotion() -> None:
    payload = summary.summarize_nightly_status(
        {
            "runner-preflight": "failure",
            "dr-snapshot": "skipped",
            "dr-validate": "skipped",
            "nightly-l3": "skipped",
            "paper-v2-live": "skipped",
        },
        run_id="9002",
        run_url="https://github.com/licong01-cloud/AIstock/actions/runs/9002",
    )

    context_pack = summary.build_context_pack(payload, github_issue_number=257)
    markdown = summary.render_context_pack_markdown(context_pack)

    assert context_pack["agent_handoff"]["handoff_mode"] == "infra_action_only"
    assert context_pack["agent_handoff"]["needs_bug_json"] is False
    assert "triage-ci-issue --issue 257" in markdown
    assert "promote-ci-issue" not in markdown
    assert "needs_bug_json: `False`" in markdown


def test_issue_on_test_fail_workflow_uses_payload_file_and_policy_gate() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/issue-on-test-fail.yml").read_text(encoding="utf-8"))
    script = workflow["jobs"]["file-p0-p1-issue"]["steps"][3]["with"]["script"]
    build_step = workflow["jobs"]["file-p0-p1-issue"]["steps"][1]["run"]

    assert "--github-issue-payload-output tmp/validation/ci_failure_issue/github-issue-payload.json" in build_step
    assert "INPUT_LLM_TRIAGE_MODE" in build_step
    assert "INPUT_LLM_AUTO_FILE_OPT_IN" in build_step
    assert 'LLM_ARGS=(--llm-triage-mode "${INPUT_LLM_TRIAGE_MODE}")' in build_step
    assert '"${LLM_ARGS[@]}"' in build_step
    assert "--wait-for-completion" in build_step
    assert "--wait-attempts 2" in build_step
    assert "--log-attempts 3" in build_step
    assert "--stdout-format compact" in build_step
    assert "const issuePayloadPath = 'tmp/validation/ci_failure_issue/github-issue-payload.json';" in script
    assert "if (!fs.existsSync(issuePayloadPath))" in script
    assert "const payload = JSON.parse(fs.readFileSync(issuePayloadPath, 'utf8'));" in script
    assert "const renderBody = (issueNumber) => payload.body.replaceAll" in script
    assert "body: renderBody(created.data.number)" in script


def test_nightly_workflow_skips_issue_write_when_payload_is_absent() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/nightly.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["full-summary"]["steps"]
    script = next(step for step in steps if step.get("name") == "Auto-register failure as actionable GitHub Issue")[
        "with"
    ]["script"]

    assert "const issuePayloadPath = 'tmp/validation/nightly_failure_issue/github-issue-payload.json';" in script
    assert "if (!fs.existsSync(issuePayloadPath))" in script
    assert "No actionable Nightly issue created." in script
    assert "const payload = JSON.parse(fs.readFileSync(issuePayloadPath, 'utf8'));" in script


def test_nightly_workflow_manual_dispatch_can_skip_dr_and_live() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/nightly.yml").read_text(encoding="utf-8"))

    # PyYAML 1.1 treats the GitHub Actions key "on" as boolean True.
    triggers = workflow.get("on") or workflow.get(True)
    dispatch_inputs = triggers["workflow_dispatch"]["inputs"]
    assert dispatch_inputs["run_dr"]["default"] is False
    assert dispatch_inputs["run_nightly_l3"]["default"] is True
    assert dispatch_inputs["run_paper_v2_live"]["default"] is False
    assert dispatch_inputs["run_code_intelligence"]["default"] is True
    assert dispatch_inputs["llm_triage_mode"]["default"] == "warning_only"
    assert dispatch_inputs["llm_auto_file_opt_in"]["default"] is False
    assert "inputs.run_dr" in workflow["jobs"]["dr-snapshot"]["if"]
    assert "inputs.run_dr" in workflow["jobs"]["dr-validate"]["if"]
    assert "inputs.run_nightly_l3" in workflow["jobs"]["nightly-l3"]["if"]
    assert "github.event_name == 'workflow_dispatch' && !inputs.run_dr" in workflow["jobs"]["nightly-l3"]["if"]
    assert "inputs.run_nightly_l3 && inputs.run_paper_v2_live" in workflow["jobs"]["paper-v2-live"]["if"]
    assert "inputs.run_paper_v2_live" in workflow["jobs"]["paper-v2-live"]["if"]
    code_steps = "\n".join(step.get("run", "") for step in workflow["jobs"]["code-intelligence-weekly"]["steps"])
    assert "triage-quality-smoke" in code_steps
    assert "llm-triage-quality.json" in code_steps
    assert "test-plan-advice" in code_steps
    assert "llm-test-plan-advice.json" in code_steps
    assert "nightly-scheduler-advice" in code_steps
    assert "llm-nightly-scheduler-advice.json" in code_steps
    assert "nightly-discovery-hypothesis" in code_steps
    assert "llm-hypotheses.json" in code_steps
    assert "selected-plans.json" in code_steps
    assert "prompt-evaluation" in code_steps
    assert "llm-prompt-evaluation.json" in code_steps
    assert "guarded-rollout-gate" in code_steps
    assert "llm-guarded-rollout-gate.json" in code_steps
    full_summary_steps = workflow["jobs"]["full-summary"]["steps"]
    summary_run = next(step for step in full_summary_steps if step.get("name") == "Compose nightly summary")["run"]
    assert "run_dr_requested" in summary_run
    assert "run_nightly_l3_requested" in summary_run
    failure_step = next(step for step in full_summary_steps if step.get("name") == "Build Nightly failure issue context")
    failure_run = failure_step["run"]
    assert "LLM_TRIAGE_MODE" in failure_step["env"]
    assert 'LLM_ARGS=(--llm-triage-mode "${LLM_TRIAGE_MODE}")' in failure_run
    assert '"${LLM_ARGS[@]}"' in failure_run
    assert "--code-intelligence-json" in failure_run


def test_nightly_workflow_success_manifests_are_compact() -> None:
    text = Path(".github/workflows/nightly.yml").read_text(encoding="utf-8")

    assert "manifest=omitted_on_success" in text
    assert "Select-Object -First 100 -ExpandProperty FullName" not in text
