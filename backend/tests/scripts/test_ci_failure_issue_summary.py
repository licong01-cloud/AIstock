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
    assert "promote-ci-issue --issue <issue-number> --apply" in markdown
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
    assert "promote-ci-issue --issue 197 --apply" in markdown


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
