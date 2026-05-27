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
    assert "assert 200 == 409" in markdown
    assert "production_ddl_gate" in markdown


def test_cli_log_file_outputs_json_and_markdown(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    log_path = tmp_path / "job.log"
    output_path = tmp_path / "summary.json"
    markdown_path = tmp_path / "summary.md"
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
    ]) == 0

    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert stdout_payload["diagnostic_status"] == "complete"
    assert file_payload["failed_jobs"][0]["nox_session"] == "paper_v2_backend"
    assert "Failed Tests" in markdown_path.read_text(encoding="utf-8")
