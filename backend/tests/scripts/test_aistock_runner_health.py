from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from scripts import aistock_runner_health as health


def test_runner_health_blocks_when_no_matching_runner() -> None:
    payload = health.build_runner_health_report(
        repo="licong01-cloud/AIstock",
        required_labels=["self-hosted", "windows"],
        runners_payload={"total_count": 0, "runners": []},
        runs_payload={"workflow_runs": []},
    )

    assert payload["schema_version"] == "aistock_runner_health_v1"
    assert payload["workflow_gate"] == "blocked"
    assert payload["online_matching_runners"] == []
    assert "no online GitHub Actions runner" in payload["blocking"][0]
    assert payload["production_gates"]["production_ddl_gate"] == "noop"


def test_runner_health_ready_with_online_self_hosted_windows_runner() -> None:
    payload = health.build_runner_health_report(
        required_labels=["self-hosted", "windows"],
        runners_payload={
            "total_count": 1,
            "runners": [
                {
                    "id": 10,
                    "name": "aistock-win-runner",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [{"name": "self-hosted"}, {"name": "Windows"}],
                }
            ],
        },
        runs_payload={"workflow_runs": []},
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["online_matching_runners"][0]["name"] == "aistock-win-runner"
    assert payload["blocking"] == []


def test_runner_health_requires_distinct_fast_and_security_roles() -> None:
    payload = health.build_runner_health_report(
        required_roles={
            "fast": ["self-hosted", "windows", "aistock-ci"],
            "security": ["self-hosted", "windows", "aistock-ci-security"],
        },
        runners_payload={
            "total_count": 2,
            "runners": [
                {
                    "id": 10,
                    "name": "aistock-fast",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci"},
                    ],
                },
                {
                    "id": 11,
                    "name": "aistock-security",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci-security"},
                    ],
                },
            ],
        },
        runs_payload={"workflow_runs": []},
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["runner_roles"]["fast"][0]["name"] == "aistock-fast"
    assert payload["runner_roles"]["security"][0]["name"] == "aistock-security"


def test_runner_health_rejects_one_runner_covering_both_parallel_roles() -> None:
    payload = health.build_runner_health_report(
        required_roles={
            "fast": ["self-hosted", "windows", "aistock-ci"],
            "security": ["self-hosted", "windows", "aistock-ci-security"],
        },
        runners_payload={
            "total_count": 1,
            "runners": [
                {
                    "id": 10,
                    "name": "aistock-combined",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci"},
                        {"name": "aistock-ci-security"},
                    ],
                }
            ],
        },
        runs_payload={"workflow_runs": []},
    )

    assert payload["workflow_gate"] == "blocked"
    assert any("distinct online capacity" in item for item in payload["blocking"])


def test_runner_health_reports_stale_queued_runs() -> None:
    payload = health.build_runner_health_report(
        required_labels=["self-hosted", "windows"],
        stale_queued_minutes=30,
        now=datetime(2026, 5, 27, 15, 0, tzinfo=timezone.utc),
        runners_payload={"total_count": 0, "runners": []},
        runs_payload={
            "workflow_runs": [
                {
                    "id": 123,
                    "status": "queued",
                    "created_at": "2026-05-27T14:00:00Z",
                    "html_url": "https://github.com/licong01-cloud/AIstock/actions/runs/123",
                    "head_branch": "main",
                    "head_sha": "abc123",
                }
            ]
        },
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["stale_queued_runs"][0]["run_id"] == 123
    assert payload["stale_queued_runs"][0]["age_minutes"] == 60.0
    assert "queued nightly.yml run" in payload["warnings"][0]


def test_runner_health_blocks_api_online_idle_runner_that_does_not_accept_work() -> None:
    payload = health.build_runner_health_report(
        workflow="codeql.yml",
        required_labels=["self-hosted", "windows", "aistock-ci-security"],
        stale_queued_minutes=10,
        now=datetime(2026, 9, 3, 4, 20, tzinfo=timezone.utc),
        runners_payload={
            "total_count": 1,
            "runners": [
                {
                    "id": 27,
                    "name": "aistock-security",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci-security"},
                    ],
                }
            ],
        },
        runs_payload={
            "workflow_runs": [
                {
                    "id": 33713291574,
                    "status": "queued",
                    "created_at": "2026-09-03T04:07:00Z",
                }
            ]
        },
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["online_but_not_accepting_work"] is True
    assert any("stuck self-update or listener" in item for item in payload["blocking"])


def test_runner_health_does_not_call_busy_runner_false_online() -> None:
    payload = health.build_runner_health_report(
        required_labels=["self-hosted", "windows", "aistock-ci"],
        stale_queued_minutes=10,
        now=datetime(2026, 9, 3, 4, 20, tzinfo=timezone.utc),
        runners_payload={
            "total_count": 1,
            "runners": [
                {
                    "id": 26,
                    "name": "aistock-ci",
                    "os": "Windows",
                    "status": "online",
                    "busy": True,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci"},
                    ],
                }
            ],
        },
        runs_payload={
            "workflow_runs": [
                {"id": 1, "status": "queued", "created_at": "2026-09-03T04:00:00Z"}
            ]
        },
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["online_but_not_accepting_work"] is False


def test_render_markdown_includes_actionable_sections() -> None:
    payload = health.build_runner_health_report(
        runners_payload={"total_count": 0, "runners": []},
        runs_payload={"workflow_runs": []},
    )

    markdown = health.render_markdown(payload)

    assert "# AIstock Runner Health" in markdown
    assert "## Blocking" in markdown
    assert "## Next Actions" in markdown
    assert "production_ddl_gate" in markdown


def test_cli_writes_outputs_and_returns_blocked(tmp_path: Path, capsys, monkeypatch) -> None:
    runners = tmp_path / "runners.json"
    runs = tmp_path / "runs.json"
    output_json = tmp_path / "runner-health.json"
    output_md = tmp_path / "runner-health.md"
    runners.write_text(json.dumps({"total_count": 0, "runners": []}), encoding="utf-8")
    runs.write_text(json.dumps({"workflow_runs": []}), encoding="utf-8")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    rc = health.main(
        [
            "doctor",
            "--runners-json",
            str(runners),
            "--runs-json",
            str(runs),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )

    assert rc == 2
    assert json.loads(output_json.read_text(encoding="utf-8"))["workflow_gate"] == "blocked"
    assert "AIstock Runner Health" in output_md.read_text(encoding="utf-8")
    captured = capsys.readouterr()
    assert json.loads(captured.out)["schema_version"] == "aistock_runner_health_v1"
    assert "::error::no online GitHub Actions runner" in captured.err


def test_resolve_github_token_prefers_env_over_gh(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_RUNNER_HEALTH_TOKEN", "runner-token")
    monkeypatch.setenv("GH_TOKEN", "gh-token")

    token, source = health.resolve_github_token()

    assert token == "runner-token"
    assert source == "AISTOCK_RUNNER_HEALTH_TOKEN"


def test_resolve_github_token_uses_gh_auth_fallback(monkeypatch) -> None:
    for name in ("AISTOCK_RUNNER_HEALTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    class Result:
        returncode = 0
        stdout = "fallback-token\n"

    monkeypatch.setattr(health.subprocess, "run", lambda *args, **kwargs: Result())

    token, source = health.resolve_github_token()

    assert token == "fallback-token"
    assert source == "gh_auth_token"


def test_resolve_github_token_reports_gh_subprocess_failure(monkeypatch) -> None:
    for name in ("AISTOCK_RUNNER_HEALTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    def raise_timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("gh", 10)

    monkeypatch.setattr(health.subprocess, "run", raise_timeout)

    token, source = health.resolve_github_token()

    assert token is None
    assert source == "gh_auth_error:TimeoutExpired"
