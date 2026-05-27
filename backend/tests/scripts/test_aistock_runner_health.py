from __future__ import annotations

import json
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


def test_cli_writes_outputs_and_returns_blocked(tmp_path: Path, capsys) -> None:
    runners = tmp_path / "runners.json"
    runs = tmp_path / "runs.json"
    output_json = tmp_path / "runner-health.json"
    output_md = tmp_path / "runner-health.md"
    runners.write_text(json.dumps({"total_count": 0, "runners": []}), encoding="utf-8")
    runs.write_text(json.dumps({"workflow_runs": []}), encoding="utf-8")

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
    assert json.loads(capsys.readouterr().out)["schema_version"] == "aistock_runner_health_v1"
