from __future__ import annotations

import json
from pathlib import Path

import scripts.bug_github_webhook as webhook


def _write_bug(tmp_path: Path, *, bug_id: str = "BUG-950", status: str = "open") -> None:
    payload = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": bug_id,
        "title": "Webhook mirrored bug",
        "description": "Existing bug entry for webhook import tests.",
        "module": "validation_center",
        "severity": "P1",
        "risk_area": "data_correctness",
        "status": status,
        "trigger_condition": {"source": "pytest"},
        "evidence_uris": [],
        "fingerprint": f"pytest::{bug_id}",
        "created_at": "2026-05-12T00:00:00Z",
        "first_seen_at": "2026-05-12T00:00:00Z",
        "last_seen_at": "2026-05-12T00:00:00Z",
        "closed_at": None,
        "events": [],
    }
    (tmp_path / f"20260512_{bug_id}.json").write_text(json.dumps(payload), encoding="utf-8")


def test_plan_from_issues_event_updates_existing_status(tmp_path: Path) -> None:
    _write_bug(tmp_path, bug_id="BUG-951", status="open")
    event = {
        "action": "closed",
        "issue": {
            "number": 951,
            "title": "[BUG-951] Webhook mirrored bug",
            "body": "<!-- aistock-bug-id: BUG-951 -->",
            "state": "closed",
            "labels": [{"name": "severity:p1"}],
        },
    }

    result = webhook.plan_from_event(event, bugs_dir=tmp_path, event_name="issues")

    assert result["status"] == "planned"
    assert result["summary"] == {"update_json": 1}
    assert result["plan"][0]["changes"]["status"] == "closed"


def test_plan_from_event_creates_github_only_issue_record(tmp_path: Path) -> None:
    event = {
        "action": "opened",
        "issue": {
            "number": 952,
            "title": "[P1] GitHub-only regression",
            "body": "Module: paper_v2\n\nA user-filed issue.",
            "state": "open",
            "labels": ["P1", "module:paper_v2"],
            "html_url": "https://github.example/issues/952",
        },
    }

    result = webhook.plan_from_event(event, bugs_dir=tmp_path, event_name="issues", p0_p1_only=True)

    assert result["summary"] == {"create_json": 1}
    assert result["plan"][0]["bug_id"] == "BUG-GH-952"
    assert result["plan"][0]["desired"]["module"] == "paper_v2"


def test_cli_defaults_to_dry_run_without_writing(tmp_path: Path, capsys) -> None:
    event_dir = tmp_path / "events"
    event_dir.mkdir()
    bugs_dir = tmp_path / "bugs"
    bugs_dir.mkdir()
    event_path = event_dir / "event.json"
    event_path.write_text(
        json.dumps({
            "action": "opened",
            "issue": {
                "number": 953,
                "title": "[P1] Dry run import",
                "state": "open",
                "labels": ["P1"],
            },
        }),
        encoding="utf-8",
    )

    code = webhook.main(["--event-path", str(event_path), "--event-name", "issues", "--bugs-dir", str(bugs_dir), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["dry_run"] is True
    assert payload["summary"] == {"create_json": 1}
    assert not list(bugs_dir.glob("*BUG-GH-953*.json"))


def test_cli_apply_writes_new_bug_json(tmp_path: Path, capsys) -> None:
    event_dir = tmp_path / "events"
    event_dir.mkdir()
    bugs_dir = tmp_path / "bugs"
    bugs_dir.mkdir()
    event_path = event_dir / "event.json"
    event_path.write_text(
        json.dumps({
            "action": "opened",
            "issue": {
                "number": 954,
                "title": "[P1] Apply import",
                "state": "open",
                "labels": ["P1"],
            },
        }),
        encoding="utf-8",
    )

    code = webhook.main(["--event-path", str(event_path), "--event-name", "issues", "--bugs-dir", str(bugs_dir), "--apply", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["dry_run"] is False
    assert payload["status"] == "applied"
    assert list(bugs_dir.glob("*BUG-GH-954*.json"))


def test_unsupported_issue_action_is_ignored(tmp_path: Path) -> None:
    result = webhook.plan_from_event(
        {"action": "assigned", "issue": {"number": 955, "title": "[P1] Assigned only"}},
        bugs_dir=tmp_path,
        event_name="issues",
    )

    assert result["status"] == "ignored"
    assert result["plan"] == []
