from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.validation_failure_event_to_bug as converter


def _failure_event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "schema_version": "aistock_validation_failure_event_v1",
        "event_id": "ci-26073670429-validation_center_backend",
        "source": "github_actions",
        "severity": "P1",
        "module": "validation_center",
        "plan_key": "validation_center_backend",
        "run_url": "https://github.example/actions/runs/26073670429",
        "commit": "733be35",
        "branch": "main",
        "title": "Validation Center backend failed: missing UI target",
        "expected": "all frontend routes are registered in ui_targets.yaml",
        "actual": "missing href /research-pipeline",
        "reproduce_command": "python -m nox -s validation_center_backend",
        "files": [
            "tests/aistock_validation/catalog/ui_targets.yaml",
            "frontend/src/lib/navigation/nav-groups.ts",
        ],
        "logs_excerpt": "FAILED backend/tests/test_validation_ui_target_catalog.py",
        "dedupe_key": "validation_center_backend:/research-pipeline:ui_target_missing",
    }
    event.update(overrides)
    return event


def _write_event(tmp_path: Path, event: dict[str, object] | None = None) -> Path:
    path = tmp_path / "failure-event.json"
    path.write_text(json.dumps(event or _failure_event()), encoding="utf-8")
    return path


def _write_existing_bug(bugs_dir: Path, payload: dict[str, object]) -> Path:
    bugs_dir.mkdir(parents=True, exist_ok=True)
    path = bugs_dir / f"20260518_{payload['bug_id']}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_dry_run_plans_new_bug_without_writing_and_github_pending(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        converter.sync.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network used")),
    )
    bugs_dir = tmp_path / "bugs"
    bugs_dir.mkdir()
    event_path = _write_event(tmp_path)

    result = converter.run(event_path=event_path, bugs_dir=bugs_dir)

    assert result["status"] == "planned"
    assert result["dry_run"] is True
    assert result["summary"] == {"create_json": 1}
    assert result["github_sync_summary"] == {"create": 1}
    plan = result["plan"][0]
    assert plan["action"] == "create_json"
    assert not Path(plan["path"]).exists()
    desired = plan["desired"]
    assert desired["bug_id"] == "BUG-001"
    assert desired["fingerprint"] == converter.fingerprint_for_failure(converter.load_failure_event(event_path))
    assert desired["validation_failure"]["event_id"] == "ci-26073670429-validation_center_backend"
    assert desired["validation_failure"]["failure_count"] == 1
    assert desired["github_sync"]["status"] == "pending"
    assert desired["github_sync"]["reason"] == "github_repo_unavailable"
    issue_body = result["github_sync_plan"][0]["desired"]["body"]
    assert "validation_failure" in issue_body
    assert "github_sync" in issue_body


def test_apply_writes_local_bug_json_and_preserves_pending_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    bugs_dir = tmp_path / "bugs"
    event_path = _write_event(tmp_path)

    code = converter.main(["--event-path", str(event_path), "--bugs-dir", str(bugs_dir), "--apply", "--json"])
    result = json.loads(capsys.readouterr().out)

    assert code == 0
    assert result["status"] == "applied"
    assert result["dry_run"] is False
    assert result["results"][0]["action"] == "created_json"
    created_path = Path(result["results"][0]["path"])
    assert created_path.exists()
    stored = json.loads(created_path.read_text(encoding="utf-8"))
    assert stored["github_sync"]["status"] == "pending"
    assert stored["github_sync"]["reason"] == "github_repo_unavailable"
    assert stored["github_sync"]["dry_run_summary"] == {"create": 1}
    assert "github_issue_number" not in stored
    assert stored["events"][-1]["action"] == "validation_failure_registered"


def test_existing_bug_dedup_updates_failure_metadata_in_place(tmp_path: Path) -> None:
    bugs_dir = tmp_path / "bugs"
    event = _failure_event(commit="newcommit", event_id="ci-repeat-2")
    fingerprint = converter.fingerprint_for_failure(converter.normalize_failure_event(event))
    existing = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": "BUG-777",
        "title": event["title"],
        "description": "Existing failure bug.",
        "module": event["module"],
        "severity": "P1",
        "risk_area": "validation_failure",
        "status": "open",
        "trigger_condition": {"source": "pytest"},
        "reproduce_command": event["reproduce_command"],
        "failing_run_id": "ci-first",
        "evidence_uris": [],
        "fingerprint": fingerprint,
        "assigned_agent": None,
        "fix_branch": None,
        "fix_commit": None,
        "verification_run_id": None,
        "created_at": "2026-05-18T00:00:00Z",
        "first_seen_at": "2026-05-18T00:00:00Z",
        "last_seen_at": "2026-05-18T00:00:00Z",
        "fixed_at": None,
        "submitted_at": "2026-05-18T00:00:00Z",
        "closed_at": None,
        "allowed_write_scope": [],
        "suspected_modules": [],
        "required_verification": [],
        "closure_requirements": [],
        "validation_failure": {
            "event_id": "ci-first",
            "source": "github_actions",
            "plan_key": "validation_center_backend",
            "run_url": "https://github.example/actions/runs/first",
            "dedupe_key": event["dedupe_key"],
            "first_seen_commit": "oldcommit",
            "last_seen_commit": "oldcommit",
            "last_seen_at": "2026-05-18T00:00:00Z",
            "failure_count": 1,
        },
        "events": [],
    }
    existing_path = _write_existing_bug(bugs_dir, existing)
    event_path = _write_event(tmp_path, event)

    result = converter.run(event_path=event_path, bugs_dir=bugs_dir, apply=True)

    assert result["summary"] == {"update_json": 1}
    assert result["plan"][0]["deduplicated"] is True
    assert len(list(bugs_dir.glob("*.json"))) == 1
    stored = json.loads(existing_path.read_text(encoding="utf-8"))
    assert stored["bug_id"] == "BUG-777"
    assert stored["validation_failure"]["failure_count"] == 2
    assert stored["validation_failure"]["first_seen_commit"] == "oldcommit"
    assert stored["validation_failure"]["last_seen_commit"] == "newcommit"
    assert stored["events"][-1]["action"] == "validation_failure_seen_again"


def test_dedupes_by_module_title_and_reproduce_when_fingerprint_missing(tmp_path: Path) -> None:
    bugs_dir = tmp_path / "bugs"
    event = _failure_event()
    existing = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": "BUG-778",
        "title": event["title"],
        "description": "Existing failure bug without a fingerprint.",
        "module": event["module"],
        "severity": "P1",
        "risk_area": "validation_failure",
        "status": "open",
        "trigger_condition": {},
        "reproduce_command": event["reproduce_command"],
        "created_at": "2026-05-18T00:00:00Z",
        "first_seen_at": "2026-05-18T00:00:00Z",
        "last_seen_at": "2026-05-18T00:00:00Z",
        "events": [],
    }
    _write_existing_bug(bugs_dir, existing)
    event_path = _write_event(tmp_path, event)

    result = converter.run(event_path=event_path, bugs_dir=bugs_dir)

    assert result["summary"] == {"update_json": 1}
    assert result["plan"][0]["bug_id"] == "BUG-778"
    assert result["plan"][0]["deduplicated"] is True


def test_github_snapshot_makes_issue_sync_dry_run_idempotent(tmp_path: Path) -> None:
    bugs_dir = tmp_path / "bugs"
    bugs_dir.mkdir()
    event_path = _write_event(tmp_path)
    issue_snapshot = tmp_path / "issues.json"

    initial = converter.run(event_path=event_path, bugs_dir=bugs_dir)
    desired_issue = initial["github_sync_plan"][0]["desired"]
    issue_snapshot.write_text(json.dumps({"issues": [{"number": 42, **desired_issue}]}), encoding="utf-8")

    result = converter.run(event_path=event_path, bugs_dir=bugs_dir, issues_snapshot=issue_snapshot)

    assert result["summary"] == {"create_json": 1}
    assert result["github_sync_summary"] == {"noop": 1}
    assert result["github_sync_plan"][0]["issue_number"] == 42


def test_cli_rejects_invalid_event_schema(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    event_path = _write_event(tmp_path, {"schema_version": "wrong", "module": "x", "title": "x", "reproduce_command": "x"})

    code = converter.main(["--event-path", str(event_path), "--bugs-dir", str(tmp_path / "bugs")])

    assert code == 2
    assert "schema_version must be" in capsys.readouterr().err
