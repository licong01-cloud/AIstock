from __future__ import annotations

import json
from pathlib import Path

from scripts import ci_change_classifier as classifier


def _write_bug(path: Path, *, status: str = "fixed") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"bug_id": "BUG-191", "status": status}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_close_sync_bug_json_skips_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="fixed")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-example.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "close_sync_metadata_only"
    assert payload["close_sync_metadata_only"] is True
    assert payload["backend_required"] is False
    assert payload["static_gate_required"] is True
    assert payload["pr_quality_required"] is True


def test_open_bug_registry_change_keeps_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="open")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-example.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert any("status=open" in reason for reason in payload["reasons"])


def test_non_registry_change_keeps_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="fixed")

    payload = classifier.classify_changed_files(
        [
            "tests/aistock_validation/bugs/20260601_BUG-191-example.json",
            "scripts/aistock_issue_workflow.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert payload["non_bug_registry_files"] == ["scripts/aistock_issue_workflow.py"]


def test_allocator_change_keeps_backend_matrix(tmp_path: Path) -> None:
    allocator = tmp_path / "tests" / "aistock_validation" / "bugs" / ".bug_id_allocator.json"
    allocator.parent.mkdir(parents=True, exist_ok=True)
    allocator.write_text(json.dumps({"last_allocated": 191}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/.bug_id_allocator.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "full_ci_required"
    assert payload["backend_required"] is True
    assert any("allocator" in reason for reason in payload["reasons"])


def test_cli_writes_github_outputs(tmp_path: Path, capsys) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    out = tmp_path / "summary.json"
    github_out = tmp_path / "github_output.txt"
    _write_bug(bug, status="closed")

    assert classifier.main([
        "--repo-root",
        str(tmp_path),
        "--changed-file",
        "tests/aistock_validation/bugs/20260601_BUG-191-example.json",
        "--output-json",
        str(out),
        "--github-output",
        str(github_out),
    ]) == 0

    assert json.loads(out.read_text(encoding="utf-8"))["backend_required"] is False
    assert "backend_required=false" in github_out.read_text(encoding="utf-8")
    assert json.loads(capsys.readouterr().out)["classification"] == "close_sync_metadata_only"
