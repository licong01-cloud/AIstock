from __future__ import annotations

import json
from pathlib import Path

from scripts import bug_registry_metadata_check as checker


def _write_bug(path: Path, **overrides: object) -> None:
    payload: dict[str, object] = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": "BUG-392",
        "status": "fixed",
        "github_issue_number": 1172,
        "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/1172",
        "production_ddl_gate": "noop",
        "production_frontend_dependency_gate": "noop",
        "production_backend_dependency_gate": "noop",
    }
    payload.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_close_sync_registry_metadata_passes_with_linkage_and_gates(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260616_BUG-392-example.json"
    _write_bug(tmp_path / bug_rel)

    payload = checker.check_bug_registry_metadata(
        repo_root=tmp_path,
        changed_files=[bug_rel],
        close_sync_only=True,
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["bug_ids"] == ["BUG-392"]
    assert payload["blocking"] == []


def test_open_bug_is_not_close_sync_metadata(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260616_BUG-392-example.json"
    _write_bug(tmp_path / bug_rel, status="open")

    payload = checker.check_bug_registry_metadata(
        repo_root=tmp_path,
        changed_files=[bug_rel],
        close_sync_only=True,
    )

    assert payload["workflow_gate"] == "blocked"
    assert any("status=open" in item for item in payload["blocking"])


def test_missing_github_linkage_blocks_registry_lane(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260616_BUG-392-example.json"
    _write_bug(tmp_path / bug_rel, github_issue_number=None, github_issue_url=None)

    payload = checker.check_bug_registry_metadata(
        repo_root=tmp_path,
        changed_files=[bug_rel],
    )

    assert payload["workflow_gate"] == "blocked"
    assert any("missing github_issue_number" in item for item in payload["blocking"])


def test_allocator_must_not_lag_changed_bug_ids(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260616_BUG-394-example.json"
    allocator_rel = "tests/aistock_validation/bugs/.bug_id_allocator.json"
    _write_bug(tmp_path / bug_rel, bug_id="BUG-394", github_issue_number=1174)
    allocator = tmp_path / allocator_rel
    allocator.parent.mkdir(parents=True, exist_ok=True)
    allocator.write_text(json.dumps({"schema_version": "aistock_bug_id_allocator_v1", "last_allocated": 393}), encoding="utf-8")

    payload = checker.check_bug_registry_metadata(
        repo_root=tmp_path,
        changed_files=[bug_rel, allocator_rel],
    )

    assert payload["workflow_gate"] == "blocked"
    assert any("behind changed BUG max=394" in item for item in payload["blocking"])


def test_cli_stdout_is_compact(tmp_path: Path, capsys) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260616_BUG-392-example.json"
    output = tmp_path / "summary.json"
    _write_bug(tmp_path / bug_rel)

    exit_code = checker.main([
        "--repo-root",
        str(tmp_path),
        "--changed-file",
        bug_rel,
        "--close-sync-only",
        "--output-json",
        str(output),
    ])
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert stdout.strip() == "BUG registry metadata check: gate=passed files=1 allocator_changed=false blocking=0"
    assert json.loads(output.read_text(encoding="utf-8"))["workflow_gate"] == "passed"
