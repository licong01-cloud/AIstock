from __future__ import annotations

import json
import subprocess
from pathlib import Path

from scripts import repo_hygiene_orphan_audit as audit


def _init_repo(root: Path) -> None:
    subprocess.run(["git", "-C", str(root), "init"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(root), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(root), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-m", "fixture"], check=True, capture_output=True)


def _write(path: Path, text: str = "x\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_repo_hygiene_audit_classifies_without_mutating_repo(tmp_path: Path) -> None:
    _write(tmp_path / ".github" / "workflows" / "nightly.yml", "name: nightly\n")
    _write(tmp_path / "backend" / "migrations" / "001_init.sql", "select 1;\n")
    _write(tmp_path / "scripts" / "manual_debug_backfill_20240101.py", "print('old')\n")
    _write(tmp_path / "backend" / "services" / "unused_service.py", "VALUE = 1\n")
    _write(tmp_path / "random_report.md", "# wrong place\n")
    _write(tmp_path / "tmp_dump.csv", "a,b\n1,2\n")
    _write(tmp_path / "docs" / "analysis" / "legacy_report.json", '{"legacy": true}\n')
    (tmp_path / "empty_orphan_dir").mkdir(parents=True)
    _write(tmp_path / "backend" / "services" / "runtime.py", "from backend.services.used import run\n")
    _write(tmp_path / "backend" / "services" / "used.py", "def run():\n    return 1\n")
    _init_repo(tmp_path)

    before = subprocess.run(
        ["git", "-C", str(tmp_path), "status", "--short"], text=True, capture_output=True, check=True
    ).stdout
    payload = audit.build_audit(tmp_path, scan_mode="tracked-and-untracked")
    after = subprocess.run(
        ["git", "-C", str(tmp_path), "status", "--short"], text=True, capture_output=True, check=True
    ).stdout

    rows = {row["path"]: row for row in payload["rows"]}
    assert before == after == ""
    assert payload["workflow_gate"] == "ready"
    assert payload["warning_only"] is True
    assert payload["candidate_only"] is True
    assert payload["side_effects"]["deletes_files"] is False
    assert payload["side_effects"]["writes_source"] is False
    assert rows[".github/workflows/nightly.yml"]["risk_level"] == "P0"
    assert rows["backend/migrations/001_init.sql"]["risk_level"] == "P0"
    assert rows["scripts/manual_debug_backfill_20240101.py"]["risk_level"] == "P1"
    assert rows["scripts/manual_debug_backfill_20240101.py"]["needs_human_approval"] is True
    assert rows["random_report.md"]["risk_level"] == "P2"
    assert rows["random_report.md"]["suggested_action"] == "relocate"
    assert rows["tmp_dump.csv"]["risk_level"] == "P4"
    assert rows["tmp_dump.csv"]["suggested_action"] == "delete_candidate"
    assert rows["docs/analysis/legacy_report.json"]["risk_level"] == "P3"
    assert rows["docs/analysis/legacy_report.json"]["suggested_action"] == "archive"
    assert rows["empty_orphan_dir"]["file_type"] == "empty_dir"
    assert rows["empty_orphan_dir"]["risk_level"] == "P4"
    assert rows["empty_orphan_dir"]["suggested_action"] == "delete_candidate"
    assert rows["backend/services/used.py"]["reference_count"] >= 1


def test_repo_hygiene_default_scan_uses_git_tracked_files_only(tmp_path: Path) -> None:
    _write(tmp_path / "scripts" / "tracked_debug.py", "print('tracked')\n")
    _init_repo(tmp_path)
    _write(tmp_path / "root_runtime_dump.json", '{"runtime": true}\n')
    (tmp_path / ".rtk" / "cache").mkdir(parents=True)
    _write(tmp_path / ".rtk" / "cache" / "tool_state.json", "{}\n")
    _write(tmp_path / "frontend" / ".next-dev-3000" / "trace.json", "{}\n")

    payload = audit.build_audit(tmp_path)
    rows = {row["path"]: row for row in payload["rows"]}

    assert payload["scan_mode"] == "tracked"
    assert "scripts/tracked_debug.py" in rows
    assert "root_runtime_dump.json" not in rows
    assert ".rtk/cache/tool_state.json" not in rows
    assert "frontend/.next-dev-3000/trace.json" not in rows


def test_repo_hygiene_opt_in_untracked_scan_still_excludes_runtime_dirs(tmp_path: Path) -> None:
    _write(tmp_path / "scripts" / "tracked_debug.py", "print('tracked')\n")
    _init_repo(tmp_path)
    _write(tmp_path / "root_runtime_dump.json", '{"runtime": true}\n')
    _write(tmp_path / "frontend" / ".next-dev-3000" / "trace.json", "{}\n")

    payload = audit.build_audit(tmp_path, scan_mode="tracked-and-untracked")
    rows = {row["path"]: row for row in payload["rows"]}

    assert "root_runtime_dump.json" in rows
    assert rows["root_runtime_dump.json"]["risk_level"] == "P4"
    assert "frontend/.next-dev-3000/trace.json" not in rows


def test_repo_hygiene_budget_failure_returns_compact_json(tmp_path: Path, capsys, monkeypatch) -> None:
    _write(tmp_path / "scripts" / "orphan.py", "VALUE = 1\n")
    _init_repo(tmp_path)

    def _raise_budget(*args, **kwargs):
        raise audit.AuditBudgetExceeded("repo hygiene audit exceeded 1s during reference indexing")

    monkeypatch.setattr(audit, "build_audit", _raise_budget)
    rc = audit.main(["--root", str(tmp_path), "--output-dir", str(tmp_path / "out"), "--json", "--max-seconds", "1"])

    assert rc == 2
    compact = json.loads(capsys.readouterr().out)
    assert compact["workflow_gate"] == "blocked"
    assert compact["audit_key"] == "repo_hygiene_orphan_audit"
    assert "reference indexing" in compact["error"]


def test_repo_hygiene_audit_writes_json_markdown_and_csv(tmp_path: Path) -> None:
    _write(tmp_path / "scripts" / "old_debug.py", "print('debug')\n")
    _init_repo(tmp_path)
    out = tmp_path / "out" / "audit.json"
    md = tmp_path / "out" / "audit.md"
    csv = tmp_path / "out" / "audit.csv"

    payload = audit.build_audit(tmp_path)
    audit.write_outputs(payload, out, md, csv)

    persisted = json.loads(out.read_text(encoding="utf-8"))
    assert persisted["schema_version"] == audit.SCHEMA_VERSION
    assert "Repo Hygiene Orphan Audit" in md.read_text(encoding="utf-8")
    csv_text = csv.read_text(encoding="utf-8-sig")
    assert "needs_human_approval" in csv_text
    assert "scripts/old_debug.py" in csv_text


def test_reference_index_fast_path_detects_import_and_path_refs(tmp_path: Path) -> None:
    _write(tmp_path / "backend" / "services" / "runtime.py", "from backend.services.used import run\n")
    _write(tmp_path / "backend" / "services" / "used.py", "def run():\n    return 1\n")
    _write(tmp_path / "docs" / "analysis" / "used.md", "`backend/services/used.py`\n")
    files = [
        "backend/services/runtime.py",
        "backend/services/used.py",
        "docs/analysis/used.md",
    ]

    refs = audit.build_reference_index(tmp_path, files)

    assert refs["backend/services/used.py"] == {
        "backend/services/runtime.py",
        "docs/analysis/used.md",
    }
    assert "docs/analysis/used.md" not in refs


def test_repo_hygiene_cli_compact_json(tmp_path: Path, capsys) -> None:
    _write(tmp_path / "scripts" / "orphan.py", "VALUE = 1\n")
    _init_repo(tmp_path)
    output_dir = tmp_path / "artifacts"

    rc = audit.main(["--root", str(tmp_path), "--output-dir", str(output_dir), "--json"])

    assert rc == 0
    stdout = capsys.readouterr().out
    compact = json.loads(stdout)
    assert compact["workflow_gate"] == "ready"
    assert compact["audit_key"] == "repo_hygiene_orphan_audit"
    assert compact["production_gates"]["production_ddl_gate"] == "noop"
    assert (output_dir / "repo-hygiene-orphan-audit.json").exists()
    assert (output_dir / "repo-hygiene-orphan-audit.md").exists()
    assert (output_dir / "repo-hygiene-orphan-audit.csv").exists()
