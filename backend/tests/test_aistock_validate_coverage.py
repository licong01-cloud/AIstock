from __future__ import annotations

import json
from pathlib import Path

from scripts import aistock_validate


def _run_cli(args: list[str]) -> int:
    parsed = aistock_validate.build_parser().parse_args(args)
    return parsed.func(parsed)


def _write_xml(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" ?>
<coverage>
  <packages>
    <package name="pkg">
      <classes>
        <class name="foo.py" filename="pkg/foo.py">
          <lines>
            <line number="1" hits="1"/>
            <line number="2" hits="0"/>
            <line number="3" hits="1" branch="true" condition-coverage="50% (1/2)"/>
            <line number="4" hits="1"/>
          </lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>
""",
        encoding="utf-8",
    )


def test_coverage_xml_writes_snapshot_and_passes_gates(tmp_path) -> None:
    xml_path = tmp_path / "coverage.xml"
    output = tmp_path / "coverage_snapshot.json"
    _write_xml(xml_path)

    rc = _run_cli([
        "coverage",
        "--module",
        "validation_center",
        "--level",
        "L2",
        "--coverage-xml",
        str(xml_path),
        "--output",
        str(output),
        "--line-threshold",
        "75",
        "--branch-threshold",
        "50",
    ])

    assert rc == 0
    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["schema_version"] == aistock_validate.COVERAGE_SNAPSHOT_SCHEMA_VERSION
    assert snapshot["status"] == "passed"
    assert snapshot["totals"]["line_percent"] == 75.0
    assert snapshot["totals"]["branch_percent"] == 50.0
    assert [gate["status"] for gate in snapshot["quality_gates"][:2]] == ["passed", "passed"]
    assert snapshot["files"][0]["missing_lines"] == [2]


def test_coverage_gate_failure_returns_nonzero_and_preserves_evidence(tmp_path) -> None:
    xml_path = tmp_path / "coverage.xml"
    output = tmp_path / "coverage_snapshot.json"
    _write_xml(xml_path)

    rc = _run_cli([
        "coverage",
        "--module",
        "validation_center",
        "--coverage-xml",
        str(xml_path),
        "--output",
        str(output),
        "--line-threshold",
        "90",
    ])

    assert rc == 1
    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["status"] == "failed"
    assert snapshot["failed_gates"][0]["metric"] == "line"
    assert snapshot["failed_gates"][0]["reason"] == "75.0 < 90.0"


def test_coverage_no_fail_records_failed_gate_without_nonzero_exit(tmp_path) -> None:
    xml_path = tmp_path / "coverage.xml"
    output = tmp_path / "coverage_snapshot.json"
    _write_xml(xml_path)

    rc = _run_cli([
        "coverage",
        "--module",
        "validation_center",
        "--coverage-xml",
        str(xml_path),
        "--output",
        str(output),
        "--line-threshold",
        "90",
        "--no-fail",
    ])

    assert rc == 0
    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["status"] == "failed"


def test_coverage_json_parser_supports_branch_totals(tmp_path) -> None:
    coverage_json = tmp_path / "coverage.json"
    output = tmp_path / "coverage_snapshot.json"
    coverage_json.write_text(
        json.dumps(
            {
                "files": {
                    "pkg/bar.py": {
                        "executed_lines": [1, 3],
                        "missing_lines": [2],
                        "summary": {
                            "covered_lines": 2,
                            "num_statements": 3,
                            "covered_branches": 1,
                            "num_branches": 2,
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    rc = _run_cli([
        "coverage",
        "--module",
        "validation_center",
        "--coverage-json",
        str(coverage_json),
        "--output",
        str(output),
        "--branch-threshold",
        "50",
    ])

    assert rc == 0
    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["totals"]["line_percent"] == 66.67
    assert snapshot["totals"]["branch_percent"] == 50.0
    assert snapshot["files"][0]["covered_lines"] == [1, 3]


def test_diff_coverage_uses_changed_executable_lines(tmp_path) -> None:
    xml_path = tmp_path / "coverage.xml"
    patch_path = tmp_path / "changes.diff"
    output = tmp_path / "coverage_snapshot.json"
    _write_xml(xml_path)
    patch_path.write_text(
        """diff --git a/pkg/foo.py b/pkg/foo.py
--- a/pkg/foo.py
+++ b/pkg/foo.py
@@ -1,0 +1,3 @@
+covered
+missing
+covered
""",
        encoding="utf-8",
    )

    rc = _run_cli([
        "coverage",
        "--module",
        "validation_center",
        "--coverage-xml",
        str(xml_path),
        "--output",
        str(output),
        "--diff-patch",
        str(patch_path),
        "--diff-line-threshold",
        "80",
    ])

    assert rc == 1
    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["diff"]["line_percent"] == 66.67
    assert snapshot["diff"]["files"][0]["executable_changed_lines"] == [1, 2, 3]
    assert snapshot["diff"]["files"][0]["missing_changed_lines"] == [2]
    assert snapshot["failed_gates"][0]["metric"] == "diff_line"


def test_diff_coverage_fails_when_changed_file_is_missing_from_coverage(tmp_path) -> None:
    xml_path = tmp_path / "coverage.xml"
    patch_path = tmp_path / "changes.diff"
    output = tmp_path / "coverage_snapshot.json"
    _write_xml(xml_path)
    patch_path.write_text(
        """diff --git a/pkg/missing.py b/pkg/missing.py
--- a/pkg/missing.py
+++ b/pkg/missing.py
@@ -1,0 +1 @@
+new
""",
        encoding="utf-8",
    )

    rc = _run_cli([
        "coverage",
        "--module",
        "validation_center",
        "--coverage-xml",
        str(xml_path),
        "--output",
        str(output),
        "--diff-patch",
        str(patch_path),
        "--diff-line-threshold",
        "1",
    ])

    assert rc == 1
    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["diff"]["missing_coverage_files"] == ["pkg/missing.py"]
    assert "changed files are missing from coverage" in snapshot["failed_gates"][0]["reason"]
