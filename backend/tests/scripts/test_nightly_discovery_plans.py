from __future__ import annotations

import json
from pathlib import Path

from scripts import nightly_discovery_plans as plans


def _write_bug(path: Path, **overrides) -> None:
    payload = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": "BUG-999",
        "title": "Issue intake regression fixture",
        "description": "This fixture has enough detail for issue intake validation.",
        "module": "validation.runner",
        "severity": "P1",
        "status": "open",
        "github_issue_number": 999,
        "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/999",
        "required_verification": ["l0"],
    }
    payload.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_issue_intake_readonly_detects_missing_linkage(tmp_path: Path) -> None:
    bug_file = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260618_BUG-999-fixture.json"
    _write_bug(bug_file, github_issue_number=None, github_issue_url=None)

    result = plans.run_plan("validation_discovery_issue_intake_readonly", root=tmp_path)

    assert result["side_effects"]["readonly"] is True
    assert result["production_gates"]["production_ddl_gate"] == "noop"
    assert result["summary"]["candidate_count"] == 1
    assert result["anomalies"][0]["type"] == "bug_missing_github_linkage"


def test_issue_intake_readonly_accepts_fixed_bug_with_fix_commit_only(tmp_path: Path) -> None:
    bug_file = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260629_BUG-544-fixture.json"
    _write_bug(
        bug_file,
        bug_id="BUG-544",
        status="fixed",
        fix_commit="de55b4a91604366efde2b4a9451a4423c288f196",
        pr_url=None,
    )

    result = plans.run_plan("validation_discovery_issue_intake_readonly", root=tmp_path)

    assert result["summary"]["candidate_count"] == 0
    assert result["summary"]["no_candidate_reason"] == "no_anomalies_detected"


def test_root_clean_guard_ignores_tmp_validation_artifacts(tmp_path: Path, monkeypatch) -> None:
    def fake_status(root: Path) -> list[str]:
        return ["?? tmp/validation/nightly/file.json", " M backend/main.py"]

    monkeypatch.setattr(plans, "_git_status_lines", fake_status)

    result = plans.run_plan("workflow_discovery_root_clean_guard", root=tmp_path)

    assert [item["type"] for item in result["anomalies"]] == ["unexpected_root_dirty_path"]
    assert result["anomalies"][0]["evidence_refs"] == ["backend/main.py"]


def test_code_intelligence_quality_detects_missing_affected_tests(tmp_path: Path) -> None:
    artifact = tmp_path / "code-intelligence-summary.json"
    artifact.write_text(json.dumps({"changed_files": ["scripts/llm_provider_adapter.py"]}), encoding="utf-8")

    result = plans.run_plan(
        "code_intelligence_discovery_affected_tests_quality",
        root=tmp_path,
        code_intelligence_json=artifact,
    )

    types = {item["type"] for item in result["anomalies"]}
    assert "changed_files_without_affected_tests" in types
    assert "codegraph_ref_missing" in types


def test_run_record_integrity_detects_missing_gates(tmp_path: Path) -> None:
    record = tmp_path / "tests" / "aistock_validation" / "history" / "validation" / "record.md"
    record.parent.mkdir(parents=True, exist_ok=True)
    record.write_text("# Run\n\nstatus: passed\n", encoding="utf-8")

    result = plans.run_plan("validation_center_discovery_run_record_integrity", root=tmp_path)

    assert result["summary"]["candidate_count"] == 1
    assert result["anomalies"][0]["type"] == "run_record_missing_production_gates"


def test_semantic_drift_fixture_generates_candidate(tmp_path: Path) -> None:
    artifact = tmp_path / "code-intelligence-summary.json"
    artifact.write_text(
        json.dumps(
            {
                "semantic_drift_signals": [
                    {
                        "title": "Issue payload contract drift",
                        "summary": "Expected/actual/reproduce sections are missing from an issue draft.",
                        "expected": "Nightly issue drafts include expected, actual, reproduce, evidence, and next command.",
                        "actual": "A draft only contains a title.",
                        "severity": "P1",
                        "module": "validation.runner",
                        "confidence": 0.91,
                        "evidence_refs": ["tmp/validation/code-intelligence/fixture/issue-payload.json"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = plans.run_plan(
        "validation_semantic_drift_discovery_readonly",
        root=tmp_path,
        code_intelligence_json=artifact,
    )

    assert result["summary"]["candidate_count"] == 1
    anomaly = result["anomalies"][0]
    assert anomaly["type"] == "semantic_drift"
    assert anomaly["severity"] == "P1"
    assert anomaly["details"]["confidence"] == 0.91
    assert anomaly["evidence_refs"] == ["tmp/validation/code-intelligence/fixture/issue-payload.json"]


def test_run_selected_executes_only_discovery_plans(tmp_path: Path, monkeypatch) -> None:
    selected = tmp_path / "selected-plans.json"
    selected.write_text(
        json.dumps(
            {
                "selected_plan_keys": [
                    "validation_catalog_integrity",
                    "workflow_discovery_root_clean_guard",
                ],
                "rotation": {
                    "focus_key": "workflow_validation",
                    "selected_plan_keys": ["workflow_discovery_root_clean_guard"],
                    "no_candidate_reason": "readonly_rotation_found_no_anomaly_yet",
                },
                "discovery_statistics": {"planned_plan_count": 1},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(plans, "_git_status_lines", lambda root: [])

    manifest = plans.run_selected(selected_plans=selected, output_dir=tmp_path / "out", root=tmp_path)

    assert manifest["executed_plan_keys"] == ["workflow_discovery_root_clean_guard"]
    assert manifest["skipped_plan_keys"] == ["validation_catalog_integrity"]
    assert manifest["summary"]["executed_count"] == 1
    assert manifest["rotation"]["focus_key"] == "workflow_validation"
    assert manifest["summary"]["candidate_count"] == 0
    assert manifest["summary"]["no_candidate_reason"] == "readonly_rotation_found_no_anomaly_yet"
    assert manifest["discovery_statistics"]["executed_plan_count"] == 1
    assert (tmp_path / "out" / "workflow_discovery_root_clean_guard.json").exists()


def test_cli_run_selected_uses_compact_success_output(tmp_path: Path, capsys, monkeypatch) -> None:
    selected = tmp_path / "selected-plans.json"
    selected.write_text(json.dumps({"selected_plan_keys": ["workflow_discovery_root_clean_guard"]}), encoding="utf-8")
    monkeypatch.setattr(plans, "_git_status_lines", lambda root: [])

    exit_code = plans.main(
        [
            "--json",
            "run-selected",
            "--selected-plans",
            str(selected),
            "--output-dir",
            str(tmp_path / "out"),
            "--root",
            str(tmp_path),
        ]
    )
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert '"check": "nightly-discovery-selected"' in stdout
    assert '"executed_count": 1' in stdout
    assert (tmp_path / "out" / "manifest.json").exists()
