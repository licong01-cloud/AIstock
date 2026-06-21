from __future__ import annotations

import json
from pathlib import Path

from scripts import nightly_bug_candidate_queue as queue


def _write_discovery_result(
    path: Path,
    *,
    plan_key: str = "workflow_discovery_root_clean_guard",
    anomaly: dict | None = None,
) -> None:
    payload = {
        "schema_version": "aistock_nightly_discovery_plan_result_v1",
        "plan_key": plan_key,
        "status": "completed",
        "anomalies": [anomaly or _high_confidence_anomaly(plan_key=plan_key)],
        "summary": {"anomaly_count": 1, "candidate_count": 1},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_manifest(path: Path, result_path: Path, *, plan_key: str = "workflow_discovery_root_clean_guard") -> None:
    payload = {
        "schema_version": "aistock_nightly_discovery_suite_v1",
        "rotation": {"focus_key": "workflow_validation", "no_candidate_reason": "readonly_rotation_found_no_anomaly_yet"},
        "results": [
            {
                "plan_key": plan_key,
                "status": "completed",
                "anomaly_count": 1,
                "artifact": result_path.as_posix(),
            }
        ],
        "summary": {"no_candidate_reason": None},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _high_confidence_anomaly(*, plan_key: str = "workflow_discovery_root_clean_guard") -> dict:
    return {
        "schema_version": "aistock_nightly_discovery_anomaly_v1",
        "plan_key": plan_key,
        "anomaly_id": "AD-high",
        "type": "unexpected_root_dirty_path",
        "severity": "P1",
        "title": "Unexpected dirty path in nightly workspace: scripts/nightly_bug_candidate_queue.py",
        "evidence_refs": ["scripts/nightly_bug_candidate_queue.py"],
        "dedupe_key": "root-dirty-fixture",
        "suggested_module": "validation.runner",
        "candidate": True,
        "details": {
            "expected": "Nightly workspace should stay clean except tmp validation artifacts.",
            "actual": "A tracked validation script was dirty before nightly execution.",
        },
    }


def test_low_confidence_synthetic_anomaly_stays_draft_and_no_issue_payload(tmp_path: Path) -> None:
    result = tmp_path / "discovery" / "workflow_discovery_root_clean_guard.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    _write_discovery_result(
        result,
        anomaly={
            **_high_confidence_anomaly(),
            "anomaly_id": "AD-synthetic",
            "title": "Synthetic smoke fixture should never create issues",
            "details": {"synthetic": True, "confidence": 0.95},
        },
    )
    _write_manifest(manifest, result)

    payload = queue.build_queue(discovery_manifest=manifest, output_dir=tmp_path / "queue", root=tmp_path)

    assert payload["summary"]["candidate_count"] == 1
    assert payload["summary"]["issue_payload_ready_count"] == 0
    assert payload["summary"]["artifact_only_count"] == 1
    candidate_path = tmp_path / payload["candidate_queue_ref"]
    candidates = json.loads(candidate_path.read_text(encoding="utf-8"))["candidates"]
    assert candidates[0]["quality_gate"]["issue_payload_ready"] is False
    assert "synthetic_anomaly" in candidates[0]["quality_gate"]["reasons"]


def test_high_confidence_fixture_generates_complete_github_issue_payload(tmp_path: Path) -> None:
    result = tmp_path / "discovery" / "workflow_discovery_root_clean_guard.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    code_ref = tmp_path / "code-intelligence-summary.json"
    ua_ref = tmp_path / "ua-summary-manifest.json"
    code_ref.write_text("{}", encoding="utf-8")
    ua_ref.write_text("{}", encoding="utf-8")
    _write_discovery_result(result)
    _write_manifest(manifest, result)

    payload = queue.build_queue(
        discovery_manifest=manifest,
        output_dir=tmp_path / "queue",
        root=tmp_path,
        code_intelligence_json=code_ref,
        ua_manifest_json=ua_ref,
    )

    assert payload["summary"]["issue_payload_ready_count"] == 1
    issue_payload_ref = payload["issue_payload_refs"][0]
    issue_payload = json.loads((tmp_path / issue_payload_ref).read_text(encoding="utf-8"))

    assert issue_payload["schema_version"] == "aistock_bug_candidate_github_issue_payload_v1"
    assert issue_payload["mode"] == "draft_only"
    assert issue_payload["auto_submit_allowed"] is False
    assert issue_payload["candidate"]["confidence"] >= 0.80
    assert issue_payload["candidate"]["allowed_write_scope"] == ["scripts/nightly_bug_candidate_queue.py"]
    assert issue_payload["candidate"]["codegraph_refs"] == ["code-intelligence-summary.json"]
    assert issue_payload["candidate"]["ua_refs"] == ["ua-summary-manifest.json"]
    assert "## Reproduce" in issue_payload["body"]
    assert "aistock-nightly-bug-candidate" in issue_payload["body"]
    assert "promote-nightly-candidate" in issue_payload["body"]
    assert "--issue-payload <this-payload-json> --create-registry-worktree --apply" in issue_payload["body"]
    assert "--opt-in-auto-file" not in issue_payload["body"]
    assert issue_payload["production_gates"]["production_ddl_gate"] == "noop"


def test_semantic_drift_candidate_generates_complete_issue_payload(tmp_path: Path) -> None:
    result = tmp_path / "discovery" / "validation_semantic_drift_discovery_readonly.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    anomaly = {
        **_high_confidence_anomaly(plan_key="validation_semantic_drift_discovery_readonly"),
        "type": "semantic_drift",
        "title": "Issue payload contract drift",
        "evidence_refs": ["tmp/validation/code-intelligence/fixture/issue-payload.json"],
        "suggested_module": "validation.runner",
        "details": {
            "summary": "Issue draft is missing required sections.",
            "expected": "Issue body includes expected, actual, reproduce, evidence, and next command.",
            "actual": "Issue body only includes a title.",
            "confidence": 0.91,
        },
    }
    _write_discovery_result(result, plan_key="validation_semantic_drift_discovery_readonly", anomaly=anomaly)
    _write_manifest(manifest, result, plan_key="validation_semantic_drift_discovery_readonly")

    payload = queue.build_queue(discovery_manifest=manifest, output_dir=tmp_path / "queue", root=tmp_path)
    issue_payload = json.loads((tmp_path / payload["issue_payload_refs"][0]).read_text(encoding="utf-8"))

    assert payload["summary"]["issue_payload_ready_count"] == 1
    assert payload["summary"]["issue_payload_ready_rate"] == 1.0
    assert issue_payload["candidate"]["failure_kind"] == "semantic_drift"
    assert issue_payload["candidate"]["source_plan_key"] == "validation_semantic_drift_discovery_readonly"
    assert "Issue body includes expected" in issue_payload["body"]


def test_historical_validation_quality_debt_is_separated_from_issue_payloads(tmp_path: Path) -> None:
    result = tmp_path / "discovery" / "validation_center_discovery_run_record_integrity.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    anomaly = {
        **_high_confidence_anomaly(plan_key="validation_center_discovery_run_record_integrity"),
        "type": "run_record_missing_production_gates",
        "severity": "P2",
        "title": "Validation history record lacks production gates: tests/aistock_validation/history/old.md",
        "evidence_refs": ["tests/aistock_validation/history/old.md"],
        "suggested_module": "tests.validation_history",
        "details": {
            "expected": "Historical validation records should include production gates.",
            "actual": "A historical validation record is missing production gate lines.",
            "confidence": 0.95,
        },
    }
    _write_discovery_result(result, plan_key="validation_center_discovery_run_record_integrity", anomaly=anomaly)
    _write_manifest(manifest, result, plan_key="validation_center_discovery_run_record_integrity")

    payload = queue.build_queue(discovery_manifest=manifest, output_dir=tmp_path / "queue", root=tmp_path)
    candidates = json.loads((tmp_path / payload["candidate_queue_ref"]).read_text(encoding="utf-8"))["candidates"]
    summary_md = (tmp_path / "queue" / "candidate-summary.md").read_text(encoding="utf-8")

    assert payload["summary"]["candidate_count"] == 1
    assert payload["summary"]["quality_debt_count"] == 1
    assert payload["summary"]["high_value_candidate_count"] == 0
    assert payload["summary"]["issue_payload_ready_count"] == 0
    assert payload["discovery_effectiveness"]["no_candidate_reason"] == "only_historical_quality_debt_candidates"
    assert candidates[0]["status"] == "quality_debt"
    assert candidates[0]["value_lane"] == "quality_debt"
    assert "historical_quality_debt" in candidates[0]["quality_gate"]["reasons"]
    assert "quality_debt: `1`" in summary_md
    assert "issue_payload_drafts: `0`" in summary_md


def test_p2_standard_candidates_do_not_compete_with_high_value_issue_drafts(tmp_path: Path) -> None:
    result = tmp_path / "discovery" / "validation_semantic_drift_discovery_readonly.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    anomaly = {
        **_high_confidence_anomaly(plan_key="validation_semantic_drift_discovery_readonly"),
        "type": "semantic_drift",
        "severity": "P2",
        "title": "Low-priority semantic drift advisory",
        "evidence_refs": ["tmp/validation/code-intelligence/advisory.json"],
        "suggested_module": "validation.runner",
        "details": {
            "expected": "Advisory drift should stay visible.",
            "actual": "A low-priority advisory was reported.",
            "confidence": 0.95,
        },
    }
    _write_discovery_result(result, plan_key="validation_semantic_drift_discovery_readonly", anomaly=anomaly)
    _write_manifest(manifest, result, plan_key="validation_semantic_drift_discovery_readonly")

    payload = queue.build_queue(discovery_manifest=manifest, output_dir=tmp_path / "queue", root=tmp_path)
    candidates = json.loads((tmp_path / payload["candidate_queue_ref"]).read_text(encoding="utf-8"))["candidates"]

    assert payload["summary"]["candidate_count"] == 1
    assert payload["summary"]["high_value_candidate_count"] == 0
    assert payload["summary"]["issue_payload_ready_count"] == 0
    assert payload["discovery_effectiveness"]["no_candidate_reason"] == "no_high_value_actionable_candidates"
    assert candidates[0]["value_lane"] == "standard"
    assert "not_high_value_candidate" in candidates[0]["quality_gate"]["reasons"]
    summary_md = (tmp_path / "queue" / "candidate-summary.md").read_text(encoding="utf-8")
    assert "no_candidate_reason: `no_high_value_actionable_candidates`" in summary_md


def test_duplicate_fingerprint_is_marked_deduped(tmp_path: Path) -> None:
    result = tmp_path / "discovery" / "workflow_discovery_root_clean_guard.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    existing_dir = tmp_path / "existing"
    fingerprint = queue.fingerprint_for_anomaly(_high_confidence_anomaly())
    existing_payload = {
        "schema_version": "aistock_bug_candidate_v1",
        "candidate_id": "NC-old",
        "fingerprint": fingerprint,
        "dedupe_fingerprint": fingerprint,
    }
    (existing_dir / "old.json").parent.mkdir(parents=True, exist_ok=True)
    (existing_dir / "old.json").write_text(json.dumps(existing_payload), encoding="utf-8")
    _write_discovery_result(result)
    _write_manifest(manifest, result)

    payload = queue.build_queue(
        discovery_manifest=manifest,
        output_dir=tmp_path / "queue",
        root=tmp_path,
        existing_queue_dirs=[existing_dir],
    )

    assert payload["summary"]["deduped_count"] == 1
    assert payload["summary"]["duplicate_rate"] == 1.0
    assert payload["summary"]["issue_payload_ready_count"] == 0
    candidates = json.loads((tmp_path / payload["candidate_queue_ref"]).read_text(encoding="utf-8"))["candidates"]
    assert candidates[0]["status"] == "deduped"
    assert "duplicate_fingerprint" in candidates[0]["quality_gate"]["reasons"]


def test_cli_prints_compact_success_and_writes_queue(tmp_path: Path, capsys) -> None:
    result = tmp_path / "discovery" / "workflow_discovery_root_clean_guard.json"
    manifest = tmp_path / "discovery" / "manifest.json"
    _write_discovery_result(result)
    _write_manifest(manifest, result)

    exit_code = queue.main(
        [
            "--json",
            "build",
            "--discovery-manifest",
            str(manifest),
            "--output-dir",
            str(tmp_path / "queue"),
            "--root",
            str(tmp_path),
        ]
    )
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert '"check": "nightly-bug-candidate-queue"' in stdout
    assert '"high_value": 1' in stdout
    assert '"issue_payloads": 1' in stdout
    assert (tmp_path / "queue" / "candidate-summary.md").exists()


def test_downloaded_artifact_manifest_resolves_sibling_result_files(tmp_path: Path) -> None:
    result = tmp_path / "downloaded" / "workflow_discovery_root_clean_guard.json"
    manifest = tmp_path / "downloaded" / "manifest.json"
    _write_discovery_result(result)
    _write_manifest(
        manifest,
        Path("tmp/validation/code-intelligence/999/discovery-plans/workflow_discovery_root_clean_guard.json"),
    )

    payload = queue.build_queue(discovery_manifest=manifest, output_dir=tmp_path / "queue", root=tmp_path)

    assert payload["summary"]["candidate_count"] == 1
    assert payload["summary"]["issue_payload_ready_count"] == 1


def test_queue_carries_rotation_and_no_candidate_reason(tmp_path: Path) -> None:
    manifest = tmp_path / "discovery" / "manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "aistock_nightly_discovery_suite_v1",
                "rotation": {
                    "focus_key": "code_intelligence_llm",
                    "no_candidate_reason": "readonly_rotation_found_no_anomaly_yet",
                },
                "results": [],
                "summary": {
                    "executed_count": 0,
                    "anomaly_count": 0,
                    "no_candidate_reason": "no_discovery_plans_selected",
                },
            }
        ),
        encoding="utf-8",
    )

    payload = queue.build_queue(discovery_manifest=manifest, output_dir=tmp_path / "queue", root=tmp_path)
    summary_md = (tmp_path / "queue" / "candidate-summary.md").read_text(encoding="utf-8")

    assert payload["rotation"]["focus_key"] == "code_intelligence_llm"
    assert payload["summary"]["candidate_count"] == 0
    assert payload["discovery_effectiveness"]["no_candidate_reason"] == "no_discovery_plans_selected"
    assert "rotation_focus: `code_intelligence_llm`" in summary_md
    assert "no_candidate_reason: `no_discovery_plans_selected`" in summary_md
