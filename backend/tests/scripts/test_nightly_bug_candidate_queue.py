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
        "results": [
            {
                "plan_key": plan_key,
                "status": "completed",
                "anomaly_count": 1,
                "artifact": result_path.as_posix(),
            }
        ],
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
    assert issue_payload["production_gates"]["production_ddl_gate"] == "noop"


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
    assert '"issue_payloads": 1' in stdout
    assert (tmp_path / "queue" / "candidate-summary.md").exists()
