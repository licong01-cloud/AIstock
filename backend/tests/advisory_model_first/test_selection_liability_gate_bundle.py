from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.selection_liability_gate_bundle import (
    find_selection_liability_gate_bundle_for_request,
    load_selection_liability_gate_bundle,
    publish_selection_liability_gate_bundle,
)
from backend.tests.advisory_model_first.test_selection_liability_gate_contracts import _request


class _Booster:
    def save_model(self, path: str) -> None:
        Path(path).write_text("liability-model", encoding="utf-8")


def _publish(tmp_path: Path, resource: int, *, incomplete: bool = False):
    policy = tmp_path / "policy"
    policy.mkdir(exist_ok=True)
    (policy / "manifest.json").write_text("{}", encoding="utf-8")
    request = _request(
        policy_dataset_bundle_root=str(policy),
        output_root=str(tmp_path / "output"),
    )
    advancement = {
        "experiment_status": (
            "NEGATIVE_STOP_INCOMPLETE_CPCV" if incomplete else "NEGATIVE_STOP_NOT_ADVANCED"
        ),
        "advanced_to_stage_b": False,
    }
    result = publish_selection_liability_gate_bundle(
        request=request,
        liability_booster=None if incomplete else _Booster(),
        feature_schema={"feature_schema_hash": request.feature_schema_hash},
        runtime_hmm_models={},
        runtime_hmm_unavailable=[],
        walk_forward_hmm_receipt={"status": "READY"},
        threshold_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        coverage_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        transform_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        trial_metrics=pd.DataFrame() if incomplete else pd.DataFrame({"trial_id": ["trial"]}),
        block_scores=pd.DataFrame() if incomplete else pd.DataFrame({"trial_id": ["trial"]}),
        candidate_diagnostics={"status": "INCOMPLETE" if incomplete else "READY"},
        pbo_receipt={"status": "NOT_COMPUTABLE" if incomplete else "READY"},
        winner_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        baseline_comparison={"status": "READY"},
        reference_comparison={"status": "READY"},
        advancement_receipt=advancement,
        training_log={"status": "READY"},
        resource_report={"peak_rss_bytes": resource},
    )
    return request, result


def test_bundle_is_immutable_and_exact_retry_reuses_identity(tmp_path: Path) -> None:
    request, first = _publish(tmp_path, 100)
    _, second = _publish(tmp_path, 200)
    assert first[0] == second[0]
    found = find_selection_liability_gate_bundle_for_request(request)
    assert found is not None and found[0] == first[0]
    loaded = load_selection_liability_gate_bundle(
        first[1],
        expected_bundle_id=first[0],
        load_booster=False,
    )
    assert loaded["manifest"]["model_files"] == {"liability": "liability_model.txt"}
    assert loaded["manifest"]["runtime_eligible"] is False
    assert loaded["manifest"]["activated"] is False


def test_incomplete_bundle_is_evidence_only_and_has_no_model_or_winner(tmp_path: Path) -> None:
    _, result = _publish(tmp_path, 100, incomplete=True)
    manifest = result[2]
    assert manifest["model_available"] is False
    assert manifest["model_files"] == {}
    assert manifest["winner"] == {}
    assert manifest["stage_b_eligible"] is False
    with pytest.raises(AdvisoryModelFirstError):
        load_selection_liability_gate_bundle(
            result[1],
            expected_bundle_id=result[0],
            load_booster=True,
        )


def test_bundle_detects_file_tampering(tmp_path: Path) -> None:
    _, result = _publish(tmp_path, 100)
    (result[1] / "liability_model.txt").write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as exc:
        load_selection_liability_gate_bundle(
            result[1],
            expected_bundle_id=result[0],
            load_booster=False,
        )
    assert exc.value.reason_code == "ADVISORY_P0K_RETRY_MISMATCH"


def test_bundle_rejects_undeclared_files(tmp_path: Path) -> None:
    _, result = _publish(tmp_path, 100)
    (result[1] / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError):
        load_selection_liability_gate_bundle(
            result[1],
            expected_bundle_id=result[0],
            load_booster=False,
        )


def test_bundle_rejects_receipt_removed_from_disk_and_file_manifest(tmp_path: Path) -> None:
    _, result = _publish(tmp_path, 100)
    root = result[1]
    missing = root / "threshold_receipt.json"
    missing.unlink()
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"].pop(missing.name)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as exc:
        load_selection_liability_gate_bundle(
            root,
            expected_bundle_id=result[0],
            load_booster=False,
        )
    assert exc.value.reason_code == "ADVISORY_P0K_RETRY_MISMATCH"


def test_bundle_rejects_malformed_file_descriptor_as_typed_error(tmp_path: Path) -> None:
    _, result = _publish(tmp_path, 100)
    manifest_path = result[1] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["resource_report.json"] = {"sha256": "bad", "size_bytes": -1}
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as exc:
        load_selection_liability_gate_bundle(
            result[1],
            expected_bundle_id=result[0],
            load_booster=False,
        )
    assert exc.value.reason_code == "ADVISORY_P0K_RETRY_MISMATCH"
