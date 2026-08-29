from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_bundle import (
    find_p0g_anchored_liability_local_reranker_bundle_for_request,
    load_p0g_anchored_liability_local_reranker_bundle,
    publish_p0g_anchored_liability_local_reranker_bundle,
)
from backend.tests.advisory_model_first.test_p0g_anchored_liability_local_reranker_contracts import (
    _request,
)


class _Booster:
    def save_model(self, path: str) -> None:
        Path(path).write_text("liability-model", encoding="utf-8")


def _publish(tmp_path: Path, *, incomplete: bool = False, resource: int = 100):
    policy = tmp_path / "policy"
    policy.mkdir(exist_ok=True)
    (policy / "manifest.json").write_text("{}", encoding="utf-8")
    request = _request(
        policy_dataset_bundle_root=str(policy),
        output_root=str(tmp_path / "output"),
    )
    status = "NEGATIVE_STOP_INCOMPLETE_CPCV" if incomplete else "NEGATIVE_STOP_NOT_ADVANCED"
    result = publish_p0g_anchored_liability_local_reranker_bundle(
        request=request,
        liability_booster=None if incomplete else _Booster(),
        feature_schema={"feature_schema_hash": request.feature_schema_hash},
        runtime_hmm_models={},
        runtime_hmm_unavailable=[],
        walk_forward_hmm_receipt={"status": "READY"},
        p0g_anchor_identity={"bundle_id": request.exact_p0g_anchor_reference.bundle_id},
        calibration_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        intervention_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        coverage_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        transform_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        trial_metrics=(pd.DataFrame() if incomplete else pd.DataFrame({"metric": [1.0]})),
        block_scores=(pd.DataFrame() if incomplete else pd.DataFrame({"metric": [1.0]})),
        candidate_diagnostics={"status": "READY"},
        pbo_receipt={"status": "NOT_COMPUTABLE" if incomplete else "READY"},
        winner_receipt={"status": "INCOMPLETE" if incomplete else "READY"},
        baseline_comparison={"status": "READY"},
        reference_comparison={"status": "READY"},
        advancement_receipt={"experiment_status": status, "advanced_to_stage_b": False},
        training_log={"status": "READY"},
        resource_report={"peak_rss_bytes": resource},
    )
    return request, result


def test_p0l_bundle_is_immutable_and_exact_retry_reuses_identity(tmp_path: Path) -> None:
    request, first = _publish(tmp_path, resource=100)
    _, second = _publish(tmp_path, resource=200)
    assert first[0] == second[0]
    found = find_p0g_anchored_liability_local_reranker_bundle_for_request(request)
    assert found is not None and found[0] == first[0]
    loaded = load_p0g_anchored_liability_local_reranker_bundle(
        first[1], expected_bundle_id=first[0], load_booster=False
    )
    assert loaded["manifest"]["model_files"] == {"liability": "liability_model.txt"}
    assert loaded["manifest"]["p0g_anchor_bundle_id"] == (
        request.exact_p0g_anchor_reference.bundle_id
    )
    assert loaded["manifest"]["runtime_eligible"] is False
    assert loaded["manifest"]["activated"] is False


def test_p0l_incomplete_bundle_has_no_model_or_winner(tmp_path: Path) -> None:
    _, result = _publish(tmp_path, incomplete=True)
    assert result[2]["model_available"] is False
    assert result[2]["winner"] == {}
    with pytest.raises(AdvisoryModelFirstError):
        load_p0g_anchored_liability_local_reranker_bundle(
            result[1], expected_bundle_id=result[0], load_booster=True
        )


def test_p0l_bundle_rejects_tampering_and_undeclared_files(tmp_path: Path) -> None:
    _, result = _publish(tmp_path)
    (result[1] / "liability_model.txt").write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        load_p0g_anchored_liability_local_reranker_bundle(
            result[1], expected_bundle_id=result[0], load_booster=False
        )
    assert exc_info.value.reason_code == "ADVISORY_P0L_RETRY_MISMATCH"


def test_p0l_bundle_rejects_nonfinite_json_and_parquet_evidence(tmp_path: Path) -> None:
    policy = tmp_path / "policy"
    policy.mkdir()
    (policy / "manifest.json").write_text("{}", encoding="utf-8")
    request = _request(
        policy_dataset_bundle_root=str(policy),
        output_root=str(tmp_path / "output"),
    )
    with pytest.raises(AdvisoryModelFirstError):
        publish_p0g_anchored_liability_local_reranker_bundle(
            request=request,
            liability_booster=_Booster(),
            feature_schema={"bad": float("nan")},
            runtime_hmm_models={},
            runtime_hmm_unavailable=[],
            walk_forward_hmm_receipt={},
            p0g_anchor_identity={},
            calibration_receipt={},
            intervention_receipt={},
            coverage_receipt={},
            transform_receipt={},
            trial_metrics=pd.DataFrame({"metric": [1.0]}),
            block_scores=pd.DataFrame({"metric": [1.0]}),
            candidate_diagnostics={},
            pbo_receipt={},
            winner_receipt={},
            baseline_comparison={},
            reference_comparison={},
            advancement_receipt={
                "experiment_status": "NEGATIVE_STOP_NOT_ADVANCED",
                "advanced_to_stage_b": False,
            },
            training_log={},
            resource_report={},
        )


def test_p0l_bundle_rejects_manifest_descriptor_damage(tmp_path: Path) -> None:
    _, result = _publish(tmp_path)
    manifest_path = result[1] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["resource_report.json"] = {"sha256": "bad", "size_bytes": -1}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError):
        load_p0g_anchored_liability_local_reranker_bundle(
            result[1], expected_bundle_id=result[0], load_booster=False
        )
