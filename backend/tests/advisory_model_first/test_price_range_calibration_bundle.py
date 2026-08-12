from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_calibration_bundle import (
    publish_calibrated_price_range_bundle,
    validate_calibrated_price_range_bundle,
)
from backend.services.advisory_model_first.price_range_calibration_contracts import (
    PriceRangeCalibrationArtifactV1,
    build_frozen_price_range_calibration_request,
)
from backend.tests.advisory_model_first.test_price_range_bundle import (
    _request as parent_request,
    _split,
    _training,
)
from backend.services.advisory_model_first.price_range_bundle import publish_price_range_bundle


def _parent(tmp_path: Path) -> tuple[str, Path, dict]:
    split = _split()
    return publish_price_range_bundle(
        model_root=tmp_path, request=parent_request(tmp_path), split=split,
        training=_training(split),
        environment_report={"conda_environment": "rdagent-gpu", "lightgbm_version": "4.0", "pyarrow_version": "20.0"},
        resource_report={"peak_rss_bytes": 100, "limit_bytes": 8 * 1024**3},
    )


def _request(tmp_path: Path, parent_id: str, parent_path: Path, manifest: dict):
    artifact = PriceRangeCalibrationArtifactV1(
        path=str(tmp_path / "input.parquet"), sha256="b" * 64, size_bytes=10,
        row_count=20, columns=("split", "instrument"),
    )
    return build_frozen_price_range_calibration_request(
        output_root=str(tmp_path), parent_price_range_request_id=manifest["request_id"],
        parent_price_range_request_sha256=manifest["request_sha256"],
        parent_price_range_bundle_id=parent_id,
        parent_price_range_manifest_file_sha256=sha256_file(parent_path / "manifest.json"),
        package_id=manifest["package_id"], manifest_sha256=manifest["manifest_sha256"],
        style_profile_id=manifest["style_profile_id"], style_profile_hash=manifest["style_profile_hash"],
        feature_schema_version=manifest["feature_schema_version"], feature_schema_hash=manifest["feature_schema_hash"],
        label_policy_version=manifest["label_policy_version"], split_sha256=sha256_file(parent_path / "split.json"),
        parent_bundle_root=str(parent_path), features_artifact=artifact, price_range_labels_artifact=artifact,
        repository_root="/repo", repository_commit="3" * 40, created_at="2026-08-12T00:00:00+00:00",
    )


def test_m5c_bundle_is_atomic_exact_parent_model_preserving_and_tamper_evident(tmp_path: Path) -> None:
    parent_id, parent_path, parent_manifest = _parent(tmp_path)
    request = _request(tmp_path, parent_id, parent_path, parent_manifest)
    spec = {
        "schema_version": "advisory_price_range_calibration_spec_v1", "request_id": request.request_id,
        "request_sha256": request.request_sha256, "calibration_policy_version": "advisory_price_range_calibration_policy_v1",
        "state": "CALIBRATED", "method": "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION", "nominal_coverage": 0.8,
        "fit_split": "validation",
        "row_count": 10, "finite_sample_rank": 9, "delta": 0.01,
        "validation_projection_hash": "4" * 64, "validation_raw_quantile_crossing_count": 0,
        "validation_metrics": {"row_count": 10}, "entry_executable_calibration_state": "UNCALIBRATED",
        "entry_executable_reason_code": "ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING",
    }
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    predictions = pd.DataFrame({
        "decision_as_of_trade_date": ["2026-01-02"], "target_trade_date": ["2026-01-03"],
        "instrument": ["000001.SZ"], "entry_gap_return": [0.01],
        "entry_gap_raw_q10": [-0.01], "entry_gap_raw_q50": [0.01], "entry_gap_raw_q90": [0.03],
        "entry_gap_calibrated_q10": [-0.02], "entry_gap_calibrated_q50": [0.01], "entry_gap_calibrated_q90": [0.04],
        "entry_gap_calibration_state": ["CALIBRATED"], "entry_executable_calibration_state": ["UNCALIBRATED"],
    })

    bundle_id, bundle_path, manifest = publish_calibrated_price_range_bundle(
        request=request, calibration_spec_path=spec_path, metrics={"test": {"row_count": 1}},
        calibrated_test_predictions=predictions, calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
    )
    retry_id, retry_path, retry_manifest = publish_calibrated_price_range_bundle(
        request=request, calibration_spec_path=spec_path, metrics={"test": {"row_count": 1}},
        calibrated_test_predictions=predictions, calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
    )

    assert (retry_id, retry_path, retry_manifest) == (bundle_id, bundle_path, manifest)
    for name in ("entry_executable_probability", "entry_gap_q10", "entry_gap_q50", "entry_gap_q90"):
        assert sha256_file(parent_path / "models" / f"{name}.txt") == sha256_file(bundle_path / "models" / f"{name}.txt")
    assert validate_calibrated_price_range_bundle(bundle_path, expected_bundle_id=bundle_id) == manifest

    (bundle_path / "calibration_spec.json").write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as tampered:
        validate_calibrated_price_range_bundle(bundle_path, expected_bundle_id=bundle_id)
    assert tampered.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_BUNDLE_INVALID"
