from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.price_range_calibration_contracts import (
    PriceRangeCalibrationArtifactV1,
    build_frozen_price_range_calibration_request,
)
from backend.services.advisory_model_first import price_range_calibration_pipeline as pipeline


def _request(tmp_path: Path):
    parent_id = "a" * 64
    artifact = PriceRangeCalibrationArtifactV1(
        path=str(tmp_path / "input.parquet"), sha256="b" * 64, size_bytes=10,
        row_count=2, columns=(*pipeline.KEYS, "split", "gap_modelable", "entry_gap_return"),
    )
    return build_frozen_price_range_calibration_request(
        output_root=str(tmp_path), parent_price_range_request_id="advprreq_parent",
        parent_price_range_request_sha256="c" * 64, parent_price_range_bundle_id=parent_id,
        parent_price_range_manifest_file_sha256="d" * 64, package_id="pkg",
        manifest_sha256="e" * 64, style_profile_id="style", style_profile_hash="f" * 64,
        feature_schema_version="advisory_feature_schema_v1", feature_schema_hash="1" * 64,
        label_policy_version="advisory_price_range_label_policy_v1", split_sha256="2" * 64,
        parent_bundle_root=str(tmp_path / parent_id), features_artifact=artifact,
        price_range_labels_artifact=artifact, repository_root="/repo", repository_commit="3" * 40,
        created_at="2026-08-12T00:00:00+00:00",
    )


def test_pipeline_freezes_validation_spec_before_test_labels_are_read(
    tmp_path: Path, monkeypatch
) -> None:
    request = _request(tmp_path)
    request_path = tmp_path / "request.json"
    request.write_json(request_path)
    features = pd.DataFrame({
        "decision_as_of_trade_date": ["2026-01-02", "2026-01-03"],
        "target_trade_date": ["2026-01-03", "2026-01-04"],
        "instrument": ["000001.SZ", "000002.SZ"],
    })
    validation = pd.DataFrame([{**features.iloc[0].to_dict(), "split": "validation", "gap_modelable": True, "entry_gap_return": 0.03}])
    test = pd.DataFrame([{**features.iloc[1].to_dict(), "split": "test", "gap_modelable": True, "entry_gap_return": 0.04}])
    frozen = Path(request.output_root) / "price_range_calibration_runs" / request.request_id / "calibration_spec.json"
    order: list[str] = []

    def read_split(_path: str, split: str) -> pd.DataFrame:
        if split == "test":
            assert frozen.is_file()
            order.append("test_after_spec")
            return test
        order.append("validation")
        return validation

    monkeypatch.setattr(pipeline, "_verify_environment", lambda _request: None)
    monkeypatch.setattr(pipeline, "read_price_range_bundle_manifest", lambda *_a, **_k: {})
    monkeypatch.setattr(pipeline, "_validate_request", lambda *_a, **_k: None)
    monkeypatch.setattr(pipeline, "_validate_descriptor", lambda _descriptor: None)
    monkeypatch.setattr(pipeline, "_read_bound_parquet", lambda _descriptor: features)
    monkeypatch.setattr(pipeline, "_read_json", lambda path: {} if path.name == "feature_schema.json" else pipeline.json.loads(path.read_text()))
    monkeypatch.setattr(pipeline, "_read_split_labels", read_split)
    monkeypatch.setattr(pipeline, "_prepare_matrix_from_schema", lambda frame, **_k: frame)
    monkeypatch.setattr(pipeline, "_load_parent_quantile_models", lambda _root: {name: object() for name in ("entry_gap_q10", "entry_gap_q50", "entry_gap_q90")})
    monkeypatch.setattr(pipeline, "_predict_triplet", lambda _models, matrix: (np.full(len(matrix), -0.01), np.full(len(matrix), 0.01), np.full(len(matrix), 0.02)))
    monkeypatch.setattr(pipeline, "publish_calibrated_price_range_bundle", lambda **_k: ("4" * 64, tmp_path / "bundle", {"schema_version": "advisory_price_range_bundle_v2"}))
    monkeypatch.setattr(pipeline, "_peak_rss_bytes", lambda: 100)
    monkeypatch.setattr(pipeline.importlib.metadata, "version", lambda _name: "test-version")

    receipt = pipeline.run_price_range_calibration_pipeline(request_path)

    assert receipt["status"] == "calibrated"
    assert receipt["price_range_binding_activated"] is False
    assert order == ["validation", "test_after_spec"]
    assert receipt["metrics"]["validation"]["row_count"] == 1
    assert receipt["metrics"]["test"]["row_count"] == 1


def test_projection_rejects_non_validation_contamination_and_missing_features() -> None:
    feature = pd.DataFrame([{"decision_as_of_trade_date": "2026-01-02", "target_trade_date": "2026-01-03", "instrument": "000001.SZ"}])
    wrong = pd.DataFrame([{**feature.iloc[0].to_dict(), "split": "test", "gap_modelable": True, "entry_gap_return": 0.01}])
    with pytest.raises(AdvisoryModelFirstError) as contaminated:
        pipeline._project(features=feature, labels=wrong, split="validation")
    assert contaminated.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_PROJECTION_INVALID"

    missing = pd.DataFrame([{"decision_as_of_trade_date": "2026-01-04", "target_trade_date": "2026-01-05", "instrument": "000002.SZ", "split": "validation", "gap_modelable": True, "entry_gap_return": 0.01}])
    with pytest.raises(AdvisoryModelFirstError) as no_feature:
        pipeline._project(features=feature, labels=missing, split="validation")
    assert no_feature.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_PROJECTION_INVALID"

    invalid_boolean = wrong.copy()
    invalid_boolean["split"] = "validation"
    invalid_boolean["gap_modelable"] = "false"
    with pytest.raises(AdvisoryModelFirstError) as invalid:
        pipeline._project(features=feature, labels=invalid_boolean, split="validation")
    assert invalid.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_PROJECTION_INVALID"


def test_environment_rejects_windows_even_with_expected_conda(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
    monkeypatch.setenv("CONDA_DEFAULT_ENV", "rdagent-gpu")
    monkeypatch.setattr(pipeline.platform, "system", lambda: "Windows")
    monkeypatch.setattr(pipeline.platform, "release", lambda: "11")
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        pipeline._verify_environment(request)
    assert exc_info.value.reason_code == "ADVISORY_MODEL_TRAINING_REQUIRES_WSL"
