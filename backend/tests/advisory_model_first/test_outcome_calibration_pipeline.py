from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_calibration_contracts import (
    OutcomeCalibrationArtifactV1,
    build_frozen_outcome_calibration_request,
)
from backend.services.advisory_model_first import outcome_calibration_pipeline as pipeline


def test_pipeline_freezes_validation_spec_before_test_labels_are_read(
    tmp_path: Path, monkeypatch
) -> None:
    parent_id = "a" * 64
    artifact = OutcomeCalibrationArtifactV1(
        path=str(tmp_path / "labels.parquet"),
        sha256="b" * 64,
        size_bytes=10,
        row_count=2,
        columns=("decision_as_of_trade_date", "target_trade_date", "instrument", "split"),
    )
    request = build_frozen_outcome_calibration_request(
        output_root=str(tmp_path / "output"),
        parent_outcome_request_id="advoutreq_parent",
        parent_outcome_request_sha256="c" * 64,
        parent_outcome_bundle_id=parent_id,
        parent_outcome_manifest_file_sha256="d" * 64,
        package_id="pkg",
        manifest_sha256="e" * 64,
        style_profile_id="style",
        style_profile_hash="f" * 64,
        feature_schema_version="advisory_feature_schema_v1",
        feature_schema_hash="1" * 64,
        label_policy_version="advisory_outcome_label_policy_v1",
        split_sha256="2" * 64,
        parent_bundle_root=str(tmp_path / parent_id),
        features_artifact=artifact,
        outcome_labels_artifact=artifact,
        repository_root="/repo",
        repository_commit="3" * 40,
        created_at="2026-08-12T00:00:00+00:00",
    )
    request_path = tmp_path / "request.json"
    request.write_json(request_path)
    keys = {
        "decision_as_of_trade_date": ["2026-01-02", "2026-01-03"],
        "target_trade_date": ["2026-01-03", "2026-01-04"],
        "instrument": ["000001.SZ", "000002.SZ"],
    }
    features = pd.DataFrame(keys)
    validation_labels = pd.DataFrame(
        {
            **{key: [value[0], f"missing-{key}"] for key, value in keys.items()},
            "split": ["validation", "validation"],
        }
    )
    test_labels = pd.DataFrame(
        {
            **{key: [value[1], f"missing-{key}"] for key, value in keys.items()},
            "split": ["test", "test"],
        }
    )
    frozen_path = (
        Path(request.output_root)
        / "outcome_calibration_runs"
        / request.request_id
        / "calibration.json"
    )
    read_order: list[str] = []

    def fake_read_parquet(_path, *, filters):  # type: ignore[no-untyped-def]
        split = str(filters[0][2])
        if split == "test":
            assert frozen_path.is_file()
            read_order.append("test_after_frozen_spec")
            return test_labels
        read_order.append("validation")
        return validation_labels

    spec = {
        "schema_version": "advisory_outcome_calibration_spec_v1",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "calibration_policy_version": "advisory_outcome_calibration_policy_v1",
        "binary_heads": {},
        "return_intervals": {},
        "path_upper": {},
        "holding_calibration_state": "UNCALIBRATED",
    }
    monkeypatch.setattr(pipeline, "_verify_environment", lambda _request: None)
    monkeypatch.setattr(pipeline, "read_outcome_bundle_manifest", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline, "_validate_request_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "_read_bound_parquet", lambda _descriptor: features)
    monkeypatch.setattr(pipeline, "_validate_parquet_descriptor", lambda _descriptor: None)
    monkeypatch.setattr(pipeline, "_read_json", lambda _path: {} if _path.name == "feature_schema.json" else pipeline.json.loads(_path.read_text()))
    monkeypatch.setattr(pipeline.pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(pipeline, "_prepare_matrix_from_schema", lambda frame, **_kwargs: frame)
    monkeypatch.setattr(pipeline, "_load_parent_models", lambda _root: {})
    monkeypatch.setattr(
        pipeline,
        "_fit_validation",
        lambda **kwargs: (spec, kwargs["merged"].loc[:, list(pipeline.KEYS)]),
    )
    monkeypatch.setattr(
        pipeline,
        "_evaluate_projection",
        lambda **kwargs: (
            {"row_count": len(kwargs["merged"]), "heads": {}},
            kwargs["merged"].loc[:, list(pipeline.KEYS)],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "publish_calibrated_outcome_bundle",
        lambda **_kwargs: (
            "4" * 64,
            tmp_path / "bundle",
            {"schema_version": "advisory_outcome_bundle_v2"},
        ),
    )
    monkeypatch.setattr(pipeline, "_peak_rss_bytes", lambda: 100)
    monkeypatch.setattr(pipeline.importlib.metadata, "version", lambda _name: "test-version")

    receipt = pipeline.run_outcome_calibration_pipeline(request_path)

    assert receipt["status"] == "calibrated"
    assert read_order == ["validation", "test_after_frozen_spec"]
    assert receipt["metrics"]["projection_counts"] == {
        "validation": {
            "label_row_count": 2,
            "feature_covered_row_count": 1,
            "missing_feature_row_count": 1,
        },
        "test": {
            "label_row_count": 2,
            "feature_covered_row_count": 1,
            "missing_feature_row_count": 1,
        },
    }


def test_feature_covered_projection_rejects_duplicate_feature_identity() -> None:
    key = {
        "decision_as_of_trade_date": "2026-01-02",
        "target_trade_date": "2026-01-03",
        "instrument": "000001.SZ",
    }
    features = pd.DataFrame([key, key])
    labels = pd.DataFrame([{**key, "split": "validation"}])

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        pipeline._feature_covered_projection(
            features=features,
            labels=labels,
            projection_name="validation",
        )

    assert exc_info.value.reason_code == "ADVISORY_OUTCOME_CALIBRATION_FAILED"
    assert "not one-to-one" in str(exc_info.value)


def test_feature_covered_projection_rejects_empty_authoritative_overlap() -> None:
    features = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": "2026-01-02",
                "target_trade_date": "2026-01-03",
                "instrument": "000001.SZ",
            }
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": "2026-01-04",
                "target_trade_date": "2026-01-05",
                "instrument": "000002.SZ",
                "split": "validation",
            }
        ]
    )

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        pipeline._feature_covered_projection(
            features=features,
            labels=labels,
            projection_name="validation",
        )

    assert exc_info.value.reason_code == "ADVISORY_OUTCOME_CALIBRATION_FAILED"
    assert "no common rows" in str(exc_info.value)
