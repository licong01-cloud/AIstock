from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH, MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.outcome_bundle import publish_outcome_bundle, read_outcome_bundle_manifest
from backend.services.advisory_model_first.outcome_calibration_bundle import publish_calibrated_outcome_bundle
from backend.services.advisory_model_first.outcome_calibration_contracts import (
    OutcomeCalibrationArtifactV1,
    build_frozen_outcome_calibration_request,
)
from backend.services.advisory_model_first.outcome_contracts import OutcomeInputArtifactV1, build_frozen_outcome_training_request
from backend.services.advisory_model_first.outcome_runtime_bundle import (
    expected_outcome_model_names,
    load_exact_outcome_bundle,
    publish_outcome_binding,
)
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit
from backend.services.advisory_model_first.outcome_training import OutcomeTrainingResult
from backend.services.advisory_model_first.prediction_source import sha256_file


class _Model:
    def __init__(self, name: str) -> None:
        self.name = name

    def save_model(self, path: str) -> None:
        Path(path).write_text(f"model={self.name}\n", encoding="utf-8")


class _LoadedModel:
    def __init__(self, path: Path) -> None:
        self.path = path

    def feature_name(self) -> list[str]:
        return list(MODEL_FEATURE_COLUMNS)


def _parent_bundle(tmp_path: Path) -> tuple[str, Path, dict]:
    input_artifact = OutcomeInputArtifactV1(
        path="/data/input.parquet",
        sha256="a" * 64,
        size_bytes=10,
        row_count=1,
        columns=("instrument",),
    )
    request = build_frozen_outcome_training_request(
        parent_request_id="advmreq_parent",
        parent_request_sha256="b" * 64,
        parent_bundle_id="c" * 64,
        parent_bundle_manifest_file_sha256="9" * 64,
        package_id="pkg_calibration",
        manifest_sha256="d" * 64,
        style_profile_id="style_calibration",
        style_profile_hash="e" * 64,
        feature_schema_version="advisory_feature_schema_v1",
        feature_schema_hash=FEATURE_SCHEMA_HASH,
        candidate_semantics_id="candidate-v1",
        candidates_artifact=input_artifact,
        features_artifact=input_artifact,
        parent_test_predictions_artifact=input_artifact,
        parent_training_request_path="/data/request.json",
        parent_feature_schema_path="/data/schema.json",
        qlib_daily_root="/data/qlib",
        suspend_data_root="/data/suspend",
        repository_root="/repo",
        repository_commit="1" * 40,
        output_root=str(tmp_path),
        created_at="2026-08-09T00:00:00Z",
    )
    date = pd.Timestamp("2024-01-02")
    split = OutcomeDateSplit((date,), (date,), (date,), (date,), (date,))
    models = {name: _Model(name) for name in expected_outcome_model_names()}
    training = OutcomeTrainingResult(
        models=models,
        feature_names=tuple(MODEL_FEATURE_COLUMNS),
        categorical_vocabulary={"l2_code_id": (1, 2)},
        metrics={"model_count": len(models)},
        test_predictions=pd.DataFrame({"decision_as_of_trade_date": [date], "instrument": ["000001.SZ"]}),
        training_log={},
    )
    return publish_outcome_bundle(
        model_root=tmp_path,
        request=request,
        split=split,
        training=training,
        environment_report={"conda_environment": "rdagent-gpu"},
        resource_report={"peak_rss_bytes": 100},
    )


def _calibration_request(tmp_path: Path, parent_id: str, parent_path: Path, parent_manifest: dict):
    artifact = OutcomeCalibrationArtifactV1(
        path="/data/input.parquet",
        sha256="f" * 64,
        size_bytes=10,
        row_count=20,
        columns=("split", "instrument"),
    )
    return build_frozen_outcome_calibration_request(
        output_root=str(tmp_path),
        parent_outcome_request_id=parent_manifest["request_id"],
        parent_outcome_request_sha256=parent_manifest["request_sha256"],
        parent_outcome_bundle_id=parent_id,
        parent_outcome_manifest_file_sha256=sha256_file(parent_path / "manifest.json"),
        package_id=parent_manifest["package_id"],
        manifest_sha256=parent_manifest["manifest_sha256"],
        style_profile_id=parent_manifest["style_profile_id"],
        style_profile_hash=parent_manifest["style_profile_hash"],
        feature_schema_version=parent_manifest["feature_schema_version"],
        feature_schema_hash=parent_manifest["feature_schema_hash"],
        label_policy_version=parent_manifest["label_policy_version"],
        split_sha256=sha256_file(parent_path / "split.json"),
        parent_bundle_root=str(parent_path),
        features_artifact=artifact,
        outcome_labels_artifact=artifact,
        repository_root="/repo",
        repository_commit="2" * 40,
        created_at="2026-08-12T00:00:00Z",
    )


def _spec(request_id: str, request_sha256: str) -> dict:
    solver = {
        "library": "scikit-learn",
        "estimator": "LogisticRegression",
        "penalty": None,
        "solver": "lbfgs",
        "fit_intercept": True,
        "max_iter": 1000,
        "random_state": 20260812,
        "library_version": "1.7.2",
    }
    return {
        "schema_version": "advisory_outcome_calibration_spec_v1",
        "request_id": request_id,
        "request_sha256": request_sha256,
        "calibration_policy_version": "advisory_outcome_calibration_policy_v1",
        "validation_projection_hash": "7" * 64,
        "binary_heads": {
            f"{family}_h{horizon}": {
                "state": "CALIBRATED",
                "head": f"{family}_h{horizon}",
                "row_count": 20,
                "positive_count": 8,
                "negative_count": 12,
                "coefficient": 1.0,
                "intercept": 0.0,
                "reason_code": None,
                "solver": solver,
                "iteration_count": 4,
                "convergence_state": "CONVERGED",
                "validation_metrics": {"raw": {}, "calibrated": {}},
            }
            for horizon in (1, 3, 5, 10, 20)
            for family in ("positive_excess", "signal_survival")
        },
        "return_intervals": {
            f"excess_return_h{horizon}": {
                "state": "CALIBRATED",
                "method": "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION",
                "nominal_coverage": 0.8,
                "delta": 0.01,
            }
            for horizon in (1, 3, 5, 10, 20)
        },
        "path_upper": {
            f"{family}_h{horizon}": {
                "state": "CALIBRATED",
                "method": "CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION",
                "nominal_coverage": 0.9,
                "delta": 0.02,
            }
            for horizon in (1, 3, 5, 10, 20)
            for family in ("path_mfe", "path_mae_loss")
        },
        "holding_calibration_state": "UNCALIBRATED",
    }


def test_v2_bundle_is_self_contained_exact_retry_and_tamper_evident(tmp_path: Path) -> None:
    parent_id, parent_path, parent_manifest = _parent_bundle(tmp_path)
    request = _calibration_request(tmp_path, parent_id, parent_path, parent_manifest)
    run_root = tmp_path / "run"
    run_root.mkdir()
    spec_path = run_root / "calibration.json"
    spec_path.write_text(json.dumps(_spec(request.request_id, request.request_sha256)), encoding="utf-8")
    predictions = pd.DataFrame({"instrument": ["000001.SZ"], "split": ["validation"]})

    first = publish_calibrated_outcome_bundle(
        request=request,
        calibration_spec_path=spec_path,
        metrics={"validation": {}, "test": {}},
        validation_predictions=predictions,
        test_predictions=predictions.assign(split="test"),
        calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
    )
    second = publish_calibrated_outcome_bundle(
        request=request,
        calibration_spec_path=spec_path,
        metrics={"validation": {}, "test": {}},
        validation_predictions=predictions,
        test_predictions=predictions.assign(split="test"),
        calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
    )

    bundle_id, bundle_path, manifest = first
    assert second[0] == bundle_id
    assert manifest["schema_version"] == "advisory_outcome_bundle_v2"
    assert manifest["calibration_state"] == "PARTIAL"
    assert manifest["holding_calibration_state"] == "UNCALIBRATED"
    assert len(list((bundle_path / "models").glob("*.txt"))) == 46
    for name in expected_outcome_model_names():
        assert sha256_file(bundle_path / "models" / f"{name}.txt") == sha256_file(parent_path / "models" / f"{name}.txt")
    assert read_outcome_bundle_manifest(bundle_path, expected_bundle_id=bundle_id) == manifest

    (bundle_path / "calibration.json").write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as error:
        read_outcome_bundle_manifest(bundle_path, expected_bundle_id=bundle_id)
    assert error.value.reason_code == "ADVISORY_OUTCOME_BUNDLE_INVALID"


def test_v2_bundle_accepts_explicit_order_reversal_uncalibrated_head(tmp_path: Path) -> None:
    parent_id, parent_path, parent_manifest = _parent_bundle(tmp_path)
    request = _calibration_request(tmp_path, parent_id, parent_path, parent_manifest)
    run_root = tmp_path / "run"
    run_root.mkdir()
    spec = _spec(request.request_id, request.request_sha256)
    spec["binary_heads"]["positive_excess_h5"] = {
        "state": "UNCALIBRATED",
        "head": "positive_excess_h5",
        "row_count": 20,
        "positive_count": 8,
        "negative_count": 12,
        "coefficient": None,
        "intercept": None,
        "reason_code": "ADVISORY_OUTCOME_CALIBRATION_ORDER_REVERSAL",
        "solver": spec["binary_heads"]["positive_excess_h1"]["solver"],
        "iteration_count": 4,
        "convergence_state": "CONVERGED_ORDER_REVERSAL",
        "validation_metrics": {"raw": {}, "calibrated": None},
    }
    spec_path = run_root / "calibration.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    predictions = pd.DataFrame({"instrument": ["000001.SZ"], "split": ["validation"]})

    bundle_id, bundle_path, manifest = publish_calibrated_outcome_bundle(
        request=request,
        calibration_spec_path=spec_path,
        metrics={"validation": {}, "test": {}},
        validation_predictions=predictions,
        test_predictions=predictions.assign(split="test"),
        calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
    )

    assert manifest["calibration_state"] == "PARTIAL"
    loaded = json.loads((bundle_path / "calibration.json").read_text(encoding="utf-8"))
    assert loaded["binary_heads"]["positive_excess_h5"]["reason_code"] == (
        "ADVISORY_OUTCOME_CALIBRATION_ORDER_REVERSAL"
    )
    assert read_outcome_bundle_manifest(bundle_path, expected_bundle_id=bundle_id) == manifest


@pytest.mark.parametrize("coefficient", [0.0, -0.25])
def test_v2_bundle_rejects_calibrated_non_positive_platt_slope(
    tmp_path: Path,
    coefficient: float,
) -> None:
    parent_id, parent_path, parent_manifest = _parent_bundle(tmp_path)
    request = _calibration_request(tmp_path, parent_id, parent_path, parent_manifest)
    run_root = tmp_path / "run"
    run_root.mkdir()
    spec = _spec(request.request_id, request.request_sha256)
    spec["binary_heads"]["positive_excess_h5"]["coefficient"] = coefficient
    spec_path = run_root / "calibration.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    predictions = pd.DataFrame({"instrument": ["000001.SZ"], "split": ["validation"]})

    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_calibrated_outcome_bundle(
            request=request,
            calibration_spec_path=spec_path,
            metrics={"validation": {}, "test": {}},
            validation_predictions=predictions,
            test_predictions=predictions.assign(split="test"),
            calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
        )

    assert error.value.reason_code == "ADVISORY_OUTCOME_CALIBRATION_BUNDLE_INVALID"
    assert "invalid Platt parameters" in str(error.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("solver", None),
        ("iteration_count", 0),
        ("convergence_state", "UNKNOWN"),
        ("head", "signal_survival_h5"),
        ("row_count", 19),
    ],
)
def test_v2_bundle_rejects_incomplete_or_inconsistent_platt_evidence(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    parent_id, parent_path, parent_manifest = _parent_bundle(tmp_path)
    request = _calibration_request(tmp_path, parent_id, parent_path, parent_manifest)
    run_root = tmp_path / "run"
    run_root.mkdir()
    spec = _spec(request.request_id, request.request_sha256)
    spec["binary_heads"]["positive_excess_h5"][field] = value
    spec_path = run_root / "calibration.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    predictions = pd.DataFrame({"instrument": ["000001.SZ"], "split": ["validation"]})

    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_calibrated_outcome_bundle(
            request=request,
            calibration_spec_path=spec_path,
            metrics={"validation": {}, "test": {}},
            validation_predictions=predictions,
            test_predictions=predictions.assign(split="test"),
            calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
        )

    assert error.value.reason_code == "ADVISORY_OUTCOME_CALIBRATION_BUNDLE_INVALID"


def test_v2_bundle_publishes_exact_binding_and_loads_calibration(tmp_path: Path) -> None:
    parent_id, parent_path, parent_manifest = _parent_bundle(tmp_path)
    request = _calibration_request(tmp_path, parent_id, parent_path, parent_manifest)
    run_root = tmp_path / "run"
    run_root.mkdir()
    spec_path = run_root / "calibration.json"
    spec = _spec(request.request_id, request.request_sha256)
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    predictions = pd.DataFrame({"instrument": ["000001.SZ"], "split": ["validation"]})
    bundle_id, _bundle_path, _manifest = publish_calibrated_outcome_bundle(
        request=request,
        calibration_spec_path=spec_path,
        metrics={"validation": {}, "test": {}},
        validation_predictions=predictions,
        test_predictions=predictions.assign(split="test"),
        calibration_log={"environment": {"conda_environment": "rdagent-gpu"}},
    )

    publish_outcome_binding(
        model_root=tmp_path,
        outcome_bundle_id=bundle_id,
        activated_at="2026-08-12T00:00:00+00:00",
    )
    loaded = load_exact_outcome_bundle(
        model_root=tmp_path,
        package_id=parent_manifest["package_id"],
        manifest_sha256=parent_manifest["manifest_sha256"],
        style_profile_hash=parent_manifest["style_profile_hash"],
        parent_bundle_id=parent_manifest["parent_bundle_id"],
        booster_factory=_LoadedModel,
    )

    assert loaded.outcome_bundle_id == bundle_id
    assert loaded.manifest["schema_version"] == "advisory_outcome_bundle_v2"
    assert loaded.calibration == spec
    assert len(loaded.models) == 46
