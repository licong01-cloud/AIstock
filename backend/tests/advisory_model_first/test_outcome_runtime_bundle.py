from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    FEATURE_SCHEMA_HASH,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_bundle import publish_outcome_bundle
from backend.services.advisory_model_first.outcome_contracts import (
    OutcomeInputArtifactV1,
    build_frozen_outcome_training_request,
)
from backend.services.advisory_model_first.outcome_runtime_bundle import (
    _validate_outcome_runtime_manifest,
    expected_outcome_model_names,
    load_exact_outcome_bundle,
    outcome_binding_path,
    publish_outcome_binding,
)
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit
from backend.services.advisory_model_first.outcome_training import OutcomeTrainingResult


class _SavedModel:
    def __init__(self, name: str) -> None:
        self.name = name

    def save_model(self, path: str) -> None:
        Path(path).write_text(f"model={self.name}\n", encoding="utf-8")


class _LoadedModel:
    def __init__(self, path: Path) -> None:
        self.path = path

    def feature_name(self) -> list[str]:
        return list(MODEL_FEATURE_COLUMNS)


def _request(tmp_path: Path):
    artifact = OutcomeInputArtifactV1(
        path="/data/input.parquet",
        sha256="a" * 64,
        size_bytes=10,
        row_count=1,
        columns=("instrument",),
    )
    return build_frozen_outcome_training_request(
        parent_request_id="advmreq_parent",
        parent_request_sha256="b" * 64,
        parent_bundle_id="c" * 64,
        parent_bundle_manifest_file_sha256="9" * 64,
        package_id="pkg_runtime",
        manifest_sha256="d" * 64,
        style_profile_id="style_runtime",
        style_profile_hash="e" * 64,
        feature_schema_version="advisory_feature_schema_v1",
        feature_schema_hash=FEATURE_SCHEMA_HASH,
        candidate_semantics_id="candidate-v1",
        candidates_artifact=artifact,
        features_artifact=artifact,
        parent_test_predictions_artifact=artifact,
        parent_training_request_path="/data/request.json",
        parent_feature_schema_path="/data/schema.json",
        qlib_daily_root="/data/qlib",
        suspend_data_root="/data/suspend",
        repository_root="/repo",
        repository_commit="1" * 40,
        output_root=str(tmp_path),
        created_at="2026-08-09T00:00:00Z",
    )


def test_outcome_binding_loads_exact_46_head_bundle(tmp_path: Path) -> None:
    date = pd.Timestamp("2024-01-02")
    split = OutcomeDateSplit((date,), (date,), (date,), (date,), (date,))
    models = {name: _SavedModel(name) for name in expected_outcome_model_names()}
    training = OutcomeTrainingResult(
        models=models,
        feature_names=tuple(MODEL_FEATURE_COLUMNS),
        categorical_vocabulary={"l2_code_id": (1, 2)},
        metrics={"model_count": len(models), "calibration_state": "UNCALIBRATED"},
        test_predictions=pd.DataFrame(
            {"decision_as_of_trade_date": [date], "instrument": ["000001.SZ"]}
        ),
        training_log={"evaluation_history": {}},
    )
    bundle_id, _bundle_path, _manifest = publish_outcome_bundle(
        model_root=tmp_path,
        request=_request(tmp_path),
        split=split,
        training=training,
        environment_report={"conda_environment": "rdagent-gpu"},
        resource_report={"peak_rss_bytes": 100},
    )
    binding_path = publish_outcome_binding(
        model_root=tmp_path,
        outcome_bundle_id=bundle_id,
        activated_at="2026-08-09T00:00:00+00:00",
    )

    loaded = load_exact_outcome_bundle(
        model_root=tmp_path,
        package_id="pkg_runtime",
        manifest_sha256="d" * 64,
        style_profile_hash="e" * 64,
        parent_bundle_id="c" * 64,
        booster_factory=_LoadedModel,
    )

    assert binding_path.is_file()
    assert loaded.outcome_bundle_id == bundle_id
    assert set(loaded.models) == set(expected_outcome_model_names())
    assert len(loaded.models) == 46

    with pytest.raises(AdvisoryModelFirstError) as error:
        load_exact_outcome_bundle(
            model_root=tmp_path,
            package_id="pkg_runtime",
            manifest_sha256="d" * 64,
            style_profile_hash="e" * 64,
            parent_bundle_id="f" * 64,
            booster_factory=_LoadedModel,
        )
    assert error.value.reason_code == "ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH"


def test_outcome_binding_path_rejects_package_escape(tmp_path: Path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        outcome_binding_path(
            tmp_path,
            package_id="../outside",
            manifest_sha256="d" * 64,
            style_profile_hash="e" * 64,
        )
    assert error.value.reason_code == "ADVISORY_OUTCOME_BUNDLE_INVALID"


def test_outcome_runtime_manifest_rejects_wrong_horizon_contract() -> None:
    manifest = {
        "schema_version": "advisory_outcome_bundle_v1",
        "status": "EXPERIMENTAL_SHADOW",
        "calibration_state": "UNCALIBRATED",
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_outcome_label_policy_v1",
        "horizons": [1, 5],
        "quantiles": [0.1, 0.5, 0.9],
        "model_count": 46,
    }
    with pytest.raises(AdvisoryModelFirstError) as error:
        _validate_outcome_runtime_manifest(manifest)
    assert error.value.reason_code == "ADVISORY_OUTCOME_BUNDLE_INVALID"
