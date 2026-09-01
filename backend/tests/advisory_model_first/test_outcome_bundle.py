from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    FEATURE_SCHEMA_HASH,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_bundle import (
    publish_outcome_bundle,
    read_outcome_bundle_manifest,
)
from backend.services.advisory_model_first.outcome_contracts import (
    OutcomeInputArtifactV1,
    build_frozen_outcome_training_request,
)
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit
from backend.services.advisory_model_first.outcome_training import OutcomeTrainingResult


class _Model:
    def __init__(self, name: str) -> None:
        self.name = name

    def save_model(self, path: str) -> None:
        Path(path).write_text(f"model={self.name}\n", encoding="utf-8")


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
        package_id="package",
        manifest_sha256="d" * 64,
        style_profile_id="style",
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


def test_outcome_bundle_is_atomic_readable_and_tamper_evident(tmp_path: Path) -> None:
    date = pd.Timestamp("2024-01-02")
    split = OutcomeDateSplit((date,), (date,), (date,), (date,), (date,))
    training = OutcomeTrainingResult(
        models={"head_a": _Model("head_a"), "head_b": _Model("head_b")},
        feature_names=tuple(MODEL_FEATURE_COLUMNS),
        categorical_vocabulary={"l2_code_id": (1, 2)},
        metrics={"model_count": 2, "calibration_state": "UNCALIBRATED"},
        test_predictions=pd.DataFrame(
            {"decision_as_of_trade_date": [date], "instrument": ["000001.SZ"]}
        ),
        training_log={"evaluation_history": {}},
    )

    bundle_id, bundle_path, manifest = publish_outcome_bundle(
        model_root=tmp_path,
        request=_request(tmp_path),
        split=split,
        training=training,
        environment_report={"conda_environment": "rdagent-gpu"},
        resource_report={"peak_rss_bytes": 100},
    )

    assert bundle_path.name == bundle_id
    assert manifest["model_count"] == 2
    assert len(list((bundle_path / "models").glob("*.txt"))) == 2
    assert read_outcome_bundle_manifest(bundle_path, expected_bundle_id=bundle_id) == manifest

    (bundle_path / "models" / "head_a.txt").write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as error:
        read_outcome_bundle_manifest(bundle_path, expected_bundle_id=bundle_id)
    assert error.value.reason_code == "ADVISORY_OUTCOME_BUNDLE_INVALID"
