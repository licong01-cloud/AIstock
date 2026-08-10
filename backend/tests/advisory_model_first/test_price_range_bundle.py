from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    FEATURE_SCHEMA_HASH,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit
from backend.services.advisory_model_first.price_range_bundle import (
    publish_price_range_bundle,
    read_price_range_bundle_manifest,
)
from backend.services.advisory_model_first.price_range_contracts import (
    PRICE_RANGE_MODEL_NAMES,
    PriceRangeInputArtifactV1,
    build_frozen_price_range_training_request,
    canonical_json_sha256,
)
from backend.services.advisory_model_first.price_range_training import (
    PriceRangeTrainingResult,
)


class _Model:
    def __init__(self, name: str) -> None:
        self.name = name

    def save_model(self, path: str) -> None:
        Path(path).write_text(f"model={self.name}\n", encoding="utf-8")


class _EmptyModel:
    def save_model(self, path: str) -> None:
        Path(path).write_bytes(b"")


def _request(tmp_path: Path):
    artifact = PriceRangeInputArtifactV1(
        path="/data/input.parquet",
        sha256="a" * 64,
        size_bytes=10,
        row_count=1,
        columns=("instrument",),
    )
    return build_frozen_price_range_training_request(
        parent_request_id="advmreq_parent",
        parent_request_sha256="b" * 64,
        parent_bundle_id="c" * 64,
        parent_bundle_manifest_file_sha256="d" * 64,
        outcome_request_id="advoutreq_parent",
        outcome_request_sha256="e" * 64,
        outcome_bundle_id="f" * 64,
        outcome_bundle_manifest_file_sha256="1" * 64,
        package_id="package",
        manifest_sha256="2" * 64,
        style_profile_id="style",
        style_profile_hash="3" * 64,
        feature_schema_version="advisory_feature_schema_v1",
        feature_schema_hash=FEATURE_SCHEMA_HASH,
        candidate_semantics_id="candidate-v1",
        candidates_artifact=artifact,
        features_artifact=artifact,
        parent_training_request_path="/data/parent/training_request.json",
        parent_feature_schema_path="/data/parent/feature_schema.json",
        outcome_training_request_path="/data/outcome/training_request.json",
        outcome_split_path="/data/outcome/split.json",
        qlib_daily_root="/data/qlib",
        suspend_data_root="/data/suspend",
        repository_root="/repo",
        repository_commit="4" * 40,
        output_root=str(tmp_path),
        created_at="2026-08-10T00:00:00Z",
    )


def _split() -> OutcomeDateSplit:
    dates = pd.bdate_range("2024-01-02", periods=406)
    return OutcomeDateSplit(
        train=tuple(dates[:226]),
        purge_1=tuple(dates[226:251]),
        validation=tuple(dates[251:301]),
        purge_2=tuple(dates[301:326]),
        test=tuple(dates[326:]),
    )


def _training(split: OutcomeDateSplit) -> PriceRangeTrainingResult:
    test_dates = pd.DatetimeIndex(split.test)
    row_count = len(test_dates) * 20
    decision_dates = test_dates.repeat(20)
    return PriceRangeTrainingResult(
        models={name: _Model(name) for name in PRICE_RANGE_MODEL_NAMES},
        feature_names=tuple(MODEL_FEATURE_COLUMNS),
        categorical_vocabulary={"l2_code_id": (1, 2)},
        metrics={
            "model_count": 4,
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "test_row_count": row_count,
            "test_date_count": 80,
            "heads": {
                "entry_executable_probability": {
                    "row_count": row_count,
                    "best_iteration": 2,
                },
                **{
                    name: {
                        "row_count": row_count,
                        "best_iteration": 2,
                        "condition": "ENTRY_EXECUTABLE",
                    }
                    for name in PRICE_RANGE_MODEL_NAMES[1:]
                },
            },
            "entry_gap_distribution": {"condition": "ENTRY_EXECUTABLE"},
        },
        test_predictions=pd.DataFrame(
            {
                "decision_as_of_trade_date": decision_dates,
                "target_trade_date": decision_dates + pd.offsets.BDay(1),
                "instrument": [
                    f"{rank:06d}.SZ" for _date in test_dates for rank in range(1, 21)
                ],
                "selection_effective_rank": list(range(1, 21)) * len(test_dates),
                "parent_combined_score": 0.5,
                "entry_label_status": "AVAILABLE",
                "entry_label_reason": "target_open_executable",
                "entry_executable": 1,
                "entry_gap_return": 0.01,
                "entry_executable_probability": 0.9,
                "entry_gap_q10": -0.01,
                "entry_gap_q50": 0.01,
                "entry_gap_q90": 0.03,
                "entry_gap_condition": "ENTRY_EXECUTABLE",
            }
        ),
        training_log={
            "evaluation_history": {name: {} for name in PRICE_RANGE_MODEL_NAMES}
        },
    )


def test_price_range_bundle_is_atomic_exact_and_tamper_evident(tmp_path: Path) -> None:
    split = _split()
    bundle_id, bundle_path, manifest = publish_price_range_bundle(
        model_root=tmp_path,
        request=_request(tmp_path),
        split=split,
        training=_training(split),
        environment_report={
            "conda_environment": "rdagent-gpu",
            "lightgbm_version": "4.0",
            "pyarrow_version": "20.0",
        },
        resource_report={"peak_rss_bytes": 100, "limit_bytes": 8 * 1024**3},
    )

    assert bundle_path.name == bundle_id
    assert manifest["model_count"] == 4
    assert manifest["entry_gap_condition"] == "ENTRY_EXECUTABLE"
    assert set(path.stem for path in (bundle_path / "models").glob("*.txt")) == set(
        PRICE_RANGE_MODEL_NAMES
    )
    assert read_price_range_bundle_manifest(bundle_path, expected_bundle_id=bundle_id) == manifest

    (bundle_path / "models" / "entry_gap_q50.txt").write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as error:
        read_price_range_bundle_manifest(bundle_path, expected_bundle_id=bundle_id)
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_INVALID"


def test_price_range_bundle_rejects_subset_model_delivery(tmp_path: Path) -> None:
    split = _split()
    training = _training(split)
    training.models.pop("entry_gap_q90")

    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_price_range_bundle(
            model_root=tmp_path,
            request=_request(tmp_path),
            split=split,
            training=training,
            environment_report={
                "conda_environment": "rdagent-gpu",
                "lightgbm_version": "4.0",
                "pyarrow_version": "20.0",
            },
            resource_report={"peak_rss_bytes": 100, "limit_bytes": 8 * 1024**3},
        )
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_INVALID"


def test_price_range_bundle_rejects_empty_model_file(tmp_path: Path) -> None:
    split = _split()
    training = _training(split)
    training.models["entry_gap_q50"] = _EmptyModel()

    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_price_range_bundle(
            model_root=tmp_path,
            request=_request(tmp_path),
            split=split,
            training=training,
            environment_report={
                "conda_environment": "rdagent-gpu",
                "lightgbm_version": "4.0",
                "pyarrow_version": "20.0",
            },
            resource_report={"peak_rss_bytes": 100, "limit_bytes": 8 * 1024**3},
        )
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_INVALID"


def test_price_range_bundle_rejects_semantically_invalid_rehashed_predictions(
    tmp_path: Path,
) -> None:
    split = _split()
    bundle_id, bundle_path, _manifest = publish_price_range_bundle(
        model_root=tmp_path,
        request=_request(tmp_path),
        split=split,
        training=_training(split),
        environment_report={
            "conda_environment": "rdagent-gpu",
            "lightgbm_version": "4.0",
            "pyarrow_version": "20.0",
        },
        resource_report={"peak_rss_bytes": 100, "limit_bytes": 8 * 1024**3},
    )
    predictions_path = bundle_path / "test_predictions.parquet"
    predictions = pd.read_parquet(predictions_path)
    predictions.loc[0, "entry_gap_q10"] = 0.10
    predictions.to_parquet(predictions_path, index=False)
    manifest_path = bundle_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["test_predictions.parquet"] = {
        "sha256": hashlib.sha256(predictions_path.read_bytes()).hexdigest(),
        "size_bytes": predictions_path.stat().st_size,
    }
    payload = {key: value for key, value in manifest.items() if key != "price_range_bundle_id"}
    replacement_id = canonical_json_sha256(payload)
    manifest["price_range_bundle_id"] = replacement_id
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    assert replacement_id != bundle_id
    with pytest.raises(AdvisoryModelFirstError) as error:
        read_price_range_bundle_manifest(bundle_path, expected_bundle_id=replacement_id)
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_INVALID"
