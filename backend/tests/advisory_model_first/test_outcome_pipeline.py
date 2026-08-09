from __future__ import annotations

from pathlib import Path
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.outcome_contracts import (
    OutcomeInputArtifactV1,
    canonical_json_sha256,
)
from backend.services.advisory_model_first.outcome_pipeline import (
    _read_and_validate_parent_bundle,
    _read_bound_parquet,
    _complete_parent_model_test_rankings,
    _write_json,
)
from backend.services.advisory_model_first.prediction_source import sha256_file


def test_outcome_parquet_readback_requires_exact_frozen_identity(tmp_path: Path) -> None:
    path = tmp_path / "features.parquet"
    frame = pd.DataFrame({"instrument": ["000001.SZ"], "score": [0.5]})
    frame.to_parquet(path, index=False)
    descriptor = OutcomeInputArtifactV1(
        path=str(path),
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_count=1,
        columns=tuple(frame.columns),
    )

    pd.testing.assert_frame_equal(_read_bound_parquet(descriptor), frame)
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(AdvisoryModelFirstError) as error:
        _read_bound_parquet(descriptor)
    assert error.value.reason_code == "ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH"


def test_outcome_json_writer_serializes_only_supported_temporal_and_numpy_values(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipt.json"
    _write_json(path, {"date": pd.Timestamp("2026-01-02"), "count": np.int64(3)})
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "date": "2026-01-02T00:00:00",
        "count": 3,
    }

    with pytest.raises(TypeError, match="unsupported outcome JSON value"):
        _write_json(path, {"invalid": object()})


def test_parent_bundle_validator_closes_canonical_and_member_identity(tmp_path: Path) -> None:
    schema_path = tmp_path / "feature_schema.json"
    schema_path.write_text('{"feature_schema_hash":"abc"}', encoding="utf-8")
    payload = {
        "schema_version": "advisory_model_bundle_v1",
        "files": {
            "feature_schema.json": {
                "sha256": sha256_file(schema_path),
                "size_bytes": schema_path.stat().st_size,
            }
        },
    }
    bundle_id = canonical_json_sha256(payload)
    manifest = {"bundle_id": bundle_id, **payload}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    read_manifest, schema = _read_and_validate_parent_bundle(
        tmp_path,
        expected_bundle_id=bundle_id,
        expected_manifest_file_sha256=sha256_file(manifest_path),
    )
    assert read_manifest == manifest
    assert schema == {"feature_schema_hash": "abc"}

    schema_path.write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as error:
        _read_and_validate_parent_bundle(
            tmp_path,
            expected_bundle_id=bundle_id,
            expected_manifest_file_sha256=sha256_file(manifest_path),
        )
    assert error.value.reason_code == "ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH"


def test_parent_model_ranking_replays_complete_test_group_and_closes_frozen_scores(
    tmp_path: Path,
) -> None:
    dates = (pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-05"))
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": [dates[0], dates[0], dates[1], dates[1]],
            "target_trade_date": pd.to_datetime(
                ["2026-01-05", "2026-01-05", "2026-01-06", "2026-01-06"]
            ),
            "instrument": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
            **{
                column: np.arange(4, dtype=float) + position
                for position, column in enumerate(MODEL_FEATURE_COLUMNS)
            },
        }
    )
    features["l2_code_id"] = [1, 2, 1, 2]
    frozen = features.loc[[0, 1, 3], [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
    ]].copy()
    frozen["advisory_model_score"] = [0.2, 0.9, 0.3]

    class _Booster:
        def feature_name(self):
            return list(MODEL_FEATURE_COLUMNS)

        def predict(self, matrix):
            assert len(matrix) == 4
            return np.asarray([0.2, 0.9, 0.1, 0.3])

    rankings = _complete_parent_model_test_rankings(
        features=features,
        test_dates=dates,
        parent_bundle_path=tmp_path,
        parent_feature_schema={"categorical_vocabulary": {"l2_code_id": [1, 2]}},
        frozen_parent_test_predictions=frozen,
        booster_factory=lambda _path: _Booster(),
    )

    assert len(rankings) == 4
    first_date = rankings[rankings["decision_as_of_trade_date"] == dates[0]]
    assert first_date.sort_values("advisory_model_rank")["instrument"].tolist() == [
        "000002.SZ",
        "000001.SZ",
    ]
