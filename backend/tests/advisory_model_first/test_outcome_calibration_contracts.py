from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.outcome_calibration_contracts import (
    OutcomeCalibrationArtifactV1,
    build_frozen_outcome_calibration_request,
    expected_binary_calibration_heads,
)


def _request(tmp_path: Path):
    parent_id = "a" * 64
    artifact = OutcomeCalibrationArtifactV1(
        path=str(tmp_path / "input.parquet"),
        sha256="b" * 64,
        size_bytes=10,
        row_count=20,
        columns=("split", "instrument"),
    )
    return build_frozen_outcome_calibration_request(
        output_root=str(tmp_path),
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
        repository_root=str(tmp_path / "repo"),
        repository_commit="3" * 40,
        created_at="2026-08-12T00:00:00+00:00",
    )


def test_calibration_request_has_deterministic_functional_identity(tmp_path: Path) -> None:
    first = _request(tmp_path)
    second = first.model_copy(update={"created_at": "2027-01-01T00:00:00+00:00", "output_root": "/other"})

    assert first.request_id.startswith("advoutcal_")
    assert first.request_sha256 == second.request_sha256
    assert len(expected_binary_calibration_heads()) == 10


def test_calibration_request_rejects_parent_root_identity_mismatch(tmp_path: Path) -> None:
    request = _request(tmp_path)
    with pytest.raises(ValidationError):
        request.model_copy(update={"parent_bundle_root": str(tmp_path / "wrong")}).model_validate(
            request.model_copy(update={"parent_bundle_root": str(tmp_path / "wrong")}).model_dump()
        )


def test_calibration_request_rejects_resource_limit_above_eight_gibibytes(tmp_path: Path) -> None:
    request = _request(tmp_path)
    with pytest.raises(ValidationError):
        request.model_validate(
            request.model_copy(update={"resource_max_rss_bytes": 8 * 1024**3 + 1}).model_dump()
        )
