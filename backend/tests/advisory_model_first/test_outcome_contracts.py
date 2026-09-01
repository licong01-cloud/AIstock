from __future__ import annotations

import pytest

from backend.services.advisory_model_first.outcome_contracts import (
    OutcomeInputArtifactV1,
    build_frozen_outcome_training_request,
)


def _values() -> dict[str, object]:
    artifact = OutcomeInputArtifactV1(
        path="/data/features.parquet",
        sha256="a" * 64,
        size_bytes=100,
        row_count=20,
        columns=("decision_as_of_trade_date", "instrument"),
    )
    return {
        "parent_request_id": "advmreq_parent",
        "parent_request_sha256": "b" * 64,
        "parent_bundle_id": "c" * 64,
        "parent_bundle_manifest_file_sha256": "9" * 64,
        "package_id": "pkg",
        "manifest_sha256": "d" * 64,
        "style_profile_id": "style",
        "style_profile_hash": "e" * 64,
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": "f" * 64,
        "candidate_semantics_id": "candidate-v1",
        "candidates_artifact": artifact,
        "features_artifact": artifact,
        "parent_test_predictions_artifact": artifact,
        "parent_training_request_path": "/data/training_request.json",
        "parent_feature_schema_path": "/data/feature_schema.json",
        "qlib_daily_root": "/data/qlib",
        "suspend_data_root": "/data/suspend",
        "repository_root": "/repo",
        "repository_commit": "1" * 40,
    }


def test_outcome_request_identity_excludes_created_at_and_output_root() -> None:
    first = build_frozen_outcome_training_request(
        **_values(), output_root="/out/a", created_at="2026-08-09T00:00:00Z"
    )
    second = build_frozen_outcome_training_request(
        **_values(), output_root="/out/b", created_at="2026-08-09T01:00:00Z"
    )

    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    assert first.request_id.startswith("advoutreq_")
    with pytest.raises(ValueError, match="horizons must equal"):
        build_frozen_outcome_training_request(
            **_values(), output_root="/out", horizons=(1, 5), created_at="2026-08-09T00:00:00Z"
        )
