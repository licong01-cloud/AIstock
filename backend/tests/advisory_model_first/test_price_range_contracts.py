from __future__ import annotations

import pytest

from backend.services.advisory_model_first.price_range_contracts import (
    PriceRangeInputArtifactV1,
    build_frozen_price_range_training_request,
)


def _values() -> dict[str, object]:
    artifact = PriceRangeInputArtifactV1(
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
        "parent_bundle_manifest_file_sha256": "d" * 64,
        "outcome_request_id": "advoutreq_parent",
        "outcome_request_sha256": "e" * 64,
        "outcome_bundle_id": "f" * 64,
        "outcome_bundle_manifest_file_sha256": "1" * 64,
        "package_id": "pkg",
        "manifest_sha256": "2" * 64,
        "style_profile_id": "style",
        "style_profile_hash": "3" * 64,
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": "4" * 64,
        "candidate_semantics_id": "candidate-v1",
        "candidates_artifact": artifact,
        "features_artifact": artifact,
        "parent_training_request_path": "/data/parent/training_request.json",
        "parent_feature_schema_path": "/data/parent/feature_schema.json",
        "outcome_training_request_path": "/data/outcome/training_request.json",
        "outcome_split_path": "/data/outcome/split.json",
        "qlib_daily_root": "/data/qlib",
        "suspend_data_root": "/data/suspend",
        "repository_root": "/repo",
        "repository_commit": "5" * 40,
    }


def test_price_range_request_identity_excludes_created_at_and_output_root() -> None:
    first = build_frozen_price_range_training_request(
        **_values(), output_root="/out/a", created_at="2026-08-10T00:00:00Z"
    )
    second = build_frozen_price_range_training_request(
        **_values(), output_root="/out/b", created_at="2026-08-10T01:00:00Z"
    )

    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    assert first.request_id.startswith("advprreq_")
    assert first.entry_gap_condition == "ENTRY_EXECUTABLE"
    with pytest.raises(ValueError, match="quantiles must equal"):
        build_frozen_price_range_training_request(
            **_values(),
            output_root="/out",
            quantiles=(0.1, 0.5),
            created_at="2026-08-10T00:00:00Z",
        )
