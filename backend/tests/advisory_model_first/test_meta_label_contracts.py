from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.meta_label_contracts import (
    FrozenAdvisoryMetaLabelTrainingRequestV1,
    approved_meta_label_families,
    build_frozen_meta_label_request,
    build_return_aware_meta_label_request,
)


def _request(**overrides):
    values = {
        "policy_dataset_bundle_root": "/data/policy",
        "policy_dataset_bundle_id": "1" * 64,
        "policy_dataset_manifest_file_sha256": "2" * 64,
        "program_id": "advp_test",
        "binding_version_id": "advb_test",
        "package_id": "pkg_test",
        "manifest_sha256": "3" * 64,
        "style_profile_id": "short_rebound_v1",
        "style_profile_hash": "4" * 64,
        "shadow_policy_sha256": "5" * 64,
        "cost_policy_sha256": "6" * 64,
        "split_policy_sha256": "7" * 64,
        "qlib_daily_root": "/data/qlib",
        "factor_data_root": "/data/factors",
        "factor_data_cutoff": "2026-06-30",
        "suspend_data_root": "/data/suspend",
        "repository_root": "/repo",
        "repository_root_windows": "F:\\repo",
        "repository_commit": "8" * 40,
        "output_root": "/output/one",
        "feature_schema_hash": "9" * 64,
        "family_specs": approved_meta_label_families(),
    }
    values.update(overrides)
    return build_frozen_meta_label_request(**values)


def _return_aware_request(**overrides):
    base = _request().model_dump(
        exclude={
            "schema_version",
            "request_id",
            "request_sha256",
            "created_at",
            "outcome_weighting",
            "reference_meta_label_bundle_root",
            "reference_meta_label_bundle_id",
            "reference_meta_label_manifest_file_sha256",
        }
    )
    base.update(
        {
            "reference_meta_label_bundle_root": "/models/reference",
            "reference_meta_label_bundle_id": "a" * 64,
            "reference_meta_label_manifest_file_sha256": "b" * 64,
        }
    )
    base.update(overrides)
    return build_return_aware_meta_label_request(**base)


def test_meta_label_request_identity_ignores_output_and_created_at() -> None:
    first = _request(created_at="2026-08-13T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-13T01:00:00+00:00")
    assert first.request_sha256 == second.request_sha256
    assert first.request_id == second.request_id


def test_meta_label_v2_round_trip_keeps_exact_functional_identity() -> None:
    request = _request(created_at="2026-08-13T00:00:00+00:00")
    payload = json.loads(request.model_dump_json())
    assert payload["schema_version"] == "frozen_advisory_meta_label_training_request_v2"
    assert payload["outcome_weighting"] is None
    assert payload["reference_meta_label_bundle_id"] is None
    loaded = FrozenAdvisoryMetaLabelTrainingRequestV1.model_validate(payload)
    assert loaded.request_sha256 == request.request_sha256
    assert loaded.functional_payload() == request.functional_payload()
    assert "outcome_weighting" not in loaded.functional_payload()


def test_return_aware_request_freezes_weighting_and_reference_identity() -> None:
    request = _return_aware_request()
    assert request.schema_version == "frozen_advisory_meta_label_training_request_v3"
    assert request.outcome_weighting is not None
    assert request.outcome_weighting.relative_cap == 4.0
    assert request.reference_meta_label_bundle_id == "a" * 64
    changed = _return_aware_request(reference_meta_label_bundle_id="c" * 64)
    assert changed.request_sha256 != request.request_sha256


def test_meta_label_request_rejects_cross_version_weighting_fields() -> None:
    with pytest.raises(ValueError, match="v2 request cannot contain"):
        _request(
            outcome_weighting={
                "schema_version": "advisory_meta_label_outcome_weighting_v1",
                "mode": "ABS_NET_EXCESS_TRAIN_MEDIAN_V1",
                "scale_statistic": "MEDIAN_ABSOLUTE_NET_EXCESS_BPS",
                "base_weight": 1.0,
                "relative_cap": 4.0,
                "normalization": "TRAIN_MEAN_ONE",
            }
        )
