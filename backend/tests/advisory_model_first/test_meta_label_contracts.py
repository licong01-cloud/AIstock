from __future__ import annotations

from backend.services.advisory_model_first.meta_label_contracts import (
    approved_meta_label_families,
    build_frozen_meta_label_request,
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
        "repository_commit": "8" * 40,
        "output_root": "/output/one",
        "feature_schema_hash": "9" * 64,
        "family_specs": approved_meta_label_families(),
    }
    values.update(overrides)
    return build_frozen_meta_label_request(**values)


def test_meta_label_request_identity_ignores_output_and_created_at() -> None:
    first = _request(created_at="2026-08-13T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-13T01:00:00+00:00")
    assert first.request_sha256 == second.request_sha256
    assert first.request_id == second.request_id
