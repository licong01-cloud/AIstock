from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.policy_utility_contracts import (
    FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    approved_policy_utility_arms,
    approved_policy_utility_families,
    build_frozen_policy_utility_request,
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
        "family_specs": approved_policy_utility_families(),
        "arm_specs": approved_policy_utility_arms(),
        "market_calendar_identity": {
            "identity_kind": "MARKET_CALENDAR",
            "sha256": "9" * 64,
            "cutoff_trade_date": "2026-06-30",
            "row_count": 2000,
        },
        "suspend_sidecar_identity": {
            "identity_kind": "SUSPEND_SIDECAR",
            "sha256": "0" * 64,
            "cutoff_trade_date": "2026-06-30",
            "row_count": 30000,
        },
        "legacy_p0d_reference": {
            "role": "LEGACY_P0_D_LINEAGE",
            "bundle_root": "/models/p0d",
            "bundle_id": "a" * 64,
            "manifest_file_sha256": "b" * 64,
        },
        "legacy_p0e_reference": {
            "role": "LEGACY_P0_E_LINEAGE",
            "bundle_root": "/models/p0e",
            "bundle_id": "c" * 64,
            "manifest_file_sha256": "d" * 64,
        },
        "model_information_cutoff_trade_date": "2026-03-10",
        "latest_training_decision_trade_date": "2026-02-02",
        "latest_training_label_observation_trade_date": "2026-03-10",
    }
    values.update(overrides)
    return build_frozen_policy_utility_request(**values)


def test_policy_utility_request_round_trip_and_dynamic_fields() -> None:
    first = _request(created_at="2026-08-24T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-24T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    loaded = FrozenAdvisoryPolicyUtilityTrainingRequestV2.model_validate_json(first.model_dump_json())
    assert loaded.functional_payload() == first.functional_payload()
    assert json.loads(first.model_dump_json())["experiment_lineage"] == ["P0-D-v2", "P0-E-v2", "P0-F-v2"]
    rebuilt_values = json.loads(first.model_dump_json())
    for key in ("request_id", "request_sha256", "created_at"):
        rebuilt_values.pop(key)
    assert build_frozen_policy_utility_request(**rebuilt_values).request_sha256 == first.request_sha256


def test_policy_utility_request_freezes_objective_and_reference_identity() -> None:
    request = _request()
    changed = _request(
        legacy_p0e_reference={
            "role": "LEGACY_P0_E_LINEAGE",
            "bundle_root": "/models/p0e",
            "bundle_id": "e" * 64,
            "manifest_file_sha256": "d" * 64,
        }
    )
    assert tuple(item.arm_id for item in request.arm_specs) == (
        "ARM_P0D_V2_BINARY_PARITY",
        "ARM_P0E_V2_WEIGHTED_BINARY",
        "ARM_P0F_V2_HUBER_UTILITY",
    )
    assert request.expected_trial_path_count == 504
    assert request.request_sha256 != changed.request_sha256


def test_policy_utility_request_rejects_future_cutoff_inconsistency() -> None:
    assert _request().model_information_cutoff_trade_date == "2026-03-10"
    with pytest.raises(ValueError, match="cutoffs are inconsistent"):
        _request(model_information_cutoff_trade_date="2026-03-09")


def test_policy_utility_request_rejects_family_or_lineage_drift() -> None:
    with pytest.raises(ValueError, match="family order"):
        _request(family_specs=tuple(reversed(approved_policy_utility_families())))
    with pytest.raises(ValueError, match="arm roster"):
        _request(arm_specs=tuple(reversed(approved_policy_utility_arms())))
    with pytest.raises(ValueError, match="lineage"):
        _request(experiment_lineage=("P0-F",))
    with pytest.raises(ValueError):
        _request(resource_max_rss_bytes=1024**3)
