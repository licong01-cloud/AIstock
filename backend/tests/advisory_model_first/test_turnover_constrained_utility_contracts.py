from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.turnover_constrained_utility_contracts import (
    FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1,
    TURNOVER_SHADOW_PRICE_MULTIPLIERS,
    approved_turnover_constrained_utility_families,
    build_frozen_turnover_constrained_utility_request,
)


def _reference(role: str) -> dict[str, object]:
    p0d = role == "P0D_V2_REFERENCE"
    return {
        "role": role,
        "bundle_root": "/models/p0d" if p0d else "/models/p0f",
        "bundle_id": ("a" if p0d else "c") * 64,
        "manifest_file_sha256": ("b" if p0d else "d") * 64,
        "arm_id": "ARM_P0D_V2_BINARY_PARITY" if p0d else "ARM_P0F_V2_HUBER_UTILITY",
        "winner_family_id": "FAMILY_POLICY_UTILITY_CORE_HMM" if p0d else "FAMILY_POLICY_UTILITY_CORE",
        "winner_seed": 20260813 if p0d else 20260817,
        "winner_training_objective": (
            "BINARY_TAKE_SKIP_PARITY_V2" if p0d else "HUBER_CONTINUOUS_POLICY_NET_EXCESS_V2"
        ),
    }


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
        "family_specs": approved_turnover_constrained_utility_families(),
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
        "exact_p0d_reference": _reference("P0D_V2_REFERENCE"),
        "exact_p0f_reference": _reference("P0F_V2_REFERENCE"),
        "model_information_cutoff_trade_date": "2026-03-10",
        "latest_training_decision_trade_date": "2026-02-02",
        "latest_training_label_observation_trade_date": "2026-03-10",
    }
    values.update(overrides)
    return build_frozen_turnover_constrained_utility_request(**values)


def test_turnover_utility_request_round_trip_and_dynamic_fields() -> None:
    first = _request(created_at="2026-08-25T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-25T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    loaded = FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1.model_validate_json(
        first.model_dump_json()
    )
    assert loaded.functional_payload() == first.functional_payload()
    assert json.loads(first.model_dump_json())["experiment_lineage"] == [
        "P0-D-v2",
        "P0-E-v2",
        "P0-F-v2",
        "P0-G-v1",
    ]


def test_turnover_utility_request_freezes_trial_and_label_identity() -> None:
    request = _request()
    assert request.expected_trial_path_count == 168
    assert request.expected_candidate_row_count == 7720
    assert request.expected_matured_row_count == 7716
    assert request.expected_label_status_counts == {
        "MATURED": 7716,
        "NOT_ENTERED_LIMIT_UP": 3,
        "CENSORED_RIGHT_BOUNDARY": 1,
    }
    assert request.shadow_price_multipliers == TURNOVER_SHADOW_PRICE_MULTIPLIERS
    assert request.training_objective == "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"


def test_turnover_utility_request_rejects_roster_reference_and_cutoff_drift() -> None:
    with pytest.raises(ValueError, match="family order"):
        _request(family_specs=tuple(reversed(approved_turnover_constrained_utility_families())))
    with pytest.raises(ValueError, match="multiplier roster"):
        _request(shadow_price_multipliers=(0.0, 1.0))
    with pytest.raises(ValueError, match="role and arm_id"):
        _request(
            exact_p0d_reference={
                **_reference("P0D_V2_REFERENCE"),
                "arm_id": "ARM_P0F_V2_HUBER_UTILITY",
            }
        )
    with pytest.raises(ValueError, match="cutoffs are inconsistent"):
        _request(model_information_cutoff_trade_date="2026-03-09")
