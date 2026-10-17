from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.grouped_rank_output_constraint_contracts import (
    GROUPED_RANK_SHADOW_PRICE_MULTIPLIERS,
    FrozenAdvisoryGroupedRankTrainingRequestV1,
    approved_grouped_rank_families,
    build_frozen_grouped_rank_request,
)
from backend.tests.advisory_model_first._test_support import p0_reference as _reference


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
        "family_specs": approved_grouped_rank_families(),
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
        "exact_p0g_reference": _reference("P0G_V1_REFERENCE"),
        "exact_p0h_reference": _reference("P0H_V1_REFERENCE"),
        "model_information_cutoff_trade_date": "2026-03-10",
        "latest_training_decision_trade_date": "2026-02-02",
        "latest_training_label_observation_trade_date": "2026-03-10",
    }
    values.update(overrides)
    return build_frozen_grouped_rank_request(**values)


def test_grouped_rank_request_round_trip_and_dynamic_fields() -> None:
    first = _request(created_at="2026-08-25T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-25T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    loaded = FrozenAdvisoryGroupedRankTrainingRequestV1.model_validate_json(first.model_dump_json())
    assert loaded.functional_payload() == first.functional_payload()
    assert first.request_id.startswith("advgroupedrankreq_")
    assert json.loads(first.model_dump_json())["experiment_lineage"][-1] == "P0-I-v1"


def test_grouped_rank_request_freezes_roster_coverage_and_physical_units() -> None:
    request = _request()
    assert request.expected_outer_trial_path_count == 168
    assert request.expected_constraint_decision_date_count == 385
    assert request.expected_label_status_counts == {
        "MATURED": 7716,
        "NOT_ENTERED_LIMIT_UP": 3,
        "CENSORED_RIGHT_BOUNDARY": 1,
    }
    assert request.shadow_price_multipliers == GROUPED_RANK_SHADOW_PRICE_MULTIPLIERS
    assert request.liability_clip_min == 2 / (5 * 20)
    assert request.liability_clip_max == 2 / 5


def test_grouped_rank_request_rejects_roster_reference_unit_and_cutoff_drift() -> None:
    with pytest.raises(ValueError, match="family order"):
        _request(family_specs=tuple(reversed(approved_grouped_rank_families())))
    with pytest.raises(ValueError, match="multiplier roster"):
        _request(shadow_price_multipliers=(0.0, 1.0))
    with pytest.raises(ValueError, match="role and arm_id"):
        _request(
            exact_p0g_reference={
                **_reference("P0G_V1_REFERENCE"),
                "arm_id": "ARM_P0F_V2_HUBER_UTILITY",
            }
        )
    with pytest.raises(ValueError, match="liability_clip_min"):
        _request(liability_clip_min=0.01)
    with pytest.raises(ValueError, match="cutoffs are inconsistent"):
        _request(model_information_cutoff_trade_date="2026-03-09")
