from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.selection_prior_residual_contracts import (
    SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS,
    FrozenAdvisorySelectionPriorResidualTrainingRequestV1,
    approved_selection_prior_residual_families,
    build_frozen_selection_prior_residual_request,
)


def _reference(role: str) -> dict[str, object]:
    values = {
        "P0D_V2_REFERENCE": (
            "a",
            "b",
            "ARM_P0D_V2_BINARY_PARITY",
            "FAMILY_POLICY_UTILITY_CORE_HMM",
            20260813,
            "BINARY_TAKE_SKIP_PARITY_V2",
        ),
        "P0F_V2_REFERENCE": (
            "c",
            "d",
            "ARM_P0F_V2_HUBER_UTILITY",
            "FAMILY_POLICY_UTILITY_CORE",
            20260817,
            "HUBER_CONTINUOUS_POLICY_NET_EXCESS_V2",
        ),
        "P0G_V1_REFERENCE": (
            "e",
            "f",
            "ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
            "FAMILY_TURNOVER_CONSTRAINED_CORE",
            20260817,
            "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1",
        ),
        "P0H_V1_REFERENCE": (
            "1",
            "2",
            "ARM_P0H_V1_DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY",
            "FAMILY_DUAL_HEAD_CORE_HMM",
            20260823,
            "P0H_DUAL_HEAD_OUTPUT_CONSTRAINT_V1",
        ),
    }
    bundle, manifest, arm, family, seed, objective = values[role]
    return {
        "role": role,
        "bundle_root": f"/models/{role.lower()}",
        "bundle_id": bundle * 64,
        "manifest_file_sha256": manifest * 64,
        "arm_id": arm,
        "winner_family_id": family,
        "winner_seed": seed,
        "winner_training_objective": objective,
        "winner_boost_rounds": 2 if role == "P0D_V2_REFERENCE" else 17,
    }


def _p0i_evidence() -> dict[str, object]:
    return {
        "role": "P0I_V1_EVIDENCE",
        "bundle_root": "/models/p0i",
        "bundle_id": "3" * 64,
        "manifest_file_sha256": "4" * 64,
        "arm_id": "ARM_P0I_V1_GROUPED_RANK_OUTPUT_CONSTRAINED_UTILITY",
        "experiment_status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "model_available": False,
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
        "family_specs": approved_selection_prior_residual_families(),
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
        "p0i_evidence_reference": _p0i_evidence(),
        "model_information_cutoff_trade_date": "2026-03-10",
        "latest_training_decision_trade_date": "2026-02-02",
        "latest_training_label_observation_trade_date": "2026-03-10",
    }
    values.update(overrides)
    return build_frozen_selection_prior_residual_request(**values)


def test_selection_prior_residual_request_round_trip_and_dynamic_fields() -> None:
    first = _request(created_at="2026-08-25T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-25T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    loaded = FrozenAdvisorySelectionPriorResidualTrainingRequestV1.model_validate_json(first.model_dump_json())
    assert loaded.functional_payload() == first.functional_payload()
    assert json.loads(first.model_dump_json())["experiment_lineage"][-1] == "P0-J-v1"


def test_selection_prior_residual_request_freezes_roster_coverage_and_physical_units() -> None:
    request = _request()
    assert request.expected_outer_trial_path_count == 168
    assert request.expected_constraint_decision_date_count == 385
    assert request.expected_label_status_counts == {
        "MATURED": 7716,
        "NOT_ENTERED_LIMIT_UP": 3,
        "CENSORED_RIGHT_BOUNDARY": 1,
    }
    assert request.shadow_price_multipliers == SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS
    assert request.liability_clip_min == 2 / (5 * 20)
    assert request.liability_clip_max == 2 / 5
    assert request.selection_prior_rank_count == 20
    assert request.reliability_denominator_epsilon == 1e-12
    assert request.return_training_objective == "HUBER_SELECTION_RANK_RESIDUAL_BPS_V1"


def test_selection_prior_residual_request_rejects_roster_reference_unit_and_cutoff_drift() -> None:
    with pytest.raises(ValueError, match="family order"):
        _request(family_specs=tuple(reversed(approved_selection_prior_residual_families())))
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
    with pytest.raises(ValueError):
        _request(resource_max_rss_bytes=1024**3)
    with pytest.raises(ValueError, match="NEGATIVE_STOP_INCOMPLETE_CPCV"):
        _request(
            p0i_evidence_reference={
                **_p0i_evidence(),
                "experiment_status": "NEGATIVE_STOP_NOT_ADVANCED",
            }
        )
