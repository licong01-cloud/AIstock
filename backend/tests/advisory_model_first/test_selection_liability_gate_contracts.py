from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.selection_liability_gate_contracts import (
    SELECTION_LIABILITY_GATE_THRESHOLDS,
    FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
    approved_selection_liability_gate_families,
    build_frozen_selection_liability_gate_request,
)


def _evidence(role: str) -> dict[str, object]:
    values = {
        "P0H_V1_EVIDENCE": ("a", "b", "NEGATIVE_STOP_NOT_ADVANCED", True),
        "P0I_V1_EVIDENCE": ("c", "d", "NEGATIVE_STOP_INCOMPLETE_CPCV", False),
        "P0J_V1_EVIDENCE": ("e", "f", "NEGATIVE_STOP_INCOMPLETE_CPCV", False),
    }
    bundle, manifest, status, model = values[role]
    return {
        "role": role,
        "bundle_root": f"/models/{role.lower()}",
        "bundle_id": bundle * 64,
        "manifest_file_sha256": manifest * 64,
        "expected_experiment_status": status,
        "expected_model_available": model,
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
        "family_specs": approved_selection_liability_gate_families(),
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
        "exact_p0d_reference": {
            "bundle_root": "/models/p0d",
            "bundle_id": "a" * 64,
            "manifest_file_sha256": "b" * 64,
            "winner_family_id": "FAMILY_POLICY_UTILITY_CORE_HMM",
            "winner_seed": 20260813,
            "winner_training_objective": "BINARY_TAKE_SKIP_PARITY_V2",
            "winner_boost_rounds": 17,
        },
        "p0h_evidence_reference": _evidence("P0H_V1_EVIDENCE"),
        "p0i_evidence_reference": _evidence("P0I_V1_EVIDENCE"),
        "p0j_evidence_reference": _evidence("P0J_V1_EVIDENCE"),
        "model_information_cutoff_trade_date": "2026-03-10",
        "latest_training_decision_trade_date": "2026-02-02",
        "latest_training_label_observation_trade_date": "2026-03-10",
    }
    values.update(overrides)
    return build_frozen_selection_liability_gate_request(**values)


def test_selection_liability_gate_request_round_trip_and_dynamic_fields() -> None:
    first = _request(created_at="2026-08-28T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-28T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    loaded = FrozenAdvisorySelectionLiabilityGateTrainingRequestV1.model_validate_json(
        first.model_dump_json()
    )
    assert loaded.functional_payload() == first.functional_payload()
    assert json.loads(first.model_dump_json())["experiment_lineage"][-1] == "P0-K-v1"


def test_selection_liability_gate_request_freezes_only_liability_model_and_roster() -> None:
    request = _request()
    assert request.expected_outer_trial_path_count == 168
    assert request.expected_constraint_decision_date_count == 385
    assert request.minimum_expected_holding_days == (1, 2, 3, 5, 10, 20)
    assert request.maximum_liability_thresholds == SELECTION_LIABILITY_GATE_THRESHOLDS
    assert request.liability_clip_min == 2 / (5 * 20)
    assert request.liability_clip_max == 2 / 5
    assert request.model_role == "OFFLINE_SELECTION_PRESERVING_LIABILITY_GATE_V1"
    assert not hasattr(request, "return_training_objective")


def test_selection_liability_gate_request_rejects_roster_evidence_and_cutoff_drift() -> None:
    with pytest.raises(ValueError, match="family order"):
        _request(family_specs=tuple(reversed(approved_selection_liability_gate_families())))
    with pytest.raises(ValueError, match="threshold roster"):
        _request(maximum_liability_thresholds=(0.4, 0.1))
    with pytest.raises(ValueError, match="holding-day roster"):
        _request(minimum_expected_holding_days=(1, 2, 4, 5, 10, 20))
    with pytest.raises(ValueError, match="evidence role and expected state"):
        _request(
            p0h_evidence_reference={
                **_evidence("P0H_V1_EVIDENCE"),
                "expected_model_available": False,
            }
        )
    with pytest.raises(ValueError, match="cutoffs are inconsistent"):
        _request(model_information_cutoff_trade_date="2026-03-09")
