from __future__ import annotations

import json

import pytest

from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_contracts import (
    P0L_GAIN_ROSTER,
    FrozenAdvisoryP0LTrainingRequestV1,
    approved_p0l_families,
    build_frozen_p0l_request,
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
        "family_specs": approved_p0l_families(),
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
        "exact_p0g_anchor_reference": {
            "bundle_root": "/models/p0g",
            "bundle_id": "c" * 64,
            "manifest_file_sha256": "d" * 64,
            "winner_family_id": "FAMILY_TURNOVER_CONSTRAINED_CORE",
            "winner_seed": 20260817,
            "winner_training_objective": "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1",
        },
        "p0h_evidence_reference": _evidence("P0H_V1_EVIDENCE", "e", "f"),
        "p0k_evidence_reference": _evidence("P0K_V1_EVIDENCE", "a", "c"),
        "model_information_cutoff_trade_date": "2026-03-10",
        "latest_training_decision_trade_date": "2026-02-02",
        "latest_training_label_observation_trade_date": "2026-03-10",
    }
    values.update(overrides)
    return build_frozen_p0l_request(**values)


def _evidence(role: str, bundle: str, manifest: str) -> dict[str, object]:
    return {
        "role": role,
        "bundle_root": f"/models/{role.lower()}",
        "bundle_id": bundle * 64,
        "manifest_file_sha256": manifest * 64,
    }


def test_p0l_request_round_trip_and_dynamic_fields() -> None:
    first = _request(created_at="2026-08-29T00:00:00+00:00")
    second = _request(output_root="/output/two", created_at="2026-08-29T01:00:00+00:00")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    loaded = FrozenAdvisoryP0LTrainingRequestV1.model_validate_json(first.model_dump_json())
    assert loaded.functional_payload() == first.functional_payload()
    assert json.loads(first.model_dump_json())["experiment_lineage"][-1] == "P0-L-v1"


def test_p0l_request_freezes_anchor_liability_and_intervention_contract() -> None:
    request = _request()
    assert request.expected_outer_trial_path_count == 168
    assert request.liability_rank_gain_roster == P0L_GAIN_ROSTER
    assert request.identity_control == "NO_SWAP_CONTROL_V1"
    assert request.max_anchor_displacement == 1
    assert request.max_adjacent_swaps_per_date == 1
    assert request.exact_p0g_anchor_reference.winner_seed == 20260817
    assert request.exact_p0g_anchor_reference.winner_boost_rounds == 19
    assert request.model_role == "OFFLINE_P0G_ANCHORED_LIABILITY_LOCAL_RERANKER_V1"
    assert not hasattr(request, "return_training_objective")


def test_p0l_request_rejects_roster_reference_and_cutoff_drift() -> None:
    with pytest.raises(ValueError, match="family order"):
        _request(family_specs=tuple(reversed(approved_p0l_families())))
    with pytest.raises(ValueError, match="roster"):
        _request(liability_rank_gain_roster=(12, 8, 4, 0))
    with pytest.raises(ValueError):
        _request(
            exact_p0g_anchor_reference={
                **_request().exact_p0g_anchor_reference.model_dump(mode="json"),
                "winner_seed": 20260813,
            }
        )
    with pytest.raises(ValueError, match="cutoffs are inconsistent"):
        _request(model_information_cutoff_trade_date="2026-03-09")
    with pytest.raises(ValueError):
        _request(
            exact_p0g_anchor_reference={
                **_request().exact_p0g_anchor_reference.model_dump(mode="json"),
                "winner_boost_rounds": 20,
            }
        )
