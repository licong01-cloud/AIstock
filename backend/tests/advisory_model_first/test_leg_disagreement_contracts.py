from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.leg_disagreement_contracts import (
    LEG_MVE_COMPARATOR_FEATURES,
    LEG_MVE_EXPANDED_FEATURES,
    LEG_MVE_FEATURE_SCHEMA_HASH,
    FrozenLegDisagreementRequestV1,
    build_default_leg_model_trials,
    build_leg_disagreement_receipt,
    build_leg_disagreement_request,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _refs() -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_leg_parent_overlay_manifest",
        "n3_leg_parent_overlay_receipt",
        "n3_leg_parent_qe_score_panel",
        "n3_leg_n2a_manifest",
        "n3_leg_n2a_request",
        "n3_leg_n2a_full_universe",
        "n3_leg_n1_manifest",
        "n3_leg_n1_cpcv",
        "n3_leg_n1_regime_daily",
    )
    return tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=f"/tmp/{index}.artifact",
            sha256=f"{index + 1:064x}",
            size_bytes=index + 1,
        )
        for index, role in enumerate(roles)
    )


def _request() -> FrozenLegDisagreementRequestV1:
    refs = _refs()
    source_dataset = "a" * 64
    parent_dataset = "b" * 64
    split = "c" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "parent_dataset_identity": parent_dataset,
            "n1_split_policy_sha256": split,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    return build_leg_disagreement_request(
        created_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
        evidence_refs=refs,
        parent_overlay_bundle_path="/tmp/" + "1" * 64,
        parent_overlay_bundle_id="1" * 64,
        parent_overlay_request_sha256="2" * 64,
        parent_overlay_receipt_sha256="3" * 64,
        n2a_bundle_path="/tmp/" + "4" * 64,
        n2a_bundle_id="4" * 64,
        n2a_request_sha256="5" * 64,
        n2a_receipt_sha256="6" * 64,
        n1_bundle_path="/tmp/" + "7" * 64,
        n1_bundle_id="7" * 64,
        n1_request_sha256="8" * 64,
        n1_split_policy_sha256=split,
        source_dataset_identity=source_dataset,
        parent_dataset_identity=parent_dataset,
        dataset_identity=dataset,
        policy_identity="d" * 64,
        registry_path="/tmp/registry.jsonl",
        route_path="/tmp/route.md",
        repository_root="/tmp/repo",
        repository_commit="e" * 40,
        output_root="/tmp/output",
    )


def test_default_trial_roster_and_feature_schema_are_exact() -> None:
    trials = build_default_leg_model_trials()
    assert len(trials) == 2
    assert trials[0].feature_columns == LEG_MVE_COMPARATOR_FEATURES
    assert trials[1].feature_columns == LEG_MVE_EXPANDED_FEATURES
    assert trials[0].role == "COMPARATOR"
    assert trials[1].role == "CANDIDATE"
    assert LEG_MVE_FEATURE_SCHEMA_HASH == canonical_json_sha256(
        {
            "schema_version": "advisory_n3_leg_disagreement_feature_schema_v1",
            "comparator_features": list(LEG_MVE_COMPARATOR_FEATURES),
            "expanded_features": list(LEG_MVE_EXPANDED_FEATURES),
            "rank_semantics": "SAME_DATE_CANONICAL_MEMBER_AVERAGE_PCT_ASCENDING",
        }
    )


def test_request_freezes_two_trials_four_hypotheses_and_safety_gates() -> None:
    request = _request()
    assert request.planned_trial_count == 2
    assert request.familywise_hypothesis_count == 4
    assert request.expected_ready_path_count == 28
    assert request.expected_oof_predictions_per_row == 7
    assert request.resource_max_wall_seconds is None
    assert request.database_read_allowed is False
    assert request.network_read_allowed is False
    assert request.qlib_read_allowed is False
    assert request.minute_data_read_allowed is False
    assert request.sealed_holdout_accessed is False
    assert request.final_model_output_allowed is False
    assert request.deployable is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("feature_schema_hash", "f" * 64),
        ("familywise_hypothesis_count", 3),
        ("resource_max_wall_seconds", 1),
        ("database_read_allowed", True),
        ("selected_trial_count", 1),
    ),
)
def test_request_rejects_contract_drift(field: str, value: object) -> None:
    payload = _request().model_dump(mode="json")
    payload[field] = value
    with pytest.raises((ValidationError, ValueError)):
        FrozenLegDisagreementRequestV1.model_validate(payload)


def test_receipt_routes_selected_zero_and_one_without_activation() -> None:
    zero = build_leg_disagreement_receipt(
        request_sha256="a" * 64,
        selected_trial_count=0,
        selected_trial_id=None,
        eligible_trial_ids=(),
        next_task="N3_MINUTE_INFORMATION_SET_MVE",
        source_identity_sha256="b" * 64,
        result_files_sha256="c" * 64,
        resource_report_sha256="d" * 64,
        created_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    selected = build_leg_disagreement_receipt(
        request_sha256="a" * 64,
        selected_trial_count=1,
        selected_trial_id="N3_LEG_DISAGREEMENT_EXPANDED_V1",
        eligible_trial_ids=("N3_LEG_DISAGREEMENT_EXPANDED_V1",),
        next_task="N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN",
        source_identity_sha256="b" * 64,
        result_files_sha256="c" * 64,
        resource_report_sha256="d" * 64,
        created_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    assert zero.next_task == "N3_MINUTE_INFORMATION_SET_MVE"
    assert selected.next_task == "N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN"
    assert zero.runtime_eligible is False and selected.final_model_written is False
