from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.margin_information_set_contracts import (
    MARGIN_MVE_CALENDAR_SHA256,
    MARGIN_MVE_CALENDAR_SIZE,
    MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX,
    MARGIN_MVE_CURRENT_MARGIN_SHA256,
    MARGIN_MVE_CURRENT_MARGIN_SIZE,
    MARGIN_MVE_EXPANDED_FEATURES,
    MARGIN_MVE_EXTERNAL_VISIBLE_MARGIN_HYPOTHESIS_COUNT,
    MARGIN_MVE_FEATURE_SCHEMA_HASH,
    MARGIN_MVE_MEMBERSHIP_FEATURES,
    MARGIN_MVE_PARENT_FEATURES,
    MARGIN_MVE_RANKED_DYNAMICS_FEATURES,
    MARGIN_MVE_SECONDARY_MARGIN_SHA256,
    MARGIN_MVE_SECONDARY_MARGIN_SIZE,
    MARGIN_MVE_SELECTABLE_HYPOTHESIS_COUNT_PRIOR,
    MARGIN_MVE_SOURCE_FIELDS,
    MARGIN_MVE_TARGET_FREE_PRIOR_PROPOSAL_COUNT,
    FrozenMarginInformationSetRequestV1,
    MarginSourceIdentityReceiptV1,
    build_default_margin_model_trials,
    build_margin_information_set_receipt,
    build_margin_information_set_request,
    build_margin_source_receipt,
    build_margin_source_request,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchStudyType,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _refs() -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_margin_generator_manifest",
        "n3_margin_generator_receipt",
        "n3_margin_n2b_manifest",
        "n3_margin_n2b_request",
        "n3_margin_n2b_outcomes",
        "n3_margin_n1_manifest",
        "n3_margin_n1_cpcv",
        "n3_margin_n1_regime_daily",
        "n3_margin_source_manifest",
        "n3_margin_source_receipt",
        "n3_margin_source_projection",
        "n3_margin_source_coverage",
        "n3_margin_cross_snapshot_parity",
        "n3_margin_candidate_state_snapshot",
    )
    return tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=f"/tmp/evidence-{index}",
            sha256=f"{index + 1:064x}",
            size_bytes=index + 1,
        )
        for index, role in enumerate(roles)
    )


def _source_request():
    return build_margin_source_request(
        created_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        repository_root="/tmp/repo",
        repository_commit="f" * 40,
        n2b_bundle_path="/tmp/" + "1" * 64,
        n2b_bundle_id="1" * 64,
        n2b_outcomes_sha256="2" * 64,
        candidate_root="/tmp/candidate",
        candidate_state_path="/tmp/candidate/direct_monthly_state.json",
        candidate_state_sha256="3" * 64,
        candidate_state_size=10,
        candidate_state_updated_at="2026-09-04T00:00:00Z",
        current_margin_path="/tmp/candidate/margin_detail.h5",
        current_margin_sha256=MARGIN_MVE_CURRENT_MARGIN_SHA256,
        current_margin_size=MARGIN_MVE_CURRENT_MARGIN_SIZE,
        secondary_margin_path="/tmp/secondary/margin_detail.h5",
        secondary_margin_sha256=MARGIN_MVE_SECONDARY_MARGIN_SHA256,
        secondary_margin_size=MARGIN_MVE_SECONDARY_MARGIN_SIZE,
        calendar_path="/tmp/candidate/calendars/day.txt",
        calendar_sha256=MARGIN_MVE_CALENDAR_SHA256,
        calendar_size=MARGIN_MVE_CALENDAR_SIZE,
    )


def _request() -> FrozenMarginInformationSetRequestV1:
    refs = _refs()
    source_dataset = "a" * 64
    route_dataset = "b" * 64
    split = "c" * 64
    source_identity = "d" * 64
    schema = MARGIN_MVE_FEATURE_SCHEMA_HASH
    policy = "e" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "route_dataset_identity": route_dataset,
            "n1_split_policy_sha256": split,
            "source_identity_sha256": source_identity,
            "feature_schema_hash": schema,
            "policy_identity": policy,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    return build_margin_information_set_request(
        created_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        evidence_refs=refs,
        generator_bundle_path="/tmp/" + "1" * 64,
        generator_bundle_id="1" * 64,
        generator_request_sha256="2" * 64,
        generator_receipt_sha256="3" * 64,
        n2b_bundle_path="/tmp/" + "4" * 64,
        n2b_bundle_id="4" * 64,
        n2b_request_sha256="5" * 64,
        n2b_receipt_sha256="6" * 64,
        n1_bundle_path="/tmp/" + "7" * 64,
        n1_bundle_id="7" * 64,
        n1_request_sha256="8" * 64,
        n1_split_policy_sha256=split,
        source_bundle_path="/tmp/" + "9" * 64,
        source_bundle_id="9" * 64,
        source_request_sha256="0" * 64,
        source_receipt_sha256="1" * 64,
        source_identity_sha256=source_identity,
        source_dataset_identity=source_dataset,
        route_dataset_identity=route_dataset,
        dataset_identity=dataset,
        policy_identity=policy,
        registry_path="/tmp/registry.jsonl",
        route_path="/tmp/route.md",
        repository_root="/tmp/repo",
        repository_commit="f" * 40,
        output_root="/tmp/output",
    )


def _source_receipt():
    return build_margin_source_receipt(
        source_request_sha256="a" * 64,
        source_identity_sha256="b" * 64,
        projection_sha256="c" * 64,
        projection_row_count=1,
        common_key_count=1,
        current_only_key_count=0,
        secondary_only_key_count=0,
        parent_row_count=1_710_301,
        decision_date_count=386,
        source_row_fraction=0.8,
        top20_source_row_fraction=0.8,
        top50_source_row_fraction=0.8,
        top20_supported_day_count=386,
        raw_field_finite_fraction={name: 1.0 for name in MARGIN_MVE_SOURCE_FIELDS},
        candidate_state_before_sha256="d" * 64,
        candidate_state_after_sha256="d" * 64,
        current_source_row_count_read=1,
        secondary_source_row_count_read=1,
        source_unique_file_count=4,
        source_bytes_read=1,
        elapsed_seconds=1.0,
        peak_rss_bytes=1,
        temporary_bytes=1,
        created_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
    )


def test_default_trial_roster_feature_schema_and_multiplicity_are_exact() -> None:
    trials = build_default_margin_model_trials()
    assert [(item.role, item.selectable) for item in trials] == [
        ("COMPARATOR", False),
        ("CONTROL", False),
        ("CANDIDATE", True),
    ]
    assert trials[0].feature_columns == MARGIN_MVE_PARENT_FEATURES
    assert trials[1].feature_columns == MARGIN_MVE_MEMBERSHIP_FEATURES
    assert trials[2].feature_columns == MARGIN_MVE_EXPANDED_FEATURES
    assert len(MARGIN_MVE_SOURCE_FIELDS) == 8
    assert len(MARGIN_MVE_RANKED_DYNAMICS_FEATURES) == 12
    assert len(MARGIN_MVE_EXPANDED_FEATURES) == 15
    assert MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX == 80
    assert MARGIN_MVE_SELECTABLE_HYPOTHESIS_COUNT_PRIOR == 73
    assert MARGIN_MVE_EXTERNAL_VISIBLE_MARGIN_HYPOTHESIS_COUNT == 6
    assert MARGIN_MVE_TARGET_FREE_PRIOR_PROPOSAL_COUNT == 3


def test_source_request_freezes_exact_sources_and_read_only_boundaries() -> None:
    request = _source_request()
    assert request.current_margin_sha256 == MARGIN_MVE_CURRENT_MARGIN_SHA256
    assert request.secondary_margin_sha256 == MARGIN_MVE_SECONDARY_MARGIN_SHA256
    assert request.calendar_sha256 == MARGIN_MVE_CALENDAR_SHA256
    assert request.source_fields == MARGIN_MVE_SOURCE_FIELDS
    assert request.target_columns_read is False
    assert request.database_read_allowed is False
    assert request.network_read_allowed is False
    assert request.sealed_holdout_accessed is False


def test_mve_request_freezes_fourteen_evidence_roles_and_no_activation() -> None:
    request = _request()
    assert len(request.evidence_refs) == 14
    assert request.planned_trial_count == 3
    assert request.selectable_trial_count == 1
    assert request.expected_ready_path_count == 28
    assert request.expected_oof_predictions_per_row == 7
    assert request.objective_contract == ObjectiveContract.ALPHA_RANKING
    assert request.study_type == ResearchStudyType.LEARNABILITY_AUDIT
    assert request.decision_use == DecisionUse.NAVIGATION_ONLY
    assert request.database_read_allowed is False
    assert request.network_read_allowed is False
    assert request.qlib_read_allowed is False
    assert request.runtime_activation_allowed is False
    assert request.final_model_output_allowed is False
    assert request.deployable is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("feature_schema_hash", "0" * 64),
        ("planned_trial_count", 2),
        ("current_familywise_hypothesis_count", 3),
        ("cumulative_candidate_index", 79),
        ("database_read_allowed", True),
        ("runtime_activation_allowed", True),
        ("selected_trial_count", 1),
    ),
)
def test_mve_request_rejects_contract_drift(field: str, value: object) -> None:
    payload = _request().model_dump(mode="json")
    payload[field] = value
    with pytest.raises((ValidationError, ValueError)):
        FrozenMarginInformationSetRequestV1.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("candidate_state_after_sha256", "e" * 64),
        ("source_row_fraction", 0.1),
        ("top20_supported_day_count", 379),
        ("raw_field_finite_fraction", {}),
        ("peak_rss_bytes", 9 * 1024**3),
        ("temporary_bytes", 9 * 1024**3),
    ),
)
def test_source_receipt_rejects_state_coverage_quality_and_resource_drift(field: str, value: object) -> None:
    payload = _source_receipt().model_dump(mode="json")
    payload[field] = value
    with pytest.raises((ValidationError, ValueError)):
        MarginSourceIdentityReceiptV1.model_validate(payload)


def test_receipt_routes_selected_zero_and_one_without_activation() -> None:
    common = {
        "request_sha256": "a" * 64,
        "source_identity_sha256": "b" * 64,
        "result_files_sha256": "c" * 64,
        "resource_report_sha256": "d" * 64,
        "created_at": datetime(2026, 9, 4, tzinfo=timezone.utc),
    }
    zero = build_margin_information_set_receipt(
        selected_trial_count=0,
        selected_trial_id=None,
        eligible_trial_ids=(),
        next_task="N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN",
        **common,
    )
    selected = build_margin_information_set_receipt(
        selected_trial_count=1,
        selected_trial_id="N3_MARGIN_DYNAMICS_EXPANDED_V1",
        eligible_trial_ids=("N3_MARGIN_DYNAMICS_EXPANDED_V1",),
        next_task="N3_MARGIN_INFORMATION_SET_CONFIRMATION_DESIGN",
        **common,
    )
    assert zero.next_task == "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN"
    assert selected.next_task == "N3_MARGIN_INFORMATION_SET_CONFIRMATION_DESIGN"
    assert zero.runtime_eligible is False
    assert selected.final_model_written is False
