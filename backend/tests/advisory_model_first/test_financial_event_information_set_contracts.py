from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.financial_event_information_set_contracts import (
    EVENT_DIRECTION_BY_TYPE,
    EVENT_DISCLOSURE_SCHEMA_FEATURES,
    EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX,
    EVENT_MVE_FEATURE_SCHEMA_HASH,
    EVENT_MVE_SOURCE_PROJECTION_SHA256,
    EVENT_SIGNED_SCHEMA_FEATURES,
    FrozenFinancialEventInformationSetRequestV1,
    build_default_event_model_trials,
    build_financial_event_receipt,
    build_financial_event_request,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchStudyType,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


ROLES = (
    "n3_event_source_manifest",
    "n3_event_source_request",
    "n3_event_source_receipt",
    "n3_event_source_projection",
    "n3_event_source_support",
    "n3_event_n2b_manifest",
    "n3_event_n2b_request",
    "n3_event_n2b_receipt",
    "n3_event_n2b_outcomes",
    "n3_event_n2b_top5",
    "n3_event_n2b_signal_daily",
    "n3_event_n1_manifest",
    "n3_event_n1_request",
    "n3_event_n1_cpcv",
    "n3_event_n1_regime_daily",
)


def build_test_request(**overrides: object) -> FrozenFinancialEventInformationSetRequestV1:
    refs = tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=f"/tmp/event-evidence-{index}",
            sha256=f"{index + 1:064x}",
            size_bytes=index + 1,
        )
        for index, role in enumerate(ROLES)
    )
    calendar = tuple(value.date() for value in pd.bdate_range("2023-01-02", "2026-03-10"))
    calendar_sha = canonical_json_sha256({"market_sessions": [value.isoformat() for value in calendar]})
    source_dataset = "a" * 64
    split = "b" * 64
    policy = "c" * 64
    source_bundle = "1" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "source_bundle_id": source_bundle,
            "source_projection_sha256": EVENT_MVE_SOURCE_PROJECTION_SHA256,
            "n1_split_policy_sha256": split,
            "trading_calendar_sha256": calendar_sha,
            "feature_schema_hash": EVENT_MVE_FEATURE_SCHEMA_HASH,
            "policy_identity": policy,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    values: dict[str, object] = {
        "created_at": datetime(2026, 9, 5, tzinfo=timezone.utc),
        "evidence_refs": refs,
        "source_bundle_path": f"/tmp/{source_bundle}",
        "source_bundle_id": source_bundle,
        "source_request_sha256": "2" * 64,
        "source_receipt_sha256": "3" * 64,
        "source_projection_sha256": EVENT_MVE_SOURCE_PROJECTION_SHA256,
        "n2b_bundle_path": "/tmp/" + "4" * 64,
        "n2b_bundle_id": "4" * 64,
        "n2b_request_sha256": "5" * 64,
        "n2b_receipt_sha256": "6" * 64,
        "n1_bundle_path": "/tmp/" + "7" * 64,
        "n1_bundle_id": "7" * 64,
        "n1_request_sha256": "8" * 64,
        "n1_split_policy_sha256": split,
        "qlib_daily_root": "/tmp/qlib",
        "n1_market_calendar_sha256": "9" * 64,
        "n1_market_calendar_row_count": 606,
        "n1_market_calendar_cutoff": date(2026, 6, 30),
        "n1_calendar_data_cutoff": date(2026, 3, 10),
        "trading_calendar": calendar,
        "trading_calendar_sha256": calendar_sha,
        "source_dataset_identity": source_dataset,
        "dataset_identity": dataset,
        "policy_identity": policy,
        "registry_path": "/tmp/registry.jsonl",
        "route_path": "/tmp/current_route.md",
        "repository_root": "/tmp/repo",
        "repository_commit": "f" * 40,
        "output_root": "/tmp/output",
    }
    values.update(overrides)
    return build_financial_event_request(**values)


def test_trial_roster_direction_schema_and_multiplicity_are_frozen() -> None:
    trials = build_default_event_model_trials()
    assert [(item.role, item.selectable) for item in trials] == [
        ("COMPARATOR", False),
        ("CONTROL", False),
        ("CANDIDATE", True),
    ]
    assert trials[1].feature_columns == EVENT_DISCLOSURE_SCHEMA_FEATURES
    assert trials[2].feature_columns == EVENT_SIGNED_SCHEMA_FEATURES
    assert len(EVENT_DIRECTION_BY_TYPE) == 12
    assert set(EVENT_DIRECTION_BY_TYPE.values()) == {-1, 0, 1}
    assert EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX == 83


def test_request_freezes_calendar_evidence_roles_and_all_side_effects_false() -> None:
    request = build_test_request()
    assert len(request.evidence_refs) == 15
    assert request.objective_contract == ObjectiveContract.ALPHA_RANKING
    assert request.study_type == ResearchStudyType.LEARNABILITY_AUDIT
    assert request.decision_use == DecisionUse.NAVIGATION_ONLY
    assert request.planned_trial_count == 3
    assert request.selectable_trial_count == 1
    assert request.cumulative_candidate_index_prior == 80
    assert request.cumulative_primary_comparison_count == 166
    assert request.database_read_allowed is False
    assert request.database_write_allowed is False
    assert request.network_read_allowed is False
    assert request.tushare_read_allowed is False
    assert request.qlib_calendar_read_allowed is True
    assert request.qlib_feature_read_allowed is False
    assert request.sealed_holdout_accessed is False
    assert request.runtime_activation_allowed is False
    assert request.final_model_output_allowed is False
    assert request.deployable is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("feature_schema_hash", "0" * 64),
        ("planned_trial_count", 2),
        ("cumulative_candidate_index", 82),
        ("cumulative_primary_comparison_count", 164),
        ("database_read_allowed", True),
        ("tushare_read_allowed", True),
        ("qlib_feature_read_allowed", True),
        ("runtime_activation_allowed", True),
        ("selected_trial_count", 1),
        ("trading_calendar_sha256", "0" * 64),
    ),
)
def test_request_rejects_contract_drift(field: str, value: object) -> None:
    payload = build_test_request().model_dump(mode="json")
    payload[field] = value
    with pytest.raises((ValidationError, ValueError)):
        FrozenFinancialEventInformationSetRequestV1.model_validate(payload)


def test_receipt_routes_selected_and_zero_without_activation() -> None:
    common = {
        "request_sha256": "a" * 64,
        "result_files_sha256": "b" * 64,
        "resource_report_sha256": "c" * 64,
        "created_at": datetime(2026, 9, 5, tzinfo=timezone.utc),
    }
    zero = build_financial_event_receipt(
        selected_trial_count=0,
        selected_trial_id=None,
        eligible_trial_ids=(),
        evidence_class="EXPLORATORY_NOT_SELECTED_NON_VINTAGE",
        next_task="N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION",
        **common,
    )
    selected = build_financial_event_receipt(
        selected_trial_count=1,
        selected_trial_id="EVENT_SIGNED_CONTENT_V1",
        eligible_trial_ids=("EVENT_SIGNED_CONTENT_V1",),
        evidence_class="EXPLORATORY_CANDIDATE_SELECTED_NON_VINTAGE",
        next_task="N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION",
        **common,
    )
    assert zero.next_task == "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION"
    assert selected.next_task == "N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION"
    assert zero.runtime_eligible is False
    assert selected.final_model_written is False
