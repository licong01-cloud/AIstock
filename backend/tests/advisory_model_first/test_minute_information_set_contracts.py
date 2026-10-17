from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.minute_information_set_contracts import (
    MINUTE_MVE_COMPARATOR_FEATURES,
    MINUTE_MVE_EXPANDED_FEATURES,
    MINUTE_MVE_FEATURE_SCHEMA_HASH,
    MINUTE_MVE_FEATURE_SCHEMA_VERSION,
    MINUTE_MVE_MINIMUM_FEATURE_COVERAGE,
    MINUTE_MVE_PROVIDER_URI,
    MINUTE_MVE_RAW_ECONOMIC_FEATURES,
    MINUTE_MVE_SOURCE_FIELDS,
    MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES,
    FrozenMinuteInformationSetRequestV1,
    build_default_minute_model_trials,
    build_minute_information_set_receipt,
    build_minute_information_set_request,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _refs() -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_minute_leg_manifest",
        "n3_minute_leg_receipt",
        "n3_minute_n2a_manifest",
        "n3_minute_n2a_request",
        "n3_minute_n2a_full_universe",
        "n3_minute_n1_manifest",
        "n3_minute_n1_cpcv",
        "n3_minute_n1_regime_daily",
        "n3_minute_source_spike_receipt",
        "n3_minute_source_meta",
        "n3_minute_source_calendar",
        "n3_minute_source_instruments",
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


def _request() -> FrozenMinuteInformationSetRequestV1:
    refs = _refs()
    source_dataset = "a" * 64
    route_dataset = "b" * 64
    split = "c" * 64
    minute_content = "d" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "route_dataset_identity": route_dataset,
            "n1_split_policy_sha256": split,
            "minute_source_content_sha256": minute_content,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    return build_minute_information_set_request(
        created_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
        evidence_refs=refs,
        leg_bundle_path="/tmp/" + "1" * 64,
        leg_bundle_id="1" * 64,
        leg_request_sha256="2" * 64,
        leg_receipt_sha256="3" * 64,
        n2a_bundle_path="/tmp/" + "4" * 64,
        n2a_bundle_id="4" * 64,
        n2a_request_sha256="5" * 64,
        n2a_receipt_sha256="6" * 64,
        n1_bundle_path="/tmp/" + "7" * 64,
        n1_bundle_id="7" * 64,
        n1_request_sha256="8" * 64,
        n1_split_policy_sha256=split,
        source_spike_receipt_path="/tmp/source-spike.json",
        source_spike_receipt_sha256="9" * 64,
        source_dataset_identity=source_dataset,
        route_dataset_identity=route_dataset,
        minute_source_content_sha256=minute_content,
        minute_source_file_count=36_024,
        dataset_identity=dataset,
        policy_identity="e" * 64,
        registry_path="/tmp/registry.jsonl",
        route_path="/tmp/route.md",
        repository_root="/tmp/repo",
        repository_commit="f" * 40,
        output_root="/tmp/output",
    )


def test_default_trial_roster_and_feature_schema_are_exact() -> None:
    trials = build_default_minute_model_trials()
    assert len(trials) == 2
    assert trials[0].feature_columns == MINUTE_MVE_COMPARATOR_FEATURES
    assert trials[1].feature_columns == MINUTE_MVE_EXPANDED_FEATURES
    assert trials[0].role == "COMPARATOR"
    assert trials[1].role == "CANDIDATE"
    assert len(MINUTE_MVE_SOURCE_FIELDS) == 8
    assert len(MINUTE_MVE_RAW_ECONOMIC_FEATURES) == 8
    assert len(MINUTE_MVE_EXPANDED_FEATURES) == 11
    assert MINUTE_MVE_FEATURE_SCHEMA_HASH == canonical_json_sha256(
        {
            "schema_version": MINUTE_MVE_FEATURE_SCHEMA_VERSION,
            "decision_clock": "T_DAY_ONLY_THROUGH_15_00_AFTER_CLOSE_RANKING",
            "source_fields": list(MINUTE_MVE_SOURCE_FIELDS),
            "raw_economic_features": list(MINUTE_MVE_RAW_ECONOMIC_FEATURES),
            "comparator_features": list(MINUTE_MVE_COMPARATOR_FEATURES),
            "expanded_features": list(MINUTE_MVE_EXPANDED_FEATURES),
            "minimum_feature_coverage": MINUTE_MVE_MINIMUM_FEATURE_COVERAGE,
            "market_wide_empty_slot_policy": "NO_VALID_OHLC_BAR_EXCLUDE_FROM_EFFECTIVE_DENOMINATOR_AND_RECORD",
            "session_wide_single_bar_deficit_policy": "KEEP_RAW_240_OF_241_COVERAGE_AND_REPORT_NORMALIZED_SESSION_CLASS",
            "normal_missing_policy": "KEEP_ALL_KEYS_TRAIN_FOLD_MEDIAN_WITH_AVAILABILITY_COVERAGE",
            "rank_semantics": "SAME_DATE_FINITE_AVERAGE_PCT_ASCENDING",
        }
    )


def test_request_freezes_minute_read_and_business_write_boundaries() -> None:
    request = _request()
    assert request.planned_trial_count == 2
    assert request.familywise_hypothesis_count == 4
    assert request.expected_ready_path_count == 28
    assert request.expected_oof_predictions_per_row == 7
    assert request.minute_provider_uri == MINUTE_MVE_PROVIDER_URI
    assert request.expected_session_wide_single_bar_deficit_dates == MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES
    assert request.resource_max_wall_seconds is None
    assert request.database_read_allowed is False
    assert request.network_read_allowed is False
    assert request.qlib_read_allowed is True
    assert request.qlib_daily_read_allowed is False
    assert request.minute_data_read_allowed is True
    assert request.sealed_holdout_accessed is False
    assert request.final_model_output_allowed is False
    assert request.deployable is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("feature_schema_hash", "0" * 64),
        ("minimum_feature_coverage", 0.5),
        ("familywise_hypothesis_count", 3),
        ("resource_max_wall_seconds", 1),
        ("database_read_allowed", True),
        ("qlib_read_allowed", False),
        ("selected_trial_count", 1),
    ),
)
def test_request_rejects_contract_drift(field: str, value: object) -> None:
    payload = _request().model_dump(mode="json")
    payload[field] = value
    with pytest.raises((ValidationError, ValueError)):
        FrozenMinuteInformationSetRequestV1.model_validate(payload)


def test_receipt_routes_selected_zero_and_one_without_activation() -> None:
    zero = build_minute_information_set_receipt(
        request_sha256="a" * 64,
        selected_trial_count=0,
        selected_trial_id=None,
        eligible_trial_ids=(),
        next_task="N3_QE_ALPHA_GENERATOR_MVE_DESIGN",
        source_identity_sha256="b" * 64,
        result_files_sha256="c" * 64,
        resource_report_sha256="d" * 64,
        created_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
    )
    selected = build_minute_information_set_receipt(
        request_sha256="a" * 64,
        selected_trial_count=1,
        selected_trial_id="N3_MINUTE_INFORMATION_EXPANDED_V1",
        eligible_trial_ids=("N3_MINUTE_INFORMATION_EXPANDED_V1",),
        next_task="N3_MINUTE_INFORMATION_SET_CONFIRMATION_DESIGN",
        source_identity_sha256="b" * 64,
        result_files_sha256="c" * 64,
        resource_report_sha256="d" * 64,
        created_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
    )
    assert zero.next_task == "N3_QE_ALPHA_GENERATOR_MVE_DESIGN"
    assert selected.next_task == "N3_MINUTE_INFORMATION_SET_CONFIRMATION_DESIGN"
    assert zero.runtime_eligible is False and selected.final_model_written is False
