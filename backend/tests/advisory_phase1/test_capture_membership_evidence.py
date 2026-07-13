"""Phase 1C-2 source/selector evidence membership contracts."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.capture_foundation import CapturePlan
from backend.services.advisory_phase1.capture_membership_evidence import (
    REASON_CAPTURE_SELECTED_MAPPING_INVALID,
    REASON_CAPTURE_SOURCE_RESOLUTION_INVALID,
    build_phase1c2_capture_memberships,
)
from backend.services.advisory_phase1.observation_selector import (
    FixtureObservationVersionSelector,
    ObservationSelectionPolicy,
    ObservationSelectionRequest,
    ObservationSelectionStatus,
    ObservationStatus,
    build_fixture_observation_version,
)
from backend.services.advisory_phase1.source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    SourceLedgerError,
)
from backend.services.advisory_phase1.source_resolution import (
    FixtureSourceRevisionResolver,
    SourceRequirement,
    SourceRequirementSet,
    build_source_requirement_common_pit_identity_hash,
)
from backend.services.advisory_phase1.source_revision import AvailabilityRequirement, SourceRevisionKind


UTC = timezone.utc
NOW = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _common_pit_hash() -> str:
    return build_source_requirement_common_pit_identity_hash(
        admission_scope_id="scope-membership",
        admission_scope_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        program_id="program-membership",
        binding_version_id="binding-membership",
        package_id="package-membership",
        manifest_sha256=HASH_E,
        alpha_mode="single_alpha",
        decision_as_of_trade_date=date(2026, 6, 30),
        requested_source_cutoff=NOW,
        query_registry_hash=HASH_F,
        calendar_hash=HASH_A,
        universe_policy_hash=HASH_B,
        data_source="DB_HISTORICAL",
        execution_origin="MANUAL_HISTORICAL_RESEARCH",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        research_only=True,
    )


def _resolution_result(*, with_event: bool = True):
    parameters = {"lookback": 20}
    requirement = SourceRequirement(
        consumer_scope_id="component-one",
        source_role="FEATURE_T",
        dataset_name="market.kline_daily_raw",
        query_template_id="fixture-template",
        query_template_version="1",
        query_template_hash=HASH_A,
        bound_parameters=parameters,
        bound_parameter_hash=canonical_json_sha256(parameters),
        partition_key={"trade_date": "2026-06-30"},
        revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
        availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
        business_min_date=date(2026, 6, 1),
        business_max_date=date(2026, 6, 30),
        requested_cutoff=NOW,
        enforced_cutoff_predicate_hash=HASH_B,
        common_pit_identity_hash=_common_pit_hash(),
    )
    requirement_set = SourceRequirementSet(
        admission_scope_id="scope-membership",
        admission_scope_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        program_id="program-membership",
        binding_version_id="binding-membership",
        package_id="package-membership",
        manifest_sha256=HASH_E,
        alpha_mode="single_alpha",
        decision_as_of_trade_date=date(2026, 6, 30),
        requested_source_cutoff=NOW,
        label_as_of_ts=NOW,
        query_registry_hash=HASH_F,
        calendar_hash=HASH_A,
        universe_policy_hash=HASH_B,
        formal_oos_status="RETROSPECTIVE_RESEARCH_ONLY",
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        requirements=(requirement,),
    )
    events = ()
    if with_event:
        ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: NOW)
        events = (
            ledger.append(
                SourceAvailabilityEventRequest(
                    dataset_name="market.kline_daily_raw",
                    source_role="FEATURE_T",
                    partition_key={"trade_date": "2026-06-30"},
                    revision_id="r1",
                    event_revision_no=1,
                    event_type=SourceAvailabilityEventType.INGESTED,
                    schema_fingerprint="fixture-schema-v1",
                    row_count=100,
                    partition_content_hash=HASH_C,
                    quality_status="PASS",
                    created_by_service_principal="fixture-observer",
                )
            ),
        )
    return requirement_set, FixtureSourceRevisionResolver().resolve(
        requirement_set=requirement_set,
        availability_events=events,
    )


def _plan(source_set_id: str, source_set_hash: str) -> CapturePlan:
    return CapturePlan(
        selection_run_id="selection-membership",
        package_id="package-membership",
        manifest_sha256=HASH_E,
        decision_as_of_trade_date=date(2026, 6, 30).isoformat(),
        selection_as_of_trade_date=date(2026, 6, 30).isoformat(),
        target_trade_date=date(2026, 7, 1).isoformat(),
        decision_cutoff_ts=NOW,
        alpha_mode="single_alpha",
        selection_runtime_semantics_hash=HASH_A,
        package_effective_config_hash=HASH_B,
        calendar_version="fixture-calendar-v1",
        calendar_hash=HASH_A,
        stable_signal_semantics_hash=HASH_D,
        canonical_signal_scope_hash=HASH_E,
        phase0a_audit_id="audit-membership",
        phase0a_audit_manifest_hash=HASH_F,
        handoff_readiness_hash=HASH_D,
        admission_scope_id="scope-membership",
        admission_scope_hash=HASH_C,
        signal_source_revision_set_id=source_set_id,
        signal_source_revision_set_hash=source_set_hash,
        phase0a_signal_context_hash=HASH_A,
        evidence_bundle_hash=HASH_B,
        selection_evidence_id="selection-evidence-membership",
        selection_evidence_hash=HASH_C,
        selection_run_content_hash=HASH_D,
        selection_score_artifact_id="selection-artifact-membership",
        selection_score_artifact_hash=HASH_E,
        runtime_profile_version_id="runtime-profile-membership",
        runtime_profile_version_hash=HASH_F,
        hmm_snapshot_status="NOT_APPLICABLE",
        risk_policy_hash=HASH_A,
        universe_policy_hash=HASH_B,
        symbol_normalization_policy_hash=HASH_C,
        valid_no_candidate=False,
        evidence_available_at=NOW,
        audit_target_id="audit-target-membership",
        target_scope_hash=HASH_D,
        capability="HISTORICAL_RESEARCH",
        oos_interval_id="oos-membership",
        oos_interval_hash=HASH_E,
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        signal_evidence_level="RETROSPECTIVE_RESEARCH_ONLY",
        effective_cutoff_date=date(2026, 6, 30).isoformat(),
        program_id="program-membership",
        binding_version_id="binding-membership",
        source_run_id="source-run-membership",
        lineage_source_type="PHASE0A_AUDIT",
    )


def test_capture_memberships_bind_requirement_receipt_source_set_and_selected_mapping() -> None:
    requirement_set, result = _resolution_result()
    assert result.source_revision_set is not None
    plan = _plan(result.source_revision_set.source_revision_set_id, result.source_revision_set.source_revision_set_hash)
    observation = build_fixture_observation_version(
        canonical_signal_id=f"acs_{HASH_E[:20]}",
        observation_revision_no=1,
        supersedes_observation_version_id=None,
        evidence_available_at=NOW,
        admission_scope_id="scope-membership",
        admission_scope_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        signal_source_revision_set_id=result.source_revision_set.source_revision_set_id,
        signal_source_revision_set_hash=result.source_revision_set.source_revision_set_hash,
        observation_status=ObservationStatus.COMPLETE,
        capability="HISTORICAL_RESEARCH",
        stage_content_hashes=(HASH_E,),
    )
    mapping = FixtureObservationVersionSelector().select(
        request=ObservationSelectionRequest(
            selection_policy=ObservationSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            canonical_signal_id=f"acs_{HASH_E[:20]}",
            requested_source_cutoff=NOW,
            required_observation_status=ObservationStatus.COMPLETE,
            required_capability="HISTORICAL_RESEARCH",
            admission_scope_id="scope-membership",
            admission_scope_hash=HASH_C,
            handoff_readiness_hash=HASH_D,
            signal_source_revision_set_hash=result.source_revision_set.source_revision_set_hash,
        ),
        observation_versions=(observation,),
    )
    assert mapping.selection_status is ObservationSelectionStatus.SELECTED

    memberships = build_phase1c2_capture_memberships(
        plan=plan,
        requirement_set=requirement_set,
        resolution_result=result,
        selected_mapping=mapping,
    )

    assert [item.evidence_role for item in memberships] == [
        "selected_observation_mapping",
        "source_requirement_set",
        "source_resolution_receipt",
        "source_revision_set",
    ]
    assert {item.evidence_content_hash for item in memberships} == {
        requirement_set.source_requirement_set_hash,
        result.receipt.source_resolution_receipt_hash,
        result.source_revision_set.source_revision_set_hash,
        mapping.selected_mapping_hash,
    }


def test_capture_membership_rejects_gap_only_resolution_and_divergent_mapping() -> None:
    requirement_set, gap_result = _resolution_result(with_event=False)
    with pytest.raises(SourceLedgerError, match=REASON_CAPTURE_SOURCE_RESOLUTION_INVALID):
        build_phase1c2_capture_memberships(
            plan=_plan("unavailable-source-set", HASH_A),
            requirement_set=requirement_set,
            resolution_result=gap_result,
        )

    requirement_set, result = _resolution_result()
    assert result.source_revision_set is not None
    plan = _plan(result.source_revision_set.source_revision_set_id, result.source_revision_set.source_revision_set_hash)
    unavailable_mapping = FixtureObservationVersionSelector().select(
        request=ObservationSelectionRequest(
            selection_policy=ObservationSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            canonical_signal_id="acs-empty",
            requested_source_cutoff=NOW,
            required_observation_status=ObservationStatus.COMPLETE,
            required_capability="HISTORICAL_RESEARCH",
            admission_scope_id="scope-membership",
            admission_scope_hash=HASH_C,
            handoff_readiness_hash=HASH_D,
            signal_source_revision_set_hash=result.source_revision_set.source_revision_set_hash,
        ),
        observation_versions=(),
    )
    with pytest.raises(SourceLedgerError, match=REASON_CAPTURE_SELECTED_MAPPING_INVALID):
        build_phase1c2_capture_memberships(
            plan=plan,
            requirement_set=requirement_set,
            resolution_result=result,
            selected_mapping=unavailable_mapping,
        )


def test_capture_membership_rejects_divergent_frozen_plan_identity() -> None:
    requirement_set, result = _resolution_result()
    assert result.source_revision_set is not None
    plan = _plan(result.source_revision_set.source_revision_set_id, result.source_revision_set.source_revision_set_hash)
    divergent_plan = plan.model_copy(update={"calendar_hash": HASH_C})

    with pytest.raises(SourceLedgerError, match=REASON_CAPTURE_SOURCE_RESOLUTION_INVALID):
        build_phase1c2_capture_memberships(
            plan=divergent_plan,
            requirement_set=requirement_set,
            resolution_result=result,
        )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    (
        ("canonical_signal_id", "acs-wrong-signal"),
        ("requested_source_cutoff", NOW + timedelta(minutes=1)),
        ("required_capability", "WRONG_CAPABILITY"),
    ),
)
def test_capture_membership_rejects_mapping_from_another_signal_cutoff_or_capability(
    field_name: str,
    field_value: object,
) -> None:
    requirement_set, result = _resolution_result()
    assert result.source_revision_set is not None
    plan = _plan(result.source_revision_set.source_revision_set_id, result.source_revision_set.source_revision_set_hash)
    observation = build_fixture_observation_version(
        canonical_signal_id=f"acs_{HASH_E[:20]}",
        observation_revision_no=1,
        supersedes_observation_version_id=None,
        evidence_available_at=NOW,
        admission_scope_id=plan.admission_scope_id,
        admission_scope_hash=plan.admission_scope_hash,
        handoff_readiness_hash=plan.handoff_readiness_hash,
        signal_source_revision_set_id=result.source_revision_set.source_revision_set_id,
        signal_source_revision_set_hash=result.source_revision_set.source_revision_set_hash,
        observation_status=ObservationStatus.COMPLETE,
        capability=plan.capability,
        stage_content_hashes=(HASH_E,),
    )
    mapping = FixtureObservationVersionSelector().select(
        request=ObservationSelectionRequest(
            selection_policy=ObservationSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1,
            canonical_signal_id=f"acs_{HASH_E[:20]}",
            requested_source_cutoff=NOW,
            required_observation_status=ObservationStatus.COMPLETE,
            required_capability=plan.capability,
            admission_scope_id=plan.admission_scope_id,
            admission_scope_hash=plan.admission_scope_hash,
            handoff_readiness_hash=plan.handoff_readiness_hash,
            signal_source_revision_set_hash=result.source_revision_set.source_revision_set_hash,
        ),
        observation_versions=(observation,),
    ).model_copy(update={field_name: field_value})

    with pytest.raises(SourceLedgerError, match=REASON_CAPTURE_SELECTED_MAPPING_INVALID):
        build_phase1c2_capture_memberships(
            plan=plan,
            requirement_set=requirement_set,
            resolution_result=result,
            selected_mapping=mapping,
        )


def test_capture_membership_rejects_plan_cutoff_divergent_from_requirement_set() -> None:
    requirement_set, result = _resolution_result()
    assert result.source_revision_set is not None
    plan = _plan(result.source_revision_set.source_revision_set_id, result.source_revision_set.source_revision_set_hash)

    with pytest.raises(SourceLedgerError, match=REASON_CAPTURE_SOURCE_RESOLUTION_INVALID):
        build_phase1c2_capture_memberships(
            plan=plan.model_copy(update={"decision_cutoff_ts": NOW + timedelta(minutes=1)}),
            requirement_set=requirement_set,
            resolution_result=result,
        )
