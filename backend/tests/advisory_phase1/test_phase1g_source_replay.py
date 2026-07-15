from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    PHASE1G_RESULT_STORE_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
    Phase1GTargetExecutionRequest,
    TargetLabel,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceReplayError,
    REASON_SOURCE_OPERATION_INVALID,
    REASON_SOURCE_REPLAY_INPUT_INVALID,
    parse_phase1g_source_operation,
    replay_phase1g_source_operation,
)
from backend.services.advisory_phase1.readiness_plan import (
    Phase1EEvidenceBinding,
    Phase1EExecutionPlan,
    Phase1EPlannedOperation,
    OperationDisposition,
    PlannedOperationType,
)
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_resolution import (
    AvailabilityRequirement,
    FixtureSourceRevisionResolver,
    SourceRequirement,
    SourceRequirementSet,
    build_source_requirement_common_pit_identity_hash,
)
from backend.services.advisory_phase1.source_revision import SourceRevisionKind

from backend.tests.advisory_phase1.phase1g_test_support import (
    h,
    input_ref,
    phase1e_plan,
)


DECISION_DATE = date(2026, 7, 1)
CUTOFF = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
OBSERVED = datetime(2026, 7, 1, 6, 0, tzinfo=UTC)


def _requirement_set(
    *, manifest_sha256: str = h("9"), alpha_mode: str = "single_alpha"
) -> SourceRequirementSet:
    params = {"lookback": 20, "source_role": "FEATURE_T"}
    common_pit_hash = build_source_requirement_common_pit_identity_hash(
        admission_scope_id="scope-a",
        admission_scope_hash=h("2"),
        handoff_readiness_hash=h("0"),
        program_id="program-a",
        binding_version_id="binding-a",
        package_id="package-a",
        manifest_sha256=manifest_sha256,
        alpha_mode=alpha_mode,
        decision_as_of_trade_date=DECISION_DATE,
        requested_source_cutoff=CUTOFF,
        query_registry_hash=h("d"),
        calendar_hash=h("e"),
        universe_policy_hash=h("f"),
        data_source="DB_HISTORICAL",
        execution_origin="MANUAL_HISTORICAL_RESEARCH",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        research_only=True,
    )
    requirement = SourceRequirement(
        consumer_scope_id="single-alpha",
        source_role="FEATURE_T",
        dataset_name="market.kline_daily_raw",
        query_template_id="fixture-kline-v1",
        query_template_version="1",
        query_template_hash=h("a"),
        bound_parameters=params,
        bound_parameter_hash=canonical_json_sha256(params),
        partition_key={"trade_date": DECISION_DATE.isoformat()},
        revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
        availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
        business_min_date=date(2026, 6, 1),
        business_max_date=DECISION_DATE,
        requested_cutoff=CUTOFF,
        enforced_cutoff_predicate_hash=h("b"),
        common_pit_identity_hash=common_pit_hash,
    )
    return SourceRequirementSet(
        admission_scope_id="scope-a",
        admission_scope_hash=h("2"),
        handoff_readiness_hash=h("0"),
        program_id="program-a",
        binding_version_id="binding-a",
        package_id="package-a",
        manifest_sha256=manifest_sha256,
        alpha_mode=alpha_mode,
        decision_as_of_trade_date=DECISION_DATE,
        requested_source_cutoff=CUTOFF,
        label_as_of_ts=CUTOFF + timedelta(days=5),
        query_registry_hash=h("d"),
        calendar_hash=h("e"),
        universe_policy_hash=h("f"),
        formal_oos_status="RETROSPECTIVE_RESEARCH_ONLY",
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        research_replay_eligible=True,
        requirements=(requirement,),
    )


def _source_event(*, observed_at: datetime = OBSERVED) -> SourceAvailabilityEvent:
    return SourceAvailabilityEvent.from_request(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role="FEATURE_T",
            partition_key={"trade_date": DECISION_DATE.isoformat()},
            revision_id="revision-1",
            event_revision_no=1,
            event_type=SourceAvailabilityEventType.INGESTED,
            schema_fingerprint="fixture-schema-v1",
            row_count=100,
            partition_content_hash=h("7"),
            quality_status="PASS",
            created_by_service_principal="fixture-observer",
        ),
        first_observed_at=observed_at,
    )


def g2_source_case(
    *,
    manifest_sha256: str = h("9"),
    alpha_mode: str = "single_alpha",
    component_ids: tuple[str, ...] = (),
) -> tuple[
    Phase1EExecutionPlanProjection,
    Phase1GTargetExecutionRequest,
    SourceAvailabilityEvent,
]:
    requirements = _requirement_set(
        manifest_sha256=manifest_sha256, alpha_mode=alpha_mode
    )
    event = _source_event()
    resolved = FixtureSourceRevisionResolver().resolve(
        requirement_set=requirements,
        availability_events=(event,),
    )
    assert resolved.source_revision_set is not None
    base = phase1e_plan()
    binding_payload = {"binding": "unit"}
    binding_data = base.evidence_binding.model_dump(
        mode="python", exclude={"evidence_binding_hash"}
    )
    binding_data.update(
        {
            "binding_payload_hash": canonical_json_sha256(binding_payload),
            "manifest_sha256": manifest_sha256,
            "alpha_mode": alpha_mode,
            "manifest_alpha_component_ids": component_ids,
            "package_asset_closure_hash": None,
            "package_lineage_hash": None,
            "stable_signal_semantics_hash": None,
            "decision_clock_hash": None,
        }
    )
    binding = Phase1EEvidenceBinding(**binding_data)
    scope_context = {
        "program_id": "program-a",
        "decision_trade_date": DECISION_DATE.isoformat(),
        "evidence_binding_hash": binding.evidence_binding_hash,
        "package_id": "package-a",
        "manifest_sha256": manifest_sha256,
        "alpha_mode": alpha_mode,
        "admission_scope_id": "scope-a",
        "admission_scope_hash": h("2"),
    }
    source_payload = {
        "schema_version": "advisory_phase1e_source_resolution_operation_v1",
        "scope_context": scope_context,
        "source_requirement_set": requirements.model_dump(mode="json"),
        "source_requirement_set_id": requirements.source_requirement_set_id,
        "source_requirement_set_hash": requirements.source_requirement_set_hash,
        "source_resolution_receipt": resolved.receipt.model_dump(mode="json"),
    }
    capture_plan = {
        "plan_hash": h("5"),
        "signal_source_revision_set_id": resolved.source_revision_set.source_revision_set_id,
        "signal_source_revision_set_hash": resolved.source_revision_set.source_revision_set_hash,
    }
    observation_payload = {
        "schema_version": "advisory_phase1e_request_template_v1",
        "operation": "observation_capture",
        "scope_context": scope_context,
        "source_resolution": {
            "source_resolution_receipt_hash": resolved.receipt.source_resolution_receipt_hash,
        },
        "capture_plan": capture_plan,
        "required_inputs": [],
    }
    slots = next(
        item.required_output_slots
        for item in base.planned_operations
        if item.operation_type is PlannedOperationType.OBSERVATION_CAPTURE
    )
    operations = (
        Phase1EPlannedOperation(
            operation_type=PlannedOperationType.SOURCE_RESOLUTION,
            operation_disposition=OperationDisposition.COMPLETE_REQUEST,
            contract_schema_version="advisory_phase1e_source_resolution_operation_v1",
            complete_request_payload=source_payload,
            complete_request_hash=canonical_json_sha256(source_payload),
        ),
        Phase1EPlannedOperation(
            operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
            operation_disposition=OperationDisposition.SEMANTIC_TEMPLATE,
            contract_schema_version="advisory_phase1e_request_template_v1",
            request_template_payload=observation_payload,
            request_template_hash=canonical_json_sha256(observation_payload),
            required_output_slots=slots,
            unresolved_input_refs=slots,
        ),
    )
    plan_data = base.model_dump(mode="python", exclude={"plan_hash", "plan_id"})
    plan_data.update(
        {
            "scope_key": {
                "program_id": "program-a",
                "decision_trade_date": DECISION_DATE,
                "package_id": "package-a",
                "manifest_sha256": manifest_sha256,
                "admission_scope_id": "scope-a",
                "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
            },
            "evidence_binding": binding,
            "planned_operations": operations,
        }
    )
    domain_plan = Phase1EExecutionPlan(**plan_data)
    projection = Phase1EExecutionPlanProjection.model_validate(
        domain_plan.model_dump(mode="json")
    )
    target = Phase1GTargetExecutionRequest(
        target_label=TargetLabel.DEV,
        release_schema_receipt_ref=input_ref(
            kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
            semantic_hash=h("1"),
            file_sha256=h("2"),
        ),
        phase1e_plan_ref=input_ref(
            kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
            semantic_hash=projection.plan_hash,
            file_sha256=h("4"),
        ),
        phase1e_plan_id=projection.plan_id,
        phase1e_plan_hash=projection.plan_hash,
        source_operation_hash=operations[0].complete_request_hash,
        observation_template_hash=operations[1].request_template_hash,
        program_id="program-a",
        decision_trade_date=DECISION_DATE,
        admission_scope_id="scope-a",
        admission_scope_hash=h("2"),
        capture_policy_registry_id=DEFAULT_CAPTURE_POLICY_REGISTRY.registry_id,
        capture_policy_registry_version=DEFAULT_CAPTURE_POLICY_REGISTRY.registry_version,
        capture_policy_registry_hash=str(DEFAULT_CAPTURE_POLICY_REGISTRY.registry_hash),
        result_store_policy_hash=str(
            PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash
        ),
        requested_at=datetime(2026, 7, 15, 1, 0, tzinfo=UTC),
    )
    return projection, target, event


def test_source_operation_parses_and_same_cutoff_replay_is_exact() -> None:
    plan, target, event = g2_source_case()
    projection = parse_phase1g_source_operation(
        phase1e_plan=plan, target_request=target
    )
    result = replay_phase1g_source_operation(
        projection=projection, availability_events=(event,)
    )

    assert result.embedded_resolution_receipt == result.replayed_resolution_receipt
    assert (
        result.source_revision_set.source_revision_set_hash
        == projection.embedded_receipt.source_revision_set_hash
    )
    assert result.freeze_intent.source_revision_set == result.source_revision_set
    assert (
        result.expected_source_event_refs[0].event_content_hash
        == event.event_content_hash
    )
    assert result.model_dump(mode="json") == replay_phase1g_source_operation(
        projection=projection,
        availability_events=(event,),
    ).model_dump(mode="json")


def test_source_replay_ignores_valid_post_cutoff_chain_tail_without_changing_receipt() -> (
    None
):
    plan, target, event = g2_source_case()
    late_payload = event.input.model_dump(
        exclude={"partition_chain_key", "append_request_hash", "first_observed_at"}
    )
    late_payload.update(
        revision_id="revision-2",
        event_revision_no=2,
        event_type=SourceAvailabilityEventType.CORRECTED,
        predecessor_event_hash=event.event_content_hash,
        partition_content_hash=h("8"),
    )
    late = SourceAvailabilityEvent.from_request(
        SourceAvailabilityEventRequest.model_validate(late_payload),
        first_observed_at=CUTOFF + timedelta(hours=1),
    )
    projection = parse_phase1g_source_operation(
        phase1e_plan=plan, target_request=target
    )
    baseline = replay_phase1g_source_operation(
        projection=projection, availability_events=(event,)
    )
    replayed = replay_phase1g_source_operation(
        projection=projection, availability_events=(event, late)
    )
    assert replayed.source_replay_result_hash == baseline.source_replay_result_hash


def test_source_replay_rejects_unrelated_chain_and_source_operation_tamper() -> None:
    plan, target, event = g2_source_case()
    projection = parse_phase1g_source_operation(
        phase1e_plan=plan, target_request=target
    )
    unrelated_payload = event.input.model_dump(
        exclude={"partition_chain_key", "append_request_hash", "first_observed_at"}
    )
    unrelated_payload["partition_key"] = {"trade_date": "2026-06-30"}
    unrelated = SourceAvailabilityEvent.from_request(
        SourceAvailabilityEventRequest.model_validate(unrelated_payload),
        first_observed_at=OBSERVED,
    )
    with pytest.raises(Phase1GSourceReplayError) as error:
        replay_phase1g_source_operation(
            projection=projection, availability_events=(event, unrelated)
        )
    assert error.value.reason_code == REASON_SOURCE_REPLAY_INPUT_INVALID

    wrong_target = target.model_copy(update={"source_operation_hash": h("f")})
    with pytest.raises(Phase1GSourceReplayError) as error:
        parse_phase1g_source_operation(phase1e_plan=plan, target_request=wrong_target)
    assert error.value.reason_code == REASON_SOURCE_OPERATION_INVALID
    assert "source_requirement_set" not in error.value.context


def test_source_replay_contract_hash_count_and_capture_ref_invariants_fail_closed() -> (
    None
):
    plan, target, event = g2_source_case()
    operation = parse_phase1g_source_operation(phase1e_plan=plan, target_request=target)
    replay = replay_phase1g_source_operation(
        projection=operation, availability_events=(event,)
    )

    operation_data = operation.model_dump(mode="python")
    operation_data["expected_capture_source_sets"] = (
        *operation.expected_capture_source_sets,
        operation.expected_capture_source_sets[0],
    )
    operation_data["source_operation_projection_hash"] = None
    with pytest.raises(ValidationError):
        type(operation).model_validate(operation_data)

    operation_data = operation.model_dump(mode="python")
    operation_data["source_operation_projection_hash"] = "f" * 64
    with pytest.raises(ValidationError):
        type(operation).model_validate(operation_data)

    freeze_data = replay.freeze_intent.model_dump(mode="python")
    freeze_data["expected_member_count"] += 1
    freeze_data["freeze_intent_hash"] = None
    with pytest.raises(ValidationError):
        type(replay.freeze_intent).model_validate(freeze_data)

    freeze_data = replay.freeze_intent.model_dump(mode="python")
    freeze_data["expected_member_hash"] = "f" * 64
    freeze_data["freeze_intent_hash"] = None
    with pytest.raises(ValidationError):
        type(replay.freeze_intent).model_validate(freeze_data)

    replay_data = replay.model_dump(mode="python")
    replay_data["source_revision_member_count"] += 1
    replay_data["source_replay_result_hash"] = None
    with pytest.raises(ValidationError):
        type(replay).model_validate(replay_data)

    replay_data = replay.model_dump(mode="python")
    replay_data["source_revision_member_hash"] = "f" * 64
    replay_data["source_replay_result_hash"] = None
    with pytest.raises(ValidationError):
        type(replay).model_validate(replay_data)

    with pytest.raises(TypeError, match="cannot be mutated"):
        replay.source_revision_set.members[0].partition_key["silent_mutation"] = True

    mismatched_ref = operation.expected_capture_source_sets[0].model_copy(
        update={"source_revision_set_hash": "f" * 64}
    )
    with pytest.raises(ValidationError):
        operation.model_copy(update={"expected_capture_source_sets": (mismatched_ref,)})
    mismatched_operation = type(operation).model_construct(
        **{
            **operation.__dict__,
            "expected_capture_source_sets": (mismatched_ref,),
        }
    )
    with pytest.raises(Phase1GSourceReplayError) as error:
        replay_phase1g_source_operation(
            projection=mismatched_operation, availability_events=(event,)
        )
    assert error.value.reason_code == "ADVISORY_PHASE1G_SOURCE_REPLAY_MISMATCH"
