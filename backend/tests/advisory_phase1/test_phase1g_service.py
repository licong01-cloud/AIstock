from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchRequest,
    CapturePlan,
)
from backend.services.advisory_phase1.control_binding import ControlBindingEvent
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    REASON_ATTEMPT_RECEIPT_STORE_FAILED,
    REASON_CAPTURE_TIMEOUT,
    REASON_OPERATION_DEFERRED,
    REASON_TARGET_DIAGNOSTIC,
    REASON_UNEXPECTED_ERROR,
    Phase1GOutputArtifactKind,
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EOperationDisposition,
    Phase1EPlanUnitKind,
    Phase1EPlannedOperationType,
)
from backend.services.advisory_phase1.phase1g_service import (
    TRACE_CAPTURE_POLICY,
    Phase1GExitClass,
    Phase1GOperationStatus,
    Phase1GService,
    Phase1GTargetInvocationOutcome,
)
from backend.services.advisory_phase1.stage_trace import TraceCaptureBinding
from backend.tests.advisory_phase1.test_phase1g_historical_trace_projection import (
    historical_raw_empty_case,
)
from backend.tests.advisory_phase1.test_capture_foundation import _plan


def _service_shell() -> Phase1GService:
    service = object.__new__(Phase1GService)
    service._registry = DEFAULT_CAPTURE_POLICY_REGISTRY
    service._trace_policy = TRACE_CAPTURE_POLICY
    service._monotonic = iter((1.0, 1.0)).__next__
    return service


def _capture_plan_and_target() -> tuple[CapturePlan, object]:
    case = historical_raw_empty_case()
    capture_plan = _plan(alpha_mode="single_alpha")
    target_payload = case["target"].model_dump(
        mode="python", exclude={"request_hash"}
    )
    target_payload.update(
        admission_scope_id=capture_plan.admission_scope_id,
        admission_scope_hash=capture_plan.admission_scope_hash,
    )
    target = Phase1GTargetExecutionRequest.model_validate(target_payload)
    return capture_plan, target


def _phase1e_with_exact_slots() -> SimpleNamespace:
    slots = tuple(
        {"slot": slot}
        for slot in (
            "control_binding_event_hash",
            "capture_batch_id",
            "capture_fencing_token",
        )
    )
    operation = SimpleNamespace(
        operation_type=Phase1EPlannedOperationType.OBSERVATION_CAPTURE,
        required_output_slots=slots,
        expected_final_request_hash=None,
    )
    return SimpleNamespace(planned_operations=(operation,))


def test_preview_and_actual_request_share_one_semantic_hash() -> None:
    service = _service_shell()
    capture_plan, target = _capture_plan_and_target()
    desired = service._desired_control(target, (capture_plan,))
    preview = service._preview_request(
        target=target,
        capture_plans=(capture_plan,),
        desired_control=desired,
        phase1e_plan=_phase1e_with_exact_slots(),
    )
    event = ControlBindingEvent.from_request(
        desired, bound_at=datetime(2026, 7, 15, tzinfo=timezone.utc)
    )
    loaded = SimpleNamespace(
        target_request=target,
        preview=preview,
        capture_plans=(capture_plan,),
    )
    request = service._materialize_request(loaded=loaded, event=event, attempt_no=3)

    assert request.capture_batch_id == f"acb_{preview.capture_request_hash[:20]}_a3"
    assert request.capture_request_hash == preview.capture_request_hash
    assert request.canonical_payload() == preview.canonical_payload
    assert request.data_source == "DB_HISTORICAL"
    assert request.execution_origin == "ADVISORY_RUN"
    assert request.execution_prohibited is True


def test_desired_binding_is_versioned_configuration_not_approval() -> None:
    service = _service_shell()
    capture_plan, target = _capture_plan_and_target()
    desired = service._desired_control(target, (capture_plan,))

    assert desired.enabled is True
    assert desired.created_by_service_principal == "advisory_phase1g_capture_service"
    assert desired.admission_scope_set_hash == canonical_json_sha256(
        {"admission_scope_hashes": [target.admission_scope_hash]}
    )
    assert desired.config_payload["capture_policy_hash"] == TRACE_CAPTURE_POLICY.policy_hash
    assert not any(
        token in key.lower()
        for key in desired.config_payload
        for token in ("approval", "role", "authorization", "backup")
    )


def test_preview_rejects_missing_or_extra_phase1e_slots() -> None:
    service = _service_shell()
    capture_plan, target = _capture_plan_and_target()
    desired = service._desired_control(target, (capture_plan,))
    invalid = SimpleNamespace(
        planned_operations=(
            SimpleNamespace(
                operation_type=Phase1EPlannedOperationType.OBSERVATION_CAPTURE,
                required_output_slots=({"slot": "control_binding_event_hash"},),
                expected_final_request_hash=None,
            ),
        )
    )

    with pytest.raises(Exception, match="output slots are not exact"):
        service._preview_request(
            target=target,
            capture_plans=(capture_plan,),
            desired_control=desired,
            phase1e_plan=invalid,
        )


def test_capture_plan_keeps_phase1e_handoff_bundle_authority() -> None:
    capture_plan, _target = _capture_plan_and_target()
    operation = SimpleNamespace(
        operation_type=Phase1EPlannedOperationType.OBSERVATION_CAPTURE,
        request_template_payload={"capture_plan": capture_plan.model_dump(mode="json")},
    )
    plan = SimpleNamespace(
        planned_operations=(operation,),
        evidence_binding=SimpleNamespace(
            phase1_handoff_bundle_hash=capture_plan.evidence_bundle_hash
        ),
    )

    assert Phase1GService._capture_plans(plan) == (capture_plan,)
    plan.evidence_binding.phase1_handoff_bundle_hash = "f" * 64
    with pytest.raises(Exception, match="handoff bundle"):
        Phase1GService._capture_plans(plan)


def test_capture_request_hash_excludes_attempt_specific_binding_fields() -> None:
    service = _service_shell()
    capture_plan, target = _capture_plan_and_target()
    first = service._trace_binding(
        target=target,
        handoff_readiness_hash=capture_plan.handoff_readiness_hash,
        control_event_hash="1" * 64,
        capture_batch_id="batch-1",
        fencing_token=1,
    )
    second = service._trace_binding(
        target=target,
        handoff_readiness_hash=capture_plan.handoff_readiness_hash,
        control_event_hash="2" * 64,
        capture_batch_id="batch-2",
        fencing_token=9,
    )
    first_request = CaptureBatchRequest(
        capture_batch_id="batch-1", binding=first, plans=(capture_plan,)
    )
    second_payload = second.model_dump(mode="python", exclude={"binding_hash"})
    second_payload["capture_fencing_token"] = 1
    second = TraceCaptureBinding.model_validate(second_payload)
    second_request = CaptureBatchRequest(
        capture_batch_id="batch-2", binding=second, plans=(capture_plan,)
    )

    assert first_request.capture_request_hash == second_request.capture_request_hash


def test_success_outcome_requires_both_durable_artifacts() -> None:
    with pytest.raises(ValueError, match="durable result and attempt"):
        Phase1GTargetInvocationOutcome(
            target_request_hash="1" * 64,
            target_plan_hash="2" * 64,
            operation_status=Phase1GOperationStatus.SUCCESS,
            dml_executed=False,
        )

    failure = Phase1GTargetInvocationOutcome(
        target_request_hash="1" * 64,
        target_plan_hash="2" * 64,
        operation_status=Phase1GOperationStatus.FAILED,
        reason_codes=(REASON_ATTEMPT_RECEIPT_STORE_FAILED,),
        dml_executed=False,
    )
    assert failure.attempt_receipt_ref is None
    assert failure.reason_codes == (REASON_ATTEMPT_RECEIPT_STORE_FAILED,)
    assert Phase1GOutputArtifactKind.BATCH_RECEIPT.value == "BATCH_RECEIPT"


def test_phase1e_disposition_is_classified_before_projection_reads() -> None:
    diagnostic = SimpleNamespace(
        plan_unit_kind=Phase1EPlanUnitKind.TARGET_DIAGNOSTIC,
        reason_codes=("ADVISORY_PHASE1E_NO_ADMISSION_SCOPE",),
        capacity_status=None,
        source_readiness=None,
    )
    with pytest.raises(Exception) as diagnostic_error:
        Phase1GService._assert_executable_phase1e(diagnostic)
    assert diagnostic_error.value.reason_code == REASON_TARGET_DIAGNOSTIC

    deferred = SimpleNamespace(
        plan_unit_kind=Phase1EPlanUnitKind.ADMISSION_SCOPE,
        planned_operations=(
            SimpleNamespace(
                operation_type=Phase1EPlannedOperationType.SOURCE_RESOLUTION,
                operation_disposition=Phase1EOperationDisposition.COMPLETE_REQUEST,
            ),
            SimpleNamespace(
                operation_type=Phase1EPlannedOperationType.OBSERVATION_CAPTURE,
                operation_disposition=Phase1EOperationDisposition.DEFERRED,
            ),
        ),
        resource_values_frozen=True,
        reason_codes=("ADVISORY_PHASE1E_CAPACITY_INSUFFICIENT",),
        capacity_status="INSUFFICIENT",
        source_readiness="RESEARCH_READY",
    )
    with pytest.raises(Exception) as deferred_error:
        Phase1GService._assert_executable_phase1e(deferred)
    assert deferred_error.value.reason_code == REASON_OPERATION_DEFERRED
    assert deferred_error.value.context["phase1e_reason_codes"] == [
        "ADVISORY_PHASE1E_CAPACITY_INSUFFICIENT"
    ]


def test_capture_timeout_pair_consumes_remaining_target_budget() -> None:
    service = _service_shell()
    service._capture_deadline = ContextVar("test_phase1g_deadline", default=None)
    service._capture_deadline.set(10.0)
    service._monotonic = lambda: 9.875

    assert service._timeout_pair() == (125, 125)

    service._monotonic = lambda: 10.0
    with pytest.raises(Exception) as timeout_error:
        service._timeout_pair()
    assert timeout_error.value.reason_code == REASON_CAPTURE_TIMEOUT


def test_multi_target_capture_continues_after_an_unhandled_target_failure(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    service = _service_shell()
    first = SimpleNamespace(
        target_request=SimpleNamespace(request_hash="1" * 64),
        target_plan_hash="a" * 64,
    )
    second = SimpleNamespace(
        target_request=SimpleNamespace(request_hash="2" * 64),
        target_plan_hash="b" * 64,
    )
    calls = []

    def capture(target_plan):  # type: ignore[no-untyped-def]
        calls.append(target_plan.target_plan_hash)
        if target_plan is first:
            raise RuntimeError("injected target-local failure")
        return Phase1GTargetInvocationOutcome(
            target_request_hash=target_plan.target_request.request_hash,
            target_plan_hash=target_plan.target_plan_hash,
            operation_status=Phase1GOperationStatus.FAILED,
            reason_codes=(REASON_CAPTURE_TIMEOUT,),
            dml_executed=False,
        )

    monkeypatch.setattr(service, "_capture_target", capture)

    outcomes = service._capture_targets((first, second))

    assert calls == ["a" * 64, "b" * 64]
    assert outcomes[0].reason_codes == (REASON_UNEXPECTED_ERROR,)
    assert outcomes[1].reason_codes == (REASON_CAPTURE_TIMEOUT,)


@pytest.mark.parametrize("pgcode", ("57014", "55P03"))
def test_service_maps_readonly_postgres_timeout_to_capture_timeout(pgcode: str) -> None:
    error = RuntimeError("database timeout")
    error.pgcode = pgcode  # type: ignore[attr-defined]

    reason, context = Phase1GService._reason_context(error)

    assert reason == REASON_CAPTURE_TIMEOUT
    assert context == {"cause_reason_code": REASON_CAPTURE_TIMEOUT}


def test_g3_unexpected_reason_is_infrastructure_failure() -> None:
    outcome = Phase1GTargetInvocationOutcome(
        target_request_hash="1" * 64,
        target_plan_hash="2" * 64,
        operation_status=Phase1GOperationStatus.FAILED,
        reason_codes=("ADVISORY_PHASE1G_G3_UNEXPECTED_ERROR",),
        dml_executed=False,
    )

    assert (
        Phase1GService._exit_class((outcome,), SimpleNamespace(), True)
        is Phase1GExitClass.INFRASTRUCTURE_FAILURE
    )
