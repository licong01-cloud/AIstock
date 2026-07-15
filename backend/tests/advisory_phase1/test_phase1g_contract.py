from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    PHASE1G_RESULT_STORE_LAYOUT_POLICY,
    Phase1GAttemptReceipt,
    Phase1GAttemptStatus,
    Phase1GBatchAttemptReceipt,
    Phase1GBatchStatus,
    Phase1GCapturePolicyRegistry,
    Phase1GComponentContract,
    Phase1GContractError,
    Phase1GExecutionBatchRequest,
    Phase1GIdentityHashRef,
    Phase1GOutputArtifactKind,
    Phase1GOutputArtifactRef,
    Phase1GStoreLayoutPolicy,
    Phase1GTargetAttemptRef,
    Phase1GTargetExecutionPlan,
    Phase1GTargetExecutionRequest,
    build_phase1g_execution_batch_plan,
    resolve_capture_policy_registry,
)
from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.tests.advisory_phase1.phase1g_test_support import (
    capture_result,
    database_identity,
    h,
    target_request,
)


def _validated_copy(model, **updates):  # type: ignore[no-untyped-def]
    payload = model.model_dump(mode="json")
    payload.update(updates)
    for identity_field in ("request_hash", "batch_request_hash", "target_plan_hash"):
        if identity_field not in updates:
            payload.pop(identity_field, None)
    return type(model).model_validate(payload)


def _target_plan(
    *,
    observed_at: datetime | None = None,
    expected_rows: int = 2,
    request: Phase1GTargetExecutionRequest | None = None,
) -> Phase1GTargetExecutionPlan:
    request = request or target_request()
    return Phase1GTargetExecutionPlan(
        target_request=request,
        release_receipt_hash=request.release_schema_receipt_ref.semantic_content_hash,
        release_catalog_fingerprint=h("8"),
        database_identity=database_identity(target_label=TargetLabel.DEV),
        phase1e_plan_id=request.phase1e_plan_id,
        phase1e_plan_hash=request.phase1e_plan_hash,
        source_resolution_expected_hash=request.source_operation_hash,
        expected_source_events=(
            Phase1GIdentityHashRef(identity="event-b", content_hash=h("b")),
            Phase1GIdentityHashRef(identity="event-a", content_hash=h("a")),
        ),
        expected_dse=Phase1GIdentityHashRef(identity="dse-a", content_hash=h("c")),
        expected_selection_artifact=Phase1GIdentityHashRef(identity="artifact-a", content_hash=h("d")),
        expected_package=Phase1GIdentityHashRef(identity="package-a", content_hash=h("e")),
        expected_capture_plan_set_hash=h("f"),
        expected_capture_plan_set_count=2,
        expected_rows=expected_rows,
        expected_bytes=4096,
        capture_policy_registry_hash=str(DEFAULT_CAPTURE_POLICY_REGISTRY.registry_hash),
        observed_current_binding_head_hash=h("0"),
        observed_capture_batch_state_hash=h("1"),
        observed_outbox_identity_hashes=(h("3"), h("2")),
        observed_at=observed_at or datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
    )


def test_registered_capture_policy_is_exact_and_has_no_fallback() -> None:
    policy = resolve_capture_policy_registry(
        registry_id="ADVISORY_PHASE1G_HISTORICAL_OBSERVATION_CAPTURE",
        registry_version="1",
    )
    assert policy is DEFAULT_CAPTURE_POLICY_REGISTRY
    assert policy.absolute_max_candidates == 1_000_000
    assert policy.absolute_max_bytes == 2_147_483_648
    assert policy.absolute_max_capture_ms == 1_800_000
    assert policy.registry_hash == "fe3548010d6343781e69f4b8aee7e49c477d1f7f29f853fd5f3fbe85e6416bf4"

    with pytest.raises(Phase1GContractError, match="not registered"):
        resolve_capture_policy_registry(registry_id=policy.registry_id, registry_version="2")

    with pytest.raises(Phase1GContractError, match="planned bytes"):
        policy.assert_within_bounds(planned_candidates=1, planned_bytes=policy.absolute_max_bytes + 1)
    with pytest.raises(Phase1GContractError, match="planned candidates"):
        policy.assert_within_bounds(planned_candidates=-1, planned_bytes=0)
    with pytest.raises(Phase1GContractError, match="planned bytes"):
        policy.assert_within_bounds(planned_candidates=0, planned_bytes=-1)


def test_policy_contract_hashes_and_timeout_order_cannot_be_overridden() -> None:
    with pytest.raises(ValidationError, match="contract_hash"):
        Phase1GComponentContract(
            component_name="source_resolver",
            contract_version="v1",
            contract_hash=h("0"),
        )
    with pytest.raises(ValidationError, match="layout_policy_hash"):
        Phase1GStoreLayoutPolicy(
            policy_id="TEST_STORE",
            policy_version="1",
            artifact_kinds=("CAPTURE_RESULT",),
            layout_version="v1",
            identity_fields=("capture_result_hash",),
            layout_policy_hash=h("0"),
        )

    payload = DEFAULT_CAPTURE_POLICY_REGISTRY.model_dump(mode="json", exclude={"registry_hash"})
    payload["lock_timeout_ms"] = payload["statement_timeout_ms"] + 1
    with pytest.raises(ValidationError, match="lock timeout"):
        Phase1GCapturePolicyRegistry.model_validate(payload)

    payload = DEFAULT_CAPTURE_POLICY_REGISTRY.model_dump(mode="json", exclude={"registry_hash"})
    payload["statement_timeout_ms"] = payload["absolute_max_capture_ms"] + 1
    payload["lock_timeout_ms"] = 1
    with pytest.raises(ValidationError, match="statement timeout"):
        Phase1GCapturePolicyRegistry.model_validate(payload)

    payload = DEFAULT_CAPTURE_POLICY_REGISTRY.model_dump(mode="json")
    payload["registry_hash"] = h("0")
    with pytest.raises(ValidationError, match="registry_hash"):
        Phase1GCapturePolicyRegistry.model_validate(payload)


def test_target_request_hash_excludes_time_and_physical_relative_paths() -> None:
    first = target_request()
    second = target_request(
        requested_at=datetime(2026, 7, 16, 3, 0, tzinfo=UTC),
        receipt_relative_path="moved/receipt.json",
        plan_relative_path="moved/plan.json",
    )

    assert first.request_hash == second.request_hash
    assert first.requested_at != second.requested_at
    assert first.release_schema_receipt_ref.relative_path != second.release_schema_receipt_ref.relative_path


def test_target_request_rejects_unregistered_result_or_capture_policy() -> None:
    request = target_request()
    payload = request.model_dump(mode="json")
    payload.pop("request_hash")
    payload["result_store_policy_hash"] = h("9")
    with pytest.raises(ValidationError, match="result store policy"):
        Phase1GTargetExecutionRequest.model_validate(payload)

    payload = request.model_dump(mode="json")
    payload.pop("request_hash")
    payload["capture_policy_registry_version"] = "2"
    with pytest.raises(Phase1GContractError, match="not registered"):
        Phase1GTargetExecutionRequest.model_validate(payload)

    payload = request.model_dump(mode="json")
    payload.pop("request_hash")
    payload["release_schema_receipt_ref"]["store_policy_hash"] = h("0")
    with pytest.raises(ValidationError, match="release_schema_receipt_ref"):
        Phase1GTargetExecutionRequest.model_validate(payload)


def test_batch_request_sorts_targets_and_keeps_each_program_independent() -> None:
    first = target_request()
    second = _validated_copy(first, program_id="program-b")
    forward = Phase1GExecutionBatchRequest(targets=(first, second))
    reverse = Phase1GExecutionBatchRequest(targets=(second, first))
    later_first = target_request(requested_at=first.requested_at + timedelta(days=1))
    later_second = _validated_copy(later_first, program_id="program-b")
    later = Phase1GExecutionBatchRequest(targets=(later_second, later_first))

    assert forward.batch_request_hash == reverse.batch_request_hash == later.batch_request_hash
    assert [item.request_hash for item in forward.targets] == sorted((first.request_hash, second.request_hash))
    assert forward.continue_on_target_failure is True
    assert forward.execution_prohibited is True

    with pytest.raises(ValidationError, match="unique request hashes"):
        Phase1GExecutionBatchRequest(targets=(first, first))

    first_plan = _target_plan(request=first)
    second_plan = _target_plan(request=second)
    batch_plan = build_phase1g_execution_batch_plan(
        batch_request=forward,
        target_plans=(second_plan, first_plan),
    )
    assert batch_plan.target_count == 2
    assert batch_plan.batch_request_hash == forward.batch_request_hash
    with pytest.raises(Phase1GContractError, match="does not close"):
        build_phase1g_execution_batch_plan(batch_request=forward, target_plans=(first_plan,))


def test_target_plan_hash_includes_observed_fact_time_and_enforces_registered_bounds() -> None:
    first = _target_plan()
    second = _target_plan(observed_at=first.observed_at + timedelta(seconds=1))

    assert first.target_request.request_hash == second.target_request.request_hash
    assert first.target_plan_hash != second.target_plan_hash
    assert [item.identity for item in first.expected_source_events] == ["event-a", "event-b"]
    assert first.observed_outbox_identity_hashes == (h("2"), h("3"))

    with pytest.raises(Phase1GContractError, match="planned candidates"):
        _target_plan(expected_rows=DEFAULT_CAPTURE_POLICY_REGISTRY.absolute_max_candidates + 1)

    payload = first.model_dump(mode="json", exclude={"target_plan_hash"})
    payload["expected_source_events"] = [
        {"identity": "event-a", "content_hash": h("a")},
        {"identity": "event-a", "content_hash": h("b")},
    ]
    with pytest.raises(ValidationError, match="identities must be unique"):
        Phase1GTargetExecutionPlan.model_validate(payload)


def test_stable_result_attempt_and_batch_receipts_preserve_distinct_identities() -> None:
    result = capture_result()
    assert [item.capture_plan_hash for item in result.selected_observation_mappings] == [h("1"), h("2")]
    result_ref = Phase1GOutputArtifactRef(
        artifact_kind=Phase1GOutputArtifactKind.CAPTURE_RESULT,
        store_policy_hash=str(PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash),
        relative_path=f"results/{str(result.capture_result_hash)[:2]}/{result.capture_result_hash}.json",
        semantic_content_hash=str(result.capture_result_hash),
        file_sha256=h("f"),
    )
    first = Phase1GAttemptReceipt(
        target_plan_hash=h("1"),
        target_request_hash=result.target_request_hash,
        attempt_invocation_id="invocation-a",
        started_at=datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
        finished_at=datetime(2026, 7, 15, 2, 1, tzinfo=UTC),
        operation_status=Phase1GAttemptStatus.SUCCESS,
        dml_executed=True,
        committed_phases=("TARGET_EVIDENCE", "BATCH_CREATED"),
        capture_batch_id=result.capture_batch_id,
        capture_attempt_no=result.capture_attempt_no,
        capture_batch_status=result.capture_status,
        capture_result_ref=result_ref,
        capture_result_hash=result.capture_result_hash,
    )
    second = Phase1GAttemptReceipt(
        **{
            **first.model_dump(mode="python", exclude={"attempt_receipt_hash"}),
            "attempt_invocation_id": "invocation-b",
            "dml_executed": False,
        }
    )

    assert first.capture_result_hash == second.capture_result_hash == result.capture_result_hash
    assert first.attempt_receipt_hash != second.attempt_receipt_hash
    assert first.committed_phases == ("BATCH_CREATED", "TARGET_EVIDENCE")

    with pytest.raises(ValidationError, match="unregistered phase"):
        Phase1GAttemptReceipt.model_validate(
            {
                **first.model_dump(mode="python", exclude={"attempt_receipt_hash"}),
                "committed_phases": ("SIMPLIFIED_WRITE",),
            }
        )

    batch = Phase1GBatchAttemptReceipt(
        batch_request_hash=h("2"),
        batch_plan_hash=h("3"),
        target_count=2,
        succeeded_count=1,
        failed_count=1,
        target_attempt_refs=(
            Phase1GTargetAttemptRef(
                target_request_hash=result.target_request_hash,
                target_plan_hash=first.target_plan_hash,
                attempt_receipt_hash=str(first.attempt_receipt_hash),
                operation_status=Phase1GAttemptStatus.SUCCESS,
                capture_result_hash=str(result.capture_result_hash),
            ),
            Phase1GTargetAttemptRef(
                target_request_hash=h("9"),
                target_plan_hash=h("8"),
                attempt_receipt_hash=h("4"),
                operation_status=Phase1GAttemptStatus.FAILED,
            ),
        ),
        batch_status=Phase1GBatchStatus.PARTIAL_FAILURE,
    )
    assert batch.batch_status is Phase1GBatchStatus.PARTIAL_FAILURE
    assert batch.target_attempt_receipt_hashes == (str(first.attempt_receipt_hash), h("4"))

    successful = Phase1GBatchAttemptReceipt(
        batch_request_hash=h("2"),
        batch_plan_hash=h("3"),
        target_count=1,
        succeeded_count=1,
        failed_count=0,
        target_attempt_refs=(
            Phase1GTargetAttemptRef(
                target_request_hash=result.target_request_hash,
                target_plan_hash=first.target_plan_hash,
                attempt_receipt_hash=str(first.attempt_receipt_hash),
                operation_status=Phase1GAttemptStatus.SUCCESS,
                capture_result_hash=str(result.capture_result_hash),
            ),
        ),
        batch_status=Phase1GBatchStatus.SUCCESS,
    )
    failed = Phase1GBatchAttemptReceipt(
        batch_request_hash=h("2"),
        batch_plan_hash=h("3"),
        target_count=1,
        succeeded_count=0,
        failed_count=1,
        target_attempt_refs=(
            Phase1GTargetAttemptRef(
                target_request_hash=h("9"),
                target_plan_hash=h("8"),
                attempt_receipt_hash=h("4"),
                operation_status=Phase1GAttemptStatus.FAILED,
            ),
        ),
        batch_status=Phase1GBatchStatus.FAILED,
    )
    assert successful.batch_status is Phase1GBatchStatus.SUCCESS
    assert failed.batch_status is Phase1GBatchStatus.FAILED


def test_stable_result_rejects_trace_mapping_or_count_drift() -> None:
    result = capture_result()
    payload = result.model_dump(mode="json", exclude={"capture_result_hash"})
    payload["selected_observation_mappings"][0]["trace_content_hash"] = h("0")
    with pytest.raises(ValidationError, match="do not match trace outbox"):
        type(result).model_validate(payload)

    payload = result.model_dump(mode="json", exclude={"capture_result_hash"})
    payload["capture_plan_set_count"] = 3
    with pytest.raises(ValidationError, match="counts do not close"):
        type(result).model_validate(payload)

    payload = result.model_dump(mode="json", exclude={"capture_result_hash"})
    payload["selected_observation_mappings"][0]["source_revision_set_id"] = "other-source-set"
    payload["selected_observation_mappings"][0]["source_revision_set_hash"] = h("f")
    with pytest.raises(ValidationError, match="source revision set"):
        type(result).model_validate(payload)


def test_target_plan_rejects_request_and_database_identity_drift() -> None:
    plan = _target_plan()
    payload = plan.model_dump(mode="json", exclude={"target_plan_hash"})
    payload["release_receipt_hash"] = h("9")
    with pytest.raises(ValidationError, match="release receipt hash"):
        Phase1GTargetExecutionPlan.model_validate(payload)

    payload = plan.model_dump(mode="json", exclude={"target_plan_hash"})
    payload["database_identity"]["target_label"] = TargetLabel.PRODUCTION.value
    with pytest.raises(ValidationError, match="database identity"):
        Phase1GTargetExecutionPlan.model_validate(payload)


def test_failed_attempt_cannot_silently_publish_a_result_or_omit_reason() -> None:
    in_progress = Phase1GAttemptReceipt(
        target_plan_hash=h("1"),
        target_request_hash=h("2"),
        attempt_invocation_id="running-a",
        started_at=datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
        operation_status=Phase1GAttemptStatus.IN_PROGRESS,
        dml_executed=False,
    )
    assert in_progress.finished_at is None
    assert in_progress.capture_result_hash is None

    base = {
        "target_plan_hash": h("1"),
        "target_request_hash": h("2"),
        "attempt_invocation_id": "failed-a",
        "started_at": datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 7, 15, 2, 1, tzinfo=UTC),
        "operation_status": Phase1GAttemptStatus.FAILED,
        "dml_executed": False,
    }
    with pytest.raises(ValidationError, match="reason code"):
        Phase1GAttemptReceipt(**base)

    with pytest.raises(ValidationError, match="cannot expose a stable result"):
        Phase1GAttemptReceipt(
            **base,
            reason_codes=("ADVISORY_PHASE1G_PLAN_STALE",),
            capture_result_hash=h("3"),
        )


def test_attempt_receipt_rejects_sensitive_error_context_and_unregistered_result_policy() -> None:
    failed = {
        "target_plan_hash": h("1"),
        "target_request_hash": h("2"),
        "attempt_invocation_id": "failed-sensitive",
        "started_at": datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 7, 15, 2, 1, tzinfo=UTC),
        "operation_status": Phase1GAttemptStatus.FAILED,
        "reason_codes": ("ADVISORY_PHASE1G_UNEXPECTED_ERROR",),
        "dml_executed": False,
    }
    with pytest.raises(ValidationError, match="prohibited sensitive field"):
        Phase1GAttemptReceipt(**failed, error_context={"password": "must-not-persist"})
    with pytest.raises(ValidationError, match="connection credentials"):
        Phase1GAttemptReceipt(**failed, error_context={"detail": "postgres://user:pass@host/db"})
    with pytest.raises(ValidationError, match="prohibited sensitive field"):
        Phase1GAttemptReceipt(**failed, error_context={"token": "must-not-persist"})

    safe = Phase1GAttemptReceipt(
        **failed,
        error_context={
            "operation": "catalog_verify",
            "error_type": "RuntimeError",
            "fencing_token": 3,
            "sqlstate": "08006",
        },
    )
    assert safe.error_context == {
        "error_type": "RuntimeError",
        "fencing_token": 3,
        "operation": "catalog_verify",
        "sqlstate": "08006",
    }

    result = capture_result()
    result_ref = Phase1GOutputArtifactRef(
        artifact_kind=Phase1GOutputArtifactKind.CAPTURE_RESULT,
        store_policy_hash=h("0"),
        relative_path=f"results/{str(result.capture_result_hash)[:2]}/{result.capture_result_hash}.json",
        semantic_content_hash=str(result.capture_result_hash),
        file_sha256=h("f"),
    )
    with pytest.raises(ValidationError, match="unregistered store policy"):
        Phase1GAttemptReceipt(
            target_plan_hash=h("1"),
            target_request_hash=result.target_request_hash,
            attempt_invocation_id="invalid-policy",
            started_at=datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
            finished_at=datetime(2026, 7, 15, 2, 1, tzinfo=UTC),
            operation_status=Phase1GAttemptStatus.SUCCESS,
            dml_executed=True,
            capture_result_ref=result_ref,
            capture_result_hash=result.capture_result_hash,
        )


def test_batch_receipt_preserves_attempt_mapping_in_target_request_order() -> None:
    refs = (
        Phase1GTargetAttemptRef(
            target_request_hash=h("9"),
            target_plan_hash=h("8"),
            attempt_receipt_hash=h("1"),
            operation_status=Phase1GAttemptStatus.FAILED,
        ),
        Phase1GTargetAttemptRef(
            target_request_hash=h("1"),
            target_plan_hash=h("2"),
            attempt_receipt_hash=h("f"),
            operation_status=Phase1GAttemptStatus.SUCCESS,
            capture_result_hash=h("7"),
        ),
    )
    receipt = Phase1GBatchAttemptReceipt(
        batch_request_hash=h("3"),
        batch_plan_hash=h("4"),
        target_count=2,
        succeeded_count=1,
        failed_count=1,
        target_attempt_refs=refs,
        batch_status=Phase1GBatchStatus.PARTIAL_FAILURE,
    )

    assert tuple(item.target_request_hash for item in receipt.target_attempt_refs) == (h("1"), h("9"))
    assert receipt.target_attempt_receipt_hashes == (h("f"), h("1"))
    assert receipt.successful_capture_result_hashes == (h("7"),)

    payload = receipt.model_dump(mode="json", exclude={"batch_attempt_receipt_hash"})
    payload["target_attempt_receipt_hashes"] = [h("1"), h("f")]
    with pytest.raises(ValidationError, match="preserve target request order"):
        Phase1GBatchAttemptReceipt.model_validate(payload)
