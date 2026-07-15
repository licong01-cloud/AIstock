from __future__ import annotations

from types import SimpleNamespace

import psycopg2
import pytest

from backend.services.advisory_phase1.capture_foundation import CapturePlan
from backend.services.advisory_phase1.phase1g_contract import (
    REASON_ATTEMPT_RECEIPT_STORE_FAILED,
    REASON_BATCH_IN_PROGRESS,
    REASON_BATCH_RECEIPT_STORE_FAILED,
    REASON_RESULT_STORE_FAILED,
    REASON_UNEXPECTED_ERROR,
    Phase1GExecutionBatchRequest,
    Phase1GAttemptReceipt,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    build_phase1g_target_projection_snapshot,
)
from backend.services.advisory_phase1.phase1g_result_store import (
    Phase1GResultStore,
    Phase1GResultStoreError,
)
from backend.services.advisory_phase1.phase1g_schema_guard import (
    Phase1GSchemaGuardEvidence,
)
from backend.services.advisory_phase1.phase1g_service import (
    Phase1GExitClass,
    Phase1GInvocationBatchStatus,
    Phase1GOperationStatus,
    Phase1GService,
    Phase1GServiceError,
    _LoadedTarget,
)
from backend.services.advisory_phase1.phase1g_source_replay import Phase1GSourceSetRef
from backend.tests.advisory_phase1.test_phase1g_g3_transactional_writer_postgres import (
    _build_target,
    _raw_factory,
)
from backend.tests.advisory_phase1.test_phase1g_historical_trace_projection import (
    historical_raw_empty_case,
    historical_multi_alpha_case,
)
from backend.tests.advisory_phase1.test_release_schema_dev_db import _fresh_apply
from scripts.advisory_phase1g_capture_observations import _verify_target_attempt_db


pytest_plugins = ("backend.tests.advisory_phase1.test_release_schema_dev_db",)


def _remove_g3_bootstrap_state(config) -> None:  # type: ignore[no-untyped-def]
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cur:
            cur.execute(
                "TRUNCATE app.advisory_capture_plan, "
                "app.advisory_capture_batch, "
                "app.advisory_phase1_control_binding_event CASCADE"
            )
        connection.commit()
    finally:
        connection.close()


def _loaded_case(
    service: Phase1GService,
    config,
    receipt,
    *,
    case_factory=historical_raw_empty_case,
    suffix: str = "g4-service",
) -> _LoadedTarget:  # type: ignore[no-untyped-def]
    case = case_factory()
    _repository, g3_target = _build_target(
        config, suffix=suffix, case_factory=case_factory
    )
    _remove_g3_bootstrap_state(config)
    plan_payload = g3_target.capture_plan.model_dump(
        mode="python", exclude={"plan_hash"}
    )
    plan_payload["evidence_bundle_hash"] = (
        case["plan"].evidence_binding.phase1_handoff_bundle_hash
    )
    capture_plan = CapturePlan.model_validate(plan_payload)
    source_ref = Phase1GSourceSetRef(
        source_revision_set_id=(
            g3_target.target_snapshot.source_revision_freeze_intent.source_revision_set.source_revision_set_id
        ),
        source_revision_set_hash=(
            g3_target.target_snapshot.source_revision_freeze_intent.source_revision_set.source_revision_set_hash
        ),
        capture_plan_hash=str(capture_plan.plan_hash),
    )
    operation = g3_target.target_snapshot.source_operation_projection.model_copy(
        update={
            "expected_capture_source_sets": (source_ref,),
            "source_operation_projection_hash": None,
        }
    )
    replay = g3_target.target_snapshot.source_replay_result.model_copy(
        update={
            "source_operation_projection_hash": operation.source_operation_projection_hash,
            "source_replay_result_hash": None,
        }
    )
    snapshot = build_phase1g_target_projection_snapshot(
        source_operation=operation,
        source_replay=replay,
        historical_trace=g3_target.target_snapshot.historical_trace_projection,
    )
    desired = service._desired_control(case["target"], (capture_plan,))
    slots = tuple(
        {"slot": value}
        for value in (
            "control_binding_event_hash",
            "capture_batch_id",
            "capture_fencing_token",
        )
    )
    preview_plan = SimpleNamespace(
        planned_operations=(
            SimpleNamespace(
                operation_type=next(
                    item.operation_type
                    for item in case["plan"].planned_operations
                    if item.operation_type.value == "OBSERVATION_CAPTURE"
                ),
                required_output_slots=slots,
                expected_final_request_hash=None,
            ),
        )
    )
    preview = service._preview_request(
        target=case["target"],
        capture_plans=(capture_plan,),
        desired_control=desired,
        phase1e_plan=preview_plan,
    )
    return _LoadedTarget(
        target_request=case["target"],
        receipt=receipt,
        phase1e_plan=case["plan"],
        schema_evidence=Phase1GSchemaGuardEvidence(
            release_receipt_hash=str(
                case["target"].release_schema_receipt_ref.semantic_content_hash
            ),
            catalog_fingerprint=str(receipt.post_catalog_fingerprint),
            database_identity=receipt.database_identity,
        ),
        snapshot=snapshot,
        capture_plans=(capture_plan,),
        desired_control=desired,
        preview=preview,
    )


@pytest.mark.parametrize(
    ("case_factory", "suffix"),
    (
        (historical_raw_empty_case, "single"),
        (historical_multi_alpha_case, "multi"),
    ),
)
def test_disposable_postgres_g4_first_run_and_complete_rerun_are_truthful(
    database_factory, tmp_path, case_factory, suffix
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _contract, receipt = _fresh_apply(config)
    result_root = tmp_path / "phase1g-results"
    service = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=SimpleNamespace(),
        result_store=Phase1GResultStore(root=result_root),
        schema_guard=SimpleNamespace(),
    )
    loaded = _loaded_case(
        service, config, receipt, case_factory=case_factory, suffix=suffix
    )
    service._load_target = lambda target: loaded  # type: ignore[method-assign]
    request = Phase1GExecutionBatchRequest(targets=(loaded.target_request,))

    plan = service.plan_batch(request)
    first = service.capture_batch(plan)
    second = service.capture_batch(plan)

    assert first.batch_status is Phase1GInvocationBatchStatus.SUCCESS
    assert second.batch_status is Phase1GInvocationBatchStatus.SUCCESS
    assert first.target_outcomes[0].operation_status is Phase1GOperationStatus.SUCCESS
    assert second.target_outcomes[0].operation_status is Phase1GOperationStatus.SUCCESS
    assert first.target_outcomes[0].dml_executed is True
    assert second.target_outcomes[0].dml_executed is False
    assert first.target_outcomes[0].committed_phases == (
        "BATCH_ACQUIRED",
        "BATCH_COMPLETED",
        "BATCH_CREATED",
        "CONTROL_BINDING",
        "RESULT_PUBLISHED",
        "TARGET_EVIDENCE",
    )
    assert second.target_outcomes[0].committed_phases == ()
    assert (
        first.target_outcomes[0].capture_result_hash
        == second.target_outcomes[0].capture_result_hash
    )
    assert (
        first.target_outcomes[0].attempt_receipt_hash
        != second.target_outcomes[0].attempt_receipt_hash
    )
    assert first.batch_attempt_receipt_hash is not None
    assert second.batch_attempt_receipt_hash is not None
    attempt = service._result_store.load(first.target_outcomes[0].attempt_receipt_ref)
    assert isinstance(attempt, Phase1GAttemptReceipt)
    _verify_target_attempt_db(
        attempt, service._result_store, config.connect_kwargs()
    )


def test_disposable_postgres_g4_unexpired_running_is_not_preempted(
    database_factory, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _contract, receipt = _fresh_apply(config)
    service = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=SimpleNamespace(),
        result_store=Phase1GResultStore(root=tmp_path / "running-results"),
        schema_guard=SimpleNamespace(),
    )
    loaded = _loaded_case(service, config, receipt, suffix="running")
    service._load_target = lambda target: loaded  # type: ignore[method-assign]
    request = Phase1GExecutionBatchRequest(targets=(loaded.target_request,))
    plan = service.plan_batch(request)
    event, _changed = service._select_control_event(
        desired=loaded.desired_control, chain=(), current_head=None
    )
    capture_request = service._materialize_request(
        loaded=loaded, event=event, attempt_no=1
    )
    planned = service._capture_repository.create(capture_request)
    service._capture_repository.acquire(
        capture_batch_id=capture_request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=service._registry.lease_seconds,
    )

    result = service.capture_batch(plan)
    outcome = result.target_outcomes[0]

    assert outcome.operation_status is Phase1GOperationStatus.FAILED
    assert outcome.reason_codes == (REASON_BATCH_IN_PROGRESS,)
    assert outcome.dml_executed is False


def test_disposable_postgres_g4_failed_completion_recovers_and_reuses_facts(
    database_factory, tmp_path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _contract, receipt = _fresh_apply(config)
    service = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=SimpleNamespace(),
        result_store=Phase1GResultStore(root=tmp_path / "recovery-results"),
        schema_guard=SimpleNamespace(),
    )
    loaded = _loaded_case(service, config, receipt, suffix="recovery")
    service._load_target = lambda target: loaded  # type: ignore[method-assign]
    request = Phase1GExecutionBatchRequest(targets=(loaded.target_request,))
    plan = service.plan_batch(request)
    original_complete = service._capture_repository.complete

    def fail_completion(**_kwargs):  # type: ignore[no-untyped-def]
        raise Phase1GServiceError(
            REASON_UNEXPECTED_ERROR, "injected completion response failure"
        )

    monkeypatch.setattr(service._capture_repository, "complete", fail_completion)
    failed = service.capture_batch(plan)
    monkeypatch.setattr(service._capture_repository, "complete", original_complete)
    recovered = service.capture_batch(plan)

    assert failed.target_outcomes[0].operation_status is Phase1GOperationStatus.FAILED
    assert recovered.target_outcomes[0].operation_status is Phase1GOperationStatus.SUCCESS
    assert recovered.target_outcomes[0].capture_attempt_no == 2
    assert "BATCH_FAILED" in failed.target_outcomes[0].committed_phases
    assert "BATCH_RECOVERED" in recovered.target_outcomes[0].committed_phases
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM app.advisory_selection_stage_trace_outbox")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT count(*) FROM app.advisory_signal_observation_version")
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT array_agg(capture_status ORDER BY capture_attempt_no) "
                "FROM app.advisory_capture_batch"
            )
            assert cur.fetchone()[0] == ["FAILED", "COMPLETE"]
    finally:
        connection.close()


def test_disposable_postgres_g4_store_failures_preserve_database_and_artifact_truth(
    database_factory, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    class FaultInjectingStore(Phase1GResultStore):
        result_calls = 0
        attempt_calls = 0
        batch_calls = 0

        def publish_result(self, result):  # type: ignore[no-untyped-def]
            self.result_calls += 1
            if self.result_calls == 1:
                raise Phase1GResultStoreError(
                    REASON_RESULT_STORE_FAILED, "injected result store failure"
                )
            return super().publish_result(result)

        def publish_attempt(self, receipt):  # type: ignore[no-untyped-def]
            self.attempt_calls += 1
            if self.attempt_calls == 2:
                raise Phase1GResultStoreError(
                    REASON_ATTEMPT_RECEIPT_STORE_FAILED,
                    "injected attempt store failure",
                )
            return super().publish_attempt(receipt)

        def publish_batch(self, receipt):  # type: ignore[no-untyped-def]
            self.batch_calls += 1
            if self.batch_calls == 2:
                raise Phase1GResultStoreError(
                    REASON_BATCH_RECEIPT_STORE_FAILED,
                    "injected batch store failure",
                )
            return super().publish_batch(receipt)

    config = database_factory()
    _contract, receipt = _fresh_apply(config)
    store = FaultInjectingStore(root=tmp_path / "store-failure-results")
    service = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=SimpleNamespace(),
        result_store=store,
        schema_guard=SimpleNamespace(),
    )
    loaded = _loaded_case(service, config, receipt, suffix="store-failures")
    service._load_target = lambda target: loaded  # type: ignore[method-assign]
    plan = service.plan_batch(
        Phase1GExecutionBatchRequest(targets=(loaded.target_request,))
    )

    result_failed = service.capture_batch(plan)
    attempt_failed = service.capture_batch(plan)
    batch_failed = service.capture_batch(plan)
    recovered = service.capture_batch(plan)

    assert result_failed.target_outcomes[0].reason_codes == (
        REASON_RESULT_STORE_FAILED,
    )
    assert result_failed.target_outcomes[0].capture_batch_status == "COMPLETE"
    assert attempt_failed.target_outcomes[0].reason_codes == (
        REASON_ATTEMPT_RECEIPT_STORE_FAILED,
    )
    assert attempt_failed.target_outcomes[0].capture_result_hash is not None
    assert attempt_failed.batch_attempt_receipt_ref is None
    assert batch_failed.target_outcomes[0].operation_status is Phase1GOperationStatus.SUCCESS
    assert batch_failed.reason_codes == (REASON_BATCH_RECEIPT_STORE_FAILED,)
    assert batch_failed.batch_attempt_receipt_ref is None
    assert batch_failed.exit_class is Phase1GExitClass.INFRASTRUCTURE_FAILURE
    assert recovered.target_outcomes[0].operation_status is Phase1GOperationStatus.SUCCESS
    assert recovered.target_outcomes[0].dml_executed is False
    assert (
        attempt_failed.target_outcomes[0].capture_result_hash
        == batch_failed.target_outcomes[0].capture_result_hash
        == recovered.target_outcomes[0].capture_result_hash
    )


def test_disposable_postgres_g4_multi_target_failure_is_isolated(
    database_factory, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    _contract, receipt = _fresh_apply(config)
    service = Phase1GService(
        connection_config=config,
        transaction_connection_factory=_raw_factory(config),
        readonly_connection_factory=_raw_factory(config),
        artifact_resolver=SimpleNamespace(),
        result_store=Phase1GResultStore(root=tmp_path / "multi-target-results"),
        schema_guard=SimpleNamespace(),
    )
    first = _loaded_case(
        service,
        config,
        receipt,
        case_factory=historical_raw_empty_case,
        suffix="multi-target-single",
    )
    second = _loaded_case(
        service,
        config,
        receipt,
        case_factory=historical_multi_alpha_case,
        suffix="multi-target-parent",
    )
    loaded_by_hash = {
        str(first.target_request.request_hash): first,
        str(second.target_request.request_hash): second,
    }
    service._load_target = lambda target: loaded_by_hash[str(target.request_hash)]  # type: ignore[method-assign]
    plan = service.plan_batch(
        Phase1GExecutionBatchRequest(
            targets=(first.target_request, second.target_request)
        )
    )
    failed_hash = str(first.target_request.request_hash)

    def capture_load(target):  # type: ignore[no-untyped-def]
        if str(target.request_hash) == failed_hash:
            raise Phase1GServiceError(
                REASON_UNEXPECTED_ERROR, "injected target-local preflight failure"
            )
        return loaded_by_hash[str(target.request_hash)]

    service._load_target = capture_load  # type: ignore[method-assign]

    outcome = service.capture_batch(plan)

    assert outcome.batch_status is Phase1GInvocationBatchStatus.PARTIAL_FAILURE
    by_hash = {item.target_request_hash: item for item in outcome.target_outcomes}
    assert by_hash[failed_hash].operation_status is Phase1GOperationStatus.FAILED
    assert by_hash[failed_hash].dml_executed is False
    successful_hash = str(second.target_request.request_hash)
    assert by_hash[successful_hash].operation_status is Phase1GOperationStatus.SUCCESS
    assert by_hash[successful_hash].dml_executed is True
    assert outcome.batch_attempt_receipt_hash is not None
