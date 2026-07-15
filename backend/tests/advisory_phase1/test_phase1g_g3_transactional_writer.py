from __future__ import annotations

from copy import deepcopy
from datetime import date
import logging
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.capture_foundation import CapturePlan
from backend.services.advisory_phase1.observation_capture import (
    build_observation_semantic_draft,
    expected_evidence_bundle_hash,
    materialize_observation_row_bundle,
)
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GTargetCommitProjection,
    Phase1GTransactionalWriteRequest,
    REASON_CAPTURE_TIMEOUT,
    REASON_G3_CAPACITY_EXCEEDED,
    REASON_G3_INPUT_INVALID,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    parse_phase1g_source_operation,
    replay_phase1g_source_operation,
)
from backend.services.advisory_phase1.phase1g_transactional_writer import (
    Phase1GTransactionalTargetInput,
    Phase1GTransactionalWriter,
    Phase1GTransactionalWriterError,
)
from backend.services.advisory_phase1.stage_trace import TraceCaptureContext
from backend.tests.advisory_phase1.test_capture_foundation import (
    _binding,
    _complete_stage_trace,
    _envelope,
    _plan,
)
from backend.tests.advisory_phase1.test_phase1g_source_replay import g2_source_case


def _pure_case():  # type: ignore[no-untyped-def]
    binding = _binding()
    envelope = _envelope(binding, stage_trace=_complete_stage_trace())
    provisional = _plan(alpha_mode="single_alpha")
    plan_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=envelope.trace_content_hash,
    )
    plan = CapturePlan.model_validate(plan_payload)
    context = TraceCaptureContext(
        selection_run_id=plan.selection_run_id,
        package_id=plan.package_id,
        manifest_sha256=plan.manifest_sha256,
        decision_as_of_trade_date=plan.decision_as_of_trade_date,
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        binding=binding,
    )
    draft = build_observation_semantic_draft(
        plan=plan, envelope=envelope, binding=binding
    )
    return plan, binding, context, envelope, draft


def test_semantic_draft_is_deeply_immutable_and_materialization_is_revision_scoped() -> (
    None
):
    _plan_value, _binding_value, _context, _envelope_value, draft = _pure_case()

    with pytest.raises(TypeError):
        draft.canonical_signal_header["package_id"] = "tampered"
    with pytest.raises(TypeError):
        draft.stage_semantic_rows[0]["reason_codes"].append("tampered")

    first = materialize_observation_row_bundle(
        draft=draft,
        observation_revision_no=1,
        supersedes_observation_version_id=None,
        created_by_capture_batch_id="capture-batch-1",
    )
    second = materialize_observation_row_bundle(
        draft=draft,
        observation_revision_no=2,
        supersedes_observation_version_id=first.observation_version[
            "observation_version_id"
        ],
        created_by_capture_batch_id="capture-batch-2",
    )

    assert (
        first.semantic_observation_key
        == second.semantic_observation_key
        == draft.semantic_observation_key
    )
    assert (
        first.observation_version["observation_version_id"]
        != second.observation_version["observation_version_id"]
    )
    assert first.bundle_row_count == 9 + 2 * len(draft.candidate_semantic_rows)
    assert len(first.stage_evidence_rows) == 5
    assert len(first.candidate_identity_rows) == len(first.candidate_payload_rows)


def test_transactional_request_and_projection_revalidate_hashes() -> None:
    request = Phase1GTransactionalWriteRequest(
        target_request_hash="1" * 64,
        phase1e_plan_id="plan-a",
        phase1e_plan_hash="2" * 64,
        g2_target_projection_snapshot_hash="3" * 64,
        capture_batch_id="batch-a",
        capture_request_hash="4" * 64,
        capture_attempt_no=1,
        expected_batch_row_version=2,
        capture_fencing_token=1,
        control_binding_event_hash="5" * 64,
        capture_plan_hash="6" * 64,
        trace_capture_context_hash="7" * 64,
        trace_capture_binding_hash="8" * 64,
        trace_outbox_id="outbox-a",
        stage_trace_envelope_hash="9" * 64,
        observation_semantic_key="a" * 64,
        observation_semantic_draft_hash="b" * 64,
        expected_rows=9,
        expected_bytes=1,
    )
    with pytest.raises(ValueError, match="write_request_hash"):
        Phase1GTransactionalWriteRequest.model_validate(
            {**request.model_dump(mode="python"), "expected_rows": 10}
        )

    projection_payload = {
        "target_request_hash": "1" * 64,
        "target_plan_hash": "2" * 64,
        "capture_batch_id": "batch-a",
        "capture_request_hash": "3" * 64,
        "capture_attempt_no": 1,
        "capture_fencing_token": 1,
        "source_revision_set_id": "source-a",
        "source_revision_set_hash": "4" * 64,
        "source_revision_member_count": 1,
        "source_revision_member_hash": "5" * 64,
        "control_binding_event_hash": "6" * 64,
        "trace_outbox_id": "outbox-a",
        "trace_content_hash": "7" * 64,
        "canonical_signal_id": "signal-a",
        "observation_version_id": "version-a",
        "observation_content_hash": "8" * 64,
        "observation_revision_no": 1,
        "lineage_id": "lineage-a",
        "lineage_content_hash": "9" * 64,
        "stage_evidence_refs": tuple(
            {
                "stage": stage,
                "stage_evidence_id": f"stage-{index}",
                "content_hash": f"{index}" * 64,
            }
            for index, stage in enumerate(
                (
                    "alpha_raw",
                    "hmm_adjusted",
                    "risk_policy_adjusted",
                    "selection_effective",
                    "advisory_model",
                ),
                start=1,
            )
        ),
        "candidate_count": 0,
        "candidate_set_hash": canonical_json_sha256(()),
        "target_membership_hash": "a" * 64,
        "delivery_event_id": "delivery-a",
        "delivery_event_hash": "b" * 64,
        "post_commit_readback_hash": "c" * 64,
    }
    projection = Phase1GTargetCommitProjection.model_validate(projection_payload)
    tampered = deepcopy(projection.model_dump(mode="python"))
    tampered["candidate_count"] = 1
    with pytest.raises(ValueError, match="target_commit_projection_hash"):
        Phase1GTargetCommitProjection.model_validate(tampered)


@pytest.mark.parametrize("pgcode", ("57014", "55P03"))
def test_writer_maps_postgres_statement_and_lock_timeout_to_capture_timeout(
    pgcode: str,
) -> None:
    error = RuntimeError("database timeout")
    error.pgcode = pgcode  # type: ignore[attr-defined]

    mapped = Phase1GTransactionalWriter._map_error(error)

    assert mapped.reason_code == REASON_CAPTURE_TIMEOUT
    assert mapped.context["exception_type"] == "RuntimeError"


def test_writer_input_closure_accepts_only_complete_typed_identity_graph() -> None:
    plan, binding, context, envelope, draft = _pure_case()
    source_plan, source_target, event = g2_source_case()
    operation = parse_phase1g_source_operation(
        phase1e_plan=source_plan, target_request=source_target
    )
    replay = replay_phase1g_source_operation(
        projection=operation, availability_events=(event,)
    )
    plan_payload = plan.model_dump(mode="python", exclude={"plan_hash"})
    lineage = {"selection_run_id": plan.selection_run_id}
    plan_payload.update(
        signal_source_revision_set_id=replay.source_revision_set.source_revision_set_id,
        signal_source_revision_set_hash=replay.source_revision_set.source_revision_set_hash,
        selection_run_content_hash=canonical_json_sha256(lineage),
        evidence_bundle_hash="0" * 64,
    )
    provisional_plan = CapturePlan.model_validate(plan_payload)
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional_plan, trace_content_hash=envelope.trace_content_hash
    )
    plan = CapturePlan.model_validate(plan_payload)
    draft = build_observation_semantic_draft(
        plan=plan, envelope=envelope, binding=binding
    )
    operation = SimpleNamespace(
        phase1e_plan_id="phase1e-plan",
        phase1e_plan_hash="f" * 64,
        expected_capture_source_sets=(
            SimpleNamespace(
                capture_plan_hash=plan.plan_hash,
                source_revision_set_id=plan.signal_source_revision_set_id,
                source_revision_set_hash=plan.signal_source_revision_set_hash,
            ),
        ),
        package_id=plan.package_id,
        manifest_sha256=plan.manifest_sha256,
        alpha_mode=plan.alpha_mode,
        decision_trade_date=date.fromisoformat(plan.decision_as_of_trade_date),
        program_id=plan.program_id,
        admission_scope_id=plan.admission_scope_id,
        admission_scope_hash=plan.admission_scope_hash,
    )
    historical = SimpleNamespace(
        dse=SimpleNamespace(
            package_id=plan.package_id,
            manifest_sha256=plan.manifest_sha256,
            evidence_id=plan.selection_evidence_id,
            artifact_hash=plan.selection_evidence_hash,
            runtime_profile_version_id=plan.runtime_profile_version_id,
            runtime_profile_hash=plan.runtime_profile_version_hash,
            evidence=SimpleNamespace(
                phase0a_candidate_lineage=lineage,
                decision_clock=SimpleNamespace(
                    decision_as_of_trade_date=date.fromisoformat(
                        plan.decision_as_of_trade_date
                    ),
                    selection_as_of_trade_date=date.fromisoformat(
                        plan.selection_as_of_trade_date
                    ),
                    target_trade_date=date.fromisoformat(plan.target_trade_date),
                    calendar_version=plan.calendar_version,
                    calendar_hash=plan.calendar_hash,
                ),
                phase0a_effective_config_chain=SimpleNamespace(
                    package_effective_config_hash=plan.package_effective_config_hash
                ),
            ),
        ),
        artifact=SimpleNamespace(
            artifact_id=plan.selection_score_artifact_id,
            artifact_payload_sha256=plan.selection_score_artifact_hash,
            score_count=envelope.candidate_count,
        ),
        package_manifest=SimpleNamespace(alpha_mode=plan.alpha_mode),
        stage_trace_builder_input=SimpleNamespace(runtime_config={"runtime": "test"}),
        candidate_outcome=(
            "VALID_NO_CANDIDATE" if plan.valid_no_candidate else "CANDIDATES_PRESENT"
        ),
    )
    plan_payload = plan.model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["selection_runtime_semantics_hash"] = canonical_json_sha256(
        historical.stage_trace_builder_input.runtime_config
    )
    plan_payload["evidence_bundle_hash"] = "0" * 64
    provisional_plan = CapturePlan.model_validate(plan_payload)
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional_plan, trace_content_hash=envelope.trace_content_hash
    )
    plan = CapturePlan.model_validate(plan_payload)
    operation.expected_capture_source_sets = (
        SimpleNamespace(
            capture_plan_hash=plan.plan_hash,
            source_revision_set_id=plan.signal_source_revision_set_id,
            source_revision_set_hash=plan.signal_source_revision_set_hash,
        ),
    )
    draft = build_observation_semantic_draft(
        plan=plan, envelope=envelope, binding=binding
    )
    snapshot = SimpleNamespace(
        target_request_hash="d" * 64,
        target_projection_snapshot_hash="e" * 64,
        source_operation_projection=operation,
        source_revision_freeze_intent=replay.freeze_intent,
        historical_trace_projection=historical,
        projected_bytes=1,
    )
    request = Phase1GTransactionalWriteRequest(
        target_request_hash=snapshot.target_request_hash,
        phase1e_plan_id=snapshot.source_operation_projection.phase1e_plan_id,
        phase1e_plan_hash=snapshot.source_operation_projection.phase1e_plan_hash,
        g2_target_projection_snapshot_hash=snapshot.target_projection_snapshot_hash,
        capture_batch_id=binding.capture_batch_id,
        capture_request_hash="0" * 64,
        capture_attempt_no=1,
        expected_batch_row_version=2,
        capture_fencing_token=binding.capture_fencing_token,
        control_binding_event_hash=binding.control_binding_event_hash,
        capture_plan_hash=plan.plan_hash,
        trace_capture_context_hash=canonical_json_sha256(
            context.model_dump(mode="json")
        ),
        trace_capture_binding_hash=binding.binding_hash,
        trace_outbox_id=envelope.trace_outbox_id,
        stage_trace_envelope_hash=envelope.trace_content_hash,
        observation_semantic_key=draft.semantic_observation_key,
        observation_semantic_draft_hash=draft.draft_content_hash,
        expected_rows=9 + 2 * len(draft.candidate_semantic_rows),
        expected_bytes=envelope.size_bytes,
    )
    target = Phase1GTransactionalTargetInput(
        request=request,
        target_snapshot=snapshot,
        capture_plan=plan,
        trace_context=context,
        persisted_binding=binding,
        current_writer_binding=binding,
        envelope=envelope,
        semantic_draft=draft,
    )

    Phase1GTransactionalWriter._validate_input(target)
    tampered_request = request.model_copy(update={"trace_outbox_id": "sto_wrong"})
    with pytest.raises(Exception, match="identities do not close"):
        Phase1GTransactionalWriter._validate_input(
            Phase1GTransactionalTargetInput(
                **{**target.__dict__, "request": tampered_request}
            )
        )

    capacity_payload = request.model_dump(mode="python", exclude={"write_request_hash"})
    capacity_payload["expected_rows"] = 1
    insufficient = Phase1GTransactionalWriteRequest.model_validate(capacity_payload)
    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        Phase1GTransactionalWriter._validate_input(
            Phase1GTransactionalTargetInput(
                **{**target.__dict__, "request": insufficient}
            )
        )
    assert exc_info.value.reason_code == REASON_G3_CAPACITY_EXCEEDED


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("target_request_hash", "1" * 64),
        ("phase1e_plan_hash", "2" * 64),
        ("control_binding_event_hash", "3" * 64),
        ("capture_plan_hash", "4" * 64),
        ("stage_trace_envelope_hash", "5" * 64),
        ("observation_semantic_draft_hash", "6" * 64),
    ),
)
def test_writer_input_closure_rejects_each_cross_identity_drift(
    field_name: str, replacement: str
) -> None:
    plan, binding, context, envelope, draft = _pure_case()
    source_plan, source_target, event = g2_source_case()
    operation = parse_phase1g_source_operation(
        phase1e_plan=source_plan, target_request=source_target
    )
    replay = replay_phase1g_source_operation(
        projection=operation, availability_events=(event,)
    )
    request = Phase1GTransactionalWriteRequest(
        target_request_hash="a" * 64,
        phase1e_plan_id="phase1e-plan",
        phase1e_plan_hash="b" * 64,
        g2_target_projection_snapshot_hash="c" * 64,
        capture_batch_id=binding.capture_batch_id,
        capture_request_hash="d" * 64,
        capture_attempt_no=1,
        expected_batch_row_version=2,
        capture_fencing_token=binding.capture_fencing_token,
        control_binding_event_hash=binding.control_binding_event_hash,
        capture_plan_hash=plan.plan_hash,
        trace_capture_context_hash=canonical_json_sha256(
            context.model_dump(mode="json")
        ),
        trace_capture_binding_hash=binding.binding_hash,
        trace_outbox_id=envelope.trace_outbox_id,
        stage_trace_envelope_hash=envelope.trace_content_hash,
        observation_semantic_key=draft.semantic_observation_key,
        observation_semantic_draft_hash=draft.draft_content_hash,
        expected_rows=9 + 2 * len(draft.candidate_semantic_rows),
        expected_bytes=envelope.size_bytes,
    )
    source_ref = SimpleNamespace(
        capture_plan_hash=plan.plan_hash,
        source_revision_set_id=replay.source_revision_set.source_revision_set_id,
        source_revision_set_hash=replay.source_revision_set.source_revision_set_hash,
    )
    snapshot = SimpleNamespace(
        target_request_hash=request.target_request_hash,
        target_projection_snapshot_hash=request.g2_target_projection_snapshot_hash,
        source_operation_projection=SimpleNamespace(
            phase1e_plan_id=request.phase1e_plan_id,
            phase1e_plan_hash=request.phase1e_plan_hash,
            expected_capture_source_sets=(source_ref,),
        ),
        source_revision_freeze_intent=replay.freeze_intent,
        projected_bytes=1,
    )
    # The first identity closure block must reject these drifts before deeper
    # historical fields are accessed.
    payload = request.model_dump(mode="python", exclude={"write_request_hash"})
    payload[field_name] = replacement
    drifted = Phase1GTransactionalWriteRequest.model_validate(payload)
    with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
        Phase1GTransactionalWriter._validate_input(
            Phase1GTransactionalTargetInput(
                request=drifted,
                target_snapshot=snapshot,
                capture_plan=plan,
                trace_context=context,
                persisted_binding=binding,
                current_writer_binding=binding,
                envelope=envelope,
                semantic_draft=draft,
            )
        )
    assert exc_info.value.reason_code == REASON_G3_INPUT_INVALID


def test_transaction_connection_factory_failure_has_stable_reason_and_log(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    request = SimpleNamespace(
        target_request_hash="1" * 64,
        capture_batch_id="batch-connection-failure",
        capture_plan_hash="2" * 64,
    )
    target = SimpleNamespace(request=request)
    monkeypatch.setattr(
        Phase1GTransactionalWriter,
        "_validate_input",
        staticmethod(lambda _target: None),
    )

    def fail_connection():  # type: ignore[no-untyped-def]
        raise OSError("injected connection failure with no credentials")

    writer = Phase1GTransactionalWriter(
        transaction_connection_factory=fail_connection,
        readonly_connection_factory=fail_connection,
    )
    with caplog.at_level(
        logging.ERROR,
        logger="backend.services.advisory_phase1.phase1g_transactional_writer",
    ):
        with pytest.raises(Phase1GTransactionalWriterError) as exc_info:
            writer.write_target(target)  # type: ignore[arg-type]

    assert exc_info.value.reason_code == "ADVISORY_PHASE1G_G3_UNEXPECTED_ERROR"
    assert isinstance(exc_info.value.__cause__, OSError)
    assert exc_info.value.context == {"capture_batch_id": "batch-connection-failure"}
    records = [
        record
        for record in caplog.records
        if record.message == "phase1g g3 transaction connection acquisition failed"
    ]
    assert len(records) == 1
    assert records[0].reason_code == "ADVISORY_PHASE1G_G3_UNEXPECTED_ERROR"
    assert "password" not in caplog.text.lower()
