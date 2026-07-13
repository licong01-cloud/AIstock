from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from copy import deepcopy
import json
from pathlib import Path

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchRequest,
    CaptureBatchStatus,
    CaptureMembership,
    CapturePlan,
    InMemoryCaptureBatchRepository,
    InMemoryTraceAdmissionValidator,
    InMemoryTraceCaptureGapRepository,
    REASON_CAPTURE_BATCH_CONFLICT,
    REASON_CAPTURE_BATCH_FENCING_INVALID,
)
from backend.services.advisory_phase1.observation_capture import (
    FrozenTradingCalendarVerifier,
    InMemoryObservationCaptureRepository,
    REASON_OBSERVATION_CALENDAR_INVALID,
    REASON_OBSERVATION_PLAN_MISMATCH,
    expected_evidence_bundle_hash,
)
from backend.services.advisory_phase1.observation_selector import FixtureObservationVersion
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import (
    PHASE1_STAGE_TRACE_SCHEMA_VERSION,
    StageTraceEnvelope,
    TraceCaptureBinding,
    TraceCapturePolicy,
)
from backend.services.advisory_phase1.trace_outbox import (
    ExpectedTraceIdentity,
    InMemoryTraceOutboxRepository,
    TraceCaptureReconciler,
)


NOW = datetime(2026, 7, 13, 1, 0, tzinfo=timezone.utc)
SELECTION_RUN_ID = "capture-selection-run"
PACKAGE_ID = "capture-package"
MANIFEST_SHA256 = "a" * 64
DECISION_DATE = date(2026, 7, 10)


def _binding(*, batch_id: str = "capture-batch-1", fencing_token: int = 1) -> TraceCaptureBinding:
    return TraceCaptureBinding(
        control_binding_event_hash="b" * 64,
        binding_id="capture-binding",
        binding_version="1",
        handoff_readiness_hash="c" * 64,
        admission_scope_id="capture-scope",
        admission_scope_hash="d" * 64,
        capture_batch_id=batch_id,
        capture_fencing_token=fencing_token,
        capture_policy=TraceCapturePolicy(
            policy_id="capture-policy",
            policy_version="1",
            max_candidates=20,
            max_bytes=100_000,
            max_capture_ms=1_000,
        ),
    )


def _plan(*, evidence_bundle_hash: str = "4" * 64, alpha_mode: str = "multi_alpha") -> CapturePlan:
    return CapturePlan(
        selection_run_id=SELECTION_RUN_ID,
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA256,
        decision_as_of_trade_date=DECISION_DATE.isoformat(),
        selection_as_of_trade_date=DECISION_DATE.isoformat(),
        target_trade_date=date(2026, 7, 13).isoformat(),
        decision_cutoff_ts=NOW,
        alpha_mode=alpha_mode,
        selection_runtime_semantics_hash="0" * 64,
        package_effective_config_hash="9" * 64,
        calendar_version="market.trading_calendar.v1",
        calendar_hash="8" * 64,
        stable_signal_semantics_hash="e" * 64,
        canonical_signal_scope_hash="f" * 64,
        phase0a_audit_id="audit-capture",
        phase0a_audit_manifest_hash="1" * 64,
        handoff_readiness_hash="c" * 64,
        admission_scope_id="capture-scope",
        admission_scope_hash="d" * 64,
        signal_source_revision_set_id="source-revision-set",
        signal_source_revision_set_hash="2" * 64,
        phase0a_signal_context_hash="3" * 64,
        evidence_bundle_hash=evidence_bundle_hash,
        selection_evidence_id="selection-evidence-capture",
        selection_evidence_hash="5" * 64,
        selection_run_content_hash="6" * 64,
        selection_score_artifact_id="selection-artifact-capture",
        selection_score_artifact_hash="7" * 64,
        runtime_profile_version_id="runtime-profile-capture",
        runtime_profile_version_hash="8" * 64,
        hmm_snapshot_status="NOT_APPLICABLE",
        risk_policy_hash="9" * 64,
        universe_policy_hash="a" * 64,
        symbol_normalization_policy_hash="b" * 64,
        valid_no_candidate=False,
        evidence_available_at=NOW,
        audit_target_id="audit-target-capture",
        target_scope_hash="c" * 64,
        capability="HISTORICAL_RESEARCH",
        oos_interval_id="oos-capture",
        oos_interval_hash="d" * 64,
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        signal_evidence_level="RETROSPECTIVE_RESEARCH_ONLY",
        effective_cutoff_date=DECISION_DATE.isoformat(),
        program_id="program-capture",
        binding_version_id="binding-version-capture",
        source_run_id="source-run-capture",
        lineage_source_type="PHASE0A_AUDIT",
    )


def _request(*, batch_id: str = "capture-batch-1") -> CaptureBatchRequest:
    return CaptureBatchRequest(capture_batch_id=batch_id, binding=_binding(batch_id=batch_id), plans=(_plan(),))


def _observation_repository(
    plan: CapturePlan,
    *,
    gaps: InMemoryTraceCaptureGapRepository | None = None,
) -> InMemoryObservationCaptureRepository:
    return InMemoryObservationCaptureRepository(
        calendar_verifier=FrozenTradingCalendarVerifier(
            calendar_version=plan.calendar_version,
            calendar_hash=plan.calendar_hash,
            next_trade_dates=((plan.decision_as_of_trade_date, plan.target_trade_date),),
        ),
        gap_repository=gaps or InMemoryTraceCaptureGapRepository(),
    )


def _envelope(binding: TraceCaptureBinding, *, stage_trace: list[dict] | None = None) -> StageTraceEnvelope:
    content = canonicalize(
        {
            "schema_version": PHASE1_STAGE_TRACE_SCHEMA_VERSION,
            "selection_identity": {
                "selection_run_id": SELECTION_RUN_ID,
                "package_id": PACKAGE_ID,
                "manifest_sha256": MANIFEST_SHA256,
                "decision_as_of_trade_date": DECISION_DATE.isoformat(),
                "data_source": "DB_HISTORICAL",
                "execution_origin": "ADVISORY_RUN",
                "research_scope": "HISTORICAL_RESEARCH_ONLY",
                "execution_prohibited": True,
            },
            "trace_capture_binding": binding.model_dump(mode="json"),
            "raw_score_artifact": {"artifact_payload_sha256": "3" * 64, "scores_json": []},
            "stage_trace": stage_trace or [],
            "hmm_metadata": {},
            "risk_metadata": {},
            "universe_metadata": {},
            "component_capability": "NOT_APPLICABLE",
        }
    )
    digest = canonical_json_sha256(content)
    return StageTraceEnvelope(
        trace_outbox_id=f"sto_{digest[:20]}",
        trace_content_hash=digest,
        trace_content=content,
        candidate_count=0,
        size_bytes=len(json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")),
    )


def test_capture_batch_requires_explicit_historical_binding_and_matching_plan() -> None:
    request = _request()

    assert request.capture_request_hash
    assert request.plans[0].plan_hash
    mismatched_payload = _plan().model_dump(mode="python", exclude={"plan_hash"})
    mismatched_payload["admission_scope_hash"] = "4" * 64
    with pytest.raises(ValueError, match="capture plan does not match"):
        CaptureBatchRequest(
            capture_batch_id="capture-batch-bad",
            binding=_binding(batch_id="capture-batch-bad"),
            plans=(CapturePlan(**mismatched_payload),),
        )


def test_capture_batch_lease_fencing_membership_and_complete_are_fail_closed() -> None:
    clock = [NOW]
    repository = InMemoryCaptureBatchRepository(now_provider=lambda: clock[0])
    planned = repository.create(_request())

    running = repository.acquire(
        capture_batch_id=planned.request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=60,
    )
    assert running.status is CaptureBatchStatus.RUNNING
    assert running.row_version == 2

    with pytest.raises(SourceLedgerError) as excinfo:
        repository.add_membership(
            capture_batch_id=running.request.capture_batch_id,
            expected_row_version=running.row_version,
            fencing_token=2,
            membership=CaptureMembership(evidence_role="trace_outbox", evidence_id="trace-1", evidence_content_hash="5" * 64),
        )
    assert excinfo.value.reason_code == REASON_CAPTURE_BATCH_FENCING_INVALID

    with_membership = repository.add_membership(
        capture_batch_id=running.request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
        membership=CaptureMembership(evidence_role="trace_outbox", evidence_id="trace-1", evidence_content_hash="5" * 64),
    )
    same_membership = repository.add_membership(
        capture_batch_id=with_membership.request.capture_batch_id,
        expected_row_version=with_membership.row_version,
        fencing_token=with_membership.fencing_token,
        membership=CaptureMembership(evidence_role="trace_outbox", evidence_id="trace-1", evidence_content_hash="5" * 64),
    )
    assert same_membership.row_version == with_membership.row_version

    complete = repository.complete(
        capture_batch_id=same_membership.request.capture_batch_id,
        expected_row_version=same_membership.row_version,
        fencing_token=same_membership.fencing_token,
    )
    assert complete.status is CaptureBatchStatus.COMPLETE
    assert complete.membership_count == 1
    assert complete.capture_receipt_hash

    with pytest.raises(SourceLedgerError) as excinfo:
        repository.complete(
            capture_batch_id=complete.request.capture_batch_id,
            expected_row_version=complete.row_version,
            fencing_token=complete.fencing_token,
        )
    assert excinfo.value.reason_code == "ADVISORY_PHASE1_CAPTURE_BATCH_STATE_INVALID"


def test_real_in_memory_admission_validator_allows_only_running_matching_batch() -> None:
    repository = InMemoryCaptureBatchRepository(now_provider=lambda: NOW)
    planned = repository.create(_request())
    running = repository.acquire(
        capture_batch_id=planned.request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=60,
    )
    validator = InMemoryTraceAdmissionValidator(batches=repository)
    outbox = InMemoryTraceOutboxRepository(admission_validator=validator)
    envelope = _envelope(running.request.binding)

    record = outbox.append(envelope, binding=running.request.binding)

    assert record.trace_outbox_id == envelope.trace_outbox_id
    assert outbox.append(envelope, binding=running.request.binding) == record
    invalid_content = deepcopy(envelope.trace_content)
    invalid_content["selection_identity"]["data_source"] = "LIVE"
    invalid_hash = canonical_json_sha256(invalid_content)
    invalid_envelope = StageTraceEnvelope(
        trace_outbox_id=f"sto_{invalid_hash[:20]}",
        trace_content_hash=invalid_hash,
        trace_content=invalid_content,
        candidate_count=0,
        size_bytes=len(json.dumps(invalid_content, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")),
    )
    with pytest.raises(SourceLedgerError) as excinfo:
        validator.validate(envelope=invalid_envelope, binding=running.request.binding)
    assert excinfo.value.reason_code == "ADVISORY_PHASE1_TRACE_ADMISSION_BATCH_INVALID"
    complete = repository.complete(
        capture_batch_id=running.request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
    )
    assert complete.status is CaptureBatchStatus.COMPLETE
    assert outbox.append(envelope, binding=running.request.binding) == record


def test_capture_recovery_preserves_request_semantics_and_requires_terminal_predecessor() -> None:
    clock = [NOW]
    repository = InMemoryCaptureBatchRepository(now_provider=lambda: clock[0])
    planned = repository.create(_request())
    running = repository.acquire(
        capture_batch_id=planned.request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=60,
    )
    with pytest.raises(SourceLedgerError) as excinfo:
        repository.create(_request(batch_id="capture-batch-duplicate"))
    assert excinfo.value.reason_code == REASON_CAPTURE_BATCH_CONFLICT

    clock[0] = NOW + timedelta(seconds=61)
    expired = repository.expire(
        capture_batch_id=running.request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
    )
    assert expired.status is CaptureBatchStatus.EXPIRED
    recovery_request = _request(batch_id="capture-batch-2")

    recovered = repository.recover(
        request=recovery_request,
        predecessor_capture_batch_id=expired.request.capture_batch_id,
        expected_predecessor_row_version=expired.row_version,
        predecessor_fencing_token=expired.fencing_token,
    )

    assert recovered.capture_attempt_no == 2
    assert recovered.predecessor_capture_batch_id == expired.request.capture_batch_id
    assert recovered.request.capture_request_hash == expired.request.capture_request_hash

    recovered_running = repository.acquire(
        capture_batch_id=recovered.request.capture_batch_id,
        expected_row_version=recovered.row_version,
        lease_seconds=60,
    )
    recovered_failed = repository.fail(
        capture_batch_id=recovered_running.request.capture_batch_id,
        expected_row_version=recovered_running.row_version,
        fencing_token=recovered_running.fencing_token,
        reason_codes=("fixture_failure",),
    )
    with pytest.raises(SourceLedgerError) as excinfo:
        repository.recover(
            request=_request(batch_id="capture-batch-3"),
            predecessor_capture_batch_id=expired.request.capture_batch_id,
            expected_predecessor_row_version=expired.row_version,
            predecessor_fencing_token=expired.fencing_token,
        )
    assert excinfo.value.reason_code == REASON_CAPTURE_BATCH_CONFLICT

    with pytest.raises(SourceLedgerError) as excinfo:
        repository.recover(
            request=_request(batch_id="capture-batch-4"),
            predecessor_capture_batch_id=recovered_failed.request.capture_batch_id,
            expected_predecessor_row_version=recovered_failed.row_version - 1,
            predecessor_fencing_token=recovered_failed.fencing_token,
        )
    assert excinfo.value.reason_code == REASON_CAPTURE_BATCH_CONFLICT


def test_reconciler_records_only_capture_lost_gap_and_is_idempotent() -> None:
    binding = _binding()
    envelope = _envelope(binding)
    identity = ExpectedTraceIdentity.from_envelope(envelope, binding=binding)
    gaps = InMemoryTraceCaptureGapRepository()
    reconciler = TraceCaptureReconciler(outbox=InMemoryTraceOutboxRepository(), gap_handler=gaps)

    assert reconciler.reconcile((identity,)) == (identity,)
    assert reconciler.reconcile((identity,)) == (identity,)
    stored = gaps.record(identity=identity, reason_code="ADVISORY_PHASE1_TRACE_CAPTURE_LOST")
    assert stored.reason_code == "ADVISORY_PHASE1_TRACE_CAPTURE_LOST"


def _complete_stage_trace() -> list[dict]:
    candidate = {"symbol": "000001.SZ", "score": 1.25, "rank": 1, "reason": "fixture"}
    return [
        {
            "stage": "alpha_raw",
            "receipt": {
                "stage": "alpha_raw",
                "status": "COMPLETE",
                "input_count": 1,
                "output_count": 1,
                "excluded_count": 0,
                "candidates": [candidate],
                "exclusions": [],
                "semantic_payload": {"artifact_id": "artifact-1", "artifact_sha256": "e" * 64},
                "reason_codes": [],
            },
            "candidate_component_evidence": {"000001.SZ": {"capability": "NOT_APPLICABLE"}},
        },
        {
            "stage": "hmm_adjusted",
            "receipt": {
                "stage": "hmm_adjusted",
                "status": "NOT_APPLICABLE",
                "input_count": 1,
                "output_count": 0,
                "excluded_count": 0,
                "candidates": [],
                "exclusions": [],
                "semantic_payload": {"enabled": False},
                "reason_codes": [],
            },
            "candidate_component_evidence": {},
        },
        {
            "stage": "risk_policy_adjusted",
            "receipt": {
                "stage": "risk_policy_adjusted",
                "status": "COMPLETE",
                "input_count": 1,
                "output_count": 1,
                "excluded_count": 0,
                "candidates": [candidate],
                "exclusions": [],
                "semantic_payload": {},
                "reason_codes": [],
            },
            "candidate_component_evidence": {"000001.SZ": {"capability": "NOT_APPLICABLE"}},
        },
        {
            "stage": "selection_effective",
            "receipt": {
                "stage": "selection_effective",
                "status": "COMPLETE",
                "input_count": 1,
                "output_count": 1,
                "excluded_count": 0,
                "candidates": [candidate],
                "exclusions": [],
                "semantic_payload": {},
                "reason_codes": [],
            },
            "candidate_component_evidence": {"000001.SZ": {"capability": "NOT_APPLICABLE"}},
        },
    ]


def test_observation_writer_uses_only_frozen_plan_and_trace_content() -> None:
    binding = _binding()
    envelope = _envelope(binding, stage_trace=_complete_stage_trace())
    provisional = _plan(alpha_mode="single_alpha")
    plan_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=envelope.trace_content_hash,
    )
    plan = CapturePlan(**plan_payload)
    gaps = InMemoryTraceCaptureGapRepository()
    repository = _observation_repository(plan, gaps=gaps)

    record = repository.append(plan=plan, envelope=envelope, binding=binding)

    assert record.observation_payload["observation_status"] == "COMPLETE"
    assert canonical_json_sha256(record.observation_payload) == record.observation_content_hash
    assert [item["stage"] for item in record.stages] == [
        "alpha_raw",
        "hmm_adjusted",
        "risk_policy_adjusted",
        "selection_effective",
        "advisory_model",
    ]
    assert repository.append(plan=plan, envelope=envelope, binding=binding) == record
    selector_version = FixtureObservationVersion.from_capture_record(record)
    assert selector_version.observation_content_hash == record.observation_content_hash
    assert selector_version.signal_source_revision_set_hash == plan.signal_source_revision_set_hash

    invalid_payload = plan.model_dump(mode="python", exclude={"plan_hash"})
    invalid_payload["package_id"] = "different-package"
    invalid_plan = CapturePlan(**invalid_payload)
    with pytest.raises(SourceLedgerError) as excinfo:
        repository.append(plan=invalid_plan, envelope=envelope, binding=binding)
    assert excinfo.value.reason_code == REASON_OBSERVATION_PLAN_MISMATCH
    assert [gap.reason_code for gap in gaps.list()] == [REASON_OBSERVATION_PLAN_MISMATCH]


def test_observation_writer_rejects_non_adjacent_target_and_records_gap() -> None:
    binding = _binding()
    envelope = _envelope(binding, stage_trace=_complete_stage_trace())
    provisional = _plan(alpha_mode="single_alpha")
    plan_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=envelope.trace_content_hash,
    )
    plan = CapturePlan(**plan_payload)
    gaps = InMemoryTraceCaptureGapRepository()
    repository = InMemoryObservationCaptureRepository(
        calendar_verifier=FrozenTradingCalendarVerifier(
            calendar_version=plan.calendar_version,
            calendar_hash=plan.calendar_hash,
            next_trade_dates=((plan.decision_as_of_trade_date, "2026-07-14"),),
        ),
        gap_repository=gaps,
    )

    with pytest.raises(SourceLedgerError) as excinfo:
        repository.append(plan=plan, envelope=envelope, binding=binding)

    assert excinfo.value.reason_code == REASON_OBSERVATION_CALENDAR_INVALID
    assert [gap.reason_code for gap in gaps.list()] == [REASON_OBSERVATION_CALENDAR_INVALID]


def test_observation_writer_keeps_multi_alpha_component_evidence_or_marks_the_stage_partial() -> None:
    binding = _binding()
    complete_trace = _complete_stage_trace()
    component_payload = {
        "schema_version": "multi_alpha_component_evidence_v1",
        "parent_package_id": PACKAGE_ID,
        "components": [],
    }
    component = {
        "capability": "FULL",
        "schema_version": "multi_alpha_component_evidence_v1",
        "component_evidence": component_payload,
        "component_evidence_hash": canonical_json_sha256(component_payload),
    }
    for stage in complete_trace:
        if stage["stage"] != "hmm_adjusted":
            stage["candidate_component_evidence"] = {"000001.SZ": component}
    envelope = _envelope(binding, stage_trace=complete_trace)
    provisional = _plan(alpha_mode="multi_alpha")
    plan_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=envelope.trace_content_hash,
    )
    plan = CapturePlan(**plan_payload)
    repository = _observation_repository(plan)

    record = repository.append(plan=plan, envelope=envelope, binding=binding)

    assert record.observation_payload["observation_status"] == "COMPLETE"
    assert record.stages[0]["candidates"][0]["component_capability"] == "FULL"

    incomplete_trace = deepcopy(complete_trace)
    incomplete_trace[0]["candidate_component_evidence"] = {}
    incomplete_envelope = _envelope(binding, stage_trace=incomplete_trace)
    partial_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    partial_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=incomplete_envelope.trace_content_hash,
    )
    partial_plan = CapturePlan(**partial_payload)
    partial_record = _observation_repository(partial_plan).append(
        plan=partial_plan,
        envelope=incomplete_envelope,
        binding=binding,
    )
    assert partial_record.observation_payload["observation_status"] == "PARTIAL"


def test_observation_writer_requires_component_evidence_for_multi_alpha_exclusions() -> None:
    binding = _binding()
    trace = _complete_stage_trace()
    excluded = trace[0]["receipt"]["candidates"].pop()
    trace[0]["receipt"].update(output_count=0, excluded_count=1, exclusions=[excluded])
    component_payload = {
        "schema_version": "multi_alpha_component_evidence_v1",
        "parent_package_id": PACKAGE_ID,
        "components": [],
    }
    trace[0]["candidate_component_evidence"] = {
        excluded["symbol"]: {
            "capability": "FULL",
            "schema_version": "multi_alpha_component_evidence_v1",
            "component_evidence": component_payload,
            "component_evidence_hash": canonical_json_sha256(component_payload),
        }
    }
    for stage in trace[1:]:
        if stage["stage"] != "hmm_adjusted":
            stage["candidate_component_evidence"] = trace[0]["candidate_component_evidence"]
    envelope = _envelope(binding, stage_trace=trace)
    provisional = _plan(alpha_mode="multi_alpha")
    plan_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=envelope.trace_content_hash,
    )
    plan = CapturePlan(**plan_payload)

    record = _observation_repository(plan).append(plan=plan, envelope=envelope, binding=binding)
    excluded_row = record.stages[0]["candidates"][0]

    assert record.stages[0]["capability_status"] == "FULL"
    assert excluded_row["membership_status"] == "EXCLUDED"
    assert excluded_row["rank"] is None
    assert excluded_row["input_rank"] == 1
    assert excluded_row["component_capability"] == "FULL"
    assert excluded_row["component_reason_codes"] == []

    missing_trace = deepcopy(trace)
    missing_trace[0]["candidate_component_evidence"] = {}
    missing_envelope = _envelope(binding, stage_trace=missing_trace)
    missing_payload = provisional.model_dump(mode="python", exclude={"plan_hash"})
    missing_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
        plan=provisional,
        trace_content_hash=missing_envelope.trace_content_hash,
    )
    missing_plan = CapturePlan(**missing_payload)
    partial = _observation_repository(missing_plan).append(
        plan=missing_plan,
        envelope=missing_envelope,
        binding=binding,
    )
    assert partial.stages[0]["capability_status"] == "PARTIAL"
    assert partial.stages[0]["candidates"][0]["component_reason_codes"]


def test_capture_foundation_migration_keeps_ddl_out_of_runtime_and_enforces_core_contracts() -> None:
    migration = (
        Path(__file__).parents[2] / "db" / "migrations" / "add_advisory_phase1_capture_foundation_20260713.sql"
    ).read_text(encoding="utf-8")
    normalized = migration.lower()

    for relation in (
        "app.advisory_capture_batch",
        "app.advisory_capture_plan",
        "app.advisory_capture_batch_evidence_membership",
        "app.advisory_capture_gap",
        "app.advisory_signal_observation",
        "app.advisory_signal_observation_version",
        "app.advisory_signal_observation_lineage",
        "app.advisory_signal_stage_evidence",
        "app.advisory_signal_stage_candidate",
    ):
        assert f"create table if not exists {relation}" in normalized
    assert "advisory_phase1_capture_batch_cas_invalid" in normalized
    assert "advisory_phase1_capture_membership_admission_invalid" in normalized
    assert "advisory_phase1_capture_immutable" in normalized
    assert "advisory_phase1_observation_calendar_invalid" in normalized
    assert "advisory_phase1_observation_revision_chain_invalid" in normalized
    assert "unique (predecessor_capture_batch_id)" in normalized
    assert "create role" not in normalized
    assert "grant " not in normalized
    assert "create table if not exists app.advisory_approval" not in normalized
    assert "create table if not exists app.advisory_authorization" not in normalized
