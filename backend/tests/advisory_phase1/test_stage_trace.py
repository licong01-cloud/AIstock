"""Phase 1B contracts for bounded trace capture and component provenance."""

from __future__ import annotations

import ast
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Event
from time import perf_counter, sleep
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.stage_trace import (
    BoundedSelectionStageTraceSink,
    ComponentCapability,
    Phase1TraceCaptureReceipt,
    Phase1TraceCaptureService,
    REASON_COMPONENT_RANK_MISSING,
    REASON_TRACE_BYTE_LIMIT_EXCEEDED,
    REASON_TRACE_CANDIDATE_LIMIT_EXCEEDED,
    REASON_TRACE_CAPTURE_TIMEOUT,
    REASON_TRACE_OUTBOX_BLOCKING_WRITER,
    TraceCaptureBinding,
    TraceCaptureContext,
    TraceCapturePolicy,
    TraceCaptureResult,
    TraceCaptureState,
    build_component_evidence,
    build_stage_trace_envelope,
)
from backend.services.advisory_phase1.trace_outbox import (
    BoundedTraceOutboxDispatcher,
    ExpectedTraceIdentity,
    InMemoryTraceOutboxRepository,
    REASON_TRACE_CAPTURE_LOST,
    REASON_TRACE_DISPATCH_QUEUE_FULL,
    REASON_TRACE_ADMISSION_UNAVAILABLE,
    REASON_TRACE_OUTBOX_CHAIN_INVALID,
    REASON_TRACE_WRITE_FAILED,
    TraceCaptureReconciler,
    TraceDeliveryEventRequest,
    TraceDeliveryEventType,
)
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    EvidenceCaptureMode,
    ProspectiveExecutionOrigin,
    ProspectiveSelectionContext,
    SelectionStageTrace,
    StageReceiptStatus,
    build_stage_receipt,
)
from backend.services.simulation_runtime.selection import StrategyPackageSelectionService
from backend.services.advisory_phase1.source_ledger import SourceLedgerError


UTC = timezone.utc
PACKAGE_ID = "pkg_parent"
MANIFEST_SHA = "a" * 64
LEG_A_HASH = "b" * 64
LEG_B_HASH = "c" * 64
WEIGHT_HASH = "d" * 64
COMBINED_HASH = "e" * 64
ARTIFACT_HASH = "f" * 64
ARTIFACT_PAYLOAD_HASH = "1" * 64
INPUT_HASH = "2" * 64
SOURCE_HASH = "3" * 64
ASSET_HASH = "4" * 64


class _FixtureAdmissionValidator:
    """Test-only stand-in for the future persisted capture-batch validator."""

    def validate(self, *, envelope, binding, conn=None) -> None:  # type: ignore[no-untyped-def]
        assert envelope.trace_content["trace_capture_binding"]["binding_hash"] == binding.binding_hash


class _MutableAdmissionValidator(_FixtureAdmissionValidator):
    def __init__(self) -> None:
        self.enabled = True

    def validate(self, *, envelope, binding, conn=None) -> None:  # type: ignore[no-untyped-def]
        if not self.enabled:
            raise SourceLedgerError(REASON_TRACE_ADMISSION_UNAVAILABLE, "capture batch is no longer running")
        super().validate(envelope=envelope, binding=binding, conn=conn)


class _BlockingWriter:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()

    def append(self, envelope, *, binding) -> None:  # type: ignore[no-untyped-def]
        self.started.set()
        self.release.wait(timeout=2)


class _RaisingCaptureService:
    enabled = True

    def capture_package(self, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("simulated trace callback failure")


class _RecordingCaptureService:
    enabled = True

    def __init__(self) -> None:
        self.kwargs = None

    def capture_package(self, **kwargs):  # type: ignore[no-untyped-def]
        self.kwargs = kwargs
        return TraceCaptureResult(package_id=kwargs["package_id"], state=TraceCaptureState.DISABLED)


def _binding(
    *, max_candidates: int = 10, max_bytes: int = 100_000, max_capture_ms: int = 10_000
) -> TraceCaptureBinding:
    return TraceCaptureBinding(
        control_binding_event_hash="7" * 64,
        binding_id="trace-binding",
        binding_version="1",
        handoff_readiness_hash="5" * 64,
        admission_scope_id="scope-1",
        admission_scope_hash="6" * 64,
        capture_batch_id="batch-1",
        capture_fencing_token=7,
        capture_policy=TraceCapturePolicy(
            policy_id="trace-policy",
            policy_version="1",
            max_candidates=max_candidates,
            max_bytes=max_bytes,
            max_capture_ms=max_capture_ms,
        ),
    )


def _candidate(*, include_rank: bool = True) -> SelectionCandidate:
    leg_a = {"raw_score": 1.0, "normalized_score": 0.2, "weight": 0.4}
    leg_b = {"raw_score": 3.0, "normalized_score": 0.8, "weight": 0.6}
    if include_rank:
        leg_a["leg_rank"] = 2
        leg_b["leg_rank"] = 1
    return SelectionCandidate(
        symbol="000001.SZ",
        score=0.56,
        rank=1,
        component_scores={"leg_a": leg_a, "leg_b": leg_b},
        reason="live_multi_alpha_inference_score",
    )


def _manifest() -> SimpleNamespace:
    return SimpleNamespace(
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        manifest_version="alpha_core_v1",
        alpha_mode="multi_alpha",
        alpha_components=[SimpleNamespace(alpha_id="leg_b"), SimpleNamespace(alpha_id="leg_a")],
        alpha_combination_policy={"method": "weighted_sum", "weights": {"leg_a": 0.4, "leg_b": 0.6}},
        source_evidence={
            "multi_alpha": {
                "legs": [
                    {"leg_id": "leg_b", "child_package_id": "pkg_b", "child_manifest_sha256": LEG_B_HASH},
                    {"leg_id": "leg_a", "child_package_id": "pkg_a", "child_manifest_sha256": LEG_A_HASH},
                ]
            }
        },
    )


def _artifact(*, candidate: SelectionCandidate | None = None, candidates: list[SelectionCandidate] | None = None) -> SimpleNamespace:
    candidate = candidate or _candidate()
    candidates = candidates or [candidate]
    component_hashes = {"leg_a": LEG_A_HASH, "leg_b": LEG_B_HASH}
    weights = {"leg_a": 0.4, "leg_b": 0.6}
    parity = {
        "parent_package_id": PACKAGE_ID,
        "parent_manifest_sha256": MANIFEST_SHA,
        "leg_ids": ["leg_b", "leg_a"],
        "component_score_artifact_sha256": component_hashes,
        "weight_artifact_id": "weight-1",
        "weight_artifact_sha256": WEIGHT_HASH,
        "combined_score_artifact_sha256": COMBINED_HASH,
        "normalization_method": "zscore",
        "weights": weights,
    }
    return SimpleNamespace(
        artifact_id="artifact-1",
        artifact_contract_version="selection_score_artifact_v2",
        artifact_payload_sha256=ARTIFACT_PAYLOAD_HASH,
        artifact_sha256=ARTIFACT_HASH,
        artifact_input_context_hash=INPUT_HASH,
        source_revision_set_hash=SOURCE_HASH,
        asset_closure_hash=ASSET_HASH,
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        scores_json=[item.model_dump(mode="json") for item in candidates],
        metadata={
            "multi_alpha_parent_parity": parity,
            "multi_alpha_parent_parity_hash": canonical_json_sha256(parity),
            "component_artifacts": {
                "leg_a": {"component_score_artifact_sha256": LEG_A_HASH},
                "leg_b": {"component_score_artifact_sha256": LEG_B_HASH},
            },
            "component_score_artifact_sha256": component_hashes,
            "weight_artifact_id": "weight-1",
            "weight_artifact_sha256": WEIGHT_HASH,
            "combined_score_artifact_sha256": COMBINED_HASH,
            "weights": weights,
            "final_topk": 5,
        },
    )


def _stage_trace(candidate: SelectionCandidate | None = None) -> SelectionStageTrace:
    candidate = candidate or _candidate()
    receipts = {}
    for stage in (
        CandidateStageName.ALPHA_RAW,
        CandidateStageName.HMM_ADJUSTED,
        CandidateStageName.RISK_POLICY_ADJUSTED,
        CandidateStageName.SELECTION_EFFECTIVE,
    ):
        receipts[stage] = build_stage_receipt(
            stage=stage,
            status=StageReceiptStatus.COMPLETE,
            input_count=1,
            candidates=[candidate],
        )
    return SelectionStageTrace(
        alpha_raw=receipts[CandidateStageName.ALPHA_RAW],
        hmm_adjusted=receipts[CandidateStageName.HMM_ADJUSTED],
        risk_policy_adjusted=receipts[CandidateStageName.RISK_POLICY_ADJUSTED],
        selection_effective=receipts[CandidateStageName.SELECTION_EFFECTIVE],
    )


def _context(binding: TraceCaptureBinding | None = None) -> TraceCaptureContext:
    return TraceCaptureContext(
        selection_run_id="selection-run-1",
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        decision_as_of_trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        binding=binding or _binding(),
    )


def test_native_multi_alpha_component_evidence_is_canonical_and_complete() -> None:
    result = build_component_evidence(
        manifest=_manifest(), artifact=_artifact(), candidate=_candidate().model_dump(mode="json"), runtime_config={}
    )

    assert result.capability is ComponentCapability.FULL
    assert result.component_evidence is not None
    assert [item["leg_role"] for item in result.component_evidence["components"]] == ["leg_a", "leg_b"]
    assert result.component_evidence["components"][0]["leg_rank"] == 2
    assert result.component_evidence["runtime_variant_id"] is None
    assert result.component_evidence_hash == canonical_json_sha256(result.component_evidence)


def test_multi_alpha_exclusions_keep_component_provenance_in_trace_envelope() -> None:
    candidate = _candidate()
    exclusion = SelectionExclusion(
        symbol=candidate.symbol,
        score=candidate.score,
        rank=candidate.rank,
        reason="fixture_exclusion",
        source="fixture",
    )
    receipts = {}
    for stage in (
        CandidateStageName.ALPHA_RAW,
        CandidateStageName.HMM_ADJUSTED,
        CandidateStageName.RISK_POLICY_ADJUSTED,
        CandidateStageName.SELECTION_EFFECTIVE,
    ):
        receipts[stage] = build_stage_receipt(
            stage=stage,
            status=StageReceiptStatus.COMPLETE,
            input_count=1,
            candidates=[],
            exclusions=[exclusion],
        )
    trace = SelectionStageTrace(
        alpha_raw=receipts[CandidateStageName.ALPHA_RAW],
        hmm_adjusted=receipts[CandidateStageName.HMM_ADJUSTED],
        risk_policy_adjusted=receipts[CandidateStageName.RISK_POLICY_ADJUSTED],
        selection_effective=receipts[CandidateStageName.SELECTION_EFFECTIVE],
    )

    envelope = build_stage_trace_envelope(
        context=_context(),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=trace,
        runtime_config={},
    )

    for stage in envelope.trace_content["stage_trace"]:
        component = stage["candidate_component_evidence"][candidate.symbol]
        assert component["capability"] in {"FULL", "PARTIAL"}
        if component["capability"] == "FULL":
            assert component["component_evidence_hash"]
        else:
            assert component["reason_codes"]
    assert envelope.trace_content["component_capability"] == "PARTIAL"


def test_missing_leg_rank_is_partial_without_changing_parent_candidate() -> None:
    candidate = _candidate(include_rank=False)
    result = build_component_evidence(
        manifest=_manifest(), artifact=_artifact(candidate=candidate), candidate=candidate.model_dump(mode="json"), runtime_config={}
    )

    assert result.capability is ComponentCapability.PARTIAL
    assert result.component_evidence is None
    assert result.reason_codes == (REASON_COMPONENT_RANK_MISSING,)
    assert candidate.score == 0.56
    assert candidate.rank == 1


def test_missing_component_payload_is_unavailable_without_changing_parent_candidate() -> None:
    candidate = _candidate().model_copy(update={"component_scores": {}})
    result = build_component_evidence(
        manifest=_manifest(), artifact=_artifact(candidate=candidate), candidate=candidate.model_dump(mode="json"), runtime_config={}
    )

    assert result.capability is ComponentCapability.UNAVAILABLE
    assert result.component_evidence is None
    assert candidate.score == 0.56
    assert candidate.rank == 1


def test_later_stage_binds_raw_component_payload_without_requiring_raw_rank_or_score() -> None:
    raw_candidate = _candidate()
    adjusted_candidate = raw_candidate.model_copy(update={"score": 0.73, "rank": 2})
    result = build_component_evidence(
        manifest=_manifest(),
        artifact=_artifact(candidate=raw_candidate),
        candidate=adjusted_candidate.model_dump(mode="json"),
        runtime_config={},
        stage_name="hmm_adjusted",
    )

    assert result.capability is ComponentCapability.FULL
    assert result.component_evidence is not None
    assert result.component_evidence["combined_score_decimal"] == "0.56"


def test_sink_copies_immutable_payload_and_enforces_candidate_bound() -> None:
    candidate = _candidate()
    context = _context()
    sink = BoundedSelectionStageTraceSink()
    result = sink.capture(
        context=context,
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )

    assert result.state is TraceCaptureState.ENVELOPE_READY
    assert result.envelope is not None
    before = result.envelope.trace_content["raw_score_artifact"]["scores_json"][0]["component_scores"]["leg_a"]["raw_score"]
    candidate.component_scores["leg_a"]["raw_score"] = 999.0
    assert float(before) == 1.0


def test_sink_returns_explicit_partial_for_candidate_limit() -> None:
    candidate = _candidate()
    trace = _stage_trace(candidate)
    extra = _candidate().model_copy(update={"symbol": "000002.SZ", "rank": 2})
    trace = trace.model_copy(
        update={
            "alpha_raw": build_stage_receipt(
                stage=CandidateStageName.ALPHA_RAW,
                status=StageReceiptStatus.COMPLETE,
                input_count=2,
                candidates=[candidate, extra],
            )
        }
    )
    result = BoundedSelectionStageTraceSink().capture(
        context=_context(_binding(max_candidates=1)),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate, candidates=[candidate, extra]),
        stage_trace=trace,
        runtime_config={},
    )

    assert result.state is TraceCaptureState.PARTIAL
    assert result.reason_codes == (REASON_TRACE_CANDIDATE_LIMIT_EXCEEDED,)


def test_sink_rejects_oversized_input_before_immutable_copy() -> None:
    candidate = _candidate()
    artifact = _artifact(candidate=candidate)
    artifact.scores_json[0]["component_scores"]["leg_a"]["diagnostic_blob"] = "x" * 4_096
    result = BoundedSelectionStageTraceSink().capture(
        context=_context(_binding(max_bytes=1_024)),
        manifest=_manifest(),
        artifact=artifact,
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )

    assert result.state is TraceCaptureState.PARTIAL
    assert result.reason_codes == (REASON_TRACE_BYTE_LIMIT_EXCEEDED,)


def test_sink_rechecks_final_envelope_size_after_bounded_input_preflight() -> None:
    candidate = _candidate()
    sink = BoundedSelectionStageTraceSink()
    result = sink.capture(
        context=_context(_binding(max_bytes=5_000)),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    sink.shutdown(wait=True)

    assert result.state is TraceCaptureState.PARTIAL
    assert result.reason_codes == (REASON_TRACE_BYTE_LIMIT_EXCEEDED,)


def test_sink_time_budget_returns_without_waiting_for_slow_capture(monkeypatch) -> None:  # noqa: ANN001
    from backend.services.advisory_phase1 import stage_trace as stage_trace_module

    candidate = _candidate()
    original = stage_trace_module.build_stage_trace_envelope

    def slow_build(**kwargs):  # type: ignore[no-untyped-def]
        sleep(0.15)
        return original(**kwargs)

    monkeypatch.setattr(stage_trace_module, "build_stage_trace_envelope", slow_build)
    sink = BoundedSelectionStageTraceSink()
    started = perf_counter()
    result = sink.capture(
        context=_context(_binding(max_capture_ms=10)),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    elapsed = perf_counter() - started
    sink.shutdown(wait=True)

    assert elapsed < 0.08
    assert result.state is TraceCaptureState.PARTIAL
    assert result.reason_codes == (REASON_TRACE_CAPTURE_TIMEOUT,)


def test_outbox_is_idempotent_and_delivery_chain_is_fail_closed() -> None:
    candidate = _candidate()
    binding = _binding()
    sink_result = BoundedSelectionStageTraceSink().capture(
        context=_context(binding),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    assert sink_result.envelope is not None
    repository = InMemoryTraceOutboxRepository(
        now_provider=lambda: datetime(2026, 7, 10, tzinfo=UTC), admission_validator=_FixtureAdmissionValidator()
    )
    first = repository.append(sink_result.envelope, binding=binding)
    assert repository.append(sink_result.envelope, binding=binding) == first
    failed = repository.append_delivery(
        TraceDeliveryEventRequest(
            trace_outbox_id=first.trace_outbox_id,
            delivery_event_no=1,
            event_type=TraceDeliveryEventType.OBSERVATION_WRITE_FAILED,
            writer_attempt_no=1,
            reason_codes=("WRITER_DOWN",),
        )
    )
    written = repository.append_delivery(
        TraceDeliveryEventRequest(
            trace_outbox_id=first.trace_outbox_id,
            delivery_event_no=2,
            predecessor_event_hash=failed.delivery_event_hash,
            event_type=TraceDeliveryEventType.OBSERVATION_WRITTEN,
            writer_attempt_no=2,
        )
    )
    with pytest.raises(SourceLedgerError, match=REASON_TRACE_OUTBOX_CHAIN_INVALID):
        repository.append_delivery(
            TraceDeliveryEventRequest(
                trace_outbox_id=first.trace_outbox_id,
                delivery_event_no=3,
                predecessor_event_hash=written.delivery_event_hash,
                event_type=TraceDeliveryEventType.OBSERVATION_WRITE_FAILED,
                writer_attempt_no=3,
            )
        )


def test_outbox_exact_retry_does_not_require_a_still_running_capture_lease() -> None:
    candidate = _candidate()
    binding = _binding()
    sink = BoundedSelectionStageTraceSink()
    sink_result = sink.capture(
        context=_context(binding),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    sink.shutdown(wait=True)
    assert sink_result.envelope is not None
    validator = _MutableAdmissionValidator()
    repository = InMemoryTraceOutboxRepository(admission_validator=validator)

    first = repository.append(sink_result.envelope, binding=binding)
    validator.enabled = False

    assert repository.append(sink_result.envelope, binding=binding) == first


def test_capture_service_never_raises_when_outbox_writer_is_absent() -> None:
    candidate = _candidate()
    service = Phase1TraceCaptureService(binding=_binding(), sink=BoundedSelectionStageTraceSink())
    result = service.capture_package(
        selection_run_id="selection-run-1",
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        decision_as_of_trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )

    assert result.state is TraceCaptureState.OUTBOX_WRITE_FAILED
    assert result.envelope is not None


def test_capture_service_persists_exact_envelope_without_rerunning_selection() -> None:
    candidate = _candidate()
    repository = InMemoryTraceOutboxRepository(
        now_provider=lambda: datetime(2026, 7, 10, tzinfo=UTC), admission_validator=_FixtureAdmissionValidator()
    )
    failures = []
    dispatcher = BoundedTraceOutboxDispatcher(
        writer=repository,
        failure_handler=lambda **kwargs: failures.append(kwargs),
    )
    service = Phase1TraceCaptureService(binding=_binding(), sink=BoundedSelectionStageTraceSink(), outbox_writer=dispatcher)
    kwargs = {
        "selection_run_id": "selection-run-1",
        "package_id": PACKAGE_ID,
        "manifest_sha256": MANIFEST_SHA,
        "decision_as_of_trade_date": date(2026, 7, 10),
        "data_source": "DB_HISTORICAL",
        "execution_origin": "ADVISORY_RUN",
        "research_scope": "HISTORICAL_RESEARCH_ONLY",
        "execution_prohibited": True,
        "manifest": _manifest(),
        "artifact": _artifact(candidate=candidate),
        "stage_trace": _stage_trace(candidate),
        "runtime_config": {},
    }

    first = service.capture_package(**kwargs)
    second = service.capture_package(**kwargs)

    dispatcher.join()
    dispatcher.shutdown()

    assert first.state is TraceCaptureState.OUTBOX_QUEUED
    assert second.state is TraceCaptureState.OUTBOX_QUEUED
    assert first.envelope is not None
    assert repository.append(first.envelope, binding=_binding()).trace_outbox_id == first.envelope.trace_outbox_id
    assert failures == []
    assert candidate.rank == 1


def test_capture_service_rejects_a_blocking_repository_on_selection_thread() -> None:
    candidate = _candidate()
    service = Phase1TraceCaptureService(
        binding=_binding(),
        sink=BoundedSelectionStageTraceSink(),
        outbox_writer=InMemoryTraceOutboxRepository(admission_validator=_FixtureAdmissionValidator()),
    )

    result = service.capture_package(
        selection_run_id="selection-run-1",
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        decision_as_of_trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )

    assert result.state is TraceCaptureState.OUTBOX_WRITE_FAILED
    assert result.reason_codes == (REASON_TRACE_OUTBOX_BLOCKING_WRITER,)


def test_async_writer_failure_is_not_mislabeled_as_crash_reconciliation_loss() -> None:
    candidate = _candidate()
    failures = []
    dispatcher = BoundedTraceOutboxDispatcher(
        writer=InMemoryTraceOutboxRepository(),
        failure_handler=lambda **kwargs: failures.append(kwargs),
    )
    service = Phase1TraceCaptureService(binding=_binding(), sink=BoundedSelectionStageTraceSink(), outbox_writer=dispatcher)

    result = service.capture_package(
        selection_run_id="selection-run-1",
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        decision_as_of_trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    dispatcher.shutdown()

    assert result.state is TraceCaptureState.OUTBOX_QUEUED
    assert failures[0]["reason_code"] == REASON_TRACE_WRITE_FAILED


def test_async_dispatch_queue_is_bounded_and_fails_loud() -> None:
    candidate = _candidate()
    binding = _binding()
    sink = BoundedSelectionStageTraceSink()
    sink_result = sink.capture(
        context=_context(binding),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    sink.shutdown(wait=True)
    assert sink_result.envelope is not None
    writer = _BlockingWriter()
    dispatcher = BoundedTraceOutboxDispatcher(writer=writer, failure_handler=lambda **_kwargs: None, max_pending=1)
    dispatcher.append(sink_result.envelope, binding=binding)
    assert writer.started.wait(timeout=1)
    dispatcher.append(sink_result.envelope, binding=binding)

    with pytest.raises(SourceLedgerError, match=REASON_TRACE_DISPATCH_QUEUE_FULL):
        dispatcher.append(sink_result.envelope, binding=binding)

    writer.release.set()
    dispatcher.shutdown()


def test_reconciliation_emits_capture_lost_only_when_durable_outbox_is_missing() -> None:
    candidate = _candidate()
    binding = _binding()
    sink = BoundedSelectionStageTraceSink()
    sink_result = sink.capture(
        context=_context(binding),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    sink.shutdown(wait=True)
    assert sink_result.envelope is not None
    identity = ExpectedTraceIdentity.from_envelope(sink_result.envelope, binding=binding)
    gaps = []
    repository = InMemoryTraceOutboxRepository(admission_validator=_FixtureAdmissionValidator())
    reconciler = TraceCaptureReconciler(
        outbox=repository,
        gap_handler=lambda **kwargs: gaps.append(kwargs),
    )

    assert reconciler.reconcile([identity]) == (identity,)
    assert gaps == [{"identity": identity, "reason_code": REASON_TRACE_CAPTURE_LOST}]

    repository.append(sink_result.envelope, binding=binding)
    gaps.clear()

    assert reconciler.reconcile([identity]) == ()
    assert gaps == []


def test_selection_capture_boundary_converts_callback_fault_to_receipt() -> None:
    candidate = _candidate()
    service = object.__new__(StrategyPackageSelectionService)
    service.phase1_trace_capture_service = _RaisingCaptureService()
    context = ProspectiveSelectionContext(
        capture_mode=EvidenceCaptureMode.PROSPECTIVE,
        selection_run_id="selection-run-1",
        execution_origin=ProspectiveExecutionOrigin.ADVISORY_RUN,
        decision_clock_seed={"decision_as_of_trade_date": date(2026, 7, 9)},
    )
    record = SimpleNamespace(current_manifest=_manifest)

    receipt = service._capture_phase1_stage_traces(
        prospective_context=context,
        data_source="DB_HISTORICAL",
        trade_date=date(2026, 7, 10),
        records_by_id={PACKAGE_ID: record},
        package_runtime_configs={PACKAGE_ID: {}},
        artifact_by_package={PACKAGE_ID: _artifact(candidate=candidate)},
        stage_trace_by_package={PACKAGE_ID: _stage_trace(candidate)},
    )

    assert receipt.requested is True
    assert receipt.results_by_package[PACKAGE_ID].state is TraceCaptureState.CAPTURE_FAILED
    assert candidate.rank == 1
    assert candidate.score == 0.56


def test_selection_capture_uses_frozen_decision_date_not_target_trade_date() -> None:
    candidate = _candidate()
    capture_service = _RecordingCaptureService()
    service = object.__new__(StrategyPackageSelectionService)
    service.phase1_trace_capture_service = capture_service
    context = ProspectiveSelectionContext(
        capture_mode=EvidenceCaptureMode.PROSPECTIVE,
        selection_run_id="selection-run-1",
        execution_origin=ProspectiveExecutionOrigin.ADVISORY_RUN,
        decision_clock_seed={
            "decision_as_of_trade_date": date(2026, 7, 9),
            "target_trade_date": date(2026, 7, 10),
        },
    )

    service._capture_phase1_stage_traces(
        prospective_context=context,
        data_source="DB_HISTORICAL",
        trade_date=date(2026, 7, 10),
        records_by_id={PACKAGE_ID: SimpleNamespace(current_manifest=_manifest)},
        package_runtime_configs={PACKAGE_ID: {}},
        artifact_by_package={PACKAGE_ID: _artifact(candidate=candidate)},
        stage_trace_by_package={PACKAGE_ID: _stage_trace(candidate)},
    )

    assert capture_service.kwargs["decision_as_of_trade_date"] == date(2026, 7, 9)


def test_disabled_trace_capture_is_a_true_noop_without_decision_clock() -> None:
    candidate = _candidate()
    service = object.__new__(StrategyPackageSelectionService)
    service.phase1_trace_capture_service = Phase1TraceCaptureService()
    context = ProspectiveSelectionContext(
        capture_mode=EvidenceCaptureMode.PROSPECTIVE,
        selection_run_id="selection-run-1",
        execution_origin=ProspectiveExecutionOrigin.ADVISORY_RUN,
    )

    receipt = service._capture_phase1_stage_traces(
        prospective_context=context,
        data_source="DB_HISTORICAL",
        trade_date=date(2026, 7, 10),
        records_by_id={PACKAGE_ID: SimpleNamespace(current_manifest=_manifest)},
        package_runtime_configs={PACKAGE_ID: {}},
        artifact_by_package={PACKAGE_ID: _artifact(candidate=candidate)},
        stage_trace_by_package={PACKAGE_ID: _stage_trace(candidate)},
    )

    assert receipt == Phase1TraceCaptureReceipt.disabled()


def test_outbox_is_fail_closed_until_capture_admission_is_available() -> None:
    candidate = _candidate()
    binding = _binding()
    sink_result = BoundedSelectionStageTraceSink().capture(
        context=_context(binding),
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    assert sink_result.envelope is not None
    with pytest.raises(SourceLedgerError, match=REASON_TRACE_ADMISSION_UNAVAILABLE):
        InMemoryTraceOutboxRepository().append(sink_result.envelope, binding=binding)


def test_capture_receipt_preserves_fail_closed_admission_reason() -> None:
    candidate = _candidate()
    failures = []
    dispatcher = BoundedTraceOutboxDispatcher(
        writer=InMemoryTraceOutboxRepository(),
        failure_handler=lambda **kwargs: failures.append(kwargs),
    )
    service = Phase1TraceCaptureService(
        binding=_binding(), sink=BoundedSelectionStageTraceSink(), outbox_writer=dispatcher
    )
    result = service.capture_package(
        selection_run_id="selection-run-1",
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA,
        decision_as_of_trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        execution_origin="ADVISORY_RUN",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        manifest=_manifest(),
        artifact=_artifact(candidate=candidate),
        stage_trace=_stage_trace(candidate),
        runtime_config={},
    )
    dispatcher.shutdown()

    assert result.state is TraceCaptureState.OUTBOX_QUEUED
    assert str(failures[0]["error"].reason_code) == REASON_TRACE_ADMISSION_UNAVAILABLE


def test_trace_module_has_no_runtime_or_io_imports() -> None:
    path = "backend/services/advisory_phase1/stage_trace.py"
    tree = ast.parse(open(path, encoding="utf-8").read())
    imports = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    ] + [
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    ]
    forbidden = ("psycopg2", "requests", "http", "paper", "simulation", "qmt", "broker", "hmm")
    assert not [name for name in imports if any(token in name.lower() for token in forbidden)]


def test_phase1_migration_enforces_trace_outbox_and_delivery_chain() -> None:
    migration = (
        Path(__file__).parents[2] / "db" / "migrations" / "add_advisory_source_availability_ledger_20260712.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS app.advisory_selection_stage_trace_outbox" in migration
    assert "CREATE TABLE IF NOT EXISTS app.advisory_selection_stage_trace_delivery_event" in migration
    assert "CREATE TABLE IF NOT EXISTS app.advisory_phase1_control_binding_event" in migration
    assert "fk_advisory_stage_trace_outbox_control_binding" in migration
    assert "UNIQUE (selection_run_id, package_id, manifest_sha256, decision_as_of_trade_date, capture_policy_hash)" in migration
    assert "ux_advisory_stage_trace_delivery_one_successor" in migration
    assert "ADVISORY_PHASE1_TRACE_DELIVERY_CHAIN_INVALID" in migration
    assert "BEFORE UPDATE OR DELETE ON app.advisory_selection_stage_trace_outbox" in migration
