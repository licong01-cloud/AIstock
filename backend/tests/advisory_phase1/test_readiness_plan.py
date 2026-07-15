from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase0a.evidence_projection import (
    ProjectedAdvisoryProgram,
    ProjectedAlphaMode,
    ProjectedBindingVersion,
    ProjectedDailySelectionEvidence,
    ProjectedPackage,
    ProjectedPackageManifest,
    ProjectedSelectionScoreArtifact,
)
from backend.services.advisory_phase0a.historical_research import (
    HISTORICAL_RESEARCH_DATA_SOURCE,
    HISTORICAL_RESEARCH_SCOPE,
    HistoricalResearchBatch,
    HistoricalResearchBatchReceipt,
    HistoricalResearchProgramContext,
    HistoricalResearchProgramRun,
    HistoricalResearchRunStatus,
    HistoricalSelectionEvidence,
    _batch_receipt_payload,
    _program_payload_hash,
)
from backend.services.advisory_phase0a.models import (
    CandidateAuthorityStatus,
    CandidateAuthorityReport,
    DecisionClockEvidence,
    FormalOOSStatus,
    HandoffAdmissionScope,
    HandoffEvidenceScope,
    HandoffReadiness,
    Phase0APolicyRegistry,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase0a.resolvers import AuditReaders
from backend.services.advisory_phase1.readiness_plan import (
    OperationDisposition,
    Phase1EPlanBatchReceipt,
    Phase1EAuditOutcome,
    Phase1EEvidenceBinding,
    Phase1EError,
    Phase1EWorkloadProjection,
    Phase1EPlannedOperation,
    Phase1EProgramDateEvidence,
    Phase1EProgramDateRequest,
    Phase1EReadinessPlanCompiler,
    Phase1ERevalidationBatchRequest,
    PlanUnitKind,
    PlannedOperationType,
    REASON_POLICY_REGISTRY_HASH_MISMATCH,
    REASON_CAPACITY_REFERENCE_MISMATCH,
    REASON_HISTORICAL_RECEIPT_CONFLICT,
    RegistrySourceRequirementCompiler,
    SourceRequirementRegistry,
    SourceRequirementTemplate,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
    Phase1EPlanUnitKind,
)
from backend.services.advisory_phase1.phase1g_contract import REASON_TARGET_DIAGNOSTIC
from backend.services.advisory_phase1.phase1g_service import Phase1GService
from backend.services.advisory_phase1.source_capacity import (
    CapacityMeasurements,
    CapacityPlanningRequest,
    CapacityStatus,
    build_capacity_receipt,
)
from backend.services.advisory_phase1.source_observer import SOURCE_QUERY_TEMPLATES, default_source_observer_config
from backend.services.advisory_phase1.source_revision import AvailabilityRequirement, SourceRevisionKind


NOW = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)
H = "a" * 64


def _policy() -> Phase0APolicyRegistry:
    return Phase0APolicyRegistry(
        policy_registry_id="phase1e-test-policy",
        policy_version="phase1e-test-v1",
        registry_content_hash="b" * 64,
        frozen_at=NOW,
        effective_from_trade_date=date(2026, 1, 1),
        benchmark_policy={"policy_hash": "c" * 64},
        cost_policy={"policy_hash": "d" * 64},
        label_policy={"policy_hash": "e" * 64},
        universe_policy={"policy_hash": "f" * 64},
        embargo_policy={
            "policy_id": "embargo",
            "policy_version": "v1",
            "policy_hash": "1" * 64,
            "cutoff_timestamp_normalization": "utc",
            "training_label_information_end_rule": "close",
            "calendar_version": "cal-v1",
            "calendar_hash": "2" * 64,
        },
        prior_policy={"policy_hash": "3" * 64},
        multiple_testing_policy={"policy_hash": "4" * 64},
        style_assignment_policy={"by_package": {"pkg": "SHORT_REBOUND"}},
    )


def _capacity_request() -> CapacityPlanningRequest:
    config = default_source_observer_config()
    return CapacityPlanningRequest(
        observer_config_hash=config.config_hash(SOURCE_QUERY_TEMPLATES),
        query_registry_hash=config.query_registry_hash(SOURCE_QUERY_TEMPLATES),
        as_of_ts=NOW,
        history_start_trade_date=date(2026, 1, 1),
        history_end_trade_date=date(2026, 7, 13),
        program_count_by_style={"SHORT_REBOUND": 1},
        candidate_depth_by_program={"SHORT_REBOUND": 20},
        universe_size_p50=100,
        universe_size_p95=200,
        universe_size_max=300,
        horizons=(5, 10),
        projection_count=2,
        stage_projection_factor=5,
        revision_multiplier_p50=1.0,
        revision_multiplier_p95=1.2,
        revision_multiplier_max=2.0,
        retained_snapshot_count=2,
        concurrent_build_count=1,
        staging_copy_count=1,
        parquet_target_file_bytes=128 * 1024 * 1024,
        memory_budget_bytes=2 * 1024 * 1024 * 1024,
        worker_memory_overheads={"arrow_builder_bytes": 100, "hash_buffer_bytes": 100, "verifier_bytes": 100},
        store_available_bytes=1_000_000_000,
        orphan_reserve_bytes=1_024,
        concurrent_build_bytes=2_048,
        manifest_overhead_bytes_per_snapshot=512,
        parquet_measurement_snapshot_limit=10,
        parquet_measurement_file_limit=1_000,
    )


def _capacity_receipt(request: CapacityPlanningRequest):
    widths = {role: 4.0 for role in ("canonical_signals", "stage_candidates", "outcome_labels", "universe_outcomes", "source_revisions")}
    return build_capacity_receipt(
        request=request,
        measurements=CapacityMeasurements(
            database_observed_at=NOW,
            database_version="PostgreSQL test",
            trading_days=100,
            observed_partitions=500,
            source_role_count=5,
            relation_size_summary={"market.kline_daily_raw": {"total_bytes": 1000}},
            row_distribution_summary={"row_count_p95": 5000},
            measured_role_row_widths=widths,
            measured_role_parquet_bytes_per_row_p95={role: 2.0 for role in widths},
            parquet_measurement_provenance={"snapshot_set_hash": H},
            observed_partitions_by_role={"FEATURE_T": 100},
            changed_partition_ratio_by_tier={"p50": 0.1, "p95": 0.25, "max": 0.5},
            source_fetch_peak_bytes=1_000,
        ),
    )


def _registry() -> SourceRequirementRegistry:
    return SourceRequirementRegistry(
        templates=(
            SourceRequirementTemplate(
                package_id="pkg",
                manifest_sha256=H,
                alpha_mode="single_alpha",
                source_role="FEATURE_T",
                dataset_name="market.kline_daily_raw",
                query_template_id="phase1e-test",
                query_template_version="v1",
                query_template_hash="c" * 64,
                bound_parameters={"window": 20},
                partition_key={"trade_date": "2026-07-10"},
                revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
                availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
                business_min_date=date(2026, 6, 1),
                business_max_date=date(2026, 7, 10),
                enforced_cutoff_predicate_hash="d" * 64,
                consumer_scope_suffix="alpha",
            ),
        ),
    )


def _program_date(*, program_id: str = "program") -> Phase1EProgramDateRequest:
    return Phase1EProgramDateRequest(
        program_id=program_id,
        decision_trade_date=date(2026, 7, 10),
        historical_batch_receipt_ref="receipt",
        label_as_of_ts=datetime(2026, 7, 11, 16, 0, tzinfo=UTC),
    )


def _batch(program_dates: tuple[Phase1EProgramDateRequest, ...], registry: SourceRequirementRegistry, capacity: CapacityPlanningRequest, receipt) -> Phase1ERevalidationBatchRequest:
    return Phase1ERevalidationBatchRequest(
        program_dates=program_dates,
        phase0a_policy_hash="b" * 64,
        source_requirement_registry_hash=str(registry.registry_hash),
        query_registry_hash=capacity.query_registry_hash,
        calendar_hash="2" * 64,
        label_policy_bundle_hash="e" * 64,
        dataset_schema_fingerprint="advisory-phase1e-test",
        partition_policy_hash="6" * 64,
        store_backend_config_hash="7" * 64,
        capacity_request_ref=capacity.request_hash,
        capacity_receipt_ref=receipt.receipt_hash,
        compiler_version="phase1e-test-v1",
        serializer_version="phase1e-json-v1",
        compiler_source_hash="8" * 64,
        artifact_store_policy_hash="9" * 64,
    )


class _Provider:
    def __init__(self) -> None:
        policy = _policy()
        program_date = _program_date()
        program_context = HistoricalResearchProgramContext(
            program_id=program_date.program_id,
            binding_version_id="binding",
            binding_payload_hash="1" * 64,
            package_id="pkg",
            manifest_sha256=H,
            policy_hash="b" * 64,
            effective_runtime_config_hash="2" * 64,
        )
        historical_evidence = HistoricalSelectionEvidence(
            evidence_id="dse",
            evidence_hash="4" * 64,
            artifact_id="artifact",
            artifact_payload_hash="5" * 64,
            source_watermark_hash="3" * 64,
            candidate_outcome="VALID_NO_CANDIDATE",
            candidates=[],
        )
        run = HistoricalResearchProgramRun(
            program_run_id="run",
            program_id=program_date.program_id,
            decision_trade_date=program_date.decision_trade_date,
            research_scope=HISTORICAL_RESEARCH_SCOPE,
            status=HistoricalResearchRunStatus.COMPLETE,
            program_payload_sha256=_program_payload_hash(
                context=program_context,
                evidence=historical_evidence,
            ),
            binding_version_id="binding",
            binding_payload_hash="1" * 64,
            package_id="pkg",
            manifest_sha256=H,
            policy_hash="b" * 64,
            effective_runtime_config_hash="2" * 64,
            source_watermark_hash="3" * 64,
            evidence_id="dse",
            evidence_hash="4" * 64,
            artifact_id="artifact",
            artifact_payload_hash="5" * 64,
            candidate_outcome="VALID_NO_CANDIDATE",
        )
        batch = HistoricalResearchBatch(
            batch_id="batch",
            request_id=__import__("uuid").uuid4(),
            batch_key="batch-key",
            decision_trade_date=program_date.decision_trade_date,
            program_ids=[program_date.program_id],
            data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
            origin="MANUAL_HISTORICAL_RESEARCH",
            request_payload_sha256="6" * 64,
            research_scope=HISTORICAL_RESEARCH_SCOPE,
            execution_prohibited=True,
            status=HistoricalResearchRunStatus.COMPLETE,
        )
        receipt = HistoricalResearchBatchReceipt(
            receipt_id="receipt",
            batch_id=batch.batch_id,
            batch_key=batch.batch_key,
            status=HistoricalResearchRunStatus.COMPLETE,
            program_runs=[run],
            receipt_hash=canonical_json_sha256(_batch_receipt_payload(batch=batch, status=HistoricalResearchRunStatus.COMPLETE, program_runs=[run])),
        )
        manifest = ProjectedPackageManifest(
            package_id="pkg",
            manifest_sha256=H,
            alpha_mode=ProjectedAlphaMode.SINGLE_ALPHA,
            style_family="SHORT_REBOUND",
        )
        package = ProjectedPackage(
            package_id="pkg",
            manifest_sha256=H,
            alpha_mode=ProjectedAlphaMode.SINGLE_ALPHA,
            source_id="source",
            manifest=manifest,
        )
        binding = ProjectedBindingVersion(
            binding_version_id="binding",
            program_id="program",
            package_mode="single",
            package_ids=["pkg"],
            effective_from_trade_date=date(2026, 1, 1),
            effective_to_trade_date=None,
            activation_status="ACTIVE",
            binding_payload_hash="1" * 64,
        )
        evidence = ProjectedDailySelectionEvidence(
            evidence_id="dse",
            target_trade_date=date(2026, 7, 11),
            cutoff_date=program_date.decision_trade_date,
            package_id="pkg",
            manifest_sha256=H,
            runtime_profile_version_id="profile",
            runtime_profile_hash="2" * 64,
            source_type="live_qe_model_inference_v1",
            data_source="DB_HISTORICAL",
            candidate_count=0,
            artifact_hash="4" * 64,
            evidence_payload_json={},
        )
        artifact = ProjectedSelectionScoreArtifact(
            artifact_id="artifact",
            package_id="pkg",
            manifest_sha256=H,
            trade_date=program_date.decision_trade_date,
            data_source="DB_HISTORICAL",
            runtime_config_hash="2" * 64,
            scores_json=[],
            artifact_sha256="a" * 64,
            score_count=0,
            universe_count=1,
            top_score_symbol=None,
            status="SUCCEEDED",
            artifact_contract_version="selection_score_artifact_v2",
            artifact_payload_sha256="5" * 64,
            artifact_input_context_hash="6" * 64,
            source_revision_set_hash="7" * 64,
            asset_closure_hash="8" * 64,
        )

        class Reader:
            def get_program(self, _program_id):
                return ProjectedAdvisoryProgram(program_id="program", target_count=5, review_policy={})

            def list_binding_versions(self, _program_id):
                return [binding]

            def get(self, _package_id):
                return package

            def list_package_assets(self, _package_id, *, protected_only=False):
                return []

            def get_daily_selection_evidence(self, _evidence_id):
                return evidence

            def list(self, **_kwargs):
                return [artifact]

            def get_run(self, _run_id):
                raise AssertionError("no-candidate audit must not read a selection run")

        reader = Reader()
        self._evidence = Phase1EProgramDateEvidence(
            historical_batch=batch,
            historical_receipt=receipt,
            historical_program_run=run,
            dated_binding=binding,
            package=package,
            selection_evidence=evidence,
            selection_artifact=artifact,
            policy=policy,
            audit_readers=AuditReaders(advisory=reader, package=reader, evidence=reader, score_artifact=reader, selection_run=reader),
            postgres_now=NOW,
        )

    def resolve_program_date(self, **_kwargs):
        return self._evidence

    def list_source_events(self, **_kwargs):
        return ()


def test_program_date_identity_is_independent_of_batch_membership() -> None:
    first = _program_date()
    second = _program_date(program_id="program-two")
    registry = _registry()
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    one = _batch((first,), registry, capacity, receipt)
    two = _batch((first, second), registry, capacity, receipt)

    assert first.program_date_request_hash == one.program_dates[0].program_date_request_hash == two.program_dates[0].program_date_request_hash
    assert one.invocation_request_hash != two.invocation_request_hash


def test_phase1e_keeps_no_admission_scope_as_target_diagnostic(monkeypatch) -> None:
    registry = _registry()
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(registry),
        capacity_request=capacity,
        capacity_receipt=receipt,
    )
    monkeypatch.setattr(compiler, "_matching_scopes", lambda *_args, **_kwargs: [])
    plans, batch_receipt = compiler.compile_batch(
        request=_batch((_program_date(),), registry, capacity, receipt),
        provider=_Provider(),
    )

    assert len(plans) == 1
    assert plans[0].plan_unit_kind is PlanUnitKind.TARGET_DIAGNOSTIC
    assert plans[0].source_readiness is None
    assert batch_receipt.all_scope_workloads_covered is False
    assert batch_receipt.failed_input_scopes == ()

    projection = Phase1EExecutionPlanProjection.model_validate(
        plans[0].model_dump(mode="json")
    )
    assert projection.plan_unit_kind is Phase1EPlanUnitKind.TARGET_DIAGNOSTIC
    with pytest.raises(Exception) as diagnostic_error:
        Phase1GService._assert_executable_phase1e(projection)
    assert diagnostic_error.value.reason_code == REASON_TARGET_DIAGNOSTIC


def test_phase1e_reports_policy_registry_mismatch_without_compiling_a_plan() -> None:
    registry = _registry()
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(registry),
        capacity_request=capacity,
        capacity_receipt=receipt,
    )
    request = _batch((_program_date(),), registry, capacity, receipt).model_copy(
        update={"phase0a_policy_hash": "f" * 64}
    )

    plans, batch_receipt = compiler.compile_batch(request=request, provider=_Provider())

    assert plans == []
    assert [item.reason_code for item in batch_receipt.failed_input_scopes] == [REASON_POLICY_REGISTRY_HASH_MISMATCH]


def test_phase1e_rejects_program_payload_hash_that_does_not_match_lineage() -> None:
    provider = _Provider()
    evidence = provider._evidence
    bad_run = replace(evidence.historical_program_run, program_payload_sha256="0" * 64)
    bad_receipt = replace(
        evidence.historical_receipt,
        program_runs=[bad_run],
        receipt_hash=canonical_json_sha256(
            _batch_receipt_payload(
                batch=evidence.historical_batch,
                status=HistoricalResearchRunStatus.COMPLETE,
                program_runs=[bad_run],
            )
        ),
    )
    bad_evidence = replace(
        evidence,
        historical_program_run=bad_run,
        historical_receipt=bad_receipt,
    )

    with pytest.raises(Phase1EError) as error:
        Phase1EReadinessPlanCompiler._validate_historical_input(
            program_date=_program_date(),
            evidence=bad_evidence,
        )

    assert error.value.reason_code == REASON_HISTORICAL_RECEIPT_CONFLICT


def test_batch_receipt_uses_original_invocation_request_identity_when_scopes_fail(monkeypatch) -> None:
    registry = _registry()
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(registry),
        capacity_request=capacity,
        capacity_receipt=receipt,
    )
    first = _batch((_program_date(),), registry, capacity, receipt)
    second = _batch(
        (_program_date(), _program_date(program_id="program-two")),
        registry,
        capacity,
        receipt,
    )

    def fail_scope(**_kwargs):
        raise Phase1EError(REASON_POLICY_REGISTRY_HASH_MISMATCH, "unit failure")

    monkeypatch.setattr(compiler, "_compile_program_date", fail_scope)
    first_plans, first_receipt = compiler.compile_batch(request=first, provider=SimpleNamespace())
    second_plans, second_receipt = compiler.compile_batch(request=second, provider=SimpleNamespace())

    assert first_plans == second_plans == []
    assert first_receipt.batch_request_hash == first.invocation_request_hash
    assert second_receipt.batch_request_hash == second.invocation_request_hash
    assert first_receipt.batch_request_hash != second_receipt.batch_request_hash


def test_snapshot_close_error_is_reported_without_aborting_other_compile_results(monkeypatch) -> None:
    registry = _registry()
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    published: list[tuple[str, str]] = []

    class RecordingStore:
        def publish(self, *, kind, identity, payload, semantic_hash):
            _ = payload
            published.append((kind, semantic_hash))
            return {"kind": kind, "identity": identity}

    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(registry),
        capacity_request=capacity,
        capacity_receipt=receipt,
        artifact_store=RecordingStore(),
    )
    monkeypatch.setattr(compiler, "_matching_scopes", lambda *_args, **_kwargs: [])
    provider = _Provider()

    def fail_close() -> None:
        raise RuntimeError("close failed")

    provider.close_program_date = fail_close
    plans, batch_receipt = compiler.compile_batch(
        request=_batch((_program_date(),), registry, capacity, receipt),
        provider=provider,
    )

    assert plans == []
    assert [item.reason_code for item in batch_receipt.failed_input_scopes] == ["ADVISORY_PHASE1E_UNEXPECTED_ERROR"]
    assert [kind for kind, _semantic_hash in published] == ["audit", "batch"]
    assert "plan" not in [kind for kind, _semantic_hash in published]


def test_batch_rejects_capacity_reference_that_does_not_match_loaded_evidence() -> None:
    registry = _registry()
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(registry),
        capacity_request=capacity,
        capacity_receipt=receipt,
    )
    request = _batch((_program_date(),), registry, capacity, receipt)
    changed_refs = request.model_copy(
        update={"capacity_request_ref": "c" * 64, "capacity_receipt_ref": "d" * 64}
    )
    compiler._validate_batch_dependencies(request)
    with pytest.raises(Phase1EError) as error:
        compiler._validate_batch_dependencies(changed_refs)
    assert error.value.reason_code == REASON_CAPACITY_REFERENCE_MISMATCH


def test_label_as_of_cannot_precede_the_immutable_decision_cutoff() -> None:
    with pytest.raises(Phase1EError) as error:
        Phase1EReadinessPlanCompiler._validate_label_as_of_cutoff(
            program_date=_program_date(),
            decision_cutoff_ts=datetime(2026, 7, 12, 9, 0, tzinfo=UTC),
        )

    assert error.value.reason_code == "ADVISORY_PHASE1E_HISTORICAL_DATE_REQUIRED"


def test_template_contract_rejects_empty_or_placeholder_request() -> None:
    with pytest.raises(ValidationError):
        Phase1EPlannedOperation(
            operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
            operation_disposition=OperationDisposition.SEMANTIC_TEMPLATE,
            contract_schema_version="v1",
            request_template_payload={"placeholder": ""},
            request_template_hash=canonical_json_sha256({"placeholder": ""}),
        )


def test_template_contract_rejects_output_slot_without_provenance_metadata() -> None:
    payload = {"scope_context": {"program_id": "program"}}
    with pytest.raises(ValidationError):
        Phase1EPlannedOperation(
            operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
            operation_disposition=OperationDisposition.SEMANTIC_TEMPLATE,
            contract_schema_version="v1",
            request_template_payload=payload,
            request_template_hash=canonical_json_sha256(payload),
            required_output_slots=({"slot": "control_binding_event_hash"},),
        )


def test_template_output_slots_are_normalized_before_persistence() -> None:
    def slot(name: str) -> dict[str, str]:
        return {
            "slot": name,
            "source_type": "phase_output",
            "slot_schema_version": "advisory_phase1e_output_slot_v1",
            "producer_operation": "phase1g_observation_capture",
            "hash_validation": "typed_request",
        }

    payload = {"scope_context": {"program_id": "program"}}
    first = Phase1EPlannedOperation(
        operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
        operation_disposition=OperationDisposition.SEMANTIC_TEMPLATE,
        contract_schema_version="v1",
        request_template_payload=payload,
        request_template_hash=canonical_json_sha256(payload),
        required_output_slots=(slot("capture_batch_id"), slot("control_binding_event_hash")),
        unresolved_input_refs=(slot("capture_batch_id"), slot("control_binding_event_hash")),
    )
    second = Phase1EPlannedOperation(
        operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
        operation_disposition=OperationDisposition.SEMANTIC_TEMPLATE,
        contract_schema_version="v1",
        request_template_payload=payload,
        request_template_hash=canonical_json_sha256(payload),
        required_output_slots=(slot("control_binding_event_hash"), slot("capture_batch_id")),
        unresolved_input_refs=(slot("control_binding_event_hash"), slot("capture_batch_id")),
    )
    assert first.model_dump(mode="json") == second.model_dump(mode="json")


def test_batch_receipt_hash_rejects_mutation() -> None:
    receipt = Phase1EPlanBatchReceipt(
        batch_request_hash=H,
        counts_by_plan_unit_kind={},
        all_scope_workloads_covered=True,
        counts_by_handoff_readiness={},
        counts_by_source_readiness={},
        counts_by_capacity_status={},
    )
    with pytest.raises(ValidationError):
        Phase1EPlanBatchReceipt.model_validate({**receipt.model_dump(mode="python"), "all_scope_workloads_covered": False})
    with pytest.raises(ValidationError):
        Phase1EPlanBatchReceipt(
            batch_request_hash=H,
            sorted_scope_plan_request_hashes=("not-a-sha256",),
            counts_by_plan_unit_kind={},
            all_scope_workloads_covered=False,
            counts_by_handoff_readiness={},
            counts_by_source_readiness={},
            counts_by_capacity_status={},
        )


def test_content_addressed_models_normalize_before_hashing() -> None:
    rows = {role: 0 for role in ("canonical_signals", "stage_candidates", "outcome_labels", "universe_outcomes", "source_revisions")}
    workload = Phase1EWorkloadProjection(
        scope_plan_request_hash=H,
        style_family="SHORT_REBOUND",
        decision_trade_date=date(2026, 7, 10),
        candidate_depth=0,
        horizons=(10, 5, 10),
        projection_count=1,
        stage_projection_factor=1,
        universe_size_p50=0,
        universe_size_p95=0,
        universe_size_max=0,
        role_rows_p50=rows,
        role_rows_p95=rows,
        role_rows_max=rows,
    )
    assert workload.horizons == (5, 10)
    assert workload.workload_projection_hash == canonical_json_sha256(workload.canonical_payload())

    receipt = Phase1EPlanBatchReceipt(
        batch_request_hash=H,
        sorted_scope_plan_request_hashes=("b" * 64, "a" * 64, "b" * 64),
        sorted_scope_plan_hashes=("d" * 64, "c" * 64, "d" * 64),
        counts_by_plan_unit_kind={},
        all_scope_workloads_covered=False,
        counts_by_handoff_readiness={},
        counts_by_source_readiness={},
        counts_by_capacity_status={},
    )
    assert receipt.sorted_scope_plan_request_hashes == ("a" * 64, "b" * 64)
    assert receipt.sorted_scope_plan_hashes == ("c" * 64, "d" * 64)
    assert receipt.batch_receipt_hash == canonical_json_sha256(receipt.canonical_payload())


def test_native_multi_alpha_keeps_distinct_leg_windows_with_one_common_pit_identity() -> None:
    scope_core = {
        "admission_scope_id": "scope",
        "audit_target_id": "target",
        "target_scope_hash": "0" * 64,
        "phase0a_signal_context_hash": "1" * 64,
        "oos_interval_id": "interval",
        "oos_interval_hash": "2" * 64,
        "capability": "candidate_signal",
        "capability_hash": "3" * 64,
        "date_start": date(2026, 7, 10),
        "date_end": date(2026, 7, 10),
        "formal_oos_status": FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY,
        "signal_evidence_level": CandidateAuthorityStatus.RETROSPECTIVE,
        "evidence_scope": HandoffEvidenceScope.RETROSPECTIVE_RESEARCH_ONLY,
        "readiness": HandoffReadiness.PARTIAL,
        "blocking_reason_codes": [],
    }
    scope = HandoffAdmissionScope(
        **scope_core,
        admission_scope_hash=canonical_json_sha256(scope_core),
    )
    binding = Phase1EEvidenceBinding(
        historical_batch_id="batch",
        historical_batch_key="batch-key",
        historical_receipt_hash="4" * 64,
        historical_program_run_id="run",
        program_payload_sha256="5" * 64,
        binding_version_id="binding",
        binding_payload_hash="6" * 64,
        package_id="pkg",
        manifest_sha256=H,
        alpha_mode="multi_alpha",
        manifest_alpha_component_ids=("fast", "slow"),
        resolved_style_family="SHORT_REBOUND",
        style_assignment_policy_hash="7" * 64,
        selection_evidence_id="dse",
        selection_evidence_hash="8" * 64,
        selection_artifact_id="artifact",
        selection_artifact_payload_hash="9" * 64,
        source_watermark_hash="a" * 64,
        phase0a_audit_id="audit",
        phase0a_audit_manifest_hash="b" * 64,
        phase0a_request_hash="c" * 64,
        handoff_readiness_report_hash="d" * 64,
        admission_scope_id=scope.admission_scope_id,
        admission_scope_hash=scope.admission_scope_hash,
        target_scope_hash=scope.target_scope_hash,
        oos_interval_hash=scope.oos_interval_hash,
        evidence_scope=scope.evidence_scope.value,
        formal_oos_status=scope.formal_oos_status.value,
        signal_evidence_level=scope.signal_evidence_level.value,
    )
    templates = (
        _registry().templates[0].model_copy(
            update={
                "alpha_mode": "multi_alpha",
                "alpha_component_id": "fast",
                "consumer_scope_suffix": "fast",
                "business_min_date": date(2026, 7, 1),
                "bound_parameters": {"window": 5},
            },
        ),
        _registry().templates[0].model_copy(
            update={
                "alpha_mode": "multi_alpha",
                "alpha_component_id": "slow",
                "consumer_scope_suffix": "slow",
                "business_min_date": date(2026, 5, 1),
                "bound_parameters": {"window": 60},
            },
        ),
    )
    registry = SourceRequirementRegistry(templates=templates)
    capacity = _capacity_request()
    receipt = _capacity_receipt(capacity)
    requirements = RegistrySourceRequirementCompiler(registry).compile(
        binding=binding,
        scope=scope,
        request=_program_date(),
        batch_request=_batch((_program_date(),), registry, capacity, receipt),
        universe_policy_hash="e" * 64,
        requested_source_cutoff=datetime(2026, 7, 10, 15, 0, tzinfo=UTC),
    )

    assert {item.business_min_date for item in requirements.requirements} == {date(2026, 7, 1), date(2026, 5, 1)}
    assert {item.common_pit_identity_hash for item in requirements.requirements} == {requirements.common_pit_identity_hash}

    incomplete_registry = SourceRequirementRegistry(templates=(templates[0],))
    with pytest.raises(Phase1EError) as error:
        RegistrySourceRequirementCompiler(incomplete_registry).compile(
            binding=binding,
            scope=scope,
            request=_program_date(),
            batch_request=_batch((_program_date(),), incomplete_registry, capacity, receipt),
            universe_policy_hash="e" * 64,
            requested_source_cutoff=datetime(2026, 7, 10, 15, 0, tzinfo=UTC),
        )
    assert error.value.reason_code == "ADVISORY_PHASE1E_SOURCE_RESOLUTION_BLOCKED"


def test_capture_plan_is_complete_only_when_all_authoritative_fields_exist() -> None:
    capacity = _capacity_request()
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=_capacity_receipt(capacity),
    )
    scope_core = {
        "admission_scope_id": "scope",
        "audit_target_id": "target",
        "target_scope_hash": "0" * 64,
        "phase0a_signal_context_hash": "1" * 64,
        "oos_interval_id": "interval",
        "oos_interval_hash": "2" * 64,
        "capability": "candidate_signal",
        "capability_hash": "3" * 64,
        "date_start": date(2026, 7, 10),
        "date_end": date(2026, 7, 10),
        "formal_oos_status": FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY,
        "signal_evidence_level": CandidateAuthorityStatus.RETROSPECTIVE,
        "evidence_scope": HandoffEvidenceScope.RETROSPECTIVE_RESEARCH_ONLY,
        "readiness": HandoffReadiness.PARTIAL,
        "stable_signal_semantics_payload_v1": {
            "selection_runtime_semantics_hash": "4" * 64,
            "package_effective_config_hash": "5" * 64,
            "calendar_hash": "6" * 64,
        },
        "stable_signal_semantics_hash": "7" * 64,
        "decision_clock_hash": "8" * 64,
        "blocking_reason_codes": [],
    }
    scope = HandoffAdmissionScope(**scope_core, admission_scope_hash=canonical_json_sha256(scope_core))
    binding = Phase1EEvidenceBinding(
        historical_batch_id="batch",
        historical_batch_key="batch-key",
        historical_receipt_hash="9" * 64,
        historical_program_run_id="run",
        program_payload_sha256="a" * 64,
        binding_version_id="binding",
        binding_payload_hash="b" * 64,
        package_id="pkg",
        manifest_sha256=H,
        alpha_mode="single_alpha",
        resolved_style_family="SHORT_REBOUND",
        style_assignment_policy_hash="c" * 64,
        selection_evidence_id="dse",
        selection_evidence_hash="d" * 64,
        selection_artifact_id="artifact",
        selection_artifact_payload_hash="e" * 64,
        source_watermark_hash="f" * 64,
        phase0a_audit_id="audit",
        phase0a_audit_manifest_hash="0" * 64,
        phase0a_request_hash="1" * 64,
        handoff_readiness_report_hash="2" * 64,
        phase1_handoff_bundle_hash="3" * 64,
        admission_scope_set_hash="4" * 64,
        admission_scope_id=scope.admission_scope_id,
        admission_scope_hash=scope.admission_scope_hash,
        target_scope_hash=scope.target_scope_hash,
        oos_interval_hash=scope.oos_interval_hash,
        evidence_scope=scope.evidence_scope.value,
        formal_oos_status=scope.formal_oos_status.value,
        signal_evidence_level=scope.signal_evidence_level.value,
        stable_signal_semantics_hash=scope.stable_signal_semantics_hash,
        decision_clock_hash=scope.decision_clock_hash,
    )
    clock = DecisionClockEvidence(
        decision_as_of_trade_date=date(2026, 7, 10),
        selection_as_of_trade_date=date(2026, 7, 10),
        target_trade_date=date(2026, 7, 11),
        effective_cutoff_date=date(2026, 7, 10),
        decision_cutoff_ts=datetime(2026, 7, 10, 15, 0, tzinfo=UTC),
        calendar_version="cal-v1",
        calendar_hash="6" * 64,
    )
    authority = CandidateAuthorityReport(
        decision_date=date(2026, 7, 10),
        package_id="pkg",
        manifest_sha256=H,
        signal_context_hash="1" * 64,
        status=CandidateAuthorityStatus.RETROSPECTIVE,
        evidence_id="dse",
        selection_run_id="selection-run",
        selection_run_content_hash="5" * 64,
        selection_score_artifact_id="artifact",
        selection_score_artifact_sha256="6" * 64,
        daily_selection_evidence_hash="d" * 64,
        decision_clock=clock,
    )
    evidence = SimpleNamespace(
        selection_evidence=SimpleNamespace(
            evidence_payload_json={"symbol_normalization_policy_hash": "7" * 64},
            created_at=datetime(2026, 7, 10, 15, 1, tzinfo=UTC),
        ),
        historical_program_run=SimpleNamespace(candidate_outcome="CANDIDATES_PRESENT"),
    )
    target = SimpleNamespace(
        audit_target_id="target",
        candidate_authority=[authority],
        runtime_semantics=[SimpleNamespace(decision_date=date(2026, 7, 10), runtime_profile_version_id="profile", runtime_profile_hash="8" * 64)],
        hmm_vintages=[SimpleNamespace(decision_date=date(2026, 7, 10), status="NOT_APPLICABLE")],
        risk_policy_evidence=[SimpleNamespace(decision_date=date(2026, 7, 10), risk_policy_hash="9" * 64)],
    )
    source_result = SimpleNamespace(
        can_create_capture_plan=True,
        source_revision_set=SimpleNamespace(source_revision_set_id="srs", source_revision_set_hash="a" * 64),
    )
    plan, missing = compiler._build_capture_plan(
        binding=binding,
        scope=scope,
        program_date=_program_date(),
        evidence=evidence,
        audit_outcome=Phase1EAuditOutcome(
            request=SimpleNamespace(),
            receipt=SimpleNamespace(),
            handoff_report=SimpleNamespace(),
            handoff_bundle=SimpleNamespace(phase1_handoff_bundle_hash="3" * 64),
        ),
        target=target,
        source_result=source_result,
        universe_policy_hash="b" * 64,
    )

    assert missing == ()
    assert plan is not None
    assert plan.plan_hash
    assert plan.evidence_bundle_hash == binding.phase1_handoff_bundle_hash


def test_capacity_receipt_cannot_cover_a_larger_declared_workload() -> None:
    capacity = _capacity_request()
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=_capacity_receipt(capacity),
    )
    rows = {role: 0 for role in ("canonical_signals", "stage_candidates", "outcome_labels", "universe_outcomes", "source_revisions")}
    workload = Phase1EWorkloadProjection(
        scope_plan_request_hash=H,
        style_family="SHORT_REBOUND",
        decision_trade_date=date(2026, 7, 10),
        candidate_depth=21,
        horizons=(5, 10),
        projection_count=2,
        stage_projection_factor=5,
        universe_size_p50=100,
        universe_size_p95=200,
        universe_size_max=300,
        role_rows_p50=rows,
        role_rows_p95=rows,
        role_rows_max=rows,
    )

    covered, reasons = compiler._capacity_coverage(workload=workload, style_family="SHORT_REBOUND")

    assert covered is False
    assert "ADVISORY_PHASE1E_CAPACITY_WORKLOAD_NOT_COVERED" in reasons


def test_partial_capacity_allows_only_documented_bounded_staging_measurement_path() -> None:
    capacity = _capacity_request()
    base_receipt = _capacity_receipt(capacity)
    partial_receipt = base_receipt.model_copy(
        update={
            "status": CapacityStatus.PARTIAL,
            "missing_measurements": ("parquet_role_measurement:universe_outcomes",),
        }
    )
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=partial_receipt,
    )
    rows = {role: 0 for role in ("canonical_signals", "stage_candidates", "outcome_labels", "universe_outcomes", "source_revisions")}
    workload = Phase1EWorkloadProjection(
        scope_plan_request_hash=H,
        style_family="SHORT_REBOUND",
        decision_trade_date=date(2026, 7, 10),
        candidate_depth=0,
        horizons=(5, 10),
        projection_count=2,
        stage_projection_factor=5,
        universe_size_p50=100,
        universe_size_p95=200,
        universe_size_max=300,
        role_rows_p50=rows,
        role_rows_p95=rows,
        role_rows_max=rows,
    )

    covered, _ = compiler._capacity_coverage(workload=workload, style_family="SHORT_REBOUND")
    assert covered is False
    assert compiler._bounded_staging_capture_allowed(workload=workload, style_family="SHORT_REBOUND") is True

    blocked_receipt = partial_receipt.model_copy(update={"missing_measurements": ("source_fetch_peak_bytes",)})
    blocked_compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=blocked_receipt,
    )
    assert blocked_compiler._bounded_staging_capture_allowed(workload=workload, style_family="SHORT_REBOUND") is False


def test_capacity_coverage_rejects_role_byte_budget_below_scope_workload() -> None:
    capacity = _capacity_request()
    base_receipt = _capacity_receipt(capacity)
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=base_receipt,
    )
    workload = compiler._workload_projection(
        scope_plan_hash=H,
        binding=SimpleNamespace(resolved_style_family="SHORT_REBOUND"),
        scope=SimpleNamespace(),
        decision_trade_date=date(2026, 7, 10),
        candidate_depth=5,
        requirements=SimpleNamespace(requirements=(SimpleNamespace(source_role="FEATURE_T"),)),
        source_result=SimpleNamespace(source_revision_set=SimpleNamespace(members=("revision",))),
    )
    role_projection = deepcopy(base_receipt.role_projection_summary)
    role_projection["tiers"]["max"]["logical_uncompressed_bytes"]["canonical_signals"] = 0
    byte_short_receipt = base_receipt.model_copy(update={"role_projection_summary": role_projection})
    byte_short_compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=byte_short_receipt,
    )

    covered, reasons = byte_short_compiler._capacity_coverage(workload=workload, style_family="SHORT_REBOUND")

    assert covered is False
    assert "ADVISORY_PHASE1E_CAPACITY_WORKLOAD_NOT_COVERED" in reasons


def test_workload_source_roles_come_from_source_requirements_not_consumer_scope_ids() -> None:
    capacity = _capacity_request()
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=_capacity_receipt(capacity),
    )
    workload = compiler._workload_projection(
        scope_plan_hash=H,
        binding=SimpleNamespace(resolved_style_family="SHORT_REBOUND"),
        scope=SimpleNamespace(),
        decision_trade_date=date(2026, 7, 10),
        candidate_depth=5,
        requirements=SimpleNamespace(
            requirements=(
                SimpleNamespace(source_role="FEATURE_T"),
                SimpleNamespace(source_role="FEATURE_T"),
                SimpleNamespace(source_role="FUNDAMENTAL_T"),
            )
        ),
        source_result=SimpleNamespace(source_revision_set=SimpleNamespace(members=("a", "b"))),
    )

    assert workload.source_role_counts == {"FEATURE_T": 2, "FUNDAMENTAL_T": 1}
    assert workload.role_logical_bytes_p50["canonical_signals"] == 20
    assert workload.role_parquet_bytes_p50["canonical_signals"] == 10


def test_downstream_templates_keep_scope_semantics_and_explicit_future_slots() -> None:
    class Dump:
        def __init__(self, payload: dict) -> None:
            self.payload = payload

        def model_dump(self, **_kwargs):
            return self.payload

    capacity = _capacity_request()
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(_registry()),
        capacity_request=capacity,
        capacity_receipt=_capacity_receipt(capacity),
    )
    source_receipt = Dump({"schema_version": "source-receipt"})
    source_receipt.source_requirement_set_id = "srs"
    source_receipt.source_requirement_set_hash = "b" * 64
    source_receipt.source_resolution_receipt_hash = "c" * 64
    source_receipt.source_revision_set_id = "revision-set"
    source_receipt.source_revision_set_hash = "d" * 64
    operations = compiler._operations_for_resolution(
        binding=SimpleNamespace(evidence_binding_hash="d" * 64),
        scope=SimpleNamespace(),
        scope_context={
            "program_id": "program",
            "decision_trade_date": date(2026, 7, 10),
            "batch_contract": {"capacity_receipt_hash": "e" * 64, "label_policy_bundle_hash": "f" * 64},
        },
        requirements=Dump({"schema_version": "source-requirements"}),
        source_result=SimpleNamespace(can_create_capture_plan=True, receipt=source_receipt),
        capture_plan=SimpleNamespace(plan_hash="0" * 64, model_dump=lambda **_kwargs: {"plan_hash": "0" * 64}),
        capture_missing=(),
        workload_covered=True,
        bounded_staging_capture_allowed=False,
    )
    by_type = {operation.operation_type: operation for operation in operations}

    observation = by_type[PlannedOperationType.OBSERVATION_CAPTURE]
    label = by_type[PlannedOperationType.LABEL_CAPTURE]
    dataset = by_type[PlannedOperationType.DATASET_BUILD]
    store = by_type[PlannedOperationType.DURABLE_STORE_PUBLISH]

    assert observation.request_template_payload["scope_context"]["program_id"] == "program"
    assert observation.request_template_payload["capture_plan"]["plan_hash"] == "0" * 64
    assert label.request_template_payload["known_label_policy_bundle_hash"] == "f" * 64
    assert "planned_label_hash" in {slot["slot"] for slot in label.required_output_slots}
    assert "dataset_policy_compatibility_hash" in {slot["slot"] for slot in dataset.required_output_slots}
    assert "durable_store_publish_receipt" in {slot["slot"] for slot in store.required_output_slots}
    for operation in (observation, label, dataset, store):
        for slot in operation.required_output_slots:
            assert set(("slot", "source_type", "slot_schema_version", "producer_operation", "hash_validation")) <= set(slot)
    assert all(operation.resource_budget_ref == "e" * 64 for operation in operations)
