from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AggregateInputReadiness,
    AlphaMode,
    HistoricalProgramStatus,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    Phase1EProgramCompilerDependency,
    Phase1EProgramDateInput,
    Phase1EProgramInputUnit,
    Phase1ERealInputBuildRequest,
    ProgramCapacityStatus,
    ProgramIdentityReadiness,
    ProgramPlanReadiness,
    ProgramSourceReadiness,
)
from backend.services.advisory_dev_input_onboarding.phase1e_input_builder import (
    build_phase1e_batch_request,
    build_program_input_unit,
    build_real_input_bundle,
)
from backend.services.advisory_phase0a.handoff import audit_request_identity_payload
from backend.services.advisory_phase0a.models import (
    AuditDateRange,
    AuditReceipt,
    AuditRequest,
    AuditTarget,
    ExpectedAlphaMode,
    HandoffReadiness,
    HandoffReadinessReport,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.readiness_plan import Phase1EProgramDateRequest
from backend.services.advisory_phase1.source_capacity import Phase1ECapacityProgramCoverageV1


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _ref(kind: str, digest: str) -> AdvisoryImmutableArtifactRef:
    return AdvisoryImmutableArtifactRef(
        artifact_kind=kind,
        store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
        relative_path=f"external/{kind}/{digest}.json",
        semantic_hash=digest,
        file_sha256=digest,
    )


def _dependency() -> Phase1EProgramCompilerDependency:
    target = AuditTarget(
        audit_target_id="target_complete",
        program_id="program_complete",
        package_id="pkg_complete",
        manifest_sha256=SHA_A,
        expected_alpha_mode=ExpectedAlphaMode.SINGLE_ALPHA,
        decision_date_range=AuditDateRange(start_date=date(2026, 7, 18), end_date=date(2026, 7, 18)),
        decision_dates=[date(2026, 7, 18)],
        style_family="trend",
        requested_capabilities=[
            "candidate_authority",
            "hmm_vintage",
            "oos_classification",
            "runtime_semantics",
            "source_availability",
        ],
        audit_policy_version="phase0a-v1",
    )
    request = AuditRequest(
        audit_id="audit_complete",
        policy_registry_id="policy_registry",
        audit_policy_version="phase0a-v1",
        policy_registry_content_hash=SHA_A,
        targets=[target],
    )
    request_hash = canonical_json_sha256(audit_request_identity_payload(request))
    receipt = AuditReceipt(
        audit_id=request.audit_id,
        audit_policy_version=request.audit_policy_version,
        request_hash=request_hash,
        audit_manifest_hash=SHA_B,
        result_hash=SHA_C,
        results=[],
    )
    return Phase1EProgramCompilerDependency(
        program_id="program_complete",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_complete",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        historical_program_run_id="run_complete",
        historical_batch_receipt_ref=_ref("historical_batch_receipt", SHA_C),
        historical_batch_receipt_hash=SHA_C,
        phase0a_audit_request=request,
        phase0a_audit_receipt=receipt,
        handoff_readiness_report=HandoffReadinessReport(
            audit_id=request.audit_id,
            audit_manifest_hash=receipt.audit_manifest_hash,
            request_hash=request_hash,
            readiness=HandoffReadiness.READY,
            handoff_readiness_hash=SHA_A,
        ),
        phase0a_policy_registry_ref=_ref(O4ArtifactKind.PHASE0A_POLICY_REGISTRY.value, SHA_A),
        phase0a_policy_registry_hash=SHA_A,
        source_query_registry_ref=_ref(O4ArtifactKind.SOURCE_QUERY_REGISTRY.value, SHA_B),
        source_query_registry_hash=SHA_B,
        observer_config_ref=_ref(O4ArtifactKind.OBSERVER_CONFIG.value, SHA_C),
        observer_config_hash=SHA_C,
        calendar_identity_ref=_ref(O4ArtifactKind.CALENDAR_IDENTITY.value, SHA_A),
        calendar_identity_hash=SHA_A,
        dataset_schema_fingerprint="schema-v1",
        partition_policy_ref=_ref(O4ArtifactKind.PARTITION_POLICY.value, SHA_B),
        partition_policy_hash=SHA_B,
        store_backend_policy_ref=_ref(O4ArtifactKind.STORE_BACKEND_POLICY.value, SHA_C),
        store_backend_policy_hash=SHA_C,
        artifact_store_policy_ref=_ref(O4ArtifactKind.ARTIFACT_STORE_POLICY.value, O4_ARTIFACT_STORE_POLICY_HASH),
        artifact_store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
        compiler_version="phase1e-v1",
        serializer_version="canonical-json-v1",
        compiler_source_hash=SHA_A,
    )


def _build_request() -> Phase1ERealInputBuildRequest:
    dependency = _dependency()
    return Phase1ERealInputBuildRequest(
        historical_run_request_ref=_ref("historical_run_request", SHA_A),
        historical_run_request_hash=SHA_A,
        historical_run_receipt_ref=_ref("historical_run_receipt", SHA_B),
        historical_run_receipt_hash=SHA_B,
        target_database_identity_hash=SHA_C,
        target_package_asset_root_hash=SHA_A,
        program_dates=(
            Phase1EProgramDateInput(
                program_id="program_waiting",
                decision_trade_date=date(2026, 7, 18),
                package_id="pkg_waiting",
                manifest_sha256=SHA_B,
                alpha_mode=AlphaMode.MULTI,
                style_family="oversold_rebound",
                historical_status=HistoricalProgramStatus.WAITING_INPUT,
                historical_reason_codes=("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",),
            ),
            Phase1EProgramDateInput(
                program_id="program_complete",
                decision_trade_date=date(2026, 7, 18),
                package_id="pkg_complete",
                manifest_sha256=SHA_A,
                alpha_mode=AlphaMode.SINGLE,
                style_family="trend",
                historical_status=HistoricalProgramStatus.COMPLETE,
                historical_program_run_id="run_complete",
                historical_batch_receipt_ref=_ref("historical_batch_receipt", SHA_C),
                historical_batch_receipt_hash=SHA_C,
                compiler_dependency_ref=_ref(
                    O4ArtifactKind.PROGRAM_COMPILER_DEPENDENCY.value,
                    str(dependency.dependency_hash),
                ),
                compiler_dependency_hash=str(dependency.dependency_hash),
            ),
        ),
        source_mapping_registry_ref=_ref(O4ArtifactKind.SOURCE_MAPPING_REGISTRY.value, SHA_B),
        source_mapping_registry_hash=SHA_B,
        capacity_policy_ref=_ref(O4ArtifactKind.CAPACITY_POLICY.value, SHA_C),
        capacity_policy_hash=SHA_C,
        code_release_id="c5b00efd",
        code_release_hash=SHA_A,
    )


def _full_program() -> Phase1EProgramInputUnit:
    dependency = _dependency()
    pairs = {
        "compiler_dependency": (O4ArtifactKind.PROGRAM_COMPILER_DEPENDENCY.value, str(dependency.dependency_hash)),
        "source_requirement_set": (O4ArtifactKind.SOURCE_REQUIREMENT_SET.value, SHA_B),
        "source_resolution_receipt": ("source_resolution_receipt", SHA_C),
        "capacity_program_workload": (O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, SHA_A),
        "capacity_coverage": (O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE.value, SHA_B),
        "phase1e_program_date_request": (O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST.value, SHA_C),
    }
    kwargs: dict[str, object] = {}
    for name, (kind, digest) in pairs.items():
        kwargs[f"{name}_ref"] = _ref(kind, digest)
        kwargs[f"{name}_hash"] = digest
    return Phase1EProgramInputUnit(
        **kwargs,
        program_id="program_complete",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_complete",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        identity_readiness=ProgramIdentityReadiness.COMPLETE,
        source_readiness=ProgramSourceReadiness.READY,
        capacity_status=ProgramCapacityStatus.MEASURED,
        plan_readiness=ProgramPlanReadiness.FULL_READY,
    )


def test_build_request_preserves_complete_and_pending_program_truth() -> None:
    request = _build_request()
    assert [item.program_id for item in request.program_dates] == ["program_complete", "program_waiting"]
    assert request.program_dates[0].compiler_dependency_ref is not None
    assert request.program_dates[1].compiler_dependency_ref is None


def test_complete_historical_program_can_record_blocked_o4_identity_without_status_drift() -> None:
    program_date = Phase1EProgramDateInput(
        program_id="program_complete",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_complete",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        historical_status=HistoricalProgramStatus.COMPLETE,
        historical_program_run_id="run_complete",
        historical_batch_receipt_ref=_ref("historical_batch_receipt", SHA_C),
        historical_batch_receipt_hash=SHA_C,
    )

    unit = build_program_input_unit(
        program_date=program_date,
        compiler_dependency_ref=None,
        compiler_dependency_hash=None,
        source_requirement_set_ref=None,
        source_requirement_set_hash=None,
        source_resolution_receipt_ref=None,
        source_resolution_receipt_hash=None,
        source_readiness=ProgramSourceReadiness.NOT_EVALUATED,
        capacity_program_workload_ref=None,
        capacity_program_workload_hash=None,
        capacity_coverage_ref=None,
        capacity_coverage=None,
        phase1e_program_date_request_ref=None,
        phase1e_program_date_request_hash=None,
        identity_blocked=True,
        reason_codes=("ADVISORY_PHASE1E_PROGRAM_IDENTITY_FAILED",),
    )

    assert program_date.historical_status is HistoricalProgramStatus.COMPLETE
    assert unit.identity_readiness is ProgramIdentityReadiness.BLOCKED
    assert unit.plan_readiness is ProgramPlanReadiness.BLOCKED


def test_mixed_bundle_keeps_full_ready_program_independent_from_pending_program() -> None:
    request = _build_request()
    pending = Phase1EProgramInputUnit(
        program_id="program_waiting",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_waiting",
        manifest_sha256=SHA_B,
        alpha_mode=AlphaMode.MULTI,
        style_family="oversold_rebound",
        identity_readiness=ProgramIdentityReadiness.PENDING,
        source_readiness=ProgramSourceReadiness.NOT_EVALUATED,
        capacity_status=ProgramCapacityStatus.NOT_MEASURED,
        plan_readiness=ProgramPlanReadiness.IDENTITY_PENDING,
        missing_slots=("compiler_dependency",),
        reason_codes=("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",),
    )
    bundle = build_real_input_bundle(
        build_request_ref=_ref(O4ArtifactKind.REAL_INPUT_BUILD_REQUEST.value, str(request.build_request_hash)),
        build_request_hash=str(request.build_request_hash),
        target_database_identity_hash=request.target_database_identity_hash,
        capacity_policy_ref=request.capacity_policy_ref,
        capacity_policy_hash=request.capacity_policy_hash,
        source_mapping_registry_ref=request.source_mapping_registry_ref,
        source_mapping_registry_hash=request.source_mapping_registry_hash,
        source_requirement_registry_ref=_ref(O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY.value, SHA_A),
        source_requirement_registry_hash=SHA_A,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_request_hash=SHA_B,
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        capacity_receipt_hash=SHA_C,
        program_inputs=(pending, _full_program()),
    )
    assert bundle.aggregate_readiness is AggregateInputReadiness.MIXED


def test_program_builder_derives_full_ready_from_exact_dependency_and_capacity() -> None:
    program_date = _build_request().program_dates[0]
    coverage = Phase1ECapacityProgramCoverageV1(
        program_id=program_date.program_id,
        decision_trade_date=program_date.decision_trade_date,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_A),
        capacity_request_hash=SHA_A,
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_B),
        capacity_receipt_hash=SHA_B,
        program_workload_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, SHA_C),
        program_workload_hash=SHA_C,
        status=ProgramCapacityStatus.MEASURED,
    )
    unit = build_program_input_unit(
        program_date=program_date,
        compiler_dependency_ref=program_date.compiler_dependency_ref,
        compiler_dependency_hash=program_date.compiler_dependency_hash,
        source_requirement_set_ref=_ref(O4ArtifactKind.SOURCE_REQUIREMENT_SET.value, SHA_B),
        source_requirement_set_hash=SHA_B,
        source_resolution_receipt_ref=_ref("source_resolution_receipt", SHA_C),
        source_resolution_receipt_hash=SHA_C,
        source_readiness=ProgramSourceReadiness.READY,
        capacity_program_workload_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, SHA_C),
        capacity_program_workload_hash=SHA_C,
        capacity_coverage_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE.value, str(coverage.coverage_hash)),
        capacity_coverage=coverage,
        phase1e_program_date_request_ref=_ref(O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST.value, SHA_A),
        phase1e_program_date_request_hash=SHA_A,
    )
    assert unit.plan_readiness is ProgramPlanReadiness.FULL_READY
    assert unit.missing_slots == ()


def test_phase1e_batch_builder_is_single_program_and_defers_label_policy() -> None:
    full = _full_program()
    dependency = _dependency()
    request = Phase1EProgramDateRequest(
        program_id=full.program_id,
        decision_trade_date=full.decision_trade_date,
        expected_package_id=full.package_id,
        expected_manifest_sha256=full.manifest_sha256,
        expected_alpha_mode=full.alpha_mode.value,
        expected_style_family=full.style_family,
        historical_batch_receipt_ref="historical_batch_receipt",
        label_as_of_ts=datetime(2026, 7, 19, tzinfo=timezone.utc),
    )
    coverage = Phase1ECapacityProgramCoverageV1(
        program_id=full.program_id,
        decision_trade_date=full.decision_trade_date,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_request_hash=SHA_B,
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        capacity_receipt_hash=SHA_C,
        program_workload_ref=full.capacity_program_workload_ref,
        program_workload_hash=str(full.capacity_program_workload_hash),
        status=ProgramCapacityStatus.MEASURED,
    )
    batch = build_phase1e_batch_request(
        program_input=full,
        program_date_request=request,
        compiler_dependency=dependency,
        source_requirement_registry_hash=SHA_B,
        capacity_request_ref=coverage.capacity_request_ref,
        capacity_receipt_ref=coverage.capacity_receipt_ref,
        capacity_coverage=coverage,
    )
    assert batch is not None
    assert len(batch.program_dates) == 1
    assert batch.label_policy_bundle_hash is None
    assert batch.capacity_request_ref == SHA_B
    assert batch.capacity_program_workload_hash == full.capacity_program_workload_hash


def test_pending_program_does_not_generate_a_phase1e_batch() -> None:
    pending = Phase1EProgramInputUnit(
        program_id="program_waiting",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_waiting",
        manifest_sha256=SHA_B,
        alpha_mode=AlphaMode.MULTI,
        style_family="oversold_rebound",
        identity_readiness=ProgramIdentityReadiness.PENDING,
        source_readiness=ProgramSourceReadiness.NOT_EVALUATED,
        capacity_status=ProgramCapacityStatus.NOT_MEASURED,
        plan_readiness=ProgramPlanReadiness.IDENTITY_PENDING,
        missing_slots=("compiler_dependency",),
        reason_codes=("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",),
    )
    request = Phase1EProgramDateRequest(
        program_id=pending.program_id,
        decision_trade_date=pending.decision_trade_date,
        expected_package_id=pending.package_id,
        expected_manifest_sha256=pending.manifest_sha256,
        expected_alpha_mode=pending.alpha_mode.value,
        expected_style_family=pending.style_family,
        historical_batch_receipt_ref="pending",
        label_as_of_ts=datetime(2026, 7, 19, tzinfo=timezone.utc),
    )
    coverage = Phase1ECapacityProgramCoverageV1(
        program_id=pending.program_id,
        decision_trade_date=pending.decision_trade_date,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_request_hash=SHA_B,
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        capacity_receipt_hash=SHA_C,
        program_workload_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, SHA_A),
        program_workload_hash=SHA_A,
        status=ProgramCapacityStatus.NOT_MEASURED,
        missing_measurements=("source_resolution_pending",),
    )
    assert build_phase1e_batch_request(
        program_input=pending,
        program_date_request=request,
        compiler_dependency=_dependency(),
        source_requirement_registry_hash=SHA_B,
        capacity_request_ref=coverage.capacity_request_ref,
        capacity_receipt_ref=coverage.capacity_receipt_ref,
        capacity_coverage=coverage,
    ) is None


def test_program_readiness_cannot_be_declared_full_with_missing_evidence() -> None:
    with pytest.raises(ValidationError, match="plan_readiness does not match"):
        Phase1EProgramInputUnit(
            program_id="program_pending",
            decision_trade_date=date(2026, 7, 18),
            package_id="pkg_pending",
            manifest_sha256=SHA_A,
            alpha_mode=AlphaMode.SINGLE,
            style_family="trend",
            identity_readiness=ProgramIdentityReadiness.PENDING,
            source_readiness=ProgramSourceReadiness.NOT_EVALUATED,
            capacity_status=ProgramCapacityStatus.NOT_MEASURED,
            plan_readiness=ProgramPlanReadiness.FULL_READY,
            missing_slots=("compiler_dependency",),
        )
