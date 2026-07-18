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


def _build_request() -> Phase1ERealInputBuildRequest:
    bindings = {
        "historical_run_request": SHA_A,
        "historical_run_receipt": SHA_B,
        "phase0a_policy_registry": SHA_A,
        "source_mapping_registry": SHA_B,
        "source_query_registry": SHA_C,
        "calendar_registry": SHA_A,
        "label_policy_bundle": SHA_B,
        "partition_policy": SHA_C,
        "store_backend_policy": SHA_A,
        "capacity_policy": SHA_B,
        "phase1e_artifact_store_policy": SHA_C,
    }
    kwargs: dict[str, object] = {}
    for name, digest in bindings.items():
        kwargs[f"{name}_ref"] = _ref(name, digest)
        kwargs[f"{name}_hash"] = digest
    return Phase1ERealInputBuildRequest(
        **kwargs,
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
            ),
        ),
        code_release_id="c5b00efd",
        code_release_hash=SHA_C,
    )


def _full_program() -> Phase1EProgramInputUnit:
    pairs = {
        "historical_program_run": ("historical_program_run", SHA_A),
        "phase0a_audit": ("phase0a_audit", SHA_B),
        "handoff_readiness": ("handoff_readiness", SHA_C),
        "handoff_bundle": ("handoff_bundle", SHA_A),
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

    assert request.build_request_hash is not None
    assert [item.program_id for item in request.program_dates] == ["program_complete", "program_waiting"]
    assert request.program_dates[0].historical_status is HistoricalProgramStatus.COMPLETE
    assert request.program_dates[1].historical_status is HistoricalProgramStatus.WAITING_INPUT


def test_program_readiness_is_derived_and_cannot_be_declared_full_with_missing_evidence() -> None:
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
            missing_slots=("historical_program_run",),
        )


def test_mixed_bundle_keeps_full_ready_program_independent_from_pending_program() -> None:
    request = _build_request()
    full = _full_program()
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
        missing_slots=("historical_program_run",),
        reason_codes=("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",),
    )
    common_refs = {
        "phase0a_policy_registry_ref": request.phase0a_policy_registry_ref,
        "phase0a_policy_registry_hash": request.phase0a_policy_registry_hash,
        "source_query_registry_ref": request.source_query_registry_ref,
        "source_query_registry_hash": request.source_query_registry_hash,
        "calendar_registry_ref": request.calendar_registry_ref,
        "calendar_registry_hash": request.calendar_registry_hash,
        "label_policy_bundle_ref": request.label_policy_bundle_ref,
        "label_policy_bundle_hash": request.label_policy_bundle_hash,
        "partition_policy_ref": request.partition_policy_ref,
        "partition_policy_hash": request.partition_policy_hash,
        "store_backend_policy_ref": request.store_backend_policy_ref,
        "store_backend_policy_hash": request.store_backend_policy_hash,
        "capacity_policy_ref": request.capacity_policy_ref,
        "capacity_policy_hash": request.capacity_policy_hash,
        "phase1e_artifact_store_policy_ref": request.phase1e_artifact_store_policy_ref,
        "phase1e_artifact_store_policy_hash": request.phase1e_artifact_store_policy_hash,
        "source_mapping_registry_ref": request.source_mapping_registry_ref,
        "source_mapping_registry_hash": request.source_mapping_registry_hash,
    }
    bundle = build_real_input_bundle(
        build_request_ref=_ref(O4ArtifactKind.REAL_INPUT_BUILD_REQUEST.value, str(request.build_request_hash)),
        build_request_hash=str(request.build_request_hash),
        target_database_identity_hash=request.target_database_identity_hash,
        **common_refs,
        source_requirement_registry_ref=_ref(O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY.value, SHA_A),
        source_requirement_registry_hash=SHA_A,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_request_hash=SHA_B,
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        capacity_receipt_hash=SHA_C,
        phase1e_revalidation_batch_request_ref=_ref(O4ArtifactKind.PHASE1E_BATCH_REQUEST.value, SHA_A),
        phase1e_revalidation_batch_request_hash=SHA_A,
        program_inputs=(pending, full),
    )

    assert bundle.aggregate_readiness is AggregateInputReadiness.MIXED
    assert {item.program_id: item.plan_readiness for item in bundle.program_inputs} == {
        "program_complete": ProgramPlanReadiness.FULL_READY,
        "program_waiting": ProgramPlanReadiness.IDENTITY_PENDING,
    }


def test_program_builder_derives_full_ready_only_from_complete_exact_evidence() -> None:
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
        historical_program_run_ref=_ref("historical_program_run", SHA_A),
        historical_program_run_hash=SHA_A,
        phase0a_audit_ref=_ref("phase0a_audit", SHA_B),
        phase0a_audit_hash=SHA_B,
        handoff_readiness_ref=_ref("handoff_readiness", SHA_C),
        handoff_readiness_hash=SHA_C,
        handoff_bundle_ref=_ref("handoff_bundle", SHA_A),
        handoff_bundle_hash=SHA_A,
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

    assert unit.identity_readiness is ProgramIdentityReadiness.COMPLETE
    assert unit.source_readiness is ProgramSourceReadiness.READY
    assert unit.capacity_status is ProgramCapacityStatus.MEASURED
    assert unit.plan_readiness is ProgramPlanReadiness.FULL_READY
    assert unit.missing_slots == ()


def test_program_builder_keeps_waiting_historical_program_pending_without_fake_refs() -> None:
    program_date = _build_request().program_dates[1]
    unit = build_program_input_unit(
        program_date=program_date,
        historical_program_run_ref=None,
        historical_program_run_hash=None,
        phase0a_audit_ref=None,
        phase0a_audit_hash=None,
        handoff_readiness_ref=None,
        handoff_readiness_hash=None,
        handoff_bundle_ref=None,
        handoff_bundle_hash=None,
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
    )

    assert unit.plan_readiness is ProgramPlanReadiness.IDENTITY_PENDING
    assert "historical_program_run" in unit.missing_slots
    assert unit.reason_codes == ("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",)


def test_phase1e_batch_builder_compiles_only_the_exact_full_ready_program_set() -> None:
    full = _full_program()
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
        missing_slots=("historical_program_run",),
        reason_codes=("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",),
    )
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
    batch = build_phase1e_batch_request(
        program_inputs=(pending, full),
        program_date_requests=(request,),
        phase0a_policy_hash=SHA_A,
        source_requirement_registry_hash=SHA_B,
        query_registry_hash=SHA_C,
        calendar_hash=SHA_A,
        label_policy_bundle_hash=SHA_B,
        dataset_schema_fingerprint="schema-v1",
        partition_policy_hash=SHA_C,
        store_backend_config_hash=SHA_A,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        compiler_version="phase1e-v1",
        serializer_version="canonical-json-v1",
        compiler_source_hash=SHA_A,
        artifact_store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
    )

    assert batch is not None
    assert [item.program_id for item in batch.program_dates] == ["program_complete"]


def test_phase1e_batch_builder_returns_none_for_zero_full_ready_programs() -> None:
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
        missing_slots=("historical_program_run",),
        reason_codes=("ADVISORY_DEV_ONBOARDING_INPUT_PENDING",),
    )

    assert build_phase1e_batch_request(
        program_inputs=(pending,),
        program_date_requests=(),
        phase0a_policy_hash=SHA_A,
        source_requirement_registry_hash=SHA_B,
        query_registry_hash=SHA_C,
        calendar_hash=SHA_A,
        label_policy_bundle_hash=SHA_B,
        dataset_schema_fingerprint="schema-v1",
        partition_policy_hash=SHA_C,
        store_backend_config_hash=SHA_A,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        compiler_version="phase1e-v1",
        serializer_version="canonical-json-v1",
        compiler_source_hash=SHA_A,
        artifact_store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
    ) is None


def test_all_capacity_partial_programs_are_not_misreported_as_all_pending() -> None:
    partial = Phase1EProgramInputUnit(
        program_id="program_partial",
        decision_trade_date=date(2026, 7, 18),
        package_id="pkg_partial",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
        historical_program_run_ref=_ref("historical_program_run", SHA_A),
        historical_program_run_hash=SHA_A,
        phase0a_audit_ref=_ref("phase0a_audit", SHA_B),
        phase0a_audit_hash=SHA_B,
        handoff_readiness_ref=_ref("handoff_readiness", SHA_C),
        handoff_readiness_hash=SHA_C,
        handoff_bundle_ref=_ref("handoff_bundle", SHA_A),
        handoff_bundle_hash=SHA_A,
        source_requirement_set_ref=_ref(O4ArtifactKind.SOURCE_REQUIREMENT_SET.value, SHA_B),
        source_requirement_set_hash=SHA_B,
        source_resolution_receipt_ref=_ref("source_resolution_receipt", SHA_C),
        source_resolution_receipt_hash=SHA_C,
        capacity_program_workload_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, SHA_A),
        capacity_program_workload_hash=SHA_A,
        capacity_coverage_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE.value, SHA_B),
        capacity_coverage_hash=SHA_B,
        identity_readiness=ProgramIdentityReadiness.COMPLETE,
        source_readiness=ProgramSourceReadiness.READY,
        capacity_status=ProgramCapacityStatus.PARTIAL,
        plan_readiness=ProgramPlanReadiness.SOURCE_READY_CAPACITY_PARTIAL,
        missing_slots=("capacity_measurement:sealed_snapshot_measurement",),
    )
    request = _build_request()

    bundle = build_real_input_bundle(
        build_request_ref=_ref(O4ArtifactKind.REAL_INPUT_BUILD_REQUEST.value, str(request.build_request_hash)),
        build_request_hash=str(request.build_request_hash),
        target_database_identity_hash=request.target_database_identity_hash,
        phase0a_policy_registry_ref=request.phase0a_policy_registry_ref,
        phase0a_policy_registry_hash=request.phase0a_policy_registry_hash,
        source_query_registry_ref=request.source_query_registry_ref,
        source_query_registry_hash=request.source_query_registry_hash,
        calendar_registry_ref=request.calendar_registry_ref,
        calendar_registry_hash=request.calendar_registry_hash,
        label_policy_bundle_ref=request.label_policy_bundle_ref,
        label_policy_bundle_hash=request.label_policy_bundle_hash,
        partition_policy_ref=request.partition_policy_ref,
        partition_policy_hash=request.partition_policy_hash,
        store_backend_policy_ref=request.store_backend_policy_ref,
        store_backend_policy_hash=request.store_backend_policy_hash,
        capacity_policy_ref=request.capacity_policy_ref,
        capacity_policy_hash=request.capacity_policy_hash,
        phase1e_artifact_store_policy_ref=request.phase1e_artifact_store_policy_ref,
        phase1e_artifact_store_policy_hash=request.phase1e_artifact_store_policy_hash,
        source_mapping_registry_ref=request.source_mapping_registry_ref,
        source_mapping_registry_hash=request.source_mapping_registry_hash,
        source_requirement_registry_ref=_ref(O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY.value, SHA_A),
        source_requirement_registry_hash=SHA_A,
        capacity_request_ref=_ref(O4ArtifactKind.CAPACITY_REQUEST.value, SHA_B),
        capacity_request_hash=SHA_B,
        capacity_receipt_ref=_ref(O4ArtifactKind.CAPACITY_RECEIPT.value, SHA_C),
        capacity_receipt_hash=SHA_C,
        phase1e_revalidation_batch_request_ref=None,
        phase1e_revalidation_batch_request_hash=None,
        program_inputs=(partial,),
    )

    assert bundle.aggregate_readiness is AggregateInputReadiness.MIXED
