from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    Phase1ECompileAggregateStatus,
    Phase1ECompileProgramResult,
    Phase1ECompileProgramStatus,
    Phase1ECompileReceipt,
    AlphaMode,
    ProgramCapacityStatus,
)
from backend.services.advisory_phase1.readiness_plan import Phase1EReadinessPlanCompiler
from backend.services.advisory_phase1.source_capacity import (
    Phase1ECapacityMeasurementsV2,
    Phase1ECapacityPolicyV1,
    Phase1EProgramCapacityWorkload,
    build_capacity_program_coverage_v1,
    build_capacity_receipt_v2,
    build_capacity_request_v2,
)


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


def _complete(program_id: str, digest: str) -> Phase1ECompileProgramResult:
    return Phase1ECompileProgramResult(
        program_id=program_id,
        decision_trade_date=date(2026, 7, 18),
        status=Phase1ECompileProgramStatus.COMPLETE,
        phase1e_batch_request_ref=_ref(O4ArtifactKind.PHASE1E_BATCH_REQUEST.value, digest),
        phase1e_batch_request_hash=digest,
        plan_refs=(_ref("phase1e_plan", digest),),
        batch_receipt_ref=_ref("phase1e_plan_batch_receipt", digest),
        batch_receipt_hash=digest,
    )


def test_compile_receipt_preserves_multiple_programs_as_independent_results() -> None:
    receipt = Phase1ECompileReceipt(
        input_bundle_ref=_ref(O4ArtifactKind.INPUT_BUNDLE.value, SHA_A),
        input_bundle_hash=SHA_A,
        program_results=(
            _complete("program_single", SHA_B),
            _complete("program_native_multi", SHA_C),
        ),
        aggregate_status=Phase1ECompileAggregateStatus.COMPLETE,
    )
    assert [item.program_id for item in receipt.program_results] == [
        "program_native_multi",
        "program_single",
    ]
    assert len({item.phase1e_batch_request_hash for item in receipt.program_results}) == 2


def test_compile_receipt_keeps_pending_program_without_fake_output_refs() -> None:
    pending = Phase1ECompileProgramResult(
        program_id="program_pending",
        decision_trade_date=date(2026, 7, 18),
        status=Phase1ECompileProgramStatus.PENDING,
        reason_codes=("ADVISORY_DEV_ONBOARDING_SOURCE_EVIDENCE_PENDING",),
    )
    receipt = Phase1ECompileReceipt(
        input_bundle_ref=_ref(O4ArtifactKind.INPUT_BUNDLE.value, SHA_A),
        input_bundle_hash=SHA_A,
        program_results=(_complete("program_complete", SHA_B), pending),
        aggregate_status=Phase1ECompileAggregateStatus.PARTIAL,
    )
    pending_result = next(item for item in receipt.program_results if item.program_id == "program_pending")
    assert pending_result.plan_refs == ()
    assert pending_result.phase1e_batch_request_ref is None


def test_all_blocked_or_failed_programs_are_not_reported_as_success() -> None:
    blocked = Phase1ECompileProgramResult(
        program_id="program_blocked",
        decision_trade_date=date(2026, 7, 18),
        status=Phase1ECompileProgramStatus.BLOCKED,
        reason_codes=("ADVISORY_DEV_ONBOARDING_SOURCE_MAPPING_CONFLICT",),
    )
    failed = Phase1ECompileProgramResult(
        program_id="program_failed",
        decision_trade_date=date(2026, 7, 18),
        status=Phase1ECompileProgramStatus.FAILED,
        reason_codes=("ADVISORY_PHASE1E_UNEXPECTED_ERROR",),
    )
    receipt = Phase1ECompileReceipt(
        input_bundle_ref=_ref(O4ArtifactKind.INPUT_BUNDLE.value, SHA_A),
        input_bundle_hash=SHA_A,
        program_results=(blocked, failed),
        aggregate_status=Phase1ECompileAggregateStatus.FAILED,
    )
    assert receipt.aggregate_status is Phase1ECompileAggregateStatus.FAILED


def test_complete_program_result_rejects_zero_plan_silent_success() -> None:
    with pytest.raises(ValidationError, match="requires batch request, plans"):
        Phase1ECompileProgramResult(
            program_id="program_invalid",
            decision_trade_date=date(2026, 7, 18),
            status=Phase1ECompileProgramStatus.COMPLETE,
            phase1e_batch_request_ref=_ref(O4ArtifactKind.PHASE1E_BATCH_REQUEST.value, SHA_A),
            phase1e_batch_request_hash=SHA_A,
            batch_receipt_ref=_ref("phase1e_plan_batch_receipt", SHA_B),
            batch_receipt_hash=SHA_B,
        )


def test_failed_program_result_preserves_real_partial_plan_and_batch_receipt_refs() -> None:
    failed = Phase1ECompileProgramResult(
        program_id="program_failed_with_evidence",
        decision_trade_date=date(2026, 7, 18),
        status=Phase1ECompileProgramStatus.FAILED,
        phase1e_batch_request_ref=_ref(O4ArtifactKind.PHASE1E_BATCH_REQUEST.value, SHA_A),
        phase1e_batch_request_hash=SHA_A,
        plan_refs=(_ref("phase1e_plan", SHA_B),),
        batch_receipt_ref=_ref("phase1e_batch_receipt", SHA_C),
        batch_receipt_hash=SHA_C,
        reason_codes=("ADVISORY_PHASE1E_SCOPE_FAILED",),
    )

    assert failed.plan_refs[0].semantic_hash == SHA_B
    assert failed.batch_receipt_ref is not None


def test_capacity_v2_partial_program_does_not_downgrade_measured_sibling() -> None:
    policy = Phase1ECapacityPolicyV1(
        policy_id="capacity-v2",
        policy_version="1",
        retained_snapshot_count=2,
        concurrent_build_count=1,
        staging_copy_count=1,
        parquet_target_file_bytes=1024,
        memory_budget_bytes=1_000_000,
        worker_memory_overheads={"arrow_builder_bytes": 1, "hash_buffer_bytes": 1, "verifier_bytes": 1},
        orphan_reserve_bytes=0,
        manifest_overhead_bytes_per_snapshot=0,
        parquet_measurement_snapshot_limit=1,
        parquet_measurement_file_limit=10,
    )
    workloads = tuple(
        Phase1EProgramCapacityWorkload(
            program_id=program_id,
            decision_trade_date=date(2026, 7, 18),
            style_family="trend",
            package_id=f"pkg_{program_id}",
            manifest_sha256=digest,
            alpha_mode=AlphaMode.SINGLE,
            candidate_depth=5,
            input_universe_count=4000,
            workload_scope="SOURCE_CAPTURE_ONLY",
            source_requirement_set_hash=digest,
        )
        for program_id, digest in (("program_a", SHA_A), ("program_b", SHA_B))
    )
    request = build_capacity_request_v2(
        observer_config_ref=_ref(O4ArtifactKind.OBSERVER_CONFIG.value, SHA_A),
        query_registry_ref=_ref(O4ArtifactKind.SOURCE_QUERY_REGISTRY.value, SHA_B),
        capacity_policy_ref=_ref(O4ArtifactKind.CAPACITY_POLICY.value, str(policy.policy_hash)),
        capacity_policy=policy,
        as_of_ts=datetime(2026, 7, 18, 8, tzinfo=timezone.utc),
        history_start_trade_date=date(2026, 7, 1),
        history_end_trade_date=date(2026, 7, 18),
        program_workloads=workloads,
        store_root_ref=_ref(O4ArtifactKind.STORE_BACKEND_POLICY.value, SHA_C),
    )
    request_ref = _ref(O4ArtifactKind.CAPACITY_REQUEST.value, str(request.request_hash))
    receipt = build_capacity_receipt_v2(
        request=request,
        request_ref=request_ref,
        measurements=Phase1ECapacityMeasurementsV2(
            target_database_identity_hash=SHA_C,
            database_observed_at=datetime(2026, 7, 18, 8, 1, tzinfo=timezone.utc),
            database_version="PostgreSQL 16",
            source_coverage_summary={},
            relation_size_summary={},
            row_distribution_summary={},
            observed_revision_multiplier_p50=1.0,
            observed_revision_multiplier_p95=1.0,
            observed_revision_multiplier_max=1.0,
            role_projection_summary={},
            parquet_measurement_summary={},
            db_transaction_budget_summary={},
            memory_budget_summary={},
            staging_store_summary={},
            durable_store_summary={},
            store_available_bytes=1_000_000,
            measured_program_workload_hashes=(str(workloads[0].program_workload_hash),),
            missing_measurements_by_program_workload_hash={
                str(workloads[1].program_workload_hash): ("parquet_metadata:outcome_labels",),
            },
            missing_measurements=("parquet_metadata:outcome_labels",),
        ),
    )
    receipt_ref = _ref(O4ArtifactKind.CAPACITY_RECEIPT.value, str(receipt.receipt_hash))
    coverage = build_capacity_program_coverage_v1(
        request=request,
        request_ref=request_ref,
        receipt=receipt,
        receipt_ref=receipt_ref,
        workload=workloads[0],
        workload_ref=_ref(O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, str(workloads[0].program_workload_hash)),
    )
    class UnusedSourceRequirementCompiler:
        registry_hash = SHA_A

        def compile(self, **kwargs):
            raise AssertionError("source requirement compilation is outside this capacity-only test")

    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=UnusedSourceRequirementCompiler(),
        capacity_request=request,
        capacity_receipt=receipt,
        capacity_program_coverage=coverage,
    )
    assert coverage.status is ProgramCapacityStatus.MEASURED
    assert compiler._program_capacity_status().value == "MEASURED"
    assert compiler._program_capacity_missing() == ()
    assert compiler._program_capacity_workload(program_id="program_a", decision_trade_date=date(2026, 7, 18)) == workloads[0]
