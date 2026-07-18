from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AlphaMode,
    HistoricalProgramStatus,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    Phase1EProgramDateInput,
    Phase1EProgramInputUnit,
    ProgramCapacityStatus,
    ProgramIdentityReadiness,
    ProgramPlanReadiness,
    ProgramSourceReadiness,
    REASON_SOURCE_MAPPING_CONFLICT,
    RealDevOnboardingError,
)
from backend.services.advisory_dev_input_onboarding.phase1e_input_builder import (
    build_phase1e_batch_request,
    build_program_input_unit,
)
from backend.services.advisory_phase1.readiness_plan import Phase1EProgramDateRequest


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


def _full_program(
    *,
    program_id: str,
    package_id: str,
    manifest_sha256: str,
    alpha_mode: AlphaMode,
    style_family: str,
) -> Phase1EProgramInputUnit:
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
        program_id=program_id,
        decision_trade_date=date(2026, 7, 18),
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=alpha_mode,
        style_family=style_family,
        identity_readiness=ProgramIdentityReadiness.COMPLETE,
        source_readiness=ProgramSourceReadiness.READY,
        capacity_status=ProgramCapacityStatus.MEASURED,
        plan_readiness=ProgramPlanReadiness.FULL_READY,
    )


def _request(program: Phase1EProgramInputUnit) -> Phase1EProgramDateRequest:
    return Phase1EProgramDateRequest(
        program_id=program.program_id,
        decision_trade_date=program.decision_trade_date,
        expected_package_id=program.package_id,
        expected_manifest_sha256=program.manifest_sha256,
        expected_alpha_mode=program.alpha_mode.value,
        expected_style_family=program.style_family,
        historical_batch_receipt_ref=f"historical_batch_receipt:{program.program_id}",
        label_as_of_ts=datetime(2026, 7, 19, tzinfo=timezone.utc),
    )


def _compile(
    *,
    programs: tuple[Phase1EProgramInputUnit, ...],
    requests: tuple[Phase1EProgramDateRequest, ...],
):
    return build_phase1e_batch_request(
        program_inputs=programs,
        program_date_requests=requests,
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


def test_phase1e_compile_preserves_single_and_native_multi_programs_independently() -> None:
    single = _full_program(
        program_id="program_single",
        package_id="pkg_single",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
    )
    multi = _full_program(
        program_id="program_multi",
        package_id="pkg_multi",
        manifest_sha256=SHA_B,
        alpha_mode=AlphaMode.MULTI,
        style_family="oversold_rebound",
    )

    batch = _compile(programs=(multi, single), requests=(_request(single), _request(multi)))

    assert batch is not None
    assert [(item.program_id, item.expected_alpha_mode) for item in batch.program_dates] == [
        ("program_multi", "multi_alpha"),
        ("program_single", "single_alpha"),
    ]


def test_phase1e_compile_all_failed_programs_remain_blocked_with_zero_plan() -> None:
    failed_units = []
    for program_id, package_id, alpha_mode in (
        ("program_failed_single", "pkg_failed_single", AlphaMode.SINGLE),
        ("program_failed_multi", "pkg_failed_multi", AlphaMode.MULTI),
    ):
        program_date = Phase1EProgramDateInput(
            program_id=program_id,
            decision_trade_date=date(2026, 7, 18),
            package_id=package_id,
            manifest_sha256=SHA_A if alpha_mode is AlphaMode.SINGLE else SHA_B,
            alpha_mode=alpha_mode,
            style_family="trend" if alpha_mode is AlphaMode.SINGLE else "oversold_rebound",
            historical_status=HistoricalProgramStatus.FAILED,
            historical_reason_codes=("ADVISORY_DEV_ONBOARDING_HISTORICAL_RUN_FAILED",),
        )
        failed_units.append(
            build_program_input_unit(
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
        )

    assert all(item.plan_readiness is ProgramPlanReadiness.BLOCKED for item in failed_units)
    assert all("ADVISORY_DEV_ONBOARDING_HISTORICAL_RUN_FAILED" in item.reason_codes for item in failed_units)
    assert _compile(programs=tuple(failed_units), requests=()) is None


def test_phase1e_compile_rejects_expected_identity_drift() -> None:
    program = _full_program(
        program_id="program_single",
        package_id="pkg_single",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.SINGLE,
        style_family="trend",
    )
    drifted = _request(program).model_copy(update={"expected_manifest_sha256": SHA_B})

    with pytest.raises(RealDevOnboardingError, match="expected identity differs") as error:
        _compile(programs=(program,), requests=(drifted,))
    assert error.value.reason_code == REASON_SOURCE_MAPPING_CONFLICT
