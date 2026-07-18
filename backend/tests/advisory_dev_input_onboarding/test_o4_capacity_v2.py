from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AlphaMode,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    ProgramCapacityStatus,
)
from backend.services.advisory_phase1.source_capacity import (
    AdvisoryPhase1CapacityProbe,
    CapacityStatus,
    Phase1ECapacityMeasurementsV2,
    Phase1ECapacityPolicyV1,
    Phase1EProgramCapacityWorkload,
    build_capacity_program_coverage_v1,
    build_capacity_request_v2,
    build_capacity_receipt_v2,
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


def _policy() -> Phase1ECapacityPolicyV1:
    return Phase1ECapacityPolicyV1(
        policy_id="phase1e_capacity",
        policy_version="1",
        retained_snapshot_count=3,
        concurrent_build_count=2,
        staging_copy_count=1,
        parquet_target_file_bytes=128 * 1024 * 1024,
        memory_budget_bytes=8 * 1024 * 1024 * 1024,
        worker_memory_overheads={
            "arrow_builder_bytes": 1024,
            "hash_buffer_bytes": 2048,
            "verifier_bytes": 4096,
        },
        orphan_reserve_bytes=1024,
        manifest_overhead_bytes_per_snapshot=512,
        parquet_measurement_snapshot_limit=5,
        parquet_measurement_file_limit=500,
    )


def test_capacity_v2_preserves_same_style_program_depths_without_aggregation() -> None:
    policy = _policy()
    workloads = (
        Phase1EProgramCapacityWorkload(
            program_id="program_b",
            decision_trade_date=date(2026, 7, 18),
            style_family="trend",
            package_id="pkg_b",
            manifest_sha256=SHA_B,
            alpha_mode=AlphaMode.MULTI,
            candidate_depth=20,
            input_universe_count=4300,
            horizons=(5, 10, 20),
            projection_count=3,
            stage_projection_factor=2,
            source_requirement_set_hash=SHA_B,
        ),
        Phase1EProgramCapacityWorkload(
            program_id="program_a",
            decision_trade_date=date(2026, 7, 18),
            style_family="trend",
            package_id="pkg_a",
            manifest_sha256=SHA_A,
            alpha_mode=AlphaMode.SINGLE,
            candidate_depth=5,
            input_universe_count=4100,
            horizons=(5, 10),
            projection_count=2,
            stage_projection_factor=1,
            source_requirement_set_hash=SHA_A,
        ),
    )
    request = build_capacity_request_v2(
        observer_config_ref=_ref("observer_config", SHA_A),
        query_registry_ref=_ref("source_query_registry", SHA_B),
        capacity_policy_ref=_ref("capacity_policy", str(policy.policy_hash)),
        capacity_policy=policy,
        as_of_ts=datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc),
        history_start_trade_date=date(2026, 1, 1),
        history_end_trade_date=date(2026, 7, 18),
        program_workloads=workloads,
        store_root_ref=_ref("store_backend_policy", SHA_C),
    )

    assert [item.program_id for item in request.program_workloads] == ["program_a", "program_b"]
    assert [item.candidate_depth for item in request.program_workloads] == [5, 20]
    assert len({item.program_workload_hash for item in request.program_workloads}) == 2

    request_ref = _ref(O4ArtifactKind.CAPACITY_REQUEST.value, str(request.request_hash))
    receipt = build_capacity_receipt_v2(
        request=request,
        request_ref=request_ref,
        measurements=Phase1ECapacityMeasurementsV2(
            target_database_identity_hash=SHA_C,
            database_observed_at=datetime(2026, 7, 18, 8, 1, tzinfo=timezone.utc),
            database_version="PostgreSQL 16",
            source_coverage_summary={"program_count": 2},
            relation_size_summary={},
            row_distribution_summary={},
            observed_revision_multiplier_p50=1.0,
            observed_revision_multiplier_p95=1.1,
            observed_revision_multiplier_max=1.2,
            role_projection_summary={"program_workload_hashes": [item.program_workload_hash for item in request.program_workloads]},
            parquet_measurement_summary={},
            db_transaction_budget_summary={},
            memory_budget_summary={},
            staging_store_summary={},
            durable_store_summary={},
            store_available_bytes=10_000,
            measured_program_workload_hashes=tuple(
                str(item.program_workload_hash) for item in request.program_workloads
            ),
            missing_measurements_by_program_workload_hash={},
        ),
    )

    assert receipt.status is CapacityStatus.MEASURED
    assert receipt.program_workload_set_hash == request.program_workload_set_hash

    receipt_ref = _ref(O4ArtifactKind.CAPACITY_RECEIPT.value, str(receipt.receipt_hash))
    workload = request.program_workloads[0]
    workload_ref = _ref(O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD.value, str(workload.program_workload_hash))
    coverage = build_capacity_program_coverage_v1(
        request=request,
        request_ref=request_ref,
        receipt=receipt,
        receipt_ref=receipt_ref,
        workload=workload,
        workload_ref=workload_ref,
    )

    assert coverage.program_id == "program_a"
    assert coverage.status is ProgramCapacityStatus.MEASURED
    assert coverage.coverage_hash is not None

    role_projection, memory, staging, durable, insufficient = AdvisoryPhase1CapacityProbe._project_v2_capacity(
        request=request,
        trading_days=120,
        revision_summary={"p50": 1.0, "p95": 1.1, "max": 1.2},
        logical_widths={role: 100.0 for role in ("canonical_signals", "stage_candidates", "outcome_labels", "universe_outcomes", "source_revisions")},
        parquet_widths={role: 50.0 for role in ("canonical_signals", "stage_candidates", "outcome_labels", "universe_outcomes", "source_revisions")},
        source_fetch_peak_bytes=10_000,
        store_available_bytes=10**12,
    )

    assert role_projection["program_workload_set_hash"] == request.program_workload_set_hash
    assert role_projection["tiers"]["max"]["role_rows"]["canonical_signals"] == 25
    assert role_projection["programs"][str(request.program_workloads[0].program_workload_hash)]["tiers"]["max"]["role_rows"]["canonical_signals"] == 5
    assert role_projection["programs"][str(request.program_workloads[1].program_workload_hash)]["tiers"]["max"]["role_rows"]["canonical_signals"] == 20
    assert memory["estimated_concurrent_peak_bytes"] is not None
    assert staging["max"]["required_free_bytes"] > 0
    assert durable["store_available_bytes"] == 10**12
    assert insufficient is False


def test_o4_source_capture_workload_has_no_phase1h_label_dimensions() -> None:
    workload = Phase1EProgramCapacityWorkload(
        program_id="program_source_only",
        decision_trade_date=date(2026, 7, 18),
        style_family="oversold_rebound",
        package_id="pkg_source_only",
        manifest_sha256=SHA_A,
        alpha_mode=AlphaMode.MULTI,
        candidate_depth=20,
        input_universe_count=4300,
        workload_scope="SOURCE_CAPTURE_ONLY",
        horizons=(),
        projection_count=0,
        stage_projection_factor=0,
        source_requirement_set_hash=SHA_B,
    )
    assert workload.horizons == ()
    assert workload.projection_count == 0


def test_source_capture_workload_rejects_placeholder_label_dimensions() -> None:
    with pytest.raises(ValueError, match="SOURCE_CAPTURE_ONLY"):
        Phase1EProgramCapacityWorkload(
            program_id="program_invalid",
            decision_trade_date=date(2026, 7, 18),
            style_family="trend",
            package_id="pkg_invalid",
            manifest_sha256=SHA_A,
            alpha_mode=AlphaMode.SINGLE,
            candidate_depth=5,
            input_universe_count=4100,
            workload_scope="SOURCE_CAPTURE_ONLY",
            horizons=(5,),
            projection_count=1,
            stage_projection_factor=1,
            source_requirement_set_hash=SHA_B,
        )


def test_capacity_v2_rejects_parquet_outside_explicit_advisory_root(tmp_path: Path) -> None:
    allowed_root = tmp_path / "advisory"
    allowed_root.mkdir()
    outside = tmp_path / "qe_backtest.parquet"
    outside.write_bytes(b"not-parquet")

    with pytest.raises(ValueError, match="outside the explicit Advisory store root"):
        AdvisoryPhase1CapacityProbe._parquet_metadata(
            uri=str(outside),
            expected_size_bytes=outside.stat().st_size,
            allowed_root=allowed_root,
        )
