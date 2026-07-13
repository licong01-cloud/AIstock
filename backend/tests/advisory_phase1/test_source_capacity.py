from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase1.source_capacity import (
    CapacityMeasurements,
    CapacityPlanningRequest,
    CapacityStatus,
    REASON_CAPACITY_BUDGET_INSUFFICIENT,
    build_capacity_receipt,
)
from backend.services.advisory_phase1.source_observer import SOURCE_QUERY_TEMPLATES, default_source_observer_config


NOW = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)


def _request(*, store_available_bytes: int = 1_000_000) -> CapacityPlanningRequest:
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
        store_available_bytes=store_available_bytes,
        orphan_reserve_bytes=1_024,
        concurrent_build_bytes=2_048,
        manifest_overhead_bytes_per_snapshot=512,
        parquet_measurement_snapshot_limit=10,
        parquet_measurement_file_limit=1_000,
    )


def _measurements(*, include_parquet: bool = True) -> CapacityMeasurements:
    widths = {
        "canonical_signals": 2.0,
        "stage_candidates": 3.0,
        "outcome_labels": 4.0,
        "universe_outcomes": 5.0,
        "source_revisions": 6.0,
    } if include_parquet else {}
    return CapacityMeasurements(
        database_observed_at=NOW,
        database_version="PostgreSQL test",
        trading_days=100,
        observed_partitions=500,
        source_role_count=5,
        relation_size_summary={"market.daily_basic": {"total_bytes": 1000}},
        row_distribution_summary={"row_count_p95": 5000},
        measured_role_row_widths=widths,
        measured_role_parquet_bytes_per_row_p95={role: value / 2 for role, value in widths.items()},
        parquet_measurement_provenance={"snapshot_set_hash": "a" * 64} if include_parquet else {},
        observed_partitions_by_role={"FEATURE_T": 100, "TRADABILITY": 200},
        changed_partition_ratio_by_tier={"p50": 0.1, "p95": 0.25, "max": 0.5} if include_parquet else {},
        source_fetch_peak_bytes=1_000,
    )


def test_complete_measurements_produce_deterministic_measured_receipt() -> None:
    request = _request()
    receipt_one = build_capacity_receipt(request=request, measurements=_measurements())
    receipt_two = build_capacity_receipt(request=request, measurements=_measurements())
    assert receipt_one.status is CapacityStatus.MEASURED
    assert receipt_one.receipt_hash == receipt_two.receipt_hash
    assert receipt_one.missing_measurements == ()
    tiers = receipt_one.role_projection_summary["tiers"]
    assert tiers["p50"]["role_rows"]["universe_outcomes"] < tiers["p95"]["role_rows"]["universe_outcomes"]
    assert tiers["p95"]["role_rows"]["universe_outcomes"] < tiers["max"]["role_rows"]["universe_outcomes"]
    assert receipt_one.durable_store_summary["retained_store_bytes_by_tier"]["max"] > 0


def test_missing_parquet_measurements_stays_partial_not_fake_measured() -> None:
    receipt = build_capacity_receipt(request=_request(), measurements=_measurements(include_parquet=False))
    assert receipt.status is CapacityStatus.PARTIAL
    assert "parquet_bytes_per_row_p95:canonical_signals" in receipt.missing_measurements


def test_known_storage_shortfall_is_explicitly_insufficient() -> None:
    receipt = build_capacity_receipt(request=_request(store_available_bytes=1), measurements=_measurements())
    assert receipt.status is CapacityStatus.INSUFFICIENT
    assert receipt.reason_codes == (REASON_CAPACITY_BUDGET_INSUFFICIENT,)


def test_request_cannot_claim_parquet_measurements() -> None:
    payload = _request().model_dump(mode="python")
    payload["measured_parquet_bytes_per_row_p95"] = {"canonical_signals": 1.0}
    with pytest.raises(ValidationError):
        CapacityPlanningRequest.model_validate(payload)
