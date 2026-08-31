from __future__ import annotations

from backend.services.dataset_release.synthetic_benchmark import (
    SYNTHETIC_BENCHMARK_SCHEMA,
    run_synthetic_benchmark,
)


def test_three_run_synthetic_benchmark_is_measured_and_query_free() -> None:
    receipt = run_synthetic_benchmark(runs=3, codes=8, timestamps=300)

    assert receipt["schema_version"] == SYNTHETIC_BENCHMARK_SCHEMA
    assert receipt["status"] == "PASS"
    assert len(receipt["baseline_samples"]) == 3
    assert len(receipt["candidate_samples"]) == 3
    assert receipt["performance_gate"]["status"] == "PASS"
    assert receipt["performance_gate"]["candidate"]["rows_per_second_median"] > 0
    assert receipt["query_count"] == 0
    assert receipt["real_data_export"] == "not_run_not_authorized"
    assert receipt["safety"] == {
        "database_reads": 0,
        "database_writes": 0,
        "provider_requests": 0,
        "production_writes": 0,
        "production_deletes": 0,
    }
