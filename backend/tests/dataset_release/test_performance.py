from __future__ import annotations

from backend.services.dataset_release.performance import (
    GIB,
    PerformanceSample,
    WorkloadIdentity,
    evaluate_performance_gate,
)


def _workload(actions=("INCREMENTAL",)) -> WorkloadIdentity:
    return WorkloadIdentity(1000, 500, tuple(actions), "cold", 2)


def _sample(
    *,
    compute: float = 10.0,
    rows: int = 1000,
    queries: int = 20,
    materialization_queries: int = 20,
    commit: int = GIB,
    wait: float = 0.0,
    workload: WorkloadIdentity | None = None,
) -> PerformanceSample:
    return PerformanceSample(
        workload=workload or _workload(),
        compute_seconds=compute,
        rows_processed=rows,
        row_query_count=queries,
        materialization_query_count=materialization_queries,
        peak_aggregate_private_commit_bytes=commit,
        peak_rss_bytes=commit // 2,
        resource_wait_seconds=wait,
        provider_wait_seconds=wait,
        control_overhead_seconds=1.0,
    )


def test_performance_gate_passes_comparable_three_run_medians_and_excludes_wait() -> None:
    baseline = [_sample(compute=value) for value in (9.5, 10.0, 10.5)]
    candidate = [_sample(compute=value, wait=100.0) for value in (10.0, 10.5, 10.8)]
    receipt = evaluate_performance_gate(baseline, candidate)
    assert receipt["status"] == "PASS"
    assert receipt["wait_time_excluded_from_compute_regression"] is True
    assert receipt["candidate"]["resource_wait_seconds_median"] == 100.0


def test_performance_gate_fails_noncomparable_or_too_few_runs() -> None:
    baseline = [_sample(), _sample()]
    candidate = [_sample(workload=_workload(("FULL_REBUILD",))) for _ in range(3)]
    receipt = evaluate_performance_gate(baseline, candidate)
    assert receipt["status"] == "FAIL"
    assert "at_least_three_runs_per_side_required" in receipt["reasons"]
    assert "semantic_workload_not_comparable" in receipt["reasons"]


def test_performance_gate_enforces_throughput_query_memory_and_control_limits() -> None:
    baseline = [_sample() for _ in range(3)]
    candidate = [
        PerformanceSample(
            workload=_workload(),
            compute_seconds=20,
            rows_processed=1000,
            row_query_count=30,
            materialization_query_count=30,
            peak_aggregate_private_commit_bytes=13 * GIB,
            peak_rss_bytes=10 * GIB,
            control_overhead_seconds=6,
        )
        for _ in range(3)
    ]
    receipt = evaluate_performance_gate(baseline, candidate)
    assert receipt["status"] == "FAIL"
    assert receipt["checks"]["rows_per_second"]["passed"] is False
    assert receipt["checks"]["row_query_count"]["passed"] is False
    assert receipt["checks"]["aggregate_private_commit_hard_cap"]["passed"] is False
    assert receipt["checks"]["control_overhead"]["passed"] is False


def test_noop_reuse_must_issue_zero_materialization_queries() -> None:
    workload = _workload(("NOOP", "REUSE"))
    baseline = [_sample(workload=workload, materialization_queries=0) for _ in range(3)]
    candidate = [_sample(workload=workload, materialization_queries=1) for _ in range(3)]
    receipt = evaluate_performance_gate(baseline, candidate)
    assert receipt["status"] == "FAIL"
    assert receipt["checks"]["no_op_reuse_materialization_queries"]["passed"] is False
