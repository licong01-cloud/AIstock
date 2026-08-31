from __future__ import annotations

import statistics
import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from .contracts import ComponentAction


GIB = 2**30
MIB = 2**20
PERFORMANCE_SCHEMA_VERSION = "dataset_release_performance_gate_v1"


@dataclass(frozen=True, slots=True)
class WorkloadIdentity:
    source_rows: int
    instrument_days: int
    component_actions: tuple[str, ...]
    cache_class: str
    reuse_partition_count: int

    def __post_init__(self) -> None:
        if self.source_rows < 0 or self.instrument_days < 0 or self.reuse_partition_count < 0:
            raise ValueError("workload counts cannot be negative")
        if not self.component_actions or not self.cache_class.strip():
            raise ValueError("workload action/cache identity is required")


@dataclass(frozen=True, slots=True)
class PerformanceSample:
    workload: WorkloadIdentity
    compute_seconds: float
    rows_processed: int
    row_query_count: int
    materialization_query_count: int
    peak_aggregate_private_commit_bytes: int
    peak_rss_bytes: int
    resource_wait_seconds: float = 0.0
    provider_wait_seconds: float = 0.0
    control_overhead_seconds: float = 0.0

    def __post_init__(self) -> None:
        numeric = (
            self.compute_seconds,
            self.rows_processed,
            self.row_query_count,
            self.materialization_query_count,
            self.peak_aggregate_private_commit_bytes,
            self.peak_rss_bytes,
            self.resource_wait_seconds,
            self.provider_wait_seconds,
            self.control_overhead_seconds,
        )
        if any(not math.isfinite(float(value)) or value < 0 for value in numeric) or self.compute_seconds <= 0:
            raise ValueError("performance sample metrics must be non-negative and compute positive")

    @property
    def rows_per_second(self) -> float:
        return self.rows_processed / self.compute_seconds


def _median(samples: Sequence[PerformanceSample], field: str) -> float:
    return float(statistics.median(float(getattr(sample, field)) for sample in samples))


def _summary(samples: Sequence[PerformanceSample]) -> dict[str, Any]:
    return {
        "runs": len(samples),
        "compute_seconds_median": _median(samples, "compute_seconds"),
        "rows_per_second_median": float(statistics.median(sample.rows_per_second for sample in samples)),
        "row_query_count_median": _median(samples, "row_query_count"),
        "materialization_query_count_max": max(sample.materialization_query_count for sample in samples),
        "peak_aggregate_private_commit_bytes_median": _median(samples, "peak_aggregate_private_commit_bytes"),
        "peak_aggregate_private_commit_bytes_max": max(
            sample.peak_aggregate_private_commit_bytes for sample in samples
        ),
        "peak_rss_bytes_max": max(sample.peak_rss_bytes for sample in samples),
        "resource_wait_seconds_median": _median(samples, "resource_wait_seconds"),
        "provider_wait_seconds_median": _median(samples, "provider_wait_seconds"),
        "control_overhead_seconds_max": max(sample.control_overhead_seconds for sample in samples),
    }


def evaluate_performance_gate(
    baseline: Sequence[PerformanceSample],
    candidate: Sequence[PerformanceSample],
) -> dict[str, Any]:
    """Compare at least three identical-workload runs without hiding wait time."""

    reasons: list[str] = []
    if len(baseline) < 3 or len(candidate) < 3:
        reasons.append("at_least_three_runs_per_side_required")
    identities = {sample.workload for sample in (*baseline, *candidate)}
    if len(identities) != 1:
        reasons.append("semantic_workload_not_comparable")
    if not baseline or not candidate:
        return {
            "schema_version": PERFORMANCE_SCHEMA_VERSION,
            "status": "FAIL",
            "reasons": reasons or ["samples_missing"],
            "checks": {},
        }

    baseline_summary = _summary(baseline)
    candidate_summary = _summary(candidate)
    baseline_compute = baseline_summary["compute_seconds_median"]
    baseline_throughput = baseline_summary["rows_per_second_median"]
    baseline_queries = baseline_summary["row_query_count_median"]
    baseline_commit = baseline_summary["peak_aggregate_private_commit_bytes_median"]
    query_allowance = max(2.0, 0.05 * baseline_queries)
    commit_allowance = max(1.10 * baseline_commit, baseline_commit + 256 * MIB)
    actions = set(next(iter(identities)).component_actions) if len(identities) == 1 else set()
    no_materialization = actions.issubset({ComponentAction.NOOP.value, ComponentAction.REUSE.value})
    checks: dict[str, Mapping[str, Any]] = {
        "compute_seconds": {
            "passed": candidate_summary["compute_seconds_median"] <= 1.10 * baseline_compute,
            "actual": candidate_summary["compute_seconds_median"],
            "limit": 1.10 * baseline_compute,
        },
        "rows_per_second": {
            "passed": candidate_summary["rows_per_second_median"] >= 0.90 * baseline_throughput,
            "actual": candidate_summary["rows_per_second_median"],
            "minimum": 0.90 * baseline_throughput,
        },
        "row_query_count": {
            "passed": candidate_summary["row_query_count_median"] <= baseline_queries + query_allowance,
            "actual": candidate_summary["row_query_count_median"],
            "limit": baseline_queries + query_allowance,
        },
        "no_op_reuse_materialization_queries": {
            "passed": (candidate_summary["materialization_query_count_max"] == 0 if no_materialization else True),
            "actual": candidate_summary["materialization_query_count_max"],
            "applicable": no_materialization,
        },
        "aggregate_private_commit_hard_cap": {
            "passed": candidate_summary["peak_aggregate_private_commit_bytes_max"] <= 12 * GIB,
            "actual": candidate_summary["peak_aggregate_private_commit_bytes_max"],
            "limit": 12 * GIB,
        },
        "aggregate_private_commit_regression": {
            "passed": candidate_summary["peak_aggregate_private_commit_bytes_median"] <= commit_allowance,
            "actual": candidate_summary["peak_aggregate_private_commit_bytes_median"],
            "limit": commit_allowance,
        },
        "control_overhead": {
            "passed": candidate_summary["control_overhead_seconds_max"] <= 5.0,
            "actual": candidate_summary["control_overhead_seconds_max"],
            "limit": 5.0,
        },
    }
    reasons.extend(name for name, check in checks.items() if not bool(check["passed"]))
    workload = asdict(next(iter(identities))) if len(identities) == 1 else None
    return {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "status": "PASS" if not reasons else "FAIL",
        "reasons": reasons,
        "workload": workload,
        "baseline": baseline_summary,
        "candidate": candidate_summary,
        "checks": checks,
        "wait_time_excluded_from_compute_regression": True,
    }
