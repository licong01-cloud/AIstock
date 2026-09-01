"""Measured fixture-only benchmark for the bounded ordered-row build path."""

from __future__ import annotations

import time
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Iterator

import psutil

from .bounded_ordered_rows import OrderedMergeMetrics, OrderedRowPartition, merge_instrument_datetime_rows
from .contracts import ComponentAction
from .performance import PerformanceSample, WorkloadIdentity, evaluate_performance_gate


SYNTHETIC_BENCHMARK_SCHEMA = "dataset_release_synthetic_benchmark_v1"


def run_synthetic_benchmark(
    *,
    runs: int = 3,
    codes: int = 20,
    timestamps: int = 800,
) -> dict[str, Any]:
    """Measure three comparable runs without touching a provider or candidate."""

    if runs < 3 or codes < 4 or codes % 2 or timestamps < 4 or timestamps % 2:
        raise ValueError("synthetic benchmark dimensions are invalid")
    workload = WorkloadIdentity(
        source_rows=codes * timestamps,
        instrument_days=codes * timestamps,
        component_actions=(ComponentAction.FULL_REBUILD.value,),
        cache_class="fixture_in_memory_no_provider_v1",
        reuse_partition_count=0,
    )
    # Untimed warmup prevents module/import/cache setup from being attributed
    # to either measured side.
    _measure(workload, codes=codes, timestamps=timestamps, checkpoint_rows=10_000)
    baseline = tuple(_measure(workload, codes=codes, timestamps=timestamps, checkpoint_rows=1) for _ in range(runs))
    candidate = tuple(
        _measure(workload, codes=codes, timestamps=timestamps, checkpoint_rows=10_000) for _ in range(runs)
    )
    gate = evaluate_performance_gate(baseline, candidate)
    return {
        "schema_version": SYNTHETIC_BENCHMARK_SCHEMA,
        "status": gate["status"],
        "mode": "fixture_temp_only_no_real_data_v1",
        "baseline_contract": "bounded_kway_checkpoint_every_row_v1",
        "candidate_contract": "bounded_kway_checkpoint_every_10000_rows_v1",
        "runs_per_side": runs,
        "baseline_samples": [_sample_dict(value) for value in baseline],
        "candidate_samples": [_sample_dict(value) for value in candidate],
        "performance_gate": gate,
        "query_count": 0,
        "real_data_export": "not_run_not_authorized",
        "safety": {
            "database_reads": 0,
            "database_writes": 0,
            "provider_requests": 0,
            "production_writes": 0,
            "production_deletes": 0,
        },
    }


def _measure(
    workload: WorkloadIdentity,
    *,
    codes: int,
    timestamps: int,
    checkpoint_rows: int,
) -> PerformanceSample:
    process = psutil.Process()
    before = _memory(process)
    metrics = OrderedMergeMetrics()
    checksum = 0.0
    started = time.perf_counter()
    for row in merge_instrument_datetime_rows(
        _partitions(codes=codes, timestamps=timestamps),
        checkpoint=lambda: None,
        checkpoint_rows=checkpoint_rows,
        metrics=metrics,
    ):
        checksum += float(row["value"])
    elapsed = time.perf_counter() - started
    after = _memory(process)
    if metrics.rows != workload.source_rows or checksum <= 0:
        raise RuntimeError("synthetic benchmark semantic checksum differs")
    return PerformanceSample(
        workload=workload,
        compute_seconds=max(elapsed, 1e-9),
        rows_processed=metrics.rows,
        row_query_count=0,
        materialization_query_count=0,
        peak_aggregate_private_commit_bytes=max(before[0], after[0]),
        peak_rss_bytes=max(before[1], after[1]),
    )


def _partitions(*, codes: int, timestamps: int) -> tuple[OrderedRowPartition, ...]:
    names = tuple(f"{value:06d}.SZ" for value in range(1, codes + 1))
    moments = tuple(datetime(2026, 1, 1) + timedelta(minutes=value) for value in range(timestamps))
    code_batches = (names[: codes // 2], names[codes // 2 :])
    date_chunks = (moments[: timestamps // 2], moments[timestamps // 2 :])
    return tuple(
        OrderedRowPartition(
            partition_key=f"time-{time_no}:codes-{code_no}",
            rows=_rows(code_batch, date_chunk),
        )
        for time_no, date_chunk in enumerate(date_chunks)
        for code_no, code_batch in enumerate(code_batches)
    )


def _rows(codes: tuple[str, ...], timestamps: tuple[datetime, ...]) -> Iterator[dict[str, Any]]:
    for code_no, code in enumerate(codes):
        for time_no, timestamp in enumerate(timestamps):
            yield {
                "instrument": code,
                "datetime": timestamp.isoformat(sep=" ", timespec="seconds"),
                "value": float(code_no + time_no + 1),
            }


def _memory(process: psutil.Process) -> tuple[int, int]:
    full = process.memory_full_info()
    basic = process.memory_info()
    private = int(getattr(full, "private", getattr(full, "uss", basic.rss)))
    peak_rss = int(getattr(basic, "peak_wset", basic.rss))
    return private, peak_rss


def _sample_dict(value: PerformanceSample) -> dict[str, Any]:
    return {
        **asdict(value),
        "rows_per_second": value.rows_per_second,
    }


__all__ = ["SYNTHETIC_BENCHMARK_SCHEMA", "run_synthetic_benchmark"]
