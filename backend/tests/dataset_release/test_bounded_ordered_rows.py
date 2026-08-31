from __future__ import annotations

import pytest

from backend.services.dataset_release.bounded_ordered_rows import (
    OrderedMergeMetrics,
    OrderedRowPartition,
    OrderedRowStreamError,
    merge_instrument_datetime_rows,
)


def _row(code: str, day: str, value: int):
    return {"instrument": code, "datetime": day, "value": value}


def test_kway_merge_orders_two_codes_across_three_date_partitions_without_frames() -> None:
    partitions = (
        OrderedRowPartition(
            "2026-05",
            iter(
                [
                    _row("000001.SZ", "2026-05-04", 1),
                    _row("600000.SH", "2026-05-04", 2),
                ]
            ),
        ),
        OrderedRowPartition(
            "2026-06",
            iter(
                [
                    _row("000001.SZ", "2026-06-01", 3),
                    _row("600000.SH", "2026-06-01", 4),
                ]
            ),
        ),
        OrderedRowPartition(
            "2026-07",
            iter(
                [
                    _row("000001.SZ", "2026-07-01", 5),
                    _row("600000.SH", "2026-07-01", 6),
                ]
            ),
        ),
    )
    metrics = OrderedMergeMetrics()
    checkpoints = []

    rows = list(
        merge_instrument_datetime_rows(
            partitions,
            checkpoint=lambda: checkpoints.append(1),
            checkpoint_rows=2,
            metrics=metrics,
        )
    )

    assert [(row["instrument"], row["datetime"]) for row in rows] == [
        ("000001.SZ", "2026-05-04"),
        ("000001.SZ", "2026-06-01"),
        ("000001.SZ", "2026-07-01"),
        ("600000.SH", "2026-05-04"),
        ("600000.SH", "2026-06-01"),
        ("600000.SH", "2026-07-01"),
    ]
    assert metrics.as_dict() == {
        "mode": "bounded_partition_kway_merge_v1",
        "partitions": 3,
        "rows": 6,
        "peak_heap_rows": 3,
        "full_frames_materialized": 0,
        "duplicate_keys": 0,
        "local_order_violations": 0,
    }
    assert len(checkpoints) == 4


def test_kway_merge_fails_closed_on_date_code_ordered_partition() -> None:
    # Date/code order regresses the instrument key when the second date starts.
    partition = OrderedRowPartition(
        "date-code-order",
        iter(
            [
                _row("000001.SZ", "2026-05-04", 1),
                _row("600000.SH", "2026-05-04", 2),
                _row("000001.SZ", "2026-05-05", 3),
            ]
        ),
    )

    with pytest.raises(OrderedRowStreamError, match="not code/time ordered"):
        list(merge_instrument_datetime_rows((partition,)))


def test_kway_merge_handles_two_code_batches_crossed_with_two_date_chunks() -> None:
    partitions = tuple(
        OrderedRowPartition(
            f"{month}:batch-{batch_no}",
            iter([_row(code, day, batch_no * 100 + position) for code in codes for position, day in enumerate(days)]),
        )
        for month, days in (
            ("2026-06", ("2026-06-29", "2026-06-30")),
            ("2026-07", ("2026-07-01", "2026-07-02")),
        )
        for batch_no, codes in (
            (1, ("000001.SZ", "000002.SZ")),
            (2, ("600000.SH", "600001.SH")),
        )
    )
    metrics = OrderedMergeMetrics()

    rows = list(merge_instrument_datetime_rows(partitions, metrics=metrics))

    assert [(row["instrument"], row["datetime"]) for row in rows] == [
        (code, day)
        for code in ("000001.SZ", "000002.SZ", "600000.SH", "600001.SH")
        for day in ("2026-06-29", "2026-06-30", "2026-07-01", "2026-07-02")
    ]
    assert metrics.partitions == 4
    assert metrics.rows == 16
    assert metrics.peak_heap_rows == 4
    assert metrics.full_frames_materialized == 0
