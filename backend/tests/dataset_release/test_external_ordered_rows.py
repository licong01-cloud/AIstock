from __future__ import annotations

import pytest

from backend.services.dataset_release.external_ordered_rows import (
    ExternalOrderedRowsError,
    ExternalOrderedRowsMetrics,
    OrderedMappingPartition,
    external_merge_ordered_rows,
)


def _row(code: str, day: int):
    return {"code": code, "day": day}


class _CloseTrackingRows:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._rows)

    def close(self) -> None:
        self.closed = True


def test_many_partitions_use_hierarchical_bounded_spools_and_leave_no_temp_files(
    tmp_path,
) -> None:
    partitions = tuple(
        OrderedMappingPartition(
            f"p{ordinal}",
            iter([_row(code, day) for code in codes for day in days]),
        )
        for ordinal, (codes, days) in enumerate(
            (
                (("000001.SZ",), (1, 2)),
                (("000001.SZ",), (3, 4)),
                (("600000.SH",), (1, 2)),
                (("600000.SH",), (3, 4)),
                (("688001.SH",), (1, 2)),
            )
        )
    )
    metrics = ExternalOrderedRowsMetrics()

    rows = list(
        external_merge_ordered_rows(
            partitions,
            key=lambda row: (row["code"], row["day"]),
            spool_root=tmp_path,
            max_open_streams=2,
            metrics=metrics,
        )
    )

    assert [(row["code"], row["day"]) for row in rows] == [
        ("000001.SZ", 1),
        ("000001.SZ", 2),
        ("000001.SZ", 3),
        ("000001.SZ", 4),
        ("600000.SH", 1),
        ("600000.SH", 2),
        ("600000.SH", 3),
        ("600000.SH", 4),
        ("688001.SH", 1),
        ("688001.SH", 2),
    ]
    assert metrics.output_rows == 10
    assert metrics.merge_passes == 2
    assert metrics.spool_rows > 0
    assert metrics.spool_bytes > 0
    assert metrics.peak_open_streams <= 2
    assert metrics.peak_heap_rows <= 2
    assert metrics.full_frames_materialized == 0
    assert list(tmp_path.iterdir()) == []


def test_external_merge_fails_on_cross_partition_duplicate_key(tmp_path) -> None:
    partitions = (
        OrderedMappingPartition("a", [_row("000001.SZ", 1)]),
        OrderedMappingPartition("b", [_row("000001.SZ", 1)]),
    )
    with pytest.raises(ExternalOrderedRowsError, match="duplicated or regressed"):
        list(
            external_merge_ordered_rows(
                partitions,
                key=lambda row: (row["code"], row["day"]),
                spool_root=tmp_path,
                max_open_streams=2,
            )
        )


def test_external_merge_close_releases_partially_consumed_source_iterators(
    tmp_path,
) -> None:
    left = _CloseTrackingRows([_row("000001.SZ", 1), _row("000001.SZ", 2)])
    right = _CloseTrackingRows([_row("600000.SH", 1), _row("600000.SH", 2)])
    rows = external_merge_ordered_rows(
        (
            OrderedMappingPartition("left", left),
            OrderedMappingPartition("right", right),
        ),
        key=lambda row: (row["code"], row["day"]),
        spool_root=tmp_path,
        max_open_streams=2,
    )

    assert next(rows) == _row("000001.SZ", 1)
    rows.close()

    assert left.closed is True
    assert right.closed is True
