"""Bounded k-way merge for code/time ordered immutable row partitions."""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from .errors import DatasetReleaseError


class OrderedRowStreamError(DatasetReleaseError):
    code = "BLOCKED_ORDERED_SOURCE_STREAM_INVALID"


@dataclass(frozen=True, slots=True)
class OrderedRowPartition:
    partition_key: str
    rows: Iterable[Mapping[str, Any]]

    def __post_init__(self) -> None:
        if not self.partition_key.strip():
            raise OrderedRowStreamError("ordered row partition key is empty")


@dataclass(slots=True)
class OrderedMergeMetrics:
    partitions: int = 0
    rows: int = 0
    peak_heap_rows: int = 0
    full_frames_materialized: int = 0
    duplicate_keys: int = 0
    local_order_violations: int = 0

    def as_dict(self) -> dict[str, int | str]:
        return {
            "mode": "bounded_partition_kway_merge_v1",
            "partitions": self.partitions,
            "rows": self.rows,
            "peak_heap_rows": self.peak_heap_rows,
            "full_frames_materialized": self.full_frames_materialized,
            "duplicate_keys": self.duplicate_keys,
            "local_order_violations": self.local_order_violations,
        }


def merge_instrument_datetime_rows(
    partitions: Sequence[OrderedRowPartition],
    *,
    checkpoint: Callable[[], None] = lambda: None,
    checkpoint_rows: int = 10_000,
    max_open_partitions: int = 128,
    metrics: OrderedMergeMetrics | None = None,
) -> Iterator[Mapping[str, Any]]:
    """Merge streams ordered by ``instrument,datetime`` using O(partitions) rows.

    The function does not sort or accumulate a partition.  A source partition
    in date/code order fails immediately; resolution must seal artifact-ready
    rows in the consumer's canonical code/time order.
    """

    if not partitions or len(partitions) > max_open_partitions or checkpoint_rows <= 0 or max_open_partitions <= 0:
        raise OrderedRowStreamError("ordered merge partition/memory boundary is invalid")
    keys = [item.partition_key for item in partitions]
    if len(keys) != len(set(keys)):
        raise OrderedRowStreamError("ordered merge partition identities are duplicated")
    report = metrics or OrderedMergeMetrics()
    report.partitions = len(partitions)
    iterators = [iter(item.rows) for item in partitions]
    local_previous: list[tuple[str, datetime] | None] = [None] * len(iterators)
    heap: list[tuple[tuple[str, datetime], int, Mapping[str, Any]]] = []

    def pull(index: int) -> None:
        try:
            row = next(iterators[index])
        except StopIteration:
            return
        key = _row_key(row)
        previous = local_previous[index]
        if previous is not None and key <= previous:
            report.local_order_violations += 1
            raise OrderedRowStreamError(f"source partition is not code/time ordered: {keys[index]}")
        local_previous[index] = key
        heapq.heappush(heap, (key, index, row))
        report.peak_heap_rows = max(report.peak_heap_rows, len(heap))
        if report.peak_heap_rows > max_open_partitions:
            raise OrderedRowStreamError("ordered merge heap exceeded hard row bound")

    for index in range(len(iterators)):
        pull(index)
    previous_global: tuple[str, datetime] | None = None
    while heap:
        key, index, row = heapq.heappop(heap)
        if previous_global is not None and key == previous_global:
            report.duplicate_keys += 1
            raise OrderedRowStreamError(f"duplicate canonical row key: {key}")
        if previous_global is not None and key < previous_global:
            raise OrderedRowStreamError("k-way merge produced non-monotonic output")
        previous_global = key
        report.rows += 1
        yield row
        pull(index)
        if report.rows % checkpoint_rows == 0:
            checkpoint()
    checkpoint()


def _row_key(row: Mapping[str, Any]) -> tuple[str, datetime]:
    missing = [field for field in ("instrument", "datetime") if field not in row]
    if missing:
        raise OrderedRowStreamError(f"canonical ordered row omits keys: {missing}")
    instrument = str(row["instrument"]).strip().upper()
    if not instrument:
        raise OrderedRowStreamError("canonical ordered row instrument is empty")
    try:
        timestamp = datetime.fromisoformat(str(row["datetime"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise OrderedRowStreamError("canonical ordered row datetime is invalid") from exc
    if timestamp.tzinfo is not None:
        timestamp = timestamp.replace(tzinfo=None)
    return instrument, timestamp


__all__ = [
    "OrderedMergeMetrics",
    "OrderedRowPartition",
    "OrderedRowStreamError",
    "merge_instrument_datetime_rows",
]
