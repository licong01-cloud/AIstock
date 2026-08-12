"""Hierarchical bounded merge for many immutable ordered source partitions."""

from __future__ import annotations

import gzip
import heapq
import json
import os
import stat
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .errors import DatasetReleaseError


class ExternalOrderedRowsError(DatasetReleaseError):
    code = "BLOCKED_EXTERNAL_ORDERED_ROWS_INVALID"


@dataclass(frozen=True, slots=True)
class OrderedMappingPartition:
    identity: str
    rows: Iterable[Mapping[str, Any]]

    def __post_init__(self) -> None:
        if not self.identity.strip():
            raise ExternalOrderedRowsError("ordered partition identity is empty")


@dataclass(slots=True)
class ExternalOrderedRowsMetrics:
    source_partitions: int = 0
    source_rows: int = 0
    output_rows: int = 0
    merge_passes: int = 0
    spool_files: int = 0
    spool_rows: int = 0
    spool_bytes: int = 0
    peak_open_streams: int = 0
    peak_heap_rows: int = 0
    full_frames_materialized: int = 0

    def as_dict(self) -> dict[str, int | str]:
        return {
            "mode": "hierarchical_bounded_external_merge_v1",
            "source_partitions": self.source_partitions,
            "source_rows": self.source_rows,
            "output_rows": self.output_rows,
            "merge_passes": self.merge_passes,
            "spool_files": self.spool_files,
            "spool_rows": self.spool_rows,
            "spool_bytes": self.spool_bytes,
            "peak_open_streams": self.peak_open_streams,
            "peak_heap_rows": self.peak_heap_rows,
            "full_frames_materialized": self.full_frames_materialized,
        }


def external_merge_ordered_rows(
    partitions: Sequence[OrderedMappingPartition],
    *,
    key: Callable[[Mapping[str, Any]], tuple[Any, ...]],
    spool_root: Path,
    max_open_streams: int = 64,
    checkpoint: Callable[[], None] = lambda: None,
    checkpoint_rows: int = 10_000,
    metrics: ExternalOrderedRowsMetrics | None = None,
) -> Iterator[Mapping[str, Any]]:
    """Merge arbitrary partition counts using O(max_open_streams) rows.

    Intermediate canonical NDJSON is gzip level 1 below an exact candidate
    staging directory.  Only files created by this invocation are unlinked.
    """

    if not partitions or not 2 <= max_open_streams <= 128 or checkpoint_rows <= 0:
        raise ExternalOrderedRowsError("external merge boundary is invalid")
    identities = [item.identity for item in partitions]
    if len(identities) != len(set(identities)):
        raise ExternalOrderedRowsError("external merge partition identity is duplicated")
    root = Path(spool_root).resolve(strict=True)
    if not root.is_dir():
        raise ExternalOrderedRowsError("external merge spool root is not a directory")
    _assert_plain(root)
    report = metrics or ExternalOrderedRowsMetrics()
    if any(value for value in report.as_dict().values() if isinstance(value, int)):
        raise ExternalOrderedRowsError("external merge metrics object is not fresh")
    report.source_partitions = len(partitions)
    current = list(partitions)
    owned_spools: set[Path] = set()
    pass_no = 0
    final_rows: Iterator[Mapping[str, Any]] | None = None
    try:
        while len(current) > max_open_streams:
            following: list[OrderedMappingPartition] = []
            for group_no, offset in enumerate(range(0, len(current), max_open_streams)):
                group = current[offset : offset + max_open_streams]
                target = root / f"merge-p{pass_no:02d}-g{group_no:05d}.ndjson.gz"
                if target.exists():
                    raise ExternalOrderedRowsError("external merge spool target already exists")
                rows = _merge_group(
                    group,
                    key=key,
                    report=report,
                    count_source=pass_no == 0,
                )
                written = _write_spool(target, rows, checkpoint=checkpoint)
                owned_spools.add(target)
                report.spool_files += 1
                report.spool_rows += written
                report.spool_bytes += target.stat().st_size
                following.append(
                    OrderedMappingPartition(
                        identity=f"spool:p{pass_no}:g{group_no}",
                        rows=_iter_spool(target),
                    )
                )
            report.merge_passes += 1
            current = following
            pass_no += 1
        final_rows = _merge_group(
            current,
            key=key,
            report=report,
            count_source=pass_no == 0,
        )
        for row in final_rows:
            report.output_rows += 1
            yield row
            if report.output_rows % checkpoint_rows == 0:
                checkpoint()
        checkpoint()
    finally:
        if final_rows is not None:
            final_rows.close()
        for path in sorted(owned_spools):
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                raise ExternalOrderedRowsError("external merge exact spool cleanup failed") from exc


def _merge_group(
    partitions: Sequence[OrderedMappingPartition],
    *,
    key: Callable[[Mapping[str, Any]], tuple[Any, ...]],
    report: ExternalOrderedRowsMetrics,
    count_source: bool,
) -> Iterator[Mapping[str, Any]]:
    report.peak_open_streams = max(report.peak_open_streams, len(partitions))
    iterators = [iter(item.rows) for item in partitions]
    previous_local: list[tuple[Any, ...] | None] = [None] * len(iterators)
    heap: list[tuple[tuple[Any, ...], int, Mapping[str, Any]]] = []

    def pull(index: int) -> None:
        try:
            row = next(iterators[index])
        except StopIteration:
            return
        observed = key(row)
        previous = previous_local[index]
        if previous is not None and observed <= previous:
            raise ExternalOrderedRowsError(f"ordered partition regressed or duplicated: {partitions[index].identity}")
        previous_local[index] = observed
        heapq.heappush(heap, (observed, index, row))
        report.peak_heap_rows = max(report.peak_heap_rows, len(heap))

    try:
        for index in range(len(iterators)):
            pull(index)
        previous_global: tuple[Any, ...] | None = None
        while heap:
            observed, index, row = heapq.heappop(heap)
            if previous_global is not None and observed <= previous_global:
                raise ExternalOrderedRowsError("cross-partition ordered key is duplicated or regressed")
            previous_global = observed
            if count_source:
                report.source_rows += 1
            yield row
            pull(index)
    finally:
        for iterator in iterators:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()


def _write_spool(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    checkpoint: Callable[[], None],
) -> int:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    count = 0
    iterator = iter(rows)
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=1, newline="\n") as handle:
            for row in iterator:
                handle.write(
                    json.dumps(
                        dict(row),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                        default=str,
                    )
                    + "\n"
                )
                count += 1
                if count % 10_000 == 0:
                    checkpoint()
        os.link(temporary, path)
    except FileExistsError as exc:
        raise ExternalOrderedRowsError("external merge spool appeared concurrently") from exc
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
        temporary.unlink(missing_ok=True)
    return count


def _iter_spool(path: Path) -> Iterator[Mapping[str, Any]]:
    _assert_plain(path)
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ExternalOrderedRowsError("external merge spool row is invalid")
            yield value


def _assert_plain(path: Path) -> None:
    try:
        value = os.lstat(path)
    except OSError as exc:
        raise ExternalOrderedRowsError("external merge path is unavailable") from exc
    if stat.S_ISLNK(value.st_mode) or bool(int(getattr(value, "st_file_attributes", 0)) & 0x0400):
        raise ExternalOrderedRowsError("external merge path is symlink/reparse")


__all__ = [
    "ExternalOrderedRowsError",
    "ExternalOrderedRowsMetrics",
    "OrderedMappingPartition",
    "external_merge_ordered_rows",
]
