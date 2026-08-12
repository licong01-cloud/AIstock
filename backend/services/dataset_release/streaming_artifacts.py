"""Bounded date-chunk, Parquet, and pandas-HDF table primitives.

Every reader either proves its chunk bound before materializing rows or fails
with a typed error.  In particular, legacy pandas ``format='fixed'`` H5 files
are never silently loaded in full under a "bounded" API.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .errors import DatasetReleaseError


MAX_ROW_GROUP_ROWS = 100_000


class StreamingArtifactError(DatasetReleaseError):
    """Base class for bounded artifact failures."""

    code = "DATASET_RELEASE_STREAMING_ARTIFACT_INVALID"


class ArtifactChunkTooLarge(StreamingArtifactError):
    """Raised before reading a row group larger than the configured bound."""

    code = "BLOCKED_ARTIFACT_CHUNK_BOUND_EXCEEDED"


class ArtifactSchemaDrift(StreamingArtifactError):
    """Raised when ordered columns, dtypes, or index shape change by chunk."""

    code = "BLOCKED_ARTIFACT_SCHEMA_DRIFT"


class LegacyFixedH5BoundedReadUnsupported(StreamingArtifactError):
    """A fixed H5 cannot be claimed as bounded without a proven low-level reader."""

    code = "BLOCKED_LEGACY_FIXED_H5_UNBOUNDED"


@dataclass(frozen=True, slots=True)
class DateChunk:
    ordinal: int
    start: date
    end: date

    def __post_init__(self) -> None:
        if int(self.ordinal) < 0:
            raise ValueError("date chunk ordinal must be non-negative")
        if self.end < self.start:
            raise ValueError("date chunk end must not precede start")

    @property
    def chunk_id(self) -> str:
        return f"chunk_{self.ordinal:04d}_{self.start:%Y%m%d}_{self.end:%Y%m%d}"

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update(
            {
                "chunk_id": self.chunk_id,
                "start": self.start.isoformat(),
                "end": self.end.isoformat(),
            }
        )
        return payload


def build_date_chunks(start: date, end: date, months: int = 3) -> list[DateChunk]:
    if end < start:
        raise ValueError("date chunk end must not precede start")
    if int(months) <= 0:
        raise ValueError("date chunk months must be positive")
    chunks: list[DateChunk] = []
    cursor = start
    while cursor <= end:
        next_start = (pd.Timestamp(cursor) + pd.DateOffset(months=int(months))).date()
        chunk_end = min(end, next_start - timedelta(days=1))
        chunks.append(DateChunk(len(chunks), cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def build_fixed_day_chunks(start: date, end: date, days: int) -> list[DateChunk]:
    if end < start:
        raise ValueError("date chunk end must not precede start")
    if int(days) <= 0:
        raise ValueError("fixed date chunk days must be positive")
    chunks: list[DateChunk] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=int(days) - 1))
        chunks.append(DateChunk(len(chunks), cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def write_frame_parquet_atomic(
    frame: pd.DataFrame,
    path: Path,
    *,
    row_group_size: int = MAX_ROW_GROUP_ROWS,
) -> dict[str, Any]:
    """Write one already-bounded frame to a new Parquet authority file."""

    _require_row_bound(row_group_size)
    normalized = _validate_frame(frame, source=str(path), allow_empty=True)
    path = Path(path)
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _new_temporary(path, suffix=".partial.parquet")
    try:
        normalized.to_parquet(
            temporary,
            engine="pyarrow",
            compression="snappy",
            index=True,
            row_group_size=int(row_group_size),
        )
        _fsync_file(temporary)
        _publish_new(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    parquet = pq.ParquetFile(path)
    largest = max(
        (parquet.metadata.row_group(index).num_rows for index in range(parquet.num_row_groups)),
        default=0,
    )
    if largest > int(row_group_size):
        raise ArtifactChunkTooLarge(f"writer produced row group {largest} above bound {row_group_size}")
    return {
        "schema_version": "dataset_release_parquet_chunk_v1",
        "path": str(path),
        "sha256": sha256_file(path),
        "rows": int(len(normalized)),
        "columns": [str(value) for value in normalized.columns],
        "row_groups": int(parquet.num_row_groups),
        "max_row_group_rows": int(largest),
        "size_bytes": int(path.stat().st_size),
    }


def iter_parquet_frames(
    paths: Iterable[Path],
    *,
    max_rows: int = MAX_ROW_GROUP_ROWS,
) -> Iterator[pd.DataFrame]:
    """Read at most one preflighted Parquet row group at a time."""

    _require_row_bound(max_rows)
    for raw_path in paths:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        for row_group in range(parquet.num_row_groups):
            rows = int(parquet.metadata.row_group(row_group).num_rows)
            if rows > int(max_rows):
                raise ArtifactChunkTooLarge(
                    f"Parquet row group exceeds bound before read: path={path} "
                    f"row_group={row_group} rows={rows} max_rows={max_rows}"
                )
            table = parquet.read_row_group(row_group)
            try:
                frame = table.to_pandas()
            finally:
                del table
            if len(frame) > int(max_rows):
                raise ArtifactChunkTooLarge(f"Parquet materialization exceeded max_rows={max_rows}")
            yield _validate_frame(frame, source=str(path), allow_empty=True)


def finalize_h5_from_parquet_chunks(
    chunk_paths: Sequence[Path],
    output_path: Path,
    *,
    expected_columns: Sequence[str] | None = None,
    dtype_overrides: Mapping[str, str] | None = None,
    max_rows_in_memory: int = MAX_ROW_GROUP_ROWS,
) -> dict[str, Any]:
    """Stream row groups into a new pandas table-format H5 artifact."""

    if not chunk_paths:
        raise ValueError("at least one Parquet chunk is required")
    _require_row_bound(max_rows_in_memory)
    output_path = Path(output_path)
    if output_path.exists():
        raise FileExistsError(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _new_temporary(output_path, suffix=".partial.h5")
    columns = list(expected_columns) if expected_columns is not None else None
    dtype_contract: dict[str, str] | None = None
    previous_last: tuple[pd.Timestamp, str] | None = None
    rows = 0
    chunks = 0
    largest = 0
    try:
        with pd.HDFStore(str(temporary), mode="w") as store:
            for frame in iter_parquet_frames(chunk_paths, max_rows=max_rows_in_memory):
                if frame.empty:
                    continue
                frame = frame.copy()
                for column, dtype in (dtype_overrides or {}).items():
                    if column in frame.columns:
                        frame[column] = pd.to_numeric(frame[column], errors="raise").astype(dtype)
                actual_columns = [str(value) for value in frame.columns]
                if columns is None:
                    columns = actual_columns
                if actual_columns != columns:
                    raise ArtifactSchemaDrift(f"H5 ordered columns drifted: expected={columns} actual={actual_columns}")
                actual_dtypes = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
                if dtype_contract is None:
                    dtype_contract = actual_dtypes
                elif actual_dtypes != dtype_contract:
                    raise ArtifactSchemaDrift(f"H5 dtype drift: expected={dtype_contract} actual={actual_dtypes}")
                first = _index_key(frame.index[0])
                last = _index_key(frame.index[-1])
                if previous_last is not None and first <= previous_last:
                    raise ArtifactSchemaDrift("H5 input row groups are not globally strictly ordered")
                store.append(
                    "data",
                    frame,
                    format="table",
                    data_columns=["datetime", "instrument"],
                    index=False,
                    min_itemsize={"instrument": 16},
                )
                previous_last = last
                rows += len(frame)
                chunks += 1
                largest = max(largest, len(frame))
        if rows <= 0:
            raise StreamingArtifactError("all H5 source chunks are empty")
        _fsync_file(temporary)
        _publish_new(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "schema_version": "dataset_release_hdf_table_v1",
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "rows": rows,
        "columns": columns or [],
        "dtypes": dtype_contract or {},
        "stream_chunks": chunks,
        "max_rows_in_memory": largest,
        "format": "pandas_hdf_table_v1",
        "size_bytes": int(output_path.stat().st_size),
    }


def finalize_parquet_chunks(
    chunk_paths: Sequence[Path],
    output_path: Path,
    *,
    max_rows_in_memory: int = MAX_ROW_GROUP_ROWS,
) -> dict[str, Any]:
    """Concatenate compatible Parquet row groups without a whole-panel frame."""

    if not chunk_paths:
        raise ValueError("at least one Parquet chunk is required")
    _require_row_bound(max_rows_in_memory)
    output_path = Path(output_path)
    if output_path.exists():
        raise FileExistsError(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _new_temporary(output_path, suffix=".partial.parquet")
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    rows = 0
    row_groups = 0
    largest = 0
    previous_last: tuple[pd.Timestamp, str] | None = None
    try:
        for path in map(Path, chunk_paths):
            parquet = pq.ParquetFile(path)
            if schema is None:
                schema = parquet.schema_arrow
                writer = pq.ParquetWriter(
                    temporary,
                    schema,
                    compression="snappy",
                    use_dictionary=True,
                )
            elif not parquet.schema_arrow.equals(schema, check_metadata=True):
                raise ArtifactSchemaDrift(f"Parquet schema drift: {path}")
            assert writer is not None
            for row_group in range(parquet.num_row_groups):
                group_rows = int(parquet.metadata.row_group(row_group).num_rows)
                if group_rows > int(max_rows_in_memory):
                    raise ArtifactChunkTooLarge(
                        f"Parquet row group exceeds bound before read: {path} "
                        f"rows={group_rows} max={max_rows_in_memory}"
                    )
                table = parquet.read_row_group(row_group)
                frame = _validate_frame(table.to_pandas(), source=str(path), allow_empty=True)
                if not frame.empty:
                    first = _index_key(frame.index[0])
                    last = _index_key(frame.index[-1])
                    if previous_last is not None and first <= previous_last:
                        raise ArtifactSchemaDrift("Parquet chunks are not globally strictly ordered")
                    previous_last = last
                writer.write_table(table)
                rows += int(table.num_rows)
                row_groups += 1
                largest = max(largest, int(table.num_rows))
                del frame, table
        if writer is None or rows <= 0:
            raise StreamingArtifactError("all Parquet source chunks are empty")
    finally:
        if writer is not None:
            writer.close()
    try:
        _fsync_file(temporary)
        _publish_new(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "schema_version": "dataset_release_parquet_aggregate_v1",
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "rows": rows,
        "row_groups": row_groups,
        "max_rows_in_memory": largest,
        "chunks": len(chunk_paths),
        "size_bytes": int(output_path.stat().st_size),
    }


def iter_hdf_frames(
    path: Path,
    *,
    chunksize: int = MAX_ROW_GROUP_ROWS,
) -> Iterator[pd.DataFrame]:
    """Bounded table-H5 reader; fixed H5 is explicitly blocked."""

    _require_row_bound(chunksize)
    previous_last: tuple[pd.Timestamp, str] | None = None
    with pd.HDFStore(str(path), mode="r") as store:
        storer = store.get_storer("data")
        if storer is None:
            raise StreamingArtifactError(f"HDF key 'data' missing: {path}")
        if not bool(getattr(storer, "is_table", False)):
            raise LegacyFixedH5BoundedReadUnsupported(f"legacy fixed H5 cannot be bounded-read safely: {path}")
        for frame in store.select("data", chunksize=int(chunksize)):
            if len(frame) > int(chunksize):
                raise ArtifactChunkTooLarge(f"HDF table returned {len(frame)} rows above chunksize={chunksize}")
            validated = _validate_frame(frame, source=str(path), allow_empty=True)
            if not validated.empty:
                first = _index_key(validated.index[0])
                last = _index_key(validated.index[-1])
                if previous_last is not None and first <= previous_last:
                    raise ArtifactSchemaDrift(f"{path} HDF chunks are not globally strictly ordered")
                previous_last = last
            yield validated


def sha256_file(path: Path, *, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def _validate_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    allow_empty: bool,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{source} must be a pandas DataFrame")
    if frame.empty and allow_empty:
        return frame
    if not isinstance(frame.index, pd.MultiIndex):
        raise ArtifactSchemaDrift(f"{source} must use a MultiIndex")
    if list(frame.index.names) != ["datetime", "instrument"]:
        raise ArtifactSchemaDrift(f"{source} index must be datetime,instrument")
    if frame.index.has_duplicates:
        raise ArtifactSchemaDrift(f"{source} contains duplicate index rows")
    if not frame.index.is_monotonic_increasing:
        raise ArtifactSchemaDrift(f"{source} index is not physically monotonically increasing")
    return frame


def _index_key(value: Any) -> tuple[pd.Timestamp, str]:
    return pd.Timestamp(value[0]), str(value[1])


def _require_row_bound(value: int) -> None:
    if not 1 <= int(value) <= MAX_ROW_GROUP_ROWS:
        raise ValueError(f"row chunk bound must be in 1..{MAX_ROW_GROUP_ROWS}")


def _new_temporary(target: Path, *, suffix: str) -> Path:
    descriptor, name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=suffix, dir=str(target.parent))
    os.close(descriptor)
    return Path(name)


def _fsync_file(path: Path) -> None:
    with Path(path).open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _publish_new(source: Path, target: Path) -> None:
    if target.exists():
        raise FileExistsError(target)
    # os.link is create-if-absent on both Windows and POSIX.  The exact
    # temporary inode is then removed by the caller; no existing target can be
    # replaced by a stale worker.
    os.link(source, target)
