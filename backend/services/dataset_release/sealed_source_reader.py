"""Bounded readers for immutable source partitions frozen in control CAS.

The reader verifies the CAS bytes and the canonical partition digest while it
feeds the exact same ordered stream to a consumer.  It never calls
``CASStore.get_bytes`` for data-bearing blobs and never re-queries a source.
"""

from __future__ import annotations

import hashlib
import json
import stat
import warnings
import zlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .cas_store import CASCorruptionError, CASRef, CASStore
from .errors import SourceManifestError, SourceSnapshotDrift
from .source_authority import SOURCE_PARTITION_ROWS_SCHEMA
from .source_manifest import (
    CanonicalPartitionHasher,
    ColumnKind,
    ColumnSpec,
    PartitionSpec,
)
from .source_rows_codec import validate_rows_envelope


MAX_NDJSON_LINE_BYTES = 8 * 1024 * 1024
MAX_COMPRESSED_READ_CHUNK_BYTES = 256 * 1024
MAX_DECOMPRESSED_READ_CHUNK_BYTES = 256 * 1024


@dataclass(slots=True)
class _CompressedReadObservation:
    compressed_digest: Any
    compressed_bytes: int = 0
    uncompressed_bytes: int = 0
    finished: bool = False


@dataclass(frozen=True, slots=True)
class SealedPartitionDescriptor:
    value: Mapping[str, Any]

    @property
    def dataset(self) -> str:
        return str(self.value.get("dataset", ""))

    @property
    def partition_key(self) -> str:
        return str(self.value.get("partition_key", ""))

    @property
    def identity(self) -> str:
        return f"{self.dataset}:{self.partition_key}"

    @property
    def spec(self) -> PartitionSpec:
        raw_columns = self.value.get("columns")
        if not isinstance(raw_columns, list) or not raw_columns:
            raise SourceManifestError(f"sealed partition columns are invalid: {self.identity}")
        try:
            columns = tuple(
                ColumnSpec(
                    name=str(item["name"]),
                    kind=ColumnKind(str(item["kind"])),
                    required=bool(item["required"]),
                )
                for item in raw_columns
            )
            primary_keys = tuple(str(item) for item in self.value["primary_keys"])
            spec = PartitionSpec(
                dataset=self.dataset,
                partition_key=self.partition_key,
                query_version=str(self.value["query_version"]),
                columns=columns,
                primary_keys=primary_keys,
                timezone_name=str(self.value.get("timezone_name", "Asia/Shanghai")),
                null_marker=str(self.value.get("null_marker", "NULL_V1")),
                float_non_finite_policy=str(self.value.get("float_non_finite_policy", "reject")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceManifestError(f"sealed partition descriptor is invalid: {self.identity}") from exc
        if spec.schema_digest != self.value.get("schema_digest"):
            raise SourceManifestError(f"sealed partition schema digest differs: {self.identity}")
        return spec


class VerifiedRowStream(Iterator[Mapping[str, Any]]):
    """One-pass row stream whose successful exhaustion is the verification gate.

    A caller that only needs a prefix must use this object as a context manager
    or call :meth:`verify`; both drain and verify the unread suffix.  ``close``
    is intentionally an abort operation and never upgrades a prefix to verified.
    Abandoning a live stream without exhaustion/close emits ``ResourceWarning``
    instead of silently presenting the prefix as a verified partition.
    """

    def __init__(self, rows: Iterator[Mapping[str, Any]], *, identity: str) -> None:
        self._rows = rows
        self.identity = identity
        self._closed = False
        self._verified = False

    @property
    def verified(self) -> bool:
        return self._verified

    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self) -> "VerifiedRowStream":
        return self

    def __next__(self) -> Mapping[str, Any]:
        if self._closed:
            raise StopIteration
        try:
            return next(self._rows)
        except StopIteration:
            self._verified = True
            self._closed = True
            raise
        except BaseException:
            self._closed = True
            raise

    def verify(self) -> None:
        """Drain the unread suffix and complete every digest/content check."""

        if self._closed:
            if not self._verified:
                raise SourceSnapshotDrift(f"sealed source verification did not complete: {self.identity}")
            return
        for _row in self:
            pass
        if not self._verified:  # defensive; exhaustion above must set this
            raise SourceSnapshotDrift(f"sealed source verification did not complete: {self.identity}")

    def close(self) -> None:
        """Abort an unexhausted stream and release its file without verification."""

        if self._closed:
            return
        self._closed = True
        self._rows.close()

    def __enter__(self) -> "VerifiedRowStream":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if exc_type is None:
            self.verify()
            return
        # The consumer is already failing, so no downstream success can be
        # claimed. Close the generator without masking the original exception.
        self.close()

    def __del__(self) -> None:
        if self._closed:
            return
        warnings.warn(
            f"sealed source stream abandoned before verification: {self.identity}",
            ResourceWarning,
            stacklevel=2,
        )
        self.close()


class CASSealedPartitionReader:
    """Read immutable CAS NDJSON partitions with a strict row bound."""

    def __init__(
        self,
        cas: CASStore,
        partitions: Sequence[Mapping[str, Any]],
        *,
        max_partition_rows: int,
        max_line_bytes: int = MAX_NDJSON_LINE_BYTES,
    ) -> None:
        if type(max_partition_rows) is not int or not 0 < max_partition_rows <= 1_000_000:
            raise ValueError("max_partition_rows must be in [1,1000000]")
        if type(max_line_bytes) is not int or not 0 < max_line_bytes <= MAX_NDJSON_LINE_BYTES:
            raise ValueError("max_line_bytes exceeds the sealed reader boundary")
        descriptors = tuple(SealedPartitionDescriptor(dict(value)) for value in partitions)
        identities = [item.identity for item in descriptors]
        if not descriptors or len(identities) != len(set(identities)):
            raise SourceManifestError("sealed source partitions are empty or duplicated")
        self.cas = cas
        self.max_partition_rows = max_partition_rows
        self.max_line_bytes = max_line_bytes
        self._partitions = {item.identity: item for item in descriptors}

    def descriptor(self, dataset: str, partition_key: str) -> SealedPartitionDescriptor:
        identity = f"{dataset}:{partition_key}"
        try:
            return self._partitions[identity]
        except KeyError as exc:
            raise SourceManifestError(f"sealed source partition is missing: {identity}") from exc

    def iter_rows(
        self,
        dataset: str,
        partition_key: str,
        *,
        decode_row_payload: bool = True,
    ) -> VerifiedRowStream:
        descriptor = self.descriptor(dataset, partition_key)
        expected_rows = _bounded_nonnegative_int(
            descriptor.value.get("row_count"), field=f"{descriptor.identity}:row_count"
        )
        if expected_rows > self.max_partition_rows:
            raise SourceManifestError(
                f"sealed source partition exceeds row bound: {descriptor.identity}:"
                f"{expected_rows}>{self.max_partition_rows}"
            )
        reference = CASRef.from_value(descriptor.value.get("rows_ref"))
        if reference.size <= 0:
            raise SourceManifestError(f"sealed source CAS reference is incomplete: {descriptor.identity}")
        envelope = validate_rows_envelope(descriptor.value, cas_size=reference.size)
        path = _canonical_cas_path(self.cas, reference)

        spec = descriptor.spec

        def verified_rows() -> Iterator[Mapping[str, Any]]:
            hasher = CanonicalPartitionHasher(
                spec,
                ingestion_audit_identity=str(descriptor.value.get("ingestion_audit_identity", "")),
                snapshot_tokens=(),
            )
            observation = _CompressedReadObservation(hashlib.sha256())
            observed_rows = 0
            header_seen = False
            for raw in _iter_gzip_lines(
                path,
                max_line_bytes=self.max_line_bytes,
                observation=observation,
                identity=descriptor.identity,
            ):
                if not raw.strip():
                    continue
                try:
                    value = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise CASCorruptionError(f"sealed source NDJSON is invalid: {descriptor.identity}") from exc
                if not isinstance(value, dict):
                    raise CASCorruptionError(f"sealed source NDJSON row is not an object: {descriptor.identity}")
                if not header_seen:
                    _verify_header(value, descriptor=descriptor, reference=reference)
                    header_seen = True
                    continue
                if observed_rows >= expected_rows:
                    raise SourceSnapshotDrift(f"sealed source contains more rows than planned: {descriptor.identity}")
                normalized = hasher.update(value)
                observed_rows += 1
                if decode_row_payload and "row_payload" in normalized:
                    try:
                        payload = json.loads(str(normalized["row_payload"]))
                    except json.JSONDecodeError as exc:
                        raise SourceManifestError(
                            f"sealed source row_payload is invalid: {descriptor.identity}"
                        ) from exc
                    if not isinstance(payload, dict):
                        raise SourceManifestError(f"sealed source row_payload is not an object: {descriptor.identity}")
                    yield payload
                else:
                    yield normalized
            if not header_seen:
                raise CASCorruptionError(f"sealed source header is missing: {descriptor.identity}")
            if (
                not observation.finished
                or observation.compressed_digest.hexdigest() != reference.sha256
                or observation.compressed_bytes != reference.size
                or observation.uncompressed_bytes != envelope["rows_uncompressed_bytes"]
            ):
                raise CASCorruptionError(f"sealed source changed during read: {descriptor.identity}")
            actual = hasher.finish()
            expected_digest = str(descriptor.value.get("content_digest", ""))
            expected_merkle = str(descriptor.value.get("merkle_root", ""))
            if (
                observed_rows != expected_rows
                or actual.row_count != expected_rows
                or actual.content_digest != expected_digest
                or actual.merkle_root != expected_merkle
                or actual.required_null_count
                != _bounded_nonnegative_int(
                    descriptor.value.get("required_null_count"),
                    field=f"{descriptor.identity}:required_null_count",
                )
                or actual.duplicate_count
                != _bounded_nonnegative_int(
                    descriptor.value.get("duplicate_count"),
                    field=f"{descriptor.identity}:duplicate_count",
                )
            ):
                raise SourceSnapshotDrift(
                    f"sealed source partition digest differs: {descriptor.identity}",
                    context={
                        "expected_rows": expected_rows,
                        "actual_rows": observed_rows,
                        "expected_content_digest": expected_digest,
                        "actual_content_digest": actual.content_digest,
                    },
                )

        return VerifiedRowStream(verified_rows(), identity=descriptor.identity)

    def read_frame(self, dataset: str, partition_key: str) -> pd.DataFrame:
        with self.iter_rows(dataset, partition_key, decode_row_payload=True) as stream:
            rows = list(stream)
        if len(rows) > self.max_partition_rows:  # defensive; generator already enforces
            raise SourceManifestError("sealed frame materialization exceeded row bound")
        return pd.DataFrame.from_records(rows)

    def iter_frames(
        self,
        dataset: str,
        partition_key: str,
        *,
        start: date,
        end: date,
        max_rows: int,
    ) -> Iterator[pd.DataFrame]:
        """Yield a date slice in bounded frames while verifying the whole CAS stream."""

        if start > end or type(max_rows) is not int or not 0 < max_rows <= 100_000:
            raise SourceManifestError("sealed source frame slice boundary is invalid")
        buffered: list[Mapping[str, Any]] = []
        with self.iter_rows(dataset, partition_key, decode_row_payload=True) as stream:
            for row in stream:
                raw_date = row.get("trade_date", row.get("datetime", row.get("cal_date")))
                if raw_date is None:
                    raise SourceManifestError(f"sealed date partition row has no date field: {dataset}:{partition_key}")
                try:
                    observed = date.fromisoformat(str(raw_date)[:10])
                except ValueError as exc:
                    raise SourceManifestError(
                        f"sealed date partition row has invalid date: {dataset}:{partition_key}"
                    ) from exc
                if start <= observed <= end:
                    buffered.append(row)
                    if len(buffered) == max_rows:
                        yield pd.DataFrame.from_records(buffered)
                        buffered = []
        if buffered:
            yield pd.DataFrame.from_records(buffered)


def _verify_header(
    value: Mapping[str, Any],
    *,
    descriptor: SealedPartitionDescriptor,
    reference: CASRef,
) -> None:
    spec = descriptor.spec
    expected = {
        "schema_version": SOURCE_PARTITION_ROWS_SCHEMA,
        "partition_identity": spec.identity,
        "query_version": spec.query_version,
        "schema_digest": spec.schema_digest,
        "columns": [item.as_dict() for item in spec.columns],
        "primary_keys": list(spec.primary_keys),
        "source_table_identity": descriptor.value.get("source_table_identity"),
        "source_table_schema_digest": descriptor.value.get("source_table_schema_digest"),
    }
    if dict(value) != expected:
        raise CASCorruptionError(f"sealed source header identity differs: {descriptor.identity}:{reference.sha256}")


def _iter_gzip_lines(
    path: Path,
    *,
    max_line_bytes: int,
    observation: _CompressedReadObservation,
    identity: str,
) -> Iterator[bytes]:
    """Incrementally decompress and frame one gzip member into bounded lines."""

    decoder = zlib.decompressobj(31)
    pending = bytearray()
    try:
        with path.open("rb") as handle:
            while compressed := handle.read(MAX_COMPRESSED_READ_CHUNK_BYTES):
                observation.compressed_digest.update(compressed)
                observation.compressed_bytes += len(compressed)
                remaining = compressed
                while remaining:
                    previous = len(remaining)
                    decompressed = decoder.decompress(
                        remaining,
                        MAX_DECOMPRESSED_READ_CHUNK_BYTES,
                    )
                    remaining = decoder.unconsumed_tail
                    if decoder.unused_data:
                        raise CASCorruptionError(f"sealed source gzip has trailing data: {identity}")
                    if not decompressed and len(remaining) == previous:
                        raise CASCorruptionError(f"sealed source gzip decoder made no progress: {identity}")
                    observation.uncompressed_bytes += len(decompressed)
                    pending.extend(decompressed)
                    yield from _drain_lines(
                        pending,
                        max_line_bytes=max_line_bytes,
                        identity=identity,
                    )
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise CASCorruptionError(f"sealed source gzip is truncated: {identity}")
        if len(pending) > max_line_bytes:
            raise CASCorruptionError(f"sealed source NDJSON line exceeds bound: {identity}")
        if pending:
            yield bytes(pending)
            pending.clear()
        observation.finished = True
    except zlib.error as exc:
        raise CASCorruptionError(f"sealed source gzip is invalid: {identity}") from exc


def _drain_lines(
    pending: bytearray,
    *,
    max_line_bytes: int,
    identity: str,
) -> Iterator[bytes]:
    consumed = 0
    while True:
        newline = pending.find(b"\n", consumed)
        if newline < 0:
            if len(pending) - consumed > max_line_bytes:
                raise CASCorruptionError(f"sealed source NDJSON line exceeds bound: {identity}")
            break
        line_size = newline + 1 - consumed
        if line_size > max_line_bytes:
            raise CASCorruptionError(f"sealed source NDJSON line exceeds bound: {identity}")
        raw = bytes(pending[consumed : newline + 1])
        consumed = newline + 1
        yield raw
    # Compact once per decompressor chunk instead of memmoving the remaining
    # bytearray once for every NDJSON row.
    if consumed:
        del pending[:consumed]


def _canonical_cas_path(cas: CASStore, reference: CASRef) -> Path:
    expected_relative = f"cas/sha256/{reference.sha256[:2]}/{reference.sha256}"
    if reference.relative_path != expected_relative:
        raise CASCorruptionError("sealed source CAS relative path is non-canonical")
    requested = cas.root / Path(expected_relative)
    try:
        path = requested.resolve(strict=True)
    except OSError as exc:
        raise CASCorruptionError("sealed source CAS blob is unavailable") from exc
    if cas.root not in path.parents or path != requested:
        raise CASCorruptionError("sealed source CAS path escapes control root")
    current = cas.root
    for part in Path(expected_relative).parts:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise CASCorruptionError("sealed source CAS path is unavailable") from exc
        reparse = int(getattr(metadata, "st_file_attributes", 0)) & 0x0400
        if stat.S_ISLNK(metadata.st_mode) or reparse:
            raise CASCorruptionError("sealed source CAS path contains a link/reparse point")
    if not path.is_file() or path.stat().st_size != reference.size:
        raise CASCorruptionError("sealed source CAS size differs before streaming")
    return path


def _bounded_nonnegative_int(value: Any, *, field: str) -> int:
    if type(value) is not int or not 0 <= value <= 1_000_000_000:
        raise SourceManifestError(f"{field} is outside the bounded integer contract")
    return value


__all__ = [
    "CASSealedPartitionReader",
    "MAX_COMPRESSED_READ_CHUNK_BYTES",
    "MAX_DECOMPRESSED_READ_CHUNK_BYTES",
    "MAX_NDJSON_LINE_BYTES",
    "SealedPartitionDescriptor",
    "VerifiedRowStream",
]
