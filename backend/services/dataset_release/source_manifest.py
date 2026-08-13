from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Sequence

from .canonical import (
    digest_named_fields,
    encode_named_fields,
    ensure_sha256,
    merkle_root_from_named_digests,
)
from .errors import SourceManifestError, SourceSnapshotDrift


CANONICAL_PARTITION_HASH_VERSION = "canonical_partition_hash_v1"


class ColumnKind(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    BYTES = "bytes"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    kind: ColumnKind
    required: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "kind": self.kind.value, "required": self.required}


@dataclass(frozen=True)
class PartitionSpec:
    dataset: str
    partition_key: str
    query_version: str
    columns: tuple[ColumnSpec, ...]
    primary_keys: tuple[str, ...]
    timezone_name: str = "Asia/Shanghai"
    null_marker: str = "NULL_V1"
    float_non_finite_policy: str = "reject"

    def __post_init__(self) -> None:
        names = [item.name for item in self.columns]
        if not self.dataset or not self.partition_key or not self.query_version:
            raise SourceManifestError("partition dataset/key/query version must be non-empty")
        if not names or len(names) != len(set(names)):
            raise SourceManifestError("partition columns must be non-empty and unique")
        if not self.primary_keys or not set(self.primary_keys).issubset(names):
            raise SourceManifestError("partition primary keys must be present in columns")
        if self.float_non_finite_policy != "reject":
            raise SourceManifestError("canonical_partition_hash_v1 requires non-finite reject")

    @property
    def schema_digest(self) -> str:
        return digest_named_fields(
            "dataset_release_partition_schema_v1",
            {
                "query_version": self.query_version,
                "columns": [item.as_dict() for item in self.columns],
                "primary_keys": list(self.primary_keys),
                "timezone_name": self.timezone_name,
                "null_marker": self.null_marker,
                "float_non_finite_policy": self.float_non_finite_policy,
            },
        )

    @property
    def identity(self) -> str:
        return f"{self.dataset}:{self.partition_key}"


class _MerkleAccumulator:
    """O(log rows) ordered Merkle-mountain accumulator."""

    def __init__(self) -> None:
        self._peaks: list[bytes | None] = []
        self.count = 0

    @staticmethod
    def _parent(left: bytes, right: bytes) -> bytes:
        return hashlib.sha256(b"node\0" + left + right).digest()

    def add(self, leaf: bytes) -> None:
        node = hashlib.sha256(b"leaf\0" + leaf).digest()
        level = 0
        while level < len(self._peaks) and self._peaks[level] is not None:
            node = self._parent(self._peaks[level] or b"", node)
            self._peaks[level] = None
            level += 1
        if level == len(self._peaks):
            self._peaks.append(node)
        else:
            self._peaks[level] = node
        self.count += 1

    def root(self) -> str:
        peaks = [
            {"height": height, "digest": node.hex()} for height, node in enumerate(self._peaks) if node is not None
        ]
        return digest_named_fields(
            "dataset_release_ordered_merkle_mountain_v1",
            {"leaf_count": self.count, "peaks_low_to_high": peaks},
        )


def _normalize_value(value: Any, column: ColumnSpec) -> Any:
    if value is None:
        return None
    try:
        if column.kind is ColumnKind.STRING:
            return str(value)
        if column.kind is ColumnKind.INTEGER:
            if isinstance(value, bool):
                raise ValueError("boolean is not an integer")
            return int(value)
        if column.kind is ColumnKind.FLOAT:
            result = float(value)
            if not math.isfinite(result):
                raise ValueError("non-finite float")
            return result
        if column.kind is ColumnKind.DECIMAL:
            result = value if isinstance(value, Decimal) else Decimal(str(value))
            if not result.is_finite():
                raise ValueError("non-finite Decimal")
            return result
        if column.kind is ColumnKind.DATE:
            if isinstance(value, datetime):
                return value.date()
            return value if isinstance(value, date) else date.fromisoformat(str(value))
        if column.kind is ColumnKind.DATETIME:
            result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
            if result.tzinfo is None or result.utcoffset() is None:
                raise ValueError("naive datetime")
            return result.astimezone(timezone.utc)
        if column.kind is ColumnKind.BOOLEAN:
            if not isinstance(value, bool):
                raise ValueError("boolean column requires bool")
            return value
        if column.kind is ColumnKind.BYTES:
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise ValueError("bytes column requires bytes-like value")
            return bytes(value)
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise SourceManifestError(f"cannot normalize {column.name} as {column.kind.value}: {value!r}") from exc
    raise SourceManifestError(f"unsupported column kind: {column.kind}")


@dataclass(frozen=True)
class PartitionSummary:
    dataset: str
    partition_key: str
    query_version: str
    schema_digest: str
    row_count: int
    min_key: tuple[Any, ...] | None
    max_key: tuple[Any, ...] | None
    required_null_count: int
    duplicate_count: int
    merkle_root: str
    content_digest: str
    ingestion_audit_identity: str
    snapshot_tokens: tuple[str, ...] = ()

    @property
    def identity(self) -> str:
        return f"{self.dataset}:{self.partition_key}"

    def content_leaf(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "partition_key": self.partition_key,
            "query_version": self.query_version,
            "schema_digest": self.schema_digest,
            "row_count": self.row_count,
            "min_key": list(self.min_key) if self.min_key is not None else None,
            "max_key": list(self.max_key) if self.max_key is not None else None,
            "required_null_count": self.required_null_count,
            "duplicate_count": self.duplicate_count,
            "merkle_root": self.merkle_root,
            "content_digest": self.content_digest,
        }

    def provenance_leaf(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "content_digest": self.content_digest,
            "ingestion_audit_identity": self.ingestion_audit_identity,
            "snapshot_tokens": list(self.snapshot_tokens),
        }


class CanonicalPartitionHasher:
    def __init__(
        self,
        spec: PartitionSpec,
        *,
        ingestion_audit_identity: str,
        snapshot_tokens: Sequence[str] = (),
    ) -> None:
        self.spec = spec
        self.ingestion_audit_identity = str(ingestion_audit_identity)
        self.snapshot_tokens = tuple(str(item) for item in snapshot_tokens)
        self._merkle = _MerkleAccumulator()
        self._previous_key: tuple[Any, ...] | None = None
        self._min_key: tuple[Any, ...] | None = None
        self._max_key: tuple[Any, ...] | None = None
        self._required_null_count = 0
        self._duplicate_count = 0

    def update(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        expected = [column.name for column in self.spec.columns]
        if set(raw) != set(expected):
            raise SourceManifestError(f"partition row columns drift: expected={expected} actual={sorted(raw)}")
        normalized: dict[str, Any] = {}
        for column in self.spec.columns:
            value = _normalize_value(raw[column.name], column)
            normalized[column.name] = value
            if value is None and column.required:
                self._required_null_count += 1
        key = tuple(normalized[name] for name in self.spec.primary_keys)
        if any(value is None for value in key):
            raise SourceManifestError("partition primary key contains NULL")
        if self._previous_key is not None:
            if key == self._previous_key:
                self._duplicate_count += 1
                raise SourceManifestError(f"duplicate partition primary key: {key}")
            if key < self._previous_key:
                raise SourceManifestError(f"partition row order drift: previous={self._previous_key} current={key}")
        self._previous_key = key
        self._min_key = key if self._min_key is None else self._min_key
        self._max_key = key
        row_bytes = encode_named_fields(
            f"{CANONICAL_PARTITION_HASH_VERSION}:row",
            [(column.name, normalized[column.name]) for column in self.spec.columns],
        )
        self._merkle.add(row_bytes)
        return normalized

    def finish(self) -> PartitionSummary:
        merkle_root = self._merkle.root()
        content = {
            "canonicalizer": CANONICAL_PARTITION_HASH_VERSION,
            "dataset": self.spec.dataset,
            "partition_key": self.spec.partition_key,
            "query_version": self.spec.query_version,
            "schema_digest": self.spec.schema_digest,
            "row_count": self._merkle.count,
            "min_key": list(self._min_key) if self._min_key is not None else None,
            "max_key": list(self._max_key) if self._max_key is not None else None,
            "required_null_count": self._required_null_count,
            "duplicate_count": self._duplicate_count,
            "merkle_root": merkle_root,
        }
        return PartitionSummary(
            dataset=self.spec.dataset,
            partition_key=self.spec.partition_key,
            query_version=self.spec.query_version,
            schema_digest=self.spec.schema_digest,
            row_count=self._merkle.count,
            min_key=self._min_key,
            max_key=self._max_key,
            required_null_count=self._required_null_count,
            duplicate_count=self._duplicate_count,
            merkle_root=merkle_root,
            content_digest=digest_named_fields(
                CANONICAL_PARTITION_HASH_VERSION,
                content,
            ),
            ingestion_audit_identity=self.ingestion_audit_identity,
            snapshot_tokens=self.snapshot_tokens,
        )


def summarize_partition(
    spec: PartitionSpec,
    rows: Iterable[Mapping[str, Any]],
    *,
    ingestion_audit_identity: str,
    snapshot_tokens: Sequence[str] = (),
) -> PartitionSummary:
    hasher = CanonicalPartitionHasher(
        spec,
        ingestion_audit_identity=ingestion_audit_identity,
        snapshot_tokens=snapshot_tokens,
    )
    for row in rows:
        hasher.update(row)
    return hasher.finish()


class VerifiedPartitionStream:
    """Hash and consume the exact same ordered row stream, exactly once."""

    def __init__(self, planned: PartitionSummary, spec: PartitionSpec) -> None:
        if planned.identity != spec.identity or planned.schema_digest != spec.schema_digest:
            raise SourceManifestError("planned partition identity/schema does not match stream spec")
        self.planned = planned
        self.spec = spec

    def consume(
        self,
        rows: Iterable[Mapping[str, Any]],
        consumer: Callable[[Mapping[str, Any]], None],
        *,
        ingestion_audit_identity: str,
        snapshot_tokens: Sequence[str] = (),
    ) -> PartitionSummary:
        hasher = CanonicalPartitionHasher(
            self.spec,
            ingestion_audit_identity=ingestion_audit_identity,
            snapshot_tokens=snapshot_tokens,
        )
        for raw in rows:
            normalized = hasher.update(raw)
            consumer(normalized)
        actual = hasher.finish()
        if actual.content_digest != self.planned.content_digest:
            raise SourceSnapshotDrift(
                f"source partition drifted: {self.spec.identity}",
                context={
                    "partition": self.spec.identity,
                    "planned": self.planned.content_digest,
                    "actual": actual.content_digest,
                    "planned_rows": self.planned.row_count,
                    "actual_rows": actual.row_count,
                },
            )
        return actual


@dataclass(frozen=True)
class SourceManifest:
    partitions: tuple[PartitionSummary, ...]

    def __post_init__(self) -> None:
        identities = [item.identity for item in self.partitions]
        if len(identities) != len(set(identities)):
            raise SourceManifestError("source manifest has duplicate partition identities")
        for item in self.partitions:
            ensure_sha256(item.content_digest, field=f"partition:{item.identity}")

    @property
    def source_content_root(self) -> str:
        return merkle_root_from_named_digests(
            "dataset_release_source_content_root_v1",
            ((item.identity, item.content_digest) for item in self.partitions),
        )

    @property
    def source_provenance_root(self) -> str:
        return digest_named_fields(
            "dataset_release_source_provenance_root_v1",
            {
                "partitions": [
                    item.provenance_leaf() for item in sorted(self.partitions, key=lambda value: value.identity)
                ]
            },
        )

    def partition_map(self) -> dict[str, PartitionSummary]:
        return {item.identity: item for item in self.partitions}
