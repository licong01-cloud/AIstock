"""Deterministic Batch D Parquet writer, schema registry, and full verifier.

PyArrow is deliberately imported only inside execution functions.  Importing
this module therefore does not initialize PyArrow from the FastAPI runtime.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import itertools
import json
import logging
import os
import pickle
from pathlib import Path, PurePosixPath
import sqlite3
import tempfile
import threading
from typing import Any, Iterable, Iterator, Mapping, Sequence
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.dataset_build import (
    AttemptOperation,
    BaseSnapshotIdentity,
    BuildCheckpoint,
    DatasetBlobHeader,
    DatasetAttemptFile,
    DatasetBuild,
    DatasetSnapshotBlobRef,
    DatasetSnapshotFile,
    DatasetSnapshotLabel,
    DatasetSnapshotObservation,
    SealedDatasetSnapshot,
    verify_attempt_file_set,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore, StoredCasObject
from backend.services.advisory_phase1.observation_selector import OBSERVATION_SELECTOR_POLICY_HASH


BATCH_D_WRITER_VERSION = "ADVISORY_PHASE1C3_PYARROW21_PARQUET_V1"
BATCH_D_BUILDER_VERSION = "ADVISORY_PHASE1C3_BUILDER_V1"
BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT = "PHASE1C3_BATCH_D_FULL_PARQUET_V1"
SNAPSHOT_SCHEMA_VERSION = "advisory_phase1c3_dataset_snapshot_v1"
SCHEMA_DESCRIPTOR_ROLE = "SCHEMA_DESCRIPTOR"

REASON_ARROW_SCHEMA_CONFLICT = "ADVISORY_PHASE1C3_ARROW_SCHEMA_CONFLICT"
REASON_PARQUET_WRITE_FAILED = "ADVISORY_PHASE1C3_PARQUET_WRITE_FAILED"
REASON_PARQUET_BYTES_CONFLICT = "ADVISORY_PHASE1C3_PARQUET_BYTES_CONFLICT"
REASON_PARQUET_FULL_VERIFY_FAILED = "ADVISORY_PHASE1C3_PARQUET_FULL_VERIFY_FAILED"
REASON_RELATIONAL_CLOSURE_FAILED = "ADVISORY_PHASE1C3_RELATIONAL_CLOSURE_FAILED"
REASON_MANIFEST_CONFLICT = "ADVISORY_PHASE1C3_MANIFEST_CONFLICT"
REASON_PROMOTION_RECEIPT_CONFLICT = "ADVISORY_PHASE1C3_PROMOTION_RECEIPT_CONFLICT"
REASON_SOURCE_SNAPSHOT_CONFLICT = "ADVISORY_PHASE1C3_SOURCE_SNAPSHOT_CONFLICT"
REASON_EVIDENCE_BLOB_INVALID = "ADVISORY_PHASE1C3_EVIDENCE_BLOB_INVALID"

logger = logging.getLogger(__name__)


class SnapshotWriterError(RuntimeError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


def _sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone")
    return value.astimezone(timezone.utc)


class SnapshotSchemaField(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    arrow_type: str = Field(min_length=1)
    nullable: bool


class LogicalDatasetRow(BaseModel):
    """One pre-sorted typed logical row; values are checked by its role schema."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    logical_role: str = Field(min_length=1)
    partition_key: dict[str, str] = Field(default_factory=dict)
    sort_key: tuple[str, ...] = ()
    values: dict[str, Any]

    @model_validator(mode="after")
    def _role_known(self) -> "LogicalDatasetRow":
        if self.logical_role not in SNAPSHOT_ARROW_SCHEMAS_V1:
            raise ValueError("logical row has an unknown dataset role")
        return self

    def canonical_identity(self) -> dict[str, Any]:
        return canonicalize(self.model_dump(mode="python"))


class SnapshotFileIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    logical_path: str = Field(min_length=1, max_length=1024)
    logical_role: str = Field(min_length=1, max_length=160)
    partition_key_hash: str = Field(min_length=64, max_length=64)
    ordinal: int = Field(ge=0)
    sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(gt=0)
    row_count: int = Field(ge=0)
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    compression: str = Field(min_length=1, max_length=80)
    writer_version: str = Field(min_length=1, max_length=160)

    @field_validator("partition_key_hash", "sha256", "schema_fingerprint", "partition_content_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return _sha256(value, field_name=info.field_name)

    @property
    def ordinal_key(self) -> tuple[str, str, int]:
        return self.logical_role, self.partition_key_hash, self.ordinal

    def canonical_identity(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class DiskBackedRows:
    """Repeatable row iterable whose payload is never accumulated in Python memory."""

    def __init__(self, rows: Iterable[Any] = ()) -> None:
        self._file = tempfile.TemporaryFile(mode="w+b")
        self._count = 0
        self.extend(rows)

    def append(self, row: Any) -> None:
        pickle.dump(row, self._file, protocol=pickle.HIGHEST_PROTOCOL)
        self._count += 1

    def extend(self, rows: Iterable[Any]) -> None:
        for row in rows:
            self.append(row)

    def __iter__(self) -> Iterator[Any]:
        self._file.flush()
        self._file.seek(0)
        while True:
            try:
                yield pickle.load(self._file)
            except EOFError:
                return

    def __len__(self) -> int:
        return self._count

    def close(self) -> None:
        self._file.close()


class LogicalRowPartitionSpool:
    """External-sort logical rows by role, partition and frozen sort key."""

    def __init__(self) -> None:
        handle = tempfile.NamedTemporaryFile(prefix="aistock-batchd-", suffix=".sqlite3", delete=False)
        handle.close()
        self._path = Path(handle.name)
        self._conn = sqlite3.connect(self._path)
        self._conn.execute(
            "CREATE TABLE rows (logical_role TEXT NOT NULL, partition_key TEXT NOT NULL, "
            "sort_key TEXT NOT NULL, payload BLOB NOT NULL, PRIMARY KEY (logical_role, partition_key, sort_key))"
        )

    def append(self, row: LogicalDatasetRow) -> None:
        partition = _canonical_json_bytes(dict(sorted(row.partition_key.items()))).decode("ascii")
        sort_key = _canonical_json_bytes(list(row.sort_key)).decode("ascii")
        try:
            self._conn.execute(
                "INSERT INTO rows (logical_role, partition_key, sort_key, payload) VALUES (?, ?, ?, ?)",
                (row.logical_role, partition, sort_key, pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)),
            )
        except sqlite3.IntegrityError as error:
            raise SnapshotWriterError(REASON_PARQUET_WRITE_FAILED, "logical rows have duplicate frozen sort keys") from error

    def commit(self) -> None:
        self._conn.commit()

    def partitions(self, logical_role: str) -> Iterator[dict[str, str]]:
        cursor = self._conn.execute(
            "SELECT DISTINCT partition_key FROM rows WHERE logical_role = ? ORDER BY partition_key",
            (logical_role,),
        )
        for (payload,) in cursor:
            yield json.loads(str(payload))

    def rows(self, *, logical_role: str, partition_key: Mapping[str, str]) -> Iterator[LogicalDatasetRow]:
        partition = _canonical_json_bytes(dict(sorted(partition_key.items()))).decode("ascii")
        cursor = self._conn.execute(
            "SELECT payload FROM rows WHERE logical_role = ? AND partition_key = ? ORDER BY sort_key",
            (logical_role, partition),
        )
        for (payload,) in cursor:
            yield pickle.loads(payload)

    def close(self) -> None:
        self._conn.close()
        self._path.unlink(missing_ok=True)


class _AttemptHeartbeat:
    def __init__(self, *, repository: Any, attempt: Any, interval_seconds: int = 60) -> None:
        self._repository = repository
        self._attempt = attempt
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"batchd-heartbeat-{attempt.attempt_id}",
            daemon=True,
        )

    def __enter__(self) -> "_AttemptHeartbeat":
        self._thread.start()
        return self

    def __exit__(self, error_type: Any, error: Any, traceback: Any) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "attempt heartbeat thread did not stop")
        if self._error is not None:
            if error is not None:
                logger.error(
                    "Batch D heartbeat also failed while operation was failing",
                    extra={"attempt_id": self._attempt.attempt_id, "error_type": type(self._error).__name__},
                )
                return
            raise self._error

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._repository.heartbeat_attempt(
                    attempt_id=self._attempt.attempt_id,
                    expected_fencing_token=self._attempt.fencing_token,
                    lease_seconds=900,
                )
            except Exception as error:
                if self._stop.is_set():
                    return
                self._error = error
                self._stop.set()
                return


class WrittenDatasetFile(SnapshotFileIdentity):
    uri: str = Field(min_length=1, max_length=4096)


class PublishedDatasetFile(SnapshotFileIdentity):
    """One immutable logical file after its bytes have been reopened from CAS."""

    content_uri: str = Field(min_length=1, max_length=4096)
    store_backend_hash: str = Field(min_length=64, max_length=64)

    @field_validator("store_backend_hash")
    @classmethod
    def _store_hash(cls, value: str) -> str:
        return _sha256(value, field_name="store_backend_hash")


class MaterializationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    build_id: str = Field(min_length=1, max_length=160)
    attempt_id: str = Field(min_length=1, max_length=160)
    source_identity_hash: str = Field(min_length=64, max_length=64)
    capture_set_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    files: tuple[SnapshotFileIdentity, ...] = Field(min_length=1)
    file_set_hash: str = Field(min_length=64, max_length=64)
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("source_identity_hash", "capture_set_hash", "source_revision_set_hash", "file_set_hash", "receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "MaterializationReceipt":
        if len({item.logical_path for item in self.files}) != len(self.files):
            raise ValueError("materialization receipt file paths are not unique")
        if len({item.ordinal_key for item in self.files}) != len(self.files):
            raise ValueError("materialization receipt file ordinals are not unique")
        expected_file_set = canonical_json_sha256(
            [item.canonical_identity() for item in sorted(self.files, key=lambda item: item.logical_path)]
        )
        if self.file_set_hash != expected_file_set:
            raise ValueError("materialization receipt file-set hash is invalid")
        payload = self.model_dump(mode="python", exclude={"receipt_hash"})
        digest = canonical_json_sha256(payload)
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("materialization receipt hash is invalid")
        object.__setattr__(self, "receipt_hash", digest)
        return self


class VerifiedDatasetFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    file: SnapshotFileIdentity
    observed_sha256: str = Field(min_length=64, max_length=64)
    observed_size_bytes: int = Field(gt=0)
    observed_row_count: int = Field(ge=0)
    observed_schema_fingerprint: str = Field(min_length=64, max_length=64)
    observed_partition_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("observed_sha256", "observed_schema_fingerprint", "observed_partition_content_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return _sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _exact(self) -> "VerifiedDatasetFile":
        if (
            self.file.sha256 != self.observed_sha256
            or self.file.size_bytes != self.observed_size_bytes
            or self.file.row_count != self.observed_row_count
            or self.file.schema_fingerprint != self.observed_schema_fingerprint
            or self.file.partition_content_hash != self.observed_partition_content_hash
        ):
            raise ValueError("verified file does not match immutable descriptor")
        return self


class FullParquetVerificationReceipt(BaseModel):
    """Content-only full verification receipt; no attempt/fencing/time fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    verification_contract_version: str = BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT
    build_id: str = Field(min_length=1, max_length=160)
    file_set_hash: str = Field(min_length=64, max_length=64)
    capture_set_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    selected_observation_mapping_set_hash: str = Field(min_length=64, max_length=64)
    selected_label_mapping_set_hash: str = Field(min_length=64, max_length=64)
    capability_manifest_hash: str = Field(min_length=64, max_length=64)
    files: tuple[VerifiedDatasetFile, ...] = Field(min_length=1)
    selected_observations: tuple[DatasetSnapshotObservation, ...]
    selected_labels: tuple[DatasetSnapshotLabel, ...]
    verified_content_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    relational_closure_summary: dict[str, Any]
    relational_closure_hash: str | None = Field(default=None, min_length=64, max_length=64)
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "file_set_hash", "capture_set_hash", "source_revision_set_hash",
        "selected_observation_mapping_set_hash", "selected_label_mapping_set_hash", "capability_manifest_hash",
        "verified_content_set_hash", "relational_closure_hash", "receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "FullParquetVerificationReceipt":
        if self.verification_contract_version != BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT:
            raise ValueError("full verification receipt contract is invalid")
        files = tuple(sorted(self.files, key=lambda item: item.file.logical_path))
        if len({item.file.logical_path for item in files}) != len(files):
            raise ValueError("full verification receipt file paths are not unique")
        content_set_hash = canonical_json_sha256([item.file.canonical_identity() for item in files])
        if self.verified_content_set_hash is not None and self.verified_content_set_hash != content_set_hash:
            raise ValueError("full verification content file-set hash is invalid")
        object.__setattr__(self, "verified_content_set_hash", content_set_hash)
        closure_hash = canonical_json_sha256(canonicalize(self.relational_closure_summary))
        if self.relational_closure_hash is not None and self.relational_closure_hash != closure_hash:
            raise ValueError("full verification closure hash is invalid")
        object.__setattr__(self, "relational_closure_hash", closure_hash)
        payload = self.model_dump(mode="python", exclude={"receipt_hash"})
        digest = canonical_json_sha256(payload)
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("full verification receipt hash is invalid")
        object.__setattr__(self, "receipt_hash", digest)
        return self


class DatasetCapabilityRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    component: str = Field(min_length=1, max_length=160)
    capability: str = Field(min_length=1, max_length=160)
    status: str = Field(min_length=1, max_length=80)
    reason_codes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _normalized(self) -> "DatasetCapabilityRow":
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))
        return self


class DatasetCapabilityManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rows: tuple[DatasetCapabilityRow, ...] = Field(min_length=3)
    manifest_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="manifest_hash") if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "DatasetCapabilityManifest":
        keys = {(item.component, item.capability) for item in self.rows}
        if len(keys) != len(self.rows):
            raise ValueError("capability manifest has duplicate component/capability rows")
        required = {
            ("MODEL", "MODEL_TRAINING_READY"),
            ("RUNTIME", "RUNTIME_ADVISORY_READY"),
            ("TRADING", "TRADING_EXECUTION_READY"),
        }
        values = {(row.component, row.capability): row.status for row in self.rows}
        if any(values.get(key) != "false" for key in required):
            raise ValueError("Batch D readiness capabilities must be explicitly false")
        digest = canonical_json_sha256(
            {"rows": [row.model_dump(mode="json") for row in sorted(self.rows, key=lambda item: (item.component, item.capability))]}
        )
        if self.manifest_hash is not None and self.manifest_hash != digest:
            raise ValueError("capability manifest hash is invalid")
        object.__setattr__(self, "manifest_hash", digest)
        return self


class DatasetManifestCore(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    files: tuple[DatasetSnapshotFile, ...] = Field(min_length=1)
    selected_observations: tuple[DatasetSnapshotObservation, ...]
    selected_labels: tuple[DatasetSnapshotLabel, ...]
    snapshot_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    capture_set_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_set_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    capability_manifest: DatasetCapabilityManifest
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    builder_version: str = Field(min_length=1, max_length=160)
    code_commit: str = Field(min_length=1, max_length=160)
    writer_version: str = Field(min_length=1, max_length=160)
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    policy_compatibility_hash: str = Field(min_length=64, max_length=64)
    base_snapshot: BaseSnapshotIdentity | None = None
    manifest_core_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "snapshot_source_revision_set_hash", "capture_set_hash", "handoff_readiness_hash", "admission_scope_set_hash",
        "query_registry_hash", "schema_fingerprint", "partition_policy_hash", "policy_compatibility_hash", "manifest_core_sha256",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "DatasetManifestCore":
        files = tuple(sorted(self.files, key=lambda item: item.logical_path))
        if len({item.logical_path for item in files}) != len(files):
            raise ValueError("manifest files are not unique")
        observations = tuple(sorted(self.selected_observations, key=lambda item: item.canonical_signal_id))
        labels = tuple(sorted(self.selected_labels, key=lambda item: item.label_key_hash))
        payload = {
            "files": [item.model_dump(mode="json") for item in files],
            "observations": [item.model_dump(mode="json") for item in observations],
            "labels": [item.model_dump(mode="json") for item in labels],
            "source_revision_set_hash": self.snapshot_source_revision_set_hash,
            "capture_set_hash": self.capture_set_hash,
            "base_snapshot": self.base_snapshot.model_dump(mode="json") if self.base_snapshot else None,
            "handoff_readiness_hash": self.handoff_readiness_hash,
            "admission_scope_set_hash": self.admission_scope_set_hash,
            "query_registry_hash": self.query_registry_hash,
            "capability_hash": self.capability_manifest.manifest_hash,
            "schema_fingerprint": self.schema_fingerprint,
            "builder_version": self.builder_version,
            "code_commit": self.code_commit,
            "writer_version": self.writer_version,
            "partition_policy_hash": self.partition_policy_hash,
            "policy_compatibility_hash": self.policy_compatibility_hash,
        }
        digest = canonical_json_sha256(payload)
        if self.manifest_core_sha256 is not None and self.manifest_core_sha256 != digest:
            raise ValueError("manifest core hash is invalid")
        object.__setattr__(self, "manifest_core_sha256", digest)
        return self


class DatasetManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_schema_version: str = SNAPSHOT_SCHEMA_VERSION
    core: DatasetManifestCore
    store_backend_hash: str = Field(min_length=64, max_length=64)
    manifest_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("store_backend_hash", "manifest_sha256")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="python", exclude={"manifest_sha256"}))

    @model_validator(mode="after")
    def _identity(self) -> "DatasetManifest":
        digest = hashlib.sha256(self.canonical_bytes()).hexdigest()
        if self.manifest_sha256 is not None and self.manifest_sha256 != digest:
            raise ValueError("manifest hash is invalid")
        object.__setattr__(self, "manifest_sha256", digest)
        return self


class PromotionReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    build_id: str = Field(min_length=1, max_length=160)
    full_verification_receipt_hash: str = Field(min_length=64, max_length=64)
    manifest_core_sha256: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    store_backend_hash: str = Field(min_length=64, max_length=64)
    verified_content_set_hash: str = Field(min_length=64, max_length=64)
    blobs: tuple[DatasetSnapshotFile, ...] = Field(min_length=1)
    receipt_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "full_verification_receipt_hash", "manifest_core_sha256", "manifest_sha256", "store_backend_hash",
        "verified_content_set_hash", "receipt_sha256",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="python", exclude={"receipt_sha256"}))

    @model_validator(mode="after")
    def _identity(self) -> "PromotionReceipt":
        if len({item.logical_path for item in self.blobs}) != len(self.blobs):
            raise ValueError("promotion receipt blobs are not unique")
        if any(item.blob.store_backend_hash != self.store_backend_hash for item in self.blobs):
            raise ValueError("promotion receipt blob backend differs from receipt store")
        digest = hashlib.sha256(self.canonical_bytes()).hexdigest()
        if self.receipt_sha256 is not None and self.receipt_sha256 != digest:
            raise ValueError("promotion receipt hash is invalid")
        object.__setattr__(self, "receipt_sha256", digest)
        return self


# The data roles intentionally mirror every non-operational authority column.
# Nested JSONB is stored as canonical UTF-8 JSON; SQL adapters must not drop it.
SNAPSHOT_ARROW_SCHEMAS_V1: dict[str, tuple[SnapshotSchemaField, ...]] = {
    "canonical_signals": (
        SnapshotSchemaField(name="canonical_signal_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="signal_schema_version", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="stable_signal_semantics_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="canonical_signal_scope_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="decision_as_of_trade_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="selection_as_of_trade_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="target_trade_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="decision_cutoff_ts", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="package_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="manifest_sha256", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="alpha_mode", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_runtime_semantics_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="package_effective_config_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="calendar_version", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="calendar_hash", arrow_type="utf8", nullable=False),
    ),
    "observation_versions": (
        SnapshotSchemaField(name="observation_version_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="canonical_signal_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="observation_schema_version", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="observation_revision_no", arrow_type="int32", nullable=False),
        SnapshotSchemaField(name="supersedes_observation_version_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="signal_source_revision_set_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="signal_source_revision_set_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="phase0a_signal_context_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="evidence_bundle_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="stage_evidence_bundle_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_evidence_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_evidence_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_run_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_run_content_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_score_artifact_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_score_artifact_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="runtime_profile_version_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="runtime_profile_version_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="hmm_snapshot_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="hmm_snapshot_hash", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="hmm_snapshot_status", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="risk_policy_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="universe_policy_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="symbol_normalization_policy_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="valid_no_candidate", arrow_type="bool", nullable=False),
        SnapshotSchemaField(name="observation_status", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="evidence_available_at", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="observation_content_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="reason_codes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="created_by_capture_batch_id", arrow_type="utf8", nullable=False),
    ),
    "selected_observations": (
        SnapshotSchemaField(name="selected_mapping_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selected_mapping_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="canonical_signal_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="terminal_observation_version_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="terminal_observation_content_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="terminal_revision_no", arrow_type="int32", nullable=False),
    ),
    "lineage": (
        SnapshotSchemaField(name="lineage_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="canonical_signal_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="observation_version_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="phase0a_audit_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="phase0a_audit_manifest_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="handoff_readiness_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="admission_scope_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="admission_scope_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="audit_target_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="target_scope_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="capability", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="stable_signal_semantics_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="canonical_signal_scope_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="phase0a_signal_context_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="oos_interval_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="oos_interval_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="evidence_scope", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="signal_evidence_level", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="effective_cutoff_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="program_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="binding_version_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="lineage_source_type", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="source_run_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="review_run_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="list_version_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="lineage_content_hash", arrow_type="utf8", nullable=False),
    ),
    "stage_summaries": (
        SnapshotSchemaField(name="stage_evidence_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="observation_version_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="stage", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="capability_status", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="input_count", arrow_type="int32", nullable=False),
        SnapshotSchemaField(name="output_count", arrow_type="int32", nullable=False),
        SnapshotSchemaField(name="excluded_count", arrow_type="int32", nullable=False),
        SnapshotSchemaField(name="observed_max_rank", arrow_type="int32", nullable=True),
        SnapshotSchemaField(name="source_artifact_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="source_artifact_hash", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="content_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="semantic_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="score_direction", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="tie_break_policy_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="tie_break_policy_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="reason_codes", arrow_type="list_utf8", nullable=False),
    ),
    "stage_candidates": (
        SnapshotSchemaField(name="stage_evidence_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="symbol", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="membership_status", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="rank", arrow_type="int32", nullable=True),
        SnapshotSchemaField(name="score_decimal", arrow_type="decimal38_12", nullable=True),
        SnapshotSchemaField(name="input_rank", arrow_type="int32", nullable=True),
        SnapshotSchemaField(name="input_score_decimal", arrow_type="decimal38_12", nullable=True),
        SnapshotSchemaField(name="exclusion_reason_code", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="component_capability", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="component_evidence_schema_version", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="component_evidence_json", arrow_type="canonical_json", nullable=True),
        SnapshotSchemaField(name="component_evidence_hash", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="component_reason_codes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="candidate_content_hash", arrow_type="utf8", nullable=False),
    ),
    "selected_labels": (
        SnapshotSchemaField(name="selector_request_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_policy", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selection_policy_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="label_key_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="requested_label_as_of_ts", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="terminal_label_version_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="terminal_label_content_hash", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="terminal_label_revision_no", arrow_type="int32", nullable=True),
        SnapshotSchemaField(name="terminal_maturity_status", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="terminal_outcome_event_status", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="terminal_reason_codes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="selection_status", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="reason_codes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="selected_label_mapping_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="selected_label_mapping_hash", arrow_type="utf8", nullable=False),
    ),
    "gaps": (
        SnapshotSchemaField(name="source_kind", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="gap_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="capture_batch_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="canonical_signal_id", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="audit_target_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="program_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="package_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="decision_as_of_trade_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="signal_capability", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="gap_class", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="evidence_scope", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="missing_evidence_hashes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="reason_codes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="gap_content_hash", arrow_type="utf8", nullable=False),
    ),
    "source_revisions": (
        SnapshotSchemaField(name="source_revision_set_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="source_revision_set_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="query_registry_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="requested_source_cutoff", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="label_as_of_ts", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="research_only", arrow_type="bool", nullable=False),
        SnapshotSchemaField(name="member_count", arrow_type="int32", nullable=False),
        SnapshotSchemaField(name="schema_version", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="member_key", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="source_role", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="dataset_name", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="query_template_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="query_template_version", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="query_template_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="bound_parameter_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="partition_key", arrow_type="canonical_json", nullable=False),
        SnapshotSchemaField(name="partition_key_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="revision_kind", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="revision_id", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="availability_event_hash", arrow_type="utf8", nullable=True),
        SnapshotSchemaField(name="availability_requirement", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="business_min_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="business_max_date", arrow_type="date32", nullable=False),
        SnapshotSchemaField(name="available_at_min", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="available_at_max", arrow_type="timestamp_us_utc", nullable=False),
        SnapshotSchemaField(name="schema_fingerprint", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="row_count", arrow_type="int64", nullable=False),
        SnapshotSchemaField(name="partition_content_hash", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="quality_status", arrow_type="utf8", nullable=False),
        SnapshotSchemaField(name="reason_codes", arrow_type="list_utf8", nullable=False),
        SnapshotSchemaField(name="enforced_cutoff_predicate_hash", arrow_type="utf8", nullable=False),
    ),
}

# Outcome labels and universe outcomes share one authority header/payload shape.
_OUTCOME_LABEL_FIELDS = (
    "label_version_id", "label_content_hash", "label_key_hash", "label_revision_no", "supersedes_label_version_id",
    "supersedes_label_version_hash", "label_append_request_hash", "label_policy_bundle_id", "label_policy_bundle_hash",
    "label_policy_hash", "label_source_revision_set_id", "label_source_revision_set_hash", "owner_type", "owner_key",
    "canonical_signal_id", "observation_version_id", "candidate_stage_evidence_id", "symbol", "universe_layer",
    "decision_as_of_trade_date", "evidence_scope", "horizon_trading_days", "projection", "projection_schema_version",
    "intended_entry_trade_date", "earliest_sell_eligible_trade_date", "exit_trade_date", "maturity_status",
    "outcome_event_status", "entry_status", "projection_payload_hash", "calculation_evidence_sha256",
    "calculation_evidence_size_bytes", "calculation_evidence_store_backend_hash", "created_by_capture_batch_id", "computed_at",
    "scheduled_maturity_ts", "source_closed_at", "event_closed_at", "failure_observed_at", "missing_source_receipt_hash",
    "projection_value_decimal", "projection_event_code", "entry_price_raw_yuan", "entry_adj_factor", "exit_price_raw_yuan",
    "exit_adj_factor", "entry_quantity", "exit_quantity", "buy_execution_price_yuan", "sell_execution_price_yuan",
    "buy_notional_yuan", "sell_notional_yuan", "buy_fee_yuan", "sell_fee_yuan", "entry_cash_yuan", "residual_cash_yuan",
    "exit_cash_yuan", "terminal_value_yuan", "cost_breakdown_hash", "benchmark_gross_total_return",
    "benchmark_net_total_return", "entry_day_touch_status", "executable_barrier_status", "executable_event_trade_date",
    "time_to_executable_hit_trading_days", "observed_holding_trading_days", "terminal_disposition", "terminal_symbol",
    "terminal_event_trade_date", "terminal_event_closed_at", "terminal_source_hash", "terminal_settlement_raw_li",
    "terminal_settlement_adj_factor", "terminal_settlement_quantity_multiplier", "terminal_settlement_cashflow_yuan_per_share",
    "censor_reason_code", "policy_bundle_hash", "price_path_hash", "corporate_actions_hash", "benchmark_bundle_hash",
    "formula_schema_version", "calculation_evidence_schema_version", "calculation_evidence_uri",
    "reason_codes",
)
_OUTCOME_DECIMAL_FIELDS = {
    "projection_value_decimal", "entry_price_raw_yuan", "entry_adj_factor", "exit_price_raw_yuan", "exit_adj_factor",
    "entry_quantity", "exit_quantity", "buy_execution_price_yuan", "sell_execution_price_yuan", "buy_notional_yuan",
    "sell_notional_yuan", "buy_fee_yuan", "sell_fee_yuan", "entry_cash_yuan", "residual_cash_yuan", "exit_cash_yuan",
    "terminal_value_yuan", "benchmark_gross_total_return", "benchmark_net_total_return", "terminal_settlement_raw_li",
    "terminal_settlement_adj_factor", "terminal_settlement_quantity_multiplier", "terminal_settlement_cashflow_yuan_per_share",
}
_OUTCOME_DATE_FIELDS = {
    "decision_as_of_trade_date", "intended_entry_trade_date", "earliest_sell_eligible_trade_date", "exit_trade_date",
    "executable_event_trade_date", "terminal_event_trade_date",
}
_OUTCOME_TIMESTAMP_FIELDS = {
    "computed_at", "scheduled_maturity_ts", "source_closed_at", "event_closed_at", "failure_observed_at", "terminal_event_closed_at",
}
_OUTCOME_INT32_FIELDS = {"label_revision_no", "horizon_trading_days", "time_to_executable_hit_trading_days", "observed_holding_trading_days"}
_OUTCOME_INT64_FIELDS = {"calculation_evidence_size_bytes"}


def _outcome_field(name: str) -> SnapshotSchemaField:
    if name == "reason_codes":
        return SnapshotSchemaField(name=name, arrow_type="list_utf8", nullable=False)
    if name in _OUTCOME_DECIMAL_FIELDS:
        return SnapshotSchemaField(name=name, arrow_type="decimal38_12", nullable=True)
    if name in _OUTCOME_DATE_FIELDS:
        return SnapshotSchemaField(name=name, arrow_type="date32", nullable=name in {"exit_trade_date", "executable_event_trade_date", "terminal_event_trade_date"})
    if name in _OUTCOME_TIMESTAMP_FIELDS:
        return SnapshotSchemaField(name=name, arrow_type="timestamp_us_utc", nullable=name not in {"computed_at", "scheduled_maturity_ts"})
    if name in _OUTCOME_INT32_FIELDS:
        return SnapshotSchemaField(
            name=name,
            arrow_type="int32",
            nullable=name in {"time_to_executable_hit_trading_days", "observed_holding_trading_days"},
        )
    if name in _OUTCOME_INT64_FIELDS:
        return SnapshotSchemaField(name=name, arrow_type="int64", nullable=False)
    nullable = name in {
        "supersedes_label_version_id", "supersedes_label_version_hash", "observation_version_id", "candidate_stage_evidence_id",
        "universe_layer", "missing_source_receipt_hash", "projection_event_code", "cost_breakdown_hash",
        "entry_day_touch_status", "executable_barrier_status", "terminal_disposition", "terminal_symbol", "terminal_source_hash",
        "censor_reason_code", "benchmark_bundle_hash",
    }
    return SnapshotSchemaField(name=name, arrow_type="utf8", nullable=nullable)


SNAPSHOT_ARROW_SCHEMAS_V1["outcome_labels"] = tuple(_outcome_field(name) for name in _OUTCOME_LABEL_FIELDS)
SNAPSHOT_ARROW_SCHEMAS_V1["universe_outcomes"] = tuple(_outcome_field(name) for name in _OUTCOME_LABEL_FIELDS)
SNAPSHOT_ARROW_SCHEMAS_V1["outcome_source_evidence"] = (
    SnapshotSchemaField(name="owner_type", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="label_version_id", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="label_key_hash", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="canonical_signal_id", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="symbol", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="horizon_trading_days", arrow_type="int32", nullable=False),
    SnapshotSchemaField(name="projection", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="calculation_evidence_sha256", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="calculation_evidence_size_bytes", arrow_type="int64", nullable=False),
    SnapshotSchemaField(name="calculation_evidence_store_backend_hash", arrow_type="utf8", nullable=False),
    SnapshotSchemaField(name="calculation_evidence_json", arrow_type="canonical_json", nullable=False),
)


def _capture_source_revision_identity(request: Any) -> tuple[str, str]:
    from backend.services.advisory_phase1.capture_foundation import CaptureBatchRequest
    from backend.services.advisory_phase1.label_capture import LabelCaptureBatchRequestV2

    if isinstance(request, LabelCaptureBatchRequestV2):
        return request.label_source_revision_set_id, request.label_source_revision_set_hash
    if isinstance(request, CaptureBatchRequest):
        identities = {
            (plan.signal_source_revision_set_id, plan.signal_source_revision_set_hash)
            for plan in request.plans
        }
        if len(identities) == 1:
            return next(iter(identities))
    raise SnapshotWriterError(
        REASON_SOURCE_SNAPSHOT_CONFLICT,
        "capture request does not freeze exactly one source revision identity",
    )


def _load_persisted_capture_request_read_only(cur: Any, row: Mapping[str, Any]) -> Any:
    from backend.services.advisory_phase1.capture_foundation import (
        CaptureBatchRequest,
        CapturePlan,
        capture_request_hash,
    )
    from backend.services.advisory_phase1.label_capture import LabelCaptureBatchRequestV2
    from backend.services.advisory_phase1.stage_trace import TraceCaptureBinding

    purpose = str(row["capture_purpose"])
    payload = canonicalize(dict(row["request_payload_jsonb"]))
    binding = canonicalize(dict(row["binding_jsonb"]))
    if purpose == "OBSERVATION_CAPTURE_V1":
        cur.execute(
            "SELECT plan_payload_jsonb FROM app.advisory_capture_plan "
            "WHERE capture_batch_id = %s ORDER BY plan_hash",
            (row["capture_batch_id"],),
        )
        plans = tuple(
            CapturePlan.model_validate(canonicalize(dict(item["plan_payload_jsonb"])))
            for item in cur.fetchall()
        )
        if not plans:
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "observation capture has no persisted plans")
        request = CaptureBatchRequest(
            capture_batch_id=str(row["capture_batch_id"]),
            binding=TraceCaptureBinding.model_validate(binding),
            plans=plans,
            capture_request_hash=str(row["capture_request_hash"]),
        )
    elif purpose == "LABEL_CAPTURE_V1":
        payload["binding"] = binding
        payload["capture_batch_id"] = str(row["capture_batch_id"])
        payload["capture_request_hash"] = str(row["capture_request_hash"])
        request = LabelCaptureBatchRequestV2.model_validate(payload)
    else:
        raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "capture purpose is unsupported")
    if capture_request_hash(request) != str(row["capture_request_hash"]):
        raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "persisted capture request hash is invalid")
    return request


class PostgresSnapshotSourceReader:
    """Read one frozen Batch D logical row set from a single database snapshot."""

    def __init__(self, *, conn_factory: Any, evidence_reader: Any) -> None:
        self._conn_factory = conn_factory
        self._evidence_reader = evidence_reader

    def read(self, build: DatasetBuild) -> dict[str, DiskBackedRows]:
        rows = {role: DiskBackedRows() for role in SNAPSHOT_ARROW_SCHEMAS_V1}
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor() as control:
                control.execute("SET LOCAL statement_timeout = '30min'")
                control.execute("SET LOCAL idle_in_transaction_session_timeout = '5min'")
                self._validate_authority_columns(control)
            try:
                initial_authority_hash = self._authority_summary_hash(conn=conn, build=build)
                selected_observations = self._selected_observations(conn=conn, build=build)
                signal_ids = [item["canonical_signal_id"] for item in selected_observations]
                signal_rows = self._query(
                    conn,
                    """
                    SELECT canonical_signal_id, signal_schema_version, stable_signal_semantics_hash,
                           canonical_signal_scope_hash, decision_as_of_trade_date, selection_as_of_trade_date,
                           target_trade_date, decision_cutoff_ts, package_id, manifest_sha256, alpha_mode,
                           selection_runtime_semantics_hash, package_effective_config_hash, calendar_version, calendar_hash
                      FROM app.advisory_signal_observation
                     WHERE canonical_signal_id = ANY(%s)
                     ORDER BY decision_as_of_trade_date, canonical_signal_id
                    """,
                    (signal_ids,),
                    name="batchd_signals",
                )
                if {item["canonical_signal_id"] for item in signal_rows} != set(signal_ids):
                    raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "frozen selected signals are missing")
                signal_dates = {item["canonical_signal_id"]: item["decision_as_of_trade_date"] for item in signal_rows}
                rows["canonical_signals"].extend(self._logical("canonical_signals", item) for item in signal_rows)
                rows["selected_observations"].extend(
                    self._logical("selected_observations", item) for item in selected_observations
                )

                observation_rows = self._query(
                    conn,
                    """
                    SELECT observation_version_id, canonical_signal_id, observation_schema_version,
                           observation_revision_no, supersedes_observation_version_id, signal_source_revision_set_id,
                           signal_source_revision_set_hash, phase0a_signal_context_hash, evidence_bundle_hash,
                           stage_evidence_bundle_hash, selection_evidence_id, selection_evidence_hash, selection_run_id,
                           selection_run_content_hash, selection_score_artifact_id, selection_score_artifact_hash,
                           runtime_profile_version_id, runtime_profile_version_hash, hmm_snapshot_id, hmm_snapshot_hash,
                           hmm_snapshot_status, risk_policy_hash, universe_policy_hash, symbol_normalization_policy_hash,
                           valid_no_candidate, observation_status, evidence_available_at, observation_content_hash,
                           reason_codes, created_by_capture_batch_id
                      FROM app.advisory_signal_observation_version
                     WHERE canonical_signal_id = ANY(%s)
                     ORDER BY canonical_signal_id, observation_revision_no, observation_version_id
                    """,
                    (signal_ids,),
                    name="batchd_observations",
                )
                observation_dates = {
                    item["observation_version_id"]: signal_dates[item["canonical_signal_id"]] for item in observation_rows
                }
                rows["observation_versions"].extend(
                    self._logical("observation_versions", item, decision_date=observation_dates[item["observation_version_id"]])
                    for item in observation_rows
                )
                observation_ids = [item["observation_version_id"] for item in observation_rows]
                lineage_rows = self._query(
                    conn,
                    "SELECT * FROM app.advisory_signal_observation_lineage WHERE observation_version_id = ANY(%s) "
                    "ORDER BY canonical_signal_id, observation_version_id, lineage_id",
                    (observation_ids,),
                    name="batchd_lineage",
                )
                rows["lineage"].extend(
                    self._logical("lineage", item, decision_date=observation_dates[item["observation_version_id"]])
                    for item in lineage_rows
                )
                stage_rows = self._query(
                    conn,
                    "SELECT * FROM app.advisory_signal_stage_evidence WHERE observation_version_id = ANY(%s) "
                    "ORDER BY observation_version_id, stage, stage_evidence_id",
                    (observation_ids,),
                    name="batchd_stages",
                )
                stage_dates = {item["stage_evidence_id"]: observation_dates[item["observation_version_id"]] for item in stage_rows}
                rows["stage_summaries"].extend(
                    self._logical("stage_summaries", item, decision_date=stage_dates[item["stage_evidence_id"]])
                    for item in stage_rows
                )
                stage_ids = [item["stage_evidence_id"] for item in stage_rows]
                candidate_rows = self._query(
                    conn,
                    "SELECT * FROM app.advisory_signal_stage_candidate WHERE stage_evidence_id = ANY(%s) "
                    "ORDER BY stage_evidence_id, symbol",
                    (stage_ids,),
                    name="batchd_candidates",
                )
                rows["stage_candidates"].extend(
                    self._logical("stage_candidates", item, decision_date=stage_dates[item["stage_evidence_id"]])
                    for item in candidate_rows
                )

                outcome_rows = self._outcome_rows(conn=conn, build=build)
                rows["outcome_labels"].extend(self._logical("outcome_labels", item) for item in outcome_rows)
                rows["universe_outcomes"].extend(
                    self._logical("universe_outcomes", item) for item in outcome_rows if item["owner_type"] == "UNIVERSE"
                )
                rows["outcome_source_evidence"].extend(self._outcome_evidence_row(item) for item in outcome_rows)
                selected_labels = self._selected_labels(build=build, outcome_rows=outcome_rows)
                outcomes_by_version = {item["label_version_id"]: item for item in outcome_rows}
                rows["selected_labels"].extend(
                    self._logical(
                        "selected_labels",
                        item,
                        outcome=outcomes_by_version[item["terminal_label_version_id"]],
                    )
                    for item in selected_labels
                )

                capture_ids = [item.capture_batch_id for item in build.request.captures]
                gap_rows = self._query(
                    conn,
                    """
                    SELECT 'DATASET_GAP' AS source_kind, gap_id, capture_batch_id, canonical_signal_id,
                           audit_target_id, program_id, package_id, decision_as_of_trade_date, signal_capability,
                           gap_class, evidence_scope, missing_evidence_hashes, reason_codes, gap_content_hash
                      FROM app.advisory_dataset_build_gap
                     WHERE capture_batch_id = ANY(%s)
                       AND decision_as_of_trade_date BETWEEN %s AND %s
                     ORDER BY decision_as_of_trade_date, source_kind, gap_id
                    """,
                    (capture_ids, build.request.date_start, build.request.date_end),
                    name="batchd_gaps",
                )
                rows["gaps"].extend(self._logical("gaps", item) for item in gap_rows)
                source_rows = self._query(
                    conn,
                    """
                    SELECT s.source_revision_set_id, s.source_revision_set_hash, s.query_registry_hash,
                           s.requested_source_cutoff, s.label_as_of_ts, s.research_only, s.member_count,
                           s.schema_version, m.member_key, m.source_role, m.dataset_name, m.query_template_id,
                           m.query_template_version, m.query_template_hash, m.bound_parameter_hash, m.partition_key,
                           m.partition_key_hash, m.revision_kind, m.revision_id, m.availability_event_hash,
                           m.availability_requirement, m.business_min_date, m.business_max_date,
                           m.available_at_min, m.available_at_max, m.schema_fingerprint, m.row_count,
                           m.partition_content_hash, m.quality_status, m.reason_codes, m.enforced_cutoff_predicate_hash
                      FROM app.advisory_source_revision_set s
                      JOIN app.advisory_source_revision_member m USING (source_revision_set_id)
                     WHERE s.source_revision_set_id = %s AND s.source_revision_set_hash = %s
                     ORDER BY s.source_revision_set_id, m.member_key
                    """,
                    (build.request.snapshot_source_revision_set_id, build.request.snapshot_source_revision_set_hash),
                    name="batchd_source_revisions",
                )
                rows["source_revisions"].extend(self._logical("source_revisions", item) for item in source_rows)
                if self._authority_summary_hash(conn=conn, build=build) != initial_authority_hash:
                    raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "frozen authority summary changed in source view")
                self._validate_stream_counts(
                    conn=conn,
                    build=build,
                    rows=rows,
                    signal_ids=signal_ids,
                    observation_ids=observation_ids,
                    stage_ids=stage_ids,
                )
                self._check_rss()
                conn.rollback()
                return rows
            except Exception:
                conn.rollback()
                for role_rows in rows.values():
                    role_rows.close()
                raise

    @staticmethod
    def _authority_summary_hash(*, conn: Any, build: DatasetBuild) -> str:
        import psycopg2.extras

        capture_ids = [item.capture_batch_id for item in build.request.captures]
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                  FROM app.advisory_capture_batch
                 WHERE capture_batch_id = ANY(%s)
                 ORDER BY capture_batch_id
                """,
                (capture_ids,),
            )
            capture_rows = [dict(row) for row in cur.fetchall()]
            expected = {item.capture_batch_id: item for item in build.request.captures}
            if {row["capture_batch_id"] for row in capture_rows} != set(expected):
                raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "frozen captures are missing")
            summary_captures: list[dict[str, Any]] = []
            compared_fields = (
                "capture_request_hash",
                "capture_receipt_hash",
                "membership_hash",
                "capture_purpose",
                "handoff_readiness_hash",
                "admission_scope_id",
                "admission_scope_hash",
                "source_revision_set_id",
                "source_revision_set_hash",
            )
            for row in capture_rows:
                member = expected[row["capture_batch_id"]]
                parsed = _load_persisted_capture_request_read_only(cur, row)
                source_revision_set_id, source_revision_set_hash = _capture_source_revision_identity(parsed)
                row["source_revision_set_id"] = source_revision_set_id
                row["source_revision_set_hash"] = source_revision_set_hash
                if (
                    row["capture_status"] != "COMPLETE"
                    or parsed.capture_request_hash != row["capture_request_hash"]
                    or any(row[field] != getattr(member, field) for field in compared_fields)
                ):
                    raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "frozen capture authority differs from build")
                summary_captures.append(
                    {
                        "capture_batch_id": row["capture_batch_id"],
                        **{field: row[field] for field in compared_fields},
                    }
                )
            cur.execute(
                """
                SELECT source_revision_set_id, source_revision_set_hash, query_registry_hash,
                       requested_source_cutoff, label_as_of_ts, research_only, member_count, schema_version
                  FROM app.advisory_source_revision_set
                 WHERE source_revision_set_id = %s AND source_revision_set_hash = %s
                """,
                (
                    build.request.snapshot_source_revision_set_id,
                    build.request.snapshot_source_revision_set_hash,
                ),
            )
            source = cur.fetchone()
            if source is None or source["query_registry_hash"] != build.request.query_registry_hash:
                raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "snapshot source revision authority is missing")
            cur.execute(
                "SELECT count(*) AS member_count FROM app.advisory_source_revision_member "
                "WHERE source_revision_set_id = %s",
                (build.request.snapshot_source_revision_set_id,),
            )
            actual_member_count = int(cur.fetchone()["member_count"])
            if actual_member_count != int(source["member_count"]):
                raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "source revision membership count differs")
        return canonical_json_sha256(
            {
                "captures": summary_captures,
                "source_revision": dict(source),
                "actual_source_member_count": actual_member_count,
            }
        )
    @staticmethod
    def _validate_stream_counts(
        *,
        conn: Any,
        build: DatasetBuild,
        rows: Mapping[str, DiskBackedRows],
        signal_ids: Sequence[str],
        observation_ids: Sequence[str],
        stage_ids: Sequence[str],
    ) -> None:
        label_capture_ids = [
            item.capture_batch_id for item in build.request.captures if item.capture_purpose == "LABEL_CAPTURE_V1"
        ]
        capture_ids = [item.capture_batch_id for item in build.request.captures]
        queries = {
            "canonical_signals": (
                "SELECT count(*) FROM app.advisory_signal_observation WHERE canonical_signal_id = ANY(%s)",
                (list(signal_ids),),
            ),
            "observation_versions": (
                "SELECT count(*) FROM app.advisory_signal_observation_version WHERE canonical_signal_id = ANY(%s)",
                (list(signal_ids),),
            ),
            "lineage": (
                "SELECT count(*) FROM app.advisory_signal_observation_lineage WHERE observation_version_id = ANY(%s)",
                (list(observation_ids),),
            ),
            "stage_summaries": (
                "SELECT count(*) FROM app.advisory_signal_stage_evidence WHERE observation_version_id = ANY(%s)",
                (list(observation_ids),),
            ),
            "stage_candidates": (
                "SELECT count(*) FROM app.advisory_signal_stage_candidate WHERE stage_evidence_id = ANY(%s)",
                (list(stage_ids),),
            ),
            "outcome_labels": (
                "SELECT count(*) FROM app.advisory_outcome_label WHERE created_by_capture_batch_id = ANY(%s) "
                "AND decision_as_of_trade_date BETWEEN %s AND %s",
                (label_capture_ids, build.request.date_start, build.request.date_end),
            ),
            "universe_outcomes": (
                "SELECT count(*) FROM app.advisory_outcome_label WHERE created_by_capture_batch_id = ANY(%s) "
                "AND decision_as_of_trade_date BETWEEN %s AND %s AND owner_type = 'UNIVERSE'",
                (label_capture_ids, build.request.date_start, build.request.date_end),
            ),
            "gaps": (
                "SELECT count(*) FROM app.advisory_dataset_build_gap WHERE capture_batch_id = ANY(%s) "
                "AND decision_as_of_trade_date BETWEEN %s AND %s",
                (capture_ids, build.request.date_start, build.request.date_end),
            ),
            "source_revisions": (
                "SELECT count(*) FROM app.advisory_source_revision_member WHERE source_revision_set_id = %s",
                (build.request.snapshot_source_revision_set_id,),
            ),
        }
        with conn.cursor() as cur:
            for role, (sql, params) in queries.items():
                cur.execute(sql, params)
                if int(cur.fetchone()[0]) != len(rows[role]):
                    raise SnapshotWriterError(
                        REASON_SOURCE_SNAPSHOT_CONFLICT,
                        f"final authority count differs from streamed role: {role}",
                    )
        if (
            len(rows["selected_observations"]) != len(build.request.selected_observation_mappings)
            or len(rows["selected_labels"]) != len(build.request.selected_label_mappings)
            or len(rows["outcome_source_evidence"]) != len(rows["outcome_labels"])
        ):
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "derived stream counts differ from frozen mappings")

    def _selected_observations(self, *, conn: Any, build: DatasetBuild) -> list[dict[str, Any]]:
        from backend.services.advisory_phase1.label_capture import LabelCaptureBatchRequestV2

        label_capture_ids = [
            item.capture_batch_id for item in build.request.captures if item.capture_purpose == "LABEL_CAPTURE_V1"
        ]
        persisted_requests = self._load_persisted_capture_requests(conn, label_capture_ids)
        result: dict[str, dict[str, Any]] = {}
        for persisted, request in persisted_requests:
            if not isinstance(request, LabelCaptureBatchRequestV2) or request.capture_request_hash != persisted["capture_request_hash"]:
                raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "label capture request payload is invalid")
            for reference in request.selected_observation_mappings:
                payload = reference.model_dump(mode="python")
                existing = result.get(reference.selected_mapping_id)
                if existing is not None and existing != payload:
                    raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "selected observation mapping conflicts across captures")
                result[reference.selected_mapping_id] = payload
        expected = {
            item.identity_id: item.identity_hash for item in build.request.selected_observation_mappings
        }
        if {key: value["selected_mapping_hash"] for key, value in result.items()} != expected:
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "selected observation mapping set differs from build")
        return [result[key] for key in sorted(result, key=lambda item: result[item]["canonical_signal_id"])]

    @staticmethod
    def _load_persisted_capture_requests(
        conn: Any,
        capture_batch_ids: Sequence[str],
    ) -> list[tuple[dict[str, Any], Any]]:
        import psycopg2.extras

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM app.advisory_capture_batch "
                "WHERE capture_batch_id = ANY(%s) AND capture_status = 'COMPLETE' ORDER BY capture_batch_id",
                (list(capture_batch_ids),),
            )
            return [
                (dict(row), _load_persisted_capture_request_read_only(cur, dict(row)))
                for row in cur.fetchall()
            ]

    def _outcome_rows(self, *, conn: Any, build: DatasetBuild) -> DiskBackedRows:
        label_capture_ids = [
            item.capture_batch_id for item in build.request.captures if item.capture_purpose == "LABEL_CAPTURE_V1"
        ]
        rows = self._query(
            conn,
            """
            SELECT h.*, p.*
              FROM app.advisory_outcome_label h
              JOIN app.advisory_outcome_label_payload p
                ON p.label_version_id = h.label_version_id
               AND p.decision_as_of_trade_date = h.decision_as_of_trade_date
             WHERE h.created_by_capture_batch_id = ANY(%s)
               AND h.decision_as_of_trade_date BETWEEN %s AND %s
             ORDER BY h.label_key_hash, h.label_revision_no, h.decision_as_of_trade_date
            """,
            (label_capture_ids, build.request.date_start, build.request.date_end),
            name="batchd_outcomes",
        )
        return DiskBackedRows({field: row[field] for field in _OUTCOME_LABEL_FIELDS} for row in rows)

    def _outcome_evidence_row(self, outcome: Mapping[str, Any]) -> LogicalDatasetRow:
        try:
            bundle = self._evidence_reader.get(
                uri=outcome["calculation_evidence_uri"],
                sha256=outcome["calculation_evidence_sha256"],
                size_bytes=outcome["calculation_evidence_size_bytes"],
                store_backend_hash=outcome["calculation_evidence_store_backend_hash"],
            )
        except Exception as error:
            raise SnapshotWriterError(REASON_EVIDENCE_BLOB_INVALID, "calculation evidence readback failed") from error
        values = {
            "owner_type": outcome["owner_type"],
            "label_version_id": outcome["label_version_id"],
            "label_key_hash": outcome["label_key_hash"],
            "canonical_signal_id": outcome["canonical_signal_id"],
            "symbol": outcome["symbol"],
            "horizon_trading_days": outcome["horizon_trading_days"],
            "projection": outcome["projection"],
            "calculation_evidence_sha256": outcome["calculation_evidence_sha256"],
            "calculation_evidence_size_bytes": outcome["calculation_evidence_size_bytes"],
            "calculation_evidence_store_backend_hash": outcome["calculation_evidence_store_backend_hash"],
            "calculation_evidence_json": bundle.model_dump(mode="json"),
        }
        return self._logical("outcome_source_evidence", values, outcome=outcome)

    def _selected_labels(self, *, build: DatasetBuild, outcome_rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        from backend.services.advisory_phase1.label_builder import (
            LabelSelectionPolicy,
            LabelSelectionRequest,
            TerminalFirstLabelSelector,
        )
        from backend.services.advisory_phase1.label_builder_postgres import PostgresOutcomeLabelRepository
        from backend.services.advisory_phase1.outcome_engine import MaturityStatus, OutcomeEventStatus

        adapter = PostgresOutcomeLabelRepository(evidence_reader=self._evidence_reader)
        expected = {item.identity_id: item.identity_hash for item in build.request.selected_label_mappings}
        matched: dict[str, dict[str, Any]] = {}
        maturity_values = tuple(MaturityStatus)
        event_values = tuple(OutcomeEventStatus)
        selector = TerminalFirstLabelSelector()
        grouped = itertools.groupby(outcome_rows, key=lambda item: item["label_key_hash"])
        for _, group in grouped:
            versions = sorted((adapter._from_row(row) for row in group), key=lambda item: item.label_revision_no)
            terminal = tuple(item for item in versions if item.computed_at <= build.request.label_as_of_ts)
            if not terminal:
                continue
            anchor = terminal[-1]
            if anchor.owner.observation_version_id is None or anchor.owner.candidate_stage_evidence_id is None:
                continue
            for maturity_count in range(1, len(maturity_values) + 1):
                for maturities in itertools.combinations(maturity_values, maturity_count):
                    for event_count in range(1, len(event_values) + 1):
                        for events in itertools.combinations(event_values, event_count):
                            for policy in LabelSelectionPolicy:
                                request = LabelSelectionRequest(
                                    selection_policy=policy,
                                    label_key_hash=anchor.label_key_hash,
                                    requested_label_as_of_ts=build.request.label_as_of_ts,
                                    required_maturity_statuses=maturities,
                                    required_outcome_event_statuses=events,
                                    required_projection_schema_version=anchor.projection_schema_version,
                                    expected_observation_version_id=anchor.owner.observation_version_id,
                                    expected_candidate_stage_evidence_id=anchor.owner.candidate_stage_evidence_id,
                                    expected_label_source_revision_set_hash=anchor.label_source_revision_set_hash,
                                    explicit_label_version_id=(anchor.label_version_id if policy is LabelSelectionPolicy.EXACT_REVISION_V1 else None),
                                )
                                mapping = selector.select(request=request, label_versions=versions)
                                mapping_id = str(mapping.selected_label_mapping_id)
                                if expected.get(mapping_id) == mapping.selected_label_mapping_hash:
                                    payload = mapping.model_dump(mode="python")
                                    existing = matched.get(mapping_id)
                                    if existing is not None and existing != payload:
                                        raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "selected label mapping is ambiguous")
                                    matched[mapping_id] = payload
        if set(matched) != set(expected):
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "selected label mappings cannot be reconstructed")
        return [matched[key] for key in sorted(matched, key=lambda item: matched[item]["label_key_hash"])]

    def _logical(
        self,
        logical_role: str,
        raw: Mapping[str, Any],
        *,
        decision_date: date | None = None,
        outcome: Mapping[str, Any] | None = None,
    ) -> LogicalDatasetRow:
        values = {field.name: raw[field.name] for field in SNAPSHOT_ARROW_SCHEMAS_V1[logical_role]}
        normalized = _coerce_values(logical_role=logical_role, values=values)
        partition = _partition_key_for_values(
            logical_role=logical_role,
            values=normalized,
            decision_date=decision_date,
            outcome=outcome,
        )
        return LogicalDatasetRow(
            logical_role=logical_role,
            partition_key=partition,
            sort_key=_logical_sort_key(logical_role, normalized),
            values=normalized,
        )

    @staticmethod
    def _query(conn: Any, sql: str, params: tuple[Any, ...], *, name: str) -> DiskBackedRows:
        import psycopg2.extras

        result = DiskBackedRows()
        try:
            with conn.cursor(name=name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.itersize = 10000
                cur.execute(sql, params)
                while True:
                    batch = cur.fetchmany(10000)
                    if not batch:
                        break
                    result.extend(dict(item) for item in batch)
                    PostgresSnapshotSourceReader._check_rss()
        except Exception:
            result.close()
            raise
        return result

    @staticmethod
    def _validate_authority_columns(cur: Any) -> None:
        table_fields = {
            "advisory_signal_observation": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["canonical_signals"]},
            "advisory_signal_observation_version": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["observation_versions"]},
            "advisory_signal_observation_lineage": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["lineage"]},
            "advisory_signal_stage_evidence": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["stage_summaries"]},
            "advisory_signal_stage_candidate": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["stage_candidates"]},
            "advisory_dataset_build_gap": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["gaps"]} - {"source_kind"},
        }
        tables = tuple(table_fields)
        cur.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = ANY(%s)",
            (list(tables),),
        )
        actual: dict[str, set[str]] = {table: set() for table in tables}
        for table_name, column_name in cur.fetchall():
            actual[str(table_name)].add(str(column_name))
        for table, expected in table_fields.items():
            if actual[table] - {"created_at"} != expected:
                raise SnapshotWriterError(
                    REASON_ARROW_SCHEMA_CONFLICT,
                    f"authority columns differ from frozen Arrow schema: {table}",
                )
        for label, source_tables, expected in (
            (
                "outcome_label",
                ("advisory_outcome_label", "advisory_outcome_label_payload"),
                set(_OUTCOME_LABEL_FIELDS),
            ),
            (
                "source_revision",
                ("advisory_source_revision_set", "advisory_source_revision_member"),
                {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["source_revisions"]},
            ),
        ):
            cur.execute(
                "SELECT DISTINCT column_name FROM information_schema.columns "
                "WHERE table_schema = 'app' AND table_name = ANY(%s)",
                (list(source_tables),),
            )
            union = {str(row[0]) for row in cur.fetchall()} - {"created_at"}
            if union != expected:
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"{label} authority columns differ from frozen schema")

    @staticmethod
    def _check_rss() -> None:
        import psutil

        if psutil.Process(os.getpid()).memory_info().rss > 2 * 1024 * 1024 * 1024:
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "Batch D materialization RSS exceeds 2 GiB")


class DescriptorCalculationEvidenceReader:
    """Resolve the existing calculation-evidence store from its canonical DB URI."""

    def __init__(self, *, repository_root: Path) -> None:
        self._repository_root = repository_root.resolve()
        self._stores: dict[tuple[str, str], Any] = {}

    def get(self, *, uri: str, sha256: str, size_bytes: int, store_backend_hash: str) -> Any:
        from backend.services.advisory_phase1.calculation_evidence import LocalCalculationEvidenceStore

        path = _path_from_file_uri(uri)
        expected_tail = ("blobs", "sha256", sha256[:2], sha256)
        if len(path.parts) < 4 or tuple(path.parts[-4:]) != expected_tail:
            raise SnapshotWriterError(REASON_EVIDENCE_BLOB_INVALID, "calculation evidence URI is not canonical")
        root = Path(*path.parts[:-4]).resolve()
        identity = {
            "backend": "LOCAL_FILESYSTEM_V1",
            "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
            "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
        }
        key = (str(root), store_backend_hash)
        store = self._stores.get(key)
        if store is None:
            store = LocalCalculationEvidenceStore(
                root=root,
                repository_root=self._repository_root,
                store_identity=identity,
            )
            if store.store_backend_hash != store_backend_hash:
                raise SnapshotWriterError(REASON_EVIDENCE_BLOB_INVALID, "calculation evidence store identity differs from DB")
            self._stores[key] = store
        return store.get(
            uri=uri,
            sha256=sha256,
            size_bytes=size_bytes,
            store_backend_hash=store_backend_hash,
        )


class DatasetSnapshotMaterializer:
    """Write the complete role set from one already-frozen source snapshot."""

    def __init__(self, *, source_reader: PostgresSnapshotSourceReader, writer: "DeterministicParquetWriter") -> None:
        self._source_reader = source_reader
        self._writer = writer

    def materialize(
        self,
        *,
        build: DatasetBuild,
        attempt_id: str,
        store: LocalContentAddressedStore,
        base_files: Sequence[DatasetSnapshotFile] = (),
    ) -> tuple[tuple[WrittenDatasetFile, ...], MaterializationReceipt]:
        if build.request.builder_version != BATCH_D_BUILDER_VERSION or build.request.writer_version != BATCH_D_WRITER_VERSION:
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "build request writer/builder version differs from Batch D")
        if build.request.compression_config != {"codec": "zstd", "level": 3}:
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "Batch D v1 compression config is not frozen")
        rows_by_role = self._source_reader.read(build)
        partition_spool = LogicalRowPartitionSpool()
        try:
            logical_source_bytes = 0
            for role, source_rows in rows_by_role.items():
                for row in source_rows:
                    if row.logical_role != role:
                        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "source reader returned a row under the wrong role")
                    logical_source_bytes += len(_canonical_json_bytes(row.canonical_identity()))
                    partition_spool.append(row)
            partition_spool.commit()
            store.ensure_capacity(logical_source_bytes=logical_source_bytes)
            files: list[WrittenDatasetFile] = []
            base_by_path = {file.logical_path: file for file in base_files}
            for role in sorted(SNAPSHOT_ARROW_SCHEMAS_V1):
                descriptor_path = store.staging_path(
                    build_id=build.build_id,
                    attempt_id=attempt_id,
                    logical_path=f"schemas/{role}.schema.json",
                )
                descriptor = self._writer.write_schema_descriptor(path=descriptor_path, logical_role=role)
                files.append(_reuse_base_file_if_exact(descriptor, base_by_path.get(descriptor.logical_path), store=store))
                partitions = tuple(partition_spool.partitions(role))
                if not partitions:
                    partitions = ({},)
                for ordinal, partition in enumerate(partitions):
                    path = _logical_parquet_path(role=role, partition_key=partition)
                    staging = store.staging_path(build_id=build.build_id, attempt_id=attempt_id, logical_path=path)
                    written = self._writer.write_parquet(
                        path=staging,
                        logical_path=path,
                        logical_role=role,
                        partition_key=partition,
                        ordinal=ordinal,
                        rows=partition_spool.rows(logical_role=role, partition_key=partition),
                    )
                    files.append(_reuse_base_file_if_exact(written, base_by_path.get(written.logical_path), store=store))
        finally:
            partition_spool.close()
            for source_rows in rows_by_role.values():
                close = getattr(source_rows, "close", None)
                if close is not None:
                    close()
        identities = tuple(
            SnapshotFileIdentity.model_validate(file.model_dump(mode="python", exclude={"uri"}))
            for file in sorted(files, key=lambda item: item.logical_path)
        )
        receipt = MaterializationReceipt(
            build_id=build.build_id,
            attempt_id=attempt_id,
            source_identity_hash=canonical_json_sha256(
                {
                    "source_revision_set_hash": build.request.snapshot_source_revision_set_hash,
                    "query_registry_hash": build.request.query_registry_hash,
                    "requested_source_cutoff": build.request.requested_source_cutoff,
                    "label_as_of_ts": build.request.label_as_of_ts,
                }
            ),
            capture_set_hash=str(build.request.capture_set_hash),
            source_revision_set_hash=build.request.snapshot_source_revision_set_hash,
            files=identities,
            file_set_hash=canonical_json_sha256([item.canonical_identity() for item in identities]),
        )
        logger.info(
            "Batch D materialized immutable files",
            extra={"build_id": build.build_id, "attempt_id": attempt_id, "file_count": len(files)},
        )
        return tuple(files), receipt


class DatasetSnapshotPipeline:
    """Drive the existing build state machine without a second runtime path."""

    def __init__(
        self,
        *,
        repository: Any,
        materializer: DatasetSnapshotMaterializer,
        store: LocalContentAddressedStore,
    ) -> None:
        self._repository = repository
        self._materializer = materializer
        self._store = store
        self._writer = materializer._writer
        self._verifier = FullParquetVerifier(writer_version=self._writer.writer_version)

    def run(self, *, build_id: str, actor: str) -> DatasetBuild:
        while True:
            build = self._repository.get_build(build_id)
            if build.checkpoint is BuildCheckpoint.SEALED:
                self._verify_sealed_readback(build=build, actor=actor)
                logger.info("Batch D build already sealed", extra={"build_id": build_id})
                return build
            if build.current_attempt_id is not None:
                expired_attempt_id = build.current_attempt_id
                self._repository.expire_attempt(
                    attempt_id=expired_attempt_id,
                    expected_fencing_token=build.current_fencing_token,
                    actor=actor,
                )
                self._repository.recover_expired_attempt(expired_attempt_id=expired_attempt_id, actor=actor)
                self._store.cleanup_attempt_staging(
                    build_id=build.build_id,
                    attempt_id=expired_attempt_id,
                )
                continue
            if build.checkpoint is BuildCheckpoint.REQUESTED:
                self._materialize(build=build, actor=actor)
            elif build.checkpoint is BuildCheckpoint.MATERIALIZED:
                self._verify(build=build, actor=actor)
            elif build.checkpoint is BuildCheckpoint.VERIFIED:
                self._promote(build=build, actor=actor)
            elif build.checkpoint is BuildCheckpoint.PROMOTED:
                self._seal(build=build, actor=actor)
            else:
                raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "unsupported Batch D checkpoint")

    def verify_read_only(self, *, build_id: str) -> FullParquetVerificationReceipt:
        return self._rebuild_verification(self._repository.get_build(build_id))

    def _start(self, *, build: DatasetBuild, operation: AttemptOperation, actor: str) -> Any:
        return self._repository.start_attempt(
            build_id=build.build_id,
            operation=operation,
            expected_build_row_version=build.row_version,
            expected_checkpoint=build.checkpoint,
            lease_owner_id=actor,
            lease_token=canonical_json_sha256(
                {"build_id": build.build_id, "operation": operation.value, "pid": os.getpid(), "row_version": build.row_version}
            ),
            lease_seconds=900,
            operation_request_hash=canonical_json_sha256(
                {"build_id": build.build_id, "operation": operation.value, "checkpoint": build.checkpoint.value}
            ),
        )

    def _materialize(self, *, build: DatasetBuild, actor: str) -> None:
        attempt = self._start(build=build, operation=AttemptOperation.MATERIALIZE, actor=actor)
        try:
            with _AttemptHeartbeat(repository=self._repository, attempt=attempt):
                self._complete_materialize_attempt(build=build, attempt=attempt, actor=actor)
        except Exception as error:
            self._fail_active_attempt(attempt=attempt, actor=actor, error=error)
            raise

    def _complete_materialize_attempt(self, *, build: DatasetBuild, attempt: Any, actor: str) -> None:
        active = self._repository.get_build(build.build_id)
        base_files: tuple[DatasetSnapshotFile, ...] = ()
        if active.request.base_snapshot is not None:
            if not hasattr(self._repository, "snapshot_files") or not hasattr(
                self._repository, "assert_base_snapshot_reusable"
            ):
                raise SnapshotWriterError(
                    REASON_MANIFEST_CONFLICT,
                    "base snapshot repository readback or invalidation check is unavailable",
                )
            self._repository.assert_base_snapshot_reusable(active.request)
            base_files = self._repository.snapshot_files(active.request.base_snapshot.snapshot_id)
            if not base_files:
                raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "base snapshot has no immutable files")
        files, materialization_receipt = self._materializer.materialize(
            build=active,
            attempt_id=attempt.attempt_id,
            store=self._store,
            base_files=base_files,
        )
        attempt_files = tuple(
            DatasetAttemptFile(
                attempt_id=attempt.attempt_id,
                fencing_token=attempt.fencing_token,
                logical_path=file.logical_path,
                logical_role=file.logical_role,
                partition_key_hash=file.partition_key_hash,
                ordinal=file.ordinal,
                staging_uri=file.uri,
                sha256=file.sha256,
                size_bytes=file.size_bytes,
                row_count=file.row_count,
                schema_fingerprint=file.schema_fingerprint,
                partition_content_hash=file.partition_content_hash,
                compression=file.compression,
                writer_version=file.writer_version,
            )
            for file in files
        )
        for file in attempt_files:
            self._repository.append_file(
                attempt_id=attempt.attempt_id,
                expected_fencing_token=attempt.fencing_token,
                file=file,
            )
        observed_hash = verify_attempt_file_set(attempt_files)
        self._repository.complete_materialize(
            attempt_id=attempt.attempt_id,
            expected_fencing_token=attempt.fencing_token,
            observed_file_set_hash=observed_hash,
            materialization_receipt=materialization_receipt,
            actor=actor,
        )

    def _written_files(self, build: DatasetBuild) -> tuple[WrittenDatasetFile, ...]:
        if build.checkpoint is BuildCheckpoint.SEALED:
            if build.sealed_snapshot_id is None or not hasattr(self._repository, "snapshot_files"):
                raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "sealed snapshot file readback is unavailable")
            snapshot_files = self._repository.snapshot_files(build.sealed_snapshot_id)
            if not snapshot_files:
                raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "sealed snapshot has no persisted files")
            return written_files_from_snapshot(snapshot_files)
        if build.materialized_attempt_id is None:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "build lacks materialized attempt")
        files = self._repository.files_for_attempt(build.materialized_attempt_id)
        return written_files_from_attempt(files)

    @staticmethod
    def _capability_manifest(build: DatasetBuild) -> DatasetCapabilityManifest:
        return capability_manifest_for_build(build)

    def _rebuild_verification(self, build: DatasetBuild) -> FullParquetVerificationReceipt:
        receipt = self._verifier.verify_files(
            build=build,
            files=self._written_files(build),
            capability_manifest=self._capability_manifest(build),
        )
        if build.checkpoint in {BuildCheckpoint.VERIFIED, BuildCheckpoint.PROMOTED, BuildCheckpoint.SEALED} and (
            receipt.receipt_hash != build.verify_receipt_hash
        ):
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "reconstructed full receipt differs from build")
        return receipt

    def _verify(self, *, build: DatasetBuild, actor: str) -> None:
        attempt = self._start(build=build, operation=AttemptOperation.VERIFY, actor=actor)
        try:
            with _AttemptHeartbeat(repository=self._repository, attempt=attempt):
                active = self._repository.get_build(build.build_id)
                receipt = self._rebuild_verification(active)
                self._repository.complete_full_verify(
                    attempt_id=attempt.attempt_id,
                    expected_fencing_token=attempt.fencing_token,
                    receipt=receipt,
                    actor=actor,
                )
        except Exception as error:
            self._fail_active_attempt(attempt=attempt, actor=actor, error=error)
            raise

    def _publish(self, build: DatasetBuild) -> tuple[
        FullParquetVerificationReceipt, DatasetManifest, PromotionReceipt, StoredCasObject
    ]:
        verification = self._rebuild_verification(build)
        promoter = DatasetCasPromoter(store=self._store)
        published = promoter.publish_files(self._written_files(build))
        snapshot_files = snapshot_files_from_published(published)
        manifest = build_dataset_manifest(
            build=build,
            verification=verification,
            files=snapshot_files,
            capability_manifest=self._capability_manifest(build),
            store_backend_hash=self._store.store_backend_hash,
        )
        promoter.publish_manifest(manifest)
        promotion = build_promotion_receipt(build=build, verification=verification, manifest=manifest)
        promotion_object = promoter.publish_promotion_receipt(promotion)
        return verification, manifest, promotion, promotion_object

    def _promote(self, *, build: DatasetBuild, actor: str) -> None:
        attempt = self._start(build=build, operation=AttemptOperation.PROMOTE, actor=actor)
        try:
            with _AttemptHeartbeat(repository=self._repository, attempt=attempt):
                active = self._repository.get_build(build.build_id)
                _, manifest, promotion, _ = self._publish(active)
                self._repository.complete_promote(
                    attempt_id=attempt.attempt_id,
                    expected_fencing_token=attempt.fencing_token,
                    receipt=promotion,
                    manifest=manifest,
                    store=self._store,
                    actor=actor,
                )
        except Exception as error:
            self._fail_active_attempt(attempt=attempt, actor=actor, error=error)
            raise

    def _seal(self, *, build: DatasetBuild, actor: str) -> None:
        verification, manifest, promotion, promotion_object = self._publish(build)
        attempt = self._start(build=build, operation=AttemptOperation.SEAL, actor=actor)
        snapshot: SealedDatasetSnapshot | None = None
        try:
            with _AttemptHeartbeat(repository=self._repository, attempt=attempt):
                active = self._repository.get_build(build.build_id)
                snapshot = assemble_sealed_snapshot(
                    build=active,
                    seal_attempt_id=attempt.attempt_id,
                    verification=verification,
                    manifest=manifest,
                    promotion=promotion,
                    promotion_object=promotion_object,
                    label_maturity_event_summary=verification.relational_closure_summary,
                )
                self._repository.save_sealed_snapshot(snapshot, actor=actor)
                self._repository.save_sealed_snapshot(snapshot, actor=actor)
                self._cleanup_materialized_staging(build)
        except Exception as error:
            current = self._repository.get_build(build.build_id)
            if current.checkpoint is BuildCheckpoint.SEALED and snapshot is not None:
                self._repository.save_sealed_snapshot(snapshot, actor=actor)
                logger.info(
                    "Batch D seal commit was confirmed by exact aggregate readback",
                    extra={"build_id": build.build_id, "snapshot_id": snapshot.snapshot_id},
                )
                self._cleanup_materialized_staging(current)
                return
            self._fail_active_attempt(attempt=attempt, actor=actor, error=error)
            raise

    def _verify_sealed_readback(self, *, build: DatasetBuild, actor: str) -> None:
        if build.sealed_attempt_id is None or build.sealed_snapshot_id is None:
            raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "sealed build lacks immutable snapshot identity")
        verification, manifest, promotion, promotion_object = self._publish(build)
        snapshot = assemble_sealed_snapshot(
            build=build,
            seal_attempt_id=build.sealed_attempt_id,
            verification=verification,
            manifest=manifest,
            promotion=promotion,
            promotion_object=promotion_object,
            label_maturity_event_summary=verification.relational_closure_summary,
        )
        if snapshot.snapshot_id != build.sealed_snapshot_id or snapshot.seal_receipt_hash != build.seal_receipt_hash:
            raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "sealed build identity differs from reconstructed aggregate")
        self._repository.save_sealed_snapshot(snapshot, actor=actor)
        self._cleanup_materialized_staging(build)

    def _cleanup_materialized_staging(self, build: DatasetBuild) -> None:
        if build.materialized_attempt_id is not None:
            self._store.cleanup_attempt_staging(
                build_id=build.build_id,
                attempt_id=build.materialized_attempt_id,
            )

    def _fail_active_attempt(self, *, attempt: Any, actor: str, error: Exception) -> None:
        reason_code = getattr(error, "reason_code", REASON_SOURCE_SNAPSHOT_CONFLICT)
        try:
            self._repository.fail_attempt(
                attempt_id=attempt.attempt_id,
                expected_fencing_token=attempt.fencing_token,
                error_code=str(reason_code),
                actor=actor,
            )
            if attempt.operation is AttemptOperation.MATERIALIZE:
                self._store.cleanup_attempt_staging(
                    build_id=attempt.build_id,
                    attempt_id=attempt.attempt_id,
                )
        except Exception:
            logger.exception(
                "Batch D attempt failure persistence or staging cleanup failed",
                extra={"attempt_id": attempt.attempt_id, "reason_code": reason_code},
            )
        logger.error(
            "Batch D operation failed",
            extra={"attempt_id": attempt.attempt_id, "reason_code": reason_code, "error_type": type(error).__name__},
        )


class DeterministicParquetWriter:
    """Write exact Parquet bytes from typed, fully materialized logical rows."""

    def __init__(self, *, writer_version: str = BATCH_D_WRITER_VERSION) -> None:
        if writer_version != BATCH_D_WRITER_VERSION:
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "unsupported Batch D writer version")
        self._writer_version = writer_version

    @property
    def writer_version(self) -> str:
        return self._writer_version

    def schema_fingerprint(self, logical_role: str) -> str:
        return canonical_json_sha256(_schema_descriptor_payload(logical_role))

    def schema_descriptor_bytes(self, logical_role: str) -> bytes:
        return _canonical_json_bytes(_schema_descriptor_payload(logical_role))

    def write_schema_descriptor(self, *, path: Path, logical_role: str) -> WrittenDatasetFile:
        payload = self.schema_descriptor_bytes(logical_role)
        _write_exact_bytes(path, payload)
        descriptor = _file_identity_for_bytes(
            path=path,
            logical_path=f"schemas/{logical_role}.schema.json",
            logical_role=SCHEMA_DESCRIPTOR_ROLE,
            partition_key_hash=canonical_json_sha256({"logical_role": logical_role}),
            ordinal=0,
            row_count=0,
            schema_fingerprint=self.schema_fingerprint(logical_role),
            partition_content_hash=canonical_json_sha256(_schema_descriptor_payload(logical_role)),
            compression="none",
            writer_version=self._writer_version,
        )
        return descriptor

    def write_parquet(
        self,
        *,
        path: Path,
        logical_path: str,
        logical_role: str,
        partition_key: Mapping[str, str],
        ordinal: int,
        rows: Iterable[LogicalDatasetRow],
    ) -> WrittenDatasetFile:
        if logical_role not in SNAPSHOT_ARROW_SCHEMAS_V1:
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "unknown Parquet logical role")
        pa, pq = _pyarrow()
        schema = _arrow_schema(logical_role)
        metadata = {
            b"aistock_snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION.encode("ascii"),
            b"aistock_logical_role": logical_role.encode("ascii"),
            b"aistock_schema_fingerprint": self.schema_fingerprint(logical_role).encode("ascii"),
            b"aistock_writer_version": self._writer_version.encode("ascii"),
        }
        schema = schema.with_metadata(metadata)
        digest = _PartitionContentDigest(logical_role=logical_role, partition_key=partition_key)
        row_count = 0
        previous_key: tuple[str, ...] | None = None
        batch: list[dict[str, Any]] = []
        batch_bytes = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as handle:
                writer = pq.ParquetWriter(
                    handle, schema, version="2.6", use_dictionary=False,
                    compression="zstd", compression_level=3, write_statistics=True,
                    use_deprecated_int96_timestamps=False, coerce_timestamps="us", allow_truncated_timestamps=False,
                    data_page_size=1048576, data_page_version="2.0", use_compliant_nested_type=True,
                    write_batch_size=1024, store_schema=True, write_page_index=False, write_page_checksum=True,
                    store_decimal_as_integer=False, use_content_defined_chunking=False,
                )
                try:
                    for row in rows:
                        if row.logical_role != logical_role:
                            raise SnapshotWriterError(
                                REASON_ARROW_SCHEMA_CONFLICT,
                                "mixed logical roles cannot share one Parquet file",
                            )
                        values = _coerce_values(logical_role=logical_role, values=row.values)
                        key = _logical_sort_key(logical_role, values)
                        if row.sort_key and row.sort_key != key:
                            raise SnapshotWriterError(
                                REASON_PARQUET_WRITE_FAILED,
                                "caller sort key differs from frozen role sort key",
                            )
                        if previous_key is not None and key <= previous_key:
                            raise SnapshotWriterError(
                                REASON_PARQUET_WRITE_FAILED,
                                "logical rows violate frozen sort/unique order",
                            )
                        row_bytes = _canonical_json_bytes(values)
                        if len(row_bytes) > 128 * 1024 * 1024 or (
                            batch and batch_bytes + len(row_bytes) > 128 * 1024 * 1024
                        ):
                            raise SnapshotWriterError(
                                REASON_PARQUET_WRITE_FAILED,
                                "one frozen 65,536-row record batch exceeds 128 MiB",
                            )
                        batch.append(values)
                        batch_bytes += len(row_bytes)
                        digest.add(values)
                        previous_key = key
                        row_count += 1
                        if len(batch) == 65536:
                            writer.write_table(pa.Table.from_pylist(batch, schema=schema), row_group_size=65536)
                            batch = []
                            batch_bytes = 0
                    if batch or row_count == 0:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema), row_group_size=65536)
                finally:
                    writer.close()
                handle.flush()
                os.fsync(handle.fileno())
        except SnapshotWriterError:
            raise
        except FileExistsError as error:
            raise SnapshotWriterError(REASON_PARQUET_WRITE_FAILED, "writer cannot overwrite an existing staging file") from error
        except Exception as error:  # pyarrow has several implementation-specific error types.
            raise SnapshotWriterError(REASON_PARQUET_WRITE_FAILED, f"cannot write deterministic Parquet: {type(error).__name__}") from error
        partition_content_hash = digest.hexdigest()
        return _file_identity_for_bytes(
            path=path, logical_path=logical_path, logical_role=logical_role,
            partition_key_hash=canonical_json_sha256(dict(sorted(partition_key.items()))), ordinal=ordinal,
            row_count=row_count, schema_fingerprint=self.schema_fingerprint(logical_role),
            partition_content_hash=partition_content_hash, compression="zstd", writer_version=self._writer_version,
        )


class FullParquetVerifier:
    """Verify every byte and every row in a frozen Batch D materialized file set."""

    def __init__(self, *, writer_version: str = BATCH_D_WRITER_VERSION) -> None:
        self._writer = DeterministicParquetWriter(writer_version=writer_version)

    def verify_files(
        self,
        *,
        build: DatasetBuild,
        files: Sequence[WrittenDatasetFile],
        capability_manifest: DatasetCapabilityManifest,
    ) -> FullParquetVerificationReceipt:
        if build.materialized_file_set_hash is None:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "build has no materialized file set")
        self._validate_complete_file_set(files)
        verified: list[VerifiedDatasetFile] = []
        rows_by_role: dict[str, list[dict[str, Any]]] = {role: [] for role in SNAPSHOT_ARROW_SCHEMAS_V1}
        for file in sorted(files, key=lambda item: item.logical_path):
            payload = _read_exact(path=_path_from_file_uri(file.uri), sha256=file.sha256, size_bytes=file.size_bytes)
            if file.logical_role == SCHEMA_DESCRIPTOR_ROLE:
                role = _role_from_schema_path(file.logical_path)
                expected = self._writer.schema_descriptor_bytes(role)
                if (
                    file.compression != "none"
                    or file.writer_version != self._writer.writer_version
                    or payload != expected
                    or file.partition_content_hash != canonical_json_sha256(_schema_descriptor_payload(role))
                ):
                    raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "schema descriptor bytes are not canonical")
                observed_rows = 0
                observed_schema = self._writer.schema_fingerprint(role)
                observed_partition = file.partition_content_hash
            else:
                actual_rows, observed_schema, observed_partition = self._verify_parquet(file=file)
                rows_by_role[file.logical_role].extend(actual_rows)
                observed_rows = len(actual_rows)
            verified.append(
                VerifiedDatasetFile(
                    file=SnapshotFileIdentity.model_validate(file.model_dump(mode="python", exclude={"uri"})),
                    observed_sha256=file.sha256, observed_size_bytes=file.size_bytes,
                    observed_row_count=observed_rows, observed_schema_fingerprint=observed_schema,
                    observed_partition_content_hash=observed_partition,
                )
            )
        relational_summary, observations, labels = _validate_relational_rows(
            build=build,
            rows_by_role=rows_by_role,
            capability_manifest=capability_manifest,
        )
        _validate_capability_against_request(build=build, capability_manifest=capability_manifest)
        observation_mapping_hash = canonical_json_sha256(
            [
                {"identity_id": row["selected_mapping_id"], "identity_hash": row["selected_mapping_hash"]}
                for row in sorted(rows_by_role["selected_observations"], key=lambda item: item["selected_mapping_id"])
            ]
        )
        label_mapping_hash = canonical_json_sha256(
            [
                {"identity_id": row["selected_label_mapping_id"], "identity_hash": row["selected_label_mapping_hash"]}
                for row in sorted(rows_by_role["selected_labels"], key=lambda item: item["selected_label_mapping_id"])
            ]
        )
        if (
            observation_mapping_hash != build.request.selected_observation_mapping_set_hash
            or label_mapping_hash != build.request.selected_label_mapping_set_hash
        ):
            raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "selected mapping sets differ from frozen build request")
        return FullParquetVerificationReceipt(
            build_id=build.build_id,
            file_set_hash=build.materialized_file_set_hash,
            capture_set_hash=str(build.request.capture_set_hash),
            source_revision_set_hash=build.request.snapshot_source_revision_set_hash,
            selected_observation_mapping_set_hash=str(build.request.selected_observation_mapping_set_hash),
            selected_label_mapping_set_hash=str(build.request.selected_label_mapping_set_hash),
            capability_manifest_hash=str(capability_manifest.manifest_hash),
            files=tuple(verified),
            selected_observations=observations,
            selected_labels=labels,
            relational_closure_summary=relational_summary,
        )

    def _validate_complete_file_set(self, files: Sequence[WrittenDatasetFile]) -> None:
        paths = [item.logical_path for item in files]
        if len(paths) != len(set(paths)):
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "materialized logical paths are not unique")
        descriptor_roles = {
            _role_from_schema_path(item.logical_path)
            for item in files
            if item.logical_role == SCHEMA_DESCRIPTOR_ROLE
        }
        data_roles = {item.logical_role for item in files if item.logical_role != SCHEMA_DESCRIPTOR_ROLE}
        required = set(SNAPSHOT_ARROW_SCHEMAS_V1)
        if descriptor_roles != required or data_roles != required:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "materialized file set does not cover every logical role")

    def _verify_parquet(self, *, file: WrittenDatasetFile) -> tuple[list[dict[str, Any]], str, str]:
        if file.logical_role not in SNAPSHOT_ARROW_SCHEMAS_V1:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "unknown Parquet role")
        if file.compression != "zstd" or file.writer_version != self._writer.writer_version:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet descriptor differs from writer v1")
        pa, pq = _pyarrow()
        path = _path_from_file_uri(file.uri)
        try:
            parquet = pq.ParquetFile(path)
            table = parquet.read()
        except Exception as error:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, f"cannot read full Parquet file: {type(error).__name__}") from error
        expected_schema = _arrow_schema(file.logical_role)
        actual_schema = table.schema.remove_metadata()
        if actual_schema != expected_schema:
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "Parquet Arrow schema differs from frozen role schema")
        metadata = table.schema.metadata or {}
        required_metadata = {
            b"aistock_snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION.encode("ascii"),
            b"aistock_logical_role": file.logical_role.encode("ascii"),
            b"aistock_schema_fingerprint": self._writer.schema_fingerprint(file.logical_role).encode("ascii"),
            b"aistock_writer_version": self._writer.writer_version.encode("ascii"),
        }
        if metadata != required_metadata:
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "Parquet metadata is not frozen")
        actual_rows = table.to_pylist()
        keys = tuple(_logical_sort_key(file.logical_role, item) for item in actual_rows)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet rows violate frozen sort/unique order")
        if len(actual_rows) != file.row_count:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet row count differs from descriptor")
        partition_key = _partition_key_from_logical_path(file.logical_path, logical_role=file.logical_role)
        if canonical_json_sha256(dict(sorted(partition_key.items()))) != file.partition_key_hash:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "logical path partition differs from descriptor")
        observed_partition = _partition_content_hash(
            logical_role=file.logical_role,
            partition_key=partition_key,
            values=actual_rows,
        )
        self._verify_parquet_physical_contract(parquet=parquet, file=file)
        return actual_rows, self._writer.schema_fingerprint(file.logical_role), observed_partition

    @staticmethod
    def _verify_parquet_physical_contract(*, parquet: Any, file: WrittenDatasetFile) -> None:
        metadata = parquet.metadata
        expected_groups = max(1, (file.row_count + 65535) // 65536)
        if metadata is None or metadata.num_rows != file.row_count or metadata.num_row_groups != expected_groups:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet row-group contract differs from writer v1")
        for group_index in range(metadata.num_row_groups):
            group = metadata.row_group(group_index)
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                if column.compression != "ZSTD" or (file.row_count > 0 and column.statistics is None):
                    raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet compression/statistics contract differs from writer v1")


class DatasetCasPromoter:
    """Publish verified staging files, manifests, and receipts to one local CAS."""

    def __init__(self, *, store: LocalContentAddressedStore) -> None:
        self._store = store

    def publish_files(self, files: Sequence[WrittenDatasetFile]) -> tuple[PublishedDatasetFile, ...]:
        published: list[PublishedDatasetFile] = []
        for file in sorted(files, key=lambda item: item.logical_path):
            if self._store.is_canonical_blob_uri(uri=file.uri, sha256=file.sha256):
                stored = self._store.describe_blob(uri=file.uri, sha256=file.sha256, size_bytes=file.size_bytes)
            else:
                stored = self._store.publish_staging_file(
                    staging_uri=file.uri,
                    sha256=file.sha256,
                    size_bytes=file.size_bytes,
                )
            self._store.verify_object(stored)
            published.append(
                PublishedDatasetFile(
                    **file.model_dump(exclude={"uri"}),
                    content_uri=stored.uri,
                    store_backend_hash=stored.store_backend_hash,
                )
            )
        if len({item.logical_path for item in published}) != len(published):
            raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "published logical paths are not unique")
        return tuple(published)

    def publish_manifest(self, manifest: DatasetManifest) -> StoredCasObject:
        if manifest.store_backend_hash != self._store.store_backend_hash:
            raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "manifest store identity differs from local CAS")
        stored = self._store.put_document_bytes(kind="manifests", payload=manifest.canonical_bytes())
        self._store.verify_object(stored)
        if stored.sha256 != manifest.manifest_sha256:
            raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "published manifest hash differs from canonical manifest")
        return stored

    def publish_promotion_receipt(self, receipt: PromotionReceipt) -> StoredCasObject:
        if receipt.store_backend_hash != self._store.store_backend_hash:
            raise SnapshotWriterError(REASON_PROMOTION_RECEIPT_CONFLICT, "promotion receipt store identity differs from local CAS")
        for blob in receipt.blobs:
            if blob.blob.store_backend_hash != self._store.store_backend_hash:
                raise SnapshotWriterError(REASON_PROMOTION_RECEIPT_CONFLICT, "promotion receipt has a foreign blob backend")
            self._store.read_bytes(uri=blob.content_uri, sha256=blob.sha256, size_bytes=blob.size_bytes)
        stored = self._store.put_document_bytes(kind="promotion_receipts", payload=receipt.canonical_bytes())
        self._store.verify_object(stored)
        if stored.sha256 != receipt.receipt_sha256:
            raise SnapshotWriterError(REASON_PROMOTION_RECEIPT_CONFLICT, "published promotion receipt hash differs from canonical receipt")
        return stored


def snapshot_files_from_published(files: Sequence[PublishedDatasetFile]) -> tuple[DatasetSnapshotFile, ...]:
    """Convert reopened CAS file descriptors to the existing seal contract."""

    return tuple(
        DatasetSnapshotFile(
            logical_path=file.logical_path,
            logical_role=file.logical_role,
            partition_key_hash=file.partition_key_hash,
            ordinal=file.ordinal,
            content_uri=file.content_uri,
            sha256=file.sha256,
            size_bytes=file.size_bytes,
            row_count=file.row_count,
            schema_fingerprint=file.schema_fingerprint,
            partition_content_hash=file.partition_content_hash,
            blob=DatasetBlobHeader(
                store_backend_hash=file.store_backend_hash,
                blob_sha256=file.sha256,
                size_bytes=file.size_bytes,
            ),
        )
        for file in sorted(files, key=lambda item: item.logical_path)
    )


def written_files_from_attempt(files: Sequence[DatasetAttemptFile]) -> tuple[WrittenDatasetFile, ...]:
    return tuple(
        WrittenDatasetFile(
            **file.model_dump(mode="python", exclude={"attempt_id", "fencing_token", "staging_uri"}),
            uri=file.staging_uri,
        )
        for file in sorted(files, key=lambda item: item.logical_path)
    )


def written_files_from_snapshot(files: Sequence[DatasetSnapshotFile]) -> tuple[WrittenDatasetFile, ...]:
    return tuple(
        WrittenDatasetFile(
            logical_path=file.logical_path,
            logical_role=file.logical_role,
            partition_key_hash=file.partition_key_hash,
            ordinal=file.ordinal,
            uri=file.content_uri,
            sha256=file.sha256,
            size_bytes=file.size_bytes,
            row_count=file.row_count,
            schema_fingerprint=file.schema_fingerprint,
            partition_content_hash=file.partition_content_hash,
            compression="none" if file.logical_role == SCHEMA_DESCRIPTOR_ROLE else "zstd",
            writer_version=BATCH_D_WRITER_VERSION,
        )
        for file in sorted(files, key=lambda item: item.logical_path)
    )


def capability_manifest_for_build(build: DatasetBuild) -> DatasetCapabilityManifest:
    rows = [
        DatasetCapabilityRow(component=item.component, capability=item.capability, status="FULL")
        for item in build.request.required_composite_capabilities
        if item.required
    ]
    rows.extend(
        (
            DatasetCapabilityRow(component="MODEL", capability="MODEL_TRAINING_READY", status="false"),
            DatasetCapabilityRow(component="RUNTIME", capability="RUNTIME_ADVISORY_READY", status="false"),
            DatasetCapabilityRow(component="TRADING", capability="TRADING_EXECUTION_READY", status="false"),
        )
    )
    return DatasetCapabilityManifest(rows=tuple(rows))


def snapshot_blob_refs(files: Sequence[DatasetSnapshotFile]) -> tuple[DatasetSnapshotBlobRef, ...]:
    return tuple(
        DatasetSnapshotBlobRef(
            logical_path=file.logical_path,
            logical_role=file.logical_role,
            partition_key_hash=file.partition_key_hash,
            ordinal=file.ordinal,
            blob=file.blob,
        )
        for file in sorted(files, key=lambda item: item.logical_path)
    )


def build_dataset_manifest(
    *,
    build: DatasetBuild,
    verification: FullParquetVerificationReceipt,
    files: Sequence[DatasetSnapshotFile],
    capability_manifest: DatasetCapabilityManifest,
    store_backend_hash: str,
) -> DatasetManifest:
    if (
        verification.build_id != build.build_id
        or verification.receipt_hash != build.verify_receipt_hash
        or verification.capability_manifest_hash != capability_manifest.manifest_hash
        or verification.capture_set_hash != build.request.capture_set_hash
        or verification.source_revision_set_hash != build.request.snapshot_source_revision_set_hash
    ):
        raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "manifest inputs differ from verified build")
    identities = tuple(
        SnapshotFileIdentity(
            logical_path=file.logical_path,
            logical_role=file.logical_role,
            partition_key_hash=file.partition_key_hash,
            ordinal=file.ordinal,
            sha256=file.sha256,
            size_bytes=file.size_bytes,
            row_count=file.row_count,
            schema_fingerprint=file.schema_fingerprint,
            partition_content_hash=file.partition_content_hash,
            compression="none" if file.logical_role == SCHEMA_DESCRIPTOR_ROLE else "zstd",
            writer_version=BATCH_D_WRITER_VERSION,
        )
        for file in sorted(files, key=lambda item: item.logical_path)
    )
    if canonical_json_sha256([item.canonical_identity() for item in identities]) != verification.verified_content_set_hash:
        raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "manifest files differ from full verification")
    core = DatasetManifestCore(
        files=tuple(files),
        selected_observations=verification.selected_observations,
        selected_labels=verification.selected_labels,
        snapshot_source_revision_set_hash=build.request.snapshot_source_revision_set_hash,
        capture_set_hash=str(build.request.capture_set_hash),
        handoff_readiness_hash=build.request.handoff_readiness_hash,
        admission_scope_set_hash=str(build.request.admission_scope_set_hash),
        query_registry_hash=build.request.query_registry_hash,
        capability_manifest=capability_manifest,
        schema_fingerprint=build.request.schema_fingerprint,
        builder_version=build.request.builder_version,
        code_commit=build.request.code_commit,
        writer_version=build.request.writer_version,
        partition_policy_hash=build.request.partition_policy_hash,
        policy_compatibility_hash=build.request.policy_compatibility_hash,
        base_snapshot=build.request.base_snapshot,
    )
    return DatasetManifest(core=core, store_backend_hash=store_backend_hash)


def build_promotion_receipt(
    *, build: DatasetBuild, verification: FullParquetVerificationReceipt, manifest: DatasetManifest
) -> PromotionReceipt:
    if verification.receipt_hash != build.verify_receipt_hash:
        raise SnapshotWriterError(REASON_PROMOTION_RECEIPT_CONFLICT, "promotion verification receipt differs from build")
    return PromotionReceipt(
        build_id=build.build_id,
        full_verification_receipt_hash=str(verification.receipt_hash),
        manifest_core_sha256=str(manifest.core.manifest_core_sha256),
        manifest_sha256=str(manifest.manifest_sha256),
        store_backend_hash=manifest.store_backend_hash,
        verified_content_set_hash=str(verification.verified_content_set_hash),
        blobs=manifest.core.files,
    )


def assemble_sealed_snapshot(
    *,
    build: DatasetBuild,
    seal_attempt_id: str,
    verification: FullParquetVerificationReceipt,
    manifest: DatasetManifest,
    promotion: PromotionReceipt,
    promotion_object: StoredCasObject,
    label_maturity_event_summary: dict[str, object],
) -> SealedDatasetSnapshot:
    checkpoint_context_valid = (
        build.checkpoint is BuildCheckpoint.PROMOTED and build.current_attempt_id == seal_attempt_id
    ) or (
        build.checkpoint is BuildCheckpoint.SEALED and build.sealed_attempt_id == seal_attempt_id
    )
    if (
        not checkpoint_context_valid
        or build.promotion_receipt_hash != promotion.receipt_sha256
        or build.promoted_manifest_hash != manifest.manifest_sha256
        or promotion_object.sha256 != promotion.receipt_sha256
        or promotion_object.store_backend_hash != promotion.store_backend_hash
        or promotion.manifest_core_sha256 != manifest.core.manifest_core_sha256
        or promotion.full_verification_receipt_hash != verification.receipt_hash
    ):
        raise SnapshotWriterError(REASON_PROMOTION_RECEIPT_CONFLICT, "seal inputs differ from promoted build")
    capability_payload = {
        "rows": [
            row.model_dump(mode="json")
            for row in sorted(manifest.core.capability_manifest.rows, key=lambda item: (item.component, item.capability))
        ]
    }
    seal_receipt_hash = canonical_json_sha256(
        {
            "build_id": build.build_id,
            "snapshot_content_hash": manifest.core.manifest_core_sha256,
            "manifest_sha256": manifest.manifest_sha256,
            "promotion_receipt_hash": promotion.receipt_sha256,
            "full_verification_receipt_hash": verification.receipt_hash,
        }
    )
    return SealedDatasetSnapshot(
        build_id=build.build_id,
        seal_attempt_id=seal_attempt_id,
        seal_receipt_hash=seal_receipt_hash,
        verification_contract_version=BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT,
        manifest_core_sha256=str(manifest.core.manifest_core_sha256),
        manifest_sha256=str(manifest.manifest_sha256),
        promotion_receipt_uri=promotion_object.uri,
        promotion_receipt_hash=str(promotion.receipt_sha256),
        snapshot_schema_version=build.request.snapshot_schema_version,
        snapshot_source_revision_set_hash=build.request.snapshot_source_revision_set_hash,
        capture_set_hash=str(build.request.capture_set_hash),
        base_snapshot=build.request.base_snapshot,
        handoff_readiness_hash=build.request.handoff_readiness_hash,
        admission_scope_set_hash=str(build.request.admission_scope_set_hash),
        query_registry_hash=build.request.query_registry_hash,
        builder_version=build.request.builder_version,
        code_commit=build.request.code_commit,
        writer_version=build.request.writer_version,
        partition_policy_hash=build.request.partition_policy_hash,
        policy_compatibility_hash=build.request.policy_compatibility_hash,
        dataset_capability_manifest=capability_payload,
        dataset_capability_manifest_hash=canonical_json_sha256(capability_payload),
        schema_fingerprint=build.request.schema_fingerprint,
        files=manifest.core.files,
        observations=manifest.core.selected_observations,
        labels=manifest.core.selected_labels,
        blob_refs=snapshot_blob_refs(manifest.core.files),
        label_maturity_event_summary=canonicalize(label_maturity_event_summary),
    )


def _schema_descriptor_payload(logical_role: str) -> dict[str, Any]:
    fields = SNAPSHOT_ARROW_SCHEMAS_V1.get(logical_role)
    if fields is None:
        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "unknown logical role schema")
    return {
        "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
        "logical_role": logical_role,
        "writer_version": BATCH_D_WRITER_VERSION,
        "fields": [field.model_dump(mode="json") for field in fields],
    }


def _arrow_schema(logical_role: str) -> Any:
    pa, _ = _pyarrow()
    fields = SNAPSHOT_ARROW_SCHEMAS_V1.get(logical_role)
    if fields is None:
        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "unknown logical role schema")
    type_map = {
        "utf8": pa.string(),
        "date32": pa.date32(),
        "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
        "decimal38_12": pa.decimal128(38, 12),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "bool": pa.bool_(),
        "list_utf8": pa.list_(pa.string()),
        "canonical_json": pa.string(),
    }
    return pa.schema([pa.field(field.name, type_map[field.arrow_type], nullable=field.nullable) for field in fields])


def _coerce_values(*, logical_role: str, values: Mapping[str, Any]) -> dict[str, Any]:
    schema = SNAPSHOT_ARROW_SCHEMAS_V1[logical_role]
    expected_names = {field.name for field in schema}
    actual_names = set(values)
    if actual_names != expected_names:
        missing = sorted(expected_names - actual_names)
        extra = sorted(actual_names - expected_names)
        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"role columns do not match frozen schema: missing={missing}, extra={extra}")
    result: dict[str, Any] = {}
    for field in schema:
        value = values[field.name]
        if value is None:
            if not field.nullable:
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"non-null role field is null: {field.name}")
            result[field.name] = None
            continue
        if field.arrow_type == "timestamp_us_utc":
            if not isinstance(value, datetime):
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"timestamp role field is not datetime: {field.name}")
            result[field.name] = _utc(value, field_name=field.name)
        elif field.arrow_type == "date32":
            if isinstance(value, datetime) or not isinstance(value, date):
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"date role field is not date: {field.name}")
            result[field.name] = value
        elif field.arrow_type == "decimal38_12":
            try:
                result[field.name] = Decimal(str(value))
            except Exception as error:
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"decimal role field is invalid: {field.name}") from error
        elif field.arrow_type in {"int32", "int64"}:
            if isinstance(value, bool) or not isinstance(value, int):
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"integer role field is invalid: {field.name}")
            result[field.name] = value
        elif field.arrow_type == "bool":
            if not isinstance(value, bool):
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"boolean role field is invalid: {field.name}")
            result[field.name] = value
        elif field.arrow_type == "list_utf8":
            if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) for item in value):
                raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"reason-code role field is invalid: {field.name}")
            result[field.name] = sorted(set(value))
        elif field.arrow_type == "canonical_json":
            if isinstance(value, str):
                try:
                    decoded = json.loads(value)
                except (TypeError, ValueError) as error:
                    raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"canonical JSON is invalid: {field.name}") from error
            else:
                decoded = value
            result[field.name] = _canonical_json_bytes(decoded).decode("utf-8")
        elif not isinstance(value, str):
            raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"text role field is invalid: {field.name}")
        else:
            result[field.name] = value
    return result


def _file_identity_for_bytes(
    *, path: Path, logical_path: str, logical_role: str, partition_key_hash: str, ordinal: int, row_count: int,
    schema_fingerprint: str, partition_content_hash: str, compression: str, writer_version: str,
) -> WrittenDatasetFile:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise SnapshotWriterError(REASON_PARQUET_BYTES_CONFLICT, "cannot reopen written file") from error
    return WrittenDatasetFile(
        logical_path=logical_path, logical_role=logical_role, partition_key_hash=partition_key_hash, ordinal=ordinal,
        uri=path.as_uri(), sha256=hashlib.sha256(payload).hexdigest(), size_bytes=len(payload), row_count=row_count,
        schema_fingerprint=schema_fingerprint, partition_content_hash=partition_content_hash,
        compression=compression, writer_version=writer_version,
    )


def _write_exact_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise SnapshotWriterError(REASON_PARQUET_WRITE_FAILED, "schema descriptor staging file already exists") from error
    except OSError as error:
        raise SnapshotWriterError(REASON_PARQUET_WRITE_FAILED, "cannot write schema descriptor") from error


def _read_exact(*, path: Path, sha256: str, size_bytes: int) -> bytes:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "cannot read frozen file") from error
    if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != sha256:
        raise SnapshotWriterError(REASON_PARQUET_BYTES_CONFLICT, "frozen file bytes do not match descriptor")
    return payload


def _path_from_file_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "file descriptor URI must be local")
    raw = unquote(parsed.path)
    if os.name == "nt" and len(raw) >= 3 and raw[0] == "/" and raw[2] == ":":
        raw = raw[1:]
    try:
        return Path(raw).resolve(strict=True)
    except OSError as error:
        raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "file descriptor URI cannot be resolved") from error


def _role_from_schema_path(logical_path: str) -> str:
    prefix = "schemas/"
    suffix = ".schema.json"
    if not logical_path.startswith(prefix) or not logical_path.endswith(suffix):
        raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "schema descriptor logical path is invalid")
    role = logical_path[len(prefix):-len(suffix)]
    if role not in SNAPSHOT_ARROW_SCHEMAS_V1:
        raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "schema descriptor role is invalid")
    return role


def _logical_sort_key(logical_role: str, values: Mapping[str, Any]) -> tuple[str, ...]:
    fields = {
        "canonical_signals": ("decision_as_of_trade_date", "canonical_signal_id"),
        "observation_versions": ("canonical_signal_id", "observation_revision_no", "observation_version_id"),
        "selected_observations": ("canonical_signal_id",),
        "lineage": ("canonical_signal_id", "observation_version_id", "lineage_id"),
        "stage_summaries": ("observation_version_id", "stage", "stage_evidence_id"),
        "stage_candidates": ("stage_evidence_id", "symbol"),
        "outcome_labels": ("decision_as_of_trade_date", "label_key_hash", "label_revision_no"),
        "selected_labels": ("label_key_hash",),
        "outcome_source_evidence": ("label_key_hash", "label_version_id"),
        "universe_outcomes": ("decision_as_of_trade_date", "label_key_hash", "label_revision_no"),
        "gaps": ("decision_as_of_trade_date", "source_kind", "gap_id"),
        "source_revisions": ("source_revision_set_id", "member_key"),
    }.get(logical_role)
    if fields is None:
        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "logical role has no frozen sort key")
    return tuple(_sortable_text(values[field]) for field in fields)


def _partition_key_for_values(
    *,
    logical_role: str,
    values: Mapping[str, Any],
    decision_date: date | None = None,
    outcome: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    if logical_role in {"selected_observations", "source_revisions"}:
        return {}
    if logical_role == "selected_labels":
        if outcome is None:
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "selected label partition lacks terminal outcome")
        return {"horizon": str(outcome["horizon_trading_days"])}
    if logical_role in {"outcome_labels", "universe_outcomes"}:
        trade_date = values["decision_as_of_trade_date"]
        return {
            "horizon": str(values["horizon_trading_days"]),
            "year": f"{trade_date.year:04d}",
            "month": f"{trade_date.month:02d}",
        }
    if logical_role == "outcome_source_evidence":
        if outcome is None:
            raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "outcome evidence partition lacks label authority")
        trade_date = outcome["decision_as_of_trade_date"]
        return {
            "owner_type": str(values["owner_type"]),
            "horizon": str(values["horizon_trading_days"]),
            "year": f"{trade_date.year:04d}",
            "month": f"{trade_date.month:02d}",
        }
    trade_date = decision_date or values.get("decision_as_of_trade_date")
    if not isinstance(trade_date, date):
        raise SnapshotWriterError(REASON_SOURCE_SNAPSHOT_CONFLICT, "dated logical role lacks decision date")
    return {"year": f"{trade_date.year:04d}", "month": f"{trade_date.month:02d}"}


def _logical_parquet_path(*, role: str, partition_key: Mapping[str, str]) -> str:
    segments = [role, *(f"{key}={value}" for key, value in partition_key.items())]
    filename = "source_revision_set.parquet" if role == "source_revisions" else "part-00000.parquet"
    return PurePosixPath(*segments, filename).as_posix()


def _reuse_base_file_if_exact(
    written: WrittenDatasetFile,
    base: DatasetSnapshotFile | None,
    *,
    store: LocalContentAddressedStore,
) -> WrittenDatasetFile:
    if base is None:
        return written
    if base.blob.store_backend_hash != store.store_backend_hash:
        raise SnapshotWriterError(REASON_MANIFEST_CONFLICT, "base snapshot store differs from child store")
    comparable = (
        "logical_path", "logical_role", "partition_key_hash", "ordinal", "sha256", "size_bytes", "row_count",
        "schema_fingerprint", "partition_content_hash",
    )
    if any(getattr(written, field) != getattr(base, field) for field in comparable):
        return written
    store.read_blob_bytes(uri=base.content_uri, sha256=base.sha256, size_bytes=base.size_bytes)
    return written.model_copy(update={"uri": base.content_uri})


def _sortable_text(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, int):
        return f"{value:020d}"
    return str(value)


def _partition_key_from_logical_path(logical_path: str, *, logical_role: str) -> dict[str, str]:
    path = PurePosixPath(logical_path)
    if not path.parts or path.parts[0] != logical_role or path.suffix != ".parquet":
        raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet logical path does not match role")
    result: dict[str, str] = {}
    for part in path.parts[1:-1]:
        if "=" not in part:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet partition path is invalid")
        key, value = part.split("=", 1)
        if not key or not value or key in result:
            raise SnapshotWriterError(REASON_PARQUET_FULL_VERIFY_FAILED, "Parquet partition key is invalid")
        result[key] = value
    return result


def _partition_content_hash(
    *, logical_role: str, partition_key: Mapping[str, str], values: Sequence[Mapping[str, Any]]
) -> str:
    return canonical_json_sha256(
        {
            "logical_role": logical_role,
            "partition_key": dict(sorted(partition_key.items())),
            "rows": [canonicalize(dict(value)) for value in values],
        }
    )


class _PartitionContentDigest:
    def __init__(self, *, logical_role: str, partition_key: Mapping[str, str]) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b'{"logical_role":')
        self._digest.update(_canonical_json_bytes(logical_role))
        self._digest.update(b',"partition_key":')
        self._digest.update(_canonical_json_bytes(dict(sorted(partition_key.items()))))
        self._digest.update(b',"rows":[')
        self._count = 0

    def add(self, values: Mapping[str, Any]) -> None:
        if self._count:
            self._digest.update(b",")
        self._digest.update(_canonical_json_bytes(dict(values)))
        self._count += 1

    def hexdigest(self) -> str:
        digest = self._digest.copy()
        digest.update(b"]}")
        return digest.hexdigest()


def _validate_capability_against_request(
    *, build: DatasetBuild, capability_manifest: DatasetCapabilityManifest
) -> None:
    rows = {(row.component, row.capability): row.status for row in capability_manifest.rows}
    readiness = {
        ("MODEL", "MODEL_TRAINING_READY"),
        ("RUNTIME", "RUNTIME_ADVISORY_READY"),
        ("TRADING", "TRADING_EXECUTION_READY"),
    }
    required = {(item.component, item.capability) for item in build.request.required_composite_capabilities if item.required}
    if set(rows) != readiness | required or any(rows[key] != "FULL" for key in required):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "capability rows differ from frozen build request")


def _validate_relational_rows(
    *,
    build: DatasetBuild,
    rows_by_role: Mapping[str, Sequence[dict[str, Any]]],
    capability_manifest: DatasetCapabilityManifest,
) -> tuple[dict[str, Any], tuple[DatasetSnapshotObservation, ...], tuple[DatasetSnapshotLabel, ...]]:
    from backend.services.advisory_phase1.outcome_engine import CalculationEvidenceBundle

    signals = {row["canonical_signal_id"]: row for row in rows_by_role["canonical_signals"]}
    if len(signals) != len(rows_by_role["canonical_signals"]):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "canonical signals are not unique")
    versions = {row["observation_version_id"]: row for row in rows_by_role["observation_versions"]}
    if len(versions) != len(rows_by_role["observation_versions"]):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "observation versions are not unique")
    lineage_by_observation: dict[str, list[dict[str, Any]]] = {}
    for row in rows_by_role["lineage"]:
        if row["observation_version_id"] not in versions or row["canonical_signal_id"] not in signals:
            raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "lineage references missing observation/signal")
        lineage_by_observation.setdefault(row["observation_version_id"], []).append(row)
    observations: list[DatasetSnapshotObservation] = []
    selected_observation_ids: set[str] = set()
    for mapping in rows_by_role["selected_observations"]:
        signal_id = mapping["canonical_signal_id"]
        version_id = mapping["terminal_observation_version_id"]
        version = versions.get(version_id)
        lineages = lineage_by_observation.get(version_id, [])
        if (
            signal_id not in signals
            or version is None
            or version["canonical_signal_id"] != signal_id
            or version["observation_content_hash"] != mapping["terminal_observation_content_hash"]
            or version["observation_revision_no"] != mapping["terminal_revision_no"]
            or version["observation_status"] != "COMPLETE"
            or len(lineages) != 1
        ):
            raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "selected observation mapping is not closed")
        selected_observation_ids.add(version_id)
        lineage = lineages[0]
        observations.append(
            DatasetSnapshotObservation(
                canonical_signal_id=signal_id,
                observation_version_id=version_id,
                evidence_scope=lineage["evidence_scope"],
                oos_interval_id=lineage["oos_interval_id"],
                selector_policy_hash=OBSERVATION_SELECTOR_POLICY_HASH,
            )
        )
    if len(observations) != len(signals):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "each canonical signal requires one selected observation")
    stages = {row["stage_evidence_id"]: row for row in rows_by_role["stage_summaries"]}
    for row in rows_by_role["stage_candidates"]:
        if row["stage_evidence_id"] not in stages:
            raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "stage candidate references missing stage")
    outcomes = {row["label_version_id"]: row for row in rows_by_role["outcome_labels"]}
    if len(outcomes) != len(rows_by_role["outcome_labels"]):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "outcome labels are not unique")
    labels: list[DatasetSnapshotLabel] = []
    for mapping in rows_by_role["selected_labels"]:
        version_id = mapping["terminal_label_version_id"]
        outcome = outcomes.get(version_id)
        if (
            mapping["selection_status"] != "SELECTED"
            or outcome is None
            or outcome["label_key_hash"] != mapping["label_key_hash"]
            or outcome["label_content_hash"] != mapping["terminal_label_content_hash"]
            or outcome["label_revision_no"] != mapping["terminal_label_revision_no"]
            or outcome["maturity_status"] != mapping["terminal_maturity_status"]
            or outcome["outcome_event_status"] != mapping["terminal_outcome_event_status"]
            or outcome["owner_type"] != "CANDIDATE"
            or outcome["observation_version_id"] not in selected_observation_ids
            or outcome["candidate_stage_evidence_id"] not in stages
        ):
            raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "selected label mapping is not closed")
        labels.append(
            DatasetSnapshotLabel(
                label_key_hash=outcome["label_key_hash"],
                label_version_id=version_id,
                canonical_signal_id=outcome["canonical_signal_id"],
                observation_version_id=outcome["observation_version_id"],
                candidate_stage_evidence_id=outcome["candidate_stage_evidence_id"],
                symbol=outcome["symbol"],
                selector_policy_hash=mapping["selection_policy_hash"],
            )
        )
    evidence = rows_by_role["outcome_source_evidence"]
    evidence_by_version = {row["label_version_id"]: row for row in evidence}
    if len(evidence_by_version) != len(evidence) or set(evidence_by_version) != set(outcomes):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "outcome evidence does not cover labels exactly once")
    for version_id, outcome in outcomes.items():
        item = evidence_by_version[version_id]
        if any(
            item[field] != outcome[field]
            for field in (
                "owner_type", "label_key_hash", "canonical_signal_id", "symbol", "horizon_trading_days", "projection",
                "calculation_evidence_sha256", "calculation_evidence_size_bytes", "calculation_evidence_store_backend_hash",
            )
        ):
            raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "outcome evidence descriptor differs from label")
        try:
            raw_evidence = item["calculation_evidence_json"]
            payload = json.loads(raw_evidence) if isinstance(raw_evidence, str) else raw_evidence
            bundle = CalculationEvidenceBundle.model_validate(payload)
        except (TypeError, ValueError) as error:
            raise SnapshotWriterError(REASON_EVIDENCE_BLOB_INVALID, "outcome evidence payload is invalid") from error
        if (
            bundle.evidence_hash != item["calculation_evidence_sha256"]
            or len(bundle.canonical_bytes()) != item["calculation_evidence_size_bytes"]
        ):
            raise SnapshotWriterError(REASON_EVIDENCE_BLOB_INVALID, "outcome evidence payload differs from descriptor")
    expected_universe = [row for row in outcomes.values() if row["owner_type"] == "UNIVERSE"]
    actual_universe = list(rows_by_role["universe_outcomes"])
    if canonicalize(sorted(expected_universe, key=lambda item: item["label_version_id"])) != canonicalize(
        sorted(actual_universe, key=lambda item: item["label_version_id"])
    ):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "universe outcomes differ from outcome authority")
    source_rows = rows_by_role["source_revisions"]
    if (
        not source_rows
        or any(row["member_count"] != len(source_rows) for row in source_rows)
        or {row["source_revision_set_id"] for row in source_rows}
        != {build.request.snapshot_source_revision_set_id}
        or {row["source_revision_set_hash"] for row in source_rows}
        != {build.request.snapshot_source_revision_set_hash}
        or {row["query_registry_hash"] for row in source_rows} != {build.request.query_registry_hash}
        or {
            row["requested_source_cutoff"].date()
            if isinstance(row["requested_source_cutoff"], datetime)
            else row["requested_source_cutoff"]
            for row in source_rows
        }
        != {build.request.requested_source_cutoff}
        or {row["label_as_of_ts"] for row in source_rows} != {build.request.label_as_of_ts}
        or {row["research_only"] for row in source_rows} != {True}
    ):
        raise SnapshotWriterError(REASON_RELATIONAL_CLOSURE_FAILED, "source revision member count is not closed")
    summary = {
        "canonical_signal_count": len(signals),
        "selected_observation_count": len(observations),
        "selected_label_count": len(labels),
        "outcome_label_count": len(outcomes),
        "outcome_evidence_count": len(evidence),
        "schema_descriptor_count": len(SNAPSHOT_ARROW_SCHEMAS_V1),
        "capability_manifest_hash": capability_manifest.manifest_hash,
    }
    return summary, tuple(observations), tuple(labels)


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(canonicalize(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def _pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, "pyarrow==21.0.0 is required for Batch D") from error
    if pa.__version__ != "21.0.0":
        raise SnapshotWriterError(REASON_ARROW_SCHEMA_CONFLICT, f"Batch D requires pyarrow 21.0.0, found {pa.__version__}")
    return pa, pq
