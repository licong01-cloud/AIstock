"""Durable local content-addressed storage for Phase 1C-3 datasets.

This module owns the filesystem primitive shared by calculation evidence and
Batch D dataset snapshots.  It deliberately contains no database, scheduler,
runtime, or model-training integration.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize


LOCAL_DATASET_STORE_SCHEMA_VERSION = "advisory_phase1_local_dataset_store_v1"
REASON_CAS_CONTENT_CONFLICT = "ADVISORY_PHASE1C3_CAS_CONTENT_CONFLICT"
REASON_DATASET_STORE_INVALID = "ADVISORY_PHASE1C3_DATASET_STORE_INVALID"
REASON_DATASET_STORE_CAPACITY_INSUFFICIENT = "ADVISORY_PHASE1C3_DATASET_STORE_CAPACITY_INSUFFICIENT"

MEBIBYTE = 1024 * 1024
GIBIBYTE = 1024 * 1024 * 1024
_SAFE_ID_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$")


class LocalContentAddressedStoreError(RuntimeError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


@dataclass(frozen=True)
class StoredCasObject:
    uri: str
    sha256: str
    size_bytes: int
    store_backend_hash: str


@dataclass(frozen=True)
class DatasetStorageCapacity:
    logical_source_bytes: int
    projected_bytes: int
    reserved_bytes: int
    min_free_after_write: int
    available_bytes: int
    volume_capacity: int

    @property
    def required_bytes(self) -> int:
        return self.projected_bytes + self.reserved_bytes + self.min_free_after_write


class LocalContentAddressedStore:
    """Create-if-absent local CAS with durable exact retry semantics."""

    def __init__(
        self,
        *,
        root: Path,
        repository_root: Path,
        store_identity: dict[str, Any],
        schema_version: str = LOCAL_DATASET_STORE_SCHEMA_VERSION,
    ) -> None:
        if not root.is_absolute() or not repository_root.is_absolute():
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_INVALID,
                "store and repository roots must be explicit absolute paths",
            )
        if not store_identity:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "store identity cannot be empty")
        expected_durability = self.expected_durability_mode()
        if store_identity.get("durability_mode") != expected_durability:
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_INVALID,
                f"store identity durability mode must explicitly declare {expected_durability}",
            )
        if store_identity.get("atomic_publish_mode") != "HARDLINK_CREATE_IF_ABSENT_V1":
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_INVALID,
                "store identity must declare HARDLINK_CREATE_IF_ABSENT_V1",
            )
        self._root = root.resolve()
        self._repository_root = repository_root.resolve()
        if (
            self._root == self._repository_root
            or self._repository_root in self._root.parents
            or self._root in self._repository_root.parents
        ):
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_INVALID,
                "store root must be outside the repository",
            )
        self._schema_version = schema_version
        self._identity = canonicalize(store_identity)
        self._store_backend_hash = canonical_json_sha256(
            {
                "schema_version": schema_version,
                "root": str(self._root),
                "identity": self._identity,
            }
        )

    @staticmethod
    def expected_durability_mode() -> str:
        return "WINDOWS_FILE_AND_DIRECTORY_FLUSH_V1" if os.name == "nt" else "POSIX_FILE_AND_DIRECTORY_FSYNC_V1"

    @property
    def root(self) -> Path:
        return self._root

    @property
    def store_backend_hash(self) -> str:
        return self._store_backend_hash

    def staging_path(self, *, build_id: str, attempt_id: str, logical_path: str) -> Path:
        self._validate_id_component(build_id, field_name="build_id")
        self._validate_id_component(attempt_id, field_name="attempt_id")
        relative = PurePosixPath(logical_path)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "logical staging path is unsafe")
        staging_root = (self._root / "staging" / build_id / attempt_id).resolve()
        self._assert_under(path=staging_root, allowed_root=(self._root / "staging").resolve())
        path = staging_root.joinpath(*relative.parts)
        self._assert_under(path=path, allowed_root=staging_root)
        return path

    def ensure_capacity(self, *, logical_source_bytes: int) -> DatasetStorageCapacity:
        if logical_source_bytes < 0:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "logical source bytes cannot be negative")
        volume_probe = self._root
        while not volume_probe.exists() and volume_probe.parent != volume_probe:
            volume_probe = volume_probe.parent
        try:
            usage = shutil.disk_usage(volume_probe)
        except OSError as error:
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_CAPACITY_INSUFFICIENT,
                f"cannot inspect dataset-store capacity: {type(error).__name__}",
            ) from error
        projected = max(512 * MEBIBYTE, 2 * logical_source_bytes)
        capacity = DatasetStorageCapacity(
            logical_source_bytes=logical_source_bytes,
            projected_bytes=projected,
            reserved_bytes=GIBIBYTE,
            min_free_after_write=max(2 * GIBIBYTE, usage.total // 10),
            available_bytes=usage.free,
            volume_capacity=usage.total,
        )
        if capacity.available_bytes < capacity.required_bytes:
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_CAPACITY_INSUFFICIENT,
                "available bytes cannot satisfy projected dataset write, reservation, and minimum free space",
            )
        return capacity

    def put_blob_bytes(self, payload: bytes) -> StoredCasObject:
        digest = hashlib.sha256(payload).hexdigest()
        path = self._root / "blobs" / "sha256" / digest[:2] / digest
        self._publish_exact(path=path, payload=payload, expected_sha256=digest)
        return self._stored(path=path, digest=digest, size_bytes=len(payload))

    def put_document_bytes(self, *, kind: str, payload: bytes) -> StoredCasObject:
        if kind not in {"manifests", "promotion_receipts"}:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "unsupported dataset document kind")
        digest = hashlib.sha256(payload).hexdigest()
        path = self._root / kind / "sha256" / digest[:2] / f"{digest}.json"
        self._publish_exact(path=path, payload=payload, expected_sha256=digest)
        return self._stored(path=path, digest=digest, size_bytes=len(payload))

    def verify_document_bytes(self, *, kind: str, payload: bytes) -> StoredCasObject:
        """Reopen one canonical manifest/receipt path without publishing it."""

        if kind not in {"manifests", "promotion_receipts"}:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "unsupported dataset document kind")
        digest = hashlib.sha256(payload).hexdigest()
        path = (self._root / kind / "sha256" / digest[:2] / f"{digest}.json").resolve()
        try:
            actual = path.read_bytes()
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "canonical dataset document is missing") from error
        if actual != payload:
            raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "canonical dataset document bytes conflict")
        stored = self._stored(path=path, digest=digest, size_bytes=len(payload))
        self.verify_object(stored)
        return stored

    def publish_staging_file(self, *, staging_uri: str, sha256: str, size_bytes: int) -> StoredCasObject:
        staging = self.path_from_uri(staging_uri, allowed_root=self._root / "staging")
        try:
            payload = staging.read_bytes()
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot read staging file") from error
        if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != sha256:
            raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "staging bytes do not match immutable descriptor")
        stored = self.put_blob_bytes(payload)
        if stored.sha256 != sha256 or stored.size_bytes != size_bytes:
            raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "published blob identity changed")
        return stored

    def read_bytes(self, *, uri: str, sha256: str, size_bytes: int) -> bytes:
        if not _is_sha256(sha256) or size_bytes < 1:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "CAS descriptor is invalid")
        path = self.path_from_uri(uri, allowed_root=self._root)
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot read CAS object") from error
        if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != sha256:
            raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "CAS bytes do not match immutable descriptor")
        return payload

    def read_blob_bytes(self, *, uri: str, sha256: str, size_bytes: int) -> bytes:
        """Read only the canonical blob path for a content descriptor."""

        if not _is_sha256(sha256) or size_bytes < 1:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "CAS blob descriptor is invalid")
        expected = (self._root / "blobs" / "sha256" / sha256[:2] / sha256).resolve()
        path = self.path_from_uri(uri, allowed_root=self._root / "blobs" / "sha256")
        if path != expected:
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_INVALID,
                "CAS blob URI does not match its canonical sha256 path",
            )
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot read CAS blob") from error
        if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != sha256:
            raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "CAS blob bytes do not match descriptor")
        return payload

    def describe_blob(self, *, uri: str, sha256: str, size_bytes: int) -> StoredCasObject:
        self.read_blob_bytes(uri=uri, sha256=sha256, size_bytes=size_bytes)
        return StoredCasObject(
            uri=uri,
            sha256=sha256,
            size_bytes=size_bytes,
            store_backend_hash=self._store_backend_hash,
        )

    def is_canonical_blob_uri(self, *, uri: str, sha256: str) -> bool:
        if not _is_sha256(sha256):
            return False
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            return False
        raw_path = unquote(parsed.path)
        if os.name == "nt" and len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
        return Path(raw_path).resolve() == (self._root / "blobs" / "sha256" / sha256[:2] / sha256).resolve()

    def verify_object(self, stored: StoredCasObject) -> None:
        if stored.store_backend_hash != self._store_backend_hash:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "CAS backend identity does not match")
        self.read_bytes(uri=stored.uri, sha256=stored.sha256, size_bytes=stored.size_bytes)

    def cleanup_attempt_staging(self, *, build_id: str, attempt_id: str) -> None:
        self._validate_id_component(build_id, field_name="build_id")
        self._validate_id_component(attempt_id, field_name="attempt_id")
        staging_root = (self._root / "staging").resolve()
        target = (staging_root / build_id / attempt_id).resolve()
        self._assert_under(path=target, allowed_root=staging_root)
        if not target.exists():
            return
        try:
            shutil.rmtree(target)
            self._flush_directory(target.parent)
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot clean attempt staging") from error

    def path_from_uri(self, uri: str, *, allowed_root: Path) -> Path:
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "dataset URI must be a local file URI")
        raw_path = unquote(parsed.path)
        if os.name == "nt" and len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
        try:
            resolved = Path(raw_path).resolve(strict=True)
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "dataset URI cannot be resolved") from error
        allowed = allowed_root.resolve()
        if resolved != allowed and allowed not in resolved.parents:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "dataset URI is outside the allowed store root")
        return resolved

    def _publish_exact(self, *, path: Path, payload: bytes, expected_sha256: str) -> None:
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "payload hash does not match requested CAS identity")
        self._assert_under_root(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._assert_same_volume(path.parent)
        staging = path.parent / f".{expected_sha256}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        try:
            with staging.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(staging, path)
            except FileExistsError:
                self._compare_existing(path=path, payload=payload, expected_sha256=expected_sha256)
            finally:
                staging.unlink(missing_ok=True)
            self._flush_directory(path.parent)
            actual = path.read_bytes()
            if actual != payload:
                raise LocalContentAddressedStoreError(REASON_CAS_CONTENT_CONFLICT, "published CAS bytes differ after reopen")
        except LocalContentAddressedStoreError:
            raise
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot publish CAS object") from error
        finally:
            if staging.exists():
                try:
                    staging.unlink()
                    self._flush_directory(path.parent)
                except OSError as error:
                    raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot clean CAS staging file") from error

    def _stored(self, *, path: Path, digest: str, size_bytes: int) -> StoredCasObject:
        return StoredCasObject(
            uri=path.as_uri(),
            sha256=digest,
            size_bytes=size_bytes,
            store_backend_hash=self._store_backend_hash,
        )

    def _assert_under_root(self, path: Path) -> None:
        self._assert_under(path=path, allowed_root=self._root)

    @staticmethod
    def _assert_under(*, path: Path, allowed_root: Path) -> None:
        resolved = path.resolve()
        allowed = allowed_root.resolve()
        if resolved != allowed and allowed not in resolved.parents:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "dataset path is outside the allowed root")

    @staticmethod
    def _validate_id_component(value: str, *, field_name: str) -> None:
        if value in {".", ".."} or _SAFE_ID_COMPONENT.fullmatch(value) is None:
            raise LocalContentAddressedStoreError(
                REASON_DATASET_STORE_INVALID,
                f"{field_name} must be one safe path component",
            )

    def _assert_same_volume(self, directory: Path) -> None:
        try:
            directory_stat = os.stat(directory)
            root_stat = os.stat(self._root if self._root.exists() else self._root.parent)
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot inspect dataset-store volume") from error
        if directory_stat.st_dev != root_stat.st_dev:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "staging and CAS must share one filesystem volume")

    @staticmethod
    def _compare_existing(*, path: Path, payload: bytes, expected_sha256: str) -> None:
        try:
            actual = path.read_bytes()
        except OSError as error:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot read existing CAS object") from error
        if len(actual) != len(payload) or hashlib.sha256(actual).hexdigest() != expected_sha256 or actual != payload:
            raise LocalContentAddressedStoreError(
                REASON_CAS_CONTENT_CONFLICT,
                "existing CAS object has the requested identity but different bytes",
            )

    @staticmethod
    def _flush_directory(directory: Path) -> None:
        if os.name != "nt":
            descriptor = os.open(str(directory), os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return
        kernel32 = ctypes.windll.kernel32
        generic_write = 0x40000000
        file_share_read = 0x00000001
        file_share_write = 0x00000002
        file_share_delete = 0x00000004
        open_existing = 3
        file_flag_backup_semantics = 0x02000000
        invalid_handle_value = ctypes.c_void_p(-1).value
        handle = kernel32.CreateFileW(
            str(directory),
            generic_write,
            file_share_read | file_share_write | file_share_delete,
            None, open_existing, file_flag_backup_semantics, None,
        )
        if handle == invalid_handle_value:
            raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot open CAS directory for durable flush")
        try:
            if not kernel32.FlushFileBuffers(handle):
                raise LocalContentAddressedStoreError(REASON_DATASET_STORE_INVALID, "cannot flush CAS directory")
        finally:
            kernel32.CloseHandle(handle)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)
