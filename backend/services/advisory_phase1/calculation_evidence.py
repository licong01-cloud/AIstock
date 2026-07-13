"""Real local content-addressed storage for Phase 1C-3 calculation evidence."""

from __future__ import annotations

import ctypes
import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.outcome_engine import CalculationEvidenceBundle


LOCAL_CALCULATION_EVIDENCE_STORE_SCHEMA_VERSION = "advisory_phase1_local_calculation_evidence_store_v1"
REASON_CAS_CONTENT_CONFLICT = "ADVISORY_PHASE1C3_CAS_CONTENT_CONFLICT"
REASON_STORE_INVALID = "ADVISORY_PHASE1C3_EVIDENCE_STORE_INVALID"


class CalculationEvidenceStoreError(RuntimeError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


@dataclass(frozen=True)
class StoredCalculationEvidence:
    uri: str
    sha256: str
    size_bytes: int
    store_backend_hash: str


class LocalCalculationEvidenceStore:
    """Create-if-absent local CAS with byte-for-byte exact retry comparison."""

    def __init__(self, *, root: Path, repository_root: Path, store_identity: dict[str, Any]) -> None:
        if not root.is_absolute():
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "store root must be an explicit absolute path")
        if not repository_root.is_absolute():
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "repository root must be an explicit absolute path")
        if not store_identity:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "store identity cannot be empty")
        expected_mode = "WINDOWS_FILE_AND_DIRECTORY_FLUSH_V1" if os.name == "nt" else "POSIX_FILE_AND_DIRECTORY_FSYNC_V1"
        if store_identity.get("durability_mode") != expected_mode:
            raise CalculationEvidenceStoreError(
                REASON_STORE_INVALID,
                f"store identity durability mode must explicitly declare {expected_mode} on this platform",
            )
        if store_identity.get("atomic_publish_mode") != "HARDLINK_CREATE_IF_ABSENT_V1":
            raise CalculationEvidenceStoreError(
                REASON_STORE_INVALID,
                "store identity must declare HARDLINK_CREATE_IF_ABSENT_V1",
            )
        self._root = root.resolve()
        repository = repository_root.resolve()
        if self._root == repository or repository in self._root.parents:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "evidence store root must be outside the repository")
        self._durability_mode = expected_mode
        self._store_backend_hash = canonical_json_sha256(
            {
                "schema_version": LOCAL_CALCULATION_EVIDENCE_STORE_SCHEMA_VERSION,
                "root": str(self._root),
                "identity": canonicalize(store_identity),
            }
        )

    @property
    def store_backend_hash(self) -> str:
        return self._store_backend_hash

    def put(self, bundle: CalculationEvidenceBundle) -> StoredCalculationEvidence:
        payload = bundle.canonical_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != bundle.evidence_hash:
            raise CalculationEvidenceStoreError(REASON_CAS_CONTENT_CONFLICT, "bundle canonical bytes do not match evidence hash")
        target = self._root / "blobs" / "sha256" / digest[:2] / digest
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = target.parent / f".{digest}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        try:
            with staging.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(staging, target)
            except FileExistsError:
                self._compare_existing(target, payload, digest)
            finally:
                staging.unlink(missing_ok=True)
            self._fsync_directory(target.parent)
        except OSError as error:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, f"cannot persist calculation evidence: {error}") from error
        finally:
            if staging.exists():
                try:
                    staging.unlink()
                    self._fsync_directory(target.parent)
                except OSError as error:
                    raise CalculationEvidenceStoreError(
                        REASON_STORE_INVALID,
                        f"cannot clean calculation evidence staging file: {error}",
                    ) from error
        return StoredCalculationEvidence(
            uri=target.as_uri(),
            sha256=digest,
            size_bytes=len(payload),
            store_backend_hash=self._store_backend_hash,
        )

    def _compare_existing(self, target: Path, expected: bytes, expected_hash: str) -> None:
        try:
            actual = target.read_bytes()
        except OSError as error:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, f"cannot read existing evidence blob: {error}") from error
        if len(actual) != len(expected) or hashlib.sha256(actual).hexdigest() != expected_hash or actual != expected:
            raise CalculationEvidenceStoreError(
                REASON_CAS_CONTENT_CONFLICT,
                "existing evidence blob has the requested hash path but different bytes",
            )

    def _fsync_directory(self, directory: Path) -> None:
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
            None,
            open_existing,
            file_flag_backup_semantics,
            None,
        )
        if handle == invalid_handle_value:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "cannot open evidence directory for durable flush")
        try:
            if not kernel32.FlushFileBuffers(handle):
                raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "cannot flush evidence directory")
        finally:
            kernel32.CloseHandle(handle)
