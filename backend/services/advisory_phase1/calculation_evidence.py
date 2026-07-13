"""Calculation evidence storage backed by the shared local CAS primitive."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.services.advisory_phase1.dataset_store import (
    REASON_CAS_CONTENT_CONFLICT as _DATASET_CAS_CONFLICT,
    LocalContentAddressedStore,
    LocalContentAddressedStoreError,
)
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
    """Public calculation-evidence API preserved over the shared local CAS."""

    def __init__(self, *, root: Path, repository_root: Path, store_identity: dict[str, Any]) -> None:
        if not root.is_absolute():
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "store root must be an explicit absolute path")
        if not repository_root.is_absolute():
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "repository root must be an explicit absolute path")
        if not store_identity:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "store identity cannot be empty")
        resolved_root = root.resolve()
        resolved_repository = repository_root.resolve()
        if resolved_root == resolved_repository or resolved_repository in resolved_root.parents:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "evidence store root must be outside the repository")
        try:
            self._store = LocalContentAddressedStore(
                root=root,
                repository_root=repository_root,
                store_identity=store_identity,
                schema_version=LOCAL_CALCULATION_EVIDENCE_STORE_SCHEMA_VERSION,
            )
        except LocalContentAddressedStoreError as error:
            raise self._translate(error) from error

    @property
    def store_backend_hash(self) -> str:
        return self._store.store_backend_hash

    def put(self, bundle: CalculationEvidenceBundle) -> StoredCalculationEvidence:
        payload = bundle.canonical_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != bundle.evidence_hash:
            raise CalculationEvidenceStoreError(
                REASON_CAS_CONTENT_CONFLICT,
                "bundle canonical bytes do not match evidence hash",
            )
        try:
            stored = self._store.put_blob_bytes(payload)
        except LocalContentAddressedStoreError as error:
            raise self._translate(error) from error
        return StoredCalculationEvidence(
            uri=stored.uri,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
            store_backend_hash=stored.store_backend_hash,
        )

    def get(
        self,
        *,
        uri: str,
        sha256: str,
        size_bytes: int,
        store_backend_hash: str,
    ) -> CalculationEvidenceBundle:
        if store_backend_hash != self._store.store_backend_hash:
            raise CalculationEvidenceStoreError(REASON_STORE_INVALID, "evidence store backend identity does not match")
        try:
            payload = self._store.read_blob_bytes(uri=uri, sha256=sha256, size_bytes=size_bytes)
        except LocalContentAddressedStoreError as error:
            raise self._translate(error) from error
        try:
            import json

            decoded = json.loads(payload.decode("utf-8"))
            bundle = CalculationEvidenceBundle.model_validate(decoded)
        except (UnicodeDecodeError, ValueError) as error:
            raise CalculationEvidenceStoreError(REASON_CAS_CONTENT_CONFLICT, "calculation evidence payload is invalid") from error
        if bundle.evidence_hash != sha256:
            raise CalculationEvidenceStoreError(REASON_CAS_CONTENT_CONFLICT, "calculation evidence canonical hash is invalid")
        return bundle

    @staticmethod
    def _translate(error: LocalContentAddressedStoreError) -> CalculationEvidenceStoreError:
        reason = REASON_CAS_CONTENT_CONFLICT if error.reason_code == _DATASET_CAS_CONFLICT else REASON_STORE_INVALID
        return CalculationEvidenceStoreError(reason, str(error).split(": ", 1)[-1])
