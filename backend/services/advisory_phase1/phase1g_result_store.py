"""External content-addressed, atomic no-replace Phase 1G artifact store."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1G_RESULT_STORE_LAYOUT_POLICY,
    REASON_ATTEMPT_RECEIPT_STORE_FAILED,
    REASON_BATCH_RECEIPT_STORE_FAILED,
    REASON_RESULT_STORE_FAILED,
    Phase1GAttemptReceipt,
    Phase1GBatchAttemptReceipt,
    Phase1GCaptureResult,
    Phase1GContractError,
    Phase1GOutputArtifactKind,
    Phase1GOutputArtifactRef,
)


_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
logger = logging.getLogger(__name__)


class Phase1GResultStoreError(Phase1GContractError):
    pass


@dataclass(frozen=True)
class StoredPhase1GArtifact:
    ref: Phase1GOutputArtifactRef
    path: Path
    idempotent: bool


Phase1GStoredModel = Phase1GCaptureResult | Phase1GAttemptReceipt | Phase1GBatchAttemptReceipt


class Phase1GResultStore:
    """Publish canonical typed artifacts without overwriting an existing identity."""

    def __init__(self, *, root: Path) -> None:
        self._root = _validate_external_root(root)
        self._store_policy_hash = str(PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash)

    @property
    def root(self) -> Path:
        return self._root

    @property
    def store_policy_hash(self) -> str:
        return self._store_policy_hash

    def publish_result(self, result: Phase1GCaptureResult) -> StoredPhase1GArtifact:
        return self._publish(
            kind=Phase1GOutputArtifactKind.CAPTURE_RESULT,
            identity=str(result.capture_result_hash),
            model=result,
        )

    def publish_attempt(self, receipt: Phase1GAttemptReceipt) -> StoredPhase1GArtifact:
        return self._publish(
            kind=Phase1GOutputArtifactKind.ATTEMPT_RECEIPT,
            identity=str(receipt.attempt_receipt_hash),
            model=receipt,
        )

    def publish_batch(self, receipt: Phase1GBatchAttemptReceipt) -> StoredPhase1GArtifact:
        return self._publish(
            kind=Phase1GOutputArtifactKind.BATCH_RECEIPT,
            identity=str(receipt.batch_attempt_receipt_hash),
            model=receipt,
        )

    def load(self, ref: Phase1GOutputArtifactRef) -> Phase1GStoredModel:
        if ref.store_policy_hash != self._store_policy_hash:
            raise self._error(ref.artifact_kind, "output ref store policy does not match this Phase 1G store")
        path = self._destination(kind=ref.artifact_kind, identity=ref.semantic_content_hash)
        if ref.relative_path != path.relative_to(self._root).as_posix():
            raise self._error(ref.artifact_kind, "output ref path does not match its content-addressed identity")
        raw = self._read_exact(path=path, kind=ref.artifact_kind)
        if hashlib.sha256(raw).hexdigest() != ref.file_sha256:
            raise self._error(ref.artifact_kind, "stored artifact raw file hash does not match its ref")
        return self._parse_model(kind=ref.artifact_kind, raw=raw, identity=ref.semantic_content_hash)

    def load_by_identity(
        self, *, kind: Phase1GOutputArtifactKind, identity: str
    ) -> Phase1GStoredModel:
        identity = _sha256(identity, field_name="artifact identity", kind=kind)
        path = self._destination(kind=kind, identity=identity)
        raw = self._read_exact(path=path, kind=kind)
        return self._parse_model(kind=kind, raw=raw, identity=identity)

    def _publish(
        self,
        *,
        kind: Phase1GOutputArtifactKind,
        identity: str,
        model: Phase1GStoredModel,
    ) -> StoredPhase1GArtifact:
        identity = _sha256(identity, field_name="artifact identity", kind=kind)
        payload = model.model_dump(mode="json")
        content = (canonical_json_text(payload) + "\n").encode("utf-8")
        destination = self._destination(kind=kind, identity=identity)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            _assert_contained(path=destination, root=self._root)
            _assert_no_reparse_path(path=destination.parent, root=self._root)
        except OSError as exc:
            raise self._error(kind, "unable to prepare Phase 1G result directory", errno=exc.errno) from exc
        if destination.exists():
            return self._existing(kind=kind, identity=identity, destination=destination, expected=content)
        temp_path: Path | None = None
        try:
            descriptor, temp_name = tempfile.mkstemp(prefix=f".{identity}.", suffix=".tmp", dir=destination.parent)
            temp_path = Path(temp_name)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            published = _publish_no_replace(source=temp_path, target=destination)
            if not published:
                return self._existing(kind=kind, identity=identity, destination=destination, expected=content)
            persisted = self._read_exact(path=destination, kind=kind)
            if persisted != content:
                raise self._error(kind, "Phase 1G artifact post-write readback differs from canonical bytes")
            self._parse_model(kind=kind, raw=persisted, identity=identity)
            return self._stored(kind=kind, identity=identity, path=destination, raw=persisted, idempotent=False)
        except Phase1GResultStoreError:
            raise
        except OSError as exc:
            raise self._error(kind, "unable to publish Phase 1G artifact", errno=exc.errno) from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.warning(
                        "Phase 1G temporary artifact cleanup failed "
                        "reason_code=%s artifact_kind=%s identity_prefix=%s errno=%s",
                        _store_reason(kind),
                        kind.value,
                        identity[:12],
                        exc.errno,
                    )

    def _existing(
        self,
        *,
        kind: Phase1GOutputArtifactKind,
        identity: str,
        destination: Path,
        expected: bytes,
    ) -> StoredPhase1GArtifact:
        persisted = self._read_exact(path=destination, kind=kind)
        if persisted != expected:
            raise self._error(kind, "Phase 1G content-addressed identity collision")
        self._parse_model(kind=kind, raw=persisted, identity=identity)
        return self._stored(kind=kind, identity=identity, path=destination, raw=persisted, idempotent=True)

    def _stored(
        self,
        *,
        kind: Phase1GOutputArtifactKind,
        identity: str,
        path: Path,
        raw: bytes,
        idempotent: bool,
    ) -> StoredPhase1GArtifact:
        return StoredPhase1GArtifact(
            ref=Phase1GOutputArtifactRef(
                artifact_kind=kind,
                store_policy_hash=self._store_policy_hash,
                relative_path=path.relative_to(self._root).as_posix(),
                semantic_content_hash=identity,
                file_sha256=hashlib.sha256(raw).hexdigest(),
            ),
            path=path,
            idempotent=idempotent,
        )

    def _read_exact(self, *, path: Path, kind: Phase1GOutputArtifactKind) -> bytes:
        try:
            _assert_contained(path=path, root=self._root)
            _assert_no_reparse_path(path=path, root=self._root)
            return path.read_bytes()
        except Phase1GResultStoreError:
            raise
        except OSError as exc:
            raise self._error(kind, "stored Phase 1G artifact is unavailable", errno=exc.errno) from exc

    def _parse_model(
        self,
        *,
        kind: Phase1GOutputArtifactKind,
        raw: bytes,
        identity: str,
    ) -> Phase1GStoredModel:
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise self._error(kind, "stored Phase 1G artifact is not valid UTF-8 JSON") from exc
        if not isinstance(document, dict):
            raise self._error(kind, "stored Phase 1G artifact must be a JSON object")
        try:
            if kind is Phase1GOutputArtifactKind.CAPTURE_RESULT:
                model: Phase1GStoredModel = Phase1GCaptureResult.model_validate(document)
                actual = model.capture_result_hash
            elif kind is Phase1GOutputArtifactKind.ATTEMPT_RECEIPT:
                model = Phase1GAttemptReceipt.model_validate(document)
                actual = model.attempt_receipt_hash
            else:
                model = Phase1GBatchAttemptReceipt.model_validate(document)
                actual = model.batch_attempt_receipt_hash
        except ValueError as exc:
            raise self._error(kind, "stored Phase 1G artifact contract is invalid") from exc
        if actual != identity:
            raise self._error(kind, "stored Phase 1G artifact content identity differs from its path")
        canonical = (canonical_json_text(model.model_dump(mode="json")) + "\n").encode("utf-8")
        if canonical != raw:
            raise self._error(kind, "stored Phase 1G artifact is not canonical JSON")
        return model

    def _destination(self, *, kind: Phase1GOutputArtifactKind, identity: str) -> Path:
        namespace = {
            Phase1GOutputArtifactKind.CAPTURE_RESULT: "results",
            Phase1GOutputArtifactKind.ATTEMPT_RECEIPT: "attempts",
            Phase1GOutputArtifactKind.BATCH_RECEIPT: "batches",
        }[kind]
        return self._root / namespace / identity[:2] / f"{identity}.json"

    @staticmethod
    def _error(kind: Phase1GOutputArtifactKind, message: str, *, errno: int | None = None) -> Phase1GResultStoreError:
        context = {"errno": errno} if errno is not None else None
        return Phase1GResultStoreError(_store_reason(kind), message, context=context)


def _store_reason(kind: Phase1GOutputArtifactKind) -> str:
    if kind is Phase1GOutputArtifactKind.ATTEMPT_RECEIPT:
        return REASON_ATTEMPT_RECEIPT_STORE_FAILED
    if kind is Phase1GOutputArtifactKind.BATCH_RECEIPT:
        return REASON_BATCH_RECEIPT_STORE_FAILED
    return REASON_RESULT_STORE_FAILED


def _publish_no_replace(*, source: Path, target: Path) -> bool:
    if os.name == "nt":
        import ctypes

        move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        move_file_ex.restype = ctypes.c_int
        movefile_write_through = 0x00000008
        if move_file_ex(str(source), str(target), movefile_write_through):
            return True
        error_code = ctypes.get_last_error()
        if error_code in {80, 183}:
            return False
        raise ctypes.WinError(error_code)
    try:
        os.link(source, target)
    except FileExistsError:
        return False
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return True


def _validate_external_root(root: Path) -> Path:
    expanded = root.expanduser()
    normalized = str(expanded).replace("\\", "/").lower()
    if not expanded.is_absolute():
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G result root must be absolute")
    if normalized.startswith("//wsl$/") or normalized.startswith("//wsl.localhost/") or normalized.startswith("/mnt/"):
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G result root cannot be a WSL path")
    prospective = expanded.resolve(strict=False)
    repository_root = Path(__file__).resolve().parents[3]
    try:
        prospective.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G result root must be outside the repository")
    existing_ancestor = expanded
    while not existing_ancestor.exists() and existing_ancestor != existing_ancestor.parent:
        existing_ancestor = existing_ancestor.parent
    _assert_existing_root_chain_has_no_reparse(existing_ancestor)
    try:
        expanded.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G result root cannot be created") from exc
    _assert_existing_root_chain_has_no_reparse(expanded)
    try:
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G result root must already exist") from exc
    if not resolved.is_dir():
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G result root must be a directory")
    return resolved


def _assert_contained(*, path: Path, root: Path) -> None:
    try:
        candidate = path.parent.resolve(strict=True) / path.name
        candidate.relative_to(root)
    except (OSError, ValueError) as exc:
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G artifact path escapes its root") from exc


def _assert_no_reparse_path(*, path: Path, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise Phase1GResultStoreError(REASON_RESULT_STORE_FAILED, "Phase 1G artifact path escapes its root") from exc
    current = root
    for part in relative.parts:
        current = current / part
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise Phase1GResultStoreError(
                REASON_RESULT_STORE_FAILED,
                "Phase 1G artifact path contains a symlink or reparse point",
            )


def _assert_existing_root_chain_has_no_reparse(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            attributes = os.lstat(current)
        except OSError as exc:
            raise Phase1GResultStoreError(
                REASON_RESULT_STORE_FAILED,
                "Phase 1G result root must already exist",
            ) from exc
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise Phase1GResultStoreError(
                REASON_RESULT_STORE_FAILED,
                "Phase 1G result root cannot traverse a symlink or reparse point",
            )


def _sha256(value: str, *, field_name: str, kind: Phase1GOutputArtifactKind) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise Phase1GResultStoreError(
            _store_reason(kind), f"{field_name} must be lowercase sha256"
        )
    return normalized
