"""External atomic content-addressed store for Phase 1G G5 evidence."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_phase1.phase1g_contract import Phase1GExecutionBatchPlan

from .phase1g_dev_evidence_contract import (
    G5_STORE_POLICY_HASH,
    G5StoredModel,
    EvidenceKind,
    Phase1GDevEvidenceError,
    Phase1GDevEvidenceRef,
    Phase1GDevEvidenceSummary,
    Phase1GDevExecutionManifest,
    Phase1GDevInputInventoryReceipt,
    Phase1GDevPersistentReceipt,
    Phase1GDevRollbackReceipt,
    REASON_EVIDENCE_STORE_FAILED,
)


LOGGER = logging.getLogger(__name__)
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400


@dataclass(frozen=True)
class StoredG5Evidence:
    ref: Phase1GDevEvidenceRef
    path: Path
    idempotent: bool


class Phase1GDevEvidenceStore:
    def __init__(self, *, root: Path) -> None:
        self._root = _validate_external_root(root)

    @property
    def root(self) -> Path:
        return self._root

    @property
    def store_policy_hash(self) -> str:
        return G5_STORE_POLICY_HASH

    def publish(self, model: G5StoredModel) -> StoredG5Evidence:
        kind, identity = _model_identity(model)
        content = (canonical_json_text(model.model_dump(mode="json")) + "\n").encode("utf-8")
        destination = self._destination(kind=kind, identity=identity)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            _assert_contained(path=destination, root=self._root)
            _assert_no_reparse_path(path=destination.parent, root=self._root)
        except (OSError, ValueError) as exc:
            raise _store_error("unable to prepare G5 evidence directory") from exc
        if destination.exists():
            return self._existing(
                kind=kind,
                identity=identity,
                destination=destination,
                expected=content,
            )
        temp_path: Path | None = None
        try:
            descriptor, temp_name = tempfile.mkstemp(
                prefix=f".{identity}.", suffix=".tmp", dir=destination.parent
            )
            temp_path = Path(temp_name)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            if not _publish_no_replace(source=temp_path, target=destination):
                return self._existing(
                    kind=kind,
                    identity=identity,
                    destination=destination,
                    expected=content,
                )
            persisted = _read_exact(path=destination, root=self._root)
            if persisted != content:
                raise _store_error("G5 evidence post-write byte readback differs")
            self._parse(kind=kind, identity=identity, raw=persisted)
            return self._stored(
                kind=kind,
                identity=identity,
                path=destination,
                raw=persisted,
                idempotent=False,
            )
        except Phase1GDevEvidenceError:
            raise
        except OSError as exc:
            raise _store_error("unable to publish G5 evidence") from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError as exc:
                    LOGGER.warning(
                        "phase1g_g5_temp_cleanup_failed identity_prefix=%s errno=%s",
                        identity[:12],
                        exc.errno,
                    )

    def load(self, ref: Phase1GDevEvidenceRef) -> G5StoredModel:
        if ref.store_policy_hash != G5_STORE_POLICY_HASH:
            raise _store_error("G5 evidence ref store policy is invalid")
        path = self._destination(
            kind=ref.evidence_kind,
            identity=ref.semantic_content_hash,
        )
        if ref.relative_path != path.relative_to(self._root).as_posix():
            raise _store_error("G5 evidence ref path differs from its identity")
        raw = _read_exact(path=path, root=self._root)
        if hashlib.sha256(raw).hexdigest() != ref.file_sha256:
            raise _store_error("G5 evidence raw hash differs from its ref")
        return self._parse(
            kind=ref.evidence_kind,
            identity=ref.semantic_content_hash,
            raw=raw,
        )

    def load_by_identity(self, *, kind: EvidenceKind, identity: str) -> G5StoredModel:
        identity = _sha256(identity)
        raw = _read_exact(
            path=self._destination(kind=kind, identity=identity),
            root=self._root,
        )
        return self._parse(kind=kind, identity=identity, raw=raw)

    def _existing(
        self,
        *,
        kind: EvidenceKind,
        identity: str,
        destination: Path,
        expected: bytes,
    ) -> StoredG5Evidence:
        raw = _read_exact(path=destination, root=self._root)
        if raw != expected:
            raise _store_error("G5 content-addressed identity collision")
        self._parse(kind=kind, identity=identity, raw=raw)
        return self._stored(
            kind=kind,
            identity=identity,
            path=destination,
            raw=raw,
            idempotent=True,
        )

    def _stored(
        self,
        *,
        kind: EvidenceKind,
        identity: str,
        path: Path,
        raw: bytes,
        idempotent: bool,
    ) -> StoredG5Evidence:
        return StoredG5Evidence(
            ref=Phase1GDevEvidenceRef(
                evidence_kind=kind,
                relative_path=path.relative_to(self._root).as_posix(),
                semantic_content_hash=identity,
                file_sha256=hashlib.sha256(raw).hexdigest(),
            ),
            path=path,
            idempotent=idempotent,
        )

    def _parse(
        self,
        *,
        kind: EvidenceKind,
        identity: str,
        raw: bytes,
    ) -> G5StoredModel:
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _store_error("G5 evidence is not valid UTF-8 JSON") from exc
        if not isinstance(document, dict):
            raise _store_error("G5 evidence must be one JSON object")
        model_type: type[Any] = {
            EvidenceKind.INVENTORY: Phase1GDevInputInventoryReceipt,
            EvidenceKind.MANIFEST: Phase1GDevExecutionManifest,
            EvidenceKind.PLAN: Phase1GExecutionBatchPlan,
            EvidenceKind.ROLLBACK: Phase1GDevRollbackReceipt,
            EvidenceKind.PERSISTENT: Phase1GDevPersistentReceipt,
            EvidenceKind.SUMMARY: Phase1GDevEvidenceSummary,
        }[kind]
        try:
            model: G5StoredModel = model_type.model_validate(document)
        except ValueError as exc:
            raise _store_error("G5 stored evidence contract is invalid") from exc
        actual_kind, actual_identity = _model_identity(model)
        if actual_kind is not kind or actual_identity != identity:
            raise _store_error("G5 evidence identity differs from path")
        canonical = (canonical_json_text(model.model_dump(mode="json")) + "\n").encode("utf-8")
        if canonical != raw:
            raise _store_error("G5 stored evidence is not canonical JSON")
        return model

    def _destination(self, *, kind: EvidenceKind, identity: str) -> Path:
        identity = _sha256(identity)
        namespace = {
            EvidenceKind.INVENTORY: "inventories",
            EvidenceKind.MANIFEST: "manifests",
            EvidenceKind.PLAN: "plans",
            EvidenceKind.ROLLBACK: "rollback",
            EvidenceKind.PERSISTENT: "persistent",
            EvidenceKind.SUMMARY: "summaries",
        }[kind]
        return self._root / namespace / identity[:2] / f"{identity}.json"


def _model_identity(model: G5StoredModel) -> tuple[EvidenceKind, str]:
    if isinstance(model, Phase1GDevInputInventoryReceipt):
        return EvidenceKind.INVENTORY, str(model.inventory_receipt_hash)
    if isinstance(model, Phase1GDevExecutionManifest):
        return EvidenceKind.MANIFEST, str(model.manifest_hash)
    if isinstance(model, Phase1GExecutionBatchPlan):
        return EvidenceKind.PLAN, str(model.batch_plan_hash)
    if isinstance(model, Phase1GDevRollbackReceipt):
        return EvidenceKind.ROLLBACK, str(model.rollback_receipt_hash)
    if isinstance(model, Phase1GDevPersistentReceipt):
        return EvidenceKind.PERSISTENT, str(model.persistent_receipt_hash)
    if isinstance(model, Phase1GDevEvidenceSummary):
        return EvidenceKind.SUMMARY, str(model.summary_hash)
    raise _store_error("unsupported G5 evidence model")


def _publish_no_replace(*, source: Path, target: Path) -> bool:
    if os.name == "nt":
        import ctypes

        move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        move_file_ex.restype = ctypes.c_int
        if move_file_ex(str(source), str(target), 0x00000008):
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
        raise _store_error("G5 evidence root must be absolute")
    if normalized.startswith("//wsl$/") or normalized.startswith("//wsl.localhost/") or normalized.startswith("/mnt/"):
        raise _store_error("G5 evidence root cannot use a WSL filesystem")
    prospective = expanded.resolve(strict=False)
    repository_root = Path(__file__).resolve().parents[3]
    try:
        prospective.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise _store_error("G5 evidence root must be outside the repository")
    existing = expanded
    while not existing.exists() and existing != existing.parent:
        existing = existing.parent
    _assert_existing_chain_no_reparse(existing)
    try:
        expanded.mkdir(parents=True, exist_ok=True)
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise _store_error("G5 evidence root cannot be created") from exc
    _assert_existing_chain_no_reparse(resolved)
    if not resolved.is_dir():
        raise _store_error("G5 evidence root must be a directory")
    return resolved


def _read_exact(*, path: Path, root: Path) -> bytes:
    try:
        _assert_contained(path=path, root=root)
        _assert_no_reparse_path(path=path, root=root)
        return path.read_bytes()
    except Phase1GDevEvidenceError:
        raise
    except OSError as exc:
        raise _store_error("G5 stored evidence is unavailable") from exc


def _assert_contained(*, path: Path, root: Path) -> None:
    try:
        (path.parent.resolve(strict=True) / path.name).relative_to(root)
    except (OSError, ValueError) as exc:
        raise _store_error("G5 evidence path escapes its root") from exc


def _assert_no_reparse_path(*, path: Path, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise _store_error("G5 evidence path escapes its root") from exc
    current = root
    for part in relative.parts:
        current = current / part
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise _store_error("G5 evidence path contains a symlink or reparse point")


def _assert_existing_chain_no_reparse(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise _store_error("G5 evidence root traverses a symlink or reparse point")


def _sha256(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise _store_error("G5 evidence identity must be lowercase sha256")
    return normalized


def _store_error(message: str) -> Phase1GDevEvidenceError:
    return Phase1GDevEvidenceError(REASON_EVIDENCE_STORE_FAILED, message)
