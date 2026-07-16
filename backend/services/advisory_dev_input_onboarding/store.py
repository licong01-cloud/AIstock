"""External atomic content-addressed store for onboarding evidence."""

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

from .contracts import (
    EvidenceKind,
    OnboardingArtifactRef,
    OnboardingBlobRef,
    PortableAdvisoryEvidenceBundle,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    REASON_EVIDENCE_STORE_FAILED,
    STORE_POLICY_HASH,
    StoredOnboardingModel,
    validate_sha256,
)


LOGGER = logging.getLogger(__name__)
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400


@dataclass(frozen=True)
class StoredOnboardingEvidence:
    ref: OnboardingArtifactRef
    path: Path
    idempotent: bool


@dataclass(frozen=True)
class StoredOnboardingBlob:
    ref: OnboardingBlobRef
    path: Path
    idempotent: bool


@dataclass(frozen=True)
class PackageAssetRootPair:
    source_readonly_root: Path
    target_no_replace_root: Path


def resolve_package_asset_roots(*, source_root: Path, target_root: Path) -> PackageAssetRootPair:
    """Resolve explicit, distinct, repository-external roots without creating the target."""

    source = _validate_external_existing_directory(source_root, label="source package asset")
    target = _validate_external_prospective_directory(target_root, label="target package asset")
    if source == target:
        raise _store_error("source and target package asset roots must be different")
    return PackageAssetRootPair(source_readonly_root=source, target_no_replace_root=target)


class RealDevOnboardingEvidenceStore:
    def __init__(self, *, root: Path) -> None:
        self._root = _validate_external_root(root)

    @property
    def root(self) -> Path:
        return self._root

    def publish(self, model: StoredOnboardingModel) -> StoredOnboardingEvidence:
        kind, identity = _model_identity(model)
        content = (canonical_json_text(model.model_dump(mode="json")) + "\n").encode("utf-8")
        destination = self._destination(kind=kind, identity=identity)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            _assert_contained(path=destination, root=self._root)
            _assert_no_reparse_path(path=destination.parent, root=self._root)
        except (OSError, ValueError) as exc:
            raise _store_error("unable to prepare onboarding evidence directory") from exc
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
            if not _publish_no_replace(source=temp_path, target=destination):
                return self._existing(kind=kind, identity=identity, destination=destination, expected=content)
            persisted = _read_exact(path=destination, root=self._root)
            if persisted != content:
                raise _store_error("onboarding evidence post-write readback differs")
            self._parse(kind=kind, identity=identity, raw=persisted)
            return self._stored(kind=kind, identity=identity, path=destination, raw=persisted, idempotent=False)
        except RealDevOnboardingError:
            raise
        except OSError as exc:
            raise _store_error("unable to publish onboarding evidence") from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError as exc:
                    LOGGER.warning("advisory_onboarding_temp_cleanup_failed identity_prefix=%s errno=%s", identity[:12], exc.errno)

    def load(self, ref: OnboardingArtifactRef) -> StoredOnboardingModel:
        if ref.store_policy_hash != STORE_POLICY_HASH:
            raise _store_error("onboarding evidence ref store policy is invalid")
        path = self._destination(kind=ref.evidence_kind, identity=ref.semantic_content_hash)
        if ref.relative_path != path.relative_to(self._root).as_posix():
            raise _store_error("onboarding evidence ref path differs from its identity")
        raw = _read_exact(path=path, root=self._root)
        if hashlib.sha256(raw).hexdigest() != ref.file_sha256:
            raise _store_error("onboarding evidence raw hash differs from its ref")
        return self._parse(kind=ref.evidence_kind, identity=ref.semantic_content_hash, raw=raw)

    def publish_blob(self, *, raw: bytes, expected_sha256: str | None = None) -> StoredOnboardingBlob:
        if not isinstance(raw, bytes):
            raise _store_error("onboarding blob payload must be bytes")
        identity = hashlib.sha256(raw).hexdigest()
        if expected_sha256 is not None and identity != validate_sha256(expected_sha256, field_name="expected_sha256"):
            raise _store_error("onboarding blob hash differs from its package asset authority")
        destination = self._blob_destination(identity)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            _assert_contained(path=destination, root=self._root)
            _assert_no_reparse_path(path=destination.parent, root=self._root)
        except (OSError, ValueError) as exc:
            raise _store_error("unable to prepare onboarding blob directory") from exc
        if destination.exists():
            persisted = _read_exact(path=destination, root=self._root)
            if persisted != raw:
                raise _store_error("onboarding blob content-addressed identity collision")
            return self._stored_blob(identity=identity, path=destination, raw=persisted, idempotent=True)
        temp_path: Path | None = None
        try:
            descriptor, temp_name = tempfile.mkstemp(prefix=f".{identity}.", suffix=".tmp", dir=destination.parent)
            temp_path = Path(temp_name)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            if not _publish_no_replace(source=temp_path, target=destination):
                persisted = _read_exact(path=destination, root=self._root)
                if persisted != raw:
                    raise _store_error("onboarding blob content-addressed identity collision")
                return self._stored_blob(identity=identity, path=destination, raw=persisted, idempotent=True)
            persisted = _read_exact(path=destination, root=self._root)
            if persisted != raw:
                raise _store_error("onboarding blob post-write readback differs")
            return self._stored_blob(identity=identity, path=destination, raw=persisted, idempotent=False)
        except RealDevOnboardingError:
            raise
        except OSError as exc:
            raise _store_error("unable to publish onboarding blob") from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError as exc:
                    LOGGER.warning("advisory_onboarding_blob_temp_cleanup_failed identity_prefix=%s errno=%s", identity[:12], exc.errno)

    def load_blob(self, ref: OnboardingBlobRef) -> bytes:
        if ref.store_policy_hash != STORE_POLICY_HASH:
            raise _store_error("onboarding blob ref store policy is invalid")
        path = self._blob_destination(ref.blob_sha256)
        if ref.relative_path != path.relative_to(self._root).as_posix():
            raise _store_error("onboarding blob ref path differs from its identity")
        raw = _read_exact(path=path, root=self._root)
        if hashlib.sha256(raw).hexdigest() != ref.blob_sha256 or len(raw) != ref.size_bytes:
            raise _store_error("onboarding blob full readback differs from its ref")
        return raw

    def verify_reference_closure(self, model: StoredOnboardingModel) -> None:
        if isinstance(model, RealDevOnboardingInventoryReceipt):
            selected = self.load(model.selected_input_ref)
            if model.selected_input_ref.evidence_kind is EvidenceKind.REQUEST:
                if not isinstance(selected, RealDevOnboardingRequest) or selected.request_hash != model.selected_request_hash:
                    raise _store_error("inventory request reference closure is invalid")
            elif not isinstance(selected, RealDevOnboardingInventoryQuery) or selected.inventory_query_hash != model.selected_inventory_query_hash:
                raise _store_error("inventory query reference closure is invalid")
        if isinstance(model, PortableAdvisoryEvidenceBundle):
            for item in model.artifact_blob_refs:
                self.load_blob(item.blob_ref)

    def _existing(self, *, kind: EvidenceKind, identity: str, destination: Path, expected: bytes) -> StoredOnboardingEvidence:
        raw = _read_exact(path=destination, root=self._root)
        if raw != expected:
            raise _store_error("onboarding content-addressed identity collision")
        self._parse(kind=kind, identity=identity, raw=raw)
        return self._stored(kind=kind, identity=identity, path=destination, raw=raw, idempotent=True)

    def _stored(self, *, kind: EvidenceKind, identity: str, path: Path, raw: bytes, idempotent: bool) -> StoredOnboardingEvidence:
        return StoredOnboardingEvidence(
            ref=OnboardingArtifactRef(
                evidence_kind=kind,
                relative_path=path.relative_to(self._root).as_posix(),
                semantic_content_hash=identity,
                file_sha256=hashlib.sha256(raw).hexdigest(),
            ),
            path=path,
            idempotent=idempotent,
        )

    def _parse(self, *, kind: EvidenceKind, identity: str, raw: bytes) -> StoredOnboardingModel:
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _store_error("onboarding evidence is not valid UTF-8 JSON") from exc
        if not isinstance(document, dict):
            raise _store_error("onboarding evidence must be one JSON object")
        model_type: type[Any] = {
            EvidenceKind.REQUEST: RealDevOnboardingRequest,
            EvidenceKind.INVENTORY_QUERY: RealDevOnboardingInventoryQuery,
            EvidenceKind.INVENTORY: RealDevOnboardingInventoryReceipt,
            EvidenceKind.BUNDLE: PortableAdvisoryEvidenceBundle,
        }[kind]
        try:
            model: StoredOnboardingModel = model_type.model_validate(document)
        except ValueError as exc:
            raise _store_error("stored onboarding evidence contract is invalid") from exc
        actual_kind, actual_identity = _model_identity(model)
        if actual_kind is not kind or actual_identity != validate_sha256(identity, field_name="identity"):
            raise _store_error("onboarding evidence identity differs from path")
        canonical = (canonical_json_text(model.model_dump(mode="json")) + "\n").encode("utf-8")
        if canonical != raw:
            raise _store_error("stored onboarding evidence is not canonical JSON")
        return model

    def _destination(self, *, kind: EvidenceKind, identity: str) -> Path:
        identity = validate_sha256(identity, field_name="identity")
        namespace = {
            EvidenceKind.REQUEST: "requests",
            EvidenceKind.INVENTORY_QUERY: "inventory-queries",
            EvidenceKind.INVENTORY: "inventories",
            EvidenceKind.BUNDLE: "bundles",
        }[kind]
        return self._root / namespace / identity[:2] / f"{identity}.json"

    def _blob_destination(self, identity: str) -> Path:
        identity = validate_sha256(identity, field_name="blob_sha256")
        return self._root / "blobs" / identity[:2] / f"{identity}.blob"

    def _stored_blob(self, *, identity: str, path: Path, raw: bytes, idempotent: bool) -> StoredOnboardingBlob:
        return StoredOnboardingBlob(
            ref=OnboardingBlobRef(
                relative_path=path.relative_to(self._root).as_posix(),
                blob_sha256=identity,
                size_bytes=len(raw),
            ),
            path=path,
            idempotent=idempotent,
        )


def _model_identity(model: StoredOnboardingModel) -> tuple[EvidenceKind, str]:
    if isinstance(model, RealDevOnboardingRequest):
        return EvidenceKind.REQUEST, str(model.request_hash)
    if isinstance(model, RealDevOnboardingInventoryQuery):
        return EvidenceKind.INVENTORY_QUERY, str(model.inventory_query_hash)
    if isinstance(model, RealDevOnboardingInventoryReceipt):
        return EvidenceKind.INVENTORY, str(model.inventory_hash)
    if isinstance(model, PortableAdvisoryEvidenceBundle):
        return EvidenceKind.BUNDLE, str(model.bundle_content_hash)
    raise _store_error("unsupported onboarding evidence model")


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
        raise _store_error("onboarding evidence root must be absolute")
    if normalized.startswith("//wsl$/") or normalized.startswith("//wsl.localhost/") or normalized.startswith("/mnt/"):
        raise _store_error("onboarding evidence root cannot use a WSL filesystem")
    prospective = expanded.resolve(strict=False)
    repository_root = Path(__file__).resolve().parents[3]
    try:
        prospective.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise _store_error("onboarding evidence root must be outside the repository")
    existing = expanded
    while not existing.exists() and existing != existing.parent:
        existing = existing.parent
    _assert_existing_chain_no_reparse(existing)
    try:
        expanded.mkdir(parents=True, exist_ok=True)
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise _store_error("onboarding evidence root cannot be created") from exc
    _assert_existing_chain_no_reparse(resolved)
    if not resolved.is_dir():
        raise _store_error("onboarding evidence root must be a directory")
    return resolved


def _validate_external_existing_directory(root: Path, *, label: str) -> Path:
    resolved = _validate_external_prospective_directory(root, label=label)
    if not resolved.exists() or not resolved.is_dir():
        raise _store_error(f"{label} root must be an existing directory")
    _assert_existing_chain_no_reparse(resolved)
    return resolved.resolve(strict=True)


def _validate_external_prospective_directory(root: Path, *, label: str) -> Path:
    expanded = root.expanduser()
    normalized = str(expanded).replace("\\", "/").lower()
    if not expanded.is_absolute():
        raise _store_error(f"{label} root must be absolute")
    if normalized.startswith("//wsl$/") or normalized.startswith("//wsl.localhost/") or normalized.startswith("/mnt/"):
        raise _store_error(f"{label} root cannot use a WSL filesystem")
    prospective = expanded.resolve(strict=False)
    repository_root = Path(__file__).resolve().parents[3]
    try:
        prospective.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise _store_error(f"{label} root must be outside the repository")
    existing = prospective
    while not existing.exists() and existing != existing.parent:
        existing = existing.parent
    _assert_existing_chain_no_reparse(existing.resolve(strict=True))
    if prospective.exists() and not prospective.is_dir():
        raise _store_error(f"{label} root must be a directory")
    return prospective


def _read_exact(*, path: Path, root: Path) -> bytes:
    try:
        _assert_contained(path=path, root=root)
        _assert_no_reparse_path(path=path, root=root)
        return path.read_bytes()
    except RealDevOnboardingError:
        raise
    except OSError as exc:
        raise _store_error("stored onboarding evidence is unavailable") from exc


def _assert_contained(*, path: Path, root: Path) -> None:
    try:
        (path.parent.resolve(strict=True) / path.name).relative_to(root)
    except (OSError, ValueError) as exc:
        raise _store_error("onboarding evidence path escapes its root") from exc


def _assert_no_reparse_path(*, path: Path, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise _store_error("onboarding evidence path escapes its root") from exc
    current = root
    for part in relative.parts:
        current = current / part
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT):
            raise _store_error("onboarding evidence path contains a symlink or reparse point")


def _assert_existing_chain_no_reparse(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT):
            raise _store_error("onboarding evidence root traverses a symlink or reparse point")


def _store_error(message: str) -> RealDevOnboardingError:
    return RealDevOnboardingError(REASON_EVIDENCE_STORE_FAILED, message)
