from __future__ import annotations

import hashlib
import ctypes
import os
import shutil
import stat
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Mapping

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_BUNDLE_HASH_MISMATCH,
    REASON_BUNDLE_INCOMPLETE,
    REASON_EXACT_RETRY_CONFLICT,
)
from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    strict_identifier,
    validated_hash,
)


BUNDLE_MANIFEST_SCHEMA_VERSION = "advisory_immutable_model_bundle_v1"
ARTIFACT_COMPLETION_SCHEMA_VERSION = "advisory_modeling_artifact_completion_receipt_v1"
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400

REQUIRED_BUNDLE_ROLES = (
    "final_model",
    "selected_fold_model",
    "style_profile",
    "feature_schema",
    "feature_formula_registry",
    "feature_snapshot_ref",
    "market_regime_policy_template",
    "fitted_fold_market_regime",
    "fitted_final_market_regime",
    "label_policy",
    "dataset_snapshot_ref",
    "training_views",
    "split_plan",
    "experiment_registry",
    "training_config",
    "environment_lock",
    "oos_metrics",
    "baseline_comparison",
    "feature_importance",
    "model_selection_receipt",
    "model_card",
)


class BundleFileDescriptorV1(FrozenModel):
    relative_path: str = Field(min_length=1, max_length=512)
    role: str = Field(min_length=1, max_length=120)
    content_sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(ge=0)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or value != path.as_posix():
            raise ValueError("relative_path must be normalized, relative and contained")
        return value

    @field_validator("role")
    @classmethod
    def _role(cls, value: str) -> str:
        return strict_identifier(value, field_name="role")

    @field_validator("content_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return str(validated_hash(value, field_name="content_sha256"))


class ImmutableModelBundleV1(FrozenModel):
    schema_version: Literal[BUNDLE_MANIFEST_SCHEMA_VERSION] = BUNDLE_MANIFEST_SCHEMA_VERSION
    request_semantic_hash: str = Field(min_length=64, max_length=64)
    style_profile_hash: str = Field(min_length=64, max_length=64)
    feature_snapshot_hash: str = Field(min_length=64, max_length=64)
    split_plan_hash: str = Field(min_length=64, max_length=64)
    experiment_registry_hash: str = Field(min_length=64, max_length=64)
    model_selection_receipt_hash: str = Field(min_length=64, max_length=64)
    environment_lock_hash: str = Field(min_length=64, max_length=64)
    files: tuple[BundleFileDescriptorV1, ...]
    bundle_status: Literal["RESEARCH_BUNDLE_COMPLETE"] = "RESEARCH_BUNDLE_COMPLETE"
    capability_status: Literal["MODEL_UNAVAILABLE", "RERANK_READY"]
    unavailable_reason_codes: tuple[str, ...] = ()
    bundle_semantic_hash: str | None = Field(default=None, min_length=64, max_length=64)
    bundle_id: str | None = Field(default=None, min_length=20, max_length=80)

    @field_validator(
        "request_semantic_hash",
        "style_profile_hash",
        "feature_snapshot_hash",
        "split_plan_hash",
        "experiment_registry_hash",
        "model_selection_receipt_hash",
        "environment_lock_hash",
        "bundle_semantic_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "ImmutableModelBundleV1":
        paths = tuple(item.relative_path for item in self.files)
        if len(set(paths)) != len(paths):
            raise ValueError("bundle file paths must be unique")
        if paths != tuple(sorted(paths)):
            raise ValueError("bundle file descriptors must be in canonical path order")
        if any(item.role != _role_from_path(item.relative_path) for item in self.files):
            raise ValueError("bundle file role differs from its canonical path role")
        roles = {item.role for item in self.files}
        missing = set(REQUIRED_BUNDLE_ROLES) - roles
        if missing:
            raise ValueError(f"bundle is missing required roles: {sorted(missing)}")
        required_paths = {
            "models/final_model.txt",
            *(f"models/selected_folds/fold-{index}/model.txt" for index in range(5)),
            "style_profile.json",
            "feature_schema.json",
            "feature_formula_registry.json",
            "feature_snapshot_ref.json",
            "market_regime_policy_template.json",
            *(f"fitted_market_regimes/fold-{index}.json" for index in range(5)),
            "fitted_market_regimes/final.json",
            "label_policy.json",
            "dataset_snapshot_ref.json",
            "training_views.json",
            "split_plan.json",
            "experiment_registry.json",
            "training_config.json",
            "environment_lock.json",
            "oos_metrics.json",
            "baseline_comparison.json",
            "feature_importance.json",
            "model_selection_receipt.json",
            "model_card.md",
        }
        if not required_paths.issubset(paths):
            raise ValueError("bundle is missing one or more required canonical paths")
        if self.capability_status == "RERANK_READY" and self.unavailable_reason_codes:
            raise ValueError("RERANK_READY bundle cannot contain unavailable reasons")
        if self.capability_status == "MODEL_UNAVAILABLE" and not self.unavailable_reason_codes:
            raise ValueError("MODEL_UNAVAILABLE bundle requires explicit reason codes")
        payload = self.model_dump(mode="python", exclude={"bundle_semantic_hash", "bundle_id"})
        digest = canonical_json_sha256(payload)
        if self.bundle_semantic_hash is not None and self.bundle_semantic_hash != digest:
            raise ValueError("bundle_semantic_hash differs from canonical bundle")
        expected_id = f"advrerank_{digest[:24]}"
        if self.bundle_id is not None and self.bundle_id != expected_id:
            raise ValueError("bundle_id differs from bundle semantic hash")
        object.__setattr__(self, "bundle_semantic_hash", digest)
        object.__setattr__(self, "bundle_id", expected_id)
        return self


class ArtifactCompletionReceiptV1(FrozenModel):
    schema_version: Literal[ARTIFACT_COMPLETION_SCHEMA_VERSION] = (
        ARTIFACT_COMPLETION_SCHEMA_VERSION
    )
    artifact_id: str = Field(min_length=1, max_length=160)
    semantic_hash: str = Field(min_length=64, max_length=64)
    file_set_hash: str = Field(min_length=64, max_length=64)
    files: tuple[BundleFileDescriptorV1, ...]
    status: Literal["COMPLETE"] = "COMPLETE"
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("semantic_hash", "file_set_hash", "receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "ArtifactCompletionReceiptV1":
        expected = canonical_json_sha256(
            tuple(item.model_dump(mode="python") for item in self.files)
        )
        if self.file_set_hash != expected:
            raise ValueError("file_set_hash differs from completion file descriptors")
        set_computed_hash(self, field_name="receipt_hash", exclude={"receipt_hash"})
        return self


class ImmutableArtifactStore:
    """Repo-external, content-addressed publisher with exact retry readback."""

    def __init__(self, *, artifact_root: Path, repository_root: Path, namespace: str) -> None:
        self.repository_root = repository_root.expanduser().resolve(strict=True)
        if not self.repository_root.is_dir():
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "repository_root is not a directory")
        self.artifact_root = _validate_artifact_root(
            artifact_root=artifact_root,
            repository_root=self.repository_root,
        )
        safe_namespace = strict_identifier(namespace, field_name="namespace")
        if PurePosixPath(safe_namespace).name != safe_namespace:
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "namespace must be one path segment")
        self.namespace_root = self.artifact_root / safe_namespace
        self.namespace_root.mkdir(parents=False, exist_ok=True)
        _assert_contained(self.namespace_root, self.artifact_root)
        _assert_plain_path(self.namespace_root, self.artifact_root)

    def publish(
        self,
        *,
        artifact_id: str,
        semantic_hash: str,
        files: Mapping[str, bytes],
    ) -> ArtifactCompletionReceiptV1:
        artifact_name = strict_identifier(artifact_id, field_name="artifact_id")
        if PurePosixPath(artifact_name).name != artifact_name:
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_id must be one path segment")
        semantic = str(validated_hash(semantic_hash, field_name="semantic_hash"))
        descriptors = _descriptors(files)
        file_set_hash = canonical_json_sha256(
            tuple(item.model_dump(mode="python") for item in descriptors)
        )
        receipt = ArtifactCompletionReceiptV1(
            artifact_id=artifact_name,
            semantic_hash=semantic,
            file_set_hash=file_set_hash,
            files=descriptors,
        )
        destination = self.namespace_root / artifact_name
        _assert_contained(destination, self.namespace_root)
        if destination.exists():
            return self.read_exact(
                artifact_id=artifact_name,
                expected_semantic_hash=semantic,
                expected_files=files,
            )

        temp_path = Path(tempfile.mkdtemp(prefix=f".{artifact_name}.", dir=self.namespace_root))
        try:
            for relative_path, payload in sorted(files.items()):
                target = temp_path / PurePosixPath(relative_path)
                _assert_contained(target, temp_path)
                target.parent.mkdir(parents=True, exist_ok=True)
                _write_bytes(target, payload)
            receipt_payload = (
                canonical_json_text(receipt.model_dump(mode="python")) + "\n"
            ).encode("utf-8")
            _write_bytes(temp_path / "completion_receipt.json", receipt_payload)
            _publish_directory_create_if_absent(source=temp_path, target=destination)
            if temp_path.exists():
                _safe_remove_temp(temp_path, self.namespace_root)
        except Exception:
            if temp_path.exists():
                _safe_remove_temp(temp_path, self.namespace_root)
            raise
        return self.read_exact(
            artifact_id=artifact_name,
            expected_semantic_hash=semantic,
            expected_files=files,
        )

    def read_exact(
        self,
        *,
        artifact_id: str,
        expected_semantic_hash: str,
        expected_files: Mapping[str, bytes] | None = None,
    ) -> ArtifactCompletionReceiptV1:
        artifact_name = strict_identifier(artifact_id, field_name="artifact_id")
        if PurePosixPath(artifact_name).name != artifact_name:
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_id must be one path segment")
        expected_semantic_hash = str(
            validated_hash(expected_semantic_hash, field_name="expected_semantic_hash")
        )
        directory = self.namespace_root / artifact_name
        _assert_contained(directory, self.namespace_root)
        if not directory.is_dir():
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact directory is missing")
        _assert_plain_path(directory, self.namespace_root)
        receipt_path = directory / "completion_receipt.json"
        try:
            receipt = ArtifactCompletionReceiptV1.model_validate_json(receipt_path.read_bytes())
        except (OSError, ValueError) as exc:
            raise AdvisoryModelingError(
                REASON_BUNDLE_INCOMPLETE, "completion receipt is missing or invalid"
            ) from exc
        if receipt.artifact_id != artifact_name or receipt.semantic_hash != expected_semantic_hash:
            raise AdvisoryModelingError(
                REASON_EXACT_RETRY_CONFLICT, "artifact identity differs on exact readback"
            )
        if expected_files is not None and receipt.files != _descriptors(expected_files):
            raise AdvisoryModelingError(
                REASON_EXACT_RETRY_CONFLICT, "artifact file descriptors differ on exact retry"
            )
        expected_paths = {item.relative_path for item in receipt.files} | {"completion_receipt.json"}
        actual_paths = {
            item.relative_to(directory).as_posix()
            for item in directory.rglob("*")
            if item.is_file()
        }
        if actual_paths != expected_paths:
            raise AdvisoryModelingError(
                REASON_BUNDLE_HASH_MISMATCH, "artifact file set differs from completion receipt"
            )
        for descriptor in receipt.files:
            path = directory / PurePosixPath(descriptor.relative_path)
            _assert_contained(path, directory)
            _assert_plain_path(path, directory)
            payload = path.read_bytes()
            if len(payload) != descriptor.size_bytes or hashlib.sha256(payload).hexdigest() != descriptor.content_sha256:
                raise AdvisoryModelingError(
                    REASON_BUNDLE_HASH_MISMATCH,
                    f"artifact file readback differs: {descriptor.relative_path}",
                )
        return receipt


class ImmutableModelBundleStore:
    def __init__(self, *, artifact_root: Path, repository_root: Path) -> None:
        self.store = ImmutableArtifactStore(
            artifact_root=artifact_root,
            repository_root=repository_root,
            namespace="model_bundles",
        )

    def publish(
        self,
        *,
        manifest: ImmutableModelBundleV1,
        payload_files: Mapping[str, bytes],
    ) -> ArtifactCompletionReceiptV1:
        descriptors = _descriptors(payload_files)
        if descriptors != manifest.files:
            raise AdvisoryModelingError(
                REASON_BUNDLE_HASH_MISMATCH, "payload files differ from bundle manifest"
            )
        manifest_bytes = (
            canonical_json_text(manifest.model_dump(mode="python")) + "\n"
        ).encode("utf-8")
        files = dict(payload_files)
        files["bundle_manifest.json"] = manifest_bytes
        receipt = self.store.publish(
            artifact_id=str(manifest.bundle_id),
            semantic_hash=str(manifest.bundle_semantic_hash),
            files=files,
        )
        readback = self.read(
            bundle_id=str(manifest.bundle_id),
            expected_bundle_hash=str(manifest.bundle_semantic_hash),
        )
        if readback != manifest:
            raise AdvisoryModelingError(
                REASON_EXACT_RETRY_CONFLICT, "bundle manifest differs on exact readback"
            )
        return receipt

    def read(self, *, bundle_id: str, expected_bundle_hash: str) -> ImmutableModelBundleV1:
        self.store.read_exact(
            artifact_id=bundle_id,
            expected_semantic_hash=expected_bundle_hash,
        )
        manifest_path = self.store.namespace_root / bundle_id / "bundle_manifest.json"
        try:
            manifest = ImmutableModelBundleV1.model_validate_json(manifest_path.read_bytes())
        except (OSError, ValueError) as exc:
            raise AdvisoryModelingError(
                REASON_BUNDLE_INCOMPLETE, "bundle manifest is missing or invalid"
            ) from exc
        if manifest.bundle_id != bundle_id or manifest.bundle_semantic_hash != expected_bundle_hash:
            raise AdvisoryModelingError(
                REASON_BUNDLE_HASH_MISMATCH, "bundle manifest identity differs from requested bundle"
            )
        return manifest


def _descriptors(files: Mapping[str, bytes]) -> tuple[BundleFileDescriptorV1, ...]:
    descriptors: list[BundleFileDescriptorV1] = []
    for relative_path, payload in sorted(files.items()):
        if not isinstance(payload, bytes):
            raise TypeError("artifact payloads must be bytes")
        role = _role_from_path(relative_path)
        descriptors.append(
            BundleFileDescriptorV1(
                relative_path=relative_path,
                role=role,
                content_sha256=hashlib.sha256(payload).hexdigest(),
                size_bytes=len(payload),
            )
        )
    if not descriptors:
        raise ValueError("artifact must contain at least one payload file")
    return tuple(descriptors)


def build_file_descriptors(files: Mapping[str, bytes]) -> tuple[BundleFileDescriptorV1, ...]:
    return _descriptors(files)


def _role_from_path(relative_path: str) -> str:
    path = PurePosixPath(relative_path)
    if path.is_absolute() or ".." in path.parts or relative_path != path.as_posix():
        raise ValueError("artifact paths must be normalized relative POSIX paths")
    if relative_path == "models/final_model.txt":
        return "final_model"
    if relative_path.startswith("models/selected_folds/"):
        return "selected_fold_model"
    if relative_path.startswith("fitted_market_regimes/fold-"):
        return "fitted_fold_market_regime"
    exact = {
        "style_profile.json": "style_profile",
        "feature_schema.json": "feature_schema",
        "feature_formula_registry.json": "feature_formula_registry",
        "feature_snapshot_ref.json": "feature_snapshot_ref",
        "market_regime_policy_template.json": "market_regime_policy_template",
        "fitted_market_regimes/final.json": "fitted_final_market_regime",
        "label_policy.json": "label_policy",
        "dataset_snapshot_ref.json": "dataset_snapshot_ref",
        "training_views.json": "training_views",
        "split_plan.json": "split_plan",
        "experiment_registry.json": "experiment_registry",
        "training_config.json": "training_config",
        "environment_lock.json": "environment_lock",
        "oos_metrics.json": "oos_metrics",
        "baseline_comparison.json": "baseline_comparison",
        "feature_importance.json": "feature_importance",
        "model_selection_receipt.json": "model_selection_receipt",
        "model_card.md": "model_card",
        "bundle_manifest.json": "bundle_manifest",
    }
    return exact.get(relative_path, f"artifact:{relative_path}")


def _validate_artifact_root(*, artifact_root: Path, repository_root: Path) -> Path:
    expanded = artifact_root.expanduser()
    normalized = str(expanded).replace("\\", "/").lower()
    if not expanded.is_absolute():
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_root must be absolute")
    if normalized.startswith("//wsl$/") or normalized.startswith("//wsl.localhost/") or normalized.startswith("/mnt/"):
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_root cannot be a WSL path")
    if not expanded.exists():
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_root must already exist")
    _assert_existing_chain_plain(expanded)
    resolved = expanded.resolve(strict=True)
    if not resolved.is_dir():
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_root must be a directory")
    try:
        resolved.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact_root must be outside repository_root")
    _assert_plain_path(resolved, resolved)
    return resolved


def _assert_contained(path: Path, root: Path) -> None:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact path escapes root") from exc


def _assert_plain_path(path: Path, root: Path) -> None:
    _assert_contained(path, root)
    current = root
    relative = path.relative_to(root)
    paths = (root, *(root / Path(*relative.parts[:index]) for index in range(1, len(relative.parts) + 1)))
    for current in paths:
        try:
            attributes = os.lstat(current)
        except OSError as exc:
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact path is missing") from exc
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact path contains a link")


def _assert_existing_chain_plain(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        try:
            attributes = os.lstat(current)
        except OSError as exc:
            raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "artifact root chain is missing") from exc
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise AdvisoryModelingError(
                REASON_BUNDLE_INCOMPLETE, "artifact root chain contains a link or reparse point"
            )


def _write_bytes(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _publish_directory_create_if_absent(*, source: Path, target: Path) -> bool:
    if os.name == "nt":
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        move_file_ex = kernel32.MoveFileExW
        move_file_ex.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
        move_file_ex.restype = ctypes.c_int
        movefile_write_through = 0x00000008
        if move_file_ex(str(source), str(target), movefile_write_through):
            return True
        error_code = ctypes.get_last_error()
        if error_code in {80, 183}:
            return False
        raise OSError(error_code, "atomic artifact directory publish failed")
    try:
        os.rename(source, target)
    except FileExistsError:
        return False
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return True


def _safe_remove_temp(path: Path, namespace_root: Path) -> None:
    _assert_contained(path, namespace_root)
    if path.parent != namespace_root or not path.name.startswith("."):
        raise AdvisoryModelingError(REASON_BUNDLE_INCOMPLETE, "refusing unsafe temp cleanup")
    shutil.rmtree(path)
