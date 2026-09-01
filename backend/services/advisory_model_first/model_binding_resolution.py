from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


DESCRIPTOR_SCHEMA_VERSION = "advisory_program_model_binding_v1"
DESCRIPTOR_SCHEMA_VERSION_V2 = "advisory_program_model_binding_v2"
CANDIDATE_PROJECTION_SCHEMA_VERSION = "advisory_candidate_projection_v1"
QUALITY_RERANKER_MODEL_ROLE = "quality_reranker"
META_LABEL_MODEL_ROLE = "meta_label_take_skip_confidence"
_DESCRIPTOR_IDENTITY_PATTERNS = {
    "program_id": re.compile(r"^advp_[A-Za-z0-9_-]{1,123}$"),
    "binding_version_id": re.compile(r"^advb_[A-Za-z0-9_-]{1,123}$"),
}


@dataclass(frozen=True)
class AdvisoryModelBindingResolutionV1:
    program_id: str
    binding_version_id: str
    package_id: str
    manifest_sha256: str
    style_profile_id: str
    style_profile_hash: str
    selection_runtime_semantics_hash: str
    feature_schema_version: str
    feature_schema_hash: str
    bundle_id: str
    bundle_manifest_sha256: str
    component_roles: dict[str, str]
    descriptor_sha256: str
    model_role: str = QUALITY_RERANKER_MODEL_ROLE
    shadow_policy_sha256: str | None = None
    terminal_weights: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class AdvisoryModelDescriptorRotationReceipt:
    operation: str
    descriptor_path: Path
    previous_descriptor_sha256: str
    descriptor_sha256: str
    rollback_snapshot_path: Path | None


class AdvisoryModelBindingResolver:
    @staticmethod
    def descriptor_path(
        *,
        model_root: str | Path,
        program_id: str,
        binding_version_id: str,
    ) -> Path:
        safe_program_id = os.path.basename(
            _descriptor_identity("program_id", program_id)
        )
        safe_binding_version_id = os.path.basename(
            _descriptor_identity("binding_version_id", binding_version_id)
        )
        root = os.path.realpath(os.fspath(model_root))
        binding_root = os.path.realpath(os.path.join(root, "program_bindings"))
        if not binding_root.startswith(root + os.sep):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor path escapes its configured root",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"field": "program_bindings_root"},
            )
        target = os.path.realpath(
            os.path.join(
                binding_root,
                safe_program_id,
                f"{safe_binding_version_id}.json",
            )
        )
        if not target.startswith(binding_root + os.sep):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor path escapes its configured root",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"field": "descriptor_path"},
            )
        return Path(target)

    def is_configured(
        self,
        *,
        model_root: str | Path,
        program_id: str,
        binding_version_id: str,
    ) -> bool:
        descriptor_path = self.descriptor_path(
            model_root=model_root,
            program_id=program_id,
            binding_version_id=binding_version_id,
        )
        normalized_path, normalized_root = _normalized_descriptor_access(
            descriptor_path=descriptor_path,
            model_root=model_root,
        )
        if not normalized_path.startswith(normalized_root + os.sep):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor path escapes its configured root",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"field": "descriptor_path"},
            )
        return os.path.isfile(normalized_path)

    def resolve(
        self,
        *,
        model_root: str | Path,
        program: Any,
        active_binding: Mapping[str, Any],
        selection_run: Any,
    ) -> AdvisoryModelBindingResolutionV1:
        package_ids = tuple(
            str(value).strip() for value in (active_binding.get("package_ids") or ())
        )
        program_package_ids = tuple(
            str(value).strip() for value in getattr(program, "package_ids", ())
        )
        if len(package_ids) != 1 or package_ids != program_package_ids:
            raise AdvisoryModelFirstError(
                "model inference requires one native StrategyPackage binding",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            )
        package_id = package_ids[0]
        manifest_sha256 = str(
            selection_run.manifest_sha256_by_package.get(package_id) or ""
        ).strip()
        if not _is_sha256(manifest_sha256):
            raise AdvisoryModelFirstError(
                "Selection run does not identify one package manifest",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        program_id = str(getattr(program, "program_id", "")).strip()
        binding_version_id = str(active_binding.get("binding_version_id") or "").strip()
        descriptor_path = self.descriptor_path(
            model_root=model_root,
            program_id=program_id,
            binding_version_id=binding_version_id,
        )
        normalized_path, normalized_root = _normalized_descriptor_access(
            descriptor_path=descriptor_path,
            model_root=model_root,
        )
        if not normalized_path.startswith(normalized_root + os.sep):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor path escapes its configured root",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"field": "descriptor_path"},
            )
        if not os.path.isfile(normalized_path):
            raise AdvisoryModelFirstError(
                "no exact model descriptor is configured for this Advisory Program binding",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
                context={
                    "program_id": program_id,
                    "binding_version_id": binding_version_id,
                },
            )
        payload = _read_json(Path(normalized_path))
        schema_version = str(payload.get("schema_version") or "")
        if schema_version not in {
            DESCRIPTOR_SCHEMA_VERSION,
            DESCRIPTOR_SCHEMA_VERSION_V2,
        }:
            raise AdvisoryModelFirstError(
                "Advisory model descriptor schema is unsupported",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"schema_version": schema_version},
            )
        required = {
            "schema_version",
            "program_id",
            "binding_version_id",
            "package_ids",
            "package_id",
            "manifest_sha256",
            "style_profile_id",
            "style_profile_hash",
            "selection_runtime_semantics_hash",
            "feature_schema_version",
            "feature_schema_hash",
            "bundle_id",
            "bundle_manifest_sha256",
            "candidate_projection",
            "created_at",
            "descriptor_sha256",
        }
        if schema_version == DESCRIPTOR_SCHEMA_VERSION_V2:
            required.update({"model_role", "shadow_policy_sha256"})
        missing = sorted(required - set(payload))
        if missing:
            raise AdvisoryModelFirstError(
                "Advisory model descriptor is incomplete",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"missing_fields": missing},
            )
        without_hash = dict(payload)
        descriptor_sha256 = str(without_hash.pop("descriptor_sha256") or "")
        if not _is_sha256(
            descriptor_sha256
        ) or descriptor_sha256 != canonical_json_sha256(without_hash):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor hash is invalid",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
        expected_identity = {
            "schema_version": schema_version,
            "program_id": program_id,
            "binding_version_id": binding_version_id,
            "package_ids": [package_id],
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
        }
        if any(payload.get(key) != value for key, value in expected_identity.items()):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor differs from the persisted Program or Selection identity",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        sha_fields = (
            "style_profile_hash",
            "selection_runtime_semantics_hash",
            "feature_schema_hash",
            "bundle_id",
            "bundle_manifest_sha256",
        )
        if schema_version == DESCRIPTOR_SCHEMA_VERSION_V2:
            sha_fields += ("shadow_policy_sha256",)
        if any(not _is_sha256(str(payload.get(field) or "")) for field in sha_fields):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor contains an invalid content identity",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
        projection = payload.get("candidate_projection")
        if (
            not isinstance(projection, Mapping)
            or projection.get("schema_version") != CANDIDATE_PROJECTION_SCHEMA_VERSION
        ):
            raise AdvisoryModelFirstError(
                "Advisory candidate projection is unsupported",
                reason_code="ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED",
            )
        roles = projection.get("component_roles")
        if not isinstance(roles, Mapping) or set(roles) != {"lstm", "fund"}:
            raise AdvisoryModelFirstError(
                "Advisory candidate projection must identify the trained component roles",
                reason_code="ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED",
            )
        component_roles = {str(key): str(value).strip() for key, value in roles.items()}
        if (
            any(not value for value in component_roles.values())
            or len(set(component_roles.values())) != 2
        ):
            raise AdvisoryModelFirstError(
                "Advisory candidate projection component roles are invalid",
                reason_code="ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED",
            )
        model_role = QUALITY_RERANKER_MODEL_ROLE
        shadow_policy_sha256: str | None = None
        terminal_weights: dict[str, float] = {}
        if schema_version == DESCRIPTOR_SCHEMA_VERSION_V2:
            model_role = str(payload.get("model_role") or "").strip()
            if model_role != META_LABEL_MODEL_ROLE:
                raise AdvisoryModelFirstError(
                    "Advisory model descriptor declares an unsupported model role",
                    reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                    context={"model_role": model_role},
                )
            shadow_policy_sha256 = str(payload["shadow_policy_sha256"])
            terminal_weights = _normalized_terminal_weights(
                projection.get("terminal_weights"),
                component_roles=component_roles,
                reason_code="ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED",
            )
        return AdvisoryModelBindingResolutionV1(
            program_id=program_id,
            binding_version_id=binding_version_id,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            style_profile_id=str(payload["style_profile_id"]),
            style_profile_hash=str(payload["style_profile_hash"]),
            selection_runtime_semantics_hash=str(
                payload["selection_runtime_semantics_hash"]
            ),
            feature_schema_version=str(payload["feature_schema_version"]),
            feature_schema_hash=str(payload["feature_schema_hash"]),
            bundle_id=str(payload["bundle_id"]),
            bundle_manifest_sha256=str(payload["bundle_manifest_sha256"]),
            component_roles=component_roles,
            descriptor_sha256=descriptor_sha256,
            model_role=model_role,
            shadow_policy_sha256=shadow_policy_sha256,
            terminal_weights=terminal_weights,
        )


def publish_program_model_descriptor(
    *,
    model_root: str | Path,
    payload: Mapping[str, Any],
) -> Path:
    _, encoded, target = _prepare_program_model_descriptor(
        model_root=model_root,
        payload=payload,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with _exclusive_descriptor_lock(target):
        if target.exists():
            if _read_descriptor_bytes(target) == encoded:
                return target
            raise AdvisoryModelFirstError(
                "exact Advisory Program model descriptor already exists with different bytes",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={
                    "program_id": str(payload.get("program_id") or "").strip(),
                    "binding_version_id": str(
                        payload.get("binding_version_id") or ""
                    ).strip(),
                },
            )
        _atomic_write_descriptor(target=target, encoded=encoded)
    return target


def rotate_program_model_descriptor(
    *,
    model_root: str | Path,
    payload: Mapping[str, Any],
    expected_current_descriptor_sha256: str,
) -> AdvisoryModelDescriptorRotationReceipt:
    expected_sha256 = str(expected_current_descriptor_sha256).strip()
    if not _is_sha256(expected_sha256):
        raise AdvisoryModelFirstError(
            "expected current Advisory model descriptor hash is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": "expected_current_descriptor_sha256"},
        )
    body, encoded, target = _prepare_program_model_descriptor(
        model_root=model_root,
        payload=payload,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with _exclusive_descriptor_lock(target):
        if not target.is_file():
            raise AdvisoryModelFirstError(
                "cannot rotate an Advisory model descriptor that is not configured",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_CONFLICT",
                context={
                    "program_id": body["program_id"],
                    "binding_version_id": body["binding_version_id"],
                    "expected_current_descriptor_sha256": expected_sha256,
                    "current_descriptor_sha256": None,
                },
            )
        current_bytes = _read_descriptor_bytes(target)
        current_payload, current_sha256 = _validated_descriptor(current_bytes)
        if (
            current_payload.get("program_id") != body["program_id"]
            or current_payload.get("binding_version_id") != body["binding_version_id"]
        ):
            raise AdvisoryModelFirstError(
                "current Advisory model descriptor identity differs from its configured path",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        if current_sha256 != expected_sha256:
            raise AdvisoryModelFirstError(
                "current Advisory model descriptor differs from the expected version",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_CONFLICT",
                context={
                    "program_id": body["program_id"],
                    "binding_version_id": body["binding_version_id"],
                    "expected_current_descriptor_sha256": expected_sha256,
                    "current_descriptor_sha256": current_sha256,
                },
            )
        next_sha256 = str(body["descriptor_sha256"])
        if current_bytes == encoded:
            return AdvisoryModelDescriptorRotationReceipt(
                operation="UNCHANGED",
                descriptor_path=target,
                previous_descriptor_sha256=current_sha256,
                descriptor_sha256=next_sha256,
                rollback_snapshot_path=None,
            )
        snapshot_path = _descriptor_history_path(
            descriptor_path=target,
            descriptor_sha256=current_sha256,
        )
        _write_immutable_descriptor_snapshot(path=snapshot_path, encoded=current_bytes)
        try:
            _atomic_write_descriptor(target=target, encoded=encoded)
        except Exception:
            _restore_descriptor_after_failed_rotation(
                target=target,
                previous_bytes=current_bytes,
            )
            raise
        return AdvisoryModelDescriptorRotationReceipt(
            operation="ROTATED",
            descriptor_path=target,
            previous_descriptor_sha256=current_sha256,
            descriptor_sha256=next_sha256,
            rollback_snapshot_path=snapshot_path,
        )


def rollback_program_model_descriptor(
    *,
    model_root: str | Path,
    program_id: str,
    binding_version_id: str,
    expected_current_descriptor_sha256: str,
    rollback_descriptor_sha256: str,
) -> AdvisoryModelDescriptorRotationReceipt:
    rollback_sha256 = str(rollback_descriptor_sha256).strip()
    if not _is_sha256(rollback_sha256):
        raise AdvisoryModelFirstError(
            "rollback Advisory model descriptor hash is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": "rollback_descriptor_sha256"},
        )
    target = AdvisoryModelBindingResolver.descriptor_path(
        model_root=model_root,
        program_id=program_id,
        binding_version_id=binding_version_id,
    )
    snapshot_path = _descriptor_history_path(
        descriptor_path=target,
        descriptor_sha256=rollback_sha256,
    )
    if not snapshot_path.is_file():
        raise AdvisoryModelFirstError(
            "requested Advisory model descriptor rollback snapshot is unavailable",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={
                "program_id": str(program_id).strip(),
                "binding_version_id": str(binding_version_id).strip(),
                "rollback_descriptor_sha256": rollback_sha256,
            },
        )
    snapshot_bytes = _read_descriptor_bytes(
        snapshot_path,
        reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
    )
    try:
        snapshot_payload, snapshot_sha256 = _validated_descriptor(snapshot_bytes)
    except AdvisoryModelFirstError as exc:
        raise AdvisoryModelFirstError(
            "requested Advisory model descriptor rollback snapshot is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={"rollback_descriptor_sha256": rollback_sha256},
        ) from exc
    if snapshot_sha256 != rollback_sha256:
        raise AdvisoryModelFirstError(
            "requested Advisory model descriptor rollback snapshot is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={"rollback_descriptor_sha256": rollback_sha256},
        )
    if (
        snapshot_payload.get("program_id") != str(program_id).strip()
        or snapshot_payload.get("binding_version_id") != str(binding_version_id).strip()
    ):
        raise AdvisoryModelFirstError(
            "rollback Advisory model descriptor identity differs from the requested target",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={"rollback_descriptor_sha256": rollback_sha256},
        )
    snapshot_payload.pop("descriptor_sha256", None)
    receipt = rotate_program_model_descriptor(
        model_root=model_root,
        payload=snapshot_payload,
        expected_current_descriptor_sha256=expected_current_descriptor_sha256,
    )
    if receipt.descriptor_path != target:
        raise AdvisoryModelFirstError(
            "rollback Advisory model descriptor identity differs from the requested target",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
        )
    return AdvisoryModelDescriptorRotationReceipt(
        operation=(
            "ROLLED_BACK" if receipt.operation == "ROTATED" else receipt.operation
        ),
        descriptor_path=receipt.descriptor_path,
        previous_descriptor_sha256=receipt.previous_descriptor_sha256,
        descriptor_sha256=receipt.descriptor_sha256,
        rollback_snapshot_path=receipt.rollback_snapshot_path,
    )


def _prepare_program_model_descriptor(
    *,
    model_root: str | Path,
    payload: Mapping[str, Any],
) -> tuple[dict[str, Any], bytes, Path]:
    body = dict(payload)
    if "descriptor_sha256" in body:
        raise AdvisoryModelFirstError(
            "descriptor_sha256 is derived and cannot be supplied",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    schema_version = str(body.get("schema_version") or "")
    required = {
        "schema_version",
        "program_id",
        "binding_version_id",
        "package_ids",
        "package_id",
        "manifest_sha256",
        "style_profile_id",
        "style_profile_hash",
        "selection_runtime_semantics_hash",
        "feature_schema_version",
        "feature_schema_hash",
        "bundle_id",
        "bundle_manifest_sha256",
        "candidate_projection",
        "created_at",
    }
    if schema_version == DESCRIPTOR_SCHEMA_VERSION_V2:
        required.update({"model_role", "shadow_policy_sha256"})
    missing = sorted(required - set(body))
    if missing:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor publish payload is incomplete",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"missing_fields": missing},
        )
    _validate_publish_payload(body)
    body["descriptor_sha256"] = canonical_json_sha256(body)
    program_id = str(body["program_id"]).strip()
    binding_version_id = str(body["binding_version_id"]).strip()
    target = AdvisoryModelBindingResolver.descriptor_path(
        model_root=model_root,
        program_id=program_id,
        binding_version_id=binding_version_id,
    )
    normalized_path, normalized_root = _normalized_descriptor_access(
        descriptor_path=target,
        model_root=model_root,
    )
    if not normalized_path.startswith(normalized_root + os.sep):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor path escapes its configured root",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": "descriptor_path"},
        )
    target = Path(normalized_path)
    encoded = (
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    return body, encoded, target


def _atomic_write_descriptor(*, target: Path, encoded: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        temporary = Path(temporary_name)
        if temporary.read_bytes() != encoded:
            raise AdvisoryModelFirstError(
                "Advisory Program model descriptor readback differs from its payload",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
        os.replace(temporary, target)
        if target.read_bytes() != encoded:
            raise AdvisoryModelFirstError(
                "Advisory Program model descriptor readback differs after atomic replace",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
    except AdvisoryModelFirstError:
        raise
    except OSError as exc:
        raise AdvisoryModelFirstError(
            "Advisory Program model descriptor atomic write failed",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    finally:
        Path(temporary_name).unlink(missing_ok=True)


def _validated_descriptor(encoded: bytes) -> tuple[dict[str, Any], str]:
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "persisted Advisory model descriptor is not valid JSON",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "persisted Advisory model descriptor must be a JSON object",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    without_hash = dict(payload)
    descriptor_sha256 = str(without_hash.pop("descriptor_sha256", "")).strip()
    if not _is_sha256(descriptor_sha256) or descriptor_sha256 != canonical_json_sha256(
        without_hash
    ):
        raise AdvisoryModelFirstError(
            "persisted Advisory model descriptor hash is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    return payload, descriptor_sha256


def _read_descriptor_bytes(
    path: Path,
    *,
    reason_code: str = "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor cannot be read",
            reason_code=reason_code,
            context={"error_type": type(exc).__name__},
        ) from exc


def _descriptor_history_path(
    *,
    descriptor_path: Path,
    descriptor_sha256: str,
) -> Path:
    if not _is_sha256(descriptor_sha256):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor history hash is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    program_root = os.path.realpath(os.fspath(descriptor_path.parent))
    history_root = os.path.realpath(os.path.join(program_root, ".descriptor_history"))
    if not history_root.startswith(program_root + os.sep):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor history path escapes its Program root",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": "descriptor_history_root"},
        )
    history_path = os.path.realpath(
        os.path.join(
            history_root,
            descriptor_path.stem,
            f"{descriptor_sha256}.json",
        )
    )
    if not history_path.startswith(history_root + os.sep):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor history path escapes its configured root",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": "descriptor_history_path"},
        )
    return Path(history_path)


def _write_immutable_descriptor_snapshot(*, path: Path, encoded: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if (
            path.is_file()
            and _read_descriptor_bytes(
                path,
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            )
            == encoded
        ):
            return
        raise AdvisoryModelFirstError(
            "Advisory model descriptor history identity already contains different bytes",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={"rollback_descriptor_sha256": path.stem},
        )
    except OSError as exc:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor history snapshot cannot be created",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={"error_type": type(exc).__name__},
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if path.read_bytes() != encoded:
            raise AdvisoryModelFirstError(
                "Advisory model descriptor history snapshot readback differs from its source",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            )
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _restore_descriptor_after_failed_rotation(
    *, target: Path, previous_bytes: bytes
) -> None:
    try:
        if target.is_file() and target.read_bytes() == previous_bytes:
            return
        _atomic_write_descriptor(target=target, encoded=previous_bytes)
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor rotation failed and the previous descriptor could not be restored",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE",
            context={"restore_error_type": type(exc).__name__},
        ) from exc


@contextmanager
def _exclusive_descriptor_lock(target: Path):
    target_parent = os.path.realpath(os.fspath(target.parent))
    lock_path = Path(
        os.path.realpath(os.path.join(target_parent, f".{target.name}.lock"))
    )
    if not os.fspath(lock_path).startswith(target_parent + os.sep):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor lock path escapes its configured root",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": "descriptor_lock_path"},
        )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = lock_path.open("a+b")
    except OSError as exc:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor lock cannot be opened",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    with handle:
        try:
            if lock_path.stat().st_size == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
        except OSError as exc:
            raise AdvisoryModelFirstError(
                "Advisory model descriptor lock cannot be initialized",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"error_type": type(exc).__name__},
            ) from exc
        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            except OSError as exc:
                raise AdvisoryModelFirstError(
                    "Advisory model descriptor lock cannot be acquired",
                    reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                    context={"error_type": type(exc).__name__},
                ) from exc
            try:
                yield
            finally:
                try:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError as exc:
                    raise AdvisoryModelFirstError(
                        "Advisory model descriptor lock cannot be released",
                        reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                        context={"error_type": type(exc).__name__},
                    ) from exc
        else:
            import fcntl

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            except OSError as exc:
                raise AdvisoryModelFirstError(
                    "Advisory model descriptor lock cannot be acquired",
                    reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                    context={"error_type": type(exc).__name__},
                ) from exc
            try:
                yield
            finally:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError as exc:
                    raise AdvisoryModelFirstError(
                        "Advisory model descriptor lock cannot be released",
                        reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                        context={"error_type": type(exc).__name__},
                    ) from exc


def _validate_publish_payload(body: Mapping[str, Any]) -> None:
    package_id = str(body.get("package_id") or "").strip()
    package_ids = body.get("package_ids")
    projection = body.get("candidate_projection")
    roles = (
        projection.get("component_roles") if isinstance(projection, Mapping) else None
    )
    schema_version = str(body.get("schema_version") or "")
    sha_fields = (
        "manifest_sha256",
        "style_profile_hash",
        "selection_runtime_semantics_hash",
        "feature_schema_hash",
        "bundle_id",
        "bundle_manifest_sha256",
    )
    if schema_version == DESCRIPTOR_SCHEMA_VERSION_V2:
        sha_fields += ("shadow_policy_sha256",)
    valid = (
        schema_version in {DESCRIPTOR_SCHEMA_VERSION, DESCRIPTOR_SCHEMA_VERSION_V2}
        and bool(str(body.get("program_id") or "").strip())
        and bool(str(body.get("binding_version_id") or "").strip())
        and isinstance(package_ids, list)
        and package_ids == [package_id]
        and bool(package_id)
        and all(_is_sha256(str(body.get(field) or "")) for field in sha_fields)
        and bool(str(body.get("style_profile_id") or "").strip())
        and bool(str(body.get("feature_schema_version") or "").strip())
        and isinstance(projection, Mapping)
        and projection.get("schema_version") == CANDIDATE_PROJECTION_SCHEMA_VERSION
        and isinstance(roles, Mapping)
        and set(roles) == {"lstm", "fund"}
    )
    normalized_roles = (
        {str(key): str(value).strip() for key, value in roles.items()}
        if isinstance(roles, Mapping)
        else {}
    )
    if (
        not valid
        or any(not value for value in normalized_roles.values())
        or len(set(normalized_roles.values())) != 2
    ):
        raise AdvisoryModelFirstError(
            "Advisory Program model descriptor publish payload is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    if schema_version == DESCRIPTOR_SCHEMA_VERSION_V2:
        if str(body.get("model_role") or "").strip() != META_LABEL_MODEL_ROLE:
            raise AdvisoryModelFirstError(
                "Advisory Program model descriptor publish payload has an unsupported model role",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
        _normalized_terminal_weights(
            (
                projection.get("terminal_weights")
                if isinstance(projection, Mapping)
                else None
            ),
            component_roles=normalized_roles,
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )


def _normalized_terminal_weights(
    value: Any,
    *,
    component_roles: Mapping[str, str],
    reason_code: str,
) -> dict[str, float]:
    component_ids = set(component_roles.values())
    if not isinstance(value, Mapping) or set(value) != component_ids:
        raise AdvisoryModelFirstError(
            "Advisory candidate projection terminal weights are invalid",
            reason_code=reason_code,
        )
    try:
        normalized = {str(key): float(raw) for key, raw in value.items()}
    except (TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "Advisory candidate projection terminal weights are invalid",
            reason_code=reason_code,
        ) from exc
    if (
        any(not (weight > 0.0) for weight in normalized.values())
        or abs(sum(normalized.values()) - 1.0) > 1e-10
    ):
        raise AdvisoryModelFirstError(
            "Advisory candidate projection terminal weights are invalid",
            reason_code=reason_code,
        )
    return normalized


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor cannot be read",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={
                "descriptor_sha256": (
                    hashlib.sha256(raw).hexdigest() if "raw" in locals() else None
                )
            },
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor must be a JSON object",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    return payload


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _descriptor_identity(field: str, value: str) -> str:
    normalized = str(value).strip()
    pattern = _DESCRIPTOR_IDENTITY_PATTERNS[field]
    if not pattern.fullmatch(normalized):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor identity cannot be used as a path component",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"field": field},
        )
    return normalized


def _normalized_descriptor_access(
    *,
    descriptor_path: Path,
    model_root: str | Path,
) -> tuple[str, str]:
    normalized_path = os.path.realpath(os.fspath(descriptor_path))
    normalized_root = os.path.realpath(
        os.path.join(os.fspath(model_root), "program_bindings")
    )
    return normalized_path, normalized_root
