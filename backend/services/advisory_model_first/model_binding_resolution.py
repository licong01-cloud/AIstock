from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


DESCRIPTOR_SCHEMA_VERSION = "advisory_program_model_binding_v1"
CANDIDATE_PROJECTION_SCHEMA_VERSION = "advisory_candidate_projection_v1"


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


class AdvisoryModelBindingResolver:
    @staticmethod
    def descriptor_path(
        *,
        model_root: str | Path,
        program_id: str,
        binding_version_id: str,
    ) -> Path:
        return (
            Path(model_root).resolve()
            / "program_bindings"
            / program_id
            / f"{binding_version_id}.json"
        )

    def is_configured(
        self,
        *,
        model_root: str | Path,
        program_id: str,
        binding_version_id: str,
    ) -> bool:
        return self.descriptor_path(
            model_root=model_root,
            program_id=program_id,
            binding_version_id=binding_version_id,
        ).is_file()

    def resolve(
        self,
        *,
        model_root: str | Path,
        program: Any,
        active_binding: Mapping[str, Any],
        selection_run: Any,
    ) -> AdvisoryModelBindingResolutionV1:
        package_ids = tuple(str(value).strip() for value in (active_binding.get("package_ids") or ()))
        program_package_ids = tuple(str(value).strip() for value in getattr(program, "package_ids", ()))
        if len(package_ids) != 1 or package_ids != program_package_ids:
            raise AdvisoryModelFirstError(
                "model inference requires one native StrategyPackage binding",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            )
        package_id = package_ids[0]
        manifest_sha256 = str(selection_run.manifest_sha256_by_package.get(package_id) or "").strip()
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
        if not descriptor_path.is_file():
            raise AdvisoryModelFirstError(
                "no exact model descriptor is configured for this Advisory Program binding",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
                context={"program_id": program_id, "binding_version_id": binding_version_id},
            )
        payload = _read_json(descriptor_path)
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
        missing = sorted(required - set(payload))
        if missing:
            raise AdvisoryModelFirstError(
                "Advisory model descriptor is incomplete",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
                context={"missing_fields": missing},
            )
        without_hash = dict(payload)
        descriptor_sha256 = str(without_hash.pop("descriptor_sha256") or "")
        if not _is_sha256(descriptor_sha256) or descriptor_sha256 != canonical_json_sha256(without_hash):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor hash is invalid",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
        expected_identity = {
            "schema_version": DESCRIPTOR_SCHEMA_VERSION,
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
        if any(not _is_sha256(str(payload.get(field) or "")) for field in sha_fields):
            raise AdvisoryModelFirstError(
                "Advisory model descriptor contains an invalid content identity",
                reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            )
        projection = payload.get("candidate_projection")
        if not isinstance(projection, Mapping) or projection.get("schema_version") != CANDIDATE_PROJECTION_SCHEMA_VERSION:
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
        if any(not value for value in component_roles.values()) or len(set(component_roles.values())) != 2:
            raise AdvisoryModelFirstError(
                "Advisory candidate projection component roles are invalid",
                reason_code="ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED",
            )
        return AdvisoryModelBindingResolutionV1(
            program_id=program_id,
            binding_version_id=binding_version_id,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            style_profile_id=str(payload["style_profile_id"]),
            style_profile_hash=str(payload["style_profile_hash"]),
            selection_runtime_semantics_hash=str(payload["selection_runtime_semantics_hash"]),
            feature_schema_version=str(payload["feature_schema_version"]),
            feature_schema_hash=str(payload["feature_schema_hash"]),
            bundle_id=str(payload["bundle_id"]),
            bundle_manifest_sha256=str(payload["bundle_manifest_sha256"]),
            component_roles=component_roles,
            descriptor_sha256=descriptor_sha256,
        )


def publish_program_model_descriptor(
    *,
    model_root: str | Path,
    payload: Mapping[str, Any],
) -> Path:
    body = dict(payload)
    if "descriptor_sha256" in body:
        raise AdvisoryModelFirstError(
            "descriptor_sha256 is derived and cannot be supplied",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
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
    }
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
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if target.exists():
        if target.read_bytes() == encoded:
            return target
        raise AdvisoryModelFirstError(
            "exact Advisory Program model descriptor already exists with different bytes",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"program_id": program_id, "binding_version_id": binding_version_id},
        )
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
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
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return target


def _validate_publish_payload(body: Mapping[str, Any]) -> None:
    package_id = str(body.get("package_id") or "").strip()
    package_ids = body.get("package_ids")
    projection = body.get("candidate_projection")
    roles = projection.get("component_roles") if isinstance(projection, Mapping) else None
    sha_fields = (
        "manifest_sha256",
        "style_profile_hash",
        "selection_runtime_semantics_hash",
        "feature_schema_hash",
        "bundle_id",
        "bundle_manifest_sha256",
    )
    valid = (
        body.get("schema_version") == DESCRIPTOR_SCHEMA_VERSION
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
    normalized_roles = {str(key): str(value).strip() for key, value in roles.items()} if isinstance(roles, Mapping) else {}
    if not valid or any(not value for value in normalized_roles.values()) or len(set(normalized_roles.values())) != 2:
        raise AdvisoryModelFirstError(
            "Advisory Program model descriptor publish payload is invalid",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "Advisory model descriptor cannot be read",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
            context={"descriptor_sha256": hashlib.sha256(raw).hexdigest() if "raw" in locals() else None},
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "Advisory model descriptor must be a JSON object",
            reason_code="ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        )
    return payload


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)
