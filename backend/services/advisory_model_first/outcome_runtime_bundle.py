from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH
from backend.services.advisory_model_first.outcome_bundle import read_outcome_bundle_manifest
from backend.services.advisory_model_first.outcome_contracts import (
    OUTCOME_HORIZONS,
    OUTCOME_QUANTILES,
    canonical_json_sha256,
)


@dataclass(frozen=True)
class LoadedAdvisoryOutcomeBundle:
    outcome_bundle_id: str
    bundle_path: Path
    manifest: dict[str, Any]
    feature_schema: dict[str, Any]
    models: dict[str, Any]


def expected_outcome_model_names() -> tuple[str, ...]:
    names: list[str] = []
    for horizon in OUTCOME_HORIZONS:
        names.extend(
            f"excess_return_h{horizon}_q{int(quantile * 100):02d}"
            for quantile in OUTCOME_QUANTILES
        )
        names.extend((f"positive_excess_h{horizon}", f"signal_survival_h{horizon}"))
        for prefix in ("path_mfe", "path_mae_loss"):
            names.extend(f"{prefix}_h{horizon}_q{value}" for value in (50, 90))
    names.append("holding_bucket")
    return tuple(names)


def outcome_binding_path(
    model_root: str | Path,
    *,
    package_id: str,
    manifest_sha256: str,
    style_profile_hash: str,
) -> Path:
    root = Path(model_root).resolve()
    if (
        not package_id
        or Path(package_id).name != package_id
        or not _is_sha256(manifest_sha256)
        or not _is_sha256(style_profile_hash)
    ):
        raise AdvisoryModelFirstError(
            "outcome binding path identity is invalid",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
        )
    target = (
        root
        / "outcome_bindings"
        / package_id
        / manifest_sha256
        / f"{style_profile_hash}.json"
    )
    try:
        target.resolve().relative_to(root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "outcome binding path escapes the model root",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
        ) from exc
    return target


def publish_outcome_binding(
    *,
    model_root: str | Path,
    outcome_bundle_id: str,
    activated_at: str | None = None,
) -> Path:
    root = Path(model_root).resolve()
    bundle_path = root / "outcome_bundles" / outcome_bundle_id
    manifest_path = bundle_path / "manifest.json"
    manifest = read_outcome_bundle_manifest(
        bundle_path,
        expected_bundle_id=outcome_bundle_id,
    )
    _validate_outcome_runtime_manifest(manifest)
    payload = {
        "schema_version": "advisory_outcome_binding_v1",
        "package_id": manifest["package_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "style_profile_id": manifest["style_profile_id"],
        "style_profile_hash": manifest["style_profile_hash"],
        "feature_schema_version": manifest["feature_schema_version"],
        "feature_schema_hash": manifest["feature_schema_hash"],
        "label_policy_version": manifest["label_policy_version"],
        "parent_bundle_id": manifest["parent_bundle_id"],
        "parent_request_id": manifest["parent_request_id"],
        "outcome_bundle_id": outcome_bundle_id,
        "outcome_bundle_manifest_sha256": _sha256_file(manifest_path),
        "activated_at": activated_at or datetime.now(timezone.utc).isoformat(),
    }
    payload["binding_sha256"] = canonical_json_sha256(payload)
    target = outcome_binding_path(
        root,
        package_id=str(manifest["package_id"]),
        manifest_sha256=str(manifest["manifest_sha256"]),
        style_profile_hash=str(manifest["style_profile_hash"]),
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        _write_json(temporary, payload)
        if _read_json(temporary) != payload:
            raise AdvisoryModelFirstError(
                "outcome binding readback differs from its published payload",
                reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            )
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def load_exact_outcome_bundle(
    *,
    model_root: str | Path,
    package_id: str,
    manifest_sha256: str,
    style_profile_hash: str,
    parent_bundle_id: str,
    booster_factory: Callable[[Path], Any] | None = None,
) -> LoadedAdvisoryOutcomeBundle:
    root = Path(model_root).resolve()
    if not _is_sha256(parent_bundle_id):
        raise AdvisoryModelFirstError(
            "active parent bundle identity is invalid",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
        )
    binding_path = outcome_binding_path(
        root,
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        style_profile_hash=style_profile_hash,
    )
    binding = _read_json(
        binding_path,
        missing_reason_code="ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE",
    )
    required = {
        "schema_version",
        "package_id",
        "manifest_sha256",
        "style_profile_id",
        "style_profile_hash",
        "feature_schema_version",
        "feature_schema_hash",
        "label_policy_version",
        "parent_bundle_id",
        "parent_request_id",
        "outcome_bundle_id",
        "outcome_bundle_manifest_sha256",
        "activated_at",
        "binding_sha256",
    }
    missing = sorted(required - set(binding))
    if missing:
        raise AdvisoryModelFirstError(
            "exact outcome binding is incomplete",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={"missing_fields": missing},
        )
    binding_payload = dict(binding)
    actual_binding_sha256 = str(binding_payload.pop("binding_sha256"))
    if actual_binding_sha256 != canonical_json_sha256(binding_payload):
        raise AdvisoryModelFirstError(
            "exact outcome binding content hash is invalid",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
        )
    if (
        binding["schema_version"] != "advisory_outcome_binding_v1"
        or binding["package_id"] != package_id
        or binding["manifest_sha256"] != manifest_sha256
        or binding["style_profile_hash"] != style_profile_hash
        or binding["parent_bundle_id"] != parent_bundle_id
    ):
        raise AdvisoryModelFirstError(
            "exact outcome binding identity differs from the active parent model",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
        )
    outcome_bundle_id = str(binding["outcome_bundle_id"])
    manifest_hash = str(binding["outcome_bundle_manifest_sha256"])
    if not _is_sha256(outcome_bundle_id) or not _is_sha256(manifest_hash):
        raise AdvisoryModelFirstError(
            "exact outcome binding contains an invalid bundle identity",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
        )
    bundle_path = root / "outcome_bundles" / outcome_bundle_id
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file() or _sha256_file(manifest_path) != manifest_hash:
        raise AdvisoryModelFirstError(
            "exact outcome bundle manifest differs from its binding",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
        )
    manifest = read_outcome_bundle_manifest(
        bundle_path,
        expected_bundle_id=outcome_bundle_id,
    )
    _validate_outcome_runtime_manifest(manifest)
    expected_identity = {
        "package_id": package_id,
        "manifest_sha256": manifest_sha256,
        "style_profile_hash": style_profile_hash,
        "parent_bundle_id": parent_bundle_id,
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
    }
    actual_identity = {key: manifest.get(key) for key in expected_identity}
    if actual_identity != expected_identity:
        raise AdvisoryModelFirstError(
            "outcome bundle identity differs from the exact binding path",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"actual_identity": actual_identity},
        )
    for field in (
        "style_profile_id",
        "feature_schema_version",
        "feature_schema_hash",
        "label_policy_version",
        "parent_request_id",
    ):
        if binding[field] != manifest.get(field):
            raise AdvisoryModelFirstError(
                "outcome binding declarations differ from its bundle manifest",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
                context={"field": field},
            )
    feature_schema = _read_json(
        bundle_path / "feature_schema.json",
        missing_reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
    )
    model_names = expected_outcome_model_names()
    model_paths = {name: bundle_path / "models" / f"{name}.txt" for name in model_names}
    actual_model_files = {
        path.stem for path in (bundle_path / "models").glob("*.txt") if path.is_file()
    }
    if actual_model_files != set(model_names):
        raise AdvisoryModelFirstError(
            "outcome bundle model set differs from the runtime contract",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={
                "missing_models": sorted(set(model_names) - actual_model_files),
                "unexpected_models": sorted(actual_model_files - set(model_names)),
            },
        )
    factory = booster_factory or _load_lightgbm_booster
    models: dict[str, Any] = {}
    for name, path in model_paths.items():
        try:
            models[name] = factory(path)
        except AdvisoryModelFirstError:
            raise
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "LightGBM outcome head cannot be loaded",
                reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
                context={"head": name, "error_type": type(exc).__name__},
            ) from exc
    return LoadedAdvisoryOutcomeBundle(
        outcome_bundle_id=outcome_bundle_id,
        bundle_path=bundle_path,
        manifest=manifest,
        feature_schema=feature_schema,
        models=models,
    )


def _validate_outcome_runtime_manifest(manifest: dict[str, Any]) -> None:
    expected = {
        "schema_version": "advisory_outcome_bundle_v1",
        "status": "EXPERIMENTAL_SHADOW",
        "calibration_state": "UNCALIBRATED",
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_outcome_label_policy_v1",
        "horizons": list(OUTCOME_HORIZONS),
        "quantiles": list(OUTCOME_QUANTILES),
        "model_count": len(expected_outcome_model_names()),
    }
    actual = {key: manifest.get(key) for key in expected}
    if actual != expected:
        raise AdvisoryModelFirstError(
            "outcome bundle runtime manifest is incompatible",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={"expected": expected, "actual": actual},
        )


def _load_lightgbm_booster(path: Path) -> Any:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable for outcome inference",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    return lgb.Booster(model_file=str(path))


def _read_json(
    path: Path,
    *,
    missing_reason_code: str = "ADVISORY_OUTCOME_BUNDLE_INVALID",
) -> dict[str, Any]:
    if not path.is_file():
        raise AdvisoryModelFirstError(
            "exact outcome binding or bundle member is not available",
            reason_code=missing_reason_code,
            context={"path": str(path)},
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "exact outcome binding or bundle member cannot be read",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={"path": str(path), "error_type": type(exc).__name__},
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "exact outcome binding or bundle member is not an object",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={"path": str(path)},
        )
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)
