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
from backend.services.advisory_model_first.price_range_bundle import (
    read_price_range_bundle_manifest,
)
from backend.services.advisory_model_first.price_range_contracts import (
    PRICE_RANGE_MODEL_NAMES,
    canonical_json_sha256,
)


@dataclass(frozen=True)
class LoadedAdvisoryPriceRangeBundle:
    price_range_bundle_id: str
    bundle_path: Path
    manifest: dict[str, Any]
    feature_schema: dict[str, Any]
    models: dict[str, Any]
    calibration_spec: dict[str, Any] | None = None


def price_range_binding_path(
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
            "price-range binding path identity is invalid",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    target = (
        root
        / "price_range_bindings"
        / package_id
        / manifest_sha256
        / f"{style_profile_hash}.json"
    )
    try:
        target.resolve().relative_to(root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "price-range binding path escapes the model root",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        ) from exc
    return target


def publish_price_range_binding(
    *,
    model_root: str | Path,
    price_range_bundle_id: str,
    activated_at: str | None = None,
) -> Path:
    root = Path(model_root).resolve()
    bundle_path = root / "price_range_bundles" / price_range_bundle_id
    manifest_path = bundle_path / "manifest.json"
    manifest = _read_runtime_bundle_manifest(bundle_path, expected_bundle_id=price_range_bundle_id)
    _validate_runtime_manifest(manifest)
    payload = {
        "schema_version": "advisory_price_range_binding_v1",
        "package_id": manifest["package_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "style_profile_id": manifest["style_profile_id"],
        "style_profile_hash": manifest["style_profile_hash"],
        "feature_schema_version": manifest["feature_schema_version"],
        "feature_schema_hash": manifest["feature_schema_hash"],
        "label_policy_version": manifest["label_policy_version"],
        "parent_bundle_id": manifest["parent_bundle_id"],
        "outcome_bundle_id": manifest["outcome_bundle_id"],
        "price_range_bundle_id": price_range_bundle_id,
        "price_range_bundle_manifest_sha256": _sha256_file(manifest_path),
        "activated_at": activated_at or datetime.now(timezone.utc).isoformat(),
    }
    payload["binding_sha256"] = canonical_json_sha256(payload)
    target = price_range_binding_path(
        root,
        package_id=str(manifest["package_id"]),
        manifest_sha256=str(manifest["manifest_sha256"]),
        style_profile_hash=str(manifest["style_profile_hash"]),
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        _write_json(temporary, payload)
        if _read_json(temporary) != payload:
            raise AdvisoryModelFirstError(
                "price-range binding readback differs from its published payload",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            )
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def load_exact_price_range_bundle(
    *,
    model_root: str | Path,
    package_id: str,
    manifest_sha256: str,
    style_profile_hash: str,
    parent_bundle_id: str,
    outcome_bundle_id: str,
    booster_factory: Callable[[Path], Any] | None = None,
) -> LoadedAdvisoryPriceRangeBundle:
    root = Path(model_root).resolve()
    if not _is_sha256(parent_bundle_id) or not _is_sha256(outcome_bundle_id):
        raise AdvisoryModelFirstError(
            "active parent or outcome bundle identity is invalid",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    binding_path = price_range_binding_path(
        root,
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        style_profile_hash=style_profile_hash,
    )
    binding = _read_json(
        binding_path,
        missing_reason_code="ADVISORY_PRICE_RANGE_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
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
        "outcome_bundle_id",
        "price_range_bundle_id",
        "price_range_bundle_manifest_sha256",
        "activated_at",
        "binding_sha256",
    }
    missing = sorted(required - set(binding))
    if missing:
        raise AdvisoryModelFirstError(
            "exact price-range binding is incomplete",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"missing_fields": missing},
        )
    payload = dict(binding)
    binding_sha256 = str(payload.pop("binding_sha256"))
    if binding_sha256 != canonical_json_sha256(payload):
        raise AdvisoryModelFirstError(
            "exact price-range binding content hash is invalid",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    expected_binding = {
        "schema_version": "advisory_price_range_binding_v1",
        "package_id": package_id,
        "manifest_sha256": manifest_sha256,
        "style_profile_hash": style_profile_hash,
        "parent_bundle_id": parent_bundle_id,
        "outcome_bundle_id": outcome_bundle_id,
    }
    actual_binding = {key: binding.get(key) for key in expected_binding}
    if actual_binding != expected_binding:
        raise AdvisoryModelFirstError(
            "exact price-range binding differs from active model identities",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"actual_identity": actual_binding},
        )
    bundle_id = str(binding["price_range_bundle_id"])
    manifest_hash = str(binding["price_range_bundle_manifest_sha256"])
    if not _is_sha256(bundle_id) or not _is_sha256(manifest_hash):
        raise AdvisoryModelFirstError(
            "exact price-range binding contains an invalid bundle identity",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    bundle_path = root / "price_range_bundles" / bundle_id
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file() or _sha256_file(manifest_path) != manifest_hash:
        raise AdvisoryModelFirstError(
            "price-range bundle manifest differs from its exact binding",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    manifest = _read_runtime_bundle_manifest(bundle_path, expected_bundle_id=bundle_id)
    _validate_runtime_manifest(manifest)
    expected_manifest = {
        "package_id": package_id,
        "manifest_sha256": manifest_sha256,
        "style_profile_hash": style_profile_hash,
        "parent_bundle_id": parent_bundle_id,
        "outcome_bundle_id": outcome_bundle_id,
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
    }
    actual_manifest = {key: manifest.get(key) for key in expected_manifest}
    if actual_manifest != expected_manifest:
        raise AdvisoryModelFirstError(
            "price-range bundle identity differs from its exact binding",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"actual_identity": actual_manifest},
        )
    for field in (
        "style_profile_id",
        "feature_schema_version",
        "feature_schema_hash",
        "label_policy_version",
    ):
        if binding[field] != manifest.get(field):
            raise AdvisoryModelFirstError(
                "price-range binding declarations differ from its bundle manifest",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
                context={"field": field},
            )
    feature_schema = _read_json(
        bundle_path / "feature_schema.json",
        missing_reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
    )
    factory = booster_factory or _load_lightgbm_booster
    models: dict[str, Any] = {}
    for name in PRICE_RANGE_MODEL_NAMES:
        path = bundle_path / "models" / f"{name}.txt"
        try:
            models[name] = factory(path)
        except AdvisoryModelFirstError:
            raise
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "LightGBM price-range head cannot be loaded",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
                context={"head": name, "error_type": type(exc).__name__},
            ) from exc
    return LoadedAdvisoryPriceRangeBundle(
        price_range_bundle_id=bundle_id,
        bundle_path=bundle_path,
        manifest=manifest,
        feature_schema=feature_schema,
        models=models,
        calibration_spec=(
            _read_json(bundle_path / "calibration_spec.json")
            if manifest.get("schema_version") == "advisory_price_range_bundle_v2"
            else None
        ),
    )


def _validate_runtime_manifest(manifest: dict[str, Any]) -> None:
    schema_version = manifest.get("schema_version")
    expected = {
        "status": "EXPERIMENTAL_SHADOW",
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_price_range_label_policy_v1",
        "entry_gap_condition": "ENTRY_EXECUTABLE",
        "model_names": list(PRICE_RANGE_MODEL_NAMES),
        "model_count": len(PRICE_RANGE_MODEL_NAMES),
    }
    actual = {key: manifest.get(key) for key in expected}
    valid_calibration = (
        schema_version == "advisory_price_range_bundle_v1"
        and manifest.get("calibration_state") == "UNCALIBRATED"
    ) or (
        schema_version == "advisory_price_range_bundle_v2"
        and manifest.get("calibration_state") == "CALIBRATED_INTERVAL"
        and manifest.get("entry_gap_calibration_state") == "CALIBRATED"
        and manifest.get("entry_executable_calibration_state") == "UNCALIBRATED"
    )
    if actual != expected or not valid_calibration:
        raise AdvisoryModelFirstError(
            "price-range bundle runtime manifest is incompatible",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"expected": expected, "actual": actual},
        )


def _read_runtime_bundle_manifest(bundle_path: Path, *, expected_bundle_id: str) -> dict[str, Any]:
    header = _read_json(bundle_path / "manifest.json")
    schema = header.get("schema_version")
    if schema in {None, "advisory_price_range_bundle_v1"}:
        return read_price_range_bundle_manifest(bundle_path, expected_bundle_id=expected_bundle_id)
    if schema == "advisory_price_range_bundle_v2":
        from backend.services.advisory_model_first.price_range_calibration_bundle import (
            validate_calibrated_price_range_bundle,
        )

        return validate_calibrated_price_range_bundle(bundle_path, expected_bundle_id=expected_bundle_id)
    raise AdvisoryModelFirstError(
        "price-range bundle schema is unsupported",
        reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        context={"schema_version": schema},
    )


def _load_lightgbm_booster(path: Path) -> Any:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable for price-range inference",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"error_type": type(exc).__name__},
        ) from exc
    return lgb.Booster(model_file=str(path))


def _read_json(
    path: Path,
    *,
    missing_reason_code: str = "ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
) -> dict[str, Any]:
    if not path.is_file():
        raise AdvisoryModelFirstError(
            "exact price-range binding or bundle member is unavailable",
            reason_code=missing_reason_code,
            context={"path": str(path)},
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "exact price-range binding or bundle member cannot be read",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"path": str(path), "error_type": type(exc).__name__},
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "exact price-range binding or bundle member is not an object",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
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
