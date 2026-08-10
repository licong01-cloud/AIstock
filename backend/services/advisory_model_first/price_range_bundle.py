from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    FEATURE_SCHEMA_HASH,
    FEATURE_SCHEMA_PAYLOAD,
)
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit
from backend.services.advisory_model_first.price_range_contracts import (
    ENTRY_GAP_CONDITION,
    PRICE_RANGE_MODEL_NAMES,
    PRICE_RANGE_QUANTILES,
    FrozenAdvisoryPriceRangeTrainingRequestV1,
    canonical_json_sha256,
)

if TYPE_CHECKING:
    from backend.services.advisory_model_first.price_range_training import PriceRangeTrainingResult


def publish_price_range_bundle(
    *,
    model_root: str | Path,
    request: FrozenAdvisoryPriceRangeTrainingRequestV1,
    split: OutcomeDateSplit,
    training: "PriceRangeTrainingResult",
    environment_report: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    if tuple(sorted(training.models)) != tuple(sorted(PRICE_RANGE_MODEL_NAMES)):
        raise AdvisoryModelFirstError(
            "price-range bundle requires the exact four model heads",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"model_names": sorted(training.models)},
        )
    root = Path(model_root).resolve()
    bundles_root = root / "price_range_bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".price-range-bundle-", dir=bundles_root))
    try:
        models_root = temporary / "models"
        models_root.mkdir()
        for name in PRICE_RANGE_MODEL_NAMES:
            training.models[name].save_model(str(models_root / f"{name}.txt"))
        _write_json(temporary / "training_request.json", request.model_dump(mode="json"))
        _write_json(
            temporary / "feature_schema.json",
            {
                **FEATURE_SCHEMA_PAYLOAD,
                "feature_schema_hash": FEATURE_SCHEMA_HASH,
                "trained_feature_names": list(training.feature_names),
                "categorical_vocabulary": {
                    name: list(values)
                    for name, values in training.categorical_vocabulary.items()
                },
            },
        )
        _write_json(
            temporary / "label_policy.json",
            {
                "schema_version": request.label_policy_version,
                "entry_session": "next_trading_day_open",
                "binary_label": "authoritative_entry_executable",
                "missing_market_row_semantics": "UNAVAILABLE_NOT_NEGATIVE",
                "entry_gap_formula": "target_open/decision_close-1",
                "entry_gap_condition": ENTRY_GAP_CONDITION,
                "quantiles": list(PRICE_RANGE_QUANTILES),
            },
        )
        _write_json(temporary / "split.json", split.as_dict())
        _write_json(temporary / "metrics.json", training.metrics)
        _write_json(
            temporary / "training_log.json",
            {
                **training.training_log,
                "environment_report": dict(environment_report),
                "resource_report": dict(resource_report),
            },
        )
        training.test_predictions.to_parquet(
            temporary / "test_predictions.parquet", index=False
        )
        files = _file_descriptors(temporary)
        manifest_payload = {
            "schema_version": "advisory_price_range_bundle_v1",
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "parent_request_id": request.parent_request_id,
            "parent_request_sha256": request.parent_request_sha256,
            "parent_bundle_id": request.parent_bundle_id,
            "outcome_request_id": request.outcome_request_id,
            "outcome_request_sha256": request.outcome_request_sha256,
            "outcome_bundle_id": request.outcome_bundle_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "style_profile_id": request.style_profile_id,
            "style_profile_hash": request.style_profile_hash,
            "feature_schema_version": request.feature_schema_version,
            "feature_schema_hash": request.feature_schema_hash,
            "label_policy_version": request.label_policy_version,
            "entry_gap_condition": ENTRY_GAP_CONDITION,
            "quantiles": list(PRICE_RANGE_QUANTILES),
            "model_names": list(PRICE_RANGE_MODEL_NAMES),
            "repository_commit": request.repository_commit,
            "model_count": len(training.models),
            "files": files,
        }
        bundle_id = canonical_json_sha256(manifest_payload)
        manifest = {**manifest_payload, "price_range_bundle_id": bundle_id}
        _write_json(temporary / "manifest.json", manifest)
        _validate_price_range_bundle(temporary, expected_bundle_id=bundle_id)
        target = bundles_root / bundle_id
        if target.exists():
            existing = _validate_price_range_bundle(target, expected_bundle_id=bundle_id)
            if existing != manifest:
                raise AdvisoryModelFirstError(
                    "existing price-range bundle identity has different content",
                    reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
                    context={"price_range_bundle_id": bundle_id},
                )
            shutil.rmtree(temporary)
            return bundle_id, target, manifest
        os.replace(temporary, target)
        return bundle_id, target, manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def read_price_range_bundle_manifest(
    bundle_path: str | Path,
    *,
    expected_bundle_id: str,
) -> dict[str, Any]:
    return _validate_price_range_bundle(
        Path(bundle_path).resolve(), expected_bundle_id=expected_bundle_id
    )


def _validate_price_range_bundle(
    bundle_path: Path,
    *,
    expected_bundle_id: str,
) -> dict[str, Any]:
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file():
        raise AdvisoryModelFirstError(
            "price-range bundle manifest is missing",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"path": str(manifest_path)},
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "price-range bundle manifest cannot be read",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    if not isinstance(manifest, dict):
        raise AdvisoryModelFirstError(
            "price-range bundle manifest is not an object",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    payload = {
        key: value for key, value in manifest.items() if key != "price_range_bundle_id"
    }
    actual_id = canonical_json_sha256(payload)
    if manifest.get("price_range_bundle_id") != expected_bundle_id or actual_id != expected_bundle_id:
        raise AdvisoryModelFirstError(
            "price-range bundle canonical identity is invalid",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"expected": expected_bundle_id, "actual": actual_id},
        )
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise AdvisoryModelFirstError(
            "price-range bundle file manifest is empty",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    required = {
        "training_request.json",
        "feature_schema.json",
        "label_policy.json",
        "split.json",
        "metrics.json",
        "training_log.json",
        "test_predictions.parquet",
        *{f"models/{name}.txt" for name in PRICE_RANGE_MODEL_NAMES},
    }
    if set(files) != required:
        raise AdvisoryModelFirstError(
            "price-range bundle members differ from the exact contract",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={
                "missing_files": sorted(required - set(files)),
                "unexpected_files": sorted(set(files) - required),
            },
        )
    for name, descriptor in files.items():
        path = _member_path(bundle_path, str(name))
        if (
            not isinstance(descriptor, dict)
            or not isinstance(descriptor.get("size_bytes"), int)
            or not isinstance(descriptor.get("sha256"), str)
            or not path.is_file()
            or path.stat().st_size != descriptor.get("size_bytes")
            or _sha256_file(path) != descriptor.get("sha256")
        ):
            raise AdvisoryModelFirstError(
                "price-range bundle member is missing or corrupt",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
                context={"filename": name},
            )
    try:
        request = FrozenAdvisoryPriceRangeTrainingRequestV1.model_validate_json(
            (bundle_path / "training_request.json").read_text(encoding="utf-8")
        )
        feature_schema = json.loads(
            (bundle_path / "feature_schema.json").read_text(encoding="utf-8")
        )
        label_policy = json.loads(
            (bundle_path / "label_policy.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "price-range bundle semantic members cannot be read",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    if (
        request.request_id != manifest.get("request_id")
        or request.request_sha256 != manifest.get("request_sha256")
        or request.parent_bundle_id != manifest.get("parent_bundle_id")
        or request.outcome_bundle_id != manifest.get("outcome_bundle_id")
        or request.feature_schema_hash != FEATURE_SCHEMA_HASH
        or manifest.get("feature_schema_hash") != FEATURE_SCHEMA_HASH
        or feature_schema.get("feature_schema_hash") != FEATURE_SCHEMA_HASH
        or tuple(feature_schema.get("trained_feature_names") or ())
        != tuple(FEATURE_SCHEMA_PAYLOAD["model_feature_columns"])
        or label_policy.get("entry_gap_condition") != ENTRY_GAP_CONDITION
        or manifest.get("entry_gap_condition") != ENTRY_GAP_CONDITION
        or tuple(manifest.get("model_names") or ()) != PRICE_RANGE_MODEL_NAMES
        or manifest.get("model_count") != len(PRICE_RANGE_MODEL_NAMES)
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle semantic identities are inconsistent",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    return manifest


def _file_descriptors(root: Path) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for path in sorted(
        item for item in root.rglob("*") if item.is_file() and item.name != "manifest.json"
    ):
        relative = path.relative_to(root).as_posix()
        output[relative] = {
            "sha256": _sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
    return output


def _member_path(root: Path, name: str) -> Path:
    resolved_root = root.resolve()
    relative = Path(name)
    if relative.is_absolute() or not name:
        raise AdvisoryModelFirstError(
            "price-range bundle member path is invalid",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"filename": name},
        )
    path = (resolved_root / relative).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "price-range bundle member escapes its root",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"filename": name},
        ) from exc
    return path


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
