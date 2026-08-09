from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from backend.services.advisory_model_first.contracts import FrozenAdvisoryTrainingRequestV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH, FEATURE_SCHEMA_PAYLOAD
from backend.services.advisory_model_first.reranker_training import RerankerTrainingResult
from backend.services.advisory_model_first.time_split import PurgedDateSplit
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


_RUNTIME_REQUIRED_BUNDLE_FILES = {
    "model.txt",
    "feature_schema.json",
    "fresh_hmm_models.json",
    "baseline_comparison.json",
}


@dataclass(frozen=True)
class LoadedAdvisoryModelBundle:
    bundle_id: str
    bundle_path: Path
    manifest: dict[str, Any]
    feature_schema: dict[str, Any]
    hmm_models: dict[str, Any]
    baselines: dict[str, Any]
    booster: Any


def shadow_binding_path(
    model_root: str | Path,
    *,
    package_id: str,
    manifest_sha256: str,
    style_profile_hash: str,
) -> Path:
    return (
        Path(model_root).resolve()
        / "shadow_bindings"
        / package_id
        / manifest_sha256
        / f"{style_profile_hash}.json"
    )


def load_exact_shadow_bundle(
    *,
    model_root: str | Path,
    package_id: str,
    manifest_sha256: str,
    style_profile_hash: str,
    booster_factory: Callable[[Path], Any] | None = None,
) -> LoadedAdvisoryModelBundle:
    root = Path(model_root).resolve()
    binding_path = shadow_binding_path(
        root,
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        style_profile_hash=style_profile_hash,
    )
    binding = _read_json(binding_path, reason="exact shadow binding is not available")
    required_binding = {
        "schema_version",
        "package_id",
        "manifest_sha256",
        "style_profile_id",
        "style_profile_hash",
        "selection_runtime_semantics_hash",
        "feature_schema_version",
        "feature_schema_hash",
        "bundle_id",
        "bundle_manifest_sha256",
        "activated_at",
        "binding_sha256",
    }
    missing_binding = sorted(required_binding - set(binding))
    if missing_binding:
        raise AdvisoryModelFirstError(
            "exact shadow binding is incomplete",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"missing_fields": missing_binding},
        )
    binding_without_hash = dict(binding)
    actual_binding_sha256 = str(binding_without_hash.pop("binding_sha256"))
    expected_binding_sha256 = canonical_json_sha256(binding_without_hash)
    if actual_binding_sha256 != expected_binding_sha256:
        raise AdvisoryModelFirstError(
            "exact shadow binding content hash is invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    if (
        binding["schema_version"] != "advisory_shadow_binding_v1"
        or binding["package_id"] != package_id
        or binding["manifest_sha256"] != manifest_sha256
        or binding["style_profile_hash"] != style_profile_hash
    ):
        raise AdvisoryModelFirstError(
            "exact shadow binding identity differs from the requested package",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
        )
    bundle_id = str(binding["bundle_id"])
    if not _is_sha256(bundle_id) or not _is_sha256(str(binding["bundle_manifest_sha256"])):
        raise AdvisoryModelFirstError(
            "exact shadow binding contains an invalid bundle identity",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    bundle_path = root / "bundles" / bundle_id
    manifest, feature_schema, hmm_models, baselines = _read_and_validate_bundle(
        bundle_path,
        expected_bundle_id=bundle_id,
        expected_manifest_file_sha256=str(binding["bundle_manifest_sha256"]),
    )
    expected_identity = {
        "package_id": package_id,
        "manifest_sha256": manifest_sha256,
        "style_profile_hash": style_profile_hash,
    }
    actual_identity = {key: manifest.get(key) for key in expected_identity}
    if actual_identity != expected_identity:
        raise AdvisoryModelFirstError(
            "shadow bundle identity differs from the exact binding path",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"actual_identity": actual_identity},
        )
    binding_manifest_fields = {
        key: binding.get(key)
        for key in (
            "style_profile_id",
            "selection_runtime_semantics_hash",
            "feature_schema_version",
            "feature_schema_hash",
        )
    }
    manifest_fields = {key: manifest.get(key) for key in binding_manifest_fields}
    if binding_manifest_fields != manifest_fields:
        raise AdvisoryModelFirstError(
            "exact shadow binding declarations differ from the bundle manifest",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"binding_fields": binding_manifest_fields, "manifest_fields": manifest_fields},
        )
    factory = booster_factory or _load_lightgbm_booster
    try:
        booster = factory(bundle_path / "model.txt")
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM model cannot be loaded from the exact shadow bundle",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"bundle_id": bundle_id, "error_type": type(exc).__name__},
        ) from exc
    return LoadedAdvisoryModelBundle(
        bundle_id=bundle_id,
        bundle_path=bundle_path,
        manifest=manifest,
        feature_schema=feature_schema,
        hmm_models=hmm_models,
        baselines=baselines,
        booster=booster,
    )


def publish_shadow_binding(
    *,
    model_root: str | Path,
    bundle_id: str,
    activated_at: str | None = None,
) -> Path:
    """Atomically publish one explicit bundle; callers must opt into this action."""

    root = Path(model_root).resolve()
    bundle_path = root / "bundles" / bundle_id
    manifest_path = bundle_path / "manifest.json"
    manifest, _feature_schema, _hmm_models, _baselines = _read_and_validate_bundle(
        bundle_path,
        expected_bundle_id=bundle_id,
        expected_manifest_file_sha256=_sha256_file(manifest_path) if manifest_path.is_file() else None,
    )
    payload = {
        "schema_version": "advisory_shadow_binding_v1",
        "package_id": manifest["package_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "style_profile_id": manifest["style_profile_id"],
        "style_profile_hash": manifest["style_profile_hash"],
        "selection_runtime_semantics_hash": manifest["selection_runtime_semantics_hash"],
        "feature_schema_version": manifest["feature_schema_version"],
        "feature_schema_hash": manifest["feature_schema_hash"],
        "bundle_id": bundle_id,
        "bundle_manifest_sha256": _sha256_file(manifest_path),
        "activated_at": activated_at or datetime.now(UTC).isoformat(),
    }
    payload["binding_sha256"] = canonical_json_sha256(payload)
    target = shadow_binding_path(
        root,
        package_id=manifest["package_id"],
        manifest_sha256=manifest["manifest_sha256"],
        style_profile_hash=manifest["style_profile_hash"],
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        _write_json(temporary, payload)
        if _read_json(temporary, reason="shadow binding readback failed") != payload:
            raise AdvisoryModelFirstError(
                "shadow binding readback differs from the published payload",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            )
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def publish_model_bundle(
    *,
    model_root: str | Path,
    request: FrozenAdvisoryTrainingRequestV1,
    split: PurgedDateSplit,
    hmm_models: Mapping[str, Any],
    hmm_unavailable: tuple[dict[str, Any], ...],
    training: RerankerTrainingResult,
    diagnostics: Mapping[str, Any],
    schema_receipt: Mapping[str, Any],
    environment_report: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    root = Path(model_root).resolve()
    bundles_root = root / "bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="advisory_bundle_", dir=root))
    try:
        training.booster.save_model(str(tmp / "model.txt"), num_iteration=training.booster.best_iteration)
        _write_json(tmp / "fresh_hmm_models.json", dict(hmm_models))
        _write_json(tmp / "fresh_hmm_unavailable.json", list(hmm_unavailable))
        _write_json(
            tmp / "feature_schema.json",
            {
                **FEATURE_SCHEMA_PAYLOAD,
                "feature_schema_hash": FEATURE_SCHEMA_HASH,
                "categorical_vocabulary": training.categorical_vocabulary,
                "trained_feature_names": training.feature_names,
            },
        )
        request.write_json(tmp / "training_request.json")
        _write_json(tmp / "split.json", split.as_dict())
        _write_json(
            tmp / "label_policy.json",
            {
                "schema_version": "advisory_label_policy_v1",
                "nominal_horizon_trading_days": 5,
                "maximum_exit_delay_trading_days": 5,
                "open_cost": 0.000095,
                "close_cost": 0.000595,
                "label_gain": [0, 1, 3, 7, 15],
            },
        )
        _write_json(tmp / "metrics.json", training.metrics)
        training.test_predictions.to_parquet(tmp / "test_predictions.parquet", index=False)
        _write_json(tmp / "baseline_comparison.json", training.baseline_comparison)
        _write_json(
            tmp / "training_log.json",
            {
                "evaluation_history": training.evaluation_history,
                "diagnostics": dict(diagnostics),
                "schema_receipt": dict(schema_receipt),
                "environment_report": dict(environment_report),
                "resource_report": dict(resource_report),
            },
        )
        files = {
            path.name: {"sha256": _sha256_file(path), "size_bytes": path.stat().st_size}
            for path in sorted(tmp.iterdir())
            if path.is_file()
        }
        manifest_without_id = {
            "schema_version": "advisory_model_bundle_v1",
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "package_asset_closure_hash": request.package_asset_closure_hash,
            "style_profile_id": request.style_profile_id,
            "style_profile_hash": request.style_profile_hash,
            "selection_runtime_semantics_id": request.selection_runtime_semantics_id,
            "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
            "selection_runtime_semantics": request.selection_runtime_semantics,
            "feature_schema_version": request.feature_schema_version,
            "feature_schema_hash": FEATURE_SCHEMA_HASH,
            "label_policy_version": request.label_policy_version,
            "decision_clock_version": request.decision_clock_version,
            "representative_seed_run_ids": request.representative_seed_run_ids,
            "representative_model_asset_sha256": request.representative_model_asset_sha256,
            "full_seed_roster": {
                leg_id: list(run_ids)
                for leg_id, run_ids in sorted(request.full_seed_roster.items())
            },
            "prediction_artifact_sha256": {
                run_id: descriptor.artifact_sha256
                for run_id, descriptor in sorted(request.prediction_artifacts.items())
            },
            "terminal_weights": request.terminal_weights,
            "combined_reference_sha256": request.combined_reference_sha256,
            "combined_reference_diagnostic_only": request.combined_reference_diagnostic_only,
            "qe_dataset": {
                "data_cutoff": request.data_cutoff,
                "hmm_continuation_cutoff": request.hmm_continuation_cutoff,
                "schema_receipt": dict(schema_receipt),
            },
            "decision_date_start": request.decision_date_start,
            "decision_date_end": request.decision_date_end,
            "repository_commit": request.repository_commit,
            "continuation_cutoff": request.hmm_continuation_cutoff,
            "training_created_at": request.created_at,
            "training_environment": dict(environment_report),
            "retrospective_test_status": training.metrics.get("status", "available"),
            "files": files,
        }
        bundle_id = canonical_json_sha256(manifest_without_id)
        manifest = {"bundle_id": bundle_id, **manifest_without_id}
        _write_json(tmp / "manifest.json", manifest)
        readback = json.loads((tmp / "manifest.json").read_text(encoding="utf-8"))
        if readback != manifest:
            raise AdvisoryModelFirstError(
                "model bundle manifest readback mismatch",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            )
        for filename, descriptor in files.items():
            if _sha256_file(tmp / filename) != descriptor["sha256"]:
                raise AdvisoryModelFirstError(
                    "model bundle file hash changed during publication",
                    reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                    context={"filename": filename},
                )
        target = bundles_root / bundle_id
        if target.exists():
            existing = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
            if existing != manifest:
                raise AdvisoryModelFirstError(
                    "existing bundle identity has different content",
                    reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                    context={"bundle_id": bundle_id},
                )
            for filename, descriptor in files.items():
                existing_file = target / filename
                if not existing_file.is_file() or _sha256_file(existing_file) != descriptor["sha256"]:
                    raise AdvisoryModelFirstError(
                        "existing bundle file is missing or corrupt",
                        reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                        context={"bundle_id": bundle_id, "filename": filename},
                    )
            shutil.rmtree(tmp)
            return bundle_id, target, manifest
        os.replace(tmp, target)
        return bundle_id, target, manifest
    except Exception:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        raise


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _read_and_validate_bundle(
    bundle_path: Path,
    *,
    expected_bundle_id: str,
    expected_manifest_file_sha256: str | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest_path = bundle_path / "manifest.json"
    if expected_manifest_file_sha256 is None or not manifest_path.is_file():
        raise AdvisoryModelFirstError(
            "exact model bundle manifest is missing",
            reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            context={"bundle_id": expected_bundle_id},
        )
    if _sha256_file(manifest_path) != expected_manifest_file_sha256:
        raise AdvisoryModelFirstError(
            "exact model bundle manifest hash differs from its binding",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"bundle_id": expected_bundle_id},
        )
    manifest = _read_json(manifest_path, reason="model bundle manifest cannot be read")
    if manifest.get("schema_version") != "advisory_model_bundle_v1":
        raise AdvisoryModelFirstError(
            "model bundle manifest schema is invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"schema_version": manifest.get("schema_version")},
        )
    actual_bundle_id = canonical_json_sha256(
        {key: value for key, value in manifest.items() if key != "bundle_id"}
    )
    if manifest.get("bundle_id") != expected_bundle_id or actual_bundle_id != expected_bundle_id:
        raise AdvisoryModelFirstError(
            "model bundle content identity is invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"expected_bundle_id": expected_bundle_id, "actual_bundle_id": actual_bundle_id},
        )
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise AdvisoryModelFirstError(
            "model bundle file manifest is empty",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    missing_runtime_files = sorted(_RUNTIME_REQUIRED_BUNDLE_FILES - set(files))
    if missing_runtime_files:
        raise AdvisoryModelFirstError(
            "model bundle manifest omits a runtime-consumed file",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"missing_files": missing_runtime_files},
        )
    for filename, descriptor in files.items():
        if not isinstance(descriptor, dict) or not {"sha256", "size_bytes"}.issubset(descriptor):
            raise AdvisoryModelFirstError(
                "model bundle file descriptor is invalid",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"filename": filename},
            )
        path = _bundle_member_path(bundle_path, str(filename))
        if (
            not path.is_file()
            or path.stat().st_size != int(descriptor["size_bytes"])
            or _sha256_file(path) != str(descriptor["sha256"])
        ):
            raise AdvisoryModelFirstError(
                "model bundle file is missing or corrupt",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"filename": filename},
            )
    feature_schema = _read_json(bundle_path / "feature_schema.json", reason="feature schema cannot be read")
    schema_identity = {key: feature_schema.get(key) for key in FEATURE_SCHEMA_PAYLOAD}
    if (
        manifest.get("feature_schema_hash") != FEATURE_SCHEMA_HASH
        or feature_schema.get("feature_schema_hash") != FEATURE_SCHEMA_HASH
        or canonical_json_sha256(schema_identity) != FEATURE_SCHEMA_HASH
    ):
        raise AdvisoryModelFirstError(
            "model bundle feature schema differs from the runtime schema",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    trained_features = tuple(feature_schema.get("trained_feature_names") or ())
    if trained_features != tuple(FEATURE_SCHEMA_PAYLOAD["model_feature_columns"]):
        raise AdvisoryModelFirstError(
            "model bundle trained feature order is invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    hmm_models = _read_json(bundle_path / "fresh_hmm_models.json", reason="fresh HMM bundle cannot be read")
    baselines = _read_json(bundle_path / "baseline_comparison.json", reason="baseline report cannot be read")
    return manifest, feature_schema, hmm_models, baselines


def _read_json(path: Path, *, reason: str) -> dict[str, Any]:
    if not path.is_file():
        reason_code = (
            "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE"
            if "binding" in reason or "available" in reason
            else "ADVISORY_MODEL_BUNDLE_INVALID"
        )
        raise AdvisoryModelFirstError(
            reason,
            reason_code=reason_code,
            context={"path": str(path)},
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            reason,
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"path": str(path), "error_type": type(exc).__name__},
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            reason,
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"path": str(path), "payload_type": type(payload).__name__},
        )
    return payload


def _load_lightgbm_booster(path: Path) -> Any:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in the inference environment",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    return lgb.Booster(model_file=str(path))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _bundle_member_path(bundle_path: Path, filename: str) -> Path:
    root = bundle_path.resolve()
    relative = Path(filename)
    if relative.is_absolute() or not filename or relative.name != filename:
        raise AdvisoryModelFirstError(
            "model bundle file name escapes the bundle root",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"filename": filename},
        )
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "model bundle file path escapes the bundle root",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            context={"filename": filename},
        ) from exc
    return path
