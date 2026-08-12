from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_bundle import read_outcome_bundle_manifest
from backend.services.advisory_model_first.outcome_calibration_contracts import (
    CALIBRATION_POLICY_VERSION,
    FrozenAdvisoryOutcomeCalibrationRequestV1,
    expected_binary_calibration_heads,
)
from backend.services.advisory_model_first.outcome_contracts import (
    OUTCOME_HORIZONS,
    OUTCOME_QUANTILES,
    canonical_json_sha256,
)
from backend.services.advisory_model_first.prediction_source import sha256_file


def _expected_model_names() -> tuple[str, ...]:
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


def publish_calibrated_outcome_bundle(
    *,
    request: FrozenAdvisoryOutcomeCalibrationRequestV1,
    calibration_spec_path: str | Path,
    metrics: Mapping[str, Any],
    validation_predictions: pd.DataFrame,
    test_predictions: pd.DataFrame,
    calibration_log: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    parent_root = Path(request.parent_bundle_root).resolve()
    parent_manifest = read_outcome_bundle_manifest(
        parent_root,
        expected_bundle_id=request.parent_outcome_bundle_id,
    )
    _validate_parent_identity(request, parent_root, parent_manifest)
    spec_path = Path(calibration_spec_path).resolve()
    calibration = _read_json(spec_path)
    _validate_calibration_spec(calibration, request=request)

    output_root = Path(request.output_root).resolve()
    bundles_root = output_root / "outcome_bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".outcome-calibration-bundle-", dir=bundles_root))
    try:
        models_root = temporary / "models"
        models_root.mkdir()
        for model_name in _expected_model_names():
            source = parent_root / "models" / f"{model_name}.txt"
            descriptor = (parent_manifest.get("files") or {}).get(f"models/{model_name}.txt") or {}
            if (
                not source.is_file()
                or descriptor.get("sha256") != sha256_file(source)
                or descriptor.get("size_bytes") != source.stat().st_size
            ):
                raise _bundle_error(
                    "parent M3 model differs from its frozen manifest",
                    model_name=model_name,
                )
            shutil.copyfile(source, models_root / source.name)
        _copy_exact(parent_root / "training_request.json", temporary / "parent_training_request.json")
        for name in ("feature_schema.json", "label_policy.json", "split.json"):
            _copy_exact(parent_root / name, temporary / name)
        _copy_exact(spec_path, temporary / "calibration.json")
        _write_json(temporary / "calibration_request.json", request.model_dump(mode="json"))
        _write_json(temporary / "metrics.json", dict(metrics))
        _write_json(temporary / "calibration_log.json", dict(calibration_log))
        validation_predictions.to_parquet(temporary / "validation_predictions.parquet", index=False)
        test_predictions.to_parquet(temporary / "test_predictions.parquet", index=False)

        files = _file_descriptors(temporary)
        binary_states = {
            str(value.get("state"))
            for value in (calibration.get("binary_heads") or {}).values()
        }
        binary_state = "CALIBRATED" if binary_states == {"CALIBRATED"} else "PARTIAL"
        manifest_payload = {
            "schema_version": "advisory_outcome_bundle_v2",
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "PARTIAL",
            "binary_calibration_state": binary_state,
            "return_interval_calibration_state": "CALIBRATED",
            "path_upper_calibration_state": "CALIBRATED",
            "holding_calibration_state": "UNCALIBRATED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "parent_outcome_bundle_id": request.parent_outcome_bundle_id,
            "parent_outcome_request_id": request.parent_outcome_request_id,
            "parent_bundle_id": parent_manifest["parent_bundle_id"],
            "parent_request_id": parent_manifest["parent_request_id"],
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "style_profile_id": request.style_profile_id,
            "style_profile_hash": request.style_profile_hash,
            "feature_schema_version": request.feature_schema_version,
            "feature_schema_hash": request.feature_schema_hash,
            "label_policy_version": request.label_policy_version,
            "calibration_policy_version": CALIBRATION_POLICY_VERSION,
            "calibration_spec_sha256": sha256_file(temporary / "calibration.json"),
            "horizons": list(parent_manifest["horizons"]),
            "quantiles": list(parent_manifest["quantiles"]),
            "repository_commit": request.repository_commit,
            "model_count": len(_expected_model_names()),
            "files": files,
        }
        bundle_id = canonical_json_sha256(manifest_payload)
        manifest = {**manifest_payload, "outcome_bundle_id": bundle_id}
        _write_json(temporary / "manifest.json", manifest)
        target = bundles_root / bundle_id
        if target.exists():
            existing = read_outcome_bundle_manifest(target, expected_bundle_id=bundle_id)
            if existing != manifest:
                raise _bundle_error("existing M5B bundle identity has different content")
            shutil.rmtree(temporary)
            return bundle_id, target, existing
        os.replace(temporary, target)
        readback = read_outcome_bundle_manifest(target, expected_bundle_id=bundle_id)
        if readback != manifest:
            raise _bundle_error("M5B bundle readback differs after publication")
        return bundle_id, target, manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def _validate_parent_identity(
    request: FrozenAdvisoryOutcomeCalibrationRequestV1,
    parent_root: Path,
    parent_manifest: Mapping[str, Any],
) -> None:
    manifest_path = parent_root / "manifest.json"
    split_path = parent_root / "split.json"
    expected = {
        "schema_version": "advisory_outcome_bundle_v1",
        "calibration_state": "UNCALIBRATED",
        "outcome_bundle_id": request.parent_outcome_bundle_id,
        "request_id": request.parent_outcome_request_id,
        "request_sha256": request.parent_outcome_request_sha256,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "style_profile_id": request.style_profile_id,
        "style_profile_hash": request.style_profile_hash,
        "feature_schema_version": request.feature_schema_version,
        "feature_schema_hash": request.feature_schema_hash,
        "label_policy_version": request.label_policy_version,
    }
    if (
        {key: parent_manifest.get(key) for key in expected} != expected
        or sha256_file(manifest_path) != request.parent_outcome_manifest_file_sha256
        or sha256_file(split_path) != request.split_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5B parent outcome bundle identity mismatch",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_PARENT_MISMATCH",
        )


def _validate_calibration_spec(
    calibration: Mapping[str, Any],
    *,
    request: FrozenAdvisoryOutcomeCalibrationRequestV1,
) -> None:
    binary_heads = calibration.get("binary_heads")
    expected_heads = set(expected_binary_calibration_heads())
    if (
        calibration.get("schema_version") != "advisory_outcome_calibration_spec_v1"
        or calibration.get("request_id") != request.request_id
        or calibration.get("request_sha256") != request.request_sha256
        or calibration.get("calibration_policy_version") != CALIBRATION_POLICY_VERSION
        or not isinstance(binary_heads, dict)
        or set(binary_heads) != expected_heads
        or set(calibration.get("return_intervals") or {})
        != {f"excess_return_h{value}" for value in (1, 3, 5, 10, 20)}
        or set(calibration.get("path_upper") or {})
        != {
            f"{family}_h{value}"
            for family in ("path_mfe", "path_mae_loss")
            for value in (1, 3, 5, 10, 20)
        }
        or calibration.get("holding_calibration_state") != "UNCALIBRATED"
        or not _is_sha256(calibration.get("validation_projection_hash"))
    ):
        raise _bundle_error("M5B calibration spec is incomplete or inconsistent")
    for head, value in binary_heads.items():
        if not isinstance(value, dict) or value.get("state") not in {"CALIBRATED", "UNCALIBRATED"}:
            raise _bundle_error("M5B binary calibration state is invalid", head=head)
        if value["state"] == "CALIBRATED" and not all(
            _is_finite_number(value.get(field)) for field in ("coefficient", "intercept")
        ):
            raise _bundle_error("M5B calibrated head omits Platt parameters", head=head)
        if value["state"] == "UNCALIBRATED" and (
            value.get("coefficient") is not None
            or value.get("intercept") is not None
            or value.get("reason_code")
            != "ADVISORY_OUTCOME_CALIBRATION_CLASS_VARIATION_MISSING"
        ):
            raise _bundle_error("M5B uncalibrated head has invalid fallback parameters", head=head)
    for family_name, expected_method, expected_coverage in (
        ("return_intervals", "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION", 0.8),
        ("path_upper", "CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION", 0.9),
    ):
        for head, value in (calibration.get(family_name) or {}).items():
            if (
                not isinstance(value, dict)
                or value.get("state") != "CALIBRATED"
                or value.get("method") != expected_method
                or value.get("nominal_coverage") != expected_coverage
                or not _is_finite_number(value.get("delta"))
                or float(value["delta"]) < 0.0
            ):
                raise _bundle_error("M5B quantile calibration spec is invalid", head=head)


def _copy_exact(source: Path, target: Path) -> None:
    if not source.is_file():
        raise _bundle_error("M5B source member is missing", path=str(source))
    shutil.copyfile(source, target)
    if sha256_file(source) != sha256_file(target):
        raise _bundle_error("M5B copied member hash differs", path=str(source))


def _file_descriptors(root: Path) -> dict[str, dict[str, Any]]:
    return {
        path.relative_to(root).as_posix(): {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _bundle_error("M5B JSON member cannot be read", path=str(path)) from exc
    if not isinstance(value, dict):
        raise _bundle_error("M5B JSON member is not an object", path=str(path))
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def _bundle_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_OUTCOME_CALIBRATION_BUNDLE_INVALID",
        context=context,
    )


def _is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )
