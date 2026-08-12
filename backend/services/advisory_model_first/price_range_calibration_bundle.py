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
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_bundle import read_price_range_bundle_manifest
from backend.services.advisory_model_first.price_range_calibration_contracts import (
    CALIBRATION_METHOD,
    CALIBRATION_POLICY_VERSION,
    FrozenAdvisoryPriceRangeCalibrationRequestV1,
)
from backend.services.advisory_model_first.price_range_contracts import (
    PRICE_RANGE_MODEL_NAMES,
    canonical_json_sha256,
)


def publish_calibrated_price_range_bundle(
    *,
    request: FrozenAdvisoryPriceRangeCalibrationRequestV1,
    calibration_spec_path: str | Path,
    metrics: Mapping[str, Any],
    calibrated_test_predictions: pd.DataFrame,
    calibration_log: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    parent_root = Path(request.parent_bundle_root).resolve()
    parent = read_price_range_bundle_manifest(
        parent_root, expected_bundle_id=request.parent_price_range_bundle_id
    )
    _validate_parent(request, parent_root, parent)
    spec_path = Path(calibration_spec_path).resolve()
    spec = _read_json(spec_path)
    _validate_spec(spec, request=request)
    bundles_root = Path(request.output_root).resolve() / "price_range_bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".price-range-calibration-bundle-", dir=bundles_root))
    try:
        (temporary / "models").mkdir()
        for name in PRICE_RANGE_MODEL_NAMES:
            source = parent_root / "models" / f"{name}.txt"
            _copy_exact(source, temporary / "models" / source.name)
        for name in (
            "training_request.json",
            "feature_schema.json",
            "label_policy.json",
            "split.json",
            "test_predictions.parquet",
        ):
            _copy_exact(parent_root / name, temporary / name)
        _copy_exact(spec_path, temporary / "calibration_spec.json")
        _write_json(temporary / "calibration_request.json", request.model_dump(mode="json"))
        _write_json(temporary / "metrics.json", dict(metrics))
        _write_json(temporary / "training_log.json", dict(calibration_log))
        calibrated_test_predictions.to_parquet(
            temporary / "calibrated_test_predictions.parquet", index=False
        )
        files = _descriptors(temporary)
        payload = {
            **{key: parent[key] for key in (
                "status", "parent_request_id", "parent_request_sha256", "parent_bundle_id",
                "outcome_request_id", "outcome_request_sha256", "outcome_bundle_id", "package_id",
                "manifest_sha256", "style_profile_id", "style_profile_hash", "feature_schema_version",
                "feature_schema_hash", "label_policy_version", "entry_gap_condition", "quantiles",
                "model_names", "model_count",
            )},
            "schema_version": "advisory_price_range_bundle_v2",
            "calibration_state": "CALIBRATED_INTERVAL",
            "entry_gap_calibration_state": "CALIBRATED",
            "entry_executable_calibration_state": "UNCALIBRATED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "parent_price_range_bundle_id": request.parent_price_range_bundle_id,
            "parent_price_range_request_id": request.parent_price_range_request_id,
            "parent_price_range_request_sha256": request.parent_price_range_request_sha256,
            "parent_price_range_manifest_file_sha256": (
                request.parent_price_range_manifest_file_sha256
            ),
            "calibration_policy_version": CALIBRATION_POLICY_VERSION,
            "calibration_method": CALIBRATION_METHOD,
            "calibration_spec_sha256": sha256_file(temporary / "calibration_spec.json"),
            "repository_commit": request.repository_commit,
            "files": files,
        }
        bundle_id = canonical_json_sha256(payload)
        manifest = {**payload, "price_range_bundle_id": bundle_id}
        _write_json(temporary / "manifest.json", manifest)
        validate_calibrated_price_range_bundle(temporary, expected_bundle_id=bundle_id)
        target = bundles_root / bundle_id
        if target.exists():
            existing = validate_calibrated_price_range_bundle(target, expected_bundle_id=bundle_id)
            if existing != manifest:
                raise bundle_error("existing M5C bundle identity has different content")
            shutil.rmtree(temporary)
            return bundle_id, target, existing
        os.replace(temporary, target)
        readback = validate_calibrated_price_range_bundle(target, expected_bundle_id=bundle_id)
        if readback != manifest:
            raise bundle_error("M5C bundle readback differs after publication")
        return bundle_id, target, manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def validate_calibrated_price_range_bundle(
    bundle_path: str | Path, *, expected_bundle_id: str
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    manifest = _read_json(root / "manifest.json")
    payload = {key: value for key, value in manifest.items() if key != "price_range_bundle_id"}
    if (
        manifest.get("price_range_bundle_id") != expected_bundle_id
        or canonical_json_sha256(payload) != expected_bundle_id
        or manifest.get("schema_version") != "advisory_price_range_bundle_v2"
        or manifest.get("calibration_state") != "CALIBRATED_INTERVAL"
        or manifest.get("entry_gap_calibration_state") != "CALIBRATED"
        or manifest.get("entry_executable_calibration_state") != "UNCALIBRATED"
        or manifest.get("calibration_method") != CALIBRATION_METHOD
    ):
        raise bundle_error("M5C manifest identity or semantics are invalid")
    files = manifest.get("files")
    required = {
        "training_request.json", "calibration_request.json", "calibration_spec.json",
        "feature_schema.json", "label_policy.json", "split.json", "metrics.json",
        "training_log.json", "test_predictions.parquet", "calibrated_test_predictions.parquet",
        *{f"models/{name}.txt" for name in PRICE_RANGE_MODEL_NAMES},
    }
    if not isinstance(files, dict) or set(files) != required:
        raise bundle_error("M5C bundle member set differs from contract")
    for name, descriptor in files.items():
        path = _member(root, name)
        if (
            not isinstance(descriptor, dict)
            or not path.is_file()
            or descriptor.get("size_bytes") != path.stat().st_size
            or descriptor.get("sha256") != sha256_file(path)
        ):
            raise bundle_error("M5C member is missing or corrupt", filename=name)
    request = FrozenAdvisoryPriceRangeCalibrationRequestV1.model_validate_json(
        (root / "calibration_request.json").read_text(encoding="utf-8")
    )
    if (
        request.request_id != manifest.get("request_id")
        or request.request_sha256 != manifest.get("request_sha256")
        or request.parent_price_range_bundle_id != manifest.get("parent_price_range_bundle_id")
        or request.parent_price_range_request_id
        != manifest.get("parent_price_range_request_id")
        or request.parent_price_range_request_sha256
        != manifest.get("parent_price_range_request_sha256")
        or request.parent_price_range_manifest_file_sha256
        != manifest.get("parent_price_range_manifest_file_sha256")
        or sha256_file(root / "calibration_spec.json") != manifest.get("calibration_spec_sha256")
    ):
        raise bundle_error("M5C request/spec identities differ from manifest")
    _validate_spec(_read_json(root / "calibration_spec.json"), request=request)
    predictions = pd.read_parquet(root / "calibrated_test_predictions.parquet")
    required_columns = {
        "decision_as_of_trade_date", "target_trade_date", "instrument", "entry_gap_return",
        "entry_gap_raw_q10", "entry_gap_raw_q50", "entry_gap_raw_q90",
        "entry_gap_calibrated_q10", "entry_gap_calibrated_q50", "entry_gap_calibrated_q90",
        "entry_gap_calibration_state", "entry_executable_calibration_state",
    }
    if required_columns - set(predictions) or predictions.empty:
        raise bundle_error("M5C calibrated test predictions are incomplete")
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    if predictions[keys].isna().any().any() or predictions.duplicated(keys).any():
        raise bundle_error("M5C calibrated test prediction identities are invalid")
    if set(predictions["entry_gap_calibration_state"].astype(str)) != {"CALIBRATED"} or set(
        predictions["entry_executable_calibration_state"].astype(str)
    ) != {"UNCALIBRATED"}:
        raise bundle_error("M5C calibrated test prediction states are invalid")
    numeric_columns = sorted(
        required_columns
        - {
            *keys,
            "entry_gap_calibration_state",
            "entry_executable_calibration_state",
        }
    )
    numeric = predictions[numeric_columns]
    if not all(math.isfinite(float(value)) for value in numeric.to_numpy().ravel()):
        raise bundle_error("M5C calibrated test predictions contain nonfinite values")
    spec = _read_json(root / "calibration_spec.json")
    delta = float(spec["delta"])
    raw_q10 = predictions["entry_gap_raw_q10"].astype(float)
    raw_q50 = predictions["entry_gap_raw_q50"].astype(float)
    raw_q90 = predictions["entry_gap_raw_q90"].astype(float)
    calibrated_q10 = predictions["entry_gap_calibrated_q10"].astype(float)
    calibrated_q50 = predictions["entry_gap_calibrated_q50"].astype(float)
    calibrated_q90 = predictions["entry_gap_calibrated_q90"].astype(float)
    if not (
        (raw_q10 <= raw_q50).all()
        and (raw_q50 <= raw_q90).all()
        and (calibrated_q10 <= calibrated_q50).all()
        and (calibrated_q50 <= calibrated_q90).all()
        and (calibrated_q50 == raw_q50).all()
        and ((raw_q10 - calibrated_q10 - delta).abs() <= 1e-12).all()
        and ((calibrated_q90 - raw_q90 - delta).abs() <= 1e-12).all()
    ):
        raise bundle_error("M5C calibrated test prediction formula is invalid")
    metrics = _read_json(root / "metrics.json")
    test_metrics = metrics.get("test") if isinstance(metrics, dict) else None
    if not isinstance(test_metrics, dict) or test_metrics.get("row_count") != len(predictions):
        raise bundle_error("M5C calibrated test metrics do not close over predictions")
    return manifest


def _validate_parent(request, root: Path, manifest: Mapping[str, Any]) -> None:
    expected = {
        "schema_version": "advisory_price_range_bundle_v1",
        "calibration_state": "UNCALIBRATED",
        "price_range_bundle_id": request.parent_price_range_bundle_id,
        "request_id": request.parent_price_range_request_id,
        "request_sha256": request.parent_price_range_request_sha256,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "style_profile_id": request.style_profile_id,
        "style_profile_hash": request.style_profile_hash,
        "feature_schema_version": request.feature_schema_version,
        "feature_schema_hash": request.feature_schema_hash,
        "label_policy_version": request.label_policy_version,
    }
    if (
        {key: manifest.get(key) for key in expected} != expected
        or sha256_file(root / "manifest.json") != request.parent_price_range_manifest_file_sha256
        or sha256_file(root / "split.json") != request.split_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5C parent price-range identity mismatch",
            reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_PARENT_MISMATCH",
        )


def _validate_spec(spec: Mapping[str, Any], *, request) -> None:
    delta = spec.get("delta")
    row_count = spec.get("row_count")
    rank = spec.get("finite_sample_rank")
    validation_metrics = spec.get("validation_metrics")
    validation_coverage = spec.get("validation_feature_coverage")
    if (
        spec.get("schema_version") != "advisory_price_range_calibration_spec_v1"
        or spec.get("request_id") != request.request_id
        or spec.get("request_sha256") != request.request_sha256
        or spec.get("calibration_policy_version") != CALIBRATION_POLICY_VERSION
        or spec.get("method") != CALIBRATION_METHOD
        or spec.get("state") != "CALIBRATED"
        or spec.get("nominal_coverage") != 0.8
        or spec.get("fit_split") != "validation"
        or not isinstance(row_count, int)
        or row_count <= 0
        or not isinstance(rank, int)
        or rank != min(math.ceil((row_count + 1) * 0.8), row_count)
        or not isinstance(spec.get("validation_projection_hash"), str)
        or len(spec["validation_projection_hash"]) != 64
        or not isinstance(spec.get("validation_raw_quantile_crossing_count"), int)
        or spec["validation_raw_quantile_crossing_count"] < 0
        or not isinstance(validation_metrics, dict)
        or validation_metrics.get("row_count") != row_count
        or not isinstance(validation_coverage, dict)
        or validation_coverage.get("feature_covered_row_count") != row_count
        or not isinstance(validation_coverage.get("eligible_row_count"), int)
        or not isinstance(validation_coverage.get("feature_unavailable_row_count"), int)
        or validation_coverage["eligible_row_count"]
        != row_count + validation_coverage["feature_unavailable_row_count"]
        or not isinstance(delta, (int, float))
        or not math.isfinite(float(delta))
        or float(delta) < 0
        or spec.get("entry_executable_calibration_state") != "UNCALIBRATED"
        or spec.get("entry_executable_reason_code")
        != "ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING"
    ):
        raise bundle_error("M5C calibration spec is invalid")


def _copy_exact(source: Path, target: Path) -> None:
    if not source.is_file():
        raise bundle_error("M5C source member is missing", path=str(source))
    shutil.copyfile(source, target)
    if sha256_file(source) != sha256_file(target):
        raise bundle_error("M5C copied member differs", path=str(source))


def _descriptors(root: Path) -> dict[str, dict[str, Any]]:
    return {path.relative_to(root).as_posix(): {"sha256": sha256_file(path), "size_bytes": path.stat().st_size}
            for path in sorted(root.rglob("*")) if path.is_file() and path.name != "manifest.json"}


def _member(root: Path, name: str) -> Path:
    path = (root / name).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise bundle_error("M5C member escapes bundle root", filename=name) from exc
    return path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise bundle_error("M5C JSON member cannot be read", path=str(path)) from exc
    if not isinstance(value, dict):
        raise bundle_error("M5C JSON member is not an object", path=str(path))
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False), encoding="utf-8")


def bundle_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_BUNDLE_INVALID", context=context)
