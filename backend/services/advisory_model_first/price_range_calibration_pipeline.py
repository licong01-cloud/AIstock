from __future__ import annotations

import gc
import importlib.metadata
import json
import os
import platform
import time
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_calibration_pipeline import (
    _peak_rss_bytes,
    _prepare_matrix_from_schema,
    _projection_hash,
)
from backend.services.advisory_model_first.outcome_pipeline import _resolve_wsl_repository_commit
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_bundle import read_price_range_bundle_manifest
from backend.services.advisory_model_first.price_range_calibration import (
    apply_entry_gap_interval_adjustment,
    fit_entry_gap_interval_adjustment,
    interval_metrics,
    monotonic_triplet,
)
from backend.services.advisory_model_first.price_range_calibration_bundle import (
    publish_calibrated_price_range_bundle,
)
from backend.services.advisory_model_first.price_range_calibration_contracts import (
    CALIBRATION_POLICY_VERSION,
    FrozenAdvisoryPriceRangeCalibrationRequestV1,
)

KEYS = ("decision_as_of_trade_date", "target_trade_date", "instrument")


def run_price_range_calibration_pipeline(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = FrozenAdvisoryPriceRangeCalibrationRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    _verify_environment(request)
    run_root = Path(request.output_root).resolve() / "price_range_calibration_runs" / request.request_id
    run_root.mkdir(parents=True, exist_ok=True)
    parent_root = Path(request.parent_bundle_root).resolve()
    parent_manifest = read_price_range_bundle_manifest(
        parent_root, expected_bundle_id=request.parent_price_range_bundle_id
    )
    _validate_request(request, parent_root, parent_manifest)
    features = _read_bound_parquet(request.features_artifact)
    _validate_descriptor(request.price_range_labels_artifact)
    schema = _read_json(parent_root / "feature_schema.json")
    models = _load_parent_quantile_models(parent_root)

    validation = _read_split_labels(request.price_range_labels_artifact.path, "validation")
    validation_merged = _project(features=features, labels=validation, split="validation")
    validation_matrix = _prepare_matrix_from_schema(validation_merged, feature_schema=schema)
    validation_raw = _predict_triplet(models, validation_matrix)
    truth = pd.to_numeric(validation_merged["entry_gap_return"], errors="raise").to_numpy()
    fitted = fit_entry_gap_interval_adjustment(
        split="validation",
        q10=validation_raw[0],
        q50=validation_raw[1],
        q90=validation_raw[2],
        truth=truth,
    )
    raw_crossing = _crossing_count(validation_raw)
    spec = {
        "schema_version": "advisory_price_range_calibration_spec_v1",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "calibration_policy_version": CALIBRATION_POLICY_VERSION,
        "state": fitted["state"],
        "method": fitted["method"],
        "nominal_coverage": fitted["nominal_coverage"],
        "fit_split": fitted["fit_split"],
        "row_count": fitted["row_count"],
        "finite_sample_rank": fitted["finite_sample_rank"],
        "delta": fitted["delta"],
        "validation_projection_hash": _projection_hash(validation_merged.loc[:, list(KEYS)]),
        "validation_raw_quantile_crossing_count": raw_crossing,
        "validation_metrics": fitted["validation_metrics"],
        "entry_executable_calibration_state": "UNCALIBRATED",
        "entry_executable_reason_code": "ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING",
    }
    spec_path = run_root / "calibration_spec.json"
    _write_json_atomic(spec_path, spec)
    frozen_spec = _read_json(spec_path)
    if frozen_spec != spec:
        raise pipeline_error("M5C frozen calibration spec readback differs")

    del validation
    gc.collect()
    # Test labels are intentionally unreachable until the validation spec is durable and read back.
    test = _read_split_labels(request.price_range_labels_artifact.path, "test")
    test_merged = _project(features=features, labels=test, split="test")
    test_matrix = _prepare_matrix_from_schema(test_merged, feature_schema=schema)
    test_raw = _predict_triplet(models, test_matrix)
    test_truth = pd.to_numeric(test_merged["entry_gap_return"], errors="raise").to_numpy()
    validation_metrics, validation_predictions = _evaluate(
        merged=validation_merged,
        raw=validation_raw,
        truth=truth,
        delta=float(frozen_spec["delta"]),
    )
    test_metrics, test_predictions = _evaluate(
        merged=test_merged,
        raw=test_raw,
        truth=test_truth,
        delta=float(frozen_spec["delta"]),
    )
    metrics = {
        "schema_version": "advisory_price_range_calibration_metrics_v1",
        "calibration_spec_sha256": sha256_file(spec_path),
        "validation": validation_metrics,
        "test": test_metrics,
        "activation_recommended": bool(
            test_metrics["calibrated_coverage_absolute_error"]
            < test_metrics["raw_coverage_absolute_error"]
        ),
    }
    log = {
        "schema_version": "advisory_price_range_calibration_log_v1",
        "environment": {
            "conda_environment": request.conda_environment,
            "python_version": platform.python_version(),
            "lightgbm_version": importlib.metadata.version("lightgbm"),
            "numpy_version": importlib.metadata.version("numpy"),
            "pyarrow_version": importlib.metadata.version("pyarrow"),
        },
        "algorithm": {
            "method": request.calibration_method,
            "nominal_coverage": request.nominal_coverage,
        },
    }
    peak_before = _peak_rss_bytes()
    if peak_before > request.resource_max_rss_bytes:
        raise AdvisoryModelFirstError(
            "M5C calibration exceeded the approved RSS limit",
            reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
            context={"peak_rss_bytes": peak_before, "limit_bytes": request.resource_max_rss_bytes},
        )
    bundle_id, bundle_path, manifest = publish_calibrated_price_range_bundle(
        request=request,
        calibration_spec_path=spec_path,
        metrics=metrics,
        calibrated_test_predictions=test_predictions,
        calibration_log=log,
    )
    del models, features, test, validation_matrix, test_matrix, validation_merged, test_merged
    gc.collect()
    final_peak = _peak_rss_bytes()
    if final_peak > request.resource_max_rss_bytes:
        raise AdvisoryModelFirstError(
            "M5C calibration exceeded the approved RSS limit",
            reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
            context={"peak_rss_bytes": final_peak, "limit_bytes": request.resource_max_rss_bytes},
        )
    receipt = {
        "schema_version": "advisory_price_range_calibration_receipt_v1",
        "status": "calibrated",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "parent_price_range_bundle_id": request.parent_price_range_bundle_id,
        "calibration_spec_sha256": sha256_file(spec_path),
        "price_range_bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "metrics": metrics,
        "resource_report": {
            "wall_seconds": round(time.monotonic() - started, 3),
            "peak_rss_bytes": final_peak,
            "limit_bytes": request.resource_max_rss_bytes,
        },
        "price_range_binding_activated": False,
        "activation_recommended": metrics["activation_recommended"],
    }
    _write_json_atomic(run_root / "price_range_calibration_receipt.json", receipt)
    return receipt


def _project(*, features: pd.DataFrame, labels: pd.DataFrame, split: str) -> pd.DataFrame:
    if set(labels["split"].astype(str)) != {split}:
        raise pipeline_error("M5C label projection has the wrong split", split=split)
    if not pd.api.types.is_bool_dtype(labels["gap_modelable"].dtype) or labels[
        "gap_modelable"
    ].isna().any():
        raise pipeline_error("M5C gap_modelable must be an explicit non-null boolean", split=split)
    eligible = labels["gap_modelable"]
    labels = labels.loc[eligible].copy()
    if labels.empty or labels["entry_gap_return"].isna().any():
        raise pipeline_error("M5C executable entry-gap projection is empty or invalid", split=split)
    try:
        merged = features.merge(labels, on=list(KEYS), how="inner", validate="one_to_one")
    except pd.errors.MergeError as exc:
        raise pipeline_error("M5C feature and label identities are not one-to-one", split=split) from exc
    if len(merged) != len(labels) or set(merged["split"].astype(str)) != {split}:
        raise pipeline_error("M5C feature-covered projection lost eligible rows", split=split)
    return merged


def _evaluate(*, merged, raw, truth, delta):
    lower, middle, upper = monotonic_triplet(*raw)
    calibrated = apply_entry_gap_interval_adjustment(q10=raw[0], q50=raw[1], q90=raw[2], delta=delta)
    metrics = interval_metrics(truth, lower, upper, delta=delta)
    metrics.update({
        "date_count": int(merged["decision_as_of_trade_date"].nunique()),
        "raw_quantile_crossing_count": _crossing_count(raw),
        "calibrated_quantile_crossing_count": 0,
    })
    output = merged.loc[:, [*KEYS, "entry_gap_return"]].reset_index(drop=True).copy()
    for name, values in zip(("q10", "q50", "q90"), (lower, middle, upper)):
        output[f"entry_gap_raw_{name}"] = values
    for name, values in zip(("q10", "q50", "q90"), calibrated):
        output[f"entry_gap_calibrated_{name}"] = values
    output["entry_gap_calibration_state"] = "CALIBRATED"
    output["entry_executable_calibration_state"] = "UNCALIBRATED"
    return metrics, output


def _predict_triplet(models: Mapping[str, Any], matrix: pd.DataFrame):
    return tuple(_predict(models[name], matrix, head=name) for name in ("entry_gap_q10", "entry_gap_q50", "entry_gap_q90"))


def _predict(model: Any, matrix: pd.DataFrame, *, head: str) -> np.ndarray:
    try:
        values = np.asarray(model.predict(matrix), dtype=float)
    except Exception as exc:
        raise pipeline_error("M5C parent head prediction failed", head=head) from exc
    if values.shape != (len(matrix),) or not np.isfinite(values).all():
        raise pipeline_error("M5C parent head returned invalid prediction", head=head)
    return values


def _load_parent_quantile_models(root: Path) -> dict[str, Any]:
    try:
        import lightgbm as lgb
        return {name: lgb.Booster(model_file=str(root / "models" / f"{name}.txt"))
                for name in ("entry_gap_q10", "entry_gap_q50", "entry_gap_q90")}
    except Exception as exc:
        raise pipeline_error("M5C parent quantile model cannot be loaded", error_type=type(exc).__name__) from exc


def _validate_request(request, root: Path, manifest: Mapping[str, Any]) -> None:
    expected = {
        "schema_version": "advisory_price_range_bundle_v1", "calibration_state": "UNCALIBRATED",
        "request_id": request.parent_price_range_request_id, "request_sha256": request.parent_price_range_request_sha256,
        "package_id": request.package_id, "manifest_sha256": request.manifest_sha256,
        "style_profile_id": request.style_profile_id, "style_profile_hash": request.style_profile_hash,
        "feature_schema_version": request.feature_schema_version, "feature_schema_hash": request.feature_schema_hash,
        "label_policy_version": request.label_policy_version,
    }
    if ({key: manifest.get(key) for key in expected} != expected
            or sha256_file(root / "manifest.json") != request.parent_price_range_manifest_file_sha256
            or sha256_file(root / "split.json") != request.split_sha256):
        raise AdvisoryModelFirstError("M5C request differs from parent M4 bundle", reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_PARENT_MISMATCH")
    parent_request = _read_json(root / "training_request.json")
    frozen_features = parent_request.get("features_artifact")
    if not isinstance(frozen_features, dict) or frozen_features != request.features_artifact.model_dump(
        mode="json"
    ):
        raise AdvisoryModelFirstError(
            "M5C features differ from the parent M4 frozen request",
            reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_PARENT_MISMATCH",
        )
    expected_label_path = (
        Path(request.output_root)
        / "price_range_runs"
        / request.parent_price_range_request_id
        / "price_range_labels.parquet"
    )
    if Path(request.price_range_labels_artifact.path) != expected_label_path:
        raise AdvisoryModelFirstError(
            "M5C labels do not come from the exact parent M4 run",
            reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_PARENT_MISMATCH",
        )
    _validate_descriptor(request.features_artifact)
    _validate_descriptor(request.price_range_labels_artifact)


def _read_bound_parquet(descriptor) -> pd.DataFrame:
    _validate_descriptor(descriptor)
    return pd.read_parquet(descriptor.path)


def _validate_descriptor(descriptor) -> None:
    path = Path(descriptor.path)
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise pipeline_error("M5C bound parquet cannot be read", path=str(path)) from exc
    if (not path.is_file() or path.stat().st_size != descriptor.size_bytes
            or sha256_file(path) != descriptor.sha256 or parquet.metadata.num_rows != descriptor.row_count
            or tuple(parquet.schema_arrow.names) != descriptor.columns):
        raise pipeline_error("M5C bound parquet descriptor mismatch", path=str(path))


def _read_split_labels(path: str, split: str) -> pd.DataFrame:
    value = pd.read_parquet(path, filters=[("split", "==", split)])
    if value.empty or set(value["split"].astype(str)) != {split}:
        raise pipeline_error("M5C split projection is empty or contaminated", split=split)
    return value


def _verify_environment(request) -> None:
    release = platform.release().lower()
    if (
        platform.system() != "Linux"
        or "microsoft" not in release
        or os.environ.get("CONDA_DEFAULT_ENV") != request.conda_environment
    ):
        raise AdvisoryModelFirstError(
            "M5C calibration requires WSL rdagent-gpu",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={
                "platform": platform.system(),
                "platform_release": platform.release(),
                "conda_environment": os.environ.get("CONDA_DEFAULT_ENV"),
            },
        )
    root = Path(request.repository_root).resolve()
    if not root.is_dir() or not Path(request.output_root).is_dir():
        raise pipeline_error(
            "M5C explicit repository or artifact root does not exist",
            repository_root_exists=root.is_dir(),
            output_root_exists=Path(request.output_root).is_dir(),
        )
    if _resolve_wsl_repository_commit(root) != request.repository_commit:
        raise pipeline_error("M5C repository commit differs from request")


def _crossing_count(raw) -> int:
    stack = np.column_stack(raw)
    return int(((stack[:, 0] > stack[:, 1]) | (stack[:, 1] > stack[:, 2])).sum())


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise pipeline_error("M5C JSON cannot be read", path=str(path)) from exc
    if not isinstance(value, dict):
        raise pipeline_error("M5C JSON is not an object", path=str(path))
    return value


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False), encoding="utf-8")
    os.replace(temporary, path)


def pipeline_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_PROJECTION_INVALID", context=context)
