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
from sklearn.metrics import mean_pinball_loss

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_bundle import read_outcome_bundle_manifest
from backend.services.advisory_model_first.outcome_calibration import (
    apply_path_upper_adjustment,
    apply_platt_calibrator,
    apply_return_interval_adjustment,
    binary_metrics,
    fit_path_upper_adjustment,
    fit_platt_calibrator,
    fit_return_interval_adjustment,
)
from backend.services.advisory_model_first.outcome_calibration_bundle import (
    publish_calibrated_outcome_bundle,
)
from backend.services.advisory_model_first.outcome_calibration_contracts import (
    CALIBRATION_POLICY_VERSION,
    FrozenAdvisoryOutcomeCalibrationRequestV1,
)
from backend.services.advisory_model_first.outcome_contracts import OUTCOME_HORIZONS
from backend.services.advisory_model_first.outcome_pipeline import _resolve_wsl_repository_commit
from backend.services.advisory_model_first.prediction_source import sha256_file

KEYS = ("decision_as_of_trade_date", "target_trade_date", "instrument")


def run_outcome_calibration_pipeline(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = FrozenAdvisoryOutcomeCalibrationRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    _verify_environment(request)
    run_root = Path(request.output_root).resolve() / "outcome_calibration_runs" / request.request_id
    run_root.mkdir(parents=True, exist_ok=True)
    parent_root = Path(request.parent_bundle_root).resolve()
    parent_manifest = read_outcome_bundle_manifest(
        parent_root,
        expected_bundle_id=request.parent_outcome_bundle_id,
    )
    _validate_request_artifacts(request, parent_root, parent_manifest)

    features = _read_bound_parquet(request.features_artifact)
    _validate_parquet_descriptor(request.outcome_labels_artifact)
    feature_schema = _read_json(parent_root / "feature_schema.json")
    validation_labels = pd.read_parquet(
        request.outcome_labels_artifact.path,
        filters=[("split", "==", "validation")],
    )
    if validation_labels.empty or set(validation_labels["split"].astype(str)) != {"validation"}:
        raise _pipeline_error("M5B validation label projection is empty or contaminated")
    validation_merged, validation_projection_counts = _feature_covered_projection(
        features=features,
        labels=validation_labels,
        projection_name="validation",
    )
    validation_matrix = _prepare_matrix_from_schema(validation_merged, feature_schema=feature_schema)
    models = _load_parent_models(parent_root)

    validation_spec, validation_predictions = _fit_validation(
        request=request,
        models=models,
        matrix=validation_matrix,
        merged=validation_merged,
    )
    spec_path = run_root / "calibration.json"
    _write_json_atomic(spec_path, validation_spec)
    frozen_spec = _read_json(spec_path)
    if frozen_spec != validation_spec:
        raise _pipeline_error("M5B frozen calibration spec readback differs")
    spec_sha256 = sha256_file(spec_path)

    del validation_labels
    gc.collect()
    # Test labels become reachable only after the validation spec is durable and read back.
    test_labels = pd.read_parquet(
        request.outcome_labels_artifact.path,
        filters=[("split", "==", "test")],
    )
    if test_labels.empty or set(test_labels["split"].astype(str)) != {"test"}:
        raise _pipeline_error("M5B test label projection is empty or contaminated")
    test_merged, test_projection_counts = _feature_covered_projection(
        features=features,
        labels=test_labels,
        projection_name="test",
    )
    test_matrix = _prepare_matrix_from_schema(test_merged, feature_schema=feature_schema)
    test_metrics, test_predictions = _evaluate_projection(
        models=models,
        matrix=test_matrix,
        merged=test_merged,
        calibration_spec=frozen_spec,
        projection_name="test",
    )
    validation_metrics, _ = _evaluate_projection(
        models=models,
        matrix=validation_matrix,
        merged=validation_merged,
        calibration_spec=frozen_spec,
        projection_name="validation",
    )
    metrics = {
        "schema_version": "advisory_outcome_calibration_metrics_v1",
        "calibration_spec_sha256": spec_sha256,
        "projection_counts": {
            "validation": validation_projection_counts,
            "test": test_projection_counts,
        },
        "validation": validation_metrics,
        "test": test_metrics,
    }
    calibration_log = {
        "schema_version": "advisory_outcome_calibration_log_v1",
        "algorithm": {
            "binary": request.binary_method,
            "return_interval": request.return_interval_method,
            "path_upper": request.path_upper_method,
            "ece_bin_count": request.ece_bin_count,
        },
        "environment": {
            "conda_environment": request.conda_environment,
            "python_version": platform.python_version(),
            "lightgbm_version": importlib.metadata.version("lightgbm"),
            "scikit_learn_version": importlib.metadata.version("scikit-learn"),
            "numpy_version": importlib.metadata.version("numpy"),
        },
    }
    peak_rss_before_publish = _peak_rss_bytes()
    if peak_rss_before_publish > request.resource_max_rss_bytes:
        raise AdvisoryModelFirstError(
            "M5B calibration exceeded the approved RSS limit before publication",
            reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
            context={
                "peak_rss_bytes": peak_rss_before_publish,
                "limit_bytes": request.resource_max_rss_bytes,
            },
        )
    bundle_id, bundle_path, manifest = publish_calibrated_outcome_bundle(
        request=request,
        calibration_spec_path=spec_path,
        metrics=metrics,
        validation_predictions=validation_predictions,
        test_predictions=test_predictions,
        calibration_log=calibration_log,
    )
    del models, validation_matrix, validation_merged, test_matrix, test_merged, features, test_labels
    gc.collect()
    peak_rss = _peak_rss_bytes()
    receipt = {
        "schema_version": "advisory_outcome_calibration_receipt_v1",
        "status": "calibrated",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "parent_outcome_bundle_id": request.parent_outcome_bundle_id,
        "calibration_spec_sha256": spec_sha256,
        "outcome_bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "metrics": metrics,
        "resource_report": {
            "wall_seconds": round(time.monotonic() - started, 3),
            "peak_rss_bytes": peak_rss,
            "limit_bytes": request.resource_max_rss_bytes,
        },
        "outcome_binding_activated": False,
    }
    _write_json_atomic(run_root / "outcome_calibration_receipt.json", receipt)
    return receipt


def _feature_covered_projection(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    projection_name: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    try:
        merged = features.merge(labels, on=list(KEYS), how="inner", validate="one_to_one")
    except pd.errors.MergeError as exc:
        raise _pipeline_error(
            f"M5B {projection_name} feature and label identities are not one-to-one",
            error_type=type(exc).__name__,
        ) from exc
    if merged.empty:
        raise _pipeline_error(
            f"M5B {projection_name} feature and label projection has no common rows"
        )
    if set(merged["split"].astype(str)) != {projection_name}:
        raise _pipeline_error(
            f"M5B {projection_name} feature-covered projection has the wrong split"
        )
    label_row_count = len(labels)
    feature_covered_row_count = len(merged)
    return merged, {
        "label_row_count": label_row_count,
        "feature_covered_row_count": feature_covered_row_count,
        "missing_feature_row_count": label_row_count - feature_covered_row_count,
    }


def _fit_validation(
    *,
    request: FrozenAdvisoryOutcomeCalibrationRequestV1,
    models: Mapping[str, Any],
    matrix: pd.DataFrame,
    merged: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    if merged.empty or set(merged["split"].astype(str)) != {"validation"}:
        raise _pipeline_error("M5B fit projection is not validation-only")
    spec: dict[str, Any] = {
        "schema_version": "advisory_outcome_calibration_spec_v1",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "calibration_policy_version": CALIBRATION_POLICY_VERSION,
        "validation_projection_hash": _projection_hash(merged.loc[:, list(KEYS)]),
        "binary_heads": {},
        "return_intervals": {},
        "path_upper": {},
        "holding_calibration_state": "UNCALIBRATED",
    }
    for horizon in OUTCOME_HORIZONS:
        eligible = merged[f"modelable_{horizon}"].astype(bool)
        if not eligible.any():
            raise _pipeline_error("M5B validation horizon has no eligible rows", horizon=horizon)
        local_matrix = matrix.loc[eligible]
        for family in ("positive_excess", "signal_survival"):
            head = f"{family}_h{horizon}"
            raw_margin = _predict(models[head], local_matrix, head=head, raw_score=True)
            raw_probability = _predict(models[head], local_matrix, head=head)
            truth = pd.to_numeric(merged.loc[eligible, f"{family}_{horizon}"], errors="raise").to_numpy()
            spec["binary_heads"][head] = fit_platt_calibrator(
                head=head,
                raw_margin=raw_margin,
                raw_probability=raw_probability,
                truth=truth,
            ).as_dict()
        raw_return = {
            quantile: _predict(
                models[f"excess_return_h{horizon}_q{quantile}"],
                local_matrix,
                head=f"excess_return_h{horizon}_q{quantile}",
            )
            for quantile in (10, 50, 90)
        }
        spec["return_intervals"][f"excess_return_h{horizon}"] = fit_return_interval_adjustment(
            q10=raw_return[10],
            q50=raw_return[50],
            q90=raw_return[90],
            truth=pd.to_numeric(merged.loc[eligible, f"excess_return_{horizon}"], errors="raise").to_numpy(),
        )
        for family in ("path_mfe", "path_mae_loss"):
            raw_q50 = _predict(models[f"{family}_h{horizon}_q50"], local_matrix, head=f"{family}_h{horizon}_q50")
            raw_q90 = _predict(models[f"{family}_h{horizon}_q90"], local_matrix, head=f"{family}_h{horizon}_q90")
            spec["path_upper"][f"{family}_h{horizon}"] = fit_path_upper_adjustment(
                q50=raw_q50,
                q90=raw_q90,
                truth=pd.to_numeric(merged.loc[eligible, f"{family}_{horizon}"], errors="raise").to_numpy(),
            )
    _metrics, output = _evaluate_projection(
        models=models,
        matrix=matrix,
        merged=merged,
        calibration_spec=spec,
        projection_name="validation",
    )
    return spec, output


def _evaluate_projection(
    *,
    models: Mapping[str, Any],
    matrix: pd.DataFrame,
    merged: pd.DataFrame,
    calibration_spec: Mapping[str, Any],
    projection_name: str,
) -> tuple[dict[str, Any], pd.DataFrame]:
    if merged.empty or set(merged["split"].astype(str)) != {projection_name}:
        raise _pipeline_error("M5B evaluation projection has the wrong split", projection=projection_name)
    output = merged.loc[:, list(KEYS)].reset_index(drop=True).copy()
    metrics: dict[str, Any] = {"row_count": len(merged), "heads": {}}
    split_positions = merged.index
    for horizon in OUTCOME_HORIZONS:
        eligible = merged[f"modelable_{horizon}"].astype(bool)
        eligible_positions = eligible[eligible].index
        output_locations = split_positions.get_indexer(eligible_positions)
        if (output_locations < 0).any() or not len(output_locations):
            raise _pipeline_error("M5B projection lost eligible row identity", horizon=horizon)
        local_matrix = matrix.loc[eligible]
        for family, output_name in (
            ("positive_excess", "positive_probability"),
            ("signal_survival", "signal_survival_probability"),
        ):
            head = f"{family}_h{horizon}"
            raw_margin = _predict(models[head], local_matrix, head=head, raw_score=True)
            raw_probability = _predict(models[head], local_matrix, head=head)
            truth = pd.to_numeric(merged.loc[eligible, f"{family}_{horizon}"], errors="raise").to_numpy()
            head_spec = (calibration_spec.get("binary_heads") or {}).get(head) or {}
            if head_spec.get("state") == "CALIBRATED":
                calibrated = apply_platt_calibrator(
                    raw_margin=raw_margin,
                    coefficient=float(head_spec["coefficient"]),
                    intercept=float(head_spec["intercept"]),
                )
                calibrated_metrics = binary_metrics(truth, calibrated)
            elif head_spec.get("state") == "UNCALIBRATED":
                calibrated = np.full(len(raw_probability), np.nan)
                calibrated_metrics = None
            else:
                raise _pipeline_error("M5B binary head spec has an unknown state", head=head)
            output.loc[output_locations, f"{output_name}_{horizon}"] = raw_probability
            output.loc[output_locations, f"{output_name}_calibrated_{horizon}"] = calibrated
            metrics["heads"][head] = {
                "state": head_spec["state"],
                "raw": binary_metrics(truth, raw_probability),
                "calibrated": calibrated_metrics,
            }

        return_raw = {
            quantile: _predict(models[f"excess_return_h{horizon}_q{quantile}"], local_matrix, head=f"excess_return_h{horizon}_q{quantile}")
            for quantile in (10, 50, 90)
        }
        ordered_return = np.sort(np.column_stack(tuple(return_raw.values())), axis=1)
        raw_return_stack = np.column_stack(tuple(return_raw.values()))
        raw_return_crossing_count = int(
            ((raw_return_stack[:, 0] > raw_return_stack[:, 1])
             | (raw_return_stack[:, 1] > raw_return_stack[:, 2])).sum()
        )
        return_spec = (calibration_spec.get("return_intervals") or {}).get(f"excess_return_h{horizon}") or {}
        return_cal = apply_return_interval_adjustment(
            q10=return_raw[10], q50=return_raw[50], q90=return_raw[90], delta=float(return_spec["delta"])
        )
        return_truth = pd.to_numeric(merged.loc[eligible, f"excess_return_{horizon}"], errors="raise").to_numpy()
        for index, quantile in enumerate((10, 50, 90)):
            output.loc[output_locations, f"excess_return_q{quantile}_{horizon}"] = ordered_return[:, index]
            output.loc[output_locations, f"excess_return_calibrated_q{quantile}_{horizon}"] = return_cal[index]
        metrics["heads"][f"excess_return_h{horizon}"] = {
            "state": "CALIBRATED",
            "raw_coverage": float(((return_truth >= ordered_return[:, 0]) & (return_truth <= ordered_return[:, 2])).mean()),
            "calibrated_coverage": float(((return_truth >= return_cal[0]) & (return_truth <= return_cal[2])).mean()),
            "raw_mean_width": float((ordered_return[:, 2] - ordered_return[:, 0]).mean()),
            "calibrated_mean_width": float((return_cal[2] - return_cal[0]).mean()),
            "raw_quantile_crossing_count": raw_return_crossing_count,
            "calibrated_quantile_crossing_count": 0,
            "raw_pinball": {
                str(q): float(mean_pinball_loss(return_truth, ordered_return[:, i], alpha=q / 100))
                for i, q in enumerate((10, 50, 90))
            },
            "calibrated_pinball": {
                str(q): float(mean_pinball_loss(return_truth, return_cal[i], alpha=q / 100))
                for i, q in enumerate((10, 50, 90))
            },
        }
        for family in ("path_mfe", "path_mae_loss"):
            raw_q50 = _predict(models[f"{family}_h{horizon}_q50"], local_matrix, head=f"{family}_h{horizon}_q50")
            raw_q90 = _predict(models[f"{family}_h{horizon}_q90"], local_matrix, head=f"{family}_h{horizon}_q90")
            raw_path_stack = np.column_stack((raw_q50, raw_q90))
            ordered_path = np.sort(np.maximum(np.column_stack((raw_q50, raw_q90)), 0.0), axis=1)
            path_spec = (calibration_spec.get("path_upper") or {}).get(f"{family}_h{horizon}") or {}
            calibrated_path = apply_path_upper_adjustment(q50=raw_q50, q90=raw_q90, delta=float(path_spec["delta"]))
            path_truth = pd.to_numeric(merged.loc[eligible, f"{family}_{horizon}"], errors="raise").to_numpy()
            output.loc[output_locations, f"{family}_q50_{horizon}"] = ordered_path[:, 0]
            output.loc[output_locations, f"{family}_q90_{horizon}"] = ordered_path[:, 1]
            output.loc[output_locations, f"{family}_calibrated_q50_{horizon}"] = calibrated_path[0]
            output.loc[output_locations, f"{family}_calibrated_q90_{horizon}"] = calibrated_path[1]
            metrics["heads"][f"{family}_h{horizon}"] = {
                "state": "CALIBRATED",
                "raw_upper_coverage": float((path_truth <= ordered_path[:, 1]).mean()),
                "calibrated_upper_coverage": float((path_truth <= calibrated_path[1]).mean()),
                "raw_mean_upper": float(ordered_path[:, 1].mean()),
                "calibrated_mean_upper": float(calibrated_path[1].mean()),
                "raw_quantile_crossing_count": int((raw_path_stack[:, 0] > raw_path_stack[:, 1]).sum()),
                "calibrated_quantile_crossing_count": 0,
                "negative_prediction_count_before_clip": int((raw_path_stack < 0.0).sum()),
                "q50_pinball": float(mean_pinball_loss(path_truth, ordered_path[:, 0], alpha=0.5)),
                "raw_q90_pinball": float(mean_pinball_loss(path_truth, ordered_path[:, 1], alpha=0.9)),
                "calibrated_q90_pinball": float(mean_pinball_loss(path_truth, calibrated_path[1], alpha=0.9)),
            }
    return metrics, output


def _load_parent_models(parent_root: Path) -> dict[str, Any]:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable for M5B calibration",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc
    names: list[str] = []
    for horizon in OUTCOME_HORIZONS:
        names.extend(f"excess_return_h{horizon}_q{q}" for q in (10, 50, 90))
        names.extend((f"positive_excess_h{horizon}", f"signal_survival_h{horizon}"))
        names.extend(f"{family}_h{horizon}_q{q}" for family in ("path_mfe", "path_mae_loss") for q in (50, 90))
    names.append("holding_bucket")
    try:
        return {name: lgb.Booster(model_file=str(parent_root / "models" / f"{name}.txt")) for name in names}
    except Exception as exc:
        raise _pipeline_error("M5B parent model cannot be loaded", error_type=type(exc).__name__) from exc


def _predict(model: Any, matrix: pd.DataFrame, *, head: str, raw_score: bool = False) -> np.ndarray:
    try:
        values = np.asarray(model.predict(matrix, raw_score=raw_score), dtype=float)
    except Exception as exc:
        raise _pipeline_error("M5B parent head prediction failed", head=head, error_type=type(exc).__name__) from exc
    if values.shape != (len(matrix),) or not np.isfinite(values).all():
        raise _pipeline_error("M5B parent head returned invalid prediction", head=head, shape=list(values.shape))
    return values


def _validate_request_artifacts(
    request: FrozenAdvisoryOutcomeCalibrationRequestV1,
    parent_root: Path,
    parent_manifest: Mapping[str, Any],
) -> None:
    expected = {
        "schema_version": "advisory_outcome_bundle_v1",
        "calibration_state": "UNCALIBRATED",
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
        or sha256_file(parent_root / "manifest.json") != request.parent_outcome_manifest_file_sha256
        or sha256_file(parent_root / "split.json") != request.split_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5B request differs from parent M3 bundle",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_PARENT_MISMATCH",
        )


def _read_bound_parquet(descriptor: Any) -> pd.DataFrame:
    _validate_parquet_descriptor(descriptor)
    path = Path(descriptor.path)
    frame = pd.read_parquet(path)
    if len(frame) != descriptor.row_count:
        raise AdvisoryModelFirstError(
            "M5B input artifact shape differs from its request",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_PARENT_MISMATCH",
            context={"path": str(path)},
        )
    return frame


def _validate_parquet_descriptor(descriptor: Any) -> None:
    path = Path(descriptor.path)
    if (
        not path.is_file()
        or path.stat().st_size != descriptor.size_bytes
        or sha256_file(path) != descriptor.sha256
    ):
        raise AdvisoryModelFirstError(
            "M5B input artifact differs from its request",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_PARENT_MISMATCH",
            context={"path": str(path)},
        )
    parquet = pq.ParquetFile(path)
    columns = tuple(parquet.schema_arrow.names)
    if parquet.metadata.num_rows != descriptor.row_count or columns != descriptor.columns:
        raise AdvisoryModelFirstError(
            "M5B input artifact metadata differs from its request",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_PARENT_MISMATCH",
            context={"path": str(path)},
        )


def _prepare_matrix_from_schema(
    frame: pd.DataFrame, *, feature_schema: Mapping[str, Any]
) -> pd.DataFrame:
    missing = sorted(set(MODEL_FEATURE_COLUMNS) - set(frame.columns))
    if missing:
        raise _pipeline_error("M5B projection is missing frozen model features", missing_features=missing)
    matrix = frame.loc[:, MODEL_FEATURE_COLUMNS].copy()
    for column in matrix.columns:
        if column not in CATEGORICAL_FEATURE_COLUMNS:
            try:
                matrix[column] = pd.to_numeric(matrix[column], errors="raise")
            except (TypeError, ValueError) as exc:
                raise _pipeline_error(
                    "M5B projection contains a non-numeric feature",
                    feature=column,
                    error_type=type(exc).__name__,
                ) from exc
    vocabulary = feature_schema.get("categorical_vocabulary") or {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        categories = tuple(int(value) for value in vocabulary.get(column) or ())
        if not categories:
            raise _pipeline_error("M5B parent categorical vocabulary is empty", feature=column)
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            matrix.loc[unseen, f"{column}__missing"] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    return matrix


def _projection_hash(keys: pd.DataFrame) -> str:
    from backend.services.advisory_model_first.outcome_contracts import canonical_json_sha256

    rows = keys.astype(str).sort_values(list(KEYS), kind="mergesort").to_dict("records")
    return canonical_json_sha256(rows)


def _verify_environment(request: FrozenAdvisoryOutcomeCalibrationRequestV1) -> None:
    release = platform.release().lower()
    if (
        platform.system() != "Linux"
        or "microsoft" not in release
        or os.environ.get("CONDA_DEFAULT_ENV") != request.conda_environment
    ):
        raise AdvisoryModelFirstError(
            "M5B calibration must run in WSL rdagent-gpu",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"platform": platform.system(), "conda_environment": os.environ.get("CONDA_DEFAULT_ENV")},
        )
    repository_root = Path(request.repository_root).resolve()
    if not repository_root.is_dir() or not Path(request.output_root).exists():
        raise AdvisoryModelFirstError(
            "M5B explicit repository or output root does not exist",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_REQUEST_INVALID",
            context={
                "repository_root_exists": repository_root.is_dir(),
                "output_root_exists": Path(request.output_root).exists(),
            },
        )
    actual_commit = _resolve_wsl_repository_commit(repository_root)
    if actual_commit != request.repository_commit:
        raise AdvisoryModelFirstError(
            "M5B repository commit differs from its frozen request",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={
                "expected_commit": request.repository_commit,
                "actual_commit": actual_commit,
            },
        )


def _peak_rss_bytes() -> int:
    try:
        import resource
    except ImportError as exc:
        raise AdvisoryModelFirstError(
            "M5B resource measurement requires the WSL training environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"platform": platform.system()},
        ) from exc
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _pipeline_error("M5B frozen JSON cannot be read", path=str(path)) from exc
    if not isinstance(value, dict):
        raise _pipeline_error("M5B frozen JSON is not an object", path=str(path))
    return value


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _pipeline_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_OUTCOME_CALIBRATION_FAILED",
        context=context,
    )
