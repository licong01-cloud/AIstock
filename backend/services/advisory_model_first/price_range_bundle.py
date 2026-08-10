from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

import numpy as np
import pandas as pd

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
            _expected_label_policy(request.label_policy_version),
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
    except Exception as exc:
        if temporary.exists():
            try:
                shutil.rmtree(temporary)
            except OSError as cleanup_exc:
                raise AdvisoryModelFirstError(
                    "incomplete price-range bundle could not be removed",
                    reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
                    context={
                        "temporary_path": str(temporary),
                        "source_error_type": type(exc).__name__,
                        "cleanup_error_type": type(cleanup_exc).__name__,
                        "cleanup_error_message": str(cleanup_exc),
                    },
                ) from cleanup_exc
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
        size_bytes = descriptor.get("size_bytes") if isinstance(descriptor, dict) else None
        if (
            not isinstance(descriptor, dict)
            or not isinstance(size_bytes, int)
            or size_bytes <= 0
            or not isinstance(descriptor.get("sha256"), str)
            or not path.is_file()
            or path.stat().st_size != size_bytes
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
        split = json.loads((bundle_path / "split.json").read_text(encoding="utf-8"))
        metrics = json.loads((bundle_path / "metrics.json").read_text(encoding="utf-8"))
        training_log = json.loads(
            (bundle_path / "training_log.json").read_text(encoding="utf-8")
        )
        predictions = pd.read_parquet(bundle_path / "test_predictions.parquet")
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "price-range bundle semantic members cannot be read",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    if (
        not isinstance(feature_schema, dict)
        or not isinstance(label_policy, dict)
        or request.request_id != manifest.get("request_id")
        or request.request_sha256 != manifest.get("request_sha256")
        or request.parent_request_id != manifest.get("parent_request_id")
        or request.parent_request_sha256 != manifest.get("parent_request_sha256")
        or request.parent_bundle_id != manifest.get("parent_bundle_id")
        or request.outcome_request_id != manifest.get("outcome_request_id")
        or request.outcome_request_sha256 != manifest.get("outcome_request_sha256")
        or request.outcome_bundle_id != manifest.get("outcome_bundle_id")
        or request.package_id != manifest.get("package_id")
        or request.manifest_sha256 != manifest.get("manifest_sha256")
        or request.style_profile_id != manifest.get("style_profile_id")
        or request.style_profile_hash != manifest.get("style_profile_hash")
        or request.feature_schema_version != manifest.get("feature_schema_version")
        or request.feature_schema_hash != FEATURE_SCHEMA_HASH
        or manifest.get("feature_schema_hash") != FEATURE_SCHEMA_HASH
        or request.label_policy_version != manifest.get("label_policy_version")
        or request.repository_commit != manifest.get("repository_commit")
        or manifest.get("schema_version") != "advisory_price_range_bundle_v1"
        or manifest.get("status") != "EXPERIMENTAL_SHADOW"
        or manifest.get("calibration_state") != "UNCALIBRATED"
        or tuple(manifest.get("quantiles") or ()) != PRICE_RANGE_QUANTILES
        or feature_schema.get("feature_schema_hash") != FEATURE_SCHEMA_HASH
        or tuple(feature_schema.get("trained_feature_names") or ())
        != tuple(FEATURE_SCHEMA_PAYLOAD["model_feature_columns"])
        or label_policy != _expected_label_policy(request.label_policy_version)
        or manifest.get("entry_gap_condition") != ENTRY_GAP_CONDITION
        or tuple(manifest.get("model_names") or ()) != PRICE_RANGE_MODEL_NAMES
        or manifest.get("model_count") != len(PRICE_RANGE_MODEL_NAMES)
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle semantic identities are inconsistent",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    _validate_split(split)
    _validate_metrics(metrics)
    _validate_training_log(training_log, request=request)
    _validate_test_predictions(predictions, split=split, metrics=metrics)
    return manifest


def _expected_label_policy(label_policy_version: str) -> dict[str, Any]:
    return {
        "schema_version": label_policy_version,
        "entry_session": "next_trading_day_open",
        "binary_label": "authoritative_entry_executable",
        "missing_market_row_semantics": "UNAVAILABLE_NOT_NEGATIVE",
        "entry_gap_formula": "target_open/decision_close-1",
        "entry_gap_condition": ENTRY_GAP_CONDITION,
        "quantiles": list(PRICE_RANGE_QUANTILES),
    }


def _validate_split(value: Any) -> None:
    expected_lengths = {
        "train": 226,
        "purge_1": 25,
        "validation": 50,
        "purge_2": 25,
        "test": 80,
    }
    if not isinstance(value, dict) or set(value) != set(expected_lengths):
        raise AdvisoryModelFirstError(
            "price-range bundle split has an invalid shape",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    parsed: dict[str, pd.DatetimeIndex] = {}
    try:
        for name, expected_length in expected_lengths.items():
            dates = pd.DatetimeIndex(pd.to_datetime(value[name])).normalize()
            if len(dates) != expected_length or not dates.is_monotonic_increasing or dates.has_duplicates:
                raise ValueError(name)
            parsed[name] = dates
    except (TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "price-range bundle split differs from the frozen M3 membership",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        ) from exc
    flattened = [date for name in expected_lengths for date in parsed[name]]
    if (
        len(set(flattened)) != sum(expected_lengths.values())
        or not pd.DatetimeIndex(flattened).is_monotonic_increasing
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle split memberships overlap or are not chronological",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )


def _validate_metrics(value: Any) -> None:
    expected_heads = set(PRICE_RANGE_MODEL_NAMES)
    distribution = value.get("entry_gap_distribution") if isinstance(value, dict) else None
    if (
        not isinstance(value, dict)
        or value.get("model_count") != len(PRICE_RANGE_MODEL_NAMES)
        or value.get("status") != "EXPERIMENTAL_SHADOW"
        or value.get("calibration_state") != "UNCALIBRATED"
        or not isinstance(value.get("heads"), dict)
        or set(value["heads"]) != expected_heads
        or not isinstance(value.get("test_row_count"), int)
        or value["test_row_count"] <= 0
        or value.get("test_date_count") != 80
        or not isinstance(distribution, dict)
        or distribution.get("condition") != ENTRY_GAP_CONDITION
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle metrics differ from the four-head contract",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    for name in PRICE_RANGE_MODEL_NAMES[1:]:
        head = value["heads"].get(name)
        if not isinstance(head, dict) or head.get("condition") != ENTRY_GAP_CONDITION:
            raise AdvisoryModelFirstError(
                "price-range quantile metric lost its executable-only condition",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
                context={"head": name},
            )
    for name in PRICE_RANGE_MODEL_NAMES:
        head = value["heads"].get(name)
        if (
            not isinstance(head, dict)
            or not isinstance(head.get("row_count"), int)
            or head["row_count"] <= 0
            or not isinstance(head.get("best_iteration"), int)
            or head["best_iteration"] <= 0
        ):
            raise AdvisoryModelFirstError(
                "price-range head metric has an invalid sample or iteration count",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
                context={"head": name},
            )


def _validate_training_log(
    value: Any,
    *,
    request: FrozenAdvisoryPriceRangeTrainingRequestV1,
) -> None:
    environment = value.get("environment_report") if isinstance(value, dict) else None
    resource_report = value.get("resource_report") if isinstance(value, dict) else None
    peak = resource_report.get("peak_rss_bytes") if isinstance(resource_report, dict) else None
    evaluation_history = value.get("evaluation_history") if isinstance(value, dict) else None
    if (
        not isinstance(environment, dict)
        or environment.get("conda_environment") != "rdagent-gpu"
        or not isinstance(environment.get("lightgbm_version"), str)
        or not isinstance(environment.get("pyarrow_version"), str)
        or not isinstance(peak, int)
        or peak <= 0
        or peak > request.resource_max_rss_bytes
        or not isinstance(evaluation_history, dict)
        or set(evaluation_history) != set(PRICE_RANGE_MODEL_NAMES)
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle training environment or resource receipt is invalid",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )


def _validate_test_predictions(
    predictions: pd.DataFrame,
    *,
    split: dict[str, Any],
    metrics: dict[str, Any],
) -> None:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    output_columns = [
        "entry_executable_probability",
        "entry_gap_q10",
        "entry_gap_q50",
        "entry_gap_q90",
    ]
    required = {
        *keys,
        "selection_effective_rank",
        "parent_combined_score",
        "entry_label_status",
        "entry_label_reason",
        "entry_executable",
        "entry_gap_return",
        *output_columns,
        "entry_gap_condition",
    }
    missing = sorted(required - set(predictions.columns))
    if (
        missing
        or len(predictions) != metrics["test_row_count"]
        or predictions.duplicated(keys).any()
        or not predictions["entry_gap_condition"].eq(ENTRY_GAP_CONDITION).all()
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle test predictions have an invalid identity or schema",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
            context={"missing_columns": missing},
        )
    numeric = predictions.loc[:, output_columns].apply(pd.to_numeric, errors="coerce")
    values = numeric.to_numpy(dtype=float)
    probability = numeric["entry_executable_probability"]
    if (
        not np.isfinite(values).all()
        or probability.lt(0.0).any()
        or probability.gt(1.0).any()
        or numeric["entry_gap_q10"].gt(numeric["entry_gap_q50"]).any()
        or numeric["entry_gap_q50"].gt(numeric["entry_gap_q90"]).any()
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle test predictions violate numeric output semantics",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    decision_dates = pd.to_datetime(predictions["decision_as_of_trade_date"]).dt.normalize()
    target_dates = pd.to_datetime(predictions["target_trade_date"]).dt.normalize()
    expected_test_dates = set(pd.to_datetime(split["test"]).normalize())
    if (
        decision_dates.isna().any()
        or target_dates.isna().any()
        or not target_dates.gt(decision_dates).all()
        or decision_dates.nunique() != metrics["test_date_count"]
        or set(decision_dates) != expected_test_dates
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle test predictions differ from the frozen test split",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )
    status = predictions["entry_label_status"].astype(str)
    executable = pd.to_numeric(predictions["entry_executable"], errors="coerce")
    observed_gap = pd.to_numeric(predictions["entry_gap_return"], errors="coerce")
    available = status.eq("AVAILABLE")
    executable_rows = available & executable.eq(1.0)
    ranks = pd.to_numeric(predictions["selection_effective_rank"], errors="coerce")
    scores = pd.to_numeric(predictions["parent_combined_score"], errors="coerce")
    group_sizes = predictions.groupby("decision_as_of_trade_date", sort=False).size()
    rank_identity_duplicates = predictions.duplicated(
        ["decision_as_of_trade_date", "selection_effective_rank"]
    )
    if (
        not set(status.unique()).issubset({"AVAILABLE", "UNAVAILABLE"})
        or executable.loc[available].isna().any()
        or not executable.loc[available].isin([0.0, 1.0]).all()
        or executable.loc[~available].notna().any()
        or observed_gap.loc[executable_rows].isna().any()
        or not np.isfinite(observed_gap.loc[executable_rows].to_numpy(dtype=float)).all()
        or observed_gap.loc[~executable_rows].notna().any()
        or ranks.isna().any()
        or ranks.le(0).any()
        or ranks.gt(20).any()
        or ranks.mod(1).ne(0).any()
        or rank_identity_duplicates.any()
        or not group_sizes.eq(20).all()
        or scores.isna().any()
        or not np.isfinite(scores.to_numpy(dtype=float)).all()
    ):
        raise AdvisoryModelFirstError(
            "price-range bundle test labels or parent ranking fields are inconsistent",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_INVALID",
        )


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
