"""Read-only diagnostics for immutable action-value increment bundles.

This pipeline does not train, select, register, or serve a model.  It binds
already-published increment bundles and describes where optional information
changes OOF predictions, selected actions, and continuous policy paths.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    ATR14_INFORMATION_BLOCK,
    CHIP_COST_INFORMATION_BLOCK,
    MONEYFLOW_INFORMATION_BLOCK,
    SW_L2_INFORMATION_BLOCK,
    TZ,
    ActionValueError,
)
from .action_value_data import file_reference
from .action_value_incremental import inspect_increment_bundle
from .action_value_pipeline import _clean_repository_commit
from .action_value_research import _select_oof_actions
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_ACTION_VALUE_DIAGNOSTICS_V1"
ARTIFACT_FOLDER = "action_value_diagnostics_v1"
REQUEST_SCHEMA = "position_timing_action_value_diagnostic_request_v1"
RECEIPT_SCHEMA = "position_timing_action_value_diagnostic_receipt_v1"
BUNDLE_SCHEMA = "position_timing_action_value_diagnostic_bundle_v1"
INTERPRETATION = "DESCRIPTIVE_DIAGNOSTIC_NOT_ALPHA_EVIDENCE"
EXPECTED_INFORMATION_BLOCKS = (
    ATR14_INFORMATION_BLOCK,
    SW_L2_INFORMATION_BLOCK,
    MONEYFLOW_INFORMATION_BLOCK,
    CHIP_COST_INFORMATION_BLOCK,
)
OOF_KEYS = ("symbol", "decision_as_of", "objective", "planned_delta_qty")
SELECTION_KEYS = ("symbol", "decision_as_of", "objective")
CONTINUOUS_KEYS = ("sleeve_id", "valuation_date", "baseline")
OOF_REQUIRED = (*OOF_KEYS, "net_action_value_bps", "predicted_action_value_bps")
CONTINUOUS_REQUIRED = (
    *CONTINUOUS_KEYS,
    "action",
    "decision_input_status",
    "fill_status",
    "planned_delta_qty",
    "incremental_net_value_cny",
)
PAIRED_REQUIRED = (
    "valuation_date",
    "incremental_net_value_cny",
    "sleeve_count",
    "incremental_net_value_bps",
)
METRIC_CONTRACT = {
    "schema_version": "position_timing_action_value_diagnostic_metric_contract_v1",
    "oof_keys": OOF_KEYS,
    "selection_keys": SELECTION_KEYS,
    "continuous_keys": CONTINUOUS_KEYS,
    "oof_metrics": (
        "spearman_prediction_label",
        "mae_bps",
        "rmse_bps",
        "calibration_intercept_bps",
        "calibration_slope",
        "predicted_positive_count",
        "predicted_positive_rate",
        "predicted_positive_realized_mean_bps",
        "predicted_positive_realized_positive_rate",
    ),
    "selection_policy": "REUSE_ACTION_VALUE_RESEARCH_SELECT_OOF_ACTIONS",
    "selection_no_action_realized_value_bps": 0.0,
    "continuous_comparison": "EXACT_FULL_KEY_SET_NO_INTERSECTION",
    "inference": "NONE_DESCRIPTIVE_ONLY",
}
METRIC_CONTRACT_SHA256 = canonical_sha256(METRIC_CONTRACT)


def _finite_float(value: Any) -> float | None:
    result = float(value)
    return result if math.isfinite(result) else None


def _rate(numerator: int, denominator: int) -> float | None:
    return _finite_float(numerator / denominator) if denominator else None


def _require_columns(frame: pd.DataFrame, columns: Sequence[str], *, code: str) -> None:
    if frame.empty or not set(columns).issubset(frame.columns):
        raise ActionValueError(code)


def _require_unique(frame: pd.DataFrame, keys: Sequence[str], *, code: str) -> None:
    if frame.duplicated(list(keys)).any():
        raise ActionValueError(code)


def _numeric(frame: pd.DataFrame, column: str, *, code: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce").astype(float)
    if not np.isfinite(values.to_numpy()).all():
        raise ActionValueError(code, field=column)
    return values


def _counts(values: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in values.astype(str).value_counts(dropna=False).sort_index().items()
    }


def _oof_slice_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    actual = _numeric(frame, "net_action_value_bps", code="DIAGNOSTIC_OOF_NON_FINITE")
    predicted = _numeric(
        frame, "predicted_action_value_bps", code="DIAGNOSTIC_OOF_NON_FINITE"
    )
    error = predicted - actual
    rank_corr = (
        predicted.rank(method="average").corr(actual.rank(method="average"))
        if predicted.nunique() > 1 and actual.nunique() > 1
        else math.nan
    )
    variance = float(np.var(predicted.to_numpy(), ddof=0))
    if variance > 0:
        slope = float(np.cov(predicted.to_numpy(), actual.to_numpy(), ddof=0)[0, 1] / variance)
        intercept = float(actual.mean() - slope * predicted.mean())
    else:
        slope = math.nan
        intercept = math.nan
    positive = predicted > 0
    positive_actual = actual.loc[positive]
    return {
        "row_count": int(len(frame)),
        "spearman_prediction_label": _finite_float(rank_corr),
        "mae_bps": _finite_float(error.abs().mean()),
        "rmse_bps": _finite_float(math.sqrt(float((error * error).mean()))),
        "calibration_intercept_bps": _finite_float(intercept),
        "calibration_slope": _finite_float(slope),
        "predicted_positive_count": int(positive.sum()),
        "predicted_positive_rate": _rate(int(positive.sum()), len(frame)),
        "predicted_positive_realized_mean_bps": _finite_float(positive_actual.mean()),
        "predicted_positive_realized_positive_rate": (
            _rate(int((positive_actual > 0).sum()), len(positive_actual))
        ),
    }


def summarize_oof(frame: pd.DataFrame) -> dict[str, Any]:
    _require_columns(frame, OOF_REQUIRED, code="DIAGNOSTIC_OOF_SCHEMA_INVALID")
    _require_unique(frame, OOF_KEYS, code="DIAGNOSTIC_OOF_KEY_DUPLICATE")
    objectives = sorted(str(value) for value in frame["objective"].unique())
    return {
        "overall": _oof_slice_metrics(frame),
        "by_objective": {
            objective: _oof_slice_metrics(frame.loc[frame["objective"].astype(str) == objective])
            for objective in objectives
        },
    }


def _normalized_key_frame(frame: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    return frame.loc[:, list(keys)].astype(str).sort_values(list(keys)).reset_index(drop=True)


def _require_same_keys(
    core: pd.DataFrame,
    optional: pd.DataFrame,
    keys: Sequence[str],
    *,
    code: str,
) -> None:
    if not _normalized_key_frame(core, keys).equals(_normalized_key_frame(optional, keys)):
        raise ActionValueError(code)


def compare_selected_actions(core: pd.DataFrame, optional: pd.DataFrame) -> dict[str, Any]:
    _require_same_keys(core, optional, OOF_KEYS, code="DIAGNOSTIC_OOF_KEYS_MISMATCH")
    core_ordered = core.sort_values(list(OOF_KEYS)).reset_index(drop=True)
    optional_ordered = optional.sort_values(list(OOF_KEYS)).reset_index(drop=True)
    core_labels = _numeric(core_ordered, "net_action_value_bps", code="DIAGNOSTIC_OOF_NON_FINITE")
    optional_labels = _numeric(
        optional_ordered, "net_action_value_bps", code="DIAGNOSTIC_OOF_NON_FINITE"
    )
    if not np.array_equal(core_labels.to_numpy(), optional_labels.to_numpy()):
        raise ActionValueError("DIAGNOSTIC_OOF_LABELS_MISMATCH")
    core_selected = _select_oof_actions(core)
    optional_selected = _select_oof_actions(optional)
    all_groups = pd.concat(
        [core.loc[:, list(SELECTION_KEYS)], optional.loc[:, list(SELECTION_KEYS)]],
        ignore_index=True,
    ).drop_duplicates()
    core_lookup = core_selected.set_index(list(SELECTION_KEYS), drop=False)
    optional_lookup = optional_selected.set_index(list(SELECTION_KEYS), drop=False)
    counts = {"neither": 0, "core_only": 0, "optional_only": 0, "both_selected": 0}
    changed = 0
    realized_differences: list[float] = []
    for row in all_groups.itertuples(index=False, name=None):
        key = tuple(row)
        core_has = key in core_lookup.index
        optional_has = key in optional_lookup.index
        if not core_has and not optional_has:
            counts["neither"] += 1
        elif core_has and not optional_has:
            counts["core_only"] += 1
        elif optional_has and not core_has:
            counts["optional_only"] += 1
        else:
            counts["both_selected"] += 1
        core_delta = float(core_lookup.loc[key, "planned_delta_qty"]) if core_has else 0.0
        optional_delta = (
            float(optional_lookup.loc[key, "planned_delta_qty"]) if optional_has else 0.0
        )
        if core_has != optional_has or core_delta != optional_delta:
            changed += 1
        core_realized = float(core_lookup.loc[key, "net_action_value_bps"]) if core_has else 0.0
        optional_realized = (
            float(optional_lookup.loc[key, "net_action_value_bps"]) if optional_has else 0.0
        )
        realized_differences.append(optional_realized - core_realized)
    group_count = len(all_groups)
    return {
        "decision_group_count": int(group_count),
        **counts,
        "core_selected_count": int(len(core_selected)),
        "optional_selected_count": int(len(optional_selected)),
        "selection_changed_count": int(changed),
        "selection_changed_rate": _rate(changed, group_count),
        "optional_minus_core_selected_realized_mean_bps": _finite_float(
            np.mean(realized_differences)
        ),
        "no_action_realized_value_bps": 0.0,
    }


def summarize_continuous(frame: pd.DataFrame) -> dict[str, Any]:
    _require_columns(frame, CONTINUOUS_REQUIRED, code="DIAGNOSTIC_CONTINUOUS_SCHEMA_INVALID")
    _require_unique(frame, CONTINUOUS_KEYS, code="DIAGNOSTIC_CONTINUOUS_KEY_DUPLICATE")
    planned = _numeric(frame, "planned_delta_qty", code="DIAGNOSTIC_CONTINUOUS_NON_FINITE")
    nonzero = planned != 0
    filled = frame["fill_status"].astype(str).eq("FILLED")
    return {
        "row_count": int(len(frame)),
        "sleeve_count": int(frame["sleeve_id"].nunique()),
        "valuation_date_count": int(frame["valuation_date"].nunique()),
        "action_counts": _counts(frame["action"]),
        "decision_input_status_counts": _counts(frame["decision_input_status"]),
        "fill_status_counts": _counts(frame["fill_status"]),
        "nonzero_plan_count": int(nonzero.sum()),
        "nonzero_plan_rate": _rate(int(nonzero.sum()), len(frame)),
        "filled_nonzero_plan_count": int((filled & nonzero).sum()),
        "fill_rate_among_nonzero_plans": _rate(int((filled & nonzero).sum()), int(nonzero.sum())),
    }


def compare_continuous(core: pd.DataFrame, optional: pd.DataFrame) -> dict[str, Any]:
    _require_same_keys(
        core, optional, CONTINUOUS_KEYS, code="DIAGNOSTIC_CONTINUOUS_KEYS_MISMATCH"
    )
    left = core.sort_values(list(CONTINUOUS_KEYS)).reset_index(drop=True)
    right = optional.sort_values(list(CONTINUOUS_KEYS)).reset_index(drop=True)
    core_delta = _numeric(left, "planned_delta_qty", code="DIAGNOSTIC_CONTINUOUS_NON_FINITE")
    optional_delta = _numeric(
        right, "planned_delta_qty", code="DIAGNOSTIC_CONTINUOUS_NON_FINITE"
    )
    core_value = _numeric(
        left, "incremental_net_value_cny", code="DIAGNOSTIC_CONTINUOUS_NON_FINITE"
    )
    optional_value = _numeric(
        right, "incremental_net_value_cny", code="DIAGNOSTIC_CONTINUOUS_NON_FINITE"
    )
    comparisons = {
        "action_changed": left["action"].astype(str).ne(right["action"].astype(str)),
        "planned_delta_changed": core_delta.ne(optional_delta),
        "decision_input_status_changed": left["decision_input_status"].astype(str).ne(
            right["decision_input_status"].astype(str)
        ),
        "fill_status_changed": left["fill_status"].astype(str).ne(
            right["fill_status"].astype(str)
        ),
        "incremental_net_value_changed": ~np.isclose(
            core_value.to_numpy(), optional_value.to_numpy(), rtol=0.0, atol=1e-9
        ),
    }
    result: dict[str, Any] = {"row_count": int(len(left))}
    for name, values in comparisons.items():
        count = int(np.asarray(values).sum())
        result[f"{name}_count"] = count
        result[f"{name}_rate"] = _rate(count, len(left))
    return result


def summarize_paired_daily(frame: pd.DataFrame, receipt: Mapping[str, Any]) -> dict[str, Any]:
    _require_columns(frame, PAIRED_REQUIRED, code="DIAGNOSTIC_PAIRED_DAILY_SCHEMA_INVALID")
    _require_unique(frame, ("valuation_date",), code="DIAGNOSTIC_PAIRED_DAILY_KEY_DUPLICATE")
    values = _numeric(frame, "incremental_net_value_bps", code="DIAGNOSTIC_PAIRED_DAILY_NON_FINITE")
    receipt_point = (receipt.get("incremental_comparison") or {}).get(
        "daily_mean_incremental_bps"
    )
    point = _finite_float(values.mean())
    if receipt_point is None or not math.isclose(
        float(receipt_point), float(point), rel_tol=0.0, abs_tol=1e-10
    ):
        raise ActionValueError("DIAGNOSTIC_PAIRED_DAILY_RECEIPT_MISMATCH")
    return {
        "row_count": int(len(frame)),
        "daily_mean_incremental_bps": point,
        "daily_std_incremental_bps": _finite_float(values.std(ddof=1)),
        "daily_min_incremental_bps": _finite_float(values.min()),
        "daily_max_incremental_bps": _finite_float(values.max()),
        "positive_day_rate": _rate(int((values > 0).sum()), len(values)),
        "period_cumulative_incremental_bps": _finite_float(values.sum()),
        "receipt_point_estimate_matches": True,
    }


def _artifact_names(manifest: Mapping[str, Any]) -> tuple[str, str]:
    names = tuple((manifest.get("files") or {}).keys())
    optional_oof = [
        name
        for name in names
        if name.endswith("_oof_action_predictions.parquet")
        and name != "core_oof_action_predictions.parquet"
    ]
    optional_continuous = [
        name
        for name in names
        if name.endswith("_continuous_sleeve_days.parquet")
        and name != "core_continuous_sleeve_days.parquet"
    ]
    required = {
        "core_oof_action_predictions.parquet",
        "core_continuous_sleeve_days.parquet",
        "paired_daily_increment.parquet",
    }
    if not required.issubset(names) or len(optional_oof) != 1 or len(optional_continuous) != 1:
        raise ActionValueError("DIAGNOSTIC_INCREMENT_ARTIFACT_SET_INVALID")
    return optional_oof[0], optional_continuous[0]


def diagnose_increment_bundle(
    bundle: Path, inspected: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    inspected = dict(inspected or inspect_increment_bundle(bundle))
    manifest = inspected["manifest"]
    optional_oof_name, optional_continuous_name = _artifact_names(manifest)
    core_oof = pd.read_parquet(bundle / "core_oof_action_predictions.parquet")
    optional_oof = pd.read_parquet(bundle / optional_oof_name)
    core_continuous = pd.read_parquet(bundle / "core_continuous_sleeve_days.parquet")
    optional_continuous = pd.read_parquet(bundle / optional_continuous_name)
    paired_daily = pd.read_parquet(bundle / "paired_daily_increment.parquet")
    return {
        "information_block": inspected["request"]["information_block"],
        "source_request_sha256": inspected["request"]["request_sha256"],
        "source_manifest_sha256": manifest["manifest_sha256"],
        "core_oof": summarize_oof(core_oof),
        "optional_oof": summarize_oof(optional_oof),
        "selection_comparison": compare_selected_actions(core_oof, optional_oof),
        "core_continuous": summarize_continuous(core_continuous),
        "optional_continuous": summarize_continuous(optional_continuous),
        "continuous_comparison": compare_continuous(core_continuous, optional_continuous),
        "paired_daily": summarize_paired_daily(paired_daily, inspected["receipt"]),
    }


def _input_reference(bundle: Path, inspected: Mapping[str, Any]) -> dict[str, Any]:
    manifest_ref = file_reference(bundle / "manifest.json")
    return {
        "information_block": inspected["request"]["information_block"],
        "bundle_path": bundle.resolve().as_posix(),
        "bundle_request_sha256": inspected["request"]["request_sha256"],
        "manifest_sha256": inspected["manifest"]["manifest_sha256"],
        "manifest_file": manifest_ref,
    }


def prepare_diagnostic_request(
    *, timing_root: Path, repository_root: Path, input_bundles: Sequence[Path]
) -> Path:
    source_commit = _clean_repository_commit(repository_root.resolve())
    by_block: dict[str, tuple[Path, Mapping[str, Any]]] = {}
    for bundle in input_bundles:
        resolved = bundle.resolve()
        inspected = inspect_increment_bundle(resolved)
        block = str(inspected["request"].get("information_block") or "")
        if block in by_block:
            raise ActionValueError("DIAGNOSTIC_INPUT_BLOCK_DUPLICATE", information_block=block)
        by_block[block] = (resolved, inspected)
    if set(by_block) != set(EXPECTED_INFORMATION_BLOCKS):
        raise ActionValueError("DIAGNOSTIC_INPUT_BLOCK_SET_INVALID")
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.resolve().as_posix(),
        "repository_commit": source_commit,
        "timing_root": timing_root.resolve().as_posix(),
        "diagnostic_only": True,
        "trial_count": 0,
        "hypothesis_count": 0,
        "metric_contract": METRIC_CONTRACT,
        "metric_contract_sha256": METRIC_CONTRACT_SHA256,
        "input_bundles": [
            _input_reference(*by_block[block]) for block in EXPECTED_INFORMATION_BLOCKS
        ],
        "global_registry_write": False,
        "increment_registry_write": False,
        "current_write": False,
        "database_write": False,
        "runtime_write": False,
        "order_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = (
        timing_root.resolve()
        / "research"
        / ARTIFACT_FOLDER
        / "requests"
        / f"{request['request_sha256']}.json"
    )
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(request))
    return path


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("DIAGNOSTIC_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    inputs = request.get("input_bundles") or ()
    flags = (
        "global_registry_write",
        "increment_registry_write",
        "current_write",
        "database_write",
        "runtime_write",
        "order_write",
    )
    input_references_valid = all(
        isinstance(item, Mapping)
        and Path(str(item.get("bundle_path") or "")).is_absolute()
        and item.get("bundle_request_sha256")
        and item.get("manifest_sha256")
        and isinstance(item.get("manifest_file"), Mapping)
        and item["manifest_file"].get("path")
        and item["manifest_file"].get("sha256")
        and isinstance(item["manifest_file"].get("size_bytes"), int)
        for item in inputs
    )
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("diagnostic_only") is not True
        or request.get("trial_count") != 0
        or request.get("hypothesis_count") != 0
        or canonical_sha256(request.get("metric_contract")) != METRIC_CONTRACT_SHA256
        or request.get("metric_contract_sha256") != METRIC_CONTRACT_SHA256
        or tuple(item.get("information_block") for item in inputs)
        != EXPECTED_INFORMATION_BLOCKS
        or not input_references_valid
        or any(request.get(flag) is not False for flag in flags)
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("DIAGNOSTIC_REQUEST_IDENTITY_MISMATCH")
    return request


def _validate_bound_input(item: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    bundle = Path(item["bundle_path"]).resolve()
    manifest_ref = file_reference(bundle / "manifest.json")
    if manifest_ref != item.get("manifest_file"):
        raise ActionValueError("DIAGNOSTIC_INPUT_MANIFEST_FILE_MISMATCH")
    inspected = inspect_increment_bundle(bundle)
    if (
        inspected["request"].get("information_block") != item.get("information_block")
        or inspected["request"].get("request_sha256") != item.get("bundle_request_sha256")
        or inspected["manifest"].get("manifest_sha256") != item.get("manifest_sha256")
    ):
        raise ActionValueError("DIAGNOSTIC_INPUT_IDENTITY_MISMATCH")
    return bundle, inspected


def _manifest(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    files = {}
    for name in ("request.json", "receipt.json"):
        reference = file_reference(root / name)
        reference.pop("path", None)
        files[name] = reference
    manifest = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": files,
    }
    manifest["manifest_sha256"] = canonical_sha256(manifest)
    return manifest


def _publish_bundle(bundle: Path, request: Mapping[str, Any], receipt: Mapping[str, Any]) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(
            staging / "request.json", canonical_json_bytes(request)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "receipt.json", canonical_json_bytes(receipt)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock_path = bundle.parents[3] / "locks" / f"action-value-diagnostic-{bundle.name}.lock"
        with _exclusive_file_lock(lock_path):
            if bundle.exists():
                inspect_diagnostic_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_diagnostic_bundle(bundle: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("DIAGNOSTIC_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if (
        manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("manifest_sha256") != canonical_sha256(manifest_identity)
        or manifest.get("request_sha256") != request.get("request_sha256")
        or manifest.get("receipt_sha256") != receipt.get("receipt_sha256")
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("pipeline_id") != PIPELINE_ID
        or receipt.get("request_sha256") != request.get("request_sha256")
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity)
        or receipt.get("diagnostic_only") is not True
        or receipt.get("trial_count") != 0
        or receipt.get("selected_trial_count") != 0
        or receipt.get("next_hypothesis") is not None
        or receipt.get("interpretation") != INTERPRETATION
        or tuple(item.get("information_block") for item in receipt.get("diagnostics") or ())
        != EXPECTED_INFORMATION_BLOCKS
        or tuple(item.get("source_request_sha256") for item in receipt.get("diagnostics") or ())
        != tuple(item.get("bundle_request_sha256") for item in request["input_bundles"])
        or tuple(item.get("source_manifest_sha256") for item in receipt.get("diagnostics") or ())
        != tuple(item.get("manifest_sha256") for item in request["input_bundles"])
    ):
        raise ActionValueError("DIAGNOSTIC_BUNDLE_IDENTITY_MISMATCH")
    for name in ("request.json", "receipt.json"):
        observed = file_reference(bundle / name)
        observed.pop("path", None)
        if (manifest.get("files") or {}).get(name) != observed:
            raise ActionValueError("DIAGNOSTIC_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {"manifest": manifest, "request": request, "receipt": receipt}


def run_diagnostic_request(request_path: Path) -> dict[str, Any]:
    request = _load_request(request_path)
    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("DIAGNOSTIC_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    if bundle.exists():
        return {"status": "ALREADY_MATERIALIZED", "bundle": bundle.as_posix(), **inspect_diagnostic_bundle(bundle)}
    diagnostics = []
    for item in request["input_bundles"]:
        input_bundle, inspected = _validate_bound_input(item)
        diagnostics.append(diagnose_increment_bundle(input_bundle, inspected))
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "metric_contract_sha256": METRIC_CONTRACT_SHA256,
        "diagnostic_only": True,
        "trial_count": 0,
        "hypothesis_count": 0,
        "selected_trial_count": 0,
        "next_hypothesis": None,
        "interpretation": INTERPRETATION,
        "serving_status": "DIAGNOSTIC_ONLY_NO_RUNTIME_MODEL",
        "input_bundle_count": len(diagnostics),
        "diagnostics": diagnostics,
        "global_registry_written": False,
        "increment_registry_written": False,
        "current_written": False,
        "database_written": False,
        "runtime_written": False,
        "order_written": False,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    _publish_bundle(bundle, request, receipt)
    return {"status": "MATERIALIZED", "bundle": bundle.as_posix(), **inspect_diagnostic_bundle(bundle)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--bundle", action="append", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = {
                "status": "PREPARED",
                "request": prepare_diagnostic_request(
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    input_bundles=args.bundle,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_diagnostic_request(args.request)
        else:
            result = {"status": "BUNDLE_VALID", **inspect_diagnostic_bundle(args.bundle)}
    except ActionValueError as exc:
        print(
            json.dumps(
                {"status": "FAILED", "error_code": exc.code, "details": exc.details},
                ensure_ascii=False,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARTIFACT_FOLDER",
    "BUNDLE_SCHEMA",
    "METRIC_CONTRACT_SHA256",
    "PIPELINE_ID",
    "RECEIPT_SCHEMA",
    "REQUEST_SCHEMA",
    "compare_continuous",
    "compare_selected_actions",
    "diagnose_increment_bundle",
    "inspect_diagnostic_bundle",
    "prepare_diagnostic_request",
    "run_diagnostic_request",
    "summarize_continuous",
    "summarize_oof",
    "summarize_paired_daily",
]
