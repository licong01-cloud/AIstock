"""Approved G2-B risk-L1 development and label-free inference contract.

The module owns one fixed binary LightGBM candidate.  It consumes an immutable
HMM-owned input bundle, never searches a dataset or model parameter, never
reads a sealed tail, and keeps research, capability, and advisory status
separate.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.hmm_risk.rotation_l1_gbdt import (
    CANONICAL_SECTOR_COUNT,
    FitProgress,
    V14_CONTINUOUS_FEATURES,
    RotationL1G2AError,
    _leaf_date_coverage,
    cross_section_rank_features,
    fold_slices,
    require_deterministic_runtime,
)
from backend.services.hmm_risk.contracts import canonical_sha256

CONTRACT_VERSION = "hmm_risk_risk_l1_g2b_v1"
INPUT_SCHEMA_VERSION = "hmm_risk_risk_l1_g2b_input_bundle_v1"
INPUT_MANIFEST_SCHEMA_VERSION = "hmm_risk_risk_l1_g2b_input_manifest_v1"
PROCESS_SCHEMA_VERSION = "hmm_risk_risk_l1_g2b_process_v1"
ACCEPTANCE_SCHEMA_VERSION = "hmm_risk_risk_l1_g2b_acceptance_v1"
MODEL_SCHEMA_VERSION = "hmm_risk_risk_l1_g2b_model_v1"
FIXED_HORIZON = 10
RISK_THRESHOLD = -0.05
RISK_FEATURES = V14_CONTINUOUS_FEATURES
MANDATORY_FEATURES = ("relative_downside_volatility_20d", "relative_max_drawdown_20d")
MINIMUM_VALID_FEATURES = 8
MINIMUM_METRIC_SECTORS = 28
MINIMUM_COVERAGE = 0.90
MINIMUM_PRECISION_LIFT = 0.10
MINIMUM_RECALL = 0.25
TARGET_COLUMN = "risk_event_10d"
ADVERSE_COLUMN = "relative_adverse_excursion_10d"
TARGET_MATURITY_COLUMN = "risk_event_10d_mature"
TARGET_REASON_COLUMN = "reason__risk_event_10d"

REASON_TARGET = "hmm_risk_risk_l1_target_unavailable"
REASON_FEATURE = "hmm_risk_risk_l1_feature_contract_invalid"
REASON_CLASS = "hmm_risk_risk_l1_train_class_insufficient"
REASON_FIT = "hmm_risk_risk_l1_fit_failed"
REASON_LEAF = "hmm_risk_risk_l1_leaf_date_coverage_insufficient"
REASON_SCORE = "hmm_risk_risk_l1_score_invalid"
REASON_COVERAGE = "hmm_risk_risk_l1_daily_coverage_insufficient"
REASON_METRIC = "hmm_risk_risk_l1_metric_unavailable"
REASON_REPRODUCIBILITY = "hmm_risk_risk_l1_reproducibility_mismatch"
REASON_INPUT = "hmm_risk_risk_l1_input_contract_invalid"
REASON_OUTPUT = "hmm_risk_risk_l1_output_collision"
REASON_READBACK = "hmm_risk_risk_l1_readback_mismatch"


class RiskL1G2BError(RuntimeError):
    """Typed fail-closed G2-B error."""

    def __init__(self, reason_code: str, message: str, *, stage: str, evidence: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.stage = stage
        self.evidence = dict(evidence or {})


def _fail(reason_code: str, message: str, *, stage: str, **evidence: Any) -> RiskL1G2BError:
    return RiskL1G2BError(reason_code, message, stage=stage, evidence=evidence)


def lightgbm_profile() -> dict[str, Any]:
    """Return the complete approved binary estimator profile."""

    return {
        "boosting_type": "gbdt",
        "objective": "binary",
        "n_estimators": 240,
        "learning_rate": 0.03,
        "max_depth": 3,
        "num_leaves": 7,
        "min_child_samples": 310,
        "min_child_weight": 0.001,
        "min_split_gain": 0.0,
        "max_bin": 63,
        "min_data_in_bin": 3,
        "subsample": 1.0,
        "subsample_freq": 0,
        "colsample_bytree": 1.0,
        "reg_alpha": 1.0,
        "reg_lambda": 10.0,
        "path_smooth": 0.0,
        "class_weight": "balanced",
        "random_state": 42,
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": 1,
        "feature_pre_filter": False,
        "use_missing": True,
        "zero_as_missing": False,
        "extra_trees": False,
        "max_delta_step": 0.0,
        "subsample_for_bin": 200000,
        "importance_type": "split",
        "verbosity": -1,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _external_new_root(path: Path, forbidden_roots: Sequence[Path]) -> Path:
    raw = Path(path)
    if not raw.is_absolute():
        raise _fail(REASON_INPUT, "risk input/output root must be absolute", stage="writer")
    parent = raw.parent.resolve(strict=True)
    resolved = parent / raw.name
    for forbidden in forbidden_roots:
        try:
            resolved.relative_to(Path(forbidden).resolve(strict=True))
        except ValueError:
            continue
        raise _fail(REASON_INPUT, "risk input/output root cannot be inside the repository", stage="writer")
    if resolved.exists() or resolved.is_symlink():
        raise _fail(REASON_OUTPUT, "risk input/output root already exists", stage="writer")
    return resolved


def validate_input_bundle(bundle: Mapping[str, Any]) -> tuple[pd.DataFrame, tuple[date, ...], tuple[str, ...]]:
    if bundle.get("schema_version") != INPUT_SCHEMA_VERSION or not isinstance(bundle.get("identity"), Mapping):
        raise _fail(REASON_INPUT, "risk input bundle schema differs", stage="input")
    panel = bundle.get("panel")
    if not isinstance(panel, pd.DataFrame) or not isinstance(panel.index, pd.MultiIndex):
        raise _fail(REASON_INPUT, "risk input panel is missing", stage="input")
    panel = panel.copy()
    if tuple(panel.index.names) != ("trade_date", "sector_code") or panel.index.has_duplicates:
        raise _fail(REASON_INPUT, "risk input identity differs", stage="input")
    normalized_dates: list[date] = []
    for value in panel.index.get_level_values("trade_date"):
        parsed = pd.Timestamp(value)
        if parsed != parsed.normalize():
            raise _fail(REASON_INPUT, "risk input date contains a time component", stage="input")
        normalized_dates.append(parsed.date())
    panel.index = pd.MultiIndex.from_arrays(
        [normalized_dates, [str(value) for value in panel.index.get_level_values("sector_code")]],
        names=["trade_date", "sector_code"],
    )
    panel = panel.sort_index()
    if panel.index.has_duplicates:
        raise _fail(REASON_INPUT, "risk input normalized identity collides", stage="input")
    required = {
        *RISK_FEATURES,
        *(f"reason__{feature}" for feature in RISK_FEATURES),
        TARGET_COLUMN,
        ADVERSE_COLUMN,
        TARGET_MATURITY_COLUMN,
        TARGET_REASON_COLUMN,
    }
    if not required.issubset(panel.columns):
        raise _fail(REASON_INPUT, "risk input columns differ", stage="input", missing=sorted(required - set(panel)))
    dates = tuple(sorted(set(panel.index.get_level_values("trade_date"))))
    sectors = tuple(sorted(set(str(value) for value in panel.index.get_level_values("sector_code"))))
    if not dates or len(sectors) != CANONICAL_SECTOR_COUNT or len(panel) != len(dates) * CANONICAL_SECTOR_COUNT:
        raise _fail(REASON_INPUT, "risk input denominator differs", stage="input")
    expected_index = pd.MultiIndex.from_product([dates, sectors], names=["trade_date", "sector_code"])
    if not panel.index.equals(expected_index):
        raise _fail(REASON_INPUT, "risk input cross-section is incomplete", stage="input")
    target = panel[TARGET_COLUMN].to_numpy(dtype=np.float64)
    adverse = panel[ADVERSE_COLUMN].to_numpy(dtype=np.float64)
    mature = panel[TARGET_MATURITY_COLUMN].astype(bool).to_numpy()
    if np.any(mature & (~np.isfinite(target) | ~np.isfinite(adverse))) or np.any(
        mature & ~np.isin(target, (0.0, 1.0))
    ):
        raise _fail(REASON_TARGET, "risk target values differ", stage="input")
    reasons = panel[TARGET_REASON_COLUMN]
    if any((bool(is_mature) and isinstance(reason, str) and reason) for is_mature, reason in zip(mature, reasons, strict=True)):
        raise _fail(REASON_TARGET, "mature risk target carries a failure reason", stage="input")
    if any((not bool(is_mature) and not (isinstance(reason, str) and reason)) for is_mature, reason in zip(mature, reasons, strict=True)):
        raise _fail(REASON_TARGET, "unavailable risk target lacks a reason", stage="input")
    identity = bundle["identity"]
    if identity.get("contract_version") != CONTRACT_VERSION or not all(
        isinstance(identity.get(key), str) and len(str(identity[key])) == 64
        for key in ("source_sha256", "mapping_sha256", "feature_contract_sha256", "target_contract_sha256")
    ):
        raise _fail(REASON_INPUT, "risk input authority differs", stage="input")
    return panel, dates, sectors


def _logical_input_sha256(panel: pd.DataFrame, identity: Mapping[str, Any]) -> str:
    columns = [
        *RISK_FEATURES,
        *(f"reason__{feature}" for feature in RISK_FEATURES),
        TARGET_COLUMN,
        ADVERSE_COLUMN,
        TARGET_MATURITY_COLUMN,
        TARGET_REASON_COLUMN,
    ]
    rows: list[list[Any]] = []
    for (day, code), raw in panel.loc[:, columns].sort_index().iterrows():
        values: list[Any] = [day.isoformat(), str(code)]
        for column in columns:
            value = raw[column]
            if column.startswith("reason__"):
                values.append(value if isinstance(value, str) and value else None)
            elif column == TARGET_MATURITY_COLUMN:
                values.append(bool(value))
            else:
                numeric = float(value)
                values.append(numeric if math.isfinite(numeric) else None)
        rows.append(values)
    return canonical_sha256({"columns": columns, "rows": rows, "identity": dict(identity)})


def write_input_bundle(
    bundle: Mapping[str, Any], output_root: Path, *, forbidden_roots: Sequence[Path]
) -> dict[str, Any]:
    panel, dates, sectors = validate_input_bundle(bundle)
    output = _external_new_root(output_root, forbidden_roots)
    logical_hash = _logical_input_sha256(panel, bundle["identity"])
    with tempfile.TemporaryDirectory(prefix=f".{output.name}.tmp-", dir=output.parent) as raw_temp:
        temporary = Path(raw_temp)
        panel_path = temporary / "panel.h5"
        storage = panel.copy()
        storage.index = pd.MultiIndex.from_arrays(
            [pd.to_datetime(storage.index.get_level_values("trade_date")), storage.index.get_level_values("sector_code")],
            names=["trade_date", "sector_code"],
        )
        for column in [*(f"reason__{feature}" for feature in RISK_FEATURES), TARGET_REASON_COLUMN]:
            storage[column] = storage[column].fillna("").astype(str)
        storage.to_hdf(panel_path, key="data", mode="w", format="table", data_columns=["trade_date", "sector_code"])
        body = {
            "schema_version": INPUT_MANIFEST_SCHEMA_VERSION,
            "input_schema_version": INPUT_SCHEMA_VERSION,
            "identity": dict(bundle["identity"]),
            "file": {"path": "panel.h5", "sha256": _sha256_file(panel_path)},
            "calendar_start": dates[0].isoformat(),
            "calendar_end": dates[-1].isoformat(),
            "calendar_count": len(dates),
            "sector_count": len(sectors),
            "row_count": len(panel),
            "logical_input_sha256": logical_hash,
        }
        manifest = {**body, "manifest_sha256": canonical_sha256(body)}
        (temporary / "manifest.json").write_bytes(canonical_json_bytes(manifest) + b"\n")
        readback = read_input_bundle(temporary, forbidden_roots=())
        if readback["manifest"] != manifest:
            raise _fail(REASON_READBACK, "risk input bundle readback differs", stage="writer")
        temporary.rename(output)
    return manifest


def read_input_bundle(root: Path, *, forbidden_roots: Sequence[Path]) -> dict[str, Any]:
    raw = Path(root)
    if not raw.is_absolute() or raw.is_symlink():
        raise _fail(REASON_INPUT, "risk input root is invalid", stage="reader")
    resolved = raw.resolve(strict=True)
    for forbidden in forbidden_roots:
        try:
            resolved.relative_to(Path(forbidden).resolve(strict=True))
        except ValueError:
            continue
        raise _fail(REASON_INPUT, "risk input root cannot be inside the repository", stage="reader")
    manifest_path = resolved / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail(REASON_INPUT, "risk input manifest cannot be read", stage="reader") from exc
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    expected_keys = {
        "schema_version", "input_schema_version", "identity", "file", "calendar_start", "calendar_end",
        "calendar_count", "sector_count", "row_count", "logical_input_sha256", "manifest_sha256",
    }
    if (
        set(manifest) != expected_keys
        or manifest.get("schema_version") != INPUT_MANIFEST_SCHEMA_VERSION
        or manifest.get("input_schema_version") != INPUT_SCHEMA_VERSION
        or manifest.get("manifest_sha256") != canonical_sha256(body)
    ):
        raise _fail(REASON_INPUT, "risk input manifest differs", stage="reader")
    file_entry = manifest.get("file")
    if not isinstance(file_entry, Mapping) or set(file_entry) != {"path", "sha256"} or file_entry.get("path") != "panel.h5":
        raise _fail(REASON_INPUT, "risk input file entry differs", stage="reader")
    panel_path = (resolved / "panel.h5").resolve(strict=True)
    if panel_path.parent != resolved or panel_path.is_symlink() or _sha256_file(panel_path) != file_entry.get("sha256"):
        raise _fail(REASON_INPUT, "risk input file identity differs", stage="reader")
    try:
        panel = pd.read_hdf(panel_path)
    except Exception as exc:
        raise _fail(REASON_INPUT, "risk input panel cannot be read", stage="reader") from exc
    for column in [*(f"reason__{feature}" for feature in RISK_FEATURES), TARGET_REASON_COLUMN]:
        panel[column] = panel[column].replace("", None)
    bundle = {"schema_version": INPUT_SCHEMA_VERSION, "identity": dict(manifest["identity"]), "panel": panel}
    validated, dates, sectors = validate_input_bundle(bundle)
    if (
        manifest["calendar_start"] != dates[0].isoformat()
        or manifest["calendar_end"] != dates[-1].isoformat()
        or manifest["calendar_count"] != len(dates)
        or manifest["sector_count"] != len(sectors)
        or manifest["row_count"] != len(validated)
        or manifest["logical_input_sha256"] != _logical_input_sha256(validated, bundle["identity"])
    ):
        raise _fail(REASON_READBACK, "risk input logical readback differs", stage="reader")
    return {"bundle": bundle, "manifest": manifest}


def _eligible(frame: pd.DataFrame) -> pd.Series:
    values = frame.loc[:, list(RISK_FEATURES)].to_numpy(dtype=np.float64)
    finite = np.isfinite(values)
    mandatory_indexes = [RISK_FEATURES.index(name) for name in MANDATORY_FEATURES]
    mask = finite[:, mandatory_indexes].all(axis=1) & (finite.sum(axis=1) >= MINIMUM_VALID_FEATURES)
    return pd.Series(mask, index=frame.index)


def _class_receipt(target: pd.Series) -> dict[str, Any]:
    values = target.to_numpy(dtype=np.float64)
    if not np.isfinite(values).all() or not np.isin(values, (0.0, 1.0)).all():
        raise _fail(REASON_TARGET, "risk training target differs", stage="fit")
    negative = int(np.sum(values == 0.0))
    positive = int(np.sum(values == 1.0))
    minimum = max(100, math.ceil(0.02 * len(values)))
    if min(negative, positive) < minimum:
        raise _fail(
            REASON_CLASS,
            "risk training class support is insufficient",
            stage="fit",
            negative_count=negative,
            positive_count=positive,
            minimum_per_class=minimum,
        )
    weights = {"0": len(values) / (2.0 * negative), "1": len(values) / (2.0 * positive)}
    body = {
        "row_count": len(values),
        "negative_count": negative,
        "positive_count": positive,
        "minimum_per_class": minimum,
        "resolved_class_weights": weights,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _booster(estimator: Any) -> Any:
    booster = getattr(estimator, "booster_", None)
    if booster is None:
        raise _fail(REASON_FIT, "risk estimator lacks a fitted booster", stage="fit")
    return booster


def _model_text(estimator: Any) -> str:
    booster = _booster(estimator)
    text = booster.model_to_string()
    tree_count = booster.num_trees() if hasattr(booster, "num_trees") else None
    if not isinstance(text, str) or not text or tree_count != 240:
        raise _fail(REASON_FIT, "risk model serialization or tree count differs", stage="fit", tree_count=tree_count)
    return text


def _predict(
    estimator: Any,
    features: pd.DataFrame,
    *,
    fit_identity: str,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    try:
        probabilities = np.asarray(estimator.predict_proba(features), dtype=np.float64)
        booster = _booster(estimator)
        raw_scores = np.asarray(booster.predict(features, raw_score=True), dtype=np.float64)
        contributions = np.asarray(booster.predict(features, pred_contrib=True), dtype=np.float64)
    except Exception as exc:
        raise _fail(REASON_SCORE, "risk prediction failed", stage="prediction", fit_identity=fit_identity) from exc
    if (
        probabilities.shape != (len(features), 2)
        or raw_scores.shape != (len(features),)
        or contributions.shape != (len(features), len(RISK_FEATURES) + 1)
        or not np.isfinite(probabilities).all()
        or not np.isfinite(raw_scores).all()
        or not np.isfinite(contributions).all()
        or np.any(probabilities < 0.0)
        or np.any(probabilities > 1.0)
    ):
        raise _fail(REASON_SCORE, "risk score or contribution shape/value differs", stage="prediction")
    reconstructed = contributions[:, :-1].sum(axis=1) + contributions[:, -1]
    tolerance = 1e-12 + 1e-10 * np.maximum(1.0, np.abs(raw_scores))
    if np.any(np.abs(reconstructed - raw_scores) > tolerance):
        raise _fail(REASON_SCORE, "risk log-odds contributions do not reconstruct raw score", stage="prediction")
    receipt = {
        "domain": "log_odds",
        "shape": list(contributions.shape),
        "canonical_sha256": canonical_sha256(contributions.tolist()),
        "maximum_reconstruction_error": float(np.max(np.abs(reconstructed - raw_scores), initial=0.0)),
    }
    return probabilities[:, 1], contributions, receipt


def project_risk_levels(scores: pd.Series, upstream_reasons: Mapping[tuple[date, str], str | None]) -> list[dict[str, Any]]:
    if not isinstance(scores.index, pd.MultiIndex) or tuple(scores.index.names) != ("trade_date", "sector_code"):
        raise _fail(REASON_SCORE, "risk score identity differs", stage="projection")
    rows: list[dict[str, Any]] = []
    for day, group in scores.groupby(level="trade_date", sort=True):
        if len(group) != CANONICAL_SECTOR_COUNT:
            raise _fail(REASON_COVERAGE, "risk projection denominator differs", stage="projection")
        finite_mask = np.isfinite(group.to_numpy(dtype=np.float64))
        available_count = int(finite_mask.sum())
        if available_count < MINIMUM_METRIC_SECTORS:
            for (_raw_day, raw_code), _score in group.items():
                code = str(raw_code)
                rows.append(
                    {
                        "trade_date": day.isoformat(),
                        "sector_code": code,
                        "availability": "unavailable",
                        "reason_code": REASON_COVERAGE,
                        "upstream_reason_code": upstream_reasons.get((day, code)),
                        "risk_score": None,
                        "risk_percentile": None,
                        "risk_level": None,
                        "predicted_warning": None,
                    }
                )
            continue
        finite = group[finite_mask]
        ranks = finite.rank(method="average", ascending=True)
        percentiles = (ranks - 1.0) / float(available_count - 1)
        for (_raw_day, raw_code), raw_score in group.items():
            code = str(raw_code)
            score = float(raw_score)
            base = {"trade_date": day.isoformat(), "sector_code": code}
            if not math.isfinite(score):
                reason = upstream_reasons.get((day, code))
                rows.append(
                    {
                        **base,
                        "availability": "unavailable",
                        "reason_code": reason or REASON_FEATURE,
                        "upstream_reason_code": reason,
                        "risk_score": None,
                        "risk_percentile": None,
                        "risk_level": None,
                        "predicted_warning": None,
                    }
                )
                continue
            percentile = float(percentiles.loc[(day, raw_code)])
            level = "high" if percentile >= 0.80 else "watch" if percentile >= 0.60 else "normal"
            rows.append(
                {
                    **base,
                    "availability": "available",
                    "reason_code": None,
                    "upstream_reason_code": None,
                    "risk_score": score,
                    "risk_percentile": percentile,
                    "risk_level": level,
                    "predicted_warning": level == "high",
                }
            )
    return rows


def risk_metrics(rows: Sequence[Mapping[str, Any]], target: pd.Series, sectors: Sequence[str]) -> dict[str, Any]:
    target_map = {(day, str(code)): float(value) for (day, code), value in target.items()}
    available_by_sector = {str(code): 0 for code in sectors}
    total_by_sector = {str(code): 0 for code in sectors}
    daily: list[dict[str, Any]] = []
    outcomes: list[bool] = []
    predictions: list[bool] = []
    by_day: dict[date, list[Mapping[str, Any]]] = {}
    for row in rows:
        day = date.fromisoformat(str(row["trade_date"]))
        by_day.setdefault(day, []).append(row)
    for day, day_rows in sorted(by_day.items()):
        if len(day_rows) != CANONICAL_SECTOR_COUNT:
            raise _fail(REASON_METRIC, "risk metric denominator differs", stage="metric")
        evaluable = 0
        for row in day_rows:
            code = str(row["sector_code"])
            total_by_sector[code] += 1
            actual = target_map.get((day, code))
            if row.get("availability") == "available" and actual in (0.0, 1.0):
                available_by_sector[code] += 1
                outcomes.append(bool(actual))
                predictions.append(row.get("predicted_warning") is True)
                evaluable += 1
        daily.append({"trade_date": day.isoformat(), "evaluable_count": evaluable, "metric_valid": evaluable >= 28})
    metric_dates = sum(bool(item["metric_valid"]) for item in daily)
    metric_date_ratio = metric_dates / len(daily) if daily else 0.0
    sector_coverage = {
        code: available_by_sector[code] / total_by_sector[code] if total_by_sector[code] else 0.0 for code in sectors
    }
    tp = sum(actual and predicted for actual, predicted in zip(outcomes, predictions, strict=True))
    fp = sum((not actual) and predicted for actual, predicted in zip(outcomes, predictions, strict=True))
    fn = sum(actual and (not predicted) for actual, predicted in zip(outcomes, predictions, strict=True))
    event_count = tp + fn
    predicted_count = tp + fp
    if not outcomes or event_count == 0 or predicted_count == 0:
        raise _fail(REASON_METRIC, "risk development metric is unavailable", stage="metric")
    base_rate = event_count / len(outcomes)
    precision = tp / predicted_count
    recall = tp / event_count
    precision_lift = precision - base_rate
    coverage_accepted = (
        metric_date_ratio >= MINIMUM_COVERAGE and all(value >= MINIMUM_COVERAGE for value in sector_coverage.values())
    )
    body = {
        "daily": daily,
        "evaluable_count": len(outcomes),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "base_rate": base_rate,
        "precision": precision,
        "precision_lift": precision_lift,
        "recall": recall,
        "metric_valid_date_ratio": metric_date_ratio,
        "sector_coverage": sector_coverage,
        "minimum_sector_coverage": min(sector_coverage.values()) if sector_coverage else 0.0,
        "coverage_accepted": coverage_accepted,
        "effect_accepted": precision_lift >= MINIMUM_PRECISION_LIFT and recall >= MINIMUM_RECALL,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _upstream_reasons(frame: pd.DataFrame) -> dict[tuple[date, str], str | None]:
    result: dict[tuple[date, str], str | None] = {}
    for identity, row in frame.iterrows():
        missing = [feature for feature in RISK_FEATURES if not math.isfinite(float(row[feature]))]
        reasons = [row.get(f"reason__{feature}") for feature in missing]
        reason = next((str(value) for value in reasons if isinstance(value, str) and value), None)
        result[(identity[0], str(identity[1]))] = reason
    return result


def _estimator_factory(factory: Any | None) -> Any:
    if factory is not None:
        return factory
    try:
        from lightgbm import LGBMClassifier
    except (ImportError, OSError) as exc:
        raise _fail(REASON_FIT, "lightgbm==4.6.0 is unavailable", stage="environment") from exc
    if importlib.metadata.version("lightgbm") != "4.6.0":
        raise _fail(REASON_FIT, "LightGBM version differs from approved contract", stage="environment")
    return LGBMClassifier


def _fit_one(
    frame: pd.DataFrame,
    *,
    factory: Any,
    progress: FitProgress,
    fit_identity: str,
) -> tuple[Any, pd.Series, np.ndarray, dict[str, Any]]:
    mask = _eligible(frame) & np.isfinite(frame[TARGET_COLUMN].to_numpy(dtype=np.float64))
    if not mask.any():
        raise _fail(REASON_FIT, "risk eligible training set is empty", stage="fit", fit_identity=fit_identity)
    target = frame.loc[mask, TARGET_COLUMN]
    class_receipt = _class_receipt(target)
    estimator = factory(**lightgbm_profile())
    try:
        progress.execute(fit_identity, lambda: estimator.fit(frame.loc[mask, list(RISK_FEATURES)], target))
    except RiskL1G2BError:
        raise
    except Exception as exc:
        raise _fail(REASON_FIT, "risk model fit failed", stage="fit", fit_identity=fit_identity) from exc
    model_text = _model_text(estimator)
    try:
        leaf = _leaf_date_coverage(
            estimator,
            frame.loc[mask, list(RISK_FEATURES)],
            frame.loc[mask].index.get_level_values("trade_date"),
            fit_identity=fit_identity,
        )
    except RotationL1G2AError as exc:
        raise _fail(
            REASON_LEAF,
            "risk leaf date coverage is insufficient",
            stage="fit",
            fit_identity=fit_identity,
        ) from exc
    return estimator, target, mask.to_numpy(dtype=bool), {
        "fit_identity": fit_identity,
        "fit_row_count": int(mask.sum()),
        "class_receipt": class_receipt,
        "leaf_date_coverage": leaf,
        "model_sha256": canonical_sha256(model_text),
    }


def run_process(
    bundle: Mapping[str, Any],
    *,
    producer_commit: str,
    process_index: int,
    estimator_factory: Any | None = None,
    runtime_validator: Any = require_deterministic_runtime,
) -> dict[str, Any]:
    if (
        process_index not in (1, 2)
        or not isinstance(producer_commit, str)
        or len(producer_commit) != 40
        or any(character not in "0123456789abcdef" for character in producer_commit)
    ):
        raise _fail(REASON_INPUT, "risk process identity differs", stage="input")
    panel, calendar, sectors = validate_input_bundle(bundle)
    try:
        ranked = cross_section_rank_features(panel, continuous_features=RISK_FEATURES)
    except RotationL1G2AError as exc:
        raise _fail(REASON_FEATURE, "risk feature ranking failed", stage="feature") from exc
    try:
        runtime = runtime_validator()
    except Exception as exc:
        raise _fail(REASON_FIT, "risk deterministic runtime differs", stage="environment") from exc
    factory = _estimator_factory(estimator_factory)
    progress = FitProgress(planned=6)
    predictions: list[pd.Series] = []
    contributions: dict[tuple[date, str], list[float]] = {}
    model_hashes: dict[tuple[date, str], str] = {}
    fold_receipts: list[dict[str, Any]] = []
    try:
        for fold in fold_slices(calendar, horizon=FIXED_HORIZON):
            train = ranked.loc[(list(fold.train_dates), slice(None)), :]
            validation = ranked.loc[(list(fold.validation_dates), slice(None)), :]
            estimator, _target, _mask, fit_receipt = _fit_one(
                train,
                factory=factory,
                progress=progress,
                fit_identity=f"{fold.name}:risk-gbdt",
            )
            validation_mask = _eligible(validation)
            score = pd.Series(np.nan, index=validation.index, dtype=np.float64)
            contribution_receipt: dict[str, Any] | None = None
            if validation_mask.any():
                feature_frame = validation.loc[validation_mask, list(RISK_FEATURES)]
                probabilities, contribution_values, contribution_receipt = _predict(
                    estimator, feature_frame, fit_identity=f"{fold.name}:risk-gbdt"
                )
                score.loc[validation_mask] = probabilities
                for identity, values in zip(feature_frame.index, contribution_values, strict=True):
                    contributions[(identity[0], str(identity[1]))] = [float(value) for value in values]
            predictions.append(score)
            for identity in validation.index:
                model_hashes[(identity[0], str(identity[1]))] = str(fit_receipt["model_sha256"])
            fold_receipts.append(
                {
                    **fold.receipt(),
                    **fit_receipt,
                    "prediction_row_count": int(validation_mask.sum()),
                    "feature_contributions": contribution_receipt,
                }
            )
        all_scores = pd.concat(predictions).sort_index()
        projected = project_risk_levels(all_scores, _upstream_reasons(ranked.loc[all_scores.index]))
        projected_by_key = {
            (date.fromisoformat(str(row["trade_date"])), str(row["sector_code"])): row for row in projected
        }
        oof_rows: list[dict[str, Any]] = []
        position = {day: index for index, day in enumerate(calendar)}
        for identity in all_scores.index:
            day, raw_code = identity
            code = str(raw_code)
            base = dict(projected_by_key[(day, code)])
            model_hash = model_hashes.get((day, code))
            if position[day] == 0 or not isinstance(model_hash, str):
                raise _fail(REASON_SCORE, "risk OOF lineage differs", stage="prediction")
            base.update(
                {
                    "as_of_date": calendar[position[day] - 1].isoformat(),
                    "model_hash": model_hash,
                    "feature_contributions": contributions.get((day, code)),
                }
            )
            if base["availability"] == "available" and base["feature_contributions"] is None:
                raise _fail(REASON_SCORE, "available risk row lacks contributions", stage="prediction")
            oof_rows.append(base)
        metrics = risk_metrics(projected, ranked.loc[all_scores.index, TARGET_COLUMN], sectors)
        if not metrics["coverage_accepted"]:
            raise _fail(REASON_COVERAGE, "risk development coverage failed", stage="research_product_gate")
        mature_dates = tuple(
            day
            for day in calendar
            if bool(ranked.loc[(day, slice(None)), TARGET_MATURITY_COLUMN].astype(bool).all())
        )
        if len(mature_dates) < 504:
            raise _fail(REASON_TARGET, "risk full-development target history is incomplete", stage="final_fit")
        final_dates = mature_dates[-504:]
        final_train = ranked.loc[(list(final_dates), slice(None)), :]
        final_estimator, _target, _mask, final_receipt = _fit_one(
            final_train,
            factory=factory,
            progress=progress,
            fit_identity="full-development:risk-gbdt",
        )
        final_model_text = _model_text(final_estimator)
    except Exception as exc:
        if isinstance(exc, RiskL1G2BError):
            evidence = {**exc.evidence, "fit_progress": progress.receipt()}
            raise RiskL1G2BError(exc.reason_code, str(exc), stage=exc.stage, evidence=evidence) from exc
        raise
    if progress.receipt() != {"planned": 6, "started": 6, "completed": 6, "failed": 0, "active_fit": None}:
        raise _fail(REASON_FIT, "risk fit progress differs", stage="final_fit", fit_progress=progress.receipt())
    model_hash = canonical_sha256(final_model_text)
    # Product-surface availability requires repository, API and real-UI readback.
    # The offline executor cannot truthfully claim that closure.
    research_status = "NOT_AVAILABLE"
    capability_status = (
        "RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED"
        if metrics["effect_accepted"]
        else "NOT_AVAILABLE"
    )
    payload = {
        "contract_version": CONTRACT_VERSION,
        "producer_commit": producer_commit,
        "runtime_identity": runtime,
        "profile": lightgbm_profile(),
        "input_identity": dict(bundle["identity"]),
        "folds": fold_receipts,
        "metrics": metrics,
        "oof_prediction_rows": oof_rows,
        "oof_prediction_rows_sha256": canonical_sha256(oof_rows),
        "final_model": {**final_receipt, "model_sha256": model_hash},
        "fit_progress": progress.receipt(),
        "fit_count": 6,
        "research_product_gate": {
            "passed": True,
            "effect_threshold_applied": False,
            "product_readback_pending": True,
        },
        "risk_l1_research_surface_status": research_status,
        "risk_l1_capability_status": capability_status,
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "tail_accessed": False,
        "target_accessed_for_single_date": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    body = {
        "schema_version": PROCESS_SCHEMA_VERSION,
        "process_index": process_index,
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": canonical_sha256(payload),
        "final_model_text": final_model_text,
    }
    return {**body, "report_sha256": canonical_sha256(body)}


def close_processes(first: Mapping[str, Any], second: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    children = (first, second)
    for expected_index, child in enumerate(children, start=1):
        body = {key: value for key, value in child.items() if key != "report_sha256"}
        if (
            child.get("schema_version") != PROCESS_SCHEMA_VERSION
            or child.get("process_index") != expected_index
            or child.get("report_sha256") != canonical_sha256(body)
            or child.get("reproducibility_payload_sha256")
            != canonical_sha256(child.get("reproducibility_payload"))
        ):
            raise _fail(REASON_REPRODUCIBILITY, "risk child receipt differs", stage="closure")
    if (
        first.get("reproducibility_payload_sha256") != second.get("reproducibility_payload_sha256")
        or first.get("final_model_text") != second.get("final_model_text")
    ):
        raise _fail(REASON_REPRODUCIBILITY, "risk fresh-process payloads differ", stage="closure")
    payload = first["reproducibility_payload"]
    model_body = {
        "schema_version": MODEL_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "model_hash": payload["final_model"]["model_sha256"],
        "model_text": first["final_model_text"],
        "feature_names": list(RISK_FEATURES),
        "profile": payload["profile"],
        "input_identity": payload["input_identity"],
        "development_metrics": payload["metrics"],
        "risk_l1_research_surface_status": payload["risk_l1_research_surface_status"],
        "risk_l1_capability_status": payload["risk_l1_capability_status"],
        "forward_power_status": payload["forward_power_status"],
        "forward_confirmation": payload["forward_confirmation"],
        "advisory_status": payload["advisory_status"],
        "tail_accessed": False,
    }
    model = {**model_body, "artifact_sha256": canonical_sha256(model_body)}
    acceptance_body = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "status": "development_complete",
        "child_report_sha256": [first["report_sha256"], second["report_sha256"]],
        "reproducibility_payload_sha256": first["reproducibility_payload_sha256"],
        "model_artifact_sha256": model["artifact_sha256"],
        "fit_count": 12,
        "metrics": payload["metrics"],
        "research_product_gate": payload["research_product_gate"],
        "risk_l1_research_surface_status": payload["risk_l1_research_surface_status"],
        "risk_l1_capability_status": payload["risk_l1_capability_status"],
        "forward_power_status": payload["forward_power_status"],
        "forward_confirmation": payload["forward_confirmation"],
        "advisory_status": payload["advisory_status"],
        "tail_accessed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**acceptance_body, "acceptance_sha256": canonical_sha256(acceptance_body)}, model


@dataclass(frozen=True)
class LoadedRiskModel:
    booster: Any
    model_hash: str
    artifact: Mapping[str, Any]


def load_model(artifact: Mapping[str, Any], *, booster_factory: Any | None = None) -> LoadedRiskModel:
    body = {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    if (
        artifact.get("schema_version") != MODEL_SCHEMA_VERSION
        or artifact.get("contract_version") != CONTRACT_VERSION
        or artifact.get("feature_names") != list(RISK_FEATURES)
        or artifact.get("profile") != lightgbm_profile()
        or artifact.get("artifact_sha256") != canonical_sha256(body)
        or artifact.get("model_hash") != canonical_sha256(artifact.get("model_text"))
        or artifact.get("tail_accessed") is not False
    ):
        raise _fail(REASON_INPUT, "risk model artifact differs", stage="model_load")
    if booster_factory is None:
        try:
            from lightgbm import Booster
        except (ImportError, OSError) as exc:
            raise _fail(REASON_FIT, "lightgbm==4.6.0 is unavailable", stage="model_load") from exc
        booster_factory = Booster
    try:
        booster = booster_factory(model_str=str(artifact["model_text"]))
    except Exception as exc:
        raise _fail(REASON_FIT, "risk model cannot be loaded", stage="model_load") from exc
    return LoadedRiskModel(booster=booster, model_hash=str(artifact["model_hash"]), artifact=dict(artifact))


def predict_single_date(
    feature_frame: pd.DataFrame,
    *,
    trade_date: date,
    as_of_date: date,
    model: LoadedRiskModel,
) -> list[dict[str, Any]]:
    if as_of_date >= trade_date or any(not isinstance(value, str) for value in feature_frame.index):
        raise _fail(REASON_INPUT, "risk single-date identity differs", stage="single_date")
    if len(feature_frame) != CANONICAL_SECTOR_COUNT or feature_frame.index.has_duplicates:
        raise _fail(REASON_COVERAGE, "risk single-date denominator differs", stage="single_date")
    missing_columns = set(RISK_FEATURES) - set(feature_frame.columns)
    if missing_columns:
        raise _fail(REASON_FEATURE, "risk single-date features differ", stage="single_date", missing=sorted(missing_columns))
    frame = feature_frame.copy().sort_index()
    frame.index = pd.MultiIndex.from_arrays(
        [[trade_date] * len(frame), frame.index], names=["trade_date", "sector_code"]
    )
    ranked = cross_section_rank_features(frame, continuous_features=RISK_FEATURES)
    mask = _eligible(ranked)
    scores = pd.Series(np.nan, index=ranked.index, dtype=np.float64)
    contributions: dict[tuple[date, str], list[float]] = {}
    if mask.any():
        features = ranked.loc[mask, list(RISK_FEATURES)]
        try:
            probabilities = np.asarray(model.booster.predict(features), dtype=np.float64)
            raw_scores = np.asarray(model.booster.predict(features, raw_score=True), dtype=np.float64)
            values = np.asarray(model.booster.predict(features, pred_contrib=True), dtype=np.float64)
        except Exception as exc:
            raise _fail(REASON_SCORE, "risk single-date prediction failed", stage="single_date") from exc
        if probabilities.shape != (len(features),) or np.any(~np.isfinite(probabilities)) or np.any(
            (probabilities < 0.0) | (probabilities > 1.0)
        ):
            raise _fail(REASON_SCORE, "risk single-date score differs", stage="single_date")
        reconstructed = values[:, :-1].sum(axis=1) + values[:, -1]
        tolerance = 1e-12 + 1e-10 * np.maximum(1.0, np.abs(raw_scores))
        if values.shape != (len(features), len(RISK_FEATURES) + 1) or np.any(~np.isfinite(values)) or np.any(
            np.abs(reconstructed - raw_scores) > tolerance
        ):
            raise _fail(REASON_SCORE, "risk single-date contributions differ", stage="single_date")
        scores.loc[mask] = probabilities
        for identity, row in zip(features.index, values, strict=True):
            contributions[(identity[0], str(identity[1]))] = [float(value) for value in row]
    projected = project_risk_levels(scores, _upstream_reasons(ranked))
    output: list[dict[str, Any]] = []
    for row in projected:
        key = (trade_date, str(row["sector_code"]))
        output.append(
            {
                **row,
                "as_of_date": as_of_date.isoformat(),
                "model_hash": model.model_hash,
                "feature_contributions": contributions.get(key),
                "model_fit_count": 0,
                "target_columns_read": False,
                "tail_accessed": False,
            }
        )
    return output


__all__ = [
    "ACCEPTANCE_SCHEMA_VERSION",
    "CONTRACT_VERSION",
    "INPUT_SCHEMA_VERSION",
    "MODEL_SCHEMA_VERSION",
    "RISK_FEATURES",
    "RiskL1G2BError",
    "close_processes",
    "lightgbm_profile",
    "load_model",
    "predict_single_date",
    "project_risk_levels",
    "read_input_bundle",
    "risk_metrics",
    "run_process",
    "validate_input_bundle",
    "write_input_bundle",
]
