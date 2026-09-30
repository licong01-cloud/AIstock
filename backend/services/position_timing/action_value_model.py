"""Small local two-head model bundles, not a model server or training daemon.

Only JSON and LightGBM's native text format are persisted; no executable pickle.
The public read path verifies identity and time before importing a model library.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import importlib.metadata
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    ActionValueError, CORE_INFORMATION_BLOCK, FEATURE_ORDER,
    FEATURE_SPEC_SHA256, POLICY_SHA256, TZ, cutoff_on,
    feature_contract, policy_sha256_for, require_causal,
)
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import POSITION_TIMING_L2_RESEARCH_CONTRACT_V1, canonical_json_bytes, canonical_sha256, validate_sha256


MODEL_SCHEMA = "position_timing_local_model_v2"
HEADS = ("ENTRY_ACTION_VALUE_V2", "EXIT_ACTION_VALUE_V2")
STRUCTURAL_MISSING = {"holding_age", "unrealized_return_bps"}


def estimator_parameters() -> dict[str, Any]:
    parameters = dict(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.models[1].parameters)
    if parameters.pop("early_stopping") is not False:
        raise ActionValueError("MODEL_SPEC_DRIFT")
    return parameters


def _lightgbm():
    try:
        version = importlib.metadata.version("lightgbm")
        if version != "4.6.0":
            raise ActionValueError("MODEL_ENVIRONMENT_MISMATCH", lightgbm=version)
        import lightgbm
        return lightgbm
    except (ImportError, importlib.metadata.PackageNotFoundError) as exc:
        raise ActionValueError("MODEL_DEPENDENCY_UNAVAILABLE", dependency="lightgbm==4.6.0") from exc


def numeric_matrix(
    frame: pd.DataFrame,
    medians: Mapping[str, float] | None = None,
    *,
    feature_order: Sequence[str] = FEATURE_ORDER,
) -> tuple[pd.DataFrame, dict[str, float]]:
    feature_order = tuple(feature_order)
    if tuple(frame.columns) != feature_order:
        raise ActionValueError("FEATURE_ORDER_MISMATCH")
    matrix = frame.apply(pd.to_numeric, errors="raise").replace([np.inf, -np.inf], np.nan)
    for field, mask in (("holding_age", "holding_age_missing"), ("unrealized_return_bps", "entry_cost_missing")):
        if (not matrix[mask].isin([0, 1]).all()
                or not matrix[field].isna().equals(matrix[mask].eq(1))):
            raise ActionValueError("STRUCTURAL_MISSING_MASK_INVALID", field=field)
    if medians is None:
        values = matrix.median(axis=0)
        missing = set(values.index[values.isna()])
        if missing - STRUCTURAL_MISSING:
            raise ActionValueError("REQUIRED_FEATURE_ALL_MISSING", features=sorted(missing - STRUCTURAL_MISSING))
        for field in missing:
            mask = "holding_age_missing" if field == "holding_age" else "entry_cost_missing"
            if not matrix[mask].eq(1).all():
                raise ActionValueError("STRUCTURAL_MISSING_MASK_INVALID", field=field)
            values[field] = 0.0  # missing feature, not a fabricated source observation
        medians = {name: float(values[name]) for name in feature_order}
    if set(medians) != set(feature_order) or not all(np.isfinite(value) for value in medians.values()):
        raise ActionValueError("PREPROCESSOR_IDENTITY_INVALID")
    return matrix.fillna(dict(medians)), dict(medians)


def training_rows_asof(
    rows: pd.DataFrame,
    cutoff: datetime,
    *,
    feature_order: Sequence[str] = FEATURE_ORDER,
) -> pd.DataFrame:
    if cutoff.tzinfo is None:
        raise ActionValueError("TRAINING_CUTOFF_NAIVE")
    required = {"decision_as_of", "label_available_at", "objective", "net_action_value_bps", *feature_order}
    if not required.issubset(rows):
        raise ActionValueError("TRAINING_SCHEMA_MISSING", missing=sorted(required - set(rows)))
    # utc=True alone would silently interpret naive timestamps as UTC.
    for column in ("decision_as_of", "label_available_at"):
        if rows[column].isna().any() or any(pd.Timestamp(value).tzinfo is None for value in rows[column]):
            raise ActionValueError("TRAINING_LABEL_TIME_INVALID", column=column)
    decisions = pd.to_datetime(rows["decision_as_of"], utc=True)
    availability = pd.to_datetime(rows["label_available_at"], utc=True)
    if (availability <= decisions).any():
        raise ActionValueError("TRAINING_LABEL_INTERVAL_INVALID")
    eligible = (decisions < cutoff) & (availability <= cutoff)
    result = rows.loc[eligible].copy()
    if result.empty or set(result.objective.unique()) != set(HEADS):
        raise ActionValueError("TRAINING_OBJECTIVE_UNAVAILABLE")
    if not np.isfinite(pd.to_numeric(result.net_action_value_bps)).all():
        raise ActionValueError("TRAINING_TARGET_NON_FINITE")
    return result


@dataclass
class LocalActionModel:
    metadata: dict[str, Any]
    boosters: dict[str, Any]
    published_at: datetime | None = None

    def predict(self, frame: pd.DataFrame, objectives: Sequence[str], *, decision_as_of: datetime) -> np.ndarray:
        require_causal(datetime.fromisoformat(self.metadata["available_at"]), decision_as_of, field="model.available_at")
        if self.metadata["temporal_mode"] == "LIVE_FINAL_FIT":
            if self.published_at is None:
                raise ActionValueError("LIVE_MODEL_NOT_PUBLISHED")
            require_causal(self.published_at, decision_as_of, field="model.published_at")
        if len(frame) != len(objectives) or not set(objectives).issubset(HEADS):
            raise ActionValueError("PREDICTION_OBJECTIVE_INVALID")
        information_block = self.metadata.get("information_block", CORE_INFORMATION_BLOCK)
        market_features, feature_order, feature_spec_sha256 = feature_contract(information_block)
        if (
            tuple(self.metadata.get("feature_order", ())) != feature_order
            or self.metadata.get("feature_spec_sha256") != feature_spec_sha256
            or self.metadata.get("policy_sha256") != policy_sha256_for(information_block)
        ):
            raise ActionValueError("MODEL_FEATURE_OR_POLICY_IDENTITY_MISMATCH")
        if tuple(frame.columns) != feature_order:
            raise ActionValueError("FEATURE_ORDER_MISMATCH")
        # A missing current source feature is not repaired by training medians.
        if len(frame) and not np.isfinite(frame.loc[:, market_features].to_numpy(dtype=float)).all():
            code = (
                "CURRENT_CORE_FEATURE_UNAVAILABLE"
                if information_block == CORE_INFORMATION_BLOCK
                else "CURRENT_OPTIONAL_FEATURE_UNAVAILABLE"
            )
            raise ActionValueError(code, information_block=information_block)
        result = np.empty(len(frame), dtype=float)
        objective_array = np.asarray(objectives)
        for head in HEADS:
            indexes = np.flatnonzero(objective_array == head)
            if not len(indexes):
                continue
            matrix, _ = numeric_matrix(
                frame.iloc[indexes],
                self.metadata["heads"][head]["medians"],
                feature_order=feature_order,
            )
            result[indexes] = self.boosters[head].predict(matrix, num_threads=1)
        if not np.isfinite(result).all():
            raise ActionValueError("MODEL_PREDICTION_NON_FINITE")
        return result


def fit_local_model(rows: pd.DataFrame, *, cutoff: datetime, available_at: datetime,
                     source_sha256: str, request_sha256: str, source_commit: str,
                     temporal_mode: str = "HISTORICAL_REPLAY",
                     information_block: str = CORE_INFORMATION_BLOCK) -> LocalActionModel:
    require_causal(cutoff, available_at, field="training.cutoff")
    for value in (source_sha256, request_sha256):
        validate_sha256(value, field="model source identity")
    if len(source_commit) != 40 or any(ch not in "0123456789abcdef" for ch in source_commit):
        raise ActionValueError("MODEL_CODE_IDENTITY_INVALID")
    if temporal_mode not in {"HISTORICAL_REPLAY", "LIVE_FINAL_FIT"}:
        raise ActionValueError("MODEL_TEMPORAL_MODE_INVALID")
    market_features, feature_order, feature_spec_sha256 = feature_contract(information_block)
    policy_sha256 = policy_sha256_for(information_block)
    lightgbm = _lightgbm()
    training = training_rows_asof(rows, cutoff, feature_order=feature_order)
    heads, boosters = {}, {}
    for head in HEADS:
        selected = training.loc[training.objective.eq(head)]
        matrix, medians = numeric_matrix(selected.loc[:, feature_order], feature_order=feature_order)
        estimator = lightgbm.LGBMRegressor(**estimator_parameters())
        estimator.fit(matrix, selected.net_action_value_bps.to_numpy(dtype=float))
        boosters[head] = estimator.booster_
        model_text = estimator.booster_.model_to_string()
        heads[head] = {
            "medians": medians, "training_rows": len(selected),
            "label_available_max": pd.to_datetime(selected.label_available_at, utc=True).max().isoformat(),
            "parameters": estimator.get_params(deep=False),
            "text_sha256": hashlib.sha256(model_text.encode("utf-8")).hexdigest(),
        }
    metadata = {
        "schema_version": MODEL_SCHEMA, "feature_order": feature_order,
        "feature_spec_sha256": feature_spec_sha256, "policy_sha256": policy_sha256,
        "source_sha256": source_sha256, "request_sha256": request_sha256,
        "source_commit": source_commit, "training_cutoff": cutoff.isoformat(),
        "available_at": available_at.isoformat(), "temporal_mode": temporal_mode,
        "package_version": lightgbm.__version__, "heads": heads,
        "interpretation": "MODEL_ESTIMATE_NOT_STOCK_CONFIDENCE",
    }
    if information_block != CORE_INFORMATION_BLOCK:
        metadata.update(
            {
                "information_block": information_block,
                "required_market_features": market_features,
            }
        )
    if temporal_mode == "LIVE_FINAL_FIT":
        completed = datetime.now(TZ)
        require_causal(cutoff, completed, field="training.cutoff")
        metadata["trained_at"] = completed.isoformat()
        metadata["available_at"] = max(available_at, completed).isoformat()
    metadata["model_sha256"] = canonical_sha256(metadata)
    return LocalActionModel(metadata, boosters)


def write_local_model(model: LocalActionModel, *, timing_root: Path) -> Path:
    import json

    digest = validate_sha256(model.metadata["model_sha256"], field="model_sha256")
    if canonical_sha256({key: value for key, value in model.metadata.items() if key != "model_sha256"}) != digest:
        raise ActionValueError("MODEL_MANIFEST_IDENTITY_MISMATCH")
    folder = (timing_root.resolve() / "models_v2" / digest).resolve()
    if not folder.is_relative_to(timing_root.resolve()):
        raise ActionValueError("MODEL_PATH_OUTSIDE_OWNER")
    with _exclusive_file_lock(timing_root.resolve() / "locks" / f"model-v2-{digest}.lock"):
        for name in ("manifest.json", "publication.json", *(f"{head}.txt" for head in HEADS)):
            if not (folder / name).resolve().is_relative_to(folder):
                raise ActionValueError("MODEL_PATH_OUTSIDE_OWNER")
        for head in HEADS:
            text = model.boosters[head].model_to_string().encode("utf-8")
            if hashlib.sha256(text).hexdigest() != model.metadata["heads"][head]["text_sha256"]:
                raise ActionValueError("MODEL_TEXT_IDENTITY_MISMATCH")
            PositionTimingArtifactStore._publish_immutable(folder / f"{head}.txt", text)
        # Manifest last: an interrupted partial bundle is never readable as complete.
        PositionTimingArtifactStore._publish_immutable(folder / "manifest.json", canonical_json_bytes(model.metadata))
        if model.metadata["temporal_mode"] == "LIVE_FINAL_FIT":
            receipt = folder / "publication.json"
            if not receipt.exists():
                payload = {"model_sha256": digest, "published_at": datetime.now(TZ).isoformat()}
                payload["publication_sha256"] = canonical_sha256(payload)
                PositionTimingArtifactStore._publish_immutable(receipt, canonical_json_bytes(payload))
            payload = json.loads(receipt.read_text(encoding="utf-8"))
            model.published_at = _publication_time(payload, model.metadata)
    return folder


def read_local_model(*, timing_root: Path, model_sha256: str, decision_as_of: datetime,
                     allow_historical: bool = False) -> LocalActionModel:
    import json

    validate_sha256(model_sha256, field="model_sha256")
    folder = (timing_root.resolve() / "models_v2" / model_sha256).resolve()
    if not folder.is_relative_to(timing_root.resolve()):
        raise ActionValueError("MODEL_PATH_OUTSIDE_OWNER")
    try:
        manifest = folder / "manifest.json"
        if not manifest.resolve().is_relative_to(folder):
            raise ActionValueError("MODEL_PATH_OUTSIDE_OWNER")
        metadata = json.loads(manifest.read_text(encoding="utf-8"))
        identity = {key: value for key, value in metadata.items() if key != "model_sha256"}
        if canonical_sha256(identity) != model_sha256 or metadata["model_sha256"] != model_sha256:
            raise ActionValueError("MODEL_MANIFEST_IDENTITY_MISMATCH")
        if (metadata["schema_version"] != MODEL_SCHEMA or tuple(metadata["feature_order"]) != FEATURE_ORDER
                or metadata["feature_spec_sha256"] != FEATURE_SPEC_SHA256 or metadata["policy_sha256"] != POLICY_SHA256):
            raise ActionValueError("MODEL_SCHEMA_OR_POLICY_DRIFT")
        if metadata["temporal_mode"] != "LIVE_FINAL_FIT" and not allow_historical:
            raise ActionValueError("HISTORICAL_MODEL_NOT_SERVABLE")
        require_causal(datetime.fromisoformat(metadata["available_at"]), decision_as_of, field="model.available_at")
        published_at = None
        if metadata["temporal_mode"] == "LIVE_FINAL_FIT":
            receipt = folder / "publication.json"
            if not receipt.resolve().is_relative_to(folder):
                raise ActionValueError("MODEL_PATH_OUTSIDE_OWNER")
            published_at = _publication_time(json.loads(receipt.read_text(encoding="utf-8")), metadata)
            require_causal(published_at, decision_as_of, field="model.published_at")
        texts = {}
        for head in HEADS:
            source = folder / f"{head}.txt"
            if not source.resolve().is_relative_to(folder):
                raise ActionValueError("MODEL_PATH_OUTSIDE_OWNER")
            text = source.read_bytes()
            if hashlib.sha256(text).hexdigest() != metadata["heads"][head]["text_sha256"]:
                raise ActionValueError("MODEL_TEXT_IDENTITY_MISMATCH")
            texts[head] = text.decode("utf-8")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, ActionValueError):
            raise
        raise ActionValueError("MODEL_ARTIFACT_UNAVAILABLE", cause=type(exc).__name__) from exc
    library = _lightgbm()
    return LocalActionModel(metadata, {head: library.Booster(model_str=texts[head]) for head in HEADS}, published_at)


def _publication_time(payload: Mapping[str, Any], metadata: Mapping[str, Any]) -> datetime:
    identity = {key: value for key, value in payload.items() if key != "publication_sha256"}
    if payload.get("model_sha256") != metadata["model_sha256"] or payload.get("publication_sha256") != canonical_sha256(identity):
        raise ActionValueError("MODEL_PUBLICATION_IDENTITY_MISMATCH")
    published_at = datetime.fromisoformat(payload["published_at"])
    require_causal(datetime.fromisoformat(metadata["trained_at"]), published_at, field="model.trained_at")
    return published_at


def monthly_training_windows(calendar: Sequence[date], *, initial_sessions: int = 756) -> tuple[dict[str, Any], ...]:
    if initial_sessions != 756:
        raise ActionValueError("TRAINING_SCHEDULE_DRIFT")
    days = tuple(calendar)
    if list(days) != sorted(set(days)):
        raise ActionValueError("TRAINING_CALENDAR_INVALID")
    result = []
    for index in range(initial_sessions - 1, len(days) - 2):
        if days[index].month == days[index + 1].month:
            continue
        result.append({"cutoff": cutoff_on(days[index]), "available_at": cutoff_on(days[index + 1]),
                       "first_target_trade_date": days[index + 2]})
    return tuple(result)
