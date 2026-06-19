"""Offline orthogonality metrics for finalized multi-alpha legs.

The service is intentionally read-only: it pulls persisted pred.pkl artifacts
through ModelStoreService and computes pairwise diversity diagnostics in memory.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from backend.services.model_store import ModelStoreService, PredictionStoreError


class MultiAlphaOrthogonalityError(RuntimeError):
    """Raised when orthogonality cannot be computed from explicit inputs."""


@dataclass(frozen=True)
class PredictionLeg:
    run_id: str
    frame: pd.DataFrame
    model: str | None = None
    factor_set: str | None = None


PredictionLoader = Callable[[str], PredictionLeg]


class MultiAlphaOrthogonalityService:
    """Compute score-correlation and top-k overlap matrices for run legs."""

    def __init__(
        self,
        *,
        model_store: ModelStoreService | None = None,
        prediction_loader: PredictionLoader | None = None,
    ) -> None:
        self._model_store = model_store or ModelStoreService()
        self._prediction_loader = prediction_loader

    def compute(self, *, run_ids: Sequence[str], k: int = 25) -> dict[str, Any]:
        selected = _normalize_run_ids(run_ids)
        if len(selected) < 2:
            raise MultiAlphaOrthogonalityError("at least two run_ids are required")
        top_k = _normalize_k(k)

        legs = [self._load_leg(run_id) for run_id in selected]
        common_dates = _common_dates(legs)
        if not common_dates:
            raise MultiAlphaOrthogonalityError("prediction legs have no common trade_date")

        pred_corr_matrix = _pair_matrix(legs, common_dates, _daily_spearman_mean)
        jaccard_matrix = _pair_matrix(legs, common_dates, lambda left, right: _daily_jaccard_mean(left, right, top_k))
        return {
            "schema_version": "multi_alpha_orthogonality_v1",
            "legs": [leg.run_id for leg in legs],
            "k": top_k,
            "pred_corr_matrix": pred_corr_matrix,
            "jaccard_matrix": jaccard_matrix,
            "n_common_dates": len(common_dates),
            "common_date_start": common_dates[0].isoformat(),
            "common_date_end": common_dates[-1].isoformat(),
            "per_leg": {
                leg.run_id: {
                    "run_id": leg.run_id,
                    "model": leg.model,
                    "factor_set": leg.factor_set,
                    "n_dates": int(leg.frame["trade_date"].nunique()),
                    "row_count": int(len(leg.frame)),
                    "instrument_count": int(leg.frame["instrument"].nunique()),
                }
                for leg in legs
            },
        }

    def _load_leg(self, run_id: str) -> PredictionLeg:
        if self._prediction_loader is not None:
            return self._prediction_loader(run_id)
        try:
            path = self._model_store.prediction_path(run_id=run_id)
            pointer = self._model_store.get_pointer(run_id=run_id)
        except PredictionStoreError as exc:
            raise MultiAlphaOrthogonalityError(f"prediction artifact missing for run_id={run_id}: {exc}") from exc
        frame = _prediction_frame_from_pickle(path, run_id=run_id)
        run_meta = pointer.get("run") if isinstance(pointer.get("run"), Mapping) else {}
        return PredictionLeg(
            run_id=run_id,
            frame=frame,
            model=_first_text(run_meta, "model_type", "model_family"),
            factor_set=_first_text(run_meta, "factor_set_hash"),
        )


def _normalize_run_ids(run_ids: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in run_ids:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _normalize_k(value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise MultiAlphaOrthogonalityError(f"k must be an integer, got {value!r}") from exc
    if parsed < 1 or parsed > 500:
        raise MultiAlphaOrthogonalityError(f"k must be between 1 and 500, got {parsed}")
    return parsed


def _prediction_frame_from_pickle(path: Any, *, run_id: str) -> pd.DataFrame:
    try:
        obj = pd.read_pickle(path)
    except Exception as exc:
        raise MultiAlphaOrthogonalityError(f"failed to read pred.pkl for run_id={run_id}: {type(exc).__name__}: {exc}") from exc
    return normalize_prediction_frame(obj, run_id=run_id)


def normalize_prediction_frame(obj: Any, *, run_id: str) -> pd.DataFrame:
    """Normalize Qlib pred.pkl shapes to trade_date/instrument/score rows."""

    if isinstance(obj, pd.Series):
        frame = obj.to_frame(name="score")
    elif isinstance(obj, pd.DataFrame):
        frame = obj.copy()
    else:
        frame = pd.DataFrame(obj)

    score_col = _score_column(frame)
    if score_col is None:
        raise MultiAlphaOrthogonalityError(f"pred.pkl for run_id={run_id} has no score column")

    frame = frame.copy()
    if isinstance(frame.index, pd.MultiIndex):
        index_names = [str(name or "").lower() for name in frame.index.names]
        date_level = _find_level(index_names, ("datetime", "date", "trade_date", "time"))
        inst_level = _find_level(index_names, ("instrument", "symbol", "ts_code", "code"))
        if date_level is None or inst_level is None:
            if frame.index.nlevels >= 2:
                date_level = 0 if date_level is None else date_level
                inst_level = 1 if inst_level is None else inst_level
            else:
                raise MultiAlphaOrthogonalityError(f"pred.pkl for run_id={run_id} MultiIndex lacks date/instrument levels")
        out = pd.DataFrame(
            {
                "trade_date": frame.index.get_level_values(date_level),
                "instrument": frame.index.get_level_values(inst_level),
                "score": frame[score_col].to_numpy(),
            }
        )
    else:
        date_col = _column_by_names(frame, ("datetime", "date", "trade_date", "time"))
        inst_col = _column_by_names(frame, ("instrument", "symbol", "ts_code", "code"))
        if date_col is None and isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index().rename(columns={frame.index.name or "index": "trade_date"})
            date_col = "trade_date"
        if date_col is None or inst_col is None:
            raise MultiAlphaOrthogonalityError(f"pred.pkl for run_id={run_id} lacks date/instrument columns")
        out = frame[[date_col, inst_col, score_col]].rename(
            columns={date_col: "trade_date", inst_col: "instrument", score_col: "score"}
        )

    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.date
    out["instrument"] = out["instrument"].astype(str)
    out["score"] = pd.to_numeric(out["score"], errors="coerce")
    out = out.dropna(subset=["trade_date", "instrument", "score"])
    if out.empty:
        raise MultiAlphaOrthogonalityError(f"pred.pkl for run_id={run_id} has no valid score rows")
    return out.groupby(["trade_date", "instrument"], as_index=False, sort=True)["score"].mean()


def _score_column(frame: pd.DataFrame) -> Any | None:
    preferred = _column_by_names(frame, ("score", "prediction", "pred", "label"))
    if preferred is not None:
        return preferred
    numeric = [col for col in frame.columns if pd.api.types.is_numeric_dtype(frame[col])]
    if len(numeric) == 1:
        return numeric[0]
    if len(frame.columns) == 1:
        return frame.columns[0]
    return None


def _column_by_names(frame: pd.DataFrame, names: Sequence[str]) -> Any | None:
    lowered = {str(col).lower(): col for col in frame.columns}
    for name in names:
        if name in lowered:
            return lowered[name]
    return None


def _find_level(names: Sequence[str], tokens: Sequence[str]) -> int | None:
    for idx, name in enumerate(names):
        if any(token in name for token in tokens):
            return idx
    return None


def _common_dates(legs: Sequence[PredictionLeg]) -> list[Any]:
    common: set[Any] | None = None
    for leg in legs:
        dates = set(leg.frame["trade_date"].unique())
        common = dates if common is None else common & dates
    return sorted(common or [])


def _pair_matrix(
    legs: Sequence[PredictionLeg],
    common_dates: Sequence[Any],
    metric: Callable[[pd.DataFrame, pd.DataFrame], float | None],
) -> list[list[float | None]]:
    matrix: list[list[float | None]] = []
    for left in legs:
        row: list[float | None] = []
        for right in legs:
            if left.run_id == right.run_id:
                row.append(1.0)
            else:
                values: list[float] = []
                for trade_date in common_dates:
                    left_day = left.frame[left.frame["trade_date"] == trade_date]
                    right_day = right.frame[right.frame["trade_date"] == trade_date]
                    value = metric(left_day, right_day)
                    if value is not None and math.isfinite(value):
                        values.append(float(value))
                row.append(_round_or_none(sum(values) / len(values) if values else None))
        matrix.append(row)
    return matrix


def _daily_spearman_mean(left_day: pd.DataFrame, right_day: pd.DataFrame) -> float | None:
    merged = left_day.merge(right_day, on="instrument", suffixes=("_left", "_right"))
    if len(merged) < 2:
        return None
    corr = merged["score_left"].corr(merged["score_right"], method="spearman")
    if pd.isna(corr):
        return None
    return float(corr)


def _daily_jaccard_mean(left_day: pd.DataFrame, right_day: pd.DataFrame, k: int) -> float | None:
    if left_day.empty or right_day.empty:
        return None
    left_top = set(left_day.sort_values("score", ascending=False).head(k)["instrument"])
    right_top = set(right_day.sort_values("score", ascending=False).head(k)["instrument"])
    union = left_top | right_top
    if not union:
        return None
    return len(left_top & right_top) / len(union)


def _round_or_none(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(float(value), 6)


def _first_text(mapping: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return str(value)
    return None
