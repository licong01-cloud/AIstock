"""Leak-safe dated panels for multi-alpha combination weights.

The panel builder turns each finalized alpha leg into a ``CombinerLeg`` with
``metric_by_date`` and ``returns_by_date`` populated from prediction-store
artifacts. For every trade date, it joins the seed-ensemble score with that
date's realized forward label and computes:

* rank IC: Spearman(score, forward_return) across instruments on that date.
* Top-K realized return: realized forward-return mean of the score Top-K names.

These are realized observations for the same trade date, so they must never be
used directly for that date's weight. The combiner's walk-forward mode enforces
that an ``apply_date`` uses only ``train_dates`` strictly before ``apply_date``.
Together this module plus ``MultiAlphaCombiner`` provide the no-leakage contract:
panel values may contain realized outcomes for date D, but weights applied to D
are fitted only from dates < D.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from backend.services.model_store import ModelStoreService, PredictionStoreError
from backend.services.multi_alpha.combiner import CombinerLeg
from backend.services.multi_alpha.orthogonality import MultiAlphaOrthogonalityError, normalize_prediction_frame


class MultiAlphaPanelError(RuntimeError):
    """Raised when a dated panel cannot be built without guessing or leakage."""

    def __init__(
        self,
        message: str,
        *,
        leg_id: str | None = None,
        run_id: str | None = None,
        reason_code: str = "panel_error",
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self.leg_id = leg_id
        self.run_id = run_id
        self.reason_code = reason_code
        self.context = dict(context or {})
        prefix = f"reason_code={reason_code}"
        if leg_id:
            prefix += f" leg_id={leg_id}"
        if run_id:
            prefix += f" run_id={run_id}"
        super().__init__(f"{prefix}: {message}")


@dataclass(frozen=True)
class PanelLegSpec:
    leg_id: str
    seed_run_ids: tuple[str, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LegPanel:
    leg_id: str
    seed_run_ids: tuple[str, ...]
    pred_frame: pd.DataFrame
    metric_by_date: dict[Any, float]
    returns_by_date: dict[Any, float]
    coverage: dict[str, Any]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_combiner_leg(self) -> CombinerLeg:
        return CombinerLeg(
            leg_id=self.leg_id,
            pred_frame=self.pred_frame,
            metric_by_date=self.metric_by_date,
            returns_by_date=self.returns_by_date,
            metadata={**dict(self.metadata), "coverage": self.coverage, "seed_run_ids": list(self.seed_run_ids)},
        )


PredictionLoader = Callable[[str], Any]
LabelLoader = Callable[[str], Any]


class MultiAlphaPanelBuilder:
    """Build seed-ensemble prediction panels with explicit labels and coverage checks."""

    def __init__(
        self,
        *,
        model_store: ModelStoreService | None = None,
        prediction_loader: PredictionLoader | None = None,
        label_loader: LabelLoader | None = None,
    ) -> None:
        self._model_store = model_store or ModelStoreService()
        self._prediction_loader = prediction_loader
        self._label_loader = label_loader

    def build_combiner_legs(
        self,
        *,
        legs: Sequence[PanelLegSpec | Mapping[str, Any]],
        oos_start: str | Any,
        oos_end: str | Any,
        topk: int = 20,
        min_date_coverage: float = 0.8,
    ) -> list[CombinerLeg]:
        return [
            panel.to_combiner_leg()
            for panel in self.build_panels(
                legs=legs,
                oos_start=oos_start,
                oos_end=oos_end,
                topk=topk,
                min_date_coverage=min_date_coverage,
            )
        ]

    def build_panels(
        self,
        *,
        legs: Sequence[PanelLegSpec | Mapping[str, Any]],
        oos_start: str | Any,
        oos_end: str | Any,
        topk: int = 20,
        min_date_coverage: float = 0.8,
    ) -> list[LegPanel]:
        start = _coerce_date(oos_start, field_name="oos_start")
        end = _coerce_date(oos_end, field_name="oos_end")
        if end < start:
            raise MultiAlphaPanelError("oos_end must be >= oos_start", reason_code="invalid_window")
        selected = [_coerce_leg_spec(item) for item in legs]
        if len(selected) < 2:
            raise MultiAlphaPanelError("at least two legs are required", reason_code="insufficient_legs")
        if len({leg.leg_id for leg in selected}) != len(selected):
            raise MultiAlphaPanelError("leg_id values must be unique", reason_code="duplicate_leg_id")
        if not 0 < float(min_date_coverage) <= 1:
            raise MultiAlphaPanelError("min_date_coverage must be in (0, 1]", reason_code="invalid_coverage_threshold")
        top_k = _normalize_topk(topk)

        return [
            self._build_one(spec, start=start, end=end, topk=top_k, min_date_coverage=float(min_date_coverage))
            for spec in selected
        ]

    def _build_one(
        self,
        spec: PanelLegSpec,
        *,
        start: Any,
        end: Any,
        topk: int,
        min_date_coverage: float,
    ) -> LegPanel:
        seed_frames = [self._load_prediction_frame(run_id, leg_id=spec.leg_id) for run_id in spec.seed_run_ids]
        ensemble = _ensemble_seed_predictions(seed_frames, leg_id=spec.leg_id)
        ensemble = ensemble[(ensemble["trade_date"] >= start) & (ensemble["trade_date"] <= end)].copy()
        if ensemble.empty:
            raise MultiAlphaPanelError(
                "seed ensemble has no prediction rows in requested OOS window",
                leg_id=spec.leg_id,
                reason_code="prediction_window_empty",
                context={"oos_start": str(start), "oos_end": str(end)},
            )

        label = self._load_label_frame(spec.seed_run_ids[0], leg_id=spec.leg_id)
        label = label[(label["trade_date"] >= start) & (label["trade_date"] <= end)].copy()
        if label.empty:
            raise MultiAlphaPanelError(
                "label artifact has no rows in requested OOS window",
                leg_id=spec.leg_id,
                run_id=spec.seed_run_ids[0],
                reason_code="label_window_empty",
                context={"oos_start": str(start), "oos_end": str(end)},
            )

        joined = ensemble.merge(label, on=["trade_date", "instrument"], how="inner")
        if joined.empty:
            raise MultiAlphaPanelError(
                "prediction and label have no common (trade_date, instrument) rows",
                leg_id=spec.leg_id,
                reason_code="prediction_label_no_overlap",
            )

        metric_by_date: dict[Any, float] = {}
        returns_by_date: dict[Any, float] = {}
        for trade_date, day in joined.groupby("trade_date", sort=True):
            if len(day) < 2:
                continue
            ic = day["score"].corr(day["forward_return"], method="spearman")
            top_return = (
                day.sort_values("score", ascending=False)
                .head(min(topk, len(day)))["forward_return"]
                .mean()
            )
            if pd.notna(ic):
                metric_by_date[trade_date] = float(ic)
            if pd.notna(top_return):
                returns_by_date[trade_date] = float(top_return)

        pred_dates = set(ensemble["trade_date"].unique())
        metric_dates = set(metric_by_date) & set(returns_by_date)
        coverage_ratio = len(metric_dates) / len(pred_dates) if pred_dates else 0.0
        coverage = {
            "prediction_date_count": len(pred_dates),
            "metric_date_count": len(metric_dates),
            "coverage_ratio": coverage_ratio,
            "oos_start": start.isoformat(),
            "oos_end": end.isoformat(),
            "topk": topk,
        }
        if coverage_ratio < min_date_coverage:
            raise MultiAlphaPanelError(
                "panel date coverage below threshold",
                leg_id=spec.leg_id,
                reason_code="panel_coverage_below_threshold",
                context={**coverage, "min_date_coverage": min_date_coverage},
            )

        return LegPanel(
            leg_id=spec.leg_id,
            seed_run_ids=spec.seed_run_ids,
            pred_frame=ensemble[["trade_date", "instrument", "score"]].sort_values(["trade_date", "instrument"]).reset_index(drop=True),
            metric_by_date=metric_by_date,
            returns_by_date=returns_by_date,
            coverage=coverage,
            metadata=spec.metadata,
        )

    def _load_prediction_frame(self, run_id: str, *, leg_id: str) -> pd.DataFrame:
        try:
            obj = self._prediction_loader(run_id) if self._prediction_loader is not None else pd.read_pickle(self._model_store.prediction_path(run_id=run_id))
            return normalize_prediction_frame(obj, run_id=run_id)
        except (PredictionStoreError, MultiAlphaOrthogonalityError, OSError, ValueError) as exc:
            raise MultiAlphaPanelError(
                f"failed to load prediction artifact: {type(exc).__name__}: {exc}",
                leg_id=leg_id,
                run_id=run_id,
                reason_code="prediction_missing_or_invalid",
            ) from exc

    def _load_label_frame(self, run_id: str, *, leg_id: str) -> pd.DataFrame:
        try:
            obj = self._label_loader(run_id) if self._label_loader is not None else pd.read_pickle(self._model_store.label_path(run_id=run_id))
            return normalize_label_frame(obj, run_id=run_id)
        except (PredictionStoreError, MultiAlphaPanelError, OSError, ValueError) as exc:
            raise MultiAlphaPanelError(
                f"failed to load label artifact: {type(exc).__name__}: {exc}",
                leg_id=leg_id,
                run_id=run_id,
                reason_code="label_missing_or_invalid",
            ) from exc


def normalize_label_frame(obj: Any, *, run_id: str) -> pd.DataFrame:
    """Normalize Qlib label.pkl shapes to trade_date/instrument/forward_return rows."""

    if isinstance(obj, pd.Series):
        frame = obj.to_frame(name="forward_return")
    elif isinstance(obj, pd.DataFrame):
        frame = obj.copy()
    else:
        frame = pd.DataFrame(obj)

    label_col = _label_column(frame)
    if label_col is None:
        raise MultiAlphaPanelError(
            "label.pkl has no numeric forward-return column",
            run_id=run_id,
            reason_code="label_column_missing",
        )

    if isinstance(frame.index, pd.MultiIndex):
        names = [str(name or "").lower() for name in frame.index.names]
        date_level = _find_level(names, ("datetime", "date", "trade_date", "time"))
        inst_level = _find_level(names, ("instrument", "symbol", "ts_code", "code"))
        if date_level is None or inst_level is None:
            if frame.index.nlevels >= 2:
                date_level = 0 if date_level is None else date_level
                inst_level = 1 if inst_level is None else inst_level
            else:
                raise MultiAlphaPanelError(
                    "label.pkl MultiIndex lacks date/instrument levels",
                    run_id=run_id,
                    reason_code="label_index_invalid",
                )
        out = pd.DataFrame(
            {
                "trade_date": frame.index.get_level_values(date_level),
                "instrument": frame.index.get_level_values(inst_level),
                "forward_return": frame[label_col].to_numpy(),
            }
        )
    else:
        date_col = _column_by_names(frame, ("datetime", "date", "trade_date", "time"))
        inst_col = _column_by_names(frame, ("instrument", "symbol", "ts_code", "code"))
        if date_col is None and isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index().rename(columns={frame.index.name or "index": "trade_date"})
            date_col = "trade_date"
        if date_col is None or inst_col is None:
            raise MultiAlphaPanelError(
                "label.pkl lacks date/instrument columns",
                run_id=run_id,
                reason_code="label_columns_invalid",
            )
        out = frame[[date_col, inst_col, label_col]].rename(
            columns={date_col: "trade_date", inst_col: "instrument", label_col: "forward_return"}
        )

    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.date
    out["instrument"] = out["instrument"].astype(str)
    out["forward_return"] = pd.to_numeric(out["forward_return"], errors="coerce")
    out = out.dropna(subset=["trade_date", "instrument", "forward_return"])
    if out.empty:
        raise MultiAlphaPanelError("label.pkl has no valid rows", run_id=run_id, reason_code="label_rows_empty")
    return out.groupby(["trade_date", "instrument"], as_index=False, sort=True)["forward_return"].mean()


def _coerce_leg_spec(item: PanelLegSpec | Mapping[str, Any]) -> PanelLegSpec:
    if isinstance(item, PanelLegSpec):
        return item
    if not isinstance(item, Mapping):
        raise MultiAlphaPanelError(f"leg must be a mapping or PanelLegSpec, got {type(item).__name__}", reason_code="invalid_leg")
    leg_id = str(item.get("leg_id") or item.get("id") or "").strip()
    run_ids_raw = item.get("seed_run_ids") or item.get("run_ids") or []
    seed_run_ids = tuple(str(value or "").strip() for value in run_ids_raw if str(value or "").strip())
    if not leg_id:
        raise MultiAlphaPanelError("leg_id is required", reason_code="leg_id_missing")
    if not seed_run_ids:
        raise MultiAlphaPanelError("seed_run_ids is required", leg_id=leg_id, reason_code="seed_run_ids_missing")
    metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
    return PanelLegSpec(leg_id=leg_id, seed_run_ids=seed_run_ids, metadata=metadata)


def _ensemble_seed_predictions(frames: Sequence[pd.DataFrame], *, leg_id: str) -> pd.DataFrame:
    if not frames:
        raise MultiAlphaPanelError("at least one seed prediction frame is required", leg_id=leg_id, reason_code="seed_prediction_missing")
    renamed: list[pd.DataFrame] = []
    for idx, frame in enumerate(frames):
        selected = frame[["trade_date", "instrument", "score"]].copy()
        selected = selected.rename(columns={"score": f"score__seed_{idx}"})
        renamed.append(selected)
    merged: pd.DataFrame | None = None
    for frame in renamed:
        merged = frame if merged is None else merged.merge(frame, on=["trade_date", "instrument"], how="outer")
    if merged is None or merged.empty:
        raise MultiAlphaPanelError("seed predictions have no rows", leg_id=leg_id, reason_code="seed_prediction_empty")
    score_cols = [col for col in merged.columns if str(col).startswith("score__seed_")]
    merged["score"] = merged[score_cols].mean(axis=1, skipna=True)
    out = merged[["trade_date", "instrument", "score"]].dropna(subset=["score"])
    if out.empty:
        raise MultiAlphaPanelError("seed ensemble produced no valid score rows", leg_id=leg_id, reason_code="seed_ensemble_empty")
    return out.groupby(["trade_date", "instrument"], as_index=False, sort=True)["score"].mean()


def _label_column(frame: pd.DataFrame) -> Any | None:
    preferred = _column_by_names(frame, ("forward_return", "label", "label0", "LABEL0", "return", "ret"))
    if preferred is not None and pd.api.types.is_numeric_dtype(frame[preferred]):
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
        if str(name).lower() in lowered:
            return lowered[str(name).lower()]
    return None


def _find_level(names: Sequence[str], tokens: Sequence[str]) -> int | None:
    for idx, name in enumerate(names):
        if any(token in name for token in tokens):
            return idx
    return None


def _coerce_date(value: str | Any, *, field_name: str) -> Any:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise MultiAlphaPanelError(f"{field_name} must be a valid date, got {value!r}", reason_code="invalid_date")
    return parsed.date()


def _normalize_topk(value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise MultiAlphaPanelError(f"topk must be an integer, got {value!r}", reason_code="invalid_topk") from exc
    if parsed < 1 or parsed > 500:
        raise MultiAlphaPanelError(f"topk must be between 1 and 500, got {parsed}", reason_code="invalid_topk")
    return parsed
