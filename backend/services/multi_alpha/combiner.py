"""Pure in-memory multi-alpha prediction combiner.

This module deliberately does not pull artifacts, run backtests, or persist
state. Callers provide already-loaded prediction frames and optional leg metrics.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import pandas as pd

from backend.services.multi_alpha.orthogonality import normalize_prediction_frame


class MultiAlphaCombinerError(RuntimeError):
    """Raised when explicit prediction legs cannot be combined safely."""


@dataclass(frozen=True)
class CombinerLeg:
    leg_id: str
    pred_frame: pd.DataFrame
    ic: float | None = None
    topk_return: float | None = None
    realized_returns: Sequence[float] | None = None
    metric_by_date: Mapping[Any, float] | None = None
    returns_by_date: Mapping[Any, float] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WalkForwardConfig:
    enabled: bool = False
    window: int = 3
    expanding: bool = False
    min_periods: int = 2


@dataclass(frozen=True)
class CombineResult:
    combined_score_frame: pd.DataFrame
    weights: dict[str, float]
    per_window_weights: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_payload(self, *, head: int = 20) -> dict[str, Any]:
        frame = self.combined_score_frame.sort_values(["trade_date", "combined_score"], ascending=[True, False])
        rows = []
        for row in frame.head(max(0, int(head))).to_dict(orient="records"):
            rows.append({key: _json_value(value) for key, value in row.items()})
        return {
            "schema_version": "multi_alpha_combine_preview_v1",
            "weights": {key: round(float(value), 10) for key, value in self.weights.items()},
            "per_window_weights": self.per_window_weights,
            "summary": self.summary,
            "combined_score_head": rows,
        }


class MultiAlphaCombiner:
    """Combine normalized alpha-leg predictions into one combined score."""

    def combine(
        self,
        *,
        legs: Sequence[CombinerLeg | Mapping[str, Any]],
        weighting_scheme: str = "equal",
        normalize_method: str = "zscore",
        walk_forward: WalkForwardConfig | Mapping[str, Any] | None = None,
    ) -> CombineResult:
        normalized_legs = [_coerce_leg(item) for item in legs]
        if len(normalized_legs) < 2:
            raise MultiAlphaCombinerError("at least two legs are required")
        if len({leg.leg_id for leg in normalized_legs}) != len(normalized_legs):
            raise MultiAlphaCombinerError("leg ids must be unique")

        scheme = _normalize_scheme(weighting_scheme)
        method = _normalize_method(normalize_method)
        wf = _coerce_walk_forward(walk_forward)
        normalized_frames = {
            leg.leg_id: _normalize_leg_scores(leg, method=method)
            for leg in normalized_legs
        }
        aligned = _aligned_panel(normalized_frames)
        dates = _sorted_dates(aligned["trade_date"].unique())
        if not dates:
            raise MultiAlphaCombinerError("prediction legs have no common trade_date")

        if wf.enabled:
            combined, per_window = _combine_walk_forward(
                legs=normalized_legs,
                aligned=aligned,
                dates=dates,
                scheme=scheme,
                config=wf,
            )
            weights = per_window[-1]["weights"] if per_window else _equal_weights(normalized_legs)
        else:
            weights = _weights_for_scheme(normalized_legs, aligned, scheme=scheme, train_dates=dates, all_dates=dates)
            combined = _combine_aligned(aligned, weights=weights, dates=dates)
            per_window = []

        summary = _summary(
            combined,
            legs=normalized_legs,
            aligned=aligned,
            scheme=scheme,
            normalize_method=method,
            walk_forward=wf,
            per_window=per_window,
        )
        return CombineResult(combined_score_frame=combined, weights=weights, per_window_weights=per_window, summary=summary)

    def combine_rank_fusion(
        self,
        *,
        legs: Sequence[CombinerLeg | Mapping[str, Any]],
        method: str = "rrf",
        rrf_k: float = 60.0,
        leg_weights: Mapping[str, float] | None = None,
    ) -> CombineResult:
        """Combine prediction legs by per-date score ranks without labels."""

        normalized_legs = [_coerce_leg(item) for item in legs]
        if len(normalized_legs) < 2:
            raise MultiAlphaCombinerError("at least two legs are required")
        if len({leg.leg_id for leg in normalized_legs}) != len(normalized_legs):
            raise MultiAlphaCombinerError("leg ids must be unique")

        fusion_method = _normalize_rank_fusion_method(method)
        k = _normalize_rrf_k(rrf_k) if fusion_method == "rrf" else 60.0
        weights = _rank_fusion_weights(normalized_legs, leg_weights)
        ranked_frames = {leg.leg_id: _rank_fusion_leg_scores(leg) for leg in normalized_legs}
        candidate_dates = _rank_fusion_candidate_dates(normalized_legs, ranked_frames)
        combined, dropped_dates = _combine_rank_fusion_frames(
            ranked_frames,
            weights=weights,
            method=fusion_method,
            rrf_k=k,
            candidate_dates=candidate_dates,
        )
        summary = _rank_fusion_summary(
            combined,
            legs=normalized_legs,
            ranked_frames=ranked_frames,
            method=fusion_method,
            rrf_k=k,
            dropped_dates=dropped_dates,
        )
        return CombineResult(combined_score_frame=combined, weights=weights, per_window_weights=[], summary=summary)

    def rank_fusion(
        self,
        *,
        legs: Sequence[CombinerLeg | Mapping[str, Any]],
        method: str = "rrf",
        rrf_k: float = 60.0,
        leg_weights: Mapping[str, float] | None = None,
    ) -> CombineResult:
        return self.combine_rank_fusion(legs=legs, method=method, rrf_k=rrf_k, leg_weights=leg_weights)


def _coerce_leg(item: CombinerLeg | Mapping[str, Any]) -> CombinerLeg:
    if isinstance(item, CombinerLeg):
        return item
    if not isinstance(item, Mapping):
        raise MultiAlphaCombinerError(f"leg must be a mapping or CombinerLeg, got {type(item).__name__}")
    leg_id = str(item.get("id") or item.get("leg_id") or item.get("run_id") or "").strip()
    if not leg_id:
        raise MultiAlphaCombinerError("each leg requires id/leg_id/run_id")
    frame = item.get("pred_frame")
    if frame is None:
        raise MultiAlphaCombinerError(f"leg {leg_id} requires pred_frame")
    metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
    return CombinerLeg(
        leg_id=leg_id,
        pred_frame=frame,
        ic=_optional_float(item.get("ic")),
        topk_return=_optional_float(item.get("topk_return") or item.get("topk")),
        realized_returns=_optional_float_list(item.get("realized_returns") or item.get("returns")),
        metric_by_date=_date_float_mapping(
            item.get("metric_by_date") or item.get("ic_by_date") or item.get("topk_by_date") or metadata.get("metric_by_date")
        ),
        returns_by_date=_date_float_mapping(item.get("returns_by_date") or metadata.get("returns_by_date")),
        metadata=metadata,
    )


def _normalize_leg_scores(leg: CombinerLeg, *, method: str) -> pd.DataFrame:
    frame = normalize_prediction_frame(leg.pred_frame, run_id=leg.leg_id)
    frame = frame.copy()
    if method == "zscore":
        frame["norm_score"] = frame.groupby("trade_date", group_keys=False)["score"].transform(_zscore)
    elif method == "rank":
        frame["norm_score"] = frame.groupby("trade_date", group_keys=False)["score"].transform(_rank_score)
    else:  # pragma: no cover - guarded by _normalize_method
        raise MultiAlphaCombinerError(f"unsupported normalize_method={method!r}")
    frame = frame.dropna(subset=["norm_score"])
    if frame.empty:
        raise MultiAlphaCombinerError(f"leg {leg.leg_id} has no valid normalized score rows")
    return frame[["trade_date", "instrument", "norm_score"]].rename(columns={"norm_score": f"score__{leg.leg_id}"})


def _zscore(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    std = values.std(ddof=0)
    if not math.isfinite(float(std)) or float(std) <= 0:
        return pd.Series(0.0, index=series.index)
    return (values - values.mean()) / std


def _rank_score(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    ranks = values.rank(method="average", ascending=True)
    n = int(ranks.count())
    if n <= 1:
        return pd.Series(0.0, index=series.index)
    return ((ranks - 1) / (n - 1)) * 2 - 1


def _aligned_panel(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for leg_id, frame in frames.items():
        selected = frame[["trade_date", "instrument", f"score__{leg_id}"]]
        merged = selected if merged is None else merged.merge(selected, on=["trade_date", "instrument"], how="inner")
    if merged is None or merged.empty:
        raise MultiAlphaCombinerError("prediction legs have no common (trade_date, instrument) rows")
    common_dates = _sorted_dates(merged["trade_date"].unique())
    if not common_dates:
        raise MultiAlphaCombinerError("prediction legs have no common trade_date")
    if merged["instrument"].nunique() < 1:
        raise MultiAlphaCombinerError("prediction legs have no common instruments")
    return merged.sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def _weights_for_scheme(
    legs: Sequence[CombinerLeg],
    aligned: pd.DataFrame,
    *,
    scheme: str,
    train_dates: Sequence[Any],
    all_dates: Sequence[Any],
    walk_forward: bool = False,
    min_periods: int | None = None,
) -> dict[str, float]:
    if scheme == "equal":
        return _equal_weights(legs)
    if scheme == "ic_weighted":
        return _metric_weights(legs, train_dates=train_dates if walk_forward else None)
    if scheme == "risk_parity":
        return _risk_parity_weights(
            legs,
            aligned=aligned,
            train_dates=train_dates,
            all_dates=all_dates,
            min_periods=min_periods,
        )
    if scheme == "orthogonality_aware":
        return _orthogonality_weights(legs, aligned=aligned, train_dates=train_dates)
    raise MultiAlphaCombinerError(f"unsupported weighting_scheme={scheme!r}")


def _equal_weights(legs: Sequence[CombinerLeg]) -> dict[str, float]:
    value = 1.0 / len(legs)
    return {leg.leg_id: value for leg in legs}


def _metric_weights(legs: Sequence[CombinerLeg], *, train_dates: Sequence[Any] | None = None) -> dict[str, float]:
    raw: dict[str, float] = {}
    for leg in legs:
        if train_dates is not None:
            if not leg.metric_by_date:
                raise MultiAlphaCombinerError(
                    f"walk_forward ic_weighted requires metric_by_date/ic_by_date/topk_by_date for leg {leg.leg_id}"
                )
            values = [leg.metric_by_date[date] for date in train_dates if date in leg.metric_by_date]
            metric = sum(values) / len(values) if values else 0.0
        else:
            if leg.topk_return is None and leg.ic is None:
                raise MultiAlphaCombinerError(f"ic_weighted requires ic or topk_return for leg {leg.leg_id}")
            metric = leg.topk_return if leg.topk_return is not None else leg.ic
        raw[leg.leg_id] = max(0.0, float(metric or 0.0))
    if sum(raw.values()) <= 0:
        raise MultiAlphaCombinerError("ic_weighted has no positive IC/TopK weights after clipping negatives to zero")
    return _normalize_positive(raw, fallback=_equal_weights(legs))


def _risk_parity_weights(
    legs: Sequence[CombinerLeg],
    *,
    aligned: pd.DataFrame,
    train_dates: Sequence[Any],
    all_dates: Sequence[Any],
    min_periods: int | None = None,
) -> dict[str, float]:
    train = aligned[aligned["trade_date"].isin(train_dates)]
    raw: dict[str, float] = {}
    diagnostics: list[dict[str, Any]] = []
    required_periods = max(2, int(min_periods or 2))
    for leg in legs:
        returns = _returns_for_leg(leg, train=train, train_dates=train_dates, all_dates=all_dates)
        finite_returns = _finite_float_series(returns)
        vol = finite_returns.std(ddof=0) if len(finite_returns) else float("nan")
        diagnostic = _risk_parity_leg_diagnostic(
            leg_id=leg.leg_id,
            returns=returns,
            finite_returns=finite_returns,
            train_dates=train_dates,
            vol=vol,
            required_periods=required_periods,
        )
        diagnostics.append(diagnostic)
        if diagnostic["reason"] is None:
            raw[leg.leg_id] = 1.0 / float(vol)
        else:
            raw[leg.leg_id] = 0.0
    invalid = [item for item in diagnostics if item["reason"] is not None]
    if invalid or sum(raw.values()) <= 0:
        raise MultiAlphaCombinerError(_risk_parity_noncomputable_message(diagnostics, train_dates=train_dates))
    return _normalize_positive(raw, fallback=_equal_weights(legs))


def _returns_for_leg(
    leg: CombinerLeg,
    *,
    train: pd.DataFrame,
    train_dates: Sequence[Any],
    all_dates: Sequence[Any],
) -> pd.Series:
    if leg.returns_by_date is not None:
        return pd.Series(
            {date: leg.returns_by_date[date] for date in train_dates if date in leg.returns_by_date},
            dtype="float64",
        )
    if leg.realized_returns is not None:
        if len(leg.realized_returns) < len(all_dates):
            raise MultiAlphaCombinerError(
                f"leg {leg.leg_id} realized_returns length={len(leg.realized_returns)} is shorter than date count={len(all_dates)}"
            )
        date_to_index = {date: idx for idx, date in enumerate(all_dates)}
        return pd.Series(
            {date: float(leg.realized_returns[date_to_index[date]]) for date in train_dates},
            dtype="float64",
        )
    raise MultiAlphaCombinerError(
        f"risk_parity requires realized_returns or returns_by_date for leg {leg.leg_id}; prediction scores are not returns"
    )


def _finite_float_series(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.empty:
        return numeric.astype("float64")
    finite_mask = numeric.notna() & numeric.map(lambda value: math.isfinite(float(value)))
    return numeric[finite_mask].astype("float64")


def _risk_parity_leg_diagnostic(
    *,
    leg_id: str,
    returns: pd.Series,
    finite_returns: pd.Series,
    train_dates: Sequence[Any],
    vol: float,
    required_periods: int,
) -> dict[str, Any]:
    missing_dates = [_date_text(date) for date in train_dates if date not in set(returns.index)]
    valid_count = int(len(finite_returns))
    reason: str | None
    if valid_count < required_periods:
        reason = "insufficient_valid_returns"
    elif not math.isfinite(float(vol)):
        reason = "non_finite_volatility"
    elif float(vol) <= 0:
        reason = "non_positive_volatility"
    else:
        reason = None
    return {
        "leg_id": leg_id,
        "reason": reason,
        "vol": float(vol) if math.isfinite(float(vol)) else float("nan"),
        "valid_return_count": valid_count,
        "observed_return_count": int(len(returns)),
        "train_date_count": int(len(train_dates)),
        "required_periods": int(required_periods),
        "missing_dates": missing_dates,
    }


def _risk_parity_noncomputable_message(
    diagnostics: Sequence[Mapping[str, Any]],
    *,
    train_dates: Sequence[Any],
) -> str:
    window = (
        f"{_date_text(train_dates[0])}..{_date_text(train_dates[-1])}"
        if train_dates
        else "<empty>"
    )
    parts = []
    for item in diagnostics:
        reason = item["reason"] or "computable"
        vol = item["vol"]
        vol_text = f"{float(vol):.12g}" if math.isfinite(float(vol)) else "nan"
        missing = ",".join(item["missing_dates"]) if item["missing_dates"] else "-"
        parts.append(
            "leg={leg_id} reason={reason} vol={vol} valid_returns={valid}/{train} "
            "observed_returns={observed} required_min_periods={required} missing_dates={missing}".format(
                leg_id=item["leg_id"],
                reason=reason,
                vol=vol_text,
                valid=item["valid_return_count"],
                train=item["train_date_count"],
                observed=item["observed_return_count"],
                required=item["required_periods"],
                missing=missing,
            )
        )
    return (
        "risk_parity has non-computable inverse-volatility weights "
        f"for train_window={window}; " + "; ".join(parts)
    )


def _orthogonality_weights(
    legs: Sequence[CombinerLeg],
    *,
    aligned: pd.DataFrame,
    train_dates: Sequence[Any],
) -> dict[str, float]:
    train = aligned[aligned["trade_date"].isin(train_dates)]
    raw: dict[str, float] = {}
    for leg in legs:
        own = f"score__{leg.leg_id}"
        corr_sum = 0.0
        for other in legs:
            if other.leg_id == leg.leg_id:
                continue
            other_col = f"score__{other.leg_id}"
            corr = train[own].corr(train[other_col], method="spearman") if len(train) >= 2 else float("nan")
            corr_sum += abs(float(corr)) if pd.notna(corr) and math.isfinite(float(corr)) else 0.0
        raw[leg.leg_id] = 1.0 / max(corr_sum, 1e-12)
    return _normalize_positive(raw, fallback=_equal_weights(legs))


def _combine_aligned(aligned: pd.DataFrame, *, weights: Mapping[str, float], dates: Sequence[Any]) -> pd.DataFrame:
    selected = aligned[aligned["trade_date"].isin(dates)].copy()
    if selected.empty:
        raise MultiAlphaCombinerError("no out-of-sample rows available for selected dates")
    selected["combined_score"] = 0.0
    for leg_id, weight in weights.items():
        col = f"score__{leg_id}"
        if col not in selected.columns:
            raise MultiAlphaCombinerError(f"aligned panel missing leg column {col}")
        selected["combined_score"] += float(weight) * selected[col]
    return selected[["trade_date", "instrument", "combined_score"]].sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def _normalize_rank_fusion_method(value: str) -> str:
    method = str(value or "rrf").strip().lower().replace("_", "-")
    aliases = {
        "reciprocal-rank-fusion": "rrf",
        "rank-fusion-rrf": "rrf",
        "rank-fusion-borda": "borda",
    }
    method = aliases.get(method, method)
    allowed = {"rrf", "borda"}
    if method not in allowed:
        raise MultiAlphaCombinerError(f"unsupported rank_fusion method={value!r}; expected one of {sorted(allowed)}")
    return method


def _normalize_rrf_k(value: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise MultiAlphaCombinerError(f"rrf_k must be numeric, got {value!r}") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise MultiAlphaCombinerError(f"rrf_k must be positive and finite, got {value!r}")
    return parsed


def _rank_fusion_weights(legs: Sequence[CombinerLeg], leg_weights: Mapping[str, float] | None) -> dict[str, float]:
    if leg_weights is None:
        return {leg.leg_id: 1.0 for leg in legs}
    input_weights = {str(key): value for key, value in leg_weights.items()}
    raw: dict[str, float] = {}
    unknown = sorted(set(input_weights) - {leg.leg_id for leg in legs})
    if unknown:
        raise MultiAlphaCombinerError(f"leg_weights contains unknown leg ids: {unknown}")
    for leg in legs:
        value = input_weights.get(leg.leg_id, 1.0)
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise MultiAlphaCombinerError(f"leg_weights for {leg.leg_id} must be numeric, got {value!r}") from exc
        if not math.isfinite(parsed) or parsed < 0:
            raise MultiAlphaCombinerError(f"leg_weights for {leg.leg_id} must be non-negative and finite, got {value!r}")
        raw[leg.leg_id] = parsed
    if sum(raw.values()) <= 0:
        raise MultiAlphaCombinerError("leg_weights must contain at least one positive weight")
    return raw


def _rank_fusion_leg_scores(leg: CombinerLeg) -> pd.DataFrame:
    _reject_label_only_rank_fusion_source(leg)
    frame = normalize_prediction_frame(leg.pred_frame, run_id=leg.leg_id)
    frame = frame.sort_values(["trade_date", "score", "instrument"], ascending=[True, False, True]).reset_index(drop=True)
    frame["rank"] = frame.groupby("trade_date").cumcount() + 1
    frame["date_count"] = frame.groupby("trade_date")["instrument"].transform("size")
    return frame[["trade_date", "instrument", "rank", "date_count"]]


def _reject_label_only_rank_fusion_source(leg: CombinerLeg) -> None:
    if isinstance(leg.pred_frame, pd.Series):
        if str(leg.pred_frame.name or "").lower() == "label":
            raise MultiAlphaCombinerError(f"rank_fusion refuses label-only score source for leg {leg.leg_id}")
        return
    frame = leg.pred_frame if isinstance(leg.pred_frame, pd.DataFrame) else pd.DataFrame(leg.pred_frame)
    score_col = _rank_fusion_column_by_names(frame, ("score", "prediction", "pred"))
    label_col = _rank_fusion_column_by_names(frame, ("label",))
    if score_col is None and label_col is not None:
        raise MultiAlphaCombinerError(f"rank_fusion refuses label-only score source for leg {leg.leg_id}")


def _rank_fusion_candidate_dates(legs: Sequence[CombinerLeg], ranked_frames: Mapping[str, pd.DataFrame]) -> list[Any]:
    candidate_dates: set[Any] = set()
    for leg in legs:
        metadata_dates = leg.metadata.get("trade_dates") if isinstance(leg.metadata, Mapping) else None
        if metadata_dates is not None:
            for raw_date in metadata_dates:
                date = pd.to_datetime(raw_date, errors="coerce")
                if pd.notna(date):
                    candidate_dates.add(date.date())
        candidate_dates.update(_candidate_dates_from_prediction_obj(leg.pred_frame))
        candidate_dates.update(ranked_frames[leg.leg_id]["trade_date"].unique())
    return _sorted_dates(candidate_dates)


def _candidate_dates_from_prediction_obj(obj: Any) -> set[Any]:
    if isinstance(obj, pd.Series):
        frame = obj.to_frame(name="score")
    elif isinstance(obj, pd.DataFrame):
        frame = obj.copy()
    else:
        frame = pd.DataFrame(obj)
    values: Any | None = None
    if isinstance(frame.index, pd.MultiIndex):
        index_names = [str(name or "").lower() for name in frame.index.names]
        date_level = _find_rank_fusion_level(index_names, ("datetime", "date", "trade_date", "time"))
        if date_level is None and frame.index.nlevels >= 2:
            date_level = 0
        values = frame.index.get_level_values(date_level) if date_level is not None else None
    elif isinstance(frame.index, pd.DatetimeIndex):
        values = frame.index
    else:
        date_col = _rank_fusion_column_by_names(frame, ("datetime", "date", "trade_date", "time"))
        values = frame[date_col] if date_col is not None else None
    if values is None:
        return set()
    dates = pd.to_datetime(values, errors="coerce")
    return {date.date() for date in dates if pd.notna(date)}


def _rank_fusion_column_by_names(frame: pd.DataFrame, names: Sequence[str]) -> Any | None:
    lowered = {str(col).lower(): col for col in frame.columns}
    for name in names:
        if name in lowered:
            return lowered[name]
    return None


def _find_rank_fusion_level(names: Sequence[str], tokens: Sequence[str]) -> int | None:
    for idx, name in enumerate(names):
        if any(token in name for token in tokens):
            return idx
    return None


def _combine_rank_fusion_frames(
    ranked_frames: Mapping[str, pd.DataFrame],
    *,
    weights: Mapping[str, float],
    method: str,
    rrf_k: float,
    candidate_dates: Sequence[Any],
) -> tuple[pd.DataFrame, list[str]]:
    rows: list[dict[str, Any]] = []
    dropped_dates: list[str] = []
    for trade_date in candidate_dates:
        score_by_instrument: dict[str, float] = {}
        for leg_id, frame in ranked_frames.items():
            day = frame[frame["trade_date"] == trade_date]
            if day.empty:
                continue
            weight = float(weights[leg_id])
            for row in day.itertuples(index=False):
                instrument = str(row.instrument)
                contribution = _rank_fusion_contribution(method=method, rank=float(row.rank), date_count=float(row.date_count), rrf_k=rrf_k)
                score_by_instrument[instrument] = score_by_instrument.get(instrument, 0.0) + weight * contribution
        if not score_by_instrument:
            dropped_dates.append(_date_text(trade_date))
            continue
        rows.extend(
            {"trade_date": trade_date, "instrument": instrument, "combined_score": score}
            for instrument, score in score_by_instrument.items()
        )
    if not rows:
        raise MultiAlphaCombinerError("rank_fusion produced no combined rows; all candidate dates were empty")
    combined = pd.DataFrame(rows)
    combined = combined.sort_values(["trade_date", "instrument"]).reset_index(drop=True)
    return combined[["trade_date", "instrument", "combined_score"]], dropped_dates


def _rank_fusion_contribution(*, method: str, rank: float, date_count: float, rrf_k: float) -> float:
    if method == "rrf":
        return 1.0 / (rrf_k + rank)
    if method == "borda":
        return date_count - rank
    raise MultiAlphaCombinerError(f"unsupported rank_fusion method={method!r}")


def _rank_fusion_summary(
    combined: pd.DataFrame,
    *,
    legs: Sequence[CombinerLeg],
    ranked_frames: Mapping[str, pd.DataFrame],
    method: str,
    rrf_k: float,
    dropped_dates: Sequence[str],
) -> dict[str, Any]:
    dates = _sorted_dates(combined["trade_date"].unique())
    observed_dates = _sorted_dates(set().union(*(set(frame["trade_date"].unique()) for frame in ranked_frames.values())))
    return {
        "leg_count": len(legs),
        "legs": [leg.leg_id for leg in legs],
        "weighting_scheme": f"rank_fusion_{method}",
        "rank_fusion_method": method,
        "rrf_k": rrf_k if method == "rrf" else None,
        "row_count": int(len(combined)),
        "input_row_count": int(sum(len(frame) for frame in ranked_frames.values())),
        "n_dates": int(len(dates)),
        "date_start": _date_text(dates[0]) if dates else None,
        "date_end": _date_text(dates[-1]) if dates else None,
        "input_date_count": int(len(observed_dates)),
        "instrument_count": int(combined["instrument"].nunique()),
        "dropped_dates": list(dropped_dates),
    }


def _combine_walk_forward(
    *,
    legs: Sequence[CombinerLeg],
    aligned: pd.DataFrame,
    dates: Sequence[Any],
    scheme: str,
    config: WalkForwardConfig,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if scheme in {"equal", "orthogonality_aware"} and config.min_periods < 1:
        raise MultiAlphaCombinerError("walk_forward min_periods must be >= 1")
    rows: list[pd.DataFrame] = []
    windows: list[dict[str, Any]] = []
    for idx, apply_date in enumerate(dates):
        if config.expanding:
            train_dates = dates[:idx]
        else:
            start = max(0, idx - config.window)
            train_dates = dates[start:idx]
        if len(train_dates) < config.min_periods:
            continue
        weights = _weights_for_scheme(
            legs,
            aligned,
            scheme=scheme,
            train_dates=train_dates,
            all_dates=dates,
            walk_forward=True,
            min_periods=config.min_periods,
        )
        out = _combine_aligned(aligned, weights=weights, dates=[apply_date])
        rows.append(out)
        windows.append(
            {
                "train_start": _date_text(train_dates[0]),
                "train_end": _date_text(train_dates[-1]),
                "apply_date": _date_text(apply_date),
                "weights": {key: round(float(value), 10) for key, value in weights.items()},
            }
        )
    if not rows:
        raise MultiAlphaCombinerError("walk_forward produced no out-of-sample dates; lower min_periods or add more dates")
    return pd.concat(rows, ignore_index=True), windows


def _normalize_positive(raw: Mapping[str, float], *, fallback: Mapping[str, float]) -> dict[str, float]:
    cleaned = {key: float(value) for key, value in raw.items() if math.isfinite(float(value)) and float(value) > 0}
    total = sum(cleaned.values())
    if total <= 0:
        return dict(fallback)
    return {key: cleaned.get(key, 0.0) / total for key in raw}


def _normalize_scheme(value: str) -> str:
    scheme = str(value or "equal").strip().lower()
    allowed = {"equal", "ic_weighted", "risk_parity", "orthogonality_aware"}
    if scheme not in allowed:
        raise MultiAlphaCombinerError(f"unsupported weighting_scheme={value!r}; expected one of {sorted(allowed)}")
    return scheme


def _normalize_method(value: str) -> str:
    method = str(value or "zscore").strip().lower()
    allowed = {"zscore", "rank"}
    if method not in allowed:
        raise MultiAlphaCombinerError(f"unsupported normalize_method={value!r}; expected one of {sorted(allowed)}")
    return method


def _coerce_walk_forward(value: WalkForwardConfig | Mapping[str, Any] | None) -> WalkForwardConfig:
    if value is None:
        return WalkForwardConfig()
    if isinstance(value, WalkForwardConfig):
        return value
    if not isinstance(value, Mapping):
        raise MultiAlphaCombinerError("walk_forward must be a mapping or WalkForwardConfig")
    return WalkForwardConfig(
        enabled=bool(value.get("enabled", False)),
        window=max(1, int(value.get("window", 3))),
        expanding=bool(value.get("expanding", False)),
        min_periods=max(1, int(value.get("min_periods", 2))),
    )


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _optional_float_list(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",") if part.strip()]
    else:
        try:
            parts = list(value)
        except TypeError:
            return None
    result: list[float] = []
    for item in parts:
        parsed = _optional_float(item)
        if parsed is not None:
            result.append(parsed)
    return result or None



def _date_float_mapping(value: Any) -> dict[Any, float] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        return None
    result: dict[Any, float] = {}
    for raw_date, raw_value in value.items():
        parsed = _optional_float(raw_value)
        if parsed is None:
            continue
        date = pd.to_datetime(raw_date, errors="coerce")
        if pd.isna(date):
            continue
        result[date.date()] = parsed
    return result or None

def _sorted_dates(values: Sequence[Any]) -> list[Any]:
    return sorted(values)


def _date_text(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _json_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float):
        if math.isfinite(value):
            return round(value, 10)
        return None
    return value


def _summary(
    combined: pd.DataFrame,
    *,
    legs: Sequence[CombinerLeg],
    aligned: pd.DataFrame,
    scheme: str,
    normalize_method: str,
    walk_forward: WalkForwardConfig,
    per_window: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    dates = _sorted_dates(combined["trade_date"].unique())
    return {
        "leg_count": len(legs),
        "legs": [leg.leg_id for leg in legs],
        "weighting_scheme": scheme,
        "normalize_method": normalize_method,
        "row_count": int(len(combined)),
        "aligned_row_count": int(len(aligned)),
        "n_dates": int(len(dates)),
        "date_start": _date_text(dates[0]) if dates else None,
        "date_end": _date_text(dates[-1]) if dates else None,
        "instrument_count": int(combined["instrument"].nunique()),
        "walk_forward": {
            "enabled": walk_forward.enabled,
            "window": walk_forward.window,
            "expanding": walk_forward.expanding,
            "min_periods": walk_forward.min_periods,
            "window_count": len(per_window),
        },
    }

