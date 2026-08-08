from __future__ import annotations

import gc
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.prediction_source import ExactPredictionSource
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID


def build_parent_diagnostics(
    *,
    source: ExactPredictionSource,
    full_seed_roster: Mapping[str, Sequence[str]],
    decision_dates: Sequence[pd.Timestamp],
    current_candidates: pd.DataFrame,
    combined_reference: pd.DataFrame,
    historical_weight_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reference = _normalize_reference(combined_reference, decision_dates)
    current_top = current_candidates[
        ["decision_as_of_trade_date", "instrument", "selection_effective_rank", "combined_score"]
    ].copy()
    current_top["decision_as_of_trade_date"] = pd.to_datetime(
        current_top["decision_as_of_trade_date"]
    ).dt.normalize()
    reference_top = (
        reference.sort_values(["trade_date", "score", "instrument"], ascending=[True, False, True])
        .groupby("trade_date", sort=False)
        .head(20)
        .copy()
    )
    overlap = []
    for decision, group in current_top.groupby("decision_as_of_trade_date", sort=True):
        current_symbols = set(group["instrument"])
        reference_symbols = set(reference_top.loc[reference_top["trade_date"] == decision, "instrument"])
        overlap.append(len(current_symbols & reference_symbols) / max(len(current_symbols), 1))
    diagnostic: dict[str, Any] = {
        "schema_version": "advisory_parent_diagnostics_v1",
        "current_runtime_vs_historical_reference": {
            "date_count": len(overlap),
            "mean_top20_overlap": float(np.mean(overlap)),
            "min_top20_overlap": float(np.min(overlap)),
            "status": "available",
        },
    }
    try:
        ensemble = {
            leg_id: _ensemble_mean(
                source,
                run_ids=run_ids,
                decision_dates=decision_dates,
            )
            for leg_id, run_ids in full_seed_roster.items()
        }
        normalized = {leg_id: _normalize_scores(frame) for leg_id, frame in ensemble.items()}
        reconstructed = normalized[LSTM_LEG_ID].merge(
            normalized[FUND_LEG_ID],
            on=["trade_date", "instrument"],
            how="inner",
            validate="one_to_one",
            suffixes=("__lstm", "__fund"),
        )
        weights = _weight_table(historical_weight_rows)
        reconstructed = reconstructed.merge(weights, on="trade_date", how="left", validate="many_to_one")
        missing_weight_dates = reconstructed.loc[
            reconstructed[["weight_lstm", "weight_fund"]].isna().any(axis=1), "trade_date"
        ].drop_duplicates()
        if not missing_weight_dates.empty:
            raise ValueError(
                "historical weight rows do not cover dates: "
                + ",".join(item.date().isoformat() for item in missing_weight_dates.iloc[:10])
            )
        reconstructed["score"] = (
            reconstructed["normalized_score__lstm"] * reconstructed["weight_lstm"]
            + reconstructed["normalized_score__fund"] * reconstructed["weight_fund"]
        )
        comparison = reconstructed[["trade_date", "instrument", "score"]].merge(
            reference,
            on=["trade_date", "instrument"],
            how="inner",
            validate="one_to_one",
            suffixes=("__reconstructed", "__reference"),
        )
        error = (comparison["score__reconstructed"] - comparison["score__reference"]).abs()
        ordering_match = _top_order_match(comparison)
        diagnostic["full_ensemble_walk_forward_reference"] = {
            "status": "available" if float(error.max()) <= 1e-8 and ordering_match else "failed",
            "leg_seed_counts": {leg: len(runs) for leg, runs in full_seed_roster.items()},
            "row_count": int(len(comparison)),
            "max_absolute_error": float(error.max()),
            "top20_ordering_match": ordering_match,
        }
        diagnostic["representative_vs_full_ensemble"] = {
            leg_id: _representative_ensemble_summary(
                source,
                representative_run_id=run_ids[0],
                ensemble=ensemble[leg_id],
                decision_dates=decision_dates,
            )
            for leg_id, run_ids in full_seed_roster.items()
        }
    except Exception as exc:
        diagnostic["full_ensemble_walk_forward_reference"] = {
            "status": "unavailable",
            "reason_code": "ADVISORY_MODEL_REFERENCE_COMBINATION_MISMATCH",
            "error_type": type(exc).__name__,
            "message": str(exc),
        }
    finally:
        gc.collect()
    return diagnostic


def _ensemble_mean(
    source: ExactPredictionSource,
    *,
    run_ids: Sequence[str],
    decision_dates: Sequence[pd.Timestamp],
) -> pd.DataFrame:
    base_index: pd.MultiIndex | None = None
    accumulator: pd.Series | None = None
    for run_id in run_ids:
        frame = source.load_scores(run_id, decision_dates=decision_dates, verify_artifact=False).set_index(
            ["trade_date", "instrument"]
        )
        scores = frame["score"].astype(np.float64)
        if base_index is None:
            base_index = frame.index
            accumulator = scores.copy()
        else:
            common = base_index.intersection(frame.index, sort=False)
            if common.empty:
                raise ValueError(f"seed predictions have no common rows: {run_id}")
            accumulator = accumulator.reindex(common) + scores.reindex(common)
            base_index = common
        del frame, scores
        gc.collect()
    if base_index is None or accumulator is None:
        raise ValueError("seed roster is empty")
    result = accumulator.rename("score").div(len(run_ids)).reset_index()
    return result[["trade_date", "instrument", "score"]]


def _normalize_scores(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    group = result.groupby("trade_date")["score"]
    mean = group.transform("mean")
    std = group.transform(lambda values: values.std(ddof=0))
    result["normalized_score"] = np.where(std > 0, (result["score"] - mean) / std, 0.0)
    return result[["trade_date", "instrument", "normalized_score"]]


def _normalize_reference(frame: pd.DataFrame, decision_dates: Sequence[pd.Timestamp]) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or not isinstance(frame.index, pd.MultiIndex) or "score" not in frame:
        raise ValueError("combined reference must be a MultiIndex DataFrame with score")
    result = frame[["score"]].reset_index()
    result["trade_date"] = pd.to_datetime(result.pop("datetime")).dt.normalize()
    result["instrument"] = result["instrument"].astype(str).str.upper()
    wanted = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize()
    return result[result["trade_date"].isin(wanted)].sort_values(["trade_date", "instrument"])


def _weight_table(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    normalized = []
    for item in rows:
        weights = item.get("weights") if isinstance(item.get("weights"), Mapping) else {}
        normalized.append(
            {
                "trade_date": pd.Timestamp(item.get("apply_date")).normalize(),
                "weight_lstm": float(weights[LSTM_LEG_ID]),
                "weight_fund": float(weights[FUND_LEG_ID]),
            }
        )
    frame = pd.DataFrame(normalized)
    if frame.empty or frame["trade_date"].duplicated().any():
        raise ValueError("historical weight rows are empty or duplicate apply_date")
    return frame


def _top_order_match(comparison: pd.DataFrame) -> bool:
    for _, group in comparison.groupby("trade_date", sort=True):
        left = group.sort_values(
            ["score__reconstructed", "instrument"], ascending=[False, True]
        ).head(20)["instrument"].tolist()
        right = group.sort_values(
            ["score__reference", "instrument"], ascending=[False, True]
        ).head(20)["instrument"].tolist()
        if left != right:
            return False
    return True


def _representative_ensemble_summary(
    source: ExactPredictionSource,
    *,
    representative_run_id: str,
    ensemble: pd.DataFrame,
    decision_dates: Sequence[pd.Timestamp],
) -> dict[str, Any]:
    representative = source.load_scores(
        representative_run_id,
        decision_dates=decision_dates,
        verify_artifact=False,
    )
    comparison = representative.merge(
        ensemble,
        on=["trade_date", "instrument"],
        how="inner",
        validate="one_to_one",
        suffixes=("__representative", "__ensemble"),
    )
    return {
        "row_count": int(len(comparison)),
        "pearson_score_correlation": float(
            comparison["score__representative"].corr(comparison["score__ensemble"], method="pearson")
        ),
        "mean_absolute_score_difference": float(
            (comparison["score__representative"] - comparison["score__ensemble"]).abs().mean()
        ),
    }
