from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID


@dataclass(frozen=True)
class CandidateBuildResult:
    candidates: pd.DataFrame
    coverage: pd.DataFrame


def build_runtime_equivalent_candidates(
    *,
    leg_frames: Mapping[str, pd.DataFrame],
    terminal_weights: Mapping[str, float],
    decision_dates: Sequence[pd.Timestamp],
    trading_calendar: Sequence[pd.Timestamp],
    identity: Mapping[str, str],
    raw_top_k: int = 25,
    target_count: int = 20,
) -> CandidateBuildResult:
    expected_legs = {LSTM_LEG_ID, FUND_LEG_ID}
    if set(leg_frames) != expected_legs or set(terminal_weights) != expected_legs:
        raise AdvisoryModelFirstError(
            "candidate builder requires the frozen two-leg identity",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"leg_ids": sorted(leg_frames), "weight_leg_ids": sorted(terminal_weights)},
        )
    if raw_top_k < target_count or target_count <= 0:
        raise ValueError("raw_top_k must be >= target_count > 0")
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    target_by_decision = _next_trading_date_map(decisions, trading_calendar)
    normalized = {
        leg_id: _normalize_leg_frame(leg_frames[leg_id], leg_id=leg_id, decision_dates=decisions)
        for leg_id in (LSTM_LEG_ID, FUND_LEG_ID)
    }
    aligned: pd.DataFrame | None = None
    for leg_id, frame in normalized.items():
        selected = frame.rename(
            columns={
                "raw_score": f"raw__{leg_id}",
                "normalized_score": f"norm__{leg_id}",
                "leg_rank": f"rank__{leg_id}",
            }
        )
        aligned = selected if aligned is None else aligned.merge(
            selected,
            on=["trade_date", "instrument"],
            how="inner",
            validate="one_to_one",
        )
    if aligned is None or aligned.empty:
        raise AdvisoryModelFirstError(
            "representative legs have no common candidate universe",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
        )
    aligned["combined_score"] = 0.0
    for leg_id in (LSTM_LEG_ID, FUND_LEG_ID):
        weight = float(terminal_weights.get(leg_id, np.nan))
        if not np.isfinite(weight) or weight <= 0:
            raise AdvisoryModelFirstError(
                "terminal weight is missing or invalid",
                reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
                context={"leg_id": leg_id, "weight": terminal_weights.get(leg_id)},
            )
        aligned["combined_score"] += weight * aligned[f"norm__{leg_id}"]
        aligned[f"weight__{leg_id}"] = weight
    if not np.isclose(sum(float(terminal_weights[leg]) for leg in terminal_weights), 1.0, atol=1e-10):
        raise AdvisoryModelFirstError(
            "terminal weights do not sum to one",
            reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
            context={"terminal_weights": dict(terminal_weights)},
        )

    ranked = aligned.sort_values(
        ["trade_date", "combined_score", "instrument"],
        ascending=[True, False, True],
    ).copy()
    ranked["selection_source_rank"] = ranked.groupby("trade_date").cumcount().add(1)
    raw = ranked[ranked["selection_source_rank"] <= raw_top_k].copy()
    selected = raw[raw["selection_source_rank"] <= target_count].copy()
    selected["selection_effective_rank"] = selected.groupby("trade_date").cumcount().add(1)
    selected["decision_as_of_trade_date"] = selected["trade_date"]
    selected["target_trade_date"] = selected["trade_date"].map(target_by_decision)
    for key, value in identity.items():
        selected[key] = value
    selected["candidate_group_size"] = selected.groupby("trade_date")["instrument"].transform("size")
    selected["alpha_mode"] = "multi_alpha"

    coverage = pd.DataFrame({"decision_as_of_trade_date": decisions})
    coverage["target_trade_date"] = coverage["decision_as_of_trade_date"].map(target_by_decision)
    common_counts = aligned.groupby("trade_date").size()
    candidate_counts = selected.groupby("trade_date").size()
    coverage["component_common_universe_count"] = coverage["decision_as_of_trade_date"].map(common_counts).fillna(0).astype(int)
    coverage["candidate_count"] = coverage["decision_as_of_trade_date"].map(candidate_counts).fillna(0).astype(int)
    coverage["status"] = np.where(coverage["candidate_count"] > 0, "available", "unavailable")
    missing_dates = coverage.loc[coverage["candidate_count"] == 0, "decision_as_of_trade_date"]
    if not missing_dates.empty:
        raise AdvisoryModelFirstError(
            "one or more decision dates produced an empty candidate group",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
            context={"missing_dates": [item.date().isoformat() for item in missing_dates[:20]]},
        )
    return CandidateBuildResult(
        candidates=selected.sort_values(["trade_date", "selection_effective_rank"]).reset_index(drop=True),
        coverage=coverage,
    )


def _normalize_leg_frame(
    frame: pd.DataFrame,
    *,
    leg_id: str,
    decision_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    required = {"trade_date", "instrument", "score"}
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "representative leg prediction frame has an invalid schema",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"leg_id": leg_id, "missing_columns": sorted(required - set(frame.columns))},
        )
    data = frame[frame["trade_date"].isin(decision_dates)].copy()
    data["raw_score"] = pd.to_numeric(data["score"], errors="coerce")
    if data["raw_score"].isna().any() or data.duplicated(["trade_date", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "representative leg prediction rows are invalid",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"leg_id": leg_id},
        )
    groups = data.groupby("trade_date")["raw_score"]
    means = groups.transform("mean")
    stds = groups.transform(lambda values: values.std(ddof=0))
    data["normalized_score"] = np.where(
        np.isfinite(stds) & (stds > 0),
        (data["raw_score"] - means) / stds,
        0.0,
    )
    ordered = data.sort_values(
        ["trade_date", "normalized_score", "instrument"],
        ascending=[True, False, True],
    ).copy()
    ordered["leg_rank"] = ordered.groupby("trade_date").cumcount().add(1)
    return ordered[["trade_date", "instrument", "raw_score", "normalized_score", "leg_rank"]]


def _next_trading_date_map(
    decision_dates: pd.DatetimeIndex,
    trading_calendar: Sequence[pd.Timestamp],
) -> dict[pd.Timestamp, pd.Timestamp]:
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    missing_decisions = decision_dates[~decision_dates.isin(calendar)]
    if len(missing_decisions):
        raise AdvisoryModelFirstError(
            "one or more decision dates are absent from the trading calendar",
            reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
            context={"decision_dates": [item.date().isoformat() for item in missing_decisions[:20]]},
        )
    positions = calendar.searchsorted(decision_dates, side="right")
    if (positions >= len(calendar)).any():
        missing = decision_dates[positions >= len(calendar)]
        raise AdvisoryModelFirstError(
            "trading calendar cannot resolve a target date",
            reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
            context={"decision_dates": [item.date().isoformat() for item in missing]},
        )
    return {decision: calendar[position] for decision, position in zip(decision_dates, positions, strict=True)}
