from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@dataclass(frozen=True)
class PolicyRankBuildResult:
    rankings: pd.DataFrame
    coverage: pd.DataFrame


def build_policy_rankings(
    *,
    leg_frames: Mapping[str, pd.DataFrame],
    terminal_weights: Mapping[str, float],
    decision_dates: Sequence[pd.Timestamp],
    trading_calendar: Sequence[pd.Timestamp],
    identity: Mapping[str, str],
    required_depth: int = 40,
) -> PolicyRankBuildResult:
    if required_depth < 40:
        raise ValueError("policy rank reconstruction requires depth >= 40")
    leg_ids = tuple(sorted(leg_frames))
    if not leg_ids or set(leg_ids) != set(terminal_weights):
        raise AdvisoryModelFirstError(
            "policy rank legs differ from terminal weights",
            reason_code="ADVISORY_POLICY_RANK_IDENTITY_MISMATCH",
        )
    total_weight = sum(float(terminal_weights[item]) for item in leg_ids)
    if abs(total_weight - 1.0) > 1e-10 or any(float(terminal_weights[item]) <= 0 for item in leg_ids):
        raise AdvisoryModelFirstError(
            "policy rank terminal weights are invalid",
            reason_code="ADVISORY_POLICY_RANK_IDENTITY_MISMATCH",
        )
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    target_map = _next_trade_map(decisions, trading_calendar)
    aligned: pd.DataFrame | None = None
    for leg_id in leg_ids:
        normalized = _normalize_leg(leg_frames[leg_id], leg_id=leg_id, decisions=decisions)
        renamed = normalized.rename(
            columns={
                "raw_score": f"raw__{leg_id}",
                "normalized_score": f"norm__{leg_id}",
                "leg_rank": f"rank__{leg_id}",
            }
        )
        aligned = renamed if aligned is None else aligned.merge(
            renamed, on=["trade_date", "instrument"], how="inner", validate="one_to_one"
        )
    if aligned is None or aligned.empty:
        raise AdvisoryModelFirstError(
            "policy rank legs have no common rows",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    aligned["combined_score"] = 0.0
    for leg_id in leg_ids:
        aligned["combined_score"] += float(terminal_weights[leg_id]) * aligned[f"norm__{leg_id}"]
        aligned[f"weight__{leg_id}"] = float(terminal_weights[leg_id])
    ranked = aligned.sort_values(
        ["trade_date", "combined_score", "instrument"], ascending=[True, False, True]
    ).copy()
    ranked["selection_effective_rank"] = ranked.groupby("trade_date").cumcount().add(1)
    depth = ranked[ranked["selection_effective_rank"] <= required_depth].copy()
    depth["decision_as_of_trade_date"] = depth["trade_date"]
    depth["target_trade_date"] = depth["trade_date"].map(target_map)
    depth["candidate_group_size"] = depth.groupby("trade_date")["instrument"].transform("size")
    depth["alpha_mode"] = "multi_alpha"
    for key, value in identity.items():
        depth[key] = value
    counts = depth.groupby("trade_date").size()
    coverage = pd.DataFrame({"decision_as_of_trade_date": decisions})
    coverage["target_trade_date"] = coverage["decision_as_of_trade_date"].map(target_map)
    coverage["rank_count"] = coverage["decision_as_of_trade_date"].map(counts).fillna(0).astype(int)
    coverage["status"] = np.where(coverage["rank_count"] == required_depth, "COMPLETE", "DATA_UNAVAILABLE")
    incomplete = coverage[coverage["status"] != "COMPLETE"]
    if not incomplete.empty:
        raise AdvisoryModelFirstError(
            "one or more policy ranking dates do not cover the required depth",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
            context={
                "required_depth": required_depth,
                "dates": [item.date().isoformat() for item in incomplete["decision_as_of_trade_date"].head(20)],
                "counts": incomplete["rank_count"].head(20).tolist(),
            },
        )
    return PolicyRankBuildResult(
        rankings=depth.sort_values(["trade_date", "selection_effective_rank", "instrument"]).reset_index(drop=True),
        coverage=coverage,
    )


def _normalize_leg(frame: pd.DataFrame, *, leg_id: str, decisions: pd.DatetimeIndex) -> pd.DataFrame:
    required = {"trade_date", "instrument", "score"}
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "policy rank leg schema is invalid",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
            context={"leg_id": leg_id, "missing_columns": sorted(required - set(frame.columns))},
        )
    data = frame.loc[frame["trade_date"].isin(decisions), ["trade_date", "instrument", "score"]].copy()
    data["trade_date"] = pd.to_datetime(data["trade_date"]).dt.normalize()
    data["instrument"] = data["instrument"].astype(str).str.upper()
    data["raw_score"] = pd.to_numeric(data["score"], errors="coerce")
    if data.empty or data["raw_score"].isna().any() or data.duplicated(["trade_date", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "policy rank leg rows are invalid",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
            context={"leg_id": leg_id},
        )
    grouped = data.groupby("trade_date")["raw_score"]
    mean = grouped.transform("mean")
    std = grouped.transform(lambda values: values.std(ddof=0))
    data["normalized_score"] = np.where(std > 0, (data["raw_score"] - mean) / std, 0.0)
    data = data.sort_values(["trade_date", "normalized_score", "instrument"], ascending=[True, False, True])
    data["leg_rank"] = data.groupby("trade_date").cumcount().add(1)
    return data[["trade_date", "instrument", "raw_score", "normalized_score", "leg_rank"]]


def _next_trade_map(
    decisions: pd.DatetimeIndex, trading_calendar: Sequence[pd.Timestamp]
) -> dict[pd.Timestamp, pd.Timestamp]:
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    positions = calendar.searchsorted(decisions)
    if (positions >= len(calendar)).any() or not (calendar[positions] == decisions).all():
        raise AdvisoryModelFirstError(
            "policy decision date is absent from the trading calendar",
            reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
        )
    targets = positions + 1
    if (targets >= len(calendar)).any():
        raise AdvisoryModelFirstError(
            "policy target date cannot be resolved",
            reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
        )
    return {decision: calendar[position] for decision, position in zip(decisions, targets, strict=True)}
