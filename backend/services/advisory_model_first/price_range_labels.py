from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.labels import _finite, _market_row, _one_price_limit_up
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit


@dataclass(frozen=True)
class PriceRangeLabelBuildResult:
    labels: pd.DataFrame
    coverage: pd.DataFrame


def build_price_range_labels(
    *,
    candidates: pd.DataFrame,
    daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
) -> PriceRangeLabelBuildResult:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    missing = sorted(set(keys) - set(candidates.columns))
    if missing:
        raise AdvisoryModelFirstError(
            "price-range candidates omit required identity columns",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE",
            context={"missing_columns": missing},
        )
    if candidates.duplicated(keys).any():
        raise AdvisoryModelFirstError(
            "price-range candidates contain duplicate identities",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE",
        )
    calendar = (
        pd.DatetimeIndex(pd.to_datetime(list(trading_calendar)))
        .normalize()
        .sort_values()
        .unique()
    )
    calendar_position = {value: position for position, value in enumerate(calendar)}
    market = daily.sort_index()
    suspended = {
        (pd.Timestamp(item.trade_date).normalize(), str(item.instrument).upper())
        for item in suspend_rows.itertuples(index=False)
    }
    rows: list[dict[str, object]] = []
    for candidate in candidates.itertuples(index=False):
        decision = pd.Timestamp(candidate.decision_as_of_trade_date).normalize()
        target = pd.Timestamp(candidate.target_trade_date).normalize()
        symbol = str(candidate.instrument).upper()
        row: dict[str, object] = {
            "decision_as_of_trade_date": decision,
            "target_trade_date": target,
            "instrument": symbol,
            "entry_label_status": "UNAVAILABLE",
            "entry_label_reason": None,
            "entry_executable": np.nan,
            "entry_gap_return": np.nan,
        }
        if decision not in calendar_position or target not in calendar_position:
            row["entry_label_reason"] = "calendar_date_missing"
            rows.append(row)
            continue
        if calendar_position[target] != calendar_position[decision] + 1:
            raise AdvisoryModelFirstError(
                "price-range target date is not the next trading day",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                context={
                    "decision_date": decision.date().isoformat(),
                    "target_date": target.date().isoformat(),
                },
            )
        target_row = _market_row(market, target, symbol)
        if (target, symbol) in suspended:
            row.update(
                entry_label_status="AVAILABLE",
                entry_label_reason="target_authoritatively_suspended",
                entry_executable=0,
            )
            rows.append(row)
            continue
        if target_row is None:
            row["entry_label_reason"] = "target_market_row_missing_unexplained"
            rows.append(row)
            continue
        if _one_price_limit_up(target_row):
            row.update(
                entry_label_status="AVAILABLE",
                entry_label_reason="target_one_price_limit_up",
                entry_executable=0,
            )
            rows.append(row)
            continue
        decision_row = _market_row(market, decision, symbol)
        target_open = _finite(target_row.get("open"))
        target_pre_close = _finite(target_row.get("prev_close"))
        decision_close = _finite(decision_row.get("close")) if decision_row is not None else None
        if (
            target_open is None
            or target_open <= 0
            or target_pre_close is None
            or target_pre_close <= 0
            or decision_close is None
            or decision_close <= 0
        ):
            row["entry_label_reason"] = "target_or_decision_price_invalid"
            rows.append(row)
            continue
        row.update(
            entry_label_status="AVAILABLE",
            entry_label_reason="target_open_executable",
            entry_executable=1,
            entry_gap_return=target_open / decision_close - 1.0,
        )
        rows.append(row)
    labels = pd.DataFrame(rows)
    coverage_rows: list[dict[str, object]] = []
    for decision, group in labels.groupby("decision_as_of_trade_date", sort=True):
        available = group["entry_label_status"].eq("AVAILABLE")
        coverage_rows.append(
            {
                "decision_as_of_trade_date": decision,
                "candidate_count": len(group),
                "positive_count": int((available & group["entry_executable"].eq(1)).sum()),
                "authoritative_negative_count": int(
                    (available & group["entry_executable"].eq(0)).sum()
                ),
                "unavailable_count": int((~available).sum()),
            }
        )
    return PriceRangeLabelBuildResult(
        labels=labels.sort_values(keys).reset_index(drop=True),
        coverage=pd.DataFrame(coverage_rows),
    )


def apply_price_range_split(
    labels: pd.DataFrame,
    split: OutcomeDateSplit,
) -> pd.DataFrame:
    result = labels.copy()
    decisions = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    result["split"] = "purged"
    for name in ("train", "validation", "test"):
        result.loc[decisions.isin(getattr(split, name)), "split"] = name
    active = result["split"].isin(["train", "validation", "test"])
    available = result["entry_label_status"].eq("AVAILABLE")
    result["binary_modelable"] = active & available
    result["gap_modelable"] = result["binary_modelable"] & result["entry_executable"].eq(1)
    return result
