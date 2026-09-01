from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.time_split import PurgedDateSplit

OPEN_COST = 0.000095
CLOSE_COST = 0.000595


@dataclass(frozen=True)
class LabelBuildResult:
    labels: pd.DataFrame
    coverage: pd.DataFrame


def build_five_day_labels(
    *,
    candidates: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
) -> LabelBuildResult:
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    calendar_position = {date: position for position, date in enumerate(calendar)}
    market = daily.sort_index()
    benchmark = _benchmark_table(benchmark_daily)
    suspended = {
        (pd.Timestamp(item.trade_date).normalize(), str(item.instrument).upper())
        for item in suspend_rows.itertuples(index=False)
    }
    results: list[dict[str, object]] = []
    for candidate in candidates.itertuples(index=False):
        decision = pd.Timestamp(candidate.decision_as_of_trade_date).normalize()
        target = pd.Timestamp(candidate.target_trade_date).normalize()
        symbol = str(candidate.instrument).upper()
        base = {
            "decision_as_of_trade_date": decision,
            "target_trade_date": target,
            "instrument": symbol,
        }
        if decision not in calendar_position or target not in calendar_position:
            results.append({**base, "label_status": "LABEL_NOT_MATURE", "label_reason": "calendar_date_missing"})
            continue
        if calendar_position[target] != calendar_position[decision] + 1:
            raise AdvisoryModelFirstError(
                "candidate target date is not the next trading day",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                context={"decision_date": decision.date().isoformat(), "target_date": target.date().isoformat()},
            )
        nominal_position = calendar_position[decision] + 5
        if nominal_position >= len(calendar):
            results.append({**base, "label_status": "LABEL_NOT_MATURE", "label_reason": "nominal_exit_missing"})
            continue
        entry = _market_row(market, target, symbol)
        if entry is None:
            results.append({**base, "label_status": "NO_EXECUTABLE_ENTRY", "label_reason": "entry_market_row_missing"})
            continue
        if (target, symbol) in suspended or _one_price_limit_up(entry):
            results.append({**base, "label_status": "NO_EXECUTABLE_ENTRY", "label_reason": "entry_not_executable"})
            continue
        entry_price = _finite(entry.get("open"))
        if entry_price is None or entry_price <= 0:
            results.append({**base, "label_status": "NO_EXECUTABLE_ENTRY", "label_reason": "entry_price_invalid"})
            continue

        actual_exit: pd.Timestamp | None = None
        exit_row: pd.Series | None = None
        for position in range(nominal_position, min(nominal_position + 6, len(calendar))):
            candidate_exit = calendar[position]
            row = _market_row(market, candidate_exit, symbol)
            if row is None or (candidate_exit, symbol) in suspended or _one_price_limit_down(row):
                continue
            actual_exit = candidate_exit
            exit_row = row
            break
        if actual_exit is None or exit_row is None:
            results.append({**base, "label_status": "RIGHT_CENSORED_EXIT", "label_reason": "exit_not_executable"})
            continue
        exit_price = _finite(exit_row.get("close"))
        benchmark_entry = benchmark.loc[target] if target in benchmark.index else None
        benchmark_exit = benchmark.loc[actual_exit] if actual_exit in benchmark.index else None
        if exit_price is None or exit_price <= 0 or benchmark_entry is None or benchmark_exit is None:
            results.append({**base, "label_status": "LABEL_NOT_MATURE", "label_reason": "exit_or_benchmark_missing"})
            continue
        benchmark_open = _finite(benchmark_entry.get("open"))
        benchmark_close = _finite(benchmark_exit.get("close"))
        if benchmark_open is None or benchmark_close is None or benchmark_open <= 0:
            results.append({**base, "label_status": "LABEL_NOT_MATURE", "label_reason": "benchmark_price_invalid"})
            continue
        path_dates = calendar[calendar_position[target] : calendar_position[actual_exit] + 1]
        try:
            path = market.loc[(path_dates, symbol), ["high", "low"]]
        except KeyError:
            path = pd.DataFrame()
        if len(path) != len(path_dates) or path[["high", "low"]].isna().any().any():
            results.append({**base, "label_status": "LABEL_NOT_MATURE", "label_reason": "path_price_missing"})
            continue
        net_path_high = pd.to_numeric(path["high"], errors="coerce") * (1 - CLOSE_COST) / (
            entry_price * (1 + OPEN_COST)
        ) - 1
        net_path_low = pd.to_numeric(path["low"], errors="coerce") * (1 - CLOSE_COST) / (
            entry_price * (1 + OPEN_COST)
        ) - 1
        stock_return = exit_price * (1 - CLOSE_COST) / (entry_price * (1 + OPEN_COST)) - 1
        benchmark_return = benchmark_close / benchmark_open - 1
        excess_return = stock_return - benchmark_return
        path_mfe = max(0.0, float(net_path_high.max()))
        path_mae_loss = max(0.0, -float(net_path_low.min()))
        utility = excess_return + 0.25 * path_mfe - 0.50 * path_mae_loss
        results.append(
            {
                **base,
                "entry_date": target,
                "entry_price": entry_price,
                "nominal_exit_date": calendar[nominal_position],
                "actual_exit_date": actual_exit,
                "exit_price": exit_price,
                "actual_holding_trading_days": calendar_position[actual_exit] - calendar_position[target] + 1,
                "stock_net_return_5": stock_return,
                "benchmark_return_5": benchmark_return,
                "excess_return_5": excess_return,
                "path_mfe_5": path_mfe,
                "path_mae_loss_5": path_mae_loss,
                "utility_5": utility,
                "label_status": "MATURE_EXECUTABLE",
                "label_reason": None,
            }
        )
    labels = pd.DataFrame(results)
    for column in (
        "entry_date",
        "entry_price",
        "nominal_exit_date",
        "actual_exit_date",
        "exit_price",
        "actual_holding_trading_days",
        "stock_net_return_5",
        "benchmark_return_5",
        "excess_return_5",
        "path_mfe_5",
        "path_mae_loss_5",
        "utility_5",
    ):
        if column not in labels:
            labels[column] = np.nan
    _assign_group_relevance(labels, eligible=labels["label_status"] == "MATURE_EXECUTABLE")
    coverage = (
        labels.groupby("decision_as_of_trade_date")
        .agg(
            candidate_count=("instrument", "size"),
            mature_executable_count=("label_status", lambda values: int((values == "MATURE_EXECUTABLE").sum())),
            modelable_count=("group_label_status", lambda values: int((values == "AVAILABLE").sum())),
        )
        .reset_index()
    )
    coverage["status"] = np.where(coverage["modelable_count"] >= 5, "available", "unavailable")
    return LabelBuildResult(labels=labels, coverage=coverage)


def filter_labels_for_purged_split(labels: pd.DataFrame, split: PurgedDateSplit, *, data_cutoff: str) -> pd.DataFrame:
    result = labels.copy()
    decision = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    exit_date = pd.to_datetime(result["actual_exit_date"], errors="coerce").dt.normalize()
    boundaries = {
        "train": split.purge_1[-1],
        "validation": split.purge_2[-1],
        "test": pd.Timestamp(data_cutoff),
    }
    result["split"] = "purged"
    for name, dates in (("train", split.train), ("validation", split.validation), ("test", split.test)):
        mask = decision.isin(dates)
        result.loc[mask, "split"] = name
        crosses = mask & (exit_date > boundaries[name])
        result.loc[crosses, "group_label_status"] = "CROSSES_PURGE_BOUNDARY"
    crosses_boundary = result["group_label_status"] == "CROSSES_PURGE_BOUNDARY"
    eligible = (
        (result["label_status"] == "MATURE_EXECUTABLE")
        & result["split"].isin(["train", "validation", "test"])
        & ~crosses_boundary
    )
    _assign_group_relevance(result, eligible=eligible)
    result.loc[crosses_boundary, "group_label_status"] = "CROSSES_PURGE_BOUNDARY"
    return result


def _assign_group_relevance(labels: pd.DataFrame, *, eligible: pd.Series) -> None:
    labels["relevance"] = np.nan
    labels["group_label_status"] = "UNAVAILABLE"
    for _, positions in labels[eligible].groupby("decision_as_of_trade_date").groups.items():
        utilities = labels.loc[positions, "utility_5"]
        distinct = utilities.nunique(dropna=True)
        if len(positions) < 5:
            labels.loc[positions, "group_label_status"] = "INSUFFICIENT_EXECUTABLE_LABELS"
            continue
        if distinct <= 1:
            labels.loc[positions, "relevance"] = 0
            labels.loc[positions, "group_label_status"] = "NO_LABEL_VARIATION"
            continue
        dense = utilities.rank(method="dense", ascending=True).astype(int) - 1
        labels.loc[positions, "relevance"] = np.floor(4 * dense / (distinct - 1)).astype(int)
        labels.loc[positions, "group_label_status"] = "AVAILABLE"


def _market_row(frame: pd.DataFrame, date: pd.Timestamp, symbol: str) -> pd.Series | None:
    try:
        row = frame.loc[(date, symbol)]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        raise AdvisoryModelFirstError(
            "daily market input has duplicate date-symbol rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"date": date.date().isoformat(), "instrument": symbol},
        )
    return row


def _one_price_limit_up(row: pd.Series) -> bool:
    limit_flag = _finite(row.get("limit_up"))
    low = _finite(row.get("low"))
    factor = _finite(row.get("factor"))
    limit_price = _finite(row.get("up_limit_price"))
    return bool(
        limit_flag is not None
        and limit_flag > 0
        and low is not None
        and factor is not None
        and limit_price is not None
        and low >= limit_price * factor - 1e-10
    )


def _one_price_limit_down(row: pd.Series) -> bool:
    limit_flag = _finite(row.get("limit_down"))
    high = _finite(row.get("high"))
    factor = _finite(row.get("factor"))
    limit_price = _finite(row.get("down_limit_price"))
    return bool(
        limit_flag is not None
        and limit_flag > 0
        and high is not None
        and factor is not None
        and limit_price is not None
        and high <= limit_price * factor + 1e-10
    )


def _benchmark_table(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"open", "close"}
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "benchmark input is missing open or close",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(frame.columns))},
        )
    reset = frame.reset_index()
    if reset["datetime"].duplicated().any():
        raise AdvisoryModelFirstError(
            "benchmark input contains duplicate dates",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    return reset.set_index("datetime")[["open", "close"]].sort_index()


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None
