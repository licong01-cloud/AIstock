from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.labels import (
    CLOSE_COST,
    OPEN_COST,
    _benchmark_table,
    _finite,
    _market_row,
    _one_price_limit_down,
    _one_price_limit_up,
)
from backend.services.advisory_model_first.outcome_contracts import OUTCOME_HORIZONS
from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit


@dataclass(frozen=True)
class OutcomeLabelBuildResult:
    labels: pd.DataFrame
    coverage: pd.DataFrame


def build_multi_horizon_outcome_labels(
    *,
    candidates: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    horizons: tuple[int, ...] = OUTCOME_HORIZONS,
) -> OutcomeLabelBuildResult:
    if tuple(horizons) != OUTCOME_HORIZONS:
        raise AdvisoryModelFirstError(
            "outcome label horizons differ from the frozen contract",
            reason_code="ADVISORY_OUTCOME_REQUEST_INVALID",
            context={"horizons": list(horizons)},
        )
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    calendar_position = {value: position for position, value in enumerate(calendar)}
    market = daily.sort_index()
    benchmark = _benchmark_table(benchmark_daily)
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
        }
        if decision not in calendar_position or target not in calendar_position:
            _mark_all_unavailable(row, "LABEL_NOT_MATURE", "calendar_date_missing")
            rows.append(row)
            continue
        if calendar_position[target] != calendar_position[decision] + 1:
            raise AdvisoryModelFirstError(
                "outcome target date is not the next trading day",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                context={"decision_date": decision.date().isoformat(), "target_date": target.date().isoformat()},
            )
        entry = _market_row(market, target, symbol)
        if entry is None:
            _mark_all_unavailable(row, "NO_EXECUTABLE_ENTRY", "entry_market_row_missing")
            rows.append(row)
            continue
        if (target, symbol) in suspended or _one_price_limit_up(entry):
            _mark_all_unavailable(row, "NO_EXECUTABLE_ENTRY", "entry_not_executable")
            rows.append(row)
            continue
        entry_price = _finite(entry.get("open"))
        if entry_price is None or entry_price <= 0:
            _mark_all_unavailable(row, "NO_EXECUTABLE_ENTRY", "entry_price_invalid")
            rows.append(row)
            continue
        row["entry_date"] = target
        row["entry_price"] = entry_price
        utilities: list[tuple[float, int]] = []
        for horizon in OUTCOME_HORIZONS:
            outcome = _build_horizon_outcome(
                market=market,
                benchmark=benchmark,
                suspended=suspended,
                calendar=calendar,
                calendar_position=calendar_position,
                decision=decision,
                target=target,
                symbol=symbol,
                entry_price=entry_price,
                horizon=horizon,
            )
            row.update(outcome)
            utility = outcome.get(f"utility_{horizon}")
            if outcome[f"label_status_{horizon}"] == "MATURE_EXECUTABLE" and utility is not None:
                utilities.append((float(utility), horizon))
        if len(utilities) == len(OUTCOME_HORIZONS):
            row["optimal_holding_bucket"] = max(utilities, key=lambda value: (value[0], -value[1]))[1]
            row["holding_label_status"] = "AVAILABLE"
        else:
            row["optimal_holding_bucket"] = np.nan
            row["holding_label_status"] = "UNAVAILABLE"
        rows.append(row)
    labels = pd.DataFrame(rows)
    _ensure_label_columns(labels)
    coverage_rows = []
    for decision, group in labels.groupby("decision_as_of_trade_date", sort=True):
        receipt: dict[str, object] = {
            "decision_as_of_trade_date": decision,
            "candidate_count": len(group),
            "holding_modelable_count": int((group["holding_label_status"] == "AVAILABLE").sum()),
        }
        for horizon in OUTCOME_HORIZONS:
            receipt[f"modelable_count_{horizon}"] = int(
                (group[f"label_status_{horizon}"] == "MATURE_EXECUTABLE").sum()
            )
        coverage_rows.append(receipt)
    return OutcomeLabelBuildResult(labels=labels, coverage=pd.DataFrame(coverage_rows))


def apply_outcome_split(
    labels: pd.DataFrame,
    split: OutcomeDateSplit,
    *,
    data_cutoff: str,
) -> pd.DataFrame:
    result = labels.copy()
    decisions = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    result["split"] = "purged"
    split_dates = {name: tuple(getattr(split, name)) for name in ("train", "validation", "test")}
    for name, dates in split_dates.items():
        result.loc[decisions.isin(dates), "split"] = name
    boundaries = {
        "train": pd.Timestamp(split.purge_1[-1]).normalize(),
        "validation": pd.Timestamp(split.purge_2[-1]).normalize(),
        "test": pd.Timestamp(data_cutoff).normalize(),
    }
    for horizon in OUTCOME_HORIZONS:
        status = result[f"label_status_{horizon}"] == "MATURE_EXECUTABLE"
        actual_exit = pd.to_datetime(result[f"actual_exit_date_{horizon}"], errors="coerce").dt.normalize()
        modelable = pd.Series(False, index=result.index)
        for name, boundary in boundaries.items():
            modelable |= status & (result["split"] == name) & actual_exit.le(boundary)
        result[f"modelable_{horizon}"] = modelable
    result["holding_modelable"] = result["split"].isin(["train", "validation", "test"])
    for horizon in OUTCOME_HORIZONS:
        result["holding_modelable"] &= result[f"modelable_{horizon}"]
    return result


def _build_horizon_outcome(
    *,
    market: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspended: set[tuple[pd.Timestamp, str]],
    calendar: pd.DatetimeIndex,
    calendar_position: dict[pd.Timestamp, int],
    decision: pd.Timestamp,
    target: pd.Timestamp,
    symbol: str,
    entry_price: float,
    horizon: int,
) -> dict[str, object]:
    def prefix(name: str) -> str:
        return f"{name}_{horizon}"

    nominal_position = calendar_position[decision] + horizon
    if nominal_position >= len(calendar):
        return {prefix("label_status"): "LABEL_NOT_MATURE", prefix("label_reason"): "nominal_exit_missing"}
    actual_exit: pd.Timestamp | None = None
    exit_row: pd.Series | None = None
    for position in range(nominal_position, min(nominal_position + 6, len(calendar))):
        candidate_exit = calendar[position]
        candidate_row = _market_row(market, candidate_exit, symbol)
        if candidate_row is None or (candidate_exit, symbol) in suspended or _one_price_limit_down(candidate_row):
            continue
        actual_exit = candidate_exit
        exit_row = candidate_row
        break
    if actual_exit is None or exit_row is None:
        return {prefix("label_status"): "RIGHT_CENSORED_EXIT", prefix("label_reason"): "exit_not_executable"}
    exit_price = _finite(exit_row.get("close"))
    benchmark_entry = benchmark.loc[target] if target in benchmark.index else None
    benchmark_exit = benchmark.loc[actual_exit] if actual_exit in benchmark.index else None
    if exit_price is None or exit_price <= 0 or benchmark_entry is None or benchmark_exit is None:
        return {prefix("label_status"): "LABEL_NOT_MATURE", prefix("label_reason"): "exit_or_benchmark_missing"}
    benchmark_open = _finite(benchmark_entry.get("open"))
    benchmark_close = _finite(benchmark_exit.get("close"))
    if benchmark_open is None or benchmark_close is None or benchmark_open <= 0:
        return {prefix("label_status"): "LABEL_NOT_MATURE", prefix("label_reason"): "benchmark_price_invalid"}
    path_dates = calendar[calendar_position[target] : calendar_position[actual_exit] + 1]
    try:
        path = market.loc[(path_dates, symbol), ["high", "low"]]
    except KeyError:
        path = pd.DataFrame()
    if len(path) != len(path_dates) or path[["high", "low"]].isna().any().any():
        return {prefix("label_status"): "LABEL_NOT_MATURE", prefix("label_reason"): "path_price_missing"}
    denominator = entry_price * (1 + OPEN_COST)
    net_path_high = pd.to_numeric(path["high"], errors="coerce") * (1 - CLOSE_COST) / denominator - 1
    net_path_low = pd.to_numeric(path["low"], errors="coerce") * (1 - CLOSE_COST) / denominator - 1
    stock_return = exit_price * (1 - CLOSE_COST) / denominator - 1
    benchmark_return = benchmark_close / benchmark_open - 1
    excess_return = stock_return - benchmark_return
    path_mfe = max(0.0, float(net_path_high.max()))
    path_mae_loss = max(0.0, -float(net_path_low.min()))
    utility = excess_return + 0.25 * path_mfe - 0.50 * path_mae_loss
    return {
        prefix("nominal_exit_date"): calendar[nominal_position],
        prefix("actual_exit_date"): actual_exit,
        prefix("actual_holding_trading_days"): calendar_position[actual_exit] - calendar_position[target] + 1,
        prefix("stock_net_return"): stock_return,
        prefix("benchmark_return"): benchmark_return,
        prefix("excess_return"): excess_return,
        prefix("path_mfe"): path_mfe,
        prefix("path_mae_loss"): path_mae_loss,
        prefix("utility"): utility,
        prefix("positive_excess"): int(excess_return > 0),
        prefix("signal_survival"): int(utility > 0),
        prefix("label_status"): "MATURE_EXECUTABLE",
        prefix("label_reason"): None,
    }


def _mark_all_unavailable(row: dict[str, object], status: str, reason: str) -> None:
    for horizon in OUTCOME_HORIZONS:
        row[f"label_status_{horizon}"] = status
        row[f"label_reason_{horizon}"] = reason
    row["optimal_holding_bucket"] = np.nan
    row["holding_label_status"] = "UNAVAILABLE"


def _ensure_label_columns(labels: pd.DataFrame) -> None:
    for horizon in OUTCOME_HORIZONS:
        for column in (
            "nominal_exit_date",
            "actual_exit_date",
            "actual_holding_trading_days",
            "stock_net_return",
            "benchmark_return",
            "excess_return",
            "path_mfe",
            "path_mae_loss",
            "utility",
            "positive_excess",
            "signal_survival",
            "label_status",
            "label_reason",
        ):
            name = f"{column}_{horizon}"
            if name not in labels:
                labels[name] = np.nan
