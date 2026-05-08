"""Offline QE loop validation for event-signal overlays.

This research-only module reads fixed QE loop artifacts plus a pre-exported
`event_signal_daily_overlay` CSV.  It does not modify QE runtime, Selection
Center, Paper v2, or live trading consumers.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

CASH_SIMULATOR_VERSION = "event_signal_overlay_cash_counterfactual_v1_20260507"
NEXT_CANDIDATE_SIMULATOR_VERSION = "event_signal_overlay_next_candidate_v1_20260507"
SIMULATOR_VERSION = CASH_SIMULATOR_VERSION


@dataclass(frozen=True)
class CandidateScore:
    ts_code: str
    score: float


@dataclass(frozen=True)
class ReplacementSlot:
    original_symbol: str
    replacement_symbol: str
    opened_trade_date: dt.date
    opened_reason: str
    score: float


@dataclass(frozen=True)
class ValidationSummary:
    validation_key: str
    experiment_id: str
    loop_id: str
    profile_id: str
    date_from: dt.date
    date_to: dt.date
    baseline_metrics: dict[str, Any]
    overlay_metrics: dict[str, Any]
    delta_metrics: dict[str, Any]
    hit_stats: dict[str, Any]
    report_path: str
    json_path: str


def _json_dumps(value: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=indent)


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as fh:
        return pickle.load(fh)


def find_portfolio_artifact_dir(loop_path: Path) -> Path:
    candidates = list(loop_path.glob("mlruns/*/*/artifacts/portfolio_analysis/report_normal_1day.pkl"))
    if not candidates:
        raise FileNotFoundError(f"cannot find report_normal_1day.pkl under {loop_path}")
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0].parent


def find_loop_artifact_dir(loop_path: Path) -> Path:
    candidates = list(loop_path.glob("mlruns/*/*/artifacts/pred.pkl"))
    if not candidates:
        raise FileNotFoundError(f"cannot find pred.pkl under {loop_path}")
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0].parent


def _date_key(value: Any) -> dt.date:
    return pd.Timestamp(value).date()


def _position_dict(position: Any) -> dict[str, dict[str, float]]:
    raw = getattr(position, "position", position)
    out: dict[str, dict[str, float]] = {}
    for symbol, payload in raw.items():
        if symbol in {"cash", "now_account_value"} or not isinstance(payload, dict):
            continue
        amount = float(payload.get("amount") or 0.0)
        price = float(payload.get("price") or 0.0)
        weight = float(payload.get("weight") or 0.0)
        if amount <= 0 or price <= 0:
            continue
        out[str(symbol)] = {"amount": amount, "price": price, "weight": weight}
    return out


def load_positions(positions_path: Path) -> dict[dt.date, dict[str, dict[str, float]]]:
    raw = _load_pickle(positions_path)
    return {_date_key(date_value): _position_dict(position) for date_value, position in raw.items()}


def load_report(report_path: Path) -> pd.DataFrame:
    report = _load_pickle(report_path)
    if not isinstance(report, pd.DataFrame):
        raise TypeError(f"report artifact must be DataFrame, got {type(report).__name__}")
    report = report.copy()
    report.index = pd.to_datetime(report.index)
    return report.sort_index()


def load_overlay_csv(path: Path) -> pd.DataFrame:
    overlay = pd.read_csv(path)
    required = {"trade_date", "ts_code", "can_buy", "force_exit", "policy_risk_level", "primary_action"}
    missing = required.difference(overlay.columns)
    if missing:
        raise ValueError(f"overlay csv missing columns: {sorted(missing)}")
    overlay = overlay.copy()
    overlay["trade_date"] = pd.to_datetime(overlay["trade_date"]).dt.date
    for column in ["can_buy", "force_exit"]:
        if overlay[column].dtype == object:
            overlay[column] = overlay[column].astype(str).str.lower().isin(["true", "1", "yes"])
    return overlay


def load_prediction_scores(path: Path) -> dict[dt.date, list[CandidateScore]]:
    pred = _load_pickle(path)
    if not isinstance(pred, pd.DataFrame):
        raise TypeError(f"prediction artifact must be DataFrame, got {type(pred).__name__}")
    if not isinstance(pred.index, pd.MultiIndex) or pred.index.nlevels < 2:
        raise ValueError("prediction artifact index must be MultiIndex(datetime, instrument)")
    score_column = "score" if "score" in pred.columns else pred.columns[0]
    frame = pred[[score_column]].copy()
    frame = frame.reset_index()
    date_col = "datetime" if "datetime" in frame.columns else frame.columns[0]
    symbol_col = "instrument" if "instrument" in frame.columns else frame.columns[1]
    frame["trade_date"] = pd.to_datetime(frame[date_col]).dt.date
    frame["ts_code"] = frame[symbol_col].astype(str)
    frame["score_value"] = pd.to_numeric(frame[score_column], errors="coerce")
    frame = frame.dropna(subset=["score_value"])
    scores: dict[dt.date, list[CandidateScore]] = {}
    for trade_date, group in frame.groupby("trade_date", sort=True):
        ordered = group.sort_values("score_value", ascending=False)
        scores[_date_key(trade_date)] = [
            CandidateScore(ts_code=str(row.ts_code), score=float(row.score_value))
            for row in ordered.itertuples(index=False)
        ]
    return scores


def load_price_returns_csv(path: Path) -> dict[dt.date, dict[str, float]]:
    frame = pd.read_csv(path)
    required = {"trade_date", "ts_code", "return"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"price return csv missing columns: {sorted(missing)}")
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    frame["ts_code"] = frame["ts_code"].astype(str)
    frame["return"] = pd.to_numeric(frame["return"], errors="coerce")
    frame = frame.dropna(subset=["return"])
    returns: dict[dt.date, dict[str, float]] = {}
    for row in frame[["trade_date", "ts_code", "return"]].to_dict("records"):
        returns.setdefault(row["trade_date"], {})[str(row["ts_code"])] = float(row["return"])
    return returns


def write_price_returns_csv(price_returns: dict[dt.date, dict[str, float]], path: Path) -> None:
    rows = [
        {"trade_date": trade_date.isoformat(), "ts_code": symbol, "return": value}
        for trade_date, values in sorted(price_returns.items())
        for symbol, value in sorted(values.items())
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["trade_date", "ts_code", "return"]).to_csv(path, index=False)


def fetch_price_returns_from_db(
    *,
    symbols: set[str],
    date_from: dt.date,
    date_to: dt.date,
) -> dict[dt.date, dict[str, float]]:
    if not symbols:
        return {}
    from dotenv import load_dotenv

    from backend.db.pg_pool import get_conn

    load_dotenv(override=True)
    lookback_start = date_from - dt.timedelta(days=30)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, RTRIM(ts_code) AS ts_code, close_li::double precision AS close_li
              FROM market.kline_daily_raw
             WHERE trade_date BETWEEN %s AND %s
               AND RTRIM(ts_code) = ANY(%s)
               AND close_li IS NOT NULL
               AND close_li > 0
             ORDER BY ts_code, trade_date
            """,
            (lookback_start, date_to, sorted(symbols)),
        )
        rows = cur.fetchall()
    frame = pd.DataFrame(rows, columns=["trade_date", "ts_code", "close_li"])
    if frame.empty:
        return {}
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    frame["close_li"] = pd.to_numeric(frame["close_li"], errors="coerce")
    frame = frame.dropna(subset=["close_li"]).sort_values(["ts_code", "trade_date"])
    frame["return"] = frame.groupby("ts_code")["close_li"].pct_change()
    frame = frame[(frame["trade_date"] >= date_from) & (frame["trade_date"] <= date_to)]
    frame = frame.dropna(subset=["return"])
    price_returns: dict[dt.date, dict[str, float]] = {}
    for row in frame[["trade_date", "ts_code", "return"]].to_dict("records"):
        price_returns.setdefault(row["trade_date"], {})[str(row["ts_code"])] = float(row["return"])
    return price_returns


def _max_drawdown(account: pd.Series) -> float:
    nav = account / float(account.iloc[0])
    dd = nav / nav.cummax() - 1.0
    return float(dd.min())


def _cagr(account: pd.Series) -> float:
    if len(account) <= 1:
        return 0.0
    years = (len(account) - 1) / 242.0
    if years <= 0:
        return 0.0
    return float((float(account.iloc[-1]) / float(account.iloc[0])) ** (1.0 / years) - 1.0)


def _metrics_from_account(report: pd.DataFrame, account: pd.Series) -> dict[str, Any]:
    returns = account.pct_change().fillna(0.0)
    return {
        "rows": int(len(account)),
        "start_date": _date_key(account.index[0]).isoformat(),
        "end_date": _date_key(account.index[-1]).isoformat(),
        "initial_account": float(account.iloc[0]),
        "final_account": float(account.iloc[-1]),
        "total_return": float(account.iloc[-1] / account.iloc[0] - 1.0),
        "cagr": _cagr(account),
        "max_drawdown": _max_drawdown(account),
        "avg_daily_return": float(returns.mean()),
        "daily_volatility": float(returns.std()) if len(returns) > 1 else 0.0,
        "avg_cash_ratio": float((report["cash"] / report["account"]).mean()) if {"cash", "account"}.issubset(report.columns) else None,
    }


def run_cash_counterfactual(
    *,
    positions: dict[dt.date, dict[str, dict[str, float]]],
    report: pd.DataFrame,
    overlay: pd.DataFrame,
    date_from: dt.date,
    date_to: dt.date,
) -> tuple[pd.Series, dict[str, Any]]:
    report_window = report[(report.index.date >= date_from) & (report.index.date <= date_to)].copy()
    if report_window.empty:
        raise ValueError("report has no rows in requested date window")

    overlay_window = overlay[(overlay["trade_date"] >= date_from) & (overlay["trade_date"] <= date_to)].copy()
    overlay_by_date: dict[dt.date, dict[str, dict[str, Any]]] = {}
    for row in overlay_window.to_dict("records"):
        overlay_by_date.setdefault(row["trade_date"], {})[str(row["ts_code"])] = row

    adjusted_returns: list[float] = []
    adjusted_account: list[float] = []
    account_prev = float(report_window["account"].iloc[0])
    adjusted_account.append(account_prev)
    adjusted_returns.append(0.0)

    dates = [_date_key(value) for value in report_window.index]
    blocked_active: set[str] = set()
    buy_hits: list[dict[str, Any]] = []
    force_exit_hits: list[dict[str, Any]] = []
    blocked_symbol_pnl: dict[str, float] = {}
    blocked_symbol_days: dict[str, int] = {}
    daily_blocked_contrib: dict[str, float] = {}

    prev_date: Optional[dt.date] = None
    for i, current_date in enumerate(dates):
        if i == 0:
            prev_date = current_date
            continue
        prev_positions = positions.get(prev_date or current_date, {})
        current_positions = positions.get(current_date, {})
        prev_symbols = set(prev_positions)
        current_symbols = set(current_positions)
        current_overlay = overlay_by_date.get(current_date, {})
        overlay_blocked = {symbol for symbol, row in current_overlay.items() if (not bool(row.get("can_buy", True))) or bool(row.get("force_exit", False))}
        force_exit_symbols = {symbol for symbol, row in current_overlay.items() if bool(row.get("force_exit", False))}

        for symbol in sorted((current_symbols - prev_symbols) & overlay_blocked):
            buy_hits.append({"trade_date": current_date.isoformat(), "ts_code": symbol, "action": current_overlay[symbol].get("primary_action")})
            blocked_active.add(symbol)
        for symbol in sorted(prev_symbols & force_exit_symbols):
            force_exit_hits.append({"trade_date": current_date.isoformat(), "ts_code": symbol, "action": current_overlay[symbol].get("primary_action")})
            blocked_active.add(symbol)

        blocked_active.intersection_update(current_symbols | prev_symbols)
        contribution = 0.0
        for symbol in sorted(blocked_active & prev_symbols & current_symbols):
            prev_price = prev_positions[symbol]["price"]
            current_price = current_positions[symbol]["price"]
            if prev_price <= 0 or current_price <= 0:
                continue
            symbol_return = current_price / prev_price - 1.0
            prev_weight = prev_positions[symbol]["weight"]
            symbol_contribution = prev_weight * symbol_return
            contribution += symbol_contribution
            pnl_money = account_prev * symbol_contribution
            blocked_symbol_pnl[symbol] = blocked_symbol_pnl.get(symbol, 0.0) + pnl_money
            blocked_symbol_days[symbol] = blocked_symbol_days.get(symbol, 0) + 1
        base_return = float(report_window["return"].iloc[i])
        adjusted_return = max(base_return - contribution, -0.999)
        account_prev = account_prev * (1.0 + adjusted_return)
        adjusted_account.append(account_prev)
        adjusted_returns.append(adjusted_return)
        daily_blocked_contrib[current_date.isoformat()] = contribution
        for symbol in list(blocked_active):
            if symbol not in current_symbols:
                blocked_active.remove(symbol)
        prev_date = current_date

    account = pd.Series(adjusted_account, index=report_window.index, name="overlay_account")
    positive = sum(1 for value in blocked_symbol_pnl.values() if value > 0)
    negative = sum(1 for value in blocked_symbol_pnl.values() if value < 0)
    zero = sum(1 for value in blocked_symbol_pnl.values() if value == 0)
    top_harm = sorted(blocked_symbol_pnl.items(), key=lambda item: item[1], reverse=True)[:20]
    top_help = sorted(blocked_symbol_pnl.items(), key=lambda item: item[1])[:20]
    hit_stats = {
        "overlay_rows": int(len(overlay_window)),
        "overlay_symbols": int(overlay_window["ts_code"].nunique()) if not overlay_window.empty else 0,
        "blocked_buy_events": len(buy_hits),
        "force_exit_events": len(force_exit_hits),
        "unique_buy_hit_symbols": len({row["ts_code"] for row in buy_hits}),
        "unique_force_exit_symbols": len({row["ts_code"] for row in force_exit_hits}),
        "blocked_contribution_sum": float(sum(daily_blocked_contrib.values())),
        "blocked_symbol_original_pnl_sum": float(sum(blocked_symbol_pnl.values())),
        "blocked_symbol_positive_count": positive,
        "blocked_symbol_negative_count": negative,
        "blocked_symbol_zero_count": zero,
        "top_original_profit_blocked_symbols": [{"ts_code": k, "pnl": v, "days": blocked_symbol_days.get(k, 0)} for k, v in top_harm],
        "top_original_loss_blocked_symbols": [{"ts_code": k, "pnl": v, "days": blocked_symbol_days.get(k, 0)} for k, v in top_help],
        "sample_buy_hits": buy_hits[:50],
        "sample_force_exit_hits": force_exit_hits[:50],
    }
    return account, hit_stats


def _overlay_by_date(overlay_window: pd.DataFrame) -> dict[dt.date, dict[str, dict[str, Any]]]:
    overlay_by_date: dict[dt.date, dict[str, dict[str, Any]]] = {}
    for row in overlay_window.to_dict("records"):
        overlay_by_date.setdefault(row["trade_date"], {})[str(row["ts_code"])] = row
    return overlay_by_date


def _blocked_symbols(current_overlay: dict[str, dict[str, Any]]) -> set[str]:
    return {
        symbol
        for symbol, row in current_overlay.items()
        if (not bool(row.get("can_buy", True))) or bool(row.get("force_exit", False))
    }


def _select_replacement_candidate(
    *,
    trade_date: dt.date,
    candidate_scores: dict[dt.date, list[CandidateScore]],
    price_returns: dict[dt.date, dict[str, float]],
    required_return_dates: list[dt.date],
    excluded_symbols: set[str],
    blocked_symbols: set[str],
) -> Optional[CandidateScore]:
    if not required_return_dates:
        return None
    for candidate in candidate_scores.get(trade_date, []):
        symbol = candidate.ts_code
        if symbol in excluded_symbols or symbol in blocked_symbols:
            continue
        if all(symbol in price_returns.get(return_date, {}) for return_date in required_return_dates):
            return candidate
    return None


def run_next_candidate_counterfactual(
    *,
    positions: dict[dt.date, dict[str, dict[str, float]]],
    report: pd.DataFrame,
    overlay: pd.DataFrame,
    candidate_scores: dict[dt.date, list[CandidateScore]],
    price_returns: dict[dt.date, dict[str, float]],
    date_from: dt.date,
    date_to: dt.date,
) -> tuple[pd.Series, dict[str, Any]]:
    report_window = report[(report.index.date >= date_from) & (report.index.date <= date_to)].copy()
    if report_window.empty:
        raise ValueError("report has no rows in requested date window")

    overlay_window = overlay[(overlay["trade_date"] >= date_from) & (overlay["trade_date"] <= date_to)].copy()
    overlay_by_date = _overlay_by_date(overlay_window)

    adjusted_returns: list[float] = []
    adjusted_account: list[float] = []
    account_prev = float(report_window["account"].iloc[0])
    adjusted_account.append(account_prev)
    adjusted_returns.append(0.0)

    dates = [_date_key(value) for value in report_window.index]
    blocked_active: set[str] = set()
    replacement_slots: dict[str, ReplacementSlot] = {}
    buy_hits: list[dict[str, Any]] = []
    force_exit_hits: list[dict[str, Any]] = []
    replacement_events: list[dict[str, Any]] = []
    replacement_pnl: dict[str, float] = {}
    original_pnl_removed: dict[str, float] = {}
    blocked_symbol_days: dict[str, int] = {}
    no_replacement_events = 0
    replacement_missing_return_days = 0
    replacement_reselect_events = 0

    def open_replacement(
        *,
        original_symbol: str,
        trade_date: dt.date,
        reason: str,
        required_return_dates: list[dt.date],
        prev_symbols: set[str],
        current_symbols: set[str],
        current_blocked: set[str],
    ) -> None:
        nonlocal no_replacement_events
        active_replacements = {slot.replacement_symbol for slot in replacement_slots.values()}
        excluded = set(prev_symbols) | set(current_symbols) | set(blocked_active) | active_replacements | {original_symbol}
        candidate = _select_replacement_candidate(
            trade_date=trade_date,
            candidate_scores=candidate_scores,
            price_returns=price_returns,
            required_return_dates=required_return_dates,
            excluded_symbols=excluded,
            blocked_symbols=current_blocked,
        )
        if candidate is None:
            replacement_slots.pop(original_symbol, None)
            no_replacement_events += 1
            replacement_events.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "original_symbol": original_symbol,
                    "replacement_symbol": None,
                    "reason": reason,
                    "status": "no_candidate",
                }
            )
            return
        replacement_slots[original_symbol] = ReplacementSlot(
            original_symbol=original_symbol,
            replacement_symbol=candidate.ts_code,
            opened_trade_date=trade_date,
            opened_reason=reason,
            score=candidate.score,
        )
        replacement_events.append(
            {
                "trade_date": trade_date.isoformat(),
                "original_symbol": original_symbol,
                "replacement_symbol": candidate.ts_code,
                "score": candidate.score,
                "reason": reason,
                "status": "opened",
            }
        )

    prev_date: Optional[dt.date] = None
    for i, current_date in enumerate(dates):
        if i == 0:
            prev_date = current_date
            continue
        next_date = dates[i + 1] if i + 1 < len(dates) else None
        prev_positions = positions.get(prev_date or current_date, {})
        current_positions = positions.get(current_date, {})
        prev_symbols = set(prev_positions)
        current_symbols = set(current_positions)
        current_overlay = overlay_by_date.get(current_date, {})
        current_blocked = _blocked_symbols(current_overlay)
        force_exit_symbols = {symbol for symbol, row in current_overlay.items() if bool(row.get("force_exit", False))}

        for original_symbol, slot in list(replacement_slots.items()):
            invalid_reason: Optional[str] = None
            if slot.replacement_symbol in current_blocked:
                invalid_reason = "replacement_blocked"
            elif slot.replacement_symbol in prev_symbols or slot.replacement_symbol in current_symbols:
                invalid_reason = "replacement_overlaps_baseline"
            if invalid_reason and original_symbol in blocked_active:
                replacement_slots.pop(original_symbol, None)
                replacement_reselect_events += 1
                required_dates = [current_date]
                open_replacement(
                    original_symbol=original_symbol,
                    trade_date=current_date,
                    reason=invalid_reason,
                    required_return_dates=required_dates,
                    prev_symbols=prev_symbols,
                    current_symbols=current_symbols,
                    current_blocked=current_blocked,
                )

        for symbol in sorted((current_symbols - prev_symbols) & current_blocked):
            action = current_overlay[symbol].get("primary_action")
            buy_hits.append({"trade_date": current_date.isoformat(), "ts_code": symbol, "action": action})
            blocked_active.add(symbol)
            if symbol not in replacement_slots:
                required_dates = [next_date] if next_date is not None else []
                open_replacement(
                    original_symbol=symbol,
                    trade_date=current_date,
                    reason="blocked_buy",
                    required_return_dates=required_dates,
                    prev_symbols=prev_symbols,
                    current_symbols=current_symbols,
                    current_blocked=current_blocked,
                )
        for symbol in sorted(prev_symbols & force_exit_symbols):
            action = current_overlay[symbol].get("primary_action")
            force_exit_hits.append({"trade_date": current_date.isoformat(), "ts_code": symbol, "action": action})
            blocked_active.add(symbol)
            if symbol not in replacement_slots:
                open_replacement(
                    original_symbol=symbol,
                    trade_date=current_date,
                    reason="force_exit",
                    required_return_dates=[current_date],
                    prev_symbols=prev_symbols,
                    current_symbols=current_symbols,
                    current_blocked=current_blocked,
                )

        blocked_active.intersection_update(current_symbols | prev_symbols)
        original_contribution = 0.0
        replacement_contribution = 0.0
        for symbol in sorted(blocked_active & prev_symbols & current_symbols):
            prev_price = prev_positions[symbol]["price"]
            current_price = current_positions[symbol]["price"]
            if prev_price <= 0 or current_price <= 0:
                continue
            symbol_return = current_price / prev_price - 1.0
            prev_weight = prev_positions[symbol]["weight"]
            symbol_contribution = prev_weight * symbol_return
            original_contribution += symbol_contribution
            original_pnl_removed[symbol] = original_pnl_removed.get(symbol, 0.0) + account_prev * symbol_contribution
            blocked_symbol_days[symbol] = blocked_symbol_days.get(symbol, 0) + 1

            slot = replacement_slots.get(symbol)
            if slot is None:
                continue
            replacement_return = price_returns.get(current_date, {}).get(slot.replacement_symbol)
            if replacement_return is None:
                replacement_missing_return_days += 1
                continue
            replacement_symbol_contribution = prev_weight * replacement_return
            replacement_contribution += replacement_symbol_contribution
            replacement_pnl[slot.replacement_symbol] = (
                replacement_pnl.get(slot.replacement_symbol, 0.0) + account_prev * replacement_symbol_contribution
            )

        base_return = float(report_window["return"].iloc[i])
        adjusted_return = max(base_return - original_contribution + replacement_contribution, -0.999)
        account_prev = account_prev * (1.0 + adjusted_return)
        adjusted_account.append(account_prev)
        adjusted_returns.append(adjusted_return)

        for symbol in list(blocked_active):
            if symbol not in current_symbols:
                blocked_active.remove(symbol)
                replacement_slots.pop(symbol, None)
        prev_date = current_date

    account = pd.Series(adjusted_account, index=report_window.index, name="overlay_next_candidate_account")
    replacement_profit_count = sum(1 for value in replacement_pnl.values() if value > 0)
    replacement_loss_count = sum(1 for value in replacement_pnl.values() if value < 0)
    original_positive = sum(1 for value in original_pnl_removed.values() if value > 0)
    original_negative = sum(1 for value in original_pnl_removed.values() if value < 0)
    top_replacements = sorted(replacement_pnl.items(), key=lambda item: item[1], reverse=True)[:20]
    worst_replacements = sorted(replacement_pnl.items(), key=lambda item: item[1])[:20]
    hit_stats = {
        "overlay_rows": int(len(overlay_window)),
        "overlay_symbols": int(overlay_window["ts_code"].nunique()) if not overlay_window.empty else 0,
        "candidate_score_dates": len(candidate_scores),
        "candidate_score_symbols": len({score.ts_code for scores in candidate_scores.values() for score in scores}),
        "price_return_dates": len(price_returns),
        "price_return_symbols": len({symbol for values in price_returns.values() for symbol in values}),
        "blocked_buy_events": len(buy_hits),
        "force_exit_events": len(force_exit_hits),
        "unique_buy_hit_symbols": len({row["ts_code"] for row in buy_hits}),
        "unique_force_exit_symbols": len({row["ts_code"] for row in force_exit_hits}),
        "replacement_open_events": sum(1 for row in replacement_events if row["status"] == "opened"),
        "replacement_no_candidate_events": no_replacement_events,
        "replacement_reselect_events": replacement_reselect_events,
        "replacement_missing_return_days": replacement_missing_return_days,
        "original_contribution_removed_sum": float(sum(original_pnl_removed.values())),
        "replacement_pnl_sum": float(sum(replacement_pnl.values())),
        "net_replacement_vs_cash_pnl": float(sum(replacement_pnl.values())),
        "original_removed_positive_count": original_positive,
        "original_removed_negative_count": original_negative,
        "replacement_profit_symbol_count": replacement_profit_count,
        "replacement_loss_symbol_count": replacement_loss_count,
        "top_replacement_profit_symbols": [{"ts_code": k, "pnl": v} for k, v in top_replacements],
        "top_replacement_loss_symbols": [{"ts_code": k, "pnl": v} for k, v in worst_replacements],
        "top_original_removed_profit_symbols": [
            {"ts_code": k, "pnl": v, "days": blocked_symbol_days.get(k, 0)}
            for k, v in sorted(original_pnl_removed.items(), key=lambda item: item[1], reverse=True)[:20]
        ],
        "top_original_removed_loss_symbols": [
            {"ts_code": k, "pnl": v, "days": blocked_symbol_days.get(k, 0)}
            for k, v in sorted(original_pnl_removed.items(), key=lambda item: item[1])[:20]
        ],
        "sample_buy_hits": buy_hits[:50],
        "sample_force_exit_hits": force_exit_hits[:50],
        "sample_replacement_events": replacement_events[:100],
    }
    return account, hit_stats


def validate_loop_overlay(
    *,
    experiment_id: str,
    loop_id: str,
    loop_path: Path,
    overlay_csv: Path,
    output_dir: Path,
    profile_id: str,
    date_from: dt.date,
    date_to: dt.date,
    simulator_mode: str = "cash",
    prediction_pkl: Optional[Path] = None,
    price_return_csv: Optional[Path] = None,
) -> ValidationSummary:
    artifact_dir = find_portfolio_artifact_dir(loop_path)
    report_path = artifact_dir / "report_normal_1day.pkl"
    positions_path = artifact_dir / "positions_normal_1day.pkl"
    report = load_report(report_path)
    positions = load_positions(positions_path)
    overlay = load_overlay_csv(overlay_csv)
    report_window = report[(report.index.date >= date_from) & (report.index.date <= date_to)].copy()
    pred_path_for_snapshot: Optional[Path] = None
    if simulator_mode == "cash":
        simulator_version = CASH_SIMULATOR_VERSION
        validation_mode = "stacked_profile"
        overlay_account, hit_stats = run_cash_counterfactual(
            positions=positions,
            report=report,
            overlay=overlay,
            date_from=date_from,
            date_to=date_to,
        )
    elif simulator_mode == "next_candidate":
        simulator_version = NEXT_CANDIDATE_SIMULATOR_VERSION
        validation_mode = "stacked_profile"
        root_artifact_dir = artifact_dir.parent
        pred_path = prediction_pkl or (root_artifact_dir / "pred.pkl")
        pred_path_for_snapshot = pred_path
        candidate_scores = load_prediction_scores(pred_path)
        if price_return_csv is None:
            candidate_symbols = {score.ts_code for scores in candidate_scores.values() for score in scores}
            price_returns = fetch_price_returns_from_db(
                symbols=candidate_symbols,
                date_from=date_from,
                date_to=date_to,
            )
        else:
            price_returns = load_price_returns_csv(price_return_csv)
        overlay_account, hit_stats = run_next_candidate_counterfactual(
            positions=positions,
            report=report,
            overlay=overlay,
            candidate_scores=candidate_scores,
            price_returns=price_returns,
            date_from=date_from,
            date_to=date_to,
        )
    else:
        raise ValueError(f"unsupported simulator_mode: {simulator_mode}")
    baseline_account = report_window["account"]
    baseline_metrics = _metrics_from_account(report_window, baseline_account)
    overlay_metrics = _metrics_from_account(report_window, overlay_account)
    delta_metrics = {
        "final_account_delta": overlay_metrics["final_account"] - baseline_metrics["final_account"],
        "total_return_delta": overlay_metrics["total_return"] - baseline_metrics["total_return"],
        "cagr_delta": overlay_metrics["cagr"] - baseline_metrics["cagr"],
        "max_drawdown_delta": overlay_metrics["max_drawdown"] - baseline_metrics["max_drawdown"],
    }
    validation_key = f"event_signal_validation:{experiment_id}:{loop_id}:{profile_id}:{simulator_version}:{date_from}:{date_to}"
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{validation_key.replace(':', '_')}.json"
    md_path = output_dir / f"{validation_key.replace(':', '_')}.md"
    payload = {
        "validation_key": validation_key,
        "experiment_id": experiment_id,
        "loop_id": loop_id,
        "loop_path": str(loop_path),
        "profile_id": profile_id,
        "validation_mode": validation_mode,
        "simulator_version": simulator_version,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "input_snapshot": {
            "artifact_dir": str(artifact_dir),
            "report_path": str(report_path),
            "positions_path": str(positions_path),
            "overlay_csv": str(overlay_csv),
            "overlay_rows": int(len(overlay)),
            "prediction_pkl": str(pred_path_for_snapshot) if pred_path_for_snapshot else None,
            "price_return_csv": str(price_return_csv) if price_return_csv else None,
        },
        "baseline_metrics": baseline_metrics,
        "overlay_metrics": overlay_metrics,
        "delta_metrics": delta_metrics,
        "hit_stats": hit_stats,
        "decision": "REVIEW",
        "decision_reason": "Offline counterfactual generated; requires review before promotion.",
    }
    json_path.write_text(_json_dumps(payload, indent=2), encoding="utf-8")
    _write_md(md_path, payload)
    return ValidationSummary(
        validation_key=validation_key,
        experiment_id=experiment_id,
        loop_id=loop_id,
        profile_id=profile_id,
        date_from=date_from,
        date_to=date_to,
        baseline_metrics=baseline_metrics,
        overlay_metrics=overlay_metrics,
        delta_metrics=delta_metrics,
        hit_stats=hit_stats,
        report_path=str(md_path),
        json_path=str(json_path),
    )


def _pct(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "NA"
    return f"{float(value) * 100:.2f}%"


def _money(value: Any) -> str:
    if value is None:
        return "NA"
    return f"{float(value):,.2f}"


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    base = payload["baseline_metrics"]
    overlay = payload["overlay_metrics"]
    delta = payload["delta_metrics"]
    hits = payload["hit_stats"]
    simulator_version = payload.get("simulator_version", "unknown")
    is_next_candidate = simulator_version == NEXT_CANDIDATE_SIMULATOR_VERSION
    lines = [
        f"# Event Signal QE Overlay Validation: {payload['experiment_id']} {payload['loop_id']}",
        "",
        f"Research-only offline counterfactual ({simulator_version}). It does not change QE runtime or trading consumers.",
        "",
        "## Metrics",
        "",
        "```text",
        "Metric             Baseline              Overlay               Delta",
        "-----------------  --------------------  --------------------  --------------------",
        f"Final account      {_money(base['final_account']):>20}  {_money(overlay['final_account']):>20}  {_money(delta['final_account_delta']):>20}",
        f"Total return       {_pct(base['total_return']):>20}  {_pct(overlay['total_return']):>20}  {_pct(delta['total_return_delta']):>20}",
        f"CAGR               {_pct(base['cagr']):>20}  {_pct(overlay['cagr']):>20}  {_pct(delta['cagr_delta']):>20}",
        f"Max drawdown       {_pct(base['max_drawdown']):>20}  {_pct(overlay['max_drawdown']):>20}  {_pct(delta['max_drawdown_delta']):>20}",
        "```",
        "",
        "## Hit Stats",
        "",
        "```text",
        f"Overlay rows                  : {hits['overlay_rows']}",
        f"Overlay symbols               : {hits['overlay_symbols']}",
        f"Blocked buy events            : {hits['blocked_buy_events']}",
        f"Force-exit events             : {hits['force_exit_events']}",
        f"Unique buy-hit symbols        : {hits['unique_buy_hit_symbols']}",
        f"Unique force-exit symbols     : {hits['unique_force_exit_symbols']}",
        (
            f"Original contribution removed : {_money(hits.get('original_contribution_removed_sum'))}"
            if is_next_candidate
            else f"Blocked symbol original PnL   : {_money(hits.get('blocked_symbol_original_pnl_sum'))}"
        ),
        (
            f"Replacement PnL               : {_money(hits.get('replacement_pnl_sum'))}"
            if is_next_candidate
            else f"Positive/Negative/Zero symbols: {hits.get('blocked_symbol_positive_count')} / {hits.get('blocked_symbol_negative_count')} / {hits.get('blocked_symbol_zero_count')}"
        ),
        (
            f"Replacement events            : open={hits.get('replacement_open_events')} no_candidate={hits.get('replacement_no_candidate_events')} reselect={hits.get('replacement_reselect_events')}"
            if is_next_candidate
            else ""
        ),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def prepare_price_return_csv(
    *,
    loop_path: Path,
    prediction_pkl: Optional[Path],
    output_path: Path,
    date_from: dt.date,
    date_to: dt.date,
) -> dict[str, Any]:
    artifact_dir = find_loop_artifact_dir(loop_path)
    pred_path = prediction_pkl or (artifact_dir / "pred.pkl")
    candidate_scores = load_prediction_scores(pred_path)
    candidate_symbols = {score.ts_code for scores in candidate_scores.values() for score in scores}
    price_returns = fetch_price_returns_from_db(
        symbols=candidate_symbols,
        date_from=date_from,
        date_to=date_to,
    )
    write_price_returns_csv(price_returns, output_path)
    return {
        "prediction_pkl": str(pred_path),
        "output_path": str(output_path),
        "candidate_score_dates": len(candidate_scores),
        "candidate_symbols": len(candidate_symbols),
        "price_return_dates": len(price_returns),
        "price_return_symbols": len({symbol for values in price_returns.values() for symbol in values}),
        "price_return_rows": sum(len(values) for values in price_returns.values()),
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Validate an event-signal overlay against a fixed QE loop")
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--loop-id", required=True)
    parser.add_argument("--loop-path", required=True)
    parser.add_argument("--overlay-csv")
    parser.add_argument("--output-dir", default="reports/event_signal/qe_overlay_validation")
    parser.add_argument("--profile-id")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--simulator-mode", default="cash", choices=["cash", "next_candidate"])
    parser.add_argument("--prediction-pkl", default=None)
    parser.add_argument("--price-return-csv", default=None)
    parser.add_argument("--prepare-price-return-csv-only", action="store_true")
    args = parser.parse_args(argv)
    if args.prepare_price_return_csv_only:
        output_path = Path(args.price_return_csv) if args.price_return_csv else Path(args.output_dir) / "candidate_price_returns.csv"
        summary_payload = prepare_price_return_csv(
            loop_path=Path(args.loop_path),
            prediction_pkl=Path(args.prediction_pkl) if args.prediction_pkl else None,
            output_path=output_path,
            date_from=dt.date.fromisoformat(args.date_from),
            date_to=dt.date.fromisoformat(args.date_to),
        )
        print(_json_dumps(summary_payload, indent=2))
        return 0
    if not args.overlay_csv:
        raise ValueError("--overlay-csv is required unless --prepare-price-return-csv-only is used")
    if not args.profile_id:
        raise ValueError("--profile-id is required unless --prepare-price-return-csv-only is used")
    summary = validate_loop_overlay(
        experiment_id=args.experiment_id,
        loop_id=args.loop_id,
        loop_path=Path(args.loop_path),
        overlay_csv=Path(args.overlay_csv),
        output_dir=Path(args.output_dir),
        profile_id=args.profile_id,
        date_from=dt.date.fromisoformat(args.date_from),
        date_to=dt.date.fromisoformat(args.date_to),
        simulator_mode=args.simulator_mode,
        prediction_pkl=Path(args.prediction_pkl) if args.prediction_pkl else None,
        price_return_csv=Path(args.price_return_csv) if args.price_return_csv else None,
    )
    print(_json_dumps(summary.__dict__, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
