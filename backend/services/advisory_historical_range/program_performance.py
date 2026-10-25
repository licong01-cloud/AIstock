"""Read-only Advisory performance: reporting clock, denominators and cohorts.

These are hypothetical reference returns, not broker fills or portfolio NAV.
Missing prices/costs remain missing; the consumer never repairs source data.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from math import isfinite
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


SHANGHAI = ZoneInfo("Asia/Shanghai")


def reporting_cutoff(now: datetime) -> date:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("performance reporting clock must be timezone-aware")
    local = now.astimezone(SHANGHAI)
    return local.date() if local.time() >= time(15) else local.date() - timedelta(days=1)


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return result if isfinite(result) else None


def day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def cost_contract(buy_cost_bps: float | None, sell_cost_bps: float | None) -> dict[str, Any] | None:
    if buy_cost_bps is None and sell_cost_bps is None:
        return None
    buy, sell = number(buy_cost_bps), number(sell_cost_bps)
    if buy is None or sell is None or not 0 <= buy < 10000 or not 0 <= sell < 10000:
        raise RuntimeConfigInvalidError("both buy_cost_bps and sell_cost_bps must be finite and within [0, 10000)")
    return {"buy_cost_bps": buy, "sell_cost_bps": sell, "basis": "EXPLICIT_HYPOTHETICAL_COST"}


def net_return(gross_bps: float, cost: Mapping[str, Any]) -> float:
    # Same multiplicative buy/sell cost convention as Advisory policy labels.
    return (
        (1 + gross_bps / 10000) * (1 - cost["sell_cost_bps"] / 10000) / (1 + cost["buy_cost_bps"] / 10000) - 1
    ) * 10000


def distribution(values: Iterable[float], *, reason_code: str | None = None) -> dict[str, Any]:
    returns = [parsed for value in values if (parsed := number(value)) is not None]
    wins, losses = [v for v in returns if v > 0], [v for v in returns if v < 0]
    return {
        "sample_count": len(returns),
        "win_count": len(wins),
        "loss_count": len(losses),
        "flat_count": len(returns) - len(wins) - len(losses),
        "win_rate": len(wins) / len(returns) if returns else None,
        "avg_return_bps": mean(returns) if returns else None,
        "median_return_bps": median(returns) if returns else None,
        "avg_win_bps": mean(wins) if wins else None,
        "avg_loss_bps": mean(losses) if losses else None,
        "payoff_ratio": mean(wins) / abs(mean(losses)) if wins and losses else None,
        "profit_factor": sum(wins) / abs(sum(losses)) if losses else None,
        "ratio_reason_code": None if wins and losses else "NO_LOSSES" if not losses else "NO_WINS",
        "reason_code": reason_code if not returns else None,
    }


def episode_performance(row: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """Do not use the ledger ACTIVE/EXITED flag as proof of financial settlement."""
    cutoff = reporting_cutoff(now)
    entry, exit_date = day(row.get("effective_entry_date")), day(row.get("effective_exit_date"))
    evidence = row.get("evidence_json") or {}
    settlement = evidence.get("metric_settlement") or {}
    mark = evidence.get("open_mark_to_market") or {}
    mark_day = day(mark.get("trade_date"))
    gross = number(row.get("return_bps"))
    entry_price = number(row.get("entry_price"))
    quality = str(row.get("price_quality_status") or "UNKNOWN")
    state, reason = "UNKNOWN", "INVALID_OR_MISSING_PRICE"
    realized = unrealized = None
    exited = row.get("status") == "EXITED"
    if entry is None or entry_price is None or entry_price <= 0:
        pass
    elif entry > cutoff:
        state, reason = "PLANNED", "ENTRY_NOT_FINALIZED"
    elif settlement.get("reason_code"):
        reason = str(settlement["reason_code"])
        state = "DEFERRED" if reason in {"SUSPENDED", "NON_EXECUTABLE"} else "UNKNOWN"
    elif quality in {"SUSPENDED", "NON_EXECUTABLE"}:
        state, reason = "DEFERRED", quality
    elif exited and exit_date is None:
        reason = "EXIT_DATE_MISSING"
    elif exited and exit_date > cutoff:
        local = now.astimezone(SHANGHAI)
        executes_at = time(9, 30) if row.get("exit_price_basis") == "next_open_executable" else time(15)
        # EXECUTED here is a reference-price observation, never a broker fill.
        reference = evidence.get("reference_execution") or {}
        observed = settlement.get("execution_price_observed") is True or (
            day(reference.get("trade_date")) == exit_date and number(reference.get("price")) is not None
        )
        state = "EXECUTED" if observed and exit_date == local.date() and local.time() >= executes_at else "PLANNED"
        reason = "EXIT_NOT_FINALIZED"
    elif exited and exit_date < entry:
        reason = "EXIT_BEFORE_ENTRY"
    elif (
        exited
        and gross is not None
        and (exit_price := number(row.get("exit_price"))) is not None
        and exit_price > 0
        and row.get("win_rate_inclusion_status") == "INCLUDED"
    ):
        state, reason, realized = "SETTLED", None, gross
    elif not exited:
        state, reason = "EXECUTED", "MARK_DATA_MISSING"
    if evidence.get("metric_mark_read_status"):
        reason = evidence["metric_mark_read_status"]
    if (
        not evidence.get("metric_mark_read_status")
        and entry is not None
        and entry <= cutoff
        and mark_day is not None
        and entry <= mark_day <= cutoff
    ):
        mark_price = number(mark.get("mark_price"))
        if (
            mark_price is not None
            and mark_price > 0
            and entry_price is not None
            and entry_price > 0
            and state != "SETTLED"
        ):
            unrealized = (mark_price / entry_price - 1) * 10000
            if not exited and state == "EXECUTED":
                reason = None
    return {
        "performance_state": state,
        "performance_reason_code": reason,
        "realized_gross_return_bps": realized,
        "unrealized_gross_return_bps": unrealized,
        "metric_as_of_trade_date": cutoff.isoformat(),
        "execution_evidence_level": "REFERENCE_HYPOTHETICAL",
    }


def performance_metrics(
    program: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    cost: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    views = [(row, episode_performance(row, now=now)) for row in rows]
    settled = [(row, view) for row, view in views if view["performance_state"] == "SETTLED"]
    marked = [(row, view) for row, view in views if view["unrealized_gross_return_bps"] is not None]
    gross = distribution(view["realized_gross_return_bps"] for _, view in settled)
    unrealized = distribution(view["unrealized_gross_return_bps"] for _, view in marked)
    states = Counter(view["performance_state"] for _, view in views)
    due = [
        (row, view)
        for row, view in views
        if row.get("status") == "EXITED"
        and (exit_date := day(row.get("effective_exit_date"))) is not None
        and exit_date <= reporting_cutoff(now)
    ]
    quality_counts = Counter(view["performance_reason_code"] for _, view in views if view["performance_reason_code"])
    drawdowns = [v for row, _ in settled if (v := number(row.get("max_drawdown_bps"))) is not None]
    mark_dates = [
        str((row.get("evidence_json", {}).get("open_mark_to_market") or {}).get("trade_date")) for row, _ in marked
    ]
    holding = [number(row.get("holding_trading_days")) for row, _ in settled]
    holding = [v for v in holding if v is not None]
    return {
        "schema_version": "advisory_program_metrics_v2",
        "metric_basis": "SETTLED_GROSS_REFERENCE_EPISODES",
        "execution_evidence_level": "REFERENCE_HYPOTHETICAL",
        "metric_as_of_trade_date": reporting_cutoff(now).isoformat(),
        "reported_at": now.isoformat(),
        "enabled_since": program.get("enabled_since"),
        "entered_episode_count": len(rows),
        "active_count": sum(row.get("status") == "ACTIVE" for row in rows),
        "take_profit_count": sum(
            row.get("exit_reason") in {"TAKE_PROFIT", "TRAILING_TAKE_PROFIT"} for row, _ in settled
        ),
        "stop_loss_count": sum(row.get("exit_reason") == "STOP_LOSS" for row, _ in settled),
        "win_rate": gross["win_rate"],
        "avg_return_bps": gross["avg_return_bps"],
        "median_return_bps": gross["median_return_bps"],
        "metric_evaluable_count": len(settled),
        "max_drawdown_bps": min(drawdowns) if drawdowns else None,
        "drawdown_basis": "MINIMUM_SINGLE_EPISODE_NOT_PORTFOLIO_MDD",
        "avg_holding_days": mean(holding) if holding else None,
        "last_review_status": program.get("last_review_status"),
        "metric_status": "READY"
        if settled
        else "UNREALIZED_ONLY"
        if marked
        else "WAITING_MARKET_DATA"
        if rows
        else "NO_EPISODES",
        "open_mark_count": len(marked),
        "missing_open_mark_count": sum(
            row.get("status") == "ACTIVE" and view["unrealized_gross_return_bps"] is None for row, view in views
        ),
        "metric_mark_trade_date": max(mark_dates) if mark_dates else None,
        "cost_contract": cost,
        "settled": {
            "gross": gross,
            "net": distribution(
                (net_return(view["realized_gross_return_bps"], cost) for _, view in settled)
                if cost is not None
                else [],
                reason_code="COST_UNSPECIFIED" if cost is None else "NO_SETTLED_EPISODES",
            ),
        },
        "unrealized": {
            "gross": unrealized,
            "basis": "CLOSE_MARK_NOT_REALIZED",
            "liquidation_net": distribution(
                (net_return(view["unrealized_gross_return_bps"], cost) for _, view in marked)
                if cost is not None
                else [],
                reason_code="COST_UNSPECIFIED" if cost is None else "NO_MARKED_EPISODES",
            ),
        },
        "coverage": {
            "total_count": len(rows),
            "due_count": len(due),
            "settled_count": len(settled),
            "settled_coverage": len(settled) / len(due) if due else None,
            "state_counts": dict(states),
            "reason_counts": dict(quality_counts),
            "immature_exit_count": sum(
                row.get("status") == "EXITED" and view["performance_reason_code"] == "EXIT_NOT_FINALIZED"
                for row, view in views
            ),
        },
    }


class ProgramPerformanceMarketReader:
    """One bounded batched read per comparison, no selection/research/DB writes."""

    def __init__(self, conn_factory: Any) -> None:
        self.conn_factory = conn_factory

    def load(self, *, symbols: Sequence[str], start: date, end: date, cutoff: date, benchmark: str) -> dict[str, Any]:
        # Request dates are entry dates; read the maturity tail, never future bars.
        price_end = min(end + timedelta(days=21), cutoff)
        with self.conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT cal_date, is_trading FROM market.trading_calendar WHERE cal_date BETWEEN %s AND %s ORDER BY cal_date",
                    (start, end + timedelta(days=21)),
                )
                calendar = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    WITH priced AS MATERIALIZED (
                        SELECT k.ts_code, k.trade_date, k.open_li, k.close_li, k.high_li, k.low_li
                        FROM market.kline_daily_raw k
                        WHERE k.ts_code = ANY(%s) AND k.trade_date BETWEEN %s AND %s
                    ), adjustments AS MATERIALIZED (
                        SELECT ts_code, trade_date, adj_factor FROM market.adj_factor
                        WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s
                    ), limits AS MATERIALIZED (
                        SELECT ts_code, trade_date, up_limit, down_limit FROM market.stk_limit
                        WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s
                    )
                    SELECT k.trade_date, TRIM(k.ts_code) AS symbol, k.open_li, k.close_li,
                           k.high_li, k.low_li, a.adj_factor, l.up_limit, l.down_limit
                    FROM priced k
                    LEFT JOIN adjustments a ON a.ts_code = k.ts_code AND a.trade_date = k.trade_date
                    LEFT JOIN limits l ON l.ts_code = k.ts_code AND l.trade_date = k.trade_date
                    ORDER BY k.trade_date, k.ts_code
                    """,
                    (list(symbols), start, price_end) * 3,
                )
                prices = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """SELECT trade_date, TRIM(ts_code) AS symbol FROM market.suspend_d
                    WHERE ts_code = ANY(%s) AND COALESCE(TRIM(suspend_type), 'S') = 'S'
                      AND trade_date BETWEEN %s AND %s""",
                    (list(symbols), start, price_end),
                )
                suspended = {(row["symbol"], row["trade_date"]) for row in cur.fetchall()}
                cur.execute(
                    """SELECT trade_date, open, close FROM market.index_daily
                    WHERE ts_code = %s AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
                    (benchmark, start, price_end),
                )
                benchmark_rows = [dict(row) for row in cur.fetchall()]
        return {"calendar": calendar, "prices": prices, "suspended": suspended, "benchmark": benchmark_rows}


def _unique_rows(rows: Sequence[Mapping[str, Any]], key: Any) -> dict[Any, Mapping[str, Any]]:
    result = {}
    for row in rows:
        identity = key(row)
        if identity in result:
            raise DataUnavailableError("duplicate performance market identity", context={"identity": str(identity)})
        result[identity] = row
    return result


def fixed_five_day_outcomes(
    rows: Sequence[Mapping[str, Any]], market: Mapping[str, Any], *, cutoff: date, cost: Mapping[str, Any] | None
) -> list[dict[str, Any]]:
    calendar_rows = _unique_rows(market["calendar"], lambda row: day(row["cal_date"]))
    calendar = sorted(d for d, row in calendar_rows.items() if row["is_trading"])
    positions = {d: index for index, d in enumerate(calendar)}
    prices = _unique_rows(market["prices"], lambda row: (row["symbol"], day(row["trade_date"])))
    benchmarks = _unique_rows(market["benchmark"], lambda row: day(row["trade_date"]))
    results = []
    for row in rows:
        entry = day(row.get("effective_entry_date"))
        position = positions.get(entry)
        maturity = calendar[position + 4] if position is not None and position + 4 < len(calendar) else None
        outcome = {
            "episode_id": row["episode_id"],
            "program_id": row["program_id"],
            "symbol": row["symbol"],
            "entry_trade_date": entry.isoformat() if entry else None,
            "maturity_trade_date": maturity.isoformat() if maturity else None,
            "status": "MISSING_CALENDAR",
            "gross_return_bps": None,
            "net_return_bps": None,
            "cost_drag_bps": None,
            "benchmark_gross_return_bps": None,
            "net_excess_return_bps": None,
            "package_ids": (row.get("evidence_json") or {}).get("package_ids"),
            "policy_sha256": (row.get("evidence_json") or {}).get("review_policy_sha256"),
            "program_version": row.get("program_version"),
        }
        outcome["source_run_id"] = row.get("source_run_id")
        outcome["entry_binding_identity"] = row.get("entry_binding_identity") or {
            "status": "UNRESOLVED_HISTORICAL_BINDING"
        }
        outcome["policy_identity_basis"] = "LATEST_RECORDED_EPISODE_POLICY_NOT_ENTRY_POLICY_PROOF"
        if outcome["entry_binding_identity"]["status"] == "EXACT_HISTORICAL_INTERVAL":
            outcome["package_ids"] = outcome["entry_binding_identity"]["package_ids"]
        if maturity is None:
            results.append(outcome)
            continue
        expected = set(entry + timedelta(days=i) for i in range((maturity - entry).days + 1))
        if not expected.issubset(calendar_rows):
            results.append(outcome)
            continue
        if maturity > cutoff:
            outcome["status"] = "IMMATURE"
            results.append(outcome)
            continue
        if (row["symbol"], entry) in market["suspended"] or (row["symbol"], maturity) in market["suspended"]:
            outcome["status"] = "SUSPENDED"
            results.append(outcome)
            continue
        first, last = prices.get((row["symbol"], entry)), prices.get((row["symbol"], maturity))
        if first is None or last is None:
            outcome["status"] = "MISSING_DATA"
            results.append(outcome)
            continue
        entry_price, exit_price = number(first.get("open_li")), number(last.get("close_li"))
        entry_adj, exit_adj = number(first.get("adj_factor")), number(last.get("adj_factor"))
        upper, lower = number(first.get("up_limit")), number(last.get("down_limit"))
        if any(value is None or value <= 0 for value in (entry_price, exit_price, entry_adj, exit_adj, upper, lower)):
            outcome["status"] = "MISSING_PRICE_ADJUSTMENT_OR_LIMIT"
            results.append(outcome)
            continue
        # Preserve conservative daily execution semantics; no minute-fill fiction.
        low = number(first.get("low_li"))
        if low is None or low <= 0:
            outcome["status"] = "MISSING_PRICE_ADJUSTMENT_OR_LIMIT"
            results.append(outcome)
            continue
        if (entry_price / 1000 >= upper and low / 1000 >= upper) or exit_price / 1000 <= lower:
            outcome["status"] = "NON_EXECUTABLE"
            results.append(outcome)
            continue
        gross = (exit_price * exit_adj / (entry_price * entry_adj) - 1) * 10000
        outcome.update(status="MATURE", gross_return_bps=gross)
        if cost is not None:
            outcome["net_return_bps"] = net_return(gross, cost)
            outcome["cost_drag_bps"] = gross - outcome["net_return_bps"]
        bm_entry, bm_exit = benchmarks.get(entry), benchmarks.get(maturity)
        bm_open = number(bm_entry.get("open")) if bm_entry else None
        bm_close = number(bm_exit.get("close")) if bm_exit else None
        if bm_open is not None and bm_close is not None and bm_open > 0 and bm_close > 0:
            outcome["benchmark_gross_return_bps"] = (bm_close / bm_open - 1) * 10000
            if outcome["net_return_bps"] is not None:
                outcome["net_excess_return_bps"] = outcome["net_return_bps"] - outcome["benchmark_gross_return_bps"]
        results.append(outcome)
    return results


def _cohort_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["entry_trade_date"]].append(row)
    metrics = {}
    for field in ("gross_return_bps", "net_return_bps", "net_excess_return_bps"):
        values = [
            mean(float(row[field]) for row in cohort)
            for cohort in grouped.values()
            if all(row[field] is not None for row in cohort)
        ]
        metrics[field] = distribution(values)
    return {
        "basis": "DATE_EQUAL_WEIGHT_FIVE_SESSION_COHORT_NOT_NAV",
        "entry_date_count": len(grouped),
        "episode_count": len(rows),
        "metrics": metrics,
    }


def compare_program_outcomes(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    program_ids: Sequence[str],
    calendar: Sequence[date],
    start: date,
    end: date,
    cost: Mapping[str, Any] | None,
    benchmark: str,
) -> dict[str, Any]:
    groups = {identity: [row for row in outcomes if row["program_id"] == identity] for identity in program_ids}
    dates = {
        identity: {row["entry_trade_date"] for row in rows if row["entry_trade_date"] is not None}
        for identity, rows in groups.items()
    }
    common_dates = set.intersection(*dates.values()) if dates else set()
    valid = {}
    required = "net_excess_return_bps" if cost else "gross_return_bps"
    for identity, rows in groups.items():
        valid[identity] = {
            d
            for d in dates[identity]
            if all(
                row["status"] == "MATURE" and row[required] is not None for row in rows if row["entry_trade_date"] == d
            )
        }
    common_valid = set.intersection(*valid.values()) if valid else set()
    common_valid &= common_dates
    trading = sorted(d for d in calendar if start <= d <= end)
    windows: dict[str, set[str]] = {"ALL": common_valid}
    for month in sorted({d[:7] for d in common_dates}):
        windows[f"MONTH:{month}"] = {d for d in common_valid if d[:7] == month}
    for size in (20, 60):
        windows[f"ROLLING:{size}"] = common_valid & {d.isoformat() for d in trading[-size:]}
    programs = []
    for identity, rows in groups.items():
        counts = Counter(row["status"] for row in rows)
        due = sum(row["maturity_trade_date"] is not None and row["status"] != "IMMATURE" for row in rows)
        identities = [
            {"package_ids": list(packages), "policy_sha256": policy, "program_version": version}
            for packages, policy, version in sorted(
                {
                    (tuple(row.get("package_ids") or ()), row.get("policy_sha256"), row.get("program_version"))
                    for row in rows
                },
                key=str,
            )
        ]
        programs.append(
            {
                "program_id": identity,
                "total_count": len(rows),
                "status_counts": dict(counts),
                "due_count": due,
                "mature_count": counts["MATURE"],
                "unknown_maturity_count": sum(row["maturity_trade_date"] is None for row in rows),
                "mature_coverage": counts["MATURE"] / due if due else None,
                "historical_identities": identities,
                "identity_basis": "PER_EPISODE_NOT_CURRENT_BINDING",
                "historical_binding_unresolved_count": sum(
                    row["entry_binding_identity"]["status"] != "EXACT_HISTORICAL_INTERVAL" for row in rows
                ),
                "candidate_dates": sorted(dates[identity]),
                "valid_dates": sorted(valid[identity]),
                "net_available_count": sum(row["net_return_bps"] is not None for row in rows),
                "benchmark_available_count": sum(row["benchmark_gross_return_bps"] is not None for row in rows),
                "sample_metrics": {
                    field: distribution(row[field] for row in rows if row["status"] == "MATURE")
                    for field in ("gross_return_bps", "net_return_bps", "net_excess_return_bps")
                },
                "sample_metrics_basis": "OWN_VALID_EPISODES_DESCRIPTIVE_NOT_CROSS_PACKAGE_COMPARISON",
                "windows": {
                    name: _cohort_summary([row for row in rows if row["entry_trade_date"] in selected])
                    for name, selected in windows.items()
                },
            }
        )
    return {
        "schema_version": "advisory_program_performance_comparison_v1",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "horizon_trading_days": 5,
        "horizon_basis": "ENTRY_OPEN_TO_FIFTH_MARKET_SESSION_CLOSE_INCLUSIVE",
        "execution_evidence_level": "REFERENCE_HYPOTHETICAL",
        "cost_contract": cost,
        "net_reason_code": None if cost else "COST_UNSPECIFIED",
        "benchmark_instrument": benchmark,
        "benchmark_basis": "GROSS_SAME_HORIZON_OPEN_TO_CLOSE",
        "comparison_metric": "NET_EXCESS_OVER_GROSS_BENCHMARK" if cost else "GROSS_PRICE_RETURN",
        "comparison_scope": "PROGRAM_HISTORY_WITH_PER_EPISODE_PACKAGE_IDENTITY",
        "common_candidate_dates": sorted(common_dates),
        "common_valid_dates": sorted(common_valid),
        "candidate_date_basis": "ENTERED_EPISODE_COHORTS_NOT_FULL_SELECTION_TOPK",
        "comparable": bool(common_valid) and len(program_ids) > 1,
        "comparison_reason_code": None
        if common_valid and len(program_ids) > 1
        else "NO_COMMON_VALID_DATES"
        if not common_valid
        else "SINGLE_PROGRAM",
        "programs": programs,
        "outcomes": list(outcomes),
    }
