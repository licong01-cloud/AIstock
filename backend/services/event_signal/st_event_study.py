"""Offline ST-first event study for independent event-signal validation.

The report is research-only: it reads market.event_signal plus local market data
and writes files under reports/.  It does not change trading consumers.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.event_signal.st_announcement_adapter import ST_UNIFIED_RULE_VERSION


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BENCHMARK = "000300.SH"
RISK_EVENT_TYPES: tuple[str, ...] = (
    "stock_delisting_confirmed",
    "stock_delisting_risk_warning",
    "stock_st_imposed",
    "stock_st_added_or_continued",
    "stock_st_removal_applied",
)
WINDOW_OFFSETS: tuple[int, ...] = (-1, 0, 1, 2)
CUM_WINDOW_NAME = "T0_T2"
PRICE_UNIT_DIVISOR = 1000.0


@dataclass(frozen=True)
class EventStudySummary:
    report_id: str
    rule_version: str
    time_mode: str
    signal_rows: int
    deduped_events: int
    detail_rows: int
    output_json: str
    output_csv: str
    output_md: str


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _mean(values: list[float]) -> Optional[float]:
    return float(statistics.fmean(values)) if values else None


def _median(values: list[float]) -> Optional[float]:
    return float(statistics.median(values)) if values else None


def _percentile(values: list[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(ordered[lo])
    weight = rank - lo
    return float(ordered[lo] * (1 - weight) + ordered[hi] * weight)


def _return_from_close(prev_close: Optional[float], close: Optional[float]) -> Optional[float]:
    if prev_close is None or close is None or prev_close <= 0:
        return None
    return close / prev_close - 1.0


def _compound(returns: Iterable[Optional[float]]) -> Optional[float]:
    product = 1.0
    count = 0
    for value in returns:
        if value is None:
            return None
        product *= 1.0 + value
        count += 1
    return product - 1.0 if count else None


def load_trading_days(conn: Any, start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date
              FROM market.trading_calendar
             WHERE is_trading = TRUE
               AND cal_date BETWEEN %s AND %s
             ORDER BY cal_date
            """,
            (start_date, end_date),
        )
        return [row[0] for row in cur.fetchall()]


def load_event_signals(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    limit: Optional[int],
) -> list[dict[str, Any]]:
    params: list[Any] = [rule_version, time_mode, list(RISK_EVENT_TYPES)]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND effective_trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND effective_trade_date <= %s"
        params.append(end_date)
    limit_sql = ""
    if limit is not None:
        limit_sql = " LIMIT %s"
        params.append(limit)
    sql = f"""
        WITH ranked AS (
            SELECT signal_id, ts_code, event_type, risk_level, action,
                   source_event_date, effective_trade_date, available_at,
                   source_time_quality, evidence,
                   row_number() OVER (
                       PARTITION BY ts_code, effective_trade_date, event_type
                       ORDER BY severity_score DESC, confidence DESC, signal_id ASC
                   ) AS rn
              FROM market.event_signal
             WHERE rule_version = %s
               AND time_mode = %s
               AND signal_status = 'ACTIVE'
               AND event_type = ANY(%s)
               {date_sql}
        )
        SELECT signal_id, ts_code, event_type, risk_level, action,
               source_event_date, effective_trade_date, available_at,
               source_time_quality, evidence
          FROM ranked
         WHERE rn = 1
         ORDER BY effective_trade_date, ts_code, event_type
         {limit_sql}
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]


def count_event_signals(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
) -> int:
    params: list[Any] = [rule_version, time_mode, list(RISK_EVENT_TYPES)]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND effective_trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND effective_trade_date <= %s"
        params.append(end_date)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*)
              FROM market.event_signal
             WHERE rule_version = %s
               AND time_mode = %s
               AND signal_status = 'ACTIVE'
               AND event_type = ANY(%s)
               {date_sql}
            """,
            tuple(params),
        )
        return int(cur.fetchone()[0])


def load_daily_prices(conn: Any, symbols: list[str], start_date: dt.date, end_date: dt.date) -> dict[tuple[str, dt.date], dict[str, Any]]:
    if not symbols:
        return {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ts_code, trade_date, open_li, high_li, low_li, close_li,
                   volume_hand, amount_li
              FROM market.kline_daily_raw
             WHERE ts_code = ANY(%s)
               AND trade_date BETWEEN %s AND %s
            """,
            (symbols, start_date, end_date),
        )
        rows = {}
        for row in cur.fetchall():
            item = dict(row)
            close_li = _safe_float(item.get("close_li"))
            item["close_yuan"] = close_li / PRICE_UNIT_DIVISOR if close_li is not None else None
            rows[(str(item["ts_code"]), item["trade_date"])] = item
        return rows


def load_index_close(conn: Any, benchmark: str, start_date: dt.date, end_date: dt.date) -> dict[dt.date, float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, close
              FROM market.index_daily
             WHERE ts_code = %s
               AND trade_date BETWEEN %s AND %s
            """,
            (benchmark, start_date, end_date),
        )
        return {row[0]: float(row[1]) for row in cur.fetchall() if row[1] is not None}


def load_limit_rows(conn: Any, symbols: list[str], start_date: dt.date, end_date: dt.date) -> dict[tuple[str, dt.date], dict[str, Any]]:
    if not symbols:
        return {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ts_code, trade_date, up_limit, down_limit
              FROM market.stk_limit
             WHERE ts_code = ANY(%s)
               AND trade_date BETWEEN %s AND %s
            """,
            (symbols, start_date, end_date),
        )
        return {(str(row["ts_code"]), row["trade_date"]): dict(row) for row in cur.fetchall()}


def load_suspend_rows(conn: Any, symbols: list[str], start_date: dt.date, end_date: dt.date) -> set[tuple[str, dt.date]]:
    if not symbols:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_code, trade_date
              FROM market.suspend_d
             WHERE ts_code = ANY(%s)
               AND trade_date BETWEEN %s AND %s
            """,
            (symbols, start_date, end_date),
        )
        return {(str(row[0]), row[1]) for row in cur.fetchall()}


def window_name(offset: int) -> str:
    if offset < 0:
        return f"T{offset}"
    if offset == 0:
        return "T0"
    return f"T+{offset}"


def is_down_limit(close_yuan: Optional[float], limit_row: Optional[dict[str, Any]]) -> bool:
    if close_yuan is None or not limit_row:
        return False
    down_limit = _safe_float(limit_row.get("down_limit"))
    if down_limit is None or down_limit <= 0:
        return False
    return close_yuan <= down_limit * 1.0005


def build_detail_rows(
    events: list[dict[str, Any]],
    trading_days: list[dt.date],
    prices: dict[tuple[str, dt.date], dict[str, Any]],
    index_close: dict[dt.date, float],
    limit_rows: dict[tuple[str, dt.date], dict[str, Any]],
    suspend_rows: set[tuple[str, dt.date]],
    *,
    benchmark: str,
) -> list[dict[str, Any]]:
    day_index = {day: idx for idx, day in enumerate(trading_days)}
    details: list[dict[str, Any]] = []
    per_event_returns: dict[int, dict[str, Optional[float]]] = {}
    per_event_benchmark: dict[int, dict[str, Optional[float]]] = {}
    per_event_flags: dict[int, dict[str, bool]] = {}

    for event in events:
        signal_id = int(event["signal_id"])
        event_idx = day_index.get(event["effective_trade_date"])
        event_raw_returns: dict[str, Optional[float]] = {}
        event_bench_returns: dict[str, Optional[float]] = {}
        event_flags = {
            "is_suspended": False,
            "hit_down_limit": False,
            "missing_price": False,
        }
        if event_idx is None:
            continue
        for offset in WINDOW_OFFSETS:
            idx = event_idx + offset
            prev_idx = idx - 1
            name = window_name(offset)
            if idx < 0 or idx >= len(trading_days) or prev_idx < 0:
                trade_date = None
                prev_date = None
            else:
                trade_date = trading_days[idx]
                prev_date = trading_days[prev_idx]

            price = prices.get((event["ts_code"], trade_date)) if trade_date else None
            prev_price = prices.get((event["ts_code"], prev_date)) if prev_date else None
            close_yuan = price.get("close_yuan") if price else None
            prev_close_yuan = prev_price.get("close_yuan") if prev_price else None
            raw_ret = _return_from_close(prev_close_yuan, close_yuan)

            bench_ret = None
            if trade_date and prev_date:
                bench_ret = _return_from_close(index_close.get(prev_date), index_close.get(trade_date))
            abnormal_ret = raw_ret - bench_ret if raw_ret is not None and bench_ret is not None else None
            limit_row = limit_rows.get((event["ts_code"], trade_date)) if trade_date else None
            suspended = (event["ts_code"], trade_date) in suspend_rows if trade_date else False
            hit_down = is_down_limit(close_yuan, limit_row)
            missing_price = price is None
            event_raw_returns[name] = raw_ret
            event_bench_returns[name] = bench_ret
            if name in {"T0", "T+1", "T+2"}:
                event_flags["is_suspended"] = event_flags["is_suspended"] or suspended
                event_flags["hit_down_limit"] = event_flags["hit_down_limit"] or hit_down
                event_flags["missing_price"] = event_flags["missing_price"] or missing_price
            details.append(
                {
                    "signal_id": signal_id,
                    "ts_code": event["ts_code"],
                    "event_type": event["event_type"],
                    "risk_level": event["risk_level"],
                    "action": event["action"],
                    "source_event_date": event["source_event_date"],
                    "effective_trade_date": event["effective_trade_date"],
                    "window_name": name,
                    "offset": offset,
                    "trade_date": trade_date,
                    "prev_trade_date": prev_date,
                    "close_yuan": close_yuan,
                    "prev_close_yuan": prev_close_yuan,
                    "raw_return": raw_ret,
                    "benchmark": benchmark,
                    "benchmark_return": bench_ret,
                    "abnormal_return": abnormal_ret,
                    "is_suspended": suspended,
                    "hit_down_limit": hit_down,
                    "volume_hand": price.get("volume_hand") if price else None,
                    "amount_yuan": (_safe_float(price.get("amount_li")) / PRICE_UNIT_DIVISOR) if price and _safe_float(price.get("amount_li")) is not None else None,
                    "missing_price": missing_price,
                }
            )
        per_event_returns[signal_id] = event_raw_returns
        per_event_benchmark[signal_id] = event_bench_returns
        per_event_flags[signal_id] = event_flags

    for event in events:
        signal_id = int(event["signal_id"])
        raw_cum = _compound([per_event_returns.get(signal_id, {}).get(name) for name in ("T0", "T+1", "T+2")])
        bench_cum = _compound([per_event_benchmark.get(signal_id, {}).get(name) for name in ("T0", "T+1", "T+2")])
        abnormal_cum = raw_cum - bench_cum if raw_cum is not None and bench_cum is not None else None
        details.append(
            {
                "signal_id": signal_id,
                "ts_code": event["ts_code"],
                "event_type": event["event_type"],
                "risk_level": event["risk_level"],
                "action": event["action"],
                "source_event_date": event["source_event_date"],
                "effective_trade_date": event["effective_trade_date"],
                "window_name": CUM_WINDOW_NAME,
                "offset": None,
                "trade_date": None,
                "prev_trade_date": None,
                "close_yuan": None,
                "prev_close_yuan": None,
                "raw_return": raw_cum,
                "benchmark": benchmark,
                "benchmark_return": bench_cum,
                "abnormal_return": abnormal_cum,
                "is_suspended": per_event_flags.get(signal_id, {}).get("is_suspended", False),
                "hit_down_limit": per_event_flags.get(signal_id, {}).get("hit_down_limit", False),
                "volume_hand": None,
                "amount_yuan": None,
                "missing_price": per_event_flags.get(signal_id, {}).get("missing_price", raw_cum is None),
            }
        )
    return details


def aggregate_details(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in details:
        groups.setdefault((row["event_type"], row["window_name"]), []).append(row)
    aggregates: list[dict[str, Any]] = []
    for (event_type, name), rows in sorted(groups.items()):
        raw_returns = [float(row["raw_return"]) for row in rows if row.get("raw_return") is not None]
        abnormal_returns = [float(row["abnormal_return"]) for row in rows if row.get("abnormal_return") is not None]
        count = len(rows)
        aggregates.append(
            {
                "event_type": event_type,
                "window_name": name,
                "rows": count,
                "valid_raw_returns": len(raw_returns),
                "mean_raw_return": _mean(raw_returns),
                "median_raw_return": _median(raw_returns),
                "p10_raw_return": _percentile(raw_returns, 0.10),
                "p25_raw_return": _percentile(raw_returns, 0.25),
                "p75_raw_return": _percentile(raw_returns, 0.75),
                "p90_raw_return": _percentile(raw_returns, 0.90),
                "negative_return_rate": (sum(1 for value in raw_returns if value < 0) / len(raw_returns)) if raw_returns else None,
                "valid_abnormal_returns": len(abnormal_returns),
                "mean_abnormal_return": _mean(abnormal_returns),
                "median_abnormal_return": _median(abnormal_returns),
                "down_limit_rate": sum(1 for row in rows if row.get("hit_down_limit")) / count if count else None,
                "suspended_rate": sum(1 for row in rows if row.get("is_suspended")) / count if count else None,
                "missing_price_rate": sum(1 for row in rows if row.get("missing_price")) / count if count else None,
            }
        )
    return aggregates


def write_outputs(
    *,
    output_dir: Path,
    report_id: str,
    summary_payload: dict[str, Any],
    details: list[dict[str, Any]],
) -> EventStudySummary:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    csv_path = output_dir / f"{report_id}_details.csv"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(summary_payload), encoding="utf-8")
    fieldnames = [
        "signal_id", "ts_code", "event_type", "risk_level", "action",
        "source_event_date", "effective_trade_date", "window_name", "offset",
        "trade_date", "prev_trade_date", "close_yuan", "prev_close_yuan",
        "raw_return", "benchmark", "benchmark_return", "abnormal_return",
        "is_suspended", "hit_down_limit", "volume_hand", "amount_yuan", "missing_price",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(details)

    lines = [
        "# ST-first Event Study Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{summary_payload['rule_version']}`",
        f"- Time mode: `{summary_payload['time_mode']}`",
        f"- Benchmark: `{summary_payload['benchmark']}`",
        f"- Signal rows: `{summary_payload['signal_rows']}`",
        f"- Deduped events: `{summary_payload['deduped_events']}`",
        f"- Detail rows: `{len(details)}`",
        "",
        "## Aggregate Metrics",
        "",
        "| event_type | window | rows | valid | mean raw | median raw | mean abnormal | down-limit | suspended | missing |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_payload["aggregates"]:
        lines.append(
            "| {event_type} | {window_name} | {rows} | {valid_raw_returns} | {mean_raw_return} | "
            "{median_raw_return} | {mean_abnormal_return} | {down_limit_rate} | {suspended_rate} | {missing_price_rate} |".format(
                **{key: (round(value, 6) if isinstance(value, float) else value) for key, value in row.items()}
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return EventStudySummary(
        report_id=report_id,
        rule_version=summary_payload["rule_version"],
        time_mode=summary_payload["time_mode"],
        signal_rows=summary_payload["signal_rows"],
        deduped_events=summary_payload["deduped_events"],
        detail_rows=len(details),
        output_json=str(json_path),
        output_csv=str(csv_path),
        output_md=str(md_path),
    )


def run_event_study(
    *,
    rule_version: str = ST_UNIFIED_RULE_VERSION,
    time_mode: str = "backtest",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    benchmark: str = DEFAULT_BENCHMARK,
    limit: Optional[int] = None,
    output_dir: Path = Path("reports/event_signal/st_first"),
) -> EventStudySummary:
    with get_conn() as conn:
        signal_count = count_event_signals(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
        )
        events = load_event_signals(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        if not events:
            raise RuntimeError("no ST-first event_signal rows found for requested study scope")
        min_effective = min(row["effective_trade_date"] for row in events)
        max_effective = max(row["effective_trade_date"] for row in events)
        trading_days = load_trading_days(conn, min_effective - dt.timedelta(days=20), max_effective + dt.timedelta(days=20))
        day_index = {day: idx for idx, day in enumerate(trading_days)}
        needed_dates = []
        for row in events:
            idx = day_index.get(row["effective_trade_date"])
            if idx is None:
                continue
            for offset in range(-2, 3):
                target = idx + offset
                if 0 <= target < len(trading_days):
                    needed_dates.append(trading_days[target])
        if not needed_dates:
            raise RuntimeError("trading calendar does not cover event-study windows")
        price_start = min(needed_dates)
        price_end = max(needed_dates)
        symbols = sorted({row["ts_code"] for row in events})
        prices = load_daily_prices(conn, symbols, price_start, price_end)
        index_close = load_index_close(conn, benchmark, price_start, price_end)
        limit_rows = load_limit_rows(conn, symbols, price_start, price_end)
        suspend_rows = load_suspend_rows(conn, symbols, price_start, price_end)

    details = build_detail_rows(
        events,
        trading_days,
        prices,
        index_close,
        limit_rows,
        suspend_rows,
        benchmark=benchmark,
    )
    aggregates = aggregate_details(details)
    report_id = "st_first_event_study_{}_{}_{}".format(
        start_date.isoformat() if start_date else "all",
        end_date.isoformat() if end_date else "all",
        dt.datetime.now().strftime("%Y%m%d_%H%M%S"),
    ).replace("-", "")
    summary_payload = {
        "report_id": report_id,
        "rule_version": rule_version,
        "time_mode": time_mode,
        "benchmark": benchmark,
        "start_date": start_date,
        "end_date": end_date,
        "signal_rows": signal_count,
        "deduped_events": len(events),
        "event_type_counts": {event_type: sum(1 for row in events if row["event_type"] == event_type) for event_type in RISK_EVENT_TYPES},
        "price_start": price_start,
        "price_end": price_end,
        "aggregates": aggregates,
    }
    return write_outputs(output_dir=output_dir, report_id=report_id, summary_payload=summary_payload, details=details)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline event study for ST-first event signals")
    parser.add_argument("--rule-version", default=ST_UNIFIED_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output-dir", default="reports/event_signal/st_first")
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    summary = run_event_study(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        benchmark=args.benchmark,
        limit=args.limit,
        output_dir=Path(args.output_dir),
    )
    print(_json_dumps(summary.__dict__))


if __name__ == "__main__":
    main()
