"""Offline event study for structured financial event signals.

This module validates financial event signals as research evidence only. It reads
market.event_signal plus local market data and writes aggregate reports under
reports/. It does not modify trading consumers or source raw tables.
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
from backend.services.event_signal.announcement_adapter import UNIFIED_RULE_VERSION


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BENCHMARK = "000300.SH"
FINANCIAL_SOURCE_TYPES: tuple[str, ...] = (
    "tushare_forecast",
    "tushare_express",
    "tushare_fina_indicator",
    "financial_relation",
)
FINANCIAL_EVENT_TYPES: tuple[str, ...] = (
    "financial_forecast_loss",
    "financial_forecast_large_decline",
    "financial_forecast_turnaround",
    "financial_forecast_large_growth",
    "financial_express_loss",
    "financial_express_large_decline",
    "financial_express_large_growth",
    "financial_indicator_loss",
    "financial_indicator_large_decline",
    "financial_indicator_large_growth",
    "financial_positive_but_miss_expectation",
)
WINDOW_OFFSETS: tuple[int, ...] = (-1, 0, 1, 2, 5, 10, 20)
CUMULATIVE_WINDOWS: tuple[tuple[str, int], ...] = (
    ("T0_T2", 2),
    ("T0_T5", 5),
    ("T0_T20", 20),
)
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
    output_md: str
    output_csv: Optional[str]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return dt.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return dt.date.fromisoformat(text)


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


def load_event_signals(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    source_types: tuple[str, ...] = FINANCIAL_SOURCE_TYPES,
    event_types: tuple[str, ...] = FINANCIAL_EVENT_TYPES,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [rule_version, time_mode, list(source_types), list(event_types)]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND es.effective_trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND es.effective_trade_date <= %s"
        params.append(end_date)
    limit_sql = ""
    if limit is not None:
        limit_sql = " LIMIT %s"
        params.append(limit)

    sql = f"""
        WITH ranked AS (
            SELECT
                es.signal_id,
                es.ts_code,
                es.source_type,
                es.event_type,
                es.risk_level,
                es.action,
                es.signal_type,
                es.source_event_date,
                es.effective_trade_date,
                es.available_at,
                es.source_time_quality,
                es.severity_score,
                es.confidence,
                es.evidence,
                ef.report_period,
                row_number() OVER (
                    PARTITION BY es.ts_code, es.effective_trade_date, es.source_type,
                                 es.event_type, ef.report_period
                    ORDER BY es.severity_score DESC, es.confidence DESC, es.signal_id ASC
                ) AS rn
              FROM market.event_signal es
              LEFT JOIN market.event_fact ef
                ON ef.event_id = es.event_id
             WHERE es.rule_version = %s
               AND es.time_mode = %s
               AND es.signal_status = 'ACTIVE'
               AND es.source_type = ANY(%s)
               AND es.event_type = ANY(%s)
               {date_sql}
        )
        SELECT signal_id, ts_code, source_type, event_type, risk_level, action,
               signal_type, source_event_date, effective_trade_date, available_at,
               source_time_quality, severity_score, confidence, evidence,
               report_period
          FROM ranked
         WHERE rn = 1
         ORDER BY effective_trade_date, ts_code, source_type, event_type, signal_id
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
    source_types: tuple[str, ...] = FINANCIAL_SOURCE_TYPES,
    event_types: tuple[str, ...] = FINANCIAL_EVENT_TYPES,
) -> int:
    params: list[Any] = [rule_version, time_mode, list(source_types), list(event_types)]
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
               AND source_type = ANY(%s)
               AND event_type = ANY(%s)
               {date_sql}
            """,
            tuple(params),
        )
        return int(cur.fetchone()[0])


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


def required_price_keys(events: list[dict[str, Any]], trading_days: list[dt.date]) -> set[tuple[str, dt.date]]:
    day_index = {day: idx for idx, day in enumerate(trading_days)}
    keys: set[tuple[str, dt.date]] = set()
    max_cum_offset = max(end_offset for _, end_offset in CUMULATIVE_WINDOWS)
    for event in events:
        ts_code = str(event["ts_code"])
        event_idx = day_index.get(event["effective_trade_date"])
        if event_idx is None:
            continue
        for offset in WINDOW_OFFSETS:
            idx = event_idx + offset
            prev_idx = idx - 1
            if 0 <= idx < len(trading_days):
                keys.add((ts_code, trading_days[idx]))
            if 0 <= prev_idx < len(trading_days):
                keys.add((ts_code, trading_days[prev_idx]))
        start_idx = event_idx
        end_idx = min(len(trading_days) - 1, event_idx + max_cum_offset)
        for idx in range(start_idx, end_idx + 1):
            keys.add((ts_code, trading_days[idx]))
        prev_idx = event_idx - 1
        if 0 <= prev_idx < len(trading_days):
            keys.add((ts_code, trading_days[prev_idx]))
    return keys


def load_market_rows_for_keys(
    conn: Any,
    keys: set[tuple[str, dt.date]],
) -> tuple[
    dict[tuple[str, dt.date], dict[str, Any]],
    dict[tuple[str, dt.date], dict[str, Any]],
    set[tuple[str, dt.date]],
]:
    if not keys:
        return {}, {}, set()
    ordered = sorted(keys)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_financial_event_study_price_keys")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_financial_event_study_price_keys (
                ts_code TEXT NOT NULL,
                trade_date DATE NOT NULL
            )
            """
        )
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO tmp_financial_event_study_price_keys (ts_code, trade_date) VALUES %s",
            ordered,
            page_size=10000,
        )
        cur.execute(
            """
            CREATE INDEX tmp_financial_event_study_price_keys_idx
                ON tmp_financial_event_study_price_keys(ts_code, trade_date)
            """
        )

        cur.execute(
            """
            SELECT k.ts_code, k.trade_date, k.close_li, k.volume_hand, k.amount_li
              FROM market.kline_daily_raw k
              JOIN tmp_financial_event_study_price_keys t
                ON t.ts_code = k.ts_code
               AND t.trade_date = k.trade_date
            """
        )
        prices: dict[tuple[str, dt.date], dict[str, Any]] = {}
        for ts_code, trade_date, close_li, volume_hand, amount_li in cur.fetchall():
            close_value = _safe_float(close_li)
            prices[(str(ts_code), trade_date)] = {
                "close_yuan": close_value / PRICE_UNIT_DIVISOR if close_value is not None else None,
                "volume_hand": volume_hand,
                "amount_li": amount_li,
            }

        cur.execute(
            """
            SELECT l.ts_code, l.trade_date, l.up_limit, l.down_limit
              FROM market.stk_limit l
              JOIN tmp_financial_event_study_price_keys t
                ON t.ts_code = l.ts_code
               AND t.trade_date = l.trade_date
            """
        )
        limit_rows = {(str(row[0]), row[1]): {"up_limit": row[2], "down_limit": row[3]} for row in cur.fetchall()}

        cur.execute(
            """
            SELECT s.ts_code, s.trade_date
              FROM market.suspend_d s
              JOIN tmp_financial_event_study_price_keys t
                ON t.ts_code = s.ts_code
               AND t.trade_date = s.trade_date
            """
        )
        suspend_rows = {(str(row[0]), row[1]) for row in cur.fetchall()}
    return prices, limit_rows, suspend_rows


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


def _flags_for_dates(
    ts_code: str,
    dates: Iterable[dt.date],
    prices: dict[tuple[str, dt.date], dict[str, Any]],
    limit_rows: dict[tuple[str, dt.date], dict[str, Any]],
    suspend_rows: set[tuple[str, dt.date]],
) -> dict[str, bool]:
    date_list = list(dates)
    return {
        "is_suspended": any((ts_code, day) in suspend_rows for day in date_list),
        "hit_down_limit": any(
            is_down_limit((prices.get((ts_code, day)) or {}).get("close_yuan"), limit_rows.get((ts_code, day)))
            for day in date_list
        ),
        "missing_price": any((ts_code, day) not in prices for day in date_list),
    }


def _base_detail(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": int(event["signal_id"]),
        "ts_code": event["ts_code"],
        "source_type": event["source_type"],
        "event_type": event["event_type"],
        "risk_level": event["risk_level"],
        "action": event["action"],
        "signal_type": event["signal_type"],
        "source_event_date": event["source_event_date"],
        "effective_trade_date": event["effective_trade_date"],
        "report_period": event.get("report_period"),
    }


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
    for event in events:
        ts_code = str(event["ts_code"])
        event_idx = day_index.get(event["effective_trade_date"])
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
            price = prices.get((ts_code, trade_date)) if trade_date else None
            prev_price = prices.get((ts_code, prev_date)) if prev_date else None
            close_yuan = price.get("close_yuan") if price else None
            prev_close_yuan = prev_price.get("close_yuan") if prev_price else None
            raw_ret = _return_from_close(prev_close_yuan, close_yuan)
            bench_ret = None
            if trade_date and prev_date:
                bench_ret = _return_from_close(index_close.get(prev_date), index_close.get(trade_date))
            abnormal_ret = raw_ret - bench_ret if raw_ret is not None and bench_ret is not None else None
            flag_dates = [trade_date] if trade_date else []
            flags = _flags_for_dates(ts_code, flag_dates, prices, limit_rows, suspend_rows)
            row = {
                **_base_detail(event),
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
                "volume_hand": price.get("volume_hand") if price else None,
                "amount_yuan": (_safe_float(price.get("amount_li")) / PRICE_UNIT_DIVISOR) if price and _safe_float(price.get("amount_li")) is not None else None,
                **flags,
            }
            details.append(row)

        for name, end_offset in CUMULATIVE_WINDOWS:
            prev_idx = event_idx - 1
            end_idx = event_idx + end_offset
            if prev_idx < 0 or end_idx >= len(trading_days):
                prev_date = None
                end_date = None
                flag_dates = []
            else:
                prev_date = trading_days[prev_idx]
                end_date = trading_days[end_idx]
                flag_dates = trading_days[event_idx : end_idx + 1]
            end_price = prices.get((ts_code, end_date)) if end_date else None
            prev_price = prices.get((ts_code, prev_date)) if prev_date else None
            raw_ret = _return_from_close(
                prev_price.get("close_yuan") if prev_price else None,
                end_price.get("close_yuan") if end_price else None,
            )
            bench_ret = None
            if end_date and prev_date:
                bench_ret = _return_from_close(index_close.get(prev_date), index_close.get(end_date))
            abnormal_ret = raw_ret - bench_ret if raw_ret is not None and bench_ret is not None else None
            flags = _flags_for_dates(ts_code, flag_dates, prices, limit_rows, suspend_rows)
            if raw_ret is None:
                flags["missing_price"] = True
            details.append(
                {
                    **_base_detail(event),
                    "window_name": name,
                    "offset": None,
                    "trade_date": end_date,
                    "prev_trade_date": prev_date,
                    "close_yuan": end_price.get("close_yuan") if end_price else None,
                    "prev_close_yuan": prev_price.get("close_yuan") if prev_price else None,
                    "raw_return": raw_ret,
                    "benchmark": benchmark,
                    "benchmark_return": bench_ret,
                    "abnormal_return": abnormal_ret,
                    "volume_hand": None,
                    "amount_yuan": None,
                    **flags,
                }
            )
    return details


def aggregate_details(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {}
    for row in details:
        key = (row["source_type"], row["event_type"], row["risk_level"], row["action"], row["window_name"])
        groups.setdefault(key, []).append(row)
    aggregates: list[dict[str, Any]] = []
    for (source_type, event_type, risk_level, action, name), rows in sorted(groups.items()):
        raw_returns = [float(row["raw_return"]) for row in rows if row.get("raw_return") is not None]
        abnormal_returns = [float(row["abnormal_return"]) for row in rows if row.get("abnormal_return") is not None]
        count = len(rows)
        aggregates.append(
            {
                "source_type": source_type,
                "event_type": event_type,
                "risk_level": risk_level,
                "action": action,
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
                "positive_return_rate": (sum(1 for value in raw_returns if value > 0) / len(raw_returns)) if raw_returns else None,
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
    write_details: bool,
) -> EventStudySummary:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    csv_path = output_dir / f"{report_id}_details.csv" if write_details else None
    json_path.write_text(_json_dumps(summary_payload), encoding="utf-8")
    if csv_path is not None:
        fieldnames = [
            "signal_id", "ts_code", "source_type", "event_type", "risk_level", "action", "signal_type",
            "source_event_date", "effective_trade_date", "report_period", "window_name", "offset",
            "trade_date", "prev_trade_date", "close_yuan", "prev_close_yuan", "raw_return",
            "benchmark", "benchmark_return", "abnormal_return", "is_suspended", "hit_down_limit",
            "volume_hand", "amount_yuan", "missing_price",
        ]
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(details)

    lines = [
        "# Financial Event Study Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{summary_payload['rule_version']}`",
        f"- Time mode: `{summary_payload['time_mode']}`",
        f"- Benchmark: `{summary_payload['benchmark']}`",
        f"- Signal rows: `{summary_payload['signal_rows']}`",
        f"- Deduped events: `{summary_payload['deduped_events']}`",
        f"- Detail rows: `{len(details)}`",
        f"- Details CSV written: `{bool(csv_path)}`",
        "",
        "## Aggregate Metrics",
        "",
        "| source | event_type | window | rows | valid | mean raw | median raw | mean abnormal | down-limit | suspended | missing |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_payload["aggregates"]:
        formatted = {key: (round(value, 6) if isinstance(value, float) else value) for key, value in row.items()}
        lines.append(
            "| {source_type} | {event_type} | {window_name} | {rows} | {valid_raw_returns} | {mean_raw_return} | "
            "{median_raw_return} | {mean_abnormal_return} | {down_limit_rate} | {suspended_rate} | {missing_price_rate} |".format(
                **formatted
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
        output_md=str(md_path),
        output_csv=str(csv_path) if csv_path else None,
    )


def run_event_study(
    *,
    rule_version: str = UNIFIED_RULE_VERSION,
    time_mode: str = "backtest",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    benchmark: str = DEFAULT_BENCHMARK,
    source_types: tuple[str, ...] = FINANCIAL_SOURCE_TYPES,
    event_types: tuple[str, ...] = FINANCIAL_EVENT_TYPES,
    limit: Optional[int] = None,
    output_dir: Path = Path("reports/event_signal/financial"),
    write_details: bool = False,
) -> EventStudySummary:
    with get_conn() as conn:
        signal_count = count_event_signals(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
            source_types=source_types,
            event_types=event_types,
        )
        events = load_event_signals(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
            source_types=source_types,
            event_types=event_types,
            limit=limit,
        )
        if not events:
            raise RuntimeError("no financial event_signal rows found for requested study scope")
        min_effective = min(row["effective_trade_date"] for row in events)
        max_effective = max(row["effective_trade_date"] for row in events)
        trading_days = load_trading_days(conn, min_effective - dt.timedelta(days=40), max_effective + dt.timedelta(days=60))
        keys = required_price_keys(events, trading_days)
        prices, limit_rows, suspend_rows = load_market_rows_for_keys(conn, keys)
        price_dates = [day for _, day in keys]
        price_start = min(price_dates)
        price_end = max(price_dates)
        index_close = load_index_close(conn, benchmark, price_start, price_end)

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
    scope = "limit{}".format(limit) if limit is not None else "full"
    report_id = "financial_event_study_{}_{}_{}_{}".format(
        start_date.isoformat() if start_date else "all",
        end_date.isoformat() if end_date else "all",
        scope,
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
        "limit": limit,
        "source_type_counts": {source_type: sum(1 for row in events if row["source_type"] == source_type) for source_type in source_types},
        "event_type_counts": {event_type: sum(1 for row in events if row["event_type"] == event_type) for event_type in event_types},
        "price_key_count": len(keys),
        "price_start": price_start,
        "price_end": price_end,
        "aggregates": aggregates,
    }
    return write_outputs(
        output_dir=output_dir,
        report_id=report_id,
        summary_payload=summary_payload,
        details=details,
        write_details=write_details,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline event study for structured financial event signals")
    parser.add_argument("--rule-version", default=UNIFIED_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    parser.add_argument("--source-type", action="append", choices=sorted(FINANCIAL_SOURCE_TYPES), default=None)
    parser.add_argument("--event-type", action="append", choices=sorted(FINANCIAL_EVENT_TYPES), default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output-dir", default="reports/event_signal/financial")
    parser.add_argument("--write-details", action="store_true", help="Write per-signal detail CSV; disabled by default to avoid large artifacts")
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
        source_types=tuple(args.source_type) if args.source_type else FINANCIAL_SOURCE_TYPES,
        event_types=tuple(args.event_type) if args.event_type else FINANCIAL_EVENT_TYPES,
        limit=args.limit,
        output_dir=Path(args.output_dir),
        write_details=args.write_details,
    )
    print(_json_dumps(summary.__dict__))


if __name__ == "__main__":
    main()
