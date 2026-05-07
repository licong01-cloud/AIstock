"""Offline quality report for ST-first announcement event signals.

The report verifies the independent signal layer itself: title-signal cross
checks against ``market.stock_st_events``, rough recall against that independent
source, and leakage of bond-like facts into active stock risk signals.  It is a
read-only research utility and does not affect trading consumers.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from statistics import fmean
from typing import Any, Iterable, Optional

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.event_signal.st_announcement_adapter import (
    ST_FIRST_EVENT_TYPES,
    ST_SIGNAL_EVENT_TYPES,
    ST_UNIFIED_RULE_VERSION,
)


ROOT = Path(__file__).resolve().parents[3]
BOND_LIKE_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "convertible_bond_delisting_or_redemption",
        "generic_bond_delisting_or_repayment",
    }
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def _date_or_none(value: Any) -> Optional[dt.date]:
    if value is None or isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def _day_distance(left: Optional[dt.date], right: Optional[dt.date]) -> Optional[int]:
    if left is None or right is None:
        return None
    return abs((left - right).days)


def _extract_cross_check(evidence: Any) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        return {}
    value = evidence.get("st_cross_check")
    return value if isinstance(value, dict) else {}


def summarize_signal_cross_checks(signals: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate embedded ``st_cross_check`` evidence from event_signal rows."""

    rows = list(signals)
    by_event_type: dict[str, dict[str, Any]] = {}
    distances_by_event_type: dict[str, list[int]] = {}
    distances: list[int] = []
    matched = 0
    checked = 0
    reason_counts: dict[str, int] = {}
    for row in rows:
        event_type = str(row.get("event_type"))
        bucket = by_event_type.setdefault(
            event_type,
            {
                "rows": 0,
                "checked_rows": 0,
                "matched_rows": 0,
                "unmatched_rows": 0,
                "match_rate": None,
                "mean_distance_days": None,
                "match_reasons": {},
            },
        )
        bucket["rows"] += 1
        cross_check = _extract_cross_check(row.get("evidence"))
        if not cross_check:
            continue
        checked += 1
        bucket["checked_rows"] += 1
        reason = str(cross_check.get("match_reason") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        bucket["match_reasons"][reason] = bucket["match_reasons"].get(reason, 0) + 1
        if cross_check.get("matched") is True:
            matched += 1
            bucket["matched_rows"] += 1
            distance = cross_check.get("distance_days")
            if isinstance(distance, int):
                distances.append(distance)
                distances_by_event_type.setdefault(event_type, []).append(distance)
        else:
            bucket["unmatched_rows"] += 1

    for event_type, bucket in by_event_type.items():
        if bucket["checked_rows"]:
            bucket["match_rate"] = bucket["matched_rows"] / bucket["checked_rows"]
        bucket_distances = distances_by_event_type.get(event_type, [])
        bucket["mean_distance_days"] = fmean(bucket_distances) if bucket_distances else None
        bucket["match_reasons"] = dict(sorted(bucket["match_reasons"].items()))

    return {
        "signal_rows": len(rows),
        "checked_rows": checked,
        "matched_rows": matched,
        "unmatched_rows": checked - matched,
        "match_rate": (matched / checked) if checked else None,
        "mean_distance_days": fmean(distances) if distances else None,
        "match_reasons": dict(sorted(reason_counts.items())),
        "by_event_type": dict(sorted(by_event_type.items())),
    }


def signal_matches_stock_st_event(signal: dict[str, Any], st_event: dict[str, Any], *, window_days: int = 5) -> bool:
    """Return whether one ST source event is covered by one title signal."""

    if str(signal.get("ts_code")) != str(st_event.get("ts_code")):
        return False
    source_event_date = _date_or_none(signal.get("source_event_date"))
    effective_trade_date = _date_or_none(signal.get("effective_trade_date"))
    pub_date = _date_or_none(st_event.get("pub_date"))
    imp_date = _date_or_none(st_event.get("imp_date"))
    distances = [
        value
        for value in (
            _day_distance(source_event_date, pub_date),
            _day_distance(effective_trade_date, imp_date),
            _day_distance(source_event_date, imp_date),
            _day_distance(effective_trade_date, pub_date),
        )
        if value is not None
    ]
    return bool(distances) and min(distances) <= window_days


def summarize_stock_st_recall(
    stock_st_events: Iterable[dict[str, Any]],
    signals: Iterable[dict[str, Any]],
    *,
    window_days: int = 5,
    max_examples: int = 20,
) -> dict[str, Any]:
    """Estimate title-signal recall against independent ``stock_st_events`` rows."""

    st_rows = list(stock_st_events)
    signal_rows = list(signals)
    signals_by_code: dict[str, list[dict[str, Any]]] = {}
    for signal in signal_rows:
        signals_by_code.setdefault(str(signal.get("ts_code")), []).append(signal)

    matched = 0
    unmatched_examples: list[dict[str, Any]] = []
    by_st_type: dict[str, dict[str, int]] = {}
    for st_event in st_rows:
        st_type = str(st_event.get("st_type") or "unknown")
        bucket = by_st_type.setdefault(st_type, {"rows": 0, "matched_rows": 0, "unmatched_rows": 0})
        bucket["rows"] += 1
        candidates = signals_by_code.get(str(st_event.get("ts_code")), [])
        is_matched = any(signal_matches_stock_st_event(signal, st_event, window_days=window_days) for signal in candidates)
        if is_matched:
            matched += 1
            bucket["matched_rows"] += 1
        else:
            bucket["unmatched_rows"] += 1
            if len(unmatched_examples) < max_examples:
                unmatched_examples.append(
                    {
                        "ts_code": st_event.get("ts_code"),
                        "pub_date": st_event.get("pub_date"),
                        "imp_date": st_event.get("imp_date"),
                        "st_type": st_event.get("st_type"),
                        "st_reason": st_event.get("st_reason"),
                    }
                )
    for bucket in by_st_type.values():
        bucket["recall_rate"] = bucket["matched_rows"] / bucket["rows"] if bucket["rows"] else None
    return {
        "stock_st_event_rows": len(st_rows),
        "matched_stock_st_event_rows": matched,
        "unmatched_stock_st_event_rows": len(st_rows) - matched,
        "recall_rate": (matched / len(st_rows)) if st_rows else None,
        "window_days": window_days,
        "by_st_type": dict(sorted(by_st_type.items())),
        "unmatched_examples": unmatched_examples,
    }


def summarize_bond_leakage(
    facts: Iterable[dict[str, Any]],
    signals: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Verify bond-like ST-first facts do not become active stock risk signals."""

    fact_rows = list(facts)
    signal_rows = list(signals)
    bond_facts = [row for row in fact_rows if row.get("event_type") in BOND_LIKE_EVENT_TYPES]
    leaked_signals = [row for row in signal_rows if row.get("event_type") in BOND_LIKE_EVENT_TYPES]
    return {
        "bond_like_fact_rows": len(bond_facts),
        "bond_like_active_signal_rows": len(leaked_signals),
        "leakage_detected": bool(leaked_signals),
        "bond_like_event_types": sorted(BOND_LIKE_EVENT_TYPES),
    }


def build_quality_payload(
    *,
    signals: Iterable[dict[str, Any]],
    stock_st_events: Iterable[dict[str, Any]],
    facts: Optional[Iterable[dict[str, Any]]] = None,
    rule_version: str = ST_UNIFIED_RULE_VERSION,
    time_mode: str = "backtest",
    window_days: int = 5,
) -> dict[str, Any]:
    signal_rows = list(signals)
    fact_rows = list(facts or [])
    event_type_counts: dict[str, int] = {}
    for row in signal_rows:
        event_type = str(row.get("event_type"))
        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
    return {
        "rule_version": rule_version,
        "time_mode": time_mode,
        "signal_rows": len(signal_rows),
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "cross_check": summarize_signal_cross_checks(signal_rows),
        "stock_st_recall": summarize_stock_st_recall(stock_st_events, signal_rows, window_days=window_days),
        "bond_leakage": summarize_bond_leakage(fact_rows, signal_rows),
    }


def load_st_signal_rows(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    limit: Optional[int],
) -> list[dict[str, Any]]:
    params: list[Any] = [rule_version, time_mode, list(ST_SIGNAL_EVENT_TYPES)]
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
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT signal_id, ts_code, event_type, risk_level, action,
                   source_event_date, effective_trade_date, source_time_quality,
                   evidence
              FROM market.event_signal
             WHERE rule_version = %s
               AND time_mode = %s
               AND signal_status = 'ACTIVE'
               AND event_type = ANY(%s)
               {date_sql}
             ORDER BY effective_trade_date, ts_code, event_type, signal_id
             {limit_sql}
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def load_st_fact_rows(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
) -> list[dict[str, Any]]:
    params: list[Any] = [rule_version, time_mode, list(ST_FIRST_EVENT_TYPES)]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND effective_trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND effective_trade_date <= %s"
        params.append(end_date)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT event_id, ts_code, event_type, source_event_date, effective_trade_date, facts
              FROM market.event_fact
             WHERE rule_version = %s
               AND time_mode = %s
               AND event_type = ANY(%s)
               {date_sql}
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def load_stock_st_event_rows(
    conn: Any,
    *,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
) -> list[dict[str, Any]]:
    date_sql = ""
    params: list[Any] = []
    if start_date is not None:
        date_sql += " AND (pub_date >= %s OR imp_date >= %s)"
        params.extend([start_date, start_date])
    if end_date is not None:
        date_sql += " AND (pub_date <= %s OR imp_date <= %s)"
        params.extend([end_date, end_date])
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT ts_code, name, pub_date, imp_date, st_type, st_reason, st_explain, source_api
              FROM market.stock_st_events
             WHERE 1 = 1
               {date_sql}
             ORDER BY pub_date, imp_date, ts_code
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def write_quality_report(*, payload: dict[str, Any], output_dir: Path, report_id: str) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(payload), encoding="utf-8")

    lines = [
        "# ST-first Signal Quality Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{payload['rule_version']}`",
        f"- Time mode: `{payload['time_mode']}`",
        f"- Signal rows: `{payload['signal_rows']}`",
        f"- Cross-check match rate: `{payload['cross_check']['match_rate']}`",
        f"- Stock ST recall rate: `{payload['stock_st_recall']['recall_rate']}`",
        f"- Bond-like fact rows: `{payload['bond_leakage']['bond_like_fact_rows']}`",
        f"- Bond-like active signal rows: `{payload['bond_leakage']['bond_like_active_signal_rows']}`",
        "",
        "## Event Type Counts",
        "",
        "| event_type | rows |",
        "| --- | ---: |",
    ]
    for event_type, count in payload["event_type_counts"].items():
        lines.append(f"| {event_type} | {count} |")
    lines.extend(
        [
            "",
            "## Cross-check By Event Type",
            "",
            "| event_type | checked | matched | match_rate | mean_distance_days |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for event_type, row in payload["cross_check"]["by_event_type"].items():
        lines.append(
            f"| {event_type} | {row['checked_rows']} | {row['matched_rows']} | "
            f"{row['match_rate']} | {row['mean_distance_days']} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def run_st_signal_quality_report(
    *,
    rule_version: str = ST_UNIFIED_RULE_VERSION,
    time_mode: str = "backtest",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    window_days: int = 5,
    limit: Optional[int] = None,
    output_dir: Path = Path("reports/event_signal/st_first_quality"),
) -> dict[str, Any]:
    with get_conn() as conn:
        signals = load_st_signal_rows(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        facts = load_st_fact_rows(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
        )
        st_events = load_stock_st_event_rows(conn, start_date=start_date, end_date=end_date)

    payload = build_quality_payload(
        signals=signals,
        stock_st_events=st_events,
        facts=facts,
        rule_version=rule_version,
        time_mode=time_mode,
        window_days=window_days,
    )
    report_id = "st_first_signal_quality_{}_{}_{}".format(
        start_date.isoformat() if start_date else "all",
        end_date.isoformat() if end_date else "all",
        dt.datetime.now().strftime("%Y%m%d_%H%M%S"),
    ).replace("-", "")
    outputs = write_quality_report(payload=payload, output_dir=output_dir, report_id=report_id)
    payload["report_id"] = report_id
    payload["outputs"] = outputs
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only ST-first signal quality report")
    parser.add_argument("--rule-version", default=ST_UNIFIED_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--window-days", type=int, default=5)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output-dir", default="reports/event_signal/st_first_quality")
    return parser.parse_args()


def main() -> int:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    payload = run_st_signal_quality_report(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        window_days=args.window_days,
        limit=args.limit,
        output_dir=Path(args.output_dir),
    )
    print(_json_dumps({"report_id": payload["report_id"], "outputs": payload["outputs"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
