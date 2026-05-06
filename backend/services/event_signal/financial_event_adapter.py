"""Financial raw event adapter for unified event facts, relations, and signals.

The adapter consumes source-only Tushare raw tables and writes derived rows into
market.event_fact, market.event_relation, and market.event_signal.  It does not
modify any alpha, backtest, paper trading, or live trading consumer.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.init_tushare_event_raw_schema import init_tushare_event_raw_schema
from backend.db.init_unified_event_signal_schema import init_unified_event_signal_schema
from backend.db.pg_pool import get_conn
from backend.services.event_signal.announcement_adapter import (
    UNIFIED_RULE_VERSION,
    seed_unified_rule_set,
)
from backend.services.event_signal.time_semantics import (
    DEFAULT_PRE_OPEN_CUTOFF as PRE_OPEN_CUTOFF,
    compute_event_time,
    next_trading_day,
)


ROOT = Path(__file__).resolve().parents[3]
ENGINE_NAME = "FinancialEventAdapter"
FINANCIAL_RULE_VERSION = "financial_event_rules_v0_20260506"

SOURCE_TABLES: dict[str, str] = {
    "tushare_forecast": "market.tushare_forecast_raw",
    "tushare_express": "market.tushare_express_raw",
    "tushare_fina_indicator": "market.tushare_fina_indicator_raw",
}


@dataclass(frozen=True)
class FinancialClassification:
    event_family: str
    event_type: str
    risk_level: str
    action: str
    signal_type: str
    severity_score: Decimal
    confidence: Decimal
    reason: str
    should_signal: bool
    metrics: dict[str, Any]


@dataclass(frozen=True)
class FactBuild:
    row: dict[str, Any]
    event_key: str
    fact_tuple: tuple[Any, ...]
    classification: FinancialClassification


@dataclass(frozen=True)
class AdapterSummary:
    run_id: str
    rule_version: str
    time_mode: str
    processed_rows: int
    fact_rows: int
    relation_rows: int
    signal_rows: int
    status: str


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _jsonb(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value, dumps=_json_dumps)


def _decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _mean_decimal(*values: Any) -> Optional[Decimal]:
    nums = [num for num in (_decimal(value) for value in values) if num is not None]
    if not nums:
        return None
    return sum(nums, Decimal("0")) / Decimal(len(nums))


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lower = (text or "").lower()
    return any(keyword.lower() in lower for keyword in keywords)


def classify_forecast(payload: dict[str, Any]) -> FinancialClassification:
    forecast_type = str(payload.get("type") or "")
    p_min = _decimal(payload.get("p_change_min"))
    p_max = _decimal(payload.get("p_change_max"))
    p_mid = _mean_decimal(p_min, p_max)
    metrics = {
        "forecast_type": forecast_type,
        "p_change_min": p_min,
        "p_change_max": p_max,
        "forecast_mid": p_mid,
        "net_profit_min": _decimal(payload.get("net_profit_min")),
        "net_profit_max": _decimal(payload.get("net_profit_max")),
    }

    if _contains_any(forecast_type, ("首亏", "续亏", "预亏", "亏损", "loss")):
        return FinancialClassification(
            "financial_forecast",
            "financial_forecast_loss",
            "P2_REVIEW",
            "warn_review",
            "risk",
            Decimal("0.70"),
            Decimal("0.75"),
            "Performance forecast indicates loss risk",
            True,
            metrics,
        )
    if (
        (p_max is not None and p_max <= Decimal("-50"))
        or (p_min is not None and p_min <= Decimal("-50"))
        or (_contains_any(forecast_type, ("预减", "略减", "decline", "decrease")) and (p_mid is None or p_mid < 0))
    ):
        return FinancialClassification(
            "financial_forecast",
            "financial_forecast_large_decline",
            "P2_REVIEW",
            "warn_review",
            "risk",
            Decimal("0.62"),
            Decimal("0.70"),
            "Performance forecast indicates material profit decline",
            True,
            metrics,
        )
    if _contains_any(forecast_type, ("扭亏", "turnaround")):
        return FinancialClassification(
            "financial_forecast",
            "financial_forecast_turnaround",
            "P3_POSITIVE_CANDIDATE",
            "record_only",
            "research",
            Decimal("0.20"),
            Decimal("0.60"),
            "Performance forecast indicates possible turnaround; alpha disabled",
            True,
            metrics,
        )
    if (
        (p_min is not None and p_min >= Decimal("50"))
        or (p_mid is not None and p_mid >= Decimal("80"))
        or (_contains_any(forecast_type, ("预增", "略增", "续盈", "increase", "growth")) and p_mid is not None and p_mid >= Decimal("50"))
    ):
        return FinancialClassification(
            "financial_forecast",
            "financial_forecast_large_growth",
            "P3_POSITIVE_CANDIDATE",
            "record_only",
            "research",
            Decimal("0.20"),
            Decimal("0.60"),
            "Performance forecast indicates large growth; alpha disabled",
            True,
            metrics,
        )
    return FinancialClassification(
        "financial_forecast",
        "financial_forecast_neutral",
        "P4_NEUTRAL",
        "record_only",
        "audit",
        Decimal("0.00"),
        Decimal("0.50"),
        "Performance forecast is neutral under v0 financial rules",
        False,
        metrics,
    )


def _actual_yoy(payload: dict[str, Any], source_type: str) -> Optional[Decimal]:
    if source_type == "tushare_express":
        return _decimal(payload.get("yoy_dedu_np")) or _decimal(payload.get("yoy_net_profit"))
    return _decimal(payload.get("dt_netprofit_yoy")) or _decimal(payload.get("netprofit_yoy"))


def classify_actual(payload: dict[str, Any], source_type: str) -> FinancialClassification:
    actual_yoy = _actual_yoy(payload, source_type)
    net_profit = _decimal(payload.get("n_income"))
    family = "financial_express" if source_type == "tushare_express" else "financial_indicator"
    suffix = "express" if source_type == "tushare_express" else "indicator"
    metrics = {
        "actual_yoy": actual_yoy,
        "net_profit": net_profit,
        "source_type": source_type,
    }

    if net_profit is not None and net_profit < 0:
        return FinancialClassification(
            family,
            f"financial_{suffix}_loss",
            "P2_REVIEW",
            "warn_review",
            "risk",
            Decimal("0.70"),
            Decimal("0.78"),
            "Financial actual data indicates net loss",
            True,
            metrics,
        )
    if actual_yoy is not None and actual_yoy <= Decimal("-50"):
        return FinancialClassification(
            family,
            f"financial_{suffix}_large_decline",
            "P2_REVIEW",
            "warn_review",
            "risk",
            Decimal("0.62"),
            Decimal("0.74"),
            "Financial actual data indicates material profit decline",
            True,
            metrics,
        )
    if actual_yoy is not None and actual_yoy >= Decimal("50"):
        return FinancialClassification(
            family,
            f"financial_{suffix}_large_growth",
            "P3_POSITIVE_CANDIDATE",
            "record_only",
            "research",
            Decimal("0.20"),
            Decimal("0.62"),
            "Financial actual data indicates large growth; alpha disabled",
            True,
            metrics,
        )
    return FinancialClassification(
        family,
        f"financial_{suffix}_neutral",
        "P4_NEUTRAL",
        "record_only",
        "audit",
        Decimal("0.00"),
        Decimal("0.50"),
        "Financial actual data is neutral under v0 financial rules",
        False,
        metrics,
    )


def classify_financial_row(source_type: str, payload: dict[str, Any]) -> FinancialClassification:
    if source_type == "tushare_forecast":
        return classify_forecast(payload)
    return classify_actual(payload, source_type)


def infer_effective_date(
    ann_date: dt.date,
    trading_days: list[dt.date],
    *,
    time_mode: str = "backtest",
    first_seen_at: Optional[dt.datetime] = None,
    observed_at: Optional[dt.datetime] = None,
    source_publish_time: Optional[dt.datetime] = None,
) -> tuple[str, Optional[dt.datetime], dt.date, str]:
    result = compute_event_time(
        ann_date,
        trading_days,
        time_mode=time_mode,
        source_publish_time=source_publish_time,
        first_seen_at=first_seen_at,
        observed_at=observed_at,
    )
    return result.source_time_quality, result.available_at, result.effective_trade_date, result.effective_rule


def build_run_id(*, time_mode: str, run_mode: str) -> str:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"event_signal_financial_{run_mode}_{time_mode}_{FINANCIAL_RULE_VERSION}_{now}"


def build_event_key(source_type: str, raw_observation_id: int, *, rule_version: str, time_mode: str) -> str:
    return f"event_fact:{source_type}:{raw_observation_id}:{rule_version}:{time_mode}"


def build_signal_key(source_type: str, raw_observation_id: int, event_type: str, *, rule_version: str, time_mode: str) -> str:
    return f"event_signal:{source_type}:{raw_observation_id}:{event_type}:{rule_version}:{time_mode}"


def build_relation_key(
    left_event_key: str,
    right_event_key: str,
    relation_type: str,
    *,
    rule_version: str,
) -> str:
    return f"event_relation:{relation_type}:{left_event_key}:{right_event_key}:{rule_version}"


def build_relation_signal_key(relation_key: str, *, rule_version: str, time_mode: str) -> str:
    return f"event_signal:financial_relation:{relation_key}:{rule_version}:{time_mode}"


def build_fact(
    row: dict[str, Any],
    *,
    trading_days: list[dt.date],
    run_id: str,
    rule_version: str,
    time_mode: str,
) -> FactBuild:
    payload = row["raw_payload"] or {}
    classification = classify_financial_row(row["source_type"], payload)
    time_result = compute_event_time(
        row["ann_date"],
        trading_days,
        time_mode=time_mode,
        first_seen_at=row.get("first_seen_at"),
        observed_at=row.get("observed_at"),
    )
    event_key = build_event_key(
        row["source_type"],
        int(row["raw_observation_id"]),
        rule_version=rule_version,
        time_mode=time_mode,
    )
    facts = {
        "adapter": ENGINE_NAME,
        "financial_rule_version": FINANCIAL_RULE_VERSION,
        "source_api": row.get("source_api"),
        "raw_observation_id": row["raw_observation_id"],
        "source_record_key": row["source_record_key"],
        "event_type": classification.event_type,
        "risk_level": classification.risk_level,
        "action": classification.action,
        "reason": classification.reason,
        "metrics": classification.metrics,
        "effective_rule": time_result.effective_rule,
        "time_semantics": time_result.trace,
        "raw_payload": payload,
    }
    fact_tuple = (
        event_key,
        row["ts_code"],
        classification.event_family,
        classification.event_type,
        "ACTIVE",
        row["source_type"],
        str(row["raw_observation_id"]),
        row["source_record_key"],
        row["ann_date"],
        time_result.source_available_at,
        time_result.source_time_quality,
        time_result.available_at,
        time_result.effective_trade_date,
        time_mode,
        row["report_period"],
        rule_version,
        run_id,
        classification.confidence,
        _jsonb(facts),
        row["source_row_hash"],
    )
    return FactBuild(row=row, event_key=event_key, fact_tuple=fact_tuple, classification=classification)


def build_signal_tuple(
    fact: FactBuild,
    *,
    event_id: int,
    run_id: str,
    rule_version: str,
    time_mode: str,
) -> tuple[Any, ...]:
    row = fact.row
    classification = fact.classification
    evidence = {
        "adapter": ENGINE_NAME,
        "financial_rule_version": FINANCIAL_RULE_VERSION,
        "source_type": row["source_type"],
        "raw_observation_id": row["raw_observation_id"],
        "source_record_key": row["source_record_key"],
        "source_api": row.get("source_api"),
        "event_id": event_id,
        "metrics": classification.metrics,
        "raw_payload": row.get("raw_payload") or {},
    }
    source_time_quality = fact.fact_tuple[10]
    available_at = fact.fact_tuple[11]
    effective_trade_date = fact.fact_tuple[12]
    return (
        build_signal_key(
            row["source_type"],
            int(row["raw_observation_id"]),
            classification.event_type,
            rule_version=rule_version,
            time_mode=time_mode,
        ),
        row["ts_code"],
        event_id,
        [event_id],
        [],
        row["source_type"],
        str(row["raw_observation_id"]),
        row["ann_date"],
        source_time_quality,
        available_at,
        effective_trade_date,
        time_mode,
        classification.event_family,
        classification.event_type,
        classification.risk_level,
        classification.action,
        classification.signal_type,
        "ACTIVE",
        classification.severity_score,
        classification.confidence,
        Decimal("0.0"),
        classification.reason,
        _jsonb(evidence),
        fact.fact_tuple[18].adapted["effective_rule"],
        rule_version,
        run_id,
    )


def fetch_latest_raw_rows(
    conn: Any,
    *,
    source_types: list[str],
    report_period: Optional[dt.date] = None,
    start_report_period: Optional[dt.date] = None,
    end_report_period: Optional[dt.date] = None,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    queries: list[str] = []
    params: list[Any] = []
    for source_type in source_types:
        table_name = SOURCE_TABLES[source_type]
        filters = []
        if report_period is not None:
            filters.append("report_period = %s")
            params.append(report_period)
        if start_report_period is not None:
            filters.append("report_period >= %s")
            params.append(start_report_period)
        if end_report_period is not None:
            filters.append("report_period <= %s")
            params.append(end_report_period)
        where_sql = "" if not filters else "WHERE " + " AND ".join(filters)
        queries.append(
            f"""
            SELECT *
              FROM (
                    SELECT DISTINCT ON (source_record_key)
                           %s::text AS source_type,
                           raw_observation_id, source_api, fetch_params, source_record_key,
                           ts_code, ann_date, report_period, source_row_hash, raw_payload,
                           first_seen_at, last_seen_at, observed_at
                      FROM {table_name}
                      {where_sql}
                     ORDER BY source_record_key, last_seen_at DESC, raw_observation_id DESC
                   ) latest_{source_type}
            """
        )
        params.insert(len(params) - len(filters), source_type)
    sql = "\nUNION ALL\n".join(queries) + "\nORDER BY source_type, raw_observation_id"
    if limit is not None:
        sql += "\nLIMIT %s"
        params.append(limit)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]


def load_trading_days(conn: Any, start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date
              FROM market.trading_calendar
             WHERE is_trading = TRUE
               AND cal_date >= %s
               AND cal_date <= %s
             ORDER BY cal_date
            """,
            (start_date - dt.timedelta(days=5), end_date + dt.timedelta(days=60)),
        )
        rows = [row[0] for row in cur.fetchall()]
    if not rows:
        raise RuntimeError("market.trading_calendar has no rows for requested financial event range")
    return rows


def upsert_facts(conn: Any, facts: list[FactBuild]) -> dict[str, int]:
    if not facts:
        return {}
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.event_fact
                (
                    event_key, ts_code, event_family, event_type, event_status,
                    source_type, source_pk, source_record_key,
                    source_event_date, source_available_at, source_time_quality,
                    available_at, effective_trade_date, time_mode, report_period,
                    rule_version, run_id, fact_confidence, facts, source_payload_hash
                )
            VALUES %s
            ON CONFLICT (event_key) DO UPDATE SET
                ts_code = EXCLUDED.ts_code,
                event_family = EXCLUDED.event_family,
                event_type = EXCLUDED.event_type,
                event_status = EXCLUDED.event_status,
                source_type = EXCLUDED.source_type,
                source_pk = EXCLUDED.source_pk,
                source_record_key = EXCLUDED.source_record_key,
                source_event_date = EXCLUDED.source_event_date,
                source_available_at = EXCLUDED.source_available_at,
                source_time_quality = EXCLUDED.source_time_quality,
                available_at = EXCLUDED.available_at,
                effective_trade_date = EXCLUDED.effective_trade_date,
                time_mode = EXCLUDED.time_mode,
                report_period = EXCLUDED.report_period,
                rule_version = EXCLUDED.rule_version,
                run_id = EXCLUDED.run_id,
                fact_confidence = EXCLUDED.fact_confidence,
                facts = EXCLUDED.facts,
                source_payload_hash = EXCLUDED.source_payload_hash,
                generated_at = NOW(),
                updated_at = NOW()
            """,
            [fact.fact_tuple for fact in facts],
            page_size=1000,
        )
        keys = [fact.event_key for fact in facts]
        cur.execute("SELECT event_key, event_id FROM market.event_fact WHERE event_key = ANY(%s)", (keys,))
        return {key: int(event_id) for key, event_id in cur.fetchall()}


def upsert_fact_signals(
    conn: Any,
    facts: list[FactBuild],
    event_ids_by_key: dict[str, int],
    *,
    run_id: str,
    rule_version: str,
    time_mode: str,
) -> int:
    values = [
        build_signal_tuple(
            fact,
            event_id=event_ids_by_key[fact.event_key],
            run_id=run_id,
            rule_version=rule_version,
            time_mode=time_mode,
        )
        for fact in facts
        if fact.classification.should_signal
    ]
    if not values:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.event_signal
                (
                    signal_key, ts_code, event_id, source_event_ids, relation_ids,
                    source_type, source_pk, source_event_date, source_time_quality,
                    available_at, effective_trade_date, time_mode,
                    event_family, event_type, risk_level, action, signal_type,
                    signal_status, severity_score, confidence, alpha_score,
                    reason, evidence, effective_rule, rule_version, run_id
                )
            VALUES %s
            ON CONFLICT (signal_key) DO UPDATE SET
                ts_code = EXCLUDED.ts_code,
                event_id = EXCLUDED.event_id,
                source_event_ids = EXCLUDED.source_event_ids,
                relation_ids = EXCLUDED.relation_ids,
                source_type = EXCLUDED.source_type,
                source_pk = EXCLUDED.source_pk,
                source_event_date = EXCLUDED.source_event_date,
                source_time_quality = EXCLUDED.source_time_quality,
                available_at = EXCLUDED.available_at,
                effective_trade_date = EXCLUDED.effective_trade_date,
                time_mode = EXCLUDED.time_mode,
                event_family = EXCLUDED.event_family,
                event_type = EXCLUDED.event_type,
                risk_level = EXCLUDED.risk_level,
                action = EXCLUDED.action,
                signal_type = EXCLUDED.signal_type,
                signal_status = EXCLUDED.signal_status,
                severity_score = EXCLUDED.severity_score,
                confidence = EXCLUDED.confidence,
                alpha_score = EXCLUDED.alpha_score,
                reason = EXCLUDED.reason,
                evidence = EXCLUDED.evidence,
                effective_rule = EXCLUDED.effective_rule,
                rule_version = EXCLUDED.rule_version,
                run_id = EXCLUDED.run_id,
                generated_at = NOW(),
                updated_at = NOW()
            """,
            values,
            page_size=1000,
        )
        return len(values)


def build_miss_relations(
    facts: list[FactBuild],
    event_ids_by_key: dict[str, int],
    *,
    rule_version: str,
    run_id: str,
) -> list[tuple[tuple[Any, ...], FactBuild, FactBuild, Decimal]]:
    grouped: dict[tuple[str, dt.date], list[FactBuild]] = {}
    for fact in facts:
        key = (fact.row["ts_code"], fact.row["report_period"])
        grouped.setdefault(key, []).append(fact)

    relations: list[tuple[tuple[Any, ...], FactBuild, FactBuild, Decimal]] = []
    for _, group in grouped.items():
        forecasts = [fact for fact in group if fact.row["source_type"] == "tushare_forecast"]
        actuals = [fact for fact in group if fact.row["source_type"] in {"tushare_express", "tushare_fina_indicator"}]
        for forecast in forecasts:
            forecast_mid = forecast.classification.metrics.get("forecast_mid")
            if forecast_mid is None or forecast_mid < Decimal("50"):
                continue
            for actual in actuals:
                actual_yoy = actual.classification.metrics.get("actual_yoy")
                if actual_yoy is None:
                    continue
                miss_gap = forecast_mid - actual_yoy
                if miss_gap < Decimal("30"):
                    continue
                strength = max(Decimal("0.30"), min(Decimal("1.00"), miss_gap / Decimal("100")))
                left_id = event_ids_by_key[forecast.event_key]
                right_id = event_ids_by_key[actual.event_key]
                relation_key = build_relation_key(
                    forecast.event_key,
                    actual.event_key,
                    "misses_prior_expectation",
                    rule_version=rule_version,
                )
                metrics = {
                    "forecast_mid": forecast_mid,
                    "actual_yoy": actual_yoy,
                    "miss_gap": miss_gap,
                    "actual_source_type": actual.row["source_type"],
                }
                evidence = {
                    "adapter": ENGINE_NAME,
                    "financial_rule_version": FINANCIAL_RULE_VERSION,
                    "left_event_key": forecast.event_key,
                    "right_event_key": actual.event_key,
                    "forecast_raw_observation_id": forecast.row["raw_observation_id"],
                    "actual_raw_observation_id": actual.row["raw_observation_id"],
                    "metrics": metrics,
                }
                relations.append(
                    (
                        (
                            relation_key,
                            "misses_prior_expectation",
                            actual.row["ts_code"],
                            actual.row["report_period"],
                            left_id,
                            right_id,
                            "ACTIVE",
                            rule_version,
                            run_id,
                            strength,
                            Decimal("0.78"),
                            _jsonb(metrics),
                            _jsonb(evidence),
                        ),
                        forecast,
                        actual,
                        strength,
                    )
                )
    return relations


def upsert_relations(
    conn: Any,
    relation_rows: list[tuple[tuple[Any, ...], FactBuild, FactBuild, Decimal]],
) -> dict[str, int]:
    if not relation_rows:
        return {}
    values = [item[0] for item in relation_rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.event_relation
                (
                    relation_key, relation_type, ts_code, report_period,
                    left_event_id, right_event_id, relation_status,
                    rule_version, run_id, strength_score, confidence,
                    metrics, evidence
                )
            VALUES %s
            ON CONFLICT (relation_key) DO UPDATE SET
                relation_type = EXCLUDED.relation_type,
                ts_code = EXCLUDED.ts_code,
                report_period = EXCLUDED.report_period,
                left_event_id = EXCLUDED.left_event_id,
                right_event_id = EXCLUDED.right_event_id,
                relation_status = EXCLUDED.relation_status,
                rule_version = EXCLUDED.rule_version,
                run_id = EXCLUDED.run_id,
                strength_score = EXCLUDED.strength_score,
                confidence = EXCLUDED.confidence,
                metrics = EXCLUDED.metrics,
                evidence = EXCLUDED.evidence,
                generated_at = NOW(),
                updated_at = NOW()
            """,
            values,
            page_size=1000,
        )
        keys = [value[0] for value in values]
        cur.execute("SELECT relation_key, relation_id FROM market.event_relation WHERE relation_key = ANY(%s)", (keys,))
        return {key: int(relation_id) for key, relation_id in cur.fetchall()}


def build_relation_signal_tuple(
    relation_tuple: tuple[Any, ...],
    forecast: FactBuild,
    actual: FactBuild,
    strength: Decimal,
    *,
    relation_id: int,
    rule_version: str,
    run_id: str,
    time_mode: str,
) -> tuple[Any, ...]:
    relation_key = relation_tuple[0]
    actual_event_id = relation_tuple[5]
    forecast_event_id = relation_tuple[4]
    actual_time_quality = actual.fact_tuple[10]
    actual_available_at = actual.fact_tuple[11]
    actual_effective_date = actual.fact_tuple[12]
    evidence = {
        "adapter": ENGINE_NAME,
        "financial_rule_version": FINANCIAL_RULE_VERSION,
        "relation_key": relation_key,
        "relation_type": "misses_prior_expectation",
        "relation_id": relation_id,
        "forecast_event_id": forecast_event_id,
        "actual_event_id": actual_event_id,
        "metrics": relation_tuple[11].adapted,
    }
    reason = (
        "Financial actual result is positive or weaker than a prior high-growth forecast; "
        "review expectation miss risk"
    )
    return (
        build_relation_signal_key(relation_key, rule_version=rule_version, time_mode=time_mode),
        actual.row["ts_code"],
        actual_event_id,
        [forecast_event_id, actual_event_id],
        [relation_id],
        "financial_relation",
        relation_key,
        actual.row["ann_date"],
        actual_time_quality,
        actual_available_at,
        actual_effective_date,
        time_mode,
        "financial_relation",
        "financial_positive_but_miss_expectation",
        "P2_REVIEW",
        "warn_review",
        "risk",
        "ACTIVE",
        max(Decimal("0.55"), strength),
        Decimal("0.75"),
        Decimal("0.0"),
        reason,
        _jsonb(evidence),
        actual.fact_tuple[18].adapted["effective_rule"],
        rule_version,
        run_id,
    )


def upsert_relation_signals(
    conn: Any,
    relation_rows: list[tuple[tuple[Any, ...], FactBuild, FactBuild, Decimal]],
    relation_ids_by_key: dict[str, int],
    *,
    rule_version: str,
    run_id: str,
    time_mode: str,
) -> int:
    values = [
        build_relation_signal_tuple(
            relation_tuple,
            forecast,
            actual,
            strength,
            relation_id=relation_ids_by_key[relation_tuple[0]],
            rule_version=rule_version,
            run_id=run_id,
            time_mode=time_mode,
        )
        for relation_tuple, forecast, actual, strength in relation_rows
    ]
    if not values:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.event_signal
                (
                    signal_key, ts_code, event_id, source_event_ids, relation_ids,
                    source_type, source_pk, source_event_date, source_time_quality,
                    available_at, effective_trade_date, time_mode,
                    event_family, event_type, risk_level, action, signal_type,
                    signal_status, severity_score, confidence, alpha_score,
                    reason, evidence, effective_rule, rule_version, run_id
                )
            VALUES %s
            ON CONFLICT (signal_key) DO UPDATE SET
                ts_code = EXCLUDED.ts_code,
                event_id = EXCLUDED.event_id,
                source_event_ids = EXCLUDED.source_event_ids,
                relation_ids = EXCLUDED.relation_ids,
                source_type = EXCLUDED.source_type,
                source_pk = EXCLUDED.source_pk,
                source_event_date = EXCLUDED.source_event_date,
                source_time_quality = EXCLUDED.source_time_quality,
                available_at = EXCLUDED.available_at,
                effective_trade_date = EXCLUDED.effective_trade_date,
                time_mode = EXCLUDED.time_mode,
                event_family = EXCLUDED.event_family,
                event_type = EXCLUDED.event_type,
                risk_level = EXCLUDED.risk_level,
                action = EXCLUDED.action,
                signal_type = EXCLUDED.signal_type,
                signal_status = EXCLUDED.signal_status,
                severity_score = EXCLUDED.severity_score,
                confidence = EXCLUDED.confidence,
                alpha_score = EXCLUDED.alpha_score,
                reason = EXCLUDED.reason,
                evidence = EXCLUDED.evidence,
                effective_rule = EXCLUDED.effective_rule,
                rule_version = EXCLUDED.rule_version,
                run_id = EXCLUDED.run_id,
                generated_at = NOW(),
                updated_at = NOW()
            """,
            values,
            page_size=1000,
        )
        return len(values)


def start_run(
    conn: Any,
    *,
    run_id: str,
    rule_version: str,
    time_mode: str,
    run_mode: str,
    report_period: Optional[dt.date],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.event_signal_run
                (run_id, rule_version, run_mode, time_mode, source_scope, date_from, date_to, status)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, 'RUNNING')
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                run_id,
                rule_version,
                run_mode,
                time_mode,
                _json_dumps(
                    {
                        "source_type": "tushare_financial_raw",
                        "source_tables": list(SOURCE_TABLES.values()),
                        "financial_rule_version": FINANCIAL_RULE_VERSION,
                        "report_period": report_period,
                    }
                ),
                report_period,
                report_period,
            ),
        )


def finish_run(
    conn: Any,
    *,
    run_id: str,
    status: str,
    source_input_rows: int,
    fact_rows: int,
    relation_rows: int,
    signal_rows: int,
    error_message: Optional[str] = None,
    metrics: Optional[dict[str, Any]] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.event_signal_run
               SET finished_at = NOW(),
                   status = %s,
                   source_input_rows = %s,
                   fact_rows = %s,
                   relation_rows = %s,
                   signal_rows = %s,
                   error_message = %s,
                   metrics = %s::jsonb,
                   updated_at = NOW()
             WHERE run_id = %s
            """,
            (status, source_input_rows, fact_rows, relation_rows, signal_rows, error_message, _json_dumps(metrics or {}), run_id),
        )


def sync_financial_event_signals(
    *,
    rule_version: str = UNIFIED_RULE_VERSION,
    time_mode: str = "backtest",
    run_mode: str = "incremental",
    report_period: Optional[dt.date] = None,
    source_types: Optional[list[str]] = None,
    limit: Optional[int] = None,
    ensure_schema: bool = True,
) -> AdapterSummary:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")
    sources = source_types or list(SOURCE_TABLES)
    if unknown := [source for source in sources if source not in SOURCE_TABLES]:
        raise ValueError(f"unknown source_types: {unknown}")
    if ensure_schema:
        init_unified_event_signal_schema()
        init_tushare_event_raw_schema()

    run_id = build_run_id(time_mode=time_mode, run_mode=run_mode)
    with get_conn() as conn:
        seed_unified_rule_set(conn, rule_version=rule_version)
        start_run(
            conn,
            run_id=run_id,
            rule_version=rule_version,
            time_mode=time_mode,
            run_mode=run_mode,
            report_period=report_period,
        )
        try:
            raw_rows = fetch_latest_raw_rows(
                conn,
                source_types=sources,
                report_period=report_period,
                limit=limit,
            )
            if not raw_rows:
                finish_run(
                    conn,
                    run_id=run_id,
                    status="SUCCESS",
                    source_input_rows=0,
                    fact_rows=0,
                    relation_rows=0,
                    signal_rows=0,
                    metrics={"empty": True, "source_types": sources},
                )
                return AdapterSummary(run_id, rule_version, time_mode, 0, 0, 0, 0, "SUCCESS")

            min_ann = min(row["ann_date"] for row in raw_rows)
            max_ann = max(row["ann_date"] for row in raw_rows)
            trading_days = load_trading_days(conn, min_ann, max_ann)
            facts = [
                build_fact(
                    row,
                    trading_days=trading_days,
                    run_id=run_id,
                    rule_version=rule_version,
                    time_mode=time_mode,
                )
                for row in raw_rows
            ]
            event_ids = upsert_facts(conn, facts)
            fact_signal_rows = upsert_fact_signals(
                conn,
                facts,
                event_ids,
                run_id=run_id,
                rule_version=rule_version,
                time_mode=time_mode,
            )
            relation_rows = build_miss_relations(facts, event_ids, rule_version=rule_version, run_id=run_id)
            relation_ids = upsert_relations(conn, relation_rows)
            relation_signal_rows = upsert_relation_signals(
                conn,
                relation_rows,
                relation_ids,
                rule_version=rule_version,
                run_id=run_id,
                time_mode=time_mode,
            )
            signal_rows = fact_signal_rows + relation_signal_rows
            finish_run(
                conn,
                run_id=run_id,
                status="SUCCESS",
                source_input_rows=len(raw_rows),
                fact_rows=len(facts),
                relation_rows=len(relation_rows),
                signal_rows=signal_rows,
                metrics={
                    "source_types": sources,
                    "report_period": report_period,
                    "fact_signal_rows": fact_signal_rows,
                    "relation_signal_rows": relation_signal_rows,
                },
            )
            status = "SUCCESS"
        except Exception as exc:
            finish_run(
                conn,
                run_id=run_id,
                status="FAILED",
                source_input_rows=0,
                fact_rows=0,
                relation_rows=0,
                signal_rows=0,
                error_message=str(exc)[:4000],
                metrics={"source_types": sources, "report_period": report_period},
            )
            raise

    return AdapterSummary(
        run_id=run_id,
        rule_version=rule_version,
        time_mode=time_mode,
        processed_rows=len(raw_rows),
        fact_rows=len(facts),
        relation_rows=len(relation_rows),
        signal_rows=signal_rows,
        status=status,
    )


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return dt.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return dt.date.fromisoformat(text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate unified financial event facts, relations, and signals")
    parser.add_argument("--rule-version", default=UNIFIED_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--run-mode", choices=["backfill", "incremental", "smoke", "repair", "research"], default="incremental")
    parser.add_argument("--report-period", default=None, help="Optional report period, for example 20231231")
    parser.add_argument("--source-type", action="append", choices=sorted(SOURCE_TABLES), default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--no-ensure-schema", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(ROOT / ".env", override=True)
    load_dotenv(override=False)
    summary = sync_financial_event_signals(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        run_mode=args.run_mode,
        report_period=_parse_date(args.report_period),
        source_types=args.source_type,
        limit=args.limit,
        ensure_schema=not args.no_ensure_schema,
    )
    print(_json_dumps(summary.__dict__))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
