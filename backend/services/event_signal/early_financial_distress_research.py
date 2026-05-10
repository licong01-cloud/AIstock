"""Read-only research for early financial distress signals before ST events.

The module studies whether structured financial event signals can warn about
future ST/delisting-risk cycles.  It writes JSON/Markdown reports only and does
not mutate raw tables, derived signals, QE, Selection, Paper, or live trading.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn


ROOT = Path(__file__).resolve().parents[3]
FINANCIAL_RULE_VERSION = "unified_event_signal_rules_v0_20260506"
ST_RULE_VERSION = "unified_event_signal_rules_st_first_v1_20260506"
DEFAULT_TIME_MODE = "backtest"
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_CYCLE_GAP_DAYS = 180
DEFAULT_COMBO_WINDOW_DAYS = 120
DEFAULT_HORIZONS = (90, 180, 365)
DEFAULT_RETURN_WINDOWS = (0, 1, 5, 10, 20, 60)
PRICE_UNIT_DIVISOR = 1000.0

FINANCIAL_RISK_EVENT_TYPES: tuple[str, ...] = (
    "financial_forecast_loss",
    "financial_forecast_large_decline",
    "financial_express_loss",
    "financial_express_large_decline",
    "financial_indicator_large_decline",
    "financial_positive_but_miss_expectation",
)

FINANCIAL_POSITIVE_OR_RECORD_EVENT_TYPES: tuple[str, ...] = (
    "financial_forecast_large_growth",
    "financial_forecast_turnaround",
    "financial_express_large_growth",
    "financial_indicator_large_growth",
)

FINANCIAL_SOURCE_TYPES: tuple[str, ...] = (
    "tushare_forecast",
    "tushare_express",
    "tushare_fina_indicator",
    "financial_relation",
)

ST_TARGET_EVENT_TYPES: tuple[str, ...] = (
    "stock_st_imposed",
    "stock_st_added_or_continued",
    "stock_delisting_risk_warning",
    "stock_delisting_confirmed",
)

LEAD_BUCKETS: tuple[tuple[str, int, Optional[int]], ...] = (
    ("1-7", 1, 7),
    ("8-30", 8, 30),
    ("31-60", 31, 60),
    ("61-120", 61, 120),
    ("121-180", 121, 180),
    ("181-365", 181, 365),
    ("365+", 366, None),
)


@dataclass(frozen=True)
class FinancialRiskSignal:
    signal_id: int
    ts_code: str
    source_type: str
    event_type: str
    risk_level: str
    action: str
    source_event_date: dt.date
    effective_trade_date: dt.date
    severity_score: Optional[float] = None
    confidence: Optional[float] = None
    metric_bucket: str = "unbucketed"
    metric_detail: Optional[dict[str, Any]] = None
    report_period: Optional[dt.date] = None


@dataclass(frozen=True)
class StTargetEvent:
    signal_id: int
    ts_code: str
    event_type: str
    source_event_date: dt.date
    effective_trade_date: dt.date


@dataclass(frozen=True)
class StCycle:
    cycle_id: str
    ts_code: str
    start_event_date: dt.date
    start_effective_trade_date: dt.date
    end_effective_trade_date: dt.date
    primary_event_type: str
    event_types: tuple[str, ...]
    signal_ids: tuple[int, ...]


@dataclass(frozen=True)
class ResearchSummary:
    report_id: str
    output_json: str
    output_md: str
    output_csv: Optional[str]
    financial_signals_loaded: int
    financial_signals_in_study_window: int
    st_events: int
    st_cycles: int


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


def _mean(values: Sequence[float]) -> Optional[float]:
    return float(statistics.fmean(values)) if values else None


def _median(values: Sequence[float]) -> Optional[float]:
    return float(statistics.median(values)) if values else None


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
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


def _return_from_close(base_close: Optional[float], target_close: Optional[float]) -> Optional[float]:
    if base_close is None or target_close is None or base_close <= 0:
        return None
    return target_close / base_close - 1.0


def _rate(numerator: int, denominator: int) -> Optional[float]:
    return numerator / denominator if denominator else None


def _lead_bucket(days: int) -> str:
    for name, lo, hi in LEAD_BUCKETS:
        if days >= lo and (hi is None or days <= hi):
            return name
    return "unknown"


def _date_minus_days(value: Optional[dt.date], days: int) -> Optional[dt.date]:
    return value - dt.timedelta(days=days) if value else None


def _date_plus_days(value: dt.date, days: int) -> dt.date:
    return value + dt.timedelta(days=days)


def _loss_abs_bucket(value: Optional[float], *, unit: str) -> str:
    if value is None:
        return f"{unit}_unknown"
    loss_abs = abs(value) if value < 0 else 0.0
    if unit == "wan":
        if loss_abs >= 100000:
            return "loss_ge_10bn_yuan"
        if loss_abs >= 10000:
            return "loss_1bn_to_10bn_yuan"
        if loss_abs >= 1000:
            return "loss_100m_to_1bn_yuan"
        return "loss_lt_100m_yuan"
    if loss_abs >= 1000000000:
        return "loss_ge_1bn_yuan"
    if loss_abs >= 100000000:
        return "loss_100m_to_1bn_yuan"
    if loss_abs >= 10000000:
        return "loss_10m_to_100m_yuan"
    return "loss_lt_10m_yuan"


def extract_report_period(evidence: Optional[Mapping[str, Any]]) -> Optional[dt.date]:
    evidence = evidence if isinstance(evidence, Mapping) else {}
    raw = evidence.get("raw_payload") if isinstance(evidence.get("raw_payload"), Mapping) else {}
    for key in ("end_date", "report_period"):
        value = raw.get(key) or evidence.get(key)
        if isinstance(value, dt.date):
            return value
        if value is not None:
            try:
                parsed = _parse_date(str(value))
            except ValueError:
                parsed = None
            if parsed is not None:
                return parsed
    source_key = str(evidence.get("source_record_key") or "")
    parts = source_key.split(":")
    if len(parts) >= 4:
        try:
            return _parse_date(parts[-1])
        except ValueError:
            return None
    return None


def classify_metric_bucket(event_type: str, evidence: Optional[Mapping[str, Any]]) -> tuple[str, dict[str, Any]]:
    evidence = evidence if isinstance(evidence, Mapping) else {}
    raw = evidence.get("raw_payload") if isinstance(evidence.get("raw_payload"), Mapping) else {}
    metrics = evidence.get("metrics") if isinstance(evidence.get("metrics"), Mapping) else {}
    if event_type == "financial_forecast_loss":
        forecast_type = str(raw.get("type") or "unknown")
        np_min = _safe_float(raw.get("net_profit_min"))
        np_max = _safe_float(raw.get("net_profit_max"))
        candidates = [value for value in (np_min, np_max) if value is not None]
        worst_loss_wan = min(candidates) if candidates else None
        p_change_min = _safe_float(raw.get("p_change_min"))
        p_change_max = _safe_float(raw.get("p_change_max"))
        p_change_mid = _safe_float(metrics.get("forecast_mid"))
        if p_change_mid is None:
            pct_candidates = [value for value in (p_change_min, p_change_max) if value is not None]
            p_change_mid = _mean(pct_candidates)
        bucket = _loss_abs_bucket(worst_loss_wan, unit="wan")
        return (
            f"forecast_loss:type={forecast_type}|{bucket}",
            {
                "forecast_type": forecast_type,
                "worst_loss_wan": worst_loss_wan,
                "p_change_min": p_change_min,
                "p_change_max": p_change_max,
                "p_change_mid": p_change_mid,
            },
        )
    if event_type == "financial_forecast_large_decline":
        p_change_min = _safe_float(raw.get("p_change_min"))
        p_change_max = _safe_float(raw.get("p_change_max"))
        p_change_mid = _safe_float(metrics.get("forecast_mid"))
        if p_change_mid is None:
            pct_candidates = [value for value in (p_change_min, p_change_max) if value is not None]
            p_change_mid = _mean(pct_candidates)
        return (
            f"{event_type}:default",
            {
                "forecast_type": str(raw.get("type") or "unknown"),
                "p_change_min": p_change_min,
                "p_change_max": p_change_max,
                "p_change_mid": p_change_mid,
                "net_profit_min_wan": _safe_float(raw.get("net_profit_min")),
                "net_profit_max_wan": _safe_float(raw.get("net_profit_max")),
            },
        )
    if event_type == "financial_express_loss":
        net_profit = _safe_float(raw.get("n_income"))
        if net_profit is None:
            net_profit = _safe_float(metrics.get("net_profit"))
        equity = _safe_float(raw.get("total_hldr_eqy_exc_min_int"))
        equity_bucket = "negative_equity" if equity is not None and equity < 0 else "equity_non_negative_or_unknown"
        bucket = _loss_abs_bucket(net_profit, unit="yuan")
        return (
            f"express_loss:{bucket}|{equity_bucket}",
            {
                "net_profit_yuan": net_profit,
                "equity_yuan": equity,
                "equity_bucket": equity_bucket,
                "actual_yoy": _safe_float(metrics.get("actual_yoy")),
                "revenue_yuan": _safe_float(raw.get("revenue")),
                "operate_profit_yuan": _safe_float(raw.get("operate_profit")),
                "total_profit_yuan": _safe_float(raw.get("total_profit")),
            },
        )
    if event_type == "financial_express_large_decline":
        return (
            f"{event_type}:default",
            {
                "actual_yoy": _safe_float(metrics.get("actual_yoy")),
                "net_profit_yuan": _safe_float(raw.get("n_income")),
                "revenue_yuan": _safe_float(raw.get("revenue")),
                "operate_profit_yuan": _safe_float(raw.get("operate_profit")),
                "total_profit_yuan": _safe_float(raw.get("total_profit")),
                "equity_yuan": _safe_float(raw.get("total_hldr_eqy_exc_min_int")),
                "diluted_roe": _safe_float(raw.get("diluted_roe")),
            },
        )
    if event_type == "financial_indicator_large_decline":
        actual_yoy = _safe_float(metrics.get("actual_yoy"))
        return (
            f"{event_type}:default",
            {
                "actual_yoy": actual_yoy,
                "netprofit_yoy": _safe_float(raw.get("netprofit_yoy")),
                "q_netprofit_yoy": _safe_float(raw.get("q_netprofit_yoy")),
                "dt_netprofit_yoy": _safe_float(raw.get("dt_netprofit_yoy")),
                "q_profit_yoy": _safe_float(raw.get("q_profit_yoy")),
                "op_yoy": _safe_float(raw.get("op_yoy")),
                "q_op_yoy": _safe_float(raw.get("q_op_yoy")),
                "or_yoy": _safe_float(raw.get("or_yoy")),
                "tr_yoy": _safe_float(raw.get("tr_yoy")),
                "q_sales_yoy": _safe_float(raw.get("q_sales_yoy")),
                "ocf_yoy": _safe_float(raw.get("ocf_yoy")),
                "q_ocf_to_sales": _safe_float(raw.get("q_ocf_to_sales")),
                "roe": _safe_float(raw.get("roe")),
                "roe_waa": _safe_float(raw.get("roe_waa")),
                "roe_dt": _safe_float(raw.get("roe_dt")),
                "roa": _safe_float(raw.get("roa")),
                "debt_to_assets": _safe_float(raw.get("debt_to_assets")),
                "current_ratio": _safe_float(raw.get("current_ratio")),
                "quick_ratio": _safe_float(raw.get("quick_ratio")),
                "grossprofit_margin": _safe_float(raw.get("grossprofit_margin")),
                "netprofit_margin": _safe_float(raw.get("netprofit_margin")),
                "profit_to_gr": _safe_float(raw.get("profit_to_gr")),
                "eps": _safe_float(raw.get("eps")),
                "dt_eps": _safe_float(raw.get("dt_eps")),
            },
        )
    if event_type == "financial_positive_but_miss_expectation":
        miss_gap = _safe_float(metrics.get("miss_gap"))
        forecast_mid = _safe_float(metrics.get("forecast_mid"))
        actual_yoy = _safe_float(metrics.get("actual_yoy"))
        actual_source_type = str(metrics.get("actual_source_type") or "unknown")
        if miss_gap is None:
            miss_gap_bucket = "miss_gap_unknown"
        elif miss_gap >= 100:
            miss_gap_bucket = "miss_gap_ge_100pct"
        elif miss_gap >= 50:
            miss_gap_bucket = "miss_gap_50pct_to_100pct"
        else:
            miss_gap_bucket = "miss_gap_30pct_to_50pct"
        return (
            f"expectation_miss:{miss_gap_bucket}|actual_source={actual_source_type}",
            {
                "forecast_mid": forecast_mid,
                "actual_yoy": actual_yoy,
                "miss_gap": miss_gap,
                "actual_source_type": actual_source_type,
                "miss_gap_bucket": miss_gap_bucket,
            },
        )
    return (f"{event_type}:default", {})


def build_st_cycles(events: Iterable[StTargetEvent], *, cycle_gap_days: int = DEFAULT_CYCLE_GAP_DAYS) -> list[StCycle]:
    """Merge repeated ST target events into per-symbol risk cycles."""

    sorted_events = sorted(
        events,
        key=lambda row: (row.ts_code, row.effective_trade_date, row.source_event_date, row.signal_id),
    )
    cycles: list[StCycle] = []
    current: list[StTargetEvent] = []
    cycle_index_by_symbol: dict[str, int] = defaultdict(int)

    def flush() -> None:
        if not current:
            return
        first = current[0]
        last_effective = max(row.effective_trade_date for row in current)
        cycle_index_by_symbol[first.ts_code] += 1
        cycle_id = "{}:{}:{:03d}".format(
            first.ts_code,
            first.effective_trade_date.isoformat().replace("-", ""),
            cycle_index_by_symbol[first.ts_code],
        )
        cycles.append(
            StCycle(
                cycle_id=cycle_id,
                ts_code=first.ts_code,
                start_event_date=min(row.source_event_date for row in current),
                start_effective_trade_date=first.effective_trade_date,
                end_effective_trade_date=last_effective,
                primary_event_type=first.event_type,
                event_types=tuple(dict.fromkeys(row.event_type for row in current)),
                signal_ids=tuple(row.signal_id for row in current),
            )
        )

    for event in sorted_events:
        if not current:
            current = [event]
            continue
        prev = current[-1]
        gap_days = (event.effective_trade_date - prev.effective_trade_date).days
        same_cycle = event.ts_code == prev.ts_code and gap_days <= cycle_gap_days
        if same_cycle:
            current.append(event)
            continue
        flush()
        current = [event]
    flush()
    return cycles


def signals_by_symbol(signals: Iterable[FinancialRiskSignal]) -> dict[str, list[FinancialRiskSignal]]:
    grouped: dict[str, list[FinancialRiskSignal]] = defaultdict(list)
    for signal in signals:
        grouped[signal.ts_code].append(signal)
    for rows in grouped.values():
        rows.sort(key=lambda row: (row.effective_trade_date, row.signal_id))
    return grouped


def cycles_by_symbol(cycles: Iterable[StCycle]) -> dict[str, list[StCycle]]:
    grouped: dict[str, list[StCycle]] = defaultdict(list)
    for cycle in cycles:
        grouped[cycle.ts_code].append(cycle)
    for rows in grouped.values():
        rows.sort(key=lambda row: (row.start_effective_trade_date, row.cycle_id))
    return grouped


def match_signals_to_cycles(
    cycles: Iterable[StCycle],
    signals: Iterable[FinancialRiskSignal],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> list[dict[str, Any]]:
    """Match prior financial risk signals to ST cycles without look-ahead."""

    signal_index = signals_by_symbol(signals)
    matches: list[dict[str, Any]] = []
    for cycle in cycles:
        prior_signals: list[FinancialRiskSignal] = []
        for signal in signal_index.get(cycle.ts_code, []):
            lead_days = (cycle.start_effective_trade_date - signal.effective_trade_date).days
            if lead_days <= 0:
                continue
            if lead_days <= lookback_days:
                prior_signals.append(signal)
        lead_values = [
            (cycle.start_effective_trade_date - signal.effective_trade_date).days
            for signal in prior_signals
        ]
        source_types = sorted({signal.source_type for signal in prior_signals})
        event_types = sorted({signal.event_type for signal in prior_signals})
        matches.append(
            {
                "cycle_id": cycle.cycle_id,
                "ts_code": cycle.ts_code,
                "cycle_start_effective_trade_date": cycle.start_effective_trade_date,
                "cycle_primary_event_type": cycle.primary_event_type,
                "cycle_event_types": list(cycle.event_types),
                "matched": bool(prior_signals),
                "matched_signal_count": len(prior_signals),
                "matched_source_types": source_types,
                "matched_event_types": event_types,
                "source_type_count": len(source_types),
                "earliest_signal_effective_trade_date": min((signal.effective_trade_date for signal in prior_signals), default=None),
                "latest_signal_effective_trade_date": max((signal.effective_trade_date for signal in prior_signals), default=None),
                "earliest_lead_days": max(lead_values) if lead_values else None,
                "latest_lead_days": min(lead_values) if lead_values else None,
            }
        )
    return matches


def _count_by_key(rows: Iterable[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        value = row.get(key)
        if isinstance(value, (list, tuple, set)):
            for item in value:
                counts[str(item)] += 1
        elif value is not None:
            counts[str(value)] += 1
    return dict(sorted(counts.items()))


def summarize_cycle_coverage(cycle_matches: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(cycle_matches)
    matched = [row for row in cycle_matches if row["matched"]]
    earliest_leads = [int(row["earliest_lead_days"]) for row in matched if row["earliest_lead_days"] is not None]
    latest_leads = [int(row["latest_lead_days"]) for row in matched if row["latest_lead_days"] is not None]
    source_coverage: dict[str, int] = defaultdict(int)
    combo_coverage: dict[str, int] = defaultdict(int)
    latest_lead_buckets: dict[str, int] = defaultdict(int)
    primary_event_rows: dict[str, dict[str, int]] = defaultdict(lambda: {"cycles": 0, "matched": 0})

    for row in cycle_matches:
        primary_event_rows[row["cycle_primary_event_type"]]["cycles"] += 1
        if row["matched"]:
            primary_event_rows[row["cycle_primary_event_type"]]["matched"] += 1
            combo_coverage[str(row["source_type_count"])] += 1
            if row["latest_lead_days"] is not None:
                latest_lead_buckets[_lead_bucket(int(row["latest_lead_days"]))] += 1
        for source_type in row["matched_source_types"]:
            source_coverage[source_type] += 1

    by_primary_event = []
    for event_type, stats in sorted(primary_event_rows.items()):
        by_primary_event.append(
            {
                "event_type": event_type,
                "cycles": stats["cycles"],
                "matched": stats["matched"],
                "coverage_rate": _rate(stats["matched"], stats["cycles"]),
            }
        )

    return {
        "cycles": total,
        "matched_cycles": len(matched),
        "coverage_rate": _rate(len(matched), total),
        "median_earliest_lead_days": _median(earliest_leads),
        "median_latest_lead_days": _median(latest_leads),
        "mean_latest_lead_days": _mean(latest_leads),
        "source_coverage": dict(sorted(source_coverage.items())),
        "source_combo_coverage": dict(sorted(combo_coverage.items(), key=lambda item: int(item[0]))),
        "latest_lead_buckets": {name: latest_lead_buckets.get(name, 0) for name, _, _ in LEAD_BUCKETS},
        "by_primary_event": by_primary_event,
    }


def _combo_context(
    signal: FinancialRiskSignal,
    symbol_signals: Sequence[FinancialRiskSignal],
    *,
    combo_window_days: int,
) -> dict[str, Any]:
    start_date = signal.effective_trade_date - dt.timedelta(days=combo_window_days)
    matched = [
        row
        for row in symbol_signals
        if start_date <= row.effective_trade_date <= signal.effective_trade_date
    ]
    sources = sorted({row.source_type for row in matched})
    events = sorted({row.event_type for row in matched})
    return {
        "combo_source_count": len(sources),
        "combo_source_key": "+".join(sources),
        "combo_event_count": len(events),
        "combo_event_key": "+".join(events),
    }


def build_precision_rows(
    signals: Iterable[FinancialRiskSignal],
    cycles: Iterable[StCycle],
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    study_start: Optional[dt.date] = None,
    study_end: Optional[dt.date] = None,
    combo_window_days: int = DEFAULT_COMBO_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    """Build per-signal future-ST labels with horizon censoring."""

    cycle_index = cycles_by_symbol(cycles)
    signal_index = signals_by_symbol(signals)
    rows: list[dict[str, Any]] = []
    for signal in sorted(signals, key=lambda row: (row.ts_code, row.effective_trade_date, row.signal_id)):
        if study_start and signal.effective_trade_date < study_start:
            continue
        if study_end and signal.effective_trade_date > study_end:
            continue

        next_cycle: Optional[StCycle] = None
        for cycle in cycle_index.get(signal.ts_code, []):
            if cycle.start_effective_trade_date > signal.effective_trade_date:
                next_cycle = cycle
                break
        days_to_next = (
            (next_cycle.start_effective_trade_date - signal.effective_trade_date).days
            if next_cycle is not None
            else None
        )
        combo_context = _combo_context(
            signal,
            signal_index.get(signal.ts_code, []),
            combo_window_days=combo_window_days,
        )
        row: dict[str, Any] = {
            "signal_id": signal.signal_id,
            "ts_code": signal.ts_code,
            "source_type": signal.source_type,
            "event_type": signal.event_type,
            "risk_level": signal.risk_level,
            "action": signal.action,
            "signal_year": signal.effective_trade_date.year,
            "metric_bucket": signal.metric_bucket,
            "metric_detail": signal.metric_detail or {},
            "report_period": signal.report_period,
            "source_event_date": signal.source_event_date,
            "effective_trade_date": signal.effective_trade_date,
            "combo_source_count": combo_context["combo_source_count"],
            "combo_source_key": combo_context["combo_source_key"],
            "combo_event_count": combo_context["combo_event_count"],
            "combo_event_key": combo_context["combo_event_key"],
            "next_cycle_id": next_cycle.cycle_id if next_cycle else None,
            "next_cycle_primary_event_type": next_cycle.primary_event_type if next_cycle else None,
            "days_to_next_st_cycle": days_to_next,
        }
        for horizon in horizons:
            eligible = study_end is None or _date_plus_days(signal.effective_trade_date, horizon) <= study_end
            row[f"eligible_{horizon}d"] = eligible
            row[f"hit_{horizon}d"] = bool(eligible and days_to_next is not None and days_to_next <= horizon)
        rows.append(row)
    return rows


def aggregate_precision_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
    horizons: Sequence[int] = DEFAULT_HORIZONS,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field) for field in group_fields)].append(row)

    aggregates: list[dict[str, Any]] = []
    for group_key, group_rows in sorted(grouped.items(), key=lambda item: tuple(str(part) for part in item[0])):
        payload = {field: group_key[idx] for idx, field in enumerate(group_fields)}
        payload["signals"] = len(group_rows)
        payload["distinct_symbols"] = len({row["ts_code"] for row in group_rows})
        hit_distances = [
            int(row["days_to_next_st_cycle"])
            for row in group_rows
            if row.get("days_to_next_st_cycle") is not None
        ]
        payload["median_days_to_next_st_cycle_any"] = _median(hit_distances)
        for horizon in horizons:
            eligible = [row for row in group_rows if row.get(f"eligible_{horizon}d")]
            hits = [row for row in eligible if row.get(f"hit_{horizon}d")]
            payload[f"eligible_{horizon}d"] = len(eligible)
            payload[f"hits_{horizon}d"] = len(hits)
            payload[f"precision_{horizon}d"] = _rate(len(hits), len(eligible))
        aggregates.append(payload)
    return aggregates


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


def _trading_day_index(trading_days: Sequence[dt.date]) -> dict[dt.date, int]:
    return {day: idx for idx, day in enumerate(trading_days)}


def required_signal_return_price_keys(
    precision_rows: Iterable[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
    *,
    return_windows: Sequence[int] = DEFAULT_RETURN_WINDOWS,
) -> set[tuple[str, dt.date]]:
    day_index = _trading_day_index(trading_days)
    keys: set[tuple[str, dt.date]] = set()
    for row in precision_rows:
        ts_code = str(row["ts_code"])
        event_idx = day_index.get(row["effective_trade_date"])
        if event_idx is None:
            continue
        for idx in (event_idx - 1, event_idx):
            if 0 <= idx < len(trading_days):
                keys.add((ts_code, trading_days[idx]))
        for window in return_windows:
            target_idx = event_idx + int(window)
            if 0 <= target_idx < len(trading_days):
                keys.add((ts_code, trading_days[target_idx]))
    return keys


def required_cycle_return_price_keys(
    cycle_matches: Iterable[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
) -> set[tuple[str, dt.date]]:
    day_index = _trading_day_index(trading_days)
    keys: set[tuple[str, dt.date]] = set()
    for row in cycle_matches:
        if not row.get("matched"):
            continue
        ts_code = str(row["ts_code"])
        cycle_idx = day_index.get(row["cycle_start_effective_trade_date"])
        if cycle_idx is not None and cycle_idx > 0:
            keys.add((ts_code, trading_days[cycle_idx - 1]))
        for date_key in ("earliest_signal_effective_trade_date", "latest_signal_effective_trade_date"):
            signal_date = row.get(date_key)
            signal_idx = day_index.get(signal_date)
            if signal_idx is not None:
                keys.add((ts_code, trading_days[signal_idx]))
    return keys


def load_close_prices_for_keys(conn: Any, keys: set[tuple[str, dt.date]]) -> dict[tuple[str, dt.date], float]:
    if not keys:
        return {}
    ordered = sorted(keys)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_early_financial_distress_price_keys")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_early_financial_distress_price_keys (
                ts_code TEXT NOT NULL,
                trade_date DATE NOT NULL
            )
            """
        )
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO tmp_early_financial_distress_price_keys (ts_code, trade_date) VALUES %s",
            ordered,
            page_size=10000,
        )
        cur.execute(
            """
            CREATE INDEX tmp_early_financial_distress_price_keys_idx
                ON tmp_early_financial_distress_price_keys(ts_code, trade_date)
            """
        )
        cur.execute(
            """
            SELECT k.ts_code, k.trade_date, k.close_li
              FROM market.kline_daily_raw k
              JOIN tmp_early_financial_distress_price_keys t
                ON t.ts_code = k.ts_code
               AND t.trade_date = k.trade_date
            """
        )
        prices: dict[tuple[str, dt.date], float] = {}
        for ts_code, trade_date, close_li in cur.fetchall():
            close_value = _safe_float(close_li)
            if close_value is not None:
                prices[(str(ts_code), trade_date)] = close_value / PRICE_UNIT_DIVISOR
        cur.execute("DROP TABLE IF EXISTS tmp_early_financial_distress_price_keys")
    return prices


def required_market_cap_keys(precision_rows: Iterable[Mapping[str, Any]]) -> set[tuple[str, dt.date]]:
    return {
        (str(row["ts_code"]), row["effective_trade_date"])
        for row in precision_rows
        if row.get("effective_trade_date") is not None
    }


def load_market_caps_for_keys(conn: Any, keys: set[tuple[str, dt.date]]) -> dict[tuple[str, dt.date], float]:
    if not keys:
        return {}
    ordered = sorted(keys)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_early_financial_distress_cap_keys")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_early_financial_distress_cap_keys (
                ts_code TEXT NOT NULL,
                trade_date DATE NOT NULL
            )
            """
        )
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO tmp_early_financial_distress_cap_keys (ts_code, trade_date) VALUES %s",
            ordered,
            page_size=10000,
        )
        cur.execute(
            """
            CREATE INDEX tmp_early_financial_distress_cap_keys_idx
                ON tmp_early_financial_distress_cap_keys(ts_code, trade_date)
            """
        )
        cur.execute(
            """
            SELECT d.ts_code, d.trade_date, COALESCE(d.circ_mv, d.total_mv) AS market_cap_wan
              FROM market.daily_basic d
              JOIN tmp_early_financial_distress_cap_keys t
                ON t.ts_code = d.ts_code
               AND t.trade_date = d.trade_date
            """
        )
        market_caps: dict[tuple[str, dt.date], float] = {}
        for ts_code, trade_date, market_cap_wan in cur.fetchall():
            value = _safe_float(market_cap_wan)
            if value is not None and value > 0:
                market_caps[(str(ts_code), trade_date)] = value
        cur.execute("DROP TABLE IF EXISTS tmp_early_financial_distress_cap_keys")
    return market_caps


def load_industries_for_keys(conn: Any, keys: set[tuple[str, dt.date]]) -> dict[tuple[str, dt.date], dict[str, str]]:
    if not keys:
        return {}
    ordered = sorted(keys)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_early_financial_distress_industry_keys")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_early_financial_distress_industry_keys (
                ts_code TEXT NOT NULL,
                trade_date DATE NOT NULL
            )
            """
        )
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO tmp_early_financial_distress_industry_keys (ts_code, trade_date) VALUES %s",
            ordered,
            page_size=10000,
        )
        cur.execute(
            """
            CREATE INDEX tmp_early_financial_distress_industry_keys_idx
                ON tmp_early_financial_distress_industry_keys(ts_code, trade_date)
            """
        )
        cur.execute(
            """
            SELECT t.ts_code, t.trade_date, b.industry AS pit_industry, s.industry AS static_industry
              FROM tmp_early_financial_distress_industry_keys t
              LEFT JOIN market.bak_basic b
                ON b.ts_code = t.ts_code
               AND b.trade_date = t.trade_date
              LEFT JOIN market.stock_basic s
                ON s.ts_code = t.ts_code
            """
        )
        industries: dict[tuple[str, dt.date], dict[str, str]] = {}
        for ts_code, trade_date, pit_industry, static_industry in cur.fetchall():
            industry = str(pit_industry or static_industry or "industry_unknown")
            source = "bak_basic" if pit_industry else ("stock_basic" if static_industry else "unknown")
            industries[(str(ts_code), trade_date)] = {"industry": industry, "industry_source": source}
        cur.execute("DROP TABLE IF EXISTS tmp_early_financial_distress_industry_keys")
    return industries


def _market_cap_bucket(market_cap_wan: Optional[float]) -> str:
    if market_cap_wan is None:
        return "mv_unknown"
    if market_cap_wan < 500000:
        return "mv_lt_5bn_yuan"
    if market_cap_wan < 1000000:
        return "mv_5bn_to_10bn_yuan"
    if market_cap_wan < 3000000:
        return "mv_10bn_to_30bn_yuan"
    if market_cap_wan < 10000000:
        return "mv_30bn_to_100bn_yuan"
    return "mv_ge_100bn_yuan"


def _loss_to_market_cap(row: Mapping[str, Any], market_cap_wan: Optional[float]) -> Optional[float]:
    if market_cap_wan is None or market_cap_wan <= 0:
        return None
    detail = row.get("metric_detail") if isinstance(row.get("metric_detail"), Mapping) else {}
    loss_wan: Optional[float] = None
    if row.get("event_type") == "financial_forecast_loss":
        loss_wan = _safe_float(detail.get("worst_loss_wan"))
    elif row.get("event_type") == "financial_express_loss":
        net_profit_yuan = _safe_float(detail.get("net_profit_yuan"))
        loss_wan = net_profit_yuan / 10000.0 if net_profit_yuan is not None else None
    if loss_wan is None or loss_wan >= 0:
        return None
    return abs(loss_wan) / market_cap_wan


def _loss_to_market_cap_bucket(value: Optional[float]) -> str:
    if value is None:
        return "loss_mv_unknown"
    if value >= 1.0:
        return "loss_ge_100pct_mv"
    if value >= 0.5:
        return "loss_50pct_to_100pct_mv"
    if value >= 0.2:
        return "loss_20pct_to_50pct_mv"
    if value >= 0.1:
        return "loss_10pct_to_20pct_mv"
    if value >= 0.05:
        return "loss_5pct_to_10pct_mv"
    return "loss_lt_5pct_mv"


def enrich_precision_rows_with_market_cap(
    precision_rows: Iterable[Mapping[str, Any]],
    market_caps: Mapping[tuple[str, dt.date], float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in precision_rows:
        payload = dict(row)
        market_cap_wan = market_caps.get((str(row["ts_code"]), row["effective_trade_date"]))
        loss_to_mv = _loss_to_market_cap(row, market_cap_wan)
        payload["market_cap_wan"] = market_cap_wan
        payload["market_cap_bucket"] = _market_cap_bucket(market_cap_wan)
        payload["loss_to_market_cap"] = loss_to_mv
        payload["loss_to_market_cap_bucket"] = _loss_to_market_cap_bucket(loss_to_mv)
        rows.append(payload)
    return rows


def enrich_precision_rows_with_industry(
    precision_rows: Iterable[Mapping[str, Any]],
    industries: Mapping[tuple[str, dt.date], Mapping[str, str]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in precision_rows:
        payload = dict(row)
        industry_info = industries.get((str(row["ts_code"]), row["effective_trade_date"]), {})
        payload["industry"] = industry_info.get("industry", "industry_unknown")
        payload["industry_source"] = industry_info.get("industry_source", "unknown")
        rows.append(payload)
    return rows


def _previous_standard_report_period(value: dt.date) -> dt.date:
    periods = ((3, 31), (6, 30), (9, 30), (12, 31))
    current = (value.month, value.day)
    try:
        idx = periods.index(current)
    except ValueError:
        earlier = [period for period in periods if period < current]
        month, day = earlier[-1] if earlier else periods[-1]
        year = value.year if earlier else value.year - 1
        return dt.date(year, month, day)
    if idx == 0:
        return dt.date(value.year - 1, 12, 31)
    month, day = periods[idx - 1]
    return dt.date(value.year, month, day)


def _loss_report_count_bucket(value: int) -> str:
    if value <= 0:
        return "loss_reports_0"
    if value == 1:
        return "loss_reports_1"
    if value == 2:
        return "loss_reports_2"
    if value == 3:
        return "loss_reports_3"
    return "loss_reports_ge_4"


def enrich_precision_rows_with_loss_history(
    precision_rows: Iterable[Mapping[str, Any]],
    *,
    lookback_days: int = 730,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in precision_rows]
    loss_periods_by_symbol: dict[str, set[dt.date]] = defaultdict(set)
    loss_events = {"financial_forecast_loss", "financial_express_loss"}
    for row in rows:
        report_period = row.get("report_period")
        if row.get("event_type") in loss_events and isinstance(report_period, dt.date):
            loss_periods_by_symbol[str(row["ts_code"])].add(report_period)

    for row in rows:
        ts_code = str(row["ts_code"])
        report_period = row.get("report_period")
        periods = loss_periods_by_symbol.get(ts_code, set())
        prior_anchor = report_period if isinstance(report_period, dt.date) else row.get("effective_trade_date")
        if isinstance(prior_anchor, dt.date):
            prior_lower_bound = prior_anchor - dt.timedelta(days=lookback_days)
            prior_rolling_count = sum(1 for period in periods if prior_lower_bound <= period <= prior_anchor)
        else:
            prior_rolling_count = 0
        if row.get("event_type") not in loss_events or not isinstance(report_period, dt.date):
            strict_streak = 0
            rolling_count = 0
        else:
            strict_streak = 1
            cursor = _previous_standard_report_period(report_period)
            while cursor in periods:
                strict_streak += 1
                cursor = _previous_standard_report_period(cursor)
            lower_bound = report_period - dt.timedelta(days=lookback_days)
            rolling_count = sum(1 for period in periods if lower_bound <= period <= report_period)

        row["loss_report_streak"] = strict_streak
        row["loss_report_streak_bucket"] = _loss_report_count_bucket(strict_streak)
        row["loss_report_count_730d"] = rolling_count
        row["loss_report_count_730d_bucket"] = _loss_report_count_bucket(rolling_count)
        row["prior_loss_report_count_730d"] = prior_rolling_count
        row["prior_loss_report_count_730d_bucket"] = _loss_report_count_bucket(prior_rolling_count)
    return rows


def build_signal_return_rows(
    precision_rows: Iterable[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
    close_prices: Mapping[tuple[str, dt.date], float],
    *,
    return_windows: Sequence[int] = DEFAULT_RETURN_WINDOWS,
) -> list[dict[str, Any]]:
    day_index = _trading_day_index(trading_days)
    rows: list[dict[str, Any]] = []
    for source in precision_rows:
        event_idx = day_index.get(source["effective_trade_date"])
        if event_idx is None:
            continue
        ts_code = str(source["ts_code"])
        prev_day = trading_days[event_idx - 1] if event_idx > 0 else None
        event_day = trading_days[event_idx]
        prev_close = close_prices.get((ts_code, prev_day)) if prev_day else None
        event_close = close_prices.get((ts_code, event_day))
        for window in return_windows:
            target_idx = event_idx + int(window)
            if not (0 <= target_idx < len(trading_days)):
                eligible = False
                target_day = None
                target_close = None
            else:
                eligible = True
                target_day = trading_days[target_idx]
                target_close = close_prices.get((ts_code, target_day))
            rows.append(
                {
                    "signal_id": source["signal_id"],
                    "ts_code": ts_code,
                    "source_type": source["source_type"],
                    "event_type": source["event_type"],
                    "risk_level": source["risk_level"],
                    "action": source["action"],
                    "signal_year": source["signal_year"],
                    "metric_bucket": source.get("metric_bucket"),
                    "effective_trade_date": source["effective_trade_date"],
                    "combo_source_count": source["combo_source_count"],
                    "combo_source_key": source.get("combo_source_key"),
                    "combo_event_count": source.get("combo_event_count"),
                    "combo_event_key": source.get("combo_event_key"),
                    "hit_90d": source.get("hit_90d"),
                    "hit_180d": source.get("hit_180d"),
                    "hit_365d": source.get("hit_365d"),
                    "days_to_next_st_cycle": source.get("days_to_next_st_cycle"),
                    "window": int(window),
                    "window_name": "T0" if int(window) == 0 else f"T0_T+{int(window)}",
                    "target_trade_date": target_day,
                    "eligible_return_window": eligible,
                    "cumulative_return_from_prev_close": _return_from_close(prev_close, target_close),
                    "post_effective_return_from_t0_close": _return_from_close(event_close, target_close),
                    "missing_price": target_close is None or prev_close is None,
                }
            )
    return rows


def aggregate_return_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field) for field in group_fields)].append(row)

    aggregates: list[dict[str, Any]] = []
    for group_key, group_rows in sorted(grouped.items(), key=lambda item: tuple(str(part) for part in item[0])):
        payload = {field: group_key[idx] for idx, field in enumerate(group_fields)}
        cumulative_values = [
            float(row["cumulative_return_from_prev_close"])
            for row in group_rows
            if row.get("cumulative_return_from_prev_close") is not None
        ]
        post_values = [
            float(row["post_effective_return_from_t0_close"])
            for row in group_rows
            if row.get("post_effective_return_from_t0_close") is not None
        ]
        payload.update(
            {
                "rows": len(group_rows),
                "valid_cumulative_returns": len(cumulative_values),
                "mean_cumulative_return": _mean(cumulative_values),
                "median_cumulative_return": _median(cumulative_values),
                "p10_cumulative_return": _percentile(cumulative_values, 0.10),
                "p90_cumulative_return": _percentile(cumulative_values, 0.90),
                "negative_cumulative_return_rate": _rate(sum(1 for value in cumulative_values if value < 0), len(cumulative_values)),
                "mean_post_effective_return": _mean(post_values),
                "median_post_effective_return": _median(post_values),
                "missing_price_rate": _rate(sum(1 for row in group_rows if row.get("missing_price")), len(group_rows)),
            }
        )
        aggregates.append(payload)
    return aggregates


def build_cycle_pre_st_return_rows(
    cycle_matches: Iterable[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
    close_prices: Mapping[tuple[str, dt.date], float],
) -> list[dict[str, Any]]:
    day_index = _trading_day_index(trading_days)
    rows: list[dict[str, Any]] = []
    for source in cycle_matches:
        if not source.get("matched"):
            continue
        cycle_idx = day_index.get(source["cycle_start_effective_trade_date"])
        if cycle_idx is None or cycle_idx <= 0:
            continue
        ts_code = str(source["ts_code"])
        pre_st_day = trading_days[cycle_idx - 1]
        pre_st_close = close_prices.get((ts_code, pre_st_day))
        for signal_kind, date_key in (
            ("earliest", "earliest_signal_effective_trade_date"),
            ("latest", "latest_signal_effective_trade_date"),
        ):
            signal_date = source.get(date_key)
            signal_close = close_prices.get((ts_code, signal_date)) if signal_date else None
            rows.append(
                {
                    "cycle_id": source["cycle_id"],
                    "ts_code": ts_code,
                    "cycle_primary_event_type": source["cycle_primary_event_type"],
                    "source_type_count": source["source_type_count"],
                    "signal_kind": signal_kind,
                    "signal_effective_trade_date": signal_date,
                    "cycle_start_effective_trade_date": source["cycle_start_effective_trade_date"],
                    "pre_st_trade_date": pre_st_day,
                    "lead_days": source.get(f"{signal_kind}_lead_days"),
                    "signal_to_pre_st_return": _return_from_close(signal_close, pre_st_close),
                    "missing_price": signal_close is None or pre_st_close is None,
                }
            )
    return rows


def aggregate_cycle_return_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field) for field in group_fields)].append(row)

    aggregates: list[dict[str, Any]] = []
    for group_key, group_rows in sorted(grouped.items(), key=lambda item: tuple(str(part) for part in item[0])):
        payload = {field: group_key[idx] for idx, field in enumerate(group_fields)}
        values = [float(row["signal_to_pre_st_return"]) for row in group_rows if row.get("signal_to_pre_st_return") is not None]
        payload.update(
            {
                "rows": len(group_rows),
                "valid_returns": len(values),
                "mean_signal_to_pre_st_return": _mean(values),
                "median_signal_to_pre_st_return": _median(values),
                "p10_signal_to_pre_st_return": _percentile(values, 0.10),
                "p90_signal_to_pre_st_return": _percentile(values, 0.90),
                "negative_return_rate": _rate(sum(1 for value in values if value < 0), len(values)),
                "missing_price_rate": _rate(sum(1 for row in group_rows if row.get("missing_price")), len(group_rows)),
            }
        )
        aggregates.append(payload)
    return aggregates


def build_candidate_rules(
    precision_by_event: Sequence[Mapping[str, Any]],
    precision_by_combo: Sequence[Mapping[str, Any]],
    precision_by_source_key: Sequence[Mapping[str, Any]],
    precision_by_metric_bucket: Sequence[Mapping[str, Any]],
    returns_by_event_window: Sequence[Mapping[str, Any]],
    precision_by_metric_bucket_combo_source_key: Sequence[Mapping[str, Any]] = (),
    precision_by_loss_to_market_cap: Sequence[Mapping[str, Any]] = (),
    precision_by_event_loss_to_market_cap: Sequence[Mapping[str, Any]] = (),
    precision_by_loss_to_market_cap_market_cap: Sequence[Mapping[str, Any]] = (),
    precision_by_event_loss_report_count: Sequence[Mapping[str, Any]] = (),
    precision_by_loss_to_market_cap_loss_report_count: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    return_index = {
        (row.get("event_type"), row.get("window")): row
        for row in returns_by_event_window
    }
    candidates: list[dict[str, Any]] = []
    for row in precision_by_event:
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        event_type = str(row.get("event_type"))
        ret20 = return_index.get((event_type, 20), {})
        mean20 = ret20.get("mean_cumulative_return")
        reasons: list[str] = []
        if precision_365 is not None and precision_365 >= 0.10:
            action = "research_score_down_candidate"
            reasons.append("precision_365d_at_least_10pct")
        elif precision_180 is not None and precision_180 >= 0.05:
            action = "warning_high_candidate"
            reasons.append("precision_180d_at_least_5pct")
        else:
            action = "warning_only"
            reasons.append("precision_not_enough_for_overlay")
        if mean20 is not None and mean20 < -0.02:
            reasons.append("mean_T0_T20_return_below_minus_2pct")
        candidates.append(
            {
                "candidate_type": "event_type",
                "key": event_type,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "mean_T0_T20_return": mean20,
                "recommended_next_step": action,
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": reasons,
            }
        )
    for row in precision_by_combo:
        combo_count = row.get("combo_source_count")
        precision_365 = row.get("precision_365d")
        if combo_count is None or int(combo_count) < 2:
            continue
        candidates.append(
            {
                "candidate_type": "combo_source_count",
                "key": f"trailing_source_count>={combo_count}",
                "signals": row.get("signals"),
                "precision_180d": row.get("precision_180d"),
                "precision_365d": precision_365,
                "recommended_next_step": "research_combo_threshold" if precision_365 is not None and precision_365 >= 0.08 else "warning_only",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["multi_source_confirmation_research_only"],
            }
        )
    for row in precision_by_source_key:
        key = str(row.get("combo_source_key") or "")
        if "+" not in key or int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        if precision_365 is None or precision_365 < 0.08:
            continue
        candidates.append(
            {
                "candidate_type": "combo_source_key",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": row.get("precision_180d"),
                "precision_365d": precision_365,
                "recommended_next_step": "research_combo_source_key",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["specific_source_combo_lift_research_only"],
            }
        )
    for row in precision_by_metric_bucket:
        if int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        key = f"{row.get('event_type')}|{row.get('metric_bucket')}"
        candidates.append(
            {
                "candidate_type": "metric_bucket",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_metric_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["metric_bucket_precision_lift_research_only"],
            }
        )
    for row in precision_by_metric_bucket_combo_source_key:
        if int(row.get("signals") or 0) < 100:
            continue
        source_key = str(row.get("combo_source_key") or "")
        if "+" not in source_key:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        key = f"{row.get('event_type')}|{row.get('metric_bucket')}|{source_key}"
        candidates.append(
            {
                "candidate_type": "metric_bucket_combo_source_key",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_metric_combo_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["metric_bucket_and_source_combo_research_only"],
            }
        )
    for row in precision_by_loss_to_market_cap:
        key = str(row.get("loss_to_market_cap_bucket") or "")
        if key == "loss_mv_unknown" or int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        candidates.append(
            {
                "candidate_type": "loss_to_market_cap_bucket",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_relative_loss_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["relative_loss_to_market_cap_research_only"],
            }
        )
    for row in precision_by_event_loss_to_market_cap:
        bucket = str(row.get("loss_to_market_cap_bucket") or "")
        if bucket == "loss_mv_unknown" or int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        key = f"{row.get('event_type')}|{bucket}"
        candidates.append(
            {
                "candidate_type": "event_loss_to_market_cap_bucket",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_event_relative_loss_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["event_relative_loss_to_market_cap_research_only"],
            }
        )
    for row in precision_by_loss_to_market_cap_market_cap:
        loss_bucket = str(row.get("loss_to_market_cap_bucket") or "")
        market_cap_bucket = str(row.get("market_cap_bucket") or "")
        if loss_bucket == "loss_mv_unknown" or int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        key = f"{loss_bucket}|{market_cap_bucket}"
        candidates.append(
            {
                "candidate_type": "loss_to_market_cap_market_cap_bucket",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_size_specific_relative_loss_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["relative_loss_and_size_bucket_research_only"],
            }
        )
    for row in precision_by_event_loss_report_count:
        count_bucket = str(row.get("loss_report_count_730d_bucket") or "")
        if count_bucket in {"loss_reports_0", "loss_reports_1"} or int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        key = f"{row.get('event_type')}|{count_bucket}"
        candidates.append(
            {
                "candidate_type": "event_loss_report_count_730d_bucket",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_consecutive_loss_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["loss_report_count_research_only"],
            }
        )
    for row in precision_by_loss_to_market_cap_loss_report_count:
        loss_bucket = str(row.get("loss_to_market_cap_bucket") or "")
        count_bucket = str(row.get("loss_report_count_730d_bucket") or "")
        if loss_bucket == "loss_mv_unknown" or count_bucket in {"loss_reports_0", "loss_reports_1"}:
            continue
        if int(row.get("signals") or 0) < 100:
            continue
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        if not ((precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10)):
            continue
        key = f"{loss_bucket}|{count_bucket}"
        candidates.append(
            {
                "candidate_type": "loss_to_market_cap_loss_report_count_730d_bucket",
                "key": key,
                "signals": row.get("signals"),
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "recommended_next_step": "research_relative_and_consecutive_loss_threshold",
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": ["relative_loss_and_loss_history_research_only"],
            }
        )
    return candidates


def _combo_threshold_from_key(key: str) -> Optional[int]:
    marker = "trailing_source_count>="
    if not key.startswith(marker):
        return None
    try:
        return int(key[len(marker) :])
    except ValueError:
        return None


def _candidate_rule_applies(row: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    candidate_type = str(candidate.get("candidate_type") or "")
    key = str(candidate.get("key") or "")
    if candidate_type == "event_type":
        return str(row.get("event_type")) == key
    if candidate_type == "combo_source_count":
        threshold = _combo_threshold_from_key(key)
        return threshold is not None and int(row.get("combo_source_count") or 0) >= threshold
    if candidate_type == "combo_source_key":
        return str(row.get("combo_source_key") or "") == key
    if candidate_type == "metric_bucket":
        return f"{row.get('event_type')}|{row.get('metric_bucket')}" == key
    if candidate_type == "metric_bucket_combo_source_key":
        return f"{row.get('event_type')}|{row.get('metric_bucket')}|{row.get('combo_source_key')}" == key
    if candidate_type == "loss_to_market_cap_bucket":
        return str(row.get("loss_to_market_cap_bucket") or "") == key
    if candidate_type == "event_loss_to_market_cap_bucket":
        return f"{row.get('event_type')}|{row.get('loss_to_market_cap_bucket')}" == key
    if candidate_type == "loss_to_market_cap_market_cap_bucket":
        return f"{row.get('loss_to_market_cap_bucket')}|{row.get('market_cap_bucket')}" == key
    if candidate_type == "event_loss_report_count_730d_bucket":
        return f"{row.get('event_type')}|{row.get('loss_report_count_730d_bucket')}" == key
    if candidate_type == "loss_to_market_cap_loss_report_count_730d_bucket":
        return f"{row.get('loss_to_market_cap_bucket')}|{row.get('loss_report_count_730d_bucket')}" == key
    return False


def build_candidate_precision_rows(
    precision_rows: Iterable[Mapping[str, Any]],
    candidate_rules: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Duplicate per-signal rows for each research candidate rule that matches."""

    rows: list[dict[str, Any]] = []
    for source in precision_rows:
        for candidate in candidate_rules:
            if not _candidate_rule_applies(source, candidate):
                continue
            payload = dict(source)
            payload.update(
                {
                    "candidate_type": candidate.get("candidate_type"),
                    "candidate_key": candidate.get("key"),
                    "recommended_next_step": candidate.get("recommended_next_step"),
                }
            )
            rows.append(payload)
    return rows


def build_candidate_return_rows(
    signal_return_rows: Iterable[Mapping[str, Any]],
    candidate_precision_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    signal_candidates: dict[int, list[tuple[str, str, str]]] = defaultdict(list)
    seen: set[tuple[int, str, str]] = set()
    for row in candidate_precision_rows:
        signal_id = int(row["signal_id"])
        candidate_type = str(row.get("candidate_type") or "")
        candidate_key = str(row.get("candidate_key") or "")
        identity = (signal_id, candidate_type, candidate_key)
        if identity in seen:
            continue
        seen.add(identity)
        signal_candidates[signal_id].append(
            (candidate_type, candidate_key, str(row.get("recommended_next_step") or ""))
        )

    rows: list[dict[str, Any]] = []
    for source in signal_return_rows:
        for candidate_type, candidate_key, recommended_next_step in signal_candidates.get(int(source["signal_id"]), []):
            payload = dict(source)
            payload.update(
                {
                    "candidate_type": candidate_type,
                    "candidate_key": candidate_key,
                    "recommended_next_step": recommended_next_step,
                }
            )
            rows.append(payload)
    return rows


def build_candidate_stability_decisions(
    precision_overall: Sequence[Mapping[str, Any]],
    precision_yearly: Sequence[Mapping[str, Any]],
    return_yearly: Sequence[Mapping[str, Any]],
    *,
    min_eligible_365: int = 100,
    min_yearly_eligible_365: int = 30,
    min_stable_years: int = 4,
) -> list[dict[str, Any]]:
    yearly_precision: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in precision_yearly:
        key = (str(row.get("candidate_type") or ""), str(row.get("candidate_key") or ""))
        if int(row.get("eligible_365d") or 0) >= min_yearly_eligible_365:
            yearly_precision[key].append(row)

    yearly_returns: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in return_yearly:
        if row.get("window") not in (20, 60):
            continue
        key = (
            str(row.get("candidate_type") or ""),
            str(row.get("candidate_key") or ""),
            int(row.get("window") or 0),
        )
        yearly_returns[key].append(row)

    decisions: list[dict[str, Any]] = []
    for row in precision_overall:
        candidate_type = str(row.get("candidate_type") or "")
        candidate_key = str(row.get("candidate_key") or "")
        identity = (candidate_type, candidate_key)
        precision_365 = row.get("precision_365d")
        precision_180 = row.get("precision_180d")
        eligible_365 = int(row.get("eligible_365d") or 0)
        year_rows = yearly_precision.get(identity, [])
        year_precisions = [
            float(year_row["precision_365d"])
            for year_row in year_rows
            if year_row.get("precision_365d") is not None
        ]
        median_year_precision = _median(year_precisions)
        min_year_precision = min(year_precisions) if year_precisions else None
        negative_return_years: dict[str, int] = {}
        valid_return_years: dict[str, int] = {}
        for window in (20, 60):
            window_rows = [
                return_row
                for return_row in yearly_returns.get((candidate_type, candidate_key, window), [])
                if int(return_row.get("valid_cumulative_returns") or 0) >= min_yearly_eligible_365
            ]
            valid_return_years[f"T0_T+{window}"] = len(window_rows)
            negative_return_years[f"T0_T+{window}"] = sum(
                1
                for return_row in window_rows
                if (
                    return_row.get("median_cumulative_return") is not None
                    and float(return_row["median_cumulative_return"]) < 0
                )
                or (
                    return_row.get("negative_cumulative_return_rate") is not None
                    and float(return_row["negative_cumulative_return_rate"]) >= 0.52
                )
            )

        reason_codes: list[str] = []
        if eligible_365 < min_eligible_365:
            decision = "warning_only"
            reason_codes.append("sample_below_min_eligible_365")
        elif precision_365 is not None and precision_365 >= 0.15 and len(year_precisions) < min_stable_years:
            decision = "reject_for_instability"
            reason_codes.append("high_overall_precision_but_too_few_stable_years")
        elif (
            precision_365 is not None
            and precision_365 >= 0.15
            and median_year_precision is not None
            and median_year_precision >= 0.12
            and (
                negative_return_years["T0_T+20"] >= math.ceil(max(1, valid_return_years["T0_T+20"]) / 2)
                or negative_return_years["T0_T+60"] >= math.ceil(max(1, valid_return_years["T0_T+60"]) / 2)
            )
        ):
            decision = "qe_overlay_research_candidate"
            reason_codes.append("precision_and_return_stability_passed")
        elif (
            precision_365 is not None
            and precision_365 >= 0.10
            and (
                (median_year_precision is not None and median_year_precision >= 0.10)
                or negative_return_years["T0_T+20"] > 0
                or negative_return_years["T0_T+60"] > 0
            )
        ):
            decision = "needs_threshold_refinement"
            reason_codes.append("moderate_precision_or_return_stability_needs_refinement")
        elif (precision_365 is not None and precision_365 >= 0.15) or (precision_180 is not None and precision_180 >= 0.10):
            decision = "needs_threshold_refinement"
            reason_codes.append("precision_lift_without_return_stability")
        else:
            decision = "warning_only"
            reason_codes.append("precision_or_stability_not_enough")

        taxonomy_guards: list[str] = []
        if "financial_positive_but_miss_expectation" in candidate_key:
            taxonomy_guards.append("miss_expectation_definition_requires_rebuild")
        if "wan_unknown" in candidate_key:
            taxonomy_guards.append("loss_amount_missing_requires_data_quality_check")
        if "loss_lt_100m_yuan" in candidate_key:
            taxonomy_guards.append("small_loss_requires_market_cap_normalization")
        if taxonomy_guards and decision == "qe_overlay_research_candidate":
            decision = "needs_threshold_refinement"
            reason_codes.extend(taxonomy_guards)

        decisions.append(
            {
                "candidate_type": candidate_type,
                "candidate_key": candidate_key,
                "signals": row.get("signals"),
                "eligible_365d": eligible_365,
                "precision_180d": precision_180,
                "precision_365d": precision_365,
                "eligible_years_365d": len(year_precisions),
                "min_yearly_precision_365d": min_year_precision,
                "median_yearly_precision_365d": median_year_precision,
                "negative_return_years_T0_T20": negative_return_years["T0_T+20"],
                "valid_return_years_T0_T20": valid_return_years["T0_T+20"],
                "negative_return_years_T0_T60": negative_return_years["T0_T+60"],
                "valid_return_years_T0_T60": valid_return_years["T0_T+60"],
                "decision": decision,
                "hard_block_allowed": False,
                "force_exit_allowed": False,
                "alpha_boost_allowed": False,
                "reason_codes": reason_codes,
            }
        )
    return sorted(
        decisions,
        key=lambda row: (
            {
                "qe_overlay_research_candidate": 0,
                "needs_threshold_refinement": 1,
                "reject_for_instability": 2,
                "warning_only": 3,
            }.get(str(row.get("decision")), 9),
            -(float(row.get("precision_365d") or 0.0)),
            str(row.get("candidate_key") or ""),
        ),
    )


def summarize_candidate_stability(
    candidate_precision_rows: Sequence[Mapping[str, Any]],
    candidate_return_rows: Sequence[Mapping[str, Any]],
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
) -> dict[str, Any]:
    precision_overall = aggregate_precision_rows(
        candidate_precision_rows,
        group_fields=("candidate_type", "candidate_key", "recommended_next_step"),
        horizons=horizons,
    )
    precision_yearly = aggregate_precision_rows(
        candidate_precision_rows,
        group_fields=("candidate_type", "candidate_key", "signal_year"),
        horizons=horizons,
    )
    return_overall = aggregate_return_rows(
        candidate_return_rows,
        group_fields=("candidate_type", "candidate_key", "window", "window_name"),
    )
    return_yearly = aggregate_return_rows(
        candidate_return_rows,
        group_fields=("candidate_type", "candidate_key", "signal_year", "window", "window_name"),
    )
    return {
        "precision_overall": precision_overall,
        "precision_yearly": precision_yearly,
        "return_overall": return_overall,
        "return_yearly": return_yearly,
        "decisions": build_candidate_stability_decisions(
            precision_overall,
            precision_yearly,
            return_yearly,
        ),
    }


def load_financial_signals(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    event_types: Sequence[str] = FINANCIAL_RISK_EVENT_TYPES,
    source_types: Sequence[str] = FINANCIAL_SOURCE_TYPES,
    limit: Optional[int] = None,
) -> list[FinancialRiskSignal]:
    params: list[Any] = [rule_version, time_mode, list(source_types), list(event_types)]
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
            SELECT signal_id, ts_code, source_type, event_type, risk_level, action,
                   source_event_date, effective_trade_date, severity_score, confidence, evidence
              FROM market.event_signal
             WHERE rule_version = %s
               AND time_mode = %s
               AND source_type = ANY(%s)
               AND event_type = ANY(%s)
               AND signal_status = 'ACTIVE'
               {date_sql}
             ORDER BY ts_code, effective_trade_date, signal_id
             {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()
    signals: list[FinancialRiskSignal] = []
    for row in rows:
        evidence = row.get("evidence")
        metric_bucket, metric_detail = classify_metric_bucket(str(row["event_type"]), evidence)
        signals.append(
            FinancialRiskSignal(
                signal_id=int(row["signal_id"]),
                ts_code=str(row["ts_code"]),
                source_type=str(row["source_type"]),
                event_type=str(row["event_type"]),
                risk_level=str(row["risk_level"]),
                action=str(row["action"]),
                source_event_date=row["source_event_date"],
                effective_trade_date=row["effective_trade_date"],
                severity_score=_safe_float(row.get("severity_score")),
                confidence=_safe_float(row.get("confidence")),
                metric_bucket=metric_bucket,
                metric_detail=metric_detail,
                report_period=extract_report_period(evidence),
            )
        )
    return signals


def load_st_target_events(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    event_types: Sequence[str] = ST_TARGET_EVENT_TYPES,
) -> list[StTargetEvent]:
    params: list[Any] = [rule_version, time_mode, list(event_types)]
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
            SELECT signal_id, ts_code, event_type, source_event_date, effective_trade_date
              FROM market.event_signal
             WHERE rule_version = %s
               AND time_mode = %s
               AND event_type = ANY(%s)
               AND signal_status = 'ACTIVE'
               {date_sql}
             ORDER BY ts_code, effective_trade_date, signal_id
            """,
            params,
        )
        rows = cur.fetchall()
    return [
        StTargetEvent(
            signal_id=int(row["signal_id"]),
            ts_code=str(row["ts_code"]),
            event_type=str(row["event_type"]),
            source_event_date=row["source_event_date"],
            effective_trade_date=row["effective_trade_date"],
        )
        for row in rows
    ]


def load_data_snapshot(conn: Any) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    with conn.cursor() as cur:
        raw_tables = [
            "tushare_forecast_raw",
            "tushare_express_raw",
            "tushare_fina_indicator_raw",
        ]
        raw_rows = []
        for table in raw_tables:
            cur.execute(
                f"""
                SELECT COUNT(*), MIN(ann_date), MAX(ann_date),
                       MIN(report_period), MAX(report_period), COUNT(DISTINCT ts_code)
                  FROM market.{table}
                """
            )
            count, min_ann, max_ann, min_period, max_period, symbols = cur.fetchone()
            raw_rows.append(
                {
                    "table": f"market.{table}",
                    "rows": count,
                    "min_ann_date": min_ann,
                    "max_ann_date": max_ann,
                    "min_report_period": min_period,
                    "max_report_period": max_period,
                    "distinct_symbols": symbols,
                }
            )
        snapshot["raw_tables"] = raw_rows
        cur.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM market.kline_daily_raw")
        min_kline, max_kline, kline_rows = cur.fetchone()
        snapshot["kline_daily_raw"] = {
            "min_trade_date": min_kline,
            "max_trade_date": max_kline,
            "rows": kline_rows,
        }
        cur.execute("SELECT MIN(cal_date), MAX(cal_date), COUNT(*) FROM market.trading_calendar WHERE is_trading = TRUE")
        min_cal, max_cal, calendar_rows = cur.fetchone()
        snapshot["trading_calendar_open_days"] = {
            "min_cal_date": min_cal,
            "max_cal_date": max_cal,
            "rows": calendar_rows,
            "note": "Calendar can include future template dates; do not treat max_cal_date as latest market data date.",
        }
    return snapshot


def write_outputs(
    *,
    output_dir: Path,
    report_id: str,
    payload: dict[str, Any],
    cycle_matches: list[dict[str, Any]],
    precision_rows: list[dict[str, Any]],
    write_details: bool,
) -> ResearchSummary:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    csv_path = output_dir / f"{report_id}_precision_details.csv" if write_details else None

    json_path.write_text(_json_dumps(payload), encoding="utf-8")
    if csv_path is not None:
        fieldnames = [
            "signal_id",
            "ts_code",
            "source_type",
            "event_type",
            "risk_level",
            "action",
            "signal_year",
            "source_event_date",
            "effective_trade_date",
            "combo_source_count",
            "next_cycle_id",
            "next_cycle_primary_event_type",
            "days_to_next_st_cycle",
        ]
        for horizon in payload["parameters"]["horizons"]:
            fieldnames.extend([f"eligible_{horizon}d", f"hit_{horizon}d"])
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(precision_rows)

    lines = [
        "# Early Financial Distress Research Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Financial rule version: `{payload['parameters']['financial_rule_version']}`",
        f"- ST rule version: `{payload['parameters']['st_rule_version']}`",
        f"- Time mode: `{payload['parameters']['time_mode']}`",
        f"- Study window: `{payload['parameters']['start_date']}` to `{payload['parameters']['end_date']}`",
        f"- Financial risk signals loaded: `{payload['counts']['financial_signals_loaded']}`",
        f"- Financial risk signals in study window: `{payload['counts']['financial_signals_in_study_window']}`",
        f"- ST target events: `{payload['counts']['st_events']}`",
        f"- ST target cycles: `{payload['counts']['st_cycles']}`",
        f"- Details CSV written: `{bool(csv_path)}`",
        "",
        "## Cycle Coverage",
        "",
        "| metric | value |",
        "| --- | ---: |",
    ]
    coverage = payload["cycle_coverage"]
    for key in [
        "cycles",
        "matched_cycles",
        "coverage_rate",
        "median_earliest_lead_days",
        "median_latest_lead_days",
        "mean_latest_lead_days",
    ]:
        value = coverage.get(key)
        lines.append(f"| {key} | {round(value, 6) if isinstance(value, float) else value} |")

    lines.extend(["", "## Precision Overall", "", "| group | signals | eligible_90d | precision_90d | eligible_180d | precision_180d | eligible_365d | precision_365d |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for row in payload["precision"]["overall"]:
        lines.append(_precision_md_row("overall", row, payload["parameters"]["horizons"]))

    lines.extend(["", "## Precision By Source", "", "| source_type | signals | eligible_90d | precision_90d | eligible_180d | precision_180d | eligible_365d | precision_365d |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for row in payload["precision"]["by_source_type"]:
        lines.append(_precision_md_row(str(row["source_type"]), row, payload["parameters"]["horizons"]))

    lines.extend(["", "## Precision By Event Type", "", "| event_type | signals | eligible_90d | precision_90d | eligible_180d | precision_180d | eligible_365d | precision_365d |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for row in payload["precision"]["by_event_type"]:
        lines.append(_precision_md_row(str(row["event_type"]), row, payload["parameters"]["horizons"]))

    lines.extend(["", "## Precision By Market Cap Bucket", "", "| market_cap_bucket | signals | eligible_90d | precision_90d | eligible_180d | precision_180d | eligible_365d | precision_365d |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for row in payload["precision"].get("by_market_cap_bucket", []):
        lines.append(_precision_md_row(str(row["market_cap_bucket"]), row, payload["parameters"]["horizons"]))

    lines.extend(["", "## Return Study By Event Type", "", "| event_type | window | rows | valid | mean cumulative | median cumulative | negative rate |", "| --- | --- | ---: | ---: | ---: | ---: | ---: |"])
    for row in payload.get("returns", {}).get("by_event_type_window", []):
        if row.get("window") not in (0, 5, 20, 60):
            continue
        lines.append(_return_md_row(str(row.get("event_type")), row))

    lines.extend(["", "## Candidate Rules", "", "| candidate | key | signals | precision_180d | precision_365d | mean_T0_T20 | next_step |", "| --- | --- | ---: | ---: | ---: | ---: | --- |"])
    for row in payload.get("candidate_rules", []):
        lines.append(_candidate_md_row(row))

    lines.extend(
        [
            "",
            "## Candidate Stability Decisions",
            "",
            "| candidate | key | signals | precision_365d | eligible_years | median_year_precision_365d | neg_T20_years | neg_T60_years | decision |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in payload.get("candidate_stability", {}).get("decisions", []):
        lines.append(_candidate_stability_md_row(row))

    lines.extend(["", "## Research Boundary", ""])
    for key, value in payload["research_boundary"].items():
        lines.append(f"- `{key}`: `{value}`")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return ResearchSummary(
        report_id=report_id,
        output_json=str(json_path),
        output_md=str(md_path),
        output_csv=str(csv_path) if csv_path else None,
        financial_signals_loaded=payload["counts"]["financial_signals_loaded"],
        financial_signals_in_study_window=payload["counts"]["financial_signals_in_study_window"],
        st_events=payload["counts"]["st_events"],
        st_cycles=payload["counts"]["st_cycles"],
    )


def _precision_md_row(label: str, row: Mapping[str, Any], horizons: Sequence[int]) -> str:
    cells: list[Any] = [label, row.get("signals")]
    for horizon in horizons:
        cells.append(row.get(f"eligible_{horizon}d"))
        value = row.get(f"precision_{horizon}d")
        cells.append(round(value, 6) if isinstance(value, float) else value)
    return "| " + " | ".join(_md_cell(cell) for cell in cells) + " |"


def _return_md_row(label: str, row: Mapping[str, Any]) -> str:
    cells = [
        label,
        row.get("window_name"),
        row.get("rows"),
        row.get("valid_cumulative_returns"),
        row.get("mean_cumulative_return"),
        row.get("median_cumulative_return"),
        row.get("negative_cumulative_return_rate"),
    ]
    formatted = [round(cell, 6) if isinstance(cell, float) else cell for cell in cells]
    return "| " + " | ".join(_md_cell(cell) for cell in formatted) + " |"


def _candidate_md_row(row: Mapping[str, Any]) -> str:
    cells = [
        row.get("candidate_type"),
        row.get("key"),
        row.get("signals"),
        row.get("precision_180d"),
        row.get("precision_365d"),
        row.get("mean_T0_T20_return"),
        row.get("recommended_next_step"),
    ]
    formatted = [round(cell, 6) if isinstance(cell, float) else cell for cell in cells]
    return "| " + " | ".join(_md_cell(cell) for cell in formatted) + " |"


def _candidate_stability_md_row(row: Mapping[str, Any]) -> str:
    cells = [
        row.get("candidate_type"),
        row.get("candidate_key"),
        row.get("signals"),
        row.get("precision_365d"),
        row.get("eligible_years_365d"),
        row.get("median_yearly_precision_365d"),
        row.get("negative_return_years_T0_T20"),
        row.get("negative_return_years_T0_T60"),
        row.get("decision"),
    ]
    formatted = [round(cell, 6) if isinstance(cell, float) else cell for cell in cells]
    return "| " + " | ".join(_md_cell(cell) for cell in formatted) + " |"


def _md_cell(value: Any) -> str:
    return str(value).replace("|", r"\|")


def run_research(
    *,
    financial_rule_version: str = FINANCIAL_RULE_VERSION,
    st_rule_version: str = ST_RULE_VERSION,
    time_mode: str = DEFAULT_TIME_MODE,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    cycle_gap_days: int = DEFAULT_CYCLE_GAP_DAYS,
    combo_window_days: int = DEFAULT_COMBO_WINDOW_DAYS,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    return_windows: Sequence[int] = DEFAULT_RETURN_WINDOWS,
    include_return_study: bool = True,
    output_dir: Path = Path("reports/event_signal/early_financial_distress"),
    write_details: bool = False,
    limit: Optional[int] = None,
) -> ResearchSummary:
    signal_load_start = _date_minus_days(start_date, lookback_days)
    with get_conn() as conn:
        data_snapshot = load_data_snapshot(conn)
        financial_signals = load_financial_signals(
            conn,
            rule_version=financial_rule_version,
            time_mode=time_mode,
            start_date=signal_load_start,
            end_date=end_date,
            limit=limit,
        )
        st_events = load_st_target_events(
            conn,
            rule_version=st_rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
        )

    if not financial_signals:
        raise RuntimeError("no financial risk event_signal rows found for requested research scope")
    if not st_events:
        raise RuntimeError("no ST target event_signal rows found for requested research scope")

    cycles = build_st_cycles(st_events, cycle_gap_days=cycle_gap_days)
    cycle_matches = match_signals_to_cycles(cycles, financial_signals, lookback_days=lookback_days)
    precision_rows = build_precision_rows(
        financial_signals,
        cycles,
        horizons=horizons,
        study_start=start_date,
        study_end=end_date,
        combo_window_days=combo_window_days,
    )
    market_cap_keys = required_market_cap_keys(precision_rows)
    with get_conn() as conn:
        market_caps = load_market_caps_for_keys(conn, market_cap_keys)
        industries = load_industries_for_keys(conn, market_cap_keys)
    precision_rows = enrich_precision_rows_with_market_cap(precision_rows, market_caps)
    precision_rows = enrich_precision_rows_with_industry(precision_rows, industries)
    precision_rows = enrich_precision_rows_with_loss_history(precision_rows)

    financial_signals_in_window = [
        row
        for row in financial_signals
        if (start_date is None or row.effective_trade_date >= start_date)
        and (end_date is None or row.effective_trade_date <= end_date)
    ]

    signal_return_rows: list[dict[str, Any]] = []
    cycle_pre_st_return_rows: list[dict[str, Any]] = []
    if include_return_study and precision_rows:
        kline_max = data_snapshot["kline_daily_raw"].get("max_trade_date")
        return_start_candidates = [row.effective_trade_date for row in financial_signals_in_window]
        return_start_candidates.extend(event.effective_trade_date for event in st_events)
        if return_start_candidates and kline_max is not None:
            trading_start = min(return_start_candidates) - dt.timedelta(days=10)
            trading_end = min(kline_max, end_date) if end_date is not None else kline_max
            with get_conn() as conn:
                trading_days = load_trading_days(conn, trading_start, trading_end)
                price_keys = required_signal_return_price_keys(
                    precision_rows,
                    trading_days,
                    return_windows=return_windows,
                )
                price_keys.update(required_cycle_return_price_keys(cycle_matches, trading_days))
                close_prices = load_close_prices_for_keys(conn, price_keys)
            signal_return_rows = build_signal_return_rows(
                precision_rows,
                trading_days,
                close_prices,
                return_windows=return_windows,
            )
            cycle_pre_st_return_rows = build_cycle_pre_st_return_rows(cycle_matches, trading_days, close_prices)

    precision_overall = aggregate_precision_rows(precision_rows, group_fields=(), horizons=horizons)
    precision_by_source = aggregate_precision_rows(precision_rows, group_fields=("source_type",), horizons=horizons)
    precision_by_event = aggregate_precision_rows(precision_rows, group_fields=("event_type",), horizons=horizons)
    precision_by_combo = aggregate_precision_rows(precision_rows, group_fields=("combo_source_count",), horizons=horizons)
    precision_by_source_key = aggregate_precision_rows(precision_rows, group_fields=("combo_source_key",), horizons=horizons)
    precision_by_metric_bucket = aggregate_precision_rows(precision_rows, group_fields=("event_type", "metric_bucket"), horizons=horizons)
    precision_by_metric_bucket_source_key = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "metric_bucket", "combo_source_key"),
        horizons=horizons,
    )
    precision_by_market_cap = aggregate_precision_rows(precision_rows, group_fields=("market_cap_bucket",), horizons=horizons)
    precision_by_event_market_cap = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "market_cap_bucket"),
        horizons=horizons,
    )
    precision_by_metric_market_cap = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "metric_bucket", "market_cap_bucket"),
        horizons=horizons,
    )
    precision_by_loss_to_market_cap = aggregate_precision_rows(
        precision_rows,
        group_fields=("loss_to_market_cap_bucket",),
        horizons=horizons,
    )
    precision_by_event_loss_to_market_cap = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "loss_to_market_cap_bucket"),
        horizons=horizons,
    )
    precision_by_metric_loss_to_market_cap = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "metric_bucket", "loss_to_market_cap_bucket"),
        horizons=horizons,
    )
    precision_by_loss_to_market_cap_market_cap = aggregate_precision_rows(
        precision_rows,
        group_fields=("loss_to_market_cap_bucket", "market_cap_bucket"),
        horizons=horizons,
    )
    precision_by_industry = aggregate_precision_rows(
        precision_rows,
        group_fields=("industry",),
        horizons=horizons,
    )
    precision_by_event_industry = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "industry"),
        horizons=horizons,
    )
    precision_by_loss_to_market_cap_industry = aggregate_precision_rows(
        precision_rows,
        group_fields=("loss_to_market_cap_bucket", "industry"),
        horizons=horizons,
    )
    precision_by_loss_report_count = aggregate_precision_rows(
        precision_rows,
        group_fields=("loss_report_count_730d_bucket",),
        horizons=horizons,
    )
    precision_by_event_loss_report_count = aggregate_precision_rows(
        precision_rows,
        group_fields=("event_type", "loss_report_count_730d_bucket"),
        horizons=horizons,
    )
    precision_by_loss_to_market_cap_loss_report_count = aggregate_precision_rows(
        precision_rows,
        group_fields=("loss_to_market_cap_bucket", "loss_report_count_730d_bucket"),
        horizons=horizons,
    )
    precision_by_year = aggregate_precision_rows(precision_rows, group_fields=("signal_year",), horizons=horizons)
    return_by_event_window = aggregate_return_rows(signal_return_rows, group_fields=("event_type", "window", "window_name"))
    candidate_rules = build_candidate_rules(
        precision_by_event,
        precision_by_combo,
        precision_by_source_key,
        precision_by_metric_bucket,
        return_by_event_window,
        precision_by_metric_bucket_source_key,
        precision_by_loss_to_market_cap,
        precision_by_event_loss_to_market_cap,
        precision_by_loss_to_market_cap_market_cap,
        precision_by_event_loss_report_count,
        precision_by_loss_to_market_cap_loss_report_count,
    )
    candidate_precision_rows = build_candidate_precision_rows(precision_rows, candidate_rules)
    candidate_return_rows = build_candidate_return_rows(signal_return_rows, candidate_precision_rows)
    candidate_stability = summarize_candidate_stability(
        candidate_precision_rows,
        candidate_return_rows,
        horizons=horizons,
    )

    report_id = "early_financial_distress_{}_{}_{}".format(
        start_date.isoformat() if start_date else "all",
        end_date.isoformat() if end_date else "all",
        dt.datetime.now().strftime("%Y%m%d_%H%M%S"),
    ).replace("-", "")
    payload = {
        "report_id": report_id,
        "parameters": {
            "financial_rule_version": financial_rule_version,
            "st_rule_version": st_rule_version,
            "time_mode": time_mode,
            "start_date": start_date,
            "end_date": end_date,
            "lookback_days": lookback_days,
            "cycle_gap_days": cycle_gap_days,
            "combo_window_days": combo_window_days,
            "horizons": list(horizons),
            "return_windows": list(return_windows),
            "include_return_study": include_return_study,
            "financial_risk_event_types": list(FINANCIAL_RISK_EVENT_TYPES),
            "st_target_event_types": list(ST_TARGET_EVENT_TYPES),
        },
        "counts": {
            "financial_signals_loaded": len(financial_signals),
            "financial_signals_in_study_window": len(financial_signals_in_window),
            "st_events": len(st_events),
            "st_cycles": len(cycles),
            "st_symbols": len({event.ts_code for event in st_events}),
            "financial_signal_symbols": len({signal.ts_code for signal in financial_signals_in_window}),
        },
        "data_snapshot": data_snapshot,
        "cycle_coverage": summarize_cycle_coverage(cycle_matches),
        "precision": {
            "overall": precision_overall,
            "by_source_type": precision_by_source,
            "by_event_type": precision_by_event,
            "by_combo_source_count": precision_by_combo,
            "by_combo_source_key": precision_by_source_key,
            "by_metric_bucket": precision_by_metric_bucket,
            "by_metric_bucket_combo_source_key": precision_by_metric_bucket_source_key,
            "by_market_cap_bucket": precision_by_market_cap,
            "by_event_type_market_cap_bucket": precision_by_event_market_cap,
            "by_metric_bucket_market_cap_bucket": precision_by_metric_market_cap,
            "by_loss_to_market_cap_bucket": precision_by_loss_to_market_cap,
            "by_event_type_loss_to_market_cap_bucket": precision_by_event_loss_to_market_cap,
            "by_metric_bucket_loss_to_market_cap_bucket": precision_by_metric_loss_to_market_cap,
            "by_loss_to_market_cap_market_cap_bucket": precision_by_loss_to_market_cap_market_cap,
            "by_industry": precision_by_industry,
            "by_event_type_industry": precision_by_event_industry,
            "by_loss_to_market_cap_industry": precision_by_loss_to_market_cap_industry,
            "by_loss_report_count_730d_bucket": precision_by_loss_report_count,
            "by_event_type_loss_report_count_730d_bucket": precision_by_event_loss_report_count,
            "by_loss_to_market_cap_loss_report_count_730d_bucket": precision_by_loss_to_market_cap_loss_report_count,
            "by_signal_year": precision_by_year,
        },
        "market_cap_enrichment": {
            "requested_keys": len(market_cap_keys),
            "matched_keys": len(market_caps),
            "matched_rate": _rate(len(market_caps), len(market_cap_keys)),
            "unit": "Tushare daily_basic circ_mv first, else total_mv; unit is 10k CNY.",
        },
        "industry_enrichment": {
            "requested_keys": len(market_cap_keys),
            "matched_keys": sum(1 for value in industries.values() if value.get("industry") != "industry_unknown"),
            "matched_rate": _rate(sum(1 for value in industries.values() if value.get("industry") != "industry_unknown"), len(market_cap_keys)),
            "primary_source": "market.bak_basic exact trade_date industry, fallback market.stock_basic static industry.",
        },
        "returns": {
            "by_event_type_window": return_by_event_window,
            "by_source_type_window": aggregate_return_rows(signal_return_rows, group_fields=("source_type", "window", "window_name")),
            "by_combo_source_count_window": aggregate_return_rows(signal_return_rows, group_fields=("combo_source_count", "window", "window_name")),
            "by_combo_source_key_window": aggregate_return_rows(signal_return_rows, group_fields=("combo_source_key", "window", "window_name")),
            "by_metric_bucket_window": aggregate_return_rows(signal_return_rows, group_fields=("event_type", "metric_bucket", "window", "window_name")),
            "by_hit_365d_window": aggregate_return_rows(signal_return_rows, group_fields=("hit_365d", "window", "window_name")),
            "cycle_signal_to_pre_st_by_signal_kind": aggregate_cycle_return_rows(cycle_pre_st_return_rows, group_fields=("signal_kind",)),
            "cycle_signal_to_pre_st_by_primary_event": aggregate_cycle_return_rows(cycle_pre_st_return_rows, group_fields=("cycle_primary_event_type", "signal_kind")),
        },
        "candidate_rules": candidate_rules,
        "candidate_stability": candidate_stability,
        "cycle_matches_sample": cycle_matches[:20],
        "research_boundary": {
            "writes_db": False,
            "trading_consumption_enabled": False,
            "hard_block_enabled": False,
            "force_exit_enabled": False,
            "alpha_overlay_enabled": False,
            "llm_enabled": False,
            "pdf_download_enabled": False,
        },
    }

    return write_outputs(
        output_dir=output_dir,
        report_id=report_id,
        payload=payload,
        cycle_matches=cycle_matches,
        precision_rows=precision_rows,
        write_details=write_details,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only early financial distress research")
    parser.add_argument("--financial-rule-version", default=FINANCIAL_RULE_VERSION)
    parser.add_argument("--st-rule-version", default=ST_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default=DEFAULT_TIME_MODE)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--cycle-gap-days", type=int, default=DEFAULT_CYCLE_GAP_DAYS)
    parser.add_argument("--combo-window-days", type=int, default=DEFAULT_COMBO_WINDOW_DAYS)
    parser.add_argument("--horizon", type=int, action="append", default=None)
    parser.add_argument("--return-window", type=int, action="append", default=None)
    parser.add_argument("--disable-return-study", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output-dir", default="reports/event_signal/early_financial_distress")
    parser.add_argument("--write-details", action="store_true")
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    summary = run_research(
        financial_rule_version=args.financial_rule_version,
        st_rule_version=args.st_rule_version,
        time_mode=args.time_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        lookback_days=args.lookback_days,
        cycle_gap_days=args.cycle_gap_days,
        combo_window_days=args.combo_window_days,
        horizons=tuple(args.horizon) if args.horizon else DEFAULT_HORIZONS,
        return_windows=tuple(args.return_window) if args.return_window else DEFAULT_RETURN_WINDOWS,
        include_return_study=not args.disable_return_study,
        output_dir=Path(args.output_dir),
        write_details=args.write_details,
        limit=args.limit,
    )
    print(_json_dumps(asdict(summary)))


if __name__ == "__main__":
    main()
