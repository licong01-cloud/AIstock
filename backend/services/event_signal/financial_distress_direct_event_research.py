"""Direct event-return research for financial-distress candidates.

This module is research-only. It reads existing event-signal and daily price
tables, computes direct post-event returns, and writes ignored report artifacts.
It does not write database rows and does not connect to QE/Paper/Selection/QMT
runtime paths.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.event_signal.early_financial_distress_research import (
    DEFAULT_RETURN_WINDOWS,
    load_close_prices_for_keys,
    load_trading_days,
    required_signal_return_price_keys,
)
from backend.services.event_signal.financial_event_study import DEFAULT_BENCHMARK, load_index_close
from backend.services.event_signal.financial_distress_qe_overlay_research import (
    FIRST_BATCH_RULES,
    LOSS_HISTORY_RULES,
    MID_LARGE_EVENT_RULES,
    PHASE24_RESEARCH_RULES,
    PHASE25_RESEARCH_RULES,
    PHASE30_RESEARCH_RULES,
    PHASE31_RESEARCH_RULES,
    REFINEMENT_RULES,
    SIZE_BUCKET_RULES,
    FinancialDistressRule,
    _display_label,
    _fixed_width_table,
    _parse_date,
    _pct,
    _rule_applies,
    load_enriched_financial_rows,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RULE_KEYS = ("indicator_large_decline_mv_10_30bn",)
DEFAULT_RETURN_WINDOWS_PHASE11 = (0, 1, 5, 10, 20, 60)
REPORT_VERSION = "financial_distress_direct_event_research_v1_20260509"
ALL_DIRECT_EVENT_RULES = (
    FIRST_BATCH_RULES
    + SIZE_BUCKET_RULES
    + LOSS_HISTORY_RULES
    + MID_LARGE_EVENT_RULES
    + REFINEMENT_RULES
    + PHASE24_RESEARCH_RULES
    + PHASE25_RESEARCH_RULES
    + PHASE30_RESEARCH_RULES
    + PHASE31_RESEARCH_RULES
)


@dataclass(frozen=True)
class DirectEventResearchSummary:
    report_id: str
    output_json: str
    output_md: str
    date_from: dt.date
    date_to: dt.date
    rules: int
    events: int
    return_rows: int


def _json_dumps(value: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=indent)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _mean(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _median(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


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
    return float(ordered[lo] * (1.0 - weight) + ordered[hi] * weight)


def _rate(numerator: int, denominator: int) -> Optional[float]:
    return numerator / denominator if denominator else None


def _sort_group_key(group_key: tuple[Any, ...]) -> tuple[tuple[int, Any], ...]:
    sortable: list[tuple[int, Any]] = []
    for value in group_key:
        if value is None:
            sortable.append((3, ""))
        elif isinstance(value, (dt.date, dt.datetime)):
            sortable.append((0, value.isoformat()))
        elif isinstance(value, (int, float)):
            sortable.append((1, float(value)))
        else:
            sortable.append((2, str(value)))
    return tuple(sortable)


def _return_from_close(base_close: Optional[float], target_close: Optional[float]) -> Optional[float]:
    if base_close is None or target_close is None or base_close <= 0:
        return None
    return target_close / base_close - 1.0


def select_rules(rule_keys: Sequence[str]) -> tuple[FinancialDistressRule, ...]:
    by_key = {rule.rule_key: rule for rule in ALL_DIRECT_EVENT_RULES}
    missing = [key for key in rule_keys if key not in by_key]
    if missing:
        raise ValueError(f"unsupported direct event research rule_key values: {missing}")
    return tuple(by_key[key] for key in rule_keys)


def build_direct_event_rows(
    financial_rows: Sequence[Mapping[str, Any]],
    rules: Sequence[FinancialDistressRule],
    *,
    date_from: dt.date,
    date_to: dt.date,
) -> list[dict[str, Any]]:
    event_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for row in financial_rows:
        effective_trade_date = row.get("effective_trade_date")
        if not isinstance(effective_trade_date, dt.date):
            continue
        if effective_trade_date < date_from or effective_trade_date > date_to:
            continue
        for rule in rules:
            if not _rule_applies(row, rule):
                continue
            key = (rule.rule_key, int(row["signal_id"]))
            if key in seen:
                continue
            seen.add(key)
            payload = dict(row)
            payload["rule_key"] = rule.rule_key
            payload["rule_title"] = rule.title
            event_rows.append(payload)
    return sorted(event_rows, key=lambda item: (item["rule_key"], item["effective_trade_date"], item["ts_code"], item["signal_id"]))


def build_direct_return_rows(
    event_rows: Sequence[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
    close_prices: Mapping[tuple[str, dt.date], float],
    *,
    return_windows: Sequence[int],
    benchmark: str = DEFAULT_BENCHMARK,
    index_close: Optional[Mapping[dt.date, float]] = None,
) -> list[dict[str, Any]]:
    day_index = {day: idx for idx, day in enumerate(trading_days)}
    index_close = index_close or {}
    rows: list[dict[str, Any]] = []
    for event in event_rows:
        event_date = event["effective_trade_date"]
        event_idx = day_index.get(event_date)
        if event_idx is None:
            continue
        ts_code = str(event["ts_code"])
        prev_day = trading_days[event_idx - 1] if event_idx > 0 else None
        prev_close = close_prices.get((ts_code, prev_day)) if prev_day else None
        event_close = close_prices.get((ts_code, event_date))
        prev_index_close = index_close.get(prev_day) if prev_day else None
        event_index_close = index_close.get(event_date)
        for window in return_windows:
            target_idx = event_idx + int(window)
            target_day = trading_days[target_idx] if 0 <= target_idx < len(trading_days) else None
            target_close = close_prices.get((ts_code, target_day)) if target_day else None
            target_index_close = index_close.get(target_day) if target_day else None
            cumulative_return = _return_from_close(prev_close, target_close)
            post_effective_return = _return_from_close(event_close, target_close)
            cumulative_benchmark_return = _return_from_close(prev_index_close, target_index_close)
            post_effective_benchmark_return = _return_from_close(event_index_close, target_index_close)
            rows.append(
                {
                    "rule_key": event["rule_key"],
                    "rule_title": event["rule_title"],
                    "signal_id": event["signal_id"],
                    "ts_code": ts_code,
                    "event_type": event.get("event_type"),
                    "source_type": event.get("source_type"),
                    "effective_trade_date": event_date,
                    "signal_year": event.get("signal_year"),
                    "industry": event.get("industry") or "industry_unknown",
                    "market_cap_bucket": event.get("market_cap_bucket") or "mv_unknown",
                    "prior_loss_report_count_730d_bucket": event.get("prior_loss_report_count_730d_bucket") or "loss_reports_0",
                    "window": int(window),
                    "window_name": "T0" if int(window) == 0 else f"T0_T+{int(window)}",
                    "target_trade_date": target_day,
                    "benchmark": benchmark,
                    "cumulative_return_from_prev_close": cumulative_return,
                    "post_effective_return_from_t0_close": post_effective_return,
                    "cumulative_benchmark_return_from_prev_close": cumulative_benchmark_return,
                    "post_effective_benchmark_return_from_t0_close": post_effective_benchmark_return,
                    "cumulative_abnormal_return_from_prev_close": (
                        cumulative_return - cumulative_benchmark_return
                        if cumulative_return is not None and cumulative_benchmark_return is not None
                        else None
                    ),
                    "post_effective_abnormal_return_from_t0_close": (
                        post_effective_return - post_effective_benchmark_return
                        if post_effective_return is not None and post_effective_benchmark_return is not None
                        else None
                    ),
                    "missing_price": prev_close is None or event_close is None or target_close is None,
                    "missing_benchmark": prev_index_close is None or event_index_close is None or target_index_close is None,
                }
            )
    return rows


def aggregate_return_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
    return_field: str = "post_effective_return_from_t0_close",
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field) for field in group_fields)].append(row)

    aggregates: list[dict[str, Any]] = []
    for group_key, group_rows in sorted(grouped.items(), key=lambda item: _sort_group_key(item[0])):
        values = [_safe_float(row.get(return_field)) for row in group_rows]
        valid = [value for value in values if value is not None]
        payload = {field: group_key[idx] for idx, field in enumerate(group_fields)}
        payload.update(
            {
                "rows": len(group_rows),
                "valid_returns": len(valid),
                "mean_return": _mean(valid),
                "median_return": _median(valid),
                "p10_return": _percentile(valid, 0.10),
                "p90_return": _percentile(valid, 0.90),
                "negative_return_rate": _rate(sum(1 for value in valid if value < 0), len(valid)),
                "positive_return_rate": _rate(sum(1 for value in valid if value > 0), len(valid)),
                "missing_price_rate": _rate(sum(1 for row in group_rows if row.get("missing_price")), len(group_rows)),
                "missing_benchmark_rate": _rate(sum(1 for row in group_rows if row.get("missing_benchmark")), len(group_rows)),
            }
        )
        aggregates.append(payload)
    return aggregates


def top_industry_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    window: int,
    limit: int = 20,
    return_field: str = "post_effective_return_from_t0_close",
) -> list[dict[str, Any]]:
    target_rows = [row for row in rows if int(row.get("window") or -1) == int(window)]
    aggregates = aggregate_return_rows(target_rows, group_fields=("rule_key", "industry", "window"), return_field=return_field)
    aggregates.sort(key=lambda row: (str(row.get("rule_key")), -int(row.get("valid_returns") or 0), str(row.get("industry"))))
    output: list[dict[str, Any]] = []
    per_rule: dict[str, int] = defaultdict(int)
    for row in aggregates:
        rule_key = str(row.get("rule_key"))
        if per_rule[rule_key] >= limit:
            continue
        per_rule[rule_key] += 1
        output.append(row)
    return output


def count_event_rows(rows: Iterable[Mapping[str, Any]], *, group_fields: Sequence[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], set[int]] = defaultdict(set)
    for row in rows:
        grouped[tuple(row.get(field) for field in group_fields)].add(int(row["signal_id"]))
    output: list[dict[str, Any]] = []
    for group_key, signal_ids in sorted(grouped.items(), key=lambda item: _sort_group_key(item[0])):
        payload = {field: group_key[idx] for idx, field in enumerate(group_fields)}
        payload["events"] = len(signal_ids)
        output.append(payload)
    return output


def _price_keys_for_returns(
    event_rows: Sequence[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
    *,
    return_windows: Sequence[int],
) -> set[tuple[str, dt.date]]:
    precision_like_rows: list[dict[str, Any]] = []
    for row in event_rows:
        precision_like_rows.append(
            {
                "ts_code": row["ts_code"],
                "effective_trade_date": row["effective_trade_date"],
            }
        )
    return required_signal_return_price_keys(precision_like_rows, trading_days, return_windows=return_windows)


def build_payload(
    *,
    date_from: dt.date,
    date_to: dt.date,
    return_windows: Sequence[int],
    rules: Sequence[FinancialDistressRule],
    event_rows: Sequence[Mapping[str, Any]],
    return_rows: Sequence[Mapping[str, Any]],
    benchmark: str = DEFAULT_BENCHMARK,
) -> dict[str, Any]:
    return {
        "report_version": REPORT_VERSION,
        "parameters": {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "return_windows": list(return_windows),
            "rule_keys": [rule.rule_key for rule in rules],
            "industry_policy": "explanatory_only_no_neutralization",
            "capital_assumption": "about_10m_cny_no_market_impact_filter",
            "benchmark": benchmark,
        },
        "event_count": len(event_rows),
        "return_row_count": len(return_rows),
        "events_by_rule": count_event_rows(event_rows, group_fields=("rule_key",)),
        "returns_by_rule_window": aggregate_return_rows(return_rows, group_fields=("rule_key", "window", "window_name")),
        "returns_by_rule_window_abnormal": aggregate_return_rows(
            return_rows,
            group_fields=("rule_key", "window", "window_name"),
            return_field="post_effective_abnormal_return_from_t0_close",
        ),
        "returns_by_rule_market_cap_window": aggregate_return_rows(
            return_rows,
            group_fields=("rule_key", "market_cap_bucket", "window", "window_name"),
        ),
        "returns_by_rule_year_window": aggregate_return_rows(return_rows, group_fields=("rule_key", "signal_year", "window", "window_name")),
        "returns_by_rule_industry_window_top20": top_industry_rows(return_rows, window=max(return_windows), limit=20),
        "returns_by_rule_industry_window_top20_abnormal": top_industry_rows(
            return_rows,
            window=max(return_windows),
            limit=20,
            return_field="post_effective_abnormal_return_from_t0_close",
        ),
    }


def _fmt_number(value: Any) -> str:
    if value is None:
        return "NA"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def write_markdown(path: Path, payload: Mapping[str, Any]) -> None:
    params = payload["parameters"]
    abnormal_by_key = {
        (row["rule_key"], row["window"]): row
        for row in payload.get("returns_by_rule_window_abnormal", [])
    }
    summary_rows = [
        [
            row["rule_key"],
            row["window_name"],
            row["valid_returns"],
            _pct(row.get("mean_return")),
            _pct(row.get("median_return")),
            abnormal_by_key.get((row["rule_key"], row["window"]), {}).get("valid_returns", 0),
            _pct(abnormal_by_key.get((row["rule_key"], row["window"]), {}).get("mean_return")),
            _pct(abnormal_by_key.get((row["rule_key"], row["window"]), {}).get("median_return")),
            _pct(row.get("negative_return_rate")),
            _pct(row.get("missing_price_rate")),
            _pct(row.get("missing_benchmark_rate")),
        ]
        for row in payload["returns_by_rule_window"]
        if int(row.get("window") or 0) in {1, 5, 20, 60}
    ]
    industry_rows = [
        [
            row["rule_key"],
            _display_label(row["industry"]),
            row["valid_returns"],
            _pct(row.get("mean_return")),
            _pct(row.get("median_return")),
            _pct(row.get("negative_return_rate")),
        ]
        for row in payload["returns_by_rule_industry_window_top20"]
    ]
    lines = [
        "# Financial Distress Direct Event Return Research",
        "",
        "Research-only direct event return study. Industry exposure is explanatory only; no industry neutralization is applied.",
        "",
        "## Scope",
        "",
        "```text",
        f"Date range     : {params['date_from']} -> {params['date_to']}",
        f"Return windows : {params['return_windows']}",
        f"Rules          : {params['rule_keys']}",
        f"Events         : {payload['event_count']}",
        f"Return rows    : {payload['return_row_count']}",
        f"Benchmark      : {params['benchmark']}",
        f"Industry policy: {params['industry_policy']}",
        "```",
        "",
        "## Rule/Window Returns",
        "",
        "```text",
        *_fixed_width_table(
            ["rule_key", "window", "valid", "mean", "median", "abn_valid", "abn_mean", "abn_median", "neg_rate", "miss_px", "miss_bm"],
            summary_rows,
        ),
        "```",
        "",
        "## Top Industry Attribution",
        "",
        "Rows are sorted by event count within each rule for the largest requested return window.",
        "",
        "```text",
        *_fixed_width_table(
            ["rule_key", "industry", "valid", "mean", "median", "neg_rate"],
            industry_rows,
        ),
        "```",
        "",
        "## Interpretation Rules",
        "",
        "- Negative direct post-event returns support risk-downweighting.",
        "- Positive direct returns do not automatically reject the signal because QE overlay effects can come from interaction with model-ranked Top50 candidates.",
        "- Industry concentration is not a neutralization target under the current 10m CNY/no-market-impact assumption.",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run_direct_event_research(
    *,
    output_dir: Path,
    date_from: dt.date,
    date_to: dt.date,
    rule_keys: Sequence[str] = DEFAULT_RULE_KEYS,
    return_windows: Sequence[int] = DEFAULT_RETURN_WINDOWS_PHASE11,
    benchmark: str = DEFAULT_BENCHMARK,
) -> DirectEventResearchSummary:
    rules = select_rules(rule_keys)
    max_window = max(int(window) for window in return_windows) if return_windows else max(DEFAULT_RETURN_WINDOWS)
    financial_rows, _ = load_enriched_financial_rows(
        date_from=date_from,
        date_to=date_to,
        active_trading_days=max(60, max_window),
    )
    event_rows = build_direct_event_rows(financial_rows, rules, date_from=date_from, date_to=date_to)

    trading_start = date_from - dt.timedelta(days=10)
    trading_end = date_to + dt.timedelta(days=max_window * 3 + 10)
    with get_conn() as conn:
        trading_days = load_trading_days(conn, trading_start, trading_end)
        price_keys = _price_keys_for_returns(event_rows, trading_days, return_windows=return_windows)
        close_prices = load_close_prices_for_keys(conn, price_keys)
        index_close = load_index_close(conn, benchmark, trading_start, trading_end)

    return_rows = build_direct_return_rows(
        event_rows,
        trading_days,
        close_prices,
        return_windows=return_windows,
        benchmark=benchmark,
        index_close=index_close,
    )
    payload = build_payload(
        date_from=date_from,
        date_to=date_to,
        return_windows=return_windows,
        rules=rules,
        event_rows=event_rows,
        return_rows=return_rows,
        benchmark=benchmark,
    )

    report_id = f"financial_distress_direct_event_{date_from:%Y%m%d}_{dt.datetime.now():%Y%m%d_%H%M%S}"
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(payload), encoding="utf-8")
    write_markdown(md_path, payload)
    return DirectEventResearchSummary(
        report_id=report_id,
        output_json=str(json_path),
        output_md=str(md_path),
        date_from=date_from,
        date_to=date_to,
        rules=len(rules),
        events=len(event_rows),
        return_rows=len(return_rows),
    )


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run direct event-return research for financial distress candidates")
    parser.add_argument("--output-dir", default="reports/event_signal/financial_distress_direct_event_returns")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--rule-key", action="append", default=None)
    parser.add_argument("--return-window", type=int, action="append", default=None)
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args(argv)
    summary = run_direct_event_research(
        output_dir=Path(args.output_dir),
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        rule_keys=tuple(args.rule_key or DEFAULT_RULE_KEYS),
        return_windows=tuple(args.return_window or DEFAULT_RETURN_WINDOWS_PHASE11),
        benchmark=args.benchmark,
    )
    print(_json_dumps(asdict(summary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
