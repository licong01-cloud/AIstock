"""Phase-32 direct-risk policy feasibility research.

This research-only script pivots from QE TopK replacement toward direct event-risk
policy feasibility. It studies whether structured financial-distress events can
support avoid-new-buy or soft downweight policies outside the alpha factor path.
It writes ignored JSON artifacts plus a curated Markdown report. It does not
write DB rows or connect signals to QE, Selection Center, Paper, QMT, or live paths.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.event_signal.early_financial_distress_research import (  # noqa: E402
    load_close_prices_for_keys,
    load_trading_days,
)
from backend.services.event_signal.financial_distress_direct_event_research import (  # noqa: E402
    _price_keys_for_returns,
    build_direct_event_rows,
    build_direct_return_rows,
    select_rules,
)
from backend.services.event_signal.financial_distress_qe_overlay_research import (  # noqa: E402
    PHASE24_RESEARCH_RULES,
    PHASE25_RESEARCH_RULES,
    PHASE30_RESEARCH_RULES,
    PHASE31_RESEARCH_RULES,
    _fixed_width_table,
    _parse_date,
    _pct,
    load_enriched_financial_rows,
)
from backend.services.event_signal.financial_event_study import DEFAULT_BENCHMARK, load_index_close  # noqa: E402


DEFAULT_OUTPUT_DIR = Path("reports/event_signal/financial_distress_phase32_direct_risk_policy_feasibility")
DEFAULT_DOC_PATH = Path("docs/analysis/event_signal_financial_distress_phase32_direct_risk_policy_feasibility_result_20260513.md")
DEFAULT_ROOT_ENV = Path("F:/Dev/AIstock/.env")
DEFAULT_DATE_FROM = "2024-07-01"
DEFAULT_DATE_TO = "2026-04-27"
DEFAULT_RETURN_WINDOWS = (1, 5, 20, 60, 120)
REPORT_VERSION = "financial_distress_phase32_direct_risk_policy_feasibility_v1_20260513"


@dataclass(frozen=True)
class Phase32Summary:
    output_json: str
    output_md: str
    rules: int
    events: int
    return_rows: int
    policy_candidates: int
    watchlist_rows: int


def default_rule_keys() -> tuple[str, ...]:
    """Use the latest structured financial-distress candidates without duplicates."""

    ordered: list[str] = []
    seen: set[str] = set()
    for rule in PHASE24_RESEARCH_RULES + PHASE25_RESEARCH_RULES + PHASE30_RESEARCH_RULES + PHASE31_RESEARCH_RULES:
        if rule.rule_key in seen:
            continue
        seen.add(rule.rule_key)
        ordered.append(rule.rule_key)
    return tuple(ordered)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


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


def _rate(count: int, denominator: int) -> Optional[float]:
    return count / denominator if denominator else None


def _compact_pct(value: Any) -> str:
    return _pct(_safe_float(value))


def _fmt_float(value: Any, digits: int = 1) -> str:
    number = _safe_float(value)
    if number is None:
        return "NA"
    return f"{number:.{digits}f}"


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def aggregate_return_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
    return_field: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field) for field in group_fields)].append(row)

    output: list[dict[str, Any]] = []
    for group_key, group_rows in sorted(grouped.items(), key=lambda item: tuple(str(value) for value in item[0])):
        values = [value for value in (_safe_float(row.get(return_field)) for row in group_rows) if value is not None]
        valid = len(values)
        payload = {field: group_key[idx] for idx, field in enumerate(group_fields)}
        payload.update(
            {
                "rows": len(group_rows),
                "valid_returns": valid,
                "mean_return": _mean(values),
                "median_return": _median(values),
                "p10_return": _percentile(values, 0.10),
                "p25_return": _percentile(values, 0.25),
                "p75_return": _percentile(values, 0.75),
                "negative_return_rate": _rate(sum(1 for value in values if value < 0), valid),
                "loss_5pct_rate": _rate(sum(1 for value in values if value <= -0.05), valid),
                "loss_10pct_rate": _rate(sum(1 for value in values if value <= -0.10), valid),
                "gain_5pct_rate": _rate(sum(1 for value in values if value >= 0.05), valid),
                "missing_price_rate": _rate(sum(1 for row in group_rows if row.get("missing_price")), len(group_rows)),
                "missing_benchmark_rate": _rate(sum(1 for row in group_rows if row.get("missing_benchmark")), len(group_rows)),
            }
        )
        output.append(payload)
    return output


def _window_risk_score(stats: Mapping[str, Any]) -> float:
    valid = int(stats.get("valid_returns") or 0)
    if valid <= 0:
        return -5.0

    median = _safe_float(stats.get("median_return"))
    negative_rate = _safe_float(stats.get("negative_return_rate"))
    p25 = _safe_float(stats.get("p25_return"))
    loss10_rate = _safe_float(stats.get("loss_10pct_rate"))
    missing_rate = _safe_float(stats.get("missing_price_rate")) or 0.0

    score = 0.0
    if median is not None:
        if median <= -0.04:
            score += 4.0
        elif median <= -0.025:
            score += 3.0
        elif median <= -0.01:
            score += 2.0
        elif median < 0:
            score += 1.0
        elif median >= 0.02:
            score -= 2.0

    if negative_rate is not None:
        if negative_rate >= 0.70:
            score += 3.0
        elif negative_rate >= 0.62:
            score += 2.0
        elif negative_rate >= 0.55:
            score += 1.0
        elif negative_rate < 0.48:
            score -= 1.5

    if p25 is not None:
        if p25 <= -0.10:
            score += 2.0
        elif p25 <= -0.06:
            score += 1.0

    if loss10_rate is not None and loss10_rate >= 0.20:
        score += 1.0

    if valid < 20:
        score -= 4.0
    elif valid < 40:
        score -= 1.0
    elif valid >= 100:
        score += 0.5

    if missing_rate >= 0.35:
        score -= 2.0
    elif missing_rate >= 0.20:
        score -= 1.0
    return score


def policy_decision(rule_key: str, window_stats: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    t5 = window_stats.get(5, {})
    t20 = window_stats.get(20, {})
    t60 = window_stats.get(60, {})
    t120 = window_stats.get(120, {})
    scores = {window: _window_risk_score(stats) for window, stats in window_stats.items()}
    medium_score = max(scores.get(20, -5.0), scores.get(60, -5.0))
    persistence_bonus = 1.0 if scores.get(20, -5.0) >= 3.0 and scores.get(60, -5.0) >= 3.0 else 0.0
    total_score = max(scores.values()) + persistence_bonus if scores else -5.0

    t20_valid = int(t20.get("valid_returns") or 0)
    t60_valid = int(t60.get("valid_returns") or 0)
    sample_valid = max(t20_valid, t60_valid)
    t20_median = _safe_float(t20.get("median_return"))
    t60_median = _safe_float(t60.get("median_return"))
    t20_neg = _safe_float(t20.get("negative_return_rate"))
    t60_neg = _safe_float(t60.get("negative_return_rate"))

    if sample_valid < 20:
        decision = "TOO_SPARSE"
        policy_shape = "watchlist_no_policy"
    elif total_score >= 7.0 and medium_score >= 4.0:
        decision = "RISK_DOWNWEIGHT_CANDIDATE"
        policy_shape = "avoid_new_buy_60td" if scores.get(60, -5.0) >= scores.get(20, -5.0) else "avoid_new_buy_20td"
    elif total_score >= 4.0 and medium_score >= 2.0:
        decision = "WATCHLIST_POLICY_RESEARCH"
        policy_shape = "soft_downweight_20_60td"
    elif _window_risk_score(t5) >= 4.0:
        decision = "SHORT_WARNING_ONLY"
        policy_shape = "avoid_new_buy_5td_research"
    else:
        decision = "REJECT_OR_MIXED"
        policy_shape = "watchlist_no_policy"

    return {
        "rule_key": rule_key,
        "direct_policy_decision": decision,
        "policy_shape": policy_shape,
        "action_boundary": "no_hard_ban_no_forced_sell_research_only",
        "risk_score": total_score,
        "medium_score": medium_score,
        "sample_valid": sample_valid,
        "t20_valid": t20_valid,
        "t20_median_abnormal": t20_median,
        "t20_negative_rate": t20_neg,
        "t60_valid": t60_valid,
        "t60_median_abnormal": t60_median,
        "t60_negative_rate": t60_neg,
        "t120_valid": int(t120.get("valid_returns") or 0),
        "t120_median_abnormal": _safe_float(t120.get("median_return")),
    }


def _policy_rows_from_window_rows(window_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_rule: dict[str, dict[int, Mapping[str, Any]]] = defaultdict(dict)
    for row in window_rows:
        by_rule[str(row.get("rule_key"))][int(row.get("window") or 0)] = row
    rows = [policy_decision(rule_key, by_window) for rule_key, by_window in by_rule.items()]
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("direct_policy_decision")) not in {"RISK_DOWNWEIGHT_CANDIDATE", "WATCHLIST_POLICY_RESEARCH"},
            -float(row.get("risk_score") or -999.0),
            str(row.get("rule_key")),
        ),
    )


def _top_policy_table_rows(rows: Sequence[Mapping[str, Any]], *, limit: int = 20) -> list[list[Any]]:
    table_rows: list[list[Any]] = []
    for row in rows[:limit]:
        table_rows.append(
            [
                row.get("direct_policy_decision"),
                row.get("policy_shape"),
                _fmt_float(row.get("risk_score"), 1),
                _fmt_int(row.get("sample_valid")),
                _compact_pct(row.get("t20_median_abnormal")),
                _compact_pct(row.get("t20_negative_rate")),
                _compact_pct(row.get("t60_median_abnormal")),
                _compact_pct(row.get("t60_negative_rate")),
                row.get("rule_key"),
            ]
        )
    return table_rows


def _window_table_rows(rows: Sequence[Mapping[str, Any]], *, limit: int = 30) -> list[list[Any]]:
    scored = []
    for row in rows:
        scored.append((_window_risk_score(row), row))
    scored.sort(key=lambda item: (-item[0], str(item[1].get("rule_key")), int(item[1].get("window") or 0)))
    table_rows: list[list[Any]] = []
    for score, row in scored[:limit]:
        table_rows.append(
            [
                _fmt_float(score, 1),
                row.get("window"),
                _fmt_int(row.get("valid_returns")),
                _compact_pct(row.get("mean_return")),
                _compact_pct(row.get("median_return")),
                _compact_pct(row.get("p25_return")),
                _compact_pct(row.get("negative_return_rate")),
                _compact_pct(row.get("loss_10pct_rate")),
                row.get("rule_key"),
            ]
        )
    return table_rows


def _family_table_rows(rows: Sequence[Mapping[str, Any]], *, limit: int = 30) -> list[list[Any]]:
    scored = []
    for row in rows:
        if int(row.get("window") or 0) not in {20, 60, 120}:
            continue
        scored.append((_window_risk_score(row), row))
    scored.sort(key=lambda item: (-item[0], str(item[1].get("event_type")), int(item[1].get("window") or 0)))
    table_rows: list[list[Any]] = []
    for score, row in scored[:limit]:
        table_rows.append(
            [
                _fmt_float(score, 1),
                row.get("event_type"),
                row.get("window"),
                _fmt_int(row.get("valid_returns")),
                _compact_pct(row.get("median_return")),
                _compact_pct(row.get("negative_return_rate")),
                _compact_pct(row.get("loss_10pct_rate")),
            ]
        )
    return table_rows


def build_phase32_payload(
    *,
    date_from: dt.date,
    date_to: dt.date,
    return_windows: Sequence[int],
    rule_keys: Sequence[str],
    benchmark: str,
) -> dict[str, Any]:
    rules = select_rules(rule_keys)
    max_window = max(return_windows)
    financial_rows, _ = load_enriched_financial_rows(
        date_from=date_from,
        date_to=date_to,
        active_trading_days=max(120, max_window),
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
    abnormal_window_rows = aggregate_return_rows(
        return_rows,
        group_fields=("rule_key", "window"),
        return_field="post_effective_abnormal_return_from_t0_close",
    )
    raw_window_rows = aggregate_return_rows(
        return_rows,
        group_fields=("rule_key", "window"),
        return_field="post_effective_return_from_t0_close",
    )
    event_type_rows = aggregate_return_rows(
        return_rows,
        group_fields=("event_type", "window"),
        return_field="post_effective_abnormal_return_from_t0_close",
    )
    market_cap_rows = aggregate_return_rows(
        return_rows,
        group_fields=("market_cap_bucket", "window"),
        return_field="post_effective_abnormal_return_from_t0_close",
    )
    policy_rows = _policy_rows_from_window_rows(abnormal_window_rows)

    return {
        "report_version": REPORT_VERSION,
        "parameters": {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "return_windows": list(return_windows),
            "rule_keys": list(rule_keys),
            "benchmark": benchmark,
            "policy_boundary": "research_only_no_runtime_hook_no_db_write",
            "capital_assumption": "about_10m_cny_no_market_impact_filter",
            "industry_policy": "no_industry_neutralization_currently_required",
        },
        "event_count": len(event_rows),
        "return_row_count": len(return_rows),
        "abnormal_returns_by_rule_window": abnormal_window_rows,
        "raw_returns_by_rule_window": raw_window_rows,
        "abnormal_returns_by_event_type_window": event_type_rows,
        "abnormal_returns_by_market_cap_window": market_cap_rows,
        "direct_policy_rows": policy_rows,
        "counts": {
            "risk_downweight_candidates": sum(1 for row in policy_rows if row["direct_policy_decision"] == "RISK_DOWNWEIGHT_CANDIDATE"),
            "watchlist_policy_research": sum(1 for row in policy_rows if row["direct_policy_decision"] == "WATCHLIST_POLICY_RESEARCH"),
            "short_warning_only": sum(1 for row in policy_rows if row["direct_policy_decision"] == "SHORT_WARNING_ONLY"),
            "too_sparse": sum(1 for row in policy_rows if row["direct_policy_decision"] == "TOO_SPARSE"),
            "reject_or_mixed": sum(1 for row in policy_rows if row["direct_policy_decision"] == "REJECT_OR_MIXED"),
        },
    }


def write_markdown(path: Path, payload: Mapping[str, Any]) -> None:
    params = payload["parameters"]
    counts = payload["counts"]
    top_policy_rows = _top_policy_table_rows(payload["direct_policy_rows"], limit=24)
    top_window_rows = _window_table_rows(payload["abnormal_returns_by_rule_window"], limit=30)
    family_rows = _family_table_rows(payload["abnormal_returns_by_event_type_window"], limit=20)

    lines = [
        "# Financial Distress Phase 32 Direct-Risk Policy Feasibility",
        "",
        "Research-only study of direct event-risk policy feasibility. No runtime consumer is changed.",
        "",
        "## Scope",
        "",
        "```text",
        *_fixed_width_table(
            ["item", "value"],
            [
                ["date range", f"{params['date_from']} -> {params['date_to']}"],
                ["rules", len(params["rule_keys"])],
                ["return windows", params["return_windows"]],
                ["benchmark", params["benchmark"]],
                ["events", _fmt_int(payload["event_count"])],
                ["return rows", _fmt_int(payload["return_row_count"])],
                ["runtime impact", "none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring"],
            ],
        ),
        "```",
        "",
        "## Outcome",
        "",
        "```text",
        *_fixed_width_table(
            ["item", "value", "interpretation"],
            [
                ["risk-downweight candidates", counts["risk_downweight_candidates"], "eligible for later offline overlay research, not live policy"],
                ["watchlist policy research", counts["watchlist_policy_research"], "has direct downside but needs policy-shape validation"],
                ["short warning only", counts["short_warning_only"], "short-lived evidence only"],
                ["too sparse", counts["too_sparse"], "sample too small for policy"],
                ["reject/mixed", counts["reject_or_mixed"], "no direct-risk policy support"],
                ["hard ban / forced sell", "0", "financial rules remain non-hard in this phase"],
            ],
        ),
        "```",
        "",
        "## Direct Policy Shortlist",
        "",
        "```text",
        *_fixed_width_table(
            ["decision", "shape", "score", "valid", "t20_med", "t20_neg", "t60_med", "t60_neg", "rule"],
            top_policy_rows,
        ),
        "```",
        "",
        "## Strongest Rule/Window Evidence",
        "",
        "```text",
        *_fixed_width_table(
            ["score", "win", "valid", "mean", "median", "p25", "neg", "loss10", "rule"],
            top_window_rows,
        ),
        "```",
        "",
        "## Event-Type Evidence",
        "",
        "```text",
        *_fixed_width_table(
            ["score", "event_type", "win", "valid", "median", "neg", "loss10"],
            family_rows,
        ),
        "```",
        "",
        "## Interpretation",
        "",
        "- Phase 32 does not promote any financial signal to hard buy-ban or forced-sell policy.",
        "- Direct downside can justify later offline studies of avoid-new-buy windows or score downweighting outside the alpha factor path.",
        "- Rules with strong direct downside but sparse samples should remain watchlist-only until more history or broader cohorts are available.",
        "- Next empirical step should test the top risk-downweight candidates as portfolio overlays, still outside QE/Paper runtime integration.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_phase32(
    *,
    output_dir: Path,
    doc_path: Path,
    date_from: dt.date,
    date_to: dt.date,
    rule_keys: Sequence[str],
    return_windows: Sequence[int],
    benchmark: str = DEFAULT_BENCHMARK,
) -> Phase32Summary:
    payload = build_phase32_payload(
        date_from=date_from,
        date_to=date_to,
        rule_keys=rule_keys,
        return_windows=return_windows,
        benchmark=benchmark,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "financial_distress_phase32_direct_risk_policy_feasibility.json"
    json_path.write_text(_json_dumps(payload), encoding="utf-8")
    write_markdown(doc_path, payload)
    counts = payload["counts"]
    return Phase32Summary(
        output_json=str(json_path),
        output_md=str(doc_path),
        rules=len(rule_keys),
        events=int(payload["event_count"]),
        return_rows=int(payload["return_row_count"]),
        policy_candidates=int(counts["risk_downweight_candidates"]),
        watchlist_rows=int(counts["watchlist_policy_research"]),
    )


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase32 direct-risk policy feasibility research")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--doc-path", default=str(DEFAULT_DOC_PATH))
    parser.add_argument("--date-from", default=DEFAULT_DATE_FROM)
    parser.add_argument("--date-to", default=DEFAULT_DATE_TO)
    parser.add_argument("--rule-key", action="append", default=None)
    parser.add_argument("--return-window", type=int, action="append", default=None)
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    parser.add_argument("--env-path", default=str(DEFAULT_ROOT_ENV))
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    env_path = Path(args.env_path)
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()
    if str(ROOT) != str(Path.cwd()):
        os.chdir(ROOT)

    rule_keys = tuple(args.rule_key) if args.rule_key else default_rule_keys()
    return_windows = tuple(args.return_window) if args.return_window else DEFAULT_RETURN_WINDOWS
    summary = run_phase32(
        output_dir=Path(args.output_dir),
        doc_path=Path(args.doc_path),
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        rule_keys=rule_keys,
        return_windows=return_windows,
        benchmark=args.benchmark,
    )
    print(_json_dumps(asdict(summary)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
