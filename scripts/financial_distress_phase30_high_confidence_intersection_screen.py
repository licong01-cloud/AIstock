"""Phase-30 high-conviction q_ocf intersection screen.

This research-only script tests whether the broad Phase-28 q_ocf signal becomes
more useful after adding size, deterioration, and rank-aware filters. It writes
ignored JSON artifacts plus a curated Markdown report. It does not write DB rows
and does not connect signals to QE, Selection Center, Paper, QMT, or live paths.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.event_signal.financial_distress_direct_event_research import run_direct_event_research  # noqa: E402
from backend.services.event_signal.financial_distress_qe_overlay_research import (  # noqa: E402
    PHASE30_RESEARCH_RULES,
    _fixed_width_table,
    _parse_date,
    _pct,
    load_loop_specs,
    run_multiloop_financial_distress_qe_overlay_research,
)


DEFAULT_LOOP_SPEC_JSON = Path("reports/event_signal/financial_distress_phase21_22_loop_overlay/phase21_loop_specs_22.json")
DEFAULT_OUTPUT_DIR = Path("reports/event_signal/financial_distress_phase30_high_confidence_intersection")
DEFAULT_DOC_PATH = Path(
    "docs/analysis/event_signal_financial_distress_phase30_high_confidence_intersection_result_20260511.md"
)
DEFAULT_ROOT_ENV = Path("F:/Dev/AIstock/.env")
BENCHMARK_RULE = "indicator_large_decline_mv_10_30bn"
QOCF_BASELINE_RULE = "indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn"
REPORT_VERSION = "financial_distress_phase30_high_confidence_intersection_v1_20260511"


@dataclass(frozen=True)
class Phase30Summary:
    output_json: str
    output_md: str
    direct_report_json: str
    overlay_report_jsons: tuple[str, ...]
    direct_rows: int
    overlay_rows: int
    shortlist_rows: int


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_float(value: Any, digits: int = 3) -> str:
    if value is None:
        return "NA"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    if not math.isfinite(number):
        return "NA"
    return f"{number:.{digits}f}"


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _rate(numerator: Any, denominator: Any) -> Optional[float]:
    den = _safe_float(denominator)
    if den <= 0:
        return None
    return _safe_float(numerator) / den


def _ex_best_avg(row: Mapping[str, Any]) -> Optional[float]:
    loops = int(row.get("loops") or 0)
    if loops <= 1:
        return None
    avg = _safe_float(row.get("avg_return_delta"))
    max_delta = _safe_float(row.get("max_return_delta"))
    return (avg * loops - max_delta) / (loops - 1)


def _mode_tag(mode: str) -> str:
    pct_match = re.search(r"rank_([0-9]+(?:\.[0-9]+)?)pct", mode)
    top_match = re.search(r"top(\d+)", mode)
    pct = pct_match.group(1) if pct_match else "NA"
    top = top_match.group(1) if top_match else "NA"
    return f"fixed{pct}_top{top}"


def _top_k_from_mode(mode: str) -> Optional[int]:
    top_match = re.search(r"top(\d+)", mode)
    return int(top_match.group(1)) if top_match else None


def _direct_summary_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for row in payload.get("returns_by_rule_window_abnormal", []):
        by_key[(str(row.get("rule_key")), int(row.get("window") or 0))] = dict(row)
    rows: list[dict[str, Any]] = []
    for rule_key in payload.get("parameters", {}).get("rule_keys", []):
        for window in (5, 20, 60):
            row = by_key.get((rule_key, window), {})
            rows.append(
                {
                    "rule_key": rule_key,
                    "window": window,
                    "valid_returns": int(row.get("valid_returns") or 0),
                    "mean_abnormal": row.get("mean_return"),
                    "median_abnormal": row.get("median_return"),
                    "negative_rate": row.get("negative_return_rate"),
                    "missing_price_rate": row.get("missing_price_rate"),
                }
            )
    return rows


def _direct_rule_score(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["rule_key"]), []).append(row)
    output: dict[str, dict[str, Any]] = {}
    for rule_key, rule_rows in grouped.items():
        t20 = next((row for row in rule_rows if int(row["window"]) == 20), {})
        t60 = next((row for row in rule_rows if int(row["window"]) == 60), {})
        med20 = t20.get("median_abnormal")
        med60 = t60.get("median_abnormal")
        neg20 = t20.get("negative_rate")
        risk_points = 0
        for value in (med20, med60):
            if value is not None and _safe_float(value) < 0:
                risk_points += 1
        if neg20 is not None and _safe_float(neg20) >= 0.55:
            risk_points += 1
        output[rule_key] = {
            "direct_risk_points": risk_points,
            "direct_decision": "supports_downweight" if risk_points >= 2 else ("mixed" if risk_points == 1 else "not_downside"),
            "t20_median_abnormal": med20,
            "t60_median_abnormal": med60,
            "t20_negative_rate": neg20,
        }
    return output


def _phase27_like_score(row: Mapping[str, Any]) -> float:
    loops = int(row.get("loops") or 0)
    avg = _safe_float(row.get("avg_return_delta"))
    median = _safe_float(row.get("median_return_delta"))
    min_delta = _safe_float(row.get("min_return_delta"))
    positive = int(row.get("positive_return_loops") or 0)
    evaluated = int(row.get("total_score_down_evaluated_topk_buy_events") or 0)
    dropped = int(row.get("total_score_down_dropped_from_topk_events") or 0)
    replacements = int(row.get("total_replacement_open_events") or 0)
    ex_best = _ex_best_avg(row) or 0.0
    score = 0.0
    score += min(max(avg / 0.002, -1.0), 2.0) * 20.0
    score += min(max(ex_best / 0.001, -1.0), 2.0) * 15.0
    score += min(max(median / 0.0002, -1.0), 1.0) * 10.0
    score += ((positive / max(loops, 1)) - 0.5) * 40.0
    score += min(dropped / 10.0, 1.0) * 10.0
    score += min(replacements / 5.0, 1.0) * 5.0
    score += min(evaluated / 500.0, 1.0) * 5.0
    score -= min(max((-min_delta - 0.003) / 0.01, 0.0), 1.0) * 15.0
    if loops >= 22:
        score += 10.0
    elif loops >= 10:
        score += 3.0
    else:
        score -= 10.0
    if median < -1e-12:
        score -= 5.0
    if ex_best <= 0.0:
        score -= 7.0
    return score


def _cheap_score(row: Mapping[str, Any]) -> float:
    score = _phase27_like_score(row)
    density = row.get("topk_hit_density")
    drop_rate = row.get("drop_rate_within_evaluated")
    overlay_rows = int(row.get("overlay_rows") or 0)
    evaluated = int(row.get("evaluated_topk_events") or 0)
    if density is not None:
        score += min(_safe_float(density) / 0.001, 1.5) * 8.0
    if drop_rate is not None:
        score += min(_safe_float(drop_rate) / 0.12, 1.0) * 5.0
    if overlay_rows > 200_000 and (density is None or _safe_float(density) < 0.0006):
        score -= 6.0
    if evaluated < 20:
        score -= 3.0
    return score


def _decision(row: Mapping[str, Any], score: float) -> tuple[str, str]:
    loops = int(row.get("loops") or 0)
    avg = _safe_float(row.get("avg_return_delta"))
    min_delta = _safe_float(row.get("min_return_delta"))
    ex_best = _ex_best_avg(row) or 0.0
    dropped = int(row.get("dropped_from_topk_events") or 0)
    evaluated = int(row.get("evaluated_topk_events") or 0)
    positive = int(row.get("positive_loops") or 0)
    positive_ratio = positive / max(loops, 1)
    if (
        loops >= 22
        and score >= 62.0
        and avg >= 0.0013
        and ex_best >= 0.0004
        and min_delta >= -0.0035
        and dropped >= 6
        and evaluated >= 20
        and positive_ratio >= 0.59
    ):
        return "WSL_TRUE_RERUN_CANDIDATE", "passes precision cheap screen; run one-loop WSL true QE before policy work"
    if loops >= 22 and avg > 0 and ex_best > 0 and min_delta >= -0.006 and positive_ratio >= 0.55:
        return "WATCHLIST", "positive cheap overlay, but insufficient for true-rerun gate"
    if avg > 0 and min_delta > -0.01:
        return "CALIBRATION_ONLY", "non-negative but too small, sparse, or low precision"
    return "REJECT", "insufficient cheap overlay evidence"


def _overlay_summary_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    exposure_by_key = {
        (str(row.get("rule_key")), int(row.get("active_trading_days") or 0)): row
        for row in payload.get("exposure_summary", [])
    }
    rows: list[dict[str, Any]] = []
    for raw in payload.get("validation_summary", {}).get("stability_rows", []):
        rule_key = str(raw.get("rule_key"))
        active_td = int(raw.get("active_trading_days") or 0)
        exposure = exposure_by_key.get((rule_key, active_td), {})
        overlay_rows = int(exposure.get("overlay_rows") or 0)
        evaluated = int(raw.get("total_score_down_evaluated_topk_buy_events") or 0)
        dropped = int(raw.get("total_score_down_dropped_from_topk_events") or 0)
        row = {
            "rule_key": rule_key,
            "active_trading_days": active_td,
            "simulator_mode": str(raw.get("simulator_mode") or ""),
            "mode_tag": _mode_tag(str(raw.get("simulator_mode") or "")),
            "top_k": _top_k_from_mode(str(raw.get("simulator_mode") or "")),
            "loops": int(raw.get("loops") or 0),
            "positive_loops": int(raw.get("positive_return_loops") or 0),
            "avg_return_delta": raw.get("avg_return_delta"),
            "median_return_delta": raw.get("median_return_delta"),
            "min_return_delta": raw.get("min_return_delta"),
            "max_return_delta": raw.get("max_return_delta"),
            "ex_best_avg_return_delta": _ex_best_avg(raw),
            "avg_mdd_delta": raw.get("avg_mdd_delta"),
            "overlay_rows": overlay_rows,
            "evaluated_topk_events": evaluated,
            "dropped_from_topk_events": dropped,
            "replacement_open_events": int(raw.get("total_replacement_open_events") or 0),
            "topk_hit_density": _rate(evaluated, overlay_rows),
            "drop_rate_within_evaluated": _rate(dropped, evaluated),
            "top_evaluated_market_cap_buckets": dict(raw.get("top_evaluated_market_cap_buckets") or {}),
            "top_dropped_market_cap_buckets": dict(raw.get("top_dropped_market_cap_buckets") or {}),
            "top_dropped_industries": dict(raw.get("top_dropped_industries") or {}),
        }
        score = _cheap_score(row)
        decision, next_action = _decision(row, score)
        row["cheap_score"] = score
        row["cheap_decision"] = decision
        row["next_action"] = next_action
        rows.append(row)
    return rows


def _best_overlay_by_rule(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        rule_key = str(row["rule_key"])
        current = best.get(rule_key)
        current_key = (float(current.get("cheap_score") or -999.0), float(current.get("avg_return_delta") or -999.0)) if current else None
        row_key = (float(row.get("cheap_score") or -999.0), float(row.get("avg_return_delta") or -999.0))
        if current is None or row_key > current_key:
            best[rule_key] = dict(row)
    return best


def _combined_shortlist(
    *,
    rule_keys: Sequence[str],
    direct_scores: Mapping[str, Mapping[str, Any]],
    overlay_best: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule_key in rule_keys:
        overlay = dict(overlay_best.get(rule_key) or {})
        direct = dict(direct_scores.get(rule_key) or {})
        overlay_decision = str(overlay.get("cheap_decision") or "NOT_RUN")
        direct_decision = str(direct.get("direct_decision") or "missing")
        if overlay_decision == "WSL_TRUE_RERUN_CANDIDATE" and direct_decision in {"supports_downweight", "mixed"}:
            final_decision = "TRUE_QE_CANDIDATE"
        elif overlay_decision in {"WSL_TRUE_RERUN_CANDIDATE", "WATCHLIST"} or direct_decision == "supports_downweight":
            final_decision = "WATCHLIST"
        else:
            final_decision = "REJECT_OR_CALIBRATION"
        rows.append(
            {
                "rule_key": rule_key,
                "final_decision": final_decision,
                "direct_decision": direct_decision,
                "direct_risk_points": direct.get("direct_risk_points"),
                "t20_median_abnormal": direct.get("t20_median_abnormal"),
                "t60_median_abnormal": direct.get("t60_median_abnormal"),
                "t20_negative_rate": direct.get("t20_negative_rate"),
                "cheap_decision": overlay_decision,
                "cheap_score": overlay.get("cheap_score"),
                "best_active_td": overlay.get("active_trading_days"),
                "best_mode": overlay.get("mode_tag"),
                "best_top_k": overlay.get("top_k"),
                "pos_loops": f"{overlay.get('positive_loops', 0)}/{overlay.get('loops', 0)}" if overlay else "0/0",
                "avg_return_delta": overlay.get("avg_return_delta"),
                "ex_best_avg_return_delta": overlay.get("ex_best_avg_return_delta"),
                "min_return_delta": overlay.get("min_return_delta"),
                "overlay_rows": overlay.get("overlay_rows"),
                "evaluated_topk_events": overlay.get("evaluated_topk_events"),
                "dropped_from_topk_events": overlay.get("dropped_from_topk_events"),
                "topk_hit_density": overlay.get("topk_hit_density"),
                "drop_rate_within_evaluated": overlay.get("drop_rate_within_evaluated"),
                "next_action": overlay.get("next_action") if overlay else "run overlay first",
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            {"TRUE_QE_CANDIDATE": 3, "WATCHLIST": 2, "REJECT_OR_CALIBRATION": 1}.get(str(row["final_decision"]), 0),
            float(row.get("cheap_score") or -999.0),
            int(row.get("direct_risk_points") or 0),
        ),
        reverse=True,
    )


def _run_overlay_sweep(args: argparse.Namespace, *, rule_keys: Sequence[str]) -> Path:
    output_dir = Path(args.output_dir) / "overlay" / "rank_aware"
    rule_by_key = {rule.rule_key: rule for rule in PHASE30_RESEARCH_RULES}
    selected_rules = tuple(rule_by_key[key] for key in rule_keys)
    summary = run_multiloop_financial_distress_qe_overlay_research(
        loop_specs=load_loop_specs(loop_specs=None, loop_spec_json=Path(args.loop_spec_json)),
        output_dir=output_dir,
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        active_trading_days_values=tuple(args.active_trading_days),
        simulator_modes=("score_down",),
        score_down_rank_penalty_pcts=tuple(args.score_down_rank_penalty_pct),
        score_down_top_k=tuple(args.top_k),
        score_down_ranking_date_mode="previous",
        write_overlay_csv=False,
        research_rules=selected_rules,
    )
    return Path(summary.output_json)


def _build_markdown(payload: Mapping[str, Any]) -> str:
    params = payload["parameters"]
    true_candidates = [
        row for row in payload["combined_shortlist"] if row.get("final_decision") == "TRUE_QE_CANDIDATE"
    ]
    best_overlay = payload["overlay_summary_rows"][0] if payload["overlay_summary_rows"] else {}
    outcome_rows = [
        [
            "true-QE candidates",
            str(len(true_candidates)),
            "0 means do not spend WSL true-rerun budget in this phase",
        ],
        [
            "best cheap row",
            (
                f"{best_overlay.get('rule_key', 'NA')} / {best_overlay.get('mode_tag', 'NA')} / "
                f"{best_overlay.get('active_trading_days', 'NA')}td"
            ),
            (
                f"score {_fmt_float(best_overlay.get('cheap_score'), 1)}, "
                f"avg {_pct(best_overlay.get('avg_return_delta'))}, "
                f"hit/overlay {_pct(best_overlay.get('topk_hit_density'))}"
            ),
        ],
        [
            "phase decision",
            "NO_WSL_TRUE_QE_RERUN",
            "direct downside exists, but cheap overlay precision/effect is far below the Phase-27 gate",
        ],
    ]
    shortlist_rows = [
        [
            row["final_decision"],
            _fmt_float(row.get("cheap_score"), 1),
            row["pos_loops"],
            _pct(row.get("avg_return_delta")),
            _pct(row.get("ex_best_avg_return_delta")),
            _pct(row.get("min_return_delta")),
            _fmt_int(row.get("evaluated_topk_events")),
            _fmt_int(row.get("dropped_from_topk_events")),
            _pct(row.get("topk_hit_density")),
            str(row.get("best_active_td") or "NA"),
            str(row.get("best_mode") or "NA"),
            row["direct_decision"],
            _pct(row.get("t20_median_abnormal")),
            row["rule_key"],
        ]
        for row in payload["combined_shortlist"][:16]
    ]
    direct_rows = [
        [
            row["rule_key"],
            f"T+{row['window']}",
            _fmt_int(row.get("valid_returns")),
            _pct(row.get("mean_abnormal")),
            _pct(row.get("median_abnormal")),
            _pct(row.get("negative_rate")),
            _pct(row.get("missing_price_rate")),
        ]
        for row in payload["direct_summary_rows"]
        if int(row["window"]) in {5, 20, 60}
    ]
    overlay_rows = [
        [
            _fmt_float(row.get("cheap_score"), 1),
            row["cheap_decision"],
            f"{row['positive_loops']}/{row['loops']}",
            _pct(row.get("avg_return_delta")),
            _pct(row.get("ex_best_avg_return_delta")),
            _pct(row.get("min_return_delta")),
            _fmt_int(row.get("evaluated_topk_events")),
            _fmt_int(row.get("dropped_from_topk_events")),
            _pct(row.get("topk_hit_density")),
            str(row.get("active_trading_days")),
            row.get("mode_tag"),
            row.get("rule_key"),
        ]
        for row in payload["overlay_summary_rows"][:24]
    ]
    benchmark_rows = [
        [
            "Phase28 q_ocf",
            "41,673",
            "221",
            "25",
            "0.530%",
            "+0.168%",
            "broad low-precision baseline",
        ],
        [
            "Phase19 indicator",
            "311",
            "311",
            "24",
            "100.000%",
            "+0.273%",
            "best one-loop true-smoke benchmark",
        ],
        [
            "Phase23 loss/mv",
            "304",
            "302",
            "61",
            "99.342%",
            "+0.066%",
            "calibration only; drops alone insufficient",
        ],
    ]
    return "\n".join(
        [
            "# Financial Distress Phase 30 High-Confidence Intersection Screen",
            "",
            "Research-only screen for q_ocf intersections and rank-aware TopK filters. No runtime consumer is changed.",
            "",
            "## Scope",
            "",
            "```text",
            * _fixed_width_table(
                ["item", "value"],
                [
                    ["date range", f"{params['date_from']} -> {params['date_to']}"],
                    ["rules", str(len(params["rule_keys"]))],
                    ["top_k sweep", ", ".join(str(item) for item in params["top_k_values"])],
                    ["active trading days", ", ".join(str(item) for item in params["active_trading_days"])],
                    ["direct report", str(payload["artifacts"]["direct_report_json"])],
                    ["overlay reports", str(len(payload["artifacts"].get("overlay_report_jsons") or []))],
                    ["runtime impact", "none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring"],
                ],
            ),
            "```",
            "",
            "## Outcome",
            "",
            "```text",
            * _fixed_width_table(["item", "value", "interpretation"], outcome_rows),
            "```",
            "",
            "## Phase 29 Calibration",
            "",
            "```text",
            * _fixed_width_table(
                ["case", "penalty", "top50", "drops", "top50/pen", "true_ret", "role"],
                benchmark_rows,
            ),
            "```",
            "",
            "## Combined Shortlist",
            "",
            "```text",
            * _fixed_width_table(
                ["decision", "score", "pos", "avg", "ex_best", "min", "eval", "drop", "hit/overlay", "td", "mode", "direct", "t20_med", "rule"],
                shortlist_rows,
            ),
            "```",
            "",
            "## Direct Event Abnormal Returns",
            "",
            "```text",
            * _fixed_width_table(["rule", "window", "valid", "abn_mean", "abn_median", "neg_rate", "miss_px"], direct_rows),
            "```",
            "",
            "## Cheap Overlay Top Rows",
            "",
            "```text",
            * _fixed_width_table(
                ["score", "decision", "pos", "avg", "ex_best", "min", "eval", "drop", "hit/overlay", "td", "mode", "rule"],
                overlay_rows,
            ),
            "```",
            "",
            "## Interpretation",
            "",
            "- Phase 30 is a cheap screen only; true QE rerun is still required before any policy or runtime integration.",
            "- The desired improvement over Phase 28 is not just higher average return, but better TopK concentration and drop precision.",
            "- Phase 30 does not produce a WSL true-QE candidate; keep these rules as watchlist/direct-event research only.",
            "- Financial distress remains non-hard at this stage: no buy ban, forced sell, score boost, DB policy write, or Paper/QE hook.",
            "- A row must pass the precision cheap screen before spending WSL true-QE budget; otherwise continue research or stop this branch.",
        ]
    )


def run_phase30(args: argparse.Namespace) -> Phase30Summary:
    load_dotenv(DEFAULT_ROOT_ENV, override=False)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rule_keys = [rule.rule_key for rule in PHASE30_RESEARCH_RULES]

    if args.reuse_direct_json:
        direct_json_path = Path(args.reuse_direct_json)
        direct_md_path = direct_json_path.with_suffix(".md")
        direct_payload = _load_json(direct_json_path)
    else:
        direct_summary = run_direct_event_research(
            output_dir=output_dir / "direct",
            date_from=_parse_date(args.date_from),
            date_to=_parse_date(args.date_to),
            rule_keys=tuple(rule_keys),
            return_windows=(0, 1, 5, 10, 20, 60),
        )
        direct_json_path = Path(direct_summary.output_json)
        direct_md_path = Path(direct_summary.output_md)
        direct_payload = _load_json(direct_json_path)
    direct_rows = _direct_summary_rows(direct_payload)

    overlay_paths: list[Path] = []
    if args.reuse_overlay_json:
        overlay_path = Path(args.reuse_overlay_json)
        overlay_paths.append(overlay_path)
        overlay_payload = _load_json(overlay_path)
        overlay_rows = _overlay_summary_rows(overlay_payload)
    elif not args.skip_overlay:
        overlay_path = _run_overlay_sweep(args, rule_keys=rule_keys)
        overlay_paths.append(overlay_path)
        overlay_payload = _load_json(overlay_path)
        overlay_rows = _overlay_summary_rows(overlay_payload)
    else:
        overlay_rows = []

    overlay_rows = sorted(
        overlay_rows,
        key=lambda row: (
            float(row.get("cheap_score") or -999.0),
            float(row.get("avg_return_delta") or -999.0),
            int(row.get("dropped_from_topk_events") or 0),
        ),
        reverse=True,
    )
    combined = _combined_shortlist(
        rule_keys=rule_keys,
        direct_scores=_direct_rule_score(direct_rows),
        overlay_best=_best_overlay_by_rule(overlay_rows),
    )
    payload = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "parameters": {
            "date_from": args.date_from,
            "date_to": args.date_to,
            "rule_keys": rule_keys,
            "loop_spec_json": str(args.loop_spec_json),
            "skip_overlay": bool(args.skip_overlay),
            "top_k_values": list(args.top_k),
            "active_trading_days": list(args.active_trading_days),
            "score_down_rank_penalty_pct": list(args.score_down_rank_penalty_pct),
            "benchmark_rule": BENCHMARK_RULE,
            "q_ocf_baseline_rule": QOCF_BASELINE_RULE,
        },
        "artifacts": {
            "direct_report_json": str(direct_json_path),
            "direct_report_md": str(direct_md_path),
            "overlay_report_jsons": [str(path) for path in overlay_paths],
            "overlay_report_mds": [str(path.with_suffix(".md")) for path in overlay_paths],
        },
        "direct_summary_rows": direct_rows,
        "overlay_summary_rows": overlay_rows,
        "combined_shortlist": combined,
        "research_boundary": {
            "writes_db": False,
            "changes_qe_runtime": False,
            "changes_selection_center": False,
            "changes_paper_trading": False,
            "changes_qmt_or_live_trading": False,
            "hard_block_enabled": False,
            "force_sell_enabled": False,
        },
    }
    output_json = output_dir / "financial_distress_phase30_high_confidence_intersection.json"
    output_json.write_text(_json_dumps(payload), encoding="utf-8")
    doc_path = Path(args.doc_path)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.write_text(_build_markdown(payload), encoding="utf-8")
    return Phase30Summary(
        output_json=str(output_json),
        output_md=str(doc_path),
        direct_report_json=str(direct_json_path),
        overlay_report_jsons=tuple(str(path) for path in overlay_paths),
        direct_rows=len(direct_rows),
        overlay_rows=len(overlay_rows),
        shortlist_rows=len(combined),
    )


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-from", default="2024-07-01")
    parser.add_argument("--date-to", default="2026-04-27")
    parser.add_argument("--loop-spec-json", type=Path, default=DEFAULT_LOOP_SPEC_JSON)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--doc-path", type=Path, default=DEFAULT_DOC_PATH)
    parser.add_argument("--top-k", type=int, action="append", default=None)
    parser.add_argument("--active-trading-days", type=int, action="append", default=None)
    parser.add_argument("--score-down-rank-penalty-pct", type=float, action="append", default=None)
    parser.add_argument("--skip-overlay", action="store_true")
    parser.add_argument("--reuse-direct-json", type=Path, default=None)
    parser.add_argument("--reuse-overlay-json", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.top_k is None:
        args.top_k = [20, 50]
    if args.active_trading_days is None:
        args.active_trading_days = [60, 90]
    if args.score_down_rank_penalty_pct is None:
        args.score_down_rank_penalty_pct = [0.10, 0.15, 0.20]
    return args


def main(argv: Optional[list[str]] = None) -> int:
    os.environ.setdefault("PYTHONPATH", str(ROOT))
    summary = run_phase30(parse_args(argv))
    print(_json_dumps(asdict(summary)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
