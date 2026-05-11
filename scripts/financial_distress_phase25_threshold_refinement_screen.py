"""Phase-25 research-only threshold refinement screen for OCF/leverage stress signals.

The script combines direct event return checks and cheap multi-loop QE overlay
summaries for newly added structured rules. It writes ignored JSON artifacts and
a curated Markdown report; it does not mutate DB rows or connect signals to QE,
Selection, Paper, QMT, or live-trading consumers.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
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
    PHASE25_RESEARCH_RULES,
    _fixed_width_table,
    _pct,
    _parse_date,
)


DEFAULT_LOOP_SPEC_JSON = Path("reports/event_signal/financial_distress_phase21_22_loop_overlay/phase21_loop_specs_22.json")
DEFAULT_OUTPUT_DIR = Path("reports/event_signal/financial_distress_phase25_threshold_refinement")
DEFAULT_DOC_PATH = Path("docs/analysis/event_signal_financial_distress_phase25_threshold_refinement_result_20260511.md")
DEFAULT_ROOT_ENV = Path("F:/Dev/AIstock/.env")
BENCHMARK_RULE = "loss_to_market_cap_ge_50pct_mv_lt_10bn"
REPORT_VERSION = "financial_distress_phase25_threshold_refinement_v1_20260511"


@dataclass(frozen=True)
class Phase25Summary:
    output_json: str
    output_md: str
    direct_report_json: str
    overlay_report_json: Optional[str]
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
        return str(int(value or 0))
    except (TypeError, ValueError):
        return "0"


def _ex_best_avg(row: Mapping[str, Any]) -> Optional[float]:
    loops = int(row.get("loops") or 0)
    if loops <= 1:
        return None
    avg = float(row.get("avg_return_delta") or 0.0)
    max_delta = float(row.get("max_return_delta") or 0.0)
    return (avg * loops - max_delta) / (loops - 1)


def _mode_tag(mode: str) -> str:
    if "rank_10pct" in mode:
        return "fixed_10"
    if "rank_20pct" in mode:
        return "fixed_20"
    if "rank_5pct" in mode:
        return "fixed_5"
    if "context_rank_decay_balanced" in mode:
        return "ctx_balanced"
    if "severity" in mode:
        return "severity"
    return mode


def _cheap_score(row: Mapping[str, Any]) -> float:
    loops = int(row.get("loops") or 0)
    avg = float(row.get("avg_return_delta") or 0.0)
    median = float(row.get("median_return_delta") or 0.0)
    min_delta = float(row.get("min_return_delta") or 0.0)
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


def _decision(row: Mapping[str, Any], score: float) -> tuple[str, str]:
    loops = int(row.get("loops") or 0)
    avg = float(row.get("avg_return_delta") or 0.0)
    min_delta = float(row.get("min_return_delta") or 0.0)
    ex_best = _ex_best_avg(row) or 0.0
    dropped = int(row.get("total_score_down_dropped_from_topk_events") or 0)
    positive = int(row.get("positive_return_loops") or 0)
    positive_ratio = positive / max(loops, 1)
    if loops >= 22 and score >= 60.0 and avg >= 0.0015 and ex_best >= 0.0005 and min_delta >= -0.0035 and dropped >= 8:
        return "WSL_TRUE_RERUN_CANDIDATE", "passes cheap screen; run one-loop WSL true QE before any policy work"
    if loops >= 22 and avg > 0 and ex_best > 0 and min_delta >= -0.006 and positive_ratio >= 0.55:
        return "WATCHLIST", "weak positive cheap overlay; needs direct evidence or better threshold"
    if avg > 0 and min_delta > -0.01:
        return "CALIBRATION_ONLY", "non-negative but too small or sparse"
    return "REJECT", "insufficient cheap overlay evidence"


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
            if value is not None and float(value) < 0:
                risk_points += 1
        if neg20 is not None and float(neg20) >= 0.55:
            risk_points += 1
        output[rule_key] = {
            "direct_risk_points": risk_points,
            "direct_decision": "supports_downweight" if risk_points >= 2 else ("mixed" if risk_points == 1 else "not_downside"),
            "t20_median_abnormal": med20,
            "t60_median_abnormal": med60,
            "t20_negative_rate": neg20,
        }
    return output


def _overlay_summary_rows(payload: Optional[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not payload:
        return []
    rows: list[dict[str, Any]] = []
    for raw in payload.get("validation_summary", {}).get("stability_rows", []):
        score = _cheap_score(raw)
        decision, next_action = _decision(raw, score)
        rows.append(
            {
                "rule_key": str(raw.get("rule_key")),
                "active_trading_days": int(raw.get("active_trading_days") or 0),
                "simulator_mode": str(raw.get("simulator_mode") or ""),
                "mode_tag": _mode_tag(str(raw.get("simulator_mode") or "")),
                "loops": int(raw.get("loops") or 0),
                "positive_loops": int(raw.get("positive_return_loops") or 0),
                "avg_return_delta": raw.get("avg_return_delta"),
                "median_return_delta": raw.get("median_return_delta"),
                "min_return_delta": raw.get("min_return_delta"),
                "max_return_delta": raw.get("max_return_delta"),
                "ex_best_avg_return_delta": _ex_best_avg(raw),
                "avg_mdd_delta": raw.get("avg_mdd_delta"),
                "evaluated_topk_events": int(raw.get("total_score_down_evaluated_topk_buy_events") or 0),
                "dropped_from_topk_events": int(raw.get("total_score_down_dropped_from_topk_events") or 0),
                "replacement_open_events": int(raw.get("total_replacement_open_events") or 0),
                "cheap_score": score,
                "cheap_decision": decision,
                "next_action": next_action,
                "top_evaluated_market_cap_buckets": dict(raw.get("top_evaluated_market_cap_buckets") or {}),
                "top_dropped_market_cap_buckets": dict(raw.get("top_dropped_market_cap_buckets") or {}),
                "top_dropped_industries": dict(raw.get("top_dropped_industries") or {}),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("cheap_score") or 0.0),
            float(row.get("avg_return_delta") or 0.0),
            int(row.get("dropped_from_topk_events") or 0),
        ),
        reverse=True,
    )


def _best_overlay_by_rule(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        rule_key = str(row["rule_key"])
        current = best.get(rule_key)
        if current is None or (float(row.get("cheap_score") or 0.0), float(row.get("avg_return_delta") or 0.0)) > (
            float(current.get("cheap_score") or 0.0),
            float(current.get("avg_return_delta") or 0.0),
        ):
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
                "pos_loops": f"{overlay.get('positive_loops', 0)}/{overlay.get('loops', 0)}" if overlay else "0/0",
                "avg_return_delta": overlay.get("avg_return_delta"),
                "ex_best_avg_return_delta": overlay.get("ex_best_avg_return_delta"),
                "min_return_delta": overlay.get("min_return_delta"),
                "dropped_from_topk_events": overlay.get("dropped_from_topk_events"),
                "next_action": overlay.get("next_action") or "run overlay first" if not overlay else overlay.get("next_action"),
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


def _run_overlay_command(args: argparse.Namespace, rule_keys: Sequence[str]) -> Optional[Path]:
    if args.skip_overlay:
        return None
    output_dir = Path(args.output_dir)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    cmd = [
        sys.executable,
        "-m",
        "backend.services.event_signal.financial_distress_qe_overlay_research",
        "--loop-spec-json",
        str(args.loop_spec_json),
        "--output-dir",
        str(output_dir / "overlay"),
        "--date-from",
        args.date_from,
        "--date-to",
        args.date_to,
        "--active-trading-days",
        "20",
        "--active-trading-days",
        "60",
        "--active-trading-days",
        "120",
        "--simulator-mode",
        "score_down",
        "--score-down-rank-penalty-pct",
        "0.10",
        "--score-down-rank-penalty-pct",
        "0.20",
        "--score-down-ranking-date-mode",
        "previous",
        "--include-phase25-rules",
        "--no-overlay-csv",
    ]
    for rule_key in rule_keys:
        cmd.extend(["--rule-key", rule_key])
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)
    candidates = sorted((output_dir / "overlay").glob("financial_distress_qe_multiloop_*.json"), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def _build_markdown(payload: Mapping[str, Any]) -> str:
    params = payload["parameters"]
    shortlist_rows = [
        [
            row["final_decision"],
            _fmt_float(row.get("cheap_score"), 1),
            row["pos_loops"],
            _pct(row.get("avg_return_delta")),
            _pct(row.get("ex_best_avg_return_delta")),
            _pct(row.get("min_return_delta")),
            _fmt_int(row.get("dropped_from_topk_events")),
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
            _fmt_int(row.get("dropped_from_topk_events")),
            str(row.get("active_trading_days")),
            row.get("mode_tag"),
            row.get("rule_key"),
        ]
        for row in payload["overlay_summary_rows"][:20]
    ]
    rule_family_rows = [
        [
            "size_split",
            "split Phase-24 OCF/leverage stress into 10-30bn and 30-100bn buckets",
            "identify whether cheap overlay benefit is concentrated in one investable size bucket",
        ],
        [
            "component_threshold",
            "isolate q_ocf_to_sales<0, OCF yoy<=-50, debt/assets>=80/90, current ratio<0.8",
            "test whether stricter quality thresholds improve tail without losing all Top50 hits",
        ],
        [
            "compound_context",
            "combine OCF/leverage stress with actual_yoy<=-80, prior losses, or profit/revenue divergence",
            "search for stronger direct downside and cheap overlay interaction before WSL true QE",
        ],
    ]
    return "\n".join(
        [
            "# Financial Distress Phase 25 Threshold Refinement Screen",
            "",
            "Research-only refinement of the Phase-24 OCF/leverage watchlist family. No runtime consumer is changed.",
            "",
            "## Scope",
            "",
            "```text",
            * _fixed_width_table(
                ["item", "value"],
                [
                    ["date range", f"{params['date_from']} -> {params['date_to']}"],
                    ["rules", str(len(params["rule_keys"]))],
                    ["direct report", str(payload["artifacts"]["direct_report_json"])],
                    ["overlay report", str(payload["artifacts"].get("overlay_report_json") or "skipped")],
                    ["runtime impact", "none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring"],
                ],
            ),
            "```",
            "",
            "## Refinement Families",
            "",
            "```text",
            * _fixed_width_table(["family", "rule idea", "validation role"], rule_family_rows),
            "```",
            "",
            "## Combined Shortlist",
            "",
            "```text",
            * _fixed_width_table(
                ["decision", "score", "pos", "avg", "ex_best", "min", "drop", "td", "mode", "direct", "t20_med", "rule"],
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
            * _fixed_width_table(["score", "decision", "pos", "avg", "ex_best", "min", "drop", "td", "mode", "rule"], overlay_rows),
            "```",
            "",
            "## Interpretation",
            "",
            "- Cheap overlay remains a shortlist gate only; true QE is required before promotion.",
            "- Financial signals are still non-hard: no buy ban, no forced sell, no alpha boost in this phase.",
            "- Phase 25 is a threshold and subrule screen, not a runtime-policy approval.",
            "- Rules with direct downside but poor overlay stay research features; rules with overlay benefit but no direct downside stay calibration-only.",
            "- If a row reaches TRUE_QE_CANDIDATE, run one-loop WSL true QE smoke before any signal-table or runtime design.",
        ]
    )


def run_phase25(args: argparse.Namespace) -> Phase25Summary:
    load_dotenv(DEFAULT_ROOT_ENV, override=False)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rule_keys = [rule.rule_key for rule in PHASE25_RESEARCH_RULES]
    direct_summary = run_direct_event_research(
        output_dir=output_dir / "direct",
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        rule_keys=tuple(rule_keys),
        return_windows=(0, 1, 5, 10, 20, 60),
    )
    direct_payload = _load_json(Path(direct_summary.output_json))
    direct_rows = _direct_summary_rows(direct_payload)
    overlay_path = _run_overlay_command(args, rule_keys)
    overlay_payload = _load_json(overlay_path) if overlay_path is not None else None
    overlay_rows = _overlay_summary_rows(overlay_payload)
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
            "benchmark_rule": BENCHMARK_RULE,
        },
        "artifacts": {
            "direct_report_json": direct_summary.output_json,
            "direct_report_md": direct_summary.output_md,
            "overlay_report_json": str(overlay_path) if overlay_path else None,
            "overlay_report_md": str(overlay_path.with_suffix(".md")) if overlay_path else None,
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
    output_json = output_dir / "financial_distress_phase25_threshold_refinement.json"
    output_json.write_text(_json_dumps(payload), encoding="utf-8")
    doc_path = Path(args.doc_path)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.write_text(_build_markdown(payload), encoding="utf-8")
    return Phase25Summary(
        output_json=str(output_json),
        output_md=str(doc_path),
        direct_report_json=direct_summary.output_json,
        overlay_report_json=str(overlay_path) if overlay_path else None,
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
    parser.add_argument("--skip-overlay", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    summary = run_phase25(parse_args(argv))
    print(_json_dumps(asdict(summary)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
