"""Build a research-only shortlist for financial-distress true QE reruns.

The script reads ignored offline overlay JSON reports under reports/event_signal
and ranks candidates before any expensive WSL full-universe true QE rerun.
It does not import or modify QE, Paper, Selection, QMT, or database runtime code.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


DEFAULT_REPORTS_DIR = Path("reports/event_signal")
BENCHMARK_RULE = "loss_to_market_cap_ge_50pct_mv_lt_10bn"
TESTED_WSL_RULE = "indicator_large_decline_mv_10_30bn"


@dataclass(frozen=True)
class CandidateRow:
    report: str
    source_json: str
    rule_key: str
    active_trading_days: int
    simulator_mode: str
    mode_tag: str
    loops: int
    positive_loops: int
    negative_loops: int
    positive_ratio: float
    avg_return_delta: float
    median_return_delta: float
    min_return_delta: float
    max_return_delta: float
    ex_best_avg_return_delta: float | None
    avg_mdd_delta: float | None
    blocked_buy_events: int
    evaluated_topk_events: int
    dropped_from_topk_events: int
    replacement_open_events: int
    avg_applied_penalty_pct: float | None
    evaluated_market_cap_buckets: dict[str, int]
    dropped_market_cap_buckets: dict[str, int]
    cheap_score: float
    decision: str
    next_action: str


def _latest_multiloop_reports(reports_dir: Path) -> list[Path]:
    paths: list[Path] = []
    if not reports_dir.exists():
        return paths
    for child in sorted(reports_dir.iterdir()):
        if not child.is_dir():
            continue
        candidates = sorted(
            child.glob("financial_distress_qe_multiloop_*.json"),
            key=lambda path: path.stat().st_mtime,
        )
        if candidates:
            paths.append(candidates[-1])
    return paths


def _latest_direct_event_report(reports_dir: Path) -> Path | None:
    direct_dir = reports_dir / "financial_distress_direct_event_returns"
    if not direct_dir.exists():
        return None
    candidates = sorted(
        direct_dir.glob("financial_distress_direct_event_*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    return candidates[-1] if candidates else None


def _mode_tag(mode: str) -> str:
    suffix = "_sector_relief" if "sector_relief" in mode else ""
    if "context_rank_decay_balanced" in mode:
        return "ctx_balanced" + suffix
    if "context_rank_decay_severity" in mode:
        return "ctx_severity" + suffix
    if "context_light" in mode:
        return "ctx_light" + suffix
    if "rank_10pct" in mode:
        return "fixed_10"
    if "rank_15pct" in mode:
        return "fixed_15"
    if "rank_20pct" in mode:
        return "fixed_20"
    if "severity" in mode:
        return "severity"
    if "skip_buy" in mode:
        return "skip_buy"
    return mode


def _float(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value is None:
        return default
    return float(value)


def _int(row: dict[str, Any], key: str, default: int = 0) -> int:
    value = row.get(key)
    if value is None:
        return default
    return int(value)


def _ex_best_avg(row: dict[str, Any]) -> float | None:
    loops = _int(row, "loops")
    if loops <= 1:
        return None
    avg = _float(row, "avg_return_delta")
    max_delta = _float(row, "max_return_delta")
    return (avg * loops - max_delta) / (loops - 1)


def _cheap_score(row: dict[str, Any]) -> float:
    """Heuristic screening score; not a trading metric.

    The score rewards stable average benefit, outlier-resistant average,
    positive-loop breadth, actual top-50 rank pressure, and enough candidate
    exposure. It penalizes tail losses, negative median, and insufficient loop
    coverage. The output is used only to triage expensive WSL reruns.
    """

    loops = _int(row, "loops")
    avg = _float(row, "avg_return_delta")
    median = _float(row, "median_return_delta")
    min_delta = _float(row, "min_return_delta")
    positive = _int(row, "positive_return_loops")
    evaluated = _int(row, "total_score_down_evaluated_topk_buy_events")
    dropped = _int(row, "total_score_down_dropped_from_topk_events")
    replacements = _int(row, "total_replacement_open_events")
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


def _decision(row: dict[str, Any], cheap_score: float) -> tuple[str, str]:
    loops = _int(row, "loops")
    avg = _float(row, "avg_return_delta")
    min_delta = _float(row, "min_return_delta")
    positive_ratio = _int(row, "positive_return_loops") / max(loops, 1)
    dropped = _int(row, "total_score_down_dropped_from_topk_events")
    ex_best = _ex_best_avg(row) or 0.0
    rule_key = str(row.get("rule_key") or "")
    active_days = _int(row, "active_trading_days")
    mode_tag = _mode_tag(str(row.get("simulator_mode") or ""))

    if (
        rule_key == TESTED_WSL_RULE
        and active_days == 60
        and mode_tag == "ctx_balanced"
        and loops >= 22
    ):
        return (
            "ALREADY_WSL_TESTED_WEAK",
            "do not expand true rerun yet; use as calibrated weak-positive baseline",
        )
    if rule_key == BENCHMARK_RULE:
        return (
            "BENCHMARK_ONLY",
            "keep as comparison benchmark; direct event returns do not prove hard risk",
        )
    if (
        loops >= 22
        and cheap_score >= 60.0
        and avg >= 0.0015
        and ex_best >= 0.0005
        and min_delta >= -0.0035
        and dropped >= 8
    ):
        return (
            "WSL_TRUE_RERUN_NOW",
            "eligible for one-loop WSL full-universe rerun before any batch rerun",
        )
    if (
        loops < 22
        and cheap_score >= 50.0
        and avg >= 0.0010
        and positive_ratio >= 0.55
        and dropped >= 5
    ):
        return (
            "EXPAND_22_LOOP_OVERLAY_FIRST",
            "run cheap 22-loop overlay expansion before WSL full-universe rerun",
        )
    if cheap_score >= 35.0:
        return (
            "WATCHLIST",
            "keep for future screen; not enough for true rerun now",
        )
    return (
        "REJECT_TRUE_RERUN",
        "do not spend WSL true-rerun budget at current evidence level",
    )


def load_candidate_rows(reports_dir: Path) -> list[CandidateRow]:
    rows: list[CandidateRow] = []
    for path in _latest_multiloop_reports(reports_dir):
        payload = json.loads(path.read_text(encoding="utf-8"))
        report = path.parent.name
        for raw in payload.get("validation_summary", {}).get("stability_rows", []):
            score = _cheap_score(raw)
            decision, next_action = _decision(raw, score)
            loops = _int(raw, "loops")
            row = CandidateRow(
                report=report,
                source_json=str(path),
                rule_key=str(raw.get("rule_key") or ""),
                active_trading_days=_int(raw, "active_trading_days"),
                simulator_mode=str(raw.get("simulator_mode") or ""),
                mode_tag=_mode_tag(str(raw.get("simulator_mode") or "")),
                loops=loops,
                positive_loops=_int(raw, "positive_return_loops"),
                negative_loops=_int(raw, "negative_return_loops"),
                positive_ratio=_int(raw, "positive_return_loops") / max(loops, 1),
                avg_return_delta=_float(raw, "avg_return_delta"),
                median_return_delta=_float(raw, "median_return_delta"),
                min_return_delta=_float(raw, "min_return_delta"),
                max_return_delta=_float(raw, "max_return_delta"),
                ex_best_avg_return_delta=_ex_best_avg(raw),
                avg_mdd_delta=(
                    float(raw["avg_mdd_delta"])
                    if raw.get("avg_mdd_delta") is not None
                    else None
                ),
                blocked_buy_events=_int(raw, "total_blocked_buy_events"),
                evaluated_topk_events=_int(raw, "total_score_down_evaluated_topk_buy_events"),
                dropped_from_topk_events=_int(raw, "total_score_down_dropped_from_topk_events"),
                replacement_open_events=_int(raw, "total_replacement_open_events"),
                avg_applied_penalty_pct=(
                    float(raw["avg_applied_penalty_pct"])
                    if raw.get("avg_applied_penalty_pct") is not None
                    else None
                ),
                evaluated_market_cap_buckets=dict(raw.get("evaluated_market_cap_buckets") or {}),
                dropped_market_cap_buckets=dict(raw.get("dropped_market_cap_buckets") or {}),
                cheap_score=score,
                decision=decision,
                next_action=next_action,
            )
            rows.append(row)
    return rows


def _best_by_rule(rows: Iterable[CandidateRow]) -> list[CandidateRow]:
    best: dict[str, CandidateRow] = {}
    for row in sorted(
        rows,
        key=lambda item: (
            item.cheap_score,
            item.loops,
            item.avg_return_delta,
            item.dropped_from_topk_events,
        ),
        reverse=True,
    ):
        best.setdefault(row.rule_key, row)
    return list(best.values())


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.3f}%"


def _table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) if rows else len(headers[index])
        for index in range(len(headers))
    ]
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def line(values: list[str]) -> str:
        return "| " + " | ".join(
            values[index].ljust(widths[index]) for index in range(len(values))
        ) + " |"

    output = [border, line(headers), border]
    output.extend(line(row) for row in rows)
    output.append(border)
    return "\n".join(output)


def build_markdown(rows: list[CandidateRow], reports_dir: Path) -> str:
    sorted_rows = sorted(
        rows,
        key=lambda item: (
            item.cheap_score,
            item.loops,
            item.avg_return_delta,
            item.dropped_from_topk_events,
        ),
        reverse=True,
    )
    best_rows = _best_by_rule(sorted_rows)
    immediate = [row for row in best_rows if row.decision == "WSL_TRUE_RERUN_NOW"]
    expand = [row for row in best_rows if row.decision == "EXPAND_22_LOOP_OVERLAY_FIRST"]
    already = [row for row in best_rows if row.decision == "ALREADY_WSL_TESTED_WEAK"]

    shortlist_rows = immediate + already[:1] + expand[:8]
    if not shortlist_rows:
        shortlist_rows = sorted_rows[:10]

    top_table = _table(
        [
            "score",
            "loops",
            "pos",
            "avg",
            "ex_best",
            "min",
            "drop",
            "td",
            "mode",
            "rule",
            "decision",
        ],
        [
            [
                f"{row.cheap_score:.1f}",
                str(row.loops),
                f"{row.positive_loops}/{row.loops}",
                _fmt_pct(row.avg_return_delta),
                _fmt_pct(row.ex_best_avg_return_delta),
                _fmt_pct(row.min_return_delta),
                str(row.dropped_from_topk_events),
                str(row.active_trading_days),
                row.mode_tag,
                row.rule_key,
                row.decision,
            ]
            for row in shortlist_rows
        ],
    )

    best_table = _table(
        [
            "score",
            "loops",
            "pos",
            "avg",
            "ex_best",
            "min",
            "drop",
            "td",
            "mode",
            "best_rule",
            "next",
        ],
        [
            [
                f"{row.cheap_score:.1f}",
                str(row.loops),
                f"{row.positive_loops}/{row.loops}",
                _fmt_pct(row.avg_return_delta),
                _fmt_pct(row.ex_best_avg_return_delta),
                _fmt_pct(row.min_return_delta),
                str(row.dropped_from_topk_events),
                str(row.active_trading_days),
                row.mode_tag,
                row.rule_key,
                row.decision,
            ]
            for row in best_rows[:16]
        ],
    )

    decision_counts: dict[str, int] = {}
    for row in rows:
        decision_counts[row.decision] = decision_counts.get(row.decision, 0) + 1
    decision_table = _table(
        ["decision", "rows", "meaning"],
        [
            [
                key,
                str(decision_counts[key]),
                {
                    "WSL_TRUE_RERUN_NOW": "passes strict cheap gate",
                    "ALREADY_WSL_TESTED_WEAK": "one-loop true rerun exists but weak",
                    "EXPAND_22_LOOP_OVERLAY_FIRST": "promising 10-loop row; cheap expansion first",
                    "BENCHMARK_ONLY": "comparison rule, not deployment thesis",
                    "WATCHLIST": "not enough for WSL budget",
                    "REJECT_TRUE_RERUN": "insufficient evidence",
                }.get(key, ""),
            ]
            for key in sorted(decision_counts)
        ],
    )
    direct_table = _build_direct_event_table(reports_dir)

    return f"""# Phase 20 Selective True QE Rerun Shortlist - {datetime.now().strftime('%Y-%m-%d')}

Research-only screening report generated from ignored offline reports in `{reports_dir}`. This is a budget gate before expensive WSL full-universe true QE reruns; it does not approve runtime integration, DB policy persistence, hard buy bans, or forced sells.

## Screening Gate

```text
+----------------------+--------------------------------------------------------------+
| gate                 | requirement                                                  |
+----------------------+--------------------------------------------------------------+
| cheap first          | use existing overlay/event-study artifacts before WSL rerun  |
| broad proof          | prefer 22-loop evidence; 10-loop rows need cheap expansion   |
| outlier control      | compare avg with ex-best average and worst loop              |
| real impact          | require actual top50 drops/replacements, not only hits       |
| runtime boundary     | no QE/Paper/Selection/QMT code path is changed               |
+----------------------+--------------------------------------------------------------+
```

## Decision Counts

```text
{decision_table}
```

## Shortlist

```text
{top_table}
```

## Best Row Per Rule

```text
{best_table}
```

## Direct Event Sanity Check

```text
{direct_table}
```

## Phase 20 Conclusion

- No new candidate passes `WSL_TRUE_RERUN_NOW`; broad WSL batch reruns are not justified yet.
- The already WSL-tested `indicator_large_decline_mv_10_30bn / 60td / ctx_balanced` remains a calibrated weak-positive baseline, not a deployment candidate.
- Strong 10-loop rows, especially `structured_financial_risk_mv_ge_10bn`, must first be expanded to the same 22-loop cheap overlay set before a WSL true-rerun budget is spent.
- The old `loss_to_market_cap_ge_50pct_mv_lt_10bn` benchmark remains useful, but direct event returns do not support a hard-risk thesis; keep it as a benchmark rather than a runtime rule.
- Next empirical step should be a cheap 22-loop overlay expansion for the top 10-loop candidates, not LLM/PDF and not production wiring.
"""


def _build_direct_event_table(reports_dir: Path) -> str:
    direct_path = _latest_direct_event_report(reports_dir)
    if direct_path is None:
        return _table(
            ["rule", "window", "mean_abn", "median_abn", "neg_rate", "valid", "note"],
            [["n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "direct event report missing"]],
        )

    payload = json.loads(direct_path.read_text(encoding="utf-8"))
    rows: list[list[str]] = []
    keep_rules = {
        "indicator_large_decline_mv_10_30bn",
        "loss_to_market_cap_ge_50pct_mv_lt_10bn",
        "structured_financial_risk_mv_10_30bn",
    }
    for item in payload.get("returns_by_rule_window_abnormal", []):
        if item.get("rule_key") not in keep_rules:
            continue
        if item.get("window") not in {5, 20, 60}:
            continue
        median_abn = float(item.get("median_return") or 0.0)
        note = "positive median" if median_abn > 0 else "negative median"
        rows.append(
            [
                str(item.get("rule_key")),
                f"T+{item.get('window')}",
                _fmt_pct(float(item.get("mean_return") or 0.0)),
                _fmt_pct(median_abn),
                _fmt_pct(float(item.get("negative_return_rate") or 0.0)),
                str(item.get("valid_returns")),
                note,
            ]
        )
    rows.sort(key=lambda row: (row[0], int(row[1].replace("T+", ""))))
    return _table(
        ["rule", "window", "mean_abn", "median_abn", "neg_rate", "valid", "note"],
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--doc-path", type=Path, default=None)
    args = parser.parse_args()

    rows = load_candidate_rows(args.reports_dir)
    if not rows:
        raise SystemExit(f"No candidate rows found under {args.reports_dir}")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "reports_dir": str(args.reports_dir),
        "candidate_rows": [asdict(row) for row in rows],
        "best_by_rule": [asdict(row) for row in _best_by_rule(rows)],
    }

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        output_json = args.output_dir / "financial_distress_true_qe_shortlist.json"
        output_json.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    markdown = build_markdown(rows, args.reports_dir)
    if args.doc_path:
        args.doc_path.parent.mkdir(parents=True, exist_ok=True)
        args.doc_path.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)


if __name__ == "__main__":
    main()
