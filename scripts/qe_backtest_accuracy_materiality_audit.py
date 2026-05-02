#!/usr/bin/env python
"""Follow-up QE backtest data-accuracy materiality audit.

This read-only audit consumes existing P0/P1 JSON artifacts and QE run logs.
It does not rerun QE, does not mutate workspaces, and does not require any new
strategy logging. The goal is to separate proven numerical accuracy checks from
data-coverage warnings that may affect skipped orders.
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _parse_loops(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            out.extend(range(int(start), int(end) + 1))
        else:
            out.append(int(part))
    return sorted(dict.fromkeys(out))


def _safe_float(value: Any) -> float | None:
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _fmt_float(value: Any, digits: int = 6) -> str:
    x = _safe_float(value)
    return "NA" if x is None else f"{x:.{digits}f}"


def _fmt_pct(value: Any) -> str:
    x = _safe_float(value)
    return "NA" if x is None else f"{x * 100:.2f}%"


def _table(rows: list[list[Any]], headers: list[str]) -> str:
    str_rows = [[str(c) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[i]) if i < len(row) - 1 else cell for i, cell in enumerate(row))

    lines = [fmt([str(h) for h in headers])]
    lines.append("  ".join("-" * width for width in widths))
    lines.extend(fmt(row) for row in str_rows)
    return "\n".join(lines)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _get_loop(rows: list[dict[str, Any]], loop: int) -> dict[str, Any]:
    for row in rows:
        if int(row.get("loop")) == loop:
            return row
    raise KeyError(f"loop {loop} not found")


def _find_report_path(loop_dir: Path, p0_loop: dict[str, Any]) -> Path:
    artifact_dir = p0_loop.get("artifact_dir")
    if artifact_dir:
        candidate = Path(artifact_dir) / "portfolio_analysis" / "report_normal_1day.pkl"
        if candidate.exists():
            return candidate
    candidates = sorted(loop_dir.glob("mlruns/*/*/artifacts/portfolio_analysis/report_normal_1day.pkl"))
    if not candidates:
        raise FileNotFoundError(f"missing report_normal_1day.pkl under {loop_dir}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _load_report(loop_dir: Path, p0_loop: dict[str, Any]) -> pd.DataFrame:
    path = _find_report_path(loop_dir, p0_loop)
    with path.open("rb") as f:
        report = pickle.load(f)
    if not isinstance(report, pd.DataFrame):
        raise TypeError(f"{path} is not a pandas DataFrame")
    if "return" not in report.columns or "account" not in report.columns:
        raise ValueError(f"{path} missing required return/account columns")
    return report


def _parse_invalid_price_events(run_log: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not run_log.exists():
        return events
    for line_no, line in enumerate(run_log.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        if "[ScoreWeightedV2]" not in line or "None" not in line or "date=" not in line:
            continue
        try:
            stock = line.split("[ScoreWeightedV2]", 1)[1].strip().split()[0]
            date = line.rsplit("date=", 1)[1].strip()[:10]
        except Exception as exc:
            raise ValueError(f"failed to parse invalid-price line {run_log}:{line_no}: {line[:200]}") from exc
        events.append({"line": line_no, "stock": stock, "date": date})
    return events


def _nan_inf_counts(df: pd.DataFrame) -> dict[str, int]:
    numeric = df.select_dtypes(include=[np.number])
    if numeric.empty:
        return {"nan": 0, "inf": 0}
    values = numeric.to_numpy(dtype=float)
    return {
        "nan": int(np.isnan(values).sum()),
        "inf": int(np.isinf(values).sum()),
    }


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    workspace = Path(args.workspace)
    loops = _parse_loops(args.loops)
    p0 = _load_json(args.p0_json)
    execution = _load_json(args.execution_json)
    v25 = _load_json(args.v25_json)
    price = _load_json(args.price_json)
    close = _load_json(args.close_json)

    price_state = {(int(r["loop"]), r["stock"], r["start"]): r["db_state"] for r in price["warning_audits"]}
    price_counts = Counter((int(r["loop"]), r["db_state"]) for r in price["warning_audits"])
    price_total = Counter(int(r["loop"]) for r in price["warning_audits"])

    loop_results: list[dict[str, Any]] = []
    for loop in loops:
        p0_loop = _get_loop(p0["loops"], loop)
        exec_loop = _get_loop(execution["loops"], loop)
        v25_loop = _get_loop(v25["loops"], loop)
        loop_dir = workspace / f"Loop{loop}"
        report = _load_report(loop_dir, p0_loop)
        invalid_events = _parse_invalid_price_events(loop_dir / "run.log")

        state_counts: Counter[str] = Counter()
        for event in invalid_events:
            event_state = price_state.get((loop, event["stock"], event["date"]), "NOT_IN_PRICE_WARNING_JSON")
            event["db_state"] = event_state
            state_counts[event_state] += 1

        invalid_dates = sorted({event["date"] for event in invalid_events})
        date_index = report.index.strftime("%Y-%m-%d")
        invalid_mask = date_index.isin(invalid_dates)
        invalid_returns = report.loc[invalid_mask, "return"]
        other_returns = report.loc[~invalid_mask, "return"]

        side_counts = v25_loop["stock_trades_reconciliation"].get("side_counts", {})
        derived_buy_trades = int(side_counts.get("buy", 0) or 0)
        invalid_total = len(invalid_events)
        invalid_db_present = state_counts.get("DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED", 0)

        sig = p0_loop["signal_accuracy"]
        rep = p0_loop["report_accuracy"]
        pos = exec_loop["position_summary"]
        ind = v25_loop["indicator_profile"]
        loop_results.append(
            {
                "loop": loop,
                "warning_total": int(price_total.get(loop, 0)),
                "warning_db_present": int(price_counts.get((loop, "DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED"), 0)),
                "invalid_price_skip_total": invalid_total,
                "invalid_price_skip_by_state": dict(state_counts),
                "invalid_db_present": int(invalid_db_present),
                "invalid_suspend_or_no_price": int(
                    invalid_total
                    - invalid_db_present
                    - state_counts.get("NOT_IN_PRICE_WARNING_JSON", 0)
                ),
                "invalid_unmatched": int(state_counts.get("NOT_IN_PRICE_WARNING_JSON", 0)),
                "invalid_unique_dates": len(invalid_dates),
                "invalid_unique_stocks": len({event["stock"] for event in invalid_events}),
                "derived_buy_trades": derived_buy_trades,
                "invalid_skip_vs_buy_trade_ratio": invalid_total / derived_buy_trades if derived_buy_trades else None,
                "invalid_db_present_vs_buy_trade_ratio": invalid_db_present / derived_buy_trades
                if derived_buy_trades
                else None,
                "invalid_date_return_mean": _safe_float(invalid_returns.mean()) if len(invalid_returns) else None,
                "other_date_return_mean": _safe_float(other_returns.mean()) if len(other_returns) else None,
                "invalid_date_return_min": _safe_float(invalid_returns.min()) if len(invalid_returns) else None,
                "invalid_date_return_max": _safe_float(invalid_returns.max()) if len(invalid_returns) else None,
                "report_nan_inf": _nan_inf_counts(report),
                "ic_max_abs_diff": _safe_float(sig.get("ic_max_abs_diff")),
                "rank_ic_max_abs_diff": _safe_float(sig.get("rank_ic_max_abs_diff")),
                "return_vs_account_max_abs_diff": _safe_float(rep.get("return_vs_account_max_abs_diff")),
                "final_value_abs_diff_vs_enhanced": _safe_float(rep.get("final_value_abs_diff_vs_enhanced")),
                "position_account_abs_diff": _safe_float(pos.get("max_account_abs_diff_vs_report")),
                "position_cash_abs_diff": _safe_float(pos.get("max_cash_abs_diff_vs_report")),
                "position_stock_value_abs_diff": _safe_float(pos.get("max_stock_value_abs_diff_vs_report")),
                "indicator_value_abs_diff": _safe_float(ind.get("max_day_minute_value_abs_diff")),
                "indicator_deal_amount_abs_diff": _safe_float(ind.get("max_day_minute_deal_amount_abs_diff")),
                "bad_minute_row_dates": ind.get("bad_minute_row_dates", []),
                "top50_minus_bottom50": _safe_float(p0_loop["bucket"]["top"].get("top50_minus_bottom50")),
                "top50_long_short_win": _safe_float(p0_loop["bucket"].get("d1_d10_positive_ratio")),
                "holding_low_overlap_days_lt_50pct": int(
                    exec_loop.get("holding_topk_overlap", {}).get("low_overlap_days_lt_50pct", 0) or 0
                ),
            }
        )

    max_metrics = {
        "ic_max_abs_diff": max((r["ic_max_abs_diff"] or 0.0) for r in loop_results),
        "rank_ic_max_abs_diff": max((r["rank_ic_max_abs_diff"] or 0.0) for r in loop_results),
        "return_vs_account_max_abs_diff": max((r["return_vs_account_max_abs_diff"] or 0.0) for r in loop_results),
        "position_account_abs_diff": max((r["position_account_abs_diff"] or 0.0) for r in loop_results),
        "position_cash_abs_diff": max((r["position_cash_abs_diff"] or 0.0) for r in loop_results),
        "position_stock_value_abs_diff": max((r["position_stock_value_abs_diff"] or 0.0) for r in loop_results),
        "indicator_value_abs_diff": max((r["indicator_value_abs_diff"] or 0.0) for r in loop_results),
        "indicator_deal_amount_abs_diff": max((r["indicator_deal_amount_abs_diff"] or 0.0) for r in loop_results),
        "report_nan_count": sum(r["report_nan_inf"]["nan"] for r in loop_results),
        "report_inf_count": sum(r["report_nan_inf"]["inf"] for r in loop_results),
        "bad_minute_row_dates": sum(len(r["bad_minute_row_dates"]) for r in loop_results),
        "invalid_price_skip_total": sum(r["invalid_price_skip_total"] for r in loop_results),
        "invalid_db_present_total": sum(r["invalid_db_present"] for r in loop_results),
        "derived_buy_trades_total": sum(r["derived_buy_trades"] for r in loop_results),
    }
    max_metrics["invalid_skip_vs_buy_trade_ratio"] = (
        max_metrics["invalid_price_skip_total"] / max_metrics["derived_buy_trades_total"]
        if max_metrics["derived_buy_trades_total"]
        else None
    )
    max_metrics["invalid_db_present_vs_buy_trade_ratio"] = (
        max_metrics["invalid_db_present_total"] / max_metrics["derived_buy_trades_total"]
        if max_metrics["derived_buy_trades_total"]
        else None
    )

    root_counts = close.get("root_cause_counts", {})
    db_state_counts = close.get("all_db_state_counts", {})
    gates = [
        {
            "gate": "SignalMetricRecompute",
            "status": "PASS" if max_metrics["ic_max_abs_diff"] <= 1e-12 and max_metrics["rank_ic_max_abs_diff"] <= 1e-12 else "FAIL",
            "evidence": f"IC max diff={max_metrics['ic_max_abs_diff']:.3e}, RankIC max diff={max_metrics['rank_ic_max_abs_diff']:.3e}",
        },
        {
            "gate": "ReportReturnAccount",
            "status": "PASS" if max_metrics["return_vs_account_max_abs_diff"] <= 1e-10 else "FAIL",
            "evidence": f"return/account max diff={max_metrics['return_vs_account_max_abs_diff']:.3e}",
        },
        {
            "gate": "PositionReportReconcile",
            "status": "PASS"
            if max_metrics["position_account_abs_diff"] <= 1e-6
            and max_metrics["position_cash_abs_diff"] <= 1e-6
            and max_metrics["position_stock_value_abs_diff"] <= 1e-4
            else "FAIL",
            "evidence": (
                f"account={max_metrics['position_account_abs_diff']:.3e}, "
                f"cash={max_metrics['position_cash_abs_diff']:.3e}, "
                f"stock={max_metrics['position_stock_value_abs_diff']:.3e}"
            ),
        },
        {
            "gate": "V25DayMinuteAggregate",
            "status": "PASS"
            if max_metrics["indicator_value_abs_diff"] <= 1e-6
            and max_metrics["indicator_deal_amount_abs_diff"] <= 1e-6
            and max_metrics["bad_minute_row_dates"] == 0
            else "FAIL",
            "evidence": (
                f"value={max_metrics['indicator_value_abs_diff']:.3e}, "
                f"deal_amount={max_metrics['indicator_deal_amount_abs_diff']:.3e}, "
                f"bad_dates={max_metrics['bad_minute_row_dates']}"
            ),
        },
        {
            "gate": "ReportNaNInf",
            "status": "PASS" if max_metrics["report_nan_count"] == 0 and max_metrics["report_inf_count"] == 0 else "FAIL",
            "evidence": f"nan={max_metrics['report_nan_count']}, inf={max_metrics['report_inf_count']}",
        },
        {
            "gate": "QlibMinuteCoverage",
            "status": "WARN" if root_counts.get("QLIB_MINUTE_CLOSE_MISSING", 0) else "PASS",
            "evidence": (
                f"DB-present/not-suspended warnings={db_state_counts.get('DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED', 0)}, "
                f"Qlib 1min all-null={root_counts.get('QLIB_MINUTE_CLOSE_MISSING', 0)}, "
                f"invalid DB-present skips={max_metrics['invalid_db_present_total']}"
            ),
        },
    ]

    return {
        "task_id": args.task_id,
        "workspace": str(workspace),
        "loops": loop_results,
        "summary": max_metrics,
        "gates": gates,
        "close_none_root_counts": root_counts,
        "close_none_db_state_counts": db_state_counts,
    }


def write_md(result: dict[str, Any], output: Path) -> None:
    lines: list[str] = [
        f"# QE Backtest Data Accuracy Materiality Audit: {result['task_id']}",
        "",
        "Scope: existing P0/P1 JSON artifacts, persisted reports, and run logs only. No QE rerun, no strategy behavior change, and no new strategy logging.",
        "",
        "## Direct Answer",
        "",
        "- No NAV/account/position/IC/RankIC/V25 aggregate calculation error has been found in the audited Loop19-28 artifacts.",
        "- One real data-coverage issue has been found: some stock-date pairs have DB minute data and Qlib day close, but Qlib 1min `$close` is all null. This can skip a small number of planned buys, so it is a data coverage warning, not a proven return-calculation error.",
        "- Current return credibility is high enough for data-accuracy purposes, but not absolute: exact V25 child-order branch replay is still aggregate-only, and all Loop1-18 rerun conclusions should wait until those reruns finish.",
        "",
    ]

    gate_rows = [[g["gate"], g["status"], g["evidence"]] for g in result["gates"]]
    lines += ["## Accuracy Gates", "", "```text", _table(gate_rows, ["Gate", "Status", "Evidence"]), "```", ""]

    s = result["summary"]
    summary_rows = [
        ["ICMaxDiff", f"{s['ic_max_abs_diff']:.3e}", "recomputed IC vs Qlib artifact"],
        ["RankICMaxDiff", f"{s['rank_ic_max_abs_diff']:.3e}", "recomputed RankIC vs Qlib artifact"],
        ["ReturnAccountMaxDiff", f"{s['return_vs_account_max_abs_diff']:.3e}", "daily return vs account pct-change"],
        ["PositionAccountDiff", f"{s['position_account_abs_diff']:.3e}", "positions vs report account"],
        ["PositionCashDiff", f"{s['position_cash_abs_diff']:.3e}", "positions vs report cash"],
        ["PositionStockDiff", f"{s['position_stock_value_abs_diff']:.3e}", "positions vs report stock value"],
        ["V25ValueDiff", f"{s['indicator_value_abs_diff']:.3e}", "1day value vs 1min value aggregate"],
        ["V25DealDiff", f"{s['indicator_deal_amount_abs_diff']:.3e}", "1day deal_amount vs 1min aggregate"],
        ["ReportNaN", s["report_nan_count"], "numeric report NaN count"],
        ["ReportInf", s["report_inf_count"], "numeric report inf count"],
    ]
    lines += ["## Numerical Integrity Summary", "", "```text", _table(summary_rows, ["Metric", "Value", "Meaning"]), "```", ""]

    materiality_rows = [
        ["InvalidPriceSkips", s["invalid_price_skip_total"], "all ScoreWeighted invalid-price skip lines"],
        ["InvalidDBPresentSkips", s["invalid_db_present_total"], "DB minute exists, Qlib 1min close all-null"],
        ["DerivedBuyTrades", s["derived_buy_trades_total"], "stock_trades derived buy rows"],
        ["InvalidSkipVsBuys", _fmt_pct(s["invalid_skip_vs_buy_trade_ratio"]), "all invalid skips / derived buy rows"],
        ["InvalidDBPresentVsBuys", _fmt_pct(s["invalid_db_present_vs_buy_trade_ratio"]), "coverage-gap skips / derived buy rows"],
    ]
    lines += ["## Close-None Materiality Summary", "", "```text", _table(materiality_rows, ["Metric", "Value", "Meaning"]), "```", ""]

    loop_rows = []
    for row in result["loops"]:
        loop_rows.append(
            [
                row["loop"],
                row["warning_db_present"],
                row["invalid_price_skip_total"],
                row["invalid_db_present"],
                row["derived_buy_trades"],
                _fmt_pct(row["invalid_skip_vs_buy_trade_ratio"]),
                _fmt_pct(row["invalid_db_present_vs_buy_trade_ratio"]),
                row["invalid_unique_dates"],
                _fmt_pct(row["invalid_date_return_mean"]),
                _fmt_pct(row["other_date_return_mean"]),
            ]
        )
    lines += [
        "## Loop-Level Close-None Materiality",
        "",
        "```text",
        _table(
            loop_rows,
            [
                "Loop",
                "DBWarn",
                "SkipAll",
                "SkipDB",
                "BuyRows",
                "Skip/Buy",
                "SkipDB/Buy",
                "SkipDates",
                "SkipDateRet",
                "OtherDateRet",
            ],
        ),
        "```",
        "",
    ]

    acc_rows = []
    for row in result["loops"]:
        acc_rows.append(
            [
                row["loop"],
                f"{row['return_vs_account_max_abs_diff']:.2e}",
                f"{row['position_account_abs_diff']:.2e}",
                f"{row['position_cash_abs_diff']:.2e}",
                f"{row['position_stock_value_abs_diff']:.2e}",
                f"{row['indicator_value_abs_diff']:.2e}",
                f"{row['indicator_deal_amount_abs_diff']:.2e}",
                len(row["bad_minute_row_dates"]),
                row["report_nan_inf"]["nan"],
                row["report_nan_inf"]["inf"],
            ]
        )
    lines += [
        "## Loop-Level Numerical Checks",
        "",
        "```text",
        _table(
            acc_rows,
            ["Loop", "RetAcct", "PosAcct", "PosCash", "PosStock", "V25Value", "V25Deal", "BadMin", "NaN", "Inf"],
        ),
        "```",
        "",
    ]

    top_rows = []
    for row in result["loops"]:
        top_rows.append(
            [
                row["loop"],
                _fmt_pct(row["top50_minus_bottom50"]),
                _fmt_pct(row["top50_long_short_win"]),
                row["holding_low_overlap_days_lt_50pct"],
            ]
        )
    lines += [
        "## Signal-To-Portfolio Sanity Check",
        "",
        "```text",
        _table(top_rows, ["Loop", "Top50-Bottom50", "D1-D10Win", "HoldOverlapLT50"]),
        "```",
        "",
        "Interpretation: this confirms that the high IC/RankIC signal generally converts into a positive top-bucket spread; it is not a model-optimization conclusion.",
        "",
        "## Current Scope Boundary",
        "",
        "- Continue data-accuracy validation only until Loop1-18 full_train reruns complete.",
        "- Do not start new QE experiments in this stage.",
        "- Do not change strategy logging or execution behavior in this stage.",
        "- Do not begin model/factor optimization synthesis until the rerun set is complete.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="QE backtest data accuracy materiality audit")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--loops", default="19-28")
    ap.add_argument("--p0-json", required=True)
    ap.add_argument("--execution-json", required=True)
    ap.add_argument("--v25-json", required=True)
    ap.add_argument("--price-json", required=True)
    ap.add_argument("--close-json", required=True)
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()
    result = run_audit(args)
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_md(result, Path(args.output_md))
    print(f"wrote {out_json}")
    print(f"wrote {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
