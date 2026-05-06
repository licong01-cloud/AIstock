#!/usr/bin/env python
"""Read-only V25/QE existing-artifact audit.

This script does not rerun Qlib backtests and does not modify QE workspaces.
It validates what can be proven from persisted artifacts:

* 1min vs 1day indicator aggregation for quantity/value fields.
* Intraday execution distribution by time window.
* Derived stock_trades consistency against daily parent-order counts.
* Replay readiness and exact branch-trace availability.

The exact V25 child-order branch path remains unverifiable if plan/no-fill/
tail-substitute event rows were not persisted by the original run.
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
import re
from collections import Counter
from datetime import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


CLOSE_NONE_RE = re.compile(r"\$close\):\s*None")
PLAN_TRACE_RE = re.compile(r"\b(V25 plan|plan weight|plan hash|early_model|late_model)\b", re.IGNORECASE)
BRANCH_TRACE_RE = re.compile(
    r"\b(no_fill|limit_up_buy_blocked|limit_down_sell_blocked|tail substitute|TAIL_SUBSTITUTE|boost fallback|backup candidate)\b",
    re.IGNORECASE,
)


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required JSON artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"missing required text artifact: {path}")
    return path.read_text(encoding="utf-8", errors="replace")


def _read_yaml_lenient(path: Path) -> dict[str, Any]:
    text = _read_text(path)
    text = re.sub(r"\{\{\s*[^{}]+\s*\}\}", "0", text)
    return yaml.safe_load(text) or {}


def _find_artifact_dir(loop_dir: Path) -> Path:
    candidates = list(loop_dir.glob("mlruns/*/*/artifacts/pred.pkl"))
    if not candidates:
        raise FileNotFoundError(f"missing mlruns artifacts/pred.pkl under {loop_dir}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0].parent


def _parse_loops(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(dict.fromkeys(out))


def _safe_float(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _fmt_num(v: Any, digits: int = 3) -> str:
    x = _safe_float(v)
    if x is None:
        return "NA"
    return f"{x:.{digits}f}"


def _fmt_pct(v: Any, digits: int = 2) -> str:
    x = _safe_float(v)
    if x is None:
        return "NA"
    return f"{x * 100:.{digits}f}%"


def _table(rows: list[list[Any]], headers: list[str]) -> str:
    str_rows = [[str(c) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    lines.append("  ".join("-" * w for w in widths))
    lines.extend("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in str_rows)
    return "\n".join(lines)


def _time_window(ts: pd.Timestamp) -> str:
    t = ts.time()
    if t == time(9, 30):
        return "open_0930"
    if time(9, 31) <= t <= time(11, 30):
        return "morning_0931_1130"
    if time(13, 0) <= t <= time(14, 29):
        return "afternoon_1300_1429"
    if time(14, 30) <= t <= time(14, 54):
        return "tail_1430_1454"
    if time(14, 55) <= t <= time(15, 0):
        return "close_1455_1500"
    return "other"


def _indicator_profile(artifact_dir: Path) -> dict[str, Any]:
    pa_dir = artifact_dir / "portfolio_analysis"
    minute = _load_pickle(pa_dir / "indicators_normal_1min.pkl").copy()
    day = _load_pickle(pa_dir / "indicators_normal_1day.pkl").copy()
    minute.index = pd.to_datetime(minute.index)
    day.index = pd.to_datetime(day.index).normalize()
    minute["date"] = minute.index.normalize()
    minute["window"] = [_time_window(pd.Timestamp(x)) for x in minute.index]
    minute["active"] = (minute["value"].fillna(0).abs() > 0) | (minute["deal_amount"].fillna(0).abs() > 0) | (minute["count"].fillna(0) > 0)

    agg = minute.groupby("date").agg(
        minute_rows=("value", "size"),
        minute_active_rows=("active", "sum"),
        minute_value=("value", "sum"),
        minute_deal_amount=("deal_amount", "sum"),
    )
    aligned = day.join(agg, how="left")
    aligned["value_diff"] = aligned["value"].fillna(0) - aligned["minute_value"].fillna(0)
    aligned["deal_amount_diff"] = aligned["deal_amount"].fillna(0) - aligned["minute_deal_amount"].fillna(0)
    active_days = aligned[aligned["minute_rows"].fillna(0) > 0]

    window_rows: list[dict[str, Any]] = []
    total_value = float(minute["value"].fillna(0).sum())
    total_count_sum = float(minute["count"].fillna(0).sum())
    for window, grp in minute.groupby("window", sort=False):
        value_sum = float(grp["value"].fillna(0).sum())
        count_sum = float(grp["count"].fillna(0).sum())
        window_rows.append(
            {
                "window": window,
                "rows": int(len(grp)),
                "active_rows": int(grp["active"].sum()),
                "value_sum": value_sum,
                "value_ratio": value_sum / total_value if total_value else None,
                "count_sum": count_sum,
                "count_ratio": count_sum / total_count_sum if total_count_sum else None,
            }
        )

    daily_window = minute.groupby(["date", "window"])["value"].sum().unstack(fill_value=0)
    daily_total = daily_window.sum(axis=1)
    tail_value = daily_window.get("tail_1430_1454", pd.Series(0, index=daily_window.index)) + daily_window.get(
        "close_1455_1500", pd.Series(0, index=daily_window.index)
    )
    tail_ratio = tail_value / daily_total.replace(0, np.nan)
    max_minute_value_ratio = (
        minute.groupby("date")["value"].max() / minute.groupby("date")["value"].sum().replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan)

    ffr = minute["ffr"].dropna() if "ffr" in minute.columns else pd.Series(dtype=float)
    pa = minute["pa"].dropna() if "pa" in minute.columns else pd.Series(dtype=float)
    pos = minute["pos"].dropna() if "pos" in minute.columns else pd.Series(dtype=float)
    ffr_out_of_range = int(((ffr < -1e-12) | (ffr > 1 + 1e-12)).sum()) if len(ffr) else 0

    return {
        "minute_rows": int(len(minute)),
        "minute_active_dates": int(len(active_days)),
        "minute_rows_by_date": {str(int(k)): int(v) for k, v in active_days["minute_rows"].value_counts().sort_index().items()},
        "bad_minute_row_dates": [
            {"date": idx.strftime("%Y-%m-%d"), "rows": int(row["minute_rows"])}
            for idx, row in active_days[~active_days["minute_rows"].isin([240, 241])].iterrows()
        ],
        "max_day_minute_value_abs_diff": float(aligned["value_diff"].abs().max()),
        "max_day_minute_deal_amount_abs_diff": float(aligned["deal_amount_diff"].abs().max()),
        "sum_value": total_value,
        "sum_deal_amount": float(minute["deal_amount"].fillna(0).sum()),
        "window_profile": sorted(window_rows, key=lambda r: r["window"]),
        "tail_proxy": {
            "tail_1430_1500_value_ratio_total": float(tail_value.sum() / daily_total.sum()) if daily_total.sum() else None,
            "tail_active_dates": int((tail_value > 0).sum()),
            "max_daily_tail_ratio": float(tail_ratio.max()) if len(tail_ratio.dropna()) else None,
            "p95_daily_tail_ratio": float(tail_ratio.quantile(0.95)) if len(tail_ratio.dropna()) else None,
            "max_minute_value_ratio": float(max_minute_value_ratio.max()) if len(max_minute_value_ratio.dropna()) else None,
            "p95_max_minute_value_ratio": float(max_minute_value_ratio.quantile(0.95)) if len(max_minute_value_ratio.dropna()) else None,
        },
        "ffr_pa_pos": {
            "ffr_count": int(len(ffr)),
            "ffr_min": float(ffr.min()) if len(ffr) else None,
            "ffr_max": float(ffr.max()) if len(ffr) else None,
            "ffr_out_of_range": ffr_out_of_range,
            "pa_min": float(pa.min()) if len(pa) else None,
            "pa_max": float(pa.max()) if len(pa) else None,
            "pos_min": float(pos.min()) if len(pos) else None,
            "pos_max": float(pos.max()) if len(pos) else None,
        },
    }


def _stock_trades_df(enhanced: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for stock, trades in (enhanced.get("stock_trades") or {}).items():
        for trade in trades:
            rows.append(
                {
                    "stock": stock,
                    "date": pd.Timestamp(trade["date"]).normalize(),
                    "side": trade.get("type"),
                    "amount": float(trade.get("amount") or 0.0),
                    "price": _safe_float(trade.get("price")),
                    "pnl": _safe_float(trade.get("pnl")),
                }
            )
    return pd.DataFrame(rows)


def _stock_trades_reconciliation(artifact_dir: Path, enhanced: dict[str, Any]) -> dict[str, Any]:
    day = _load_pickle(artifact_dir / "portfolio_analysis" / "indicators_normal_1day.pkl").copy()
    day.index = pd.to_datetime(day.index).normalize()
    trades = _stock_trades_df(enhanced)
    if trades.empty:
        raise ValueError("enhanced stock_trades is empty; cannot reconcile derived trade summary")

    by_date = trades.groupby("date").agg(
        stock_trade_count=("amount", "size"),
        stock_trade_amount=("amount", "sum"),
        buy_count=("side", lambda s: int((s == "buy").sum())),
        sell_count=("side", lambda s: int((s == "sell").sum())),
    )
    aligned = day.join(by_date, how="outer")
    aligned[["stock_trade_count", "stock_trade_amount", "buy_count", "sell_count"]] = aligned[
        ["stock_trade_count", "stock_trade_amount", "buy_count", "sell_count"]
    ].fillna(0)
    aligned["count_diff"] = aligned["count"].fillna(0) - aligned["stock_trade_count"].fillna(0)
    aligned["value_diff_vs_stock_trades"] = aligned["value"].fillna(0) - aligned["stock_trade_amount"].fillna(0)
    aligned["value_rel_diff_vs_stock_trades"] = (
        aligned["value_diff_vs_stock_trades"].abs() / aligned["value"].abs().replace(0, np.nan)
    )

    side_counts = Counter(trades["side"].astype(str))
    return {
        "trade_rows": int(len(trades)),
        "stocks": int(trades["stock"].nunique()),
        "side_counts": dict(side_counts),
        "indicator_count_vs_stock_trades_max_abs_diff": float(aligned["count_diff"].abs().max()),
        "indicator_count_diff_dates": int((aligned["count_diff"].abs() > 1e-9).sum()),
        "indicator_value_vs_stock_trades_max_abs_diff": float(aligned["value_diff_vs_stock_trades"].abs().max()),
        "indicator_value_vs_stock_trades_median_rel_diff": float(aligned["value_rel_diff_vs_stock_trades"].median()),
        "indicator_value_vs_stock_trades_p95_rel_diff": float(aligned["value_rel_diff_vs_stock_trades"].quantile(0.95)),
        "indicator_value_vs_stock_trades_max_rel_diff": float(aligned["value_rel_diff_vs_stock_trades"].max()),
        "note": "stock_trades is derived from daily position changes, while indicators.value/count are execution aggregates; derived counts and values can differ and are not the authoritative child-order ledger.",
    }


def _extract_strategy_kwargs(conf: dict[str, Any]) -> dict[str, Any]:
    def walk(obj: Any) -> dict[str, Any] | None:
        if isinstance(obj, dict):
            if obj.get("class") == "TailTWAPWithV25TwoStageStrategy":
                return obj.get("kwargs") or {}
            for v in obj.values():
                found = walk(v)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for v in obj:
                found = walk(v)
                if found is not None:
                    return found
        return None

    return walk(conf) or {}


def _replay_readiness(loop_dir: Path, artifact_dir: Path, conf: dict[str, Any], run_log: str) -> dict[str, Any]:
    strategy_kwargs = _extract_strategy_kwargs(conf)
    required = {
        "conf_yaml": loop_dir / "conf.yaml",
        "tail_twap_v25_strategy": loop_dir / "tail_twap_v25_strategy.py",
        "tail_twap_strategy": loop_dir / "tail_twap_strategy.py",
        "custom_strategy": loop_dir / "custom_strategy.py",
        "pred_pkl": artifact_dir / "pred.pkl",
        "label_pkl": artifact_dir / "label.pkl",
        "indicators_1min": artifact_dir / "portfolio_analysis" / "indicators_normal_1min.pkl",
        "indicators_1day": artifact_dir / "portfolio_analysis" / "indicators_normal_1day.pkl",
        "positions_1day": artifact_dir / "portfolio_analysis" / "positions_normal_1day.pkl",
        "report_1day": artifact_dir / "portfolio_analysis" / "report_normal_1day.pkl",
    }
    early_model = strategy_kwargs.get("early_model_path")
    late_model = strategy_kwargs.get("late_model_path")
    if early_model:
        required["early_model_path"] = Path(str(early_model))
    if late_model:
        required["late_model_path"] = Path(str(late_model))

    required_status = {k: p.exists() for k, p in required.items()}
    missing = [k for k, exists in required_status.items() if not exists]
    plan_trace_hits = len(PLAN_TRACE_RE.findall(run_log))
    branch_trace_hits = len(BRANCH_TRACE_RE.findall(run_log))
    close_none_warning_count = len(CLOSE_NONE_RE.findall(run_log))
    exact_branch_trace_available = branch_trace_hits > 0 and plan_trace_hits > 0

    return {
        "required_status": required_status,
        "missing_required": missing,
        "early_model_path": str(early_model) if early_model else None,
        "late_model_path": str(late_model) if late_model else None,
        "plan_trace_hits_in_run_log": int(plan_trace_hits),
        "branch_trace_hits_in_run_log": int(branch_trace_hits),
        "close_none_warning_count": int(close_none_warning_count),
        "exact_branch_trace_available": bool(exact_branch_trace_available),
        "replay_level": "AGGREGATE_VERIFIABLE_ONLY" if not exact_branch_trace_available else "BRANCH_TRACE_PRESENT",
    }


def audit_loop(workspace: Path, loop: int) -> dict[str, Any]:
    loop_dir = workspace / f"Loop{loop}"
    if not loop_dir.exists():
        raise FileNotFoundError(f"loop directory does not exist: {loop_dir}")
    artifact_dir = _find_artifact_dir(loop_dir)
    enhanced = _read_json(loop_dir / "qlib_results_enhanced.json")
    conf = _read_yaml_lenient(loop_dir / "conf.yaml")
    run_log = _read_text(loop_dir / "run.log")
    return {
        "loop": loop,
        "loop_dir": str(loop_dir),
        "artifact_dir": str(artifact_dir),
        "indicator_profile": _indicator_profile(artifact_dir),
        "stock_trades_reconciliation": _stock_trades_reconciliation(artifact_dir, enhanced),
        "replay_readiness": _replay_readiness(loop_dir, artifact_dir, conf, run_log),
    }


def write_md(result: dict[str, Any], output: Path) -> None:
    lines: list[str] = [
        f"# P0/P1 QE V25 Existing Artifact Audit: {result['task_id']}",
        "",
        "Scope: read-only analysis of completed QE artifacts only. No QE task is rerun and no workspace artifact is modified.",
        "",
    ]

    rows = []
    for audit in result["loops"]:
        ind = audit["indicator_profile"]
        tail = ind["tail_proxy"]
        fpp = ind["ffr_pa_pos"]
        rows.append(
            [
                audit["loop"],
                ind["minute_rows"],
                ind["minute_active_dates"],
                json.dumps(ind["minute_rows_by_date"], ensure_ascii=False),
                len(ind["bad_minute_row_dates"]),
                _fmt_num(ind["max_day_minute_value_abs_diff"], 6),
                _fmt_num(ind["max_day_minute_deal_amount_abs_diff"], 6),
                fpp["ffr_out_of_range"],
                _fmt_pct(tail["tail_1430_1500_value_ratio_total"]),
                _fmt_pct(tail["max_daily_tail_ratio"]),
            ]
        )
    lines += [
        "## P0 Minute Indicator Truth",
        "",
        "```text",
        _table(
            rows,
            [
                "Loop",
                "MinRows",
                "Dates",
                "RowsByDate",
                "BadDates",
                "MaxValueDiff",
                "MaxAmountDiff",
                "FFROut",
                "TailValue%",
                "MaxDayTail%",
            ],
        ),
        "```",
        "",
    ]

    rows = []
    for audit in result["loops"]:
        for row in audit["indicator_profile"]["window_profile"]:
            rows.append(
                [
                    audit["loop"],
                    row["window"],
                    row["rows"],
                    row["active_rows"],
                    _fmt_pct(row["value_ratio"]),
                    _fmt_pct(row["count_ratio"]),
                ]
            )
    lines += [
        "## P0 Intraday Window Distribution",
        "",
        "```text",
        _table(rows, ["Loop", "Window", "Rows", "ActiveRows", "ValueRatio", "CountRatio"]),
        "```",
        "",
    ]

    rows = []
    for audit in result["loops"]:
        rec = audit["stock_trades_reconciliation"]
        rows.append(
            [
                audit["loop"],
                rec["trade_rows"],
                rec["stocks"],
                json.dumps(rec["side_counts"], ensure_ascii=False),
                _fmt_num(rec["indicator_count_vs_stock_trades_max_abs_diff"], 2),
                rec["indicator_count_diff_dates"],
                _fmt_pct(rec["indicator_value_vs_stock_trades_median_rel_diff"]),
                _fmt_pct(rec["indicator_value_vs_stock_trades_p95_rel_diff"]),
                _fmt_pct(rec["indicator_value_vs_stock_trades_max_rel_diff"]),
            ]
        )
    lines += [
        "## P0 Stock Trades Derived-Summary Reconciliation",
        "",
        "```text",
        _table(
            rows,
            [
                "Loop",
                "Rows",
                "Stocks",
                "Sides",
                "MaxCountDiff",
                "CountDiffDays",
                "MedianValueDiff%",
                "P95ValueDiff%",
                "MaxValueDiff%",
            ],
        ),
        "```",
        "",
    ]

    rows = []
    for audit in result["loops"]:
        rr = audit["replay_readiness"]
        rows.append(
            [
                audit["loop"],
                len(rr["missing_required"]),
                rr["plan_trace_hits_in_run_log"],
                rr["branch_trace_hits_in_run_log"],
                rr["close_none_warning_count"],
                rr["exact_branch_trace_available"],
                rr["replay_level"],
            ]
        )
    lines += [
        "## P0 V25 Offline Replay Readiness",
        "",
        "```text",
        _table(
            rows,
            [
                "Loop",
                "MissingReq",
                "PlanTraceHits",
                "BranchTraceHits",
                "CloseNoneWarn",
                "ExactBranchTrace",
                "ReplayLevel",
            ],
        ),
        "```",
        "",
    ]

    lines += [
        "## Evidence Notes",
        "",
        "- `indicators_normal_1min.pkl` and `indicators_normal_1day.pkl` aggregate exactly for both `value` and `deal_amount` when MaxValueDiff/MaxAmountDiff are zero.",
        "- Qlib indicator `deal_amount` is a traded quantity-style field; monetary turnover is represented by `value`.",
        "- `stock_trades` in `qlib_results_enhanced.json` is derived from daily position changes. Its count and amount can differ from execution `count`/`value`; it is useful for stock-level attribution, not as the authoritative child-order ledger.",
        "- V25 exact branch replay requires persisted plan/no-fill/tail-substitute event rows. If ExactBranchTrace is false, only aggregate execution truth is verifiable from current artifacts.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only V25/QE existing-artifact audit")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--loops", default="19-28")
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()

    workspace = Path(args.workspace) if args.workspace else Path("/mnt/f/Dev/RD-Agent-main/qe_workspace") / args.task_id
    audits = [audit_loop(workspace, loop) for loop in _parse_loops(args.loops)]
    result = {"task_id": args.task_id, "workspace": str(workspace), "loops": audits}

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_md(result, Path(args.output_md))
    print(f"wrote {out_json}")
    print(f"wrote {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
