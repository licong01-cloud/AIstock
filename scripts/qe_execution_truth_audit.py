#!/usr/bin/env python
"""Read-only QE strategy/execution truth audit.

This script validates completed QE loop artifacts without rerunning QE. It is
intended to run in the WSL/rdagent environment because Qlib position pickles
contain Qlib classes.
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_yaml_lenient(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    # Some RD-Agent workspaces preserve Jinja placeholders in the model section.
    # The strategy/execution audit only needs concrete backtest sections, so
    # replace unresolved placeholders with a scalar to keep YAML parsing strict
    # without inventing business values.
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
    except Exception:
        return None


def _position_raw(pos: Any) -> dict[str, Any]:
    raw = getattr(pos, "position", None)
    if isinstance(raw, dict):
        return raw
    if isinstance(pos, dict):
        return pos
    raise TypeError(f"unsupported Position object: {type(pos)!r}")


def _position_summary(pos_dict: dict[pd.Timestamp, Any], report: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for dt, pos in sorted(pos_dict.items()):
        raw = _position_raw(pos)
        cash = _safe_float(raw.get("cash")) or 0.0
        nav = _safe_float(raw.get("now_account_value"))
        stock_count = 0
        stock_value = 0.0
        for sid, info in raw.items():
            if sid in {"cash", "now_account_value"} or not isinstance(info, dict):
                continue
            amount = _safe_float(info.get("amount")) or 0.0
            price = _safe_float(info.get("price")) or 0.0
            if amount > 1e-8:
                stock_count += 1
                stock_value += amount * price
        rows.append(
            {
                "datetime": pd.Timestamp(dt),
                "cash": cash,
                "stock_value": stock_value,
                "account": nav if nav is not None else cash + stock_value,
                "stock_count": stock_count,
            }
        )
    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    aligned = df.join(report[["account", "cash", "value"]].rename(
        columns={"account": "report_account", "cash": "report_cash", "value": "report_value"}
    ), how="inner")
    for col_a, col_b, out_col in [
        ("account", "report_account", "account_abs_diff"),
        ("cash", "report_cash", "cash_abs_diff"),
        ("stock_value", "report_value", "stock_value_abs_diff"),
    ]:
        aligned[out_col] = (aligned[col_a] - aligned[col_b]).abs()
    counts = df["stock_count"]
    return {
        "rows": int(len(df)),
        "min_holding_count": int(counts.min()) if len(counts) else None,
        "max_holding_count": int(counts.max()) if len(counts) else None,
        "avg_holding_count": float(counts.mean()) if len(counts) else None,
        "p95_holding_count": float(counts.quantile(0.95)) if len(counts) else None,
        "final_holding_count": int(counts.iloc[-1]) if len(counts) else None,
        "max_account_abs_diff_vs_report": float(aligned["account_abs_diff"].max()) if len(aligned) else None,
        "max_cash_abs_diff_vs_report": float(aligned["cash_abs_diff"].max()) if len(aligned) else None,
        "max_stock_value_abs_diff_vs_report": float(aligned["stock_value_abs_diff"].max()) if len(aligned) else None,
        "count_series": {d.strftime("%Y-%m-%d"): int(v) for d, v in counts.items()},
    }


def _standardize_pred(pred: pd.DataFrame | pd.Series) -> pd.DataFrame:
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    score_col = "score" if "score" in pred.columns else pred.columns[0]
    out = pred[[score_col]].rename(columns={score_col: "score"}).dropna()
    if not isinstance(out.index, pd.MultiIndex):
        raise ValueError("pred index must be MultiIndex(datetime, instrument)")
    return out


def _holding_overlap(
    pred: pd.DataFrame,
    pos_dict: dict[pd.Timestamp, Any],
    *,
    topk: int,
) -> dict[str, Any]:
    pred = _standardize_pred(pred)
    pred_dates = sorted(pd.Timestamp(x) for x in pred.index.get_level_values("datetime").unique())
    top_by_date: dict[pd.Timestamp, set[str]] = {}
    for dt, g in pred.groupby(level="datetime", sort=True):
        top_by_date[pd.Timestamp(dt)] = set(g.sort_values("score", ascending=False).head(topk).index.get_level_values("instrument"))

    rows: list[dict[str, Any]] = []
    pred_i = -1
    for dt, pos in sorted(pos_dict.items()):
        dt = pd.Timestamp(dt)
        while pred_i + 1 < len(pred_dates) and pred_dates[pred_i + 1] < dt:
            pred_i += 1
        if pred_i < 0:
            continue
        signal_dt = pred_dates[pred_i]
        raw = _position_raw(pos)
        holdings = {
            sid for sid, info in raw.items()
            if sid not in {"cash", "now_account_value"} and isinstance(info, dict) and (_safe_float(info.get("amount")) or 0.0) > 1e-8
        }
        if not holdings:
            continue
        top = top_by_date.get(signal_dt, set())
        inter = holdings & top
        rows.append({
            "date": dt.strftime("%Y-%m-%d"),
            "signal_date": signal_dt.strftime("%Y-%m-%d"),
            "holdings": len(holdings),
            "topk": len(top),
            "overlap": len(inter),
            "overlap_vs_holdings": len(inter) / len(holdings),
            "overlap_vs_topk": len(inter) / len(top) if top else None,
        })
    if not rows:
        return {"rows": 0}
    ratios = np.array([r["overlap_vs_holdings"] for r in rows], dtype=float)
    return {
        "rows": len(rows),
        "avg_overlap_vs_holdings": float(np.nanmean(ratios)),
        "median_overlap_vs_holdings": float(np.nanmedian(ratios)),
        "p10_overlap_vs_holdings": float(np.nanquantile(ratios, 0.10)),
        "low_overlap_days_lt_50pct": int((ratios < 0.5).sum()),
        "examples_low_overlap": sorted(rows, key=lambda r: r["overlap_vs_holdings"])[:5],
    }


def _indicator_consistency(artifact_dir: Path) -> dict[str, Any]:
    day_path = artifact_dir / "portfolio_analysis" / "indicators_normal_1day.pkl"
    min_path = artifact_dir / "portfolio_analysis" / "indicators_normal_1min.pkl"
    if not day_path.exists() or not min_path.exists():
        return {"available": False, "reason": "missing indicators_normal_1day.pkl or indicators_normal_1min.pkl"}
    day = _load_pickle(day_path)
    minute = _load_pickle(min_path)
    minute = minute.copy()
    minute["date"] = pd.to_datetime(minute.index).normalize()
    agg = minute.groupby("date").agg(
        minute_rows=("count", "size"),
        minute_deal_amount=("deal_amount", "sum"),
        minute_value=("value", "sum"),
        minute_child_count=("count", "sum"),
    )
    aligned = day.join(agg, how="left")
    aligned["deal_amount_abs_diff"] = (aligned["deal_amount"].fillna(0) - aligned["minute_deal_amount"].fillna(0)).abs()
    active = aligned[aligned["minute_rows"].fillna(0) > 0]
    row_counts = active["minute_rows"].value_counts().sort_index()
    anomalies = active[~active["minute_rows"].isin([240, 241])]
    return {
        "available": True,
        "day_rows": int(len(day)),
        "minute_rows": int(len(minute)),
        "minute_dates": int(len(active)),
        "minute_rows_by_date": {str(int(k)): int(v) for k, v in row_counts.items()},
        "bad_minute_row_dates": [
            {"date": d.strftime("%Y-%m-%d"), "rows": int(r["minute_rows"])}
            for d, r in anomalies.head(20).iterrows()
        ],
        "max_day_vs_minute_deal_amount_abs_diff": float(aligned["deal_amount_abs_diff"].max()),
        "sum_day_deal_amount": float(day["deal_amount"].sum()),
        "sum_minute_deal_amount": float(minute["deal_amount"].sum()),
    }


def _configured_costs(conf: dict[str, Any]) -> dict[str, float]:
    backtest = (((conf.get("port_analysis_config") or {}).get("backtest") or {}).get("exchange_kwargs") or {})
    return {
        "open_cost": float(backtest.get("open_cost") or 0.0),
        "close_cost": float(backtest.get("close_cost") or 0.0),
        "min_cost": float(backtest.get("min_cost") or 0.0),
    }


def _cost_overlay(enhanced: dict[str, Any], conf: dict[str, Any], report: pd.DataFrame) -> dict[str, Any]:
    costs = _configured_costs(conf)
    daily_cost: dict[pd.Timestamp, float] = defaultdict(float)
    trade_rows = 0
    buy_value = 0.0
    sell_value = 0.0
    for _sid, trades in (enhanced.get("stock_trades") or {}).items():
        if not isinstance(trades, list):
            continue
        for tr in trades:
            date = pd.Timestamp(tr.get("date")).normalize()
            typ = str(tr.get("type") or "").lower()
            value = _safe_float(tr.get("amount")) or 0.0
            if value <= 0:
                continue
            trade_rows += 1
            if typ == "buy":
                buy_value += value
                daily_cost[date] += max(value * costs["open_cost"], costs["min_cost"])
            elif typ == "sell":
                sell_value += value
                daily_cost[date] += max(value * costs["close_cost"], costs["min_cost"])
    cost_series = pd.Series(daily_cost, dtype=float).reindex(report.index, fill_value=0.0)
    report_cost = report["cost"].fillna(0.0) if "cost" in report else pd.Series(0.0, index=report.index)
    report_total_cost = report["total_cost"].fillna(0.0) if "total_cost" in report else pd.Series(0.0, index=report.index)
    prev_account = report["account"].shift(1).fillna(report["account"].iloc[0])
    adjusted_ret = report["return"].fillna(0.0) - cost_series / prev_account
    nav = (1.0 + adjusted_ret).cumprod()
    dd = nav / nav.cummax() - 1.0
    return {
        "open_cost": costs["open_cost"],
        "close_cost": costs["close_cost"],
        "min_cost": costs["min_cost"],
        "stock_trade_rows": trade_rows,
        "buy_value": buy_value,
        "sell_value": sell_value,
        "configured_cost_lower_bound": float(cost_series.sum()),
        "report_cost_sum": float(report_cost.sum()),
        "report_total_cost_last": float(report_total_cost.iloc[-1]) if len(report_total_cost) else None,
        "cost_metric_missing_from_report": bool(cost_series.sum() > 1e-6 and report_cost.sum() == 0.0),
        "posthoc_double_count_cost_cagr": float(nav.iloc[-1] ** (242 / max(len(nav), 1)) - 1.0),
        "posthoc_double_count_cost_mdd": float(dd.min()),
        "nav_cost_application_code_path": "Exchange.deal_order -> Account.update_order -> Position.update_order subtracts cost from cash; inner generate_portfolio_metrics=false prevents accum_info/report cost recording",
    }


def _strategy_config(conf: dict[str, Any], loop_dir: Path) -> dict[str, Any]:
    pac = conf.get("port_analysis_config") or {}
    strategy = pac.get("strategy") or {}
    executor = pac.get("executor") or {}
    kwargs = executor.get("kwargs") or {}
    inner = kwargs.get("inner_strategy") or {}
    inner_kwargs = inner.get("kwargs") or {}
    model_paths = {
        "early_model_path": inner_kwargs.get("early_model_path"),
        "late_model_path": inner_kwargs.get("late_model_path"),
    }
    code_text = (loop_dir / "tail_twap_v25_strategy.py").read_text(encoding="utf-8", errors="replace") if (loop_dir / "tail_twap_v25_strategy.py").exists() else ""
    tail_text = (loop_dir / "tail_twap_strategy.py").read_text(encoding="utf-8", errors="replace") if (loop_dir / "tail_twap_strategy.py").exists() else ""
    custom_text = (loop_dir / "custom_strategy.py").read_text(encoding="utf-8", errors="replace") if (loop_dir / "custom_strategy.py").exists() else ""
    log_text = (loop_dir / "run.log").read_text(encoding="utf-8", errors="replace") if (loop_dir / "run.log").exists() else ""
    return {
        "outer_strategy_class": strategy.get("class"),
        "outer_strategy_module": strategy.get("module_path"),
        "topk": (strategy.get("kwargs") or {}).get("topk"),
        "n_drop": (strategy.get("kwargs") or {}).get("n_drop"),
        "risk_degree": (strategy.get("kwargs") or {}).get("risk_degree"),
        "inner_strategy_class": inner.get("class"),
        "inner_strategy_module": inner.get("module_path"),
        "device": inner_kwargs.get("device"),
        "unfilled_handler": inner_kwargs.get("unfilled_handler"),
        "unfilled_backup_depth": inner_kwargs.get("unfilled_backup_depth"),
        "filter_suspended_on_signal": inner_kwargs.get("filter_suspended_on_signal"),
        "suspend_filter_file_exists": (loop_dir / str(inner_kwargs.get("suspend_filter_file") or "")).exists(),
        "early_model_path_exists": Path(str(model_paths["early_model_path"])).exists() if model_paths["early_model_path"] else False,
        "late_model_path_exists": Path(str(model_paths["late_model_path"])).exists() if model_paths["late_model_path"] else False,
        "v25_code_has_no_twap_fallback_guard": "refusing to fall back to TWAP" in code_text,
        "v25_code_has_weight_guard": "V25 plan weight mismatch" in code_text and "EARLY_WEIGHT = 0.8879" in code_text,
        "tail_substitute_code_present": "_do_realloc_substitute" in tail_text and "TAIL_SUBSTITUTE" in tail_text,
        "backup_candidate_code_present": "_backup_candidates" in custom_text,
        "runlog_v25_plan_lines": len(re.findall(r"generated plan stock=", log_text)),
        "runlog_tail_substitute_mentions": len(re.findall(r"TAIL_SUBSTITUTE", log_text)),
        "runlog_missing_data_errors": len(re.findall(r"missing_data_error", log_text, flags=re.IGNORECASE)),
    }


def _market_state_segments(report: pd.DataFrame) -> dict[str, Any]:
    bench = report["bench"].fillna(0.0)
    ret = report["return"].fillna(0.0)
    roll60 = (1.0 + bench).rolling(60, min_periods=20).apply(np.prod, raw=True) - 1.0
    vol20 = bench.rolling(20, min_periods=10).std()
    vol_med = float(vol20.median())
    state = pd.Series("sideways", index=report.index)
    state[roll60 > 0.05] = "bull"
    state[roll60 < -0.05] = "bear"
    vol_state = pd.Series("low_vol", index=report.index)
    vol_state[vol20 > vol_med] = "high_vol"
    rows = {}
    for name, mask in {
        "bull": state == "bull",
        "bear": state == "bear",
        "sideways": state == "sideways",
        "high_vol": vol_state == "high_vol",
        "low_vol": vol_state == "low_vol",
    }.items():
        r = ret[mask].dropna()
        if len(r) == 0:
            rows[name] = {"days": 0}
            continue
        nav = (1.0 + r).cumprod()
        rows[name] = {
            "days": int(len(r)),
            "return_total": float(nav.iloc[-1] - 1.0),
            "mean_daily_return": float(r.mean()),
            "sharpe": float(r.mean() / r.std() * math.sqrt(242)) if r.std() else None,
            "max_drawdown": float((nav / nav.cummax() - 1.0).min()),
            "win_rate": float((r > 0).mean()),
        }
    return {"vol20_median": vol_med, "segments": rows}


def audit_loop(workspace: Path, loop: int) -> dict[str, Any]:
    loop_dir = workspace / f"Loop{loop}"
    artifact_dir = _find_artifact_dir(loop_dir)
    conf = _read_yaml_lenient(loop_dir / "conf.yaml")
    enhanced = _read_json(loop_dir / "qlib_results_enhanced.json")
    report = _load_pickle(artifact_dir / "portfolio_analysis" / "report_normal_1day.pkl")
    positions = _load_pickle(artifact_dir / "portfolio_analysis" / "positions_normal_1day.pkl")
    pred = _load_pickle(artifact_dir / "pred.pkl")
    topk = int((((conf.get("port_analysis_config") or {}).get("strategy") or {}).get("kwargs") or {}).get("topk") or 50)
    return {
        "loop": loop,
        "strategy_config": _strategy_config(conf, loop_dir),
        "position_summary": _position_summary(positions, report),
        "holding_topk_overlap": _holding_overlap(pred, positions, topk=topk),
        "indicator_consistency": _indicator_consistency(artifact_dir),
        "configured_cost_overlay": _cost_overlay(enhanced, conf, report),
        "market_state_segments": _market_state_segments(report),
        "training": enhanced.get("training_diagnostics") or {},
        "trade_diagnostics": enhanced.get("trade_diagnostics") or {},
    }


def _fmt_pct(v: Any, width: int = 9) -> str:
    x = _safe_float(v)
    if x is None:
        return " " * (width - 2) + "NA"
    return f"{x * 100:>{width - 1}.2f}%"


def _fmt_num(v: Any, width: int = 8, digits: int = 3) -> str:
    x = _safe_float(v)
    if x is None:
        return " " * (width - 2) + "NA"
    return f"{x:>{width}.{digits}f}"


def _table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    out = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    out.append("  ".join("-" * w for w in widths))
    out.extend("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)) for row in rows)
    return "\n".join(out)


def write_md(task_id: str, audits: list[dict[str, Any]], output: Path) -> None:
    lines = [f"# QE Strategy and Execution Truth Audit: {task_id}", ""]
    lines.append("Scope: read-only validation from completed artifacts, config, code, and persisted report data. No QE task is rerun.")
    lines.append("")

    rows = []
    for a in audits:
        ps = a["position_summary"]
        ov = a["holding_topk_overlap"]
        rows.append([
            str(a["loop"]),
            str(ps["min_holding_count"]),
            _fmt_num(ps["avg_holding_count"], 7, 1),
            _fmt_num(ps["p95_holding_count"], 7, 1),
            str(ps["max_holding_count"]),
            _fmt_pct(ov.get("avg_overlap_vs_holdings"), 9),
            str(ov.get("low_overlap_days_lt_50pct")),
            _fmt_num(ps["max_account_abs_diff_vs_report"], 11, 3),
        ])
    lines += ["## P0 Daily Strategy / Position Truth", "", "```text", _table(rows, ["Loop", "MinPos", "AvgPos", "P95Pos", "MaxPos", "HoldTop50", "LowOvDays", "AcctDiff"]), "```", ""]

    rows = []
    for a in audits:
        c = a["configured_cost_overlay"]
        rows.append([
            str(a["loop"]),
            _fmt_num(c["open_cost"], 9, 6),
            _fmt_num(c["close_cost"], 9, 6),
            _fmt_num(c["configured_cost_lower_bound"], 14, 0),
            _fmt_num(c["report_cost_sum"], 10, 0),
            "MISSING" if c["cost_metric_missing_from_report"] else "OK",
            "CASH_COST",
            _fmt_pct(c["posthoc_double_count_cost_cagr"], 12),
        ])
    lines += [
        "## P0 Configured Cost Recording",
        "",
        "CostLB is a deterministic lower-bound magnitude from persisted stock_trades and configured open/close cost. It must not be subtracted again from NAV without a no-cost rerun, because Qlib code subtracts trade cost from Position cash even when report cost metrics are not recorded.",
        "",
        "```text",
        _table(rows, ["Loop", "OpenCost", "CloseCost", "CostLB", "ReportCost", "Metric", "CodePath", "DoubleCntCAGR"]),
        "```",
        "",
    ]

    rows = []
    for a in audits:
        ind = a["indicator_consistency"]
        rows.append([
            str(a["loop"]),
            str(ind.get("day_rows")),
            str(ind.get("minute_dates")),
            json.dumps(ind.get("minute_rows_by_date"), ensure_ascii=False),
            str(len(ind.get("bad_minute_row_dates") or [])),
            _fmt_num(ind.get("max_day_vs_minute_deal_amount_abs_diff"), 12, 2),
        ])
    lines += ["## P0 V25 Minute Indicator Consistency", "", "```text", _table(rows, ["Loop", "DayRows", "MinDates", "RowsByDate", "BadDates", "DealDiff"]), "```", ""]

    rows = []
    for a in audits:
        s = a["strategy_config"]
        rows.append([
            str(a["loop"]),
            str(s["inner_strategy_class"]),
            str(s["unfilled_handler"]),
            str(s["suspend_filter_file_exists"]),
            str(s["early_model_path_exists"] and s["late_model_path_exists"]),
            str(s["v25_code_has_no_twap_fallback_guard"]),
            str(s["tail_substitute_code_present"] and s["backup_candidate_code_present"]),
            str(s["runlog_v25_plan_lines"]),
            str(s["runlog_missing_data_errors"]),
        ])
    lines += ["## P0 V25 / Tail Substitute Evidence Gate", "", "```text", _table(rows, ["Loop", "InnerClass", "TailMode", "SuspendFile", "Models", "NoFallback", "TailCode", "PlanLogs", "DataErrLogs"]), "```", ""]

    rows = []
    for a in audits:
        seg = (a["market_state_segments"] or {}).get("segments") or {}
        for state in ["bull", "bear", "sideways", "high_vol", "low_vol"]:
            r = seg.get(state) or {}
            rows.append([
                str(a["loop"]),
                state,
                str(r.get("days")),
                _fmt_pct(r.get("return_total"), 9),
                _fmt_num(r.get("sharpe"), 8, 3),
                _fmt_pct(r.get("max_drawdown"), 9),
                _fmt_pct(r.get("win_rate"), 9),
            ])
    lines += ["## P1 Market-State Segments", "", "States use report benchmark data: 60D benchmark return for bull/bear/sideways and 20D benchmark volatility median for high/low vol.", "", "```text", _table(rows, ["Loop", "State", "Days", "Return", "Sharpe", "MDD", "WinRate"]), "```", ""]

    rows = []
    for a in audits:
        t = a.get("training") or {}
        rows.append([
            str(a["loop"]),
            str(t.get("total_epochs")),
            str(t.get("best_epoch")),
            str(t.get("early_stop_triggered")),
            str(t.get("training_failed")),
            _fmt_num(t.get("final_val_loss"), 8, 6),
            "NO_SEED_EVIDENCE",
        ])
    lines += ["## P1 Training / Seed Evidence", "", "```text", _table(rows, ["Loop", "Epochs", "BestEp", "EarlyStop", "TrainFailFlag", "FinalVal", "SeedStatus"]), "```", ""]

    lines += [
        "## Evidence Notes",
        "",
        "- Position/account/cash checks are recomputed from `positions_normal_1day.pkl` and compared to `report_normal_1day.pkl`.",
        "- Holding overlap uses previous trading day's prediction because the outer daily strategy requests signal with `shift=1`.",
        "- V25 order-level plan/no-fill traces are not persisted in the current artifacts; `PlanLogs=0` means plan generation cannot be independently replay-audited from logs.",
        "- Report cost fields are zero even though config has non-zero open/close costs; code inspection shows this is a cost metric recording gap caused by inner executor `generate_portfolio_metrics=false`, not proof that NAV ignored costs.",
        "- The `DoubleCntCAGR` column is shown only as a guardrail: subtracting CostLB from report returns would double-count costs unless a dedicated no-cost rerun proves otherwise.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only QE strategy/execution truth audit")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--loops", default="19-28")
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()
    workspace = Path(args.workspace) if args.workspace else Path("/mnt/f/Dev/RD-Agent-main/qe_workspace") / args.task_id
    audits = [audit_loop(workspace, loop) for loop in _parse_loops(args.loops)]
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({"task_id": args.task_id, "workspace": str(workspace), "loops": audits}, ensure_ascii=False, indent=2), encoding="utf-8")
    write_md(args.task_id, audits, Path(args.output_md))
    print(f"wrote {out_json}")
    print(f"wrote {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
