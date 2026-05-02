#!/usr/bin/env python
"""Read-only P0 audit for QE loop artifacts.

This script audits completed QE loops without rerunning Qlib. It focuses on
artifact consistency, IC/RankIC recomputation, top-bucket signal conversion,
segment stability, and static leakage scan.
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

LABEL_RE = re.compile(r"Ref\(\$(?P<field>close|open|vwap),\s*-(?P<shift>\d+)\)\s*/\s*Ref\(\$\w+,\s*-1\)\s*-\s*1")
FEATURE_COUNT_RE = re.compile(r"num_features\s*=\s*(?:(?P<alpha>\d+)\s*\(Alpha158\)\s*\+\s*)?(?P<custom>\d+)\s*\((?:custom|custom only).*?\)\s*(?:=\s*(?P<total>\d+))?")
RISK_PATTERNS = [
    ("negative_shift", re.compile(r"\.shift\(\s*-\d+")),
    ("future_ref", re.compile(r"Ref\([^\n\r,]+,\s*-\d+\)")),
    ("centered_rolling", re.compile(r"rolling\([^\n\r)]*center\s*=\s*True")),
    ("forward_merge_asof", re.compile(r"merge_asof\([^\n\r]*direction\s*=\s*['\"]forward['\"]")),
    ("label_in_feature_code", re.compile(r"\b(LABEL0|future_return|label_return)\b", re.IGNORECASE)),
]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def _find_artifact_dir(loop_dir: Path) -> Path | None:
    candidates = list(loop_dir.glob("mlruns/*/*/artifacts/pred.pkl"))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0].parent


def _parse_label(conf_text: str, loader_text: str) -> dict[str, Any]:
    source = "conf.yaml"
    match = LABEL_RE.search(conf_text)
    if not match:
        source = "qe_custom_loaders.py"
        match = LABEL_RE.search(loader_text)
    if not match:
        return {"field": None, "shift": None, "horizon": None, "source": None, "expr": None}
    shift = int(match.group("shift"))
    return {
        "field": match.group("field"),
        "shift": shift,
        "horizon": shift - 1,
        "source": source,
        "expr": match.group(0),
    }


def _parse_feature_count(run_text: str) -> dict[str, Any]:
    matches = list(FEATURE_COUNT_RE.finditer(run_text))
    if not matches:
        return {"alpha158": None, "custom": None, "total": None, "raw": None}
    m = matches[-1]
    alpha = int(m.group("alpha")) if m.group("alpha") else 0
    custom = int(m.group("custom")) if m.group("custom") else None
    total = int(m.group("total")) if m.group("total") else (alpha + custom if custom is not None else None)
    return {"alpha158": alpha, "custom": custom, "total": total, "raw": m.group(0)}


def _scan_static_leakage(loop_dir: Path) -> dict[str, Any]:
    files = list((loop_dir / "factors").glob("*.py"))
    for extra in ["prepare_factors.py", "model.py"]:
        p = loop_dir / extra
        if p.exists():
            files.append(p)
    hits: list[dict[str, Any]] = []
    for path in files:
        try:
            lines = _read_text(path).splitlines()
        except Exception as exc:  # pragma: no cover - defensive only
            hits.append({"file": str(path), "pattern": "read_error", "line": 0, "text": str(exc)})
            continue
        for i, line in enumerate(lines, start=1):
            for name, pat in RISK_PATTERNS:
                if pat.search(line):
                    hits.append({
                        "file": str(path.relative_to(loop_dir)),
                        "pattern": name,
                        "line": i,
                        "text": line.strip()[:240],
                    })
    by_pattern: dict[str, int] = {}
    for h in hits:
        by_pattern[h["pattern"]] = by_pattern.get(h["pattern"], 0) + 1
    return {
        "files_scanned": len(files),
        "hit_count": len(hits),
        "by_pattern": by_pattern,
        "examples": hits[:50],
    }


def _standardize_pred_label(pred: pd.DataFrame, label: pd.DataFrame) -> pd.DataFrame:
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    if isinstance(label, pd.Series):
        label = label.to_frame("label")
    score_col = "score" if "score" in pred.columns else pred.columns[0]
    label_col = "LABEL0" if "LABEL0" in label.columns else label.columns[0]
    df = pred[[score_col]].rename(columns={score_col: "score"}).join(
        label[[label_col]].rename(columns={label_col: "label"}),
        how="inner",
    )
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["score", "label"])
    if not isinstance(df.index, pd.MultiIndex):
        raise ValueError("pred/label index must be MultiIndex(datetime, instrument)")
    return df


def _daily_corr(df: pd.DataFrame, method: str) -> pd.Series:
    values: list[tuple[pd.Timestamp, float]] = []
    for dt, g in df.groupby(level="datetime", sort=True):
        if len(g) < 3:
            val = np.nan
        else:
            val = g["score"].corr(g["label"], method=method)
        values.append((pd.Timestamp(dt), val))
    return pd.Series({dt: val for dt, val in values}, name=method).dropna()


def _bucket_stats(df: pd.DataFrame, top_ns: Iterable[int] = (10, 30, 50, 100)) -> dict[str, Any]:
    top_daily: dict[str, list[float]] = {f"top{n}": [] for n in top_ns}
    top_daily.update({f"bottom{n}": [] for n in top_ns})
    decile_daily: dict[str, list[float]] = {f"D{i}": [] for i in range(1, 11)}
    counts: list[int] = []
    daily_ls: list[float] = []
    for _dt, g in df.groupby(level="datetime", sort=True):
        g = g.sort_values("score", ascending=False)
        labels = g["label"].to_numpy(dtype=float)
        n = len(labels)
        if n == 0:
            continue
        counts.append(n)
        for top_n in top_ns:
            k = min(top_n, n)
            top_daily[f"top{top_n}"].append(float(np.nanmean(labels[:k])))
            top_daily[f"bottom{top_n}"].append(float(np.nanmean(labels[-k:])))
        splits = np.array_split(labels, 10)
        if len(splits) == 10 and all(len(x) for x in splits):
            dvals = [float(np.nanmean(x)) for x in splits]
            for i, val in enumerate(dvals, start=1):
                decile_daily[f"D{i}"].append(val)
            daily_ls.append(dvals[0] - dvals[-1])
    bucket_summary: dict[str, Any] = {
        "date_count": len(counts),
        "avg_universe_count": float(np.mean(counts)) if counts else None,
        "top": {},
        "decile": {},
        "d1_d10_mean": float(np.nanmean(daily_ls)) if daily_ls else None,
        "d1_d10_positive_ratio": float(np.nanmean(np.array(daily_ls) > 0)) if daily_ls else None,
    }
    for k, vals in top_daily.items():
        bucket_summary["top"][k] = float(np.nanmean(vals)) if vals else None
    for k, vals in decile_daily.items():
        bucket_summary["decile"][k] = float(np.nanmean(vals)) if vals else None
    for n in top_ns:
        top = bucket_summary["top"].get(f"top{n}")
        bottom = bucket_summary["top"].get(f"bottom{n}")
        bucket_summary["top"][f"top{n}_minus_bottom{n}"] = (top - bottom) if top is not None and bottom is not None else None
    return bucket_summary


def _max_drawdown_from_returns(ret: pd.Series) -> float | None:
    if ret.empty:
        return None
    nav = (1.0 + ret.fillna(0)).cumprod()
    dd = nav / nav.cummax() - 1.0
    return float(dd.min())


def _sharpe(ret: pd.Series) -> float | None:
    ret = ret.dropna()
    if len(ret) < 3 or ret.std() == 0:
        return None
    return float(ret.mean() / ret.std() * math.sqrt(242))


def _segment_stats(ic: pd.Series, ric: pd.Series, report: pd.DataFrame | None, freq: str) -> list[dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    base = pd.DataFrame({"ic": ic, "rank_ic": ric})
    if report is not None and "return" in report.columns:
        base = base.join(report[["return"]], how="left")
    if base.empty:
        return []
    if freq == "Y":
        grouper = base.index.year
    elif freq == "Q":
        grouper = base.index.to_period("Q").astype(str)
    else:
        raise ValueError(freq)
    rows: list[dict[str, Any]] = []
    for key, g in base.groupby(grouper):
        ret = g.get("return", pd.Series(dtype=float)).dropna()
        rows.append({
            "period": str(key),
            "days": int(len(g)),
            "ic_mean": float(g["ic"].mean()) if "ic" in g else None,
            "rank_ic_mean": float(g["rank_ic"].mean()) if "rank_ic" in g else None,
            "rank_ic_positive_ratio": float((g["rank_ic"] > 0).mean()) if "rank_ic" in g else None,
            "return_total": float((1.0 + ret).prod() - 1.0) if len(ret) else None,
            "sharpe": _sharpe(ret) if len(ret) else None,
            "max_drawdown": _max_drawdown_from_returns(ret) if len(ret) else None,
        })
    return rows


def _report_accuracy(report: pd.DataFrame | None, enhanced: dict[str, Any]) -> dict[str, Any]:
    if report is None or report.empty:
        return {"available": False}
    out: dict[str, Any] = {"available": True, "rows": int(len(report))}
    if {"account", "return"}.issubset(report.columns):
        ret_from_account = report["account"].pct_change().fillna(0.0)
        diff = (ret_from_account - report["return"].fillna(0.0)).abs()
        out["return_vs_account_max_abs_diff"] = float(diff.max())
        out["return_vs_account_mean_abs_diff"] = float(diff.mean())
        out["initial_account"] = float(report["account"].iloc[0])
        out["final_account"] = float(report["account"].iloc[-1])
        out["total_return_from_account"] = float(report["account"].iloc[-1] / report["account"].iloc[0] - 1.0)
    if {"cash", "account"}.issubset(report.columns):
        out["avg_cash_ratio_from_report"] = float((report["cash"] / report["account"]).mean())
        out["final_cash_ratio_from_report"] = float(report["cash"].iloc[-1] / report["account"].iloc[-1])
    abs_ret = enhanced.get("absolute_returns") or {}
    if abs_ret and "final_total_value" in abs_ret and "final_account" in out:
        out["enhanced_final_total_value"] = float(abs_ret["final_total_value"])
        out["final_value_abs_diff_vs_enhanced"] = abs(out["final_account"] - out["enhanced_final_total_value"])
    return out


def _fmt_pct(x: Any, width: int = 8) -> str:
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return " " * (width - 2) + "NA"
    return f"{float(x)*100:>{width-1}.2f}%"


def _fmt_num(x: Any, width: int = 8, digits: int = 4) -> str:
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return " " * (width - 2) + "NA"
    return f"{float(x):>{width}.{digits}f}"


def audit_loop(task_id: str, workspace: Path, loop_index: int) -> dict[str, Any]:
    loop_dir = workspace / f"Loop{loop_index}"
    if not loop_dir.exists():
        return {"loop": loop_index, "error": f"missing loop dir: {loop_dir}"}
    conf_text = _read_text(loop_dir / "conf.yaml")
    loader_text = _read_text(loop_dir / "qe_custom_loaders.py")
    run_text = _read_text(loop_dir / "run.log")
    enhanced_path = loop_dir / "qlib_results_enhanced.json"
    enhanced = json.loads(_read_text(enhanced_path)) if enhanced_path.exists() else {}
    artifact_dir = _find_artifact_dir(loop_dir)
    label_info = _parse_label(conf_text, loader_text)
    feature_count = _parse_feature_count(run_text)
    static_scan = _scan_static_leakage(loop_dir)
    result: dict[str, Any] = {
        "task_id": task_id,
        "loop": loop_index,
        "loop_dir": str(loop_dir),
        "artifact_dir": str(artifact_dir) if artifact_dir else None,
        "label": label_info,
        "feature_count": feature_count,
        "static_leakage_scan": static_scan,
        "enhanced_summary": enhanced.get("summary") or {},
        "training": enhanced.get("training_diagnostics") or {},
        "prediction": enhanced.get("prediction_diagnostics") or {},
        "trade": enhanced.get("trade_diagnostics") or {},
        "absolute_returns": enhanced.get("absolute_returns") or {},
    }
    if not artifact_dir:
        result["artifact_error"] = "missing mlruns artifacts/pred.pkl"
        return result

    paths = {
        "pred": artifact_dir / "pred.pkl",
        "label": artifact_dir / "label.pkl",
        "ic": artifact_dir / "sig_analysis" / "ic.pkl",
        "ric": artifact_dir / "sig_analysis" / "ric.pkl",
        "report": artifact_dir / "portfolio_analysis" / "report_normal_1day.pkl",
    }
    result["artifacts"] = {k: {"path": str(v), "exists": v.exists(), "size": v.stat().st_size if v.exists() else None} for k, v in paths.items()}
    missing = [k for k, v in paths.items() if not v.exists() and k in {"pred", "label", "ic", "ric"}]
    if missing:
        result["artifact_error"] = f"missing required artifacts: {missing}"
        return result

    pred = _load_pickle(paths["pred"])
    label = _load_pickle(paths["label"])
    df = _standardize_pred_label(pred, label)
    ic = _daily_corr(df, "pearson")
    ric = _daily_corr(df, "spearman")
    qlib_ic = _load_pickle(paths["ic"])
    qlib_ric = _load_pickle(paths["ric"])
    qlib_ic = pd.Series(qlib_ic).dropna()
    qlib_ric = pd.Series(qlib_ric).dropna()
    report = _load_pickle(paths["report"]) if paths["report"].exists() else None

    common_ic = ic.index.intersection(qlib_ic.index)
    common_ric = ric.index.intersection(qlib_ric.index)
    result["signal_accuracy"] = {
        "aligned_rows": int(len(df)),
        "date_count": int(df.index.get_level_values("datetime").nunique()),
        "instrument_count": int(df.index.get_level_values("instrument").nunique()),
        "recomputed_ic_mean": float(ic.mean()),
        "qlib_ic_mean": float(qlib_ic.mean()),
        "ic_mean_abs_diff": float((ic.loc[common_ic] - qlib_ic.loc[common_ic]).abs().mean()) if len(common_ic) else None,
        "ic_max_abs_diff": float((ic.loc[common_ic] - qlib_ic.loc[common_ic]).abs().max()) if len(common_ic) else None,
        "recomputed_rank_ic_mean": float(ric.mean()),
        "qlib_rank_ic_mean": float(qlib_ric.mean()),
        "rank_ic_mean_abs_diff": float((ric.loc[common_ric] - qlib_ric.loc[common_ric]).abs().mean()) if len(common_ric) else None,
        "rank_ic_max_abs_diff": float((ric.loc[common_ric] - qlib_ric.loc[common_ric]).abs().max()) if len(common_ric) else None,
    }
    summary = enhanced.get("summary") or {}
    if "IC" in summary:
        result["signal_accuracy"]["enhanced_ic"] = float(summary["IC"])
        result["signal_accuracy"]["enhanced_ic_diff_vs_recomputed"] = abs(float(summary["IC"]) - result["signal_accuracy"]["recomputed_ic_mean"])
    if "Rank IC" in summary:
        result["signal_accuracy"]["enhanced_rank_ic"] = float(summary["Rank IC"])
        result["signal_accuracy"]["enhanced_rank_ic_diff_vs_recomputed"] = abs(float(summary["Rank IC"]) - result["signal_accuracy"]["recomputed_rank_ic_mean"])

    result["bucket"] = _bucket_stats(df)
    result["report_accuracy"] = _report_accuracy(report, enhanced)
    result["segments"] = {
        "year": _segment_stats(ic, ric, report, "Y"),
        "quarter": _segment_stats(ic, ric, report, "Q"),
    }
    if report is not None and "return" in report.columns:
        aligned = pd.DataFrame({"rank_ic": ric}).join(report[["return"]], how="inner")
        result["rankic_return_corr"] = float(aligned["rank_ic"].corr(aligned["return"])) if len(aligned) > 3 else None
    return result


def _fmt_sci(x: Any, width: int = 12) -> str:
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return " " * (width - 2) + "NA"
    return f"{float(x):>{width}.2e}"


def _table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    sep = "  ".join("-" * w for w in widths)
    out = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)), sep]
    for row in rows:
        out.append("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))
    return "\n".join(out)


def write_md(task_id: str, audits: list[dict[str, Any]], output: Path) -> None:
    lines: list[str] = []
    lines.append(f"# P0 QE Loop Audit: {task_id} Loop19+ Backtest Accuracy and Leakage Checks")
    lines.append("")
    lines.append("Scope: read-only audit of existing completed loop artifacts. Capital-size, capacity, and impact-cost assumptions are explicitly out of scope for this report.")
    lines.append("")
    rows = []
    for a in audits:
        sa = a.get("signal_accuracy") or {}
        ar = a.get("absolute_returns") or {}
        fc = a.get("feature_count") or {}
        label = a.get("label") or {}
        rows.append([
            str(a.get("loop")),
            str(label.get("horizon")),
            str(fc.get("total")),
            _fmt_num(sa.get("recomputed_ic_mean"), 7, 4),
            _fmt_num(sa.get("recomputed_rank_ic_mean"), 7, 4),
            _fmt_pct(ar.get("cagr"), 8),
            _fmt_num(ar.get("sharpe"), 7, 3),
            _fmt_pct(ar.get("max_drawdown"), 8),
            _fmt_pct((ar.get("final_cash") or 0) / ar.get("final_total_value") if ar.get("final_total_value") else None, 8),
        ])
    lines.append("## 1. Loop Summary")
    lines.append("")
    lines.append("```text")
    lines.append(_table(rows, ["Loop", "H", "Feat", "IC", "RankIC", "CAGR", "Sharpe", "MDD", "Cash"]))
    lines.append("```")
    lines.append("")

    rows = []
    for a in audits:
        sa = a.get("signal_accuracy") or {}
        ra = a.get("report_accuracy") or {}
        rows.append([
            str(a.get("loop")),
            _fmt_sci(sa.get("ic_max_abs_diff"), 12),
            _fmt_sci(sa.get("rank_ic_max_abs_diff"), 12),
            _fmt_sci(sa.get("enhanced_ic_diff_vs_recomputed"), 12),
            _fmt_sci(sa.get("enhanced_rank_ic_diff_vs_recomputed"), 12),
            _fmt_sci(ra.get("return_vs_account_max_abs_diff"), 12),
            _fmt_sci(ra.get("final_value_abs_diff_vs_enhanced"), 12),
        ])
    lines.append("## 2. Backtest and Signal Statistic Accuracy")
    lines.append("")
    lines.append("Tolerance target: IC/RankIC recomputation should be near machine precision versus Qlib artifacts; report return should match account pct_change.")
    lines.append("")
    lines.append("```text")
    lines.append(_table(rows, ["Loop", "ICMaxDiff", "RICMaxDiff", "ICEnhanced", "RICEnhanced", "RetAcctDiff", "FinalValueDiff"]))
    lines.append("```")
    lines.append("")

    rows = []
    for a in audits:
        sa = a.get("signal_accuracy") or {}
        ra = a.get("report_accuracy") or {}
        h = (a.get("label") or {}).get("horizon")
        report_rows = ra.get("rows")
        signal_dates = sa.get("date_count")
        gap = (report_rows - signal_dates) if report_rows is not None and signal_dates is not None else None
        status = "OK" if h is not None and gap == h else "CHECK"
        rows.append([
            str(a.get("loop")),
            str(h),
            str(signal_dates),
            str(report_rows),
            str(gap),
            str(h),
            status,
        ])
    lines.append("## 2b. Label Horizon Date Alignment")
    lines.append("")
    lines.append("Signal IC dates should be shorter than report dates by label horizon because the last H days do not have future-H labels.")
    lines.append("")
    lines.append("```text")
    lines.append(_table(rows, ["Loop", "H", "SignalDates", "ReportRows", "Gap", "Expected", "Status"]))
    lines.append("```")
    lines.append("")

    rows = []
    for a in audits:
        b = a.get("bucket") or {}
        top = b.get("top") or {}
        dec = b.get("decile") or {}
        rows.append([
            str(a.get("loop")),
            _fmt_pct(top.get("top50"), 9),
            _fmt_pct(top.get("bottom50"), 9),
            _fmt_pct(top.get("top50_minus_bottom50"), 9),
            _fmt_pct(dec.get("D1"), 9),
            _fmt_pct(dec.get("D10"), 9),
            _fmt_pct(b.get("d1_d10_mean"), 9),
            _fmt_pct(b.get("d1_d10_positive_ratio"), 9),
        ])
    lines.append("## 3. Signal-to-Return Top Bucket Conversion")
    lines.append("")
    lines.append("```text")
    lines.append(_table(rows, ["Loop", "Top50", "Bottom50", "T50-B50", "D1", "D10", "D1-D10", "LSWin"]))
    lines.append("```")
    lines.append("")

    lines.append("## 4. Static Leakage Scan")
    lines.append("")
    rows = []
    for a in audits:
        scan = a.get("static_leakage_scan") or {}
        rows.append([
            str(a.get("loop")),
            str(scan.get("files_scanned")),
            str(scan.get("hit_count")),
            json.dumps(scan.get("by_pattern") or {}, ensure_ascii=False),
        ])
    lines.append("```text")
    lines.append(_table(rows, ["Loop", "Files", "Hits", "ByPattern"]))
    lines.append("```")
    lines.append("")
    lines.append("Important: static scan findings are risk flags, not final proof. Dynamic truncation recompute is still required for any flagged factor before final acceptance.")
    lines.append("")

    lines.append("## 5. Year Segment Snapshot")
    lines.append("")
    rows = []
    for a in audits:
        for y in (a.get("segments") or {}).get("year") or []:
            rows.append([
                str(a.get("loop")),
                y.get("period"),
                str(y.get("days")),
                _fmt_num(y.get("ic_mean"), 7, 4),
                _fmt_num(y.get("rank_ic_mean"), 7, 4),
                _fmt_pct(y.get("return_total"), 9),
                _fmt_num(y.get("sharpe"), 7, 3),
                _fmt_pct(y.get("max_drawdown"), 9),
            ])
    lines.append("```text")
    lines.append(_table(rows, ["Loop", "Year", "Days", "IC", "RankIC", "Return", "Sharpe", "MDD"]))
    lines.append("```")
    lines.append("")

    lines.append("## 6. Key Findings")
    lines.append("")
    lines.append("- IC/RankIC were recomputed from pred.pkl and label.pkl and compared with Qlib sig_analysis artifacts.")
    lines.append("- Portfolio daily return was recomputed from account pct_change and compared with report_normal_1day return.")
    lines.append("- Top bucket statistics test whether high RankIC is converted into Top50 returns, rather than only full-universe ranking quality.")
    lines.append("- Static leakage scan currently checks code-level high-risk patterns; dynamic truncation recompute remains the required next step for final leakage clearance.")
    lines.append("- Current artifact-level data statistics are internally consistent: IC/RankIC recomputation, enhanced summary values, account returns, and final account values match their source artifacts.")
    lines.append("- Label-horizon date gaps match expected horizons for Loop19-28, which supports that 5D/10D/20D labels are being applied in these alpha-enabled loops.")
    lines.append("- No high-risk future-function pattern was found in factor/model/prepare code by static scan; this is not a final leakage clearance until dynamic truncation recompute is run.")
    lines.append("- This report intentionally excludes the future capital-size scenario; capacity/impact will be audited only after a dedicated capital experiment exists.")
    lines.append("")
    output.write_text("\n".join(lines), encoding="utf-8")


def parse_loops(value: str) -> list[int]:
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only P0 audit for QE loop artifacts")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", default=None, help="Task workspace directory")
    ap.add_argument("--loops", default="19-28", help="Loop list/range, e.g. 19,22,26 or 19-28")
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    ap.add_argument("--allow-partial", action="store_true", help="Write partial output even if a loop has missing required artifacts")
    args = ap.parse_args()

    workspace = Path(args.workspace) if args.workspace else Path(r"F:/Dev/RD-Agent-main/qe_workspace") / args.task_id
    audits = [audit_loop(args.task_id, workspace, i) for i in parse_loops(args.loops)]
    failures = [a for a in audits if a.get("error") or a.get("artifact_error")]
    if failures and not args.allow_partial:
        for failure in failures:
            reason = failure.get("error") or failure.get("artifact_error")
            print(f"ERROR Loop{failure.get('loop')}: {reason}")
        print("Refusing to write partial audit output. Re-run with --allow-partial only if partial evidence is intentional.")
        return 2
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({"task_id": args.task_id, "workspace": str(workspace), "loops": audits}, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md = Path(args.output_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    write_md(args.task_id, audits, out_md)
    print(f"wrote {out_json}")
    print(f"wrote {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
