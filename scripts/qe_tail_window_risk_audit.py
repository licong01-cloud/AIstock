#!/usr/bin/env python
"""Tail-window execution risk audit from existing QE minute artifacts.

This is a read-only diagnostic. It reads persisted minute/day indicators and
daily reports, then measures whether high tail-window turnover days coincide
with worse same-day returns or drawdowns. It does not infer the exact V25 tail
substitute branch because current artifacts do not persist branch events.
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


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


def _fmt_pct(v: Any) -> str:
    x = _safe_float(v)
    return "NA" if x is None else f"{x * 100:.2f}%"


def _fmt_num(v: Any) -> str:
    x = _safe_float(v)
    return "NA" if x is None else f"{x:.4f}"


def _table(rows: list[list[Any]], headers: list[str]) -> str:
    str_rows = [[str(c) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    def fmt(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[i]) if i < len(row) - 1 else cell for i, cell in enumerate(row))

    lines = [fmt([str(h) for h in headers])]
    lines.append("  ".join("-" * w for w in widths))
    lines.extend(fmt(row) for row in str_rows)
    return "\n".join(lines)


def _max_drawdown(ret: pd.Series) -> float | None:
    if ret.empty:
        return None
    nav = (1 + ret.fillna(0)).cumprod()
    dd = nav / nav.cummax() - 1
    return float(dd.min())


def audit_loop(workspace: Path, loop: int) -> dict[str, Any]:
    loop_dir = workspace / f"Loop{loop}"
    artifact_dir = _find_artifact_dir(loop_dir)
    pa_dir = artifact_dir / "portfolio_analysis"
    minute = _load_pickle(pa_dir / "indicators_normal_1min.pkl").copy()
    report = _load_pickle(pa_dir / "report_normal_1day.pkl").copy()
    minute.index = pd.to_datetime(minute.index)
    report.index = pd.to_datetime(report.index).normalize()

    value = minute["value"].fillna(0)
    date = minute.index.normalize()
    tail_mask = minute.index.time >= pd.Timestamp("14:30:00").time()
    close_mask = minute.index.time >= pd.Timestamp("14:55:00").time()
    daily = pd.DataFrame({"date": date, "value": value, "tail_value": value.where(tail_mask, 0), "close_value": value.where(close_mask, 0)})
    agg = daily.groupby("date").sum()
    agg["tail_ratio"] = agg["tail_value"] / agg["value"].replace(0, np.nan)
    agg["close_ratio"] = agg["close_value"] / agg["value"].replace(0, np.nan)
    aligned = agg.join(report[["return", "account"]], how="left")
    valid = aligned[aligned["value"] > 0].copy()
    if valid.empty:
        raise ValueError(f"Loop{loop} has no positive execution value rows")

    p90 = float(valid["tail_ratio"].quantile(0.90))
    p95 = float(valid["tail_ratio"].quantile(0.95))
    high = valid[valid["tail_ratio"] >= p90]
    low = valid[valid["tail_ratio"] < p90]
    top_days = valid.sort_values("tail_ratio", ascending=False).head(10)
    corr = valid[["tail_ratio", "return"]].corr(method="spearman").iloc[0, 1]

    return {
        "loop": loop,
        "days": int(len(valid)),
        "tail_ratio_mean": float(valid["tail_ratio"].mean()),
        "tail_ratio_p90": p90,
        "tail_ratio_p95": p95,
        "tail_ratio_max": float(valid["tail_ratio"].max()),
        "close_ratio_mean": float(valid["close_ratio"].mean()),
        "high_tail_days": int(len(high)),
        "high_tail_mean_return": float(high["return"].mean()),
        "low_tail_mean_return": float(low["return"].mean()),
        "high_tail_win_rate": float((high["return"] > 0).mean()),
        "low_tail_win_rate": float((low["return"] > 0).mean()),
        "high_tail_max_drawdown": _max_drawdown(high["return"]),
        "low_tail_max_drawdown": _max_drawdown(low["return"]),
        "tail_return_spearman": _safe_float(corr),
        "top_tail_days": [
            {
                "date": idx.strftime("%Y-%m-%d"),
                "tail_ratio": float(row["tail_ratio"]),
                "close_ratio": float(row["close_ratio"]),
                "return": float(row["return"]),
                "value": float(row["value"]),
            }
            for idx, row in top_days.iterrows()
        ],
    }


def write_md(result: dict[str, Any], output: Path) -> None:
    lines = [
        f"# QE Tail-Window Existing-Artifact Risk Audit: {result['task_id']}",
        "",
        "Scope: persisted minute/day indicators and daily reports only. No QE rerun and no strategy logging changes.",
        "",
    ]
    rows = []
    for loop in result["loops"]:
        rows.append(
            [
                loop["loop"],
                loop["days"],
                _fmt_pct(loop["tail_ratio_mean"]),
                _fmt_pct(loop["tail_ratio_p95"]),
                _fmt_pct(loop["tail_ratio_max"]),
                loop["high_tail_days"],
                _fmt_pct(loop["high_tail_mean_return"]),
                _fmt_pct(loop["low_tail_mean_return"]),
                _fmt_num(loop["tail_return_spearman"]),
            ]
        )
    lines += [
        "## Tail-Window Summary",
        "",
        "```text",
        _table(
            rows,
            ["Loop", "Days", "TailMean", "TailP95", "TailMax", "HighDays", "HighRet", "LowRet", "TailRetR"],
        ),
        "```",
        "",
    ]

    top_rows: list[list[Any]] = []
    for loop in result["loops"]:
        for row in loop["top_tail_days"][:5]:
            top_rows.append(
                [
                    loop["loop"],
                    row["date"],
                    _fmt_pct(row["tail_ratio"]),
                    _fmt_pct(row["close_ratio"]),
                    _fmt_pct(row["return"]),
                    f"{row['value']:.2f}",
                ]
            )
    lines += [
        "## Top Tail-Ratio Days",
        "",
        "```text",
        _table(top_rows, ["Loop", "Date", "TailRatio", "CloseRatio", "Return", "Value"]),
        "```",
        "",
        "## Evidence Notes",
        "",
        "- Tail ratio is `sum(value from 14:30 through 15:00) / daily sum(value)` from `indicators_normal_1min.pkl`.",
        "- This is a tail-activity proxy, not proof of the exact tail-substitute branch, because exact branch events are not persisted.",
        "- A weak or negative `TailRetR` means high tail activity is not mechanically associated with better same-day returns in the persisted artifacts.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="QE tail-window risk audit from existing artifacts")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--loops", default="24,25,27")
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()
    workspace = Path(args.workspace) if args.workspace else Path("/mnt/f/Dev/RD-Agent-main/qe_workspace") / args.task_id
    loops = [audit_loop(workspace, loop) for loop in _parse_loops(args.loops)]
    result = {"task_id": args.task_id, "workspace": str(workspace), "loops": loops}
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_md(result, Path(args.output_md))
    print(f"wrote {out_json}")
    print(f"wrote {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
