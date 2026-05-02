#!/usr/bin/env python
"""Select high-priority QE factors for dynamic truncation audits.

The selector is read-only. It ranks factors from persisted
qlib_results_enhanced.json feature_importance sections and checks whether the
corresponding generated factor script exists in loop workspaces.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


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


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


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


def select_factors(workspace: Path, loops: list[int], top_n: int) -> dict[str, Any]:
    agg: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "loops": set(),
            "gain_pct_values": [],
            "gain_values": [],
            "methods": set(),
            "available_loops": set(),
        }
    )
    for loop in loops:
        loop_dir = workspace / f"Loop{loop}"
        enhanced_path = loop_dir / "qlib_results_enhanced.json"
        if not enhanced_path.exists():
            raise FileNotFoundError(f"missing enhanced metrics: {enhanced_path}")
        enhanced = json.loads(enhanced_path.read_text(encoding="utf-8"))
        feature_importance = (enhanced.get("factor_analysis") or {}).get("feature_importance") or []
        if not isinstance(feature_importance, list):
            raise TypeError(f"unexpected feature_importance type in {enhanced_path}: {type(feature_importance)!r}")
        for item in feature_importance:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            rec = agg[name]
            rec["loops"].add(loop)
            rec["gain_pct_values"].append(_safe_float(item.get("gain_pct")))
            rec["gain_values"].append(_safe_float(item.get("gain")))
            if item.get("method"):
                rec["methods"].add(str(item.get("method")))
            if (loop_dir / "factors" / f"{name}.py").exists():
                rec["available_loops"].add(loop)

    rows: list[dict[str, Any]] = []
    for name, rec in agg.items():
        gain_pct_values = rec["gain_pct_values"]
        gain_values = rec["gain_values"]
        rows.append(
            {
                "factor": name,
                "loops": sorted(rec["loops"]),
                "loop_count": len(rec["loops"]),
                "available_loops": sorted(rec["available_loops"]),
                "available_loop_count": len(rec["available_loops"]),
                "mean_gain_pct": sum(gain_pct_values) / len(gain_pct_values) if gain_pct_values else 0.0,
                "max_gain_pct": max(gain_pct_values) if gain_pct_values else 0.0,
                "mean_gain": sum(gain_values) / len(gain_values) if gain_values else 0.0,
                "methods": sorted(rec["methods"]),
            }
        )
    rows.sort(key=lambda r: (r["mean_gain_pct"], r["max_gain_pct"], r["loop_count"], r["available_loop_count"]), reverse=True)
    selected = rows[:top_n]
    return {"workspace": str(workspace), "loops": loops, "top_n": top_n, "ranked": rows, "selected": selected}


def write_md(result: dict[str, Any], task_id: str, output: Path) -> None:
    rows = []
    for rec in result["selected"]:
        rows.append(
            [
                rec["factor"],
                rec["loop_count"],
                rec["available_loop_count"],
                f"{rec['mean_gain_pct']:.2f}",
                f"{rec['max_gain_pct']:.2f}",
                ",".join(str(x) for x in rec["available_loops"]),
            ]
        )
    lines = [
        f"# QE Factor Importance Selector: {task_id}",
        "",
        "Scope: read-only factor ranking from existing qlib_results_enhanced.json feature_importance artifacts.",
        "",
        "```text",
        _table(rows, ["Factor", "LoopCnt", "AvailCnt", "MeanGainPct", "MaxGainPct", "AvailLoops"]),
        "```",
        "",
        "## Dynamic Truncation Factor List",
        "",
        "```text",
        ",".join(rec["factor"] for rec in result["selected"]),
        "```",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Select high-priority factors for QE dynamic truncation audits")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--loops", default="19-28")
    ap.add_argument("--top-n", type=int, default=12)
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()

    workspace = Path(args.workspace) if args.workspace else Path("/mnt/f/Dev/RD-Agent-main/qe_workspace") / args.task_id
    result = select_factors(workspace, _parse_loops(args.loops), args.top_n)
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=list), encoding="utf-8")
    write_md(result, args.task_id, Path(args.output_md))
    print(f"wrote {out_json}")
    print(f"wrote {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
