"""Build HMM QE candidate metrics and TopK attribution tables.

This read-only helper combines:
- QE task/API loop metrics, including turnover and cost diagnostics when the
  loop has completed and enhanced metrics are available.
- Registered HMM coefficient overlay stats.
- Offline TopK enter/drop attribution replayed from the baseline no-HMM
  prediction artifact.

It is intended for HMM shadow-loop comparisons where annualized return alone is
not enough to pick a candidate.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.diagnostics.hmm_sector_factor_overlay_diagnostic import (  # noqa: E402
    compute_replacements,
    enrich_db_forward_returns,
    find_base_artifacts,
    label_to_series,
    load_pickle,
    load_stock_sector_map,
    pred_to_series,
    safe_float,
    split_periods,
)


SOURCE_QE_TASK = "qe_20260502_131502_9b54"
DEFAULT_HMM_DIAG_DIR = Path(".codex_tmp/hmm_offline_diag") / SOURCE_QE_TASK
DEFAULT_REGISTRY_GLOB = ".codex_tmp/hmm_registry_updates/hmm_sector_factor_gate_registry_result_*.json"
DEFAULT_OUTPUT_DIR = Path(".codex_tmp/hmm_qe_candidate_attribution")
RUNTIME_PRESET = "preset_A"
TEST_START = "2024-07-01"
BACKTEST_END = "2026-04-27"

BASELINE_COEFFICIENTS = [
    {
        "label": "LOOP2_BASE__old_covfix_w3_raw",
        "snapshot_id": "bbec3863-fb67-445f-938e-66f092d18696",
        "coefficients_path": Path(
            "backend/data/hmm_models/b99c907b-873a-4173-a4ee-5eab266f8c49/2026-04-27/"
            f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
        ),
        "base_key": "L2",
    },
    {
        "label": "LOOP10_BASE__penalty_only_f096",
        "snapshot_id": "6ea64754-003d-48d8-ad9e-d0e7857716c8",
        "coefficients_path": Path(
            "backend/data/hmm_models/ce4952c1-4b0d-46a7-81f2-ae1d4a249555/2026-05-04/"
            f"coefficients_{RUNTIME_PRESET}_{TEST_START}_{BACKTEST_END}.json"
        ),
        "base_key": "L10",
    },
]


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def pct(value: Any, digits: int = 2) -> str:
    f = safe_float(value)
    if f is None:
        return "NA"
    return f"{f * 100:.{digits}f}%"


def num(value: Any, digits: int = 4) -> str:
    f = safe_float(value)
    if f is None:
        return "NA"
    return f"{f:.{digits}f}"


def latest_registry_path(pattern: str) -> Path:
    paths = sorted(Path().glob(pattern), key=lambda p: p.stat().st_mtime)
    if not paths:
        raise RuntimeError(f"registry result file not found: {pattern}")
    return paths[-1]


def api_get(api_base: str, endpoint: str, timeout: int = 60) -> dict[str, Any]:
    url = f"{api_base.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def unwrap(payload: Any) -> Any:
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def flatten_loop_configs(task: dict[str, Any]) -> dict[int, dict[str, Any]]:
    cfg = task.get("strategy_evo_config") or {}
    if isinstance(cfg, str):
        cfg = json.loads(cfg)
    loops = cfg.get("loops") if isinstance(cfg, dict) else []
    out: dict[int, dict[str, Any]] = {}
    for loop in loops or []:
        idx = int(loop.get("loop_index") or 0)
        if idx:
            out[idx] = loop
    return out


def extract_metric_row(
    *,
    loop_row: dict[str, Any],
    configured_loop: dict[str, Any],
    enhanced: dict[str, Any] | None,
) -> dict[str, Any]:
    metrics = loop_row.get("metrics_json") or {}
    if isinstance(metrics, str):
        metrics = json.loads(metrics)
    enhanced = enhanced or metrics.get("enhanced_metrics") or {}
    summary = enhanced.get("summary") or {}
    trade = enhanced.get("trade_diagnostics") or {}
    pred = enhanced.get("prediction_diagnostics") or {}
    abs_ret = enhanced.get("absolute_returns") or {}

    ann_cost = safe_float(summary.get("1day.excess_return_with_cost.annualized_return"))
    ann_no_cost = safe_float(summary.get("1day.excess_return_without_cost.annualized_return"))
    cost_drag = safe_float(trade.get("cost_drag_annualized"))
    if cost_drag is None and ann_no_cost is not None and ann_cost is not None:
        cost_drag = ann_no_cost - ann_cost

    return {
        "loop_index": loop_row.get("loop_index"),
        "status": loop_row.get("status"),
        "label": configured_loop.get("label"),
        "snapshot_id": configured_loop.get("hmm_model_version_id"),
        "enable_sector_hmm": bool(configured_loop.get("enable_sector_hmm")),
        "experiment_id": loop_row.get("experiment_id"),
        "node_id": configured_loop.get("node_id") or loop_row.get("node_id"),
        "ann_return_with_cost": ann_cost,
        "ann_return_no_cost": ann_no_cost,
        "cost_drag_annualized": cost_drag,
        "max_drawdown_with_cost": safe_float(summary.get("1day.excess_return_with_cost.max_drawdown")),
        "sharpe_with_cost": safe_float(summary.get("1day.excess_return_with_cost.information_ratio")),
        "abs_cagr": safe_float(abs_ret.get("cagr")),
        "abs_sharpe": safe_float(abs_ret.get("sharpe")),
        "avg_turnover": safe_float(trade.get("avg_turnover")),
        "annualized_turnover": safe_float(trade.get("annualized_turnover")),
        "daily_trade_count_avg": safe_float(trade.get("daily_trade_count_avg")),
        "pred_rank_turnover": safe_float(pred.get("pred_rank_turnover")),
        "top30_stability": safe_float(pred.get("top30_stability")),
        "ic": safe_float(summary.get("IC")),
        "rank_ic": safe_float(summary.get("Rank IC")),
    }


def fetch_metric_rows(task_id: str, api_base: str) -> tuple[dict[str, Any], pd.DataFrame]:
    task = unwrap(api_get(api_base, f"quantevolver/evolution/tasks/{task_id}"))
    configured = flatten_loop_configs(task)
    rows = []
    for loop_row in sorted(task.get("loops") or [], key=lambda item: int(item.get("loop_index") or 0)):
        loop_index = int(loop_row.get("loop_index") or 0)
        enhanced = None
        if loop_row.get("status") == "completed":
            try:
                enhanced = unwrap(
                    api_get(
                        api_base,
                        f"quantevolver/evolution/tasks/{task_id}/loops/Loop{loop_index}/enhanced-metrics",
                        timeout=120,
                    )
                )
            except Exception as exc:
                enhanced = {"_enhanced_fetch_error": str(exc)}
        rows.append(
            extract_metric_row(
                loop_row=loop_row,
                configured_loop=configured.get(loop_index, {}),
                enhanced=enhanced,
            )
        )
    return task, pd.DataFrame(rows)


def load_candidate_specs(registry_path: Path) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    specs.extend(
        {
            **item,
            "coefficients_path": str((ROOT / item["coefficients_path"]).resolve()),
            "overlay_stats": {},
        }
        for item in BASELINE_COEFFICIENTS
    )
    registry = read_json(registry_path)

    # Sector-factor gate registry stores rows under "registered"; HMM input
    # preprocessing registry stores equivalent rows under "results".
    for row in registry.get("registered") or []:
        specs.append(
            {
                "label": row["display_name"],
                "snapshot_id": row["snapshot_id"],
                "coefficients_path": row["coefficients_path"],
                "base_key": row["variant_name"].split("_", 1)[0],
                "overlay_stats": row.get("overlay_stats") or {},
            }
        )
    for row in registry.get("results") or []:
        specs.append(
            {
                "label": row["display_name"],
                "snapshot_id": row["snapshot_id"],
                "coefficients_path": row["coefficients_path"],
                "base_key": row.get("base_key"),
                "overlay_stats": {
                    "preprocess_mode": row.get("mode"),
                    "coefficient_meta": row.get("coefficient_meta") or {},
                },
            }
        )
    return specs


def compute_topk_attribution(
    *,
    hmm_diag_dir: Path,
    registry_path: Path,
    topk: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pred_path, label_path = find_base_artifacts(hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(hmm_diag_dir)
    candidates = load_candidate_specs(registry_path)

    rep_frames = []
    day_frames = []
    summary_rows: list[dict[str, Any]] = []
    for spec in candidates:
        coeff_path = Path(spec["coefficients_path"])
        if not coeff_path.is_file():
            raise RuntimeError(f"coefficient file missing for {spec['label']}: {coeff_path}")
        payload = read_json(coeff_path)
        coeffs = payload.get("daily_coefficients") or {}
        rep, day, _sector = compute_replacements(
            pred_ser,
            label_ser,
            coeffs,
            stock_sector_map,
            topk,
            spec["label"],
        )
        rep["snapshot_id"] = spec["snapshot_id"]
        day["snapshot_id"] = spec["snapshot_id"]
        rep_frames.append(rep)
        day_frames.append(day)
        enriched = enrich_db_forward_returns(rep, [5, 10, 20]) if not rep.empty else rep
        for row in split_periods(enriched, day, spec["label"], "2025-05-01"):
            row["snapshot_id"] = spec["snapshot_id"]
            row["base_key"] = spec.get("base_key")
            overlay = spec.get("overlay_stats") or {}
            row["overlay_changed_sector_dates"] = overlay.get("changed_sector_date_count")
            row["overlay_same_as_base"] = overlay.get("same_as_base")
            row["overlay_missing_rank_count"] = overlay.get("missing_rank_count")
            summary_rows.append(row)

    all_rep = pd.concat(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    all_day = pd.concat(day_frames, ignore_index=True) if day_frames else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    return summary, all_rep, all_day


def build_symbol_attribution(replacements: pd.DataFrame, limit: int = 100) -> pd.DataFrame:
    if replacements.empty:
        return pd.DataFrame()
    rows = []
    for (candidate, typ, symbol, sector), group in replacements.groupby(
        ["candidate", "replacement_type", "symbol", "sector_code"],
        dropna=False,
    ):
        rows.append(
            {
                "candidate": candidate,
                "replacement_type": typ,
                "symbol": symbol,
                "sector_code": sector,
                "days": group["date"].nunique(),
                "mean_label_10d": safe_float(group["label_10d"].mean()),
                "mean_raw_rank": safe_float(group["raw_rank"].mean()),
                "mean_adjusted_rank": safe_float(group["adjusted_rank"].mean()),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return (
        out.sort_values(["candidate", "replacement_type", "days", "mean_label_10d"], ascending=[True, True, False, False])
        .groupby(["candidate", "replacement_type"], as_index=False, group_keys=False)
        .head(limit)
    )


def frame_to_text(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "(no rows)"
    return "```text\n" + frame.to_string(index=False) + "\n```"


def write_report(path: Path, task: dict[str, Any], merged: pd.DataFrame, attribution: pd.DataFrame) -> None:
    completed = merged[merged["status"] == "completed"].copy()
    if not completed.empty:
        completed = completed.sort_values(
            ["ann_return_with_cost", "cost_drag_annualized", "avg_turnover"],
            ascending=[False, True, True],
        )
    holdout = attribution[attribution["period"] == "holdout"].copy()
    lines = [
        f"# HMM QE Candidate Attribution - {task.get('task_id')}",
        "",
        f"- Task status: `{task.get('status')}`",
        f"- Source offline prediction task: `{SOURCE_QE_TASK}`",
        "- Ranking should consider return, drawdown, turnover, cost drag and TopK replacement quality together.",
        "",
        "## Completed Loop Metrics",
        "",
    ]
    if completed.empty:
        lines.append("(no completed loops with enhanced metrics yet)")
    else:
        lines.append(frame_to_text(
            completed[
                [
                    "loop_index",
                    "label",
                    "ann_return_with_cost",
                    "max_drawdown_with_cost",
                    "sharpe_with_cost",
                    "avg_turnover",
                    "cost_drag_annualized",
                    "changed_days_holdout",
                    "net_mean_db_ret_10d_holdout",
                ]
            ]
        ))
    lines.extend(["", "## Offline TopK Holdout Attribution", ""])
    if holdout.empty:
        lines.append("(no attribution rows)")
    else:
        view = holdout.sort_values(
            ["net_mean_db_ret_10d", "net_mean_label_10d", "changed_days"],
            ascending=[False, False, False],
        ).head(20)
        lines.append(frame_to_text(
            view[
                [
                    "candidate",
                    "changed_days",
                    "avg_entered_per_day",
                    "net_mean_label_10d",
                    "net_mean_db_ret_5d",
                    "net_mean_db_ret_10d",
                    "net_mean_db_ret_20d",
                    "positive_net_label_day_ratio",
                ]
            ]
        ))
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", help="QE custom evolution task id")
    parser.add_argument("--api-base", default="http://127.0.0.1:8001/api/v1")
    parser.add_argument("--hmm-diag-dir", type=Path, default=DEFAULT_HMM_DIAG_DIR)
    parser.add_argument("--registry", type=Path, default=None)
    parser.add_argument("--registry-glob", default=DEFAULT_REGISTRY_GLOB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()

    registry_path = args.registry or latest_registry_path(args.registry_glob)
    out_dir = args.output_dir / args.task_id
    out_dir.mkdir(parents=True, exist_ok=True)

    task, metrics = fetch_metric_rows(args.task_id, args.api_base)
    attribution, replacements, daily = compute_topk_attribution(
        hmm_diag_dir=args.hmm_diag_dir,
        registry_path=registry_path,
        topk=args.topk,
    )

    holdout = attribution[attribution["period"] == "holdout"].copy()
    holdout_suffix = holdout.add_suffix("_holdout")
    merged = metrics.merge(
        holdout_suffix,
        left_on="label",
        right_on="candidate_holdout",
        how="left",
    )

    symbol_attr = build_symbol_attribution(replacements)
    metrics_path = out_dir / "loop_metrics_with_attribution.csv"
    attr_path = out_dir / "topk_attribution_summary.csv"
    rep_path = out_dir / "topk_replacements.csv"
    day_path = out_dir / "topk_daily_summary.csv"
    sym_path = out_dir / "top_symbols_by_candidate.csv"
    report_path = out_dir / "hmm_qe_candidate_attribution.md"
    raw_task_path = out_dir / "task_api_snapshot.json"

    merged.to_csv(metrics_path, index=False, encoding="utf-8-sig")
    attribution.to_csv(attr_path, index=False, encoding="utf-8-sig")
    replacements.to_csv(rep_path, index=False, encoding="utf-8-sig")
    daily.to_csv(day_path, index=False, encoding="utf-8-sig")
    symbol_attr.to_csv(sym_path, index=False, encoding="utf-8-sig")
    write_json(raw_task_path, task)
    write_report(report_path, task, merged, attribution)

    print(json.dumps(
        {
            "task_id": args.task_id,
            "task_status": task.get("status"),
            "registry": str(registry_path),
            "output_dir": str(out_dir),
            "files": {
                "metrics": str(metrics_path),
                "attribution": str(attr_path),
                "replacements": str(rep_path),
                "daily": str(day_path),
                "symbols": str(sym_path),
                "report": str(report_path),
                "task_api_snapshot": str(raw_task_path),
            },
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
