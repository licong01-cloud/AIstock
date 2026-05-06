"""Loop10-centered attribution for HMM QE candidates.

This diagnostic is intentionally read-only.  It compares completed QE loops and
candidate HMM coefficient files against the retained Loop10 HMM baseline:

- daily portfolio return deltas from QE enhanced return curves;
- TopK enter/drop attribution relative to Loop10 adjusted rankings;
- coefficient delta summaries against Loop10 coefficients;
- sector-level replacement contribution summaries.

It is used before launching more remote QE loops so weak HMM remaps can be
filtered at script level first.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.diagnostics.hmm_qe_candidate_attribution import (  # noqa: E402
    BASELINE_COEFFICIENTS,
    load_candidate_specs,
    read_json,
)
from scripts.diagnostics.hmm_sector_factor_overlay_diagnostic import (  # noqa: E402
    enrich_db_forward_returns,
    find_base_artifacts,
    get_label_value,
    label_to_series,
    load_pickle,
    load_stock_sector_map,
    pred_to_series,
    safe_float,
    split_periods,
)


DEFAULT_TASK_ID = "qe_20260504_184036_3a3c"
DEFAULT_SOURCE_QE_TASK = "qe_20260502_131502_9b54"
DEFAULT_HMM_DIAG_DIR = Path(".codex_tmp/hmm_offline_diag") / DEFAULT_SOURCE_QE_TASK
DEFAULT_DETAIL_JSON = Path(".codex_tmp") / f"{DEFAULT_TASK_ID}_detail_final.json"
DEFAULT_SUMMARY_CSV = Path(".codex_tmp") / f"{DEFAULT_TASK_ID}_loop_summary.csv"
DEFAULT_REGISTRY = (
    Path(".codex_tmp/hmm_registry_updates")
    / "hmm_utility_mapping_registry_result_20260504_183459.json"
)
DEFAULT_OUTPUT_DIR = Path(".codex_tmp/hmm_loop10_centered_attribution")
LOOP10_SNAPSHOT_ID = "6ea64754-003d-48d8-ad9e-d0e7857716c8"
HOLDOUT_START = "2025-05-01"


def unwrap_task(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, dict) else payload


def load_task_detail(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"task detail json not found: {path}")
    return unwrap_task(read_json(path))


def metric_value(metrics: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = safe_float(metrics.get(key))
        if value is not None:
            return value
    return None


def task_loop_rows(task: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for loop in sorted(task.get("loops") or [], key=lambda x: int(x.get("loop_index") or 0)):
        cfg = loop.get("config_json") or {}
        metrics = loop.get("metrics_json") or {}
        if isinstance(cfg, str):
            cfg = json.loads(cfg)
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        model_params = cfg.get("model_params") or {}
        model_params_text = str(model_params)
        snapshot_id = cfg.get("hmm_model_version_id")
        if snapshot_id is None and isinstance(model_params, dict):
            snapshot_id = model_params.get("hmm_model_version_id")
        enable_sector_hmm = cfg.get("enable_sector_hmm")
        if enable_sector_hmm is None and isinstance(model_params, dict):
            enable_sector_hmm = model_params.get("enable_sector_hmm")
        rows.append(
            {
                "loop_index": int(loop.get("loop_index") or 0),
                "loop_label": cfg.get("label"),
                "snapshot_id": snapshot_id,
                "enable_sector_hmm": bool(
                    enable_sector_hmm
                    or "enable_sector_hmm=True" in model_params_text
                    or snapshot_id
                ),
                "status": loop.get("status"),
                "experiment_id": loop.get("experiment_id"),
                "annualized_return": metric_value(
                    metrics,
                    "annualized_return",
                    "1day.excess_return_with_cost.annualized_return",
                ),
                "sharpe": metric_value(
                    metrics,
                    "sharpe",
                    "1day.excess_return_with_cost.information_ratio",
                ),
                "max_drawdown": metric_value(
                    metrics,
                    "max_drawdown",
                    "1day.excess_return_with_cost.max_drawdown",
                ),
                "final_nav": metric_value(metrics, "final_nav"),
            }
        )
    return pd.DataFrame(rows)


def load_coefficients(path: Path) -> dict[str, dict[str, float]]:
    payload = read_json(path)
    coeffs = payload.get("daily_coefficients")
    if not isinstance(coeffs, dict):
        raise ValueError(f"daily_coefficients missing in {path}")
    out: dict[str, dict[str, float]] = {}
    for date, row in coeffs.items():
        if not isinstance(row, dict):
            continue
        out[str(date)] = {str(k): float(v) for k, v in row.items()}
    return out


def build_spec_frame(specs: list[dict[str, Any]], loops: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    loop_by_snapshot = {
        str(row["snapshot_id"]): row
        for _, row in loops.dropna(subset=["snapshot_id"]).iterrows()
    }
    for spec in specs:
        snapshot_id = str(spec.get("snapshot_id") or "")
        loop = loop_by_snapshot.get(snapshot_id)
        stage3 = spec.get("stage3_metrics") or {}
        coeff_stats = spec.get("coefficient_stats") or {}
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "candidate": spec.get("label"),
                "base_key": spec.get("base_key"),
                "variant_name": spec.get("variant_name"),
                "coefficients_path": str(spec.get("coefficients_path")),
                "loop_index": int(loop["loop_index"]) if loop is not None else None,
                "loop_label": loop["loop_label"] if loop is not None else None,
                "qe_annualized_return": loop["annualized_return"] if loop is not None else None,
                "qe_sharpe": loop["sharpe"] if loop is not None else None,
                "qe_max_drawdown": loop["max_drawdown"] if loop is not None else None,
                "coefficient_min": coeff_stats.get("coefficient_min"),
                "coefficient_max": coeff_stats.get("coefficient_max"),
                "coefficient_active_rate": coeff_stats.get("active_rate"),
                "source_panel_path": coeff_stats.get("source_panel_path"),
                "stage3_candidate": stage3.get("candidate"),
                "stage3_transform": stage3.get("transform"),
                "stage3_range": stage3.get("range_name"),
                "stage3_rank_ic_10d": stage3.get("rank_ic_mean_10d"),
                "stage3_weighted_rank_ic": stage3.get("weighted_rank_ic_mean"),
            }
        )
    return pd.DataFrame(rows)


def adjusted_top(
    scores: pd.Series,
    coeffs: dict[str, float],
    stock_sector_map: dict[str, str],
    topk: int,
) -> tuple[set[str], pd.Series, pd.Series, pd.Series]:
    scores = scores.dropna().astype(float)
    sector = pd.Series(scores.index, index=scores.index).map(stock_sector_map)
    multiplier = sector.map(lambda x: coeffs.get(str(x), 1.0) if x is not None else 1.0).astype(float)
    adjusted = scores * multiplier
    ranked = adjusted.sort_values(ascending=False, kind="mergesort")
    rank = pd.Series(np.arange(1, len(ranked) + 1), index=ranked.index)
    return set(ranked.head(topk).index), adjusted, rank, multiplier


def pairwise_topk_attribution(
    pred_ser: pd.Series,
    label_ser: pd.Series,
    stock_sector_map: dict[str, str],
    base_coeffs: dict[str, dict[str, float]],
    cand_coeffs: dict[str, dict[str, float]],
    candidate: str,
    topk: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pred_dates = set(pred_ser.index.get_level_values(0).unique())
    coeff_dates = set(pd.to_datetime(list(base_coeffs.keys()))) & set(pd.to_datetime(list(cand_coeffs.keys())))
    dates = sorted(pred_dates & coeff_dates)
    pred_by_day = {dt: s.droplevel(0) for dt, s in pred_ser.groupby(level=0, sort=True)}
    rows: list[dict[str, Any]] = []
    day_rows: list[dict[str, Any]] = []

    for dt in dates:
        date_str = pd.Timestamp(dt).strftime("%Y-%m-%d")
        scores = pred_by_day.get(dt)
        if scores is None or scores.empty:
            continue
        base_top, _base_adj, base_rank, base_mult = adjusted_top(
            scores, base_coeffs.get(date_str) or {}, stock_sector_map, topk
        )
        cand_top, _cand_adj, cand_rank, cand_mult = adjusted_top(
            scores, cand_coeffs.get(date_str) or {}, stock_sector_map, topk
        )
        entered = sorted(cand_top - base_top)
        dropped = sorted(base_top - cand_top)
        entered_label: list[float] = []
        dropped_label: list[float] = []
        sector = pd.Series(scores.index, index=scores.index).map(stock_sector_map)
        for replacement_type, symbols in (
            ("entered_vs_loop10", entered),
            ("dropped_vs_loop10", dropped),
        ):
            for symbol in symbols:
                lab = get_label_value(label_ser, dt, symbol)
                if lab is not None:
                    if replacement_type == "entered_vs_loop10":
                        entered_label.append(lab)
                    else:
                        dropped_label.append(lab)
                rows.append(
                    {
                        "candidate": candidate,
                        "date": date_str,
                        "symbol": symbol,
                        "replacement_type": replacement_type,
                        "sector_code": sector.get(symbol),
                        "base_coefficient": safe_float(base_mult.get(symbol)),
                        "candidate_coefficient": safe_float(cand_mult.get(symbol)),
                        "base_rank": int(base_rank.get(symbol)) if symbol in base_rank.index else None,
                        "candidate_rank": int(cand_rank.get(symbol)) if symbol in cand_rank.index else None,
                        "label_10d": lab,
                    }
                )
        day_rows.append(
            {
                "candidate": candidate,
                "date": date_str,
                "base_top_count": len(base_top),
                "candidate_top_count": len(cand_top),
                "common_count": len(base_top & cand_top),
                "entered_count": len(entered),
                "dropped_count": len(dropped),
                "replacement_count": len(entered) + len(dropped),
                "mean_entered_label_10d": float(np.nanmean(entered_label)) if entered_label else np.nan,
                "mean_dropped_label_10d": float(np.nanmean(dropped_label)) if dropped_label else np.nan,
                "net_enter_minus_drop_label_10d": (
                    float(np.nanmean(entered_label) - np.nanmean(dropped_label))
                    if entered_label and dropped_label
                    else np.nan
                ),
            }
        )
    return pd.DataFrame(rows), pd.DataFrame(day_rows)


def normalize_replacement_type(rep: pd.DataFrame) -> pd.DataFrame:
    if rep.empty:
        return rep
    out = rep.copy()
    out["replacement_type_raw"] = out["replacement_type"]
    out["replacement_type"] = out["replacement_type"].replace(
        {
            "entered_vs_loop10": "entered_by_hmm",
            "dropped_vs_loop10": "dropped_by_hmm",
        }
    )
    return out


def summarize_pairwise_periods(rep: pd.DataFrame, day: pd.DataFrame, candidate: str) -> list[dict[str, Any]]:
    normalized = normalize_replacement_type(rep)
    rows = split_periods(normalized, day, candidate, HOLDOUT_START)
    for row in rows:
        row["baseline"] = "LOOP10_BASE__penalty_only_f096"
    return rows


def coefficient_delta_summary(
    base_coeffs: dict[str, dict[str, float]],
    cand_coeffs: dict[str, dict[str, float]],
    candidate: str,
) -> dict[str, Any]:
    deltas: list[float] = []
    cand_values: list[float] = []
    base_values: list[float] = []
    cand_gt = cand_lt = same = 0
    for date in sorted(set(base_coeffs) & set(cand_coeffs)):
        sectors = set(base_coeffs[date]) | set(cand_coeffs[date])
        for sector in sectors:
            base = float(base_coeffs[date].get(sector, 1.0))
            cand = float(cand_coeffs[date].get(sector, 1.0))
            delta = cand - base
            deltas.append(delta)
            cand_values.append(cand)
            base_values.append(base)
            if delta > 1e-12:
                cand_gt += 1
            elif delta < -1e-12:
                cand_lt += 1
            else:
                same += 1
    arr = np.array(deltas, dtype=float)
    cand_arr = np.array(cand_values, dtype=float)
    base_arr = np.array(base_values, dtype=float)
    total = int(arr.size)
    return {
        "candidate": candidate,
        "sector_date_pairs": total,
        "mean_delta_vs_loop10": float(np.nanmean(arr)) if total else None,
        "mean_abs_delta_vs_loop10": float(np.nanmean(np.abs(arr))) if total else None,
        "p95_abs_delta_vs_loop10": float(np.nanpercentile(np.abs(arr), 95)) if total else None,
        "candidate_gt_loop10_share": cand_gt / total if total else None,
        "candidate_lt_loop10_share": cand_lt / total if total else None,
        "same_as_loop10_share": same / total if total else None,
        "candidate_min": float(np.nanmin(cand_arr)) if total else None,
        "candidate_max": float(np.nanmax(cand_arr)) if total else None,
        "loop10_penalty_share": float(np.nanmean(base_arr < 1.0 - 1e-12)) if total else None,
        "candidate_penalty_share": float(np.nanmean(cand_arr < 1.0 - 1e-12)) if total else None,
        "candidate_boost_share": float(np.nanmean(cand_arr > 1.0 + 1e-12)) if total else None,
    }


def daily_portfolio_deltas(task: dict[str, Any], loops: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    curves: dict[int, pd.Series] = {}
    for loop in task.get("loops") or []:
        idx = int(loop.get("loop_index") or 0)
        metrics = loop.get("metrics_json") or {}
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        enhanced = metrics.get("enhanced_metrics") or {}
        if not isinstance(enhanced, dict):
            continue
        rc = enhanced.get("return_curves") or {}
        dates = rc.get("dates") or []
        values = rc.get("cumulative_portfolio_with_cost") or rc.get("cumulative_portfolio")
        if dates and values:
            series = pd.Series(values, index=pd.to_datetime(dates), dtype=float).sort_index()
            daily = series.pct_change()
            daily.iloc[0] = 0.0
            curves[idx] = daily
    if 3 not in curves:
        return pd.DataFrame(), pd.DataFrame()

    loop_labels = {
        int(row["loop_index"]): row["loop_label"]
        for _, row in loops.iterrows()
        if not pd.isna(row["loop_index"])
    }
    base = curves[3]
    rows: list[dict[str, Any]] = []
    worst_rows: list[dict[str, Any]] = []
    for idx, daily in curves.items():
        if idx == 3:
            continue
        aligned = pd.concat([daily.rename("candidate"), base.rename("loop10")], axis=1).dropna()
        diff = aligned["candidate"] - aligned["loop10"]
        std = diff.std(ddof=1)
        n = int(diff.count())
        t_stat = float(diff.mean() / (std / math.sqrt(n))) if std and n > 1 else None
        rows.append(
            {
                "loop_index": idx,
                "loop_label": loop_labels.get(idx),
                "mean_daily_diff_vs_loop10": float(diff.mean()) if n else None,
                "t_stat_vs_loop10": t_stat,
                "win_rate_vs_loop10": float((diff > 0).mean()) if n else None,
                "worst_daily_diff_vs_loop10": float(diff.min()) if n else None,
                "best_daily_diff_vs_loop10": float(diff.max()) if n else None,
                "days": n,
            }
        )
        for date, value in diff.nsmallest(10).items():
            worst_rows.append(
                {
                    "loop_index": idx,
                    "loop_label": loop_labels.get(idx),
                    "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
                    "daily_diff_vs_loop10": float(value),
                    "candidate_daily_return": float(aligned.loc[date, "candidate"]),
                    "loop10_daily_return": float(aligned.loc[date, "loop10"]),
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(worst_rows)


def sector_replacement_summary(replacements: pd.DataFrame) -> pd.DataFrame:
    if replacements.empty:
        return pd.DataFrame()
    rep = normalize_replacement_type(replacements)
    rows: list[dict[str, Any]] = []
    for (candidate, sector), group in rep.groupby(["candidate", "sector_code"], dropna=False):
        entered = group[group["replacement_type"] == "entered_by_hmm"]
        dropped = group[group["replacement_type"] == "dropped_by_hmm"]
        row = {
            "candidate": candidate,
            "sector_code": sector,
            "entered_rows": int(len(entered)),
            "dropped_rows": int(len(dropped)),
            "net_rows": int(len(entered) - len(dropped)),
        }
        for col in ["label_10d", "db_ret_5d", "db_ret_10d", "db_ret_20d"]:
            if col in group.columns:
                ent = entered[col].dropna()
                drp = dropped[col].dropna()
                row[f"entered_mean_{col}"] = safe_float(ent.mean()) if not ent.empty else None
                row[f"dropped_mean_{col}"] = safe_float(drp.mean()) if not drp.empty else None
                row[f"net_mean_{col}"] = (
                    safe_float(ent.mean() - drp.mean()) if not ent.empty and not drp.empty else None
                )
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty or "net_mean_db_ret_10d" not in out.columns:
        return out
    return out.sort_values(["candidate", "net_mean_db_ret_10d"], ascending=[True, True])


def write_report(
    path: Path,
    task: dict[str, Any],
    merged_summary: pd.DataFrame,
    daily_deltas: pd.DataFrame,
    coeff_summary: pd.DataFrame,
) -> None:
    holdout = merged_summary[merged_summary["period"] == "holdout"].copy()
    if not holdout.empty:
        holdout = holdout.sort_values("net_mean_db_ret_10d", ascending=False)
    daily = daily_deltas.sort_values("mean_daily_diff_vs_loop10", ascending=False) if not daily_deltas.empty else daily_deltas
    coeff = coeff_summary.sort_values("mean_abs_delta_vs_loop10", ascending=True) if not coeff_summary.empty else coeff_summary

    def table(df: pd.DataFrame, cols: list[str]) -> str:
        if df.empty:
            return "(no rows)"
        existing = [c for c in cols if c in df.columns]
        return "```text\n" + df[existing].to_string(index=False) + "\n```"

    lines = [
        f"# Loop10-Centered HMM Attribution - {task.get('task_id')}",
        "",
        f"- Task status: `{task.get('status')}`",
        f"- Baseline: `LOOP10_BASE__penalty_only_f096` / `{LOOP10_SNAPSHOT_ID}`",
        f"- Holdout split: `{HOLDOUT_START}`",
        "",
        "## QE Daily Delta vs Loop10",
        "",
        table(
            daily,
            [
                "loop_index",
                "loop_label",
                "mean_daily_diff_vs_loop10",
                "t_stat_vs_loop10",
                "win_rate_vs_loop10",
                "worst_daily_diff_vs_loop10",
            ],
        ),
        "",
        "## TopK Replacement Attribution vs Loop10 (Holdout)",
        "",
        table(
            holdout,
            [
                "candidate",
                "loop_index",
                "loop_label",
                "changed_days",
                "avg_entered_per_day",
                "net_mean_label_10d",
                "net_mean_db_ret_5d",
                "net_mean_db_ret_10d",
                "net_mean_db_ret_20d",
                "positive_net_label_day_ratio",
            ],
        ),
        "",
        "## Coefficient Delta vs Loop10",
        "",
        table(
            coeff,
            [
                "candidate",
                "loop_index",
                "mean_abs_delta_vs_loop10",
                "candidate_gt_loop10_share",
                "candidate_lt_loop10_share",
                "candidate_penalty_share",
                "candidate_boost_share",
                "candidate_min",
                "candidate_max",
            ],
        ),
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", default=DEFAULT_TASK_ID)
    parser.add_argument("--task-detail-json", type=Path, default=DEFAULT_DETAIL_JSON)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--hmm-diag-dir", type=Path, default=DEFAULT_HMM_DIAG_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()

    task = load_task_detail(args.task_detail_json)
    loops = task_loop_rows(task)
    specs = load_candidate_specs(args.registry)
    spec_frame = build_spec_frame(specs, loops)
    loop10_spec = next(
        (spec for spec in specs if str(spec.get("snapshot_id")) == LOOP10_SNAPSHOT_ID),
        None,
    )
    if loop10_spec is None:
        raise RuntimeError(f"Loop10 coefficients not found in baseline specs: {LOOP10_SNAPSHOT_ID}")
    base_coeffs = load_coefficients(Path(loop10_spec["coefficients_path"]))

    pred_path, label_path = find_base_artifacts(args.hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(args.hmm_diag_dir)

    summary_rows: list[dict[str, Any]] = []
    rep_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    coeff_rows: list[dict[str, Any]] = []

    for spec in specs:
        snapshot_id = str(spec.get("snapshot_id") or "")
        if snapshot_id == LOOP10_SNAPSHOT_ID:
            continue
        # Keep the current task loops plus utility registry candidates; skip no-HMM.
        if not snapshot_id:
            continue
        coeff_path = Path(str(spec["coefficients_path"]))
        if not coeff_path.is_file():
            raise FileNotFoundError(f"coefficient file missing for {spec.get('label')}: {coeff_path}")
        candidate = str(spec.get("label") or snapshot_id)
        cand_coeffs = load_coefficients(coeff_path)
        rep, day = pairwise_topk_attribution(
            pred_ser,
            label_ser,
            stock_sector_map,
            base_coeffs,
            cand_coeffs,
            candidate,
            args.topk,
        )
        if not rep.empty:
            rep["snapshot_id"] = snapshot_id
            rep["loop_index"] = spec_frame.loc[
                spec_frame["snapshot_id"] == snapshot_id, "loop_index"
            ].dropna().head(1).squeeze() if snapshot_id in set(spec_frame["snapshot_id"]) else None
        if not day.empty:
            day["snapshot_id"] = snapshot_id
        enriched = enrich_db_forward_returns(rep, [5, 10, 20]) if not rep.empty else rep
        rep_frames.append(enriched)
        daily_frames.append(day)
        for row in summarize_pairwise_periods(enriched, day, candidate):
            row["snapshot_id"] = snapshot_id
            summary_rows.append(row)
        coeff_rows.append(coefficient_delta_summary(base_coeffs, cand_coeffs, candidate))

    summary = pd.DataFrame(summary_rows)
    replacements = pd.concat(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    topk_daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    coeff_summary = pd.DataFrame(coeff_rows)

    merge_cols = [
        "snapshot_id",
        "loop_index",
        "loop_label",
        "qe_annualized_return",
        "qe_sharpe",
        "qe_max_drawdown",
        "variant_name",
        "stage3_candidate",
        "stage3_transform",
        "stage3_range",
    ]
    merged_summary = summary.merge(
        spec_frame[[c for c in merge_cols if c in spec_frame.columns]],
        on="snapshot_id",
        how="left",
    )
    coeff_summary = coeff_summary.merge(
        spec_frame[[c for c in merge_cols if c in spec_frame.columns]],
        left_on="candidate",
        right_on=spec_frame["candidate"] if False else "snapshot_id",
        how="left",
    ) if False else coeff_summary.merge(
        spec_frame[["candidate", *[c for c in merge_cols if c != "snapshot_id" and c in spec_frame.columns]]],
        on="candidate",
        how="left",
    )

    daily_deltas, worst_dates = daily_portfolio_deltas(task, loops)
    sector_summary = sector_replacement_summary(replacements)

    out_dir = args.output_dir / args.task_id
    out_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "task_loop_metrics": out_dir / "task_loop_metrics.csv",
        "candidate_specs": out_dir / "candidate_specs.csv",
        "daily_portfolio_deltas": out_dir / "daily_portfolio_deltas_vs_loop10.csv",
        "worst_daily_dates": out_dir / "worst_daily_dates_vs_loop10.csv",
        "topk_summary": out_dir / "topk_pairwise_summary_vs_loop10.csv",
        "topk_replacements": out_dir / "topk_pairwise_replacements_vs_loop10.csv",
        "topk_daily": out_dir / "topk_pairwise_daily_vs_loop10.csv",
        "coefficient_delta": out_dir / "coefficient_delta_summary_vs_loop10.csv",
        "sector_summary": out_dir / "sector_replacement_summary_vs_loop10.csv",
        "report": out_dir / "loop10_centered_attribution_report.md",
    }
    loops.to_csv(files["task_loop_metrics"], index=False, encoding="utf-8-sig")
    spec_frame.to_csv(files["candidate_specs"], index=False, encoding="utf-8-sig")
    daily_deltas.to_csv(files["daily_portfolio_deltas"], index=False, encoding="utf-8-sig")
    worst_dates.to_csv(files["worst_daily_dates"], index=False, encoding="utf-8-sig")
    merged_summary.to_csv(files["topk_summary"], index=False, encoding="utf-8-sig")
    replacements.to_csv(files["topk_replacements"], index=False, encoding="utf-8-sig")
    topk_daily.to_csv(files["topk_daily"], index=False, encoding="utf-8-sig")
    coeff_summary.to_csv(files["coefficient_delta"], index=False, encoding="utf-8-sig")
    sector_summary.to_csv(files["sector_summary"], index=False, encoding="utf-8-sig")
    write_report(files["report"], task, merged_summary, daily_deltas, coeff_summary)

    print(
        json.dumps(
            {
                "task_id": args.task_id,
                "task_status": task.get("status"),
                "output_dir": str(out_dir),
                "files": {key: str(path) for key, path in files.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
