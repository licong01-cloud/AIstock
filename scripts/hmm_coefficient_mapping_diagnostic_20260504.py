#!/usr/bin/env python3
"""Evaluate HMM score-to-coefficient mappings before QE registration.

This script consumes score panels produced by
``hmm_sector_factor_retrain_diagnostic_20260504.py`` and evaluates how HMM
posterior/utility scores behave after being mapped to practical QE-style
sector coefficients.  It is intentionally file-only: no DB writes, no HMM
snapshot registration, and no QE task submission.
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
HORIZONS = (5, 10, 20)
HORIZON_WEIGHTS = {5: 0.35, 10: 0.35, 20: 0.30}
DEFAULT_SOURCE_DIRS = [
    ".codex_tmp/hmm_sector_factor_stage2_diag3_20260504",
    ".codex_tmp/hmm_sector_factor_stage2_diag2_20260504",
    ".codex_tmp/hmm_sector_factor_stage2_diag4_20260504",
]
DEFAULT_CANDIDATES = {
    "flow_plus_breadth",
    "flow_core",
    "flow_std_only",
    "vol_compress",
    "baseline_legacy7_winsor_zscore",
}
DEFAULT_SCORE_COLUMNS = ("utility_raw_score", "hmm_score")


@dataclass(frozen=True)
class RangeSpec:
    name: str
    low: float
    high: float
    description: str


@dataclass(frozen=True)
class TransformSpec:
    name: str
    description: str


RANGES = (
    RangeSpec("conservative_0p98_1p03", 0.98, 1.03, "Conservative coefficient range."),
    RangeSpec("neutral_0p97_1p05", 0.97, 1.05, "Neutral coefficient range."),
    RangeSpec("aggressive_0p95_1p08", 0.95, 1.08, "Aggressive coefficient range."),
)
TRANSFORMS = (
    TransformSpec("cs_rank", "Same-date cross-sectional rank mapped to coefficient range."),
    TransformSpec("cs_zscore_clip2", "Same-date cross-sectional z-score clipped to +/-2."),
    TransformSpec("val_zscore_clip2", "Validation-window global z-score clipped to +/-2."),
    TransformSpec("val_softsign", "Validation-window global z-score softsign compression."),
)


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def safe_tstat(values: Iterable[float]) -> float:
    series = pd.Series(list(values), dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(series) < 2:
        return float("nan")
    std = series.std(ddof=1)
    if not std or math.isnan(std):
        return float("nan")
    return float(series.mean() / (std / math.sqrt(len(series))))


def centered_to_coeff(centered: pd.Series | np.ndarray, low: float, high: float) -> np.ndarray:
    arr = np.asarray(centered, dtype=np.float64)
    arr = np.clip(arr, -1.0, 1.0)
    return np.where(arr >= 0, 1.0 + arr * (high - 1.0), 1.0 + arr * (1.0 - low))


def score_centered(frame: pd.DataFrame, score_col: str, transform: str) -> pd.Series:
    score = frame[score_col].astype(float)
    if transform == "cs_rank":
        ranks = score.groupby(frame["trade_date"]).rank(pct=True)
        return (2.0 * ranks - 1.0).fillna(0.0)
    if transform == "cs_zscore_clip2":
        by_date = score.groupby(frame["trade_date"])
        mean = by_date.transform("mean")
        std = by_date.transform("std").replace(0, np.nan)
        return (((score - mean) / std).clip(-2.0, 2.0) / 2.0).fillna(0.0)
    validation = frame.loc[frame["split"] == "validation", score_col].astype(float)
    mean = float(validation.mean()) if len(validation) else float(score.mean())
    std = float(validation.std(ddof=0)) if len(validation) else float(score.std(ddof=0))
    if not math.isfinite(std) or std < 1e-12:
        std = 1.0
    z = (score - mean) / std
    if transform == "val_zscore_clip2":
        return (z.clip(-2.0, 2.0) / 2.0).fillna(0.0)
    if transform == "val_softsign":
        soft = z / (1.0 + z.abs())
        return soft.clip(-1.0, 1.0).fillna(0.0)
    raise ValueError(f"Unknown transform: {transform}")


def load_run_config(source_dir: Path) -> dict[str, Any]:
    path = source_dir / "run_config.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_summary(source_dir: Path) -> pd.DataFrame:
    path = source_dir / "summary.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def iter_score_panels(source_dirs: list[Path], candidates: set[str]) -> list[tuple[Path, str, Path]]:
    panels: list[tuple[Path, str, Path]] = []
    for source_dir in source_dirs:
        models_dir = source_dir / "models"
        if not models_dir.exists():
            continue
        for path in sorted(models_dir.glob("*/score_panel.csv")):
            candidate = path.parent.name
            if candidates and candidate not in candidates:
                continue
            panels.append((source_dir, candidate, path))
    return panels


def evaluate_mapping(frame: pd.DataFrame, meta: dict[str, Any], mapping_name: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame = frame.sort_values(["sector_code", "trade_date"])
    frame["prev_coeff"] = frame.groupby("sector_code")["coefficient"].shift(1)
    frame["coeff_changed"] = (frame["prev_coeff"].notna()) & ((frame["coefficient"] - frame["prev_coeff"]).abs() > 0.001)
    frame["abs_coeff_change"] = (frame["coefficient"] - frame["prev_coeff"]).abs()

    for split in ("validation", "holdout"):
        split_frame = frame[frame["split"] == split].copy()
        if split_frame.empty:
            continue
        for horizon in HORIZONS:
            label_col = f"fwd_excess_{horizon}d"
            valid = split_frame.dropna(subset=["coefficient", label_col]).copy()
            if valid.empty:
                rank_ics: list[float] = []
                spreads: list[float] = []
                hits: list[float] = []
                tilt_returns: list[float] = []
                coeff_ranges: list[float] = []
                coeff_abs_devs: list[float] = []
                active_rates: list[float] = []
                coeff_change_rates: list[float] = []
                coeff_abs_changes: list[float] = []
            else:
                by_date = valid.groupby("trade_date", sort=True)
                counts = by_date[label_col].size()
                eligible_dates = counts[counts >= 20].index
                valid = valid[valid["trade_date"].isin(eligible_dates)].copy()
                by_date = valid.groupby("trade_date", sort=True)

                valid["coeff_rank"] = by_date["coefficient"].rank(method="average")
                valid["fwd_rank"] = by_date[label_col].rank(method="average")
                x_mean = by_date["coeff_rank"].transform("mean")
                y_mean = by_date["fwd_rank"].transform("mean")
                x_dev = valid["coeff_rank"] - x_mean
                y_dev = valid["fwd_rank"] - y_mean
                cov = (x_dev * y_dev).groupby(valid["trade_date"]).sum()
                x_ss = (x_dev * x_dev).groupby(valid["trade_date"]).sum()
                y_ss = (y_dev * y_dev).groupby(valid["trade_date"]).sum()
                denom = np.sqrt(x_ss * y_ss).replace(0, np.nan)
                daily_rank_ic = cov / denom

                coeff_pct_rank = by_date["coefficient"].rank(pct=True)
                top_mask = coeff_pct_rank >= 0.8
                bottom_mask = coeff_pct_rank <= 0.2
                fwd = valid[label_col].astype(float)
                top_mean = fwd.where(top_mask).groupby(valid["trade_date"]).mean()
                bottom_mean = fwd.where(bottom_mask).groupby(valid["trade_date"]).mean()
                daily_spread = top_mean - bottom_mean
                daily_median = by_date[label_col].transform("median")
                top_hit = (fwd.where(top_mask) > daily_median).where(top_mask).groupby(valid["trade_date"]).mean()
                bottom_hit = (fwd.where(bottom_mask) < daily_median).where(bottom_mask).groupby(valid["trade_date"]).mean()
                daily_hit = 0.5 * (top_hit + bottom_hit)

                coeff = valid["coefficient"].astype(float)
                coeff_centered = coeff - by_date["coefficient"].transform("mean")
                daily_tilt = (coeff_centered * fwd).groupby(valid["trade_date"]).mean()
                daily_coeff_range = by_date["coefficient"].max() - by_date["coefficient"].min()
                daily_abs_dev = (coeff - 1.0).abs().groupby(valid["trade_date"]).mean()
                daily_active_rate = ((coeff - 1.0).abs() > 0.001).astype(float).groupby(valid["trade_date"]).mean()
                daily_change_rate = valid["coeff_changed"].astype(float).groupby(valid["trade_date"]).mean()
                daily_abs_change = valid["abs_coeff_change"].groupby(valid["trade_date"]).mean()

                rank_ics = daily_rank_ic.dropna().astype(float).tolist()
                spreads = daily_spread.dropna().astype(float).tolist()
                hits = daily_hit.dropna().astype(float).tolist()
                tilt_returns = daily_tilt.dropna().astype(float).tolist()
                coeff_ranges = daily_coeff_range.dropna().astype(float).tolist()
                coeff_abs_devs = daily_abs_dev.dropna().astype(float).tolist()
                active_rates = daily_active_rate.dropna().astype(float).tolist()
                coeff_change_rates = daily_change_rate.dropna().astype(float).tolist()
                coeff_abs_changes = daily_abs_change.dropna().astype(float).tolist()

                daily_metric_frame = pd.DataFrame(
                    {
                        "rank_ic": daily_rank_ic,
                        "top_bottom_spread": daily_spread,
                        "hit_rate": daily_hit,
                        "tilt_return": daily_tilt,
                        "coeff_range": daily_coeff_range,
                        "avg_abs_coeff_dev": daily_abs_dev,
                        "active_rate": daily_active_rate,
                        "change_rate": daily_change_rate,
                        "avg_abs_coeff_change": daily_abs_change,
                        "sector_count": counts.reindex(eligible_dates),
                    }
                )
                for td, row in daily_metric_frame.iterrows():
                    daily_rows.append(
                        {
                            **meta,
                            "mapping_name": mapping_name,
                            "split": split,
                            "trade_date": pd.Timestamp(td).date().isoformat(),
                            "horizon": horizon,
                            "rank_ic": float(row["rank_ic"]) if pd.notna(row["rank_ic"]) else float("nan"),
                            "top_bottom_spread": float(row["top_bottom_spread"]) if pd.notna(row["top_bottom_spread"]) else float("nan"),
                            "hit_rate": float(row["hit_rate"]) if pd.notna(row["hit_rate"]) else float("nan"),
                            "tilt_return": float(row["tilt_return"]) if pd.notna(row["tilt_return"]) else float("nan"),
                            "coeff_range": float(row["coeff_range"]) if pd.notna(row["coeff_range"]) else float("nan"),
                            "avg_abs_coeff_dev": float(row["avg_abs_coeff_dev"]) if pd.notna(row["avg_abs_coeff_dev"]) else float("nan"),
                            "active_rate": float(row["active_rate"]) if pd.notna(row["active_rate"]) else float("nan"),
                            "change_rate": float(row["change_rate"]) if pd.notna(row["change_rate"]) else float("nan"),
                            "avg_abs_coeff_change": float(row["avg_abs_coeff_change"]) if pd.notna(row["avg_abs_coeff_change"]) else float("nan"),
                            "sector_count": int(row["sector_count"]) if pd.notna(row["sector_count"]) else 0,
                        }
                    )

            summary_rows.append(
                {
                    **meta,
                    "mapping_name": mapping_name,
                    "split": split,
                    "horizon": horizon,
                    "rank_ic_mean": float(np.mean(rank_ics)) if rank_ics else float("nan"),
                    "rank_ic_t": safe_tstat(rank_ics),
                    "rank_ic_pos_ratio": float(np.mean(np.asarray(rank_ics) > 0)) if rank_ics else float("nan"),
                    "top_bottom_spread_mean": float(np.mean(spreads)) if spreads else float("nan"),
                    "top_bottom_spread_win_ratio": float(np.mean(np.asarray(spreads) > 0)) if spreads else float("nan"),
                    "hit_rate_mean": float(np.mean(hits)) if hits else float("nan"),
                    "tilt_return_mean": float(np.mean(tilt_returns)) if tilt_returns else float("nan"),
                    "tilt_return_t": safe_tstat(tilt_returns),
                    "tilt_return_pos_ratio": float(np.mean(np.asarray(tilt_returns) > 0)) if tilt_returns else float("nan"),
                    "avg_coeff_range": float(np.mean(coeff_ranges)) if coeff_ranges else float("nan"),
                    "avg_abs_coeff_dev": float(np.mean(coeff_abs_devs)) if coeff_abs_devs else float("nan"),
                    "avg_active_rate": float(np.mean(active_rates)) if active_rates else float("nan"),
                    "avg_coeff_change_rate": float(np.mean(coeff_change_rates)) if coeff_change_rates else float("nan"),
                    "avg_abs_coeff_change": float(np.mean(coeff_abs_changes)) if coeff_abs_changes else float("nan"),
                    "daily_count": len(rank_ics),
                }
            )
    return daily_rows, summary_rows


def weighted_holdout_summary(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    key_cols = [
        "source_dir",
        "candidate",
        "n_states",
        "covariance_type",
        "score_column",
        "transform",
        "range_name",
        "coefficient_low",
        "coefficient_high",
        "mapping_name",
    ]
    holdout = summary[summary["split"] == "holdout"].copy()
    for key, group in holdout.groupby(key_cols, dropna=False):
        base = dict(zip(key_cols, key))
        item: dict[str, Any] = dict(base)
        for metric in (
            "rank_ic_mean",
            "top_bottom_spread_mean",
            "hit_rate_mean",
            "tilt_return_mean",
            "avg_coeff_range",
            "avg_abs_coeff_dev",
            "avg_active_rate",
            "avg_coeff_change_rate",
            "avg_abs_coeff_change",
        ):
            total = 0.0
            used = 0.0
            for horizon, weight in HORIZON_WEIGHTS.items():
                row = group[group["horizon"] == horizon]
                if row.empty:
                    continue
                value = float(row[metric].iloc[0])
                if math.isfinite(value):
                    total += weight * value
                    used += weight
            item[f"weighted_{metric}"] = total / used if used else float("nan")
        for horizon in HORIZONS:
            row = group[group["horizon"] == horizon]
            if row.empty:
                continue
            for metric in ("rank_ic_mean", "top_bottom_spread_mean", "hit_rate_mean", "tilt_return_mean"):
                item[f"{metric}_{horizon}d"] = float(row[metric].iloc[0])
        rows.append(item)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["efficiency_tilt_per_abs_dev"] = out["weighted_tilt_return_mean"] / out["weighted_avg_abs_coeff_dev"].replace(0, np.nan)
    return out.sort_values(
        ["weighted_rank_ic_mean", "weighted_tilt_return_mean", "weighted_hit_rate_mean"],
        ascending=[False, False, False],
    )


def write_report(output_dir: Path, ranked: pd.DataFrame) -> Path:
    path = output_dir / "report.md"
    cols = [
        "candidate",
        "n_states",
        "score_column",
        "transform",
        "range_name",
        "weighted_rank_ic_mean",
        "rank_ic_mean_5d",
        "rank_ic_mean_10d",
        "rank_ic_mean_20d",
        "weighted_top_bottom_spread_mean",
        "weighted_hit_rate_mean",
        "weighted_tilt_return_mean",
        "weighted_avg_abs_coeff_dev",
        "weighted_avg_coeff_change_rate",
    ]
    present = [col for col in cols if col in ranked.columns]
    top = ranked[present].head(25)
    lines = [
        "# HMM Coefficient Mapping Diagnostic",
        "",
        f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
        "- Scope: score-panel file diagnostics only; no DB writes, no snapshot registration, no QE task submission.",
        "- Coefficient mapping is evaluated against sector forward excess returns before deciding whether any HMM version should enter QE.",
        "",
        "## Top mappings",
        "",
        "| " + " | ".join(present) + " |",
        "| " + " | ".join(["---"] * len(present)) + " |",
    ]
    for _, row in top.iterrows():
        vals = []
        for col in present:
            value = row[col]
            if isinstance(value, float):
                vals.append("" if math.isnan(value) else f"{value:.6f}")
            else:
                vals.append(str(value))
        lines.append("| " + " | ".join(vals) + " |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- RankIC and top-bottom spread mostly test whether the HMM score orders sectors correctly.",
            "- `weighted_tilt_return_mean` and coefficient-change metrics test whether coefficient amplitude is practical.",
            "- Conservative mappings are preferred when RankIC is close, because QE stock-level backtests will add turnover and transaction-cost drag.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dirs", nargs="*", default=DEFAULT_SOURCE_DIRS)
    parser.add_argument("--candidates", nargs="*", default=sorted(DEFAULT_CANDIDATES))
    parser.add_argument("--score-columns", nargs="*", default=list(DEFAULT_SCORE_COLUMNS))
    parser.add_argument("--output-dir", default=".codex_tmp/hmm_coeffmap_stage3_20260504")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_dirs = [Path(item) for item in args.source_dirs]
    candidates = set(args.candidates)
    score_columns = list(args.score_columns)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        output_dir / "run_config.json",
        {
            "source_dirs": [str(path) for path in source_dirs],
            "candidates": sorted(candidates),
            "score_columns": score_columns,
            "ranges": [spec.__dict__ for spec in RANGES],
            "transforms": [spec.__dict__ for spec in TRANSFORMS],
            "horizons": HORIZONS,
            "horizon_weights": HORIZON_WEIGHTS,
            "safety": {"db_writes": False, "snapshot_registration": False, "qe_task_submission": False},
        },
    )

    panel_refs = iter_score_panels(source_dirs, candidates)
    if not panel_refs:
        raise RuntimeError("No score_panel.csv files found for requested sources/candidates")

    all_daily: list[dict[str, Any]] = []
    all_summary: list[dict[str, Any]] = []
    loaded_rows: list[dict[str, Any]] = []
    for source_dir, candidate, path in panel_refs:
        run_config = load_run_config(source_dir)
        run_args = run_config.get("args", {})
        summary = load_summary(source_dir)
        candidate_summary = summary[summary["candidate"] == candidate] if not summary.empty and "candidate" in summary.columns else pd.DataFrame()
        frame = pd.read_csv(path)
        required = {"trade_date", "sector_code", "split", *[f"fwd_excess_{h}d" for h in HORIZONS]}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{path} missing required columns: {missing}")
        loaded_rows.append(
            {
                "source_dir": str(source_dir),
                "candidate": candidate,
                "rows": len(frame),
                "dates": frame["trade_date"].nunique(),
                "sectors": frame["sector_code"].nunique(),
                "n_states": run_args.get("n_states"),
                "covariance_type": run_args.get("covariance_type"),
            }
        )
        for score_col in score_columns:
            if score_col not in frame.columns:
                continue
            for transform in TRANSFORMS:
                centered = score_centered(frame, score_col, transform.name)
                for range_spec in RANGES:
                    work = frame.copy()
                    work["coefficient"] = centered_to_coeff(centered, range_spec.low, range_spec.high)
                    mapping_name = f"{score_col}__{transform.name}__{range_spec.name}"
                    meta = {
                        "source_dir": str(source_dir),
                        "candidate": candidate,
                        "n_states": int(run_args.get("n_states", -1)),
                        "covariance_type": str(run_args.get("covariance_type", "")),
                        "score_column": score_col,
                        "transform": transform.name,
                        "transform_description": transform.description,
                        "range_name": range_spec.name,
                        "coefficient_low": range_spec.low,
                        "coefficient_high": range_spec.high,
                        "range_description": range_spec.description,
                        "candidate_stage2_rank": int(candidate_summary.index[0]) + 1 if not candidate_summary.empty else None,
                    }
                    daily_rows, summary_rows = evaluate_mapping(work, meta, mapping_name)
                    all_daily.extend(daily_rows)
                    all_summary.extend(summary_rows)

    daily = pd.DataFrame(all_daily)
    summary_df = pd.DataFrame(all_summary)
    ranked = weighted_holdout_summary(summary_df)
    pd.DataFrame(loaded_rows).to_csv(output_dir / "loaded_score_panels.csv", index=False, encoding="utf-8-sig")
    daily.to_csv(output_dir / "daily_mapping_metrics.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(output_dir / "mapping_summary_by_horizon.csv", index=False, encoding="utf-8-sig")
    ranked.to_csv(output_dir / "mapping_summary_ranked.csv", index=False, encoding="utf-8-sig")
    report_path = write_report(output_dir, ranked)
    print("DONE")
    print(f"ranked={output_dir / 'mapping_summary_ranked.csv'}")
    print(f"report={report_path}")


if __name__ == "__main__":
    main()
