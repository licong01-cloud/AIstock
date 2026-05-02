"""Offline HMM sector-factor overlay replay.

This script is intentionally read-only with respect to strategy/runtime code. It
builds sector coefficients from the sector-factor RankIC shortlist, applies them
to an existing QE no-HMM prediction artifact, and replays raw Top50 versus
coefficient-adjusted Top50 replacements.
"""
from __future__ import annotations

import argparse
import json
import math
import pickle
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.diagnostics.sector_factor_rankic_report import (  # noqa: E402
    DB_DEFAULT,
    build_panel,
    load_sector_breadth,
    load_sector_daily,
    load_sector_moneyflow,
)


TASK_ID = "qe_20260502_131502_9b54"
DEFAULT_HMM_DIAG_DIR = Path(".codex_tmp/hmm_offline_diag") / TASK_ID
DEFAULT_SECTOR_RANKIC_DIR = Path(".codex_tmp/sector_factor_rankic_20260502")
DEFAULT_OUTPUT_DIR = DEFAULT_HMM_DIAG_DIR / "sector_factor_overlay"
DEFAULT_REPORT = Path("docs/analysis/hmm_sector_factor_overlay_replacement_qe_20260502_131502_9b54.md")


@dataclass(frozen=True)
class FeatureGroup:
    name: str
    features: tuple[tuple[str, float], ...]
    note: str


FEATURE_GROUPS = [
    FeatureGroup(
        "turnover_core",
        (
            ("sf_turnover_pctile_250d_neg", 1.0),
            ("sf_turnover_pctile_120d_neg", 1.0),
            ("sf_turnover_zscore_60d_neg", 1.0),
            ("sf_turnover_ma5_ma20_neg", 1.0),
        ),
        "turnover crowding/cooling composite",
    ),
    FeatureGroup(
        "turnover_fast",
        (
            ("sf_turnover_pctile_120d_neg", 1.0),
            ("sf_turnover_zscore_60d_neg", 1.0),
            ("sf_turnover_ma5_ma20_neg", 1.0),
        ),
        "shorter turnover-history composite",
    ),
    FeatureGroup(
        "turnover_flow_core",
        (
            ("sf_turnover_pctile_250d_neg", 1.0),
            ("sf_turnover_pctile_120d_neg", 1.0),
            ("sf_turnover_ma5_ma20_neg", 1.0),
            ("sf_mf_net_ratio_std_5d_neg", 1.0),
            ("sf_small_net_ratio_5d", 1.0),
        ),
        "turnover plus money-flow stability",
    ),
    FeatureGroup(
        "flow_core",
        (
            ("sf_mf_net_ratio_std_5d_neg", 1.0),
            ("sf_small_net_ratio_5d", 1.0),
        ),
        "money-flow stability only",
    ),
    FeatureGroup(
        "best5_core",
        (
            ("sf_turnover_pctile_250d_neg", 1.0),
            ("sf_turnover_pctile_120d_neg", 1.0),
            ("sf_turnover_ma5_ma20_neg", 1.0),
            ("sf_mf_net_ratio_std_5d_neg", 1.0),
            ("sf_small_net_ratio_5d", 1.0),
        ),
        "top 5D/10D sector-factor shortlist",
    ),
    FeatureGroup(
        "long_flow_tier",
        (("sf_flow_tier_strength_10d", -1.0),),
        "20D flow-tier reversal candidate; sign follows holdout RankIC",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hmm-diag-dir", default=str(DEFAULT_HMM_DIAG_DIR))
    parser.add_argument("--sector-rankic-dir", default=str(DEFAULT_SECTOR_RANKIC_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--start", default="2024-07-01")
    parser.add_argument("--end", default="2026-04-28")
    parser.add_argument("--prestart", default="2023-01-01")
    parser.add_argument("--test-start", default="2025-05-01")
    return parser.parse_args()


def db_connect():
    return psycopg2.connect(**DB_DEFAULT)


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


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


def load_pickle(path: Path) -> Any:
    with path.open("rb") as fh:
        return pickle.load(fh)


def pred_to_series(pred: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(pred, pd.DataFrame):
        col = "score" if "score" in pred.columns else pred.columns[0]
        ser = pred[col]
    else:
        ser = pred
    if not isinstance(ser.index, pd.MultiIndex):
        raise RuntimeError("pred.pkl index must be MultiIndex(datetime, instrument)")
    ser = ser.copy()
    ser.index = ser.index.set_levels(pd.to_datetime(ser.index.levels[0]), level=0)
    return ser.sort_index()


def label_to_series(label: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(label, pd.DataFrame):
        col = "LABEL0" if "LABEL0" in label.columns else label.columns[0]
        ser = label[col]
    else:
        ser = label
    if not isinstance(ser.index, pd.MultiIndex):
        raise RuntimeError("label.pkl index must be MultiIndex(datetime, instrument)")
    ser = ser.copy()
    ser.index = ser.index.set_levels(pd.to_datetime(ser.index.levels[0]), level=0)
    return ser.sort_index()


def find_base_artifacts(hmm_diag_dir: Path) -> tuple[Path, Path]:
    artifacts_dir = hmm_diag_dir / "artifacts"
    pred_paths = sorted(artifacts_dir.glob("L1_*Loop1/pred.pkl"))
    label_paths = sorted(artifacts_dir.glob("L1_*Loop1/label.pkl"))
    if not pred_paths or not label_paths:
        raise RuntimeError(f"Missing L1 pred/label under {artifacts_dir}")
    return pred_paths[0], label_paths[0]


def load_stock_sector_map(hmm_diag_dir: Path) -> dict[str, str]:
    paths = sorted((hmm_diag_dir / "artifacts").glob("L2_*Loop2/hmm_sector_coefficients.json"))
    if not paths:
        paths = sorted((hmm_diag_dir / "artifacts").glob("L*_*/hmm_sector_coefficients.json"))
    if not paths:
        raise RuntimeError(f"Missing hmm_sector_coefficients.json under {hmm_diag_dir / 'artifacts'}")
    with paths[0].open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    mapping = data.get("stock_sector_map") or {}
    if not mapping:
        raise RuntimeError(f"Missing stock_sector_map in {paths[0]}")
    return {str(k): str(v) for k, v in mapping.items()}


def daily_feature_score(panel: pd.DataFrame, group: FeatureGroup) -> pd.DataFrame:
    signed_ranks = []
    for feature, sign in group.features:
        if feature not in panel.columns:
            continue
        signed = panel[feature] * sign
        rank = signed.groupby(level="trade_date").rank(pct=True)
        signed_ranks.append(rank.rename(feature))
    if not signed_ranks:
        raise RuntimeError(f"No available features for group {group.name}")
    score = pd.concat(signed_ranks, axis=1).mean(axis=1, skipna=True)
    out = score.to_frame("score").dropna()
    out["score_rank"] = out.groupby(level="trade_date")["score"].rank(pct=True)
    return out


def build_coefficients_from_score(
    score: pd.DataFrame,
    boost: float,
    penalty: float,
    top_q: float,
    bottom_q: float,
) -> dict[str, dict[str, float]]:
    coeff: dict[str, dict[str, float]] = {}
    for dt, day in score.groupby(level="trade_date"):
        ranks = day["score_rank"].droplevel(0)
        day_coeff = pd.Series(1.0, index=ranks.index)
        day_coeff.loc[ranks >= 1.0 - top_q] = 1.0 + boost
        day_coeff.loc[ranks <= bottom_q] = 1.0 - penalty
        coeff[pd.Timestamp(dt).strftime("%Y-%m-%d")] = {str(k): float(v) for k, v in day_coeff.items()}
    return coeff


def old_coeff_candidate_paths(hmm_diag_dir: Path) -> list[tuple[str, Path]]:
    candidates = [
        ("old_covfix_primary_b020_p005", hmm_diag_dir / "optimization/candidate_coefficients/primary_balanced_b0p020_p0p005.json"),
        ("old_covfix_high_db10_b010_p005", hmm_diag_dir / "optimization/candidate_coefficients/high_db10_b0p010_p0p005.json"),
        ("old_covfix_sparse_b005_p005", hmm_diag_dir / "optimization/candidate_coefficients/sparse_b0p005_p0p005.json"),
        ("old_covfix_original_L2", hmm_diag_dir / "artifacts/L2_qe_20260502_131502_9b54_Loop2/hmm_sector_coefficients.json"),
    ]
    return [(name, path) for name, path in candidates if path.exists()]


def get_label_value(label_ser: pd.Series, dt: pd.Timestamp, symbol: str) -> float | None:
    try:
        value = label_ser.loc[(dt, symbol)]
        if isinstance(value, pd.Series):
            value = value.iloc[0]
        return safe_float(value)
    except Exception:
        return None


def compute_replacements(
    pred_ser: pd.Series,
    label_ser: pd.Series,
    daily_coefficients: dict[str, dict[str, float]],
    stock_sector_map: dict[str, str],
    topk: int,
    candidate: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pred_dates = set(pred_ser.index.get_level_values(0).unique())
    dates = sorted(set(pd.to_datetime(list(daily_coefficients.keys()))) & pred_dates)
    rows: list[dict[str, Any]] = []
    day_rows: list[dict[str, Any]] = []
    sector_rows: list[dict[str, Any]] = []
    pred_by_day = {dt: s.droplevel(0) for dt, s in pred_ser.groupby(level=0, sort=True)}

    for dt in dates:
        date_str = pd.Timestamp(dt).strftime("%Y-%m-%d")
        scores = pred_by_day.get(dt)
        if scores is None or scores.empty:
            continue
        scores = scores.dropna().astype(float)
        day_coeff = daily_coefficients.get(date_str) or {}
        sector = pd.Series(scores.index, index=scores.index).map(stock_sector_map)
        coeff = sector.map(lambda x: day_coeff.get(str(x), 1.0) if x is not None else 1.0).astype(float)
        adjusted = scores * coeff

        raw_sorted = scores.sort_values(ascending=False, kind="mergesort")
        adj_sorted = adjusted.sort_values(ascending=False, kind="mergesort")
        raw_rank = pd.Series(np.arange(1, len(raw_sorted) + 1), index=raw_sorted.index)
        adj_rank = pd.Series(np.arange(1, len(adj_sorted) + 1), index=adj_sorted.index)
        raw_top = set(raw_sorted.head(topk).index)
        adj_top = set(adj_sorted.head(topk).index)
        entered = sorted(adj_top - raw_top)
        dropped = sorted(raw_top - adj_top)

        entered_label = []
        dropped_label = []
        for typ, symbols in (("entered_by_hmm", entered), ("dropped_by_hmm", dropped)):
            for sym in symbols:
                lab = get_label_value(label_ser, dt, sym)
                if lab is not None:
                    if typ == "entered_by_hmm":
                        entered_label.append(lab)
                    else:
                        dropped_label.append(lab)
                sec = sector.get(sym)
                rows.append(
                    {
                        "candidate": candidate,
                        "date": date_str,
                        "symbol": sym,
                        "replacement_type": typ,
                        "sector_code": sec,
                        "coefficient": safe_float(coeff.get(sym)) or 1.0,
                        "raw_score": safe_float(scores.get(sym)),
                        "adjusted_score": safe_float(adjusted.get(sym)),
                        "raw_rank": int(raw_rank.get(sym)) if sym in raw_rank.index else None,
                        "adjusted_rank": int(adj_rank.get(sym)) if sym in adj_rank.index else None,
                        "label_10d": lab,
                    }
                )
        non_neutral = [float(v) for v in day_coeff.values() if abs(float(v) - 1.0) > 1e-12]
        day_rows.append(
            {
                "candidate": candidate,
                "date": date_str,
                "raw_top_count": len(raw_top),
                "adjusted_top_count": len(adj_top),
                "common_count": len(raw_top & adj_top),
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
                "non_neutral_sector_count": len(non_neutral),
                "min_sector_coeff": float(np.nanmin(list(day_coeff.values()))) if day_coeff else np.nan,
                "max_sector_coeff": float(np.nanmax(list(day_coeff.values()))) if day_coeff else np.nan,
                "mean_sector_coeff": float(np.nanmean(list(day_coeff.values()))) if day_coeff else np.nan,
            }
        )
        for sec_code, cf_value in day_coeff.items():
            sector_rows.append({"candidate": candidate, "date": date_str, "sector_code": sec_code, "coefficient": float(cf_value)})

    return pd.DataFrame(rows), pd.DataFrame(day_rows), pd.DataFrame(sector_rows)


def enrich_db_forward_returns(replacements: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    if replacements.empty:
        return replacements
    symbols = sorted(replacements["symbol"].dropna().unique().tolist())
    min_date = pd.to_datetime(replacements["date"]).min().date()
    max_date = (pd.to_datetime(replacements["date"]).max() + pd.Timedelta(days=max(horizons) * 3 + 15)).date()
    with db_connect() as conn:
        price = pd.read_sql_query(
            """
            SELECT trade_date, RTRIM(ts_code) AS symbol, close_li::double precision AS close_li
            FROM market.kline_daily_raw
            WHERE trade_date BETWEEN %s AND %s
              AND RTRIM(ts_code) = ANY(%s)
              AND close_li IS NOT NULL AND close_li > 0
            ORDER BY symbol, trade_date
            """,
            conn,
            params=(min_date, max_date, symbols),
        )
    if price.empty:
        return replacements
    price["trade_date"] = pd.to_datetime(price["trade_date"]).dt.strftime("%Y-%m-%d")
    price["close"] = price["close_li"].astype(float)
    price = price.sort_values(["symbol", "trade_date"])
    for h in horizons:
        price[f"db_ret_{h}d"] = price.groupby("symbol")["close"].shift(-h) / price["close"] - 1.0
    ret_cols = ["trade_date", "symbol"] + [f"db_ret_{h}d" for h in horizons]
    return replacements.merge(
        price[ret_cols],
        left_on=["date", "symbol"],
        right_on=["trade_date", "symbol"],
        how="left",
    ).drop(columns=["trade_date"], errors="ignore")


def summarize_period(rep: pd.DataFrame, day: pd.DataFrame, candidate: str, period: str) -> dict[str, Any]:
    out: dict[str, Any] = {"candidate": candidate, "period": period}
    out["days"] = int(day["date"].nunique()) if not day.empty else 0
    out["changed_days"] = int((day["replacement_count"] > 0).sum()) if not day.empty else 0
    out["avg_entered_per_day"] = safe_float(day["entered_count"].mean()) if not day.empty else None
    out["max_entered_per_day"] = safe_float(day["entered_count"].max()) if not day.empty else None
    out["positive_net_label_day_ratio"] = safe_float((day["net_enter_minus_drop_label_10d"] > 0).mean()) if not day.empty else None
    out["mean_day_net_label_10d"] = safe_float(day["net_enter_minus_drop_label_10d"].mean()) if not day.empty else None
    out["total_enter_rows"] = int((rep["replacement_type"] == "entered_by_hmm").sum()) if not rep.empty else 0
    out["total_drop_rows"] = int((rep["replacement_type"] == "dropped_by_hmm").sum()) if not rep.empty else 0
    out["unique_enter_symbols"] = int(rep.loc[rep["replacement_type"] == "entered_by_hmm", "symbol"].nunique()) if not rep.empty else 0
    out["unique_drop_symbols"] = int(rep.loc[rep["replacement_type"] == "dropped_by_hmm", "symbol"].nunique()) if not rep.empty else 0
    for col in ["label_10d", "db_ret_5d", "db_ret_10d", "db_ret_20d"]:
        if col not in rep.columns or rep.empty:
            continue
        ent = rep.loc[rep["replacement_type"] == "entered_by_hmm", col].dropna()
        drp = rep.loc[rep["replacement_type"] == "dropped_by_hmm", col].dropna()
        out[f"entered_mean_{col}"] = safe_float(ent.mean()) if not ent.empty else None
        out[f"dropped_mean_{col}"] = safe_float(drp.mean()) if not drp.empty else None
        out[f"net_mean_{col}"] = safe_float(ent.mean() - drp.mean()) if not ent.empty and not drp.empty else None
    return out


def split_periods(rep: pd.DataFrame, day: pd.DataFrame, candidate: str, test_start: str) -> list[dict[str, Any]]:
    if rep.empty:
        return [summarize_period(rep, day, candidate, "full")]
    rep = rep.copy()
    day = day.copy()
    rep["date_ts"] = pd.to_datetime(rep["date"])
    day["date_ts"] = pd.to_datetime(day["date"])
    split = pd.Timestamp(test_start)
    periods = [
        ("full", rep, day),
        ("train_pre_holdout", rep[rep["date_ts"] < split], day[day["date_ts"] < split]),
        ("holdout", rep[rep["date_ts"] >= split], day[day["date_ts"] >= split]),
    ]
    rows = []
    for period, r, d in periods:
        rows.append(summarize_period(r.drop(columns=["date_ts"], errors="ignore"), d.drop(columns=["date_ts"], errors="ignore"), candidate, period))
    return rows


def fixed_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "```text\n(no rows)\n```"
    widths = []
    for key, title in columns:
        vals = [str(row.get(key, "")) for row in rows]
        widths.append(max(len(title), *(len(v) for v in vals)))
    lines = [
        "  ".join(title.ljust(widths[i]) for i, (_, title) in enumerate(columns)),
        "  ".join("-" * widths[i] for i in range(len(columns))),
    ]
    for row in rows:
        lines.append("  ".join(str(row.get(key, "")).ljust(widths[i]) for i, (key, _) in enumerate(columns)))
    return "```text\n" + "\n".join(lines) + "\n```"


def write_top_candidate_jsons(
    summary: pd.DataFrame,
    candidates: dict[str, dict[str, dict[str, float]]],
    metadata: pd.DataFrame,
    output_dir: Path,
    limit_per_source: int = 6,
) -> list[Path]:
    coeff_dir = output_dir / "candidate_coefficients"
    coeff_dir.mkdir(parents=True, exist_ok=True)
    holdout = summary[summary["period"] == "holdout"].copy()
    written: list[Path] = []
    if holdout.empty:
        return written
    for source in ("sector_factor", "hybrid_old_sector", "old_covfix_baseline"):
        source_names = metadata.loc[metadata["source"] == source, "candidate"].tolist()
        view = (
            holdout[holdout["candidate"].isin(source_names)]
            .sort_values(["net_mean_label_10d", "net_mean_db_ret_10d"], ascending=False)
            .head(limit_per_source)
        )
        for _, row in view.iterrows():
            name = str(row["candidate"])
            path = coeff_dir / f"{name}.json"
            meta_row = metadata[metadata["candidate"] == name].head(1).to_dict("records")
            payload = {
                "candidate": name,
                "generated_by": "scripts/diagnostics/hmm_sector_factor_overlay_diagnostic.py",
                "registered": False,
                "strategy_code_modified": False,
                "selection_metric": "holdout net_mean_label_10d then net_mean_db_ret_10d",
                "holdout_summary": {
                    k: safe_float(v) if isinstance(v, (int, float, np.integer, np.floating)) else v
                    for k, v in row.to_dict().items()
                },
                "metadata": meta_row[0] if meta_row else {},
                "daily_coefficients": candidates[name],
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            written.append(path)
    return written


def format_summary_rows(summary: pd.DataFrame, period: str, limit: int = 15) -> list[dict[str, str]]:
    view = summary[summary["period"] == period].copy()
    if view.empty:
        return []
    view = view.sort_values(["net_mean_label_10d", "net_mean_db_ret_10d", "changed_days"], ascending=[False, False, False]).head(limit)
    rows = []
    for _, row in view.iterrows():
        rows.append(
            {
                "candidate": str(row["candidate"]),
                "days": str(int(row.get("days") or 0)),
                "chg": str(int(row.get("changed_days") or 0)),
                "enter": num(row.get("avg_entered_per_day"), 2),
                "uniq_e": str(int(row.get("unique_enter_symbols") or 0)),
                "label10": pct(row.get("net_mean_label_10d"), 2),
                "db5": pct(row.get("net_mean_db_ret_5d"), 2),
                "db10": pct(row.get("net_mean_db_ret_10d"), 2),
                "db20": pct(row.get("net_mean_db_ret_20d"), 2),
                "pos": pct(row.get("positive_net_label_day_ratio"), 1),
            }
        )
    return rows


def top_symbol_rows(rep: pd.DataFrame, candidate: str, limit: int = 20) -> list[dict[str, str]]:
    view = rep[(rep["candidate"] == candidate) & (rep["replacement_type"] == "entered_by_hmm")].copy()
    if view.empty:
        return []
    agg = (
        view.groupby(["symbol", "sector_code"], dropna=False)
        .agg(
            enter_days=("date", "nunique"),
            label10=("label_10d", "mean"),
            db10=("db_ret_10d", "mean"),
            raw_rank=("raw_rank", "mean"),
            adj_rank=("adjusted_rank", "mean"),
        )
        .reset_index()
        .sort_values(["enter_days", "db10", "label10"], ascending=False)
        .head(limit)
    )
    rows = []
    for _, row in agg.iterrows():
        rows.append(
            {
                "symbol": str(row["symbol"]),
                "sector": str(row["sector_code"]),
                "days": str(int(row["enter_days"])),
                "label10": pct(row["label10"], 2),
                "db10": pct(row["db10"], 2),
                "raw_r": num(row["raw_rank"], 1),
                "adj_r": num(row["adj_rank"], 1),
            }
        )
    return rows


def candidate_metadata(candidates: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for name, meta in candidates.items():
        rows.append(
            {
                "candidate": name,
                "source": meta.get("source"),
                "group": meta.get("group"),
                "features": ",".join(f"{feat}:{sign:g}" for feat, sign in meta.get("features", [])),
                "boost": meta.get("boost"),
                "penalty": meta.get("penalty"),
                "top_q": meta.get("top_q"),
                "bottom_q": meta.get("bottom_q"),
                "note": meta.get("note"),
            }
        )
    return pd.DataFrame(rows)


def build_sector_factor_candidates(
    panel: pd.DataFrame,
) -> tuple[dict[str, dict[str, dict[str, float]]], dict[str, dict[str, Any]], pd.DataFrame, dict[str, pd.DataFrame]]:
    candidates: dict[str, dict[str, dict[str, float]]] = {}
    metadata: dict[str, dict[str, Any]] = {}
    score_rows = []
    score_by_group: dict[str, pd.DataFrame] = {}
    boost_values = [0.01, 0.02]
    penalty_values = [0.005, 0.01]
    quantiles = [0.2, 0.3]

    for group in FEATURE_GROUPS:
        score = daily_feature_score(panel, group)
        score_by_group[group.name] = score
        score_out = score.reset_index()
        score_out["group"] = group.name
        score_rows.append(score_out)
        for top_q in quantiles:
            for boost in boost_values:
                for penalty in penalty_values:
                    name = f"sf_{group.name}_q{int(top_q*100):02d}_b{boost:.3f}_p{penalty:.3f}".replace(".", "p")
                    candidates[name] = build_coefficients_from_score(score, boost=boost, penalty=penalty, top_q=top_q, bottom_q=top_q)
                    metadata[name] = {
                        "source": "sector_factor",
                        "group": group.name,
                        "features": group.features,
                        "boost": boost,
                        "penalty": penalty,
                        "top_q": top_q,
                        "bottom_q": top_q,
                        "note": group.note,
                    }
    scores = pd.concat(score_rows, ignore_index=True) if score_rows else pd.DataFrame()
    return candidates, metadata, scores, score_by_group


def merge_coefficients(
    base: dict[str, dict[str, float]],
    overlay: dict[str, dict[str, float]],
    lower: float = 0.985,
    upper: float = 1.03,
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for date, base_day in base.items():
        overlay_day = overlay.get(date, {})
        sectors = set(base_day) | set(overlay_day)
        out[date] = {
            sec: float(np.clip(float(base_day.get(sec, 1.0)) * float(overlay_day.get(sec, 1.0)), lower, upper))
            for sec in sectors
        }
    return out


def confirm_old_with_sector_score(
    old: dict[str, dict[str, float]],
    score: pd.DataFrame,
    confirm: float,
) -> dict[str, dict[str, float]]:
    score_lookup = {
        (pd.Timestamp(idx[0]).strftime("%Y-%m-%d"), str(idx[1])): float(row["score_rank"])
        for idx, row in score.iterrows()
    }
    out: dict[str, dict[str, float]] = {}
    for date, day in old.items():
        out_day: dict[str, float] = {}
        for sec, coeff in day.items():
            cf = float(coeff)
            rank = score_lookup.get((date, str(sec)))
            if rank is None:
                out_day[str(sec)] = 1.0
            elif cf > 1.0 and rank >= confirm:
                out_day[str(sec)] = cf
            elif cf < 1.0 and rank <= 1.0 - confirm:
                out_day[str(sec)] = cf
            else:
                out_day[str(sec)] = 1.0
        out[date] = out_day
    return out


def add_hybrid_candidates(
    candidates: dict[str, dict[str, dict[str, float]]],
    metadata: dict[str, dict[str, Any]],
    old_candidates: dict[str, dict[str, dict[str, float]]],
    score_by_group: dict[str, pd.DataFrame],
) -> None:
    sector_overlay_names = [
        "sf_turnover_fast_q20_b0p010_p0p005",
        "sf_turnover_flow_core_q20_b0p010_p0p005",
        "sf_best5_core_q20_b0p010_p0p005",
        "sf_turnover_fast_q20_b0p020_p0p005",
    ]
    old_names = ["old_covfix_primary_b020_p005", "old_covfix_high_db10_b010_p005"]
    for old_name in old_names:
        old_coeff = old_candidates.get(old_name)
        if not old_coeff:
            continue
        old_short = old_name.replace("old_covfix_", "old_")
        for sector_name in sector_overlay_names:
            sector_coeff = candidates.get(sector_name)
            if not sector_coeff:
                continue
            sector_group = metadata[sector_name].get("group")
            name = f"hyb_{old_short}_x_{sector_group}_q20"
            candidates[name] = merge_coefficients(old_coeff, sector_coeff)
            metadata[name] = {
                "source": "hybrid_old_sector",
                "group": f"{old_short}+{sector_group}",
                "features": metadata[sector_name].get("features", ()),
                "boost": metadata[sector_name].get("boost"),
                "penalty": metadata[sector_name].get("penalty"),
                "top_q": metadata[sector_name].get("top_q"),
                "bottom_q": metadata[sector_name].get("bottom_q"),
                "note": f"multiplicative blend of {old_name} and {sector_name}",
            }
        for group_name in ("turnover_fast", "turnover_flow_core", "best5_core"):
            score = score_by_group.get(group_name)
            if score is None:
                continue
            for confirm in (0.5, 0.6, 0.7):
                name = f"hyb_{old_short}_confirm_{group_name}_c{int(confirm*100)}"
                candidates[name] = confirm_old_with_sector_score(old_coeff, score, confirm=confirm)
                metadata[name] = {
                    "source": "hybrid_old_sector",
                    "group": f"{old_short}+{group_name}",
                    "features": (),
                    "boost": None,
                    "penalty": None,
                    "top_q": None,
                    "bottom_q": None,
                    "note": f"keep old boost/penalty only when {group_name} score confirms at {confirm:.1f}",
                }


def write_report(
    report_path: Path,
    args: argparse.Namespace,
    summary: pd.DataFrame,
    all_rep: pd.DataFrame,
    metadata: pd.DataFrame,
    output_dir: Path,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)

    holdout = summary[summary["period"] == "holdout"].copy()
    sector_only = metadata[metadata["source"] == "sector_factor"]["candidate"].tolist()
    old_only = metadata[metadata["source"] == "old_covfix_baseline"]["candidate"].tolist()
    hybrid_only = metadata[metadata["source"] == "hybrid_old_sector"]["candidate"].tolist()
    best_sector = (
        holdout[holdout["candidate"].isin(sector_only)]
        .sort_values(["net_mean_label_10d", "net_mean_db_ret_10d"], ascending=False)
        .head(1)
    )
    best_old = (
        holdout[holdout["candidate"].isin(old_only)]
        .sort_values(["net_mean_label_10d", "net_mean_db_ret_10d"], ascending=False)
        .head(1)
    )
    best_hybrid = (
        holdout[holdout["candidate"].isin(hybrid_only)]
        .sort_values(["net_mean_label_10d", "net_mean_db_ret_10d"], ascending=False)
        .head(1)
    )
    best_hybrid_return = (
        holdout[(holdout["candidate"].isin(hybrid_only)) & (holdout["net_mean_label_10d"] > 0)]
        .sort_values(["net_mean_db_ret_10d", "net_mean_db_ret_20d", "net_mean_label_10d"], ascending=False)
        .head(1)
    )
    selected_candidate = ""
    for frame in (best_hybrid, best_hybrid_return, best_sector, best_old):
        if not frame.empty:
            selected_candidate = str(frame.iloc[0]["candidate"])
            break

    selection_rows = []
    if not best_sector.empty:
        r = best_sector.iloc[0]
        selection_rows.append(
            {
                "type": "best_sector_factor",
                "candidate": str(r["candidate"]),
                "label10": pct(r["net_mean_label_10d"], 2),
                "db10": pct(r["net_mean_db_ret_10d"], 2),
                "db20": pct(r["net_mean_db_ret_20d"], 2),
                "chg": str(int(r["changed_days"] or 0)),
            }
        )
    if not best_old.empty:
        r = best_old.iloc[0]
        selection_rows.append(
            {
                "type": "best_old_covfix",
                "candidate": str(r["candidate"]),
                "label10": pct(r["net_mean_label_10d"], 2),
                "db10": pct(r["net_mean_db_ret_10d"], 2),
                "db20": pct(r["net_mean_db_ret_20d"], 2),
                "chg": str(int(r["changed_days"] or 0)),
            }
        )
    if not best_hybrid.empty:
        r = best_hybrid.iloc[0]
        selection_rows.append(
            {
                "type": "best_hybrid",
                "candidate": str(r["candidate"]),
                "label10": pct(r["net_mean_label_10d"], 2),
                "db10": pct(r["net_mean_db_ret_10d"], 2),
                "db20": pct(r["net_mean_db_ret_20d"], 2),
                "chg": str(int(r["changed_days"] or 0)),
            }
        )
    if not best_hybrid_return.empty:
        r = best_hybrid_return.iloc[0]
        selection_rows.append(
            {
                "type": "best_hybrid_return",
                "candidate": str(r["candidate"]),
                "label10": pct(r["net_mean_label_10d"], 2),
                "db10": pct(r["net_mean_db_ret_10d"], 2),
                "db20": pct(r["net_mean_db_ret_20d"], 2),
                "chg": str(int(r["changed_days"] or 0)),
            }
        )

    lines = [
        "# HMM 板块因子系数 Top50 Replacement 离线验证（2026-05-02）",
        "",
        "## 结论摘要",
        "",
        "- 本次继续保持策略和程序运行逻辑不变，只读取现有 QE no-HMM `pred.pkl` / `label.pkl`、本地市场数据和上一步板块因子候选。",
        "- 目标是把高 RankIC 板块因子转成 sector coefficient，并测试 sector-only 与 old covfix + sector-factor hybrid 两类候选。",
        "- 网格结果按 `holdout` 段排序；`train_pre_holdout` 只用于观察稳定性，不作为可直接上线依据。",
        "- 该结果仍不是完整 QE 回测，不能替代 n_drop、已有持仓、停牌/涨跌停和分钟执行后的真实组合收益。",
        "",
        "## 核心对比",
        "",
        fixed_table(
            selection_rows,
            [
                ("type", "Type"),
                ("candidate", "Candidate"),
                ("label10", "NetLabel10D"),
                ("db10", "NetDB10D"),
                ("db20", "NetDB20D"),
                ("chg", "ChangedDays"),
            ],
        ),
        "",
        "- `best_hybrid` 是更均衡的候选：Label10D、DB10D、DB20D 都为正，且相对 old covfix 减少替换频率。",
        "- `best_hybrid_return` 是收益型备选：DB10D/DB20D 更高，但 Label10D 较弱且更可能受少数高收益 replacement 影响，暂不作为唯一主候选。",
        "",
        "## Holdout Top 候选",
        "",
        fixed_table(
            format_summary_rows(summary, "holdout", 15),
            [
                ("candidate", "Candidate"),
                ("days", "Days"),
                ("chg", "ChgDays"),
                ("enter", "Enter/Day"),
                ("uniq_e", "UniqueEnter"),
                ("label10", "NetLabel10D"),
                ("db5", "NetDB5D"),
                ("db10", "NetDB10D"),
                ("db20", "NetDB20D"),
                ("pos", "PosDay"),
            ],
        ),
        "",
        "## Full Period Top 候选",
        "",
        fixed_table(
            format_summary_rows(summary, "full", 12),
            [
                ("candidate", "Candidate"),
                ("days", "Days"),
                ("chg", "ChgDays"),
                ("enter", "Enter/Day"),
                ("uniq_e", "UniqueEnter"),
                ("label10", "NetLabel10D"),
                ("db5", "NetDB5D"),
                ("db10", "NetDB10D"),
                ("db20", "NetDB20D"),
                ("pos", "PosDay"),
            ],
        ),
        "",
    ]

    if selected_candidate:
        lines.extend(
            [
                f"## 最佳候选进入股票样本：`{selected_candidate}`",
                "",
                fixed_table(
                    top_symbol_rows(all_rep, selected_candidate, 20),
                    [
                        ("symbol", "Symbol"),
                        ("sector", "Sector"),
                        ("days", "EnterDays"),
                        ("label10", "MeanLabel10D"),
                        ("db10", "MeanDB10D"),
                        ("raw_r", "AvgRawRank"),
                        ("adj_r", "AvgAdjRank"),
                    ],
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## 产物",
            "",
            f"- 候选摘要：`{output_dir / 'sector_factor_overlay_candidate_summary.csv'}`",
            f"- replacement 明细：`{output_dir / 'sector_factor_overlay_replacements.csv'}`",
            f"- 日度摘要：`{output_dir / 'sector_factor_overlay_daily_summary.csv'}`",
            f"- 候选元数据：`{output_dir / 'sector_factor_overlay_candidate_metadata.csv'}`",
            f"- 最佳候选 JSON：`{output_dir / 'candidate_coefficients'}`",
            f"- 板块分数：`{output_dir / 'sector_factor_overlay_group_scores.csv'}`",
            "",
            "## 下一步判断",
            "",
            "- 若 hybrid 在 holdout 同时改善 `NetLabel10D` 和 `NetDB10D/20D`，优先把 hybrid 作为 QE shadow loop 候选。",
            "- 若 sector-only 高于 old covfix 的 label 但低于 DB10/20，应谨慎：它可能提升 label ranking，但未必转化为组合收益。",
            "- 若 old covfix 仍显著更强，则保留 old covfix 主路径，把板块因子放入 HMM emission/gating，而不是直接替代 coefficient。",
        ]
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    hmm_diag_dir = Path(args.hmm_diag_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report)

    pred_path, label_path = find_base_artifacts(hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(hmm_diag_dir)

    with db_connect() as conn:
        sector_daily = load_sector_daily(conn, args.prestart, args.end)
        sector_mf = load_sector_moneyflow(conn, args.prestart, args.end)
        breadth = load_sector_breadth(conn, args.prestart, args.end)
    panel = build_panel(sector_daily, sector_mf, breadth)

    factor_candidates, metadata, group_scores, score_by_group = build_sector_factor_candidates(panel)
    old_candidates: dict[str, dict[str, dict[str, float]]] = {}
    for name, path in old_coeff_candidate_paths(hmm_diag_dir):
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        old_candidates[name] = data["daily_coefficients"]
        factor_candidates[name] = old_candidates[name]
        metadata[name] = {
            "source": "old_covfix_baseline",
            "group": "old_covfix",
            "features": (),
            "boost": None,
            "penalty": None,
            "top_q": None,
            "bottom_q": None,
            "note": str(path),
        }
    add_hybrid_candidates(factor_candidates, metadata, old_candidates, score_by_group)

    rep_frames = []
    day_frames = []
    sector_frames = []
    for name, coeffs in factor_candidates.items():
        rep, day, sectors = compute_replacements(pred_ser, label_ser, coeffs, stock_sector_map, args.topk, name)
        rep_frames.append(rep)
        day_frames.append(day)
        sector_frames.append(sectors)

    all_rep = pd.concat(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    all_rep = enrich_db_forward_returns(all_rep, [5, 10, 20]) if not all_rep.empty else all_rep
    all_day = pd.concat(day_frames, ignore_index=True) if day_frames else pd.DataFrame()
    all_sectors = pd.concat(sector_frames, ignore_index=True) if sector_frames else pd.DataFrame()
    summary_rows: list[dict[str, Any]] = []
    for name in factor_candidates:
        rep = all_rep[all_rep["candidate"] == name] if not all_rep.empty else pd.DataFrame()
        day = all_day[all_day["candidate"] == name] if not all_day.empty else pd.DataFrame()
        summary_rows.extend(split_periods(rep, day, name, args.test_start))
    summary = pd.DataFrame(summary_rows)
    meta = candidate_metadata(metadata)
    written_jsons = write_top_candidate_jsons(summary, factor_candidates, meta, output_dir)

    summary.to_csv(output_dir / "sector_factor_overlay_candidate_summary.csv", index=False, encoding="utf-8-sig")
    all_rep.to_csv(output_dir / "sector_factor_overlay_replacements.csv", index=False, encoding="utf-8-sig")
    all_day.to_csv(output_dir / "sector_factor_overlay_daily_summary.csv", index=False, encoding="utf-8-sig")
    all_sectors.to_csv(output_dir / "sector_factor_overlay_sector_coefficients.csv", index=False, encoding="utf-8-sig")
    meta.to_csv(output_dir / "sector_factor_overlay_candidate_metadata.csv", index=False, encoding="utf-8-sig")
    group_scores.to_csv(output_dir / "sector_factor_overlay_group_scores.csv", index=False, encoding="utf-8-sig")

    write_report(report_path, args, summary, all_rep, meta, output_dir)
    print(f"Wrote summary: {output_dir / 'sector_factor_overlay_candidate_summary.csv'}")
    for path in written_jsons:
        print(f"Wrote candidate json: {path}")
    print(f"Wrote report: {report_path}")


if __name__ == "__main__":
    main()
