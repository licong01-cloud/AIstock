#!/usr/bin/env python3
"""Convert redefined regime-HMM scores into bounded sector coefficients.

The script is read-only for AIstock production state: it does not register HMM
snapshots, does not write DB rows, and does not submit QE tasks. It consumes the
offline score outputs from ``hmm_sector_rotation_redefine_screen_20260509.py``,
builds bounded top/bottom sector coefficient maps, then runs sector-level and
TopK replacement diagnostics before any expensive QE backtest.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.diagnostics.hmm_sector_factor_overlay_diagnostic import (  # noqa: E402
    compute_replacements,
    find_base_artifacts,
    label_to_series,
    load_pickle,
    load_stock_sector_map,
    pred_to_series,
    safe_float,
    split_periods,
)

HORIZONS = (5, 10, 20)
DEFAULT_REDEFINE_DIR = ROOT / ".codex_tmp" / "hmm_sector_rotation_redefine_20260509_oriented_full"
DEFAULT_OUTPUT_DIR = ROOT / ".codex_tmp" / "hmm_regime_bounded_candidate_screen_20260509"
DEFAULT_HMM_DIAG_DIR = (
    ROOT.parent / "hmm-evo-baseline-20260506" / ".codex_tmp" / "hmm_offline_diag" / "qe_20260506_220823_6489"
)
DEFAULT_BASELINES = (
    "LOOP2_COVFIX=F:/Dev/AIstock/backend/data/hmm_models/"
    "b99c907b-873a-4173-a4ee-5eab266f8c49/2026-04-27/"
    "coefficients_preset_A_2024-07-01_2026-04-27.json",
    "LOOP10_PENALTY=F:/Dev/AIstock/backend/data/hmm_models/"
    "ce4952c1-4b0d-46a7-81f2-ae1d4a249555/2026-05-04/"
    "coefficients_preset_A_2024-07-01_2026-04-27.json",
)
DEFAULT_SOURCES = (
    "ROT_REGIME_TOPBOT_LINEAR_v1__INV",
    "ROT_REGIME_LINEAR_v1__INV",
    "ROT_DRAWDOWN_RISK_v1__INV",
)


@dataclass(frozen=True)
class VariantSpec:
    source: str
    variant: str
    top_q: float
    bottom_q: float
    boost: float
    penalty: float

    @property
    def name(self) -> str:
        short = self.source.replace("ROT_", "").replace("_v1__INV", "").replace("_", "")
        top = int(round(self.top_q * 100))
        bottom = int(round(self.bottom_q * 100))
        boost = str(round(self.boost, 4)).replace(".", "p")
        penalty = str(round(self.penalty, 4)).replace(".", "p")
        return f"REGHMM_{short}_{self.variant}_T{top:02d}_B{bottom:02d}_BOOST{boost}_PEN{penalty}"


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default), encoding="utf-8")


def read_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def windows_to_wsl_path(path: str | Path) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def local_path(path: str | Path) -> Path:
    return Path(windows_to_wsl_path(path)).expanduser()


def candidate_hosts(initial: str) -> list[str]:
    hosts: list[str] = []
    for item in (initial, os.getenv("TDX_DB_HOST"), "127.0.0.1", "localhost"):
        if item and item not in hosts:
            hosts.append(str(item))
    try:
        ip = subprocess.check_output(
            "sed -n 's/^nameserver //p' /etc/resolv.conf | head -1",
            shell=True,
            text=True,
            timeout=3,
        ).strip()
        if ip and ip not in hosts:
            hosts.append(ip)
    except Exception:
        pass
    return hosts


def connect_db_readonly(args: argparse.Namespace):
    errors: list[str] = []
    password = args.db_password or os.getenv("TDX_DB_PASSWORD", "")
    for host in candidate_hosts(args.db_host):
        try:
            conn = psycopg2.connect(
                host=host,
                port=args.db_port,
                dbname=args.db_name,
                user=args.db_user,
                password=password,
                connect_timeout=5,
            )
            conn.set_session(readonly=True, autocommit=True)
            return conn, host
        except Exception as exc:
            errors.append(f"{host}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot connect to DB. Tried: " + "; ".join(errors))


def safe_tstat(values: Iterable[float]) -> float | None:
    arr = pd.Series(list(values), dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(arr) < 2:
        return None
    std = float(arr.std(ddof=1))
    if std <= 0 or math.isnan(std):
        return None
    return float(arr.mean() / (std / math.sqrt(len(arr))))


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


def load_panel(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        panel = pd.read_parquet(path)
    else:
        panel = pd.read_csv(path)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"]).dt.date
    return panel


def load_prediction(path: Path, source: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"trade_date", "sector_code", "score"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{path} missing columns: {sorted(missing)}")
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["sector_code"] = df["sector_code"].astype(str)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].dropna(subset=["score"])
    if df.empty:
        raise RuntimeError(f"prediction source {source} has no rows in {start}~{end}")
    return df[["trade_date", "sector_code", "score"]].copy()


def load_coefficients(path: Path) -> dict[str, dict[str, float]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    daily = payload.get("daily_coefficients") if isinstance(payload, dict) else None
    if not isinstance(daily, dict):
        raise RuntimeError(f"missing daily_coefficients: {path}")
    return {
        str(day): {str(sec): float(value) for sec, value in row.items()}
        for day, row in daily.items()
        if isinstance(row, dict)
    }


def load_named_coefficients(items: list[str]) -> dict[str, dict[str, dict[str, float]]]:
    out: dict[str, dict[str, dict[str, float]]] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"invalid named coefficients {item!r}; expected NAME=PATH")
        name, raw_path = item.split("=", 1)
        out[name] = load_coefficients(local_path(raw_path))
    return out


def coefficients_to_frame(name: str, coeffs: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day, row in coeffs.items():
        for sector_code, coeff in row.items():
            rows.append({"candidate": name, "trade_date": parse_date(day), "sector_code": sector_code, "coefficient": float(coeff)})
    return pd.DataFrame(rows)


def build_coefficients_from_score(score_df: pd.DataFrame, spec: VariantSpec) -> dict[str, dict[str, float]]:
    coeffs: dict[str, dict[str, float]] = {}
    for trade_date, group in score_df.groupby("trade_date", sort=True):
        group = group.sort_values(["score", "sector_code"], kind="mergesort").copy()
        n = len(group)
        if n == 0:
            continue
        ranks = group["score"].rank(method="first", pct=True)
        day = pd.Series(1.0, index=group["sector_code"].astype(str), dtype="float64")
        if spec.penalty > 0:
            day.loc[ranks.to_numpy() <= spec.bottom_q] = 1.0 - spec.penalty
        if spec.boost > 0:
            day.loc[ranks.to_numpy() >= 1.0 - spec.top_q] = 1.0 + spec.boost
        coeffs[pd.Timestamp(trade_date).strftime("%Y-%m-%d")] = {str(k): float(v) for k, v in day.items()}
    return coeffs


def candidate_specs_for_source(source: str, top_qs: list[float], bottom_qs: list[float]) -> list[VariantSpec]:
    specs: list[VariantSpec] = []
    for top_q in top_qs:
        for bottom_q in bottom_qs:
            for penalty in (0.005, 0.01, 0.015, 0.02, 0.03):
                specs.append(VariantSpec(source, "RISK", top_q, bottom_q, 0.0, penalty))
                for boost in (0.005, 0.01, 0.015):
                    specs.append(VariantSpec(source, "BOTH", top_q, bottom_q, boost, penalty))
            for boost in (0.005, 0.01, 0.015):
                specs.append(VariantSpec(source, "BOOST", top_q, bottom_q, boost, 0.0))
    return specs


def evaluate_sector_metrics(
    name: str,
    coeffs: dict[str, dict[str, float]],
    panel: pd.DataFrame,
    start: date,
    end: date,
    top_quantile: float,
) -> dict[str, Any]:
    coeff_df = coefficients_to_frame(name, coeffs)
    cols = ["trade_date", "sector_code"] + [f"future_excess_{h}d" for h in HORIZONS] + [f"future_rank_{h}d" for h in HORIZONS]
    merged = coeff_df.merge(panel[cols], on=["trade_date", "sector_code"], how="inner")
    merged = merged[(merged["trade_date"] >= start) & (merged["trade_date"] <= end)].dropna(subset=["coefficient"])
    out: dict[str, Any] = {
        "candidate": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "rows": int(len(merged)),
        "date_count": int(merged["trade_date"].nunique()) if not merged.empty else 0,
        "sector_count": int(merged["sector_code"].nunique()) if not merged.empty else 0,
        "non_neutral_sector_avg": None,
        "coefficient_change_fraction": None,
        "horizons": {},
    }
    if merged.empty:
        return out

    day_non_neutral = merged.assign(non_neutral=(merged["coefficient"] - 1.0).abs() > 1e-12).groupby("trade_date")["non_neutral"].sum()
    out["non_neutral_sector_avg"] = float(day_non_neutral.mean())
    pivot = merged.pivot(index="trade_date", columns="sector_code", values="coefficient").sort_index()
    changed = pivot.ne(pivot.shift(1)).sum(axis=1).iloc[1:]
    out["coefficient_change_fraction"] = float((changed / pivot.shape[1]).mean()) if len(changed) else None

    for horizon in HORIZONS:
        future_col = f"future_excess_{horizon}d"
        rank_col = f"future_rank_{horizon}d"
        rankics: list[float] = []
        spreads: list[float] = []
        rank_spreads: list[float] = []
        hit_rates: list[float] = []
        for _, group in merged.dropna(subset=[future_col, rank_col]).groupby("trade_date", sort=True):
            if len(group) < 10 or group["coefficient"].nunique() <= 1:
                continue
            corr = group["coefficient"].rank(method="average").corr(group[future_col].rank(method="average"))
            if pd.notna(corr):
                rankics.append(float(corr))
            n_tail = max(1, int(math.floor(len(group) * top_quantile)))
            ordered = group.sort_values(["coefficient", "sector_code"], kind="mergesort")
            bottom = ordered.head(n_tail)
            top = ordered.tail(n_tail)
            spreads.append(float(top[future_col].mean() - bottom[future_col].mean()))
            rank_spreads.append(float(top[rank_col].mean() - bottom[rank_col].mean()))
            hit_rates.append(float((top[future_col] > group[future_col].median()).mean()))
        out["horizons"][str(horizon)] = {
            "rankic_mean": float(np.mean(rankics)) if rankics else None,
            "rankic_tstat": safe_tstat(rankics),
            "spread_mean": float(np.mean(spreads)) if spreads else None,
            "spread_pct": float(np.mean(spreads) * 100.0) if spreads else None,
            "spread_tstat": safe_tstat(spreads),
            "rank_spread_mean": float(np.mean(rank_spreads)) if rank_spreads else None,
            "top_hit_rate": float(np.mean(hit_rates)) if hit_rates else None,
        }
    return out


def metric(payload: dict[str, Any], horizon: int, key: str) -> float:
    value = payload.get("horizons", {}).get(str(horizon), {}).get(key)
    if value is None:
        return float("nan")
    try:
        value = float(value)
        return value if math.isfinite(value) else float("nan")
    except Exception:
        return float("nan")


def flatten_sector_metrics(metrics: dict[str, dict[str, Any]], split: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, payload in metrics.items():
        base = {
            "candidate": name,
            "split": split,
            "date_count": payload.get("date_count"),
            "sector_count": payload.get("sector_count"),
            "non_neutral_sector_avg": payload.get("non_neutral_sector_avg"),
            "coefficient_change_fraction": payload.get("coefficient_change_fraction"),
        }
        for horizon, hrow in payload.get("horizons", {}).items():
            row = dict(base)
            row["horizon"] = int(horizon)
            row.update(hrow)
            rows.append(row)
    return rows


def coefficient_delta_summary(name: str, coeffs: dict[str, dict[str, float]], base: dict[str, dict[str, float]]) -> dict[str, Any]:
    deltas: list[float] = []
    values: list[float] = []
    gt = lt = total = 0
    for day, row in coeffs.items():
        base_row = base.get(day, {})
        for sec, value in row.items():
            base_value = float(base_row.get(sec, 1.0))
            value = float(value)
            deltas.append(abs(value - base_value))
            values.append(value)
            total += 1
            if value > base_value + 1e-12:
                gt += 1
            elif value < base_value - 1e-12:
                lt += 1
    if not values:
        return {"candidate": name}
    arr = np.asarray(values, dtype=np.float64)
    return {
        "candidate": name,
        "mean_abs_delta_vs_loop2": float(np.mean(deltas)) if deltas else 0.0,
        "candidate_gt_loop2_share": gt / total if total else None,
        "candidate_lt_loop2_share": lt / total if total else None,
        "penalty_share": float(np.mean(arr < 1.0 - 1e-12)),
        "boost_share": float(np.mean(arr > 1.0 + 1e-12)),
        "candidate_min": float(np.min(arr)),
        "candidate_max": float(np.max(arr)),
        "candidate_mean": float(np.mean(arr)),
    }


def score_for_shortlist(row: dict[str, Any], baseline_best_spread: float, baseline_best_rankic: float) -> float:
    val_10_spread = row.get("val_spread_10d")
    test_10_spread = row.get("test_spread_10d")
    val_10_rankic = row.get("val_rankic_10d")
    test_10_rankic = row.get("test_rankic_10d")
    non_neutral = row.get("non_neutral_sector_avg") or 0.0
    churn = row.get("coefficient_change_fraction") or 0.0
    score = 0.0
    for value, weight in (
        (test_10_spread, 16.0),
        (val_10_spread, 12.0),
        (test_10_rankic, 0.25),
        (val_10_rankic, 0.20),
    ):
        if value is not None and math.isfinite(float(value)):
            score += float(value) * weight
    if test_10_spread is not None and test_10_spread > baseline_best_spread:
        score += 0.04
    if test_10_rankic is not None and test_10_rankic > baseline_best_rankic:
        score += 0.04
    if non_neutral > 55:
        score -= 0.03
    if churn > 0.35:
        score -= 0.03
    return score


def summarize_topk_periods(rep: pd.DataFrame, day: pd.DataFrame, candidate: str, split_date: str) -> list[dict[str, Any]]:
    rows = split_periods(rep, day, candidate, split_date)
    for row in rows:
        avg_enter = row.get("avg_entered_per_day")
        row["turnover_proxy_topk"] = float(avg_enter) / 50.0 if avg_enter is not None else None
    return rows


def enrich_db_forward_returns_env(replacements: pd.DataFrame, horizons: list[int], args: argparse.Namespace) -> pd.DataFrame:
    if replacements.empty:
        return replacements
    symbols = sorted(replacements["symbol"].dropna().unique().tolist())
    min_date = pd.to_datetime(replacements["date"]).min().date()
    max_date = (pd.to_datetime(replacements["date"]).max() + pd.Timedelta(days=max(horizons) * 3 + 15)).date()
    conn, host = connect_db_readonly(args)
    print(f"DB connected readonly for forward returns via host={host}")
    try:
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
    finally:
        conn.close()
    if price.empty:
        return replacements
    price["trade_date"] = pd.to_datetime(price["trade_date"]).dt.strftime("%Y-%m-%d")
    price["close"] = price["close_li"].astype(float)
    price = price.sort_values(["symbol", "trade_date"])
    for horizon in horizons:
        price[f"db_ret_{horizon}d"] = price.groupby("symbol")["close"].shift(-horizon) / price["close"] - 1.0
    ret_cols = ["trade_date", "symbol"] + [f"db_ret_{horizon}d" for horizon in horizons]
    return replacements.merge(
        price[ret_cols],
        left_on=["date", "symbol"],
        right_on=["trade_date", "symbol"],
        how="left",
    ).drop(columns=["trade_date"], errors="ignore")


def build_report(
    output_dir: Path,
    sector_summary: pd.DataFrame,
    topk_summary: pd.DataFrame,
    delta_summary: pd.DataFrame,
    selected: list[str],
    selected_reasons: dict[str, list[str]],
) -> None:
    def table(df: pd.DataFrame, cols: list[str], limit: int = 20) -> str:
        if df.empty:
            return "(no rows)"
        view = df.head(limit).copy()
        existing = [col for col in cols if col in view.columns]
        return "```text\n" + view[existing].to_string(index=False) + "\n```"

    holdout = topk_summary[topk_summary["period"] == "holdout"].copy()
    if not holdout.empty and "net_mean_db_ret_10d" in holdout.columns:
        holdout = holdout.sort_values(["net_mean_db_ret_10d", "avg_entered_per_day"], ascending=[False, False])
    elif not holdout.empty and "net_mean_label_10d" in holdout.columns:
        holdout = holdout.sort_values(["net_mean_label_10d", "avg_entered_per_day"], ascending=[False, False])
    test10 = sector_summary[(sector_summary["split"] == "test") & (sector_summary["horizon"] == 10)].copy()
    if not test10.empty:
        test10 = test10.sort_values("screen_score", ascending=False)

    selected_text = "none"
    if selected:
        selected_text = ", ".join(
            f"`{name}` ({'/'.join(selected_reasons.get(name, [])) or 'gate'})" for name in selected
        )

    lines = [
        "# Regime-HMM Bounded Coefficient Screen",
        "",
        f"- Generated at: {datetime.now(timezone.utc).astimezone().isoformat()}",
        "- Registry/QE impact: none; coefficient files are temporary offline artifacts only.",
        "- Selected for possible next hidden registration: " + selected_text,
        "",
        "## Sector-Level Ranking",
        "",
        table(
            test10,
            [
                "candidate",
                "screen_score",
                "rankic_mean",
                "spread_mean",
                "spread_pct",
                "top_hit_rate",
                "non_neutral_sector_avg",
                "coefficient_change_fraction",
            ],
        ),
        "",
        "## TopK Holdout Attribution",
        "",
        table(
            holdout,
            [
                "candidate",
                "changed_days",
                "avg_entered_per_day",
                "turnover_proxy_topk",
                "net_mean_label_10d",
                "net_mean_db_ret_5d",
                "net_mean_db_ret_10d",
                "net_mean_db_ret_20d",
                "positive_net_label_day_ratio",
            ],
        ),
        "",
        "## Coefficient Delta vs Loop2",
        "",
        table(
            delta_summary.sort_values("mean_abs_delta_vs_loop2"),
            [
                "candidate",
                "mean_abs_delta_vs_loop2",
                "candidate_gt_loop2_share",
                "candidate_lt_loop2_share",
                "penalty_share",
                "boost_share",
                "candidate_min",
                "candidate_max",
            ],
        ),
    ]
    (output_dir / "analysis_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--redefine-dir", default=str(DEFAULT_REDEFINE_DIR))
    parser.add_argument("--hmm-diag-dir", default=str(DEFAULT_HMM_DIAG_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--baseline-coefficients", action="append", default=list(DEFAULT_BASELINES))
    parser.add_argument("--start", default="2024-07-01")
    parser.add_argument("--end", default="2026-04-27")
    parser.add_argument("--val-start", default="2025-06-02")
    parser.add_argument("--val-end", default="2025-08-29")
    parser.add_argument("--test-start", default="2025-09-01")
    parser.add_argument("--top-quantile", type=float, default=0.20)
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--shortlist", type=int, default=24)
    parser.add_argument("--skip-db-forward", action="store_true")
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser.parse_args()


def main() -> None:
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument("--env-file", default=None)
    env_args, _ = env_parser.parse_known_args()
    read_env_file(ROOT / ".env")
    if env_args.env_file:
        read_env_file(local_path(env_args.env_file))
    args = parse_args()

    redefine_dir = local_path(args.redefine_dir)
    hmm_diag_dir = local_path(args.hmm_diag_dir)
    output_dir = local_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    coeff_dir = output_dir / "candidate_coefficients"
    coeff_dir.mkdir(parents=True, exist_ok=True)

    start = parse_date(args.start)
    end = parse_date(args.end)
    val_start = parse_date(args.val_start)
    val_end = parse_date(args.val_end)
    test_start = parse_date(args.test_start)
    sources = tuple(args.source or DEFAULT_SOURCES)

    panel_path = redefine_dir / "sector_rotation_panel.parquet"
    if not panel_path.is_file():
        panel_path = redefine_dir / "sector_rotation_panel.csv"
    panel = load_panel(panel_path)
    panel = panel[(panel["trade_date"] >= start) & (panel["trade_date"] <= end)].copy()
    print(
        "Loaded sector panel: "
        f"rows={len(panel)}, dates={panel['trade_date'].nunique()}, sectors={panel['sector_code'].nunique()}"
    )

    baseline_coeffs = load_named_coefficients(args.baseline_coefficients)
    if "LOOP2_COVFIX" not in baseline_coeffs:
        raise RuntimeError("LOOP2_COVFIX baseline is required for delta comparison")

    all_coeffs: dict[str, dict[str, dict[str, float]]] = dict(baseline_coeffs)
    metadata_rows: list[dict[str, Any]] = []
    top_qs = [0.15, 0.20, 0.25]
    bottom_qs = [0.15, 0.20, 0.25]
    for source in sources:
        pred_path = redefine_dir / f"predictions_{source}.csv"
        pred = load_prediction(pred_path, source, start, end)
        print(f"Building bounded variants for {source}: prediction_rows={len(pred)}")
        for spec in candidate_specs_for_source(source, top_qs, bottom_qs):
            coeffs = build_coefficients_from_score(pred, spec)
            all_coeffs[spec.name] = coeffs
            metadata_rows.append(
                {
                    "candidate": spec.name,
                    "source": source,
                    "variant": spec.variant,
                    "top_q": spec.top_q,
                    "bottom_q": spec.bottom_q,
                    "boost": spec.boost,
                    "penalty": spec.penalty,
                }
            )
    print(f"Built candidate coefficient maps: total={len(all_coeffs)}, baselines={len(baseline_coeffs)}")

    val_metrics: dict[str, dict[str, Any]] = {}
    test_metrics: dict[str, dict[str, Any]] = {}
    for idx, (name, coeffs) in enumerate(all_coeffs.items(), start=1):
        if idx == 1 or idx % 50 == 0 or idx == len(all_coeffs):
            print(f"Sector metric evaluation progress: {idx}/{len(all_coeffs)}")
        val_metrics[name] = evaluate_sector_metrics(name, coeffs, panel, val_start, val_end, args.top_quantile)
        test_metrics[name] = evaluate_sector_metrics(name, coeffs, panel, test_start, end, args.top_quantile)

    baseline_best_spread = max(metric(test_metrics[name], 10, "spread_mean") for name in baseline_coeffs)
    baseline_best_rankic = max(metric(test_metrics[name], 10, "rankic_mean") for name in baseline_coeffs)
    sector_rows: list[dict[str, Any]] = []
    for split, metrics in (("validation", val_metrics), ("test", test_metrics)):
        sector_rows.extend(flatten_sector_metrics(metrics, split))
    sector_summary = pd.DataFrame(sector_rows)
    test10_lookup = sector_summary[(sector_summary["split"] == "test") & (sector_summary["horizon"] == 10)].set_index("candidate")
    val10_lookup = sector_summary[(sector_summary["split"] == "validation") & (sector_summary["horizon"] == 10)].set_index("candidate")
    scores: list[dict[str, Any]] = []
    for name in all_coeffs:
        if name in baseline_coeffs:
            continue
        test_row = test10_lookup.loc[name].to_dict() if name in test10_lookup.index else {}
        val_row = val10_lookup.loc[name].to_dict() if name in val10_lookup.index else {}
        row = {
            "candidate": name,
            "test_rankic_10d": test_row.get("rankic_mean"),
            "test_spread_10d": test_row.get("spread_mean"),
            "val_rankic_10d": val_row.get("rankic_mean"),
            "val_spread_10d": val_row.get("spread_mean"),
            "non_neutral_sector_avg": test_row.get("non_neutral_sector_avg"),
            "coefficient_change_fraction": test_row.get("coefficient_change_fraction"),
        }
        row["screen_score"] = score_for_shortlist(row, baseline_best_spread, baseline_best_rankic)
        scores.append(row)
    screen_scores = pd.DataFrame(scores).sort_values("screen_score", ascending=False)
    sector_summary = sector_summary.merge(screen_scores[["candidate", "screen_score"]], on="candidate", how="left")

    shortlist = screen_scores.head(args.shortlist)["candidate"].tolist()
    print(f"Shortlisted for TopK attribution: {len(shortlist)} candidates + {len(baseline_coeffs)} baselines")
    topk_candidates = {name: all_coeffs[name] for name in list(baseline_coeffs) + shortlist}

    pred_path, label_path = find_base_artifacts(hmm_diag_dir)
    pred_ser = pred_to_series(load_pickle(pred_path))
    label_ser = label_to_series(load_pickle(label_path))
    stock_sector_map = load_stock_sector_map(hmm_diag_dir)

    rep_frames: list[pd.DataFrame] = []
    day_frames: list[pd.DataFrame] = []
    for name, coeffs in topk_candidates.items():
        print(f"TopK attribution: {name}")
        rep, day, _sector = compute_replacements(pred_ser, label_ser, coeffs, stock_sector_map, args.topk, name)
        rep_frames.append(rep)
        day_frames.append(day)

    replacements = pd.concat(rep_frames, ignore_index=True) if rep_frames else pd.DataFrame()
    day_summary = pd.concat(day_frames, ignore_index=True) if day_frames else pd.DataFrame()
    if not args.skip_db_forward and not replacements.empty:
        replacements = enrich_db_forward_returns_env(replacements, list(HORIZONS), args)

    topk_rows: list[dict[str, Any]] = []
    for name in topk_candidates:
        rep = replacements[replacements["candidate"] == name].copy() if not replacements.empty else pd.DataFrame()
        day = day_summary[day_summary["candidate"] == name].copy() if not day_summary.empty else pd.DataFrame()
        topk_rows.extend(summarize_topk_periods(rep, day, name, args.test_start))
    topk_summary = pd.DataFrame(topk_rows)

    delta_summary = pd.DataFrame(
        coefficient_delta_summary(name, coeffs, baseline_coeffs["LOOP2_COVFIX"])
        for name, coeffs in topk_candidates.items()
    )

    holdout = topk_summary[topk_summary["period"] == "holdout"].copy()
    if not topk_summary.empty and "net_mean_db_ret_10d" in topk_summary.columns:
        pivot = topk_summary.pivot(index="candidate", columns="period", values="net_mean_db_ret_10d")
    else:
        pivot = pd.DataFrame()
    baseline_full_values = [
        value
        for value in (
            safe_float(pivot.loc[name, "full"])
            for name in baseline_coeffs
            if name in pivot.index and "full" in pivot.columns
        )
        if value is not None
    ]
    baseline_holdout_values = [
        value
        for value in (
            safe_float(pivot.loc[name, "holdout"])
            for name in baseline_coeffs
            if name in pivot.index and "holdout" in pivot.columns
        )
        if value is not None
    ]
    baseline_full = max(baseline_full_values) if baseline_full_values else None
    baseline_holdout = max(baseline_holdout_values) if baseline_holdout_values else None
    selected: list[str] = []
    selected_reasons: dict[str, list[str]] = {}
    rank_metric = None
    if "net_mean_db_ret_10d" in holdout.columns:
        rank_metric = "net_mean_db_ret_10d"
    elif "net_mean_label_10d" in holdout.columns:
        rank_metric = "net_mean_label_10d"
    if not holdout.empty and rank_metric is not None:
        ranked = holdout.sort_values(rank_metric, ascending=False)
        for _, row in ranked.iterrows():
            if row["candidate"] in baseline_coeffs:
                continue
            if safe_float(row.get("net_mean_db_ret_10d")) is None:
                continue
            name = str(row["candidate"])
            full = safe_float(pivot.loc[name, "full"]) if name in pivot.index and "full" in pivot.columns else None
            train_pre = (
                safe_float(pivot.loc[name, "train_pre_holdout"])
                if name in pivot.index and "train_pre_holdout" in pivot.columns
                else None
            )
            holdout_10d = safe_float(row.get("net_mean_db_ret_10d"))
            holdout_label = safe_float(row.get("net_mean_label_10d"))
            avg_entered = float(row.get("avg_entered_per_day") or 0.0)
            changed_days = float(row.get("changed_days") or 0.0)
            reasons: list[str] = []
            if (
                baseline_holdout is not None
                and holdout_10d is not None
                and holdout_10d > baseline_holdout
                and full is not None
                and full >= 0.0
                and train_pre is not None
                and train_pre >= -0.003
                and avg_entered >= 0.05
                and changed_days >= 10
            ):
                reasons.append("recent_holdout")
            if (
                holdout_10d is None
                and holdout_label is not None
                and train_pre is not None
                and train_pre >= 0.0
                and full is not None
                and full >= 0.0
                and avg_entered >= 0.05
                and changed_days >= 10
            ):
                reasons.append("label_only_smoke")
            if (
                baseline_full is not None
                and full is not None
                and full > baseline_full
                and train_pre is not None
                and train_pre >= 0.0
                and holdout_10d is not None
                and holdout_10d > 0.0
                and avg_entered >= 0.05
                and changed_days >= 10
            ):
                reasons.append("robust_full")
            if reasons and name not in selected:
                selected.append(name)
                selected_reasons[name] = reasons
            if len(selected) >= 6:
                break

    for name in selected:
        payload = {
            "generated_by": "scripts/hmm_regime_bounded_candidate_screen_20260509.py",
            "registered_for_qe": False,
            "candidate": name,
            "source_metadata": next((row for row in metadata_rows if row["candidate"] == name), {}),
            "selection_status": "offline_screen_candidate__not_registered",
            "selection_reasons": selected_reasons.get(name, []),
            "daily_coefficients": all_coeffs[name],
            "stock_sector_map": stock_sector_map,
        }
        write_json(coeff_dir / f"{name}.json", payload)

    metadata = pd.DataFrame(metadata_rows)
    sector_summary.to_csv(output_dir / "sector_metric_summary.csv", index=False)
    screen_scores.to_csv(output_dir / "screen_scores.csv", index=False)
    topk_summary.to_csv(output_dir / "topk_summary.csv", index=False)
    delta_summary.to_csv(output_dir / "coefficient_delta_summary.csv", index=False)
    metadata.to_csv(output_dir / "candidate_metadata.csv", index=False)
    if not replacements.empty:
        replacements.to_csv(output_dir / "topk_replacements.csv", index=False)
    if not day_summary.empty:
        day_summary.to_csv(output_dir / "topk_day_summary.csv", index=False)
    write_json(
        output_dir / "run_context.json",
        {
            "redefine_dir": str(redefine_dir),
            "hmm_diag_dir": str(hmm_diag_dir),
            "sources": sources,
            "baseline_coefficients": args.baseline_coefficients,
            "shortlist": shortlist,
            "selected": selected,
            "selected_reasons": selected_reasons,
            "topk": args.topk,
            "skip_db_forward": args.skip_db_forward,
            "start": start,
            "end": end,
            "val_start": val_start,
            "val_end": val_end,
            "test_start": test_start,
        },
    )
    build_report(output_dir, sector_summary, topk_summary, delta_summary, selected, selected_reasons)
    print(
        json.dumps(
            {"output_dir": str(output_dir), "selected": selected, "selected_reasons": selected_reasons, "shortlist_top5": shortlist[:5]},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
