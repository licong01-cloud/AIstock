#!/usr/bin/env python3
"""Script-level HMM sector-factor retraining diagnostics.

This helper is intentionally read-only for AIstock registries: it does not
insert HMM snapshots, does not precompute QE coefficient artifacts, and does
not submit QE tasks.  Its only persistent outputs are diagnostic artifacts
under the requested output directory.

The important contract is that every non-baseline candidate appends the chosen
sector-factor columns to the legacy HMM observation matrix before
``GaussianHMM.fit`` is called.  This is not an overlay/gate backtest.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import psycopg2
from hmmlearn.hmm import GaussianHMM
from scipy.stats import spearmanr


ROOT = Path(__file__).resolve().parents[1]
RDAGENT_ROOT = ROOT.parent / "RD-Agent-main"
for item in (str(RDAGENT_ROOT), str(ROOT)):
    if item not in sys.path:
        sys.path.insert(0, item)

from model_training.common.data_loader import (  # noqa: E402
    get_limit_up_ratio_by_sector,
    load_csi300_daily,
    load_l2_sector_data,
    load_market_total_volume,
    load_sector_stock_mapping,
    read_qlib_calendar,
)
from model_training.hmm.config import HMMTrainConfig  # noqa: E402
from model_training.hmm.train_sector_hmm import (  # noqa: E402
    build_observation_matrix,
    covariance_bound_stats,
    smooth_transition_matrix,
    validate_and_fix_covariance,
)
from scripts.diagnostics.sector_factor_rankic_report import (  # noqa: E402
    build_panel as build_sector_factor_panel,
    load_sector_breadth,
    load_sector_daily,
    load_sector_moneyflow,
)


BASE_FEATURES = [
    "daily_return",
    "excess_return_Nd",
    "volume_ratio",
    "limit_up_ratio",
    "volatility_Nd",
    "net_mf_ratio",
    "elg_net_mf_ratio",
]
STATE_LABELS = ("fading", "neutral", "trending")
STATE_SCORE = {"fading": -1.0, "neutral": 0.0, "trending": 1.0}
HORIZONS = (5, 10, 20)
HORIZON_WEIGHTS = {5: 0.35, 10: 0.35, 20: 0.30}


@dataclass(frozen=True)
class CandidateSpec:
    name: str
    description: str
    sector_features: tuple[str, ...]
    preprocess: str = "winsor_zscore"


CANDIDATES: tuple[CandidateSpec, ...] = (
    CandidateSpec(
        name="baseline_legacy7_raw",
        description="Control: legacy 7 HMM observations, original unscaled old-covfix style.",
        sector_features=(),
        preprocess="identity",
    ),
    CandidateSpec(
        name="baseline_legacy7_winsor_zscore",
        description="Control: legacy 7 HMM observations with train-only 1/99 winsor + z-score.",
        sector_features=(),
    ),
    CandidateSpec(
        name="turnover_core",
        description="Turnover crowding/cooling features added into HMM emissions.",
        sector_features=(
            "sf_turnover_pctile_250d_neg",
            "sf_turnover_pctile_120d_neg",
            "sf_turnover_zscore_60d_neg",
            "sf_turnover_ma5_ma20_neg",
        ),
    ),
    CandidateSpec(
        name="flow_core",
        description="Money-flow stability and small-order net-flow features added into HMM emissions.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
        ),
    ),
    CandidateSpec(
        name="flow_std_only",
        description="Ablation: only the money-flow stability/noise feature enters HMM emissions.",
        sector_features=("sf_mf_net_ratio_std_5d_neg",),
    ),
    CandidateSpec(
        name="small_net_only",
        description="Ablation: only the small-order net-flow feature enters HMM emissions.",
        sector_features=("sf_small_net_ratio_5d",),
    ),
    CandidateSpec(
        name="flow_stability_alt",
        description="Alternative flow-state pair using flow stability ratio plus small-order net flow.",
        sector_features=(
            "sf_flow_stability_5d",
            "sf_small_net_ratio_5d",
        ),
    ),
    CandidateSpec(
        name="flow_tier_core",
        description="Flow-state pair using main-flow stability plus large-vs-small flow tier strength.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_flow_tier_strength_10d",
        ),
    ),
    CandidateSpec(
        name="flow_plus_vol_defensive",
        description="Flow core plus low-range/relative-volatility defensive confirmation.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_intraday_range_5d_neg",
            "sf_vol_vs_market_20d",
        ),
    ),
    CandidateSpec(
        name="flow_plus_breadth",
        description="Flow core plus breadth confirmation and dispersion reversal.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
        ),
    ),
    CandidateSpec(
        name="turnover_flow_core",
        description="Compact crowding plus flow-stability feature set added into HMM emissions.",
        sector_features=(
            "sf_turnover_pctile_250d_neg",
            "sf_turnover_pctile_120d_neg",
            "sf_turnover_ma5_ma20_neg",
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
        ),
    ),
    CandidateSpec(
        name="vol_compress",
        description="Volatility/range compression and relative-volatility features added into HMM emissions.",
        sector_features=(
            "sf_intraday_range_5d_neg",
            "sf_atr14_pctile_250d_neg",
            "sf_range_vs_market_10d",
            "sf_vol_vs_market_20d",
        ),
    ),
    CandidateSpec(
        name="breadth_rev",
        description="Sector breadth/dispersion rotation features added into HMM emissions.",
        sector_features=(
            "sf_breadth_1d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
        ),
    ),
    CandidateSpec(
        name="all_core",
        description="Union of compact turnover, flow, volatility and breadth features added into HMM emissions.",
        sector_features=(
            "sf_turnover_pctile_250d_neg",
            "sf_turnover_pctile_120d_neg",
            "sf_turnover_ma5_ma20_neg",
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_intraday_range_5d_neg",
            "sf_atr14_pctile_250d_neg",
            "sf_range_vs_market_10d",
            "sf_vol_vs_market_20d",
            "sf_breadth_1d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
        ),
    ),
)

SCORE_METHODS = (
    ("trend_fade", "hmm_score", "P(trending) - P(fading) using validation-utility state labels."),
    ("label_ordinal", "label_score", "Decoded state label as -1/0/+1."),
    ("utility_z", "utility_z_score", "Posterior-weighted validation future-utility z-score by hidden state."),
    ("utility_raw", "utility_raw_score", "Posterior-weighted raw validation future utility by hidden state."),
)


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
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def read_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


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


def connect_readonly(host: str, port: int, dbname: str, user: str, password: str):
    errors: list[str] = []
    for candidate in candidate_hosts(host):
        try:
            conn = psycopg2.connect(
                host=candidate,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=5,
            )
            conn.set_session(readonly=True, autocommit=True)
            print(f"DB connected readonly via host={candidate}")
            return conn, candidate
        except Exception as exc:
            errors.append(f"{candidate}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot connect to DB. Tried: " + "; ".join(errors))


def safe_tstat(values: Iterable[float]) -> float:
    arr = pd.Series(list(values), dtype="float64").dropna()
    if len(arr) < 2:
        return float("nan")
    std = arr.std(ddof=1)
    if not std or math.isnan(std):
        return float("nan")
    return float(arr.mean() / (std / math.sqrt(len(arr))))


def rank_scale(values: np.ndarray) -> np.ndarray:
    n = int(values.shape[0])
    if n <= 1:
        return np.zeros(n, dtype=np.float64)
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(n, dtype=np.float64)
    ranks[order] = np.arange(n, dtype=np.float64)
    return ranks / max(n - 1, 1) * 2.0 - 1.0


def future_sum(series: pd.Series, horizon: int) -> pd.Series:
    shifted = [series.shift(-step) for step in range(1, horizon + 1)]
    return pd.concat(shifted, axis=1).sum(axis=1, min_count=horizon)


def add_future_excess(panel: pd.DataFrame, csi300_pct: dict[date, float]) -> pd.DataFrame:
    out = panel.copy()
    bench = {pd.Timestamp(td): float(pct) / 100.0 for td, pct in csi300_pct.items()}
    dates = out.index.get_level_values("trade_date")
    out["benchmark_ret_1d"] = [bench.get(pd.Timestamp(td), np.nan) for td in dates]
    out["daily_excess"] = out["ret_1d"] - out["benchmark_ret_1d"]
    by_sector = out.groupby(level="sector_code", group_keys=False)
    for horizon in HORIZONS:
        out[f"fwd_excess_{horizon}d"] = by_sector["daily_excess"].apply(lambda s, h=horizon: future_sum(s, h))
    return out.replace([np.inf, -np.inf], np.nan)


def make_cfg(args: argparse.Namespace, db_host: str) -> HMMTrainConfig:
    return HMMTrainConfig(
        n_states=args.n_states,
        covariance_type=args.covariance_type,
        n_iter=args.n_iter,
        rolling_window=args.rolling_window,
        zscore=False,
        use_limit_down=False,
        train_start=parse_date(args.train_start),
        train_end=parse_date(args.train_end),
        val_start=parse_date(args.val_start),
        val_end=parse_date(args.val_end),
        sector_level="L2",
        cooldown_days=3,
        min_trading_days=args.min_trading_days,
        qlib_bin_dir=args.qlib_bin_dir,
        db_host=db_host,
        db_port=args.db_port,
        db_user=args.db_user,
        db_password=args.db_password,
        db_name=args.db_name,
    )


def load_legacy_base_observations(
    conn,
    cfg: HMMTrainConfig,
    start: date,
    end: date,
    max_sectors: int | None,
) -> tuple[pd.DataFrame, dict[str, str], dict[str, Any]]:
    print(f"Loading legacy HMM base observations {start} to {end}")
    sector_data = load_l2_sector_data(conn, start, end)
    csi300 = load_csi300_daily(conn, start, end)
    market_vol = load_market_total_volume(conn, start, end)
    sector_stocks = load_sector_stock_mapping(conn, cfg.sector_level)
    calendar = read_qlib_calendar(cfg.qlib_bin_dir)
    limit_up_data, limit_down_data = get_limit_up_ratio_by_sector(
        cfg.qlib_bin_dir,
        sector_stocks,
        calendar,
        start,
        end,
    )

    records: list[dict[str, Any]] = []
    sector_names: dict[str, str] = {}
    selected_items = sorted(sector_data.items())
    if max_sectors:
        selected_items = selected_items[: max(1, max_sectors)]
    skipped = 0
    for sector_code, rows in selected_items:
        if not rows:
            skipped += 1
            continue
        obs, obs_dates = build_observation_matrix(
            rows,
            csi300,
            market_vol,
            limit_up_data.get(sector_code, {}),
            limit_down_data.get(sector_code, {}),
            rolling_window=cfg.rolling_window,
            use_limit_down=cfg.use_limit_down,
        )
        if obs.shape[0] == 0:
            skipped += 1
            continue
        sector_names[str(sector_code)] = str(rows[0].get("l2_name") or sector_code)
        for row_idx, td in enumerate(obs_dates):
            item = {"trade_date": pd.Timestamp(td), "sector_code": str(sector_code)}
            item.update({name: float(obs[row_idx, col]) for col, name in enumerate(BASE_FEATURES)})
            records.append(item)

    if not records:
        raise RuntimeError("No legacy HMM observations were built")
    frame = pd.DataFrame(records).set_index(["trade_date", "sector_code"]).sort_index()
    meta = {
        "sector_count_raw": len(sector_data),
        "sector_count_used": len(sector_names),
        "skipped_sector_count": skipped,
        "base_observation_rows": len(frame),
        "csi300_days": len(csi300),
        "market_vol_days": len(market_vol),
        "qlib_calendar_days": len(calendar),
    }
    print(f"Legacy observation meta: {meta}")
    return frame, sector_names, meta


def load_sector_factor_features(conn, prestart: str, end: str, csi300_pct: dict[date, float]) -> tuple[pd.DataFrame, dict[str, Any]]:
    print(f"Loading sector-factor panel {prestart} to {end}")
    sector_daily = load_sector_daily(conn, prestart, end)
    sector_mf = load_sector_moneyflow(conn, prestart, end)
    breadth = load_sector_breadth(conn, prestart, end)
    panel = build_sector_factor_panel(sector_daily, sector_mf, breadth)
    panel = add_future_excess(panel, csi300_pct)
    meta = {
        "sector_daily_rows": len(sector_daily),
        "sector_moneyflow_rows": len(sector_mf),
        "sector_breadth_rows": len(breadth),
        "panel_rows": len(panel),
        "panel_dates": int(panel.index.get_level_values("trade_date").nunique()) if len(panel) else 0,
        "panel_sectors": int(panel.index.get_level_values("sector_code").nunique()) if len(panel) else 0,
        "panel_start": str(panel.index.get_level_values("trade_date").min().date()) if len(panel) else None,
        "panel_end": str(panel.index.get_level_values("trade_date").max().date()) if len(panel) else None,
    }
    print(f"Sector-factor panel meta: {meta}")
    return panel, meta


def fit_preprocess(obs_by_sector: dict[str, np.ndarray], mode: str, winsor_q: float) -> dict[str, Any]:
    all_obs = np.vstack([obs for obs in obs_by_sector.values() if len(obs)])
    if all_obs.size == 0:
        raise RuntimeError("Cannot fit preprocessing on empty observations")
    params: dict[str, Any] = {
        "mode": mode,
        "fit_scope": "train_window_only",
        "feature_count": int(all_obs.shape[1]),
        "train_observation_count": int(all_obs.shape[0]),
    }
    if mode == "identity":
        return params
    if mode != "winsor_zscore":
        raise ValueError(f"Unsupported preprocess mode: {mode}")
    lower = np.quantile(all_obs, winsor_q, axis=0)
    upper = np.quantile(all_obs, 1.0 - winsor_q, axis=0)
    clipped = np.clip(all_obs, lower, upper)
    mean = clipped.mean(axis=0)
    std = np.where(clipped.std(axis=0) < 1e-10, 1.0, clipped.std(axis=0))
    params.update(
        {
            "winsor_q": float(winsor_q),
            "lower": lower.tolist(),
            "upper": upper.tolist(),
            "mean": mean.tolist(),
            "std": std.tolist(),
        }
    )
    return params


def apply_preprocess(obs: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    if params["mode"] == "identity":
        return obs.astype(np.float64, copy=False)
    lower = np.asarray(params["lower"], dtype=np.float64)
    upper = np.asarray(params["upper"], dtype=np.float64)
    mean = np.asarray(params["mean"], dtype=np.float64)
    std = np.asarray(params["std"], dtype=np.float64)
    return (np.clip(obs, lower, upper) - mean) / std


def forward_filter_posteriors(hmm: GaussianHMM, obs: np.ndarray) -> np.ndarray:
    from hmmlearn import _hmmc

    log_frameprob = hmm._compute_log_likelihood(obs)
    try:
        _, fwd_lattice = _hmmc.forward_log(hmm.startprob_, hmm.transmat_, log_frameprob)
    except TypeError:
        _, fwd_lattice = _hmmc.forward_log(hmm.startprob_, hmm.transmat_, log_frameprob)
    row_max = np.max(fwd_lattice, axis=1, keepdims=True)
    row_max = np.where(np.isfinite(row_max), row_max, 0.0)
    posteriors = np.exp(fwd_lattice - row_max)
    denom = posteriors.sum(axis=1, keepdims=True)
    n_states = posteriors.shape[1]
    return np.divide(
        posteriors,
        denom,
        out=np.full_like(posteriors, 1.0 / max(n_states, 1)),
        where=denom > 0,
    )


def label_states_by_validation(
    states: np.ndarray,
    future_by_horizon: dict[int, np.ndarray],
    n_states: int,
    means: np.ndarray,
) -> tuple[dict[str, str], dict[str, float | None], str]:
    values: dict[int, list[float]] = {idx: [] for idx in range(n_states)}
    for i, state in enumerate(states):
        total = 0.0
        ok = True
        for horizon, weight in HORIZON_WEIGHTS.items():
            arr = future_by_horizon[horizon]
            if i >= len(arr) or not np.isfinite(arr[i]):
                ok = False
                break
            total += weight * float(arr[i])
        if ok:
            values[int(state)].append(total)
    utilities = {idx: (float(np.mean(vals)) if vals else float("nan")) for idx, vals in values.items()}
    usable = {idx: value for idx, value in utilities.items() if np.isfinite(value)}
    if len(usable) >= n_states:
        ordered = sorted(usable, key=lambda idx: usable[idx])
        method = "validation_future_utility"
    else:
        ordered = list(np.argsort(np.asarray(means)[:, 0]))
        method = "fallback_mean_first_feature"
    if n_states == 2:
        labels = {str(int(ordered[0])): "fading", str(int(ordered[-1])): "trending"}
    else:
        labels = {
            str(int(ordered[0])): "fading",
            str(int(ordered[1])): "neutral",
            str(int(ordered[-1])): "trending",
        }
    utility_out = {str(k): (None if not np.isfinite(v) else float(v)) for k, v in utilities.items()}
    return labels, utility_out, method


def build_candidate_frames(
    combined: pd.DataFrame,
    spec: CandidateSpec,
    train_start: date,
    train_end: date,
    min_trading_days: int,
) -> tuple[dict[str, pd.DataFrame], dict[str, np.ndarray], dict[str, Any]]:
    feature_names = BASE_FEATURES + list(spec.sector_features)
    missing_features = [name for name in feature_names if name not in combined.columns]
    if missing_features:
        raise ValueError(f"{spec.name} missing required features: {missing_features}")
    required = feature_names + [f"fwd_excess_{h}d" for h in HORIZONS]
    frames: dict[str, pd.DataFrame] = {}
    train_obs: dict[str, np.ndarray] = {}
    missing_rows_by_feature = {
        feature: int(combined[feature].isna().sum()) if feature in combined.columns else None
        for feature in spec.sector_features
    }
    too_short = 0
    for sector_code, sector_frame in combined[required].sort_index().groupby(level="sector_code"):
        sector_frame = sector_frame.droplevel("sector_code")
        sector_frame = sector_frame.sort_index()
        usable = sector_frame.dropna(subset=feature_names).copy()
        if usable.empty:
            too_short += 1
            continue
        train = usable.loc[
            (usable.index.date >= train_start) & (usable.index.date <= train_end),
            feature_names,
        ]
        if len(train) < min_trading_days:
            too_short += 1
            continue
        frames[str(sector_code)] = usable
        train_obs[str(sector_code)] = train.to_numpy(dtype=np.float64)
    meta = {
        "feature_names": feature_names,
        "feature_count": len(feature_names),
        "sector_feature_count": len(spec.sector_features),
        "sector_features": list(spec.sector_features),
        "preprocess": spec.preprocess,
        "trainable_sector_count": len(train_obs),
        "too_short_or_missing_sector_count": too_short,
        "missing_rows_by_sector_feature": missing_rows_by_feature,
        "train_observation_rows": int(sum(len(obs) for obs in train_obs.values())),
    }
    if not train_obs:
        raise RuntimeError(f"No trainable sectors for {spec.name}")
    return frames, train_obs, meta


def train_candidate(
    spec: CandidateSpec,
    combined: pd.DataFrame,
    sector_names: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, Any]:
    t0 = time.time()
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    val_start = parse_date(args.val_start)
    val_end = parse_date(args.val_end)
    holdout_start = parse_date(args.holdout_start)
    holdout_end = parse_date(args.holdout_end)

    frames, train_obs_raw, meta = build_candidate_frames(
        combined,
        spec,
        train_start,
        train_end,
        args.min_trading_days,
    )
    preprocess = fit_preprocess(train_obs_raw, spec.preprocess, args.winsor_q)
    models: dict[str, Any] = {}
    score_rows: list[dict[str, Any]] = []
    fit_call_count = 0
    covariance_fixed_count = 0
    covariance_anomalies = 0
    label_fallback_count = 0
    failed: list[dict[str, str]] = []

    feature_names = meta["feature_names"]
    for sector_code in sorted(train_obs_raw):
        sector_frame = frames[sector_code]
        train_mask = (sector_frame.index.date >= train_start) & (sector_frame.index.date <= train_end)
        val_mask = (sector_frame.index.date >= val_start) & (sector_frame.index.date <= val_end)
        eval_mask = (sector_frame.index.date >= val_start) & (sector_frame.index.date <= holdout_end)
        train_obs = apply_preprocess(sector_frame.loc[train_mask, feature_names].to_numpy(dtype=np.float64), preprocess)
        val_obs_raw = sector_frame.loc[val_mask, feature_names].to_numpy(dtype=np.float64)
        eval_obs_raw = sector_frame.loc[eval_mask, feature_names].to_numpy(dtype=np.float64)
        if len(train_obs) < args.min_trading_days or len(val_obs_raw) < 30 or len(eval_obs_raw) < 30:
            failed.append({"sector_code": sector_code, "error": "insufficient train/val/eval observations"})
            continue
        try:
            hmm = GaussianHMM(
                n_components=args.n_states,
                covariance_type=args.covariance_type,
                n_iter=args.n_iter,
                min_covar=args.min_covar,
                random_state=args.random_state,
            )
            hmm.fit(train_obs)
            fit_call_count += 1
            fixed, anomaly_count = validate_and_fix_covariance(
                hmm,
                max_covar=args.max_covar,
                min_covar=args.min_covar,
            )
            if fixed:
                covariance_fixed_count += 1
                covariance_anomalies += int(anomaly_count)
            hmm.transmat_ = smooth_transition_matrix(
                hmm.transmat_,
                alpha=args.alpha_smooth,
                min_self_trans=args.min_self_trans,
            )
            cov_stats = covariance_bound_stats(hmm, max_covar=args.max_covar, min_covar=args.min_covar)

            val_obs = apply_preprocess(val_obs_raw, preprocess)
            val_posteriors = forward_filter_posteriors(hmm, val_obs)
            val_states = val_posteriors.argmax(axis=1)
            val_frame = sector_frame.loc[val_mask]
            future_by_horizon = {
                horizon: val_frame[f"fwd_excess_{horizon}d"].to_numpy(dtype=np.float64)
                for horizon in HORIZONS
            }
            labels, utilities, label_method = label_states_by_validation(
                val_states,
                future_by_horizon,
                args.n_states,
                hmm.means_,
            )
            if label_method != "validation_future_utility":
                label_fallback_count += 1
            utility_raw_by_state = {
                int(state): (0.0 if value is None or not np.isfinite(float(value)) else float(value))
                for state, value in utilities.items()
            }
            finite_utilities = np.asarray(
                [
                    value
                    for value in utility_raw_by_state.values()
                    if np.isfinite(value)
                ],
                dtype=np.float64,
            )
            if finite_utilities.size and float(finite_utilities.std()) > 1e-12:
                center = float(finite_utilities.mean())
                scale = float(finite_utilities.std())
                utility_z_by_state = {
                    state: float((value - center) / scale)
                    for state, value in utility_raw_by_state.items()
                }
            else:
                utility_z_by_state = {state: 0.0 for state in range(args.n_states)}

            eval_frame = sector_frame.loc[eval_mask]
            eval_obs = apply_preprocess(eval_obs_raw, preprocess)
            posteriors = forward_filter_posteriors(hmm, eval_obs)
            states = posteriors.argmax(axis=1)
            label_to_state = {label: int(state) for state, label in labels.items()}
            trending_state = label_to_state.get("trending")
            fading_state = label_to_state.get("fading")
            for idx, (td, row) in enumerate(eval_frame.iterrows()):
                state = int(states[idx])
                state_label = labels.get(str(state), "neutral")
                p_trend = float(posteriors[idx, trending_state]) if trending_state is not None else 0.0
                p_fade = float(posteriors[idx, fading_state]) if fading_state is not None else 0.0
                utility_raw_score = float(
                    sum(
                        posteriors[idx, state] * utility_raw_by_state.get(state, 0.0)
                        for state in range(args.n_states)
                    )
                )
                utility_z_score = float(
                    sum(
                        posteriors[idx, state] * utility_z_by_state.get(state, 0.0)
                        for state in range(args.n_states)
                    )
                )
                out = {
                    "candidate": spec.name,
                    "trade_date": pd.Timestamp(td).date().isoformat(),
                    "sector_code": sector_code,
                    "sector_name": sector_names.get(sector_code, sector_code),
                    "state": state,
                    "state_label": state_label,
                    "hmm_score": p_trend - p_fade,
                    "label_score": STATE_SCORE.get(state_label, 0.0),
                    "utility_raw_score": utility_raw_score,
                    "utility_z_score": utility_z_score,
                    "decoded_state_utility_raw": utility_raw_by_state.get(state, 0.0),
                    "decoded_state_utility_z": utility_z_by_state.get(state, 0.0),
                    "p_trending": p_trend,
                    "p_fading": p_fade,
                    "p_neutral": (
                        float(posteriors[idx, label_to_state["neutral"]])
                        if "neutral" in label_to_state
                        else float("nan")
                    ),
                    "split": "validation" if pd.Timestamp(td).date() <= val_end else "holdout",
                }
                for horizon in HORIZONS:
                    out[f"fwd_excess_{horizon}d"] = float(row[f"fwd_excess_{horizon}d"])
                score_rows.append(out)

            models[sector_code] = {
                "sector_code": sector_code,
                "sector_name": sector_names.get(sector_code, sector_code),
                "n_states": args.n_states,
                "covariance_type": args.covariance_type,
                "startprob": hmm.startprob_.tolist(),
                "transmat": hmm.transmat_.tolist(),
                "means": hmm.means_.tolist(),
                "covars": np.asarray(hmm.covars_).tolist(),
                "feature_names": feature_names,
                "sector_features_added_before_fit": list(spec.sector_features),
                "state_labels": labels,
                "state_validation_utilities": utilities,
                "state_validation_utility_z": {str(k): v for k, v in utility_z_by_state.items()},
                "state_label_method": label_method,
                "training_days": int(len(train_obs)),
                "fit_called": True,
                "converged": bool(getattr(hmm.monitor_, "converged", False)),
                "em_iterations": int(getattr(hmm.monitor_, "iter", 0)),
                "em_logprob": float(getattr(hmm.monitor_, "history", [float("nan")])[-1]),
                "covariance_fixed": bool(fixed),
                "covariance_anomaly_count": int(anomaly_count),
                **cov_stats,
            }
        except Exception as exc:
            failed.append({"sector_code": sector_code, "error": repr(exc)})

    if not models or not score_rows:
        raise RuntimeError(f"Candidate {spec.name} produced no trained models or scores")

    score_panel = pd.DataFrame(score_rows)
    method_summaries: list[dict[str, Any]] = []
    method_rankic: list[pd.DataFrame] = []
    method_spread: list[pd.DataFrame] = []
    method_hit: list[pd.DataFrame] = []
    method_state: list[pd.DataFrame] = []
    method_stability: list[pd.DataFrame] = []
    for score_method, score_col, score_description in SCORE_METHODS:
        if score_col not in score_panel.columns:
            continue
        method_panel = score_panel.copy()
        method_panel["hmm_score"] = method_panel[score_col]
        daily_rankic, spread, hit_rate, state_metrics, stability, summary = evaluate_candidate_scores(
            spec,
            method_panel,
            meta,
            holdout_start,
            holdout_end,
        )
        for frame in (daily_rankic, spread, hit_rate, state_metrics, stability):
            if not frame.empty:
                frame["score_method"] = score_method
        summary["score_method"] = score_method
        summary["score_column"] = score_col
        summary["score_description"] = score_description
        method_summaries.append(summary)
        method_rankic.append(daily_rankic)
        method_spread.append(spread)
        method_hit.append(hit_rate)
        method_state.append(state_metrics)
        method_stability.append(stability)
    if not method_summaries:
        raise RuntimeError(f"Candidate {spec.name} produced no score-method diagnostics")
    best_summary = sorted(
        method_summaries,
        key=lambda row: (
            -1e9 if not math.isfinite(float(row.get("holdout_weighted_rank_ic", float("nan")))) else float(row["holdout_weighted_rank_ic"]),
            -1e9 if not math.isfinite(float(row.get("holdout_top_bottom_spread_10d", float("nan")))) else float(row["holdout_top_bottom_spread_10d"]),
            -1e9 if not math.isfinite(float(row.get("holdout_hit_rate_10d", float("nan")))) else float(row["holdout_hit_rate_10d"]),
        ),
        reverse=True,
    )[0]
    daily_rankic = pd.concat(method_rankic, ignore_index=True) if method_rankic else pd.DataFrame()
    spread = pd.concat(method_spread, ignore_index=True) if method_spread else pd.DataFrame()
    hit_rate = pd.concat(method_hit, ignore_index=True) if method_hit else pd.DataFrame()
    state_metrics = pd.concat(method_state, ignore_index=True) if method_state else pd.DataFrame()
    stability = pd.concat(method_stability, ignore_index=True) if method_stability else pd.DataFrame()
    common_summary_fields = {
        "candidate": spec.name,
        "description": spec.description,
        "preprocess": spec.preprocess,
        "feature_count": int(meta["feature_count"]),
        "sector_feature_count": int(meta["sector_feature_count"]),
        "sector_features": "|".join(spec.sector_features),
        "trainable_sector_count": int(meta["trainable_sector_count"]),
        "trained_sector_count": len(models),
        "failed_sector_count": len(failed),
        "fit_call_count": fit_call_count,
        "fit_proof": "sector_features_appended_before_GaussianHMM.fit" if spec.sector_features else "baseline_GaussianHMM.fit",
        "label_fallback_sector_count": label_fallback_count,
        "covariance_fixed_sector_count": covariance_fixed_count,
        "covariance_anomaly_count": covariance_anomalies,
        "runtime_seconds": round(time.time() - t0, 3),
        "n_states": int(args.n_states),
        "covariance_type": str(args.covariance_type),
        "rolling_window": int(args.rolling_window),
        "random_state": int(args.random_state),
    }
    for row in method_summaries:
        row.update(common_summary_fields)
    summary = best_summary
    result = {
        "spec": asdict(spec),
        "meta": meta,
        "preprocess": preprocess,
        "models": models,
        "failed": failed,
        "score_panel": score_panel,
        "daily_rankic": daily_rankic,
        "top_bottom_spread": spread,
        "hit_rate": hit_rate,
        "state_metrics": state_metrics,
        "stability": stability,
        "summary": summary,
        "method_summaries": method_summaries,
    }
    return result


def evaluate_candidate_scores(
    spec: CandidateSpec,
    score_panel: pd.DataFrame,
    meta: dict[str, Any],
    holdout_start: date,
    holdout_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    rankic_rows: list[dict[str, Any]] = []
    spread_rows: list[dict[str, Any]] = []
    hit_rows: list[dict[str, Any]] = []

    score_panel = score_panel.copy()
    score_panel["trade_date"] = pd.to_datetime(score_panel["trade_date"])
    for split in ("validation", "holdout"):
        split_df = score_panel[score_panel["split"] == split]
        for horizon in HORIZONS:
            label_col = f"fwd_excess_{horizon}d"
            daily_ics: list[float] = []
            daily_spreads: list[float] = []
            daily_hits: list[float] = []
            for td, group in split_df[["trade_date", "sector_code", "hmm_score", label_col]].dropna().groupby("trade_date"):
                if len(group) < 20 or group["hmm_score"].nunique() < 3 or group[label_col].nunique() < 3:
                    continue
                corr = spearmanr(group["hmm_score"], group[label_col]).correlation
                if corr is None or not math.isfinite(corr):
                    continue
                daily_ics.append(float(corr))
                rankic_rows.append(
                    {
                        "candidate": spec.name,
                        "split": split,
                        "trade_date": pd.Timestamp(td).date().isoformat(),
                        "horizon": horizon,
                        "rank_ic": float(corr),
                        "n": int(len(group)),
                    }
                )
                ranks = group["hmm_score"].rank(pct=True)
                top = group.loc[ranks >= 0.8, label_col]
                bottom = group.loc[ranks <= 0.2, label_col]
                if not top.empty and not bottom.empty:
                    spread = float(top.mean() - bottom.mean())
                    daily_spreads.append(spread)
                    spread_rows.append(
                        {
                            "candidate": spec.name,
                            "split": split,
                            "trade_date": pd.Timestamp(td).date().isoformat(),
                            "horizon": horizon,
                            "top_mean": float(top.mean()),
                            "bottom_mean": float(bottom.mean()),
                            "spread": spread,
                            "n": int(len(group)),
                        }
                    )
                    median = float(group[label_col].median())
                    top_hit = float((top > median).mean())
                    bottom_hit = float((bottom < median).mean())
                    hit = 0.5 * (top_hit + bottom_hit)
                    daily_hits.append(hit)
                    hit_rows.append(
                        {
                            "candidate": spec.name,
                            "split": split,
                            "trade_date": pd.Timestamp(td).date().isoformat(),
                            "horizon": horizon,
                            "top_hit_rate": top_hit,
                            "bottom_hit_rate": bottom_hit,
                            "combined_hit_rate": hit,
                            "n": int(len(group)),
                        }
                    )

    state_rows: list[dict[str, Any]] = []
    for split in ("validation", "holdout"):
        split_df = score_panel[score_panel["split"] == split]
        for horizon in HORIZONS:
            label_col = f"fwd_excess_{horizon}d"
            means_by_state: dict[str, float] = {}
            for label in STATE_LABELS:
                vals = split_df.loc[split_df["state_label"] == label, label_col].dropna()
                mean_value = float(vals.mean()) if len(vals) else float("nan")
                means_by_state[label] = mean_value
                state_rows.append(
                    {
                        "candidate": spec.name,
                        "split": split,
                        "horizon": horizon,
                        "state_label": label,
                        "mean_fwd_excess": mean_value,
                        "mean_fwd_excess_pct": mean_value * 100.0 if math.isfinite(mean_value) else float("nan"),
                        "n": int(len(vals)),
                    }
                )
            monotonic = (
                math.isfinite(means_by_state["fading"])
                and math.isfinite(means_by_state["neutral"])
                and math.isfinite(means_by_state["trending"])
                and means_by_state["fading"] <= means_by_state["neutral"] <= means_by_state["trending"]
            )
            state_rows.append(
                {
                    "candidate": spec.name,
                    "split": split,
                    "horizon": horizon,
                    "state_label": "__monotonic__",
                    "mean_fwd_excess": float(monotonic),
                    "mean_fwd_excess_pct": float(monotonic),
                    "n": 0,
                }
            )

    stability_rows: list[dict[str, Any]] = []
    work = score_panel.sort_values(["sector_code", "trade_date"]).copy()
    work["prev_label"] = work.groupby("sector_code")["state_label"].shift(1)
    work["prev_score"] = work.groupby("sector_code")["hmm_score"].shift(1)
    work["changed"] = (work["prev_label"].notna()) & (work["state_label"] != work["prev_label"])
    work["abs_score_delta"] = (work["hmm_score"] - work["prev_score"]).abs()
    for split, split_df in work.groupby("split"):
        by_day = split_df.groupby("trade_date").agg(
            decoded_sectors=("sector_code", "nunique"),
            changed_sectors=("changed", "sum"),
            change_rate=("changed", "mean"),
            mean_abs_score_delta=("abs_score_delta", "mean"),
        )
        if by_day.empty:
            continue
        stability_rows.append(
            {
                "candidate": spec.name,
                "split": split,
                "date_count": int(len(by_day)),
                "avg_decoded_sectors": float(by_day["decoded_sectors"].mean()),
                "avg_changed_sectors": float(by_day["changed_sectors"].mean()),
                "avg_change_rate": float(by_day["change_rate"].mean()),
                "avg_abs_score_delta": float(by_day["mean_abs_score_delta"].mean()),
            }
        )

    daily_rankic = pd.DataFrame(rankic_rows)
    spread = pd.DataFrame(spread_rows)
    hit_rate = pd.DataFrame(hit_rows)
    state_metrics = pd.DataFrame(state_rows)
    stability = pd.DataFrame(stability_rows)

    summary: dict[str, Any] = {}
    for split in ("validation", "holdout"):
        for horizon in HORIZONS:
            ic_vals = daily_rankic.loc[
                (daily_rankic.get("split") == split) & (daily_rankic.get("horizon") == horizon),
                "rank_ic",
            ] if not daily_rankic.empty else pd.Series(dtype=float)
            spread_vals = spread.loc[
                (spread.get("split") == split) & (spread.get("horizon") == horizon),
                "spread",
            ] if not spread.empty else pd.Series(dtype=float)
            hit_vals = hit_rate.loc[
                (hit_rate.get("split") == split) & (hit_rate.get("horizon") == horizon),
                "combined_hit_rate",
            ] if not hit_rate.empty else pd.Series(dtype=float)
            mono_row = state_metrics[
                (state_metrics["split"] == split)
                & (state_metrics["horizon"] == horizon)
                & (state_metrics["state_label"] == "__monotonic__")
            ]
            summary[f"{split}_rank_ic_{horizon}d"] = float(ic_vals.mean()) if len(ic_vals) else float("nan")
            summary[f"{split}_rank_ic_{horizon}d_t"] = safe_tstat(ic_vals)
            summary[f"{split}_rank_ic_{horizon}d_pos_ratio"] = float((ic_vals > 0).mean()) if len(ic_vals) else float("nan")
            summary[f"{split}_rank_ic_{horizon}d_days"] = int(len(ic_vals))
            summary[f"{split}_top_bottom_spread_{horizon}d"] = float(spread_vals.mean()) if len(spread_vals) else float("nan")
            summary[f"{split}_top_bottom_spread_{horizon}d_win_ratio"] = float((spread_vals > 0).mean()) if len(spread_vals) else float("nan")
            summary[f"{split}_hit_rate_{horizon}d"] = float(hit_vals.mean()) if len(hit_vals) else float("nan")
            summary[f"{split}_state_monotonic_{horizon}d"] = bool(mono_row["mean_fwd_excess"].iloc[0]) if not mono_row.empty else False

    holdout_weights = []
    for horizon, weight in HORIZON_WEIGHTS.items():
        value = summary.get(f"holdout_rank_ic_{horizon}d")
        if value is not None and math.isfinite(float(value)):
            holdout_weights.append(weight * float(value))
    summary["holdout_weighted_rank_ic"] = float(sum(holdout_weights)) if holdout_weights else float("nan")
    summary["holdout_all_horizon_monotonic"] = all(
        bool(summary.get(f"holdout_state_monotonic_{horizon}d")) for horizon in HORIZONS
    )
    holdout_stability = stability[stability["split"] == "holdout"] if not stability.empty else pd.DataFrame()
    if not holdout_stability.empty:
        row = holdout_stability.iloc[0]
        summary["holdout_avg_changed_sectors"] = float(row["avg_changed_sectors"])
        summary["holdout_avg_change_rate"] = float(row["avg_change_rate"])
        summary["holdout_avg_abs_score_delta"] = float(row["avg_abs_score_delta"])
    summary["holdout_start"] = holdout_start.isoformat()
    summary["holdout_end"] = holdout_end.isoformat()
    summary["candidate_meta_feature_count"] = int(meta["feature_count"])
    return daily_rankic, spread, hit_rate, state_metrics, stability, summary


def write_report(
    output_dir: Path,
    args: argparse.Namespace,
    load_meta: dict[str, Any],
    candidate_summaries: pd.DataFrame,
) -> Path:
    def markdown_table(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "(empty)"
        headers = list(frame.columns)
        rows = []
        for _, item in frame.iterrows():
            row = []
            for col in headers:
                value = item[col]
                if isinstance(value, float):
                    row.append("" if math.isnan(value) else f"{value:.6f}")
                else:
                    row.append(str(value))
            rows.append(row)
        lines_out = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for row in rows:
            lines_out.append("| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |")
        return "\n".join(lines_out)

    path = output_dir / "report.md"
    ranked = candidate_summaries.sort_values(
        ["holdout_weighted_rank_ic", "holdout_top_bottom_spread_10d", "holdout_hit_rate_10d"],
        ascending=[False, False, False],
    )
    lines = [
        "# HMM Sector-Factor Retraining Diagnostic",
        "",
        f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
        f"- Train: {args.train_start} to {args.train_end}",
        f"- Validation/calibration: {args.val_start} to {args.val_end}",
        f"- Holdout: {args.holdout_start} to {args.holdout_end}",
        "- Safety: no DB writes, no HMM registry insert, no QE task submission.",
        "- Proof: non-baseline candidates append sector-factor columns to the observation matrix before GaussianHMM.fit.",
        "",
        "## Data Load",
        "",
    ]
    for key, value in load_meta.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Ranked Candidates", ""])
    cols = [
        "candidate",
        "score_method",
        "feature_count",
        "trained_sector_count",
        "fit_call_count",
        "holdout_weighted_rank_ic",
        "holdout_rank_ic_5d",
        "holdout_rank_ic_10d",
        "holdout_rank_ic_20d",
        "holdout_top_bottom_spread_10d",
        "holdout_hit_rate_10d",
        "holdout_all_horizon_monotonic",
        "holdout_avg_changed_sectors",
    ]
    present = [col for col in cols if col in ranked.columns]
    lines.append(markdown_table(ranked[present]))
    lines.extend(["", "## Candidate Feature Sets", ""])
    for _, row in candidate_summaries.sort_values("candidate").iterrows():
        features = str(row.get("sector_features") or "")
        if not features:
            features = "(none; baseline)"
        lines.append(f"- {row['candidate']}: {features}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train-start", default="2022-01-01")
    parser.add_argument("--train-end", default="2024-06-30")
    parser.add_argument("--val-start", default="2024-07-01")
    parser.add_argument("--val-end", default="2025-03-31")
    parser.add_argument("--holdout-start", default="2025-04-01")
    parser.add_argument("--holdout-end", default="2026-04-27")
    parser.add_argument("--prestart", default=None, help="Sector-factor rolling warmup start; default train_start - 520 days.")
    parser.add_argument("--output-dir", default=".codex_tmp/hmm_sector_factor_retrain_20260504")
    parser.add_argument("--candidates", nargs="*", default=[item.name for item in CANDIDATES])
    parser.add_argument("--max-sectors", type=int, default=None, help="Debug only: limit sector count.")
    parser.add_argument("--n-states", type=int, default=3)
    parser.add_argument("--covariance-type", default="diag")
    parser.add_argument("--n-iter", type=int, default=300)
    parser.add_argument("--rolling-window", type=int, default=3)
    parser.add_argument("--min-trading-days", type=int, default=120)
    parser.add_argument("--winsor-q", type=float, default=0.01)
    parser.add_argument("--min-covar", type=float, default=1e-3)
    parser.add_argument("--max-covar", type=float, default=10.0)
    parser.add_argument("--alpha-smooth", type=float, default=0.1)
    parser.add_argument("--min-self-trans", type=float, default=0.3)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--qlib-bin-dir", default=os.getenv("QLIB_BIN_DIR", "/home/lc999/data/qlib_bin"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    read_env_file(ROOT / ".env")
    args.db_port = int(os.getenv("TDX_DB_PORT", "5432"))
    args.db_user = os.getenv("TDX_DB_USER", "postgres")
    args.db_password = os.getenv("TDX_DB_PASSWORD", "")
    args.db_name = os.getenv("TDX_DB_NAME", "aistock")
    args.db_host = os.getenv("TDX_DB_HOST", "127.0.0.1")
    if not args.db_password:
        raise RuntimeError("TDX_DB_PASSWORD is required in .env or environment")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    models_dir = output_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    train_start = parse_date(args.train_start)
    holdout_end = parse_date(args.holdout_end)
    prestart = parse_date(args.prestart) if args.prestart else train_start - timedelta(days=520)
    data_end = holdout_end + timedelta(days=45)

    conn, db_host = connect_readonly(args.db_host, args.db_port, args.db_name, args.db_user, args.db_password)
    args.db_host = db_host
    cfg = make_cfg(args, db_host)

    base_frame, sector_names, base_meta = load_legacy_base_observations(
        conn,
        cfg,
        prestart,
        data_end,
        args.max_sectors,
    )
    csi300 = load_csi300_daily(conn, prestart, data_end)
    factor_panel, factor_meta = load_sector_factor_features(conn, prestart.isoformat(), data_end.isoformat(), csi300)
    conn.close()

    all_factor_names = sorted({name for item in CANDIDATES for name in item.sector_features})
    joined = base_frame.join(
        factor_panel[all_factor_names + [f"fwd_excess_{h}d" for h in HORIZONS]],
        how="left",
    )
    joined = joined.loc[
        (joined.index.get_level_values("trade_date").date >= prestart)
        & (joined.index.get_level_values("trade_date").date <= data_end)
    ].sort_index()
    write_json(
        output_dir / "run_config.json",
        {
            "args": vars(args),
            "candidate_specs": [asdict(item) for item in CANDIDATES],
            "selected_candidates": args.candidates,
            "base_features": BASE_FEATURES,
            "score_methods": [
                {"name": name, "column": column, "description": description}
                for name, column, description in SCORE_METHODS
            ],
            "horizons": HORIZONS,
            "horizon_weights": HORIZON_WEIGHTS,
            "safety": {
                "db_readonly": True,
                "register_snapshots": False,
                "submit_qe_tasks": False,
            },
        },
    )

    selected = [item for item in CANDIDATES if item.name in set(args.candidates)]
    missing = sorted(set(args.candidates) - {item.name for item in selected})
    if missing:
        raise ValueError(f"Unknown candidates: {missing}")

    summaries: list[dict[str, Any]] = []
    all_rankic: list[pd.DataFrame] = []
    all_spread: list[pd.DataFrame] = []
    all_hit: list[pd.DataFrame] = []
    all_state: list[pd.DataFrame] = []
    all_stability: list[pd.DataFrame] = []
    all_method_summaries: list[dict[str, Any]] = []
    for spec in selected:
        print(f"\n=== Training candidate {spec.name}: +{len(spec.sector_features)} sector features ===")
        result = train_candidate(spec, joined, sector_names, args)
        summaries.append(result["summary"])
        all_method_summaries.extend(result["method_summaries"])
        all_rankic.append(result["daily_rankic"])
        all_spread.append(result["top_bottom_spread"])
        all_hit.append(result["hit_rate"])
        all_state.append(result["state_metrics"])
        all_stability.append(result["stability"])
        model_payload = {
            "spec": result["spec"],
            "meta": result["meta"],
            "preprocess": result["preprocess"],
            "models": result["models"],
            "failed": result["failed"],
            "summary": result["summary"],
            "safety": {
                "registry_write": False,
                "qe_task_submission": False,
            },
        }
        write_json(models_dir / spec.name / "model_diagnostics.json", model_payload)
        # Keep the row-level score panel for attribution while avoiding DB writes.
        result["score_panel"].to_csv(models_dir / spec.name / "score_panel.csv", index=False, encoding="utf-8-sig")
        print(f"Candidate {spec.name} summary: {result['summary']}")

    summary_df = pd.DataFrame(summaries)
    method_summary_df = pd.DataFrame(all_method_summaries)
    rankic_df = pd.concat(all_rankic, ignore_index=True) if all_rankic else pd.DataFrame()
    spread_df = pd.concat(all_spread, ignore_index=True) if all_spread else pd.DataFrame()
    hit_df = pd.concat(all_hit, ignore_index=True) if all_hit else pd.DataFrame()
    state_df = pd.concat(all_state, ignore_index=True) if all_state else pd.DataFrame()
    stability_df = pd.concat(all_stability, ignore_index=True) if all_stability else pd.DataFrame()

    summary_df.sort_values("holdout_weighted_rank_ic", ascending=False).to_csv(
        output_dir / "summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    method_summary_df.sort_values("holdout_weighted_rank_ic", ascending=False).to_csv(
        output_dir / "score_method_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    rankic_df.to_csv(output_dir / "daily_rankic.csv", index=False, encoding="utf-8-sig")
    spread_df.to_csv(output_dir / "top_bottom_spread.csv", index=False, encoding="utf-8-sig")
    hit_df.to_csv(output_dir / "hit_rate.csv", index=False, encoding="utf-8-sig")
    state_df.to_csv(output_dir / "state_metrics.csv", index=False, encoding="utf-8-sig")
    stability_df.to_csv(output_dir / "stability.csv", index=False, encoding="utf-8-sig")
    load_meta = {"base_" + k: v for k, v in base_meta.items()}
    load_meta.update({"factor_" + k: v for k, v in factor_meta.items()})
    report_path = write_report(output_dir, args, load_meta, summary_df)
    print("\nDONE")
    print(f"summary={output_dir / 'summary.csv'}")
    print(f"report={report_path}")


if __name__ == "__main__":
    main()
