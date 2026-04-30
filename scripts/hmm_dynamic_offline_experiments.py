#!/usr/bin/env python3
"""Offline dynamic HMM experiments with qlib-data validation.

Scope guardrails:
- no writes to model_train_* database tables;
- no changes to existing HMM model versions;
- only standalone experiment models, coefficients, and validation reports are written.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import psycopg2.extras
from hmmlearn.hmm import GaussianHMM

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_horizon_v2_train as base  # noqa: E402


HORIZON_PRESETS: dict[str, dict[int, float]] = {
    "blend_5_10_20": {5: 0.35, 10: 0.35, 20: 0.30},
    "blend_10_20": {10: 0.45, 20: 0.55},
}


@dataclass(frozen=True)
class VariantSpec:
    name: str
    direction: str
    n_states: int
    method: str  # er, pup, additive_pup
    horizon_preset: str
    coefficient_lambda: float
    coeff_min: float
    coeff_max: float
    confidence_scale: float
    additive_beta: float = 0.0
    random_state: int = 42
    neutral_band: float = 0.0
    confidence_floor: float = 0.0

    @property
    def horizon_weights(self) -> dict[int, float]:
        return HORIZON_PRESETS[self.horizon_preset]

    @property
    def qe_ready(self) -> bool:
        return self.method in {"er", "er_winsor", "er_median", "pup", "pup_z", "pup_rank"}


DEFAULT_VARIANTS: list[VariantSpec] = [
    VariantSpec(
        name="dyncoef_er_blend_k3_clip_0p98_1p02",
        direction="dynamic expected-return coefficient, 5D/10D/20D blend",
        n_states=3,
        method="er",
        horizon_preset="blend_5_10_20",
        coefficient_lambda=0.012,
        coeff_min=0.98,
        coeff_max=1.02,
        confidence_scale=0.30,
        random_state=43,
    ),
    VariantSpec(
        name="dyncoef_pup_blend_k3_clip_0p98_1p02",
        direction="dynamic probability-up coefficient, 5D/10D/20D blend",
        n_states=3,
        method="pup",
        horizon_preset="blend_5_10_20",
        coefficient_lambda=0.060,
        coeff_min=0.98,
        coeff_max=1.02,
        confidence_scale=0.30,
        random_state=44,
    ),
    VariantSpec(
        name="dyncoef_er_10_20_k3_clip_0p98_1p02",
        direction="horizon-aware expected-return coefficient, 10D/20D blend",
        n_states=3,
        method="er",
        horizon_preset="blend_10_20",
        coefficient_lambda=0.012,
        coeff_min=0.98,
        coeff_max=1.02,
        confidence_scale=0.30,
        random_state=45,
    ),
    VariantSpec(
        name="dyncoef_pup_10_20_k3_clip_0p98_1p02",
        direction="horizon-aware probability-up coefficient, 10D/20D blend",
        n_states=3,
        method="pup",
        horizon_preset="blend_10_20",
        coefficient_lambda=0.060,
        coeff_min=0.98,
        coeff_max=1.02,
        confidence_scale=0.30,
        random_state=46,
    ),
    VariantSpec(
        name="dyncoef_pup_blend_k4_clip_0p98_1p02",
        direction="four-state HMM with dynamic probability-up coefficient",
        n_states=4,
        method="pup",
        horizon_preset="blend_5_10_20",
        coefficient_lambda=0.060,
        coeff_min=0.98,
        coeff_max=1.02,
        confidence_scale=0.25,
        random_state=47,
    ),
    VariantSpec(
        name="additive_rank_pup_blend_k3_beta_0p03",
        direction="additive rank overlay using probability-up signal, script-diagnostic only",
        n_states=3,
        method="additive_pup",
        horizon_preset="blend_5_10_20",
        coefficient_lambda=0.0,
        coeff_min=1.0,
        coeff_max=1.0,
        confidence_scale=0.30,
        additive_beta=0.03,
        random_state=48,
    ),
]


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def forward_posteriors(hmm: GaussianHMM, obs: np.ndarray) -> np.ndarray:
    from hmmlearn import _hmmc

    log_frameprob = hmm._compute_log_likelihood(obs)
    # hmmlearn 0.3.x forward_log expects probability start/trans matrices
    # and log frame likelihoods. Passing log start/trans produces NaNs.
    _, fwd_lattice = _hmmc.forward_log(hmm.startprob_, hmm.transmat_, log_frameprob)
    row_max = np.max(fwd_lattice, axis=1, keepdims=True)
    row_max = np.where(np.isfinite(row_max), row_max, 0.0)
    posteriors = np.exp(fwd_lattice - row_max)
    denom = posteriors.sum(axis=1, keepdims=True)
    n_states = posteriors.shape[1]
    posteriors = np.divide(
        posteriors,
        denom,
        out=np.full_like(posteriors, 1.0 / max(n_states, 1)),
        where=denom > 0,
    )
    return np.nan_to_num(posteriors, nan=1.0 / max(n_states, 1), posinf=1.0 / max(n_states, 1), neginf=0.0)


def confidence_from_posterior(prob: np.ndarray, scale: float) -> float:
    prob = np.nan_to_num(prob, nan=0.0, posinf=0.0, neginf=0.0)
    total = float(prob.sum())
    if total <= 0:
        return 0.0
    prob = prob / total
    ordered = np.sort(prob)
    margin = float(ordered[-1] - ordered[-2]) if len(ordered) > 1 else 1.0
    return float(np.clip(margin / max(scale, 1e-6), 0.0, 1.0))


def robust_z_by_date(raw_by_sector: dict[str, float]) -> dict[str, float]:
    values = np.asarray([v for v in raw_by_sector.values() if math.isfinite(v)], dtype=np.float64)
    if len(values) < 3:
        return {k: 0.0 for k in raw_by_sector}
    med = float(np.median(values))
    q25, q75 = np.quantile(values, [0.25, 0.75])
    iqr = float(q75 - q25)
    scale = iqr / 1.349 if iqr > 1e-12 else float(np.std(values))
    if scale < 1e-12:
        return {k: 0.0 for k in raw_by_sector}
    return {k: float(np.clip((v - med) / scale, -3.0, 3.0)) for k, v in raw_by_sector.items()}


def rank_signal_by_date(raw_by_sector: dict[str, float]) -> dict[str, float]:
    valid = [(k, v) for k, v in raw_by_sector.items() if math.isfinite(v)]
    if len(valid) < 2:
        return {k: 0.0 for k in raw_by_sector}
    ordered = sorted(valid, key=lambda item: item[1])
    denom = max(len(ordered) - 1, 1)
    ranks = {k: (idx / denom) * 2.0 - 1.0 for idx, (k, _) in enumerate(ordered)}
    return {k: float(ranks.get(k, 0.0)) for k in raw_by_sector}


def fetch_sector_memberships(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT ts_code, l2_code, in_date, out_date
        FROM market.sw_index_member
        WHERE ts_code IS NOT NULL AND l2_code IS NOT NULL AND in_date IS NOT NULL
        """
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "ts_code": str(row["ts_code"]),
                "l2_code": str(row["l2_code"]),
                "in_date": parse_date(row["in_date"]),
                "out_date": parse_date(row["out_date"]) if row["out_date"] else None,
            }
        )
    cur.close()
    return rows


def build_date_sector_maps(memberships: list[dict[str, Any]], signal_dates: Iterable[date]) -> dict[date, dict[str, str]]:
    by_date: dict[date, dict[str, str]] = {}
    for td in signal_dates:
        mapping = {}
        for row in memberships:
            out = row["out_date"]
            if row["in_date"] <= td and (out is None or out >= td):
                mapping[row["ts_code"]] = row["l2_code"]
        by_date[td] = mapping
    return by_date


def compute_state_validation_stats(
    hmm: GaussianHMM,
    obs: np.ndarray,
    dates: list[date],
    future: dict[int, list[float]],
    val_start: date,
    val_end: date,
    n_states: int,
) -> dict[str, Any]:
    post = forward_posteriors(hmm, obs)
    hard_states = np.argmax(post, axis=1)
    stats: dict[int, dict[int, list[float]]] = {s: {5: [], 10: [], 20: []} for s in range(n_states)}
    for i, td in enumerate(dates):
        if not (val_start <= td <= val_end):
            continue
        state = int(hard_states[i])
        for horizon in (5, 10, 20):
            value = future[horizon][i] if i < len(future[horizon]) else float("nan")
            if not math.isnan(value):
                stats[state][horizon].append(float(value))

    out: dict[str, Any] = {}
    for state in range(n_states):
        row: dict[str, Any] = {}
        for horizon in (5, 10, 20):
            vals = stats[state][horizon]
            if vals:
                arr = np.asarray(vals, dtype=np.float64)
                lo, hi = np.quantile(arr, [0.05, 0.95]) if len(arr) >= 5 else (float(np.min(arr)), float(np.max(arr)))
                row[f"mu_{horizon}d"] = float(np.mean(arr))
                row[f"winsor_mu_{horizon}d"] = float(np.mean(np.clip(arr, lo, hi)))
                row[f"median_{horizon}d"] = float(np.median(arr))
                row[f"pup_{horizon}d"] = float(np.mean(arr > 0))
            else:
                row[f"mu_{horizon}d"] = 0.0
                row[f"winsor_mu_{horizon}d"] = 0.0
                row[f"median_{horizon}d"] = 0.0
                row[f"pup_{horizon}d"] = 0.5
            row[f"n_{horizon}d"] = len(vals)
        out[str(state)] = row
    return out


def utility_from_state_stats(
    prob: np.ndarray,
    state_stats: dict[str, Any],
    method: str,
    horizon_weights: dict[int, float],
) -> float:
    if method in {"er", "er_winsor", "er_median"}:
        field_prefix = {"er": "mu", "er_winsor": "winsor_mu", "er_median": "median"}[method]
        total = 0.0
        for horizon, weight in horizon_weights.items():
            vals = np.asarray(
                [state_stats[str(s)].get(f"{field_prefix}_{horizon}d", 0.0) for s in range(len(prob))],
                dtype=np.float64,
            )
            total += weight * float(np.dot(prob, vals))
        return total
    if method in {"pup", "pup_z", "pup_rank", "additive_pup"}:
        total = 0.0
        for horizon, weight in horizon_weights.items():
            vals = np.asarray(
                [state_stats[str(s)].get(f"pup_{horizon}d", 0.5) for s in range(len(prob))],
                dtype=np.float64,
            )
            total += weight * float(np.dot(prob, vals))
        return 2.0 * (total - 0.5)
    raise ValueError(f"Unknown method: {method}")


def train_one_variant(
    spec: VariantSpec,
    args: argparse.Namespace,
    sector_data: dict[str, list[dict[str, Any]]],
    index_ret: dict[date, float],
    market_vol: dict[date, float],
    output_root: Path,
) -> dict[str, Any]:
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    val_start = parse_date(args.val_start)
    val_end = parse_date(args.val_end)
    test_start = parse_date(args.test_start)
    test_end = parse_date(args.test_end)

    train_obs_raw: dict[str, np.ndarray] = {}
    train_rows_by_sector: dict[str, list[dict[str, Any]]] = {}
    full_obs_by_sector: dict[str, tuple[np.ndarray, list[date], dict[int, list[float]]]] = {}
    for code, rows in sector_data.items():
        full_obs, full_dates, full_future = base.build_observations(rows, index_ret, market_vol)
        if len(full_obs) < args.min_trading_days:
            continue
        full_obs_by_sector[code] = (full_obs, full_dates, full_future)
        train_rows = [row for row in rows if train_start <= row["trade_date"] <= train_end]
        obs, _, _ = base.build_observations(train_rows, index_ret, market_vol)
        if len(obs) < args.min_trading_days:
            continue
        train_obs_raw[code] = obs
        train_rows_by_sector[code] = train_rows
    if not train_obs_raw:
        raise RuntimeError("No sector has enough training observations")

    preprocess_params = base.preprocess_fit(train_obs_raw, args.winsor_q)
    models: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, str]] = []
    fixed_count = 0
    anomaly_total = 0
    for idx, code in enumerate(sorted(train_obs_raw)):
        obs = base.preprocess_apply(train_obs_raw[code], preprocess_params)
        sector_name = train_rows_by_sector[code][0].get("l2_name") or code
        try:
            hmm = GaussianHMM(
                n_components=spec.n_states,
                covariance_type="diag",
                n_iter=args.n_iter,
                min_covar=args.min_covar,
                random_state=spec.random_state,
            )
            hmm.fit(obs)
            cov_stats = base.fix_diag_covariance(hmm, args.min_covar, args.max_covar)
            hmm.transmat_ = base.smooth_transition(hmm.transmat_, args.alpha_smooth, args.min_self_trans)
            full_obs, full_dates, full_future = full_obs_by_sector[code]
            full_obs_scaled = base.preprocess_apply(full_obs, preprocess_params)
            state_stats = compute_state_validation_stats(
                hmm,
                full_obs_scaled,
                full_dates,
                full_future,
                val_start,
                val_end,
                spec.n_states,
            )
            if cov_stats["covariance_fixed"]:
                fixed_count += 1
                anomaly_total += int(cov_stats["covariance_anomaly_count"])
            models[code] = {
                "sector_code": code,
                "sector_name": sector_name,
                "n_states": spec.n_states,
                "covariance_type": "diag",
                "startprob": hmm.startprob_.tolist(),
                "transmat": hmm.transmat_.tolist(),
                "means": hmm.means_.tolist(),
                "covars": np.asarray(hmm.covars_).tolist(),
                "state_validation_stats": state_stats,
                "state_self_transition": {str(i): float(hmm.transmat_[i, i]) for i in range(spec.n_states)},
                "feature_names": base.FEATURE_NAMES,
                "preprocess": preprocess_params,
                "training_days": int(len(obs)),
                **cov_stats,
            }
        except Exception as exc:
            skipped.append({"sector_code": code, "error": repr(exc)})
            print(f"WARNING: skipped {code} ({sector_name}): {exc}", file=sys.stderr)
        if (idx + 1) % 25 == 0:
            print(f"[{spec.name}] trained {idx + 1}/{len(train_obs_raw)} sectors")

    if not models:
        raise RuntimeError(f"Training produced no models for {spec.name}")

    raw_signal_by_date: dict[str, dict[str, float]] = defaultdict(dict)
    confidence_by_date: dict[str, dict[str, float]] = defaultdict(dict)
    for code, info in models.items():
        full_obs, full_dates, _ = full_obs_by_sector[code]
        hmm = base.restore_hmm(info)
        post = forward_posteriors(hmm, base.preprocess_apply(full_obs, preprocess_params))
        for i, td in enumerate(full_dates):
            if not (test_start <= td <= test_end):
                continue
            prob = post[i]
            signal = utility_from_state_stats(prob, info["state_validation_stats"], spec.method, spec.horizon_weights)
            if not math.isfinite(signal):
                signal = 0.0
            raw_signal_by_date[td.isoformat()][code] = float(signal)
            confidence_by_date[td.isoformat()][code] = confidence_from_posterior(prob, spec.confidence_scale)

    signal_payload: dict[str, dict[str, dict[str, float]]] = {}
    daily_coefficients: dict[str, dict[str, float]] = {}
    for d, raw_map in raw_signal_by_date.items():
        if spec.method in {"er", "er_winsor", "er_median", "pup_z"}:
            z_map = robust_z_by_date(raw_map)
        elif spec.method == "pup_rank":
            z_map = rank_signal_by_date(raw_map)
        else:
            z_map = raw_map
        signal_payload[d] = {}
        daily_coefficients[d] = {}
        for code, raw_signal in raw_map.items():
            confidence = confidence_by_date[d].get(code, 0.0)
            if spec.method in {"er", "er_winsor", "er_median", "pup_z", "pup_rank"}:
                normalized_signal = z_map.get(code, 0.0)
                if not math.isfinite(normalized_signal):
                    normalized_signal = 0.0
                coeff = 1.0 + spec.coefficient_lambda * confidence * normalized_signal
            elif spec.method == "pup":
                normalized_signal = float(np.clip(raw_signal, -1.0, 1.0))
                if not math.isfinite(normalized_signal):
                    normalized_signal = 0.0
                coeff = 1.0 + spec.coefficient_lambda * confidence * normalized_signal
            else:
                normalized_signal = float(np.clip(raw_signal, -1.0, 1.0))
                if not math.isfinite(normalized_signal):
                    normalized_signal = 0.0
                coeff = 1.0
            if confidence < spec.confidence_floor or abs(normalized_signal) < spec.neutral_band:
                coeff = 1.0
                if spec.method == "additive_pup":
                    normalized_signal = 0.0
            coeff = float(np.clip(coeff, spec.coeff_min, spec.coeff_max))
            if not math.isfinite(coeff):
                coeff = 1.0
            daily_coefficients[d][code] = round(coeff, 8)
            signal_payload[d][code] = {
                "raw_signal": round(float(raw_signal), 10),
                "normalized_signal": round(float(normalized_signal), 10),
                "confidence": round(float(confidence), 8),
                "coefficient": round(coeff, 8),
            }

    variant_id = f"offline_{spec.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    variant_dir = output_root / "models" / variant_id
    variant_dir.mkdir(parents=True, exist_ok=False)
    model_path = variant_dir / "models.json"
    coeff_path = variant_dir / f"coefficients_{spec.name}_{test_start}_{test_end}.json"
    metadata_path = variant_dir / "metadata.json"
    model_path.write_text(json.dumps(models, ensure_ascii=False, indent=2), encoding="utf-8")
    coeff_payload = {
        "version": "offline_dynamic_hmm_20260429",
        "variant_name": spec.name,
        "direction": spec.direction,
        "qe_ready": spec.qe_ready,
        "model_path": base.wsl_to_windows_path(model_path),
        "model_path_wsl": str(model_path),
        "test_start": test_start.isoformat(),
        "backtest_end": test_end.isoformat(),
        "train_period": f"{train_start} ~ {train_end}",
        "validation_period": f"{val_start} ~ {val_end}",
        "method": spec.method,
        "n_states": spec.n_states,
        "horizon_weights": {str(k): v for k, v in spec.horizon_weights.items()},
        "coefficient_lambda": spec.coefficient_lambda,
        "coefficient_bounds": [spec.coeff_min, spec.coeff_max],
        "confidence_scale": spec.confidence_scale,
        "additive_beta": spec.additive_beta,
        "neutral_band": spec.neutral_band,
        "confidence_floor": spec.confidence_floor,
        "daily_coefficients": daily_coefficients,
        "daily_sector_signals": signal_payload,
        "stock_sector_map_policy": "PIT membership is applied in validation; static map intentionally omitted",
    }
    coeff_path.write_text(json.dumps(coeff_payload, ensure_ascii=False), encoding="utf-8")
    metadata = {
        "variant_id": variant_id,
        "variant": asdict(spec),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "script": "scripts/hmm_dynamic_offline_experiments.py",
        "db_write": False,
        "sector_count": len(models),
        "skipped_sector_count": len(skipped),
        "covariance_fixed_sector_count": fixed_count,
        "covariance_anomaly_count_sum": anomaly_total,
        "feature_names": base.FEATURE_NAMES,
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "val_start": val_start.isoformat(),
        "val_end": val_end.isoformat(),
        "test_start": test_start.isoformat(),
        "test_end": test_end.isoformat(),
        "model_path": base.wsl_to_windows_path(model_path),
        "coefficients_path": base.wsl_to_windows_path(coeff_path),
        "model_path_wsl": str(model_path),
        "coefficients_path_wsl": str(coeff_path),
        "skipped": skipped,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return {**metadata, "coeff_payload": coeff_payload}


def load_qlib_prices(qlib_uri: str, instruments_file: Path, start: date, end: date, max_symbols: int | None) -> pd.DataFrame:
    import qlib
    from qlib.data import D

    qlib.init(provider_uri=qlib_uri, region="cn")
    symbols: list[str] = []
    with instruments_file.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if not parts:
                continue
            symbols.append(parts[0])
            if max_symbols and len(symbols) >= max_symbols:
                break
    print(f"Loading qlib prices for symbols={len(symbols)} {start}..{end}")
    df = D.features(
        instruments=symbols,
        fields=["$close", "$volume"],
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        freq="day",
    )
    df = df.reset_index().rename(
        columns={"instrument": "ts_code", "datetime": "trade_date", "$close": "close", "$volume": "volume"}
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["close", "volume"])
    df = df[(df["close"] > 0) & (df["volume"] > 0)]
    return df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)


def prepare_stock_scores(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("ts_code", group_keys=False)
    for window in (5, 10, 20):
        df[f"past_{window}d"] = grouped["close"].pct_change(window)
        df[f"fwd_{window}d"] = grouped["close"].shift(-window) / df["close"] - 1.0
    for window in (5, 10, 20):
        df[f"rank_past_{window}d"] = df.groupby("trade_date")[f"past_{window}d"].rank(pct=True)
    df["raw_score"] = 0.35 * df["rank_past_5d"] + 0.35 * df["rank_past_10d"] + 0.30 * df["rank_past_20d"]
    return df[np.isfinite(df["raw_score"])].copy()


def max_drawdown(nav: list[float]) -> float:
    peak = -float("inf")
    worst = 0.0
    for value in nav:
        peak = max(peak, value)
        if peak > 0:
            worst = min(worst, value / peak - 1.0)
    return worst


def annualized_return(total_return: float, periods: int, rebalance_days: int) -> float:
    if periods <= 0:
        return 0.0
    return (1.0 + total_return) ** (252.0 / (periods * rebalance_days)) - 1.0


def run_backtest(
    *,
    name: str,
    coeff_payload: dict[str, Any] | None,
    by_date: dict[date, pd.DataFrame],
    signal_dates: list[date],
    date_sector_maps: dict[date, dict[str, str]],
    rebalance_days: int,
    topk: int,
) -> dict[str, Any]:
    nav = [1.0]
    period_rows: list[dict[str, Any]] = []
    previous: set[str] | None = None
    daily_coefficients = coeff_payload.get("daily_coefficients", {}) if coeff_payload else {}
    daily_sector_signals = coeff_payload.get("daily_sector_signals", {}) if coeff_payload else {}
    method = coeff_payload.get("method") if coeff_payload else "baseline"
    additive_beta = float(coeff_payload.get("additive_beta", 0.0)) if coeff_payload else 0.0
    missing_coeff_count = 0

    for signal_date in signal_dates:
        frame = by_date.get(signal_date)
        sector_map = date_sector_maps.get(signal_date, {})
        if frame is None or frame.empty or not sector_map:
            continue
        candidates = frame.dropna(subset=[f"fwd_{rebalance_days}d", "fwd_10d", "fwd_20d"]).copy()
        if candidates.empty:
            continue
        candidates["sector_code"] = candidates["ts_code"].map(sector_map)
        candidates = candidates.dropna(subset=["sector_code"])
        if candidates.empty:
            continue
        raw_top = candidates.nlargest(topk, "raw_score")

        if coeff_payload:
            day_coeffs = daily_coefficients.get(signal_date.isoformat(), {})
            day_signals = daily_sector_signals.get(signal_date.isoformat(), {})
            coeffs: list[float] = []
            sector_signals: list[float] = []
            for sector in candidates["sector_code"]:
                coeff = day_coeffs.get(str(sector))
                signal_info = day_signals.get(str(sector), {})
                if coeff is None:
                    missing_coeff_count += 1
                    coeff = 1.0
                coeffs.append(float(coeff))
                sector_signals.append(float(signal_info.get("normalized_signal", 0.0)))
            candidates["hmm_coeff"] = coeffs
            candidates["hmm_signal"] = sector_signals
            if method == "additive_pup":
                candidates["adjusted_score"] = candidates["raw_score"] + additive_beta * candidates["hmm_signal"]
            else:
                candidates["adjusted_score"] = candidates["raw_score"] * candidates["hmm_coeff"]
        else:
            candidates["hmm_coeff"] = 1.0
            candidates["hmm_signal"] = 0.0
            candidates["adjusted_score"] = candidates["raw_score"]

        selected = candidates.nlargest(topk, "adjusted_score")
        selected_set = set(selected["ts_code"])
        raw_set = set(raw_top["ts_code"])
        hmm_only = selected[selected["ts_code"].isin(selected_set - raw_set)]
        raw_only = raw_top[raw_top["ts_code"].isin(raw_set - selected_set)]
        period_ret = float(selected[f"fwd_{rebalance_days}d"].mean()) if not selected.empty else 0.0
        nav.append(nav[-1] * (1.0 + period_ret))
        turnover = None if previous is None else len(selected_set.symmetric_difference(previous)) / max(len(selected_set), 1)
        previous = selected_set
        row = {
            "signal_date": signal_date.isoformat(),
            "period_return": period_ret,
            "nav": nav[-1],
            "selected_count": int(len(selected)),
            "raw_selected_count": int(len(raw_top)),
            "replaced_count": int(len(hmm_only)),
            "turnover_proxy": turnover,
            "avg_coeff": float(selected["hmm_coeff"].mean()),
            "avg_signal": float(selected["hmm_signal"].mean()),
        }
        for horizon in (5, 10, 20):
            row[f"selected_fwd{horizon}"] = float(selected[f"fwd_{horizon}d"].mean()) if not selected.empty else None
            row[f"hmm_only_fwd{horizon}"] = float(hmm_only[f"fwd_{horizon}d"].mean()) if not hmm_only.empty else None
            row[f"raw_only_fwd{horizon}"] = float(raw_only[f"fwd_{horizon}d"].mean()) if not raw_only.empty else None
        period_rows.append(row)

    period_returns = [r["period_return"] for r in period_rows]
    total_return = nav[-1] - 1.0 if nav else 0.0
    mean = float(np.mean(period_returns)) if period_returns else 0.0
    std = float(np.std(period_returns, ddof=1)) if len(period_returns) > 1 else 0.0
    sharpe = (mean / std * math.sqrt(252.0 / rebalance_days)) if std > 1e-12 else 0.0
    monthly: dict[str, float] = {}
    for row in period_rows:
        month = row["signal_date"][:7]
        monthly[month] = (1.0 + monthly.get(month, 0.0)) * (1.0 + row["period_return"]) - 1.0

    summary: dict[str, Any] = {
        "name": name,
        "method": method,
        "qe_ready": bool(coeff_payload.get("qe_ready", True)) if coeff_payload else True,
        "periods": len(period_rows),
        "total_return": total_return,
        "annualized_return": annualized_return(total_return, len(period_rows), rebalance_days),
        "sharpe": sharpe,
        "max_drawdown": max_drawdown(nav),
        "monthly_win_rate": float(np.mean([1.0 if v > 0 else 0.0 for v in monthly.values()])) if monthly else 0.0,
        "avg_replaced_count": float(np.mean([r["replaced_count"] for r in period_rows])) if period_rows else 0.0,
        "avg_turnover_proxy": float(np.nanmean([r["turnover_proxy"] for r in period_rows if r["turnover_proxy"] is not None])) if len(period_rows) > 1 else None,
        "missing_coeff_count": missing_coeff_count,
        "period_rows": period_rows,
        "monthly_returns": monthly,
    }
    for horizon in (5, 10, 20):
        hmm_vals = [r[f"hmm_only_fwd{horizon}"] for r in period_rows if r[f"hmm_only_fwd{horizon}"] is not None]
        raw_vals = [r[f"raw_only_fwd{horizon}"] for r in period_rows if r[f"raw_only_fwd{horizon}"] is not None]
        summary[f"hmm_only_fwd{horizon}"] = float(np.mean(hmm_vals)) if hmm_vals else None
        summary[f"raw_only_fwd{horizon}"] = float(np.mean(raw_vals)) if raw_vals else None
        summary[f"replacement_spread_{horizon}"] = (
            float(np.mean(hmm_vals) - np.mean(raw_vals)) if hmm_vals and raw_vals else None
        )
    return summary


def format_pct(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return ""
    return f"{float(value) * 100:.2f}%"


def write_outputs(output_root: Path, result: dict[str, Any]) -> None:
    json_path = output_root / "run_summary.json"
    csv_path = output_root / "summary.csv"
    monthly_path = output_root / "monthly.csv"
    report_path = output_root / "report.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = [{k: v for k, v in item.items() if k not in {"period_rows", "monthly_returns"}} for item in result["summaries"]]
    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")

    monthly_rows = []
    for item in result["summaries"]:
        for month, ret in item.get("monthly_returns", {}).items():
            monthly_rows.append({"name": item["name"], "month": month, "return": ret})
    pd.DataFrame(monthly_rows).to_csv(monthly_path, index=False, encoding="utf-8-sig")

    ranked = sorted(result["summaries"], key=lambda x: (x["total_return"], x["sharpe"], -x["max_drawdown"]), reverse=True)
    baseline = next((x for x in result["summaries"] if x["name"] == "NO_HMM_BASELINE"), None)
    base_total = baseline["total_return"] if baseline else 0.0
    base_sharpe = baseline["sharpe"] if baseline else -999.0
    lines = [
        "# HMM Dynamic Coefficient Offline Experiment Report",
        "",
        f"Created at: {result['created_at']}",
        "",
        "## Scope",
        "",
        "- Offline-only: no writes to model_train_* DB tables.",
        "- No AIstock backend/frontend program code was modified by this experiment script run.",
        "- qlib daily data is used for the one-year stock return validation; DB sector data is used only for HMM training inputs.",
        "- Additive overlay is script-diagnostic only because QE runtime may require a score-adjustment change.",
        "",
        "## Split",
        "",
        f"- Train: {result['split']['train_start']} ~ {result['split']['train_end']}",
        f"- Validation: {result['split']['val_start']} ~ {result['split']['val_end']}",
        f"- Test: {result['split']['test_start']} ~ {result['split']['test_end']}",
        "",
        "## Ranking",
        "",
        "| Rank | Version | QE-ready | Total | Ann. | Sharpe | MaxDD | Avg Replaced | Replacement Spread 5D | Recommendation |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for i, item in enumerate(ranked, 1):
        delta = item["total_return"] - base_total
        if item["name"] == "NO_HMM_BASELINE":
            rec = "baseline"
        elif item.get("method") == "additive_pup":
            rec = "script-only; QE requires runtime change"
        elif delta >= -0.01 and item["sharpe"] >= base_sharpe - 0.15:
            rec = "candidate for QE validation"
        elif delta >= -0.03:
            rec = "keep for modification / second pass"
        else:
            rec = "reject or redesign"
        lines.append(
            f"| {i} | `{item['name']}` | {str(item.get('qe_ready', True)).lower()} | "
            f"{format_pct(item['total_return'])} | {format_pct(item['annualized_return'])} | "
            f"{item['sharpe']:.3f} | {format_pct(item['max_drawdown'])} | "
            f"{item.get('avg_replaced_count', 0):.2f} | {format_pct(item.get('replacement_spread_5'))} | {rec} |"
        )
    lines.extend(
        [
            "",
            "## Artifact Paths",
            "",
            f"- JSON: `{base.wsl_to_windows_path(json_path)}`",
            f"- Summary CSV: `{base.wsl_to_windows_path(csv_path)}`",
            f"- Monthly CSV: `{base.wsl_to_windows_path(monthly_path)}`",
            "",
        ]
    )
    for meta in result["variant_metadata"]:
        lines.append(f"- `{meta['variant']['name']}`: `{meta['model_path']}`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run offline dynamic HMM experiments and qlib-data validation")
    parser.add_argument("--train-start", default="2021-01-04")
    parser.add_argument("--train-end", default="2024-11-29")
    parser.add_argument("--val-start", default="2024-12-02")
    parser.add_argument("--val-end", default="2025-03-10")
    parser.add_argument("--test-start", default="2025-03-11")
    parser.add_argument("--test-end", default="2026-03-03")
    parser.add_argument("--output-root", default="/mnt/f/Dev/AIstock/.codex_tmp/hmm_dynamic_offline_20260429")
    parser.add_argument("--qlib-uri", default="/home/lc999/data/qlib_bin")
    parser.add_argument("--instruments-file", default="/home/lc999/data/qlib_bin/instruments/all.txt")
    parser.add_argument("--rebalance-days", type=int, default=5)
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--n-iter", type=int, default=300)
    parser.add_argument("--min-trading-days", type=int, default=120)
    parser.add_argument("--min-covar", type=float, default=1e-3)
    parser.add_argument("--max-covar", type=float, default=10.0)
    parser.add_argument("--alpha-smooth", type=float, default=0.1)
    parser.add_argument("--min-self-trans", type=float, default=0.75)
    parser.add_argument("--winsor-q", type=float, default=0.01)
    parser.add_argument("--max-symbols", type=int, default=None, help="Debug-only qlib symbol cap")
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    val_start = parse_date(args.val_start)
    val_end = parse_date(args.val_end)
    test_start = parse_date(args.test_start)
    test_end = parse_date(args.test_end)
    if not (train_end < val_start and val_end < test_start):
        raise ValueError("Split must satisfy train_end < val_start and val_end < test_start")

    output_root = Path(base.windows_to_wsl_path(args.output_root)).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    history_start = train_start - timedelta(days=80)
    data_end = test_end + timedelta(days=40)

    conn = base.connect_db(
        base.DBConfig(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password or os.getenv("TDX_DB_PASSWORD", ""),
        )
    )
    print(f"Loading sector data {history_start}..{data_end}")
    sector_data = base.fetch_sector_data(conn, history_start, data_end)
    index_ret = base.fetch_index_daily(conn, history_start, data_end)
    market_vol = base.fetch_market_volume(conn, history_start, data_end)
    memberships = fetch_sector_memberships(conn)
    conn.close()
    print(
        f"Loaded sector_count={len(sector_data)}, index_days={len(index_ret)}, "
        f"market_vol_days={len(market_vol)}, memberships={len(memberships)}"
    )

    variant_metadata: list[dict[str, Any]] = []
    coeff_payloads: dict[str, dict[str, Any]] = {}
    for spec in DEFAULT_VARIANTS:
        print(f"\n=== Training {spec.name} ===")
        meta = train_one_variant(spec, args, sector_data, index_ret, market_vol, output_root)
        coeff_payloads[spec.name] = meta.pop("coeff_payload")
        variant_metadata.append(meta)

    qlib_start = test_start - timedelta(days=80)
    qlib_end = test_end + timedelta(days=12)
    qlib_df = load_qlib_prices(args.qlib_uri, Path(args.instruments_file), qlib_start, qlib_end, args.max_symbols)
    scored = prepare_stock_scores(qlib_df)
    test_dates = sorted(d for d in scored["trade_date"].unique() if test_start <= d <= test_end)
    signal_dates = test_dates[:: args.rebalance_days]
    signal_date_set = set(signal_dates)
    by_date = {td: frame for td, frame in scored.groupby("trade_date") if td in signal_date_set}
    date_sector_maps = build_date_sector_maps(memberships, signal_dates)
    print(f"Running validation signal_dates={len(signal_dates)}, qlib_rows={len(scored)}")

    summaries: list[dict[str, Any]] = [
        run_backtest(
            name="NO_HMM_BASELINE",
            coeff_payload=None,
            by_date=by_date,
            signal_dates=signal_dates,
            date_sector_maps=date_sector_maps,
            rebalance_days=args.rebalance_days,
            topk=args.topk,
        )
    ]
    for name, payload in coeff_payloads.items():
        summaries.append(
            run_backtest(
                name=name,
                coeff_payload=payload,
                by_date=by_date,
                signal_dates=signal_dates,
                date_sector_maps=date_sector_maps,
                rebalance_days=args.rebalance_days,
                topk=args.topk,
            )
        )

    result = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "split": {
            "train_start": train_start.isoformat(),
            "train_end": train_end.isoformat(),
            "val_start": val_start.isoformat(),
            "val_end": val_end.isoformat(),
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
        },
        "scope": {
            "db_write": False,
            "qe_experiment": False,
            "application_code_change": False,
            "qlib_uri": args.qlib_uri,
            "topk": args.topk,
            "rebalance_days": args.rebalance_days,
        },
        "variant_metadata": variant_metadata,
        "summaries": summaries,
    }
    write_outputs(output_root, result)
    print(
        json.dumps(
            {
                "output_root": base.wsl_to_windows_path(output_root),
                "summaries": [{k: v for k, v in s.items() if k not in {"period_rows", "monthly_returns"}} for s in summaries],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
