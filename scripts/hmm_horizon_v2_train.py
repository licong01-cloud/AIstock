#!/usr/bin/env python3
"""Additive horizon-aware sector HMM trainer.

This script creates a new HMM asset and optional DB snapshot. It does not
modify legacy HMM scripts or overwrite existing model directories.
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
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from hmmlearn.hmm import GaussianHMM

FEATURE_NAMES = [
    "daily_return",
    "excess_return_5d_mean",
    "excess_return_10d_mean",
    "excess_return_20d_mean",
    "volatility_5d",
    "volatility_10d",
    "volatility_20d",
    "volume_share_5d_mean",
    "net_mf_ratio_5d_mean",
    "elg_net_mf_ratio_5d_mean",
]
LABELS = ("fading", "neutral", "trending")
HORIZON_WEIGHTS = {5: 0.35, 10: 0.35, 20: 0.30}


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def windows_to_wsl_path(path: str) -> str:
    text = path.replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def wsl_to_windows_path(path: Path | str) -> str:
    text = str(path)
    if text.startswith("/mnt/") and len(text) > 6 and text[6] == "/":
        drive = text[5].upper()
        rest = text[6:].replace("/", "\\")
        return f"{drive}:{rest}"
    return text


def candidate_hosts(initial: str) -> list[str]:
    hosts: list[str] = []
    for item in (initial, os.getenv("TDX_DB_HOST"), "127.0.0.1", "localhost"):
        if item and item not in hosts:
            hosts.append(item)
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


def connect_db(cfg: DBConfig):
    errors: list[str] = []
    for host in candidate_hosts(cfg.host):
        try:
            conn = psycopg2.connect(
                host=host,
                port=cfg.port,
                dbname=cfg.dbname,
                user=cfg.user,
                password=cfg.password,
                connect_timeout=5,
            )
            print(f"DB connected via host={host}")
            return conn
        except Exception as exc:
            errors.append(f"{host}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot connect to DB. Tried: " + "; ".join(errors))


def fetch_sector_data(conn, start: date, end: date) -> dict[str, list[dict[str, Any]]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT DISTINCT ON (m.l2_code, sd.trade_date)
            m.l2_code, m.l2_name, sd.trade_date,
            sd.sw2_pct_change, sd.sw2_vol, sd.sw2_amount,
            sd.sw2_mf_net_amt, sd.sw2_mf_buy_elg_amt, sd.sw2_mf_sell_elg_amt
        FROM market.sector_data sd
        JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
        WHERE sd.trade_date BETWEEN %s AND %s
          AND m.in_date <= sd.trade_date
          AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
        ORDER BY m.l2_code, sd.trade_date, sd.ts_code
        """,
        (start, end),
    )
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cur.fetchall():
        result[str(row["l2_code"])].append(
            {
                "trade_date": row["trade_date"],
                "l2_name": row["l2_name"] or str(row["l2_code"]),
                "pct_change": float(row["sw2_pct_change"] or 0.0),
                "vol": float(row["sw2_vol"] or 0.0),
                "amount": float(row["sw2_amount"] or 0.0),
                "mf_net_amt": float(row["sw2_mf_net_amt"] or 0.0),
                "mf_buy_elg_amt": float(row["sw2_mf_buy_elg_amt"] or 0.0),
                "mf_sell_elg_amt": float(row["sw2_mf_sell_elg_amt"] or 0.0),
            }
        )
    cur.close()
    return dict(result)


def fetch_index_daily(conn, start: date, end: date) -> dict[date, float]:
    cur = conn.cursor()
    for code in ("000300.SH", "399300.SZ"):
        cur.execute(
            """
            SELECT trade_date, pct_chg
            FROM market.index_daily
            WHERE ts_code = %s AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (code, start, end),
        )
        rows = cur.fetchall()
        if rows:
            cur.close()
            print(f"Loaded benchmark {code}: {len(rows)} days")
            return {td: float(pct or 0.0) / 100.0 for td, pct in rows}
    cur.close()
    return {}


def fetch_market_volume(conn, start: date, end: date) -> dict[date, float]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_date, SUM(vol)
        FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        (start, end),
    )
    data = {td: float(vol or 0.0) for td, vol in cur.fetchall()}
    cur.close()
    return data


def fetch_stock_sector_map(conn, as_of: date) -> dict[str, str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts_code, l2_code
        FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        """,
        (as_of, as_of),
    )
    data = {str(ts): str(code) for ts, code in cur.fetchall() if ts and code}
    cur.close()
    return data


def trailing_mean(values: list[float], idx: int, window: int) -> float:
    chunk = values[max(0, idx - window + 1) : idx + 1]
    return float(np.mean(chunk)) if chunk else 0.0


def trailing_std(values: list[float], idx: int, window: int) -> float:
    chunk = values[max(0, idx - window + 1) : idx + 1]
    return float(np.std(chunk)) if len(chunk) > 1 else 0.0


def build_observations(
    rows: list[dict[str, Any]],
    index_ret: dict[date, float],
    market_vol: dict[date, float],
) -> tuple[np.ndarray, list[date], dict[int, list[float]]]:
    rows = sorted(rows, key=lambda item: item["trade_date"])
    dates: list[date] = []
    daily_ret: list[float] = []
    daily_excess: list[float] = []
    vol_share: list[float] = []
    net_mf_ratio: list[float] = []
    elg_mf_ratio: list[float] = []

    for row in rows:
        td = row["trade_date"]
        bench = index_ret.get(td)
        total_vol = market_vol.get(td)
        if bench is None or total_vol is None or total_vol <= 0:
            continue
        ret = float(row["pct_change"]) / 100.0
        amount = float(row["amount"] or 0.0)
        elg_net = float(row["mf_buy_elg_amt"] or 0.0) - float(row["mf_sell_elg_amt"] or 0.0)
        dates.append(td)
        daily_ret.append(ret)
        daily_excess.append(ret - bench)
        vol_share.append(float(row["vol"] or 0.0) / total_vol)
        net_mf_ratio.append(float(row["mf_net_amt"] or 0.0) / amount if amount > 0 else 0.0)
        elg_mf_ratio.append(elg_net / amount if amount > 0 else 0.0)

    features: list[list[float]] = []
    for i, ret in enumerate(daily_ret):
        feature_row = [
            ret,
            trailing_mean(daily_excess, i, 5),
            trailing_mean(daily_excess, i, 10),
            trailing_mean(daily_excess, i, 20),
            trailing_std(daily_ret, i, 5),
            trailing_std(daily_ret, i, 10),
            trailing_std(daily_ret, i, 20),
            trailing_mean(vol_share, i, 5),
            trailing_mean(net_mf_ratio, i, 5),
            trailing_mean(elg_mf_ratio, i, 5),
        ]
        features.append(feature_row if np.isfinite(feature_row).all() else [0.0] * len(FEATURE_NAMES))

    future_excess: dict[int, list[float]] = {1: [], 3: [], 5: [], 10: [], 20: []}
    for i in range(len(dates)):
        for horizon in future_excess:
            if i + horizon < len(daily_excess):
                future_excess[horizon].append(float(sum(daily_excess[i + 1 : i + horizon + 1])))
            else:
                future_excess[horizon].append(float("nan"))

    if not features:
        return np.empty((0, len(FEATURE_NAMES)), dtype=np.float64), [], future_excess
    return np.asarray(features, dtype=np.float64), dates, future_excess


def preprocess_fit(obs_by_sector: dict[str, np.ndarray], winsor_q: float) -> dict[str, Any]:
    all_obs = np.vstack([obs for obs in obs_by_sector.values() if len(obs)])
    lower = np.quantile(all_obs, winsor_q, axis=0)
    upper = np.quantile(all_obs, 1.0 - winsor_q, axis=0)
    clipped = np.clip(all_obs, lower, upper)
    mean = clipped.mean(axis=0)
    std = clipped.std(axis=0)
    std = np.where(std < 1e-10, 1.0, std)
    return {
        "winsor_q": winsor_q,
        "winsor_lower": lower.tolist(),
        "winsor_upper": upper.tolist(),
        "zscore_mean": mean.tolist(),
        "zscore_std": std.tolist(),
    }


def preprocess_apply(obs: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    lower = np.asarray(params["winsor_lower"], dtype=np.float64)
    upper = np.asarray(params["winsor_upper"], dtype=np.float64)
    mean = np.asarray(params["zscore_mean"], dtype=np.float64)
    std = np.asarray(params["zscore_std"], dtype=np.float64)
    return (np.clip(obs, lower, upper) - mean) / std


def forward_filter_states(hmm: GaussianHMM, obs: np.ndarray) -> np.ndarray:
    from hmmlearn import _hmmc
    from hmmlearn.utils import normalize as hmm_normalize

    log_startprob = np.log(hmm.startprob_ + 1e-300)
    log_transmat = np.log(hmm.transmat_ + 1e-300)
    log_frameprob = hmm._compute_log_likelihood(obs)
    _, fwd_lattice = _hmmc.forward_log(log_startprob, log_transmat, log_frameprob)
    posteriors = np.exp(fwd_lattice)
    hmm_normalize(posteriors, axis=1)
    return posteriors.argmax(axis=1)


def fix_diag_covariance(hmm: GaussianHMM, min_covar: float, max_covar: float) -> dict[str, Any]:
    covars = np.asarray(hmm.covars_, dtype=np.float64)
    diag = np.asarray([np.diag(covars[i]) for i in range(covars.shape[0])]) if covars.ndim == 3 else covars.copy()
    mask = (diag < min_covar) | (diag > max_covar)
    fixed = np.clip(diag, min_covar, max_covar)
    hmm.covars_ = fixed
    return {
        "covariance_fixed": bool(mask.sum()),
        "covariance_anomaly_count": int(mask.sum()),
        "covariance_min_after": float(np.min(fixed)),
        "covariance_max_after": float(np.max(fixed)),
    }


def smooth_transition(transmat: np.ndarray, alpha: float, min_self: float) -> np.ndarray:
    n = transmat.shape[0]
    smoothed = np.zeros_like(transmat, dtype=np.float64)
    for i in range(n):
        row = transmat[i] + alpha
        row = row / row.sum()
        if row[i] < min_self:
            other_sum = row.sum() - row[i]
            row[i] = min_self
            for j in range(n):
                if j != i:
                    row[j] = row[j] * (1.0 - min_self) / other_sum if other_sum > 0 else (1.0 - min_self) / (n - 1)
        smoothed[i] = row / row.sum()
    return smoothed


def state_utilities(states: np.ndarray, future_excess: dict[int, list[float]], n_states: int) -> dict[int, float]:
    values: dict[int, list[float]] = {idx: [] for idx in range(n_states)}
    for i, state in enumerate(states):
        ok = True
        total = 0.0
        for horizon, weight in HORIZON_WEIGHTS.items():
            arr = future_excess[horizon]
            if i >= len(arr) or math.isnan(arr[i]):
                ok = False
                break
            total += weight * float(arr[i])
        if ok:
            values[int(state)].append(total)
    return {state: float(np.mean(vals)) if vals else float("nan") for state, vals in values.items()}


def label_states(utilities: dict[int, float], means: np.ndarray) -> dict[str, str]:
    usable = {state: value for state, value in utilities.items() if not math.isnan(value)}
    ordered = sorted(usable, key=lambda state: usable[state]) if len(usable) >= 3 else list(np.argsort(means[:, 0]))
    return {str(ordered[0]): "fading", str(ordered[1]): "neutral", str(ordered[2]): "trending"}


def restore_hmm(info: dict[str, Any]) -> GaussianHMM:
    hmm = GaussianHMM(n_components=int(info["n_states"]), covariance_type=info["covariance_type"])
    hmm.startprob_ = np.asarray(info["startprob"], dtype=np.float64)
    hmm.transmat_ = np.asarray(info["transmat"], dtype=np.float64)
    hmm.means_ = np.asarray(info["means"], dtype=np.float64)
    covars = np.asarray(info["covars"], dtype=np.float64)
    if info["covariance_type"] == "diag" and covars.ndim == 3:
        covars = np.asarray([np.diag(covars[i]) for i in range(covars.shape[0])], dtype=np.float64)
    hmm.covars_ = covars
    return hmm


def aggregate_validation_metrics(
    models: dict[str, dict[str, Any]],
    sector_data: dict[str, list[dict[str, Any]]],
    index_ret: dict[date, float],
    market_vol: dict[date, float],
    preprocess_params: dict[str, Any],
    val_start: date,
    val_end: date,
) -> dict[str, Any]:
    by_label: dict[str, dict[int, list[float]]] = {label: {1: [], 3: [], 5: [], 10: [], 20: []} for label in LABELS}
    decoded = 0
    predictions = 0
    for code, info in models.items():
        rows = sector_data.get(code)
        if not rows:
            continue
        obs, dates, future = build_observations(rows, index_ret, market_vol)
        if len(obs) < 30:
            continue
        states = forward_filter_states(restore_hmm(info), preprocess_apply(obs, preprocess_params))
        decoded += 1
        labels = info["state_labels"]
        for i, td in enumerate(dates):
            if not (val_start <= td <= val_end):
                continue
            label = labels.get(str(int(states[i])), "neutral")
            predictions += 1
            for horizon, vals in by_label[label].items():
                value = future[horizon][i] if i < len(future[horizon]) else float("nan")
                if not math.isnan(value):
                    vals.append(value)

    metrics: dict[str, Any] = {"decoded_sectors": decoded, "total_predictions": predictions, "validation_by_label": {}}
    for label in LABELS:
        row: dict[str, Any] = {}
        for horizon, vals in by_label[label].items():
            row[f"return_{horizon}d"] = float(np.mean(vals)) if vals else None
            row[f"return_{horizon}d_pct"] = round(float(np.mean(vals)) * 100.0, 6) if vals else None
            row[f"return_{horizon}d_n"] = len(vals)
        metrics["validation_by_label"][label] = row
    return metrics


def calibration_from_validation(metrics: dict[str, Any], lam: float, lo: float, hi: float) -> dict[str, Any]:
    utilities: dict[str, float] = {}
    for label in LABELS:
        row = metrics["validation_by_label"].get(label, {})
        total = 0.0
        ok = True
        for horizon, weight in HORIZON_WEIGHTS.items():
            value = row.get(f"return_{horizon}d")
            if value is None:
                ok = False
                break
            total += weight * float(value)
        utilities[label] = total if ok else 0.0
    arr = np.asarray([utilities[label] for label in LABELS], dtype=np.float64)
    center = float(arr.mean())
    std = float(arr.std())
    coeffs: dict[str, float] = {}
    for label in LABELS:
        z = (utilities[label] - center) / std if std > 1e-12 else 0.0
        coeff = float(np.clip(1.0 + lam * z, lo, hi))
        if utilities[label] <= 0.0:
            coeff = min(coeff, 1.0)
        coeffs[label] = round(coeff, 6)
    return {
        "preset_key": "preset_horizon_v2",
        "preset_coeffs": coeffs,
        "label_utilities": {k: round(v, 8) for k, v in utilities.items()},
        "label_utilities_pct": {k: round(v * 100.0, 6) for k, v in utilities.items()},
        "lambda": lam,
        "bounds": [lo, hi],
        "weights": {str(k): v for k, v in HORIZON_WEIGHTS.items()},
    }


def precompute_daily_coefficients(
    models: dict[str, dict[str, Any]],
    sector_data: dict[str, list[dict[str, Any]]],
    index_ret: dict[date, float],
    market_vol: dict[date, float],
    preprocess_params: dict[str, Any],
    start: date,
    end: date,
    coeffs: dict[str, float],
) -> dict[str, dict[str, float]]:
    sector_date_labels: dict[str, dict[str, str]] = {}
    for code, info in models.items():
        rows = sector_data.get(code)
        if not rows:
            continue
        obs, dates, _ = build_observations(rows, index_ret, market_vol)
        if len(obs) < 30:
            continue
        try:
            states = forward_filter_states(restore_hmm(info), preprocess_apply(obs, preprocess_params))
        except Exception as exc:
            print(f"WARNING: forward filter failed for {code}: {exc}", file=sys.stderr)
            continue
        labels = info["state_labels"]
        by_date = {
            td.isoformat(): labels.get(str(int(states[i])), "neutral")
            for i, td in enumerate(dates)
            if start <= td <= end
        }
        if by_date:
            sector_date_labels[code] = by_date

    all_dates = sorted({d for labels in sector_date_labels.values() for d in labels})
    daily: dict[str, dict[str, float]] = {}
    for d in all_dates:
        daily[d] = {code: float(coeffs.get(labels.get(d, "neutral"), 1.0)) for code, labels in sector_date_labels.items()}
    return daily


def train(args: argparse.Namespace) -> dict[str, Any]:
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    val_start = parse_date(args.val_start)
    val_end = parse_date(args.val_end)
    coeff_start = parse_date(args.coefficient_start)
    coeff_end = parse_date(args.coefficient_end)
    history_start = train_start - timedelta(days=60)
    data_end = coeff_end + timedelta(days=30)

    conn = connect_db(
        DBConfig(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password or os.getenv("TDX_DB_PASSWORD", ""),
        )
    )
    print(f"Loading data {history_start} to {data_end}")
    sector_data = fetch_sector_data(conn, history_start, data_end)
    index_ret = fetch_index_daily(conn, history_start, data_end)
    market_vol = fetch_market_volume(conn, history_start, data_end)
    stock_sector_map = fetch_stock_sector_map(conn, coeff_end)
    print(f"Loaded sectors={len(sector_data)}, index_days={len(index_ret)}, market_vol_days={len(market_vol)}, stock_map={len(stock_sector_map)}")

    train_obs_raw: dict[str, np.ndarray] = {}
    train_future_by_sector: dict[str, dict[int, list[float]]] = {}
    train_rows_by_sector: dict[str, list[dict[str, Any]]] = {}
    for code, rows in sector_data.items():
        train_rows = [row for row in rows if train_start <= row["trade_date"] <= train_end]
        if len(train_rows) < args.min_trading_days:
            continue
        obs, _, future = build_observations(train_rows, index_ret, market_vol)
        if len(obs) < args.min_trading_days:
            continue
        train_obs_raw[code] = obs
        train_future_by_sector[code] = future
        train_rows_by_sector[code] = train_rows
    if not train_obs_raw:
        raise RuntimeError("No sector has enough training observations")

    preprocess_params = preprocess_fit(train_obs_raw, args.winsor_q)
    models: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, str]] = []
    fixed_count = 0
    anomaly_total = 0
    for idx, code in enumerate(sorted(train_obs_raw)):
        obs = preprocess_apply(train_obs_raw[code], preprocess_params)
        sector_name = train_rows_by_sector[code][0].get("l2_name") or code
        try:
            hmm = GaussianHMM(
                n_components=args.n_states,
                covariance_type=args.covariance_type,
                n_iter=args.n_iter,
                min_covar=args.min_covar,
                random_state=args.random_state,
            )
            hmm.fit(obs)
            cov_stats = fix_diag_covariance(hmm, args.min_covar, args.max_covar)
            hmm.transmat_ = smooth_transition(hmm.transmat_, args.alpha_smooth, args.min_self_trans)
            states = forward_filter_states(hmm, obs)
            utilities = state_utilities(states, train_future_by_sector[code], args.n_states)
            labels = label_states(utilities, hmm.means_)
            if cov_stats["covariance_fixed"]:
                fixed_count += 1
                anomaly_total += int(cov_stats["covariance_anomaly_count"])
            models[code] = {
                "sector_code": code,
                "sector_name": sector_name,
                "n_states": args.n_states,
                "covariance_type": args.covariance_type,
                "startprob": hmm.startprob_.tolist(),
                "transmat": hmm.transmat_.tolist(),
                "means": hmm.means_.tolist(),
                "covars": np.asarray(hmm.covars_).tolist(),
                "state_labels": labels,
                "state_train_utilities": {str(k): None if math.isnan(v) else float(v) for k, v in utilities.items()},
                "state_self_transition": {str(i): float(hmm.transmat_[i, i]) for i in range(args.n_states)},
                "feature_names": FEATURE_NAMES,
                "preprocess": preprocess_params,
                "horizon_weights": {str(k): v for k, v in HORIZON_WEIGHTS.items()},
                "training_days": int(len(obs)),
                **cov_stats,
            }
        except Exception as exc:
            skipped.append({"sector_code": code, "error": repr(exc)})
            print(f"WARNING: skipped {code} ({sector_name}): {exc}", file=sys.stderr)
        if (idx + 1) % 20 == 0:
            print(f"Trained {idx + 1}/{len(train_obs_raw)} sectors")
    if not models:
        raise RuntimeError("Training produced no models")

    validation_metrics = aggregate_validation_metrics(models, sector_data, index_ret, market_vol, preprocess_params, val_start, val_end)
    calibration = calibration_from_validation(validation_metrics, args.calibration_lambda, args.coeff_min, args.coeff_max)
    daily_coefficients = precompute_daily_coefficients(
        models, sector_data, index_ret, market_vol, preprocess_params, coeff_start, coeff_end, calibration["preset_coeffs"]
    )
    print(f"Precomputed coefficients for {len(daily_coefficients)} dates")

    display_name = args.display_name or (
        f"HMM_HORIZON_V2_w5w10w20_n{args.n_states}_{args.covariance_type}_"
        f"train{train_start:%Y%m%d}_{train_end:%Y%m%d}_val{val_start:%Y%m%d}_{val_end:%Y%m%d}"
    )
    output_root = Path(windows_to_wsl_path(args.output_root)).resolve()
    snapshot_date = args.snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    metrics_json = {
        "snapshot_display_name": f"SNAPSHOT_{display_name}",
        "config_display_name": display_name,
        "version": "horizon_v2",
        "train_period": f"{train_start} ~ {train_end}",
        "val_period": f"{val_start} ~ {val_end}",
        "coefficient_period": f"{coeff_start} ~ {coeff_end}",
        "sector_count": len(models),
        "skipped_sector_count": len(skipped),
        "covariance_fixed_sector_count": fixed_count,
        "covariance_anomaly_count_sum": anomaly_total,
        "feature_names": FEATURE_NAMES,
        "horizon_weights": {str(k): v for k, v in HORIZON_WEIGHTS.items()},
        "calibration": calibration,
        **validation_metrics,
    }

    config_id: str
    snapshot_id: str | None = None
    job_id: str | None = None
    if args.register_db:
        cur = conn.cursor()
        config_json = {
            "script": "scripts/hmm_horizon_v2_train.py",
            "version": "horizon_v2",
            "n_states": args.n_states,
            "covariance_type": args.covariance_type,
            "n_iter": args.n_iter,
            "min_self_trans": args.min_self_trans,
            "alpha_smooth": args.alpha_smooth,
            "min_covar": args.min_covar,
            "max_covar": args.max_covar,
            "feature_names": FEATURE_NAMES,
            "winsor_q": args.winsor_q,
            "horizon_weights": {str(k): v for k, v in HORIZON_WEIGHTS.items()},
            "train_start": train_start.isoformat(),
            "train_end": train_end.isoformat(),
            "val_start": val_start.isoformat(),
            "val_end": val_end.isoformat(),
            "coefficient_start": coeff_start.isoformat(),
            "coefficient_end": coeff_end.isoformat(),
        }
        cur.execute(
            "INSERT INTO model_train_configs (model_type, display_name, config_json) VALUES ('sector_hmm', %s, %s) RETURNING config_id",
            (display_name, json.dumps(config_json, ensure_ascii=False)),
        )
        config_id = cur.fetchone()[0]
    else:
        config_id = f"hmm_horizon_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    model_dir = output_root / config_id / snapshot_date
    model_dir.mkdir(parents=True, exist_ok=False)
    model_path = model_dir / "models.json"
    coeff_path = model_dir / f"coefficients_{calibration['preset_key']}_{coeff_start}_{coeff_end}.json"
    model_path.write_text(json.dumps(models, ensure_ascii=False, indent=2), encoding="utf-8")
    coeff_payload = {
        "model_path": wsl_to_windows_path(model_path),
        "preset_key": calibration["preset_key"],
        "preset_coeffs": calibration["preset_coeffs"],
        "calibration": calibration,
        "test_start": coeff_start.isoformat(),
        "backtest_end": coeff_end.isoformat(),
        "sector_count": len(models),
        "daily_coefficients": daily_coefficients,
        "stock_sector_map": stock_sector_map,
    }
    coeff_path.write_text(json.dumps(coeff_payload, ensure_ascii=False), encoding="utf-8")

    if args.register_db:
        cur.execute(
            "INSERT INTO model_train_snapshots (config_id, model_path, sector_count, status, metrics_json) VALUES (%s, %s, %s, 'completed', %s) RETURNING snapshot_id",
            (config_id, wsl_to_windows_path(model_path), len(models), json.dumps(metrics_json, ensure_ascii=False)),
        )
        snapshot_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO model_train_jobs (config_id, snapshot_id, status, started_at, completed_at) VALUES (%s, %s, 'completed', NOW(), NOW()) RETURNING job_id",
            (config_id, snapshot_id),
        )
        job_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

    result = {
        "display_name": display_name,
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "model_path": wsl_to_windows_path(model_path),
        "coefficients_path": wsl_to_windows_path(coeff_path),
        "model_path_wsl": str(model_path),
        "coefficients_path_wsl": str(coeff_path),
        "metrics": metrics_json,
        "skipped": skipped,
    }
    result_path = model_dir / "training_result.json"
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    conn.close()
    print(json.dumps({**result, "metrics": "<omitted>", "skipped": skipped[:5]}, ensure_ascii=False, indent=2))
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train additive horizon-aware HMM v2")
    parser.add_argument("--train-start", default="2022-09-01")
    parser.add_argument("--train-end", default="2025-05-30")
    parser.add_argument("--val-start", default="2025-06-02")
    parser.add_argument("--val-end", default="2025-08-29")
    parser.add_argument("--coefficient-start", default="2025-09-01")
    parser.add_argument("--coefficient-end", default="2026-03-03")
    parser.add_argument("--n-states", type=int, default=3)
    parser.add_argument("--covariance-type", default="diag", choices=["diag"])
    parser.add_argument("--n-iter", type=int, default=300)
    parser.add_argument("--min-trading-days", type=int, default=120)
    parser.add_argument("--min-covar", type=float, default=1e-3)
    parser.add_argument("--max-covar", type=float, default=10.0)
    parser.add_argument("--alpha-smooth", type=float, default=0.1)
    parser.add_argument("--min-self-trans", type=float, default=0.75)
    parser.add_argument("--winsor-q", type=float, default=0.01)
    parser.add_argument("--calibration-lambda", type=float, default=0.015)
    parser.add_argument("--coeff-min", type=float, default=0.97)
    parser.add_argument("--coeff-max", type=float, default=1.03)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--display-name", default=None)
    parser.add_argument("--snapshot-date", default=None)
    parser.add_argument("--output-root", default="/mnt/f/Dev/AIstock/backend/data/hmm_models")
    parser.add_argument("--register-db", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser


def main() -> None:
    train(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
