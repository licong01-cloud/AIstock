#!/usr/bin/env python3
"""Precompute sector-HMM coefficients for QE/Paper selection.

The script runs inside WSL and reads a JSON payload from stdin. It supports two
model JSON schemas used by AIstock:
- legacy 4/7-feature sector HMM snapshots with optional zscore_mean/zscore_std;
- horizon-v2 snapshots produced by scripts/hmm_horizon_v2_train.py with
  feature_names + preprocess metadata.

It prints the coefficient JSON to stdout and writes the same JSON to
--output-path when provided. Business failures are fail-fast: empty sector decode
or empty daily coefficients are treated as errors, never as a successful neutral
fallback.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))


def forward_filter_posteriors(hmm: Any, obs: np.ndarray) -> np.ndarray:
    """Causal forward-filter posterior probabilities; no future observations are used."""
    from hmmlearn import _hmmc

    log_frameprob = hmm._compute_log_likelihood(obs)
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
    return np.nan_to_num(
        posteriors,
        nan=1.0 / max(n_states, 1),
        posinf=1.0 / max(n_states, 1),
        neginf=0.0,
    )


def forward_filter_states(hmm: Any, obs: np.ndarray) -> np.ndarray:
    """Causal forward-filter state decoding; no future observations are used."""
    return forward_filter_posteriors(hmm, obs).argmax(axis=1)


def confidence_from_posterior(prob: np.ndarray, scale: float) -> float:
    prob = np.nan_to_num(prob, nan=0.0, posinf=0.0, neginf=0.0)
    total = float(prob.sum())
    if total <= 0:
        return 0.0
    prob = prob / total
    ordered = np.sort(prob)
    margin = float(ordered[-1] - ordered[-2]) if len(ordered) > 1 else 1.0
    return float(np.clip(margin / max(scale, 1e-6), 0.0, 1.0))


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
                [
                    state_stats[str(s)].get(f"{field_prefix}_{horizon}d", 0.0)
                    for s in range(len(prob))
                ],
                dtype=np.float64,
            )
            total += weight * float(np.dot(prob, vals))
        return total
    if method in {"pup", "pup_z", "pup_rank", "additive_pup"}:
        total = 0.0
        for horizon, weight in horizon_weights.items():
            vals = np.asarray(
                [
                    state_stats[str(s)].get(f"pup_{horizon}d", 0.5)
                    for s in range(len(prob))
                ],
                dtype=np.float64,
            )
            total += weight * float(np.dot(prob, vals))
        return 2.0 * (total - 0.5)
    raise ValueError(f"Unknown dynamic HMM method: {method}")


def robust_z_by_date(raw_by_sector: dict[str, float]) -> dict[str, float]:
    values = np.asarray(
        [v for v in raw_by_sector.values() if math.isfinite(v)],
        dtype=np.float64,
    )
    if len(values) < 3:
        return {k: 0.0 for k in raw_by_sector}
    med = float(np.median(values))
    q25, q75 = np.quantile(values, [0.25, 0.75])
    iqr = float(q75 - q25)
    scale = iqr / 1.349 if iqr > 1e-12 else float(np.std(values))
    if scale < 1e-12:
        return {k: 0.0 for k in raw_by_sector}
    return {
        k: float(np.clip((v - med) / scale, -3.0, 3.0))
        for k, v in raw_by_sector.items()
    }


def rank_signal_by_date(raw_by_sector: dict[str, float]) -> dict[str, float]:
    valid = [(k, v) for k, v in raw_by_sector.items() if math.isfinite(v)]
    if len(valid) < 2:
        return {k: 0.0 for k in raw_by_sector}
    ordered = sorted(valid, key=lambda item: item[1])
    denom = max(len(ordered) - 1, 1)
    ranks = {k: (idx / denom) * 2.0 - 1.0 for idx, (k, _) in enumerate(ordered)}
    return {k: float(ranks.get(k, 0.0)) for k in raw_by_sector}


def restore_hmm(info: dict[str, Any]) -> Any:
    """Rebuild GaussianHMM from both legacy and horizon-v2 JSON payloads."""
    from hmmlearn.hmm import GaussianHMM

    n_states = int(info["n_states"])
    cov_type = info.get("covariance_type", "diag")
    hmm = GaussianHMM(n_components=n_states, covariance_type=cov_type)
    hmm.startprob_ = np.asarray(
        info.get("startprob") or np.full(n_states, 1.0 / n_states),
        dtype=np.float64,
    )
    hmm.transmat_ = np.asarray(info["transmat"], dtype=np.float64)
    hmm.means_ = np.asarray(info["means"], dtype=np.float64)
    covars = np.asarray(info["covars"], dtype=np.float64)
    if cov_type == "diag":
        if covars.ndim == 3:
            covars = np.asarray([np.diag(covars[i]) for i in range(covars.shape[0])], dtype=np.float64)
        covars = np.maximum(covars, 1e-6)
    elif cov_type == "full":
        for i in range(covars.shape[0]):
            covars[i] = (covars[i] + covars[i].T) / 2
            covars[i] += np.eye(covars[i].shape[0]) * 1e-6
    hmm.covars_ = covars
    return hmm


def build_legacy_observations(
    rows_by_date: dict[date, dict[str, Any]],
    sorted_dates: list[date],
    csi300_pct: dict[date, float],
    market_vol: dict[date, float],
    rolling_window: int,
    n_features: int,
) -> tuple[np.ndarray, list[date]]:
    rows: list[list[float]] = []
    dates_out: list[date] = []
    win = max(rolling_window, 2)
    for i, td in enumerate(sorted_dates):
        rec = rows_by_date[td]
        csi_pct = csi300_pct.get(td)
        mvol = market_vol.get(td)
        if csi_pct is None or mvol is None:
            continue

        pct = float(rec["sw2_pct_change"] or 0.0)
        vol = float(rec["sw2_vol"] or 0.0)
        amount = float(rec["sw2_amount"] or 0.0)
        mf_net = float(rec["sw2_mf_net_amt"] or 0.0)
        mf_buy_elg = float(rec["sw2_mf_buy_elg_amt"] or 0.0)
        mf_sell_elg = float(rec["sw2_mf_sell_elg_amt"] or 0.0)

        daily_ret = pct / 100.0
        csi_window: list[float] = []
        ret_window: list[float] = []
        for j in range(max(0, i - win + 1), i + 1):
            d2 = sorted_dates[j]
            c2 = csi300_pct.get(d2)
            if c2 is not None and d2 in rows_by_date:
                ret2 = float(rows_by_date[d2]["sw2_pct_change"] or 0.0) / 100.0
                csi_window.append(ret2 - c2 / 100.0)
                ret_window.append(ret2)

        excess_nd = float(np.mean(csi_window)) if csi_window else 0.0
        vol_ratio = vol / mvol if mvol > 0 else 0.0
        limit_up_ratio = 0.0
        volatility = float(np.std(ret_window)) if len(ret_window) > 1 else 0.0
        mf_net_ratio = mf_net / amount if amount > 0 else 0.0
        elg_ratio = (mf_buy_elg - mf_sell_elg) / amount if amount > 0 else 0.0

        if n_features >= 7:
            row = [daily_ret, excess_nd, vol_ratio, limit_up_ratio, volatility, mf_net_ratio, elg_ratio]
        else:
            row = [daily_ret, excess_nd, vol_ratio, limit_up_ratio]
        if any(np.isnan(v) or np.isinf(v) for v in row):
            continue
        rows.append(row)
        dates_out.append(td)
    return np.asarray(rows, dtype=np.float64), dates_out


def build_horizon_v2_observations(
    rows: list[dict[str, Any]],
    csi300_pct: dict[date, float],
    market_vol: dict[date, float],
    preprocess: dict[str, Any],
) -> tuple[np.ndarray, list[date]]:
    from hmm_horizon_v2_train import build_observations, preprocess_apply

    csi300_decimal = {td: pct / 100.0 for td, pct in csi300_pct.items()}
    obs, dates_out, _ = build_observations(rows, csi300_decimal, market_vol)
    if len(obs):
        obs = preprocess_apply(obs, preprocess)
    return obs, dates_out


def parse_stdin() -> dict[str, Any]:
    raw = sys.stdin.read().lstrip("\ufeff")
    if not raw.strip():
        print("ERROR: no stdin JSON payload received", file=sys.stderr)
        sys.exit(1)
    return json.loads(raw)


def main() -> None:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=None)
    args, _ = parser.parse_known_args()

    params = parse_stdin()
    model_path = params["model_path"]
    test_start = params["test_start"]
    backtest_end = params["backtest_end"]
    preset_coeffs = params.get("preset_coeffs") or {
        "trending": 1.05,
        "neutral": 1.00,
        "fading": 0.96,
    }
    preset_key = params.get("preset_key")
    output_trade_date = params.get("output_trade_date")
    as_of_trade_date = params.get("as_of_trade_date")
    config_json = params.get("config_json") if isinstance(params.get("config_json"), dict) else {}

    db_host = params.get("db_host", "127.0.0.1")
    db_port = params.get("db_port", 5432)
    db_name = params.get("db_name", "aistock")
    db_user = params.get("db_user", "postgres")
    db_password = params.get("db_password", "")

    print(f"HMM precompute: model={model_path}", file=sys.stderr)
    print(f"  date range: {test_start} ~ {backtest_end}", file=sys.stderr)
    print(f"  preset: {preset_key}, coeffs: {preset_coeffs}", file=sys.stderr)
    print(f"  DB: {db_user}@{db_host}:{db_port}/{db_name}", file=sys.stderr)

    with open(model_path, "r", encoding="utf-8") as f:
        models = json.load(f)
    if not models:
        print(f"ERROR: empty HMM model file: {model_path}", file=sys.stderr)
        sys.exit(1)
    print(f"  loaded {len(models)} sector models", file=sys.stderr)

    first = next(iter(models.values()))
    n_features = len(first.get("means", [[]])[0]) if first.get("means") else 4
    rolling_window = int(first.get("rolling_window", 5))
    has_horizon_v2_features = bool(first.get("feature_names") and first.get("preprocess"))
    uses_dynamic_coefficients = bool(
        first.get("state_validation_stats") and not first.get("state_labels")
    )
    dynamic_method = str(config_json.get("method") or params.get("dynamic_method") or "").strip()
    horizon_weights = {
        int(k): float(v)
        for k, v in (config_json.get("horizon_weights") or {}).items()
    }
    if uses_dynamic_coefficients:
        if dynamic_method not in {"er", "er_winsor", "er_median", "pup", "pup_z", "pup_rank", "additive_pup"}:
            print(
                "ERROR: dynamic HMM model requires explicit supported config_json.method",
                file=sys.stderr,
            )
            sys.exit(1)
        if not horizon_weights:
            print(
                "ERROR: dynamic HMM model requires config_json.horizon_weights",
                file=sys.stderr,
            )
            sys.exit(1)
        missing_dynamic_keys = [
            key
            for key in ("coefficient_lambda", "coefficient_bounds", "confidence_scale")
            if key not in config_json
        ]
        if missing_dynamic_keys:
            print(
                "ERROR: dynamic HMM model requires config_json keys: "
                + ",".join(missing_dynamic_keys),
                file=sys.stderr,
            )
            sys.exit(1)
    coefficient_lambda = float(config_json.get("coefficient_lambda", 0.0))
    coefficient_bounds = config_json.get("coefficient_bounds") or [0.0, float("inf")]
    if uses_dynamic_coefficients and (
        not isinstance(coefficient_bounds, list) or len(coefficient_bounds) != 2
    ):
        print("ERROR: dynamic HMM model requires two coefficient_bounds values", file=sys.stderr)
        sys.exit(1)
    coeff_min = float(coefficient_bounds[0])
    coeff_max = float(coefficient_bounds[1])
    confidence_scale = float(config_json.get("confidence_scale", 1.0))
    neutral_band = float(config_json.get("neutral_band", 0.0))
    confidence_floor = float(config_json.get("confidence_floor", 0.0))
    has_zscore = "zscore_mean" in first
    zscore_mean = np.asarray(first["zscore_mean"], dtype=np.float64) if has_zscore else None
    zscore_std = np.asarray(first["zscore_std"], dtype=np.float64) if has_zscore else None
    print(
        f"  features={n_features}, rolling_window={rolling_window}, "
        f"zscore={has_zscore}, horizon_v2_schema={has_horizon_v2_features}, "
        f"dynamic_coefficients={uses_dynamic_coefficients}",
        file=sys.stderr,
    )

    hmm_objs: dict[str, tuple[Any, dict[str, str] | None, dict[str, Any]]] = {}
    for code, info in models.items():
        try:
            labels = info.get("state_labels")
            if uses_dynamic_coefficients:
                stats = info.get("state_validation_stats")
                if not isinstance(stats, dict) or not stats:
                    raise KeyError("state_validation_stats")
                labels = None
            elif not isinstance(labels, dict) or not labels:
                raise KeyError("state_labels")
            hmm_objs[code] = (restore_hmm(info), labels, info)
        except Exception as exc:
            print(f"  WARNING: failed to restore HMM {code}: {exc}", file=sys.stderr)
    if not hmm_objs:
        print("ERROR: all HMM models failed to restore", file=sys.stderr)
        sys.exit(1)
    print(f"  restored {len(hmm_objs)}/{len(models)} HMM models", file=sys.stderr)

    start_d = date.fromisoformat(test_start)
    end_d = date.fromisoformat(backtest_end)
    history_start = start_d - timedelta(days=int(3.0 * 365 + 30))

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)
    print("  loading DB data...", file=sys.stderr)

    cur.execute(
        """
        SELECT DISTINCT ON (m.l2_code, sd.trade_date)
               m.l2_code AS sector_code, m.l2_name AS sector_name, sd.trade_date,
               sd.sw2_pct_change, sd.sw2_vol, sd.sw2_amount,
               sd.sw2_mf_net_amt, sd.sw2_mf_buy_elg_amt, sd.sw2_mf_sell_elg_amt,
               sd.ts_code
        FROM market.sector_data sd
        JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
        WHERE sd.trade_date BETWEEN %s AND %s
          AND m.in_date <= sd.trade_date
          AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
        ORDER BY m.l2_code, sd.trade_date, sd.ts_code
        """,
        (history_start, end_d),
    )
    all_sector_rows = cur.fetchall()

    sector_data: dict[str, dict[date, dict[str, Any]]] = {}
    sector_rows: dict[str, list[dict[str, Any]]] = {}
    for row in all_sector_rows:
        code = str(row["sector_code"])
        td = row["trade_date"]
        sector_data.setdefault(code, {})[td] = row
        sector_rows.setdefault(code, []).append(
            {
                "trade_date": td,
                "l2_name": row.get("sector_name") or code,
                "pct_change": float(row["sw2_pct_change"] or 0.0),
                "vol": float(row["sw2_vol"] or 0.0),
                "amount": float(row["sw2_amount"] or 0.0),
                "mf_net_amt": float(row["sw2_mf_net_amt"] or 0.0),
                "mf_buy_elg_amt": float(row["sw2_mf_buy_elg_amt"] or 0.0),
                "mf_sell_elg_amt": float(row["sw2_mf_sell_elg_amt"] or 0.0),
            }
        )

    cur.execute(
        """
        SELECT trade_date, pct_chg FROM market.index_daily
        WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """,
        (history_start, end_d),
    )
    csi300 = {r["trade_date"]: float(r["pct_chg"] or 0.0) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT trade_date, SUM(vol) AS total_vol FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date ORDER BY trade_date
        """,
        (history_start, end_d),
    )
    market_vol = {r["trade_date"]: float(r["total_vol"] or 0.0) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT ts_code, l2_code FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        """,
        (end_d, start_d),
    )
    stock_sector_map = {
        r["ts_code"]: r["l2_code"]
        for r in cur.fetchall()
        if r["ts_code"] and r["l2_code"]
    }
    cur.close()
    conn.close()

    print(
        f"  loaded sectors={len(sector_data)}, CSI300={len(csi300)}, "
        f"market_vol={len(market_vol)}, stock_map={len(stock_sector_map)}",
        file=sys.stderr,
    )
    if not stock_sector_map:
        print("ERROR: empty stock-sector map", file=sys.stderr)
        sys.exit(1)

    print("  decoding sector states...", file=sys.stderr)
    sector_date_labels: dict[str, dict[str, str]] = {}
    dynamic_signal_by_date: dict[str, dict[str, float]] = {}
    dynamic_confidence_by_date: dict[str, dict[str, float]] = {}
    for idx, (code, (hmm, labels, info)) in enumerate(hmm_objs.items()):
        if code not in sector_data:
            continue
        if has_horizon_v2_features and info.get("preprocess"):
            obs, dates_out = build_horizon_v2_observations(
                sector_rows[code], csi300, market_vol, info["preprocess"]
            )
        else:
            sorted_dates = sorted(sector_data[code].keys())
            obs, dates_out = build_legacy_observations(
                sector_data[code], sorted_dates, csi300, market_vol, rolling_window, n_features
            )
            if len(obs) and zscore_mean is not None:
                obs = (obs - zscore_mean) / zscore_std

        if len(obs) < 20:
            continue
        expected_features = int(hmm.means_.shape[1])
        if obs.shape[1] != expected_features:
            print(
                f"  WARNING: feature dimension mismatch {code}: obs={obs.shape[1]}, model={expected_features}",
                file=sys.stderr,
            )
            continue
        try:
            posteriors = forward_filter_posteriors(hmm, obs)
        except Exception as exc:
            print(f"  WARNING: forward filter failed {code}: {exc}", file=sys.stderr)
            continue

        if uses_dynamic_coefficients:
            state_stats = info.get("state_validation_stats")
            if not isinstance(state_stats, dict) or not state_stats:
                print(f"  WARNING: missing state_validation_stats {code}", file=sys.stderr)
                continue
            for i, td in enumerate(dates_out):
                if start_d <= td <= end_d:
                    prob = posteriors[i]
                    try:
                        raw_signal = utility_from_state_stats(
                            prob,
                            state_stats,
                            dynamic_method,
                            horizon_weights,
                        )
                    except Exception as exc:
                        print(f"  WARNING: dynamic signal failed {code}: {exc}", file=sys.stderr)
                        continue
                    if not math.isfinite(raw_signal):
                        raw_signal = 0.0
                    d = td.isoformat()
                    dynamic_signal_by_date.setdefault(d, {})[code] = float(raw_signal)
                    dynamic_confidence_by_date.setdefault(d, {})[code] = confidence_from_posterior(
                        prob,
                        confidence_scale,
                    )
        else:
            assert labels is not None
            states = posteriors.argmax(axis=1)
            by_date = {}
            for i, td in enumerate(dates_out):
                if start_d <= td <= end_d:
                    by_date[td.isoformat()] = labels.get(str(int(states[i])), "neutral")
            if by_date:
                sector_date_labels[code] = by_date
        if (idx + 1) % 20 == 0:
            print(f"  processed {idx + 1}/{len(hmm_objs)} sectors", file=sys.stderr)

    daily_coefficients: dict[str, dict[str, float]] = {}
    if uses_dynamic_coefficients:
        dynamic_sector_count = len({code for by_sector in dynamic_signal_by_date.values() for code in by_sector})
        print(f"  decoded dynamic sectors={dynamic_sector_count}", file=sys.stderr)
        if not dynamic_signal_by_date:
            print("ERROR: no dynamic HMM signals decoded; refusing empty coefficient output", file=sys.stderr)
            sys.exit(1)
        for d in sorted(dynamic_signal_by_date):
            raw_map = dynamic_signal_by_date[d]
            if dynamic_method in {"er", "er_winsor", "er_median", "pup_z"}:
                normalized_map = robust_z_by_date(raw_map)
            elif dynamic_method == "pup_rank":
                normalized_map = rank_signal_by_date(raw_map)
            else:
                normalized_map = raw_map
            daily_coefficients[d] = {}
            for code, raw_signal in raw_map.items():
                confidence = dynamic_confidence_by_date.get(d, {}).get(code, 0.0)
                if dynamic_method in {"er", "er_winsor", "er_median", "pup_z", "pup_rank"}:
                    normalized_signal = normalized_map.get(code, 0.0)
                    if not math.isfinite(normalized_signal):
                        normalized_signal = 0.0
                    coeff = 1.0 + coefficient_lambda * confidence * normalized_signal
                elif dynamic_method == "pup":
                    normalized_signal = float(np.clip(raw_signal, -1.0, 1.0))
                    if not math.isfinite(normalized_signal):
                        normalized_signal = 0.0
                    coeff = 1.0 + coefficient_lambda * confidence * normalized_signal
                else:
                    normalized_signal = float(np.clip(raw_signal, -1.0, 1.0))
                    if not math.isfinite(normalized_signal):
                        normalized_signal = 0.0
                    coeff = 1.0
                if confidence < confidence_floor or abs(normalized_signal) < neutral_band:
                    coeff = 1.0
                coeff = float(np.clip(coeff, coeff_min, coeff_max))
                if not math.isfinite(coeff):
                    print(f"  WARNING: non-finite dynamic coefficient {code} {d}", file=sys.stderr)
                    continue
                daily_coefficients[d][code] = round(coeff, 8)
    else:
        print(f"  decoded sectors={len(sector_date_labels)}", file=sys.stderr)
        if not sector_date_labels:
            print("ERROR: no HMM sectors decoded; refusing empty coefficient output", file=sys.stderr)
            sys.exit(1)

        all_dates = sorted({d for labels in sector_date_labels.values() for d in labels})
        for d in all_dates:
            daily_coefficients[d] = {
                code: float(preset_coeffs.get(labels.get(d, "neutral"), 1.0))
                for code, labels in sector_date_labels.items()
            }
    if not daily_coefficients:
        print("ERROR: no daily HMM coefficients generated", file=sys.stderr)
        sys.exit(1)

    if output_trade_date:
        source_trade_date = as_of_trade_date or backtest_end
        source_coefficients = daily_coefficients.get(source_trade_date)
        if not isinstance(source_coefficients, dict) or not source_coefficients:
            available = sorted(daily_coefficients.keys())[-5:]
            print(
                "ERROR: coefficients missing for "
                f"as_of_trade_date={source_trade_date}; available_tail={available}",
                file=sys.stderr,
            )
            sys.exit(1)
        daily_coefficients = {str(output_trade_date): source_coefficients}

    result: dict[str, Any] = {
        "model_path": model_path,
        "preset_key": preset_key,
        "preset_coeffs": preset_coeffs,
        "test_start": test_start,
        "backtest_end": backtest_end,
        "sector_count": (
            len({code for by_sector in dynamic_signal_by_date.values() for code in by_sector})
            if uses_dynamic_coefficients
            else len(sector_date_labels)
        ),
        "dynamic_coefficients": uses_dynamic_coefficients,
        "daily_coefficients": daily_coefficients,
        "stock_sector_map": stock_sector_map,
    }
    if output_trade_date:
        result.update(
            {
                "generation_mode": params.get("generation_mode") or "daily_asof_prediction_v1",
                "as_of_trade_date": as_of_trade_date or backtest_end,
                "effective_trade_date": str(output_trade_date),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "snapshot_id": params.get("snapshot_id"),
                "config_id": params.get("config_id"),
                "input_data_max_dates": params.get("input_data_max_dates"),
            }
        )

    result_json = json.dumps(result, ensure_ascii=False)
    print(result_json)
    if args.output_path:
        os.makedirs(os.path.dirname(args.output_path) or ".", exist_ok=True)
        with open(args.output_path, "w", encoding="utf-8") as f:
            f.write(result_json)
        print(f"  wrote result: {args.output_path}", file=sys.stderr)
    print("HMM precompute completed", file=sys.stderr)


if __name__ == "__main__":
    main()
