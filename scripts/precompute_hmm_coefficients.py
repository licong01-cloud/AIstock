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


def forward_filter_states(hmm: Any, obs: np.ndarray) -> np.ndarray:
    """Causal forward-filter state decoding; no future observations are used."""
    from hmmlearn import _hmmc
    from hmmlearn.utils import normalize as hmm_normalize

    log_startprob = np.log(hmm.startprob_ + 1e-300)
    log_transmat = np.log(hmm.transmat_ + 1e-300)
    log_frameprob = hmm._compute_log_likelihood(obs)
    _, fwd_lattice = _hmmc.forward_log(log_startprob, log_transmat, log_frameprob)
    posteriors = np.exp(fwd_lattice)
    hmm_normalize(posteriors, axis=1)
    return posteriors.argmax(axis=1)


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
    raw = sys.stdin.read()
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
    has_zscore = "zscore_mean" in first
    zscore_mean = np.asarray(first["zscore_mean"], dtype=np.float64) if has_zscore else None
    zscore_std = np.asarray(first["zscore_std"], dtype=np.float64) if has_zscore else None
    print(
        f"  features={n_features}, rolling_window={rolling_window}, "
        f"zscore={has_zscore}, horizon_v2_schema={has_horizon_v2_features}",
        file=sys.stderr,
    )

    hmm_objs: dict[str, tuple[Any, dict[str, str], dict[str, Any]]] = {}
    for code, info in models.items():
        try:
            hmm_objs[code] = (restore_hmm(info), info["state_labels"], info)
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
            states = forward_filter_states(hmm, obs)
        except Exception as exc:
            print(f"  WARNING: forward filter failed {code}: {exc}", file=sys.stderr)
            continue

        by_date = {}
        for i, td in enumerate(dates_out):
            if start_d <= td <= end_d:
                by_date[td.isoformat()] = labels.get(str(int(states[i])), "neutral")
        if by_date:
            sector_date_labels[code] = by_date
        if (idx + 1) % 20 == 0:
            print(f"  processed {idx + 1}/{len(hmm_objs)} sectors", file=sys.stderr)

    print(f"  decoded sectors={len(sector_date_labels)}", file=sys.stderr)
    if not sector_date_labels:
        print("ERROR: no HMM sectors decoded; refusing empty coefficient output", file=sys.stderr)
        sys.exit(1)

    all_dates = sorted({d for labels in sector_date_labels.values() for d in labels})
    daily_coefficients: dict[str, dict[str, float]] = {}
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
        "sector_count": len(sector_date_labels),
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
