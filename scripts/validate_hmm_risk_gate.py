#!/usr/bin/env python3
"""Validate HMM risk gate effectiveness by comparing forward returns of blocked vs allowed stocks.

Reads a precomputed risk gate artifact and a QE pred.pkl to simulate which stocks
would have been blocked, then computes forward returns to verify the gate adds value.

Usage (WSL):
    python scripts/validate_hmm_risk_gate.py \
        --gate-artifact backend/data/hmm_models/.../hmm_risk_gate_*.json \
        --pred-pkl rdagent_assets/qe_experiments/qe_20260502_131502_9b54/Loop1/pred.pkl \
        --top-k 50 \
        --output-dir .codex_tmp/hmm_risk_gate_validation/

Acceptance criteria:
  - Blocked stocks avg 10D forward return < allowed stocks avg 10D forward return
  - Annual return degradation < 0.5%
  - Max drawdown improvement > 1%
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))


def load_gate_artifact(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        artifact = json.load(f)
    if artifact.get("artifact_type") != "hmm_risk_gate_v1":
        raise ValueError(f"Invalid artifact type: {artifact.get('artifact_type')}")
    return artifact


def load_pred_pkl(path: str) -> pd.DataFrame:
    pred = pd.read_pickle(path)
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    if pred.index.nlevels == 2:
        pred.index.names = ["datetime", "instrument"]
    return pred


def load_forward_returns(db_host: str, db_port: int, db_name: str, db_user: str,
                         db_password: str, start_d: date, end_d: date) -> pd.DataFrame:
    """Load daily close prices and compute forward returns."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password,
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)

    extended_end = end_d + timedelta(days=40)
    cur.execute(
        """
        SELECT ts_code, trade_date, close_li
        FROM market.kline_daily_raw
        WHERE trade_date BETWEEN %s AND %s AND close_li IS NOT NULL
        ORDER BY ts_code, trade_date
        """,
        (start_d, extended_end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No price data loaded")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.set_index(["ts_code", "trade_date"]).sort_index()

    results = []
    for ts_code, group in df.groupby(level=0):
        group = group.droplevel(0)
        close = group["close_li"]
        for horizon in [1, 3, 5, 10, 20]:
            fwd = close.shift(-horizon) / close - 1
            fwd.name = f"fwd_{horizon}d"
            group = group.join(fwd)
        group["ts_code"] = ts_code
        results.append(group.reset_index())

    return pd.concat(results, ignore_index=True)


def simulate_gate(
    pred: pd.DataFrame,
    artifact: dict[str, Any],
    fwd_returns: pd.DataFrame,
    top_k: int,
) -> dict[str, Any]:
    """Simulate risk gate on pred scores and compute blocked vs allowed forward returns."""
    daily_gates = artifact["daily_gates"]
    stock_sector_map = artifact["stock_sector_map"]

    results_by_day = []

    for trade_date, group in pred.groupby(level=0):
        if isinstance(trade_date, pd.Timestamp):
            d_iso = trade_date.strftime("%Y-%m-%d")
        else:
            d_iso = str(trade_date)[:10]

        gates_today = daily_gates.get(d_iso)
        if gates_today is None:
            continue

        scores = group.droplevel(0)
        if "score" in scores.columns:
            scores = scores["score"].sort_values(ascending=False)
        else:
            scores = scores.iloc[:, 0].sort_values(ascending=False)

        top_candidates = scores.head(top_k * 3).index.tolist()

        blocked_symbols = []
        allowed_symbols = []
        for sym in top_candidates:
            sector = stock_sector_map.get(sym)
            if sector and gates_today.get(sector, {}).get("blocked", False):
                blocked_symbols.append(sym)
            else:
                allowed_symbols.append(sym)

        raw_top_k = scores.head(top_k).index.tolist()
        gated_top_k = [s for s in raw_top_k if s not in blocked_symbols]
        replacements = [s for s in allowed_symbols if s not in gated_top_k][:top_k - len(gated_top_k)]
        gated_top_k.extend(replacements)

        blocked_in_raw_top_k = [s for s in raw_top_k if s in blocked_symbols]

        results_by_day.append({
            "trade_date": d_iso,
            "raw_top_k_count": len(raw_top_k),
            "blocked_count": len(blocked_in_raw_top_k),
            "blocked_symbols": blocked_in_raw_top_k,
            "gated_top_k": gated_top_k[:top_k],
            "sectors_blocked_today": sum(1 for g in gates_today.values() if g.get("blocked")),
        })

    print(f"  simulated {len(results_by_day)} trading days", file=sys.stderr)

    blocked_fwd = {h: [] for h in [1, 3, 5, 10, 20]}
    allowed_fwd = {h: [] for h in [1, 3, 5, 10, 20]}
    raw_portfolio_returns = []
    gated_portfolio_returns = []

    fwd_lookup = fwd_returns.set_index(["ts_code", "trade_date"])

    for day_info in results_by_day:
        d = pd.Timestamp(day_info["trade_date"])

        for sym in day_info["blocked_symbols"]:
            if (sym, d) in fwd_lookup.index:
                row = fwd_lookup.loc[(sym, d)]
                for h in [1, 3, 5, 10, 20]:
                    val = row.get(f"fwd_{h}d")
                    if pd.notna(val):
                        blocked_fwd[h].append(float(val))

        for sym in day_info["gated_top_k"]:
            if (sym, d) in fwd_lookup.index:
                row = fwd_lookup.loc[(sym, d)]
                for h in [1, 3, 5, 10, 20]:
                    val = row.get(f"fwd_{h}d")
                    if pd.notna(val):
                        allowed_fwd[h].append(float(val))

        raw_rets = []
        for sym in day_info["gated_top_k"][:top_k]:
            if (sym, d) in fwd_lookup.index:
                val = fwd_lookup.loc[(sym, d)].get("fwd_5d")
                if pd.notna(val):
                    raw_rets.append(float(val))
        if raw_rets:
            gated_portfolio_returns.append(np.mean(raw_rets))

    summary = {
        "total_days": len(results_by_day),
        "days_with_blocks": sum(1 for d in results_by_day if d["blocked_count"] > 0),
        "avg_blocked_per_day": np.mean([d["blocked_count"] for d in results_by_day]) if results_by_day else 0,
        "total_blocked_instances": sum(d["blocked_count"] for d in results_by_day),
    }

    for h in [1, 3, 5, 10, 20]:
        b_mean = np.mean(blocked_fwd[h]) if blocked_fwd[h] else float("nan")
        a_mean = np.mean(allowed_fwd[h]) if allowed_fwd[h] else float("nan")
        summary[f"blocked_avg_fwd_{h}d"] = round(b_mean * 100, 4) if np.isfinite(b_mean) else None
        summary[f"allowed_avg_fwd_{h}d"] = round(a_mean * 100, 4) if np.isfinite(a_mean) else None
        spread = (a_mean - b_mean) if (np.isfinite(a_mean) and np.isfinite(b_mean)) else float("nan")
        summary[f"spread_fwd_{h}d_pct"] = round(spread * 100, 4) if np.isfinite(spread) else None

    summary["blocked_sample_count"] = len(blocked_fwd[10])
    summary["allowed_sample_count"] = len(allowed_fwd[10])

    if blocked_fwd[10] and allowed_fwd[10]:
        blocked_arr = np.array(blocked_fwd[10])
        allowed_arr = np.array(allowed_fwd[10])
        summary["blocked_win_rate_10d"] = round(float(np.mean(blocked_arr > 0)) * 100, 2)
        summary["allowed_win_rate_10d"] = round(float(np.mean(allowed_arr > 0)) * 100, 2)

    return {
        "summary": summary,
        "daily_details": results_by_day,
    }


def main() -> None:
    import os

    parser = argparse.ArgumentParser(description="Validate HMM risk gate effectiveness")
    parser.add_argument("--gate-artifact", required=True)
    parser.add_argument("--pred-pkl", required=True)
    parser.add_argument("--top-k", type=int, default=50)
    parser.add_argument("--output-dir", default=".codex_tmp/hmm_risk_gate_validation")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="aistock")
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-password", default=None)
    args = parser.parse_args()

    db_password = args.db_password or os.environ.get("TDX_DB_PASSWORD", "")
    if not db_password:
        print("ERROR: DB password required (--db-password or TDX_DB_PASSWORD env)", file=sys.stderr)
        sys.exit(1)

    print("Loading gate artifact...", file=sys.stderr)
    artifact = load_gate_artifact(args.gate_artifact)
    print(f"  artifact: {artifact['sector_count']} sectors, "
          f"{artifact['summary']['total_days']} days, "
          f"threshold={artifact['gate_config']['confidence_threshold']}", file=sys.stderr)

    print("Loading pred.pkl...", file=sys.stderr)
    pred = load_pred_pkl(args.pred_pkl)
    print(f"  pred shape: {pred.shape}", file=sys.stderr)

    dates_in_pred = pred.index.get_level_values(0).unique()
    start_d = dates_in_pred.min()
    end_d = dates_in_pred.max()
    if isinstance(start_d, pd.Timestamp):
        start_d = start_d.date()
        end_d = end_d.date()

    print(f"Loading forward returns ({start_d} ~ {end_d})...", file=sys.stderr)
    fwd_returns = load_forward_returns(
        args.db_host, args.db_port, args.db_name, args.db_user, db_password,
        start_d, end_d,
    )
    print(f"  loaded {len(fwd_returns)} price rows", file=sys.stderr)

    print("Simulating risk gate...", file=sys.stderr)
    results = simulate_gate(pred, artifact, fwd_returns, args.top_k)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / "validation_summary.json"
    summary_path.write_text(
        json.dumps(results["summary"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\n" + "=" * 60, file=sys.stderr)
    print("VALIDATION RESULTS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    s = results["summary"]
    print(f"  Total days: {s['total_days']}", file=sys.stderr)
    print(f"  Days with blocks: {s['days_with_blocks']} ({s['days_with_blocks']/max(s['total_days'],1)*100:.1f}%)", file=sys.stderr)
    print(f"  Avg blocked per day: {s['avg_blocked_per_day']:.2f}", file=sys.stderr)
    print(f"  Total blocked instances: {s['total_blocked_instances']}", file=sys.stderr)
    print(f"  Blocked sample count: {s['blocked_sample_count']}", file=sys.stderr)
    print(f"  Allowed sample count: {s['allowed_sample_count']}", file=sys.stderr)
    print("", file=sys.stderr)
    print("  Forward Return Comparison (blocked vs allowed):", file=sys.stderr)
    for h in [1, 3, 5, 10, 20]:
        b = s.get(f"blocked_avg_fwd_{h}d")
        a = s.get(f"allowed_avg_fwd_{h}d")
        sp = s.get(f"spread_fwd_{h}d_pct")
        b_str = f"{b:.3f}%" if b is not None else "N/A"
        a_str = f"{a:.3f}%" if a is not None else "N/A"
        sp_str = f"{sp:.3f}%" if sp is not None else "N/A"
        verdict = "PASS" if (sp is not None and sp > 0) else "FAIL"
        print(f"    {h:2d}D: blocked={b_str:>8s}  allowed={a_str:>8s}  spread={sp_str:>8s}  [{verdict}]", file=sys.stderr)

    if s.get("blocked_win_rate_10d") is not None:
        print(f"\n  10D win rate: blocked={s['blocked_win_rate_10d']:.1f}%  allowed={s['allowed_win_rate_10d']:.1f}%", file=sys.stderr)

    overall_pass = all(
        s.get(f"spread_fwd_{h}d_pct") is not None and s[f"spread_fwd_{h}d_pct"] > 0
        for h in [5, 10, 20]
    )
    print(f"\n  OVERALL: {'PASS' if overall_pass else 'FAIL'} (5D/10D/20D spreads all positive)", file=sys.stderr)
    print(f"\n  Results saved to: {out_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
