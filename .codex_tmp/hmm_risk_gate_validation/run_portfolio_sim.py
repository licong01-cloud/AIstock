"""Full portfolio simulation: compare no-HMM vs risk-gate using pred.pkl and real prices."""
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys

pred_path = "docs/analysis/tmp/qe_20260502_193154_17a2_hmm_diag/qe_20260502_193154_17a2/artifacts/L1_qe_20260502_193154_17a2_Loop1/pred.pkl"
pred = pd.read_pickle(pred_path)
if isinstance(pred, pd.Series):
    pred = pred.to_frame("score")
pred.index.names = ["datetime", "instrument"]

with open(".codex_tmp/hmm_risk_gate_validation/hmm_risk_gate_duration_5d.json") as f:
    artifact = json.load(f)

daily_gates = artifact["daily_gates"]
stock_sector_map = artifact["stock_sector_map"]

conn = psycopg2.connect(
    host=os.environ.get("TDX_DB_HOST", "127.0.0.1").strip(),
    port=int(os.environ.get("TDX_DB_PORT", "5432").strip()),
    dbname=os.environ.get("TDX_DB_NAME", "aistock").strip(),
    user=os.environ.get("TDX_DB_USER", "postgres").strip(),
    password=os.environ["TDX_DB_PASSWORD"].strip(),
)
conn.autocommit = True
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute("""
    SELECT ts_code, trade_date, close_li
    FROM market.kline_daily_raw
    WHERE trade_date BETWEEN '2024-07-01' AND '2026-05-10' AND close_li IS NOT NULL
    ORDER BY ts_code, trade_date
""")
prices_raw = cur.fetchall()
cur.close()
conn.close()

prices_df = pd.DataFrame(prices_raw)
prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"])
prices_pivot = prices_df.pivot(index="trade_date", columns="ts_code", values="close_li")

trade_dates = sorted(pred.index.get_level_values(0).unique())

TOP_K = 50
REBAL_DAYS = 5

def get_top_k(pred_day, blocked_symbols=None):
    if isinstance(pred_day.index, pd.MultiIndex):
        scores = pred_day.droplevel(0)["score"].sort_values(ascending=False)
    else:
        scores = pred_day["score"].sort_values(ascending=False)
    if blocked_symbols:
        scores = scores.drop(blocked_symbols, errors="ignore")
    return scores.head(TOP_K).index.tolist()

nav_no_gate = [1.0]
nav_with_gate = [1.0]
rebal_dates = trade_dates[::REBAL_DAYS]

holdings_with_gate = []
blocked_total = 0
rebal_count = 0

for i, rebal_date in enumerate(rebal_dates[:-1]):
    next_rebal = rebal_dates[i + 1] if i + 1 < len(rebal_dates) else trade_dates[-1]

    pred_day = pred.loc[rebal_date]
    d_iso = rebal_date.strftime("%Y-%m-%d") if hasattr(rebal_date, "strftime") else str(rebal_date)[:10]

    top_k_no_gate = get_top_k(pred_day)

    gates_today = daily_gates.get(d_iso, {})
    blocked_sectors = {s for s, g in gates_today.items() if g.get("blocked", False)}
    blocked_symbols = []
    if isinstance(pred_day.index, pd.MultiIndex):
        all_scores = pred_day.droplevel(0)["score"].sort_values(ascending=False)
    else:
        all_scores = pred_day["score"].sort_values(ascending=False)
    all_candidates = all_scores.head(TOP_K * 3).index
    for sym in all_candidates:
        sector = stock_sector_map.get(sym)
        if sector and sector in blocked_sectors and sym not in set(holdings_with_gate):
            blocked_symbols.append(sym)

    top_k_with_gate = get_top_k(pred_day, blocked_symbols)
    blocked_in_top50 = len(set(top_k_no_gate) - set(top_k_with_gate))
    blocked_total += blocked_in_top50
    rebal_count += 1

    rebal_ts = pd.Timestamp(rebal_date)
    next_ts = pd.Timestamp(next_rebal)

    if rebal_ts in prices_pivot.index and next_ts in prices_pivot.index:
        p_start = prices_pivot.loc[rebal_ts]
        p_end = prices_pivot.loc[next_ts]

        rets_no_gate = []
        for sym in top_k_no_gate:
            if sym in p_start.index and sym in p_end.index:
                s, e = p_start[sym], p_end[sym]
                if pd.notna(s) and pd.notna(e) and s > 0:
                    rets_no_gate.append(e / s - 1)

        rets_with_gate = []
        for sym in top_k_with_gate:
            if sym in p_start.index and sym in p_end.index:
                s, e = p_start[sym], p_end[sym]
                if pd.notna(s) and pd.notna(e) and s > 0:
                    rets_with_gate.append(e / s - 1)

        ret_no = np.mean(rets_no_gate) if rets_no_gate else 0.0
        ret_gate = np.mean(rets_with_gate) if rets_with_gate else 0.0

        nav_no_gate.append(nav_no_gate[-1] * (1 + ret_no))
        nav_with_gate.append(nav_with_gate[-1] * (1 + ret_gate))

    holdings_with_gate = top_k_with_gate

nav_no = np.array(nav_no_gate)
nav_gate = np.array(nav_with_gate)

periods = len(nav_no) - 1
ann_factor = 252.0 / (REBAL_DAYS * periods) if periods > 0 else 1.0

total_ret_no = nav_no[-1] / nav_no[0] - 1
total_ret_gate = nav_gate[-1] / nav_gate[0] - 1
ann_ret_no = (1 + total_ret_no) ** ann_factor - 1
ann_ret_gate = (1 + total_ret_gate) ** ann_factor - 1

def max_drawdown(nav):
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    return float(dd.min())

mdd_no = max_drawdown(nav_no)
mdd_gate = max_drawdown(nav_gate)

def sharpe(nav, rf=0.0):
    rets = np.diff(nav) / nav[:-1]
    if len(rets) < 2:
        return 0.0
    excess = rets - rf / 252
    return float(np.mean(excess) / max(np.std(excess, ddof=1), 1e-10) * np.sqrt(252 / REBAL_DAYS))

sharpe_no = sharpe(nav_no)
sharpe_gate = sharpe(nav_gate)

print("=" * 70)
print("PORTFOLIO SIMULATION: no-Gate vs Risk Gate (5D rebalance, Top50 EW)")
print("=" * 70)
print(f"  Rebalance periods: {rebal_count}")
print(f"  Avg blocked from Top50 per rebal: {blocked_total/max(rebal_count,1):.2f}")
print()
header = f"  {'Metric':<25} {'No-Gate':>12} {'Risk-Gate':>12} {'Delta':>12}"
print(header)
print(f"  {'-'*61}")
print(f"  {'Total Return':<25} {total_ret_no*100:>10.2f}%  {total_ret_gate*100:>10.2f}%  {(total_ret_gate-total_ret_no)*100:>+10.2f}%")
print(f"  {'Annualized Return':<25} {ann_ret_no*100:>10.2f}%  {ann_ret_gate*100:>10.2f}%  {(ann_ret_gate-ann_ret_no)*100:>+10.2f}%")
print(f"  {'Max Drawdown':<25} {mdd_no*100:>10.2f}%  {mdd_gate*100:>10.2f}%  {(mdd_gate-mdd_no)*100:>+10.2f}%")
print(f"  {'Sharpe Ratio':<25} {sharpe_no:>12.4f} {sharpe_gate:>12.4f} {sharpe_gate-sharpe_no:>+12.4f}")
print(f"  {'Final NAV':<25} {nav_no[-1]:>12.4f} {nav_gate[-1]:>12.4f} {nav_gate[-1]-nav_no[-1]:>+12.4f}")
