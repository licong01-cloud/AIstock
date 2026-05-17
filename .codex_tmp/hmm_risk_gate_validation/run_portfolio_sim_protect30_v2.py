"""Portfolio sim variant: only gate rank 31-50 (protect Top30 alpha)."""
import json, numpy as np, pandas as pd, psycopg2, os
from psycopg2.extras import RealDictCursor

pred = pd.read_pickle("rdagent_assets/strategy_package_runtime/_qe_node_sources/qe_20260508_060509_1268_L2/rdagent-node1/qe_20260508_060509_1268/Loop2/mlruns/141010509364948396/0c290fd68d924a67a3e8f3605fa0782a/artifacts/pred.pkl")
if isinstance(pred, pd.Series):
    pred = pred.to_frame("score")
pred.index.names = ["datetime", "instrument"]

with open(".codex_tmp/hmm_risk_gate_validation/hmm_risk_gate_duration_5d.json") as f:
    artifact = json.load(f)
daily_gates = artifact["daily_gates"]
stock_sector_map = artifact["stock_sector_map"]

conn = psycopg2.connect(host=os.environ.get("TDX_DB_HOST","127.0.0.1").strip(), port=int(os.environ.get("TDX_DB_PORT","5432").strip()), dbname=os.environ.get("TDX_DB_NAME","aistock").strip(), user=os.environ.get("TDX_DB_USER","postgres").strip(), password=os.environ["TDX_DB_PASSWORD"].strip())
conn.autocommit = True
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute("SELECT ts_code, trade_date, close_li FROM market.kline_daily_raw WHERE trade_date BETWEEN '2024-07-01' AND '2026-05-10' AND close_li IS NOT NULL ORDER BY ts_code, trade_date")
prices_df = pd.DataFrame(cur.fetchall())
cur.close(); conn.close()
prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"])
prices_pivot = prices_df.pivot(index="trade_date", columns="ts_code", values="close_li")

trade_dates = sorted(pred.index.get_level_values(0).unique())
TOP_K = 50; REBAL_DAYS = 5; PROTECT_TOP = 30
rebal_dates = trade_dates[::REBAL_DAYS]

nav_no_gate = [1.0]; nav_gate_protect30 = [1.0]
holdings_gate = []; blocked_total = 0; rebal_count = 0

for i, rd in enumerate(rebal_dates[:-1]):
    next_rd = rebal_dates[i+1]
    pred_day = pred.loc[rd]
    if isinstance(pred_day.index, pd.MultiIndex):
        scores = pred_day.droplevel(0)["score"].sort_values(ascending=False)
    else:
        scores = pred_day["score"].sort_values(ascending=False)

    d_iso = rd.strftime("%Y-%m-%d") if hasattr(rd, "strftime") else str(rd)[:10]
    top_k_no_gate = scores.head(TOP_K).index.tolist()

    gates_today = daily_gates.get(d_iso, {})
    blocked_sectors = {s for s, g in gates_today.items() if g.get("blocked", False)}

    protected = set(scores.head(PROTECT_TOP).index.tolist())
    blocked_symbols = []
    for sym in scores.head(TOP_K * 3).index:
        if sym in protected:
            continue
        if sym in set(holdings_gate):
            continue
        sector = stock_sector_map.get(sym)
        if sector and sector in blocked_sectors:
            blocked_symbols.append(sym)

    filtered_scores = scores.drop(blocked_symbols, errors="ignore")
    top_k_gate = filtered_scores.head(TOP_K).index.tolist()
    blocked_in_top50 = len(set(top_k_no_gate) - set(top_k_gate))
    blocked_total += blocked_in_top50
    rebal_count += 1

    rebal_ts = pd.Timestamp(rd); next_ts = pd.Timestamp(next_rd)
    if rebal_ts in prices_pivot.index and next_ts in prices_pivot.index:
        p_s = prices_pivot.loc[rebal_ts]; p_e = prices_pivot.loc[next_ts]

        def port_ret(syms):
            rets = []
            for sym in syms:
                if sym in p_s.index and sym in p_e.index:
                    s, e = p_s[sym], p_e[sym]
                    if pd.notna(s) and pd.notna(e) and s > 0:
                        rets.append(e/s - 1)
            return np.mean(rets) if rets else 0.0

        nav_no_gate.append(nav_no_gate[-1] * (1 + port_ret(top_k_no_gate)))
        nav_gate_protect30.append(nav_gate_protect30[-1] * (1 + port_ret(top_k_gate)))

    holdings_gate = top_k_gate

nav_no = np.array(nav_no_gate); nav_g30 = np.array(nav_gate_protect30)

def mdd(nav):
    return float(((nav - np.maximum.accumulate(nav)) / np.maximum.accumulate(nav)).min())

def sharpe_ratio(nav):
    r = np.diff(nav)/nav[:-1]
    return float(np.mean(r)/max(np.std(r,ddof=1),1e-10)*np.sqrt(252/REBAL_DAYS)) if len(r)>1 else 0

periods = len(nav_no)-1
af = 252.0/(REBAL_DAYS*periods) if periods>0 else 1
tr_no = nav_no[-1]/nav_no[0]-1; tr_g = nav_g30[-1]/nav_g30[0]-1
ar_no = (1+tr_no)**af-1; ar_g = (1+tr_g)**af-1

print("="*70)
print("VARIANT: Risk Gate with Top30 Protection (only gate rank 31-50)")
print("="*70)
print("  Rebalance periods: %d" % rebal_count)
print("  Avg blocked from Top50 per rebal: %.2f" % (blocked_total/max(rebal_count,1)))
print()
print("  %-25s %12s %12s %12s" % ("Metric", "No-Gate", "Gate+P30", "Delta"))
print("  " + "-"*61)
print("  %-25s %10.2f%%  %10.2f%%  %+10.2f%%" % ("Total Return", tr_no*100, tr_g*100, (tr_g-tr_no)*100))
print("  %-25s %10.2f%%  %10.2f%%  %+10.2f%%" % ("Annualized Return", ar_no*100, ar_g*100, (ar_g-ar_no)*100))
print("  %-25s %10.2f%%  %10.2f%%  %+10.2f%%" % ("Max Drawdown", mdd(nav_no)*100, mdd(nav_g30)*100, (mdd(nav_g30)-mdd(nav_no))*100))
print("  %-25s %12.4f %12.4f %+12.4f" % ("Sharpe Ratio", sharpe_ratio(nav_no), sharpe_ratio(nav_g30), sharpe_ratio(nav_g30)-sharpe_ratio(nav_no)))
print("  %-25s %12.4f %12.4f %+12.4f" % ("Final NAV", nav_no[-1], nav_g30[-1], nav_g30[-1]-nav_no[-1]))
