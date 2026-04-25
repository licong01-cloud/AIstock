"""检查刚跑完的 10 个因子 v2 字段覆盖情况。"""
import os
import sys
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")

import psycopg2
import psycopg2.extras

FACTORS = [
    "dynamic_flow_volatility_sentiment",
    "Value_PBInv_Momentum_20D",
    "industry_stock_momentum_diff_10d",
    "Liquid_Assets_Market_Cap_Ratio_Momentum",
    "small_order_flow_intensity",
    "Value_PBInv_Momentum_VolAdj_20D",
    "value_pe_inv_momentum",
    "short_term_reversal_5d",
    "dynamic_pe_momentum_rank",
    "dynamic_pe_inv_momentum_turnover_ratio",
]

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"],
    port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("""
        SELECT factor_name, category, factor_dimension,
               ts_info_density, cross_horizon_consistency,
               direction, signal_mechanism, sector_exposure_corr,
               horizon_class, best_horizon, best_horizon_advantage,
               linearity, holding_period_class, data_source_group, update_freq,
               ic_sign_consistency_12m, ic_oos_is_ratio, monthly_ic_trend_slope,
               cluster_id, analyzed_at
        FROM qe_factor_classification
        WHERE factor_name = ANY(%s)
        ORDER BY analyzed_at DESC
    """, (FACTORS,))
    rows = cur.fetchall()

FIELDS = [
    ("category", "str"), ("factor_dimension", "str"),
    ("ts_info_density", "str"), ("cross_horizon_consistency", "num"),
    ("direction", "num"), ("signal_mechanism", "str"), ("sector_exposure_corr", "num"),
    ("horizon_class", "str"), ("best_horizon", "num"), ("best_horizon_advantage", "num"),
    ("linearity", "str"), ("holding_period_class", "str"),
    ("data_source_group", "str"), ("update_freq", "str"),
    ("ic_sign_consistency_12m", "num"), ("ic_oos_is_ratio", "num"),
    ("monthly_ic_trend_slope", "num"), ("cluster_id", "num"),
]

print(f"{'FACTOR':<42}", end="")
for f, _ in FIELDS:
    short = f[:8]
    print(f" {short:<9}", end="")
print()
print("-" * (42 + 10 * len(FIELDS)))

for r in rows:
    print(f"{r['factor_name']:<42}", end="")
    for field, tp in FIELDS:
        v = r.get(field)
        if v is None:
            s = "NULL"
        elif tp == "num":
            try:
                s = f"{float(v):.3f}"
            except Exception:
                s = str(v)
        else:
            s = str(v)[:8]
        print(f" {s:<9}", end="")
    print()

# 覆盖度统计
print("\n" + "=" * 60)
print("覆盖度统计 (10 个因子):")
print("=" * 60)
for field, _ in FIELDS:
    filled = sum(1 for r in rows if r.get(field) is not None)
    status = "[OK]" if filled == len(rows) else ("[WARN]" if filled > 0 else "[FAIL]")
    print(f"  {status} {field:<32} {filled}/{len(rows)}")

conn.close()
