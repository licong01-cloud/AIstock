"""生成因子库清理候选清单 (只生成 CSV, 不动库)。

清理分类:
  delete_no_signal    : 真硬失败 — hard_gate_flags.a_core_ic=false (核心 IC 不达 A 级下限)
  delete_force_d      : overfit_force_d=true (过拟合强制 D)
  weak_signal         : ts_info_density=low + cross_horizon_consistency<0.5 (双弱)
  unstable_direction  : ic_sign_consistency_12m<0.5 + ic_oos_is_ratio<0.3
  pending_corr_dedup  : 待 cluster_id 出来后, 高相关簇内冗余因子 (本次不出, 等修 dispatch)
  keep                : 其他

输出: F:\\Dev\\AIstock\\reports\\factor_cleanup_candidates_<date>.csv
"""
import os
import csv
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2
import psycopg2.extras

OUT_DIR = r"F:\Dev\AIstock\reports"
os.makedirs(OUT_DIR, exist_ok=True)
OUT_CSV = os.path.join(OUT_DIR, f"factor_cleanup_candidates_{datetime.now().strftime('%Y%m%d')}.csv")

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("""
        WITH latest_rating AS (
            SELECT DISTINCT ON (factor_catalog_id)
                   factor_catalog_id, official_grade, official_score,
                   hard_gate_flags, grade_reason_structured, graded_at
            FROM qe_factor_official_ratings
            ORDER BY factor_catalog_id, graded_at DESC
        )
        SELECT
            a.id              AS catalog_id,
            a.factor_name,
            a.source,
            a.is_available,
            r.official_grade,
            r.official_score,
            r.hard_gate_flags,
            r.grade_reason_structured,
            c.ts_info_density,
            c.cross_horizon_consistency,
            c.ic_sign_consistency_12m,
            c.ic_oos_is_ratio,
            c.monthly_ic_trend_slope,
            c.factor_dimension,
            c.holding_period_class,
            c.signal_mechanism
        FROM aistock_factor_catalog a
        LEFT JOIN qe_factor_classification c ON c.factor_catalog_id = a.id
        LEFT JOIN latest_rating r ON r.factor_catalog_id = a.id
        WHERE a.is_available = true
        ORDER BY r.official_score DESC NULLS LAST
    """)
    rows = cur.fetchall()

print(f"读取启用因子 {len(rows)} 个")

# 分类逻辑
def classify(r):
    flags = r["hard_gate_flags"] or {}
    grade = r["official_grade"]
    score = r["official_score"]
    ts = r["ts_info_density"]
    cross = r["cross_horizon_consistency"]
    sign12 = r["ic_sign_consistency_12m"]
    oos = r["ic_oos_is_ratio"]

    reasons = []

    # 1. 真硬失败: a_core_ic=false
    if flags.get("a_core_ic") is False:
        reasons.append("a_core_ic=false (核心IC不达A级下限)")
        return "delete_no_signal", reasons

    # 2. 过拟合强制 D
    if flags.get("overfit_force_d") is True:
        reasons.append("overfit_force_d=true (过拟合强制D)")
        return "delete_force_d", reasons

    # 3. 双弱信号
    if ts == "low" and cross is not None and float(cross) < 0.5:
        reasons.append(f"ts_info_density=low + cross_horizon={float(cross):.2f}")
        return "weak_signal", reasons

    # 4. 方向极不稳定
    if sign12 is not None and oos is not None:
        if float(sign12) < 0.5 and float(oos) < 0.3:
            reasons.append(f"sign_12m={float(sign12):.2f} + oos/is={float(oos):.2f}")
            return "unstable_direction", reasons

    return "keep", []


# 分类 + 写 CSV
counts = {}
with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow([
        "catalog_id", "factor_name", "source", "cleanup_category", "cleanup_reasons",
        "official_grade", "official_score",
        "ts_info_density", "cross_horizon_consistency",
        "ic_sign_consistency_12m", "ic_oos_is_ratio", "monthly_ic_trend_slope",
        "factor_dimension", "holding_period_class", "signal_mechanism",
        "a_core_ic", "overfit_force_d", "a_overfit", "a_turnover",
        "failed_gates_a", "failed_gates_s",
    ])
    for r in rows:
        cat, reasons = classify(r)
        counts[cat] = counts.get(cat, 0) + 1
        flags = r["hard_gate_flags"] or {}
        gates_struct = r["grade_reason_structured"] or {}
        fg = gates_struct.get("failed_gates", []) if isinstance(gates_struct, dict) else []
        a_gates = [g for g in fg if g.startswith("a_")]
        s_gates = [g for g in fg if g.startswith("s_")]
        writer.writerow([
            r["catalog_id"], r["factor_name"], r["source"], cat, " | ".join(reasons),
            r["official_grade"], r["official_score"],
            r["ts_info_density"],
            None if r["cross_horizon_consistency"] is None else float(r["cross_horizon_consistency"]),
            None if r["ic_sign_consistency_12m"] is None else float(r["ic_sign_consistency_12m"]),
            None if r["ic_oos_is_ratio"] is None else float(r["ic_oos_is_ratio"]),
            None if r["monthly_ic_trend_slope"] is None else float(r["monthly_ic_trend_slope"]),
            r["factor_dimension"], r["holding_period_class"], r["signal_mechanism"],
            flags.get("a_core_ic"), flags.get("overfit_force_d"),
            flags.get("a_overfit"), flags.get("a_turnover"),
            ",".join(a_gates), ",".join(s_gates),
        ])

# Console summary
print("\n" + "=" * 70)
print("清理候选分布:")
print("=" * 70)
total = sum(counts.values())
for cat in ["delete_no_signal", "delete_force_d", "weak_signal", "unstable_direction", "keep"]:
    n = counts.get(cat, 0)
    pct = 100 * n / total if total else 0
    print(f"  {cat:<22}  {n:>5}  ({pct:5.1f}%)")
print("-" * 70)
print(f"  {'TOTAL':<22}  {total:>5}")
print(f"\nCSV 已写入: {OUT_CSV}")

# 进一步交叉表: 按 grade × category
print("\n" + "=" * 70)
print("evaluate × cleanup_category 交叉表:")
print("=" * 70)
import collections
cross = collections.defaultdict(lambda: collections.Counter())
for r in rows:
    cat, _ = classify(r)
    cross[r["official_grade"] or "NULL"][cat] += 1
print(f"  {'grade':<8}", end="")
for c in ["delete_no_signal", "delete_force_d", "weak_signal", "unstable_direction", "keep"]:
    print(f" {c[:14]:<14}", end="")
print()
for g in ["S", "A", "B", "C", "D", "NULL"]:
    if g not in cross:
        continue
    print(f"  {g:<8}", end="")
    for c in ["delete_no_signal", "delete_force_d", "weak_signal", "unstable_direction", "keep"]:
        print(f" {cross[g][c]:<14}", end="")
    print()

conn.close()
