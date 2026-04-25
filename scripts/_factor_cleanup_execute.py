"""执行因子库清理: Rule A (近同质) + Rule B (纯噪声).

规则 (无 SOTA 依赖):

  Rule A: near_identical  (|corr| ≥ 0.999 簇内非 rep member)
    → 条件: qe_factor_classification.cluster_role='member'
    → 动作: is_available=false, disable_reason='v2_cleanup:near_identical'

  Rule B: pure_noise      (极低 IC + 方向不稳)
    → 条件 (ALL 必须满足):
       - official_grade='D'
       - ABS(ic_value) < 0.01
       - ic_sign_consistency_12m < 0.55
       - ts_info_density = 'low'
    → 动作: is_available=false, disable_reason='v2_cleanup:pure_noise'

  取消原 weak_signal rehab (Rule B 直接判定).
  取消 SOTA 豁免.

  全部操作可通过 disable_batch_id 一键回滚.

用法:
    python F:\\Dev\\AIstock\\scripts\\_factor_cleanup_execute.py             # dry-run
    python F:\\Dev\\AIstock\\scripts\\_factor_cleanup_execute.py --execute   # 写库
"""
import argparse
import os
import sys
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv

load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

parser = argparse.ArgumentParser()
parser.add_argument("--execute", action="store_true")
parser.add_argument("--ic-abs-threshold", type=float, default=0.01,
                    help="Rule B: |ic_value| 阈值, 默认 0.01")
parser.add_argument("--sign-threshold", type=float, default=0.55,
                    help="Rule B: ic_sign_consistency_12m 阈值, 默认 0.55")
args = parser.parse_args()

BATCH_ID = f"v2_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
IC_TH = args.ic_abs_threshold
SIGN_TH = args.sign_threshold
DRY_RUN = not args.execute

print(f"批次 ID       : {BATCH_ID}")
print(f"Rule A        : cluster_role='member' (近同质 |corr|≥0.999)")
print(f"Rule B        : grade='D' AND |ic|<{IC_TH} AND sign<{SIGN_TH} AND ts='low'")
print(f"模式          : {'DRY-RUN' if DRY_RUN else 'EXECUTE'}\n")

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

# === 1. Rule A: cluster_role='member' ===
with conn.cursor() as cur:
    cur.execute("""
        SELECT a.id, a.factor_name, c.cluster_id, c.cluster_size, c.intra_cluster_max_corr
        FROM aistock_factor_catalog a
        JOIN qe_factor_classification c ON c.factor_catalog_id = a.id
        WHERE a.is_available = true AND c.cluster_role = 'member'
        ORDER BY c.cluster_id, a.factor_name
    """)
    rule_a_rows = cur.fetchall()
print(f"Rule A (near_identical member): {len(rule_a_rows)}")

# === 2. Rule B: 纯噪声 ===
with conn.cursor() as cur:
    cur.execute(f"""
        WITH latest AS (
            SELECT DISTINCT ON (factor_catalog_id)
                   factor_catalog_id, official_grade, official_score
            FROM qe_factor_official_ratings
            ORDER BY factor_catalog_id, graded_at DESC
        )
        SELECT a.id, a.factor_name, r.official_grade, r.official_score,
               c.ic_value, c.ic_sign_consistency_12m, c.ts_info_density
        FROM aistock_factor_catalog a
        JOIN latest r ON r.factor_catalog_id = a.id
        JOIN qe_factor_classification c ON c.factor_catalog_id = a.id
        WHERE a.is_available = true
          AND r.official_grade = 'D'
          AND c.ic_value IS NOT NULL AND ABS(c.ic_value) < {IC_TH}
          AND c.ic_sign_consistency_12m IS NOT NULL
              AND c.ic_sign_consistency_12m < {SIGN_TH}
          AND c.ts_info_density = 'low'
        ORDER BY ABS(c.ic_value), a.factor_name
    """)
    rule_b_rows = cur.fetchall()
print(f"Rule B (pure_noise)           : {len(rule_b_rows)}")

# 去重 (若 A 和 B 重叠, 以 A 优先)
rule_a_ids = {r[0] for r in rule_a_rows}
rule_b_ids = {r[0] for r in rule_b_rows} - rule_a_ids
rule_b_rows_dedup = [r for r in rule_b_rows if r[0] in rule_b_ids]
overlap = {r[0] for r in rule_b_rows} & rule_a_ids
print(f"重叠 (A∩B, 归 A)              : {len(overlap)}")
print(f"Rule B 去重后                 : {len(rule_b_rows_dedup)}")
print(f"合计 disable                  : {len(rule_a_ids) + len(rule_b_rows_dedup)}\n")

# === 3. 按簇展示 Rule A 样本 ===
print("=== Rule A 全部簇 member (前 20) ===")
by_cluster = {}
for row in rule_a_rows:
    by_cluster.setdefault(row[2], []).append(row)
for cid in list(sorted(by_cluster.keys()))[:20]:
    members = by_cluster[cid]
    names = [m[1] for m in members[:5]]
    more = f" (+{len(members)-5})" if len(members) > 5 else ""
    print(f"  cluster {cid} size={members[0][3]}: {', '.join(names)}{more}")

# === 4. Rule B 样本 ===
print("\n=== Rule B 纯噪声样本 (前 15) ===")
for r in rule_b_rows_dedup[:15]:
    fid, name, grade, score, ic, sign, ts = r
    print(f"  {name:<45} grade={grade} score={score:.1f} ic={ic:+.4f} sign={sign:.2f} ts={ts}")
if len(rule_b_rows_dedup) > 15:
    print(f"  ... 还有 {len(rule_b_rows_dedup)-15} 个")

# === 5. dry-run 汇总 ===
if DRY_RUN:
    print("\n[DRY-RUN] 不写库. 加 --execute 真实执行.")
    print(f"\n回滚 SQL (执行后可用):")
    print(f"  UPDATE aistock_factor_catalog SET is_available=true, disable_reason=NULL,")
    print(f"    disable_batch_id=NULL, disable_at=NULL WHERE disable_batch_id='{BATCH_ID}';")
    conn.close()
    sys.exit(0)

# === 6. 执行 ===
with conn.cursor() as cur:
    a_ids = [r[0] for r in rule_a_rows]
    if a_ids:
        cur.execute("""
            UPDATE aistock_factor_catalog
            SET is_available=false,
                disable_reason='v2_cleanup:near_identical',
                disable_batch_id=%s,
                disable_at=NOW()
            WHERE id = ANY(%s) AND is_available=true
        """, (BATCH_ID, a_ids))
        print(f"  Rule A 更新: {cur.rowcount}")

    b_ids = [r[0] for r in rule_b_rows_dedup]
    if b_ids:
        cur.execute("""
            UPDATE aistock_factor_catalog
            SET is_available=false,
                disable_reason='v2_cleanup:pure_noise',
                disable_batch_id=%s,
                disable_at=NOW()
            WHERE id = ANY(%s) AND is_available=true
        """, (BATCH_ID, b_ids))
        print(f"  Rule B 更新: {cur.rowcount}")

    conn.commit()
    print("\n[OK] 已提交.")

# === 7. 验证 ===
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE is_available=true")
    enabled = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE is_available=false")
    disabled = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE disable_batch_id=%s", (BATCH_ID,))
    batch_n = cur.fetchone()[0]
    cur.execute("""
        SELECT disable_reason, COUNT(*)
        FROM aistock_factor_catalog WHERE disable_batch_id=%s
        GROUP BY disable_reason ORDER BY COUNT(*) DESC
    """, (BATCH_ID,))
    by_reason = cur.fetchall()

print(f"\n=== 验证 ===")
print(f"  enabled       : {enabled}")
print(f"  disabled      : {disabled}")
print(f"  本批次        : {batch_n}")
for reason, n in by_reason:
    print(f"    {reason}: {n}")
print(f"\n回滚 SQL:")
print(f"  UPDATE aistock_factor_catalog SET is_available=true, disable_reason=NULL,")
print(f"    disable_batch_id=NULL, disable_at=NULL WHERE disable_batch_id='{BATCH_ID}';")

conn.close()
