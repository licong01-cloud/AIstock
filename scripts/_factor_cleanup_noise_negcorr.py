"""增量清理: Rule B v2 (|IC|≈0 纯噪声) + Rule A' (|corr|≈1 反向重复).

在 v2_cleanup_20260423_181027 (87 disable) 之上增量执行.

Rule B v2 (pure_noise):
    grade='D'
    AND ABS(ic_mean) < 0.003
    AND ABS(rank_ic_mean) < 0.003
    AND ic_positive_ratio BETWEEN 0.45 AND 0.55
    AND ABS(rank_icir) < 0.1

Rule A' (reverse_redundant):
    corr <= -0.999 的启用因子对, 去掉重复:
      - corr == -1.0 精确 → 保留 ic_mean 正的
      - corr in (-1.0, -0.999] → 保留 |ic_mean| 更大的

用法:
    python F:\\Dev\\AIstock\\scripts\\_factor_cleanup_noise_negcorr.py              # dry-run
    python F:\\Dev\\AIstock\\scripts\\_factor_cleanup_noise_negcorr.py --execute    # 写库
"""
import argparse
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

parser = argparse.ArgumentParser()
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()

BATCH_ID = f"v2_noise_neg_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
DRY_RUN = not args.execute

# 常量
IC_TH = 0.003
RANK_IC_TH = 0.003
POS_LO = 0.45
POS_HI = 0.55
RICIR_TH = 0.1
NEG_CORR_TH = -0.999
EXACT_NEG = -0.9999  # 视为 "精确 -1"

print(f"批次 ID : {BATCH_ID}")
print(f"模式    : {'DRY-RUN' if DRY_RUN else 'EXECUTE'}\n")

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

# === 加载 启用因子 + metrics ===
with conn.cursor() as cur:
    cur.execute("""
        WITH rated AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id, official_grade, official_score
            FROM qe_factor_official_ratings ORDER BY factor_catalog_id, graded_at DESC
        ),
        metrics AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id,
                   ic_mean, rank_ic_mean, icir, rank_icir, ic_positive_ratio, coverage
            FROM aistock_factor_metrics
            WHERE eval_window='out_sample' AND return_horizon='1d'
            ORDER BY factor_catalog_id, calculated_at DESC
        )
        SELECT a.id, a.factor_name, r.official_grade, r.official_score,
               m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir,
               m.ic_positive_ratio, m.coverage
        FROM aistock_factor_catalog a
        LEFT JOIN rated r ON r.factor_catalog_id=a.id
        LEFT JOIN metrics m ON m.factor_catalog_id=a.id
        WHERE a.is_available=true
    """)
    rows = cur.fetchall()
print(f"启用因子总数: {len(rows)}")

factors = {r[0]: {
    "name": r[1], "grade": r[2], "score": r[3],
    "ic": r[4] if r[4] is not None else 0.0,
    "rank_ic": r[5] if r[5] is not None else 0.0,
    "icir": r[6], "rank_icir": r[7],
    "pos_ratio": r[8], "coverage": r[9],
} for r in rows}

# === Rule B v2 ===
rule_b_ids = set()
for fid, f in factors.items():
    if (f["grade"] == "D"
        and f["ic"] is not None and abs(f["ic"]) < IC_TH
        and f["rank_ic"] is not None and abs(f["rank_ic"]) < RANK_IC_TH
        and f["pos_ratio"] is not None and POS_LO <= f["pos_ratio"] <= POS_HI
        and f["rank_icir"] is not None and abs(f["rank_icir"]) < RICIR_TH):
        rule_b_ids.add(fid)
print(f"Rule B v2 (pure_noise)       : {len(rule_b_ids)}")

# === Rule A' ===
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT DISTINCT ON (factor_a_id, factor_b_id) factor_a_id, factor_b_id, correlation
        FROM qe_factor_correlations
        ORDER BY factor_a_id, factor_b_id, as_of_date DESC
    """)
    neg_pairs = [(a, b, float(c)) for a, b, c in cur.fetchall()
                 if c <= NEG_CORR_TH and a in factors and b in factors]
print(f"corr ≤ {NEG_CORR_TH} 启用对   : {len(neg_pairs)}")

rule_a_prime = []  # [(keep_id, drop_id, corr, reason)]
rule_a_ids = set()
for a, b, c in neg_pairs:
    ic_a = factors[a]["ic"]
    ic_b = factors[b]["ic"]
    if c <= EXACT_NEG:
        # 精确 -1, 保留正 IC
        if ic_a >= 0 and ic_b < 0:
            keep, drop = a, b
        elif ic_b >= 0 and ic_a < 0:
            keep, drop = b, a
        else:
            # 两边同号或同零: 按 |IC| 大的
            keep = a if abs(ic_a) >= abs(ic_b) else b
            drop = b if keep == a else a
        reason = "corr=-1, 留正 IC"
    else:
        # 非精确 -1, 保留 |IC| 大的
        keep = a if abs(ic_a) >= abs(ic_b) else b
        drop = b if keep == a else a
        reason = "corr<-0.999, 留 |IC| 大"
    rule_a_prime.append((keep, drop, c, reason))
    rule_a_ids.add(drop)

print(f"Rule A' (reverse_redundant)  : {len(rule_a_ids)}")

# 重叠去重: A' 优先 (reason 不同)
overlap = rule_a_ids & rule_b_ids
rule_b_ids_final = rule_b_ids - rule_a_ids
print(f"A' ∩ B (归 A')                : {len(overlap)}")
print(f"Rule B 去重后                 : {len(rule_b_ids_final)}")
print(f"合计 disable                  : {len(rule_a_ids) + len(rule_b_ids_final)}\n")

# === 展示 ===
print("=== Rule A' 全部 10 个 ===")
print(f"{'corr':>8} | {'reason':<22} | {'keep':<42} | {'drop':<42}")
print("-"*130)
for keep, drop, c, reason in sorted(rule_a_prime, key=lambda x: x[2]):
    k_name = factors[keep]["name"]
    d_name = factors[drop]["name"]
    k_ic = factors[keep]["ic"]
    d_ic = factors[drop]["ic"]
    print(f"{c:>+8.4f} | {reason:<22} | {k_name[:40]:<42} ic={k_ic:+.4f} | {d_name[:40]:<42} ic={d_ic:+.4f}")

print(f"\n=== Rule B v2 样本 (前 20, 按 |IC| 升序) ===")
b_rows = [(fid, factors[fid]) for fid in rule_b_ids_final]
b_rows.sort(key=lambda x: abs(x[1]["ic"]) + abs(x[1]["rank_ic"]))
for fid, f in b_rows[:20]:
    print(f"  {f['name']:<50} grade={f['grade']} score={f['score']:.1f} "
          f"ic={f['ic']:+.4f} rankIC={f['rank_ic']:+.4f} "
          f"pos={f['pos_ratio']*100:.1f}%")
if len(b_rows) > 20:
    print(f"  ... 还有 {len(b_rows)-20} 个")

print("\n=== Rule B v2 grade 分布 ===")
from collections import Counter
grades = Counter(factors[fid]["grade"] for fid in rule_b_ids_final)
print(f"  {dict(grades)}  (应全 D)")

# === 执行 ===
if DRY_RUN:
    print("\n[DRY-RUN] 不写库. 加 --execute 真实执行.")
    print(f"\n回滚 SQL (执行后):")
    print(f"  UPDATE aistock_factor_catalog SET is_available=true, disable_reason=NULL,")
    print(f"    disable_batch_id=NULL, disable_at=NULL WHERE disable_batch_id='{BATCH_ID}';")
    conn.close()
    sys.exit(0)

with conn.cursor() as cur:
    # Rule B v2 → pure_noise
    b_ids = list(rule_b_ids_final)
    if b_ids:
        cur.execute("""
            UPDATE aistock_factor_catalog
            SET is_available=false,
                disable_reason='v2_cleanup:pure_noise_v2',
                disable_batch_id=%s,
                disable_at=NOW()
            WHERE id = ANY(%s) AND is_available=true
        """, (BATCH_ID, b_ids))
        print(f"  Rule B v2 更新: {cur.rowcount}")

    # Rule A' → reverse_redundant
    a_ids = list(rule_a_ids)
    if a_ids:
        cur.execute("""
            UPDATE aistock_factor_catalog
            SET is_available=false,
                disable_reason='v2_cleanup:reverse_redundant',
                disable_batch_id=%s,
                disable_at=NOW()
            WHERE id = ANY(%s) AND is_available=true
        """, (BATCH_ID, a_ids))
        print(f"  Rule A' 更新  : {cur.rowcount}")

    conn.commit()
    print("\n[OK] 已提交.")

# 验证
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE is_available=true")
    enabled = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE is_available=false")
    disabled = cur.fetchone()[0]
    cur.execute("""
        SELECT disable_reason, COUNT(*) FROM aistock_factor_catalog
        WHERE disable_batch_id=%s GROUP BY disable_reason ORDER BY COUNT(*) DESC
    """, (BATCH_ID,))
    by_reason = cur.fetchall()

print(f"\n=== 验证 ===")
print(f"  enabled   : {enabled}")
print(f"  disabled  : {disabled}")
print(f"  本批次    : ")
for reason, n in by_reason:
    print(f"    {reason}: {n}")
print(f"\n回滚 SQL:")
print(f"  UPDATE aistock_factor_catalog SET is_available=true, disable_reason=NULL,")
print(f"    disable_batch_id=NULL, disable_at=NULL WHERE disable_batch_id='{BATCH_ID}';")

conn.close()
