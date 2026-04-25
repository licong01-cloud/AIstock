"""计算因子相关性聚类, 写回 qe_factor_classification.

算法: complete-linkage 层次聚类 (scipy)
  - 距离: dist = 1 - corr (原始相关性, 方向敏感)
  - 缺失对: dist = 1.0 (相当于 corr=0)
  - 阈值: dist ≤ 1 - threshold (e.g. threshold=0.999 → dist ≤ 0.001)
  - 保证簇内**任意**对因子相关性 ≥ threshold (无 transitive dilution)
  - 原始 corr (非 |corr|): neg_X 与 X 不会合并

Rep 规则 (无 SOTA 依赖):
  1. official_score DESC
  2. ic_sign_consistency_12m DESC
  3. id ASC

填充字段:
  cluster_id              — 簇 ID (仅 size>=2 的簇, 单例 NULL)
  cluster_role            — 'representative' | 'member' (单例 NULL)
  cluster_size            — 簇内因子数
  intra_cluster_max_corr  — 簇内最大 |corr| (含自身簇外不计)
  representative_score    — official_score (无 SOTA 加权)

用法:
    python F:\\Dev\\AIstock\\scripts\\_factor_cluster_compute.py              # dry-run
    python F:\\Dev\\AIstock\\scripts\\_factor_cluster_compute.py --execute    # 写库
    python F:\\Dev\\AIstock\\scripts\\_factor_cluster_compute.py --threshold 0.995 --execute
"""
import argparse
import os
import sys
from collections import defaultdict, Counter
from dotenv import load_dotenv

load_dotenv(r"F:\Dev\AIstock\.env")
import numpy as np
import psycopg2
import psycopg2.extras
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

parser = argparse.ArgumentParser()
parser.add_argument("--execute", action="store_true")
parser.add_argument("--threshold", type=float, default=0.999,
                    help="corr 阈值, 簇内任意对 corr >= threshold (默认 0.999)")
args = parser.parse_args()

THRESHOLD = args.threshold
DIST_CUTOFF = 1.0 - THRESHOLD
DRY_RUN = not args.execute

print(f"算法: complete-linkage, 原始 corr (方向敏感)")
print(f"阈值: corr >= {THRESHOLD}  <=>  dist <= {DIST_CUTOFF:.6f}")
print(f"Rep: (official_score DESC, ic_sign_consistency_12m DESC, id ASC)  [无 SOTA]")
print(f"模式: {'DRY-RUN' if DRY_RUN else 'EXECUTE'}\n")

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

# === 1. 加载启用因子 + 评级 + ic_sign_consistency_12m ===
with conn.cursor() as cur:
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (factor_catalog_id)
                   factor_catalog_id, official_grade, official_score
            FROM qe_factor_official_ratings
            ORDER BY factor_catalog_id, graded_at DESC
        )
        SELECT a.id, a.factor_name,
               r.official_grade,
               COALESCE(r.official_score, 0)::float AS score,
               COALESCE(c.ic_sign_consistency_12m, 0)::float AS sign_cons
        FROM aistock_factor_catalog a
        LEFT JOIN latest r ON r.factor_catalog_id = a.id
        LEFT JOIN qe_factor_classification c ON c.factor_catalog_id = a.id
        WHERE a.is_available = true
        ORDER BY a.id
    """)
    rows = cur.fetchall()
    factors = {row[0]: {"name": row[1], "grade": row[2], "score": row[3], "sign_cons": row[4]}
               for row in rows}
    fid_list = [row[0] for row in rows]
    fid_idx = {fid: i for i, fid in enumerate(fid_list)}
n = len(fid_list)
print(f"启用因子: {n}")

# === 2. 构建距离矩阵 (n x n) ===
# 初始化: 非自身 = 1.0 (等价 corr=0), 对角 = 0
D = np.ones((n, n), dtype=np.float32)
np.fill_diagonal(D, 0.0)

with conn.cursor() as cur:
    cur.execute("""
        SELECT DISTINCT ON (factor_a_id, factor_b_id)
               factor_a_id, factor_b_id, correlation
        FROM qe_factor_correlations
        ORDER BY factor_a_id, factor_b_id, as_of_date DESC
    """)
    n_edges = 0
    for a, b, c in cur.fetchall():
        if a not in fid_idx or b not in fid_idx:
            continue
        i, j = fid_idx[a], fid_idx[b]
        dist = 1.0 - float(c)  # 原始 corr
        # 对称
        D[i, j] = min(D[i, j], dist)
        D[j, i] = min(D[j, i], dist)
        n_edges += 1
print(f"加载相关性对: {n_edges}")

# === 3. complete-linkage 聚类 ===
# squareform 需要对称零对角 → OK
condensed = squareform(D, checks=False)
Z = linkage(condensed, method="complete")
labels = fcluster(Z, t=DIST_CUTOFF, criterion="distance")

# 按 label 分组
comp = defaultdict(list)
for i, lab in enumerate(labels):
    comp[int(lab)].append(fid_list[i])

multi = [m for m in comp.values() if len(m) >= 2]
singleton_n = sum(1 for m in comp.values() if len(m) == 1)
print(f"非平凡簇 (size>=2): {len(multi)}, 单例: {singleton_n}")
size_dist = Counter(len(m) for m in multi)
print(f"簇大小分布: {dict(sorted(size_dist.items()))}")
print(f"簇内因子总数 (multi): {sum(len(m) for m in multi)}  冗余(非 rep): {sum(len(m)-1 for m in multi)}")

# === 4. 验证每簇内 min corr ≥ threshold ===
bad = 0
for members in multi:
    idxs = [fid_idx[fid] for fid in members]
    sub = D[np.ix_(idxs, idxs)]
    # 非对角最大 dist
    np.fill_diagonal(sub, 0.0)
    max_dist = float(sub.max())
    min_corr = 1.0 - max_dist
    if min_corr < THRESHOLD - 1e-6:
        bad += 1
        print(f"  [WARN] 簇 size={len(members)} min_corr={min_corr:.4f} < {THRESHOLD}")
print(f"簇内最小 corr 违规簇数: {bad}  (应为 0)")

# === 5. 计算 intra_cluster_max_corr (簇内最大 |corr|) ===
intra_max = {}
for members in multi:
    idxs = [fid_idx[fid] for fid in members]
    sub = D[np.ix_(idxs, idxs)]
    # corr = 1 - dist, |corr| = |1 - dist|, 但簇内 dist ≤ 0.001, corr 近 1, 简单取
    for k, fid in enumerate(members):
        row_dist = sub[k].copy()
        row_dist[k] = np.inf  # 排除自身
        min_dist = float(row_dist.min())
        intra_max[fid] = 1.0 - min_dist  # 最近邻 corr

# === 6. Rep 选择 (无 SOTA) ===
cluster_of = {}
for cid_idx, members in enumerate(multi, start=1):
    for fid in members:
        cluster_of[fid] = cid_idx

updates = []
for cid_idx, members in enumerate(multi, start=1):
    ranked = sorted(
        members,
        key=lambda fid: (-factors[fid]["score"], -factors[fid]["sign_cons"], fid)
    )
    rep_fid = ranked[0]
    cluster_size = len(members)
    for fid in members:
        role = "representative" if fid == rep_fid else "member"
        rep_score = factors[fid]["score"]  # 无 SOTA 加权
        updates.append((
            fid, cid_idx, role, cluster_size,
            float(intra_max.get(fid, 0.0)), rep_score,
        ))

multi_fids = set(cluster_of)
singletons = [fid for fid in factors if fid not in multi_fids]
print(f"\n待写入: multi {len(updates)} 行, singleton 清空 {len(singletons)} 行")

# === 7. 展示前 10 大簇 ===
print("\n=== 前 10 大簇 (rep 标 [REP]) ===")
multi_sorted = sorted(multi, key=lambda m: -len(m))
for members in multi_sorted[:10]:
    ranked = sorted(members, key=lambda fid: (-factors[fid]["score"], -factors[fid]["sign_cons"], fid))
    print(f"\n  簇 size={len(members)}:")
    for j, fid in enumerate(ranked[:8]):
        f = factors[fid]
        mk = "[REP]" if j == 0 else "     "
        print(f"    {mk} grade={f['grade'] or '-'} score={f['score']:.1f} sign={f['sign_cons']:.2f} {f['name']}")
    if len(ranked) > 8:
        print(f"    ... 还有 {len(ranked)-8} 个")

# === 8. 执行或预览 ===
if DRY_RUN:
    print("\n[DRY-RUN] 加 --execute 真实写库.")
    conn.close()
    sys.exit(0)

with conn.cursor() as cur:
    if singletons:
        cur.execute("""
            UPDATE qe_factor_classification
            SET cluster_id=NULL, cluster_role=NULL, cluster_size=NULL,
                intra_cluster_max_corr=NULL, representative_score=NULL
            WHERE factor_catalog_id = ANY(%s)
        """, (singletons,))
        print(f"  singleton 清空: {cur.rowcount}")

    psycopg2.extras.execute_batch(cur, """
        UPDATE qe_factor_classification
        SET cluster_id=%s, cluster_role=%s, cluster_size=%s,
            intra_cluster_max_corr=%s, representative_score=%s
        WHERE factor_catalog_id=%s
    """, [(cid, role, size, corr, rep_score, fid)
          for fid, cid, role, size, corr, rep_score in updates], page_size=200)
    print(f"  multi 簇写入: {len(updates)}")

    conn.commit()
    print("\n[OK] 已提交.")

with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE c.cluster_id IS NOT NULL) AS has_cid,
               COUNT(*) AS total,
               COUNT(DISTINCT c.cluster_id) AS n_clusters,
               COUNT(*) FILTER (WHERE c.cluster_role='representative') AS n_reps,
               COUNT(*) FILTER (WHERE c.cluster_role='member') AS n_members
        FROM qe_factor_classification c
        JOIN aistock_factor_catalog a ON a.id = c.factor_catalog_id
        WHERE a.is_available=true
    """)
    row = cur.fetchone()
    print(f"\n=== 验证 ===")
    print(f"  启用因子 total       : {row[1]}")
    print(f"  带 cluster_id        : {row[0]}")
    print(f"  不同簇数             : {row[2]}")
    print(f"  representative 数    : {row[3]} (应=簇数)")
    print(f"  member 数            : {row[4]}")

conn.close()
