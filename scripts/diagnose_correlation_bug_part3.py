"""
诊断 Part 3: 追查 HDF5 corr=1.0 的真正来源。

已确认事实:
1. GEMM 算法正确 — 585因子单日结果和2因子完全一致
2. 153 对假阳性 — parquet 未更新过, MD5 不同, 但 HDF5 中 corr=1.0
3. 47 组 MD5 重复因子，但 HDF5 中大量期望的 corr=1.0 反而不是 1.0

假说:
A. HDF5 矩阵索引和 factor_names 有映射偏移
B. HDF5 是旧版代码/不同逻辑产生的
C. DB 中的 corr 值和 HDF5 不同（来自 _estimate_by_category fallback）
D. _merged_panel.parquet 缓存在计算时含有错误数据

本脚本验证所有假说。
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd
import h5py
from scipy import stats
from collections import defaultdict

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"
HDF5_PATH = r"F:\Dev\AIstock\data\correlation_matrices\corr_20260403.h5"


def test_hypothesis_A():
    """假说A: 矩阵索引偏移 — 查看 HDF5 中 corr=1.0 的位置是否对应对角线附近"""
    print("=" * 70)
    print("  假说A: 矩阵索引偏移检查")
    print("=" * 70)

    with h5py.File(HDF5_PATH, "r") as f:
        matrix = f["matrix"][:]
        raw_names = f.attrs["factor_names"]
        names = [s.decode("utf-8") if isinstance(s, bytes) else str(s) for s in raw_names]

    K = len(names)

    # 找所有 corr=1.0 的 (i,j) 位置
    ones_positions = []
    for i in range(K):
        for j in range(i+1, K):
            if abs(matrix[i, j] - 1.0) < 1e-6:
                ones_positions.append((i, j, j - i))

    print(f"corr=1.0 对数: {len(ones_positions)}")
    print(f"\n距离分布 (j-i):")
    dist_counts = defaultdict(int)
    for i, j, d in ones_positions:
        dist_counts[d] += 1
    for d in sorted(dist_counts.keys())[:20]:
        print(f"  距离 {d}: {dist_counts[d]} 对")

    # 检查对角线附近是否集中
    near_diag = sum(1 for _, _, d in ones_positions if d <= 5)
    far_diag = sum(1 for _, _, d in ones_positions if d > 50)
    print(f"\n对角线 <=5: {near_diag}, >50: {far_diag}")

    # 具体看几对假阳性的矩阵坐标
    print(f"\n假阳性坐标详情 (前20):")
    for i, j, d in ones_positions[:20]:
        print(f"  [{i:3d},{j:3d}] d={d:3d}: {names[i]} <-> {names[j]}")

    # 检查: 如果索引偏移了 N 位, 那么 matrix[i,j] 实际上是 matrix[i+N, j+N] 的值
    # 简单测试: 某对已知相关性的因子, 看矩阵值是否对上
    print(f"\n对角线检查 (matrix[i,i] 应该全是 1.0):")
    non_one_diag = [(i, matrix[i,i]) for i in range(K) if abs(matrix[i,i] - 1.0) > 1e-6]
    print(f"  对角线非 1.0 的元素: {len(non_one_diag)}")
    if non_one_diag:
        for i, v in non_one_diag[:5]:
            print(f"    [{i}] {names[i]}: {v}")

    return names, matrix


def test_hypothesis_B(names, matrix):
    """假说B: 重新用当前代码计算几天, 和 HDF5 比较"""
    print("\n" + "=" * 70)
    print("  假说B: 重算验证 — 用当前数据和代码重新计算")
    print("=" * 70)

    # 找测试日期
    first_fn = names[0]
    fp = os.path.join(SINGLE_DIR, f"{first_fn}.parquet")
    df = pd.read_parquet(fp, columns=[])
    all_dates = df.index.get_level_values(0).unique().sort_values()
    window_dates = all_dates[-252:]

    # 选 10 个均匀分布的日期做抽样验证
    sample_indices = np.linspace(0, len(window_dates)-1, 10, dtype=int)
    sample_dates = [window_dates[i] for i in sample_indices]

    # 选 5 对因子做验证（含假阳性和正常对）
    test_pairs = [
        ("CHIP_COST_DISPERSION", "cp_cost_spread"),       # 假阳性 HDF5=1.0, 实际=0.85
        ("BookToPrice_Ratio", "cost_pressure"),             # 假阳性 HDF5=1.0, 实际=-0.03
        ("ChipCostIndustryDivergence", "ChipPressureFactor"),  # 假阳性 HDF5=1.0, 实际=-0.99
        ("momentum_20D", "momentum_price_strength"),        # MD5 重复, 期望=1.0
        ("MOM5", "Momentum_5D_Return"),                     # MD5 重复, 期望=1.0
    ]

    # 加载所有测试因子数据
    test_factor_set = set()
    for a, b in test_pairs:
        test_factor_set.add(a)
        test_factor_set.add(b)

    factor_data = {}
    for fn in test_factor_set:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if not os.path.isfile(fp):
            print(f"  {fn}: 无 parquet")
            continue
        df = pd.read_parquet(fp)
        if "value" in df.columns:
            df = df.rename(columns={"value": fn})
        factor_data[fn] = df

    # 对每对因子, 在每个采样日计算 scipy spearman
    print(f"\n各对因子在采样日的 scipy Spearman:")
    for name_a, name_b in test_pairs:
        if name_a not in factor_data or name_b not in factor_data:
            print(f"\n  {name_a} <-> {name_b}: 缺数据")
            continue

        # HDF5 值
        if name_a in names and name_b in names:
            idx_a = names.index(name_a)
            idx_b = names.index(name_b)
            hdf5_val = matrix[idx_a, idx_b]
        else:
            hdf5_val = float('nan')

        daily_corrs = []
        for ts in sample_dates:
            try:
                sec_a = factor_data[name_a].loc[ts]
                sec_b = factor_data[name_b].loc[ts]
            except KeyError:
                continue
            merged = pd.concat([sec_a, sec_b], axis=1).dropna()
            if len(merged) < 100:
                continue
            corr, _ = stats.spearmanr(merged.iloc[:, 0], merged.iloc[:, 1])
            daily_corrs.append(corr)

        if daily_corrs:
            mean_corr = np.mean(daily_corrs)
            std_corr = np.std(daily_corrs)
            print(f"\n  {name_a} <-> {name_b}:")
            print(f"    HDF5: {hdf5_val:.6f}")
            print(f"    scipy mean: {mean_corr:.6f} (std={std_corr:.6f}, n_days={len(daily_corrs)})")
            print(f"    daily: {[f'{c:.4f}' for c in daily_corrs]}")
            if abs(hdf5_val - 1.0) < 1e-6 and abs(mean_corr - 1.0) > 0.1:
                print(f"    *** BUG CONFIRMED: HDF5=1.0 但实际={mean_corr:.4f} ***")


def test_hypothesis_C():
    """假说C: 检查 DB 中存的值是否和 HDF5 一致"""
    print("\n" + "=" * 70)
    print("  假说C: DB 中存储的相关性值")
    print("=" * 70)

    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="aistock",
        user="postgres", password=os.environ.get("PGPASSWORD", "lc78080808")
    )

    with conn.cursor() as cur:
        # 检查假阳性对在 DB 中的值
        test_pairs = [
            ("CHIP_COST_DISPERSION", "cp_cost_spread"),
            ("BookToPrice_Ratio", "cost_pressure"),
            ("ChipCostIndustryDivergence", "ChipPressureFactor"),
            ("ChipCostIndustryDivergence", "DividendToFreeTurnover_Ratio"),
        ]

        print(f"\nDB 中假阳性对的 correlation 值:")
        for name_a, name_b in test_pairs:
            cur.execute("""
                SELECT fc.correlation, fc.method, fc.computed_at, fc.as_of_date
                FROM qe_factor_correlations fc
                JOIN aistock_factor_catalog ca ON ca.id = fc.factor_a_id
                JOIN aistock_factor_catalog cb ON cb.id = fc.factor_b_id
                WHERE (ca.factor_name = %s AND cb.factor_name = %s)
                   OR (ca.factor_name = %s AND cb.factor_name = %s)
            """, (name_a, name_b, name_b, name_a))

            rows = cur.fetchall()
            if rows:
                for corr, method, computed_at, as_of in rows:
                    print(f"  {name_a} <-> {name_b}: "
                          f"DB corr={corr:.6f}, method={method}, "
                          f"computed={computed_at}, as_of={as_of}")
            else:
                print(f"  {name_a} <-> {name_b}: DB 中无记录")

        # DB 中 corr=1.0 的记录数
        cur.execute("""
            SELECT count(*) FROM qe_factor_correlations WHERE abs(correlation - 1.0) < 0.000001
        """)
        db_ones = cur.fetchone()[0]

        cur.execute("SELECT count(*) FROM qe_factor_correlations")
        db_total = cur.fetchone()[0]

        cur.execute("""
            SELECT count(*) FROM qe_factor_correlations WHERE abs(correlation) > 0.99
        """)
        db_high = cur.fetchone()[0]

        print(f"\nDB 统计:")
        print(f"  总记录: {db_total}")
        print(f"  corr=1.0: {db_ones}")
        print(f"  |corr|>0.99: {db_high}")

        # 检查 method 分布
        cur.execute("""
            SELECT method, count(*) FROM qe_factor_correlations GROUP BY method
        """)
        print(f"\nmethod 分布:")
        for method, cnt in cur.fetchall():
            print(f"  {method}: {cnt}")

        # 检查 _estimate_by_category 是否写入了 corr=1.0
        cur.execute("""
            SELECT ca.factor_name, cb.factor_name, fc.correlation, fc.method
            FROM qe_factor_correlations fc
            JOIN aistock_factor_catalog ca ON ca.id = fc.factor_a_id
            JOIN aistock_factor_catalog cb ON cb.id = fc.factor_b_id
            WHERE abs(fc.correlation - 1.0) < 0.000001
            LIMIT 20
        """)
        print(f"\nDB 中 corr=1.0 的记录 (前20):")
        for fa, fb, corr, method in cur.fetchall():
            print(f"  {fa} <-> {fb}: {corr:.6f} method={method}")

        # 检查 _estimate_by_category 的 fallback 值
        cur.execute("""
            SELECT fc.correlation, count(*) as cnt
            FROM qe_factor_correlations fc
            WHERE fc.method = 'category_estimate'
               OR fc.method LIKE '%estimate%'
            GROUP BY fc.correlation
            ORDER BY cnt DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        if rows:
            print(f"\nestimate 方法的值分布:")
            for corr, cnt in rows:
                print(f"  corr={corr:.4f}: {cnt} 条")
        else:
            print(f"\n无 estimate 方法的记录")

    conn.close()


def test_hypothesis_D(names, matrix):
    """假说D: 矩阵行列映射验证 — 用已知真重复对校验"""
    print("\n" + "=" * 70)
    print("  假说D: 矩阵行列映射精确校验")
    print("=" * 70)

    import hashlib

    # 已知 MD5 相同的对
    md5_map = {}
    for name in names:
        fpath = os.path.join(SINGLE_DIR, f"{name}.parquet")
        if os.path.isfile(fpath):
            with open(fpath, "rb") as fobj:
                md5_map[name] = hashlib.md5(fobj.read()).hexdigest()

    # 找所有真重复对
    md5_groups = defaultdict(list)
    for name, md5 in md5_map.items():
        if name in names:
            md5_groups[md5].append(name)
    dup_groups = {md5: fns for md5, fns in md5_groups.items() if len(fns) > 1}

    # 真重复对的 matrix 值分布
    print(f"MD5 重复组: {len(dup_groups)} 组, 总因子: {sum(len(v) for v in dup_groups.values())}")
    print(f"\n真重复对的 HDF5 矩阵值:")

    all_dup_vals = []
    wrong_dup_vals = []
    for md5, fns in sorted(dup_groups.items(), key=lambda x: -len(x[1])):
        for i_fn in range(len(fns)):
            for j_fn in range(i_fn + 1, len(fns)):
                a, b = fns[i_fn], fns[j_fn]
                if a in names and b in names:
                    ia = names.index(a)
                    ib = names.index(b)
                    val = matrix[ia, ib]
                    all_dup_vals.append(val)
                    if abs(val - 1.0) > 1e-6:
                        wrong_dup_vals.append((a, b, val, ia, ib))

    print(f"  总真重复对: {len(all_dup_vals)}")
    print(f"  corr=1.0 正确: {sum(1 for v in all_dup_vals if abs(v-1.0)<1e-6)}")
    print(f"  corr!=1.0 (应为1.0但不是): {len(wrong_dup_vals)}")

    if wrong_dup_vals:
        print(f"\n  应为 1.0 但不是的真重复对:")
        for a, b, val, ia, ib in wrong_dup_vals[:20]:
            print(f"    [{ia},{ib}] {a} <-> {b}: {val:.6f}")

        # 对这些对，检查矩阵中它们的行/列值模式
        print(f"\n  这些对在矩阵中的邻近值:")
        for a, b, val, ia, ib in wrong_dup_vals[:3]:
            # 看 [ia, :] 和 [ib, :] 的差异
            row_a = matrix[ia, :]
            row_b = matrix[ib, :]
            valid = ~(np.isnan(row_a) | np.isnan(row_b))
            if valid.sum() > 0:
                diff = np.abs(row_a[valid] - row_b[valid])
                print(f"    {a} vs {b}: 行差异 mean={diff.mean():.6f}, max={diff.max():.6f}")
                # 如果两行应该完全一致但不是，说明矩阵值确实和当前数据不匹配
                if diff.mean() > 0.01:
                    print(f"    *** 行不一致！矩阵可能基于不同数据 ***")
            # 检查 matrix[ia, ib] 附近的值
            neighbors = []
            for di in [-2, -1, 0, 1, 2]:
                for dj in [-2, -1, 0, 1, 2]:
                    ni, nj = ia + di, ib + dj
                    if 0 <= ni < len(names) and 0 <= nj < len(names) and ni != nj:
                        neighbors.append((di, dj, matrix[ni, nj]))
            print(f"    [{ia},{ib}] 邻域值:")
            for di, dj, v in neighbors:
                flag = " <-- 目标" if di == 0 and dj == 0 else ""
                print(f"      [{ia+di},{ib+dj}] = {v:.6f}{flag}")


def test_hypothesis_E(names, matrix):
    """假说E: 检查是否全 NaN 行的因子导致了假阳性"""
    print("\n" + "=" * 70)
    print("  假说E: 全 NaN 行因子分析")
    print("=" * 70)

    K = len(names)
    nan_rows = []
    for i in range(K):
        row = matrix[i, :]
        row_no_diag = np.delete(row, i)
        if np.all(np.isnan(row_no_diag)):
            nan_rows.append((i, names[i]))

    print(f"全 NaN 行因子: {len(nan_rows)}")
    for idx, name in nan_rows:
        print(f"  [{idx}] {name}")

    # 这些因子是否参与了假阳性？不可能，因为 NaN != 1.0
    # 但如果矩阵有索引偏移，NaN 行可能挡住了真正的值

    # 检查 NaN 行因子附近的矩阵值
    if nan_rows:
        print(f"\n全 NaN 行因子附近的矩阵模式:")
        for idx, name in nan_rows[:3]:
            print(f"\n  [{idx}] {name}:")
            # 查看 row idx-2 到 idx+2
            for r in range(max(0, idx-2), min(K, idx+3)):
                row_vals = matrix[r, max(0,idx-2):min(K,idx+3)]
                vals_str = " ".join(f"{v:7.4f}" if not np.isnan(v) else "    NaN" for v in row_vals)
                marker = " <--" if r == idx else ""
                print(f"    row[{r:3d}]: {vals_str}{marker}")


def test_full_recomputation(names):
    """用当前代码和数据做完整重算 (取一个子集), 和 HDF5 比较"""
    print("\n" + "=" * 70)
    print("  完整重算验证 (20因子子集 x 252天)")
    print("=" * 70)

    # 选 20 个因子: 含假阳性涉及的因子 + 一些正常因子
    target_factors = [
        "CHIP_COST_DISPERSION", "cp_cost_spread",  # 假阳性对1
        "BookToPrice_Ratio", "cost_pressure",  # 假阳性对2
        "ChipCostIndustryDivergence", "ChipPressureFactor",  # 假阳性对3
        "DividendToFreeTurnover_Ratio",
        "momentum_20D", "momentum_price_strength",  # 真重复
        "MOM5", "Momentum_5D_Return",  # 真重复
        "free_turnover_rate", "m_free_turnover_rate",  # 正常对
        "IntradayVolatility_5D", "atr_5d",
        "mf_net_ratio", "chip_concentration_index",
        "m_atr_compression", "m_atr_normalized_20d",
        "turnover_adjusted_momentum_10d",
    ]

    # 只用有数据的
    available = []
    for fn in target_factors:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if os.path.isfile(fp):
            available.append(fn)
    print(f"可用因子: {len(available)}/{len(target_factors)}")

    # 使用 FactorValueLoader + CorrelationEngine 重算
    from services.quantevolver.factor_value_loader import FactorValueLoader
    from services.quantevolver.correlation_engine import CorrelationEngine

    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()
    loader = FactorValueLoader(source="single")
    engine = CorrelationEngine(loader)

    t0 = time.time()
    result = engine.compute_full_matrix(available, save_hdf5=False)
    elapsed = time.time() - t0
    print(f"重算完成: {len(result.factor_names)} 因子, {result.effective_window} 天, {elapsed:.1f}s")

    # 和 HDF5 比较
    with h5py.File(HDF5_PATH, "r") as f:
        hdf5_matrix = f["matrix"][:]
        raw_names = f.attrs["factor_names"]
        hdf5_names = [s.decode("utf-8") if isinstance(s, bytes) else str(s) for s in raw_names]

    print(f"\n重算结果 vs HDF5 对比:")
    print(f"{'因子A':<35} {'因子B':<35} {'重算':>8} {'HDF5':>8} {'差':>8}")
    print("-" * 105)

    for i, fn_a in enumerate(result.factor_names):
        for j in range(i+1, len(result.factor_names)):
            fn_b = result.factor_names[j]
            recomp_val = result.matrix[i, j]

            if fn_a in hdf5_names and fn_b in hdf5_names:
                ha = hdf5_names.index(fn_a)
                hb = hdf5_names.index(fn_b)
                hdf5_val = hdf5_matrix[ha, hb]
            else:
                hdf5_val = float('nan')

            diff = abs(recomp_val - hdf5_val) if not (np.isnan(recomp_val) or np.isnan(hdf5_val)) else 0

            if diff > 0.01 or (abs(hdf5_val - 1.0) < 1e-6 and abs(recomp_val - 1.0) > 0.01):
                flag = " *** MISMATCH ***"
            else:
                flag = ""

            # 只输出有差异或相关性高的对
            if diff > 0.01 or abs(recomp_val) > 0.8:
                print(f"{fn_a:<35} {fn_b:<35} {recomp_val:8.4f} {hdf5_val:8.4f} {diff:8.4f}{flag}")


def main():
    print("*" * 70)
    print("  相关性 Bug 诊断 Part 3 — 追查 HDF5 真正来源")
    print(f"  时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("*" * 70)

    # 假说 A: 索引偏移
    names, matrix = test_hypothesis_A()

    # 假说 B: 重算验证
    test_hypothesis_B(names, matrix)

    # 假说 C: DB 值
    test_hypothesis_C()

    # 假说 D: 行列映射
    test_hypothesis_D(names, matrix)

    # 假说 E: NaN 行
    test_hypothesis_E(names, matrix)

    # 完整重算验证
    test_full_recomputation(names)

    print(f"\n{'='*70}")
    print(f"  Part 3 诊断完成")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
