"""
相关性计算 Bug 精确诊断脚本 v2。

逐步追踪完整计算链路，找到 corr=1.0 假阳性的真正根因。
对比：scipy.stats.spearmanr (参考) vs GEMM (被测)
"""
import sys, os, hashlib, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd
from scipy import stats
from collections import defaultdict
import h5py

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"
HDF5_PATH = r"F:\Dev\AIstock\data\correlation_matrices\corr_20260403.h5"


# ═══════════════════════════════════════
#  PART 1: HDF5 矩阵分析 — 找出所有假阳性
# ═══════════════════════════════════════

def part1_analyze_hdf5():
    print("=" * 70)
    print("  PART 1: HDF5 矩阵分析")
    print("=" * 70)

    with h5py.File(HDF5_PATH, "r") as f:
        matrix = f["matrix"][:]
        raw_names = f.attrs["factor_names"]
        names = [s.decode("utf-8") if isinstance(s, bytes) else str(s) for s in raw_names]
        eff_window = int(f.attrs["effective_window"])
        comp_time = float(f.attrs["computation_time_sec"])

    K = len(names)
    print(f"因子数: {K}, 有效窗口天数: {eff_window}, 计算耗时: {comp_time:.1f}s")

    # 矩阵基本统计
    upper = matrix[np.triu_indices(K, k=1)]
    nan_count = np.isnan(upper).sum()
    print(f"\n上三角元素: {len(upper)}")
    print(f"NaN: {nan_count} ({nan_count/len(upper)*100:.1f}%)")
    print(f"|corr|=1.0:  {(np.abs(upper[~np.isnan(upper)] - 1.0) < 1e-6).sum()}")
    print(f"|corr|>0.99: {(np.abs(upper[~np.isnan(upper)]) > 0.99).sum()}")
    print(f"|corr|>0.9:  {(np.abs(upper[~np.isnan(upper)]) > 0.9).sum()}")
    print(f"corr mean:   {np.nanmean(upper):.6f}")
    print(f"corr median: {np.nanmedian(upper):.6f}")

    # 计算所有 parquet MD5
    print(f"\n计算 parquet MD5...")
    md5_map = {}
    missing_parquet = []
    for name in names:
        fpath = os.path.join(SINGLE_DIR, f"{name}.parquet")
        if os.path.isfile(fpath):
            with open(fpath, "rb") as f:
                md5_map[name] = hashlib.md5(f.read()).hexdigest()
        else:
            missing_parquet.append(name)

    print(f"有 parquet: {len(md5_map)}, 缺失 parquet: {len(missing_parquet)}")
    if missing_parquet:
        print(f"  缺失因子: {missing_parquet[:20]}{'...' if len(missing_parquet)>20 else ''}")

    # 分类 corr=1.0 的对
    true_dups = []   # MD5 相同
    false_pos = []   # MD5 不同但 corr=1.0
    nan_involved = []  # 至少一个无 parquet

    for i in range(K):
        for j in range(i+1, K):
            if abs(matrix[i, j] - 1.0) < 1e-6:
                md5_i = md5_map.get(names[i])
                md5_j = md5_map.get(names[j])
                if md5_i is None or md5_j is None:
                    nan_involved.append((names[i], names[j]))
                elif md5_i == md5_j:
                    true_dups.append((names[i], names[j]))
                else:
                    false_pos.append((names[i], names[j]))

    print(f"\ncorr=1.0 分类:")
    print(f"  真重复 (MD5相同): {len(true_dups)} 对")
    print(f"  假阳性 (MD5不同): {len(false_pos)} 对")
    print(f"  含缺失因子:       {len(nan_involved)} 对")

    # 找出参与假阳性的所有因子
    false_pos_factors = set()
    for a, b in false_pos:
        false_pos_factors.add(a)
        false_pos_factors.add(b)
    print(f"  假阳性涉及因子数: {len(false_pos_factors)}")

    # 每个因子的 corr=1.0 计数（排除真重复）
    factor_false_count = defaultdict(int)
    for a, b in false_pos:
        factor_false_count[a] += 1
        factor_false_count[b] += 1

    print(f"\n假阳性最多的因子 (top20):")
    for name, cnt in sorted(factor_false_count.items(), key=lambda x: -x[1])[:20]:
        row_idx = names.index(name)
        row = matrix[row_idx]
        row_no_diag = np.delete(row, row_idx)
        nan_in_row = np.isnan(row_no_diag).sum()
        print(f"  {name}: {cnt} 对假阳性, 行NaN数={nan_in_row}")

    # 检查整行异常: 某因子和大量其他因子 corr 相同
    print(f"\n检查行异常模式:")
    for i, name in enumerate(names):
        row = matrix[i, :]
        row_no_diag = np.concatenate([row[:i], row[i+1:]])
        valid_vals = row_no_diag[~np.isnan(row_no_diag)]
        if len(valid_vals) == 0:
            print(f"  {name}: 全 NaN 行!")
            continue
        unique_vals = np.unique(np.round(valid_vals, 6))
        if len(unique_vals) <= 3 and len(valid_vals) > 10:
            val_counts = {v: (valid_vals == v).sum() for v in unique_vals}
            print(f"  {name}: 仅 {len(unique_vals)} 个唯一值, 分布={val_counts}")

    return names, matrix, false_pos, md5_map


# ═══════════════════════════════════════
#  PART 2: 数据加载链路诊断
# ═══════════════════════════════════════

def part2_data_loading(names, false_pos):
    print("\n" + "=" * 70)
    print("  PART 2: 数据加载链路诊断")
    print("=" * 70)

    # 选取 3 对假阳性做详细诊断
    if not false_pos:
        print("无假阳性可诊断")
        return None, None
    sample_pairs = false_pos[:3]

    # 找公共日期范围
    first_factor = sample_pairs[0][0]
    fpath = os.path.join(SINGLE_DIR, f"{first_factor}.parquet")
    first_df = pd.read_parquet(fpath, columns=[])
    dates = first_df.index.get_level_values(0).unique().sort_values()
    # 取最后 252 天
    window_dates = dates[-252:]
    start_date = window_dates[0].strftime("%Y-%m-%d")
    end_date = window_dates[-1].strftime("%Y-%m-%d")
    print(f"测试窗口: {start_date} ~ {end_date} ({len(window_dates)} 天)")

    # 检查 master_index 一致性
    print(f"\n检查 master_index 一致性...")
    all_test_factors = list(set(
        [a for a, b in sample_pairs] + [b for a, b in sample_pairs]
    ))

    index_sizes = {}
    for fn in all_test_factors:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if os.path.isfile(fp):
            df = pd.read_parquet(fp, columns=[])
            idx = df.index
            d = idx.get_level_values(0)
            idx_window = idx[(d >= window_dates[0]) & (d <= window_dates[-1])]
            index_sizes[fn] = len(idx_window)
            n_dates = idx_window.get_level_values(0).nunique()
            n_stocks = idx_window.get_level_values(1).nunique()
            print(f"  {fn}: {len(idx_window)} 行, {n_dates} 天, {n_stocks} 只股票")

    # 检查索引是否完全相同
    ref_idx = None
    ref_name = None
    mismatches = []
    for fn in all_test_factors:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if not os.path.isfile(fp):
            continue
        df = pd.read_parquet(fp, columns=[])
        idx = df.index
        d = idx.get_level_values(0)
        idx_window = idx[(d >= window_dates[0]) & (d <= window_dates[-1])]
        if ref_idx is None:
            ref_idx = idx_window
            ref_name = fn
        else:
            if not idx_window.equals(ref_idx):
                mismatches.append(fn)
                # 详细差异
                only_in_ref = ref_idx.difference(idx_window)
                only_in_fn = idx_window.difference(ref_idx)
                print(f"  [MISMATCH] {fn} vs {ref_name}: "
                      f"仅在ref: {len(only_in_ref)}, 仅在{fn}: {len(only_in_fn)}")

    if not mismatches:
        print(f"  所有测试因子索引一致")
    else:
        print(f"  {len(mismatches)} 个因子索引不一致!")

    # 检查 float32 vs float64
    print(f"\n检查 float32 精度影响...")
    for fn in all_test_factors[:2]:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if not os.path.isfile(fp):
            continue
        df = pd.read_parquet(fp)
        if "value" in df.columns:
            vals = df["value"].dropna()
        else:
            vals = df.iloc[:, 0].dropna()
        vals_f64 = vals.values.astype(np.float64)
        vals_f32 = vals.values.astype(np.float32)
        diff = np.abs(vals_f64 - vals_f32.astype(np.float64))
        max_diff = diff.max()
        mean_diff = diff.mean()
        # 检查 tied ranks 差异
        ranks_f64 = stats.rankdata(vals_f64[:5000])
        ranks_f32 = stats.rankdata(vals_f32[:5000])
        rank_diff = (ranks_f64 != ranks_f32).sum()
        print(f"  {fn}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}, "
              f"rank_diff(前5000)={rank_diff}")

    return sample_pairs, (start_date, end_date)


# ═══════════════════════════════════════
#  PART 3: GEMM 逐步追踪
# ═══════════════════════════════════════

def gemm_pearson_cpu(X, M):
    """原版 GEMM (correlation_engine.py:536-558)"""
    N_pairs = M.T @ M
    SX = X.T @ M
    SX2 = (X ** 2).T @ M
    SXY = X.T @ X

    numerator = N_pairs * SXY - SX * SX.T
    var_x = N_pairs * SX2 - SX ** 2
    var_y = N_pairs * SX2.T - SX.T ** 2
    denominator = np.sqrt(np.maximum(var_x * var_y, 0.0))

    max_var = N_pairs * N_pairs * (N_pairs + 1) / 12.0
    var_threshold = 0.01
    var_ok = (var_x > var_threshold * max_var) & (var_y > var_threshold * max_var)
    valid_pair = var_ok & (N_pairs >= 30)

    sub_mat = np.where(valid_pair, numerator / denominator, np.nan)
    np.fill_diagonal(sub_mat, 1.0)
    return sub_mat


def part3_gemm_trace(sample_pairs, date_range, all_names):
    print("\n" + "=" * 70)
    print("  PART 3: GEMM 逐步追踪 — 从2因子到N因子找 break point")
    print("=" * 70)

    if not sample_pairs:
        return

    start_date, end_date = date_range
    name_a, name_b = sample_pairs[0]

    # 选一个测试日期（倒数第10天）
    fp_a = os.path.join(SINGLE_DIR, f"{name_a}.parquet")
    df_a_full = pd.read_parquet(fp_a)
    if "value" in df_a_full.columns:
        df_a_full = df_a_full.rename(columns={"value": name_a})
    dates = df_a_full.index.get_level_values(0).unique().sort_values()
    test_date = dates[-10]
    print(f"\n目标因子对: {name_a} <-> {name_b}")
    print(f"测试日期: {test_date.strftime('%Y-%m-%d')}")

    # 加载所有因子的单日数据 (float64 原始)
    print(f"\n加载所有因子的单日数据 (float64)...")
    all_dfs = {}
    for fn in all_names:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if not os.path.isfile(fp):
            continue
        try:
            df = pd.read_parquet(fp)
            if "value" in df.columns:
                df = df.rename(columns={"value": fn})
            try:
                day = df.loc[test_date]
                if isinstance(day, pd.Series):
                    day = day.to_frame().T
                all_dfs[fn] = day
            except KeyError:
                pass
        except Exception:
            pass

    print(f"成功加载: {len(all_dfs)} 因子")

    if name_a not in all_dfs or name_b not in all_dfs:
        print(f"目标因子 {name_a} 或 {name_b} 无数据!")
        return

    # 合并为大截面
    merged = pd.concat(all_dfs.values(), axis=1)
    loaded_names = list(merged.columns)
    print(f"截面: {merged.shape[0]} 股票 x {merged.shape[1]} 因子")

    # scipy 参考值
    valid = pd.concat([all_dfs[name_a], all_dfs[name_b]], axis=1).dropna()
    ref_corr, _ = stats.spearmanr(valid.iloc[:, 0], valid.iloc[:, 1])
    print(f"\nscipy 参考 Spearman: {ref_corr:.6f} (N={len(valid)})")

    # 增量测试：从 2 到 N 因子
    test_sizes = sorted(set([2, 3, 5, 10, 20, 50, 100, 150, 200, 250, 300,
                             400, 500, len(loaded_names)]))
    test_sizes = [s for s in test_sizes if s <= len(loaded_names)]

    print(f"\n{'K':>5} {'usable':>6} {'GEMM_corr':>12} {'scipy_ref':>12} {'diff':>10} {'status':>10}")
    print("-" * 60)

    break_point = None

    for size in test_sizes:
        # 确保目标因子在子集中
        subset = [name_a, name_b]
        for fn in loaded_names:
            if fn not in subset and len(subset) < size:
                subset.append(fn)

        sub_df = merged[subset]
        vc = sub_df.count()
        usable = vc[vc >= 30].index.tolist()

        if name_a not in usable or name_b not in usable:
            print(f"{size:5d} {len(usable):6d}   目标因子被排除")
            continue

        sub_clean = sub_df[usable]
        ranked = sub_clean.rank(method="average", na_option="keep")
        R = ranked.values
        nm = np.isnan(R)
        M = (~nm).astype(np.float64)
        X = np.where(nm, 0.0, R)

        cols = list(sub_clean.columns)
        ia = cols.index(name_a)
        ib = cols.index(name_b)

        mat = gemm_pearson_cpu(X, M)
        result = mat[ia, ib]
        diff = abs(result - ref_corr)
        is_bug = abs(result - 1.0) < 0.01 and abs(ref_corr - 1.0) > 0.1
        status = "*** BUG ***" if is_bug else ("OK" if diff < 0.05 else "DRIFT")

        if is_bug and break_point is None:
            break_point = size

        print(f"{size:5d} {len(usable):6d} {result:12.6f} {ref_corr:12.6f} {diff:10.6f} {status:>10}")

    # 如果找到 break point，做精细搜索
    if break_point is not None and break_point > 2:
        prev_size = [s for s in test_sizes if s < break_point][-1]
        print(f"\n精细搜索 break point: {prev_size} ~ {break_point}")
        for size in range(prev_size, break_point + 1):
            subset = [name_a, name_b]
            for fn in loaded_names:
                if fn not in subset and len(subset) < size:
                    subset.append(fn)

            sub_df = merged[subset]
            vc = sub_df.count()
            usable = vc[vc >= 30].index.tolist()

            if name_a not in usable or name_b not in usable:
                continue

            sub_clean = sub_df[usable]
            ranked = sub_clean.rank(method="average", na_option="keep")
            R = ranked.values
            nm = np.isnan(R)
            M_mat = (~nm).astype(np.float64)
            X_mat = np.where(nm, 0.0, R)

            cols = list(sub_clean.columns)
            ia = cols.index(name_a)
            ib = cols.index(name_b)

            mat = gemm_pearson_cpu(X_mat, M_mat)
            result = mat[ia, ib]
            is_bug = abs(result - 1.0) < 0.01 and abs(ref_corr - 1.0) > 0.1

            if is_bug:
                # 找到了！是哪个因子加入后导致的？
                print(f"  K={size}: {result:.6f} *** BUG 首次出现 ***")
                last_added = subset[-1]
                print(f"  最后加入的因子: {last_added}")

                # 检查这个因子的数据质量
                bad_factor_data = merged[last_added].dropna()
                print(f"  {last_added}: N={len(bad_factor_data)}, "
                      f"unique={bad_factor_data.nunique()}, "
                      f"zero_pct={((bad_factor_data == 0).sum()/len(bad_factor_data)*100):.1f}%")

                # 验证: 去掉这个因子后是否恢复
                subset_without = [x for x in subset if x != last_added]
                sub_df2 = merged[subset_without]
                vc2 = sub_df2.count()
                usable2 = vc2[vc2 >= 30].index.tolist()
                if name_a in usable2 and name_b in usable2:
                    sub_clean2 = sub_df2[usable2]
                    ranked2 = sub_clean2.rank(method="average", na_option="keep")
                    R2 = ranked2.values
                    nm2 = np.isnan(R2)
                    M2 = (~nm2).astype(np.float64)
                    X2 = np.where(nm2, 0.0, R2)
                    cols2 = list(sub_clean2.columns)
                    mat2 = gemm_pearson_cpu(X2, M2)
                    r2 = mat2[cols2.index(name_a), cols2.index(name_b)]
                    print(f"  去掉 {last_added} 后: corr={r2:.6f} "
                          f"{'恢复正常' if abs(r2 - ref_corr) < 0.05 else '仍异常'}")
                break
            else:
                if size % 10 == 0:
                    print(f"  K={size}: {result:.6f}")

    # 对所有假阳性对做快速验证
    if len(sample_pairs) > 1:
        print(f"\n其他假阳性对快速验证:")
        for name_a2, name_b2 in sample_pairs[1:]:
            if name_a2 not in all_dfs or name_b2 not in all_dfs:
                print(f"  {name_a2} <-> {name_b2}: 缺数据")
                continue
            valid2 = pd.concat([all_dfs[name_a2], all_dfs[name_b2]], axis=1).dropna()
            if len(valid2) < 30:
                print(f"  {name_a2} <-> {name_b2}: 有效数据不足 ({len(valid2)})")
                continue
            ref2, _ = stats.spearmanr(valid2.iloc[:, 0], valid2.iloc[:, 1])
            print(f"  {name_a2} <-> {name_b2}: scipy={ref2:.6f}")

    return break_point


# ═══════════════════════════════════════
#  PART 4: 深入 GEMM 中间值
# ═══════════════════════════════════════

def part4_gemm_internals(sample_pairs, all_names):
    """在 break point 前后，dump GEMM 每一步中间值"""
    print("\n" + "=" * 70)
    print("  PART 4: GEMM 中间值逐项对比")
    print("=" * 70)

    if not sample_pairs:
        return

    name_a, name_b = sample_pairs[0]

    # 加载测试日数据
    fp_a = os.path.join(SINGLE_DIR, f"{name_a}.parquet")
    df_a_full = pd.read_parquet(fp_a)
    if "value" in df_a_full.columns:
        df_a_full = df_a_full.rename(columns={"value": name_a})
    dates = df_a_full.index.get_level_values(0).unique().sort_values()
    test_date = dates[-10]

    # 只加载目标对 (2因子)
    fp_b = os.path.join(SINGLE_DIR, f"{name_b}.parquet")
    df_b_full = pd.read_parquet(fp_b)
    if "value" in df_b_full.columns:
        df_b_full = df_b_full.rename(columns={"value": name_b})

    try:
        sec_a = df_a_full.loc[test_date]
        sec_b = df_b_full.loc[test_date]
    except KeyError:
        print(f"测试日期 {test_date} 无数据")
        return

    merged_2 = pd.concat([sec_a, sec_b], axis=1)
    vc = merged_2.count()
    usable = vc[vc >= 30].index.tolist()
    sub = merged_2[usable]

    print(f"\n2因子模式 ({name_a}, {name_b}):")
    print(f"  截面: {sub.shape[0]} 股票")

    ranked = sub.rank(method="average", na_option="keep")
    R = ranked.values
    nm = np.isnan(R)
    M = (~nm).astype(np.float64)
    X = np.where(nm, 0.0, R)

    N_pairs = M.T @ M
    SX = X.T @ M
    SX2 = (X ** 2).T @ M
    SXY = X.T @ X

    ia = list(sub.columns).index(name_a)
    ib = list(sub.columns).index(name_b)

    print(f"  N_pairs[a,b] = {N_pairs[ia,ib]:.0f}")
    print(f"  SX[a,b] = {SX[ia,ib]:.2f}")
    print(f"  SX[b,a] = {SX[ib,ia]:.2f}")
    print(f"  SX2[a,b] = {SX2[ia,ib]:.2f}")
    print(f"  SXY[a,b] = {SXY[ia,ib]:.2f}")

    numerator = N_pairs * SXY - SX * SX.T
    var_x = N_pairs * SX2 - SX ** 2
    var_y = N_pairs * SX2.T - SX.T ** 2
    denom = np.sqrt(np.maximum(var_x * var_y, 0.0))

    print(f"  numerator = {numerator[ia,ib]:.4f}")
    print(f"  var_x = {var_x[ia,ib]:.4f}")
    print(f"  var_y = {var_y[ia,ib]:.4f}")
    print(f"  denominator = {denom[ia,ib]:.4f}")
    if denom[ia,ib] > 0:
        print(f"  corr = {numerator[ia,ib]/denom[ia,ib]:.6f}")

    # 同样操作，但模拟 FactorValueLoader 的 float32
    print(f"\n模拟 float32 加载:")
    merged_f32 = merged_2.astype(np.float32)
    sub_f32 = merged_f32[usable]
    ranked_f32 = sub_f32.rank(method="average", na_option="keep")
    R_f32 = ranked_f32.values
    nm_f32 = np.isnan(R_f32)
    M_f32 = (~nm_f32).astype(np.float64)
    X_f32 = np.where(nm_f32, 0.0, R_f32)

    N_pairs_f32 = M_f32.T @ M_f32
    SX_f32 = X_f32.T @ M_f32
    SXY_f32 = X_f32.T @ X_f32
    SX2_f32 = (X_f32 ** 2).T @ M_f32

    print(f"  N_pairs[a,b] = {N_pairs_f32[ia,ib]:.0f}")
    print(f"  SX[a,b] = {SX_f32[ia,ib]:.2f} (diff={abs(SX[ia,ib]-SX_f32[ia,ib]):.2f})")
    print(f"  SXY[a,b] = {SXY_f32[ia,ib]:.2f} (diff={abs(SXY[ia,ib]-SXY_f32[ia,ib]):.2f})")

    num_f32 = N_pairs_f32 * SXY_f32 - SX_f32 * SX_f32.T
    var_x_f32 = N_pairs_f32 * SX2_f32 - SX_f32 ** 2
    var_y_f32 = N_pairs_f32 * SX2_f32.T - SX_f32.T ** 2
    denom_f32 = np.sqrt(np.maximum(var_x_f32 * var_y_f32, 0.0))

    print(f"  numerator = {num_f32[ia,ib]:.4f} (diff={abs(numerator[ia,ib]-num_f32[ia,ib]):.4f})")
    print(f"  var_x = {var_x_f32[ia,ib]:.4f}")
    print(f"  denominator = {denom_f32[ia,ib]:.4f}")
    if denom_f32[ia,ib] > 0:
        print(f"  corr = {num_f32[ia,ib]/denom_f32[ia,ib]:.6f}")

    # NaN 引入差异: float32 可能让某些值变成完全相同 → tied ranks → 不同的 NaN 分布
    ranks_64 = sub[[name_a]].rank(method="average", na_option="keep").values.flatten()
    ranks_32 = sub_f32[[name_a]].rank(method="average", na_option="keep").values.flatten()
    valid_both = ~(np.isnan(ranks_64) | np.isnan(ranks_32))
    rank_diffs = np.abs(ranks_64[valid_both] - ranks_32[valid_both])
    print(f"\n  因子A排名 f64 vs f32: 最大差异={rank_diffs.max():.4f}, "
          f"非零差异数={np.sum(rank_diffs > 0)}")


# ═══════════════════════════════════════
#  PART 5: 退化因子全量扫描
# ═══════════════════════════════════════

def part5_degenerate_scan(all_names):
    print("\n" + "=" * 70)
    print("  PART 5: 退化因子全量扫描")
    print("=" * 70)

    # 随机取一天做快速扫描
    first_fn = all_names[0]
    fp = os.path.join(SINGLE_DIR, f"{first_fn}.parquet")
    if not os.path.isfile(fp):
        for fn in all_names:
            fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
            if os.path.isfile(fp):
                first_fn = fn
                break

    df = pd.read_parquet(fp, columns=[])
    dates = df.index.get_level_values(0).unique().sort_values()
    test_date = dates[-10]

    degenerate = []
    normal = []

    for fn in all_names:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if not os.path.isfile(fp):
            continue
        try:
            df = pd.read_parquet(fp)
            if "value" in df.columns:
                df = df.rename(columns={"value": fn})
            try:
                day = df.loc[test_date]
                vals = day.iloc[:, 0] if isinstance(day, pd.DataFrame) else day
                total = len(vals)
                nan_count = vals.isna().sum()
                valid = vals.dropna()
                n_valid = len(valid)
                if n_valid == 0:
                    degenerate.append((fn, "ALL_NAN", 0, 0, total))
                    continue
                n_unique = valid.nunique()
                zero_pct = (valid == 0).sum() / n_valid
                unique_ratio = n_unique / n_valid

                if zero_pct > 0.9 or n_unique < 10 or unique_ratio < 0.005:
                    degenerate.append((fn, "DEGENERATE", n_valid, n_unique,
                                      round(zero_pct * 100, 1)))
                else:
                    normal.append(fn)
            except KeyError:
                degenerate.append((fn, "NO_DATE", 0, 0, 0))
        except Exception as e:
            degenerate.append((fn, f"ERROR:{e}", 0, 0, 0))

    print(f"正常因子: {len(normal)}, 退化因子: {len(degenerate)}")
    print(f"\n退化因子列表:")
    print(f"{'因子名':<45} {'类型':<12} {'有效数':>6} {'唯一值':>6} {'零值%':>6}")
    print("-" * 80)
    for fn, reason, n_valid, n_unique, zero_pct in sorted(degenerate, key=lambda x: x[1]):
        print(f"{fn:<45} {reason:<12} {n_valid:>6} {n_unique:>6} {zero_pct:>6}")

    return degenerate


# ═══════════════════════════════════════
#  PART 6: EWMA 聚合诊断
# ═══════════════════════════════════════

def part6_ewma_trace(sample_pairs, all_names):
    """检查 EWMA 聚合阶段是否引入 bug"""
    print("\n" + "=" * 70)
    print("  PART 6: EWMA 聚合阶段诊断")
    print("=" * 70)

    if not sample_pairs:
        return

    name_a, name_b = sample_pairs[0]

    # 加载目标对的完整数据
    fp_a = os.path.join(SINGLE_DIR, f"{name_a}.parquet")
    fp_b = os.path.join(SINGLE_DIR, f"{name_b}.parquet")
    df_a = pd.read_parquet(fp_a)
    df_b = pd.read_parquet(fp_b)
    if "value" in df_a.columns:
        df_a = df_a.rename(columns={"value": name_a})
    if "value" in df_b.columns:
        df_b = df_b.rename(columns={"value": name_b})

    dates = df_a.index.get_level_values(0).unique().sort_values()
    window_dates = dates[-252:]

    # 逐日用 scipy 计算参考 Spearman
    daily_ref = []
    daily_gemm_2 = []  # 2因子 GEMM
    valid_dates = []

    for ts in window_dates:
        try:
            sec_a = df_a.loc[ts]
            sec_b = df_b.loc[ts]
        except KeyError:
            continue

        merged = pd.concat([sec_a, sec_b], axis=1)
        valid = merged.dropna()
        if len(valid) < 100:
            continue

        # scipy 参考
        ref, _ = stats.spearmanr(valid.iloc[:, 0], valid.iloc[:, 1])
        if np.isnan(ref):
            continue

        # 2因子 GEMM
        ranked = merged.rank(method="average", na_option="keep")
        R = ranked.values
        nm = np.isnan(R)
        M = (~nm).astype(np.float64)
        X = np.where(nm, 0.0, R)
        mat = gemm_pearson_cpu(X, M)
        gemm_val = mat[0, 1]

        daily_ref.append(ref)
        daily_gemm_2.append(gemm_val)
        valid_dates.append(ts.strftime("%Y-%m-%d"))

    print(f"有效天数: {len(daily_ref)}")

    # EWMA 聚合
    half_life = 125
    lam = 2.0 ** (-1.0 / half_life)
    T = len(daily_ref)
    weights = np.array([lam ** (T - 1 - t) for t in range(T)])

    ref_arr = np.array(daily_ref)
    gemm_arr = np.array(daily_gemm_2)

    ewma_ref = np.average(ref_arr, weights=weights)
    ewma_gemm = np.average(gemm_arr[~np.isnan(gemm_arr)],
                           weights=weights[~np.isnan(gemm_arr)])

    print(f"\n252天 EWMA 结果:")
    print(f"  scipy 参考: {ewma_ref:.6f}")
    print(f"  2因子 GEMM: {ewma_gemm:.6f}")
    print(f"  差异: {abs(ewma_ref - ewma_gemm):.6f}")

    # 日别差异统计
    diffs = np.abs(ref_arr - gemm_arr[~np.isnan(gemm_arr)][:len(ref_arr)])
    print(f"\n日别差异统计:")
    print(f"  均值: {diffs.mean():.6f}")
    print(f"  最大: {diffs.max():.6f}")
    print(f"  >0.01的天数: {(diffs > 0.01).sum()}")

    # 检查有效天数是否满足 min_days=126
    nan_gemm_count = np.isnan(gemm_arr).sum()
    print(f"\n2因子GEMM NaN天数: {nan_gemm_count}")
    print(f"有效天数 >= 126 (min_days): {'YES' if T - nan_gemm_count >= 126 else 'NO'}")


# ═══════════════════════════════════════
#  PART 7: 合并面板缓存检查
# ═══════════════════════════════════════

def part7_merged_cache_check():
    print("\n" + "=" * 70)
    print("  PART 7: 合并面板缓存检查")
    print("=" * 70)

    merged_path = os.path.join(SINGLE_DIR, "_merged_panel.parquet")
    if not os.path.isfile(merged_path):
        print("无合并缓存文件")
        return

    file_size = os.path.getsize(merged_path) / (1024**2)
    mtime = os.path.getmtime(merged_path)
    mtime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime))
    print(f"缓存文件: {merged_path}")
    print(f"大小: {file_size:.1f} MB")
    print(f"修改时间: {mtime_str}")

    # 读取 schema 检查
    import pyarrow.parquet as pq
    schema = pq.read_schema(merged_path)
    n_cols = len(schema.names)
    print(f"列数(因子数): {n_cols}")

    # 抽样检查：对比缓存 vs 原始 parquet
    print(f"\n缓存 vs 原始 parquet 数据对比:")
    cols_to_check = schema.names[:5]
    panel_cache = pd.read_parquet(merged_path, columns=cols_to_check)
    dates_cache = panel_cache.index.get_level_values(0)
    date_range = f"{dates_cache.min().date()} ~ {dates_cache.max().date()}"
    print(f"缓存日期范围: {date_range}")

    for col in cols_to_check:
        fp = os.path.join(SINGLE_DIR, f"{col}.parquet")
        if not os.path.isfile(fp):
            print(f"  {col}: 无原始 parquet!")
            continue
        df_orig = pd.read_parquet(fp)
        if "value" in df_orig.columns:
            df_orig = df_orig.rename(columns={"value": col})
        # 对齐日期范围
        d = df_orig.index.get_level_values(0)
        df_orig = df_orig.loc[(d >= dates_cache.min()) & (d <= dates_cache.max())]

        cache_vals = panel_cache[col]
        # reindex to common
        common_idx = cache_vals.index.intersection(df_orig.index)
        if len(common_idx) == 0:
            print(f"  {col}: 无共同索引!")
            continue

        v_cache = cache_vals.loc[common_idx].values.astype(np.float64)
        v_orig = df_orig.loc[common_idx].iloc[:, 0].values.astype(np.float64)

        both_valid = ~(np.isnan(v_cache) | np.isnan(v_orig))
        if both_valid.sum() == 0:
            print(f"  {col}: 全 NaN!")
            continue

        max_diff = np.abs(v_cache[both_valid] - v_orig[both_valid]).max()
        mean_diff = np.abs(v_cache[both_valid] - v_orig[both_valid]).mean()
        nan_mismatch = (np.isnan(v_cache) != np.isnan(v_orig)).sum()
        print(f"  {col}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}, "
              f"NaN不匹配={nan_mismatch}, 比较行数={both_valid.sum()}")


# ═══════════════════════════════════════
#  主函数
# ═══════════════════════════════════════

def main():
    print("*" * 70)
    print("  AIstock 相关性计算 Bug 精确诊断 v2")
    print(f"  时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("*" * 70)

    t0 = time.time()

    # PART 1: HDF5 分析
    names, matrix, false_pos, md5_map = part1_analyze_hdf5()

    # PART 2: 数据加载诊断
    sample_pairs, date_range = part2_data_loading(names, false_pos)

    # PART 3: GEMM 增量追踪 (核心!)
    break_point = part3_gemm_trace(sample_pairs, date_range, names)

    # PART 4: GEMM 中间值
    part4_gemm_internals(sample_pairs, names)

    # PART 5: 退化因子扫描
    degenerate = part5_degenerate_scan(names)

    # PART 6: EWMA 诊断
    part6_ewma_trace(sample_pairs, names)

    # PART 7: 合并缓存检查
    part7_merged_cache_check()

    elapsed = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"  诊断完成, 总耗时 {elapsed:.1f}s")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
