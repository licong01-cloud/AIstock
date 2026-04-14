"""
诊断 Part 5: 终极定位 — 逐步比较 compute_pairwise vs compute_full_matrix。

已确认:
1. GEMM 正确 (单日单独测试)
2. Panel 数据正确 (vs 原始 parquet)
3. 但 compute_full_matrix 产生错误的 EWMA 结果
4. HDF5 和 DB 中都存了错误值

本脚本:
- 用 compute_pairwise (scipy spearmanr) 作为参考
- 用 compute_full_matrix (GEMM) 比较
- 手动逐日追踪定位差异来源
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd
from scipy import stats
import time

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"


def test1_pairwise_vs_manual():
    """用 compute_pairwise 和手动计算对比，验证参考值"""
    from services.quantevolver.factor_value_loader import FactorValueLoader
    from services.quantevolver.correlation_engine import CorrelationEngine

    print("=" * 70)
    print("  Test 1: compute_pairwise vs 手动 scipy (参考值)")
    print("=" * 70)

    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()
    loader = FactorValueLoader(source="single")
    engine = CorrelationEngine(loader)

    pairs = [
        ("CHIP_COST_DISPERSION", "cp_cost_spread"),
        ("BookToPrice_Ratio", "cost_pressure"),
        ("momentum_20D", "momentum_price_strength"),
    ]

    for name_a, name_b in pairs:
        result = engine.compute_pairwise(name_a, name_b)
        print(f"\n  {name_a} <-> {name_b}:")
        print(f"    compute_pairwise: {result.correlation:.6f} ({result.effective_days} 天)")
        if result.daily_correlations:
            last5 = result.daily_correlations[-5:]
            print(f"    最近5天: {[(d, f'{c:.4f}') for d, c in last5]}")


def test2_full_matrix_small():
    """小规模 full_matrix 并逐步检查"""
    from services.quantevolver.factor_value_loader import FactorValueLoader
    from services.quantevolver.correlation_engine import CorrelationEngine

    print("\n" + "=" * 70)
    print("  Test 2: compute_full_matrix (3因子) vs compute_pairwise")
    print("=" * 70)

    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()
    loader = FactorValueLoader(source="single")
    engine = CorrelationEngine(loader)

    factors_3 = ["CHIP_COST_DISPERSION", "cp_cost_spread", "BookToPrice_Ratio"]

    # full_matrix
    result_full = engine.compute_full_matrix(factors_3, save_hdf5=False)
    print(f"  factor_names: {result_full.factor_names}")
    print(f"  effective_window: {result_full.effective_window}")

    for i in range(len(result_full.factor_names)):
        for j in range(i+1, len(result_full.factor_names)):
            val = result_full.matrix[i, j]
            print(f"  full_matrix[{result_full.factor_names[i]}, {result_full.factor_names[j]}] = {val:.6f}")

    # pairwise 对比
    for i in range(len(result_full.factor_names)):
        for j in range(i+1, len(result_full.factor_names)):
            pw = engine.compute_pairwise(result_full.factor_names[i], result_full.factor_names[j])
            full_val = result_full.matrix[i, j]
            diff = abs(full_val - pw.correlation)
            status = "OK" if diff < 0.01 else "*** MISMATCH ***"
            print(f"  {result_full.factor_names[i]} <-> {result_full.factor_names[j]}: "
                  f"full={full_val:.6f} pair={pw.correlation:.6f} diff={diff:.6f} {status}")


def test3_incremental_growth():
    """从3因子逐步增加到20因子，每步验证"""
    from services.quantevolver.factor_value_loader import FactorValueLoader
    from services.quantevolver.correlation_engine import CorrelationEngine

    print("\n" + "=" * 70)
    print("  Test 3: 逐步增加因子数，观察 compute_full_matrix 结果变化")
    print("=" * 70)

    target_pair = ("CHIP_COST_DISPERSION", "cp_cost_spread")

    factors_list = [
        "CHIP_COST_DISPERSION", "cp_cost_spread",
        "BookToPrice_Ratio", "cost_pressure",
        "ChipCostIndustryDivergence", "ChipPressureFactor",
        "DividendToFreeTurnover_Ratio",
        "momentum_20D", "momentum_price_strength",
        "MOM5", "Momentum_5D_Return",
        "m_free_turnover_rate",
        "IntradayVolatility_5D",
        "mf_net_ratio", "chip_concentration_index",
        "m_atr_compression",
    ]

    available = [fn for fn in factors_list
                 if os.path.isfile(os.path.join(SINGLE_DIR, f"{fn}.parquet"))]

    # 先算 pairwise 参考值
    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()
    loader = FactorValueLoader(source="single")
    engine = CorrelationEngine(loader)

    pw = engine.compute_pairwise(target_pair[0], target_pair[1])
    print(f"  pairwise 参考: {pw.correlation:.6f} ({pw.effective_days} 天)")

    # 逐步增加因子数
    print(f"\n  {'K':>3} {'因子列表':>60} {'full_matrix_corr':>16} {'diff_vs_pair':>12}")
    print("  " + "-" * 95)

    for k in range(2, len(available) + 1):
        # 确保目标对在子集中
        subset = list(target_pair)
        for fn in available:
            if fn not in subset and len(subset) < k:
                subset.append(fn)

        FactorValueLoader.invalidate_single_cache()
        FactorValueLoader.invalidate_merged_cache()
        loader = FactorValueLoader(source="single")
        engine = CorrelationEngine(loader)

        result = engine.compute_full_matrix(subset, save_hdf5=False)

        # 找目标对在结果中的位置
        fn = result.factor_names
        if target_pair[0] in fn and target_pair[1] in fn:
            ia = fn.index(target_pair[0])
            ib = fn.index(target_pair[1])
            full_val = result.matrix[ia, ib]
            diff = abs(full_val - pw.correlation)
            status = " *** MISMATCH ***" if diff > 0.01 else ""
            factors_str = ",".join(fn)
            if len(factors_str) > 60:
                factors_str = factors_str[:57] + "..."
            print(f"  {k:3d} {factors_str:>60} {full_val:16.6f} {diff:12.6f}{status}")
        else:
            print(f"  {k:3d} 目标因子被排除")


def test4_daily_comparison():
    """逐日比较: full_matrix 的日相关性 vs scipy 参考"""
    from services.quantevolver.factor_value_loader import FactorValueLoader
    from services.quantevolver.correlation_engine import CorrelationEngine

    print("\n" + "=" * 70)
    print("  Test 4: 逐日比较 full_matrix 日相关 vs scipy 参考")
    print("=" * 70)

    target_pair = ("CHIP_COST_DISPERSION", "cp_cost_spread")
    extra_factors = ["BookToPrice_Ratio", "momentum_20D", "MOM5"]

    all_factors = list(target_pair) + extra_factors
    available = [fn for fn in all_factors
                 if os.path.isfile(os.path.join(SINGLE_DIR, f"{fn}.parquet"))]

    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()
    loader = FactorValueLoader(source="single")

    # 加载面板
    _, end_date = loader.get_date_range()
    all_dates = loader.get_trading_dates("2000-01-01", end_date)
    window_dates = all_dates[-252:]
    start_date = window_dates[0]

    panel = loader.load_factor_panel(available, start_date, end_date)
    print(f"  面板: {panel.shape}, 列: {list(panel.columns)}")

    ia_name, ib_name = target_pair
    if ia_name not in panel.columns or ib_name not in panel.columns:
        print(f"  目标因子不在面板中!")
        return

    # 逐日计算两种方式
    print(f"\n  日期         N_stocks  scipy_spearman  gemm_full  diff     status")
    print("  " + "-" * 80)

    from services.quantevolver.correlation_engine import CorrelationEngine
    engine = CorrelationEngine(loader)
    K = panel.shape[1]

    mismatch_days = []

    for date_str in window_dates[-20:]:  # 最近20天
        ts = pd.Timestamp(date_str)
        try:
            section = panel.loc[ts]
        except KeyError:
            continue

        if len(section) < 100:
            continue

        # scipy 参考 (只用目标对)
        pair_data = section[[ia_name, ib_name]].dropna()
        if len(pair_data) < 30:
            continue
        ref_corr, _ = stats.spearmanr(pair_data[ia_name], pair_data[ib_name])

        # Winsorize + GEMM (全量因子)
        section_w = engine._winsorize_cross_section(section)
        corr_mat = engine._cross_sectional_spearman_gemm(section_w)

        if corr_mat is None:
            continue

        col_names = list(section_w.columns)
        if ia_name in col_names and ib_name in col_names:
            ia = col_names.index(ia_name)
            ib = col_names.index(ib_name)
            gemm_val = corr_mat[ia, ib]
        else:
            gemm_val = float('nan')

        diff = abs(ref_corr - gemm_val)
        status = "OK" if diff < 0.01 else "*** MISMATCH ***"
        if diff > 0.01:
            mismatch_days.append((date_str, ref_corr, gemm_val))

        print(f"  {date_str}  {len(section):5d}    {ref_corr:14.6f}  {gemm_val:9.6f}  {diff:.6f}  {status}")

    if mismatch_days:
        print(f"\n  不匹配天数: {len(mismatch_days)}")
        # 深入分析第一个不匹配天
        date_str, ref_corr, gemm_val = mismatch_days[0]
        print(f"\n  深入分析: {date_str}")
        ts = pd.Timestamp(date_str)
        section = panel.loc[ts]
        section_w = engine._winsorize_cross_section(section)

        col_names = list(section_w.columns)
        ia = col_names.index(ia_name)
        ib = col_names.index(ib_name)

        # 检查 winsorize 是否改变了排名
        raw_rank_a = section[ia_name].rank(method="average", na_option="keep")
        raw_rank_b = section[ib_name].rank(method="average", na_option="keep")
        win_rank_a = section_w[ia_name].rank(method="average", na_option="keep")
        win_rank_b = section_w[ib_name].rank(method="average", na_option="keep")

        common_valid = raw_rank_a.notna() & win_rank_a.notna()
        rank_diff_a = (raw_rank_a[common_valid] - win_rank_a[common_valid]).abs().max()
        rank_diff_b = (raw_rank_b[common_valid] - win_rank_b[common_valid]).abs().max()

        print(f"    因子A rank差异 (raw vs winsorized): {rank_diff_a:.2f}")
        print(f"    因子B rank差异 (raw vs winsorized): {rank_diff_b:.2f}")

        # Winsorize 后的 scipy spearman
        win_pair = section_w[[ia_name, ib_name]].dropna()
        win_corr, _ = stats.spearmanr(win_pair[ia_name], win_pair[ib_name])
        print(f"    Raw scipy: {ref_corr:.6f}")
        print(f"    Winsorized scipy: {win_corr:.6f}")
        print(f"    GEMM: {gemm_val:.6f}")

        # GEMM 中间值
        valid_counts = section_w.count()
        usable_cols = valid_counts[valid_counts >= 30].index
        sub = section_w[usable_cols]
        ranked = sub.rank(method="average", na_option="keep")
        R = ranked.values
        nm = np.isnan(R)
        M = (~nm).astype(np.float64)
        X = np.where(nm, 0.0, R)

        sub_cols = list(sub.columns)
        if ia_name in sub_cols and ib_name in sub_cols:
            ia_sub = sub_cols.index(ia_name)
            ib_sub = sub_cols.index(ib_name)

            N_pairs = M.T @ M
            SX = X.T @ M
            SX2 = (X ** 2).T @ M
            SXY = X.T @ X

            n = N_pairs[ia_sub, ib_sub]
            sx_ab = SX[ia_sub, ib_sub]
            sx_ba = SX[ib_sub, ia_sub]
            sxy = SXY[ia_sub, ib_sub]
            sx2_ab = SX2[ia_sub, ib_sub]
            sx2_ba = SX2[ib_sub, ia_sub]

            print(f"\n    GEMM 中间值:")
            print(f"      N_pairs = {n:.0f}")
            print(f"      SX[a,b] = {sx_ab:.2f}  (sum of rank_a where both valid)")
            print(f"      SX[b,a] = {sx_ba:.2f}  (sum of rank_b where both valid)")
            print(f"      SXY = {sxy:.2f}")

            # 关键: 检查 SX[i,j] 是否正确
            # SX[i,j] = X[:, i].T @ M[:, j] = sum(rank_i * mask_j)
            # 这是因子 i 的排名乘以因子 j 的有效性掩码的总和
            # 对于完全有效的因子, SX[a,b] = sum(rank_a) = n*(n+1)/2
            expected_sx = n * (n + 1) / 2
            print(f"      期望 SX (if all valid) = {expected_sx:.2f}")
            print(f"      实际 SX[a,b] = {sx_ab:.2f}")
            print(f"      差异 = {abs(sx_ab - expected_sx):.2f}")

            # SX[i,j] 和 SX[j,i] 的含义:
            # SX[a,b] = sum(rank_a[s] for s where mask_b[s]=1)
            # SX[b,a] = sum(rank_b[s] for s where mask_a[s]=1)
            # 如果 a 和 b 有相同的 NaN 模式, 则
            # SX[a,b] = sum(rank_a) 和 SX[b,a] = sum(rank_b)

            numerator = n * sxy - sx_ab * sx_ba
            var_x = n * sx2_ab - sx_ab ** 2
            var_y = n * sx2_ba - sx_ba ** 2
            denominator = np.sqrt(max(var_x * var_y, 0))
            if denominator > 0:
                manual_corr = numerator / denominator
            else:
                manual_corr = float('nan')
            print(f"      手动 GEMM corr = {manual_corr:.6f}")
    else:
        print(f"\n  所有天数一致! Bug 不在单日 GEMM 阶段。")
        print(f"  Bug 可能在 EWMA 聚合阶段。")

        # 验证 EWMA 聚合
        print(f"\n  验证 EWMA 聚合...")
        daily_corrs_ref = []
        daily_corrs_gemm = []
        valid_dates_list = []

        for date_str in window_dates:
            ts = pd.Timestamp(date_str)
            try:
                section = panel.loc[ts]
            except KeyError:
                continue
            if len(section) < 100:
                continue

            # scipy 参考
            pair_data = section[[ia_name, ib_name]].dropna()
            if len(pair_data) < 30:
                continue
            ref_corr, _ = stats.spearmanr(pair_data[ia_name], pair_data[ib_name])
            if np.isnan(ref_corr):
                continue

            # GEMM
            section_w = engine._winsorize_cross_section(section)
            corr_mat = engine._cross_sectional_spearman_gemm(section_w)
            if corr_mat is None:
                continue

            col_names = list(section_w.columns)
            ia = col_names.index(ia_name)
            ib = col_names.index(ib_name)
            gemm_val = corr_mat[ia, ib]

            if np.isnan(gemm_val):
                continue

            daily_corrs_ref.append(ref_corr)
            daily_corrs_gemm.append(gemm_val)
            valid_dates_list.append(date_str)

        T = len(daily_corrs_ref)
        half_life = 125
        lam = 2.0 ** (-1.0 / half_life)
        weights = np.array([lam ** (T - 1 - t) for t in range(T)])

        ewma_ref = np.average(daily_corrs_ref, weights=weights)
        ewma_gemm = np.average(daily_corrs_gemm, weights=weights)

        print(f"  有效天数: {T}")
        print(f"  EWMA (scipy ref): {ewma_ref:.6f}")
        print(f"  EWMA (GEMM):      {ewma_gemm:.6f}")
        print(f"  差异: {abs(ewma_ref - ewma_gemm):.6f}")

        # 日别差异统计
        diffs = np.abs(np.array(daily_corrs_ref) - np.array(daily_corrs_gemm))
        print(f"  日别差异: mean={diffs.mean():.6f}, max={diffs.max():.6f}, >0.01={(diffs>0.01).sum()}")


if __name__ == "__main__":
    t0 = time.time()
    test1_pairwise_vs_manual()
    test2_full_matrix_small()
    test3_incremental_growth()
    test4_daily_comparison()
    print(f"\n总耗时: {time.time()-t0:.1f}s")
