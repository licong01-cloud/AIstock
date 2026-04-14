"""
诊断 Part 4: 定位 FactorValueLoader 列映射 bug。

已确认：
1. GEMM 算法正确
2. 重算也产生错误结果 → bug 在 GEMM 之前
3. 重算中 cp_cost_spread <-> momentum_20D = 1.0（完全不同的因子！）
4. 怀疑: _load_panel_from_singles 的 ThreadPoolExecutor + as_completed
   导致数据写入了错误的列位置

本脚本: 直接比较 FactorValueLoader 返回的面板和原始 parquet 数据。
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd
from scipy import stats

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"


def verify_panel_vs_raw():
    """直接比较 FactorValueLoader 面板 vs 原始 parquet"""
    from services.quantevolver.factor_value_loader import FactorValueLoader

    print("=" * 70)
    print("  Panel 列映射验证: FactorValueLoader vs 原始 Parquet")
    print("=" * 70)

    # 清除所有缓存
    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()

    test_factors = [
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
        "turnover_adjusted_momentum_10d",
    ]

    # 过滤可用
    available = [fn for fn in test_factors
                 if os.path.isfile(os.path.join(SINGLE_DIR, f"{fn}.parquet"))]
    print(f"可用因子: {len(available)}")

    loader = FactorValueLoader(source="single")

    # 找日期范围
    fp = os.path.join(SINGLE_DIR, f"{available[0]}.parquet")
    df = pd.read_parquet(fp, columns=[])
    dates = df.index.get_level_values(0).unique().sort_values()
    test_date = dates[-10].strftime("%Y-%m-%d")
    start_date = dates[-252].strftime("%Y-%m-%d")
    end_date = dates[-1].strftime("%Y-%m-%d")

    print(f"测试日期: {test_date}")
    print(f"面板范围: {start_date} ~ {end_date}")

    # 加载面板
    print(f"\n加载面板...")
    panel = loader.load_factor_panel(available, start_date, end_date)
    print(f"面板形状: {panel.shape}")
    print(f"面板列顺序: {list(panel.columns)}")
    print(f"输入顺序:   {available}")

    # 顺序是否一致？
    order_match = list(panel.columns) == available
    print(f"列顺序和输入一致: {order_match}")

    # 逐因子验证: 面板中的数据 vs 直接读取 parquet
    print(f"\n{'='*70}")
    print(f"  逐因子数据验证 (测试日 {test_date})")
    print(f"{'='*70}")

    ts = pd.Timestamp(test_date)
    try:
        panel_day = panel.loc[ts]
    except KeyError:
        print(f"面板中无 {test_date} 日期!")
        return

    print(f"面板当日截面: {panel_day.shape}")

    swap_detected = []

    for fn in available:
        # 直接读取原始 parquet
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        raw_df = pd.read_parquet(fp)
        if "value" in raw_df.columns:
            raw_df = raw_df.rename(columns={"value": fn})

        try:
            raw_day = raw_df.loc[ts]
            raw_vals = raw_day.iloc[:, 0] if isinstance(raw_day, pd.DataFrame) else raw_day
        except KeyError:
            print(f"  {fn}: 原始 parquet 无 {test_date}")
            continue

        # 面板中该列
        panel_vals = panel_day[fn]

        # 比较
        common = raw_vals.index.intersection(panel_vals.index)
        if len(common) == 0:
            print(f"  {fn}: 无共同索引")
            continue

        raw_c = raw_vals.loc[common].values.astype(np.float64)
        panel_c = panel_vals.loc[common].values.astype(np.float64)

        both_valid = ~(np.isnan(raw_c) | np.isnan(panel_c))
        if both_valid.sum() == 0:
            print(f"  {fn}: 全 NaN")
            continue

        max_diff = np.abs(raw_c[both_valid] - panel_c[both_valid]).max()
        mean_diff = np.abs(raw_c[both_valid] - panel_c[both_valid]).mean()
        corr = np.corrcoef(raw_c[both_valid], panel_c[both_valid])[0, 1]

        status = "OK" if max_diff < 1e-3 else "*** MISMATCH ***"
        print(f"  {fn:<45} max_diff={max_diff:.2e} corr={corr:.6f} {status}")

        if max_diff > 1e-3:
            swap_detected.append(fn)
            # 找它实际匹配的是哪个因子
            print(f"    面板中 '{fn}' 列的数据实际是哪个因子？")
            best_match = None
            best_corr = 0
            for other_fn in available:
                if other_fn == fn:
                    continue
                other_fp = os.path.join(SINGLE_DIR, f"{other_fn}.parquet")
                other_df = pd.read_parquet(other_fp)
                if "value" in other_df.columns:
                    other_df = other_df.rename(columns={"value": other_fn})
                try:
                    other_day = other_df.loc[ts]
                    other_vals = other_day.iloc[:, 0] if isinstance(other_day, pd.DataFrame) else other_day
                except KeyError:
                    continue
                other_common = panel_vals.index.intersection(other_vals.index)
                if len(other_common) < 100:
                    continue
                ov = other_vals.loc[other_common].values.astype(np.float64)
                pv = panel_vals.loc[other_common].values.astype(np.float64)
                bv = ~(np.isnan(ov) | np.isnan(pv))
                if bv.sum() < 100:
                    continue
                c = np.corrcoef(ov[bv], pv[bv])[0, 1]
                if abs(c) > abs(best_corr):
                    best_corr = c
                    best_match = other_fn
            if best_match:
                print(f"    → 面板列 '{fn}' 实际数据最匹配: '{best_match}' (corr={best_corr:.6f})")

    if swap_detected:
        print(f"\n{'='*70}")
        print(f"  列映射错误汇总: {len(swap_detected)} 个因子数据被放错位置!")
        print(f"  错误因子: {swap_detected}")
        print(f"{'='*70}")
    else:
        print(f"\n所有因子数据验证通过，无列映射错误")
        print(f"假阳性来源需要在其他环节查找")


def verify_panel_large_scale():
    """大规模验证: 585 因子面板"""
    from services.quantevolver.factor_value_loader import FactorValueLoader

    print(f"\n{'='*70}")
    print(f"  大规模面板验证 (585因子)")
    print(f"{'='*70}")

    FactorValueLoader.invalidate_single_cache()
    FactorValueLoader.invalidate_merged_cache()
    loader = FactorValueLoader(source="single")

    all_factors = loader.get_available_factors()
    print(f"可用因子: {len(all_factors)}")

    # 找日期范围
    fp = os.path.join(SINGLE_DIR, f"{all_factors[0]}.parquet")
    df = pd.read_parquet(fp, columns=[])
    dates = df.index.get_level_values(0).unique().sort_values()
    test_date = dates[-10]
    start_date = dates[-252].strftime("%Y-%m-%d")
    end_date = dates[-1].strftime("%Y-%m-%d")

    print(f"加载 {len(all_factors)} 因子面板...")
    import time
    t0 = time.time()
    panel = loader.load_factor_panel(all_factors, start_date, end_date)
    t1 = time.time()
    print(f"加载完成: {panel.shape}, 耗时 {t1-t0:.1f}s")
    print(f"面板列数: {len(panel.columns)}")

    # 随机抽样 30 个因子验证
    import random
    random.seed(42)
    sample_factors = random.sample(list(panel.columns), min(30, len(panel.columns)))

    ts = test_date
    try:
        panel_day = panel.loc[ts]
    except KeyError:
        print(f"面板中无 {ts}")
        return

    print(f"\n抽样验证 {len(sample_factors)} 个因子:")
    mismatch_count = 0
    mismatch_list = []

    for fn in sample_factors:
        fp = os.path.join(SINGLE_DIR, f"{fn}.parquet")
        if not os.path.isfile(fp):
            continue
        raw_df = pd.read_parquet(fp)
        if "value" in raw_df.columns:
            raw_df = raw_df.rename(columns={"value": fn})
        try:
            raw_day = raw_df.loc[ts]
            raw_vals = raw_day.iloc[:, 0] if isinstance(raw_day, pd.DataFrame) else raw_day
        except KeyError:
            continue

        panel_vals = panel_day[fn]
        common = raw_vals.index.intersection(panel_vals.index)
        if len(common) == 0:
            continue

        raw_c = raw_vals.loc[common].values.astype(np.float64)
        panel_c = panel_vals.loc[common].values.astype(np.float64)
        both_valid = ~(np.isnan(raw_c) | np.isnan(panel_c))
        if both_valid.sum() == 0:
            continue

        max_diff = np.abs(raw_c[both_valid] - panel_c[both_valid]).max()
        corr = np.corrcoef(raw_c[both_valid], panel_c[both_valid])[0, 1] if both_valid.sum() > 1 else 0

        if max_diff > 1e-3:
            mismatch_count += 1
            mismatch_list.append((fn, max_diff, corr))
            print(f"  {fn:<45} *** MISMATCH *** max_diff={max_diff:.2e}, corr={corr:.6f}")
        else:
            print(f"  {fn:<45} OK (max_diff={max_diff:.2e})")

    print(f"\n大规模验证结果: {mismatch_count}/{len(sample_factors)} 因子不匹配")
    if mismatch_list:
        print("不匹配因子:")
        for fn, md, c in mismatch_list:
            print(f"  {fn}: max_diff={md:.2e}, corr={c:.6f}")


def trace_load_order():
    """追踪 ThreadPoolExecutor 的加载顺序问题"""
    print(f"\n{'='*70}")
    print(f"  ThreadPoolExecutor 加载顺序追踪")
    print(f"{'='*70}")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    test_factors = [
        "CHIP_COST_DISPERSION", "cp_cost_spread",
        "BookToPrice_Ratio", "cost_pressure",
        "momentum_20D", "momentum_price_strength",
        "MOM5", "Momentum_5D_Return",
    ]

    available = [fn for fn in test_factors
                 if os.path.isfile(os.path.join(SINGLE_DIR, f"{fn}.parquet"))]

    # 模拟 _load_panel_from_singles 的加载逻辑
    fp = os.path.join(SINGLE_DIR, f"{available[0]}.parquet")
    df = pd.read_parquet(fp, columns=[])
    dates = df.index.get_level_values(0).unique().sort_values()
    sd = dates[-252]
    ed = dates[-1]

    master_index = df.index
    d = master_index.get_level_values(0)
    master_index = master_index[(d >= sd) & (d <= ed)].sort_values()

    N = len(master_index)
    K = len(available)

    print(f"模拟加载: {K} 因子, {N} 行")
    print(f"输入顺序: {available}")

    def _read_as_array(fname):
        fpath = os.path.join(SINGLE_DIR, f"{fname}.parquet")
        df = pd.read_parquet(fpath)
        if "value" in df.columns:
            df = df.rename(columns={"value": fname})
        idx_dates = df.index.get_level_values(0)
        df = df.loc[(idx_dates >= sd) & (idx_dates <= ed)]
        series = df.iloc[:, 0]
        if series.index.equals(master_index):
            return fname, series.values.astype(np.float32)
        else:
            return fname, series.reindex(master_index).values.astype(np.float32)

    # 方法1: 按 as_completed 顺序（和 FactorValueLoader 一样）
    data_async = np.full((N, K), np.nan, dtype=np.float32)
    loaded_async = []
    batch_results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_read_as_array, fn): fn for fn in available}
        for future in as_completed(futures):
            batch_results.append(future.result())

    for fname, arr in batch_results:
        if arr is not None:
            col_idx = len(loaded_async)
            data_async[:, col_idx] = arr
            loaded_async.append(fname)

    print(f"\nas_completed 顺序: {loaded_async}")

    # 方法2: 按输入顺序
    data_sync = np.full((N, K), np.nan, dtype=np.float32)
    loaded_sync = []
    for fn in available:
        fname, arr = _read_as_array(fn)
        if arr is not None:
            col_idx = len(loaded_sync)
            data_sync[:, col_idx] = arr
            loaded_sync.append(fname)

    print(f"顺序加载:       {loaded_sync}")

    # 构建 DataFrame 并比较
    df_async = pd.DataFrame(data_async[:, :len(loaded_async)],
                            index=master_index, columns=loaded_async)
    df_sync = pd.DataFrame(data_sync[:, :len(loaded_sync)],
                           index=master_index, columns=loaded_sync)

    # 验证列数据是否正确
    ts = dates[-10]
    print(f"\n验证日期: {ts.strftime('%Y-%m-%d')}")
    print(f"\n{'因子名':<40} {'async值(前3)':>25} {'sync值(前3)':>25} {'匹配':>5}")
    print("-" * 100)

    for fn in available:
        if fn in df_async.columns and fn in df_sync.columns:
            try:
                a_vals = df_async.loc[ts, fn]
                s_vals = df_sync.loc[ts, fn]
                if isinstance(a_vals, pd.Series):
                    a_sample = a_vals.dropna().values[:3]
                    s_sample = s_vals.dropna().values[:3]
                else:
                    a_sample = [a_vals]
                    s_sample = [s_vals]
                match = np.allclose(a_sample, s_sample, equal_nan=True) if len(a_sample) == len(s_sample) else False
                print(f"  {fn:<40} {str(a_sample)[:25]:>25} {str(s_sample)[:25]:>25} {'OK' if match else 'DIFF':>5}")
            except:
                print(f"  {fn:<40} 无法比较")

    # 关键: 检查 async 和 sync 的 Spearman 相关性是否一致
    print(f"\nSpearman 相关性对比 (async vs sync):")
    pairs = [
        ("CHIP_COST_DISPERSION", "cp_cost_spread"),
        ("BookToPrice_Ratio", "cost_pressure"),
        ("momentum_20D", "momentum_price_strength"),
        ("MOM5", "Momentum_5D_Return"),
    ]
    for a, b in pairs:
        if a in df_async.columns and b in df_async.columns:
            try:
                sec_async = df_async.loc[ts, [a, b]].dropna()
                sec_sync = df_sync.loc[ts, [a, b]].dropna()
                if len(sec_async) > 30 and len(sec_sync) > 30:
                    corr_async, _ = stats.spearmanr(sec_async[a], sec_async[b])
                    corr_sync, _ = stats.spearmanr(sec_sync[a], sec_sync[b])
                    match = abs(corr_async - corr_sync) < 0.001
                    print(f"  {a} <-> {b}: async={corr_async:.6f} sync={corr_sync:.6f} "
                          f"{'OK' if match else '*** DIFF ***'}")
            except:
                print(f"  {a} <-> {b}: 无法比较")


if __name__ == "__main__":
    verify_panel_vs_raw()
    trace_load_order()
    verify_panel_large_scale()
