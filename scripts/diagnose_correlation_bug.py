"""诊断相关性计算 bug - 追踪每个计算步骤"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np
from scipy import stats

from services.quantevolver.factor_value_loader import FactorValueLoader

def main():
    loader = FactorValueLoader(source="single")

    factor_a = "Earnings_Growth_Acceleration"
    factor_b = "turnover_adjusted_momentum_10d"

    print(f"=== 诊断相关性计算 bug ===")
    print(f"因子A: {factor_a}")
    print(f"因子B: {factor_b}\n")

    # 加载最近60天数据
    end_date = "2026-03-12"
    start_date = "2025-12-01"

    print(f"加载数据: {start_date} ~ {end_date}")
    panel = loader.load_factor_panel(
        [factor_a, factor_b],
        start_date=start_date,
        end_date=end_date
    )

    if panel is None or panel.empty:
        print("[ERROR] 面板数据为空")
        return

    print(f"[OK] 面板数据: {panel.shape}\n")

    # 获取所有日期
    dates = sorted(panel.index.get_level_values(0).unique())
    print(f"总交易日数: {len(dates)}\n")

    # 逐日计算相关性
    print("=== 逐日相关性计算 ===")
    daily_results = []

    for i, date in enumerate(dates[-10:]):  # 只看最近10天
        try:
            section = panel.loc[date]
        except KeyError:
            continue

        # 去除 NaN
        valid = section.dropna()
        if len(valid) < 30:
            print(f"{date.strftime('%Y-%m-%d')}: 有效样本不足 ({len(valid)})")
            continue

        a_vals = valid[factor_a].values
        b_vals = valid[factor_b].values

        # 计算 Spearman 相关性
        corr_scipy, _ = stats.spearmanr(a_vals, b_vals)

        # 手动计算 Spearman (排名后的 Pearson)
        a_rank = stats.rankdata(a_vals, method='average')
        b_rank = stats.rankdata(b_vals, method='average')
        corr_manual = np.corrcoef(a_rank, b_rank)[0, 1]

        # 检查方差
        var_a = np.var(a_rank, ddof=1)
        var_b = np.var(b_rank, ddof=1)
        denominator = np.sqrt(var_a * var_b)

        daily_results.append({
            'date': date.strftime('%Y-%m-%d'),
            'n_stocks': len(valid),
            'corr_scipy': corr_scipy,
            'corr_manual': corr_manual,
            'var_a_rank': var_a,
            'var_b_rank': var_b,
            'denominator': denominator,
        })

        print(f"{date.strftime('%Y-%m-%d')}: "
              f"n={len(valid):4d}, "
              f"corr={corr_scipy:7.4f}, "
              f"var_a={var_a:10.2f}, "
              f"var_b={var_b:10.2f}, "
              f"denom={denominator:10.2f}")

    if not daily_results:
        print("[ERROR] 没有有效的日期数据")
        return

    # EWMA 聚合
    print(f"\n=== EWMA 聚合 (half_life=125) ===")
    half_life = 125
    lambda_val = 0.5 ** (1.0 / half_life)
    T = len(daily_results)

    weights = np.array([lambda_val ** (T - 1 - t) for t in range(T)])
    corrs = np.array([r['corr_scipy'] for r in daily_results])

    # 检查是否有 NaN
    nan_count = np.sum(np.isnan(corrs))
    if nan_count > 0:
        print(f"[WARNING] {nan_count}/{T} 天的相关性为 NaN")
        valid_mask = ~np.isnan(corrs)
        weighted_sum = np.sum(corrs[valid_mask] * weights[valid_mask])
        weight_sum = np.sum(weights[valid_mask])
    else:
        weighted_sum = np.sum(corrs * weights)
        weight_sum = np.sum(weights)

    ewma_corr = weighted_sum / weight_sum if weight_sum > 0 else np.nan

    print(f"权重总和: {weight_sum:.6f}")
    print(f"加权相关性总和: {weighted_sum:.6f}")
    print(f"EWMA 相关性: {ewma_corr:.6f}")

    # 简单平均对比
    simple_avg = np.nanmean(corrs)
    print(f"简单平均: {simple_avg:.6f}")

    # 检查是否有异常值
    print(f"\n=== 异常值检查 ===")
    print(f"最小相关性: {np.nanmin(corrs):.6f}")
    print(f"最大相关性: {np.nanmax(corrs):.6f}")
    print(f"标准差: {np.nanstd(corrs):.6f}")

    # 检查是否有接近 1.0 的值
    high_corr = corrs[np.abs(corrs) > 0.5]
    if len(high_corr) > 0:
        print(f"[WARNING] 发现 {len(high_corr)} 天的相关性 > 0.5:")
        for i, r in enumerate(daily_results):
            if abs(r['corr_scipy']) > 0.5:
                print(f"  {r['date']}: {r['corr_scipy']:.6f}")

if __name__ == "__main__":
    main()
