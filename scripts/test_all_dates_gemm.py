"""测试所有日期的 GEMM 计算 - 找出异常值"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np
from scipy import stats

from services.quantevolver.factor_value_loader import FactorValueLoader

def gemm_pearson(X: np.ndarray, M: np.ndarray) -> np.ndarray:
    """GEMM Pearson 实现"""
    N_pairs = M.T @ M
    SX = X.T @ M
    SX2 = (X ** 2).T @ M
    SXY = X.T @ X

    numerator = N_pairs * SXY - SX * SX.T
    var_x = N_pairs * SX2 - SX ** 2
    var_y = N_pairs * SX2.T - SX.T ** 2
    denominator = np.sqrt(np.maximum(var_x * var_y, 0.0))

    valid_pair = (denominator > 0) & (N_pairs >= 30)
    sub_mat = np.where(valid_pair, numerator / denominator, np.nan)
    np.fill_diagonal(sub_mat, 1.0)
    return sub_mat

def main():
    loader = FactorValueLoader(source="single")

    factor_a = "Earnings_Growth_Acceleration"
    factor_b = "turnover_adjusted_momentum_10d"

    print(f"=== 测试所有日期的 GEMM 计算 ===")
    print(f"因子A: {factor_a}")
    print(f"因子B: {factor_b}\n")

    # 加载最近60天数据
    end_date = "2026-03-12"
    start_date = "2025-12-01"

    panel = loader.load_factor_panel(
        [factor_a, factor_b],
        start_date=start_date,
        end_date=end_date
    )

    if panel is None or panel.empty:
        print("[ERROR] 数据为空")
        return

    dates = sorted(panel.index.get_level_values(0).unique())
    print(f"总交易日数: {len(dates)}\n")

    anomalies = []

    for date in dates:
        try:
            section = panel.loc[date]
        except KeyError:
            continue

        valid = section.dropna()
        if len(valid) < 30:
            continue

        a_vals = valid[factor_a].values
        b_vals = valid[factor_b].values

        # Scipy Spearman
        corr_scipy, _ = stats.spearmanr(a_vals, b_vals)

        # GEMM Spearman
        a_rank = stats.rankdata(a_vals, method='average')
        b_rank = stats.rankdata(b_vals, method='average')
        R = np.column_stack([a_rank, b_rank])
        nan_mask = np.isnan(R)
        M = (~nan_mask).astype(np.float64)
        X = np.where(nan_mask, 0.0, R)

        corr_mat = gemm_pearson(X, M)
        corr_gemm = corr_mat[0, 1]

        # 检查差异
        diff = abs(corr_scipy - corr_gemm)

        if np.isnan(corr_gemm) or diff > 0.001 or abs(corr_gemm) > 0.5:
            anomalies.append({
                'date': date.strftime('%Y-%m-%d'),
                'n': len(valid),
                'scipy': corr_scipy,
                'gemm': corr_gemm,
                'diff': diff,
            })
            print(f"[ANOMALY] {date.strftime('%Y-%m-%d')}: "
                  f"scipy={corr_scipy:7.4f}, gemm={corr_gemm:7.4f}, "
                  f"diff={diff:.6f}, n={len(valid)}")

    print(f"\n=== 汇总 ===")
    print(f"总天数: {len(dates)}")
    print(f"异常天数: {len(anomalies)}")

    if anomalies:
        print(f"\n异常详情:")
        for a in anomalies:
            print(f"  {a['date']}: scipy={a['scipy']:.6f}, gemm={a['gemm']:.6f}, diff={a['diff']:.6f}")
    else:
        print(f"\n[OK] 所有日期的 GEMM 计算都正确")

if __name__ == "__main__":
    main()
