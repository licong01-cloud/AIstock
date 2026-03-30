"""测试 GEMM Pearson 算法 - 重现 correlation=1.0 bug"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np
from scipy import stats

from services.quantevolver.factor_value_loader import FactorValueLoader

def gemm_pearson_reference(X: np.ndarray, M: np.ndarray) -> np.ndarray:
    """参考实现：correlation_engine.py 的 _gemm_pearson_cpu"""
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

def gemm_pearson_fixed(X: np.ndarray, M: np.ndarray) -> np.ndarray:
    """修复版本：增加 denominator 阈值检查"""
    N_pairs = M.T @ M
    SX = X.T @ M
    SX2 = (X ** 2).T @ M
    SXY = X.T @ X

    numerator = N_pairs * SXY - SX * SX.T
    var_x = N_pairs * SX2 - SX ** 2
    var_y = N_pairs * SX2.T - SX.T ** 2
    denominator = np.sqrt(np.maximum(var_x * var_y, 0.0))

    valid_pair = (denominator > 1e-8) & (N_pairs >= 30)
    sub_mat = np.where(valid_pair, numerator / denominator, np.nan)
    np.fill_diagonal(sub_mat, 1.0)
    return sub_mat

def main():
    loader = FactorValueLoader(source="single")

    factor_a = "Earnings_Growth_Acceleration"
    factor_b = "turnover_adjusted_momentum_10d"

    print(f"=== 测试 GEMM Pearson 算法 ===")
    print(f"因子A: {factor_a}")
    print(f"因子B: {factor_b}\n")

    # 加载一天的数据
    date = "2026-03-06"
    panel = loader.load_factor_panel(
        [factor_a, factor_b],
        start_date=date,
        end_date=date
    )

    if panel is None or panel.empty:
        print("[ERROR] 数据为空")
        return

    section = panel.loc[pd.Timestamp(date)]
    valid = section.dropna()

    print(f"日期: {date}")
    print(f"有效样本数: {len(valid)}\n")

    # 提取值并排名
    a_vals = valid[factor_a].values
    b_vals = valid[factor_b].values

    # Scipy Spearman 作为基准
    corr_scipy, _ = stats.spearmanr(a_vals, b_vals)
    print(f"Scipy Spearman: {corr_scipy:.6f}\n")

    # 手动排名
    a_rank = stats.rankdata(a_vals, method='average')
    b_rank = stats.rankdata(b_vals, method='average')

    # 构造 GEMM 输入
    R = np.column_stack([a_rank, b_rank])  # (N, 2)
    nan_mask = np.isnan(R)
    M = (~nan_mask).astype(np.float64)
    X = np.where(nan_mask, 0.0, R)

    print(f"=== GEMM 输入 ===")
    print(f"X shape: {X.shape}")
    print(f"M shape: {M.shape}")
    print(f"X 前5行:\n{X[:5]}")
    print(f"M 前5行:\n{M[:5]}\n")

    # 测试原始版本
    print(f"=== 原始 GEMM (denominator > 0) ===")
    corr_mat_orig = gemm_pearson_reference(X, M)
    print(f"相关性矩阵:\n{corr_mat_orig}")
    print(f"A vs B: {corr_mat_orig[0, 1]:.6f}\n")

    # 测试修复版本
    print(f"=== 修复 GEMM (denominator > 1e-8) ===")
    corr_mat_fixed = gemm_pearson_fixed(X, M)
    print(f"相关性矩阵:\n{corr_mat_fixed}")
    print(f"A vs B: {corr_mat_fixed[0, 1]:.6f}\n")

    # 详细诊断
    print(f"=== 详细诊断 ===")
    N_pairs = M.T @ M
    SX = X.T @ M
    SX2 = (X ** 2).T @ M
    SXY = X.T @ X

    numerator = N_pairs * SXY - SX * SX.T
    var_x = N_pairs * SX2 - SX ** 2
    var_y = N_pairs * SX2.T - SX.T ** 2
    denominator = np.sqrt(np.maximum(var_x * var_y, 0.0))

    print(f"N_pairs:\n{N_pairs}")
    print(f"\nSX:\n{SX}")
    print(f"\nSX2:\n{SX2}")
    print(f"\nSXY:\n{SXY}")
    print(f"\nnumerator:\n{numerator}")
    print(f"\nvar_x:\n{var_x}")
    print(f"\nvar_y:\n{var_y}")
    print(f"\ndenominator:\n{denominator}")
    print(f"\nnumerator[0,1] / denominator[0,1] = {numerator[0,1] / denominator[0,1]:.6f}")

if __name__ == "__main__":
    main()
