"""测试 Winsorize 对相关性计算的影响"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np
from scipy import stats

from services.quantevolver.factor_value_loader import FactorValueLoader

def winsorize_cross_section(section: pd.DataFrame, q: float = 0.025) -> pd.DataFrame:
    """Winsorize 实现（来自 correlation_engine.py）"""
    data = section.values.copy()
    valid_counts = np.sum(~np.isnan(data), axis=0)
    q_lo = q * 100
    q_hi = (1.0 - q) * 100
    lo = np.nanpercentile(data, q_lo, axis=0)
    hi = np.nanpercentile(data, q_hi, axis=0)
    skip = valid_counts < 10
    lo[skip] = -np.inf
    hi[skip] = np.inf
    data = np.clip(data, lo[np.newaxis, :], hi[np.newaxis, :])
    return pd.DataFrame(data, index=section.index, columns=section.columns)

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

    print(f"  N_pairs: {N_pairs[0,1]:.0f}")
    print(f"  var_x[0,1]: {var_x[0,1]:.2e}")
    print(f"  var_y[0,1]: {var_y[0,1]:.2e}")
    print(f"  denominator[0,1]: {denominator[0,1]:.2e}")

    valid_pair = (denominator > 0) & (N_pairs >= 30)
    sub_mat = np.where(valid_pair, numerator / denominator, np.nan)
    np.fill_diagonal(sub_mat, 1.0)
    return sub_mat

def main():
    loader = FactorValueLoader(source="single")

    factor_a = "Earnings_Growth_Acceleration"
    factor_b = "turnover_adjusted_momentum_10d"

    print(f"=== 测试 Winsorize 对相关性的影响 ===\n")

    # 测试一天的数据
    date = "2026-03-06"
    panel = loader.load_factor_panel(
        [factor_a, factor_b],
        start_date=date,
        end_date=date
    )

    section = panel.loc[pd.Timestamp(date)]
    print(f"日期: {date}")
    print(f"原始数据: {section.shape}\n")

    # 不做 Winsorize
    print("=== 不做 Winsorize ===")
    valid = section.dropna()
    a_vals = valid[factor_a].values
    b_vals = valid[factor_b].values
    a_rank = stats.rankdata(a_vals, method='average')
    b_rank = stats.rankdata(b_vals, method='average')
    R = np.column_stack([a_rank, b_rank])
    M = np.ones_like(R)
    X = R
    corr_mat_no_wins = gemm_pearson(X, M)
    print(f"  相关性: {corr_mat_no_wins[0,1]:.6f}\n")

    # 做 Winsorize
    print("=== 做 Winsorize (q=0.025) ===")
    section_w = winsorize_cross_section(section, q=0.025)
    valid_w = section_w.dropna()
    a_vals_w = valid_w[factor_a].values
    b_vals_w = valid_w[factor_b].values
    a_rank_w = stats.rankdata(a_vals_w, method='average')
    b_rank_w = stats.rankdata(b_vals_w, method='average')
    R_w = np.column_stack([a_rank_w, b_rank_w])
    M_w = np.ones_like(R_w)
    X_w = R_w
    corr_mat_wins = gemm_pearson(X_w, M_w)
    print(f"  相关性: {corr_mat_wins[0,1]:.6f}\n")

    # 检查 Winsorize 前后的数据变化
    print("=== Winsorize 前后对比 ===")
    print(f"样本数: {len(valid)} -> {len(valid_w)}")
    print(f"因子A 范围: [{a_vals.min():.2f}, {a_vals.max():.2f}] -> [{a_vals_w.min():.2f}, {a_vals_w.max():.2f}]")
    print(f"因子B 范围: [{b_vals.min():.2f}, {b_vals.max():.2f}] -> [{b_vals_w.min():.2f}, {b_vals_w.max():.2f}]")

if __name__ == "__main__":
    main()
