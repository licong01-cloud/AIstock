"""检查因子相关性异常 — 验证 factor_momentum_20d 和 factor_volatility_adj 的实际数据"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np
from scipy.stats import spearmanr

from services.quantevolver.factor_value_loader import FactorValueLoader

def main():
    loader = FactorValueLoader(source="single")

    factor_a = "Earnings_Growth_Acceleration"
    factor_b = "turnover_adjusted_momentum_10d"

    print(f"=== 检查因子数据异常 ===")
    print(f"因子A: {factor_a}")
    print(f"因子B: {factor_b}\n")

    # 加载最近2个月数据
    start_date = "2026-01-15"
    end_date = "2026-03-15"

    print(f"加载数据: {start_date} ~ {end_date}")
    df_a = loader.load_single_factor(factor_a, start_date=start_date, end_date=end_date)
    df_b = loader.load_single_factor(factor_b, start_date=start_date, end_date=end_date)

    if df_a is None:
        print(f"[ERROR] 因子A数据不存在")
        return
    if df_b is None:
        print(f"[ERROR] 因子B数据不存在")
        return

    print(f"[OK] 数据加载成功\n")

    # 1. 基础统计
    print("=== 1. 基础统计 ===")
    print(f"因子A shape: {df_a.shape}, 列: {list(df_a.columns)}")
    print(f"因子B shape: {df_b.shape}, 列: {list(df_b.columns)}\n")

    # 提取值列
    val_a = df_a[factor_a] if factor_a in df_a.columns else df_a.iloc[:, 0]
    val_b = df_b[factor_b] if factor_b in df_b.columns else df_b.iloc[:, 0]

    print(f"因子A非NaN样本数: {val_a.notna().sum()} / {len(val_a)} ({val_a.notna().sum()/len(val_a)*100:.1f}%)")
    print(f"因子B非NaN样本数: {val_b.notna().sum()} / {len(val_b)} ({val_b.notna().sum()/len(val_b)*100:.1f}%)")

    print(f"\n因子A统计:")
    print(val_a.describe())
    print(f"\n因子B统计:")
    print(val_b.describe())

    # 2. 数据完全相同检查
    print("\n=== 2. 数据一致性检查 ===")

    # 对齐索引
    common_idx = val_a.index.intersection(val_b.index)
    val_a_aligned = val_a.loc[common_idx]
    val_b_aligned = val_b.loc[common_idx]

    print(f"共同索引数: {len(common_idx)}")

    # 检查是否完全相同
    is_identical = val_a_aligned.equals(val_b_aligned)
    print(f"数据是否完全相同: {is_identical}")

    if not is_identical:
        # 计算差异
        diff = (val_a_aligned - val_b_aligned).abs()
        print(f"最大差异: {diff.max():.6f}")
        print(f"平均差异: {diff.mean():.6f}")
        print(f"差异>1e-10的样本数: {(diff > 1e-10).sum()}")

    # 3. 相关性计算
    print("\n=== 3. 相关性计算 ===")

    # 只用非NaN的配对样本
    mask = val_a_aligned.notna() & val_b_aligned.notna()
    valid_a = val_a_aligned[mask]
    valid_b = val_b_aligned[mask]

    print(f"有效配对样本数: {len(valid_a)}")

    if len(valid_a) >= 30:
        corr_pearson = valid_a.corr(valid_b)
        corr_spearman, _ = spearmanr(valid_a, valid_b, nan_policy='omit')
        print(f"Pearson 相关系数: {corr_pearson:.6f}")
        print(f"Spearman 相关系数: {corr_spearman:.6f}")
    else:
        print(f"[ERROR] 有效样本不足30个，无法计算可靠相关性")

    # 4. 按日期查看截面相关性
    print("\n=== 4. 截面相关性（最近5天）===")

    dates = sorted(df_a.index.get_level_values(0).unique())[-5:]
    for date in dates:
        try:
            sec_a = df_a.loc[date, factor_a] if factor_a in df_a.columns else df_a.loc[date].iloc[:, 0]
            sec_b = df_b.loc[date, factor_b] if factor_b in df_b.columns else df_b.loc[date].iloc[:, 0]

            mask = sec_a.notna() & sec_b.notna()
            if mask.sum() >= 30:
                corr, _ = spearmanr(sec_a[mask], sec_b[mask])
                print(f"{date.strftime('%Y-%m-%d')}: Spearman={corr:.4f}, 有效样本={mask.sum()}")
            else:
                print(f"{date.strftime('%Y-%m-%d')}: 有效样本不足 ({mask.sum()})")
        except KeyError:
            print(f"{date.strftime('%Y-%m-%d')}: 数据缺失")

    # 5. 数据样本展示
    print("\n=== 5. 数据样本（前10行）===")
    sample = pd.DataFrame({
        'factor_A': val_a_aligned.head(10),
        'factor_B': val_b_aligned.head(10),
        'diff': (val_a_aligned - val_b_aligned).abs().head(10)
    })
    print(sample)

if __name__ == "__main__":
    main()
