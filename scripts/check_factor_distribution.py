"""检查 Earnings_Growth_Acceleration 的分布"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np

from services.quantevolver.factor_value_loader import FactorValueLoader

def main():
    loader = FactorValueLoader(source="single")
    factor_name = "Earnings_Growth_Acceleration"

    print(f"=== {factor_name} 分布分析 ===\n")

    date = "2026-03-06"
    panel = loader.load_factor_panel(
        [factor_name],
        start_date=date,
        end_date=date
    )

    section = panel.loc[pd.Timestamp(date)]
    vals = section[factor_name].dropna().values

    print(f"样本数: {len(vals)}")
    print(f"均值: {np.mean(vals):.4f}")
    print(f"标准差: {np.std(vals):.4f}")
    print(f"最小值: {np.min(vals):.4f}")
    print(f"最大值: {np.max(vals):.4f}\n")

    # 分位数
    percentiles = [0, 1, 2.5, 5, 10, 25, 50, 75, 90, 95, 97.5, 99, 100]
    print("分位数分布:")
    for p in percentiles:
        val = np.percentile(vals, p)
        print(f"  {p:5.1f}%: {val:10.4f}")

    # 检查 0 值占比
    zero_count = np.sum(vals == 0)
    print(f"\n值为 0 的样本数: {zero_count} ({zero_count/len(vals)*100:.1f}%)")

    # 检查接近 0 的值
    near_zero = np.sum(np.abs(vals) < 0.01)
    print(f"值接近 0 (|x|<0.01) 的样本数: {near_zero} ({near_zero/len(vals)*100:.1f}%)")

if __name__ == "__main__":
    main()
