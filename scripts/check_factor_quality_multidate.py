"""检查 Earnings_Growth_Acceleration 在多个日期的数据质量"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np

from services.quantevolver.factor_value_loader import FactorValueLoader

def main():
    loader = FactorValueLoader(source="single")
    factor_name = "Earnings_Growth_Acceleration"

    print(f"=== {factor_name} 多日期数据质量检查 ===\n")

    # 检查最近10个交易日
    end_date = "2026-03-12"
    start_date = "2026-02-01"

    panel = loader.load_factor_panel(
        [factor_name],
        start_date=start_date,
        end_date=end_date
    )

    if panel is None or panel.empty:
        print("[ERROR] 数据为空")
        return

    dates = sorted(panel.index.get_level_values(0).unique())[-10:]

    print(f"检查最近 {len(dates)} 个交易日:\n")
    print(f"{'日期':<12} {'样本数':>6} {'零值占比':>8} {'非零数':>6} {'均值':>8} {'标准差':>8} {'最小值':>8} {'最大值':>8}")
    print("-" * 80)

    for date in dates:
        section = panel.loc[date]
        vals = section[factor_name].dropna().values

        zero_count = np.sum(vals == 0)
        zero_pct = zero_count / len(vals) * 100 if len(vals) > 0 else 0
        non_zero = len(vals) - zero_count

        print(f"{date.strftime('%Y-%m-%d'):<12} {len(vals):>6} {zero_pct:>7.1f}% {non_zero:>6} "
              f"{np.mean(vals):>8.4f} {np.std(vals):>8.4f} {np.min(vals):>8.2f} {np.max(vals):>8.2f}")

    # 检查整个时间段的统计
    print(f"\n=== 整体统计 ===")
    all_vals = panel[factor_name].dropna().values
    zero_count = np.sum(all_vals == 0)
    print(f"总样本数: {len(all_vals)}")
    print(f"零值样本数: {zero_count} ({zero_count/len(all_vals)*100:.1f}%)")
    print(f"非零样本数: {len(all_vals) - zero_count}")
    print(f"均值: {np.mean(all_vals):.4f}")
    print(f"标准差: {np.std(all_vals):.4f}")

if __name__ == "__main__":
    main()
