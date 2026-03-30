"""检查 Earnings_Growth_Acceleration 是否为季度更新因子"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np

from services.quantevolver.factor_value_loader import FactorValueLoader

def main():
    loader = FactorValueLoader(source="single")
    factor_name = "Earnings_Growth_Acceleration"

    print(f"=== {factor_name} 时间模式分析 ===\n")

    # 加载3个月数据
    end_date = "2026-03-12"
    start_date = "2025-12-01"

    panel = loader.load_factor_panel(
        [factor_name],
        start_date=start_date,
        end_date=end_date
    )

    dates = sorted(panel.index.get_level_values(0).unique())

    print(f"=== 非零值的时间分布 ===\n")

    non_zero_dates = []
    for date in dates:
        section = panel.loc[date]
        vals = section[factor_name].dropna().values
        non_zero_count = np.sum(vals != 0)

        if non_zero_count > 0:
            non_zero_dates.append({
                'date': date,
                'non_zero': non_zero_count,
                'total': len(vals),
                'pct': non_zero_count / len(vals) * 100
            })

    if non_zero_dates:
        print(f"共 {len(non_zero_dates)} 天有非零值:\n")
        for item in non_zero_dates[-20:]:  # 最近20天
            print(f"{item['date'].strftime('%Y-%m-%d')}: {item['non_zero']:>4} / {item['total']:>4} ({item['pct']:>5.2f}%)")

        # 检查非零值日期的间隔
        if len(non_zero_dates) > 1:
            print(f"\n=== 非零值日期间隔分析 ===")
            intervals = []
            for i in range(1, len(non_zero_dates)):
                days = (non_zero_dates[i]['date'] - non_zero_dates[i-1]['date']).days
                intervals.append(days)

            print(f"平均间隔: {np.mean(intervals):.1f} 天")
            print(f"最小间隔: {np.min(intervals)} 天")
            print(f"最大间隔: {np.max(intervals)} 天")
    else:
        print("[WARNING] 所有日期的值都是 0")

    # 检查值的变化模式
    print(f"\n=== 值的变化模式 ===")
    all_non_zero = []
    for date in dates:
        section = panel.loc[date]
        vals = section[factor_name].dropna().values
        non_zero_vals = vals[vals != 0]
        if len(non_zero_vals) > 0:
            all_non_zero.extend(non_zero_vals)

    if all_non_zero:
        print(f"非零值总数: {len(all_non_zero)}")
        print(f"唯一值数量: {len(np.unique(all_non_zero))}")
        print(f"均值: {np.mean(all_non_zero):.4f}")
        print(f"标准差: {np.std(all_non_zero):.4f}")

if __name__ == "__main__":
    main()
