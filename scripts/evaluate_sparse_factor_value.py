"""评估稀疏因子的预测价值"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pandas as pd
import numpy as np
from scipy import stats

from services.quantevolver.factor_value_loader import FactorValueLoader

def main():
    loader = FactorValueLoader(source="single")
    factor_name = "Earnings_Growth_Acceleration"

    print(f"=== {factor_name} 预测价值评估 ===\n")

    # 加载因子数据和收益数据
    end_date = "2026-03-12"
    start_date = "2026-01-01"

    # 加载因子
    factor_panel = loader.load_factor_panel(
        [factor_name],
        start_date=start_date,
        end_date=end_date
    )

    # 加载收益（假设有 ret_1d 列）
    try:
        ret_panel = loader.load_factor_panel(
            ["ret_1d"],
            start_date=start_date,
            end_date=end_date
        )
    except:
        print("[ERROR] 无法加载收益数据 ret_1d")
        return

    dates = sorted(factor_panel.index.get_level_values(0).unique())

    print(f"=== 整体 IC 分析 ===\n")

    ic_all = []
    ic_non_zero = []
    ic_zero = []

    for date in dates[:-1]:  # 排除最后一天（没有未来收益）
        try:
            factor_today = factor_panel.loc[date, factor_name]
            ret_tomorrow = ret_panel.loc[dates[dates.index(date) + 1], "ret_1d"]

            # 对齐
            common_idx = factor_today.index.intersection(ret_tomorrow.index)
            f = factor_today.loc[common_idx]
            r = ret_tomorrow.loc[common_idx]

            # 去除 NaN
            valid = ~(f.isna() | r.isna())
            f_valid = f[valid]
            r_valid = r[valid]

            if len(f_valid) < 30:
                continue

            # 整体 IC
            ic, _ = stats.spearmanr(f_valid, r_valid)
            if not np.isnan(ic):
                ic_all.append(ic)

            # 非零值 IC
            non_zero_mask = f_valid != 0
            if non_zero_mask.sum() >= 10:
                ic_nz, _ = stats.spearmanr(f_valid[non_zero_mask], r_valid[non_zero_mask])
                if not np.isnan(ic_nz):
                    ic_non_zero.append(ic_nz)

            # 零值 IC
            zero_mask = f_valid == 0
            if zero_mask.sum() >= 30:
                ic_z, _ = stats.spearmanr(f_valid[zero_mask], r_valid[zero_mask])
                if not np.isnan(ic_z):
                    ic_zero.append(ic_z)

        except Exception as e:
            continue

    if ic_all:
        print(f"整体 IC:")
        print(f"  均值: {np.mean(ic_all):.6f}")
        print(f"  标准差: {np.std(ic_all):.6f}")
        print(f"  ICIR: {np.mean(ic_all) / np.std(ic_all):.4f}")
        print(f"  有效天数: {len(ic_all)}\n")

    if ic_non_zero:
        print(f"非零值 IC (只看有非零值的股票):")
        print(f"  均值: {np.mean(ic_non_zero):.6f}")
        print(f"  标准差: {np.std(ic_non_zero):.6f}")
        print(f"  ICIR: {np.mean(ic_non_zero) / np.std(ic_non_zero):.4f}")
        print(f"  有效天数: {len(ic_non_zero)}\n")

    if ic_zero:
        print(f"零值 IC (只看零值的股票):")
        print(f"  均值: {np.mean(ic_zero):.6f}")
        print(f"  标准差: {np.std(ic_zero):.6f}")
        print(f"  有效天数: {len(ic_zero)}\n")

    # 分层收益分析
    print(f"=== 分层收益分析 ===\n")

    group_rets = {
        'zero': [],
        'positive': [],
        'negative': []
    }

    for date in dates[:-1]:
        try:
            factor_today = factor_panel.loc[date, factor_name]
            ret_tomorrow = ret_panel.loc[dates[dates.index(date) + 1], "ret_1d"]

            common_idx = factor_today.index.intersection(ret_tomorrow.index)
            f = factor_today.loc[common_idx]
            r = ret_tomorrow.loc[common_idx]

            valid = ~(f.isna() | r.isna())
            f_valid = f[valid]
            r_valid = r[valid]

            # 分组
            zero_mask = f_valid == 0
            pos_mask = f_valid > 0
            neg_mask = f_valid < 0

            if zero_mask.sum() > 0:
                group_rets['zero'].append(r_valid[zero_mask].mean())
            if pos_mask.sum() > 0:
                group_rets['positive'].append(r_valid[pos_mask].mean())
            if neg_mask.sum() > 0:
                group_rets['negative'].append(r_valid[neg_mask].mean())

        except:
            continue

    for group, rets in group_rets.items():
        if rets:
            print(f"{group:>8} 组: 平均收益 = {np.mean(rets)*100:.4f}%, 天数 = {len(rets)}")

if __name__ == "__main__":
    main()
