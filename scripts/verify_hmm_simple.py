#!/usr/bin/env python
"""HMM 系数效果验证脚本 - 纯 Python 实现，无需 QE 实验.

原理:
1. 加载 HMM 系数文件
2. 加载股票收益数据
3. 应用 HMM 系数调整权重
4. 计算组合收益
5. 对比旧版本 vs 新版本

优点:
- 快速: 5-10 分钟
- 简单: 纯 Python，无需 WSL/RDAgent
- 直接: 只验证 HMM 系数的增量效果
"""
import sys
import json
import numpy as np
import pandas as pd
import psycopg2
from datetime import date, datetime
from typing import Dict, List, Tuple

sys.stdout.reconfigure(encoding='utf-8')

# 配置
OLD_COEFF_PATH = "backend/data/hmm_models/564b407f-1541-4b18-a087-2a45cfbca9d9/2026-04-04/coefficients_preset_A_2024-07-01_2026-03-03.json"
NEW_COEFF_PATH = "backend/data/hmm_models/b2d5bcc6-8463-4156-bf1a-e1392a00279a/2026-04-27/coefficients_preset_A_2026-01-26_2026-04-24.json"

DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 5432,
    'database': 'aistock',
    'user': 'postgres',
    'password': 'lc78080808',
}


def load_coefficients(coeff_path: str) -> Dict:
    """加载 HMM 系数文件."""
    with open(coeff_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_stock_returns(start_date: str, end_date: str) -> pd.DataFrame:
    """从数据库加载股票收益数据.

    Returns:
        DataFrame with columns: trade_date, ts_code, pct_chg, sector_code
    """
    conn = psycopg2.connect(**DB_CONFIG)

    query = """
        SELECT
            d.trade_date,
            d.ts_code,
            d.pct_chg,
            sm.sector_code
        FROM market.stock_daily d
        LEFT JOIN market.stock_sector_mapping sm
            ON d.ts_code = sm.ts_code AND sm.sector_level = 'L2'
        WHERE d.trade_date >= %s
            AND d.trade_date <= %s
            AND d.pct_chg IS NOT NULL
        ORDER BY d.trade_date, d.ts_code
    """

    df = pd.read_sql(query, conn, params=(start_date, end_date))
    conn.close()

    return df


def apply_hmm_coefficients(
    returns_df: pd.DataFrame,
    coefficients: Dict,
) -> pd.DataFrame:
    """应用 HMM 系数调整股票权重.

    Args:
        returns_df: 股票收益数据
        coefficients: HMM 系数字典

    Returns:
        DataFrame with additional column: hmm_adjusted_return
    """
    daily_coeffs = coefficients['daily_coefficients']
    stock_sector_map = coefficients['stock_sector_map']

    # 为每只股票添加 HMM 系数
    def get_hmm_coeff(row):
        trade_date = row['trade_date'].strftime('%Y-%m-%d')
        ts_code = row['ts_code']

        # 获取股票所属行业
        sector_code = stock_sector_map.get(ts_code)
        if not sector_code:
            return 1.0  # 无行业映射，使用中性系数

        # 获取该日期该行业的系数
        if trade_date not in daily_coeffs:
            return 1.0

        return daily_coeffs[trade_date].get(sector_code, 1.0)

    returns_df['hmm_coeff'] = returns_df.apply(get_hmm_coeff, axis=1)
    returns_df['hmm_adjusted_return'] = returns_df['pct_chg'] * returns_df['hmm_coeff']

    return returns_df


def calculate_portfolio_metrics(
    returns_df: pd.DataFrame,
    return_col: str = 'pct_chg',
    top_n: int = 50,
) -> Dict:
    """计算组合收益指标.

    策略: 每日选择收益最高的 top_n 只股票等权持有

    Args:
        returns_df: 股票收益数据
        return_col: 用于排序的收益列 ('pct_chg' 或 'hmm_adjusted_return')
        top_n: 每日持仓数量

    Returns:
        指标字典: annual_return, sharpe, max_drawdown, win_rate, etc.
    """
    # 按日期分组，选择 top_n
    daily_returns = []

    for trade_date, group in returns_df.groupby('trade_date'):
        # 按调整后收益排序，选择 top_n
        top_stocks = group.nlargest(top_n, return_col)

        # 等权组合收益
        portfolio_return = top_stocks['pct_chg'].mean()
        daily_returns.append({
            'trade_date': trade_date,
            'return': portfolio_return,
        })

    daily_df = pd.DataFrame(daily_returns)
    daily_df['cumulative'] = (1 + daily_df['return'] / 100).cumprod()

    # 计算指标
    total_return = daily_df['cumulative'].iloc[-1] - 1
    trading_days = len(daily_df)
    annual_return = (1 + total_return) ** (252 / trading_days) - 1

    daily_std = daily_df['return'].std()
    sharpe = (daily_df['return'].mean() / daily_std * np.sqrt(252)) if daily_std > 0 else 0

    # 最大回撤
    cumulative = daily_df['cumulative']
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    max_drawdown = drawdown.min()

    # 胜率
    win_rate = (daily_df['return'] > 0).sum() / len(daily_df)

    return {
        'annual_return': annual_return * 100,
        'sharpe': sharpe,
        'max_drawdown': max_drawdown * 100,
        'win_rate': win_rate * 100,
        'total_return': total_return * 100,
        'trading_days': trading_days,
    }


def compare_versions(
    old_coeff_path: str,
    new_coeff_path: str,
    start_date: str = None,
    end_date: str = None,
) -> None:
    """对比两个版本的 HMM 系数效果."""

    print("="*100)
    print("HMM 系数效果验证 - 快速脚本")
    print("="*100)

    # 1. 加载系数
    print("\n1. 加载 HMM 系数文件...")
    old_coeff = load_coefficients(old_coeff_path)
    new_coeff = load_coefficients(new_coeff_path)

    print(f"   旧版本: {old_coeff['test_start']} ~ {old_coeff['backtest_end']}")
    print(f"   新版本: {new_coeff['test_start']} ~ {new_coeff['backtest_end']}")

    # 确定重叠时间段
    old_dates = set(old_coeff['daily_coefficients'].keys())
    new_dates = set(new_coeff['daily_coefficients'].keys())
    overlap_dates = sorted(old_dates & new_dates)

    if not overlap_dates:
        print("\n❌ 错误: 两个版本没有重叠的时间段")
        return

    test_start = overlap_dates[0]
    test_end = overlap_dates[-1]

    print(f"\n   重叠时间段: {test_start} ~ {test_end} ({len(overlap_dates)} 天)")

    # 2. 加载股票收益数据
    print("\n2. 加载股票收益数据...")
    returns_df = load_stock_returns(test_start, test_end)
    print(f"   加载完成: {len(returns_df)} 条记录")
    print(f"   股票数: {returns_df['ts_code'].nunique()}")
    print(f"   交易日: {returns_df['trade_date'].nunique()}")

    # 3. Baseline: 不使用 HMM
    print("\n3. 计算 Baseline (不使用 HMM)...")
    baseline_metrics = calculate_portfolio_metrics(returns_df, return_col='pct_chg')

    # 4. 旧版本 HMM
    print("\n4. 计算旧版本 HMM 效果...")
    old_df = apply_hmm_coefficients(returns_df.copy(), old_coeff)
    old_metrics = calculate_portfolio_metrics(old_df, return_col='hmm_adjusted_return')

    # 5. 新版本 HMM
    print("\n5. 计算新版本 HMM 效果...")
    new_df = apply_hmm_coefficients(returns_df.copy(), new_coeff)
    new_metrics = calculate_portfolio_metrics(new_df, return_col='hmm_adjusted_return')

    # 6. 对比结果
    print("\n" + "="*100)
    print("回测结果对比")
    print("="*100)

    print(f"\n测试时间段: {test_start} ~ {test_end} ({baseline_metrics['trading_days']} 天)")
    print(f"持仓策略: 每日等权持有 top 50 股票")

    print(f"\n{'指标':20s} | {'Baseline':12s} | {'旧版本 HMM':12s} | {'新版本 HMM':12s} | {'改进':12s}")
    print("-" * 80)

    metrics = [
        ('年化收益率 (%)', 'annual_return'),
        ('Sharpe 比率', 'sharpe'),
        ('最大回撤 (%)', 'max_drawdown'),
        ('胜率 (%)', 'win_rate'),
        ('累计收益 (%)', 'total_return'),
    ]

    for name, key in metrics:
        baseline_val = baseline_metrics[key]
        old_val = old_metrics[key]
        new_val = new_metrics[key]
        improvement = new_val - old_val

        if key == 'sharpe':
            print(f"{name:20s} | {baseline_val:12.3f} | {old_val:12.3f} | {new_val:12.3f} | {improvement:+12.3f}")
        else:
            print(f"{name:20s} | {baseline_val:12.2f} | {old_val:12.2f} | {new_val:12.2f} | {improvement:+12.2f}")

    # 7. 总结
    print("\n" + "="*100)
    print("总结")
    print("="*100)

    # HMM vs Baseline
    old_vs_baseline = old_metrics['annual_return'] - baseline_metrics['annual_return']
    new_vs_baseline = new_metrics['annual_return'] - baseline_metrics['annual_return']

    print(f"\nHMM 相对 Baseline 的提升:")
    print(f"  旧版本: {old_vs_baseline:+.2f}%")
    print(f"  新版本: {new_vs_baseline:+.2f}%")

    # 新版本 vs 旧版本
    improvement = new_metrics['annual_return'] - old_metrics['annual_return']
    improvement_pct = (improvement / abs(old_metrics['annual_return']) * 100) if old_metrics['annual_return'] != 0 else 0

    print(f"\n新版本相对旧版本的改进:")
    print(f"  年化收益: {improvement:+.2f}% (相对提升 {improvement_pct:+.1f}%)")
    print(f"  Sharpe:   {new_metrics['sharpe'] - old_metrics['sharpe']:+.3f}")
    print(f"  最大回撤: {new_metrics['max_drawdown'] - old_metrics['max_drawdown']:+.2f}%")

    # 评价
    print("\n评价:")
    if improvement > 0.5:
        print("  ✅ 显著改进 (>0.5%)")
    elif improvement > 0.2:
        print("  ✅ 中等改进 (0.2-0.5%)")
    elif improvement > 0:
        print("  ✅ 轻微改进 (0-0.2%)")
    else:
        print("  ⚠️  无改进或变差")

    print("\n注意事项:")
    print("  1. 本验证基于简化策略 (top 50 等权)")
    print("  2. 未考虑交易成本和滑点")
    print("  3. 实际效果可能因策略不同而异")
    print("  4. 建议结合完整回测验证")


if __name__ == "__main__":
    import os

    # 检查文件是否存在
    if not os.path.exists(OLD_COEFF_PATH):
        print(f"❌ 旧版本系数文件不存在: {OLD_COEFF_PATH}")
        sys.exit(1)

    if not os.path.exists(NEW_COEFF_PATH):
        print(f"❌ 新版本系数文件不存在: {NEW_COEFF_PATH}")
        print("\n需要先生成新版本的系数文件")
        print("当前新版本只有验证集系数 (2026-01-26 ~ 2026-04-24)")
        print("\n两个选项:")
        print("  1. 使用现有验证集系数进行快速验证 (21天)")
        print("  2. 等待生成全量系数文件 (2024-07-01 ~ 2026-03-03)")

        # 使用现有的验证集系数
        print("\n使用选项 1: 验证集快速验证")

    try:
        compare_versions(OLD_COEFF_PATH, NEW_COEFF_PATH)
    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
