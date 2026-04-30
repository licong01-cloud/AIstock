#!/usr/bin/env python
"""HMM 系数效果验证 - 直接读取 bin 文件.

不使用 Qlib API，直接读取 bin 文件数据。
"""
import sys
import json
import numpy as np
import pandas as pd
import struct
from pathlib import Path
from typing import Dict

sys.stdout.reconfigure(encoding='utf-8')

# 配置
QLIB_BIN_PATH = Path("/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20260311")
OLD_COEFF_PATH = "/mnt/f/Dev/AIstock/backend/data/hmm_models/564b407f-1541-4b18-a087-2a45cfbca9d9/2026-04-04/coefficients_preset_A_2024-07-01_2026-03-03.json"
NEW_COEFF_PATH = "/mnt/f/Dev/AIstock/backend/data/hmm_models/b2d5bcc6-8463-4156-bf1a-e1392a00279a/2026-04-27/coefficients_preset_A_2024-07-01_2026-03-03.json"  # 使用全量系数


def load_coefficients(coeff_path: str) -> Dict:
    """加载 HMM 系数文件."""
    with open(coeff_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_bin_file(bin_path: Path) -> np.ndarray:
    """读取 Qlib bin 文件."""
    if not bin_path.exists():
        return np.array([])

    with open(bin_path, 'rb') as f:
        # Qlib bin 格式: 每个值�� float32
        data = f.read()
        values = struct.unpack(f'{len(data)//4}f', data)
        return np.array(values)


def load_stock_data_from_bin(qlib_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    """直接从 bin 文件加载股票数据."""

    print(f"   从 bin 文件加载数据: {start_date} ~ {end_date}")

    # 读取交易日历
    calendar_file = qlib_path / "calendars" / "day.txt"
    if not calendar_file.exists():
        print(f"   ❌ 交易日历文件不存在: {calendar_file}")
        return pd.DataFrame()

    with open(calendar_file, 'r') as f:
        all_dates = [line.strip() for line in f if line.strip()]

    # 过滤日期范围并建立索引
    date_to_idx = {d: i for i, d in enumerate(all_dates)}
    dates = [d for d in all_dates if start_date <= d <= end_date]
    print(f"   交易日: {len(dates)}")

    if not dates:
        print(f"   ❌ 没有找到日期范围内的数据")
        return pd.DataFrame()

    # 获取日期索引范围
    start_idx = date_to_idx[dates[0]]
    end_idx = date_to_idx[dates[-1]]
    print(f"   日期索引范围: {start_idx} ~ {end_idx}")

    # 读取股票列表
    instruments_dir = qlib_path / "instruments"
    all_txt = instruments_dir / "all.txt"

    if not all_txt.exists():
        print(f"   ❌ 股票列表文件不存在: {all_txt}")
        return pd.DataFrame()

    with open(all_txt, 'r') as f:
        instruments = [line.strip().split('\t')[0] for line in f if line.strip()]  # 只取第一列

    print(f"   股票数: {len(instruments)}")

    # 读取每只股票的数据
    all_data = []
    features_dir = qlib_path / "features"
    loaded_count = 0

    for i, instrument in enumerate(instruments[:500]):  # 限制500只股票加快速度
        if i % 100 == 0:
            print(f"   加载进度: {i}/{min(500, len(instruments))}, 已加载数据: {len(all_data)}")

        # 读取 close 和计算 change - 注意大小写转换
        instrument_lower = instrument.lower()
        close_file = features_dir / instrument_lower / "close.day.bin"

        if not close_file.exists():
            continue

        close_data = read_bin_file(close_file)

        if len(close_data) == 0:
            continue

        # 检查数据长度是否足够
        if len(close_data) <= start_idx:
            continue

        # 计算涨跌幅
        change_data = np.zeros(len(close_data))
        for j in range(1, len(close_data)):
            if close_data[j-1] > 0:
                change_data[j] = (close_data[j] / close_data[j-1] - 1) * 100

        # 匹配日期范围 - 使用实际索引
        for date_idx in range(start_idx, end_idx + 1):
            if date_idx < len(change_data) and date_idx > 0:
                pct_chg = change_data[date_idx]
                if not np.isnan(pct_chg) and not np.isinf(pct_chg) and abs(pct_chg) < 20:
                    all_data.append({
                        'date': all_dates[date_idx],
                        'instrument': instrument,
                        'pct_chg': pct_chg,
                    })

        if len(all_data) > loaded_count:
            loaded_count = len(all_data)
            if i < 5:  # 前5只股票打印调试信息
                print(f"     {instrument}: 添加了 {len(all_data) - loaded_count + (end_idx - start_idx + 1)} 条数据")

    df = pd.DataFrame(all_data)
    print(f"   加载完成: {len(df)} 条记录")

    # 添加数据统计
    if len(df) > 0:
        print(f"   数据统计:")
        print(f"     涨跌幅范围: {df['pct_chg'].min():.2f}% ~ {df['pct_chg'].max():.2f}%")
        print(f"     涨跌幅均值: {df['pct_chg'].mean():.2f}%")
        print(f"     涨跌幅中位数: {df['pct_chg'].median():.2f}%")

    return df


def apply_hmm_coefficients(returns_df: pd.DataFrame, coefficients: Dict) -> pd.DataFrame:
    """应用 HMM 系数."""
    daily_coeffs = coefficients['daily_coefficients']
    stock_sector_map = coefficients['stock_sector_map']

    def get_hmm_coeff(row):
        date_str = row['date']
        instrument = row['instrument']

        sector_code = stock_sector_map.get(instrument)
        if not sector_code or date_str not in daily_coeffs:
            return 1.0

        return daily_coeffs[date_str].get(sector_code, 1.0)

    returns_df['hmm_coeff'] = returns_df.apply(get_hmm_coeff, axis=1)
    returns_df['hmm_adjusted_return'] = returns_df['pct_chg'] * returns_df['hmm_coeff']

    return returns_df


def calculate_portfolio_metrics(returns_df: pd.DataFrame, return_col: str = 'pct_chg', top_n: int = 50) -> Dict:
    """计算组合收益指标."""
    if len(returns_df) == 0:
        return {
            'annual_return': 0.0,
            'sharpe': 0.0,
            'max_drawdown': 0.0,
            'win_rate': 0.0,
            'total_return': 0.0,
            'trading_days': 0,
        }

    daily_returns = []

    for trade_date, group in returns_df.groupby('date'):
        top_stocks = group.nlargest(top_n, return_col)
        portfolio_return = top_stocks['pct_chg'].mean()
        daily_returns.append({
            'date': trade_date,
            'return': portfolio_return,
        })

    daily_df = pd.DataFrame(daily_returns)
    daily_df = daily_df.sort_values('date')
    daily_df['cumulative'] = (1 + daily_df['return'] / 100).cumprod()

    # 添加调试输出
    print(f"     每日收益样本 (前5天):")
    for i in range(min(5, len(daily_df))):
        print(f"       {daily_df.iloc[i]['date']}: {daily_df.iloc[i]['return']:.2f}%")

    # 计算指标
    total_return = daily_df['cumulative'].iloc[-1] - 1
    trading_days = len(daily_df)

    # 修正年化收益率计算
    if trading_days > 0:
        annual_return = ((1 + total_return) ** (252 / trading_days) - 1) * 100
    else:
        annual_return = 0.0

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
        'annual_return': annual_return,
        'sharpe': sharpe,
        'max_drawdown': max_drawdown * 100,
        'win_rate': win_rate * 100,
        'total_return': total_return * 100,
        'trading_days': trading_days,
    }


def compare_versions(old_coeff_path: str, new_coeff_path: str, qlib_path: Path) -> None:
    """对比两个版本."""

    print("="*100)
    print("HMM 系数效果验证 - 直接读取 bin 文件")
    print("="*100)

    # 1. 加载系数
    print("\n1. 加载 HMM 系数文件...")
    old_coeff = load_coefficients(old_coeff_path)
    new_coeff = load_coefficients(new_coeff_path)

    print(f"   旧版本: {old_coeff.get('test_start', old_coeff.get('start_date'))} ~ {old_coeff.get('backtest_end', old_coeff.get('end_date'))}")
    print(f"   新版本: {new_coeff.get('test_start', new_coeff.get('start_date'))} ~ {new_coeff.get('backtest_end', new_coeff.get('end_date'))}")

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

    # 2. 加载数据
    print("\n2. 加载股票数据...")
    returns_df = load_stock_data_from_bin(qlib_path, test_start, test_end)

    if len(returns_df) == 0:
        print("\n❌ 错误: 没有加载到数据")
        return

    # 3. Baseline
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
    print(f"股票池: {returns_df['instrument'].nunique()} 只")

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

    old_vs_baseline = old_metrics['annual_return'] - baseline_metrics['annual_return']
    new_vs_baseline = new_metrics['annual_return'] - baseline_metrics['annual_return']

    print(f"\nHMM 相对 Baseline 的提升:")
    print(f"  旧版本: {old_vs_baseline:+.2f}%")
    print(f"  新版本: {new_vs_baseline:+.2f}%")

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
        print("  → 建议: 直接部署，Phase 2 优化可选")
    elif improvement > 0.2:
        print("  ✅ 中等改进 (0.2-0.5%)")
        print("  → 建议: 执行 Phase 2 优化，预期再提升 0.1-0.2%")
    elif improvement > 0:
        print("  ✅ 轻微改进 (0-0.2%)")
        print("  → 建议: 执行 Phase 2 优化，或重新评估方案")
    else:
        print("  ⚠️  无改进或变差")
        print("  → 建议: 深入分析原因，可能需要重新设计")

    print("\n" + "="*100)
    print("验证完成")
    print("="*100)


if __name__ == "__main__":
    import os

    if not os.path.exists(OLD_COEFF_PATH):
        print(f"❌ 旧版本系数文件不存在: {OLD_COEFF_PATH}")
        sys.exit(1)

    if not os.path.exists(NEW_COEFF_PATH):
        print(f"❌ 新版本系数文件不存在: {NEW_COEFF_PATH}")
        sys.exit(1)

    try:
        compare_versions(OLD_COEFF_PATH, NEW_COEFF_PATH, QLIB_BIN_PATH)
    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
