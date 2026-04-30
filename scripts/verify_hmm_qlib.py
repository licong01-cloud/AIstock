#!/usr/bin/env python
"""HMM 系数效果验证 - 使用 Qlib bin 数据.

优势:
- 快速: 直接读取 bin 文件，无需数据库
- 可靠: 使用与 QE 实验相同的数据源
- 完整: 数据更新及时

数据源: qlib_bin/qlib_bin_20260311/
"""
import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

sys.stdout.reconfigure(encoding='utf-8')

# 配置
QLIB_BIN_PATH = Path("qlib_bin/qlib_bin_20260311")
OLD_COEFF_PATH = "backend/data/hmm_models/564b407f-1541-4b18-a087-2a45cfbca9d9/2026-04-04/coefficients_preset_A_2024-07-01_2026-03-03.json"
NEW_COEFF_PATH = "backend/data/hmm_models/b2d5bcc6-8463-4156-bf1a-e1392a00279a/2026-04-27/coefficients_preset_A_2026-01-26_2026-04-24.json"


def load_coefficients(coeff_path: str) -> Dict:
    """加载 HMM 系数文件."""
    with open(coeff_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_qlib_data(qlib_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    """从 Qlib bin 文件加载股票数据.

    Returns:
        DataFrame with columns: datetime, instrument, $close, $change
    """
    try:
        import qlib
        from qlib.data import D
    except ImportError:
        print("❌ 错误: 需要安装 qlib")
        print("   pip install pyqlib")
        sys.exit(1)

    # 初始化 Qlib
    qlib.init(provider_uri=str(qlib_path), region="cn")

    # 加载数据
    print(f"   从 Qlib 加载数据: {start_date} ~ {end_date}")

    # 获取股票列表
    instruments = D.instruments(market="all")
    print(f"   股票数: {len(instruments)}")

    # 加载收盘价和涨跌幅
    fields = ["$close", "$change"]
    df = D.features(
        instruments=instruments,
        fields=fields,
        start_time=start_date,
        end_time=end_date,
        freq="day",
    )

    # 重置索引
    df = df.reset_index()
    df.columns = ['datetime', 'instrument', 'close', 'pct_chg']

    # 过滤无效数据
    df = df.dropna(subset=['pct_chg'])

    print(f"   加载完成: {len(df)} 条记录")
    print(f"   交易日: {df['datetime'].nunique()}")

    return df


def load_sector_mapping(qlib_path: Path) -> Dict[str, str]:
    """加载股票-行业映射.

    从 instruments 目录读取行业分类信息
    """
    # 简化版本: 从 HMM 系数文件中获取映射
    # 实际应该从 Qlib 的 instruments 或数据库读取
    return {}


def apply_hmm_coefficients(
    returns_df: pd.DataFrame,
    coefficients: Dict,
) -> pd.DataFrame:
    """应用 HMM 系数调整股票权重."""
    daily_coeffs = coefficients['daily_coefficients']
    stock_sector_map = coefficients['stock_sector_map']

    # 转换日期格式
    returns_df['date_str'] = returns_df['datetime'].dt.strftime('%Y-%m-%d')

    # 为每只股票添加 HMM 系数
    def get_hmm_coeff(row):
        date_str = row['date_str']
        instrument = row['instrument']

        # 获取股票所属行业
        sector_code = stock_sector_map.get(instrument)
        if not sector_code:
            return 1.0

        # 获取该日期该行业的系数
        if date_str not in daily_coeffs:
            return 1.0

        return daily_coeffs[date_str].get(sector_code, 1.0)

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
    """
    daily_returns = []

    for trade_date, group in returns_df.groupby('datetime'):
        # 按调整后收益排序，选择 top_n
        top_stocks = group.nlargest(top_n, return_col)

        # 等权组合收益
        portfolio_return = top_stocks['pct_chg'].mean()
        daily_returns.append({
            'datetime': trade_date,
            'return': portfolio_return,
        })

    daily_df = pd.DataFrame(daily_returns)
    daily_df = daily_df.sort_values('datetime')
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
        'daily_returns': daily_df,  # 保存用于绘图
    }


def plot_comparison(baseline_metrics, old_metrics, new_metrics, output_path='hmm_comparison.png'):
    """绘制对比图."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg')  # 非交互式后端

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # 1. 累计收益曲线
        ax1 = axes[0, 0]
        baseline_df = baseline_metrics['daily_returns']
        old_df = old_metrics['daily_returns']
        new_df = new_metrics['daily_returns']

        ax1.plot(baseline_df['datetime'], baseline_df['cumulative'], label='Baseline', linewidth=2)
        ax1.plot(old_df['datetime'], old_df['cumulative'], label='Old HMM', linewidth=2)
        ax1.plot(new_df['datetime'], new_df['cumulative'], label='New HMM', linewidth=2)
        ax1.set_title('Cumulative Returns', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Cumulative Return')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 2. 回撤曲线
        ax2 = axes[0, 1]
        baseline_dd = (baseline_df['cumulative'] - baseline_df['cumulative'].expanding().max()) / baseline_df['cumulative'].expanding().max()
        old_dd = (old_df['cumulative'] - old_df['cumulative'].expanding().max()) / old_df['cumulative'].expanding().max()
        new_dd = (new_df['cumulative'] - new_df['cumulative'].expanding().max()) / new_df['cumulative'].expanding().max()

        ax2.plot(baseline_df['datetime'], baseline_dd * 100, label='Baseline', linewidth=2)
        ax2.plot(old_df['datetime'], old_dd * 100, label='Old HMM', linewidth=2)
        ax2.plot(new_df['datetime'], new_dd * 100, label='New HMM', linewidth=2)
        ax2.set_title('Drawdown', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Drawdown (%)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # 3. 指标对比柱状图
        ax3 = axes[1, 0]
        metrics_names = ['Annual Return\n(%)', 'Sharpe', 'Max DD\n(%)']
        baseline_vals = [baseline_metrics['annual_return'], baseline_metrics['sharpe'], baseline_metrics['max_drawdown']]
        old_vals = [old_metrics['annual_return'], old_metrics['sharpe'], old_metrics['max_drawdown']]
        new_vals = [new_metrics['annual_return'], new_metrics['sharpe'], new_metrics['max_drawdown']]

        x = np.arange(len(metrics_names))
        width = 0.25

        ax3.bar(x - width, baseline_vals, width, label='Baseline')
        ax3.bar(x, old_vals, width, label='Old HMM')
        ax3.bar(x + width, new_vals, width, label='New HMM')
        ax3.set_title('Metrics Comparison', fontsize=14, fontweight='bold')
        ax3.set_xticks(x)
        ax3.set_xticklabels(metrics_names)
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')

        # 4. 改进幅度
        ax4 = axes[1, 1]
        improvements = [
            new_metrics['annual_return'] - old_metrics['annual_return'],
            (new_metrics['sharpe'] - old_metrics['sharpe']) * 10,  # 放大10倍便于显示
            new_metrics['max_drawdown'] - old_metrics['max_drawdown'],
        ]
        colors = ['green' if x > 0 else 'red' for x in improvements]

        ax4.bar(metrics_names, improvements, color=colors, alpha=0.7)
        ax4.set_title('Improvement (New vs Old)', fontsize=14, fontweight='bold')
        ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax4.set_ylabel('Improvement')
        ax4.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\n✅ 对比图已保存: {output_path}")

    except ImportError:
        print("\n⚠️  matplotlib 未安装，跳过绘图")


def compare_versions(
    old_coeff_path: str,
    new_coeff_path: str,
    qlib_path: Path,
) -> None:
    """对比两个版本的 HMM 系数效果."""

    print("="*100)
    print("HMM 系数效果验证 - Qlib 数据")
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

    # 2. 加载 Qlib 数据
    print("\n2. 加载 Qlib 数据...")
    returns_df = load_qlib_data(qlib_path, test_start, test_end)

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

    # 8. 绘制对比图
    print("\n6. 生成对比图...")
    plot_comparison(baseline_metrics, old_metrics, new_metrics)

    print("\n" + "="*100)
    print("验证完成")
    print("="*100)


if __name__ == "__main__":
    import os

    # 检查 Qlib bin 路径
    if not QLIB_BIN_PATH.exists():
        print(f"❌ Qlib bin 路径不存在: {QLIB_BIN_PATH}")
        sys.exit(1)

    # 检查系数文件
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
