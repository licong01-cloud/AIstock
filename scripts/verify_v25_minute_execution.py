#!/usr/bin/env python3
"""v25策略分钟级执行验证 - 独立测试脚本

不依赖QE框架，直接使用qlib + v25 executor验证分钟级交易分布
"""
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, '/mnt/f/Dev/AIstock')

print('=== v25策略分钟级执行验证 ===\n')

# 1. 初始化qlib
print('1. 初始化qlib...')
import qlib
qlib.init(
    provider_uri={
        'day': '/home/lc999/data/qlib_bin',
        '1min': '/home/lc999/data/qlib_minute_bin'
    },
    region='cn'
)
print('  ✓ qlib初始化完成\n')

# 2. 导入v25 executor
print('2. 导入v25 executor...')
from rl_execution.executor.v25_two_stage_executor import V25TwoStageExecutor

executor = V25TwoStageExecutor(
    early_model_path='/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt',
    late_model_path='/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt',
    device='cpu'
)
print('  ✓ v25 executor加载完成\n')

# 3. 准备测试数据
print('3. 准备测试数据...')
from qlib.data import D

# 选择5只股票，5个交易日
test_stocks = ['600519.SH', '000001.SZ', '600036.SH', '601318.SH', '000858.SZ']
test_dates = ['2025-01-02', '2025-01-03', '2025-01-06', '2025-01-07', '2025-01-08']

print(f'  测试股票: {test_stocks}')
print(f'  测试日期: {test_dates}\n')

# 4. 模拟回测
print('4. 执行分钟级回测...\n')

minute_trades = defaultdict(lambda: defaultdict(list))  # {date: {minute: [trades]}}
daily_summary = []

for date in test_dates:
    print(f'  处理日期: {date}')

    for stock in test_stocks:
        try:
            # 获取分钟数据
            df = D.features(
                [stock],
                ['', '', '', '', ''],
                start_time=date,
                end_time=date,
                freq='1min'
            )

            if df.empty:
                continue

            # 获取前一日收盘价
            prev_df = D.features(
                [stock],
                [''],
                start_time=pd.Timestamp(date) - pd.Timedelta(days=5),
                end_time=pd.Timestamp(date) - pd.Timedelta(days=1),
                freq='day'
            )

            if prev_df.empty:
                continue

            prev_close = prev_df.iloc[-1]['']

            # 准备全天数据
            close_arr = df[''].values
            vol_arr = df[''].values
            high_arr = df[''].values
            low_arr = df[''].values

            if len(close_arr) < 240:
                print(f'    {stock}: 数据不足({len(close_arr)}分钟)，跳过')
                continue

            # 调用v25 executor生成执行计划
            plan = executor.generate_plan(
                full_day_close=close_arr[:240],
                full_day_volume=vol_arr[:240],
                full_day_high=high_arr[:240],
                full_day_low=low_arr[:240],
                prev_close=prev_close,
                stock_id=stock,
                is_buy=True,
                day_features=np.zeros(10, dtype=np.float32)
            )

            if plan is None:
                print(f'    {stock}: 计划生成失败')
                continue

            # 模拟执行
            total_quantity = 10000  # 总目标数量
            executed_per_minute = plan * total_quantity

            # 记录每分钟交易
            for minute in range(240):
                if executed_per_minute[minute] > 0:
                    minute_trades[date][minute].append({
                        'stock': stock,
                        'quantity': executed_per_minute[minute],
                        'price': close_arr[minute]
                    })

            # 统计前30分钟vs后210分钟
            early_weight = plan[:30].sum()
            late_weight = plan[30:].sum()

            daily_summary.append({
                'date': date,
                'stock': stock,
                'early_30min_weight': early_weight,
                'late_210min_weight': late_weight,
                'early_pct': early_weight * 100,
                'late_pct': late_weight * 100
            })

            print(f'    {stock}: 前30分钟={early_weight:.2%}, 后210分钟={late_weight:.2%}')

        except Exception as e:
            print(f'    {stock}: 错误 - {e}')
            continue

print('\n5. 分析结果...\n')

# 汇总统计
df_summary = pd.DataFrame(daily_summary)

if not df_summary.empty:
    print('=== 分钟级权重分布统计 ===\n')
    print(f'总样本数: {len(df_summary)}\n')

    print('前30分钟权重:')
    print(f'  平均: {df_summary["early_pct"].mean():.2f}%')
    print(f'  中位数: {df_summary["early_pct"].median():.2f}%')
    print(f'  标准差: {df_summary["early_pct"].std():.2f}%')
    print(f'  最小: {df_summary["early_pct"].min():.2f}%')
    print(f'  最大: {df_summary["early_pct"].max():.2f}%\n')

    print('后210分钟权重:')
    print(f'  平均: {df_summary["late_pct"].mean():.2f}%')
    print(f'  中位数: {df_summary["late_pct"].median():.2f}%')
    print(f'  标准差: {df_summary["late_pct"].std():.2f}%')
    print(f'  最小: {df_summary["late_pct"].min():.2f}%')
    print(f'  最大: {df_summary["late_pct"].max():.2f}%\n')

    print('=== v25 Oracle目标 ===')
    print('  前30分钟: 88.79%')
    print('  后210分钟: 11.21%\n')

    # 判断是否符合v25特征
    avg_early = df_summary['early_pct'].mean()
    if avg_early > 70:
        print(f'✅ 验证通过: 前30分钟平均权重{avg_early:.2f}%，符合v25高权重特征')
    else:
        print(f'⚠️  验证失败: 前30分钟平均权重{avg_early:.2f}%，低于预期')

    # 保存详细结果
    df_summary.to_csv('/tmp/v25_minute_verification.csv', index=False)
    print(f'\n详细结果已保存到: /tmp/v25_minute_verification.csv')

    # 分析每分钟交易分布
    print('\n=== 每分钟交易分布（前10分钟）===\n')

    # 统计所有日期的分钟交易
    minute_totals = defaultdict(float)
    for date, minutes in minute_trades.items():
        for minute, trades in minutes.items():
            minute_totals[minute] += sum(t['quantity'] for t in trades)

    total_qty = sum(minute_totals.values())

    print(f"{'分钟':<6} {'交易量':>12} {'占比':>8}")
    print('-' * 30)
    for minute in range(min(10, len(minute_totals))):
        qty = minute_totals.get(minute, 0)
        pct = (qty / total_qty * 100) if total_qty > 0 else 0
        print(f'{minute:<6} {qty:>12,.0f} {pct:>7.2f}%')

else:
    print('⚠️  无有效数据，请检查股票代码和日期')

print('\n=== 验证完成 ===')
