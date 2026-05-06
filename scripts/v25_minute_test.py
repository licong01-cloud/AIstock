#!/usr/bin/env python3
"""v25分钟级执行验证 - 基于v24 mini backtest修改"""
import sys
import logging
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, '/mnt/f/Dev/AIstock')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info('=== v25分钟级执行验证 ===')

# 1. 初始化qlib
logger.info('Step 1: 初始化qlib')
import qlib
qlib.init(
    provider_uri={
        'day': '/home/lc999/data/qlib_bin',
        '1min': '/home/lc999/data/qlib_minute_bin'
    },
    region='cn'
)
logger.info('  ✓ qlib初始化完成')

# 2. 导入v25 executor
logger.info('Step 2: 导入v25 executor')
from rl_execution.executor.v25_two_stage_executor import V25TwoStageExecutor

executor = V25TwoStageExecutor(
    early_model_path='/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt',
    late_model_path='/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt',
    device='cpu'
)
logger.info('  ✓ v25 executor加载完成')

# 3. 准备测试数据
logger.info('Step 3: 准备测试数据')
from qlib.data import D

test_stocks = ['600519.SH']
start = '2024-12-20'
end = '2024-12-31'

logger.info(f'  股票: {test_stocks}')
logger.info(f'  日期: {start} ~ {end}')

# 获取分钟数据
fields = ['', '', '', '', '']
df = D.features(test_stocks, fields, start_time=start, end_time=end, freq='1min')

if df.empty:
    logger.error('  ✗ 无分钟数据')
    sys.exit(1)

logger.info(f'  ✓ 数据shape: {df.shape}')

# 4. 按日期处理
dates = df.index.get_level_values(0).unique()
logger.info(f'  交易日数: {len(dates)}')

results = []

for date in dates:
    date_str = str(date)[:10]
    logger.info(f'\n处理日期: {date_str}')

    for stock in test_stocks:
        try:
            # 获取当日数据
            day_df = df.loc[(date, stock)]

            if len(day_df) < 240:
                logger.warning(f'  {stock}: 数据不足({len(day_df)}分钟)')
                continue

            close_arr = day_df[''].values[:240]
            vol_arr = day_df[''].values[:240]
            high_arr = day_df[''].values[:240]
            low_arr = day_df[''].values[:240]
            prev_close = day_df[''].iloc[0]

            # 调用v25生成执行计划
            plan = executor.generate_plan(
                full_day_close=close_arr,
                full_day_volume=vol_arr,
                full_day_high=high_arr,
                full_day_low=low_arr,
                prev_close=prev_close,
                stock_id=stock,
                is_buy=True,
                day_features=np.zeros(10, dtype=np.float32)
            )

            if plan is None:
                logger.error(f'  {stock}: 计划生成失败')
                continue

            # 统计前30分钟vs后210分钟
            early_weight = plan[:30].sum()
            late_weight = plan[30:].sum()

            logger.info(f'  {stock}: 前30分钟={early_weight:.4f} ({early_weight*100:.2f}%), 后210分钟={late_weight:.4f} ({late_weight*100:.2f}%)')

            results.append({
                'date': date_str,
                'stock': stock,
                'early_30min': early_weight,
                'late_210min': late_weight,
                'early_pct': early_weight * 100,
                'late_pct': late_weight * 100
            })

        except Exception as e:
            logger.error(f'  {stock}: 错误 - {e}')
            continue

# 5. 汇总分析
logger.info('\n=== 分析结果 ===')

if results:
    df_results = pd.DataFrame(results)

    logger.info(f'\n总样本数: {len(df_results)}')
    logger.info(f'\n前30分钟权重统计:')
    logger.info(f'  平均: {df_results["early_pct"].mean():.2f}%')
    logger.info(f'  中位数: {df_results["early_pct"].median():.2f}%')
    logger.info(f'  标准差: {df_results["early_pct"].std():.2f}%')
    logger.info(f'  最小: {df_results["early_pct"].min():.2f}%')
    logger.info(f'  最大: {df_results["early_pct"].max():.2f}%')

    logger.info(f'\n后210分钟权重统计:')
    logger.info(f'  平均: {df_results["late_pct"].mean():.2f}%')
    logger.info(f'  中位数: {df_results["late_pct"].median():.2f}%')
    logger.info(f'  标准差: {df_results["late_pct"].std():.2f}%')
    logger.info(f'  最小: {df_results["late_pct"].min():.2f}%')
    logger.info(f'  最大: {df_results["late_pct"].max():.2f}%')

    logger.info(f'\n=== v25 Oracle目标 ===')
    logger.info(f'  前30分钟: 88.79%')
    logger.info(f'  后210分钟: 11.21%')

    avg_early = df_results['early_pct'].mean()
    if avg_early > 70:
        logger.info(f'\n✅ 验证通过: 前30分钟平均权重{avg_early:.2f}%，符合v25高权重特征')
    else:
        logger.info(f'\n⚠️  验证失败: 前30分钟平均权重{avg_early:.2f}%，低于预期')

    # 保存结果
    df_results.to_csv('/tmp/v25_minute_verification.csv', index=False)
    logger.info(f'\n详细结果已保存到: /tmp/v25_minute_verification.csv')
else:
    logger.error('\n⚠️  无有效结果')

logger.info('\n=== 验证完成 ===')
