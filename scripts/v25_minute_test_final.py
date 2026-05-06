#!/usr/bin/env python3
"""v25分钟级执行验证 - 独立测试"""
import sys
import logging
import numpy as np
import pandas as pd

sys.path.insert(0, '/mnt/f/Dev/AIstock')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

logger.info('=== v25分钟级执行验证 ===\n')

# 1. 初始化
logger.info('Step 1: 初始化qlib')
import qlib
qlib.init(provider_uri={'day': '/home/lc999/data/qlib_bin', '1min': '/home/lc999/data/qlib_minute_bin'}, region='cn')

# 2. 导入v25
logger.info('Step 2: 导入v25 executor')
from rl_execution.executor.v25_two_stage_executor import V25TwoStageExecutor

executor = V25TwoStageExecutor(
    early_model_path='/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt',
    late_model_path='/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt',
    device='cpu'
)

# 3. 测试数据
logger.info('Step 3: 准备测试数据')
from qlib.data import D

stock = '600519.SH'
start = '2024-12-20'
end = '2024-12-31'

df = D.features([stock], ['', '', '', '', ''],
                start_time=start, end_time=end, freq='1min')

if df.empty:
    logger.error('无数据')
    sys.exit(1)

logger.info(f'数据shape: {df.shape}')

# 4. 处理每一天
dates = df.index.get_level_values(0).unique()
logger.info(f'交易日数: {len(dates)}\n')

results = []

for date in dates:
    try:
        day_df = df.loc[(date, stock)]

        if len(day_df) < 240:
            continue

        close = day_df[''].values[:240]
        vol = day_df[''].values[:240]
        high = day_df[''].values[:240]
        low = day_df[''].values[:240]
        prev_close = day_df[''].iloc[0]

        plan = executor.generate_plan(
            full_day_close=close,
            full_day_volume=vol,
            full_day_high=high,
            full_day_low=low,
            prev_close=prev_close,
            stock_id=stock,
            is_buy=True,
            day_features=np.zeros(10, dtype=np.float32)
        )

        if plan is None:
            continue

        early = plan[:30].sum()
        late = plan[30:].sum()

        logger.info(f'{str(date)[:10]}: 前30分钟={early*100:.2f}%, 后210分钟={late*100:.2f}%')

        results.append({
            'date': str(date)[:10],
            'early_pct': early * 100,
            'late_pct': late * 100
        })

    except Exception as e:
        logger.error(f'{date}: {e}')

# 5. 汇总
logger.info('\n=== 结果汇总 ===\n')

if results:
    df_r = pd.DataFrame(results)

    logger.info(f'样本数: {len(df_r)}')
    logger.info(f'前30分钟平均: {df_r["early_pct"].mean():.2f}%')
    logger.info(f'后210分钟平均: {df_r["late_pct"].mean():.2f}%')
    logger.info(f'\nv25目标: 前30分钟=88.79%, 后210分钟=11.21%')

    avg = df_r['early_pct'].mean()
    if avg > 70:
        logger.info(f'\n✅ 验证通过: {avg:.2f}% 符合v25特征')
    else:
        logger.info(f'\n⚠️  验证失败: {avg:.2f}% 低于预期')

    df_r.to_csv('/tmp/v25_test.csv', index=False)
    logger.info(f'\n结果已保存: /tmp/v25_test.csv')
else:
    logger.error('无有效结果')

logger.info('\n=== 完成 ===')
