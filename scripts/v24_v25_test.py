#!/usr/bin/env python3
import sys
import os
import logging
sys.path.insert(0, '/mnt/f/Dev/AIstock')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

os.environ['SAVE_MINUTE_TRADES'] = '1'

logger.info('=== v24 vs v25 对比测试 ===')

# 初始化qlib
import qlib
qlib.init(provider_uri={'day': '/home/lc999/data/qlib_bin', '1min': '/home/lc999/data/qlib_minute_bin'}, region='cn')

# 导入v25
from rl_execution.executor.v25_two_stage_executor import V25TwoStageExecutor
import numpy as np

v25 = V25TwoStageExecutor(
    early_model_path='/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt',
    late_model_path='/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt',
    device='cpu'
)

# 测试v25执行计划生成
logger.info('测试v25执行计划生成...')

# 模拟数据
close = np.linspace(1800, 1820, 240)
vol = np.ones(240) * 1000
high = close + 5
low = close - 5
prev_close = 1800.0

# 重置v25状态
v25.reset(total_amount=10000, open_price=1810, prev_close=prev_close, stock_id='TEST')

# 生成计划
plan = v25._generate_plan(day_features=np.zeros(10, dtype=np.float32))

if plan is not None:
    early = plan[:30].sum()
    late = plan[30:].sum()

    logger.info(f'✓ 计划生成成功')
    logger.info(f'  前30分钟: {early:.4f} ({early*100:.2f}%)')
    logger.info(f'  后210分钟: {late:.4f} ({late*100:.2f}%)')
    logger.info(f'  v25目标: 88.79%')

    if early * 100 > 70:
        logger.info(f'  ✅ 符合v25高权重特征')
    else:
        logger.info(f'  ⚠️  权重低于预期')
else:
    logger.error('✗ 计划生成失败')

logger.info('测试完成')
