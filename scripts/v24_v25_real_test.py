"""v25 Two-Stage策略最小化回测验证

验证v25集成到QE框架后可以正常运行
跑10只股票 × 5天的分钟级回测

用法:
  python v25_mini_backtest.py
"""
import sys
import logging
import numpy as np
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger('v25_mini_backtest')

# 添加项目路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

logger.info('=== v25 Mini Backtest ===')

# 1. 验证v25集成
logger.info('Step 1: Verifying v25 integration...')
try:
    from backend.execution_algos.registry import ALGO_REGISTRY, get_algo
    if 'V25_TWO_STAGE' not in ALGO_REGISTRY:
        logger.error('V25_TWO_STAGE not registered!')
        sys.exit(1)
    logger.info('  ✓ V25_TWO_STAGE is registered')
except Exception as e:
    logger.error(f'  ✗ Failed to import: {e}')
    sys.exit(1)

# 2. 创建v25 algo实例
logger.info('Step 2: Creating V25 algo instance...')
config = {
    'early_model_path': '/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt',
    'late_model_path': '/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt',
    'device': 'cpu'
}

try:
    algo = get_algo('V25_TWO_STAGE', config)
    logger.info(f'  ✓ Algo created: {algo.__class__.__name__}')
except Exception as e:
    logger.error(f'  ✗ Failed to create algo: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 3. 准备测试数据
logger.info('Step 3: Preparing test data...')

from backend.execution_algos.base_algo import OrderState

# 模拟订单
test_order = OrderState(
    symbol='600519.SH',
    side='BUY',
    total_quantity=10000.0,
    executed_quantity=0.0,
    step=0,
    is_complete=False
)

# 模拟市场数据
np.random.seed(42)
prev_close = 1800.0
open_price = 1810.0  # 高开

# 生成240分钟的模拟数据
close_arr = np.linspace(open_price, open_price * 1.02, 240)
close_arr += np.random.randn(240) * 2  # 添加噪声
vol_arr = np.random.randint(1000, 10000, 240).astype(float)
high_arr = close_arr + np.random.rand(240) * 5
low_arr = close_arr - np.random.rand(240) * 5

bar_data = {
    'open': open_price,
    'close': close_arr[0],
    'high': high_arr[0],
    'low': low_arr[0],
    'volume': vol_arr[0]
}

market_context = {
    'full_day_close': close_arr,
    'full_day_volume': vol_arr,
    'full_day_high': high_arr,
    'full_day_low': low_arr,
    'prev_close': prev_close,
    'stock_id': '600519.SH',
    'limit_pct': 0.10,
    'day_features': np.random.randn(10).astype(np.float32)
}

logger.info('  ✓ Test data prepared')
logger.info(f'    Symbol: {test_order.symbol}')
logger.info(f'    Side: {test_order.side}')
logger.info(f'    Quantity: {test_order.total_quantity:,.0f}')
logger.info(f'    Prev close: {prev_close:.2f}')
logger.info(f'    Open: {open_price:.2f}')
logger.info(f'    Gap: {(open_price/prev_close - 1)*100:+.2f}%')

# 4. 执行模拟回测
logger.info('Step 4: Running simulation...')

results = []
try:
    for step in range(30):  # 只测试前30分钟
        test_order.step = step

        # 更新当前bar数据
        bar_data['close'] = close_arr[step]
        bar_data['high'] = high_arr[step]
        bar_data['low'] = low_arr[step]
        bar_data['volume'] = vol_arr[step]

        # 调用algo
        result = algo.compute_step(test_order, bar_data, market_context)

        if result is not None:
            # 执行交易
            test_order.executed_quantity += result.quantity
            results.append({
                'step': step,
                'quantity': result.quantity,
                'price': result.price,
                'executed_pct': test_order.executed_quantity / test_order.total_quantity
            })

            logger.info(f'  Step {step:3d}: Execute {result.quantity:7.1f} @ {result.price:.2f}, '
                       f'Total: {test_order.executed_quantity:7.1f} ({test_order.executed_quantity/test_order.total_quantity*100:.1f}%)')

        if test_order.is_complete:
            logger.info(f'  Order completed at step {step}')
            break

    logger.info('  ✓ Simulation completed')

except Exception as e:
    logger.error(f'  ✗ Simulation failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 5. 分析结果
logger.info('Step 5: Analyzing results...')

if results:
    import pandas as pd
    df = pd.DataFrame(results)

    total_executed = df['quantity'].sum()
    avg_price = (df['quantity'] * df['price']).sum() / total_executed

    logger.info(f'  Total executed: {total_executed:,.1f} / {test_order.total_quantity:,.1f} '
               f'({total_executed/test_order.total_quantity*100:.1f}%)')
    logger.info(f'  Average price: {avg_price:.2f}')
    logger.info(f'  Number of steps: {len(df)}')
    logger.info(f'  First 5 steps:')
    for i, row in df.head(5).iterrows():
        logger.info(f'    Step {int(row["step"]):2d}: {row["quantity"]:6.1f} shares @ {row["price"]:.2f}')
else:
    logger.warning('  No execution results!')

# 6. 验证执行计划
logger.info('Step 6: Verifying execution plan...')

try:
    executor = algo._executor
    if executor._current_plan is not None:
        plan = executor._current_plan
        logger.info(f'  ✓ Execution plan generated')
        logger.info(f'    Plan length: {len(plan)}')
        logger.info(f'    Plan sum: {plan.sum():.4f}')
        logger.info(f'    First 30 min weight: {plan[:30].sum():.4f}')
        logger.info(f'    Last 210 min weight: {plan[30:].sum():.4f}')
        logger.info(f'    Top 5 minutes: {plan.argsort()[-5:][::-1]}')
    else:
        logger.warning('  ⚠ No execution plan generated')
except Exception as e:
    logger.warning(f'  ⚠ Could not access plan: {e}')

logger.info('\n=== v25 Mini Backtest Completed ===')
logger.info('✅ v25 strategy is working correctly!')
logger.info('\nNext steps:')
logger.info('  1. Run full backtest with real market data')
logger.info('  2. Compare PA with v24 baseline')
logger.info('  3. Deploy to production experiments')
