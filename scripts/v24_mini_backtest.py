"""v24 策略最小化回测验证 — 验证修复后的策略能正确执行。

不依赖完整训练流程，直接用 qlib 底层 API 构建 Exchange + Strategy，
跑 5 只股票 × 5 天的分钟级回测。

用法:
  python v24_mini_backtest.py
"""
import sys
import os
import logging
import numpy as np
import pandas as pd
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("v24_mini_backtest")

# 把 scripts 目录加入 path
scripts_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(scripts_dir))

# ── 1. Qlib 初始化 ──
import qlib
from qlib.config import C
C["kernels"] = 1

qlib.init(
    provider_uri={
        "day": "/home/lc999/data/qlib_bin",
        "1min": "/home/lc999/data/qlib_minute_bin",
    },
    region="cn",
    dataset_cache=None,
    expression_cache=None,
)

from qlib.data import D
from qlib.backtest import backtest as qlib_backtest
from qlib.backtest.executor import SimulatorExecutor, NestedExecutor
from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy

# ── 2. 验证数据可用性 ──
test_stocks = ["600519.SH", "000001.SZ", "300750.SZ"]
fields = ["$close", "$prev_close", "$up_limit_price", "$down_limit_price"]
start = "2025-01-02"
end = "2025-01-10"

logger.info("Checking data availability...")
df = D.features(test_stocks, fields, start_time=start, end_time=end, freq="1min")
logger.info(f"  D.features shape: {df.shape}")
if df.empty:
    logger.error("D.features returned empty! Minute bins not ready.")
    sys.exit(1)

for stock in test_stocks:
    try:
        sub = df.xs(stock, level="instrument")
        valid = (~sub["$prev_close"].isna()).sum()
        logger.info(f"  {stock}: {len(sub)} bars, prev_close valid={valid}")
    except KeyError:
        logger.warning(f"  {stock}: not in result")

# ── 3. 导入并实例化 v24 策略 ──
logger.info("Loading v24 strategy...")
from tail_twap_v24_strategy import TailTWAPWithV24PlanStrategy

model_path = "/home/lc999/data/rl_models/v24/v24_plan_net.pt"
if not Path(model_path).exists():
    logger.error(f"Model not found: {model_path}")
    sys.exit(1)

strategy = TailTWAPWithV24PlanStrategy(
    model_path=model_path,
    warmup_minutes=30,
    warmup_alloc=0.20,
    device="cpu",
)
logger.info("Strategy instantiated OK")

# ── 4. 构建简单信号 (等权买入 top 3) ──
logger.info("Building mock signal...")
# 用 D.features 获取日级 close 来构造一个假信号
day_df = D.features(test_stocks, ["$close"], start_time=start, end_time=end, freq="day")
# 简单信号: 按 close 排序 (仅为了触发交易)
signal = day_df.copy()
signal.columns = ["score"]
# 归一化到 [0, 1]
signal["score"] = signal.groupby(level="datetime")["score"].rank(pct=True)
logger.info(f"  Signal shape: {signal.shape}")

# ── 5. 执行回测 ──
logger.info("=" * 60)
logger.info("Running mini backtest...")
logger.info("=" * 60)

try:
    portfolio_metric_dict, indicator_dict = qlib_backtest(
        start_time=start,
        end_time=end,
        strategy=TopkDropoutStrategy(
            signal=signal,
            topk=3,
            n_drop=1,
        ),
        executor=NestedExecutor(
            time_per_step="day",
            inner_executor=SimulatorExecutor(
                time_per_step="1min",
                generate_portfolio_metrics=False,
            ),
            inner_strategy=strategy,
            generate_portfolio_metrics=True,
        ),
        benchmark=None,
        account=10000000,
        exchange_kwargs={
            "freq": "1min",
            "limit_threshold": ("$limit_up", "$limit_down"),
            "deal_price": "close",
            "open_cost": 0.000095,
            "close_cost": 0.000595,
            "min_cost": 5,
            "trade_unit": 100,
            "subscribe_fields": [
                "$up_limit_price", "$down_limit_price", "$prev_close",
                "$high", "$low", "$open", "$volume",
            ],
        },
    )
    logger.info("=" * 60)
    logger.info("BACKTEST COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)

    # 输出基本指标
    if portfolio_metric_dict:
        for key, val in portfolio_metric_dict.items():
            if isinstance(val, (int, float)):
                logger.info(f"  {key}: {val:.6f}")
            elif isinstance(val, pd.Series) and len(val) < 20:
                logger.info(f"  {key}: {val.to_dict()}")

except Exception as e:
    logger.error("=" * 60)
    logger.error(f"BACKTEST FAILED: {e}")
    logger.error("=" * 60)
    import traceback
    traceback.print_exc()
    sys.exit(1)

logger.info("v24 mini backtest verification PASSED")
