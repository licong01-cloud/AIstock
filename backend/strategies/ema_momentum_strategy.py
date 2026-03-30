"""EMA 动量择时策略（日内分钟级）

移植自 Qlib SBBStrategyEMA，适配 AIStock BaseStrategy 框架。

策略逻辑（基于 EMA 双线交叉 + 趋势强度判断）：
- 计算 EMA(fast_period) 和 EMA(slow_period) 在分钟 K 线上的差值
- EMA差值 > 0 且从负转正 → 看多趋势确认 → BUY
- EMA差值 < 0 且从正转负 → 看空趋势确认 → SELL
- EMA差值 == 0 或信号不明 → 不操作（TREND_MID）

原版 Qlib SBBStrategyEMA 在相邻两个 Bar 中选择更优执行时机：
- 第一个 Bar 预测趋势方向
- 第二个 Bar 根据预测方向决定是否加倍执行

本适配版简化为：每个调度周期独立判断 EMA 趋势方向，生成信号。

典型配置：
  schedule_config: {"type": "minute", "interval": 5}
  config: {
      "symbols": ["600519.SH", "000001.SZ"],
      "fast_period": 10,        # 快线 EMA 周期
      "slow_period": 20,        # 慢线 EMA 周期
      "period": "5m",           # 数据周期
      "position_size": 0.1,     # 仓位比例
      "trend_strength": 0.001,  # 趋势强度阈值（EMA差值占价格比例）
      "price_type": "LIMIT"
  }

来源：Qlib qlib.contrib.strategy.rule_strategy.SBBStrategyEMA
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .base_strategy import BaseStrategy, TradeSignal
from ..data_service import api as data_api

logger = logging.getLogger("strategy.ema_momentum")

# 趋势常量（与 Qlib SBBStrategyBase 一致）
TREND_MID = 0
TREND_SHORT = 1
TREND_LONG = 2


class EMAMomentumStrategy(BaseStrategy):
    """EMA 动量择时策略（分钟级）

    基于 EMA(fast) - EMA(slow) 信号判断短期趋势方向。
    """

    def __init__(
        self,
        strategy_id: str = "ema_momentum_001",
        executor=None,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(strategy_id, executor, config)
        self.fast_period = self.config.get("fast_period", 10)
        self.slow_period = self.config.get("slow_period", 20)
        self.period = self.config.get("period", "5m")
        self.symbols = self.config.get("symbols", [])
        self.position_size = self.config.get("position_size", 0.1)
        self.price_type = self.config.get("price_type", "LIMIT")
        self.trend_strength = self.config.get("trend_strength", 0.001)

        # 趋势状态追踪：{symbol: last_trend}
        self._trend_state: Dict[str, int] = {}

    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略：获取分钟数据 → 计算 EMA → 判断趋势 → 生成信号"""
        try:
            data = self._fetch_stock_data(symbol)
            if data is None or data.empty:
                return {"success": False, "signals": [], "message": f"无法获取 {symbol} 的分钟数据"}

            signal = self.generate_signal({"symbol": symbol, "data": data})

            if signal is None:
                return {"success": True, "signals": [], "message": f"{symbol} 无交易信号（趋势中性）"}

            result = self.execute_signal(signal)
            return {
                "success": result.get("success", False),
                "signals": [signal.__dict__],
                "message": result.get("message", ""),
                "order_id": result.get("order_id"),
            }

        except Exception as e:
            self.logger.error(f"EMA动量策略执行异常: {e}", exc_info=True)
            return {"success": False, "signals": [], "message": f"执行异常: {str(e)}"}

    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """基于 EMA 差值信号生成交易决策

        复用 Qlib SBBStrategyEMA 的核心逻辑：
        EMA(close, fast_period) - EMA(close, slow_period)
        """
        symbol = data.get("symbol")
        df = data.get("data")

        if df is None or df.empty or len(df) < self.slow_period + 2:
            return None

        # 计算 EMA 差值（与 Qlib 完全一致的 EMA 信号）
        ema_fast = df["close"].ewm(span=self.fast_period, adjust=False).mean()
        ema_slow = df["close"].ewm(span=self.slow_period, adjust=False).mean()
        ema_diff = ema_fast - ema_slow

        current_diff = float(ema_diff.iloc[-1])
        prev_diff = float(ema_diff.iloc[-2])
        current_price = float(df["close"].iloc[-1])

        # 归一化差值（占价格比例）
        normalized_diff = current_diff / current_price if current_price > 0 else 0

        # 趋势判断（与 Qlib SBBStrategyEMA._pred_price_trend 一致）
        if np.isnan(current_diff) or abs(normalized_diff) < self.trend_strength:
            current_trend = TREND_MID
        elif current_diff > 0:
            current_trend = TREND_LONG
        else:
            current_trend = TREND_SHORT

        last_trend = self._trend_state.get(symbol, TREND_MID)
        self._trend_state[symbol] = current_trend

        # 趋势转换时产生信号
        # 从非多头转为多头 → BUY
        if current_trend == TREND_LONG and last_trend != TREND_LONG and prev_diff <= 0 < current_diff:
            quantity = self._calculate_quantity(symbol, current_price)
            if quantity > 0:
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="BUY",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"EMA({self.fast_period})上穿EMA({self.slow_period}), 差值={current_diff:.4f}",
                    signal_data={
                        "ema_fast": float(ema_fast.iloc[-1]),
                        "ema_slow": float(ema_slow.iloc[-1]),
                        "ema_diff": current_diff,
                        "normalized_diff": normalized_diff,
                        "trend": "LONG",
                        "price": current_price,
                    },
                )

        # 从非空头转为空头 → SELL
        elif current_trend == TREND_SHORT and last_trend != TREND_SHORT and prev_diff >= 0 > current_diff:
            quantity = self._calculate_sell_quantity(symbol)
            if quantity > 0:
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="SELL",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"EMA({self.fast_period})下穿EMA({self.slow_period}), 差值={current_diff:.4f}",
                    signal_data={
                        "ema_fast": float(ema_fast.iloc[-1]),
                        "ema_slow": float(ema_slow.iloc[-1]),
                        "ema_diff": current_diff,
                        "normalized_diff": normalized_diff,
                        "trend": "SHORT",
                        "price": current_price,
                    },
                )

        return None

    def _fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取分钟级 K 线数据"""
        try:
            bars = max(self.slow_period * 3, 60)

            if self.period in ("1m", "5m", "15m", "30m", "60m"):
                df = data_api.get_intraday_window(
                    [symbol],
                    bars=bars,
                    fields=["open", "high", "low", "close", "volume", "amount"],
                    freq=self.period,
                )
            else:
                df = data_api.get_history_window(
                    [symbol],
                    bars=bars,
                    fields=["open", "high", "low", "close", "volume", "amount"],
                    freq="1d",
                )

            if df is None or df.empty:
                return None

            try:
                return df.xs(symbol, level="instrument")
            except Exception:
                return None

        except Exception as e:
            self.logger.error(f"获取股票数据失败: {e}", exc_info=True)
            return None

    def _calculate_quantity(self, symbol: str, price: float) -> int:
        """计算买入数量"""
        try:
            if self.executor is None:
                from ..infra.strategy_executor import SimpleStrategyExecutor
                self.executor = SimpleStrategyExecutor()

            account_info = self.executor.qmt_client.get_account_info()
            available_cash = account_info.get("available_cash", 0.0)
            target_amount = available_cash * self.position_size
            quantity = int(target_amount / price / 100) * 100
            return max(0, quantity)

        except Exception as e:
            self.logger.error(f"计算买入数量失败: {e}", exc_info=True)
            return 0

    def _calculate_sell_quantity(self, symbol: str) -> int:
        """计算卖出数量（卖出全部持仓）"""
        try:
            if self.executor is None:
                from ..infra.strategy_executor import SimpleStrategyExecutor
                self.executor = SimpleStrategyExecutor()

            positions = self.executor.qmt_client.get_positions()
            for pos in positions:
                if pos.get("stock_code") == symbol:
                    return pos.get("can_sell", 0)
            return 0

        except Exception as e:
            self.logger.error(f"计算卖出数量失败: {e}", exc_info=True)
            return 0
