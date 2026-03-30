"""TWAP 拆单执行策略（日内分钟级）

移植自 Qlib TWAPStrategy，适配 AIStock BaseStrategy 框架。

策略逻辑：
- 接收一个目标订单（买/卖 N 股某只股票）
- 将订单拆分为 split_count 等份，每个调度周期执行一份
- 支持 1m/5m/15m/30m 分钟级调度
- 最后一步强制执行剩余全部数量
- 按 100 股对齐（A股交易单位）

典型配置：
  schedule_config: {"type": "minute", "interval": 5}  # 每5分钟执行一次
  config: {
      "symbols": ["600519.SH"],
      "target_orders": {"600519.SH": {"side": "BUY", "total_quantity": 1000}},
      "split_count": 12,        # 拆为12份
      "start_time": "09:35",    # 开始时间
      "end_time": "14:50",      # 结束时间
      "period": "5m",           # 数据周期
      "price_type": "LIMIT"
  }

来源：Qlib qlib.contrib.strategy.rule_strategy.TWAPStrategy
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dt_time
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .base_strategy import BaseStrategy, TradeSignal
from ..data_service import api as data_api

logger = logging.getLogger("strategy.twap_execution")


class TWAPExecutionStrategy(BaseStrategy):
    """TWAP 时间加权均匀拆单执行策略"""

    def __init__(
        self,
        strategy_id: str = "twap_001",
        executor=None,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(strategy_id, executor, config)
        self.split_count = self.config.get("split_count", 12)
        self.start_time = self.config.get("start_time", "09:35")
        self.end_time = self.config.get("end_time", "14:50")
        self.period = self.config.get("period", "5m")
        self.symbols = self.config.get("symbols", [])
        self.price_type = self.config.get("price_type", "LIMIT")
        self.lot_size = self.config.get("lot_size", 100)

        # 目标订单：{symbol: {"side": "BUY"/"SELL", "total_quantity": int}}
        self.target_orders: Dict[str, Dict[str, Any]] = self.config.get("target_orders", {})

        # 执行状态追踪：{symbol: {"executed": int, "step": int}}
        self._execution_state: Dict[str, Dict[str, Any]] = {}
        for sym, order in self.target_orders.items():
            self._execution_state[sym] = {
                "executed": 0,
                "step": 0,
                "total": order.get("total_quantity", 0),
                "side": order.get("side", "BUY"),
            }

    def _is_within_time_window(self) -> bool:
        """检查当前是否在执行时间窗口内"""
        now = datetime.now().time()
        start = dt_time(*map(int, self.start_time.split(":")))
        end = dt_time(*map(int, self.end_time.split(":")))
        return start <= now <= end

    def run(self, symbol: str) -> Dict[str, Any]:
        """每个调度周期执行一次，发出一份拆单"""
        if not self._is_within_time_window():
            return {"success": True, "signals": [], "message": "不在执行时间窗口内"}

        state = self._execution_state.get(symbol)
        if state is None:
            return {"success": False, "signals": [], "message": f"{symbol} 无目标订单"}

        total = state["total"]
        executed = state["executed"]
        remaining = total - executed
        step = state["step"]

        if remaining <= 0:
            return {"success": True, "signals": [], "message": f"{symbol} 已执行完毕"}

        # 获取当前价格
        data = self._fetch_current_price(symbol)
        if data is None:
            return {"success": False, "signals": [], "message": f"无法获取 {symbol} 当前价格"}

        signal = self.generate_signal({
            "symbol": symbol,
            "data": data,
            "state": state,
        })

        if signal is None:
            return {"success": True, "signals": [], "message": f"{symbol} 本步无信号"}

        # 更新状态
        state["executed"] += signal.quantity
        state["step"] += 1

        # 执行信号
        result = self.execute_signal(signal)
        return {
            "success": result.get("success", False),
            "signals": [signal.__dict__],
            "message": result.get("message", ""),
            "order_id": result.get("order_id"),
            "progress": f"{state['executed']}/{total} (step {state['step']}/{self.split_count})",
        }

    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """计算本步应执行的数量并生成信号"""
        symbol = data.get("symbol")
        price_data = data.get("data")
        state = data.get("state")

        if price_data is None or state is None:
            return None

        total = state["total"]
        executed = state["executed"]
        remaining = total - executed
        step = state["step"]
        side = state["side"]

        if remaining <= 0:
            return None

        # TWAP 核心逻辑：均匀拆分
        steps_remaining = self.split_count - step
        if steps_remaining <= 0:
            steps_remaining = 1

        # 最后一步：强制执行全部剩余
        if step >= self.split_count - 1:
            step_quantity = remaining
        else:
            # 期望已执行量
            expected_executed = total / self.split_count * (step + 1)
            step_quantity = expected_executed - executed

        # 按 lot_size 取整
        step_quantity = int(np.floor(step_quantity / self.lot_size)) * self.lot_size
        step_quantity = min(step_quantity, remaining)

        if step_quantity <= 0:
            return None

        # 获取当前价格
        current_price = float(price_data.iloc[-1]["close"]) if isinstance(price_data, pd.DataFrame) else float(price_data)

        return TradeSignal(
            strategy_id=self.strategy_id,
            symbol=symbol,
            side=side,
            quantity=step_quantity,
            price_type=self.price_type,
            price=current_price,
            reason=f"TWAP step {step + 1}/{self.split_count}, 本次 {step_quantity} 股",
            signal_data={
                "step": step + 1,
                "split_count": self.split_count,
                "step_quantity": step_quantity,
                "total_quantity": total,
                "executed": executed,
                "remaining": remaining,
                "price": current_price,
            },
        )

    def _fetch_current_price(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取当前分钟级价格数据"""
        try:
            df = data_api.get_intraday_window(
                [symbol],
                bars=5,
                fields=["open", "high", "low", "close", "volume", "amount"],
                freq=self.period,
            )
            if df is None or df.empty:
                return None
            try:
                return df.xs(symbol, level="instrument")
            except Exception:
                return None
        except Exception as e:
            logger.error(f"获取分钟数据失败 {symbol}: {e}", exc_info=True)
            return None

    def reset_orders(self, target_orders: Dict[str, Dict[str, Any]]) -> None:
        """重置目标订单（用于每日开盘前更新）"""
        self.target_orders = target_orders
        self._execution_state.clear()
        for sym, order in target_orders.items():
            self._execution_state[sym] = {
                "executed": 0,
                "step": 0,
                "total": order.get("total_quantity", 0),
                "side": order.get("side", "BUY"),
            }
