"""波动率突破策略（日内分钟级）

移植自 Qlib ACStrategy（Almgren-Chriss 最优执行），适配 AIStock BaseStrategy 框架。

原版 AC 策略的核心思想：
- 计算股票的历史波动率（对数收益率标准差）
- 高波动期间减少交易（降低市场冲击）
- 低波动期间加大交易（捕捉趋势突破）

本适配版将波动率模型转化为独立的日内交易信号策略：
- 计算分钟级滚动波动率
- 波动率从高位回落（压缩）→ 趋势酝酿 → 准备建仓
- 价格突破波动率通道上轨 → BUY
- 价格跌破波动率通道下轨 → SELL
- 波动率骤升（扩展）→ 加速执行或止损

波动率计算公式（与 Qlib ACStrategy 完全一致）：
  sigma = sqrt(sum(log(close/prev_close)^2, window) / (window-1)
          - sum(log(close/prev_close), window)^2 / (window*(window-1)))

典型配置：
  schedule_config: {"type": "minute", "interval": 5}
  config: {
      "symbols": ["600519.SH", "000001.SZ"],
      "vol_window": 20,          # 波动率计算窗口
      "breakout_multiplier": 2.0, # 突破通道倍数（N倍sigma）
      "vol_squeeze_threshold": 0.3, # 波动率压缩阈值（当前vol/历史vol）
      "period": "5m",
      "position_size": 0.1,
      "price_type": "LIMIT"
  }

来源：Qlib qlib.contrib.strategy.rule_strategy.ACStrategy
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .base_strategy import BaseStrategy, TradeSignal
from ..data_service import api as data_api

logger = logging.getLogger("strategy.volatility_breakout")


class VolatilityBreakoutStrategy(BaseStrategy):
    """波动率突破策略（分钟级）

    基于 Almgren-Chriss 波动率模型的趋势突破信号。
    """

    def __init__(
        self,
        strategy_id: str = "vol_breakout_001",
        executor=None,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(strategy_id, executor, config)
        self.vol_window = self.config.get("vol_window", 20)
        self.breakout_multiplier = self.config.get("breakout_multiplier", 2.0)
        self.vol_squeeze_threshold = self.config.get("vol_squeeze_threshold", 0.3)
        self.period = self.config.get("period", "5m")
        self.symbols = self.config.get("symbols", [])
        self.position_size = self.config.get("position_size", 0.1)
        self.price_type = self.config.get("price_type", "LIMIT")

        # 状态追踪
        self._last_signal: Dict[str, str] = {}  # symbol -> "BUY"/"SELL"/None

    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略：获取分钟数据 → 计算波动率 → 判断突破 → 生成信号"""
        try:
            data = self._fetch_stock_data(symbol)
            if data is None or data.empty:
                return {"success": False, "signals": [], "message": f"无法获取 {symbol} 的分钟数据"}

            signal = self.generate_signal({"symbol": symbol, "data": data})

            if signal is None:
                return {"success": True, "signals": [], "message": f"{symbol} 无突破信号"}

            result = self.execute_signal(signal)
            return {
                "success": result.get("success", False),
                "signals": [signal.__dict__],
                "message": result.get("message", ""),
                "order_id": result.get("order_id"),
            }

        except Exception as e:
            self.logger.error(f"波动率突破策略执行异常: {e}", exc_info=True)
            return {"success": False, "signals": [], "message": f"执行异常: {str(e)}"}

    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """基于波动率通道突破生成交易信号

        波动率计算复用 Qlib ACStrategy._reset_signal 的公式：
        sigma = sqrt(sum(log_ret^2, w)/(w-1) - sum(log_ret, w)^2/(w*(w-1)))
        """
        symbol = data.get("symbol")
        df = data.get("data")

        if df is None or df.empty or len(df) < self.vol_window + 5:
            return None

        close = df["close"].astype(float)

        # 计算对数收益率
        log_ret = np.log(close / close.shift(1))
        log_ret = log_ret.dropna()

        if len(log_ret) < self.vol_window:
            return None

        # 波动率计算（与 Qlib ACStrategy 完全一致）
        w = self.vol_window
        sum_sq = log_ret.rolling(w).apply(lambda x: (x ** 2).sum(), raw=True)
        sum_lr = log_ret.rolling(w).sum()
        variance = sum_sq / (w - 1) - sum_lr ** 2 / (w * (w - 1))
        # 防止负数（浮点精度问题）
        volatility = np.sqrt(variance.clip(lower=0))

        current_vol = float(volatility.iloc[-1])
        if np.isnan(current_vol) or current_vol <= 0:
            return None

        # 历史波动率均值（用于判断压缩/扩展）
        hist_vol_mean = float(volatility.iloc[-min(60, len(volatility)):].mean())
        vol_ratio = current_vol / hist_vol_mean if hist_vol_mean > 0 else 1.0

        # 当前价格和移动平均
        current_price = float(close.iloc[-1])
        ma = float(close.rolling(self.vol_window).mean().iloc[-1])

        if np.isnan(ma) or ma <= 0:
            return None

        # 波动率通道
        upper_band = ma + self.breakout_multiplier * current_vol * ma
        lower_band = ma - self.breakout_multiplier * current_vol * ma

        prev_price = float(close.iloc[-2])
        last_signal = self._last_signal.get(symbol)

        signal_data = {
            "volatility": current_vol,
            "hist_vol_mean": hist_vol_mean,
            "vol_ratio": vol_ratio,
            "upper_band": upper_band,
            "lower_band": lower_band,
            "ma": ma,
            "price": current_price,
            "is_squeeze": vol_ratio < self.vol_squeeze_threshold,
        }

        # 突破上轨 → BUY（且之前没买过或上次是卖出）
        if current_price > upper_band and prev_price <= upper_band and last_signal != "BUY":
            quantity = self._calculate_quantity(symbol, current_price)
            if quantity > 0:
                self._last_signal[symbol] = "BUY"
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="BUY",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"价格突破波动率上轨 (σ={current_vol:.6f}, ratio={vol_ratio:.2f})",
                    signal_data=signal_data,
                )

        # 跌破下轨 → SELL
        elif current_price < lower_band and prev_price >= lower_band and last_signal != "SELL":
            quantity = self._calculate_sell_quantity(symbol)
            if quantity > 0:
                self._last_signal[symbol] = "SELL"
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="SELL",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"价格跌破波动率下轨 (σ={current_vol:.6f}, ratio={vol_ratio:.2f})",
                    signal_data=signal_data,
                )

        return None

    def _fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取分钟级 K 线数据"""
        try:
            bars = max(self.vol_window * 5, 100)

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
