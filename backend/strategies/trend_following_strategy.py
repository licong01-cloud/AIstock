"""趋势跟踪策略（QMT 交易系统）"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from .base_strategy import BaseStrategy, TradeSignal
from ..data_service import api as data_api


class TrendFollowingStrategy(BaseStrategy):
    """趋势跟踪策略

    策略逻辑：
    - 价格突破N日均线且成交量放大：买入信号
    - 价格跌破N日均线：卖出信号
    """

    def __init__(
        self,
        strategy_id: str = "trend_following_001",
        executor=None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """初始化趋势跟踪策略

        Args:
            strategy_id: 策略ID
            executor: 策略执行器
            config: 策略配置，包含：
                - ma_period: 均线周期（默认20）
                - volume_ratio: 成交量放大倍数（默认1.5）
                - symbols: 股票代码列表
                - position_size: 仓位大小（0-1，默认0.1）
                - price_type: 价格类型（LIMIT/MARKET，默认LIMIT）
        """
        super().__init__(strategy_id, executor, config)
        self.ma_period = self.config.get("ma_period", 20)
        self.volume_ratio = self.config.get("volume_ratio", 1.5)
        self.symbols = self.config.get("symbols", [])
        self.position_size = self.config.get("position_size", 0.1)
        self.price_type = self.config.get("price_type", "LIMIT")

    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略

        Args:
            symbol: 股票代码（如 "600519.SH"）

        Returns:
            执行结果字典
        """
        try:
            # 1. 获取股票数据
            data = self._fetch_stock_data(symbol)
            if data is None or data.empty:
                return {
                    "success": False,
                    "signals": [],
                    "message": f"无法获取 {symbol} 的数据",
                }

            # 2. 生成交易信号
            signal = self.generate_signal({"symbol": symbol, "data": data})

            if signal is None:
                return {
                    "success": True,
                    "signals": [],
                    "message": f"{symbol} 无交易信号",
                }

            # 3. 执行信号
            result = self.execute_signal(signal)

            return {
                "success": result.get("success", False),
                "signals": [signal.__dict__],
                "message": result.get("message", ""),
                "order_id": result.get("order_id"),
            }

        except Exception as e:
            self.logger.error(f"策略执行异常: {e}", exc_info=True)
            return {
                "success": False,
                "signals": [],
                "message": f"执行异常: {str(e)}",
            }

    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """生成交易信号

        Args:
            data: 包含 symbol 和 data (DataFrame) 的字典

        Returns:
            交易信号（如果无信号则返回 None）
        """
        symbol = data.get("symbol")
        df = data.get("data")

        if df is None or df.empty or len(df) < self.ma_period + 1:
            return None

        # 计算均线和成交量均值
        df["ma"] = df["close"].rolling(window=self.ma_period).mean()
        df["volume_ma"] = df["volume"].rolling(window=self.ma_period).mean()

        # 获取最近两条数据
        if len(df) < 2:
            return None

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        current_price = float(latest["close"])
        current_volume = float(latest["volume"])
        volume_ma = float(latest["volume_ma"])

        # 买入信号：价格突破均线且成交量放大
        if (
            prev["close"] <= prev["ma"]
            and latest["close"] > latest["ma"]
            and current_volume > volume_ma * self.volume_ratio
        ):
            quantity = self._calculate_quantity(symbol, current_price)

            if quantity > 0:
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="BUY",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"价格突破MA{self.ma_period}且成交量放大",
                    signal_data={
                        "ma": float(latest["ma"]),
                        "price": current_price,
                        "volume_ratio": current_volume / volume_ma if volume_ma > 0 else 0,
                    },
                )

        # 卖出信号：价格跌破均线
        elif prev["close"] >= prev["ma"] and latest["close"] < latest["ma"]:
            quantity = self._calculate_sell_quantity(symbol)

            if quantity > 0:
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="SELL",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"价格跌破MA{self.ma_period}",
                    signal_data={
                        "ma": float(latest["ma"]),
                        "price": current_price,
                    },
                )

        return None

    def _fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """通过数据服务层获取股票历史数据（用于趋势判断）。

        - 使用 get_history_window 获取最近一段日线数据（约 3 个月，按
          bars 数量近似）；
        - 不再依赖 UnifiedDataAccess 或实时订阅，避免混用多套数据。
        """
        try:
            universe = [symbol]

            # 为了计算均线和成交量均值，需要 ma_period 条以上的历史，额外
            # 多取一些以增强鲁棒性。
            bars = max(self.ma_period + 10, self.ma_period * 3)

            df = data_api.get_history_window(
                universe,
                bars=bars,
                fields=["open", "high", "low", "close", "volume", "amount"],
                freq="1d",
            )

            if df is None or df.empty:
                return None

            # MultiIndex(datetime, instrument) → 单标的 DataFrame
            try:
                df_symbol = df.xs(symbol, level="instrument")
            except Exception:
                return None

            if df_symbol is None or df_symbol.empty:
                return None

            # 确保有 close / volume 列
            if "close" not in df_symbol.columns and "Close" in df_symbol.columns:
                df_symbol = df_symbol.rename(columns={"Close": "close"})
            if "volume" not in df_symbol.columns and "Volume" in df_symbol.columns:
                df_symbol = df_symbol.rename(columns={"Volume": "volume"})

            return df_symbol

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

            # 使用仓位大小计算
            target_amount = available_cash * self.position_size
            quantity = int(target_amount / price / 100) * 100  # 取整到100股

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

