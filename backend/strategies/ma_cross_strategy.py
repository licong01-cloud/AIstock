"""双均线交叉策略（QMT 交易系统）"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pandas as pd

from .base_strategy import BaseStrategy, TradeSignal
from ..data_service import api as data_api
from ..infra.qmt_client import QMTNotAvailableError, get_qmt_client_singleton


class MACrossStrategy(BaseStrategy):
    """双均线交叉策略

    策略逻辑：
    - MA5 上穿 MA20：买入信号
    - MA5 下穿 MA20：卖出信号
    """

    def __init__(
        self,
        strategy_id: str = "ma_cross_001",
        executor=None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """初始化双均线策略

        Args:
            strategy_id: 策略ID
            executor: 策略执行器
            config: 策略配置，包含：
                - ma_short: 短期均线周期（默认5）
                - ma_long: 长期均线周期（默认20）
                - period: 数据周期（默认"1d"，支持"15m", "1m", "5m", "30m", "1d"等）
                - symbols: 股票代码列表
                - position_size: 仓位大小（0-1，默认0.1）
                - price_type: 价格类型（LIMIT/MARKET，默认LIMIT）
        """
        super().__init__(strategy_id, executor, config)
        self.ma_short = self.config.get("ma_short", 5)
        self.ma_long = self.config.get("ma_long", 20)
        self.period = self.config.get("period", "1d")  # 数据周期：15m, 1d等
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

        if df is None or df.empty or len(df) < self.ma_long:
            return None

        # 计算均线
        df["ma_short"] = df["close"].rolling(window=self.ma_short).mean()
        df["ma_long"] = df["close"].rolling(window=self.ma_long).mean()

        # 获取最近两条数据
        if len(df) < 2:
            return None

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # 判断交叉
        # MA5 上穿 MA20：买入信号
        if (
            prev["ma_short"] <= prev["ma_long"]
            and latest["ma_short"] > latest["ma_long"]
        ):
            current_price = float(latest["close"])
            quantity = self._calculate_quantity(symbol, current_price)

            if quantity > 0:
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="BUY",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"MA{self.ma_short}上穿MA{self.ma_long}",
                    signal_data={
                        "ma_short": float(latest["ma_short"]),
                        "ma_long": float(latest["ma_long"]),
                        "price": current_price,
                    },
                )

        # MA5 下穿 MA20：卖出信号
        elif (
            prev["ma_short"] >= prev["ma_long"]
            and latest["ma_short"] < latest["ma_long"]
        ):
            current_price = float(latest["close"])
            quantity = self._calculate_sell_quantity(symbol)

            if quantity > 0:
                return TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="SELL",
                    quantity=quantity,
                    price_type=self.price_type,
                    price=current_price,
                    reason=f"MA{self.ma_short}下穿MA{self.ma_long}",
                    signal_data={
                        "ma_short": float(latest["ma_short"]),
                        "ma_long": float(latest["ma_long"]),
                        "price": current_price,
                    },
                )

        return None

    def _fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """通过数据服务层获取股票历史数据。

        - 对于日线周期（"1d"），使用 get_history_window；
        - 对于分钟/其他周期（"1m"/"5m"/"15m"/"30m" 等），使用
          get_intraday_window；
        - 不再直接依赖 UnifiedDataAccess 或 xtquant 低层 API。
        """
        try:
            universe = [symbol]

            # 为了计算均线，至少需要 ma_long 条，额外多取一些做缓冲。
            bars = max(self.ma_long + 5, self.ma_long * 2)

            if self.period in ["1m", "5m", "15m", "30m", "60m"]:
                df = data_api.get_intraday_window(
                    universe,
                    bars=bars,
                    fields=["open", "high", "low", "close", "volume", "amount"],
                    freq=self.period if self.period != "60m" else "60m",
                )
            else:
                # 默认按日线处理
                df = data_api.get_history_window(
                    universe,
                    bars=bars,
                    fields=["open", "high", "low", "close", "volume", "amount"],
                    freq="1d",
                )

            if df is None or df.empty:
                return None

            # get_*_window 返回 MultiIndex(datetime, instrument)，取出单标的
            try:
                df_symbol = df.xs(symbol, level="instrument")
            except Exception:
                # 若索引结构异常，直接返回 None 以避免错误信号
                return None

            if df_symbol is None or df_symbol.empty:
                return None

            # 确保有 close 列
            if "close" not in df_symbol.columns and "Close" in df_symbol.columns:
                df_symbol = df_symbol.rename(columns={"Close": "close"})

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

        except QMTNotAvailableError as e:
            try:
                client = get_qmt_client_singleton()
                st = client.status()
                self.logger.warning(
                    "QMT 未连接，无法计算买入数量: %s | pid=%s client_object_id=%s client_class=%s status=%s",
                    str(e),
                    os.getpid(),
                    hex(id(client)),
                    f"{client.__class__.__module__}.{client.__class__.__name__}",
                    st.__dict__,
                )
            except Exception:
                self.logger.warning(f"QMT 未连接，无法计算买入数量: {e}")
            return 0
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

