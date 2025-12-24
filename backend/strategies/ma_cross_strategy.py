"""双均线交叉策略（QMT 交易系统）"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from .base_strategy import BaseStrategy, TradeSignal


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
        """获取股票历史数据（历史K线 + 实时行情更新）
        
        数据来源优先级：
        1. 历史K线数据（根据period配置）- 用于计算均线
        2. xtquant 实时行情（如果可用）- 更新最新价格
        3. TDX/Tushare 实时行情（兜底）
        
        支持的周期：
        - 15m: 15分钟线（日内交易）
        - 1d: 日线（日频交易）
        - 1m, 5m, 30m等
        """
        try:
            from ..core.unified_data_access_impl import UnifiedDataAccess

            # 优先尝试通过 xtquant 补充所需周期的历史数据（如果可用）
            # 参考 xtdata 文档：支持的周期包括 1m / 5m / 15m / 30m / 1d 等
            try:  # best-effort，不影响后续 UnifiedDataAccess 兜底
                import xtquant.xtdata as xtdata  # type: ignore[import]

                if self.period in ["1m", "5m", "15m", "30m"]:
                    # 补齐对应分钟线历史数据
                    self.logger.info(
                        "准备通过 xtquant 下载 %s 的 %s K 线历史数据", symbol, self.period
                    )
                    xtdata.download_history_data(symbol, self.period)
                    self.logger.info(
                        "xtquant 下载 %s 的 %s K 线历史数据完成（如有缺失将按需补齐）",
                        symbol,
                        self.period,
                    )
                elif self.period == "1d":
                    # 日线策略，补齐日线历史
                    self.logger.info("准备通过 xtquant 下载 %s 的日线历史数据", symbol)
                    xtdata.download_history_data(symbol, "1d")
                    self.logger.info(
                        "xtquant 下载 %s 的日线历史数据完成（如有缺失将按需补齐）",
                        symbol,
                    )
            except Exception as e:
                # xtquant 不可用或补数据失败时，记录 warning 并回退到 UnifiedDataAccess
                self.logger.warning(
                    "通过 xtquant 下载 %s 的 %s 历史数据失败，将回退到 UnifiedDataAccess: %s",
                    symbol,
                    self.period,
                    e,
                )

            data_access = UnifiedDataAccess()
            
            # 根据周期确定历史数据范围
            # 15分钟线：获取最近1个月的数据
            # 日线：获取最近3个月的数据
            if self.period == "15m":
                hist_period = "1mo"
            elif self.period in ["1m", "5m"]:
                hist_period = "1mo"
            else:
                hist_period = "3mo"
            
            df = data_access.get_stock_data(symbol, period=hist_period)

            if df is None or df.empty:
                return None

            # 确保有 close 列
            if "close" not in df.columns and "Close" in df.columns:
                df = df.rename(columns={"Close": "close"})
            
            # 如果配置了分钟线（1m/5m/15m/30m），尝试从 xtquant 获取对应周期 K 线数据
            if self.period in ["1m", "5m", "15m", "30m"]:
                try:
                    import xtquant.xtdata as xtdata  # type: ignore[import]

                    kline_data = xtdata.get_market_data(
                        field_list=["time", "close", "open", "high", "low", "volume"],
                        stock_list=[symbol],
                        period=self.period,
                        count=100,
                    )

                    # kline_data 应为 { field: DataFrame }，但需要防御性检查
                    if not kline_data or "close" not in kline_data:
                        self.logger.warning(
                            "xtquant 返回的 %s K 线数据为空或缺少 close 字段: %s",
                            self.period,
                            symbol,
                        )
                    else:
                        df_close = kline_data["close"]
                        if df_close is None or df_close.empty:
                            self.logger.warning(
                                "xtquant %s K 线 close 数据为空: %s",
                                self.period,
                                symbol,
                            )
                        else:
                            import pandas as pd

                            # 取第一个合约（列 0）的时间序列
                            close_series = df_close.iloc[:, 0]
                            open_series = (
                                kline_data["open"].iloc[:, 0]
                                if "open" in kline_data and not kline_data["open"].empty
                                else None
                            )
                            high_series = (
                                kline_data["high"].iloc[:, 0]
                                if "high" in kline_data and not kline_data["high"].empty
                                else None
                            )
                            low_series = (
                                kline_data["low"].iloc[:, 0]
                                if "low" in kline_data and not kline_data["low"].empty
                                else None
                            )
                            volume_series = (
                                kline_data["volume"].iloc[:, 0]
                                if "volume" in kline_data and not kline_data["volume"].empty
                                else None
                            )

                            df_kline = pd.DataFrame({"close": close_series})
                            if open_series is not None:
                                df_kline["open"] = open_series
                            if high_series is not None:
                                df_kline["high"] = high_series
                            if low_series is not None:
                                df_kline["low"] = low_series
                            if volume_series is not None:
                                df_kline["volume"] = volume_series

                            if "time" in kline_data and not kline_data["time"].empty:
                                df_kline.index = kline_data["time"].iloc[:, 0]

                            if not df_kline.empty:
                                self.logger.info(
                                    "从xtquant获取%s K线数据: %d根", self.period, len(df_kline)
                                )
                                df = df_kline
                            else:
                                self.logger.warning(
                                    "xtquant %s K 线转换后为空: %s", self.period, symbol
                                )
                except Exception as e:
                    self.logger.warning(
                        "从xtquant获取%s K线失败，使用 UnifiedDataAccess 历史数据: %s",
                        self.period,
                        e,
                    )

            # 优先使用 xtquant 获取实时行情（从 miniQMT）
            current_price = None
            try:
                from ..infra.realtime_quote_subscriber import get_realtime_quote_subscriber
                subscriber = get_realtime_quote_subscriber()
                quote = subscriber.get_latest_quote(symbol)
                if quote:
                    # 字段对齐：xtquant返回lastPrice，策略使用close
                    current_price = quote.get("close") or quote.get("lastPrice")
                    self.logger.info(f"从 xtquant 获取 {symbol} 实时价格: {current_price}")
            except Exception as e:
                self.logger.debug(f"xtquant 实时行情不可用: {e}")

            # 如果 xtquant 不可用，使用 TDX/Tushare 实时行情
            if current_price is None:
                try:
                    realtime_quote = data_access.get_realtime_quotes(symbol)
                    if realtime_quote:
                        # 字段对齐：TDX/Tushare返回price，策略使用close
                        current_price = realtime_quote.get("price")
                        self.logger.info(f"从 TDX/Tushare 获取 {symbol} 实时价格: {current_price}")
                except Exception as e:
                    self.logger.warning(f"获取实时行情失败，使用历史数据: {e}")

            # 更新最新价格（字段对齐：统一使用close字段）
            if current_price and len(df) > 0:
                last_price = float(df.iloc[-1]["close"])
                if abs(last_price - current_price) > 0.01:
                    df.iloc[-1, df.columns.get_loc("close")] = current_price
                    self.logger.info(f"已更新 {symbol} 最新价格: {last_price} -> {current_price}")

            return df

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

