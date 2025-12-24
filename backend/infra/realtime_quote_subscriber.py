"""实时行情订阅服务（使用 xtquant）

提供从 miniQMT 获取实时行情的功能。
"""
from __future__ import annotations

import logging
import threading
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import xtquant.xtdata as xtdata
    XTDATA_AVAILABLE = True
except ImportError:
    XTDATA_AVAILABLE = False
    logger.warning("xtquant.xtdata 不可用，实时行情订阅功能将不可用")


class RealtimeQuoteSubscriber:
    """实时行情订阅服务"""

    def __init__(self):
        self.subscriptions: Dict[int, List[str]] = {}  # seq -> stocks
        self.callbacks: Dict[str, List[Callable]] = {}  # stock -> callbacks
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def subscribe(self, stocks: List[str], callback: Callable) -> Optional[int]:
        """订阅股票实时行情

        Args:
            stocks: 股票代码列表，如 ["600519.SH", "000001.SZ"]
            callback: 回调函数，格式: callback(stock_code: str, quote: dict)

        Returns:
            订阅号（成功返回 > 0，失败返回 None）
        """
        if not XTDATA_AVAILABLE:
            logger.error("xtquant.xtdata 不可用，无法订阅实时行情")
            return None

        try:
            # 注册回调
            with self._lock:
                for stock in stocks:
                    if stock not in self.callbacks:
                        self.callbacks[stock] = []
                    self.callbacks[stock].append(callback)

            # 订阅全推行情
            seq = xtdata.subscribe_whole_quote(
                code_list=stocks,
                callback=self._on_quote,
            )

            if seq > 0:
                with self._lock:
                    self.subscriptions[seq] = stocks
                logger.info(f"成功订阅实时行情: {stocks}, 订阅号: {seq}")
                return seq
            else:
                logger.error(f"订阅实时行情失败: {stocks}")
                return None

        except Exception as e:
            logger.error(f"订阅实时行情异常: {e}", exc_info=True)
            return None

    def _on_quote(self, datas: Dict):
        """行情回调"""
        try:
            for stock_code, quote in datas.items():
                with self._lock:
                    callbacks = self.callbacks.get(stock_code, [])

                for callback in callbacks:
                    try:
                        callback(stock_code, quote)
                    except Exception as e:
                        logger.error(
                            f"行情回调执行失败 {stock_code}: {e}", exc_info=True
                        )
        except Exception as e:
            logger.error(f"处理行情回调异常: {e}", exc_info=True)

    def unsubscribe(self, seq: int) -> bool:
        """取消订阅

        Args:
            seq: 订阅号

        Returns:
            是否成功
        """
        if not XTDATA_AVAILABLE:
            return False

        try:
            with self._lock:
                if seq in self.subscriptions:
                    stocks = self.subscriptions[seq]
                    xtdata.unsubscribe_quote(seq)
                    del self.subscriptions[seq]

                    # 清理回调
                    for stock in stocks:
                        if stock in self.callbacks:
                            del self.callbacks[stock]

                    logger.info(f"已取消订阅: {stocks}, 订阅号: {seq}")
                    return True
                else:
                    logger.warning(f"订阅号不存在: {seq}")
                    return False

        except Exception as e:
            logger.error(f"取消订阅异常: {e}", exc_info=True)
            return False

    def start(self):
        """启动订阅服务（在后台线程运行）"""
        if self.running:
            return

        if not XTDATA_AVAILABLE:
            logger.error("xtquant.xtdata 不可用，无法启动订阅服务")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run, name="realtime-quote-subscriber", daemon=True)
        self.thread.start()
        logger.info("实时行情订阅服务已启动")

    def stop(self):
        """停止订阅服务"""
        if not self.running:
            return

        self.running = False

        # 取消所有订阅
        with self._lock:
            for seq in list(self.subscriptions.keys()):
                self.unsubscribe(seq)

        logger.info("实时行情订阅服务已停止")

    def _run(self):
        """运行订阅循环"""
        try:
            xtdata.run()  # 阻塞运行，持续接收回调
        except Exception as e:
            logger.error(f"实时行情订阅服务异常: {e}", exc_info=True)
        finally:
            self.running = False

    def get_latest_quote(self, stock_code: str) -> Optional[Dict]:
        """获取最新行情（从缓存）

        Args:
            stock_code: 股票代码

        Returns:
            行情数据字典，字段对齐策略使用：
            - lastPrice -> close (最新价)
            - volume -> volume (成交量)
            - open, high, low, amount 等
        """
        if not XTDATA_AVAILABLE:
            return None

        try:
            # 获取最新tick数据
            data = xtdata.get_market_data(
                field_list=["time", "lastPrice", "open", "high", "low", "volume", "amount"],
                stock_list=[stock_code],
                period="tick",
                count=1,
            )

            if data and "lastPrice" in data:
                df_price = data["lastPrice"]
                df_time = data.get("time")
                df_volume = data.get("volume")

                if not df_price.empty:
                    quote = {
                        "time": int(df_time.iloc[0, 0]) if df_time is not None and not df_time.empty else None,
                        "lastPrice": float(df_price.iloc[0, 0]),  # xtquant字段：最新价
                        "close": float(df_price.iloc[0, 0]),      # 对齐策略字段：收盘价
                        "volume": float(df_volume.iloc[0, 0]) if df_volume is not None and not df_volume.empty else None,
                        "open": float(data.get("open").iloc[0, 0]) if "open" in data and not data["open"].empty else None,
                        "high": float(data.get("high").iloc[0, 0]) if "high" in data and not data["high"].empty else None,
                        "low": float(data.get("low").iloc[0, 0]) if "low" in data and not data["low"].empty else None,
                        "amount": float(data.get("amount").iloc[0, 0]) if "amount" in data and not data["amount"].empty else None,
                    }
                    return quote

        except Exception as e:
            logger.error(f"获取最新行情失败 {stock_code}: {e}", exc_info=True)

        return None


# 全局订阅服务实例
_subscriber_instance: Optional[RealtimeQuoteSubscriber] = None


def get_realtime_quote_subscriber() -> RealtimeQuoteSubscriber:
    """获取全局实时行情订阅服务实例"""
    global _subscriber_instance
    if _subscriber_instance is None:
        _subscriber_instance = RealtimeQuoteSubscriber()
    return _subscriber_instance

