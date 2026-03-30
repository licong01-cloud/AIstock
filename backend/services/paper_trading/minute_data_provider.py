"""分钟行情数据统一提供者 — 解决 kline_minute_raw 列名映射和单位转换.

支持两种数据源:
- DB (kline_minute_raw): 历史分钟线，收盘后入库
- TDX 分钟 K 线接口: 开盘期间当天实时分钟线 (/api/kline?type=minute1)
"""
from __future__ import annotations

import logging
from datetime import date
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from ...db.pg_pool import get_conn
from ...data_service.tdx_adapter import fetch_minute_kline_batch_tdx

logger = logging.getLogger("aistock.paper_trading.minute_data")

# kline_minute_raw 价格存储单位为厘（1/1000 元），kline_daily_raw 已是元无需转换
PRICE_UNIT_DIVISOR = 1000.0


class MinuteDataProvider:
    """分钟行情数据统一提供者."""

    @staticmethod
    def fetch_all_bars(
        symbols: List[str], trade_date: date,
    ) -> List[Tuple[object, Dict[str, Dict[str, float]]]]:
        """获取当日全部分钟 bar，按 trade_time 升序分组.

        Returns:
            [(trade_time, {symbol: {open, high, low, close, volume}}), ...]
            价格单位：元
        """
        if not symbols:
            return []

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_time, ts_code,
                           open_li, high_li, low_li, close_li, volume_hand
                    FROM market.kline_minute_raw
                    WHERE ts_code = ANY(%s)
                      AND trade_time >= %s::date
                      AND trade_time < %s::date + interval '1 day'
                    ORDER BY trade_time ASC
                    """,
                    (symbols, trade_date, trade_date),
                )
                rows = cur.fetchall()

        if not rows:
            return []

        time_groups: OrderedDict = OrderedDict()
        for row in rows:
            t = row[0]
            if t not in time_groups:
                time_groups[t] = {}
            time_groups[t][row[1]] = {
                "open": float(row[2]) / PRICE_UNIT_DIVISOR if row[2] else 0,
                "high": float(row[3]) / PRICE_UNIT_DIVISOR if row[3] else 0,
                "low": float(row[4]) / PRICE_UNIT_DIVISOR if row[4] else 0,
                "close": float(row[5]) / PRICE_UNIT_DIVISOR if row[5] else 0,
                "volume": float(row[6]) if row[6] else 0,
            }

        return list(time_groups.items())

    @staticmethod
    def fetch_latest_bar(
        symbols: List[str], trade_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """获取最新 bar（DISTINCT ON 取每个 symbol 最新一条）.

        Returns:
            {symbol: {open, high, low, close, volume}}
            价格单位：元
        """
        if not symbols:
            return {}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (ts_code)
                        ts_code, open_li, high_li, low_li, close_li, volume_hand
                    FROM market.kline_minute_raw
                    WHERE ts_code = ANY(%s)
                      AND trade_time >= %s::date
                      AND trade_time < %s::date + interval '1 day'
                    ORDER BY ts_code, trade_time DESC
                    """,
                    (symbols, trade_date, trade_date),
                )
                result = {}
                for row in cur.fetchall():
                    result[row[0]] = {
                        "open": float(row[1]) / PRICE_UNIT_DIVISOR if row[1] else 0,
                        "high": float(row[2]) / PRICE_UNIT_DIVISOR if row[2] else 0,
                        "low": float(row[3]) / PRICE_UNIT_DIVISOR if row[3] else 0,
                        "close": float(row[4]) / PRICE_UNIT_DIVISOR if row[4] else 0,
                        "volume": float(row[5]) if row[5] else 0,
                    }
        return result

    @staticmethod
    def fetch_daily_close(
        symbols: List[str], trade_date: date,
    ) -> Dict[str, float]:
        """获取日线收盘价（元）.

        注意: kline_daily_raw.close 已经是元，无需单位转换。
        （仅 kline_minute_raw 的 open_li/high_li/low_li/close_li 是厘）

        Returns:
            {symbol: close_price}
        """
        if not symbols:
            return {}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code, close
                    FROM market.kline_daily_raw
                    WHERE ts_code = ANY(%s) AND trade_date = %s
                    """,
                    (symbols, trade_date),
                )
                return {
                    r[0]: float(r[1])
                    for r in cur.fetchall() if r[1]
                }

    @staticmethod
    def fetch_latest_bar_tdx(
        symbols: List[str], trade_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """从 TDX 分钟 K 线接口获取每只股票的最新 bar.

        用于开盘期间获取当天实时分钟线数据（此时 kline_minute_raw 尚无当天数据）。
        调用 /api/kline?type=minute1 获取完整分钟 K 线，取每只股票最后一根 bar。

        Returns:
            {symbol: {open, high, low, close, volume}}
            价格单位：元
        """
        if not symbols:
            return {}

        batch = fetch_minute_kline_batch_tdx(symbols, trade_date)
        result: Dict[str, Dict[str, float]] = {}
        for sym, bars in batch.items():
            if bars:
                last = bars[-1]  # 已按时间升序，最后一根即最新
                result[sym] = {
                    "open": last["open"],
                    "high": last["high"],
                    "low": last["low"],
                    "close": last["close"],
                    "volume": last["volume"],
                }
        return result

    @staticmethod
    def fetch_all_bars_tdx(
        symbols: List[str], trade_date: date,
    ) -> List[Tuple[object, Dict[str, Dict[str, float]]]]:
        """从 TDX 分钟 K 线接口获取当日全部分钟 bar，按时间升序分组.

        与 fetch_all_bars (DB) 返回格式完全一致，用于当天开盘期间。

        Returns:
            [(trade_time, {symbol: {open, high, low, close, volume}}), ...]
            价格单位：元
        """
        if not symbols:
            return []

        batch = fetch_minute_kline_batch_tdx(symbols, trade_date)

        # 将所有股票的 bars 按时间合并分组
        time_groups: OrderedDict = OrderedDict()
        for sym, bars in batch.items():
            for bar in bars:
                t = bar["time"]
                if t not in time_groups:
                    time_groups[t] = {}
                time_groups[t][sym] = {
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                }

        # 按时间排序
        sorted_items = sorted(time_groups.items(), key=lambda x: x[0])
        return sorted_items

    @staticmethod
    def fetch_limit_prices(
        symbols: List[str], trade_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """从 market.stk_limit 获取涨跌停价格（元）.

        Returns:
            {symbol: {"up_limit": float, "down_limit": float}}
        """
        if not symbols:
            return {}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code, up_limit, down_limit
                    FROM market.stk_limit
                    WHERE ts_code = ANY(%s) AND trade_date = %s
                    """,
                    (symbols, trade_date),
                )
                result = {}
                for row in cur.fetchall():
                    if row[1] is not None and row[2] is not None:
                        result[row[0]] = {
                            "up_limit": float(row[1]),
                            "down_limit": float(row[2]),
                        }
        return result
