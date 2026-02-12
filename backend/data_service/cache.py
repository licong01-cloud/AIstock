"""
数据缓存模块

实现同一交易日内的数据缓存，避免重复SQL查询。

功能：
- 缓存同一交易日的 df_history 和 df_fund_raw
- 缓存有效期为当日，次日自动失效
- 支持手动清除缓存
- 支持缓存统计信息查询
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """缓存条目"""
    trade_date: date
    universe_hash: int
    df_history: pd.DataFrame
    df_fund_raw: pd.DataFrame
    created_at: datetime = field(default_factory=datetime.now)
    hit_count: int = 0


class SelectionDataCache:
    """
    选股数据缓存

    缓存同一交易日的SQL查询结果，多次选股复用。

    使用示例:
        cache = SelectionDataCache()

        # 尝试获取缓存
        cached = cache.get(trade_date, universe)
        if cached:
            df_history, df_fund_raw = cached
        else:
            # 缓存未命中，执行查询
            df_history = get_history_window(...)
            df_fund_raw = fetch_fundamental_data_ts(...)
            # 存入缓存
            cache.put(trade_date, universe, df_history, df_fund_raw)
    """

    def __init__(self, max_entries: int = 3):
        """
        初始化缓存

        Args:
            max_entries: 最大缓存条目数，超过时删除最旧的条目
        """
        self._cache: dict[tuple[date, int], CacheEntry] = {}
        self._lock = threading.Lock()
        self._max_entries = max_entries
        self._total_hits = 0
        self._total_misses = 0

    def _compute_universe_hash(self, universe: list[str]) -> int:
        """计算股票池的哈希值"""
        return hash(tuple(sorted(universe)))

    def get(
        self,
        trade_date: date,
        universe: list[str],
    ) -> Optional[tuple[pd.DataFrame, pd.DataFrame]]:
        """
        获取缓存数据

        Args:
            trade_date: 交易日期
            universe: 股票池列表

        Returns:
            (df_history, df_fund_raw) 如果缓存命中，否则返回 None
        """
        if isinstance(trade_date, datetime):
            trade_date = trade_date.date()

        universe_hash = self._compute_universe_hash(universe)
        cache_key = (trade_date, universe_hash)

        with self._lock:
            entry = self._cache.get(cache_key)

            if entry is None:
                self._total_misses += 1
                logger.debug(f"缓存未命中: trade_date={trade_date}, universe_size={len(universe)}")
                return None

            # 检查是否过期（跨日失效）
            if entry.created_at.date() != datetime.now().date():
                logger.info(f"缓存已过期（跨日）: trade_date={trade_date}")
                del self._cache[cache_key]
                self._total_misses += 1
                return None

            # 缓存命中
            entry.hit_count += 1
            self._total_hits += 1
            logger.info(
                f"缓存命中: trade_date={trade_date}, universe_size={len(universe)}, "
                f"hit_count={entry.hit_count}"
            )

            return entry.df_history.copy(), entry.df_fund_raw.copy()

    def put(
        self,
        trade_date: date,
        universe: list[str],
        df_history: pd.DataFrame,
        df_fund_raw: pd.DataFrame,
    ) -> None:
        """
        存入缓存

        Args:
            trade_date: 交易日期
            universe: 股票池列表
            df_history: 历史行情数据
            df_fund_raw: 基本面+资金流数据
        """
        if isinstance(trade_date, datetime):
            trade_date = trade_date.date()

        universe_hash = self._compute_universe_hash(universe)
        cache_key = (trade_date, universe_hash)

        with self._lock:
            # 检查是否超过最大条目数
            if len(self._cache) >= self._max_entries and cache_key not in self._cache:
                # 删除最旧的条目
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
                del self._cache[oldest_key]
                logger.debug(f"缓存已满，删除最旧条目: {oldest_key}")

            # 存入新条目
            self._cache[cache_key] = CacheEntry(
                trade_date=trade_date,
                universe_hash=universe_hash,
                df_history=df_history.copy(),
                df_fund_raw=df_fund_raw.copy(),
            )

            logger.info(
                f"数据已缓存: trade_date={trade_date}, universe_size={len(universe)}, "
                f"df_history_shape={df_history.shape}, df_fund_raw_shape={df_fund_raw.shape}"
            )

    def clear(self) -> int:
        """
        清除所有缓存

        Returns:
            清除的条目数
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"缓存已清除: {count} 条")
            return count

    def clear_expired(self) -> int:
        """
        清除过期缓存（跨日的缓存）

        Returns:
            清除的条目数
        """
        today = datetime.now().date()
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items()
                if v.created_at.date() != today
            ]
            for k in expired_keys:
                del self._cache[k]

            if expired_keys:
                logger.info(f"清除过期缓存: {len(expired_keys)} 条")

            return len(expired_keys)

    def get_stats(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            统计信息字典
        """
        with self._lock:
            total_requests = self._total_hits + self._total_misses
            hit_rate = self._total_hits / total_requests if total_requests > 0 else 0

            entries_info = []
            for key, entry in self._cache.items():
                entries_info.append({
                    "trade_date": str(entry.trade_date),
                    "created_at": entry.created_at.isoformat(),
                    "hit_count": entry.hit_count,
                    "df_history_shape": entry.df_history.shape,
                    "df_fund_raw_shape": entry.df_fund_raw.shape,
                })

            return {
                "total_entries": len(self._cache),
                "max_entries": self._max_entries,
                "total_hits": self._total_hits,
                "total_misses": self._total_misses,
                "hit_rate": f"{hit_rate:.2%}",
                "entries": entries_info,
            }


# 全局缓存实例
_global_cache: Optional[SelectionDataCache] = None
_cache_lock = threading.Lock()


def get_selection_data_cache() -> SelectionDataCache:
    """
    获取全局缓存实例（单例模式）

    Returns:
        SelectionDataCache 实例
    """
    global _global_cache
    if _global_cache is None:
        with _cache_lock:
            if _global_cache is None:
                _global_cache = SelectionDataCache(max_entries=3)
                logger.info("全局选股数据缓存已初始化")
    return _global_cache


def clear_global_cache() -> int:
    """
    清除全局缓存

    Returns:
        清除的条目数
    """
    cache = get_selection_data_cache()
    return cache.clear()


def get_cache_stats() -> dict:
    """
    获取全局缓存统计信息

    Returns:
        统计信息字典
    """
    cache = get_selection_data_cache()
    return cache.get_stats()
