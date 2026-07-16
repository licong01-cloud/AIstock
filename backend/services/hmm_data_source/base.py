"""
HMM 数据源抽象接口

所有数据源必须实现此接口，确保业务逻辑与数据来源解耦。

设计原则:
1. 返回标准化的 pandas DataFrame，列名固定
2. 日期范围包含边界（[start_date, end_date]）
3. 数据源内部处理缓存、错误重试
4. 不抛出未处理异常，使用明确的错误类型
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional, Tuple

import pandas as pd


class HMMDataSourceInterface(ABC):
    """
    HMM 数据源抽象接口

    所有数据源（回测/实时）必须实现此接口。
    """

    @property
    @abstractmethod
    def mode(self) -> str:
        """
        返回数据源模式标识

        Returns:
            "backtest" 或 "realtime"
        """
        pass

    @abstractmethod
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        获取预测分数

        Args:
            start_date: 起始日期（包含）
            end_date: 结束日期（包含）

        Returns:
            DataFrame with columns:
            - trade_date: date        交易日期
            - symbol: str             股票代码（含后缀 .SZ/.SH）
            - score: float            预测分数（原始分数，未经 HMM 调整）
            - rank: int (optional)    原始排名

        Raises:
            DataSourceError: 数据获取失败
            DateRangeError: 日期范围无效或超出可用范围

        Performance:
            回测模式：首次 < 30s，缓存命中 < 1s
            实时模式：< 2s
        """
        pass

    @abstractmethod
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        获取未来收益标签（ground truth）

        Args:
            start_date: 起始日期（包含）
            end_date: 结束日期（包含）
            horizon_days: 未来收益窗口（天数）

        Returns:
            DataFrame with columns:
            - trade_date: date        交易日期（T日）
            - symbol: str             股票代码
            - future_return: float    未来 N 日收益率（T+horizon_days vs T）
            - label_date: date        标签日期（T+horizon_days）

        Notes:
            - 回测模式：从 label.pkl 读取，数据完整
            - 实时模式：从 kline_daily_raw 计算，未来数据不可用，
              只返回已实现的收益（用于事后验证）

        Raises:
            DataSourceError: 数据获取失败
            HorizonError: horizon_days 超出合理范围（1-30）
        """
        pass

    @abstractmethod
    async def get_sector_mapping(
        self,
        trade_date: date,
    ) -> dict[str, str]:
        """
        获取股票板块映射（申万 L2）

        Args:
            trade_date: 交易日期

        Returns:
            {symbol: sector_code}
            例如: {"000001.SZ": "801780.SI", "600000.SH": "801192.SI"}

        Notes:
            - 使用 market.stock_basic + market.sw_member
            - 板块代码为申万 L2 级别（801xxx.SI）
            - 如果股票不属于任何 L2 板块，返回 None

        Raises:
            DataSourceError: 数据获取失败
        """
        pass

    async def validate_date_range(
        self,
        start_date: date,
        end_date: date,
    ) -> Tuple[bool, Optional[str]]:
        """
        验证日期范围是否有效

        Args:
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            (is_valid, error_message)
            is_valid=True: 日期范围有效
            is_valid=False: 日期范围无效，error_message 说明原因

        Checks:
            1. start_date <= end_date
            2. 日期在数据源可用范围内
            3. 日期跨度不超过限制（回测: 无限制，实时: 最多 2 年）
        """
        if start_date > end_date:
            return False, f"起始日期 {start_date} 晚于结束日期 {end_date}"

        try:
            available_start, available_end = await self.get_available_date_range()
        except Exception as e:
            return False, f"获取可用日期范围失败: {e}"

        if start_date < available_start:
            return False, f"起始日期 {start_date} 早于数据可用起始日期 {available_start}"

        if end_date > available_end:
            return False, f"结束日期 {end_date} 晚于数据可用结束日期 {available_end}"

        return True, None

    @abstractmethod
    async def get_available_date_range(self) -> Tuple[date, date]:
        """
        获取数据源可用的日期范围

        Returns:
            (start_date, end_date)

        Notes:
            - 回测模式：返回 QE artifact 的日期范围
            - 实时模式：返回 DB 中最早和 t-1 的日期
        """
        pass
