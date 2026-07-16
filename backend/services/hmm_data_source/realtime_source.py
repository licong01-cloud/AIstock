"""
实时数据源实现

连接数据库查询 t-1 数据，用于生产环境的风险监控。

特性:
- 查询 t-1 数据（lag_days 可配置）
- 限制单次查询天数（max_query_days）
- 板块映射查询（market.sw_member）
- 未来收益计算（kline_daily_raw）
"""

from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd

from backend.db.pg_pool import get_conn

from .base import HMMDataSourceInterface
from .exceptions import DataSourceError, DateRangeError, HorizonError, DataNotFoundError


class RealtimeDataSource(HMMDataSourceInterface):
    """
    实时数据源

    从数据库查询 t-1 数据，用于生产环境。

    Example:
        source = RealtimeDataSource(
            snapshot_id="latest",
            lag_days=1,
            max_query_days=730,
        )
        pred_df = await source.get_predictions(
            start_date=date(2024, 7, 1),
            end_date=date(2024, 7, 5),
        )
    """

    def __init__(
        self,
        snapshot_id: str = "latest",
        lag_days: int = 1,
        max_query_days: int = 730,
    ):
        """
        Args:
            snapshot_id: HMM snapshot ID（"latest" 或具体 ID）
            lag_days: 数据延迟天数（t-1 为 1）
            max_query_days: 单次查询最大天数（防止查询过大）
        """
        self.snapshot_id = snapshot_id
        self.lag_days = lag_days
        self.max_query_days = max_query_days

        # 缓存最新可用日期（减少重复查询）
        self._latest_available_date: Optional[date] = None

    @property
    def mode(self) -> str:
        return "realtime"

    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """获取预测分数"""
        # 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)

        # 检查查询天数限制
        query_days = (end_date - start_date).days + 1
        if query_days > self.max_query_days:
            raise DateRangeError(
                f"Query span ({query_days} days) exceeds max_query_days ({self.max_query_days})"
            )

        # 查询数据库
        try:
            df = await self._query_predictions_from_db(start_date, end_date)

            if df.empty:
                raise DataNotFoundError(
                    f"No predictions found for date range [{start_date}, {end_date}]"
                )

            return df

        except DataNotFoundError:
            raise
        except Exception as e:
            raise DataSourceError(f"Failed to query predictions: {e}")

    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        获取未来收益标签

        Notes:
            实时模式下，只能返回已实现的收益（过去的数据）。
            未来的收益标签不可用，用于事后验证。
        """
        # 验证 horizon
        if not 1 <= horizon_days <= 30:
            raise HorizonError(
                f"horizon_days must be between 1 and 30, got {horizon_days}"
            )

        # 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)

        # 查询已实现的收益
        try:
            df = await self._query_realized_returns(start_date, end_date, horizon_days)

            if df.empty:
                raise DataNotFoundError(
                    f"No realized returns found for date range [{start_date}, {end_date}] "
                    f"with horizon_days={horizon_days}"
                )

            return df

        except DataNotFoundError:
            raise
        except Exception as e:
            raise DataSourceError(f"Failed to query realized returns: {e}")

    async def get_sector_mapping(self, trade_date: date) -> dict[str, str]:
        """获取股票板块映射（申万 L2）"""
        try:
            async with get_conn() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT
                        sb.symbol,
                        sw.index_code as sector_code
                    FROM market.stock_basic sb
                    LEFT JOIN market.sw_member sw ON sb.ts_code = sw.con_code
                    WHERE sw.level = 'L2'
                      AND sw.in_date <= %(trade_date)s
                      AND (sw.out_date IS NULL OR sw.out_date > %(trade_date)s)
                    """
                    await cur.execute(query, {'trade_date': trade_date})
                    rows = await cur.fetchall()

                    # 构建映射
                    mapping = {}
                    for row in rows:
                        symbol, sector_code = row
                        if symbol and sector_code:
                            mapping[symbol] = sector_code

                    return mapping

        except Exception as e:
            raise DataSourceError(f"Failed to query sector mapping: {e}")

    async def get_available_date_range(self) -> Tuple[date, date]:
        """获取数据源可用的日期范围"""
        try:
            async with get_conn() as conn:
                async with conn.cursor() as cur:
                    # 查询最早和最晚日期
                    query = """
                    SELECT
                        MIN(trade_date) as min_date,
                        MAX(trade_date) as max_date
                    FROM market.kline_daily_raw
                    """
                    await cur.execute(query)
                    row = await cur.fetchone()

                    if not row or not row[0]:
                        raise DataSourceError("No data available in kline_daily_raw")

                    min_date, max_date = row

                    # 应用 lag_days（t-1）
                    max_date_adjusted = max_date - timedelta(days=self.lag_days)

                    return min_date, max_date_adjusted

        except Exception as e:
            raise DataSourceError(f"Failed to query available date range: {e}")

    async def _query_predictions_from_db(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        从数据库查询预测分数

        优先查询 model_train_predictions（如果存在），
        否则 fallback 到其他数据源。

        Returns:
            DataFrame with columns: trade_date, symbol, score
        """
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                # 尝试查询 model_train_predictions
                query = """
                SELECT
                    trade_date,
                    symbol,
                    score
                FROM model_train_predictions
                WHERE trade_date >= %(start_date)s
                  AND trade_date <= %(end_date)s
                ORDER BY trade_date, symbol
                """
                await cur.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date,
                })
                rows = await cur.fetchall()

                if rows:
                    # 找到数据
                    df = pd.DataFrame(rows, columns=['trade_date', 'symbol', 'score'])
                    return df
                else:
                    # 没有数据，返回空 DataFrame
                    return pd.DataFrame(columns=['trade_date', 'symbol', 'score'])

    async def _query_realized_returns(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int,
    ) -> pd.DataFrame:
        """
        查询已实现的收益

        从 kline_daily_raw 计算未来 N 日收益率。
        只返回已实现的收益（label_date <= 今天）。

        Returns:
            DataFrame with columns: trade_date, symbol, horizon_days, future_return, label_date
        """
        try:
            async with get_conn() as conn:
                async with conn.cursor() as cur:
                    # 查询交易日历，获取每个 trade_date 的 label_date
                    query = """
                    WITH base_dates AS (
                        SELECT DISTINCT cal_date as trade_date
                        FROM market.trade_cal
                        WHERE cal_date >= %(start_date)s
                          AND cal_date <= %(end_date)s
                          AND is_open = 1
                    ),
                    label_dates AS (
                        SELECT
                            bd.trade_date,
                            (
                                SELECT cal_date
                                FROM market.trade_cal
                                WHERE cal_date > bd.trade_date
                                  AND is_open = 1
                                ORDER BY cal_date
                                LIMIT 1 OFFSET %(offset)s
                            ) as label_date
                        FROM base_dates bd
                    ),
                    returns AS (
                        SELECT
                            ld.trade_date,
                            k1.symbol,
                            %(horizon_days)s as horizon_days,
                            (k2.close / k1.close - 1) as future_return,
                            ld.label_date
                        FROM label_dates ld
                        JOIN market.kline_daily_raw k1
                          ON k1.trade_date = ld.trade_date
                        LEFT JOIN market.kline_daily_raw k2
                          ON k2.symbol = k1.symbol
                          AND k2.trade_date = ld.label_date
                        WHERE ld.label_date IS NOT NULL
                          AND ld.label_date <= CURRENT_DATE
                          AND k2.close IS NOT NULL
                    )
                    SELECT
                        trade_date,
                        symbol,
                        horizon_days,
                        future_return,
                        label_date
                    FROM returns
                    ORDER BY trade_date, symbol
                    """
                    await cur.execute(query, {
                        'start_date': start_date,
                        'end_date': end_date,
                        'horizon_days': horizon_days,
                        'offset': horizon_days - 1,
                    })
                    rows = await cur.fetchall()

                    if rows:
                        df = pd.DataFrame(
                            rows,
                            columns=[
                                'trade_date',
                                'symbol',
                                'horizon_days',
                                'future_return',
                                'label_date',
                            ]
                        )
                        return df
                    else:
                        return pd.DataFrame(
                            columns=[
                                'trade_date',
                                'symbol',
                                'horizon_days',
                                'future_return',
                                'label_date',
                            ]
                        )

        except Exception as e:
            raise DataSourceError(f"Failed to query realized returns: {e}")
