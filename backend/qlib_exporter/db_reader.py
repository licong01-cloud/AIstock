from __future__ import annotations

"""从本地数据库读取行情数据的工具.

数据导出策略：
- 使用不复权价格 + 复权因子
- $close = 不复权价格(元) × 前复权因子
- $factor = 前复权因子
- 原始价格 = $close / $factor

支持的数据类型：
- 日线数据（股票）
- 分钟线数据（股票）
- 板块数据（TDX）
"""

from datetime import date, datetime
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

from app_pg import get_conn  # type: ignore[attr-defined]

from .config import (
    DAILY_QFQ_TABLE,
    DAILY_RAW_TABLE,
    FIELD_MAPPING_DB_DAILY,
    FIELD_MAPPING_DB_MINUTE,
    MINUTE_QFQ_TABLE,
    MINUTE_RAW_TABLE,
    PRICE_UNIT_DIVISOR,
    TDX_BOARD_DAILY_TABLE,
    TDX_BOARD_INDEX_TABLE,
    TDX_BOARD_MEMBER_TABLE,
)
from .adj_factor_provider import AdjFactorProvider


class DBReader:
    """封装针对前复权日线表和分钟线表的读取逻辑."""

    def get_all_ts_codes(self) -> List[str]:
        sql = f"""
            SELECT DISTINCT ts_code
            FROM {DAILY_QFQ_TABLE}
            ORDER BY ts_code
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [r[0] for r in rows]

    def get_all_board_codes(self, idx_types: List[str] | None = None) -> List[str]:
        """获取全部（或指定类型）板块代码列表.

        来自 TDX_BOARD_INDEX_TABLE，按 ts_code 去重。
        idx_types 为空时不过滤类型。
        """

        conditions: list[str] = []
        params: dict[str, object] = {}
        if idx_types:
            conditions.append("idx_type = ANY(%(idx_types)s)")
            params["idx_types"] = idx_types
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"""
            SELECT DISTINCT ts_code
              FROM {TDX_BOARD_INDEX_TABLE}
              {where_clause}
             ORDER BY ts_code
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql, params or None)
                rows = cur.fetchall()
        return [r[0] for r in rows]

    def get_all_ts_codes_minute(self) -> List[str]:
        sql = f"""
            SELECT DISTINCT ts_code
            FROM {MINUTE_QFQ_TABLE}
            WHERE freq = '1m'
            ORDER BY ts_code
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [r[0] for r in rows]

    def load_daily(
        self,
        ts_codes: Iterable[str],
        start: date | None,
        end: date | None,
    ) -> pd.DataFrame:
        """加载指定股票在给定日期区间内的前复权日线数据.

        返回 MultiIndex (datetime, instrument) 的 DataFrame，列使用逻辑字段名。
        当前仅包含基础 OHLCV + amount 列。
        """

        codes = list(ts_codes)
        if not codes:
            return pd.DataFrame()

        conditions: list[str] = ["ts_code = ANY(%(codes)s)"]
        params: dict[str, object] = {"codes": codes}

        if start is not None:
            conditions.append("trade_date >= %(start)s")
            params["start"] = start
        if end is not None:
            conditions.append("trade_date <= %(end)s")
            params["end"] = end

        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT
                trade_date,
                ts_code,
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand,
                amount_li
            FROM {DAILY_QFQ_TABLE}
            WHERE {where_clause}
            ORDER BY trade_date, ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return df

        # 重命名列到逻辑字段
        rename_map = {
            FIELD_MAPPING_DB_DAILY["datetime"]: "datetime",
            FIELD_MAPPING_DB_DAILY["open"]: "open",
            FIELD_MAPPING_DB_DAILY["high"]: "high",
            FIELD_MAPPING_DB_DAILY["low"]: "low",
            FIELD_MAPPING_DB_DAILY["close"]: "close",
            FIELD_MAPPING_DB_DAILY["volume"]: "volume",
            FIELD_MAPPING_DB_DAILY["amount"]: "amount",
        }
        df = df.rename(columns=rename_map)

        # 构造 MultiIndex (datetime, instrument)
        df["datetime"] = pd.to_datetime(df["datetime"], utc=False)
        df = df.set_index(["datetime", "ts_code"])  # type: ignore[call-arg]
        df.index = df.index.set_names(["datetime", "instrument"])

        # 仅保留逻辑字段列
        cols = ["open", "high", "low", "close", "volume", "amount"]
        df = df[cols]

        return df

    def load_board_daily(
        self,
        board_codes: Iterable[str],
        start: date | None,
        end: date | None,
    ) -> pd.DataFrame:
        """加载指定板块在给定日期区间内的日线数据.

        返回 MultiIndex (datetime, board) 的 DataFrame，列为 OHLCV+amount+pct_chg。
        """

        codes = list(board_codes)
        if not codes:
            return pd.DataFrame()

        conditions: list[str] = ["ts_code = ANY(%(codes)s)"]
        params: dict[str, object] = {"codes": codes}

        if start is not None:
            conditions.append("trade_date >= %(start)s")
            params["start"] = start
        if end is not None:
            conditions.append("trade_date <= %(end)s")
            params["end"] = end

        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT
                trade_date,
                ts_code,
                open,
                high,
                low,
                close,
                vol AS volume,
                amount,
                pct_chg
            FROM {TDX_BOARD_DAILY_TABLE}
            WHERE {where_clause}
            ORDER BY trade_date, ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df = df.drop(columns=["trade_date"])
        df = df.set_index(["datetime", "ts_code"])  # type: ignore[call-arg]
        df.index = df.index.set_names(["datetime", "board"])

        cols = ["open", "high", "low", "close", "volume", "amount", "pct_chg"]
        df = df[cols]

        return df

    def load_minute(
        self,
        ts_codes: Iterable[str],
        start: date | None,
        end: date | None,
        freq: str = "1m",
    ) -> pd.DataFrame:
        """加载指定股票在给定日期区间内的分钟线数据.

        返回 MultiIndex (datetime, instrument) 的 DataFrame，列使用逻辑字段名。
        当前仅包含基础 OHLCV + amount 列。
        """

        codes = list(ts_codes)
        if not codes:
            return pd.DataFrame()

        conditions: list[str] = ["ts_code = ANY(%(codes)s)", "freq = %(freq)s"]
        params: dict[str, object] = {"codes": codes, "freq": freq}

        if start is not None:
            conditions.append("trade_time::date >= %(start)s")
            params["start"] = start
        if end is not None:
            conditions.append("trade_time::date <= %(end)s")
            params["end"] = end

        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT
                trade_time,
                ts_code,
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand,
                amount_li
            FROM {MINUTE_QFQ_TABLE}
            WHERE {where_clause}
            ORDER BY trade_time, ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return df

        rename_map = {
            FIELD_MAPPING_DB_MINUTE["datetime"]: "datetime",
            FIELD_MAPPING_DB_MINUTE["open"]: "open",
            FIELD_MAPPING_DB_MINUTE["high"]: "high",
            FIELD_MAPPING_DB_MINUTE["low"]: "low",
            FIELD_MAPPING_DB_MINUTE["close"]: "close",
            FIELD_MAPPING_DB_MINUTE["volume"]: "volume",
            FIELD_MAPPING_DB_MINUTE["amount"]: "amount",
        }
        df = df.rename(columns=rename_map)

        df["datetime"] = pd.to_datetime(df["datetime"], utc=False)
        # 强制转换 ts_code 为普通 str，避免 Pandas StringDtype 导致 HDF5 写入失败
        df["ts_code"] = df["ts_code"].astype(str)
        df = df.set_index(["datetime", "ts_code"])  # type: ignore[call-arg]
        df.index = df.index.set_names(["datetime", "instrument"])

        cols = ["open", "high", "low", "close", "volume", "amount"]
        df = df[cols]

        return df

    def load_minute_batched(
        self,
        ts_codes: Iterable[str],
        start: date,
        end: date,
        freq: str = "1m",
        batch_days: int = 30,
    ):
        """分批加载分钟线数据（生成器）.

        按日期范围分批加载，避免一次性加载过多数据导致内存溢出。

        Args:
            ts_codes: 股票代码列表
            start: 开始日期
            end: 结束日期
            freq: 频率，默认 1m
            batch_days: 每批加载的天数，默认 30 天

        Yields:
            (batch_start, batch_end, DataFrame) 元组
        """
        from datetime import timedelta

        codes = list(ts_codes)
        if not codes:
            return

        current_start = start
        while current_start <= end:
            current_end = min(current_start + timedelta(days=batch_days - 1), end)

            df = self.load_minute(codes, current_start, current_end, freq)

            if not df.empty:
                yield (current_start, current_end, df)

            current_start = current_end + timedelta(days=1)

    def get_minute_date_range(self) -> tuple[date | None, date | None]:
        """获取分钟线数据的日期范围."""
        sql = f"""
            SELECT MIN(trade_time::date), MAX(trade_time::date)
            FROM {MINUTE_QFQ_TABLE}
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
        if row and row[0] and row[1]:
            return row[0], row[1]
        return None, None

    def get_minute_row_count(
        self,
        ts_codes: Iterable[str] | None,
        start: date | None,
        end: date | None,
    ) -> int:
        """获取分钟线数据行数（用于进度估算）."""
        conditions: list[str] = []
        params: dict[str, object] = {}

        codes = list(ts_codes) if ts_codes else None
        if codes:
            conditions.append("ts_code = ANY(%(codes)s)")
            params["codes"] = codes
        if start:
            conditions.append("trade_time::date >= %(start)s")
            params["start"] = start
        if end:
            conditions.append("trade_time::date <= %(end)s")
            params["end"] = end

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"SELECT COUNT(*) FROM {MINUTE_QFQ_TABLE} WHERE {where_clause}"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        return row[0] if row else 0

    def load_board_index(
        self,
        start: date | None,
        end: date | None,
        idx_types: List[str] | None = None,
    ) -> pd.DataFrame:
        """加载板块索引数据（tdx_board_index）.

        返回 DataFrame，列为 trade_date, ts_code, name, idx_type, idx_count。
        """

        conditions: list[str] = []
        params: dict[str, object] = {}

        if start is not None:
            conditions.append("trade_date >= %(start)s")
            params["start"] = start
        if end is not None:
            conditions.append("trade_date <= %(end)s")
            params["end"] = end
        if idx_types:
            conditions.append("idx_type = ANY(%(idx_types)s)")
            params["idx_types"] = idx_types

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"""
            SELECT
                trade_date,
                ts_code,
                name,
                idx_type,
                idx_count
            FROM {TDX_BOARD_INDEX_TABLE}
            {where_clause}
            ORDER BY trade_date, ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params or None)

        if df.empty:
            return df

        # 转换数据类型
        df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
        df["ts_code"] = df["ts_code"].astype(str)
        df["name"] = df["name"].astype(str)
        df["idx_type"] = df["idx_type"].astype(str)
        df["idx_count"] = pd.to_numeric(df["idx_count"], errors="coerce").fillna(0).astype(int)

        return df

    def load_board_member(
        self,
        start: date | None,
        end: date | None,
        board_codes: List[str] | None = None,
    ) -> pd.DataFrame:
        """加载板块成员数据（tdx_board_member）.

        返回 DataFrame，列为 trade_date, ts_code (板块代码), con_code (成分股代码), con_name。
        """

        conditions: list[str] = []
        params: dict[str, object] = {}

        if start is not None:
            conditions.append("trade_date >= %(start)s")
            params["start"] = start
        if end is not None:
            conditions.append("trade_date <= %(end)s")
            params["end"] = end
        if board_codes:
            conditions.append("ts_code = ANY(%(board_codes)s)")
            params["board_codes"] = board_codes

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"""
            SELECT
                trade_date,
                ts_code,
                con_code,
                con_name
            FROM {TDX_BOARD_MEMBER_TABLE}
            {where_clause}
            ORDER BY trade_date, ts_code, con_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params or None)

        if df.empty:
            return df

        # 转换数据类型
        df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False)
        df["ts_code"] = df["ts_code"].astype(str)
        df["con_code"] = df["con_code"].astype(str)
        df["con_name"] = df["con_name"].astype(str)

        return df

    # =========================================================================
    # Qlib 格式数据导出（daily_pv.h5 格式）
    # =========================================================================

    def _ts_code_to_instrument(self, ts_code: str) -> str:
        """将 ts_code 转换为 Qlib instrument 格式.

        Examples:
            "000001.SZ" -> "SZ000001"
            "600000.SH" -> "SH600000"
            "430047.BJ" -> "BJ430047"
        """
        if "." not in ts_code:
            return ts_code
        code, exchange = ts_code.split(".")
        return f"{exchange}{code}"

    def load_qlib_daily_data(
        self,
        ts_codes: Iterable[str],
        start: date,
        end: date,
        use_tushare_adj: bool = True,
    ) -> pd.DataFrame:
        """加载 Qlib 格式日线数据.

        使用不复权价格 + 复权因子计算：
        - $close = 不复权价格(元) × 前复权因子
        - $factor = 前复权因子

        返回 DataFrame 格式:
        - Index: MultiIndex (datetime, instrument)
        - Columns: $open, $close, $high, $low, $volume, $factor
        - 数据类型: float32

        Args:
            ts_codes: 股票代码列表
            start: 开始日期
            end: 结束日期
            use_tushare_adj: 是否使用 Tushare 复权因子（当本地无数据时）

        Returns:
            符合 Qlib 格式的 DataFrame
        """
        codes = list(ts_codes)
        if not codes:
            return pd.DataFrame()

        # 1. 从不复权表加载价格数据
        sql = f"""
            SELECT
                ts_code,
                trade_date,
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand
            FROM {DAILY_RAW_TABLE}
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY trade_date, ts_code
        """
        params = {"codes": codes, "start": start, "end": end}

        with get_conn() as conn:
            price_df = pd.read_sql(sql, conn, params=params)

        if price_df.empty:
            return pd.DataFrame()

        # 2. 获取复权因子
        adj_provider = AdjFactorProvider(use_tushare_fallback=use_tushare_adj)
        adj_df = adj_provider.get_adj_factor(codes, start, end)

        # 3. 计算前复权因子
        if adj_df.empty:
            # 严格模式：不允许没有复权因子就继续导出
            raise RuntimeError(
                "No adjustment factors found for requested codes/date range; "
                "please ensure adj_factor table or Tushare data is available."
            )

        adj_df = adj_provider.calculate_qfq_factor(adj_df)
        # 转换日期格式以便合并
        adj_df["trade_date"] = pd.to_datetime(adj_df["trade_date"]).dt.date
        price_df["trade_date"] = pd.to_datetime(price_df["trade_date"]).dt.date

        # 合并复权因子
        price_df = price_df.merge(
            adj_df[["ts_code", "trade_date", "qfq_factor"]],
            on=["ts_code", "trade_date"],
            how="left",
        )

        # 不允许缺失复权因子
        if price_df["qfq_factor"].isna().any():
            missing = price_df[price_df["qfq_factor"].isna()][["ts_code", "trade_date"]].drop_duplicates()
            raise RuntimeError(
                "Missing adjustment factors for some records after merge; "
                f"examples: {missing.head().to_dict(orient='records')}"
            )

        # 4. 计算 Qlib 格式数据
        # 价格单位转换：厘 -> 元，并按前复权因子调整
        price_df["$open"] = (price_df["open_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)
        price_df["$high"] = (price_df["high_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)
        price_df["$low"] = (price_df["low_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)
        price_df["$close"] = (price_df["close_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)

        # 成交量：hand -> shares，并按前复权因子反向调整
        # Qlib 分钟线使用 volume_raw / factor，这里日线保持一致的复权方式
        price_df["_volume_shares"] = price_df["volume_hand"] * 100.0
        price_df["$volume"] = (price_df["_volume_shares"] / price_df["qfq_factor"]).astype(np.float32)

        # 成交额：amount_li 为厘，这里仅做单位转换为元，不做复权
        if "amount_li" in price_df.columns:
            price_df["$amount"] = (price_df["amount_li"] / PRICE_UNIT_DIVISOR).astype(np.float32)

        price_df["$factor"] = price_df["qfq_factor"].astype(np.float32)

        # 5. 转换为 Qlib 格式
        price_df["instrument"] = price_df["ts_code"].apply(self._ts_code_to_instrument)
        price_df["datetime"] = pd.to_datetime(price_df["trade_date"])

        # 6. 设置 MultiIndex
        price_df = price_df.set_index(["datetime", "instrument"])

        # 7. 只保留 Qlib 列（$amount 为可选列）
        qlib_cols = ["$open", "$close", "$high", "$low", "$volume", "$factor"]
        if "$amount" in price_df.columns:
            qlib_cols.append("$amount")
        result = price_df[qlib_cols].copy()

        # 8. 排序
        result = result.sort_index()

        return result

    def load_qlib_daily_data_all(
        self,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        use_tushare_adj: bool = True,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载全部股票的 Qlib 格式日线数据.

        Args:
            start: 开始日期
            end: 结束日期
            exchanges: 交易所过滤，如 ["sh", "sz", "bj"]
            use_tushare_adj: 是否使用 Tushare 复权因子
            exclude_st: 是否排除所有在 stock_st 表中出现过的股票（曾经 / 当前 ST）
            exclude_delisted_or_paused: 是否排除退市或当前暂停上市股票（stock_basic.list_status in ('D','P')）

        Returns:
            符合 Qlib 格式的 DataFrame
        """
        # 构建交易所过滤条件
        exchange_filter = ""
        if exchanges:
            exchange_conditions = []
            for ex in exchanges:
                ex_upper = ex.upper()
                exchange_conditions.append(f"dr.ts_code LIKE '%%.{ex_upper}'")
            if exchange_conditions:
                exchange_filter = " AND (" + " OR ".join(exchange_conditions) + ")"

        # ST / 退市 / 暂停上市过滤
        st_filter = ""
        if exclude_st:
            st_filter = "AND dr.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)"

        status_filter = ""
        if exclude_delisted_or_paused:
            status_filter = (
                " AND dr.ts_code NOT IN ("
                "SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P')"
                ")"
            )

        # 1. 获取符合条件的股票代码
        sql = f"""
            SELECT DISTINCT dr.ts_code
            FROM {DAILY_RAW_TABLE} AS dr
            WHERE dr.trade_date >= %(start)s
              AND dr.trade_date <= %(end)s
              {exchange_filter}
              {st_filter}
              {status_filter}
            ORDER BY dr.ts_code
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"start": start, "end": end})
                codes = [row[0] for row in cur.fetchall()]

        if not codes:
            return pd.DataFrame()

        # 2. 分批加载数据（避免内存溢出）
        batch_size = 500
        all_data = []

        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i : i + batch_size]
            batch_df = self.load_qlib_daily_data(batch_codes, start, end, use_tushare_adj)
            if not batch_df.empty:
                all_data.append(batch_df)

        if not all_data:
            return pd.DataFrame()

        result = pd.concat(all_data)
        result = result.sort_index()

        return result

    # =========================================================================
    # 兼容旧接口（逐步废弃）
    # =========================================================================

    def load_factor_data(
        self,
        ts_codes: Iterable[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """加载因子数据（兼容旧接口）.

        已废弃，请使用 load_qlib_daily_data
        """
        return self.load_qlib_daily_data(ts_codes, start, end)

    def load_factor_data_all(
        self,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """加载全部因子数据（兼容旧接口）.

        已废弃，请使用 load_qlib_daily_data_all
        """
        return self.load_qlib_daily_data_all(start, end, exchanges)

    # =========================================================================
    # 分钟线 Qlib 格式导出
    # =========================================================================

    def load_qlib_minute_data(
        self,
        ts_codes: Iterable[str],
        start: date,
        end: date,
        freq: str = "1m",
        use_tushare_adj: bool = True,
    ) -> pd.DataFrame:
        """加载 Qlib 格式分钟线数据.

        分钟线复权说明：
        - 使用日线的复权因子
        - 同一天内的分钟线使用相同的复权因子
        - $close = 不复权价格(元) × 当日前复权因子

        返回 DataFrame 格式:
        - Index: MultiIndex (datetime, instrument)
        - Columns: $open, $close, $high, $low, $volume, $factor
        - 数据类型: float32

        Args:
            ts_codes: 股票代码列表
            start: 开始日期
            end: 结束日期
            freq: 频率，默认 1m
            use_tushare_adj: 是否使用 Tushare 复权因子

        Returns:
            符合 Qlib 格式的 DataFrame
        """
        codes = list(ts_codes)
        if not codes:
            return pd.DataFrame()

        # 1. 加载分钟线原始数据
        sql = f"""
            SELECT
                trade_time,
                ts_code,
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand
            FROM {MINUTE_RAW_TABLE}
            WHERE ts_code = ANY(%(codes)s)
              AND freq = %(freq)s
              AND trade_time::date >= %(start)s
              AND trade_time::date <= %(end)s
            ORDER BY trade_time, ts_code
        """
        params = {"codes": codes, "freq": freq, "start": start, "end": end}

        with get_conn() as conn:
            price_df = pd.read_sql(sql, conn, params=params)

        if price_df.empty:
            return pd.DataFrame()

        # 2. 获取日线复权因子
        adj_provider = AdjFactorProvider(use_tushare_fallback=use_tushare_adj)
        adj_df = adj_provider.get_adj_factor(codes, start, end)

        # 3. 计算前复权因子
        if adj_df.empty:
            # 严格模式：不允许没有复权因子就继续导出
            raise RuntimeError(
                "No adjustment factors found for requested codes/date range (minute data); "
                "please ensure adj_factor table or Tushare data is available."
            )

        adj_df = adj_provider.calculate_qfq_factor(adj_df)
        adj_df["trade_date"] = pd.to_datetime(adj_df["trade_date"]).dt.date

        # 提取分钟线的日期
        price_df["trade_date"] = pd.to_datetime(price_df["trade_time"]).dt.date

        # 合并复权因子（按日期匹配）
        price_df = price_df.merge(
            adj_df[["ts_code", "trade_date", "qfq_factor"]],
            on=["ts_code", "trade_date"],
            how="left",
        )

        # 不允许缺失复权因子
        if price_df["qfq_factor"].isna().any():
            missing = price_df[price_df["qfq_factor"].isna()][["ts_code", "trade_date"]].drop_duplicates()
            raise RuntimeError(
                "Missing adjustment factors for some minute records after merge; "
                f"examples: {missing.head().to_dict(orient='records')}"
            )

        # 4. 计算 Qlib 格式数据
        # 价格单位转换：厘 -> 元，并按前复权因子调整
        price_df["$open"] = (price_df["open_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)
        price_df["$high"] = (price_df["high_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)
        price_df["$low"] = (price_df["low_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)
        price_df["$close"] = (price_df["close_li"] / PRICE_UNIT_DIVISOR * price_df["qfq_factor"]).astype(np.float32)

        # 成交量：hand -> shares，并按前复权因子反向调整（与 Qlib 分钟线逻辑一致：volume_raw / factor）
        price_df["_volume_shares"] = price_df["volume_hand"] * 100.0
        price_df["$volume"] = (price_df["_volume_shares"] / price_df["qfq_factor"]).astype(np.float32)

        # 成交额：amount_li 为厘，仅做单位转换为元，不做复权
        if "amount_li" in price_df.columns:
            price_df["$amount"] = (price_df["amount_li"] / PRICE_UNIT_DIVISOR).astype(np.float32)

        price_df["$factor"] = price_df["qfq_factor"].astype(np.float32)

        # 5. 转换为 Qlib 格式
        price_df["instrument"] = price_df["ts_code"].apply(self._ts_code_to_instrument)
        price_df["datetime"] = pd.to_datetime(price_df["trade_time"])

        # 6. 设置 MultiIndex
        price_df = price_df.set_index(["datetime", "instrument"])

        # 7. 只保留 Qlib 列（$amount 为可选列）
        qlib_cols = ["$open", "$close", "$high", "$low", "$volume", "$factor"]
        if "$amount" in price_df.columns:
            qlib_cols.append("$amount")
        result = price_df[qlib_cols].copy()

        # 8. 排序
        result = result.sort_index()

        return result

    # =========================================================================
    # 板块数据 Qlib 格式导出
    # =========================================================================

    def load_qlib_board_data(
        self,
        board_codes: Iterable[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """加载 Qlib 格式板块日线数据.

        板块数据说明：
        - 板块指数不需要复权（没有分红送股）
        - $factor 固定为 1.0
        - 价格单位已经是元

        返回 DataFrame 格式:
        - Index: MultiIndex (datetime, instrument)
        - Columns: $open, $close, $high, $low, $volume, $factor
        - 数据类型: float32

        Args:
            board_codes: 板块代码列表
            start: 开始日期
            end: 结束日期

        Returns:
            符合 Qlib 格式的 DataFrame
        """
        codes = list(board_codes)
        if not codes:
            return pd.DataFrame()

        sql = f"""
            SELECT
                trade_date,
                ts_code,
                open,
                high,
                low,
                close,
                vol as volume
            FROM {TDX_BOARD_DAILY_TABLE}
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY trade_date, ts_code
        """
        params = {"codes": codes, "start": start, "end": end}

        with get_conn() as conn:
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return pd.DataFrame()

        # 板块数据价格已经是元，不需要单位转换
        # 板块不需要复权，$factor = 1.0
        df["$open"] = df["open"].astype(np.float32)
        df["$high"] = df["high"].astype(np.float32)
        df["$low"] = df["low"].astype(np.float32)
        df["$close"] = df["close"].astype(np.float32)
        df["$volume"] = df["volume"].astype(np.float32)
        df["$factor"] = np.float32(1.0)

        # 转换为 Qlib 格式
        # 板块代码格式：保持原样或添加前缀
        df["instrument"] = df["ts_code"].astype(str)
        df["datetime"] = pd.to_datetime(df["trade_date"])

        df = df.set_index(["datetime", "instrument"])

        qlib_cols = ["$open", "$close", "$high", "$low", "$volume", "$factor"]
        result = df[qlib_cols].copy()
        result = result.sort_index()

        return result
