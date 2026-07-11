"""从本地数据库读取行情数据的工具.

数据导出策略：
- 使用不复权价格 + 复权因子
- $close = 不复权价格(元) × 前复权因子
- $factor = 前复权因子
- 原始价格 = $close / $factor

支持的数据类型：
- 日线数据（股票）
- 分钟线数据（股票）
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Iterable, List, Mapping, Optional

import numpy as np
import pandas as pd

from backend.db.pg_pool import get_conn
from backend.data_service.moneyflow_contract import normalize_tushare_moneyflow_units
from backend.services.industry_code_map import (
    UNKNOWN_L2_CODE_ID,
    encode_l2_codes,
    load_sw_l2_code_map,
)

from .config import (
    DAILY_RAW_TABLE,
    FIELD_MAPPING_DB_MINUTE,
    INDEX_BASIC_TABLE,
    INDEX_DAILY_TABLE,
    INDEX_DAILY_TDX_TABLE,
    IPO_FILTER_DAYS,
    MINUTE_RAW_TABLE,
    MINUTE_QFQ_TABLE,
    MONEYFLOW_TS_TABLE,
    PRICE_UNIT_DIVISOR,
)
from .adj_factor_provider import AdjFactorProvider


def _filter_instrument_start_dates(
    price_df: pd.DataFrame,
    instrument_start_dates: Mapping[str, date] | None,
) -> pd.DataFrame:
    """Drop source rows earlier than each instrument's admissible start date."""

    if price_df.empty or not instrument_start_dates:
        return price_df
    starts = {
        str(code).strip().upper(): pd.Timestamp(start_date).date()
        for code, start_date in instrument_start_dates.items()
        if start_date is not None
    }
    row_starts = price_df["ts_code"].astype(str).str.strip().str.upper().map(starts)
    trade_dates = pd.to_datetime(price_df["trade_date"], errors="coerce").dt.date
    keep = row_starts.isna() | (trade_dates >= row_starts)
    return price_df.loc[keep].copy()


class DBReader:
    """封装针对前复权日线表和分钟线表的读取逻辑."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _quote_sql_strings(self, values: Iterable[str]) -> str:
        items: List[str] = []
        for v in values:
            s = str(v)
            items.append("'" + s.replace("'", "''") + "'")
        return ",".join(items)

    def _normalize_ts_code(self, code: str) -> str:
        s = str(code).strip()
        if not s:
            return s
        if "." in s:
            return s.upper()
        up = s.upper()
        if len(up) >= 8 and up[:2] in {"SH", "SZ", "BJ"}:
            return f"{up[2:]}.{up[:2]}"
        return up

    def _normalize_stock_export_exchanges(self, exchanges: Optional[List[str]]) -> set[str]:
        """AIstock stock data exports are SH/SZ only; BJ/BSE must fail fast."""

        if not exchanges:
            return {"sh", "sz"}
        normalized = {e.strip().lower() for e in exchanges if e and e.strip()}
        if not normalized:
            return {"sh", "sz"}
        if "bj" in normalized:
            raise ValueError("BJ/BSE stocks are excluded from AIstock stock data exports; use sh/sz only")
        unsupported = normalized - {"sh", "sz"}
        if unsupported:
            raise ValueError(f"unsupported exchange(s) for stock data export: {', '.join(sorted(unsupported))}")
        return normalized

    def load_daily_basic_panel(
        self,
        *,
        start: date,
        end: date,
        ts_codes: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载 Tushare daily_basic 指标并转换为 Qlib/RD-Agent 友好的面板格式.

        源表：market.daily_basic

        Returns:
            DataFrame
                - Index: MultiIndex (datetime, instrument)
                - Columns: db_* 系列字段（float32）
        """
        # 使用JOIN stock_basic方式，无需指定股票列表
        # 通过stock_basic表过滤ST、退市股票和交易所
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件
        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)
        
        sql = f"""
            SELECT
                d.trade_date,
                d.ts_code,
                d.close,
                d.turnover_rate,
                d.turnover_rate_f,
                d.volume_ratio,
                d.pe,
                d.pe_ttm,
                d.pb,
                d.ps,
                d.ps_ttm,
                d.dv_ratio,
                d.dv_ttm,
                d.total_share,
                d.float_share,
                d.free_share,
                d.total_mv,
                d.circ_mv
            FROM market.daily_basic d
            INNER JOIN market.stock_basic s ON d.ts_code = s.ts_code
            WHERE d.trade_date >= '{start.isoformat()}'
              AND d.trade_date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY d.trade_date, d.ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df["instrument"] = df["ts_code"].apply(self._normalize_ts_code).astype(str)
        df = df.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        rename_map = {
            "close": "db_close",
            "turnover_rate": "db_turnover_rate",
            "turnover_rate_f": "db_turnover_rate_f",
            "volume_ratio": "db_volume_ratio",
            "pe": "db_pe",
            "pe_ttm": "db_pe_ttm",
            "pb": "db_pb",
            "ps": "db_ps",
            "ps_ttm": "db_ps_ttm",
            "dv_ratio": "db_dv_ratio",
            "dv_ttm": "db_dv_ttm",
            "total_share": "db_total_share",
            "float_share": "db_float_share",
            "free_share": "db_free_share",
            "total_mv": "db_total_mv",
            "circ_mv": "db_circ_mv",
        }

        df = df.rename(columns=rename_map)

        # 仅保留 db_ 列，并统一为 float32
        db_cols = [c for c in df.columns if c.startswith("db_")]
        result = df[db_cols].copy()
        for c in db_cols:
            result[c] = pd.to_numeric(result[c], errors="coerce").astype("float32")
        result = result.sort_index()

        return result


    def _get_moneyflow_universe(
        self,
        *,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> List[str]:
        """moneyflow_ts 覆盖的股票池（ts_code），带同样过滤规则。"""
        # 使用JOIN stock_basic方式，无需指定股票列表
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件
        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)
        
        sql = f"""
            SELECT DISTINCT m.ts_code
            FROM {MONEYFLOW_TS_TABLE} m
            INNER JOIN market.stock_basic s ON m.ts_code = s.ts_code
            WHERE m.trade_date >= '{start.isoformat()}'
              AND m.trade_date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY m.ts_code
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)
        if df.empty:
            return []
        return [self._normalize_ts_code(x) for x in df["ts_code"].astype(str).tolist()]

    def _get_daily_basic_universe(
        self,
        *,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> List[str]:
        """daily_basic 覆盖的股票池（ts_code），带同样过滤规则。"""
        # 使用JOIN stock_basic方式，无需指定股票列表
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件
        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)
        
        sql = f"""
            SELECT DISTINCT d.ts_code
            FROM market.daily_basic d
            INNER JOIN market.stock_basic s ON d.ts_code = s.ts_code
            WHERE d.trade_date >= '{start.isoformat()}'
              AND d.trade_date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY d.ts_code
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)
        if df.empty:
            return []
        return [self._normalize_ts_code(x) for x in df["ts_code"].astype(str).tolist()]

    def _get_minute_universe(
        self,
        *,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        freq: str = "1m",
    ) -> List[str]:
        """分钟线覆盖的股票池（ts_code），带同样过滤规则。"""
        # 使用JOIN stock_basic方式，无需指定股票列表
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件
        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)
        
        sql = f"""
            SELECT DISTINCT m.ts_code
            FROM {MINUTE_QFQ_TABLE} m
            INNER JOIN market.stock_basic s ON m.ts_code = s.ts_code
            WHERE m.freq = '{freq}'
              AND m.trade_time::date >= '{start.isoformat()}'
              AND m.trade_time::date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY m.ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)
        if df.empty:
            return []
        return [self._normalize_ts_code(x) for x in df["ts_code"].astype(str).tolist()]

    def get_base_ts_codes(
        self,
        *,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> List[str]:
        """基础股票池：仅基于 market.stock_basic / market.stock_st 过滤。

        注意：不再跨数据集（daily_basic / moneyflow / minute）求交集。
        """

        conditions: list[str] = []

        # 时间过滤规则 A：仅要求 end 之前已上市（字段可能为空，保守处理）
        # IPO 过滤：上市满 IPO_FILTER_DAYS 天后才纳入股票池
        conditions.append(
            "(list_date IS NULL OR list_date + INTERVAL '%d days' <= '%s')"
            % (IPO_FILTER_DAYS, end.isoformat())
        )

        # 按交易所过滤（基于 ts_code 后缀 .SH / .SZ / .BJ；兼容 SHxxxxxx 形式）
        normalized = self._normalize_stock_export_exchanges(exchanges)
        exchange_conds: list[str] = []
        if "sh" in normalized:
            exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
        if "sz" in normalized:
            exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
        if exchange_conds:
            conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append(f"ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            conditions.append("list_status NOT IN ('D','P')")

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        sql = f"""
            SELECT ts_code
            FROM market.stock_basic
            WHERE {where_clause}
            ORDER BY ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)

        if df.empty:
            return []
        return [self._normalize_ts_code(x) for x in df["ts_code"].astype(str).tolist()]

    def get_all_ts_codes(self) -> List[str]:
        sql = f"""
            SELECT DISTINCT ts_code
            FROM {DAILY_RAW_TABLE}
            ORDER BY ts_code
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [r[0] for r in rows]

    # ---------------------------------------------------------------------
    # 指数基础信息 & 日线行情
    # ---------------------------------------------------------------------

    def get_all_index_markets(self) -> List[str]:
        """获取 index_basic.market 的去重列表（按字典序排序）。"""

        sql = f"SELECT DISTINCT market FROM {INDEX_BASIC_TABLE} ORDER BY market"
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        markets: List[str] = []
        for r in rows:
            if r[0] is None:
                continue
            m = str(r[0]).strip()
            if m:
                markets.append(m)
        return markets

    def load_index_basic_by_markets(self, markets: Optional[List[str]] = None) -> pd.DataFrame:
        """按 market 过滤加载指数基础信息.

        返回 DataFrame，列至少包含: ts_code, name, fullname, market。
        若 markets 为空，则返回全部指数基础信息。
        """

        base_sql = f"SELECT ts_code, name, fullname, market FROM {INDEX_BASIC_TABLE}"
        params: dict[str, object] = {}
        where_clause = ""
        if markets:
            markets_clean = [m.strip().upper() for m in markets if m and m.strip()]
            if markets_clean:
                where_clause = " WHERE market = ANY(%(markets)s)"
                params["markets"] = markets_clean

        sql = base_sql + where_clause + " ORDER BY ts_code"

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params or None)

        if df.empty:
            return df

        # 规范化类型
        df["ts_code"] = df["ts_code"].astype(str)
        if "name" in df.columns:
            df["name"] = df["name"].astype(str)
        if "fullname" in df.columns:
            df["fullname"] = df["fullname"].astype(str)
        if "market" in df.columns:
            df["market"] = df["market"].astype(str)

        return df

    def load_index_daily(
        self,
        ts_code: str,
        start: date | None,
        end: date | None,
    ) -> pd.DataFrame:
        """加载单个指数在给定日期区间内的日线行情.

        返回 DataFrame，列为 trade_date, ts_code, open, high, low, close, vol, amount，
        已按 trade_date 升序排序。调用方可据此构建符合 Qlib dump_bin.py 的 CSV。
        """

        code = (ts_code or "").strip()
        if not code:
            return pd.DataFrame()

        conditions: list[str] = ["ts_code = %(ts_code)s"]
        params: dict[str, object] = {"ts_code": code}

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
                vol   AS volume,
                amount
            FROM {INDEX_DAILY_TABLE}
            WHERE {where_clause}
            ORDER BY trade_date
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return df

        # 统一日期类型
        df["trade_date"] = pd.to_datetime(df["trade_date"], utc=False).dt.date
        df["ts_code"] = df["ts_code"].astype(str)

        # 指数成交量：手 -> 股（不做复权处理）
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100.0

        return df

    def _ts_code_to_tdx_index_code(self, ts_code: str) -> str:
        """将 Tushare ts_code 转换为 TDX index_code.

        Examples:
            000300.SH -> sh000300
            399001.SZ -> sz399001
        """

        code = (ts_code or "").strip()
        if not code:
            return ""
        uc = code.upper()
        if uc.endswith(".SH"):
            return "sh" + uc.split(".")[0]
        if uc.endswith(".SZ"):
            return "sz" + uc.split(".")[0]
        return code

    def _tdx_index_code_to_ts_code(self, index_code: str) -> str:
        """将 TDX index_code 转换为 Tushare ts_code.

        Examples:
            sh000300 -> 000300.SH
            sz399001 -> 399001.SZ
        """

        s = (index_code or "").strip()
        if not s:
            return ""
        low = s.lower()
        if low.startswith("sh") and len(s) >= 8:
            return f"{s[2:]}.SH"
        if low.startswith("sz") and len(s) >= 8:
            return f"{s[2:]}.SZ"
        return s

    def load_index_list_tdx(self) -> pd.DataFrame:
        """从 TDX 指数日线表罗列可导出的指数列表（按实际数据去重）。

        逻辑：
        - 从 market.index_daily_tdx 取 DISTINCT index_code；
        - 转换为 ts_code；
        - 尽量用 index_basic 补齐 name/fullname/market（补不上则为 None）。

        Returns:
            DataFrame 列：ts_code, name, fullname, market
        """

        sql = f"SELECT DISTINCT index_code FROM {INDEX_DAILY_TDX_TABLE} ORDER BY index_code"
        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)
        if df.empty or "index_code" not in df.columns:
            return pd.DataFrame(columns=["ts_code", "name", "fullname", "market"])

        df["ts_code"] = df["index_code"].astype(str).apply(self._tdx_index_code_to_ts_code)
        df = df[df["ts_code"].astype(str).str.len() > 0].copy()
        df = df.drop_duplicates(subset=["ts_code"]).sort_values("ts_code")

        # 补齐基础信息（若 index_basic 中不存在则留空）
        ts_codes = df["ts_code"].astype(str).tolist()
        if not ts_codes:
            return pd.DataFrame(columns=["ts_code", "name", "fullname", "market"])

        base_sql = f"SELECT ts_code, name, fullname, market FROM {INDEX_BASIC_TABLE} WHERE ts_code = ANY(%(codes)s)"
        with get_conn() as conn:  # type: ignore[attr-defined]
            base_df = pd.read_sql(base_sql, conn, params={"codes": ts_codes})

        out = pd.DataFrame({"ts_code": ts_codes})
        if not base_df.empty:
            base_df["ts_code"] = base_df["ts_code"].astype(str)
            out = out.merge(base_df, how="left", on="ts_code")
        else:
            out["name"] = None
            out["fullname"] = None
            out["market"] = None
        return out

    def load_index_daily_tdx(
        self,
        ts_code: str,
        start: date | None,
        end: date | None,
    ) -> pd.DataFrame:
        """加载单个指数的 TDX 日线原始数据并转换为 bin 导出期望口径.

        数据源表：market.index_daily_tdx
        - 价格/成交额：厘 -> 元（/1000）
        - 成交量：手（保持不变）

        Returns:
            DataFrame 列：trade_date, ts_code, open, high, low, close, volume, amount
        """

        code = (ts_code or "").strip()
        if not code:
            return pd.DataFrame()

        tdx_code = self._ts_code_to_tdx_index_code(code)
        if not tdx_code:
            return pd.DataFrame()

        conditions: list[str] = ["index_code = %(index_code)s"]
        params: dict[str, object] = {"index_code": tdx_code}

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
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand,
                amount_li
            FROM {INDEX_DAILY_TDX_TABLE}
            WHERE {where_clause}
            ORDER BY trade_date
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return df

        out = pd.DataFrame()
        out["trade_date"] = pd.to_datetime(df["trade_date"], utc=False).dt.date
        out["ts_code"] = code
        out["open"] = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        out["high"] = pd.to_numeric(df["high_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        out["low"] = pd.to_numeric(df["low_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        out["close"] = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        # 指数成交量：手 -> 股（不做复权处理）
        out["volume"] = pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0
        out["amount"] = pd.to_numeric(df["amount_li"], errors="coerce") / PRICE_UNIT_DIVISOR

        return out

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
        table_name: str | None = None,
        with_factor: bool = False,
    ) -> pd.DataFrame:
        """加载指定股票在给定日期区间内的日线数据.

        返回 MultiIndex (datetime, instrument) 的 DataFrame，列使用逻辑字段名。
        
        Phase 2 强化:
        - 自动将 *_li 字段除以 1000 (厘 -> 元);
        - 自动将 volume_hand 乘以 100 (手 -> 股);
        - 若 with_factor=True, 则关联获取前复权因子 $factor.
        """

        codes = list(ts_codes)
        if not codes:
            return pd.DataFrame()

        # 修复: 默认使用不复权表，避免使用未全量更新的前复权表
        target_table = table_name or DAILY_RAW_TABLE

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
            FROM {target_table}
            WHERE {where_clause}
            ORDER BY trade_date, ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn, params=params)

        if df.empty:
            return df

        # 1. 数值缩放 (厘 -> 元, 手 -> 股)
        df["open"] = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        df["high"] = pd.to_numeric(df["high_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        df["low"] = pd.to_numeric(df["low_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        df["close"] = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        df["amount"] = pd.to_numeric(df["amount_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        df["volume"] = pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0

        # 2. 获取复权因子 (如果需要)
        if with_factor:
            provider = AdjFactorProvider()
            adj_df = provider.get_adj_factor(codes, start or date(2000, 1, 1), end or date.today())
            if not adj_df.empty:
                adj_df = provider.calculate_qfq_factor(adj_df)
                # 转换日期格式以便 merge
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                adj_df["trade_date"] = pd.to_datetime(adj_df["trade_date"])
                df = df.merge(adj_df[["ts_code", "trade_date", "qfq_factor"]], on=["ts_code", "trade_date"], how="left")
                df = df.rename(columns={"qfq_factor": "factor"})
            else:
                # 修复: 复权因子缺失时报错，不使用兜底值
                raise ValueError(
                    f"复权因子数据缺失，无法计算前复权价格。"
                    f"请检查 market.adj_factor 表是否有 {len(codes)} 只股票在 {start} 至 {end} 期间的数据。"
                )
        else:
            df["factor"] = 1.0

        # 3. 构造 MultiIndex (datetime, instrument)
        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df = df.set_index(["datetime", "ts_code"])  # type: ignore[call-arg]
        df.index = df.index.set_names(["datetime", "instrument"])

        # 仅保留逻辑字段列
        cols = ["open", "high", "low", "close", "volume", "amount", "factor"]
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
            conditions.append("trade_time >= %(start_ts)s")
            params["start_ts"] = datetime.combine(start, datetime.min.time())
        if end is not None:
            # Keep the predicate sargable for minute-table indexes; avoid trade_time::date.
            conditions.append("trade_time < %(end_next_ts)s")
            params["end_next_ts"] = datetime.combine(end + timedelta(days=1), datetime.min.time())

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
        return self._normalize_ts_code(ts_code)

    def load_qlib_daily_data(
        self,
        ts_codes: Iterable[str],
        start: date,
        end: date,
        use_tushare_adj: bool = True,
        instrument_start_dates: Mapping[str, date] | None = None,
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
            instrument_start_dates: 可选的逐股票最早有效日期；在复权合并前过滤更早源记录

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
                volume_hand,
                amount_li
            FROM {DAILY_RAW_TABLE}
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY trade_date, ts_code
        """
        params = {"codes": codes, "start": start, "end": end}

        with get_conn() as conn:
            price_df = pd.read_sql(sql, conn, params=params)

        price_df = _filter_instrument_start_dates(price_df, instrument_start_dates)
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
            missing_count = len(missing)
            
            # 获取缺失复权因子的日期范围
            min_missing_date = missing["trade_date"].min()
            max_missing_date = missing["trade_date"].max()
            affected_stocks = missing["ts_code"].nunique()
            
            # 检查 adj_factor 表的数据范围
            adj_min_date = adj_df["trade_date"].min() if not adj_df.empty else None
            adj_max_date = adj_df["trade_date"].max() if not adj_df.empty else None
            
            raise RuntimeError(
                f"Missing adjustment factors for {missing_count} records ({affected_stocks} stocks). "
                f"Date range: {min_missing_date} to {max_missing_date}. "
                f"adj_factor table available range: {adj_min_date} to {adj_max_date}. "
                f"Please ensure the export date range is within the adj_factor data range. "
                f"Examples: {missing.head().to_dict(orient='records')}"
            )

        # 4. 计算 Qlib 格式数据
        # 价格单位转换：厘 -> 元，并按前复权因子调整

        # 严格检查：不允许 qfq_factor 为 0，抛出异常
        zero_factor_mask = price_df["qfq_factor"] == 0
        if zero_factor_mask.any():
            # 获取所有 qfq_factor = 0 的股票代码和日期
            zero_factor_records = price_df[zero_factor_mask][["ts_code", "trade_date"]].drop_duplicates()
            # 按股票分组统计
            zero_factor_by_stock = zero_factor_records.groupby("ts_code").size().to_dict()
            # 找出所有记录都是异常的股票
            all_zero_factor_stocks = []
            for stock in zero_factor_by_stock:
                total_records = len(price_df[price_df["ts_code"] == stock])
                if zero_factor_by_stock[stock] == total_records:
                    all_zero_factor_stocks.append(stock)
            
            raise RuntimeError(
                f"Found {zero_factor_mask.sum()} records with qfq_factor = 0, "
                f"which would cause division by zero. "
                f"Stocks with all zero factors: {all_zero_factor_stocks[:10]}... "
                f"Please check the adj_factor data for these stocks."
            )

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

        # 5. 数据质量验证
        # 检查价格是否为 0 或负数
        invalid_price_mask = (price_df["$open"] <= 0) | (price_df["$high"] <= 0) | (price_df["$low"] <= 0) | (price_df["$close"] <= 0)
        if invalid_price_mask.any():
            invalid_price_records = price_df[invalid_price_mask][["ts_code", "trade_date", "$open", "$high", "$low", "$close"]].drop_duplicates()
            raise RuntimeError(
                f"Found {invalid_price_mask.sum()} records with invalid prices (<= 0). "
                f"Examples: {invalid_price_records.head().to_dict(orient='records')}"
            )

        # 检查成交量是否为负数或无穷大（成交量为 0 是允许的，表示停牌）
        invalid_volume_mask = (price_df["$volume"] < 0) | (~np.isfinite(price_df["$volume"]))
        if invalid_volume_mask.any():
            invalid_volume_records = price_df[invalid_volume_mask][["ts_code", "trade_date", "$volume"]].drop_duplicates()
            raise RuntimeError(
                f"Found {invalid_volume_mask.sum()} records with invalid volume (< 0 or inf). "
                f"Examples: {invalid_volume_records.head().to_dict(orient='records')}"
            )

        # 检查复权因子是否在合理范围内（0 < factor <= 1）
        invalid_factor_mask = (price_df["$factor"] <= 0) | (price_df["$factor"] > 1)
        if invalid_factor_mask.any():
            invalid_factor_records = price_df[invalid_factor_mask][["ts_code", "trade_date", "$factor"]].drop_duplicates()
            raise RuntimeError(
                f"Found {invalid_factor_mask.sum()} records with invalid adjustment factor (<= 0 or > 1). "
                f"Examples: {invalid_factor_records.head().to_dict(orient='records')}"
            )

        # 6. 转换为 Qlib 格式
        # 为了与 bin 目录和其他 H5 数据集保持一致，这里直接使用 ts_code 作为 instrument，
        # 统一采用 Tushare ts_code 格式（例如 000001.SZ / 600000.SH）。
        price_df["instrument"] = price_df["ts_code"].astype(str)
        price_df["datetime"] = pd.to_datetime(price_df["trade_date"])

        # 6. 设置 MultiIndex：Index = (datetime, instrument)
        price_df = price_df.set_index(["datetime", "instrument"])

        # 7. 只保留 Qlib 列（$amount 为可选列）
        qlib_cols = ["$open", "$close", "$high", "$low", "$volume", "$factor"]
        if "$amount" in price_df.columns:
            qlib_cols.append("$amount")
        result = price_df[qlib_cols].copy()

        # 8. 排序
        result = result.sort_index()

        return result

    def load_qlib_minute_data_all(
        self,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        use_tushare_adj: bool = True,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
        freq: str = "1m",
    ) -> pd.DataFrame:
        """加载全部股票的 Qlib 格式分钟线数据。

        过滤规则与日线的 ``load_qlib_daily_data_all`` 保持一致，包括：
        - 按交易所筛选（sh/sz）
        - 可选排除 ST 股票
        - 可选排除退市或当前暂停上市股票
        """

        # 与 H5 导出保持一致：股票池严格来自 stock_basic + stock_st 过滤
        # 时间过滤规则 A：list_date <= end
        codes = self.get_base_ts_codes(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if not codes:
            return pd.DataFrame()

        batch_size = 500
        all_data = []

        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i : i + batch_size]
            batch_df = self.load_qlib_minute_data(
                batch_codes,
                start,
                end,
                freq=freq,
                use_tushare_adj=use_tushare_adj,
            )
            if not batch_df.empty:
                all_data.append(batch_df)

        if not all_data:
            return pd.DataFrame()

        result = pd.concat(all_data)
        result = result.sort_index()

        return result

    def load_moneyflow_panel(
        self,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        *,
        ts_codes: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载个股资金流向数据（moneyflow_ts）并转换为 Qlib/RD-Agent 友好的面板格式.

        返回 DataFrame:
        - Index: MultiIndex (datetime, instrument)
        - Columns: mf_sm_buy_vol, mf_sm_sell_vol, mf_sm_buy_amt, mf_sm_sell_amt, ...
          单位：_vol 为股，_amt 为元。
        """
        # 使用JOIN stock_basic方式，无需指定股票列表
        # 通过stock_basic表过滤ST、退市股票和交易所
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件
        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)

        # 如果调用方传入了 ts_codes（来自 get_base_ts_codes），则额外限制股票范围，
        # 确保 moneyflow 的股票集与其他数据集完全一致。
        ts_code_filter = ""
        if ts_codes:
            # 将 Qlib 格式（000001.SZ）还原为数据库格式进行匹配
            quoted = ", ".join(f"'{c}'" for c in ts_codes)
            ts_code_filter = f" AND m.ts_code IN ({quoted})"
        
        sql = f"""
            SELECT
                m.trade_date,
                m.ts_code,
                m.buy_sm_vol,
                m.buy_sm_amount,
                m.sell_sm_vol,
                m.sell_sm_amount,
                m.buy_md_vol,
                m.buy_md_amount,
                m.sell_md_vol,
                m.sell_md_amount,
                m.buy_lg_vol,
                m.buy_lg_amount,
                m.sell_lg_vol,
                m.sell_lg_amount,
                m.buy_elg_vol,
                m.buy_elg_amount,
                m.sell_elg_vol,
                m.sell_elg_amount,
                m.net_mf_vol,
                m.net_mf_amount
            FROM {MONEYFLOW_TS_TABLE} m
            INNER JOIN market.stock_basic s ON m.ts_code = s.ts_code
            WHERE m.trade_date >= '{start.isoformat()}'
              AND m.trade_date <= '{end.isoformat()}'
              AND {where_clause}{ts_code_filter}
            ORDER BY m.trade_date, m.ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            # 直接使用 cursor.execute + fetchall，且不再使用 DB-API 参数占位符，
            # 避免驱动在处理参数列表时出现索引越界等问题。
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            return df

        # Source DB keeps Tushare units (hand/10k CNY); all consumers use share/CNY.
        df = normalize_tushare_moneyflow_units(df, copy=False)

        # 构造 MultiIndex (datetime, instrument)
        # 直接使用 ts_code 作为 instrument，保证与日线 / bin 中使用的代码格式一致（000001.SZ/600000.SH）。
        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df["instrument"] = df["ts_code"].apply(self._normalize_ts_code).astype(str)
        df = df.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        # 重命名列为 mf_*_* 格式
        rename_map = {
            "buy_sm_vol": "mf_sm_buy_vol",
            "sell_sm_vol": "mf_sm_sell_vol",
            "buy_sm_amount": "mf_sm_buy_amt",
            "sell_sm_amount": "mf_sm_sell_amt",
            "buy_md_vol": "mf_md_buy_vol",
            "sell_md_vol": "mf_md_sell_vol",
            "buy_md_amount": "mf_md_buy_amt",
            "sell_md_amount": "mf_md_sell_amt",
            "buy_lg_vol": "mf_lg_buy_vol",
            "sell_lg_vol": "mf_lg_sell_vol",
            "buy_lg_amount": "mf_lg_buy_amt",
            "sell_lg_amount": "mf_lg_sell_amt",
            "buy_elg_vol": "mf_elg_buy_vol",
            "sell_elg_vol": "mf_elg_sell_vol",
            "buy_elg_amount": "mf_elg_buy_amt",
            "sell_elg_amount": "mf_elg_sell_amt",
            "net_mf_vol": "mf_net_vol",
            "net_mf_amount": "mf_net_amt",
        }
        df = df.rename(columns=rename_map)

        # 仅保留 mf_* 列，并统一为 float32
        mf_cols = [col for col in df.columns if col.startswith("mf_")]
        result = df[mf_cols].astype("float32")

        # 排序索引
        result = result.sort_index()

        return result

    def load_sector_data_panel(
        self,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        *,
        ts_codes: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载 sector_data（申万 L2 行业展开到个股级别）并转换为 Qlib/RD-Agent 格式.

        返回 DataFrame:
        - Index: MultiIndex (datetime, instrument)
        - Columns: sw2_* 22 numeric float32 columns plus int l2_code_id
        """
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")

        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")

        where_clause = " AND ".join(base_conds)

        ts_code_filter = ""
        if ts_codes:
            quoted = ", ".join(f"'{c}'" for c in ts_codes)
            ts_code_filter = f" AND sd.ts_code IN ({quoted})"

        sw2_cols = [
            "sw2_open", "sw2_high", "sw2_low", "sw2_close", "sw2_pct_change",
            "sw2_vol", "sw2_amount", "sw2_pe", "sw2_pb", "sw2_total_mv",
            "sw2_mf_buy_sm_amt", "sw2_mf_sell_sm_amt",
            "sw2_mf_buy_md_amt", "sw2_mf_sell_md_amt",
            "sw2_mf_buy_lg_amt", "sw2_mf_sell_lg_amt",
            "sw2_mf_buy_elg_amt", "sw2_mf_sell_elg_amt",
            "sw2_mf_net_amt",
            "sw2_mf_buy_elg_vol", "sw2_mf_sell_elg_vol",
            "sw2_mf_net_vol",
        ]
        col_list = ", ".join(f"sd.{c}" for c in sw2_cols)

        sql = f"""
            SELECT
                sd.trade_date,
                sd.ts_code,
                ind.l2_code,
                {col_list}
            FROM market.sector_data sd
            INNER JOIN market.stock_basic s ON sd.ts_code = s.ts_code
            LEFT JOIN LATERAL (
                SELECT l2_code
                FROM market.sw_index_member m
                WHERE m.ts_code = sd.ts_code
                  AND m.in_date <= sd.trade_date
                  AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
                ORDER BY m.in_date DESC NULLS LAST, m.out_date DESC NULLS LAST
                LIMIT 1
            ) ind ON TRUE
            WHERE sd.trade_date >= '{start.isoformat()}'
              AND sd.trade_date <= '{end.isoformat()}'
              AND {where_clause}{ts_code_filter}
            ORDER BY sd.trade_date, sd.ts_code
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
            l2_code_map = load_sw_l2_code_map(conn)
        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            return df

        # 构造 MultiIndex (datetime, instrument)
        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df["instrument"] = df["ts_code"].apply(self._normalize_ts_code).astype(str)
        df = df.set_index(["datetime", "instrument"])

        # 仅保留 sw2_* 列，统一为 float32
        result = df[sw2_cols].astype("float32")
        result["l2_code_id"] = np.asarray(
            encode_l2_codes(df["l2_code"].tolist(), l2_code_map),
            dtype=np.int16,
        )
        result = result.sort_index()
        self._warn_low_l2_code_coverage(result)
        return result

    def _warn_low_l2_code_coverage(self, df: pd.DataFrame) -> None:
        if df.empty or "l2_code_id" not in df.columns:
            return
        coverage = df["l2_code_id"].ne(UNKNOWN_L2_CODE_ID).groupby(level="datetime").agg(["sum", "count"])
        for dt_value, row in coverage.iterrows():
            total = int(row["count"])
            matched = int(row["sum"])
            if total <= 0:
                continue
            ratio = matched / total
            if ratio < 0.90:
                missing = total - matched
                label = dt_value.date().isoformat() if hasattr(dt_value, "date") else str(dt_value)
                self.logger.warning(
                    "sector_data_l2_code_id_coverage_below_threshold "
                    "reason_code=sector_data_l2_code_id_low_coverage "
                    "trade_date=%s coverage=%.4f missing_count=%d total_count=%d",
                    label,
                    ratio,
                    missing,
                    total,
                )

    def get_moneyflow_ts_codes(
        self,
        *,
        start: date,
        end: date,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> List[str]:
        """按 moneyflow_ts 的覆盖范围获取股票列表（ts_code），用于对齐各导出数据集的股票池。"""

        conditions: list[str] = [
            f"trade_date >= '{start.isoformat()}'",
            f"trade_date <= '{end.isoformat()}'",
        ]

        normalized = self._normalize_stock_export_exchanges(exchanges)
        exchange_conds: list[str] = []
        if "sh" in normalized:
            exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
        if "sz" in normalized:
            exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
        if exchange_conds:
            conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append(
                "ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)",
            )
        if exclude_delisted_or_paused:
            conditions.append(
                "ts_code NOT IN ("
                "SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P')"
                ")",
            )

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT ts_code
            FROM {MONEYFLOW_TS_TABLE}
            WHERE {where_clause}
            ORDER BY ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        return [self._normalize_ts_code(r[0]) for r in rows]

    def _get_base_universe(
        self,
        exchanges: Optional[List[str]] = None,
        *,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> List[str]:
        """基于 stock_basic 构建统一股票池（返回 ts_code 列表）。

        约定：
        - 默认仅包含上交所 / 深交所（.SH / .SZ），不包含北交所（.BJ）。
        - exclude_st=True 时，剔除在 stock_st 中出现过的所有股票（曾经 / 当前 ST）。
        - exclude_delisted_or_paused=True 时，剔除 list_status IN ('D','P') 的股票。
        """

        conditions: list[str] = []

        # 交易所过滤
        normalized = self._normalize_stock_export_exchanges(exchanges)

        exchange_conds: list[str] = []
        if "sh" in normalized:
            exchange_conds.append("ts_code LIKE '%.SH'")
        if "sz" in normalized:
            exchange_conds.append("ts_code LIKE '%.SZ'")
        if exchange_conds:
            conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append(
                "ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)",
            )

        if exclude_delisted_or_paused:
            conditions.append("list_status NOT IN ('D','P')")

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
            SELECT DISTINCT ts_code
            FROM market.stock_basic
            {where_clause}
            ORDER BY ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        return [r[0] for r in rows]

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

        股票池严格基于 stock_basic：
        - 默认只包含 SH/SZ A 股（.SH/.SZ），不含 BJ；
        - 根据 exclude_st / exclude_delisted_or_paused 统一剔除 ST 和退市 / 暂停上市股票；
        - 然后在给定日期区间内从日线原始表加载数据。
        """

        # 1. 基于 stock_basic 获取统一股票池（与 H5 导出保持一致）
        # 时间过滤规则 A：list_date <= end
        codes = self.get_base_ts_codes(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
        )

        if not codes:
            return pd.DataFrame()

        # 2. 分批加载数据（避免内存溢出）
        batch_size = 500
        all_data = []

        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i : i + batch_size]
            batch_df = self.load_qlib_daily_data(batch_codes, start, end, use_tushare_adj)
            if batch_df.empty:
                # 如果某个批次的数据为空，记录警告并跳过
                self.logger.warning(
                    f"Batch {i // batch_size + 1} (codes {batch_codes[0]} - {batch_codes[-1]}) "
                    f"returned empty DataFrame, skipping"
                )
            else:
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

        # 1. 加载分钟线原始数据（含涨跌停价，用于生成 $limit_up/$limit_down）
        sql = f"""
            SELECT
                k.trade_time,
                k.ts_code,
                k.open_li,
                k.high_li,
                k.low_li,
                k.close_li,
                k.volume_hand,
                k.amount_li,
                sl.up_limit,
                sl.down_limit
            FROM {MINUTE_RAW_TABLE} k
            LEFT JOIN market.stk_limit sl
                ON k.ts_code = sl.ts_code
                AND k.trade_time::date = sl.trade_date
            WHERE k.ts_code = ANY(%(codes)s)
              AND k.freq = %(freq)s
              AND k.trade_time::date >= %(start)s
              AND k.trade_time::date <= %(end)s
            ORDER BY k.trade_time, k.ts_code
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

        # 4b. 计算涨跌停标志（不复权价格与 stk_limit 比较）
        if "up_limit" in price_df.columns and "down_limit" in price_df.columns:
            open_yuan  = price_df["open_li"]  / PRICE_UNIT_DIVISOR
            high_yuan  = price_df["high_li"]  / PRICE_UNIT_DIVISOR
            low_yuan   = price_df["low_li"]   / PRICE_UNIT_DIVISOR
            close_yuan = price_df["close_li"] / PRICE_UNIT_DIVISOR
            # 一字涨停：open/low/close 全部 >= up_limit
            limit_up_mask = (
                (close_yuan >= price_df["up_limit"]) &
                (open_yuan  >= price_df["up_limit"]) &
                (low_yuan   >= price_df["up_limit"])
            )
            # 一字跌停：open/high/close 全部 <= down_limit
            limit_down_mask = (
                (close_yuan <= price_df["down_limit"]) &
                (open_yuan  <= price_df["down_limit"]) &
                (high_yuan  <= price_df["down_limit"])
            )
            price_df["$limit_up"]   = limit_up_mask.astype(np.float32)
            price_df["$limit_down"] = limit_down_mask.astype(np.float32)
            # 无 stk_limit 数据或无 OHLC 时置 NaN
            nan_mask = price_df["up_limit"].isna() | price_df["close_li"].isna()
            price_df.loc[nan_mask, "$limit_up"]   = np.nan
            price_df.loc[nan_mask, "$limit_down"] = np.nan

        # 5. 转换为 Qlib 格式
        price_df["instrument"] = price_df["ts_code"].astype(str)
        price_df["datetime"] = pd.to_datetime(price_df["trade_time"])

        # 6. 设置 MultiIndex
        price_df = price_df.set_index(["datetime", "instrument"])

        # 7. 只保留 Qlib 列（$amount/$limit_up/$limit_down 为可选列）
        qlib_cols = ["$open", "$close", "$high", "$low", "$volume", "$factor"]
        if "$amount" in price_df.columns:
            qlib_cols.append("$amount")
        if "$limit_up" in price_df.columns:
            qlib_cols.append("$limit_up")
        if "$limit_down" in price_df.columns:
            qlib_cols.append("$limit_down")
        result = price_df[qlib_cols].copy()

        # 8. 排序
        result = result.sort_index()

        return result

    def load_bak_basic_panel(
        self,
        *,
        start: date,
        end: date,
        ts_codes: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载 Tushare bak_basic 历史股票列表数据并转换为 Qlib/RD-Agent 友好的面板格式.

        源表：market.bak_basic

        Returns:
            DataFrame
                - Index: MultiIndex (datetime, instrument)
                - Columns: bb_* 系列字段（float32）
        """
        # 使用JOIN stock_basic方式，无需指定股票列表
        # 通过stock_basic表过滤ST、退市股票和交易所
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件（ST、退市、上市时间）
        base_conds = [
            "s.list_status = 'L'",  # 上市状态
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",  # 已上市
            "b.trade_date >= s.list_date",  # 只导出上市日期之后的数据
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)
        
        sql = f"""
            SELECT
                b.trade_date,
                b.ts_code,
                b.name,
                b.industry,
                b.area,
                b.pe_dyn,
                b.total_assets,
                b.liquid_assets,
                b.fixed_assets,
                b.reserved,
                b.reserved_pershare,
                b.eps,
                b.bvps,
                b.list_date,
                b.undp,
                b.per_undp,
                b.rev_yoy,
                b.profit_yoy,
                b.gpr,
                b.npr,
                b.holder_num
            FROM market.bak_basic b
            INNER JOIN market.stock_basic s ON b.ts_code = s.ts_code
            WHERE b.trade_date >= '{start.isoformat()}'
              AND b.trade_date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY b.trade_date, b.ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df["instrument"] = df["ts_code"].apply(self._normalize_ts_code).astype(str)
        df = df.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        rename_map = {
            "pe_dyn": "bb_pe_dyn",
            "total_assets": "bb_total_assets",
            "liquid_assets": "bb_liquid_assets",
            "fixed_assets": "bb_fixed_assets",
            "reserved": "bb_reserved",
            "reserved_pershare": "bb_reserved_pershare",
            "eps": "bb_eps",
            "bvps": "bb_bvps",
            "undp": "bb_undp",
            "per_undp": "bb_per_undp",
            "rev_yoy": "bb_rev_yoy",
            "profit_yoy": "bb_profit_yoy",
            "gpr": "bb_gpr",
            "npr": "bb_npr",
            "holder_num": "bb_holder_num",
        }

        df = df.rename(columns=rename_map)

        # 仅保留 bb_ 列，并统一为 float32
        bb_cols = [c for c in df.columns if c.startswith("bb_")]
        result = df[bb_cols].copy()
        
        # 添加调试日志
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[load_bak_basic_panel] 开始转换 {len(bb_cols)} 列到 float32")
        
        for c in bb_cols:
            # 记录转换前的非空值数量
            pre_count = result[c].notna().sum()
            
            # 安全转换：处理可能的 Decimal 类型
            if result[c].dtype == 'object':
                # 对 object 类型（可能是 Decimal）使用更安全的转换
                result[c] = result[c].apply(
                    lambda x: float(x) if pd.notna(x) and str(x).strip() not in ['', 'None', 'nan'] else np.nan
                )
            else:
                # 对其他类型使用 pd.to_numeric
                result[c] = pd.to_numeric(result[c], errors="coerce")
            
            # 转换为 float32
            result[c] = result[c].astype("float32")
            
            # 记录转换后的非空值数量
            post_count = result[c].notna().sum()
            if post_count == 0 and pre_count > 0:
                logger.warning(f"[load_bak_basic_panel] 列 {c}: 转换前 {pre_count} 个非空值，转换后全部丢失！")
            elif pre_count != post_count:
                logger.warning(f"[load_bak_basic_panel] 列 {c}: 转换前 {pre_count} 个非空值，转换后 {post_count} 个")
        
        result = result.sort_index()
        logger.info(f"[load_bak_basic_panel] 数据转换完成，最终形状: {result.shape}")

        return result

    def load_cyq_perf_panel(
        self,
        *,
        start: date,
        end: date,
        ts_codes: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载 Tushare cyq_perf 每日筹码及胜率数据并转换为 Qlib/RD-Agent 友好的面板格式.

        源表：market.cyq_perf

        Returns:
            DataFrame
                - Index: MultiIndex (datetime, instrument)
                - Columns: cp_* 系列字段（float32）
        """
        # 使用JOIN stock_basic方式，无需指定股票列表
        # 通过stock_basic表过滤ST、退市股票和交易所
        
        # 构建交易所过滤条件
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            # 默认只包含SH/SZ
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")
        
        # 构建基础过滤条件
        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
        ]
        
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")
        
        where_clause = " AND ".join(base_conds)
        
        sql = f"""
            SELECT
                c.trade_date,
                c.ts_code,
                c.his_low,
                c.his_high,
                c.cost_5pct,
                c.cost_15pct,
                c.cost_50pct,
                c.cost_85pct,
                c.cost_95pct,
                c.weight_avg,
                c.winner_rate
            FROM market.cyq_perf c
            INNER JOIN market.stock_basic s ON c.ts_code = s.ts_code
            WHERE c.trade_date >= '{start.isoformat()}'
              AND c.trade_date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY c.trade_date, c.ts_code
        """

        with get_conn() as conn:  # type: ignore[attr-defined]
            df = pd.read_sql(sql, conn)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df["instrument"] = df["ts_code"].apply(self._normalize_ts_code).astype(str)
        df = df.set_index(["datetime", "instrument"])  # type: ignore[call-arg]

        rename_map = {
            "his_low": "cp_his_low",
            "his_high": "cp_his_high",
            "cost_5pct": "cp_cost_5pct",
            "cost_15pct": "cp_cost_15pct",
            "cost_50pct": "cp_cost_50pct",
            "cost_85pct": "cp_cost_85pct",
            "cost_95pct": "cp_cost_95pct",
            "weight_avg": "cp_weight_avg",
            "winner_rate": "cp_winner_rate",
        }

        df = df.rename(columns=rename_map)

        # 仅保留 cp_ 列，并统一为 float32
        cp_cols = [c for c in df.columns if c.startswith("cp_")]
        result = df[cp_cols].copy()
        for c in cp_cols:
            result[c] = pd.to_numeric(result[c], errors="coerce").astype("float32")
        result = result.sort_index()

        return result

    def load_margin_detail_panel(
        self,
        *,
        start: date,
        end: date,
        ts_codes: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        exclude_st: bool = False,
        exclude_delisted_or_paused: bool = False,
    ) -> pd.DataFrame:
        """加载融资融券明细数据并转换为 Qlib 面板格式.

        源表：market.margin_detail

        Returns:
            DataFrame - Index: MultiIndex (datetime, instrument), Columns: md_* (float32)
        """
        exchange_conds = []
        if exchanges:
            normalized = self._normalize_stock_export_exchanges(exchanges)
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(s.ts_code LIKE '%.SZ' OR s.ts_code LIKE 'SZ%')")
        else:
            exchange_conds.append("(s.ts_code LIKE '%.SH' OR s.ts_code LIKE '%.SZ')")

        base_conds = [
            "s.list_status = 'L'",
            f"s.list_date + INTERVAL '{IPO_FILTER_DAYS} days' <= '{end.isoformat()}'",
            "m.trade_date >= s.list_date",
        ]
        if exclude_st:
            base_conds.append(f"s.ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st WHERE ann_date < '{end.isoformat()}')")
        if exclude_delisted_or_paused:
            base_conds.append("s.list_status NOT IN ('D', 'P')")
        if exchange_conds:
            base_conds.append("(" + " OR ".join(exchange_conds) + ")")

        where_clause = " AND ".join(base_conds)

        sql = f"""
            SELECT
                m.trade_date, m.ts_code,
                m.rzye, m.rqye, m.rzmre, m.rqyl,
                m.rzche, m.rqchl, m.rqmcl, m.rzrqye
            FROM market.margin_detail m
            INNER JOIN market.stock_basic s ON m.ts_code = s.ts_code
            WHERE m.trade_date >= '{start.isoformat()}'
              AND m.trade_date <= '{end.isoformat()}'
              AND {where_clause}
            ORDER BY m.trade_date, m.ts_code
        """

        with get_conn() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["trade_date"], utc=False)
        df["instrument"] = df["ts_code"].apply(self._normalize_ts_code).astype(str)
        df = df.set_index(["datetime", "instrument"])

        rename_map = {
            "rzye": "md_rzye", "rqye": "md_rqye", "rzmre": "md_rzmre",
            "rqyl": "md_rqyl", "rzche": "md_rzche", "rqchl": "md_rqchl",
            "rqmcl": "md_rqmcl", "rzrqye": "md_rzrqye",
        }
        df = df.rename(columns=rename_map)

        md_cols = [c for c in df.columns if c.startswith("md_")]
        result = df[md_cols].copy()
        for c in md_cols:
            result[c] = pd.to_numeric(result[c], errors="coerce").astype("float32")
        result = result.sort_index()
        return result
