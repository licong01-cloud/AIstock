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

from backend.db.pg_pool import get_conn

from .config import (
    ADJ_FACTOR_TABLE,
    DAILY_RAW_TABLE,
    FACTOR_DATA_TABLE,
    FIELD_MAPPING_DB_DAILY,
    FIELD_MAPPING_DB_MINUTE,
    FIELD_MAPPING_FACTOR,
    INDEX_BASIC_TABLE,
    INDEX_DAILY_TABLE,
    INDEX_DAILY_TDX_TABLE,
    MINUTE_RAW_TABLE,
    MINUTE_QFQ_TABLE,
    MONEYFLOW_TS_TABLE,
    PRICE_UNIT_DIVISOR,
    TDX_BOARD_DAILY_TABLE,
    TDX_BOARD_INDEX_TABLE,
    TDX_BOARD_MEMBER_TABLE,
)
from .adj_factor_provider import AdjFactorProvider


class DBReader:
    """封装针对前复权日线表和分钟线表的读取逻辑."""

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

        conditions: list[str] = [
            f"trade_date >= '{start.isoformat()}'",
            f"trade_date <= '{end.isoformat()}'",
        ]

        if ts_codes:
            codes = [self._normalize_ts_code(c) for c in ts_codes if str(c).strip()]
            if codes:
                conditions.append(f"ts_code IN ({self._quote_sql_strings(codes)})")

        # 按交易所过滤（基于 ts_code 后缀 .SH / .SZ / .BJ）
        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e and e.strip()}
            exchange_conds: list[str] = []
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
                if "bj" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
            if exchange_conds:
                conditions.append("(" + " OR ".join(exchange_conds) + ")")

        # ST / 退市 / 暂停上市过滤
        if exclude_st:
            conditions.append("ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)")
        if exclude_delisted_or_paused:
            conditions.append(
                "ts_code NOT IN (SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P'))",
            )

        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT
                trade_date,
                ts_code,
                close,
                turnover_rate,
                turnover_rate_f,
                volume_ratio,
                pe,
                pe_ttm,
                pb,
                ps,
                ps_ttm,
                dv_ratio,
                dv_ttm,
                total_share,
                float_share,
                free_share,
                total_mv,
                circ_mv
            FROM market.daily_basic
            WHERE {where_clause}
            ORDER BY trade_date, ts_code
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

        conditions: list[str] = [
            f"trade_date >= '{start.isoformat()}'",
            f"trade_date <= '{end.isoformat()}'",
        ]

        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}
            exchange_conds: list[str] = []
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
                if "bj" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
            if exchange_conds:
                conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append("ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)")
        if exclude_delisted_or_paused:
            conditions.append(
                "ts_code NOT IN (SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P'))",
            )

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT ts_code
            FROM {MONEYFLOW_TS_TABLE}
            WHERE {where_clause}
            ORDER BY ts_code
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

        conditions: list[str] = [
            f"trade_date >= '{start.isoformat()}'",
            f"trade_date <= '{end.isoformat()}'",
        ]

        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}
            exchange_conds: list[str] = []
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
                if "bj" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
            if exchange_conds:
                conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append("ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)")
        if exclude_delisted_or_paused:
            conditions.append(
                "ts_code NOT IN (SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P'))",
            )

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT ts_code
            FROM market.daily_basic
            WHERE {where_clause}
            ORDER BY ts_code
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

        # 使用内联 SQL（不使用 %(...)s 参数占位符），避免 LIKE 子句中的 %
        # 与 psycopg2 的 pyformat 参数占位符冲突，触发 "argument formats can't be mixed"。
        conditions: list[str] = [
            f"freq = '{freq}'",
            f"trade_time::date >= '{start.isoformat()}'",
            f"trade_time::date <= '{end.isoformat()}'",
        ]

        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}
        else:
            normalized = set()

        exchange_conds: list[str] = []
        if normalized:
            if "sh" in normalized:
                exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
            if "sz" in normalized:
                exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
            if "bj" in normalized:
                exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
        if exchange_conds:
            conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append("ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)")
        if exclude_delisted_or_paused:
            conditions.append(
                "ts_code NOT IN (SELECT ts_code FROM market.stock_basic WHERE list_status IN ('D','P'))",
            )

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT ts_code
            FROM {MINUTE_QFQ_TABLE}
            WHERE {where_clause}
            ORDER BY ts_code
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
        conditions.append("(list_date IS NULL OR list_date <= '%s')" % end.isoformat())

        # 按交易所过滤（基于 ts_code 后缀 .SH / .SZ / .BJ；兼容 SHxxxxxx 形式）
        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e and e.strip()}
            exchange_conds: list[str] = []
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
                if "bj" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
            if exchange_conds:
                conditions.append("(" + " OR ".join(exchange_conds) + ")")

        if exclude_st:
            conditions.append("ts_code NOT IN (SELECT DISTINCT ts_code FROM market.stock_st)")
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
        return self._normalize_ts_code(ts_code)

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
        - 按交易所筛选（sh/sz/bj）
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

        # 起止日期已由 Pydantic 校验为合法日期，这里直接内联到 SQL 字符串，
        # 避免 psycopg2 在参数绑定时出现 "list index out of range" 等兼容性问题。
        conditions: list[str] = [
            f"trade_date >= '{start.isoformat()}'",
            f"trade_date <= '{end.isoformat()}'",
        ]

        if ts_codes:
            codes = [self._normalize_ts_code(c) for c in ts_codes if str(c).strip()]
            if codes:
                conditions.append(f"ts_code IN ({self._quote_sql_strings(codes)})")

        # 按交易所过滤（基于 ts_code 后缀 .SH / .SZ / .BJ）
        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}
            exchange_conds: list[str] = []
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
                if "bj" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
            if exchange_conds:
                conditions.append("(" + " OR ".join(exchange_conds) + ")")

        # ST / 退市 / 暂停上市过滤
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
            SELECT
                trade_date,
                ts_code,
                buy_sm_vol,
                buy_sm_amount,
                sell_sm_vol,
                sell_sm_amount,
                buy_md_vol,
                buy_md_amount,
                sell_md_vol,
                sell_md_amount,
                buy_lg_vol,
                buy_lg_amount,
                sell_lg_vol,
                sell_lg_amount,
                buy_elg_vol,
                buy_elg_amount,
                sell_elg_vol,
                sell_elg_amount,
                net_mf_vol,
                net_mf_amount
            FROM {MONEYFLOW_TS_TABLE}
            WHERE {where_clause}
            ORDER BY trade_date, ts_code
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

        # 单位转换：手 -> 股，万元 -> 元
        vol_cols = [
            "buy_sm_vol",
            "sell_sm_vol",
            "buy_md_vol",
            "sell_md_vol",
            "buy_lg_vol",
            "sell_lg_vol",
            "buy_elg_vol",
            "sell_elg_vol",
            "net_mf_vol",
        ]
        amt_cols = [
            "buy_sm_amount",
            "sell_sm_amount",
            "buy_md_amount",
            "sell_md_amount",
            "buy_lg_amount",
            "sell_lg_amount",
            "buy_elg_amount",
            "sell_elg_amount",
            "net_mf_amount",
        ]

        for col in vol_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce") * 100.0
        for col in amt_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce") * 10000.0

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

        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}
            exchange_conds: list[str] = []
            if normalized:
                if "sh" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SH' OR ts_code LIKE 'SH%')")
                if "sz" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.SZ' OR ts_code LIKE 'SZ%')")
                if "bj" in normalized:
                    exchange_conds.append("(ts_code LIKE '%.BJ' OR ts_code LIKE 'BJ%')")
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
        if exchanges:
            normalized = {e.strip().lower() for e in exchanges if e.strip()}
        else:
            # 默认只导出 SH / SZ，不包含 BJ
            normalized = {"sh", "sz"}

        exchange_conds: list[str] = []
        if "sh" in normalized:
            exchange_conds.append("ts_code LIKE '%.SH'")
        if "sz" in normalized:
            exchange_conds.append("ts_code LIKE '%.SZ'")
        if "bj" in normalized:
            exchange_conds.append("ts_code LIKE '%.BJ'")
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
                volume_hand,
                amount_li
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
        price_df["instrument"] = price_df["ts_code"].astype(str)
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
