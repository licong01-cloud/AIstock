"""
RealtimeFactorDataLoader - 实时因子数据加载器

为因子代码提供统一的数据访问接口，直接从数据库读取数据，
替代对QLib H5文件的依赖。

数据一致性保证：
- 复权逻辑与 generate_static_factors_bundle.py 完全一致
- 字段名映射与 qlib_exporter/config.py 的 FIELD_MAPPING_DB_DAILY 一致
- 价格单位：数据库存储为厘(×1000)，输出为元
- 前复权：价格×adj_factor，成交量÷adj_factor，成交额不复权
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from ..db.pg_pool import get_conn
from backend.services.market_data.instrument_validator import (
    DEFAULT_SQL_CHUNK_SIZE,
    load_chunks_with_logging,
    normalize_and_validate_ts_codes,
    normalize_ts_code,
)

logger = logging.getLogger("aistock.realtime_factor_data_loader")

# 价格单位转换：数据库存储为厘，输出为元
PRICE_UNIT_DIVISOR = 1000.0
def _normalize_realtime_instrument(code: object) -> str:
    return normalize_ts_code(code)


def _validate_realtime_instruments(
    instruments: list[object],
    *,
    source: str,
    start_date: object | None = None,
    end_date: object | None = None,
) -> List[str]:
    return normalize_and_validate_ts_codes(
        instruments,
        source=source,
        start_date=start_date,
        end_date=end_date,
    )


class RealtimeFactorDataLoader:
    """
    实时因子数据加载器。

    为因子代码提供与QLib D.features兼容的数据访问接口，
    直接从PostgreSQL数据库读取数据，支持前复权处理。

    支持快照模式：通过 set_snapshot() 注入预加载的 DataFrame，
    所有实例的 load() 将从快照切片返回，零 DB 访问。

    使用方式（在转换后的因子代码中）：
        loader = RealtimeFactorDataLoader()
        df = loader.load(
            instruments=['000001.SZ', '600000.SH'],
            start_date='2024-01-01',
            end_date='2024-12-31',
            fields=['open', 'close', 'high', 'low', 'volume', 'amount', 'factor']
        )
        # df 为 MultiIndex(datetime, instrument) 的 DataFrame
        # 列名为 open, close, high, low, volume, amount, factor
    """

    # ── 类级别快照（所有实例共享，批次内只读） ──
    _snapshot_df: Optional[pd.DataFrame] = None
    _snapshot_data_date: Optional[str] = None

    @classmethod
    def set_snapshot(cls, df: pd.DataFrame, data_date: str) -> None:
        """注入快照 DataFrame，后续所有 load() 从快照切片。线程安全（设置后只读）。"""
        cls._snapshot_df = df
        cls._snapshot_data_date = data_date
        logger.info(
            f"行情快照已注入: data_date={data_date}, "
            f"{len(df)} 行, {list(df.columns)}"
        )

    @classmethod
    def clear_snapshot(cls) -> None:
        """清除快照，恢复 DB 查询模式。"""
        cls._snapshot_df = None
        cls._snapshot_data_date = None
        logger.info("行情快照已清除")

    # 字段名 -> 数据库列名映射
    # 数据库中价格单位为厘，需要除以1000转换为元
    FIELD_MAP: Dict[str, str] = {
        "open": "open",
        "close": "close",
        "high": "high",
        "low": "low",
        "volume": "volume",
        "amount": "amount",
        "factor": "adj_factor",
    }

    # 价格字段（需要除以1000转换为元）
    PRICE_FIELDS = {"open", "close", "high", "low"}

    # 成交量字段（不需要单位转换）
    VOLUME_FIELDS = {"volume"}

    # 成交额字段（需要除以1000转换为元）
    AMOUNT_FIELDS = {"amount"}

    def __init__(self):
        self._trading_calendar_cache: Optional[pd.DatetimeIndex] = None
        # 全字段缓存: (instruments_hash, start_date, end_date, adjust) -> DataFrame
        self._full_cache: Optional[pd.DataFrame] = None
        self._full_cache_key: Optional[tuple] = None

    def _normalize_instrument(self, code: str) -> str:
        """Normalize stock code to '000001.SZ' format."""
        return _normalize_realtime_instrument(code)

    def _to_ts_code(self, instrument: str) -> str:
        """Convert instrument to validated database ts_code format."""
        return _validate_realtime_instruments(
            [instrument], source="RealtimeFactorDataLoader._to_ts_code"
        )[0]

    def load(
        self,
        instruments: Union[List[str], str],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        fields: Optional[List[str]] = None,
        freq: str = "day",
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """
        加载因子所需的市场数据。

        Args:
            instruments: 股票代码列表，格式 '000001.SZ' 或 'SZ000001'
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要的字段列表，默认加载全部OHLCV+复权因子
                    支持: open, close, high, low, volume, amount, factor
            freq: 频率，目前只支持 'day'
            adjust: 复权方式，'qfq'=前复权（默认），'none'=不复权

        Returns:
            MultiIndex(datetime, instrument) 的 DataFrame
            列名为请求的字段名（如 close, volume 等）
        """
        if freq != "day":
            raise NotImplementedError(f"RealtimeFactorDataLoader 目前只支持 freq='day'，不支持 '{freq}'")

        # 标准化参数
        if isinstance(instruments, str):
            instruments = [instruments]
        else:
            instruments = list(instruments)

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()

        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()

        instruments = _validate_realtime_instruments(
            instruments,
            source="RealtimeFactorDataLoader.load",
            start_date=start_date,
            end_date=end_date,
        )

        if fields is None:
            fields = ["open", "close", "high", "low", "volume", "amount", "factor"]

        # ── 快照模式：从类级别快照切片，零 DB 访问 ──
        if self.__class__._snapshot_df is not None:
            return self._slice_snapshot(instruments, start_date, end_date, fields)

        # ── 防护：如果快照曾被注入但被意外清除，不允许静默回退到 DB ──
        if self.__class__._snapshot_data_date is not None:
            raise RuntimeError(
                f"快照数据已被清除但 snapshot_data_date={self.__class__._snapshot_data_date} 仍存在。"
                "这表明快照在批量计算过程中被意外清除，拒绝回退到 DB 查询。"
            )

        # ── DB 模式：仅用于快照创建（DataSnapshotManager.create_snapshot）──
        cache_key = (tuple(sorted(instruments)), start_date, end_date, adjust)
        if self._full_cache is not None and self._full_cache_key == cache_key:
            # 从缓存中取请求的列
            available = [f for f in fields if f in self._full_cache.columns]
            if available:
                return self._full_cache[available].copy()

        # 首次加载: 全字段查询 → 缓存完整 DataFrame
        all_fields = ["open", "close", "high", "low", "volume", "amount", "factor"]

        # 确定需要从数据库查询的列
        db_cols_needed = set()
        for f in all_fields:
            db_col = self.FIELD_MAP.get(f)
            if db_col:
                db_cols_needed.add(db_col)

        # 始终需要 adj_factor 用于前复权
        if adjust == "qfq":
            db_cols_needed.add("adj_factor")

        # 从数据库加载数据
        df_raw = self._fetch_from_db(instruments, start_date, end_date, db_cols_needed)

        if df_raw.empty:
            logger.warning(f"数据库中没有找到数据: instruments={instruments[:3]}..., {start_date} ~ {end_date}")
            return pd.DataFrame()

        # 应用前复权
        if adjust == "qfq" and "adj_factor" in df_raw.columns:
            df_raw = self._apply_qfq(df_raw)

        # 重命名列为QLib字段名 (全字段)
        df_result = self._rename_to_qlib_fields(df_raw, all_fields)

        # 缓存完整结果
        self._full_cache = df_result
        self._full_cache_key = cache_key
        logger.info(
            f"行情数据已缓存: {len(instruments)} 只股票, "
            f"{start_date}~{end_date}, {len(df_result)} 行, "
            f"{df_result.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB"
        )

        # 返回请求的字段子集
        available = [f for f in fields if f in df_result.columns]
        if available:
            return df_result[available].copy()
        return df_result.copy()

    def _slice_snapshot(
        self,
        instruments: List[str],
        start_date: date,
        end_date: date,
        fields: List[str],
    ) -> pd.DataFrame:
        """从类级别快照 DataFrame 按 instruments/dates/fields 切片。

        快照 DataFrame 结构: MultiIndex(datetime, instrument), 列=[open,close,high,low,volume,amount,factor]
        快照是只读共享的，线程安全。
        """
        df = self.__class__._snapshot_df
        if df is None or df.empty:
            raise RuntimeError(
                "行情快照为空，无法切片。"
                "请检查快照是否正确创建。"
            )

        # 按日期过滤
        sd = pd.Timestamp(start_date)
        ed = pd.Timestamp(end_date)
        dates = df.index.get_level_values(0)
        date_mask = (dates >= sd) & (dates <= ed)

        # 按股票过滤
        instr_set = set(instruments)
        instrs = df.index.get_level_values(1)
        instr_mask = instrs.isin(instr_set)

        mask = date_mask & instr_mask
        result = df.loc[mask]

        if result.empty:
            raise RuntimeError(
                f"行情快照切片结果为空: "
                f"instruments={len(instruments)}只, "
                f"date_range={start_date}~{end_date}, "
                f"快照日期范围={dates.min()}~{dates.max()}"
            )

        # 按字段过滤
        available = [f for f in fields if f in result.columns]
        if available:
            return result[available].copy()
        return result.copy()

    def _fetch_from_db(
        self,
        instruments: List[str],
        start_date: date,
        end_date: date,
        db_cols_needed: set,
    ) -> pd.DataFrame:
        """从 kline_daily_raw + adj_factor 获取K线数据，与 qe_data_service.load_daily_pv 完全一致"""
        from datetime import date as date_type

        ts_codes = _validate_realtime_instruments(
            instruments,
            source="RealtimeFactorDataLoader._fetch_from_db",
            start_date=start_date,
            end_date=end_date,
        )
        # 1. 加载未复权原始行情。大股票池必须分块查询并记录 correlation，
        # 避免盘中执行窗口触发不可诊断的超长 SQL / Broken pipe。
        def _load_raw_chunk(chunk: list[str], _chunk_index: int, correlation_id: str) -> pd.DataFrame:
            placeholders = ",".join(["%s"] * len(chunk))
            sql_raw = f"""
                SELECT trade_date, ts_code,
                       open_li, high_li, low_li, close_li, volume_hand, amount_li
                FROM market.kline_daily_raw
                WHERE ts_code IN ({placeholders})
                  AND trade_date >= %s
                  AND trade_date <= %s
                ORDER BY trade_date, ts_code
            """
            params_raw = chunk + [start_date.isoformat(), end_date.isoformat()]
            try:
                with get_conn() as conn:
                    return pd.read_sql(sql_raw, conn, params=params_raw)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "RealtimeFactorDataLoader raw chunk failed: correlation_id=%s symbols=%s start=%s end=%s error=%s",
                    correlation_id,
                    len(chunk),
                    start_date,
                    end_date,
                    exc,
                )
                raise

        raw_chunks = load_chunks_with_logging(
            ts_codes=ts_codes,
            source="RealtimeFactorDataLoader._fetch_from_db.raw",
            start_date=start_date,
            end_date=end_date,
            chunk_size=DEFAULT_SQL_CHUNK_SIZE,
            loader=_load_raw_chunk,
        )
        df_raw = pd.concat(
            [chunk for chunk in raw_chunks if not chunk.empty],
            ignore_index=True,
        ) if raw_chunks else pd.DataFrame()

        if df_raw.empty:
            return pd.DataFrame()

        # 2. 加载复权因子（查询到今天，确保能获取最新 adj_factor 用于归一化）
        today = date_type.today()

        def _load_adj_chunk(chunk: list[str], _chunk_index: int, correlation_id: str) -> pd.DataFrame:
            placeholders = ",".join(["%s"] * len(chunk))
            sql_adj = f"""
                SELECT ts_code, trade_date, adj_factor
                FROM market.adj_factor
                WHERE ts_code IN ({placeholders})
                  AND trade_date >= %s AND trade_date <= %s
                ORDER BY ts_code, trade_date
            """
            params_adj = chunk + [start_date.isoformat(), today.isoformat()]
            try:
                with get_conn() as conn:
                    return pd.read_sql(sql_adj, conn, params=params_adj)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "RealtimeFactorDataLoader adj_factor chunk failed: correlation_id=%s symbols=%s start=%s end=%s error=%s",
                    correlation_id,
                    len(chunk),
                    start_date,
                    today,
                    exc,
                )
                raise

        adj_chunks = load_chunks_with_logging(
            ts_codes=ts_codes,
            source="RealtimeFactorDataLoader._fetch_from_db.adj_factor",
            start_date=start_date,
            end_date=today,
            chunk_size=DEFAULT_SQL_CHUNK_SIZE,
            loader=_load_adj_chunk,
        )
        df_adj = pd.concat(
            [chunk for chunk in adj_chunks if not chunk.empty],
            ignore_index=True,
        ) if adj_chunks else pd.DataFrame()

        if df_adj.empty:
            raise RuntimeError(f"adj_factor 表中没有找到复权因子数据: {ts_codes[:5]}")

        # 3. 计算前复权因子：qfq_factor = adj_t / adj_latest（最新日=1.0）
        df_adj["adj_factor"] = pd.to_numeric(df_adj["adj_factor"], errors="coerce")
        adj_latest = df_adj.groupby("ts_code")["adj_factor"].transform("max")
        df_adj["qfq_factor"] = df_adj["adj_factor"] / adj_latest

        # 4. merge 复权因子到原始行情
        df_raw["trade_date"] = pd.to_datetime(df_raw["trade_date"]).dt.date
        df_adj["trade_date"] = pd.to_datetime(df_adj["trade_date"]).dt.date

        df = df_raw.merge(
            df_adj[["ts_code", "trade_date", "qfq_factor"]],
            on=["ts_code", "trade_date"],
            how="left",
        )

        # 5. 前向/后向填充缺失的复权因子
        missing_count = df["qfq_factor"].isna().sum()
        if missing_count > 0:
            df = df.sort_values(["ts_code", "trade_date"])
            df["qfq_factor"] = (
                df.groupby("ts_code")["qfq_factor"]
                .transform(lambda s: s.ffill().bfill())
            )

        still_missing = df["qfq_factor"].isna()
        if still_missing.any():
            missing_stocks = df.loc[still_missing, "ts_code"].unique().tolist()
            raise RuntimeError(f"以下股票完全没有复权因子数据: {missing_stocks[:10]}")

        # 6. 计算前复权价格（与 qe_data_service.load_daily_pv 完全一致）
        qfq = df["qfq_factor"].astype("float64")
        df["open"]   = df["open_li"].astype("float64")   / PRICE_UNIT_DIVISOR * qfq
        df["high"]   = df["high_li"].astype("float64")   / PRICE_UNIT_DIVISOR * qfq
        df["low"]    = df["low_li"].astype("float64")    / PRICE_UNIT_DIVISOR * qfq
        df["close"]  = df["close_li"].astype("float64")  / PRICE_UNIT_DIVISOR * qfq
        # 成交量：手->股（×100），再按前复权因子反向调整（÷qfq_factor）
        df["volume"] = df["volume_hand"].astype("float64") * 100.0 / qfq
        # 成交额：厘->元，不复权
        df["amount"] = df["amount_li"].astype("float64") / PRICE_UNIT_DIVISOR
        # 前复权因子（最新日=1.0）
        df["adj_factor"] = qfq

        # 7. 设置索引
        df["datetime"] = pd.to_datetime(df["trade_date"])
        df["instrument"] = df["ts_code"].apply(self._normalize_instrument)
        df = df.set_index(["datetime", "instrument"]).sort_index()

        return df

    def _apply_qfq(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用前复权处理。

        前复权规则（与 generate_static_factors_bundle.py 一致）：
        - 价格（open/high/low/close）× adj_factor
        - 成交量（volume）÷ adj_factor
        - 成交额（amount）不复权
        """
        df = df.copy()
        adj = df["adj_factor"]

        price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
        for col in price_cols:
            df[col] = df[col] * adj

        if "volume" in df.columns:
            df["volume"] = df["volume"] / adj.replace(0, np.nan)

        return df

    def _rename_to_qlib_fields(self, df: pd.DataFrame, requested_fields: List[str]) -> pd.DataFrame:
        """将数据库列名重命名为QLib字段名，并只保留请求的字段"""
        # 反向映射：db列名 -> QLib字段名
        reverse_map: Dict[str, str] = {}
        for qlib_field in requested_fields:
            db_col = self.FIELD_MAP.get(qlib_field)
            if db_col and db_col in df.columns:
                if db_col not in reverse_map:
                    reverse_map[db_col] = qlib_field

        df_result = df.rename(columns=reverse_map)

        # 只保留请求的字段
        keep_cols = [f for f in requested_fields if f in df_result.columns]
        if keep_cols:
            df_result = df_result[keep_cols]

        return df_result

    def get_trading_calendar(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
    ) -> List[date]:
        """获取交易日历"""
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        sql = """
            SELECT cal_date FROM market.trading_calendar
            WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
            ORDER BY cal_date
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (start_date.isoformat(), end_date.isoformat()))
                rows = cur.fetchall()
        return [row[0] for row in rows]

    def get_stock_universe(
        self,
        trade_date: Union[str, date],
        exclude_st: bool = True,
        exchanges: Optional[List[str]] = None,
    ) -> List[str]:
        """
        获取指定交易日的股票池（上市且未退市的股票）

        Args:
            trade_date: 交易日期
            exclude_st: 是否排除ST股票
            exchanges: 交易所过滤，默认 ['SH', 'SZ']

        Returns:
            股票代码列表，格式 '000001.SZ'
        """
        if isinstance(trade_date, str):
            trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()

        if exchanges is None:
            exchanges = ["SH", "SZ"]

        exchange_conds = []
        for ex in exchanges:
            ex_upper = ex.upper()
            exchange_conds.append(f"ts_code LIKE '%.{ex_upper}'")

        exchange_clause = " OR ".join(exchange_conds) if exchange_conds else "1=1"

        st_clause = ""
        if exclude_st:
            st_clause = f"""
                AND ts_code NOT IN (
                    SELECT DISTINCT ts_code FROM market.stock_st
                    WHERE ann_date <= '{trade_date.isoformat()}'
                )
            """

        sql = f"""
            SELECT ts_code FROM market.stock_basic
            WHERE list_status = 'L'
              AND list_date + INTERVAL '365 days' <= '{trade_date.isoformat()}'
              AND ({exchange_clause})
              {st_clause}
            ORDER BY ts_code
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        return _validate_realtime_instruments(
            [row[0] for row in rows],
            source="RealtimeFactorDataLoader.get_stock_universe",
            start_date=trade_date,
            end_date=trade_date,
        )


# 模块级单例，供因子代码直接使用
_default_loader: Optional[RealtimeFactorDataLoader] = None


def get_default_loader() -> RealtimeFactorDataLoader:
    """获取默认的数据加载器单例"""
    global _default_loader
    if _default_loader is None:
        _default_loader = RealtimeFactorDataLoader()
    return _default_loader
