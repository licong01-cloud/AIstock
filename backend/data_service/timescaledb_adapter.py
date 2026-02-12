"""TimescaleDB adapter for AIstock data service.

Provides read-only access to historical bars stored in TimescaleDB.
This module should not introduce any new write paths; it only queries
existing AIstock tables.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List, Optional

import logging
import time
import pandas as pd

from ..qlib_exporter.config import DAILY_RAW_TABLE
from ..qlib_exporter.db_reader import DBReader


logger = logging.getLogger("aistock.timescaledb_adapter")


def fetch_history_window_ts(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
    adj: str = "front",
) -> pd.DataFrame:
    """Fetch historical window from TimescaleDB.

    Current implementation:
    - Only supports daily frequency (freq="1d");
    - Supports adj: 'front' (reads from DAILY_QFQ_TABLE), 'none' (reads from DAILY_RAW_TABLE);
    - Returns a MultiIndex(datetime, instrument) DataFrame with columns
      [open, high, low, close, volume, amount];
    - If *fields* is provided, columns are filtered accordingly;
    - If *bars* is provided, the result is trimmed per instrument to the
      latest N bars.
    """

    if freq != "1d":
        raise NotImplementedError("timescaledb_adapter currently only supports freq='1d'")

    if adj not in ("front", "none"):
        raise NotImplementedError(f"timescaledb_adapter does not yet support adj='{adj}'")

    reader = DBReader()

    # Derive date range from start/end/bars
    start_date: Optional[date]
    end_date: Optional[date]

    if end is not None:
        end_date = end.date()
    else:
        end_date = datetime.now().date()

    if bars is not None and bars > 0:
        # 给一个略宽的日期窗口，后续再按每个标的截取最后 bars 条
        window_days = max(bars * 3, bars + 10)
        start_date = end_date - timedelta(days=window_days)
    else:
        start_date = start.date() if start is not None else None

    # 根据 adj 参数决定读取哪个表
    # Phase 2 要求: 除非显式不复权，否则默认尝试关联 $factor 字段
    with_factor = (adj != "none")
    if adj == "none":
        df = reader.load_daily(universe, start_date, end_date, table_name=DAILY_RAW_TABLE, with_factor=False)
    else:
        df = reader.load_daily(universe, start_date, end_date, with_factor=with_factor)

    if df.empty:
        return df

    # Optional column filtering
    if fields is not None and len(fields) > 0:
        keep = [c for c in fields if c in df.columns]
        if keep:
            df = df[keep]

    # If bars is specified, trim per instrument
    if bars is not None and bars > 0:
        df = (
            df.groupby(level="instrument", group_keys=True)
            .tail(bars)
            .sort_index()
        )

    return df


def fetch_fundamental_data_ts(
    universe: List[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """从 PostgreSQL (quant schema) 获取基本面与资金流数据。
    
    返回以 (datetime, instrument) 为 MultiIndex 的 DataFrame。
    """
    from ..db.pg_pool import get_conn
    
    # 优化：如果日期范围过大，强制限制为 end_date 之前的 180 天
    # 避免加载过多历史数据导致超时（1100万行数据处理极其缓慢）
    max_days = 180
    if (end_date - start_date).days > max_days:
        adjusted_start = end_date - timedelta(days=max_days)
        logger.warning(f"fetch_fundamental_data_ts: start_date {start_date} too early, adjusting to {adjusted_start} to avoid timeout")
        start_date = adjusted_start

    # 1. 获取 daily_basic
    basic_sql = """
        SELECT trade_date as datetime, ts_code as instrument,
               turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
               dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv
        FROM market.daily_basic
        WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s
    """
    
    # 2. 获取 money_flow
    flow_sql = """
        SELECT trade_date as datetime, ts_code as instrument,
               buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
               buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
               buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
               buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
               net_mf_vol, net_mf_amount
        FROM market.moneyflow_ts
        WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s
    """
    
    try:
        t0 = time.time()
        with get_conn() as conn:
            t_basic0 = time.time()
            df_basic = pd.read_sql(basic_sql, conn, params=(universe, start_date, end_date))
            t_basic1 = time.time()

            t_flow0 = time.time()
            df_flow = pd.read_sql(flow_sql, conn, params=(universe, start_date, end_date))
            t_flow1 = time.time()

        logger.info(
            "fetch_fundamental_data_ts sql_done"
            f" universe_size={len(universe)} start_date={start_date} end_date={end_date}"
            f" daily_basic_rows={len(df_basic)} daily_basic_sec={t_basic1 - t_basic0:.3f}"
            f" moneyflow_rows={len(df_flow)} moneyflow_sec={t_flow1 - t_flow0:.3f}"
            f" total_sec={time.time() - t0:.3f}"
        )
            
        if df_basic.empty and df_flow.empty:
            return pd.DataFrame()
            
        # 合并并设置索引
        if not df_basic.empty:
            df_basic["datetime"] = pd.to_datetime(df_basic["datetime"])
            df_basic.set_index(["datetime", "instrument"], inplace=True)
            
        if not df_flow.empty:
            df_flow["datetime"] = pd.to_datetime(df_flow["datetime"])
            df_flow.set_index(["datetime", "instrument"], inplace=True)
            
        if not df_basic.empty and not df_flow.empty:
            return pd.concat([df_basic, df_flow], axis=1).sort_index()
        return df_basic if not df_basic.empty else df_flow
        
    except Exception as e:
        logger.warning(f"获取基本面数据失败: {e}")
        return pd.DataFrame()


def fetch_latest_market_dates_ts() -> dict:
    from ..db.pg_pool import get_conn

    sqls = {
        "kline_daily_raw": "SELECT MAX(trade_date) FROM market.kline_daily_raw",
        "daily_basic": "SELECT MAX(trade_date) FROM market.daily_basic",
        "moneyflow_ts": "SELECT MAX(trade_date) FROM market.moneyflow_ts",
    }

    out: dict = {}
    t0 = time.time()
    with get_conn() as conn:
        with conn.cursor() as cur:
            for k, sql in sqls.items():
                q0 = time.time()
                cur.execute(sql)
                row = cur.fetchone()
                q1 = time.time()
                out[k] = row[0].isoformat() if row and row[0] is not None else None
                logger.info(f"fetch_latest_market_dates_ts {k}={out[k]} sec={q1 - q0:.3f}")
    logger.info(f"fetch_latest_market_dates_ts done total_sec={time.time() - t0:.3f}")
    return out
