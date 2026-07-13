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
from .moneyflow_contract import normalize_tushare_moneyflow_units


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
    # ⚠️ 关键修复：强制只保留请求的字段，防止返回所有数据库列
    if fields is not None and len(fields) > 0:
        keep = [c for c in fields if c in df.columns]
        if keep:
            df = df[keep]
        else:
            # 如果请求的字段都不存在，返回空DataFrame
            logger.warning(f"请求的字段都不存在: {fields}, 可用字段: {list(df.columns)}")
    else:
        # 如果没有指定fields，默认只返回OHLCV字段
        default_fields = ['open', 'high', 'low', 'close', 'volume', 'amount']
        keep = [c for c in default_fields if c in df.columns]
        if keep:
            logger.warning(f"未指定fields参数，默认只返回OHLCV字段: {keep}")
            df = df[keep]
    
    # 验证返回的列数
    logger.info(f"fetch_history_window_ts返回列数: {len(df.columns)}, 列名: {list(df.columns)}")

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
               close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
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

    # 3. 获取 bak_basic（历史股票列表数据，bb_*字段）
    # 与 qlib_exporter/db_reader.py load_bak_basic_panel 完全一致的字段和过滤逻辑
    bak_sql = """
        SELECT
            b.trade_date as datetime, b.ts_code as instrument,
            b.pe_dyn, b.total_assets, b.liquid_assets, b.fixed_assets,
            b.reserved, b.reserved_pershare, b.eps, b.bvps,
            b.undp, b.per_undp, b.rev_yoy, b.profit_yoy,
            b.gpr, b.npr, b.holder_num
        FROM market.bak_basic b
        INNER JOIN market.stock_basic s ON b.ts_code = s.ts_code
        WHERE b.ts_code = ANY(%s)
          AND b.trade_date >= %s AND b.trade_date <= %s
          AND s.list_status = 'L'
          AND b.trade_date >= s.list_date + INTERVAL '365 days'
    """

    # 4. 获取 cyq_perf（每日筹码及胜率数据，cp_*字段）
    # 与 qlib_exporter/db_reader.py load_cyq_perf_panel 完全一致的字段和过滤逻辑
    cyq_sql = """
        SELECT
            c.trade_date as datetime, c.ts_code as instrument,
            c.his_low, c.his_high,
            c.cost_5pct, c.cost_15pct, c.cost_50pct, c.cost_85pct, c.cost_95pct,
            c.weight_avg, c.winner_rate
        FROM market.cyq_perf c
        INNER JOIN market.stock_basic s ON c.ts_code = s.ts_code
        WHERE c.ts_code = ANY(%s)
          AND c.trade_date >= %s AND c.trade_date <= %s
          AND s.list_status = 'L'
    """

    # 5. 获取 sector_data（申万L2行业板块数据，sw2_*字段）
    # 列名在数据库中已有 sw2_ 前缀，无需 rename
    # 与 qe_data_service.py SECTOR_DATA_COLUMNS 完全一致的 22 列
    sector_sql = """
        SELECT trade_date as datetime, ts_code as instrument,
               sw2_open, sw2_high, sw2_low, sw2_close, sw2_pct_change,
               sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv,
               sw2_mf_net_amt, sw2_mf_net_vol,
               sw2_mf_buy_elg_amt, sw2_mf_buy_elg_vol,
               sw2_mf_sell_elg_amt, sw2_mf_sell_elg_vol,
               sw2_mf_buy_lg_amt, sw2_mf_sell_lg_amt,
               sw2_mf_buy_md_amt, sw2_mf_sell_md_amt,
               sw2_mf_buy_sm_amt, sw2_mf_sell_sm_amt
        FROM market.sector_data
        WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s
    """

    # bak_basic 字段映射（与 db_reader.py load_bak_basic_panel 一致）
    bak_rename = {
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

    # cyq_perf 字段映射（与 db_reader.py load_cyq_perf_panel 一致）
    cyq_rename = {
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
    
    try:
        t0 = time.time()
        with get_conn() as conn:
            t_basic0 = time.time()
            df_basic = pd.read_sql(basic_sql, conn, params=(universe, start_date, end_date))
            t_basic1 = time.time()

            t_flow0 = time.time()
            df_flow = pd.read_sql(flow_sql, conn, params=(universe, start_date, end_date))
            t_flow1 = time.time()

            t_bak0 = time.time()
            df_bak = pd.read_sql(bak_sql, conn, params=(universe, start_date, end_date))
            t_bak1 = time.time()

            t_cyq0 = time.time()
            df_cyq = pd.read_sql(cyq_sql, conn, params=(universe, start_date, end_date))
            t_cyq1 = time.time()

            t_sec0 = time.time()
            df_sector = pd.read_sql(sector_sql, conn, params=(universe, start_date, end_date))
            t_sec1 = time.time()

        logger.info(
            "fetch_fundamental_data_ts sql_done"
            f" universe_size={len(universe)} start_date={start_date} end_date={end_date}"
            f" daily_basic_rows={len(df_basic)} daily_basic_sec={t_basic1 - t_basic0:.3f}"
            f" moneyflow_rows={len(df_flow)} moneyflow_sec={t_flow1 - t_flow0:.3f}"
            f" bak_basic_rows={len(df_bak)} bak_basic_sec={t_bak1 - t_bak0:.3f}"
            f" cyq_perf_rows={len(df_cyq)} cyq_perf_sec={t_cyq1 - t_cyq0:.3f}"
            f" sector_data_rows={len(df_sector)} sector_data_sec={t_sec1 - t_sec0:.3f}"
            f" total_sec={time.time() - t0:.3f}"
        )

        parts = []

        if not df_basic.empty:
            df_basic["datetime"] = pd.to_datetime(df_basic["datetime"])
            df_basic.set_index(["datetime", "instrument"], inplace=True)
            parts.append(df_basic)

        if not df_flow.empty:
            df_flow = normalize_tushare_moneyflow_units(df_flow, copy=False)
            df_flow["datetime"] = pd.to_datetime(df_flow["datetime"])
            df_flow.set_index(["datetime", "instrument"], inplace=True)
            parts.append(df_flow)

        if not df_bak.empty:
            df_bak["datetime"] = pd.to_datetime(df_bak["datetime"])
            df_bak.set_index(["datetime", "instrument"], inplace=True)
            df_bak = df_bak.rename(columns=bak_rename)
            bb_cols = [c for c in df_bak.columns if c.startswith("bb_")]
            df_bak = df_bak[bb_cols]
            for c in bb_cols:
                df_bak[c] = pd.to_numeric(df_bak[c], errors="coerce").astype("float32")
            parts.append(df_bak)

        if not df_cyq.empty:
            df_cyq["datetime"] = pd.to_datetime(df_cyq["datetime"])
            df_cyq.set_index(["datetime", "instrument"], inplace=True)
            df_cyq = df_cyq.rename(columns=cyq_rename)
            cp_cols = [c for c in df_cyq.columns if c.startswith("cp_")]
            df_cyq = df_cyq[cp_cols]
            for c in cp_cols:
                df_cyq[c] = pd.to_numeric(df_cyq[c], errors="coerce").astype("float32")
            parts.append(df_cyq)

        if not df_sector.empty:
            df_sector["datetime"] = pd.to_datetime(df_sector["datetime"])
            df_sector.set_index(["datetime", "instrument"], inplace=True)
            sw2_cols = [c for c in df_sector.columns if c.startswith("sw2_")]
            df_sector = df_sector[sw2_cols]
            for c in sw2_cols:
                df_sector[c] = pd.to_numeric(df_sector[c], errors="coerce").astype("float32")
            parts.append(df_sector)

        if not parts:
            return pd.DataFrame()

        result = pd.concat(parts, axis=1).sort_index()
        return result
        
    except Exception as e:
        logger.warning(f"获取基本面数据失败: {e}")
        return pd.DataFrame()


def fetch_latest_market_dates_ts() -> dict:
    from ..db.pg_pool import get_conn

    sqls = {
        "kline_daily_raw": "SELECT MAX(trade_date) FROM market.kline_daily_raw",
        "daily_basic": "SELECT MAX(trade_date) FROM market.daily_basic",
        "moneyflow_ts": "SELECT MAX(trade_date) FROM market.moneyflow_ts",
        "sector_data": "SELECT MAX(trade_date) FROM market.sector_data",
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
