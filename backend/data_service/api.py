"""Public data service API for AIstock strategies and engines.

This module provides the unified interfaces described in the
implementation design document. Concrete IO is delegated to the
underlying adapter modules.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, List, Optional

import logging
import pandas as pd

from . import miniqmt_adapter, timescaledb_adapter, xtquant_adapter, tdx_adapter
from .errors import DataSourceContext, DataSourceError, log_data_source_failure
from .miniqmt_adapter import Order, PortfolioState, Trade


logger = logging.getLogger("aistock.data_service")


@dataclass
class QuoteBatch:
    timestamp: datetime
    data: pd.DataFrame  # index: instrument, columns: normalized quote fields


def get_realtime_snapshot(
    universe: List[str],
    *,
    fields: Optional[List[str]] = None,
    level: str = "stock",
    freq: str = "1d",
) -> pd.DataFrame:
    """Return a realtime snapshot DataFrame for given universe.

    严格模式要求：
    - 以 xtquant 作为首选实时行情源；
    - 仅当 xtquant 抛错或返回空数据时，才尝试使用 TDX 作为严格
      备选源；
    - 若两个源都不可用或返回空数据，**直接抛出异常**，而不是
      使用任何形式的“近似快照”或模拟数据。
    """

    # Primary: xtquant
    try:
        snap_xt = xtquant_adapter.fetch_realtime_snapshot_xt(
            universe, fields=fields, freq=freq
        )
    except Exception as exc:
        ctx_xt = DataSourceContext(
            api="get_realtime_snapshot",
            source="xtquant",
            universe_size=len(universe),
            freq=freq,
        )
        log_data_source_failure(
            "xtquant realtime source unavailable", context=ctx_xt, exc=exc
        )
        snap_xt = None

    if snap_xt is not None and not snap_xt.empty:
        logger.info(
            "realtime_snapshot_source",
            extra={
                "event": {
                    "api": "get_realtime_snapshot",
                    "source": "xtquant",
                    "universe_size": len(universe),
                    "freq": freq,
                }
            },
        )
        return snap_xt

    # xtquant 空或不可用时，严格尝试 TDX 作为备选源
    try:
        snap_tdx = tdx_adapter.fetch_realtime_snapshot_tdx(
            universe, fields=fields, freq=freq
        )
    except Exception as exc:
        ctx_tdx = DataSourceContext(
            api="get_realtime_snapshot",
            source="tdx",
            universe_size=len(universe),
            freq=freq,
        )
        log_data_source_failure(
            "TDX realtime source unavailable", context=ctx_tdx, exc=exc
        )
        snap_tdx = None

    if snap_tdx is not None and not snap_tdx.empty:
        logger.info(
            "realtime_snapshot_source",
            extra={
                "event": {
                    "api": "get_realtime_snapshot",
                    "source": "tdx",
                    "universe_size": len(universe),
                    "freq": freq,
                }
            },
        )
        return snap_tdx

    # 两个源都失败或返回空：严格模式下视为硬错误
    ctx = DataSourceContext(
        api="get_realtime_snapshot",
        source="xtquant+tdx",
        universe_size=len(universe),
        freq=freq,
    )
    log_data_source_failure(
        "no realtime data from either xtquant or TDX for requested universe",
        context=ctx,
    )
    raise DataSourceError(
        "get_realtime_snapshot: no data available from xtquant or TDX",
        context=ctx,
    )


def stream_quotes(
    universe: List[str],
    *,
    fields: Optional[List[str]] = None,
    level: str = "stock",
    freq: str = "tick",
) -> Iterator[QuoteBatch]:
    """Yield QuoteBatch objects based on underlying xtquant subscriptions."""

    for batch in xtquant_adapter.stream_quotes_xt(
        universe, fields=fields, freq=freq
    ):
        yield QuoteBatch(timestamp=batch.timestamp, data=batch.data)


def _normalize_fields(fields: Optional[List[str]]) -> Optional[List[str]]:
    """将 Qlib 风格的字段名 ($open) 映射回 AIstock 标准字段名 (open)."""
    if fields is None:
        return None
    return [f.lstrip("$") for f in fields]


def _apply_qlib_field_naming(df: pd.DataFrame, requested_fields: Optional[List[str]]) -> pd.DataFrame:
    """若用户请求了 $ 形式的字段，则将结果列也重命名为 $ 形式以保持一致."""
    if requested_fields is None or df.empty:
        return df
    
    mapping = {}
    for f in requested_fields:
        if f.startswith("$"):
            standard_name = f.lstrip("$")
            if standard_name in df.columns:
                mapping[standard_name] = f
    
    if mapping:
        return df.rename(columns=mapping)
    return df


def get_history_window(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
    adj: str = "front",
    nan_policy: str = "dataservice_default",
    strict_fundamental: Optional[bool] = None,
) -> pd.DataFrame:
    """Return a historical window as MultiIndex(datetime, instrument).

    NOTE: In strict mode, xtquant is treated as the primary source for
    K-line history. TimescaleDB is only used as a secondary fallback
    when xtquant cannot provide data. If both sources fail or return no
    data, a DataSourceError is raised.

    Phase 2 (REQ-DATASVC-P2-001) 增强:
    - 支持 Qlib 风格字段名 (如 $open, $close, $volume, $amount, $factor);
    - 自动映射到内部标准字段并返回对应的列名.
    - 支持复权策略 (adj): 'front' (前复权), 'back' (后复权), 'none' (不复权);
    - 支持缺失值处理策略 (nan_policy): 'dataservice_default' (前向填充).
    """
    
    logger.info(
        "get_history_window request"
        f" universe_size={len(universe)} start={start} end={end} bars={bars} freq={freq} adj={adj} fields={fields}"
    )

    # 1. 字段标准化 (处理 $ 前缀)
    standard_fields = _normalize_fields(fields)

    # 修改：优先使用TimescaleDB（已确认更新到最新），确保选股使用最新数据
    # Primary path: TimescaleDB
    df_ts = pd.DataFrame()
    try:
        df_ts = timescaledb_adapter.fetch_history_window_ts(
            universe,
            start=start,
            end=end,
            bars=bars,
            fields=standard_fields,
            freq=freq,
            adj=adj,
        )
        if df_ts is not None and not df_ts.empty:
            try:
                dt_min = df_ts.index.get_level_values("datetime").min() if "datetime" in df_ts.index.names else None
                dt_max = df_ts.index.get_level_values("datetime").max() if "datetime" in df_ts.index.names else None
            except Exception:
                dt_min, dt_max = None, None
            logger.info(f"get_history_window timescaledb hit rows={len(df_ts)} dt_min={dt_min} dt_max={dt_max}")
    except Exception as exc:
        ctx_ts = DataSourceContext(
            api="get_history_window",
            source="timescaledb",
            universe_size=len(universe),
            freq=freq,
        )
        log_data_source_failure(
            "TimescaleDB history source raised error", context=ctx_ts, exc=exc
        )

    # 如果TimescaleDB有数据，直接使用
    if df_ts is not None and not df_ts.empty:
        df_result = _apply_qlib_field_naming(df_ts, fields)
    else:
        # Fallback: xtquant (only when TimescaleDB has no data or fails)
        logger.info("TimescaleDB无数据，尝试使用xtquant作为备选数据源")
        try:
            df_xt = xtquant_adapter.fetch_history_window_xt(
                universe,
                start=start,
                end=end,
                bars=bars,
                fields=standard_fields,
                freq=freq,
                adj=adj,
            )
            if df_xt is not None and not df_xt.empty:
                try:
                    dt_min = df_xt.index.get_level_values("datetime").min() if "datetime" in df_xt.index.names else None
                    dt_max = df_xt.index.get_level_values("datetime").max() if "datetime" in df_xt.index.names else None
                except Exception:
                    dt_min, dt_max = None, None
                logger.info(f"get_history_window xtquant hit rows={len(df_xt)} dt_min={dt_min} dt_max={dt_max}")
                df_result = _apply_qlib_field_naming(df_xt, fields)
            else:
                # Both sources failed or returned no data: in strict mode this is a hard error
                ctx = DataSourceContext(
                    api="get_history_window",
                    source="xtquant+timescaledb",
                    universe_size=len(universe),
                    freq=freq,
                )
                log_data_source_failure("no history data from either xtquant or TimescaleDB", context=ctx)
                raise DataSourceError("get_history_window: no data available from xtquant or TimescaleDB", context=ctx)
        except Exception as exc:
            if isinstance(exc, DataSourceError):
                raise
            ctx_ts = DataSourceContext(
                api="get_history_window",
                source="timescaledb",
                universe_size=len(universe),
                freq=freq,
            )
            log_data_source_failure("TimescaleDB history source raised error", context=ctx_ts, exc=exc)
            raise DataSourceError(f"get_history_window: TimescaleDB error: {exc}", context=ctx_ts) from exc

    # 2. 注入基本面与资金流数据 (REQ-DATASVC-P3-005)
    strict_fund = bool(strict_fundamental) if strict_fundamental is not None else (
        os.getenv("AISTOCK_STRICT_FUNDAMENTAL", "").strip().lower() in {"1", "true", "yes", "y", "on"}
    )
    need_fund = False
    if fields is None:
        need_fund = True
    else:
        # Normalize possible qlib-style names
        req = set(str(x).lstrip("$") for x in (fields or []) if isinstance(x, str))
        ohlcv = {"open", "high", "low", "close", "volume", "amount", "factor"}
        need_fund = bool(req - ohlcv)

    logger.info(f"get_history_window fundamental need_fund={need_fund} strict_fund={strict_fund}")

    if need_fund:
        try:
            df_fund = timescaledb_adapter.fetch_fundamental_data_ts(
                universe=universe,
                start_date=(start or datetime(2000, 1, 1)).date(),
                end_date=(end or datetime.now()).date(),
            )
            if not df_fund.empty:
                # 2026-01-05 Fix: 确保 fundamental 数据的索引类型与行情一致 (datetime64[ns])
                df_result = df_result.join(df_fund, how="left")
                try:
                    dt_min = df_fund.index.get_level_values("datetime").min() if "datetime" in df_fund.index.names else None
                    dt_max = df_fund.index.get_level_values("datetime").max() if "datetime" in df_fund.index.names else None
                except Exception:
                    dt_min, dt_max = None, None
                logger.info(
                    "get_history_window fundamental joined"
                    f" rows={len(df_fund)} cols={len(df_fund.columns)} dt_min={dt_min} dt_max={dt_max}"
                )
            else:
                logger.warning(
                    "get_history_window fundamental empty"
                    f" universe_size={len(universe)} start={start} end={end} strict_fund={strict_fund}"
                )
                if strict_fund:
                    raise DataSourceError(
                        "get_history_window: fundamental join required but returned empty",
                        context=DataSourceContext(
                            api="get_history_window",
                            source="timescaledb_fundamental",
                            universe_size=len(universe),
                            freq=freq,
                        ),
                    )
        except Exception as e:
            if isinstance(e, DataSourceError):
                raise
            if strict_fund:
                raise DataSourceError(
                    f"get_history_window: fundamental join failed: {e}",
                    context=DataSourceContext(
                        api="get_history_window",
                        source="timescaledb_fundamental",
                        universe_size=len(universe),
                        freq=freq,
                    ),
                ) from e
            logger.warning(f"Join fundamental data failed: {e}")

    # 3. 处理复权因子 (If 'factor' or '$factor' was requested)
    if fields is None or any(f.endswith("factor") for f in fields):
        try:
            from .adj_factor_provider import AdjFactorProvider
            provider = AdjFactorProvider()
            adj_df = provider.get_adj_factor(universe, start or datetime(2000, 1, 1), end or datetime.now())
            if not adj_df.empty:
                adj_df = provider.calculate_qfq_factor(adj_df)
                temp = df_result.reset_index()
                temp["trade_date"] = pd.to_datetime(temp["datetime"]).dt.normalize()
                adj_df["trade_date"] = pd.to_datetime(adj_df["trade_date"]).dt.normalize()
                
                merged = temp.merge(adj_df[["ts_code", "trade_date", "qfq_factor"]], 
                                    left_on=["instrument", "trade_date"], 
                                    right_on=["ts_code", "trade_date"], 
                                    how="left")
                
                df_result["factor"] = merged.set_index(["datetime", "instrument"])["qfq_factor"].fillna(1.0)
                if fields and "$factor" in fields:
                    df_result = df_result.rename(columns={"factor": "$factor"})
            else:
                df_result["factor"] = 1.0
        except Exception as e:
            logger.warning(f"Failed to fetch adj_factor: {e}")
            df_result["factor"] = 1.0

    return _apply_nan_policy(df_result, nan_policy)


def _apply_nan_policy(df: pd.DataFrame, policy: str) -> pd.DataFrame:
    """根据策略处理 DataFrame 中的缺失值."""
    if df.empty or policy == "none":
        return df
    
    if policy == "dataservice_default":
        # 默认策略: 按 instrument 进行前向填充 (ffill)
        # 适用于行情数据，保留中间缺失但延续上一个有效值
        return df.groupby(level="instrument", group_keys=False).apply(lambda x: x.ffill())
    
    return df


def get_intraday_window(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1m",
    adj: str = "none",
) -> pd.DataFrame:
    """Return an intraday K-line window using xtquant as source.

    - 支持典型周期："1m"、"5m"、"15m"、"30m"、"60m" 等；
    - 数据源为 xtquant 的 ``get_market_data``，通过
      ``xtquant_adapter.fetch_history_window_xt`` 封装；
    - 严格模式下，不做任何插值或合成，若底层无数据则直接抛错。

    Phase 2 (REQ-DATASVC-P2-001) 增强:
    - 支持 Qlib 风格字段名 (如 $open, $close 等).
    - 支持复权策略 (adj): 默认为 'none'，可选 'front' / 'back'.
    """

    allowed_freqs = {"1m", "5m", "15m", "30m", "60m"}
    if freq not in allowed_freqs:
        raise ValueError(f"unsupported intraday freq: {freq}")

    logger.info(
        "get_intraday_window request"
        f" universe_size={len(universe)} start={start} end={end} bars={bars} freq={freq} adj={adj} fields={fields}"
    )

    # 1. 字段标准化
    standard_fields = _normalize_fields(fields)

    try:
        df_xt = xtquant_adapter.fetch_history_window_xt(
            universe,
            start=start,
            end=end,
            bars=bars,
            fields=standard_fields,
            freq=freq,
            adj=adj,
        )
    except Exception as exc:
        ctx_xt = DataSourceContext(
            api="get_intraday_window",
            source="xtquant",
            universe_size=len(universe),
            freq=freq,
        )
        log_data_source_failure(
            "xtquant intraday source raised error", context=ctx_xt, exc=exc
        )
        raise DataSourceError(
            "get_intraday_window: xtquant intraday source unavailable",
            context=ctx_xt,
        ) from exc

    if df_xt is None or df_xt.empty:
        ctx = DataSourceContext(
            api="get_intraday_window",
            source="xtquant",
            universe_size=len(universe),
            freq=freq,
        )
        log_data_source_failure(
            "xtquant returned empty intraday window for requested universe",
            context=ctx,
        )
        raise DataSourceError(
            "get_intraday_window: xtquant returned no intraday data for the requested universe",
            context=ctx,
        )

    logger.info(
        "intraday_window_source",
        extra={
            "event": {
                "api": "get_intraday_window",
                "source": "xtquant",
                "universe_size": len(universe),
                "freq": freq,
            }
        },
    )

    return _apply_qlib_field_naming(df_xt, fields)


def get_xt_trader_and_account():
    """获取全局 QMT 交易客户端与账户实例。
    
    对接 infra.qmt_client 中的单例实现，确保 DSL 与交易系统共享同一个连接。
    """
    try:
        from ..infra.qmt_client import get_qmt_client_singleton, XtQuantQMTClient
        client = get_qmt_client_singleton()
        if isinstance(client, XtQuantQMTClient):
            return client._trader, client._account
        return None, None
    except Exception as e:
        logger.warning(f"获取 QMT 交易实例失败: {e}")
        return None, None


def get_trading_calendar() -> List[datetime]:
    """Return the trading calendar (trading days) from xtquant."""
    try:
        from xtquant import xtdata
        # xtdata.get_trading_calendar returns a list of strings 'YYYYMMDD'
        calendar_str = xtdata.get_trading_calendar("SH")
        return [datetime.strptime(d, "%Y%m%d") for d in calendar_str]
    except Exception as exc:
        ctx = DataSourceContext(api="get_trading_calendar", source="xtdata")
        log_data_source_failure("Failed to fetch trading calendar", context=ctx, exc=exc)
        raise DataSourceError("get_trading_calendar failed", context=ctx)


def get_portfolio_state() -> PortfolioState:
    """Return current portfolio state via miniQMT adapter."""

    return miniqmt_adapter.load_portfolio_state_qmt()


def get_open_orders() -> List[Order]:
    """Return currently open orders via miniQMT adapter."""

    return miniqmt_adapter.load_open_orders_qmt()


def get_trades(
    *, start: Optional[datetime] = None, end: Optional[datetime] = None
) -> List[Trade]:
    """Return trades within an optional time range via miniQMT adapter."""

    return miniqmt_adapter.load_trades_qmt(start=start, end=end)
