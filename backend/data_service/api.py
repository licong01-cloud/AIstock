"""Public data service API for AIstock strategies and engines.

This module provides the unified interfaces described in the
implementation design document. Concrete IO is delegated to the
underlying adapter modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, List, Optional

import logging
import pandas as pd

from . import miniqmt_adapter, timescaledb_adapter, xtquant_adapter, tdx_adapter
from .errors import DataSourceContext, DataSourceError, log_data_source_failure
from .miniqmt_adapter import Order, PortfolioState, Position, Trade


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


def get_history_window(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
) -> pd.DataFrame:
    """Return a historical window as MultiIndex(datetime, instrument).

    NOTE: In strict mode, xtquant is treated as the primary source for
    K-line history. TimescaleDB is only used as a secondary fallback
    when xtquant cannot provide data. If both sources fail or return no
    data, a DataSourceError is raised.
    """
    # Primary path: xtquant
    try:
        df_xt = xtquant_adapter.fetch_history_window_xt(
            universe,
            start=start,
            end=end,
            bars=bars,
            fields=fields,
            freq=freq,
        )
    except Exception as exc:
        ctx_xt = DataSourceContext(
            api="get_history_window",
            source="xtquant",
            universe_size=len(universe),
            freq=freq,
        )
        log_data_source_failure(
            "xtquant history source raised error", context=ctx_xt, exc=exc
        )
        df_xt = pd.DataFrame()

    if df_xt is not None and not df_xt.empty:
        logger.info(
            "history_window_source",
            extra={
                "event": {
                    "api": "get_history_window",
                    "source": "xtquant",
                    "universe_size": len(universe),
                    "freq": freq,
                }
            },
        )
        return df_xt

    # Fallback: TimescaleDB (only when xtquant has no data or fails).
    try:
        df_ts = timescaledb_adapter.fetch_history_window_ts(
            universe,
            start=start,
            end=end,
            bars=bars,
            fields=fields,
            freq=freq,
        )
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
        df_ts = pd.DataFrame()

    if df_ts is not None and not df_ts.empty:
        logger.info(
            "history_window_source",
            extra={
                "event": {
                    "api": "get_history_window",
                    "source": "timescaledb",
                    "universe_size": len(universe),
                    "freq": freq,
                }
            },
        )
        return df_ts

    # Both primary and fallback sources failed or returned no data: in
    # strict mode this is a hard error, not an empty frame.
    ctx = DataSourceContext(
        api="get_history_window",
        source="xtquant+timescaledb",
        universe_size=len(universe),
        freq=freq,
    )
    log_data_source_failure(
        "no history data from either xtquant or TimescaleDB", context=ctx
    )
    raise DataSourceError(
        "get_history_window: no data available from xtquant or TimescaleDB",
        context=ctx,
    )


def get_intraday_window(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1m",
) -> pd.DataFrame:
    """Return an intraday K-line window using xtquant as source.

    - 支持典型周期："1m"、"5m"、"15m"、"30m"、"60m" 等；
    - 数据源为 xtquant 的 ``get_market_data``，通过
      ``xtquant_adapter.fetch_history_window_xt`` 封装；
    - 严格模式下，不做任何插值或合成，若底层无数据则直接抛错。
    """

    allowed_freqs = {"1m", "5m", "15m", "30m", "60m"}
    if freq not in allowed_freqs:
        raise ValueError(f"unsupported intraday freq: {freq}")

    try:
        df_xt = xtquant_adapter.fetch_history_window_xt(
            universe,
            start=start,
            end=end,
            bars=bars,
            fields=fields,
            freq=freq,
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

    return df_xt


def get_xt_trader_and_account():
    """Helper to get the global xtquant trader and account instance.
    
    This is used by miniqmt_adapter and other modules to access the
    active trading session.
    """
    try:
        from xtquant import xttrader
        # Assuming the system has a way to store or retrieve these global instances
        # For now, we'll try to get them from a known global or raise if not initialized
        trader = getattr(xttrader, '_active_trader', None)
        account = getattr(xttrader, '_active_account', None)
        return trader, account
    except ImportError:
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
