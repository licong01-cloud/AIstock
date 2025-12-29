"""Download past 3 years of daily K-line data for two symbols via xtquant.

Symbols:
- 688981.SH
- 000688.SZ

Behavior:
- Uses xtquant.xtdata to ensure local history is downloaded (download_history_data).
- Then calls get_market_data with dividend_type='front' (前复权) to fetch
  approximately past 3 years of daily bars.
- Prints basic statistics and the head of each DataFrame so you can verify
  data quality before wiring into strategies.

Usage (from project root):

    python -m backend.scripts.download_xtquant_history_two_symbols

This script is read-only with respect to trading; it only downloads and reads
market data into local xtquant cache (typically under the miniQMT data
directory on the local disk).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List

from xtquant import xtdata  # type: ignore[import]

logger = logging.getLogger(__name__)

SYMBOLS: List[str] = ["688981.SH", "000688.SZ"]
INTRADAY_FREQS: List[str] = ["1m", "5m", "15m", "30m", "60m"]


def _init_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
    )


def _ensure_history_downloaded(symbols: List[str]) -> None:
    """Ensure raw history is downloaded for given symbols (daily + key intraday).

    xtdata.download_history_data 本身不支持指定起止日期，会尽量补齐该标的
    对应周期的历史数据到本地缓存目录；后续通过 get_market_data + start_time/count
    控制实际使用的时间范围。
    """

    for code in symbols:
        # 日线
        try:
            xtdata.download_history_data(code, "1d")
            logger.info("download_history_data OK (1d): %s", code)
        except Exception as exc:  # noqa: BLE001
            logger.warning("download_history_data FAILED (1d) for %s: %s", code, exc)

        # 分钟线：1m/5m/15m/30m/60m
        for freq in INTRADAY_FREQS:
            try:
                xtdata.download_history_data(code, freq)
                logger.info("download_history_data OK (%s): %s", freq, code)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "download_history_data FAILED (%s) for %s: %s", freq, code, exc
                )


def _fetch_front_adjusted_3y(symbol: str):
    """Fetch approximately past 3 years of daily K-line, front-adjusted (前复权)."""
    # 3 年大约 3 * 365 个自然日，这里用起始日期控制范围
    end = datetime.now()
    start = end - timedelta(days=3 * 365)
    start_str = start.strftime("%Y%m%d")

    field_list = ["open", "high", "low", "close", "volume", "amount"]

    logger.info(
        "Fetching 3y daily K for %s from %s (front-adjusted)", symbol, start_str
    )
    data = xtdata.get_market_data(
        field_list=field_list,
        stock_list=[symbol],
        period="1d",
        start_time=start_str,
        end_time="",
        count=-1,
        dividend_type="front",  # 前复权
        fill_data=True,
    )

    if not isinstance(data, dict) or not data:
        logger.warning("get_market_data returned empty/invalid for %s", symbol)
        return None

    # 以 close 字段为基准组装 DataFrame
    close_df = data.get("close")
    if close_df is None or getattr(close_df, "empty", True):
        logger.warning("close field empty for %s", symbol)
        return None

    try:
        row_close = close_df.loc[symbol]
    except Exception as exc:  # noqa: BLE001
        logger.warning("cannot locate %s in close DataFrame: %s", symbol, exc)
        return None

    if row_close.empty:
        logger.warning("close series empty for %s", symbol)
        return None

    # 时间索引是字符串时间戳，截取前 8 位作为日期
    time_index = [str(t) for t in row_close.index]
    dates = [datetime.strptime(t[:8], "%Y%m%d") for t in time_index]

    def _field(name: str):
        df = data.get(name)
        if df is None or getattr(df, "empty", True):
            return [None] * len(dates)
        try:
            row = df.loc[symbol]
        except Exception:
            return [None] * len(dates)
        return [row.get(t, None) for t in row.index]

    import pandas as pd  # local import to avoid polluting module namespace

    df = pd.DataFrame(
        {
            "date": dates,
            "open": _field("open"),
            "high": _field("high"),
            "low": _field("low"),
            "close": list(row_close.values),
            "volume": _field("volume"),
            "amount": _field("amount"),
        }
    )
    df = df.sort_values("date").reset_index(drop=True)
    return df


def _fetch_intraday_last_month(symbol: str, freq: str):
    """Fetch approximately last 1 month of intraday K-line for given freq."""

    end = datetime.now()
    start = end - timedelta(days=30)
    start_str = start.strftime("%Y%m%d")

    field_list = ["open", "high", "low", "close", "volume", "amount"]

    logger.info(
        "Fetching ~1m intraday K (%s) for %s from %s", freq, symbol, start_str
    )
    data = xtdata.get_market_data(
        field_list=field_list,
        stock_list=[symbol],
        period=freq,
        start_time=start_str,
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    )

    if not isinstance(data, dict) or not data:
        logger.warning("get_market_data intraday returned empty/invalid for %s %s", symbol, freq)
        return None

    close_df = data.get("close")
    if close_df is None or getattr(close_df, "empty", True):
        logger.warning("intraday close empty for %s %s", symbol, freq)
        return None

    try:
        row_close = close_df.loc[symbol]
    except Exception as exc:  # noqa: BLE001
        logger.warning("cannot locate %s in intraday close (%s): %s", symbol, freq, exc)
        return None

    if row_close.empty:
        logger.warning("intraday close series empty for %s %s", symbol, freq)
        return None

    # 时间索引为时间戳（或字符串），直接转为 pandas datetime
    time_index = [str(t) for t in row_close.index]
    from pandas import to_datetime  # type: ignore[import]

    times = to_datetime(time_index, errors="coerce")

    def _field(name: str):
        df = data.get(name)
        if df is None or getattr(df, "empty", True):
            return [None] * len(times)
        try:
            row = df.loc[symbol]
        except Exception:
            return [None] * len(times)
        return [row.get(t, None) for t in row.index]

    import pandas as pd  # local import

    df = pd.DataFrame(
        {
            "datetime": times,
            "open": _field("open"),
            "high": _field("high"),
            "low": _field("low"),
            "close": list(row_close.values),
            "volume": _field("volume"),
            "amount": _field("amount"),
        }
    )
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


def main() -> None:
    _init_logging()

    logger.info("Symbols to process: %s", SYMBOLS)
    _ensure_history_downloaded(SYMBOLS)

    for code in SYMBOLS:
        df = _fetch_front_adjusted_3y(code)
        if df is None or df.empty:
            logger.warning("No data for %s after download + fetch", code)
            continue

        logger.info("%s: got %d daily bars (front-adjusted)", code, len(df))
        print("\n===", code, "===")
        print(df.tail(5))

        # 同时输出最近 1 个月的分钟 K 线（1m/5m/15m/30m/60m）
        for freq in INTRADAY_FREQS:
            intraday_df = _fetch_intraday_last_month(code, freq)
            if intraday_df is None or intraday_df.empty:
                logger.warning("No intraday data for %s %s", code, freq)
                continue
            logger.info(
                "%s: got %d intraday bars for freq=%s", code, len(intraday_df), freq
            )
            print(f"\n--- {code} freq={freq} last 5 bars ---")
            print(intraday_df.tail(5))


if __name__ == "__main__":
    main()
