"""TimescaleDB adapter for AIstock data service.

Provides read-only access to historical bars stored in TimescaleDB.
This module should not introduce any new write paths; it only queries
existing AIstock tables.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd

from ..qlib_exporter.db_reader import DBReader


def fetch_history_window_ts(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
) -> pd.DataFrame:
    """Fetch historical window from TimescaleDB.

    Current implementation:
    - Only supports daily frequency (freq="1d");
    - Reuses DBReader.load_daily to read from the Qlib-friendly
      `DAILY_QFQ_TABLE` (前复权日线表);
    - Returns a MultiIndex(datetime, instrument) DataFrame with columns
      [open, high, low, close, volume, amount];
    - If *fields* is provided, columns are filtered accordingly;
    - If *bars* is provided, the result is trimmed per instrument to the
      latest N bars.
    """

    if freq != "1d":
        raise NotImplementedError("timescaledb_adapter currently only supports freq='1d'")

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

    df = reader.load_daily(universe, start_date, end_date)

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
