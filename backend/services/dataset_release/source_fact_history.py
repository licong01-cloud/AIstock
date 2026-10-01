"""Keep observed source history separate from executable PIT eligibility.

Raw daily_basic facts may precede entry (IPO warmup, ST exclusion or re-entry).
Keeping those facts does not admit a security to the executable population.
"""
from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd

SOURCE_HISTORY_CONTRACT = "unique_selected_security_source_window_facts_v1"


def filter_source_fact_history(
    frame: pd.DataFrame, *, codes: Iterable[str], start: date, end: date,
) -> pd.DataFrame:
    if end < start:
        raise ValueError("source history window is inverted")
    if frame.empty:
        return frame.copy()
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["datetime", "instrument"]:
        raise ValueError("source history requires MultiIndex[datetime,instrument]")
    if frame.index.has_duplicates:
        raise ValueError("source history contains duplicate facts")
    dates = pd.DatetimeIndex(frame.index.get_level_values("datetime"))
    if dates.hasnans or dates.tz is not None or not dates.equals(dates.normalize()):
        raise ValueError("source history requires timezone-naive daily fact dates")
    selected = set(codes)
    symbols = frame.index.get_level_values("instrument")
    keep = symbols.isin(selected) & (dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))
    return frame.loc[keep].sort_index().copy()
