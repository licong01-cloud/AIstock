"""Tushare adapter for AIstock data service.

This module wraps Tushare (or Tushare Pro) HTTP APIs and exposes
normalized historical windows and, optionally, fundamental data.

NOTE: Users must provide their own Tushare API token via configuration;
this module MUST NOT hard-code any credentials.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

import pandas as pd


def fetch_history_window_tushare(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
    adj: str = "none",
) -> pd.DataFrame:
    """Fetch historical window from Tushare.

    Implementation should:
    - Use the configured Tushare token;
    - Handle paging/quotas and basic retry logic;
    - Normalize field names and index layout.
    """

    raise NotImplementedError
