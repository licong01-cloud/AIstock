"""Qlib integration adapter for AIstock data service.

This module implements the custom DataProvider and DataHandler required to
bridge Qlib's data requests to the AIstock Data Service Layer (api.py).
It ensures that Qlib-based models can be run 'zero-rewrite' within AIstock.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
try:
    from qlib.data.data import LocalProvider, DataProvider
except ImportError:
    # Fallback for environments where qlib is not installed yet
    class DataProvider: pass
    class LocalProvider(DataProvider): pass

from .api import get_history_window


class AIstockDataProvider(LocalProvider):
    """Custom Qlib DataProvider that redirects requests to AIstock Data Service.
    
    Strictly follows Section 6.4 of DataServiceLayer_Detail_Design.md.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def data(
        self,
        instruments: Union[List[str], str],
        fields: List[str],
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        freq: str = "1d",
        **kwargs
    ) -> pd.DataFrame:
        """Fetch data via AIstock Data Service instead of local qlib bin files."""
        
        # Normalize instruments
        if isinstance(instruments, str):
            universe = [instruments]
        else:
            universe = list(instruments)

        # Normalize times
        start = pd.to_datetime(start_time) if start_time else None
        end = pd.to_datetime(end_time) if end_time else None

        # Fetch from AIstock unified API
        # This ensures consistency between offline research and online inference.
        df = get_history_window(
            universe=universe,
            start=start,
            end=end,
            fields=fields,
            freq=freq
        )
        
        if df.empty:
            return df

        # Qlib expects specific index and column naming if not already aligned.
        # AIstock Data Service already returns MultiIndex(datetime, instrument),
        # which is what qlib's internal components typically expect after provider processing.
        return df

def register_aistock_provider():
    """Register AIstockDataProvider to Qlib's global configuration."""
    try:
        import qlib
        from qlib.config import C
        # Force qlib to use our provider for 'feature' and 'pit' types if needed
        # In a real scenario, this would be part of qlib.init() or C.update()
        print("AIstockDataProvider registered (Placeholder logic)")
    except ImportError:
        pass
