"""TDX adapter for AIstock data service.

This module provides read-only access to local TDX/通达信格式数据
(e.g. day/minute files or local TDX servers), exposing them as normalized
historical windows compatible with other adapters.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import os

import pandas as pd
import requests


def fetch_history_window_tdx(
    universe: List[str],
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bars: Optional[int] = None,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
    market: str = "CN",
) -> pd.DataFrame:
    """Fetch historical window from local TDX data.

    Implementation should:
    - Locate local TDX files or query the local TDX server;
    - Handle TDX-specific code prefixes and adjustment factors;
    - Return a MultiIndex(datetime, instrument) DataFrame.
    """

    raise NotImplementedError


TDX_DEFAULT_PORT = os.environ.get("TDX_HTTP_PORT", "19080").strip() or "19080"
TDX_BASE_URL = f"http://localhost:{TDX_DEFAULT_PORT}"


def _to_tdx_codes(universe: List[str]) -> List[str]:
    """Convert instruments like 000001.SZ / 600000.SH to TDX codes.

    TDX expects codes like SZ000001 / SH600000 (see TDX server error
    message "例如:SZ000001"). This mirrors the logic used in the
    verification script and keeps the mapping centralized.
    """

    tdx_codes: List[str] = []
    for inst in universe:
        code, _, market = inst.partition(".")
        market = market.upper()
        if len(code) == 6 and market in {"SZ", "SH", "BJ"}:
            tdx_codes.append(f"{market}{code}")
        else:
            tdx_codes.append(inst)
    return tdx_codes


def fetch_realtime_snapshot_tdx(
    universe: List[str],
    *,
    fields: Optional[List[str]] = None,
    freq: str = "1d",
) -> pd.DataFrame:
    """Fetch realtime snapshot from local TDX HTTP backend.

    This is designed as a *strict* alternative to xtquant realtime
    data, using the verified /api/batch-quote endpoint. It returns a
    DataFrame indexed by instrument, with normalized fields compatible
    with xtquant_adapter.fetch_realtime_snapshot_xt.
    """

    if not universe:
        return pd.DataFrame()

    url = f"{TDX_BASE_URL}/api/batch-quote"
    tdx_codes = _to_tdx_codes(universe)
    resp = requests.post(url, json={"codes": tdx_codes}, timeout=5)
    resp.raise_for_status()
    payload = resp.json()

    # Unified response wrapper: {"code": int, "message": str,
    # "data": [...]}
    if not isinstance(payload, dict):
        raise RuntimeError("unexpected TDX response format: not a JSON object")

    if payload.get("code") != 0:
        msg = payload.get("message") or "TDX batch-quote returned non-zero code"
        raise RuntimeError(str(msg))

    items = payload.get("data") or []
    if not isinstance(items, list):
        raise RuntimeError("unexpected TDX data payload: not a list")

    rows: List[Dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        code = str(item.get("Code") or "").strip()
        exch_raw = item.get("Exchange")
        exch: str
        # Exchange 在协议里是 uint8 枚举: 0=SZ,1=SH,2=BJ
        if isinstance(exch_raw, (int, float)):
            m = {0: "SZ", 1: "SH", 2: "BJ"}
            exch = m.get(int(exch_raw), "")
        else:
            exch = str(exch_raw or "").strip().upper()
        if not code or not exch:
            continue

        instrument = f"{code}.{exch}"

        k = item.get("K") or {}
        if not isinstance(k, dict):
            k = {}

        # Prices in K are scaled (verified via parity tests). Use a
        # fixed factor of 1000 to map back to xtquant-style prices.
        scale = 1000.0
        def _p(name: str) -> Optional[float]:
            v = k.get(name)
            if v is None:
                return None
            try:
                return float(v) / scale
            except Exception:
                return None

        close = _p("Last")
        open_ = _p("Open")
        high = _p("High")
        low = _p("Low")

        volume = item.get("TotalHand")
        amount = item.get("Amount")

        row: Dict[str, object] = {"instrument": instrument}
        if close is not None:
            row["close"] = close
        if open_ is not None:
            row["open"] = open_
        if high is not None:
            row["high"] = high
        if low is not None:
            row["low"] = low
        if volume is not None:
            row["volume"] = float(volume)
        if amount is not None:
            row["amount"] = float(amount)

        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("instrument")

    # If a specific subset of fields is requested, align to that
    # subset, but never fabricate values.
    if fields is not None:
        keep = [f for f in fields if f in df.columns]
        df = df[keep]

    return df
