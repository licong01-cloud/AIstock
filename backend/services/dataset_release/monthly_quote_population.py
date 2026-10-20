"""Observed monthly quote days are not PIT buy-eligibility intervals."""
from __future__ import annotations

from datetime import date, datetime
import math
from typing import Any, Iterable, Mapping


def observed_quote_days(
    rows: Iterable[Mapping[str, Any]], *, known_codes: frozenset[str],
    trading_days: tuple[date, ...],
) -> tuple[tuple[str, date], ...]:
    """Select exact real daily facts; never extend a listing/catalog endpoint.

    The sealed raw input is already authoritative. This only identifies the
    additional quote population, leaving price/session validation to the shared
    transformer. Zero-turnover halt placeholders do not create quote days.
    """
    calendar = frozenset(trading_days)
    keys = set()
    for row in rows:
        code = str(row["ts_code"]).upper()
        value = row["trade_date"]
        day = value.date() if isinstance(value, datetime) else value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        if code not in known_codes or day not in calendar:
            continue
        prices = [float(row[field]) for field in ("open_li", "high_li", "low_li", "close_li")]
        if all(math.isfinite(value) and value > 0 for value in prices) and (
            float(row.get("volume_hand") or 0) > 0 or float(row.get("amount_li") or 0) > 0
        ):
            keys.add((code, day))
    return tuple(sorted(keys))
