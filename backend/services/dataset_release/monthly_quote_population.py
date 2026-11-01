"""Observed daily facts and minute quote days are not PIT buy intervals."""
from __future__ import annotations

from datetime import date, datetime
from collections.abc import Iterator
import math
import re
from typing import Any, Iterable, Mapping

from .pit import FrozenPitSnapshot


def observed_quote_days(
    rows: Iterable[Mapping[str, Any]], *, known_codes: frozenset[str] | None = None,
    trading_days: tuple[date, ...],
) -> tuple[tuple[str, date], ...]:
    """Select exact real daily facts; never extend a listing/catalog endpoint.

    The sealed raw input is already authoritative. This only identifies the
    additional quote population, leaving price/session validation to the shared
    transformer. Zero-turnover halt placeholders do not create quote days.
    """
    return _observed_days(rows, known_codes=known_codes, trading_days=trading_days, require_turnover=True)


def observed_daily_fact_days(
    rows: Iterable[Mapping[str, Any]], *, known_codes: frozenset[str] | None = None,
    trading_days: tuple[date, ...],
) -> tuple[tuple[str, date], ...]:
    """Keep real daily source keys, including existing zero-turnover rows.

    This only joins auxiliary facts for daily export; it neither asserts a
    suspension nor creates minute quotes, synthetic prices or buy eligibility.
    """
    return _observed_days(rows, known_codes=known_codes, trading_days=trading_days, require_turnover=False)


def _observed_days(
    rows: Iterable[Mapping[str, Any]], *, known_codes: frozenset[str] | None,
    trading_days: tuple[date, ...], require_turnover: bool,
) -> tuple[tuple[str, date], ...]:
    calendar = frozenset(trading_days)
    keys = set()
    for row in rows:
        code = str(row["ts_code"]).upper()
        value = row["trade_date"]
        day = value.date() if isinstance(value, datetime) else value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        if (re.fullmatch(r"\d{6}\.(SH|SZ)", code) is None
                or (known_codes is not None and code not in known_codes) or day not in calendar):
            continue
        prices = [float(row[field]) for field in ("open_li", "high_li", "low_li", "close_li")]
        if all(math.isfinite(value) and value > 0 for value in prices) and (not require_turnover or
            float(row.get("volume_hand") or 0) > 0 or float(row.get("amount_li") or 0) > 0
        ):
            keys.add((code, day))
    return tuple(sorted(keys))


def filter_minute_support_rows(
    rows: Iterable[Mapping[str, Any]], *, pit_snapshot: FrozenPitSnapshot,
    quote_days: tuple[tuple[str, date], ...],
) -> Iterator[Mapping[str, Any]]:
    """Join daily/suspension support to the same required days as limit rows.

    Auxiliary daily facts can exist when there is no minute quotation. Do not
    feed those unmatched rows to the ordered minute join, extend PIT or create
    bars. Keep ordering and same-day S/R rows; downstream validation is intact.
    """
    spans: dict[str, list[tuple[date, date]]] = {}
    for span in pit_snapshot.spans:
        spans.setdefault(span.ts_code, []).append((span.eligible_start, span.eligible_end))
    quotes = frozenset(quote_days)
    for row in rows:
        value = row["trade_date"]
        day = value.date() if isinstance(value, datetime) else value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        code = str(row["ts_code"]).upper()
        if (code, day) in quotes or any(start <= day <= end for start, end in spans.get(code, ())):
            yield row
