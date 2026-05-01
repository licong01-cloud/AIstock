"""Shared factor-value cache coverage rules.

The cache's stored date_range is the first/last date with actual factor values.
Rolling-window factors can legitimately start after the requested train_start,
so callers must not treat that leading warm-up gap as a cache miss when the
cache records the requested computation window.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Tuple


DEFAULT_WARMUP_TOLERANCE_DAYS = 60


def _parse_iso_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(str(value), "%Y-%m-%d")


def factor_cache_covers_window(
    *,
    cache_start_date: Optional[str],
    cache_end_date: Optional[str],
    target_start: Optional[str],
    target_end: Optional[str],
    entry: Optional[Mapping[str, Any]] = None,
    max_start_gap_days: int = DEFAULT_WARMUP_TOLERANCE_DAYS,
) -> Tuple[bool, str]:
    """Return whether a factor cache covers the requested window.

    End coverage is always strict.  Start coverage can be satisfied either by:
    - an explicit stored computation window (`window_train_start`), or
    - the actual first factor-value date being no more than
      `max_start_gap_days` after `target_start`.
    """

    if not cache_start_date or not cache_end_date:
        return False, "missing_cache_date_range"

    try:
        cache_start = _parse_iso_date(cache_start_date)
        cache_end = _parse_iso_date(cache_end_date)
        req_start = _parse_iso_date(target_start)
        req_end = _parse_iso_date(target_end)
    except ValueError:
        return False, "invalid_date_format"

    if req_end and cache_end and cache_end < req_end:
        return False, "end_before_target"

    if not req_start or not cache_start:
        return True, "end_covered"

    entry = entry or {}
    window_start_raw = entry.get("window_train_start")
    if window_start_raw:
        try:
            window_start = _parse_iso_date(str(window_start_raw))
        except ValueError:
            return False, "invalid_window_start"
        if window_start and window_start <= req_start:
            return True, "covered_by_recorded_window"

    if cache_start <= req_start:
        return True, "covered_by_date_range"

    gap_days = (cache_start - req_start).days
    if gap_days <= max_start_gap_days:
        return True, "covered_by_warmup_tolerance"

    return False, "start_after_target"
