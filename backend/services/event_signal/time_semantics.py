"""Point-in-time availability rules for unified event signals.

The module is intentionally pure: callers pass the trading calendar in, and no
database or trading consumer is touched here.
"""

from __future__ import annotations

import datetime as dt
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Sequence


TimeMode = Literal["backtest", "paper", "live", "observed"]

CHINA_TZ = dt.timezone(dt.timedelta(hours=8), name="Asia/Shanghai")
DEFAULT_PRE_OPEN_CUTOFF = dt.time(9, 25)
DEFAULT_MARKET_CLOSE_CUTOFF = dt.time(15, 0)

EXACT = "EXACT"
DATE_ONLY = "DATE_ONLY"
MIDNIGHT_DEFAULT = "MIDNIGHT_DEFAULT"
MISSING = "MISSING"
OBSERVED = "OBSERVED"
LOCAL_FIRST_SEEN = "LOCAL_FIRST_SEEN"


@dataclass(frozen=True)
class EventTimeInput:
    """Input contract for one event-time decision."""

    source_event_date: dt.date
    trading_days: Sequence[dt.date]
    time_mode: TimeMode | str = "backtest"
    source_publish_time: Optional[dt.datetime] = None
    first_seen_at: Optional[dt.datetime] = None
    observed_at: Optional[dt.datetime] = None
    pre_open_cutoff: dt.time = DEFAULT_PRE_OPEN_CUTOFF
    market_close_cutoff: dt.time = DEFAULT_MARKET_CLOSE_CUTOFF
    midnight_is_default: bool = True


@dataclass(frozen=True)
class EventTimeResult:
    """Resolved point-in-time fields persisted to event facts/signals."""

    source_event_date: dt.date
    source_available_at: Optional[dt.datetime]
    available_at: Optional[dt.datetime]
    effective_trade_date: dt.date
    time_mode: str
    source_time_quality: str
    effective_rule: str
    trace: dict[str, Any] = field(default_factory=dict)


def normalize_time_mode(time_mode: TimeMode | str) -> str:
    mode = (time_mode or "backtest").lower()
    if mode not in {"backtest", "paper", "live", "observed"}:
        raise ValueError(f"unsupported event time_mode: {time_mode!r}")
    return mode


def normalize_trading_days(trading_days: Sequence[dt.date]) -> list[dt.date]:
    days = sorted(set(trading_days))
    if not days:
        raise ValueError("trading calendar is empty")
    return days


def next_trading_day(trading_days: Sequence[dt.date], base_date: dt.date, *, strictly_after: bool) -> dt.date:
    """Return the first trading day on/after or strictly after base_date."""

    days = normalize_trading_days(trading_days)
    idx = bisect_right(days, base_date) if strictly_after else bisect_left(days, base_date)
    if idx >= len(days):
        relation = "after" if strictly_after else "on or after"
        raise ValueError(f"trading calendar has no effective date {relation} {base_date.isoformat()}")
    return days[idx]


def to_local_time(value: dt.datetime, *, local_tz: dt.tzinfo = CHINA_TZ) -> dt.datetime:
    """Normalize source or observation timestamps to the AIstock local timezone."""

    if value.tzinfo is None:
        return value.replace(tzinfo=local_tz)
    return value.astimezone(local_tz)


def _is_midnight(value: dt.datetime) -> bool:
    local_time = value.timetz().replace(tzinfo=None)
    return local_time == dt.time(0, 0)


def _effective_from_timestamp(
    *,
    timestamp: dt.datetime,
    trading_days: Sequence[dt.date],
    pre_open_cutoff: dt.time,
    rule_prefix: str,
) -> tuple[dt.date, str]:
    local_date = timestamp.date()
    local_time = timestamp.timetz().replace(tzinfo=None)
    if local_time <= pre_open_cutoff:
        effective = next_trading_day(trading_days, local_date, strictly_after=False)
        if effective == local_date:
            return effective, f"{rule_prefix}_before_preopen"
        return effective, f"{rule_prefix}_before_preopen_next_trading_day"
    return (
        next_trading_day(trading_days, local_date, strictly_after=True),
        f"{rule_prefix}_after_preopen_next_trading_day",
    )


def _date_only_result(event_input: EventTimeInput, *, quality: str, rule: str) -> EventTimeResult:
    mode = normalize_time_mode(event_input.time_mode)
    return EventTimeResult(
        source_event_date=event_input.source_event_date,
        source_available_at=None,
        available_at=None,
        effective_trade_date=next_trading_day(event_input.trading_days, event_input.source_event_date, strictly_after=True),
        time_mode=mode,
        source_time_quality=quality,
        effective_rule=rule,
        trace={
            "time_mode": mode,
            "source_event_date": event_input.source_event_date.isoformat(),
            "date_only_conservative": True,
        },
    )


def _observed_timestamp_for_mode(event_input: EventTimeInput, mode: str) -> tuple[Optional[dt.datetime], Optional[str], str]:
    if mode in {"paper", "live"}:
        if event_input.first_seen_at is not None:
            return event_input.first_seen_at, LOCAL_FIRST_SEEN, "local_first_seen"
        if event_input.observed_at is not None:
            return event_input.observed_at, OBSERVED, "observed_at"
        return None, None, ""
    if mode == "observed":
        if event_input.observed_at is not None:
            return event_input.observed_at, OBSERVED, "observed_at"
        if event_input.first_seen_at is not None:
            return event_input.first_seen_at, LOCAL_FIRST_SEEN, "local_first_seen"
    return None, None, ""


def compute_event_time(
    source_event_date: dt.date,
    trading_days: Sequence[dt.date],
    *,
    time_mode: TimeMode | str = "backtest",
    source_publish_time: Optional[dt.datetime] = None,
    first_seen_at: Optional[dt.datetime] = None,
    observed_at: Optional[dt.datetime] = None,
    pre_open_cutoff: dt.time = DEFAULT_PRE_OPEN_CUTOFF,
    market_close_cutoff: dt.time = DEFAULT_MARKET_CLOSE_CUTOFF,
    midnight_is_default: bool = True,
) -> EventTimeResult:
    """Compute leakage-safe availability fields for an event.

    Backtests ignore local observation timestamps because they are only known
    after AIstock actually fetched the data. Paper/live may use the first local
    observation timestamp because it is a real runtime availability boundary.
    """

    if source_event_date is None:
        raise ValueError("source_event_date is required to resolve effective_trade_date")

    event_input = EventTimeInput(
        source_event_date=source_event_date,
        trading_days=normalize_trading_days(trading_days),
        time_mode=time_mode,
        source_publish_time=source_publish_time,
        first_seen_at=first_seen_at,
        observed_at=observed_at,
        pre_open_cutoff=pre_open_cutoff,
        market_close_cutoff=market_close_cutoff,
        midnight_is_default=midnight_is_default,
    )
    mode = normalize_time_mode(event_input.time_mode)

    if mode == "backtest" and event_input.source_publish_time is not None:
        publish_time = to_local_time(event_input.source_publish_time)
        if event_input.midnight_is_default and _is_midnight(publish_time):
            return EventTimeResult(
                source_event_date=event_input.source_event_date,
                source_available_at=publish_time,
                available_at=publish_time,
                effective_trade_date=next_trading_day(
                    event_input.trading_days,
                    event_input.source_event_date,
                    strictly_after=True,
                ),
                time_mode=mode,
                source_time_quality=MIDNIGHT_DEFAULT,
                effective_rule="midnight_default_next_trading_day",
                trace={
                    "time_mode": mode,
                    "source_event_date": event_input.source_event_date.isoformat(),
                    "source_publish_time": publish_time.isoformat(),
                    "midnight_is_default": True,
                },
            )
        effective, rule = _effective_from_timestamp(
            timestamp=publish_time,
            trading_days=event_input.trading_days,
            pre_open_cutoff=event_input.pre_open_cutoff,
            rule_prefix="exact_publish_time",
        )
        return EventTimeResult(
            source_event_date=event_input.source_event_date,
            source_available_at=publish_time,
            available_at=publish_time,
            effective_trade_date=effective,
            time_mode=mode,
            source_time_quality=EXACT,
            effective_rule=rule,
            trace={
                "time_mode": mode,
                "source_event_date": event_input.source_event_date.isoformat(),
                "source_publish_time": publish_time.isoformat(),
            },
        )

    observed_time, quality, rule_prefix = _observed_timestamp_for_mode(event_input, mode)
    if observed_time is not None and quality is not None:
        local_observed = to_local_time(observed_time)
        effective, rule = _effective_from_timestamp(
            timestamp=local_observed,
            trading_days=event_input.trading_days,
            pre_open_cutoff=event_input.pre_open_cutoff,
            rule_prefix=rule_prefix,
        )
        return EventTimeResult(
            source_event_date=event_input.source_event_date,
            source_available_at=None,
            available_at=local_observed,
            effective_trade_date=effective,
            time_mode=mode,
            source_time_quality=quality,
            effective_rule=rule,
            trace={
                "time_mode": mode,
                "source_event_date": event_input.source_event_date.isoformat(),
                "observation_field": rule_prefix,
                "observed_local_time": local_observed.isoformat(),
            },
        )

    return _date_only_result(event_input, quality=DATE_ONLY, rule="tushare_date_only_next_trading_day")
