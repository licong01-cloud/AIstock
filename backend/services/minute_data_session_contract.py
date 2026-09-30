"""Reject 13:00 bars; suppress only proven suspended provider placeholders.

Auction bars stay raw provider facts. This guard neither fabricates 11:30
bars nor shifts any timestamps, and does not mutate existing rows.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation

CHINA_TZ = dt.timezone(dt.timedelta(hours=8))
SUSPEND_SQL = """
SELECT EXISTS (
    SELECT 1 FROM market.suspend_d
     WHERE ts_code=%s AND trade_date=%s AND suspend_type='S'
       AND NULLIF(BTRIM(COALESCE(suspend_timing, '')), '') IS NULL
), EXISTS (
    SELECT 1 FROM market.kline_daily_raw
     WHERE ts_code=%s AND trade_date=%s AND adjust_type='none'
       AND volume_hand=0 AND amount_li=0
), EXISTS (
    SELECT 1 FROM market.kline_daily_raw
     WHERE ts_code=%s AND trade_date=%s AND adjust_type='none'
       AND (volume_hand IS NULL OR amount_li IS NULL OR volume_hand<>0 OR amount_li<>0)
)
"""
MINUTE_CONFLICT_SQL = """
SELECT EXISTS (
    SELECT 1 FROM market.kline_minute_raw
     WHERE ts_code=%s AND freq='1m' AND trade_time >= %s AND trade_time < %s
       AND (volume_hand IS NULL OR amount_li IS NULL OR volume_hand<>0 OR amount_li<>0)
)
"""


def _zero(value):
    try:
        number = Decimal(str(value))
        return number.is_finite() and number == 0
    except (InvalidOperation, ValueError):
        return False


def local_timestamp(value):
    parsed = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError("minute timestamp requires explicit timezone")
    return parsed.astimezone(CHINA_TZ)


def suspended_no_trade(conn, symbol, day):
    start = dt.datetime.combine(day, dt.time(), CHINA_TZ)
    with conn.cursor() as cur:
        cur.execute(SUSPEND_SQL, (symbol, day, symbol, day, symbol, day))
        full_day, daily_zero, daily_conflict = cur.fetchone()
        if not full_day or not daily_zero or daily_conflict:
            return False
        cur.execute(MINUTE_CONFLICT_SQL, (symbol, start, start + dt.timedelta(days=1)))
        return not cur.fetchone()[0]


def guard_minute_values(conn, symbol, day, values):
    """Return validated tuples, or [] for an independently proven placeholder."""
    stamps = [local_timestamp(row[0]) for row in values]
    if not any(stamp.time() == dt.time(13) for stamp in stamps):
        return values
    if any(stamp.date() != day for stamp in stamps):
        raise ValueError(f"minute_session_date_mismatch: {symbol} {day}")
    if not all(_zero(row[7]) and _zero(row[8]) for row in values):
        raise ValueError(f"minute_session_invalid_1300_with_turnover: {symbol} {day}")
    if not suspended_no_trade(conn, symbol, day):
        raise ValueError(f"minute_session_invalid_1300_suspension_unproven: {symbol} {day}")
    return []
