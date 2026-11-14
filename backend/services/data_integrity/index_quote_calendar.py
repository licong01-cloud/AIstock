"""Dated source-calendar applicability, separate from an index's publisher.

No network or DB writes occur while reading/validating these observations.
The producer captures hk_tradecal once per needed year, not once per index.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
import logging

from .contracts import digest, finite_number

SCHEMA = 'aistock_index_market_session_v1'
MAPPING_VERSION = 'csi_hk_equity_index_calendar_v1'
# Exact identities checked against Tushare index_basic and HKEX's published
# CSI index catalogue. CSI is the publisher, NOT an A-share trading calendar.
# Never extend this to mixed Shanghai/Shenzhen/Hong Kong indices by name/prefix.
HK_EQUITY_INDICES = frozenset({
    '930709.CSI', '930746.CSI', '930792.CSI', '930914.CSI', '930930.CSI',
    '930931.CSI', '930957.CSI', '930965.CSI', '930975.CSI', '931024.CSI',
    '931027.CSI', '931073.CSI', '931074.CSI', '931075.CSI', '931076.CSI',
    '931077.CSI', '931454.CSI', '931573.CSI', '931574.CSI', '931637.CSI',
    'H11100.CSI', 'H11110.CSI', 'H11120.CSI', 'H11123.CSI', 'H11140.CSI',
    'H11143.CSI', 'H11144.CSI', 'H11146.CSI', 'H11152.CSI', 'H11153.CSI',
    'H11164.CSI', 'H30332.CSI', 'H50069.CSI',
})
IDENTITY_SOURCE = 'https://tushare.pro/document/2?doc_id=94'
HKEX_CALENDAR_SOURCE = (
    'https://www.hkex.com.hk/Services/Trading/Securities/Overview/Trading-Calendar?sc_lang=en')


def capture_index_sessions(rows_by_day, query_calendar):
    """Small producer observation in the existing index source receipt.

    Published positive quotes need no additional query. A failed/missing or
    conflicting calendar response is UNKNOWN, never an inferred closed day.
    Calendar failures do not discard already-collected index quote facts.
    """
    needed = sorted(day for day, facts in rows_by_day.items() if any(
        str(row.get('ts_code', '')).strip() in HK_EQUITY_INDICES and row.get('close') is None
        for row in facts))
    by_year = defaultdict(list)
    for day in needed:
        by_year[day.year].append(day)
    observations = {}
    for days in by_year.values():
        start, end = days[0], days[-1]
        captured_at = datetime.now(timezone.utc).isoformat()
        grouped = defaultdict(list)
        problem = None
        unkeyed_rows = 0
        raw = []
        try:
            raw = query_calendar(start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
        except Exception as exc:
            problem = type(exc).__name__
            logging.getLogger(__name__).warning('index HK calendar unavailable %s..%s: %s', start, end, problem)
        if not isinstance(raw, list):
            problem = 'invalid_calendar_response'
            raw = []
        for row in raw:
            if not isinstance(row, dict):
                unkeyed_rows += 1
                continue
            try:
                day = date.fromisoformat(str(row['cal_date']))
            except (KeyError, TypeError, ValueError):
                unkeyed_rows += 1
                continue
            grouped[day].append(row.get('is_open'))
        try:
            response_hash = digest(raw)
            hash_format = 'canonical_json_v1'
        except (ValueError, TypeError):
            # A non-JSON numeric anomaly is evidence of a bad calendar value,
            # not a reason to discard thousands of good index quote records.
            response_hash = digest(repr(raw))
            hash_format = 'invalid_parsed_response_repr_v1'
        for day in days:
            flags = grouped[day]
            flag = flags[0] if len(flags) == 1 else None
            known = (problem is None and type(flag) in {int, str} and str(flag) in {'0', '1'})
            body = {'schema_version': SCHEMA, 'exchange': 'HKEX', 'session_date': str(day),
                'mapping_version': MAPPING_VERSION, 'identity_source': IDENTITY_SOURCE,
                'calendar_source': HKEX_CALENDAR_SOURCE, 'calendar_api': 'tushare.hk_tradecal',
                'requested_start': str(start), 'requested_end': str(end),
                'captured_at': captured_at, 'response_sha256': response_hash,
                'response_hash_format': hash_format,
                'unkeyed_response_rows': unkeyed_rows,
                'status': 'CONFIRMED' if known else 'UNAVAILABLE',
                'is_open': str(flag) == '1' if known else None,
                'reason': None if known else problem or 'missing_or_conflicting_session'}
            observations[day] = {**body, 'session_digest': digest(body)}
    return observations


def closed_placeholder_codes(expected, facts, *, session, trade_date):
    """Only exact known HK identities with an unmodified, matching closed date."""
    if not isinstance(session, dict):
        return set()
    body = {k: v for k, v in session.items() if k != 'session_digest'}
    if (session.get('schema_version') != SCHEMA or session.get('exchange') != 'HKEX'
            or session.get('mapping_version') != MAPPING_VERSION
            or session.get('calendar_api') != 'tushare.hk_tradecal'
            or session.get('session_date') != str(trade_date)
            or session.get('status') != 'CONFIRMED' or session.get('is_open') is not False
            or session.get('session_digest') != digest(body)):
        return set()
    return {code for code in set(expected) & facts.keys() & HK_EQUITY_INDICES
        if all(facts[code].get(field) is None for field in ('close', 'open', 'high', 'low'))
        and all(facts[code].get(field) is None or
                (finite_number(facts[code][field]) and facts[code][field] == 0)
                for field in ('vol', 'amount'))}
