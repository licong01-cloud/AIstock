"""Invalidate dated fact scope when the shared listing directory changes.

No market-data scans or provider requests: reuse the existing partition ledger
and queue. Listing identity allows collection, never fabricated price/factors.
"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .repository import mark_changed, schema_installed
from .stock_scope import is_supported_stock_code


def _day(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value)
    return datetime.strptime(text, '%Y%m%d').date() if len(text) == 8 else date.fromisoformat(text)


def listing_change_window(old, new, *, today):
    old, new = _day(old), _day(new)
    if new is None or old == new:
        return None  # unknown listing date is not an invented scope boundary
    start = min(old, new) if old is not None else new
    end = min(max(old, new) - timedelta(days=1), today) if old is not None else today
    return (start, end) if start <= end else None


def store_listing_refresh(conn, records, write):
    """Persist catalogue plus changed-scope dirtiness in the caller's transaction."""
    supported = {str(row['ts_code']).strip() for row in records
                 if is_supported_stock_code(str(row.get('ts_code') or '').strip())}
    codes = sorted(supported)
    if not codes:
        return write(records)
    owns = conn.autocommit
    if owns:
        conn.autocommit = False
    try:
        installed = schema_installed(conn)
        before = {}
        if installed:
            with conn.cursor() as cur:
                cur.execute('SELECT ts_code,list_date FROM market.stock_basic '
                            'WHERE ts_code=ANY(%s::character(9)[]) FOR UPDATE', (codes,))
                before = {str(code).strip(): day for code, day in cur.fetchall()}
        count = write(records)
        if installed:
            today = datetime.now(ZoneInfo('Asia/Shanghai')).date()
            windows = {window for row in records if str(row['ts_code']).strip() in supported
                       if (window := listing_change_window(before.get(str(row['ts_code']).strip()),
                           row.get('list_date'), today=today)) is not None}
            affected = set()
            if windows:
                with conn.cursor() as cur:
                    cur.execute("""SELECT DISTINCT p.dataset,p.trade_date
                        FROM market.dataset_date_refresh_audit p
                        JOIN unnest(%s::date[],%s::date[]) w(start_day,end_day)
                          ON p.trade_date BETWEEN w.start_day AND w.end_day
                        WHERE p.data_source='canonical_db'
                          AND p.dataset=ANY(%s::text[])""",
                        ([w[0] for w in windows], [w[1] for w in windows],
                         ['kline_daily_raw', 'adj_factor']))
                    affected.update(cur.fetchall())
            for dataset in sorted({dataset for dataset, _day in affected}):
                mark_changed(conn, dataset, [day for name, day in affected if name == dataset],
                             reason='stock_listing_scope_changed', source='stock_basic')
        if owns:
            conn.commit()
        return count
    except BaseException:
        if owns:
            conn.rollback()
        raise
    finally:
        if owns:
            conn.autocommit = True
