"""Supported equity facts, independent of PIT buy eligibility.

AIstock stores SH/SZ A-share facts, including ST/IPO/exit-period quotes. BSE and
B shares are not supported. Index/fund catalogues retain their own contracts.
Unknown/malformed source identities are NOT silently discarded by row filters.
"""
from __future__ import annotations

import re
from datetime import date

from backend.services.market_data.instrument_validator import normalize_ts_code

STOCK_APIS = frozenset({
    'daily', 'weekly', 'daily_basic', 'adj_factor', 'stock_basic', 'stock_st', 'st',
    'bak_basic', 'stk_limit', 'suspend_d', 'dividend', 'moneyflow', 'cyq_perf', 'cyq_chips',
    'forecast_vip', 'express_vip', 'fina_indicator_vip', 'margin_detail', 'index_member_all',
})
_SUPPORTED = re.compile(r'^(?:(?:00|30)\d{4}\.SZ|6\d{5}\.SH)$')


def is_supported_stock_code(code):
    return bool(_SUPPORTED.fullmatch(normalize_ts_code(code)))


def is_excluded_stock_code(code):
    value = normalize_ts_code(code)
    return (value.endswith('.BJ') or value.startswith('BJ')
            or bool(re.fullmatch(r'(?:20\d{4}\.SZ|900\d{3}\.SH)', value)))


def filter_stock_rows(rows):
    return [row for row in rows if str(row.get('exchange', '')).upper() != 'BSE'
            and not is_excluded_stock_code(row.get('ts_code'))]


def filter_dataset_rows(spec, rows):
    return filter_stock_rows(rows) if spec.tushare_api in STOCK_APIS else rows


def stock_exchange_selection(exchanges=None):
    selected = []
    for exchange in exchanges or ('sh', 'sz'):
        value = str(exchange).strip().lower()
        if value in {'bj', 'bse'}:
            continue
        values = ('sh', 'sz') if value == 'all' else (value,)
        for item in values:
            if item not in {'sh', 'sz'}:
                raise ValueError(f'Unsupported stock exchange: {exchange}')
            if item not in selected:
                selected.append(item)
    return selected


def stock_scope_sql(column='ts_code'):
    """Trusted-column SQL fragment, including supported legacy SH/SZ notation."""
    if not re.fullmatch(r'(?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*', column):
        raise ValueError('stock scope requires an SQL column identifier')
    return (f" AND upper(btrim({column})) ~ "
            "'^(00[0-9]{4}[.]SZ|30[0-9]{4}[.]SZ|6[0-9]{5}[.]SH|"
            "SZ(00|30)[0-9]{4}|SH6[0-9]{5})$'")


def dataset_scope_sql(spec, column='ts_code'):
    if spec.tushare_api not in STOCK_APIS:
        return ''
    if spec.tushare_api == 'margin_detail':
        # Financing records also include legitimate ETFs; exclude only the
        # unsupported equity boards, not the separately supported fund scope.
        if not re.fullmatch(r'(?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*', column):
            raise ValueError('stock scope requires an SQL column identifier')
        return f" AND upper(btrim({column})) !~ '([.]BJ$|^BJ|^20[0-9]{{4}}[.]SZ$|^900[0-9]{{3}}[.]SH$)'"
    return stock_scope_sql(column)


DELISTING_PANEL_DATASETS = frozenset({'bak_basic', 'cyq_perf'})
DELISTING_PANEL_POLICY = 'dated_delisting_consolidation_excluded_v1'


def delisting_panel_exclusions(conn, dataset, day, codes):
    """Only dated, published consolidation events waive optional stock panels.

    This is collection applicability, not the buy-eligible PIT universe. No
    current name, guessed 15-day period, ordinary ST, or quote absence is proof.
    A later real listing starts a new lifecycle; it must not inherit the old exit.
    """
    if dataset.removeprefix('market.') not in DELISTING_PANEL_DATASETS or not codes:
        return {}
    codes = sorted({normalize_ts_code(code) for code in codes})
    with conn.cursor() as cur:
        cur.execute("""SELECT e.ts_code,MIN(GREATEST(e.pub_date,e.imp_date))
            FROM market.stock_st_events e
            WHERE e.ts_code=ANY(%s::text[]) AND btrim(e.st_type)=%s
              AND e.pub_date<=%s AND e.imp_date<=%s
              AND NOT EXISTS (SELECT 1 FROM market.stock_basic b
                  WHERE b.ts_code=e.ts_code::bpchar
                    AND b.list_date>e.imp_date AND b.list_date<=%s)
            GROUP BY e.ts_code""", (codes, '退市整理期', day, day, day))
        return {normalize_ts_code(code): boundary for code, boundary in cur.fetchall()}


def filter_delisting_panel_rows(conn, spec, records):
    """Do not insert newly fetched out-of-scope panel rows; never delete history."""
    if spec.name not in DELISTING_PANEL_DATASETS or not records:
        return records
    by_day = {}
    for row in records:
        value = row['trade_date']
        day = value if isinstance(value, date) else date.fromisoformat(str(value))
        by_day.setdefault(day, set()).add(normalize_ts_code(row['ts_code']))
    excluded = {day: delisting_panel_exclusions(conn, spec.name, day, codes)
                for day, codes in by_day.items()}
    return [row for row in records if normalize_ts_code(row['ts_code']) not in excluded[
        row['trade_date'] if isinstance(row['trade_date'], date) else date.fromisoformat(str(row['trade_date']))]]


def wake_delisting_panel_validations(conn, events):
    """A newly stored lifecycle boundary wakes only affected old gap verdicts."""
    codes = sorted({normalize_ts_code(row['ts_code']) for row in events
                    if str(row.get('st_type', '')).strip() == '退市整理期'})
    if not codes:
        return 0
    from .repository import enqueue_validation, schema_installed
    if not schema_installed(conn):
        return 0
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT p.dataset,p.trade_date
            FROM market.dataset_date_refresh_audit p
            JOIN market.stock_st_events e ON e.ts_code=ANY(%s::text[])
              AND btrim(e.st_type)=%s AND e.pub_date<=p.trade_date AND e.imp_date<=p.trade_date
            WHERE p.data_source='canonical_db' AND p.dataset=ANY(%s::text[])
              AND p.validation_status='INCOMPLETE'
              AND p.metadata#>'{validation,gaps}' @> jsonb_build_array(
                  jsonb_build_object('classification','MISSING','symbols',jsonb_build_array(e.ts_code)))
              AND NOT COALESCE(p.metadata#>'{validation,checks,excluded_delisting_symbols}', '[]'::jsonb)
                  @> jsonb_build_array(e.ts_code)
            ORDER BY p.dataset,p.trade_date""", (codes, '退市整理期', sorted(DELISTING_PANEL_DATASETS)))
        dates = cur.fetchall()
    for dataset, day in dates:
        enqueue_validation(conn, dataset, day, source_completed=True)
    return len(dates)
