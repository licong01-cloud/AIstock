"""Supported equity facts, independent of PIT buy eligibility.

AIstock stores SH/SZ A-share facts, including ST/IPO/exit-period quotes. BSE and
B shares are not supported. Index/fund catalogues retain their own contracts.
Unknown/malformed source identities are NOT silently discarded by row filters.
"""
from __future__ import annotations

import re

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
