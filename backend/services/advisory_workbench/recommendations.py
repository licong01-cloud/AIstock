"""Read-only recommendation segments; prices never represent broker fills."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta

from .projection import WorkbenchError, identity, membership
from .quotes import CN, positive, quote_service, symbols_from_query
from .repository import ReadRepository


def segments(program_id, selected_id, versions, items, calendar, symbols):
    versions = sorted((v for v in versions if v['version_status'] == 'PUBLISHED'),
                      key=lambda v: (str(v['trade_date']), v['list_version_id']))
    by_id = {v['list_version_id']: v for v in versions}
    selected = by_id.get(selected_id)
    if not selected or selected['program_id'] != program_id:
        raise WorkbenchError('LIST_IDENTITY_CHANGED', '名单身份不可用，请刷新名单。', 409)
    dates, rows = defaultdict(list), defaultdict(list)
    for v in versions:
        dates[str(v['trade_date'])].append(v)
    for row in items:
        rows[(row['list_version_id'], row['symbol'])].append(row)
    facts = {str(r['cal_date']): r['is_trading'] for r in calendar}

    def adjacent(a, b):
        start, end = date.fromisoformat(str(a['trade_date'])), date.fromisoformat(str(b['trade_date']))
        if start >= end or facts.get(str(start)) is not True or facts.get(str(end)) is not True:
            return False
        day = start + timedelta(days=1)
        while day < end:
            if facts.get(str(day)) is not False:
                return False
            day += timedelta(days=1)
        return len(dates[str(start)]) == len(dates[str(end)]) == 1

    def member(v, symbol):
        found = rows[(v['list_version_id'], symbol)]
        if len(found) > 1 or len(dates[str(v['trade_date'])]) != 1:
            return 'UNKNOWN'
        if not found:
            return 'OUTSIDE_LIST'
        row = found[0]
        if row['program_id'] != program_id or row['binding_version_id'] != v['binding_version_id']:
            return 'UNKNOWN'
        return membership(row)

    result = {}
    for symbol in symbols:
        out = dict(symbol=symbol, segment_id=None, start_date=None, end_date=None,
                   base_price=None, end_price=None, return_bps=None, price_basis='RAW_CNY',
                   state='UNKNOWN', reason_code='CONTINUITY_UNKNOWN')
        result[symbol] = out
        current = selected
        group = member(current, symbol)
        if group == 'UNKNOWN':
            continue
        # A removal row belongs to the segment present in the immediately prior list.
        if group != 'IN_SCOPE':
            prior = by_id.get(current.get('previous_list_version_id'))
            if (not prior or prior['binding_version_id'] != current['binding_version_id']
                    or not adjacent(prior, current) or member(prior, symbol) != 'IN_SCOPE'):
                out.update(state='NOT_RECOMMENDED', reason_code='NOT_RECOMMENDED')
                continue
            current = prior
        first = current
        valid = True
        while True:
            previous_id = first.get('previous_list_version_id')
            if not previous_id:
                valid = not any(str(v['trade_date']) < str(first['trade_date']) for v in versions)
                break
            previous = by_id.get(previous_id)
            if not previous or not adjacent(previous, first):
                valid = False
                break
            if previous['binding_version_id'] != first['binding_version_id']:
                break
            prior_group = member(previous, symbol)
            if prior_group == 'UNKNOWN':
                valid = False
                break
            if prior_group != 'IN_SCOPE':
                break
            first = previous
        if not valid or facts.get(str(first['trade_date'])) is not True:
            continue
        out.update(segment_id=identity((program_id, first['binding_version_id'], first['list_version_id'], symbol)),
                   start_date=str(first['trade_date']), binding_version_id=first['binding_version_id'])
        last = current
        for following in (v for v in versions if str(v['trade_date']) > str(current['trade_date'])):
            if not adjacent(last, following) or following.get('previous_list_version_id') != last['list_version_id']:
                valid = False
                break
            next_group = member(following, symbol)
            if next_group == 'UNKNOWN':
                valid = False
                break
            if following['binding_version_id'] != first['binding_version_id'] or next_group != 'IN_SCOPE':
                out['end_date'] = str(following['trade_date'])
                break
            last = following
        out['known_through_date'] = str(last['trade_date'])
        if valid:
            out.update(state='KNOWN', reason_code=None)
    return result


def value_segment(segment, quote, prices, now):
    out = dict(segment)
    if out['state'] != 'KNOWN':
        return out
    now = now.astimezone(CN)
    today = now.date().isoformat()
    start, end = out['start_date'], out['end_date']
    if start > today or (start == today and now.time() < time(9, 30)):
        out.update(reason_code='OPEN_NOT_YET_AVAILABLE', state='RECOMMENDED')
        return out

    def opening(day):
        row = prices.get((out['symbol'], day)) or {}
        if row.get('suspended'):
            return None, 'SUSPENDED'
        price = positive(row.get('open_price'))
        if price:
            return price, 'LOCAL_RAW_OPEN'
        if quote.get('trade_date') == day and quote.get('kind') in {'LIVE', 'PAUSED', 'CLOSE'}:
            price = positive(quote.get('open_price'))
            if price:
                return price, 'TDX_RAW_OPEN'
        return None, None

    base, source = opening(start)
    ended = bool(end and (end < today or (end == today and now.time() >= time(9, 30))))
    out.update(base_price=base, base_source=source, state='ENDED' if ended else 'RECOMMENDED')
    if ended:
        mark, mark_source = opening(end)
        out.update(end_price=mark, end_source=mark_source, valuation_date=end)
    else:
        mark = positive(quote.get('price')) if (quote.get('trade_date') or '') >= start else None
        out['valuation_date'] = quote.get('trade_date')
    if not base:
        out['reason_code'] = 'BASE_SUSPENDED' if source == 'SUSPENDED' else 'BASE_OPEN_MISSING'
    elif not mark:
        out['reason_code'] = 'END_OPEN_MISSING' if ended else 'QUOTE_NOT_VALID_FOR_SEGMENT'
    else:
        out.update(return_bps=(mark / base - 1) * 10000, reason_code=None)
    return out


class RecommendationReturns:
    def __init__(self, repository=None, quotes=None, clock=None):
        self.repository = repository or ReadRepository()
        self.quotes = quotes or quote_service
        self.clock = clock or (lambda: datetime.now(CN))

    def get(self, program_id, list_version_id, raw):
        symbols = symbols_from_query(raw)
        now = self.clock().astimezone(CN)
        with self.repository.transaction() as repo:
            repo.program(program_id)
            versions = repo.versions(program_id, limit=4097)
            if len(versions) > 4096:
                raise WorkbenchError('RETURN_HISTORY_LIMIT', '名单历史超出单次读取限额。', 422)
            start = min([date.fromisoformat(str(v['trade_date'])) for v in versions] + [now.date()])
            end = max([date.fromisoformat(str(v['trade_date'])) for v in versions] + [now.date()])
            calendar = repo.calendar(start, end)
            items = repo.recommendation_items(program_id, [v['list_version_id'] for v in versions], symbols)
            scopes = segments(program_id, list_version_id, versions, items, calendar, symbols)
            days = sorted({v[k] for v in scopes.values() for k in ('start_date', 'end_date')
                           if v.get(k) and v[k] <= now.date().isoformat()})
            prices = repo.open_prices(symbols, days)
        # No database connection or read transaction is held across TDX calls.
        quotes = self.quotes.get(raw)
        return {**quotes, 'program_id': program_id, 'list_version_id': list_version_id,
                'return_schema_version': 'advisory_recommendation_returns_v1',
                'items': {s: {**q, 'recommendation': value_segment(scopes[s], q, prices, now)}
                          for s, q in quotes['items'].items()}}
