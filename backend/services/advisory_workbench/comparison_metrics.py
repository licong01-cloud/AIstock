"""Original-slot fixed 5TD statistics; overlapping cohorts are not a NAV."""
from collections import Counter
from math import ceil, sqrt
from statistics import mean

from backend.services.advisory_historical_range.program_performance import distribution, net_return
from .projection import membership


def list_identity_valid(version, items):
    symbols = [r['symbol'] for r in items]
    return len(symbols) == len(set(symbols)) and all(
        r.get('program_id') == version['program_id'] and r.get('binding_version_id') == version['binding_version_id'] for r in items)


def known_empty_scope(version, items):
    return (all(version.get(k) == 0 for k in ('active_count', 'entered_count', 'held_count', 'waiting_count'))
            and list_identity_valid(version, items) and not any(membership(r) in {'IN_SCOPE', 'UNKNOWN'} for r in items))


def cohort(version, items, market, cutoff, cost):
    start = str(version['trade_date'])
    end = market.maturity(start)
    result = dict(date=start, maturity=end, list_version_id=version['list_version_id'],
                  binding_version_id=version['binding_version_id'], source=version['source'],
                  status='IMMATURE', gross_bps=None, net_bps=None, benchmark_bps=None,
                  excess_bps=None, slots=[], original_slots=0, cash_slots=0, unknown_slots=0)
    slots = [r for r in items if membership(r) == 'IN_SCOPE']
    result['original_slots'] = len(slots)
    unknown_members = sum(membership(r) == 'UNKNOWN' for r in items)
    if unknown_members:
        result.update(status='MEMBERSHIP_UNKNOWN', unknown_slots=unknown_members)
        return result
    if not slots and known_empty_scope(version, items):
        result['status'] = 'NOT_EVALUATED_EMPTY_SCOPE'
        return result
    if not end:
        result['status'] = 'CALENDAR_UNKNOWN'
    elif end > cutoff:
        return result
    else:
        result['status'] = 'UNKNOWN'
    if not slots or not list_identity_valid(version, items):
        result['status'] = 'LIST_IDENTITY_UNKNOWN'
        result['unknown_slots'] = len(slots)
        return result
    if not end:
        result['unknown_slots'] = len(slots)
        return result
    for row in slots:
        entry, state = market.price(row['symbol'], start, 'open', execution=True)
        gross = net = None
        if state.startswith('CASH_'):
            gross = net = 0.0
            result['cash_slots'] += 1
        elif entry:
            exit_price, state = market.price(row['symbol'], end, execution=True)
            if exit_price:
                gross = (exit_price / entry - 1) * 10000
                net = net_return(gross, cost)
        if net is None:
            result['unknown_slots'] += 1
        result['slots'].append(dict(symbol=row['symbol'], state=state, gross_bps=gross, net_bps=net))
    begin, _ = market.price('__HS300__', start, 'open')
    finish, _ = market.price('__HS300__', end)
    if begin and finish:
        result['benchmark_bps'] = (finish / begin - 1) * 10000
    if not result['unknown_slots']:
        result.update(status='EVALUABLE', gross_bps=mean(s['gross_bps'] for s in result['slots']),
                      net_bps=mean(s['net_bps'] for s in result['slots']))
        if result['benchmark_bps'] is not None:
            result['excess_bps'] = result['net_bps'] - result['benchmark_bps']
    return result


def hac_interval(values):
    """Newey-West lag 4, normal descriptive interval, not independent proof."""
    n = len(values)
    if n < 5:
        return None
    avg = mean(values)
    centered = [v - avg for v in values]
    variance = sum(v * v for v in centered) / n
    for lag in range(1, min(4, n - 1) + 1):
        covariance = sum(centered[i] * centered[i - lag] for i in range(lag, n)) / n
        variance += 2 * (1 - lag / 5) * covariance
    se = sqrt(max(0, variance) / n)
    return [avg - 1.96 * se, avg + 1.96 * se]


def summarize(rows):
    net = [r['net_bps'] for r in rows if r['net_bps'] is not None]
    excess = [r['excess_bps'] for r in rows if r['excess_bps'] is not None]
    ordered = sorted(net)
    tail = ordered[:max(1, ceil(len(ordered) * .1))]
    return dict(net=distribution(net), gross=distribution(r['gross_bps'] for r in rows),
                excess=distribution(excess), excess_interval_bps=hac_interval(excess),
                interval_basis='DESCRIPTIVE_NEWEY_WEST_LAG4_NOT_INDEPENDENT_CONFIRMATION',
                worst_bps=min(net) if net else None, tail_10pct_bps=mean(tail) if tail else None,
                status_counts=dict(Counter(r['status'] for r in rows)),
                original_slots=sum(r['original_slots'] for r in rows),
                cash_slots=sum(r['cash_slots'] for r in rows),
                unknown_slots=sum(r['unknown_slots'] for r in rows))
