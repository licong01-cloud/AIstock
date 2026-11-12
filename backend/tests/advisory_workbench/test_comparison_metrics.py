from datetime import date, timedelta

import pytest

from backend.services.advisory_workbench.comparison_market import Market
from backend.services.advisory_workbench.comparison_metrics import cohort, hac_interval, summarize
from backend.services.advisory_workbench.projection import WorkbenchError

COST = dict(buy_cost_bps=.95, sell_cost_bps=5.95)


def build_market(count=12):
    first = date(2026, 10, 1)
    days = [first + timedelta(days=i) for i in range(count)]
    return dict(calendar=[dict(cal_date=d, is_trading=True) for d in days], suspended=set(),
        prices=[dict(symbol=s, trade_date=d, open_li=10000, close_li=11000 if i == 4 else 10000,
                     high_li=12000, low_li=9000, adj_factor=1, up_limit=12, down_limit=8)
                for s in ('A', 'B') for i, d in enumerate(days)],
        benchmark=[dict(trade_date=d, open=100, close=102) for d in days])


def version(day='2026-10-01', pid='p'):
    return dict(list_version_id=pid + day, program_id=pid, binding_version_id='b', trade_date=day, source='FORWARD')


def items(v=None):
    v = v or version()
    return [dict(symbol=s, program_id=v['program_id'], binding_version_id='b',
                 list_version_id=v['list_version_id'], action='HOLD', item_state='ACTIVE') for s in ('A', 'B')]


def test_original_slots_cash_costs_benchmark_and_maturity():
    raw = build_market()
    raw['suspended'].add(('A', date(2026, 10, 1)))
    row = cohort(version(), items(), Market(raw), '2026-10-05', COST)
    assert row['gross_bps'] == pytest.approx(500)
    expected = ((1.1 * (1 - .000595) / (1 + .000095) - 1) * 10000) / 2
    assert row['net_bps'] == pytest.approx(expected)
    assert row['excess_bps'] == pytest.approx(expected - 200)
    assert row['cash_slots'] == 1 and row['unknown_slots'] == 0
    assert cohort(version(), items(), Market(raw), '2026-10-04', COST)['status'] == 'IMMATURE'
    raw = build_market()
    raw['prices'][0].update(open_li=12000, low_li=12000, adj_factor=None)
    blocked = cohort(version(), items(), Market(raw), '2026-10-05', COST)
    assert blocked['cash_slots'] == 1 and blocked['net_bps'] == pytest.approx(expected)


@pytest.mark.parametrize('failure', ['price', 'adjustment', 'limit', 'exit_limit', 'duplicate_symbol', 'binding', 'duplicate_outside', 'outside_binding'])
def test_unknown_cannot_shrink_denominator_or_turn_into_cash(failure):
    raw, selected = build_market(), items()
    if failure == 'price':
        raw['prices'] = [r for r in raw['prices'] if not (r['symbol'] == 'A' and str(r['trade_date']) == '2026-10-05')]
    if failure == 'adjustment':
        raw['prices'][0]['adj_factor'] = None
    if failure == 'limit':
        raw['prices'][0]['up_limit'] = None
    if failure == 'exit_limit':
        raw['prices'][4]['down_limit'] = 11
    if failure == 'duplicate_symbol':
        selected.append(selected[0])
    if failure == 'binding':
        selected[0]['binding_version_id'] = 'foreign'
    if failure in {'duplicate_outside', 'outside_binding'}:
        selected.append(dict(selected[0], action='WATCH', reason_code='OUTSIDE_ENTRY_THRESHOLD',
                             symbol='A' if failure == 'duplicate_outside' else 'C',
                             binding_version_id='foreign' if failure == 'outside_binding' else 'b'))
    row = cohort(version(), selected, Market(raw), '2026-10-05', COST)
    assert row['net_bps'] is None and row['unknown_slots'] > 0


def test_calendar_gap_and_duplicate_quote_fail_closed():
    unknown_calendar = build_market()
    unknown_calendar['calendar'][2]['is_trading'] = None
    assert not Market(unknown_calendar).covers('2026-10-01', '2026-10-05')
    raw = build_market()
    raw['calendar'].pop(2)
    assert cohort(version(), items(), Market(raw), '2026-10-12', COST)['status'] == 'CALENDAR_UNKNOWN'
    raw['prices'].append(raw['prices'][0])
    with pytest.raises(WorkbenchError, match='重复身份'):
        Market(raw)


def test_corporate_action_uses_factor_ratio_not_raw_price_change():
    raw = build_market()
    for row in raw['prices']:
        if str(row['trade_date']) == '2026-10-05':
            row.update(close_li=5000, adj_factor=2, low_li=4500, down_limit=4)
    row = cohort(version(), items(), Market(raw), '2026-10-05', dict(buy_cost_bps=0, sell_cost_bps=0))
    assert row['net_bps'] == pytest.approx(0)


def test_tail_hac_and_missing_values_are_explicit():
    rows = [dict(status='EVALUABLE', original_slots=10, cash_slots=0, unknown_slots=0,
                 net_bps=v, gross_bps=v, excess_bps=v - 10) for v in [-100, 0, 100, 200, 300]]
    rows.append(dict(status='UNKNOWN', original_slots=10, cash_slots=0, unknown_slots=1,
                     net_bps=None, gross_bps=None, excess_bps=None))
    stats = summarize(rows)
    assert stats['net']['avg_return_bps'] == 100 and stats['net']['sample_count'] == 5
    assert stats['worst_bps'] == -100 and stats['tail_10pct_bps'] == -100
    assert stats['original_slots'] == 60 and stats['unknown_slots'] == 1
    assert stats['excess_interval_bps'][0] < 90 < stats['excess_interval_bps'][1]
    assert hac_interval([1, 2, 3, 4]) is None


def test_unknown_membership_cannot_be_dropped_from_native_population():
    selected = items() + [dict(symbol='C', action='UNRECOGNIZED', item_state='WATCH')]
    row = cohort(version(), selected, Market(build_market()), '2026-10-05', COST)
    assert row['status'] == 'MEMBERSHIP_UNKNOWN' and row['net_bps'] is None


def test_verified_empty_scope_is_legal_na_not_a_win_or_unknown_price():
    v = dict(version(), active_count=0, entered_count=0, held_count=0, waiting_count=0)
    row = cohort(v, [], Market(build_market()), '2026-10-05', COST)
    assert row['status'] == 'NOT_EVALUATED_EMPTY_SCOPE' and row['net_bps'] is None
    assert row['unknown_slots'] == 0
    row = cohort(dict(v, waiting_count=20), [], Market(build_market()), '2026-10-05', COST)
    assert row['status'] == 'LIST_IDENTITY_UNKNOWN'
