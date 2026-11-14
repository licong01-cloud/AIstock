import pytest

from backend.services.advisory_workbench.comparison_market import Market
from backend.services.advisory_workbench.comparison_portfolio import reference_portfolio
from backend.tests.advisory_workbench.test_comparison_metrics import build_market, items, version

ZERO = dict(buy_cost_bps=0, sell_cost_bps=0)


def simulate(raw=None, days=6, cost=None):
    raw = raw or build_market()
    versions = [version(f'2026-10-{i:02}') for i in range(1, days + 1)]
    return reference_portfolio(versions, {v['list_version_id']: items(v) for v in versions},
                               Market(raw), '2026-10-01', f'2026-10-{days:02}', cost or ZERO)


def test_five_tranches_conserve_cash_and_sale_is_available_only_next_day():
    result = simulate()
    series = result['series']
    assert series[0]['nav'] == pytest.approx(1)
    assert series[0]['cash'] == pytest.approx(.8)
    assert series[4]['nav'] == pytest.approx(1.1)
    assert series[4]['cash'] == pytest.approx(.22)
    assert series[4]['invested'] == pytest.approx(.88)
    assert series[5]['nav'] == pytest.approx(1.02)
    assert all(r['cash'] >= -1e-12 for r in series)
    assert result['max_drawdown_bps'] == pytest.approx((1.02 / 1.1 - 1) * 10000)


def test_fees_are_charged_on_actual_allocations_and_unknown_is_not_cash():
    cost = dict(buy_cost_bps=100, sell_cost_bps=100)
    result = simulate(days=1, cost=cost)
    assert result['final_nav'] == pytest.approx(.8 + .2 / 1.01)
    assert result['cost_drag_bps'] == pytest.approx(.2 / 1.01 * .01 * 10000)
    raw = build_market()
    raw['prices'] = [r for r in raw['prices'] if not (r['symbol'] == 'A' and str(r['trade_date']) == '2026-10-03')]
    result = simulate(raw)
    assert result['unknown_from'] == '2026-10-03'
    assert all(r['nav'] is None for r in result['series'][2:])
    assert result['total_return_bps'] is None and result['max_drawdown_bps'] is None


def test_missing_publication_stays_cash_and_split_has_no_false_loss():
    market = Market(build_market())
    empty = reference_portfolio([], {}, market, '2026-10-01', '2026-10-05', ZERO)
    assert empty['final_nav'] == 1 and empty['diagnostics']['NO_PUBLICATION_CASH'] == 5
    raw = build_market()
    for row in raw['prices']:
        if str(row['trade_date']) == '2026-10-05':
            row.update(open_li=5000, close_li=5000, adj_factor=2, low_li=4500, up_limit=6, down_limit=4)
    assert simulate(raw, days=5)['final_nav'] == pytest.approx(1)


def test_verified_empty_list_stays_cash_but_missing_items_do_not():
    v = dict(version(), active_count=0, entered_count=0, held_count=0, waiting_count=0)
    market = Market(build_market())
    result = reference_portfolio([v], {}, market, '2026-10-01', '2026-10-05', ZERO)
    assert result['final_nav'] == 1 and result['diagnostics']['EMPTY_SCOPE_CASH'] == 1
    result = reference_portfolio([dict(v, waiting_count=20)], {}, market, '2026-10-01', '2026-10-05', ZERO)
    assert result['status'] == 'UNKNOWN' and result['total_return_bps'] is None
