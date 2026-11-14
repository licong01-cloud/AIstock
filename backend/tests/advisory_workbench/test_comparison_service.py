from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from threading import Event, Lock

import pytest

from backend.services.advisory_workbench.comparison_cache import ComparisonCache
from backend.services.advisory_workbench.comparison_service import ComparisonService
from backend.services.advisory_workbench.comparison_tracking import tracking_metrics
from backend.services.advisory_workbench.comparison_market import Market
from backend.services.advisory_workbench.projection import WorkbenchError
from backend.services.advisory_workbench.quotes import CN
from backend.tests.advisory_workbench.test_comparison_metrics import build_market, items, version


class Repo:
    def __init__(self):
        self.versions = [version(pid='p'), version(pid='q')]

    @contextmanager
    def transaction(self):
        yield self

    def programs(self, _):
        return [dict(program_id=p, program_name=p) for p in ('p', 'q')]

    def active_bindings(self, ids):
        return [dict(program_id=p, binding_version_id='b') for p in ids]

    def calendar(self, *_):
        return build_market()['calendar']

    def versions_many(self, ids, start, end):
        return [v for v in self.versions if v['program_id'] in ids and str(start) <= v['trade_date'] <= str(end)]

    def items_many(self, ids):
        return [r for v in self.versions if v['list_version_id'] in ids for r in items(v)]

    def rows(self, sql, params):
        assert 'latest.effective_entry_date BETWEEN %s AND %s' in sql and len(params) == 4
        return []


class Reader:
    def load(self, **kwargs):
        assert kwargs['end'] == kwargs['cutoff']
        raw = build_market()
        raw['prices'] = [r for r in raw['prices'] if r['trade_date'] <= kwargs['cutoff']]
        raw['benchmark'] = [r for r in raw['benchmark'] if r['trade_date'] <= kwargs['cutoff']]
        return raw


def service(repo=None):
    return ComparisonService(repo or Repo(), Reader(), ComparisonCache(),
                             lambda: datetime(2026, 10, 12, 16, tzinfo=CN))


ARGS = dict(period='CUSTOM', start='2026-10-01', end='2026-10-05', buy_bps=0, sell_bps=0)


def test_common_dates_original_published_holds_and_lazy_curve_share_identity():
    svc = service()
    result = svc.get(**ARGS)
    assert result['common_count'] == 1
    assert 'series' not in result
    assert all(r['fixed_5td']['net']['avg_return_bps'] == pytest.approx(1000) for r in result['items'])
    assert all(r['coverage']['original_slots'] == 2 for r in result['items'])
    curve = svc.get(series=True, **ARGS)
    assert curve['source_version'] == result['source_version']
    assert curve['series'][0]['nav'][0][1] == 1


@pytest.mark.parametrize('period,start,published', [('5', '2026-10-08', 2), ('10', '2026-10-01', 3)])
def test_short_periods_count_market_sessions_and_keep_unmatured_targets(period, start, published):
    class WeekdayRepo(Repo):
        def calendar(self, *_):
            return [dict(r, is_trading=r['cal_date'].weekday() < 5) for r in build_market(24)['calendar']]

    class WeekdayReader:
        def load(self, **kw):
            raw = build_market(24)
            raw['calendar'] = kw['calendar']
            raw['prices'] = [r for r in raw['prices'] if r['trade_date'] <= kw['cutoff']]
            raw['benchmark'] = [r for r in raw['benchmark'] if r['trade_date'] <= kw['cutoff']]
            return raw

    repo = WeekdayRepo()
    repo.versions = [version(day=day, pid=pid) for day in ('2026-10-01', '2026-10-08', '2026-10-14') for pid in ('p', 'q')]
    svc = ComparisonService(repo, WeekdayReader(), ComparisonCache(),
        lambda: datetime(2026, 10, 14, 16, tzinfo=CN))
    summary = svc.get(period=period)
    assert summary['range'] == dict(start=start, end='2026-10-14', period=period)
    for row in summary['items']:
        assert row['coverage']['published_count'] == published
        assert row['coverage']['mature_count'] == published - 1
        assert row['coverage']['status_counts']['IMMATURE'] == 1
    assert svc.get(period=period, series=True)['source_version'] == summary['source_version']


def test_empty_common_never_ranks_different_periods_and_own_is_explicit():
    repo = Repo()
    repo.versions[1]['trade_date'] = '2026-10-02'
    svc = service(repo)
    result = svc.get(**ARGS)
    assert result['status'] == 'NO_COMPARABLE_SAMPLE'
    assert all(r['fixed_5td']['net']['avg_return_bps'] is None for r in result['items'])
    own = svc.get(**ARGS, sample='OWN')
    assert own['filters']['sample'] == 'OWN' and own['items'][0]['coverage']['compared_count'] == 1


def test_same_day_duplicate_versions_are_conflicts_not_overwritten():
    repo = Repo()
    repo.versions.append({**repo.versions[0], 'list_version_id': 'duplicate'})
    result = service(repo).get(**ARGS, sample='OWN')
    assert result['items'][0]['coverage']['status_counts'] == {'DUPLICATE_PUBLICATION': 2}
    assert result['items'][0]['fixed_5td']['net']['avg_return_bps'] is None
    assert result['items'][0]['portfolio']['status'] == 'UNKNOWN'


def test_selected_end_prevents_maturity_lookahead_and_future_tracking():
    result = service().get(**{**ARGS, 'end': '2026-10-04'})
    assert result['items'][0]['coverage']['status_counts'] == {'IMMATURE': 1}
    assert result['items'][0]['fixed_5td']['net']['sample_count'] == 0
    assert result['range']['end'] == '2026-10-04'
    with pytest.raises(WorkbenchError, match='开始和结束'):
        service().get(period='CUSTOM')


def test_tracking_exit_open_limit_down_is_not_a_fill_or_zero_return():
    raw = build_market()
    raw['prices'][4].update(open_li=8000, down_limit=8)
    row = dict(symbol='A', effective_entry_date='2026-10-01', effective_exit_date='2026-10-05',
               episode_status='EXITED', entry_price_basis='next_open_executable', exit_price_basis='next_open_executable')
    result = tracking_metrics([row], Market(raw), '2026-10-01', '2026-10-05', dict(buy_cost_bps=0, sell_cost_bps=0))
    assert result['gross']['sample_count'] == 0 and result['pending_exit_count'] == 1
    invalid = dict(row, effective_entry_date='2026-10-05', effective_exit_date='2026-10-03')
    result = tracking_metrics([invalid], Market(raw), '2026-10-01', '2026-10-05', dict(buy_cost_bps=0, sell_cost_bps=0))
    assert result['gross']['sample_count'] == 0 and result['floating_gross']['sample_count'] == 0
    assert result['missing_mark_count'] == 1


def test_cache_singleflight_ttl_force_refresh_and_capacity():
    now = [0]
    cache = ComparisonCache(ttl=10, capacity=2, clock=lambda: now[0])
    entered, release, lock = Event(), Event(), Lock()
    count = [0]

    def build():
        with lock:
            count[0] += 1
        entered.set()
        assert release.wait(2)
        return count[0]

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(cache.get, 'x', build) for _ in range(6)]
        assert entered.wait(2)
        release.set()
        assert [f.result() for f in futures] == [1] * 6
    assert cache.get('x', build) == 1
    assert cache.get('x', build, force=True) == 2
    now[0] = 11
    assert cache.get('x', build) == 3
    cache.get('y', lambda: 4)
    cache.get('z', lambda: 5)
    assert len(cache.values) == 2 and 'x' not in cache.values


def test_curve_downsampling_keeps_extrema_and_unknown_boundary():
    rows = [dict(date=str(i), nav=1.0) for i in range(4000)]
    rows[750]['nav'] = 2
    rows[751]['nav'] = .5
    for row in rows[3000:]:
        row['nav'] = None
    result = ComparisonService.downsample(rows)
    assert len(result) <= 800
    assert ['750', 2] in result and ['751', .5] in result and ['3000', None] in result


def test_refresh_observes_price_revision_without_max_date_change():
    class Revised(Reader):
        close = 11000

        def load(self, **kwargs):
            raw = super().load(**kwargs)
            for row in raw['prices']:
                if str(row['trade_date']) == '2026-10-05':
                    row['close_li'] = self.close
            return raw
    reader = Revised()
    svc = service()
    svc.market_reader = reader
    old = svc.get(**ARGS)
    reader.close = 10000
    assert svc.get(**ARGS)['source_version'] == old['source_version']
    new = svc.get(force_refresh=True, **ARGS)
    assert new['source_version'] != old['source_version']
    assert new['items'][0]['fixed_5td']['net']['avg_return_bps'] == 0


def test_tracking_preserves_raw_price_basis_separate_from_fixed_adjusted_basis():
    raw = build_market()
    for row in raw['prices']:
        if str(row['trade_date']) == '2026-10-05':
            row.update(close_li=5000, adj_factor=2, down_limit=4, low_li=4500)
    row = dict(symbol='A', effective_entry_date='2026-10-01', effective_exit_date='2026-10-05',
               episode_status='EXITED', entry_price_basis='next_open_executable', exit_price_basis='next_close')
    result = tracking_metrics([row], Market(raw), '2026-10-01', '2026-10-05', dict(buy_cost_bps=0, sell_cost_bps=0))
    assert result['gross']['avg_return_bps'] == -5000


def test_no_publication_in_selected_source_is_not_zero_return_and_is_not_a_missing_publication():
    repo = Repo()
    repo.versions[0]['source'] = 'REPLAY'
    result = service(repo).get(**ARGS)
    p = result['items'][0]
    assert p['coverage']['source_excluded_date_count'] == 1
    assert p['coverage']['missing_publication_count'] == 4
    assert p['portfolio']['status'] == 'NOT_EVALUATED' and p['portfolio']['total_return_bps'] is None


def test_unknown_publication_source_cannot_hide_duplicate_identity_or_be_cash():
    repo = Repo()
    repo.versions.append(dict(repo.versions[0], list_version_id='unknown', source='UNKNOWN'))
    p = service(repo).get(**ARGS, sample='OWN')['items'][0]
    assert p['coverage']['status_counts'] == {'PUBLICATION_SOURCE_UNKNOWN': 1}
    assert p['fixed_5td']['net']['avg_return_bps'] is None
    assert p['portfolio']['status'] == 'UNKNOWN' and p['portfolio']['total_return_bps'] is None
