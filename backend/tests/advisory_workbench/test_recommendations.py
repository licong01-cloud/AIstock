from contextlib import contextmanager
from datetime import date, datetime

import pytest

from backend.services.advisory_workbench.recommendations import RecommendationReturns, segments, value_segment
from backend.services.advisory_workbench.quotes import CN
from backend.services.advisory_workbench.projection import WorkbenchError

S = '600001.SH'
CAL = [dict(cal_date=date(2026, 10, d), is_trading=True) for d in range(26, 31)]


def version(d, prior=None, binding='b'):
    return dict(program_id='p', list_version_id=f'v{d}', trade_date=date(2026, 10, d),
                previous_list_version_id=f'v{prior}' if prior else None,
                binding_version_id=binding, version_status='PUBLISHED')


def item(d, binding='b', **kw):
    return dict(program_id='p', list_version_id=f'v{d}', binding_version_id=binding,
                symbol=S, action='WATCH', reason_code='PENDING_TARGET_OPEN_ENTRY', **kw)


def project(selected=27, versions=None, items=None, calendar=None):
    return segments('p', f'v{selected}', versions or [version(26), version(27, 26), version(28, 27), version(29, 28)],
                    items if items is not None else [item(26), item(27), item(29)], CAL if calendar is None else calendar, [S])[S]


def quote(day=29, **kw):
    return dict(symbol=S, price=12, trade_date=f'2026-10-{day}', kind='LIVE', open_price=11, **kw)


def now(day=29, hour=10):
    return datetime(2026, 10, day, hour, tzinfo=CN)


def test_continuing_removal_reentry_and_identity_are_separate():
    old = project()
    removed = project(28)
    new = project(29)
    assert old['start_date'] == '2026-10-26' and old['end_date'] == '2026-10-28'
    assert old['segment_id'] == removed['segment_id'] != new['segment_id']
    assert new['start_date'] == '2026-10-29' and new['end_date'] is None
    assert project(28, items=[item(29)])['state'] == 'NOT_RECOMMENDED'


def test_new_binding_closes_old_segment_and_starts_new_base():
    versions = [version(26), version(27, 26, 'other')]
    items = [item(26), item(27, 'other')]
    assert project(26, versions, items)['end_date'] == '2026-10-27'
    assert project(27, versions, items)['start_date'] == '2026-10-27'


@pytest.mark.parametrize('case', ['missing_link', 'gap', 'calendar', 'duplicate_symbol', 'duplicate_version', 'wrong_binding', 'unknown_state'])
def test_unknown_continuity_is_never_guessed(case):
    versions = [version(26), version(27, 26)]
    items = [item(26), item(27)]
    calendar = CAL
    if case == 'missing_link':
        versions[1]['previous_list_version_id'] = 'missing'
    if case == 'gap':
        versions[1] = version(28, 26)
        items[1] = item(28)
    if case == 'calendar':
        calendar = CAL[1:]
    if case == 'duplicate_symbol':
        items.append(item(26))
    if case == 'duplicate_version':
        versions.append({**versions[0], 'list_version_id': 'v26-duplicate'})
    if case == 'wrong_binding':
        items[0]['binding_version_id'] = 'foreign'
    if case == 'unknown_state':
        items[0]['action'] = 'UNKNOWN'
    result = project(28 if case == 'gap' else 27, versions, items, calendar)
    assert result['state'] == 'UNKNOWN' and result['return_bps'] is None


def test_missing_future_chain_does_not_keep_claiming_unended_return():
    result = project(26, [version(26), version(28, 26)], [item(26), item(28)])
    assert result['reason_code'] == 'CONTINUITY_UNKNOWN'


def test_weekend_continuity_requires_every_calendar_date():
    versions = [version(23), version(26, 23)]
    cal = [dict(cal_date=date(2026, 10, d), is_trading=d in {23, 26}) for d in range(23, 27)]
    assert project(26, versions, [item(23), item(26)], cal)['start_date'] == '2026-10-23'
    assert project(26, versions, [item(23), item(26)], cal[1:])['state'] == 'UNKNOWN'


def test_same_day_recommendation_without_episode_uses_tdx_open():
    result = value_segment(project(29), quote(), {}, now())
    assert result['base_price'] == 11 and result['base_source'] == 'TDX_RAW_OPEN'
    assert result['return_bps'] == pytest.approx((12 / 11 - 1) * 10000)


def test_ended_segment_is_frozen_despite_changed_current_price():
    prices = {(S, '2026-10-26'): dict(open_price=10), (S, '2026-10-28'): dict(open_price=10.5)}
    first = value_segment(project(), quote(), prices, now())
    second = value_segment(project(), {**quote(), 'price': 100}, prices, now())
    assert first['state'] == 'ENDED' and first['return_bps'] == pytest.approx(500)
    assert first == second
    del prices[(S, '2026-10-28')]
    assert value_segment(project(), quote(), prices, now())['reason_code'] == 'END_OPEN_MISSING'


def test_future_removal_does_not_use_future_open():
    prices = {(S, '2026-10-26'): dict(open_price=10), (S, '2026-10-28'): dict(open_price=100)}
    out = value_segment(project(), quote(27), prices, now(27))
    assert out['state'] == 'RECOMMENDED' and out['return_bps'] == pytest.approx(2000)


def test_preopen_missing_suspended_and_old_quote_are_not_zero():
    scope = project(29)
    assert value_segment(scope, quote(), {}, now(hour=9))['reason_code'] == 'OPEN_NOT_YET_AVAILABLE'
    assert value_segment(scope, {**quote(), 'kind': 'NO_TRADES', 'trade_date': None}, {}, now())['return_bps'] is None
    assert value_segment(scope, quote(), {(S, '2026-10-29'): dict(open_price=10, suspended=True)}, now())['reason_code'] == 'BASE_SUSPENDED'
    assert value_segment(scope, quote(28), {(S, '2026-10-29'): dict(open_price=10)}, now())['reason_code'] == 'QUOTE_NOT_VALID_FOR_SEGMENT'


def test_list_identity_is_mandatory_and_program_scoped():
    with pytest.raises(WorkbenchError, match='名单身份'):
        segments('wrong', 'v26', [version(26)], [item(26)], CAL, [S])
    with pytest.raises(WorkbenchError):
        segments('p', 'not-found', [version(26)], [item(26)], CAL, [S])


def test_batched_database_snapshot_ends_before_quote_request():
    class Repo:
        active = False

        @contextmanager
        def transaction(self):
            self.active = True
            try:
                yield self
            finally:
                self.active = False

        def program(self, p):
            assert p == 'p'

        def versions(self, p, limit):
            assert limit == 4097
            return [version(29)]

        def calendar(self, *_):
            return CAL

        def recommendation_items(self, p, ids, symbols):
            assert ids == ['v29'] and symbols == [S]
            return [item(29)]

        def open_prices(self, symbols, days):
            assert self.active and symbols == [S] and days == ['2026-10-29']
            return {}

    repo = Repo()

    class Quotes:
        def get(self, raw):
            assert not repo.active and raw == S
            return dict(schema_version='advisory_display_quotes_v1', items={S: quote()})

    out = RecommendationReturns(repo, Quotes(), now).get('p', 'v29', S)
    assert out['items'][S]['recommendation']['return_bps'] is not None
