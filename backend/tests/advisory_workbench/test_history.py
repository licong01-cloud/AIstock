from contextlib import contextmanager
from datetime import date, datetime

import pytest

from backend.services.advisory_workbench.history import HistoryService
from backend.services.advisory_workbench.projection import WorkbenchError
from backend.services.advisory_workbench.quotes import CN
from backend.services.advisory_workbench.recommendations import segments, value_segment


class Repo:
    def __init__(self):
        self.v = dict(program_id='p', list_version_id='replay-2', binding_version_id='b',
                      trade_date=date(2026, 10, 27), version_status='REPLAY', source='REPLAY',
                      previous_list_version_id='replay-1')

    @contextmanager
    def transaction(self):
        yield self

    def program(self, pid):
        assert pid == 'p'

    def version(self, pid, vid):
        assert pid == 'p'
        if vid == 'replay-2':
            return self.v
        if vid == 'replay-1':
            return dict(self.v, list_version_id=vid, trade_date=date(2026, 10, 26), previous_list_version_id=None)
        raise WorkbenchError('HISTORY_VERSION_NOT_FOUND', 'missing', 404)

    def items(self, pid, ids):
        return [dict(program_id=pid, list_version_id=vid, binding_version_id='b', symbol='A',
                     action='HOLD', item_state='ACTIVE', rank=1) for vid in ids]

    def calendar(self, *_):
        return [dict(cal_date=date(2026, 10, i), is_trading=True) for i in (26, 27)]

    def history(self, pid, **filters):
        assert filters['offset'] == 20 and filters['limit'] == 20 and filters['source'] == 'ALL'
        return [self.v], 43

    def entry_links(self, *args):
        return []


def test_all_saved_history_metadata_pagination_and_exact_replay_detail():
    svc = HistoryService(Repo())
    assert svc.versions('p', offset=20)['total_count'] == 43
    detail = svc.detail('p', 'replay-2')
    assert detail['list_version_id'] == 'replay-2' and detail['status'] == 'REPLAY'
    assert detail['summary']['continuing'] == 1
    with pytest.raises(WorkbenchError, match='更新'):
        svc.detail('p', 'replay-2', snapshot_id='wrong')


def test_replay_segment_is_isolated_and_stale_publication_does_not_extend_returns():
    repo = Repo()
    versions = [repo.version('p', 'replay-1'), repo.v]
    rows = repo.items('p', ['replay-1', 'replay-2'])
    scope = segments('p', 'replay-2', versions, rows, repo.calendar(), ['A'])['A']
    assert scope['start_date'] == '2026-10-26' and scope['known_through_date'] == '2026-10-27'
    assert scope['consecutive_days'] == 2
    result = value_segment(scope, dict(price=100, trade_date='2026-11-09'),
                           {('A', '2026-10-26'): dict(open_price=10)}, datetime(2026, 11, 9, 16, tzinfo=CN))
    assert result['state'] == 'UNKNOWN' and result['reason_code'] == 'CONTINUITY_UNKNOWN'
    assert result['return_bps'] is None


@pytest.mark.parametrize('binding', ['b', 'wrong'])
def test_page_specific_enter_links_require_program_security_and_binding_identity(binding):
    class Linked(Repo):
        def entry_links(self, program, version, symbols):
            assert symbols == ['A'] and version['list_version_id'] == 'replay-2'
            return [dict(symbol='A', episode_id='e')]

        def episodes(self, pid, filters):
            assert filters == {'episode_ids': ['e']}
            return [dict(episode_id='e', program_id=pid, symbol='A', origin_binding_id=binding)], 1
    row = HistoryService(Linked()).detail('p', 'replay-2')['items'][0]
    assert row.get('episode_id') == ('e' if binding == 'b' else None)


def test_cross_binding_history_item_is_unknown_membership():
    class Wrong(Repo):
        def items(self, pid, ids):
            return [dict(r, binding_version_id='wrong') for r in super().items(pid, ids)]
    result = HistoryService(Wrong()).detail('p', 'replay-2', group='UNKNOWN')
    assert result['summary']['in_scope'] == 0 and result['summary']['unknown'] == 1
    assert result['items'][0]['membership'] == 'UNKNOWN'
