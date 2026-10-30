from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from backend.services.advisory_workbench.service import WorkbenchService
from backend.services.advisory_workbench.projection import WorkbenchError


class Repo:
    @contextmanager
    def transaction(self):
        yield self

    def program(self, _id):
        return {"program_id": _id, "status": "ENABLED", "program_name": "test"}

    def versions(self, *_args):
        return []

    def bindings(self, *_args):
        return []

    def calendar(self, *_args):
        from datetime import date
        return [{"cal_date": date(2026, 10, 28), "is_trading": True}]

    def watermark(self, *_args):
        return {"source": "v1"}


def service():
    return WorkbenchService(Repo(), lambda: datetime(2026, 10, 28, 12, tzinfo=ZoneInfo("Asia/Shanghai")))


def test_absent_today_list_does_not_load_old_list_or_model():
    result = service().daily("p")
    assert result["status"] == "NO_CURRENT_LIST"
    assert result["items"] == [] and result["summary"] is None
    assert result["target_date"] == "2026-10-28"


def test_stale_paged_request_fails_before_rows_are_served():
    with pytest.raises(WorkbenchError) as error:
        service().daily("p", snapshot_id="0" * 64)
    assert error.value.status == 409


def test_current_binding_without_formal_identity_does_not_include_old_bindings():
    filters = WorkbenchService._filters({"active": None}, {"binding": "CURRENT"})
    assert filters["binding"] == "__NO_ACTIVE_BINDING__"
    with pytest.raises(WorkbenchError):
        WorkbenchService._filters({"active": None}, {"start": "2026-10-28", "end": "2026-10-12"})


def test_membership_is_attached_to_episode_not_reused_for_same_stock():
    class MembershipRepo:
        def items(self, *_args):
            return [dict(episode_id="new", symbol="600001.SH", action="HOLD", item_state="ACTIVE")]
        def entry_links(self, *_args):
            return []
    rows = [dict(episode_id="old", symbol="600001.SH"), dict(episode_id="new", symbol="600001.SH")]
    WorkbenchService._memberships(MembershipRepo(), {"program": {"program_id": "p"}, "selected": {"list_version_id": "v"}}, rows)
    assert [row["selected_membership"] for row in rows] == ["OUTSIDE_LIST", "IN_SCOPE"]
    WorkbenchService._memberships(MembershipRepo(), {"program": {"program_id": "p"}, "selected": None}, rows)
    assert all(row["selected_membership"] == "NO_CURRENT_LIST" for row in rows)


def test_later_enter_membership_does_not_transfer_to_old_same_symbol_episode():
    class MembershipRepo:
        def items(self, *_args):
            return [dict(episode_id=None, symbol='600001.SH', action='WATCH', reason_code='PENDING_TARGET_OPEN_ENTRY')]
        def entry_links(self, *_args):
            return [dict(symbol='600001.SH', episode_id='new')]
    rows = [dict(episode_id=e, program_id='p', symbol='600001.SH', binding_version_id='b') for e in ['old', 'new']]
    ctx = dict(program={'program_id': 'p'}, selected={'list_version_id': 'v', 'binding_version_id': 'b'})
    WorkbenchService._memberships(MembershipRepo(), ctx, rows)
    assert [r['selected_membership'] for r in rows] == ['OUTSIDE_LIST', 'IN_SCOPE']
    rows[1]['binding_version_id'] = 'foreign'
    WorkbenchService._memberships(MembershipRepo(), ctx, rows)
    assert rows[1]['selected_membership'] == 'OUTSIDE_LIST'


@pytest.mark.parametrize('fault', [None, 'multiple', 'symbol', 'binding', 'program', 'absent'])
def test_daily_resolves_only_unique_exact_later_enter(monkeypatch, fault):
    from datetime import date
    from backend.services.advisory_program import AdvisoryProgramService
    v = dict(list_version_id='v', binding_version_id='b', trade_date=date(2026, 10, 28),
             previous_list_version_id=None, version_status='PUBLISHED')

    class LinkedRepo(Repo):
        def items(self, p, ids):
            return [dict(list_version_id='v', symbol='600001.SH', binding_version_id='b',
                         action='WATCH', reason_code='PENDING_TARGET_OPEN_ENTRY', episode_id=None)]

        def marks(self, *_):
            return {}

        def entry_links(self, p, version, symbols):
            assert p == 'p' and version == v and symbols == ['600001.SH']
            return [dict(symbol='600001.SH', episode_id=e) for e in (['e', 'other'] if fault == 'multiple' else ['e'])]

        def episodes(self, p, filters):
            assert filters == {'episode_ids': ['e']}
            return [], 0

    svc = WorkbenchService(LinkedRepo())
    ctx = dict(program={'program_id': 'p'}, selected=v, active=None, versions=[v],
               calendar=[dict(cal_date=date(2026, 10, 28), is_trading=True)],
               target='2026-10-28', snapshot='x', now=datetime(2026, 10, 29))
    monkeypatch.setattr(svc, '_context', lambda *_: ctx)
    view = dict(episode_id='e', program_id='wrong' if fault == 'program' else 'p',
                symbol='600002.SH' if fault == 'symbol' else '600001.SH',
                binding_version_id='other' if fault == 'binding' else 'b', progress='TRACKING')
    monkeypatch.setattr(svc, '_metric_rows', lambda *_: ([] if fault == 'absent' else [view], {}))
    monkeypatch.setattr(AdvisoryProgramService, '_enrich_display_names', lambda _, rows: rows)
    row = svc.daily('p')['items'][0]
    if fault is None:
        assert row['episode_id'] == 'e' and row['progress'] == 'TRACKING'
    else:
        assert row['episode_id'] is None and 'episode' not in row
        assert row['episode_link_state'] == ('AMBIGUOUS_ENTER' if fault == 'multiple' else 'EPISODE_IDENTITY_MISMATCH')
