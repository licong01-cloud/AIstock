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
    rows = [dict(episode_id="old", symbol="600001.SH"), dict(episode_id="new", symbol="600001.SH")]
    WorkbenchService._memberships(MembershipRepo(), {"program": {"program_id": "p"}, "selected": {"list_version_id": "v"}}, rows)
    assert [row["selected_membership"] for row in rows] == ["OUTSIDE_LIST", "IN_SCOPE"]
    WorkbenchService._memberships(MembershipRepo(), {"program": {"program_id": "p"}, "selected": None}, rows)
    assert all(row["selected_membership"] == "NO_CURRENT_LIST" for row in rows)
