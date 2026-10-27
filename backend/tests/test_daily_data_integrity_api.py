from datetime import date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.routers.local_data import get_integrity_service, router
from backend.services.data_integrity.catalog import CN, IntegrityCatalogueUnavailable, obligations
from backend.services.data_integrity.repository import IntegrityRevisionChanged
from backend.services.data_integrity.service import DailyIntegrityService


def test_catalogue_nonempty_ignores_maintenance_and_keeps_disabled_history():
    required, identity = obligations()
    assert len(identity) == 64 and len(required) > 20
    assert not any(r.dataset.startswith("_") for r in required)
    assert next(r for r in required if r.dataset == "suspend_d").category == "event"
    # Past obligation survives disabling an additional formal dataset schedule.
    old = {"dataset": "old_formal_dataset", "category": "daily_series", "ready_time": "19:00"}
    assert "old_formal_dataset" in {r.dataset for r in obligations(historical=[old])[0]}
    with pytest.raises(IntegrityCatalogueUnavailable, match="no formal"):
        obligations([{"dataset": "unknown_enabled_dataset", "enabled": True}])


def test_tplus1_publication_updates_original_fact_date():
    required, _ = obligations()
    margin = next(r for r in required if r.dataset == "margin_detail")
    day = date(2026, 10, 23)
    due = margin.due_at(day, {day: True, date(2026, 10, 24): False, date(2026, 10, 26): True})
    assert due.date() == date(2026, 10, 26)
    assert margin.due_at(day, {day: True}) is None


def test_unknown_day_and_committed_dirty_revision_not_old_green():
    day = date(2026, 10, 23)
    now = datetime(2026, 10, 24, 12, tzinfo=CN)
    unknown = DailyIntegrityService._day(day, {}, [], {}, {}, now)
    assert unknown["state"] == "UNVERIFIED" and not unknown["all_sync_complete"]
    weekend = DailyIntegrityService._day(day, {day: False}, [], {}, {}, now)
    assert weekend["state"] == "NOT_TRADING"
    parts = {}
    for obligation in obligations()[0]:
        parts[(day, obligation.dataset)] = {"data_revision": 1, "validated_revision": 1,
            "validation_status": "VALID", "scope_version": obligation.scope_version,
            "rule_version": obligation.rule_version, "validation_id": "v1"}
    complete = DailyIntegrityService._day(day, {day: True}, [], {}, parts, now)
    assert complete["all_sync_complete"]
    parts[(day, "adj_factor")]["data_revision"] = 2
    dirty = DailyIntegrityService._day(day, {day: True}, [], {}, parts, now)
    assert not dirty["all_sync_complete"] and dirty["watermark"] != complete["watermark"]


def test_readonly_routes_distinguish_bad_request_failure_and_revision_change():
    class Service:
        def day(self, value):
            if value == "bad":
                raise ValueError("invalid date")
            if value == "unavailable":
                raise RuntimeError("private database diagnostics")
            return {"state": "PARTIAL", "all_sync_complete": False}

        def gaps(self, *_a, **_kw):
            raise IntegrityRevisionChanged("restart page")

        def calendar(self, month):
            return {"month": month, "etag": "dirty-aware"}

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_integrity_service] = Service
    client = TestClient(app)
    assert client.get("/api/v1/local-data/integrity/days/2026-10-23").json()["state"] == "PARTIAL"
    assert client.get("/api/v1/local-data/integrity/days/bad").status_code == 400
    response = client.get("/api/v1/local-data/integrity/days/unavailable")
    assert response.status_code == 503 and "private database" not in response.text
    assert client.get("/api/v1/local-data/integrity/days/2026-10-23/gaps?dataset=x").status_code == 409
    assert client.get("/api/v1/local-data/integrity/calendar?month=2026-10").headers["etag"] == '"dirty-aware"'


def test_release_read_failure_never_reports_proven_stale_files(monkeypatch):
    from backend.services.data_integrity import release
    def unavailable():
        raise OSError("unavailable release volume")
    monkeypatch.setattr(release, "active_release_pins", unavailable)
    result = DailyIntegrityService._release_view([{"all_sync_complete": True}])[0]
    assert result["all_sync_complete"] is True
    assert result["release_contains_revision"] is None and result["release_coverage_state"] == "READ_ERROR"
