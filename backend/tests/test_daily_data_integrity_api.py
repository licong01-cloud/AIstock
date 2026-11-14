from datetime import date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.routers.local_data import get_integrity_service, router
from backend.routers import ingestion


from backend.services.data_integrity.catalog import CN, IntegrityCatalogueUnavailable, obligations
from backend.services.data_integrity.repository import IntegrityRevisionChanged
from backend.services.data_integrity.service import DailyIntegrityService


def test_schedule_list_reads_job_keys_once_and_preserves_response(monkeypatch):
    queries = []
    row = {"schedule_id": "schedule-a", "dataset": "etf_share_size", "mode": "incremental",
           "enabled": False, "frequency": "daily", "options": {},
           "last_job_summary": {"dataset": "etf_share_size", "inserted_rows": 25,
                                "error": "real failure"}}
    def fetch(query, params=()):
        queries.append(query)
        return [row]
    monkeypatch.setattr(ingestion, "_fetchall", fetch)
    result = ingestion.list_ingestion_schedules()
    assert len(queries) == 1
    query = queries[0]
    assert "job_keys AS MATERIALIZED" in query
    assert "DISTINCT ON (dataset)" in query and "DISTINCT ON (schedule_id)" in query
    assert "VALUES" in query  # choose newest across BOTH identity matches, not coalesce
    assert "WHERE summary->>'schedule_id'" not in query
    assert "status IN" not in query and "interval" not in query.lower()
    assert result["items"][0]["last_inserted_rows"] == 25
    assert result["items"][0]["enabled"] is False


@pytest.mark.skipif(__import__("os").environ.get("AISTOCK_INTEGRITY_DEV_TEST") != "1",
                    reason="explicit existing DEV opt-in")
def test_schedule_list_native_dev_preserves_or_identity_history_and_missing_jobs(monkeypatch):
    import os
    from pathlib import Path
    from uuid import UUID
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor
    from scripts.prepare_canonical_pit_monthly import _load_database_config
    cfg = _load_database_config("dev", Path(os.environ["AISTOCK_INTEGRITY_ENV_FILE"]))
    assert cfg.dbname == "aistock_dev" and cfg.port == 5433 and cfg.host in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(host=cfg.host, port=cfg.port, dbname=cfg.dbname,
                           user=cfg.user, password=cfg.password, options="-c statement_timeout=10000")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE schedule_fixture (LIKE market.ingestion_schedules INCLUDING DEFAULTS) ON COMMIT DROP")
            cur.execute("CREATE TEMP TABLE job_fixture (LIKE market.ingestion_jobs INCLUDING DEFAULTS) ON COMMIT DROP")
            schedule_ids = [str(UUID(int=i)) for i in range(1, 5)]
            for identity, dataset, mode in zip(schedule_ids, ["a", "a", "b", "empty"],
                                              ["init", "incremental", "init", "init"]):
                cur.execute("""INSERT INTO schedule_fixture(schedule_id,dataset,mode,enabled,frequency,options)
                    VALUES(%s,%s,%s,false,'daily','{}')""", (identity, dataset, mode))
            fixtures = [
                (10, {"dataset": "a", "rows": 1}, "2024-01-01", "success"),
                # The newest dataset match must beat an older schedule match.
                (11, {"schedule_id": schedule_ids[0], "rows": 2}, "2024-01-02", "success"),
                (12, {"dataset": "a", "rows": 3}, "2024-01-03", "failed"),
                # A schedule-only match is valid even if its dataset differs.
                (13, {"schedule_id": schedule_ids[2], "dataset": "other", "rows": 4}, "2024-01-04", "failed"),
                (14, {"dataset": "b", "rows": 5}, "2024-01-03", "success"),
            ]
            for key, summary, created, status in fixtures:
                cur.execute("INSERT INTO job_fixture(job_id,job_type,status,summary,created_at) VALUES(%s,'init',%s,%s,%s)",
                            (str(UUID(int=key)), status, Json(summary), created))
        def fetch(query, params=()):
            query = query.replace("market.ingestion_schedules", "pg_temp.schedule_fixture").replace(
                "market.ingestion_jobs", "pg_temp.job_fixture")
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
        monkeypatch.setattr(ingestion, "_fetchall", fetch)
        items = ingestion.list_ingestion_schedules()["items"]
        assert {(r["dataset"], r["mode"]): r.get("last_inserted_rows") for r in items} == {
            ("a", "init"): 3, ("a", "incremental"): 3, ("b", "init"): 4, ("empty", "init"): None}
        assert len(items) == 4 and all(r["enabled"] is False for r in items)
    finally:
        conn.rollback()
        conn.close()


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
