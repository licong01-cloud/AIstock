import datetime as dt
import uuid
from dataclasses import replace

import backend.services.tushare_sync_engine as sync_engine
import backend.services.stock_universe_pit_service as pit_service
from backend.services.tushare_dataset_specs import SUSPEND_D
from backend.services.tushare_sync_engine import TushareSyncEngine


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))


class _FakeConn:
    def __init__(self):
        self.autocommit = True
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_sync_by_date_replaces_suspend_d_date_even_when_tushare_returns_empty(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    trade_date = dt.date(2026, 4, 28)
    upserted = []

    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: [])
    monkeypatch.setattr(engine, "_upsert_batch", lambda conn, spec, rows: upserted.append(rows) or 0)
    monkeypatch.setattr(engine, "_update_progress", lambda conn, job_id, result: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(conn, SUSPEND_D, trade_date, trade_date, uuid.uuid4())

    assert result.failed_batches == 0
    assert conn.executed[0] == ("DELETE FROM market.suspend_d WHERE trade_date = %s", (trade_date,))
    assert any("dataset_date_refresh_audit" in sql for sql, _params in conn.executed)
    assert upserted == [[]]
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.autocommit is True


def test_sync_by_date_uses_upsert_only_when_replace_existing_dates_is_disabled(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    trade_date = dt.date(2026, 4, 28)
    spec = replace(SUSPEND_D, name="suspend_d_append_only", replace_existing_dates=False)
    rows = [{"trade_date": "20260428", "ts_code": "000001.SZ", "suspend_type": "S"}]
    upserted = []

    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: rows)
    monkeypatch.setattr(engine, "_upsert_batch", lambda conn, spec, rows: upserted.append(rows) or len(rows))
    monkeypatch.setattr(engine, "_update_progress", lambda conn, job_id, result: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(conn, spec, trade_date, trade_date, uuid.uuid4())

    assert result.inserted_rows == 1
    assert all("DELETE FROM market.suspend_d" not in sql for sql, _params in conn.executed)
    assert any("dataset_date_refresh_audit" in sql for sql, _params in conn.executed)
    assert upserted == [rows]
    assert conn.commits == 0


def test_stock_st_events_success_hook_marks_dirty_and_ensures(monkeypatch):
    calls = []

    class _FakePitService:
        def mark_dirty(self, **kwargs):
            calls.append(("mark_dirty", kwargs))
            return {"dirty": True}

        def ensure_st_pit_universe(self, **kwargs):
            calls.append(("ensure", kwargs))
            return {"status": "ready", "rebuilt": False}

    monkeypatch.setattr(pit_service, "StockUniversePitService", _FakePitService)

    result = TushareSyncEngine()._run_stock_universe_pit_post_sync_hook("stock_st_events")

    assert result["ok"] is True
    assert calls[0][0] == "mark_dirty"
    assert calls[0][1]["source_dataset"] == "stock_st_events"
    assert calls[1] == ("ensure", {"strict": False})


def test_stock_basic_success_hook_also_ensures(monkeypatch):
    calls = []

    class _FakePitService:
        def mark_dirty(self, **kwargs):
            calls.append(("mark_dirty", kwargs))
            return {"dirty": True}

        def ensure_st_pit_universe(self, **kwargs):
            calls.append(("ensure", kwargs))
            return {"status": "ready", "rebuilt": True}

    monkeypatch.setattr(pit_service, "StockUniversePitService", _FakePitService)

    result = TushareSyncEngine()._run_stock_universe_pit_post_sync_hook("stock_basic")

    assert result["ok"] is True
    assert calls[0][0] == "mark_dirty"
    assert calls[0][1]["source_dataset"] == "stock_basic"
    assert calls[1] == ("ensure", {"strict": False})
