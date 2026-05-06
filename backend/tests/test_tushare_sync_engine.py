import datetime as dt
import uuid
from dataclasses import replace
from types import SimpleNamespace

import backend.services.tushare_sync_engine as sync_engine
import backend.services.event_signal.tushare_event_raw_sync as raw_sync_module
import backend.services.stock_universe_pit_service as pit_service
from backend.services.tushare_dataset_specs import DATASET_REGISTRY, QueryMode, SUSPEND_D, TUSHARE_FORECAST_RAW
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


class _PeriodCursor(_FakeCursor):
    def __init__(self, conn):
        super().__init__(conn)
        self._rows = []

    def execute(self, sql, params=None):
        super().execute(sql, params)
        if "GROUP BY" in sql and "market.tushare_forecast_raw" in sql:
            self._rows = [(dt.date(2024, 2, 1), 3)]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


class _PeriodConn(_FakeConn):
    def cursor(self):
        return _PeriodCursor(self)


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


def test_financial_event_raw_dataset_specs_use_period_vip_sync_mode():
    for name, api in {
        "tushare_forecast_raw": "forecast_vip",
        "tushare_express_raw": "express_vip",
        "tushare_fina_indicator_raw": "fina_indicator_vip",
    }.items():
        spec = DATASET_REGISTRY[name]
        assert spec.query_mode == QueryMode.BY_PERIOD
        assert spec.tushare_api == api
        assert spec.date_column == "ann_date"
        assert spec.incremental_cursor_from_audit is True


def test_sync_by_period_uses_financial_raw_service_and_records_sparse_audit(monkeypatch):
    calls = []

    class _FakeRawService:
        def sync_period(self, dataset, *, period, job_id=None):
            calls.append((dataset, period, job_id))
            return SimpleNamespace(fetched_rows=5, written_rows=4, skipped_rows=1)

    monkeypatch.setattr(raw_sync_module, "TushareEventRawSyncService", _FakeRawService)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    engine = TushareSyncEngine()
    conn = _PeriodConn()
    job_id = uuid.uuid4()
    result = engine._sync_by_period(
        conn,
        TUSHARE_FORECAST_RAW,
        dt.date(2024, 2, 1),
        dt.date(2024, 2, 2),
        job_id,
    )

    assert result.ok is True
    assert result.periods == ["20231231"]
    assert result.inserted_rows == 4
    assert calls == [("forecast", "20231231", job_id)]
    audit_rows = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert len(audit_rows) == 2
    assert audit_rows[0][0] == "tushare_forecast_raw"
    assert audit_rows[0][1] == dt.date(2024, 2, 1)
    assert audit_rows[0][5] == 3
    assert audit_rows[0][12] == "ok"
    assert audit_rows[1][1] == dt.date(2024, 2, 2)
    assert audit_rows[1][5] == 0
    assert audit_rows[1][12] == "empty_valid"


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
