import datetime as dt
import uuid
from dataclasses import replace
from types import SimpleNamespace

import backend.services.tushare_sync_engine as sync_engine
import backend.services.event_signal.tushare_event_raw_sync as raw_sync_module
import backend.services.stock_universe_pit_service as pit_service
from backend.services.tushare_dataset_specs import CYQ_PERF, DATASET_REGISTRY, QueryMode, SUSPEND_D, TUSHARE_FORECAST_RAW
from backend.services.tushare_sync_engine import TushareSyncEngine


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        if "FROM market.trading_calendar" in sql:
            start_date, end_date = params
            rows = []
            cur = start_date
            while cur <= end_date:
                if cur.weekday() < 5:
                    rows.append((cur,))
                cur += dt.timedelta(days=1)
            self._rows = rows
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


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


def test_cyq_perf_dataset_spec_is_engine_managed_by_date():
    spec = DATASET_REGISTRY["cyq_perf"]

    assert spec is CYQ_PERF
    assert spec.query_mode == QueryMode.BY_DATE
    assert spec.tushare_api == "cyq_perf"
    assert spec.target_table == "market.cyq_perf"
    assert spec.primary_keys == ["trade_date", "ts_code"]
    assert spec.page_limit == 4900
    assert spec.incremental_cursor_from_audit is True
    assert spec.bootstrap_start_date == "2018-01-01"
    assert spec.date_sequence == "trading"


def test_cyq_perf_by_date_sync_writes_refresh_audit(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    trade_date = dt.date(2026, 5, 18)
    rows = [
        {
            "trade_date": "20260518",
            "ts_code": "000001.SZ",
            "his_low": 1,
            "his_high": 2,
            "cost_5pct": 1,
            "cost_15pct": 1,
            "cost_50pct": 1,
            "cost_85pct": 1,
            "cost_95pct": 1,
            "weight_avg": 1,
            "winner_rate": 50,
        }
    ]

    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: rows)
    monkeypatch.setattr(engine, "_upsert_batch", lambda conn, spec, rows: len(rows))
    monkeypatch.setattr(engine, "_update_progress", lambda conn, job_id, result: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(conn, CYQ_PERF, trade_date, trade_date, uuid.uuid4())

    assert result.ok is True
    assert result.inserted_rows == 1
    audit_rows = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert audit_rows
    assert audit_rows[-1][0] == "cyq_perf"
    assert audit_rows[-1][1] == trade_date
    assert audit_rows[-1][4] == "success"
    assert audit_rows[-1][12] == "ok"


def test_cyq_perf_by_date_sync_skips_non_trading_dates(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    fetched_dates = []

    def _fetch(_spec, params):
        fetched_dates.append(params["trade_date"])
        return [
            {
                "trade_date": params["trade_date"],
                "ts_code": "000001.SZ",
                "his_low": 1,
                "his_high": 2,
                "cost_5pct": 1,
                "cost_15pct": 1,
                "cost_50pct": 1,
                "cost_85pct": 1,
                "cost_95pct": 1,
                "weight_avg": 1,
                "winner_rate": 50,
            }
        ]

    monkeypatch.setattr(engine, "_fetch_from_tushare", _fetch)
    monkeypatch.setattr(engine, "_upsert_batch", lambda conn, spec, rows: len(rows))
    monkeypatch.setattr(engine, "_update_progress", lambda conn, job_id, result: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(
        conn,
        CYQ_PERF,
        dt.date(2026, 5, 16),
        dt.date(2026, 5, 18),
        uuid.uuid4(),
    )

    assert result.ok is True
    assert result.total_batches == 1
    assert fetched_dates == ["20260518"]


def test_cyq_perf_incremental_cursor_uses_audit_not_physical_table():
    class _Cursor(_FakeCursor):
        def __init__(self, conn):
            super().__init__(conn)
            self._row = (dt.date(2026, 5, 18),)

        def execute(self, sql, params=None):
            self._conn.executed.append((sql, params))
            if "FROM market.dataset_date_refresh_audit" in sql:
                self._rows = [(dt.date(2026, 5, 18), True)]
            else:
                self._rows = []

        def fetchone(self):
            return self._row

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    conn = _Conn()

    cursor = TushareSyncEngine()._get_incremental_cursor(conn, CYQ_PERF)

    assert cursor == dt.date(2026, 5, 18)
    assert any("market.dataset_date_refresh_audit" in sql for sql, _params in conn.executed)
    assert all("market.cyq_perf" not in sql for sql, _params in conn.executed)


def test_cyq_perf_audit_cursor_stops_before_unresolved_audit_gap():
    class _Cursor(_FakeCursor):
        def execute(self, sql, params=None):
            self._conn.executed.append((sql, params))
            if "FROM market.dataset_date_refresh_audit" in sql:
                self._rows = [
                    (dt.date(2026, 5, 14), True),
                    (dt.date(2026, 5, 15), False),
                    (dt.date(2026, 5, 18), True),
                ]
            elif "FROM market.trading_calendar" in sql:
                self._rows = [
                    (dt.date(2026, 5, 14),),
                    (dt.date(2026, 5, 15),),
                    (dt.date(2026, 5, 18),),
                ]
            else:
                self._rows = []

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    conn = _Conn()

    cursor = TushareSyncEngine()._get_incremental_cursor(conn, CYQ_PERF)

    assert cursor == dt.date(2026, 5, 14)
    assert all("market.cyq_perf" not in sql for sql, _params in conn.executed)


def test_cyq_perf_bootstrap_incremental_uses_full_start_only_when_audit_and_table_empty(monkeypatch):
    class _Cursor(_FakeCursor):
        def __init__(self, conn):
            super().__init__(conn)
            self._row = (None,)

        def fetchone(self):
            return self._row

    class _Conn(_FakeConn):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cursor(self)

    conn = _Conn()
    engine = TushareSyncEngine()
    captured = {}

    monkeypatch.setattr(sync_engine, "get_conn", lambda: conn)
    monkeypatch.setattr(
        engine,
        "_sync_by_date",
        lambda _conn, spec, start_date, end_date, job_id: captured.update(
            {"start_date": start_date, "end_date": end_date, "job_id": job_id}
        )
        or sync_engine.SyncResult(dataset=spec.name, mode="sync", job_id=job_id),
    )

    result = engine.sync(CYQ_PERF, mode="incremental", end_date=dt.date(2026, 5, 18))

    assert result.ok is True
    assert captured["start_date"] == dt.date(2018, 1, 1)
    assert captured["end_date"] == dt.date(2026, 5, 18)


def test_cyq_perf_audit_cursor_missing_seeds_from_physical_table_before_bootstrap():
    class _Cursor(_FakeCursor):
        def __init__(self, conn):
            super().__init__(conn)
            self._row = (None,)

        def execute(self, sql, params=None):
            self._conn.executed.append((sql, params))
            if "FROM market.dataset_date_refresh_audit" in sql:
                self._row = (None,)
                self._rows = []
            elif "FROM market.cyq_perf" in sql and "GROUP BY" in sql:
                self._rows = [(dt.date(2026, 5, 18), 32961)]
            elif "FROM market.trading_calendar" in sql:
                start_date, end_date = params
                self._rows = [(start_date,)] if start_date == end_date else []
            else:
                self._rows = []

        def fetchone(self):
            return self._row

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    conn = _Conn()

    cursor = TushareSyncEngine()._get_incremental_cursor(conn, CYQ_PERF)

    assert cursor == dt.date(2026, 5, 18)
    audit_rows = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert audit_rows
    assert audit_rows[-1][0] == "cyq_perf"
    assert audit_rows[-1][1] == dt.date(2026, 5, 18)
    assert audit_rows[-1][2] == "physical_audit_seed"
    assert audit_rows[-1][4] == "success"


def test_cyq_perf_audit_seed_returns_safe_cursor_before_physical_gap():
    class _Cursor(_FakeCursor):
        def __init__(self, conn):
            super().__init__(conn)
            self._row = (None,)

        def execute(self, sql, params=None):
            self._conn.executed.append((sql, params))
            if "FROM market.dataset_date_refresh_audit" in sql:
                self._row = (None,)
                self._rows = []
            elif "FROM market.cyq_perf" in sql and "GROUP BY" in sql:
                self._rows = [
                    (dt.date(2026, 5, 14), 32000),
                    (dt.date(2026, 5, 18), 32961),
                ]
            elif "FROM market.trading_calendar" in sql:
                self._rows = [
                    (dt.date(2026, 5, 14),),
                    (dt.date(2026, 5, 15),),
                    (dt.date(2026, 5, 18),),
                ]
            else:
                self._rows = []

        def fetchone(self):
            return self._row

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    conn = _Conn()

    cursor = TushareSyncEngine()._get_incremental_cursor(conn, CYQ_PERF)

    assert cursor == dt.date(2026, 5, 14)
    audit_rows = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert [row[1] for row in audit_rows] == [dt.date(2026, 5, 14), dt.date(2026, 5, 15)]
    assert audit_rows[1][4] == "failed"
    assert audit_rows[1][13] == "physical_gap"


def test_all_audit_cursor_specs_seed_from_existing_physical_table_when_audit_missing():
    class _Cursor(_FakeCursor):
        def __init__(self, conn):
            super().__init__(conn)
            self._row = (None,)

        def execute(self, sql, params=None):
            self._conn.executed.append((sql, params))
            if "FROM market.dataset_date_refresh_audit" in sql:
                self._row = (None,)
                self._rows = []
            elif "GROUP BY" in sql:
                self._rows = [(dt.date(2026, 5, 18), 7)]
            elif "FROM market.trading_calendar" in sql:
                self._rows = [(dt.date(2026, 5, 18),)]
            else:
                self._rows = []

        def fetchone(self):
            return self._row

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    audit_cursor_datasets = [
        "stock_st_events",
        "cyq_perf",
        "tushare_forecast_raw",
        "tushare_express_raw",
        "tushare_fina_indicator_raw",
    ]

    for name in audit_cursor_datasets:
        conn = _Conn()
        spec = DATASET_REGISTRY[name]
        cursor = TushareSyncEngine()._get_incremental_cursor(conn, spec)

        assert cursor == dt.date(2026, 5, 18)
        assert any(spec.target_table in sql and "GROUP BY" in sql for sql, _params in conn.executed)
        audit_rows = [
            params
            for sql, params in conn.executed
            if "INSERT INTO market.dataset_date_refresh_audit" in sql
        ]
        assert audit_rows[-1][0] == name
        assert audit_rows[-1][2] == "physical_audit_seed"


def test_by_date_sync_records_provider_contract_error_for_wrong_trade_date(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    trade_date = dt.date(2026, 5, 18)
    rows = [
        {
            "trade_date": "20260517",
            "ts_code": "000001.SZ",
        }
    ]

    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: rows)
    monkeypatch.setattr(engine, "_update_progress", lambda conn, job_id, result: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(conn, CYQ_PERF, trade_date, trade_date, uuid.uuid4())

    assert result.failed_batches == 1
    audit_rows = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert audit_rows
    assert audit_rows[-1][4] == "failed"
    assert audit_rows[-1][12] == "error"
    assert audit_rows[-1][13] == "provider_contract_error"


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
