import datetime as dt
import uuid
from dataclasses import replace
from types import SimpleNamespace

import pytest

import backend.services.tushare_sync_engine as sync_engine
import backend.services.event_signal.tushare_event_raw_sync as raw_sync_module
import backend.services.stock_universe_pit_service as pit_service
from backend.services.tushare_dataset_specs import (
    CYQ_PERF,
    DATASET_REGISTRY,
    QueryMode,
    STOCK_ST_EVENTS,
    SUSPEND_D,
    SW_DAILY,
    SW_INDEX_MEMBER,
    TUSHARE_FORECAST_RAW,
)
from backend.services.tushare_sync_engine import TushareSyncEngine


class _NoopTargetRepository:
    def upsert_target(self, record):
        return {"target_id": "test-target"}

    def record_attempt(self, record):
        return {"attempt_id": "test-attempt"}


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))

    def fetchone(self):
        return self._conn.fetchone_value


class _FakeConn:
    def __init__(self):
        self.autocommit = True
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.fetchone_value = (0,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

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


def test_sync_by_date_treats_empty_stock_st_events_as_valid_no_change(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    publish_date = dt.date(2026, 7, 13)

    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: [])
    monkeypatch.setattr(engine, "_upsert_batch", lambda conn, spec, rows: 0)
    monkeypatch.setattr(engine, "_update_progress", lambda conn, job_id, result: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(
        conn,
        STOCK_ST_EVENTS,
        publish_date,
        publish_date,
        uuid.uuid4(),
    )

    assert result.failed_batches == 0
    assert result.success_batches == 1
    assert result.inserted_rows == 0
    assert conn.executed[0] == (
        "DELETE FROM market.stock_st_events WHERE pub_date = %s",
        (publish_date,),
    )
    audit_params = next(
        params
        for sql, params in conn.executed
        if "dataset_date_refresh_audit" in sql
    )
    assert audit_params[4] == "success"
    assert audit_params[-2] == "empty_valid"


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
    assert calls[1] == ("ensure", {"strict": False, "refresh_policy": "source_fingerprint"})


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
    assert calls[1] == ("ensure", {"strict": False, "refresh_policy": "source_fingerprint"})


class _RowsCursor(_FakeCursor):
    def __init__(self, conn):
        super().__init__(conn)
        self._rows = []

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self._rows = self._conn.query_results.pop(0) if self._conn.query_results else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _RowsConn(_FakeConn):
    def __init__(self, query_results=None):
        super().__init__()
        self.query_results = list(query_results or [])

    def cursor(self, *args, **kwargs):
        return _RowsCursor(self)


def test_trade_date_tushare_specs_use_trading_day_sequence():
    for name in {"daily_basic", "adj_factor", "bak_basic", "margin_detail", "stk_limit", "cyq_perf"}:
        spec = DATASET_REGISTRY[name]
        assert spec.query_mode == QueryMode.BY_DATE
        assert spec.date_column == "trade_date"
        assert spec.trading_day_only is True

    assert DATASET_REGISTRY["stock_st_events"].trading_day_only is False
    assert DATASET_REGISTRY["stock_st_events"].date_column == "pub_date"
    assert DATASET_REGISTRY["suspend_d"].trading_day_only is False
    assert DATASET_REGISTRY["suspend_d"].replace_existing_dates is True
    assert "suspend_d" in sync_engine.ZERO_ROW_VALID_DATASETS
    assert "stk_limit" not in sync_engine.ZERO_ROW_VALID_DATASETS
    assert "margin_detail" not in sync_engine.ZERO_ROW_VALID_DATASETS


def test_trading_day_sequence_uses_calendar_service_for_trade_date_specs(monkeypatch):
    engine = TushareSyncEngine(target_repository=None)
    calls = []

    def _list_trading_days_from_conn(conn, start_date, end_date, *, allow_empty=False):
        calls.append((conn, start_date, end_date, allow_empty))
        return [dt.date(2026, 5, 25)]

    monkeypatch.setattr(
        sync_engine.TradingCalendarStatusService,
        "list_trading_days_from_conn",
        staticmethod(_list_trading_days_from_conn),
    )
    conn = _FakeConn()

    days = engine._date_sequence_for_by_date(
        conn,
        DATASET_REGISTRY["stk_limit"],
        dt.date(2026, 5, 23),
        dt.date(2026, 5, 25),
    )

    assert days == [dt.date(2026, 5, 25)]
    assert calls == [(conn, dt.date(2026, 5, 23), dt.date(2026, 5, 25), True)]


def test_sync_by_date_skips_non_trading_days_for_stk_limit_and_margin_detail(monkeypatch):
    for dataset in ["stk_limit", "margin_detail"]:
        engine = TushareSyncEngine(target_repository=_NoopTargetRepository())
        conn = _FakeConn()
        fetched = []

        def _list_trading_days_from_conn(conn, start_date, end_date, *, allow_empty=False):
            assert start_date == dt.date(2026, 5, 23)
            assert end_date == dt.date(2026, 5, 25)
            return [dt.date(2026, 5, 25)]

        monkeypatch.setattr(
            sync_engine.TradingCalendarStatusService,
            "list_trading_days_from_conn",
            staticmethod(_list_trading_days_from_conn),
        )
        monkeypatch.setattr(
            engine,
            "_fetch_from_tushare",
            lambda spec, params: fetched.append((spec.name, params)) or [
                {"trade_date": "20260525", "ts_code": "000001.SZ"}
            ],
        )
        monkeypatch.setattr(engine, "_upsert_batch", lambda _conn, _spec, rows: len(rows))
        monkeypatch.setattr(engine, "_update_progress", lambda _conn, _job_id, _result: None)
        monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

        result = engine._sync_by_date(
            conn,
            DATASET_REGISTRY[dataset],
            dt.date(2026, 5, 23),
            dt.date(2026, 5, 25),
            uuid.uuid4(),
        )

        assert result.ok is True
        assert result.total_batches == 1
        assert result.success_batches == 1
        assert result.failed_batches == 0
        assert fetched == [(dataset, {"trade_date": "20260525"})]
        failure_audits = [
            params
            for sql, params in conn.executed
            if "INSERT INTO market.dataset_date_refresh_audit" in sql and params[4] == "failed"
        ]
        assert failure_audits == []


def test_cyq_dataset_specs_are_registered_with_independent_tables_and_limits():
    assert DATASET_REGISTRY["cyq_perf"] is CYQ_PERF
    assert "cyq_chips" not in DATASET_REGISTRY
    assert CYQ_PERF.query_mode == QueryMode.BY_DATE
    assert CYQ_PERF.target_table == "market.cyq_perf"
    assert CYQ_PERF.primary_keys == ["trade_date", "ts_code"]
    assert CYQ_PERF.initial_start_date == "2018-01-01"
    assert CYQ_PERF.fetch_params["limit"] == 4900
    assert CYQ_PERF.fetch_params["max_pages"] == 3
    assert CYQ_PERF.incremental_cursor_from_audit is True
    assert CYQ_PERF.trading_day_only is True


def test_fetch_from_tushare_paginates_when_limit_is_configured(monkeypatch):
    calls = []

    class _PagingHttpResponse:
        status_code = 200
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def _paging_post(url, payload, timeout):
        params = payload["params"]
        fields = str(payload["fields"]).split(",")
        calls.append(params)
        offset = int(params.get("offset") or 0)
        count = 4900 if offset == 0 else 1
        items = [[f"v{offset + i}" for _ in fields] for i in range(count)]
        return _PagingHttpResponse({"code": 0, "data": {"fields": fields, "items": items}})

    class _NoopLimiter:
        def acquire(self):
            return None

    monkeypatch.setattr(sync_engine, "get_limiter", lambda *_args, **_kwargs: _NoopLimiter())
    monkeypatch.setattr(sync_engine, "_http_post", _paging_post)
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    engine = TushareSyncEngine()

    rows = engine._fetch_from_tushare(CYQ_PERF, {"trade_date": "20260518"})

    assert len(rows) == 4901
    assert calls[0]["limit"] == 4900
    assert calls[0]["offset"] == 0
    assert calls[1]["offset"] == 4900


def test_incremental_cursor_from_audit_does_not_fall_back_to_physical_cursor():
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [],  # audit cursor missing
        [],  # physical table also empty
    ])

    cursor = engine._get_incremental_cursor(conn, CYQ_PERF)

    assert cursor is None
    assert not any("SELECT max" in sql.lower() and "market.cyq_perf" in sql for sql, _params in conn.executed)


def test_resolve_incremental_start_seeds_physical_rows_then_uses_first_gap():
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [],  # no audit rows
        [(dt.date(2026, 5, 15), 42), (dt.date(2026, 5, 18), 43)],  # physical row counts
        [
            (dt.date(2026, 5, 15), True),
            (dt.date(2026, 5, 16), False),
            (dt.date(2026, 5, 17), False),
            (dt.date(2026, 5, 18), True),
        ],  # trading-date sequence
        [],  # INSERT for 2026-05-15 audit row
        [],  # INSERT for 2026-05-18 audit row
    ])

    start, metadata = engine._resolve_incremental_start_date(conn, CYQ_PERF, dt.date(2026, 5, 18))

    assert start == dt.date(2026, 5, 19)
    assert metadata["safe_audit_cursor_date"] == "2026-05-18"
    assert metadata["audit_reconciled_to_end_date"] is True
    audit_params = [params for sql, params in conn.executed if "INSERT INTO market.dataset_date_refresh_audit" in sql]
    assert len(audit_params) == 2
    assert {params[1] for params in audit_params} == {dt.date(2026, 5, 15), dt.date(2026, 5, 18)}


def test_seed_missing_audit_does_not_fabricate_empty_success():
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [],  # no physical row counts
    ])

    seeded = engine._seed_missing_audit_from_physical(
        conn, CYQ_PERF, dt.date(2026, 5, 16), dt.date(2026, 5, 16)
    )

    assert seeded == 0
    assert not any("INSERT INTO market.dataset_date_refresh_audit" in sql for sql, _params in conn.executed)


def test_physical_audit_seed_records_gap_failure_and_stops_safe_cursor(monkeypatch):
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [(dt.date(2026, 5, 14), 10), (dt.date(2026, 5, 18), 11)],  # physical rows
        [],  # seed 2026-05-14 success
        [],  # seed 2026-05-15 gap failure
    ])
    monkeypatch.setattr(
        engine,
        "_date_sequence_for_by_date",
        lambda _conn, _spec, start, end: [
            day
            for day in [dt.date(2026, 5, 14), dt.date(2026, 5, 15), dt.date(2026, 5, 18)]
            if start <= day <= end
        ],
    )

    seeded = engine._seed_refresh_audit_from_physical_table(conn, CYQ_PERF)

    assert seeded is not None
    assert seeded.safe_cursor == dt.date(2026, 5, 14)
    assert seeded.max_table_date == dt.date(2026, 5, 18)
    assert seeded.success_dates == 1
    assert seeded.failed_gap_dates == 1
    assert any(
        "market.dataset_date_refresh_audit" in sql and params[1] == dt.date(2026, 5, 15) and params[13] == "physical_gap"
        for sql, params in conn.executed
    )


def test_resolve_incremental_start_reconciles_physical_rows_then_uses_first_gap(monkeypatch):
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [],  # no audit rows
        [(dt.date(2026, 5, 14), 10), (dt.date(2026, 5, 15), 11)],  # physical row counts
        [],  # INSERT for seeded 2026-05-14 audit row
        [],  # INSERT for seeded 2026-05-15 audit row
    ])
    monkeypatch.setattr(
        engine,
        "_date_sequence_for_by_date",
        lambda _conn, _spec, start, end: [
            day
            for day in [dt.date(2026, 5, 14), dt.date(2026, 5, 15), dt.date(2026, 5, 18)]
            if start <= day <= end
        ],
    )

    start, metadata = engine._resolve_incremental_start_date(conn, CYQ_PERF, dt.date(2026, 5, 18))

    assert start == dt.date(2026, 5, 18)
    assert metadata["safe_audit_cursor_date"] == "2026-05-15"


def test_resolve_incremental_start_skips_when_physical_and_audit_cover_end(monkeypatch):
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [(dt.date(2026, 5, 18), True)],  # audit rows cover end date
        [(dt.date(2026, 5, 18),)],  # trading-date sequence
    ])
    monkeypatch.setattr(
        engine,
        "_date_sequence_for_by_date",
        lambda _conn, _spec, start, end: [dt.date(2026, 5, 18)],
    )

    start, metadata = engine._resolve_incremental_start_date(conn, CYQ_PERF, dt.date(2026, 5, 18))

    assert start == dt.date(2026, 5, 19)
    assert metadata["audit_reconciled_to_end_date"] is True


def test_sync_incremental_reconciles_audit_before_fetching_first_gap(monkeypatch):
    engine = TushareSyncEngine(target_repository=None)
    conn = _RowsConn([
        [],  # audit cursor absent
        [(dt.date(2026, 5, 14), 10), (dt.date(2026, 5, 15), 11)],  # physical row counts
        [],  # seed 2026-05-14
        [],  # seed 2026-05-15
        [],  # create ingestion job
        [],  # write start log
        [],  # finish job select summary
        [],  # finish job update
    ])
    created_job = uuid.uuid4()
    sync_calls = []
    finish_calls = []
    summaries = []

    monkeypatch.setattr(sync_engine, "get_conn", lambda: conn)
    monkeypatch.setattr(engine, "_ensure_target_table_exists", lambda _conn, _spec: None)
    monkeypatch.setattr(
        engine,
        "_date_sequence_for_by_date",
        lambda _conn, _spec, start, end: [
            day
            for day in [dt.date(2026, 5, 14), dt.date(2026, 5, 15), dt.date(2026, 5, 18)]
            if start <= day <= end
        ],
    )

    def _create_job(_conn, job_type, summary):
        summaries.append((job_type, summary))
        return created_job

    def _sync_by_date(_conn, spec, start_date, end_date, job_id):
        sync_calls.append((spec.name, start_date, end_date, job_id))
        return sync_engine.SyncResult(
            dataset=spec.name,
            mode="sync",
            job_id=job_id,
            total_batches=1,
            success_batches=1,
            inserted_rows=7,
        )

    monkeypatch.setattr(engine, "_create_job", _create_job)
    monkeypatch.setattr(engine, "_sync_by_date", _sync_by_date)
    monkeypatch.setattr(engine, "_finish_job", lambda _conn, job_id, status, summary: finish_calls.append((job_id, status, summary)))

    result = engine.sync(CYQ_PERF, mode="incremental", end_date=dt.date(2026, 5, 18))

    assert result.ok is True
    assert sync_calls == [("cyq_perf", dt.date(2026, 5, 18), dt.date(2026, 5, 18), created_job)]
    assert summaries[0][1]["start_date"] == "2026-05-18"
    assert summaries[0][1]["safe_audit_cursor_date"] == "2026-05-15"
    assert finish_calls[0][1] == "success"
    audit_params = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert len(audit_params) == 2
    assert {params[1] for params in audit_params} == {dt.date(2026, 5, 14), dt.date(2026, 5, 15)}


def test_sync_by_date_rejects_provider_contract_date_mismatch(monkeypatch):
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [(dt.date(2026, 5, 18), True)],  # trading days
        [],  # create target swallowed by repo fallback
        [],  # audit failure insert
        [],  # progress update
    ])
    job_id = uuid.uuid4()
    rows = [{
        "trade_date": "20260517",
        "ts_code": "000001.SZ",
        "his_low": 1,
        "his_high": 2,
        "cost_5pct": 1,
        "cost_15pct": 1,
        "cost_50pct": 1,
        "cost_85pct": 1,
        "cost_95pct": 1,
        "weight_avg": 1,
        "winner_rate": 1,
    }]
    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda _spec, _params: rows)
    monkeypatch.setattr(engine, "_upsert_batch", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)

    result = engine._sync_by_date(conn, CYQ_PERF, dt.date(2026, 5, 18), dt.date(2026, 5, 18), job_id)

    assert result.failed_batches == 1
    audit_params = [
        params
        for sql, params in conn.executed
        if "INSERT INTO market.dataset_date_refresh_audit" in sql
    ]
    assert audit_params[-1][13] == "provider_contract_error"
    assert not any("INSERT INTO market.cyq_perf" in sql for sql, _params in conn.executed)


def test_sync_fails_fast_when_physical_table_is_missing_before_cold_floor(monkeypatch):
    engine = TushareSyncEngine()
    conn = _RowsConn([
        [(None,)],  # to_regclass(target table) says physical table is absent
        [],  # create failed job
        [],  # finish failed job select summary
        [],  # finish failed job update
    ])
    created_job = uuid.uuid4()

    monkeypatch.setattr(sync_engine, "get_conn", lambda: conn)
    monkeypatch.setattr(engine, "_create_job", lambda _conn, _mode, _summary: created_job)

    result = engine.sync(CYQ_PERF, mode="incremental", end_date=dt.date(2026, 5, 18))

    assert result.ok is False
    assert "target table market.cyq_perf is missing" in (result.error or "")
    assert "scripts/create_cyq_tables.py" in (result.error or "")
    assert not any("dataset_date_refresh_audit" in sql for sql, _params in conn.executed)


def _member_row(ts_code, l2_code="801767.SI", in_date="19960628", out_date=None):
    return {
        "l1_code": "801760.SI",
        "l1_name": "能源",
        "l2_code": l2_code,
        "l2_name": "数字媒体",
        "l3_code": "",
        "l3_name": "",
        "ts_code": ts_code,
        "name": "测试股",
        "in_date": in_date,
        "out_date": out_date,
        "is_new": "Y",
    }


def _stub_execute_values(monkeypatch):
    def _record(cur, sql, values, *args, **kwargs):
        cur._conn.executed.append((sql, None))

    monkeypatch.setattr(sync_engine.pgx, "execute_values", _record)


def test_sw_index_member_spec_enables_replace_by_code():
    assert SW_INDEX_MEMBER.replace_by_code is True
    assert SW_DAILY.replace_by_code is False


def test_replace_code_batch_deletes_then_inserts_in_single_transaction(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    _stub_execute_values(monkeypatch)
    rows = [_member_row("000406.SZ"), _member_row("000817.SZ", in_date="19980528")]

    inserted = engine._replace_code_batch(conn, SW_INDEX_MEMBER, "801767.SI", rows)

    assert inserted == 2
    assert conn.executed[0] == (
        "DELETE FROM market.sw_index_member WHERE l2_code = %s",
        ("801767.SI",),
    )
    assert any("INSERT INTO market.sw_index_member" in sql for sql, _ in conn.executed)
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.autocommit is True


def test_replace_code_batch_rolls_back_when_insert_fails(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()

    def _boom(conn, spec, rows):
        raise RuntimeError("insert failed")

    monkeypatch.setattr(engine, "_upsert_batch", _boom)

    with pytest.raises(RuntimeError, match="insert failed"):
        engine._replace_code_batch(conn, SW_INDEX_MEMBER, "801767.SI", [_member_row("000406.SZ")])

    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert conn.autocommit is True


def _patch_by_code_loop(engine, monkeypatch, conn, codes, payload):
    monkeypatch.setattr(sync_engine, "get_conn", lambda: conn)
    monkeypatch.setattr(engine, "_fetch_code_list", lambda _conn, _spec: codes)
    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: payload)
    monkeypatch.setattr(engine, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_update_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_record_by_code_audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)
    _stub_execute_values(monkeypatch)


def test_by_code_batched_mirrors_payload_and_takes_down_missing_rows(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    _patch_by_code_loop(
        engine, monkeypatch, conn, ["801767.SI"], [_member_row("000406.SZ")]
    )

    result = engine._sync_by_code_batched(SW_INDEX_MEMBER, None, None, uuid.uuid4())

    assert result.success_batches == 1
    assert result.failed_batches == 0
    assert result.inserted_rows == 1
    assert (
        "DELETE FROM market.sw_index_member WHERE l2_code = %s",
        ("801767.SI",),
    ) in conn.executed
    assert any("INSERT INTO market.sw_index_member" in sql for sql, _ in conn.executed)


def test_by_code_batched_refuses_to_mirror_empty_payload(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    conn.fetchone_value = (3,)  # local rows exist for the code
    _patch_by_code_loop(engine, monkeypatch, conn, ["801767.SI"], [])

    result = engine._sync_by_code_batched(SW_INDEX_MEMBER, None, None, uuid.uuid4())

    assert result.success_batches == 0
    assert result.failed_batches == 1
    assert result.inserted_rows == 0
    assert any(
        "SELECT COUNT(*) FROM market.sw_index_member WHERE l2_code = %s" in sql
        for sql, _ in conn.executed
    )
    assert not any("DELETE FROM market.sw_index_member" in sql for sql, _ in conn.executed)


def test_by_code_batched_accepts_empty_payload_when_local_empty(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()  # fetchone_value defaults to (0,): no local rows
    _patch_by_code_loop(engine, monkeypatch, conn, ["801217.SI"], [])

    result = engine._sync_by_code_batched(SW_INDEX_MEMBER, None, None, uuid.uuid4())

    assert result.success_batches == 1
    assert result.failed_batches == 0
    assert result.inserted_rows == 0
    assert not any("DELETE FROM market.sw_index_member" in sql for sql, _ in conn.executed)


def test_by_code_batched_keeps_upsert_when_replace_by_code_disabled(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    spec = replace(SW_INDEX_MEMBER, replace_by_code=False)
    _patch_by_code_loop(
        engine, monkeypatch, conn, ["801767.SI"], [_member_row("000406.SZ")]
    )

    result = engine._sync_by_code_batched(spec, None, None, uuid.uuid4())

    assert result.success_batches == 1
    assert result.failed_batches == 0
    assert not any("DELETE FROM market.sw_index_member" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO market.sw_index_member" in sql for sql, _ in conn.executed)


# ---------------------------------------------------------------------------
# BUG-947: tushare falsy-Response masking — direct dataapi transport
# ---------------------------------------------------------------------------


class _FakeHttpResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


def test_query_tushare_dataapi_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(
        sync_engine,
        "_http_post",
        lambda url, payload, timeout: _FakeHttpResponse(503, text="Bad Gateway"),
    )

    with pytest.raises(sync_engine.TushareHttpError, match="HTTP 503"):
        sync_engine._query_tushare_dataapi("sw_daily", {"ts_code": "801033.SI"}, "ts_code", token="t")


def test_query_tushare_dataapi_raises_on_nonzero_code(monkeypatch):
    monkeypatch.setattr(
        sync_engine,
        "_http_post",
        lambda url, payload, timeout: _FakeHttpResponse(200, {"code": 40020, "msg": "token invalid"}),
    )

    with pytest.raises(sync_engine.TushareHttpError, match="code=40020"):
        sync_engine._query_tushare_dataapi("sw_daily", {}, "ts_code", token="t")


def test_query_tushare_dataapi_returns_rows_and_genuine_empty(monkeypatch):
    responses = iter([
        _FakeHttpResponse(200, {"code": 0, "data": {"fields": ["ts_code", "close"], "items": [["801033.SI", 12.3]]}}),
        _FakeHttpResponse(200, {"code": 0, "data": {"fields": [], "items": []}}),
    ])
    monkeypatch.setattr(sync_engine, "_http_post", lambda url, payload, timeout: next(responses))

    rows = sync_engine._query_tushare_dataapi("sw_daily", {}, "ts_code,close", token="t")
    assert rows == [{"ts_code": "801033.SI", "close": 12.3}]
    # protocol-level success with zero items = legitimate empty, not an error
    assert sync_engine._query_tushare_dataapi("sw_daily", {}, "ts_code,close", token="t") == []


def test_fetch_from_tushare_retries_after_http_error_then_succeeds(monkeypatch):
    engine = TushareSyncEngine()
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)
    calls = []

    def _flaky(url, payload, timeout):
        calls.append(url)
        if len(calls) < 3:
            return _FakeHttpResponse(429, text="Too Many Requests")
        return _FakeHttpResponse(
            200,
            {"code": 0, "data": {"fields": ["l2_code", "ts_code"], "items": [["801767.SI", "000406.SZ"]]}},
        )

    monkeypatch.setattr(sync_engine, "_http_post", _flaky)

    rows = engine._fetch_from_tushare(SW_INDEX_MEMBER, {"l2_code": "801767.SI"})

    assert len(calls) == 3
    assert any(r["ts_code"] == "000406.SZ" for r in rows)


def test_fetch_from_tushare_raises_after_three_http_errors(monkeypatch):
    engine = TushareSyncEngine()
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        sync_engine,
        "_http_post",
        lambda url, payload, timeout: _FakeHttpResponse(500, text="boom"),
    )

    with pytest.raises(RuntimeError, match="failed after 3 retries"):
        engine._fetch_from_tushare(SW_INDEX_MEMBER, {"l2_code": "801767.SI"})


def test_by_code_batched_warns_on_empty_upsert_with_local_history(monkeypatch):
    engine = TushareSyncEngine()
    conn = _FakeConn()
    conn.fetchone_value = (124,)  # sw_daily has local history for this code
    spec = replace(SW_INDEX_MEMBER, replace_by_code=False)
    logs = []
    monkeypatch.setattr(sync_engine, "get_conn", lambda: conn)
    monkeypatch.setattr(engine, "_fetch_code_list", lambda _conn, _spec: ["801217.SI"])
    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda spec, params: [])
    monkeypatch.setattr(engine, "_log", lambda *args: logs.append(args))
    monkeypatch.setattr(engine, "_update_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_record_by_code_audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync_engine.time, "sleep", lambda seconds: None)
    _stub_execute_values(monkeypatch)

    result = engine._sync_by_code_batched(spec, None, None, uuid.uuid4())

    assert result.success_batches == 1
    assert result.failed_batches == 0
    warnings = [args for args in logs if len(args) >= 4 and str(args[2]).lower() == "warning"]
    assert len(warnings) == 1
    assert "upstream returned no rows but local history exists" in warnings[0][3]
