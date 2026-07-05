import datetime as dt
import uuid

from scripts import ingest_incremental as inc


class DummyCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))


class DummyConn:
    def __init__(self):
        self.executed = []
        self.values = []
        self.rollback_count = 0

    def cursor(self):
        return DummyCursor(self)

    def rollback(self):
        self.rollback_count += 1


def _minute_row(day: dt.date, minute: int, price: float = 10.0):
    trade_time = dt.datetime.combine(day, dt.time(9, 30)) + dt.timedelta(minutes=minute)
    return {
        "Time": trade_time.isoformat(),
        "Open": price,
        "High": price + 0.2,
        "Low": price - 0.1,
        "Close": price + 0.1,
        "Volume": 100 + minute,
        "Amount": 10000 + minute,
    }


def test_minute_history_backfill_uses_kline_all_and_upserts_true_ohlc(monkeypatch):
    target = dt.date(2026, 5, 29)
    latest = dt.date(2026, 7, 3)
    calls = []

    def fake_http_get(path, params=None):
        calls.append((path, params))
        return {
            "code": 0,
            "data": {
                "list": [
                    _minute_row(latest, 0, 20.0),
                    _minute_row(target, 1, 10.0),
                    _minute_row(target, 0, 9.9),
                ]
            },
        }

    def fake_execute_values(cur, sql, values):
        cur.conn.executed.append((sql, None))
        cur.conn.values.extend(values)

    monkeypatch.setattr(inc, "http_get", fake_http_get)
    monkeypatch.setattr(inc.pgx, "execute_values", fake_execute_values)

    rows = inc.fetch_minute_range("688591", target, target)
    conn = DummyConn()
    inserted, last_ts = inc.upsert_minute(conn, "688591.SH", target, rows)

    assert calls == [("/api/kline-all/tdx", {"code": "688591", "type": "minute1"})]
    assert inserted == 2
    assert last_ts is not None
    assert [value[3:7] for value in conn.values] == [
        (9.9, 10.1, 9.8, 10.0),
        (10.0, 10.2, 9.9, 10.1),
    ]
    assert all(value[2] == "1m" and value[9] == "none" and value[10] == "tdx_api" for value in conn.values)


def test_minute_completeness_detects_gap_and_repairs(monkeypatch):
    target = dt.date(2026, 5, 29)
    run_id = uuid.uuid4()
    job_id = uuid.uuid4()
    conn = DummyConn()
    gap = {"ts_code": "688591.SH", "actual_bars": 0, "expected_bars": 240}
    find_calls = []
    upserts = []

    monkeypatch.setattr(inc, "is_trading_day", lambda _conn, _date: True)
    monkeypatch.setattr(inc, "get_expected_minute_codes", lambda _conn, _date, _codes: ["688591.SH"])

    def fake_find(_conn, _date, _codes, _expected=240):
        find_calls.append(1)
        return [gap] if len(find_calls) == 1 else []

    monkeypatch.setattr(inc, "find_minute_day_gaps", fake_find)
    monkeypatch.setattr(inc, "fetch_minute_range", lambda _code, _start, _end: [_minute_row(target, idx) for idx in range(240)])

    def fake_upsert(_conn, ts_code, trade_date, bars):
        upserts.append((ts_code, trade_date, len(bars)))
        return len(bars), bars[-1]["Time"]

    monkeypatch.setattr(inc, "upsert_minute", fake_upsert)
    monkeypatch.setattr(inc, "upsert_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(inc, "upsert_checkpoint", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(inc, "update_job_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(inc, "log_ingestion", lambda *_args, **_kwargs: None)

    errors = []
    monkeypatch.setattr(inc, "log_error", lambda *args, **_kwargs: errors.append(args))

    stats = {"inserted_rows": 0}
    remaining = inc.validate_minute_day_and_repair(
        conn, run_id, job_id, "kline_minute_raw", ["688591.SH"], target, stats
    )

    assert remaining == []
    assert upserts == [("688591.SH", target, 240)]
    assert stats["inserted_rows"] == 240
    assert stats["completeness_initial_gap_codes"] == 1
    assert stats["completeness_repaired_codes"] == 1
    assert errors == []


def test_minute_completeness_logs_error_when_retry_remains_short(monkeypatch):
    target = dt.date(2026, 6, 4)
    run_id = uuid.uuid4()
    job_id = uuid.uuid4()
    gap = {"ts_code": "688591.SH", "actual_bars": 12, "expected_bars": 240}

    monkeypatch.setattr(inc, "is_trading_day", lambda _conn, _date: True)
    monkeypatch.setattr(inc, "get_expected_minute_codes", lambda _conn, _date, _codes: ["688591.SH"])
    monkeypatch.setattr(inc, "find_minute_day_gaps", lambda *_args, **_kwargs: [gap])
    monkeypatch.setattr(inc, "fetch_minute_range", lambda _code, _start, _end: [])
    monkeypatch.setattr(inc, "log_ingestion", lambda *_args, **_kwargs: None)

    errors = []
    monkeypatch.setattr(
        inc,
        "log_error",
        lambda _conn, _run_id, _dataset, ts_code, message, detail=None: errors.append((ts_code, message, detail)),
    )

    stats = {"inserted_rows": 0}
    remaining = inc.validate_minute_day_and_repair(
        DummyConn(), run_id, job_id, "kline_minute_raw", ["688591.SH"], target, stats
    )

    assert remaining == [gap]
    assert stats["completeness_failed_codes"] == 1
    assert any(message == "minute completeness retry returned no bars" for _ts_code, message, _detail in errors)
    assert any(message == "minute completeness check failed after retry" for _ts_code, message, _detail in errors)


def test_ingest_minute_marks_failed_when_completeness_still_has_gaps(monkeypatch):
    target = dt.date(2026, 6, 25)
    run_id = uuid.uuid4()
    job_id = uuid.uuid4()
    statuses = {}
    validation_calls = []

    monkeypatch.setattr(inc, "tqdm", None)
    monkeypatch.setattr(inc, "create_job", lambda *_args, **_kwargs: job_id)
    monkeypatch.setattr(inc, "create_run", lambda *_args, **_kwargs: run_id)
    monkeypatch.setattr(inc, "update_job_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(inc, "create_task", lambda *_args, **_kwargs: uuid.uuid4())
    monkeypatch.setattr(inc, "complete_task", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(inc, "fetch_minute_range", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(inc, "log_ingestion", lambda *_args, **_kwargs: None)

    def fake_validate(conn, observed_run_id, observed_job_id, dataset, codes, start_date, end_date, stats):
        validation_calls.append((observed_run_id, observed_job_id, dataset, codes, start_date, end_date))
        return [{"ts_code": "688591.SH", "actual_bars": 12, "expected_bars": 240}]

    def fake_finish_run(_conn, observed_run_id, status, stats):
        statuses["run"] = (observed_run_id, status, dict(stats))

    def fake_finish_job(_conn, observed_job_id, status, summary):
        statuses["job"] = (observed_job_id, status, summary)

    monkeypatch.setattr(inc, "validate_minute_range_and_repair", fake_validate)
    monkeypatch.setattr(inc, "finish_run", fake_finish_run)
    monkeypatch.setattr(inc, "finish_job", fake_finish_job)

    inc.ingest_minute(DummyConn(), ["688591.SH"], target, target, batch_size=1, max_empty=0)

    assert validation_calls == [(run_id, job_id, "kline_minute_raw", ["688591.SH"], target, target)]
    assert statuses["run"][1] == "failed"
    assert statuses["job"][1] == "failed"
    assert statuses["run"][2]["failed_codes"] == 1
