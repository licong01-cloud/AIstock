"""Publication/recovery contracts with no provider calls or database access."""
import datetime as dt
from types import SimpleNamespace
import uuid

import pytest

import backend.services.data_sync_targets as targets
from backend.services.tushare_sync_engine import TushareSyncEngine
from backend.services.tushare_dataset_specs import ETF_SHARE_SIZE, DAILY_BASIC, MARGIN_DETAIL, ADJ_FACTOR
from backend.services.audit_backed_data_health import AuditBackedDataHealthChecker
import backend.ingestion.tdx_scheduler as scheduler_module
from backend.ingestion.tdx_scheduler import TDXScheduler


def previous(day):
    return day - dt.timedelta(days=1)


@pytest.mark.parametrize("dataset,hour,expected", [
    ("margin_detail", 10, dt.date(2026, 10, 12)),
    ("margin_detail", 20, dt.date(2026, 10, 13)),
    ("etf_share_size", 7, dt.date(2026, 10, 12)),
    ("etf_share_size", 9, dt.date(2026, 10, 13)),
])
def test_publication_date_respects_ready_time(dataset, hour, expected):
    now = dt.datetime(2026, 10, 14, hour)
    assert targets.publication_target_date(dataset, now.date(), now, previous) == expected


def test_health_uses_same_publication_policy():
    checker = AuditBackedDataHealthChecker({})
    checker._now_cn = lambda: dt.datetime(2026, 10, 14, 10)
    checker._previous_trading_day = previous
    assert checker._expected_date("margin_detail", dt.date(2026, 10, 14)) == dt.date(2026, 10, 12)


def test_etf_completed_cursor_rechecks_bounded_tail():
    engine = TushareSyncEngine()
    engine._get_incremental_cursor = lambda *_args: dt.date(2026, 10, 13)
    engine._next_sync_date_after = lambda *_args: dt.date(2026, 10, 14)
    engine._iter_sync_dates = lambda *_args: [dt.date(2026, 10, d) for d in (8, 9, 12, 13)]
    engine._unclosed_publication_dates = lambda *_args: [dt.date(2026, 9, 30)]
    start, metadata = engine._resolve_incremental_start_date(None, ETF_SHARE_SIZE, dt.date(2026, 10, 13))
    assert start == dt.date(2026, 10, 8)
    assert metadata["late_publication_recheck"] is True
    assert engine._late_recheck_dates[ETF_SHARE_SIZE.name] == [dt.date(2026, 9, 30)]


def test_freshness_target_is_not_lagged_twice(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    writes = []
    repo = SimpleNamespace(upsert_target=lambda record: writes.append(record) or {"target_id": "t"})
    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: repo)
    scheduler._retry_schedule_dataset = lambda dataset: dataset
    scheduler._final_deadline_for_target = lambda *_args: None
    scheduler._availability_adjusted_target_date = lambda *_args: pytest.fail("double lag")
    scheduler._record_freshness_retry_targets([
        SimpleNamespace(dataset="margin_detail", status="stale", expected_date=dt.date(2026, 10, 12))
    ])
    assert writes[0].target_date == dt.date(2026, 10, 12)


def test_sector_historical_failure_does_not_starve_current_append(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    actions = []
    builder = SimpleNamespace(
        pending_dates=lambda _end: [dt.date(2021, 7, 30)],
        build_date=lambda _day: (_ for _ in ()).throw(ValueError("missing_pit_mappings=92")),
        build_range=lambda start, end: actions.append(("append", start, end)) or 12,
    )
    monkeypatch.setattr("backend.services.sector_data_builder.SectorDataBuilder", lambda: builder)
    scheduler._query_max_date = lambda table, _col: dt.date(2026, 10, 12 if table.endswith("sector_data") else 13)
    scheduler._latest_completed_trading_day = lambda: dt.date(2026, 10, 13)
    scheduler._execute = lambda *_args: None
    scheduler._record_refresh_audit_from_table_range = lambda **kw: actions.append(("audit", kw)) or True
    scheduler._log_ingestion_run = lambda *a, **kw: actions.append(("log", a, kw))
    scheduler._update_ingestion_schedule = lambda *a, **kw: actions.append(("schedule", kw))
    scheduler._run_sector_data_build(uuid.uuid4(), "s", "incremental", "unit", {})
    assert actions[0] == ("append", dt.date(2026, 10, 13), dt.date(2026, 10, 13))
    assert next(item[1][4] for item in actions if item[0] == "log") == "partial"
    assert "2021-07-30" in next(item[2]["error"] for item in actions if item[0] == "log")


def test_sector_init_audit_failure_is_not_success_and_remains_pending(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    writes, logs = [], []
    builder = SimpleNamespace(build_range=lambda *_args: 12)
    monkeypatch.setattr("backend.services.sector_data_builder.SectorDataBuilder", lambda: builder)
    scheduler._execute = lambda *args: writes.append(args)
    scheduler._record_refresh_audit_from_table_range = lambda **_kw: False
    scheduler._log_ingestion_run = lambda *args, **kw: logs.append((args, kw))
    scheduler._update_ingestion_schedule = lambda *_args, **_kw: None
    scheduler._run_sector_data_build(uuid.uuid4(), "s", "init", "unit", {
        "start_date": "2026-10-13", "end_date": "2026-10-13",
    })
    assert logs[0][0][4] == "failed"
    assert "audit" in logs[0][1]["error"]
    assert any("repair_pending" in sql and params[1] == [dt.date(2026, 10, 13)] for sql, params in writes)


def test_repair_retains_finite_values_and_only_fills_source_nulls():
    from scripts.repair_local_data_daily_sync import plan_group
    columns = ("trade_date", "ts_code", "nav", "total_size", "close")
    day = dt.date(2026, 10, 13)
    row = dict(trade_date=day, ts_code="510050.SH", nav=1.5, total_size=None, close=1.4)
    before = {row["ts_code"]: dict(row, nav=1.6, close=None)}
    patches, stats = plan_group("etf_share_size", columns, [row], before)
    assert patches == [dict(row, nav=None)]
    assert stats["null_fields_to_fill"] == 1


@pytest.mark.parametrize("value", [float("nan"), float("inf"), "NaN"])
def test_late_numeric_invalid_fact_is_rejected_before_database(value):
    engine = TushareSyncEngine()
    with pytest.raises(ValueError, match="non-finite"):
        engine._upsert_plain_batch(None, ETF_SHARE_SIZE, [{
            "trade_date": "20261013", "ts_code": "510050.SH", "nav": value,
        }])


@pytest.mark.parametrize("spec", [ETF_SHARE_SIZE, DAILY_BASIC, MARGIN_DETAIL, ADJ_FACTOR])
def test_shared_upsert_preserves_nullable_late_facts_without_changing_adj_factor(monkeypatch, spec):
    from contextlib import nullcontext
    import backend.services.tushare_sync_engine as module

    captured = []
    monkeypatch.setattr(module.pgx, "execute_values", lambda _cur, sql, vals: captured.append((sql, vals)))
    conn = SimpleNamespace(cursor=lambda: nullcontext(None))
    TushareSyncEngine()._upsert_plain_batch(conn, spec, [{"trade_date": "20261013", "ts_code": "000001.SZ"}])
    assert ("COALESCE(EXCLUDED." in captured[0][0]) == (spec is not ADJ_FACTOR)
    assert None in captured[0][1][0]


def test_target_refresh_retains_deadline_and_scheduler_metadata():
    from contextlib import nullcontext

    captured = []
    cur = SimpleNamespace(execute=lambda sql, params: captured.append(sql))
    conn = SimpleNamespace(cursor=lambda: nullcontext(cur))
    repo = targets.DataSyncTargetRepository(lambda: nullcontext(conn))
    repo._row = lambda _cur: {}
    repo.upsert_target(targets.DataSyncTargetRecord(dataset="margin_detail", data_source="tushare"))
    assert "data_sync_targets.metadata || EXCLUDED.metadata" in captured[0]
    assert "COALESCE(EXCLUDED.required_before, data_sync_targets.required_before)" in captured[0]


def test_daily_source_units_and_exact_share_precision():
    from scripts.repair_local_data_daily_sync import daily_row
    row = daily_row(dict(ts_code="000031.SZ", open=2.3, high=2.4, low=2.2, close=2.39,
                         vol=123.45, amount=321.123))
    assert row["volume_hand"] == 123
    assert row["volume_shares"] == 12345
    assert row["amount_li"] == 321123000
    assert row["close_li"] == 2390


def test_raw_existing_fact_cannot_be_changed():
    from scripts.repair_local_data_daily_sync import plan_group
    row = dict(ts_code="000031.SZ", trade_date=dt.date(2026, 10, 8), close_li=2390)
    with pytest.raises(ValueError, match="no overwrite"):
        plan_group("kline_daily_raw", tuple(row), [row], {row["ts_code"]: dict(row, close_li=None)})


def test_go_watcher_timeout_preserves_async_job_identity(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    actions = []
    def fetch(sql, _params=()):
        if "trading_calendar" in sql:
            return [{"latest": dt.date(2026, 10, 13)}]
        if "stock_basic" in sql:
            return [{"ts_code": "000001.SZ"}]
        assert "INTERVAL '45 days'" in sql
        return [{"mx": dt.date(2026, 10, 13)}]
    scheduler._fetchall = fetch
    scheduler._latest_completed_trading_day = lambda: dt.date(2026, 10, 13)
    scheduler._execute = lambda *a: actions.append(("sql", a))
    scheduler._log_ingestion_run = lambda *a, **kw: actions.append(("log", a, kw))
    scheduler._update_ingestion_schedule = lambda *a, **kw: actions.append(("schedule", kw))
    posted = []
    def post(_url, **kw):
        posted.append(kw["json"])
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"code": 0, "data": {"task_id": "go-t"}})
    monkeypatch.setattr(scheduler_module.requests, "post", post)
    ticks = iter([0, 2401])
    monkeypatch.setattr(scheduler_module.time, "time", lambda: next(ticks))
    scheduler._run_go_incremental(uuid.uuid4(), "s", "kline_minute_raw", "unit", {})
    assert posted[0]["codes"] == ["000001.SZ"]
    assert not posted[0]["options"]["truncate_before"]
    assert not any("status='failed'" in action[1][0] for action in actions if action[0] == "sql")
    accepted = next(action[1] for action in actions if action[0] == "log" and action[1][4] == "running")
    assert accepted[5]["go_task_id"] == "go-t"
    assert next(action[1]["last_status"] for action in actions if action[0] == "schedule") == "delayed"


def test_go_completion_audit_failure_is_failed_in_all_projections(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    statuses = []
    scheduler._latest_completed_trading_day = lambda: dt.date(2026, 10, 13)
    scheduler._fetchall = lambda sql, *args: ([{"ts_code": "000001.SZ"}]
                                            if "stock_basic" in sql else [{"mx": dt.date(2026, 10, 13)}])
    scheduler._execute = lambda *_args: None
    scheduler._record_refresh_audit_from_table_range = lambda **_kw: False
    scheduler._log_ingestion_run = lambda *a, **_kw: statuses.append(a[4])
    scheduler._update_ingestion_schedule = lambda *a, **kw: statuses.append(kw["last_status"])
    response = lambda data: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"code": 0, "data": data})
    monkeypatch.setattr(scheduler_module.requests, "post", lambda *a, **kw: response({"task_id": "go-t"}))
    monkeypatch.setattr(scheduler_module.requests, "get", lambda *a, **kw: response({"status": "success"}))
    monkeypatch.setattr(scheduler_module.time, "sleep", lambda *_args: None)
    scheduler._run_go_incremental(uuid.uuid4(), "s", "kline_minute_raw", "unit", {})
    assert statuses == ["running", "failed", "failed"]


def test_raw_completeness_query_is_exact_date_and_uses_china_clock():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    def fetch(sql, params):
        calls.append((sql, params))
        if "kline_daily_raw" in sql:
            return [{"ts_code": "000001.SZ", "volume_hand": 10, "volume_shares": 1000}]
        return [{"ts_code": "000001.SZ", "bars": 239, "bad_bars": 0, "traded": True}]
    scheduler._fetchall = fetch
    assert scheduler._raw_date_completeness("kline_minute_raw", dt.date(2026, 10, 8))["incomplete_symbols"] == 1
    assert "Asia/Shanghai" in calls[1][0]
    assert "trade_date=%s" in calls[0][0]
    assert all("JOIN" not in call[0] for call in calls)
