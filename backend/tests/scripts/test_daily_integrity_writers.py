"""Direct legacy writer contracts, without opening DB/network connections."""
from contextlib import contextmanager
from datetime import date
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import ingest_tushare_moneyflow_ts as moneyflow
from scripts import sync_cninfo_anns_metadata as cninfo
from backend.services.data_integrity import writes


@pytest.mark.parametrize('module,functions', [
    ('ingest_tushare_adj_factor', ['_insert_adj_factor_init', '_upsert_adj_factor']),
    ('ingest_tushare_daily_basic', ['_upsert_daily_basic']),
    ('ingest_tushare_moneyflow_ts', ['_upsert_moneyflow']),
    ('ingest_tushare_stock_basic', ['_insert_stock_basic']),
    ('ingest_tushare_stock_st', ['_upsert_stock_st']),
    ('ingest_tushare_bak_basic', ['_upsert_bak_basic']),
    ('ingest_tushare_cyq', ['upsert_cyq_perf', 'upsert_cyq_chips']),
])
def test_legacy_stock_writers_never_open_db_for_bse_only_payload(monkeypatch, module, functions):
    import importlib
    import dotenv
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda *_a, **_kw: None)
    script = importlib.import_module('scripts.' + module)
    # None is intentional: an unsupported-only payload must return before SQL,
    # marks, factor normalization, or a source retry can happen.
    for function in functions:
        assert getattr(script, function)(None, [{'ts_code': '920296.BJ'}, {'ts_code': 'BJ430047'}]) == 0


@pytest.mark.parametrize('name', ['ingest_incremental', 'ingest_full_daily_raw', 'ingest_full_minute'])
def test_legacy_tdx_collectors_expand_all_to_shsz_and_reject_bse_before_io(monkeypatch, name):
    import importlib
    script = importlib.import_module('scripts.' + name)
    queried = []
    def fetch(_path, params=None):
        queried.append(params['exchange'])
        return {'data': {'codes': ['000001', '600000', '430047', '920296', '200001']}}
    monkeypatch.setattr(script, 'http_get', fetch)
    assert script.fetch_codes(['bj']) == [] and queried == []
    assert script.fetch_codes(['all']) == ['000001.SZ', '600000.SH']
    assert queried == ['sh', 'sz']


class Connection:
    autocommit = False

    @contextmanager
    def cursor(self):
        yield object()


@pytest.mark.parametrize("changed", [False, True])
def test_moneyflow_preserves_finite_values_and_marks_only_actual_changes(monkeypatch, changed):
    statements, markers = [], []
    def execute(_cur, query, values, **_options):
        statements.append((query, values))
        return [(date(2026, 9, 1),)] if changed else []
    monkeypatch.setattr(writes.extras, "execute_values", execute)
    monkeypatch.setattr(writes, "mark_changed", lambda _c, ds, days, **_kw: markers.append((ds, days)))
    row = {"ts_code": "000001.SZ", "trade_date": date(2026, 9, 1),
           "buy_sm_amount": float("nan"), "sell_sm_amount": None, "net_mf_amount": -3.5}
    assert moneyflow._upsert_moneyflow(Connection(), [row]) == 1
    query, values = statements[0]
    assert "COALESCE(EXCLUDED.buy_sm_amount,moneyflow_ts.buy_sm_amount)" in query
    assert "IS DISTINCT FROM" in query and "RETURNING trade_date" in query
    assert values[0][3] is None and values[0][-1] == -3.5
    assert markers == [("moneyflow_ts", [date(2026, 9, 1)] if changed else [])]


def test_cninfo_metadata_only_retry_does_not_dirty_facts(monkeypatch):
    queries, markers = [], []
    def execute(_cur, query, _values, **_options):
        queries.append(query)
        return []
    monkeypatch.setattr(writes.extras, "execute_values", execute)
    monkeypatch.setattr(writes, "mark_changed", lambda _c, ds, days, **_kw: markers.append((ds, days)))
    row = (date(2026, 9, 1), "000001.SZ", "真实名称", "原始标题", "https://example.com/official", None, "pending")
    assert cninfo.upsert_rows(Connection(), [row]) == 1
    assert "IS DISTINCT FROM" in queries[0] and "RETURNING ann_date" in queries[0]
    assert "last_seen_at=NOW()" in queries[1]
    assert markers == [("anns_metadata", [])]


@pytest.mark.parametrize('name', ['bak_basic', 'stock_st'])
def test_legacy_stock_collectors_do_not_report_success_after_commit_failure(monkeypatch, name):
    import importlib
    script = importlib.import_module('scripts.ingest_tushare_' + name)
    class FailingConnection:
        rollbacks = 0
        def commit(self):
            raise OSError('commit failed')
        def rollback(self):
            self.rollbacks += 1
    conn = FailingConnection()
    monkeypatch.setattr(script, '_fetch_' + name + '_for_date', lambda *_a: [])
    monkeypatch.setattr(script, '_upsert_' + name, lambda *_a: 0)
    if hasattr(script, '_get_trading_days'):
        monkeypatch.setattr(script, '_get_trading_days', lambda *_a: [date(2026, 10, 28)])
    monkeypatch.setattr(script, '_update_job_progress', lambda *_a: None)
    with pytest.raises(OSError, match='commit failed'):
        script.run_ingestion(conn, None, 'incremental', date(2026, 10, 28), date(2026, 10, 28), None, 0)
    assert conn.rollbacks == 1


@pytest.mark.parametrize("script", ["scripts/import_minutes_gap.py", "scripts/import_minutes_to_db.py"])
def test_legacy_minute_import_is_side_effect_free_and_uses_shared_db_configuration(monkeypatch, script):
    import runpy
    import sys
    import psycopg2
    from backend.db.pg_pool import _db_cfg
    def forbidden(**_kwargs):
        raise AssertionError("import must not connect to a database")
    monkeypatch.setattr(psycopg2, "connect", forbidden)
    monkeypatch.setattr(sys, "path", list(sys.path))
    namespace = runpy.run_path(script, run_name="legacy_import_test")
    assert namespace["_db_cfg"] is _db_cfg and "DB_CONFIG" not in namespace
    assert callable(namespace["main"])


def test_moneyflow_commit_failure_is_not_returned_as_success(monkeypatch):
    class FailingConnection:
        rollbacks = 0
        def commit(self):
            raise OSError("commit failed")
        def rollback(self):
            self.rollbacks += 1
    conn = FailingConnection()
    monkeypatch.setattr(moneyflow, "_fetch_moneyflow_for_date", lambda *_a: [])
    monkeypatch.setattr(moneyflow, "_upsert_moneyflow", lambda *_a: 0)
    monkeypatch.setattr(moneyflow, "_update_job_progress", lambda *_a: None)
    with pytest.raises(OSError, match="commit failed"):
        moneyflow.run_ingestion(conn, None, "incremental", date(2026, 9, 1), date(2026, 9, 1), None)
    assert conn.rollbacks == 1


def test_cninfo_rollback_failure_is_loud_and_connection_is_closed(monkeypatch):
    class FailingConnection:
        closed = False
        def rollback(self):
            raise OSError("connection lost")
        def close(self):
            self.closed = True
    conn = FailingConnection()
    monkeypatch.setattr(cninfo, "get_conn", lambda **_kw: conn)
    def failed_scan(**_kw):
        raise RuntimeError("page failed")
    monkeypatch.setattr(cninfo, "fetch_cninfo_date", failed_scan)
    with pytest.raises(RuntimeError, match="rollback failed"):
        cninfo.sync_one_date(date(2026, 9, 1), 0, 1, False)
    assert conn.closed


@pytest.mark.parametrize("changed", [False, True])
def test_index_writer_preserves_values_and_tracks_actual_dates(monkeypatch, changed):
    from scripts import ingest_tushare_index_daily as index_daily
    statements, markers = [], []
    def execute(_cur, query, values, **_options):
        statements.append((query, values))
        return [(date(2026, 10, 27),)] if changed else []
    monkeypatch.setattr(writes.extras, "execute_values", execute)
    monkeypatch.setattr(writes, "mark_changed", lambda _c, ds, days, **_kw: markers.append((ds, days)))
    assert index_daily._upsert_index_daily(Connection(), [{"ts_code": "000300.SH", "trade_date": date(2026, 10, 27),
        "open": 2, "close": float("nan")}]) == 1
    query, values = statements[0]
    assert "COALESCE(EXCLUDED.close,index_daily.close)" in query
    assert "IS DISTINCT FROM" in query and "RETURNING trade_date" in query
    assert values[0][2] is None
    assert markers == [("index_daily", [date(2026, 10, 27)] if changed else [])]


@pytest.mark.parametrize("complete", [False, True])
def test_index_completed_scan_records_dates_without_inventing_empty_quotes(monkeypatch, complete):
    from scripts import ingest_tushare_index_daily as index_daily
    from backend.services.data_refresh_audit import DataRefreshAuditRepository
    first, second = date(2026, 10, 26), date(2026, 10, 27)
    records = []
    class Cursor:
        def execute(self, query, _params=()):
            self.calendar = "trading_calendar" in query
        def fetchall(self):
            return [(first,), (second,)] if self.calendar else [("000300.SH",), ("000985.CSI",)]
    class Conn(Connection):
        @contextmanager
        def cursor(self):
            yield Cursor()
    monkeypatch.setattr(DataRefreshAuditRepository, "record_success", lambda _self, **kw: records.append(kw))
    codes = [("000300.SH", "SSE")]+([("000985.CSI", "CSI")] if complete else [])
    index_daily._record_index_scans(Conn(), {first: [{"ts_code": "000300.SH", "trade_date": first}]},
        start_date=first, end_date=second, codes=codes, markets=None, job_id=None)
    assert [r["trade_date"] for r in records] == [first, second]
    assert all(r["metadata"]["provider_query_scope_complete"] is complete for r in records)
    assert records[0]["metadata"]["provider_scan"]["entries"]
    assert records[1]["metadata"]["provider_scan"]["entries"] == []
    assert records[1]["quality_status"] == "empty_valid" and records[1]["row_count"] == 0


@pytest.mark.parametrize("failure", ["source", "truncated", "wrong_key", "commit", "none"])
def test_index_scan_published_only_after_all_queries_and_commits_succeed(monkeypatch, failure):
    from scripts import ingest_tushare_index_daily as index_daily
    import pandas as pd
    calls, writes_seen = [], []
    day = date(2026, 10, 27)
    class Cursor:
        def execute(self, *_a):
            pass
    class Conn(Connection):
        rollbacks = 0
        @contextmanager
        def cursor(self):
            yield Cursor()
        def commit(self):
            if failure == "commit":
                raise OSError("commit failed")
        def rollback(self):
            self.rollbacks += 1
    class Provider:
        def index_daily(self, **_kw):
            if failure == "source":
                raise RuntimeError("provider failure")
            code = "WRONG" if failure == "wrong_key" else "000300.SH"
            return pd.DataFrame([{"ts_code": code, "trade_date": "20261027"}]* (8000 if failure == "truncated" else 1))
    monkeypatch.setattr(index_daily, "_fetch_index_codes", lambda *_a: [("000300.SH", "SSE")])
    monkeypatch.setattr(index_daily, "_log", lambda *_a: None)
    monkeypatch.setattr(index_daily, "_upsert_index_daily", lambda *_a: writes_seen.append(True) or 1)
    monkeypatch.setattr(index_daily, "_record_index_scans", lambda _conn, data, **_kw: calls.append(data))
    conn = Conn()
    args = dict(conn=conn, pro=Provider(), mode="init", start_date=day, end_date=day, markets=None, job_id=None, batch_sleep=0)
    if failure == "none":
        assert index_daily.run_ingestion(**args)["done_ts"] == 1 and len(calls) == 1
    else:
        with pytest.raises((RuntimeError, ValueError, OSError)):
            index_daily.run_ingestion(**args)
        assert not calls
        if failure != "commit":
            assert not writes_seen


def test_moneyflow_absolute_path_can_load_shared_contracts(tmp_path):
    script = Path(moneyflow.__file__).resolve()
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run([sys.executable, "-c",
        "import runpy; m=runpy.run_path("+repr(str(script))+"); "
        "assert m['_upsert_moneyflow'](None,[{'ts_code':'','trade_date':None}])==0"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("failed_days", [0, 1])
def test_main_owns_connection_lifetime_not_outer_transaction(monkeypatch, failed_days):
    class Connection:
        autocommit = False
        closed = False

        def __enter__(self):
            raise AssertionError("connection context starts an unwanted outer transaction")

        def close(self):
            self.closed = True

    conn = Connection()
    args = SimpleNamespace(mode="incremental", start_date="2026-10-28", end_date="2026-10-28",
                           job_id=None, bulk_session_tune=False, truncate=False)
    monkeypatch.setattr(moneyflow, "parse_args", lambda: args)
    monkeypatch.setattr(moneyflow.psycopg2, "connect", lambda **_kwargs: conn)
    monkeypatch.setattr(moneyflow, "pro_api", lambda: object())
    monkeypatch.setattr(moneyflow, "_create_job", lambda *_args: "job")
    monkeypatch.setattr(moneyflow, "_log", lambda *_args: None)
    monkeypatch.setattr(moneyflow, "run_ingestion", lambda *_args: {"failed_days": failed_days})
    finished = []
    monkeypatch.setattr(moneyflow, "_finish_job", lambda *args: finished.append(args[2]))
    if failed_days:
        with pytest.raises(SystemExit) as failure:
            moneyflow.main()
        assert failure.value.code == 1
    else:
        moneyflow.main()
    assert finished == ["failed" if failed_days else "success"]
    assert conn.closed
