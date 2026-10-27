"""Direct legacy writer contracts, without opening DB/network connections."""
from contextlib import contextmanager
from datetime import date

import pytest

from scripts import ingest_tushare_moneyflow_ts as moneyflow
from scripts import sync_cninfo_anns_metadata as cninfo
from backend.services.data_integrity import writes


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
