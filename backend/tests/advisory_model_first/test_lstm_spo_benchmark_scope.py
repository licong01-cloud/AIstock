"""Only the consumed close slice may certify the LSTM benchmark, with revision pins."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_spo_shadow_source_v1 as source
from backend.services.data_integrity.catalog import RULE_VERSION, SCOPE_VERSION
from backend.services.data_integrity.inputs import InputReadinessError


@pytest.fixture
def db(monkeypatch):
    days = list(pd.bdate_range("2026-09-01", periods=20).date)

    class Connection:
        calendar = days
        quotes = [(day, source.BENCHMARK, 4000. + i) for i, day in enumerate(days)]

        @contextmanager
        def cursor(self):
            yield self

        def execute(self, sql, params):
            if "trading_calendar" in sql:
                assert params == (str(days[-1]),)
                self.result = [(d,) for d in reversed(self.calendar)]
            else:
                assert "SELECT trade_date,ts_code,close" in sql and "LIMIT 21" in sql
                assert params == (source.BENCHMARK, list(map(str, days)))
                self.result = [r for r in self.quotes if r[1] == source.BENCHMARK]

        def fetchall(self):
            return self.result

    conn = Connection()
    rows = [dict(dataset=ds, trade_date=str(day), data_revision=7, validated_revision=7,
                 validation_status="INCOMPLETE" if ds == "index_daily" else "VALID",
                 validation_id="v7", scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
            for day in days for ds in source.EXTRA_DATASETS]
    monkeypatch.setattr(source, "partition_pins", lambda *args: deepcopy(rows))
    return SimpleNamespace(conn=conn, days=days, pins=rows)


def test_other_index_failures_preserved_while_consumed_close_is_valid(db):
    db.conn.quotes = [*db.conn.quotes, (db.days[0], "999999.SH", None)]
    before = deepcopy(db.pins)
    receipt = source.extra_input_capture(db.conn, db.days)
    assert db.pins == before
    benchmark = receipt["benchmark"]
    assert benchmark["valid"] == benchmark["expected"] == 20
    assert benchmark["instrument"] == "000300.SH" and benchmark["fields"] == ["close"]
    assert benchmark["whole_index_partition_certified"] is False
    assert all(r["global_validation_status"] == "INCOMPLETE" for r in benchmark["partition_refs"])
    assert all(r["data_revision"] == 7 for r in benchmark["partition_refs"])
    assert len(receipt["partition_refs"]) == 40


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), float("-inf"), 0., -1., True, "4000"])
def test_consumed_close_must_be_finite_positive_number(db, bad):
    day, code, _ = db.conn.quotes[0]
    db.conn.quotes = [(day, code, bad), *db.conn.quotes[1:]]
    with pytest.raises(InputReadinessError, match="缺行、重复或无效"):
        source.extra_input_capture(db.conn, db.days)


@pytest.mark.parametrize("change", ["missing", "duplicate", "duplicate_replaces_day", "foreign"])
def test_consumed_keys_must_match_every_session_once(db, change):
    quotes = list(db.conn.quotes)
    if change == "missing":
        quotes.pop()
    elif change == "duplicate":
        quotes.append(quotes[0])
    elif change == "duplicate_replaces_day":
        quotes[-1] = quotes[0]
    else:
        quotes[-1] = (db.days[-1] + timedelta(days=1), source.BENCHMARK, 4000.)
    db.conn.quotes = quotes
    with pytest.raises(InputReadinessError):
        source.extra_input_capture(db.conn, db.days)


@pytest.mark.parametrize("change", ["missing_pin", "duplicate_pin", "revision_unknown", "old_rule", "stock_invalid"])
def test_current_revision_and_stock_contracts_are_required(db, change):
    index = next(r for r in db.pins if r["dataset"] == "index_daily")
    if change == "missing_pin":
        db.pins.remove(index)
    elif change == "duplicate_pin":
        db.pins.append(dict(index))
    elif change == "revision_unknown":
        index["data_revision"] = None
    elif change == "old_rule":
        index["rule_version"] = "obsolete"
    else:
        db.pins[0]["validation_status"] = "UNVERIFIED"
    with pytest.raises(InputReadinessError):
        source.extra_input_capture(db.conn, db.days)


def test_window_is_the_authoritative_twenty_sessions_ending_on_d(db):
    db.conn.calendar = [db.days[0] - timedelta(days=1), *db.days[1:]]
    with pytest.raises(InputReadinessError, match="D对应"):
        source.extra_input_capture(db.conn, db.days)


@pytest.mark.parametrize("change", ["none", "revision", "validation", "value", "missing", "tampered_window"])
def test_post_compute_rechecks_same_scope_values_and_revision(db, monkeypatch, change):
    extra = source.extra_input_capture(db.conn, db.days)
    baseline_checks = []
    monkeypatch.setattr(source, "verify_current", lambda conn, bindings: baseline_checks.append(bindings))
    class Session:
        @contextmanager
        def connection(self):
            yield db.conn

        def close(self):
            pass

    reader = source.LstmSpoShadowSource(session_factory=Session)
    if change == "revision":
        next(r for r in db.pins if r["dataset"] == "index_daily")["data_revision"] += 1
    elif change == "validation":
        next(r for r in db.pins if r["dataset"] == "index_daily")["validation_id"] = "new"
    elif change == "value":
        day, code, close = db.conn.quotes[0]
        db.conn.quotes = [(day, code, close + 1), *db.conn.quotes[1:]]
    elif change == "missing":
        db.conn.quotes = db.conn.quotes[:-1]
    elif change == "tampered_window":
        extra["session_dates"][0] = "2000-01-01"
    receipt = {"price": {"input_readiness": extra}, "baseline_input_bindings": {"original": "retained"}}
    if change == "none":
        reader.verify_capture(receipt)
    else:
        with pytest.raises(InputReadinessError):
            reader.verify_capture(receipt)
    assert baseline_checks == [{"original": "retained"}]
