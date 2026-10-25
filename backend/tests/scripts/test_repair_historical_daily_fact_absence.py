"""Bounded historical daily repair must not reinterpret or overwrite facts."""
import hashlib
import json

import pytest

from scripts.repair_historical_daily_fact_absence import (
    COLUMNS, build_sql, load_capture, normalize_row,
)


def row(**changes):
    return {"ts_code": "000585.SZ", "trade_date": "20180801", "open": 2.51,
            "high": 2.61, "low": 2.5, "close": 2.55, "pre_close": 2.54,
            "vol": 121.23, "amount": 123.4567, **changes}


def test_units_preserve_exact_source_shares_and_native_li():
    value = dict(zip(COLUMNS, normalize_row(row(), "a" * 64)))
    assert value["open_li"] == 2510
    assert value["volume_hand"] == 121
    assert value["volume_shares"] == 12123
    assert value["amount_li"] == 123456700
    assert value["volume_shares_source"] == "tushare_daily"
    assert value["source"] == "tushare_api"


@pytest.mark.parametrize("change", [
    {"open": None}, {"close": float("nan")}, {"amount": float("inf")},
    {"vol": -1}, {"vol": 12.345}, {"high": 2.4}, {"low": 2.6},
    {"open": True}, {"ts_code": "000585.SZ';DELETE"},
    {"trade_date": "20261001"}, {"trade_date": "20180731"},
    {"close": 2.5555}, {"vol": 0, "amount": 0},
])
def test_invalid_source_is_not_defaulted(change):
    with pytest.raises(ValueError):
        normalize_row(row(**change), "a" * 64)


def capture(tmp_path, source_rows):
    source = {"api_name": "daily", "request": {"ts_code": "000585.SZ", "start_date": "20180801", "end_date": "20260930"},
              "response": {"code": 0, "data": {"fields": list(row()),
                  "items": [[item[key] for key in row()] for item in source_rows]}}}
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(source), encoding="utf8")
    pin = hashlib.sha256(source_path.read_bytes()).hexdigest()
    receipt = {"schema_version": "aistock_daily_source_absence_provider_readback_v1", "database_write": False,
               "actual_traded_rows": len(source_rows), "symbols": [{"symbol": "000585.SZ", "provider_code": 0,
                   "source_path": str(source_path), "source_sha256": pin, "actual_traded_rows": len(source_rows)}]}
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(receipt), encoding="utf8")
    return path, source_path


def test_capture_pins_rows_and_source_identity(tmp_path):
    receipt, _ = capture(tmp_path, [row()])
    facts = load_capture(receipt)
    assert len(facts) == 1
    assert dict(zip(COLUMNS, facts[0]))["ts_code"] == "000585.SZ"


def test_changed_source_bytes_fail_closed(tmp_path):
    receipt, source = capture(tmp_path, [row()])
    source.write_text("{}", encoding="utf8")
    with pytest.raises(ValueError, match="pin"):
        load_capture(receipt)


def test_duplicate_source_key_fails_closed(tmp_path):
    receipt, _ = capture(tmp_path, [row(), row()])
    with pytest.raises(ValueError, match="duplicate"):
        load_capture(receipt)


def test_new_key_only_sql_is_database_bound_conflict_guarded_and_verified():
    sql = build_sql([normalize_row(row(), "a" * 64)], database="aistock")
    assert "current_database() <> 'aistock'" in sql
    assert "ON CONFLICT (ts_code,trade_date) DO NOTHING" in sql
    assert "existing raw daily fact conflicts" in sql
    assert "readback is incomplete or changed" in sql
    assert "::bpchar" in sql
    assert "UPDATE " not in sql and "DELETE " not in sql
    assert "CREATE TABLE" not in sql and "ALTER TABLE" not in sql


def test_sql_does_not_accept_unknown_database_or_empty_facts():
    with pytest.raises(ValueError):
        build_sql([], database="aistock")
    with pytest.raises(ValueError):
        build_sql([normalize_row(row(), "a" * 64)], database="other")
