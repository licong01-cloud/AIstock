from __future__ import annotations

import datetime as dt
import inspect
from pathlib import Path

import pytest

from scripts import repair_minute_via_minute_api as repair


class DummyCursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class DummyConn:
    def __init__(self, rows=None):
        self.cursor_obj = DummyCursor(rows)

    def cursor(self):
        return self.cursor_obj


def _bar(target: dt.date, minute: int = 31):
    return {
        "Time": f"{target.isoformat()} 09:{minute:02d}:00",
        "Open": 10,
        "High": 11,
        "Low": 9,
        "Close": 10,
        "Volume": 100,
        "Amount": 1000,
    }


def test_normalize_target_bars_canonicalizes_today_and_rejects_other_dates():
    target = dt.date(2026, 7, 23)

    rows = repair.normalize_target_bars(target, [_bar(target)])

    assert rows[0]["TradeTime"] == "2026-07-23T09:31:00+08:00"
    with pytest.raises(repair.RepairError, match="outside the authorized date"):
        repair.normalize_target_bars(target, [_bar(dt.date(2026, 7, 22))])


def test_fetch_minute_single_day_uses_true_ohlc_endpoint_and_filters_history(monkeypatch):
    target = dt.date(2026, 7, 23)
    calls = []

    def fake_http_get(api_base, path, params):
        calls.append((api_base, path, params))
        return {"code": 0, "data": {"list": [_bar(dt.date(2026, 7, 22)), _bar(target)]}}

    monkeypatch.setattr(repair, "_http_get", fake_http_get)

    rows = repair.fetch_minute_single_day("http://tdx", "000001.SZ", target)

    assert calls == [("http://tdx", "/api/kline-all/tdx", {"code": "000001", "type": "minute1"})]
    assert len(rows) == 1
    assert rows[0]["TradeTime"] == "2026-07-23T09:31:00+08:00"


def test_fetch_minute_single_day_accepts_explicit_empty_source(monkeypatch):
    target = dt.date(2026, 7, 23)
    monkeypatch.setattr(
        repair,
        "_http_get",
        lambda *_args, **_kwargs: {"code": 0, "data": {"count": 0, "list": None}},
    )

    assert repair.fetch_minute_single_day("http://tdx", "000004.SZ", target) == []


def test_fetch_current_stock_codes_filters_non_stock_tdx_directory_rows(monkeypatch):
    monkeypatch.setattr(
        repair,
        "_http_get",
        lambda *_args, **_kwargs: {
            "code": 0,
            "data": {
                "codes": [
                    {"code": "000001", "exchange": "sz"},
                    {"code": "600000", "exchange": "sh"},
                    {"code": "920001", "exchange": "bj"},
                    {"code": "302132", "exchange": "sz"},
                    {"code": "070422", "exchange": "sz"},
                ]
            },
        },
    )

    assert repair.fetch_current_stock_codes("http://tdx") == [
        "000001.SZ",
        "302132.SZ",
        "600000.SH",
        "920001.BJ",
    ]


def test_database_config_accepts_only_existing_local_dev_profile(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    values = {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5433",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_DEV_USER": "dev_user",
        "TDX_DB_DEV_PASSWORD": "configured",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    config = repair._database_config("dev", env_file)

    assert config["host"] == "127.0.0.1"
    assert config["port"] == 5433
    assert config["dbname"] == "aistock_dev"
    monkeypatch.setenv("TDX_DB_DEV_PORT", "5432")
    with pytest.raises(repair.RepairError, match="not the existing local DEV database"):
        repair._database_config("dev", env_file)


def test_main_refuses_any_date_other_than_china_today(monkeypatch):
    monkeypatch.setattr(repair, "china_today", lambda: dt.date(2026, 7, 23))

    with pytest.raises(repair.RepairError, match="limited to today"):
        repair.main(["--target-db", "dev", "--date", "2026-07-22"])


def test_production_apply_requires_exact_target_confirmation(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    monkeypatch.setattr(repair, "china_today", lambda: dt.date(2026, 7, 23))
    for suffix, value in {
        "HOST": "127.0.0.1",
        "PORT": "5432",
        "NAME": "aistock",
        "USER": "prod_user",
        "PASSWORD": "configured",
    }.items():
        monkeypatch.setenv(f"TDX_DB_{suffix}", value)

    with pytest.raises(repair.RepairError, match="production confirmation mismatch"):
        repair.main(
            [
                "--target-db",
                "production",
                "--date",
                "2026-07-23",
                "--env-file",
                str(env_file),
                "--apply",
            ]
        )


def test_source_contains_no_destructive_table_statement():
    source = inspect.getsource(repair).upper()

    assert "DELETE FROM" not in source
    assert "TRUNCATE " not in source
    assert "DROP TABLE" not in source


def test_historical_snapshot_is_scoped_to_latest_existing_historical_day(monkeypatch):
    target = dt.date(2026, 7, 23)
    cursor = DummyCursor(
        [
            (
                100,
                1,
                2,
                3,
                4,
                5,
                6,
            )
        ]
    )
    conn = DummyConn()
    conn.cursor_obj = cursor
    monkeypatch.setattr(repair, "_latest_historical_data_date", lambda _conn, _date: dt.date(2026, 7, 22))

    snapshot = repair.historical_snapshot(conn, target)

    assert snapshot.trade_date == "2026-07-22"
    assert snapshot.row_count == 100
    assert cursor.executed[0][1] == (dt.date(2026, 7, 22), dt.date(2026, 7, 22))


def test_repair_plan_only_performs_no_job_or_upsert(monkeypatch):
    target = dt.date(2026, 7, 23)
    monkeypatch.setattr(repair, "fetch_current_stock_codes", lambda _api: ["000001.SZ"])
    monkeypatch.setattr(
        repair,
        "inspect_completeness",
        lambda _conn, _date, _expected: {
            "expected_codes": ["000001.SZ"],
            "expected_code_count": 1,
            "gap_count": 1,
            "gap_samples": [{"ts_code": "000001.SZ"}],
        },
    )
    monkeypatch.setattr(
        repair,
        "historical_snapshot",
        lambda _conn, _date: repair.HistoricalSnapshot("2026-07-22", 240, 1, 1, 1, 1, 1, 1),
    )
    monkeypatch.setattr(
        repair.incremental,
        "create_job",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("plan-only must not create a job")),
    )

    result = repair.repair_today(
        DummyConn(),
        target_date=target,
        api_base="http://127.0.0.1:19080",
        apply=False,
        workers=1,
        batch_size=10,
        target_db="dev",
    )

    assert result["status"] == "plan_only"
    assert result["write_mode"] == "upsert_only"
    assert result["initial_gap_count"] == 1


def test_repair_apply_upserts_every_expected_code_and_records_success(monkeypatch):
    target = dt.date(2026, 7, 23)
    expected = ["000001.SZ", "600000.SH"]
    inspections = iter(
        [
            {"expected_codes": expected, "expected_code_count": 2, "gap_count": 2, "gap_samples": []},
            {"expected_codes": expected, "expected_code_count": 2, "gap_count": 0, "gap_samples": []},
        ]
    )
    snapshot = repair.HistoricalSnapshot("2026-07-22", 480, 1, 1, 1, 1, 1, 1)
    monkeypatch.setattr(repair, "fetch_current_stock_codes", lambda _api: expected)
    monkeypatch.setattr(repair, "inspect_completeness", lambda *_args: next(inspections))
    monkeypatch.setattr(repair, "historical_snapshot", lambda *_args: snapshot)
    monkeypatch.setattr(
        repair,
        "_fetch_batch",
        lambda *_args: iter((code, [_bar(target)], None) for code in expected),
    )
    monkeypatch.setattr(repair, "_target_stats", lambda *_args: (480, None))
    monkeypatch.setattr(repair, "_target_code_counts", lambda *_args: {code: 1 for code in expected})
    monkeypatch.setattr(repair.incremental, "find_minute_day_gaps", lambda *_args: [])
    created_job_types = []
    monkeypatch.setattr(
        repair.incremental,
        "create_job",
        lambda _conn, job_type, _summary: (created_job_types.append(job_type) or "job-id"),
    )
    monkeypatch.setattr(repair.incremental, "create_run", lambda *_args, **_kwargs: "run-id")
    monkeypatch.setattr(repair.incremental, "update_job_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repair.incremental, "finish_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repair.incremental, "finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repair.incremental, "log_error", lambda *_args, **_kwargs: None)
    touched = []
    monkeypatch.setattr(
        repair.incremental,
        "upsert_minute",
        lambda _conn, code, day, bars: (touched.append((code, day, len(bars))) or (len(bars), None)),
    )

    audit_calls = []

    class Audit:
        def record_success(self, **kwargs):
            audit_calls.append(("success", kwargs))

        def record_failure(self, **kwargs):
            audit_calls.append(("failure", kwargs))

    monkeypatch.setattr(repair, "DataRefreshAuditRepository", Audit)

    result = repair.repair_today(
        DummyConn(),
        target_date=target,
        api_base="http://127.0.0.1:19080",
        apply=True,
        workers=2,
        batch_size=10,
        target_db="dev",
    )

    assert result["status"] == "success"
    assert created_job_types == ["incremental"]
    assert touched == [("000001.SZ", target, 1), ("600000.SH", target, 1)]
    assert audit_calls[0][0] == "success"
    assert audit_calls[0][1]["coverage_ratio"] == 1.0
    assert audit_calls[0][1]["metadata"]["upsert_only"] is True
