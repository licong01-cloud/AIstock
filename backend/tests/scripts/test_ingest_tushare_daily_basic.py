from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import scripts.ingest_tushare_daily_basic as ingestion


NUMERIC_FIELDS = (
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)


def _provider_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20260904", "20260904"],
            **{field: [1.0, 2.0] for field in NUMERIC_FIELDS},
        }
    )


class _Provider:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.calls: list[dict[str, Any]] = []

    def daily_basic(self, **kwargs: Any) -> pd.DataFrame:
        self.calls.append(kwargs)
        return self.frame.copy()


def test_fetch_explicitly_requests_the_declared_daily_basic_fields() -> None:
    provider = _Provider(_provider_frame())
    rows = ingestion._fetch_daily_basic_for_date(provider, dt.date(2026, 9, 4))
    assert len(rows) == 2
    assert set(provider.calls[0]["fields"].split(",")) == {
        "ts_code", "trade_date", *NUMERIC_FIELDS
    }


@pytest.mark.parametrize("field", ["turnover_rate_f", "free_share"])
@pytest.mark.parametrize("missing", [None, np.nan, np.inf, Decimal("NaN")])
def test_fetch_rejects_whole_snapshot_missing_free_float_fields(field, missing) -> None:
    frame = _provider_frame()
    frame[field] = missing
    with pytest.raises(ValueError, match="DAILY_BASIC_SNAPSHOT_INCOMPLETE.*" + field):
        ingestion._fetch_daily_basic_for_date(_Provider(frame), dt.date(2026, 9, 4))


def test_fetch_keeps_normal_missing_rows_and_zero_turnover() -> None:
    frame = _provider_frame()
    frame.loc[0, ["turnover_rate_f", "free_share"]] = np.nan
    frame.loc[1, "turnover_rate_f"] = 0.0
    frame["pe"] = np.nan
    rows = ingestion._fetch_daily_basic_for_date(_Provider(frame), dt.date(2026, 9, 4))
    assert [row["ts_code"] for row in rows] == frame["ts_code"].tolist()
    assert pd.isna(rows[0]["turnover_rate_f"])
    assert rows[1]["turnover_rate_f"] == 0.0
    assert all(pd.isna(row["pe"]) for row in rows)


def test_fetch_rejects_missing_declared_column() -> None:
    frame = _provider_frame().drop(columns=["free_share"])
    with pytest.raises(ValueError, match="missing_columns=.*free_share"):
        ingestion._fetch_daily_basic_for_date(_Provider(frame), dt.date(2026, 9, 4))


def test_free_float_completeness_is_checked_after_all_pages(monkeypatch) -> None:
    first = pd.concat([_provider_frame()] * 3000, ignore_index=True)
    first["ts_code"] = [f"{index:06d}.SZ" for index in range(6000)]
    first[["turnover_rate_f", "free_share"]] = np.nan
    last = _provider_frame().iloc[:1].copy()
    last["ts_code"] = "600000.SH"
    pages = iter([first, last])
    calls = []

    def fetch(**kwargs):
        calls.append(kwargs)
        return next(pages)

    monkeypatch.setattr(ingestion.time, "sleep", lambda *args: None)
    rows = ingestion._fetch_daily_basic_for_date(
        SimpleNamespace(daily_basic=fetch), dt.date(2026, 9, 4)
    )
    assert len(rows) == 6001
    assert [call["offset"] for call in calls] == [0, 6000]
    assert pd.isna(rows[0]["free_share"])
    assert rows[-1]["free_share"] == 1.0


@pytest.mark.parametrize("bad_date", ["20260907", None, "invalid"])
def test_fetch_rejects_wrong_or_missing_source_date(bad_date) -> None:
    frame = _provider_frame()
    frame.loc[0, "trade_date"] = bad_date
    with pytest.raises(ValueError, match="DAILY_BASIC_SNAPSHOT_IDENTITY_MISMATCH"):
        ingestion._fetch_daily_basic_for_date(_Provider(frame), dt.date(2026, 9, 4))


@pytest.mark.parametrize("bad_code", ["", "  ", None, 100])
def test_fetch_rejects_invalid_source_key(bad_code) -> None:
    frame = _provider_frame()
    frame.loc[0, "ts_code"] = bad_code
    with pytest.raises(ValueError, match="DAILY_BASIC_SNAPSHOT_IDENTITY_MISMATCH"):
        ingestion._fetch_daily_basic_for_date(_Provider(frame), dt.date(2026, 9, 4))


def test_fetch_rejects_duplicate_source_keys() -> None:
    frame = _provider_frame()
    frame.loc[1, "ts_code"] = frame.loc[0, "ts_code"]
    with pytest.raises(ValueError, match="DAILY_BASIC_SNAPSHOT_IDENTITY_MISMATCH"):
        ingestion._fetch_daily_basic_for_date(_Provider(frame), dt.date(2026, 9, 4))


def test_init_records_failure_dates_without_changing_explicit_range_semantics(monkeypatch) -> None:
    calls = []

    def fetch(_pro, day):
        calls.append(day)
        if day == dt.date(2026, 9, 1):
            raise ValueError("DAILY_BASIC_SNAPSHOT_INCOMPLETE")
        return []

    monkeypatch.setattr(ingestion, "_fetch_daily_basic_for_date", fetch)
    monkeypatch.setattr(ingestion, "_is_trading_day", lambda _conn, _day: False)
    monkeypatch.setattr(ingestion, "_log", lambda *args: None)
    monkeypatch.setattr(ingestion, "_update_job_progress", lambda *args: None)
    stats = ingestion.run_ingestion(
        SimpleNamespace(commit=lambda: None), object(), "init",
        dt.date(2026, 9, 1), dt.date(2026, 9, 2), uuid.uuid4(), 0,
    )
    assert calls == [dt.date(2026, 9, 1), dt.date(2026, 9, 2)]
    assert stats["failed_dates"] == ["2026-09-01"]
    assert stats["deferred_days"] == 0


def test_incremental_does_not_advance_past_an_incomplete_day(monkeypatch) -> None:
    calls = []
    writes = []

    def fetch(_pro, day):
        calls.append(day)
        if day == dt.date(2026, 9, 1):
            raise ValueError("DAILY_BASIC_SNAPSHOT_INCOMPLETE")
        return [{"trade_date": day, "turnover_rate_f": 1.0}]

    monkeypatch.setattr(ingestion, "_fetch_daily_basic_for_date", fetch)
    monkeypatch.setattr(ingestion, "_upsert_daily_basic", lambda *args: writes.append(args) or 1)
    monkeypatch.setattr(ingestion, "_log", lambda *args: None)
    monkeypatch.setattr(ingestion, "_update_job_progress", lambda *args: None)
    stats = ingestion.run_ingestion(
        SimpleNamespace(commit=lambda: None), object(), "incremental",
        dt.date(2026, 8, 31), dt.date(2026, 9, 2), uuid.uuid4(), 0,
    )
    assert calls == [dt.date(2026, 8, 31), dt.date(2026, 9, 1)]
    assert len(writes) == 1
    assert stats["success_days"] == stats["failed_days"] == 1
    assert stats["failed_dates"] == ["2026-09-01"]
    assert stats["deferred_days"] == 1


def test_incremental_rejects_empty_trading_day_and_does_not_advance(monkeypatch) -> None:
    calls = []
    writes = []
    monkeypatch.setattr(
        ingestion,
        "_fetch_daily_basic_for_date",
        lambda _pro, day: calls.append(day) or [],
    )
    monkeypatch.setattr(ingestion, "_is_trading_day", lambda _conn, _day: True)
    monkeypatch.setattr(ingestion, "_upsert_daily_basic", lambda *args: writes.append(args) or 0)
    monkeypatch.setattr(ingestion, "_log", lambda *args: None)
    monkeypatch.setattr(ingestion, "_update_job_progress", lambda *args: None)

    stats = ingestion.run_ingestion(
        SimpleNamespace(commit=lambda: None), object(), "incremental",
        dt.date(2026, 9, 7), dt.date(2026, 9, 8), uuid.uuid4(), 0,
    )

    assert calls == [dt.date(2026, 9, 7)]
    assert writes == []
    assert stats["success_days"] == 0
    assert stats["failed_dates"] == ["2026-09-07"]
    assert stats["deferred_days"] == 1


def test_incomplete_snapshot_is_not_written_or_counted_as_success(monkeypatch) -> None:
    frame = _provider_frame()
    frame["turnover_rate_f"] = np.nan
    writes = []
    logs = []
    monkeypatch.setattr(ingestion, "_upsert_daily_basic", lambda *args: writes.append(args))
    monkeypatch.setattr(ingestion, "_log", lambda *args: logs.append(args[-1]))
    monkeypatch.setattr(ingestion, "_update_job_progress", lambda *args: None)
    stats = ingestion.run_ingestion(
        SimpleNamespace(commit=lambda: None), _Provider(frame), "incremental",
        dt.date(2026, 9, 4), dt.date(2026, 9, 4), uuid.uuid4(), 0,
    )
    assert writes == []
    assert stats["success_days"] == 0
    assert stats["failed_days"] == 1
    assert any("DAILY_BASIC_SNAPSHOT_INCOMPLETE" in message for message in logs)


@pytest.mark.parametrize("failed_days", [0, 1])
def test_main_exit_code_matches_failed_day_status(monkeypatch, failed_days) -> None:
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    args = SimpleNamespace(
        mode="init", start_date="2026-09-04", end_date="2026-09-04",
        truncate=False, job_id=None, batch_sleep=0,
    )
    terminal = []
    monkeypatch.setattr(ingestion, "parse_args", lambda: args)
    monkeypatch.setattr(ingestion.psycopg2, "connect", lambda **kwargs: Connection())
    monkeypatch.setattr(ingestion, "pro_api", lambda: object())
    monkeypatch.setattr(ingestion, "_create_job", lambda *args: uuid.uuid4())
    monkeypatch.setattr(ingestion, "_log", lambda *args: None)
    monkeypatch.setattr(ingestion, "run_ingestion", lambda *args: {"failed_days": failed_days})
    monkeypatch.setattr(ingestion, "_finish_job", lambda *args: terminal.append(args[2]))
    if failed_days:
        with pytest.raises(SystemExit) as exc:
            ingestion.main()
        assert exc.value.code == 1
    else:
        ingestion.main()
    assert terminal == ["failed" if failed_days else "success"]


class _Cursor:
    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _Connection:
    def cursor(self) -> _Cursor:
        return _Cursor()


class _CalendarCursor(_Cursor):
    def __init__(self, row: tuple[bool] | None) -> None:
        self.row = row
        self.executed: list[tuple[str, tuple[dt.date]]] = []

    def execute(self, sql: str, params: tuple[dt.date]) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> tuple[bool] | None:
        return self.row


class _CalendarConnection:
    def __init__(self, row: tuple[bool] | None) -> None:
        self.calendar_cursor = _CalendarCursor(row)

    def cursor(self) -> _CalendarCursor:
        return self.calendar_cursor


@pytest.mark.parametrize(("row", "expected"), [((True,), True), ((False,), False)])
def test_is_trading_day_uses_authoritative_calendar(row, expected) -> None:
    conn = _CalendarConnection(row)
    assert ingestion._is_trading_day(conn, dt.date(2026, 9, 7)) is expected
    assert conn.calendar_cursor.executed[0][1] == (dt.date(2026, 9, 7),)


def test_is_trading_day_rejects_missing_calendar_identity() -> None:
    with pytest.raises(ingestion.DailyBasicIngestionError, match="calendar identity is unavailable"):
        ingestion._is_trading_day(_CalendarConnection(None), dt.date(2026, 9, 7))


def test_parse_ymd_only_swallows_expected_date_errors() -> None:
    class _UnexpectedStringFailure:
        def __str__(self) -> str:
            raise RuntimeError("unexpected string conversion failure")

    assert ingestion._parse_ymd("20260616") == dt.date(2026, 6, 16)
    assert ingestion._parse_ymd("not-a-date") is None
    with pytest.raises(RuntimeError, match="unexpected string conversion failure"):
        ingestion._parse_ymd(_UnexpectedStringFailure())


def test_run_ingestion_reports_progress_rollback_and_log_failures(monkeypatch: Any) -> None:
    class _BrokenProgressConnection:
        def rollback(self) -> None:
            raise RuntimeError("rollback failed")

    trade_date = dt.date(2026, 6, 16)
    monkeypatch.setattr(ingestion, "_date_range", lambda *_args: [trade_date])
    monkeypatch.setattr(ingestion, "_fetch_daily_basic_for_date", lambda *_args: [])
    monkeypatch.setattr(ingestion, "_is_trading_day", lambda _conn, _day: False)
    monkeypatch.setattr(ingestion, "_upsert_daily_basic", lambda *_args: 0)

    def fail_progress(*_args: object) -> None:
        raise RuntimeError("progress failed")

    def fail_log(*_args: object) -> None:
        raise RuntimeError("log failed")

    monkeypatch.setattr(ingestion, "_update_job_progress", fail_progress)
    monkeypatch.setattr(ingestion, "_log", fail_log)

    stats = ingestion.run_ingestion(
        _BrokenProgressConnection(),
        object(),
        "init",
        trade_date,
        trade_date,
        uuid.UUID("00000000-0000-0000-0000-000000000001"),
        0,
    )

    assert stats["progress_update_failures"] == 1
    assert stats["progress_rollback_failures"] == 1
    assert stats["progress_log_failures"] == 1
    assert stats["last_progress_error"] == "progress failed"
    assert stats["last_progress_rollback_error"] == "rollback failed"
    assert stats["last_progress_log_error"] == "log failed"


def test_upsert_normalizes_non_finite_provider_values(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def fake_execute_values(_cursor: object, sql: str, values: list[tuple[Any, ...]]) -> None:
        captured["sql"] = sql
        captured["values"] = values

    monkeypatch.setattr(ingestion.pgx, "execute_values", fake_execute_values)
    row = {
        "trade_date": dt.date(2026, 6, 16),
        "ts_code": "000001.SZ",
        "close": Decimal("10.25"),
        "turnover_rate": float("nan"),
        "turnover_rate_f": Decimal("NaN"),
        "volume_ratio": float("inf"),
        "pe": float("-inf"),
    }

    assert ingestion._upsert_daily_basic(_Connection(), [row]) == 1

    values = captured["values"][0]
    assert values[:3] == (dt.date(2026, 6, 16), "000001.SZ", Decimal("10.25"))
    assert values[3:7] == (None, None, None, None)
    assert all(value is None for value in values[7:])


def test_upsert_preserves_finite_target_when_provider_value_is_invalid(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def fake_execute_values(_cursor: object, sql: str, values: list[tuple[Any, ...]]) -> None:
        captured["sql"] = sql
        captured["values"] = values

    monkeypatch.setattr(ingestion.pgx, "execute_values", fake_execute_values)

    ingestion._upsert_daily_basic(
        _Connection(),
        [{"trade_date": dt.date(2026, 6, 16), "ts_code": "000001.SZ", "free_share": Decimal("Infinity")}],
    )

    assert "free_share=COALESCE(EXCLUDED.free_share, target.free_share)" in captured["sql"]
    assert "INSERT INTO market.daily_basic AS target" in captured["sql"]
    assert captured["values"][0][15] is None


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        pd.NA,
        np.float64("nan"),
        np.float32("inf"),
        float("-inf"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        "NaN",
        "Infinity",
        "-Infinity",
        "nan",
        "inf",
        "-inf",
    ],
)
def test_upsert_normalizes_every_numeric_field_and_keeps_idempotent_sql(
    monkeypatch: Any,
    invalid: object,
) -> None:
    captured: list[tuple[str, list[tuple[Any, ...]]]] = []

    def fake_execute_values(_cursor: object, sql: str, values: list[tuple[Any, ...]]) -> None:
        captured.append((sql, values))

    monkeypatch.setattr(ingestion.pgx, "execute_values", fake_execute_values)
    row = {
        "trade_date": dt.date(2026, 6, 16),
        "ts_code": "000001.SZ",
        **{field: invalid for field in NUMERIC_FIELDS},
    }

    assert ingestion._upsert_daily_basic(_Connection(), [row]) == 1
    assert ingestion._upsert_daily_basic(_Connection(), [row]) == 1

    first_sql, first_values = captured[0]
    assert captured[1] == captured[0]
    assert len(first_values[0]) == 2 + len(NUMERIC_FIELDS)
    assert all(value is None for value in first_values[0][2:])
    for field in NUMERIC_FIELDS:
        assert f"{field}=COALESCE(EXCLUDED.{field}, target.{field})" in first_sql
