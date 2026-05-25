import datetime as dt

import pytest
from fastapi import HTTPException

from backend.routers import ingestion
from backend.services.trading_core.errors import DataUnavailableError


class _TableMaxCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        return None

    def fetchone(self):
        return (dt.date(2026, 5, 22),)


class _TableMaxConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _TableMaxCursor()


def test_ingestion_auto_range_uses_calendar_service_for_engine_dataset(monkeypatch):
    calls = []

    def _fake_fetchall(sql, params=()):
        calls.append((sql, params))
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "data_kind": "stk_limit",
                    "table_name": "market.stk_limit",
                    "date_column": "trade_date",
                    "extra_info": {"cursor_source": "refresh_audit"},
                }
            ]
        if "FROM market.dataset_date_refresh_audit" in sql:
            return [{"mx": dt.date(2026, 5, 22)}]
        return []

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)
    monkeypatch.setattr(ingestion, "get_conn", lambda: _TableMaxConn())
    monkeypatch.setattr(ingestion, "_latest_trading_day_on_or_before", lambda _as_of_date=None: dt.date(2026, 5, 25))
    monkeypatch.setattr(ingestion, "_next_trading_day_after", lambda _anchor_date: dt.date(2026, 5, 25))

    response = ingestion.get_ingestion_auto_range("stk_limit")

    assert response["start_date"] == "2026-05-25"
    assert response["latest_date"] == "2026-05-25"
    assert response["current_max_date"] == "2026-05-22"
    assert response["cursor_source"] == "refresh_audit"
    assert not any("market.trading_calendar" in sql for sql, _params in calls)


def test_ingestion_auto_range_fails_fast_when_calendar_service_unavailable(monkeypatch):
    def _fake_fetchall(sql, params=()):
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "data_kind": "stk_limit",
                    "table_name": "market.stk_limit",
                    "date_column": "trade_date",
                    "extra_info": {"cursor_source": "refresh_audit"},
                }
            ]
        if "FROM market.dataset_date_refresh_audit" in sql:
            return [{"mx": dt.date(2026, 5, 22)}]
        return []

    monkeypatch.setattr(ingestion, "_fetchall", _fake_fetchall)
    monkeypatch.setattr(ingestion, "get_conn", lambda: _TableMaxConn())
    monkeypatch.setattr(ingestion, "_latest_trading_day_on_or_before", lambda _as_of_date=None: dt.date(2026, 5, 25))

    def _missing_next(_anchor_date):
        raise DataUnavailableError("trading calendar rows are missing in requested range", context={"missing_count": 1})

    monkeypatch.setattr(ingestion, "_next_trading_day_after", _missing_next)

    with pytest.raises(HTTPException) as exc_info:
        ingestion.get_ingestion_auto_range("stk_limit")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["error_code"] == "DATA_UNAVAILABLE"
    assert exc_info.value.detail["context"]["missing_count"] == 1
