from __future__ import annotations

from types import SimpleNamespace

from backend.routers import ingestion


class _FakeDataFrame:
    empty = False

    def iterrows(self):
        yield 0, {"cal_date": "20260529", "is_open": 1}
        yield 1, {"cal_date": "20260530", "is_open": 0}


class _FakeTushare:
    def pro_api(self, token: str):
        assert token == "token_for_test"
        return self

    def trade_cal(self, **kwargs):
        assert kwargs == {"exchange": "SSE", "start_date": "20260529", "end_date": "20260530"}
        return _FakeDataFrame()


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor()


def test_calendar_sync_refreshes_trading_calendar_status_cache(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setenv("TUSHARE_TOKEN", "token_for_test")
    monkeypatch.setattr(ingestion.importlib, "import_module", lambda name: _FakeTushare() if name == "tushare" else None)
    monkeypatch.setattr(ingestion, "get_conn", lambda: _FakeConn())
    monkeypatch.setattr(
        ingestion.pgx,
        "execute_values",
        lambda cur, sql, rows, *args, **kwargs: captured.setdefault("rows", list(rows)),
    )

    class _FakeCalendarService:
        def _refresh_cache(self, reason: str):
            captured["refresh_reason"] = reason
            return {
                "generated_at": "2026-05-29T09:00:00+08:00",
                "coverage_start": "2026-05-29",
                "coverage_end": "2026-05-30",
                "calendar": [{"date": "2026-05-29", "is_trading": True}],
                "checksum": "checksum_for_test",
                "_refresh_reason": reason,
            }

    monkeypatch.setattr(ingestion, "_trading_calendar_service", lambda: _FakeCalendarService())

    response = ingestion.calendar_sync(
        payload=ingestion.CalendarSyncRequest(
            start_date="2026-05-29",
            end_date="2026-05-30",
            exchange="SSE",
        )
    )

    assert response["inserted_or_updated"] == 2
    assert captured["rows"] == [("2026-05-29", True), ("2026-05-30", False)]
    assert captured["refresh_reason"] == "calendar_sync"
    assert response["calendar_status_cache"] == {
        "generated_at": "2026-05-29T09:00:00+08:00",
        "coverage_start": "2026-05-29",
        "coverage_end": "2026-05-30",
        "calendar_row_count": 1,
        "checksum": "checksum_for_test",
        "refresh_reason": "calendar_sync",
    }


def test_calendar_sync_skips_cache_refresh_when_tushare_returns_no_rows(monkeypatch) -> None:
    class _EmptyDataFrame:
        empty = True

    monkeypatch.setenv("TUSHARE_TOKEN", "token_for_test")
    monkeypatch.setattr(
        ingestion.importlib,
        "import_module",
        lambda name: SimpleNamespace(pro_api=lambda token: SimpleNamespace(trade_cal=lambda **kwargs: _EmptyDataFrame())) if name == "tushare" else None,
    )

    def _unexpected_refresh():
        raise AssertionError("cache refresh should not run without inserted calendar rows")

    monkeypatch.setattr(ingestion, "_trading_calendar_service", _unexpected_refresh)

    response = ingestion.calendar_sync(
        payload=ingestion.CalendarSyncRequest(
            start_date="2026-05-29",
            end_date="2026-05-30",
            exchange="SSE",
        )
    )

    assert response == {"inserted_or_updated": 0, "calendar_status_cache": None}
