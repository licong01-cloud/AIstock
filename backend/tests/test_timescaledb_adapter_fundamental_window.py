from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from backend.data_service import timescaledb_adapter
from backend.db import pg_pool
from backend.inference_engine import _fetch_inference_fundamental_data


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False


def _capture_query_start_dates(monkeypatch, *, max_natural_days: int | None) -> list[date]:
    captured: list[date] = []
    monkeypatch.setattr(pg_pool, "get_conn", lambda: _Connection())

    def fake_read_sql(_sql, _conn, *, params):
        captured.append(params[1])
        return pd.DataFrame()

    monkeypatch.setattr(timescaledb_adapter.pd, "read_sql", fake_read_sql)
    result = timescaledb_adapter.fetch_fundamental_data_ts(
        universe=["000001.SZ"],
        start_date=date(2025, 6, 1),
        end_date=date(2026, 7, 15),
        max_natural_days=max_natural_days,
    )
    assert result.empty
    return captured


def test_fundamental_query_keeps_default_180_natural_day_cap(monkeypatch) -> None:
    captured = _capture_query_start_dates(monkeypatch, max_natural_days=180)

    assert captured == [date(2026, 7, 15) - timedelta(days=180)] * 5


def test_fundamental_query_preserves_trading_calendar_window_when_cap_is_disabled(monkeypatch) -> None:
    captured = _capture_query_start_dates(monkeypatch, max_natural_days=None)

    assert captured == [date(2025, 6, 1)] * 5


def test_strict_inference_disables_the_secondary_natural_day_cap(monkeypatch) -> None:
    captured: dict[str, object] = {}
    marker = pd.DataFrame({"value": [1.0]})

    def fake_fetch(**kwargs):
        captured.update(kwargs)
        return marker

    monkeypatch.setattr(timescaledb_adapter, "fetch_fundamental_data_ts", fake_fetch)

    result = _fetch_inference_fundamental_data(
        universe=["000001.SZ"],
        start_date=date(2025, 6, 1),
        end_date=date(2026, 7, 15),
    )

    assert result is marker
    assert captured == {
        "universe": ["000001.SZ"],
        "start_date": date(2025, 6, 1),
        "end_date": date(2026, 7, 15),
        "max_natural_days": None,
    }
