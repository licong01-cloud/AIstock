"""RealtimeDataSource and canonical DB repository contract tests."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.hmm_data_source.db_repository import HMMDataRepository
from backend.services.hmm_data_source.exceptions import DataSourceError, DateRangeError
from backend.services.hmm_data_source.realtime_source import RealtimeDataSource


class FakeRepository:
    def __init__(self) -> None:
        self.completed_date = date(2026, 7, 15)
        self.return_calls: list[dict] = []

    def get_available_date_range(self, *, lag_trading_days, as_of_date):
        assert lag_trading_days == 1
        assert as_of_date == date(2026, 7, 16)
        return date(2024, 1, 2), self.completed_date

    def get_sector_mapping(self, trade_date):
        assert trade_date == self.completed_date
        return {"000001.SZ": "801780.SI"}

    def get_realized_returns(self, **kwargs):
        self.return_calls.append(kwargs)
        return pd.DataFrame([{
            "trade_date": date(2026, 7, 1),
            "symbol": "000001.SZ",
            "horizon_days": 10,
            "future_return": 0.02,
            "label_date": self.completed_date,
        }])


class FakePredictionProvider:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def get_predictions(self, **kwargs):
        self.calls.append(kwargs)
        return pd.DataFrame([{
            "trade_date": date(2026, 7, 15),
            "symbol": "000001.SZ",
            "score": 0.5,
        }])


@pytest.mark.asyncio
async def test_prediction_provider_receives_explicit_candidate_and_completed_date():
    repository = FakeRepository()
    provider = FakePredictionProvider()
    source = RealtimeDataSource(
        candidate_id="candidate-1",
        as_of_date=date(2026, 7, 16),
        repository=repository,
        prediction_provider=provider,
    )

    result = await source.get_predictions(date(2026, 7, 15), date(2026, 7, 15))

    assert len(result) == 1
    assert provider.calls == [{
        "candidate_id": "candidate-1",
        "start_date": date(2026, 7, 15),
        "end_date": date(2026, 7, 15),
        "as_of_date": date(2026, 7, 15),
    }]


@pytest.mark.asyncio
async def test_implicit_latest_candidate_is_rejected():
    source = RealtimeDataSource(
        snapshot_id="latest",
        as_of_date=date(2026, 7, 16),
        repository=FakeRepository(),
        prediction_provider=FakePredictionProvider(),
    )

    with pytest.raises(DataSourceError, match="explicit candidate_id"):
        await source.get_predictions(date(2026, 7, 15), date(2026, 7, 15))


@pytest.mark.asyncio
async def test_missing_prediction_provider_fails_closed():
    source = RealtimeDataSource(
        candidate_id="candidate-1",
        as_of_date=date(2026, 7, 16),
        repository=FakeRepository(),
    )

    with pytest.raises(DataSourceError, match="provider is not configured"):
        await source.get_predictions(date(2026, 7, 15), date(2026, 7, 15))


@pytest.mark.asyncio
async def test_realized_returns_use_completed_as_of_date():
    repository = FakeRepository()
    source = RealtimeDataSource(
        candidate_id="candidate-1",
        as_of_date=date(2026, 7, 16),
        repository=repository,
    )

    result = await source.get_labels(
        date(2026, 7, 1),
        date(2026, 7, 1),
        horizon_days=10,
    )

    assert len(result) == 1
    assert repository.return_calls == [{
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 7, 1),
        "horizon_days": 10,
        "as_of_date": date(2026, 7, 15),
    }]


@pytest.mark.asyncio
async def test_sector_mapping_uses_repository():
    source = RealtimeDataSource(
        candidate_id="candidate-1",
        as_of_date=date(2026, 7, 16),
        repository=FakeRepository(),
    )
    assert await source.get_sector_mapping(date(2026, 7, 15)) == {
        "000001.SZ": "801780.SI"
    }


@pytest.mark.asyncio
async def test_max_query_days_applies_to_labels_and_predictions():
    source = RealtimeDataSource(
        candidate_id="candidate-1",
        as_of_date=date(2026, 7, 16),
        max_query_days=2,
        repository=FakeRepository(),
        prediction_provider=FakePredictionProvider(),
    )
    with pytest.raises(DateRangeError, match="max_query_days"):
        await source.get_labels(date(2026, 7, 1), date(2026, 7, 3))


class RecordingCursor:
    def __init__(self, *, fetchone_values=None, fetchall_values=None) -> None:
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.executed: list[tuple[str, tuple | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.fetchone_values.pop(0)

    def fetchall(self):
        return self.fetchall_values.pop(0)


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


def test_repository_uses_canonical_sector_schema():
    cursor = RecordingCursor(fetchall_values=[[('000001.SZ', '801780.SI')]])
    repository = HMMDataRepository(
        conn_factory=lambda: RecordingConnection(cursor)
    )

    assert repository.get_sector_mapping(date(2026, 7, 15)) == {
        "000001.SZ": "801780.SI"
    }
    query = cursor.executed[0][0]
    assert "market.sw_index_member" in query
    assert "l2_code" in query
    assert "market.sw_member" not in query


def test_repository_resolves_completed_trading_day_not_calendar_subtraction():
    cursor = RecordingCursor(fetchone_values=[
        (date(2024, 1, 2), date(2026, 7, 15)),
        (date(2026, 7, 15),),
    ])
    repository = HMMDataRepository(
        conn_factory=lambda: RecordingConnection(cursor)
    )

    assert repository.get_available_date_range(
        lag_trading_days=1,
        as_of_date=date(2026, 7, 16),
    ) == (date(2024, 1, 2), date(2026, 7, 15))
    query, params = cursor.executed[1]
    assert "market.trading_calendar" in query
    assert "is_trading = TRUE" in query
    assert params == (date(2026, 7, 16), date(2026, 7, 15), 0)


def test_repository_realized_return_query_uses_ts_code_and_close_li():
    cursor = RecordingCursor(fetchall_values=[[
        (
            date(2026, 7, 1),
            "000001.SZ",
            10,
            0.02,
            date(2026, 7, 15),
        )
    ]])
    repository = HMMDataRepository(
        conn_factory=lambda: RecordingConnection(cursor)
    )

    result = repository.get_realized_returns(
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 1),
        horizon_days=10,
        as_of_date=date(2026, 7, 15),
    )

    assert len(result) == 1
    query = cursor.executed[0][0]
    assert "market.trading_calendar" in query
    assert "k1.ts_code" in query
    assert "k1.close_li" in query
    assert "CURRENT_DATE" not in query
