from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pandas as pd
import pytest

from backend.data_service import qe_data_service
from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader
from backend.services.market_data import instrument_validator
from backend.services.strategy_package import selection_artifact
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService


@contextmanager
def _dummy_conn():
    yield object()


def _raw_df(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": [date(2026, 6, 1)] * len(symbols),
            "ts_code": symbols,
            "open_li": [10000] * len(symbols),
            "high_li": [11000] * len(symbols),
            "low_li": [9000] * len(symbols),
            "close_li": [10500] * len(symbols),
            "volume_hand": [100] * len(symbols),
            "amount_li": [1000000] * len(symbols),
        }
    )


def _adj_df(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": [date(2026, 6, 1)] * len(symbols),
            "ts_code": symbols,
            "adj_factor": [1.0] * len(symbols),
        }
    )


def test_shared_validator_rejects_timestamp_mixed_ts_code_before_sql() -> None:
    with pytest.raises(ValueError) as exc_info:
        instrument_validator.normalize_and_validate_ts_codes(
            ["000001.SZ", "603819.S2026-06-01T01:59:30.734977444Z"],
            source="phase1.guard",
            start_date="2026-06-01",
            end_date="2026-06-01",
        )

    message = str(exc_info.value)
    assert "invalid ts_code values before SQL execution" in message
    assert "source=phase1.guard" in message
    assert "invalid_count=1" in message
    assert "603819.S2026-06-01T01:59:30.734977444Z" in message


def test_load_daily_pv_chunks_large_symbol_pool_before_raw_sql(monkeypatch) -> None:
    symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
    raw_calls: list[list[str]] = []
    adj_calls: list[list[str]] = []
    monkeypatch.setattr(qe_data_service, "get_conn", _dummy_conn)
    monkeypatch.setattr(qe_data_service, "DEFAULT_SQL_CHUNK_SIZE", 2)

    def fake_read_sql(sql, _conn, params=None):
        params = list(params or [])
        query_symbols = [item for item in params if isinstance(item, str) and item.endswith(".SZ")]
        if "FROM market.kline_daily_raw" in sql:
            raw_calls.append(query_symbols)
            return _raw_df(query_symbols)
        if "FROM market.adj_factor" in sql:
            adj_calls.append(query_symbols)
            return _adj_df(query_symbols)
        raise AssertionError(f"unexpected SQL: {sql}")

    monkeypatch.setattr(qe_data_service.pd, "read_sql", fake_read_sql)

    result = qe_data_service.load_daily_pv(symbols, "2026-06-01", "2026-06-01")

    assert [len(call) for call in raw_calls] == [2, 2, 1]
    assert [len(call) for call in adj_calls] == [2, 2, 1]
    assert result.index.get_level_values("instrument").nunique() == len(symbols)


def test_realtime_loader_chunks_large_symbol_pool_before_raw_sql(monkeypatch) -> None:
    symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
    raw_calls: list[list[str]] = []
    adj_calls: list[list[str]] = []
    loader = RealtimeFactorDataLoader()
    monkeypatch.setattr("backend.data_service.realtime_factor_data_loader.get_conn", _dummy_conn)
    monkeypatch.setattr("backend.data_service.realtime_factor_data_loader.DEFAULT_SQL_CHUNK_SIZE", 2)

    def fake_read_sql(sql, _conn, params=None):
        params = list(params or [])
        query_symbols = [item for item in params if isinstance(item, str) and item.endswith(".SZ")]
        if "FROM market.kline_daily_raw" in sql:
            raw_calls.append(query_symbols)
            return _raw_df(query_symbols)
        if "FROM market.adj_factor" in sql:
            adj_calls.append(query_symbols)
            return _adj_df(query_symbols)
        raise AssertionError(f"unexpected SQL: {sql}")

    monkeypatch.setattr("backend.data_service.realtime_factor_data_loader.pd.read_sql", fake_read_sql)

    result = loader._fetch_from_db(
        symbols,
        date(2026, 6, 1),
        date(2026, 6, 1),
        {"open_li", "close_li", "adj_factor"},
    )

    assert [len(call) for call in raw_calls] == [2, 2, 1]
    assert [len(call) for call in adj_calls] == [2, 2, 1]
    assert result.index.get_level_values("instrument").nunique() == len(symbols)


class _ReferenceCursor:
    def __init__(self, calls: list[list[str]]) -> None:
        self.calls = calls
        self._rows: list[tuple[str, int]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql, params=None) -> None:
        symbols = list((params or [None, []])[1])
        self.calls.append(symbols)
        self._rows = [(symbol, 10000) for symbol in symbols]

    def fetchall(self):
        return self._rows


class _ReferenceConn:
    def __init__(self, calls: list[list[str]]) -> None:
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return _ReferenceCursor(self.calls)


def test_selection_artifact_reference_price_rejects_malformed_ts_code_before_sql() -> None:
    calls: list[list[str]] = []
    service = StrategyPackageSelectionArtifactService(
        conn_factory=lambda: _ReferenceConn(calls),
        package_repository=object(),
        artifact_repository=object(),
        runtime_asset_resolver=object(),
    )

    with pytest.raises(ValueError) as exc_info:
        service._load_reference_prices(
            ["000001.SZ", "603819.S2026-06-01T01:59:30.734977444Z"],
            date(2026, 6, 1),
        )

    message = str(exc_info.value)
    assert "invalid ts_code values before SQL execution" in message
    assert "StrategyPackageSelectionArtifactService._load_reference_prices" in message
    assert calls == []


def test_selection_artifact_reference_price_chunks_large_symbol_pool(monkeypatch) -> None:
    symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
    calls: list[list[str]] = []
    service = StrategyPackageSelectionArtifactService(
        conn_factory=lambda: _ReferenceConn(calls),
        package_repository=object(),
        artifact_repository=object(),
        runtime_asset_resolver=object(),
    )
    monkeypatch.setattr(selection_artifact, "DEFAULT_SQL_CHUNK_SIZE", 2)

    result = service._load_reference_prices(symbols, date(2026, 6, 1))

    assert [len(call) for call in calls] == [2, 2, 1]
    assert set(result) == set(symbols)
    assert all(price == 10.0 for price in result.values())
