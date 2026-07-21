from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest

from backend.services.hmm_evolution import market_repository as market_repository_module
from backend.services.hmm_evolution.market_repository import (
    MARKET_RETURN_CALCULATOR_VERSION,
    HMMMarketReturnRepository,
)


class _Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.queries = []
        self.current = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None):
        self.queries.append((" ".join(sql.split()), params))
        if not sql.lstrip().startswith("SET TRANSACTION"):
            self.current = self.rows.pop(0)

    def fetchone(self):
        return self.current

    def fetchall(self):
        return self.current


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _factory(cursor):
    @contextmanager
    def factory():
        yield _Connection(cursor)

    return factory


def test_latest_common_watermark_is_frozen_in_read_only_transaction() -> None:
    cursor = _Cursor(
        [
            (date(2020, 1, 2), date(2026, 7, 17)),
            (date(2026, 7, 16),),
            (4900, 5000),
        ]
    )
    repository = HMMMarketReturnRepository(_factory(cursor))

    watermark = repository.resolve_watermark(
        policy="latest_common_completed",
        requested_date=None,
    )

    assert watermark.resolved_as_of_date == date(2026, 7, 16)
    assert watermark.dataset_max_dates == {
        "market.trading_calendar": date(2026, 7, 17),
        "market.kline_daily_raw": date(2026, 7, 16),
    }
    assert cursor.queries[0][0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    assert all("CURRENT_DATE" not in sql for sql, _params in cursor.queries)


def test_forward_returns_use_bounded_index_friendly_reads_and_preserve_float_semantics() -> None:
    cursor = _Cursor(
        [
            [
                (date(2026, 1, 5), date(2026, 1, 19)),
            ],
            [
                (date(2026, 1, 5), "A", 10),
                (date(2026, 1, 5), "B", 20),
                (date(2026, 1, 19), "A", 11),
                (date(2026, 1, 19), "B", 18),
            ],
        ]
    )
    repository = HMMMarketReturnRepository(_factory(cursor))

    read = repository.read_forward_returns(
        symbols=["B", "A", "A"],
        trade_dates=[date(2026, 1, 5)],
        horizon_trading_days=10,
        as_of_date=date(2026, 1, 19),
    )

    assert len(read.returns) == 2
    assert read.requested_symbol_count == 2
    calendar_query = cursor.queries[1]
    price_query = cursor.queries[2]
    assert "LEAD(cal_date, %s)" in calendar_query[0]
    assert "market.kline_daily_raw" not in calendar_query[0]
    assert "trade_date = ANY(%s)" in price_query[0]
    assert "ts_code = ANY(%s)" in price_query[0]
    assert "JOIN" not in price_query[0]
    assert "CROSS JOIN" not in price_query[0]
    assert price_query[1][1] == ["A", "B"]
    assert read.returns["future_return"].tolist() == pytest.approx([0.1, -0.1])
    assert read.price_row_count == 4
    assert read.missing_evidence == ()
    assert len(cursor.queries) == 3


def test_forward_returns_persist_exact_missing_price_reason() -> None:
    cursor = _Cursor(
        [
            [
                (date(2025, 5, 9), date(2025, 5, 23)),
            ],
            [
                (date(2025, 5, 9), "600358.SH", 10),
                (date(2025, 5, 9), "603557.SH", 10),
                (date(2025, 5, 23), "603557.SH", 12),
            ],
        ]
    )
    repository = HMMMarketReturnRepository(_factory(cursor))

    read = repository.read_forward_returns(
        symbols=["600358.SH", "603557.SH"],
        trade_dates=[date(2025, 5, 9)],
        horizon_trading_days=10,
        as_of_date=date(2025, 5, 23),
    )

    assert read.missing_evidence == (
        {
            "trade_date": "2025-05-09",
            "symbol": "600358.SH",
            "label_date": "2025-05-23",
            "reason": "horizon_price_missing",
        },
    )
    manifest = read.as_manifest_evidence()
    assert manifest["market_return_calculator_version"] == MARKET_RETURN_CALCULATOR_VERSION
    assert len(manifest["market_return_content_hash"]) == 64
    assert manifest["missing_return_count"] == 1
    assert manifest["missing_return_reason_counts"] == {"horizon_price_missing": 1}
    assert read.price_row_count == 2


def test_forward_returns_preserve_missing_reason_priority() -> None:
    cursor = _Cursor(
        [
            [
                (date(2025, 5, 9), None),
                (date(2025, 5, 12), date(2025, 5, 26)),
            ],
            [
                (date(2025, 5, 26), "A", 12),
            ],
        ]
    )
    repository = HMMMarketReturnRepository(_factory(cursor))

    read = repository.read_forward_returns(
        symbols=["A"],
        trade_dates=[date(2025, 5, 9), date(2025, 5, 12)],
        horizon_trading_days=10,
        as_of_date=date(2025, 5, 26),
    )

    assert read.returns.empty
    assert read.missing_evidence == (
        {
            "trade_date": "2025-05-09",
            "symbol": "A",
            "label_date": None,
            "reason": "forward_horizon_not_completed",
        },
        {
            "trade_date": "2025-05-12",
            "symbol": "A",
            "label_date": "2025-05-26",
            "reason": "start_price_missing",
        },
    )


def test_forward_return_price_reads_are_bounded_by_date_and_symbol_batches(
    monkeypatch,
) -> None:
    monkeypatch.setattr(market_repository_module, "_PRICE_DATE_BATCH_SIZE", 2)
    monkeypatch.setattr(market_repository_module, "_PRICE_SYMBOL_BATCH_SIZE", 2)
    calendar_rows = [
        (date(2025, 1, 2), date(2025, 1, 3)),
        (date(2025, 1, 3), date(2025, 1, 6)),
        (date(2025, 1, 6), date(2025, 1, 7)),
    ]
    cursor = _Cursor([calendar_rows, [], [], [], []])
    repository = HMMMarketReturnRepository(_factory(cursor))

    read = repository.read_forward_returns(
        symbols=["A", "B", "C"],
        trade_dates=[date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6)],
        horizon_trading_days=1,
        as_of_date=date(2025, 1, 7),
    )

    price_queries = cursor.queries[2:]
    assert len(price_queries) == 4
    assert all(len(params[0]) <= 2 for _sql, params in price_queries)
    assert all(len(params[1]) <= 2 for _sql, params in price_queries)
    assert all("CROSS JOIN" not in sql for sql, _params in price_queries)
    assert all("JOIN market.kline_daily_raw" not in sql for sql, _params in price_queries)
    assert len(read.missing_evidence) == 9
    assert {item["reason"] for item in read.missing_evidence} == {"start_price_missing"}


def test_market_return_content_hash_changes_when_values_change_without_count_change() -> None:
    cursor = _Cursor(
        [
            [(date(2026, 1, 5), date(2026, 1, 19))],
            [(date(2026, 1, 5), "A", 10), (date(2026, 1, 19), "A", 11)],
            [(date(2026, 1, 5), date(2026, 1, 19))],
            [(date(2026, 1, 5), "A", 10), (date(2026, 1, 19), "A", 12)],
        ]
    )
    repository = HMMMarketReturnRepository(_factory(cursor))
    kwargs = {
        "symbols": ["A"],
        "trade_dates": [date(2026, 1, 5)],
        "horizon_trading_days": 10,
        "as_of_date": date(2026, 1, 19),
    }

    first = repository.read_forward_returns(**kwargs).as_manifest_evidence()
    second = repository.read_forward_returns(**kwargs).as_manifest_evidence()

    assert first["return_row_count"] == second["return_row_count"] == 1
    assert first["market_return_content_hash"] != second["market_return_content_hash"]
