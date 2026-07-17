from __future__ import annotations

from contextlib import contextmanager
from datetime import date

from backend.services.hmm_evolution.market_repository import HMMMarketReturnRepository


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


def test_forward_returns_use_one_bulk_trading_calendar_query() -> None:
    cursor = _Cursor(
        [
            [
                (date(2026, 1, 5), "A", 10, 0.1, date(2026, 1, 19), 4),
                (date(2026, 1, 5), "B", 10, -0.1, date(2026, 1, 19), 4),
            ]
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
    query = cursor.queries[1]
    assert "LEAD(cal_date, %s)" in query[0]
    assert "market.kline_daily_raw" in query[0]
    assert query[1][-2] == ["A", "B"]
    assert read.price_row_count == 4
    assert len(cursor.queries) == 2
