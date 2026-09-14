from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prospective_price_evaluation import (
    PostgresAdvisoryPriceOutcomeSource,
)


class _Cursor:
    def __init__(self, *, audits, opens, suspends) -> None:
        self._audits = audits
        self._opens = opens
        self._suspends = suspends
        self.rows = []
        self.sql: list[str] = []

    def execute(self, sql, _params) -> None:
        normalized = " ".join(str(sql).split())
        self.sql.append(normalized)
        if "dataset_date_refresh_audit" in normalized:
            self.rows = list(self._audits)
        elif "kline_daily_raw" in normalized:
            self.rows = list(self._opens)
        elif "suspend_d" in normalized:
            self.rows = list(self._suspends)
        else:
            raise AssertionError(normalized)

    def fetchall(self):
        return list(self.rows)

    def close(self) -> None:
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.session = None
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def set_session(self, **kwargs) -> None:
        self.session = kwargs

    def rollback(self) -> None:
        self.rollbacks += 1


def _audits():
    return [
        (
            dataset,
            date(2026, 9, 15),
            "test",
            "success",
            "ok" if dataset == "kline_daily_raw" else "empty_valid",
            100,
            datetime(2026, 9, 15, 10, tzinfo=timezone.utc),
        )
        for dataset in ("kline_daily_raw", "suspend_d")
    ]


def _source(*, audits=None, opens=None, suspends=None):
    cursor = _Cursor(
        audits=_audits() if audits is None else audits,
        opens=[] if opens is None else opens,
        suspends=[] if suspends is None else suspends,
    )
    conn = _Connection(cursor)

    @contextmanager
    def factory():
        yield conn

    return PostgresAdvisoryPriceOutcomeSource(connection_context_factory=factory), conn, cursor


def test_postgres_source_is_readonly_and_projects_only_open_and_suspend() -> None:
    source, conn, cursor = _source(
        opens=[("000001.SZ", 10120)],
        suspends=[("000002.SZ", "S")],
    )
    snapshot = source.load(
        symbols=("000001.SZ", "000002.SZ"),
        target_trade_date=date(2026, 9, 15),
    )
    assert snapshot.raw_open_by_symbol == {"000001.SZ": 10.12}
    assert snapshot.suspended_symbols == {"000002.SZ"}
    assert conn.session == {
        "isolation_level": "REPEATABLE READ",
        "readonly": True,
        "autocommit": False,
    }
    price_sql = next(value for value in cursor.sql if "kline_daily_raw" in value)
    assert "SELECT ts_code, open_li" in price_sql
    for forbidden in ("high_li", "low_li", "close_li", "minute", "UPDATE", "INSERT", "DELETE"):
        assert forbidden not in price_sql


def test_postgres_source_keeps_unknown_missing_as_typed_failure() -> None:
    source, _conn, _cursor = _source(opens=[("000001.SZ", 10120)])
    with pytest.raises(AdvisoryModelFirstError) as captured:
        source.load(
            symbols=("000001.SZ", "000002.SZ"),
            target_trade_date=date(2026, 9, 15),
        )
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_UNEXPLAINED_MISSING"


def test_postgres_source_waits_for_both_refresh_audits() -> None:
    source, _conn, _cursor = _source(audits=_audits()[:1])
    with pytest.raises(AdvisoryModelFirstError) as captured:
        source.load(symbols=("000001.SZ",), target_trade_date=date(2026, 9, 15))
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE"
