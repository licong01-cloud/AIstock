from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeOutcomeSourceError,
    HistoricalRangeSymbolPathRequestV1,
    PostgresHistoricalRangeOutcomeSourceProvider,
)


class _Cursor:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.rows = rows
        self.query = ""

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: tuple[object, ...]) -> None:
        self.query = query

    def fetchall(self) -> list[dict[str, object]]:
        return list(self.rows)


class _Connection:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.cursor_instance = _Cursor(rows)

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def set_session(self, **_kwargs: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _Cursor:
        return self.cursor_instance

    def rollback(self) -> None:
        return None


def _membership(*, l3_code: str, in_date: str = "2026-01-01") -> dict[str, object]:
    return {
        "l1_code": "L1",
        "l1_name": "L1_NAME",
        "l2_code": "L2",
        "l2_name": "L2_NAME",
        "l3_code": l3_code,
        "l3_name": l3_code,
        "in_date": in_date,
        "out_date": None,
    }


def test_industry_at_t_fails_closed_on_conflicting_overlaps() -> None:
    connection = _Connection((_membership(l3_code="L3-A"), _membership(l3_code="L3-B")))
    provider = PostgresHistoricalRangeOutcomeSourceProvider(conn_factory=lambda: connection)

    with pytest.raises(HistoricalRangeOutcomeSourceError) as exc_info:
        provider.load_industry_at_t(symbol="000001.SZ", decision_trade_date=date(2026, 7, 1))

    assert exc_info.value.reason_code == "ADVISORY_HR_OUTCOME_INDUSTRY_MEMBERSHIP_CONFLICT"
    assert "LIMIT 1" not in connection.cursor_instance.query.upper()


def test_industry_at_t_accepts_equivalent_duplicate_memberships_deterministically() -> None:
    first = _membership(l3_code="L3-A", in_date="2026-06-01")
    second = _membership(l3_code="L3-A", in_date="2026-01-01")
    connection = _Connection((first, second))
    provider = PostgresHistoricalRangeOutcomeSourceProvider(conn_factory=lambda: connection)

    industry, evidence_hash = provider.load_industry_at_t(symbol="000001.SZ", decision_trade_date=date(2026, 7, 1))

    assert industry == "L3-A"
    assert evidence_hash == canonical_json_sha256(first)


def test_industry_at_t_returns_typed_unknown_when_no_membership_exists() -> None:
    connection = _Connection(())
    provider = PostgresHistoricalRangeOutcomeSourceProvider(conn_factory=lambda: connection)

    industry, evidence_hash = provider.load_industry_at_t(symbol="000001.SZ", decision_trade_date=date(2026, 7, 1))

    assert industry == "UNKNOWN_AT_T"
    assert evidence_hash == canonical_json_sha256(
        {
            "symbol": "000001.SZ",
            "decision_trade_date": date(2026, 7, 1),
            "industry": "UNKNOWN_AT_T",
        }
    )


def _path_row(*, adjustment_present: bool, limit_present: bool) -> dict[str, object]:
    return {
        "trade_date": date(2026, 7, 1),
        "open_li": 10000,
        "high_li": 10500,
        "low_li": 9500,
        "close_li": 10100,
        "adj_factor": 1 if adjustment_present else None,
        "adjustment_present": adjustment_present,
        "suspended": False,
        "up_limit": 11000 if limit_present else None,
        "down_limit": 9000 if limit_present else None,
        "limit_present": limit_present,
    }


def _path_request(
    provider: PostgresHistoricalRangeOutcomeSourceProvider,
) -> HistoricalRangeSymbolPathRequestV1:
    bundle = provider.resolve_source_revision_bundle(
        symbol="000001.SZ",
        start_trade_date=date(2026, 7, 1),
        end_trade_date=date(2026, 7, 1),
        label_as_of_ts=datetime(2026, 7, 1, 12, tzinfo=UTC),
    )
    return HistoricalRangeSymbolPathRequestV1(
        symbol="000001.SZ",
        start_trade_date=date(2026, 7, 1),
        end_trade_date=date(2026, 7, 1),
        label_as_of_trade_date=date(2026, 7, 1),
        source_available_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        price_source=bundle.price_source,
        adjustment_source=bundle.adjustment_source,
        tradability_source=bundle.tradability_source,
        expected_source_revision_set_hash=str(bundle.source_revision_set.source_revision_set_hash),
    )


@pytest.mark.parametrize(
    ("adjustment_present", "limit_present"),
    ((False, True), (True, False)),
)
def test_symbol_path_fails_closed_when_adjustment_or_limits_are_missing(
    adjustment_present: bool,
    limit_present: bool,
) -> None:
    connection = _Connection(
        (
            _path_row(
                adjustment_present=adjustment_present,
                limit_present=limit_present,
            ),
        )
    )
    provider = PostgresHistoricalRangeOutcomeSourceProvider(conn_factory=lambda: connection)
    request = _path_request(provider)

    with pytest.raises(HistoricalRangeOutcomeSourceError) as exc_info:
        provider.load_symbol_path(request)

    assert exc_info.value.reason_code == "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE"


def test_operation_scoped_path_cache_reuses_one_label_as_of_query_and_resets() -> None:
    provider = PostgresHistoricalRangeOutcomeSourceProvider(conn_factory=lambda: None)
    calls: list[tuple[date, date]] = []
    rows = (
        {**_path_row(adjustment_present=True, limit_present=True), "trade_date": date(2026, 7, 1)},
        {**_path_row(adjustment_present=True, limit_present=True), "trade_date": date(2026, 7, 2)},
    )

    def load_rows(*, symbol: str, start_trade_date: date, end_trade_date: date):  # type: ignore[no-untyped-def]
        del symbol
        calls.append((start_trade_date, end_trade_date))
        return rows

    provider._load_symbol_rows = load_rows
    provider.begin_operation("a" * 64)
    first = provider._load_symbol_rows_as_of(
        symbol="000001.SZ",
        start_trade_date=date(2026, 7, 1),
        end_trade_date=date(2026, 7, 1),
        label_as_of_trade_date=date(2026, 7, 2),
    )
    second = provider._load_symbol_rows_as_of(
        symbol="000001.SZ",
        start_trade_date=date(2026, 7, 1),
        end_trade_date=date(2026, 7, 2),
        label_as_of_trade_date=date(2026, 7, 2),
    )
    provider.begin_operation("b" * 64)
    provider._load_symbol_rows_as_of(
        symbol="000001.SZ",
        start_trade_date=date(2026, 7, 1),
        end_trade_date=date(2026, 7, 2),
        label_as_of_trade_date=date(2026, 7, 2),
    )

    assert tuple(row["trade_date"] for row in first) == (date(2026, 7, 1),)
    assert tuple(row["trade_date"] for row in second) == (
        date(2026, 7, 1),
        date(2026, 7, 2),
    )
    assert calls == [
        (date(2026, 7, 1), date(2026, 7, 2)),
        (date(2026, 7, 1), date(2026, 7, 2)),
    ]
