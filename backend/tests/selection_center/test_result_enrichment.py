from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.models.analysis import StockQuote
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.result_enrichment import (
    SELECTION_PRICE_MODE_DAILY_DB_ONLY,
    SelectionResultEnrichmentService,
    component_scores_with_display_fields,
    display_fields_from_component_scores,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


class _FakeNameResolver:
    def resolve(self, symbols):
        return {symbol: f"Name-{symbol}" for symbol in symbols}


class _FakeCursor:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.rows = rows if rows is not None else [{"ts_code": "000001.SZ", "close_li": 12340, "volume_hand": 777}]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql, params) -> None:
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_obj = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


def _quote(symbol: str, *, price: float = 13.2) -> StockQuote:
    return StockQuote(
        symbol=symbol,
        name=f"TDX-{symbol}",
        current_price=price,
        pre_close=12.8,
        volume=123456,
        quote_source="TDX_REALTIME",
        quote_timestamp=datetime(2026, 5, 25, 10, 1, 2),
    )


def test_historical_selection_entry_price_uses_pit_reference_close_not_current_quote() -> None:
    cursor = _FakeCursor()
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: _FakeConn(cursor),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=_quote,
        today_provider=lambda: date(2026, 5, 25),
    )

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 5, 13),
        runtime_config={"point_in_time_context": {"reference_price_trade_date": "2026-05-12"}},
    )

    candidate = enriched[0]
    assert candidate.stock_name == "Name-000001.SZ"
    assert candidate.selection_entry_price == pytest.approx(12.34)
    assert candidate.reference_price == pytest.approx(12.34)
    assert candidate.selection_entry_price_source == "market.kline_daily_raw.close:2026-05-12"
    assert candidate.current_price == pytest.approx(13.2)
    assert candidate.current_price_source == "TDX_REALTIME"
    assert candidate.volume == pytest.approx(123456)
    assert cursor.executed[0][1][0] == date(2026, 5, 12)
    assert "trade_date = %s" in cursor.executed[0][0]
    assert "trade_date <= %s" not in cursor.executed[0][0]


def test_historical_selection_rejects_invalid_pit_reference_date_before_query() -> None:
    cursor = _FakeCursor()
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: _FakeConn(cursor),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=_quote,
        today_provider=lambda: date(2026, 5, 25),
    )

    with pytest.raises(
        RuntimeConfigInvalidError,
        match="invalid point_in_time_context reference_price_trade_date",
    ) as exc_info:
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 5, 13),
            runtime_config={"point_in_time_context": {"reference_price_trade_date": "2026/05/12"}},
        )

    assert exc_info.value.context == {
        "reference_price_trade_date": "2026/05/12",
        "allowed_format": "YYYY-MM-DD",
    }
    assert cursor.executed == []


def test_historical_selection_audits_pit_query_failure_before_reference_price_fallback(caplog) -> None:
    def raise_connection_failure():
        raise RuntimeError("database unavailable")

    service = SelectionResultEnrichmentService(
        conn_factory=raise_connection_failure,
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=_quote,
        today_provider=lambda: date(2026, 5, 25),
    )

    caplog.set_level("WARNING", logger="backend.services.selection_center.result_enrichment")

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 5, 13),
        runtime_config={"point_in_time_context": {"reference_price_trade_date": "2026-05-12"}},
    )

    candidate = enriched[0]
    assert candidate.selection_entry_price == pytest.approx(99.0)
    assert candidate.selection_entry_price_source == "selection_candidate.reference_price"
    assert "historical selection entry price query failed" in caplog.text
    assert "trade_date=2026-05-12" in caplog.text
    assert "symbol_count=1" in caplog.text
    assert "error_type=RuntimeError" in caplog.text
    assert "database unavailable" not in caplog.text


def test_current_day_selection_entry_price_uses_tdx_quote_and_display_fields_persist() -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("current-day enrichment must not query PIT daily rows"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda symbol: _quote(symbol, price=15.6),
        today_provider=lambda: date(2026, 5, 25),
    )

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 5, 25),
        runtime_config={},
    )

    candidate = enriched[0]
    assert candidate.selection_entry_price == pytest.approx(15.6)
    assert candidate.reference_price == pytest.approx(15.6)
    assert candidate.current_price == pytest.approx(15.6)

    component_scores = component_scores_with_display_fields(candidate)
    display = display_fields_from_component_scores(component_scores)
    assert display["selection_entry_price"] == pytest.approx(15.6)
    assert display["current_price_display_only"] is True


def test_current_day_selection_uses_tdx_pre_close_before_first_live_price() -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("current-day enrichment must not query PIT daily rows"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda symbol: _quote(symbol, price=0.0),
        today_provider=lambda: date(2026, 5, 25),
    )

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 5, 25),
        runtime_config={},
    )

    candidate = enriched[0]
    assert candidate.selection_entry_price == pytest.approx(12.8)
    assert candidate.reference_price == pytest.approx(12.8)
    assert candidate.previous_close == pytest.approx(12.8)
    assert candidate.current_price is None
    assert candidate.selection_entry_price_source == "TDX latest close / pre_close"

    display = display_fields_from_component_scores(candidate.component_scores)
    assert display["selection_entry_price"] == pytest.approx(12.8)
    assert display["selection_entry_price_source"] == "TDX latest close / pre_close"
    assert display["current_price"] is None


def test_current_day_selection_fails_fast_when_tdx_quote_price_is_missing() -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("current-day enrichment must not query PIT daily rows"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda symbol: StockQuote(symbol=symbol, name="missing-price", quote_source="TDX_REALTIME"),
        today_provider=lambda: date(2026, 5, 25),
    )

    with pytest.raises(DataUnavailableError, match="TDX quote price"):
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 5, 25),
            runtime_config={},
        )


def test_daily_db_only_future_target_uses_cutoff_close_without_realtime_quote() -> None:
    cursor = _FakeCursor()
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: _FakeConn(cursor),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda _symbol: pytest.fail("DAILY_DB_ONLY must not request realtime quotes"),
        today_provider=lambda: date(2026, 9, 12),
    )

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 9, 14),
        runtime_config={
            "selection_price_mode": SELECTION_PRICE_MODE_DAILY_DB_ONLY,
            "point_in_time_context": {"reference_price_trade_date": "2026-09-11"},
        },
    )

    candidate = enriched[0]
    assert candidate.selection_entry_price == pytest.approx(12.34)
    assert candidate.reference_price == pytest.approx(12.34)
    assert candidate.previous_close == pytest.approx(12.34)
    assert candidate.volume == pytest.approx(777)
    assert candidate.selection_entry_price_source == "market.kline_daily_raw.close:2026-09-11"
    assert candidate.selection_entry_price_time == "2026-09-11"
    assert candidate.current_price is None
    assert candidate.current_price_source is None
    assert cursor.executed[0][1][0] == date(2026, 9, 11)


def test_daily_db_only_missing_cutoff_close_fails_closed_without_candidate_fallback() -> None:
    cursor = _FakeCursor(rows=[])
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: _FakeConn(cursor),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda _symbol: pytest.fail("DAILY_DB_ONLY must not request realtime quotes"),
        today_provider=lambda: date(2026, 9, 12),
    )

    with pytest.raises(DataUnavailableError, match="authoritative database close prices") as captured:
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 9, 14),
            runtime_config={
                "selection_price_mode": SELECTION_PRICE_MODE_DAILY_DB_ONLY,
                "point_in_time_context": {"reference_price_trade_date": "2026-09-11"},
            },
        )

    assert captured.value.context["source"] == "market.kline_daily_raw.close_li"
    assert captured.value.context["missing_price_count"] == 1


def test_daily_db_only_uses_last_available_close_for_normal_suspension_gap() -> None:
    cursor = _FakeCursor(
        rows=[
            {
                "ts_code": "000001.SZ",
                "trade_date": date(2026, 9, 10),
                "close_li": 12340,
                "volume_hand": 777,
            }
        ]
    )
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: _FakeConn(cursor),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda _symbol: pytest.fail("DAILY_DB_ONLY must not request realtime quotes"),
        today_provider=lambda: date(2026, 9, 12),
    )

    candidate = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 9, 14),
        runtime_config={
            "selection_price_mode": SELECTION_PRICE_MODE_DAILY_DB_ONLY,
            "point_in_time_context": {"reference_price_trade_date": "2026-09-11"},
        },
    )[0]

    assert candidate.selection_entry_price == pytest.approx(12.34)
    assert candidate.selection_entry_price_source == "market.kline_daily_raw.close:2026-09-10"
    assert candidate.selection_entry_price_time == "2026-09-10"
    assert candidate.volume == 0.0
    assert display_fields_from_component_scores(candidate.component_scores)["reference_price_trade_date"] == "2026-09-10"
    assert "SELECT DISTINCT ON (ts_code)" in cursor.executed[0][0]
    assert "trade_date <= %s" in cursor.executed[0][0]


def test_daily_db_only_database_failure_fails_closed_without_candidate_fallback() -> None:
    def raise_connection_failure():
        raise RuntimeError("database unavailable")

    service = SelectionResultEnrichmentService(
        conn_factory=raise_connection_failure,
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda _symbol: pytest.fail("DAILY_DB_ONLY must not request realtime quotes"),
        today_provider=lambda: date(2026, 9, 12),
    )

    with pytest.raises(DataUnavailableError, match="database price query failed") as captured:
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 9, 14),
            runtime_config={
                "selection_price_mode": SELECTION_PRICE_MODE_DAILY_DB_ONLY,
                "point_in_time_context": {"reference_price_trade_date": "2026-09-11"},
            },
        )

    assert captured.value.context["source"] == "market.kline_daily_raw.close_li"
    assert captured.value.context["error_type"] == "RuntimeError"


@pytest.mark.parametrize("mode", ["UNKNOWN", 1, []])
def test_selection_price_mode_rejects_unknown_values(mode: object) -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("invalid mode must fail before DB access"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda _symbol: pytest.fail("invalid mode must fail before quote access"),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="selection_price_mode"):
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 9, 14),
            runtime_config={"selection_price_mode": mode},
        )


def test_daily_db_only_requires_cutoff_before_target_date() -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("invalid cutoff must fail before DB access"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda _symbol: pytest.fail("invalid cutoff must fail before quote access"),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="must be before trade_date"):
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 9, 14),
            runtime_config={
                "selection_price_mode": SELECTION_PRICE_MODE_DAILY_DB_ONLY,
                "point_in_time_context": {"reference_price_trade_date": "2026-09-14"},
            },
        )
