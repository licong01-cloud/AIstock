from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from backend.services.simulation_runtime.localsim_daily_limit_authority import (
    LocalSimDailyLimitAuthorityProvider,
)
from backend.services.simulation_data.daily_context_provider import DailyTradingContextProvider
from backend.services.simulation_data.daily_context import (
    DailyLimitAuthorityV2,
    DailyTradingAuthorityStateV2,
    SimulationBrokerBackend,
)
from backend.services.trading_core.errors import DataUnavailableError


TRADE_DATE = date(2026, 8, 26)
AS_OF = datetime(2026, 8, 26, 9, 12)
SYMBOLS = ["000001.SZ", "600000.SH"]


def _supporting(*, symbols: list[str], trade_date: date) -> dict:
    assert trade_date == TRADE_DATE
    ordered = sorted(symbols)
    return {
        "schema_version": "daily_trading_supporting_facts_v1",
        "trade_date": trade_date.isoformat(),
        "symbol_set": ordered,
        "stock_st": {"source": "market.stock_st", "batch_hash": "1" * 64},
        "suspend_d": {"source": "market.suspend_d", "batch_hash": "2" * 64},
        "stock_st_facts": {
            symbol: {"is_st": False, "source": "market.stock_st", "evidence_hash": "3" * 64} for symbol in ordered
        },
        "suspend_facts": {
            symbol: {"is_suspended": False, "suspend_type": None, "suspend_timing": None} for symbol in ordered
        },
    }


def _attempt(rows: list[dict], *, availability: str = "AVAILABLE"):
    def load(*, symbols: list[str], trade_date: date) -> dict:
        return {
            "schema_version": "stk_limit_authority_attempt_v1",
            "trade_date": trade_date.isoformat(),
            "symbol_set": sorted(symbols),
            "availability": availability,
            "unavailable_reason": None,
            "refresh_identity": "refresh_test" if availability != "UNAVAILABLE" else None,
            "rows": rows,
        }

    return load


def _row(symbol: str, pre_close: float | None, up: float, down: float) -> dict:
    return {
        "symbol": symbol,
        "trade_date": TRADE_DATE.isoformat(),
        "pre_close": pre_close,
        "up_limit": up,
        "down_limit": down,
    }


def _quote(symbol: str, pre_close_li: int) -> dict:
    return {"K": {"Last": pre_close_li}, "time": "20260826091200"}


def _load(attempt, tdx_reader):
    return LocalSimDailyLimitAuthorityProvider(
        stk_limit_attempt_loader=attempt,
        supporting_fact_loader=_supporting,
        tdx_reference_reader=tdx_reader,
    ).load(
        symbols=SYMBOLS,
        trade_date=TRADE_DATE,
        as_of_time=AS_OF,
        calendar_service_snapshot={"is_trading_day": True},
        binding_identity="binding:hash",
        package_identity="package:manifest",
        release_identity="release:hash",
    )


def test_localsim_uses_stk_limit_first_and_tdx_only_for_missing_symbols() -> None:
    calls: list[list[str]] = []

    def read_tdx(symbols: list[str]) -> dict:
        calls.append(symbols)
        return {"600000.SH": _quote("600000.SH", 8_000)}

    context = _load(_attempt([_row("000001.SZ", 10.0, 11.0, 9.0)]), read_tdx)

    assert context.broker_backend is SimulationBrokerBackend.LOCAL_SIM
    assert calls == [["600000.SH"]]
    assert context.symbols["000001.SZ"].limit_authority is DailyLimitAuthorityV2.TUSHARE_STK_LIMIT
    derived = context.symbols["600000.SH"]
    assert derived.limit_authority is DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1
    assert (derived.pre_close, derived.up_limit, derived.down_limit) == (8.0, 8.8, 7.2)
    assert context.sources.stk_limit is not None
    assert context.sources.tdx_reference is not None


def test_localsim_zero_row_availability_derives_once_for_exact_batch() -> None:
    calls: list[list[str]] = []

    def read_tdx(symbols: list[str]) -> dict:
        calls.append(symbols)
        return {"000001.SZ": _quote("000001.SZ", 10_000), "600000.SH": _quote("600000.SH", 8_000)}

    context = _load(_attempt([], availability="ZERO_ROWS"), read_tdx)
    assert calls == [SYMBOLS]
    assert {fact.limit_authority for fact in context.symbols.values()} == {
        DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1
    }


def test_localsim_deterministic_bad_row_never_uses_tdx_to_mask_it() -> None:
    calls: list[list[str]] = []
    context = _load(
        _attempt([_row("000001.SZ", 10.0, 9.0, 11.0), _row("600000.SH", 8.0, 8.8, 7.2)]),
        lambda symbols: calls.append(symbols) or {},
    )
    assert calls == []
    failed = context.symbols["000001.SZ"]
    assert failed.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
    assert failed.authority_reason_code == "DAILY_TRADING_CONTEXT_STK_LIMIT_INVALID"
    assert context.symbols["600000.SH"].authority_state is DailyTradingAuthorityStateV2.READY


def test_localsim_missing_tdx_symbol_is_isolated_without_reallocation() -> None:
    context = _load(
        _attempt([_row("000001.SZ", 10.0, 11.0, 9.0)]),
        lambda symbols: {},
    )
    assert context.symbols["000001.SZ"].authority_state is DailyTradingAuthorityStateV2.READY
    assert context.symbols["600000.SH"].authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED


def test_localsim_cross_date_stk_limit_fails_batch_without_tdx() -> None:
    bad = _row("000001.SZ", 10.0, 11.0, 9.0)
    bad["trade_date"] = "2026-08-25"
    with pytest.raises(DataUnavailableError, match="cross-date"):
        _load(_attempt([bad]), lambda symbols: pytest.fail("TDX must not mask cross-date corruption"))


def test_localsim_all_unavailable_symbols_fail_the_batch() -> None:
    with pytest.raises(DataUnavailableError, match="every plan symbol"):
        _load(_attempt([], availability="UNAVAILABLE"), lambda symbols: {})


def test_stk_limit_attempt_unavailable_does_not_query_market_table() -> None:
    class Audit:
        @staticmethod
        def require_success(*, dataset: str, trade_date: date):
            raise DataUnavailableError("refresh unavailable")

    provider = DailyTradingContextProvider(
        conn_factory=lambda: pytest.fail("market.stk_limit must not be queried when refresh is unavailable"),
        audit_repository=Audit(),
    )
    result = provider.load_stk_limit_authority_attempt(symbols=["000001.SZ"], trade_date=TRADE_DATE)
    assert result["availability"] == "UNAVAILABLE"
    assert result["rows"] == []


def test_stk_limit_attempt_uses_one_exact_set_based_query() -> None:
    queries: list[tuple[str, tuple]] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql: str, params: tuple) -> None:
            queries.append((sql, params))

        @staticmethod
        def fetchall() -> list[tuple]:
            return [("000001.SZ", TRADE_DATE, 10.0, 11.0, 9.0)]

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def cursor() -> Cursor:
            return Cursor()

    audit = SimpleNamespace(
        require_success=lambda **_kwargs: SimpleNamespace(
            dataset="stk_limit",
            trade_date=TRADE_DATE,
            data_source="tushare",
            status="success",
            row_count=1,
            refreshed_at=AS_OF,
            job_id="job-test",
            quality_status="passed",
        )
    )
    provider = DailyTradingContextProvider(conn_factory=Conn, audit_repository=audit)
    result = provider.load_stk_limit_authority_attempt(symbols=["000001.SZ"], trade_date=TRADE_DATE)
    assert result["availability"] == "AVAILABLE"
    assert len(queries) == 1
    assert "FROM market.stk_limit" in queries[0][0]
    assert queries[0][1] == (["000001.SZ"], TRADE_DATE)
