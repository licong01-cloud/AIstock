from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from backend.execution_algos.adaptive_is.contracts import DepthQuantityUnit, QuoteSourceMethod
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.miniqmt_quote_contract_config import QuoteContractPolicy
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    BoundedNormalizedQuoteStore,
    QuoteEvaluationContextStore,
)
from backend.services.miniqmt_execution_runtime.quote_ingress import (
    PhaseOneQuoteProjectionSink,
    PhaseOneRawQuoteSnapshotStore,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import capture_raw_quote_frame
from backend.services.paper_trading_v2.market_data import DailySuspendStatus, EquityInstrumentMetadata, PreviousClose
from backend.services.simulation_runtime.miniqmt_quote_context import (
    MiniQMTQuoteContextAuthorityAdapter,
    QuoteContextSymbolSpec,
)
from backend.services.trading_core.limit_price_provider import DailyLimitPrice


def _policy() -> QuoteContractPolicy:
    return QuoteContractPolicy.from_execution_policy(
        {
            "quote_contract": {
                "schema_version": "miniqmt_quote_contract_policy_v2",
                "control_revision": "B0_QUOTE_V2",
                "required_capabilities": [
                    "CALENDAR",
                    "DEPTH_UNIT_SHARES",
                    "EXCHANGE_TIMESTAMP",
                    "FIVE_LEVEL_DEPTH",
                    "RAW_PRICE_BASIS",
                    "TRADABILITY",
                ],
                "max_receive_age_ms": 1000,
                "max_source_lag_ms": 1000,
                "max_exchange_age_ms": 1000,
                "max_negative_skew_ms": 10,
                "max_clock_age_divergence_ms": 10,
                "max_dependency_group_skew_ms": 100,
                "auction_mode": "OBSERVE_ONLY",
            }
        }
    )


class _Calendar:
    def __init__(self, *, trading: bool = True, checksum: str | None = "calendar-v1") -> None:
        self.trading = trading
        self.checksum = checksum

    def status(self, *, as_of_date: date) -> dict[str, object]:
        return {
            "as_of_date": as_of_date.isoformat(),
            "is_trading_day": self.trading,
            "source": "market.trading_calendar:file_cache",
            "cache": {"checksum": self.checksum},
        }


class _Providers:
    def __init__(self) -> None:
        self.calls = 0
        self.fail = False
        self.listed = True

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        self.calls += 1
        return DailySuspendStatus(symbol=symbol, trade_date=trade_date, is_suspended=False)

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        self.calls += 1
        if self.fail:
            raise RuntimeError("market.stk_limit unavailable")
        return DailyLimitPrice(symbol=symbol, trade_date=trade_date, pre_close=10.0, up_limit=11.0, down_limit=9.0)

    def get_previous_close(self, symbol: str, trade_date: date) -> PreviousClose:
        self.calls += 1
        return PreviousClose(symbol=symbol, trade_date=trade_date, previous_trade_date=trade_date, pre_close=10.0)

    def get_equity_metadata(self, symbol: str, trade_date: date) -> EquityInstrumentMetadata:
        self.calls += 1
        return EquityInstrumentMetadata(
            symbol=symbol,
            market="MAIN",
            exchange="SZSE",
            list_status="L" if self.listed else "D",
            list_date=date(1990, 1, 1),
            delist_date=None,
            product_type="EQUITY",
            source="market.stock_basic",
            source_version="stock-basic-v1",
        )


def _adapter(
    store: QuoteEvaluationContextStore,
    providers: _Providers,
    calendar: _Calendar | None = None,
) -> MiniQMTQuoteContextAuthorityAdapter:
    return MiniQMTQuoteContextAuthorityAdapter(
        context_store=store,
        trading_calendar_service=calendar or _Calendar(),
        suspend_status_provider=providers,
        limit_price_provider=providers,
        previous_close_provider=providers,
        equity_metadata_provider=providers,
    )


def _spec() -> QuoteContextSymbolSpec:
    return QuoteContextSymbolSpec(
        symbol="000001.SZ",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="xtdata-unit-v1",
        price_tick=Decimal("0.01"),
        lot_size=100,
        intraday_halt=False,
        intraday_halt_source="authority-v1",
    )


def _frame():
    return capture_raw_quote_frame(
        {
            "time": "09300000",
            "lastPrice": "10.00",
            "preClose": "10.00",
            "bidPrice": ["9.99", "9.98", None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": ["10.01", "10.02", None, None, None],
            "askVol": [100, 100, 0, 0, 0],
            "openint": "OPEN",
        },
        callback_symbol="000001.SZ",
        source_session_id="context-test-session",
        ingress_generation=3,
        ingress_sequence=1,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=100_000_000,
        clock_domain_id="context-test-domain",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )


def test_context_preload_reuses_authority_providers_without_callback_db_io() -> None:
    store = QuoteEvaluationContextStore()
    providers = _Providers()
    adapter = _adapter(store, providers)
    context = adapter.preload(
        symbol_specs=[_spec()],
        policy=_policy(),
        clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=100_500_000,
        clock_domain_id="context-test-domain",
    )
    assert context.symbol_context("000001.SZ") is not None
    calls_after_preload = providers.calls

    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)
    sink = PhaseOneQuoteProjectionSink(raw_store=raw, normalized_store=normalized, context_store=store)
    sink.replace_admitted(("000001.SZ",))
    sink.on_generation_published(3)
    sink.project(_frame())

    assert providers.calls == calls_after_preload
    assert normalized.get("000001.SZ", context_id=context.context_id) is not None


def test_provider_failure_is_loud_and_does_not_publish_partial_context() -> None:
    store = QuoteEvaluationContextStore()
    providers = _Providers()
    adapter = _adapter(store, providers)
    adapter.preload(
        symbol_specs=[_spec()],
        policy=_policy(),
        clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=100_500_000,
        clock_domain_id="context-test-domain",
    )
    providers.fail = True

    with pytest.raises(QuoteContractError) as exc_info:
        adapter.preload(
            symbol_specs=[_spec()],
            policy=_policy(),
            clock_at_utc=datetime(2026, 7, 12, 1, 31, tzinfo=UTC),
            clock_monotonic_ns=160_500_000,
            clock_domain_id="context-test-domain",
        )

    assert exc_info.value.reason_code.value == "ADAPTIVE_IS_TRADABILITY_DATA_INVALID"
    assert store.snapshot() is None
    assert store.health()["status"] == "INVALID"
    assert store.health()["last_error"]["reason_code"] == "ADAPTIVE_IS_TRADABILITY_DATA_INVALID"


def test_lifecycle_refresh_requires_explicit_symbol_and_policy_authority_without_defaults() -> None:
    store = QuoteEvaluationContextStore()
    providers = _Providers()
    adapter = _adapter(store, providers)

    with pytest.raises(QuoteContractError) as exc_info:
        adapter.refresh_lifecycle(
            clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
            clock_monotonic_ns=100_500_000,
        )

    assert exc_info.value.reason_code.value == "ADAPTIVE_IS_QUOTE_POLICY_SCHEMA_INVALID"
    assert store.health()["status"] == "INVALID"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"unit_evidence_version": ""},
        {"lot_size": 0},
        {"price_tick": Decimal("0")},
        {"intraday_halt": "false"},
    ],
)
def test_context_symbol_spec_rejects_unproven_unit_tick_or_lot_without_defaults(kwargs: dict[str, object]) -> None:
    values = {
        "symbol": "000001.SZ",
        "depth_quantity_unit": DepthQuantityUnit.SHARES,
        "unit_evidence_version": "xtdata-unit-v1",
        "price_tick": Decimal("0.01"),
        "lot_size": 100,
        "intraday_halt": False,
        "intraday_halt_source": "authority-v1",
    }
    values.update(kwargs)
    with pytest.raises(QuoteContractError):
        QuoteContextSymbolSpec(**values)  # type: ignore[arg-type]


def test_calendar_failure_is_loud_and_non_equity_or_halt_remains_explicit_state() -> None:
    store = QuoteEvaluationContextStore()
    providers = _Providers()
    non_trading = _adapter(store, providers, _Calendar(trading=False))
    with pytest.raises(QuoteContractError) as exc_info:
        non_trading.preload(
            symbol_specs=[_spec()], policy=_policy(), clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
            clock_monotonic_ns=1, clock_domain_id="context-test-domain",
        )
    assert exc_info.value.reason_code.value == "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID"
    assert store.snapshot() is None

    providers.listed = False
    non_equity = _adapter(QuoteEvaluationContextStore(), providers)
    context = non_equity.preload(
        symbol_specs=[_spec()], policy=_policy(), clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=2, clock_domain_id="context-test-domain",
    )
    symbol_context = context.symbol_context("000001.SZ")
    assert symbol_context is not None and symbol_context.product_type_proven_equity is False
    assert symbol_context.tradability is not None and symbol_context.tradability.state.value == "STATUS_UNKNOWN"

    providers.listed = True
    halted = _adapter(QuoteEvaluationContextStore(), providers).preload(
        symbol_specs=[QuoteContextSymbolSpec(**{**_spec().__dict__, "intraday_halt": True})],
        policy=_policy(), clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=3, clock_domain_id="context-test-domain",
    )
    assert halted.symbol_context("000001.SZ").tradability.state.value == "INTRADAY_HALT"  # type: ignore[union-attr]
