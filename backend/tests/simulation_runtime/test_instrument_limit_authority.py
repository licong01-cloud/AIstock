from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.paper_trading_v2.market_data import DailyTradingContextProvider, PreTradeTradabilityProvider
from backend.services.simulation_runtime.decision import ExecutionPlanCompiler, TradingRuleService
from backend.services.simulation_runtime.miniqmt_daily_limit_authority import (
    MINIQMT_NO_DAILY_LIMIT_RULE_VERSION,
    MiniQMTDailyLimitAuthorityProvider,
)
from backend.services.simulation_runtime.models import (
    DailyLimitAuthorityV2,
    DailyTradingAuthorityStateV2,
    SimulationBrokerBackend,
    canonical_json_sha256,
)
from backend.services.simulation_runtime.scheduler import (
    ProductionSimulationRunContextProvider,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


TRADE_DATE = date(2026, 8, 26)
CAPTURED_AT = datetime(2026, 8, 26, 9, 5, tzinfo=UTC)
SYMBOLS = ["000001.SZ", "600000.SH"]
HASH_A = "a" * 64
HASH_B = "b" * 64


class _Cursor:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.rows: list[tuple[object, ...]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, _params: tuple[object, ...]) -> None:
        self.sql.append(sql)
        self.rows = (
            []
            if "FROM market.suspend_d" in sql
            else [("000001.SZ", False, None, None, TRADE_DATE)]
            if "FROM market.stock_st" in sql
            else []
        )

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self.rows)


class _Conn:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _Cursor:
        return self.cursor_value


class _Audit:
    @staticmethod
    def require_success(*, dataset: str, trade_date: date) -> SimpleNamespace:
        return SimpleNamespace(
            dataset=dataset,
            trade_date=trade_date,
            data_source="tushare",
            status="success",
            row_count=1,
            refreshed_at=CAPTURED_AT,
            job_id="job_suspend",
            quality_status="passed",
        )


def _instrument_row(symbol: str, **overrides: object) -> dict[str, object]:
    code, exchange = symbol.split(".", 1)
    return {
        "InstrumentID": code,
        "ExchangeID": exchange,
        "PreClose": 10.0,
        "UpStopPrice": 11.0,
        "DownStopPrice": 9.0,
        "PriceTick": 0.01,
        "InstrumentStatus": 0,
        "IsTrading": True,
        "TradingDay": "20260826",
        "OpenDate": "20100101",
        "DayCountFromIPO": 1000,
        **overrides,
    }


def _supporting_facts(*, symbols: list[str], trade_date: date) -> dict[str, object]:
    normalized = sorted(symbols)
    st = {
        symbol: {
            "is_st": False,
            "source": "market.stock_st.pit",
            "evidence_hash": canonical_json_sha256({"symbol": symbol, "is_st": False}),
        }
        for symbol in normalized
    }
    suspend = {symbol: {"is_suspended": False, "suspend_type": None, "suspend_timing": None} for symbol in normalized}
    return {
        "schema_version": "daily_trading_supporting_facts_v1",
        "trade_date": trade_date.isoformat(),
        "symbol_set": normalized,
        "stock_st": {"source": "market.stock_st", "batch_hash": HASH_A},
        "suspend_d": {"source": "market.suspend_d", "batch_hash": HASH_B},
        "stock_st_facts": st,
        "suspend_facts": suspend,
    }


def _provider(rows: dict[str, dict[str, object]]) -> tuple[MiniQMTDailyLimitAuthorityProvider, list[list[str]]]:
    calls: list[list[str]] = []

    def read(symbols: list[str]) -> dict[str, dict[str, object]]:
        calls.append(list(symbols))
        return {symbol: dict(rows[symbol]) for symbol in symbols if symbol in rows}

    return (
        MiniQMTDailyLimitAuthorityProvider(
            instrument_batch_reader=read,
            supporting_fact_loader=_supporting_facts,
        ),
        calls,
    )


def _load(provider: MiniQMTDailyLimitAuthorityProvider, symbols: list[str] | None = None):
    return provider.load(
        symbols=list(symbols or SYMBOLS),
        trade_date=TRADE_DATE,
        as_of_time=CAPTURED_AT,
        calendar_service_snapshot={"is_trading_day": True, "source": "global-calendar"},
        binding_identity="binding:hash",
        package_identity="package:manifest",
        release_identity="release:hash",
        runtime_identity="QMT_SIM_ACCOUNT:XtQuantQMTClient",
        quote_continuity_identity="c" * 64,
    )


def test_miniqmt_direct_authority_builds_one_v2_batch_without_tushare_or_tdx() -> None:
    provider, calls = _provider({symbol: _instrument_row(symbol) for symbol in SYMBOLS})

    context = _load(provider)

    assert calls == [sorted(SYMBOLS)]
    assert context.broker_backend is SimulationBrokerBackend.MINIQMT_SIM
    assert context.sources.stk_limit is None
    assert context.sources.tdx_reference is None
    assert context.sources.miniqmt_instrument["source"] == "xtdata.get_instrument_detail"
    assert {fact.limit_authority for fact in context.symbols.values()} == {
        DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1
    }
    statuses = provider.to_pre_trade_statuses(context)
    assert all(status["daily_trading_context"]["context"] == context.carrier_payload() for status in statuses.values())


def test_miniqmt_supporting_fact_batch_never_queries_stk_limit() -> None:
    conn = _Conn()
    provider = DailyTradingContextProvider(conn_factory=lambda: conn, audit_repository=_Audit())

    payload = provider.load_supporting_facts(symbols=["000001.SZ"], trade_date=TRADE_DATE)

    sql = "\n".join(conn.cursor_value.sql)
    assert "market.suspend_d" in sql
    assert "market.stock_st" in sql
    assert "market.stk_limit" not in sql
    assert payload["symbol_set"] == ["000001.SZ"]


def test_miniqmt_direct_authority_isolates_one_invalid_symbol_without_reallocation() -> None:
    rows = {symbol: _instrument_row(symbol) for symbol in SYMBOLS}
    rows["600000.SH"].pop("TradingDay")
    provider, _calls = _provider(rows)

    context = _load(provider)

    assert context.symbol_set == tuple(sorted(SYMBOLS))
    assert context.symbols["000001.SZ"].authority_state is DailyTradingAuthorityStateV2.READY
    failed = context.symbols["600000.SH"]
    assert failed.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
    assert failed.limit_authority is DailyLimitAuthorityV2.UNAVAILABLE
    assert failed.pre_close is None and failed.up_limit is None and failed.down_limit is None
    statuses = provider.to_pre_trade_statuses(context)
    assert statuses["600000.SH"]["is_tradable"] is False
    decision = TradingRuleService().decide_order_quantity(
        symbol="600000.SH",
        side="BUY",
        requested_quantity=100,
        tradability_status=statuses["600000.SH"],
    )
    assert decision.legal_quantity == 0
    assert decision.reason_code == "DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE"


def test_miniqmt_direct_authority_rejects_batch_when_every_symbol_fails() -> None:
    rows = {symbol: _instrument_row(symbol, TradingDay="20260825") for symbol in SYMBOLS}
    provider, _calls = _provider(rows)

    with pytest.raises(DataUnavailableError) as error:
        _load(provider)

    assert error.value.context["reason_code"] == "DAILY_TRADING_CONTEXT_AUTHORITY_INVALID"


def test_miniqmt_no_daily_limit_requires_versioned_instrument_evidence() -> None:
    symbol = "000001.SZ"
    provider, _calls = _provider(
        {
            symbol: _instrument_row(
                symbol,
                UpStopPrice=0,
                DownStopPrice=0,
                OpenDate="20260824",
                DayCountFromIPO=3,
            )
        }
    )

    context = _load(provider, [symbol])
    fact = context.symbols[symbol]

    assert fact.authority_state is DailyTradingAuthorityStateV2.NO_DAILY_LIMIT
    assert fact.limit_authority is DailyLimitAuthorityV2.NO_DAILY_LIMIT
    assert fact.rule_version == MINIQMT_NO_DAILY_LIMIT_RULE_VERSION
    assert fact.up_limit is None and fact.down_limit is None


def test_scheduler_miniqmt_daily_context_never_calls_v1_stk_limit_loader() -> None:
    class QmtClient:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def get_instrument_details(self, symbols: list[str]) -> dict[str, dict[str, object]]:
            self.calls.append(list(symbols))
            return {symbol: _instrument_row(symbol) for symbol in symbols}

    class DailyProvider:
        def load(self, **_kwargs: object) -> object:
            raise AssertionError("MiniQMT must not call the V1 stk_limit provider")

        load_supporting_facts = staticmethod(_supporting_facts)

    qmt = QmtClient()
    provider = ProductionSimulationRunContextProvider(
        qmt_client_factory=lambda: qmt,
        daily_trading_context_provider=DailyProvider(),
        position_loader=lambda _strategy, _date: {},
    )
    binding = SimpleNamespace(
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="binding_miniqmt",
        binding_hash="binding_hash",
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_id="strategy_miniqmt",
        binding_config_json={
            "miniqmt_quote_control": {
                "schema_version": "miniqmt_quote_control_binding_v1",
                "control_revision": "B0_QUOTE_V2",
            }
        },
    )
    release = SimpleNamespace(
        package_id="package_miniqmt",
        manifest_sha256="manifest_hash",
        release_id="release_miniqmt",
        release_hash="release_hash",
    )

    statuses = provider.load_daily_trading_context(
        symbols=SYMBOLS,
        trade_date=TRADE_DATE,
        binding=binding,
        runtime_release=release,
        as_of_time=CAPTURED_AT,
        calendar_service_snapshot={"is_trading_day": True, "source": "global-calendar"},
    )

    assert qmt.calls == [sorted(SYMBOLS)]
    assert {status["daily_trading_context"]["limit_authority"] for status in statuses.values()} == {
        "MINIQMT_INSTRUMENT_DETAIL_V1"
    }


def test_execution_plan_and_recovery_read_back_exact_v2_context() -> None:
    provider, _calls = _provider({symbol: _instrument_row(symbol) for symbol in SYMBOLS})
    context = _load(provider)
    statuses = provider.to_pre_trade_statuses(context)
    decisions = [
        SimpleNamespace(
            decision_id=f"decision_{symbol}",
            symbol=symbol,
            price_limit_rule={"pre_trade_tradability": status},
        )
        for symbol, status in statuses.items()
    ]
    binding = SimpleNamespace(
        binding_id="binding_miniqmt",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
    )

    carrier = ExecutionPlanCompiler._daily_trading_context_carrier(
        binding=binding,
        target_trade_date=TRADE_DATE,
        trading_rule_decisions=decisions,
    )
    assert carrier == context.carrier_payload()
    plan = SimpleNamespace(
        plan_id="plan_miniqmt",
        trading_rule_decisions=decisions,
        plan_payload_json={"daily_trading_context": carrier},
    )
    recovered = ProductionSimulationRunContextProvider._frozen_pre_trade_tradability(plan)
    assert set(recovered) == set(SYMBOLS)

    corrupted = deepcopy(statuses[SYMBOLS[0]])
    corrupted["daily_trading_context"]["source_evidence_hash"] = "f" * 64
    bad_decisions = [
        SimpleNamespace(
            decision_id="decision_bad",
            symbol=SYMBOLS[0],
            price_limit_rule={"pre_trade_tradability": corrupted},
        )
    ]
    with pytest.raises(RuntimeConfigInvalidError):
        ExecutionPlanCompiler._daily_trading_context_carrier(
            binding=binding,
            target_trade_date=TRADE_DATE,
            trading_rule_decisions=bad_decisions,
        )


@pytest.mark.parametrize(
    ("no_daily_limit", "last_price", "expected_reason", "expected_source"),
    (
        (False, 11.0, "LIMIT_UP_BUY_BLOCKED", "MINIQMT_INSTRUMENT_DETAIL_V1:frozen_daily_trading_context_v2"),
        (
            True,
            10.5,
            "OK",
            "MINIQMT_INSTRUMENT_DETAIL_V1:frozen_daily_trading_context_v2:no_daily_limit",
        ),
    ),
)
def test_miniqmt_quote_consumes_frozen_v2_limit_without_static_requery(
    no_daily_limit: bool,
    last_price: float,
    expected_reason: str,
    expected_source: str,
) -> None:
    symbol = "000001.SZ"
    row = (
        _instrument_row(symbol, UpStopPrice=0, DownStopPrice=0, OpenDate="20260824", DayCountFromIPO=3)
        if no_daily_limit
        else _instrument_row(symbol)
    )
    authority, _calls = _provider({symbol: row})
    frozen = authority.to_pre_trade_statuses(_load(authority, [symbol]))

    class PoisonStaticProvider:
        def get_suspend_status(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("suspend_d was queried after V2 context freeze")

        def get_st_status(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("stock_st was queried after V2 context freeze")

    provider = PreTradeTradabilityProvider(
        suspend_status_provider=PoisonStaticProvider(),
        st_status_provider=PoisonStaticProvider(),
        realtime_quote_source="MINIQMT_REALTIME.broker_quote",
        realtime_quote_fetcher=lambda _symbols: {
            symbol: {
                "price_basis": "yuan",
                "lastPrice": last_price,
                "pre_close": 9.5,
                "open": 10.1,
                "high": max(10.2, last_price),
                "low": 10.0,
                "volume": 100,
                "amount": 1000,
                "bid_price_1": last_price - 0.01,
                "bid_volume_1": 100,
                "ask_price_1": last_price,
                "ask_volume_1": 100,
                "time": "2026-08-26 10:05:00",
            }
        },
    )

    status = provider.get_statuses(
        [symbol],
        TRADE_DATE,
        require_realtime_quote=True,
        as_of_time=datetime(2026, 8, 26, 10, 5, 30),
        side_by_symbol={symbol: "BUY"},
        frozen_daily_statuses=frozen,
    )[symbol]

    assert status["reason_code"] == expected_reason
    assert status["quote_evidence"]["limit_price_source"] == expected_source
    assert status["daily_trading_context"]["limit_authority"] == (
        "NO_DAILY_LIMIT" if no_daily_limit else "MINIQMT_INSTRUMENT_DETAIL_V1"
    )
