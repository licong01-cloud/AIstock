from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from backend.services.simulation_runtime.daily_limit_authority import (
    DailyLimitAuthorityContractError,
    parse_daily_trading_context,
)
from backend.services.simulation_runtime.models import (
    DAILY_LIMIT_AUTHORITY_BY_BROKER_V2,
    DAILY_LIMIT_RESOLVER_BY_BROKER_V2,
    DailyLimitAuthorityV2,
    DailyTradingAuthorityStateV2,
    DailyTradingContextSourcesV2,
    DailyTradingContextV1,
    DailyTradingContextV2,
    DailyTradingSymbolFactV1,
    DailyTradingSymbolFactV2,
    SimulationBrokerBackend,
    canonical_json_sha256,
)


TRADE_DATE = date(2026, 8, 26)
CAPTURED_AT = datetime(2026, 8, 26, 9, 10, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
RULE_VERSION = "cn_a_share_price_limit_v2_20260706"


def _fact_v2(
    symbol: str,
    *,
    authority: DailyLimitAuthorityV2,
    state: DailyTradingAuthorityStateV2 = DailyTradingAuthorityStateV2.READY,
) -> DailyTradingSymbolFactV2:
    common = {
        "symbol": symbol,
        "trade_date": TRADE_DATE,
        "authority_state": state,
        "limit_authority": authority,
        "source_evidence_hash": HASH_A,
        "is_st": False,
        "st_source": "market.stock_st.pit",
        "st_evidence_hash": HASH_B,
        "is_suspended": False,
        "suspend_source": "market.suspend_d",
        "board": "SH_MAIN" if symbol.endswith(".SH") else "SZ_MAIN",
        "lot_rule": {"min_quantity": 100, "increment": 100},
    }
    if state is DailyTradingAuthorityStateV2.READY:
        return DailyTradingSymbolFactV2(
            **common,
            has_daily_limit=True,
            pre_close=10.0,
            up_limit=11.0,
            down_limit=9.0,
            price_tick=0.01,
            rule_version=RULE_VERSION if authority is DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1 else None,
            derivation_hash=HASH_C if authority is DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1 else None,
        )
    if state is DailyTradingAuthorityStateV2.NO_DAILY_LIMIT:
        return DailyTradingSymbolFactV2(
            **common,
            has_daily_limit=False,
            pre_close=10.0,
            price_tick=0.01,
            rule_version=RULE_VERSION,
            authority_reason_code="IPO_FIRST_FIVE_TRADING_DAYS_V1",
        )
    return DailyTradingSymbolFactV2(
        **common,
        has_daily_limit=False,
        authority_reason_code="DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE",
    )


def _sources_v2(
    broker: SimulationBrokerBackend,
    symbols: dict[str, DailyTradingSymbolFactV2],
) -> DailyTradingContextSourcesV2:
    actual = tuple(sorted({fact.limit_authority for fact in symbols.values()}, key=lambda value: value.value))
    versions = tuple(sorted({fact.rule_version for fact in symbols.values() if fact.rule_version is not None}))
    actual_set = set(actual)
    return DailyTradingContextSourcesV2.build(
        resolver=DAILY_LIMIT_RESOLVER_BY_BROKER_V2[broker],
        allowed_source_kinds=tuple(sorted(DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[broker], key=lambda value: value.value)),
        actual_source_kinds=actual,
        trade_date=TRADE_DATE,
        read_at=CAPTURED_AT,
        rule_versions=versions,
        stock_st={"source": "market.stock_st", "batch_hash": HASH_A},
        suspend_d={"source": "market.suspend_d", "batch_hash": HASH_B},
        stk_limit=(
            {"source": "market.stk_limit", "batch_hash": HASH_A}
            if broker is SimulationBrokerBackend.LOCAL_SIM
            else None
        ),
        tdx_reference=(
            {"source": "TDX_REALTIME.batch_quote.K.Last", "batch_hash": HASH_B}
            if DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1 in actual_set
            else None
        ),
        miniqmt_instrument=(
            {"source": "xtdata.get_instrument_detail", "batch_hash": HASH_C}
            if broker is SimulationBrokerBackend.MINIQMT_SIM
            else None
        ),
    )


def _context_v2(
    broker: SimulationBrokerBackend,
    symbols: dict[str, DailyTradingSymbolFactV2],
) -> DailyTradingContextV2:
    return DailyTradingContextV2.build(
        trade_date=TRADE_DATE,
        plan_identity="plan_v2",
        binding_identity="binding_v2",
        package_identity="package_v2",
        calendar_service_snapshot_id="calendar_snapshot_v2",
        captured_at=CAPTURED_AT,
        broker_backend=broker,
        sources=_sources_v2(broker, symbols),
        symbols=symbols,
    )


def _context_v1() -> DailyTradingContextV1:
    symbol = "600000.SH"
    row_payload = {
        "source": "market.stk_limit",
        "symbol": symbol,
        "trade_date": TRADE_DATE.isoformat(),
        "pre_close": 10.0,
        "up_limit": 11.0,
        "down_limit": 9.0,
        "price_basis": "raw",
    }
    fact = DailyTradingSymbolFactV1(
        symbol=symbol,
        trade_date=TRADE_DATE,
        pre_close=10.0,
        up_limit=11.0,
        down_limit=9.0,
        stk_limit_row_hash=canonical_json_sha256(row_payload),
        is_st=False,
        st_source="market.stock_st",
        st_evidence_hash=HASH_A,
        is_suspended=False,
        suspend_source="market.suspend_d",
        board="SH_MAIN",
        lot_rule={"min_quantity": 100, "increment": 100},
    )
    symbol_set = (symbol,)
    symbol_set_hash = canonical_json_sha256(list(symbol_set))
    sources = {
        "stk_limit": {"source": "market.stk_limit"},
        "stock_st": {"source": "market.stock_st"},
        "suspend_d": {"source": "market.suspend_d"},
    }
    canonical = {
        "schema_version": "daily_trading_context_v1",
        "trade_date": TRADE_DATE.isoformat(),
        "timezone": "Asia/Shanghai",
        "plan_identity": "plan_v1",
        "binding_identity": "binding_v1",
        "package_identity": "package_v1",
        "symbol_set": list(symbol_set),
        "symbol_set_hash": symbol_set_hash,
        "calendar_service_snapshot_id": "calendar_snapshot_v1",
        "captured_at": CAPTURED_AT.isoformat(),
        "sources": sources,
        "symbols": {symbol: fact.canonical_payload()},
    }
    digest = canonical_json_sha256(canonical)
    return DailyTradingContextV1(
        context_id=f"dtc_{digest[:16]}",
        context_hash=digest,
        trade_date=TRADE_DATE,
        plan_identity="plan_v1",
        binding_identity="binding_v1",
        package_identity="package_v1",
        symbol_set=symbol_set,
        symbol_set_hash=symbol_set_hash,
        calendar_service_snapshot_id="calendar_snapshot_v1",
        captured_at=CAPTURED_AT,
        sources=sources,
        symbols={symbol: fact},
    )


def test_localsim_v2_supports_tushare_and_tdx_mixed_source_hash_closed_context() -> None:
    symbols = {
        "000001.SZ": _fact_v2("000001.SZ", authority=DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1),
        "600000.SH": _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.TUSHARE_STK_LIMIT),
    }
    context = _context_v2(SimulationBrokerBackend.LOCAL_SIM, symbols)

    assert context.symbol_set == ("000001.SZ", "600000.SH")
    assert context.sources.limit_resolution.actual_source_kinds == (
        DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
        DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
    )
    readback = parse_daily_trading_context(context.carrier_payload())
    assert isinstance(readback, DailyTradingContextV2)
    assert readback.context_hash == context.context_hash
    assert readback.carrier_payload() == context.carrier_payload()


def test_miniqmt_v2_accepts_only_direct_instrument_authority() -> None:
    symbols = {"600000.SH": _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1)}
    context = _context_v2(SimulationBrokerBackend.MINIQMT_SIM, symbols)

    assert context.sources.stk_limit is None
    assert context.sources.tdx_reference is None
    assert context.sources.miniqmt_instrument is not None


def test_v2_preserves_no_limit_and_symbol_failed_facts_without_disappearing() -> None:
    symbols = {
        "600000.SH": _fact_v2(
            "600000.SH",
            authority=DailyLimitAuthorityV2.NO_DAILY_LIMIT,
            state=DailyTradingAuthorityStateV2.NO_DAILY_LIMIT,
        ),
        "600001.SH": _fact_v2(
            "600001.SH",
            authority=DailyLimitAuthorityV2.UNAVAILABLE,
            state=DailyTradingAuthorityStateV2.SYMBOL_FAILED,
        ),
    }
    context = _context_v2(SimulationBrokerBackend.LOCAL_SIM, symbols)

    assert set(context.symbols) == set(symbols)
    assert context.symbols["600000.SH"].up_limit is None
    assert context.symbols["600001.SH"].pre_close is None
    assert context.symbols["600001.SH"].authority_reason_code == "DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE"


def test_v2_rejects_hash_tampering_and_cross_broker_sources() -> None:
    local_symbols = {"600000.SH": _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.TUSHARE_STK_LIMIT)}
    context = _context_v2(SimulationBrokerBackend.LOCAL_SIM, local_symbols)
    tampered = context.carrier_payload()
    tampered["symbols"]["600000.SH"]["up_limit"] = 12.0
    with pytest.raises(ValidationError, match="context_hash mismatch"):
        DailyTradingContextV2.model_validate(tampered)

    source_tampered = context.carrier_payload()
    source_tampered["sources"]["stock_st"]["batch_hash"] = HASH_C
    with pytest.raises(ValidationError, match="root_batch_hash mismatch"):
        DailyTradingContextV2.model_validate(source_tampered)

    mini_sources = _sources_v2(
        SimulationBrokerBackend.MINIQMT_SIM,
        {"600000.SH": _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1)},
    )
    with pytest.raises(ValidationError, match="resolver does not match broker authority"):
        DailyTradingContextV2.build(
            trade_date=TRADE_DATE,
            plan_identity="plan_cross",
            binding_identity="binding_cross",
            package_identity="package_cross",
            calendar_service_snapshot_id="calendar_cross",
            captured_at=CAPTURED_AT,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            sources=mini_sources,
            symbols=local_symbols,
        )


def test_discriminated_readback_keeps_v1_exact_and_never_guesses_upgrade() -> None:
    v1 = _context_v1()

    readback = parse_daily_trading_context(v1.carrier_payload())
    assert isinstance(readback, DailyTradingContextV1)
    assert readback.carrier_payload() == v1.carrier_payload()

    missing_schema = v1.carrier_payload()
    missing_schema.pop("schema_version")
    with pytest.raises(DailyLimitAuthorityContractError, match="explicit supported schema_version"):
        parse_daily_trading_context(missing_schema)

    unknown_schema = {**v1.carrier_payload(), "schema_version": "daily_trading_context_v3"}
    with pytest.raises(DailyLimitAuthorityContractError, match="explicit supported schema_version"):
        parse_daily_trading_context(unknown_schema)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(has_daily_limit=False),
        lambda payload: payload.update(up_limit=None),
        lambda payload: payload.update(authority_reason_code="UNEXPECTED"),
        lambda payload: payload.update(is_st=1),
        lambda payload: payload.update(pre_close=True),
        lambda payload: payload.update(price_tick=0.03),
        lambda payload: payload.update(lot_rule={"min_quantity": "100", "increment": 100}),
    ],
)
def test_ready_symbol_fact_rejects_partial_or_coerced_authority(mutate) -> None:  # type: ignore[no-untyped-def]
    payload = _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.TUSHARE_STK_LIMIT).model_dump(mode="python")
    mutate(payload)

    with pytest.raises(ValidationError):
        DailyTradingSymbolFactV2.model_validate(payload)


def test_v2_builder_rejects_non_string_symbol_identity_instead_of_coercing() -> None:
    fact = _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.TUSHARE_STK_LIMIT)
    sources = _sources_v2(SimulationBrokerBackend.LOCAL_SIM, {"600000.SH": fact})

    with pytest.raises(ValueError, match="canonical string symbol keys"):
        DailyTradingContextV2.build(
            trade_date=TRADE_DATE,
            plan_identity="plan_invalid_key",
            binding_identity="binding_invalid_key",
            package_identity="package_invalid_key",
            calendar_service_snapshot_id="calendar_invalid_key",
            captured_at=CAPTURED_AT,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            sources=sources,
            symbols={600000: fact},  # type: ignore[dict-item]
        )
