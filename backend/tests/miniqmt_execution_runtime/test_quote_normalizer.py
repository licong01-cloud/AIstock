from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    DepthQuantityUnit,
    MarketCode,
    PriceBasis,
    QuoteValidationState,
    TradabilitySnapshot,
    TradabilityState,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.services.miniqmt_execution_runtime.quote_normalizer import (
    capture_raw_quote_frame,
    normalize_raw_quote_frame,
    parse_miniqmt_quote_timestamp_v2,
)
from backend.execution_algos.adaptive_is.contracts import QuoteSourceMethod


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "time": "09300000",
        "lastPrice": "10.00",
        "preClose": "9.80",
        "bidPrice": ["9.99", "9.98", None, None, None],
        "bidVol": [100, 200, 0, 0, 0],
        "askPrice": ["10.01", "10.02", None, None, None],
        "askVol": [100, 200, 0, 0, 0],
        "volume": 1000,
        "amount": 10000,
        "stockStatus": "NORMAL",
        "openint": "OPEN",
    }
    payload.update(overrides)
    return payload


def _frame(payload: dict[str, object]):
    return capture_raw_quote_frame(
        payload,
        callback_symbol="000001.SZ",
        source_session_id="session-normalizer",
        ingress_generation=2,
        ingress_sequence=7,
        received_at_utc=datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
        received_monotonic_ns=999_000,
        clock_domain_id="normalizer-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )


def test_capture_is_whitelist_only_and_deeply_immutable() -> None:
    payload = _payload(account_id="must-not-enter-hash")
    frame = _frame(payload)
    payload["bidPrice"][0] = "1.00"  # type: ignore[index]

    assert "account_id" not in frame.whitelisted_raw_fields
    assert frame.whitelisted_raw_fields["bidPrice"][0] == "9.99"
    with pytest.raises(TypeError):
        frame.whitelisted_raw_fields["time"] = "10000000"  # type: ignore[index]
    assert len(frame.source_payload_sha256) == 64


@pytest.mark.parametrize("field_name,bad_value", [("source", "UNREGISTERED"), ("source_method", "UNREGISTERED")])
def test_raw_quote_frame_rejects_unregistered_source_identity(field_name: str, bad_value: str) -> None:
    frame = _frame(_payload())
    with pytest.raises(QuoteContractError) as exc_info:
        replace(frame, **{field_name: bad_value})
    assert exc_info.value.reason_code == QuoteContractReasonCode.PAYLOAD_INVALID


def test_normalizer_uses_clock_trade_date_for_compact_timestamp_and_preserves_full_depth() -> None:
    quote = normalize_raw_quote_frame(
        _frame(_payload()),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="xtdata-depth-unit-v1",
    )

    assert quote.validation_state == QuoteValidationState.VALID
    assert quote.source_exchange_time_utc == datetime(2026, 7, 12, 1, 30, tzinfo=UTC)
    assert quote.bid_prices == (Decimal("9.99"), Decimal("9.98"), None, None, None)
    assert quote.bid_quantities == (100, 200, 0, 0, 0)
    assert quote.security_status == "NORMAL"
    assert quote.openint_status == "OPEN"


def test_symbol_alias_conflict_is_loud_and_never_fuzzy_matched() -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        _frame(_payload(symbol="000001.SH"))
    assert exc_info.value.reason_code == QuoteContractReasonCode.ALIAS_CONFLICT


def test_invalid_exchange_timestamp_never_becomes_current_time() -> None:
    frame = _frame(_payload(time="not-a-timestamp"))
    with pytest.raises(QuoteContractError) as exc_info:
        normalize_raw_quote_frame(
            frame,
            clock_trade_date=date(2026, 7, 12),
            board="MAIN",
            depth_quantity_unit=DepthQuantityUnit.SHARES,
            unit_evidence_version="unit-v1",
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.TIMESTAMP_INVALID


def test_timestamp_parser_accepts_registered_compact_time_but_rejects_date_only() -> None:
    assert parse_miniqmt_quote_timestamp_v2("093000", trade_date=date(2026, 7, 12)) == datetime(2026, 7, 12, 1, 30, tzinfo=UTC)
    with pytest.raises(QuoteContractError) as exc_info:
        parse_miniqmt_quote_timestamp_v2("2026-07-12", trade_date=date(2026, 7, 12))
    assert exc_info.value.reason_code == QuoteContractReasonCode.TIMESTAMP_INVALID


def test_missing_exchange_timestamp_is_explicit_capability_missing_not_a_current_time_substitute() -> None:
    quote = normalize_raw_quote_frame(
        _frame(_payload(time=None)),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
    )

    assert quote.source_exchange_time_utc is None
    assert quote.validation_state == QuoteValidationState.CAPABILITY_MISSING
    assert QuoteContractReasonCode.TIMESTAMP_INVALID in quote.validation_reasons


@pytest.mark.parametrize(
    "payload",
    [
        _payload(last_price="11.00"),
        _payload(askVol=None),
        _payload(bidPrice=["9.99", "not-a-price", None, None, None]),
    ],
)
def test_alias_and_depth_schema_errors_are_loud(payload: dict[str, object]) -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        normalize_raw_quote_frame(
            _frame(payload),
            clock_trade_date=date(2026, 7, 12),
            board="MAIN",
            depth_quantity_unit=DepthQuantityUnit.SHARES,
            unit_evidence_version="unit-v1",
        )
    assert exc_info.value.reason_code in {
        QuoteContractReasonCode.ALIAS_CONFLICT,
        QuoteContractReasonCode.DEPTH_SCHEMA_INVALID,
        QuoteContractReasonCode.PAYLOAD_INVALID,
    }


def test_non_convertible_depth_quantity_and_missing_lot_evidence_stay_fail_closed() -> None:
    fractional_shares = normalize_raw_quote_frame(
        _frame(_payload(bidVol=["1.5", 2, 0, 0, 0], askVol=["1.5", 2, 0, 0, 0])),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
    )
    unproven_lots = normalize_raw_quote_frame(
        _frame(_payload()),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.LOTS,
        unit_evidence_version="unit-v1",
    )

    for quote in (fractional_shares, unproven_lots):
        assert quote.validation_state != QuoteValidationState.VALID
        assert QuoteContractReasonCode.UNIT_UNPROVEN in quote.validation_reasons


def test_capture_rejects_unsupported_raw_object_instead_of_retaining_callback_object() -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        _frame(_payload(auctionPrice=object()))
    assert exc_info.value.reason_code == QuoteContractReasonCode.PAYLOAD_INVALID


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), Decimal("NaN")])
def test_capture_rejects_non_finite_raw_values_with_typed_loud_error(bad_value: object) -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        _frame(_payload(auctionPrice=bad_value))
    assert exc_info.value.reason_code == QuoteContractReasonCode.PAYLOAD_INVALID


def test_naive_datetime_timestamp_is_frozen_without_current_time_fallback_and_parsed_as_china_time() -> None:
    quote = normalize_raw_quote_frame(
        _frame(_payload(time=datetime(2026, 7, 12, 9, 30))),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
    )
    assert quote.source_exchange_time_utc == datetime(2026, 7, 12, 1, 30, tzinfo=UTC)


def test_l1_only_payload_remains_capability_missing_not_fake_five_level_depth() -> None:
    frame = _frame(
        _payload(
            bidPrice=None,
            bidVol=None,
            askPrice=None,
            askVol=None,
            bid_price_1="9.99",
            bid_volume_1=100,
            ask_price_1="10.01",
            ask_volume_1=100,
        )
    )
    quote = normalize_raw_quote_frame(
        frame,
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
    )

    assert quote.validation_state == QuoteValidationState.CAPABILITY_MISSING
    assert QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING in quote.validation_reasons
    assert quote.bid_prices is None
    assert quote.ask_prices is None


def test_unknown_depth_unit_fails_closed_without_defaulting_to_shares() -> None:
    quote = normalize_raw_quote_frame(
        _frame(_payload()),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.UNKNOWN,
        unit_evidence_version="unit-unproven-v1",
    )

    assert quote.validation_state != QuoteValidationState.VALID
    assert QuoteContractReasonCode.UNIT_UNPROVEN in quote.validation_reasons
    assert quote.bid_quantities is None


def test_source_trade_date_conflict_is_invalid_not_rebased_to_clock_day() -> None:
    quote = normalize_raw_quote_frame(
        _frame(_payload(time="20260711093000")),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
    )

    assert quote.validation_state == QuoteValidationState.INVALID
    assert QuoteContractReasonCode.CLOCK_CALENDAR_INVALID in quote.validation_reasons


def test_lot_depth_uses_explicit_tradability_lot_size() -> None:
    tradability = TradabilitySnapshot(
        schema_version="tradability-v1",
        tradability_id="trad-lots",
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=date(2026, 7, 12),
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=Decimal("9.80"),
        limit_up=Decimal("10.78"),
        limit_down=Decimal("8.82"),
        price_tick=Decimal("0.01"),
        lot_size=100,
        is_suspended=False,
        suspension_source=None,
        security_status="NORMAL",
        openint_status="OPEN",
        observed_at_utc=datetime(2026, 7, 12, 1, tzinfo=UTC),
        source="test",
        source_version="v1",
        state=TradabilityState.TRADABLE,
    )
    quote = normalize_raw_quote_frame(
        _frame(_payload(bidVol=[1, 2, 0, 0, 0], askVol=[1, 2, 0, 0, 0])),
        clock_trade_date=date(2026, 7, 12),
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.LOTS,
        unit_evidence_version="lot-evidence-v1",
        tradability=tradability,
    )

    assert quote.validation_state == QuoteValidationState.VALID
    assert quote.bid_quantities == (100, 200, 0, 0, 0)
    assert quote.bid_quantities_raw == (Decimal("1"), Decimal("2"), Decimal("0"), Decimal("0"), Decimal("0"))
