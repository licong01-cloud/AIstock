from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    CLOSING_AUCTION_SCHEMA_VERSION,
    AuctionCapabilityState,
    AuctionFieldManifest,
    ClosingAuctionSnapshot,
    DepthQuantityUnit,
    MarketPhase,
    PriceBasis,
    QuoteSource,
    QuoteSourceMethod,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.services.miniqmt_execution_runtime.quote_auction import ClosingAuctionCapabilityProbe


def _manifest() -> AuctionFieldManifest:
    return AuctionFieldManifest(
        auction_capability_id="xtdata-auction-v1",
        field_map_version="miniqmt-auction-field-map-v1",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        indicative_match_price_field="auctionIndicativePrice",
        indicative_match_volume_field="auctionMatchedVolume",
        unmatched_side_field="auctionUnmatchedSide",
        unmatched_quantity_field="auctionUnmatchedQuantity",
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        volume_unit=DepthQuantityUnit.SHARES,
    )


def test_auction_available_requires_versioned_raw_field_manifest() -> None:
    manifest = _manifest()
    snapshot = ClosingAuctionSnapshot(
        schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
        symbol="000001.SZ",
        clock_event_id="clock-auction-1",
        market_phase=MarketPhase.CLOSING_AUCTION,
        capability_state=AuctionCapabilityState.AVAILABLE,
        exchange_time_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
        received_at_utc=datetime(2026, 7, 13, 6, 57, 1, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256="a" * 64,
        indicative_match_price=Decimal("10.00"),
        indicative_match_volume=Decimal("200"),
        unmatched_side="BUY",
        unmatched_quantity=Decimal("100"),
        auction_field_manifest=manifest,
        source_payload_sha256="b" * 64,
    )
    assert snapshot.auction_capability_id == manifest.auction_capability_id
    assert snapshot.source_field_names == (
        "auctionIndicativePrice",
        "auctionMatchedVolume",
        "auctionUnmatchedSide",
        "auctionUnmatchedQuantity",
    )
    with pytest.raises(QuoteContractError):
        ClosingAuctionSnapshot(
            **{
                **snapshot.__dict__,
                "auction_field_manifest": None,
            }
        )


def test_normal_quote_depth_last_preclose_and_limit_never_synthesize_auction_fields() -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        ClosingAuctionSnapshot(
            schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
            symbol="000001.SZ",
            clock_event_id="clock-auction-1",
            market_phase=MarketPhase.CLOSING_AUCTION,
            capability_state=AuctionCapabilityState.UNAVAILABLE,
            exchange_time_utc=None,
            received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
            source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
            normalized_quote_sha256=None,
            indicative_match_price=Decimal("10.00"),  # ordinary last/quote price is forbidden here
            indicative_match_volume=Decimal("100"),  # ordinary depth is forbidden here
            unmatched_side="BUY",
            unmatched_quantity=Decimal("100"),
            reasons=(QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE,),
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE

    probe = ClosingAuctionCapabilityProbe()
    unavailable = probe.observe(
        symbol="000001.SZ",
        clock_event_id="clock-auction-1",
        market_phase=MarketPhase.CLOSING_AUCTION,
        received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256="a" * 64,
        raw_auction_fields={"lastPrice": "10.00", "bidPrice": ["9.99"], "preClose": "9.80"},
        manifest=None,
        exchange_time_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
    )
    assert unavailable.capability_state == AuctionCapabilityState.UNAVAILABLE
    assert unavailable.indicative_match_price is None


def test_raw_manifest_probe_only_accepts_the_declared_auction_fields() -> None:
    manifest = _manifest()
    probe = ClosingAuctionCapabilityProbe()
    available = probe.observe(
        symbol="000001.SZ",
        clock_event_id="clock-auction-1",
        market_phase=MarketPhase.CLOSING_AUCTION,
        received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256="a" * 64,
        raw_auction_fields={
            "auctionIndicativePrice": "10.01",
            "auctionMatchedVolume": 200,
            "auctionUnmatchedSide": "SELL",
            "auctionUnmatchedQuantity": 50,
        },
        manifest=manifest,
        exchange_time_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
    )
    invalid = probe.observe(
        symbol="000001.SZ",
        clock_event_id="clock-auction-1",
        market_phase=MarketPhase.CLOSING_AUCTION,
        received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256="a" * 64,
        raw_auction_fields={"lastPrice": "10.01"},
        manifest=manifest,
        exchange_time_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
    )
    assert available.capability_state == AuctionCapabilityState.AVAILABLE
    assert available.indicative_match_price == Decimal("10.01")
    assert invalid.capability_state == AuctionCapabilityState.INVALID


def test_raw_manifest_probe_fails_loud_or_invalid_for_non_auction_or_unproven_values() -> None:
    probe = ClosingAuctionCapabilityProbe()
    manifest = _manifest()
    with pytest.raises(QuoteContractError):
        probe.observe(
            symbol="000001.SZ",
            clock_event_id="clock-auction-1",
            market_phase=MarketPhase.CONTINUOUS,
            received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
            source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
            normalized_quote_sha256=None,
            raw_auction_fields=None,
            manifest=manifest,
            exchange_time_utc=None,
        )
    unavailable = probe.observe(
        symbol="000001.SZ",
        clock_event_id="clock-auction-1",
        market_phase=MarketPhase.CLOSING_AUCTION,
        received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256=None,
        raw_auction_fields=None,
        manifest=manifest,
        exchange_time_utc=None,
    )
    invalid = probe.observe(
        symbol="000001.SZ",
        clock_event_id="clock-auction-1",
        market_phase=MarketPhase.CLOSING_AUCTION,
        received_at_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256=None,
        raw_auction_fields={
            "auctionIndicativePrice": True,
            "auctionMatchedVolume": 1,
            "auctionUnmatchedSide": "BUY",
            "auctionUnmatchedQuantity": 1,
        },
        manifest=manifest,
        exchange_time_utc=datetime(2026, 7, 13, 6, 57, tzinfo=UTC),
    )
    assert unavailable.capability_state == AuctionCapabilityState.INVALID
    assert invalid.capability_state == AuctionCapabilityState.INVALID
